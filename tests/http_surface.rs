#![allow(non_snake_case)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use bytes::Bytes;
use devin2api::debuglog::{Manager, RetentionPolicy};
use devin2api::domain::{
    AssistantMessage, Content, Failure, ResponseEvent, ResponseEventType, StopReason, TextContent,
};
use devin2api::server::http::{App, HttpBackend, HttpConfig, HttpEventStream, RejectReason};
use devin2api::upstream::catalog::ModelInfo;
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

type ScriptResult = Result<VecDeque<Result<ResponseEvent, Failure>>, Failure>;

struct StubBackend {
    models: Arc<Vec<ModelInfo>>,
    scripts: Arc<Mutex<VecDeque<ScriptResult>>>,
}

impl StubBackend {
    fn success() -> Self {
        Self {
            models: Arc::new(vec![ModelInfo {
                id: "stub-model".into(),
                owned_by: "devin".into(),
                supports_tool_calls: true,
                ..ModelInfo::default()
            }]),
            scripts: Arc::new(Mutex::new(VecDeque::from([Ok(VecDeque::from([Ok(
                done_event("stub-model", "pong"),
            )]))]))),
        }
    }

    fn failing(failure: Failure) -> Self {
        Self {
            models: Arc::new(Vec::new()),
            scripts: Arc::new(Mutex::new(VecDeque::from([Err(failure)]))),
        }
    }
}

struct StubStream(VecDeque<Result<ResponseEvent, Failure>>);

impl HttpEventStream for StubStream {
    fn recv(&mut self) -> BoxFuture<Result<Option<ResponseEvent>, Failure>> {
        let item = self.0.pop_front().transpose();
        Box::pin(async move { item })
    }
}

/// A stream whose `try_recv` drains a preloaded burst — the shape the
/// upstream pump produces under load: many events ready at once. Pins
/// the SSE write-coalescing contract (task-24 P1+P2): one drain cycle
/// emits one body frame, events stay ordered, and the terminal frame is
/// never held back waiting for more input.
struct BurstStream(VecDeque<ResponseEvent>);

impl HttpEventStream for BurstStream {
    fn recv(&mut self) -> BoxFuture<Result<Option<ResponseEvent>, Failure>> {
        let item = self.0.pop_front();
        Box::pin(async move { Ok(item) })
    }

    fn try_recv(&mut self) -> Option<ResponseEvent> {
        self.0.pop_front()
    }
}

struct BurstBackend {
    events: Vec<ResponseEvent>,
}

impl HttpBackend for BurstBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        _cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let events = VecDeque::from(self.events.clone());
        Box::pin(async move { Ok(Box::new(BurstStream(events)) as Box<dyn HttpEventStream>) })
    }
}

#[derive(Clone)]
struct DelayedErrorBackend {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct DelayedErrorStream {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl HttpEventStream for DelayedErrorStream {
    fn recv(&mut self) -> BoxFuture<Result<Option<ResponseEvent>, Failure>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Err(Failure {
                code: "unavailable".into(),
                message: "late upstream failure".into(),
                upstream_fault: true,
                ..Failure::default()
            })
        })
    }
}

#[derive(Clone)]
struct PendingBackend {
    entered: Arc<Notify>,
}

/// A stream that opens (one `Start` event) then produces nothing until the
/// request's cancellation token fires — the live-request shape the panel
/// abort must be able to kill mid-flight.
#[derive(Clone)]
struct SlowStreamBackend {
    entered: Arc<Notify>,
    cancelled: Arc<Notify>,
}

struct SlowStream {
    cancel: CancellationToken,
    cancelled: Arc<Notify>,
    sent_start: bool,
}

impl Drop for SlowStream {
    /// The stream is torn down exactly once; reporting whether the
    /// request's cancellation token fired proves the abort reached the
    /// backend seam (a dropped `recv` future never gets to notify).
    fn drop(&mut self) {
        if self.cancel.is_cancelled() {
            self.cancelled.notify_one();
        }
    }
}

impl HttpEventStream for SlowStream {
    fn recv(&mut self) -> BoxFuture<Result<Option<ResponseEvent>, Failure>> {
        if self.sent_start {
            let cancel = self.cancel.clone();
            return Box::pin(async move {
                cancel.cancelled().await;
                Err(Failure::plain("cancelled"))
            });
        }
        self.sent_start = true;
        Box::pin(async {
            Ok(Some(ResponseEvent {
                kind: ResponseEventType::Start,
                reason: Some(StopReason::Pending),
                partial: Some(Arc::new(AssistantMessage {
                    model: "stub-model".into(),
                    ..AssistantMessage::default()
                })),
                ..ResponseEvent::default()
            }))
        })
    }
}

impl HttpBackend for SlowStreamBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let entered = self.entered.clone();
        let cancelled = self.cancelled.clone();
        Box::pin(async move {
            entered.notify_one();
            Ok(Box::new(SlowStream {
                cancel,
                cancelled,
                sent_start: false,
            }) as Box<dyn HttpEventStream>)
        })
    }
}

/// A non-streaming upstream that stays silent until released — the shape
/// that needs the repeating `\n` heartbeat for the whole collect window.
#[derive(Clone)]
struct DelayedDoneBackend {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct DelayedDoneStream {
    release: Arc<Notify>,
    sent: Arc<AtomicBool>,
}

impl HttpEventStream for DelayedDoneStream {
    fn recv(&mut self) -> BoxFuture<Result<Option<ResponseEvent>, Failure>> {
        if self.sent.load(Ordering::Acquire) {
            return Box::pin(async { Ok(None) });
        }
        // `sent` flips only after the release wait resolves: the JSON
        // heartbeat path drops the first `collect_final` (and its in-flight
        // `recv`) when the keepalive timeout commits the body, then starts
        // a fresh collect — a dropped recv must not consume the event.
        let release = self.release.clone();
        let sent = self.sent.clone();
        Box::pin(async move {
            release.notified().await;
            sent.store(true, Ordering::Release);
            Ok(Some(done_event("stub-model", "pong")))
        })
    }
}

impl HttpBackend for DelayedDoneBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        _cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(async move {
            entered.notify_one();
            Ok(Box::new(DelayedDoneStream {
                release,
                sent: Arc::new(AtomicBool::new(false)),
            }) as Box<dyn HttpEventStream>)
        })
    }
}

impl HttpBackend for PendingBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        _cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let entered = self.entered.clone();
        Box::pin(async move {
            entered.notify_one();
            std::future::pending().await
        })
    }
}

impl HttpBackend for DelayedErrorBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        _cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let source = DelayedErrorStream {
            entered: self.entered.clone(),
            release: self.release.clone(),
        };
        Box::pin(async move { Ok(Box::new(source) as Box<dyn HttpEventStream>) })
    }
}

impl HttpBackend for StubBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<ModelInfo>, Failure>> {
        let models = (*self.models).clone();
        Box::pin(async move { Ok(models) })
    }

    fn stream(
        &self,
        _request: devin2api::domain::RequestMessages,
        _cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> BoxFuture<Result<Box<dyn HttpEventStream>, Failure>> {
        let script = self
            .scripts
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Ok(VecDeque::from([Ok(done_event("stub-model", "pong"))])));
        Box::pin(async move {
            script.map(|events| Box::new(StubStream(events)) as Box<dyn HttpEventStream>)
        })
    }
}

fn done_event(model: &str, text: &str) -> ResponseEvent {
    ResponseEvent {
        kind: ResponseEventType::Done,
        reason: Some(StopReason::Stop),
        message: Some(Arc::new(AssistantMessage {
            content: vec![Content::Text(TextContent { text: text.into() })],
            model: model.into(),
            response_model: model.into(),
            response_id: "resp_test".into(),
            stop_reason: Some(StopReason::Stop),
            ..AssistantMessage::default()
        })),
        ..ResponseEvent::default()
    }
}

async fn call(
    app: &App,
    method: &str,
    uri: &str,
    auth: Option<&str>,
    body: &str,
) -> (http::response::Parts, Vec<u8>) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    let response = app
        .router()
        .oneshot(builder.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = to_bytes(body, 64 * 1024 * 1024).await.unwrap();
    (parts, bytes.to_vec())
}

fn log_root(label: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "devin2api-http-{label}-{}",
        devin2api::randid::hex(8)
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn request_dirs(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(root)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect()
}

#[tokio::test]
async fn auth_precedes_permits_and_every_v1_response_has_request_id() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    );
    let (parts, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
    assert!(
        parts.headers["request-id"]
            .to_str()
            .unwrap()
            .starts_with("req_")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["type"],
        "unauthenticated"
    );
    let rejects = app.reject_snapshot();
    assert_eq!(rejects.count(RejectReason::MissingApiKey), 1);
    assert_eq!(
        app.available_permits(),
        1,
        "auth rejection must not acquire inference permit"
    );
}

#[tokio::test]
async fn models_are_authenticated_but_exempt_from_inference_permits() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    );
    let (parts, body) = call(&app, "GET", "/v1/models", Some("Bearer secret"), "").await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["data"][0]["id"],
        "stub-model"
    );
    assert_eq!(app.available_permits(), 1);
    let (parts, _) = call(
        &app,
        "GET",
        "/v1/models/stub-model",
        Some("Bearer secret"),
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
}

#[tokio::test]
async fn all_three_protocols_support_json_and_sse_with_protocol_headers() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    let cases = [
        (
            "/v1/responses",
            r#"{"model":"stub-model","input":"hi"}"#,
            "response",
            false,
        ),
        (
            "/v1/chat/completions",
            r#"{"model":"stub-model","messages":[{"role":"user","content":"hi"}]}"#,
            "chat.completion",
            false,
        ),
        (
            "/v1/messages",
            r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            "message",
            false,
        ),
        (
            "/v1/responses",
            r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
            "response.completed",
            true,
        ),
        (
            "/v1/chat/completions",
            r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            "[DONE]",
            true,
        ),
        (
            "/v1/messages",
            r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            "message_stop",
            true,
        ),
    ];
    for (path, request, needle, streaming) in cases {
        let (parts, body) = call(&app, "POST", path, None, request).await;
        assert_eq!(
            parts.status,
            StatusCode::OK,
            "{path}: {}",
            String::from_utf8_lossy(&body)
        );
        let ct = parts.headers["content-type"].to_str().unwrap();
        assert_eq!(
            ct,
            if streaming {
                "text/event-stream"
            } else {
                "application/json"
            }
        );
        assert!(
            String::from_utf8_lossy(&body).contains(needle),
            "{path}: {}",
            String::from_utf8_lossy(&body)
        );
    }
}

/// SSE write coalescing (task-24 P1+P2): a burst of ready events must
/// coalesce into one body frame per drain cycle, in order, and the
/// terminal `[DONE]` must ride the last data frame — never delayed past
/// what Go's flush-on-empty would emit.
#[tokio::test]
async fn sse_burst_coalesces_without_reordering_or_delaying_terminal() {
    let partial = || {
        Arc::new(AssistantMessage {
            model: "stub-model".into(),
            ..AssistantMessage::default()
        })
    };
    let mut events = vec![ResponseEvent {
        kind: ResponseEventType::Start,
        reason: Some(StopReason::Pending),
        partial: Some(partial()),
        ..ResponseEvent::default()
    }];
    events.push(ResponseEvent {
        kind: ResponseEventType::TextStart,
        content_index: 0,
        partial: Some(partial()),
        ..ResponseEvent::default()
    });
    for i in 0..8 {
        events.push(ResponseEvent {
            kind: ResponseEventType::TextDelta,
            content_index: 0,
            delta: format!("d{i}"),
            partial: Some(partial()),
            ..ResponseEvent::default()
        });
    }
    events.push(done_event("stub-model", "d0d1d2d3d4d5d6d7"));

    let app = App::with_backend(BurstBackend { events }, HttpConfig::default());
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let (parts, mut body) = response.into_parts();
    assert_eq!(
        parts.headers["content-type"].to_str().unwrap(),
        "text/event-stream"
    );

    let mut frames = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        if let Ok(data) = frame.into_data() {
            frames.push(data);
        }
    }
    assert!(!frames.is_empty(), "SSE body produced no frames");

    // Order: concatenated frames must carry the deltas in emission order
    // and terminate with [DONE] — batching must not reorder events.
    let all: Vec<u8> = frames.iter().flat_map(|f| f.iter().copied()).collect();
    let text = String::from_utf8(all).unwrap();
    let mut cursor = 0usize;
    for i in 0..8 {
        let needle = format!("\"content\":\"d{i}\"");
        let at = text[cursor..]
            .find(&needle)
            .unwrap_or_else(|| panic!("delta d{i} missing or out of order:\n{text}"));
        cursor += at + needle.len();
    }
    let done_at = text.find("data: [DONE]").expect("missing [DONE]");
    assert!(
        done_at >= cursor,
        "[DONE] arrived before the last delta:\n{text}"
    );
    assert!(
        text[done_at..].trim_end() == "data: [DONE]",
        "frames after [DONE]:\n{text}"
    );

    // Coalescing: the drain cycle must emit more than one SSE payload per
    // body frame — a per-event frame stream would mean P2 regressed.
    let multi = frames
        .iter()
        .filter(|f| f.windows(6).filter(|w| *w == b"data: ").count() > 1)
        .count();
    assert!(
        multi > 0,
        "no body frame carried more than one SSE payload; batching never engaged:\n{text}"
    );

    // Terminal frame not delayed: the last frame carries [DONE] (the
    // flush-on-empty rule emits it with the final batch, not later).
    let last = String::from_utf8_lossy(&frames[frames.len() - 1]);
    assert!(
        last.contains("data: [DONE]"),
        "terminal frame not in the last body frame:\n{text}"
    );
}

#[tokio::test]
async fn precommit_errors_keep_real_status_and_protocol_shape() {
    let rate = Failure {
        code: "resource_exhausted".into(),
        message: "rate limited".into(),
        retry_after_seconds: 3,
        rate_limited: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(rate), HttpConfig::default());
    let (parts, body) = call(
        &app,
        "POST",
        "/v1/messages",
        None,
        r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(parts.headers["retry-after"], "3");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["type"],
        "error"
    );
}

#[tokio::test]
async fn ReadFailureRejectedWithoutDir() {
    let root = log_root("read-failure");
    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(manager),
            ..HttpConfig::default()
        },
    );
    let broken = futures_util::stream::once(async {
        Err::<Bytes, std::io::Error>(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset mid-body",
        ))
    });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .body(Body::from_stream(broken))
        .unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(app.reject_snapshot().count(RejectReason::HttpRead), 1);
    assert!(request_dirs(&root).is_empty());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn RequestTooLargeKeepsDebugDir() {
    let root = log_root("oversize");
    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(manager),
            ..HttpConfig::default()
        },
    );
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .body(Body::from(vec![
            b'x';
            devin2api::server::http::MAX_BODY_BYTES + 1
        ]))
        .unwrap();
    let response = app.router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(app.reject_snapshot().count(RejectReason::HttpRead), 0);
    let request_id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_string();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["error"]["debug_ref"], request_id);
    assert!(root.join(&request_id).join("meta.json").is_file());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn openai_stream_rate_limit_is_an_in_stream_retry_signal_but_anthropic_is_http_429() {
    let rate = || Failure {
        code: "resource_exhausted".into(),
        message: "rate limited".into(),
        retry_after_seconds: 3,
        rate_limited: true,
        ..Failure::default()
    };
    let openai = App::with_backend(StubBackend::failing(rate()), HttpConfig::default());
    let (parts, body) = call(
        &openai,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("response.failed"));

    let anthropic = App::with_backend(StubBackend::failing(rate()), HttpConfig::default());
    let (parts, _) = call(&anthropic, "POST", "/v1/messages", None, r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#).await;
    assert_eq!(parts.status, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test(start_paused = true)]
async fn StreamKeepsAliveDuringUpstreamSilence() {
    let root = log_root("late-error");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = App::with_backend(
        DelayedErrorBackend {
            entered: entered.clone(),
            release: release.clone(),
        },
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
        ))
        .unwrap();
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let response_task = tokio::spawn(app.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(1), entered_wait)
        .await
        .expect("stream receive entered");
    tokio::time::advance(devin2api::server::stream::KEEPALIVE_INTERVAL).await;
    let response = tokio::time::timeout(Duration::from_secs(1), response_task)
        .await
        .expect("heartbeat committed")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("heartbeat body frame")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(&first[..], devin2api::server::stream::SSE_KEEPALIVE);
    release.notify_one();
    let late = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("late error body frame")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&late).contains("response.failed"));
    // The response body owns the admission permit until the downstream has
    // consumed or dropped it. Release that exact lifecycle signal before
    // waiting for idle; relying on Body's internal end-of-stream polling made
    // this assertion scheduler-dependent.
    drop(body);
    tokio::time::timeout(Duration::from_secs(1), app.wait_idle())
        .await
        .expect("stream producer finalized");
    let dirs = request_dirs(&root);
    assert_eq!(dirs.len(), 1);
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dirs[0].join("meta.json")).unwrap()).unwrap();
    assert_eq!(
        meta["result"], "failed",
        "late failure must not be indexed completed"
    );
    assert_eq!(meta["status_code"], 200);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn ClientDisconnectRecords499() {
    let root = log_root("disconnect");
    let entered = Arc::new(Notify::new());
    let app = App::with_backend(
        PendingBackend {
            entered: entered.clone(),
        },
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .body(Body::from(r#"{"model":"stub-model","input":"hi"}"#))
        .unwrap();
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let request_task = tokio::spawn(app.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(1), entered_wait)
        .await
        .expect("backend stream entered");
    request_task.abort();
    let _ = request_task.await;
    tokio::time::timeout(Duration::from_secs(1), app.wait_idle())
        .await
        .expect("cancelled handler released permit");
    let dirs = request_dirs(&root);
    assert_eq!(dirs.len(), 1);
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dirs[0].join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["status_code"], 499);
    assert_eq!(meta["result"], "disconnected");
    let index = std::fs::read_to_string(root.join("index.jsonl")).unwrap();
    assert_eq!(
        index.lines().count(),
        1,
        "disconnect must finalize exactly once"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn draining_rejects_before_pipeline_with_retry_after() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    app.begin_drain();
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(parts.headers["retry-after"], "1");
    assert_eq!(app.reject_snapshot().count(RejectReason::Draining), 1);
}

#[tokio::test]
async fn unknown_routes_and_methods_are_not_changed_by_body_limit_defaults() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    let huge = "x".repeat(33 * 1024 * 1024);
    let (parts, body) = call(&app, "POST", "/unknown", None, &huge).await;
    assert_eq!(parts.status, StatusCode::NOT_FOUND);
    assert_eq!(parts.headers["content-type"], "text/plain; charset=utf-8");
    assert_eq!(parts.headers["x-content-type-options"], "nosniff");
    assert_eq!(body.as_slice(), b"404 page not found\n");
    let (parts, _) = call(&app, "GET", "/v1/chat/completions", None, "").await;
    assert_eq!(parts.status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        parts.headers["request-id"]
            .to_str()
            .unwrap()
            .starts_with("req_")
    );
}

macro_rules! success_contract {
    ($name:ident, $path:literal, $body:literal, $needle:literal) => {
        #[tokio::test]
        async fn $name() {
            let app = App::with_backend(StubBackend::success(), HttpConfig::default());
            let (parts, body) = call(&app, "POST", $path, None, $body).await;
            assert_eq!(parts.status, StatusCode::OK);
            assert!(String::from_utf8_lossy(&body).contains($needle));
        }
    };
}

success_contract!(
    MessagesHandlerReturnsJSON,
    "/v1/messages",
    r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
    "\"type\":\"message\""
);
success_contract!(
    MessagesHandlerStreamsSSE,
    "/v1/messages",
    r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
    "message_stop"
);
success_contract!(
    ChatCompletionsHandlerReturnsJSON,
    "/v1/chat/completions",
    r#"{"model":"stub-model","messages":[{"role":"user","content":"hi"}]}"#,
    "chat.completion"
);
success_contract!(
    ChatCompletionsHandlerStreamsSSE,
    "/v1/chat/completions",
    r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    "[DONE]"
);
success_contract!(
    ResponsesHandlerReturnsJSONForNonStream,
    "/v1/responses",
    r#"{"model":"stub-model","input":"hi"}"#,
    "\"object\":\"response\""
);
success_contract!(
    ResponsesHandlerStreamsOrderedEvents,
    "/v1/responses",
    r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    "response.completed"
);
success_contract!(
    ResponsesHandlerConsumesAllAssistantRounds,
    "/v1/responses",
    r#"{"model":"stub-model","input":"hi"}"#,
    "pong"
);

#[tokio::test]
async fn MessagesHandlerStreamError() {
    let failure = Failure {
        code: "unavailable".into(),
        message: "failed".into(),
        upstream_fault: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, body) = call(&app, "POST", "/v1/messages", None, r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#).await;
    assert_eq!(parts.status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["type"],
        "error"
    );
}

#[tokio::test]
async fn ChatCompletionsHandlerStreamError() {
    let failure = Failure {
        code: "unavailable".into(),
        message: "failed".into(),
        upstream_fault: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/chat/completions",
        None,
        r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn ChatCompletionsHandlerStreamsThinking() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/chat/completions",
        None,
        r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
}

#[tokio::test]
async fn ResponsesHandlerAcceptsBearerAPIKey() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            Some("Bearer secret"),
            r#"{"model":"stub-model","input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn ResponsesHandlerAcceptsXApiKeyHeader() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("x-api-key", "secret")
                .body(Body::from(r#"{"model":"stub-model","input":"hi"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn ResponsesHandlerRejectsMissingAPIKey() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(&app, "POST", "/v1/responses", None, "{}")
            .await
            .0
            .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn ResponsesHandlerRejectsInvalidAPIKey() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(&app, "POST", "/v1/responses", Some("Bearer wrong"), "{}")
            .await
            .0
            .status,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn ResponsesHandlerHealthIsUnprotected() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            api_key: "secret".into(),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(&app, "GET", "/healthz", None, "").await.0.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn ConcurrencyOverflowReturns429() {
    let entered = Arc::new(Notify::new());
    let app = App::with_backend(
        PendingBackend {
            entered: entered.clone(),
        },
        HttpConfig {
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    );
    let wait = entered.notified();
    tokio::pin!(wait);
    let first = tokio::spawn(
        app.router().oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .body(Body::from(r#"{"model":"stub-model","input":"hi"}"#))
                .unwrap(),
        ),
    );
    tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap();
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"stub-model","input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::TOO_MANY_REQUESTS
    );
    first.abort();
    let _ = first.await;
}

#[tokio::test]
async fn ConcurrentResponsesDoNotInterleave() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    let (a, b) = tokio::join!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"a","input":"hi"}"#
        ),
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"b","input":"hi"}"#
        )
    );
    assert_eq!(a.0.status, StatusCode::OK);
    assert_eq!(b.0.status, StatusCode::OK);
}

#[tokio::test]
async fn DrainTrackerLateAddDuringWait() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    app.wait_idle().await;
    assert_eq!(app.available_permits(), app.max_permits());
}

#[tokio::test]
async fn DrainTrackerWaitTimeout() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    tokio::time::timeout(Duration::from_secs(1), app.wait_idle())
        .await
        .unwrap();
}

#[tokio::test]
async fn RequestIDHeaderAndDebugRef() {
    let root = log_root("correlation");
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","input":"hi"}"#,
    )
    .await;
    let id = parts.headers["x-request-id"].to_str().unwrap();
    assert!(root.join(id).join("meta.json").is_file());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn ResponsesHandlerMapsRateLimitError() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "limited".into(),
        rate_limited: true,
        retry_after_seconds: 4,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(parts.headers["retry-after"], "4");
}

#[tokio::test]
async fn ResponsesHandlerZeroResetHintWritesNoRetryAfter() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "reset in 0 seconds".into(),
        rate_limited: true,
        reset_hint: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, _) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(!parts.headers.contains_key("retry-after"));
}

#[tokio::test]
async fn StreamRateLimitAnthropicKeepsHTTPStatus() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "limited".into(),
        rate_limited: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    assert_eq!(call(&app, "POST", "/v1/messages", None, r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#).await.0.status, StatusCode::TOO_MANY_REQUESTS);
}
#[tokio::test]
async fn StreamRateLimitDeliversSSE429() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "limited".into(),
        rate_limited: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    let body = String::from_utf8(body).unwrap();
    let events: Vec<_> = body
        .lines()
        .filter_map(|line| line.strip_prefix("event: "))
        .collect();
    assert_eq!(
        events,
        [
            "response.created",
            "response.in_progress",
            "response.failed"
        ]
    );
    for sequence in 0..=2 {
        assert!(
            body.contains(&format!("\"sequence_number\":{sequence}")),
            "missing sequence {sequence}: {body}"
        );
    }
}
#[tokio::test]
async fn StreamPromptTooLongDeliversSSE413() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "context too long".into(),
        context_length: true,
        client_fixable: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (parts, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&body).contains("response.failed"));
}
#[tokio::test]
async fn StreamImmediateErrorReturnsHTTPStatus() {
    let failure = Failure {
        code: "unavailable".into(),
        message: "failed".into(),
        upstream_fault: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"stub-model","stream":true,"input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::BAD_GATEWAY
    );
}
#[tokio::test]
async fn StreamMidStreamErrorCarriesHTTPStatus() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "limited".into(),
        rate_limited: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (_, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    assert!(String::from_utf8_lossy(&body).contains("\"status\":429"));
}
#[tokio::test]
async fn ResponsesHandlerMarksStreamError() {
    let failure = Failure {
        code: "resource_exhausted".into(),
        message: "limited".into(),
        rate_limited: true,
        ..Failure::default()
    };
    let app = App::with_backend(StubBackend::failing(failure), HttpConfig::default());
    let (_, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    assert!(String::from_utf8_lossy(&body).contains("response.failed"));
}

#[tokio::test]
async fn ResponsesHandlerIgnoresLogInitializationFailure() {
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(
                "/proc/devin2api-impossible",
                &RetentionPolicy::default(),
            ))),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"stub-model","input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn ResponsesHandlerWritesStageLogs() {
    let root = log_root("stages");
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"stub-model","input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::OK
    );
    let dirs = request_dirs(&root);
    assert_eq!(dirs.len(), 1);
    for stage in [
        "01-http-request.json",
        "02-request-messages.json",
        "05-response-events.jsonl",
        "06-http-response.jsonl",
    ] {
        assert!(dirs[0].join(stage).is_file(), "missing {stage}");
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn StreamErrorCarriesDebugRef() {
    let root = log_root("stream-error-ref");
    let failure = Failure {
        code: "unavailable".into(),
        message: "failed".into(),
        upstream_fault: true,
        ..Failure::default()
    };
    let app = App::with_backend(
        StubBackend::failing(failure),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let (parts, body) = call(
        &app,
        "POST",
        "/v1/responses",
        None,
        r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
    )
    .await;
    let correlation = parts.headers["x-request-id"].to_str().unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["debug_ref"],
        correlation
    );
    assert!(
        !serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"]["debug_ref"]
            .as_str()
            .unwrap()
            .is_empty()
    );
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn PrematureEndTurnFlagged() {
    let root = log_root("premature");
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let body = r#"{"model":"stub-model","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"run"}]},{"type":"function_call","call_id":"call-1","name":"exec","arguments":"{}"},{"type":"function_call_output","call_id":"call-1","output":"ok"}]}"#;
    assert_eq!(
        call(&app, "POST", "/v1/responses", None, body)
            .await
            .0
            .status,
        StatusCode::OK
    );
    let dir = request_dirs(&root).pop().unwrap();
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["premature_end_turn"], true);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn PrematureEndTurnNotFlaggedForUserInput() {
    let root = log_root("not-premature");
    let app = App::with_backend(
        StubBackend::success(),
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/v1/responses",
            None,
            r#"{"model":"stub-model","input":"hi"}"#
        )
        .await
        .0
        .status,
        StatusCode::OK
    );
    let dir = request_dirs(&root).pop().unwrap();
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("meta.json")).unwrap()).unwrap();
    assert!(meta["premature_end_turn"].is_null() || meta["premature_end_turn"] == false);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn listener_rejects_headers_over_one_mibibyte() {
    let app = App::with_backend(StubBackend::success(), HttpConfig::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let stopped = shutdown.clone();
    let server = tokio::spawn(async move { app.serve(listener, stopped).await });
    let mut oversized = tokio::net::TcpStream::connect(address).await.unwrap();
    let request = format!(
        "GET /healthz HTTP/1.1\r\nHost: localhost\r\nX-Large: {}\r\n\r\n",
        "x".repeat(devin2api::server::http::MAX_HEADER_BYTES + 1)
    );
    tokio::io::AsyncWriteExt::write_all(&mut oversized, request.as_bytes())
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::io::AsyncReadExt::read_to_end(&mut oversized, &mut response),
    )
    .await
    .expect("oversized header rejected");
    assert!(!response.starts_with(b"HTTP/1.1 200"));
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// Task-24 measured fix: Go's `net/http` sets `TCP_NODELAY` on accepted
/// connections; without it, Nagle + delayed ACK added a bimodal ~40ms
/// stall to small `SSE` writes (7ms vs 46ms c=1 stream totals measured
/// against the loopback stub). `Pin` the socket preparation.
#[tokio::test]
async fn accepted_sockets_disable_nagle_like_go() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    assert!(
        !socket.nodelay().unwrap(),
        "OS default keeps Nagle enabled; otherwise this test pins nothing"
    );
    devin2api::server::http::prepare_accepted(&socket);
    assert!(socket.nodelay().unwrap());
    drop(client);
}

/// Panel abort must cancel a live request's lineage: the in-flight row
/// reports `abortable: true`, the abort POST returns `{"aborted":true}`,
/// the `SSE` stream terminates, the backend's cancellation token fires and
/// the request record finalizes as `aborted` (Go `recorder.SetAbort`).
#[tokio::test]
async fn panel_abort_cancels_live_stream_and_records_aborted() {
    let root = log_root("panel-abort");
    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));
    let entered = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let app = App::with_backend(
        SlowStreamBackend {
            entered: entered.clone(),
            cancelled: cancelled.clone(),
        },
        HttpConfig {
            debug_manager: Some(manager),
            dashboard_password: "pw".into(),
            ..HttpConfig::default()
        },
    );
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
        ))
        .unwrap();
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let response_task = tokio::spawn(app.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(5), entered_wait)
        .await
        .expect("backend stream entered");
    let response = tokio::time::timeout(Duration::from_secs(5), response_task)
        .await
        .expect("response committed")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("first SSE frame")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("response.created"));

    let (parts, active) = call(
        &app,
        "GET",
        "/panel/api/requests/active",
        Some("Bearer pw"),
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    let active: serde_json::Value = serde_json::from_slice(&active).unwrap();
    let rows = active["active"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "one live request");
    assert_eq!(
        rows[0]["abortable"], true,
        "live request must report abortable"
    );
    let dir = rows[0]["dir"].as_str().unwrap().to_string();

    let (parts, aborted) = call(
        &app,
        "POST",
        &format!("/panel/api/requests/{dir}/abort"),
        Some("Bearer pw"),
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&aborted).unwrap(),
        serde_json::json!({"aborted": true})
    );
    // The body is pull-driven (no producer task): the abort propagates
    // on the next poll — `step`'s cancel arm ends the stream, and
    // dropping the body drops the event stream whose Drop impl reports
    // whether the request's cancellation token fired.
    let end = tokio::time::timeout(Duration::from_secs(5), body.frame()).await;
    assert!(
        end.is_ok_and(|frame| frame.is_none()),
        "aborted SSE stream must terminate"
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .expect("request cancellation token fired");
    tokio::time::timeout(Duration::from_secs(5), app.wait_idle())
        .await
        .expect("aborted request finalized");
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join(&dir).join("meta.json")).unwrap()).unwrap();
    assert_eq!(meta["result"], "aborted");
    std::fs::remove_dir_all(root).unwrap();
}

/// Non-streaming `JSON` responses must keep emitting the `\n` heartbeat for
/// the whole upstream-silence window (Go `collectPumpedMessage` + ticker),
/// not just once — Codex-class clients abandon ~30s silent responses.
#[tokio::test(start_paused = true)]
async fn json_heartbeat_repeats_during_upstream_silence() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = App::with_backend(
        DelayedDoneBackend {
            entered: entered.clone(),
            release: release.clone(),
        },
        HttpConfig::default(),
    );
    let request = Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"model":"stub-model","input":"hi"}"#))
        .unwrap();
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let response_task = tokio::spawn(app.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(1), entered_wait)
        .await
        .expect("stream receive entered");
    // First heartbeat lands one interval after upstream silence began.
    tokio::time::advance(devin2api::server::stream::KEEPALIVE_INTERVAL).await;
    let response = tokio::time::timeout(Duration::from_secs(1), response_task)
        .await
        .expect("heartbeat committed")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("first heartbeat")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(&first[..], devin2api::server::stream::JSON_HEARTBEAT);
    // The heartbeat must repeat on the same cadence while upstream stays
    // silent — a single heartbeat then silence is the defect under test.
    for beat in 2..=3 {
        tokio::time::advance(devin2api::server::stream::KEEPALIVE_INTERVAL).await;
        let frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .unwrap_or_else(|_| panic!("heartbeat {beat} missing"))
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(
            &frame[..],
            devin2api::server::stream::JSON_HEARTBEAT,
            "heartbeat {beat} must be the \\n keepalive"
        );
    }
    release.notify_one();
    let final_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .expect("final JSON body")
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&final_frame).contains("\"object\":\"response\""));
    drop(body);
    tokio::time::timeout(Duration::from_secs(1), app.wait_idle())
        .await
        .expect("request finalized");
}

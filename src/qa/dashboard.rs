//! Process-surface QA for task 17's dashboard APIs.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use axum::body::{Body, to_bytes};
use http::Request;
use http_body_util::BodyExt as _;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

use crate::dashboard::{Config, ConfigReloadReport, Dashboard};
use crate::debuglog::{
    Completion, LogValue, Manager, Recorder, RequestMeta, RetentionPolicy, STAGE_HTTP_RESPONSE,
};
use crate::domain::{
    AssistantMessage, Failure, RequestMessages, ResponseEvent, ResponseEventType, StopReason,
};
use crate::metrics::Metrics;
use crate::server::http::{App, HttpBackend, HttpConfig, HttpEventStream};
use crate::upstream::catalog::ModelInfo;

#[derive(Serialize)]
struct RouteResult {
    method: String,
    path: String,
    status: u16,
}

async fn request(
    dashboard: &Dashboard,
    method: &str,
    path: &str,
    auth: bool,
    body: &str,
) -> anyhow::Result<(u16, http::HeaderMap, Vec<u8>)> {
    let mut builder = Request::builder().method(method).uri(path).header(
        "content-type",
        if path == "/panel/login" {
            "application/x-www-form-urlencoded"
        } else {
            "application/json"
        },
    );
    if auth {
        builder = builder.header("authorization", "Bearer pw");
    }
    let response = dashboard
        .router()
        .oneshot(builder.body(Body::from(body.to_string()))?)
        .await
        .context("dashboard request")?;
    let (parts, body) = response.into_parts();
    Ok((
        parts.status.as_u16(),
        parts.headers,
        to_bytes(body, 8 * 1024 * 1024).await?.to_vec(),
    ))
}

pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    match case {
        None | Some("happy") => happy(evidence).await,
        Some("live-abort") => live_abort(evidence).await,
        Some("auth-path-and-truncation") => failure(evidence).await,
        Some(other) => anyhow::bail!("unknown dashboard-api case {other}"),
    }
}

fn fixture(evidence: &Path) -> anyhow::Result<(Dashboard, Arc<Manager>, String)> {
    let logs = evidence.join("dashboard-state");
    let _ = std::fs::remove_dir_all(&logs);
    std::fs::create_dir_all(&logs)?;
    std::fs::write(logs.join("stderr.log"), b"dashboard qa process log\n")?;
    std::fs::write(
        logs.join("quota.jsonl"),
        b"{\"at\":1700000000,\"daily_remaining\":90}\n{\"at\":1700003600,\"daily_remaining\":80}\n",
    )?;
    let manager = Arc::new(Manager::new(&logs, &RetentionPolicy::default()));
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        api: "openai-responses".into(),
        client_request_id: "qa".into(),
        ..RequestMeta::default()
    });
    recorder.set_model("stub-model");
    recorder.write_json(
        "03-devin-request.json",
        LogValue::serde(json!({"model":"stub-model","token":"qa-upstream-secret"})),
    );
    recorder.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.output_text.delta",
        LogValue::serde(json!({"type":"response.output_text.delta","delta":"pong"})),
    );
    let dir = recorder.dir_name();
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "stub-model".into(),
        ..Completion::default()
    });
    let dashboard = Dashboard::new(Config {
        password: "pw".into(),
        version: "qa-dashboard".into(),
        token: Arc::new(|| "qa-upstream-secret".into()),
        metrics: Some(Arc::new(Metrics::new())),
        debug_manager: Some(manager.clone()),
        config_current: Some(Arc::new(
            || json!({"config":{"devin":{"token":"qa-upstream-secret","proxy":"http://user:pass@proxy.test"},"dashboard":{"password":"pw"},"auth":{"api_key":"qa-api-secret"}},"stale":false}),
        )),
        config_reload: Some(Arc::new(|| {
            Ok(ConfigReloadReport {
                at: "2026-09-20T00:00:00Z".into(),
                applied: vec!["debug.enabled".into()],
                requires_restart: vec![],
            })
        })),
        ..Config::default()
    });
    Ok((dashboard, manager, dir))
}

async fn happy(evidence: &Path) -> anyhow::Result<i32> {
    let (dashboard, manager, dir) = fixture(evidence)?;
    let routes = [
        ("GET", "/panel", false, ""),
        ("POST", "/panel/login", false, "password=pw"),
        ("GET", "/panel/static/panel.css", false, ""),
        ("GET", "/panel/api", true, ""),
        ("GET", "/panel/api/status", true, ""),
        ("GET", "/panel/api/models", true, ""),
        ("GET", "/panel/api/stats", true, ""),
        ("GET", "/panel/api/requests", true, ""),
        ("GET", "/panel/api/requests/matrix", true, ""),
        ("GET", "/panel/api/requests/export?format=csv", true, ""),
        ("GET", "/panel/api/requests/active", true, ""),
        ("GET", "DETAIL", true, ""),
        ("GET", "MERGED", true, ""),
        ("GET", "FILE", true, ""),
        ("POST", "ABORT", true, ""),
        ("GET", "/panel/api/logs", true, ""),
        ("GET", "/panel/api/quota", true, ""),
        ("GET", "/panel/api/usage", true, ""),
        (
            "POST",
            "/panel/api/debug/toggle",
            true,
            "{\"enabled\":true}",
        ),
        ("GET", "/panel/api/config", true, ""),
        ("POST", "/panel/api/config/reload", true, ""),
    ];
    let mut results = Vec::new();
    for (method, template, auth, body) in routes {
        let path = match template {
            "DETAIL" => format!("/panel/api/requests/{dir}"),
            "MERGED" => format!("/panel/api/requests/{dir}/merged"),
            "FILE" => format!("/panel/api/requests/{dir}/file/03-devin-request.json"),
            "ABORT" => format!("/panel/api/requests/{dir}/abort"),
            other => other.to_string(),
        };
        let (status, _, _) = request(&dashboard, method, &path, auth, body).await?;
        results.push(RouteResult {
            method: method.into(),
            path,
            status,
        });
    }
    // Static content belongs to task 18 and a completed request is no longer abortable.
    let routes_passed = results.iter().all(|r| {
        r.status == 200
            || (r.path.contains("/static/") && r.status == 404)
            || (r.path.ends_with("/abort") && r.status == 404)
    });
    let state = manager.root().to_path_buf();
    manager.close();
    std::fs::remove_dir_all(state)?;
    // The completed-request abort above only proves the 404 path; the live
    // case exercises the real wiring (abortable flag, cancel, termination).
    let live = live_abort(&evidence.join("live-abort")).await?;
    let passed = routes_passed && live == 0;
    std::fs::write(
        evidence.join("routes.json"),
        serde_json::to_vec_pretty(
            &json!({"case":"happy","passed":passed,"routes":results,"live_abort_exit":live}),
        )?,
    )?;
    Ok(i32::from(!passed))
}

/// A stream that opens (one `Start` event) then produces nothing until the
/// request's cancellation token fires — the live-request shape the panel
/// abort must interrupt.
struct SlowBackend {
    entered: Arc<Notify>,
    cancelled: Arc<Notify>,
}

struct SlowStream {
    cancel: CancellationToken,
    cancelled: Arc<Notify>,
    sent_start: bool,
}

impl Drop for SlowStream {
    /// Teardown runs exactly once; reporting whether the request's
    /// cancellation token fired proves the abort reached the backend seam
    /// (a dropped `recv` future never gets to notify).
    fn drop(&mut self) {
        if self.cancel.is_cancelled() {
            self.cancelled.notify_one();
        }
    }
}

impl HttpEventStream for SlowStream {
    fn recv(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ResponseEvent>, Failure>> + Send + '_>> {
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

impl HttpBackend for SlowBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ModelInfo>, Failure>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        _request: RequestMessages,
        cancel: CancellationToken,
        _recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn HttpEventStream>, Failure>> + Send + '_>> {
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

/// Live-request abort: the in-flight row must report `abortable: true`,
/// the abort POST must return `{"aborted":true}`, the SSE stream must
/// terminate, the request's cancellation token must fire and the record
/// must finalize as `aborted` (Go `recorder.SetAbort(cancel)`).
// One end-to-end abort scenario; splitting would scatter the timeline.
#[allow(clippy::too_many_lines)]
async fn live_abort(evidence: &Path) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let logs = evidence.join("state");
    let _ = std::fs::remove_dir_all(&logs);
    std::fs::create_dir_all(&logs)?;
    let manager = Arc::new(Manager::new(&logs, &RetentionPolicy::default()));
    let entered = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let app = App::with_backend(
        SlowBackend {
            entered: entered.clone(),
            cancelled: cancelled.clone(),
        },
        HttpConfig {
            debug_manager: Some(manager.clone()),
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
        ))?;
    let entered_wait = entered.notified();
    tokio::pin!(entered_wait);
    let response_task = tokio::spawn(app.router().oneshot(request));
    tokio::time::timeout(Duration::from_secs(5), entered_wait)
        .await
        .context("backend stream entered")?;
    let response = tokio::time::timeout(Duration::from_secs(5), response_task)
        .await
        .context("response committed")?
        .context("response task panicked")?
        .expect("oneshot is infallible");
    let committed = response.status().as_u16();
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .context("first SSE frame timeout")?
        .context("stream ended before first frame")??;
    let first_text = first
        .into_data()
        .map(|data| String::from_utf8_lossy(&data).into_owned())
        .unwrap_or_default();

    let active_response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api/requests/active")
                .header("authorization", "Bearer pw")
                .body(Body::empty())?,
        )
        .await?;
    let active: Value =
        serde_json::from_slice(&to_bytes(active_response.into_body(), 8 * 1024 * 1024).await?)?;
    let rows = active["active"].as_array().cloned().unwrap_or_default();
    let abortable = rows
        .first()
        .and_then(|row| row["abortable"].as_bool())
        .unwrap_or(false);
    let dir = rows
        .first()
        .and_then(|row| row["dir"].as_str())
        .unwrap_or_default()
        .to_string();

    let abort_response = app
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/panel/api/requests/{dir}/abort"))
                .header("authorization", "Bearer pw")
                .body(Body::empty())?,
        )
        .await?;
    let abort_status = abort_response.status().as_u16();
    let abort_body: Value =
        serde_json::from_slice(&to_bytes(abort_response.into_body(), 8 * 1024 * 1024).await?)?;
    // The body is pull-driven (no producer task): the abort propagates
    // on the next poll — `step`'s cancel arm ends the stream, and
    // dropping the body drops the event stream whose Drop impl reports
    // whether the request's cancellation token fired.
    let stream_ended = tokio::time::timeout(Duration::from_secs(5), body.frame())
        .await
        .is_ok_and(|frame| frame.is_none());
    drop(body);
    let token_fired = tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
        .await
        .is_ok();
    tokio::time::timeout(Duration::from_secs(5), app.wait_idle())
        .await
        .context("aborted request finalized")?;
    let meta: Value = serde_json::from_slice(
        &std::fs::read(logs.join(&dir).join("meta.json")).context("meta.json")?,
    )?;
    let result = meta["result"].as_str().unwrap_or_default().to_string();

    let passed = committed == 200
        && first_text.contains("response.created")
        && rows.len() == 1
        && abortable
        && abort_status == 200
        && abort_body["aborted"] == true
        && token_fired
        && stream_ended
        && result == "aborted";
    std::fs::write(
        evidence.join("live-abort.json"),
        serde_json::to_vec_pretty(&json!({
            "case": "live-abort",
            "passed": passed,
            "committed_status": committed,
            "first_frame": first_text,
            "active_rows": rows.len(),
            "abortable": abortable,
            "abort_status": abort_status,
            "abort_body": abort_body,
            "cancel_token_fired": token_fired,
            "stream_ended": stream_ended,
            "meta_result": result,
        }))?,
    )?;
    manager.close();
    std::fs::remove_dir_all(&logs)?;
    Ok(i32::from(!passed))
}

async fn failure(evidence: &Path) -> anyhow::Result<i32> {
    let (dashboard, manager, dir) = fixture(evidence)?;
    let unauthorized = request(&dashboard, "GET", "/panel/api/requests", false, "")
        .await?
        .0;
    let unauthorized_abort = request(
        &dashboard,
        "POST",
        &format!("/panel/api/requests/{dir}/abort"),
        false,
        "",
    )
    .await?
    .0;
    let mut lock_status = 0;
    for _ in 0..6 {
        lock_status = request(&dashboard, "GET", "/panel/api", false, "").await?.0;
    }
    // The unauthenticated requests carry no wrong Bearer and therefore do not count.
    for _ in 0..6 {
        lock_status = request_wrong_bearer(&dashboard).await?;
    }
    let traversal = request(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/file/../../config.yaml"),
        true,
        "",
    )
    .await?
    .0;
    let oversized = manager.root().join(&dir).join("oversized.json");
    std::fs::write(&oversized, vec![b'x'; (4 << 20) + 1])?;
    let (_, _, body) = request(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/file/oversized.json"),
        true,
        "",
    )
    .await?;
    let truncated = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v["truncated"].as_bool())
        .unwrap_or(false);
    let (_, _, config) = request(&dashboard, "GET", "/panel/api/config", true, "").await?;
    let config_text = String::from_utf8_lossy(&config);
    let redacted = !config_text.contains("qa-upstream-secret")
        && !config_text.contains("qa-api-secret")
        && !config_text.contains("user:pass")
        && !config_text.contains("\"password\":\"pw\"")
        && config_text.contains("sha256:")
        && config_text.contains("http://proxy.test");
    let passed = unauthorized == 401
        && unauthorized_abort == 401
        && lock_status == 429
        && traversal == 404
        && truncated
        && redacted;
    std::fs::write(
        evidence.join("auth-path-and-truncation.json"),
        serde_json::to_vec_pretty(
            &json!({"case":"auth-path-and-truncation","passed":passed,"unauthorized_read":unauthorized,"unauthorized_abort":unauthorized_abort,"lockout":lock_status,"traversal":traversal,"truncated":truncated,"redacted":redacted}),
        )?,
    )?;
    let state = manager.root().to_path_buf();
    manager.close();
    std::fs::remove_dir_all(state)?;
    Ok(i32::from(!passed))
}

async fn request_wrong_bearer(dashboard: &Dashboard) -> anyhow::Result<u16> {
    let response = dashboard
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api")
                .header("authorization", "Bearer wrong")
                .body(Body::empty())?,
        )
        .await
        .context("wrong bearer request")?;
    Ok(response.status().as_u16())
}

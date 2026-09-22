use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use devin2api::config;
use devin2api::debuglog::{Manager, RetentionPolicy};
use devin2api::domain::{
    AssistantMessage, Content, Failure, RequestMessages, ResponseEvent, ResponseEventType,
    StopReason, TextContent,
};
use devin2api::server::http::{BoxFuture, HttpBackend, HttpConfig, HttpEventStream};
use devin2api::server::lifecycle::{self, RuntimeConfig, listen_url};
use devin2api::upstream::catalog::{Adapter, AdapterConfig, ModelInfo};
use http::{Request, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn temp(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "devin2api-lifecycle-{name}-{}",
        devin2api::randid::hex(8)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn yaml(api_key: &str, model: &str, listen: &str) -> String {
    format!(
        "server:\n  listen: '{listen}'\n  max_concurrency: 1\ndevin:\n  base_url: 'http://127.0.0.1:9'\n  token: token\n  model: '{model}'\ndebug:\n  enabled: false\ndashboard:\n  password: old\nauth:\n  api_key: '{api_key}'\n"
    )
}

struct Backend;
impl HttpBackend for Backend {
    fn list_models(&self, _: CancellationToken) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn stream(
        &self,
        _: RequestMessages,
        _: CancellationToken,
        _: devin2api::debuglog::Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        unreachable!()
    }
}

#[test]
fn version_resolution_precedence_and_dirty_short_revision() {
    assert_eq!(
        lifecycle::resolve_version(Some("v2"), Some("v1"), Some("abcdef"), false, "v0"),
        "v2"
    );
    assert_eq!(
        lifecycle::resolve_version(Some("dev"), Some("v1"), Some("abcdef"), false, "v0"),
        "v1"
    );
    assert_eq!(
        lifecycle::resolve_version(None, None, Some("0123456789abcdef"), true, "v0"),
        "dev-0123456789ab-dirty"
    );
    assert_eq!(
        lifecycle::resolve_version(None, None, None, false, " v0.11.0\n"),
        "v0.11.0"
    );
    assert_eq!(
        lifecycle::resolve_version(None, None, None, false, " \n"),
        "dev"
    );
    assert_ne!(lifecycle::BUILD_VERSION, env!("CARGO_PKG_VERSION"));
}

#[test]
fn bind_collision_is_recorded_and_recovery_is_idempotent() {
    let root = temp("bind");
    let one = lifecycle::record_bind_failure(&root, "127.0.0.1:1", "pid=1").unwrap();
    let two = lifecycle::record_bind_failure(&root, "127.0.0.1:1", "pid=2").unwrap();
    assert_eq!(one.count, 1);
    assert_eq!(two.count, 2);
    assert!(lifecycle::mark_bind_recovered(&root).unwrap().is_some());
    assert!(lifecycle::mark_bind_recovered(&root).unwrap().is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn listen_url_matches_go() {
    for (listen, expected) in [
        (":8080", ":8080 (http://localhost:8080)"),
        ("0.0.0.0:8080", "0.0.0.0:8080 (http://localhost:8080)"),
        ("127.0.0.1:9090", "http://127.0.0.1:9090"),
        ("[::]:8080", "[::]:8080 (http://localhost:8080)"),
        ("invalid", "invalid"),
    ] {
        assert_eq!(listen_url(listen), expected);
    }
}

#[tokio::test]
async fn reload_is_validate_then_commit_and_updates_auth() {
    let root = temp("reload");
    let config_path = root.join("config.yaml");
    std::fs::write(&config_path, yaml("old-key", "old-model", "127.0.0.1:1")).unwrap();
    let initial = config::load(config_path.to_str().unwrap()).unwrap();
    let adapter = Adapter::new(AdapterConfig::from_devin(&initial.devin, None, None)).unwrap();
    let manager = Arc::new(Manager::new(root.join("logs"), &RetentionPolicy::default()));
    let runtime = Arc::new(RuntimeConfig::new(
        config_path.clone(),
        root.join("logs"),
        initial.clone(),
    ));
    let app = devin2api::server::http::App::with_backend(
        Backend,
        HttpConfig::from_server(&initial.server, initial.auth.api_key.clone()),
    );
    runtime.attach(adapter, app.clone(), manager);

    std::fs::write(&config_path, yaml("bad-key", "", "127.0.0.1:2")).unwrap();
    assert!(runtime.reload().is_err());
    assert_eq!(runtime.config().auth.api_key, "old-key");

    std::fs::write(&config_path, yaml("new-key", "new-model", "127.0.0.1:2")).unwrap();
    let report = runtime.reload().unwrap();
    assert!(report.applied.contains(&"auth.api_key".to_string()));
    assert!(report.applied.contains(&"devin.model".to_string()));
    assert!(
        report
            .requires_restart
            .contains(&"server.listen".to_string())
    );

    let request = Request::builder()
        .uri("/v1/models")
        .header("authorization", "Bearer old-key")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.router().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let request = Request::builder()
        .uri("/v1/models")
        .header("authorization", "Bearer new-key")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.router().oneshot(request).await.unwrap().status(),
        StatusCode::OK
    );
    std::fs::remove_dir_all(root).unwrap();
}

struct PendingBackend(Arc<Notify>);
struct PendingStream;
impl HttpEventStream for PendingStream {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        Box::pin(std::future::pending())
    }
}
impl HttpBackend for PendingBackend {
    fn list_models(&self, _: CancellationToken) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn stream(
        &self,
        _: RequestMessages,
        _: CancellationToken,
        _: devin2api::debuglog::Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        self.0.notify_one();
        Box::pin(async { Ok(Box::new(PendingStream) as Box<dyn HttpEventStream>) })
    }
}

/// Backend whose stream parks until `release` fires, then emits one Done
/// event — the deterministic in-flight request for drain tests.
struct GateBackend {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
struct GateStream {
    release: Arc<Notify>,
    done: bool,
}
impl HttpEventStream for GateStream {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let release = self.release.clone();
        let done = self.done;
        self.done = true;
        Box::pin(async move {
            if done {
                return Ok(None);
            }
            release.notified().await;
            Ok(Some(ResponseEvent {
                kind: ResponseEventType::Done,
                reason: Some(StopReason::Stop),
                message: Some(AssistantMessage {
                    content: vec![Content::Text(TextContent {
                        text: "pong".to_string(),
                    })],
                    stop_reason: Some(StopReason::Stop),
                    ..AssistantMessage::default()
                }),
                ..ResponseEvent::default()
            }))
        })
    }
}
impl HttpBackend for GateBackend {
    fn list_models(&self, _: CancellationToken) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(vec![]) })
    }
    fn stream(
        &self,
        _: RequestMessages,
        _: CancellationToken,
        _: devin2api::debuglog::Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        self.entered.notify_one();
        let release = self.release.clone();
        Box::pin(async move {
            Ok(Box::new(GateStream {
                release,
                done: false,
            }) as Box<dyn HttpEventStream>)
        })
    }
}

const CHAT_STREAM_BODY: &str =
    r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}"#;

async fn raw_post(addr: std::net::SocketAddr, body: &str) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/chat/completions HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    stream
}

/// Read until `EOF` or the bound; returns whatever arrived.
async fn read_bounded(stream: &mut tokio::net::TcpStream, bound: Duration) -> Vec<u8> {
    let mut data = Vec::new();
    let _ = tokio::time::timeout(bound, stream.read_to_end(&mut data)).await;
    data
}

#[tokio::test]
async fn drain_completes_inflight_and_refuses_new() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = devin2api::server::http::App::with_backend(
        GateBackend {
            entered: entered.clone(),
            release: release.clone(),
        },
        HttpConfig {
            max_concurrency: 4,
            ..HttpConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let drain_app = app.clone();
    let drain = tokio::spawn(lifecycle::serve_until_shutdown(
        drain_app,
        listener,
        shutdown.clone(),
        false,
        Duration::from_secs(600),
    ));

    // In-flight stream parked on the gate.
    let mut inflight = raw_post(addr, CHAT_STREAM_BODY).await;
    entered.notified().await;

    shutdown.cancel();
    // Wait on the exact state transition (the drain flag), not a delay.
    while !app.is_draining() {
        tokio::task::yield_now().await;
    }
    let mut probe = raw_post(addr, CHAT_STREAM_BODY).await;
    // The 503 carries `connection: close`, so the body ends at EOF.
    let refused = read_bounded(&mut probe, Duration::from_secs(5)).await;
    let refused = String::from_utf8_lossy(&refused);
    assert!(refused.starts_with("HTTP/1.1 503"), "{refused}");
    assert!(refused.contains("retry-after: 1"), "{refused}");
    assert!(refused.contains("connection: close"), "{refused}");
    assert!(refused.contains("server_draining"), "{refused}");

    // The in-flight request is not dropped: releasing the gate lets it
    // finish normally even though the server is draining.
    release.notify_one();
    let body = read_bounded(&mut inflight, Duration::from_secs(10)).await;
    let body = String::from_utf8_lossy(&body);
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains("[DONE]"), "{body}");

    assert!(!drain.await.unwrap().unwrap(), "drain was not forced");
}

#[tokio::test]
async fn forced_close_aborts_inflight_after_deadline() {
    let entered = Arc::new(Notify::new());
    let app = devin2api::server::http::App::with_backend(
        PendingBackend(entered.clone()),
        HttpConfig {
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let drain = tokio::spawn(lifecycle::serve_until_shutdown(
        app,
        listener,
        shutdown.clone(),
        false,
        Duration::from_millis(150),
    ));
    let mut inflight = raw_post(addr, CHAT_STREAM_BODY).await;
    entered.notified().await;
    shutdown.cancel();
    // The request never finishes; the drain deadline bounds it and the
    // forced close drops the connection (Go server.Close parity).
    let body = read_bounded(&mut inflight, Duration::from_secs(10)).await;
    assert!(
        drain.await.unwrap().unwrap(),
        "drain must report the forced deadline"
    );
    let body = String::from_utf8_lossy(&body);
    // The connection was cut mid-stream: at most a committed 200 header,
    // never a completed stream.
    assert!(!body.contains("[DONE]"), "{body}");
}

#[tokio::test]
async fn probe_existing_instance_reports_holder() {
    async fn canned(response: &'static str) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0_u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        addr
    }

    let healthy = canned(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 74\r\nConnection: close\r\n\r\n{\"version\":\"v1.2.3\",\"uptime_seconds\":42,\"draining\":true,\"pid\":777}",
    )
    .await;
    let holder = lifecycle::probe_existing_instance(&healthy.to_string()).await;
    assert_eq!(
        holder,
        "devin-2api pid=777 version=v1.2.3 uptime=42s draining=true"
    );

    let foreign =
        canned("HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnope").await;
    assert_eq!(
        lifecycle::probe_existing_instance(&foreign.to_string()).await,
        "not devin-2api"
    );

    // Nothing listening: connection refused maps to unresponsive.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = closed.local_addr().unwrap();
    drop(closed);
    assert_eq!(
        lifecycle::probe_existing_instance(&dead.to_string()).await,
        "unresponsive"
    );

    // Wildcard listen addresses probe through loopback (Go parity).
    let wildcard = format!(":{}", healthy.port());
    assert!(
        lifecycle::probe_existing_instance(&wildcard)
            .await
            .starts_with("devin-2api pid=777")
    );
}

#[tokio::test(start_paused = true)]
async fn forced_drain_deadline_uses_virtual_time() {
    let entered = Arc::new(Notify::new());
    let app = devin2api::server::http::App::with_backend(
        PendingBackend(entered.clone()),
        HttpConfig {
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    );
    let request_app = app.clone();
    let request = tokio::spawn(async move {
        let body =
            Body::from(r#"{"model":"m","messages":[{"role":"user","content":"x"}],"stream":true}"#);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(body)
            .unwrap();
        request_app.router().oneshot(request).await
    });
    entered.notified().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let drain = tokio::spawn(lifecycle::serve_until_shutdown(
        app,
        listener,
        shutdown,
        false,
        Duration::from_secs(600),
    ));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(drain.await.unwrap().unwrap());
    request.abort();
}

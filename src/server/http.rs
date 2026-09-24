//! Authenticated HTTP inference and model surface.
//!
//! The router intentionally applies no global body-limit middleware: only
//! inference handlers read a bounded body, so unknown routes retain chi's
//! 404/405 behavior even when a client sends a large body.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, Request, State};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use bytes::Bytes;
use http::header::{AUTHORIZATION, CONNECTION, CONTENT_TYPE, RETRY_AFTER, X_CONTENT_TYPE_OPTIONS};
use http::{HeaderValue, StatusCode};
use http_body::{Body as HttpBody, Frame, SizeHint};
use hyper::rt::{Sleep as HyperSleep, Timer as HyperTimer};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as ConnectionBuilder;
use hyper_util::service::TowerToHyperService;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::config::ServerConfig;
use crate::dashboard::{Config as DashboardConfig, Dashboard, DashboardData};
use crate::debuglog::{
    Completion, LogValue, Manager, Recorder, RequestMeta, STAGE_HTTP_REQUEST,
    STAGE_REQUEST_MESSAGES, projection::request_messages_projection,
};
use crate::domain::{
    AssistantMessage, Failure, RequestMessages, ResponseEvent, ResponseEventType, StopReason,
    classify,
};
use crate::metrics::{Metrics, RejectEvent, RequestMetrics};
use crate::protocol::common::http_status;
use crate::protocol::{chat, messages, responses};
use crate::upstream::catalog::{Adapter, ModelInfo, model_entry};
use crate::upstream::gate::GateStats;

use super::stream::{
    AdapterEventStream, ProtocolKind, encode_http_error, json_response, sse_response,
};

/// Request-header read deadline from the Go server.
pub const READ_HEADER_TIMEOUT: Duration = Duration::from_secs(60);
/// Total request read deadline from the Go server.
pub const READ_TIMEOUT: Duration = Duration::from_secs(120);
/// Keep-alive idle deadline from the Go server.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(360);
/// Go `http.Server.MaxHeaderBytes`.
pub const MAX_HEADER_BYTES: usize = 1 << 20;
/// Inference JSON body limit; image base64 payloads require the 32 MiB cap.
pub const MAX_BODY_BYTES: usize = 32 << 20;
/// Default shared inference-turn capacity.
pub const DEFAULT_MAX_CONCURRENCY: usize = 1024;

/// Boxed future used by the injectable HTTP backend.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Object-safe event stream consumed by the HTTP commit-boundary owner.
pub trait HttpEventStream: Send {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>>;

    /// Non-blocking receive of an already-ready event (Go's
    /// `select/default` burst drain). `None` means "nothing ready right
    /// now", never end-of-stream. Default: no fast path.
    fn try_recv(&mut self) -> Option<ResponseEvent> {
        None
    }
}

/// Adapter seam. Production uses [`Adapter`]; tests and QA use deterministic
/// loopback/stub implementations without weakening the HTTP integration.
pub trait HttpBackend: Send + Sync + 'static {
    fn list_models(
        &self,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>>;

    /// Full upstream status aggregation for the admin panel. Test backends
    /// that do not model Devin operational APIs retain an empty status; the
    /// production [`Adapter`] implementation performs every Go RPC.
    fn dashboard_status(&self) -> BoxFuture<'_, Result<Value, String>> {
        Box::pin(async { Ok(json!({})) })
    }

    /// Live upstream credential reader used by panel masking and Seat calls.
    fn dashboard_token(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        Arc::new(String::new)
    }

    /// Rate-gate snapshot for the panel `gate` stats section (Go
    /// `panel.SetGateStats(devinAdapter.GateStats)`); `None` omits the
    /// section — test backends without a gate keep the default.
    fn gate_stats(&self) -> Option<GateStats> {
        None
    }

    fn stream(
        &self,
        request: RequestMessages,
        cancel: CancellationToken,
        recorder: Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>>;
}

impl HttpBackend for Adapter {
    fn list_models(
        &self,
        cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async move {
            self.list_models(&cancel)
                .await
                .map(|models| (*models).clone())
                .map_err(|err| classify(&err))
        })
    }

    // The status aggregation future is inherently large (many awaited
    // RPCs); it is boxed at the trait boundary like every sibling.
    #[allow(clippy::large_futures)]
    fn dashboard_status(&self) -> BoxFuture<'_, Result<Value, String>> {
        Box::pin(async move { Ok(crate::dashboard::adapter_status(self).await) })
    }

    fn dashboard_token(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        self.token_func()
    }

    fn gate_stats(&self) -> Option<GateStats> {
        Some(Adapter::gate_stats(self))
    }

    // The stream future carries the full upstream retry state; it is
    // boxed at the trait boundary like every sibling.
    #[allow(clippy::large_futures)]
    fn stream(
        &self,
        request: RequestMessages,
        cancel: CancellationToken,
        recorder: Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        Box::pin(async move {
            Adapter::stream(self, request, cancel, recorder)
                .await
                .map(|stream| Box::new(AdapterEventStream(stream)) as Box<dyn HttpEventStream>)
        })
    }
}

/// HTTP assembly options.
#[derive(Clone)]
pub struct HttpConfig {
    pub api_key: String,
    pub max_concurrency: usize,
    pub version: String,
    pub debug_manager: Option<Arc<Manager>>,
    pub dashboard_password: String,
    pub dashboard_config_current: Option<Arc<dyn Fn() -> Value + Send + Sync>>,
    pub dashboard_config_reload:
        Option<Arc<dyn Fn() -> Result<crate::dashboard::ConfigReloadReport, String> + Send + Sync>>,
    pub quota_interval: Duration,
    /// Optional exact-state probe used by framed transport tests.
    #[doc(hidden)]
    pub websocket_probe: Option<Arc<super::websocket::WebSocketProbe>>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            version: String::new(),
            debug_manager: None,
            dashboard_password: String::new(),
            dashboard_config_current: None,
            dashboard_config_reload: None,
            quota_interval: Duration::ZERO,
            websocket_probe: None,
        }
    }
}

impl HttpConfig {
    /// Construct the HTTP projection from the repository config types.
    pub fn from_server(server: &ServerConfig, api_key: String) -> Self {
        Self {
            api_key,
            max_concurrency: usize::try_from(server.max_concurrency)
                .ok()
                .filter(|limit| *limit > 0)
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            ..Self::default()
        }
    }

    /// Construct the complete production HTTP and panel projection.
    pub fn from_config(
        config: &crate::config::Config,
        version: String,
        debug_manager: Option<Arc<Manager>>,
        current: Arc<dyn Fn() -> Value + Send + Sync>,
        reload: Arc<dyn Fn() -> Result<crate::dashboard::ConfigReloadReport, String> + Send + Sync>,
    ) -> Self {
        Self {
            api_key: config.auth.api_key.clone(),
            max_concurrency: usize::try_from(config.server.max_concurrency)
                .ok()
                .filter(|limit| *limit > 0)
                .unwrap_or(DEFAULT_MAX_CONCURRENCY),
            version,
            debug_manager,
            dashboard_password: config.dashboard.password.clone(),
            dashboard_config_current: Some(current),
            dashboard_config_reload: Some(reload),
            quota_interval: config
                .debug
                .quota_interval_minutes
                .filter(|minutes| *minutes > 0)
                .and_then(|minutes| u64::try_from(minutes).ok())
                .map_or(Duration::ZERO, |minutes| Duration::from_secs(minutes * 60)),
            websocket_probe: None,
        }
    }
}

pub use crate::metrics::RejectReason;

#[derive(Clone, Default)]
pub struct RejectSnapshot(BTreeMap<RejectReason, u64>);

impl RejectSnapshot {
    #[must_use]
    pub fn count(&self, reason: RejectReason) -> u64 {
        self.0.get(&reason).copied().unwrap_or(0)
    }
}

pub(crate) struct Inner {
    backend: Arc<dyn HttpBackend>,
    api_key: RwLock<String>,
    permits: Arc<Semaphore>,
    max_permits: usize,
    draining: AtomicBool,
    /// `drainTracker` — counts every admitted turn (including reject
    /// responses still being written) and wakes drain waiters at zero.
    /// Zero-crossing and wakeup are one critical section, so admits during
    /// drain are safe (Go's comment on why this is not a `WaitGroup`).
    inflight: Inflight,
    rejects: Mutex<BTreeMap<RejectReason, u64>>,
    version: String,
    started: Instant,
    debug_manager: Option<Arc<Manager>>,
    metrics: Arc<Metrics>,
    websocket_probe: Option<Arc<super::websocket::WebSocketProbe>>,
    /// `SetKeepAlivesEnabled(false)` equivalent: cancelled by
    /// `begin_drain`, each accepted connection graceful-shuts down — idle
    /// keep-alive connections close at once, in-flight ones after their
    /// current response.
    conn_drain: CancellationToken,
    /// `server.Close()` equivalent: cancelled after the drain window,
    /// force-closing every still-open connection.
    conn_close: CancellationToken,
}

impl Inner {
    fn reject(&self, reason: RejectReason, event: RejectEvent) {
        *self
            .rejects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(reason)
            .or_default() += 1;
        self.metrics.reject(reason, event);
    }
}

#[derive(Default)]
struct ReadActivity {
    generation: AtomicU64,
    waker: futures_util::task::AtomicWaker,
}

struct ActivityIo {
    socket: tokio::net::TcpStream,
    activity: Arc<ReadActivity>,
}

impl AsyncRead for ActivityIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let result = Pin::new(&mut self.socket).poll_read(context, buffer);
        if matches!(result, Poll::Ready(Ok(()))) && buffer.filled().len() > before {
            self.activity.generation.fetch_add(1, Ordering::Release);
            self.activity.waker.wake();
        }
        result
    }
}

impl AsyncWrite for ActivityIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write(context, buffer)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.socket.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.socket).poll_write_vectored(context, buffers)
    }
}

#[derive(Clone)]
struct HeaderTimer(Arc<ReadActivity>);

struct HeaderSleep {
    activity: Arc<ReadActivity>,
    baseline: u64,
    idle: Pin<Box<tokio::time::Sleep>>,
    header: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl HeaderSleep {
    fn new(activity: Arc<ReadActivity>) -> Self {
        let baseline = activity.generation.load(Ordering::Acquire);
        Self {
            activity,
            baseline,
            idle: Box::pin(tokio::time::sleep(IDLE_TIMEOUT)),
            header: None,
        }
    }

    fn reset(mut self: Pin<&mut Self>) {
        self.baseline = self.activity.generation.load(Ordering::Acquire);
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + IDLE_TIMEOUT);
        self.header = None;
    }
}

impl Future for HeaderSleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.activity.generation.load(Ordering::Acquire) > self.baseline && self.header.is_none()
        {
            self.header = Some(Box::pin(tokio::time::sleep(READ_HEADER_TIMEOUT)));
        }
        if let Some(header) = &mut self.header
            && header.as_mut().poll(context).is_ready()
        {
            return Poll::Ready(());
        }
        if self.idle.as_mut().poll(context).is_ready() {
            return Poll::Ready(());
        }
        self.activity.waker.register(context.waker());
        if self.activity.generation.load(Ordering::Acquire) > self.baseline {
            context.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl HyperSleep for HeaderSleep {}

impl HyperTimer for HeaderTimer {
    fn sleep(&self, _duration: Duration) -> Pin<Box<dyn HyperSleep>> {
        Box::pin(HeaderSleep::new(self.0.clone()))
    }

    fn sleep_until(&self, _deadline: std::time::Instant) -> Pin<Box<dyn HyperSleep>> {
        Box::pin(HeaderSleep::new(self.0.clone()))
    }

    fn reset(&self, sleep: &mut Pin<Box<dyn HyperSleep>>, _deadline: std::time::Instant) {
        if let Some(timer) = sleep.as_mut().downcast_mut_pin::<HeaderSleep>() {
            timer.reset();
        } else {
            *sleep = self.sleep(IDLE_TIMEOUT);
        }
    }
}

/// Cloneable application assembly.
/// Go's `net/http` accepts with `TCP_NODELAY` set; without it, Nagle plus
/// delayed ACK added a measured bimodal ~40ms stall to small SSE writes
/// (task-24 profiling: 7ms vs 46ms c=1 stream totals on loopback).
pub fn prepare_accepted(socket: &tokio::net::TcpStream) {
    let _ = socket.set_nodelay(true);
}

#[derive(Clone)]
pub struct App {
    inner: Arc<Inner>,
    router: Router,
    dashboard: Dashboard,
}

struct HttpDashboardData {
    backend: Arc<dyn HttpBackend>,
}

impl DashboardData for HttpDashboardData {
    fn status(&self) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        self.backend.dashboard_status()
    }

    fn models(&self) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async move {
            self.backend
                .list_models(CancellationToken::new())
                .await
                .map(|models| {
                    json!({"models": models.into_iter().map(|model| json!({
                        "uid": model.id,
                        "provider": model.owned_by,
                        "supports_images": model.supports_images,
                        "supports_tool_calls": model.supports_tool_calls,
                        "supports_parallel_tool_calls": model.supports_parallel_tool_calls,
                        "supports_thinking": model.supports_thinking,
                        "preserve_thinking": model.preserve_thinking,
                        "is_model_router": model.is_model_router,
                        "context_tokens": model.context_tokens,
                        "max_output_tokens": model.max_output_tokens,
                    })).collect::<Vec<_>>()})
                })
                .map_err(|error| error.message)
        })
    }
}

impl App {
    pub fn with_backend<B: HttpBackend>(backend: B, config: HttpConfig) -> Self {
        let limit = config.max_concurrency.max(1);
        let backend: Arc<dyn HttpBackend> = Arc::new(backend);
        let dashboard_token = backend.dashboard_token();
        let dashboard_password = config.dashboard_password.clone();
        let dashboard_config_current = config.dashboard_config_current.clone().or_else(|| {
            Some(Arc::new(|| json!({"config":{},"stale":false}))
                as Arc<dyn Fn() -> Value + Send + Sync>)
        });
        let dashboard_config_reload = config.dashboard_config_reload.clone().or_else(|| {
            Some(
                Arc::new(|| Err("config reload hook unavailable".to_string()))
                    as Arc<
                        dyn Fn() -> Result<crate::dashboard::ConfigReloadReport, String>
                            + Send
                            + Sync,
                    >,
            )
        });
        let quota_interval = config.quota_interval;
        let inner = Arc::new(Inner {
            backend,
            api_key: RwLock::new(config.api_key),
            permits: Arc::new(Semaphore::new(limit)),
            max_permits: limit,
            draining: AtomicBool::new(false),
            inflight: Inflight::default(),
            rejects: Mutex::new(BTreeMap::new()),
            version: config.version,
            started: Instant::now(),
            debug_manager: config.debug_manager,
            metrics: Arc::new(Metrics::new()),
            websocket_probe: config.websocket_probe,
            conn_drain: CancellationToken::new(),
            conn_close: CancellationToken::new(),
        });
        let dashboard = Dashboard::new(DashboardConfig {
            password: dashboard_password,
            version: inner.version.clone(),
            token: dashboard_token,
            metrics: Some(inner.metrics.clone()),
            debug_manager: inner.debug_manager.clone(),
            data: Arc::new(HttpDashboardData {
                backend: inner.backend.clone(),
            }),
            gate_stats: {
                let gate_backend = inner.backend.clone();
                Some(Arc::new(move || gate_backend.gate_stats())
                    as Arc<dyn Fn() -> Option<GateStats> + Send + Sync>)
            },

            config_current: dashboard_config_current,
            config_reload: dashboard_config_reload,
        });
        dashboard.start_quota_sampler(quota_interval);
        let router = Router::new()
            .route("/healthz", get(health))
            .route("/v1/models", get(list_models))
            .route("/v1/models/{model}", get(get_model))
            .route(
                "/v1/responses",
                post(create_responses).get(super::websocket::upgrade),
            )
            .route("/v1/chat/completions", post(create_chat))
            .route("/v1/messages", post(create_messages))
            .fallback(not_found)
            .with_state(Arc::clone(&inner))
            .merge(dashboard.router())
            // The guard wraps every route (including panel and health):
            // during drain all responses carry `Connection: close`, the
            // Go `SetKeepAlivesEnabled(false)` wire behavior.
            .layer(middleware::from_fn_with_state(Arc::clone(&inner), v1_guard));
        Self {
            inner,
            router,
            dashboard,
        }
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Replace the API key atomically for subsequent requests.
    pub fn set_api_key(&self, api_key: String) {
        *self
            .inner
            .api_key
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = api_key;
    }

    /// Replace the open-by-default panel password at runtime. Existing panel
    /// sessions are revoked, matching config reload in the Go service.
    pub fn set_dashboard_password(&self, password: String) {
        self.dashboard.set_password(password);
    }

    /// Shared operational metrics source for the panel and diagnostic listener.
    #[must_use]
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.inner.metrics)
    }

    /// Whether the drain flag is set (healthz `draining`, tests).
    #[must_use]
    pub fn is_draining(&self) -> bool {
        self.inner.draining.load(Ordering::Acquire)
    }

    pub fn begin_drain(&self) {
        self.inner.draining.store(true, Ordering::Release);
        // New requests fast-503 via `admit`; already-accepted connections
        // get hyper graceful shutdown so idle keep-alive sockets close
        // immediately instead of lingering until process exit.
        self.inner.conn_drain.cancel();
    }

    /// Force-close every connection still open — the `server.Close()`
    /// step that follows the drain window in the Go lifecycle.
    pub fn close_connections(&self) {
        self.inner.conn_close.cancel();
    }

    #[must_use]
    pub fn available_permits(&self) -> usize {
        self.inner.permits.available_permits()
    }

    #[must_use]
    pub fn max_permits(&self) -> usize {
        self.inner.max_permits
    }

    /// Wait until every admitted turn — in-flight request or reject write
    /// — has finished. Tests and graceful-drain callers use this exact
    /// state transition instead of polling.
    pub async fn wait_idle(&self) {
        self.inner.inflight.wait().await;
    }

    /// Serve with the task-14 connection contract applied at the protocol
    /// parser: bounded headers and header-read deadlines for HTTP/1, and an
    /// idle liveness timeout for HTTP/2. The caller owns the bound listener,
    /// so readiness is observable without polling.
    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        loop {
            let accepted = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                accepted = listener.accept() => accepted,
            };
            let (socket, peer) = accepted?;
            prepare_accepted(&socket);
            let service = TowerToHyperService::new(self.router().layer(axum::Extension(peer)));
            let metrics = self.inner.metrics.clone();
            let conn_drain = self.inner.conn_drain.clone();
            let conn_close = self.inner.conn_close.clone();
            let drain_inner = self.inner.clone();
            // Connections accepted while draining must still answer their
            // first request (the 503 + Connection: close); arming graceful
            // shutdown on them could close the socket before the refusal
            // is written. Pre-drain connections arm immediately.
            let drain_armed = !self.inner.draining.load(Ordering::Acquire);
            tokio::spawn(async move {
                let _task = metrics.begin_task();
                let activity = Arc::new(ReadActivity::default());
                let io = ActivityIo {
                    socket,
                    activity: activity.clone(),
                };
                let mut builder = ConnectionBuilder::new(TokioExecutor::new());
                builder
                    .http1()
                    .timer(HeaderTimer(activity))
                    // HeaderTimer applies IDLE_TIMEOUT until the first read,
                    // then READ_HEADER_TIMEOUT while the header is partial.
                    .header_read_timeout(READ_HEADER_TIMEOUT)
                    .max_buf_size(MAX_HEADER_BYTES);
                builder
                    .http2()
                    .timer(TokioTimer::new())
                    .keep_alive_interval(Some(IDLE_TIMEOUT))
                    .keep_alive_timeout(IDLE_TIMEOUT);
                let connection = builder.serve_connection_with_upgrades(TokioIo::new(io), service);
                tokio::pin!(connection);
                // Keep polling the connection while drain waits for active
                // response bodies. Entering hyper graceful shutdown as soon
                // as SIGTERM arrives can truncate an SSE body that has
                // already sent headers; only disable keepalive after every
                // admitted turn has released its drain guard.
                let drain_ready = async move {
                    conn_drain.cancelled().await;
                    drain_inner.inflight.wait().await;
                };
                tokio::pin!(drain_ready);
                let result = tokio::select! {
                    result = connection.as_mut() => result,
                    () = drain_ready.as_mut(), if drain_armed => {
                        connection.as_mut().graceful_shutdown();
                        connection.await
                    }
                    () = conn_close.cancelled() => {
                        // Forced close: dropping the connection future
                        // aborts in-flight work (Go `server.Close`).
                        return;
                    }
                };
                if let Err(error) = result {
                    tracing::debug!(%error, "HTTP connection closed");
                }
            });
        }
    }

    #[must_use]
    pub fn reject_snapshot(&self) -> RejectSnapshot {
        RejectSnapshot(
            self.inner
                .rejects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    }
}

async fn v1_guard(State(inner): State<Arc<Inner>>, request: Request, next: Next) -> Response {
    if !request.uri().path().starts_with("/v1/") {
        let mut response = next.run(request).await;
        if inner.draining.load(Ordering::Acquire) {
            response
                .headers_mut()
                .insert(CONNECTION, HeaderValue::from_static("close"));
        }
        return response;
    }
    let request_id = crate::randid::prefixed("req_");
    let auth_error = {
        let api_key = inner
            .api_key
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        authenticate(&request, &api_key)
    };
    let mut response = if let Some((reason, message)) = auth_error {
        inner.reject(
            reason,
            reject_event(&request, StatusCode::UNAUTHORIZED.as_u16()),
        );
        json_response_value(
            StatusCode::UNAUTHORIZED,
            &json!({"error":{"message":message,"type":"unauthenticated","code":null,"param":null}}),
        )
    } else {
        next.run(request).await
    };
    response.headers_mut().insert(
        "request-id",
        HeaderValue::from_str(&request_id).expect("generated request id is a header value"),
    );
    if inner.draining.load(Ordering::Acquire) {
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("close"));
    }
    response
}

fn reject_event(request: &Request, status: u16) -> RejectEvent {
    let credential = request
        .headers()
        .get(AUTHORIZATION)
        .or_else(|| request.headers().get("x-api-key"))
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let key_hash = if credential.is_empty() {
        String::new()
    } else {
        let digest = Sha256::digest(credential.as_bytes());
        let prefix = digest[..6].iter().fold(String::new(), |mut out, byte| {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
            out
        });
        format!("sha256:{prefix}")
    };
    RejectEvent {
        status,
        path: request.uri().path().to_string(),
        key_hash,
        user_agent: request
            .headers()
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        ..RejectEvent::default()
    }
}

fn authenticate(request: &Request, expected: &str) -> Option<(RejectReason, &'static str)> {
    if expected.trim().is_empty() {
        return None;
    }
    let bearer = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let provided = bearer.or_else(|| {
        request
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    });
    let Some(provided) = provided else {
        return Some((RejectReason::MissingApiKey, "Missing API key"));
    };
    let expected_hash = Sha256::digest(expected.as_bytes());
    let provided_hash = Sha256::digest(provided.as_bytes());
    if expected_hash.ct_eq(&provided_hash).unwrap_u8() != 1 {
        return Some((RejectReason::InvalidApiKey, "Invalid API key"));
    }
    None
}

async fn health(State(inner): State<Arc<Inner>>) -> Response {
    json_response_value(
        StatusCode::OK,
        &json!({
            "status":"ok", "version":inner.version,
            "uptime_seconds":inner.started.elapsed().as_secs(),
            "debug_logging":inner.debug_manager.as_ref().is_some_and(|manager| manager.enabled()),
            "draining":inner.draining.load(Ordering::Acquire),
            "active_requests":inner.metrics.active(),
            "pid":std::process::id(),
        }),
    )
}

async fn list_models(State(inner): State<Arc<Inner>>) -> Response {
    let cancel = CancellationToken::new();
    match inner.backend.list_models(cancel).await {
        Ok(models) => {
            let data: Vec<Value> = models
                .iter()
                .map(|model| Value::Object(model_entry(model)))
                .collect();
            json_response_value(StatusCode::OK, &json!({"object":"list","data":data}))
        }
        Err(failure) => catalog_failure_response(failure),
    }
}

async fn get_model(State(inner): State<Arc<Inner>>, Path(id): Path<String>) -> Response {
    if id.is_empty() {
        return simple_openai_error(
            StatusCode::BAD_REQUEST,
            "model id is required",
            "invalid_request_error",
        );
    }
    let cancel = CancellationToken::new();
    match inner.backend.list_models(cancel).await {
        Ok(models) => models.iter().find(|model| model.id == id).map_or_else(
            || {
                simple_openai_error(
                    StatusCode::NOT_FOUND,
                    &format!("model {id:?} not found"),
                    "invalid_request_error",
                )
            },
            |model| json_response_value(StatusCode::OK, &Value::Object(model_entry(model))),
        ),
        Err(failure) => catalog_failure_response(failure),
    }
}

async fn not_found() -> Response {
    let mut response = Response::new(Body::from("404 page not found\n"));
    *response.status_mut() = StatusCode::NOT_FOUND;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

async fn create_responses(state: State<Arc<Inner>>, request: Request) -> Response {
    create_completion(state.0, request, ProtocolKind::Responses).await
}
async fn create_chat(state: State<Arc<Inner>>, request: Request) -> Response {
    create_completion(state.0, request, ProtocolKind::Chat).await
}
async fn create_messages(state: State<Arc<Inner>>, request: Request) -> Response {
    create_completion(state.0, request, ProtocolKind::Anthropic).await
}

struct Decoded {
    messages: RequestMessages,
    streaming: bool,
    include_usage: bool,
}

struct ImmediateErrorStream(Option<Failure>);

impl HttpEventStream for ImmediateErrorStream {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let event = self.0.take().map(|failure| ResponseEvent {
            kind: ResponseEventType::Error,
            reason: Some(StopReason::Error),
            error: Some(Arc::new(AssistantMessage {
                error_message: failure.to_string(),
                failure: Some(Box::new(failure)),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        });
        Box::pin(async move { Ok(event) })
    }
}

fn decode(kind: ProtocolKind, body: &[u8], collect_dropped: bool) -> Result<Decoded, Failure> {
    match kind {
        ProtocolKind::Responses => {
            responses::decode_request(body, collect_dropped).map(|request| Decoded {
                messages: request.context,
                streaming: request.options.stream,
                include_usage: false,
            })
        }
        ProtocolKind::Chat => chat::decode_request(body, collect_dropped).map(|request| Decoded {
            messages: request.context,
            streaming: request.options.stream,
            include_usage: request.options.include_usage,
        }),
        ProtocolKind::Anthropic => {
            messages::decode_request(body, collect_dropped).map(|request| Decoded {
                messages: request.context,
                streaming: request.options.stream,
                include_usage: false,
            })
        }
    }
}

struct RecorderMeta {
    method: String,
    path: String,
    user_agent: String,
    websocket: bool,
}

fn start_recorder(
    inner: &Inner,
    meta: &RecorderMeta,
    kind: ProtocolKind,
    cancel: &CancellationToken,
) -> Recorder {
    let recorder = inner
        .debug_manager
        .as_ref()
        .map_or_else(Recorder::none, |manager| {
            manager.start(&RequestMeta {
                method: meta.method.clone(),
                path: meta.path.clone(),
                api: if meta.websocket {
                    "responses-ws"
                } else {
                    match kind {
                        ProtocolKind::Responses => "openai-responses",
                        ProtocolKind::Chat => "openai-chat",
                        ProtocolKind::Anthropic => "anthropic",
                    }
                }
                .to_string(),
                user_agent: meta.user_agent.clone(),
                ..RequestMeta::default()
            })
        });
    // Go `recorder.SetAbort(cancel)`: the panel abort endpoint cancels the
    // same token the routing/gate/backoff/connect/receive/downstream
    // lineage observes, so live requests report `abortable` and actually
    // terminate. `complete` clears the hook; `CancelOnDrop` releases the
    // token exactly once.
    let abort_token = cancel.clone();
    recorder.set_abort(Arc::new(move || abort_token.cancel()));
    recorder
}

pub(crate) async fn ws_run_turn(inner: Arc<Inner>, request: Request) -> Response {
    create_completion(inner, request, ProtocolKind::Responses).await
}

pub(crate) fn ws_probe(inner: &Inner) -> Option<Arc<super::websocket::WebSocketProbe>> {
    inner.websocket_probe.clone()
}

pub(crate) fn ws_is_draining(inner: &Inner) -> bool {
    inner.draining.load(Ordering::Acquire)
}

pub(crate) fn ws_reject(inner: &Inner, reason: RejectReason) {
    let status = match reason {
        RejectReason::Draining => StatusCode::SERVICE_UNAVAILABLE,
        RejectReason::ConcurrencyLimit | RejectReason::WsConnectionLimit => {
            StatusCode::TOO_MANY_REQUESTS
        }
        RejectReason::MissingApiKey | RejectReason::InvalidApiKey => StatusCode::UNAUTHORIZED,
        RejectReason::HttpRead => StatusCode::BAD_REQUEST,
    };
    inner.reject(
        reason,
        RejectEvent {
            status: status.as_u16(),
            path: "/v1/responses".into(),
            ..RejectEvent::default()
        },
    );
}

async fn create_completion(inner: Arc<Inner>, request: Request, kind: ProtocolKind) -> Response {
    let _task = inner.metrics.begin_task();
    let _span = inner.metrics.begin_span();
    let queued_at = Instant::now();
    let admission = match admit(&inner, &request) {
        Ok(admission) => {
            inner.metrics.observe_queue_wait(queued_at.elapsed());
            admission
        }
        Err(response) => return response,
    };
    let request_metrics = inner.metrics.begin();
    let response =
        create_completion_inner(inner.clone(), request, kind, admission, &request_metrics).await;
    track_response(response, request_metrics)
}

// One request pipeline mirroring Go's completion handler for parity
// review.
#[allow(clippy::too_many_lines)]
async fn create_completion_inner(
    inner: Arc<Inner>,
    request: Request,
    kind: ProtocolKind,
    admission: Admission,
    request_metrics: &RequestMetrics,
) -> Response {
    // Capture metadata, but do not create a recorder until the complete body
    // arrived. Transport failures are pre-pipeline rejects and must not make
    // request directories.
    let recorder_meta = RecorderMeta {
        method: request.method().to_string(),
        path: request.uri().path().to_string(),
        websocket: request
            .extensions()
            .get::<super::websocket::WsTurn>()
            .is_some(),
        user_agent: request
            .headers()
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
    };
    // The request's cancellation token (Go `reqCtx`): created before the
    // body read and attached to the recorder at creation so the panel
    // abort can interrupt the in-flight request; `CancelOnDrop` is the
    // `defer cancel()` backstop releasing it exactly once.
    let cancel = CancellationToken::new();
    let mut cancel_guard = CancelOnDrop(Some(cancel.clone()));
    let body_result =
        tokio::time::timeout(READ_TIMEOUT, to_bytes(request.into_body(), MAX_BODY_BYTES)).await;
    let body = match body_result {
        Ok(Ok(body)) => body,
        Ok(Err(err)) if is_body_too_large(&err) => {
            let recorder = start_recorder(&inner, &recorder_meta, kind, &cancel);
            return with_debug_header(
                finish_error(
                    &recorder,
                    kind,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Failure::invalid_argument("request payload exceeds the 32 MiB limit"),
                    "http_read",
                ),
                &recorder,
            );
        }
        Ok(Err(err)) => {
            inner.reject(
                RejectReason::HttpRead,
                RejectEvent {
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    path: recorder_meta.path.clone(),
                    user_agent: recorder_meta.user_agent.clone(),
                    ..RejectEvent::default()
                },
            );
            return protocol_error_response(
                kind,
                StatusCode::BAD_REQUEST,
                Failure::plain(format!("read request: {err}")),
                "http_read",
                "",
            );
        }
        Err(_) => {
            inner.reject(
                RejectReason::HttpRead,
                RejectEvent {
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    path: recorder_meta.path.clone(),
                    user_agent: recorder_meta.user_agent.clone(),
                    ..RejectEvent::default()
                },
            );
            return protocol_error_response(
                kind,
                StatusCode::BAD_REQUEST,
                Failure::plain("read request: deadline exceeded"),
                "http_read",
                "",
            );
        }
    };
    let recorder = start_recorder(&inner, &recorder_meta, kind, &cancel);
    if recorder_meta.websocket {
        recorder.flush();
    }
    if recorder.is_active() {
        let parsed_body = serde_json::from_slice::<Value>(&body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
        recorder.write_json(
            STAGE_HTTP_REQUEST,
            LogValue::serde(json!({
                "method": recorder_meta.method,
                "path": recorder_meta.path,
                "body": parsed_body,
            })),
        );
    }
    let mut completion_guard = RequestCompletionGuard {
        recorder: recorder.clone(),
        armed: true,
    };
    let decoded = match decode(kind, &body, recorder.is_active()) {
        Ok(decoded) => decoded,
        Err(failure) => {
            return with_debug_header(
                finish_error(
                    &recorder,
                    kind,
                    StatusCode::BAD_REQUEST,
                    failure,
                    "http_decode",
                ),
                &recorder,
            );
        }
    };
    request_metrics.observe(decoded.streaming, body.len());
    recorder.set_model(&decoded.messages.model);
    if recorder.is_active() {
        recorder.write_json(
            STAGE_REQUEST_MESSAGES,
            LogValue::Tree(request_messages_projection(&decoded.messages)),
        );
    }
    recorder.note_request_ready();
    let model = decoded.messages.model.trim().to_string();
    let premature_candidate = matches!(
        decoded.messages.messages.last(),
        Some(crate::domain::Message::ToolResult(_))
    );
    let source = match inner
        .backend
        .stream(decoded.messages, cancel.clone(), recorder.clone())
        .await
    {
        Ok(source) => source,
        Err(failure)
            if decoded.streaming
                && kind != ProtocolKind::Anthropic
                && (failure.rate_limited || failure.context_length) =>
        {
            Box::new(ImmediateErrorStream(Some(failure))) as Box<dyn HttpEventStream>
        }
        Err(failure) => {
            return with_debug_header(
                finish_failure(&recorder, kind, failure, "provider_stream"),
                &recorder,
            );
        }
    };
    let completion = Completion {
        status_code: 500,
        result: "failed".into(),
        model: model.clone(),
        requested_model: model.clone(),
        stream: decoded.streaming,
        premature_end_turn: premature_candidate,
        ..Completion::default()
    };
    let result = if decoded.streaming {
        sse_response(
            source,
            kind,
            model,
            decoded.include_usage,
            cancel,
            recorder.clone(),
            admission,
            completion.clone(),
        )
        .await
    } else {
        json_response(
            source,
            kind,
            model,
            cancel,
            recorder.clone(),
            admission,
            completion.clone(),
        )
        .await
    };
    match result {
        Ok((response, producer_owns_completion)) => {
            if producer_owns_completion {
                cancel_guard.0.take();
                completion_guard.armed = false;
            } else {
                let mut completed = completion;
                completed.status_code = 200;
                completed.result = "completed".into();
                recorder.complete(completed);
            }
            with_debug_header(response, &recorder)
        }
        Err(failure) => with_debug_header(
            finish_failure(&recorder, kind, failure, "provider_stream"),
            &recorder,
        ),
    }
}

struct TrackedBody {
    inner: Body,
    request: Option<RequestMetrics>,
    status: u16,
    bytes: u64,
    /// The stream body's own terminal verdict (Go's `completion.Result`
    /// derived from the handler error — the egress bytes are never
    /// scanned). `None` for non-streamed responses, where the status
    /// line carries the same information.
    result_cell: Option<super::stream::TerminalCell>,
}

impl TrackedBody {
    fn finish(&mut self, result: &str) {
        if let Some(request) = self.request.take() {
            request.finish(self.status, self.bytes, result);
        }
    }
}

impl Drop for TrackedBody {
    /// A body dropped before end-of-stream is a disconnect regardless of
    /// the cell: the cell is written at the terminal transition, which a
    /// dropped body may have reached while its tail was still queued —
    /// the scan this replaced also reported `disconnected` there.
    fn drop(&mut self) {
        self.finish("disconnected");
    }
}

impl HttpBody for TrackedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(context) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.bytes = this.bytes.saturating_add(data.len() as u64);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish("failed");
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                // End of stream: the stream body has already recorded its
                // verdict in the cell. Non-streamed responses carry no
                // cell — their status line is the verdict (error bodies
                // are only produced with 4xx/5xx).
                let result = this
                    .result_cell
                    .as_ref()
                    .and_then(super::stream::TerminalCell::get)
                    .unwrap_or(if this.status >= 400 {
                        "failed"
                    } else {
                        "completed"
                    });
                this.finish(result);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn track_response(response: Response, request: RequestMetrics) -> Response {
    let (mut parts, body) = response.into_parts();
    let result_cell = parts.extensions.remove::<super::stream::TerminalCell>();
    let tracked = TrackedBody {
        inner: body,
        request: Some(request),
        status: parts.status.as_u16(),
        bytes: 0,
        result_cell,
    };
    Response::from_parts(parts, Body::new(tracked))
}

struct RequestCompletionGuard {
    recorder: Recorder,
    armed: bool,
}

impl Drop for RequestCompletionGuard {
    fn drop(&mut self) {
        if self.armed {
            let failure = Failure::plain("client disconnected");
            self.recorder.write_error("client_disconnected", &failure);
            self.recorder.complete(Completion {
                status_code: 499,
                result: "disconnected".into(),
                ..Completion::default()
            });
        }
    }
}

struct CancelOnDrop(Option<CancellationToken>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = self.0.take() {
            cancel.cancel();
        }
    }
}

fn is_body_too_large(err: &axum::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = current {
        if error.is::<http_body_util::LengthLimitError>() {
            return true;
        }
        current = error.source();
    }
    false
}

fn with_debug_header(mut response: Response, recorder: &Recorder) -> Response {
    let dir = recorder.dir_name();
    if !dir.is_empty()
        && let Ok(value) = HeaderValue::from_str(&dir)
    {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

/// `drainTracker` state: a count plus a `Notify` fired on each
/// zero-crossing. Waiters observe the first zero after they start waiting,
/// matching Go's close-once drained channel.
#[derive(Default)]
struct Inflight {
    count: Mutex<usize>,
    drained: tokio::sync::Notify,
}

impl Inflight {
    fn add(&self) {
        *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
    }

    fn done(&self) {
        let mut count = self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.drained.notify_waiters();
        }
    }

    async fn wait(&self) {
        if *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == 0
        {
            return;
        }
        let notified = self.drained.notified();
        tokio::pin!(notified);
        // Register the waiter, then re-check under the lock: a
        // zero-crossing before registration is caught by the re-check,
        // one after registration resolves `notified`.
        let _ = notified.as_mut().enable();
        if *self
            .count
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == 0
        {
            return;
        }
        notified.as_mut().await;
    }
}

/// Decrements the drain tracker when the reject `Response` is dropped —
/// after hyper has written it, mirroring Go's "release after the refusal
/// frame is sent" ordering.
#[derive(Clone)]
struct RejectRelease(Arc<Inner>);

impl Drop for RejectRelease {
    fn drop(&mut self) {
        self.0.inflight.done();
    }
}

/// One admitted inference turn: the concurrency permit plus the
/// drain-tracker slot. The guard lives as long as the handler (or the
/// spawned body pump it hands off to), so `wait_idle` covers the whole
/// request including mid-pipeline aborts (Go `defer release()` parity).
pub struct Admission {
    _permit: OwnedSemaphorePermit,
    inner: Arc<Inner>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        self.inner.inflight.done();
    }
}

// The Err arm is a fully-built reject Response (Go returns the refusal
// frame directly); boxing it would add an indirection for a cold path.
#[allow(clippy::result_large_err)]
fn admit(inner: &Arc<Inner>, request: &Request) -> Result<Admission, Response> {
    // The tracker increments before the draining check so a drain waiter
    // covers every request already past this point (Go `inflight.Add`
    // ordering).
    inner.inflight.add();
    if inner.draining.load(Ordering::Acquire) {
        inner.reject(
            RejectReason::Draining,
            reject_event(request, StatusCode::SERVICE_UNAVAILABLE.as_u16()),
        );
        let mut response = json_response_value(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error":{"message":"server is draining for restart; retry the request","type":"server_error","code":"server_draining","param":null}}),
        );
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        response
            .extensions_mut()
            .insert(RejectRelease(inner.clone()));
        return Err(response);
    }
    if let Ok(permit) = inner.permits.clone().try_acquire_owned() {
        Ok(Admission {
            _permit: permit,
            inner: inner.clone(),
        })
    } else {
        inner.reject(
            RejectReason::ConcurrencyLimit,
            reject_event(request, StatusCode::TOO_MANY_REQUESTS.as_u16()),
        );
        let mut response = json_response_value(
            StatusCode::TOO_MANY_REQUESTS,
            &json!({"error":{"message":"server is busy, please try again later","type":"rate_limit_error","code":"rate_limit_exceeded","param":null}}),
        );
        response
            .headers_mut()
            .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        response
            .extensions_mut()
            .insert(RejectRelease(inner.clone()));
        Err(response)
    }
}

fn finish_failure(
    recorder: &Recorder,
    kind: ProtocolKind,
    failure: Failure,
    stage: &str,
) -> Response {
    let status = StatusCode::from_u16(http_status(&failure)).unwrap_or(StatusCode::BAD_GATEWAY);
    finish_error(recorder, kind, status, failure, stage)
}

fn finish_error(
    recorder: &Recorder,
    kind: ProtocolKind,
    status: StatusCode,
    failure: Failure,
    stage: &str,
) -> Response {
    recorder.write_error(stage, &failure);
    let debug_ref = recorder.dir_name();
    let response = protocol_error_response(kind, status, failure, stage, &debug_ref);
    recorder.complete(Completion {
        status_code: i64::from(status.as_u16()),
        result: "failed".into(),
        ..Completion::default()
    });
    response
}

fn catalog_failure_response(failure: Failure) -> Response {
    let mapped = StatusCode::from_u16(http_status(&failure)).unwrap_or(StatusCode::BAD_GATEWAY);
    let status = if mapped != StatusCode::TOO_MANY_REQUESTS && mapped.is_client_error() {
        StatusCode::BAD_GATEWAY
    } else {
        mapped
    };
    protocol_error_response(
        ProtocolKind::Responses,
        status,
        failure,
        "provider_catalog",
        "",
    )
}

fn protocol_error_response(
    kind: ProtocolKind,
    status: StatusCode,
    mut failure: Failure,
    stage: &str,
    debug_ref: &str,
) -> Response {
    let retry_after = failure.retry_after_seconds;
    if matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE
    ) {
        failure.client_fixable = true;
    }
    let body = encode_http_error(kind, failure, stage, debug_ref);
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if status == StatusCode::TOO_MANY_REQUESTS
        && retry_after > 0
        && let Ok(value) = HeaderValue::from_str(&retry_after.to_string())
    {
        response.headers_mut().insert(RETRY_AFTER, value);
    }
    response
}

fn simple_openai_error(status: StatusCode, message: &str, error_type: &str) -> Response {
    json_response_value(
        status,
        &json!({"error":{"message":message,"type":error_type,"code":null,"param":null}}),
    )
}

fn json_response_value(status: StatusCode, value: &Value) -> Response {
    let mut body = serde_json::to_vec(&value).expect("JSON value serializes");
    body.push(b'\n');
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Backward-compatible alias retained for task-1 callers.
pub fn placeholder_router() -> Router {
    Router::new()
}

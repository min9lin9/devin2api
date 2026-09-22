//! Lock-free request counters and capability-described Rust diagnostics.
//!
//! The HTTP field names and rate/trend semantics match `internal/obs` in the
//! Go implementation. Go-runtime-only values remain present as JSON null;
//! Rust replacements are exposed separately with explicit capability data.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::response::Response;
use axum::routing::{any, get};
use http::{HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

const TREND_BUCKET_SECONDS: i64 = 10;
const TREND_BUCKETS: usize = 360;
// TREND_BUCKETS as i64 (kept literal: const TryFrom is not relied on).
const TREND_BUCKETS_I64: i64 = 360;
const TREND_WINDOW_MINUTES: i64 = 60;
const REJECT_EVENT_CAPACITY: usize = 256;

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
}

#[derive(Clone, Copy, Debug, Default)]
struct TrendBucket {
    at: i64,
    requests: u64,
    errors: u64,
}

/// Stable pre-pipeline rejection vocabulary shared by HTTP and WebSocket.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectReason {
    Draining,
    ConcurrencyLimit,
    WsConnectionLimit,
    MissingApiKey,
    InvalidApiKey,
    HttpRead,
}

impl RejectReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::ConcurrencyLimit => "concurrency_limit",
            Self::WsConnectionLimit => "ws_connection_limit",
            Self::MissingApiKey => "missing_api_key",
            Self::InvalidApiKey => "invalid_api_key",
            Self::HttpRead => "http_read",
        }
    }
}

/// Structured trace for a request rejected before the normal pipeline.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RejectEvent {
    pub at: i64,
    pub reason: String,
    pub status: u16,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub ip: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub key_hash: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_agent: String,
}

#[derive(Debug, Default)]
struct RejectState {
    counts: BTreeMap<RejectReason, u64>,
    recent: VecDeque<RejectEvent>,
}

#[derive(Debug, Default)]
struct ProcessSampleState {
    system: sysinfo::System,
    last_cpu_seconds: f64,
    last_sample: Option<Instant>,
}

impl ProcessSampleState {
    fn initialized() -> Self {
        use sysinfo::ProcessesToUpdate;
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let mut system = sysinfo::System::new();
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let last_cpu_seconds = system.process(pid).map_or(0.0, |process| {
            f64::from(u32::try_from(process.accumulated_cpu_time()).unwrap_or(u32::MAX)) / 1000.0
        });
        Self {
            system,
            last_cpu_seconds,
            last_sample: Some(Instant::now()),
        }
    }
}

#[derive(Debug, Default)]
struct MetricsInner {
    active: AtomicI64,
    completed: AtomicU64,
    rejected: AtomicU64,
    ok_responses: AtomicU64,
    client_errors: AtomicU64,
    server_errors: AtomicU64,
    streaming: AtomicU64,
    buffered: AtomicU64,
    request_bytes: AtomicU64,
    response_bytes: AtomicU64,
    rejects: Mutex<RejectState>,
    buckets: Mutex<Vec<TrendBucket>>,
    process: Mutex<ProcessSampleState>,
    tasks_active: AtomicU64,
    tasks_spawned: AtomicU64,
    spans_active: AtomicU64,
    spans_opened: AtomicU64,
    queue_wait_count: AtomicU64,
    queue_wait_total_ns: AtomicU64,
    queue_wait_max_ns: AtomicU64,
    queue_wait_buckets: [AtomicU64; 7],
}

/// Process-wide operational metrics. Clones share the same counters.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
    started: Instant,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Metrics")
            .field("inner", &self.inner)
            .field("started", &self.started)
            .finish_non_exhaustive()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(unix_seconds))
    }

    /// Construct metrics with an injected Unix-seconds source. This keeps
    /// bucket/rate tests deterministic without changing production time.
    #[doc(hidden)]
    pub fn with_clock(clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                buckets: Mutex::new(vec![TrendBucket::default(); TREND_BUCKETS]),
                process: Mutex::new(ProcessSampleState::initialized()),
                ..MetricsInner::default()
            }),
            started: Instant::now(),
            clock,
        }
    }

    fn now(&self) -> i64 {
        (self.clock)()
    }

    pub fn begin(&self) -> RequestMetrics {
        self.inner.active.fetch_add(1, Ordering::Relaxed);
        RequestMetrics {
            metrics: self.clone(),
            observed: AtomicBool::new(false),
            streaming: AtomicBool::new(false),
            request_bytes: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        }
    }

    pub fn active(&self) -> i64 {
        self.inner.active.load(Ordering::Relaxed)
    }

    pub fn reject(&self, reason: RejectReason, mut event: RejectEvent) {
        self.inner.rejected.fetch_add(1, Ordering::Relaxed);
        let now = self.now();
        self.record_bucket(now, true);
        event.at = now;
        event.reason = reason.as_str().to_string();
        let mut rejects = self
            .inner
            .rejects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *rejects.counts.entry(reason).or_default() += 1;
        rejects.recent.push_front(event);
        rejects.recent.truncate(REJECT_EVENT_CAPACITY);
    }

    pub fn seed_trend(&self, finished_at: SystemTime, is_error: bool) {
        let at = finished_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .cast_signed();
        if at >= self.now() - TREND_WINDOW_MINUTES * 60 {
            self.record_bucket(at, is_error);
        }
    }

    fn record_bucket(&self, at: i64, is_error: bool) {
        let aligned = at / TREND_BUCKET_SECONDS * TREND_BUCKET_SECONDS;
        let index = usize::try_from((aligned / TREND_BUCKET_SECONDS).rem_euclid(TREND_BUCKETS_I64))
            .unwrap_or_default();
        let mut buckets = self
            .inner
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buckets[index].at != aligned {
            buckets[index] = TrendBucket {
                at: aligned,
                ..TrendBucket::default()
            };
        }
        buckets[index].requests += 1;
        if is_error {
            buckets[index].errors += 1;
        }
    }

    pub fn begin_task(&self) -> TaskGuard {
        self.inner.tasks_active.fetch_add(1, Ordering::Relaxed);
        self.inner.tasks_spawned.fetch_add(1, Ordering::Relaxed);
        TaskGuard {
            metrics: self.clone(),
        }
    }

    pub fn begin_span(&self) -> SpanGuard {
        self.inner.spans_active.fetch_add(1, Ordering::Relaxed);
        self.inner.spans_opened.fetch_add(1, Ordering::Relaxed);
        SpanGuard {
            metrics: self.clone(),
        }
    }

    pub fn observe_queue_wait(&self, wait: Duration) {
        let nanos = u64::try_from(wait.as_nanos()).unwrap_or(u64::MAX);
        self.inner.queue_wait_count.fetch_add(1, Ordering::Relaxed);
        self.inner
            .queue_wait_total_ns
            .fetch_add(nanos, Ordering::Relaxed);
        self.inner
            .queue_wait_max_ns
            .fetch_max(nanos, Ordering::Relaxed);
        let ms = wait.as_secs_f64() * 1000.0;
        let index = [1.0, 5.0, 10.0, 50.0, 100.0, 500.0]
            .iter()
            .position(|bound| ms <= *bound)
            .unwrap_or(6);
        self.inner.queue_wait_buckets[index].fetch_add(1, Ordering::Relaxed);
    }

    /// Go-compatible HTTP snapshot plus the approved runtime marker.
    pub fn snapshot(&self) -> Value {
        let now = self.now();
        json!({
            "runtime": "rust",
            "uptime_seconds": self.started.elapsed().as_secs(),
            "active_requests": self.active(),
            "completed_requests": self.inner.completed.load(Ordering::Relaxed),
            "rejected_requests": self.inner.rejected.load(Ordering::Relaxed),
            "ok_responses": self.inner.ok_responses.load(Ordering::Relaxed),
            "client_error_responses": self.inner.client_errors.load(Ordering::Relaxed),
            "server_error_responses": self.inner.server_errors.load(Ordering::Relaxed),
            "streaming_requests": self.inner.streaming.load(Ordering::Relaxed),
            "non_streaming_requests": self.inner.buffered.load(Ordering::Relaxed),
            "request_body_bytes": self.inner.request_bytes.load(Ordering::Relaxed),
            "response_body_bytes": self.inner.response_bytes.load(Ordering::Relaxed),
            "trend_minutes": self.trend(now),
            "rates": self.rates(now),
            "process": self.process_snapshot(),
            "rejects": self.rejects_snapshot(),
            "capabilities": Self::capabilities(),
        })
    }

    /// Rust-specific task/wait/span snapshot served by the diagnostic listener.
    // u64→f64 mirrors Go's float64 stats; queue waits stay far below
    // 2^53 ns so precision loss is theoretical.
    #[allow(clippy::cast_precision_loss)]
    pub fn diagnostics_snapshot(&self) -> Value {
        let count = self.inner.queue_wait_count.load(Ordering::Relaxed);
        let total = self.inner.queue_wait_total_ns.load(Ordering::Relaxed);
        let buckets: Vec<Value> = [
            "le_1_ms",
            "le_5_ms",
            "le_10_ms",
            "le_50_ms",
            "le_100_ms",
            "le_500_ms",
            "gt_500_ms",
        ]
        .iter()
        .zip(&self.inner.queue_wait_buckets)
        .map(|(label, value)| json!({"label":label,"count":value.load(Ordering::Relaxed)}))
        .collect();
        json!({
            "runtime": "rust",
            "schema_version": 1,
            "pid": std::process::id(),
            "http": self.snapshot(),
            "threads": thread_snapshot(),
            "tasks": {
                "active": self.inner.tasks_active.load(Ordering::Relaxed),
                "spawned_total": self.inner.tasks_spawned.load(Ordering::Relaxed),
                "scope": "instrumented_application_tasks",
            },
            "queue_waits": {
                "count": count,
                "total_ms": total as f64 / 1_000_000.0,
                "max_ms": self.inner.queue_wait_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
                "mean_ms": if count == 0 { Value::Null } else { json!(total as f64 / count as f64 / 1_000_000.0) },
                "buckets": buckets,
            },
            "allocator": {
                "allocated_bytes": Value::Null,
                "resident_bytes": Value::Null,
                "supported": false,
                "reason": "the system allocator exposes no portable live-allocation counters",
            },
            "process": self.process_snapshot(),
            "tracing_spans": {
                "active": self.inner.spans_active.load(Ordering::Relaxed),
                "opened_total": self.inner.spans_opened.load(Ordering::Relaxed),
                "scope": "instrumented_application_spans",
            },
            "capabilities": Self::capabilities(),
        })
    }

    fn rejects_snapshot(&self) -> Value {
        let rejects = self
            .inner
            .rejects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let counts: BTreeMap<&str, u64> = rejects
            .counts
            .iter()
            .map(|(reason, count)| (reason.as_str(), *count))
            .collect();
        json!({
            "by_reason": counts,
            "recent": rejects.recent,
            "labels": [
                {"reason":"draining","label":"排空"},
                {"reason":"concurrency_limit","label":"并发上限"},
                {"reason":"ws_connection_limit","label":"WS连接上限"},
                {"reason":"missing_api_key","label":"缺API Key"},
                {"reason":"invalid_api_key","label":"错API Key"},
                {"reason":"http_read","label":"读体失败"},
            ],
        })
    }

    fn trend(&self, now: i64) -> Vec<Value> {
        let current = now / TREND_BUCKET_SECONDS;
        let buckets = self
            .inner
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        (current - TREND_BUCKETS_I64 + 1..=current)
            .map(|slot| {
                let at = slot * TREND_BUCKET_SECONDS;
                let bucket = buckets
                    [usize::try_from(slot.rem_euclid(TREND_BUCKETS_I64)).unwrap_or_default()];
                if bucket.at == at {
                    json!({"at":at,"requests":bucket.requests,"errors":bucket.errors})
                } else {
                    json!({"at":at,"requests":0,"errors":0})
                }
            })
            .collect()
    }

    // u64/i64→f64 mirrors Go's float64 rate math; counters stay far
    // below 2^53 so precision loss is theoretical.
    #[allow(clippy::cast_precision_loss)]
    fn rates(&self, now: i64) -> Value {
        let buckets = self
            .inner
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let window_start =
            (now / TREND_BUCKET_SECONDS - TREND_BUCKETS_I64 + 1) * TREND_BUCKET_SECONDS;
        let mut total = 0_u64;
        let mut earliest = 0_i64;
        let mut per_minute = BTreeMap::<i64, u64>::new();
        for bucket in buckets {
            if bucket.at == 0 || bucket.at < window_start || bucket.at > now {
                continue;
            }
            total += bucket.requests;
            *per_minute.entry(bucket.at / 60).or_default() += bucket.requests;
            if earliest == 0 || bucket.at < earliest {
                earliest = bucket.at;
            }
        }
        let current = per_minute.get(&(now / 60)).copied().unwrap_or(0);
        let peak = per_minute.values().copied().max().unwrap_or(0);
        let mut elapsed = (self.started.elapsed().as_secs() / 60 + 1).cast_signed();
        if earliest != 0 {
            elapsed = elapsed.max((now - earliest) / 60 + 1);
        }
        elapsed = elapsed.min(TREND_WINDOW_MINUTES);
        json!({
            "rpm_current": current,
            "rpm_peak": peak,
            "rpm_avg": total as f64 / elapsed as f64,
            "qps_current": current as f64 / (now.rem_euclid(60) + 1) as f64,
        })
    }

    fn process_snapshot(&self) -> Value {
        use sysinfo::ProcessesToUpdate;
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let mut state = self
            .inner
            .process
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .system
            .refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let (rss, virtual_memory, cpu_seconds) =
            state
                .system
                .process(pid)
                .map_or((None, None, None), |process| {
                    (
                        Some(process.memory()),
                        Some(process.virtual_memory()),
                        Some(
                            f64::from(
                                u32::try_from(process.accumulated_cpu_time()).unwrap_or(u32::MAX),
                            ) / 1000.0,
                        ),
                    )
                });
        let now = Instant::now();
        let wall = state
            .last_sample
            .map_or(0.0, |last| now.duration_since(last).as_secs_f64());
        let cpu_limit = f64::from(
            u32::try_from(
                std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            )
            .unwrap_or(u32::MAX),
        ) * 100.0;
        let cpu_percent = cpu_seconds.and_then(|cpu| {
            let cpu_delta = cpu - state.last_cpu_seconds;
            (wall > 0.0 && cpu_delta >= 0.0)
                .then(|| (cpu_delta / wall * 100.0).clamp(0.0, cpu_limit))
        });
        if let Some(cpu) = cpu_seconds {
            state.last_cpu_seconds = cpu;
        }
        state.last_sample = Some(now);
        json!({
            "goroutines": Value::Null,
            "num_cpu": std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
            "heap_alloc_bytes": Value::Null,
            "heap_sys_bytes": Value::Null,
            "stack_inuse": Value::Null,
            "alloc_total": Value::Null,
            "num_gc": Value::Null,
            "gc_pause_total_ms": Value::Null,
            "gc_cpu_fraction": Value::Null,
            "cpu_seconds": cpu_seconds,
            "cpu_percent": cpu_percent,
            "max_rss_bytes": peak_rss_bytes(),
            "rss_bytes": rss,
            "virtual_memory_bytes": virtual_memory,
        })
    }

    fn capabilities() -> Value {
        json!({
            "process_memory": {"supported": true, "source": "sysinfo"},
            "process_cpu": {"supported": true, "source": "sysinfo_accumulated_cpu_time"},
            "peak_rss": {"supported": cfg!(target_os = "linux"), "source": if cfg!(target_os = "linux") { "proc_status_VmHWM" } else { "unsupported" }},
            "threads": {"supported": cfg!(target_os = "linux"), "source": if cfg!(target_os = "linux") { "proc_status_Threads" } else { "unsupported" }},
            "tasks": {"supported": true, "source": "application_instrumentation"},
            "queue_waits": {"supported": true, "source": "application_instrumentation"},
            "allocator": {"supported": false, "reason": "no portable allocator telemetry configured"},
            "tracing_spans": {"supported": true, "source": "application_instrumentation"},
            "cpu_profile": {"supported": cfg!(target_os = "linux"), "tool": if cfg!(target_os = "linux") { "perf" } else { "platform tooling required" }},
            "go_pprof": {"supported": false, "replacement": "/debug/diagnostics/profile"},
        })
    }
}

/// One HTTP request lifecycle. `finish` is idempotent.
pub struct RequestMetrics {
    metrics: Metrics,
    observed: AtomicBool,
    streaming: AtomicBool,
    request_bytes: AtomicU64,
    finished: AtomicBool,
}

impl RequestMetrics {
    pub fn observe(&self, streaming: bool, request_body_bytes: usize) {
        self.observed.store(true, Ordering::Relaxed);
        self.streaming.store(streaming, Ordering::Relaxed);
        self.request_bytes
            .store(request_body_bytes as u64, Ordering::Relaxed);
    }

    pub fn finish(&self, status: u16, response_body_bytes: u64, result: &str) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = &self.metrics.inner;
        inner.active.fetch_sub(1, Ordering::Relaxed);
        inner.completed.fetch_add(1, Ordering::Relaxed);
        if self.observed.load(Ordering::Relaxed) {
            if self.streaming.load(Ordering::Relaxed) {
                inner.streaming.fetch_add(1, Ordering::Relaxed);
            } else {
                inner.buffered.fetch_add(1, Ordering::Relaxed);
            }
        }
        inner.request_bytes.fetch_add(
            self.request_bytes.load(Ordering::Relaxed),
            Ordering::Relaxed,
        );
        inner
            .response_bytes
            .fetch_add(response_body_bytes, Ordering::Relaxed);
        if status >= 500 {
            inner.server_errors.fetch_add(1, Ordering::Relaxed);
        } else if status >= 400 {
            inner.client_errors.fetch_add(1, Ordering::Relaxed);
        } else {
            inner.ok_responses.fetch_add(1, Ordering::Relaxed);
        }
        self.metrics.record_bucket(
            self.metrics.now(),
            status >= 400 || (!result.is_empty() && result != "completed"),
        );
    }
}

impl Drop for RequestMetrics {
    fn drop(&mut self) {
        self.finish(499, 0, "disconnected");
    }
}

pub struct TaskGuard {
    metrics: Metrics,
}
impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.metrics
            .inner
            .tasks_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}
pub struct SpanGuard {
    metrics: Metrics,
}
impl Drop for SpanGuard {
    fn drop(&mut self) {
        self.metrics
            .inner
            .spans_active
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
fn proc_status_value(name: &str) -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix(name)?.trim().strip_suffix("kB")?.trim();
            value.parse::<u64>().ok().map(|kb| kb * 1024)
        })
}
#[cfg(not(target_os = "linux"))]
fn proc_status_value(_name: &str) -> Option<u64> {
    None
}
fn peak_rss_bytes() -> Option<u64> {
    proc_status_value("VmHWM:")
}

fn thread_snapshot() -> Value {
    #[cfg(target_os = "linux")]
    {
        let count = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("Threads:")?.trim().parse::<u64>().ok())
            });
        json!({"count":count,"supported":count.is_some(),"source":"proc_status_Threads"})
    }
    #[cfg(not(target_os = "linux"))]
    {
        json!({"count":Value::Null,"supported":false,"reason":"thread count is not implemented on this platform"})
    }
}

/// Bound, unauthenticated diagnostics listener. Binding is restricted to a
/// literal loopback address because the address itself is the trust boundary.
#[derive(Debug)]
pub struct DiagnosticsListener {
    listener: tokio::net::TcpListener,
    metrics: Arc<Metrics>,
}

impl DiagnosticsListener {
    pub async fn bind(address: &str, metrics: Arc<Metrics>) -> io::Result<Self> {
        Self::bind_with(address, metrics, false).await
    }

    /// Bind with the process `SO_REUSEPORT` flag applied — the Go pprof
    /// listener follows the main listener's reuseport switch so a handoff
    /// peer can bind the diagnostic port while the old instance drains.
    pub async fn bind_with(
        address: &str,
        metrics: Arc<Metrics>,
        reuse_port: bool,
    ) -> io::Result<Self> {
        let parsed: SocketAddr = address.parse().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("diagnostic address must be a socket address: {error}"),
            )
        })?;
        if !parsed.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "diagnostic listener must bind a loopback address",
            ));
        }
        let listener = if reuse_port {
            crate::server::lifecycle::bind_listener(parsed, true)?
        } else {
            tokio::net::TcpListener::bind(parsed).await?
        };
        Ok(Self { listener, metrics })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    pub async fn serve(self, shutdown: CancellationToken) -> io::Result<()> {
        let state = self.metrics;
        let router = Router::new()
            .route("/", get(diagnostic_index))
            .route("/debug/diagnostics/runtime", get(runtime_diagnostics))
            .route("/debug/diagnostics/tasks", get(runtime_diagnostics))
            .route("/debug/diagnostics/tracing", get(runtime_diagnostics))
            .route("/debug/diagnostics/profile", get(profile_diagnostics))
            .route("/debug/fgprof", any(legacy_profile))
            .route("/debug/pprof", any(legacy_profile))
            .route("/debug/pprof/", any(legacy_profile))
            .route("/debug/pprof/{*path}", any(legacy_profile))
            .with_state(state);
        axum::serve(self.listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    }
}

fn json_response(status: StatusCode, value: &Value) -> Response {
    let mut response = Response::new(axum::body::Body::from(
        serde_json::to_vec(&value).expect("JSON value serializes"),
    ));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

async fn diagnostic_index() -> Response {
    json_response(
        StatusCode::OK,
        &json!({
            "runtime":"rust", "schema_version":1,
            "authentication":"none; listener is restricted to loopback",
            "endpoints": {
                "runtime":"/debug/diagnostics/runtime",
                "tasks":"/debug/diagnostics/tasks",
                "tracing":"/debug/diagnostics/tracing",
                "profile":"/debug/diagnostics/profile"
            }
        }),
    )
}
async fn runtime_diagnostics(State(metrics): State<Arc<Metrics>>) -> Response {
    json_response(StatusCode::OK, &metrics.diagnostics_snapshot())
}
async fn profile_diagnostics() -> Response {
    let supported = cfg!(target_os = "linux");
    let tool = if supported {
        "perf"
    } else {
        "platform tooling required"
    };
    let commands = if supported {
        vec![
            format!(
                "perf record -F 99 -g -p {} -o cpu.perf.data -- sleep 15",
                std::process::id()
            ),
            "perf report -i cpu.perf.data".to_string(),
        ]
    } else {
        Vec::new()
    };
    json_response(
        StatusCode::OK,
        &json!({
            "runtime":"rust", "pid":std::process::id(),
            "cpu": {"supported":supported, "tool":tool},
            "commands": commands,
            "note":"Run the profiler locally or through an SSH session; this unauthenticated listener stays loopback-only."
        }),
    )
}
async fn legacy_profile() -> Response {
    json_response(
        StatusCode::NOT_IMPLEMENTED,
        &json!({
            "runtime":"rust", "error":"unsupported_go_profile",
            "message":"Go pprof/fgprof profiles do not exist in the Rust runtime",
            "replacement":"/debug/diagnostics/profile"
        }),
    )
}

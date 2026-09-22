//! Request debug logs, history aggregation and retention.
//!
//! Port of `G/internal/debuglog`: per-request stage directories with
//! JSON/JSONL files, a bounded per-request write queue (4096) drained by one
//! worker thread, deferred serialization (thunks evaluate inside the
//! worker), a global `index.jsonl` with tail-cap rewrite, startup replay
//! into the usage aggregator, attachments, and layered retention.
//!
//! Differences forced by Rust rather than by design:
//! - the worker is a `std::thread` per request (Go used a goroutine);
//! - deferred values are `Box<dyn FnOnce() -> LogValue + Send>` — ownership
//!   moves into the worker, so the Go "caller must not mutate captured
//!   data" contract is enforced by the type system;
//! - `Recorder` is a cheap cloneable handle; `Recorder::none()` is the
//!   disabled/null recorder whose methods are no-ops, mirroring Go's
//!   nil-receiver idiom.

pub mod cleaner;
pub mod gojson;
pub mod gotime;
pub mod index;
pub mod projection;
pub mod reader;
pub mod sanitize;
pub mod stages;
pub mod usage;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use jiff::Zoned;

use gojson::Obj;
// Stage/file-name constants arrive via `pub use stages::*` below.

pub use gojson::JVal;
pub use index::{DEFAULT_INDEX_FILE_CAP, IndexEntry, USAGE_REPLAY_TAIL_BYTES, optional_latency};
pub use reader::{
    ActiveRequest, FILE_READ_CAP, ListResult, RequestDetail, RequestFileInfo, RequestFilter,
    detail, is_request_dir_name, list_request_files, read_process_log, read_request_file,
    tail_read, truncate_to_tail, valid_file_rel_path,
};
pub use stages::*;
pub use usage::{
    DimensionAgg, LatencyStats, LatencySummary, RateLimitEvent, USAGE_MIN_BUCKETS,
    USAGE_SAMPLE_CAPACITY, UsageAggregator, UsageDayRow, UsageMinPoint, UsageSnapshot, UsageTotals,
    error_owner, is_rate_limited,
};

/// `writeQueueSize` — per-request write-task queue bound; streaming frames
/// stay well under ten thousand.
pub const QUEUE_DEPTH: usize = 4096;

/// `RetentionPolicy` — request-log lifecycle policy.
#[derive(Debug, Clone, Default)]
pub struct RetentionPolicy {
    /// Whole-dir retention in days; `<=0` disables time cleanup.
    pub days: i64,
    /// `logs/` root size cap in MB; over it the oldest dirs are evicted;
    /// `<=0` disables size cleanup.
    pub max_total_mb: i64,
    /// Hours before bulky stage files (03/04/06 and `attachments/`) are
    /// stripped, keeping meta.json/error.json/01/02 evidence; `<=0`
    /// disables stripping.
    pub payload_hours: i64,
    /// Number of newest failure dirs (with `error.json`) protected from
    /// capacity eviction; `<=0` disables protection.
    pub keep_error_dirs: i64,
}

/// `RequestMeta` — HTTP metadata known when the request log is created.
/// JSON tags match meta.json's client block; `ActiveRequest.meta` uses the
/// same wire shape as completed requests.
#[derive(Debug, Clone, Default)]
pub struct RequestMeta {
    /// HTTP method.
    pub method: String,
    /// HTTP path.
    pub path: String,
    /// Entry protocol (`openai-chat`, `openai-responses`, `responses-ws`,
    /// `anthropic`).
    pub api: String,
    /// Downstream client address (no port).
    pub client_ip: String,
    /// Downstream user agent.
    pub user_agent: String,
    /// SHA-256 prefix (8 bytes hex) of the client credential — correlates
    /// requests per key without persisting it.
    pub key_hash: String,
    /// Client-supplied correlation ID (X-Request-Id / X-Session-Id).
    pub client_request_id: String,
}

impl RequestMeta {
    /// `json.Marshal(meta)` — Go struct field order, `omitempty` on
    /// `api/client_ip/user_agent/key_hash/client_request_id`.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = gojson::ObjWriter::new();
        w.field_str("method", &self.method)
            .field_str("path", &self.path)
            .field_str_nonempty("api", &self.api)
            .field_str_nonempty("client_ip", &self.client_ip)
            .field_str_nonempty("user_agent", &self.user_agent)
            .field_str_nonempty("key_hash", &self.key_hash)
            .field_str_nonempty("client_request_id", &self.client_request_id);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `Completion` — the result summary written to meta.json at request end.
#[derive(Debug, Clone, Default)]
pub struct Completion {
    /// Final HTTP status code.
    pub status_code: i64,
    /// `completed`, `failed` or `disconnected`.
    pub result: String,
    /// Model actually sent upstream (post-alias).
    pub model: String,
    /// Client's original model name (may have hit an alias).
    pub requested_model: String,
    /// Model declared by the upstream response; empty = undeclared.
    pub response_model: String,
    /// Upstream-declared model differs from the requested one.
    pub model_mismatch: bool,
    /// Provider that produced the response.
    pub provider: String,
    /// Whether the request used streaming.
    pub stream: bool,
    /// Upstream trace ID for this call.
    pub upstream_request_id: String,
    /// Final token usage reported upstream; zero on failure/no report.
    pub usage: crate::domain::Usage,
    /// Suspicious normal finish: request ended on a tool result but the
    /// model returned a tool-call-less `end_turn`. Observability only.
    pub premature_end_turn: bool,
}

/// `retryAttempt` — one upstream resend: `attempt` is the body number
/// (2-based), `cause` the trigger (token self-heal / empty-response
/// continuation / transport reopen).
#[derive(Debug, Clone)]
pub struct RetryAttempt {
    pub attempt: i64,
    pub cause: String,
    pub elapsed_ms: i64,
}

impl RetryAttempt {
    fn to_go_json(&self) -> Vec<u8> {
        let mut w = gojson::ObjWriter::new();
        w.field_int("attempt", self.attempt)
            .field_str("cause", &self.cause)
            .field_int("elapsed_ms", self.elapsed_ms);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// A value queued for logging. `Raw` mirrors `json.RawMessage` (verbatim
/// bytes, prescreened); `Tree` is an already-built `JVal`; `Text` is a plain
/// string; `Serde` defers `serde` serialization into the worker (Go's
/// `json.Marshaler` path); `Deferred` defers projection building (Go's
/// `func() any` thunk).
pub enum LogValue {
    /// Verbatim JSON bytes (`json.RawMessage` parity).
    Raw(Vec<u8>),
    /// Pre-built JSON tree.
    Tree(JVal),
    /// Plain string value.
    Text(String),
    /// Serialize in the worker via serde, then walk the tree (Go's default
    /// `any` path: marshal → unmarshal → sanitize).
    Serde(Box<dyn FnOnce() -> Result<Vec<u8>, String> + Send>),
    /// Build the value in the worker (Go's `func() any` thunk).
    Deferred(Box<dyn FnOnce() -> LogValue + Send>),
}

impl LogValue {
    /// Wrap verbatim JSON bytes.
    pub fn raw(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Raw(bytes.into())
    }

    /// Wrap a pre-built tree.
    pub fn tree(value: JVal) -> Self {
        Self::Tree(value)
    }

    /// Wrap a plain string.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// Wrap a serde-serializable value; serialization happens in the worker.
    pub fn serde<T: serde::Serialize + Send + 'static>(value: T) -> Self {
        Self::Serde(Box::new(move || {
            serde_json::to_vec(&value).map_err(|e| e.to_string())
        }))
    }

    /// Wrap a deferred projection thunk.
    pub fn deferred<F>(f: F) -> Self
    where
        F: FnOnce() -> LogValue + Send + 'static,
    {
        Self::Deferred(Box::new(f))
    }
}

impl From<JVal> for LogValue {
    fn from(value: JVal) -> Self {
        Self::Tree(value)
    }
}

/// `evalDeferred` — unwrap one thunk level (Go evaluates `func() any` once).
fn eval_deferred(value: LogValue) -> LogValue {
    match value {
        LogValue::Deferred(f) => f(),
        other => other,
    }
}

/// One-shot latch (`chan struct{}` close semantics).
pub(crate) struct Latch {
    state: Mutex<bool>,
    cond: Condvar,
}

impl Latch {
    fn new() -> Self {
        Self {
            state: Mutex::new(false),
            cond: Condvar::new(),
        }
    }

    fn signal(&self) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = true;
        self.cond.notify_all();
    }

    fn wait(&self) {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*guard {
            guard = self
                .cond
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn wait_timeout(&self, dur: std::time::Duration) -> bool {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if *guard {
                return true;
            }
            let (g, timeout) = self
                .cond
                .wait_timeout(guard, dur)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = g;
            if *guard {
                return true;
            }
            if timeout.timed_out() {
                return false;
            }
        }
    }
}

/// Pause gate for the write worker: open by default; `set_paused(true)`
/// makes the worker wait before each task (deterministic queue-pressure
/// tests without sleeps).
struct Gate {
    paused: Mutex<bool>,
    cond: Condvar,
}

impl Gate {
    fn new() -> Self {
        Self {
            paused: Mutex::new(false),
            cond: Condvar::new(),
        }
    }

    fn set(&self, paused: bool) {
        let mut guard = self
            .paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = paused;
        self.cond.notify_all();
    }

    fn wait(&self) {
        let mut guard = self
            .paused
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *guard {
            guard = self
                .cond
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// usize → i64 with saturation (counters can never legitimately exceed it).
fn to_i64(v: usize) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// u64 → i64 with saturation.
fn u64_to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// `os.WriteFile(path, data, 0o600)` — create-or-truncate with 0600 on
/// creation (Go perm parity).
pub(crate) fn write_file_600(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut file = create_file(path)?;
    file.write_all(data)
}

/// `os.OpenFile(path, O_CREATE|O_TRUNC|O_WRONLY, 0o600)`.
pub(crate) fn create_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// `os.OpenFile(path, O_CREATE|O_APPEND|O_WRONLY, 0o600)`.
fn append_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// `os.MkdirAll(path, 0o700)` helper for the log root / attachments dir.
fn mkdir_all_700(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// `os.Mkdir(path, 0o700)` — single-level create.
fn mkdir_700(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    Ok(())
}

/// `mkdirRequestDir` — create the request dir; when the root was deleted at
/// runtime, recreate the parent and retry once. No preemptive `mkdir_all`:
/// the root is created by `Manager::new`, and a per-request stat is waste.
fn mkdir_request_dir(path: &Path) -> std::io::Result<()> {
    match mkdir_700(path) {
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent()
                && mkdir_all_700(parent).is_ok()
            {
                return mkdir_700(path);
            }
            Err(err)
        }
        other => other,
    }
}

/// `validLogName` — stage file names: no traversal, required extension.
/// Go's `filepath.Base(name) == name` on Linux only rejects `/` separators.
fn valid_log_name(name: &str, extension: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && name != "."
        && name != ".."
        && name.ends_with(extension)
}

/// Index-file state guarded by `index_mu` (Go `indexMu`): the lazy-opened
/// append handle, its buffered writer and the tracked size.
#[derive(Default)]
struct IndexState {
    writer: Option<BufWriter<std::fs::File>>,
    bytes: i64,
}

/// `Manager` — creates per-request recorders under a fixed logs root and
/// owns the global index, the usage aggregator and the background cleaner.
pub struct Manager {
    inner: Arc<ManagerInner>,
    cleaner_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

struct ManagerInner {
    /// Logs root; empty path = debug logging disabled.
    root: PathBuf,
    /// Current time source; tests pin it to verify same-second dir
    /// allocation.
    now: Box<dyn Fn() -> Zoned + Send + Sync>,
    /// Serializes dir-name allocation and `active_dirs` maintenance — only
    /// memory work happens inside; mkdir/index IO stay outside (`index_mu`)
    /// so a stalled disk cannot wedge every queued request's allocation.
    mutex: Mutex<DirState>,
    /// Runtime switch; off → `start` returns a null recorder, existing dirs
    /// unaffected.
    enabled: AtomicBool,
    /// Lifecycle policy; config reload swaps it at runtime.
    policy: Mutex<RetentionPolicy>,
    /// Serializes all `index.jsonl` IO (lazy open/append/flush/truncate)
    /// and the startup-replay snapshot boundary — index IO and dir
    /// allocation take separate locks so index-side disk stalls no longer
    /// block `start`.
    index_mu: Mutex<IndexState>,
    /// Set once the startup replay has taken its index snapshot inside
    /// `index_mu`: lines completed before the boundary are inside the
    /// snapshot and replayed wholesale (live `append_index` skips them);
    /// lines after it count via the live path — each line exactly once.
    index_snapshotted: AtomicBool,
    /// Per-manager index cap (Go's `indexFileCap` var, made per-instance so
    /// parallel tests cannot race the global).
    index_file_cap: AtomicI64,
    /// Cleaner lifecycle; `None` when no retention dimension is enabled.
    /// Set before `inner` is shared — plain field, not interior-mutable.
    cleaner_stop: Option<Arc<Latch>>,
    /// Total write tasks dropped across requests, surfaced by `stats`.
    dropped_total: AtomicU64,
    /// Index and stage-file write failures — the log pipeline's own faults
    /// are not silent.
    io_errors: AtomicU64,
    /// In-memory `index.jsonl` aggregator; replayed at startup, incremented
    /// on completion.
    usage: UsageAggregator,
    /// Signaled when startup replay ends; `usage_stats` waits on it rather
    /// than returning half-built data.
    replay_done: Latch,
    /// `list_requests` tail-window parse cache.
    list_cache: Mutex<reader::ListIndexCache>,
}

struct DirState {
    /// Dir name → recorder for in-flight requests; the cleaner must skip
    /// these and `active_requests` reads live snapshots from them.
    active: BTreeMap<String, Arc<Shared>>,
    /// Names this process knows exist on disk but are not active (leftover
    /// dirs hit via mkdir EEXIST) — skipped during name allocation so a
    /// same-second restart does not collide repeatedly.
    taken: BTreeSet<String>,
}

impl Manager {
    /// `NewManager` — a manager writing under `root`; an empty path returns
    /// a disabled manager. `policy` drives the background cleaner (any
    /// enabled dimension starts it). Startup replays the `index.jsonl` tail
    /// asynchronously — `usage_stats` blocks on the read side until replay
    /// finishes instead of returning partial data.
    pub fn new(root: impl Into<PathBuf>, policy: &RetentionPolicy) -> Self {
        Self::with_clock(root, policy, Box::new(gotime::now))
    }

    /// `NewManager` with an injectable clock (Go's `manager.now` seam).
    pub fn with_clock(
        root: impl Into<PathBuf>,
        policy: &RetentionPolicy,
        now: Box<dyn Fn() -> Zoned + Send + Sync>,
    ) -> Self {
        let root = root.into();
        let want_cleaner = !root.as_os_str().is_empty()
            && (policy.days > 0 || policy.max_total_mb > 0 || policy.payload_hours > 0);
        let cleaner_stop = want_cleaner.then(|| Arc::new(Latch::new()));
        let inner = Arc::new(ManagerInner {
            root: root.clone(),
            now,
            mutex: Mutex::new(DirState {
                active: BTreeMap::new(),
                taken: BTreeSet::new(),
            }),
            enabled: AtomicBool::new(true),
            policy: Mutex::new(policy.clone()),
            index_mu: Mutex::new(IndexState::default()),
            index_snapshotted: AtomicBool::new(false),
            index_file_cap: AtomicI64::new(DEFAULT_INDEX_FILE_CAP),
            cleaner_stop,
            dropped_total: AtomicU64::new(0),
            io_errors: AtomicU64::new(0),
            usage: UsageAggregator::new(),
            replay_done: Latch::new(),
            list_cache: Mutex::new(reader::ListIndexCache::default()),
        });
        let mut cleaner_handle = None;
        if root.as_os_str().is_empty() {
            inner.replay_done.signal();
        } else {
            // Create the root eagerly: top-level files (quota.jsonl,
            // stderr.log) do not go through `start`'s lazy mkdir.
            if let Err(err) = mkdir_all_700(&root) {
                tracing::warn!(root = %root.display(), error = %err, "debuglog: create log root failed");
            }
            let replay_inner = Arc::clone(&inner);
            std::thread::Builder::new()
                .name("debuglog-replay".into())
                .spawn(move || replay_index(&replay_inner))
                .ok();
            if let Some(stop) = &inner.cleaner_stop {
                let weak = Arc::downgrade(&inner);
                let stop_flag = Arc::clone(stop);
                cleaner_handle = std::thread::Builder::new()
                    .name("debuglog-cleaner".into())
                    .spawn(move || run_cleaner(&weak, &stop_flag))
                    .ok();
            }
        }
        Self {
            inner,
            cleaner_handle: Mutex::new(cleaner_handle),
        }
    }

    /// `Close` — stop the cleaner and close the index handle; call once
    /// before process exit.
    pub fn close(&self) {
        if let Some(stop) = &self.inner.cleaner_stop {
            stop.signal();
        }
        if let Some(handle) = self
            .cleaner_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
        let mut index = self
            .inner
            .index_mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(mut writer) = index.writer.take()
            && writer.flush().is_err()
        {
            self.inner.io_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `SetEnabled` — runtime toggle; off → new requests create no dirs,
    /// history stays queryable.
    pub fn set_enabled(&self, enabled: bool) {
        self.inner.enabled.store(enabled, Ordering::Relaxed);
    }

    /// `Enabled` — whether request logging is on. A rootless manager can
    /// never write and reports disabled (healthz reads this).
    pub fn enabled(&self) -> bool {
        self.inner.enabled.load(Ordering::Relaxed) && !self.inner.root.as_os_str().is_empty()
    }

    /// `SetPolicy` — swap the lifecycle policy at runtime (config reload);
    /// the cleaner's next tick uses it.
    pub fn set_policy(&self, policy: RetentionPolicy) {
        *self
            .inner
            .policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
    }

    /// `Policy` — the currently effective policy snapshot.
    pub fn policy(&self) -> RetentionPolicy {
        self.inner
            .policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// `Root` — the logs root; empty when disabled. Top-level files like
    /// quota history share this directory.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// `Stats` — the log pipeline's own health: drops, active request dirs,
    /// queue backlog, IO failures.
    pub fn stats(&self) -> JVal {
        let (active, queued) = {
            let dirs = self
                .inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let active = dirs.active.len();
            let queued: usize = dirs
                .active
                .values()
                .map(|r| r.queued.load(Ordering::Relaxed))
                .sum();
            (active, queued)
        };
        let index_bytes = std::fs::metadata(self.inner.root.join(INDEX_FILE))
            .map_or(0, |m| i64::try_from(m.len()).unwrap_or(i64::MAX));
        // bind-failure.json is written by main on listen bind failure;
        // missing/corrupt is not surfaced — the panel only needs "why the
        // last bind failed", and absent means it never happened.
        let bind_failure = std::fs::read(self.inner.root.join(BIND_FAILURE_FILE))
            .ok()
            .filter(|data| reader::json_valid(data));
        let policy = self.policy();
        let mut obj = Obj::default()
            .set(
                "log_root",
                JVal::Str(self.inner.root.to_string_lossy().into_owned()),
            )
            .set(
                "enabled",
                JVal::Bool(self.inner.enabled.load(Ordering::Relaxed)),
            )
            .set("active_request_dirs", JVal::Int(to_i64(active)))
            .set("queued_log_events", JVal::Int(to_i64(queued)))
            .set(
                "queue_capacity",
                JVal::Int(to_i64(active.saturating_mul(QUEUE_DEPTH))),
            )
            .set(
                "dropped_log_events",
                JVal::Int(u64_to_i64(self.inner.dropped_total.load(Ordering::Relaxed))),
            )
            .set(
                "io_errors",
                JVal::Int(u64_to_i64(self.inner.io_errors.load(Ordering::Relaxed))),
            )
            .set("index_bytes", JVal::Int(index_bytes))
            .set("retention_days", JVal::Int(policy.days))
            .set("max_total_mb", JVal::Int(policy.max_total_mb))
            .set("payload_hours", JVal::Int(policy.payload_hours))
            .set("keep_error_dirs", JVal::Int(policy.keep_error_dirs));
        if let Some(data) = bind_failure {
            obj = obj.set("last_bind_failure", JVal::Raw(data));
        }
        obj.build()
    }

    /// `UsageStats` — the aggregated `index.jsonl` snapshot (today/window
    /// totals, per-model, per-key, error stages, hourly trend, latency
    /// quantiles). Blocks until startup replay finishes so the panel never
    /// sees partial data.
    pub fn usage_stats(&self) -> UsageSnapshot {
        self.inner.replay_done.wait();
        self.inner.usage.snapshot()
    }

    /// `UsageLatency` — the global latency quantile pair; the 1Hz
    /// `/panel/api/stats` poll consumes only these. Same blocking semantics
    /// as `usage_stats`.
    pub fn usage_latency(&self) -> LatencySummary {
        self.inner.replay_done.wait();
        self.inner.usage.latency_summary()
    }

    /// `Abort` — cancel the named in-flight request; false when the dir is
    /// unknown or not abortable.
    pub fn abort(&self, dir: &str) -> bool {
        if !is_request_dir_name(dir) {
            return false;
        }
        let recorder = {
            self.inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .get(dir)
                .cloned()
        };
        let Some(shared) = recorder else {
            return false;
        };
        abort_shared(&shared)
    }

    /// `Start` — create a per-request log dir named by entry second.
    /// The name is reserved under the lock (written into `active_dirs`);
    /// mkdir happens outside it — a disk stall slows only this request.
    /// EEXIST means a leftover dir this process does not know (same-second
    /// restart); it is recorded in `taken` and the next name is tried.
    pub fn start(&self, meta: &RequestMeta) -> Recorder {
        self.start_inner(meta, false)
    }

    /// Test seam: like `start`, but the write worker is paused before it
    /// spawns — the queue fills deterministically (Go's bare-Recorder test
    /// constructs the channel without a worker).
    #[doc(hidden)]
    pub fn start_paused(&self, meta: &RequestMeta) -> Recorder {
        self.start_inner(meta, true)
    }

    /// Test seam: move the replay-snapshot gate (Go tests poke
    /// `indexSnapshotted` under `indexMu`).
    #[doc(hidden)]
    pub fn debug_set_index_snapshotted(&self, value: bool) {
        let _guard = self
            .inner
            .index_mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.index_snapshotted.store(value, Ordering::Relaxed);
    }

    fn start_inner(&self, meta: &RequestMeta, paused: bool) -> Recorder {
        if self.inner.root.as_os_str().is_empty() || !self.inner.enabled.load(Ordering::Relaxed) {
            return Recorder::none();
        }
        let lock_wait_at = std::time::Instant::now();
        let mut dirs_guard = self
            .inner
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let waited = lock_wait_at.elapsed();
        if waited > std::time::Duration::from_secs(5) {
            // Normally only memory work happens under the lock — waiting
            // this long means a path brought IO back in; warn.
            tracing::warn!(
                waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                "debuglog: dir allocation lock wait exceeded"
            );
        }
        let now = (self.inner.now)();
        let base = gotime::dir_stamp(&now);
        let mut suffix = 1u32;
        loop {
            let name = if suffix > 1 {
                format!("{base}-{suffix:02}")
            } else {
                base.clone()
            };
            if dirs_guard.active.contains_key(&name) || dirs_guard.taken.contains(&name) {
                suffix += 1;
                continue;
            }
            let directory = self.inner.root.join(&name);
            let shared = Arc::new(Shared::new(
                Arc::downgrade(&self.inner),
                directory.clone(),
                now.clone(),
                meta.clone(),
            ));
            if paused {
                shared.pause_gate.set(true);
            }
            dirs_guard.active.insert(name.clone(), Arc::clone(&shared));
            drop(dirs_guard);
            match mkdir_request_dir(&directory) {
                Ok(()) => {
                    let recorder = Recorder {
                        shared: Some(shared),
                    };
                    if !recorder.spawn_writer() {
                        // Thread spawn failure: release the name and count
                        // the IO-class error like a mkdir failure.
                        self.inner
                            .mutex
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .active
                            .remove(&name);
                        self.inner.io_errors.fetch_add(1, Ordering::Relaxed);
                        return Recorder::none();
                    }
                    // meta.json is the first queued write task: "a dir
                    // always has meta" holds while the synchronous write
                    // stays out of the dir-allocation lock.
                    recorder.enqueue(Box::new(|ctx| ctx.write_meta(None)));
                    return recorder;
                }
                Err(err) => {
                    let mut dirs = self
                        .inner
                        .mutex
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    dirs.active.remove(&name);
                    if err.kind() == std::io::ErrorKind::AlreadyExists {
                        dirs.taken.insert(name);
                        suffix += 1;
                        dirs_guard = dirs;
                        continue;
                    }
                    drop(dirs);
                    // mkdir failure → this request silently has no log;
                    // io_errors + warn keep "where did the logs go"
                    // answerable (disk full / permissions).
                    self.inner.io_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(dir = %name, error = %err, "debuglog: create request dir failed");
                    return Recorder::none();
                }
            }
        }
    }

    /// `Detail` — meta.json plus file listing for one request dir.
    pub fn detail(&self, dir: &str) -> std::io::Result<RequestDetail> {
        if self.inner.root.as_os_str().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "disabled",
            ));
        }
        reader::detail(&self.inner.root, dir)
    }

    /// `ReadFile` — a request-dir file, truncated past `FILE_READ_CAP`.
    /// Returns `(data, total_size, truncated)`.
    pub fn read_file(&self, dir: &str, name: &str) -> std::io::Result<(Vec<u8>, i64, bool)> {
        if self.inner.root.as_os_str().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "disabled",
            ));
        }
        reader::read_request_file(&self.inner.root, dir, name)
    }

    /// `ActiveRequests` — live snapshots of dirs still being written.
    pub fn active_requests(&self) -> Vec<ActiveRequest> {
        let recorders: Vec<Arc<Shared>> = {
            self.inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .values()
                .cloned()
                .collect()
        };
        let mut out: Vec<ActiveRequest> = recorders
            .iter()
            .map(|shared| {
                let mut snap = snapshot_shared(shared);
                snap.files = reader::list_request_files(&shared.directory);
                snap
            })
            .collect();
        out.sort_by_key(|a| std::cmp::Reverse(a.started_at.timestamp()));
        out
    }

    /// `ListRequests` — newest `limit` index summaries (newest first).
    pub fn list_requests(&self, limit: usize, filter: &RequestFilter) -> ListResult {
        if self.inner.root.as_os_str().is_empty() {
            return ListResult::default();
        }
        reader::ListIndexCache::list(&self.inner.list_cache, &self.inner.root, limit, filter)
    }

    /// `ReadProcessLog` — `stderr.log` tail plus the next pull offset.
    pub fn read_process_log(&self, offset: i64) -> std::io::Result<(Vec<u8>, i64)> {
        if self.inner.root.as_os_str().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "disabled",
            ));
        }
        reader::read_process_log(&self.inner.root, offset)
    }

    /// `cleanOnce` — one retention pass (the background ticker calls this;
    /// tests call it directly for determinism).
    pub fn clean_once(&self) -> i64 {
        let active: BTreeSet<String> = {
            self.inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .keys()
                .cloned()
                .collect()
        };
        let policy = self.policy();
        cleaner::clean_once(&self.inner.root, &active, &policy)
    }

    /// Test seam: shrink the index cap so truncation paths are reachable
    /// (Go's `indexFileCap` var, per-manager here so parallel tests do not
    /// race a global).
    #[doc(hidden)]
    pub fn set_index_file_cap(&self, cap: i64) {
        self.inner.index_file_cap.store(cap, Ordering::Relaxed);
    }

    /// Test seam: wait for the startup replay to finish.
    #[doc(hidden)]
    pub fn wait_replay(&self) {
        self.inner.replay_done.wait();
    }

    /// `appendIndex` — write the completion summary line to `index.jsonl`.
    /// One flush per line: the index is debugging evidence and a crash must
    /// not lose the tail (flush reaches the kernel page cache — power loss
    /// is out of scope). Serialization happens outside `index_mu`; the lock
    /// only covers the file IO and the snapshot gate.
    fn append_index(inner: &ManagerInner, shared: &Shared, completion: &Completion) {
        let retries = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retries
            .clone();
        let entry = IndexEntry {
            dir: shared
                .directory
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
            started_at: gotime::rfc3339_nano(&shared.started_at),
            duration_ms: gotime::since_ms(&shared.started_at),
            request_ready_ms: optional_latency(shared.request_ready_ms.load(Ordering::Relaxed)),
            upstream_sent_ms: optional_latency(shared.upstream_sent_ms.load(Ordering::Relaxed)),
            upstream_open_ms: optional_latency(shared.upstream_open_ms.load(Ordering::Relaxed)),
            first_upstream_ms: optional_latency(shared.first_upstream_ms.load(Ordering::Relaxed)),
            first_client_ms: optional_latency(shared.first_client_ms.load(Ordering::Relaxed)),
            api: shared.request_meta.api.clone(),
            method: shared.request_meta.method.clone(),
            path: shared.request_meta.path.clone(),
            status_code: completion.status_code,
            result: completion.result.clone(),
            requested_model: completion.requested_model.clone(),
            model: completion.model.clone(),
            response_model: completion.response_model.clone(),
            model_mismatch: completion.model_mismatch,
            stream: completion.stream,
            input_tokens: completion.usage.input,
            output_tokens: completion.usage.output,
            cache_read_tokens: completion.usage.cache_read,
            cache_write_tokens: completion.usage.cache_write,
            reasoning_tokens: completion.usage.reasoning.unwrap_or(0),
            total_tokens: completion.usage.total_tokens,
            upstream_request_id: completion.upstream_request_id.clone(),
            client_ip: shared.request_meta.client_ip.clone(),
            key_hash: shared.request_meta.key_hash.clone(),
            client_request_id: shared.request_meta.client_request_id.clone(),
            error_stage: shared
                .error_stage
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
            dropped_events: shared.dropped.load(Ordering::Relaxed),
            retry_after_seconds: shared.retry_after_seconds.load(Ordering::Relaxed),
            rate_limited: shared.rate_limited.load(Ordering::Relaxed),
            retries: to_i64(retries.len()),
            premature_end_turn: completion.premature_end_turn,
            repairs: shared
                .repairs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map_or(0, super::domain::request::RequestRepairs::total),
        };
        let data = entry.to_go_json();

        let mut index = inner
            .index_mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if index.writer.is_none() {
            if let Ok(file) = append_file(&inner.root.join(INDEX_FILE)) {
                if let Ok(meta) = file.metadata() {
                    index.bytes = meta.len().cast_signed();
                }
                index.writer = Some(BufWriter::new(file));
            } else {
                inner.io_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        let Some(writer) = index.writer.as_mut() else {
            return;
        };
        if writer.write_all(&data).is_err() || writer.write_all(b"\n").is_err() {
            inner.io_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if writer.flush().is_err() {
            inner.io_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        index.bytes += to_i64(data.len()) + 1;
        // Requests completed before the replay snapshot do not count
        // separately: their index line is inside the snapshot and replay
        // counts it wholesale; lines after the boundary are invisible to the
        // snapshot and must count via the live path — the gate counts each
        // line exactly once.
        if inner.index_snapshotted.load(Ordering::Relaxed) {
            inner.usage.add(&entry);
        }
        if index.bytes > inner.index_file_cap.load(Ordering::Relaxed) {
            truncate_index_locked(inner, &mut index);
        }
    }

    /// `releaseDir` — drop the dir from the active set so the cleaner may
    /// reclaim it.
    fn release_dir(inner: &ManagerInner, directory: &Path) {
        let name = directory
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        inner
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&name);
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.close();
    }
}

/// `truncateIndexLocked` — shrink `index.jsonl` to its tail half; the
/// caller holds `index_mu`. Failure only counts `io_errors`: the writer
/// resets to lazy-reopen and appends continue.
fn truncate_index_locked(inner: &ManagerInner, index: &mut IndexState) {
    if let Some(mut writer) = index.writer.take()
        && writer.flush().is_err()
    {
        inner.io_errors.fetch_add(1, Ordering::Relaxed);
    }
    let path = inner.root.join(INDEX_FILE);
    match reader::truncate_to_tail(&path, inner.index_file_cap.load(Ordering::Relaxed) / 2) {
        Ok(kept) => index.bytes = kept,
        Err(_) => {
            inner.io_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Startup replay: snapshot the index tail inside `index_mu`, then feed the
/// lines to the aggregator. The boundary must be drawn under the lock —
/// `append_index` does "write file + conditional count" inside the same
/// `index_mu`, so pre-boundary lines land inside the snapshot (their own
/// counting is skipped by the `index_snapshotted` gate and replayed
/// wholesale) and post-boundary lines count via the live path.
fn replay_index(inner: &Arc<ManagerInner>) {
    let data = {
        let path = inner.root.join(INDEX_FILE);
        let mut result;
        {
            let index_guard = inner
                .index_mu
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            result = reader::tail_read(&path, USAGE_REPLAY_TAIL_BYTES);
            if let Err(err) = &result {
                if err.kind() == std::io::ErrorKind::NotFound {
                    inner.index_snapshotted.store(true, Ordering::Relaxed);
                } else {
                    // One in-place retry on transient IO failure: a
                    // snapshot that was never read but still drops the
                    // gate would let pre-boundary lines be assumed
                    // replayed while the live path skipped them —
                    // permanently lost.
                    drop(index_guard);
                    std::thread::sleep(std::time::Duration::from_millis(200));
                    let index_guard = inner
                        .index_mu
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    result = reader::tail_read(&path, USAGE_REPLAY_TAIL_BYTES);
                    inner.index_snapshotted.store(true, Ordering::Relaxed);
                    drop(index_guard);
                }
            } else {
                inner.index_snapshotted.store(true, Ordering::Relaxed);
            }
        }
        result
    };
    match data {
        Err(err) => {
            // Missing index (first install) is normal; other read failures
            // lose window history and deserve a warning.
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(error = %err, "debuglog: replay index tail failed");
            }
        }
        Ok(data) => {
            // The replay window equals the index cap, so normally the whole
            // file is covered; a file larger than what was read means the
            // cap was exceeded (external append / double writer / constant
            // drift) and the oldest lines are silently invisible to
            // aggregation — warn rather than lose history quietly.
            if let Ok(info) = std::fs::metadata(inner.root.join(INDEX_FILE))
                && usize::try_from(info.len()).unwrap_or(usize::MAX) > data.len()
            {
                tracing::warn!(
                    size = info.len(),
                    replayed_bytes = data.len(),
                    "debuglog: index.jsonl exceeds replay window; oldest entries excluded from usage stats"
                );
            }
            let parsed = inner.usage.replay_lines(&data);
            if parsed > 0 {
                tracing::info!(entries = parsed, "debuglog: replayed request index");
            }
        }
    }
    inner.replay_done.signal();
}

/// `runCleaner` — background retention loop: `clean_once` per tick, exit on
/// stop or when the manager is gone.
fn run_cleaner(inner: &Weak<ManagerInner>, stop: &Latch) {
    loop {
        if stop.wait_timeout(cleaner::CLEANER_INTERVAL) {
            return;
        }
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let active: BTreeSet<String> = {
            inner
                .mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .keys()
                .cloned()
                .collect()
        };
        let policy = inner
            .policy
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let removed = cleaner::clean_once(&inner.root, &active, &policy);
        if removed > 0 {
            tracing::debug!(removed_dirs = removed, "debuglog: cleaned old request logs");
        }
    }
}

/// Shared per-request state reachable from both the request threads and the
/// write worker (Go's mutex/atomic fields on `Recorder`).
pub(crate) struct Shared {
    manager: Weak<ManagerInner>,
    /// This request's log directory.
    pub(crate) directory: PathBuf,
    /// When the HTTP request entered the app.
    started_at: Zoned,
    request_meta: RequestMeta,
    /// Mutex state: closed flag, abort hook, model names, retry list.
    state: Mutex<RecorderState>,

    /// Signaled when the worker drained the queue and closed the files.
    writer_done: Latch,
    /// Pause gate for deterministic queue-pressure tests.
    pause_gate: Gate,
    /// Tasks currently sitting in the channel (Go `len(tasks)`).
    queued: AtomicUsize,
    /// This request's dropped write-task count.
    dropped: AtomicU64,
    /// Panel-initiated abort marker (distinct from client disconnect).
    aborted: AtomicBool,
    /// Bytes already sent to the client.
    client_bytes: AtomicI64,
    /// First-byte latency breakdown markers; -1 = has not happened:
    ///   ready→sent          local projection (validate/sanitize/routing/
    ///                       buildRequest/gate queueing)
    ///   sent→open           upstream stream-establishment round trip
    ///   `open→first_upstream` upstream thinking TTFT
    ///   `first_upstream→first_client` proxy encode+flush
    request_ready_ms: AtomicI64,
    upstream_sent_ms: AtomicI64,
    upstream_open_ms: AtomicI64,
    first_upstream_ms: AtomicI64,
    first_client_ms: AtomicI64,
    /// Upstream rate-limit reset hint in seconds; >0 lands in meta/index.
    retry_after_seconds: AtomicI64,
    /// Rate-limit-terminated marker (upstream 429 or local gate fast-fail).
    rate_limited: AtomicBool,
    /// Silent projection repair counts; `None` = never set (meta omits the
    /// field — "the proxy did not touch it" is itself an answer).
    repairs: Mutex<Option<crate::domain::RequestRepairs>>,
    /// First error's stage name, written by the `write_error` task and read
    /// by `complete` after `writer_done` (happens-after), kept under a
    /// mutex for safety.
    error_stage: Mutex<String>,
    /// Per-kind dedup of reported write failures (`ioErrSeen` in Go —
    /// worker-local there; here shared because `complete` also reports).
    io_err_seen: Mutex<BTreeSet<String>>,
}

struct RecorderState {
    /// `complete` ran: the queue is closed and late enqueues count as drops.
    closed: bool,
    /// The request's cancel hook for panel abort; `None` = not interruptible.
    abort_cancel: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Decoded client-requested model name (in-flight list display).
    requested_model: String,
    /// Post-alias/routing model uid actually sent upstream.
    resolved_model: String,
    /// Upstream resend records (attempt2+), same source as 04's
    /// `retry_attempt` separator lines.
    retries: Vec<RetryAttempt>,
    /// The channel sender; `complete` drops it to close the queue.
    tx: Option<std::sync::mpsc::SyncSender<WriteTask>>,
    /// The channel receiver; moved into the worker thread at spawn.
    rx: Option<std::sync::mpsc::Receiver<WriteTask>>,
}

/// A queued write task; runs serially inside the worker.
type WriteTask = Box<dyn FnOnce(&mut WCtx<'_>) + Send>;

/// Worker-side context: the per-request state only the write worker
/// touches (Go's comment "以下字段仅由写 worker 访问，无需加锁").
pub struct WCtx<'a> {
    shared: &'a Shared,
    /// Per-JSONL-file sequence counters.
    sequences: BTreeMap<String, i64>,
    /// Attachment dedup + numbering.
    attachments: sanitize::AttachmentStore,
    /// Open JSONL files — avoids open/close per frame.
    jsonl_files: BTreeMap<String, BufWriter<std::fs::File>>,
    /// `error.json` keeps only the first error (the earliest failure point
    /// has the most diagnostic value).
    error_written: bool,
}

impl WCtx<'_> {
    fn sanitizer(&mut self) -> sanitize::Sanitizer<'_> {
        sanitize::Sanitizer {
            shared: self.shared,
            attachments: &mut self.attachments,
        }
    }

    /// `writeMeta` — write meta.json: at creation (`completion` = `None`)
    /// the entry time and client metadata; at completion the finish time,
    /// duration, status, result and usage join. Called by the worker and by
    /// `complete` (after `writer_done`).
    // One ordered meta.json field assembly mirroring Go's writeMeta.
    #[allow(clippy::too_many_lines)]
    fn write_meta(&mut self, completion: Option<&Completion>) {
        let shared = self.shared;
        let mut meta = Obj::default()
            .set(
                "started_at",
                JVal::Str(gotime::rfc3339_nano(&shared.started_at)),
            )
            .set("method", JVal::Str(shared.request_meta.method.clone()))
            .set("path", JVal::Str(shared.request_meta.path.clone()));
        if !shared.request_meta.api.is_empty() {
            meta = meta.set("api", JVal::Str(shared.request_meta.api.clone()));
        }
        let mut client = Obj::default();
        if !shared.request_meta.client_ip.is_empty() {
            client = client.set("ip", JVal::Str(shared.request_meta.client_ip.clone()));
        }
        if !shared.request_meta.user_agent.is_empty() {
            client = client.set(
                "user_agent",
                JVal::Str(shared.request_meta.user_agent.clone()),
            );
        }
        if !shared.request_meta.key_hash.is_empty() {
            client = client.set("key_hash", JVal::Str(shared.request_meta.key_hash.clone()));
        }
        if !shared.request_meta.client_request_id.is_empty() {
            client = client.set(
                "request_id",
                JVal::Str(shared.request_meta.client_request_id.clone()),
            );
        }
        let client = client.build();
        let client_nonempty = matches!(&client, JVal::Obj(map) if !map.is_empty());
        if client_nonempty {
            meta = meta.set("client", client);
        }
        let ready = shared.request_ready_ms.load(Ordering::Relaxed);
        if ready >= 0 {
            meta = meta.set("request_ready_ms", JVal::Int(ready));
        }
        let sent = shared.upstream_sent_ms.load(Ordering::Relaxed);
        if sent >= 0 {
            meta = meta.set("upstream_sent_ms", JVal::Int(sent));
        }
        let open = shared.upstream_open_ms.load(Ordering::Relaxed);
        if open >= 0 {
            meta = meta.set("upstream_open_ms", JVal::Int(open));
        }
        let first = shared.first_upstream_ms.load(Ordering::Relaxed);
        if first >= 0 {
            meta = meta.set("first_upstream_ms", JVal::Int(first));
        }
        let first_client = shared.first_client_ms.load(Ordering::Relaxed);
        if first_client >= 0 {
            meta = meta.set("first_client_ms", JVal::Int(first_client));
        }
        let dropped = shared.dropped.load(Ordering::Relaxed);
        if dropped > 0 {
            meta = meta.set("dropped_events", JVal::Int(u64_to_i64(dropped)));
        }
        let retry = shared.retry_after_seconds.load(Ordering::Relaxed);
        if retry > 0 {
            meta = meta.set("retry_after_seconds", JVal::Int(retry));
        }
        if shared.rate_limited.load(Ordering::Relaxed) {
            meta = meta.set("rate_limited", JVal::Bool(true));
        }
        if let Some(repairs) = shared
            .repairs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            // Go marshals the RequestRepairs struct (tagged fields).
            if let Ok(raw) = serde_json::to_vec(repairs) {
                meta = meta.set("repairs", JVal::Raw(raw));
            }
        }
        let retries = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retries
            .clone();
        if !retries.is_empty() {
            meta = meta.set(
                "retry_attempts",
                JVal::Arr(retries.iter().map(|r| JVal::Raw(r.to_go_json())).collect()),
            );
        }
        if let Some(completion) = completion {
            let finished_at = gotime::now();
            meta = meta
                .set("finished_at", JVal::Str(gotime::rfc3339_nano(&finished_at)))
                .set(
                    "duration_ms",
                    JVal::Int(gotime::millis_between(
                        shared.started_at.timestamp(),
                        finished_at.timestamp(),
                    )),
                )
                .set("status_code", JVal::Int(completion.status_code))
                .set("result", JVal::Str(completion.result.clone()))
                .set("model", JVal::Str(completion.model.clone()))
                .set("provider", JVal::Str(completion.provider.clone()))
                .set("stream", JVal::Bool(completion.stream));
            if !completion.requested_model.is_empty() {
                meta = meta.set(
                    "requested_model",
                    JVal::Str(completion.requested_model.clone()),
                );
            }
            if !completion.response_model.is_empty() {
                meta = meta.set(
                    "response_model",
                    JVal::Str(completion.response_model.clone()),
                );
            }
            if completion.model_mismatch {
                meta = meta.set("model_mismatch", JVal::Bool(true));
            }
            if completion.premature_end_turn {
                meta = meta.set("premature_end_turn", JVal::Bool(true));
            }
            if !completion.upstream_request_id.is_empty() {
                meta = meta.set(
                    "upstream_request_id",
                    JVal::Str(completion.upstream_request_id.clone()),
                );
            }
            if completion.usage != crate::domain::Usage::default() {
                meta = meta.set(
                    "usage",
                    Obj::default()
                        .set("input", JVal::Int(completion.usage.input))
                        .set("output", JVal::Int(completion.usage.output))
                        .set("cache_read", JVal::Int(completion.usage.cache_read))
                        .set("cache_write", JVal::Int(completion.usage.cache_write))
                        .set(
                            "reasoning",
                            JVal::Int(completion.usage.reasoning.unwrap_or(0)),
                        )
                        .set("total", JVal::Int(completion.usage.total_tokens))
                        .build(),
                );
            }
        }
        let meta = meta.build();
        let Ok(mut data) = gojson::marshal_indent(&meta) else {
            return;
        };
        data.push(b'\n');
        if let Err(err) = write_file_600(&shared.directory.join(META_FILE), &data) {
            self_note_io_err(shared, "file", &err);
        }
    }

    /// `appendJSONL` — append one serialized record to a JSONL file's
    /// buffer; worker-only.
    fn append_jsonl(&mut self, name: &str, data: &[u8]) {
        if let Err(err) = self.append_jsonl_inner(name, data) {
            self_note_io_err(self.shared, "jsonl", &err);
        }
    }

    fn append_jsonl_inner(&mut self, name: &str, data: &[u8]) -> std::io::Result<()> {
        if !self.jsonl_files.contains_key(name) {
            let file = append_file(&self.shared.directory.join(name))?;
            self.jsonl_files
                .insert(name.to_string(), BufWriter::new(file));
        }
        let writer = self.jsonl_files.get_mut(name).expect("inserted above");
        writer.write_all(data)?;
        writer.write_all(b"\n")
    }

    /// `flushJSONL` — flush every open JSONL buffer; worker-only.
    fn flush_jsonl(&mut self) {
        for writer in self.jsonl_files.values_mut() {
            if let Err(err) = writer.flush() {
                self_note_io_err(self.shared, "jsonl", &err);
            }
        }
    }
}

/// `noteIOErr` — count one write failure per kind per dir into
/// `io_errors` and warn; a persistent failure (full disk) counted per frame
/// would distort the total beyond the real blast radius.
pub(crate) fn self_note_io_err(shared: &Shared, kind: &str, err: &std::io::Error) {
    {
        let mut seen = shared
            .io_err_seen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !seen.insert(kind.to_string()) {
            return;
        }
    }
    if let Some(manager) = shared.manager.upgrade() {
        manager.io_errors.fetch_add(1, Ordering::Relaxed);
    }
    tracing::warn!(
        dir = %shared.directory.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default(),
        kind,
        error = %err,
        "debuglog: write failed"
    );
}

/// `Recorder` — one request's directory, start time and async write queue.
/// Cloneable handle; `Recorder::none()` is the disabled recorder whose
/// methods are no-ops (Go's nil-receiver idiom).
#[derive(Clone)]
pub struct Recorder {
    shared: Option<Arc<Shared>>,
}

impl Shared {
    fn new(
        manager: Weak<ManagerInner>,
        directory: PathBuf,
        started_at: Zoned,
        request_meta: RequestMeta,
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE_DEPTH);
        Self {
            manager,
            directory,
            started_at,
            request_meta,
            state: Mutex::new(RecorderState {
                closed: false,
                abort_cancel: None,
                requested_model: String::new(),
                resolved_model: String::new(),
                retries: Vec::new(),
                tx: Some(tx),
                rx: Some(rx),
            }),
            writer_done: Latch::new(),
            pause_gate: Gate::new(),
            queued: AtomicUsize::new(0),
            dropped: AtomicU64::new(0),
            aborted: AtomicBool::new(false),
            client_bytes: AtomicI64::new(0),
            request_ready_ms: AtomicI64::new(-1),
            upstream_sent_ms: AtomicI64::new(-1),
            upstream_open_ms: AtomicI64::new(-1),
            first_upstream_ms: AtomicI64::new(-1),
            first_client_ms: AtomicI64::new(-1),
            retry_after_seconds: AtomicI64::new(0),
            rate_limited: AtomicBool::new(false),
            repairs: Mutex::new(None),
            error_stage: Mutex::new(String::new()),
            io_err_seen: Mutex::new(BTreeSet::new()),
        }
    }
}

impl Recorder {
    /// The disabled/null recorder: every method is a no-op.
    pub fn none() -> Self {
        Self { shared: None }
    }

    /// Whether this recorder writes (Go's `recorder != nil` checks).
    pub fn is_active(&self) -> bool {
        self.shared.is_some()
    }

    /// `DirectoryPath` — this request's log dir; empty when disabled.
    pub fn directory_path(&self) -> PathBuf {
        self.shared
            .as_ref()
            .map(|s| s.directory.clone())
            .unwrap_or_default()
    }

    /// The request dir name (Go `filepath.Base(recorder.directory)`).
    pub fn dir_name(&self) -> String {
        self.shared
            .as_ref()
            .and_then(|s| {
                s.directory
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
            })
            .unwrap_or_default()
    }

    /// `enqueue` — hand a write task to the worker; drops and counts when
    /// the queue is full or closed. The drop accounting boundary splits at
    /// the moment `closed` is set: drops before it land in
    /// `shared.dropped` and fold into `dropped_total` at `complete`; drops
    /// after it fold directly — late enqueues (e.g. an unjoined pump
    /// thread) cannot land in a field nobody reads anymore.
    fn enqueue(&self, task: WriteTask) {
        let Some(shared) = &self.shared else {
            return;
        };
        let state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            shared.dropped.fetch_add(1, Ordering::Relaxed);
            if let Some(manager) = shared.manager.upgrade() {
                manager.dropped_total.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        let Some(tx) = &state.tx else {
            return;
        };
        match tx.try_send(task) {
            Ok(()) => {
                shared.queued.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                shared.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Block until every write queued before this call has completed.
    ///
    /// This is used at protocol commit boundaries that promise metadata is
    /// observable before a corresponding response frame. The barrier itself
    /// is a worker task, so completion is an exact ordering signal rather
    /// than filesystem polling.
    pub fn flush(&self) {
        let Some(shared) = &self.shared else {
            return;
        };
        let tx = {
            let state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return;
            }
            state.tx.clone()
        };
        let Some(tx) = tx else {
            return;
        };
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
        shared.queued.fetch_add(1, Ordering::Relaxed);
        if tx
            .send(Box::new(move |_| {
                let _ = done_tx.send(());
            }))
            .is_err()
        {
            shared.queued.fetch_sub(1, Ordering::Relaxed);
            return;
        }
        let _ = done_rx.recv();
    }

    /// Spawn the write worker thread. `runWriter` executes tasks serially
    /// (JSONL event order = enqueue order), flushes when the queue drains
    /// (in-flight dirs stay panel-readable in real time), and after the
    /// channel closes drains the rest, flushes and closes all files.
    fn spawn_writer(&self) -> bool {
        let Some(shared) = &self.shared else {
            return false;
        };
        let rx = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rx
            .take();
        let Some(rx) = rx else {
            return false;
        };
        let worker = Arc::clone(shared);
        std::thread::Builder::new()
            .name("debuglog-writer".into())
            .spawn(move || {
                let mut ctx = WCtx {
                    shared: &worker,
                    sequences: BTreeMap::new(),
                    attachments: sanitize::AttachmentStore::default(),
                    jsonl_files: BTreeMap::new(),
                    error_written: false,
                };
                run_writer(&worker, &rx, &mut ctx);
            })
            .is_ok()
    }

    /// `WriteJSON` — queue a stage snapshot; the worker serializes it as
    /// indented JSON. `value` may be `LogValue::Deferred`/`Serde` for
    /// in-worker evaluation.
    pub fn write_json(&self, name: &str, value: LogValue) {
        if self.shared.is_none() || !valid_log_name(name, ".json") {
            return;
        }
        let name = name.to_string();
        self.enqueue(Box::new(move |ctx| {
            let value = ctx.sanitizer().sanitize(eval_deferred(value));
            let Ok(mut data) = gojson::marshal_indent(&value) else {
                return;
            };
            data.push(b'\n');
            if let Err(err) = write_file_600(&ctx.shared.directory.join(&name), &data) {
                self_note_io_err(ctx.shared, "file", &err);
            }
        }));
    }

    /// `AppendJSONL` — append one ordered event to a JSONL file with the
    /// `JSONLRecord` envelope (`seq/time/elapsed_ms/event/data`).
    pub fn append_jsonl(&self, name: &str, event: &str, value: LogValue) {
        if self.shared.is_none() || !valid_log_name(name, ".jsonl") {
            return;
        }
        let name = name.to_string();
        let event = event.to_string();
        self.enqueue(Box::new(move |ctx| {
            let seq = ctx.sequences.get(&name).copied().unwrap_or(0) + 1;
            ctx.sequences.insert(name.clone(), seq);
            let data_value = ctx.sanitizer().sanitize(eval_deferred(value));
            let mut w = gojson::ObjWriter::new();
            w.field_int("seq", seq)
                .field_str("time", &gotime::rfc3339_nano(&gotime::now()))
                .field_int("elapsed_ms", gotime::since_ms(&ctx.shared.started_at))
                .field_str_nonempty("event", &event)
                .field("data", &data_value);
            let Ok(data) = w.finish() else {
                return;
            };
            ctx.append_jsonl(&name, &data);
        }));
    }

    /// `AppendValueJSONL` — append a structured value as a JSONL line
    /// without the event envelope.
    pub fn append_value_jsonl(&self, name: &str, value: LogValue) {
        if self.shared.is_none() || !valid_log_name(name, ".jsonl") {
            return;
        }
        let name = name.to_string();
        self.enqueue(Box::new(move |ctx| {
            let value = ctx.sanitizer().sanitize(eval_deferred(value));
            let Ok(data) = gojson::marshal(&value) else {
                return;
            };
            ctx.append_jsonl(&name, &data);
        }));
    }

    /// `WriteError` — record the failure stage and error summary; only the
    /// first error is kept.
    pub fn write_error(&self, stage: &str, err: &(dyn std::error::Error + Send + Sync)) {
        if self.shared.is_none() {
            return;
        }
        let stage = stage.to_string();
        let message = err.to_string();
        self.enqueue(Box::new(move |ctx| {
            if ctx.error_written {
                return;
            }
            ctx.error_written = true;
            ctx.shared
                .error_stage
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone_from(&stage);
            let elapsed_ms = gotime::since_ms(&ctx.shared.started_at);
            let value = ctx.sanitizer().sanitize(LogValue::Tree(
                Obj::default()
                    .set("stage", JVal::Str(stage))
                    .set("message", JVal::Str(message))
                    .set("elapsed_ms", JVal::Int(elapsed_ms))
                    .build(),
            ));
            let Ok(mut data) = gojson::marshal_indent(&value) else {
                return;
            };
            data.push(b'\n');
            if let Err(err) = write_file_600(&ctx.shared.directory.join(ERROR_FILE), &data) {
                self_note_io_err(ctx.shared, "file", &err);
            }
        }));
    }

    /// `Complete` — close the write queue, wait for the drain, then write
    /// the final meta.json, append the index line and release the dir's
    /// cleanup protection. Idempotent: a second call returns immediately —
    /// otherwise meta and the index line would land twice.
    pub fn complete(&self, mut completion: Completion) {
        let Some(shared) = &self.shared else {
            return;
        };
        {
            let mut state = shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return;
            }
            state.closed = true;
            state.abort_cancel = None;
            // Dropping the sender closes the channel (Go `close(tasks)`).
            state.tx.take();
            // The fold must happen inside the lock: late enqueues past
            // `closed` self-fold via `enqueue`'s closed branch; folding
            // outside would double-count drops in the window.
            if let Some(manager) = shared.manager.upgrade() {
                manager
                    .dropped_total
                    .fetch_add(shared.dropped.load(Ordering::Relaxed), Ordering::Relaxed);
            }
        }
        if shared.aborted.load(Ordering::Relaxed) && completion.result == "disconnected" {
            completion.result = "aborted".to_string();
        }
        shared.writer_done.wait();
        let mut ctx = WCtx {
            shared,
            sequences: BTreeMap::new(),
            attachments: sanitize::AttachmentStore::default(),
            jsonl_files: BTreeMap::new(),
            error_written: true,
        };
        ctx.write_meta(Some(&completion));
        if let Some(manager) = shared.manager.upgrade() {
            Manager::append_index(&manager, shared, &completion);
            Manager::release_dir(&manager, &shared.directory);
        }
    }

    /// `NoteRequestReady` — request body decoded + projected, the pump is
    /// about to call the adapter; everything before is the entry segment
    /// (body read + JSON decode + message projection).
    pub fn note_request_ready(&self) {
        if let Some(s) = &self.shared {
            s.request_ready_ms
                .compare_exchange(
                    -1,
                    gotime::since_ms(&s.started_at),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
    }

    /// `NoteUpstreamSend` — the first upstream RPC actually hit the wire
    /// (idempotent, first call wins). The delta from `request_ready` is the
    /// adapter conversion time including local rate-gate queueing.
    pub fn note_upstream_send(&self) {
        if let Some(s) = &self.shared {
            s.upstream_sent_ms
                .compare_exchange(
                    -1,
                    gotime::since_ms(&s.started_at),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
    }

    /// `NoteUpstreamOpen` — upstream stream established (response headers
    /// arrived). Delta from `upstream_sent` is the round trip; delta to
    /// `first_upstream` is the real upstream thinking TTFT.
    pub fn note_upstream_open(&self) {
        if let Some(s) = &self.shared {
            s.upstream_open_ms
                .compare_exchange(
                    -1,
                    gotime::since_ms(&s.started_at),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
    }

    /// `NoteUpstreamLatency` — first upstream event arrival (idempotent).
    pub fn note_upstream_latency(&self) {
        if let Some(s) = &self.shared {
            s.first_upstream_ms
                .compare_exchange(
                    -1,
                    gotime::since_ms(&s.started_at),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
    }

    /// `NoteClientLatency` — first content byte sent to the client. SSE
    /// keepalive comments do not count — they are link keepalive, not
    /// content.
    pub fn note_client_latency(&self) {
        if let Some(s) = &self.shared {
            s.first_client_ms
                .compare_exchange(
                    -1,
                    gotime::since_ms(&s.started_at),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .ok();
        }
    }

    /// `SetAbort` — attach the request's cancel hook so the panel abort
    /// actually interrupts. Auto-cleared by `complete`.
    pub fn set_abort(&self, cancel: Arc<dyn Fn() + Send + Sync>) {
        let Some(shared) = &self.shared else {
            return;
        };
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closed {
            state.abort_cancel = Some(cancel);
        }
    }

    /// `SetModel` — the decoded client-requested model name.
    pub fn set_model(&self, model: &str) {
        if let Some(shared) = &self.shared {
            shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .requested_model = model.to_string();
        }
    }

    /// `SetResolvedModel` — the model uid actually sent upstream after
    /// alias/routing; in-flight rows render the mapping target immediately.
    pub fn set_resolved_model(&self, model: &str) {
        if let Some(shared) = &self.shared {
            shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .resolved_model = model.to_string();
        }
    }

    /// `AddClientBytes` — bytes already sent to the client (in-flight list
    /// observes outflow rate).
    pub fn add_client_bytes(&self, n: i64) {
        if let Some(shared) = &self.shared
            && n > 0
        {
            shared.client_bytes.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// `SetRetryAfter` — the upstream rate-limit reset hint in seconds
    /// (lands in meta/index so grep/aggregation need not parse error text);
    /// <=0 or non-limit errors are ignored.
    pub fn set_retry_after(&self, seconds: i64) {
        if let Some(shared) = &self.shared
            && seconds > 0
        {
            shared.retry_after_seconds.store(seconds, Ordering::Relaxed);
        }
    }

    /// `SetRateLimited` — mark the request as rate-limit terminated: errors
    /// mapping to 429 (upstream `resource_exhausted` / local gate) set this —
    /// an in-stream limit error still ships HTTP 200, and without the flag
    /// the aggregation layer cannot recognize it.
    pub fn set_rate_limited(&self) {
        if let Some(shared) = &self.shared {
            shared.rate_limited.store(true, Ordering::Relaxed);
        }
    }

    /// `SetRepairs` — projection repair counts; all-zero is not stored so
    /// meta.json omits `repairs` entirely.
    pub fn set_repairs(&self, repairs: crate::domain::RequestRepairs) {
        if let Some(shared) = &self.shared {
            if repairs.total() == 0 {
                return;
            }
            *shared
                .repairs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(repairs);
        }
    }

    /// `NoteRetryAttempt` — one upstream resend and its cause; the caller
    /// writes 04's `retry_attempt` separator line at the same place — both
    /// records share one source.
    pub fn note_retry_attempt(&self, attempt: i64, cause: &str) {
        if let Some(shared) = &self.shared {
            shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retries
                .push(RetryAttempt {
                    attempt,
                    cause: cause.to_string(),
                    elapsed_ms: gotime::since_ms(&shared.started_at),
                });
        }
    }

    /// `Abort` — mark aborted and invoke the attached cancel hook; false
    /// when nothing is attached or the request already completed.
    pub fn abort(&self) -> bool {
        let Some(shared) = &self.shared else {
            return false;
        };
        abort_shared(shared)
    }

    /// Test seam: pause/resume the write worker before its next task —
    /// deterministic queue-pressure tests without sleeps.
    #[doc(hidden)]
    pub fn set_writer_paused(&self, paused: bool) {
        if let Some(shared) = &self.shared {
            shared.pause_gate.set(paused);
        }
    }

    /// Test seam: this request's dropped-task count.
    #[doc(hidden)]
    pub fn dropped(&self) -> u64 {
        self.shared
            .as_ref()
            .map_or(0, |s| s.dropped.load(Ordering::Relaxed))
    }

    /// Test seam: tasks still queued (not yet received by the worker).
    #[doc(hidden)]
    pub fn queued(&self) -> usize {
        self.shared
            .as_ref()
            .map_or(0, |s| s.queued.load(Ordering::Relaxed))
    }
}

/// `Recorder.Abort` on shared state (manager-level abort resolves the
/// recorder first).
fn abort_shared(shared: &Shared) -> bool {
    let cancel = {
        shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .abort_cancel
            .clone()
    };
    let Some(cancel) = cancel else {
        return false;
    };
    shared.aborted.store(true, Ordering::Relaxed);
    cancel();
    true
}

/// `snapshot` — a live snapshot of an in-flight request: queue backlog,
/// first-byte timing, stage state, model. State is three-tier:
/// `waiting_upstream` → `receiving_upstream` → `streaming_client`.
fn snapshot_shared(shared: &Shared) -> ActiveRequest {
    let (model, resolved, retries, last_retry_cause, abortable) = {
        let state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.requested_model.clone(),
            state.resolved_model.clone(),
            to_i64(state.retries.len()),
            state
                .retries
                .last()
                .map(|r| r.cause.clone())
                .unwrap_or_default(),
            state.abort_cancel.is_some(),
        )
    };
    let first_upstream = optional_latency(shared.first_upstream_ms.load(Ordering::Relaxed));
    let state = if shared.first_client_ms.load(Ordering::Relaxed) >= 0 {
        "streaming_client"
    } else if first_upstream.is_some() {
        "receiving_upstream"
    } else {
        "waiting_upstream"
    };
    ActiveRequest {
        dir: shared
            .directory
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        meta: shared.request_meta.clone(),
        model,
        resolved_model: resolved,
        retries,
        last_retry_cause,
        started_at: shared.started_at.clone(),
        elapsed_ms: gotime::since_ms(&shared.started_at),
        state: state.to_string(),
        first_upstream_ms: first_upstream,
        client_bytes: shared.client_bytes.load(Ordering::Relaxed),
        queued_events: to_i64(shared.queued.load(Ordering::Relaxed)),
        dropped_events: shared.dropped.load(Ordering::Relaxed),
        abortable,
        files: Vec::new(),
    }
}

/// `runWriter` — the per-request write worker: serial task execution keeps
/// JSONL order identical to enqueue order; an emptied queue flushes buffers
/// (in-flight dirs stay panel-readable); after close it drains the rest,
/// flushes and closes all JSONL files.
fn run_writer(shared: &Shared, rx: &std::sync::mpsc::Receiver<WriteTask>, ctx: &mut WCtx<'_>) {
    while let Ok(task) = rx.recv() {
        shared.queued.fetch_sub(1, Ordering::Relaxed);
        shared.pause_gate.wait();
        task(ctx);
        // A stale queue-length read is harmless: one extra task seen just
        // means one fewer flush — the final flush on close covers it.
        if shared.queued.load(Ordering::Relaxed) == 0 {
            ctx.flush_jsonl();
        }
    }
    for (_, mut writer) in std::mem::take(&mut ctx.jsonl_files) {
        if let Err(err) = writer.flush() {
            self_note_io_err(shared, "jsonl", &err);
        }
    }
    shared.writer_done.signal();
}

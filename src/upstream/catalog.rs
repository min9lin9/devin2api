//! Model catalog cache, `AssignModel` routing, capability projection,
//! aliases and token-generation repair.
//!
//! Port of `G/internal/adapter/devin/devin.go` (catalog/assign/token
//! sections), `G/internal/app/app.go` (`modelEntry`/`mergeAliases`
//! projection) and the token-repair half of `Stream`.
//!
//! Approved parity exceptions implemented here:
//! - Token generations: a failed attempt may retry once against a newer
//!   generation even when another request performed the refresh; no extra
//!   retry when no new token exists.
//! - The catalog fetch is ONE bounded shared task; leader and followers
//!   independently observe their own cancellation (Go detaches the fetch
//!   via `context.WithoutCancel` but the leader still waits on it — the
//!   Rust port lets the leader return on cancel while the spawned fetch
//!   completes and commits for everyone).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, SystemTime};

use connectrpc::{ConnectError, ErrorCode};
use devin_proto::buffa::MessageField;
use devin_proto::generated::exa::api_server_pb as pb;
use devin_proto::generated::exa::api_server_pb::ApiServerServiceClient;
use serde_json::{Map, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::domain::failure::{Canceled, DeadlineExceeded, Failure, classify};
use crate::domain::request::{Content, Message, RequestMessages};
use crate::upstream::gate::{Gate, GateConfig, GateStats};
use crate::upstream::request::{ClientIdentity, build_metadata, derive_session_ids};
use crate::upstream::transport::{
    SeatClient, TokenSource, TransportConfig, TransportError, UpstreamTransport, build_http_client,
};

/// `modelsCacheTTL` — Go `5 * time.Minute`.
pub const CATALOG_CACHE_TTL: Duration = Duration::from_secs(300);
/// `catalogRetryBackoff` — cooldown after a catalog fetch failure whose
/// error carries no reset hint; a hint cools down for the hinted duration
/// (upstream knows best when it lifts).
pub const CATALOG_RETRY_BACKOFF: Duration = Duration::from_secs(30);
/// Bound on the (router uid, cascade id) assignment cache — session keys
/// accumulate over uptime; at the cap the table clears and sessions
/// re-resolve (Go `assignments` behavior).
const ASSIGNMENT_CACHE_CAP: usize = 4096;

/// `adapter.ModelInfo` — one catalog entry in the `OpenAI` `/v1/models`
/// shape.
// The capability bools mirror the upstream catalog schema one-for-one.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ModelInfo {
    /// Model identifier (`OpenAI` model id / Devin `model_uid`).
    pub id: String,
    /// Catalog entry Unix-seconds timestamp; 0 when unknown.
    pub created: i64,
    /// Owning party display name.
    pub owned_by: String,
    /// Multimodal image input support; false when the catalog is silent.
    pub supports_images: bool,
    /// Tool-call support; false when undeclared.
    pub supports_tool_calls: bool,
    /// Same-turn parallel tool calls; false when undeclared.
    pub supports_parallel_tool_calls: bool,
    /// The model produces thinking content.
    pub supports_thinking: bool,
    /// Thinking must be replayed verbatim in later turns.
    pub preserve_thinking: bool,
    /// The uid is an upstream router, not a concrete model: direct
    /// `GetChatMessage` is refused `unavailable`; resolve via `AssignModel`.
    pub is_model_router: bool,
    /// Upstream-declared context window tokens; 0 when unknown.
    pub context_tokens: i64,
    /// Upstream-declared per-response output cap; 0 when unknown.
    pub max_output_tokens: i64,
    /// Non-empty marks this entry a client alias, not a real uid: requests
    /// under this id are rewritten to `alias_of` and inherit the target's
    /// capability bits.
    pub alias_of: String,
}

/// `devin.Config` — the adapter's fixed upstream configuration.
#[derive(Clone, Default)]
pub struct AdapterConfig {
    /// Devin Connect service base URL.
    pub base_url: String,
    /// Devin session token; never written to logs.
    pub token: String,
    /// Devin chat model uid.
    pub model: String,
    /// Optional HTTP/HTTPS/SOCKS5 proxy; empty means direct or env.
    pub proxy: String,
    /// Force HTTP/1.1, one connection per request.
    pub force_http1: bool,
    /// Client model name → upstream uid map (`devin.aliases`), normalized
    /// at config load.
    pub aliases: BTreeMap<String, String>,
    /// `metadata.extension_name`/`ide_name` override.
    pub client_name: String,
    /// `metadata.extension_version`/`ide_version` override.
    pub client_version: String,
    /// `metadata.os` override.
    pub client_os: String,
    /// Rate-gate parameters (`devin.max_rpm`/`gate_*`).
    pub gate: GateConfig,
    /// When set, the cooldown latch deadline persists here across restarts.
    pub gate_state_path: Option<PathBuf>,
    /// Optional credential re-read callback for unauthenticated repair:
    /// the Devin CLI renews `credentials.toml`, so a statically cached
    /// token silently expires; the callback re-reads the same source and
    /// an empty return means "no new credentials".
    pub token_source: Option<TokenSource>,
    /// Catalog cache TTL override; `Duration::ZERO` selects
    /// [`CATALOG_CACHE_TTL`] (Go `modelsCacheTTL` is a struct field, not a
    /// config key — same here).
    pub catalog_cache_ttl: Duration,
    /// Extra PEM roots (test seam; production passes none).
    pub extra_root_pems: Vec<Vec<u8>>,
}

impl AdapterConfig {
    /// Build from the `devin` config section — the Rust equivalent of
    /// `devin.New(cfg.Devin, ...)`.
    #[must_use]
    pub fn from_devin(
        devin: &crate::config::DevinConfig,
        token_source: Option<TokenSource>,
        gate_state_path: Option<PathBuf>,
    ) -> Self {
        Self {
            base_url: devin.base_url.clone(),
            token: devin.token.clone(),
            model: devin.model.clone(),
            proxy: devin.proxy.clone(),
            force_http1: devin
                .force_http1
                .unwrap_or(crate::config::DEFAULT_FORCE_HTTP1),
            aliases: devin.aliases.clone(),
            client_name: devin.client_name.clone(),
            client_version: devin.client_version.clone(),
            client_os: devin.client_os.clone(),
            gate: GateConfig::from_devin(devin),
            gate_state_path,
            token_source,
            catalog_cache_ttl: Duration::ZERO,
            extra_root_pems: Vec::new(),
        }
    }

    /// The transport-facing projection of this config.
    #[must_use]
    pub fn transport_config(&self) -> TransportConfig {
        TransportConfig {
            base_url: self.base_url.clone(),
            proxy: self.proxy.clone(),
            force_http1: self.force_http1,
            extra_root_pems: self.extra_root_pems.clone(),
        }
    }

    /// The request client identity; empty fields fall back to the captured
    /// CLI defaults at resolve time (`Config.ClientIdentity`).
    #[must_use]
    pub fn client_identity(&self) -> ClientIdentity {
        ClientIdentity {
            name: self.client_name.clone(),
            version: self.client_version.clone(),
            os: self.client_os.clone(),
        }
    }
}

/// `devin.New` construction failures.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// `devin.base_url` empty (Go `errors.New`).
    #[error("devin base URL is required")]
    MissingBaseUrl,
    /// `devin.model` empty (Go `errors.New`).
    #[error("devin model is required")]
    MissingModel,
    /// Transport construction failed (Go `create proxy transport: %w`).
    #[error("create proxy transport: {0}")]
    Transport(#[from] TransportError),
}

/// A token plus its generation. Generations implement the approved
/// concurrent-repair rule: a request that failed `unauthenticated` on
/// generation N may retry once against generation >N — whether the newer
/// token came from its own credential re-read, another request's repair,
/// or a config reload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSnapshot {
    /// The credential itself.
    pub token: String,
    /// Monotonic generation; bumped on every installed token change.
    pub generation: u64,
}

/// The live upstream credential with generation tracking — Go's
/// `tokenMu`-guarded `token` plus the approved generation counter.
/// `reload` serializes credential-source reads: concurrent
/// stale-generation repairs converge to at most one source call, and a
/// request whose generation went stale while it waited on the lock sees
/// the newer generation without calling the source again.
pub struct TokenStore {
    state: RwLock<TokenState>,
    reload: Mutex<()>,
}

struct TokenState {
    token: String,
    generation: u64,
}

impl TokenStore {
    /// A store holding `token` at generation 0.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            state: RwLock::new(TokenState {
                token: token.into(),
                generation: 0,
            }),
            reload: Mutex::new(()),
        }
    }

    /// The current credential snapshot (Go `currentToken` + generation).
    #[must_use]
    pub fn current(&self) -> TokenSnapshot {
        self.state.read().expect("token state poisoned").snapshot()
    }

    /// Install a token from outside the repair path (config reload —
    /// Go `ApplyConfig`); bumps the generation so in-flight stale
    /// requests can retry against it.
    pub fn set(&self, token: String) {
        let mut state = self.state.write().expect("token state poisoned");
        if state.token != token {
            state.token = token;
            state.generation += 1;
        }
    }

    /// Repair after an `unauthenticated` failure on `attempt_generation`:
    /// returns a newer-generation token when one exists — already
    /// installed, or freshly read from `source` (Go `reloadToken`: only a
    /// non-empty, different token counts). `None` means no newer token —
    /// the caller must not retry.
    pub fn repair(
        &self,
        attempt_generation: u64,
        source: Option<&TokenSource>,
    ) -> Option<TokenSnapshot> {
        let current = self.current();
        if current.generation > attempt_generation {
            return Some(current);
        }
        let source = source?;
        let _guard = self.reload.lock().expect("token reload lock poisoned");
        // Re-check under the reload lock: a concurrent repair that
        // completed while this caller waited makes its generation newer —
        // retry against it without a redundant source read.
        let current = self.current();
        if current.generation > attempt_generation {
            return Some(current);
        }
        let token = source().trim().to_string();
        if token.is_empty() {
            tracing::warn!("upstream unauthenticated but TokenSource returned no token");
            return None;
        }
        let mut state = self.state.write().expect("token state poisoned");
        if token == state.token {
            tracing::warn!(
                "upstream unauthenticated and TokenSource returned the same token; \
                 credential refresh did not help"
            );
            return None;
        }
        state.token = token;
        state.generation += 1;
        tracing::info!("reloaded upstream token after unauthenticated error");
        Some(state.snapshot())
    }
}

impl TokenState {
    fn snapshot(&self) -> TokenSnapshot {
        TokenSnapshot {
            token: self.token.clone(),
            generation: self.generation,
        }
    }
}

/// `ListModels` failure surface.
#[derive(Debug, Clone)]
pub enum CatalogError {
    /// The shared fetch failed; the classified upstream failure is the
    /// source (Go `fmt.Errorf("devin GetCliModelConfigs: %w", err)`).
    Fetch(Arc<Failure>),
    /// The caller's own cancellation fired while waiting on the shared
    /// fetch (Go `ctx.Err()`).
    Cancelled,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fetch(inner) => write!(f, "devin GetCliModelConfigs: {inner}"),
            Self::Cancelled => f.write_str("context canceled"),
        }
    }
}

static CANCELED: Canceled = Canceled;

impl Error for CatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Fetch(inner) => Some(inner.as_ref()),
            Self::Cancelled => Some(&CANCELED),
        }
    }
}

/// `resolveModelRouting` result: the upstream uid to send plus the
/// `AssignModel` jwt binding it to this request's cascade id (empty for
/// non-router models).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelRouting {
    /// Resolved upstream model uid.
    pub model: String,
    /// `model_assignment_jwt` for the wire request; empty when the model
    /// is not a router.
    pub jwt: String,
}

/// `resolvedAssignment` — `AssignModel`'s per-(router, cascade) result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedAssignment {
    /// The concrete model uid the router assigned.
    pub model_uid: String,
    /// The cascade-bound assignment jwt.
    pub jwt: String,
}

/// Shared catalog state (Go `modelsMu`-guarded fields).
#[derive(Default)]
struct CatalogState {
    /// Last fetched catalog; `None` until the first success.
    models: Option<Arc<Vec<ModelInfo>>>,
    /// Cache expiry instant.
    expiry: Option<SystemTime>,
    /// Failure-cooldown deadline: while armed, the catalog never hits
    /// upstream — stale cache if present, else the recorded error.
    retry_until: Option<SystemTime>,
    /// The last fetch error, served during cooldown on an empty cache.
    last_err: Option<Arc<Failure>>,
    /// In-flight shared fetch. Only a `Receiver` is stored — the `Sender`
    /// lives exclusively inside the fetch task, so a panicking task closes
    /// the channel and followers elect a new leader instead of hanging on
    /// a permanently-open dead slot. `seq` lets a follower clear a dead
    /// slot without racing a newer fetch; `waiters` counts parked
    /// followers for diagnostics.
    fetch: Option<FetchSlot>,
    fetch_seq: u64,
}

struct FetchSlot {
    seq: u64,
    rx: watch::Receiver<()>,
    waiters: Arc<std::sync::atomic::AtomicUsize>,
}

/// What the fetch task committed, returned to the leader via its join
/// handle (Go returns the fetch result to the leader directly).
struct FetchOutcome {
    result: Result<Arc<Vec<ModelInfo>>, CatalogError>,
    /// Stale cache present at commit time — served on fetch failure.
    stale: Option<Arc<Vec<ModelInfo>>>,
}

struct AdapterInner {
    /// Hot-swapped config snapshot (Go `configMu`/`currentConfig`).
    config: RwLock<Arc<AdapterConfig>>,
    tokens: Arc<TokenStore>,
    /// Re-evaluated per attempt by the transport (Go `tokenFunc`).
    token_source: TokenSource,
    /// `streamClient` — no whole-call deadline (SSE long connections).
    pub(crate) stream_client: ApiServerServiceClient<UpstreamTransport>,
    /// `apiClient` — 610s whole-call deadline for unary RPCs.
    pub(crate) api_client: ApiServerServiceClient<UpstreamTransport>,
    /// Shared proxy-capable client for the Seat endpoint (Bearer auth,
    /// windsurf identity — distinct from the chat Basic/chisel shape).
    http_client: reqwest::Client,
    catalog: Mutex<CatalogState>,
    cache_ttl: Duration,
    /// Per-process dedup for "model absent from catalog" warnings.
    warned_absent: Mutex<HashSet<String>>,
    /// Upstream message rate gate (task 10).
    pub(crate) gate: Gate,
    /// (router uid, cascade id) → resolved assignment (bounded).
    assignments: Mutex<HashMap<String, ResolvedAssignment>>,
    /// Clock for TTL/cooldown/`created` — injectable for deterministic
    /// tests (Go tests pin `time.Now` the same way).
    clock: Mutex<Box<dyn Fn() -> SystemTime + Send + Sync>>,
    /// Monotonic test observations for cancellation seam synchronization.
    retry_backoff_waits: watch::Sender<u64>,
    stream_receive_waits: watch::Sender<u64>,
}

impl AdapterInner {
    fn config(&self) -> Arc<AdapterConfig> {
        self.config.read().expect("config lock poisoned").clone()
    }

    fn catalog_now(&self) -> SystemTime {
        (self.clock.lock().expect("catalog clock poisoned"))()
    }
}

/// The Devin adapter: catalog cache, router resolution, token repair and
/// the shared upstream clients. `Clone` is cheap (inner `Arc`) — the
/// catalog fetch task and every request share one instance.
#[derive(Clone)]
pub struct Adapter {
    inner: Arc<AdapterInner>,
}

/// Per-adapter observation of lifecycle wait boundaries. This is a
/// synchronization seam only: it cannot pause, cancel or otherwise change
/// production behavior.
#[doc(hidden)]
pub struct LifecycleProbe {
    retry_backoff_waits: watch::Receiver<u64>,
    stream_receive_waits: watch::Receiver<u64>,
}

impl LifecycleProbe {
    /// Number of retry-backoff waits entered so far.
    #[must_use]
    pub fn retry_backoff_waits(&self) -> u64 {
        *self.retry_backoff_waits.borrow()
    }

    /// Number of stream-receive waits entered so far.
    #[must_use]
    pub fn stream_receive_waits(&self) -> u64 {
        *self.stream_receive_waits.borrow()
    }

    /// Wait until the adapter has entered at least `target` retry backoffs.
    pub async fn wait_for_retry_backoff(&mut self, target: u64) {
        while *self.retry_backoff_waits.borrow() < target {
            self.retry_backoff_waits
                .changed()
                .await
                .expect("adapter dropped before retry-backoff observation");
        }
    }

    /// Wait until the adapter has entered at least `target` receive waits.
    pub async fn wait_for_stream_receive(&mut self, target: u64) {
        while *self.stream_receive_waits.borrow() < target {
            self.stream_receive_waits
                .changed()
                .await
                .expect("adapter dropped before stream-receive observation");
        }
    }
}

impl Adapter {
    /// `devin.New`: validates required fields, builds the shared HTTP
    /// client and the stream/api Connect clients.
    ///
    /// # Errors
    /// `MissingBaseUrl`/`MissingModel` on empty required fields;
    /// `Transport` on proxy/base-url parse or client build failure.
    pub fn new(config: AdapterConfig) -> Result<Self, AdapterError> {
        Self::new_with_catalog_clock(config, Box::new(SystemTime::now))
    }

    /// Constructor with an injectable catalog clock — the deterministic
    /// test seam for TTL/cooldown arithmetic.
    ///
    /// # Errors
    /// Same as [`Adapter::new`].
    #[doc(hidden)]
    pub fn new_with_catalog_clock(
        config: AdapterConfig,
        clock: Box<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Result<Self, AdapterError> {
        if config.base_url.trim().is_empty() {
            return Err(AdapterError::MissingBaseUrl);
        }
        // The token may be empty: it is a runtime field — unauthenticated
        // repair re-reads the source and config reload can supply it.
        if config.model.trim().is_empty() {
            return Err(AdapterError::MissingModel);
        }
        let transport_cfg = config.transport_config();
        let http = build_http_client(&transport_cfg)?;
        let uri: http::Uri = transport_cfg
            .base_url
            .parse()
            .map_err(|_| TransportError::InvalidBaseUrl(transport_cfg.base_url.clone()))?;
        // Go's client never advertises Connect compression
        // (`connect-accept-encoding` is only sent when CompressionPools is
        // non-empty): an empty registry drops the header.
        let client_config = connectrpc::client::ClientConfig::new(uri)
            .with_compression(connectrpc::compression::CompressionRegistry::new());
        // The transport re-reads the live token per attempt, so
        // credential repair needs no client rebuild (Go tokenFunc).
        let tokens = Arc::new(TokenStore::new(config.token.clone()));
        let token_source: TokenSource = {
            let tokens = tokens.clone();
            Arc::new(move || tokens.current().token)
        };
        let gate = Gate::new(config.gate, config.gate_state_path.clone());
        let cache_ttl = if config.catalog_cache_ttl.is_zero() {
            CATALOG_CACHE_TTL
        } else {
            config.catalog_cache_ttl
        };
        let (retry_backoff_waits, _) = watch::channel(0);
        let (stream_receive_waits, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(AdapterInner {
                config: RwLock::new(Arc::new(config)),
                tokens,
                token_source: token_source.clone(),
                stream_client: ApiServerServiceClient::new(
                    UpstreamTransport::streaming(http.clone(), token_source.clone()),
                    client_config.clone(),
                ),
                api_client: ApiServerServiceClient::new(
                    UpstreamTransport::unary(http.clone(), token_source.clone()),
                    client_config,
                ),
                http_client: http,
                catalog: Mutex::new(CatalogState::default()),
                cache_ttl,
                warned_absent: Mutex::new(HashSet::new()),
                gate,
                assignments: Mutex::new(HashMap::new()),
                clock: Mutex::new(clock),
                retry_backoff_waits,
                stream_receive_waits,
            }),
        })
    }

    /// Observe lifecycle wait entry without changing runtime behavior.
    #[doc(hidden)]
    #[must_use]
    pub fn lifecycle_probe(&self) -> LifecycleProbe {
        LifecycleProbe {
            retry_backoff_waits: self.inner.retry_backoff_waits.subscribe(),
            stream_receive_waits: self.inner.stream_receive_waits.subscribe(),
        }
    }

    pub(crate) fn note_retry_backoff_wait(&self) {
        self.inner
            .retry_backoff_waits
            .send_modify(|count| *count += 1);
    }

    pub(crate) fn note_stream_receive_wait(&self) {
        self.inner
            .stream_receive_waits
            .send_modify(|count| *count += 1);
    }

    /// The current config snapshot (Go `currentConfig`).
    #[must_use]
    pub fn config(&self) -> Arc<AdapterConfig> {
        self.inner.config()
    }

    /// The request client identity with config overrides applied
    /// (unresolved — empty fields fall back at resolve time).
    #[must_use]
    pub fn client_identity(&self) -> ClientIdentity {
        self.config().client_identity()
    }

    /// The live alias map (Go `Aliases()` — for the panel's
    /// absent-target check).
    #[must_use]
    pub fn aliases(&self) -> BTreeMap<String, String> {
        self.config().aliases.clone()
    }

    /// The current credential snapshot (Go `currentToken` + generation).
    #[must_use]
    pub fn current_token(&self) -> TokenSnapshot {
        self.inner.tokens.current()
    }

    /// A function reading the live credential — shared with the panel so
    /// every consumer of this upstream account follows repair results
    /// (Go `TokenFunc`).
    #[must_use]
    pub fn token_func(&self) -> TokenSource {
        self.inner.token_source.clone()
    }

    /// `reloadToken` + the generation rule: after an `unauthenticated`
    /// failure on `attempt_generation`, returns a newer-generation token
    /// when one exists. Concurrent stale-generation callers converge on
    /// one credential-source read; no newer token → `None` → no retry.
    pub fn repair_token(&self, attempt_generation: u64) -> Option<TokenSnapshot> {
        let source = self.config().token_source.clone();
        self.inner
            .tokens
            .repair(attempt_generation, source.as_ref())
    }

    /// The rate gate (task 11 admits every real send through it).
    #[must_use]
    pub fn gate(&self) -> &Gate {
        &self.inner.gate
    }

    /// Gate stats snapshot for the panel (Go `GateStats`).
    #[must_use]
    pub fn gate_stats(&self) -> GateStats {
        self.inner.gate.stats()
    }

    /// The streaming Connect client (task 11's `GetChatMessage` path).
    #[must_use]
    pub fn stream_client(&self) -> &ApiServerServiceClient<UpstreamTransport> {
        &self.inner.stream_client
    }

    /// The unary Connect client (catalog/AssignModel).
    #[must_use]
    pub fn api_client(&self) -> &ApiServerServiceClient<UpstreamTransport> {
        &self.inner.api_client
    }

    /// The Seat endpoint client: shares the proxy-capable HTTP client and
    /// the live token source, but sends `Bearer` auth and windsurf
    /// metadata — the chat path's `Basic token-token` + chisel identity
    /// stays distinct (Go `dashboard.fetchUserStatus`).
    #[must_use]
    pub fn seat_client(&self) -> SeatClient {
        SeatClient::new(
            self.inner.http_client.clone(),
            &self.config().base_url,
            self.inner.token_source.clone(),
        )
    }

    /// Number of callers parked on the in-flight shared fetch.
    #[doc(hidden)]
    #[must_use]
    pub fn catalog_fetch_waiters(&self) -> usize {
        self.inner
            .catalog
            .lock()
            .expect("catalog state poisoned")
            .fetch
            .as_ref()
            .map_or(0, |slot| {
                slot.waiters.load(std::sync::atomic::Ordering::SeqCst)
            })
    }

    /// `ApplyConfig`: hot-apply a new config. Fields read per request
    /// (model, aliases, client_*) and gate params/token swap in place;
    /// transport-baked fields (`base_url/proxy/force_http1`) do not take
    /// effect and are reported as `requires_restart`. Only changed fields
    /// are listed.
    pub fn apply_config(&self, mut next: AdapterConfig) -> (Vec<String>, Vec<String>) {
        let prev = self.config();
        // Runtime fields are not config-owned: the state path carries over.
        next.gate_state_path.clone_from(&prev.gate_state_path);
        let mut applied = Vec::new();
        let mut requires_restart = Vec::new();
        if prev.model != next.model {
            applied.push("devin.model".to_string());
        }
        if prev.aliases != next.aliases {
            applied.push("devin.aliases".to_string());
        }
        if prev.client_name != next.client_name {
            applied.push("devin.client_name".to_string());
        }
        if prev.client_version != next.client_version {
            applied.push("devin.client_version".to_string());
        }
        if prev.client_os != next.client_os {
            applied.push("devin.client_os".to_string());
        }
        if prev.token != next.token {
            self.inner.tokens.set(next.token.clone());
            applied.push("devin.token".to_string());
        }
        self.inner.gate.set_params(next.gate);
        if prev.gate.max_rpm != next.gate.max_rpm {
            applied.push("devin.max_rpm".to_string());
        }
        if prev.gate.max_hold != next.gate.max_hold {
            applied.push("devin.gate_max_hold_seconds".to_string());
        }
        if prev.gate.drip_interval != next.gate.drip_interval {
            applied.push("devin.gate_drip_interval_seconds".to_string());
        }
        if prev.gate.default_latch != next.gate.default_latch {
            applied.push("devin.gate_default_latch_seconds".to_string());
        }
        if prev.gate.window_offset_ns != next.gate.window_offset_ns {
            applied.push("devin.gate_window_offset_seconds".to_string());
        }
        if prev.gate.window_guard != next.gate.window_guard {
            applied.push("devin.gate_window_guard_seconds".to_string());
        }
        if prev.base_url != next.base_url {
            requires_restart.push("devin.base_url".to_string());
        }
        if prev.proxy != next.proxy {
            requires_restart.push("devin.proxy".to_string());
        }
        if prev.force_http1 != next.force_http1 {
            requires_restart.push("devin.force_http1".to_string());
        }
        *self.inner.config.write().expect("config lock poisoned") = Arc::new(next);
        (applied, requires_restart)
    }

    /// `ListModels`: TTL-cached catalog; concurrent misses converge on
    /// ONE bounded shared fetch (spawned, detached from every caller's
    /// cancellation — the leader returns on cancel while the fetch
    /// completes and commits for the followers). Failure arms a cooldown
    /// window: stale cache when present, else the recorded error.
    ///
    /// # Errors
    /// [`CatalogError::Fetch`] on upstream failure with no cache;
    /// [`CatalogError::Cancelled`] when the caller's token fires while
    /// waiting.
    pub async fn list_models(
        &self,
        cancel: &CancellationToken,
    ) -> Result<Arc<Vec<ModelInfo>>, CatalogError> {
        enum Action {
            Wait(
                u64,
                watch::Receiver<()>,
                Arc<std::sync::atomic::AtomicUsize>,
            ),
            Lead(JoinHandle<FetchOutcome>),
        }
        loop {
            let action = {
                let mut state = self.inner.catalog.lock().expect("catalog state poisoned");
                let now = self.inner.catalog_now();
                if let Some(models) = &state.models
                    && state.expiry.is_some_and(|expiry| now < expiry)
                {
                    return Ok(models.clone());
                }
                // Inside the failure cooldown the catalog never hits
                // upstream: stale cache when present, else the last error.
                if let Some(until) = state.retry_until
                    && now < until
                {
                    if let Some(models) = &state.models {
                        return Ok(models.clone());
                    }
                    return Err(CatalogError::Fetch(state.last_err.clone().unwrap_or_else(
                        || Arc::new(Failure::plain("catalog fetch failed")),
                    )));
                }
                if let Some(slot) = &state.fetch {
                    slot.waiters
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Action::Wait(slot.seq, slot.rx.clone(), slot.waiters.clone())
                } else {
                    let (tx, rx) = watch::channel(());
                    state.fetch_seq += 1;
                    state.fetch = Some(FetchSlot {
                        seq: state.fetch_seq,
                        rx,
                        waiters: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    });
                    Action::Lead(tokio::spawn(run_catalog_fetch(self.inner.clone(), tx)))
                }
            };
            match action {
                Action::Wait(seq, mut rx, _waiters) => {
                    tokio::select! {
                        result = rx.changed() => {
                            if result.is_err() {
                                // The fetch task died without committing:
                                // clear the dead slot (seq-guarded) so the
                                // next loop elects a new leader instead of
                                // spinning on a closed channel.
                                let mut state =
                                    self.inner.catalog.lock().expect("catalog state poisoned");
                                if state.fetch.as_ref().is_some_and(|slot| slot.seq == seq) {
                                    state.fetch = None;
                                }
                            }
                            // Committed state (cache or cooldown) is
                            // re-checked on the next iteration.
                        }
                        () = cancel.cancelled() => return Err(CatalogError::Cancelled),
                    }
                }
                Action::Lead(task) => {
                    tokio::select! {
                        outcome = task => {
                            match outcome {
                                Ok(outcome) => match outcome.result {
                                    Ok(models) => return Ok(models),
                                    Err(err) => {
                                        if let Some(stale) = outcome.stale {
                                            if !cancel.is_cancelled() {
                                                tracing::warn!(
                                                    error = %err,
                                                    "model catalog refresh failed; serving stale cache"
                                                );
                                            }
                                            return Ok(stale);
                                        }
                                        return Err(err);
                                    }
                                },
                                Err(join) => {
                                    return Err(CatalogError::Fetch(Arc::new(Failure::plain(
                                        format!("catalog fetch task failed: {join}"),
                                    ))));
                                }
                            }
                        }
                        () = cancel.cancelled() => return Err(CatalogError::Cancelled),
                    }
                }
            }
        }
    }

    /// `ensureCatalog`: best-effort catalog load — router detection,
    /// image capability checks and the absent-model warning all key off
    /// the catalog; a fetch failure is logged and the request proceeds
    /// (upstream adjudicates).
    pub async fn ensure_catalog(&self, cancel: &CancellationToken) {
        if let Err(err) = self.list_models(cancel).await {
            // A dead caller context (disconnect/drain) is noise, not signal.
            if !cancel.is_cancelled() {
                tracing::warn!(error = %err, "model catalog unavailable; router detection skipped");
            }
        }
    }

    /// `resolveModelRouting`: for catalog uids flagged `is_model_router`,
    /// call `AssignModel` to resolve the concrete model uid plus the
    /// cascade-bound assignment jwt — a router uid sent directly only gets
    /// `unavailable: third-party model provider`, a permanent failure
    /// masquerading as transient. Uids the catalog does not cover pass
    /// through for upstream adjudication.
    ///
    /// # Errors
    /// The classified `AssignModel` failure (prefixed `AssignModel(uid)`).
    pub async fn resolve_model_routing(
        &self,
        request: &RequestMessages,
        model: &str,
        cancel: &CancellationToken,
    ) -> Result<ModelRouting, Failure> {
        let is_router = {
            let state = self.inner.catalog.lock().expect("catalog state poisoned");
            state
                .models
                .as_ref()
                .is_some_and(|models| models.iter().any(|m| m.id == model && m.is_model_router))
        };
        if !is_router {
            return Ok(ModelRouting {
                model: model.to_string(),
                jwt: String::new(),
            });
        }
        // The jwt binds cascade_id: it must be the same derived value the
        // wire request carries.
        let (_, cascade_id) = derive_session_ids(request);
        let assignment = self.assign_model(model, &cascade_id, cancel).await?;
        tracing::info!(
            router = model,
            model = %assignment.model_uid,
            "resolved model router via AssignModel"
        );
        Ok(ModelRouting {
            model: assignment.model_uid,
            jwt: assignment.jwt,
        })
    }

    /// `assignModel`: resolve a router uid to a concrete model + jwt via
    /// upstream `AssignModel`, cached per (router uid, cascade id) — the jwt
    /// binds the cascade id, so same-session reuse skips a per-request
    /// resolution round trip.
    ///
    /// # Errors
    /// Classified upstream failure prefixed `AssignModel(uid)`; an empty
    /// assignment is `invalid_argument`.
    pub async fn assign_model(
        &self,
        router_uid: &str,
        cascade_id: &str,
        cancel: &CancellationToken,
    ) -> Result<ResolvedAssignment, Failure> {
        let key = format!("{router_uid}|{cascade_id}");
        if let Some(cached) = self
            .inner
            .assignments
            .lock()
            .expect("assignments poisoned")
            .get(&key)
        {
            return Ok(cached.clone());
        }
        let identity = self.config().client_identity().resolve();
        let request = pb::AssignModelRequest {
            metadata: MessageField::some(build_metadata(
                &self.inner.tokens.current().token,
                &identity.name,
                &identity.version,
                &identity.os,
                366,
            )),
            model_router_uid: Some(router_uid.to_string()),
            cascade_id: Some(cascade_id.to_string()),
            ..Default::default()
        };
        let resp = tokio::select! {
            r = self.inner.api_client.assign_model(request) => r,
            () = cancel.cancelled() => {
                return Err(Failure::plain("context canceled").with_cause(Canceled));
            }
        };
        let resp = match resp {
            Ok(resp) => resp.into_owned(),
            Err(err) => {
                // Attribution goes into `message` — classify recovers the
                // inner record; wrapper text never reaches client-visible
                // wording.
                let failure = classify(&err)
                    .with_cause(err)
                    .prefixed(format!("AssignModel({router_uid})"));
                return Err(failure);
            }
        };
        let model_uid = resp
            .assignment
            .model_uid
            .clone()
            .unwrap_or_default()
            .trim()
            .to_string();
        let jwt = resp.assignment.assignment_jwt.clone().unwrap_or_default();
        if model_uid.is_empty() || jwt.is_empty() {
            return Err(Failure::invalid_argument(format!(
                "AssignModel({router_uid}) returned empty assignment"
            )));
        }
        let resolved = ResolvedAssignment { model_uid, jwt };
        let mut assignments = self.inner.assignments.lock().expect("assignments poisoned");
        if assignments.len() >= ASSIGNMENT_CACHE_CAP {
            assignments.clear();
        }
        assignments.insert(key, resolved.clone());
        Ok(resolved)
    }

    /// `catalogSupportsImages`: the catalog's image-capability bit for
    /// `model`; `None` when the catalog does not cover the uid.
    #[must_use]
    pub fn catalog_supports_images(&self, model: &str) -> Option<bool> {
        let state = self.inner.catalog.lock().expect("catalog state poisoned");
        state
            .models
            .as_ref()?
            .iter()
            .find(|m| m.id == model)
            .map(|m| m.supports_images)
    }

    /// `warnIfModelAbsentFromCatalog`: when the catalog is loaded and the
    /// resolved uid is absent, warn once per uid per process — an alias
    /// pointing at a dead model only earns a vague upstream
    /// `permission_denied`, and this log line is the diagnostic anchor.
    /// An unloaded or empty catalog passes (a configured model may
    /// legitimately be absent).
    pub fn warn_if_model_absent_from_catalog(&self, model: &str) {
        {
            let state = self.inner.catalog.lock().expect("catalog state poisoned");
            let Some(models) = &state.models else {
                return;
            };
            if models.is_empty() || models.iter().any(|m| m.id == model) {
                return;
            }
        }
        if !self
            .inner
            .warned_absent
            .lock()
            .expect("warned set poisoned")
            .insert(model.to_string())
        {
            return;
        }
        tracing::warn!(
            model,
            hint = "check devin.aliases target or bump devin.client_version",
            "model absent from upstream catalog; upstream will likely return a vague permission_denied"
        );
    }

    /// `validateImagesForModel`: locally reject "no-vision model + images"
    /// early with a client-readable error. The catalog's `supports_images`
    /// wins when the uid is covered; uncovered uids fall back to the
    /// prefix heuristic.
    ///
    /// # Errors
    /// `invalid_argument` when the resolved model does not support images.
    pub fn validate_images_for_model(
        &self,
        request: &RequestMessages,
        model: &str,
    ) -> Result<(), Failure> {
        if !request_has_images(request) {
            return Ok(());
        }
        let supported = self
            .catalog_supports_images(model)
            .unwrap_or_else(|| model_likely_supports_images(model));
        if !supported {
            return Err(Failure::invalid_argument(format!(
                "model {model:?} does not support image inputs (supports_images=false); \
                 use a vision-capable model or remove images"
            )));
        }
        Ok(())
    }
}

/// The shared fetch body: runs detached from every caller's cancellation
/// (the unary transport's 610s deadline bounds it — Go
/// `context.WithoutCancel` + `http.Client.Timeout`), commits cache or
/// cooldown under the catalog lock, then wakes subscribers.
async fn run_catalog_fetch(inner: Arc<AdapterInner>, done: watch::Sender<()>) -> FetchOutcome {
    let result = fetch_model_catalog(&inner).await;
    let mut state = inner.catalog.lock().expect("catalog state poisoned");
    let outcome = match result {
        Ok(models) => {
            let models = Arc::new(models);
            state.models = Some(models.clone());
            state.expiry = Some(inner.catalog_now() + inner.cache_ttl);
            state.retry_until = None;
            state.last_err = None;
            FetchOutcome {
                result: Ok(models),
                stale: None,
            }
        }
        Err(err) => {
            // A dead caller context is not an upstream failure: cancel and
            // deadline errors do not arm the cooldown (Go checks the raw
            // error chain, not the classified code).
            let cancel_like = is_cancel_like(&err);
            let failure = Arc::new(classify(&err).with_cause(err));
            if !cancel_like {
                let backoff = if failure.retry_after_seconds > 0 {
                    Duration::from_secs(failure.retry_after_seconds.max(0).cast_unsigned())
                } else {
                    CATALOG_RETRY_BACKOFF
                };
                state.retry_until = Some(inner.catalog_now() + backoff);
                state.last_err = Some(failure.clone());
            }
            FetchOutcome {
                result: Err(CatalogError::Fetch(failure)),
                stale: state.models.clone(),
            }
        }
    };
    state.fetch = None;
    drop(state);
    // Commit before notify: woken followers re-check and only ever see
    // committed state.
    let _ = done.send(());
    outcome
}

/// `fetchModelCatalog`: one `GetCliModelConfigs` pull plus shaping —
/// dedup, configured-model insertion, alias entry merge. Runs off-lock;
/// convergence, cache commit and cooldown live in `list_models`.
// One projection pass mirroring Go's fetchModelCatalog for parity review.
#[allow(clippy::too_many_lines)]
async fn fetch_model_catalog(inner: &AdapterInner) -> Result<Vec<ModelInfo>, ConnectError> {
    let cfg = inner.config();
    let identity = cfg.client_identity().resolve();
    let resp = inner
        .api_client
        .get_cli_model_configs(pb::GetCliModelConfigsRequest {
            metadata: MessageField::some(build_metadata(
                &inner.tokens.current().token,
                &identity.name,
                &identity.version,
                &identity.os,
                0,
            )),
            ..Default::default()
        })
        .await?;
    let resp = resp.into_owned();
    let now = inner
        .catalog_now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    let mut models = Vec::with_capacity(resp.client_model_configs.len());
    let mut seen = HashSet::with_capacity(resp.client_model_configs.len());
    for config in &resp.client_model_configs {
        if config.disabled.unwrap_or(false) {
            continue;
        }
        let mut uid = config.model_uid.clone().unwrap_or_default();
        if uid.is_empty()
            && let Some(pb::__buffa::oneof::exa_codeium_common_pb_model_or_alias::Choice::ModelUid(
                alias_uid,
            )) = &config.model_or_alias.choice
        {
            uid.clone_from(alias_uid);
        }
        if uid.is_empty() || !seen.insert(uid.clone()) {
            continue;
        }
        let provider = config
            .provider
            .unwrap_or(pb::ExaCodeiumCommonPb_ModelProvider::ExaCodeiumCommonPb_ModelProvider_MODEL_PROVIDER_UNSPECIFIED);
        let mut info = ModelInfo {
            id: uid,
            created: now,
            owned_by: owned_by_from_provider(provider),
            supports_images: config.supports_images.unwrap_or(false),
            context_tokens: i64::from(config.max_tokens.unwrap_or(0)),
            ..Default::default()
        };
        if config.model_info.is_set() {
            let model_info = &*config.model_info;
            info.max_output_tokens = i64::from(model_info.max_output_tokens.unwrap_or(0));
            info.is_model_router = model_info.is_model_router.unwrap_or(false);
            if info.context_tokens == 0 {
                info.context_tokens = i64::from(model_info.max_tokens.unwrap_or(0));
            }
            if model_info.model_features.is_set() {
                let features = &*model_info.model_features;
                info.supports_tool_calls = features.supports_tool_calls.unwrap_or(false);
                info.supports_parallel_tool_calls =
                    features.supports_parallel_tool_calls.unwrap_or(false);
                info.supports_thinking = features.supports_thinking.unwrap_or(false);
                info.preserve_thinking = features.preserve_thinking.unwrap_or(false);
                if !info.supports_images {
                    info.supports_images = features.supports_images.unwrap_or(false);
                }
            }
        }
        models.push(info);
    }
    // An explicitly configured model (e.g. gpt5.6) is discoverable and
    // callable even when Devin's list omits it.
    let configured = cfg.model.trim();
    if !configured.is_empty() && !seen.contains(configured) {
        models.push(ModelInfo {
            id: configured.to_string(),
            created: now,
            owned_by: "devin".to_string(),
            // The configured model's image capability is unknowable;
            // assuming support is friendlier.
            supports_images: true,
            ..Default::default()
        });
    }

    // Alias entries join the catalog so `/v1/models` pickers can discover
    // them. "*" is a catch-all matcher, not a nameable model — excluded.
    // Capability bits inherit from the target entry (an aliased request
    // really runs the target); an absent target falls back to the same
    // placeholder policy as the configured model. An alias key colliding
    // with a real uid rewrites that entry into alias_of form — requests
    // under that name are rerouted, so showing the target's capabilities
    // is the truthful behavior.
    let mut by_id: HashMap<String, usize> = models
        .iter()
        .enumerate()
        .map(|(i, m)| (m.id.clone(), i))
        .collect();
    for (name, target) in &cfg.aliases {
        if name == "*" {
            continue;
        }
        let mut entry = ModelInfo {
            id: name.clone(),
            created: now,
            owned_by: "devin".to_string(),
            alias_of: target.clone(),
            supports_images: true,
            ..Default::default()
        };
        if let Some(&i) = by_id.get(target) {
            let t = &models[i];
            entry.supports_images = t.supports_images;
            entry.supports_tool_calls = t.supports_tool_calls;
            entry.supports_parallel_tool_calls = t.supports_parallel_tool_calls;
            entry.supports_thinking = t.supports_thinking;
            entry.preserve_thinking = t.preserve_thinking;
            entry.is_model_router = t.is_model_router;
            entry.context_tokens = t.context_tokens;
            entry.max_output_tokens = t.max_output_tokens;
        }
        if let Some(&i) = by_id.get(name) {
            entry.created = models[i].created;
            entry.owned_by.clone_from(&models[i].owned_by);
            models[i] = entry;
        } else {
            by_id.insert(name.clone(), models.len());
            models.push(entry);
        }
    }
    Ok(models)
}

/// `ownedBy` derivation (Go `fetchModelCatalog`): the provider enum's
/// trailing name segment, lowercased — `MODEL_PROVIDER_OPENAI` →
/// `openai`; absent/unspecified → `unspecified`, non-enum text → `devin`.
fn owned_by_from_provider(provider: pb::ExaCodeiumCommonPb_ModelProvider) -> String {
    let name = format!("{provider:?}");
    match name.rfind('_') {
        Some(i) if i + 1 < name.len() => name[i + 1..].to_lowercase(),
        _ => "devin".to_string(),
    }
}

/// `isUnauthenticated`: the first `ConnectError` in the chain carries
/// `unauthenticated` (credential expired).
#[must_use]
pub fn is_unauthenticated(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(connect_err) = err.downcast_ref::<ConnectError>() {
            return connect_err.code == ErrorCode::Unauthenticated;
        }
        current = err.source();
    }
    false
}

/// The fetcher's own disconnect is not an upstream failure: cancellation
/// and deadline sentinels in the error chain suppress the cooldown (Go
/// `errors.Is(err, context.Canceled/DeadlineExceeded)`).
fn is_cancel_like(err: &ConnectError) -> bool {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(err) = current {
        if err.is::<Canceled>()
            || err.is::<DeadlineExceeded>()
            || err.is::<tokio::time::error::Elapsed>()
        {
            return true;
        }
        // Go's `http.Client.Timeout` failure unwraps to
        // `context.DeadlineExceeded` via `url.Error`; reqwest's timeout
        // marker is the equivalent caller-side deadline, not an upstream
        // refusal — no cooldown.
        if let Some(req_err) = err.downcast_ref::<reqwest::Error>()
            && req_err.is_timeout()
        {
            return true;
        }
        current = err.source();
    }
    false
}

/// `requestHasImages`: any image block in user or tool-result messages —
/// the gate for skipping catalog capability checks on image-free
/// requests.
#[must_use]
pub fn request_has_images(request: &RequestMessages) -> bool {
    for message in &request.messages {
        let content = match message {
            Message::User(typed) => &typed.content,
            Message::ToolResult(typed) => &typed.content,
            Message::Assistant(_) => continue,
        };
        if content
            .iter()
            .any(|block| matches!(block, Content::Image(_)))
        {
            return true;
        }
    }
    false
}

/// `modelLikelySupportsImages`: known no-vision uid prefixes; uncertain
/// uids pass for upstream adjudication. Prefixes carry a boundary (equal
/// or `-`/`_` continuation): a bare prefix would misjudge same-head
/// different-name uids like `o10`.
#[must_use]
pub fn model_likely_supports_images(model: &str) -> bool {
    // Aligned with the common supports_images=false uids from
    // GetCascadeModelConfigs.
    const NO_VISION_PREFIXES: &[&str] = &[
        "glm-5-2",
        "glm-5",
        "glm-4.7",
        "glm-4-7",
        "glm-4",
        "deepseek",
        "kimi-k2",
        "qwen3-coder",
        "o1",
        "o3-mini",
        "o4-mini",
    ];
    let m = model.trim().to_lowercase();
    if m.is_empty() {
        return true;
    }
    for prefix in NO_VISION_PREFIXES {
        if m == *prefix
            || m.starts_with(&format!("{prefix}-"))
            || m.starts_with(&format!("{prefix}_"))
        {
            return false;
        }
    }
    true
}

/// `ResolveModelAlias`: rewrite a client model name to an upstream uid —
/// exact hit → case-folded hit → `"*"` catch-all → passthrough. Aliases
/// arrive normalized from config load (keys trimmed, chains expanded,
/// case duplicates rejected), so the fold fallback is a linear scan over
/// a small table.
#[must_use]
pub fn resolve_model_alias(aliases: &BTreeMap<String, String>, model: &str) -> String {
    if let Some(target) = aliases.get(model) {
        return target.clone();
    }
    for (name, target) in aliases {
        if name.to_lowercase() == model.to_lowercase() {
            return target.clone();
        }
    }
    if let Some(target) = aliases.get("*") {
        return target.clone();
    }
    model.to_string()
}

/// `modelCreatedFallback`: the shared fallback timestamp for catalog
/// entries lacking `created` (process start) — a per-request `now` would
/// let one model's `created` drift between calls.
static MODEL_CREATED_FALLBACK: LazyLock<i64> = LazyLock::new(|| {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
});

/// `modelEntry`: project a catalog entry into the `OpenAI` `/v1/models`
/// shape; list and detail endpoints share the field set. Non-standard
/// fields let the panel/gateway gate requests on capability (including
/// `is_model_router`: a router uid sent directly is refused upstream).
#[must_use]
pub fn model_entry(model: &ModelInfo) -> Map<String, Value> {
    let created = if model.created == 0 {
        *MODEL_CREATED_FALLBACK
    } else {
        model.created
    };
    let owned_by = if model.owned_by.is_empty() {
        "devin"
    } else {
        model.owned_by.as_str()
    };
    let mut entry = Map::new();
    entry.insert("id".to_string(), Value::from(model.id.as_str()));
    entry.insert("object".to_string(), Value::from("model"));
    entry.insert("created".to_string(), Value::from(created));
    entry.insert("owned_by".to_string(), Value::from(owned_by));
    entry.insert(
        "supports_images".to_string(),
        Value::from(model.supports_images),
    );
    entry.insert(
        "supports_tool_calls".to_string(),
        Value::from(model.supports_tool_calls),
    );
    entry.insert(
        "supports_parallel_tool_calls".to_string(),
        Value::from(model.supports_parallel_tool_calls),
    );
    entry.insert(
        "supports_thinking".to_string(),
        Value::from(model.supports_thinking),
    );
    entry.insert(
        "preserve_thinking".to_string(),
        Value::from(model.preserve_thinking),
    );
    entry.insert(
        "is_model_router".to_string(),
        Value::from(model.is_model_router),
    );
    entry.insert(
        "context_tokens".to_string(),
        Value::from(model.context_tokens),
    );
    entry.insert(
        "max_output_tokens".to_string(),
        Value::from(model.max_output_tokens),
    );
    // `alias_of` marks this id a client alias: requests are rewritten to
    // the target uid.
    if !model.alias_of.is_empty() {
        entry.insert("alias_of".to_string(), Value::from(model.alias_of.as_str()));
    }
    entry
}

/// `mergeAliases`: fold `devin.aliases` into the model-list projection —
/// an alias shadowing a real catalog entry annotates it `alias_of` (the
/// name stays reachable but requests reroute; unannotated, the catalog
/// would lie) and copies the target's capability bits; an alias absent
/// from the catalog gets a synthetic entry so discovery surfaces see it.
#[must_use]
pub fn merge_aliases(
    mut data: Vec<Map<String, Value>>,
    aliases: &BTreeMap<String, String>,
) -> Vec<Map<String, Value>> {
    const CAPABILITY_KEYS: &[&str] = &[
        "supports_images",
        "supports_tool_calls",
        "supports_parallel_tool_calls",
        "supports_thinking",
        "preserve_thinking",
    ];
    if aliases.is_empty() {
        return data;
    }
    let mut by_id: HashMap<String, usize> = HashMap::with_capacity(data.len());
    for (i, entry) in data.iter().enumerate() {
        if let Some(id) = entry.get("id").and_then(Value::as_str) {
            by_id.insert(id.to_string(), i);
        }
    }
    for (name, raw_target) in aliases {
        let target = raw_target.trim();
        if target.is_empty() {
            continue;
        }
        let target_caps: Vec<(&'static str, Value)> = by_id
            .get(target)
            .map(|&i| {
                CAPABILITY_KEYS
                    .iter()
                    .filter_map(|key| data[i].get(*key).map(|v| (*key, v.clone())))
                    .collect()
            })
            .unwrap_or_default();
        if let Some(&i) = by_id.get(name.as_str()) {
            // Shadowed name: the entry keeps its catalog slot, but
            // capability bits come from the target that actually runs —
            // the original model's bits would lie.
            data[i].insert("alias_of".to_string(), Value::from(target));
            for (key, value) in target_caps {
                data[i].insert(key.to_string(), value);
            }
            continue;
        }
        let mut entry = Map::new();
        entry.insert("id".to_string(), Value::from(name.as_str()));
        entry.insert("object".to_string(), Value::from("model"));
        entry.insert("created".to_string(), Value::from(*MODEL_CREATED_FALLBACK));
        entry.insert("owned_by".to_string(), Value::from("alias"));
        entry.insert("alias_of".to_string(), Value::from(target));
        for (key, value) in target_caps {
            entry.insert(key.to_string(), value);
        }
        // Go builds `byID` once and never registers appended entries: an
        // alias targeting another alias's synthetic entry gets no
        // capability copy. Keep the same asymmetry.
        data.push(entry);
    }
    data
}

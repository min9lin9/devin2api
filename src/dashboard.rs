//! Admin panel APIs, authentication and the embedded panel assets.
//!
//! The panel pages (login/panel HTML, css, js, vendored libraries) are
//! copied from `internal/dashboard/static` (see `assets/panel/NOTICE.md`
//! for provenance and third-party licenses) and embedded at compile time,
//! matching Go's `go:embed` single-binary deployment. This module owns the
//! authenticated JSON surface and deliberately keeps its data providers
//! injectable so tests and the runtime can use the same handlers.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Query, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, SET_COOKIE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq as _;

use crate::debuglog::{self, IndexEntry, Manager, RequestFilter, error_owner};
use crate::metrics::Metrics;
use crate::upstream::catalog::Adapter;
use crate::upstream::gate::GateStats;
use crate::upstream::transport::build_metadata;
use buffa::MessageField;
use devin_proto::generated::exa::api_server_pb as pb;
use tokio_util::sync::CancellationToken;

const REQUESTS_FETCH_CAP: usize = 2_000;
const LOGIN_MAX_FAILS: u32 = 5;
const LOGIN_LOCKOUT: Duration = Duration::from_mins(10);
const SESSION_LIFETIME: Duration = Duration::from_hours(24);
const SESSION_SWEEP_THRESHOLD: usize = 64;
const QUOTA_HISTORY_CAP: usize = 10_000;

type DataFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// Upstream-backed panel data. The production assembler may provide the full
/// Seat/catalog implementation; handlers preserve the Go response boundary.
pub trait DashboardData: Send + Sync + 'static {
    fn status(&self) -> DataFuture<'_>;
    fn models(&self) -> DataFuture<'_>;
}

/// Production status aggregation: Seat account/plan, capacity, IDE status,
/// model health, providers, and alias-to-catalog validation. Individual RPC
/// failures remain fields in the successful aggregate, exactly like Go.
// One aggregation pass mirroring Go's status assembly for parity review.
#[allow(clippy::too_many_lines)]
pub async fn adapter_status(adapter: &Adapter) -> Value {
    let token = adapter.current_token().token;
    let metadata = || MessageField::some(build_metadata(&token, "windsurf", "1.48.2", "win", 32));
    let client = adapter.api_client();
    let seat_client = adapter.seat_client();
    let seat = seat_client.get_status_root();
    let capacity = client.check_chat_capacity(pb::CheckChatCapacityRequest {
        metadata: metadata(),
        ..Default::default()
    });
    let status = client.get_status(pb::GetStatusRequest {
        metadata: metadata(),
        ..Default::default()
    });
    let model_statuses = client.get_model_statuses(pb::GetModelStatusesRequest {
        metadata: metadata(),
        ..Default::default()
    });
    let providers = client.get_model_providers(pb::GetModelProvidersRequest::default());
    let catalog_cancel = CancellationToken::new();
    let catalog = adapter.list_models(&catalog_cancel);
    let (seat, capacity, status, model_statuses, providers, catalog) =
        tokio::join!(seat, capacity, status, model_statuses, providers, catalog);

    let mut out = Map::new();
    match seat {
        Ok(raw) => {
            if let Value::Object(projected) = project_seat_response(&raw) {
                out.extend(projected);
            }
        }
        Err(error) => {
            out.insert("user_status_error".into(), json!(error.to_string()));
        }
    }
    match capacity {
        Ok(response) => {
            let response = response.into_owned();
            out.insert(
                "capacity".into(),
                json!({
                    "has_capacity": response.has_capacity.unwrap_or(false),
                    "message": response.message.unwrap_or_default(),
                    "active_sessions": response.active_sessions.unwrap_or(0),
                }),
            );
        }
        Err(error) => {
            out.insert("capacity_error".into(), json!(error.to_string()));
        }
    }
    match status {
        Ok(response) => {
            let response = response.into_owned();
            if let Some(status) = response.status.as_option() {
                out.insert(
                    "ide_status".into(),
                    json!({
                        "level": status.level.map_or(String::new(), |value| short_enum(&format!("{value:?}"))),
                        "message": status.message.clone().unwrap_or_default(),
                    }),
                );
            }
            out.insert(
                "show_review_prompt".into(),
                json!(response.show_review_prompt.unwrap_or(false)),
            );
        }
        Err(error) => {
            out.insert("status_error".into(), json!(error.to_string()));
        }
    }
    match model_statuses {
        Ok(response) => {
            let statuses = response
                .into_owned()
                .model_status_infos
                .into_iter()
                .map(|status| {
                    json!({
                        "model": status.model.map_or(String::new(), |value| short_enum(&format!("{value:?}"))),
                        "model_uid": status.model_uid.unwrap_or_default(),
                        "status": status.status.map_or(String::new(), |value| short_enum(&format!("{value:?}"))),
                        "message": status.message.unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>();
            out.insert("model_statuses".into(), Value::Array(statuses));
        }
        Err(error) => {
            out.insert("model_status_error".into(), json!(error.to_string()));
        }
    }
    match providers {
        Ok(response) => {
            let providers = response
                .into_owned()
                .model_providers
                .into_iter()
                .map(|provider| {
                    json!({
                        "provider": provider.provider.map_or(String::new(), |value| short_enum(&format!("{value:?}"))),
                        "display_name": provider.display_name.unwrap_or_default(),
                    })
                })
                .collect::<Vec<_>>();
            out.insert("providers".into(), Value::Array(providers));
        }
        Err(error) => {
            out.insert("providers_error".into(), json!(error.to_string()));
        }
    }
    match catalog {
        Ok(models) => {
            let uids = models
                .iter()
                .map(|model| model.id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let mut absent = Vec::new();
            let mut shadowed = Vec::new();
            for (name, target) in adapter.aliases() {
                let target = target.trim();
                if target.is_empty() {
                    continue;
                }
                if !uids.contains(target) {
                    absent.push(format!("{name}→{target}"));
                }
                if uids.contains(name.as_str()) {
                    shadowed.push(format!("{name}→{target}"));
                }
            }
            shadowed.sort();
            if !absent.is_empty() {
                out.insert("alias_targets_absent".into(), json!(absent));
            }
            if !shadowed.is_empty() {
                out.insert("alias_shadows_catalog".into(), json!(shadowed));
            }
        }
        Err(error) => {
            out.insert("alias_check_error".into(), json!(error.to_string()));
        }
    }
    Value::Object(out)
}

fn short_enum(value: &str) -> String {
    value.rsplit('_').next().unwrap_or(value).to_string()
}

/// Project the version-flexible Seat JSON response into panel fields.
// Mirrors Go's field-by-field projection for parity review.
#[allow(clippy::too_many_lines)]
#[doc(hidden)]
pub fn project_seat_response(root: &Value) -> Value {
    let mut out = Map::new();
    let raw = root
        .get("userStatus")
        .or_else(|| root.get("user_status"))
        .expect("SeatClient validates userStatus");
    let get = |camel: &str, snake: &str| {
        raw.get(camel)
            .or_else(|| raw.get(snake))
            .cloned()
            .unwrap_or(Value::Null)
    };
    out.insert(
        "user".into(),
        json!({
            "name": get("name", "name"),
            "email": get("email", "email"),
            "pro": get("pro", "pro").as_bool().unwrap_or(false),
            "user_id": get("userId", "user_id"),
            "team_id": get("teamId", "team_id"),
            "teams_tier": get("teamsTier", "teams_tier").as_str().map(short_enum).unwrap_or_default(),
            "used_prompt_credits": get("userUsedPromptCredits", "user_used_prompt_credits"),
            "used_flow_credits": get("userUsedFlowCredits", "user_used_flow_credits"),
            "max_premium_chat": get("maxNumPremiumChatMessages", "max_num_premium_chat_messages"),
        }),
    );
    let plan = raw.get("planStatus").or_else(|| raw.get("plan_status"));
    if let Some(plan) = plan {
        let field = |camel: &str, snake: &str| {
            plan.get(camel)
                .or_else(|| plan.get(snake))
                .cloned()
                .unwrap_or(Value::Null)
        };
        let mut projected = json!({
            "available_prompt_credits": field("availablePromptCredits", "available_prompt_credits"),
            "available_flow_credits": field("availableFlowCredits", "available_flow_credits"),
            "available_flex_credits": field("availableFlexCredits", "available_flex_credits"),
            "used_flex_credits": field("usedFlexCredits", "used_flex_credits"),
            "used_flow_credits": field("usedFlowCredits", "used_flow_credits"),
            "used_prompt_credits": field("usedPromptCredits", "used_prompt_credits"),
            "daily_quota_remaining": field("dailyQuotaRemainingPercent", "daily_quota_remaining_percent"),
            "weekly_quota_remaining": field("weeklyQuotaRemainingPercent", "weekly_quota_remaining_percent"),
            "daily_quota_reset": field("dailyQuotaResetAtUnix", "daily_quota_reset_at_unix"),
            "weekly_quota_reset": field("weeklyQuotaResetAtUnix", "weekly_quota_reset_at_unix"),
            "acu_consumed": field("acuConsumed", "acu_consumed"),
            "acu_limit": field("acuLimit", "acu_limit"),
            "overage_balance_micros": field("overageBalanceMicros", "overage_balance_micros"),
            "plan_start": field("planStart", "plan_start"),
            "plan_end": field("planEnd", "plan_end"),
            "was_reduced_by_orphaned_usage": field("wasReducedByOrphanedUsage", "was_reduced_by_orphaned_usage").as_bool().unwrap_or(false),
            "grace_period_status": field("gracePeriodStatus", "grace_period_status").as_str().map(short_enum).unwrap_or_default(),
            "grace_period_end": field("gracePeriodEnd", "grace_period_end"),
        });
        if let Some(top_up) = plan
            .get("topUpStatus")
            .or_else(|| plan.get("top_up_status"))
        {
            let value = |camel: &str, snake: &str| {
                top_up
                    .get(camel)
                    .or_else(|| top_up.get(snake))
                    .cloned()
                    .unwrap_or(Value::Null)
            };
            projected["top_up_status"] = json!({
                "enabled": value("topUpEnabled", "top_up_enabled").as_bool().unwrap_or(false),
                "transaction_status": value("topUpTransactionStatus", "top_up_transaction_status").as_str().map(short_enum).unwrap_or_default(),
                "monthly_amount": value("monthlyTopUpAmount", "monthly_top_up_amount"),
                "spent": value("topUpSpent", "top_up_spent"),
                "increment": value("topUpIncrement", "top_up_increment"),
                "criteria_met": value("topUpCriteriaMet", "top_up_criteria_met").as_bool().unwrap_or(false),
            });
        }
        if let Some(info) = plan.get("planInfo").or_else(|| plan.get("plan_info")) {
            let value = |camel: &str, snake: &str| {
                info.get(camel)
                    .or_else(|| info.get(snake))
                    .cloned()
                    .unwrap_or(Value::Null)
            };
            projected["plan_name"] = value("planName", "plan_name");
            projected["monthly_prompt_credits"] =
                value("monthlyPromptCredits", "monthly_prompt_credits");
            projected["monthly_flow_credits"] = value("monthlyFlowCredits", "monthly_flow_credits");
            projected["billing_strategy"] = json!(
                value("billingStrategy", "billing_strategy")
                    .as_str()
                    .map(short_enum)
                    .unwrap_or_default()
            );
            projected["is_teams"] = json!(value("isTeams", "is_teams").as_bool().unwrap_or(false));
            projected["is_enterprise"] = json!(
                value("isEnterprise", "is_enterprise")
                    .as_bool()
                    .unwrap_or(false)
            );
            projected["can_buy_more"] = json!(
                value("canBuyMoreCredits", "can_buy_more_credits")
                    .as_bool()
                    .unwrap_or(false)
            );
            projected["has_paid_features"] = json!(
                value("hasPaidFeatures", "has_paid_features")
                    .as_bool()
                    .unwrap_or(false)
            );
            out.insert(
                "plan_info".into(),
                json!({
                    "plan_name": projected["plan_name"],
                    "monthly_prompt_credits": projected["monthly_prompt_credits"],
                    "monthly_flow_credits": projected["monthly_flow_credits"],
                    "billing_strategy": projected["billing_strategy"],
                    "is_teams": projected["is_teams"],
                    "is_enterprise": projected["is_enterprise"],
                    "has_paid_features": projected["has_paid_features"],
                }),
            );
        }
        out.insert("plan_status".into(), projected);
    }

    if let Some(info) = root.get("planInfo").or_else(|| root.get("plan_info")) {
        let value = |camel: &str, snake: &str| {
            info.get(camel)
                .or_else(|| info.get(snake))
                .cloned()
                .unwrap_or(Value::Null)
        };
        out.insert(
            "plan_info".into(),
            json!({
                "plan_name": value("planName", "plan_name"),
                "monthly_prompt_credits": value("monthlyPromptCredits", "monthly_prompt_credits"),
                "monthly_flow_credits": value("monthlyFlowCredits", "monthly_flow_credits"),
                "billing_strategy": value("billingStrategy", "billing_strategy").as_str().map(short_enum).unwrap_or_default(),
                "is_teams": value("isTeams", "is_teams").as_bool().unwrap_or(false),
                "is_enterprise": value("isEnterprise", "is_enterprise").as_bool().unwrap_or(false),
                "has_paid_features": value("hasPaidFeatures", "has_paid_features").as_bool().unwrap_or(false),
                "max_premium_chat_messages": value("maxNumPremiumChatMessages", "max_num_premium_chat_messages"),
            }),
        );
    }
    Value::Object(out)
}

struct EmptyData;
impl DashboardData for EmptyData {
    fn status(&self) -> DataFuture<'_> {
        Box::pin(async { Ok(json!({})) })
    }
    fn models(&self) -> DataFuture<'_> {
        Box::pin(async { Ok(json!({"models":[]})) })
    }
}

/// Successful hot-reload response.
#[derive(Clone, Debug, Serialize)]
pub struct ConfigReloadReport {
    pub at: String,
    pub applied: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub requires_restart: Vec<String>,
}

/// Panel assembly configuration.
#[derive(Clone)]
pub struct Config {
    pub password: String,
    pub version: String,
    pub token: Arc<dyn Fn() -> String + Send + Sync>,
    pub metrics: Option<Arc<Metrics>>,
    pub debug_manager: Option<Arc<Manager>>,
    pub data: Arc<dyn DashboardData>,
    /// `SetGateStats` — the rate-gate snapshot source for the `gate`
    /// section of `/panel/api/stats`; `None` omits the section, and a
    /// provider returning `None` omits it for that request.
    pub gate_stats: Option<Arc<dyn Fn() -> Option<GateStats> + Send + Sync>>,
    pub config_current: Option<Arc<dyn Fn() -> Value + Send + Sync>>,
    pub config_reload: Option<Arc<dyn Fn() -> Result<ConfigReloadReport, String> + Send + Sync>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            password: String::new(),
            version: String::new(),
            token: Arc::new(String::new),
            metrics: None,
            debug_manager: None,
            data: Arc::new(EmptyData),
            gate_stats: None,
            config_current: None,
            config_reload: None,
        }
    }
}

#[derive(Clone)]
pub struct Dashboard {
    inner: Arc<Inner>,
    router: Router,
}

struct AuthState {
    password: String,
    hash: [u8; 32],
}

struct LoginFail {
    fails: u32,
    locked_until: Option<Instant>,
    last_seen: Instant,
}

struct SessionState {
    tokens: HashMap<String, Instant>,
    failures: HashMap<String, LoginFail>,
}

struct Inner {
    auth: RwLock<AuthState>,
    sessions: Mutex<SessionState>,
    version: String,
    token: Arc<dyn Fn() -> String + Send + Sync>,
    recent_tokens: Mutex<VecDeque<String>>,
    metrics: Option<Arc<Metrics>>,
    debug_manager: Option<Arc<Manager>>,
    data: Arc<dyn DashboardData>,
    gate_stats: Option<Arc<dyn Fn() -> Option<GateStats> + Send + Sync>>,
    config_current: Option<Arc<dyn Fn() -> Value + Send + Sync>>,
    config_reload: Option<Arc<dyn Fn() -> Result<ConfigReloadReport, String> + Send + Sync>>,
}

impl Dashboard {
    pub fn new(config: Config) -> Self {
        let hash = Sha256::digest(config.password.as_bytes()).into();
        let inner = Arc::new(Inner {
            auth: RwLock::new(AuthState {
                password: config.password,
                hash,
            }),
            sessions: Mutex::new(SessionState {
                tokens: HashMap::new(),
                failures: HashMap::new(),
            }),
            version: config.version,
            token: config.token,
            recent_tokens: Mutex::new(VecDeque::new()),
            metrics: config.metrics,
            debug_manager: config.debug_manager,
            data: config.data,
            gate_stats: config.gate_stats,
            config_current: config.config_current,
            config_reload: config.config_reload,
        });
        let router = routes().with_state(inner.clone());
        Self { inner, router }
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Runtime password replacement revokes every existing session, matching
    /// the Go hot-reload semantics.
    pub fn set_password(&self, password: String) {
        let hash = Sha256::digest(password.as_bytes()).into();
        *self
            .inner
            .auth
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = AuthState { password, hash };
        self.inner
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .tokens
            .clear();
    }

    /// Start the Go-compatible quota history sampler. A zero interval or a
    /// disabled log manager leaves sampling off.
    pub fn start_quota_sampler(&self, interval: Duration) {
        let Some(manager) = self.inner.debug_manager.clone() else {
            return;
        };
        if interval.is_zero() || manager.root().as_os_str().is_empty() {
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            loop {
                sample_quota(&inner, &manager).await;
                tokio::time::sleep(interval).await;
            }
        });
    }
}

fn routes() -> Router<Arc<Inner>> {
    Router::new()
        .route("/panel", get(panel))
        .route("/panel/login", post(login))
        .route("/panel/static/{*file}", get(static_file))
        .route("/panel/api", get(api_index))
        .route("/panel/api/status", get(api_status))
        .route("/panel/api/models", get(api_models))
        .route("/panel/api/stats", get(api_stats))
        .route("/panel/api/requests", get(api_requests))
        .route("/panel/api/requests/matrix", get(api_request_matrix))
        .route("/panel/api/requests/export", get(api_export_requests))
        .route("/panel/api/requests/active", get(api_active_requests))
        .route("/panel/api/requests/{dir}", get(api_request_detail))
        .route("/panel/api/requests/{dir}/merged", get(api_merged_response))
        .route(
            "/panel/api/requests/{dir}/file/{*file}",
            get(api_request_file),
        )
        .route("/panel/api/requests/{dir}/abort", post(api_abort_request))
        .route("/panel/api/logs", get(api_process_log))
        .route("/panel/api/quota", get(api_quota))
        .route("/panel/api/usage", get(api_usage))
        .route("/panel/api/debug/toggle", post(api_debug_toggle))
        .route("/panel/api/config", get(api_config_current))
        .route("/panel/api/config/reload", post(api_config_reload))
        .layer(middleware::from_fn(peer_identity))
}

fn json_response(status: StatusCode, value: &Value) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
        ))
        .expect("valid response")
}

fn bytes_response(status: StatusCode, content_type: &'static str, bytes: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(bytes))
        .expect("valid response")
}

const PEER_HEADER: &str = "x-devin2api-peer-address";

async fn peer_identity(mut request: Request, next: Next) -> Response {
    request.headers_mut().remove(PEER_HEADER);
    if let Some(peer) = request.extensions().get::<SocketAddr>()
        && let Ok(value) = HeaderValue::from_str(&peer.ip().to_string())
    {
        request.headers_mut().insert(PEER_HEADER, value);
    }
    next.run(request).await
}

fn client_key(headers: &HeaderMap) -> String {
    headers
        .get(PEER_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

impl Inner {
    fn note_failure(&self, key: String) -> bool {
        let now = Instant::now();
        let mut state = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failure = state.failures.entry(key).or_insert(LoginFail {
            fails: 0,
            locked_until: None,
            last_seen: now,
        });
        if failure.locked_until.is_some_and(|until| now < until) {
            failure.last_seen = now;
            return true;
        }
        if now.duration_since(failure.last_seen) > LOGIN_LOCKOUT {
            failure.fails = 0;
        }
        failure.last_seen = now;
        failure.fails += 1;
        if failure.fails >= LOGIN_MAX_FAILS {
            failure.fails = 0;
            failure.locked_until = Some(now + LOGIN_LOCKOUT);
        }
        if state.failures.len() > SESSION_SWEEP_THRESHOLD {
            state.failures.retain(|_, f| {
                f.locked_until.is_some_and(|until| now < until)
                    || now.duration_since(f.last_seen) <= LOGIN_LOCKOUT
            });
        }
        false
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let auth = self
            .auth
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if auth.password.is_empty() {
            return Ok(());
        }
        let key = client_key(headers);
        if let Some(value) = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        {
            let provided: [u8; 32] = Sha256::digest(value.as_bytes()).into();
            if bool::from(provided.ct_eq(&auth.hash)) {
                drop(auth);
                self.sessions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .failures
                    .remove(&key);
                return Ok(());
            }
            if self.note_failure(key) {
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
        }
        drop(auth);
        if let Some(id) = cookie_value(headers, "devin_panel_session") {
            let now = Instant::now();
            let mut state = self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.tokens.get(id).is_some_and(|expiry| now <= *expiry) {
                return Ok(());
            }
            state.tokens.remove(id);
        }
        Err(StatusCode::UNAUTHORIZED)
    }

    fn require_auth(&self, headers: &HeaderMap) -> Option<Response> {
        match self.authenticate(headers) {
            Ok(()) => None,
            Err(StatusCode::TOO_MANY_REQUESTS) => Some(json_response(
                StatusCode::TOO_MANY_REQUESTS,
                &json!({"error":"登录尝试过多，请稍后再试"}),
            )),
            Err(_) => Some(json_response(
                StatusCode::UNAUTHORIZED,
                &json!({"error":"未授权"}),
            )),
        }
    }

    fn masked(&self, mut data: Vec<u8>) -> Vec<u8> {
        let token = (self.token)();
        let mut recent = self
            .recent_tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !token.is_empty() && recent.front() != Some(&token) {
            recent.retain(|v| v != &token);
            recent.push_front(token);
            recent.truncate(8);
        }
        for token in recent.iter() {
            if let Ok(encoded) = serde_json::to_string(token) {
                let escaped = &encoded.as_bytes()[1..encoded.len() - 1];
                if escaped != token.as_bytes() {
                    data = replace_all(data, escaped, b"<redacted>");
                }
            }
            data = replace_all(data, token.as_bytes(), b"<redacted>");
        }
        data
    }
}

fn replace_all(data: Vec<u8>, from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return data;
    }
    let mut out = Vec::with_capacity(data.len());
    let mut rest = data.as_slice();
    while let Some(pos) = rest.windows(from.len()).position(|w| w == from) {
        out.extend_from_slice(&rest[..pos]);
        out.extend_from_slice(to);
        rest = &rest[pos + from.len()..];
    }
    out.extend_from_slice(rest);
    out
}

fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix(name)?.strip_prefix('='))
}

async fn panel(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    let password_set = !inner
        .auth
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .password
        .is_empty();
    let login = password_set && inner.authenticate(&headers).is_err();
    // ?v= 版本戳让 HTML 引用的资源随发版必然换新 URL；模块 import 走
    // ETag 条件请求，覆盖 import 链注不进版本戳的部分（Go servePanel）。
    let version = if inner.version.is_empty() {
        "dev"
    } else {
        inner.version.as_str()
    };
    let page = if login { LOGIN_PAGE } else { PANEL_PAGE };
    bytes_response(
        StatusCode::OK,
        "text/html; charset=utf-8",
        page.replace("__VERSION__", version).into_bytes(),
    )
}

// Embedded panel assets (Go `go:embed static` parity). The login/panel HTML
// templates are also reachable under /panel/static/* exactly like Go's
// staticFS, served with the raw __VERSION__ placeholder unsubstituted.
const LOGIN_PAGE: &str = include_str!("../assets/panel/login.html");
const PANEL_PAGE: &str = include_str!("../assets/panel/panel.html");

const STATIC_FILES: &[(&str, &[u8])] = &[
    ("login.html", LOGIN_PAGE.as_bytes()),
    ("panel.html", PANEL_PAGE.as_bytes()),
    ("panel.css", include_bytes!("../assets/panel/panel.css")),
    (
        "echarts.min.js",
        include_bytes!("../assets/panel/echarts.min.js"),
    ),
    ("js/boot.js", include_bytes!("../assets/panel/js/boot.js")),
    (
        "js/charts.js",
        include_bytes!("../assets/panel/js/charts.js"),
    ),
    ("js/core.js", include_bytes!("../assets/panel/js/core.js")),
    (
        "js/tab-models.js",
        include_bytes!("../assets/panel/js/tab-models.js"),
    ),
    (
        "js/tab-overview.js",
        include_bytes!("../assets/panel/js/tab-overview.js"),
    ),
    (
        "js/tab-quota.js",
        include_bytes!("../assets/panel/js/tab-quota.js"),
    ),
    (
        "js/tab-requests.js",
        include_bytes!("../assets/panel/js/tab-requests.js"),
    ),
    (
        "js/tab-system.js",
        include_bytes!("../assets/panel/js/tab-system.js"),
    ),
    (
        "js/tab-usage.js",
        include_bytes!("../assets/panel/js/tab-usage.js"),
    ),
    (
        "js/vendor/morphdom.js",
        include_bytes!("../assets/panel/js/vendor/morphdom.js"),
    ),
];

/// One embedded asset with its precomputed revalidation/encoding state:
/// content is fixed at compile time, so the `ETag` and the gzip body (only
/// worth it at >= 1KB, like Go) are computed once on first use.
struct StaticEntry {
    body: &'static [u8],
    etag: String,
    gzip: Option<Vec<u8>>,
}

fn static_entries() -> &'static HashMap<&'static str, StaticEntry> {
    static ENTRIES: OnceLock<HashMap<&'static str, StaticEntry>> = OnceLock::new();
    ENTRIES.get_or_init(|| {
        STATIC_FILES
            .iter()
            .map(|(name, body)| {
                let sum: [u8; 32] = Sha256::digest(body).into();
                let etag = format!("\"{}\"", hex16(sum));
                let gzip = (body.len() >= 1024).then(|| {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(body).expect("gzip of memory body");
                    encoder.finish().expect("gzip of memory body")
                });
                (*name, StaticEntry { body, etag, gzip })
            })
            .collect()
    })
}

fn hex16(sum: [u8; 32]) -> String {
    use std::fmt::Write as _;
    sum[..16]
        .iter()
        .fold(String::with_capacity(32), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

/// Go `path.Clean` for the static wildcard: dot segments resolve in-tree,
/// anything escaping the root (or empty) is rejected.
fn clean_static_name(raw: &str) -> Option<String> {
    let mut parts: Vec<&str> = Vec::new();
    for segment in raw.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            _ => parts.push(segment),
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join("/"))
}

/// Go `staticContentType`: only these extensions get a real MIME, the rest
/// stream as octets.
fn static_content_type(name: &str) -> &'static str {
    match name.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    }
}

async fn static_file(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    AxumPath(file): AxumPath<String>,
) -> Response {
    let Some(name) = clean_static_name(&file) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // panel.css 例外：登录页与它共享同一套设计令牌，设了密码（登录页
    // 唯一会出现的场景）时若拦它，登录页会裸成浏览器默认样式。
    if name != "panel.css"
        && let Some(r) = inner.require_auth(&headers)
    {
        return r;
    }
    let Some(entry) = static_entries().get(name.as_str()) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Response::builder()
        .header(CONTENT_TYPE, static_content_type(&name))
        .header("cache-control", "private, no-cache")
        .header("etag", entry.etag.as_str())
        // 响应体随客户端 Accept-Encoding 变体——无论 200 还是 304 都要声明。
        .header("vary", "Accept-Encoding");
    if headers
        .get("if-none-match")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|value| value == entry.etag)
    {
        return response
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .expect("valid response");
    }
    if let Some(gzip) = &entry.gzip {
        let accepts_gzip = headers
            .get("accept-encoding")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|value| value.contains("gzip"));
        if accepts_gzip {
            response = response.header("content-encoding", "gzip");
            return response
                .status(StatusCode::OK)
                .body(Body::from(gzip.clone()))
                .expect("valid response");
        }
    }
    response
        .status(StatusCode::OK)
        .body(Body::from(entry.body))
        .expect("valid response")
}

trait IntoResponse {
    fn into_response(self) -> Response;
}
impl IntoResponse for StatusCode {
    fn into_response(self) -> Response {
        Response::builder()
            .status(self)
            .body(Body::empty())
            .expect("valid response")
    }
}

async fn login(State(inner): State<Arc<Inner>>, headers: HeaderMap, body: Bytes) -> Response {
    let auth = inner
        .auth
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if auth.password.is_empty() {
        return json_response(StatusCode::OK, &json!({"ok":true,"open":true}));
    }
    let supplied = form_value(&body, "password").unwrap_or_default();
    let provided: [u8; 32] = Sha256::digest(supplied.as_bytes()).into();
    if !bool::from(provided.ct_eq(&auth.hash)) {
        drop(auth);
        if inner.note_failure(client_key(&headers)) {
            return json_response(
                StatusCode::TOO_MANY_REQUESTS,
                &json!({"error":"登录尝试过多，请稍后再试"}),
            );
        }
        return json_response(StatusCode::UNAUTHORIZED, &json!({"error":"密码错误"}));
    }
    drop(auth);
    let key = client_key(&headers);
    let now = Instant::now();
    let id = crate::randid::hex(32);
    let mut sessions = inner
        .sessions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    sessions.failures.remove(&key);
    if sessions.tokens.len() > SESSION_SWEEP_THRESHOLD {
        sessions.tokens.retain(|_, expiry| now <= *expiry);
    }
    sessions.tokens.insert(id.clone(), now + SESSION_LIFETIME);
    drop(sessions);
    let mut response = json_response(StatusCode::OK, &json!({"ok":true}));
    response.headers_mut().insert(
        SET_COOKIE,
        HeaderValue::from_str(&format!(
            "devin_panel_session={id}; Path=/; Max-Age=86400; HttpOnly; SameSite=Lax"
        ))
        .expect("session cookie is valid"),
    );
    response
}

fn form_value(body: &[u8], key: &str) -> Option<String> {
    std::str::from_utf8(body).ok()?.split('&').find_map(|part| {
        let (k, v) = part.split_once('=')?;
        (percent_decode(k) == key).then(|| percent_decode(v))
    })
}
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&value[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn api_index(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let endpoints = [
        ("GET", "/panel/api/status"),
        ("GET", "/panel/api/models"),
        ("GET", "/panel/api/stats"),
        ("GET", "/panel/api/config"),
        ("POST", "/panel/api/config/reload"),
        ("GET", "/panel/api/usage"),
        ("GET", "/panel/api/requests"),
        ("GET", "/panel/api/requests/matrix"),
        ("GET", "/panel/api/requests/export"),
        ("GET", "/panel/api/requests/active"),
        ("GET", "/panel/api/requests/{dir}"),
        ("GET", "/panel/api/requests/{dir}/merged"),
        ("GET", "/panel/api/requests/{dir}/file/{name}"),
        ("POST", "/panel/api/requests/{dir}/abort"),
        ("GET", "/panel/api/logs"),
        ("GET", "/panel/api/quota"),
        ("POST", "/panel/api/debug/toggle"),
    ]
    .into_iter()
    .map(|(method, path)| json!({"method":method,"path":path,"description":""}))
    .collect::<Vec<_>>();
    json_response(
        StatusCode::OK,
        &json!({"service":"devin-2api","version":inner.version,"auth":"dashboard.password 非空时可用 cookie 会话或 Authorization: Bearer <密码>","endpoints":endpoints,"debug_workflow":["每个 /v1/* 响应带 X-Request-Id 头","凭 dir 查询请求详情与文件","也可直接读磁盘日志"]}),
    )
}

async fn api_status(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    match inner.data.status().await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => json_response(StatusCode::OK, &json!({"user_status_error":error})),
    }
}
async fn api_models(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    match inner.data.models().await {
        Ok(value) => json_response(StatusCode::OK, &value),
        Err(error) => json_response(StatusCode::BAD_GATEWAY, &json!({"error":error})),
    }
}
async fn api_stats(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let mut value = Map::new();
    value.insert("version".into(), Value::String(inner.version.clone()));
    if let Some(metrics) = &inner.metrics {
        value.insert("http".into(), metrics.snapshot());
    }
    if let Some(manager) = &inner.debug_manager {
        value.insert(
            "debuglog".into(),
            parse_json(&debuglog::gojson::marshal(&manager.stats()).unwrap_or_default()),
        );
        value.insert(
            "usage".into(),
            parse_json(&manager.usage_latency().to_go_json()),
        );
    }
    // Go `payload["gate"] = h.gateStats()`: present only when a provider
    // is wired and returns a snapshot.
    if let Some(provider) = &inner.gate_stats
        && let Some(stats) = provider()
    {
        value.insert("gate".into(), json!(stats));
    }
    json_response(StatusCode::OK, &Value::Object(value))
}

#[derive(Default, Deserialize)]
struct RequestQuery {
    limit: Option<i64>,
    offset: Option<i64>,
    q: Option<String>,
    status: Option<String>,
    status_class: Option<String>,
    result: Option<String>,
    model: Option<String>,
    error_stage: Option<String>,
    since: Option<String>,
    until: Option<String>,
    format: Option<String>,
    raw: Option<String>,
}
fn filter(q: &RequestQuery) -> RequestFilter {
    RequestFilter {
        query: q.q.clone().unwrap_or_default(),
        status: q.status.clone().unwrap_or_default(),
        status_class: q.status_class.clone().unwrap_or_default(),
        result: q.result.clone().unwrap_or_default(),
        model: q.model.clone().unwrap_or_default(),
        error_stage: q.error_stage.clone().unwrap_or_default(),
        since: q.since.as_deref().and_then(|v| v.parse().ok()),
        until: q.until.as_deref().and_then(|v| v.parse().ok()),
    }
}
fn entries_json(entries: &[IndexEntry]) -> Vec<Value> {
    entries
        .iter()
        .map(|e| parse_json(&e.to_go_json()))
        .collect()
}

async fn api_requests(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    Query(query): Query<RequestQuery>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(StatusCode::OK, &json!({"requests":[],"disabled":true}));
    };
    let result = manager.list_requests(REQUESTS_FETCH_CAP, &filter(&query));
    let total = result.entries.len();
    let offset = query.offset.unwrap_or(0);
    let limit =
        usize::try_from(query.limit.filter(|v| *v > 0 && *v <= 500).unwrap_or(50)).unwrap_or(50);
    let start = usize::try_from(offset.max(0))
        .unwrap_or(usize::MAX)
        .min(total);
    let end = (start + limit).min(total);
    let mut payload = json!({"requests":entries_json(&result.entries[start..end]),"total":total,"offset":offset,"limit":limit,"has_more":result.has_more});
    if let Some(metrics) = &inner.metrics {
        payload["rejects"] = metrics.snapshot()["rejects"].clone();
    }
    json_response(StatusCode::OK, &payload)
}

async fn api_request_matrix(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    Query(query): Query<RequestQuery>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(StatusCode::OK, &json!({"entries":[],"disabled":true}));
    };
    let parsed = filter(&query);
    let result = manager.list_requests(REQUESTS_FETCH_CAP, &parsed);
    let entries = result.entries.iter().map(|e| json!({"started_at":e.started_at,"model":e.model,"requested_model":e.requested_model,"status_code":e.status_code,"result":e.result,"error_stage":e.error_stage,"owner":error_owner(e),"duration_ms":e.duration_ms,"first_upstream_ms":e.first_upstream_ms,"rate_limited":e.rate_limited})).collect::<Vec<_>>();
    let tail_after_since = result.has_more
        && parsed.since.as_ref().is_some_and(|since| {
            result
                .index_tail_start
                .parse::<jiff::Zoned>()
                .is_ok_and(|tail| tail.timestamp() > since.timestamp())
        });
    let truncated = result.entries.len() >= REQUESTS_FETCH_CAP || tail_after_since;
    json_response(
        StatusCode::OK,
        &json!({"entries":entries,"total":entries.len(),"truncated":truncated}),
    )
}

async fn api_export_requests(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    Query(query): Query<RequestQuery>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled"}),
        );
    };
    let result = manager.list_requests(REQUESTS_FETCH_CAP, &filter(&query));
    let mut response = if query.format.as_deref() == Some("csv") {
        let mut out = String::from(
            "dir,started_at,method,path,api,model,requested_model,response_model,status,result,duration_ms,first_upstream_ms,first_client_ms,input_tokens,output_tokens,cache_read_tokens,cache_write_tokens,reasoning_tokens,total_tokens,stream,key_hash,client_request_id,error_stage,retries\n",
        );
        for e in &result.entries {
            out.push_str(&csv_row(e));
        }
        let mut r = bytes_response(StatusCode::OK, "text/csv; charset=utf-8", out.into_bytes());
        r.headers_mut().insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"requests.csv\""),
        );
        r
    } else {
        json_response(StatusCode::OK, &Value::Array(entries_json(&result.entries)))
    };
    if result.has_more {
        response
            .headers_mut()
            .insert("x-truncated", HeaderValue::from_static("true"));
    }
    response
}

fn csv_escape(mut value: String) -> String {
    if value.starts_with(['=', '+', '-', '@']) {
        value.insert(0, '\'');
    }
    if value.contains([',', '"', '\n']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}
fn csv_row(e: &IndexEntry) -> String {
    let fields = [
        e.dir.clone(),
        e.started_at.clone(),
        e.method.clone(),
        e.path.clone(),
        e.api.clone(),
        e.model.clone(),
        e.requested_model.clone(),
        e.response_model.clone(),
        e.status_code.to_string(),
        e.result.clone(),
        e.duration_ms.to_string(),
        e.first_upstream_ms.map_or(String::new(), |v| v.to_string()),
        e.first_client_ms.map_or(String::new(), |v| v.to_string()),
        e.input_tokens.to_string(),
        e.output_tokens.to_string(),
        e.cache_read_tokens.to_string(),
        e.cache_write_tokens.to_string(),
        e.reasoning_tokens.to_string(),
        e.total_tokens.to_string(),
        e.stream.to_string(),
        e.key_hash.clone(),
        e.client_request_id.clone(),
        e.error_stage.clone(),
        e.retries.to_string(),
    ];
    format!(
        "{}\n",
        fields
            .into_iter()
            .map(csv_escape)
            .collect::<Vec<_>>()
            .join(",")
    )
}

async fn api_active_requests(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let bytes = inner.debug_manager.as_ref().map_or_else(
        || b"[]".to_vec(),
        |m| debuglog::reader::active_requests_json(&m.active_requests()),
    );
    json_response(StatusCode::OK, &json!({"active":parse_json(&bytes)}))
}
async fn api_request_detail(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    AxumPath(dir): AxumPath<String>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled"}),
        );
    };
    if !safe_request_dir(manager.root(), &dir) {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"request log not found or already cleaned"}),
        );
    }
    match manager.detail(&dir) {
        Ok(mut detail) => {
            if let Some(meta) = detail.meta.take() {
                detail.meta = Some(inner.masked(meta));
            }
            json_response(StatusCode::OK, &parse_json(&detail.to_go_json()))
        }
        Err(_) => json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"request log not found or already cleaned"}),
        ),
    }
}
async fn api_request_file(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    AxumPath((dir, file)): AxumPath<(String, String)>,
    Query(query): Query<RequestQuery>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled"}),
        );
    };
    if !safe_request_file(manager.root(), &dir, &file) {
        return json_response(StatusCode::NOT_FOUND, &json!({"error":"file not found"}));
    }
    let Ok((data, size, truncated)) = manager.read_file(&dir, &file) else {
        return json_response(StatusCode::NOT_FOUND, &json!({"error":"file not found"}));
    };
    let data = inner.masked(data);
    if query.raw.as_deref() == Some("1") {
        let mut response = bytes_response(StatusCode::OK, detect_content_type(&data), data);
        response.headers_mut().insert(
            "content-security-policy",
            HeaderValue::from_static("sandbox"),
        );
        response.headers_mut().insert(
            "x-content-type-options",
            HeaderValue::from_static("nosniff"),
        );
        return response;
    }
    match String::from_utf8(data) {
        Ok(text) => json_response(
            StatusCode::OK,
            &json!({"name":file,"size":size,"truncated":truncated,"text":text}),
        ),
        Err(_) => json_response(
            StatusCode::OK,
            &json!({"name":file,"size":size,"truncated":truncated,"binary":true}),
        ),
    }
}
fn safe_request_dir(root: &Path, dir: &str) -> bool {
    if !debuglog::is_request_dir_name(dir) {
        return false;
    }
    let Ok(root) = root.canonicalize() else {
        return false;
    };
    root.join(dir)
        .canonicalize()
        .is_ok_and(|p| p.parent() == Some(root.as_path()))
}
fn safe_request_file(root: &Path, dir: &str, file: &str) -> bool {
    if !debuglog::is_request_dir_name(dir) || !debuglog::valid_file_rel_path(file) {
        return false;
    }
    let Ok(base) = root.join(dir).canonicalize() else {
        return false;
    };
    base.join(file)
        .canonicalize()
        .is_ok_and(|path| path.starts_with(&base) && path.is_file())
}
fn detect_content_type(data: &[u8]) -> &'static str {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if data.starts_with(b"GIF8") {
        "image/gif"
    } else if data.starts_with(b"\xff\xd8\xff") {
        "image/jpeg"
    } else if std::str::from_utf8(data).is_ok() {
        "text/plain; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

async fn api_merged_response(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    AxumPath(dir): AxumPath<String>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled"}),
        );
    };
    if !safe_request_file(manager.root(), &dir, debuglog::STAGE_HTTP_RESPONSE) {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"response stream file not found"}),
        );
    }
    match manager.read_file(&dir, debuglog::STAGE_HTTP_RESPONSE) {
        Ok((data, _, truncated)) => {
            let mut merged = merge_stream(&inner.masked(data));
            merged["truncated"] = Value::Bool(truncated);
            json_response(StatusCode::OK, &merged)
        }
        Err(_) => json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"response stream file not found"}),
        ),
    }
}
fn merge_stream(data: &[u8]) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool = String::new();
    let mut usage = Value::Null;
    let mut finish = String::new();
    let mut events = 0_u64;
    let mut names = BTreeMap::<String, u64>::new();
    for line in data.split(|b| *b == b'\n') {
        let Ok(record) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        events += 1;
        let name = record["event"].as_str().unwrap_or_default().to_string();
        *names.entry(name).or_default() += 1;
        let p = &record["data"];
        if let Some(choices) = p["choices"].as_array() {
            for c in choices {
                if let Some(s) = c["delta"]["content"].as_str() {
                    text.push_str(s);
                }
                if let Some(s) = c["delta"]["reasoning_content"].as_str() {
                    reasoning.push_str(s);
                }
                if let Some(s) = c["finish_reason"].as_str() {
                    finish = s.into();
                }
            }
        }
        if !p["usage"].is_null() {
            usage = p["usage"].clone();
        }
        match p["type"].as_str().unwrap_or_default() {
            "response.output_text.delta" => {
                if let Some(s) = p["delta"].as_str() {
                    text.push_str(s);
                }
            }
            "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => {
                if let Some(s) = p["delta"].as_str() {
                    reasoning.push_str(s);
                }
            }
            "response.completed" | "response.done" | "response.incomplete" => {
                if !p["response"]["usage"].is_null() {
                    usage = p["response"]["usage"].clone();
                }
            }
            "content_block_delta" => match p["delta"]["type"].as_str().unwrap_or_default() {
                "text_delta" => {
                    if let Some(s) = p["delta"]["text"].as_str() {
                        text.push_str(s);
                    }
                }
                "thinking_delta" => {
                    if let Some(s) = p["delta"]["thinking"].as_str() {
                        reasoning.push_str(s);
                    }
                }
                "input_json_delta" => {
                    if let Some(s) = p["delta"]["partial_json"].as_str() {
                        tool.push_str(s);
                    }
                }
                _ => {}
            },
            "message_delta" => {
                if let Some(s) = p["delta"]["stop_reason"].as_str() {
                    finish = s.into();
                }
                if !p["usage"].is_null() {
                    usage = p["usage"].clone();
                }
            }
            _ => {}
        }
    }
    let mut out = json!({"text":text,"events":events,"event_names":names});
    if !reasoning.is_empty() {
        out["reasoning"] = json!(reasoning);
    }
    if !tool.is_empty() {
        out["tool_input"] = json!(tool);
    }
    if !usage.is_null() {
        out["usage"] = usage;
    }
    if !finish.is_empty() {
        out["finish_reason"] = json!(finish);
    }
    out
}

async fn api_abort_request(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    AxumPath(dir): AxumPath<String>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    if inner.debug_manager.as_ref().is_some_and(|m| m.abort(&dir)) {
        json_response(StatusCode::OK, &json!({"aborted":true}))
    } else {
        json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"no active request for dir"}),
        )
    }
}
async fn api_process_log(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled"}),
        );
    };
    let offset = query
        .get("offset")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    match manager.read_process_log(offset) {
        Ok((data, next)) => json_response(
            StatusCode::OK,
            &json!({"text":String::from_utf8_lossy(&inner.masked(data)),"next_offset":next}),
        ),
        Err(_) => json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"process log unavailable"}),
        ),
    }
}

/// Build one `quota.jsonl` row from an aggregated status response.
#[doc(hidden)]
pub fn quota_point_from_status(status: &Value, at: i64) -> Option<Value> {
    let plan = status.get("plan_status")?;
    let number = |name: &str| plan.get(name).and_then(Value::as_f64);
    // Rust's float->int cast saturates; the value is a plan
    // timestamp/count where saturation is harmless.
    #[allow(clippy::cast_possible_truncation)]
    let integer = |name: &str| {
        plan.get(name)
            .and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_f64().map(|value| value as i64))
            })
            .unwrap_or(0)
    };
    let grace_period_end = plan
        .get("grace_period_end")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<jiff::Timestamp>().ok())
        .map_or(0, jiff::Timestamp::as_second);
    let top_up = plan.get("top_up_status");
    Some(json!({
        "at": at,
        "daily_remaining": number("daily_quota_remaining"),
        "weekly_remaining": number("weekly_quota_remaining"),
        "daily_reset_at": integer("daily_quota_reset"),
        "weekly_reset_at": integer("weekly_quota_reset"),
        "prompt_credits": number("available_prompt_credits").unwrap_or(0.0),
        "flow_credits": number("available_flow_credits").unwrap_or(0.0),
        "flex_credits": number("available_flex_credits").unwrap_or(0.0),
        "acu_consumed": number("acu_consumed").unwrap_or(0.0),
        "acu_limit": number("acu_limit").unwrap_or(0.0),
        "used_prompt_credits": number("used_prompt_credits").unwrap_or(0.0),
        "used_flow_credits": number("used_flow_credits").unwrap_or(0.0),
        "used_flex_credits": number("used_flex_credits").unwrap_or(0.0),
        "grace_period_status": plan.get("grace_period_status").and_then(Value::as_str).unwrap_or(""),
        "grace_period_end": grace_period_end,
        "was_reduced_by_orphaned_usage": plan.get("was_reduced_by_orphaned_usage").and_then(Value::as_bool).unwrap_or(false),
        "top_up_enabled": top_up.and_then(|value| value.get("enabled")).and_then(Value::as_bool).unwrap_or(false),
        "top_up_transaction_status": top_up.and_then(|value| value.get("transaction_status")).and_then(Value::as_str).unwrap_or(""),
    }))
}

async fn sample_quota(inner: &Inner, manager: &Manager) {
    use std::io::Write as _;
    let Ok(status) = inner.data.status().await else {
        return;
    };
    let at = jiff::Timestamp::now().as_second();
    let Some(point) = quota_point_from_status(&status, at) else {
        return;
    };
    let path = manager.root().join("quota.jsonl");
    if std::fs::metadata(&path).is_ok_and(|info| info.len() > 4 << 20) {
        let _ = debuglog::truncate_to_tail(&path, 2 << 20);
    }
    let Ok(mut file) = quota_append_file(&path) else {
        return;
    };
    if let Ok(mut bytes) = serde_json::to_vec(&point) {
        bytes.push(b'\n');
        let _ = file.write_all(&bytes);
    }
}

fn quota_append_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct QuotaPoint {
    at: i64,
    #[serde(default)]
    daily_remaining: Option<f64>,
    #[serde(default)]
    weekly_remaining: Option<f64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    daily_reset_at: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    weekly_reset_at: i64,
    #[serde(flatten)]
    extra: Map<String, Value>,
}
// serde's skip_serializing_if protocol passes `&i64`.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(v: &i64) -> bool {
    *v == 0
}
fn quota_history(manager: &Manager) -> Vec<QuotaPoint> {
    let Ok(data) = debuglog::tail_read(
        &manager.root().join("quota.jsonl"),
        i64::try_from(QUOTA_HISTORY_CAP * 256).unwrap_or(i64::MAX),
    ) else {
        return vec![];
    };
    let mut points = data
        .split(|b| *b == b'\n')
        .filter_map(|line| serde_json::from_slice(line).ok())
        .collect::<Vec<_>>();
    if points.len() > QUOTA_HISTORY_CAP {
        points.drain(..points.len() - QUOTA_HISTORY_CAP);
    }
    points
}
// Go computes the burn forecast in float64; the same conversions are
// kept bit-for-bit for parity.
#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn forecast(points: &[QuotaPoint], lookback: i64, daily: bool) -> Value {
    if points.len() < 2 {
        return Value::Null;
    }
    let last = &points[points.len() - 1];
    let cutoff = last.at - lookback;
    let mut first = &points[0];
    for p in points[..points.len() - 1].iter().rev() {
        if p.at <= cutoff {
            break;
        }
        first = p;
    }
    if first.at == last.at {
        return Value::Null;
    }
    let pick = |p: &QuotaPoint| {
        if daily {
            p.daily_remaining.unwrap_or(0.0)
        } else {
            p.weekly_remaining.unwrap_or(0.0)
        }
    };
    let reset = if daily {
        last.daily_reset_at
    } else {
        last.weekly_reset_at
    };
    let hours = (last.at - first.at) as f64 / 3600.0;
    let rate = (pick(first) - pick(last)) / hours;
    let mut out = json!({"window_hours":hours,"remaining":pick(last),"reset_at":reset,"burn_per_hour":rate,"burn_per_day":rate*24.0});
    if rate > 0.0 {
        let left = pick(last) / rate;
        let exhausted = last.at + (left * 3600.0) as i64;
        out["hours_left"] = json!(left);
        if reset > last.at && exhausted > reset {
            out["survives_until_reset"] = json!(true);
        } else {
            out["exhausted_at"] = json!(exhausted);
        }
    }
    out
}
async fn api_quota(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let points = inner
        .debug_manager
        .as_ref()
        .map_or_else(Vec::new, |m| quota_history(m));
    json_response(
        StatusCode::OK,
        &json!({"points":points,"daily":forecast(&points,86_400,true),"weekly":forecast(&points,604_800,false)}),
    )
}

// Cost/context estimates are float64 like Go's; token counts fit well
// inside the f64 mantissa.
#[allow(clippy::cast_precision_loss)]
async fn api_usage(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(StatusCode::OK, &json!({"disabled":true}));
    };
    let snapshot = parse_json(&manager.usage_stats().to_go_json());
    let catalog = inner
        .data
        .models()
        .await
        .ok()
        .and_then(|value| {
            value["models"].as_array().map(|models| {
                models
                    .iter()
                    .filter_map(|model| {
                        Some((
                            model["uid"].as_str()?.to_string(),
                            (
                                number(&model["price_input"]),
                                number(&model["price_cached"]),
                                number(&model["price_output"]),
                                model["context_tokens"].as_i64().unwrap_or(0),
                            ),
                        ))
                    })
                    .collect::<HashMap<_, _>>()
            })
        })
        .unwrap_or_default();
    let mut total_cost = 0.0;
    let mut models = snapshot["models"].as_array().cloned().unwrap_or_default();
    for row in &mut models {
        let Some(name) = row["name"].as_str() else {
            continue;
        };
        let Some((input, cached, output, context)) = catalog.get(name) else {
            continue;
        };
        let requests = row["requests"].as_i64().unwrap_or(0);
        let context_used = row["input_tokens"].as_i64().unwrap_or(0)
            + row["cache_read_tokens"].as_i64().unwrap_or(0)
            + row["cache_write_tokens"].as_i64().unwrap_or(0);
        let cost = ((row["input_tokens"].as_i64().unwrap_or(0)
            + row["cache_write_tokens"].as_i64().unwrap_or(0)) as f64
            * input
            + row["cache_read_tokens"].as_i64().unwrap_or(0) as f64 * cached
            + row["output_tokens"].as_i64().unwrap_or(0) as f64 * output)
            / 1_000_000.0;
        row["est_cost"] = json!(cost);
        total_cost += cost;
        if *context > 0 && requests > 0 {
            let average = context_used as f64 / requests as f64;
            row["context_tokens"] = json!(context);
            row["avg_context_tokens"] = json!(average);
            row["context_fill_pct"] = json!(average / *context as f64 * 100.0);
        }
    }
    json_response(
        StatusCode::OK,
        &json!({"snapshot":snapshot,"models":models,"est_cost":total_cost,"cost_basis":"catalog price per 1M tokens (estimate, not invoice)","price_missing":catalog.is_empty()}),
    )
}

fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
        .unwrap_or(0.0)
}
#[derive(Deserialize)]
struct Toggle {
    enabled: Option<bool>,
}
async fn api_debug_toggle(
    State(inner): State<Arc<Inner>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(manager) = &inner.debug_manager else {
        return json_response(
            StatusCode::NOT_FOUND,
            &json!({"error":"debug log disabled at startup"}),
        );
    };
    let Ok(toggle) = serde_json::from_slice::<Toggle>(&body) else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error":"body must be {\"enabled\":bool}"}),
        );
    };
    let Some(enabled) = toggle.enabled else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error":"body must be {\"enabled\":bool}"}),
        );
    };
    manager.set_enabled(enabled);
    json_response(StatusCode::OK, &json!({"enabled":manager.enabled()}))
}
async fn api_config_current(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    inner.config_current.as_ref().map_or_else(
        || StatusCode::NOT_FOUND.into_response(),
        |f| {
            let mut value = f();
            if let Some(config) = value.get_mut("config").and_then(Value::as_object_mut) {
                crate::config::redact_config_secrets(config);
            }
            json_response(StatusCode::OK, &value)
        },
    )
}
async fn api_config_reload(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    if let Some(r) = inner.require_auth(&headers) {
        return r;
    }
    let Some(reload) = &inner.config_reload else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match reload() {
        Ok(report) => json_response(
            StatusCode::OK,
            &serde_json::to_value(report).unwrap_or_default(),
        ),
        Err(error) => json_response(StatusCode::UNPROCESSABLE_ENTITY, &json!({"error":error})),
    }
}
fn parse_json(data: &[u8]) -> Value {
    serde_json::from_slice(data).unwrap_or(Value::Null)
}

/// Constant-time byte comparison retained for callers that need to compare
/// already-hashed values.
pub fn token_eq(a: &[u8], b: &[u8]) -> bool {
    bool::from(a.ct_eq(b))
}
/// Generate a random session identifier component.
pub fn new_session_token() -> u64 {
    rand::random()
}

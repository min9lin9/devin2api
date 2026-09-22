//! Task 7 catalog/`auth` contract: model catalog cache/TTL/cooldown/stale,
//! `AssignModel` router resolution + `JWT` cache, capability projection,
//! aliases, and token-generation repair.
//!
//! The upstream is a loopback Connect stub (axum) speaking the same unary
//! and server-streaming wire shapes the generated client uses:
//!   - unary success: 200 `application/proto` + bare protobuf body
//!   - unary error:   non-200 + `{"code","message"}` `JSON` body
//!   - stream error:  200 `application/connect+proto` + `END_STREAM` envelope
//!     carrying `{"error":{"code","message"}}`
//!   - stream OK:     data envelopes + `END_STREAM` `{}`
//!
//! Concurrency scenarios are driven by pre-armed barriers, watch gates and
//! counters only — no timing sleeps. `wait_until` bounds every spin by
//! yield count, never wall time.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use bytes::Bytes;
use devin_proto::buffa::{Message as _, MessageField};
use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::domain::failure::{Canceled, Failure, classify};
use devin2api::domain::request::{
    Content, ImageContent, Message, RequestMessages, TextContent, UserMessage,
};
use devin2api::upstream::catalog::{
    Adapter, AdapterConfig, AdapterError, CatalogError, ModelInfo, merge_aliases, model_entry,
    resolve_model_alias,
};
use devin2api::upstream::request::{CallBinding, build_request, derive_session_ids};
use tokio::sync::{Barrier, watch};
use tokio_util::sync::CancellationToken;

const BOUND: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Connect stub
// ---------------------------------------------------------------------------

/// Scripted reply for a unary Connect RPC.
#[derive(Clone)]
enum StubReply {
    /// 200 + `application/proto` body.
    Proto(Vec<u8>),
    /// Non-200 + Connect `JSON` error body.
    JsonError(u16, &'static str, &'static str),
}

type CatalogMeta = (String, String, String, String, String, String);

struct StubState {
    catalog_calls: AtomicUsize,
    assign_calls: AtomicUsize,
    chat_calls: AtomicUsize,
    /// (`api_key`, `extension_name`, `extension_version`, `os`, `fingerprint`, `auth`).
    catalog_meta: Mutex<Vec<CatalogMeta>>,
    /// (`router_uid`, `cascade_id`, `fingerprint`, `auth`).
    assign_meta: Mutex<Vec<(String, String, String, String)>>,
    /// `api_key` carried in each chat request's metadata.
    chat_meta: Mutex<Vec<String>>,
    /// (`authorization`, `extension_name`, `os`) seen by the Seat endpoint.
    seat_meta: Mutex<Vec<(String, String, String)>>,
    catalog_replies: Mutex<VecDeque<StubReply>>,
    assign_replies: Mutex<VecDeque<StubReply>>,
    /// Full `Authorization` header values the chat endpoint accepts;
    /// anything else gets an `unauthenticated` `END_STREAM` error.
    chat_valid_auth: Mutex<Vec<String>>,
    /// The first N catalog/chat sends park on the matching gate — pre-armed
    /// so every concurrent caller's stale attempt is in flight before any
    /// repair or completion happens.
    catalog_hold_first: AtomicUsize,
    chat_hold_first: AtomicUsize,
    catalog_gate: watch::Sender<bool>,
    chat_gate: watch::Sender<bool>,
}

impl StubState {
    fn new() -> Self {
        Self {
            catalog_calls: AtomicUsize::new(0),
            assign_calls: AtomicUsize::new(0),
            chat_calls: AtomicUsize::new(0),
            catalog_meta: Mutex::new(Vec::new()),
            assign_meta: Mutex::new(Vec::new()),
            chat_meta: Mutex::new(Vec::new()),
            seat_meta: Mutex::new(Vec::new()),
            catalog_replies: Mutex::new(VecDeque::new()),
            assign_replies: Mutex::new(VecDeque::new()),
            chat_valid_auth: Mutex::new(Vec::new()),
            catalog_hold_first: AtomicUsize::new(0),
            chat_hold_first: AtomicUsize::new(0),
            catalog_gate: watch::channel(false).0,
            chat_gate: watch::channel(false).0,
        }
    }
}

struct Stub {
    base_url: String,
    state: Arc<StubState>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

fn respond(reply: StubReply) -> Response {
    match reply {
        StubReply::Proto(body) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/proto")
            .body(body.into())
            .unwrap(),
        StubReply::JsonError(status, code, message) => Response::builder()
            .status(StatusCode::from_u16(status).unwrap())
            .header("content-type", "application/json")
            .body(format!(r#"{{"code":"{code}","message":"{message}"}}"#).into())
            .unwrap(),
    }
}

fn metadata_fields(
    meta: &pb::ExaCodeiumCommonPb_Metadata,
) -> (String, String, String, String, String) {
    (
        meta.api_key.clone().unwrap_or_default(),
        meta.extension_name.clone().unwrap_or_default(),
        meta.extension_version.clone().unwrap_or_default(),
        meta.os.clone().unwrap_or_default(),
        meta.f.clone().unwrap_or_default(),
    )
}

fn auth_header(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

async fn catalog_handler(
    State(state): State<Arc<StubState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let call = state.catalog_calls.fetch_add(1, Ordering::SeqCst) + 1;
    let req =
        pb::GetCliModelConfigsRequest::decode_from_slice(&body).expect("decode catalog request");
    let (key, name, version, os, f) = metadata_fields(&req.metadata);
    state
        .catalog_meta
        .lock()
        .unwrap()
        .push((key, name, version, os, f, auth_header(&headers)));
    if call <= state.catalog_hold_first.load(Ordering::SeqCst) {
        let mut gate = state.catalog_gate.subscribe();
        let _ = gate.wait_for(|open| *open).await;
    }
    let reply = state
        .catalog_replies
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| {
            StubReply::Proto(pb::GetCliModelConfigsResponse::default().encode_to_vec())
        });
    respond(reply)
}

async fn assign_handler(
    State(state): State<Arc<StubState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.assign_calls.fetch_add(1, Ordering::SeqCst);
    let req = pb::AssignModelRequest::decode_from_slice(&body).expect("decode assign request");
    let (_, _, _, _, f) = metadata_fields(&req.metadata);
    state.assign_meta.lock().unwrap().push((
        req.model_router_uid.clone().unwrap_or_default(),
        req.cascade_id.clone().unwrap_or_default(),
        f,
        auth_header(&headers),
    ));
    let reply = state
        .assign_replies
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| StubReply::Proto(pb::AssignModelResponse::default().encode_to_vec()));
    respond(reply)
}

/// Connect streaming envelope: flag byte + big-endian u32 length + payload.
fn envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    out
}

async fn chat_handler(
    State(state): State<Arc<StubState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let call = state.chat_calls.fetch_add(1, Ordering::SeqCst) + 1;
    // Connect streaming requests are envelope-framed: flag + u32 len + payload.
    assert!(body.len() >= 5 && body[0] == 0, "expected data envelope");
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    assert!(body.len() >= 5 + len, "truncated envelope");
    let req = pb::GetChatMessageRequest::decode_from_slice(&body[5..5 + len])
        .expect("decode chat request");
    state
        .chat_meta
        .lock()
        .unwrap()
        .push(req.metadata.api_key.clone().unwrap_or_default());
    if call <= state.chat_hold_first.load(Ordering::SeqCst) {
        let mut gate = state.chat_gate.subscribe();
        let _ = gate.wait_for(|open| *open).await;
    }
    let auth = auth_header(&headers);
    let valid = state.chat_valid_auth.lock().unwrap().contains(&auth);
    if !valid {
        // Semantic Connect error inside the stream: END_STREAM envelope.
        let end = br#"{"error":{"code":"unauthenticated","message":"bad session token"}}"#;
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/connect+proto")
            .body(envelope(0x02, end).into())
            .unwrap();
    }
    let mut stream = Vec::new();
    stream.extend_from_slice(&envelope(
        0x00,
        &pb::GetChatMessageResponse {
            delta_text: Some("ok".to_string()),
            ..Default::default()
        }
        .encode_to_vec(),
    ));
    stream.extend_from_slice(&envelope(0x02, b"{}"));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/connect+proto")
        .body(stream.into())
        .unwrap()
}

async fn seat_handler(
    State(state): State<Arc<StubState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let json: serde_json::Value = serde_json::from_slice(&body).expect("seat json");
    let meta = &json["metadata"];
    state.seat_meta.lock().unwrap().push((
        auth_header(&headers),
        meta["extension_name"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        meta["os"].as_str().unwrap_or_default().to_string(),
    ));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(r#"{"userStatus":{"name":"stub"}}"#.into())
        .unwrap()
}

impl Stub {
    async fn start() -> Self {
        let state = Arc::new(StubState::new());
        let app = Router::new()
            .route(
                "/exa.api_server_pb.ApiServerService/GetCliModelConfigs",
                post(catalog_handler),
            )
            .route(
                "/exa.api_server_pb.ApiServerService/AssignModel",
                post(assign_handler),
            )
            .route(
                "/exa.api_server_pb.ApiServerService/GetChatMessage",
                post(chat_handler),
            )
            .route(
                "/exa.seat_management_pb.SeatManagementService/GetUserStatus",
                post(seat_handler),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app.into_make_service())
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .unwrap();
        });
        Self {
            base_url: format!("http://{addr}"),
            state,
            shutdown,
            task,
        }
    }

    fn push_catalog(&self, reply: StubReply) {
        self.state.catalog_replies.lock().unwrap().push_back(reply);
    }

    fn push_assign(&self, reply: StubReply) {
        self.state.assign_replies.lock().unwrap().push_back(reply);
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

/// Spin (bounded by yield count, never wall time) until `cond` holds.
async fn wait_until(cond: impl Fn() -> bool, what: &str) {
    for _ in 0..100_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {what}");
}

fn adapter_for(stub: &Stub, token: &str) -> Adapter {
    Adapter::new(AdapterConfig {
        base_url: stub.base_url.clone(),
        token: token.to_string(),
        model: "swe-2-max".to_string(),
        ..Default::default()
    })
    .expect("adapter")
}

fn catalog_proto(configs: Vec<pb::ExaCodeiumCommonPb_ClientModelConfig>) -> StubReply {
    StubReply::Proto(
        pb::GetCliModelConfigsResponse {
            client_model_configs: configs,
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn model_config(uid: &str, router: bool, images: bool) -> pb::ExaCodeiumCommonPb_ClientModelConfig {
    pb::ExaCodeiumCommonPb_ClientModelConfig {
        model_uid: Some(uid.to_string()),
        max_tokens: Some(64000),
        supports_images: Some(images),
        provider: Some(
            pb::ExaCodeiumCommonPb_ModelProvider::ExaCodeiumCommonPb_ModelProvider_MODEL_PROVIDER_OPENAI,
        ),
        model_info: MessageField::some(pb::ExaCodeiumCommonPb_ModelInfo {
            is_model_router: Some(router),
            max_output_tokens: Some(8192),
            model_features: MessageField::some(pb::ExaCodeiumCommonPb_ModelFeatures {
                supports_tool_calls: Some(true),
                supports_parallel_tool_calls: Some(true),
                supports_thinking: Some(true),
                preserve_thinking: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn assign_proto(model_uid: &str, jwt: &str) -> StubReply {
    StubReply::Proto(
        pb::AssignModelResponse {
            assignment: MessageField::some(pb::ModelAssignment {
                model_uid: Some(model_uid.to_string()),
                assignment_jwt: Some(jwt.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec(),
    )
}

fn user_request(text: &str) -> RequestMessages {
    RequestMessages {
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: text.to_string(),
            })],
            ..Default::default()
        })],
        ..Default::default()
    }
}

/// The connect-phase repair rule from `G/internal/adapter/devin/devin.go`
/// `Stream`: on `unauthenticated`, retry once against a newer token
/// generation; no extra attempt when no newer token exists.
async fn connect_with_repair(
    adapter: &Adapter,
    request: &RequestMessages,
    cancel: &CancellationToken,
) -> Result<(), Failure> {
    let attempt = adapter.current_token();
    let identity = adapter.client_identity();
    let send = |token: &str| {
        let binding = CallBinding {
            token: token.to_string(),
            model: adapter.config().model.clone(),
            model_assignment_jwt: String::new(),
        };
        let (proto_request, _) =
            build_request(request, &identity, &binding).expect("build_request");
        adapter.stream_client().get_chat_message(proto_request)
    };
    let cancelled = || Failure::plain("context canceled").with_cause(Canceled);
    let result = tokio::select! {
        r = send(&attempt.token) => r.map_err(|e| classify(&e)),
        () = cancel.cancelled() => Err(cancelled()),
    };
    let mut stream = match result {
        Ok(stream) => stream,
        Err(failure) => {
            if failure.code == "unauthenticated"
                && let Some(newer) = adapter.repair_token(attempt.generation)
            {
                return tokio::select! {
                    r = send(&newer.token) => r.map(|_| ()).map_err(|e| classify(&e)),
                    () = cancel.cancelled() => Err(cancelled()),
                };
            }
            return Err(failure);
        }
    };
    // A stream that opened still reports a semantic error on first frame.
    match stream.message::<pb::GetChatMessageResponse>().await {
        Ok(_) => Ok(()),
        Err(e) => {
            let failure = classify(&e);
            if failure.code == "unauthenticated"
                && let Some(newer) = adapter.repair_token(attempt.generation)
            {
                return tokio::select! {
                    r = send(&newer.token) => r.map(|_| ()).map_err(|e| classify(&e)),
                    () = cancel.cancelled() => Err(cancelled()),
                };
            }
            Err(failure)
        }
    }
}

// ---------------------------------------------------------------------------
// Happy path: projection, metadata identity, aliases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_projection_and_metadata_identity() {
    let stub = Stub::start().await;
    stub.push_catalog(catalog_proto(vec![
        model_config("swe-2-max", false, true),
        model_config("router-uid", true, false),
        // Disabled and uid-less entries are dropped; duplicates deduped.
        pb::ExaCodeiumCommonPb_ClientModelConfig {
            model_uid: Some("disabled-uid".to_string()),
            disabled: Some(true),
            ..Default::default()
        },
        pb::ExaCodeiumCommonPb_ClientModelConfig {
            model_or_alias: MessageField::some(pb::ExaCodeiumCommonPb_ModelOrAlias {
                choice: Some(
                    pb::__buffa::oneof::exa_codeium_common_pb_model_or_alias::Choice::ModelUid(
                        "alias-target".to_string(),
                    ),
                ),
                ..Default::default()
            }),
            ..Default::default()
        },
        model_config("swe-2-max", false, true),
    ]));
    let mut config = AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "tok-1".to_string(),
        model: "configured-uid".to_string(),
        ..Default::default()
    };
    config
        .aliases
        .insert("my-alias".to_string(), "swe-2-max".to_string());
    let adapter = Adapter::new(config).expect("adapter");

    let models = adapter
        .list_models(&CancellationToken::new())
        .await
        .expect("list_models");
    let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
    // Upstream order preserved; configured model appended; alias entry last.
    assert_eq!(
        ids,
        vec![
            "swe-2-max",
            "router-uid",
            "alias-target",
            "configured-uid",
            "my-alias"
        ]
    );
    let swe = &models[0];
    assert!(swe.supports_images && swe.supports_tool_calls && swe.supports_thinking);
    assert!(swe.supports_parallel_tool_calls && swe.preserve_thinking);
    assert_eq!(swe.context_tokens, 64000);
    assert_eq!(swe.max_output_tokens, 8192);
    assert_eq!(swe.owned_by, "openai");
    assert!(swe.created > 0);
    assert!(models[1].is_model_router);
    // Alias entry inherits the target's capability bits.
    let alias = &models[4];
    assert_eq!(alias.alias_of, "swe-2-max");
    assert!(alias.supports_tool_calls && alias.supports_thinking);
    assert_eq!(alias.context_tokens, 64000);
    // Configured model placeholder: images assumed supported.
    assert!(models[3].supports_images);

    // Chat-path client identity: chisel defaults, no fingerprint on the
    // catalog call, Basic token-token auth.
    let meta = stub.state.catalog_meta.lock().unwrap();
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0].0, "tok-1");
    assert_eq!(meta[0].1, "chisel");
    assert_eq!(meta[0].2, "3000.2.17");
    assert_eq!(meta[0].3, "mac");
    assert_eq!(meta[0].4, "");
    assert_eq!(meta[0].5, "Basic tok-1-tok-1");
}

#[tokio::test]
async fn alias_resolution_and_capability_projection() {
    let aliases = BTreeMap::from([
        ("swe-2".to_string(), "swe-2-max".to_string()),
        ("GLM".to_string(), "glm-5-2".to_string()),
        ("*".to_string(), "fallback-uid".to_string()),
    ]);
    // Exact > case-folded > "*" catch-all > passthrough (Go ResolveModelAlias).
    assert_eq!(resolve_model_alias(&aliases, "swe-2"), "swe-2-max");
    assert_eq!(resolve_model_alias(&aliases, "glm"), "glm-5-2");
    assert_eq!(resolve_model_alias(&aliases, "SWE-2"), "swe-2-max");
    assert_eq!(resolve_model_alias(&aliases, "other"), "fallback-uid");
    assert_eq!(
        resolve_model_alias(&BTreeMap::from([("a".to_string(), "b".to_string())]), "x"),
        "x"
    );

    // modelEntry: OpenAI /v1/models projection with capability bits.
    let entry = model_entry(&ModelInfo {
        id: "m1".to_string(),
        created: 0,
        owned_by: String::new(),
        supports_images: true,
        is_model_router: true,
        alias_of: "real-uid".to_string(),
        ..Default::default()
    });
    assert_eq!(entry["id"], "m1");
    assert_eq!(entry["object"], "model");
    assert_eq!(entry["owned_by"], "devin");
    assert_eq!(entry["is_model_router"], true);
    assert_eq!(entry["alias_of"], "real-uid");
    assert!(entry["created"].as_i64().unwrap() > 0);

    // mergeAliases: a shadowed real entry gets alias_of + the target's
    // capability bits; an absent alias gets a synthetic entry; "*" is
    // listed too (Go mergeAliases iterates every configured key).
    let data = vec![
        model_entry(&ModelInfo {
            id: "swe-2".to_string(),
            supports_images: false,
            supports_tool_calls: false,
            ..Default::default()
        }),
        model_entry(&ModelInfo {
            id: "swe-2-max".to_string(),
            supports_images: true,
            supports_tool_calls: true,
            ..Default::default()
        }),
    ];
    let merged = merge_aliases(
        data,
        &BTreeMap::from([
            ("swe-2".to_string(), "swe-2-max".to_string()),
            ("ghost".to_string(), "swe-2-max".to_string()),
            ("*".to_string(), "swe-2-max".to_string()),
        ]),
    );
    let by_id: BTreeMap<&str, &serde_json::Map<String, serde_json::Value>> = merged
        .iter()
        .map(|e| (e["id"].as_str().unwrap(), e))
        .collect();
    assert_eq!(by_id["swe-2"]["alias_of"], "swe-2-max");
    assert_eq!(by_id["swe-2"]["supports_tool_calls"], true);
    assert_eq!(by_id["ghost"]["owned_by"], "alias");
    assert_eq!(by_id["ghost"]["supports_images"], true);
    assert_eq!(by_id["*"]["alias_of"], "swe-2-max");
}

// ---------------------------------------------------------------------------
// Singleflight fetch + independent cancellation (approved exception 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_requests_share_one_fetch() {
    const WAITERS: usize = 8;
    let stub = Stub::start().await;
    stub.state.catalog_hold_first.store(1, Ordering::SeqCst);
    stub.push_catalog(catalog_proto(vec![model_config("m-a", false, true)]));
    let adapter = adapter_for(&stub, "tok");
    let cancel = CancellationToken::new();

    let barrier = Arc::new(Barrier::new(WAITERS));
    let mut tasks = Vec::new();
    for _ in 0..WAITERS {
        let adapter = adapter.clone();
        let cancel = cancel.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            adapter.list_models(&cancel).await
        }));
    }
    // The fetch has started; park every follower on the shared fetch
    // before releasing the stub handler.
    wait_until(
        || stub.state.catalog_calls.load(Ordering::SeqCst) >= 1,
        "catalog call",
    )
    .await;
    wait_until(
        || adapter.catalog_fetch_waiters() >= WAITERS - 1,
        "followers parked on shared fetch",
    )
    .await;
    stub.state.catalog_gate.send_replace(true);

    for task in tasks {
        let models = task.await.unwrap().expect("list_models");
        assert_eq!(models[0].id, "m-a");
    }
    assert_eq!(
        stub.state.catalog_calls.load(Ordering::SeqCst),
        1,
        "concurrent miss must converge to ONE upstream fetch"
    );
}

// One sequential concurrency scenario; splitting would scatter it.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn concurrent_refresh_and_cancelled_leader() {
    const REQUESTS: usize = 6;
    // Part A — cancelled fetch leader: the shared fetch is detached from
    // every caller's cancellation (approved fix), so the leader's cancel
    // neither kills the in-flight upstream call nor strands followers.
    let stub = Stub::start().await;
    stub.state.catalog_hold_first.store(1, Ordering::SeqCst);
    stub.push_catalog(catalog_proto(vec![model_config("m-b", false, true)]));
    let adapter = adapter_for(&stub, "tok");

    let leader_cancel = CancellationToken::new();
    let leader = tokio::spawn({
        let adapter = adapter.clone();
        let token = leader_cancel.clone();
        async move { adapter.list_models(&token).await }
    });

    wait_until(
        || stub.state.catalog_calls.load(Ordering::SeqCst) >= 1,
        "leader's fetch to reach upstream",
    )
    .await;

    // Two followers park on the in-flight fetch, then the leader cancels.
    let follower_cancel = CancellationToken::new();
    let mut followers = Vec::new();
    for _ in 0..2 {
        let adapter = adapter.clone();
        let cancel = follower_cancel.clone();
        followers.push(tokio::spawn(
            async move { adapter.list_models(&cancel).await },
        ));
    }
    wait_until(|| adapter.catalog_fetch_waiters() >= 2, "followers parked").await;
    leader_cancel.cancel();

    // The leader observes its own cancellation immediately — it does not
    // wait for the fetch it started.
    let leader_result = tokio::time::timeout(BOUND, leader).await.unwrap().unwrap();
    assert!(
        matches!(leader_result, Err(CatalogError::Cancelled)),
        "cancelled leader must return Cancelled, got {leader_result:?}"
    );

    // The fetch is still parked on the stub; followers are still waiting.
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 1);
    stub.state.catalog_gate.send_replace(true);
    for follower in followers {
        let models = tokio::time::timeout(BOUND, follower)
            .await
            .unwrap()
            .unwrap()
            .expect("follower list_models");
        assert_eq!(models[0].id, "m-b");
    }
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 1);

    // Part B — cancelled follower: a waiting caller exits on its own
    // cancellation without disturbing the fetch or other waiters.
    let stub2 = Stub::start().await;
    stub2.state.catalog_hold_first.store(1, Ordering::SeqCst);
    stub2.push_catalog(catalog_proto(vec![model_config("m-c", false, true)]));
    let adapter2 = adapter_for(&stub2, "tok");

    let leader2 = tokio::spawn({
        let adapter = adapter2.clone();
        async move { adapter.list_models(&CancellationToken::new()).await }
    });
    wait_until(
        || stub2.state.catalog_calls.load(Ordering::SeqCst) >= 1,
        "second stub fetch",
    )
    .await;
    let waiter_cancel = CancellationToken::new();
    let waiter = tokio::spawn({
        let adapter = adapter2.clone();
        let cancel = waiter_cancel.clone();
        async move { adapter.list_models(&cancel).await }
    });
    wait_until(|| adapter2.catalog_fetch_waiters() >= 1, "waiter parked").await;
    waiter_cancel.cancel();
    let waiter_result = tokio::time::timeout(BOUND, waiter).await.unwrap().unwrap();
    assert!(matches!(waiter_result, Err(CatalogError::Cancelled)));

    stub2.state.catalog_gate.send_replace(true);
    let models = tokio::time::timeout(BOUND, leader2)
        .await
        .unwrap()
        .unwrap()
        .expect("leader list_models");
    assert_eq!(models[0].id, "m-c");
    assert_eq!(stub2.state.catalog_calls.load(Ordering::SeqCst), 1);

    // Part C — concurrent stale-generation repair: N requests all sent
    // with the generation-0 token hit unauthenticated; exactly one
    // credential source read happens and every request retries at most
    // once against the newer generation.
    let stub3 = Stub::start().await;
    stub3
        .state
        .chat_hold_first
        .store(REQUESTS, Ordering::SeqCst);
    *stub3.state.chat_valid_auth.lock().unwrap() = vec!["Basic new-token-new-token".to_string()];
    let source_calls = Arc::new(AtomicUsize::new(0));
    let adapter3 = Adapter::new(AdapterConfig {
        base_url: stub3.base_url.clone(),
        token: "old-token".to_string(),
        model: "swe-2-max".to_string(),
        token_source: Some({
            let calls = source_calls.clone();
            Arc::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                "new-token".to_string()
            })
        }),
        ..Default::default()
    })
    .expect("adapter");

    let barrier = Arc::new(Barrier::new(REQUESTS));
    let mut tasks = Vec::new();
    for i in 0..REQUESTS {
        let adapter = adapter3.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            connect_with_repair(
                &adapter,
                &user_request(&format!("r{i}")),
                &CancellationToken::new(),
            )
            .await
        }));
    }
    // All N stale-generation sends are parked inside the stub.
    wait_until(
        || stub3.state.chat_calls.load(Ordering::SeqCst) >= REQUESTS,
        "all stale sends parked",
    )
    .await;
    stub3.state.chat_gate.send_replace(true);

    for task in tasks {
        tokio::time::timeout(BOUND, task)
            .await
            .unwrap()
            .unwrap()
            .expect("repaired request must succeed");
    }
    assert_eq!(
        source_calls.load(Ordering::SeqCst),
        1,
        "concurrent stale-generation repairs must converge to one credential read"
    );
    assert_eq!(
        stub3.state.chat_calls.load(Ordering::SeqCst),
        2 * REQUESTS,
        "each request sends exactly one stale + one new-generation attempt"
    );
    assert_eq!(adapter3.current_token().token, "new-token");
    // The rebuilt retry carries the new token in metadata too.
    let meta = stub3.state.chat_meta.lock().unwrap();
    assert!(meta[..REQUESTS].iter().all(|k| k == "old-token"));
    assert!(meta[REQUESTS..].iter().all(|k| k == "new-token"));
}

// ---------------------------------------------------------------------------
// TTL / cooldown / stale
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_ttl_cooldown_and_stale() {
    let stub = Stub::start().await;
    stub.push_catalog(catalog_proto(vec![model_config("m-ttl", false, true)]));
    stub.push_catalog(StubReply::JsonError(500, "internal", "catalog exploded"));

    // Manual clock: TTL/cooldown arithmetic is deterministic without
    // sleeping (Go tests pin `time.Now` the same way).
    let clock = Arc::new(Mutex::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000),
    ));
    let adapter = Adapter::new_with_catalog_clock(
        AdapterConfig {
            base_url: stub.base_url.clone(),
            token: "tok".to_string(),
            model: "swe-2-max".to_string(),
            catalog_cache_ttl: Duration::from_secs(60),
            ..Default::default()
        },
        {
            let clock = clock.clone();
            Box::new(move || *clock.lock().unwrap())
        },
    )
    .expect("adapter");
    let cancel = CancellationToken::new();

    // Miss → fetch → cache hit inside TTL (no second call).
    let models = adapter.list_models(&cancel).await.expect("first fetch");
    assert_eq!(models[0].id, "m-ttl");
    adapter.list_models(&cancel).await.expect("cached");
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 1);

    // Past TTL → refetch; upstream 500 → stale cache served + cooldown set.
    *clock.lock().unwrap() += Duration::from_secs(61);
    let stale = adapter.list_models(&cancel).await.expect("stale on error");
    assert_eq!(stale[0].id, "m-ttl");
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 2);

    // Inside the 30s cooldown: stale served, no new upstream call.
    *clock.lock().unwrap() += Duration::from_secs(10);
    adapter.list_models(&cancel).await.expect("cooldown stale");
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 2);

    // Past cooldown: refetch allowed (default empty catalog reply).
    *clock.lock().unwrap() += Duration::from_secs(21);
    adapter
        .list_models(&cancel)
        .await
        .expect("post-cooldown fetch");
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn empty_cache_cooldown_returns_last_error() {
    let stub = Stub::start().await;
    stub.push_catalog(StubReply::JsonError(500, "internal", "reset in 45 seconds"));
    let clock = Arc::new(Mutex::new(
        SystemTime::UNIX_EPOCH + Duration::from_secs(2_000_000),
    ));
    let adapter = Adapter::new_with_catalog_clock(
        AdapterConfig {
            base_url: stub.base_url.clone(),
            token: "tok".to_string(),
            model: "swe-2-max".to_string(),
            ..Default::default()
        },
        {
            let clock = clock.clone();
            Box::new(move || *clock.lock().unwrap())
        },
    )
    .expect("adapter");
    let cancel = CancellationToken::new();

    // No cache: the fetch error propagates and arms the cooldown from the
    // upstream reset hint (45s), not the 30s default.
    let err = adapter.list_models(&cancel).await.expect_err("fetch fails");
    let failure = classify(&err);
    assert_eq!(failure.code, "internal");
    assert!(err.to_string().contains("devin GetCliModelConfigs"));

    *clock.lock().unwrap() += Duration::from_secs(30);
    let err = adapter
        .list_models(&cancel)
        .await
        .expect_err("cooldown error");
    assert_eq!(classify(&err).code, "internal");
    assert_eq!(
        stub.state.catalog_calls.load(Ordering::SeqCst),
        1,
        "cooldown must suppress upstream calls"
    );

    // Past the hinted reset: refetch succeeds (default empty reply).
    *clock.lock().unwrap() += Duration::from_secs(16);
    adapter.list_models(&cancel).await.expect("refetch");
    assert_eq!(stub.state.catalog_calls.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// AssignModel router resolution + JWT cache
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assign_model_router_resolution_and_cache() {
    let stub = Stub::start().await;
    stub.push_catalog(catalog_proto(vec![
        model_config("router-uid", true, true),
        model_config("real-uid", false, true),
    ]));
    stub.push_assign(assign_proto("real-uid", "jwt-abc"));
    let adapter = adapter_for(&stub, "tok");
    let cancel = CancellationToken::new();

    adapter.ensure_catalog(&cancel).await;
    let request = user_request("hello");

    // Router uid resolves through AssignModel; the jwt binds cascade_id.
    let routing = adapter
        .resolve_model_routing(&request, "router-uid", &cancel)
        .await
        .expect("resolve router");
    assert_eq!(routing.model, "real-uid");
    assert_eq!(routing.jwt, "jwt-abc");
    assert_eq!(stub.state.assign_calls.load(Ordering::SeqCst), 1);

    // Same (router uid, cascade id) hits the assignment cache.
    let again = adapter
        .resolve_model_routing(&request, "router-uid", &cancel)
        .await
        .expect("cached assignment");
    assert_eq!(again.jwt, "jwt-abc");
    assert_eq!(stub.state.assign_calls.load(Ordering::SeqCst), 1);

    // Non-router catalog uid and unknown uid pass through untouched.
    let direct = adapter
        .resolve_model_routing(&request, "real-uid", &cancel)
        .await
        .expect("non-router");
    assert_eq!(direct.model, "real-uid");
    assert_eq!(direct.jwt, "");
    let unknown = adapter
        .resolve_model_routing(&request, "not-in-catalog", &cancel)
        .await
        .expect("unknown uid");
    assert_eq!(unknown.model, "not-in-catalog");
    assert_eq!(stub.state.assign_calls.load(Ordering::SeqCst), 1);

    // AssignModel request shape: router uid + request-derived cascade id +
    // chisel identity with the 366-byte hex fingerprint.
    let meta = stub.state.assign_meta.lock().unwrap();
    assert_eq!(meta.len(), 1);
    assert_eq!(meta[0].0, "router-uid");
    assert_eq!(meta[0].1, derive_session_ids(&request).1);
    assert_eq!(meta[0].2.len(), 732, "366-byte hex fingerprint");
    assert_eq!(meta[0].3, "Basic tok-tok");
}

#[tokio::test]
async fn assign_model_error_classification() {
    let stub = Stub::start().await;
    stub.push_catalog(catalog_proto(vec![model_config("router-uid", true, true)]));
    stub.push_assign(StubReply::JsonError(
        400,
        "invalid_argument",
        "not a router",
    ));
    stub.push_assign(assign_proto("", "jwt")); // empty assignment
    let adapter = adapter_for(&stub, "tok");
    let cancel = CancellationToken::new();
    adapter.ensure_catalog(&cancel).await;
    let request = user_request("x");

    let err = adapter
        .resolve_model_routing(&request, "router-uid", &cancel)
        .await
        .expect_err("upstream refusal");
    assert_eq!(err.code, "invalid_argument");
    assert!(err.message.contains("AssignModel(router-uid)"));

    // A different cascade id misses the cache and hits the empty-assignment
    // guard.
    let mut other = user_request("different session");
    other.session_key = "other-session".to_string();
    let err = adapter
        .resolve_model_routing(&other, "router-uid", &cancel)
        .await
        .expect_err("empty assignment");
    assert_eq!(err.code, "invalid_argument");
    assert!(err.message.contains("empty assignment"));
    assert_eq!(stub.state.assign_calls.load(Ordering::SeqCst), 2);
}

// ---------------------------------------------------------------------------
// Capability validation + token precedence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_images_uses_catalog_then_heuristic() {
    let stub = Stub::start().await;
    stub.push_catalog(catalog_proto(vec![
        model_config("vision-uid", false, true),
        model_config("blind-uid", false, false),
    ]));
    let adapter = adapter_for(&stub, "tok");
    adapter.ensure_catalog(&CancellationToken::new()).await;

    let mut request = user_request("look");
    request.messages.push(Message::User(UserMessage {
        content: vec![Content::Image(ImageContent {
            data: "aGk=".to_string(),
            mime_type: "image/png".to_string(),
        })],
        ..Default::default()
    }));

    adapter
        .validate_images_for_model(&request, "vision-uid")
        .expect("vision model accepts images");
    let err = adapter
        .validate_images_for_model(&request, "blind-uid")
        .expect_err("catalog non-vision model rejected");
    assert_eq!(err.code, "invalid_argument");
    assert!(err.message.contains("does not support image inputs"));
    // Unknown uid falls back to the no-vision prefix heuristic.
    adapter
        .validate_images_for_model(&request, "unknown-uid")
        .expect("unknown uid passes");
    adapter
        .validate_images_for_model(&request, "glm-5-2")
        .expect_err("heuristic no-vision uid rejected");
    // No images → no validation.
    adapter
        .validate_images_for_model(&user_request("plain"), "blind-uid")
        .expect("no images skips check");
}

#[tokio::test]
async fn token_source_precedence_and_identity() {
    let stub = Stub::start().await;
    let source_calls = Arc::new(AtomicUsize::new(0));
    let adapter = Adapter::new(AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "config-token".to_string(),
        model: "swe-2-max".to_string(),
        token_source: Some({
            let calls = source_calls.clone();
            Arc::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                "sourced-token".to_string()
            })
        }),
        ..Default::default()
    })
    .expect("adapter");

    // Config token is authoritative until an unauthenticated repair.
    assert_eq!(adapter.current_token().token, "config-token");
    assert_eq!(adapter.current_token().generation, 0);
    assert_eq!(adapter.token_func()(), "config-token");

    // Stale generation → source consulted once, token + generation swap.
    let newer = adapter.repair_token(0).expect("repair installs new token");
    assert_eq!(newer.token, "sourced-token");
    assert_eq!(newer.generation, 1);
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.token_func()(), "sourced-token");

    // A request still holding generation 0 sees the newer generation
    // without another credential-source read.
    let newer = adapter
        .repair_token(0)
        .expect("stale gen sees installed token");
    assert_eq!(newer.generation, 1);
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);

    // Repair at the current generation re-reads the source; the same
    // token back does not count as a repair (Go reloadToken).
    assert!(adapter.repair_token(1).is_none());
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);

    // Same-token source output does not count as a repair (Go reloadToken).
    let adapter2 = Adapter::new(AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "same".to_string(),
        model: "swe-2-max".to_string(),
        token_source: Some(Arc::new(|| "same".to_string())),
        ..Default::default()
    })
    .expect("adapter2");
    assert!(adapter2.repair_token(0).is_none());
    assert_eq!(adapter2.current_token().generation, 0);

    // An empty source return is not a repair either (Go reloadToken).
    let adapter_empty = Adapter::new(AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "t".to_string(),
        model: "swe-2-max".to_string(),
        token_source: Some(Arc::new(String::new)),
        ..Default::default()
    })
    .expect("adapter_empty");
    assert!(adapter_empty.repair_token(0).is_none());

    // A config swap is itself a newer generation — a stale request retries
    // against it without any credential-source read.
    let mut next = adapter2.config().as_ref().clone();
    next.token = "swapped".to_string();
    let (applied, _) = adapter2.apply_config(next);
    assert!(applied.contains(&"devin.token".to_string()));
    let repaired = adapter2.repair_token(0).expect("newer generation");
    assert_eq!(repaired.token, "swapped");

    // Seat client shares the token source but keeps its own identity:
    // Bearer auth + windsurf metadata (task-6 transport contract).
    adapter2
        .seat_client()
        .get_user_status()
        .await
        .expect("seat status");
    let seat = stub.state.seat_meta.lock().unwrap();
    assert_eq!(seat.len(), 1);
    assert_eq!(seat[0].0, "Bearer swapped");
    assert_eq!(seat[0].1, "windsurf");
    assert_eq!(seat[0].2, "windows");
}

#[tokio::test]
async fn apply_config_reports_applied_and_restart_fields() {
    let stub = Stub::start().await;
    let adapter = adapter_for(&stub, "tok");
    let mut next = adapter.config().as_ref().clone();
    next.model = "other-model".to_string();
    next.aliases.insert("a".to_string(), "b".to_string());
    next.client_version = "9.9.9".to_string();
    next.gate.max_rpm = 42;
    next.base_url = "http://elsewhere.invalid".to_string();
    next.proxy = "http://proxy.invalid:8080".to_string();
    let (applied, restart) = adapter.apply_config(next);
    for field in [
        "devin.model",
        "devin.aliases",
        "devin.client_version",
        "devin.max_rpm",
    ] {
        assert!(
            applied.contains(&field.to_string()),
            "missing applied {field}"
        );
    }
    for field in ["devin.base_url", "devin.proxy"] {
        assert!(
            restart.contains(&field.to_string()),
            "missing restart {field}"
        );
    }
    assert_eq!(adapter.config().model, "other-model");
    assert_eq!(adapter.aliases()["a"], "b");
    assert_eq!(adapter.client_identity().version, "9.9.9");
}

#[tokio::test]
async fn adapter_new_validates_required_fields() {
    assert!(matches!(
        Adapter::new(AdapterConfig {
            base_url: String::new(),
            model: "m".to_string(),
            ..Default::default()
        }),
        Err(AdapterError::MissingBaseUrl)
    ));
    assert!(matches!(
        Adapter::new(AdapterConfig {
            base_url: "http://x".to_string(),
            model: String::new(),
            ..Default::default()
        }),
        Err(AdapterError::MissingModel)
    ));
}

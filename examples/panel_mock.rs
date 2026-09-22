//! Disposable mock-backed panel server for `e2e/panel.spec.ts`.
//!
//! Serves the real dashboard router (embedded assets included) on an
//! ephemeral loopback port with seeded history: synthetic `index.jsonl`
//! usage, quota samples, two completed requests and one abortable
//! in-flight request. Two mock-only control routes drive the failure
//! scenarios without touching the product surface:
//!
//! - `POST /mock/revoke-sessions` — revoke all panel sessions (expired
//!   session scenario; the real hot-reload password hook semantics).
//! - `POST /mock/upstream-failure` — body `{"on":true|false}` toggles the
//!   upstream status failure rendered as the panel alert banner.
//!
//! Readiness is signalled on stdout as `PANEL_READY <port>`; the state
//! directory is `PANEL_STATE_DIR` when set, otherwise a fresh temp dir.

use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::body::Bytes;
use axum::routing::post;
use devin2api::dashboard::{Config, ConfigReloadReport, Dashboard, DashboardData};
use devin2api::debuglog::{
    Completion, LogValue, Manager, RequestMeta, RetentionPolicy, STAGE_HTTP_RESPONSE,
};
use devin2api::domain::Usage;
use devin2api::metrics::{Metrics, RejectEvent, RejectReason};
use serde_json::{Value, json};

const PASSWORD: &str = "pw";

struct MockData {
    fail_upstream: AtomicBool,
}

type DataFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

impl DashboardData for MockData {
    fn status(&self) -> DataFuture<'_> {
        Box::pin(async move {
            if self.fail_upstream.load(Ordering::Relaxed) {
                return Err("mock upstream unreachable: connect timeout".to_string());
            }
            let now = jiff::Timestamp::now().as_second();
            Ok(json!({
                "user": {
                    "name": "QA Mock",
                    "email": "qa@example.test",
                    "pro": true,
                    "teams_tier": "TEAMS",
                    "used_prompt_credits": 128,
                    "used_flow_credits": 12,
                },
                "plan_status": {
                    "available_prompt_credits": 1872,
                    "available_flow_credits": 88,
                    "daily_quota_remaining": 62.5,
                    "weekly_quota_remaining": 71.0,
                    "daily_quota_reset": now + 62_000,
                    "weekly_quota_reset": now + 4 * 86_400,
                    "plan_start": "2026-09-01T00:00:00Z",
                    "plan_end": "2026-10-01T00:00:00Z",
                },
                "plan_info": {
                    "plan_name": "Teams Mock",
                    "monthly_prompt_credits": 2000,
                    "monthly_flow_credits": 100,
                    "is_teams": true,
                    "has_paid_features": true,
                },
                "capacity": {"has_capacity": true, "message": "", "active_sessions": 2},
                "ide_status": {"level": "OK", "message": ""},
                "model_statuses": [],
                "providers": [
                    {"provider": "WINDSURF", "display_name": "Windsurf"},
                    {"provider": "ANTHROPIC", "display_name": "Anthropic"},
                ],
            }))
        })
    }

    fn models(&self) -> DataFuture<'_> {
        Box::pin(async {
            Ok(json!({"models": [
                {
                    "uid": "mock-pro-1", "provider": "WINDSURF", "api_provider": "windsurf",
                    "display_name": "Mock Pro 1", "family": "mock", "cost_tier": "high",
                    "credit_multiplier": 2.0, "multiplier_known": true, "pricing_type": "credits",
                    "price_input": 3.0, "price_cached": 0.3, "price_output": 15.0,
                    "context_tokens": 200_000, "supports_images": true, "is_recommended": true,
                    "description": "Mock flagship model"
                },
                {
                    "uid": "mock-lite-2", "provider": "WINDSURF", "api_provider": "windsurf",
                    "display_name": "Mock Lite 2", "family": "mock", "cost_tier": "low",
                    "credit_multiplier": 0.5, "multiplier_known": true, "pricing_type": "credits",
                    "price_input": 0.4, "price_cached": 0.04, "price_output": 1.6,
                    "context_tokens": 128_000, "fast": {"active": true},
                    "description": "Mock fast model"
                },
                {
                    "uid": "mock-free-0", "provider": "ANTHROPIC", "api_provider": "anthropic",
                    "display_name": "Mock Free 0", "family": "mock", "cost_tier": "free",
                    "credit_multiplier": 0, "multiplier_known": true, "pricing_type": "credits",
                    "context_tokens": 64000, "is_new": true, "supports_images": true,
                    "description": "Mock free tier"
                },
                {
                    "uid": "mock-beta-x", "provider": "ANTHROPIC", "api_provider": "anthropic",
                    "display_name": "Mock Beta X", "family": "mock", "cost_tier": "medium",
                    "credit_multiplier": 1.0, "multiplier_known": false, "pricing_type": "credits",
                    "price_input": 1.2, "price_cached": 0.12, "price_output": 6.0,
                    "context_tokens": 100_000, "is_beta": true,
                    "description": "Mock beta model"
                }
            ]}))
        })
    }
}

fn rfc3339(ts: i64) -> String {
    // Same shape as Go's time.RFC3339 in UTC — the panel parses these with
    // Date.parse, which rejects RFC 9557 `[UTC]` suffixes.
    let dt = jiff::Timestamp::from_second(ts)
        .unwrap()
        .to_zoned(jiff::tz::TimeZone::UTC)
        .datetime();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

/// Synthetic index history: three days of traffic across the mock catalog
/// so usage aggregation, the health matrix and the request list all render.
fn seed_index(root: &std::path::Path, now: i64) {
    let mut lines = String::new();
    let models = ["mock-pro-1", "mock-lite-2", "mock-free-0"];
    // Dense recent window: the overview health matrix reads the last 30
    // minutes, so seed it at sub-minute spacing with mixed outcomes.
    for j in 0..40_i64 {
        let at = now - j * 45;
        let model = models[usize::try_from(j).unwrap() % models.len()];
        let entry = json!({
            "dir": format!("mock-recent-{j:03}"),
            "started_at": rfc3339(at),
            "duration_ms": 900 + (j % 5) * 400,
            "first_upstream_ms": 280 + (j % 4) * 90,
            "api": "openai-responses",
            "method": "POST",
            "path": "/v1/responses",
            "status_code": if j % 13 == 0 { 502 } else if j % 7 == 0 { 429 } else { 200 },
            "result": if j % 13 == 0 { "failed" } else { "completed" },
            "requested_model": model,
            "model": model,
            "stream": true,
            "input_tokens": 1_200 + j * 17,
            "output_tokens": 180 + j * 5,
            "cache_read_tokens": 700 + j * 9,
            "total_tokens": 2_080 + j * 31,
            "client_ip": "127.0.0.1",
            "key_hash": "mockkeyhash",
            "error_stage": if j % 13 == 0 { "provider_stream" } else { "" },
            "rate_limited": j % 7 == 0 && j % 13 != 0,
        });
        let mut compact = entry.as_object().unwrap().clone();
        compact.retain(|_, v| !(v.is_string() && v.as_str().unwrap().is_empty()));
        lines.push_str(&serde_json::to_string(&Value::Object(compact)).unwrap());
        lines.push('\n');
    }
    for i in 0..72_i64 {
        let at = now - i * 3_600;
        let model = models[usize::try_from(i).unwrap() % models.len()];
        let failed = i % 17 == 0;
        let limited = !failed && i % 11 == 0;
        let entry = json!({
            "dir": format!("mock-history-{i:03}"),
            "started_at": rfc3339(at),
            "duration_ms": 2_000 + (i % 7) * 900,
            "first_upstream_ms": 350 + (i % 5) * 120,
            "api": if i % 2 == 0 { "openai-responses" } else { "anthropic" },
            "method": "POST",
            "path": if i % 2 == 0 { "/v1/responses" } else { "/v1/messages" },
            "status_code": if failed { 502 } else if limited { 429 } else { 200 },
            "result": if failed { "failed" } else { "completed" },
            "requested_model": model,
            "model": model,
            "stream": true,
            "input_tokens": 1_500 + i * 31,
            "output_tokens": 220 + i * 7,
            "cache_read_tokens": 900 + i * 11,
            "total_tokens": 2_620 + i * 49,
            "client_ip": "127.0.0.1",
            "key_hash": "mockkeyhash",
            "error_stage": if failed { "provider_stream" } else { "" },
            "rate_limited": limited,
        });
        // serde(default) fills what the line omits; empty strings must stay
        // absent to mirror Go's omitempty index encoding.
        let mut compact = entry.as_object().unwrap().clone();
        compact.retain(|_, v| !(v.is_string() && v.as_str().unwrap().is_empty()));
        lines.push_str(&serde_json::to_string(&Value::Object(compact)).unwrap());
        lines.push('\n');
    }
    std::fs::write(root.join("index.jsonl"), lines).unwrap();
}

/// Quota samples over the past 24h so the quota curve and burn forecast
/// render deterministically.
fn seed_quota(root: &std::path::Path, now: i64) {
    let mut lines = String::new();
    for i in (0..24_i32).rev() {
        let at = now - i64::from(i) * 3_600;
        let daily = 90.0 - f64::from(24 - i) * 1.2;
        let weekly = 85.0 - f64::from(24 - i) * 0.6;
        lines.push_str(
            &json!({
                "at": at,
                "daily_remaining": daily,
                "weekly_remaining": weekly,
                "daily_reset_at": now + 62_000,
                "weekly_reset_at": now + 4 * 86_400,
                "prompt_credits": 1872.0,
                "flow_credits": 88.0,
            })
            .to_string(),
        );
        lines.push('\n');
    }
    std::fs::write(root.join("quota.jsonl"), lines).unwrap();
}

/// Seed the three request records the panel exercises: one failed, one
/// completed with merged-view content, one abortable in-flight.
/// Assemble the dashboard with mock data plus the two test-only control
/// routes (session revocation and the upstream-failure toggle).
fn build_router(manager: Arc<Manager>, metrics: Arc<Metrics>) -> Router {
    let data = Arc::new(MockData {
        fail_upstream: AtomicBool::new(false),
    });
    let dashboard = Dashboard::new(Config {
        password: PASSWORD.into(),
        version: "qa-panel-mock".into(),
        token: Arc::new(|| "mock-upstream-token".into()),
        metrics: Some(metrics),
        debug_manager: Some(manager.clone()),
        data: data.clone(),
        config_current: Some(Arc::new(|| {
            json!({
                "path": "/mock/config.yaml",
                "loaded_at": rfc3339(jiff::Timestamp::now().as_second()),
                "stale": false,
                "config": {
                    "devin": {"token": "mock-upstream-token", "base_url": "https://mock.upstream.test", "aliases": {"gpt-mock": "mock-pro-1"}},
                    "auth": {"api_key": "mock-api-key"},
                    "dashboard": {"password": PASSWORD},
                    "server": {"listen": "127.0.0.1:0"},
                    "debug": {"enabled": true},
                },
            })
        })),
        gate_stats: None,
        config_reload: Some(Arc::new(|| {
            Ok(ConfigReloadReport {
                at: rfc3339(jiff::Timestamp::now().as_second()),
                applied: vec!["debug.enabled".into()],
                requires_restart: vec![],
            })
        })),
    });

    let mock = Router::new()
        .route(
            "/mock/revoke-sessions",
            post({
                let dashboard = dashboard.clone();
                move || {
                    let dashboard = dashboard.clone();
                    async move {
                        // Same revocation path as a config reload swapping the
                        // password: every existing session is dropped.
                        dashboard.set_password(PASSWORD.to_string());
                        axum::http::StatusCode::NO_CONTENT
                    }
                }
            }),
        )
        .route(
            "/mock/upstream-failure",
            post(move |body: Bytes| {
                let data = data.clone();
                async move {
                    let on = serde_json::from_slice::<Value>(&body)
                        .ok()
                        .and_then(|v| v["on"].as_bool())
                        .unwrap_or(false);
                    data.fail_upstream.store(on, Ordering::Relaxed);
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        )
        .route(
            "/mock/spawn-active",
            post(move || {
                let manager = manager.clone();
                async move {
                    spawn_active(&manager);
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        );
    dashboard.router().merge(mock)
}

/// One abortable in-flight request; its abort hook completes it with the
/// real Go abort semantics (499 + result=aborted). Reused at startup and by
/// POST /mock/spawn-active (the abort test consumes the seeded request).
fn spawn_active(manager: &Manager) {
    let active = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        api: "openai-chat".into(),
        client_ip: "127.0.0.1".into(),
        user_agent: "panel-e2e".into(),
        ..RequestMeta::default()
    });
    active.set_model("mock-pro-1");
    active.add_client_bytes(512);
    let aborting = active.clone();
    active.set_abort(Arc::new(move || {
        aborting.complete(Completion {
            status_code: 499,
            result: "aborted".into(),
            model: "mock-pro-1".into(),
            requested_model: "mock-pro-1".into(),
            stream: true,
            ..Completion::default()
        });
    }));
}

fn seed_requests(manager: &Manager) {
    // One failed request, completed first so the successful request below
    // sorts as the newest completed row (the requests list is newest-first).
    let failed = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        client_ip: "127.0.0.1".into(),
        ..RequestMeta::default()
    });
    failed.set_model("mock-lite-2");
    failed.write_json(
        "error.json",
        LogValue::serde(json!({"stage": "provider_stream", "error": "upstream 502"})),
    );
    failed.complete(Completion {
        status_code: 502,
        result: "failed".into(),
        model: "mock-lite-2".into(),
        requested_model: "mock-lite-2".into(),
        ..Completion::default()
    });
    // Keep the two started_at timestamps distinct for deterministic ordering.
    std::thread::sleep(Duration::from_millis(2));

    // One completed streaming request with merged-view content.
    let done = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        api: "openai-responses".into(),
        client_ip: "127.0.0.1".into(),
        user_agent: "panel-e2e".into(),
        client_request_id: "e2e-completed".into(),
        ..RequestMeta::default()
    });
    done.set_model("mock-pro-1");
    done.write_json(
        "03-devin-request.json",
        LogValue::serde(json!({"model": "mock-pro-1", "input": "say hello"})),
    );
    done.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.output_text.delta",
        LogValue::serde(json!({"type": "response.output_text.delta", "delta": "Hello "})),
    );
    done.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.output_text.delta",
        LogValue::serde(json!({"type": "response.output_text.delta", "delta": "from the mock"})),
    );
    done.append_jsonl(
    STAGE_HTTP_RESPONSE,
    "response.completed",
    LogValue::serde(
        json!({"type": "response.completed", "response": {"usage": {"input_tokens": 12, "output_tokens": 5, "total_tokens": 17}}}),
    ),
);
    done.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "mock-pro-1".into(),
        requested_model: "mock-pro-1".into(),
        stream: true,
        usage: Usage {
            input: 12,
            output: 5,
            total_tokens: 17,
            ..Usage::default()
        },
        ..Completion::default()
    });

    spawn_active(manager);
}

#[tokio::main]
async fn main() {
    let now = jiff::Timestamp::now().as_second();
    let root = std::env::var_os("PANEL_STATE_DIR").map_or_else(
        || {
            std::env::temp_dir().join(format!(
                "devin2api-panel-mock-{}",
                devin2api::randid::hex(8)
            ))
        },
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("stderr.log"),
        b"ts=2026-09-20T00:00:00Z level=INFO msg=\"mock panel server booted\"\n\
          ts=2026-09-20T00:00:01Z level=INFO msg=\"config loaded\" path=/mock/config.yaml\n\
          ts=2026-09-20T00:00:02Z level=WARN msg=\"upstream slow\" elapsed_ms=1200\n",
    )
    .unwrap();
    seed_index(&root, now);
    seed_quota(&root, now);

    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));

    seed_requests(&manager);

    let metrics = Arc::new(Metrics::new());
    // Fill every 10s trend bucket of the past hour so the traffic chart
    // renders a continuous band; a sparse error every ~7 minutes.
    for i in 0..359_i64 {
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(u64::try_from(now - i * 10).unwrap());
        metrics.seed_trend(at, i % 41 == 0);
        if i % 3 != 0 {
            metrics.seed_trend(at, false);
        }
    }
    metrics.reject(
        RejectReason::InvalidApiKey,
        RejectEvent {
            status: 401,
            path: "/v1/messages".into(),
            ip: "127.0.0.1".into(),
            key_hash: "badkey01".into(),
            user_agent: "curl/8".into(),
            ..RejectEvent::default()
        },
    );

    let router = build_router(manager, metrics);

    let port: u16 = std::env::var("PANEL_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .unwrap();
    let bound = listener.local_addr().unwrap();
    println!("PANEL_READY {}", bound.port());
    std::io::stdout().flush().unwrap();
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

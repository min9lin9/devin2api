use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use devin2api::dashboard::{
    Config, ConfigReloadReport, Dashboard, DashboardData, project_seat_response,
    quota_point_from_status,
};
use devin2api::debuglog::{
    Completion, LogValue, Manager, Recorder, RequestMeta, RetentionPolicy, STAGE_HTTP_RESPONSE,
};
use devin2api::domain::{Failure, RequestMessages};
use devin2api::metrics::Metrics;
use devin2api::server::http::{App, HttpBackend, HttpConfig, HttpEventStream};
use devin2api::upstream::catalog::ModelInfo;
use devin2api::upstream::gate::{Gate, GateConfig, GateStats};
use http::{Request, StatusCode};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

type DataFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

struct SeedData;
impl DashboardData for SeedData {
    fn status(&self) -> DataFuture<'_> {
        Box::pin(async {
            Ok(
                json!({"user":{"email":"qa@example.test"},"plan_status":{"daily_quota_remaining":75}}),
            )
        })
    }
    fn models(&self) -> DataFuture<'_> {
        Box::pin(async {
            Ok(
                json!({"models":[{"uid":"stub-model","price_input":1.0,"price_cached":0.1,"price_output":2.0}]}),
            )
        })
    }
}

struct NeverBackend;
struct NeverStream;
impl HttpEventStream for NeverStream {
    fn recv(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<devin2api::domain::ResponseEvent>, Failure>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { Ok(None) })
    }
}
impl HttpBackend for NeverBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ModelInfo>, Failure>> + Send + '_>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn dashboard_status(&self) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        Box::pin(async { Ok(json!({"capacity":{"has_capacity":true}})) })
    }
    fn dashboard_token(&self) -> Arc<dyn Fn() -> String + Send + Sync> {
        Arc::new(|| "real-token".into())
    }
    fn stream(
        &self,
        _request: RequestMessages,
        _cancel: CancellationToken,
        _recorder: Recorder,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn HttpEventStream>, Failure>> + Send + '_>> {
        Box::pin(async { Ok(Box::new(NeverStream) as Box<dyn HttpEventStream>) })
    }
}

fn temp_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "devin2api-dashboard-{label}-{}",
        devin2api::randid::hex(8)
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

async fn call(
    dashboard: &Dashboard,
    method: &str,
    uri: &str,
    auth: Option<&str>,
    cookie: Option<&str>,
    body: &str,
) -> (http::response::Parts, Vec<u8>) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    if let Some(cookie) = cookie {
        request = request.header("cookie", cookie);
    }
    let response = dashboard
        .router()
        .oneshot(request.body(Body::from(body.to_owned())).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, 8 * 1024 * 1024).await.unwrap().to_vec();
    (parts, body)
}

fn value(body: &[u8]) -> Value {
    serde_json::from_slice(body).unwrap()
}

fn fixture_config(password: &str, manager: Option<Arc<Manager>>) -> Config {
    let current = Arc::new(
        || json!({"config":{"devin":{"token":"upstream-secret","proxy":"http://user:pass@proxy.test"},"dashboard":{"password":"pw"},"auth":{"api_key":"api-secret"}},"stale":false}),
    );
    let reload = Arc::new(|| {
        Ok(ConfigReloadReport {
            at: "2026-09-20T00:00:00Z".into(),
            applied: vec!["dashboard.password".into()],
            requires_restart: vec!["server.listen".into()],
        })
    });
    Config {
        password: password.into(),
        version: "qa-dashboard".into(),
        token: Arc::new(|| "upstream-secret".into()),
        metrics: Some(Arc::new(Metrics::new())),
        debug_manager: manager,
        data: Arc::new(SeedData),
        gate_stats: None,
        config_current: Some(current),
        config_reload: Some(reload),
    }
}

fn fixture(password: &str, manager: Option<Arc<Manager>>) -> Dashboard {
    Dashboard::new(fixture_config(password, manager))
}

#[test]
fn seat_top_up_root_plan_info_and_quota_projection_match_go() {
    let status = project_seat_response(&json!({
        "userStatus": {
            "name": "QA",
            "planStatus": {
                "dailyQuotaRemainingPercent": 42.5,
                "gracePeriodStatus": "GRACE_PERIOD_STATUS_ACTIVE",
                "gracePeriodEnd": "2026-09-20T16:00:00Z",
                "topUpStatus": {
                    "topUpEnabled": true,
                    "topUpTransactionStatus": "TRANSACTION_STATUS_SUCCEEDED",
                    "monthlyTopUpAmount": 20,
                    "topUpSpent": 3,
                    "topUpIncrement": 5,
                    "topUpCriteriaMet": true
                },
                "planInfo": {"planName":"nested-plan"}
            }
        },
        "planInfo": {
            "planName": "root-plan",
            "monthlyPromptCredits": 1000,
            "monthlyFlowCredits": 2000,
            "billingStrategy": "BILLING_STRATEGY_MONTHLY",
            "isTeams": true,
            "isEnterprise": false,
            "hasPaidFeatures": true,
            "maxNumPremiumChatMessages": 99
        }
    }));
    assert_eq!(status["plan_status"]["top_up_status"]["enabled"], true);
    assert_eq!(
        status["plan_status"]["top_up_status"]["transaction_status"],
        "SUCCEEDED"
    );
    assert_eq!(status["plan_status"]["top_up_status"]["monthly_amount"], 20);
    assert_eq!(status["plan_info"]["plan_name"], "root-plan");
    assert_eq!(status["plan_info"]["max_premium_chat_messages"], 99);

    let point = quota_point_from_status(&status, 1_700_000_000).unwrap();
    assert_eq!(point["grace_period_end"], 1_789_920_000_i64);
    assert_eq!(point["top_up_enabled"], true);
    assert_eq!(point["top_up_transaction_status"], "SUCCEEDED");
}

#[tokio::test]
async fn production_http_router_mounts_panel_and_runtime_password_updates_auth() {
    let app = App::with_backend(
        NeverBackend,
        HttpConfig {
            dashboard_password: "pw".into(),
            dashboard_config_current: Some(Arc::new(
                || json!({"config":{"devin":{"token":"real-token"}},"stale":false}),
            )),
            dashboard_config_reload: Some(Arc::new(|| {
                Ok(ConfigReloadReport {
                    at: "2026-09-20T00:00:00Z".into(),
                    applied: vec![],
                    requires_restart: vec![],
                })
            })),
            ..HttpConfig::default()
        },
    );
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api/config")
                .header("authorization", "Bearer pw")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api/status")
                .header("authorization", "Bearer pw")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(value(&body)["capacity"]["has_capacity"], true);
    app.set_dashboard_password("next".into());
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api")
                .header("authorization", "Bearer pw")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn authentication_cookie_bearer_and_shared_lockout_match_panel_contract() {
    let dashboard = fixture("pw", None);
    let (parts, body) = call(&dashboard, "GET", "/panel/api", None, None, "").await;
    assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
    assert_eq!(value(&body), json!({"error":"未授权"}));

    for _ in 0..5 {
        let (parts, _) = call(
            &dashboard,
            "GET",
            "/panel/api",
            Some("Bearer wrong"),
            None,
            "",
        )
        .await;
        assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
    }
    let (parts, body) = call(
        &dashboard,
        "GET",
        "/panel/api",
        Some("Bearer wrong"),
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(value(&body), json!({"error":"登录尝试过多，请稍后再试"}));
    let (parts, _) = call(&dashboard, "GET", "/panel/api", Some("Bearer pw"), None, "").await;
    assert_eq!(
        parts.status,
        StatusCode::OK,
        "correct Bearer bypasses lockout"
    );

    let (parts, body) = call(
        &dashboard,
        "POST",
        "/panel/login",
        None,
        None,
        "password=pw",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(value(&body), json!({"ok":true}));
    let set_cookie = parts.headers["set-cookie"].to_str().unwrap();
    assert!(set_cookie.contains("devin_panel_session="));
    assert!(
        set_cookie.contains("HttpOnly")
            && set_cookie.contains("SameSite=Lax")
            && set_cookie.contains("Max-Age=86400")
    );
    let cookie = set_cookie.split(';').next().unwrap();
    let (parts, _) = call(
        &dashboard,
        "GET",
        "/panel/api/status",
        None,
        Some(cookie),
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
}

#[tokio::test]
async fn lockout_uses_transport_peer_and_ignores_forwarded_ip_headers() {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    let dashboard = fixture("pw", None);
    let peer_one = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 1111);
    let peer_two = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 2222);
    let wrong = |peer| {
        dashboard.router().layer(axum::Extension(peer)).oneshot(
            Request::builder()
                .uri("/panel/api")
                .header("authorization", "Bearer wrong")
                .header("x-real-ip", "203.0.113.9")
                .body(Body::empty())
                .unwrap(),
        )
    };
    for _ in 0..5 {
        assert_eq!(
            wrong(peer_one).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        wrong(peer_two).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        wrong(peer_one).await.unwrap().status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

// One sequential route sweep; splitting would scatter the scenario.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn every_admin_route_and_history_behavior_is_wired() {
    let root = temp_root("routes");
    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/messages".into(),
        api: "anthropic".into(),
        client_request_id: "=formula".into(),
        ..RequestMeta::default()
    });
    recorder.set_model("stub-model");
    recorder.write_json(
        "03-devin-request.json",
        LogValue::serde(json!({"model":"stub-model","token":"upstream-secret"})),
    );
    recorder.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.output_text.delta",
        LogValue::serde(json!({"type":"response.output_text.delta","delta":"hello"})),
    );
    recorder.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.completed",
        LogValue::serde(
            json!({"type":"response.completed","response":{"usage":{"output_tokens":2}}}),
        ),
    );
    let dir = recorder.dir_name();
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "stub-model".into(),
        ..Completion::default()
    });
    let dashboard = fixture("pw", Some(manager.clone()));
    let bearer = Some("Bearer pw");

    for route in [
        "/panel/api",
        "/panel/api/status",
        "/panel/api/models",
        "/panel/api/stats",
        "/panel/api/requests",
        "/panel/api/requests/matrix",
        "/panel/api/requests/export",
        "/panel/api/requests/active",
        "/panel/api/quota",
        "/panel/api/usage",
        "/panel/api/config",
    ] {
        let (parts, _) = call(&dashboard, "GET", route, bearer, None, "").await;
        assert_eq!(parts.status, StatusCode::OK, "GET {route}");
    }
    let (_, body) = call(
        &dashboard,
        "GET",
        "/panel/api/requests?model=stub-model&status=2xx&limit=1",
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(value(&body)["requests"].as_array().unwrap().len(), 1);
    assert_eq!(value(&body)["has_more"], false);
    let (_, body) = call(
        &dashboard,
        "GET",
        "/panel/api/requests?offset=99",
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(value(&body)["requests"], json!([]));

    let (parts, csv) = call(
        &dashboard,
        "GET",
        "/panel/api/requests/export?format=csv",
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.headers["content-type"], "text/csv; charset=utf-8");
    assert!(
        String::from_utf8_lossy(&csv).contains("'=formula"),
        "CSV injection prefix is escaped"
    );

    let (_, body) = call(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(value(&body)["dir"], dir);
    let (_, body) = call(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/file/03-devin-request.json"),
        bearer,
        None,
        "",
    )
    .await;
    assert!(
        !value(&body)["text"]
            .as_str()
            .unwrap()
            .contains("upstream-secret")
    );
    let (_, body) = call(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/merged"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(value(&body)["text"], "hello");
    assert_eq!(value(&body)["truncated"], false);

    let (parts, body) = call(
        &dashboard,
        "POST",
        "/panel/api/debug/toggle",
        bearer,
        None,
        r#"{"enabled":false}"#,
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(value(&body), json!({"enabled":false}));
    let (parts, body) = call(
        &dashboard,
        "POST",
        "/panel/api/config/reload",
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(value(&body)["requires_restart"], json!(["server.listen"]));
    let (_, body) = call(&dashboard, "GET", "/panel/api/config", bearer, None, "").await;
    let config = String::from_utf8_lossy(&body);
    assert!(!config.contains("upstream-secret"));
    assert!(!config.contains("api-secret"));
    assert!(!config.contains("user:pass"));
    assert!(config.contains("sha256:"));
    assert!(config.contains("http://proxy.test"));

    manager.close();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn active_abort_truncation_and_path_escape_are_defended() {
    let root = temp_root("security");
    let manager = Arc::new(Manager::new(&root, &RetentionPolicy::default()));
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        ..RequestMeta::default()
    });
    let dir = recorder.dir_name();
    recorder.set_abort(Arc::new(|| {}));
    let oversized = vec![b'x'; (4 << 20) + 1];
    std::fs::write(recorder.directory_path().join("oversized.json"), oversized).unwrap();
    std::fs::write(root.join("outside.json"), b"secret outside").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        root.join("outside.json"),
        recorder.directory_path().join("escape.json"),
    )
    .unwrap();
    let dashboard = fixture("pw", Some(manager.clone()));
    let bearer = Some("Bearer pw");

    let (_, body) = call(
        &dashboard,
        "GET",
        "/panel/api/requests/active",
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(value(&body)["active"].as_array().unwrap().len(), 1);
    let (parts, body) = call(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/file/oversized.json"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(value(&body)["truncated"], true);
    #[cfg(unix)]
    {
        let (parts, _) = call(
            &dashboard,
            "GET",
            &format!("/panel/api/requests/{dir}/file/escape.json"),
            bearer,
            None,
            "",
        )
        .await;
        assert_eq!(
            parts.status,
            StatusCode::NOT_FOUND,
            "symlinks cannot escape the request directory"
        );
    }
    let (parts, _) = call(
        &dashboard,
        "GET",
        &format!("/panel/api/requests/{dir}/file/../../outside.json"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::NOT_FOUND);
    let (parts, body) = call(
        &dashboard,
        "POST",
        &format!("/panel/api/requests/{dir}/abort"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(value(&body), json!({"aborted":true}));
    // Like Go's context.CancelFunc, abort remains idempotently callable until
    // the request completes and clears its callback.
    let (parts, _) = call(
        &dashboard,
        "POST",
        &format!("/panel/api/requests/{dir}/abort"),
        bearer,
        None,
        "",
    )
    .await;
    assert_eq!(parts.status, StatusCode::OK);

    recorder.complete(Completion {
        status_code: 499,
        result: "aborted".into(),
        ..Completion::default()
    });
    manager.close();
    std::fs::remove_dir_all(root).unwrap();
}

/// `/panel/api/stats` must carry the `gate` section when a gate-stats
/// provider is configured (Go `payload["gate"] = h.gateStats()`), with the
/// same field names Go emits; without a provider the key stays absent.
/// The App-level half proves the `HttpBackend::gate_stats` wiring reaches
/// the mounted panel.
#[tokio::test]
async fn stats_includes_gate_section_when_provider_is_configured() {
    struct GateBackend {
        gate: Arc<Gate>,
    }
    impl HttpBackend for GateBackend {
        fn list_models(
            &self,
            _cancel: CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<ModelInfo>, Failure>> + Send + '_>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn stream(
            &self,
            _request: RequestMessages,
            _cancel: CancellationToken,
            _recorder: Recorder,
        ) -> Pin<Box<dyn Future<Output = Result<Box<dyn HttpEventStream>, Failure>> + Send + '_>>
        {
            Box::pin(async { Ok(Box::new(NeverStream) as Box<dyn HttpEventStream>) })
        }
        fn gate_stats(&self) -> Option<GateStats> {
            Some(self.gate.stats())
        }
    }
    let bearer = Some("Bearer pw");

    let without = fixture("pw", None);
    let (_, body) = call(&without, "GET", "/panel/api/stats", bearer, None, "").await;
    assert!(
        value(&body).get("gate").is_none(),
        "no provider means no gate key"
    );

    let gate = Arc::new(Gate::new(
        GateConfig {
            max_rpm: 80,
            ..GateConfig::default()
        },
        None,
    ));
    let provider_gate = gate.clone();
    let mut config = fixture_config("pw", None);
    config.gate_stats = Some(Arc::new(move || Some(provider_gate.stats())));
    let dashboard = Dashboard::new(config);
    let (parts, body) = call(&dashboard, "GET", "/panel/api/stats", bearer, None, "").await;
    assert_eq!(parts.status, StatusCode::OK);
    let gate_section = &value(&body)["gate"];
    for key in [
        "latched",
        "latch_count",
        "drip_count",
        "reject_latched_count",
        "reject_hold_count",
        "window_quota",
        "window_used",
        "window_open",
        "window_next",
        "sendable",
        "waiters",
    ] {
        assert!(
            gate_section.get(key).is_some(),
            "gate stats missing {key}: {gate_section}"
        );
    }
    assert_eq!(gate_section["window_quota"], 80);
    assert_eq!(gate_section["latched"], false);
    assert_eq!(gate_section["waiters"], 0);

    let app = App::with_backend(
        GateBackend { gate },
        HttpConfig {
            dashboard_password: "pw".into(),
            ..HttpConfig::default()
        },
    );
    let response = app
        .router()
        .oneshot(
            Request::builder()
                .uri("/panel/api/stats")
                .header("authorization", "Bearer pw")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let gate_section = &value(&body)["gate"];
    assert_eq!(
        gate_section["window_quota"], 80,
        "backend gate stats must reach the panel: {gate_section}"
    );
    assert_eq!(gate_section["latched"], false);
}

//! Task 18: embedded panel assets, template login/panel selection and the
//! Go `serveStatic` MIME/cache/traversal contract.

use axum::body::{Body, to_bytes};
use devin2api::dashboard::{Config, Dashboard};
use devin2api::metrics::Metrics;
use http::{Request, StatusCode};
use std::io::Read as _;
use std::sync::Arc;
use tower::ServiceExt as _;

fn fixture(password: &str) -> Dashboard {
    Dashboard::new(Config {
        password: password.into(),
        version: "qa-assets".into(),
        metrics: Some(Arc::new(Metrics::new())),
        ..Config::default()
    })
}

async fn call(
    dashboard: &Dashboard,
    uri: &str,
    headers: &[(&str, &str)],
) -> (http::response::Parts, Vec<u8>) {
    let mut request = Request::builder().method("GET").uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = dashboard
        .router()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let body = to_bytes(body, 16 * 1024 * 1024).await.unwrap().to_vec();
    (parts, body)
}

async fn login_cookie(dashboard: &Dashboard, password: &str) -> String {
    let response = dashboard
        .router()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/panel/login")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!("password={password}")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let set_cookie = response.headers()["set-cookie"].to_str().unwrap();
    set_cookie.split(';').next().unwrap().to_string()
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .read_to_end(&mut out)
        .expect("gzip body decodes");
    out
}

#[tokio::test]
async fn panel_serves_login_or_dashboard_template_with_version_stamp() {
    let guarded = fixture("pw");
    let (parts, body) = call(&guarded, "/panel", &[]).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(parts.headers["content-type"], "text/html; charset=utf-8");
    let html = String::from_utf8(body).unwrap();
    assert!(html.contains("id=\"loginForm\""), "login form renders");
    assert!(
        html.contains("/panel/static/panel.css?v=qa-assets"),
        "version stamp substitutes __VERSION__"
    );
    assert!(!html.contains("__VERSION__"));
    assert!(!html.contains("page-overview"), "panel chrome stays hidden");

    let cookie = login_cookie(&guarded, "pw").await;
    let (parts, body) = call(&guarded, "/panel", &[("cookie", &cookie)]).await;
    assert_eq!(parts.status, StatusCode::OK);
    let html = String::from_utf8(body).unwrap();
    assert!(html.contains("id=\"page-overview\""), "panel renders");
    assert!(html.contains("/panel/static/js/boot.js?v=qa-assets"));
    assert!(!html.contains("__VERSION__"));

    // Empty password = open access, no login interstitial.
    let open = fixture("");
    let (parts, body) = call(&open, "/panel", &[]).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("id=\"page-overview\"")
    );

    // The version stamp falls back to `dev` exactly like Go.
    let dev = Dashboard::new(Config::default());
    let (_, body) = call(&dev, "/panel", &[]).await;
    assert!(String::from_utf8(body).unwrap().contains("?v=dev"));
}

#[tokio::test]
async fn static_assets_serve_with_mime_cache_and_auth_boundaries() {
    let dashboard = fixture("pw");
    let cookie = login_cookie(&dashboard, "pw").await;
    let auth = [("cookie", cookie.as_str())];

    // panel.css is the deliberate unauthenticated exception: the login page
    // shares its design tokens.
    let (parts, body) = call(&dashboard, "/panel/static/panel.css", &[]).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(parts.headers["content-type"], "text/css; charset=utf-8");
    assert_eq!(parts.headers["cache-control"], "private, no-cache");
    assert_eq!(parts.headers["vary"], "Accept-Encoding");
    let etag = parts.headers["etag"].to_str().unwrap().to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"') && etag.len() == 34);
    assert!(String::from_utf8(body).unwrap().contains("--bg"));

    // Everything else requires the session.
    let (parts, _) = call(&dashboard, "/panel/static/js/core.js", &[]).await;
    assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
    let (parts, body) = call(&dashboard, "/panel/static/js/core.js", &auth).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(
        parts.headers["content-type"],
        "text/javascript; charset=utf-8"
    );
    assert!(String::from_utf8(body).unwrap().contains("morphdom"));

    // Nested vendor asset resolves; unknown names and extensions fall back.
    let (parts, _) = call(&dashboard, "/panel/static/js/vendor/morphdom.js", &auth).await;
    assert_eq!(parts.status, StatusCode::OK);
    let (parts, _) = call(&dashboard, "/panel/static/nope.js", &auth).await;
    assert_eq!(parts.status, StatusCode::NOT_FOUND);
    let (parts, _) = call(&dashboard, "/panel/static/login.html", &auth).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(
        parts.headers["content-type"], "application/octet-stream",
        "extensions outside the Go MIME switch stay octet-stream"
    );
}

#[tokio::test]
async fn static_etag_revalidation_and_gzip_match_go() {
    let dashboard = fixture("");
    let (parts, _) = call(&dashboard, "/panel/static/panel.css", &[]).await;
    let etag = parts.headers["etag"].to_str().unwrap().to_string();

    let (parts, body) = call(
        &dashboard,
        "/panel/static/panel.css",
        &[("if-none-match", &etag)],
    )
    .await;
    assert_eq!(parts.status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(parts.headers["etag"], etag);
    assert_eq!(parts.headers["vary"], "Accept-Encoding");

    // echarts is far above the 1KB gzip threshold: negotiated gzip must
    // decode to the exact embedded bytes.
    let (parts, plain) = call(&dashboard, "/panel/static/echarts.min.js", &[]).await;
    assert!(!parts.headers.contains_key("content-encoding"));
    let (parts, gz) = call(
        &dashboard,
        "/panel/static/echarts.min.js",
        &[("accept-encoding", "gzip")],
    )
    .await;
    assert_eq!(parts.headers["content-encoding"], "gzip");
    assert!(gz.len() < plain.len());
    assert_eq!(gunzip(&gz), plain);
    assert!(
        String::from_utf8(plain)
            .unwrap()
            .contains("Bundled license information"),
        "third-party license banner survives embedding"
    );

    // Small assets skip gzip: the header overhead outweighs the savings.
    let (parts, _) = call(
        &dashboard,
        "/panel/static/js/boot.js",
        &[("accept-encoding", "gzip")],
    )
    .await;
    assert!(!parts.headers.contains_key("content-encoding"));

    // Conditional requests win over content negotiation, like Go.
    let (parts, body) = call(
        &dashboard,
        "/panel/static/panel.css",
        &[("if-none-match", &etag), ("accept-encoding", "gzip")],
    )
    .await;
    assert_eq!(parts.status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
}

#[tokio::test]
async fn static_path_traversal_is_rejected_after_cleaning() {
    let dashboard = fixture("");
    for uri in [
        "/panel/static/../src/dashboard.rs",
        "/panel/static/%2e%2e/%2e%2e/Cargo.toml",
        "/panel/static/../../etc/passwd",
        "/panel/static//etc/passwd",
        "/panel/static/",
    ] {
        let (parts, _) = call(&dashboard, uri, &[]).await;
        assert_eq!(
            parts.status,
            StatusCode::NOT_FOUND,
            "{uri} must not escape the embedded tree"
        );
    }
    // Go's path.Clean keeps in-tree dot segments working.
    let (parts, _) = call(&dashboard, "/panel/static/js/../panel.css", &[]).await;
    assert_eq!(parts.status, StatusCode::OK);
    assert_eq!(parts.headers["content-type"], "text/css; charset=utf-8");
}

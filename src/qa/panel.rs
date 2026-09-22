//! Process-surface QA for task 18's embedded panel: byte-identical asset
//! serving against the Go source tree, route/fallback/auth boundaries, and
//! real-browser desktop/mobile screenshots of the existing panel UI.
//!
//! The dashboard router is served on an ephemeral loopback port exactly as
//! production wires it (task 14 merges `Dashboard::router()`); a fixed
//! loopback `SocketAddr` extension stands in for the per-connection peer
//! address the production accept loop injects.

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::dashboard::{Config, Dashboard, DashboardData};
use crate::debuglog::{
    Completion, LogValue, Manager, RequestMeta, RetentionPolicy, STAGE_HTTP_RESPONSE,
};
use crate::metrics::Metrics;

const PASSWORD: &str = "pw";
const BROWSER_WAIT: Duration = Duration::from_secs(15);
/// Panel assets whose bytes intentionally differ from the Go source: the
/// approved runtime-diagnostics rendering change (plan exception 5). Every
/// other file must be byte-identical to `G/internal/dashboard/static/`.
const APPROVED_DIVERGENT: &[&str] = &["js/core.js", "js/tab-overview.js", "js/tab-system.js"];

/// Counts elements wider than the viewport that are neither inside a
/// horizontal scroll container nor inside the upstream components that are
/// wide by original design: the `.mx` health matrix (no overflow-x wrapper)
/// and `.chart-grid` (minmax(430px,1fr) columns). Both overflow a 390px
/// viewport in the Go original too — preserved, not a Rust regression.
const OVERFLOW_PROBE: &str = r"(()=>{
const d=document.documentElement;let outside=0;const names=[];
document.querySelectorAll('*').forEach(e=>{
  const r=e.getBoundingClientRect();
  if(r.right<=d.clientWidth+1&&r.left>=-1)return;
  let n=e,known=false,contained=false;
  while(n){
    if(n.classList&&(n.classList.contains('mx')||n.classList.contains('chart-grid')))known=true;
    const cs=getComputedStyle(n);
    if(['auto','scroll'].includes(cs.overflowX)&&n.scrollWidth>n.clientWidth)contained=true;
    n=n.parentElement;
  }
  if(!contained&&!known){outside++;if(names.length<8)names.push(e.tagName+'.'+e.className)}
});
return {scroll_width:d.scrollWidth,client_width:d.clientWidth,uncontained_outside_mx:outside,examples:names}})()";

type DataFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

struct PanelData {
    fail_upstream: AtomicBool,
}

impl DashboardData for PanelData {
    fn status(&self) -> DataFuture<'_> {
        Box::pin(async move {
            if self.fail_upstream.load(Ordering::Relaxed) {
                return Err("mock upstream unreachable: connect timeout".to_string());
            }
            Ok(json!({
                "user": {"name": "QA Mock", "email": "qa@example.test", "pro": true},
                "plan_status": {"available_prompt_credits": 1872, "daily_quota_remaining": 90},
                "capacity": {"has_capacity": true, "active_sessions": 1},
            }))
        })
    }
    fn models(&self) -> DataFuture<'_> {
        Box::pin(async move {
            if self.fail_upstream.load(Ordering::Relaxed) {
                return Err("mock upstream unreachable: connect timeout".to_string());
            }
            Ok(json!({"models":[{"id":"stub-model","display_name":"Stub Model"}]}))
        })
    }
}

struct Fixture {
    dashboard: Dashboard,
    data: Arc<PanelData>,
    manager: Arc<Manager>,
    request_dir: String,
}

fn fixture(evidence: &Path) -> anyhow::Result<Fixture> {
    let logs = evidence.join("panel-state");
    let _ = std::fs::remove_dir_all(&logs);
    std::fs::create_dir_all(&logs)?;
    std::fs::write(logs.join("stderr.log"), b"panel qa process log\n")?;
    std::fs::write(
        logs.join("quota.jsonl"),
        b"{\"at\":1700000000,\"daily_remaining\":90}\n{\"at\":1700003600,\"daily_remaining\":80}\n",
    )?;
    let manager = Arc::new(Manager::new(&logs, &RetentionPolicy::default()));
    let recorder = manager.start(&RequestMeta {
        method: "POST".into(),
        path: "/v1/responses".into(),
        api: "openai-responses".into(),
        client_request_id: "qa".into(),
        ..RequestMeta::default()
    });
    recorder.set_model("stub-model");
    recorder.write_json(
        "03-devin-request.json",
        LogValue::serde(json!({"model":"stub-model"})),
    );
    recorder.append_jsonl(
        STAGE_HTTP_RESPONSE,
        "response.output_text.delta",
        LogValue::serde(json!({"type":"response.output_text.delta","delta":"pong"})),
    );
    let request_dir = recorder.dir_name();
    recorder.complete(Completion {
        status_code: 200,
        result: "completed".into(),
        model: "stub-model".into(),
        ..Completion::default()
    });
    let data = Arc::new(PanelData {
        fail_upstream: AtomicBool::new(false),
    });
    let dashboard = Dashboard::new(Config {
        password: PASSWORD.into(),
        version: "qa-panel".into(),
        metrics: Some(Arc::new(Metrics::new())),
        debug_manager: Some(manager.clone()),
        data: data.clone(),
        ..Config::default()
    });
    Ok(Fixture {
        dashboard,
        data,
        manager,
        request_dir,
    })
}

/// Serve the dashboard router on an ephemeral loopback port. The extension
/// supplies the peer address `peer_identity` reads for lockout keys; on
/// loopback every client is 127.0.0.1 anyway.
async fn serve(dashboard: &Dashboard) -> anyhow::Result<(u16, CancellationToken)> {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let port = listener.local_addr()?.port();
    let stop = CancellationToken::new();
    let app = dashboard
        .router()
        .layer(axum::Extension("127.0.0.1:0".parse::<SocketAddr>()?));
    let shutdown = stop.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await;
    });
    Ok((port, stop))
}

fn sha256_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let sum: [u8; 32] = Sha256::digest(data).into();
    sum.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Recursively list files under `dir` with `/`-separated relative names.
fn list_files(dir: &Path, prefix: &str, out: &mut Vec<String>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = format!("{prefix}{}", entry.file_name().to_string_lossy());
        if entry.file_type()?.is_dir() {
            list_files(&entry.path(), &format!("{name}/"), out)?;
        } else {
            out.push(name);
        }
    }
    Ok(())
}

struct Check {
    name: String,
    ok: bool,
    detail: String,
}

impl Check {
    fn new(name: &str, ok: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            ok,
            detail: detail.into(),
        }
    }
}

struct Http {
    client: reqwest::Client,
    base: String,
}

impl Http {
    fn new(port: u16) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            client,
            base: format!("http://127.0.0.1:{port}"),
        })
    }

    async fn get(&self, path: &str, headers: &[(&str, &str)]) -> anyhow::Result<reqwest::Response> {
        let mut request = self.client.get(format!("{}{path}", self.base));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        Ok(request.send().await?)
    }

    async fn login_cookie(&self) -> anyhow::Result<String> {
        let response = self
            .client
            .post(format!("{}/panel/login", self.base))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("password={PASSWORD}"))
            .send()
            .await?;
        anyhow::ensure!(
            response.status() == 200,
            "login status {}",
            response.status()
        );
        let cookie = response
            .headers()
            .get("set-cookie")
            .context("login set-cookie")?
            .to_str()?;
        Ok(cookie.split(';').next().unwrap_or_default().to_string())
    }
}

/// Every embedded asset must serve byte-identically; every Go source asset
/// must be embedded byte-identically except the approved diagnostics files.
async fn check_assets(
    http: &Http,
    cookie: &str,
    go_root: &Path,
    checks: &mut Vec<Check>,
) -> anyhow::Result<()> {
    let embedded_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/panel");
    let go_static = go_root.join("internal/dashboard/static");
    let mut names = Vec::new();
    list_files(&embedded_root, "", &mut names)?;
    names.sort();

    let mut go_names = Vec::new();
    if go_static.is_dir() {
        list_files(&go_static, "", &mut go_names)?;
        go_names.sort();
    }
    let missing: Vec<String> = go_names
        .iter()
        .filter(|n| !names.contains(n))
        .cloned()
        .collect();
    checks.push(Check::new(
        "all Go static files embedded",
        missing.is_empty(),
        if missing.is_empty() {
            format!("{} files", go_names.len())
        } else {
            format!("missing: {}", missing.join(","))
        },
    ));
    let extra: Vec<String> = names
        .iter()
        .filter(|n| !go_names.contains(n) && n.as_str() != "NOTICE.md")
        .cloned()
        .collect();
    checks.push(Check::new(
        "no unexpected embedded files",
        extra.is_empty(),
        if extra.is_empty() {
            "only NOTICE.md added".to_string()
        } else {
            format!("extra: {}", extra.join(","))
        },
    ));

    let auth = [("cookie", cookie)];
    for name in &names {
        if name == "NOTICE.md" {
            continue;
        }
        let embedded = std::fs::read(embedded_root.join(name))?;
        let response = http.get(&format!("/panel/static/{name}"), &auth).await?;
        let status = response.status().as_u16();
        let served = response.bytes().await?.to_vec();
        let served_ok = status == 200 && served == embedded;
        let mut detail = format!("status {status}, {} bytes", served.len());
        if go_static.is_dir() {
            let go_path = go_static.join(name);
            if go_path.is_file() {
                let go_bytes = std::fs::read(&go_path)?;
                if go_bytes == embedded {
                    detail.push_str(", identical to Go source");
                } else if APPROVED_DIVERGENT.contains(&name.as_str()) {
                    detail.push_str(", approved divergence (Rust diagnostics rendering)");
                } else {
                    checks.push(Check::new(
                        &format!("asset {name} byte-identical to Go"),
                        false,
                        format!(
                            "go sha256 {} != embedded {}",
                            sha256_hex(&go_bytes),
                            sha256_hex(&embedded)
                        ),
                    ));
                    checks.push(Check::new(
                        &format!("asset {name} serves embedded bytes"),
                        served_ok,
                        detail,
                    ));
                    continue;
                }
            }
        }
        checks.push(Check::new(
            &format!("asset {name} serves embedded bytes"),
            served_ok,
            detail,
        ));
    }
    Ok(())
}

/// Route surface and fallback behavior: Go registers only `/panel`,
/// `/panel/login`, `/panel/static/*` and `/panel/api*` — everything else
/// under /panel is a 404 (there is no SPA history fallback to preserve).
// One route table; splitting scatters the endpoint matrix.
#[allow(clippy::too_many_lines)]
async fn check_routes(http: &Http, cookie: &str, checks: &mut Vec<Check>) -> anyhow::Result<()> {
    let auth = [("cookie", cookie)];

    let response = http.get("/panel", &[]).await?;
    let body = response.text().await?;
    checks.push(Check::new(
        "unauthenticated /panel renders login",
        body.contains("id=\"loginForm\"") && !body.contains("__VERSION__"),
        format!("{} bytes", body.len()),
    ));
    let response = http.get("/panel", &auth).await?;
    let body = response.text().await?;
    checks.push(Check::new(
        "authenticated /panel renders panel with version stamp",
        body.contains("id=\"page-overview\"") && body.contains("?v=qa-panel"),
        format!("{} bytes", body.len()),
    ));

    for (path, expected, note) in [
        ("/panel/", 404_u16, "trailing slash is not a route"),
        (
            "/panel/requests",
            404,
            "deep link: no SPA fallback, same as Go",
        ),
        ("/panel/api/unknown", 404, "unknown API suffix"),
        ("/panel/static", 404, "bare static prefix"),
        ("/panel/static/", 404, "empty static name"),
        ("/panel/static/nope.js", 404, "missing asset"),
        ("/panel/static/../Cargo.toml", 404, "traversal rejected"),
    ] {
        let status = http.get(path, &auth).await?.status().as_u16();
        checks.push(Check::new(
            &format!("{path} -> {expected}"),
            status == expected,
            format!("{note}; got {status}"),
        ));
    }

    // Auth boundaries: panel.css is the deliberate open exception.
    let status = http.get("/panel/static/panel.css", &[]).await?.status();
    checks.push(Check::new(
        "panel.css unauthenticated (login page styling)",
        status == 200,
        format!("{status}"),
    ));
    let status = http.get("/panel/static/js/core.js", &[]).await?.status();
    checks.push(Check::new(
        "other static assets require auth",
        status == 401,
        format!("{status}"),
    ));
    let status = http.get("/panel/api", &[]).await?.status();
    checks.push(Check::new(
        "panel API requires auth",
        status == 401,
        format!("{status}"),
    ));

    // MIME / cache / revalidation contract.
    let response = http.get("/panel/static/panel.css", &[]).await?;
    let headers = response.headers().clone();
    let etag = headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    checks.push(Check::new(
        "panel.css MIME/cache headers",
        headers.get("content-type").and_then(|v| v.to_str().ok())
            == Some("text/css; charset=utf-8")
            && headers.get("cache-control").and_then(|v| v.to_str().ok())
                == Some("private, no-cache")
            && etag.starts_with('"'),
        format!("etag {etag}"),
    ));
    let status = http
        .get("/panel/static/panel.css", &[("if-none-match", &etag)])
        .await?
        .status();
    checks.push(Check::new(
        "etag revalidation returns 304",
        status == 304,
        format!("{status}"),
    ));
    let response = http
        .get(
            "/panel/static/echarts.min.js",
            &[("cookie", cookie), ("accept-encoding", "gzip")],
        )
        .await?;
    let gz = response
        .headers()
        .get("content-encoding")
        .is_some_and(|v| v == "gzip");
    checks.push(Check::new(
        "gzip negotiation for large assets",
        gz,
        format!(
            "content-encoding {:?}",
            response.headers().get("content-encoding")
        ),
    ));

    // Login failure path.
    let response = http
        .client
        .post(format!("{}/panel/login", http.base))
        .header("content-type", "application/x-www-form-urlencoded")
        .body("password=wrong")
        .send()
        .await?;
    checks.push(Check::new(
        "wrong password -> 401",
        response.status() == 401,
        format!("{}", response.status()),
    ));
    Ok(())
}

/// API contract spot checks on the real router (full coverage is task 17's
/// `dashboard-api` suite; here we verify the panel's own surface works).
async fn check_api(
    http: &Http,
    cookie: &str,
    dir: &str,
    checks: &mut Vec<Check>,
) -> anyhow::Result<()> {
    let auth = [("cookie", cookie)];
    let response = http.get("/panel/api", &auth).await?;
    let body: Value = response.json().await?;
    checks.push(Check::new(
        "GET /panel/api index",
        body["service"] == "devin-2api" && body["endpoints"].is_array(),
        format!(
            "{} endpoints",
            body["endpoints"].as_array().map_or(0, Vec::len)
        ),
    ));
    let response = http.get("/panel/api/stats", &auth).await?;
    let body: Value = response.json().await?;
    checks.push(Check::new(
        "GET /panel/api/stats carries rust runtime marker",
        body["version"] == "qa-panel" && body["http"]["runtime"] == "rust",
        format!("runtime {}", body["http"]["runtime"]),
    ));
    let response = http.get("/panel/api/requests", &auth).await?;
    let body: Value = response.json().await?;
    checks.push(Check::new(
        "GET /panel/api/requests lists seeded request",
        body["requests"].as_array().is_some_and(|r| !r.is_empty()),
        format!("total {}", body["total"]),
    ));
    let status = http
        .get(&format!("/panel/api/requests/{dir}"), &auth)
        .await?
        .status();
    checks.push(Check::new(
        "GET /panel/api/requests/{dir} detail",
        status == 200,
        format!("{status}"),
    ));
    Ok(())
}

// ---------- browser driver ----------

/// Minimal driver over the `agent-browser` CLI (CDP-backed Chromium). Each
/// invocation is a short-lived process against a named session; the session
/// is closed on drop. Returns None when the CLI is unavailable.
struct Browser {
    session: String,
}

impl Browser {
    fn detect() -> Option<Self> {
        let session = format!("qa-panel-{}", std::process::id());
        let ok = std::process::Command::new("agent-browser")
            .args(["open", "about:blank", "--json", "--session"])
            .arg(&session)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        ok.then_some(Self { session })
    }

    fn run(&self, args: &[&str]) -> anyhow::Result<String> {
        let output = std::process::Command::new("agent-browser")
            .args(args)
            .arg("--session")
            .arg(&self.session)
            .stdin(Stdio::null())
            .output()
            .context("agent-browser spawn")?;
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        if !output.status.success() {
            anyhow::bail!(
                "agent-browser {} failed: {}",
                args.first().copied().unwrap_or(""),
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(400)
                    .collect::<String>()
            );
        }
        Ok(stdout)
    }

    fn open(&self, url: &str) -> anyhow::Result<()> {
        self.run(&["open", url, "--json"]).map(|_| ())
    }
    fn set_viewport(&self, w: u32, h: u32) -> anyhow::Result<()> {
        self.run(&["set", "viewport", &w.to_string(), &h.to_string()])
            .map(|_| ())
    }
    fn fill(&self, selector: &str, text: &str) -> anyhow::Result<()> {
        self.run(&["fill", selector, text]).map(|_| ())
    }
    fn click(&self, selector: &str) -> anyhow::Result<()> {
        self.run(&["click", selector]).map(|_| ())
    }
    fn screenshot(&self, path: &Path) -> anyhow::Result<()> {
        self.run(&["screenshot", &path.to_string_lossy()])
            .map(|_| ())
    }
    fn eval(&self, js: &str) -> anyhow::Result<String> {
        self.run(&["eval", js, "--json"])
    }
    /// Evaluate JS and return the `data.result` field (the envelope's
    /// `success` flag is always true, so raw substring checks lie).
    fn eval_result(&self, js: &str) -> anyhow::Result<Value> {
        let out = self.eval(js)?;
        let parsed: Value = serde_json::from_str(&out).unwrap_or(Value::Null);
        Ok(parsed["data"]["result"].clone())
    }
    /// Page JS errors since the last clear.
    fn errors(&self) -> anyhow::Result<Vec<String>> {
        let out = self.run(&["errors", "--json"])?;
        let parsed: Value = serde_json::from_str(&out).unwrap_or(Value::Null);
        let list = parsed["data"]["errors"]
            .as_array()
            .or_else(|| parsed["errors"].as_array())
            .or_else(|| parsed["data"].as_array())
            .cloned()
            .unwrap_or_default();
        Ok(list
            .iter()
            .map(|e| {
                e["message"]
                    .as_str()
                    .or_else(|| e.as_str())
                    .unwrap_or(&e.to_string())
                    .to_string()
            })
            .collect())
    }

    /// Poll a JS predicate until true or the bounded wait expires. This is
    /// an event wait, not a correctness sleep: the predicate is the gate.
    fn wait_eval(&self, js: &str, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.eval_result(js).ok() == Some(Value::Bool(true)) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timeout waiting for page condition: {js}");
            }
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    fn close(&self) {
        let _ = std::process::Command::new("agent-browser")
            .args(["close", "--session"])
            .arg(&self.session)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        self.close();
    }
}

/// Desktop + mobile passes over the real panel: login, every tab, console
/// errors, horizontal-overflow check, screenshots.
fn browser_pass(
    browser: &Browser,
    base: &str,
    evidence: &Path,
    checks: &mut Vec<Check>,
) -> anyhow::Result<()> {
    // `report/` is the playwright HTML-report dir used by the e2e spec;
    // keep this driver's extra shots in `screenshots/` to avoid clobbering.
    let report_dir = evidence.join("screenshots");
    std::fs::create_dir_all(&report_dir)?;

    // Desktop pass: real login flow, then each tab.
    browser.set_viewport(1440, 900)?;
    browser.open(&format!("{base}/panel"))?;
    browser.wait_eval("!!document.getElementById('loginForm')", BROWSER_WAIT)?;
    browser.screenshot(&report_dir.join("login-desktop.png"))?;
    browser.fill("#password", PASSWORD)?;
    browser.click("#loginForm button[type=submit]")?;
    browser.wait_eval(
        "!!document.getElementById('page-overview') && document.querySelectorAll('#page-overview .kpi').length > 0",
        BROWSER_WAIT,
    )?;
    browser.screenshot(&evidence.join("desktop.png"))?;
    let _ = browser.run(&["errors", "--clear"]);

    for tab in ["requests", "usage", "quota", "models", "system"] {
        browser.eval(&format!("location.hash='#{tab}'"))?;
        browser.wait_eval(
            &format!(
                "document.getElementById('page-{tab}') && document.getElementById('page-{tab}').classList.contains('on')"
            ),
            BROWSER_WAIT,
        )?;
        // Let the tab's first render pass complete before shooting.
        browser.wait_eval(
            &format!(
                "document.querySelectorAll('#page-{tab} .kpi, #page-{tab} table, #page-{tab} .err-banner, #page-{tab} .card').length > 0"
            ),
            BROWSER_WAIT,
        )?;
        browser.screenshot(&report_dir.join(format!("tab-{tab}-desktop.png")))?;
    }
    let errors = browser.errors()?;
    checks.push(Check::new(
        "desktop pass: zero JS errors",
        errors.is_empty(),
        if errors.is_empty() {
            "clean".into()
        } else {
            errors.join(" | ")
        },
    ));
    // The upstream health matrix (.mx) is the only component allowed to
    // overflow the viewport uncontained — that is the original Go design
    // (no overflow-x wrapper), not a Rust regression. Everything else must
    // stay inside the viewport or inside a scroll container.
    let overflow = browser.eval_result(OVERFLOW_PROBE).unwrap_or(Value::Null);
    checks.push(Check::new(
        "desktop pass: no unexpected horizontal overflow",
        overflow["uncontained_outside_mx"] == 0,
        overflow.to_string(),
    ));

    // Mobile pass: same session cookie persists across viewport change.
    browser.set_viewport(390, 844)?;
    browser.open(&format!("{base}/panel#overview"))?;
    browser.wait_eval(
        "document.querySelectorAll('#page-overview .kpi').length > 0",
        BROWSER_WAIT,
    )?;
    browser.screenshot(&evidence.join("mobile.png"))?;
    let _ = browser.run(&["errors", "--clear"]);
    for tab in ["requests", "usage"] {
        browser.eval(&format!("location.hash='#{tab}'"))?;
        browser.wait_eval(
            &format!(
                "document.getElementById('page-{tab}') && document.getElementById('page-{tab}').classList.contains('on')"
            ),
            BROWSER_WAIT,
        )?;
        browser.screenshot(&report_dir.join(format!("tab-{tab}-mobile.png")))?;
    }
    let errors = browser.errors()?;
    checks.push(Check::new(
        "mobile pass: zero JS errors",
        errors.is_empty(),
        if errors.is_empty() {
            "clean".into()
        } else {
            errors.join(" | ")
        },
    ));
    let overflow = browser.eval_result(OVERFLOW_PROBE).unwrap_or(Value::Null);
    checks.push(Check::new(
        "mobile pass: no unexpected horizontal overflow",
        overflow["uncontained_outside_mx"] == 0,
        overflow.to_string(),
    ));
    Ok(())
}

/// Failure case `missing-index-and-api-404`: missing assets and API
/// failures surface as the documented status codes, and the panel renders
/// the upstream-failure banner without JS exceptions, then recovers.
// One failure drill; splitting scatters the recovery narrative.
#[allow(clippy::too_many_lines)]
async fn failure_case(evidence: &Path, checks: &mut Vec<Check>) -> anyhow::Result<()> {
    let fixture = fixture(evidence)?;
    let (port, stop) = serve(&fixture.dashboard).await?;
    let http = Http::new(port)?;
    let cookie = http.login_cookie().await?;
    let auth = [("cookie", cookie.as_str())];

    // Missing assets / missing index entries / unknown API routes.
    for (path, expected, note) in [
        (
            "/panel/static/missing.js",
            404_u16,
            "missing embedded asset",
        ),
        ("/panel/static/js/missing.js", 404, "missing nested asset"),
        ("/panel/static/%2e%2e/Cargo.toml", 404, "encoded traversal"),
        (
            "/panel/api/requests/20990101-000000-aaaaaa",
            404,
            "missing index dir",
        ),
        (
            "/panel/api/requests/20990101-000000-aaaaaa/merged",
            404,
            "missing merged",
        ),
        (
            "/panel/api/requests/20990101-000000-aaaaaa/abort",
            404,
            "abort missing dir",
        ),
        ("/panel/api/nope", 404, "unknown api route"),
        ("/panel/api/requests/../config", 404, "api traversal"),
    ] {
        let method = if path.ends_with("/abort") {
            "POST"
        } else {
            "GET"
        };
        let response = match method {
            "POST" => {
                http.client
                    .post(format!("{}{path}", http.base))
                    .header("cookie", cookie.as_str())
                    .send()
                    .await?
            }
            _ => http.get(path, &auth).await?,
        };
        checks.push(Check::new(
            &format!("{method} {path} -> {expected}"),
            response.status().as_u16() == expected,
            format!("{note}; got {}", response.status()),
        ));
    }

    // API failure surfaces: upstream status failure stays a 200 with the
    // error field (Go contract); models failure is 502.
    fixture.data.fail_upstream.store(true, Ordering::Relaxed);
    let response = http.get("/panel/api/status", &auth).await?;
    let status = response.status();
    let body: Value = response.json().await?;
    checks.push(Check::new(
        "upstream failure -> 200 + user_status_error field",
        status == 200 && body["user_status_error"].is_string(),
        format!("{}", body["user_status_error"]),
    ));
    let status = http.get("/panel/api/models", &auth).await?.status();
    checks.push(Check::new(
        "models upstream failure -> 502",
        status == 502,
        format!("{status}"),
    ));

    // Browser: the failure renders as the panel's error banner, no JS
    // exceptions, and recovers when the upstream heals.
    if let Some(browser) = Browser::detect() {
        let base = format!("http://127.0.0.1:{port}");
        browser.set_viewport(1440, 900)?;
        browser.open(&format!("{base}/panel"))?;
        browser.fill("#password", PASSWORD)?;
        browser.click("#loginForm button[type=submit]")?;
        browser.wait_eval(
            "document.querySelectorAll('#page-overview .kpi').length > 0",
            BROWSER_WAIT,
        )?;
        let _ = browser.run(&["errors", "--clear"]);
        browser.open(&format!("{base}/panel#overview"))?;
        browser.wait_eval(
            "document.querySelectorAll('#page-overview .err-banner').length > 0",
            BROWSER_WAIT,
        )?;
        std::fs::create_dir_all(evidence.join("screenshots"))?;
        browser.screenshot(&evidence.join("screenshots").join("upstream-failure.png"))?;
        let errors = browser.errors()?;
        checks.push(Check::new(
            "upstream failure renders err-banner without JS errors",
            errors.is_empty(),
            if errors.is_empty() {
                "clean".into()
            } else {
                errors.join(" | ")
            },
        ));
        fixture.data.fail_upstream.store(false, Ordering::Relaxed);
        browser.open(&format!("{base}/panel#overview"))?;
        let recovered = browser
            .wait_eval(
                "document.querySelectorAll('#page-overview .kpi').length > 0",
                BROWSER_WAIT,
            )
            .is_ok();
        checks.push(Check::new(
            "panel recovers after upstream heals",
            recovered,
            String::new(),
        ));
        browser.close();
    } else {
        checks.push(Check::new(
            "browser failure-state check",
            false,
            "agent-browser CLI not found; banner check skipped",
        ));
    }

    // Expired session: rotate the password (Go hot-reload semantics revoke
    // all sessions); the old cookie must stop authenticating.
    fixture.dashboard.set_password("rotated".into());
    let status = http.get("/panel/api", &auth).await?.status();
    checks.push(Check::new(
        "expired session -> 401",
        status == 401,
        format!("{status}"),
    ));

    stop.cancel();
    let state = fixture.manager.root().to_path_buf();
    fixture.manager.close();
    let _ = std::fs::remove_dir_all(state);
    Ok(())
}

pub async fn run(evidence: &Path, go_root: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    match case {
        None | Some("happy") => {}
        Some("missing-index-and-api-404") => {
            let mut checks = Vec::new();
            failure_case(evidence, &mut checks).await?;
            return finish(evidence, "missing-index-and-api-404", &checks);
        }
        Some(other) => anyhow::bail!("unknown panel case {other}"),
    }

    let mut checks = Vec::new();
    let fixture = fixture(evidence)?;
    let (port, stop) = serve(&fixture.dashboard).await?;
    let http = Http::new(port)?;
    let cookie = http.login_cookie().await?;

    check_assets(&http, &cookie, go_root, &mut checks).await?;
    check_routes(&http, &cookie, &mut checks).await?;
    check_api(&http, &cookie, &fixture.request_dir, &mut checks).await?;

    if let Some(browser) = Browser::detect() {
        let base = format!("http://127.0.0.1:{port}");
        browser_pass(&browser, &base, evidence, &mut checks)?;
    } else {
        checks.push(Check::new(
            "browser screenshots",
            false,
            "agent-browser CLI not found on PATH",
        ));
    }

    stop.cancel();
    let state = fixture.manager.root().to_path_buf();
    fixture.manager.close();
    let _ = std::fs::remove_dir_all(state);
    finish(evidence, "happy", &checks)
}

fn finish(evidence: &Path, case: &str, checks: &[Check]) -> anyhow::Result<i32> {
    let failed: Vec<&Check> = checks.iter().filter(|c| !c.ok).collect();
    let report = json!({
        "case": case,
        "checks": checks.iter().map(|c| json!({"name":c.name,"ok":c.ok,"detail":c.detail})).collect::<Vec<_>>(),
        "passed": checks.len() - failed.len(),
        "failed": failed.len(),
    });
    let report_name = if case == "happy" {
        "qa.json".to_string()
    } else {
        format!("{case}.json")
    };
    std::fs::write(
        evidence.join(report_name),
        serde_json::to_string_pretty(&report)?,
    )?;
    for check in checks {
        println!("{} {}", if check.ok { "PASS" } else { "FAIL" }, check.name);
    }
    if failed.is_empty() {
        println!("qa panel: {} checks passed", checks.len());
        Ok(0)
    } else {
        for check in &failed {
            eprintln!("qa panel FAIL: {} — {}", check.name, check.detail);
        }
        Ok(1)
    }
}

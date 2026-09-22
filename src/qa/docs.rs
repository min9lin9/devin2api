//! Task-25 documented-smoke QA: executes the shipped quickstart and the
//! documented rollback-copy workflow end to end from a packaged release
//! artifact, plus the `invalid-config-and-missing-token` failure case.
//!
//! The flow mirrors README.md + docs/deployment.md literally: copy
//! `config.example.yaml`, apply the documented edits, run the packaged
//! binary against a local stub upstream, probe healthz/models/inference/
//! panel, SIGTERM for a clean shutdown, then exercise the documented
//! state-backup rollback. Synchronization uses child output events and
//! bounded HTTP probes; no sleeps.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Context as _;
use serde_json::{Value, json};

use crate::qa::process::{self, ManagedChild};

const READY_TIMEOUT: Duration = Duration::from_secs(90);
const HTTP_TIMEOUT: Duration = Duration::from_secs(20);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
/// Version stamped into the QA release build; the packaged asset must
/// report it back through `-version`.
const QA_VERSION: &str = "task25-qa";
/// Synthetic credentials — never valid upstream, used to prove the
/// documented surfaces neither require nor leak real secrets.
const SYNTHETIC_TOKEN: &str = "devin-session-token$qa-synthetic-not-a-real-token";
const API_KEY: &str = "qa-smoke-key";
const PANEL_PASSWORD: &str = "qa-panel-pass";

fn output(command: &mut Command) -> anyhow::Result<std::process::Output> {
    let shown = format!("{command:?}");
    let out = command
        .output()
        .with_context(|| format!("spawn failed: {shown}"))?;
    anyhow::ensure!(
        out.status.success(),
        "command failed: {shown}\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(out)
}

/// Build the release daemon with an explicit version stamp (same approach
/// as packaging QA: the env-stamped fingerprint only recompiles the top
/// crate against the shared target dir).
fn build_daemon(root: &Path) -> anyhow::Result<PathBuf> {
    let mut command = Command::new("cargo");
    command
        .args(["build", "--locked", "--release", "--bin", "devin-2api"])
        .current_dir(root)
        .env("CARGO_BUILD_JOBS", "2")
        .env("DEVIN2API_BUILD_VERSION", QA_VERSION)
        .env_remove("DEVIN2API_PACKAGE_VERSION");
    output(&mut command)?;
    let binary = root.join("target/release").join(if cfg!(windows) {
        "devin-2api.exe"
    } else {
        "devin-2api"
    });
    anyhow::ensure!(binary.is_file(), "daemon build produced no binary");
    Ok(binary)
}

/// Package the built binary through the shipped `scripts/package-release.sh`
/// and verify `checksums.txt` against the packaged bytes — the artifact the
/// quickstart then runs is the release asset, not the cargo output.
fn package(root: &Path, binary: &Path, evidence: &Path) -> anyhow::Result<PathBuf> {
    let dist = evidence.join("dist");
    if dist.exists() {
        std::fs::remove_dir_all(&dist)?;
    }
    std::fs::create_dir_all(&dist)?;
    output(
        Command::new("bash")
            .arg(root.join("scripts/package-release.sh").canonicalize()?)
            .args(["--target", "x86_64-unknown-linux-musl"])
            .arg("--binary")
            .arg(binary)
            .arg("--out")
            .arg(&dist)
            .args(["--version", QA_VERSION]),
    )?;
    let asset = dist.join(if cfg!(windows) {
        "devin-2api-windows-amd64.zip"
    } else {
        "devin-2api-linux-amd64"
    });
    anyhow::ensure!(asset.is_file(), "packaged asset missing");
    output(
        Command::new("sha256sum")
            .args(["-c", "checksums.txt"])
            .current_dir(&dist),
    )
    .context("sha256sum -c checksums.txt")?;
    let reported = output(Command::new(&asset).arg("-version"))?;
    let reported = String::from_utf8(reported.stdout)?.trim().to_string();
    anyhow::ensure!(
        reported == QA_VERSION,
        "packaged asset reports {reported}, want {QA_VERSION}"
    );
    Ok(asset)
}

/// Spawn the QA upstream stub child (`__http-upstream`) and return it with
/// its bound address. Readiness is the child's `READY <addr>` line.
fn spawn_stub(mode: &str, log_dir: &Path) -> anyhow::Result<(Child, String)> {
    let exe = std::env::current_exe()?;
    let exe = exe
        .to_string_lossy()
        .strip_suffix(" (deleted)")
        .map_or(exe.clone(), PathBuf::from);
    let stderr = std::fs::File::create(log_dir.join("upstream.stderr.log"))?;
    let mut child = Command::new(&exe)
        .args(["__http-upstream", mode])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .spawn()
        .with_context(|| format!("spawn upstream stub {}", exe.display()))?;
    let stdout = child.stdout.take().context("stub stdout")?;
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut sent = false;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if !sent && line.starts_with("READY ") {
                let _ = tx.send(line);
                sent = true;
            }
            // Keep draining so later child output never blocks on a full pipe.
        }
    });
    let ready = rx
        .recv_timeout(READY_TIMEOUT)
        .context("timed out waiting for stub READY")?;
    let addr = ready
        .strip_prefix("READY ")
        .context("malformed stub READY line")?
        .trim()
        .to_string();
    Ok((child, addr))
}

struct StubGuard(Child);
impl Drop for StubGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The documented quickstart config step: copy `config.example.yaml` and
/// apply exactly the edits the README describes (listen port, upstream
/// `base_url`, token, model — plus the auth keys the smoke asserts). Every
/// replacement must anchor on the shipped example text.
fn write_config(
    root: &Path,
    work: &Path,
    port: u16,
    upstream: &str,
    token: &str,
) -> anyhow::Result<PathBuf> {
    let example = std::fs::read_to_string(root.join("config.example.yaml"))?;
    let edits: [(&str, String); 6] = [
        (
            "  listen: \":8080\"",
            format!("  listen: \"127.0.0.1:{port}\""),
        ),
        (
            "  base_url: \"https://server.codeium.com\"",
            format!("  base_url: \"http://{upstream}\""),
        ),
        ("  token: \"\"", format!("  token: \"{token}\"")),
        (
            "  model: \"glm-5-2\"",
            "  model: \"stub-model\"".to_string(),
        ),
        (
            "  password: \"\"",
            format!("  password: \"{PANEL_PASSWORD}\""),
        ),
        ("  api_key: \"\"", format!("  api_key: \"{API_KEY}\"")),
    ];
    let mut config = example;
    for (from, to) in edits {
        anyhow::ensure!(
            config.matches(from).count() == 1,
            "config.example.yaml anchor {from:?} not found exactly once — docs drifted"
        );
        config = config.replacen(from, &to, 1);
    }
    let path = work.join("config.yaml");
    std::fs::write(&path, config)?;
    Ok(path)
}

/// Spawn the packaged daemon the documented way: `-config` + `-state-dir`,
/// stdout/stderr captured under the work dir.
fn spawn_daemon(
    asset: &Path,
    config: &Path,
    state: &Path,
    log_dir: &Path,
    name: &str,
    empty_home: Option<&Path>,
) -> anyhow::Result<ManagedChild> {
    let mut command = Command::new(asset);
    command
        .arg("-config")
        .arg(config)
        .arg("-state-dir")
        .arg(state)
        // Deterministic credential/path resolution: no ambient token, no
        // ambient config/state overrides, no reuseport handoff.
        .env_remove("DEVIN_TOKEN")
        .env_remove("WINDSURF_API_KEY")
        .env_remove("DEVIN2API_CONFIG")
        .env_remove("DEVIN2API_STATE_DIR")
        .env_remove("DEVIN2API_REUSEPORT");
    if let Some(home) = empty_home {
        command
            .env("HOME", home)
            .env("XDG_DATA_HOME", home.join("xdg-data"))
            .env("XDG_CONFIG_HOME", home.join("xdg-config"))
            .env("XDG_STATE_HOME", home.join("xdg-state"))
            .env("APPDATA", home.join("appdata"))
            .env("LOCALAPPDATA", home.join("local-appdata"));
    }
    process::spawn_logged(log_dir, name, &mut command).context("spawn daemon")
}

async fn get(client: &reqwest::Client, url: &str, key: Option<&str>) -> anyhow::Result<Value> {
    let mut request = client.get(url).timeout(HTTP_TIMEOUT);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    let response = request.send().await?;
    Ok(json!({
        "status": response.status().as_u16(),
        "body": response.text().await?,
    }))
}

async fn post(
    client: &reqwest::Client,
    url: &str,
    key: Option<&str>,
    body: &str,
) -> anyhow::Result<Value> {
    let mut request = client
        .post(url)
        .timeout(HTTP_TIMEOUT)
        .header("content-type", "application/json")
        .body(body.to_string());
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    let response = request.send().await?;
    Ok(json!({
        "status": response.status().as_u16(),
        "body": response.text().await?,
    }))
}

fn copy_dir(src: &Path, dst: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

fn index_lines(state: &Path) -> usize {
    std::fs::read_to_string(state.join("logs/index.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

/// Assert none of the synthetic secrets appear in a captured text surface.
fn assert_no_leak(surface: &str, what: &str) -> anyhow::Result<()> {
    for secret in [SYNTHETIC_TOKEN, API_KEY, PANEL_PASSWORD] {
        anyhow::ensure!(
            !surface.contains(secret),
            "{what} leaked secret material ({secret})"
        );
    }
    Ok(())
}

/// The documented quickstart + rollback-copy workflow, executed against the
/// packaged release asset and a local stub upstream.
// One sequential quickstart workflow; splitting scatters the doc steps.
#[allow(clippy::too_many_lines)]
async fn happy(evidence: &Path) -> anyhow::Result<Value> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let work = evidence.join("quickstart");
    if work.exists() {
        std::fs::remove_dir_all(&work)?;
    }
    std::fs::create_dir_all(&work)?;

    eprintln!("qa documented-smoke: build + package");
    let binary = build_daemon(root)?;
    let asset = package(root, &binary, evidence)?;

    eprintln!("qa documented-smoke: stub upstream");
    let (stub, upstream_addr) = spawn_stub("normal", &work)?;
    let _stub = StubGuard(stub);

    eprintln!("qa documented-smoke: configure + run");
    let port = process::free_port()?;
    let config = write_config(root, &work, port, &upstream_addr, SYNTHETIC_TOKEN)?;
    let state = work.join("state");
    std::fs::create_dir_all(&state)?;
    let mut daemon = spawn_daemon(&asset, &config, &state, &work, "daemon", None)?;
    let base = format!("http://127.0.0.1:{port}");
    process::wait_ready(&mut daemon, &format!("{base}/healthz"), READY_TIMEOUT).await?;

    let client = reqwest::Client::builder().build()?;
    let mut checks = serde_json::Map::new();

    checks.insert(
        "healthz".into(),
        get(&client, &format!("{base}/healthz"), None).await?,
    );
    checks.insert(
        "models_unauthorized".into(),
        get(&client, &format!("{base}/v1/models"), None).await?,
    );
    checks.insert(
        "models".into(),
        get(&client, &format!("{base}/v1/models"), Some(API_KEY)).await?,
    );
    checks.insert(
        "chat_sse".into(),
        post(
            &client,
            &format!("{base}/v1/chat/completions"),
            Some(API_KEY),
            r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"ping"}]}"#,
        )
        .await?,
    );
    checks.insert(
        "responses_json".into(),
        post(
            &client,
            &format!("{base}/v1/responses"),
            Some(API_KEY),
            r#"{"model":"stub-model","input":"ping"}"#,
        )
        .await?,
    );
    checks.insert(
        "panel_page".into(),
        get(&client, &format!("{base}/panel"), None).await?,
    );
    checks.insert(
        "panel_api_unauthorized".into(),
        get(&client, &format!("{base}/panel/api/status"), None).await?,
    );
    // Documented agent access path: Bearer <dashboard.password>.
    checks.insert(
        "panel_api".into(),
        get(
            &client,
            &format!("{base}/panel/api/status"),
            Some(PANEL_PASSWORD),
        )
        .await?,
    );
    // Documented browser path: form login sets the session cookie.
    let login = client
        .post(format!("{base}/panel/login"))
        .timeout(HTTP_TIMEOUT)
        .header("content-type", "application/x-www-form-urlencoded")
        .body(format!("password={PANEL_PASSWORD}"))
        .send()
        .await?;
    let login_cookie = login
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .to_string();
    checks.insert(
        "panel_login".into(),
        json!({"status": login.status().as_u16(), "cookie": !login_cookie.is_empty()}),
    );
    checks.insert(
        "panel_config_redacted".into(),
        get(
            &client,
            &format!("{base}/panel/api/config"),
            Some(PANEL_PASSWORD),
        )
        .await?,
    );

    let mut failures = Vec::new();
    let expect = |name: &str, ok: bool, failures: &mut Vec<String>| {
        if !ok {
            failures.push(format!("{name}: {:?}", checks.get(name)));
        }
    };
    expect(
        "healthz",
        checks["healthz"]["status"] == 200
            && checks["healthz"]["body"]
                .as_str()
                .is_some_and(|b| b.contains("\"ok\"")),
        &mut failures,
    );
    expect(
        "models_unauthorized",
        checks["models_unauthorized"]["status"] == 401,
        &mut failures,
    );
    expect("models", checks["models"]["status"] == 200, &mut failures);
    expect(
        "chat_sse",
        checks["chat_sse"]["status"] == 200
            && checks["chat_sse"]["body"]
                .as_str()
                .is_some_and(|b| b.contains("pong")),
        &mut failures,
    );
    expect(
        "responses_json",
        checks["responses_json"]["status"] == 200,
        &mut failures,
    );
    expect(
        "panel_page",
        checks["panel_page"]["status"] == 200
            && checks["panel_page"]["body"]
                .as_str()
                .is_some_and(|b| b.contains("<html") || b.contains("<!DOCTYPE")),
        &mut failures,
    );
    expect(
        "panel_api_unauthorized",
        checks["panel_api_unauthorized"]["status"] == 401,
        &mut failures,
    );
    expect(
        "panel_api",
        checks["panel_api"]["status"] == 200,
        &mut failures,
    );
    expect(
        "panel_login",
        checks["panel_login"]["status"] == 200 && checks["panel_login"]["cookie"] == true,
        &mut failures,
    );
    let config_view = checks["panel_config_redacted"]["body"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    expect(
        "panel_config_redacted",
        checks["panel_config_redacted"]["status"] == 200,
        &mut failures,
    );
    if let Err(e) = assert_no_leak(&config_view, "panel config view") {
        failures.push(e.to_string());
    }

    // Documented shutdown: SIGTERM drains and exits cleanly.
    eprintln!("qa documented-smoke: SIGTERM shutdown");
    let exit = daemon.shutdown(SHUTDOWN_GRACE).await?;
    if !exit.starts_with("exit status: 0") && !exit.contains("code: Some(0)") {
        failures.push(format!("daemon did not exit cleanly: {exit}"));
    }

    // Documented rollback workflow: keep a separate backed-up state copy,
    // run the "new" install, then restore the copy and verify the daemon
    // serves the pre-upgrade history.
    eprintln!("qa documented-smoke: rollback-copy workflow");
    let backup = work.join("state-backup");
    copy_dir(&state, &backup)?;
    let pre_rows = index_lines(&state);
    anyhow::ensure!(pre_rows > 0, "no request history recorded before backup");

    let mut daemon2 = spawn_daemon(&asset, &config, &state, &work, "daemon-post-upgrade", None)?;
    process::wait_ready(&mut daemon2, &format!("{base}/healthz"), READY_TIMEOUT).await?;
    let post_upgrade = post(
        &client,
        &format!("{base}/v1/chat/completions"),
        Some(API_KEY),
        r#"{"model":"stub-model","stream":false,"messages":[{"role":"user","content":"ping"}]}"#,
    )
    .await?;
    if post_upgrade["status"] != 200 {
        failures.push(format!("post-upgrade inference failed: {post_upgrade}"));
    }
    let exit2 = daemon2.shutdown(SHUTDOWN_GRACE).await?;
    if !exit2.starts_with("exit status: 0") && !exit2.contains("code: Some(0)") {
        failures.push(format!("post-upgrade daemon did not exit cleanly: {exit2}"));
    }
    let grown = index_lines(&state);
    if grown <= pre_rows {
        failures.push(format!(
            "state did not grow after upgrade run ({pre_rows} -> {grown})"
        ));
    }

    // Roll back: the documented procedure restores the separate backup copy
    // over the state dir, then starts the previous binary.
    std::fs::remove_dir_all(&state)?;
    copy_dir(&backup, &state)?;
    let mut daemon3 = spawn_daemon(&asset, &config, &state, &work, "daemon-rollback", None)?;
    process::wait_ready(&mut daemon3, &format!("{base}/healthz"), READY_TIMEOUT).await?;
    let requests = get(
        &client,
        &format!("{base}/panel/api/requests"),
        Some(PANEL_PASSWORD),
    )
    .await?;
    let restored_rows = index_lines(&state);
    if restored_rows != pre_rows {
        failures.push(format!(
            "rolled-back index has {restored_rows} rows, want {pre_rows}"
        ));
    }
    if requests["status"] != 200 {
        failures.push(format!("panel requests after rollback: {requests}"));
    }
    let exit3 = daemon3.shutdown(SHUTDOWN_GRACE).await?;
    if !exit3.starts_with("exit status: 0") && !exit3.contains("code: Some(0)") {
        failures.push(format!("rollback daemon did not exit cleanly: {exit3}"));
    }

    // Secret hygiene on every captured daemon surface.
    for log in [
        "daemon.stderr.log",
        "daemon-post-upgrade.stderr.log",
        "daemon-rollback.stderr.log",
    ] {
        let text = std::fs::read_to_string(work.join(log)).unwrap_or_default();
        if let Err(e) = assert_no_leak(&text, log) {
            failures.push(e.to_string());
        }
    }

    Ok(json!({
        "asset": asset,
        "checks": checks,
        "rollback": {
            "pre_backup_index_rows": pre_rows,
            "post_upgrade_index_rows": grown,
            "restored_index_rows": restored_rows,
            "panel_requests_after_rollback": requests["status"],
        },
        "exits": [exit, exit2, exit3],
        "failures": failures,
        "passed": failures.is_empty(),
    }))
}

/// `--case invalid-config-and-missing-token`: the documented diagnostics for
/// a malformed config and for a missing upstream token, with a leak check on
/// every emitted surface.
async fn failure_case(evidence: &Path) -> anyhow::Result<Value> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let work = evidence.join("failure-work");
    if work.exists() {
        std::fs::remove_dir_all(&work)?;
    }
    std::fs::create_dir_all(&work)?;
    let binary = build_daemon(root)?;
    let asset = package(root, &binary, evidence)?;
    let mut failures = Vec::new();

    // 1. Invalid config: unknown key + a secret in the file. The daemon must
    //    refuse to start, name the offending key, and never echo the secret.
    let bad_config = work.join("bad-config.yaml");
    let mut text = std::fs::read_to_string(root.join("config.example.yaml"))?;
    text = text.replacen(
        "  token: \"\"",
        &format!("  token: \"{SYNTHETIC_TOKEN}\""),
        1,
    );
    text.push_str("\nbogus_top_level_key: 1\n");
    std::fs::write(&bad_config, text)?;
    let out = Command::new(&asset)
        .arg("-config")
        .arg(&bad_config)
        .arg("-state-dir")
        .arg(work.join("state-bad"))
        .env_remove("DEVIN_TOKEN")
        .env_remove("WINDSURF_API_KEY")
        .output()
        .context("run daemon with invalid config")?;
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    std::fs::write(work.join("invalid-config.stderr.log"), &stderr)?;
    let invalid_ok = !out.status.success()
        && stderr.contains("bogus_top_level_key")
        && !stderr.contains(SYNTHETIC_TOKEN);
    if !invalid_ok {
        failures.push(format!(
            "invalid-config diagnostics wrong (exit {:?}): {stderr}",
            out.status.code()
        ));
    }

    // 2. Missing token: empty devin.token, no env, no credentials.toml
    //    (empty HOME). The documented behavior is: daemon starts, /v1/*
    //    answers a normalized upstream-auth failure, stderr carries the
    //    actionable token-source warning, nothing leaks.
    let (stub, upstream_addr) = spawn_stub("unauthenticated", &work)?;
    let _stub = StubGuard(stub);
    let port = process::free_port()?;
    let empty_home = work.join("empty-home");
    std::fs::create_dir_all(&empty_home)?;
    let config = write_config(root, &work, port, &upstream_addr, "")?;
    let state = work.join("state-no-token");
    std::fs::create_dir_all(&state)?;
    let mut daemon = spawn_daemon(
        &asset,
        &config,
        &state,
        &work,
        "daemon-no-token",
        Some(&empty_home),
    )?;
    let base = format!("http://127.0.0.1:{port}");
    process::wait_ready(&mut daemon, &format!("{base}/healthz"), READY_TIMEOUT).await?;
    let client = reqwest::Client::builder().build()?;
    let models = get(&client, &format!("{base}/v1/models"), Some(API_KEY)).await?;
    let body = models["body"].as_str().unwrap_or_default();
    // Catalog fetch hits the stub's 401 -> unauthenticated; the normalized
    // upstream-auth failure is a 502 (upstream-responsible) whose body
    // carries the actionable `unauthenticated` classification — the
    // documented diagnostic surface for a missing/expired token.
    if models["status"] != 502
        || !body.contains("unauthenticated")
        || !body.contains("authentication_error")
    {
        failures.push(format!("missing-token /v1/models: {models}"));
    }
    let exit = daemon.shutdown(SHUTDOWN_GRACE).await?;
    if !exit.starts_with("exit status: 0") && !exit.contains("code: Some(0)") {
        failures.push(format!("no-token daemon did not exit cleanly: {exit}"));
    }
    let daemon_stderr =
        std::fs::read_to_string(work.join("daemon-no-token.stderr.log")).unwrap_or_default();
    if let Err(e) = assert_no_leak(&daemon_stderr, "no-token stderr") {
        failures.push(e.to_string());
    }
    if let Err(e) = assert_no_leak(body, "no-token /v1/models body") {
        failures.push(e.to_string());
    }

    Ok(json!({
        "invalid_config": {
            "exit_code": out.status.code(),
            "names_offending_key": stderr.contains("bogus_top_level_key"),
            "stderr_log": work.join("invalid-config.stderr.log"),
        },
        "missing_token": {
            "models": models,
            "body_has_diagnostic": body.contains("unauthenticated"),
            "exit": exit,
        },
        "failures": failures,
        "passed": failures.is_empty(),
    }))
}

/// Run documented-smoke QA, optionally selecting the failure case.
pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let report = match case {
        Some("invalid-config-and-missing-token") => failure_case(evidence).await?,
        Some(other) => anyhow::bail!("unknown documented-smoke case {other}"),
        None => happy(evidence).await?,
    };
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    if report["passed"].as_bool().unwrap_or(false) {
        println!("qa documented-smoke: all documented workflows passed");
        Ok(0)
    } else {
        for failure in report["failures"].as_array().into_iter().flatten() {
            eprintln!("qa documented-smoke FAIL: {failure}");
        }
        Ok(1)
    }
}

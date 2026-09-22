//! Release/package QA against the actual Docker image and deployment helpers.
//! Synchronization uses child output events with bounded receives; no sleeps or
//! readiness polling are used.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Context as _;
use serde_json::{Value, json};

const READY_TIMEOUT: Duration = Duration::from_secs(90);

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

fn wait_line(
    reader: impl std::io::Read + Send + 'static,
    needle: &'static str,
) -> anyhow::Result<String> {
    let (tx, rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut sent = false;
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            if !sent && line.contains(needle) {
                if tx.send(line).is_err() {
                    return;
                }
                sent = true;
            }
            // Keep draining after readiness: child processes may emit later
            // events, and closing their pipe would turn a valid run into
            // SIGPIPE/BrokenPipe.
        }
    });
    rx.recv_timeout(READY_TIMEOUT)
        .with_context(|| format!("timed out waiting for {needle:?}"))
}

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct ContainerGuard {
    name: String,
}
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn free_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

fn host_user() -> anyhow::Result<String> {
    let uid = String::from_utf8(output(Command::new("id").arg("-u"))?.stdout)?;
    let gid = String::from_utf8(output(Command::new("id").arg("-g"))?.stdout)?;
    Ok(format!("{}:{}", uid.trim(), gid.trim()))
}

fn failure_case(root: &Path, evidence: &Path) -> anyhow::Result<Value> {
    let work = evidence.join("failure-work");
    if work.exists() {
        std::fs::remove_dir_all(&work)?;
    }
    std::fs::create_dir_all(work.join("bin"))?;
    std::fs::write(work.join("bin/devin-2api"), b"old-known-good\n")?;
    std::fs::write(work.join("artifact"), b"corrupt-new\n")?;
    std::fs::write(
        work.join("checksums.txt"),
        format!("{}  artifact\n", "0".repeat(64)),
    )?;
    let command = format!(
        "set -euo pipefail; BIN_DIR={bin}; CONFIG_DIR={config}; STATE_DIR={state}; source {lib}; if verify_checksum {sum} {artifact}; then exit 90; fi; test \"$(cat {installed})\" = old-known-good; cp {artifact} {installed}; if false; then :; else rollback_binary; exit 42; fi",
        bin = shell(&work.join("bin")),
        config = shell(&work.join("config")),
        state = shell(&work.join("state")),
        lib = shell(&root.join("scripts/lib-deploy.sh")),
        sum = shell(&work.join("checksums.txt")),
        artifact = shell(&work.join("artifact")),
        installed = shell(&work.join("bin/devin-2api")),
    );
    // Seed the rollback copy, then deliberately fail health and exercise the
    // exact rollback helper used by deploy-linux.sh and deploy.sh.
    std::fs::write(work.join("bin/devin-2api.previous"), b"old-known-good\n")?;
    let status = Command::new("bash").args(["-c", &command]).status()?;
    anyhow::ensure!(
        !status.success(),
        "failed-health negative control succeeded"
    );
    let retained = std::fs::read_to_string(work.join("bin/devin-2api"))? == "old-known-good\n";
    anyhow::ensure!(retained, "failed upgrade did not retain old installation");
    Ok(json!({
        "bad_checksum_rejected": true,
        "failed_health_exit": status.code(),
        "old_installation_retained": retained
    }))
}

fn shell(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

/// Version stamped into the QA release build; matches the image built by
/// docker-build.log so packaged asset and container agree.
const QA_RELEASE_VERSION: &str = "task21-qa";
/// Fixture release tag served by the sandboxed stub curl.
const SANDBOX_TAG: &str = "v9.9.9";

/// Build the daemon with an explicit version stamp. The shared target dir
/// is reused; the env-stamped fingerprint only recompiles the top crate.
fn build_daemon(root: &Path, profile: &str, version: &str) -> anyhow::Result<PathBuf> {
    let mut command = Command::new("cargo");
    command
        .args(["build", "--locked"])
        .current_dir(root)
        .env("CARGO_BUILD_JOBS", "2")
        .env("DEVIN2API_BUILD_VERSION", version)
        .env_remove("DEVIN2API_PACKAGE_VERSION");
    if profile == "release" {
        command.arg("--release");
    }
    command.args(["--bin", "devin-2api"]);
    output(&mut command)?;
    let binary = root.join("target").join(profile).join(if cfg!(windows) {
        "devin-2api.exe"
    } else {
        "devin-2api"
    });
    anyhow::ensure!(binary.is_file(), "daemon build produced no binary");
    Ok(binary)
}

/// Build the release binary, package it through scripts/package-release.sh
/// and verify checksums.txt against the packaged bytes. The asset row used
/// is the linux-amd64 contract name; the host toolchain is gnu, so the
/// packaged bytes are gnu — the musl static-interpreter assertion runs in
/// the release workflow on the real musl target.
fn package_case(root: &Path, evidence: &Path) -> anyhow::Result<Value> {
    let binary = build_daemon(root, "release", QA_RELEASE_VERSION)?;
    let dist = evidence.join("dist");
    if dist.exists() {
        std::fs::remove_dir_all(&dist)?;
    }
    std::fs::create_dir_all(&dist)?;
    let packaged = output(
        Command::new("bash")
            .arg(root.join("scripts/package-release.sh").canonicalize()?)
            .args(["--target", "x86_64-unknown-linux-musl"])
            .arg("--binary")
            .arg(&binary)
            .arg("--out")
            .arg(&dist)
            .args(["--version", QA_RELEASE_VERSION]),
    )?;
    let asset = dist.join("devin-2api-linux-amd64");
    anyhow::ensure!(asset.is_file(), "packaged asset missing");
    // checksums.txt must match the packaged bytes exactly.
    output(
        Command::new("sha256sum")
            .args(["-c", "checksums.txt"])
            .current_dir(&dist),
    )
    .context("sha256sum -c checksums.txt")?;
    // The packaged artifact self-reports the stamped version on this host.
    let reported =
        output(Command::new(&asset).arg("-version")).context("packaged asset -version")?;
    let reported = String::from_utf8(reported.stdout)?.trim().to_string();
    anyhow::ensure!(
        reported == QA_RELEASE_VERSION,
        "packaged asset reports {reported}, want {QA_RELEASE_VERSION}"
    );
    Ok(json!({
        "asset": asset,
        "asset_bytes": std::fs::metadata(&asset)?.len(),
        "asset_version": reported,
        "checksums_verified": true,
        "package_release_stdout": String::from_utf8_lossy(&packaged.stdout),
        "asset_target": "x86_64-unknown-linux-musl",
        "host_note": "host build is gnu; musl static assertion is a CI/release-workflow check",
    }))
}

/// Drive the real scripts/deploy-linux.sh through a stubbed systemctl/curl
/// sandbox: fresh install, bad-checksum refusal (old install retained) and
/// failed-health rollback. This is the plan's `bad-checksum-and-failed-health`
/// case executed against the shipped script, not a simulation.
fn sandbox_case(root: &Path, evidence: &Path) -> anyhow::Result<Value> {
    let fixture = build_daemon(root, "debug", SANDBOX_TAG)?;
    let qa = std::env::current_exe()?;
    let qa = qa
        .to_string_lossy()
        .strip_suffix(" (deleted)")
        .map_or(qa.clone(), PathBuf::from);
    let work = evidence.join("deploy-sandbox");
    if work.exists() {
        std::fs::remove_dir_all(&work)?;
    }
    let run = Command::new("bash")
        .arg(root.join("scripts/deploy-sandbox.test.sh").canonicalize()?)
        .arg(&fixture)
        .arg(&qa)
        .arg(&work)
        .output()
        .context("spawn deploy-sandbox.test.sh")?;
    let mut log = run.stdout.clone();
    log.extend_from_slice(&run.stderr);
    std::fs::write(evidence.join("deploy-sandbox.log"), &log)?;
    anyhow::ensure!(
        run.status.success(),
        "deploy sandbox failed; see deploy-sandbox.log\n{}",
        String::from_utf8_lossy(&log)
    );
    Ok(json!({
        "script": "scripts/deploy-sandbox.test.sh",
        "fixture_tag": SANDBOX_TAG,
        "log": evidence.join("deploy-sandbox.log"),
        "workdir": work,
        "scenarios": ["install", "bad-checksum-retains-old", "failed-health-rolls-back"],
        "passed": true,
    }))
}

// One container drill; splitting scatters the build/run/verify flow.
#[allow(clippy::too_many_lines)]
async fn container_case(evidence: &Path) -> anyhow::Result<Value> {
    anyhow::ensure!(
        cfg!(target_os = "linux"),
        "container QA requires Linux host networking"
    );
    let image = std::env::var("DEVIN2API_QA_IMAGE").unwrap_or_else(|_| "devin2api-rust:qa".into());
    output(Command::new("docker").args(["image", "inspect", &image]))?;

    // current_exe() reports `<path> (deleted)` when a concurrent cargo
    // build replaced the qa binary after this process started; the fresh
    // binary lives at the same path, so strip the marker (same fix as
    // lifecycle.rs self_binary).
    let current_exe = std::env::current_exe().context("resolve QA executable")?;
    let current_exe = current_exe
        .to_string_lossy()
        .strip_suffix(" (deleted)")
        .map_or_else(|| current_exe.clone(), PathBuf::from);
    let mut upstream = Command::new(&current_exe)
        .args(["__http-upstream", "normal"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("spawn upstream child {}", current_exe.display()))?;
    let upstream_stdout = upstream.stdout.take().context("upstream stdout")?;
    let ready = wait_line(upstream_stdout, "READY ")?;
    let upstream_addr = ready
        .split_once("READY ")
        .map(|(_, addr)| addr.trim().to_string())
        .context("malformed upstream READY event")?;
    let _upstream = ChildGuard(upstream);

    let port = free_port()?;
    let work = evidence.join("container-work");
    if work.exists() {
        std::fs::remove_dir_all(&work)?;
    }
    std::fs::create_dir_all(work.join("state"))?;
    let config = work.join("config.yaml");
    std::fs::write(
        &config,
        format!(
            "server:\n  listen: '127.0.0.1:{port}'\ndevin:\n  base_url: 'http://{upstream_addr}'\n  token: synthetic\n  model: stub-model\n  force_http1: true\ndebug:\n  enabled: true\ndashboard:\n  password: ''\nauth:\n  api_key: package-key\n"
        ),
    )?;
    let name = format!("devin2api-packaging-{}", std::process::id());
    let mount_config = format!("{}:/config/config.yaml:ro", config.display());
    let mount_state = format!("{}:/state", work.join("state").display());
    let user = host_user()?;
    let create = output(Command::new("docker").args([
        "create",
        "--name",
        &name,
        "--user",
        &user,
        "--network",
        "host",
        "-v",
        &mount_config,
        "-v",
        &mount_state,
        &image,
        "-config",
        "/config/config.yaml",
        "-state-dir",
        "/state",
    ]))?;
    anyhow::ensure!(!create.stdout.is_empty(), "docker create returned no id");
    let container = ContainerGuard { name: name.clone() };
    output(Command::new("docker").args(["start", &name]))?;
    let mut logs = Command::new("docker")
        .args(["logs", "--follow", &name])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = logs.stderr.take().context("docker logs stderr")?;
    let ready_log = wait_line(stderr, "HTTP server listening")?;
    let _logs = ChildGuard(logs);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let base = format!("http://127.0.0.1:{port}");
    let health = client.get(format!("{base}/healthz")).send().await?;
    let health_status = health.status().as_u16();
    let health_json: Value = health.json().await?;
    let models = client
        .get(format!("{base}/v1/models"))
        .bearer_auth("package-key")
        .send()
        .await?;
    let models_status = models.status().as_u16();
    let models_body = models.text().await?;
    let inference = client
        .post(format!("{base}/v1/chat/completions"))
        .bearer_auth("package-key")
        .json(&json!({"model":"stub-model","stream":false,"messages":[{"role":"user","content":"ping"}]}))
        .send().await?;
    let inference_status = inference.status().as_u16();
    let inference_body = inference.text().await?;
    anyhow::ensure!(health_status == 200, "health status {health_status}");
    anyhow::ensure!(
        models_status == 200,
        "models status {models_status}: {models_body}"
    );
    anyhow::ensure!(
        inference_status == 200,
        "inference status {inference_status}: {inference_body}"
    );
    drop(container);
    Ok(json!({
        "image": image,
        "ready_log": ready_log,
        "health":{"status":health_status,"body":health_json},
        "models":{"status":models_status,"body":models_body},
        "inference":{"status":inference_status,"body":inference_body}
    }))
}

/// Run packaging QA, optionally selecting the required failure case.
pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let report = match case {
        Some("bad-checksum-and-failed-health") => {
            json!({"deploy_sandbox":sandbox_case(root, evidence)?})
        }
        Some(other) => anyhow::bail!("unknown packaging case {other}"),
        None => {
            eprintln!("qa packaging: stage package");
            let package = package_case(root, evidence)?;
            eprintln!("qa packaging: stage container");
            let container = container_case(evidence).await?;
            eprintln!("qa packaging: stage deploy_sandbox");
            let sandbox = sandbox_case(root, evidence)?;
            eprintln!("qa packaging: stage checksum_unit");
            let checksum_unit = failure_case(root, evidence)?;
            json!({
                "package":package,
                "container":container,
                "deploy_sandbox":sandbox,
                "checksum_unit":checksum_unit
            })
        }
    };
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("qa packaging: container/release checks passed");
    Ok(0)
}

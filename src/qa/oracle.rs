//! Go contract oracle: builds the unmodified G tree into a QA-owned work
//! directory (never inside G), launches the Go daemon and upstreamstub on
//! isolated ports and state, captures baseline transcripts and exits
//! cleanly.
//!
//! Isolation contract:
//! - `go build -o` writes only under `work_dir`; `git status` is verified
//!   clean before and after every run.
//! - The daemon gets a scrubbed environment (no `DEVIN_TOKEN` /
//!   `WINDSURF_API_KEY` / proxy vars / `DEVIN2API_*`), a synthetic token and
//!   a loopback-only upstream stub — no live upstream traffic is possible.
//! - Every child is `SIGTERM`ed then `SIGKILL`ed and reaped; ports are
//!   allocated per run via bind(0).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::compare::{CanonRules, CapturedResponse, CaseResult};
use super::process;

/// The Go binaries the oracle knows how to build.
pub const GO_BINARIES: &[&str] = &[
    "devin-2api",
    "upstreamstub",
    "probe",
    "protocensus",
    "protoextract",
    "loadtest",
];

/// Summary of a `go test` run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoTestSummary {
    pub command: String,
    pub exit_code: i32,
    /// Packages reported `ok`.
    pub ok: u32,
    /// Packages reported `FAIL`.
    pub failed: u32,
    /// Packages with no test files.
    pub no_tests: u32,
    /// Total result lines seen (`ok` + `failed` + `no_tests`).
    pub total: u32,
    /// Names of failed packages (pre-existing failures are recorded, never
    /// hidden — the baseline contract is to report actual Go status).
    pub failed_packages: Vec<String>,
    /// Path of the full captured log, relative to the work dir.
    pub log: String,
}

/// One baseline request to issue against the running daemon.
#[derive(Debug, Clone)]
pub struct BaselineRequest {
    pub name: String,
    pub method: String,
    pub path: String,
    /// Body for POST requests.
    pub body: Option<String>,
    /// Extra headers (name, value).
    pub headers: Vec<(String, String)>,
    /// Whether to attach `Authorization: Bearer <api_key>` when the daemon
    /// was configured with an api key.
    pub auth: bool,
}

impl BaselineRequest {
    pub fn get(name: &str, path: &str) -> Self {
        Self {
            name: name.into(),
            method: "GET".into(),
            path: path.into(),
            body: None,
            headers: vec![],
            auth: true,
        }
    }

    pub fn post_json(name: &str, path: &str, body: &str) -> Self {
        Self {
            name: name.into(),
            method: "POST".into(),
            path: path.into(),
            body: Some(body.into()),
            headers: vec![("content-type".into(), "application/json".into())],
            auth: true,
        }
    }
}

/// Oracle run configuration.
#[derive(Debug, Clone)]
pub struct OracleConfig {
    /// QA-owned work dir: binaries, state, logs and captured output live
    /// here. Must not be inside the Go tree.
    pub work_dir: PathBuf,
    /// Readiness deadline for each spawned service.
    pub startup_timeout: Duration,
    /// Go packages to build (e.g. `./cmd/devin-2api`).
    pub build_packages: Vec<String>,
    /// Go packages to test (e.g. `./...` or `./internal/config`).
    pub test_packages: Vec<String>,
    /// Baseline requests to issue once the daemon is ready.
    pub requests: Vec<BaselineRequest>,
}

/// Result of a baseline capture.
#[derive(Debug, Serialize, Deserialize)]
pub struct Baseline {
    pub go_version: String,
    pub go_commit: String,
    pub git_clean_before: bool,
    pub git_clean_after: bool,
    pub built_binaries: Vec<String>,
    pub go_test_summary: GoTestSummary,
    /// `devin-2api -version` output.
    pub daemon_version: String,
    pub cases: Vec<CaseResult>,
    /// Canonicalized view of each case body (what parity compares).
    pub canonical_cases: Vec<serde_json::Value>,
    /// Daemon exit description, e.g. `exit:0`.
    pub daemon_exit: Option<String>,
    /// Canonicalization rules used, serialized for the record.
    pub rules_note: String,
}

/// The Go oracle bound to a reference checkout.
pub struct GoOracle {
    go_root: PathBuf,
    go_bin: PathBuf,
}

impl GoOracle {
    /// Oracle bound to `go_root`; the Go binary is resolved lazily per call
    /// (`QA_GO_BIN` > `~/sdk/go/bin/go` > `go` on PATH).
    pub fn new(go_root: PathBuf) -> Self {
        Self {
            go_root,
            go_bin: PathBuf::new(),
        }
    }

    /// Default Go reference location: `../devin2api` relative to this
    /// crate, overridable with `QA_GO_ROOT`.
    pub fn default_go_root() -> PathBuf {
        if let Ok(root) = std::env::var("QA_GO_ROOT") {
            return PathBuf::from(root);
        }
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../devin2api")
    }

    pub fn go_root(&self) -> &Path {
        &self.go_root
    }

    fn go(&self) -> PathBuf {
        if !self.go_bin.as_os_str().is_empty() {
            return self.go_bin.clone();
        }
        if let Ok(bin) = std::env::var("QA_GO_BIN") {
            return PathBuf::from(bin);
        }
        let sdk = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("sdk/go/bin/go");
        if sdk.is_file() {
            return sdk;
        }
        PathBuf::from("go")
    }

    /// `go version` output, e.g. `go version go1.27.1 linux/amd64`.
    pub fn go_version(&self) -> anyhow::Result<String> {
        let out = std::process::Command::new(self.go())
            .arg("version")
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "go version failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// `git rev-parse HEAD` in the Go tree.
    pub fn go_commit(&self) -> anyhow::Result<String> {
        let out = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&self.go_root)
            .output()?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Whether `git status --porcelain` is empty in the Go tree.
    pub fn git_status_clean(&self) -> anyhow::Result<bool> {
        let out = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&self.go_root)
            .output()?;
        Ok(String::from_utf8_lossy(&out.stdout).trim().is_empty())
    }

    /// Build `packages` into `<work_dir>/bin/`. Output paths are always
    /// outside the Go tree; `GOFLAGS=-mod=readonly` forbids go.mod writes.
    pub fn build(&self, work_dir: &Path, packages: &[String]) -> anyhow::Result<Vec<String>> {
        let bin_dir = work_dir.join("bin");
        std::fs::create_dir_all(&bin_dir)?;
        let mut built = Vec::new();
        for pkg in packages {
            let name = pkg.rsplit('/').next().unwrap_or(pkg);
            let out_path = bin_dir.join(name);
            let status = std::process::Command::new(self.go())
                .args(["build", "-o"])
                .arg(&out_path)
                .arg(pkg)
                .current_dir(&self.go_root)
                .env("GOFLAGS", "-mod=readonly")
                .env("GOTOOLCHAIN", "local")
                .status()?;
            if !status.success() {
                anyhow::bail!("go build -o {} {pkg} failed ({status})", out_path.display());
            }
            built.push(name.to_string());
        }
        Ok(built)
    }

    /// Run `go test` on `packages`, capturing the full log under
    /// `work_dir`. Returns the parsed summary; a nonzero go-test exit is
    /// recorded, not hidden.
    pub fn run_go_tests(
        &self,
        work_dir: &Path,
        packages: &[String],
    ) -> anyhow::Result<GoTestSummary> {
        let log_rel = "go-test.log".to_string();
        let log_path = work_dir.join(&log_rel);
        let log_file = std::fs::File::create(&log_path)?;
        let mut cmd = std::process::Command::new(self.go());
        cmd.arg("test")
            .args(packages)
            .current_dir(&self.go_root)
            .env("GOFLAGS", "-mod=readonly")
            .env("GOTOOLCHAIN", "local")
            .stdout(log_file.try_clone()?)
            .stderr(log_file)
            .stdin(Stdio::null());
        let status = cmd.status()?;
        let log = std::fs::read_to_string(&log_path).unwrap_or_default();
        let mut summary = GoTestSummary {
            command: format!("go test {}", packages.join(" ")),
            exit_code: status.code().unwrap_or(-1),
            ok: 0,
            failed: 0,
            no_tests: 0,
            total: 0,
            failed_packages: vec![],
            log: log_rel,
        };
        for line in log.lines() {
            if let Some(rest) = line
                .strip_prefix("ok  \t")
                .or_else(|| line.strip_prefix("ok \t"))
            {
                summary.ok += 1;
                let _ = rest;
            } else if line.starts_with("FAIL\t") {
                summary.failed += 1;
                let pkg = line
                    .trim_start_matches("FAIL\t")
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_string();
                if !pkg.is_empty() && pkg != "FAIL" {
                    summary.failed_packages.push(pkg);
                }
            } else if line.contains("[no test files]") {
                summary.no_tests += 1;
            }
        }
        summary.total = summary.ok + summary.failed + summary.no_tests;
        Ok(summary)
    }

    /// Full baseline capture: build, test, launch stub + daemon on isolated
    /// ports/state, issue `config.requests`, shut everything down.
    pub async fn capture_baseline(&self, config: &OracleConfig) -> anyhow::Result<Baseline> {
        self.check_work_dir(&config.work_dir)?;
        std::fs::create_dir_all(&config.work_dir)?;
        let git_clean_before = self.git_status_clean()?;
        let go_version = self.go_version()?;
        let go_commit = self.go_commit()?;

        let built = self.build(&config.work_dir, &config.build_packages)?;
        let test_summary = self.run_go_tests(&config.work_dir, &config.test_packages)?;

        // `devin-2api -version` (no daemon needed).
        let daemon_bin = config.work_dir.join("bin/devin-2api");
        let version_out = std::process::Command::new(&daemon_bin)
            .arg("-version")
            .output()?;
        let daemon_version = String::from_utf8_lossy(&version_out.stdout)
            .trim()
            .to_string();

        let (mut daemon, mut stub, base_url) = self.launch_stack(config).await?;

        let mut cases = Vec::new();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        for req in &config.requests {
            cases.push(
                self.issue(&client, &base_url, req, "qa-synthetic-key")
                    .await,
            );
        }

        let daemon_exit = daemon.shutdown(Duration::from_secs(15)).await.ok();
        let _ = stub.shutdown(Duration::from_secs(5)).await;
        let git_clean_after = self.git_status_clean()?;

        let rules = self.canon_rules(&config.work_dir);
        let canonical_cases = cases
            .iter()
            .map(|c| {
                serde_json::from_str::<serde_json::Value>(&c.response.body).map_or_else(
                    |_| serde_json::Value::String("<non-json body>".into()),
                    |v| super::compare::canonicalize_json(&v, &rules),
                )
            })
            .collect();

        Ok(Baseline {
            go_version,
            go_commit,
            git_clean_before,
            git_clean_after,
            built_binaries: built,
            go_test_summary: test_summary,
            daemon_version,
            cases,
            canonical_cases,
            daemon_exit,
            rules_note: "canonicalize: timestamps, durations, generated ids, \
                         resource metrics, work/state dir paths; never: status, \
                         error kinds, ordering, usage, signatures, presence, \
                         retry counts, truncation flags"
                .into(),
        })
    }

    /// Canonicalization rules for this oracle run, including path
    /// substitutions for the work/state dirs.
    pub fn canon_rules(&self, work_dir: &Path) -> CanonRules {
        CanonRules {
            path_substitutions: vec![
                (work_dir.display().to_string(), "work".into()),
                (self.go_root.display().to_string(), "go-root".into()),
            ],
            ..CanonRules::default()
        }
    }

    /// Launch upstreamstub + daemon on fresh loopback ports with isolated
    /// state and a scrubbed environment. On daemon readiness failure both
    /// children are shut down before the error propagates.
    async fn launch_stack(
        &self,
        config: &OracleConfig,
    ) -> anyhow::Result<(process::ManagedChild, process::ManagedChild, String)> {
        let stub_port = process::free_port()?;
        let daemon_port = process::free_port()?;
        let state_dir = config.work_dir.join("state");
        let home_dir = config.work_dir.join("home");
        std::fs::create_dir_all(&state_dir)?;
        std::fs::create_dir_all(&home_dir)?;

        // upstreamstub: deterministic full stream, small for a fast baseline.
        let stub_bin = config.work_dir.join("bin/upstreamstub");
        let mut stub_cmd = std::process::Command::new(&stub_bin);
        stub_cmd
            .arg("-listen")
            .arg(format!("127.0.0.1:{stub_port}"))
            .arg("-scenario")
            .arg("stream")
            .arg("-deltas")
            .arg("8")
            .arg("-delta-bytes")
            .arg("16")
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", &home_dir);
        let mut stub = process::spawn_logged(&config.work_dir, "upstreamstub", &mut stub_cmd)?;
        process::wait_tcp(&mut stub, stub_port, config.startup_timeout).await?;

        // Daemon config: loopback listen, stub upstream, synthetic token,
        // api key so the 401 path is exercised.
        let config_path = config.work_dir.join("config.yaml");
        std::fs::write(
            &config_path,
            format!(
                "server:\n  listen: \"127.0.0.1:{daemon_port}\"\n\
                 devin:\n  base_url: \"http://127.0.0.1:{stub_port}\"\n  token: \"qa-synthetic-token\"\n  model: \"stub-model\"\n\
                 auth:\n  api_key: \"qa-synthetic-key\"\n"
            ),
        )?;
        let daemon_bin = config.work_dir.join("bin/devin-2api");
        let mut daemon_cmd = std::process::Command::new(&daemon_bin);
        daemon_cmd
            .arg("-config")
            .arg(&config_path)
            .arg("-state-dir")
            .arg(&state_dir)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOME", &home_dir);
        let mut daemon = process::spawn_logged(&config.work_dir, "devin-2api", &mut daemon_cmd)?;
        let base_url = format!("http://127.0.0.1:{daemon_port}");
        if let Err(err) = process::wait_ready(
            &mut daemon,
            &format!("{base_url}/healthz"),
            config.startup_timeout,
        )
        .await
        {
            let _ = daemon.shutdown(Duration::from_secs(5)).await;
            let _ = stub.shutdown(Duration::from_secs(5)).await;
            return Err(err);
        }
        Ok((daemon, stub, base_url))
    }

    async fn issue(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        req: &BaselineRequest,
        api_key: &str,
    ) -> CaseResult {
        let url = format!("{base_url}{}", req.path);
        let mut builder = client.request(
            reqwest::Method::from_bytes(req.method.as_bytes()).unwrap_or(reqwest::Method::GET),
            &url,
        );
        for (name, value) in &req.headers {
            builder = builder.header(name, value);
        }
        if req.auth {
            builder = builder.bearer_auth(api_key);
        }
        if let Some(body) = &req.body {
            builder = builder.body(body.clone());
        }
        match builder.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(n, v)| (n.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect();
                let body = resp.text().await.unwrap_or_default();
                CaseResult {
                    name: req.name.clone(),
                    response: CapturedResponse {
                        status,
                        headers,
                        body,
                    },
                }
            }
            Err(err) => CaseResult {
                name: req.name.clone(),
                response: CapturedResponse {
                    status: 0,
                    headers: vec![],
                    body: format!("transport error: {err}"),
                },
            },
        }
    }

    fn check_work_dir(&self, work_dir: &Path) -> anyhow::Result<()> {
        let work = work_dir
            .canonicalize()
            .unwrap_or_else(|_| work_dir.to_path_buf());
        let root = self
            .go_root
            .canonicalize()
            .unwrap_or_else(|_| self.go_root.clone());
        if work.starts_with(&root) || work == root {
            anyhow::bail!(
                "oracle work dir {} must not be inside the Go reference {}",
                work.display(),
                root.display()
            );
        }
        Ok(())
    }
}

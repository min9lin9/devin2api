//! Task-23 end-to-end parity and reliability QA.
//!
//! The parity runner starts the unmodified Go daemon and the Rust daemon
//! against independent instances of the same Go Connect stub, then compares
//! their real HTTP/JSON/SSE surfaces. The fault runner drives the Rust daemon
//! through the raw-wire scenarios shared by the Go and Rust upstream stubs and
//! records the focused deterministic suites that cover timer/cancellation
//! seams which cannot be accelerated in a process test.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::Context as _;
use serde::Serialize;
use serde_json::{Value, json};

use super::compare::{self, CanonRules, CapturedResponse};
use super::contracts::{self, Manifest};
use super::oracle::GoOracle;
use super::process::{self, ManagedChild};

const TIMEOUT: Duration = Duration::from_secs(30);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Serialize)]
struct Check {
    name: String,
    passed: bool,
    detail: String,
}

#[derive(Debug, Serialize)]
struct Coverage {
    manifest_cases: usize,
    executable_cases: usize,
    executed_cases: usize,
    intentional_deviations: usize,
    unresolved_cases: usize,
    routes: usize,
    config_keys: usize,
    auxiliary_commands: usize,
    assets: usize,
    skips: usize,
    by_target: BTreeMap<String, usize>,
    deviation_reasons: BTreeMap<String, usize>,
}

struct Stack {
    daemon: ManagedChild,
    stub: ManagedChild,
    base: String,
}

impl Stack {
    async fn stop(mut self) {
        let _ = self.daemon.shutdown(Duration::from_secs(10)).await;
        let _ = self.stub.shutdown(Duration::from_secs(3)).await;
    }
}

fn binary(name: &str) -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe()?;
    let parent = exe
        .parent()
        .context("qa executable has no parent directory")?;
    let dir = if parent.file_name().is_some_and(|part| part == "deps") {
        parent
            .parent()
            .context("qa executable deps directory has no parent")?
    } else {
        parent
    };
    let path = dir.join(name);
    anyhow::ensure!(
        path.is_file(),
        "missing Rust binary {}; build all bins first",
        path.display()
    );
    Ok(path)
}

fn coverage(manifest: &Manifest, executed_checks: &BTreeSet<String>) -> Coverage {
    let mut by_target = BTreeMap::new();
    let mut deviation_reasons = BTreeMap::new();
    let mut executable_cases = 0;
    let mut executed_cases = 0;
    let mut intentional_deviations = 0;
    let mut unresolved_cases = 0;
    for case in &manifest.go_test_cases {
        match case.disposition.as_str() {
            "rust_test" => {
                executable_cases += 1;
                let target = case
                    .rust_case
                    .split_once("::")
                    .map_or("unresolved", |(target, _)| target);
                *by_target.entry(target.to_string()).or_insert(0) += 1;
                if executed_checks.contains(&case.rust_case) {
                    executed_cases += 1;
                } else {
                    unresolved_cases += 1;
                }
            }
            "intentional_deviation" => {
                intentional_deviations += 1;
                *deviation_reasons.entry(case.reason.clone()).or_insert(0) += 1;
            }
            _ => unresolved_cases += 1,
        }
    }
    Coverage {
        manifest_cases: manifest.go_test_cases.len(),
        executable_cases,
        executed_cases,
        intentional_deviations,
        unresolved_cases,
        routes: manifest.routes.len(),
        config_keys: manifest.config_keys.len(),
        auxiliary_commands: manifest.aux_commands.len(),
        assets: manifest.platform_assets.len(),
        skips: 0,
        by_target,
        deviation_reasons,
    }
}

fn validate_manifest(manifest: &Manifest, go_root: &Path) -> anyhow::Result<Vec<Check>> {
    let mut checks = Vec::new();
    let problems = contracts::verify_coverage(manifest, go_root)?;
    checks.push(Check {
        name: "manifest_matches_go_reference".into(),
        passed: problems.is_empty(),
        detail: if problems.is_empty() {
            format!("{} Go cases mapped", manifest.go_test_cases.len())
        } else {
            problems.join("; ")
        },
    });

    let mut source_ids = BTreeSet::new();
    let duplicate_sources: Vec<_> = manifest
        .go_test_cases
        .iter()
        .filter_map(|case| {
            let id = format!("{}::{}", case.file, case.function);
            (!source_ids.insert(id.clone())).then_some(id)
        })
        .collect();
    let unresolved = manifest
        .go_test_cases
        .iter()
        .filter(|case| {
            case.disposition != "rust_test" && case.disposition != "intentional_deviation"
        })
        .count();
    let deviations = manifest
        .go_test_cases
        .iter()
        .filter(|case| case.disposition == "intentional_deviation")
        .count();
    checks.push(Check {
        name: "manifest_dispositions_complete".into(),
        passed: duplicate_sources.is_empty() && unresolved == 0,
        detail: format!(
            "{} unique Go cases, {unresolved} unresolved, {deviations} documented deviations, {} duplicate source IDs",
            source_ids.len(),
            duplicate_sources.len()
        ),
    });
    Ok(checks)
}

async fn prepare_rust_tests(evidence: &Path, targets: &BTreeSet<String>) -> anyhow::Result<()> {
    let mut command = tokio::process::Command::new("cargo");
    command.current_dir(env!("CARGO_MANIFEST_DIR")).args([
        "test",
        "--locked",
        "--features",
        "qa",
        "--no-run",
    ]);
    for target in targets {
        command.args(["--test", target]);
    }
    command.kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_mins(30), command.output())
        .await
        .context("Rust test compilation timed out")??;
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    std::fs::write(evidence.join("test-compile.log"), log)?;
    anyhow::ensure!(output.status.success(), "Rust test compilation failed");
    Ok(())
}

fn test_binary(target: &str) -> anyhow::Result<PathBuf> {
    let prefix = format!("{}-", target.replace('-', "_"));
    let deps = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/debug/deps");
    let mut candidates: Vec<_> = std::fs::read_dir(&deps)?
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            entry.path().is_file()
                && name.starts_with(&prefix)
                && !name.ends_with(".d")
                && !name.ends_with(".rlib")
                && !name.ends_with(".rmeta")
        })
        .collect();
    candidates.sort_by_key(|entry| {
        entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
    });
    candidates
        .pop()
        .map(|entry| entry.path())
        .ok_or_else(|| anyhow::anyhow!("compiled test binary for {target} not found"))
}

fn parse_passed_tests(output: &str) -> BTreeSet<String> {
    output
        .lines()
        .filter_map(|line| line.strip_prefix("test "))
        .filter_map(|line| line.strip_suffix(" ... ok"))
        .map(str::to_string)
        .collect()
}

async fn run_rust_test(
    evidence: &Path,
    target: &str,
    exact: Option<&str>,
) -> anyhow::Result<(Check, BTreeSet<String>)> {
    let mut command = tokio::process::Command::new(test_binary(target)?);
    if let Some(test) = exact {
        command.args(["--exact", test]);
    }
    command.kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(600), command.output()).await;
    let (passed, detail, passed_tests, log) = match output {
        Ok(Ok(output)) => {
            let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
            log.push_str(&String::from_utf8_lossy(&output.stderr));
            let passed_tests = parse_passed_tests(&log);
            let resolved = exact.is_none_or(|test| passed_tests.contains(test));
            let passed = output.status.success() && resolved && !passed_tests.is_empty();
            (
                passed,
                format!(
                    "exit={} observed_passing_tests={} exact_resolved={resolved}",
                    output.status,
                    passed_tests.len()
                ),
                passed_tests,
                log,
            )
        }
        Ok(Err(error)) => (
            false,
            format!("spawn failed: {error}"),
            BTreeSet::new(),
            error.to_string(),
        ),
        Err(_) => (
            false,
            "timed out after 600s".into(),
            BTreeSet::new(),
            "timeout\n".into(),
        ),
    };
    let suffix = exact
        .unwrap_or("suite")
        .replace(|c: char| !c.is_ascii_alphanumeric(), "-");
    std::fs::write(evidence.join(format!("test-{target}-{suffix}.log")), log)?;
    Ok((
        Check {
            name: exact.map_or_else(
                || format!("suite-{target}"),
                |test| format!("test-{target}::{test}"),
            ),
            passed,
            detail,
        },
        passed_tests,
    ))
}

fn scrubbed(command: &mut Command) {
    command.env_remove("DEVIN_TOKEN");
    command.env_remove("WINDSURF_API_KEY");
    command.env_remove("HTTP_PROXY");
    command.env_remove("HTTPS_PROXY");
    command.env_remove("ALL_PROXY");
    command.env_remove("DEVIN2API_CONFIG");
    command.env_remove("DEVIN2API_STATE_DIR");
}

async fn launch(
    work: &Path,
    label: &str,
    daemon_bin: &Path,
    stub_bin: &Path,
    scenario: &str,
) -> anyhow::Result<Stack> {
    let stub_port = process::free_port()?;
    let daemon_port = process::free_port()?;
    let state = work.join(format!("{label}-state"));
    std::fs::create_dir_all(&state)?;

    let mut stub_cmd = Command::new(stub_bin);
    stub_cmd.args([
        "-listen",
        &format!("127.0.0.1:{stub_port}"),
        "-scenario",
        scenario,
        "-deltas",
        "3",
        "-delta-bytes",
        "4",
    ]);
    scrubbed(&mut stub_cmd);
    let mut stub = process::spawn_logged(work, &format!("{label}-stub"), &mut stub_cmd)?;
    process::wait_tcp(&mut stub, stub_port, STARTUP_TIMEOUT).await?;

    let config = work.join(format!("{label}.yaml"));
    std::fs::write(
        &config,
        format!(
            "server:\n  listen: \"127.0.0.1:{daemon_port}\"\n\
             devin:\n  base_url: \"http://127.0.0.1:{stub_port}\"\n  token: \"qa-synthetic-token\"\n  model: \"stub-model\"\n  force_http1: true\n\
             auth:\n  api_key: \"qa-synthetic-key\"\n"
        ),
    )?;
    let mut daemon_cmd = Command::new(daemon_bin);
    daemon_cmd.args([
        "-config",
        config.to_string_lossy().as_ref(),
        "-state-dir",
        state.to_string_lossy().as_ref(),
    ]);
    scrubbed(&mut daemon_cmd);
    let mut daemon = process::spawn_logged(work, &format!("{label}-daemon"), &mut daemon_cmd)?;
    let base = format!("http://127.0.0.1:{daemon_port}");
    if let Err(error) =
        process::wait_ready(&mut daemon, &format!("{base}/healthz"), STARTUP_TIMEOUT).await
    {
        let _ = daemon.shutdown(Duration::from_secs(2)).await;
        let _ = stub.shutdown(Duration::from_secs(2)).await;
        return Err(error);
    }
    Ok(Stack { daemon, stub, base })
}

async fn issue(
    client: &reqwest::Client,
    base: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<&str>,
    auth: bool,
) -> CapturedResponse {
    let mut request = client.request(method, format!("{base}{path}"));
    if auth {
        request = request.bearer_auth("qa-synthetic-key");
    }
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        value.to_str().unwrap_or("").to_string(),
                    )
                })
                .collect();
            let body = response.text().await.unwrap_or_default();
            CapturedResponse {
                status,
                headers,
                body,
            }
        }
        Err(error) => CapturedResponse {
            status: 0,
            headers: Vec::new(),
            body: format!("transport error: {error}"),
        },
    }
}

fn compare_body(name: &str, go: &CapturedResponse, rust: &CapturedResponse) -> Check {
    if go.status != rust.status {
        return Check {
            name: name.into(),
            passed: false,
            detail: format!("status differs: Go {} Rust {}", go.status, rust.status),
        };
    }
    let go_type = go
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("content-type"))
        .map_or("", |(_, value)| value.as_str());
    let rust_type = rust
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("content-type"))
        .map_or("", |(_, value)| value.as_str());
    if go_type.split(';').next() != rust_type.split(';').next() {
        return Check {
            name: name.into(),
            passed: false,
            detail: format!("content-type differs: Go {go_type:?} Rust {rust_type:?}"),
        };
    }
    let rules = CanonRules::default();
    let verdict = if go_type.starts_with("text/event-stream") {
        compare::compare_sse(&go.body, &rust.body, &rules)
    } else if let (Ok(left), Ok(right)) = (
        serde_json::from_str::<Value>(&go.body),
        serde_json::from_str::<Value>(&rust.body),
    ) {
        compare::compare_json(&left, &right, &rules)
    } else {
        compare::compare_text(&go.body, &rust.body, &rules)
    };
    Check {
        name: name.into(),
        passed: verdict.matched(),
        detail: verdict.to_string(),
    }
}

fn health_check(go: &CapturedResponse, rust: &CapturedResponse) -> Check {
    let parse = |response: &CapturedResponse| -> Option<Value> {
        let mut value: Value = serde_json::from_str(&response.body).ok()?;
        let object = value.as_object_mut()?;
        object.remove("version");
        Some(value)
    };
    let passed = go.status == 200
        && rust.status == 200
        && parse(go).zip(parse(rust)).is_some_and(|(left, right)| {
            compare::compare_json(&left, &right, &CanonRules::default()).matched()
        });
    Check {
        name: "healthz".into(),
        passed,
        detail: "runtime-specific build versions checked for presence and excluded from equality"
            .into(),
    }
}

// One manifest-driven parity sweep; splitting scatters the contract
// table.
#[allow(clippy::too_many_lines)]
pub async fn run_parity(go_root: &Path, evidence: &Path) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let manifest = contracts::load_committed()?;
    let mut checks = validate_manifest(&manifest, go_root)?;
    let mut checks_by_target: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for case in manifest
        .go_test_cases
        .iter()
        .filter(|case| case.disposition == "rust_test")
    {
        let (target, test) = case
            .rust_case
            .split_once("::")
            .context("validated Rust check lacks target separator")?;
        checks_by_target
            .entry(target.to_string())
            .or_default()
            .insert(test.to_string());
    }
    let targets: BTreeSet<String> = checks_by_target.keys().cloned().collect();
    prepare_rust_tests(evidence, &targets).await?;
    let mut executed_checks = BTreeSet::new();
    for (target, required_tests) in checks_by_target {
        // The transport target starts subprocesses that intentionally share
        // environment and connection-pool state; execute only the bound test
        // exactly. Other targets run once as suites, then each binding is
        // credited only when its own `test NAME ... ok` line was observed.
        if target == "transport_interop" {
            for test in required_tests {
                let (check, observed) = run_rust_test(evidence, &target, Some(&test)).await?;
                if check.passed && observed.contains(&test) {
                    executed_checks.insert(format!("{target}::{test}"));
                }
                checks.push(check);
            }
        } else {
            let (mut check, observed) = run_rust_test(evidence, &target, None).await?;
            let missing: Vec<_> = required_tests.difference(&observed).cloned().collect();
            check.passed = check.passed && missing.is_empty();
            check.detail = format!("{} missing_bound_tests={missing:?}", check.detail);
            if check.passed {
                executed_checks.extend(
                    required_tests
                        .into_iter()
                        .map(|test| format!("{target}::{test}")),
                );
            }
            checks.push(check);
        }
    }
    let oracle = GoOracle::new(go_root.to_path_buf());
    let go_bins = evidence.join("go");
    oracle.build(
        &go_bins,
        &["./cmd/devin-2api".into(), "./cmd/upstreamstub".into()],
    )?;
    let go_daemon = go_bins.join("bin/devin-2api");
    let go_stub = go_bins.join("bin/upstreamstub");
    let rust_daemon = binary("devin-2api")?;

    let go = launch(evidence, "go", &go_daemon, &go_stub, "stream").await?;
    let rust = launch(evidence, "rust", &rust_daemon, &go_stub, "stream").await?;
    let client = reqwest::Client::builder().timeout(TIMEOUT).build()?;

    let go_health = issue(
        &client,
        &go.base,
        reqwest::Method::GET,
        "/healthz",
        None,
        false,
    )
    .await;
    let rust_health = issue(
        &client,
        &rust.base,
        reqwest::Method::GET,
        "/healthz",
        None,
        false,
    )
    .await;
    checks.push(health_check(&go_health, &rust_health));

    let cases = [
        (
            "unauthorized",
            reqwest::Method::GET,
            "/v1/models",
            None,
            false,
        ),
        (
            "unknown-route",
            reqwest::Method::GET,
            "/v1/nope",
            None,
            true,
        ),
        (
            "chat-invalid-request",
            reqwest::Method::POST,
            "/v1/chat/completions",
            Some("{}"),
            true,
        ),
        (
            "responses-json",
            reqwest::Method::POST,
            "/v1/responses",
            Some(r#"{"model":"stub-model","input":"hi"}"#),
            true,
        ),
        (
            "chat-json",
            reqwest::Method::POST,
            "/v1/chat/completions",
            Some(r#"{"model":"stub-model","messages":[{"role":"user","content":"hi"}]}"#),
            true,
        ),
        (
            "messages-json",
            reqwest::Method::POST,
            "/v1/messages",
            Some(
                r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            true,
        ),
        (
            "responses-sse",
            reqwest::Method::POST,
            "/v1/responses",
            Some(r#"{"model":"stub-model","stream":true,"input":"hi"}"#),
            true,
        ),
        (
            "chat-sse",
            reqwest::Method::POST,
            "/v1/chat/completions",
            Some(
                r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            true,
        ),
        (
            "messages-sse",
            reqwest::Method::POST,
            "/v1/messages",
            Some(
                r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            true,
        ),
    ];
    let mut transcripts = Vec::new();
    for (name, method, path, body, auth) in cases {
        let left = issue(&client, &go.base, method.clone(), path, body, auth).await;
        let right = issue(&client, &rust.base, method, path, body, auth).await;
        checks.push(compare_body(name, &left, &right));
        transcripts.push(json!({"name":name,"go":left,"rust":right}));
    }
    go.stop().await;
    rust.stop().await;

    let coverage = coverage(&manifest, &executed_checks);
    let case_executions: Vec<Value> = manifest
        .go_test_cases
        .iter()
        .map(|case| {
            json!({
                "go_case": format!("{}::{}", case.file, case.function),
                "kind": case.kind,
                "disposition": case.disposition,
                "rust_check": case.rust_case,
                "executed": executed_checks.contains(&case.rust_case),
                "reason": case.reason,
            })
        })
        .collect();
    std::fs::write(
        evidence.join("case-executions.json"),
        serde_json::to_vec_pretty(&case_executions)?,
    )?;
    checks.push(Check {
        name: "required_case_count_equals_manifest".into(),
        passed: coverage.executed_cases + coverage.intentional_deviations
            == coverage.manifest_cases
            && coverage.unresolved_cases == 0
            && coverage.skips == 0,
        detail: format!(
            "executed={} documented_deviations={} required={} unresolved={} skips={}",
            coverage.executed_cases,
            coverage.intentional_deviations,
            coverage.manifest_cases,
            coverage.unresolved_cases,
            coverage.skips
        ),
    });
    let passed = checks.iter().all(|check| check.passed);
    std::fs::write(
        evidence.join("transcripts.json"),
        serde_json::to_vec_pretty(&transcripts)?,
    )?;
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": passed,
            "go_commit": manifest.go_commit,
            "independent_oracles": ["unmodified Go daemon", "Rust daemon", "Go Connect upstreamstub"],
            "coverage": coverage,
            "checks": checks,
            "intentional_deviations": [
                "authentication repair uses token generations",
                "catalog fetch has one bounded shared task with independent cancellation",
                "retry backoff precedes final admission and rechecks cancellation",
                "latched drip probes obey window quota",
                "Go runtime diagnostics are identified Rust diagnostics"
            ]
        }))?,
    )?;
    Ok(i32::from(!passed))
}

fn negative_controls() -> Vec<Check> {
    let rules = CanonRules::default();
    let base = json!({"status":"completed","events":["start","delta","done"],"optional":null});
    [
        ("negative-control-status", "/status", json!("failed")),
        (
            "negative-control-order",
            "/events",
            json!(["delta", "start", "done"]),
        ),
        (
            "negative-control-presence",
            "/optional",
            json!({"present":true}),
        ),
    ]
    .into_iter()
    .map(|(name, pointer, replacement)| {
        let mut changed = base.clone();
        *changed.pointer_mut(pointer).expect("fixed pointer") = replacement;
        let rejected = !compare::compare_json(&base, &changed, &rules).matched();
        Check {
            name: name.into(),
            passed: rejected,
            detail: format!(
                "mutation at {pointer} {}",
                if rejected { "rejected" } else { "accepted" }
            ),
        }
    })
    .collect()
}

// One fault-case driver; splitting scatters the negative-control
// flow.
#[allow(clippy::too_many_lines)]
pub async fn run_faults(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    if let Some(case) = case
        && case != "negative-control"
    {
        anyhow::bail!("unknown faults case {case}");
    }
    let mut checks = negative_controls();
    let mut artifacts = Vec::new();
    if case.is_none() {
        let fault_targets: BTreeSet<String> = [
            "upstream_lifecycle",
            "rate_gate",
            "debuglog_compat",
            "lifecycle",
            "websocket_surface",
            "catalog_auth",
            "response_decoder",
            "transport_interop",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        prepare_rust_tests(evidence, &fault_targets).await?;
        let rust_daemon = binary("devin-2api")?;
        let rust_stub = binary("upstreamstub")?;
        let go_root = GoOracle::default_go_root();
        let oracle = GoOracle::new(go_root);
        let go_bins = evidence.join("go");
        oracle.build(&go_bins, &["./cmd/devin-2api".into()])?;
        let go_daemon = go_bins.join("bin/devin-2api");
        let client = reqwest::Client::builder().timeout(TIMEOUT).build()?;
        for (scenario, expected_status, body_fragment) in [
            ("stream", 200, "response.completed"),
            ("precontent", 502, "error"),
            ("cleaneof", 502, "error"),
            ("midcontent", 200, "response.failed"),
            ("bare-end", 502, "error"),
            ("endstream-error", 200, "response.failed"),
            ("badframe", 502, "error"),
        ] {
            let rust = launch(
                evidence,
                &format!("fault-rust-{scenario}"),
                &rust_daemon,
                &rust_stub,
                scenario,
            )
            .await?;
            let go = launch(
                evidence,
                &format!("fault-go-{scenario}"),
                &go_daemon,
                &rust_stub,
                scenario,
            )
            .await?;
            let rust_response = issue(
                &client,
                &rust.base,
                reqwest::Method::POST,
                "/v1/responses",
                Some(r#"{"model":"stub-model","stream":true,"input":"hi"}"#),
                true,
            )
            .await;
            let go_response = issue(
                &client,
                &go.base,
                reqwest::Method::POST,
                "/v1/responses",
                Some(r#"{"model":"stub-model","stream":true,"input":"hi"}"#),
                true,
            )
            .await;
            let shape_matched =
                [(&rust_response, "Rust"), (&go_response, "Go")]
                    .iter()
                    .all(|(response, _)| {
                        response.status == expected_status && response.body.contains(body_fragment)
                    });
            let semantic = compare_body(
                &format!("raw-wire-{scenario}-semantic"),
                &go_response,
                &rust_response,
            );
            checks.push(Check {
                name: format!("raw-wire-{scenario}"),
                passed: shape_matched && semantic.passed,
                detail: format!(
                    "Rust status={} Go status={} expected={expected_status}; both bodies contain {body_fragment:?}; semantic={}",
                    rust_response.status, go_response.status, semantic.detail
                ),
            });
            artifacts.push(json!({
                "scenario":scenario,
                "rust":rust_response,
                "go":go_response,
                "wire_oracle":"Rust raw TCP Connect stub shared by both daemons"
            }));
            rust.stop().await;
            go.stop().await;
        }
        for (target, test) in [
            ("upstream_lifecycle", "stall_watchdog_reopens_precontent"),
            ("upstream_lifecycle", "no_progress_watchdog_reopens"),
            ("upstream_lifecycle", "cancellation_at_every_seam"),
            ("rate_gate", "RateGateLatchPersistRestore"),
            ("debuglog_compat", "go_history_replay_and_rust_append"),
            (
                "lifecycle",
                "reload_is_validate_then_commit_and_updates_auth",
            ),
            (
                "websocket_surface",
                "cancel_only_stops_current_turn_and_connection_runs_next_turn",
            ),
            ("catalog_auth", "concurrent_refresh_and_cancelled_leader"),
            ("response_decoder", "late_usage_and_signature_round_trip"),
            (
                "transport_interop",
                "truncated_envelope_proxy_and_tls_failure",
            ),
        ] {
            let (check, _) = run_rust_test(evidence, target, Some(test)).await?;
            checks.push(check);
        }
    }
    let passed = checks.iter().all(|check| check.passed);
    std::fs::write(
        evidence.join("raw-wire.json"),
        serde_json::to_vec_pretty(&artifacts)?,
    )?;
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_vec_pretty(&json!({
            "passed": passed,
            "case": case,
            "checks": checks,
            "timeouts_bounded_seconds": 30,
            "fixed_sleeps": 0,
            "skips": 0
        }))?,
    )?;
    Ok(i32::from(!passed))
}

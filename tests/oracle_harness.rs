//! Integration tests for the task-2 QA infrastructure: the Go oracle runner,
//! the differential comparator and the contracts manifest.
//!
//! The oracle tests build the unmodified Go reference into a QA-owned work
//! directory (never inside G) and exercise the real daemon against the real
//! Go upstreamstub on loopback. They require the Go 1.27.1 toolchain
//! (`QA_GO_BIN` overrides discovery; default `~/sdk/go/bin/go` then `go` on
//! PATH) and the Go reference checkout (`QA_GO_ROOT` overrides the default
//! `../devin2api` next to this repository).

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use devin2api::qa::compare::{self, CanonRules};
use devin2api::qa::contracts;
use devin2api::qa::oracle::{BaselineRequest, OracleConfig};
use devin2api::qa::process;
use support::{require_go_oracle, work_dir};

// ---------------------------------------------------------------------------
// Comparator: canonicalization and diffing (pure, no oracle needed).
// ---------------------------------------------------------------------------

#[test]
fn comparator_canonicalizes_volatile_fields() {
    let rules = CanonRules {
        path_substitutions: vec![
            ("/tmp/qa-work-123".into(), "work".into()),
            ("/tmp/qa-work-999".into(), "work".into()),
        ],
        ..CanonRules::default()
    };
    let left = serde_json::json!({
        "status": "ok",
        "version": "dev-abc123",
        "uptime_seconds": 12,
        "pid": 4242,
        "request_id": "req_9f8e7d6c5b4a32109f8e7d6c5b4a3210",
        "created": 1_758_000_000,
        "id": "chatcmpl-00112233445566778899aabbccddeeff",
        "session": "018f3c5e-2b7a-7c4d-9e1f-0a1b2c3d4e5f",
        "dir": "/tmp/qa-work-123/logs/2026-09-16T03-00-00_abcd",
        "message": "ok"
    });
    let right = serde_json::json!({
        "status": "ok",
        "version": "dev-abc123",
        "uptime_seconds": 987,
        "pid": 777,
        "request_id": "req_0000000000000000aaaaaaaaaaaaaaaa",
        "created": 1_758_009_999,
        "id": "chatcmpl-ffeeddccbbaa00998877665544332211",
        "session": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "dir": "/tmp/qa-work-999/logs/2026-09-16T04-00-00_wxyz",
        "message": "ok"
    });
    let report = compare::compare_json(&left, &right, &rules);
    assert!(
        report.matched(),
        "volatile fields must canonicalize away: {report}"
    );
}

#[test]
fn comparator_preserves_id_linkage() {
    // The same generated id appearing twice must map to the same placeholder,
    // and a changed linkage (id A where id B was) must be detected.
    let rules = CanonRules::default();
    let left = serde_json::json!({
        "first": "req_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "second": "req_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    });
    let same = serde_json::json!({
        "first": "req_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "second": "req_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    });
    let broken = serde_json::json!({
        "first": "req_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "second": "req_cccccccccccccccccccccccccccccccc"
    });
    assert!(compare::compare_json(&left, &same, &rules).matched());
    let report = compare::compare_json(&left, &broken, &rules);
    assert!(!report.matched(), "broken id linkage must be detected");
}

#[test]
fn comparator_does_not_normalize_semantic_fields() {
    // Plan contract: status, error kinds, ordering, usage, signatures,
    // presence, retry counts and truncation flags are never normalized.
    let rules = CanonRules::default();
    let base = serde_json::json!({
        "status": "ok",
        "error": {"type": "rate_limit_error", "code": "rate_limited"},
        "events": ["a", "b", "c"],
        "usage": {"input_tokens": 10, "output_tokens": 20},
        "signature": "deadbeef",
        "retries": 2,
        "truncated": false,
        "optional": null
    });
    for (pointer, value) in [
        ("/status", serde_json::json!("error")),
        ("/error/type", serde_json::json!("server_error")),
        ("/events", serde_json::json!(["a", "c", "b"])),
        ("/usage/output_tokens", serde_json::json!(21)),
        ("/signature", serde_json::json!("feedface")),
        ("/retries", serde_json::json!(3)),
        ("/truncated", serde_json::json!(true)),
        ("/optional", serde_json::json!("present")),
    ] {
        let mut mutated = base.clone();
        *mutated.pointer_mut(pointer).expect("pointer") = value;
        let report = compare::compare_json(&base, &mutated, &rules);
        assert!(
            !report.matched(),
            "mutation at {pointer} must be detected: {report}"
        );
    }
}

#[test]
fn comparator_detects_missing_and_extra_keys() {
    let rules = CanonRules::default();
    let left = serde_json::json!({"a": 1, "b": {"c": 2}});
    let missing = serde_json::json!({"a": 1, "b": {}});
    let extra = serde_json::json!({"a": 1, "b": {"c": 2, "d": 3}});
    assert!(!compare::compare_json(&left, &missing, &rules).matched());
    assert!(!compare::compare_json(&left, &extra, &rules).matched());
}

#[test]
fn comparator_sse_streams() {
    let rules = CanonRules::default();
    let left = concat!(
        "event: response.created\n",
        "data: {\"id\":\"resp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"created_at\":1758000000}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"delta\":\"hello\"}\n\n",
        "data: [DONE]\n\n"
    );
    let same = concat!(
        "event: response.created\n",
        "data: {\"id\":\"resp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"created_at\":1758009999}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"delta\":\"hello\"}\n\n",
        "data: [DONE]\n\n"
    );
    let reordered = concat!(
        "event: response.output_text.delta\n",
        "data: {\"delta\":\"hello\"}\n\n",
        "event: response.created\n",
        "data: {\"id\":\"resp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"created_at\":1758009999}\n\n",
        "data: [DONE]\n\n"
    );
    assert!(compare::compare_sse(left, same, &rules).matched());
    assert!(
        !compare::compare_sse(left, reordered, &rules).matched(),
        "SSE event reordering must be detected"
    );
}

#[test]
fn comparator_response_status_and_headers() {
    let rules = CanonRules::default();
    let expected = compare::CapturedResponse {
        status: 200,
        headers: vec![
            ("content-type".into(), "application/json".into()),
            ("date".into(), "Tue, 16 Sep 2026 03:00:00 GMT".into()),
            (
                "request-id".into(),
                "req_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            ),
        ],
        body: "{\"ok\":true}".into(),
    };
    let mut same = expected.clone();
    same.headers[1].1 = "Tue, 16 Sep 2026 09:30:00 GMT".into();
    same.headers[2].1 = "req_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into();
    assert!(compare::compare_response(&expected, &same, &rules).matched());

    let mut wrong_status = same.clone();
    wrong_status.status = 500;
    assert!(!compare::compare_response(&expected, &wrong_status, &rules).matched());
}

// ---------------------------------------------------------------------------
// Contracts manifest: scanning G and coverage verification.
// ---------------------------------------------------------------------------

#[test]
fn contracts_manifest_is_exhaustive() {
    let Some(oracle) = require_go_oracle() else {
        return;
    };
    let manifest = contracts::scan_go_reference(oracle.go_root()).expect("scan G");
    assert!(
        manifest.go_test_cases.len() > 100,
        "expected >100 mapped Go test cases, got {}",
        manifest.go_test_cases.len()
    );
    assert!(
        manifest.routes.len() >= 25,
        "expected the full route surface, got {} routes",
        manifest.routes.len()
    );
    assert!(
        manifest.config_keys.len() >= 25,
        "expected every YAML key, got {}",
        manifest.config_keys.len()
    );
    for name in [
        "configs", "status", "assign", "chat", "replay", "hist", "rerun", "bigctx", "misc", "edge",
    ] {
        assert!(
            manifest
                .aux_commands
                .iter()
                .any(|c| c.binary == "probe" && c.name == name),
            "probe subcommand {name} missing from manifest"
        );
    }
    let problems = contracts::verify_coverage(&manifest, oracle.go_root()).expect("verify");
    assert!(problems.is_empty(), "coverage problems: {problems:?}");
}

#[test]
fn contracts_manifest_committed_copy_matches_scan() {
    let Some(oracle) = require_go_oracle() else {
        return;
    };
    let committed = contracts::load_committed().expect("load tests/contracts.json");
    let scanned = contracts::scan_go_reference(oracle.go_root()).expect("scan G");
    assert_eq!(
        committed, scanned,
        "tests/contracts.json is stale; regenerate with `cargo run --features qa --bin qa -- manifest --evidence <dir>`"
    );
}

#[test]
fn contracts_rejects_missing_case() {
    let Some(oracle) = require_go_oracle() else {
        return;
    };
    let mut manifest = contracts::scan_go_reference(oracle.go_root()).expect("scan G");
    manifest.go_test_cases.pop();
    let problems = contracts::verify_coverage(&manifest, oracle.go_root()).expect("verify");
    assert!(
        !problems.is_empty(),
        "dropping a mapped case must fail coverage verification"
    );
}

// ---------------------------------------------------------------------------
// Oracle: build, launch, baseline capture, timeout and cleanup.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_builds_and_captures_baseline() {
    let Some(oracle) = require_go_oracle() else {
        return;
    };
    let work = work_dir("baseline");
    let config = OracleConfig {
        work_dir: work.clone(),
        startup_timeout: Duration::from_secs(30),
        build_packages: vec!["./cmd/devin-2api".into(), "./cmd/upstreamstub".into()],
        test_packages: vec!["./internal/config".into()],
        requests: vec![
            BaselineRequest::get("healthz", "/healthz"),
            BaselineRequest::get("models", "/v1/models"),
            BaselineRequest::post_json(
                "chat-stream",
                "/v1/chat/completions",
                r#"{"model":"stub","stream":true,"messages":[{"role":"user","content":"Reply exactly: pong"}]}"#,
            ),
            BaselineRequest::post_json("chat-bad-json", "/v1/chat/completions", "{not json"),
        ],
    };
    let baseline = oracle
        .capture_baseline(&config)
        .await
        .expect("baseline capture");
    assert_eq!(baseline.cases.len(), 4);
    assert!(
        baseline.cases.iter().all(|c| c.response.status > 0),
        "every case must record a real HTTP response"
    );
    let healthz = baseline
        .cases
        .iter()
        .find(|c| c.name == "healthz")
        .expect("healthz case");
    assert_eq!(healthz.response.status, 200);
    let chat = baseline
        .cases
        .iter()
        .find(|c| c.name == "chat-stream")
        .expect("chat case");
    assert_eq!(chat.response.status, 200);
    assert!(
        chat.response.body.contains("data:"),
        "chat-stream must be a real SSE stream: {}",
        chat.response.body
    );
    assert_eq!(baseline.daemon_exit.as_deref(), Some("exit status: 0"));
    assert!(
        baseline.go_test_summary.total > 0,
        "go test must have run at least one package"
    );
    // The oracle must leave nothing running and G must be untouched.
    assert!(oracle.git_status_clean().expect("git status"));
    let _ = std::fs::remove_dir_all(&work);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oracle_startup_timeout_and_cleanup() {
    // A child that binds nothing must trip the readiness timeout, be reaped,
    // and leave no process behind.
    let work = work_dir("timeout");
    let mut child = process::spawn_logged(
        &work,
        "sleeper",
        std::process::Command::new("sleep").arg("60"),
    )
    .expect("spawn sleeper");
    let pid = child.pid();
    let err = process::wait_ready(
        &mut child,
        "http://127.0.0.1:1/healthz",
        Duration::from_millis(500),
    )
    .await
    .expect_err("sleeper must never become ready");
    assert!(
        err.to_string().contains("timeout") || err.to_string().contains("ready"),
        "unexpected error: {err}"
    );
    child
        .shutdown(Duration::from_secs(5))
        .await
        .expect("shutdown");
    assert!(!process::pid_alive(pid), "sleeper pid {pid} must be reaped");

    // A child that ignores SIGTERM must be escalated to SIGKILL.
    let mut stubborn = process::spawn_logged(
        &work,
        "stubborn",
        std::process::Command::new("bash")
            .arg("-c")
            .arg("trap '' TERM; sleep 60"),
    )
    .expect("spawn stubborn");
    let stubborn_pid = stubborn.pid();
    stubborn
        .shutdown(Duration::from_millis(300))
        .await
        .expect("escalating shutdown");
    assert!(
        !process::pid_alive(stubborn_pid),
        "SIGTERM-ignoring child must be SIGKILLed"
    );
    let _ = std::fs::remove_dir_all(&work);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detects_mutated_status_and_missing_case() {
    // Negative control: a comparator verdict over a case set must reject a
    // deliberately mutated status and a dropped case.
    let rules = CanonRules::default();
    let expected = vec![compare::CaseResult {
        name: "healthz".into(),
        response: compare::CapturedResponse {
            status: 200,
            headers: vec![],
            body: "{\"status\":\"ok\"}".into(),
        },
    }];
    let mut mutated = expected.clone();
    mutated[0].response.status = 503;
    let verdict = compare::compare_case_sets(&expected, &mutated, &rules);
    assert!(!verdict.matched(), "mutated status must fail: {verdict}");

    let missing: Vec<compare::CaseResult> = vec![];
    let verdict = compare::compare_case_sets(&expected, &missing, &rules);
    assert!(!verdict.matched(), "missing case must fail: {verdict}");

    let verdict = compare::compare_case_sets(&expected, &expected.clone(), &rules);
    assert!(verdict.matched(), "identical sets must pass: {verdict}");

    // Zero executed cases is a failure, never a pass.
    let empty: Vec<compare::CaseResult> = vec![];
    let verdict = compare::compare_case_sets(&empty, &empty, &rules);
    assert!(!verdict.matched(), "zero cases must not pass");
}

#[test]
fn subcommand_registry_lists_planned_commands() {
    let names: BTreeSet<&str> = devin2api::qa::SUBCOMMANDS.iter().map(|s| s.name).collect();
    for required in [
        "baseline",
        "http",
        "sse",
        "websocket",
        "dashboard-api",
        "diagnostics",
        "parity",
        "faults",
        "lifecycle",
        "cli",
        "bench",
        "stress",
        "documented-smoke",
        "packaging",
        "coverage",
        "live",
        "final-surface",
        "manifest",
    ] {
        assert!(
            names.contains(required),
            "qa subcommand {required} not registered"
        );
    }
    // Landed subcommands must be real handlers, not stale placeholders.
    let http = devin2api::qa::SUBCOMMANDS
        .iter()
        .find(|s| s.name == "http")
        .expect("http registered");
    assert!(
        http.implemented,
        "http task landed without enabling its handler"
    );
    let baseline = devin2api::qa::SUBCOMMANDS
        .iter()
        .find(|s| s.name == "baseline")
        .expect("baseline registered");
    assert!(baseline.implemented, "baseline is implemented by task 2");
}

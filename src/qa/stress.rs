//! Task-24 reliability stress gate: 100,000 healthy stub-backed requests
//! across SSE/JSON/WS with zero unexpected failures, duplicate/missing
//! terminal events or leaked permits; cancellation integration p99 under
//! 250 ms with no send after observed cancellation; clean shutdown with
//! zero owned request tasks.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::bench::{self, Protocol, StubShape};

/// Hard cap for one load leg (100k requests at ~800 rps finishes in
/// minutes; this only backstops a hung run).
const LEG_BUDGET: Duration = Duration::from_mins(25);
const CANCEL_POLL: Duration = Duration::from_millis(10);
const CANCEL_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const CANCEL_P99_LIMIT_MS: f64 = 250.0;

const THROUGHPUT_SHAPE: StubShape = StubShape {
    deltas: 20,
    delta_bytes: 32,
    interval_ms: 0,
    ttfb_ms: 0,
};
const SLOW_SHAPE: StubShape = StubShape {
    deltas: 400,
    delta_bytes: 32,
    interval_ms: 50,
    ttfb_ms: 0,
};

#[derive(Default)]
struct Tally {
    completed: u64,
    errors: u64,
    missing_terminal: u64,
    duplicate_terminal: u64,
    error_kinds: BTreeMap<String, u64>,
}

impl Tally {
    fn record(&mut self, record: &bench::RequestRecord) {
        if let Some(error) = &record.error {
            self.errors += 1;
            *self.error_kinds.entry(error.clone()).or_insert(0) += 1;
            if error.contains("terminal events: 0") {
                self.missing_terminal += 1;
            } else if error.starts_with("terminal events:") {
                self.duplicate_terminal += 1;
            }
        } else {
            self.completed += 1;
        }
    }
}

/// Fixed-concurrency HTTP leg until `target` requests complete.
async fn http_leg(
    base: &str,
    api_key: &str,
    protocol: Protocol,
    concurrency: usize,
    target: u64,
) -> Tally {
    let client = reqwest::Client::new();
    let issued = Arc::new(AtomicU64::new(0));
    let records = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let deadline = Instant::now() + LEG_BUDGET;
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let base = base.to_string();
        let key = api_key.to_string();
        let issued = issued.clone();
        let records = records.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                if Instant::now() >= deadline {
                    records.lock().await.push(bench::RequestRecord {
                        error: Some("leg budget exhausted".into()),
                        ..Default::default()
                    });
                    return;
                }
                let n = issued.fetch_add(1, Ordering::SeqCst);
                if n >= target {
                    return;
                }
                let record = bench::http_request(&client, &base, &key, protocol).await;
                records.lock().await.push(record);
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let mut tally = Tally::default();
    for record in records.lock().await.iter() {
        tally.record(record);
    }
    tally
}

/// WS leg: `connections` persistent sockets, sequential turns until target.
async fn ws_leg(base: &str, api_key: &str, connections: usize, target: u64) -> Tally {
    let issued = Arc::new(AtomicU64::new(0));
    let records = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let deadline = Instant::now() + LEG_BUDGET;
    let mut tasks = Vec::new();
    for _ in 0..connections {
        let base = base.to_string();
        let key = api_key.to_string();
        let issued = issued.clone();
        let records = records.clone();
        tasks.push(tokio::spawn(async move {
            let mut ws = match bench::WsClient::connect(&base, &key).await {
                Ok(ws) => ws,
                Err(err) => {
                    records.lock().await.push(bench::RequestRecord {
                        error: Some(format!("ws connect: {err}")),
                        ..Default::default()
                    });
                    return;
                }
            };
            loop {
                if Instant::now() >= deadline {
                    records.lock().await.push(bench::RequestRecord {
                        error: Some("leg budget exhausted".into()),
                        ..Default::default()
                    });
                    return;
                }
                let n = issued.fetch_add(1, Ordering::SeqCst);
                if n >= target {
                    return;
                }
                let record = match ws.turn().await {
                    Ok((_, total, bytes, terminals)) => bench::RequestRecord {
                        total_ms: total,
                        bytes,
                        error: if terminals == 1 {
                            None
                        } else {
                            Some(format!("terminal events: {terminals}"))
                        },
                        terminal_ok: terminals == 1,
                        ..Default::default()
                    },
                    Err(err) => bench::RequestRecord {
                        error: Some(format!("ws turn: {err}")),
                        ..Default::default()
                    },
                };
                let failed = record.error.is_some();
                records.lock().await.push(record);
                if failed {
                    match bench::WsClient::connect(&base, &key).await {
                        Ok(fresh) => ws = fresh,
                        Err(err) => {
                            records.lock().await.push(bench::RequestRecord {
                                error: Some(format!("ws reconnect: {err}")),
                                ..Default::default()
                            });
                            return;
                        }
                    }
                }
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let mut tally = Tally::default();
    for record in records.lock().await.iter() {
        tally.record(record);
    }
    tally
}

/// Count of in-flight requests reported by the daemon (debug manager).
async fn active_count(client: &reqwest::Client, base: &str) -> anyhow::Result<usize> {
    let body: Value = client
        .get(format!("{base}/panel/api/requests/active"))
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .json()
        .await?;
    Ok(body["active"].as_array().map_or(0, Vec::len))
}

/// Count stub `request #N` log lines (each is one upstream send).
fn stub_request_count(stub_stderr: &Path) -> u64 {
    std::fs::read_to_string(stub_stderr).map_or(0, |text| text.matches("request #").count() as u64)
}

/// Cancellation integration: `rounds` x `width` SSE requests against the
/// slow stub; abort each round after first content and measure per-request
/// permit-release latency from the active-request decrements. Also proves
/// the stub sees no sends after the drain is observed.
// One sequential stress phase; splitting would scatter the timeline.
#[allow(clippy::too_many_lines)]
async fn cancellation_phase(
    evidence: &Path,
    rust_daemon: &Path,
    stub_bin: &Path,
    api_key: &str,
) -> anyhow::Result<Value> {
    let work = evidence.join("cancellation");
    std::fs::create_dir_all(&work)?;
    let stack = bench::launch(
        &work,
        "cancel",
        rust_daemon,
        stub_bin,
        &SLOW_SHAPE,
        true,
        api_key,
    )
    .await?;
    let client = reqwest::Client::new();
    let rounds = 8;
    let width = 8;
    let mut latencies: Vec<f64> = Vec::new();
    for round in 0..rounds {
        let base = stack.base.clone();
        let key = api_key.to_string();
        let mut aborts = Vec::new();
        let mut spawned = Vec::new();
        for _ in 0..width {
            let client = client.clone();
            let base = base.clone();
            let key = key.clone();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            aborts.push(rx);
            spawned.push(tokio::spawn(async move {
                let record = bench::http_request(&client, &base, &key, Protocol::ChatSse).await;
                let _ = tx.send(());
                record
            }));
        }
        // Wait until all `width` requests are active (event signal), bounded.
        let wait_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let active = active_count(&client, &stack.base).await.unwrap_or(0);
            if active >= width || Instant::now() >= wait_deadline {
                anyhow::ensure!(
                    active >= width,
                    "round {round}: only {active}/{width} requests became active"
                );
                break;
            }
            tokio::time::sleep(CANCEL_POLL).await;
        }
        // The stub emits the first delta immediately (ttfb 0), so active
        // streams are already delivering content; abort now and measure
        // permit release.
        for handle in &spawned {
            handle.abort();
        }
        let abort_at = Instant::now();
        for rx in aborts {
            let _ = tokio::time::timeout(Duration::from_secs(5), rx).await;
        }
        // Poll the active count; each decrement is one released permit.
        let mut last_count = width;
        let drain_deadline = Instant::now() + CANCEL_DRAIN_TIMEOUT;
        loop {
            let active = active_count(&client, &stack.base)
                .await
                .unwrap_or(last_count);
            if active < last_count {
                let latency = ms_since(abort_at);
                for _ in active..last_count {
                    latencies.push(latency);
                }
                last_count = active;
            }
            if active == 0 || Instant::now() >= drain_deadline {
                anyhow::ensure!(
                    active == 0,
                    "round {round}: {active} permits still held {CANCEL_DRAIN_TIMEOUT:?} after abort"
                );
                break;
            }
            tokio::time::sleep(CANCEL_POLL).await;
        }
    }
    latencies.sort_by(f64::total_cmp);
    let p99 = if latencies.is_empty() {
        f64::NAN
    } else {
        // f64 percentile rank mirrors the Go bench math.
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let rank = (0.99 * latencies.len() as f64).ceil() as usize;
        latencies[rank.saturating_sub(1).min(latencies.len() - 1)]
    };
    // No send after observed cancellation: the stub's request count must be
    // stable once the drain is observed (allow one slow interval settle).
    // settle window after the drain is observed: any send after this is a
    // send after observed cancellation.
    let count_at_drain = stub_request_count(&stack.stub_stderr);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let count_after_settle = stub_request_count(&stack.stub_stderr);
    let sends_after_cancel = count_after_settle - count_at_drain;
    stack.stop().await;
    Ok(json!({
        "rounds": rounds,
        "width": width,
        "cancelled_requests": latencies.len(),
        "cancel_latency_p99_ms": p99,
        "cancel_latency_max_ms": latencies.last(),
        "gate_p99_ms": CANCEL_P99_LIMIT_MS,
        "passed": p99 < CANCEL_P99_LIMIT_MS && sends_after_cancel == 0,
        "stub_sends_after_observed_drain": sends_after_cancel,
    }))
}

fn ms_since(instant: Instant) -> f64 {
    instant.elapsed().as_secs_f64() * 1000.0
}

/// `qa stress` entry point.
// One sequential stress driver; splitting scatters the leg/gate flow.
// u64→f64 mirrors the Go bench math; counters stay far below 2^53.
#[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
pub async fn run(evidence: &Path, requests: u64) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    anyhow::ensure!(requests > 0, "--requests must be positive");
    let api_key = "qa-stress-key";
    let go_root = super::oracle::GoOracle::default_go_root();
    let binaries = bench::resolve_binaries(&go_root, evidence)?;

    let work = evidence.join("throughput");
    std::fs::create_dir_all(&work)?;
    let stack = bench::launch(
        &work,
        "stress",
        &binaries.rust_daemon,
        &binaries.go_stub,
        &THROUGHPUT_SHAPE,
        true,
        api_key,
    )
    .await?;
    let client = reqwest::Client::new();

    // Split across SSE/JSON/WS: 40/30/30.
    let sse_target = requests * 40 / 100;
    let json_target = requests * 30 / 100;
    let ws_target = requests - sse_target - json_target;

    let started = Instant::now();
    let sse = http_leg(&stack.base, api_key, Protocol::ChatSse, 64, sse_target).await;
    let json_leg = http_leg(&stack.base, api_key, Protocol::ChatJson, 64, json_target).await;
    let ws = ws_leg(&stack.base, api_key, 16, ws_target).await;
    let elapsed = started.elapsed();

    // Leaked permits: active requests must drain to zero (bounded wait).
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    let active_after = loop {
        let active = active_count(&client, &stack.base)
            .await
            .unwrap_or(usize::MAX);
        if active == 0 || Instant::now() >= drain_deadline {
            break active;
        }
        tokio::time::sleep(CANCEL_POLL).await;
    };

    let cancellation =
        cancellation_phase(evidence, &binaries.rust_daemon, &binaries.go_stub, api_key).await?;

    // Clean shutdown: SIGTERM must end the daemon within the drain grace.
    let shutdown = stack.daemon;
    let stub = stack.stub;
    let mut daemon = shutdown;
    let exit = daemon.shutdown(Duration::from_secs(30)).await?;
    let clean_exit = !exit.contains("killed");
    let mut stub = stub;
    let _ = stub.shutdown(Duration::from_secs(5)).await;

    let total_completed = sse.completed + json_leg.completed + ws.completed;
    let total_errors = sse.errors + json_leg.errors + ws.errors;
    let missing_terminal = sse.missing_terminal + json_leg.missing_terminal + ws.missing_terminal;
    let duplicate_terminal =
        sse.duplicate_terminal + json_leg.duplicate_terminal + ws.duplicate_terminal;

    let mut gates = Vec::new();
    let mut gate = |name: &str, passed: bool, detail: String| {
        gates.push(json!({"name": name, "passed": passed, "detail": detail}));
    };
    gate(
        "completed_requests",
        total_completed >= requests,
        format!("{total_completed}/{requests} completed"),
    );
    gate(
        "zero_unexpected_failures",
        total_errors == 0,
        format!(
            "errors={total_errors} sse={:?} json={:?} ws={:?}",
            sse.error_kinds, json_leg.error_kinds, ws.error_kinds
        ),
    );
    gate(
        "terminal_event_integrity",
        missing_terminal == 0 && duplicate_terminal == 0,
        format!("missing={missing_terminal} duplicate={duplicate_terminal}"),
    );
    gate(
        "no_leaked_permits",
        active_after == 0,
        format!("active requests after legs: {active_after}"),
    );
    gate(
        "cancellation_integration",
        cancellation["passed"].as_bool() == Some(true),
        format!(
            "p99={:.1}ms (limit {CANCEL_P99_LIMIT_MS}ms), stub sends after observed drain: {}",
            cancellation["cancel_latency_p99_ms"]
                .as_f64()
                .unwrap_or(f64::NAN),
            cancellation["stub_sends_after_observed_drain"]
        ),
    );
    gate("clean_shutdown", clean_exit, format!("daemon exit: {exit}"));

    let passed = gates.iter().all(|g| g["passed"].as_bool() == Some(true));
    let report = json!({
        "subcommand": "stress",
        "requested": requests,
        "completed": total_completed,
        "errors": total_errors,
        "missing_terminal": missing_terminal,
        "duplicate_terminal": duplicate_terminal,
        "elapsed_secs": elapsed.as_secs_f64(),
        "rps": total_completed as f64 / elapsed.as_secs_f64(),
        "legs": {
            "sse": {"completed": sse.completed, "errors": sse.errors},
            "json": {"completed": json_leg.completed, "errors": json_leg.errors},
            "ws": {"completed": ws.completed, "errors": ws.errors},
        },
        "cancellation": cancellation,
        "rust_daemon_sha256": binaries.rust_sha256,
        "go_stub_sha256": binaries.go_stub_sha256,
        "gates": gates,
        "passed": passed,
    });
    std::fs::write(
        evidence.join("stress.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    for gate in &gates {
        println!(
            "gate {}: {}",
            gate["name"].as_str().unwrap_or_default(),
            if gate["passed"].as_bool() == Some(true) {
                "PASS"
            } else {
                "FAIL"
            }
        );
    }
    println!(
        "qa stress: {} ({total_completed}/{requests} requests, {:.0} rps)",
        if passed { "PASS" } else { "FAIL" },
        total_completed as f64 / elapsed.as_secs_f64()
    );
    Ok(i32::from(!passed))
}

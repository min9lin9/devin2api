//! Task-24 failure scenario: the benchmark verdict must reject parity-only
//! results, error-filled fast runs and absent samples, and accept a real
//! improvement with all nonregression/resource gates green. Pure functions
//! over synthetic samples — deterministic, no timing.

use devin2api::qa::verdict::{self, Cell, Sample, evaluate};

// Fixture counts are small non-negative floats; the truncating cast is
// intentional (Go's uint64(f)).
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn sample(rps: f64, overhead_p99_ms: f64, rss: u64, errors: u64) -> Sample {
    Sample {
        requests: (rps * 30.0) as u64,
        errors,
        elapsed_secs: 30.0,
        rps,
        ttfb_p50_ms: 2.0,
        ttfb_p99_ms: 8.0,
        ttft_p50_ms: 2.5,
        ttft_p99_ms: 9.0,
        total_p50_ms: 40.0,
        total_p99_ms: 80.0,
        overhead_p99_ms,
        rss_peak_bytes: rss,
        cpu_ms_per_request: 1.0,
        bytes_per_request: 7000.0,
    }
}

/// The 8 primary cells (c=1/8/64/256 x debug off/on) with the given
/// per-side sample generator.
fn primary_matrix(go: &Sample, rust: &Sample) -> Vec<Cell> {
    let mut cells = Vec::new();
    for concurrency in [1u32, 8, 64, 256] {
        for debug in [false, true] {
            cells.push(Cell {
                name: format!(
                    "chat-sse-c{concurrency}-debug-{}",
                    if debug { "on" } else { "off" }
                ),
                primary: true,
                concurrency,
                debug_logging: debug,
                go: (0..verdict::PRIMARY_SAMPLES).map(|_| go.clone()).collect(),
                rust: (0..verdict::PRIMARY_SAMPLES)
                    .map(|_| rust.clone())
                    .collect(),
            });
        }
    }
    cells
}

fn gate(report: &verdict::Verdict, name: &str) -> bool {
    report
        .gates
        .iter()
        .find(|g| g.name == name)
        .unwrap_or_else(|| panic!("missing gate {name}"))
        .passed
}

#[test]
fn improvement_with_clean_samples_passes() {
    // Rust sustains 20% more throughput at equal overhead and RSS.
    let cells = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(620.0, 8.0, 100_000_000, 0),
    );
    let report = evaluate(&cells);
    assert!(
        report.passed,
        "gates: {:?}",
        report
            .gates
            .iter()
            .map(|g| (&g.name, g.passed, &g.detail))
            .collect::<Vec<_>>()
    );
    assert!(gate(&report, "samples_complete_and_clean"));
    assert!(gate(&report, "throughput_nonregression"));
    assert!(gate(&report, "local_overhead_nonregression"));
    assert!(gate(&report, "rss_ceiling"));
    assert!(gate(&report, "improvement_c64_both_logging_modes"));
}

#[test]
fn overhead_improvement_alone_passes() {
    // Throughput parity but overhead p99 cut by 20%: the alternative
    // improvement arm must satisfy the gate in both c=64 logging modes.
    let cells = primary_matrix(
        &sample(500.0, 10.0, 100_000_000, 0),
        &sample(500.0, 7.5, 100_000_000, 0),
    );
    let report = evaluate(&cells);
    assert!(gate(&report, "improvement_c64_both_logging_modes"));
    assert!(report.passed);
}

#[test]
fn no_gain_and_missing_samples_fail() {
    // Parity-only: identical performance satisfies nonregression but must
    // NOT pass — the rewrite may not be called faster on parity alone.
    let parity = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(500.0, 8.0, 100_000_000, 0),
    );
    let report = evaluate(&parity);
    assert!(!report.passed, "parity-only result must fail");
    assert!(gate(&report, "throughput_nonregression"));
    assert!(!gate(&report, "improvement_c64_both_logging_modes"));

    // Error-filled fast run: 3x throughput with errors must fail.
    let fast_dirty = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(1500.0, 4.0, 100_000_000, 25),
    );
    let report = evaluate(&fast_dirty);
    assert!(!report.passed, "error-filled fast run must fail");
    assert!(!gate(&report, "samples_complete_and_clean"));

    // Absent samples: a cell with 7 of 10 samples must fail even if the
    // recorded samples show a huge gain.
    let mut short = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(900.0, 4.0, 100_000_000, 0),
    );
    short[0].rust.truncate(7);
    let report = evaluate(&short);
    assert!(!report.passed, "absent samples must fail");
    assert!(!gate(&report, "samples_complete_and_clean"));

    // A missing primary cell entirely must fail.
    let mut missing = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(900.0, 4.0, 100_000_000, 0),
    );
    missing.pop();
    let report = evaluate(&missing);
    assert!(!report.passed, "a missing primary cell must fail");
    assert!(!gate(&report, "samples_complete_and_clean"));
}

#[test]
fn regressions_fail_nonregression_gates() {
    // Throughput regression beyond the 0.95 floor must fail gate 2.
    let cells = primary_matrix(
        &sample(500.0, 4.0, 100_000_000, 0),
        &sample(400.0, 4.0, 100_000_000, 0),
    );
    let report = evaluate(&cells);
    assert!(!report.passed);
    assert!(!gate(&report, "throughput_nonregression"));

    // Overhead blow-up beyond max(1.10x, +1ms) must fail gate 3.
    let cells = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(600.0, 20.0, 100_000_000, 0),
    );
    let report = evaluate(&cells);
    assert!(!report.passed);
    assert!(!gate(&report, "local_overhead_nonregression"));

    // RSS beyond 1.15x must fail gate 4.
    let cells = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(600.0, 8.0, 200_000_000, 0),
    );
    let report = evaluate(&cells);
    assert!(!report.passed);
    assert!(!gate(&report, "rss_ceiling"));

    // Improvement in only ONE c=64 logging mode must fail gate 5.
    let mut cells = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(500.0, 8.0, 100_000_000, 0),
    );
    for cell in &mut cells {
        if cell.concurrency == 64 && !cell.debug_logging {
            cell.rust = (0..verdict::PRIMARY_SAMPLES)
                .map(|_| sample(600.0, 8.0, 100_000_000, 0))
                .collect();
        }
    }
    let report = evaluate(&cells);
    assert!(!report.passed);
    assert!(!gate(&report, "improvement_c64_both_logging_modes"));
}

#[test]
fn bootstrap_is_deterministic() {
    let cells = primary_matrix(
        &sample(500.0, 8.0, 100_000_000, 0),
        &sample(620.0, 8.0, 100_000_000, 0),
    );
    let first = evaluate(&cells);
    let second = evaluate(&cells);
    let a = serde_json::to_string(&first.cells).unwrap();
    let b = serde_json::to_string(&second.cells).unwrap();
    assert_eq!(a, b, "seeded bootstrap must be reproducible");
}

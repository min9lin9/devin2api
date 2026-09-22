//! Task-24 paired-bootstrap verdict logic for the performance gate.
//!
//! Pure functions over recorded per-sample metrics: no IO, no timing, no
//! randomness outside the seeded `SplitMix64` below, so the verdict is fully
//! reproducible from the raw samples (seed 20260916, 10,000 resamples).
//!
//! Gates (plan, Verification strategy / performance gate):
//! - every primary cell has exactly 10 paired samples with zero errors and
//!   nonzero completed requests (absent samples / error-filled fast runs
//!   fail);
//! - nonregression: throughput ratio (Rust/Go) lower 95% CI >= 0.95 in all
//!   8 primary cells; local-overhead p99 upper 95% CI <= max(Go x1.10,
//!   Go+1ms); peak-RSS ratio upper 95% CI <= 1.15;
//! - improvement: in BOTH preselected c=64 logging cells, throughput lower
//!   95% CI >= 1.10 OR local-overhead p99 ratio upper 95% CI <= 0.90.

use serde::{Deserialize, Serialize};

/// Resamples for the paired bootstrap (plan-mandated).
pub const RESAMPLES: usize = 10_000;
/// Bootstrap / ABBA seed (plan-mandated).
pub const SEED: u64 = 20_260_916;
/// Paired samples per primary cell.
pub const PRIMARY_SAMPLES: usize = 10;
/// Paired samples per secondary cell (descriptive only).
pub const SECONDARY_SAMPLES: usize = 3;

const THROUGHPUT_FLOOR: f64 = 0.95;
const OVERHEAD_RATIO_CEIL: f64 = 1.10;
const OVERHEAD_DIFF_CEIL_MS: f64 = 1.0;
const RSS_RATIO_CEIL: f64 = 1.15;
const IMPROVE_THROUGHPUT: f64 = 1.10;
const IMPROVE_OVERHEAD_RATIO: f64 = 0.90;

/// One 30s (primary) or shorter (secondary) sample of one leg of one cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Completed requests without error.
    pub requests: u64,
    /// Requests that failed (any non-200 / truncation / missing terminal).
    pub errors: u64,
    /// Wall-clock sampling window in seconds.
    pub elapsed_secs: f64,
    /// Completed requests per second.
    pub rps: f64,
    /// Wire TTFB percentiles (first body byte), ms.
    pub ttfb_p50_ms: f64,
    pub ttfb_p99_ms: f64,
    /// Semantic TTFT percentiles (first renderable content), ms.
    pub ttft_p50_ms: f64,
    pub ttft_p99_ms: f64,
    /// End-to-end latency percentiles, ms.
    pub total_p50_ms: f64,
    pub total_p99_ms: f64,
    /// Local-overhead p99, ms: debuglog decode+transform+egress when debug
    /// logging is on; end-to-end latency under the zero-delay loopback stub
    /// when debug logging is off (proxy, labelled in the report).
    pub overhead_p99_ms: f64,
    /// Peak resident set of the daemon during the sample, bytes.
    pub rss_peak_bytes: u64,
    /// Daemon CPU time per completed request, ms.
    pub cpu_ms_per_request: f64,
    /// Response bytes per completed request.
    pub bytes_per_request: f64,
}

/// One benchmark cell: a fixed protocol/stream-shape/concurrency/debug
/// combination with paired Go and Rust samples.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub name: String,
    pub primary: bool,
    pub concurrency: u32,
    pub debug_logging: bool,
    pub go: Vec<Sample>,
    pub rust: Vec<Sample>,
}

/// Two-sided 95% bootstrap CI for a paired statistic.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ci {
    pub lower: f64,
    pub upper: f64,
    pub point: f64,
}

/// Per-cell computed statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellStats {
    pub cell: String,
    pub throughput_ratio: Ci,
    pub overhead_ratio: Ci,
    pub overhead_diff_ms: Ci,
    pub rss_ratio: Ci,
}

/// One evaluated gate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateResult {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

/// Full verdict: per-cell stats plus gate outcomes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub cells: Vec<CellStats>,
    pub gates: Vec<GateResult>,
    pub passed: bool,
}

/// Deterministic `SplitMix64` — the bootstrap RNG. Seeded once with SEED;
/// identical output on every host and rand version.
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform index in `0..n` (n > 0).
    pub fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next_u64() % n as u64).unwrap_or_default()
    }
}

fn percentile(sorted: &mut [f64], q: f64) -> f64 {
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return f64::NAN;
    }
    // Percentile rank arithmetic is float like Go's; counts are tiny.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let rank = (q / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

/// Generic paired bootstrap over a statistic of (`go_resample`, `rust_resample`).
fn bootstrap(go: &[f64], rust: &[f64], seed: u64, statistic: impl Fn(&[f64], &[f64]) -> f64) -> Ci {
    let n = go.len().min(rust.len());
    if n == 0 {
        return Ci {
            lower: f64::NAN,
            upper: f64::NAN,
            point: f64::NAN,
        };
    }
    let go = &go[..n];
    let rust = &rust[..n];
    let point = statistic(go, rust);
    let mut rng = SplitMix64::new(seed);
    let mut stats = Vec::with_capacity(RESAMPLES);
    let mut gs = Vec::with_capacity(n);
    let mut rs = Vec::with_capacity(n);
    for _ in 0..RESAMPLES {
        gs.clear();
        rs.clear();
        for _ in 0..n {
            let i = rng.below(n);
            gs.push(go[i]);
            rs.push(rust[i]);
        }
        stats.push(statistic(&gs, &rs));
    }
    Ci {
        lower: percentile(&mut stats.clone(), 2.5),
        upper: percentile(&mut stats, 97.5),
        point,
    }
}

fn mean(values: &[f64]) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let len = values.len() as f64;
    values.iter().sum::<f64>() / len
}

fn ratio_statistic(go: &[f64], rust: &[f64]) -> f64 {
    let g = mean(go);
    if g == 0.0 {
        return f64::NAN;
    }
    mean(rust) / g
}

fn diff_statistic(go: &[f64], rust: &[f64]) -> f64 {
    mean(rust) - mean(go)
}

/// Compute the paired-bootstrap statistics for one cell.
// `go_rss`/`rust_rss` are the paired-leg names; u64→f64 mirrors the
// Go bench math (RSS stays far below 2^53 bytes).
#[allow(clippy::similar_names, clippy::cast_precision_loss)]
pub fn cell_stats(cell: &Cell) -> CellStats {
    let go_rps: Vec<f64> = cell.go.iter().map(|s| s.rps).collect();
    let rust_rps: Vec<f64> = cell.rust.iter().map(|s| s.rps).collect();
    let go_oh: Vec<f64> = cell.go.iter().map(|s| s.overhead_p99_ms).collect();
    let rust_oh: Vec<f64> = cell.rust.iter().map(|s| s.overhead_p99_ms).collect();
    let go_rss: Vec<f64> = cell.go.iter().map(|s| s.rss_peak_bytes as f64).collect();
    let rust_rss: Vec<f64> = cell.rust.iter().map(|s| s.rss_peak_bytes as f64).collect();
    CellStats {
        cell: cell.name.clone(),
        throughput_ratio: bootstrap(&go_rps, &rust_rps, SEED, ratio_statistic),
        overhead_ratio: bootstrap(&go_oh, &rust_oh, SEED, ratio_statistic),
        overhead_diff_ms: bootstrap(&go_oh, &rust_oh, SEED, diff_statistic),
        rss_ratio: bootstrap(&go_rss, &rust_rss, SEED, ratio_statistic),
    }
}

fn pass_gate(name: &str, passed: bool, detail: String) -> GateResult {
    GateResult {
        name: name.to_string(),
        passed,
        detail,
    }
}

/// Evaluate all published gates against the recorded cells. Any primary
/// cell that is absent, short on samples, or contains errors fails the
/// whole verdict — parity-only results (no improvement) also fail.
// One gate table; splitting scatters the acceptance contract.
#[allow(clippy::too_many_lines)]
pub fn evaluate(cells: &[Cell]) -> Verdict {
    let mut gates = Vec::new();
    let primary: Vec<&Cell> = cells.iter().filter(|c| c.primary).collect();

    // Gate 1: sample completeness and cleanliness.
    let mut problems = Vec::new();
    for cell in &primary {
        if cell.go.len() != PRIMARY_SAMPLES || cell.rust.len() != PRIMARY_SAMPLES {
            problems.push(format!(
                "{}: samples go={} rust={} (want {PRIMARY_SAMPLES} each)",
                cell.name,
                cell.go.len(),
                cell.rust.len()
            ));
        }
        for (side, samples) in [("go", &cell.go), ("rust", &cell.rust)] {
            for (i, s) in samples.iter().enumerate() {
                if s.errors > 0 {
                    problems.push(format!("{}:{side}[{i}]: {} errors", cell.name, s.errors));
                }
                if s.requests == 0 {
                    problems.push(format!(
                        "{}:{side}[{i}]: zero completed requests",
                        cell.name
                    ));
                }
            }
        }
    }
    let expected_primary = 8;
    if primary.len() != expected_primary {
        problems.push(format!(
            "primary cells: {} present, want {expected_primary}",
            primary.len()
        ));
    }
    gates.push(pass_gate(
        "samples_complete_and_clean",
        problems.is_empty(),
        if problems.is_empty() {
            format!(
                "{} primary cells x {PRIMARY_SAMPLES} paired samples, zero errors",
                primary.len()
            )
        } else {
            problems.join("; ")
        },
    ));

    let stats: Vec<CellStats> = cells.iter().map(cell_stats).collect();
    let primary_stats: Vec<&CellStats> = stats
        .iter()
        .filter(|s| primary.iter().any(|c| c.name == s.cell))
        .collect();

    // Gate 2: throughput nonregression in every primary cell.
    let worst = primary_stats
        .iter()
        .map(|s| (s.throughput_ratio.lower, s.cell.as_str()))
        .fold(
            (f64::INFINITY, ""),
            |acc, x| {
                if x.0 < acc.0 { x } else { acc }
            },
        );
    let ok = !primary_stats.is_empty()
        && primary_stats
            .iter()
            .all(|s| s.throughput_ratio.lower >= THROUGHPUT_FLOOR);
    gates.push(pass_gate(
        "throughput_nonregression",
        ok,
        format!(
            "worst lower-95% CI ratio {:.4} in {} (floor {THROUGHPUT_FLOOR})",
            worst.0, worst.1
        ),
    ));

    // Gate 3: local-overhead nonregression in every primary cell.
    let ok = !primary_stats.is_empty()
        && primary_stats.iter().all(|s| {
            s.overhead_ratio.upper <= OVERHEAD_RATIO_CEIL
                || s.overhead_diff_ms.upper <= OVERHEAD_DIFF_CEIL_MS
        });
    let detail = primary_stats
        .iter()
        .map(|s| {
            format!(
                "{} ratio_hi={:.3} diff_hi={:.3}ms",
                s.cell, s.overhead_ratio.upper, s.overhead_diff_ms.upper
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    gates.push(pass_gate(
        "local_overhead_nonregression",
        ok,
        format!("ceil ratio {OVERHEAD_RATIO_CEIL} or diff {OVERHEAD_DIFF_CEIL_MS}ms: {detail}"),
    ));

    // Gate 4: RSS ceiling at matched throughput.
    let worst_rss = primary_stats
        .iter()
        .map(|s| (s.rss_ratio.upper, s.cell.as_str()))
        .fold((0.0_f64, ""), |acc, x| if x.0 > acc.0 { x } else { acc });
    let ok = !primary_stats.is_empty()
        && primary_stats
            .iter()
            .all(|s| s.rss_ratio.upper <= RSS_RATIO_CEIL);
    gates.push(pass_gate(
        "rss_ceiling",
        ok,
        format!(
            "worst upper-95% CI RSS ratio {:.4} in {} (ceil {RSS_RATIO_CEIL})",
            worst_rss.0, worst_rss.1
        ),
    ));

    // Gate 5: improvement in BOTH preselected c=64 logging cells.
    let c64: Vec<&&Cell> = primary.iter().filter(|c| c.concurrency == 64).collect();
    let mut improve_detail = Vec::new();
    let mut improve_ok = c64.len() == 2;
    for cell in c64 {
        let s = stats.iter().find(|s| s.cell == cell.name);
        let Some(s) = s else {
            improve_ok = false;
            continue;
        };
        let throughput_win = s.throughput_ratio.lower >= IMPROVE_THROUGHPUT;
        let overhead_win = s.overhead_ratio.upper <= IMPROVE_OVERHEAD_RATIO;
        improve_ok &= throughput_win || overhead_win;
        improve_detail.push(format!(
            "{}: thr_lo={:.3} oh_hi={:.3} throughput_win={throughput_win} overhead_win={overhead_win}",
            cell.name, s.throughput_ratio.lower, s.overhead_ratio.upper
        ));
    }
    gates.push(pass_gate(
        "improvement_c64_both_logging_modes",
        improve_ok,
        improve_detail.join("; "),
    ));

    let passed = gates.iter().all(|g| g.passed);
    Verdict {
        cells: stats,
        gates,
        passed,
    }
}

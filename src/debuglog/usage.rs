//! In-memory aggregation of `index.jsonl` summaries for the panel — port of
//! `G/internal/debuglog/usage.go`.
//!
//! - startup replays the index tail (bounded by `USAGE_REPLAY_TAIL_BYTES`)
//!   so restarts keep history;
//! - `add` runs inside `append_index` — one mutex-guarded counter update on
//!   the hot path;
//! - latency uses fixed-size reservoirs (last `USAGE_SAMPLE_CAPACITY`
//!   completed requests) for p50/p95/p99;
//! - day aggregation is local-time; 10-minute buckets keep the last
//!   `USAGE_MIN_BUCKETS` (8 days).

use std::collections::BTreeMap;
use std::sync::Mutex;

use jiff::Zoned;

use super::gojson::{JVal, ObjWriter};
use super::gotime;
use super::index::IndexEntry;

/// `usageSampleCapacity` — latency reservoir size (most recent N completed
/// requests).
pub const USAGE_SAMPLE_CAPACITY: usize = 4096;
/// `usageMinBuckets` — number of retained 10-minute trend buckets (8 days).
pub const USAGE_MIN_BUCKETS: usize = 6 * 24 * 8;
/// `usageMaxDays` — cap on emitted day rows.
pub const USAGE_MAX_DAYS: usize = 31;
/// `usageMinSampleCap` — per-bucket latency sample cap (ring overwrite).
const USAGE_MIN_SAMPLE_CAP: usize = 256;
/// `rateLimitEventCap` — retained 429 samples.
const RATE_LIMIT_EVENT_CAP: usize = 256;
/// `startsCap` — start-timestamp window compaction threshold.
const STARTS_CAP: usize = 4096;
/// `maxPlausibleDecodeTPS` — upper bound of a believable per-request decode
/// rate; burst-delivered final frames produce measurement artifacts, not
/// real speed.
const MAX_PLAUSIBLE_DECODE_TPS: i64 = 400;

/// `usageTotals` — base counters for a group of requests. Field order
/// matches the Go struct for JSON emission.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UsageTotals {
    pub requests: i64,
    /// status>=400 or result=failed.
    pub errors: i64,
    /// Includes aborted.
    pub disconnected: i64,
    /// Upstream 429 count (local concurrency rejects never reach the index).
    pub rate_limited: i64,
    /// Caller-fault attribution (disconnects, request-body stage failures).
    pub client_faults: i64,
    /// Server-fault attribution (upstream errors, proxy failures) — the only
    /// SLA-losing class.
    pub upstream_faults: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_write_tokens: i64,
    pub reasoning_tokens: i64,
    pub total_tokens: i64,
    /// Decode-rate denominator/numerator over trustworthy streaming entries
    /// (`decode_window`); `omitempty` on the wire.
    pub gen_ms: i64,
    pub gen_tokens: i64,
}

impl UsageTotals {
    /// `usageTotals.add` — fold one index line into the counters.
    fn add(&mut self, e: &IndexEntry) {
        self.requests += 1;
        // Disconnects/abort classify by result first: a pre-commit
        // disconnect's error status (500/499) is a "never written"
        // placeholder, not a server fault.
        if e.result == "disconnected" || e.result == "aborted" {
            self.disconnected += 1;
        } else if e.status_code >= 400 || e.result == "failed" {
            self.errors += 1;
        }
        match error_owner(e) {
            "client" => self.client_faults += 1,
            "upstream" => self.upstream_faults += 1,
            _ => {}
        }
        if is_rate_limited(e) {
            self.rate_limited += 1;
        }
        self.input_tokens += e.input_tokens;
        self.output_tokens += e.output_tokens;
        self.cache_read_tokens += e.cache_read_tokens;
        self.cache_write_tokens += e.cache_write_tokens;
        self.reasoning_tokens += e.reasoning_tokens;
        self.total_tokens += e.total_tokens;
        // Only trustworthy streaming entries: failed/disconnected durations
        // carry non-generation components and burst delivery fakes the rate.
        if let Some((out, gen_ms)) = decode_window(e) {
            self.gen_ms += gen_ms;
            self.gen_tokens += out;
        }
    }

    /// Emit in Go struct field order (`omitempty` only on `gen_ms/gen_tokens`).
    fn write_to(&self, w: &mut ObjWriter) {
        w.field_int("requests", self.requests)
            .field_int("errors", self.errors)
            .field_int("disconnected", self.disconnected)
            .field_int("rate_limited", self.rate_limited)
            .field_int("client_faults", self.client_faults)
            .field_int("upstream_faults", self.upstream_faults)
            .field_int("input_tokens", self.input_tokens)
            .field_int("output_tokens", self.output_tokens)
            .field_int("cache_read_tokens", self.cache_read_tokens)
            .field_int("cache_write_tokens", self.cache_write_tokens)
            .field_int("reasoning_tokens", self.reasoning_tokens)
            .field_int("total_tokens", self.total_tokens)
            .field_int_nonzero("gen_ms", self.gen_ms)
            .field_int_nonzero("gen_tokens", self.gen_tokens);
    }

    /// Totals as a `JVal` object (for map-valued fields like `model_days`).
    fn to_jval(&self) -> JVal {
        let mut w = ObjWriter::new();
        self.write_to(&mut w);
        JVal::Raw(w.finish().unwrap_or_else(|_| b"{}".to_vec()).into())
    }
}

/// `decodeWindow` — (output tokens, generation ms) for an entry, or `None`
/// when the entry has no first-frame timestamp, no output, or an implausible
/// surface rate (burst delivery).
fn decode_window(e: &IndexEntry) -> Option<(i64, i64)> {
    if e.result != "completed" || e.output_tokens <= 0 {
        return None;
    }
    let first = e.first_upstream_ms?;
    let gen_ms = (e.duration_ms - first).max(0);
    if gen_ms <= 0 || e.output_tokens * 1000 > MAX_PLAUSIBLE_DECODE_TPS * gen_ms {
        return None;
    }
    Some((e.output_tokens, gen_ms))
}

/// `isRateLimited` — whether the index line ended in rate-limit semantics:
/// HTTP 429 (upstream reject or local gate fast-fail) or a 200 with an
/// in-stream limit error (only the `rate_limited` flag sees that).
pub fn is_rate_limited(e: &IndexEntry) -> bool {
    e.status_code == 429 || e.rate_limited
}

/// `ErrorOwner` — failure attribution for one index line:
/// - `"client"`: client disconnect/panel abort, or request-body stage
///   failures — upstream was never reached;
/// - `"business_limited"`: 429s — quota actions are not service faults and
///   leave the SLA denominator;
/// - `"upstream"`: everything else failed — the only SLA-losing class;
/// - `""`: non-failure.
///
/// Uses only index fields (`result/status/error_stage/rate_limited`) so old
/// replayed lines classify too — pre-flag lines count in-stream limits as
/// upstream.
pub fn error_owner(e: &IndexEntry) -> &'static str {
    if is_rate_limited(e) {
        return "business_limited";
    }
    if e.result == "disconnected" || e.result == "aborted" {
        return "client";
    }
    if e.status_code < 400 && e.result != "failed" {
        return "";
    }
    if e.error_stage == super::stages::ERR_STAGE_HTTP_READ
        || e.error_stage == super::stages::ERR_STAGE_HTTP_DECODE
    {
        return "client";
    }
    "upstream"
}

/// `usageMinBucket` — one 10-minute window of request/token aggregates for
/// the trend chart, plus bounded latency sample rings.
#[derive(Clone, Default)]
struct UsageMinBucket {
    /// Bucket start unix second (600-aligned); 0 = empty slot.
    at: i64,
    totals: UsageTotals,
    /// `duration_ms` samples (ring, capped).
    durs: Vec<i64>,
    dur_head: usize,
    /// `first_upstream_ms` samples.
    ttfbs: Vec<i64>,
    ttfb_head: usize,
}

/// `pushSample` — append to a capacity-bounded sample vec; overwrite the
/// oldest once full.
fn push_sample(samples: &mut Vec<i64>, head: &mut usize, v: i64) {
    if samples.len() < USAGE_MIN_SAMPLE_CAP {
        samples.push(v);
        return;
    }
    samples[*head] = v;
    *head = (*head + 1) % USAGE_MIN_SAMPLE_CAP;
}

/// `usageMinPoint` — one 10-minute data point for the panel: the bucket's
/// totals plus per-bucket latency summaries.
#[derive(Debug, Clone, Default)]
pub struct UsageMinPoint {
    /// Bucket start unix second.
    pub at: i64,
    pub totals: UsageTotals,
    pub avg_duration_ms: i64,
    pub duration_p95_ms: i64,
    pub avg_ttfb_ms: i64,
    pub ttfb_p95_ms: i64,
}

impl UsageMinPoint {
    fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_int("at", self.at);
        self.totals.write_to(&mut w);
        w.field_int("avg_duration_ms", self.avg_duration_ms)
            .field_int("duration_p95_ms", self.duration_p95_ms)
            .field_int("avg_ttfb_ms", self.avg_ttfb_ms)
            .field_int("ttfb_p95_ms", self.ttfb_p95_ms);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `dimensionAgg` — per-model or per-key-hash row: embedded totals plus
/// latency means, last-seen state, token quantiles and the SLA rate.
#[derive(Debug, Clone)]
pub struct DimensionAgg {
    pub name: String,
    pub totals: UsageTotals,
    sum_duration: i64,
    ttfb_samples: i64,
    sum_ttfb: i64,
    pub last_result: String,
    pub last_status: i64,
    pub last_at: String,
    pub avg_duration_ms: f64,
    pub avg_ttfb_ms: f64,
    pub success_rate: f64,
    /// Server-side success rate: client-fault and 429 entries leave the
    /// denominator; upstream-fault share of the rest is inverted.
    pub sla_success_rate: f64,
    pub input_p50: i64,
    pub input_p95: i64,
    pub output_p50: i64,
    pub output_p95: i64,
    in_tok_samples: SampleRing,
    out_tok_samples: SampleRing,
}

/// `dimensionSampleCapacity` — per-dimension token sample reservoir.
const DIMENSION_SAMPLE_CAPACITY: usize = 2048;

impl DimensionAgg {
    fn new(name: String) -> Self {
        Self {
            name,
            totals: UsageTotals::default(),
            sum_duration: 0,
            ttfb_samples: 0,
            sum_ttfb: 0,
            last_result: String::new(),
            last_status: 0,
            last_at: String::new(),
            avg_duration_ms: 0.0,
            avg_ttfb_ms: 0.0,
            success_rate: 0.0,
            sla_success_rate: 0.0,
            input_p50: 0,
            input_p95: 0,
            output_p50: 0,
            output_p95: 0,
            in_tok_samples: SampleRing::new(DIMENSION_SAMPLE_CAPACITY),
            out_tok_samples: SampleRing::new(DIMENSION_SAMPLE_CAPACITY),
        }
    }

    /// `dimensionAgg.addEntry` — totals plus dimension-specific latency
    /// sums, token reservoirs and last-seen snapshot.
    fn add_entry(&mut self, e: &IndexEntry) {
        self.totals.add(e);
        self.sum_duration += e.duration_ms;
        if let Some(first) = e.first_upstream_ms {
            self.ttfb_samples += 1;
            self.sum_ttfb += first;
        }
        self.in_tok_samples.push(e.input_tokens);
        if e.output_tokens > 0 {
            self.out_tok_samples.push(e.output_tokens);
        }
        self.last_result.clone_from(&e.result);
        self.last_status = e.status_code;
        self.last_at.clone_from(&e.started_at);
    }

    /// `dimensionAgg.finish` — derive means and success rates at snapshot
    /// time.
    // i64→f64 mirrors Go's float64 conversions; counts stay far below
    // 2^53 so the precision loss is theoretical.
    #[allow(clippy::cast_precision_loss)]
    fn finish(&mut self) {
        if self.totals.requests > 0 {
            self.avg_duration_ms = self.sum_duration as f64 / self.totals.requests as f64;
        }
        if self.ttfb_samples > 0 {
            self.avg_ttfb_ms = self.sum_ttfb as f64 / self.ttfb_samples as f64;
        }
        if self.totals.requests > 0 {
            self.success_rate =
                (self.totals.requests - self.totals.errors - self.totals.disconnected) as f64
                    / self.totals.requests as f64;
        }
        // SLA view: only "within the service promise" requests stay in the
        // denominator — client faults and rate limits are removed wholesale.
        let slable = self.totals.requests - self.totals.client_faults - self.totals.rate_limited;
        if slable > 0 {
            self.sla_success_rate = (slable - self.totals.upstream_faults) as f64 / slable as f64;
        }
        let in_stats = self.in_tok_samples.stats();
        if in_stats.samples > 0 {
            self.input_p50 = in_stats.p50;
            self.input_p95 = in_stats.p95;
        }
        let out_stats = self.out_tok_samples.stats();
        if out_stats.samples > 0 {
            self.output_p50 = out_stats.p50;
            self.output_p95 = out_stats.p95;
        }
    }

    /// Marshal in Go `dimensionAgg` field order (embedded totals first).
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("name", &self.name);
        self.totals.write_to(&mut w);
        w.field_str_nonempty("last_result", &self.last_result)
            .field_int_nonzero("last_status", self.last_status)
            .field_str_nonempty("last_at", &self.last_at)
            .field_float("avg_duration_ms", self.avg_duration_ms)
            .field_float("avg_ttfb_ms", self.avg_ttfb_ms)
            .field_float("success_rate", self.success_rate)
            .field_float("sla_success_rate", self.sla_success_rate)
            .field_int_nonzero("input_p50", self.input_p50)
            .field_int_nonzero("input_p95", self.input_p95)
            .field_int_nonzero("output_p50", self.output_p50)
            .field_int_nonzero("output_p95", self.output_p95);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `latencyStats` — reservoir-derived latency distribution.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LatencyStats {
    pub samples: i64,
    pub p50: i64,
    pub p90: i64,
    pub p95: i64,
    pub p99: i64,
    pub avg: i64,
    pub max: i64,
}

impl LatencyStats {
    /// Marshal in Go field order.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_int("samples", self.samples)
            .field_int("p50", self.p50)
            .field_int("p90", self.p90)
            .field_int("p95", self.p95)
            .field_int("p99", self.p99)
            .field_int("avg", self.avg)
            .field_int("max", self.max);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `usageDayRow` — a single-day aggregate.
#[derive(Debug, Clone)]
pub struct UsageDayRow {
    pub date: String,
    pub totals: UsageTotals,
}

impl UsageDayRow {
    fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("date", &self.date);
        self.totals.write_to(&mut w);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `rateLimitEvent` — one 429 sample: when it happened (≈ completion time),
/// the model, and how many requests started in the 60s before it. `stage`
/// distinguishes the local gate fast-fail (`rate_gate`, RPM ≈ arrival rate)
/// from a real upstream 429 (RPM ≈ send rate seen by upstream).
#[derive(Debug, Clone)]
pub struct RateLimitEvent {
    /// Unix seconds.
    pub at: i64,
    pub model: String,
    /// Requests started within (at-60s, at].
    pub rpm: i64,
    pub stage: String,
}

impl RateLimitEvent {
    fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_int("at", self.at)
            .field_str_nonempty("model", &self.model)
            .field_int("rpm", self.rpm)
            .field_str_nonempty("stage", &self.stage);
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `UsageSnapshot` — the complete aggregation view.
#[derive(Debug, Clone, Default)]
pub struct UsageSnapshot {
    /// Earliest replayed entry's time (RFC3339).
    pub window_start: String,
    /// Index lines that fed the aggregation.
    pub entries: i64,
    pub today: UsageTotals,
    pub window: UsageTotals,
    /// Newest first.
    pub days: Vec<UsageDayRow>,
    /// Oldest to newest, 10-minute granularity, zero buckets included.
    pub points: Vec<UsageMinPoint>,
    pub models: Vec<DimensionAgg>,
    pub keys: Vec<DimensionAgg>,
    /// model × day totals matrix for the panel's range selector.
    pub model_days: BTreeMap<String, BTreeMap<String, UsageTotals>>,
    pub error_stages: BTreeMap<String, i64>,
    pub duration: LatencyStats,
    pub ttfb: LatencyStats,
    /// Recent upstream-429 samples, oldest to newest.
    pub rate_limit_events: Vec<RateLimitEvent>,
}

impl UsageSnapshot {
    /// `json.Marshal(snapshot)` in Go field order. `model_days` and
    /// `error_stages` are Go maps — their keys sort.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut w = ObjWriter::new();
        w.field_str("window_start", &self.window_start)
            .field_int("entries", self.entries);
        let mut inner = ObjWriter::new();
        self.today.write_to(&mut inner);
        w.field_raw("today", &inner.finish().unwrap_or_else(|_| b"{}".to_vec()));
        let mut inner = ObjWriter::new();
        self.window.write_to(&mut inner);
        w.field_raw("window", &inner.finish().unwrap_or_else(|_| b"{}".to_vec()));
        // Go marshals a nil slice as null.
        if self.days.is_empty() {
            w.field("days", &JVal::Null);
        } else {
            w.field(
                "days",
                &JVal::Arr(
                    self.days
                        .iter()
                        .map(|d| JVal::Raw(d.to_go_json().into()))
                        .collect(),
                ),
            );
        }
        w.field(
            "points",
            &JVal::Arr(
                self.points
                    .iter()
                    .map(|p| JVal::Raw(p.to_go_json().into()))
                    .collect(),
            ),
        );
        w.field(
            "models",
            &JVal::Arr(
                self.models
                    .iter()
                    .map(|m| JVal::Raw(m.to_go_json().into()))
                    .collect(),
            ),
        );
        w.field(
            "keys",
            &JVal::Arr(
                self.keys
                    .iter()
                    .map(|k| JVal::Raw(k.to_go_json().into()))
                    .collect(),
            ),
        );
        if !self.model_days.is_empty() {
            let outer: BTreeMap<String, JVal> = self
                .model_days
                .iter()
                .map(|(model, days)| {
                    let inner: BTreeMap<String, JVal> = days
                        .iter()
                        .map(|(day, totals)| (day.clone(), totals.to_jval()))
                        .collect();
                    (model.clone(), JVal::Obj(inner))
                })
                .collect();
            w.field("model_days", &JVal::Obj(outer));
        }
        let stages: BTreeMap<String, JVal> = self
            .error_stages
            .iter()
            .map(|(k, v)| (k.clone(), JVal::Int(*v)))
            .collect();
        w.field("error_stages", &JVal::Obj(stages));
        w.field_raw("duration", &self.duration.to_go_json());
        w.field_raw("ttfb", &self.ttfb.to_go_json());
        if !self.rate_limit_events.is_empty() {
            w.field(
                "rate_limit_events",
                &JVal::Arr(
                    self.rate_limit_events
                        .iter()
                        .map(|e| JVal::Raw(e.to_go_json().into()))
                        .collect(),
                ),
            );
        }
        w.finish().unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `sampleRing` — fixed-size latency reservoir; writes wrap over the oldest
/// sample once full.
#[derive(Debug, Clone)]
struct SampleRing {
    vals: Vec<i64>,
    head: usize,
    size: usize,
}

impl SampleRing {
    fn new(capacity: usize) -> Self {
        Self {
            vals: vec![0; capacity],
            head: 0,
            size: 0,
        }
    }

    fn push(&mut self, v: i64) {
        self.vals[self.head] = v;
        self.head = (self.head + 1) % self.vals.len();
        if self.size < self.vals.len() {
            self.size += 1;
        }
    }

    /// Quantile summary; zero value when empty.
    // f64 quantile index math mirrors Go; the ring is 4096 entries.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    fn stats(&self) -> LatencyStats {
        if self.size == 0 {
            return LatencyStats::default();
        }
        let mut sorted = self.vals[..self.size].to_vec();
        sorted.sort_unstable();
        let sum: i64 = sorted.iter().sum();
        let pick = |q: f64| -> i64 {
            let idx = (q * (sorted.len() - 1) as f64) as usize;
            sorted[idx]
        };
        LatencyStats {
            samples: super::to_i64(self.size),
            p50: pick(0.50),
            p90: pick(0.90),
            p95: pick(0.95),
            p99: pick(0.99),
            avg: sum / super::to_i64(sorted.len()),
            max: sorted[sorted.len() - 1],
        }
    }
}

/// `usageAggregator` — all aggregation state; `add` runs inside
/// `append_index`, snapshots serve the panel.
pub struct UsageAggregator {
    state: Mutex<AggState>,
}

struct AggState {
    window_start: Option<Zoned>,
    entries: i64,
    total: UsageTotals,
    days: BTreeMap<String, UsageTotals>,
    mins: Vec<UsageMinBucket>,
    /// Reservoirs over `first_upstream_ms` and `duration_ms`.
    ttfb_samples: SampleRing,
    duration_samples: SampleRing,
    per_model: BTreeMap<String, DimensionAgg>,
    per_model_day: BTreeMap<String, BTreeMap<String, UsageTotals>>,
    per_key: BTreeMap<String, DimensionAgg>,
    err_stages: BTreeMap<String, i64>,
    /// Recent request start timestamps (seconds, appended in completion
    /// order — approximately chronological) for the 60s lookback when a 429
    /// lands. In-flight requests are not indexed yet, so the rate reads low.
    starts: Vec<i64>,
    rl_events: Vec<RateLimitEvent>,
}

impl Default for UsageAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageAggregator {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(AggState {
                window_start: None,
                entries: 0,
                total: UsageTotals::default(),
                days: BTreeMap::new(),
                mins: vec![UsageMinBucket::default(); USAGE_MIN_BUCKETS],
                ttfb_samples: SampleRing::new(USAGE_SAMPLE_CAPACITY),
                duration_samples: SampleRing::new(USAGE_SAMPLE_CAPACITY),
                per_model: BTreeMap::new(),
                per_model_day: BTreeMap::new(),
                per_key: BTreeMap::new(),
                err_stages: BTreeMap::new(),
                starts: Vec::new(),
                rl_events: Vec::new(),
            }),
        }
    }

    /// `add` — fold one completed request's index line into every dimension.
    pub fn add(&self, e: &IndexEntry) {
        let started = gotime::parse_rfc3339(&e.started_at).unwrap_or_else(gotime::now);
        // Go `started.Local()`: day buckets use the system local zone, not
        // the entry's own offset.
        let started_local = started.timestamp().to_zoned(jiff::tz::TimeZone::system());
        let day = gotime::day_key(&started_local);
        let slot = gotime::unix(&started) / 600;

        let mut a = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if a.entries == 0
            || started.timestamp()
                < a.window_start
                    .as_ref()
                    .map_or_else(|| jiff::Timestamp::MAX, jiff::Zoned::timestamp)
        {
            a.window_start = Some(started.clone());
        }
        a.entries += 1;
        a.total.add(e);
        a.days.entry(day.clone()).or_default().add(e);

        // Ring slots wrap after USAGE_MIN_BUCKETS (8 days): a replayed old
        // entry can collide with the current bucket's slot — resetting would
        // erase counted data. On collision only the newer slot's bucket is
        // kept (the older entry's bucket update is dropped; totals/days/
        // dimensions still accumulate); a newer slot resets the stale bucket.
        // A negative slot (pre-1970 timestamp) cannot index the ring — Go
        // would panic; the port drops the bucket update instead.
        if slot >= 0 {
            let idx =
                usize::try_from(slot % i64::try_from(USAGE_MIN_BUCKETS).unwrap_or(1)).unwrap_or(0);
            if a.mins[idx].at < slot * 600 {
                a.mins[idx] = UsageMinBucket {
                    at: slot * 600,
                    ..UsageMinBucket::default()
                };
            }
            if a.mins[idx].at == slot * 600 {
                a.mins[idx].totals.add(e);
                let bucket = &mut a.mins[idx];
                push_sample(&mut bucket.durs, &mut bucket.dur_head, e.duration_ms);
                if let Some(first) = e.first_upstream_ms {
                    push_sample(&mut bucket.ttfbs, &mut bucket.ttfb_head, first);
                }
            }
        }

        a.duration_samples.push(e.duration_ms);
        if let Some(first) = e.first_upstream_ms {
            a.ttfb_samples.push(first);
        }

        let mut model = e.model.as_str();
        if model.is_empty() {
            model = &e.requested_model;
        }
        if !model.is_empty() {
            let agg = a
                .per_model
                .entry(model.to_string())
                .or_insert_with(|| DimensionAgg::new(model.to_string()));
            agg.add_entry(e);
            a.per_model_day
                .entry(model.to_string())
                .or_default()
                .entry(day.clone())
                .or_default()
                .add(e);
        }
        if !e.key_hash.is_empty() {
            a.per_key
                .entry(e.key_hash.clone())
                .or_insert_with(|| DimensionAgg::new(e.key_hash.clone()))
                .add_entry(e);
        }
        if !e.error_stage.is_empty() {
            *a.err_stages.entry(e.error_stage.clone()).or_insert(0) += 1;
        }

        a.starts.push(gotime::unix(&started));
        // Past the cap, trim starts to the 61s window — older samples can
        // never join a future 429's rate computation. Safe during replay:
        // entries arrive in completion order.
        if a.starts.len() > STARTS_CAP {
            let cut = gotime::unix(&started) - 61;
            a.starts.retain(|s| *s >= cut);
        }
        if is_rate_limited(e) {
            let end = gotime::unix(&started) + e.duration_ms / 1000;
            let rpm = super::to_i64(
                a.starts
                    .iter()
                    .filter(|s| **s > end - 60 && **s <= end)
                    .count(),
            );
            let mut model = e.model.as_str();
            if model.is_empty() {
                model = &e.requested_model;
            }
            a.rl_events.push(RateLimitEvent {
                at: end,
                model: model.to_string(),
                rpm,
                stage: e.error_stage.clone(),
            });
            if a.rl_events.len() > RATE_LIMIT_EVENT_CAP {
                let excess = a.rl_events.len() - RATE_LIMIT_EVENT_CAP;
                a.rl_events.drain(..excess);
            }
        }
    }

    /// `snapshot` — deep-copy view of all aggregates.
    pub fn snapshot(&self) -> UsageSnapshot {
        let mut a = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let today_key = gotime::day_key(&gotime::local_now());
        let mut snap = UsageSnapshot {
            // Go formats a zero time.Time as "0001-01-01T00:00:00Z".
            window_start: a
                .window_start
                .as_ref()
                .map_or_else(|| "0001-01-01T00:00:00Z".to_string(), gotime::rfc3339),
            entries: a.entries,
            today: a.days.get(&today_key).cloned().unwrap_or_default(),
            window: a.total.clone(),
            error_stages: a.err_stages.clone(),
            duration: a.duration_samples.stats(),
            ttfb: a.ttfb_samples.stats(),
            ..UsageSnapshot::default()
        };

        let keys: Vec<String> = a.days.keys().rev().take(USAGE_MAX_DAYS).cloned().collect();
        for k in &keys {
            if let Some(totals) = a.days.get(k) {
                snap.days.push(UsageDayRow {
                    date: k.clone(),
                    totals: totals.clone(),
                });
            }
        }
        if !a.per_model_day.is_empty() {
            let keep: std::collections::BTreeSet<&String> = keys.iter().collect();
            for (model, day_map) in &a.per_model_day {
                let out: BTreeMap<String, UsageTotals> = day_map
                    .iter()
                    .filter(|(d, _)| keep.contains(d))
                    .map(|(d, t)| (d.clone(), t.clone()))
                    .collect();
                if !out.is_empty() {
                    snap.model_days.insert(model.clone(), out);
                }
            }
        }

        let current = gotime::unix_now() / 600;
        snap.points = Vec::with_capacity(USAGE_MIN_BUCKETS);
        for s in (current - i64::try_from(USAGE_MIN_BUCKETS).unwrap_or_default() + 1)..=current {
            let bucket = &a.mins
                [usize::try_from(s % i64::try_from(USAGE_MIN_BUCKETS).unwrap_or(1)).unwrap_or(0)];
            let mut point = UsageMinPoint {
                at: s * 600,
                ..UsageMinPoint::default()
            };
            if bucket.at == s * 600 {
                point.totals = bucket.totals.clone();
                let (avg, p95) = sample_summary(&bucket.durs);
                point.avg_duration_ms = avg;
                point.duration_p95_ms = p95;
                let (avg, p95) = sample_summary(&bucket.ttfbs);
                point.avg_ttfb_ms = avg;
                point.ttfb_p95_ms = p95;
            }
            snap.points.push(point);
        }

        snap.models = sorted_aggs(&mut a.per_model);
        snap.keys = sorted_aggs(&mut a.per_key);
        snap.rate_limit_events.clone_from(&a.rl_events);
        snap
    }

    /// `latencySummary` — only the two global quantile rows; the 1Hz panel
    /// poll consumes just these, and a full snapshot would sort 1152 bucket
    /// samples plus per-dimension rings for nothing.
    pub fn latency_summary(&self) -> LatencySummary {
        let a = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        LatencySummary {
            duration: a.duration_samples.stats(),
            ttfb: a.ttfb_samples.stats(),
        }
    }

    /// `replayLines` — parse snapshot bytes line by line into the
    /// aggregation; returns the number of parsed lines. The snapshot
    /// boundary is drawn by the caller (the replay thread holds `index_mu`
    /// while reading): lines inside the snapshot are counted here, lines
    /// appended after it count via the live path — each line exactly once.
    pub fn replay_lines(&self, data: &[u8]) -> i64 {
        let mut parsed = 0;
        for line in data.split(|b| *b == b'\n') {
            let line = trim_ascii(line);
            if line.is_empty() {
                continue;
            }
            if let Some(entry) = IndexEntry::parse(line) {
                self.add(&entry);
                parsed += 1;
            }
        }
        parsed
    }
}

/// `UsageLatency` result — the two global quantile rows.
#[derive(Debug, Clone)]
pub struct LatencySummary {
    pub duration: LatencyStats,
    pub ttfb: LatencyStats,
}

impl LatencySummary {
    /// `map[string]latencyStats` — Go map marshal sorts keys:
    /// `{"duration":{...},"ttfb":{...}}`.
    pub fn to_go_json(&self) -> Vec<u8> {
        let mut map = BTreeMap::new();
        map.insert(
            "duration".to_string(),
            JVal::Raw(self.duration.to_go_json().into()),
        );
        map.insert("ttfb".to_string(), JVal::Raw(self.ttfb.to_go_json().into()));
        super::gojson::marshal(&JVal::Obj(map)).unwrap_or_else(|_| b"{}".to_vec())
    }
}

/// `sampleSummary` — mean and p95 of a sample vec; zeros when empty.
fn sample_summary(samples: &[i64]) -> (i64, i64) {
    if samples.is_empty() {
        return (0, 0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let sum: i64 = sorted.iter().sum();
    (
        sum / super::to_i64(sorted.len()),
        sorted[p95_index(sorted.len())],
    )
}

/// `0.95 * (len - 1)` truncated — Go's float64 index math for the p95
/// pick; `len` is bounded by the sample ring so precision is exact.
fn p95_index(len: usize) -> usize {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let idx = (0.95 * (len - 1) as f64) as usize;
    idx
}

/// `sortedAggs` — dimension map to rows sorted by request count desc.
fn sorted_aggs(m: &mut BTreeMap<String, DimensionAgg>) -> Vec<DimensionAgg> {
    let mut out: Vec<DimensionAgg> = m
        .values_mut()
        .map(|agg| {
            agg.finish();
            agg.clone()
        })
        .collect();
    out.sort_by(|a, b| {
        b.totals
            .requests
            .cmp(&a.totals.requests)
            .then_with(|| a.name.cmp(&b.name))
    });
    out
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while let Some((&first, rest)) = bytes.split_first() {
        if matches!(first, b' ' | b'\t' | b'\r' | b'\n') {
            bytes = rest;
        } else {
            break;
        }
    }
    while let Some((&last, rest)) = bytes.split_last() {
        if matches!(last, b' ' | b'\t' | b'\r' | b'\n') {
            bytes = rest;
        } else {
            break;
        }
    }
    bytes
}

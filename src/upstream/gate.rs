//! Persisted rate gate with precise admission — port of
//! `G/internal/adapter/devin/rategate.go`.
//!
//! Two independent mechanisms shape the `GetChatMessage` send stream:
//!
//! 1. Aligned minute window: the upstream limiter counts in natural minute
//!    buckets (observed boundary drift around local `:59`-`:00`), so sends
//!    are aligned to the same buckets — `max_rpm` quota per window with a
//!    `guard`-second dead zone on both edges, keeping every send interval a
//!    strict subset of one upstream bucket. No intra-bucket pacing: the
//!    upstream only counts per minute, so burst and even spacing are
//!    equivalent in its counter. Requests that exhaust the quota or land in
//!    the dead zone sleep to the next window open; a predicted wait beyond
//!    `max_hold` fails fast locally with `Retry-After`.
//! 2. Cooldown latch: an upstream `resource_exhausted` declaring "reset in
//!    N" latches until that instant (minute hints align up to the `:59`
//!    bucket boundary). The limiter counts rejected attempts too, so
//!    queueing the whole backlog to fire at recovery re-trips the limit —
//!    inside the latch there is no queue: drip-interval probes are
//!    released, everything else fails fast with `Retry-After` = latch
//!    remainder. Any upstream success frame releases early. Probes only
//!    release inside the sendable interval and count against the bucket.
//!
//! Approved compatibility exception 4 (the only deliberate deviation):
//! latched drip probes obey the configured window quota as well as drip
//! spacing — a free drip slot in a full bucket fast-fails instead of
//! donating count to the upstream bucket.
//!
//! The latch deadline persists to `state_path` (tmp+rename) so a restarted
//! instance inside the latch does not bare-send and extend the upstream
//! limit. The clock is injectable (`with_clock`) — window position depends
//! on wall time, so quota/dead-zone/latch cases pin the clock to a chosen
//! minute-second.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::DevinConfig;
use crate::domain::failure::{self, Failure};

/// Longest in-gate queue wait (Go `gateDefaultMaxHold`): a predicted sleep
/// to the next window beyond this fails fast locally with `Retry-After` —
/// the client/downstream gateway backs off to the declared instant instead
/// of holding a concurrency slot through the cooldown.
pub const DEFAULT_MAX_HOLD: Duration = Duration::from_secs(15);
/// Latched-probe drip interval (Go `gateDefaultDripInterval`): the upstream
/// limiter counts per minute, so one probe per few seconds detects release
/// without extending the debt.
pub const DEFAULT_DRIP_INTERVAL: Duration = Duration::from_secs(8);
/// Fallback latch when upstream `resource_exhausted` carries no reset hint
/// (Go `gateDefaultLatch`, aligned to the common 60s cooldown default).
pub const DEFAULT_LATCH: Duration = Duration::from_secs(60);
/// Upstream limiter counting period (Go `windowPeriod`): natural minute
/// buckets.
const WINDOW_PERIOD: Duration = Duration::from_secs(60);
const WINDOW_PERIOD_NS: i64 = 60_000_000_000;
/// Dead-zone margin on both sides of the estimated bucket boundary (Go
/// `gateDefaultWindowGuard`): covers boundary-estimate error and
/// multi-shard drift; nothing sends inside the dead zone.
pub const DEFAULT_WINDOW_GUARD: Duration = Duration::from_secs(2);
/// Latch-transition event ring capacity (Go `gateEventCap`): latch
/// transitions are low-frequency; 64 entries covers a full day.
const EVENT_CAP: usize = 64;

/// Latch-transition event kinds (Go `gateEvent*`): `Latched` on upstream
/// limit latch/extension, `Released` on early success-frame release,
/// `Expired` on natural deadline expiry, `Restored` on restart recovery of
/// an unexpired latch.
const EVENT_LATCHED: &str = "latched";
const EVENT_RELEASED: &str = "released";
const EVENT_EXPIRED: &str = "expired";
const EVENT_RESTORED: &str = "restored";

/// Panel display name for a latch event (Go `gateEventLabel`): vocabulary
/// and display text are produced in the same place so the frontend event
/// table holds no mirror label table; an in-latch extension
/// (`latched`+`extended`) merges into its own label at production.
fn gate_event_label(kind: &str, detail: &str) -> String {
    match kind {
        EVENT_LATCHED => {
            if detail == "extended" {
                "延闩"
            } else {
                "上闩"
            }
        }
        EVENT_RELEASED => "解闩",
        EVENT_EXPIRED => "到期失效",
        EVENT_RESTORED => "重启恢复",
        _ => kind,
    }
    .to_string()
}

/// One sampled latch-state transition (Go `GateEvent`). `until` is the
/// latch deadline the event involves; `label` is the panel display name
/// computed at production (merged `kind`+`detail` text).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateEvent {
    #[serde(with = "time_serde::system")]
    pub at: SystemTime,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none", with = "time_serde::option")]
    pub until: Option<SystemTime>,
    /// `"extended"` when a `latched` event refreshed a live latch.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub detail: String,
    pub label: String,
}

/// One latch interval reconstructed from the event ring (Go
/// `GateLatchRange`). `start: None` means the opening event rolled out of
/// the ring — the left edge is unknowable and the display layer clips to
/// the view window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateLatchRange {
    #[serde(skip_serializing_if = "Option::is_none", with = "time_serde::option")]
    pub start: Option<SystemTime>,
    #[serde(with = "time_serde::system")]
    pub end: SystemTime,
}

/// Gate state snapshot (Go `GateStats`) — the `gate` section of the panel
/// `/panel/api/stats` payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GateStats {
    pub latched: bool,
    #[serde(skip_serializing_if = "Option::is_none", with = "time_serde::option")]
    pub limited_until: Option<SystemTime>,
    pub latch_count: i64,
    pub drip_count: i64,
    pub reject_latched_count: i64,
    pub reject_hold_count: i64,
    /// Per-bucket quota (= `max_rpm`); 0 means no window pacing.
    pub window_quota: i64,
    /// Admitted sends in the current bucket.
    pub window_used: i64,
    /// Current bucket's sendable-window start.
    #[serde(skip_serializing_if = "Option::is_none", with = "time_serde::option")]
    pub window_open: Option<SystemTime>,
    /// Next bucket's sendable-window open instant.
    #[serde(skip_serializing_if = "Option::is_none", with = "time_serde::option")]
    pub window_next: Option<SystemTime>,
    /// Whether `now` is inside the sendable interval (not the dead zone).
    pub sendable: bool,
    /// Requests currently sleeping to the next window (latched fast-fails
    /// never enter this count).
    pub waiters: i64,
    /// Latch-transition events, newest first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<GateEvent>,
    /// Latch intervals reconstructed from the event ring under the same
    /// lock as the rest of the snapshot — the frontend trend chart paints
    /// them directly instead of replaying the state machine in JS.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub latch_ranges: Vec<GateLatchRange>,
}

/// Tunable gate parameters (Go `GateConfig`): durations at zero fall back
/// to defaults. `Config.devin`'s gate keys map onto this whole block for
/// startup construction and `set_params` hot reload — no per-field
/// translation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GateConfig {
    /// `GetChatMessage` quota per aligned minute window; `<=0` disables
    /// window pacing. The cooldown latch is unaffected and always applies.
    pub max_rpm: i64,
    /// Longest queued wait allowed outside the latch.
    pub max_hold: Duration,
    /// Release interval for latched drip probes.
    pub drip_interval: Duration,
    /// Fallback latch when upstream carries no reset hint.
    pub default_latch: Duration,
    /// Estimated position of the upstream minute-bucket boundary inside the
    /// local minute, in signed nanoseconds (Go `time.Duration`; negative
    /// folds mod 60s, so `-1s` = `:59`). Default 0 centers the estimate on
    /// local `:00` — observed refusal-hint deadlines land at `:58.6`-`:01`
    /// (upstream clock runs ~1s fast).
    pub window_offset_ns: i64,
    /// Dead-zone seconds on both sides of the estimated boundary: the
    /// sendable interval is `[offset+guard, offset+60-guard)`, so as long
    /// as the real boundary stays within estimate ±guard, every sendable
    /// interval is a strict subset of one real upstream bucket.
    pub window_guard: Duration,
}

impl GateConfig {
    /// Maps the `devin.*` config section onto gate parameters — the Rust
    /// equivalent of the `Gate:` block in `G/cmd/devin-2api/main.go`.
    #[must_use]
    pub fn from_devin(devin: &DevinConfig) -> Self {
        Self {
            max_rpm: devin.max_rpm,
            max_hold: seconds_to_duration(devin.gate_max_hold_seconds),
            drip_interval: seconds_to_duration(devin.gate_drip_interval_seconds),
            default_latch: seconds_to_duration(devin.gate_default_latch_seconds),
            window_offset_ns: devin
                .gate_window_offset_seconds
                .saturating_mul(1_000_000_000),
            window_guard: seconds_to_duration(devin.gate_window_guard_seconds),
        }
    }
}

/// Config seconds → `Duration`; `<=0` maps to zero so `set_params` applies
/// the documented default fallback (Go `time.Duration(int) * time.Second`
/// keeps the sign, and the `<=0` checks catch it the same way).
fn seconds_to_duration(seconds: i64) -> Duration {
    Duration::from_secs(u64::try_from(seconds).unwrap_or_default())
}

/// `<=0` duration parameters fall back to defaults (Go
/// `gateDurationOrDefault`); `Duration` is unsigned, so zero is the only
/// "unset" reachable here — negative config values already collapsed to
/// zero in `seconds_to_duration`.
fn duration_or_default(value: Duration, fallback: Duration) -> Duration {
    if value.is_zero() { fallback } else { value }
}

/// Mutable gate internals (Go `rateGate` fields guarded by `mu`).
struct State {
    /// Per-bucket quota (= `max_rpm`); `<=0` disables window pacing.
    quota: i64,
    /// Sendable-window start within the minute (= `offset+guard`, mod 60s),
    /// nanoseconds into the minute.
    window_open_ns: u64,
    /// Sendable-interval length (= 60s − 2·guard).
    usable: Duration,
    /// Current counting bucket's window start.
    bucket_start: SystemTime,
    /// Admitted sends in this bucket (drip probes included — matching the
    /// upstream "rejected attempts count too" accounting).
    bucket_used: i64,
    /// Cooldown-latch deadline; `None` = unlatched.
    limited_until: Option<SystemTime>,
    /// Next probe-release instant inside the latch.
    next_drip: Option<SystemTime>,
    max_hold: Duration,
    drip_interval: Duration,
    default_latch: Duration,
    // Panel-visible counters; all read/written under the state lock.
    latch_count: i64,
    drip_count: i64,
    /// Requests fast-failed inside the latch.
    reject_latched: i64,
    /// Requests fast-failed outside the latch on predicted wait > `max_hold`.
    reject_hold: i64,
    /// Requests currently sleeping to the next window.
    waiters: i64,
    /// Latch-transition event ring: counters say how many latches happened,
    /// the ring answers when, how long and how each ended — the overview
    /// trend chart's latch shading and the system page's event table share
    /// this source.
    events: VecDeque<GateEvent>,
}

/// The persisted rate gate (Go `rateGate`). Admission is invoked
/// immediately before every real upstream send — including transient-error
/// retries, since rejected attempts still push the upstream recovery
/// instant out.
pub struct Gate {
    state: Mutex<State>,
    /// When set, the latch deadline persists here (tmp+rename): a restarted
    /// instance still inside the latch does not bare-send upstream and
    /// extend the limit.
    state_path: Option<PathBuf>,
    /// Clock source — tests inject a controllable fake (Go `now`); window
    /// position depends on wall time, so pinning the clock pins the
    /// minute-second every rule evaluates against.
    clock: Mutex<Box<dyn Fn() -> SystemTime + Send + Sync>>,
}

impl Gate {
    /// Create the gate on the real clock (Go `newRateGate`); `max_rpm <= 0`
    /// leaves only the cooldown latch. A non-`None` `state_path` restores
    /// an unexpired latch.
    #[must_use]
    pub fn new(params: GateConfig, state_path: Option<PathBuf>) -> Self {
        Self::with_clock(params, state_path, Box::new(SystemTime::now))
    }

    /// Clock-injecting constructor — the deterministic-test seam (Go tests
    /// assign `gate.now`). The injected clock is the single time source for
    /// window position, latch deadlines and event timestamps.
    #[must_use]
    pub fn with_clock(
        params: GateConfig,
        state_path: Option<PathBuf>,
        clock: Box<dyn Fn() -> SystemTime + Send + Sync>,
    ) -> Self {
        let gate = Self {
            state: Mutex::new(State {
                quota: 0,
                window_open_ns: 0,
                usable: Duration::ZERO,
                bucket_start: SystemTime::UNIX_EPOCH,
                bucket_used: 0,
                limited_until: None,
                next_drip: None,
                max_hold: DEFAULT_MAX_HOLD,
                drip_interval: DEFAULT_DRIP_INTERVAL,
                default_latch: DEFAULT_LATCH,
                latch_count: 0,
                drip_count: 0,
                reject_latched: 0,
                reject_hold: 0,
                waiters: 0,
                events: VecDeque::with_capacity(EVENT_CAP),
            }),
            state_path,
            clock: Mutex::new(clock),
        };
        gate.set_params(params);
        gate.restore_state();
        gate
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("rate gate state poisoned")
    }

    fn now(&self) -> SystemTime {
        (self.clock.lock().expect("rate gate clock poisoned"))()
    }

    /// Hot-update gate parameters (Go `setParams` reload path): latch state
    /// is preserved; the next `wait`/`stats` recomputes the current bucket
    /// under the new boundaries, and a different bucket start opens a fresh
    /// bucket.
    pub fn set_params(&self, params: GateConfig) {
        let mut state = self.lock();
        state.max_hold = duration_or_default(params.max_hold, DEFAULT_MAX_HOLD);
        state.drip_interval = duration_or_default(params.drip_interval, DEFAULT_DRIP_INTERVAL);
        state.default_latch = duration_or_default(params.default_latch, DEFAULT_LATCH);
        let offset = params.window_offset_ns.rem_euclid(WINDOW_PERIOD_NS);
        let mut guard = params.window_guard;
        if guard.is_zero()
            || guard
                .checked_mul(2)
                .is_none_or(|doubled| doubled >= WINDOW_PERIOD)
        {
            guard = DEFAULT_WINDOW_GUARD;
        }
        state.quota = params.max_rpm;
        let open = (offset + i64::try_from(guard.as_nanos()).unwrap_or(i64::MAX))
            .rem_euclid(WINDOW_PERIOD_NS);
        state.window_open_ns = u64::try_from(open).unwrap_or_default();
        state.usable = WINDOW_PERIOD.checked_sub(2 * guard).unwrap();
    }

    /// Block until this upstream send is admitted, or decide it is not
    /// worth waiting (Go `wait`):
    ///   - latched: a free drip slot inside the sendable interval admits
    ///     immediately — the request IS the probe and counts against this
    ///     bucket's quota (approved exception 4: probes obey the configured
    ///     window quota as well as drip spacing); otherwise a local-gate
    ///     rejection carries `Retry-After` = latch remainder — the client
    ///     sleeping to recovery beats polling on slot rhythm;
    ///   - unlatched: inside the sendable interval with quota left admits;
    ///     exhausted quota or dead zone sleeps to the next window open, and
    ///     a predicted wait beyond `max_hold` rejects the same way;
    ///   - no quota reservation while sleeping: wakers race new arrivals at
    ///     the loop head, and whoever loses sees a full bucket and decides
    ///     again — minute-granularity ordering fairness is not worth the
    ///     complexity. Wakers re-evaluate rather than admitting directly:
    ///     latch state may have changed during the sleep.
    ///
    /// `cancel` is the request lineage's cancellation token (Go `ctx`).
    pub async fn wait(&self, cancel: &CancellationToken) -> Result<(), WaitError> {
        let mut sleeping = false; // this request holds a waiters slot
        loop {
            match self.evaluate(cancel, &mut sleeping) {
                Err(err) => return Err(err),
                Ok(Decision::Admit) => return Ok(()),
                Ok(Decision::Reject(retry_after)) => {
                    return Err(WaitError::Rejected(gate_rejection(retry_after)));
                }
                Ok(Decision::Sleep(wait)) => {
                    tokio::select! {
                        () = cancel.cancelled() => {
                            self.lock().waiters -= 1;
                            return Err(WaitError::Cancelled);
                        }
                        () = tokio::time::sleep(wait) => {}
                    }
                }
            }
        }
    }

    /// One admission evaluation under the state lock — the body of Go's
    /// `wait` loop above the sleep.
    fn evaluate(
        &self,
        cancel: &CancellationToken,
        sleeping: &mut bool,
    ) -> Result<Decision, WaitError> {
        let mut state = self.lock();
        if *sleeping {
            state.waiters -= 1;
            *sleeping = false;
        }
        // Waking and cancellation can be ready together (select picks
        // randomly): re-check at the loop head so a cancelled request is
        // not admitted and counted when its timer happened to win.
        if cancel.is_cancelled() {
            return Err(WaitError::Cancelled);
        }
        let now = self.now();
        // Latch expiry is natural lapse, not release (no success-frame
        // evidence).
        self.expire_if_due(&mut state, now);
        // The counting bucket rolls with the window boundary; an expired
        // bucket's usage does not carry over.
        let ws = window_start(&state, now);
        if ws != state.bucket_start {
            state.bucket_start = ws;
            state.bucket_used = 0;
        }
        let sendable = now.duration_since(ws).unwrap_or_default() < state.usable;
        if state.limited_until.is_some_and(|until| now < until) {
            let drip_open = state.next_drip.is_none_or(|next| now >= next);
            let quota_open = state.quota <= 0 || state.bucket_used < state.quota;
            if sendable && drip_open && quota_open {
                // Probe slot free inside the sendable interval: admit and
                // advance the next slot. No probes in the dead zone — a
                // send near the boundary can land in the adjacent real
                // bucket and donate count. `quota_open` is the approved
                // fix: probes are real upstream sends and obey the
                // configured window quota as well as drip spacing.
                state.next_drip = Some(now + state.drip_interval);
                state.drip_count += 1;
                state.bucket_used += 1;
                return Ok(Decision::Admit);
            }
            state.reject_latched += 1;
            let retry_after = state
                .limited_until
                .and_then(|until| until.duration_since(now).ok())
                .unwrap_or_default();
            return Ok(Decision::Reject(retry_after));
        }
        if state.quota <= 0 {
            return Ok(Decision::Admit);
        }
        if sendable && state.bucket_used < state.quota {
            state.bucket_used += 1;
            return Ok(Decision::Admit);
        }
        let wait = (ws + WINDOW_PERIOD).duration_since(now).unwrap_or_default();
        if wait > state.max_hold {
            state.reject_hold += 1;
            return Ok(Decision::Reject(wait));
        }
        state.waiters += 1;
        *sleeping = true;
        Ok(Decision::Sleep(wait))
    }

    /// Refresh the cooldown latch from an upstream failure (Go
    /// `noteUpstreamError`): only `resource_exhausted` is rate-limit
    /// evidence; every other error is ignored as-is. The latch only
    /// extends, never shortens; the drip clock re-arms only when the latch
    /// was extended — a repeated refusal with an unchanged deadline means
    /// the window has not passed and the probing rhythm still holds, so
    /// re-arming would only delay the next probe for nothing.
    pub fn note_upstream_error(&self, err: &(dyn Error + 'static)) {
        let failure = failure::classify(err);
        // The local gate's own rejections (`local_gate`) carry no upstream
        // evidence and cannot arm the latch.
        if failure.local_gate || !failure.rate_limited {
            return;
        }
        let now = self.now();
        let hinted = failure.rate_limit_reset(now);
        // `default_latch` is hot-updated by `set_params` — read it under
        // the lock.
        let mut state = self.lock();
        let until = hinted.unwrap_or_else(|| now + state.default_latch);
        let extended = state.limited_until.is_none_or(|current| until > current);
        let remaining = state
            .limited_until
            .and_then(|current| current.duration_since(now).ok())
            .unwrap_or_default();
        if extended {
            state.limited_until = Some(until);
            state.next_drip = Some(now + state.drip_interval);
            state.latch_count += 1;
            // A re-latch inside a live latch records `extended` — the
            // deadline moved before expiry, distinct from a fresh latch.
            let detail = if remaining > Duration::ZERO {
                "extended"
            } else {
                ""
            };
            self.push_event(&mut state, EVENT_LATCHED, Some(until), detail);
            // Persist under the lock: after unlock, persist could interleave
            // with a concurrent release's clear — clear first, write after,
            // and the released deadline survives as a ghost latch on
            // restart.
            self.persist_state(until);
        }
        drop(state);
        if extended {
            warn!(
                "upstream message rate limited; drip-latching new requests until {}",
                format_rfc3339_nano(until)
            );
        } else {
            info!("upstream message rate limited while latched; remaining {remaining:?}");
        }
    }

    /// Release the latch on any upstream data frame (Go
    /// `noteUpstreamSuccess`): a frame proves the send crossed upstream
    /// admission (refusals are probabilistic at the margin), so holding the
    /// latch to the declared deadline only wastes drip windows. If that
    /// send later ends in a rate-limit error, `note_upstream_error`
    /// re-latches — the sub-millisecond release window between the two
    /// judgements leaks at most one waiting request, the same price as one
    /// drip probe. Post-release admission still obeys the window quota —
    /// remaining quota is the natural cap on a post-latch burst.
    pub fn note_upstream_success(&self) {
        let mut state = self.lock();
        let latched = state.limited_until.is_some();
        if latched {
            let until = state.limited_until;
            self.push_event(&mut state, EVENT_RELEASED, until, "");
            state.limited_until = None;
            state.next_drip = None;
            // Clear shares the latch writer's lock order: outside the lock a
            // "persist after clear" interleave would write the released
            // deadline back into the state file.
            self.clear_state();
        }
        drop(state);
        if latched {
            info!("rate gate released: upstream accepted a message");
        }
    }

    /// Gate state snapshot (Go `stats`). Also lazily settles due latch
    /// expiry and bucket rollover: `wait` only runs on traffic, so this
    /// polling path records idle-time transitions and closes the panel
    /// timeline.
    pub fn stats(&self) -> GateStats {
        let mut state = self.lock();
        let now = self.now();
        self.expire_if_due(&mut state, now);
        let ws = window_start(&state, now);
        if ws != state.bucket_start {
            state.bucket_start = ws;
            state.bucket_used = 0;
        }
        let mut snapshot = GateStats {
            latched: state.limited_until.is_some_and(|until| now < until),
            limited_until: state.limited_until,
            latch_count: state.latch_count,
            drip_count: state.drip_count,
            reject_latched_count: state.reject_latched,
            reject_hold_count: state.reject_hold,
            window_quota: state.quota,
            window_used: state.bucket_used,
            window_open: None,
            window_next: None,
            sendable: now.duration_since(ws).unwrap_or_default() < state.usable,
            waiters: state.waiters,
            // Newest first — the ring stores write order (oldest first).
            events: state.events.iter().rev().cloned().collect(),
            latch_ranges: Vec::new(),
        };
        if state.quota > 0 {
            snapshot.window_open = Some(ws);
            snapshot.window_next = Some(ws + WINDOW_PERIOD);
        }
        snapshot.latch_ranges = latch_ranges(&state, now);
        snapshot
    }

    /// Natural latch expiry (Go `expireIfDue`): appends `expired` and
    /// clears — expiry is not release (no success-frame evidence), but the
    /// deadline has passed so memory and the state file both close out.
    /// Caller holds the state lock; `wait` checks once per request and
    /// `stats` polling covers idle periods.
    fn expire_if_due(&self, state: &mut State, now: SystemTime) {
        let Some(until) = state.limited_until else {
            return;
        };
        if now < until {
            return;
        }
        self.push_event(state, EVENT_EXPIRED, Some(until), "");
        state.limited_until = None;
        state.next_drip = None;
        self.clear_state();
    }

    /// Append a latch-transition event (Go `pushEvent`); the caller holds
    /// the state lock (startup restore runs before concurrency, which Go
    /// treats as holding it).
    fn push_event(&self, state: &mut State, kind: &str, until: Option<SystemTime>, detail: &str) {
        let event = GateEvent {
            at: self.now(),
            kind: kind.to_string(),
            until,
            detail: detail.to_string(),
            label: gate_event_label(kind, detail),
        };
        if state.events.len() == EVENT_CAP {
            state.events.pop_front();
        }
        state.events.push_back(event);
    }

    /// Startup restore of an unexpired latch (Go `restoreState`): the drip
    /// clock re-arms by interval. Missing/corrupt/expired files all mean
    /// "no latch", and a stale file is removed in passing.
    fn restore_state(&self) {
        let Some(path) = &self.state_path else {
            return;
        };
        let Ok(data) = fs::read(path) else {
            return;
        };
        let now = self.now();
        let until = serde_json::from_slice::<GateStateFile>(&data)
            .ok()
            .filter(|file| file.limited_until > now)
            .map(|file| file.limited_until);
        let Some(until) = until else {
            let _ = fs::remove_file(path);
            return;
        };
        let mut state = self.lock();
        state.limited_until = Some(until);
        state.next_drip = Some(now + state.drip_interval);
        self.push_event(&mut state, EVENT_RESTORED, Some(until), "");
        drop(state);
        warn!(
            "rate gate latch restored from state file until {}",
            format_rfc3339_nano(until)
        );
    }

    /// Atomic latch-deadline persist (Go `persistState`): tmp+rename; write
    /// failures only log — persistence is restart insurance and does not
    /// block the request path. Caller holds the state lock.
    fn persist_state(&self, until: SystemTime) {
        let Some(path) = &self.state_path else {
            return;
        };
        let Ok(data) = serde_json::to_vec(&GateStateFile {
            limited_until: until,
        }) else {
            return;
        };
        let tmp = tmp_path(path);
        if let Err(err) = write_mode_600(&tmp, &data) {
            warn!("rate gate state write failed: {err}");
            return;
        }
        if let Err(err) = fs::rename(&tmp, path) {
            warn!("rate gate state rename failed: {err}");
        }
    }

    /// Remove the state file on unlatch (Go `clearState`); a missing file
    /// is not an error.
    fn clear_state(&self) {
        let Some(path) = &self.state_path else {
            return;
        };
        if let Err(err) = fs::remove_file(path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!("rate gate state remove failed: {err}");
        }
    }
}

/// One admission-evaluation outcome inside `wait`'s loop.
enum Decision {
    Admit,
    Reject(Duration),
    Sleep(Duration),
}

/// `wait` failure (Go `error` result): either a local-gate rejection
/// carrying the classified [`Failure`], or cancellation of the wait
/// lineage.
#[derive(Debug)]
pub enum WaitError {
    /// Local-gate rejection — the request never reached upstream
    /// (`Failure.local_gate`); `code`/`retry_after_seconds` are set.
    Rejected(Failure),
    /// The wait's cancellation token fired (Go `context.Cause(ctx)` →
    /// `context.Canceled` for a plain cancel).
    Cancelled,
}

/// Static cancellation sentinel for `WaitError::Cancelled`'s source chain —
/// `classify` downcasts it the way Go probes `context.Canceled`.
static CANCELED: failure::Canceled = failure::Canceled;

impl fmt::Display for WaitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(failure) => fmt::Display::fmt(failure, f),
            Self::Cancelled => fmt::Display::fmt(&CANCELED, f),
        }
    }
}

impl Error for WaitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Rejected(failure) => Some(failure),
            Self::Cancelled => Some(&CANCELED),
        }
    }
}

/// Bucket window start containing `t` (Go `windowStart`): the most recent
/// `window_open` boundary inside t's minute; before the boundary, the
/// previous minute's boundary.
fn window_start(state: &State, t: SystemTime) -> SystemTime {
    let since_epoch = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let t_ns = u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX);
    let minute_base = t_ns - t_ns % (WINDOW_PERIOD_NS as u64);
    let open_ns = minute_base + state.window_open_ns;
    let start_ns = if open_ns > t_ns {
        // Before this minute's boundary → the previous minute's. Pre-epoch
        // results cannot exist as `SystemTime` nanoseconds; saturate to the
        // epoch (real times are never near it).
        open_ns.saturating_sub(WINDOW_PERIOD_NS as u64)
    } else {
        open_ns
    };
    SystemTime::UNIX_EPOCH + Duration::from_nanos(start_ns)
}

/// Reconstruct latch intervals from the event ring in write order (Go
/// `latchRanges`): `latched`/`restored` open a range, `released` closes
/// early, `expired` closes at the deadline; an in-latch extension only
/// advances the right edge. A still-open range ends at `now`; a live latch
/// whose opening event rolled out of the ring yields `start: None`.
fn latch_ranges(state: &State, now: SystemTime) -> Vec<GateLatchRange> {
    if state.events.is_empty() {
        return Vec::new();
    }
    let mut ranges: Vec<GateLatchRange> = Vec::new();
    let mut open: Option<GateLatchRange> = None;
    for ev in &state.events {
        let until = ev.until.unwrap_or(ev.at);
        match ev.kind.as_str() {
            EVENT_LATCHED | EVENT_RESTORED => {
                // An opening event later than the current range's right
                // edge means the previous latch lapsed naturally (its
                // `expired` may have rolled out of the ring): close the old
                // range first, then open a new one.
                if let Some(current) = &open
                    && ev.at > current.end
                {
                    let end = current.end;
                    close_open(&mut open, &mut ranges, end);
                }
                match &mut open {
                    None => {
                        open = Some(GateLatchRange {
                            start: Some(ev.at),
                            end: until,
                        });
                    }
                    Some(current) => {
                        if until > current.end {
                            current.end = until;
                        }
                    }
                }
            }
            EVENT_RELEASED => close_open(&mut open, &mut ranges, ev.at),
            EVENT_EXPIRED => close_open(&mut open, &mut ranges, until),
            _ => {}
        }
    }
    if let Some(current) = &open {
        let end = current.end.min(now);
        close_open(&mut open, &mut ranges, end);
    } else if state.limited_until.is_some_and(|until| now < until) {
        // The live latch's opening event rolled out of the ring: the left
        // edge is unknowable — `start: None` (Go nil `Start`).
        ranges.push(GateLatchRange {
            start: None,
            end: now,
        });
    }
    ranges
}

/// Close the in-progress range at `end` (Go `closeOpen`).
fn close_open(
    open: &mut Option<GateLatchRange>,
    ranges: &mut Vec<GateLatchRange>,
    end: SystemTime,
) {
    if let Some(mut range) = open.take() {
        range.end = end;
        ranges.push(range);
    }
}

/// Local-gate rejection record (Go `gateRejection`): `resource_exhausted`
/// keeps the downstream 429 translation, `local_gate` marks "never reached
/// upstream" for attribution (`rate_gate`), and `retry_after_seconds`
/// carries the exact wait — no fake "reset in N seconds" text for
/// downstream to re-parse. The message keeps the original wording clients
/// and logs already see. Derived fields stay zeroed; `classify` fills them
/// uniformly.
fn gate_rejection(retry_after: Duration) -> Failure {
    // math.Ceil(retryAfter.Seconds()).
    let seconds = i64::try_from(retry_after.as_secs()).unwrap_or(i64::MAX)
        + i64::from(retry_after.subsec_nanos() > 0);
    Failure {
        code: "resource_exhausted".to_string(),
        message: format!(
            "upstream message rate limited by local gate; your limit will reset in {seconds} seconds."
        ),
        local_gate: true,
        retry_after_seconds: seconds,
        ..Failure::default()
    }
}

/// Persisted latch shape (Go `gateStateFile`): only the deadline is stored
/// — the drip clock and window counts deliberately are not (a restart
/// counts a fresh window; the in-latch rhythm re-arms by `drip_interval`).
#[derive(Debug, Serialize, Deserialize)]
struct GateStateFile {
    #[serde(with = "time_serde::system")]
    limited_until: SystemTime,
}

/// `statePath + ".tmp"` — byte-level append like Go string concat.
fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Go `os.WriteFile(tmp, data, 0o600)`: create/truncate with mode 0600.
#[cfg(unix)]
fn write_mode_600(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut file| file.write_all(data))
}

/// Non-Unix fallback: platform default permissions (Go's mode argument is
/// equally advisory there).
#[cfg(not(unix))]
fn write_mode_600(path: &Path, data: &[u8]) -> io::Result<()> {
    fs::write(path, data)
}

/// Go `time.RFC3339Nano` formatting (UTC): trailing fractional zeros
/// trimmed, no fraction when zero, `Z` suffix.
fn format_rfc3339_nano(t: SystemTime) -> String {
    let total_ns: i128 = match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => i128::from(d.as_secs()) * 1_000_000_000 + i128::from(d.subsec_nanos()),
        Err(e) => {
            let d = e.duration();
            -(i128::from(d.as_secs()) * 1_000_000_000 + i128::from(d.subsec_nanos()))
        }
    };
    let secs = total_ns.div_euclid(1_000_000_000);
    let nanos = total_ns.rem_euclid(1_000_000_000);
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let mut out = format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    );
    if nanos > 0 {
        let frac = format!("{nanos:09}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push('Z');
    out
}

/// Strict RFC3339 parse: `YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)` — the shape
/// Go's `time.Time` JSON unmarshal accepts; anything else is corrupt state.
fn parse_rfc3339_nano(text: &str) -> Option<SystemTime> {
    let bytes = text.as_bytes();
    let fixed = |index: usize, byte: u8| bytes.get(index) == Some(&byte);
    if bytes.len() < 20
        || !fixed(4, b'-')
        || !fixed(7, b'-')
        || !fixed(13, b':')
        || !fixed(16, b':')
        || !matches!(bytes.get(10), Some(b'T' | b't' | b' '))
    {
        return None;
    }
    let year = digits_i128(text, 0, 4)?;
    let month = digits_i128(text, 5, 2)?;
    let day = digits_i128(text, 8, 2)?;
    let hour = digits_i128(text, 11, 2)?;
    let minute = digits_i128(text, 14, 2)?;
    let second = digits_i128(text, 17, 2)?;
    if !(1..=12).contains(&month)
        || hour > 23
        || minute > 59
        || second > 59
        || day < 1
        || day > days_in_month(year, month)
    {
        return None;
    }
    let mut index = 19;
    let mut nanos: i128 = 0;
    if fixed(index, b'.') {
        index += 1;
        let start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        let digits = index - start;
        if digits == 0 || digits > 9 {
            return None;
        }
        nanos = digits_i128(text, start, digits)? * 10_i128.pow(u32::try_from(9 - digits).ok()?);
    }
    let offset_seconds: i128 = match bytes.get(index) {
        Some(b'Z' | b'z') => {
            index += 1;
            0
        }
        Some(b'+' | b'-') => {
            let sign: i128 = if bytes[index] == b'-' { -1 } else { 1 };
            if !fixed(index + 3, b':') {
                return None;
            }
            let off_hour = digits_i128(text, index + 1, 2)?;
            let off_minute = digits_i128(text, index + 4, 2)?;
            if off_hour > 23 || off_minute > 59 {
                return None;
            }
            index += 6;
            sign * (off_hour * 3600 + off_minute * 60)
        }
        _ => return None,
    };
    if index != bytes.len() {
        return None;
    }
    let secs = days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second
        - offset_seconds;
    let total_ns = secs * 1_000_000_000 + nanos;
    if let Ok(ns) = u64::try_from(total_ns) {
        Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(ns))
    } else {
        let neg = u64::try_from(-total_ns).ok()?;
        Some(SystemTime::UNIX_EPOCH - Duration::from_nanos(neg))
    }
}

fn digits_i128(text: &str, start: usize, len: usize) -> Option<i128> {
    let slice = text.get(start..start + len)?;
    if !slice.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    slice.parse().ok()
}

fn days_in_month(year: i128, month: i128) -> i128 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

/// Days since the Unix epoch → civil date (Hinnant's `civil_from_days`,
/// proleptic Gregorian). `719_468` shifts the epoch-based day count to
/// the algorithm's 0000-03-01 day zero — without it every rendered date
/// lands ~1970 years early.
fn civil_from_days(days: i128) -> (i128, i128, i128) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let doe = days.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Civil date → days since the Unix epoch (Hinnant's `days_from_civil`).
/// `- 719_468` shifts the 0000-03-01 day count back to the epoch origin —
/// the mirror of [`civil_from_days`].
fn days_from_civil(year: i128, month: i128, day: i128) -> i128 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year.rem_euclid(400);
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `RFC3339Nano` `SystemTime` serde — Go `time.Time`'s JSON shape
/// (`"2006-01-02T15:04:05.999999999Z07:00"`; written as UTC `Z`, any offset
/// accepted on read). Shared by the persisted state file and the stats
/// payload.
mod time_serde {
    /// `SystemTime` ↔ `RFC3339Nano` string.
    pub mod system {
        use serde::{Deserialize as _, Deserializer, Serializer};

        use super::super::{format_rfc3339_nano, parse_rfc3339_nano};
        use std::time::SystemTime;

        pub fn serialize<S: Serializer>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error> {
            s.serialize_str(&format_rfc3339_nano(*t))
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<SystemTime, D::Error> {
            let text = String::deserialize(d)?;
            parse_rfc3339_nano(&text)
                .ok_or_else(|| serde::de::Error::custom("invalid RFC3339 timestamp"))
        }
    }

    /// `Option<SystemTime>` → nullable `RFC3339Nano` string (serialize
    /// only — stats payload fields never deserialize).
    pub mod option {
        use serde::Serializer;

        use super::super::format_rfc3339_nano;
        use std::time::SystemTime;

        // serde's serialize_with protocol passes `&Option<T>`; the
        // reference is required by the attribute.
        #[allow(clippy::ref_option)]
        pub fn serialize<S: Serializer>(t: &Option<SystemTime>, s: S) -> Result<S::Ok, S::Error> {
            match t {
                Some(t) => s.serialize_some(&format_rfc3339_nano(*t)),
                None => s.serialize_none(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{format_rfc3339_nano, parse_rfc3339_nano};
    use std::time::{Duration, SystemTime};

    /// Fixed instant `secs` after the Unix epoch — test timestamps stay in
    /// epoch seconds, the natural unit for known-instant assertions.
    fn epoch_plus(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// Absolute format assertions — a self-consistent format/parse pair
    /// passes round-trips even when both share the same epoch-shift bug,
    /// so these pin known instants to their exact RFC3339 strings.
    #[test]
    fn format_rfc3339_nano_absolute() {
        assert_eq!(
            format_rfc3339_nano(SystemTime::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
        assert_eq!(
            format_rfc3339_nano(epoch_plus(86_400)),
            "1970-01-02T00:00:00Z"
        );
        // 2026-09-22T16:09:00Z — the epoch-shift defect rendered this as
        // year 0056.
        assert_eq!(
            format_rfc3339_nano(epoch_plus(1_790_093_340)),
            "2026-09-22T16:09:00Z"
        );
        // Fractional seconds keep Go's RFC3339Nano trailing-zero trim.
        assert_eq!(
            format_rfc3339_nano(epoch_plus(1_790_093_340) + Duration::from_micros(123_400)),
            "2026-09-22T16:09:00.1234Z"
        );
        // Pre-epoch instants format with a negative-divided day count.
        assert_eq!(
            format_rfc3339_nano(SystemTime::UNIX_EPOCH - Duration::from_secs(1)),
            "1969-12-31T23:59:59Z"
        );
    }

    /// Absolute parse assertions — Go-written `gate-state.json` deadlines
    /// must land on the real instant (the missing `- 719_468` overflowed
    /// `u64` nanoseconds, failing deserialization and deleting the file).
    #[test]
    fn parse_rfc3339_nano_absolute() {
        assert_eq!(
            parse_rfc3339_nano("1970-01-01T00:00:00Z"),
            Some(SystemTime::UNIX_EPOCH)
        );
        assert_eq!(
            parse_rfc3339_nano("2026-09-22T16:30:00Z"),
            Some(epoch_plus(1_790_094_600))
        );
        // Fractional seconds and a non-UTC offset resolve to the same
        // instant.
        assert_eq!(
            parse_rfc3339_nano("2026-09-22T16:30:00.5Z"),
            Some(epoch_plus(1_790_094_600) + Duration::from_millis(500))
        );
        assert_eq!(
            parse_rfc3339_nano("2026-09-22T18:30:00+02:00"),
            Some(epoch_plus(1_790_094_600))
        );
    }

    /// Format/parse self-consistency across epoch, fractional and
    /// pre-epoch instants.
    #[test]
    fn rfc3339_nano_round_trips() {
        for nanos in [
            0_u128,
            1_790_093_340_000_000_000,
            1_790_093_340_123_456_789,
            86_400_000_000_000,
        ] {
            let t = SystemTime::UNIX_EPOCH + Duration::from_nanos(u64::try_from(nanos).unwrap());
            assert_eq!(
                parse_rfc3339_nano(&format_rfc3339_nano(t)),
                Some(t),
                "round-trip {nanos}ns"
            );
        }
        let pre = SystemTime::UNIX_EPOCH - Duration::new(3_600, 250_000_000);
        assert_eq!(
            parse_rfc3339_nano(&format_rfc3339_nano(pre)),
            Some(pre),
            "pre-epoch round-trip"
        );
    }
}

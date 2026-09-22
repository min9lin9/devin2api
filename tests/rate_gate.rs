//! Rate-gate contract tests for the devin2api Rust port (plan task 10).
//!
//! Ports `G/internal/adapter/devin/rategate_test.go` against
//! `devin2api::upstream::gate`. Test names mirror the manifest's
//! `rate_gate::<GoFunc>` `rust_case` entries (hence the file-level
//! `non_snake_case` allow). All timing is deterministic: the gate clock is
//! a pinned fake advanced by hand, and `tokio::time` runs paused — no
//! sleep-based correctness anywhere.
#![allow(non_snake_case)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use connectrpc::{ConnectError, ErrorCode};
use devin2api::domain::failure::{self, Failure};
use devin2api::upstream::gate::{Gate, GateConfig, WaitError};
use tokio_util::sync::CancellationToken;

/// `connect.NewError(connect.CodeResourceExhausted, errors.New(text))`.
fn rate_limit_err(text: &str) -> ConnectError {
    ConnectError::new(ErrorCode::ResourceExhausted, text)
}

/// Test fake clock (Go `fakeClock`): a static instant the case pins to a
/// chosen minute-second and advances by hand.
#[derive(Clone)]
struct FakeClock {
    t: Arc<Mutex<SystemTime>>,
}

impl FakeClock {
    fn now(&self) -> SystemTime {
        *self.t.lock().expect("clock poisoned")
    }

    fn advance(&self, d: Duration) {
        let mut t = self.t.lock().expect("clock poisoned");
        *t += d;
    }
}

/// `time.Now().Truncate(time.Minute).Add(sec)` — the epoch minute base plus
/// `sec` seconds (fractional allowed).
fn minute_second(sec: f64) -> SystemTime {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post-epoch");
    let base = now.as_secs() - now.as_secs() % 60;
    SystemTime::UNIX_EPOCH + Duration::from_secs(base) + Duration::from_secs_f64(sec)
}

/// Gate pinned at minute-second `sec` (Go `pinGateClock`). Default params
/// make `:02`-`:58` sendable and `:58`-`:02` the dead zone.
fn pinned_gate(params: GateConfig, sec: f64) -> (Arc<Gate>, FakeClock) {
    let clock = FakeClock {
        t: Arc::new(Mutex::new(minute_second(sec))),
    };
    let gate_clock = clock.clone();
    let gate = Gate::with_clock(params, None, Box::new(move || gate_clock.now()));
    (Arc::new(gate), clock)
}

/// Advance the fake clock and Tokio's paused timer together — the
/// deterministic equivalent of Go's `offsetGateClock` + real sleep.
async fn advance(clock: &FakeClock, d: Duration) {
    clock.advance(d);
    tokio::time::advance(d).await;
}

/// Spin until `n` waiters are registered (bounded; deterministic — the
/// waiter registers `waiters` and its timer in the same poll).
async fn wait_for_waiters(gate: &Gate, n: i64) {
    for _ in 0..10_000 {
        if gate.stats().waiters == n {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("waiters never reached {n}");
}

/// Extract the local-gate `Failure` from a `wait` result.
fn gate_failure(result: Result<(), WaitError>) -> Failure {
    match result {
        Err(WaitError::Rejected(failure)) => failure,
        other => panic!("want local-gate rejection, got {other:?}"),
    }
}

/// Fresh per-test work directory (unique per test via `tag`).
fn work_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "devin2api-gate-{}-{}-{:?}",
        tag,
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create work dir");
    dir
}

fn never() -> CancellationToken {
    CancellationToken::new()
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/adapter/devin/rategate_test.go
// ---------------------------------------------------------------------------

/// Port of `TestRateGateLatchRejectsUntilReset`: a latch short of its
/// declared instant rejects locally with `Retry-After` = latch remainder
/// (minute hints align up to the `:59` bucket boundary).
#[tokio::test(start_paused = true)]
async fn RateGateLatchRejectsUntilReset() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Please try again later. Your limit will reset in 8 minutes. (trace ID: x)",
    ));
    let failure = gate_failure(gate.wait(&never()).await);
    assert!(failure.local_gate, "want local-gate failure");
    // Pinned clock: :10 + 8min → :18:10 → bucket :18 → :18:59 → 529s.
    assert_eq!(failure.retry_after_seconds, 529);
    // The classified record must translate to 429 downstream: the
    // `resource_exhausted` code + `rate_limited` derivation are what the
    // error pipeline's HTTPStatus maps (ported with the API surface task).
    let classified = failure::classify(&failure);
    assert_eq!(classified.code, "resource_exhausted");
    assert!(classified.rate_limited);
}

/// Port of `TestRateGateLatchFastFails`: no queueing inside the latch —
/// regardless of remaining time the request fast-fails with `Retry-After`
/// = latch remainder; the client sleeps to recovery instead of holding a
/// concurrency slot.
#[tokio::test(start_paused = true)]
async fn RateGateLatchFastFails() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 1 seconds.",
    ));
    let failure = gate_failure(gate.wait(&never()).await);
    assert!(
        failure.retry_after_seconds > 0 && failure.retry_after_seconds <= 1,
        "want RetryAfterSeconds ~1s, got {}",
        failure.retry_after_seconds
    );
}

/// Port of `TestRateGateDripReleasesProbes`: inside the latch probes release
/// on the drip interval — free slot → admit; taken slot → fast-fail. The
/// probe is the only request reaching upstream during the limit.
#[tokio::test(start_paused = true)]
async fn RateGateDripReleasesProbes() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            drip_interval: Duration::from_millis(50),
            ..GateConfig::default()
        },
        10.0,
    );
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 30 seconds.",
    ));
    // The first slot opens `drip_interval` after latching; earlier
    // arrivals fast-fail.
    gate_failure(gate.wait(&never()).await);
    advance(&clock, Duration::from_millis(60)).await;
    gate.wait(&never()).await.expect("drip slot should admit");
    // Slot taken — the next arrival fast-fails again.
    gate_failure(gate.wait(&never()).await);
    advance(&clock, Duration::from_millis(60)).await;
    gate.wait(&never())
        .await
        .expect("next drip slot should admit");
}

/// Port of `TestRateGateDripRespectsDeadZone`: no probes inside the dead
/// zone — a send near the bucket boundary can land in the adjacent real
/// upstream bucket and donate count.
#[tokio::test(start_paused = true)]
async fn RateGateDripRespectsDeadZone() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            drip_interval: Duration::from_millis(1),
            ..GateConfig::default()
        },
        10.0,
    );
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 60 seconds.",
    ));
    advance(&clock, Duration::from_secs(48)).await; // :58 — latched, dead zone, slot open
    gate_failure(gate.wait(&never()).await);
    advance(&clock, Duration::from_secs(5)).await; // :03 next minute — sendable again
    gate.wait(&never())
        .await
        .expect("sendable drip slot should admit");
}

/// Port of `TestRateGateUnlatchesOnUpstreamSuccess`: any upstream success
/// frame releases immediately — at the margin refusals are probabilistic
/// and a success frame proves the window passed.
#[tokio::test(start_paused = true)]
async fn RateGateUnlatchesOnUpstreamSuccess() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 30 seconds.",
    ));
    gate_failure(gate.wait(&never()).await);
    gate.note_upstream_success();
    gate.wait(&never())
        .await
        .expect("released gate should admit");
}

/// Port of `TestRateGateLatchSelective`: non-limit errors never latch; a new
/// latch only extends, never shortens.
#[tokio::test(start_paused = true)]
async fn RateGateLatchSelective() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&ConnectError::new(
        ErrorCode::InvalidArgument,
        "bad request",
    ));
    gate.wait(&never())
        .await
        .expect("no latch on non-limit error");
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 10 minutes.",
    ));
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 1 seconds.",
    ));
    let failure = gate_failure(gate.wait(&never()).await);
    // :10 + 10min → :20:10 → bucket :20 → :20:59 → 649s (max wins).
    assert!(
        failure.retry_after_seconds >= 590,
        "want latch ~600s, got {}",
        failure.retry_after_seconds
    );
}

/// Port of `TestRateGateZeroSecondHint`: "reset in 0 seconds" declares the
/// bucket boundary arrived (new bucket already blown, no extra ban) —
/// latched it neither extends the deadline nor re-arms the drip clock;
/// unlatched it latches to `now` and expires on arrival. Neither form
/// falls back to the 60s default latch.
#[tokio::test(start_paused = true)]
async fn RateGateZeroSecondHint() {
    let (gate, clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 30 seconds.",
    ));
    let latched_until = gate.stats().limited_until;
    advance(&clock, Duration::from_secs(5)).await;
    gate.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 0 seconds.",
    ));
    assert_eq!(
        gate.stats().limited_until,
        latched_until,
        "0-hint while latched must not extend the latch"
    );
    // The drip clock must not re-arm either: the original slot (armed at
    // latch + 8s = :18) still admits a probe at :19.
    advance(&clock, Duration::from_secs(4)).await;
    gate.wait(&never())
        .await
        .expect("original drip slot should still admit");

    // Unlatched: a 0-hint latches to `now` and expires at once.
    let (fresh, _clock) = pinned_gate(GateConfig::default(), 10.0);
    fresh.note_upstream_error(&rate_limit_err(
        "Reached overall message rate limit. Your limit will reset in 0 seconds.",
    ));
    fresh
        .wait(&never())
        .await
        .expect("0-hint latch expires at arrival");
    assert!(!fresh.stats().latched, "0-hint latch must expire cleanly");
}

/// Port of `TestRateGateIgnoresTransportMasquerade`: `http2`
/// `ENHANCE_YOUR_CALM` maps to `resource_exhausted` — a transport event, not
/// an upstream limit; it must not arm the latch.
#[tokio::test(start_paused = true)]
async fn RateGateIgnoresTransportMasquerade() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 10.0);
    gate.note_upstream_error(&ConnectError::new(
        ErrorCode::ResourceExhausted,
        "bandwidth exhausted: stream error: stream ID 5; ENHANCE_YOUR_CALM; received from peer",
    ));
    let stats = gate.stats();
    assert!(!stats.latched && stats.latch_count == 0);
    gate.wait(&never())
        .await
        .expect("masqueraded error must not latch");
}

/// Port of `TestRateGateWindowQuotaReject`: once the bucket's admissions hit
/// quota, requests sleep to the next window; a predicted wait beyond
/// `max_hold` rejects locally instead of sending upstream debt.
#[tokio::test(start_paused = true)]
async fn RateGateWindowQuotaReject() {
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 2,
            ..GateConfig::default()
        },
        10.0,
    );
    for i in 0..2 {
        gate.wait(&never())
            .await
            .unwrap_or_else(|_| panic!("wait {i}"));
    }
    let failure = gate_failure(gate.wait(&never()).await);
    // Quota exhausted at :10 → next window opens at :02+60 → ~52s.
    assert_eq!(failure.retry_after_seconds, 52);
}

/// Port of `TestRateGateWindowRollover`: the counting bucket rolls with the
/// window boundary — last bucket's usage does not carry over.
#[tokio::test(start_paused = true)]
async fn RateGateWindowRollover() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            ..GateConfig::default()
        },
        10.0,
    );
    gate.wait(&never()).await.expect("first wait");
    gate_failure(gate.wait(&never()).await);
    advance(&clock, Duration::from_secs(60)).await; // same second, next bucket
    gate.wait(&never()).await.expect("new bucket admits");
}

/// Port of `TestRateGateDeadZoneSleepsToNextWindow`: a dead-zone request
/// sleeps to the next window open instead of fast-failing — a wait inside
/// `max_hold` is worth sleeping.
#[tokio::test(start_paused = true)]
async fn RateGateDeadZoneSleepsToNextWindow() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            ..GateConfig::default()
        },
        58.9,
    );
    let token = never();
    let waiter = tokio::spawn({
        let gate = Arc::clone(&gate);
        let token = token.clone();
        async move { gate.wait(&token).await }
    });
    wait_for_waiters(&gate, 1).await;
    advance(&clock, Duration::from_millis(100)).await; // :59.0 → next window :02
    advance(&clock, Duration::from_secs(3)).await;
    waiter
        .await
        .expect("join")
        .expect("admitted after sleeping to window open");
    assert_eq!(gate.stats().waiters, 0);
}

/// Port of `TestRateGateDeadZoneFastFails`: a dead-zone wait beyond
/// `max_hold` fast-fails with `Retry-After` to the next window.
#[tokio::test(start_paused = true)]
async fn RateGateDeadZoneFastFails() {
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            max_hold: Duration::from_secs(1),
            ..GateConfig::default()
        },
        58.5,
    );
    let failure = gate_failure(gate.wait(&never()).await);
    // Dead-zone head → next window ~3.5s → ceil 4.
    assert_eq!(failure.retry_after_seconds, 4);
}

/// Port of `TestRateGateZeroQuotaUnlimited`: `quota <= 0` disables window
/// pacing — even the dead zone admits.
#[tokio::test(start_paused = true)]
async fn RateGateZeroQuotaUnlimited() {
    let (gate, _clock) = pinned_gate(GateConfig::default(), 59.0);
    for i in 0..3 {
        gate.wait(&never())
            .await
            .unwrap_or_else(|_| panic!("wait {i}"));
    }
}

/// Port of `TestRateGateWaitCancelRefunds`: a cancelled wait returns the
/// cancellation and refunds its `waiters` slot.
#[tokio::test(start_paused = true)]
async fn RateGateWaitCancelRefunds() {
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            ..GateConfig::default()
        },
        58.2,
    );
    let token = never();
    let waiter = tokio::spawn({
        let gate = Arc::clone(&gate);
        let token = token.clone();
        async move { gate.wait(&token).await }
    });
    wait_for_waiters(&gate, 1).await;
    token.cancel();
    let err = waiter.await.expect("join").expect_err("cancelled wait");
    assert!(matches!(err, WaitError::Cancelled));
    assert_eq!(gate.stats().waiters, 0);
}

/// Port of `TestRetryAfterParsesMinutes`.
#[test]
fn RetryAfterParsesMinutes() {
    for (text, want) in [
        ("Your limit will reset in 32 seconds.", 32),
        ("Your limit will reset in 1 minute.", 60),
        ("Your limit will reset in 8 minutes.", 480),
    ] {
        assert_eq!(
            failure::classify_text(text).retry_after_seconds,
            want,
            "classify_text({text:?})"
        );
    }
    assert_eq!(
        failure::classify_text("no hint here").retry_after_seconds,
        0
    );
    assert!(
        rate_limit_err("reset in 5 seconds.")
            .to_string()
            .contains("resource_exhausted")
    );
}

/// Port of `TestRateLimitResetBucketAlignsMinutes`: minute hints align up to
/// the `:59` bucket boundary (the upstream's minute-bucket remainder is
/// floor-rounded); second hints land exactly.
#[test]
fn RateLimitResetBucketAlignsMinutes() {
    // Fixed instant at :12 of a minute — Go's Local-zone date is replaced
    // by the epoch minute base (minute buckets are zone-independent).
    let base = SystemTime::UNIX_EPOCH + Duration::from_mins(1_000);
    let now = base + Duration::from_secs(12);
    let reset = failure::classify_text("Your limit will reset in 1 minute.")
        .rate_limit_reset(now)
        .expect("minute hint parses");
    // now+1min = :12 of the next bucket → that bucket's :59.
    assert_eq!(reset, base + Duration::from_secs(119));
    // Target already past :59 → the next minute's boundary.
    let reset = failure::classify_text("Your limit will reset in 1 minute.")
        .rate_limit_reset(base + Duration::from_millis(59_500))
        .expect("minute hint parses");
    assert_eq!(reset, base + Duration::from_secs(179));
    // "0 minutes" is an explicit declaration, not a missing hint: minute
    // granularity 0 is the floor-rounded remainder → this bucket's :59.
    let reset = failure::classify_text("Your limit will reset in 0 minutes.")
        .rate_limit_reset(now)
        .expect("zero-minute hint parses");
    assert_eq!(reset, base + Duration::from_secs(59));
    let reset = failure::classify_text("Your limit will reset in 1 minute.")
        .rate_limit_reset(base + Duration::from_secs(1))
        .expect("minute hint parses");
    assert_eq!(reset, base + Duration::from_secs(119));
    // Second hints land exactly.
    let reset = failure::classify_text("Your limit will reset in 30 seconds.")
        .rate_limit_reset(now)
        .expect("seconds hint parses");
    assert_eq!(reset, now + Duration::from_secs(30));
}

/// Port of `TestRateGateLatchRanges`: the event ring replays in write order —
/// latch opens a range, extension advances the right edge, release/expiry
/// closes it; a live range ends at `now`, and a range whose opener rolled
/// out reports `start: None`.
#[tokio::test(start_paused = true)]
async fn RateGateLatchRanges() {
    let (gate, clock) = pinned_gate(GateConfig::default(), 10.0);
    // Latch 1: latch → extend → early release → one [latch_at, release_at]
    // range whose right edge is the release instant, not the extended
    // deadline.
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 1 minutes.",
    ));
    let latch_at = clock.now();
    advance(&clock, Duration::from_secs(5)).await;
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 3 minutes.",
    ));
    advance(&clock, Duration::from_secs(5)).await;
    let release_at = clock.now();
    gate.note_upstream_success();
    let ranges = gate.stats().latch_ranges;
    assert_eq!(ranges.len(), 1, "latch→release ranges: {ranges:?}");
    assert_eq!(ranges[0].start, Some(latch_at));
    assert_eq!(ranges[0].end, release_at);
    // Latch 2: latch then natural expiry — `expire_if_due` inside `stats`
    // appends `expired`, closing the range at the deadline, not `now`.
    advance(&clock, Duration::from_secs(60)).await;
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 2 minutes.",
    ));
    let second_latch_at = clock.now();
    advance(&clock, Duration::from_secs(180)).await; // past the deadline
    let stats = gate.stats();
    assert_eq!(
        stats.latch_ranges.len(),
        2,
        "ranges: {:?}",
        stats.latch_ranges
    );
    // Minute hint aligns to :59: deadline = (now+2min)'s bucket :59.
    let base_secs = second_latch_at
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post-epoch")
        .as_secs();
    let want_end = SystemTime::UNIX_EPOCH + Duration::from_secs(base_secs - base_secs % 60 + 179);
    assert_eq!(stats.latch_ranges[1].start, Some(second_latch_at));
    assert_eq!(stats.latch_ranges[1].end, want_end);
    // Latch 3: still live with its opener visible → last range ends at
    // `now` and starts at the latch instant.
    advance(&clock, Duration::from_secs(60)).await;
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 5 minutes.",
    ));
    let third_latch_at = clock.now();
    advance(&clock, Duration::from_secs(30)).await;
    let stats = gate.stats();
    let last = stats.latch_ranges.last().expect("open range");
    assert_eq!(last.start, Some(third_latch_at));
    assert_eq!(last.end, clock.now());
}

/// Port of `TestRateGateLatchPersistRestore`: latching writes the state
/// file; a fresh instance (restart) restores an unexpired latch so it does
/// not bare-send and extend the upstream limit; release removes the file;
/// an expired file is ignored and removed.
#[tokio::test(start_paused = true)]
async fn RateGateLatchPersistRestore() {
    let dir = work_dir("persist");
    let path = dir.join("gate-state.json");
    let clock = FakeClock {
        t: Arc::new(Mutex::new(minute_second(10.0))),
    };
    let gate_clock = clock.clone();
    let gate = Gate::with_clock(
        GateConfig {
            max_rpm: 60,
            ..GateConfig::default()
        },
        Some(path.clone()),
        Box::new(move || gate_clock.now()),
    );
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 8 minutes.",
    ));
    assert!(path.exists(), "state file not written");

    let gate_clock = clock.clone();
    let restarted = Gate::with_clock(
        GateConfig {
            max_rpm: 60,
            ..GateConfig::default()
        },
        Some(path.clone()),
        Box::new(move || gate_clock.now()),
    );
    let stats = restarted.stats();
    assert!(stats.latched && stats.limited_until.is_some());
    restarted
        .wait(&never())
        .await
        .expect_err("restored latch should keep rejecting");

    restarted.note_upstream_success();
    assert!(!path.exists(), "release should remove the state file");
    assert!(!restarted.stats().latched);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Port of `TestRateGateStateExpiredIgnored`.
#[tokio::test(start_paused = true)]
async fn RateGateStateExpiredIgnored() {
    let dir = work_dir("expired");
    let path = dir.join("gate-state.json");
    let past = SystemTime::now() - Duration::from_secs(60);
    // The Go test marshals `gateStateFile` — write the same RFC3339 shape.
    let secs = past
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("post-epoch")
        .as_secs();
    std::fs::write(
        &path,
        format!("{{\"limited_until\":\"{}\"}}", rfc3339_utc(secs)),
    )
    .expect("write state");
    let gate = Gate::new(GateConfig::default(), Some(path.clone()));
    assert!(!gate.stats().latched, "expired state must not latch");
    assert!(!path.exists(), "expired state file should be removed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Port of `TestRateGateSetParamsPreservesLatch`.
#[tokio::test(start_paused = true)]
async fn RateGateSetParamsPreservesLatch() {
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 60,
            ..GateConfig::default()
        },
        10.0,
    );
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 8 minutes.",
    ));
    gate.set_params(GateConfig {
        max_rpm: 30,
        ..GateConfig::default()
    });
    let stats = gate.stats();
    assert!(stats.latched, "set_params must preserve the latch");
    assert_eq!(stats.window_quota, 30);
    gate.set_params(GateConfig::default());
    assert_eq!(gate.stats().window_quota, 0, "quota=0 disables pacing");
}

// ---------------------------------------------------------------------------
// QA scenarios (plan task 10)
// ---------------------------------------------------------------------------

/// QA failure case `quota_exhaustion_cancel_and_corrupt_state`: quota
/// exhaustion rejects with a bounded `Retry-After`; a waiter cancelled at
/// the same instant its timer fires resolves to cancellation without
/// consuming an admission; a corrupt state file is treated as no latch and
/// removed — the source-equivalent invalid-state handling.
#[tokio::test(start_paused = true)]
async fn quota_exhaustion_cancel_and_corrupt_state() {
    // Quota exhaustion → local-gate rejection, no admission consumed.
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            ..GateConfig::default()
        },
        10.0,
    );
    gate.wait(&never()).await.expect("first admission");
    let failure = gate_failure(gate.wait(&never()).await);
    assert!(failure.local_gate && failure.retry_after_seconds > 0);
    assert_eq!(
        gate.stats().window_used,
        1,
        "rejections never consume quota"
    );

    // Simultaneous cancel/timer: the waiter is asleep on a timer due at
    // the next window; cancel and fire the timer in the same step. Either
    // select arm must resolve to cancellation — the loop-head re-check
    // covers a winning timer — and no admission is consumed.
    for _ in 0..8 {
        let (gate, clock) = pinned_gate(
            GateConfig {
                max_rpm: 1,
                ..GateConfig::default()
            },
            58.0,
        );
        let token = never();
        let waiter = tokio::spawn({
            let gate = Arc::clone(&gate);
            let token = token.clone();
            async move { gate.wait(&token).await }
        });
        wait_for_waiters(&gate, 1).await;
        token.cancel();
        advance(&clock, Duration::from_secs(4)).await; // fires the :02 timer
        let err = waiter.await.expect("join").expect_err("cancelled wait");
        assert!(matches!(err, WaitError::Cancelled));
        let stats = gate.stats();
        assert_eq!(stats.waiters, 0);
        assert_eq!(stats.window_used, 0, "cancelled wait must not consume");
    }

    // Corrupt state file → no latch, file removed (same handling as an
    // expired one).
    let dir = work_dir("corrupt");
    let path = dir.join("gate-state.json");
    std::fs::write(&path, "{not json").expect("write corrupt state");
    let gate = Gate::new(GateConfig::default(), Some(path.clone()));
    assert!(!gate.stats().latched, "corrupt state must not latch");
    assert!(!path.exists(), "corrupt state file should be removed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// QA happy case: controlled window rollover and drip recovery preserve
/// counts — admissions, probes, rejections and waiters stay exact across
/// the boundary.
#[tokio::test(start_paused = true)]
async fn window_rollover_drip_recovery_preserves_counts() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            max_rpm: 3,
            drip_interval: Duration::from_secs(5),
            ..GateConfig::default()
        },
        10.0,
    );
    // Fill the bucket, then roll the window: usage resets, quota does not
    // carry over.
    gate.wait(&never()).await.expect("admit 1");
    gate.wait(&never()).await.expect("admit 2");
    gate.wait(&never()).await.expect("admit 3");
    gate_failure(gate.wait(&never()).await);
    advance(&clock, Duration::from_secs(55)).await; // :05 next minute
    let stats = gate.stats();
    assert_eq!(stats.window_used, 0, "rollover resets the bucket");
    assert_eq!(stats.reject_hold_count, 1);
    gate.wait(&never()).await.expect("new bucket admits");
    assert_eq!(gate.stats().window_used, 1);

    // Latch, then drip through recovery: probes count as drips and bucket
    // usage; a success frame releases and normal quota accounting resumes.
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 30 seconds.",
    ));
    advance(&clock, Duration::from_secs(5)).await; // drip slot opens
    gate.wait(&never()).await.expect("probe admits");
    let stats = gate.stats();
    assert_eq!(stats.drip_count, 1);
    assert_eq!(stats.window_used, 2, "probe counts against the bucket");
    gate.note_upstream_success();
    gate.wait(&never()).await.expect("released admits");
    let stats = gate.stats();
    assert!(!stats.latched);
    assert_eq!(stats.window_used, 3);
    assert_eq!(stats.latch_count, 1);
    assert_eq!(stats.reject_latched_count, 0);
}

/// Acceptance: every admitted send consumes exactly one unit of bucket
/// quota; rejections consume none.
#[tokio::test(start_paused = true)]
async fn every_send_consumes_one_admission() {
    let (gate, _clock) = pinned_gate(
        GateConfig {
            max_rpm: 3,
            ..GateConfig::default()
        },
        10.0,
    );
    for i in 1..=3 {
        gate.wait(&never()).await.expect("admission");
        assert_eq!(gate.stats().window_used, i, "admission {i} consumes one");
    }
    gate_failure(gate.wait(&never()).await);
    assert_eq!(gate.stats().window_used, 3, "rejection consumes nothing");
}

/// Approved exception 4: a latched drip probe also obeys the configured
/// window quota — a free drip slot in a full bucket fast-fails instead of
/// donating count to the upstream bucket.
#[tokio::test(start_paused = true)]
async fn drip_probe_obeys_window_quota() {
    let (gate, clock) = pinned_gate(
        GateConfig {
            max_rpm: 1,
            drip_interval: Duration::from_secs(5),
            ..GateConfig::default()
        },
        10.0,
    );
    gate.wait(&never()).await.expect("fill the bucket");
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 90 seconds.",
    ));
    advance(&clock, Duration::from_secs(5)).await; // drip slot open, bucket full
    let failure = gate_failure(gate.wait(&never()).await);
    assert!(failure.local_gate, "probe in a full bucket fast-fails");
    assert_eq!(gate.stats().drip_count, 0, "no probe was released");
    // After rollover the latch still holds (until :11:40) and the drip
    // slot is still open (it was never consumed): the probe admits
    // against the fresh bucket.
    advance(&clock, Duration::from_secs(55)).await;
    gate.wait(&never())
        .await
        .expect("probe admits after rollover");
    assert_eq!(gate.stats().drip_count, 1);
    assert_eq!(gate.stats().window_used, 1);
}

// ---------------------------------------------------------------------------
// D3 regression: gate RFC3339 timestamps must be real instants
// ---------------------------------------------------------------------------

/// Fixed instant `secs` after the Unix epoch — test timestamps stay in
/// epoch seconds, the natural unit for known-instant assertions.
fn epoch_plus(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// A Go-written `gate-state.json` (real 2026 `limited_until`, the shape
/// `json.Marshal(gateStateFile)` emits) restores the latch on startup —
/// the missing `- 719_468` overflowed the parse, so the file was treated
/// as corrupt and deleted, silently dropping the latch on Go→Rust
/// migration.
#[tokio::test(start_paused = true)]
async fn go_written_state_file_restores_latch() {
    let dir = work_dir("go-state");
    let path = dir.join("gate-state.json");
    // Go `time.Time` JSON output, including a fractional form.
    std::fs::write(&path, "{\"limited_until\":\"2026-09-22T16:30:00Z\"}")
        .expect("write Go-format state");
    let clock = FakeClock {
        // Pinned at 2026-09-22T16:09:00Z — inside the latch window.
        t: Arc::new(Mutex::new(epoch_plus(1_790_093_340))),
    };
    let gate_clock = clock.clone();
    let gate = Gate::with_clock(
        GateConfig::default(),
        Some(path.clone()),
        Box::new(move || gate_clock.now()),
    );
    let stats = gate.stats();
    assert!(stats.latched, "Go-written latch must restore");
    assert_eq!(stats.limited_until, Some(epoch_plus(1_790_094_600)));
    assert!(
        path.exists(),
        "a valid Go state file must not be deleted as corrupt"
    );
    let events = serde_json::to_value(&stats.events).expect("events serialize");
    assert_eq!(events[0]["kind"], "restored");
    assert_eq!(events[0]["at"], "2026-09-22T16:09:00Z");
    assert_eq!(events[0]["until"], "2026-09-22T16:30:00Z");
    gate.wait(&never())
        .await
        .expect_err("restored latch should keep rejecting");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A Rust-written `gate-state.json` must carry a real RFC3339 timestamp
/// Go's `time.Time` unmarshal accepts — the missing `+ 719_468` wrote
/// year-0056 deadlines that Go read as expired, losing the latch on
/// Rust→Go rollback. Asserts the absolute string, not a round-trip.
#[tokio::test(start_paused = true)]
async fn rust_written_state_file_is_go_parseable() {
    let dir = work_dir("rust-state");
    let path = dir.join("gate-state.json");
    let clock = FakeClock {
        t: Arc::new(Mutex::new(epoch_plus(1_790_093_340))),
    };
    let gate_clock = clock.clone();
    let gate = Gate::with_clock(
        GateConfig::default(),
        Some(path.clone()),
        Box::new(move || gate_clock.now()),
    );
    gate.note_upstream_error(&rate_limit_err(
        "rate limited. Your limit will reset in 30 seconds.",
    ));
    let body = std::fs::read_to_string(&path).expect("state file written");
    assert_eq!(
        body, "{\"limited_until\":\"2026-09-22T16:09:30Z\"}",
        "state file must hold the real instant"
    );
    // The stats payload's timestamps are the same code path: window and
    // event times must render the real 2026 wall clock, not year 0056.
    let stats = serde_json::to_value(gate.stats()).expect("stats serialize");
    assert_eq!(stats["limited_until"], "2026-09-22T16:09:30Z");
    assert_eq!(stats["events"][0]["at"], "2026-09-22T16:09:00Z");
    assert_eq!(stats["events"][0]["until"], "2026-09-22T16:09:30Z");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The panel's `gate.window_open`/`window_next` fields render real
/// wall-clock minutes (the defect surfaced as `"0056-11-20T16:09:02Z"`).
#[tokio::test(start_paused = true)]
async fn stats_window_timestamps_are_real_instants() {
    let clock = FakeClock {
        // Pinned at 2026-09-22T16:09:10Z — inside the sendable window, so
        // `window_open` is this minute's boundary + the 2s guard.
        t: Arc::new(Mutex::new(epoch_plus(1_790_093_350))),
    };
    let gate_clock = clock.clone();
    let gate = Gate::with_clock(
        GateConfig {
            max_rpm: 90,
            ..GateConfig::default()
        },
        None,
        Box::new(move || gate_clock.now()),
    );
    let stats = serde_json::to_value(gate.stats()).expect("stats serialize");
    assert_eq!(stats["window_open"], "2026-09-22T16:09:02Z");
    assert_eq!(stats["window_next"], "2026-09-22T16:10:02Z");
}

/// `SystemTime` seconds → `"YYYY-MM-DDTHH:MM:SSZ"` for hand-written state
/// fixtures (mirrors Go `time.Time` `JSON` output).
fn rfc3339_utc(secs: u64) -> String {
    let days = secs / 86_400;
    let sod = secs % 86_400;
    let (y, m, d) = civil(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

// Civil-from-days math mirrors Go's date conversion (Hinnant's
// `civil_from_days`): `+ 719_468` shifts the epoch-based day count to the
// algorithm's 0000-03-01 day zero. The i64/u64 casts are the same
// wrapping conversions Go performs.
#[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
fn civil(days: u64) -> (u64, u64, u64) {
    let days = days.cast_signed() + 719_468;
    let era = days.div_euclid(146_097);
    let doe = days.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (
        (if month <= 2 { year + 1 } else { year }) as u64,
        month as u64,
        day as u64,
    )
}

//! Bounded streaming, retries and cancellation — the stream owner.
//!
//! Port of `G/internal/adapter/devin/devin.go` `Adapter.Stream`,
//! `getChatMessageWithRetry`, `pumpUpstream`, `responseStream` and
//! `isTransientConnectError` (devin.go:320-601, 991-1315), composed over
//! the catalog (task 7), request projection (task 8), response decoder
//! (task 9), transport (task 6) and rate gate (task 10).
//!
//! Lifecycle states: connecting (`get_chat_message_with_retry`), open with
//! no semantic output (start withheld by `start_hold`, pre-content reopen
//! still legal), streaming (`produced_events`), stop-tail (`has_stop_reason`
//! shrinks the silence watchdog to `TAIL_GRACE`), terminal (`finished`).
//!
//! Retry limits nest exactly as in Go:
//!   - connect phase: up to [`MAX_CONNECT_ATTEMPTS`] sends for transient
//!     transport breaks, each admitted by the gate;
//!   - connect phase: one extra rebuild+send when the first failure is
//!     `unauthenticated` and a newer token generation exists;
//!   - stream phase: one pre-content reopen (`retried`), for a transport
//!     break, an `unauthenticated` with a newer token, a watchdog kill, or
//!     the empty-`end_turn` continuation rebuild.
//!
//! No replay after a semantic event: `produced_events` bars `try_reopen`.
//!
//! Cancellation: one lineage token reaches the gate, the backoff sleep,
//! the connect call and the consumer's `recv`; a cancelled wait never
//! sends. Backoff runs BEFORE gate admission (approved exception 3) and
//! cancellation is rechecked immediately before every actual send.
//!
//! No pump task: `ServerStream::message` is cancel-safe (receive state
//! lives in the stream's buffer, the terminal record replays from
//! `self.end`), so the consumer polls it directly — Go's `pumpUpstream`
//! goroutine + channel is a task hop Rust does not need. A watchdog kill
//! or client disconnect drops the stream, which closes the exchange.
//! Reopen connects run in a spawned task tracked by `pending_reopen` so
//! a dropped `recv` poll can never duplicate a resend.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use connectrpc::ConnectError;
use connectrpc::client::ServerStream;
use devin_proto::generated::exa::api_server_pb as pb;
use tokio::task::JoinHandle;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::debuglog::{self, LogValue, Recorder};
use crate::domain::failure::{
    Canceled, DeadlineExceeded, Failure, classify, is_connect_stream_break,
    is_http2_transport_error, is_idle_conn_closed_error,
};
use crate::domain::request::{Content, Message, RequestMessages, TextContent, UserMessage};
use crate::domain::response::{ResponseEvent, ResponseEventType, ResponseStream, StopReason};
use crate::upstream::catalog::{Adapter, is_unauthenticated};
use crate::upstream::gate::WaitError;
use crate::upstream::request::{CallBinding, ClientIdentity, build_request};
use crate::upstream::response::{ResponseDecoder, custom_tool_names};
use crate::upstream::sanitize::sanitize_request;

/// `upstreamStallTimeout` — the longest silence allowed between upstream
/// frames; expiry judges the transport dead (half-open connection, hung
/// upstream) and finishes as a transport error instead of waiting forever.
/// Chosen above the observed first-frame latency (long thinking reaches
/// 45s+).
pub const STALL_TIMEOUT: Duration = Duration::from_secs(120);

/// `upstreamNoProgressTimeout` — the "no content progress" bound: any
/// frame (including upstream latency liveness frames) feeds the stall
/// watchdog, but only event-producing frames feed this one. Observed
/// healthy content-frame gaps reach ~60s, while a degenerate upstream can
/// send zero-event frames forever — 10min is a 10x-margin backstop.
pub const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(600);

/// `upstreamTailGrace` — the grace for terminal frames after a stop
/// reason was consumed. Healthy tail frames (usage/dim/endstream) arrive
/// <1ms after stopReason; a client stack that drains the body waiting for
/// transport EOF would hang on the watchdog when upstream never closes —
/// once semantic content is complete, finish normally instead of waiting.
pub const TAIL_GRACE: Duration = Duration::from_secs(15);

/// `startHoldTimeout` — the longest the `start` event
/// (`message_start`/`response.created`) may be withheld. The hold gives
/// "fails before producing content" a window to return a real HTTP status
/// — observed failures all settle within ~9s — while downstream clients
/// drop at ~30s of silence and intermediate gateways only pass bytes
/// after the first protocol event (keepalive comment lines do not count).
/// 15s sits between: fast failures still get a real status, long thinking
/// releases `start` first so the client stays alive.
pub const START_HOLD_TIMEOUT: Duration = Duration::from_secs(15);

/// `maxConnectAttempts` — the cap on `GetChatMessage` establishment-phase
/// retries for transient transport errors (EOF / connection reset /
/// timeout).
const MAX_CONNECT_ATTEMPTS: u32 = 3;

/// Cancellation token shared across one upstream attempt lineage.
pub type Cancel = CancellationToken;

/// Boxed error carrier for connect-phase results: the raw error is kept
/// (not flattened into `Failure`) so `is_transient_connect_error` /
/// `is_unauthenticated` still see the real chain — Go returns `error`.
type BoxedError = Box<dyn Error + Send + Sync>;

/// The live upstream stream for the current attempt (Go's `stream` —
/// reqwest body + connect envelope decoder). `message()` is cancel-safe:
/// partial frames stay in its buffer and the terminal record replays, so
/// the consumer polls it directly and a dropped poll loses nothing.
/// `None` once the terminal record is consumed or the attempt is killed —
/// dropping it mid-stream is the kill (Go's `stream.cancel`).
type UpstreamStream = ServerStream<
    crate::upstream::transport::CheckedResponseBody,
    pb::GetChatMessageResponseView<'static>,
>;

/// A pre-content resend in flight. The connect runs in a spawned task so
/// the `JoinHandle` — not a `recv`-local future — owns it: dropping a
/// `recv` poll mid-reopen leaves the resend alive and resumable instead
/// of silently re-sending the request (exactly-once upstream sends).
struct PendingReopen {
    /// The original terminal cause — needed for the finish path when the
    /// resend fails (Go returns the original error, not the resend's).
    cause: Option<BoxedError>,
    /// The spawned `get_chat_message_with_retry`: gate wait, backoff and
    /// send all answer to the lineage token inside it.
    handle: JoinHandle<Result<UpstreamStream, BoxedError>>,
}

fn normalize_wire_error(error: ConnectError) -> ConnectError {
    let message = error.message.as_deref().unwrap_or_default();
    if let Some(rest) = message.strip_prefix("message size ")
        && let Some((promised, _)) = rest.split_once(" exceeds limit ")
        && promised.parse::<u32>().is_ok()
    {
        return ConnectError::new(
            connectrpc::ErrorCode::InvalidArgument,
            format!("protocol error: promised {promised} bytes in enveloped message, got 0 bytes"),
        );
    }
    let Some(start) = message.find("protocol error:") else {
        return error;
    };
    let protocol = &message[start..];
    let code = if protocol == "protocol error: unexpected EOF" {
        connectrpc::ErrorCode::Internal
    } else {
        connectrpc::ErrorCode::InvalidArgument
    };
    ConnectError::new(code, protocol)
}

/// Locally synthesized stream errors (watchdog kills): Go
/// builds these with `fmt.Errorf`, and a plain error is transient under
/// `isTransientConnectError` (no `connect.Error` in the chain). Using a
/// dedicated type — not `Failure` — keeps that classification: `Failure`
/// is the producer-classified record and short-circuits the probe.
#[derive(Debug)]
struct StreamFault(String);

impl fmt::Display for StreamFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for StreamFault {}

/// `isTransientConnectError` — whether the error is a transport-layer
/// break (retryable, recorded `devin_transport`). The judgement looks at
/// the unwrap chain for io/net errors and the connect/http2 stack's fixed
/// wording, not at the code: connect wraps RoundTrip/read-write breaks as
/// `unavailable`, truncated envelope frames as `invalid_argument`
/// "protocol error: ...", a bare mid-stream EOF as `internal` "ended
/// without `END_STREAM` envelope", and peer `RST_STREAM`/`GOAWAY` into semantic
/// codes — while a `ConnectError` with no transport source is an upstream
/// semantic refusal (fixed `unavailable` template, `invalid_argument`
/// parameters, `resource_exhausted`, `permission_denied`) where a retry
/// only reproduces the failure.
///
/// Rust port notes: `Failure` is transparent when it carries a `cause`
/// (the port wraps classified records around the original error, e.g.
/// `AssignModel(uid): ...`); a cause-less `Failure` is a producer-made
/// semantic record (local gate rejection, local validation) and
/// short-circuits like Go's `errors.As` hit. `reqwest::Error` is the
/// `net.Error` equivalent; `is_timeout` maps to the deadline branch.
pub fn is_transient_connect_error(err: &(dyn Error + 'static)) -> bool {
    // Phase 1 — Go's chain-wide probes: caller cancellation is not a
    // transport fault (a client disconnect/timeout must not be misjudged
    // as a retryable break, nor recorded `devin_transport`); io/net
    // errors anywhere in the chain are transport breaks.
    let mut current = Some(err);
    while let Some(err) = current {
        if err.is::<Canceled>()
            || err.is::<DeadlineExceeded>()
            || err.is::<tokio::time::error::Elapsed>()
        {
            return false;
        }
        if let Some(req_err) = err.downcast_ref::<reqwest::Error>()
            && req_err.is_timeout()
        {
            return false;
        }
        if let Some(failure) = err.downcast_ref::<Failure>()
            && failure.cause.is_none()
        {
            // A producer-classified record with no deeper chain (gate
            // rejection, local validation) is a semantic refusal.
            return false;
        }
        if err.is::<io::Error>() || err.is::<reqwest::Error>() {
            return true;
        }
        current = err.source();
    }
    // Phase 2 — Go's `errors.As(&connectErr)`: the first ConnectError in
    // the chain decides by wording; no ConnectError at all means a plain
    // transport error (transient).
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(connect_err) = err.downcast_ref::<ConnectError>() {
            let message = connect_err.message.as_deref().unwrap_or_default();
            // http2 RST/GOAWAY wording precedes the code: the mapped code
            // (unavailable/resource_exhausted/internal/permission_denied)
            // has no relation to real semantics.
            if is_http2_transport_error(message) {
                return true;
            }
            // h1 pool shape (force_http1): reusing an idle connection the
            // peer already closed fails before any byte is written — same
            // class as RST/GOAWAY, safe to retry.
            if is_idle_conn_closed_error(message) {
                return true;
            }
            // Rust connectrpc's local frame-parse failures — the
            // equivalents of connect-go's "protocol error: ..." wording:
            // a truncated envelope sequence (mid-stream EOF) and a body
            // read break are transport breaks, not upstream semantics.
            if is_connect_stream_break(message) {
                return true;
            }
            return (connect_err.code == connectrpc::ErrorCode::InvalidArgument
                || connect_err.code == connectrpc::ErrorCode::Internal)
                && message.starts_with("protocol error:");
        }
        current = err.source();
    }
    true
}

/// Go `time.Duration.String()` for the whole-second timeouts this module
/// interpolates into watchdog error text ("2m0s", "10m0s").
fn go_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs.is_multiple_of(60) {
        format!("{}m0s", secs / 60)
    } else {
        format!("{secs}s")
    }
}

/// `recordProtoJSON` — log a proto message; `.jsonl` names append, others
/// write whole. Serialization is deferred into the log worker
/// (`LogValue::serde`, Go's `json.Marshaler` thunk) so the pump/decode
/// path never pays marshal cost. `recorder` may be `Recorder::none()`.
fn record_proto_json<M: serde::Serialize + Send + 'static>(
    recorder: &Recorder,
    name: &str,
    message: M,
) {
    if name.len() >= 6 && name[name.len() - 6..].eq_ignore_ascii_case(".jsonl") {
        recorder.append_value_jsonl(name, LogValue::serde(message));
        return;
    }
    recorder.write_json(name, LogValue::serde(message));
}

/// `emptyEndTurn` — whether `finish`'s events are "normal stop with zero
/// content": upstream occasionally ends directly with a stopReason and no
/// deltas. `StopSequence` does not count — zero content hitting a stop
/// sequence is more likely an intended truncation than a degenerate turn.
fn empty_end_turn(events: &[ResponseEvent]) -> bool {
    for event in events {
        if event.kind != ResponseEventType::Done {
            continue;
        }
        return event.message.as_ref().is_some_and(|message| {
            message.stop_reason == Some(StopReason::Stop) && message.content.is_empty()
        });
    }
    false
}

/// The owned upstream event stream — Go's `responseStream`. Single
/// consumer contract: `recv` is goroutine-local state (decoder, queue,
/// watchdogs), exactly like Go's `Recv`.
///
/// Terminal delivery is once-only: `finished` latches, the queue drains,
/// and `recv` then returns `Ok(None)` (Go's `io.EOF`).
#[allow(clippy::struct_excessive_bools)] // mirrors the Go struct's flags
pub struct DevinStream {
    adapter: Adapter,
    /// The request as sanitized/bound for the first send; `reopen`
    /// rebuilds from it (the empty-end_turn continuation appends a
    /// "continue" user message to a copy).
    request: RequestMessages,
    identity: ClientIdentity,
    /// Per-call binding for the current attempt; `token` is refreshed on
    /// credential repair.
    binding: CallBinding,
    /// Generation of `binding.token` — the repair rule's staleness key.
    token_generation: u64,
    /// Send counter (Go `attempt`): the first send is 1, every resend
    /// bumps it before logging `retry_attempt`.
    attempt: u32,
    recorder: Recorder,
    /// The request lineage token (Go `ctx`): consumer cancellation and
    /// reopen child tokens derive from it.
    cancel: CancellationToken,
    /// The live upstream stream; `None` after the terminal record is
    /// consumed or the attempt is killed (dropping it is the kill).
    stream: Option<UpstreamStream>,
    /// A pre-content resend in flight (spawned connect task); polled to
    /// completion inside `recv`'s wait loop so a dropped `recv` poll
    /// resumes it instead of duplicating the send.
    pending_reopen: Option<PendingReopen>,
    decoder: ResponseDecoder,
    /// `decoder.start()` was requested.
    started: bool,
    /// `start` events withheld until the first real event batch (or the
    /// hold expires).
    pending_start: Vec<ResponseEvent>,
    /// `start` was already delivered to the client: a retried stream's
    /// new start must be dropped or the client sees a second
    /// `message_start`.
    start_released: bool,
    start_hold: Option<Pin<Box<Sleep>>>,
    /// The decoder produced a terminal event; upstream is no longer read.
    finished: bool,
    /// Converted events awaiting the consumer.
    queue: VecDeque<ResponseEvent>,
    /// Any upstream frame produced events: content is flowing to the
    /// client, so failures can only pass through — no whole-request
    /// replay.
    produced_events: bool,
    /// Upstream produced a first non-error frame: the rate latch releases
    /// on it (at the margin a refusal is probabilistic; a success frame is
    /// evidence the window passed).
    upstream_confirmed: bool,
    /// One pre-content whole-request retry was already done (cap 1).
    retried: bool,
    /// Silence watchdog, reused across `recv` calls (Go `stream.stall`).
    stall: Option<Pin<Box<Sleep>>>,
    /// No-content-progress bound, reused across `recv` calls; only
    /// event-producing frames feed it — a degenerate upstream's
    /// zero-event liveness frames cannot keep the stream alive forever.
    progress: Option<Pin<Box<Sleep>>>,
}

#[allow(clippy::large_futures)] // async port of Go's goroutine/channel shape
impl DevinStream {
    /// `Recv` — the next intermediate response event. `Ok(None)` is end
    /// of stream (Go `io.EOF`); `Err` carries the classified failure.
    /// Cancellation of the lineage token makes a waiting `recv` return a
    /// cancel error and interrupts the pump's blocked receive.
    #[allow(clippy::too_many_lines)] // one-to-one port of Go's `Recv` loop
    async fn recv_inner(&mut self) -> Result<Option<ResponseEvent>, Failure> {
        // The silence timer lives on the stream across `recv` calls: it is
        // reset before every wait so it covers the inter-frame gap. When
        // converted events are already queued no wait happens — skip the
        // resets entirely (hot path: one timer op per event, not three).
        if self.queue.is_empty() && !self.finished {
            match &mut self.stall {
                Some(stall) => stall.as_mut().reset(Instant::now() + STALL_TIMEOUT),
                None => {
                    self.stall = Some(Box::pin(tokio::time::sleep(STALL_TIMEOUT)));
                }
            }
            // `progress` is structurally identical: reset on each wait
            // entry — the window covers "zero events while the consumer is
            // actively waiting". It is never stopped on return: a `Stop`
            // there would let zero-event frames after first content dodge
            // the watchdog and hang the stream forever.
            match &mut self.progress {
                Some(progress) => progress
                    .as_mut()
                    .reset(Instant::now() + NO_PROGRESS_TIMEOUT),
                None => {
                    self.progress = Some(Box::pin(tokio::time::sleep(NO_PROGRESS_TIMEOUT)));
                }
            }
        }
        while self.queue.is_empty() && !self.finished {
            if self.cancel.is_cancelled() {
                // Match the cancellation select arm even when the token
                // was already armed at loop entry. Returning directly here
                // made terminal delivery scheduler-dependent: cancellation
                // observed in `select!` produced one error event, while the
                // same cancellation observed here skipped it.
                self.kill_attempt();
                let events = self.decoder.finish(Some(&Canceled));
                let events = self.release(events);
                self.queue.extend(events);
                self.finished = true;
                continue;
            }
            if !self.started {
                // `start()` initializes `decoder.partial` and must run
                // before `decode`; the event itself is withheld in
                // `pending_start` to ship with the first real events.
                self.started = true;
                self.pending_start = self.decoder.start();
                if self.start_released {
                    // The client already saw a start on the retried
                    // stream; a second one violates the protocol.
                    self.pending_start = Vec::new();
                } else {
                    match &mut self.start_hold {
                        Some(hold) => hold.as_mut().reset(Instant::now() + START_HOLD_TIMEOUT),
                        None => {
                            self.start_hold =
                                Some(Box::pin(tokio::time::sleep(START_HOLD_TIMEOUT)));
                        }
                    }
                }
                continue;
            }
            // After stopReason only tail frames remain (observed <1ms):
            // the wait window shrinks from the silence watchdog to the
            // tail grace — a client stack draining the body for transport
            // EOF would otherwise stretch a normal ending into a stall.
            let stall_deadline = if self.decoder.has_stop_reason() {
                TAIL_GRACE
            } else {
                STALL_TIMEOUT
            };
            self.stall
                .as_mut()
                .expect("stall armed")
                .as_mut()
                .reset(Instant::now() + stall_deadline);
            // Observation-only synchronization: tests can pre-arm
            // cancellation after this receive wait is entered, rather than
            // racing the loop-head cancellation check.
            self.adapter.note_stream_receive_wait();
            // `biased`: the frame arm is polled first — on the hot path
            // (a frame already buffered) the timer and cancellation arms
            // never register a waker, saving wheel/lock ops per delta.
            // Cancellation is still observed at the loop head on the next
            // iteration, matching the select arm's outcome.
            tokio::select! {
                biased;
                frame = async {
                    match &mut self.stream {
                        Some(stream) => stream.message::<pb::GetChatMessageResponse>().await,
                        None => std::future::pending().await,
                    }
                }, if self.pending_reopen.is_none() => {
                    match frame {
                        Ok(Some(response)) => {
                            self.absorb_data(&response);
                        }
                        Ok(None) => {
                            self.resolve_end(None);
                        }
                        Err(err) => {
                            let err = normalize_wire_error(err);
                            self.resolve_end(Some(Box::new(err) as BoxedError));
                        }
                    }
                }
                outcome = async {
                    match &mut self.pending_reopen {
                        Some(pending) => (&mut pending.handle).await,
                        None => std::future::pending().await,
                    }
                }, if self.pending_reopen.is_some() => {
                    self.finish_reopen(outcome);
                }
                () = async {
                    match &mut self.start_hold {
                        Some(hold) => hold.as_mut().await,
                        None => std::future::pending().await,
                    }
                }, if self.pending_reopen.is_none() => {
                    // Go's timer channel delivers once; a `Sleep` stays
                    // ready after elapsing — disarm or the arm spins.
                    self.start_hold = None;
                    // Upstream silent past the hold: release the withheld
                    // start — to the client it is the first visible byte
                    // and every link's idle timer refreshes.
                    if self.pending_start.is_empty() {
                        continue;
                    }
                    self.queue.extend(self.pending_start.drain(..));
                    self.start_released = true;
                }
                () = self.stall.as_mut().expect("stall armed"), if self.pending_reopen.is_none() => {
                    // Upstream silence timeout: drop the stream to break
                    // the parked receive; buffered unconsumed frames are
                    // logged as evidence, then the stream ends as a
                    // transport error.
                    self.drain_frames();
                    self.stream = None;
                    if self.decoder.has_stop_reason() {
                        // Semantic content is complete; only the transport
                        // tail never arrived — finish as a normal EOF.
                        warn!("upstream held connection after stop reason; finishing after tail grace");
                        let events = self.decoder.finish(None);
                        let events = self.release(events);
                        if empty_end_turn(&events) && self.begin_reopen_empty() {
                            continue;
                        }
                        self.queue.extend(events);
                        self.finished = true;
                        continue;
                    }
                    let stall_err = StreamFault(format!(
                        "devin stream stalled: no frames for {}",
                        go_duration(STALL_TIMEOUT)
                    ));
                    if self.begin_reopen(&stall_err, false) {
                        continue;
                    }
                    self.record_upstream_failure(Some(&stall_err));
                    let events = self.decoder.finish(Some(&stall_err));
                    let events = self.release(events);
                    self.queue.extend(events);
                    self.finished = true;
                }
                () = self.progress.as_mut().expect("progress armed"), if self.pending_reopen.is_none() => {
                    // Frames flow but content progress has been zero for
                    // the whole window (upstream latency liveness frames
                    // do not count): the degenerate-shape backstop —
                    // pre-content may resend whole, post-content ends as
                    // a transport error.
                    self.drain_frames();
                    self.stream = None;
                    let progress_err = StreamFault(format!(
                        "devin stream made no progress for {}",
                        go_duration(NO_PROGRESS_TIMEOUT)
                    ));
                    if self.begin_reopen(&progress_err, false) {
                        continue;
                    }
                    self.record_upstream_failure(Some(&progress_err));
                    let events = self.decoder.finish(Some(&progress_err));
                    let events = self.release(events);
                    self.queue.extend(events);
                    self.finished = true;
                }
                () = self.cancel.cancelled() => {
                    self.kill_attempt();
                    let events = self.decoder.finish(Some(&Canceled));
                    let events = self.release(events);
                    self.queue.extend(events);
                    self.finished = true;
                }
            }
        }
        if self.finished {
            self.kill_attempt();
        }
        if let Some(event) = self.queue.pop_front() {
            return Ok(Some(event));
        }
        Ok(None)
    }

    /// Absorb one data frame: gate release, deferred proto logging,
    /// decode, progress feed and queueing — the synchronous half of the
    /// pump receive arm, shared by `recv` and `try_recv_event`.
    fn absorb_data(&mut self, response: &connectrpc::StreamMessage<pb::GetChatMessageResponse>) {
        if !self.upstream_confirmed {
            self.upstream_confirmed = true;
            self.adapter.gate().note_upstream_success();
        }
        // The disabled recorder discards the deferred thunk: skip the
        // per-frame owned materialization it would carry (hot path).
        if self.recorder.is_active() {
            record_proto_json(
                &self.recorder,
                debuglog::STAGE_DEVIN_RESPONSE,
                response.to_owned_message(),
            );
        }
        let events = self.decoder.decode(response.view());
        if !events.is_empty() {
            self.produced_events = true;
            if let Some(progress) = &mut self.progress {
                progress
                    .as_mut()
                    .reset(Instant::now() + NO_PROGRESS_TIMEOUT);
            }
        }
        let events = self.release(events);
        self.queue.extend(events);
        self.finished = self.decoder.is_finished();
    }

    /// Resolve the stream's terminal record: reopen when the failure is
    /// still replayable, else finish the decoder and queue the terminal
    /// events. Synchronous — the resend's connect runs in the spawned
    /// `pending_reopen` task, so this is safe to call from the
    /// non-blocking drain too.
    fn resolve_end(&mut self, upstream_err: Option<BoxedError>) {
        if upstream_err.is_some() {
            match self.arm_reopen(upstream_err, false) {
                Ok(()) => return,
                Err(cause) => {
                    self.finish_end(cause.as_ref());
                    return;
                }
            }
        }
        self.finish_end(upstream_err.as_ref());
    }

    /// The non-reopen half of end-of-stream: finish the decoder, release
    /// the terminal events (a clean empty `end_turn` may still arm the
    /// continuation resend), record the failure and latch `finished`.
    fn finish_end(&mut self, upstream_err: Option<&BoxedError>) {
        let events = self
            .decoder
            .finish(upstream_err.map(|err| &**err as &(dyn Error + 'static)));
        let events = self.release(events);
        if upstream_err.is_none() && empty_end_turn(&events) && self.begin_reopen_empty() {
            return;
        }
        self.record_upstream_failure(upstream_err.map(|err| &**err));
        self.queue.extend(events);
        self.finished = true;
    }

    /// Cancel-safe non-blocking receive: pop an already-decoded event, or
    /// synchronously absorb a frame the transport has already delivered.
    /// `None` means "nothing ready" — never end-of-stream. `message()` is
    /// polled once with a noop waker: it is cancel-safe (partial frames
    /// stay in the stream buffer, the terminal record replays), so a
    /// Pending poll loses nothing and the real waker is registered by the
    /// next blocking `recv` before the consumer ever sleeps.
    pub fn try_recv_event(&mut self) -> Option<ResponseEvent> {
        if let Some(event) = self.queue.pop_front() {
            return Some(event);
        }
        if self.finished || !self.started || self.pending_reopen.is_some() {
            return None;
        }
        let outcome = {
            let Some(stream) = &mut self.stream else {
                return None;
            };
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            let mut message = std::pin::pin!(stream.message::<pb::GetChatMessageResponse>());
            message.as_mut().poll(&mut cx)
        };
        match outcome {
            Poll::Ready(Ok(Some(response))) => {
                self.absorb_data(&response);
                self.queue.pop_front()
            }
            Poll::Ready(Ok(None)) => {
                self.resolve_end(None);
                self.queue.pop_front()
            }
            Poll::Ready(Err(err)) => {
                let err = normalize_wire_error(err);
                self.resolve_end(Some(Box::new(err) as BoxedError));
                self.queue.pop_front()
            }
            Poll::Pending => None,
        }
    }

    /// `tryReopen` — resend the whole request once while upstream failed
    /// before producing any content: the client has only seen the
    /// withheld start, so a resend has no visible effect. Returns true
    /// when the resend was armed; `pending_reopen` then owns the connect
    /// and `finish_reopen` adopts the new stream.
    fn begin_reopen(&mut self, cause: &StreamFault, continue_empty: bool) -> bool {
        self.arm_reopen(
            Some(Box::new(StreamFault(cause.0.clone())) as BoxedError),
            continue_empty,
        )
        .is_ok()
    }

    /// The `continue_empty` arm of `tryReopen` (Go calls it with a nil
    /// cause).
    fn begin_reopen_empty(&mut self) -> bool {
        self.arm_reopen(None, true).is_ok()
    }

    /// Resolve the spawned resend: adopt the new stream on success; on
    /// failure finish with the ORIGINAL cause (Go returns `nil, nil,
    /// cause` — the resend's own error is logged, not surfaced).
    fn finish_reopen(
        &mut self,
        outcome: Result<Result<UpstreamStream, BoxedError>, tokio::task::JoinError>,
    ) {
        let Some(pending) = self.pending_reopen.take() else {
            return;
        };
        match outcome {
            Ok(Ok(stream)) => {
                self.adopt(stream);
            }
            Ok(Err(err)) => {
                // The resend's own failure also leaves a trace: error.json
                // is first-write-wins and only records the original
                // failure point — "what the resend hit" lives only here.
                self.recorder.append_jsonl(
                    debuglog::STAGE_DEVIN_RESPONSE,
                    "retry_failed",
                    LogValue::serde(serde_json::json!({
                        "attempt": self.attempt,
                        "error": err.to_string(),
                    })),
                );
                self.retried = true;
                self.finish_end(pending.cause.as_ref());
            }
            Err(join) => {
                self.recorder.append_jsonl(
                    debuglog::STAGE_DEVIN_RESPONSE,
                    "retry_failed",
                    LogValue::serde(serde_json::json!({
                        "attempt": self.attempt,
                        "error": format!("resend task failed: {join}"),
                    })),
                );
                self.retried = true;
                self.finish_end(pending.cause.as_ref());
            }
        }
    }

    /// Kill the current attempt's upstream side: drop the live stream
    /// (closing the exchange — Go's `stream.cancel`) and abort any
    /// in-flight resend connect.
    fn kill_attempt(&mut self) {
        self.stream = None;
        if let Some(pending) = self.pending_reopen.take() {
            pending.handle.abort();
        }
    }

    /// Switch to the reopened stream: the old stream was already dropped
    /// when the resend was armed (a watchdog reopen's old stream may
    /// still be parked in `message()` and would otherwise ride the old
    /// HTTP stream to request end).
    fn adopt(&mut self, stream: UpstreamStream) {
        self.retried = true;
        self.stream = Some(stream);
        self.decoder = ResponseDecoder::new(
            &self.binding.model,
            &self.request.stop_sequences,
            custom_tool_names(&self.request.tools),
        );
        self.started = false;
        self.pending_start = Vec::new();
        self.finished = false;
        self.queue.clear();
        // The new stream's first non-error frame regains latch-release
        // standing — the previous stream's confirmation cannot prove this
        // retry actually crossed the limiter.
        self.upstream_confirmed = false;
        // The new stream's no-progress window starts fresh: the old
        // stream's timer (possibly just fired) does not carry over — the
        // consumer gets the full zero-event tolerance again.
        self.progress
            .as_mut()
            .expect("progress armed")
            .as_mut()
            .reset(Instant::now() + NO_PROGRESS_TIMEOUT);
    }

    /// The `reopen` closure, split at its single await: decide
    /// retryability, rebuild the request and spawn the resend's connect
    /// as `pending_reopen`. Retryable causes are a transport break,
    /// `unauthenticated` with a newer token generation (credential
    /// self-heal), and the empty-end_turn continuation. Upstream semantic
    /// refusals (parameter validation, permissions, rate limits) only
    /// reproduce on resend — they pass through. `Err` hands the original
    /// cause back for the finish path (Go returns `nil, nil, cause` /
    /// `nil, nil, err` to the same effect); a build failure is logged
    /// `retry_failed` here, a resend failure in `finish_reopen`.
    fn arm_reopen(
        &mut self,
        cause: Option<BoxedError>,
        continue_empty: bool,
    ) -> Result<(), Option<BoxedError>> {
        if self.retried
            || self.produced_events
            || self.pending_reopen.is_some()
            || (!continue_empty && cause.is_none())
        {
            return Err(cause);
        }
        let mut retry_request = self.request.clone();
        let cause_text;
        if continue_empty {
            // Empty end_turn (stopReason with zero content — an observed
            // degenerate upstream shape): append a "continue" user message
            // and resend once so the model continues in the same context.
            retry_request.messages.push(Message::User(UserMessage {
                content: vec![Content::Text(TextContent {
                    text: "continue".to_string(),
                })],
                ..UserMessage::default()
            }));
            cause_text = "empty end_turn: continue".to_string();
            warn!("reopening stream: upstream ended with empty content");
        } else {
            let cause_ref = cause
                .as_deref()
                .expect("non-empty reopen always carries a cause");
            if is_transient_connect_error(cause_ref) {
                cause_text = format!("transport: {cause_ref}");
                warn!(error = %cause_ref, "reopening stream: transport error before first content");
            } else if is_unauthenticated(cause_ref)
                && let Some(snapshot) = self.adapter.repair_token(self.token_generation)
            {
                self.binding.token = snapshot.token;
                self.token_generation = snapshot.generation;
                cause_text = "unauthenticated: token reloaded".to_string();
                info!("reopening stream: token reloaded after unauthenticated");
            } else {
                return Err(cause);
            }
        }
        let retry_cancel = self.cancel.child_token();
        let message = match build_request(&retry_request, &self.identity, &self.binding) {
            Ok((message, _)) => message,
            Err(err) => {
                retry_cancel.cancel();
                self.recorder.append_jsonl(
                    debuglog::STAGE_DEVIN_RESPONSE,
                    "retry_failed",
                    LogValue::serde(serde_json::json!({
                        "attempt": self.attempt,
                        "error": err.to_string(),
                    })),
                );
                return Err(cause);
            }
        };
        self.attempt += 1;
        note_retry(
            &self.recorder,
            self.attempt,
            &cause_text,
            continue_empty,
            &message,
        );
        // The old attempt's stream is dead or about to be killed — drop
        // it now so it cannot ride the old HTTP exchange while the
        // resend connects.
        self.stream = None;
        let adapter = self.adapter.clone();
        let recorder = self.recorder.clone();
        self.pending_reopen = Some(PendingReopen {
            cause,
            handle: tokio::spawn(async move {
                get_chat_message_with_retry(&adapter, &recorder, &retry_cancel, message).await
            }),
        });
        Ok(())
    }

    /// `release` — hand the decoder's first event batch to the caller:
    /// non-error batches get the withheld `start` prepended; when the
    /// first batch IS an error (upstream failed before producing
    /// content), `start` is dropped so the error becomes the stream's
    /// first outward event — the HTTP layer can then return a real error
    /// status instead of a committed 200 + SSE error.
    fn release(&mut self, events: Vec<ResponseEvent>) -> Vec<ResponseEvent> {
        if events.is_empty() || self.pending_start.is_empty() {
            return events;
        }
        let mut start = std::mem::take(&mut self.pending_start);
        if events[0].kind == ResponseEventType::Error {
            return events;
        }
        start.extend(events);
        start
    }

    /// `recordUpstreamFailure` — record a non-retryable upstream-side
    /// failure as the request dir's first failure point: transport breaks
    /// go to `devin_transport` (connect-wrapped EOF/truncated frames/
    /// connection resets included — see `is_transient_connect_error`),
    /// upstream semantic errors to `devin_connect`. Cancellation is not
    /// recorded — a client disconnect is logged `client_disconnected` by
    /// the outer HTTP layer and must not be preempted by an upstream
    /// stage. `WriteError` is first-write-wins; after this the outer
    /// `http_stream` only supplements.
    fn record_upstream_failure(&mut self, cause: Option<&(dyn Error + Send + Sync + 'static)>) {
        let Some(cause) = cause else {
            return;
        };
        // The rate-limit conclusion is independent of the log switch: an
        // upstream `resource_exhausted` arms the latch.
        self.adapter.gate().note_upstream_error(cause);
        if is_cancel_like(cause) {
            return;
        }
        let stage = if is_transient_connect_error(cause) {
            debuglog::ERR_STAGE_DEVIN_TRANSPORT
        } else {
            debuglog::ERR_STAGE_DEVIN_CONNECT
        };
        self.recorder.write_error(stage, cause);
    }

    /// `drainFrames` — log frames already buffered but unconsumed when a
    /// watchdog kills the stream: "what did it last output before dying"
    /// is the key evidence for judging the upstream hang shape. Polls
    /// `message()` with a noop waker: cancel-safe, and only frames the
    /// transport already holds are drained.
    fn drain_frames(&mut self) {
        let Some(stream) = &mut self.stream else {
            return;
        };
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        loop {
            let mut message = std::pin::pin!(stream.message::<pb::GetChatMessageResponse>());
            match message.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(Some(frame))) => {
                    record_proto_json(
                        &self.recorder,
                        debuglog::STAGE_DEVIN_RESPONSE,
                        frame.to_owned_message(),
                    );
                }
                _ => return,
            }
        }
    }
}

impl ResponseStream for DevinStream {
    #[allow(clippy::large_futures)] // async port of Go's `Recv`
    async fn recv(&mut self) -> Result<Option<ResponseEvent>, Failure> {
        self.recv_inner().await
    }

    fn try_recv(&mut self) -> Option<ResponseEvent> {
        self.try_recv_event()
    }
}

impl Drop for DevinStream {
    /// Dropping the stream kills the attempt lineage: the live upstream
    /// stream and any in-flight resend connect die with it (Go's
    /// `stream.cancel` on every exit path).
    fn drop(&mut self) {
        self.kill_attempt();
    }
}

/// `noteRetry` — one resend's bookkeeping: bump the attempt counter,
/// index the retry, leave the `retry_attempt` divider line in 04 and
/// write the rebuilt wire request to its `03.attemptN` shard — the single
/// point where all of that happens (two call sites each wrote their own
/// once, and the divider fields drifted).
fn note_retry(
    recorder: &Recorder,
    attempt: u32,
    cause: &str,
    continue_empty: bool,
    message: &pb::GetChatMessageRequest,
) {
    recorder.note_retry_attempt(i64::from(attempt), cause);
    recorder.append_jsonl(
        debuglog::STAGE_DEVIN_RESPONSE,
        "retry_attempt",
        LogValue::serde(serde_json::json!({
            "attempt": attempt,
            "cause": cause,
            "continue_empty": continue_empty,
        })),
    );
    record_proto_json(
        recorder,
        &debuglog::stage_devin_request_attempt(attempt),
        message.clone(),
    );
}

/// `getChatMessageWithRetry` — retry transient transport errors during
/// stream establishment only; once the stream is open, errors surface
/// through the event stream and the request is never resent. Every real
/// send (including transient retries) passes the rate gate: rejected
/// attempts still push the upstream recovery instant out, so local
/// shaping is the only mitigation. Approved exception 3: backoff
/// completes BEFORE final rate admission, and cancellation is rechecked
/// immediately before the actual send.
async fn get_chat_message_with_retry(
    adapter: &Adapter,
    recorder: &Recorder,
    cancel: &CancellationToken,
    request: pb::GetChatMessageRequest,
) -> Result<
    ServerStream<
        crate::upstream::transport::CheckedResponseBody,
        pb::GetChatMessageResponseView<'static>,
    >,
    BoxedError,
> {
    // sent/open instrumentation is idempotent (CAS -1): on retry, `sent`
    // stays at the first send and `open` records the first successful
    // connect — the sent→open delta honestly includes the backoff time.
    let mut last_err: Option<BoxedError> = None;
    for attempt in 0..MAX_CONNECT_ATTEMPTS {
        if attempt > 0 {
            // ±25% jitter: fixed-cadence retries stack up under upstream
            // transient congestion.
            let base = Duration::from_millis(400 * u64::from(attempt));
            let backoff = base.mul_f64(0.75 + 0.5 * rand::random::<f64>());
            // Signal only after the retry has reached its backoff boundary;
            // the hook is observation-only and cannot release the wait.
            adapter.note_retry_backoff_wait();
            tokio::select! {
                () = cancel.cancelled() => {
                    return Err(Box::new(Canceled));
                }
                () = tokio::time::sleep(backoff) => {}
            }
        }
        if let Err(err) = adapter.gate().wait(cancel).await {
            // A gate fast-fail is recorded `rate_gate` at the origin
            // (WriteError is first-write-wins): this function is shared
            // by first send and reopen retries, and the reopen path's
            // error keeps bubbling through the stream exit — without the
            // stage here it would be overwritten as provider_stream,
            // blaming local throttling on upstream.
            if let WaitError::Rejected(failure) = &err
                && failure.local_gate
            {
                recorder.write_error(debuglog::ERR_STAGE_RATE_GATE, &err);
            }
            return Err(Box::new(err));
        }
        // Cancellation is rechecked immediately before the actual send: a
        // token that fired while the gate admitted must not put bytes on
        // the wire.
        if cancel.is_cancelled() {
            return Err(Box::new(Canceled));
        }
        recorder.note_upstream_send();
        let result = tokio::select! {
            () = cancel.cancelled() => {
                return Err(Box::new(Canceled));
            }
            result = adapter.stream_client().get_chat_message(request.clone()) => result,
        };
        match result {
            Ok(stream) => {
                recorder.note_upstream_open();
                return Ok(stream);
            }
            Err(err) => {
                let transient = is_transient_connect_error(&err);
                last_err = Some(Box::new(err));
                if !transient {
                    break;
                }
            }
        }
    }
    if let Some(err) = &last_err {
        adapter.gate().note_upstream_error(&**err);
    }
    Err(last_err.expect("connect attempts always record an error"))
}

/// The fetcher's own disconnect is not an upstream failure: cancellation
/// and deadline sentinels in the chain suppress error recording (Go
/// `errors.Is(err, context.Canceled/DeadlineExceeded)`).
fn is_cancel_like(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        if err.is::<Canceled>()
            || err.is::<DeadlineExceeded>()
            || err.is::<tokio::time::error::Elapsed>()
        {
            return true;
        }
        if let Some(req_err) = err.downcast_ref::<reqwest::Error>()
            && req_err.is_timeout()
        {
            return true;
        }
        current = err.source();
    }
    false
}

impl Adapter {
    /// `Stream` — convert one intermediate request into a Devin RPC and
    /// return the intermediate response event stream. `request` has
    /// already been through the protocol decode validation; `recorder`
    /// carries the request's debug-log handle (Go `debuglog.FromContext`).
    ///
    /// # Errors
    /// The classified failure (`llm.Classify` parity): local validation
    /// and projection errors, gate rejections, connect-phase transport
    /// breaks and upstream semantic refusals.
    #[allow(clippy::too_many_lines)] // one-to-one port of Go's `Stream`
    pub async fn stream(
        &self,
        request: RequestMessages,
        cancel: CancellationToken,
        recorder: Recorder,
    ) -> Result<DevinStream, Failure> {
        let mut request = request;
        let sanitize_hits = sanitize_request(&mut request);
        let cfg = self.config();
        let mut model = request.model.trim().to_string();
        if model.is_empty() {
            model = cfg.model.clone();
        }
        model = crate::upstream::catalog::resolve_model_alias(&cfg.aliases, &model);
        // The catalog backs router detection and capability checks; on a
        // lazy load this is the one catch-up fetch.
        self.ensure_catalog(&cancel).await;
        let routing = match self.resolve_model_routing(&request, &model, &cancel).await {
            Ok(routing) => routing,
            Err(err) => {
                // AssignModel is a connect-phase RPC too: transport breaks
                // and semantic refusals stay in separate stages.
                let stage = if is_transient_connect_error(&err) {
                    debuglog::ERR_STAGE_DEVIN_TRANSPORT
                } else {
                    debuglog::ERR_STAGE_DEVIN_CONNECT
                };
                recorder.write_error(stage, &err);
                return Err(err);
            }
        };
        let model = routing.model;
        // Alias and routing decisions end here: record the resolved uid so
        // the in-flight list shows "requested name → actual uid" without
        // waiting for the response identity to fill in.
        recorder.set_resolved_model(&model);
        // Capability validation and the absence warning act on the
        // resolved uid — a router entry's own catalog capability bits are
        // unrelated to the model that ends up serving the request.
        self.warn_if_model_absent_from_catalog(&model);
        if let Err(err) = self.validate_images_for_model(&request, &model) {
            // A local validation refusal records `request_build` at the
            // origin: the error keeps bubbling and the stream-layer exit
            // would overwrite it as provider_stream.
            recorder.write_error(debuglog::ERR_STAGE_REQUEST_BUILD, &err);
            return Err(err);
        }
        // `binding` carries the per-call fields: `model` is the final uid
        // after alias/router rewriting, `token` is fetched now (a
        // self-heal retry swaps it), `jwt` is this routing's binding
        // product.
        let token = self.current_token();
        let mut binding = CallBinding {
            token: token.token,
            model: model.clone(),
            model_assignment_jwt: routing.jwt,
        };
        let identity = cfg.client_identity();
        let (mut proto_request, mut repairs) = match build_request(&request, &identity, &binding) {
            Ok(built) => built,
            Err(err) => {
                recorder.write_error(debuglog::ERR_STAGE_REQUEST_BUILD, &err);
                return Err(err);
            }
        };
        repairs.sanitize_hits = sanitize_hits;
        recorder.set_repairs(repairs);
        if recorder.is_active() {
            record_proto_json(
                &recorder,
                debuglog::STAGE_DEVIN_REQUEST,
                proto_request.clone(),
            );
        }
        // `attempt` distinguishes multiple sends: self-heal resends and
        // pre-content reopens both rebuild the request body; attempt 2+
        // writes its own file and leaves a retry_attempt divider in 04,
        // otherwise 04's frames cannot be attributed to a send.
        let mut attempt = 1u32;
        // `stream_cancel` scopes the connect phase of this attempt: a
        // client disconnect cancels the in-flight RPC; once the stream is
        // open, kills drop the stream itself.
        let stream_cancel = cancel.child_token();
        let mut result =
            get_chat_message_with_retry(self, &recorder, &stream_cancel, proto_request.clone())
                .await;
        if let Err(err) = &result
            && is_unauthenticated(&**err)
            && let Some(snapshot) = self.repair_token(token.generation)
        {
            // Credential self-heal: the CLI renews credentials.toml;
            // rebuild the request with the new token and retry once. No
            // retry when the token did not change.
            binding.token = snapshot.token.clone();
            match build_request(&request, &identity, &binding) {
                Ok((rebuilt, _)) => {
                    proto_request = rebuilt;
                    attempt += 1;
                    note_retry(
                        &recorder,
                        attempt,
                        "unauthenticated: token reloaded",
                        false,
                        &proto_request,
                    );
                    result =
                        get_chat_message_with_retry(self, &recorder, &stream_cancel, proto_request)
                            .await;
                }
                Err(build_err) => {
                    result = Err(Box::new(build_err));
                }
            }
        }
        let token_generation = self.current_token().generation;
        let decoder = ResponseDecoder::new(
            &model,
            &request.stop_sequences,
            custom_tool_names(&request.tools),
        );
        let stream = match result {
            Ok(stream) => stream,
            Err(err) => {
                // Judge the parent token, not `stream_cancel`: after
                // `cancel()` the child is always cancelled, and checking
                // it would make the whole WriteError block dead code
                // (every rate_gate stage name lost). A cancelled parent
                // (client disconnect/drain) is not recorded — the outer
                // layer logs client_disconnected and a first-failure
                // record here would preempt it.
                let parent_done = cancel.is_cancelled();
                stream_cancel.cancel();
                if !parent_done {
                    // A gate refusal was already recorded `rate_gate` at
                    // the wait failure; what remains is upstream connect
                    // failure — transport breaks and upstream semantic
                    // refusals (devin_connect) stay in separate layers.
                    let stage = if is_transient_connect_error(&*err) {
                        debuglog::ERR_STAGE_DEVIN_TRANSPORT
                    } else {
                        debuglog::ERR_STAGE_DEVIN_CONNECT
                    };
                    recorder.write_error(stage, &*err);
                }
                // The classified record travels with the error — the
                // downstream recovers the structured facts via `classify`
                // instead of re-deriving them from text.
                return Err(classify(&*err));
            }
        };
        Ok(DevinStream {
            adapter: self.clone(),
            request,
            identity,
            binding,
            token_generation,
            attempt,
            recorder,
            cancel,
            stream: Some(stream),
            pending_reopen: None,
            decoder,
            started: false,
            pending_start: Vec::new(),
            start_released: false,
            start_hold: None,
            finished: false,
            queue: VecDeque::new(),
            produced_events: false,
            upstream_confirmed: false,
            retried: false,
            stall: None,
            progress: None,
        })
    }
}

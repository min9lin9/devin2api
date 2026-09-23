//! Downstream JSON/SSE ownership and the HTTP commit boundary.
//!
//! Once a response body is returned, the body stream itself owns
//! cancellation, the inference permit and debug-log finalization until it
//! reaches a real terminal outcome — Go's single-writer shape, where the
//! handler's write loop consumes the event stream directly. Dropping the
//! body cancels the same token used upstream.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use futures_util::stream;
use http::header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE};
use http::{HeaderValue, StatusCode};
use tokio_util::sync::CancellationToken;

use crate::debuglog::{Completion, LogValue, Recorder, STAGE_HTTP_RESPONSE};
use crate::domain::{
    AssistantMessage, Failure, ResponseEvent, ResponseEventType, ResponseStream, StopReason,
    failure_of,
};
use crate::protocol::common::{SSE_DONE, SseFrame};
use crate::protocol::dispatch::{
    AnthropicProtocol, ChatProtocol, HttpError, ProtocolEncoder, ResponsesProtocol, StreamEncoder,
};

use super::http::{Admission, HttpEventStream};

pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
pub const SSE_KEEPALIVE: &[u8] = b": keepalive\n\n";
pub const JSON_HEARTBEAT: &[u8] = b"\n";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolKind {
    Responses,
    Chat,
    Anthropic,
}

impl ProtocolKind {
    fn encoder(self) -> Box<dyn ProtocolEncoder + Send + Sync> {
        match self {
            Self::Responses => Box::new(ResponsesProtocol),
            Self::Chat => Box::new(ChatProtocol),
            Self::Anthropic => Box::new(AnthropicProtocol),
        }
    }
}

pub(crate) struct AdapterEventStream<S>(pub S);

impl<S> HttpEventStream for AdapterEventStream<S>
where
    S: ResponseStream + Send + 'static,
{
    fn recv(&mut self) -> super::http::BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        Box::pin(ResponseStream::recv(&mut self.0))
    }

    fn try_recv(&mut self) -> Option<ResponseEvent> {
        ResponseStream::try_recv(&mut self.0)
    }
}

/// The SSE response body: the event consumer, encoder, recorder,
/// cancellation lineage and permit all live in the body stream — the
/// producer task and its `mpsc<Bytes>` hop are gone (Go's
/// `writeProtocolStream` is the handler's own write loop). `pending`
/// holds encoded chunks not yet pulled by hyper; `terminal_result` is
/// the completion result once the stream's outcome is known, with
/// finalization deferred until the queued tail is delivered or the body
/// is dropped — a body dropped with the tail unsent is a disconnect
/// (the old producer's failed `tx.send`), a body drained to `None` keeps
/// the recorded result.
struct SseBody {
    source: Box<dyn HttpEventStream>,
    encoder: Box<dyn StreamEncoder>,
    recorder: Recorder,
    cancel: CancellationToken,
    /// Permit + drain slot: released when the body is dropped — after
    /// hyper has flushed (or abandoned) the queued tail. `Option` so
    /// `finalize` can move it into the `complete` task: Go's
    /// `defer release()` runs after `defer Complete`, so the slot stays
    /// occupied through the drain wait and meta/index writes.
    admission: Option<Admission>,
    completion: Option<Completion>,
    interval: tokio::time::Interval,
    /// Encoded bytes accumulate while upstream keeps supplying events;
    /// the batch flushes when the event source is momentarily empty —
    /// Go's `writeProtocolStream` batching rule (idle path = per-event
    /// writes, bursts amortize the socket write). `BytesMut` so a flush
    /// is a zero-copy `split().freeze()` that keeps the spare tail
    /// capacity — Go's `batch = batch[:0]` reuse.
    batch: BytesMut,
    /// Chunks committed to the wire but not yet pulled by hyper.
    pending: VecDeque<Bytes>,
    /// Per-event frame spans of the last `encode_into` call — the debug
    /// recorder replays them for its name/payload log split; reused across
    /// events so the hot path allocates once.
    frames: Vec<SseFrame>,
    /// The stream's terminal outcome once known; `None` while live.
    terminal_result: Option<&'static str>,
    /// The terminal outcome was already recorded via `complete`.
    finalized: bool,
    /// The cancel arm already logged `client_disconnected` (the drop
    /// path must not preempt a recorded upstream failure with a second
    /// write — `write_error` is first-write-wins anyway, this keeps the
    /// intent explicit).
    disconnect_logged: bool,
}

impl SseBody {
    fn new(
        source: Box<dyn HttpEventStream>,
        encoder: Box<dyn StreamEncoder>,
        recorder: Recorder,
        cancel: CancellationToken,
        admission: Admission,
        completion: Completion,
    ) -> Self {
        // `interval`'s first tick fires immediately; `reset` pushes it out
        // one period — the spawned producer's `interval.tick().await`
        // warm-up, so the first keepalive lands 10s after commit.
        let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
        interval.reset();
        Self {
            source,
            encoder,
            recorder,
            cancel,
            admission: Some(admission),
            completion: Some(completion),
            interval,
            batch: BytesMut::new(),
            pending: VecDeque::new(),
            frames: Vec::new(),
            terminal_result: None,
            finalized: false,
            disconnect_logged: false,
        }
    }

    /// A body whose whole payload is already known (the pre-commit
    /// in-stream error path): the completion was already recorded, the
    /// body only delivers the bytes and still owns cancel + permit.
    fn terminal(chunk: Bytes, cancel: CancellationToken, admission: Admission) -> Self {
        Self {
            source: Box::new(EmptyEventStream),
            encoder: Box::new(NoopEncoder),
            recorder: Recorder::none(),
            cancel,
            admission: Some(admission),
            completion: None,
            interval: tokio::time::interval(KEEPALIVE_INTERVAL),
            batch: BytesMut::new(),
            pending: VecDeque::from([chunk]),
            frames: Vec::new(),
            terminal_result: None,
            finalized: true,
            disconnect_logged: true,
        }
    }

    /// Record the terminal outcome once the tail is queued: mirrors the
    /// producer's `complete()` after its last `tx.send` — enqueueing the
    /// final bytes is the commit point, delivery is hyper's job. The
    /// drain wait + meta/index writes run on the blocking pool: Go parks
    /// a goroutine in `Complete`, which is cheap — a parked tokio worker
    /// is a quarter of the runtime.
    async fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        let Some(result) = self.terminal_result else {
            return;
        };
        self.finalized = true;
        if let Some(completion) = self.completion.take()
            && let Some(task) = spawn_complete(
                self.recorder.clone(),
                completion,
                result,
                self.admission.take(),
            )
        {
            let _ = task.await;
        }
    }

    /// Queue encoded bytes for the wire (the producer's `tx.send`): the
    /// queue cannot fail — a client that is already gone surfaces as the
    /// body being dropped, which finalizes as `disconnected`.
    fn emit(&mut self, chunk: Bytes) {
        self.recorder.note_client_latency();
        self.pending.push_back(chunk);
    }

    /// One `poll_next` step: the next committed chunk, or `None` at end
    /// of stream. This is the producer loop verbatim with `tx.send`
    /// replaced by `emit`/`pending` and `break` by `terminal_result`.
    // Kept as one loop mirroring Go's producer for parity review.
    #[allow(clippy::too_many_lines)]
    async fn step(&mut self) -> Option<Bytes> {
        if let Some(chunk) = self.pending.pop_front() {
            return Some(chunk);
        }
        if self.terminal_result.is_some() {
            self.finalize().await;
            return None;
        }
        'outer: loop {
            let next = if self.batch.is_empty() {
                // Fast path: an already-decoded event skips the select —
                // building the cancel/tick futures per event is the
                // measured per-frame cost under a saturated pump. The
                // post-`next` `is_cancelled` check below keeps Go's
                // cancel attribution identical to the select arm.
                if let Some(event) = self.source.try_recv() {
                    Ok(Some(event))
                } else {
                    tokio::select! {
                        () = self.cancel.cancelled() => {
                            self.terminal_result = Some("disconnected");
                            self.disconnect_logged = true;
                            self.recorder.write_error("client_disconnected", &Failure::plain("client disconnected"));
                            break 'outer;
                        }
                        _ = self.interval.tick() => {
                            return Some(Bytes::from_static(SSE_KEEPALIVE));
                        }
                        next = self.source.recv() => next,
                    }
                }
            } else {
                // Go `select { case item := <-items: ... default: flush }`:
                // drain only events already ready. `try_recv` is the
                // cancel-safe form — it never polls a future, so nothing
                // can be dropped mid-await (a `now_or_never(recv())` here
                // could lose a consumed pump frame inside a pending
                // `try_reopen`).
                match self.source.try_recv() {
                    Some(event) => Ok(Some(event)),
                    None => {
                        // Source momentarily empty: flush the batch (Go
                        // flushes when the pump channel would block).
                        // `split().freeze()` emits the accumulated bytes
                        // zero-copy and keeps the tail capacity — Go's
                        // `batch = batch[:0]` reuse across the stream.
                        return Some(self.batch.split().freeze());
                    }
                }
            };
            // Cancellation attribution mirrors Go's per-item ctx.Err()
            // check: a cancel observed mid-batch still classifies the
            // request as disconnected rather than completed/failed.
            if self.cancel.is_cancelled() {
                self.terminal_result = Some("disconnected");
                self.disconnect_logged = true;
                self.recorder.write_error(
                    "client_disconnected",
                    &Failure::plain("client disconnected"),
                );
                break 'outer;
            }
            match next {
                Ok(Some(event)) => {
                    self.recorder.note_upstream_latency();
                    if let Some(message) = event
                        .message
                        .as_ref()
                        .or(event.partial.as_deref())
                        .or(event.error.as_ref())
                    {
                        update_completion(
                            self.completion
                                .as_mut()
                                .expect("live body holds its completion"),
                            message,
                        );
                    }
                    match append_event(
                        &mut *self.encoder,
                        &self.recorder,
                        &event,
                        &mut self.batch,
                        &mut self.frames,
                    ) {
                        Ok(_) => {
                            if event.kind == ResponseEventType::Error {
                                // The producer flushed the batch before
                                // recording the failure — keep that order
                                // (client-latency note precedes the error
                                // record in the log queue).
                                if !self.batch.is_empty() {
                                    let batch = self.batch.split().freeze();
                                    self.emit(batch);
                                }
                                self.recorder.write_error(
                                    "provider_stream",
                                    &failure_of(event.error.as_ref()),
                                );
                                self.terminal_result = Some("failed");
                                break 'outer;
                            }
                        }
                        Err(failure) => {
                            self.recorder.write_error("response_event", &failure);
                            self.terminal_result = Some("failed");
                            break 'outer;
                        }
                    }
                }
                Ok(None) => {
                    self.terminal_result = Some("completed");
                    break 'outer;
                }
                Err(failure) => {
                    let event = terminal_error_event(failure.clone());
                    let before = self.batch.len();
                    if append_event(
                        &mut *self.encoder,
                        &self.recorder,
                        &event,
                        &mut self.batch,
                        &mut self.frames,
                    )
                    .is_ok()
                        && self.batch.len() > before
                    {
                        let batch = self.batch.split().freeze();
                        self.emit(batch);
                    }
                    self.recorder.write_error("provider_stream", &failure);
                    self.terminal_result = Some("failed");
                    break 'outer;
                }
            }
        }
        // Terminal paths flush the accumulated batch before ending (the
        // producer's post-loop `tx.send(batch)`). When nothing is left to
        // send this poll is the body's last — finalize here so `complete`
        // and the permit release still precede EOF (Go's `defer` order);
        // otherwise `drop` would run them detached and the next turn on a
        // sequential transport could observe the permit still held.
        if !self.batch.is_empty() {
            let batch = self.batch.split().freeze();
            self.emit(batch);
        }
        if let Some(chunk) = self.pending.pop_front() {
            Some(chunk)
        } else {
            self.finalize().await;
            None
        }
    }
}

impl Drop for SseBody {
    /// Dropping the body is the client-disconnect signal (Go's writer
    /// seeing ctx.Done): cancel the upstream lineage and, when the stream
    /// never reached a terminal outcome, record the disconnect — the
    /// producer task's cancel arm, run at the same point in the lineage.
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.finalized {
            return;
        }
        self.finalized = true;
        if self.terminal_result.is_none() {
            self.terminal_result = Some("disconnected");
            if !self.disconnect_logged {
                self.recorder.write_error(
                    "client_disconnected",
                    &Failure::plain("client disconnected"),
                );
            }
        }
        if let Some(completion) = self.completion.take() {
            // Detached: the body is gone either way — the drain wait must
            // not park the dropping worker either.
            let _ = spawn_complete(
                self.recorder.clone(),
                completion,
                self.terminal_result.unwrap_or("disconnected"),
                self.admission.take(),
            );
        }
    }
}

/// Placeholder event source for an already-finalized terminal body.
struct EmptyEventStream;

impl HttpEventStream for EmptyEventStream {
    fn recv(&mut self) -> super::http::BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        Box::pin(async { Ok(None) })
    }
}

/// Placeholder encoder for the terminal body (never used: the terminal
/// body only drains `pending`).
struct NoopEncoder;

impl StreamEncoder for NoopEncoder {
    fn encode_into(
        &mut self,
        _event: &ResponseEvent,
        _dst: &mut BytesMut,
        _frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        Ok(())
    }
}

fn sse_body_stream(state: SseBody) -> Body {
    Body::from_stream(stream::unfold(state, |mut state| async move {
        state
            .step()
            .await
            .map(|chunk| (Ok::<_, Infallible>(chunk), state))
    }))
}

fn committed_response(body: Body, sse: bool) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(if sse {
            "text/event-stream"
        } else {
            "application/json"
        }),
    );
    if sse {
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("keep-alive"));
    }
    response
}

/// Encode one event's SSE frames straight into `dst` — Go's
/// `batch = protocol.AppendSSE(batch, ...)` shape: serialization lands in
/// the output buffer with no per-event temporaries. `frames` is a
/// caller-owned scratch the encoder fills with each frame's payload span
/// for the debug recorder. Returns the number of bytes appended.
fn append_event(
    encoder: &mut dyn StreamEncoder,
    recorder: &Recorder,
    event: &ResponseEvent,
    dst: &mut BytesMut,
    frames: &mut Vec<SseFrame>,
) -> Result<usize, Failure> {
    // The disabled recorder is a no-op sink: skip the per-event record and
    // the per-frame payload copies it would discard anyway (hot path).
    let active = recorder.is_active();
    if active {
        recorder.record_response_event(event);
    }
    let before = dst.len();
    frames.clear();
    if let Err(failure) = encoder.encode_into(event, dst, frames) {
        // A failed event must not leave half-written frames in the batch —
        // the old Vec<SseEvent> shape made that structurally impossible.
        dst.truncate(before);
        return Err(failure);
    }
    if active {
        for frame in frames.iter() {
            let data = &dst[frame.data.clone()];
            if frame.name == SSE_DONE {
                // Go logs the [DONE] marker as a JSON string, not raw
                // bytes (stream.go: `AppendJSONL(..., string(data))`).
                recorder.append_jsonl(
                    STAGE_HTTP_RESPONSE,
                    frame.name,
                    LogValue::text(String::from_utf8_lossy(data).into_owned()),
                );
            } else {
                recorder.append_jsonl(
                    STAGE_HTTP_RESPONSE,
                    frame.name,
                    LogValue::Raw(data.to_vec()),
                );
            }
        }
    }
    Ok(dst.len() - before)
}

fn terminal_error_event(failure: Failure) -> ResponseEvent {
    ResponseEvent {
        kind: ResponseEventType::Error,
        reason: Some(StopReason::Error),
        error: Some(AssistantMessage {
            error_message: failure.to_string(),
            failure: Some(Box::new(failure)),
            ..AssistantMessage::default()
        }),
        ..ResponseEvent::default()
    }
}

fn update_completion(completion: &mut Completion, message: &AssistantMessage) {
    completion.provider.clone_from(&message.provider);
    completion
        .upstream_request_id
        .clone_from(&message.upstream_request_id);
    completion
        .response_model
        .clone_from(&message.response_model);
    completion.usage = message.usage.clone();
    completion.premature_end_turn = completion.premature_end_turn
        && message.stop_reason == Some(StopReason::Stop)
        && !message
            .content
            .iter()
            .any(|content| content.content_type() == crate::domain::ContentType::ToolCall);
    if !message.response_model.is_empty() {
        completion.model.clone_from(&message.response_model);
    } else if !message.model.is_empty() {
        completion.model.clone_from(&message.model);
    }
}

fn complete(recorder: &Recorder, mut completion: Completion, result: &str) {
    completion.status_code = 200;
    completion.result = result.to_string();
    recorder.complete(completion);
}

/// `complete` on the blocking pool when a runtime is reachable (the
/// recorder's `writer_done` wait and meta/index writes are blocking IO);
/// inline when there is none (tests dropping bodies outside tokio).
/// Returns the join handle so async callers can preserve Go's
/// EOF-after-`Complete` ordering; `None` means it already ran inline.
fn spawn_complete(
    recorder: Recorder,
    completion: Completion,
    result: &'static str,
    admission: Option<Admission>,
) -> Option<tokio::task::JoinHandle<()>> {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        return Some(handle.spawn_blocking(move || {
            complete(&recorder, completion, result);
            // The permit releases only after `complete` — Go's
            // `defer release()` ordering inside the handler.
            drop(admission);
        }));
    }
    complete(&recorder, completion, result);
    drop(admission);
    None
}

/// Returns `(response, body_owns_completion)`. `true` means the response
/// body (or an already-finalized in-stream error) owns the recorder; the
/// handler must not finalize it.
// The parameter set is the request pipeline's context; grouping it into
// a struct would obscure the Go handler's argument order.
#[allow(clippy::too_many_arguments)]
pub async fn sse_response(
    mut source: Box<dyn HttpEventStream>,
    protocol_kind: ProtocolKind,
    model: String,
    include_usage: bool,
    cancel: CancellationToken,
    recorder: Recorder,
    admission: Admission,
    mut completion: Completion,
) -> Result<(Response, bool), Failure> {
    let protocol = protocol_kind.encoder();
    let mut encoder = protocol.new_stream_encoder(&model, include_usage);
    let first = tokio::select! {
        () = cancel.cancelled() => return Err(Failure::plain("client disconnected")),
        result = tokio::time::timeout(KEEPALIVE_INTERVAL, source.recv()) => result,
    };
    let mut pending = VecDeque::new();
    match first {
        Err(_) => {
            pending.push_back(Bytes::from_static(SSE_KEEPALIVE));
        }
        Ok(Err(err)) => return Err(err),
        Ok(Ok(None)) => {
            return Err(Failure::plain(
                "response stream ended without a final message",
            ));
        }
        Ok(Ok(Some(event))) if event.kind == ResponseEventType::Error => {
            let failure = failure_of(event.error.as_ref());
            if !(protocol.stream_error_events() && (failure.rate_limited || failure.context_length))
            {
                return Err(failure);
            }
            // OpenAI clients only retry these pre-content failures when they
            // arrive in-stream. Match Go's commit sequence by synthesizing
            // Start first; the encoder emits response.created/in_progress,
            // then assigns response.failed the next sequence number.
            let start = ResponseEvent {
                kind: ResponseEventType::Start,
                reason: Some(StopReason::Pending),
                partial: event.error.clone().map(Arc::new),
                ..ResponseEvent::default()
            };
            let mut body = BytesMut::new();
            let mut frames = Vec::new();
            append_event(&mut *encoder, &recorder, &start, &mut body, &mut frames)?;
            append_event(&mut *encoder, &recorder, &event, &mut body, &mut frames)?;
            recorder.note_client_latency();
            recorder.write_error("provider_stream", &failure);
            complete(&recorder, completion, "failed");
            return Ok((
                committed_response(
                    sse_body_stream(SseBody::terminal(body.freeze(), cancel, admission)),
                    true,
                ),
                true,
            ));
        }
        Ok(Ok(Some(event))) => {
            recorder.note_upstream_latency();
            if let Some(message) = event
                .message
                .as_ref()
                .or(event.partial.as_deref())
                .or(event.error.as_ref())
            {
                update_completion(&mut completion, message);
            }
            let mut body = BytesMut::new();
            let mut frames = Vec::new();
            if append_event(&mut *encoder, &recorder, &event, &mut body, &mut frames)? > 0 {
                recorder.note_client_latency();
                pending.push_back(body.freeze());
            }
        }
    }

    let mut state = SseBody::new(source, encoder, recorder, cancel, admission, completion);
    state.pending = pending;
    Ok((committed_response(sse_body_stream(state), true), true))
}

async fn collect_final(
    source: &mut dyn HttpEventStream,
    cancel: &CancellationToken,
    recorder: &Recorder,
) -> Result<AssistantMessage, Failure> {
    let mut final_message = None;
    loop {
        let event = tokio::select! {
            () = cancel.cancelled() => return Err(Failure::plain("client disconnected")),
            event = source.recv() => event?,
        };
        let Some(event) = event else {
            return final_message
                .ok_or_else(|| Failure::plain("response stream ended without a final message"));
        };
        if recorder.is_active() {
            recorder.record_response_event(&event);
        }
        match event.kind {
            ResponseEventType::Done => final_message = event.message,
            ResponseEventType::Error => return Err(failure_of(event.error.as_ref())),
            _ => {}
        }
    }
}

/// The JSON heartbeat path's body: `collect_final` runs inside the body
/// stream and the single terminal chunk (success body or error body) is
/// queued for the wire — the same fused ownership as [`SseBody`].
type CollectFuture = Pin<Box<dyn Future<Output = Result<AssistantMessage, Failure>> + Send>>;

struct JsonBody {
    /// The in-flight collect, owning the event source: heartbeat ticks
    /// interleave with it without dropping a pending `recv` — Go's
    /// `collectPumpedMessage` select loop over items, ticker and ctx.
    collection: Option<CollectFuture>,
    protocol: Box<dyn ProtocolEncoder + Send + Sync>,
    model: String,
    recorder: Recorder,
    cancel: CancellationToken,
    /// Same permit-through-`complete` contract as [`SseBody::admission`].
    admission: Option<Admission>,
    completion: Option<Completion>,
    pending: VecDeque<Bytes>,
    /// Repeating `\n` heartbeat for the collect window (Go's
    /// `time.NewTicker(keepaliveInterval)`); the first tick lands one
    /// interval after the body is committed — the queued `pending`
    /// heartbeat is the tick the handler already observed.
    interval: tokio::time::Interval,
    /// `true` until `collect_final` resolves — a drop while collecting is
    /// the producer's abandoned `collect_final` (a `response_event`
    /// disconnect); a drop after the terminal chunk was queued is the
    /// producer's failed `tx.send` (`client_disconnected`).
    collecting: bool,
    /// The stream's terminal outcome once known; `None` while live.
    terminal_result: Option<&'static str>,
    finalized: bool,
}

impl JsonBody {
    /// Record the terminal outcome once the terminal chunk is queued —
    /// the producer's `complete()` after its `tx.send`. Same blocking-pool
    /// rationale as [`SseBody::finalize`].
    async fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        let Some(result) = self.terminal_result else {
            return;
        };
        self.finalized = true;
        if let Some(completion) = self.completion.take()
            && let Some(task) = spawn_complete(
                self.recorder.clone(),
                completion,
                result,
                self.admission.take(),
            )
        {
            let _ = task.await;
        }
    }

    async fn step(&mut self) -> Option<Bytes> {
        if let Some(chunk) = self.pending.pop_front() {
            return Some(chunk);
        }
        if self.terminal_result.is_some() {
            self.finalize().await;
            return None;
        }
        let mut collection = self.collection.take()?;
        let result = tokio::select! {
            result = &mut collection => Some(result),
            // Go's ticker arm: the heartbeat payload repeats for the whole
            // collect window — a single heartbeat then silence is exactly
            // the client-timeout case the heartbeat exists to prevent.
            _ = self.interval.tick() => None,
        };
        self.collection = Some(collection);
        let Some(result) = result else {
            // Go's ticker arm: the heartbeat payload repeats for the whole
            // collect window — a single heartbeat then silence is exactly
            // the client-timeout case the heartbeat exists to prevent.
            return Some(Bytes::from_static(JSON_HEARTBEAT));
        };
        // Cancellation attribution mirrors Go's `ctx.Err()` check after
        // every await: a cancelled lineage classifies as disconnected
        // (panel abort rewrites it to `aborted` in `complete`), never as
        // a stream failure — and no error body is written.
        if self.cancel.is_cancelled() {
            self.collecting = false;
            self.recorder.write_error(
                "client_disconnected",
                &Failure::plain("client disconnected"),
            );
            self.terminal_result = Some("disconnected");
            return if let Some(chunk) = self.pending.pop_front() {
                Some(chunk)
            } else {
                self.finalize().await;
                None
            };
        }
        match result {
            Ok(message) => {
                self.collecting = false;
                update_completion(
                    self.completion
                        .as_mut()
                        .expect("live body holds its completion"),
                    &message,
                );
                match self.protocol.encode_final(Some(&message), &self.model) {
                    Ok(body) => {
                        self.recorder.append_jsonl(
                            STAGE_HTTP_RESPONSE,
                            "response",
                            LogValue::Raw(body.clone()),
                        );
                        self.recorder.note_client_latency();
                        self.pending.push_back(Bytes::from(body));
                        self.terminal_result = Some("completed");
                    }
                    Err(err) => {
                        self.recorder.note_client_latency();
                        self.recorder.write_error("http_encode", &err);
                        self.pending.push_back(Bytes::from(
                            self.protocol.encode_error(&err, &self.recorder.dir_name()),
                        ));
                        self.terminal_result = Some("failed");
                    }
                }
            }
            Err(err) => {
                self.collecting = false;
                self.recorder.note_client_latency();
                self.recorder.write_error("response_event", &err);
                self.pending.push_back(Bytes::from(
                    self.protocol.encode_error(&err, &self.recorder.dir_name()),
                ));
                self.terminal_result = Some("failed");
            }
        }
        if let Some(chunk) = self.pending.pop_front() {
            Some(chunk)
        } else {
            self.finalize().await;
            None
        }
    }
}

impl Drop for JsonBody {
    /// Same disconnect contract as [`SseBody`]: cancel the lineage and
    /// finalize. The error stage mirrors the producer's attribution —
    /// `response_event` when the collect itself was abandoned,
    /// `client_disconnected` once a terminal chunk existed (the failed
    /// `tx.send` path).
    fn drop(&mut self) {
        self.cancel.cancel();
        if self.finalized {
            return;
        }
        self.finalized = true;
        if self.terminal_result.is_none() {
            self.terminal_result = Some("disconnected");
            if self.collecting {
                self.recorder
                    .write_error("response_event", &Failure::plain("client disconnected"));
            } else {
                self.recorder.write_error(
                    "client_disconnected",
                    &Failure::plain("client disconnected"),
                );
            }
        }
        if let Some(completion) = self.completion.take() {
            let _ = spawn_complete(
                self.recorder.clone(),
                completion,
                self.terminal_result.unwrap_or("disconnected"),
                self.admission.take(),
            );
        }
    }
}

pub async fn json_response(
    mut source: Box<dyn HttpEventStream>,
    protocol_kind: ProtocolKind,
    model: String,
    cancel: CancellationToken,
    recorder: Recorder,
    admission: Admission,
    mut completion: Completion,
) -> Result<(Response, bool), Failure> {
    let protocol = protocol_kind.encoder();
    if protocol_kind == ProtocolKind::Anthropic {
        let message = collect_final(&mut *source, &cancel, &recorder).await?;
        update_completion(&mut completion, &message);
        let body = protocol.encode_final(Some(&message), &model)?;
        recorder.append_jsonl(STAGE_HTTP_RESPONSE, "response", LogValue::Raw(body.clone()));
        recorder.note_client_latency();
        let mut response = Response::new(Body::from(body));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        return Ok((response, false));
    }

    let first_wait = {
        let collection = collect_final(&mut *source, &cancel, &recorder);
        tokio::pin!(collection);
        tokio::time::timeout(KEEPALIVE_INTERVAL, &mut collection).await
    };
    if let Ok(result) = first_wait {
        let message = result?;
        update_completion(&mut completion, &message);
        let body = protocol.encode_final(Some(&message), &model)?;
        recorder.append_jsonl(STAGE_HTTP_RESPONSE, "response", LogValue::Raw(body.clone()));
        recorder.note_client_latency();
        let mut response = Response::new(Body::from(body));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok((response, false))
    } else {
        let mut pending = VecDeque::new();
        pending.push_back(Bytes::from_static(JSON_HEARTBEAT));
        let mut interval = tokio::time::interval(KEEPALIVE_INTERVAL);
        interval.reset();
        let collect_cancel = cancel.clone();
        let collect_recorder = recorder.clone();
        let collection = Box::pin(async move {
            collect_final(&mut *source, &collect_cancel, &collect_recorder).await
        });
        let state = JsonBody {
            collection: Some(collection),
            protocol,
            model,
            recorder,
            cancel,
            admission: Some(admission),
            completion: Some(completion),
            pending,
            interval,
            collecting: true,
            terminal_result: None,
            finalized: false,
        };
        let body = Body::from_stream(stream::unfold(state, |mut state| async move {
            state
                .step()
                .await
                .map(|chunk| (Ok::<_, Infallible>(chunk), state))
        }));
        Ok((committed_response(body, false), true))
    }
}

pub fn encode_http_error(
    protocol_kind: ProtocolKind,
    failure: Failure,
    stage: &str,
    debug_ref: &str,
) -> Vec<u8> {
    protocol_kind.encoder().encode_http_error(&HttpError {
        client_fixable: failure.client_fixable,
        failure,
        stage: stage.to_string(),
        debug_ref: debug_ref.to_string(),
    })
}

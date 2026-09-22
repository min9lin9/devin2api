//! `OpenAI` Responses WebSocket transport and per-connection replay state.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use futures_util::{Sink, SinkExt, StreamExt};
use http::{HeaderMap, HeaderValue, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Map, Value, json};
use tokio::sync::{OwnedSemaphorePermit, mpsc};
use tokio_util::sync::CancellationToken;

use super::http::{Inner, RejectReason};

pub const SUBPROTOCOL: &str = "responses_websockets=2026-02-06";
pub const WRITE_DEADLINE: Duration = Duration::from_secs(60);
pub const FIRST_MESSAGE_TIMEOUT: Duration = Duration::from_secs(30);
pub const IDLE_TIMEOUT: Duration = Duration::from_mins(5);
pub const PING_INTERVAL: Duration = Duration::from_mins(2);
pub const MAX_CONNECTIONS: usize = 256;
pub const MAX_QUEUED_BYTES: usize = 64 << 20;
pub const MAX_TRANSCRIPT_BYTES: usize = 32 << 20;
const MESSAGE_QUEUE_CAPACITY: usize = 64;

/// Marker attached only to synthetic per-turn requests from this transport.
#[derive(Clone, Copy)]
pub(crate) struct WsTurn;

/// Exact lifecycle notifications for deterministic framed-transport tests.
#[derive(Default)]
pub struct WebSocketProbe {
    ping_armed: tokio::sync::Notify,
    write_blocked: tokio::sync::Notify,
    watch_write_blocked: AtomicBool,
}

impl WebSocketProbe {
    pub async fn ping_armed(&self) {
        self.ping_armed.notified().await;
    }

    pub fn arm_write_blocked(&self) {
        self.watch_write_blocked.store(true, Ordering::Release);
    }

    pub async fn write_blocked(&self) {
        self.write_blocked.notified().await;
    }

    fn note_write_blocked(&self) {
        if self.watch_write_blocked.swap(false, Ordering::AcqRel) {
            self.write_blocked.notify_one();
        }
    }
}

struct ProbedSink<S> {
    inner: S,
    probe: Option<Arc<WebSocketProbe>>,
}

impl<S> Sink<Message> for ProbedSink<S>
where
    S: Sink<Message> + Unpin,
{
    type Error = S::Error;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_ready(context);
        if result.is_pending()
            && let Some(probe) = &self.probe
        {
            probe.note_write_blocked();
        }
        result
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_flush(context);
        if result.is_pending()
            && let Some(probe) = &self.probe
        {
            probe.note_write_blocked();
        }
        result
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let result = Pin::new(&mut self.inner).poll_close(context);
        if result.is_pending()
            && let Some(probe) = &self.probe
        {
            probe.note_write_blocked();
        }
        result
    }
}

static CONNECTIONS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

fn connection_slots() -> Arc<tokio::sync::Semaphore> {
    CONNECTIONS
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS)))
        .clone()
}

/// HTTP GET upgrade entry, invoked by the shared authenticated `/v1` router.
pub(crate) async fn upgrade(
    State(inner): State<Arc<Inner>>,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
    headers: HeaderMap,
) -> Response {
    let Ok(ws) = ws else {
        let mut response = Response::new(Body::from("Method Not Allowed\n"));
        *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        response
            .headers_mut()
            .insert(http::header::ALLOW, HeaderValue::from_static("POST"));
        return response;
    };
    if super::http::ws_is_draining(&inner) {
        super::http::ws_reject(&inner, RejectReason::Draining);
        let mut response = json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error":{"message":"server is draining for restart; retry the request","type":"server_error","code":null,"param":null}}),
        );
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
        return response;
    }
    let Ok(permit) = connection_slots().try_acquire_owned() else {
        super::http::ws_reject(&inner, RejectReason::WsConnectionLimit);
        return json_response(
            StatusCode::TOO_MANY_REQUESTS,
            &json!({"error":{
                "message":"websocket connection limit reached; close an existing connection or retry later",
                "type":"rate_limit_error",
                "code":"responses_websocket_connection_limit_exceeded"
            }}),
        );
    };
    let turn_state = headers.get("x-codex-turn-state").cloned();
    let upgrade_headers = headers.clone();
    let probe = super::http::ws_probe(&inner);
    let mut response = ws
        .protocols([SUBPROTOCOL])
        .max_message_size(MAX_TRANSCRIPT_BYTES)
        .max_frame_size(MAX_TRANSCRIPT_BYTES)
        .on_failed_upgrade(|error| tracing::debug!(%error, "websocket upgrade failed"))
        .on_upgrade(move |socket| run_connection(socket, inner, upgrade_headers, permit, probe))
        .into_response();
    if let Some(value) = turn_state {
        response.headers_mut().insert("x-codex-turn-state", value);
    }
    response
}

fn json_response(status: StatusCode, value: &Value) -> Response {
    let mut response = Response::new(Body::from(serde_json::to_vec(&value).expect("JSON value")));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

enum Inbound {
    Message(Message, usize),
    ReadClosed,
    QueueExceeded,
}

// One connection lifecycle: reader/writer tasks, turn serialization,
// close semantics — mirrors the Go websocket handler for parity review.
#[allow(clippy::too_many_lines)]
async fn run_connection(
    socket: WebSocket,
    inner: Arc<Inner>,
    headers: HeaderMap,
    _connection_permit: OwnedSemaphorePermit,
    probe: Option<Arc<WebSocketProbe>>,
) {
    let (sink, mut source) = socket.split();
    let mut sink = ProbedSink {
        inner: sink,
        probe: probe.clone(),
    };
    let connection_cancel = CancellationToken::new();
    let active_turn = Arc::new(Mutex::new(None::<CancellationToken>));
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let queue_exceeded = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::channel(MESSAGE_QUEUE_CAPACITY);
    let reader_cancel = connection_cancel.clone();
    let reader_turn = active_turn.clone();
    let reader_queued = queued_bytes.clone();
    let reader_exceeded = queue_exceeded.clone();
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let budget = if first {
                FIRST_MESSAGE_TIMEOUT
            } else {
                IDLE_TIMEOUT
            };
            let next = tokio::time::timeout(budget, source.next()).await;
            let Ok(Some(Ok(message))) = next else {
                reader_cancel.cancel();
                let _ = tx.send(Inbound::ReadClosed).await;
                return;
            };
            first = false;
            if let Message::Text(text) = &message
                && text.contains("\"response.cancel\"")
                && event_type(text.as_bytes()) == Some("response.cancel")
                && let Some(cancel) = reader_turn
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            {
                cancel.cancel();
                continue;
            }
            let size = message_size(&message);
            let total = reader_queued.fetch_add(size, Ordering::AcqRel) + size;
            if total > MAX_QUEUED_BYTES {
                reader_exceeded.store(1, Ordering::Release);
                reader_cancel.cancel();
                let _ = tx.send(Inbound::QueueExceeded).await;
                return;
            }
            if tx.send(Inbound::Message(message, size)).await.is_err() {
                reader_queued.fetch_sub(size, Ordering::AcqRel);
                return;
            }
        }
    });

    let mut session = Session::default();
    let mut ping = Box::pin(periodic_ping_frame());
    if let Some(probe) = &probe {
        probe.ping_armed.notify_one();
    }
    loop {
        let inbound = tokio::select! {
            biased;
            inbound = rx.recv() => inbound,
            message = &mut ping => {
                if send_message(&mut sink, message).await.is_err() { return; }
                ping.set(periodic_ping_frame());
                continue;
            }
        };
        let Some(inbound) = inbound else { return };
        let message = match inbound {
            Inbound::ReadClosed => return,
            Inbound::QueueExceeded => {
                close_too_big(&mut sink, "inbound queue byte limit exceeded").await;
                return;
            }
            Inbound::Message(message, size) => {
                queued_bytes.fetch_sub(size, Ordering::AcqRel);
                message
            }
        };
        let Message::Text(payload) = message else {
            if matches!(message, Message::Close(_)) {
                return;
            }
            if matches!(message, Message::Ping(_) | Message::Pong(_)) {
                continue;
            }
            if write_error(
                &mut sink,
                400,
                "invalid_request_error",
                "unsupported_frame",
                "",
                "only text websocket messages are supported",
            )
            .await
            .is_err()
            {
                return;
            }
            continue;
        };

        let generate = generate_disabled(payload.as_bytes());
        let normalized = match session.normalize(payload.as_bytes()) {
            Ok(normalized) => normalized,
            Err(error) => {
                let (code, param) = match error.kind {
                    SessionErrorKind::PreviousNotFound => {
                        ("previous_response_not_found", "previous_response_id")
                    }
                    SessionErrorKind::Unsupported => ("unsupported_event", ""),
                    SessionErrorKind::Invalid => ("invalid_request", ""),
                };
                if write_error(
                    &mut sink,
                    400,
                    "invalid_request_error",
                    code,
                    param,
                    &error.message,
                )
                .await
                .is_err()
                {
                    return;
                }
                continue;
            }
        };
        if generate {
            match prewarm(&mut sink, &normalized).await {
                Ok(result) => session.commit(result),
                Err(()) => return,
            }
            continue;
        }

        let turn_cancel = connection_cancel.child_token();
        *active_turn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(turn_cancel.clone());
        let outcome = run_turn(
            inner.clone(),
            &headers,
            normalized,
            turn_cancel.clone(),
            &connection_cancel,
            &mut sink,
        )
        .await;
        *active_turn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        if queue_exceeded.load(Ordering::Acquire) != 0 {
            close_too_big(&mut sink, "inbound queue byte limit exceeded").await;
            return;
        }
        let Ok(outcome) = outcome else { return };
        match outcome.terminal {
            Terminal::Completed => session.commit(outcome.result),
            Terminal::Failed | Terminal::Error => session.require_replacement(),
            Terminal::Draining => return,
            Terminal::None => {
                session.require_replacement();
                let (status, code, message) =
                    if turn_cancel.is_cancelled() && !connection_cancel.is_cancelled() {
                        (400, "turn_cancelled", "turn cancelled by client")
                    } else if connection_cancel.is_cancelled() {
                        return;
                    } else {
                        (
                            502,
                            "upstream_stream_interrupted",
                            "upstream response was interrupted; resend the full conversation input",
                        )
                    };
                if write_error(
                    &mut sink,
                    status,
                    if status == 400 {
                        "invalid_request_error"
                    } else {
                        "server_error"
                    },
                    code,
                    "",
                    message,
                )
                .await
                .is_err()
                {
                    return;
                }
            }
        }
    }
}

/// Produce the exact control frame used by the connection heartbeat after
/// one full ping interval. Exposed for deterministic paused-clock contract
/// tests; production and tests execute this same future.
#[doc(hidden)]
pub async fn periodic_ping_frame() -> Message {
    tokio::time::sleep(PING_INTERVAL).await;
    Message::Ping(Vec::new().into())
}

fn message_size(message: &Message) -> usize {
    match message {
        Message::Text(value) => value.len(),
        Message::Binary(value) | Message::Ping(value) | Message::Pong(value) => value.len(),
        Message::Close(frame) => frame.as_ref().map_or(0, |frame| frame.reason.len() + 2),
    }
}

async fn send_message<S>(sink: &mut S, message: Message) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    tokio::time::timeout(WRITE_DEADLINE, sink.send(message))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

async fn close_too_big<S>(sink: &mut S, reason: &'static str)
where
    S: futures_util::Sink<Message> + Unpin,
{
    let _ = send_message(
        sink,
        Message::Close(Some(CloseFrame {
            code: 1009,
            reason: Utf8Bytes::from_static(reason),
        })),
    )
    .await;
}

async fn write_json<S>(sink: &mut S, value: &Value) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    send_message(
        sink,
        Message::Text(serde_json::to_string(value).map_err(|_| ())?.into()),
    )
    .await
}

async fn write_error<S>(
    sink: &mut S,
    status: u16,
    error_type: &str,
    code: &str,
    param: &str,
    message: &str,
) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let mut error = json!({"message":message,"type":error_type});
    if !code.is_empty() {
        error["code"] = Value::String(code.to_string());
    }
    if !param.is_empty() {
        error["param"] = Value::String(param.to_string());
    }
    write_json(sink, &json!({"type":"error","status":status,"error":error})).await
}

#[derive(Default)]
struct TurnOutcome {
    terminal: Terminal,
    result: TurnResult,
}

#[derive(Clone, Copy, Default)]
enum Terminal {
    #[default]
    None,
    Completed,
    Failed,
    Error,
    Draining,
}

// One upstream turn: request build, stream pump, terminal handling —
// mirrors the Go turn loop for parity review.
#[allow(clippy::too_many_lines)]
async fn run_turn<S>(
    inner: Arc<Inner>,
    headers: &HeaderMap,
    body: Vec<u8>,
    turn_cancel: CancellationToken,
    connection_cancel: &CancellationToken,
    sink: &mut S,
) -> Result<TurnOutcome, ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let mut builder = http::Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("content-type", "application/json");
    for name in [
        "authorization",
        "x-api-key",
        "x-session-id",
        "user-agent",
        "session-id",
        "session_id",
        "thread-id",
        "x-codex-turn-metadata",
        "x-client-request-id",
    ] {
        if let Some(value) = headers.get(name) {
            builder = builder.header(name, value);
        }
    }
    let mut request = builder.body(Body::from(body)).map_err(|_| ())?;
    request.extensions_mut().insert(WsTurn);
    let response = tokio::select! {
        biased;
        () = connection_cancel.cancelled() => return Ok(TurnOutcome::default()),
        () = turn_cancel.cancelled() => return Ok(TurnOutcome::default()),
        response = super::http::ws_run_turn(inner.clone(), request) => response,
    };
    let status = response.status().as_u16();
    if status == StatusCode::TOO_MANY_REQUESTS.as_u16() {
        write_error(
            sink,
            status,
            "rate_limit_error",
            "rate_limit",
            "",
            "server is busy, please try again later",
        )
        .await?;
        return Ok(TurnOutcome {
            terminal: Terminal::Error,
            ..TurnOutcome::default()
        });
    }
    if status == StatusCode::SERVICE_UNAVAILABLE.as_u16() && super::http::ws_is_draining(&inner) {
        write_error(
            sink,
            status,
            "server_error",
            "server_draining",
            "",
            "server is draining for restart; resend the request",
        )
        .await?;
        return Ok(TurnOutcome {
            terminal: Terminal::Draining,
            ..TurnOutcome::default()
        });
    }
    let debug_ref = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let mut response_body = response.into_body();
    let mut buffered = Vec::new();
    let mut outcome = TurnOutcome::default();
    loop {
        let frame = tokio::select! {
            biased;
            () = connection_cancel.cancelled() => return Ok(outcome),
            () = turn_cancel.cancelled() => return Ok(outcome),
            frame = response_body.frame() => frame,
        };
        let Some(frame) = frame else { break };
        let frame = frame.map_err(|_| ())?;
        if let Ok(data) = frame.into_data() {
            buffered.extend_from_slice(&data);
            while let Some(index) = buffered.windows(2).position(|pair| pair == b"\n\n") {
                let frame = buffered.drain(..index + 2).collect::<Vec<_>>();
                if frame.starts_with(b":") {
                    send_message(sink, Message::Ping(Vec::new().into())).await?;
                    continue;
                }
                if let Some(data) = sse_data(&frame) {
                    process_event(data, debug_ref.as_deref(), &mut outcome, sink).await?;
                }
            }
        }
    }
    let tail = trim_ascii(&buffered);
    if !tail.is_empty() {
        let mut value: Value = serde_json::from_slice(tail)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(tail).into_owned()));
        if value.get("type").is_none() && value.get("error").is_some() {
            value = json!({"type":"error","status":status,"error":value["error"].clone()});
        }
        outcome.terminal = Terminal::Error;
        write_json(sink, &value).await?;
    }
    Ok(outcome)
}

fn sse_data(frame: &[u8]) -> Option<&[u8]> {
    frame.split(|byte| *byte == b'\n').find_map(|line| {
        line.strip_prefix(b"data: ")
            .or_else(|| line.strip_prefix(b"data:"))
    })
}

async fn process_event<S>(
    data: &[u8],
    debug_ref: Option<&str>,
    outcome: &mut TurnOutcome,
    sink: &mut S,
) -> Result<(), ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let mut value: Value = serde_json::from_slice(data).map_err(|_| ())?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if kind == "response.created"
        && let Some(debug_ref) = debug_ref
        && !debug_ref.is_empty()
        && let Some(response) = value.get_mut("response").and_then(Value::as_object_mut)
    {
        response.insert("debug_ref".into(), Value::String(debug_ref.to_string()));
    }
    if value.pointer("/error/code").and_then(Value::as_str) == Some("message_too_big") {
        close_too_big(sink, "upstream websocket message too big").await;
        return Err(());
    }
    if kind == "response.output_item.done"
        && let Some(item) = value.get("item").cloned()
    {
        let index = value.get("output_index").and_then(Value::as_i64);
        if outcome.result.collect(index, item).is_err() {
            close_too_big(sink, "response output exceeds websocket transcript limit").await;
            return Err(());
        }
    }
    match kind.as_str() {
        "response.completed" | "response.done" | "response.incomplete" => {
            outcome.terminal = Terminal::Completed;
            if let Some(response) = value.get("response") {
                outcome.result.response_id = response
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                if let Some(output) = response.get("output").and_then(Value::as_array)
                    && !output.is_empty()
                {
                    outcome.result.output = output.clone();
                }
            }
        }
        "response.failed" => {
            outcome.terminal = Terminal::Failed;
            outcome.result.response_id = value
                .pointer("/response/id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
        }
        "error" => outcome.terminal = Terminal::Error,
        _ => {}
    }
    write_json(sink, &value).await
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(u8::is_ascii_whitespace) {
        value = &value[1..];
    }
    while value.last().is_some_and(u8::is_ascii_whitespace) {
        value = &value[..value.len() - 1];
    }
    value
}

#[derive(Default)]
struct TurnResult {
    output: Vec<Value>,
    indexed: BTreeMap<i64, Value>,
    unindexed: Vec<Value>,
    output_bytes: usize,
    response_id: String,
}

impl TurnResult {
    fn collect(&mut self, index: Option<i64>, item: Value) -> Result<(), ()> {
        let bytes = serde_json::to_vec(&item).map_err(|_| ())?.len();
        if let Some(index) = index.filter(|index| *index >= 0) {
            if let Some(old) = self.indexed.insert(index, item) {
                self.output_bytes = self
                    .output_bytes
                    .saturating_sub(serde_json::to_vec(&old).map_err(|_| ())?.len());
            }
        } else {
            self.unindexed.push(item);
        }
        self.output_bytes += bytes;
        if self.output_bytes > MAX_TRANSCRIPT_BYTES {
            return Err(());
        }
        Ok(())
    }

    fn final_output(mut self) -> Vec<Value> {
        if !self.output.is_empty() {
            return self.output;
        }
        let mut output: Vec<Value> = self
            .indexed
            .into_values()
            .filter(complete_replay_item)
            .collect();
        output.extend(self.unindexed.drain(..).filter(complete_replay_item));
        output
    }
}

async fn prewarm<S>(sink: &mut S, request: &[u8]) -> Result<TurnResult, ()>
where
    S: futures_util::Sink<Message> + Unpin,
{
    let value: Value = serde_json::from_slice(request).map_err(|_| ())?;
    let id = crate::randid::prefixed("resp_prewarm_");
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut response = json!({
        "id":id,"object":"response","created_at":created_at,"status":"in_progress",
        "background":false,"error":null,"model":value.get("model").and_then(Value::as_str).unwrap_or_default(),"output":[]
    });
    write_json(
        sink,
        &json!({"type":"response.created","sequence_number":0,"response":response.clone()}),
    )
    .await?;
    response["status"] = Value::String("completed".into());
    response["usage"] = json!({"input_tokens":0,"output_tokens":0,"total_tokens":0});
    write_json(
        sink,
        &json!({"type":"response.completed","sequence_number":1,"response":response}),
    )
    .await?;
    Ok(TurnResult {
        response_id: id,
        ..TurnResult::default()
    })
}

fn event_type(payload: &[u8]) -> Option<&'static str> {
    let value: Value = serde_json::from_slice(payload).ok()?;
    (value.get("type")?.as_str()? == "response.cancel").then_some("response.cancel")
}

fn generate_disabled(payload: &[u8]) -> bool {
    serde_json::from_slice::<Value>(payload)
        .ok()
        .and_then(|value| value.get("generate").and_then(Value::as_bool))
        .is_some_and(|value| !value)
}

#[derive(Default)]
struct Session {
    last_top: Option<Map<String, Value>>,
    last_items: Vec<Value>,
    last_output: Vec<Value>,
    last_response_id: String,
    pending_calls: Vec<String>,
    replacement_needed: bool,
    staged_top: Option<Map<String, Value>>,
    staged_items: Vec<Value>,
}

struct SessionError {
    kind: SessionErrorKind,
    message: String,
}
enum SessionErrorKind {
    Invalid,
    Unsupported,
    PreviousNotFound,
}
impl SessionError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: SessionErrorKind::Invalid,
            message: message.into(),
        }
    }
    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: SessionErrorKind::Unsupported,
            message: message.into(),
        }
    }
    fn previous(message: impl Into<String>) -> Self {
        Self {
            kind: SessionErrorKind::PreviousNotFound,
            message: message.into(),
        }
    }
}

impl Session {
    fn normalize(&mut self, payload: &[u8]) -> Result<Vec<u8>, SessionError> {
        let value: Value = serde_json::from_slice(payload)
            .map_err(|_| SessionError::invalid("invalid websocket request JSON"))?;
        let mut top = value
            .as_object()
            .cloned()
            .ok_or_else(|| SessionError::unsupported("unsupported websocket request type: \"\""))?;
        let request_type = top
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if request_type != "response.create" && request_type != "response.append" {
            return Err(SessionError::unsupported(format!(
                "unsupported websocket request type: {request_type:?}"
            )));
        }
        let previous = top
            .get("previous_response_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        if !previous.is_empty() && !previous.starts_with("resp_") {
            return Err(SessionError::invalid(format!(
                "previous_response_id {previous:?} is not a response id"
            )));
        }
        if self.last_top.is_none() {
            if request_type == "response.append" {
                return Err(SessionError::invalid(
                    "response.append received before response.create",
                ));
            }
            if !previous.is_empty() {
                return Err(SessionError::previous(
                    "previous response is not available on this websocket; resend the full conversation input without previous_response_id",
                ));
            }
            if top
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .is_empty()
            {
                return Err(SessionError::invalid(
                    "missing model in response.create request",
                ));
            }
            strip_envelope(&mut top);
            top.entry("input")
                .or_insert_with(|| Value::Array(Vec::new()));
            top.insert("stream".into(), Value::Bool(true));
            let items = array_items(top.get("input"));
            return self.finish(top, items);
        }
        if let Some(input) = top.get("input")
            && !input.is_array()
        {
            return Err(SessionError::invalid(
                "websocket request requires array field: input",
            ));
        }
        let next = array_items(top.get("input"));
        if contains_completed_transcript(&next) {
            return self.replacement(top);
        }
        if !previous.is_empty() && previous != self.last_response_id {
            return Err(SessionError::previous(format!(
                "previous response is not available on this websocket; resend the full conversation input without previous_response_id: {previous:?}"
            )));
        }
        if self.replacement_needed && request_type == "response.create" && previous.is_empty() {
            return self.replacement(top);
        }
        if !self.pending_calls.is_empty() && !satisfies_calls(&next, &self.pending_calls) {
            if !previous.is_empty() || request_type == "response.append" {
                return Err(SessionError::invalid(
                    "incremental websocket request is missing output for a pending tool call",
                ));
            }
            return self.replacement(top);
        }
        let mut merged = self.last_items.clone();
        merged.extend(self.last_output.clone());
        merged.extend(next);
        merged = dedupe_items(merged);
        inherit(&mut top, self.last_top.as_ref().expect("last top"));
        top.insert("input".into(), Value::Array(merged.clone()));
        self.finish(top, merged)
    }

    fn replacement(&mut self, mut top: Map<String, Value>) -> Result<Vec<u8>, SessionError> {
        self.replacement_needed = false;
        inherit(&mut top, self.last_top.as_ref().expect("last top"));
        let items = array_items(top.get("input"));
        self.finish(top, items)
    }

    fn finish(
        &mut self,
        top: Map<String, Value>,
        items: Vec<Value>,
    ) -> Result<Vec<u8>, SessionError> {
        let normalized =
            serde_json::to_vec(&top).map_err(|error| SessionError::invalid(error.to_string()))?;
        if normalized.len() > MAX_TRANSCRIPT_BYTES {
            return Err(SessionError::invalid(format!(
                "websocket transcript exceeds {MAX_TRANSCRIPT_BYTES} byte limit; compact and replay the conversation"
            )));
        }
        self.staged_top = Some(top);
        self.staged_items = items;
        Ok(normalized)
    }

    fn commit(&mut self, result: TurnResult) {
        self.last_top = self.staged_top.take();
        self.last_items = std::mem::take(&mut self.staged_items);
        self.last_response_id = result.response_id.trim().to_string();
        self.last_output = result.final_output();
        self.pending_calls = pending_calls(&self.last_output);
        self.replacement_needed = false;
    }

    fn require_replacement(&mut self) {
        self.replacement_needed = true;
    }
}

fn strip_envelope(top: &mut Map<String, Value>) {
    top.remove("type");
    top.remove("previous_response_id");
    top.remove("generate");
}

fn inherit(top: &mut Map<String, Value>, last: &Map<String, Value>) {
    strip_envelope(top);
    if top
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .is_empty()
        && let Some(model) = last.get("model")
    {
        top.insert("model".into(), model.clone());
    }
    if !top.contains_key("instructions")
        && let Some(instructions) = last.get("instructions")
    {
        top.insert("instructions".into(), instructions.clone());
    }
    top.insert("stream".into(), Value::Bool(true));
}

fn array_items(value: Option<&Value>) -> Vec<Value> {
    value.and_then(Value::as_array).cloned().unwrap_or_default()
}

fn item_type(item: &Value) -> &str {
    item.get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
}
fn item_string<'a>(item: &'a Value, key: &str) -> &'a str {
    item.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
}
fn is_tool_call(item: &Value) -> bool {
    matches!(item_type(item), "function_call" | "custom_tool_call")
}
fn is_tool_output(item: &Value) -> bool {
    matches!(
        item_type(item),
        "function_call_output" | "custom_tool_call_output"
    )
}
fn complete_replay_item(item: &Value) -> bool {
    if !is_tool_call(item) {
        return true;
    }
    !item_string(item, "call_id").is_empty()
        && !item_string(item, "name").is_empty()
        && if item_type(item) == "custom_tool_call" {
            item.get("input").is_some_and(Value::is_string)
        } else {
            item.get("arguments").is_some_and(Value::is_string)
        }
}

fn dedupe_items(items: Vec<Value>) -> Vec<Value> {
    let referenced: HashSet<String> = items
        .iter()
        .filter(|item| is_tool_output(item))
        .map(|item| item_string(item, "call_id").to_string())
        .filter(|id| !id.is_empty())
        .collect();
    let mut first_calls = HashSet::new();
    let mut filtered = Vec::new();
    for item in items {
        let call_id = item_string(&item, "call_id").to_string();
        if is_tool_call(&item) && !call_id.is_empty() && !first_calls.insert(call_id) {
            continue;
        }
        filtered.push(item);
    }
    let mut keep = HashMap::<String, usize>::new();
    for (index, item) in filtered.iter().enumerate().rev() {
        let id = item_string(item, "id");
        if id.is_empty() {
            continue;
        }
        if let Some(existing) = keep.get(id).copied() {
            let old_referenced = referenced.contains(item_string(&filtered[existing], "call_id"));
            let new_referenced = referenced.contains(item_string(item, "call_id"));
            if !old_referenced && new_referenced {
                keep.insert(id.to_string(), index);
            }
        } else {
            keep.insert(id.to_string(), index);
        }
    }
    filtered
        .into_iter()
        .enumerate()
        .filter(|(index, item)| {
            let id = item_string(item, "id");
            id.is_empty() || keep.get(id) == Some(index)
        })
        .map(|(_, item)| item)
        .collect()
}

fn pending_calls(output: &[Value]) -> Vec<String> {
    let mut seen = HashSet::new();
    output
        .iter()
        .filter(|item| complete_replay_item(item) && is_tool_call(item))
        .map(|item| item_string(item, "call_id").to_string())
        .filter(|id| !id.is_empty() && seen.insert(id.clone()))
        .collect()
}

fn satisfies_calls(items: &[Value], pending: &[String]) -> bool {
    let outputs: HashSet<&str> = items
        .iter()
        .filter(|item| is_tool_output(item))
        .map(|item| item_string(item, "call_id"))
        .collect();
    pending.iter().all(|id| outputs.contains(id.as_str()))
}

fn contains_completed_transcript(items: &[Value]) -> bool {
    const SUMMARY: &str = "Another language model started to solve this problem and produced a summary of its thinking process.";
    items.iter().any(|item| {
        matches!(
            item_type(item),
            "compaction" | "compaction_summary" | "function_call" | "custom_tool_call"
        ) || item_string(item, "role") == "assistant"
            || (item_string(item, "role") == "user" && message_text(item).starts_with(SUMMARY))
    })
}

fn message_text(item: &Value) -> String {
    match item.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter(|part| matches!(item_type(part), "input_text" | "text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect(),
        _ => String::new(),
    }
}

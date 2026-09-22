//! Task 11 upstream lifecycle contract: bounded streaming, retries and
//! cancellation composed over transport/catalog/projection/gate/decoder.
//!
//! The upstream is a raw-`TCP` `HTTP`/1.1 stub speaking the Connect wire
//! shapes the generated client uses (data envelopes + `END_STREAM`, unary
//! `JSON` errors). Every connection runs a pre-armed script — drops, held
//! responses, metadata ticks and `END_STREAM` errors are all deterministic,
//! and `connection: close` on every response makes wire sends equal
//! accepted connections. Timer paths run on Tokio's paused clock; every
//! wait is bounded by yield count or a pre-armed event — no sleeps.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use devin_proto::buffa::Message as _;
use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::debuglog::{self, Completion, Manager, RequestMeta, RetentionPolicy};
use devin2api::domain::request::{
    Content, Message as DMessage, RequestMessages, TextContent, UserMessage,
};
use devin2api::domain::response::{ResponseEvent, ResponseEventType, ResponseStream, StopReason};
use devin2api::upstream::catalog::{Adapter, AdapterConfig};
use devin2api::upstream::gate::GateConfig;
use devin2api::upstream::transport::static_token;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const CHAT_PATH: &str = "/exa.api_server_pb.ApiServerService/GetChatMessage";
const CATALOG_PATH: &str = "/exa.api_server_pb.ApiServerService/GetCliModelConfigs";

// ---------------------------------------------------------------------------
// Scripted raw-TCP stub
// ---------------------------------------------------------------------------

/// What one chat connection does after the request body is read.
struct ChatScript {
    /// Delay all response bytes until this fires (connect-phase hold).
    hold_headers: Option<Arc<Notify>>,
    /// Non-200 Connect `JSON` error (terminal, no stream).
    json_error: Option<(u16, &'static str, &'static str)>,
    /// Data frames sent after the 200 headers.
    frames: Vec<pb::GetChatMessageResponse>,
    /// What happens after `frames`.
    tail: Tail,
}

enum Tail {
    /// Send `END_STREAM` `{}` and close.
    EndStream,
    /// Close without `END_STREAM` (mid-stream `EOF`).
    Close,
    /// Send an `END_STREAM` error envelope and close.
    Error(&'static str, &'static str),
    /// Hold until the notify fires (or the peer goes away), then `then`.
    Hold(Arc<Notify>, Box<Tail>),
    /// Send one metadata-only frame every interval until the conn dies.
    Tick(Duration),
    /// Close without any response bytes.
    DropNow,
}

fn script_frames(frames: Vec<pb::GetChatMessageResponse>) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames,
        tail: Tail::EndStream,
    }
}

fn script_drop() -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames: Vec::new(),
        tail: Tail::DropNow,
    }
}

fn script_json_error(status: u16, code: &'static str, message: &'static str) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: Some((status, code, message)),
        frames: Vec::new(),
        tail: Tail::EndStream,
    }
}

fn script_close_after(frames: Vec<pb::GetChatMessageResponse>) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames,
        tail: Tail::Close,
    }
}

fn script_end_error(
    frames: Vec<pb::GetChatMessageResponse>,
    code: &'static str,
    message: &'static str,
) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames,
        tail: Tail::Error(code, message),
    }
}

fn script_hold(
    frames: Vec<pb::GetChatMessageResponse>,
    release: Arc<Notify>,
    then: Tail,
) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames,
        tail: Tail::Hold(release, Box::new(then)),
    }
}

fn script_tick(frames: Vec<pb::GetChatMessageResponse>, every: Duration) -> ChatScript {
    ChatScript {
        hold_headers: None,
        json_error: None,
        frames,
        tail: Tail::Tick(every),
    }
}

fn script_hold_headers(hold: Arc<Notify>, then: Tail) -> ChatScript {
    ChatScript {
        hold_headers: Some(hold),
        json_error: None,
        frames: Vec::new(),
        tail: then,
    }
}

fn delta(text: &str) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_text: Some(text.to_string()),
        ..Default::default()
    }
}

fn stop_frame() -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        stop_reason: Some(
            pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN,
        ),
        ..Default::default()
    }
}

/// A frame that produces no decoder events (metadata only) — feeds the
/// stall watchdog but never the progress watchdog.
fn liveness_frame() -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        request_id: Some("req-liveness".to_string()),
        ..Default::default()
    }
}

struct StubState {
    /// Requests fully read on the chat endpoint — the wire-send count.
    chat_sends: AtomicUsize,
    /// Currently open accepted connections.
    open_conns: AtomicUsize,
    /// Total accepted connections.
    accepted: AtomicUsize,
    /// `metadata.api_key` of each chat request, in arrival order.
    chat_api_keys: Mutex<Vec<String>>,
    /// Raw chat request bodies (protobuf payload of the first envelope).
    chat_bodies: Mutex<Vec<Vec<u8>>>,
    chat_scripts: Mutex<VecDeque<ChatScript>>,
}

struct Stub {
    base_url: String,
    state: Arc<StubState>,
    shutdown: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Stub {
    async fn start() -> Self {
        let state = Arc::new(StubState {
            chat_sends: AtomicUsize::new(0),
            open_conns: AtomicUsize::new(0),
            accepted: AtomicUsize::new(0),
            chat_api_keys: Mutex::new(Vec::new()),
            chat_bodies: Mutex::new(Vec::new()),
            chat_scripts: Mutex::new(VecDeque::new()),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let inner = state.clone();
        let stop = shutdown.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    accepted = listener.accept() => {
                        let Ok((sock, _)) = accepted else { return };
                        inner.open_conns.fetch_add(1, Ordering::SeqCst);
                        inner.accepted.fetch_add(1, Ordering::SeqCst);
                        let conn_state = inner.clone();
                        // The spawned connection future carries the full
                        // request pipeline; it is detached by design.
                        #[allow(clippy::large_futures)]
                        tokio::spawn(async move {
                            run_conn(sock, conn_state.clone()).await;
                            conn_state.open_conns.fetch_sub(1, Ordering::SeqCst);
                        });
                    }
                }
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            state,
            shutdown,
            task,
        }
    }

    fn push_chat(&self, script: ChatScript) {
        self.state.chat_scripts.lock().unwrap().push_back(script);
    }

    fn chat_sends(&self) -> usize {
        self.state.chat_sends.load(Ordering::SeqCst)
    }

    fn open_conns(&self) -> usize {
        self.state.open_conns.load(Ordering::SeqCst)
    }

    fn chat_api_keys(&self) -> Vec<String> {
        self.state.chat_api_keys.lock().unwrap().clone()
    }

    /// Decode the nth chat request's protobuf payload.
    fn chat_request(&self, n: usize) -> pb::GetChatMessageRequest {
        let bodies = self.state.chat_bodies.lock().unwrap();
        pb::GetChatMessageRequest::decode_from_slice(&bodies[n]).expect("decode chat request")
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

struct RequestHead {
    path: String,
    body: Vec<u8>,
}

/// Read one `HTTP`/1.1 request: headers, then the body by content-length or
/// chunked transfer encoding.
async fn read_request(r: &mut tokio::net::tcp::OwnedReadHalf) -> io::Result<RequestHead> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = r.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "eof in headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > 256 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "headers too large",
            ));
        }
    };
    let headers = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut rest = buf.split_off(header_end);
    let path = headers
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let lower = headers.to_ascii_lowercase();
    let content_length = lower
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let chunked = lower.contains("transfer-encoding: chunked");
    let body = if chunked {
        read_chunked(r, &mut rest).await?
    } else {
        while rest.len() < content_length {
            let n = r.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof in body"));
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        rest.truncate(content_length);
        rest
    };
    Ok(RequestHead { path, body })
}

async fn read_chunked(
    r: &mut tokio::net::tcp::OwnedReadHalf,
    rest: &mut Vec<u8>,
) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        // Read until a CRLF-terminated size line is available.
        let line_end = loop {
            if let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
                break pos;
            }
            let n = r.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof in chunk size",
                ));
            }
            rest.extend_from_slice(&chunk[..n]);
        };
        let size_text = String::from_utf8_lossy(&rest[..line_end]).into_owned();
        let size = usize::from_str_radix(size_text.trim(), 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?;
        rest.drain(..line_end + 2);
        if size == 0 {
            // Optional trailers then the final CRLF.
            loop {
                if let Some(pos) = rest.windows(2).position(|w| w == b"\r\n") {
                    rest.drain(..pos + 2);
                    if rest.starts_with(b"\r\n") || size == 0 {
                        // Consume trailer lines until an empty line.
                    }
                    break;
                }
                let n = r.read(&mut chunk).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "eof in trailers",
                    ));
                }
                rest.extend_from_slice(&chunk[..n]);
            }
            return Ok(body);
        }
        while rest.len() < size + 2 {
            let n = r.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof in chunk"));
            }
            rest.extend_from_slice(&chunk[..n]);
        }
        body.extend_from_slice(&rest[..size]);
        rest.drain(..size + 2);
    }
}

/// Connect streaming envelope: flag byte + big-endian u32 length + payload.
fn envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    out
}

async fn write_all(w: &mut tokio::net::tcp::OwnedWriteHalf, bytes: &[u8]) -> io::Result<()> {
    w.write_all(bytes).await
}

async fn write_chunk(w: &mut tokio::net::tcp::OwnedWriteHalf, payload: &[u8]) -> io::Result<()> {
    write_all(w, format!("{:x}\r\n", payload.len()).as_bytes()).await?;
    write_all(w, payload).await?;
    write_all(w, b"\r\n").await
}

/// True when the peer went away (`EOF` or reset) — used to release held
/// connections deterministically instead of parking forever.
async fn peer_gone(r: &mut tokio::net::tcp::OwnedReadHalf) -> bool {
    let mut buf = [0u8; 64];
    match r.read(&mut buf).await {
        Ok(0) | Err(_) => true,
        Ok(_) => false, // unexpected extra bytes; keep going
    }
}

#[allow(clippy::large_futures)]
async fn run_conn(sock: TcpStream, state: Arc<StubState>) {
    let (mut r, mut w) = sock.into_split();
    let Ok(req) = read_request(&mut r).await else {
        return;
    };
    if req.path == CATALOG_PATH {
        let _ = write_all(
            &mut w,
            b"HTTP/1.1 200 OK\r\ncontent-type: application/proto\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        )
        .await;
        return;
    }
    if req.path != CHAT_PATH {
        let _ = write_all(
            &mut w,
            b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        )
        .await;
        return;
    }
    // Envelope-framed request body: flag + u32 len + protobuf payload.
    let body = &req.body;
    if body.len() >= 5 {
        let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
        if body.len() >= 5 + len {
            let payload = body[5..5 + len].to_vec();
            if let Ok(decoded) = pb::GetChatMessageRequest::decode_from_slice(&payload) {
                state
                    .chat_api_keys
                    .lock()
                    .unwrap()
                    .push(decoded.metadata.api_key.clone().unwrap_or_default());
            }
            state.chat_bodies.lock().unwrap().push(payload);
        }
    }
    state.chat_sends.fetch_add(1, Ordering::SeqCst);
    let script = state
        .chat_scripts
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or_else(|| script_frames(Vec::new()));
    if let Some(hold) = &script.hold_headers {
        tokio::select! {
            () = hold.notified() => {}
            _ = peer_gone(&mut r) => return,
        }
    }
    if let Some((status, code, message)) = script.json_error {
        let reason = match status {
            401 => "Unauthorized",
            403 => "Forbidden",
            429 => "Too Many Requests",
            _ => "Error",
        };
        let body = format!(r#"{{"code":"{code}","message":"{message}"}}"#);
        let _ = write_all(
            &mut w,
            format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await;
        return;
    }
    if matches!(script.tail, Tail::DropNow) {
        return;
    }
    if write_all(
        &mut w,
        b"HTTP/1.1 200 OK\r\ncontent-type: application/connect+proto\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n",
    )
    .await
    .is_err()
    {
        return;
    }
    for frame in &script.frames {
        if write_chunk(&mut w, &envelope(0x00, &frame.encode_to_vec()))
            .await
            .is_err()
        {
            return;
        }
    }
    run_tail(&mut r, &mut w, &script.tail).await;
}

async fn run_tail(
    r: &mut tokio::net::tcp::OwnedReadHalf,
    w: &mut tokio::net::tcp::OwnedWriteHalf,
    tail: &Tail,
) {
    match tail {
        Tail::EndStream => {
            let _ = write_chunk(w, &envelope(0x02, b"{}")).await;
            let _ = write_all(w, b"0\r\n\r\n").await;
        }
        Tail::Close | Tail::DropNow => {}
        Tail::Error(code, message) => {
            let end = format!(r#"{{"error":{{"code":"{code}","message":"{message}"}}}}"#);
            let _ = write_chunk(w, &envelope(0x02, end.as_bytes())).await;
            let _ = write_all(w, b"0\r\n\r\n").await;
        }
        Tail::Hold(release, then) => {
            tokio::select! {
                () = release.notified() => {}
                _ = peer_gone(r) => return,
            }
            Box::pin(run_tail(r, w, then)).await;
        }
        Tail::Tick(every) => {
            let frame = envelope(0x00, &liveness_frame().encode_to_vec());
            loop {
                tokio::select! {
                    () = tokio::time::sleep(*every) => {
                        if write_chunk(w, &frame).await.is_err() {
                            return;
                        }
                    }
                    _ = peer_gone(r) => return,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Spin (bounded by yield count, never wall time) until `cond` holds.
async fn wait_until(cond: impl Fn() -> bool, what: &str) {
    for _ in 0..100_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {what}");
}

fn user_request(text: &str) -> RequestMessages {
    RequestMessages {
        messages: vec![DMessage::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: text.to_string(),
            })],
            ..Default::default()
        })],
        ..Default::default()
    }
}

fn adapter_for(stub: &Stub, token: &str) -> Adapter {
    Adapter::new(AdapterConfig {
        base_url: stub.base_url.clone(),
        token: token.to_string(),
        model: "swe-2-max".to_string(),
        ..Default::default()
    })
    .expect("adapter")
}

/// Drain the stream into an event list; a terminal `Err` is appended as
/// `Err` and ends the drain.
async fn collect(stream: &mut devin2api::upstream::retry::DevinStream) -> Vec<ResponseEvent> {
    let mut events = Vec::new();
    loop {
        match stream.recv().await {
            Ok(Some(event)) => events.push(event),
            Ok(None) => return events,
            Err(failure) => panic!("stream failed: {failure}"),
        }
    }
}

fn kinds(events: &[ResponseEvent]) -> Vec<ResponseEventType> {
    events.iter().map(|e| e.kind).collect()
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

/// Normal stream: withheld start rides the first content batch, events
/// arrive in decoder order, the terminal `done` is delivered once and
/// `recv` then reports end-of-stream forever.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn happy_path_events_and_once_only_terminal() {
    let stub = Stub::start().await;
    stub.push_chat(script_frames(vec![
        delta("hello "),
        delta("world"),
        stop_frame(),
    ]));
    let adapter = adapter_for(&stub, "t1");
    let cancel = CancellationToken::new();
    let mut stream = adapter
        .stream(
            user_request("hi"),
            cancel,
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(
        kinds(&events),
        vec![
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ]
    );
    let done = events.last().unwrap();
    assert_eq!(done.reason, Some(StopReason::Stop));
    assert_eq!(
        done.message
            .as_ref()
            .unwrap()
            .content
            .iter()
            .map(|c| match c {
                Content::Text(t) => t.text.clone(),
                _ => String::new(),
            })
            .collect::<String>(),
        "hello world"
    );
    // Once-only terminal: every later recv is a clean end, never a repeat.
    assert!(stream.recv().await.unwrap().is_none());
    assert!(stream.recv().await.unwrap().is_none());
    assert_eq!(stub.chat_sends(), 1);
    drop(stream);
    wait_until(|| stub.open_conns() == 0, "connection released").await;
}

/// A transient connect-phase break retries inside the establishment
/// budget and produces an ordinary, non-duplicated event stream.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn connect_transient_retry_then_success() {
    let stub = Stub::start().await;
    stub.push_chat(script_drop());
    stub.push_chat(script_frames(vec![delta("ok"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(
        kinds(&events),
        vec![
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ]
    );
    assert_eq!(stub.chat_sends(), 2, "one transient retry then success");
}

/// A semantic connect-phase refusal is not retried and surfaces as the
/// classified failure.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn connect_semantic_error_no_retry() {
    let stub = Stub::start().await;
    stub.push_chat(script_json_error(
        403,
        "permission_denied",
        "model not allowed",
    ));
    let adapter = adapter_for(&stub, "t1");
    let err = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .map(|_| ())
        .expect_err("semantic refusal");
    assert_eq!(err.code, "permission_denied");
    assert!(err.client_fixable);
    assert_eq!(stub.chat_sends(), 1, "semantic refusals never retry");
}

/// Transient connect failures stop at the three-attempt cap.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn connect_retry_cap_three_sends() {
    let stub = Stub::start().await;
    for _ in 0..3 {
        stub.push_chat(script_drop());
    }
    let adapter = adapter_for(&stub, "t1");
    let err = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .map(|_| ())
        .expect_err("all attempts dropped");
    assert!(err.upstream_fault, "transport break: {err}");
    assert_eq!(stub.chat_sends(), 3, "maxConnectAttempts = 3");
}

/// `unauthenticated` at connect time repairs the token once against a
/// newer generation and resends with it.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn connect_unauthenticated_repairs_token_once() {
    let stub = Stub::start().await;
    stub.push_chat(script_json_error(
        401,
        "unauthenticated",
        "bad session token",
    ));
    stub.push_chat(script_frames(vec![delta("ok"), stop_frame()]));
    let mut config = AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "t1".to_string(),
        model: "swe-2-max".to_string(),
        ..Default::default()
    };
    config.token_source = Some(static_token("t2"));
    let adapter = Adapter::new(config).expect("adapter");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(stub.chat_sends(), 2);
    assert_eq!(
        stub.chat_api_keys(),
        vec!["t1".to_string(), "t2".to_string()]
    );
}

/// A mid-stream `EOF` before any content is a transport break: the request
/// is resent once, the client sees one start and one done — no replay.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn midstream_eof_reopens_once() {
    let stub = Stub::start().await;
    stub.push_chat(script_close_after(vec![]));
    stub.push_chat(script_frames(vec![delta("retry"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(
        kinds(&events),
        vec![
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ],
        "pre-content reopen must not duplicate events"
    );
    assert_eq!(stub.chat_sends(), 2);
}

/// Once content flowed, an upstream break is terminal — no replay after
/// a semantic event.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn post_content_eof_no_replay() {
    let stub = Stub::start().await;
    stub.push_chat(script_close_after(vec![delta("partial")]));
    stub.push_chat(script_frames(vec![delta("never"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    let kinds = kinds(&events);
    assert_eq!(
        kinds[..3],
        [
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta
        ]
    );
    let last = events.last().unwrap();
    assert_eq!(last.kind, ResponseEventType::Error);
    let failure = last.error.as_ref().unwrap().failure.as_ref().unwrap();
    assert!(
        failure.upstream_fault,
        "EOF mid-stream is a transport break"
    );
    assert_eq!(stub.chat_sends(), 1, "no replay after semantic output");
}

/// The empty-end_turn degenerate shape (stopReason, zero content) resends
/// once with a "continue" user message appended.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn empty_end_turn_continuation() {
    let stub = Stub::start().await;
    stub.push_chat(script_frames(vec![stop_frame()]));
    stub.push_chat(script_frames(vec![delta("continued"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(
        kinds(&events),
        vec![
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ]
    );
    assert_eq!(stub.chat_sends(), 2);
    let resent = stub.chat_request(1);
    let last = resent
        .chat_message_prompts
        .last()
        .expect("continuation prompt");
    assert_eq!(
        last.prompt.as_deref(),
        Some("continue"),
        "continuation appends a continue user prompt"
    );
}

/// `unauthenticated` inside the stream (`END_STREAM` error) repairs the
/// token once and resends — still pre-content, so no duplicates.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn midstream_unauthenticated_repairs_once() {
    let stub = Stub::start().await;
    stub.push_chat(script_end_error(vec![], "unauthenticated", "expired"));
    stub.push_chat(script_frames(vec![delta("ok"), stop_frame()]));
    let mut config = AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "t1".to_string(),
        model: "swe-2-max".to_string(),
        ..Default::default()
    };
    config.token_source = Some(static_token("t2"));
    let adapter = Adapter::new(config).expect("adapter");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(events.first().unwrap().kind, ResponseEventType::Start);
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == ResponseEventType::Start)
            .count(),
        1,
        "retried stream must not emit a second start"
    );
    assert_eq!(stub.chat_sends(), 2);
    assert_eq!(
        stub.chat_api_keys(),
        vec!["t1".to_string(), "t2".to_string()]
    );
}

// ---------------------------------------------------------------------------
// Timer transitions (paused clock)
// ---------------------------------------------------------------------------

/// With the upstream silent, the withheld start is released when the
/// start-hold expires — the client's first visible byte.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn start_hold_releases_pending_start() {
    let stub = Stub::start().await;
    let release = Arc::new(Notify::new());
    stub.push_chat(script_hold(
        vec![delta("late"), stop_frame()],
        release.clone(),
        Tail::EndStream,
    ));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    // The runtime auto-advances paused time to the start-hold deadline
    // while everything is parked.
    let first = stream.recv().await.unwrap().expect("start released");
    assert_eq!(first.kind, ResponseEventType::Start);
    release.notify_one();
    let events = collect(&mut stream).await;
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(stub.chat_sends(), 1);
}

/// Total upstream silence past the stall watchdog kills the stream and —
/// still pre-content — resends once.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn stall_watchdog_reopens_precontent() {
    let stub = Stub::start().await;
    let release = Arc::new(Notify::new());
    stub.push_chat(script_hold(vec![], release, Tail::Close));
    stub.push_chat(script_frames(vec![delta("after-stall"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(events.first().unwrap().kind, ResponseEventType::Start);
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == ResponseEventType::Start)
            .count(),
        1
    );
    assert_eq!(stub.chat_sends(), 2, "stall kill resends once pre-content");
}

/// After a stop reason the silence deadline shrinks to the tail grace: a
/// stream that never sends `END_STREAM` still finishes normally.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn tail_grace_finishes_after_stop() {
    let stub = Stub::start().await;
    let release = Arc::new(Notify::new()); // never fired: upstream holds the body
    stub.push_chat(script_hold(
        vec![delta("x"), stop_frame()],
        release,
        Tail::Close,
    ));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    let done = events.last().unwrap();
    assert_eq!(done.kind, ResponseEventType::Done);
    assert_eq!(done.reason, Some(StopReason::Stop));
    assert_eq!(stub.chat_sends(), 1, "tail grace ends without resend");
}

/// Zero-event liveness frames feed the stall watchdog but not the
/// progress watchdog: a degenerate stream is killed and resent once.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn no_progress_watchdog_reopens() {
    let stub = Stub::start().await;
    stub.push_chat(script_tick(vec![], Duration::from_secs(60)));
    stub.push_chat(script_frames(vec![delta("progressed"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    assert_eq!(events.first().unwrap().kind, ResponseEventType::Start);
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == ResponseEventType::Error)
            .count(),
        0,
        "pre-content watchdog reopen must not surface an error event"
    );
    assert_eq!(stub.chat_sends(), 2);
}

// ---------------------------------------------------------------------------
// Ordered drain under consumer stall
// ---------------------------------------------------------------------------

/// With no pump channel, upstream read-ahead is bounded by the transport
/// buffers (hyper body channel + `TCP` window): a stalled consumer applies
/// backpressure at the socket, and every frame still arrives in order
/// once drained.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn ordered_drain_after_consumer_stall() {
    const FRAMES: usize = 104;
    let stub = Stub::start().await;
    let frames: Vec<_> = (0..FRAMES)
        .map(|i| delta(&format!("d{i}")))
        .chain(std::iter::once(stop_frame()))
        .collect();
    stub.push_chat(script_frames(frames));
    let adapter = adapter_for(&stub, "t1");
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    // Stall the consumer while the stub pushes the whole script: nothing
    // is lost and every frame still arrives in order once drained.
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    let events = collect(&mut stream).await;
    let deltas: Vec<&str> = events
        .iter()
        .filter(|e| e.kind == ResponseEventType::TextDelta)
        .map(|e| e.delta.as_str())
        .collect();
    assert_eq!(deltas.len(), FRAMES);
    for (i, text) in deltas.iter().enumerate() {
        assert_eq!(*text, format!("d{i}"), "frame order");
    }
    assert_eq!(events.last().unwrap().kind, ResponseEventType::Done);
    assert_eq!(stub.chat_sends(), 1);
}

// ---------------------------------------------------------------------------
// Cancellation at every seam
// ---------------------------------------------------------------------------

/// Pre-armed cancellation at each boundary — gate wait, retry backoff,
/// connect send, in-stream receive and blocked-consumer pump send —
/// terminates the lineage with zero sends after the observation and all
/// resources released.
#[tokio::test]
// One sequential seam-by-seam scenario; splitting would scatter it.
#[allow(clippy::too_many_lines, clippy::large_futures)]
async fn cancellation_at_every_seam() {
    // Seam 1 — gate wait: exhaust the window quota so the second request
    // parks in `gate.wait`, then cancel mid-wait.
    let stub = Stub::start().await;
    stub.push_chat(script_frames(vec![delta("a"), stop_frame()]));
    let mut config = AdapterConfig {
        base_url: stub.base_url.clone(),
        token: "t1".to_string(),
        model: "swe-2-max".to_string(),
        ..Default::default()
    };
    config.gate = GateConfig {
        max_rpm: 1,
        max_hold: Duration::from_secs(3600),
        ..Default::default()
    };
    let adapter = Adapter::new(config).expect("adapter");
    let mut first = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    collect(&mut first).await;
    assert_eq!(stub.chat_sends(), 1);

    let cancel2 = CancellationToken::new();
    let adapter2 = adapter.clone();
    let cancel2_task = cancel2.clone();
    let pending = tokio::spawn(async move {
        adapter2
            .stream(
                user_request("hi"),
                cancel2_task,
                devin2api::debuglog::Recorder::none(),
            )
            .await
    });
    wait_until(
        || adapter.gate_stats().waiters == 1,
        "request parked in gate",
    )
    .await;
    cancel2.cancel();
    let Err(err) = pending.await.unwrap() else {
        panic!("gate-cancelled request unexpectedly opened a stream");
    };
    assert!(err.canceled, "gate wait cancel: {err}");
    assert_eq!(stub.chat_sends(), 1, "no send after gate cancellation");

    // Seam 2 — backoff: first conn drops (transient), cancel while the
    // retry sleeps.
    let stub = Stub::start().await;
    stub.push_chat(script_drop());
    stub.push_chat(script_frames(vec![delta("b"), stop_frame()]));
    let adapter = adapter_for(&stub, "t1");
    let mut probe = adapter.lifecycle_probe();
    let backoff_target = probe.retry_backoff_waits() + 1;
    let cancel = CancellationToken::new();
    let adapter2 = adapter.clone();
    let cancel_clone = cancel.clone();
    let pending = tokio::spawn(async move {
        adapter2
            .stream(
                user_request("hi"),
                cancel_clone,
                devin2api::debuglog::Recorder::none(),
            )
            .await
    });
    probe.wait_for_retry_backoff(backoff_target).await;
    assert_eq!(stub.chat_sends(), 1, "retry is parked before its send");
    cancel.cancel();
    let Err(err) = pending.await.unwrap() else {
        panic!("backoff-cancelled request unexpectedly opened a stream");
    };
    assert!(err.canceled, "backoff cancel: {err}");
    // Let any in-flight work settle, then confirm the resend never ran.
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(stub.chat_sends(), 1, "no send after backoff cancellation");

    // Seam 3 — connect: the request is on the wire but response headers
    // are held; cancel inside `get_chat_message`.
    let stub = Stub::start().await;
    let hold = Arc::new(Notify::new());
    stub.push_chat(script_hold_headers(hold, Tail::EndStream));
    let adapter = adapter_for(&stub, "t1");
    let cancel = CancellationToken::new();
    let adapter2 = adapter.clone();
    let cancel_clone = cancel.clone();
    let pending = tokio::spawn(async move {
        adapter2
            .stream(
                user_request("hi"),
                cancel_clone,
                devin2api::debuglog::Recorder::none(),
            )
            .await
    });
    wait_until(|| stub.chat_sends() == 1, "request on the wire").await;
    cancel.cancel();
    let Err(err) = pending.await.unwrap() else {
        panic!("connect-cancelled request unexpectedly opened a stream");
    };
    assert!(err.canceled, "connect cancel: {err}");
    assert_eq!(stub.chat_sends(), 1);
    wait_until(|| stub.open_conns() == 0, "connect conn released").await;

    // Seam 4 — receive: drain the only data frame, then prove the next
    // `recv` is parked in its select before cancellation.
    let stub = Stub::start().await;
    let release = Arc::new(Notify::new());
    stub.push_chat(script_hold(vec![delta("x")], release, Tail::EndStream));
    let adapter = adapter_for(&stub, "t1");
    let mut probe = adapter.lifecycle_probe();
    let cancel = CancellationToken::new();
    let mut stream = adapter
        .stream(
            user_request("hi"),
            cancel.clone(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    assert_eq!(
        stream.recv().await.unwrap().unwrap().kind,
        ResponseEventType::Start
    );
    assert_eq!(
        stream.recv().await.unwrap().unwrap().kind,
        ResponseEventType::TextStart
    );
    assert_eq!(
        stream.recv().await.unwrap().unwrap().kind,
        ResponseEventType::TextDelta
    );

    // Poll the next recv until it has entered the in-stream select, then
    // cancel. This excludes the loop-head path and pins Go's select-arm
    // shape: one queued Error event followed by EOF.
    let receive_target = probe.stream_receive_waits() + 1;
    let terminal = {
        let pending_recv = stream.recv();
        tokio::pin!(pending_recv);
        tokio::select! {
            () = probe.wait_for_stream_receive(receive_target) => {}
            result = &mut pending_recv => panic!("recv completed before parking: {result:?}"),
        }
        cancel.cancel();
        pending_recv
            .await
            .expect("receive cancellation is an event, not a direct error")
            .expect("receive cancellation emits one terminal event")
    };
    assert_eq!(terminal.kind, ResponseEventType::Error);
    assert!(
        terminal
            .error
            .as_ref()
            .and_then(|message| message.failure.as_ref())
            .is_some_and(|failure| failure.canceled),
        "receive cancellation event must retain canceled classification"
    );
    assert!(
        stream.recv().await.unwrap().is_none(),
        "terminal is once-only"
    );
    wait_until(|| stub.open_conns() == 0, "receive conn released").await;
    assert_eq!(stub.chat_sends(), 1, "no send after receive cancellation");

    // Seam 5 — stalled consumer: the stream is open with frames in
    // flight that nobody drains; cancel releases the exchange without
    // further sends.
    let stub = Stub::start().await;
    let frames: Vec<_> = (0..84).map(|i| delta(&format!("d{i}"))).collect();
    stub.push_chat(script_frames(frames));
    let adapter = adapter_for(&stub, "t1");
    let cancel = CancellationToken::new();
    let mut stream = adapter
        .stream(
            user_request("hi"),
            cancel.clone(),
            devin2api::debuglog::Recorder::none(),
        )
        .await
        .expect("stream");
    // Let the stub push frames while the consumer never polls, then
    // cancel mid-stream.
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    cancel.cancel();

    // Cancellation was armed only after `Sender::send` polled Pending.
    // The Rust stream owner observes the canceled lineage at Recv entry,
    // emits exactly one classified terminal Error, then latches EOF.
    let terminal = stream
        .recv()
        .await
        .expect("blocked-send cancellation is a terminal event")
        .expect("blocked-send cancellation emits one terminal event");
    assert_eq!(terminal.kind, ResponseEventType::Error);
    assert!(
        terminal
            .error
            .as_ref()
            .and_then(|message| message.failure.as_ref())
            .is_some_and(|failure| failure.canceled),
        "blocked-send cancellation event must retain canceled classification"
    );
    assert!(
        stream.recv().await.unwrap().is_none(),
        "terminal is once-only"
    );
    wait_until(|| stub.open_conns() == 0, "blocked conn released").await;
    assert_eq!(stub.chat_sends(), 1, "no send after consumer cancellation");
}

// ---------------------------------------------------------------------------
// Failure recording
// ---------------------------------------------------------------------------

/// A local gate rejection never reaches the wire and records `rate_gate`
/// as the first failure point.
#[tokio::test]
#[allow(clippy::large_futures)]
async fn gate_rejection_records_rate_gate() {
    let stub = Stub::start().await;
    let adapter = adapter_for(&stub, "t1");
    // Arm the cooldown latch with an upstream-style refusal.
    adapter
        .gate()
        .note_upstream_error(&connectrpc::ConnectError::new(
            connectrpc::ErrorCode::ResourceExhausted,
            "rate limited; reset in 60 seconds",
        ));
    let dir = std::env::temp_dir().join(format!("devin2api-t11-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let manager = Manager::new(&dir, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        ..Default::default()
    });
    let err = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            recorder.clone(),
        )
        .await
        .map(|_| ())
        .expect_err("latched gate rejects");
    assert!(err.local_gate, "local gate rejection: {err}");
    assert!(err.rate_limited);
    assert_eq!(stub.chat_sends(), 0, "a gate rejection never sends");
    recorder.complete(Completion {
        status_code: 429,
        result: "failed".to_string(),
        ..Default::default()
    });
    let error_json = std::fs::read_to_string(dir.join(recorder.dir_name()).join("error.json"))
        .expect("error.json written");
    assert!(
        error_json.contains("\"stage\": \"rate_gate\"")
            || error_json.contains("\"stage\":\"rate_gate\""),
        "error.json must record rate_gate: {error_json}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed reopen preserves the original failure and records the
/// secondary retry failure in `04-devin-response.jsonl`.
#[tokio::test(start_paused = true)]
#[allow(clippy::large_futures)]
async fn failed_reopen_preserves_original_and_records_secondary() {
    let stub = Stub::start().await;
    // Conn 1: EOF before content (transient → reopen). The reopen's own
    // connect attempts all drop: 3 nested sends, then the original error.
    stub.push_chat(script_close_after(vec![]));
    for _ in 0..3 {
        stub.push_chat(script_drop());
    }
    let adapter = adapter_for(&stub, "t1");
    let dir = std::env::temp_dir().join(format!("devin2api-t11-retry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let manager = Manager::new(&dir, &RetentionPolicy::default());
    let recorder = manager.start(&RequestMeta {
        method: "POST".to_string(),
        path: "/v1/chat/completions".to_string(),
        ..Default::default()
    });
    let mut stream = adapter
        .stream(
            user_request("hi"),
            CancellationToken::new(),
            recorder.clone(),
        )
        .await
        .expect("stream");
    let events = collect(&mut stream).await;
    let last = events.last().unwrap();
    assert_eq!(last.kind, ResponseEventType::Error);
    let failure = last.error.as_ref().unwrap().failure.as_ref().unwrap();
    assert!(
        failure.upstream_fault,
        "the original EOF classification is preserved: {failure}"
    );
    assert_eq!(
        stub.chat_sends(),
        4,
        "1 original + 3 nested connect attempts"
    );
    recorder.complete(Completion {
        status_code: 502,
        result: "failed".to_string(),
        ..Default::default()
    });
    let req_dir = dir.join(recorder.dir_name());
    let jsonl = std::fs::read_to_string(req_dir.join(debuglog::STAGE_DEVIN_RESPONSE))
        .expect("04-devin-response.jsonl written");
    assert!(
        jsonl.contains("\"retry_failed\""),
        "secondary retry failure recorded: {jsonl}"
    );
    assert!(
        jsonl.contains("\"retry_attempt\""),
        "retry divider recorded: {jsonl}"
    );
    let error_json = std::fs::read_to_string(req_dir.join("error.json")).expect("error.json");
    assert!(
        error_json.contains("devin_transport"),
        "original EOF classified devin_transport: {error_json}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

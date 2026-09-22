use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use devin2api::debuglog::{Manager, RetentionPolicy};
use devin2api::domain::{
    AssistantMessage, Content, Failure, Message, RequestMessages, ResponseEvent, ResponseEventType,
    StopReason, TextContent, ToolCall,
};
use devin2api::server::http::{App, HttpBackend, HttpConfig, HttpEventStream};
use devin2api::upstream::catalog::ModelInfo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct ScriptBackend {
    scripts: Arc<Mutex<VecDeque<Script>>>,
    requests: Arc<Mutex<Vec<RequestMessages>>>,
}

enum Script {
    Events(VecDeque<Result<ResponseEvent, Failure>>),
    EchoToolCallCount,
    Flood {
        dropped: oneshot::Sender<()>,
    },
    Block {
        entered: Arc<Notify>,
        cancelled: oneshot::Sender<()>,
    },
}

struct EventStream(VecDeque<Result<ResponseEvent, Failure>>);
impl HttpEventStream for EventStream {
    fn recv(
        &mut self,
    ) -> devin2api::server::http::BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let next = self.0.pop_front().transpose();
        Box::pin(async move { next })
    }
}

struct BlockStream {
    entered: Arc<Notify>,
    cancel: CancellationToken,
    cancelled: Option<oneshot::Sender<()>>,
}
impl HttpEventStream for BlockStream {
    fn recv(
        &mut self,
    ) -> devin2api::server::http::BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let entered = self.entered.clone();
        let cancel = self.cancel.clone();
        Box::pin(async move {
            entered.notify_one();
            cancel.cancelled().await;
            Err(Failure::plain("cancelled"))
        })
    }
}

struct FloodStream {
    events: VecDeque<Result<ResponseEvent, Failure>>,
    dropped: Option<oneshot::Sender<()>>,
}
impl HttpEventStream for FloodStream {
    fn recv(
        &mut self,
    ) -> devin2api::server::http::BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let next = self.events.pop_front();
        Box::pin(async move {
            match next {
                Some(next) => next.map(Some),
                None => std::future::pending().await,
            }
        })
    }
}
impl Drop for FloodStream {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

impl Drop for BlockStream {
    fn drop(&mut self) {
        if let Some(cancelled) = self.cancelled.take() {
            let _ = cancelled.send(());
        }
    }
}

impl HttpBackend for ScriptBackend {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> devin2api::server::http::BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        request: RequestMessages,
        cancel: CancellationToken,
        _recorder: devin2api::debuglog::Recorder,
    ) -> devin2api::server::http::BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        let tool_call_count = request
            .messages
            .iter()
            .filter_map(|message| match message {
                Message::Assistant(assistant) => Some(&assistant.content),
                _ => None,
            })
            .flatten()
            .filter(|content| matches!(content, Content::ToolCall(_)))
            .count();
        self.requests.lock().unwrap().push(request);
        let script = self.scripts.lock().unwrap().pop_front().expect("script");
        Box::pin(async move {
            Ok(match script {
                Script::Events(events) => Box::new(EventStream(events)) as Box<dyn HttpEventStream>,
                Script::EchoToolCallCount => {
                    let Script::Events(events) =
                        text_turn(&format!("tool_calls={tool_call_count}"))
                    else {
                        unreachable!()
                    };
                    Box::new(EventStream(events)) as Box<dyn HttpEventStream>
                }
                Script::Flood { dropped } => Box::new(FloodStream {
                    events: flood_events(),
                    dropped: Some(dropped),
                }),
                Script::Block { entered, cancelled } => Box::new(BlockStream {
                    entered,
                    cancel,
                    cancelled: Some(cancelled),
                }),
            })
        })
    }
}

fn tool_turn(call_id: &str) -> Script {
    let call = ToolCall {
        id: call_id.into(),
        name: "shell".into(),
        arguments: r#"{"cmd":"ls"}"#.into(),
        ..ToolCall::default()
    };
    let partial = AssistantMessage {
        content: vec![Content::ToolCall(call.clone())],
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    Script::Events(VecDeque::from([
        Ok(ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::ToolCallStart,
            content_index: 0,
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::ToolCallDelta,
            content_index: 0,
            delta: call.arguments.clone(),
            tool_call_id: call.id.clone(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::ToolCallEnd,
            content_index: 0,
            tool_call: Some(call),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(StopReason::ToolUse),
            message: Some(AssistantMessage {
                content: partial.content,
                response_model: "stub-model".into(),
                stop_reason: Some(StopReason::ToolUse),
                ..AssistantMessage::default()
            }),
            ..ResponseEvent::default()
        }),
    ]))
}

fn interrupted_turn() -> Script {
    Script::Events(VecDeque::from([Ok(ResponseEvent {
        kind: ResponseEventType::Start,
        partial: Some(Arc::new(AssistantMessage {
            stop_reason: Some(StopReason::Pending),
            ..AssistantMessage::default()
        })),
        ..ResponseEvent::default()
    })]))
}

fn failed_turn() -> Script {
    Script::Events(VecDeque::from([
        Ok(ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::Error,
            reason: Some(StopReason::Error),
            error: Some(AssistantMessage {
                error_message: "upstream exploded".into(),
                ..AssistantMessage::default()
            }),
            ..ResponseEvent::default()
        }),
    ]))
}

fn flood_events() -> VecDeque<Result<ResponseEvent, Failure>> {
    let chunk = "w".repeat(512 << 10);
    let partial = AssistantMessage {
        content: vec![Content::Text(TextContent {
            text: chunk.clone(),
        })],
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    let mut events = VecDeque::from([
        Ok(ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::TextStart,
            content_index: 0,
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
    ]);
    for _ in 0..128 {
        events.push_back(Ok(ResponseEvent {
            kind: ResponseEventType::TextDelta,
            content_index: 0,
            delta: chunk.clone(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }));
    }
    events
}

fn upstream_too_big_turn() -> Script {
    Script::Events(VecDeque::from([
        Ok(ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::Error,
            reason: Some(StopReason::Error),
            error: Some(AssistantMessage {
                error_message: "upstream websocket message too big".into(),
                failure: Some(Box::new(Failure {
                    code: "message_too_big".into(),
                    message: "upstream websocket message too big".into(),
                    ..Failure::default()
                })),
                ..AssistantMessage::default()
            }),
            ..ResponseEvent::default()
        }),
    ]))
}

fn text_turn(text: &str) -> Script {
    let partial = AssistantMessage {
        content: vec![Content::Text(TextContent { text: text.into() })],
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    Script::Events(VecDeque::from([
        Ok(ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::TextStart,
            content_index: 0,
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::TextDelta,
            content_index: 0,
            delta: text.into(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::TextEnd,
            content_index: 0,
            content: text.into(),
            partial: Some(Arc::new(partial)),
            ..ResponseEvent::default()
        }),
        Ok(ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(StopReason::Stop),
            message: Some(AssistantMessage {
                response_id: "resp_upstream".into(),
                response_model: "stub-model".into(),
                stop_reason: Some(StopReason::Stop),
                content: vec![Content::Text(TextContent { text: text.into() })],
                ..AssistantMessage::default()
            }),
            ..ResponseEvent::default()
        }),
    ]))
}

struct WsClient {
    stream: tokio::net::TcpStream,
}
impl WsClient {
    async fn connect(address: std::net::SocketAddr, auth: Option<&str>) -> (Self, String) {
        Self::connect_with_headers(address, auth, "").await
    }

    async fn connect_with_headers(
        address: std::net::SocketAddr,
        auth: Option<&str>,
        extra_headers: &str,
    ) -> (Self, String) {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let auth = auth.map_or(String::new(), |value| {
            format!("Authorization: Bearer {value}\r\n")
        });
        let request = format!(
            "GET /v1/responses HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: responses_websockets=2026-02-06\r\n{auth}{extra_headers}\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        loop {
            let mut byte = [0];
            tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            response.push(byte[0]);
            if response.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        (Self { stream }, String::from_utf8(response).unwrap())
    }

    async fn send_text(&mut self, text: &str) {
        self.send_frame(1, text.as_bytes()).await.unwrap();
    }

    async fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> std::io::Result<()> {
        let mut frame = vec![0x80 | opcode];
        if payload.len() < 126 {
            frame.push(0x80 | u8::try_from(payload.len()).unwrap_or(0));
        } else if u16::try_from(payload.len()).is_ok() {
            frame.push(0x80 | 0x7e);
            frame.extend_from_slice(
                &u16::try_from(payload.len())
                    .unwrap_or(u16::MAX)
                    .to_be_bytes(),
            );
        } else {
            frame.push(0x80 | 127);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        let mask = [1_u8, 2, 3, 4];
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, byte)| byte ^ mask[i % 4]),
        );
        self.stream.write_all(&frame).await
    }

    async fn event(&mut self) -> serde_json::Value {
        loop {
            let (opcode, payload) = self.frame().await;
            match opcode {
                1 => return serde_json::from_slice(&payload).unwrap(),
                9 => self.send_control(10, &payload).await,
                8 => panic!("unexpected close: {}", String::from_utf8_lossy(&payload)),
                _ => {}
            }
        }
    }

    async fn until(&mut self, kinds: &[&str]) -> serde_json::Value {
        self.until_with_order(kinds).await.0
    }

    async fn until_with_order(&mut self, kinds: &[&str]) -> (serde_json::Value, Vec<String>) {
        let mut order = Vec::new();
        for _ in 0..64 {
            let event = tokio::time::timeout(Duration::from_secs(3), self.event())
                .await
                .unwrap();
            let kind = event["type"].as_str().unwrap_or_default().to_string();
            order.push(kind.clone());
            if kinds.contains(&kind.as_str()) {
                return (event, order);
            }
        }
        panic!("terminal event not received")
    }

    async fn frame(&mut self) -> (u8, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(3), self.try_frame())
            .await
            .unwrap()
            .unwrap()
    }

    async fn try_frame(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        let mut head = [0_u8; 2];
        self.stream.read_exact(&mut head).await?;
        let opcode = head[0] & 0x0f;
        let mut length = u64::from(head[1] & 0x7f);
        if length == 126 {
            let mut bytes = [0; 2];
            self.stream.read_exact(&mut bytes).await?;
            length = u64::from(u16::from_be_bytes(bytes));
        }
        if length == 127 {
            let mut bytes = [0; 8];
            self.stream.read_exact(&mut bytes).await?;
            length = u64::from_be_bytes(bytes);
        }
        let mut payload = vec![0; usize::try_from(length).unwrap_or(usize::MAX)];
        self.stream.read_exact(&mut payload).await?;
        Ok((opcode, payload))
    }

    async fn send_control(&mut self, opcode: u8, payload: &[u8]) {
        let mask = [5_u8, 6, 7, 8];
        let mut frame = vec![
            0x80 | opcode,
            0x80 | u8::try_from(payload.len()).unwrap_or(0),
        ];
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(i, byte)| byte ^ mask[i % 4]),
        );
        self.stream.write_all(&frame).await.unwrap();
    }
}

async fn server(
    backend: ScriptBackend,
    key: &str,
) -> (App, std::net::SocketAddr, CancellationToken) {
    server_with_config(
        backend,
        HttpConfig {
            api_key: key.into(),
            max_concurrency: 1,
            ..HttpConfig::default()
        },
    )
    .await
}

async fn server_with_config(
    backend: ScriptBackend,
    config: HttpConfig,
) -> (App, std::net::SocketAddr, CancellationToken) {
    let app = App::with_backend(backend, config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    tokio::spawn({
        let app = app.clone();
        let stop = stop.clone();
        async move {
            app.serve(listener, stop).await.unwrap();
        }
    });
    (app, address, stop)
}

#[tokio::test]
async fn streams_two_serial_turns_and_replays_previous_output() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            text_turn("first"),
            text_turn("second"),
        ]))),
        requests: requests.clone(),
    };
    let (_app, address, stop) = server(backend, "secret").await;
    let (mut client, handshake) = WsClient::connect(address, Some("secret")).await;
    assert!(handshake.starts_with("HTTP/1.1 101"), "{handshake}");
    assert!(
        handshake
            .to_ascii_lowercase()
            .contains("sec-websocket-protocol: responses_websockets=2026-02-06")
    );

    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"one"}]}]}"#).await;
    let (first, order) = client.until_with_order(&["response.completed"]).await;
    assert_eq!(
        order,
        [
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ]
    );
    let id = first["response"]["id"].as_str().unwrap();
    client.send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"message","role":"user","content":[{{"type":"input_text","text":"two"}}]}}]}}"#)).await;
    client.until(&["response.completed"]).await;

    let recorded = requests.lock().unwrap();
    assert_eq!(recorded.len(), 2);
    assert_eq!(
        recorded[1].messages.len(),
        3,
        "second turn must contain user + replayed assistant + user"
    );
    assert!(matches!(recorded[1].messages[1], Message::Assistant(_)));
    assert_eq!(recorded[1].model, "stub-model");
    stop.cancel();
}

#[tokio::test]
async fn cancel_only_stops_current_turn_and_connection_runs_next_turn() {
    let entered = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            Script::Block {
                entered: entered.clone(),
                cancelled: cancelled_tx,
            },
            text_turn("recovered"),
        ]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (app, address, stop) = server(backend, "").await;
    let (mut client, handshake) = WsClient::connect(address, None).await;
    assert!(handshake.starts_with("HTTP/1.1 101"));
    let waiting = entered.notified();
    tokio::pin!(waiting);
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
        .await;
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap();
    assert_eq!(app.available_permits(), 0);
    client.send_text(r#"{"type":"response.cancel"}"#).await;
    let cancelled = client.until(&["error"]).await;
    assert_eq!(cancelled["error"]["code"], "turn_cancelled");
    tokio::time::timeout(Duration::from_secs(3), cancelled_rx)
        .await
        .unwrap()
        .unwrap();
    app.wait_idle().await;
    assert_eq!(app.available_permits(), 1);

    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"again"}"#)
        .await;
    assert_eq!(
        client.until(&["response.completed"]).await["type"],
        "response.completed"
    );
    stop.cancel();
}

#[tokio::test]
async fn prewarm_is_local_and_its_response_id_can_be_replayed() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([text_turn("real")]))),
        requests: requests.clone(),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","generate":false,"model":"stub-model","input":[{"type":"message","role":"user","content":"context"}]}"#)
        .await;
    let prewarm = client.until(&["response.completed"]).await;
    let id = prewarm["response"]["id"].as_str().unwrap();
    assert!(id.starts_with("resp_prewarm_"));
    assert!(
        requests.lock().unwrap().is_empty(),
        "prewarm must stay local"
    );
    client
        .send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"message","role":"user","content":"question"}}]}}"#))
        .await;
    client.until(&["response.completed"]).await;
    assert_eq!(requests.lock().unwrap()[0].messages.len(), 2);
    stop.cancel();
}

#[tokio::test]
async fn malformed_messages_are_errors_without_closing_the_socket() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([text_turn("alive")]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client.send_text("{bad").await;
    assert_eq!(
        client.until(&["error"]).await["error"]["code"],
        "invalid_request"
    );
    client.send_text(r#"{"type":"other"}"#).await;
    assert_eq!(
        client.until(&["error"]).await["error"]["code"],
        "unsupported_event"
    );
    client.send_text(r#"{"type":"response.create","model":"stub-model","previous_response_id":"msg_foreign","input":[]}"#).await;
    assert_eq!(
        client.until(&["error"]).await["error"]["code"],
        "invalid_request"
    );
    client
        .send_text(r#"{"type":"response.append","input":[]}"#)
        .await;
    assert_eq!(
        client.until(&["error"]).await["error"]["code"],
        "invalid_request"
    );
    client.send_frame(2, b"binary").await.unwrap();
    let binary = client.until(&["error"]).await;
    assert_eq!(
        binary,
        serde_json::json!({
            "type":"error","status":400,
            "error":{"message":"only text websocket messages are supported","type":"invalid_request_error","code":"unsupported_frame"}
        })
    );
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"ok"}"#)
        .await;
    assert_eq!(
        client.until(&["response.completed"]).await["type"],
        "response.completed"
    );
    stop.cancel();
}

#[tokio::test]
async fn websocket_turn_logs_use_responses_ws_api_label() {
    let root =
        std::env::temp_dir().join(format!("devin2api-ws-label-{}", devin2api::randid::hex(6)));
    std::fs::create_dir_all(&root).unwrap();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([text_turn("logged")]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = App::with_backend(
        backend,
        HttpConfig {
            debug_manager: Some(Arc::new(Manager::new(&root, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    tokio::spawn({
        let app = app.clone();
        let stop = stop.clone();
        async move {
            app.serve(listener, stop).await.unwrap();
        }
    });
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"log me"}"#)
        .await;
    let created = client.event().await;
    assert_eq!(created["type"], "response.created");
    let debug_ref = created["response"]["debug_ref"].as_str().unwrap();
    let meta_path = root.join(debug_ref).join("meta.json");
    let meta: serde_json::Value = serde_json::from_slice(
        &std::fs::read(meta_path).expect("response.created must follow metadata flush"),
    )
    .unwrap();
    assert_eq!(meta["api"], "responses-ws");
    client.until(&["response.completed"]).await;
    stop.cancel();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn previous_response_mismatch_is_exact_and_connection_survives() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            text_turn("one"),
            text_turn("two"),
        ]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"one"}"#)
        .await;
    client.until(&["response.completed"]).await;
    client
        .send_text(r#"{"type":"response.create","previous_response_id":"resp_missing","input":[]}"#)
        .await;
    let error = client.until(&["error"]).await;
    assert_eq!(
        error,
        serde_json::json!({
            "type":"error","status":400,
            "error":{
                "message":"previous response is not available on this websocket; resend the full conversation input without previous_response_id: \"resp_missing\"",
                "type":"invalid_request_error","code":"previous_response_not_found","param":"previous_response_id"
            }
        })
    );
    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"replacement"}]}"#).await;
    assert_eq!(
        client.until(&["response.completed"]).await["type"],
        "response.completed"
    );
    stop.cancel();
}

#[tokio::test]
async fn pending_tool_call_requires_output_then_replays_it() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            tool_turn("call_abc"),
            text_turn("used"),
        ]))),
        requests: requests.clone(),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"run ls"}]}"#).await;
    let completed = client.until(&["response.completed"]).await;
    let id = completed["response"]["id"].as_str().unwrap();
    client.send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"message","role":"user","content":"missing"}}]}}"#)).await;
    let error = client.until(&["error"]).await;
    assert_eq!(error["error"]["code"], "invalid_request");
    assert_eq!(
        error["error"]["message"],
        "incremental websocket request is missing output for a pending tool call"
    );
    client.send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"function_call_output","call_id":"call_abc","output":"file.txt"}},{{"type":"message","role":"user","content":"continue"}}]}}"#)).await;
    client.until(&["response.completed"]).await;
    assert_eq!(requests.lock().unwrap()[1].messages.len(), 4);
    stop.cancel();
}

#[tokio::test]
async fn interrupted_turn_requires_full_replacement_replay() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            text_turn("ok"),
            interrupted_turn(),
            text_turn("fresh"),
        ]))),
        requests: requests.clone(),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"first"}]}"#).await;
    let first = client.until(&["response.completed"]).await;
    let id = first["response"]["id"].as_str().unwrap();
    client.send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"message","role":"user","content":"interrupted"}}]}}"#)).await;
    let error = client.until(&["error"]).await;
    assert_eq!(error["error"]["code"], "upstream_stream_interrupted");
    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"fresh"}]}"#).await;
    client.until(&["response.completed"]).await;
    assert_eq!(requests.lock().unwrap()[2].messages.len(), 1);
    stop.cancel();
}

#[tokio::test]
async fn failed_turn_does_not_advance_previous_response_id() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            text_turn("ok"),
            failed_turn(),
            text_turn("recovered"),
        ]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"one"}"#)
        .await;
    let first = client.until(&["response.completed"]).await;
    let first_id = first["response"]["id"].as_str().unwrap().to_string();
    client
        .send_text(&format!(
            r#"{{"type":"response.create","previous_response_id":"{first_id}","input":[]}}"#
        ))
        .await;
    let failed = client.until(&["response.failed"]).await;
    let failed_id = failed["response"]["id"].as_str().unwrap();
    client
        .send_text(&format!(
            r#"{{"type":"response.create","previous_response_id":"{failed_id}","input":[]}}"#
        ))
        .await;
    assert_eq!(
        client.until(&["error"]).await["error"]["code"],
        "previous_response_not_found"
    );
    client.send_text(&format!(r#"{{"type":"response.create","previous_response_id":"{first_id}","input":[{{"type":"message","role":"user","content":"recover"}}]}}"#)).await;
    client.until(&["response.completed"]).await;
    stop.cancel();
}

#[tokio::test]
async fn inference_limit_is_per_turn_not_per_idle_connection() {
    let entered = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([
            Script::Block {
                entered: entered.clone(),
                cancelled: cancelled_tx,
            },
            text_turn("after busy"),
        ]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (app, address, stop) = server(backend, "").await;
    let (mut active, _) = WsClient::connect(address, None).await;
    let (mut idle, _) = WsClient::connect(address, None).await;
    let waiting = entered.notified();
    tokio::pin!(waiting);
    active
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
        .await;
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap();
    idle.send_text(r#"{"type":"response.create","model":"stub-model","input":"busy"}"#)
        .await;
    let busy = idle.until(&["error"]).await;
    assert_eq!(
        busy,
        serde_json::json!({"type":"error","status":429,"error":{"message":"server is busy, please try again later","type":"rate_limit_error","code":"rate_limit"}})
    );
    active.send_text(r#"{"type":"response.cancel"}"#).await;
    active.until(&["error"]).await;
    tokio::time::timeout(Duration::from_secs(3), cancelled_rx)
        .await
        .unwrap()
        .unwrap();
    app.wait_idle().await;
    idle.send_text(r#"{"type":"response.create","model":"stub-model","input":"works"}"#)
        .await;
    idle.until(&["response.completed"]).await;
    stop.cancel();
}

#[tokio::test]
async fn non_upgrade_get_is_405_with_post_allow() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(
            format!("GET /v1/responses HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(response).unwrap().to_ascii_lowercase();
    assert!(
        response.starts_with("http/1.1 405 method not allowed"),
        "{response}"
    );
    assert!(response.contains("allow: post\r\n"), "{response}");
    stop.cancel();
}

#[tokio::test]
async fn inbound_queue_limit_closes_1009_with_exact_reason() {
    let entered = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([Script::Block {
            entered: entered.clone(),
            cancelled: cancelled_tx,
        }]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    let waiting = entered.notified();
    tokio::pin!(waiting);
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
        .await;
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap();
    let payload = vec![b'x'; 24 << 20];
    for _ in 0..3 {
        if client.send_frame(2, &payload).await.is_err() {
            break;
        }
    }
    let (opcode, close) = client.frame().await;
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([close[0], close[1]]), 1009);
    assert_eq!(&close[2..], b"inbound queue byte limit exceeded");
    tokio::time::timeout(Duration::from_secs(3), cancelled_rx)
        .await
        .unwrap()
        .unwrap();
    stop.cancel();
}

#[tokio::test]
async fn response_append_deduplicates_duplicate_call_ids_on_the_wire() {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([Script::EchoToolCallCount]))),
        requests: requests.clone(),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client.send_text(r#"{"type":"response.create","generate":false,"model":"stub-model","input":[{"id":"fc_one","type":"function_call","call_id":"call_dup","name":"shell","arguments":"{}"},{"id":"fc_two","type":"function_call","call_id":"call_dup","name":"shell","arguments":"{}"}]}"#).await;
    let first = client.until(&["response.completed"]).await;
    let id = first["response"]["id"].as_str().unwrap();
    client.send_text(&format!(r#"{{"type":"response.append","previous_response_id":"{id}","input":[{{"type":"message","role":"user","content":"continue"}}]}}"#)).await;
    let mut observed = None;
    loop {
        let event = client.event().await;
        if event["type"] == "response.output_text.delta" {
            observed = event["delta"].as_str().map(str::to_string);
        }
        if event["type"] == "response.completed" {
            break;
        }
    }
    assert_eq!(observed.as_deref(), Some("tool_calls=1"));
    assert_eq!(requests.lock().unwrap()[0].model, "stub-model");
    stop.cancel();
}

#[tokio::test]
async fn merged_transcript_limit_rejects_without_closing_connection() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([text_turn("after compact")]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    let large = "a".repeat(17 << 20);
    let prewarm = serde_json::json!({
        "type":"response.create","generate":false,"model":"stub-model",
        "input":[{"type":"message","role":"user","content":large}]
    });
    client
        .send_text(&serde_json::to_string(&prewarm).unwrap())
        .await;
    let completed = client.until(&["response.completed"]).await;
    let id = completed["response"]["id"].as_str().unwrap();
    let next = serde_json::json!({
        "type":"response.create","previous_response_id":id,
        "input":[{"type":"message","role":"user","content":"b".repeat(17 << 20)}]
    });
    client
        .send_text(&serde_json::to_string(&next).unwrap())
        .await;
    let error = client.until(&["error"]).await;
    assert_eq!(error["error"]["code"], "invalid_request");
    assert_eq!(
        error["error"]["message"],
        format!(
            "websocket transcript exceeds {} byte limit; compact and replay the conversation",
            devin2api::server::websocket::MAX_TRANSCRIPT_BYTES
        )
    );
    client.send_text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"compacted"}]}"#).await;
    client.until(&["response.completed"]).await;
    stop.cancel();
}

#[tokio::test]
async fn turn_state_header_is_echoed_on_upgrade() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (_client, handshake) =
        WsClient::connect_with_headers(address, None, "x-codex-turn-state: sticky-7\r\n").await;
    assert!(
        handshake
            .to_ascii_lowercase()
            .contains("x-codex-turn-state: sticky-7\r\n"),
        "{handshake}"
    );
    stop.cancel();
}

#[tokio::test(start_paused = true)]
async fn first_message_idle_ping_and_write_budgets_match_go() {
    assert_eq!(
        devin2api::server::websocket::FIRST_MESSAGE_TIMEOUT,
        Duration::from_secs(30)
    );
    assert_eq!(
        devin2api::server::websocket::IDLE_TIMEOUT,
        Duration::from_mins(5)
    );
    assert_eq!(
        devin2api::server::websocket::PING_INTERVAL,
        Duration::from_mins(2)
    );
    assert_eq!(
        devin2api::server::websocket::WRITE_DEADLINE,
        Duration::from_secs(60)
    );

    let entered = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([Script::Block {
            entered: entered.clone(),
            cancelled: cancelled_tx,
        }]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (silent, _) = WsClient::connect(address, None).await;
    tokio::time::advance(devin2api::server::websocket::FIRST_MESSAGE_TIMEOUT).await;
    for _ in 0..10_000 {
        let mut byte = [0];
        if matches!(silent.stream.try_read(&mut byte), Ok(0)) {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut byte = [0];
    assert!(
        matches!(silent.stream.try_read(&mut byte), Ok(0)),
        "silent first-message connection stayed open"
    );

    let (mut live, _) = WsClient::connect(address, None).await;
    let waiting = entered.notified();
    tokio::pin!(waiting);
    live.send_text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
        .await;
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .unwrap();
    tokio::time::advance(devin2api::server::stream::KEEPALIVE_INTERVAL).await;
    tokio::task::yield_now().await;
    let (opcode, payload) = live.frame().await;
    assert_eq!((opcode, payload), (9, Vec::new()));
    live.send_text(r#"{"type":"response.cancel"}"#).await;
    live.until(&["error"]).await;
    tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
        .await
        .unwrap()
        .unwrap();

    drop(live);
    stop.cancel();

    let idle_backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_idle_app, idle_address, idle_stop) = server(idle_backend, "").await;
    let (mut idle, _) = WsClient::connect(idle_address, None).await;
    idle.send_text("{malformed").await;
    idle.until(&["error"]).await;
    tokio::time::advance(devin2api::server::websocket::IDLE_TIMEOUT).await;
    let mut reached_eof = false;
    for _ in 0..100_000 {
        let mut buffered = [0_u8; 1024];
        match idle.stream.try_read(&mut buffered) {
            Ok(0) => {
                reached_eof = true;
                break;
            }
            Ok(_) => tokio::task::yield_now().await,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                tokio::task::yield_now().await;
            }
            Err(error) => panic!("idle socket read failed: {error}"),
        }
    }
    assert!(reached_eof, "idle websocket stayed open past five minutes");
    idle_stop.cancel();
}

#[tokio::test]
async fn upstream_message_too_big_closes_with_exact_reason() {
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([upstream_too_big_turn()]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"x"}"#)
        .await;
    loop {
        let (opcode, payload) = client.frame().await;
        if opcode == 8 {
            assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1009);
            assert_eq!(&payload[2..], b"upstream websocket message too big");
            break;
        }
    }
    stop.cancel();
}

#[tokio::test]
async fn oversized_response_output_closes_with_exact_reason() {
    let huge = "z".repeat((33 << 20) + 1);
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([text_turn(&huge)]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"x"}"#)
        .await;
    for _ in 0..16 {
        let (opcode, payload) = client.frame().await;
        if opcode == 8 {
            assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1009);
            assert_eq!(
                &payload[2..],
                b"response output exceeds websocket transcript limit"
            );
            stop.cancel();
            return;
        }
    }
    panic!("output transcript close frame not received");
}

#[tokio::test]
async fn simultaneous_cancel_and_close_cancels_once_without_double_terminal() {
    let entered = Arc::new(Notify::new());
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([Script::Block {
            entered: entered.clone(),
            cancelled: cancelled_tx,
        }]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server(backend, "").await;
    let (mut client, _) = WsClient::connect(address, None).await;
    let waiting = entered.notified();
    tokio::pin!(waiting);
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
        .await;
    tokio::time::timeout(Duration::from_secs(3), waiting)
        .await
        .unwrap();
    client.send_text(r#"{"type":"response.cancel"}"#).await;
    client.send_control(8, &1000_u16.to_be_bytes()).await;
    tokio::time::timeout(Duration::from_secs(3), cancelled_rx)
        .await
        .unwrap()
        .unwrap();
    let terminals = tokio::time::timeout(Duration::from_secs(3), async {
        let mut terminals = 0;
        loop {
            let (opcode, payload) = client.try_frame().await.unwrap();
            if opcode == 1 {
                let event: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                if matches!(
                    event["type"].as_str(),
                    Some("error" | "response.completed" | "response.failed")
                ) {
                    terminals += 1;
                }
            }
            if opcode == 8 {
                terminals += 1;
                return terminals;
            }
        }
    })
    .await
    .expect("server must answer cancel+close with a terminal close frame");
    assert_eq!(
        terminals, 1,
        "cancel+close must emit exactly one terminal frame"
    );
    stop.cancel();
}

#[tokio::test(start_paused = true)]
async fn periodic_ping_arrives_on_live_connection_after_two_minutes() {
    let probe = Arc::new(devin2api::server::websocket::WebSocketProbe::default());
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::new())),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let (_app, address, stop) = server_with_config(
        backend,
        HttpConfig {
            websocket_probe: Some(probe.clone()),
            ..HttpConfig::default()
        },
    )
    .await;
    let ping_armed = probe.ping_armed();
    tokio::pin!(ping_armed);
    let (mut client, _) = WsClient::connect(address, None).await;
    ping_armed.await;
    client.send_text("{malformed").await;
    client.until(&["error"]).await;
    let ping_frame = tokio::spawn(async move { client.try_frame().await });
    tokio::task::yield_now().await;
    tokio::time::advance(devin2api::server::websocket::PING_INTERVAL).await;
    let (opcode, payload) = ping_frame.await.unwrap().unwrap();
    assert_eq!((opcode, payload), (9, Vec::new()));
    stop.cancel();
}

#[tokio::test(start_paused = true)]
async fn stalled_write_expires_budget_and_closes_connection() {
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let backend = ScriptBackend {
        scripts: Arc::new(Mutex::new(VecDeque::from([Script::Flood {
            dropped: dropped_tx,
        }]))),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let probe = Arc::new(devin2api::server::websocket::WebSocketProbe::default());
    let (_app, address, stop) = server_with_config(
        backend,
        HttpConfig {
            websocket_probe: Some(probe.clone()),
            ..HttpConfig::default()
        },
    )
    .await;
    let (mut client, _) = WsClient::connect(address, None).await;
    socket2::SockRef::from(&client.stream)
        .set_recv_buffer_size(1024)
        .unwrap();
    probe.arm_write_blocked();
    let write_blocked = probe.write_blocked();
    tokio::pin!(write_blocked);
    client
        .send_text(r#"{"type":"response.create","model":"stub-model","input":"flood"}"#)
        .await;
    write_blocked.await;
    tokio::time::advance(devin2api::server::websocket::WRITE_DEADLINE).await;
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .unwrap()
        .unwrap();
    let mut replacements = Vec::with_capacity(devin2api::server::websocket::MAX_CONNECTIONS);
    for index in 0..devin2api::server::websocket::MAX_CONNECTIONS {
        let (replacement, response) = WsClient::connect(address, None).await;
        assert!(
            response.starts_with("HTTP/1.1 101"),
            "write-timeout connection still held its slot at replacement {index}: {response}"
        );
        replacements.push(replacement);
    }
    drop(replacements);
    drop(client);
    stop.cancel();
}

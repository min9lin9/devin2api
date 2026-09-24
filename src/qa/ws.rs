//! Real framed-client QA for the Responses WebSocket surface.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

use crate::debuglog::{Manager, RetentionPolicy};
use crate::domain::{
    AssistantMessage, Content, Failure, RequestMessages, ResponseEvent, ResponseEventType,
    StopReason, TextContent, ThinkingContent, ToolCall,
};
use crate::server::http::{App, BoxFuture, HttpBackend, HttpConfig, HttpEventStream};
use crate::upstream::catalog::ModelInfo;

const TIMEOUT: Duration = Duration::from_secs(10);

enum Script {
    Text(String),
    ToolReasoning,
    Pending {
        entered: Arc<Notify>,
        dropped: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
struct Stub {
    scripts: Arc<Mutex<VecDeque<Script>>>,
    requests: Arc<Mutex<Vec<RequestMessages>>>,
}

struct Events(VecDeque<ResponseEvent>);
impl HttpEventStream for Events {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let event = self.0.pop_front();
        Box::pin(async move { Ok(event) })
    }
}

struct Pending {
    entered: Arc<Notify>,
    cancel: CancellationToken,
    dropped: Option<oneshot::Sender<()>>,
}
impl HttpEventStream for Pending {
    fn recv(&mut self) -> BoxFuture<'_, Result<Option<ResponseEvent>, Failure>> {
        let entered = self.entered.clone();
        let cancel = self.cancel.clone();
        Box::pin(async move {
            entered.notify_one();
            cancel.cancelled().await;
            Err(Failure::plain("cancelled"))
        })
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

impl HttpBackend for Stub {
    fn list_models(
        &self,
        _cancel: CancellationToken,
    ) -> BoxFuture<'_, Result<Vec<ModelInfo>, Failure>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn stream(
        &self,
        request: RequestMessages,
        cancel: CancellationToken,
        _recorder: crate::debuglog::Recorder,
    ) -> BoxFuture<'_, Result<Box<dyn HttpEventStream>, Failure>> {
        self.requests.lock().unwrap().push(request);
        let script = self.scripts.lock().unwrap().pop_front().expect("QA script");
        Box::pin(async move {
            Ok(match script {
                Script::Text(text) => {
                    Box::new(Events(text_events(&text))) as Box<dyn HttpEventStream>
                }
                Script::ToolReasoning => {
                    Box::new(Events(tool_reasoning_events())) as Box<dyn HttpEventStream>
                }
                Script::Pending { entered, dropped } => Box::new(Pending {
                    entered,
                    cancel,
                    dropped: Some(dropped),
                }),
            })
        })
    }
}

fn tool_reasoning_events() -> VecDeque<ResponseEvent> {
    let thinking = ThinkingContent {
        thinking: "inspect".into(),
        thinking_signature: "sealed-signature".into(),
        signature_type: "sealed".into(),
        ..ThinkingContent::default()
    };
    let call = ToolCall {
        id: "call_qa".into(),
        name: "shell".into(),
        arguments: r#"{"cmd":"pwd"}"#.into(),
        ..ToolCall::default()
    };
    let partial = AssistantMessage {
        content: vec![
            Content::Thinking(thinking.clone()),
            Content::ToolCall(call.clone()),
        ],
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    VecDeque::from([
        ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ThinkingStart,
            content_index: 0,
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ThinkingDelta,
            content_index: 0,
            delta: "inspect".into(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ThinkingEnd,
            content_index: 0,
            content: "inspect".into(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ToolCallStart,
            content_index: 1,
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ToolCallDelta,
            content_index: 1,
            delta: call.arguments.clone(),
            tool_call_id: call.id.clone(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::ToolCallEnd,
            content_index: 1,
            tool_call: Some(Box::new(call)),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(StopReason::ToolUse),
            message: Some(Arc::new(AssistantMessage {
                content: partial.content,
                response_model: "stub-model".into(),
                stop_reason: Some(StopReason::ToolUse),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        },
    ])
}

fn text_events(text: &str) -> VecDeque<ResponseEvent> {
    let partial = AssistantMessage {
        content: vec![Content::Text(TextContent { text: text.into() })],
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    VecDeque::from([
        ResponseEvent {
            kind: ResponseEventType::Start,
            partial: Some(Arc::new(AssistantMessage {
                stop_reason: Some(StopReason::Pending),
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::TextStart,
            content_index: 0,
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::TextDelta,
            content_index: 0,
            delta: text.into(),
            partial: Some(Arc::new(partial.clone())),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::TextEnd,
            content_index: 0,
            content: text.into(),
            partial: Some(Arc::new(partial)),
            ..ResponseEvent::default()
        },
        ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(StopReason::Stop),
            message: Some(Arc::new(AssistantMessage {
                response_model: "stub-model".into(),
                stop_reason: Some(StopReason::Stop),
                content: vec![Content::Text(TextContent { text: text.into() })],
                ..AssistantMessage::default()
            })),
            ..ResponseEvent::default()
        },
    ])
}

struct Client(tokio::net::TcpStream);
impl Client {
    async fn connect(address: std::net::SocketAddr) -> anyhow::Result<(Self, String)> {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(format!(
            "GET /v1/responses HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: responses_websockets=2026-02-06\r\n\r\n"
        ).as_bytes()).await?;
        let mut response = Vec::new();
        loop {
            let mut byte = [0];
            tokio::time::timeout(TIMEOUT, stream.read_exact(&mut byte))
                .await
                .context("websocket handshake timeout")??;
            response.push(byte[0]);
            if response.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        Ok((Self(stream), String::from_utf8(response)?))
    }

    async fn text(&mut self, text: &str) -> anyhow::Result<()> {
        self.frame(1, text.as_bytes()).await
    }

    async fn frame(&mut self, opcode: u8, payload: &[u8]) -> anyhow::Result<()> {
        let mut frame = vec![0x80 | opcode];
        if payload.len() < 126 {
            frame.push(0x80 | u8::try_from(payload.len()).expect("len < 126"));
        } else if let Ok(len) = u16::try_from(payload.len()) {
            frame.push(0x80 | 0x7e);
            frame.extend_from_slice(&len.to_be_bytes());
        } else {
            frame.push(0x80 | 0x7f);
            frame.extend_from_slice(
                &u64::try_from(payload.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
        }
        let mask = [11_u8, 22, 33, 44];
        frame.extend_from_slice(&mask);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ mask[index % 4]),
        );
        self.0.write_all(&frame).await?;
        Ok(())
    }

    async fn read_frame(&mut self) -> anyhow::Result<(u8, Vec<u8>)> {
        let mut head = [0; 2];
        tokio::time::timeout(TIMEOUT, self.0.read_exact(&mut head))
            .await
            .context("websocket frame timeout")??;
        let mut length = u64::from(head[1] & 0x7f);
        if length == 126 {
            let mut bytes = [0; 2];
            self.0.read_exact(&mut bytes).await?;
            length = u64::from(u16::from_be_bytes(bytes));
        }
        if length == 127 {
            let mut bytes = [0; 8];
            self.0.read_exact(&mut bytes).await?;
            length = u64::from_be_bytes(bytes);
        }
        let mut payload = vec![0; usize::try_from(length).unwrap_or(usize::MAX)];
        self.0.read_exact(&mut payload).await?;
        Ok((head[0] & 0x0f, payload))
    }

    async fn until(&mut self, wanted: &[&str]) -> anyhow::Result<(Value, Vec<String>)> {
        let mut seen = Vec::new();
        for _ in 0..64 {
            let (opcode, payload) = self.read_frame().await?;
            if opcode == 9 {
                self.frame(10, &payload).await?;
                continue;
            }
            anyhow::ensure!(opcode == 1, "unexpected websocket opcode {opcode}");
            let event: Value = serde_json::from_slice(&payload)?;
            let kind = event["type"].as_str().unwrap_or_default().to_string();
            seen.push(kind.clone());
            if wanted.contains(&kind.as_str()) {
                return Ok((event, seen));
            }
        }
        anyhow::bail!("terminal event not received")
    }
}

async fn start(
    stub: Stub,
    logs: &Path,
) -> anyhow::Result<(App, std::net::SocketAddr, CancellationToken)> {
    let app = App::with_backend(
        stub,
        HttpConfig {
            max_concurrency: 2,
            debug_manager: Some(Arc::new(Manager::new(logs, &RetentionPolicy::default()))),
            ..HttpConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let stop = CancellationToken::new();
    tokio::spawn({
        let app = app.clone();
        let stop = stop.clone();
        async move {
            let _ = app.serve(listener, stop).await;
        }
    });
    Ok((app, address, stop))
}

// One sequential WS drill; splitting scatters the turn/cancel flow.
#[allow(clippy::too_many_lines)]
pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    if let Some(case) = case
        && case != "limits-and-disconnect"
    {
        anyhow::bail!("unknown websocket case {case}");
    }
    let logs = evidence.join(if case.is_some() {
        "logs-failure"
    } else {
        "logs-happy"
    });
    if logs.exists() {
        std::fs::remove_dir_all(&logs)?;
    }
    let requests = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(Notify::new());
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let cancel_close_entered = Arc::new(Notify::new());
    let (cancel_close_dropped_tx, cancel_close_dropped_rx) = oneshot::channel();
    let scripts = if case.is_some() {
        VecDeque::from([
            Script::Pending {
                entered: entered.clone(),
                dropped: dropped_tx,
            },
            Script::Pending {
                entered: cancel_close_entered.clone(),
                dropped: cancel_close_dropped_tx,
            },
        ])
    } else {
        VecDeque::from([
            Script::ToolReasoning,
            Script::Text("second".into()),
            Script::Pending {
                entered: entered.clone(),
                dropped: dropped_tx,
            },
            Script::Text("after cancel".into()),
        ])
    };
    let stub = Stub {
        scripts: Arc::new(Mutex::new(scripts)),
        requests: requests.clone(),
    };
    let (app, address, stop) = start(stub, &logs).await?;
    let (mut client, handshake) = Client::connect(address).await?;
    anyhow::ensure!(
        handshake.starts_with("HTTP/1.1 101"),
        "upgrade failed: {handshake}"
    );
    anyhow::ensure!(
        handshake
            .to_ascii_lowercase()
            .contains("sec-websocket-protocol: responses_websockets=2026-02-06"),
        "subprotocol not negotiated"
    );

    let report = if case.is_none() {
        client
            .text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"run pwd"}]}"#)
            .await?;
        let (first, first_order) = client
            .until(&["response.completed"])
            .await
            .context("first turn")?;
        let id = first
            .pointer("/response/id")
            .and_then(Value::as_str)
            .context("first response id")?;
        client.text(&format!(r#"{{"type":"response.create","previous_response_id":"{id}","input":[{{"type":"function_call_output","call_id":"call_qa","output":"/tmp"}},{{"type":"message","role":"user","content":"continue"}}]}}"#)).await?;
        let (_, second_order) = client
            .until(&["response.completed"])
            .await
            .context("second turn")?;
        let waiting = entered.notified();
        tokio::pin!(waiting);
        client
            .text(r#"{"type":"response.create","model":"stub-model","input":[]}"#)
            .await?;
        tokio::time::timeout(TIMEOUT, waiting)
            .await
            .context("cancel turn did not enter stub")?;
        client.text(r#"{"type":"response.cancel"}"#).await?;
        let (cancelled, _) = client.until(&["error"]).await.context("cancel event")?;
        tokio::time::timeout(TIMEOUT, dropped_rx)
            .await
            .context("cancelled stream was not dropped")??;
        client
            .text(r#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"after cancel"}]}"#)
            .await?;
        let (recovered, recovered_order) = client
            .until(&["response.completed"])
            .await
            .context("recovery turn")?;
        app.wait_idle().await;
        let counts: Vec<usize> = requests
            .lock()
            .unwrap()
            .iter()
            .map(|request| request.messages.len())
            .collect();
        let expected_first = vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.reasoning_summary_part.added",
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.done",
            "response.reasoning_summary_part.done",
            "response.output_item.done",
            "response.output_item.added",
            "response.function_call_arguments.delta",
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.completed",
        ];
        let expected_text = vec![
            "response.created",
            "response.in_progress",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.completed",
        ];
        let rows: Vec<Value> = std::fs::read_to_string(logs.join("index.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let api_labels: Vec<&str> = rows.iter().filter_map(|row| row["api"].as_str()).collect();
        json!({
            "passed": cancelled.pointer("/error/code") == Some(&Value::String("turn_cancelled".into()))
                && recovered["type"] == "response.completed"
                && counts.get(1).copied().unwrap_or_default() >= 4
                && first_order == expected_first && second_order == expected_text
                && recovered_order == expected_text
                && !api_labels.is_empty() && api_labels.iter().all(|label| *label == "responses-ws"),
            "real_tcp_server":true,"real_framed_client":true,"subprotocol":crate::server::websocket::SUBPROTOCOL,
            "first_order":first_order,"second_order":second_order,"recovered_order":recovered_order,
            "expected_first_order":expected_first,"expected_text_order":expected_text,
            "request_message_counts":counts,"debug_api_labels":api_labels
        })
    } else {
        client.text("{malformed").await?;
        let (malformed, _) = client.until(&["error"]).await.context("malformed event")?;
        let waiting = entered.notified();
        tokio::pin!(waiting);
        client
            .text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
            .await?;
        tokio::time::timeout(TIMEOUT, waiting)
            .await
            .context("disconnect turn did not enter stub")?;
        let queued_frame = vec![b'x'; 24 << 20];
        for _ in 0..3 {
            if client.frame(2, &queued_frame).await.is_err() {
                break;
            }
        }
        let (close_opcode, close_payload) = client.read_frame().await?;
        let close_code = close_payload
            .get(..2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .unwrap_or_default();
        let close_reason = String::from_utf8_lossy(close_payload.get(2..).unwrap_or_default());
        anyhow::ensure!(
            close_opcode == 8
                && close_code == 1009
                && close_reason == "inbound queue byte limit exceeded",
            "queue limit close = opcode {close_opcode}, code {close_code}, reason {close_reason:?}"
        );
        drop(client);
        tokio::time::timeout(TIMEOUT, dropped_rx)
            .await
            .context("disconnected stream was not dropped")??;
        app.wait_idle().await;

        let (mut cancel_close, response) = Client::connect(address).await?;
        anyhow::ensure!(
            response.starts_with("HTTP/1.1 101"),
            "cancel+close upgrade failed: {response}"
        );
        let cancel_close_wait = cancel_close_entered.notified();
        tokio::pin!(cancel_close_wait);
        cancel_close
            .text(r#"{"type":"response.create","model":"stub-model","input":"hold"}"#)
            .await?;
        tokio::time::timeout(TIMEOUT, cancel_close_wait)
            .await
            .context("cancel+close turn did not enter stub")?;
        cancel_close.text(r#"{"type":"response.cancel"}"#).await?;
        cancel_close.frame(8, &1000_u16.to_be_bytes()).await?;
        tokio::time::timeout(TIMEOUT, cancel_close_dropped_rx)
            .await
            .context("cancel+close stream was not dropped")??;
        let cancel_close_terminals = tokio::time::timeout(TIMEOUT, async {
            let mut terminals = 0_u64;
            loop {
                let (opcode, payload) = cancel_close.read_frame().await?;
                if opcode == 1 {
                    let event: Value = serde_json::from_slice(&payload)?;
                    if matches!(
                        event["type"].as_str(),
                        Some("error" | "response.completed" | "response.failed")
                    ) {
                        terminals += 1;
                    }
                }
                if opcode == 8 {
                    terminals += 1;
                    return anyhow::Ok(terminals);
                }
            }
        })
        .await
        .context("cancel+close terminal frame timeout")??;
        anyhow::ensure!(
            cancel_close_terminals == 1,
            "cancel+close emitted {cancel_close_terminals} terminal frames"
        );
        drop(cancel_close);

        let mut held_connections = Vec::with_capacity(crate::server::websocket::MAX_CONNECTIONS);
        for index in 0..crate::server::websocket::MAX_CONNECTIONS {
            let (connection, response) = Client::connect(address).await?;
            anyhow::ensure!(
                response.starts_with("HTTP/1.1 101"),
                "connection {index} was not admitted: {response}"
            );
            held_connections.push(connection);
        }
        let (rejected_connection, rejected_response) = Client::connect(address).await?;
        anyhow::ensure!(
            rejected_response.starts_with("HTTP/1.1 429"),
            "connection cap response: {rejected_response}"
        );
        drop(rejected_connection);
        drop(held_connections);

        let rows: Vec<Value> = std::fs::read_to_string(logs.join("index.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        let phantom_completed = rows.iter().any(|row| row["result"] == "completed");
        json!({
            "passed":malformed.pointer("/error/code") == Some(&Value::String("invalid_request".into())) && close_code == 1009 && close_reason == "inbound queue byte limit exceeded" && cancel_close_terminals == 1 && rejected_response.starts_with("HTTP/1.1 429") && !phantom_completed,
            "malformed":malformed,"disconnect_cancelled":true,"queue_close_code":close_code,"queue_close_reason":close_reason,"cancel_close_terminal_frames":cancel_close_terminals,"connection_limit_status":429,"phantom_completed":phantom_completed,
            "limits":{"message_bytes":crate::server::websocket::MAX_TRANSCRIPT_BYTES,"queue_bytes":crate::server::websocket::MAX_QUEUED_BYTES,"connections":crate::server::websocket::MAX_CONNECTIONS}
        })
    };
    stop.cancel();
    std::fs::write(
        evidence.join(if case.is_some() {
            "limits-and-disconnect.json"
        } else {
            "websocket.json"
        }),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(i32::from(report["passed"] != true))
}

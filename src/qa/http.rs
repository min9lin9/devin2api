//! Task-14 process-level HTTP QA: a child HTTP server uses the production
//! Adapter against a separate Connect-wire stub child.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use buffa::{Message, MessageField};
use devin_proto::generated::exa::api_server_pb as pb;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::debuglog::{Manager, RetentionPolicy};
use crate::server::http::{App, HttpConfig};
use crate::upstream::catalog::{Adapter, AdapterConfig};

const QA_TIMEOUT: Duration = Duration::from_secs(20);
const CHAT_PATH: &str = "/exa.api_server_pb.ApiServerService/GetChatMessage";

fn envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(payload.len() + 5);
    output.push(flag);
    output.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    output.extend_from_slice(payload);
    output
}

fn chat_stream() -> Vec<u8> {
    let frame = |message: &pb::GetChatMessageResponse| envelope(0, &message.encode_to_vec());
    let mut output = frame(&pb::GetChatMessageResponse {
        message_id: Some("bot-http-qa".into()),
        request_id: Some("upstream-http-qa".into()),
        usage: MessageField::some(pb::ExaCodeiumCommonPb_ModelUsageStats {
            model_uid: Some("stub-model".into()),
            ..Default::default()
        }),
        ..Default::default()
    });
    output.extend(frame(&pb::GetChatMessageResponse {
        delta_text: Some("pong".into()),
        ..Default::default()
    }));
    output.extend(frame(&pb::GetChatMessageResponse {
        stop_reason: Some(pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN),
        ..Default::default()
    }));
    output.extend(envelope(2, b"{}"));
    output
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> std::io::Result<(String, bool)> {
    let mut data = Vec::new();
    let mut part = [0_u8; 8192];
    let head_end = loop {
        let count = stream.read(&mut part).await?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        data.extend_from_slice(&part[..count]);
        if let Some(position) = data.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if data.len() > 1 << 20 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
    };
    let head = String::from_utf8_lossy(&data[..head_end]);
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_string();
    let json_wire = head
        .to_ascii_lowercase()
        .contains("application/connect+json");
    let length = head
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let have = data.len() - head_end;
    if have < length {
        let mut remaining = vec![0; length - have];
        stream.read_exact(&mut remaining).await?;
    }
    Ok((path, json_wire))
}

/// Hidden child mode used only by the QA parent.
pub async fn upstream_child(mode: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("READY {}", listener.local_addr()?);
    // A semaphore retains RELEASE when stdin wins the scheduling race after
    // REQUEST is printed but before the connection arms its wait. Notify's
    // notify_waiters loses that signal and makes lifecycle QA time out.
    let release = Arc::new(Semaphore::new(0));
    let stdin_release = release.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line == "RELEASE" {
                stdin_release.add_permits(1);
            }
        }
    });
    loop {
        let (mut socket, _) = listener.accept().await?;
        let release = release.clone();
        let mode = mode.to_string();
        tokio::spawn(async move {
            let Ok((path, json_wire)) = read_request(&mut socket).await else {
                return;
            };
            if mode == "unauthenticated" {
                // Connect maps HTTP 401 to `unauthenticated` regardless of
                // body; the JSON body mirrors connect-go's error shape.
                let body = br#"{"code":"unauthenticated","message":"invalid session token"}"#;
                let head = format!(
                    "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
                return;
            }
            if path != CHAT_PATH {
                let (content_type, body) = if json_wire {
                    ("application/json", b"{}".to_vec())
                } else {
                    ("application/proto", Vec::new())
                };
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
                return;
            }
            let content_type = if json_wire {
                "application/connect+json"
            } else {
                "application/connect+proto"
            };
            if mode == "early-error" {
                let body = b"upstream unavailable";
                let head = format!(
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
                return;
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nConnection: close\r\n\r\n"
            );
            if socket.write_all(head.as_bytes()).await.is_err() {
                return;
            }
            println!("REQUEST");
            if (mode == "gate" || mode == "late-error")
                && let Ok(permit) = release.acquire().await
            {
                permit.forget();
                println!("RELEASED");
            }
            if mode == "late-error" {
                let _ = socket
                    .write_all(&envelope(
                        2,
                        br#"{"error":{"code":"unavailable","message":"late stub failure"}}"#,
                    ))
                    .await;
            } else {
                let _ = socket.write_all(&chat_stream()).await;
            }
        });
    }
}

/// Hidden production-server child mode used only by the QA parent.
pub async fn server_child(
    upstream: &str,
    logs: &Path,
    api_key: &str,
    max_concurrency: usize,
    draining: bool,
) -> anyhow::Result<()> {
    let adapter = Adapter::new(AdapterConfig {
        base_url: upstream.to_string(),
        token: "qa-token".into(),
        model: "stub-model".into(),
        force_http1: true,
        ..AdapterConfig::default()
    })?;
    let manager = Arc::new(Manager::new(logs, &RetentionPolicy::default()));
    let app = App::with_backend(
        adapter,
        HttpConfig {
            api_key: api_key.to_string(),
            max_concurrency,
            version: "qa-http".into(),
            debug_manager: Some(manager),
            ..HttpConfig::default()
        },
    );
    if draining {
        app.begin_drain();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    println!("READY {}", listener.local_addr()?);
    let control_app = app.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line == "WAIT_IDLE" {
                control_app.wait_idle().await;
                println!("IDLE");
            }
        }
    });
    app.serve(listener, CancellationToken::new()).await?;
    Ok(())
}

struct Process {
    child: Child,
    stdin: ChildStdin,
    lines: mpsc::Receiver<String>,
    address: String,
}

impl Process {
    async fn start(args: &[String]) -> anyhow::Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child.stdin.take().context("child stdin")?;
        let stdout = child.stdout.take().context("child stdout")?;
        let (tx, mut lines) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                if tx.send(line).await.is_err() {
                    break;
                }
            }
        });
        let ready = tokio::time::timeout(QA_TIMEOUT, lines.recv())
            .await
            .context("child readiness timeout")?
            .context("child exited before readiness")?;
        let address = ready
            .strip_prefix("READY ")
            .context("invalid readiness line")?
            .to_string();
        Ok(Self {
            child,
            stdin,
            lines,
            address,
        })
    }

    async fn command(&mut self, command: &str) -> anyhow::Result<()> {
        self.stdin
            .write_all(format!("{command}\n").as_bytes())
            .await?;
        self.stdin.flush().await?;
        Ok(())
    }

    async fn event(&mut self, expected: &str) -> anyhow::Result<()> {
        let line = tokio::time::timeout(QA_TIMEOUT, self.lines.recv())
            .await
            .context("child event timeout")?
            .context("child exited before event")?;
        anyhow::ensure!(
            line == expected,
            "child event {line:?}, expected {expected:?}"
        );
        Ok(())
    }

    async fn stop(mut self) -> anyhow::Result<()> {
        self.child.kill().await?;
        tokio::time::timeout(QA_TIMEOUT, self.child.wait())
            .await
            .context("child exit timeout")??;
        Ok(())
    }
}

async fn pair(
    evidence: &Path,
    mode: &str,
    api_key: &str,
    max: usize,
    draining: bool,
) -> anyhow::Result<(Process, Process, String, PathBuf)> {
    let mut upstream = Process::start(&["__http-upstream".into(), mode.into()]).await?;
    let logs = evidence.join(format!("logs-{mode}-{}", crate::randid::hex(4)));
    let server = Process::start(&[
        "__http-server".into(),
        format!("http://{}", upstream.address),
        logs.display().to_string(),
        api_key.into(),
        max.to_string(),
        draining.to_string(),
    ])
    .await?;
    let base = format!("http://{}", server.address);
    // Keep mutable ownership explicit; both children are stopped by every path.
    upstream.stdin.flush().await?;
    Ok((upstream, server, base, logs))
}

async fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
    key: Option<&str>,
    body: Option<&str>,
) -> anyhow::Result<Value> {
    let mut request = client.request(method, url).timeout(QA_TIMEOUT);
    if let Some(key) = key {
        request = request.bearer_auth(key);
    }
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(key, value)| {
            (
                key.to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let body = response.text().await?;
    Ok(json!({"status":status,"headers":headers,"body":body}))
}

fn index_rows(logs: &Path) -> Vec<Value> {
    std::fs::read_to_string(logs.join("index.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

// One sequential route matrix; splitting scatters the case table.
#[allow(clippy::too_many_lines)]
pub async fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let client = reqwest::Client::builder().build()?;
    if let Some(case) = case
        && case != "precommit-errors"
    {
        anyhow::bail!("unknown http case {case}");
    }
    if case.is_none() {
        let (upstream, server, base, _) = pair(evidence, "normal", "", 8, false).await?;
        let mut cases = Vec::new();
        cases.push((
            "models",
            request(
                &client,
                reqwest::Method::GET,
                format!("{base}/v1/models"),
                None,
                None,
            )
            .await?,
        ));
        cases.push((
            "model",
            request(
                &client,
                reqwest::Method::GET,
                format!("{base}/v1/models/stub-model"),
                None,
                None,
            )
            .await?,
        ));
        for (name, path, body) in [
            (
                "responses-json",
                "/v1/responses",
                r#"{"model":"stub-model","input":"hi"}"#,
            ),
            (
                "responses-sse",
                "/v1/responses",
                r#"{"model":"stub-model","stream":true,"input":"hi"}"#,
            ),
            (
                "chat-json",
                "/v1/chat/completions",
                r#"{"model":"stub-model","messages":[{"role":"user","content":"hi"}]}"#,
            ),
            (
                "chat-sse",
                "/v1/chat/completions",
                r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            (
                "messages-json",
                "/v1/messages",
                r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
            (
                "messages-sse",
                "/v1/messages",
                r#"{"model":"stub-model","stream":true,"max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
            ),
        ] {
            cases.push((
                name,
                request(
                    &client,
                    reqwest::Method::POST,
                    format!("{base}{path}"),
                    None,
                    Some(body),
                )
                .await?,
            ));
        }
        let ok = cases.iter().all(|(_, response)| response["status"] == 200);
        server.stop().await?;
        upstream.stop().await?;
        std::fs::write(
            evidence.join("http.json"),
            serde_json::to_vec_pretty(
                &json!({"server_process":"child qa __http-server","upstream_process":"child qa __http-upstream","production_adapter":true,"cases":cases,"passed":ok}),
            )?,
        )?;
        return Ok(i32::from(!ok));
    }

    let (upstream, server, base, _) = pair(evidence, "normal", "secret", 8, false).await?;
    let unauthorized = request(
        &client,
        reqwest::Method::GET,
        format!("{base}/v1/models"),
        None,
        None,
    )
    .await?;
    server.stop().await?;
    upstream.stop().await?;

    let (mut upstream, server, base, _) = pair(evidence, "gate", "", 1, false).await?;
    let first_client = client.clone();
    let first_url = format!("{base}/v1/responses");
    let first = tokio::spawn(async move {
        request(
            &first_client,
            reqwest::Method::POST,
            first_url,
            None,
            Some(r#"{"model":"stub-model","input":"hi"}"#),
        )
        .await
    });
    upstream.event("REQUEST").await?;
    let overloaded = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/v1/responses"),
        None,
        Some(r#"{"model":"stub-model","input":"hi"}"#),
    )
    .await?;
    upstream.command("RELEASE").await?;
    let _ = tokio::time::timeout(QA_TIMEOUT, first).await??;
    server.stop().await?;
    upstream.stop().await?;

    let (upstream, server, base, _) = pair(evidence, "normal", "", 8, true).await?;
    let draining = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/v1/responses"),
        None,
        Some(r#"{"model":"stub-model","input":"hi"}"#),
    )
    .await?;
    server.stop().await?;
    upstream.stop().await?;

    let (upstream, server, base, _) = pair(evidence, "early-error", "", 8, false).await?;
    let early = request(
        &client,
        reqwest::Method::POST,
        format!("{base}/v1/messages"),
        None,
        Some(
            r#"{"model":"stub-model","max_tokens":8,"messages":[{"role":"user","content":"hi"}]}"#,
        ),
    )
    .await?;
    server.stop().await?;
    upstream.stop().await?;

    let (mut upstream, mut server, base, _) = pair(evidence, "late-error", "", 8, false).await?;
    let late_client = client.clone();
    let late_url = format!("{base}/v1/responses");
    let late_task = tokio::spawn(async move {
        late_client
            .post(late_url)
            .header("content-type", "application/json")
            .body(r#"{"model":"stub-model","stream":true,"input":"hi"}"#)
            .timeout(QA_TIMEOUT)
            .send()
            .await
    });
    upstream.event("REQUEST").await?;
    let late_response = tokio::time::timeout(QA_TIMEOUT, late_task).await???;
    let late_status = late_response.status().as_u16();
    upstream.command("RELEASE").await?;
    let late_body = late_response.text().await?;
    server.command("WAIT_IDLE").await?;
    server.event("IDLE").await?;
    let late = json!({"status":late_status,"body":late_body});
    server.stop().await?;
    upstream.stop().await?;

    let (mut upstream, mut server, base, logs) = pair(evidence, "late-error", "", 8, false).await?;
    let cancel_client = client.clone();
    let cancel_url = format!("{base}/v1/responses");
    let cancel_task = tokio::spawn(async move {
        cancel_client
            .post(cancel_url)
            .header("content-type", "application/json")
            .body(r#"{"model":"stub-model","stream":true,"input":"hi"}"#)
            .timeout(QA_TIMEOUT)
            .send()
            .await
    });
    upstream.event("REQUEST").await?;
    let cancel_response = tokio::time::timeout(QA_TIMEOUT, cancel_task).await???;
    drop(cancel_response);
    upstream.command("RELEASE").await?;
    server.command("WAIT_IDLE").await?;
    server.event("IDLE").await?;
    let cancel_rows = index_rows(&logs);
    server.stop().await?;
    upstream.stop().await?;

    let cancellation_ok = cancel_rows.len() == 1 && cancel_rows[0]["result"] == "disconnected";
    let ok = unauthorized["status"] == 401
        && overloaded["status"] == 429
        && draining["status"] == 503
        && early["status"].as_u64().is_some_and(|status| status >= 500)
        && late["status"] == 200
        && late["body"]
            .as_str()
            .is_some_and(|body| body.contains("response.failed"))
        && cancellation_ok;
    let report = json!({"unauthorized":unauthorized,"overloaded":overloaded,"draining":draining,"early_upstream_error":early,"post_heartbeat_error":late,"cancellation_rows":cancel_rows,"cancellation_no_phantom_logs":cancellation_ok,"production_adapter":true,"passed":ok});
    std::fs::write(
        evidence.join("precommit-errors.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    Ok(i32::from(!ok))
}

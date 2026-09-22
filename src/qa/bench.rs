//! Task-24 performance gate: paired Go/Rust benchmark matrix.
//!
//! Same host, sequential isolated legs, one shared loopback Go upstream
//! stub, matched configs (rate shaping disabled in BOTH benchmark configs
//! only), fixed worker counts (4 = host cores, Go via GOMAXPROCS, Rust via
//! tokio default). Go legs run the recorded baseline binaries built from
//! unmodified G at the pinned commit with the checked-in PGO profile (task 2
//! artifacts; sha256 + build metadata recorded). Primary cells take ten
//! paired 30s samples in seeded ABBA order; secondary cells take three
//! paired 10s samples and are descriptive only. Raw samples, per-cell
//! bootstrap CIs and build hashes are written under `--evidence`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use anyhow::Context as _;
use serde::Serialize;
use serde_json::{Value, json};

use super::oracle::GoOracle;
use super::process::{self, ManagedChild};
use super::verdict::{self, Cell, Sample, SplitMix64};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PRIMARY_SAMPLE_SECS: u64 = 30;
const SECONDARY_SAMPLE_SECS: u64 = 10;
/// Hard cap for the whole bench run (well above the expected ~100 minutes).
const RUN_BUDGET: Duration = Duration::from_hours(3);

/// Which upstream stub scenario shape a cell measures.
#[derive(Debug, Clone, Copy)]
pub struct StubShape {
    pub deltas: u32,
    pub delta_bytes: u32,
    pub interval_ms: u32,
    pub ttfb_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Protocol {
    ChatSse,
    ChatJson,
    ResponsesSse,
    MessagesSse,
    Ws,
}

#[derive(Debug, Clone, Copy)]
pub struct CellSpec {
    pub name: &'static str,
    pub protocol: Protocol,
    pub concurrency: u32,
    pub debug: bool,
    pub primary: bool,
    pub shape: StubShape,
    pub samples: usize,
    pub sample_secs: u64,
}

const CHAT_SHAPE: StubShape = StubShape {
    deltas: 200,
    delta_bytes: 32,
    interval_ms: 0,
    ttfb_ms: 0,
};

/// The published matrix: 8 primary Chat-SSE cells + secondary
/// representative cells (no Cartesian blow-up).
pub fn matrix() -> Vec<CellSpec> {
    let mut cells = Vec::new();
    for concurrency in [1u32, 8, 64, 256] {
        for debug in [false, true] {
            cells.push(CellSpec {
                name: if debug {
                    match concurrency {
                        1 => "chat-sse-c1-debug-on",
                        8 => "chat-sse-c8-debug-on",
                        64 => "chat-sse-c64-debug-on",
                        _ => "chat-sse-c256-debug-on",
                    }
                } else {
                    match concurrency {
                        1 => "chat-sse-c1-debug-off",
                        8 => "chat-sse-c8-debug-off",
                        64 => "chat-sse-c64-debug-off",
                        _ => "chat-sse-c256-debug-off",
                    }
                },
                protocol: Protocol::ChatSse,
                concurrency,
                debug,
                primary: true,
                shape: CHAT_SHAPE,
                samples: verdict::PRIMARY_SAMPLES,
                sample_secs: PRIMARY_SAMPLE_SECS,
            });
        }
    }
    let secondary = [
        ("responses-sse-c8", Protocol::ResponsesSse, 8, CHAT_SHAPE),
        ("messages-sse-c8", Protocol::MessagesSse, 8, CHAT_SHAPE),
        ("chat-json-c8", Protocol::ChatJson, 8, CHAT_SHAPE),
        ("ws-c8", Protocol::Ws, 8, CHAT_SHAPE),
        (
            "chat-sse-4kib-c64",
            Protocol::ChatSse,
            64,
            StubShape {
                delta_bytes: 4096,
                ..CHAT_SHAPE
            },
        ),
        (
            "chat-sse-2000d-c64",
            Protocol::ChatSse,
            64,
            StubShape {
                deltas: 2000,
                ..CHAT_SHAPE
            },
        ),
        (
            "chat-sse-ttft1s-c8",
            Protocol::ChatSse,
            8,
            StubShape {
                ttfb_ms: 1000,
                interval_ms: 5,
                ..CHAT_SHAPE
            },
        ),
    ];
    for (name, protocol, concurrency, shape) in secondary {
        cells.push(CellSpec {
            name,
            protocol,
            concurrency,
            debug: false,
            primary: false,
            shape,
            samples: verdict::SECONDARY_SAMPLES,
            sample_secs: SECONDARY_SAMPLE_SECS,
        });
    }
    cells
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    use sha2::Digest as _;
    let data = std::fs::read(path)?;
    Ok(hex_lower(&sha2::Sha256::digest(&data)))
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Binaries under test plus provenance.
pub struct Binaries {
    pub go_daemon: PathBuf,
    pub go_stub: PathBuf,
    pub rust_daemon: PathBuf,
    pub go_source: String,
    pub rust_sha256: String,
    pub go_daemon_sha256: String,
    pub go_stub_sha256: String,
}

/// Resolve the Go side from the recorded task-2 baseline (same-host build
/// of unmodified G with the checked-in PGO profile). If the recording is
/// absent and a Go toolchain is resolvable, build into the evidence dir as
/// a fallback and say so in `go_source`.
pub fn resolve_binaries(go_root: &Path, evidence: &Path) -> anyhow::Result<Binaries> {
    let recorded = evidence
        .ancestors()
        .find_map(|dir| {
            let candidate = dir
                .join("oracle-baseline/oracle-work/bin")
                .canonicalize()
                .ok()?;
            candidate.join("devin-2api").is_file().then_some(candidate)
        })
        .or_else(|| {
            let candidate = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../.omo/evidence/devin2api-rust-parity/oracle-baseline/oracle-work/bin");
            let candidate = candidate.canonicalize().ok()?;
            candidate.join("devin-2api").is_file().then_some(candidate)
        });
    let (go_daemon, go_stub, go_source) = if let Some(bin) = recorded {
        let baseline_meta = bin
            .parent()
            .and_then(|work| work.parent())
            .map(|base| base.join("baseline.json"))
            .filter(|p| p.is_file())
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|text| serde_json::from_str::<Value>(&text).ok());
        let commit = baseline_meta
            .as_ref()
            .and_then(|m| m["go_commit"].as_str())
            .unwrap_or("unknown");
        let version = baseline_meta
            .as_ref()
            .and_then(|m| m["daemon_version"].as_str())
            .unwrap_or("unknown");
        (
            bin.join("devin-2api"),
            bin.join("upstreamstub"),
            format!(
                "recorded task-2 baseline binaries: go_commit={commit} daemon_version={version} (built from unmodified G with checked-in PGO profile)"
            ),
        )
    } else {
        let oracle = GoOracle::new(go_root.to_path_buf());
        let out = evidence.join("go-build");
        oracle.build(
            &out,
            &["./cmd/devin-2api".into(), "./cmd/upstreamstub".into()],
        )?;
        (
            out.join("bin/devin-2api"),
            out.join("bin/upstreamstub"),
            "fallback: built from --go-root with default production flags (checked-in PGO auto-applied)".to_string(),
        )
    };
    let exe = std::env::current_exe()?;
    let dir = exe
        .parent()
        .and_then(|p| {
            if p.file_name().is_some_and(|n| n == "deps") {
                p.parent()
            } else {
                Some(p)
            }
        })
        .context("qa executable has no target dir")?;
    let rust_daemon = dir.join("devin-2api");
    anyhow::ensure!(
        rust_daemon.is_file(),
        "missing Rust release daemon {}; build --release --bins first",
        rust_daemon.display()
    );
    anyhow::ensure!(
        go_daemon.is_file(),
        "missing Go daemon {}",
        go_daemon.display()
    );
    anyhow::ensure!(go_stub.is_file(), "missing Go stub {}", go_stub.display());
    Ok(Binaries {
        go_daemon_sha256: sha256_file(&go_daemon)?,
        go_stub_sha256: sha256_file(&go_stub)?,
        rust_sha256: sha256_file(&rust_daemon)?,
        go_daemon,
        go_stub,
        rust_daemon,
        go_source,
    })
}

fn scrubbed(command: &mut Command) {
    for var in [
        "DEVIN_TOKEN",
        "WINDSURF_API_KEY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "DEVIN2API_CONFIG",
        "DEVIN2API_STATE_DIR",
    ] {
        command.env_remove(var);
    }
}

pub struct Stack {
    pub daemon: ManagedChild,
    pub stub: ManagedChild,
    pub base: String,
    pub state: PathBuf,
    pub stub_stderr: PathBuf,
}

impl Stack {
    pub async fn stop(mut self) {
        let _ = self.daemon.shutdown(Duration::from_secs(10)).await;
        let _ = self.stub.shutdown(Duration::from_secs(3)).await;
    }
}

/// Launch one daemon leg + the shared-shape stub. `go` selects the daemon
/// binary; the stub is always the same Go upstreamstub binary.
pub async fn launch(
    work: &Path,
    label: &str,
    daemon_bin: &Path,
    stub_bin: &Path,
    shape: &StubShape,
    debug: bool,
    api_key: &str,
) -> anyhow::Result<Stack> {
    let stub_port = process::free_port()?;
    let daemon_port = process::free_port()?;
    let state = work.join(format!("{label}-state"));
    std::fs::create_dir_all(&state)?;

    let mut stub_cmd = Command::new(stub_bin);
    stub_cmd.args([
        "-listen",
        &format!("127.0.0.1:{stub_port}"),
        "-scenario",
        "stream",
        "-deltas",
        &shape.deltas.to_string(),
        "-delta-bytes",
        &shape.delta_bytes.to_string(),
        "-interval",
        &format!("{}ms", shape.interval_ms),
        "-ttfb",
        &format!("{}ms", shape.ttfb_ms),
    ]);
    scrubbed(&mut stub_cmd);
    let stub = process::spawn_logged(work, &format!("{label}-stub"), &mut stub_cmd)?;
    let stub_stderr = stub.stderr_log();
    let mut stub = stub;
    process::wait_tcp(&mut stub, stub_port, STARTUP_TIMEOUT).await?;

    let config = work.join(format!("{label}.yaml"));
    std::fs::write(
        &config,
        format!(
            "server:\n  listen: \"127.0.0.1:{daemon_port}\"\n  max_concurrency: 1024\n\
             devin:\n  base_url: \"http://127.0.0.1:{stub_port}\"\n  token: \"qa-bench-token\"\n  model: \"stub-model\"\n  force_http1: true\n  max_rpm: 0\n\
             auth:\n  api_key: \"{api_key}\"\n\
             debug:\n  enabled: {debug}\n"
        ),
    )?;
    let mut daemon_cmd = Command::new(daemon_bin);
    daemon_cmd.args([
        "-config",
        config.to_string_lossy().as_ref(),
        "-state-dir",
        state.to_string_lossy().as_ref(),
    ]);
    scrubbed(&mut daemon_cmd);
    // Fixed runtime worker count: 4 host cores for both runtimes.
    daemon_cmd.env("GOMAXPROCS", "4");
    let mut daemon = process::spawn_logged(work, &format!("{label}-daemon"), &mut daemon_cmd)?;
    let base = format!("http://127.0.0.1:{daemon_port}");
    if let Err(error) =
        process::wait_ready(&mut daemon, &format!("{base}/healthz"), STARTUP_TIMEOUT).await
    {
        let _ = daemon.shutdown(Duration::from_secs(2)).await;
        let _ = stub.shutdown(Duration::from_secs(2)).await;
        return Err(error);
    }
    Ok(Stack {
        daemon,
        stub,
        base,
        state,
        stub_stderr,
    })
}

/// One measured request.
#[derive(Debug, Default, Clone)]
pub struct RequestRecord {
    pub ttfb_ms: Option<f64>,
    pub ttft_ms: Option<f64>,
    pub total_ms: f64,
    pub bytes: u64,
    pub error: Option<String>,
    /// SSE legs: terminal marker seen exactly once.
    pub terminal_ok: bool,
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

pub fn request_spec(protocol: Protocol) -> (&'static str, Vec<u8>) {
    match protocol {
        Protocol::ChatSse => (
            "/v1/chat/completions",
            br#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"load test"}]}"#
                .to_vec(),
        ),
        Protocol::ChatJson => (
            "/v1/chat/completions",
            br#"{"model":"stub-model","stream":false,"messages":[{"role":"user","content":"load test"}]}"#
                .to_vec(),
        ),
        Protocol::ResponsesSse => (
            "/v1/responses",
            br#"{"model":"stub-model","stream":true,"input":"load test"}"#.to_vec(),
        ),
        Protocol::MessagesSse => (
            "/v1/messages",
            br#"{"model":"stub-model","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"load test"}]}"#
                .to_vec(),
        ),
        Protocol::Ws => ("/v1/responses", Vec::new()),
    }
}

fn semantic_content(protocol: Protocol, payload: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(payload) else {
        return false;
    };
    match protocol {
        Protocol::ChatSse => value
            .pointer("/choices/0/delta/content")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty()),
        Protocol::ResponsesSse => {
            value["type"].as_str() == Some("response.output_text.delta")
                && value["delta"].as_str().is_some_and(|s| !s.is_empty())
        }
        Protocol::MessagesSse => {
            value["type"].as_str() == Some("content_block_delta")
                && value
                    .pointer("/delta/text")
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty())
        }
        _ => false,
    }
}

fn is_terminal(protocol: Protocol, payload: &str) -> bool {
    match protocol {
        Protocol::ChatSse => payload.trim() == "[DONE]",
        Protocol::ResponsesSse => serde_json::from_str::<Value>(payload)
            .is_ok_and(|v| v["type"].as_str() == Some("response.completed")),
        Protocol::MessagesSse => serde_json::from_str::<Value>(payload)
            .is_ok_and(|v| v["type"].as_str() == Some("message_stop")),
        _ => false,
    }
}

/// One streaming/JSON HTTP request against the daemon under test.
pub async fn http_request(
    client: &reqwest::Client,
    base: &str,
    api_key: &str,
    protocol: Protocol,
) -> RequestRecord {
    let (path, body) = request_spec(protocol);
    let started = Instant::now();
    let send = client
        .post(format!("{base}{path}"))
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {api_key}"))
        .body(body)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await;
    let resp = match send {
        Ok(resp) => resp,
        Err(err) => {
            return RequestRecord {
                total_ms: ms(started.elapsed()),
                error: Some(err.to_string()),
                ..Default::default()
            };
        }
    };
    let status = resp.status();
    let is_sse = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/event-stream"));
    let mut record = RequestRecord::default();
    let mut terminals = 0u32;
    let mut line: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                if !bytes.is_empty() && record.ttfb_ms.is_none() {
                    record.ttfb_ms = Some(ms(started.elapsed()));
                }
                record.bytes += bytes.len() as u64;
                if is_sse {
                    line.extend_from_slice(&bytes);
                    while let Some(nl) = line.iter().position(|b| *b == b'\n') {
                        let text: Vec<u8> = line.drain(..=nl).collect();
                        let text = String::from_utf8_lossy(&text);
                        if let Some(payload) = text.trim_end().strip_prefix("data:") {
                            let payload = payload.trim();
                            if is_terminal(protocol, payload) {
                                terminals += 1;
                            }
                            if record.ttft_ms.is_none() && semantic_content(protocol, payload) {
                                record.ttft_ms = Some(ms(started.elapsed()));
                            }
                        }
                    }
                }
            }
            Err(err) => {
                record.total_ms = ms(started.elapsed());
                record.error = Some(format!("stream truncated: {err}"));
                return record;
            }
        }
    }
    record.total_ms = ms(started.elapsed());
    if status != reqwest::StatusCode::OK {
        record.error = Some(format!("status {}", status.as_u16()));
        return record;
    }
    if is_sse {
        record.terminal_ok = terminals == 1;
        if terminals != 1 {
            record.error = Some(format!("terminal events: {terminals}"));
        }
        if record.ttft_ms.is_none() && record.error.is_none() {
            record.error = Some("no semantic content".into());
        }
    } else {
        record.terminal_ok = true;
        record.ttft_ms = Some(record.total_ms);
    }
    if record.ttfb_ms.is_none() {
        record.ttfb_ms = Some(record.total_ms);
    }
    record
}

/// A minimal framed WebSocket client for the Responses WS surface
/// (same handshake shape as `qa::ws`, against the real daemon).
pub struct WsClient(tokio::net::TcpStream);

impl WsClient {
    pub async fn connect(base: &str, api_key: &str) -> anyhow::Result<Self> {
        let address = base
            .strip_prefix("http://")
            .context("ws bench needs http:// base")?
            .to_string();
        let mut stream = tokio::net::TcpStream::connect(&address).await?;
        stream
            .write_all(
                format!(
                    "GET /v1/responses HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: responses_websockets=2026-02-06\r\nAuthorization: Bearer {api_key}\r\n\r\n"
                )
                .as_bytes(),
            )
            .await?;
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8];
            tokio::time::timeout(REQUEST_TIMEOUT, stream.read_exact(&mut byte))
                .await
                .context("ws handshake timeout")??;
            response.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&response).to_string();
        anyhow::ensure!(head.contains("101"), "ws handshake rejected: {head}");
        Ok(Self(stream))
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
        let mask = [7_u8, 17, 27, 37];
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
        let mut head = [0u8; 2];
        tokio::time::timeout(REQUEST_TIMEOUT, self.0.read_exact(&mut head))
            .await
            .context("ws frame timeout")??;
        let mut length = u64::from(head[1] & 0x7f);
        if length == 126 {
            let mut bytes = [0u8; 2];
            self.0.read_exact(&mut bytes).await?;
            length = u64::from(u16::from_be_bytes(bytes));
        } else if length == 127 {
            let mut bytes = [0u8; 8];
            self.0.read_exact(&mut bytes).await?;
            length = u64::from_be_bytes(bytes);
        }
        let mut payload = vec![0u8; usize::try_from(length).unwrap_or(usize::MAX)];
        self.0.read_exact(&mut payload).await?;
        Ok((head[0] & 0x0f, payload))
    }

    /// One WS turn: response.create, read until response.completed.
    /// Returns (first-content latency, total latency, bytes, terminal count).
    pub async fn turn(&mut self) -> anyhow::Result<(f64, f64, u64, u32)> {
        let started = Instant::now();
        self.frame(
            1,
            br#"{"type":"response.create","model":"stub-model","input":[{"type":"message","role":"user","content":"load test"}]}"#,
        )
        .await?;
        let mut first_content = None;
        let mut bytes = 0u64;
        let mut terminals = 0u32;
        loop {
            let (opcode, payload) = self.read_frame().await?;
            if opcode == 9 {
                self.frame(10, &payload).await?;
                continue;
            }
            anyhow::ensure!(opcode == 1, "unexpected ws opcode {opcode}");
            bytes += payload.len() as u64;
            let event: Value = serde_json::from_slice(&payload)?;
            let kind = event["type"].as_str().unwrap_or_default();
            if first_content.is_none()
                && kind == "response.output_text.delta"
                && event["delta"].as_str().is_some_and(|s| !s.is_empty())
            {
                first_content = Some(ms(started.elapsed()));
            }
            if kind == "response.completed" {
                terminals += 1;
                return Ok((
                    first_content.unwrap_or_else(|| ms(started.elapsed())),
                    ms(started.elapsed()),
                    bytes,
                    terminals,
                ));
            }
            if kind == "response.failed" || kind == "error" {
                anyhow::bail!("ws turn failed: {kind}");
            }
        }
    }
}

fn pct(values: &[f64], p: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return f64::NAN;
    }
    // f64 percentile rank mirrors the Go bench math; sample vectors are
    // far below 2^53 entries.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

/// Peak-RSS/CPU sampler over `/proc/<pid>` (Linux bench host).
pub struct ProcSampler {
    stop: Arc<AtomicBool>,
    peak_rss: Arc<AtomicU64>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ProcSampler {
    pub fn start(pid: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss = Arc::new(AtomicU64::new(0));
        let task = tokio::spawn({
            let stop = stop.clone();
            let peak_rss = peak_rss.clone();
            async move {
                let status_path = format!("/proc/{pid}/status");
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(text) = std::fs::read_to_string(&status_path) {
                        for line in text.lines() {
                            if let Some(kb) = line
                                .strip_prefix("VmRSS:")
                                .and_then(|rest| rest.trim().strip_suffix(" kB"))
                                .and_then(|n| n.trim().parse::<u64>().ok())
                            {
                                peak_rss.fetch_max(kb * 1024, Ordering::Relaxed);
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });
        Self {
            stop,
            peak_rss,
            task: Some(task),
        }
    }

    pub async fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(mut task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut task).await;
        }
        self.peak_rss.load(Ordering::Relaxed)
    }
}

/// Daemon CPU jiffies (utime + stime) from /proc.
pub fn cpu_jiffies(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // fields[0] = state (field 3); utime = field 14 -> index 11, stime 15 -> 12.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Run one sample: fixed-concurrency closed-loop load for `secs`.
// One closed-loop sample driver; the warmup/spawn/collect phases read
// as a unit. `ttfb`/`ttft` are the metric names, not typos.
#[allow(clippy::too_many_lines, clippy::similar_names)]
pub async fn run_sample(
    stack: &Stack,
    spec: &CellSpec,
    api_key: &str,
) -> anyhow::Result<(Sample, Vec<RequestRecord>)> {
    // Event-signaled warmup: sequential requests until 3 consecutive clean
    // completions (bounded), so JIT-less warm state is proven, not timed.
    let client = reqwest::Client::new();
    if spec.protocol == Protocol::Ws {
        let mut ws = WsClient::connect(&stack.base, api_key).await?;
        ws.turn().await.context("ws warmup turn")?;
    } else {
        let mut consecutive = 0;
        let mut attempts = 0;
        while consecutive < 3 && attempts < 60 {
            attempts += 1;
            let record = http_request(&client, &stack.base, api_key, spec.protocol).await;
            consecutive = if record.error.is_none() {
                consecutive + 1
            } else {
                0
            };
        }
        anyhow::ensure!(
            consecutive == 3,
            "warmup never stabilized after {attempts} attempts"
        );
    }

    let index_offset = index_jsonl(&stack.state).and_then(|p| p.metadata().ok().map(|m| m.len()));

    let sampler = ProcSampler::start(stack.daemon.pid());
    let cpu_start = cpu_jiffies(stack.daemon.pid());
    let started = Instant::now();
    let deadline = started + Duration::from_secs(spec.sample_secs);
    let records: Arc<tokio::sync::Mutex<Vec<RequestRecord>>> = Arc::default();
    let mut tasks = Vec::new();
    for _ in 0..spec.concurrency {
        let client = client.clone();
        let base = stack.base.clone();
        let key = api_key.to_string();
        let records = records.clone();
        let protocol = spec.protocol;
        tasks.push(tokio::spawn(async move {
            let mut ws = if protocol == Protocol::Ws {
                match WsClient::connect(&base, &key).await {
                    Ok(ws) => Some(ws),
                    Err(err) => {
                        records.lock().await.push(RequestRecord {
                            error: Some(format!("ws connect: {err}")),
                            ..Default::default()
                        });
                        return;
                    }
                }
            } else {
                None
            };
            while Instant::now() < deadline {
                let record = if let Some(ws) = ws.as_mut() {
                    match ws.turn().await {
                        Ok((ttft, total, bytes, terminals)) => RequestRecord {
                            ttfb_ms: Some(ttft),
                            ttft_ms: Some(ttft),
                            total_ms: total,
                            bytes,
                            error: if terminals == 1 {
                                None
                            } else {
                                Some(format!("terminal events: {terminals}"))
                            },
                            terminal_ok: terminals == 1,
                        },
                        Err(err) => {
                            records.lock().await.push(RequestRecord {
                                error: Some(format!("ws turn: {err}")),
                                ..Default::default()
                            });
                            match WsClient::connect(&base, &key).await {
                                Ok(fresh) => {
                                    *ws = fresh;
                                    continue;
                                }
                                Err(err) => {
                                    records.lock().await.push(RequestRecord {
                                        error: Some(format!("ws reconnect: {err}")),
                                        ..Default::default()
                                    });
                                    return;
                                }
                            }
                        }
                    }
                } else {
                    http_request(&client, &base, &key, protocol).await
                };
                records.lock().await.push(record);
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }
    let elapsed = started.elapsed();
    let peak_rss = sampler.stop().await;
    let cpu_end = cpu_jiffies(stack.daemon.pid());
    let records = records.lock().await.clone();

    let ok: Vec<&RequestRecord> = records.iter().filter(|r| r.error.is_none()).collect();
    let errors = records.len() as u64 - ok.len() as u64;
    let ttfb: Vec<f64> = ok.iter().filter_map(|r| r.ttfb_ms).collect();
    let ttft: Vec<f64> = ok.iter().filter_map(|r| r.ttft_ms).collect();
    let total: Vec<f64> = ok.iter().map(|r| r.total_ms).collect();
    let bytes: u64 = ok.iter().map(|r| r.bytes).sum();

    // Local overhead: debuglog stage decomposition when debug logging is
    // on (decode+transform+egress = upstream_sent + first_client -
    // first_upstream); with logging off, end-to-end latency under the
    // zero-delay loopback stub is the documented proxy.
    let overhead_values = if spec.debug {
        read_overhead(&stack.state, index_offset)
    } else {
        total.clone()
    };
    // u64/usize→f64 mirrors the Go bench math; counters stay far below
    // 2^53 so precision loss is theoretical.
    #[allow(clippy::cast_precision_loss)]
    let cpu_ms_per_request = match (cpu_start, cpu_end, ok.len()) {
        (Some(s), Some(e), n) if n > 0 => (e - s) as f64 * 10.0 / n as f64, // 100 jiffies/s
        _ => f64::NAN,
    };
    #[allow(clippy::cast_precision_loss)]
    let sample = Sample {
        requests: ok.len() as u64,
        errors,
        elapsed_secs: elapsed.as_secs_f64(),
        rps: ok.len() as f64 / elapsed.as_secs_f64(),
        ttfb_p50_ms: pct(&ttfb, 50.0),
        ttfb_p99_ms: pct(&ttfb, 99.0),
        ttft_p50_ms: pct(&ttft, 50.0),
        ttft_p99_ms: pct(&ttft, 99.0),
        total_p50_ms: pct(&total, 50.0),
        total_p99_ms: pct(&total, 99.0),
        overhead_p99_ms: pct(&overhead_values, 99.0),
        rss_peak_bytes: peak_rss,
        cpu_ms_per_request,
        bytes_per_request: if ok.is_empty() {
            f64::NAN
        } else {
            bytes as f64 / ok.len() as f64
        },
    };
    Ok((sample, records))
}

fn index_jsonl(state: &Path) -> Option<PathBuf> {
    [state.join("logs/index.jsonl"), state.join("index.jsonl")]
        .into_iter()
        .find(|candidate| candidate.is_file())
}

/// Parse per-request local overhead (ms) from index.jsonl entries appended
/// after `offset` (None = from the start).
pub fn read_overhead(state: &Path, offset: Option<u64>) -> Vec<f64> {
    let Some(path) = index_jsonl(state) else {
        return Vec::new();
    };
    let Ok(data) = std::fs::read(&path) else {
        return Vec::new();
    };
    let start = usize::try_from(
        offset
            .unwrap_or(0)
            .min(u64::try_from(data.len()).unwrap_or(u64::MAX)),
    )
    .unwrap_or(0);
    let data = &data[start..];
    let mut values = Vec::new();
    for line in data.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Ok(entry) = serde_json::from_slice::<Value>(line) else {
            continue;
        };
        let get = |key: &str| entry[key].as_i64();
        let (Some(sent), Some(first_upstream), Some(first_client)) = (
            get("upstream_sent_ms"),
            get("first_upstream_ms"),
            get("first_client_ms"),
        ) else {
            continue;
        };
        // decode + transform + egress, excluding upstream connect/TTFT.
        let overhead = sent + first_client - first_upstream;
        if overhead >= 0 {
            #[allow(clippy::cast_precision_loss)]
            values.push(overhead as f64);
        }
    }
    values
}

#[derive(Serialize)]
struct BenchReport {
    seed: u64,
    resamples: usize,
    host: Value,
    binaries: Value,
    overhead_metric: String,
    cells: Vec<Cell>,
    verdict: verdict::Verdict,
    bench_infrastructure: Value,
}

/// `qa bench` entry point.
// One sequential matrix driver; splitting the cell loop would scatter
// the ABBA pairing contract.
#[allow(clippy::too_many_lines)]
pub async fn run(go_root: &Path, evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    let run_started = Instant::now();
    let budget_deadline = run_started + RUN_BUDGET;
    let binaries = resolve_binaries(go_root, evidence)?;
    let api_key = "qa-bench-key";
    let nproc = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);

    let mut specs = matrix();
    if let Some(case) = case {
        specs.retain(|s| s.name == case || s.name.contains(case));
        anyhow::ensure!(!specs.is_empty(), "no bench cell matches --case {case}");
    }

    let mut cells: Vec<Cell> = Vec::new();
    let mut rng = SplitMix64::new(verdict::SEED);
    for spec in &specs {
        let cell_work = evidence.join(spec.name);
        std::fs::create_dir_all(&cell_work)?;
        let mut cell = Cell {
            name: spec.name.to_string(),
            primary: spec.primary,
            concurrency: spec.concurrency,
            debug_logging: spec.debug,
            go: Vec::new(),
            rust: Vec::new(),
        };
        for pair in 0..spec.samples {
            if Instant::now() >= budget_deadline {
                anyhow::bail!(
                    "bench run budget {RUN_BUDGET:?} exhausted in cell {}",
                    spec.name
                );
            }
            // Seeded ABBA: per pair, seeded coin picks which side leads.
            let go_first = rng.below(2) == 0;
            for leg in 0..2 {
                let is_go = (leg == 0) == go_first;
                let label = format!("pair{pair}-{}", if is_go { "go" } else { "rust" });
                let daemon = if is_go {
                    &binaries.go_daemon
                } else {
                    &binaries.rust_daemon
                };
                let stack = launch(
                    &cell_work,
                    &label,
                    daemon,
                    &binaries.go_stub,
                    &spec.shape,
                    spec.debug,
                    api_key,
                )
                .await?;
                let state = stack.state.clone();
                let result = run_sample(&stack, spec, api_key).await;
                stack.stop().await;
                let (sample, records) = result?;
                let errors: std::collections::BTreeMap<String, usize> = records
                    .iter()
                    .filter_map(|r| r.error.clone())
                    .fold(std::collections::BTreeMap::default(), |mut map, e| {
                        *map.entry(e).or_insert(0) += 1;
                        map
                    });
                std::fs::write(
                    cell_work.join(format!("{label}.json")),
                    serde_json::to_string_pretty(&json!({
                        "leg": if is_go { "go" } else { "rust" },
                        "pair": pair,
                        "abba_go_first": go_first,
                        "sample": sample,
                        "error_kinds": errors,
                    }))?,
                )?;
                if spec.debug {
                    // Preserve the per-request overhead values backing the p99.
                    let values = read_overhead(&state, None);
                    std::fs::write(
                        cell_work.join(format!("{label}-overhead.json")),
                        serde_json::to_string(&values)?,
                    )?;
                }
                // Bound disk: per-sample daemon state can hold thousands of
                // request dirs; raw metrics are already extracted above.
                let _ = std::fs::remove_dir_all(&state);
                if is_go {
                    cell.go.push(sample);
                } else {
                    cell.rust.push(sample);
                }
            }
        }
        cells.push(cell);
    }

    let verdict = verdict::evaluate(&cells);
    let report = BenchReport {
        seed: verdict::SEED,
        resamples: verdict::RESAMPLES,
        host: json!({
            "cores": nproc,
            "go_runtime": "GOMAXPROCS=4",
            "rust_runtime": "tokio multi_thread (4 cores)",
            "affinity": "all 4 host cores (4-core host: no spare core set for isolation; legs run sequentially)",
        }),
        binaries: json!({
            "go_daemon_sha256": binaries.go_daemon_sha256,
            "go_stub_sha256": binaries.go_stub_sha256,
            "rust_daemon_sha256": binaries.rust_sha256,
            "go_source": binaries.go_source,
            "stub": "same Go upstreamstub binary drives both legs",
        }),
        overhead_metric: "debug on: debuglog decode+transform+egress (upstream_sent + first_client - first_upstream); debug off: end-to-end latency p99 under zero-delay loopback stub".into(),
        cells,
        bench_infrastructure: json!({
            "harness_wall_secs": run_started.elapsed().as_secs_f64(),
            "note": "harness runs one leg at a time; harness cost (process spawn, warmup, sampling) is outside the 30s/10s measurement windows",
        }),
        verdict,
    };
    std::fs::write(
        evidence.join("bench-report.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_string_pretty(&json!({
            "subcommand": "bench",
            "cells": report.cells.len(),
            "primary_cells": report.cells.iter().filter(|c| c.primary).count(),
            "passed": report.verdict.passed,
            "gates": report.verdict.gates,
            "report": "bench-report.json",
        }))?,
    )?;
    for gate in &report.verdict.gates {
        println!(
            "gate {}: {}",
            gate.name,
            if gate.passed { "PASS" } else { "FAIL" }
        );
        println!("  {}", gate.detail);
    }
    println!(
        "qa bench: {} ({} cells, report {})",
        if report.verdict.passed {
            "PASS"
        } else {
            "FAIL"
        },
        report.cells.len(),
        evidence.join("bench-report.json").display()
    );
    Ok(i32::from(!report.verdict.passed))
}

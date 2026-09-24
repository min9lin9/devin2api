//! Task 6 transport interop: the reqwest-backed `ClientTransport` against a
//! real loopback Go Connect server (connect-go + the same generated
//! bindings the reference daemon uses), plus `HTTP`/SOCKS5 proxy stubs and a
//! TLS certificate check.
//!
//! The Go oracle source lives in `tests/fixtures/task6/oracle/` and is
//! built once per test process into a per-test work dir — never inside G.

mod support;

use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use connectrpc::ErrorCode;
use connectrpc::client::CallOptions;
use devin_proto::generated::exa::api_server_pb::{
    ApiServerServiceClient, GetChatMessageRequest, GetStatusRequest,
};
use devin2api::qa::process::{self, ManagedChild};
use devin2api::upstream::transport::{
    SeatClient, SeatError, TransportConfig, UpstreamTransport, build_http_client, build_metadata,
    upstream_clients,
};

const ORACLE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/task6/oracle");
/// Evidence root for this task — Go oracle binaries are built with `-o`
/// under E per the workspace rules (never inside G, never in /tmp).
const EVIDENCE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../.omo/evidence/devin2api-rust-parity/task-6"
);
const STARTUP: Duration = Duration::from_secs(15);
const STATS_TIMEOUT: Duration = Duration::from_secs(10);

/// Test-only hostname the proxy stubs map to 127.0.0.1 — lets env-proxy
/// cases exercise a non-localhost target (Go bypasses proxies for
/// localhost/loopback, so a loopback `URL` would never reach the proxy).
const PROXIED_HOST: &str = "test.invalid";

// ---------------------------------------------------------------------------
// Go oracle lifecycle
// ---------------------------------------------------------------------------

static ORACLE_BIN: OnceLock<Option<PathBuf>> = OnceLock::new();

fn go_bin() -> PathBuf {
    if let Ok(bin) = std::env::var("QA_GO_BIN") {
        return PathBuf::from(bin);
    }
    let sdk = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("sdk/go/bin/go");
    if sdk.is_file() {
        return sdk;
    }
    PathBuf::from("go")
}

/// Build the Go oracle once per test process into the task-6 evidence
/// dir. Returns `None` (with a SKIP note) when the Go toolchain is
/// unavailable.
fn oracle_bin() -> Option<PathBuf> {
    ORACLE_BIN
        .get_or_init(|| {
            let go = go_bin();
            let ok = std::process::Command::new(&go)
                .arg("version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|s| s.success());
            if !ok {
                eprintln!("SKIP: Go toolchain unavailable for transport oracle");
                return None;
            }
            let bin_dir = PathBuf::from(EVIDENCE_DIR).join("oracle");
            std::fs::create_dir_all(&bin_dir).expect("create oracle bin dir");
            let out = bin_dir.join("task6-oracle");
            let status = std::process::Command::new(&go)
                .args(["build", "-o"])
                .arg(&out)
                .arg(".")
                .current_dir(ORACLE_DIR)
                .env("GOFLAGS", "-mod=readonly")
                .env("GOTOOLCHAIN", "local")
                .status()
                .expect("spawn go build");
            if !status.success() {
                eprintln!("SKIP: go build of task6 oracle failed ({status})");
                return None;
            }
            Some(out)
        })
        .clone()
}

struct Oracle {
    child: ManagedChild,
    port: u16,
    work: PathBuf,
}

/// Unique suffix per oracle — tests run in parallel and `work_dir` wipes
/// its target, so same-tag dirs would race (a second TLS oracle's setup
/// deletes the first's `cert.pem` while it is still serving).
static ORACLE_SEQ: AtomicU64 = AtomicU64::new(0);

impl Oracle {
    async fn start(mode: &str) -> Option<Self> {
        let seq = ORACLE_SEQ.fetch_add(1, Ordering::SeqCst);
        let work = support::work_dir(&format!("t6-oracle-{mode}-{seq}"));
        let bin = oracle_bin()?;
        let port = process::free_port().ok()?;
        let mut cmd = std::process::Command::new(bin);
        cmd.arg("-listen")
            .arg(format!("127.0.0.1:{port}"))
            .arg("-mode")
            .arg(mode)
            .arg("-seat-token")
            .arg("qa-seat-token");
        if mode == "tls" {
            cmd.arg("-cert-out").arg(work.join("cert.pem"));
        }
        let mut child = process::spawn_logged(&work, "oracle", &mut cmd).ok()?;
        process::wait_tcp(&mut child, port, STARTUP).await.ok()?;
        Some(Self { child, port, work })
    }

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn tls_base(&self) -> String {
        format!("https://localhost:{}", self.port)
    }

    /// The oracle writes cert.pem before it binds, so once the port is
    /// up the file is on its way — but a stale listener can satisfy
    /// `wait_tcp` early, so poll briefly for a complete `PEM`.
    async fn cert_pem(&self) -> Vec<u8> {
        let path = self.work.join("cert.pem");
        let deadline = Instant::now() + STARTUP;
        loop {
            match std::fs::read(&path) {
                Ok(pem) if pem.ends_with(b"-----END CERTIFICATE-----\n") => {
                    return pem;
                }
                _ if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                _ => panic!("oracle cert.pem missing or incomplete"),
            }
        }
    }

    /// Read the oracle's /stats counters with a throwaway no-proxy client
    /// (each call costs exactly one server-side connection — tests account
    /// for it).
    async fn stats(&self) -> serde_json::Value {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        client
            .get(format!("{}/stats", self.base()))
            .send()
            .await
            .expect("stats request")
            .json()
            .await
            .expect("stats json")
    }

    /// `Poll` /stats until `key` reaches `want` or the bounded wait expires.
    async fn wait_stat(&self, key: &str, want: u64) -> u64 {
        let deadline = Instant::now() + STATS_TIMEOUT;
        loop {
            let got = self
                .stats()
                .await
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            if got >= want || Instant::now() >= deadline {
                return got;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn shutdown(mut self) {
        let _ = self.child.shutdown(Duration::from_secs(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Client helpers
// ---------------------------------------------------------------------------

fn token_source(
    initial: &str,
) -> (
    Arc<Mutex<String>>,
    devin2api::upstream::transport::TokenSource,
) {
    let cell = Arc::new(Mutex::new(initial.to_string()));
    let src = {
        let cell = cell.clone();
        Arc::new(move || cell.lock().unwrap().clone())
            as devin2api::upstream::transport::TokenSource
    };
    (cell, src)
}

fn config_for(base: &str) -> TransportConfig {
    TransportConfig {
        base_url: base.to_string(),
        proxy: String::new(),
        force_http1: true,
        extra_root_pems: Vec::new(),
    }
}

fn status_request(token: &str) -> GetStatusRequest {
    GetStatusRequest {
        metadata: build_metadata(token, "windsurf", "1.48.2", "win", 32).into(),
        ..Default::default()
    }
}

fn chat_request(token: &str) -> GetChatMessageRequest {
    GetChatMessageRequest {
        metadata: build_metadata(token, "windsurf", "1.48.2", "win", 32).into(),
        prompt: Some("hello".into()),
        ..Default::default()
    }
}

fn header<'a>(headers: &'a http::HeaderMap, name: &str) -> &'a str {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
}

/// Assert the echo headers every oracle handler emits.
fn assert_wire_headers(headers: &http::HeaderMap, want_auth: &str) {
    assert_eq!(
        header(headers, "x-seen-authorization"),
        want_auth,
        "authorization"
    );
    assert_eq!(
        header(headers, "x-seen-user-agent-present"),
        "false",
        "User-Agent must be absent on the wire"
    );
    assert_eq!(
        header(headers, "x-seen-connect-protocol-version"),
        "1",
        "Connect-Protocol-Version"
    );
}

// ---------------------------------------------------------------------------
// Proxy stubs
// ---------------------------------------------------------------------------

/// Resolve a stub-dialed host: anything that is not directly resolvable
/// maps to 127.0.0.1, so `test.invalid` targets reach the loopback oracle.
fn stub_dial_addr(host: &str, port: u16) -> String {
    if format!("{host}:{port}").to_socket_addrs().is_ok() {
        format!("{host}:{port}")
    } else {
        format!("127.0.0.1:{port}")
    }
}

struct HttpProxy {
    port: u16,
    requests: Arc<AtomicU64>,
    /// First request-line targets seen, e.g. `POST http://host/path`.
    targets: Arc<Mutex<Vec<String>>>,
    /// Full request heads (request line + headers) for header assertions.
    heads: Arc<Mutex<Vec<String>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpProxy {
    async fn start() -> Self {
        Self::start_inner(false).await
    }

    async fn dead() -> Self {
        Self::start_inner(true).await
    }

    async fn start_inner(dead: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(AtomicU64::new(0));
        let targets = Arc::new(Mutex::new(Vec::new()));
        let heads = Arc::new(Mutex::new(Vec::new()));
        let (reqs, tgts, hds) = (requests.clone(), targets.clone(), heads.clone());
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let (reqs, tgts, hds) = (reqs.clone(), tgts.clone(), hds.clone());
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    if dead {
                        return; // drop: dead proxy
                    }
                    reqs.fetch_add(1, Ordering::SeqCst);
                    // Read the request head (up to \r\n\r\n), parse the
                    // request line, then relay verbatim — a TCP-level
                    // forward proxy that preserves headers/body/trailers.
                    let mut head = Vec::with_capacity(1024);
                    let mut byte = [0u8; 1];
                    loop {
                        match sock.read(&mut byte).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {
                                head.push(byte[0]);
                                if head.ends_with(b"\r\n\r\n") || head.len() > 64 * 1024 {
                                    break;
                                }
                            }
                        }
                    }
                    let head_text = String::from_utf8_lossy(&head).to_string();
                    hds.lock().unwrap().push(head_text.clone());
                    let request_line = head_text.lines().next().unwrap_or("").to_string();
                    tgts.lock().unwrap().push(request_line.clone());
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("");
                    let target = parts.next().unwrap_or("");
                    if method.eq_ignore_ascii_case("connect") {
                        // CONNECT host:port — tunnel after 200.
                        let (host, port) = target
                            .rsplit_once(':')
                            .map_or((target, 443), |(h, p)| (h, p.parse().unwrap_or(443)));
                        let dial = stub_dial_addr(host, port);
                        let Ok(mut upstream) = tokio::net::TcpStream::connect(&dial).await else {
                            let _ = sock.write_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await;
                            return;
                        };
                        if sock
                            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let _ = tokio::io::copy_bidirectional(&mut sock, &mut upstream).await;
                    } else {
                        // Absolute-form: http://host[:port]/path
                        let hostport = target
                            .strip_prefix("http://")
                            .and_then(|rest| rest.split('/').next())
                            .unwrap_or("");
                        let (host, port) = hostport
                            .rsplit_once(':')
                            .map_or((hostport, 80), |(h, p)| (h, p.parse().unwrap_or(80)));
                        let dial = stub_dial_addr(host, port);
                        let Ok(mut upstream) = tokio::net::TcpStream::connect(&dial).await else {
                            return;
                        };
                        if upstream.write_all(&head).await.is_err() {
                            return;
                        }
                        let _ = tokio::io::copy_bidirectional(&mut sock, &mut upstream).await;
                    }
                });
            }
        });
        Self {
            port,
            requests,
            targets,
            heads,
            task: Some(task),
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn count(&self) -> u64 {
        self.requests.load(Ordering::SeqCst)
    }

    fn first_target(&self) -> String {
        self.targets
            .lock()
            .unwrap()
            .first()
            .cloned()
            .unwrap_or_default()
    }

    fn first_head(&self) -> String {
        self.heads
            .lock()
            .unwrap()
            .first()
            .cloned()
            .unwrap_or_default()
    }
}

impl Drop for HttpProxy {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

/// Recorded SOCKS5 connection request.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SocksTarget {
    /// ATYP byte: 0x01 IPv4, 0x03 domain, 0x04 IPv6.
    atyp: u8,
    /// Address as sent by the client (dotted IPv4 or domain string).
    addr: String,
    port: u16,
}

struct Socks5Proxy {
    port: u16,
    seen: Arc<Mutex<Vec<SocksTarget>>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Socks5Proxy {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let seen = seen2.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    // Greeting: VER NMETHODS METHODS — accept no-auth only.
                    let mut buf = [0u8; 2];
                    if sock.read_exact(&mut buf).await.is_err() || buf[0] != 0x05 {
                        return;
                    }
                    let mut methods = vec![0u8; buf[1] as usize];
                    if sock.read_exact(&mut methods).await.is_err() {
                        return;
                    }
                    if sock.write_all(&[0x05, 0x00]).await.is_err() {
                        return;
                    }
                    // Request: VER CMD RSV ATYP DST.ADDR DST.PORT
                    let mut hdr = [0u8; 4];
                    if sock.read_exact(&mut hdr).await.is_err() || hdr[0] != 0x05 {
                        return;
                    }
                    let atyp = hdr[3];
                    let addr = match atyp {
                        0x01 => {
                            let mut a = [0u8; 4];
                            if sock.read_exact(&mut a).await.is_err() {
                                return;
                            }
                            format!("{}.{}.{}.{}", a[0], a[1], a[2], a[3])
                        }
                        0x03 => {
                            let mut len = [0u8; 1];
                            if sock.read_exact(&mut len).await.is_err() {
                                return;
                            }
                            let mut name = vec![0u8; len[0] as usize];
                            if sock.read_exact(&mut name).await.is_err() {
                                return;
                            }
                            String::from_utf8_lossy(&name).to_string()
                        }
                        0x04 => {
                            let mut a = [0u8; 16];
                            if sock.read_exact(&mut a).await.is_err() {
                                return;
                            }
                            format!("{:?}", std::net::Ipv6Addr::from(a))
                        }
                        _ => return,
                    };
                    let mut pbuf = [0u8; 2];
                    if sock.read_exact(&mut pbuf).await.is_err() {
                        return;
                    }
                    let dport = u16::from_be_bytes(pbuf);
                    seen.lock().unwrap().push(SocksTarget {
                        atyp,
                        addr: addr.clone(),
                        port: dport,
                    });
                    // Dial the requested target; unresolvable names map to
                    // the loopback oracle like the HTTP stub.
                    let dial = stub_dial_addr(&addr, dport);
                    let Ok(mut upstream) = tokio::net::TcpStream::connect(&dial).await else {
                        // REP=5 connection refused.
                        let _ = sock
                            .write_all(&[0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                            .await;
                        return;
                    };
                    // REP=0 success, BND.ADDR 0.0.0.0:0.
                    if sock
                        .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
                        .await
                        .is_err()
                    {
                        return;
                    }
                    let _ = tokio::io::copy_bidirectional(&mut sock, &mut upstream).await;
                });
            }
        });
        Self {
            port,
            seen,
            task: Some(task),
        }
    }

    fn url(&self, scheme: &str) -> String {
        format!("{scheme}://127.0.0.1:{}", self.port)
    }

    fn first(&self) -> Option<SocksTarget> {
        self.seen.lock().unwrap().first().cloned()
    }

    fn last(&self) -> Option<SocksTarget> {
        self.seen.lock().unwrap().last().cloned()
    }

    fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

impl Drop for Socks5Proxy {
    fn drop(&mut self) {
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Unary + server-streaming over forced `HTTP`/1.1: headers, Basic `auth`,
/// omitted User-Agent, message decode, end-stream trailers, keep-alive
/// connection reuse.
#[tokio::test]
async fn unary_and_stream_h1_headers_trailers_reuse() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (_cell, token) = token_source("qa-token-1");
    let clients = upstream_clients(&config_for(&oracle.base()), token).unwrap();

    // Unary call.
    let resp = clients
        .api
        .get_status(status_request("qa-token-1"))
        .await
        .expect("get_status");
    assert_wire_headers(resp.headers(), "Basic qa-token-1-qa-token-1");
    assert_eq!(
        header(resp.headers(), "x-seen-content-type"),
        "application/proto"
    );
    // Unary: connect-go leaves Accept-Encoding to the transport, so Go's
    // http.Transport (DisableCompression=false) sends `gzip` — reqwest's
    // gzip feature fills the same vacant header.
    assert_eq!(
        header(resp.headers(), "x-seen-accept-encoding"),
        "gzip",
        "unary Accept-Encoding must be the transport's implicit gzip"
    );
    let owned = resp.into_owned();
    assert_eq!(owned.show_review_prompt, Some(true));

    // Server-streaming call.
    let mut stream = clients
        .stream
        .get_chat_message(chat_request("qa-token-1"))
        .await
        .expect("get_chat_message");
    assert_wire_headers(stream.headers(), "Basic qa-token-1-qa-token-1");
    assert_eq!(
        header(stream.headers(), "x-seen-content-type"),
        "application/connect+proto"
    );
    // Streaming: connect-go pins `Accept-Encoding: identity`
    // (protocol_connect.go WriteRequestHeader, streamType != unary).
    assert_eq!(
        header(stream.headers(), "x-seen-accept-encoding"),
        "identity",
        "streaming Accept-Encoding must be pinned to identity"
    );

    let mut deltas = Vec::new();
    let mut saw_stop = false;
    loop {
        match stream.message().await {
            Ok(Some(msg)) => {
                let v = msg.view();
                if let Some(t) = v.delta_text {
                    deltas.push(t.to_string());
                }
                if v.stop_reason.is_some() {
                    saw_stop = true;
                }
            }
            Ok(None) => break,
            Err(e) => panic!("stream error: {e}"),
        }
    }
    assert_eq!(
        deltas,
        vec!["stub: hello ".to_string(), "world".to_string()]
    );
    assert!(saw_stop, "expected a stop_reason frame");
    // Connect end-stream metadata surfaces as trailers.
    let trailers = stream.trailers().expect("end-stream metadata");
    assert_eq!(header(trailers, "x-stub-trailer"), "t1");

    // Keep-alive: the unary and streaming RPCs above must have shared
    // one TCP connection. The conns counter also counts the wait_tcp
    // probe and every /stats poll conn, so compare deltas — each stats
    // read adds exactly one conn for itself. Re-measure with a third
    // RPC after dropping the stream so its conn returns to the pool:
    // a reused pooled conn means zero new conns for the RPC.
    drop(stream);
    let before = oracle
        .stats()
        .await
        .get("conns")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    clients
        .api
        .get_status(status_request("qa-token-1"))
        .await
        .expect("third RPC on same client");
    let after = oracle
        .stats()
        .await
        .get("conns")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    // after = before + new_rpc_conns + 1 (this stats call's own conn).
    let new_rpc_conns = after.saturating_sub(before).saturating_sub(1);
    assert_eq!(
        new_rpc_conns, 0,
        "third RPC must reuse the pooled keep-alive conn"
    );

    oracle.shutdown().await;
}

/// The token source is re-evaluated per send (credential repair needs no
/// transport rebuild), and a pre-set Authorization (Seat Bearer shape)
/// passes through untouched.
#[tokio::test]
async fn per_attempt_token_refresh_and_bearer_passthrough() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (cell, token) = token_source("old-token");
    let clients = upstream_clients(&config_for(&oracle.base()), token).unwrap();

    let resp = clients
        .api
        .get_status(status_request("old-token"))
        .await
        .unwrap();
    assert_eq!(
        header(resp.headers(), "x-seen-authorization"),
        "Basic old-token-old-token"
    );

    *cell.lock().unwrap() = "new-token".to_string();
    let resp = clients
        .api
        .get_status(status_request("new-token"))
        .await
        .unwrap();
    assert_eq!(
        header(resp.headers(), "x-seen-authorization"),
        "Basic new-token-new-token"
    );

    // Explicit Bearer is preserved, not overwritten by Basic.
    let resp = clients
        .api
        .get_status_with_options(
            status_request("new-token"),
            CallOptions::default().with_header("authorization", "Bearer seat-token"),
        )
        .await
        .unwrap();
    assert_eq!(
        header(resp.headers(), "x-seen-authorization"),
        "Bearer seat-token"
    );

    oracle.shutdown().await;
}

/// Seat `JSON` Connect call: Bearer `auth`, Connect-Protocol-Version, `JSON`
/// body, shared proxy-capable client.
#[tokio::test]
async fn seat_get_user_status_json_connect() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (_cell, token) = token_source("qa-seat-token");
    let client = build_http_client(&config_for(&oracle.base())).unwrap();
    let seat = SeatClient::new(client, &oracle.base(), token);

    let status = seat.get_user_status().await.expect("get_user_status");
    assert_eq!(status["userId"], "u-stub");
    assert_eq!(status["pro"], true);
    assert_eq!(status["planStatus"]["availablePromptCredits"], 42);

    // Wrong token → 401 surfaces as SeatError::Http, not a transport error.
    let (_cell2, bad) = token_source("wrong-token");
    let client = build_http_client(&config_for(&oracle.base())).unwrap();
    let seat_bad = SeatClient::new(client, &oracle.base(), bad);
    match seat_bad.get_user_status().await {
        Err(SeatError::Http(401, _)) => {}
        other => panic!("expected SeatError::Http(401), got {other:?}"),
    }

    oracle.shutdown().await;
}

/// `HTTP`/2 over TLS with a trusted self-signed oracle cert: ALPN negotiates
/// h2 for both unary and streaming; forcing `HTTP`/1.1 negotiates http/1.1.
#[tokio::test]
async fn http2_over_tls_and_force_http1() {
    let Some(oracle) = Oracle::start("tls").await else {
        return;
    };
    let pem = oracle.cert_pem().await;
    let mut cfg = config_for(&oracle.tls_base());
    cfg.extra_root_pems = vec![pem];
    cfg.force_http1 = false; // ALPN: h2 preferred

    let (_cell, token) = token_source("qa-tls-token");
    let clients = upstream_clients(&cfg, token).unwrap();

    let resp = clients
        .api
        .get_status(status_request("qa-tls-token"))
        .await
        .expect("h2 unary");
    assert_eq!(
        header(resp.headers(), "x-seen-http-version"),
        "HTTP/2.0",
        "ALPN should negotiate h2"
    );
    assert_wire_headers(resp.headers(), "Basic qa-tls-token-qa-tls-token");

    let mut stream = clients
        .stream
        .get_chat_message(chat_request("qa-tls-token"))
        .await
        .expect("h2 stream");
    assert_eq!(header(stream.headers(), "x-seen-http-version"), "HTTP/2.0");
    let mut n = 0;
    while let Ok(Some(_)) = stream.message().await {
        n += 1;
    }
    assert_eq!(n, 4, "meta + 2 deltas + stop");

    // Same TLS endpoint with force_http1 → http/1.1.
    let mut cfg1 = cfg.clone();
    cfg1.force_http1 = true;
    let (_c2, token2) = token_source("qa-tls-token");
    let clients1 = upstream_clients(&cfg1, token2).unwrap();
    let resp = clients1
        .api
        .get_status(status_request("qa-tls-token"))
        .await
        .expect("h1-over-tls unary");
    assert_eq!(
        header(resp.headers(), "x-seen-http-version"),
        "HTTP/1.1",
        "force_http1 must pin ALPN to http/1.1"
    );

    oracle.shutdown().await;
}

/// Gzip in both directions: the client advertises gzip, the Go server
/// compresses the response, and request compression is honored.
#[tokio::test]
async fn gzip_request_and_response_compression() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (_cell, token) = token_source("qa-gzip-token");
    let client = build_http_client(&config_for(&oracle.base())).unwrap();
    let uri: http::Uri = oracle.base().parse().unwrap();
    let config = connectrpc::client::ClientConfig::new(uri).compress_requests("gzip");
    let api = ApiServerServiceClient::new(
        UpstreamTransport::unary(client.clone(), token.clone()),
        config.clone(),
    );
    let stream_client =
        ApiServerServiceClient::new(UpstreamTransport::streaming(client, token), config);

    // Unary: request Content-Encoding + response Content-Encoding.
    let resp = api
        .get_status_with_options(
            status_request("qa-gzip-token"),
            CallOptions::default().with_compress(true),
        )
        .await
        .expect("gzip unary");
    assert_eq!(
        header(resp.headers(), "x-seen-content-encoding"),
        "gzip",
        "server should see a gzipped unary request body"
    );
    // The server gzipped the response, but the transport decodes it
    // transparently and strips content-encoding/content-length — the
    // same view Go's http.Transport gives connect-go when it added the
    // Accept-Encoding itself. The advertised value is the connectrpc
    // registry's list (gzip+zstd registered by `compress_requests`),
    // which the transport leaves untouched.
    assert_eq!(
        header(resp.headers(), "x-seen-accept-encoding"),
        "gzip, zstd",
        "server should see the registry's advertised encodings"
    );
    assert_eq!(
        header(resp.headers(), "content-encoding"),
        "",
        "transparent gzip decode strips content-encoding"
    );
    assert_eq!(resp.into_owned().show_review_prompt, Some(true));

    // Streaming: Connect-Content-Encoding on request and response.
    let mut stream = stream_client
        .get_chat_message_with_options(
            chat_request("qa-gzip-token"),
            CallOptions::default().with_compress(true),
        )
        .await
        .expect("gzip stream");
    assert_eq!(
        header(stream.headers(), "x-seen-connect-content-encoding"),
        "gzip"
    );
    // Streaming still pins Accept-Encoding: identity even when Connect
    // compression is negotiated (connect-go overwrites it).
    assert_eq!(
        header(stream.headers(), "x-seen-accept-encoding"),
        "identity"
    );
    assert_eq!(
        header(stream.headers(), "connect-content-encoding"),
        "gzip",
        "server should gzip stream envelopes"
    );
    let mut n = 0;
    while let Ok(Some(_)) = stream.message().await {
        n += 1;
    }
    assert_eq!(n, 4);

    oracle.shutdown().await;
}

/// Explicit `HTTP` forward proxy: absolute-form requests relayed verbatim,
/// headers preserved end-to-end; proxy userinfo becomes
/// Proxy-Authorization like Go's http.ProxyURL.
#[tokio::test]
async fn explicit_http_proxy_forwards_absolute_form() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let proxy = HttpProxy::start().await;
    let mut cfg = config_for(&oracle.base());
    cfg.proxy = proxy.url();

    let (_cell, token) = token_source("qa-proxy-token");
    let clients = upstream_clients(&cfg, token).unwrap();
    let resp = clients
        .api
        .get_status(status_request("qa-proxy-token"))
        .await
        .expect("proxied unary");
    assert_wire_headers(resp.headers(), "Basic qa-proxy-token-qa-proxy-token");

    assert!(proxy.count() >= 1, "proxy saw no requests");
    let target = proxy.first_target();
    assert!(
        target.contains(&format!("http://127.0.0.1:{}/", oracle.port)),
        "expected absolute-form target, got {target:?}"
    );

    // Proxy auth: userinfo in the proxy URL → Proxy-Authorization header.
    let proxy2 = HttpProxy::start().await;
    let mut cfg2 = config_for(&oracle.base());
    cfg2.proxy = format!("http://user:pass@127.0.0.1:{}", proxy2.port);
    let (_c2, t2) = token_source("qa-proxy-token");
    let clients2 = upstream_clients(&cfg2, t2).unwrap();
    clients2
        .api
        .get_status(status_request("qa-proxy-token"))
        .await
        .expect("authed proxied unary");
    let head = proxy2.first_head();
    assert!(
        head.to_ascii_lowercase()
            .contains("proxy-authorization: basic dxnlcjpwyxnz"),
        "expected Proxy-Authorization in request head: {head:?}"
    );

    oracle.shutdown().await;
}

/// Explicit `HTTP` proxy also tunnels https via CONNECT.
#[tokio::test]
async fn explicit_http_proxy_connect_tls() {
    let Some(oracle) = Oracle::start("tls").await else {
        return;
    };
    let proxy = HttpProxy::start().await;
    let mut cfg = config_for(&oracle.tls_base());
    cfg.extra_root_pems = vec![oracle.cert_pem().await];
    cfg.proxy = proxy.url();

    let (_cell, token) = token_source("qa-proxy-tls");
    let clients = upstream_clients(&cfg, token).unwrap();
    let resp = clients
        .api
        .get_status(status_request("qa-proxy-tls"))
        .await
        .expect("CONNECT-tunneled unary");
    assert_wire_headers(resp.headers(), "Basic qa-proxy-tls-qa-proxy-tls");
    assert!(
        proxy.first_target().starts_with("CONNECT "),
        "expected CONNECT tunnel, got {:?}",
        proxy.first_target()
    );

    oracle.shutdown().await;
}

/// SOCKS5 `DNS` behavior: Go's dialer always sends the hostname to the
/// proxy (remote `DNS`) — `socks5` is treated the same as `socks5h`, so
/// both must arrive as ATYP=domain, never a locally-resolved IPv4.
#[tokio::test]
async fn socks5_and_socks5h_remote_dns() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let socks = Socks5Proxy::start().await;

    // socks5h: hostname forwarded to the proxy.
    let mut cfg = config_for(&format!("http://localhost:{}", oracle.port));
    cfg.proxy = socks.url("socks5h");
    let (_c1, t1) = token_source("qa-socks-token");
    let clients = upstream_clients(&cfg, t1).unwrap();
    clients
        .api
        .get_status(status_request("qa-socks-token"))
        .await
        .expect("socks5h unary");
    let first = socks.first().expect("socks5h request");
    assert_eq!(first.atyp, 0x03, "socks5h must forward the domain name");
    assert_eq!(first.addr, "localhost");
    assert_eq!(first.port, oracle.port);

    // socks5: Go treats it identically — the hostname is still forwarded
    // (x/net socks dialer sends ATYP=domain for non-IP hosts).
    let mut cfg = config_for(&format!("http://localhost:{}", oracle.port));
    cfg.proxy = socks.url("socks5");
    let (_c2, t2) = token_source("qa-socks-token");
    let clients = upstream_clients(&cfg, t2).unwrap();
    clients
        .api
        .get_status(status_request("qa-socks-token"))
        .await
        .expect("socks5 unary");
    let last = socks.last().expect("socks5 request");
    assert_eq!(
        last.atyp, 0x03,
        "Go treats socks5 like socks5h: hostname forwarded, not resolved"
    );
    assert_eq!(last.addr, "localhost");

    oracle.shutdown().await;
}

/// Environment proxy precedence — parent driver. Each case runs in a
/// spawned copy of this test binary (`std::env::set_var` is unsafe and
/// forbidden workspace-wide; reqwest reads env at client build anyway).
/// Cases assert Go's `ProxyFromEnvironment` semantics: `HTTP_PROXY`/`HTTPS_PROXY`
/// honored, `ALL_PROXY` ignored, localhost bypass, explicit proxy wins,
/// env socks5 = remote `DNS`, `NO_PROXY` matching.
// One sequential proxy-matrix scenario; splitting would scatter it.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_proxy_precedence() {
    if let Ok(case) = std::env::var("T6_ENV_CASE") {
        env_proxy_child(&case).await;
        return;
    }
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let Some(tls_oracle) = Oracle::start("tls").await else {
        oracle.shutdown().await;
        return;
    };
    let http_proxy = HttpProxy::start().await;
    let explicit = HttpProxy::start().await;
    let socks = Socks5Proxy::start().await;
    let exe = std::env::current_exe().unwrap();
    let cert_path = tls_oracle.work.join("cert.pem").display().to_string();

    let run_case = |case: &str, extra_env: Vec<(&str, String)>| {
        let mut cmd = tokio::process::Command::new(exe.clone());
        cmd.args([
            "env_proxy_precedence",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("T6_ENV_CASE", case)
        .env("T6_ORACLE_PORT", oracle.port.to_string())
        .env("T6_TLS_PORT", tls_oracle.port.to_string())
        .env("T6_EXPLICIT_PROXY_PORT", explicit.port.to_string())
        .env("T6_TLS_CERT", &cert_path)
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .env_remove("REQUEST_METHOD");
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        async move { cmd.status().await.expect("spawn env-proxy child") }
    };

    // Case 1: HTTP_PROXY (bare host:port form) is honored for a
    // non-localhost http target.
    let st = run_case(
        "http_env",
        vec![("HTTP_PROXY", format!("127.0.0.1:{}", http_proxy.port))],
    )
    .await;
    assert!(st.success(), "http_env child failed: {st}");
    assert!(http_proxy.count() >= 1, "env HTTP_PROXY was not used");

    // Case 2: HTTPS_PROXY is honored for https targets (CONNECT tunnel).
    let before = http_proxy.count();
    let st = run_case("https_env", vec![("HTTPS_PROXY", http_proxy.url())]).await;
    assert!(st.success(), "https_env child failed: {st}");
    assert!(http_proxy.count() > before, "env HTTPS_PROXY was not used");
    assert!(
        http_proxy
            .targets
            .lock()
            .unwrap()
            .iter()
            .any(|t| t.starts_with("CONNECT ")),
        "expected CONNECT for https target"
    );

    // Case 3: ALL_PROXY is ignored entirely (Go reads only HTTP(S)_PROXY).
    let socks_before = socks.count();
    let st = run_case(
        "all_proxy_ignored",
        vec![("ALL_PROXY", socks.url("socks5h"))],
    )
    .await;
    assert!(st.success(), "all_proxy_ignored child failed: {st}");
    assert_eq!(
        socks.count(),
        socks_before,
        "ALL_PROXY must be ignored like Go"
    );

    // Case 4: localhost bypasses env proxies unconditionally.
    let before = http_proxy.count();
    let st = run_case("localhost_bypass", vec![("HTTP_PROXY", http_proxy.url())]).await;
    assert!(st.success(), "localhost_bypass child failed: {st}");
    assert_eq!(
        http_proxy.count(),
        before,
        "localhost must bypass env proxies"
    );

    // Case 5: explicit proxy beats the environment.
    let before = http_proxy.count();
    let st = run_case(
        "explicit_wins",
        vec![
            ("HTTP_PROXY", http_proxy.url()),
            ("ALL_PROXY", socks.url("socks5h")),
        ],
    )
    .await;
    assert!(st.success(), "explicit_wins child failed: {st}");
    assert_eq!(http_proxy.count(), before, "env proxy must not be used");
    assert!(explicit.count() >= 1, "explicit proxy was not used");

    // Case 6: env socks5 means remote DNS (Go maps it to socks5h).
    let st = run_case(
        "socks_env_remote_dns",
        vec![("HTTP_PROXY", socks.url("socks5"))],
    )
    .await;
    assert!(st.success(), "socks_env_remote_dns child failed: {st}");
    let last = socks.last().expect("env socks5 request");
    assert_eq!(
        last.atyp, 0x03,
        "env socks5 must forward the hostname (remote DNS)"
    );
    assert_eq!(last.addr, PROXIED_HOST);

    // Case 7: NO_PROXY excludes the target → direct dial fails DNS.
    let before = http_proxy.count();
    let st = run_case(
        "no_proxy_bypass",
        vec![
            ("HTTP_PROXY", http_proxy.url()),
            ("NO_PROXY", PROXIED_HOST.to_string()),
        ],
    )
    .await;
    assert!(st.success(), "no_proxy_bypass child failed: {st}");
    assert_eq!(http_proxy.count(), before, "NO_PROXY host must bypass");

    tls_oracle.shutdown().await;
    oracle.shutdown().await;
}

/// Child-side env-proxy cases. Each asserts its observable outcome and
/// exits nonzero on failure (panic → child status failure → parent fails).
async fn env_proxy_child(case: &str) {
    let oracle_port: u16 = std::env::var("T6_ORACLE_PORT").unwrap().parse().unwrap();
    let tls_port: u16 = std::env::var("T6_TLS_PORT").unwrap().parse().unwrap();
    let explicit_port: u16 = std::env::var("T6_EXPLICIT_PROXY_PORT")
        .unwrap()
        .parse()
        .unwrap();
    let proxied_base = format!("http://{PROXIED_HOST}:{oracle_port}");
    let localhost_base = format!("http://localhost:{oracle_port}");

    match case {
        "http_env" => {
            // Bare host:port HTTP_PROXY honored for non-localhost http.
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&config_for(&proxied_base), t).unwrap();
            clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect("env-proxied unary must succeed");
        }
        "https_env" => {
            // HTTPS_PROXY honored; cert trusted via extra roots. The
            // target is non-localhost (loopback bypasses env proxies).
            let mut cfg = config_for(&format!("https://{PROXIED_HOST}:{tls_port}"));
            cfg.extra_root_pems =
                vec![std::fs::read(std::env::var("T6_TLS_CERT").unwrap()).unwrap()];
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&cfg, t).unwrap();
            clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect("env-proxied https unary must succeed");
        }
        "all_proxy_ignored" => {
            // ALL_PROXY set but ignored → direct dial of test.invalid
            // fails DNS (unavailable), proving the socks proxy was unused.
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&config_for(&proxied_base), t).unwrap();
            let err = clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect_err("direct dial of test.invalid must fail DNS");
            assert_eq!(err.code, ErrorCode::Unavailable);
        }
        "localhost_bypass" => {
            // HTTP_PROXY set but localhost bypasses → direct success.
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&config_for(&localhost_base), t).unwrap();
            clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect("localhost must bypass env proxy and succeed");
        }
        "explicit_wins" => {
            // Explicit proxy set → env vars ignored entirely.
            let mut cfg = config_for(&proxied_base);
            cfg.proxy = format!("http://127.0.0.1:{explicit_port}");
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&cfg, t).unwrap();
            clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect("explicit-proxied unary must succeed");
        }
        "socks_env_remote_dns" => {
            // HTTP_PROXY=socks5://… → remote DNS (ATYP=domain asserted by
            // the parent); the stub maps test.invalid → 127.0.0.1.
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&config_for(&proxied_base), t).unwrap();
            clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect("env socks5 unary must succeed");
        }
        "no_proxy_bypass" => {
            // NO_PROXY matches test.invalid → direct dial → DNS failure.
            let (_c, t) = token_source("qa-env-token");
            let clients = upstream_clients(&config_for(&proxied_base), t).unwrap();
            let err = clients
                .api
                .get_status(status_request("qa-env-token"))
                .await
                .expect_err("NO_PROXY bypass must dial directly and fail DNS");
            assert_eq!(err.code, ErrorCode::Unavailable);
        }
        other => panic!("unknown T6_ENV_CASE {other}"),
    }
}

/// Dropping a live server stream cancels the RPC: the Go handler observes
/// `ctx.Done()` promptly (bounded poll, no sleeps).
#[tokio::test]
async fn cancel_drop_observed_by_server() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (_cell, token) = token_source("qa-cancel-token");
    let clients = upstream_clients(&config_for(&oracle.base()), token).unwrap();

    let mut stream = clients
        .stream
        .get_chat_message_with_options(
            chat_request("qa-cancel-token"),
            CallOptions::default().with_header("x-stub-mode", "hang"),
        )
        .await
        .expect("hang stream");
    // First frame arrives, then the server holds.
    let first = stream.message().await.expect("first frame");
    assert!(first.is_some(), "expected the meta frame");
    drop(stream);

    let cancels = oracle.wait_stat("cancels", 1).await;
    assert!(
        cancels >= 1,
        "server never observed the drop (cancels={cancels})"
    );

    oracle.shutdown().await;
}

/// `Failure` QA: truncated envelope, dead proxy and TLS certificate
/// rejection are transport failures — distinct from semantic Connect
/// errors, which keep their server-sent code.
#[tokio::test]
async fn truncated_envelope_proxy_and_tls_failure() {
    let Some(oracle) = Oracle::start("h1").await else {
        return;
    };
    let (_cell, token) = token_source("qa-fail-token");
    let clients = upstream_clients(&config_for(&oracle.base()), token).unwrap();

    // 1. Semantic Connect error: server-sent resource_exhausted survives
    //    as-is (unary and streaming).
    let err = clients
        .api
        .get_status_with_options(
            status_request("qa-fail-token"),
            CallOptions::default().with_header("x-stub-mode", "error"),
        )
        .await
        .expect_err("stub error unary");
    assert_eq!(err.code, ErrorCode::ResourceExhausted, "semantic code lost");

    let mut stream = clients
        .stream
        .get_chat_message_with_options(
            chat_request("qa-fail-token"),
            CallOptions::default().with_header("x-stub-mode", "error"),
        )
        .await
        .expect("stream construction ok");
    let err = stream.message().await.expect_err("stub error stream");
    assert_eq!(err.code, ErrorCode::ResourceExhausted);

    // 2. Truncated envelope mid-stream: a transport-level failure, NOT a
    //    semantic Connect error — the client must not invent a clean end
    //    or a server code.
    let mut stream = clients
        .stream
        .get_chat_message_with_options(
            chat_request("qa-fail-token"),
            CallOptions::default().with_header("x-stub-mode", "truncate"),
        )
        .await
        .expect("truncate stream construction");
    let mut first_ok = false;
    let err = loop {
        match stream.message().await {
            Ok(Some(_)) => first_ok = true,
            Ok(None) => panic!("truncated stream reported a clean end"),
            Err(e) => break e,
        }
    };
    assert!(first_ok, "meta frame should decode before the cut");
    assert_eq!(
        err.code,
        ErrorCode::Internal,
        "truncated envelope must be a wire error, got {:?}: {}",
        err.code,
        err.message.as_deref().unwrap_or("")
    );

    // 3. Dead proxy: connection failure is a transport error (unavailable),
    //    never a semantic Connect code.
    let dead = HttpProxy::dead().await;
    let mut cfg = config_for(&oracle.base());
    cfg.proxy = dead.url();
    let (_c3, t3) = token_source("qa-fail-token");
    let clients_dead = upstream_clients(&cfg, t3).unwrap();
    let err = clients_dead
        .api
        .get_status(status_request("qa-fail-token"))
        .await
        .expect_err("dead proxy unary");
    assert_eq!(
        err.code,
        ErrorCode::Unavailable,
        "dead proxy must be a transport failure, got {:?}",
        err.code
    );

    // 4. TLS: a self-signed cert NOT in the root store is rejected —
    //    verification is on, failure is transport-level.
    let Some(tls_oracle) = Oracle::start("tls").await else {
        oracle.shutdown().await;
        return;
    };
    let cfg = config_for(&tls_oracle.tls_base()); // no extra roots
    let (_c4, t4) = token_source("qa-fail-token");
    let clients_tls = upstream_clients(&cfg, t4).unwrap();
    let err = clients_tls
        .api
        .get_status(status_request("qa-fail-token"))
        .await
        .expect_err("untrusted cert must fail");
    assert_eq!(
        err.code,
        ErrorCode::Unavailable,
        "cert rejection must be a transport failure, got {:?}: {}",
        err.code,
        err.message.as_deref().unwrap_or("")
    );

    tls_oracle.shutdown().await;
    oracle.shutdown().await;
}

/// Construction-time validation: unsupported proxy schemes fail like Go's
/// `NewTransport`, and a bad base `URL` is rejected before any RPC.
#[test]
fn invalid_proxy_scheme_and_base_url_rejected() {
    let mut cfg = config_for("http://127.0.0.1:1");
    cfg.proxy = "gopher://example.com".to_string();
    let err = build_http_client(&cfg).expect_err("gopher scheme must fail");
    assert!(
        err.to_string().contains("unsupported proxy scheme"),
        "wrong error: {err}"
    );

    cfg.proxy = "http://[::1".to_string();
    assert!(build_http_client(&cfg).is_err(), "malformed proxy URL");

    let mut cfg = config_for("not a url");
    cfg.proxy = String::new();
    let (_c, t) = token_source("x");
    assert!(upstream_clients(&cfg, t).is_err(), "bad base URL");
}

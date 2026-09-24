//! Connect and Seat transports with Go parity.
//!
//! Mirrors `devin2api/internal/adapter/devin/devin.go` (Connect unary +
//! server-streaming over `connectrpc.com/connect`) and
//! `devin2api/internal/dashboard/upstream.go` (Seat JSON Connect
//! endpoint) on top of the `connectrpc` crate's `ClientTransport` trait
//! and reqwest.
//!
//! Parity contract (from the Go sources):
//! - `Authorization: Basic <token>-<token>` is injected per attempt when
//!   the request carries no Authorization header; a pre-set header (e.g.
//!   `Bearer` from the Seat path or a per-call override) passes through
//!   untouched. The token function is re-evaluated on every request so
//!   credential repair needs no transport rebuild.
//! - No `User-Agent` is ever sent (Go's `RoundTripper` sets it to the empty
//!   string, which net/http omits entirely).
//! - `ResponseHeaderTimeout` is 600s: a request whose response headers
//!   have not arrived by then fails with `deadline_exceeded`.
//! - Unary calls additionally get a whole-call deadline of 610s
//!   (`http.Client.Timeout`), covering the body; streaming calls have no
//!   whole-call deadline (Go's stream client leaves `Timeout` unset).
//! - `ForceAttemptHTTP2` is false: HTTP/1.1 on cleartext, negotiated
//!   HTTP/2 over TLS. `force_http1` pins ALPN to http/1.1 even on TLS.
//! - Proxy: `devin.proxy` non-empty → that proxy for all schemes
//!   (`http.Transport.Proxy` static); empty → `ProxyFromEnvironment`
//!   semantics via [`crate::upstream::envproxy`]. Go accepts only
//!   http/https/socks5/socks5h proxy schemes; `socks5` means remote DNS
//!   (Go's dialer always sends the hostname — `socks5` is treated the
//!   same as `socks5h`), so both map to reqwest's `socks5h`.
//! - Pool: `MaxIdleConns` 2000, `MaxIdleConnsPerHost` 200,
//!   `IdleConnTimeout` 120s, `TLSHandshakeTimeout` 10s,
//!   `ExpectContinueTimeout` 1s, `DialContext` 30s/keepalive 30s.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use connectrpc::ErrorCode;
use connectrpc::client::{BoxFuture, ClientBody, ClientTransport};
use connectrpc::error::ConnectError;
use devin_proto::generated::exa::api_server_pb::{
    ApiServerServiceClient, ExaCodeiumCommonPb_Metadata,
};
use http::HeaderValue;
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt as _;
use tokio_util::sync::CancellationToken;

use crate::config::DevinConfig;

/// `ResponseHeaderTimeout` — Go `http.Transport.ResponseHeaderTimeout`.
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(600);
/// Unary whole-call deadline — Go `http.Client.Timeout` on the unary
/// client (headers + body + trailers).
const UNARY_CALL_TIMEOUT: Duration = Duration::from_secs(610);
/// Seat endpoint whole-call deadline — Go `http.Client.Timeout` on the
/// dashboard client.
const SEAT_CALL_TIMEOUT: Duration = Duration::from_secs(610);
/// Go `http.Transport.DialContext` timeout.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Go `http.Transport.IdleConnTimeout`.
const IDLE_CONN_TIMEOUT: Duration = Duration::from_secs(120);
/// Go `http.Transport.MaxIdleConnsPerHost`. (Go's pool-wide
/// `MaxIdleConns` 2000 has no reqwest equivalent; the per-host cap is
/// the binding constraint in practice.)
const MAX_IDLE_PER_HOST: usize = 200;
/// Seat response body cap — Go `io.LimitReader(4<<20)`.
const SEAT_BODY_LIMIT: usize = 4 << 20;
/// Seat endpoint path (Go `seatUserStatusPath`).
const SEAT_USER_STATUS_PATH: &str = "/exa.seat_management_pb.SeatManagementService/GetUserStatus";
/// Seat metadata client identity (Go `clientName`/`clientVersion`).
const SEAT_CLIENT_NAME: &str = "windsurf";
const SEAT_CLIENT_VERSION: &str = "1.48.2";

/// Per-attempt bearer token (Go `tokenFunc func() string`). Called once
/// per request attempt; implementations may re-read a refreshed token.
pub type TokenSource = Arc<dyn Fn() -> String + Send + Sync>;

/// Static token source for tests and simple callers.
#[must_use]
pub fn static_token(token: impl Into<String>) -> TokenSource {
    let token = token.into();
    Arc::new(move || token.clone())
}

/// Transport construction knobs (Go `devin.Config` transport fields).
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// `devin.base_url` — e.g. `https://api.devin.ai`.
    pub base_url: String,
    /// `devin.proxy` — empty means `ProxyFromEnvironment`.
    pub proxy: String,
    /// `devin.force_http1`.
    pub force_http1: bool,
    /// Extra PEM root certificates (test seam for the QA oracle's
    /// self-signed cert; production passes none).
    pub extra_root_pems: Vec<Vec<u8>>,
}

impl TransportConfig {
    /// Build from `devin.yaml` config (Go `devin.New(cfg.Devin, ...)`).
    #[must_use]
    pub fn from_devin(devin: &DevinConfig) -> Self {
        Self {
            base_url: devin.base_url.clone(),
            proxy: devin.proxy.clone(),
            force_http1: devin
                .force_http1
                .unwrap_or(crate::config::DEFAULT_FORCE_HTTP1),
            extra_root_pems: Vec::new(),
        }
    }
}

/// Errors from transport construction (Go `NewTransport`/`New` paths).
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// `devin.base_url` failed to parse (Go connect.NewClient would fail).
    #[error("invalid devin.base_url: {0}")]
    InvalidBaseUrl(String),
    /// `devin.proxy` failed to parse (Go `url.Parse` error).
    #[error("parse proxy URL: {0}")]
    InvalidProxy(String),
    /// `devin.proxy` scheme is not one Go supports.
    #[error("unsupported proxy scheme {0:?} (use http, https, socks5, or socks5h)")]
    UnsupportedProxyScheme(String),
    /// reqwest client construction failed.
    #[error("http client build failed: {0}")]
    ClientBuild(String),
}

/// Build the shared `reqwest::Client` — Go's tuned `http.Transport`
/// shared by the stream, api and seat clients.
///
/// Go validates `devin.base_url` lazily (connect.NewClient errors on a
/// bad URL at call time); we validate eagerly here so construction
/// failures surface before any RPC, matching `devin.New`'s error paths.
pub fn build_http_client(cfg: &TransportConfig) -> Result<reqwest::Client, TransportError> {
    let base = cfg
        .base_url
        .parse::<http::Uri>()
        .map_err(|_| TransportError::InvalidBaseUrl(cfg.base_url.clone()))?;
    match base.scheme_str() {
        Some("http" | "https") => {}
        _ => return Err(TransportError::InvalidBaseUrl(cfg.base_url.clone())),
    }

    let mut builder = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_idle_timeout(IDLE_CONN_TIMEOUT)
        .pool_max_idle_per_host(MAX_IDLE_PER_HOST)
        // Go's transport sends no User-Agent (RoundTripper sets it to
        // ""); reqwest adds none by default and send() strips any that
        // arrives on the request.
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true);

    if cfg.force_http1 {
        builder = builder.http1_only();
    }

    let proxy = cfg.proxy.trim();
    if proxy.is_empty() {
        // Go: http.DefaultTransport → ProxyFromEnvironment.
        builder = builder.proxy(crate::upstream::envproxy::env_proxy());
    } else {
        let parsed =
            reqwest::Url::parse(proxy).map_err(|e| TransportError::InvalidProxy(e.to_string()))?;
        let scheme = parsed.scheme().to_ascii_lowercase();
        let proxy_url = match scheme.as_str() {
            // Go's socks dialer always sends the hostname (remote DNS);
            // reqwest's `socks5` resolves locally, so map to `socks5h`.
            "socks5" => proxy.replacen("socks5://", "socks5h://", 1),
            "http" | "https" | "socks5h" => proxy.to_string(),
            _ => return Err(TransportError::UnsupportedProxyScheme(scheme)),
        };
        let p = reqwest::Proxy::all(&proxy_url)
            .map_err(|e| TransportError::InvalidProxy(e.to_string()))?;
        builder = builder.proxy(p);
    }

    for pem in &cfg.extra_root_pems {
        let cert = reqwest::Certificate::from_pem(pem)
            .map_err(|e| TransportError::ClientBuild(e.to_string()))?;
        builder = builder.add_root_certificate(cert);
    }

    builder
        .build()
        .map_err(|e| TransportError::ClientBuild(e.to_string()))
}

/// `ClientTransport` over reqwest — Go's `BasicAuthTransport` wrapping
/// the tuned `http.Transport`, used by `connect.NewClient`/
/// `connect.NewStreamClient`.
#[derive(Clone)]
pub struct UpstreamTransport {
    client: reqwest::Client,
    token_source: TokenSource,
    /// Whole-call deadline applied per request (Go `http.Client.Timeout`
    /// on the unary client; `None` for streaming).
    call_timeout: Option<Duration>,
    /// Per-request build caches shared across clones: the parsed request
    /// URL (Connect calls hit one fixed path per client, so a one-entry
    /// memo keyed on the `http::Uri` skips `Url::parse`'s idna/uts46
    /// pass) and the `Basic <token>-<token>` header value keyed on the
    /// token (the token function still runs per request — credential
    /// repair needs no transport rebuild — but the header is only
    /// rebuilt when the token actually changed).
    url_cache: Arc<std::sync::Mutex<Option<(http::Uri, reqwest::Url)>>>,
    auth_cache: Arc<std::sync::Mutex<Option<(String, HeaderValue)>>>,
}

impl UpstreamTransport {
    /// Streaming transport — no whole-call deadline (Go stream client).
    #[must_use]
    pub fn streaming(client: reqwest::Client, token_source: TokenSource) -> Self {
        Self {
            client,
            token_source,
            call_timeout: None,
            url_cache: Arc::new(std::sync::Mutex::new(None)),
            auth_cache: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Unary transport — 610s whole-call deadline (Go unary client).
    #[must_use]
    pub fn unary(client: reqwest::Client, token_source: TokenSource) -> Self {
        Self {
            client,
            token_source,
            call_timeout: Some(UNARY_CALL_TIMEOUT),
            url_cache: Arc::new(std::sync::Mutex::new(None)),
            auth_cache: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// `Basic <token>-<token>` header for `token`, rebuilt only when the
    /// token changed since the last request on this transport.
    fn auth_header(&self, token: &str) -> Result<HeaderValue, ConnectError> {
        let mut cache = self
            .auth_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((cached_token, value)) = &*cache
            && cached_token == token
        {
            return Ok(value.clone());
        }
        let value = HeaderValue::from_str(&format!("Basic {token}-{token}")).map_err(|_| {
            ConnectError::new(ErrorCode::Internal, "token is not a valid header value")
        })?;
        *cache = Some((token.to_string(), value.clone()));
        Ok(value)
    }

    /// `reqwest::Url` for the request URI, parsed once per distinct URI
    /// (Connect requests always target the client's fixed endpoint).
    fn request_url(&self, uri: &http::Uri) -> Result<reqwest::Url, ConnectError> {
        let mut cache = self
            .url_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((cached_uri, url)) = &*cache
            && cached_uri == uri
        {
            return Ok(url.clone());
        }
        let url = reqwest::Url::parse(&uri.to_string()).map_err(|e| {
            ConnectError::new(
                ErrorCode::Internal,
                format!("request URI {uri} is not absolute: {e}"),
            )
        })?;
        *cache = Some((uri.clone(), url.clone()));
        Ok(url)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResponseBodyError {
    #[error(transparent)]
    Transport(#[from] reqwest::Error),
    #[error("{0}")]
    Wire(String),
}

/// The streaming transport's response body: a read-ahead pump owns the
/// wire body so socket reads are decoupled from envelope consumption —
/// Go's `pumpUpstream` goroutine shape (devin.go:1017), where the pump
/// runs ahead of the consumer instead of the consumer's `message()` poll
/// being the only thing that pulls bytes off the socket. Without it one
/// downstream event costs one body poll and buffered envelopes never
/// accumulate, which is the measured per-frame pipeline cost behind the
/// SSE throughput gap. Unary calls keep the direct body — a pump there
/// only adds a hop.
pub enum CheckedResponseBody {
    /// Unary path: the envelope-checking body, polled by the caller.
    Direct(EnvelopeCheckedBody),
    /// Streaming path: frames ferried by the read-ahead pump task.
    Pumped(PumpedBody),
}

/// Frames the pump may hold ahead of the consumer — Go's
/// `upstreamFrameBuffer` depth (devin.go `make(chan upstreamFrame, 64)`):
/// 64 body chunks of read-ahead per request.
const BODY_PUMP_CHUNKS: usize = 64;

/// The receiving half of the read-ahead pump: `poll_frame` is a channel
/// receive, so `ServerStream`'s decode loop drains every complete
/// envelope the pump has already buffered without touching the socket.
/// Dropping it cancels the pump, which drops the real body and kills the
/// exchange (Go's `stream.cancel`).
pub struct PumpedBody {
    rx: tokio::sync::mpsc::Receiver<Result<Frame<Bytes>, ResponseBodyError>>,
    cancel: CancellationToken,
    /// The pump reported a terminal item (error or EOF); after the
    /// channel drains, the body is over.
    closed: bool,
}

impl PumpedBody {
    /// Spawn the ferry: poll the inner body continuously, forward every
    /// frame (data AND trailers — `ServerStream` consumes both) into the
    /// bounded channel. Exits on terminal body state, a closed receiver,
    /// or cancellation.
    fn ferry(
        mut inner: EnvelopeCheckedBody,
        tx: tokio::sync::mpsc::Sender<Result<Frame<Bytes>, ResponseBodyError>>,
        cancel: CancellationToken,
    ) {
        tokio::spawn(async move {
            loop {
                let frame = tokio::select! {
                    () = cancel.cancelled() => break,
                    frame = inner.frame() => frame,
                };
                let terminal = !matches!(&frame, Some(Ok(f)) if f.is_data());
                match frame {
                    Some(frame) => {
                        if tx.send(frame).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
                if terminal {
                    break;
                }
            }
        });
    }
}

impl Drop for PumpedBody {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Body for PumpedBody {
    type Data = Bytes;
    type Error = ResponseBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.closed {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                if item.is_err() {
                    self.closed = true;
                }
                Poll::Ready(Some(item))
            }
            // The pump exited: channel closed and drained.
            Poll::Ready(None) => {
                self.closed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.closed && self.rx.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

impl Body for CheckedResponseBody {
    type Data = Bytes;
    type Error = ResponseBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut *self {
            Self::Direct(body) => Pin::new(body).poll_frame(cx),
            Self::Pumped(body) => Pin::new(body).poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Direct(body) => body.is_end_stream(),
            Self::Pumped(body) => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Direct(body) => body.size_hint(),
            Self::Pumped(body) => body.size_hint(),
        }
    }
}

/// Pass-through response body that independently tracks Connect envelopes.
/// connectrpc 0.9 treats a clean HTTP-body EOF with an incomplete envelope as
/// merely "missing `END_STREAM`", losing the distinction connect-go exposes.
/// Preserve that raw framing fact so the retry layer can emit Go-compatible
/// protocol errors without buffering or altering delivered bytes.
///
/// The tracker is a counting state machine — a 5-byte header scratch plus
/// a payload skip counter — so body bytes are never copied or drained:
/// the old `Vec<u8>` buffer paid one full-body copy plus an O(remaining)
/// `drain` memmove per envelope.
// The bools mirror the Go envelope parser's flags one-for-one.
#[allow(clippy::struct_excessive_bools)]
pub struct EnvelopeCheckedBody {
    inner: Pin<Box<reqwest::Body>>,
    /// Bytes of the in-flight envelope header collected so far (0..5).
    header: [u8; 5],
    header_len: usize,
    /// Payload bytes still owed by the current envelope; 0 with
    /// `header_len == 0` means the stream sits on an envelope boundary.
    remaining: usize,
    /// The in-flight envelope carries the `END_STREAM` flag; latched into
    /// `saw_end_stream` only once its payload is fully consumed — a
    /// truncated `END_STREAM` envelope still reports "promised N got M".
    end_stream_pending: bool,
    saw_end_stream: bool,
    inspect_connect_stream: bool,
    eof_reported: bool,
}

impl EnvelopeCheckedBody {
    fn new(body: reqwest::Body, inspect_connect_stream: bool) -> Self {
        Self {
            inner: Box::pin(body),
            header: [0; 5],
            header_len: 0,
            remaining: 0,
            end_stream_pending: false,
            saw_end_stream: false,
            inspect_connect_stream,
            eof_reported: false,
        }
    }

    /// Consume `data` through the envelope state machine: header bytes
    /// fill the 5-byte scratch, payload bytes only decrement the skip
    /// counter — nothing is retained.
    fn observe(&mut self, mut data: &[u8]) {
        if !self.inspect_connect_stream || self.saw_end_stream {
            return;
        }
        while !data.is_empty() {
            if self.header_len < 5 {
                let take = (5 - self.header_len).min(data.len());
                self.header[self.header_len..self.header_len + take].copy_from_slice(&data[..take]);
                self.header_len += take;
                data = &data[take..];
                if self.header_len < 5 {
                    return;
                }
                self.remaining = u32::from_be_bytes([
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ]) as usize;
                self.end_stream_pending = self.header[0] & 0x02 != 0;
                continue;
            }
            let take = self.remaining.min(data.len());
            self.remaining -= take;
            data = &data[take..];
            if self.remaining == 0 {
                // Envelope complete: back to the boundary state; latch
                // END_STREAM only now so a truncated END_STREAM payload
                // still reports the promised/got mismatch at EOF.
                self.header_len = 0;
                self.saw_end_stream = self.end_stream_pending;
                self.end_stream_pending = false;
            }
        }
    }

    /// The Go-worded protocol error for a body EOF at the current parser
    /// position; `None` on a clean envelope boundary (handled by the
    /// caller's "unexpected EOF" arm).
    fn eof_error(&self) -> Option<String> {
        if self.header_len == 0 && self.remaining == 0 {
            return None;
        }
        if self.header_len < 5 {
            return Some("protocol error: incomplete envelope: unexpected EOF".into());
        }
        let promised = u32::from_be_bytes([
            self.header[1],
            self.header[2],
            self.header[3],
            self.header[4],
        ]) as usize;
        let got = promised - self.remaining;
        Some(format!(
            "protocol error: promised {promised} bytes in enveloped message, got {got} bytes"
        ))
    }
}

impl Body for EnvelopeCheckedBody {
    type Data = Bytes;
    type Error = ResponseBodyError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.observe(data);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(ResponseBodyError::Transport(error))))
            }
            Poll::Ready(None)
                if self.inspect_connect_stream && !self.saw_end_stream && !self.eof_reported =>
            {
                self.eof_reported = true;
                let message = self
                    .eof_error()
                    .unwrap_or_else(|| "protocol error: unexpected EOF".into());
                Poll::Ready(Some(Err(ResponseBodyError::Wire(message))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.eof_reported && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl ClientTransport for UpstreamTransport {
    type ResponseBody = CheckedResponseBody;
    type Error = ConnectError;

    fn send(
        &self,
        request: http::Request<ClientBody>,
    ) -> BoxFuture<'static, Result<http::Response<Self::ResponseBody>, Self::Error>> {
        let this = self.clone();
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let mut headers = parts.headers;

            // Go BasicAuthTransport.RoundTrip: inject Basic <token>-<token>
            // only when Authorization is absent; a pre-set header (Seat
            // Bearer, per-call override) passes through untouched.
            if !headers.contains_key(http::header::AUTHORIZATION) {
                let token = (this.token_source)();
                headers.insert(http::header::AUTHORIZATION, this.auth_header(&token)?);
            }
            // Go sets User-Agent to "" which net/http omits entirely.
            headers.remove(http::header::USER_AGENT);
            // Go's connect-go sends `Accept-Encoding: identity` on
            // streaming calls (protocol_connect.go); reqwest adds none
            // when built without compression features. Match the wire.
            if !headers.contains_key(http::header::ACCEPT_ENCODING) {
                headers.insert(
                    http::header::ACCEPT_ENCODING,
                    HeaderValue::from_static("identity"),
                );
            }

            let url = this.request_url(&parts.uri)?;

            let mut builder = this
                .client
                .request(parts.method, url)
                .headers(headers)
                .body(reqwest::Body::wrap(body));
            if let Some(t) = this.call_timeout {
                // reqwest's per-request timeout covers the whole call
                // including the response body — Go's http.Client.Timeout.
                builder = builder.timeout(t);
            }
            let req = builder.build().map_err(|e| {
                ConnectError::new(ErrorCode::Internal, format!("build request: {e}"))
            })?;

            // ResponseHeaderTimeout: covers everything up to the first
            // response header byte; the body is unbounded for streaming.
            let resp = match tokio::time::timeout(RESPONSE_HEADER_TIMEOUT, this.client.execute(req))
                .await
            {
                Ok(Ok(resp)) => resp,
                Ok(Err(e)) => {
                    return Err(ConnectError::unavailable_from_transport(
                        "request failed",
                        e,
                    ));
                }
                Err(_) => {
                    return Err(ConnectError::new(
                        ErrorCode::DeadlineExceeded,
                        "context deadline exceeded (ResponseHeaderTimeout)",
                    ));
                }
            };
            let inspect_connect_stream = resp
                .headers()
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("application/connect+"));
            let response: http::Response<reqwest::Body> = resp.into();
            let (parts, body) = response.into_parts();
            let checked = EnvelopeCheckedBody::new(body, inspect_connect_stream);
            let body = if this.call_timeout.is_none() {
                // Streaming call: hand the wire body to the read-ahead
                // pump so socket reads are decoupled from envelope
                // consumption (Go's pumpUpstream).
                let (tx, rx) = tokio::sync::mpsc::channel(BODY_PUMP_CHUNKS);
                let cancel = CancellationToken::new();
                PumpedBody::ferry(checked, tx, cancel.clone());
                CheckedResponseBody::Pumped(PumpedBody {
                    rx,
                    cancel,
                    closed: false,
                })
            } else {
                CheckedResponseBody::Direct(checked)
            };
            Ok(http::Response::from_parts(parts, body))
        })
    }
}

/// The stream + api clients sharing one transport — Go `devin.New`'s
/// `streamClient`/`apiClient` pair.
pub struct UpstreamClients {
    /// `connect.NewStreamClient` — server-streaming RPCs, no call timeout.
    pub stream: ApiServerServiceClient<UpstreamTransport>,
    /// `connect.NewClient` — unary RPCs, 610s whole-call deadline.
    pub api: ApiServerServiceClient<UpstreamTransport>,
}

/// Build stream + api clients over one shared `reqwest::Client` (Go
/// `devin.New`: one `http.Transport`, two `http.Client`s).
pub fn upstream_clients(
    cfg: &TransportConfig,
    token_source: TokenSource,
) -> Result<UpstreamClients, TransportError> {
    let client = build_http_client(cfg)?;
    let uri: http::Uri = cfg
        .base_url
        .parse()
        .map_err(|_| TransportError::InvalidBaseUrl(cfg.base_url.clone()))?;
    // Go's client never advertises Connect compression
    // (`connect-accept-encoding` is only sent when CompressionPools is
    // non-empty, and devin.go sets none): an empty registry drops the
    // header — a wire-visible parity fix, not just a skipped code path.
    let config = connectrpc::client::ClientConfig::new(uri)
        .with_compression(connectrpc::compression::CompressionRegistry::new());
    Ok(UpstreamClients {
        stream: ApiServerServiceClient::new(
            UpstreamTransport::streaming(client.clone(), token_source.clone()),
            config.clone(),
        ),
        api: ApiServerServiceClient::new(UpstreamTransport::unary(client, token_source), config),
    })
}

/// Seat endpoint client (Go `dashboard.fetchUserStatus`): JSON Connect
/// POST with Bearer auth, sharing the proxy-capable `reqwest::Client`.
pub struct SeatClient {
    client: reqwest::Client,
    url: String,
    token_source: TokenSource,
}

/// Seat endpoint errors (Go `fetchUserStatus` failure modes).
#[derive(Debug, thiserror::Error)]
pub enum SeatError {
    /// Non-200 status (Go `GetUserStatus HTTP %d: %s`).
    #[error("GetUserStatus HTTP {0}: {1}")]
    Http(u16, String),
    /// Body read or JSON decode failure.
    #[error("decode GetUserStatus: {0}")]
    Decode(String),
    /// `userStatus` missing from the response.
    #[error("GetUserStatus: empty userStatus")]
    EmptyUserStatus,
    /// Transport failure.
    #[error("GetUserStatus transport: {0}")]
    Transport(String),
}

impl SeatClient {
    /// `client` is the shared proxy-capable client; `base` is
    /// `devin.base_url`; `token_source` is re-evaluated per call.
    #[must_use]
    pub fn new(client: reqwest::Client, base: &str, token_source: TokenSource) -> Self {
        Self {
            client,
            url: format!("{}{}", base.trim_end_matches('/'), SEAT_USER_STATUS_PATH),
            token_source,
        }
    }

    /// `POST GetUserStatus` — returns the raw `userStatus` object.
    pub async fn get_user_status(&self) -> Result<serde_json::Value, SeatError> {
        let root = self.get_status_root().await?;
        Ok(root
            .get("userStatus")
            .or_else(|| root.get("user_status"))
            .cloned()
            .expect("get_status_root validates userStatus"))
    }

    /// `POST GetUserStatus` retaining the response root. The dashboard needs
    /// root-level `planInfo` used by some Seat response versions.
    pub async fn get_status_root(&self) -> Result<serde_json::Value, SeatError> {
        let token = (self.token_source)();
        let body = serde_json::json!({
            "metadata": {
                "api_key": token,
                "extension_name": SEAT_CLIENT_NAME,
                "extension_version": SEAT_CLIENT_VERSION,
                "ide_name": SEAT_CLIENT_NAME,
                "ide_version": SEAT_CLIENT_VERSION,
                "locale": "en",
                "os": "windows",
            }
        });
        let resp = tokio::time::timeout(
            SEAT_CALL_TIMEOUT,
            self.client
                .post(&self.url)
                .header(http::header::CONTENT_TYPE, "application/json")
                .header("Connect-Protocol-Version", "1")
                .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
                .header(http::header::USER_AGENT, "")
                .body(body.to_string())
                .send(),
        )
        .await
        .map_err(|_| SeatError::Transport("context deadline exceeded".into()))?
        .map_err(|e| SeatError::Transport(e.to_string()))?;

        let status = resp.status().as_u16();
        let raw = resp
            .bytes()
            .await
            .map_err(|e| SeatError::Decode(e.to_string()))?;
        if raw.len() > SEAT_BODY_LIMIT {
            return Err(SeatError::Decode("response exceeds 4MiB".into()));
        }
        if status != 200 {
            let snippet = String::from_utf8_lossy(&raw);
            let snippet = snippet.chars().take(300).collect::<String>();
            return Err(SeatError::Http(status, snippet));
        }
        let root: serde_json::Value =
            serde_json::from_slice(&raw).map_err(|e| SeatError::Decode(e.to_string()))?;
        if root
            .get("userStatus")
            .or_else(|| root.get("user_status"))
            .is_none()
        {
            return Err(SeatError::EmptyUserStatus);
        }
        Ok(root)
    }
}

/// `BuildMetadata` — the `Metadata` message G attaches to every upstream
/// call (Go `upstream.BuildMetadata`). `fingerprint_bytes` is the random
/// hex fingerprint size in bytes (chisel CLI uses 366, Windsurf 32, 0
/// omits the field).
#[must_use]
pub fn build_metadata(
    token: &str,
    client_name: &str,
    client_version: &str,
    os: &str,
    fingerprint_bytes: usize,
) -> ExaCodeiumCommonPb_Metadata {
    let mut metadata = ExaCodeiumCommonPb_Metadata {
        api_key: Some(token.into()),
        extension_name: Some(client_name.into()),
        extension_version: Some(client_version.into()),
        ide_name: Some(client_name.into()),
        ide_version: Some(client_version.into()),
        locale: Some("en".into()),
        os: Some(os.into()),
        ..Default::default()
    };
    if fingerprint_bytes > 0 {
        metadata.f = Some(random_hex(fingerprint_bytes));
    }
    metadata
}

/// `randid.Hex` — hex-encoded random bytes.
fn random_hex(nbytes: usize) -> String {
    use std::fmt::Write as _;
    let mut buf = vec![0u8; nbytes];
    rand::fill(&mut buf);
    let mut out = String::with_capacity(nbytes * 2);
    for b in &buf {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    /// The read-ahead pump (task-24 C): frames must reach the channel
    /// without the consumer ever polling the body — Go's `pumpUpstream`
    /// shape. Deterministic on a current-thread runtime: `yield_now`
    /// lets the ferry run to channel capacity before any `poll_frame`.
    #[tokio::test(flavor = "current_thread")]
    async fn pump_runs_ahead_of_consumer() {
        let chunks: Vec<Result<Frame<Bytes>, std::convert::Infallible>> = (0..8)
            .map(|i| {
                Ok(Frame::data(Bytes::from(vec![
                    u8::try_from(i).unwrap_or(0);
                    4
                ])))
            })
            .collect();
        let inner = EnvelopeCheckedBody::new(
            reqwest::Body::wrap(http_body_util::StreamBody::new(stream::iter(chunks))),
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(BODY_PUMP_CHUNKS);
        let cancel = CancellationToken::new();
        PumpedBody::ferry(inner, tx, cancel.clone());
        let mut body = PumpedBody {
            rx,
            cancel,
            closed: false,
        };
        // No poll_frame yet: the ferry must have drained the whole body.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(body.rx.len(), 8, "pump did not run ahead of the consumer");
        // Frames arrive in order, then a clean end.
        for i in 0..8u8 {
            let frame = body
                .frame()
                .await
                .expect("stream ended early")
                .expect("frame error");
            assert_eq!(frame.into_data().unwrap(), Bytes::from(vec![i; 4]));
        }
        assert!(body.frame().await.is_none(), "expected end of stream");
    }

    /// A truncated Connect envelope surfaces through the pump as the
    /// wire error the checker synthesizes at EOF, then the body ends.
    #[tokio::test(flavor = "current_thread")]
    async fn pump_forwards_truncated_envelope_error() {
        // One envelope header promising 10 bytes, only 2 delivered.
        let wire = vec![0x00, 0x00, 0x00, 0x00, 0x0A, 0xAA, 0xBB];
        let inner = EnvelopeCheckedBody::new(
            reqwest::Body::wrap(http_body_util::StreamBody::new(stream::iter(vec![Ok::<
                _,
                std::convert::Infallible,
            >(
                Frame::data(Bytes::from(wire)),
            )]))),
            true,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(BODY_PUMP_CHUNKS);
        let cancel = CancellationToken::new();
        PumpedBody::ferry(inner, tx, cancel.clone());
        let mut body = PumpedBody {
            rx,
            cancel,
            closed: false,
        };
        let mut saw_error = false;
        while let Some(item) = body.frame().await {
            match item {
                Ok(_) => {}
                Err(ResponseBodyError::Wire(message)) => {
                    saw_error = true;
                    assert!(
                        message.contains("promised 10 bytes"),
                        "unexpected wire error: {message}"
                    );
                }
                Err(other) => panic!("unexpected transport error: {other}"),
            }
        }
        assert!(saw_error, "truncated envelope error never surfaced");
    }

    /// Dropping the pumped body cancels the ferry: a body that never ends
    /// must not pin the task forever.
    #[tokio::test(flavor = "current_thread")]
    async fn drop_cancels_pump() {
        let (never_tx, mut never_rx) =
            tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::convert::Infallible>>(1);
        let inner = EnvelopeCheckedBody::new(
            reqwest::Body::wrap(http_body_util::StreamBody::new(
                futures_util::stream::poll_fn(move |cx| never_rx.poll_recv(cx)),
            )),
            false,
        );
        let (tx, rx) = tokio::sync::mpsc::channel(BODY_PUMP_CHUNKS);
        let cancel = CancellationToken::new();
        PumpedBody::ferry(inner, tx, cancel.clone());
        let body = PumpedBody {
            rx,
            cancel: cancel.clone(),
            closed: false,
        };
        drop(body);
        assert!(cancel.is_cancelled());
        drop(never_tx);
    }
}

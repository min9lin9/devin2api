//! The unified app-layer adaptation boundary of the three API protocols.
//!
//! Port of `G/internal/app/protocols.go`: per-protocol stream/final/error
//! encoding plus SSE framing, behind one trait so the HTTP stream pump
//! needs no per-protocol adaptation.

use bytes::BytesMut;
use serde_json::{Value, json};

use crate::domain::{AssistantMessage, Failure, ResponseEvent, classify};

use super::common::{
    SSE_DONE, SseEvent, anthropic_error_type, build_error_payload, go_marshal, openai_error_type,
};
use super::{chat, messages, responses};

/// The app layer's normalized error view: the protocol layer only packs
/// the same field set into its own envelope. `client_fixable` marks a
/// caller-fixable request error (unsupported image / `invalid_argument` /
/// over-long) and overrides the protocol's default type derivation — both
/// protocols name that semantic `invalid_request_error`.
#[derive(Debug, Clone)]
pub struct HttpError {
    /// The classified record of this failure: type/code/diagnostic fields
    /// all derive from it; the protocol layer no longer re-derives from
    /// text.
    pub failure: Failure,
    /// Caller-fixable request error marker.
    pub client_fixable: bool,
    /// The processing stage where the failure happened (`http_decode`,
    /// `provider_stream`, ...).
    pub stage: String,
    /// Local debug directory name; empty omits the field.
    pub debug_ref: String,
}

/// The shared intermediate-event encoding of the three protocols (Go's
/// `streamEncoder` interface).
pub trait StreamEncoder: Send {
    /// Expands one intermediate event into zero or more SSE events.
    fn encode(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure>;
}

impl StreamEncoder for responses::StreamEncoder {
    fn encode(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        Self::encode(self, event)
    }
}

impl StreamEncoder for chat::StreamEncoder {
    fn encode(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        Self::encode(self, event)
    }
}

impl StreamEncoder for messages::StreamEncoder {
    fn encode(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        Self::encode(self, event)
    }
}

/// Streaming and non-streaming protocol encoding (Go's `protocolEncoder`
/// interface).
pub trait ProtocolEncoder {
    /// Creates a stream encoder bound to this HTTP request.
    fn new_stream_encoder(&self, model: &str, include_usage: bool) -> Box<dyn StreamEncoder>;
    /// Encodes the final assistant message as a complete non-streaming
    /// JSON response body. `model` is echoed to the client — the request
    /// text (may be an alias); empty falls back to the upstream-declared /
    /// resolved uid. The streaming path takes the same name in
    /// `new_stream_encoder`, so both modes echo identically.
    fn encode_final(
        &self,
        message: Option<&AssistantMessage>,
        model: &str,
    ) -> Result<Vec<u8>, Failure>;
    /// Encodes an error as this protocol's error JSON body — after a
    /// non-streaming heartbeat committed 200, the error can only go down
    /// as an error body shaped per the client's protocol.
    fn encode_error(&self, err: &(dyn std::error::Error + 'static), debug_ref: &str) -> Vec<u8>;
    /// Encodes an error as this protocol's complete error response body
    /// for the HTTP error path before response headers commit:
    /// /v1/messages failures must be Anthropic's
    /// `{"type":"error","error":{...}}` envelope — an `OpenAI` shape leaves
    /// Claude Code unable to parse the error field.
    fn encode_http_error(&self, error: &HttpError) -> Vec<u8>;
    /// Whether this protocol's streaming clients treat in-stream error
    /// events as retryable signals: rate limiting (429) is the only
    /// pre-stream failure class converted to "200 + error event" — Codex
    /// terminates on any HTTP 429 (codex-rs `retry_429` hardcoded false,
    /// while 5xx/transport retry normally), so only an in-stream error
    /// event enters its retry loop; deterministic 4xx retries are
    /// pointless and keep their real status so downstream gateways
    /// classify them as request-level errors. `false` (Anthropic) means
    /// the client retries by HTTP status, and committing 200 early would
    /// degrade the failure into an unretryable malformed response.
    fn stream_error_events(&self) -> bool;
    /// Appends one SSE event to `dst`; the writer owns `dst`, avoiding a
    /// temporary slice per frame copied wholesale into the batch buffer.
    /// `BytesMut` so the SSE body's batch flushes zero-copy.
    fn append_sse(&self, dst: &mut BytesMut, name: &str, data: &[u8]);
}

/// Encodes the OpenAI-family (chat/responses shared) `{"error":{...}}`
/// envelope; a non-empty `stage` means the HTTP error response shape —
/// with a `stage` field and a trailing newline, which in-stream error
/// bodies do not carry.
fn marshal_openai_error(
    failure: &Failure,
    error_type: &str,
    debug_ref: &str,
    stage: &str,
) -> Vec<u8> {
    let mut payload =
        build_error_payload(&failure.to_string(), failure, error_type, debug_ref, true);
    if !stage.is_empty() {
        payload["stage"] = Value::String(stage.to_string());
    }
    let mut body = go_marshal(&json!({"error": payload}));
    if !stage.is_empty() {
        body.push(b'\n');
    }
    body
}

/// Encodes the OpenAI-family (chat/responses shared) HTTP error body.
fn openai_http_error(error: &HttpError) -> Vec<u8> {
    let mut error_type = openai_error_type(&error.failure);
    if error.client_fixable {
        error_type = "invalid_request_error";
    }
    marshal_openai_error(&error.failure, error_type, &error.debug_ref, &error.stage)
}

/// Encodes the OpenAI-family (chat/responses shared) error JSON body.
fn openai_error_body(err: &(dyn std::error::Error + 'static), debug_ref: &str) -> Vec<u8> {
    let failure = classify(err);
    marshal_openai_error(&failure, openai_error_type(&failure), debug_ref, "")
}

/// Appends one named SSE frame to `dst` — `fmt.Appendf` would parse a
/// format string and reflect-box per frame; direct concatenation skips
/// that layer.
fn append_named_sse(dst: &mut BytesMut, name: &str, data: &[u8]) {
    dst.extend_from_slice(b"event: ");
    dst.extend_from_slice(name.as_bytes());
    dst.extend_from_slice(b"\ndata: ");
    dst.extend_from_slice(data);
    dst.extend_from_slice(b"\n\n");
}

/// The `OpenAI` Responses API protocol.
pub struct ResponsesProtocol;

impl ProtocolEncoder for ResponsesProtocol {
    fn new_stream_encoder(&self, model: &str, _include_usage: bool) -> Box<dyn StreamEncoder> {
        Box::new(responses::StreamEncoder::new(model))
    }

    fn encode_final(
        &self,
        message: Option<&AssistantMessage>,
        model: &str,
    ) -> Result<Vec<u8>, Failure> {
        responses::encode_response(message, model)
    }

    fn encode_error(&self, err: &(dyn std::error::Error + 'static), debug_ref: &str) -> Vec<u8> {
        openai_error_body(err, debug_ref)
    }

    fn encode_http_error(&self, error: &HttpError) -> Vec<u8> {
        openai_http_error(error)
    }

    fn stream_error_events(&self) -> bool {
        true
    }

    fn append_sse(&self, dst: &mut BytesMut, name: &str, data: &[u8]) {
        append_named_sse(dst, name, data);
    }
}

/// The `OpenAI` Chat Completions API protocol.
pub struct ChatProtocol;

impl ProtocolEncoder for ChatProtocol {
    fn new_stream_encoder(&self, model: &str, include_usage: bool) -> Box<dyn StreamEncoder> {
        Box::new(chat::StreamEncoder::new(model, include_usage))
    }

    fn encode_final(
        &self,
        message: Option<&AssistantMessage>,
        model: &str,
    ) -> Result<Vec<u8>, Failure> {
        chat::encode_response(message, model)
    }

    fn encode_error(&self, err: &(dyn std::error::Error + 'static), debug_ref: &str) -> Vec<u8> {
        openai_error_body(err, debug_ref)
    }

    fn encode_http_error(&self, error: &HttpError) -> Vec<u8> {
        openai_http_error(error)
    }

    fn stream_error_events(&self) -> bool {
        true
    }

    fn append_sse(&self, dst: &mut BytesMut, name: &str, data: &[u8]) {
        // OpenAI Chat Completions uses data-only SSE; [DONE] is the stream
        // terminator.
        if name == SSE_DONE {
            dst.extend_from_slice(b"data: [DONE]\n\n");
            return;
        }
        dst.extend_from_slice(b"data: ");
        dst.extend_from_slice(data);
        dst.extend_from_slice(b"\n\n");
    }
}

/// The Anthropic Messages API protocol.
pub struct AnthropicProtocol;

impl ProtocolEncoder for AnthropicProtocol {
    fn new_stream_encoder(&self, model: &str, _include_usage: bool) -> Box<dyn StreamEncoder> {
        Box::new(messages::StreamEncoder::new(model))
    }

    fn encode_final(
        &self,
        message: Option<&AssistantMessage>,
        model: &str,
    ) -> Result<Vec<u8>, Failure> {
        messages::encode_response(message, model)
    }

    fn encode_error(&self, err: &(dyn std::error::Error + 'static), debug_ref: &str) -> Vec<u8> {
        let failure = classify(err);
        let payload = build_error_payload(
            &failure.to_string(),
            &failure,
            anthropic_error_type(&failure),
            debug_ref,
            false,
        );
        go_marshal(&json!({"type": "error", "error": payload}))
    }

    fn encode_http_error(&self, error: &HttpError) -> Vec<u8> {
        let mut error_type = anthropic_error_type(&error.failure);
        if error.client_fixable {
            error_type = "invalid_request_error";
        }
        let mut payload = build_error_payload(
            &error.failure.to_string(),
            &error.failure,
            error_type,
            &error.debug_ref,
            false,
        );
        payload["stage"] = Value::String(error.stage.clone());
        let mut body = go_marshal(&json!({"type": "error", "error": payload}));
        body.push(b'\n');
        body
    }

    fn stream_error_events(&self) -> bool {
        false
    }

    fn append_sse(&self, dst: &mut BytesMut, name: &str, data: &[u8]) {
        append_named_sse(dst, name, data);
    }
}

//! Shared helpers for the `OpenAI`- and Anthropic-compatible frontends:
//! request decoding plus the response-side SSE/error/usage projections.
//!
//! Port of `G/internal/api/common/{content,fields,toolchoice}.go` (decoder
//! half) and `{sse,usage,error}.go` plus the `ContentAt` accessor from
//! `content.go` (encoder half).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};

use crate::domain::{
    AssistantMessage, Content, Failure, ImageContent, ResponseEvent, TextContent, ToolChoice,
    ToolChoiceMode, Usage, failure_of, is_json_object,
};

/// Unix-millisecond timestamp for decoded messages — Go's
/// `time.Now().UnixMilli()`.
pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Unix-second timestamp for response envelopes — Go's `time.Now().Unix()`.
pub fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs().cast_signed())
}

// ---------------------------------------------------------------------------
// sse.go — shared SSE event representation
// ---------------------------------------------------------------------------

/// OpenAI-style data-only stream terminator: Chat Completions sends a final
/// `data: [DONE]` frame after the last chunk.
pub const SSE_DONE: &str = "[DONE]";

/// A single SSE event to write out (Go `common.SSEEvent`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// SSE `event:` field value; Chat's data-only style uses `""` for
    /// ordinary chunks and `[DONE]` as the stream-end marker.
    pub name: &'static str,
    /// JSON-encoded SSE `data:` payload.
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// usage.go — OpenAI-side token totals
// ---------------------------------------------------------------------------

/// OpenAI-side usage totals: input folds `cache_read`/`cache_write` into the
/// prompt (a cache hit is still consumed input); when upstream gave no
/// `total_tokens` it is synthesized as `input + output`.
pub fn usage_totals(usage: &Usage) -> (i64, i64) {
    let input = usage.input + usage.cache_read + usage.cache_write;
    let mut total = usage.total_tokens;
    if total == 0 {
        total = input + usage.output;
    }
    (input, total)
}

// ---------------------------------------------------------------------------
// error.go — failure record -> protocol error type / HTTP status
// ---------------------------------------------------------------------------

/// Maps a failure record to the `OpenAI` `error.type`; unrecognized records
/// yield `"server_error"`. Upstream-responsible failures report
/// `server_error` regardless of code — upstream packs internal faults into
/// fixable codes, and mapping by code would blame the caller.
pub fn openai_error_type(failure: &Failure) -> &'static str {
    if !failure.upstream_fault {
        let mapped = match failure.code.as_str() {
            "invalid_argument" | "failed_precondition" | "out_of_range" | "unimplemented" => {
                Some("invalid_request_error")
            }
            "unauthenticated" => Some("authentication_error"),
            // Devin folds content-policy blocks, invalid model UIDs and
            // unauthorized models into permission_denied; all are
            // caller-fixable request errors.
            "permission_denied" => Some("invalid_request_error"),
            "not_found" => Some("not_found_error"),
            "resource_exhausted" => Some("rate_limit_error"),
            "deadline_exceeded" => Some("timeout_error"),
            "unavailable" | "internal" | "unknown" => Some("server_error"),
            _ => None,
        };
        if let Some(mapped) = mapped {
            return mapped;
        }
    }
    "server_error"
}

/// Maps a failure record to the Anthropic `error.type`; unrecognized
/// records yield `"api_error"`. `upstream_fault` overrides the code mapping
/// the same way as [`openai_error_type`].
pub fn anthropic_error_type(failure: &Failure) -> &'static str {
    if !failure.upstream_fault {
        let mapped = match failure.code.as_str() {
            "invalid_argument" | "failed_precondition" | "out_of_range" | "unimplemented" => {
                Some("invalid_request_error")
            }
            "unauthenticated" => Some("authentication_error"),
            "permission_denied" => Some("invalid_request_error"),
            "not_found" => Some("not_found_error"),
            "resource_exhausted" => Some("rate_limit_error"),
            "deadline_exceeded" => Some("timeout_error"),
            "unavailable" | "internal" | "unknown" => Some("api_error"),
            _ => None,
        };
        if let Some(mapped) = mapped {
            return mapped;
        }
    }
    "api_error"
}

/// HTTP status for a failure record. The same mapping feeds both the
/// response-line status and the `status` field of streaming error events:
/// downstream gateways separate request-level errors (4xx, no channel
/// cooldown) from channel faults; context overflow gets 413 with
/// `error.code` so gateways classify it as a client problem. Client cancel
/// maps to 499 (nginx convention), timeout to 504 — counting a client
/// disconnect as 502 would pollute metrics and make gateways misjudge the
/// channel. `canceled` also covers this end aborting the upstream context
/// (panel abort) — again 499, the channel is not at fault. Upstream-
/// responsible faults (transport breaks, internal errors disguised in
/// fixable codes) return 502 — that is what they are. Unrecognized errors
/// return 502, an upstream service fault.
pub fn http_status(failure: &Failure) -> u16 {
    if failure.canceled {
        return 499;
    }
    if failure.timeout {
        return 504;
    }
    if failure.context_length {
        return 413;
    }
    if failure.upstream_fault {
        return 502;
    }
    if failure.client_fixable {
        // permission_denied normalizes to 400 rather than 403: downstream
        // gateways cool down per model scope on 4xx instead of marking the
        // whole channel dead.
        return 400;
    }
    match failure.code.as_str() {
        "unauthenticated" => 401,
        "not_found" => 404,
        "resource_exhausted" => 429,
        _ => 502,
    }
}

/// The error object's `code` field: context overflow is uniformly
/// `"context_length_exceeded"` (OpenAI/Anthropic convention, and lets
/// gateways see a request-level problem rather than a channel fault);
/// rate limiting gives `"rate_limit_exceeded"` — Codex only files an
/// in-stream error under its `RateLimitExceeded` retry class when
/// `error.code` is that value (codex-rs sse/responses.rs). Everything else
/// is `null`.
pub fn error_code(failure: &Failure) -> Value {
    if failure.context_length {
        return Value::String("context_length_exceeded".to_string());
    }
    if failure.rate_limited {
        return Value::String("rate_limit_exceeded".to_string());
    }
    if failure.code == "message_too_big" {
        return Value::String("message_too_big".to_string());
    }
    Value::Null
}

/// Appends the Codex-parseable wait hint `" (try again in Ns)"` to a
/// rate-limit message: codex-rs only sleeps until the server-suggested
/// instant when `error.code == "rate_limit_exceeded"` and the message
/// matches `/try again in N(s|ms|seconds)/` (sse/responses.rs
/// `try_parse_retry_after`); otherwise it falls back to a local ~200ms
/// exponential backoff that burns the retry budget inside a minute-scale
/// limit episode. The wait comes from the same source as the unified-reset
/// header (minute hints align up to the bucket boundary). Errors with no
/// hint or an already-past reset return the message unchanged.
pub fn retry_after_hint(failure: &Failure, now: SystemTime) -> String {
    let message = failure.to_string();
    let Some(reset_at) = failure.rate_limit_reset(now) else {
        return message;
    };
    // f64→i64 truncation is the intended ceil-then-floor; sub-second
    // remainders round up to the next whole second like Go's math.Ceil.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let wait = reset_at
        .duration_since(now)
        .map_or(0, |left| left.as_secs_f64().ceil() as i64);
    if wait <= 0 {
        return message;
    }
    format!("{message} (try again in {wait}s)")
}

/// Upstream diagnostic fields to merge into the error object:
/// `upstream_trace_id` (support anchor) and `retry_after` (rate-limit reset
/// seconds hint).
pub fn upstream_error_details(failure: &Failure) -> Map<String, Value> {
    let mut details = Map::new();
    if !failure.trace_id.is_empty() {
        details.insert(
            "upstream_trace_id".to_string(),
            Value::String(failure.trace_id.clone()),
        );
    }
    if failure.retry_after_seconds > 0 {
        details.insert(
            "retry_after".to_string(),
            Value::Number(failure.retry_after_seconds.into()),
        );
    }
    details
}

/// Shared prelude of the three streaming failure frames: takes the
/// classified record from the terminal event, appends the "try again in Ns"
/// wait hint to rate-limit messages (the fallback covers an empty error),
/// and produces the [`build_error_payload`] result plus the matching HTTP
/// status. `openai` selects the `OpenAI` dialect (error.type naming +
/// `"param":null`); `false` selects the Anthropic dialect. Each encoder
/// only packs the payload into its own wire frame.
pub fn stream_error(event: &ResponseEvent, fallback_message: &str, openai: bool) -> (Value, u16) {
    let failure = failure_of(event.error.as_ref());
    let mut message = fallback_message.to_string();
    if !failure.to_string().is_empty() {
        message = retry_after_hint(&failure, SystemTime::now());
    }
    let (error_type, openai_param) = if openai {
        (openai_error_type(&failure), true)
    } else {
        (anthropic_error_type(&failure), false)
    };
    let debug_ref = event
        .error
        .as_ref()
        .map_or("", |error| error.debug_ref.as_str());
    (
        build_error_payload(&message, &failure, error_type, debug_ref, openai_param),
        http_status(&failure),
    )
}

/// Assembles the `error` field of a protocol error object: the
/// message/type/code triple, OpenAI-style `"param":null` when
/// `openai_param`, then the upstream diagnostic fields
/// (`upstream_trace_id`/`retry_after`) and the debug-directory reference
/// `debug_ref`. All three protocols share this field list so the shapes do
/// not drift. `message` is the full text sent down (possibly already
/// extended by [`retry_after_hint`]).
pub fn build_error_payload(
    message: &str,
    failure: &Failure,
    error_type: &str,
    debug_ref: &str,
    openai_param: bool,
) -> Value {
    let mut payload = json!({
        "message": message,
        "type": error_type,
        "code": error_code(failure),
    });
    if openai_param {
        payload["param"] = Value::Null;
    }
    for (key, value) in upstream_error_details(failure) {
        payload[key] = value;
    }
    if !debug_ref.is_empty() {
        payload["debug_ref"] = Value::String(debug_ref.to_string());
    }
    payload
}

// ---------------------------------------------------------------------------
// content.go — ContentAt accessor
// ---------------------------------------------------------------------------

/// The content block at `index` in a partial/final message (Go
/// `common.ContentAt`): `None` when the message is absent or the index is
/// out of range; callers refine with `Content::as_thinking` /
/// `as_tool_call`.
pub fn content_at(message: Option<&AssistantMessage>, index: i32) -> Option<&Content> {
    let message = message?;
    if index < 0 {
        return None;
    }
    message
        .content
        .get(usize::try_from(index).unwrap_or(usize::MAX))
}

// ---------------------------------------------------------------------------
// Go-compatible JSON marshal
// ---------------------------------------------------------------------------

/// Serializes `value` exactly like Go's `json.Marshal`: `serde_json` emits
/// the same bytes except inside string literals, where Go additionally
/// escapes `<`, `>`, `&` (its HTML-safe default), U+2028/U+2029, and writes
/// `\u0008`/`\u000c` where serde emits `\b`/`\f`.
/// [`go_escape_strings`] rewrites serde's output to Go's byte shape so SSE
/// frames and response bodies are wire-identical, not merely semantically
/// equal. Marshal errors are impossible for the map/struct/Value shapes
/// encoded here; like Go's `data, _ := json.Marshal(...)`, a failure would
/// yield empty data.
pub fn go_marshal<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let raw = serde_json::to_vec(value).unwrap_or_default();
    go_escape_strings(&raw)
}

/// Rewrites `serde_json` output to Go `encoding/json` byte shape. Only bytes
/// inside string literals are transformed: raw `<`/`>`/`&` become
/// `\u003c`/`\u003e`/`\u0026`, the U+2028/U+2029 UTF-8 sequences become
/// `\u2028`/`\u2029`, and serde's `\b`/`\f` shorthand becomes Go's
/// `\u0008`/`\u000c`. Structural bytes and other escapes pass through
/// untouched.
fn go_escape_strings(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() + 16);
    let mut in_string = false;
    let mut i = 0;
    while i < input.len() {
        let byte = input[i];
        if !in_string {
            out.push(byte);
            if byte == b'"' {
                in_string = true;
            }
            i += 1;
            continue;
        }
        match byte {
            b'"' => {
                out.push(byte);
                in_string = false;
                i += 1;
            }
            b'\\' => {
                match input.get(i + 1) {
                    Some(b'b') => out.extend_from_slice(b"\\u0008"),
                    Some(b'f') => out.extend_from_slice(b"\\u000c"),
                    _ => out.extend_from_slice(&input[i..i + 2]),
                }
                i += 2;
            }
            b'<' => {
                out.extend_from_slice(b"\\u003c");
                i += 1;
            }
            b'>' => {
                out.extend_from_slice(b"\\u003e");
                i += 1;
            }
            b'&' => {
                out.extend_from_slice(b"\\u0026");
                i += 1;
            }
            0xE2 if input.get(i + 1) == Some(&0x80)
                && matches!(input.get(i + 2), Some(&0xA8 | &0xA9)) =>
            {
                out.extend_from_slice(if input[i + 2] == 0xA8 {
                    b"\\u2028"
                } else {
                    b"\\u2029"
                });
                i += 3;
            }
            _ => {
                out.push(byte);
                i += 1;
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Go-semantics serde helpers
// ---------------------------------------------------------------------------
// encoding/json semantics the request DTOs rely on:
//  * `"null"` into a string/bool/struct leaves the zero value (no error);
//  * `"null"` into a slice leaves nil (≈ empty);
//  * `"null"` into json.RawMessage stores the literal bytes "null";
//  * absent fields leave the zero value.
// serde's defaults differ (null into String/Vec errors; Option swallows
// null), so each Go-typed field gets a dedicated deserializer.

/// Go `string` field: absent/null/wrong-null → `""`; non-string errors.
pub fn de_go_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go `bool` field: absent/null → `false`; non-bool errors.
pub fn de_go_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go `[]T` field: absent/null → empty vec; non-array errors.
pub fn de_go_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go `struct` field: absent/null → `T::default()`; non-object errors.
pub fn de_go_value<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go `json.RawMessage` field: absent → `None`; any JSON value including
/// `null` → `Some(verbatim)` — matching Go's `len(raw) == 0` absent check
/// while keeping the literal `null` Go stores.
pub fn de_go_raw<'de, D>(deserializer: D) -> Result<Option<Box<RawValue>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // Box<RawValue> deserializes any JSON value verbatim, including null.
    Box::<RawValue>::deserialize(deserializer).map(Some)
}

/// Why [`decode_single_json`] failed — callers map each arm to their
/// protocol-specific error text.
#[derive(Debug)]
pub enum SingleJsonError {
    /// The body is not one well-formed JSON value of the expected shape.
    Syntax(serde_json::Error),
    /// Non-whitespace content follows the top-level JSON value.
    Trailing,
}

impl SingleJsonError {
    /// Maps to a [`Failure`]: syntax errors get `syntax_prefix`, trailing
    /// data gets `trailing_message` — mirroring Go's
    /// `fmt.Errorf("<prefix>: %w", err)` versus `errors.New("<trailing>")`.
    pub fn into_failure(self, syntax_prefix: &str, trailing_message: &str) -> Failure {
        match self {
            Self::Syntax(err) => Failure::plain(format!("{syntax_prefix}: {err}")),
            Self::Trailing => Failure::plain(trailing_message),
        }
    }
}

/// Decodes one JSON value and rejects non-whitespace trailing data — the
/// equivalent of Go's `json.Decoder.Decode` + `decoder.More()` pair.
pub fn decode_single_json<T>(data: &[u8]) -> Result<T, SingleJsonError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = serde_json::Deserializer::from_slice(data);
    let value = T::deserialize(&mut deserializer).map_err(SingleJsonError::Syntax)?;
    deserializer.end().map_err(|_| SingleJsonError::Trailing)?;
    Ok(value)
}

// ---------------------------------------------------------------------------
// fields.go — unconsumed top-level field scan
// ---------------------------------------------------------------------------

/// Returns the request's top-level field names not in `consumed`, as
/// `"field:<name>"` markers for `RequestMessages::dropped`. Parameters with
/// no upstream wire counterpart must surface as dropped, not be swallowed
/// silently. Non-object `data` yields no markers — the caller's main decode
/// path reports the syntax error.
pub fn unconsumed_fields(data: &[u8], consumed: &BTreeSet<&'static str>) -> Vec<String> {
    // IgnoredAny values: only the key set matters; serde_json has no
    // set-shaped container for object keys.
    #[allow(clippy::zero_sized_map_values)]
    let Ok(fields) = serde_json::from_slice::<BTreeMap<String, serde::de::IgnoredAny>>(data) else {
        return Vec::new();
    };
    fields
        .keys()
        .filter(|name| !consumed.contains(name.as_str()))
        .map(|name| format!("field:{name}"))
        .collect()
}

// ---------------------------------------------------------------------------
// toolchoice.go — tool_choice / parallel_tool_calls parsing
// ---------------------------------------------------------------------------

/// Parses the OpenAI-style `tool_choice` field: `"auto"`/`"none"`/
/// `"required"` strings, or `{"type":"function","function":{"name":X}}`
/// objects (the Responses API's flat `{"type":"function","name":X}` shape is
/// accepted too). Empty input and `"auto"` return `None` (model's choice,
/// same as absent). Non-function object shapes (`file_search/mcp`/
/// `allowed_tools` hosted-tool constraints) have no upstream counterpart:
/// recorded as dropped and passed through as auto — those constraints point
/// at tools already dropped from `tools`, so failing the whole request over
/// an unsatisfiable mandate is pointless.
pub fn parse_openai_tool_choice(
    raw: Option<&RawValue>,
    dropped: &mut Vec<String>,
) -> Result<Option<ToolChoice>, Failure> {
    #[derive(Deserialize)]
    struct ChoiceObject {
        #[serde(rename = "type", default, deserialize_with = "de_go_string")]
        kind: String,
        #[serde(default, deserialize_with = "de_go_string")]
        name: String,
        #[serde(default, deserialize_with = "de_go_value")]
        function: ChoiceFunction,
    }
    #[derive(Default, Deserialize)]
    struct ChoiceFunction {
        #[serde(default, deserialize_with = "de_go_string")]
        name: String,
    }
    let Some(raw) = raw else {
        return Ok(None);
    };
    let text = raw.get().trim();
    if text.is_empty() || text == "null" {
        return Ok(None);
    }
    if let Ok(name) = serde_json::from_str::<String>(text) {
        return match name.as_str() {
            "" | "auto" => Ok(None),
            "none" => Ok(Some(ToolChoice {
                mode: ToolChoiceMode::None,
                tool_name: String::new(),
            })),
            "required" => Ok(Some(ToolChoice {
                mode: ToolChoiceMode::Required,
                tool_name: String::new(),
            })),
            _ => Err(Failure::plain(format!("unsupported tool_choice {name:?}"))),
        };
    }
    let object: ChoiceObject = serde_json::from_str(text)
        .map_err(|err| Failure::plain(format!("decode tool_choice: {err}")))?;
    if !object.kind.is_empty() && object.kind != "function" {
        dropped.push(format!("tool_choice:{}", object.kind));
        return Ok(None);
    }
    let tool_name = if object.name.is_empty() {
        object.function.name
    } else {
        object.name
    };
    if tool_name.is_empty() {
        return Err(Failure::plain(
            "tool_choice object requires a function name",
        ));
    }
    Ok(Some(ToolChoice {
        mode: ToolChoiceMode::Named,
        tool_name,
    }))
}

/// Parses the Anthropic-style `tool_choice` object:
/// `{"type":"auto"|"any"|"tool"|"none", "name":X,
/// "disable_parallel_tool_use":bool}`.
/// Anthropic's `"any"` (some tool must be called) normalizes to
/// `ToolChoiceMode::Required` — the Devin upstream's `option_name` rejects
/// `"any"` (observed `invalid_argument`). The second return value is
/// `disable_parallel_tool_use`; the non-canonical
/// `"disable_parallel_tool_calls"` spelling was historically parsed by this
/// proxy, so both keys are accepted (either true disables).
pub fn parse_anthropic_tool_choice(
    raw: Option<&RawValue>,
) -> Result<(Option<ToolChoice>, bool), Failure> {
    #[derive(Deserialize)]
    struct ChoiceObject {
        #[serde(rename = "type", default, deserialize_with = "de_go_string")]
        kind: String,
        #[serde(default, deserialize_with = "de_go_string")]
        name: String,
        #[serde(default, deserialize_with = "de_go_bool")]
        disable_parallel_tool_use: bool,
        #[serde(default, deserialize_with = "de_go_bool")]
        disable_parallel_tool_calls: bool,
    }
    let Some(raw) = raw else {
        return Ok((None, false));
    };
    let text = raw.get().trim();
    if text.is_empty() || text == "null" {
        return Ok((None, false));
    }
    let object: ChoiceObject = serde_json::from_str(text)
        .map_err(|err| Failure::plain(format!("decode tool_choice: {err}")))?;
    let choice = match object.kind.as_str() {
        "" | "auto" => None,
        "any" => Some(ToolChoice {
            mode: ToolChoiceMode::Required,
            tool_name: String::new(),
        }),
        "tool" => {
            if object.name.is_empty() {
                return Err(Failure::plain("tool_choice type=tool requires a name"));
            }
            Some(ToolChoice {
                mode: ToolChoiceMode::Named,
                tool_name: object.name,
            })
        }
        "none" => Some(ToolChoice {
            mode: ToolChoiceMode::None,
            tool_name: String::new(),
        }),
        other => {
            return Err(Failure::plain(format!(
                "unsupported tool_choice type {other:?}"
            )));
        }
    };
    Ok((
        choice,
        object.disable_parallel_tool_use || object.disable_parallel_tool_calls,
    ))
}

/// Normalizes tool-call argument bodies replayed from history: empty/null
/// collapses to `{}` (upstream only accepts JSON objects); non-JSON-object
/// verbatim text (malformed JSON, scalars) is marked custom and travels the
/// Custom channel losslessly — collapsing it to `{}` would silently empty
/// the call semantics upstream sees.
///
/// Returns `(arguments, custom)`.
pub fn normalize_tool_arguments(raw: &str) -> (String, bool) {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return ("{}".to_string(), false);
    }
    if !is_json_object(raw) {
        return (raw.to_string(), true);
    }
    (raw.to_string(), false)
}

// ---------------------------------------------------------------------------
// content.go — content-part and image decoding
// ---------------------------------------------------------------------------

/// Decodes a JSON string or part array into intermediate content blocks.
/// Parts dropped/downgraded during decode are recorded in `dropped`
/// (`"content_part:<type>"`), which callers feed to
/// `RequestMessages::dropped` — the silent side of "decode is filter" must
/// be observable.
pub fn decode_content(raw: &str, dropped: &mut Vec<String>) -> Result<Vec<Content>, Failure> {
    // Go's json.Unmarshal accepts `null` into a string (leaving ""), so a
    // literal null content decodes to one empty text block.
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw) {
        return Ok(vec![Content::Text(TextContent {
            text: text.unwrap_or_default(),
        })]);
    }
    let parts: Vec<Box<RawValue>> = serde_json::from_str(raw).map_err(|err| {
        Failure::invalid_argument(format!("decode message content: {err}")).with_cause(err)
    })?;
    let mut content = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        #[derive(Default, Deserialize)]
        struct PartHeader {
            #[serde(rename = "type", default, deserialize_with = "de_go_string")]
            kind: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
        }
        // Go's json.Unmarshal leaves a zero struct on `null` — the part
        // then falls into the unknown-type arm instead of erroring.
        let header: PartHeader = serde_json::from_str::<Option<PartHeader>>(part.get())
            .map_err(|err| {
                Failure::invalid_argument(format!("content[{index}]: {err}")).with_cause(err)
            })?
            .unwrap_or_default();
        match header.kind.as_str() {
            "input_text" | "output_text" | "text" => {
                content.push(Content::Text(TextContent { text: header.text }));
            }
            "input_image" | "image_url" | "image" => {
                let image = decode_image_part(part.get()).map_err(|err| {
                    // Go's errors.As split: a *Failure keeps its code, a
                    // plain error becomes invalid_argument.
                    let mut failure = err.prefixed(format_args!("content[{index}]"));
                    if failure.code.is_empty() {
                        failure.code = "invalid_argument".to_string();
                    }
                    failure
                })?;
                content.push(Content::Image(image));
            }
            "input_file" | "file" | "document" | "input_audio" => {
                // Document/audio parts have no upstream channel and their
                // content is necessarily lost; silent dropping would let the
                // model answer without context and nobody would notice — a
                // placeholder text at least makes the omission visible.
                dropped.push(format!("content_part:{}", header.kind));
                content.push(Content::Text(TextContent {
                    text: format!("[content omitted: {} part not supported]", header.kind),
                }));
            }
            _ => {
                // Unknown parts are ignored so extra IDE fields do not fail
                // the whole request.
                dropped.push(format!("content_part:{}", header.kind));
            }
        }
    }
    Ok(content)
}

/// Error from image-value decoding: `Shape` is the internal control-flow
/// sentinel for "unrecognized image value shape" (Go's `errImageShape`),
/// never client-facing; `Failure` carries the real rejections.
#[derive(Debug)]
pub enum ImageValueError {
    /// Unrecognized image value shape.
    Shape,
    /// A classified rejection (or a plain JSON error with empty code).
    Failure(Failure),
}

impl fmt::Display for ImageValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape => f.write_str("unrecognized image value shape"),
            Self::Failure(failure) => fmt::Display::fmt(failure, f),
        }
    }
}

impl std::error::Error for ImageValueError {}

impl From<ImageValueError> for Failure {
    fn from(err: ImageValueError) -> Self {
        match err {
            ImageValueError::Shape => Failure::plain("unrecognized image value shape"),
            ImageValueError::Failure(failure) => failure,
        }
    }
}

/// Decodes the common image-part shapes of `OpenAI` Responses / Chat
/// Completions / Anthropic.
pub fn decode_image_part(raw: &str) -> Result<ImageContent, Failure> {
    #[derive(Default, Deserialize)]
    struct Envelope {
        // `type` is declared but unused in Go too — the part kind was
        // already dispatched by the caller.
        #[serde(rename = "image_url", default, deserialize_with = "de_go_raw")]
        image_url: Option<Box<RawValue>>,
        #[serde(default, deserialize_with = "de_go_raw")]
        image: Option<Box<RawValue>>,
        #[serde(default, deserialize_with = "de_go_raw")]
        source: Option<Box<RawValue>>,
        #[serde(default, deserialize_with = "de_go_string")]
        file_id: String,
        // A few clients put the data URL directly in `url` / `data`.
        #[serde(default, deserialize_with = "de_go_string")]
        url: String,
        #[serde(default, deserialize_with = "de_go_string")]
        data: String,
    }
    // Go's json.Unmarshal leaves a zero struct on `null` — the part then
    // reports the missing-image error rather than a syntax error.
    let envelope: Envelope = serde_json::from_str::<Option<Envelope>>(raw)
        .map_err(|err| Failure::plain(err.to_string()))?
        .unwrap_or_default();
    if !envelope.file_id.is_empty() {
        return Err(invalid_request(
            "file_id images are not supported; use base64 data URL in image_url",
        ));
    }
    for candidate in [&envelope.image_url, &envelope.image, &envelope.source]
        .into_iter()
        .flatten()
    {
        let text = candidate.get().trim();
        if text.is_empty() || text == "null" {
            continue;
        }
        match decode_image_value(candidate.get()) {
            Ok(image) => return Ok(image),
            Err(ImageValueError::Shape) => {}
            Err(ImageValueError::Failure(failure)) => return Err(failure),
        }
    }
    if !envelope.url.is_empty() {
        return decode_data_image(&envelope.url);
    }
    if !envelope.data.is_empty() {
        return decode_data_image(&envelope.data);
    }
    Err(invalid_request(
        "image part missing image_url/url/data (base64 data URL required)",
    ))
}

/// Parses a JSON string or image object.
pub fn decode_image_value(raw: &str) -> Result<ImageContent, ImageValueError> {
    #[derive(Deserialize)]
    struct ImageObject {
        #[serde(default, deserialize_with = "de_go_string")]
        url: String,
        #[serde(default, deserialize_with = "de_go_string")]
        data: String,
        #[serde(default, deserialize_with = "de_go_string")]
        base64: String,
        #[serde(default, deserialize_with = "de_go_string")]
        b64_json: String,
        #[serde(default, deserialize_with = "de_go_string")]
        mime_type: String,
        #[serde(default, deserialize_with = "de_go_string")]
        media_type: String,
        // anthropic source.type = base64; declared but unused in Go too.
        #[serde(default, deserialize_with = "de_go_string")]
        file_id: String,
    }
    if let Ok(as_string) = serde_json::from_str::<Option<String>>(raw) {
        return decode_data_image(&as_string.unwrap_or_default()).map_err(ImageValueError::Failure);
    }
    let object: ImageObject = serde_json::from_str(raw).map_err(|_| ImageValueError::Shape)?;
    if !object.file_id.is_empty() {
        return Err(ImageValueError::Failure(invalid_request(
            "file_id images are not supported; use base64 data URL",
        )));
    }
    if !object.url.is_empty() {
        return decode_data_image(&object.url).map_err(ImageValueError::Failure);
    }
    let mut encoded = object.data;
    if encoded.is_empty() {
        encoded = object.base64;
    }
    if encoded.is_empty() {
        encoded = object.b64_json;
    }
    if encoded.is_empty() {
        return Err(ImageValueError::Shape);
    }
    let mut mime_type = object.mime_type;
    if mime_type.is_empty() {
        mime_type = object.media_type;
    }
    if encoded.starts_with("data:") {
        return decode_data_image(&encoded).map_err(ImageValueError::Failure);
    }
    if mime_type.is_empty() {
        mime_type = sniff_image_mime(&encoded).unwrap_or_default().to_string();
    }
    if mime_type.is_empty() {
        return Err(ImageValueError::Failure(invalid_request(
            "image base64 requires mime_type/media_type or data URL prefix",
        )));
    }
    decode_raw_base64(&encoded, &mime_type).map_err(ImageValueError::Failure)
}

/// Parses a data URL or bare base64 image string.
pub fn decode_data_image(value: &str) -> Result<ImageContent, Failure> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_request("image url/data is empty"));
    }
    if value.starts_with("http://") || value.starts_with("https://") {
        return Err(invalid_request(
            "http(s) image URLs are not fetched yet; embed as data:image/<mime>;base64,<data>",
        ));
    }
    if !value.starts_with("data:") {
        // Bare base64: try magic-number sniffing.
        if let Some(mime_type) = sniff_image_mime(value) {
            return decode_raw_base64(value, mime_type);
        }
        return Err(invalid_request(
            "only data URL or raw base64 images are supported",
        ));
    }
    let Some((meta, encoded)) = value.split_once(',') else {
        return Err(invalid_request("image must be a base64 data URL"));
    };
    let meta = meta.strip_prefix("data:").unwrap_or(meta);
    // Both data:image/png;base64,xxx and
    // data:image/png;charset=utf-8;base64,xxx are allowed.
    let is_base64 = meta.contains(";base64") || !meta.contains(';');
    let mut mime_type = meta;
    if let Some(index) = mime_type.find(';') {
        mime_type = &mime_type[..index];
    }
    let mime_type = mime_type.trim();
    let mime_type = if mime_type.is_empty() {
        "image/png"
    } else {
        mime_type
    };
    if let Err(err) = parse_media_type(mime_type) {
        return Err(Failure::invalid_argument(format!(
            "invalid image MIME type: {err}"
        )));
    }
    if !is_base64 {
        return Err(invalid_request("image data URL must be base64 encoded"));
    }
    decode_raw_base64(encoded, mime_type)
}

/// Removes whitespace/newlines (some clients wrap lines).
fn strip_base64_whitespace(encoded: &str) -> String {
    encoded
        .chars()
        .filter(|c| !matches!(c, '\n' | '\r' | ' ' | '\t'))
        .collect()
}

/// Decodes a base64 string and returns the intermediate image block.
pub fn decode_raw_base64(encoded: &str, mime_type: &str) -> Result<ImageContent, Failure> {
    let encoded = encoded.trim();
    // Fast path: clean standard base64 decodes directly and the original
    // string is the upstream payload — a successful STANDARD decode proves
    // alphabet and padding legal, and re-encoding would only normalize the
    // string form (same bytes), so it is skipped.
    if let Ok(data) = STANDARD.decode(encoded)
        && !data.is_empty()
    {
        return Ok(ImageContent {
            data: encoded.to_string(),
            mime_type: mime_type.to_string(),
        });
    }
    let cleaned = strip_base64_whitespace(encoded);
    let mut data = STANDARD.decode(&cleaned);
    if let Ok(decoded) = &data
        && !decoded.is_empty()
    {
        return Ok(ImageContent {
            data: cleaned,
            mime_type: mime_type.to_string(),
        });
    }
    // URL-safe and unpadded variants: they decode but the string form must
    // normalize to standard base64.
    if data.is_err() {
        data = URL_SAFE
            .decode(&cleaned)
            .or_else(|_| STANDARD_NO_PAD.decode(&cleaned))
            .or_else(|_| URL_SAFE_NO_PAD.decode(&cleaned));
    }
    let data = data.map_err(|err| {
        Failure::invalid_argument(format!("decode image data: {err}")).with_cause(err)
    })?;
    if data.is_empty() {
        return Err(invalid_request("image data is empty"));
    }
    // Upstream takes a bare base64 string without a data: prefix.
    Ok(ImageContent {
        data: STANDARD.encode(&data),
        mime_type: mime_type.to_string(),
    })
}

/// Guesses an image MIME type from the file magic bytes behind the base64.
pub fn sniff_image_mime(encoded: &str) -> Option<&'static str> {
    // Magic detection needs only the first 12 decoded bytes (16 base64
    // chars); decode just the leading non-whitespace characters instead of
    // Map+DecodeString over a whole large image.
    let head: String = encoded
        .bytes()
        .filter(|b| !matches!(b, b'\n' | b'\r' | b' ' | b'\t'))
        .take(24)
        .map(char::from)
        .collect();
    // The head carries no padding (which only appears at the very end of the
    // full string); the raw variant tolerates non-4-aligned lengths. Short
    // strings may carry '=', so fall back to the padded engine.
    let raw = STANDARD_NO_PAD
        .decode(&head)
        .or_else(|_| STANDARD.decode(&head))
        .unwrap_or_default();
    if raw.len() < 4 {
        return None;
    }
    if raw.len() >= 3 && raw[0] == 0xff && raw[1] == 0xd8 && raw[2] == 0xff {
        return Some("image/jpeg");
    }
    if raw.len() >= 8 && raw[0] == 0x89 && raw[1] == 0x50 && raw[2] == 0x4e && raw[3] == 0x47 {
        return Some("image/png");
    }
    if raw.len() >= 6 && raw[0] == 0x47 && raw[1] == 0x49 && raw[2] == 0x46 {
        return Some("image/gif");
    }
    if raw.len() >= 12
        && raw[0] == 0x52
        && raw[1] == 0x49
        && raw[2] == 0x46
        && raw[3] == 0x46
        && raw[8] == 0x57
        && raw[9] == 0x45
        && raw[10] == 0x42
        && raw[11] == 0x50
    {
        return Some("image/webp");
    }
    None
}

/// Concatenates the text of all `TextContent` blocks.
pub fn content_text(content: &[Content]) -> String {
    let mut text = String::new();
    for block in content {
        if let Content::Text(block) = block {
            text.push_str(&block.text);
        }
    }
    text
}

/// Identifies the upstream signature regime of a replayable thinking
/// signature by content shape: `sealed.*` is the sealed format this proxy
/// has issued; a serialized Responses reasoning-item array is the openai
/// regime (the observed shape of the upstream `signature` field).
/// `signature_type` is an upstream-regime property, not an entry-protocol
/// property — cross-frontend replays must use the same test everywhere, or
/// an openai-regime signature tagged anthropic triggers upstream
/// `invalid_argument`. Other foreign opaque payloads are undecodable and
/// return `None`; the caller decides whether to drop or tag with the local
/// regime.
pub fn classify_signature_type(blob: &str) -> Option<&'static str> {
    if blob.starts_with("sealed.") {
        return Some("sealed");
    }
    if is_openai_reasoning_signature(blob) {
        return Some("openai");
    }
    None
}

/// Whether the payload is an openai-type signature — the observed shape of
/// the upstream `signature` field is a serialized Responses reasoning-item
/// array. On the way down the whole blob goes into `encrypted_content`
/// verbatim; on replay the same shape is recognized.
pub fn is_openai_reasoning_signature(blob: &str) -> bool {
    openai_reasoning_items(blob).is_some()
}

/// A serialized reasoning item projected from an openai-type signature;
/// `id` is the real upstream-assigned `rs_*` item identifier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct OpenAIReasoningItem {
    /// Item identifier.
    #[serde(default, deserialize_with = "de_go_string")]
    pub id: String,
    /// Item type (`reasoning`).
    #[serde(rename = "type", default, deserialize_with = "de_go_string")]
    pub kind: String,
}

/// Parses an openai-type signature payload; returns `None` for other shapes
/// (non-JSON-array, empty array, or first item not `reasoning`). The
/// responses frontend reuses the first item's id as the downstream item
/// identifier — matching what upstream issued.
pub fn openai_reasoning_items(blob: &str) -> Option<Vec<OpenAIReasoningItem>> {
    let trimmed = blob.trim();
    if !trimmed.starts_with('[') {
        return None;
    }
    let items: Vec<OpenAIReasoningItem> = serde_json::from_str(trimmed).ok()?;
    if items.is_empty() || items[0].kind != "reasoning" {
        return None;
    }
    Some(items)
}

/// Builds a classified record for caller-fixable request-shape errors —
/// decode-layer rejections (missing fields, unsupported `file_id`, non-base64)
/// are all request errors, not upstream faults.
fn invalid_request(message: impl Into<String>) -> Failure {
    Failure::invalid_argument(message)
}

// ---------------------------------------------------------------------------
// mime.go — Go mime.ParseMediaType disposition check
// ---------------------------------------------------------------------------

/// Port of Go's `mime.checkMediaTypeDisposition` (the part of
/// `ParseMediaType` reachable from `DecodeDataImage`, which always passes a
/// parameter-free, trimmed media type). Error strings mirror Go's sentinel
/// errors so wrapped messages stay greppable.
fn parse_media_type(mediatype: &str) -> Result<(), &'static str> {
    let lower = mediatype.to_lowercase();
    let (typ, rest) = consume_token(&lower);
    if typ.is_empty() {
        return Err("mime: no media type");
    }
    if rest.is_empty() {
        return Ok(());
    }
    let Some(rest) = rest.strip_prefix('/') else {
        return Err("mime: expected slash after first token");
    };
    let (subtype, rest) = consume_token(rest);
    if subtype.is_empty() {
        return Err("mime: expected token after slash");
    }
    if !rest.is_empty() {
        return Err("mime: unexpected content after media subtype");
    }
    Ok(())
}

/// Port of Go's `consumeToken`: splits at the first non-token byte.
fn consume_token(value: &str) -> (&str, &str) {
    for (index, byte) in value.bytes().enumerate() {
        if !is_token_char(byte) {
            return (&value[..index], &value[index..]);
        }
    }
    (value, "")
}

/// Port of Go's `isTokenChar`: RFC 2045 token bytes — US-ASCII except
/// space, CTLs and tspecials. Note `*` IS a token char (so `*/jpeg` passes
/// Go's validation).
fn is_token_char(byte: u8) -> bool {
    matches!(byte,
        b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' |
        b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' |
        b'^' | b'_' | b'`' | b'{' | b'|' | b'}' | b'~')
}

//! `OpenAI` Chat Completions API (`/v1/chat/completions`) request decoder.
//!
//! Port of `G/internal/api/openai/chat/request.go` — the decoder half; the
//! JSON/SSE encoder half lands with the output-protocol task.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::domain::{
    AssistantMessage, Content, Failure, Message, RequestMessages, TextContent, ThinkingContent,
    ToolCall, ToolChoice, ToolChoiceMode, ToolDefinition, ToolResultMessage, UserMessage,
};

use super::common::{
    content_text, de_go_bool, de_go_raw, de_go_string, de_go_value, de_go_vec, decode_content,
    decode_single_json, normalize_tool_arguments, now_unix_ms, parse_openai_tool_choice,
    unconsumed_fields,
};

/// The subset of `OpenAI` Chat Completions request fields this adapter
/// supports.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Request {
    /// Model identifier to use.
    #[serde(default, deserialize_with = "de_go_string")]
    pub model: String,
    /// Conversation messages.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub messages: Vec<ChatMessage>,
    /// Function tool definitions.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub tools: Vec<Tool>,
    /// Tool-call behavior control.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub tool_choice: Option<Box<RawValue>>,
    /// `functions` and `function_call` are the pre-2023-06 legacy
    /// function-calling shape: when `functions` and `tools` coexist both are
    /// accepted, and `function_call` only backs the tool choice when
    /// `tool_choice` is absent.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub functions: Vec<FunctionTool>,
    /// Legacy request-level function-call selector.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub function_call: Option<Box<RawValue>>,
    /// Whether a streaming response was requested.
    #[serde(default, deserialize_with = "de_go_bool")]
    pub stream: bool,
    /// Streaming extra options.
    #[serde(default)]
    pub stream_options: Option<StreamOptions>,
    /// Optional output token cap (legacy field).
    pub max_tokens: Option<i64>,
    /// Optional output token cap (takes precedence over `max_tokens`).
    pub max_completion_tokens: Option<i64>,
    /// Optional sampling temperature.
    pub temperature: Option<f64>,
    /// Optional nucleus sampling parameter.
    pub top_p: Option<f64>,
    /// Stop sequence(s): a string or a string array.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub stop: Option<Box<RawValue>>,
    /// Optional top-k sampling parameter.
    pub top_k: Option<i64>,
    /// Optional sampling seed.
    pub seed: Option<i64>,
    /// Optional caller user identifier.
    #[serde(default, deserialize_with = "de_go_string")]
    pub user: String,
    /// Optional caller cache key.
    #[serde(default, deserialize_with = "de_go_string")]
    pub prompt_cache_key: String,
    /// `false` forbids parallel tool calls.
    pub parallel_tool_calls: Option<bool>,
    /// Number of completions; only 1 is supported upstream.
    pub n: Option<i64>,
}

/// A Chat Completions message entry.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChatMessage {
    /// Message role.
    #[serde(default, deserialize_with = "de_go_string")]
    pub role: String,
    /// String or part-array content.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub content: Option<Box<RawValue>>,
    /// Assistant tool calls (also used for streaming deltas).
    #[serde(default, deserialize_with = "de_go_vec")]
    pub tool_calls: Vec<ToolCallEntry>,
    /// Call id on `role:"tool"` result messages.
    #[serde(default, deserialize_with = "de_go_string")]
    pub tool_call_id: String,
    /// Legacy (pre-2023-06) function-calling assistant call; mutually
    /// exclusive with `tool_calls`, converted to a `ToolCall` with a
    /// synthesized call id at decode time.
    #[serde(default)]
    pub function_call: Option<FunctionCall>,
    /// Function name carried by legacy `role:"function"` result messages.
    #[serde(default, deserialize_with = "de_go_string")]
    pub name: String,
    /// The DeepSeek-family / some-proxy convention field for returning
    /// thinking text; decoded into `ThinkingContent` so client-replayed
    /// history does not silently lose thinking.
    #[serde(default, deserialize_with = "de_go_string")]
    pub reasoning_content: String,
}

/// A tool call inside an assistant message.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ToolCallEntry {
    /// Call identifier.
    #[serde(default, deserialize_with = "de_go_string")]
    pub id: String,
    /// Call type (`function`).
    #[serde(rename = "type", default, deserialize_with = "de_go_string")]
    pub kind: String,
    /// The function part of the call.
    #[serde(default, deserialize_with = "de_go_value")]
    pub function: FunctionCall,
}

/// The function part of a tool call.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FunctionCall {
    /// Function name.
    #[serde(default, deserialize_with = "de_go_string")]
    pub name: String,
    /// Argument JSON text.
    #[serde(default, deserialize_with = "de_go_string")]
    pub arguments: String,
}

/// An `OpenAI` Chat function tool definition.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Tool {
    /// Tool type (`function`).
    #[serde(rename = "type", default, deserialize_with = "de_go_string")]
    pub kind: String,
    /// Function tool details.
    #[serde(default, deserialize_with = "de_go_value")]
    pub function: FunctionTool,
}

/// Function tool details.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FunctionTool {
    /// Function name.
    #[serde(default, deserialize_with = "de_go_string")]
    pub name: String,
    /// Function description.
    #[serde(default, deserialize_with = "de_go_string")]
    pub description: String,
    /// Input JSON Schema.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub parameters: Option<Box<RawValue>>,
}

/// Streaming extra options.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct StreamOptions {
    /// Whether to include usage in the stream.
    #[serde(default, deserialize_with = "de_go_bool")]
    pub include_usage: bool,
}

/// Top-level fields `decode_request` consumes; the rest
/// (`reasoning_effort`/`store`/`service_tier`/`metadata`/`response_format`
/// etc.) have no upstream counterpart and are recorded in `dropped` rather
/// than swallowed.
static CHAT_REQUEST_FIELDS: LazyLock<BTreeSet<&'static str>> = LazyLock::new(|| {
    BTreeSet::from([
        "model",
        "messages",
        "tools",
        "tool_choice",
        "functions",
        "function_call",
        "stream",
        "stream_options",
        "max_tokens",
        "max_completion_tokens",
        "temperature",
        "top_p",
        "stop",
        "top_k",
        "seed",
        "user",
        "prompt_cache_key",
        "parallel_tool_calls",
        "n",
    ])
});

/// An adapted Chat request: the intermediate request plus generation
/// options.
#[derive(Debug, Clone, Default)]
pub struct AdaptedRequest {
    /// Provider-independent full conversation context.
    pub context: RequestMessages,
    /// Protocol options needed for generation, outside the history.
    pub options: RequestOptions,
}

/// Generation-control parameters that are not part of the conversation
/// history.
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// Whether the caller requested a streaming response.
    pub stream: bool,
    /// Whether the caller asked for usage in the stream.
    pub include_usage: bool,
}

/// Converts an `OpenAI` Chat Completions JSON request to the intermediate
/// request.
///
/// `collect_dropped` controls a second full scan of the body collecting
/// top-level unconsumed fields (`field:*` markers); when false the scan is
/// skipped — `dropped`'s only reader is the debuglog request projection, so
/// with debug off the whole field tree would be wasted work. The other
/// `dropped` write sites are all low-frequency branches and are not gated.
// Mirrors the Go decodeRequest flow for parity review.
#[allow(clippy::too_many_lines)]
pub fn decode_request(data: &[u8], collect_dropped: bool) -> Result<AdaptedRequest, Failure> {
    let request: Request = decode_single_json(data).map_err(|err| {
        err.into_failure(
            "decode chat request",
            // Content after the top-level JSON means the body is not a
            // single request object — most likely a client bug or a proxy
            // mis-concatenation; silently ignoring it would mask
            // truncation/framing bugs.
            "chat request has trailing data after JSON body",
        )
    })?;
    if request.model.is_empty() {
        return Err(Failure::plain("chat request model is required"));
    }
    if request.messages.is_empty() {
        return Err(Failure::plain("chat request messages are required"));
    }

    let mut context = RequestMessages {
        model: request.model,
        ..RequestMessages::default()
    };
    if collect_dropped {
        context
            .dropped
            .extend(unconsumed_fields(data, &CHAT_REQUEST_FIELDS));
    }
    // max_completion_tokens takes precedence over max_tokens (OpenAI
    // semantics); silently dropping a non-positive selected value would let
    // the caller believe the cap took effect — record it in `dropped`.
    let (max_tokens_value, dropped_max_tokens) = match request.max_completion_tokens {
        Some(value) => (Some(value), "field:max_completion_tokens"),
        None => (request.max_tokens, "field:max_tokens"),
    };
    if let Some(value) = max_tokens_value {
        if value > 0 {
            context.max_tokens = Some(value);
        } else {
            context.dropped.push(dropped_max_tokens.to_string());
        }
    }
    context.temperature = request.temperature;
    context.top_p = request.top_p;
    if let Some(top_k) = request.top_k {
        if top_k > 0 {
            context.top_k = Some(top_k);
        } else {
            context.dropped.push("field:top_k".to_string());
        }
    }
    context.seed = request.seed;
    // The upstream CASCADE channel only supports a single completion:
    // num_completions>1 crashes the stream midway — an early local reject
    // is more readable than hitting upstream.
    if request.n.is_some_and(|n| n > 1) {
        return Err(Failure::plain(
            "chat request n > 1 is not supported by this provider",
        ));
    }
    context.tool_choice =
        parse_openai_tool_choice(request.tool_choice.as_deref(), &mut context.dropped)?;
    // The request-level function_call is tool_choice's legacy predecessor
    // ("auto"/"none" strings or a {"name":X} object); tool_choice wins when
    // present.
    if context.tool_choice.is_none() {
        match parse_legacy_function_call(request.function_call.as_deref()) {
            Ok(choice) => context.tool_choice = choice,
            Err(()) => context.dropped.push("field:function_call".to_string()),
        }
    }
    if request.parallel_tool_calls == Some(false) {
        context.disable_parallel_tool_calls = true;
    }
    if let Some(stop) = &request.stop {
        let trimmed = stop.get().trim();
        if !trimmed.is_empty() && trimmed != "null" {
            let stops: Option<Vec<String>> = serde_json::from_str(trimmed).ok().or_else(|| {
                serde_json::from_str::<String>(trimmed)
                    .ok()
                    .filter(|single| !single.is_empty())
                    .map(|single| vec![single])
            });
            match stops {
                // A stop of unrecognized shape (number/object) does not take
                // effect and must not be swallowed silently.
                None => context.dropped.push("field:stop".to_string()),
                Some(stops) => context.stop_sequences = stops,
            }
        }
    }
    context.session_key = request.prompt_cache_key;
    if context.session_key.is_empty() {
        context.session_key = request.user;
    }
    // call_ids registers every issued call id (real tool_call ids and the
    // legacy function_call's synthesized ids): function_call has no id
    // field, so a call_function_N ordinal id is minted — but only after
    // confirming it does not collide with an existing id. function_ids is
    // the legacy shape's name→synthesized-id map: role:"function" result
    // messages reconcile by function name, not call id.
    let mut call_ids = HashSet::new();
    let mut function_ids = HashMap::new();
    append_messages(
        &mut context,
        &request.messages,
        &mut call_ids,
        &mut function_ids,
    )?;
    for tool in &request.tools {
        if tool.kind != "function" {
            context.dropped.push(format!("tool:{}", tool.kind));
            continue;
        }
        let schema = tool
            .function
            .parameters
            .as_deref()
            .map_or_else(|| "{\"type\":\"object\"}".to_string(), RawValue::to_string);
        context.tools.push(ToolDefinition {
            name: tool.function.name.clone(),
            description: tool.function.description.clone(),
            input_schema: schema,
            custom: false,
        });
    }
    // functions is the legacy tool-declaration shape: same structure as
    // tools, merged straight in.
    for function in &request.functions {
        let schema = function
            .parameters
            .as_deref()
            .map_or_else(|| "{\"type\":\"object\"}".to_string(), RawValue::to_string);
        context.tools.push(ToolDefinition {
            name: function.name.clone(),
            description: function.description.clone(),
            input_schema: schema,
            custom: false,
        });
    }
    // Adjacent assistant turns merge first (same IR-layer shared
    // implementation as the responses/anthropic faces): fake turn
    // boundaries on the wire raise the premature-EOS probability.
    context.merge_adjacent_assistant_turns();
    // Orphan tool results demote to USER text uniformly before IR
    // validation — validation requires a non-empty call id, and an orphan's
    // call id is missing by definition.
    context.demote_orphan_tool_results();
    if let Err(err) = context.validate() {
        return Err(Failure::invalid_argument(format!(
            "validate adapted request: {err}"
        )));
    }

    Ok(AdaptedRequest {
        context,
        options: RequestOptions {
            stream: request.stream,
            include_usage: request
                .stream_options
                .is_some_and(|options| options.include_usage),
        },
    })
}

/// Decodes the messages array one by one, appending in order.
fn append_messages(
    context: &mut RequestMessages,
    messages: &[ChatMessage],
    call_ids: &mut HashSet<String>,
    function_ids: &mut HashMap<String, String>,
) -> Result<(), Failure> {
    for (index, message) in messages.iter().enumerate() {
        append_message(context, message, call_ids, function_ids)
            .map_err(|err| err.prefixed(format_args!("message[{index}]")))?;
    }
    Ok(())
}

/// Decodes one message into the conversation by role.
fn append_message(
    context: &mut RequestMessages,
    message: &ChatMessage,
    call_ids: &mut HashSet<String>,
    function_ids: &mut HashMap<String, String>,
) -> Result<(), Failure> {
    match message.role.as_str() {
        "system" | "developer" => {
            let blocks = match &message.content {
                Some(raw) => decode_content(raw.get(), &mut context.dropped)?,
                // Go's json.Unmarshal(nil) errors — absent content is a
                // decode error, unlike explicit null which yields one empty
                // text block.
                None => {
                    return Err(Failure::invalid_argument(
                        "decode message content: unexpected end of JSON input",
                    ));
                }
            };
            if blocks.is_empty() {
                // Content present but no decodable blocks (empty array / all
                // parts unrecognized): contributes nothing to the system
                // prompt — record `dropped` so the gap reconciles, same
                // convention as the anthropic/responses faces.
                context
                    .dropped
                    .push(format!("empty_message:{}", message.role));
            }
            let text = content_text(&blocks);
            if !context.system_prompt.is_empty() && !text.is_empty() {
                context.system_prompt.push('\n');
            }
            context.system_prompt.push_str(&text);
        }
        "user" => {
            let blocks = decode_user_content(context, message.content.as_deref())?;
            context.messages.push(Message::User(UserMessage {
                content: blocks,
                timestamp_ms: now_unix_ms(),
            }));
        }
        "assistant" => {
            let blocks = decode_assistant_content(context, message, call_ids, function_ids)?;
            if blocks.is_empty() {
                // No content, no tool_calls/function_call/reasoning: the
                // assistant message contributed nothing and the wire side
                // drops it as DroppedEmptyAssistant — record `dropped` to
                // match the anthropic convention.
                context.dropped.push("empty_message:assistant".to_string());
            }
            context.messages.push(Message::Assistant(AssistantMessage {
                content: blocks,
                timestamp_ms: now_unix_ms(),
                ..AssistantMessage::default()
            }));
        }
        "tool" => {
            // Results with a missing call id or one matching no earlier
            // call enter the IR as-is; `demote_orphan_tool_results` at the
            // decode tail demotes them to USER text uniformly.
            let blocks = match &message.content {
                Some(raw) => decode_content(raw.get(), &mut context.dropped)?,
                None => {
                    return Err(Failure::invalid_argument(
                        "decode message content: unexpected end of JSON input",
                    ));
                }
            };
            context
                .messages
                .push(Message::ToolResult(ToolResultMessage {
                    tool_call_id: message.tool_call_id.clone(),
                    content: blocks,
                    is_error: false,
                    timestamp_ms: now_unix_ms(),
                }));
        }
        "function" => {
            // Legacy tool result: no call id — reconcile by name against the
            // corresponding function_call's synthesized id; a miss means the
            // history has no such call, so mint an orphan id and let
            // `demote_orphan_tool_results` demote it to text instead of
            // failing the whole request.
            let blocks = match &message.content {
                Some(raw) => decode_content(raw.get(), &mut context.dropped)?,
                None => {
                    return Err(Failure::invalid_argument(
                        "decode message content: unexpected end of JSON input",
                    ));
                }
            };
            let id = if let Some(id) = function_ids.get(&message.name) {
                id.clone()
            } else {
                context
                    .dropped
                    .push(format!("unmatched_function_name:{}", message.name));
                format!("call_function_unmatched_{}", message.name)
            };
            context
                .messages
                .push(Message::ToolResult(ToolResultMessage {
                    tool_call_id: id,
                    content: blocks,
                    is_error: false,
                    timestamp_ms: now_unix_ms(),
                }));
        }
        other => context.dropped.push(format!("role:{other}")),
    }
    Ok(())
}

/// Decodes user message content; empty/null normalizes to an empty text
/// block. Content present but undecodable (empty array / all parts
/// unrecognized) likewise lands as an empty-text placeholder to keep the
/// turn, with `empty_message:user` recorded — same convention as the
/// anthropic face.
fn decode_user_content(
    context: &mut RequestMessages,
    raw: Option<&RawValue>,
) -> Result<Vec<Content>, Failure> {
    let Some(raw) = raw else {
        return Ok(vec![Content::Text(TextContent::default())]);
    };
    let trimmed = raw.get().trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(vec![Content::Text(TextContent::default())]);
    }
    let blocks = decode_content(raw.get(), &mut context.dropped)?;
    if blocks.is_empty() {
        context.dropped.push("empty_message:user".to_string());
        return Ok(vec![Content::Text(TextContent::default())]);
    }
    Ok(blocks)
}

/// Decodes an assistant message's body and `tool_calls` (including the legacy
/// single `function_call` field shape).
fn decode_assistant_content(
    context: &mut RequestMessages,
    message: &ChatMessage,
    call_ids: &mut HashSet<String>,
    function_ids: &mut HashMap<String, String>,
) -> Result<Vec<Content>, Failure> {
    let mut blocks = Vec::new();
    if let Some(raw) = &message.content {
        let trimmed = raw.get().trim();
        if !trimmed.is_empty() && trimmed != "null" {
            blocks.extend(decode_content(raw.get(), &mut context.dropped)?);
        }
    }
    if !message.reasoning_content.is_empty() {
        blocks.push(Content::Thinking(ThinkingContent {
            thinking: message.reasoning_content.clone(),
            ..ThinkingContent::default()
        }));
    }
    for call in &message.tool_calls {
        if !call.kind.is_empty() && call.kind != "function" {
            context.dropped.push(format!("tool_call:{}", call.kind));
            continue;
        }
        let (arguments, custom) = normalize_tool_arguments(&call.function.arguments);
        call_ids.insert(call.id.clone());
        blocks.push(Content::ToolCall(ToolCall {
            id: call.id.clone(),
            name: call.function.name.clone(),
            arguments,
            custom,
        }));
    }
    if let Some(call) = &message.function_call {
        // The legacy function_call has no call id: mint a call_function_N
        // ordinal id from the registered-call count, and record name→id in
        // function_ids for role:"function" result messages to reconcile.
        let mut index = call_ids.len();
        let call_id = loop {
            let candidate = format!("call_function_{index}");
            if !call_ids.contains(&candidate) {
                break candidate;
            }
            index += 1;
        };
        call_ids.insert(call_id.clone());
        function_ids.insert(call.name.clone(), call_id.clone());
        let (arguments, custom) = normalize_tool_arguments(&call.arguments);
        blocks.push(Content::ToolCall(ToolCall {
            id: call_id,
            name: call.name.clone(),
            arguments,
            custom,
        }));
    }
    Ok(blocks)
}

/// Parses the request-level legacy `function_call` field: `"auto"`/`"none"`
/// strings or a `{"name":X}` object; empty/null input returns `Ok(None)`
/// for "field absent", other unrecognized shapes return `Err(())` so the
/// caller records a dropped marker.
fn parse_legacy_function_call(raw: Option<&RawValue>) -> Result<Option<ToolChoice>, ()> {
    #[derive(Default, Deserialize)]
    struct NamedCall {
        #[serde(default, deserialize_with = "de_go_string")]
        name: String,
    }
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.get().trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(None);
    }
    if let Ok(mode) = serde_json::from_str::<String>(trimmed) {
        return match mode.as_str() {
            "" | "auto" => Ok(None),
            "none" => Ok(Some(ToolChoice {
                mode: ToolChoiceMode::None,
                tool_name: String::new(),
            })),
            _ => Err(()),
        };
    }
    // Go's json.Unmarshal leaves a zero struct on `null` (already excluded
    // above) and errors on non-objects.
    if let Ok(named) = serde_json::from_str::<Option<NamedCall>>(trimmed)
        && let Some(named) = named
        && !named.name.is_empty()
    {
        return Ok(Some(ToolChoice {
            mode: ToolChoiceMode::Named,
            tool_name: named.name,
        }));
    }
    Err(())
}

// ===========================================================================
// response.go — final JSON and SSE chunk encoding
// ===========================================================================

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::{ResponseEvent, ResponseEventType, StopReason, Usage};
use crate::randid;

use bytes::BytesMut;

use super::common::{
    SSE_DONE, SseFrame, append_data_frame, go_marshal, go_marshal_into, now_unix_secs,
    stream_error, usage_totals,
};

/// Per-request Chat Completions SSE encoding state.
pub struct StreamEncoder {
    model: String,
    response_id: String,
    created_at: i64,
    include_usage: bool,
    /// `text_started`/`thinking_started` record block open/close by llm
    /// content index: same-kind blocks at different indices can interleave
    /// (text between thinking blocks), and a single bool would misjudge the
    /// second block's delta as "not started".
    text_started: HashMap<i32, bool>,
    thinking_started: HashMap<i32, bool>,
    tool_calls: Vec<ToolCallState>,
    /// `tool_by_content` indexes tool state by llm content index. The content
    /// index is the global block index into `partial.content` (text,
    /// thinking and tool calls interleaved) — a different numbering than
    /// the `tool_calls` output ordinal `state.index`; lookups must go
    /// through this table, not compare ordinals against indices.
    tool_by_content: HashMap<i32, usize>,
    finished: bool,
    final_usage: Usage,
}

struct ToolCallState {
    index: usize,
    id: String,
    name: String,
}

impl StreamEncoder {
    /// Creates encoding state for one Chat Completions stream.
    pub fn new(model: &str, include_usage: bool) -> Self {
        Self {
            model: model.to_string(),
            response_id: randid::prefixed("chatcmpl-"),
            created_at: now_unix_secs(),
            include_usage,
            text_started: HashMap::new(),
            thinking_started: HashMap::new(),
            tool_calls: Vec::new(),
            tool_by_content: HashMap::new(),
            finished: false,
            final_usage: Usage::default(),
        }
    }

    /// Expands one intermediate response event into zero or more ordered
    /// Chat Completions SSE chunks, written straight into `dst`.
    pub fn encode_into(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        if let Err(err) = event.validate() {
            return Err(Failure::plain(format!("validate response event: {err}")));
        }
        if self.finished {
            return Err(Failure::plain("chat completion stream is already done"));
        }
        match event.kind {
            ResponseEventType::Start => {
                self.start(dst, frames);
                Ok(())
            }
            ResponseEventType::TextStart => {
                self.start_text(event);
                Ok(())
            }
            ResponseEventType::TextDelta => self.text_delta(event, dst, frames),
            ResponseEventType::TextEnd => self.end_text(event),
            ResponseEventType::ThinkingStart => {
                self.start_thinking(event);
                Ok(())
            }
            ResponseEventType::ThinkingDelta => self.thinking_delta(event, dst, frames),
            ResponseEventType::ThinkingEnd => self.end_thinking(event),
            ResponseEventType::ThinkingSignature => {
                // Chat Completions has no signature concept; thinking
                // signatures only affect the Anthropic/Responses shapes.
                Ok(())
            }
            ResponseEventType::ToolCallStart => {
                self.start_tool_call(event, dst, frames);
                Ok(())
            }
            ResponseEventType::ToolCallDelta => self.tool_call_delta(event, dst, frames),
            ResponseEventType::ToolCallEnd => self.end_tool_call(event),
            ResponseEventType::Done => {
                self.finish(event, dst, frames);
                Ok(())
            }
            ResponseEventType::Error => {
                self.failed(event, dst, frames);
                Ok(())
            }
        }
    }

    /// Emits the stream's first chunk: a delta carrying only
    /// `role=assistant`.
    fn start(&mut self, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        self.chunk(
            &[ChatChoice {
                delta: ChatDelta {
                    role: "assistant",
                    ..ChatDelta::default()
                },
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
    }

    /// Marks a text block open; the Chat stream has no separate block-start
    /// frame. All later block-level events (delta/end/signature) depend on
    /// the matching *_start — the decoder contract guarantees that order,
    /// and a missing one is a decoder bug, so each handler errors
    /// explicitly instead of auto-completing or silently dropping, same as
    /// the other two protocol encoders.
    fn start_text(&mut self, event: &ResponseEvent) {
        self.text_started.insert(event.content_index, true);
    }

    /// Emits one body increment.
    fn text_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        if !self.text_started.contains_key(&event.content_index) {
            return Err(Failure::plain(format!(
                "text delta at content index {} without text_start",
                event.content_index
            )));
        }
        self.chunk(
            &[ChatChoice {
                delta: ChatDelta {
                    content: event.delta.as_str(),
                    ..ChatDelta::default()
                },
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
        Ok(())
    }

    /// Closes a text block; the Chat stream has no block-end frame, only
    /// state reset.
    fn end_text(&mut self, event: &ResponseEvent) -> Result<(), Failure> {
        if !self.text_started.contains_key(&event.content_index) {
            return Err(Failure::plain(format!(
                "text end at content index {} without text_start",
                event.content_index
            )));
        }
        self.text_started.remove(&event.content_index);
        Ok(())
    }

    /// Marks a thinking block open.
    ///
    /// `OpenAI` Chat Completions has no official reasoning field; following
    /// the DeepSeek-style convention, thinking goes out as
    /// `choices[0].delta.reasoning_content`.
    fn start_thinking(&mut self, event: &ResponseEvent) {
        self.thinking_started.insert(event.content_index, true);
    }

    /// Emits one thinking increment as `reasoning_content`.
    fn thinking_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        if !self.thinking_started.contains_key(&event.content_index) {
            return Err(Failure::plain(format!(
                "thinking delta at content index {} without thinking_start",
                event.content_index
            )));
        }
        self.chunk(
            &[ChatChoice {
                delta: ChatDelta {
                    reasoning_content: event.delta.as_str(),
                    ..ChatDelta::default()
                },
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
        Ok(())
    }

    /// Closes a thinking block; only state reset.
    fn end_thinking(&mut self, event: &ResponseEvent) -> Result<(), Failure> {
        if !self.thinking_started.contains_key(&event.content_index) {
            return Err(Failure::plain(format!(
                "thinking end at content index {} without thinking_start",
                event.content_index
            )));
        }
        self.thinking_started.remove(&event.content_index);
        Ok(())
    }

    /// Registers tool-call state and emits the first `tool_calls` frame
    /// carrying id/name.
    fn start_tool_call(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) {
        let index = self.tool_calls.len();
        self.tool_calls.push(ToolCallState {
            index,
            id: event.tool_call_id.clone(),
            name: event.tool_name.clone(),
        });
        self.tool_by_content.insert(event.content_index, index);
        let state = &self.tool_calls[index];
        self.chunk(
            &[ChatChoice {
                delta: ChatDelta {
                    tool_calls: Some(&[ChatToolCall {
                        index: state.index,
                        id: state.id.as_str(),
                        kind: "function",
                        function: ChatToolCallFunction {
                            name: state.name.as_str(),
                            ..ChatToolCallFunction::default()
                        },
                    }]),
                    ..ChatDelta::default()
                },
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
    }

    /// Emits one tool-argument increment.
    fn tool_call_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let Some(state_index) = self.find_tool(&event.tool_call_id, event.content_index) else {
            return Err(Failure::plain(format!(
                "tool call delta at content index {} (call {:?}) without toolcall_start",
                event.content_index, event.tool_call_id
            )));
        };
        self.chunk(
            &[ChatChoice {
                delta: ChatDelta {
                    tool_calls: Some(&[ChatToolCall {
                        index: state_index,
                        function: ChatToolCallFunction {
                            arguments: event.delta.as_str(),
                            ..ChatToolCallFunction::default()
                        },
                        ..ChatToolCall::default()
                    }]),
                    ..ChatDelta::default()
                },
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
        Ok(())
    }

    /// Verifies the block was opened; `OpenAI` Chat Completions streaming
    /// tool calls emit no separate end chunk — `finish_reason` marks the
    /// end.
    fn end_tool_call(&mut self, event: &ResponseEvent) -> Result<(), Failure> {
        if !self.tool_by_content.contains_key(&event.content_index) {
            return Err(Failure::plain(format!(
                "tool call end at content index {} without toolcall_start",
                event.content_index
            )));
        }
        Ok(())
    }

    /// Emits the `finish_reason` chunk, the optional usage chunk and the
    /// `[DONE]` terminator.
    fn finish(&mut self, event: &ResponseEvent, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        self.finished = true;
        self.final_usage = event
            .message
            .as_ref()
            .map_or_else(Usage::default, |message| message.usage.clone());
        let reason = finish_reason(event.reason);
        self.chunk(
            &[ChatChoice {
                finish_reason: reason,
                ..ChatChoice::default()
            }],
            &Value::Null,
            dst,
            frames,
        );
        if self.include_usage {
            let usage = chat_usage(&self.final_usage);
            self.chunk(&[], &usage, dst, frames);
        }
        append_data_frame(dst, frames, SSE_DONE, SSE_DONE.as_bytes());
    }

    /// Emits one terminal chunk carrying an `error` field and closes the
    /// stream.
    fn failed(&mut self, event: &ResponseEvent, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        self.finished = true;
        // OpenAI Chat Completions has no official unified streaming error
        // format. Emitting a chat.completion.chunk with an error field lets
        // clients like openai-python raise on data.error. The top-level
        // status lets downstream gateways classify by real HTTP semantics.
        let (error_payload, status) = stream_error(event, "chat completion stream failed", true);
        let start = dst.len();
        dst.extend_from_slice(b"data: ");
        go_marshal_into(
            dst,
            &json!({
                "id": self.response_id,
                "object": "chat.completion.chunk",
                "created": self.created_at,
                "model": self.model,
                "choices": [],
                "usage": null,
                "status": status,
                "error": error_payload,
            }),
        );
        dst.extend_from_slice(b"\n\n");
        frames.push(SseFrame {
            name: "",
            data: start + "data: ".len()..dst.len() - 2,
        });
        // Trailing [DONE]: without a terminator some clients read the
        // stream tail as a transport truncation rather than a clean
        // terminal error.
        append_data_frame(dst, frames, SSE_DONE, SSE_DONE.as_bytes());
    }

    /// Matches by provider call id first; when the id is absent (upstream
    /// may not produce ids — the decoder has a separate empty-id fallback
    /// path) falls back to the content-index mapping.
    fn find_tool(&self, id: &str, content_index: i32) -> Option<usize> {
        for state in &self.tool_calls {
            if !state.id.is_empty() && state.id == id {
                return Some(state.index);
            }
        }
        self.tool_by_content.get(&content_index).copied()
    }

    /// Packs choices/usage into the fixed envelope and marshals one SSE
    /// frame straight into `dst`. `&self` (not `&mut`) so callers can
    /// borrow `tool_calls` state into the payload.
    fn chunk(
        &self,
        choices: &[ChatChoice<'_>],
        usage: &Value,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) {
        let start = dst.len();
        dst.extend_from_slice(b"data: ");
        go_marshal_into(
            dst,
            &ChatChunk {
                id: &self.response_id,
                object: "chat.completion.chunk",
                created: self.created_at,
                model: &self.model,
                choices,
                usage,
            },
        );
        dst.extend_from_slice(b"\n\n");
        frames.push(SseFrame {
            name: "",
            data: start + "data: ".len()..dst.len() - 2,
        });
    }
}

/// The fixed envelope of a streaming chunk: five keys always, only
/// choices/usage vary per event. Struct encoding replaces per-frame map
/// marshal (measured ~2.7x faster, ~5.7x fewer allocations in Go); all
/// payload fields borrow — no per-frame `String`/`Vec` copies.
#[derive(Serialize)]
struct ChatChunk<'a> {
    id: &'a str,
    object: &'static str,
    created: i64,
    model: &'a str,
    choices: &'a [ChatChoice<'a>],
    usage: &'a Value,
}

// The types below declare fields in the alphabetical order Go's map
// marshal produced, keeping output byte-identical with the old
// map[string]any encoding.

/// The single-element shape of the `choices` array: `finish_reason` is
/// always emitted (may be null), `index` is always 0.
#[derive(Serialize, Default)]
struct ChatChoice<'a> {
    delta: ChatDelta<'a>,
    finish_reason: Value,
    index: i32,
}

/// The union of all delta keys: each event fills exactly one of them.
#[derive(Serialize, Default)]
struct ChatDelta<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    content: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    reasoning_content: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    role: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<&'a [ChatToolCall<'a>]>,
}

/// An element of `delta.tool_calls`: the start event carries id/type/name,
/// argument-delta frames only `function.arguments` plus `index`.
#[derive(Serialize, Default)]
struct ChatToolCall<'a> {
    function: ChatToolCallFunction<'a>,
    #[serde(skip_serializing_if = "str::is_empty")]
    id: &'a str,
    index: usize,
    #[serde(skip_serializing_if = "str::is_empty", rename = "type")]
    kind: &'a str,
}

#[derive(Serialize, Default)]
struct ChatToolCallFunction<'a> {
    arguments: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    name: &'a str,
}

/// Encodes the final assistant message as non-streaming Chat Completions
/// JSON. `model` is echoed to the client (request text, may be an alias);
/// empty falls back to the upstream-declared actual uid then the resolved
/// request uid.
pub fn encode_response(
    message: Option<&AssistantMessage>,
    model: &str,
) -> Result<Vec<u8>, Failure> {
    let Some(message) = message else {
        return Err(Failure::plain("response message is nil"));
    };
    let mut model = model;
    if model.is_empty() {
        model = &message.response_model;
    }
    if model.is_empty() {
        model = &message.model;
    }
    if model.is_empty() {
        model = "devin";
    }
    let message_obj = message_to_chat(message);
    let response = json!({
        "id": randid::prefixed("chatcmpl-"),
        "object": "chat.completion",
        "created": now_unix_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message_obj,
            "finish_reason": finish_reason(message.stop_reason),
        }],
        "usage": chat_usage(&message.usage),
    });
    Ok(go_marshal(&response))
}

/// Projects the final message into a chat `message` object plus the
/// `tool_calls` array.
fn message_to_chat(message: &AssistantMessage) -> Value {
    let mut text_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut tool_calls = Vec::new();
    for block in &message.content {
        match block {
            Content::Text(content) => text_parts.push(content.text.as_str()),
            // In non-streaming mode thinking goes separately into
            // reasoning_content; the body holds only text.
            Content::Thinking(content) => reasoning_parts.push(content.thinking.as_str()),
            Content::ToolCall(content) => tool_calls.push(json!({
                "id": content.id,
                "type": "function",
                "function": {"name": content.name, "arguments": content.arguments},
            })),
            Content::Image(_) => {}
        }
    }
    let mut message_obj = json!({
        "role": "assistant",
        "content": text_parts.concat(),
    });
    if !reasoning_parts.is_empty() {
        message_obj["reasoning_content"] = Value::String(reasoning_parts.concat());
    }
    if !tool_calls.is_empty() {
        message_obj["tool_calls"] = Value::Array(tool_calls);
        // content and tool_calls may coexist: keep the body when there is
        // one; only a pure call turn gets null.
        if text_parts.is_empty() {
            message_obj["content"] = Value::Null;
        }
    }
    message_obj
}

/// Projects the Chat Completions usage shape, including cache and
/// reasoning details.
fn chat_usage(usage: &Usage) -> Value {
    let (input_tokens, total) = usage_totals(usage);
    let mut result = json!({
        "prompt_tokens": input_tokens,
        "completion_tokens": usage.output,
        "total_tokens": total,
        "prompt_tokens_details": {
            "cached_tokens": usage.cache_read,
            "cache_write_tokens": usage.cache_write,
        },
    });
    // `reasoning` absent means upstream did not report the reasoning
    // subset; always emitting 0 would fake "unknown" into "no reasoning"
    // and skew downstream reasoning-share stats.
    if let Some(reasoning) = usage.reasoning {
        result["completion_tokens_details"] = json!({"reasoning_tokens": reasoning});
    }
    result
}

/// Maps the Chat Completions `finish_reason` enum.
fn finish_reason(reason: Option<StopReason>) -> Value {
    match reason {
        Some(StopReason::ToolUse) => Value::String("tool_calls".to_string()),
        Some(StopReason::Length) => Value::String("length".to_string()),
        Some(StopReason::Stop | StopReason::StopSequence) => Value::String("stop".to_string()),
        Some(StopReason::ContentFilter | StopReason::Error | StopReason::Aborted) => {
            Value::String("content_filter".to_string())
        }
        _ => Value::Null,
    }
}

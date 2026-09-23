//! `OpenAI` Responses API (`/v1/responses`) request decoder.
//!
//! Port of `G/internal/api/openai/responses/request.go` — the decoder half;
//! the JSON/SSE encoder half lands with the output-protocol task.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::domain::{
    AssistantMessage, Content, Failure, Message, RequestMessages, StopReason, TextContent,
    ThinkingContent, ToolCall, ToolDefinition, ToolResultMessage, UserMessage,
};

use super::common::{
    classify_signature_type, content_text, de_go_bool, de_go_raw, de_go_string, de_go_vec,
    decode_content, decode_single_json, normalize_tool_arguments, now_unix_ms,
    parse_openai_tool_choice, unconsumed_fields,
};

/// The subset of `OpenAI` Responses request fields this adapter supports.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Request {
    /// Model identifier to use.
    #[serde(default, deserialize_with = "de_go_string")]
    pub model: String,
    /// System prompt kept separate from `input`.
    #[serde(default, deserialize_with = "de_go_string")]
    pub instructions: String,
    /// A string or an array of Responses input items.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub input: Option<Box<RawValue>>,
    /// `OpenAI` function tool definitions.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub tools: Vec<Tool>,
    /// Whether a streaming response was requested.
    #[serde(default, deserialize_with = "de_go_bool")]
    pub stream: bool,
    /// Optional output token cap.
    pub max_output_tokens: Option<i64>,
    /// Optional sampling temperature.
    pub temperature: Option<f64>,
    /// Caller-provided upstream response association. This proxy has no
    /// server-side response store (`store=false`), so a non-empty value is
    /// rejected with 400 at the app layer on the HTTP path — otherwise the
    /// incremental input would be treated as the full one and context would
    /// silently vanish. The WS session path strips the field during
    /// normalization for local merging and is unaffected.
    #[serde(default, deserialize_with = "de_go_string")]
    pub previous_response_id: String,
    /// Optional nucleus sampling parameter.
    pub top_p: Option<f64>,
    /// Optional caller user identifier.
    #[serde(default, deserialize_with = "de_go_string")]
    pub user: String,
    /// Optional caller cache key.
    #[serde(default, deserialize_with = "de_go_string")]
    pub prompt_cache_key: String,
    /// Tool-call behavior control: `"auto"`/`"none"`/`"required"` or a
    /// function object.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub tool_choice: Option<Box<RawValue>>,
    /// `false` forbids parallel tool calls.
    pub parallel_tool_calls: Option<bool>,
}

/// Top-level fields `decode_request` consumes; the rest
/// (`reasoning`/`store`/`service_tier`/`include` etc.) have no upstream
/// counterpart and are recorded in `dropped` rather than swallowed.
/// `previous_response_id` is consumed for explicit rejection, so it is
/// marked read to keep even an empty value out of `dropped`.
static RESPONSES_REQUEST_FIELDS: LazyLock<BTreeSet<&'static str>> = LazyLock::new(|| {
    BTreeSet::from([
        "model",
        "instructions",
        "input",
        "tools",
        "stream",
        "max_output_tokens",
        "temperature",
        "top_p",
        "user",
        "prompt_cache_key",
        "tool_choice",
        "parallel_tool_calls",
        "previous_response_id",
    ])
});

/// An `OpenAI` Responses tool definition; `type` supports `function` and
/// `custom` (freeform).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Tool {
    /// Tool type: `function` or `custom`.
    #[serde(rename = "type", default, deserialize_with = "de_go_string")]
    pub kind: String,
    /// Tool name.
    #[serde(default, deserialize_with = "de_go_string")]
    pub name: String,
    /// Tool-purpose description.
    #[serde(default, deserialize_with = "de_go_string")]
    pub description: String,
    /// Function-tool input JSON Schema; custom tools have no such field.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub parameters: Option<Box<RawValue>>,
    /// A custom tool's input-grammar declaration (e.g. `apply_patch`'s lark
    /// grammar) — the only format spec the model can see, injected with the
    /// description.
    #[serde(default)]
    pub format: Option<ToolFormat>,
}

/// A custom tool's input-grammar declaration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ToolFormat {
    /// Grammar syntax name.
    #[serde(default, deserialize_with = "de_go_string")]
    pub syntax: String,
    /// Grammar definition text.
    #[serde(default, deserialize_with = "de_go_string")]
    pub definition: String,
}

/// Wraps a freeform tool as the function shape upstream accepts: the
/// upstream `is_custom_tool` declaration channel is deterministically
/// unknown, so the tool is declared as a function with a single string
/// parameter and the model fills `input` with the verbatim text (observed:
/// `apply_patch` patches arrive this way).
const CUSTOM_TOOL_INPUT_SCHEMA: &str = r#"{"type":"object","properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}"#;

/// An adapted `OpenAI` request: the intermediate request plus generation
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
    /// Caller-provided upstream response association.
    pub previous_response_id: String,
}

/// Converts an `OpenAI` Responses JSON request to the intermediate request.
///
/// `collect_dropped` controls a second full scan of the body collecting
/// top-level unconsumed fields (`field:*` markers); when false the scan is
/// skipped — `dropped`'s only reader is the debuglog request projection, so
/// with debug off the whole field tree would be wasted work. The other
/// `dropped` write sites are all low-frequency branches and are not gated.
pub fn decode_request(data: &[u8], collect_dropped: bool) -> Result<AdaptedRequest, Failure> {
    let request: Request = decode_single_json(data).map_err(|err| {
        err.into_failure(
            "decode responses request",
            // Content after the top-level JSON means the body is not a
            // single request object — most likely a client bug or a proxy
            // mis-concatenation; silently ignoring it would mask
            // truncation/framing bugs.
            "responses request has trailing data after JSON body",
        )
    })?;
    if request.model.is_empty() {
        return Err(Failure::plain("responses request model is required"));
    }

    let mut context = RequestMessages {
        model: request.model,
        system_prompt: request.instructions,
        ..RequestMessages::default()
    };
    if collect_dropped {
        context
            .dropped
            .extend(unconsumed_fields(data, &RESPONSES_REQUEST_FIELDS));
    }
    if let Some(max_output_tokens) = request.max_output_tokens
        && max_output_tokens > 0
    {
        context.max_tokens = Some(max_output_tokens);
    }
    context.temperature = request.temperature;
    context.top_p = request.top_p;
    context.session_key = request.prompt_cache_key;
    if context.session_key.is_empty() {
        context.session_key = request.user;
    }
    context.tool_choice =
        parse_openai_tool_choice(request.tool_choice.as_deref(), &mut context.dropped)?;
    if request.parallel_tool_calls == Some(false) {
        context.disable_parallel_tool_calls = true;
    }
    append_input_messages(&mut context, request.input.as_deref())?;
    // An empty input only buys an upstream semantic error — same as the
    // chat/anthropic frontends, a local 400 gives the caller an actionable
    // error immediately.
    if context.messages.is_empty() {
        return Err(Failure::plain("responses request input is required"));
    }
    for tool in &request.tools {
        match tool.kind.as_str() {
            "function" => {
                let schema = tool
                    .parameters
                    .as_deref()
                    .map_or_else(|| "{\"type\":\"object\"}".to_string(), RawValue::to_string);
                context.tools.push(ToolDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    input_schema: schema,
                    custom: false,
                });
            }
            "custom" => {
                let mut description = tool.description.clone();
                if let Some(format) = &tool.format
                    && !format.definition.is_empty()
                {
                    use std::fmt::Write as _;
                    write!(
                        description,
                        "\n\nInput grammar ({}):\n{}",
                        format.syntax, format.definition
                    )
                    .unwrap();
                }
                context.tools.push(ToolDefinition {
                    name: tool.name.clone(),
                    description,
                    input_schema: CUSTOM_TOOL_INPUT_SCHEMA.to_string(),
                    custom: true,
                });
            }
            other => context.dropped.push(format!("tool:{other}")),
        }
    }
    // Adjacent assistant turns merge first (same IR-layer shared
    // implementation as the chat/anthropic faces): fake turn boundaries on
    // the wire raise the premature-EOS probability.
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
            previous_response_id: request.previous_response_id,
        },
    })
}

/// Handles `input` as either a string or an item array.
fn append_input_messages(
    context: &mut RequestMessages,
    raw: Option<&RawValue>,
) -> Result<(), Failure> {
    let Some(raw) = raw else {
        return Ok(());
    };
    let text = raw.get().trim();
    if text.is_empty() || text == "null" {
        return Ok(());
    }
    if let Ok(text) = serde_json::from_str::<String>(raw.get()) {
        context.messages.push(Message::User(UserMessage {
            content: vec![Content::Text(TextContent { text })],
            timestamp_ms: now_unix_ms(),
        }));
        return Ok(());
    }
    let items: Vec<Box<RawValue>> = serde_json::from_str(raw.get())
        .map_err(|err| Failure::plain(format!("decode responses input: {err}")))?;
    // A reasoning item sits before the output item it belongs to (assistant
    // message / function_call): summary text is buffered and attached to the
    // next assistant product. `encrypted_content` of the sealed.* form is
    // our own upstream signature, replayed to upstream on the wire with the
    // thinking; foreign opaque payloads are undecodable and ignored.
    let mut pending = PendingReasoning::default();
    for (index, item) in items.iter().enumerate() {
        append_input_item(context, item, &mut pending)
            .map_err(|err| err.prefixed(format_args!("input[{index}]")))?;
    }
    // Trailing orphan reasoning: no assistant product follows it.
    drop_pending_reasoning(context, &mut pending);
    Ok(())
}

/// Buffers a reasoning item's summary text and replayable signature.
#[derive(Debug, Default)]
struct PendingReasoning {
    texts: Vec<String>,
    signature: String,
    signature_type: String,
}

impl PendingReasoning {
    /// Takes the accumulated reasoning summary as a leading
    /// `ThinkingContent` block. A signature with no visible text is treated
    /// as redacted, matching upstream's sealed representation.
    fn consume(&mut self) -> Vec<Content> {
        if self.texts.is_empty() && self.signature.is_empty() {
            return Vec::new();
        }
        let block = Content::Thinking(ThinkingContent {
            thinking: self.texts.join("\n"),
            thinking_signature: std::mem::take(&mut self.signature),
            signature_type: std::mem::take(&mut self.signature_type),
            redacted: self.texts.is_empty(),
        });
        self.texts.clear();
        vec![block]
    }
}

/// Drops reasoning buffered without an assistant product and leaves a
/// trace — shares the `"reasoning:orphan"` marker with input-tail orphans;
/// mid-stream truncated drops used to be invisible, and decode being a
/// filter means every drop must land in `dropped` to reconcile.
fn drop_pending_reasoning(context: &mut RequestMessages, pending: &mut PendingReasoning) {
    if !pending.texts.is_empty() || !pending.signature.is_empty() {
        context.dropped.push("reasoning:orphan".to_string());
    }
    *pending = PendingReasoning::default();
}

/// Dispatches one input element by item type (message/reasoning/
/// `function_call` etc.); unknown types are recorded in `dropped` and
/// skipped.
// One dispatch arm per input item type, mirroring the Go decoder's
// switch for parity review.
#[allow(clippy::too_many_lines)]
fn append_input_item(
    context: &mut RequestMessages,
    raw: &RawValue,
    pending: &mut PendingReasoning,
) -> Result<(), Failure> {
    #[derive(Default, Deserialize)]
    struct ItemHeader {
        #[serde(rename = "type", default, deserialize_with = "de_go_string")]
        kind: String,
        #[serde(default, deserialize_with = "de_go_string")]
        role: String,
    }
    // Clients plant the call id under four field names (`call_id` is
    // canonical; the rest come from Chat habits / camelCase
    // serialization / implementations where id IS the call id) —
    // the first non-empty in order wins.
    #[derive(Default, Deserialize)]
    struct CallOutputItem {
        #[serde(default, deserialize_with = "de_go_string")]
        call_id: String,
        #[serde(default, deserialize_with = "de_go_string")]
        tool_call_id: String,
        #[serde(rename = "callId", default, deserialize_with = "de_go_string")]
        call_id_camel: String,
        #[serde(default, deserialize_with = "de_go_string")]
        id: String,
        #[serde(default, deserialize_with = "de_go_raw")]
        output: Option<Box<RawValue>>,
    }
    // Go's json.Unmarshal leaves a zero struct on `null`.
    let mut header: ItemHeader = serde_json::from_str::<Option<ItemHeader>>(raw.get())
        .map_err(|err| Failure::plain(format!("decode input item: {err}")))?
        .unwrap_or_default();
    if header.kind.is_empty() && !header.role.is_empty() {
        header.kind = "message".to_string();
    }
    match header.kind.as_str() {
        "reasoning" => {
            #[derive(Default, Deserialize)]
            struct ReasoningItem {
                #[serde(default, deserialize_with = "de_go_vec")]
                summary: Vec<ReasoningPart>,
                // Newer Responses put the reasoning body in
                // content[].reasoning_text — reading only summary would
                // silently drop the whole thinking (same shape as CPA#5378).
                #[serde(default, deserialize_with = "de_go_vec")]
                content: Vec<ReasoningPart>,
                #[serde(default, deserialize_with = "de_go_string")]
                encrypted_content: String,
            }
            #[derive(Default, Deserialize)]
            struct ReasoningPart {
                #[serde(rename = "type", default, deserialize_with = "de_go_string")]
                kind: String,
                #[serde(default, deserialize_with = "de_go_string")]
                text: String,
            }
            // Go's json.Unmarshal leaves a zero struct on `null`.
            let item: ReasoningItem = serde_json::from_str::<Option<ReasoningItem>>(raw.get())
                .map_err(|err| Failure::plain(err.to_string()))?
                .unwrap_or_default();
            for part in &item.summary {
                if part.kind == "summary_text" && !part.text.is_empty() {
                    pending.texts.push(part.text.clone());
                }
            }
            for part in &item.content {
                if part.kind == "reasoning_text" && !part.text.is_empty() {
                    pending.texts.push(part.text.clone());
                }
            }
            if let Some(signature_type) = classify_signature_type(&item.encrypted_content) {
                pending.signature = item.encrypted_content;
                pending.signature_type = signature_type.to_string();
            } else if !item.encrypted_content.is_empty() {
                // Foreign opaque payloads are undecodable — recorded, not
                // passed through.
                context
                    .dropped
                    .push("reasoning:encrypted_content".to_string());
            }
            Ok(())
        }
        "message" => append_message_item(context, raw, &header.role, pending),
        "function_call" => {
            #[derive(Default, Deserialize)]
            struct FunctionCallItem {
                #[serde(default, deserialize_with = "de_go_string")]
                call_id: String,
                #[serde(default, deserialize_with = "de_go_string")]
                name: String,
                #[serde(default, deserialize_with = "de_go_string")]
                arguments: String,
            }
            let item: FunctionCallItem =
                serde_json::from_str::<Option<FunctionCallItem>>(raw.get())
                    .map_err(|err| Failure::plain(err.to_string()))?
                    .unwrap_or_default();
            let (arguments, custom) = normalize_tool_arguments(&item.arguments);
            let mut blocks = pending.consume();
            blocks.push(Content::ToolCall(ToolCall {
                id: item.call_id,
                name: item.name,
                arguments,
                custom,
            }));
            context.messages.push(Message::Assistant(AssistantMessage {
                content: blocks,
                stop_reason: Some(StopReason::ToolUse),
                timestamp_ms: now_unix_ms(),
                ..AssistantMessage::default()
            }));
            Ok(())
        }
        "custom_tool_call" => {
            // A freeform tool call's `input` is verbatim text, not JSON
            // (e.g. an apply_patch patch) — it travels the Custom channel
            // verbatim up to `invalid_json_str`.
            #[derive(Default, Deserialize)]
            struct CustomToolCallItem {
                #[serde(default, deserialize_with = "de_go_string")]
                call_id: String,
                #[serde(default, deserialize_with = "de_go_string")]
                name: String,
                #[serde(default, deserialize_with = "de_go_string")]
                input: String,
            }
            let item: CustomToolCallItem =
                serde_json::from_str::<Option<CustomToolCallItem>>(raw.get())
                    .map_err(|err| Failure::plain(err.to_string()))?
                    .unwrap_or_default();
            let mut blocks = pending.consume();
            blocks.push(Content::ToolCall(ToolCall {
                id: item.call_id,
                name: item.name,
                arguments: item.input,
                custom: true,
            }));
            context.messages.push(Message::Assistant(AssistantMessage {
                content: blocks,
                stop_reason: Some(StopReason::ToolUse),
                timestamp_ms: now_unix_ms(),
                ..AssistantMessage::default()
            }));
            Ok(())
        }
        "function_call_output" | "custom_tool_call_output" => {
            // A result item inserted between reasoning and its product makes
            // the reasoning an orphan — drop the buffer.
            drop_pending_reasoning(context, pending);
            let item: CallOutputItem = serde_json::from_str::<Option<CallOutputItem>>(raw.get())
                .map_err(|err| Failure::plain(err.to_string()))?
                .unwrap_or_default();
            let call_id = [
                &item.call_id,
                &item.tool_call_id,
                &item.call_id_camel,
                &item.id,
            ]
            .into_iter()
            .find(|id| !id.is_empty())
            .cloned()
            .unwrap_or_default();
            let blocks = decode_tool_output(context, item.output.as_deref())?;
            // Results with a missing call id or one matching no earlier
            // function_call enter the IR as-is; `demote_orphan_tool_results`
            // at the decode tail demotes them to USER text uniformly.
            context
                .messages
                .push(Message::ToolResult(ToolResultMessage {
                    tool_call_id: call_id,
                    content: blocks,
                    is_error: false,
                    timestamp_ms: now_unix_ms(),
                }));
            Ok(())
        }
        other => {
            // Server-side tool products (tool_search_output / mcp_* etc.)
            // have no intermediate counterpart; silently dropping them would
            // lose context, so they demote to USER text to keep the content.
            context.dropped.push(format!("item:{other}"));
            context.messages.push(Message::User(UserMessage {
                content: vec![Content::Text(TextContent {
                    text: format!("[input item type={other}]\n{}", raw.get()),
                })],
                timestamp_ms: now_unix_ms(),
            }));
            Ok(())
        }
    }
}

/// Decodes a `function_call_output`/`custom_tool_call_output` `output`: a
/// string becomes text directly; a part array (which may contain
/// `input_image` — the upstream tool-result image sub-channel is observed
/// to work) decodes as message content; any other JSON becomes literal
/// text. Part-decode failure tolerance is deliberate — `output` allows
/// arbitrary JSON anyway, so degrading to literal text keeps the content
/// (the message path 400s the same shape because `content` semantics are
/// fixed there), but a part-looking array that cannot decode leaves a
/// `dropped` marker for reconciliation.
fn decode_tool_output(
    context: &mut RequestMessages,
    raw: Option<&RawValue>,
) -> Result<Vec<Content>, Failure> {
    let Some(raw) = raw else {
        return Err(Failure::plain("function call output is required"));
    };
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw.get()) {
        return Ok(vec![Content::Text(TextContent {
            text: text.unwrap_or_default(),
        })]);
    }
    let trimmed = raw.get().trim();
    if trimmed.is_empty() {
        return Err(Failure::plain("function call output is required"));
    }
    if trimmed.starts_with('[') {
        match decode_content(raw.get(), &mut context.dropped) {
            Ok(blocks) if !blocks.is_empty() => return Ok(blocks),
            Ok(_) => {}
            Err(_) => {
                if tool_output_looks_like_parts(raw.get()) {
                    context
                        .dropped
                        .push("tool_output:malformed_parts".to_string());
                }
            }
        }
    }
    Ok(vec![Content::Text(TextContent {
        text: raw.get().to_string(),
    })])
}

/// Whether array elements carry a `type` key — i.e. the caller encoded
/// content-part intent (as opposed to an inherently arbitrary JSON array),
/// making a decode failure worth a `dropped` marker.
fn tool_output_looks_like_parts(raw: &str) -> bool {
    let Ok(elements) = serde_json::from_str::<Vec<serde_json::Map<String, serde_json::Value>>>(raw)
    else {
        return false;
    };
    elements.iter().any(|element| element.contains_key("type"))
}

/// Decodes one `message` item into the conversation by role; unknown roles
/// are recorded in `dropped`.
fn append_message_item(
    context: &mut RequestMessages,
    raw: &RawValue,
    role: &str,
    pending: &mut PendingReasoning,
) -> Result<(), Failure> {
    #[derive(Default, Deserialize)]
    struct MessageItem {
        #[serde(default, deserialize_with = "de_go_string")]
        id: String,
        #[serde(default, deserialize_with = "de_go_raw")]
        content: Option<Box<RawValue>>,
    }
    match role {
        "user" | "assistant" | "system" | "developer" => {}
        _ => {
            context.dropped.push(format!("message_role:{role}"));
            return Ok(());
        }
    }
    // Go's json.Unmarshal leaves a zero struct on `null`.
    let item: MessageItem = serde_json::from_str::<Option<MessageItem>>(raw.get())
        .map_err(|err| Failure::plain(err.to_string()))?
        .unwrap_or_default();
    let mut blocks = match &item.content {
        Some(raw) => decode_content(raw.get(), &mut context.dropped)?,
        // Go's json.Unmarshal(nil) errors — absent content is a decode
        // error, unlike explicit null which yields one empty text block.
        None => {
            return Err(Failure::invalid_argument(
                "decode message content: unexpected end of JSON input",
            ));
        }
    };
    if blocks.is_empty() {
        // Empty content array or all parts dropped: the message must not
        // silently vanish — record `dropped` and continue through the role
        // branches. A user gets an empty-text placeholder to keep the turn
        // structure; an assistant keeps the empty message (counted as
        // DroppedEmptyAssistant on the wire); system/developer contribute
        // nothing to the system prompt. Same convention as the anthropic
        // face.
        context.dropped.push(format!("empty_message:{role}"));
        if role == "user" {
            blocks = vec![Content::Text(TextContent::default())];
        }
    }
    match role {
        "user" => {
            // A non-assistant product intervening makes buffered reasoning
            // an orphan — drop it.
            drop_pending_reasoning(context, pending);
            context.messages.push(Message::User(UserMessage {
                content: blocks,
                timestamp_ms: now_unix_ms(),
            }));
        }
        "assistant" => {
            let mut merged = pending.consume();
            merged.extend(blocks);
            let mut assistant = AssistantMessage {
                content: merged,
                timestamp_ms: now_unix_ms(),
                ..AssistantMessage::default()
            };
            if item.id.starts_with("msg_") {
                // msg_* is the real OpenAI-side message-item identifier; the
                // upstream output_id replay uses the same value.
                assistant.output_id = item.id;
            }
            context.messages.push(Message::Assistant(assistant));
        }
        _ => {
            // system | developer
            drop_pending_reasoning(context, pending);
            let text = content_text(&blocks);
            if !context.system_prompt.is_empty() && !text.is_empty() {
                context.system_prompt.push('\n');
            }
            context.system_prompt.push_str(&text);
        }
    }
    Ok(())
}

// ===========================================================================
// response.go — final JSON and typed SSE encoding
// ===========================================================================

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::{ResponseEvent, ResponseEventType, Usage};
use crate::randid;

use bytes::BytesMut;

use super::common::{
    SseFrame, content_at, go_marshal, go_marshal_into, now_unix_secs, openai_reasoning_items,
    stream_error, usage_totals,
};

/// Per-request Responses SSE encoding state: protocol state plus the full
/// output items of one HTTP Responses stream.
pub struct StreamEncoder {
    /// Model identifier echoed to the client (request text, may be an alias).
    model: String,
    /// Stable `resp_` identifier of this HTTP response.
    response_id: String,
    /// Unix-second creation timestamp of the response.
    created_at: i64,
    /// Sequence number of the next SSE event.
    sequence_number: i64,
    /// In-flight or finished output items keyed by intermediate content
    /// block index.
    items: BTreeMap<i32, StreamItem>,
    /// Finished output items by `output_index`, replayable next turn.
    output: Vec<Option<Value>>,
    /// `response.created` and `response.in_progress` were emitted.
    started: bool,
    /// A terminal event was emitted.
    completed: bool,
    /// The upstream `outputId` was claimed by the first message item: item
    /// ids must be unique within one response, so later message blocks get
    /// synthesized `msg_` ids.
    output_id_claimed: bool,
}

/// Encoding state of one reasoning, `function_call` or message output item.
struct StreamItem {
    /// Responses output item type.
    kind: &'static str,
    /// `rs_`/`fc_`/`msg_`-prefixed item identifier.
    id: String,
    /// Item index in the response `output` array.
    output_index: usize,
    /// Business call id linking `function_call` and `function_call_output`.
    call_id: String,
    /// Tool name of a `function_call`.
    name: String,
    /// Index of the `output_text` part inside a message.
    content_index: i32,
    /// Accumulated text, thinking summary or tool arguments.
    value: String,
    /// Replayable thinking signature; empty means the provider gave none.
    encrypted_content: String,
    /// The item produced `output_item.done`.
    closed: bool,
    /// Reasoning close-out deferred, waiting for a signature frame that may
    /// arrive after the body.
    pending_done: bool,
    /// Full summary text replayed when the reasoning item closes.
    pending_text: String,
}

impl StreamEncoder {
    /// Creates independent SSE encoding state for one HTTP Responses request.
    pub fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            response_id: randid::prefixed("resp_"),
            created_at: now_unix_secs(),
            sequence_number: 0,
            items: BTreeMap::new(),
            output: Vec::new(),
            started: false,
            completed: false,
            output_id_claimed: false,
        }
    }

    /// Expands one intermediate response event into zero or more ordered
    /// Responses SSE events, written straight into `dst`.
    pub fn encode_into(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        if let Err(err) = event.validate() {
            return Err(Failure::plain(format!("validate response event: {err}")));
        }
        if self.completed {
            return Err(Failure::plain("response stream is already completed"));
        }
        // Upstream sends thinking signatures as trailing frames after the
        // body — possibly a whole toolcall block later and split across
        // frames (observed thinking_end -> toolcall_* -> thinking_signature).
        // Signature events only accumulate; closing on the first fragment
        // would write a truncated signature into output_item.done. Close-out
        // is uniformly emitted by the pre-Done fallback flush so a pending
        // item cannot block completion.
        if event.kind == ResponseEventType::Done {
            self.flush_pending_reasoning(dst, frames);
        }
        match event.kind {
            ResponseEventType::Start => {
                self.start(dst, frames);
                Ok(())
            }
            ResponseEventType::ThinkingStart => self.start_reasoning(event, dst, frames),
            ResponseEventType::ThinkingDelta => self.reasoning_delta(event, dst, frames),
            ResponseEventType::ThinkingEnd => self.end_reasoning(event, dst, frames),
            ResponseEventType::ThinkingSignature => self.reasoning_signature(event),
            ResponseEventType::TextStart => self.start_text(event, dst, frames),
            ResponseEventType::TextDelta => self.text_delta(event, dst, frames),
            ResponseEventType::TextEnd => self.end_text(event, dst, frames),
            ResponseEventType::ToolCallStart => self.start_tool_call(event, dst, frames),
            ResponseEventType::ToolCallDelta => self.tool_call_delta(event, dst, frames),
            ResponseEventType::ToolCallEnd => self.end_tool_call(event, dst, frames),
            ResponseEventType::Done => self.done(event, dst, frames),
            ResponseEventType::Error => {
                self.failed(event, dst, frames);
                Ok(())
            }
        }
    }

    /// Emits the two opening frames: `response.created` and
    /// `response.in_progress`.
    fn start(&mut self, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        if self.started {
            return;
        }
        self.started = true;
        let created = base_response(
            &self.response_id,
            &self.model,
            self.created_at,
            "in_progress",
        );
        self.emit(
            "response.created",
            json!({"response": created.clone()}),
            dst,
            frames,
        );
        self.emit(
            "response.in_progress",
            json!({"response": created}),
            dst,
            frames,
        );
    }

    /// Opens a reasoning item and emits `output_item.added` plus
    /// `reasoning_summary_part.added`; openai-type signatures use the inner
    /// real `rs_*` id.
    fn start_reasoning(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let (mut id, output_index) = self.new_item(event.content_index, "reasoning", "rs")?;
        let mut encrypted_content = String::new();
        if let Some(thinking) =
            content_at(event.partial.as_deref(), event.content_index).and_then(Content::as_thinking)
        {
            encrypted_content.clone_from(&thinking.thinking_signature);
            if thinking.signature_type == "openai" {
                // The whole signature blob goes into encrypted_content
                // (replay recognizes the same shape), but the item id is the
                // inner real rs_* — matching what upstream issued.
                if let Some(items) = openai_reasoning_items(&encrypted_content)
                    && !items[0].id.is_empty()
                {
                    id.clone_from(&items[0].id);
                }
            }
        }
        {
            let item = self
                .items
                .get_mut(&event.content_index)
                .expect("new_item registered");
            item.id.clone_from(&id);
            item.encrypted_content.clone_from(&encrypted_content);
        }
        let mut added_item = json!({"id": id, "type": "reasoning", "summary": []});
        if !encrypted_content.is_empty() {
            added_item["encrypted_content"] = Value::String(encrypted_content);
        }
        self.emit(
            "response.output_item.added",
            json!({"output_index": output_index, "item": added_item}),
            dst,
            frames,
        );
        self.emit(
            "response.reasoning_summary_part.added",
            json!({
                "item_id": id, "output_index": output_index, "summary_index": 0,
                "part": {"type": "summary_text", "text": ""},
            }),
            dst,
            frames,
        );
        Ok(())
    }

    /// Emits a thinking increment as `reasoning_summary_text.delta`.
    fn reasoning_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let item = item_in(&mut self.items, event.content_index, &["reasoning"])?;
        item.value.push_str(&event.delta);
        emit_delta(
            &mut self.sequence_number,
            DeltaEvent {
                kind: "response.reasoning_summary_text.delta",
                item_id: item.id.as_str(),
                output_index: item.output_index,
                summary_index: Some(0),
                delta: event.delta.as_str(),
                ..DeltaEvent::default()
            },
            dst,
            frames,
        );
        Ok(())
    }

    /// Closes a reasoning item: with a signature it closes immediately and
    /// emits the three done frames; without one it stays pending for a
    /// trailing signature frame.
    fn end_reasoning(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let (text, mut encrypted_content) = {
            let item = item_in(&mut self.items, event.content_index, &["reasoning"])?;
            let mut text = event.content.clone();
            if text.is_empty() {
                text.clone_from(&item.value);
            }
            (text, std::mem::take(&mut item.encrypted_content))
        };
        if let Some(thinking) =
            content_at(event.partial.as_deref(), event.content_index).and_then(Content::as_thinking)
            && !thinking.thinking_signature.is_empty()
        {
            encrypted_content.clone_from(&thinking.thinking_signature);
        }
        // The thinking text lands in pending_text whether or not close-out
        // is deferred: when signature and body arrive in the same frame the
        // pending branch is skipped, and assigning only there would emit
        // empty summary/done text from reasoning_done.
        {
            let item = self
                .items
                .get_mut(&event.content_index)
                .expect("item_in found");
            item.pending_text = text;
            item.encrypted_content = encrypted_content;
            // Upstream sends the signature as a trailing frame after the
            // body: with no signature yet, defer the close-out events.
            if item.encrypted_content.is_empty() {
                item.pending_done = true;
                return Ok(());
            }
        }
        self.reasoning_done(event.content_index, dst, frames);
        Ok(())
    }

    /// Emits the three close-out events of a reasoning item.
    fn reasoning_done(
        &mut self,
        content_index: i32,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) {
        let (id, output_index, pending_text, encrypted_content) = {
            let item = self
                .items
                .get_mut(&content_index)
                .expect("pending reasoning item");
            item.pending_done = false;
            (
                item.id.clone(),
                item.output_index,
                item.pending_text.clone(),
                item.encrypted_content.clone(),
            )
        };
        let mut completed_item = json!({
            "id": id, "type": "reasoning",
            "summary": [{"type": "summary_text", "text": pending_text}],
        });
        if !encrypted_content.is_empty() {
            completed_item["encrypted_content"] = Value::String(encrypted_content);
        }
        self.close_item(content_index, completed_item.clone());
        self.emit(
            "response.reasoning_summary_text.done",
            json!({
                "item_id": id, "output_index": output_index, "summary_index": 0,
                "text": pending_text,
            }),
            dst,
            frames,
        );
        self.emit(
            "response.reasoning_summary_part.done",
            json!({
                "item_id": id, "output_index": output_index, "summary_index": 0,
                "part": {"type": "summary_text", "text": pending_text},
            }),
            dst,
            frames,
        );
        self.emit(
            "response.output_item.done",
            json!({"output_index": output_index, "item": completed_item}),
            dst,
            frames,
        );
    }

    /// Merges a trailing signature into the reasoning item: Responses has no
    /// incremental signature channel (`encrypted_content` only appears in item
    /// payloads), so signature events only accumulate and stay pending; the
    /// stream-terminal `flush_pending_reasoning` emits the close-out triple
    /// with the full signature — upstream may split the signature across
    /// frames and closing on the first fragment would put a truncated
    /// signature into `output_item.done`, which upstream rejects with
    /// `invalid_argument` on the next replay. When the item is already closed
    /// (signature arrived in the `thinking_end` frame, or a late frame after
    /// the flush) only the completed output's `encrypted_content` is
    /// rewritten; no reasoning item at the index is a decoder bug (the
    /// start-before-block-events contract broke) and errors explicitly
    /// rather than being dropped silently.
    fn reasoning_signature(&mut self, event: &ResponseEvent) -> Result<(), Failure> {
        let (closed, output_index) = {
            let Some(item) = self.items.get_mut(&event.content_index) else {
                return Err(Failure::plain(format!(
                    "thinking signature at content index {} without thinking_start",
                    event.content_index
                )));
            };
            if item.kind != "reasoning" {
                return Err(Failure::plain(format!(
                    "thinking signature at content index {} without thinking_start",
                    event.content_index
                )));
            }
            item.encrypted_content.push_str(&event.delta);
            (item.closed, item.output_index)
        };
        if closed && let Some(Some(completed)) = self.output.get_mut(output_index) {
            completed["encrypted_content"] =
                Value::String(self.items[&event.content_index].encrypted_content.clone());
        }
        Ok(())
    }

    /// Emits pending reasoning close-outs before stream termination (Done),
    /// so items still close normally when upstream never sends a trailing
    /// signature. Signature frames may arrive after later content blocks;
    /// calling mid-stream would close items early and leave late signatures
    /// nowhere to land.
    fn flush_pending_reasoning(&mut self, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        let indices: Vec<i32> = self.items.keys().copied().collect();
        for index in indices {
            if self.items[&index].pending_done {
                self.reasoning_done(index, dst, frames);
            }
        }
    }

    /// Opens a message item and emits `output_item.added` plus
    /// `content_part.added`.
    fn start_text(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let (mut id, output_index) = self.new_item(event.content_index, "message", "msg")?;
        // The upstream output_id is the real OpenAI-side message-item
        // identifier (msg_*); echoing it aligns the client's replayed item
        // with upstream records. One upstream outputId is claimed only
        // once: several message blocks sharing it would collide output item
        // ids, so later blocks fall back to a synthesized msg_.
        let output_id = event
            .partial
            .as_ref()
            .map_or("", |partial| partial.output_id.as_str());
        if !output_id.is_empty() && !self.output_id_claimed {
            id = output_id.to_string();
            self.output_id_claimed = true;
            self.items
                .get_mut(&event.content_index)
                .expect("new_item registered")
                .id
                .clone_from(&id);
        }
        let content_index = self.items[&event.content_index].content_index;
        self.emit(
            "response.output_item.added",
            json!({
                "output_index": output_index,
                "item": {"id": id, "type": "message", "status": "in_progress", "role": "assistant", "content": []},
            }),
            dst,
            frames,
        );
        self.emit(
            "response.content_part.added",
            json!({
                "item_id": id, "output_index": output_index, "content_index": content_index,
                "part": {"type": "output_text", "text": "", "annotations": [], "logprobs": []},
            }),
            dst,
            frames,
        );
        Ok(())
    }

    /// Emits a body increment as `output_text.delta`.
    fn text_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let item = item_in(&mut self.items, event.content_index, &["message"])?;
        item.value.push_str(&event.delta);
        emit_delta(
            &mut self.sequence_number,
            DeltaEvent {
                kind: "response.output_text.delta",
                item_id: item.id.as_str(),
                output_index: item.output_index,
                content_index: Some(item.content_index),
                delta: event.delta.as_str(),
                ..DeltaEvent::default()
            },
            dst,
            frames,
        );
        Ok(())
    }

    /// Closes a message item and emits the `output_text.done`,
    /// `content_part.done`, `output_item.done` close-out triple.
    fn end_text(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let (id, output_index, content_index, text) = {
            let item = item_in(&mut self.items, event.content_index, &["message"])?;
            let mut text = event.content.clone();
            if text.is_empty() {
                text.clone_from(&item.value);
            }
            (item.id.clone(), item.output_index, item.content_index, text)
        };
        let part = json!({"type": "output_text", "text": text, "annotations": [], "logprobs": []});
        let completed_item = json!({
            "id": id, "type": "message", "status": "completed", "role": "assistant",
            "content": [part.clone()],
        });
        self.close_item(event.content_index, completed_item.clone());
        self.emit(
            "response.output_text.done",
            json!({
                "item_id": id, "output_index": output_index, "content_index": content_index,
                "text": text, "logprobs": [],
            }),
            dst,
            frames,
        );
        self.emit(
            "response.content_part.done",
            json!({
                "item_id": id, "output_index": output_index, "content_index": content_index,
                "part": part,
            }),
            dst,
            frames,
        );
        self.emit(
            "response.output_item.done",
            json!({"output_index": output_index, "item": completed_item}),
            dst,
            frames,
        );
        Ok(())
    }

    /// Opens a `function_call`/`custom_tool_call` item and emits
    /// `output_item.added`.
    fn start_tool_call(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        // Custom/freeform call argument bodies are not JSON (upstream
        // is_custom_tool_call): they go down as Responses custom_tool_call
        // items — an `input` field instead of `arguments`.
        let kind = if content_at(event.partial.as_deref(), event.content_index)
            .and_then(Content::as_tool_call)
            .is_some_and(|call| call.custom)
        {
            "custom_tool_call"
        } else {
            "function_call"
        };
        let (id, output_index) = self.new_item(event.content_index, kind, "fc")?;
        {
            let item = self
                .items
                .get_mut(&event.content_index)
                .expect("new_item registered");
            item.call_id.clone_from(&event.tool_call_id);
            item.name.clone_from(&event.tool_name);
        }
        let mut added_item = json!({
            "id": id, "type": kind, "status": "in_progress",
            "call_id": event.tool_call_id, "name": event.tool_name,
        });
        if kind == "custom_tool_call" {
            added_item["input"] = Value::String(String::new());
        } else {
            added_item["arguments"] = Value::String(String::new());
        }
        self.emit(
            "response.output_item.added",
            json!({"output_index": output_index, "item": added_item}),
            dst,
            frames,
        );
        Ok(())
    }

    /// Emits `arguments.delta` or `custom_tool_call_input.delta` per item
    /// kind.
    fn tool_call_delta(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let item = item_in(
            &mut self.items,
            event.content_index,
            &["function_call", "custom_tool_call"],
        )?;
        let event_name = if item.kind == "custom_tool_call" {
            "response.custom_tool_call_input.delta"
        } else {
            "response.function_call_arguments.delta"
        };
        emit_delta(
            &mut self.sequence_number,
            DeltaEvent {
                kind: event_name,
                item_id: item.id.as_str(),
                output_index: item.output_index,
                delta: event.delta.as_str(),
                ..DeltaEvent::default()
            },
            dst,
            frames,
        );
        Ok(())
    }

    /// Closes a tool item and emits `*.done` plus `output_item.done`; the
    /// complete arguments prefer the `ToolCall` carried by the end event.
    fn end_tool_call(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        let (id, output_index, kind) = {
            let item = item_in(
                &mut self.items,
                event.content_index,
                &["function_call", "custom_tool_call"],
            )?;
            (item.id.clone(), item.output_index, item.kind)
        };
        // event.validate() guarantees ToolCallEnd carries a ToolCall; the
        // complete arguments come straight from it instead of relying on
        // accumulated deltas.
        let call = event.tool_call.as_ref().expect("validated tool call");
        let arguments = call.arguments.clone();
        {
            let item = self
                .items
                .get_mut(&event.content_index)
                .expect("item_any_kind found");
            item.call_id.clone_from(&call.id);
            item.name.clone_from(&call.name);
        }
        let (event_name, field) = if kind == "custom_tool_call" {
            ("response.custom_tool_call_input.done", "input")
        } else {
            ("response.function_call_arguments.done", "arguments")
        };
        let mut completed_item = json!({
            "id": id, "type": kind, "status": "completed",
            "call_id": call.id, "name": call.name,
        });
        completed_item[field] = Value::String(arguments.clone());
        self.close_item(event.content_index, completed_item.clone());
        self.emit(
            event_name,
            json!({"item_id": id, "output_index": output_index, field: arguments}),
            dst,
            frames,
        );
        self.emit(
            "response.output_item.done",
            json!({"output_index": output_index, "item": completed_item}),
            dst,
            frames,
        );
        Ok(())
    }

    /// Verifies no dangling items, then emits the
    /// `response.completed`/`incomplete` terminal frame.
    fn done(
        &mut self,
        event: &ResponseEvent,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) -> Result<(), Failure> {
        for item in self.items.values() {
            if !item.closed {
                return Err(Failure::plain(format!(
                    "cannot finish response with open {} item at output index {}",
                    item.kind, item.output_index
                )));
            }
        }
        self.completed = true;
        let mut response = base_response(
            &self.response_id,
            &self.model,
            self.created_at,
            response_status(event.reason),
        );
        response["output"] = Value::Array(self.completed_output());
        let usage = event
            .message
            .as_ref()
            .map_or_else(Usage::default, |m| m.usage.clone());
        response["usage"] = response_usage(&usage);
        let mut event_name = "response.completed";
        if matches!(
            event.reason,
            Some(StopReason::Length | StopReason::ContentFilter)
        ) {
            event_name = "response.incomplete";
            let reason = if event.reason == Some(StopReason::ContentFilter) {
                "content_filter"
            } else {
                "max_output_tokens"
            };
            response["incomplete_details"] = json!({"reason": reason});
        } else {
            response["completed_at"] = Value::Number(now_unix_secs().into());
        }
        self.emit(event_name, json!({"response": response}), dst, frames);
        Ok(())
    }

    /// Emits pending reasoning close-outs first, then `response.failed`, and
    /// closes the stream.
    fn failed(&mut self, event: &ResponseEvent, dst: &mut BytesMut, frames: &mut Vec<SseFrame>) {
        self.completed = true;
        // In the OpenAI Responses API a streaming failure sends a
        // response.failed event carrying a status="failed" response object
        // plus the error field. The top-level status lets downstream
        // gateways classify by real HTTP semantics; error.code lets context
        // overflow be recognized as a request-level problem rather than a
        // channel fault. The event-level error and response.error share one
        // payload (spec position and debugging position share one source of
        // truth), so diagnostic fields like debug_ref agree in both places.
        let (error_payload, status) = stream_error(event, "response stream failed", true);
        let mut response = base_response(&self.response_id, &self.model, self.created_at, "failed");
        response["error"] = error_payload.clone();
        // Pending reasoning items close out before the failure event, same
        // as the Done path — otherwise items waiting for trailing
        // signatures dangle outside the output.
        self.flush_pending_reasoning(dst, frames);
        self.emit(
            "response.failed",
            json!({"response": response, "status": status, "error": error_payload}),
            dst,
            frames,
        );
    }

    /// Registers a new output item under the llm `content_index`; a
    /// duplicate index is a decoder bug.
    fn new_item(
        &mut self,
        content_index: i32,
        kind: &'static str,
        prefix: &str,
    ) -> Result<(String, usize), Failure> {
        if self.items.contains_key(&content_index) {
            return Err(Failure::plain(format!(
                "content index {content_index} already has an output item"
            )));
        }
        let item = StreamItem {
            kind,
            id: randid::prefixed(&format!("{prefix}_")),
            output_index: self.output.len(),
            call_id: String::new(),
            name: String::new(),
            content_index: 0,
            value: String::new(),
            encrypted_content: String::new(),
            closed: false,
            pending_done: false,
            pending_text: String::new(),
        };
        let id = item.id.clone();
        let output_index = item.output_index;
        self.items.insert(content_index, item);
        self.output.push(None);
        Ok((id, output_index))
    }

    /// Closes the item and writes its final shape into the output slot.
    fn close_item(&mut self, content_index: i32, output: Value) {
        let slot = {
            let item = self
                .items
                .get_mut(&content_index)
                .expect("close_item on registered item");
            item.closed = true;
            item.output_index
        };
        self.output[slot] = Some(output);
    }

    /// Final shapes of the closed items (skips unclosed slots).
    fn completed_output(&self) -> Vec<Value> {
        self.output.iter().flatten().cloned().collect()
    }

    /// Adds `type`/`sequence_number` and marshals into one SSE frame
    /// written straight into `dst`.
    fn emit(
        &mut self,
        name: &'static str,
        mut payload: Value,
        dst: &mut BytesMut,
        frames: &mut Vec<SseFrame>,
    ) {
        payload["type"] = Value::String(name.to_string());
        payload["sequence_number"] = Value::Number(self.sequence_number.into());
        self.sequence_number += 1;
        let start = dst.len();
        dst.extend_from_slice(b"event: ");
        dst.extend_from_slice(name.as_bytes());
        dst.extend_from_slice(b"\ndata: ");
        go_marshal_into(dst, &payload);
        dst.extend_from_slice(b"\n\n");
        frames.push(SseFrame {
            name,
            data: start + "event: ".len() + name.len() + "\ndata: ".len()..dst.len() - 2,
        });
    }
}

/// The in-flight item at `content_index`; `kind` must be in `kinds` —
/// `function_call` and `custom_tool_call` only become distinguishable
/// mid-event. A free function (not a `&mut self` method) so callers can
/// hold the item borrow while mutating `sequence_number`.
fn item_in<'m>(
    items: &'m mut BTreeMap<i32, StreamItem>,
    content_index: i32,
    kinds: &[&'static str],
) -> Result<&'m mut StreamItem, Failure> {
    let Some(item) = items.get_mut(&content_index) else {
        return Err(Failure::plain(format!(
            "content index {content_index} has no active output item"
        )));
    };
    if kinds.contains(&item.kind) {
        if item.closed {
            return Err(Failure::plain(format!(
                "content index {content_index} output item is already closed"
            )));
        }
        return Ok(item);
    }
    Err(Failure::plain(format!(
        "content index {content_index} is {:?}, want one of [{}]",
        item.kind,
        kinds.join(" ")
    )))
}

/// Adds `sequence_number` and struct-encodes one delta SSE frame straight
/// into `dst`. Free function for the same borrow-split reason as
/// [`item_in`]: callers hold a `&mut items` borrow across the call.
fn emit_delta(
    sequence_number: &mut i64,
    mut event: DeltaEvent<'_>,
    dst: &mut BytesMut,
    frames: &mut Vec<SseFrame>,
) {
    event.sequence_number = *sequence_number;
    *sequence_number += 1;
    let start = dst.len();
    dst.extend_from_slice(b"event: ");
    dst.extend_from_slice(event.kind.as_bytes());
    dst.extend_from_slice(b"\ndata: ");
    go_marshal_into(dst, &event);
    dst.extend_from_slice(b"\n\n");
    frames.push(SseFrame {
        name: event.kind,
        data: start + "event: ".len() + event.kind.len() + "\ndata: ".len()..dst.len() - 2,
    });
}

/// The fixed encoding shape of high-frequency delta events: the key set
/// matches `emit(map)` output one-to-one but goes through struct encoding —
/// one map-reflection marshal less per frame. `skip_serializing_if` on the
/// optional fields keeps absent keys absent, matching each event's original
/// map key set. Field order mirrors the Go struct declaration. Payload
/// fields borrow — no per-frame `String` copies.
#[derive(Serialize, Default)]
struct DeltaEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    sequence_number: i64,
    item_id: &'a str,
    output_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary_index: Option<i32>,
    delta: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    logprobs: Option<&'a [Value]>,
}

/// Encodes the final assistant message as a non-streaming Responses JSON
/// response. `model` is echoed to the client (request text, may be an
/// alias); empty falls back to the upstream-declared actual uid then the
/// resolved request uid.
pub fn encode_response(
    message: Option<&AssistantMessage>,
    model: &str,
) -> Result<Vec<u8>, Failure> {
    let Some(message) = message else {
        return Err(Failure::plain("response message is nil"));
    };
    let output = output_from_message(message)?;
    let mut model = model;
    if model.is_empty() {
        model = &message.response_model;
    }
    if model.is_empty() {
        model = &message.model;
    }
    let generated_id;
    let mut response_id = message.response_id.as_str();
    if !response_id.starts_with("resp_") {
        generated_id = randid::prefixed("resp_");
        response_id = &generated_id;
    }
    let mut created_at = message.timestamp_ms.div_euclid(1000);
    if message.timestamp_ms <= 0 {
        created_at = now_unix_secs();
    }
    let status = response_status(message.stop_reason);
    let mut response = base_response(response_id, model, created_at, status);
    if status == "completed" {
        response["completed_at"] = Value::Number(now_unix_secs().into());
    }
    response["output"] = Value::Array(output);
    response["usage"] = response_usage(&message.usage);
    Ok(go_marshal(&response))
}

/// The stable field skeleton of a Response object (`store=false` etc. —
/// see the comment in the body).
fn base_response(id: &str, model: &str, created_at: i64, status: &str) -> Value {
    // Aligned with the OpenAI Response object's stable fields. store=false
    // is an honest declaration: this proxy has no response store, and
    // reporting true would lure clients like Codex into previous_response_id
    // chaining that silently drops all context; false makes clients fall
    // back to carrying the full history every time.
    json!({
        "id": id, "object": "response", "created_at": created_at, "status": status,
        "error": null, "incomplete_details": null, "instructions": null, "model": model,
        "output": [], "parallel_tool_calls": true, "previous_response_id": null,
        "reasoning": {"effort": null, "summary": null}, "store": false,
        "temperature": null, "top_p": null, "truncation": "disabled",
        "tool_choice": "auto", "tools": [], "usage": null, "metadata": {},
        "max_output_tokens": null, "text": {"format": {"type": "text"}},
    })
}

/// Projects the Responses usage shape, including cache and reasoning
/// details.
fn response_usage(usage: &Usage) -> Value {
    let (input_tokens, total) = usage_totals(usage);
    let mut result = json!({
        "input_tokens": input_tokens,
        "input_tokens_details": {
            "cached_tokens": usage.cache_read, "cache_write_tokens": usage.cache_write,
        },
        "output_tokens": usage.output,
        "total_tokens": total,
    });
    // `reasoning` absent means upstream did not report the reasoning
    // subset; always emitting 0 would fake "unknown" into "no reasoning" —
    // same handling as the chat side's completion_tokens_details.
    if let Some(reasoning) = usage.reasoning {
        result["output_tokens_details"] = json!({"reasoning_tokens": reasoning});
    }
    result
}

/// Projects the final message's content blocks into the Responses output
/// array.
fn output_from_message(message: &AssistantMessage) -> Result<Vec<Value>, Failure> {
    // Common OpenAI order: reasoning -> function_call -> message; the stable
    // sort keeps IDEs that read output[0] as the message happy.
    let mut reasonings = Vec::new();
    let mut tool_calls = Vec::new();
    let mut messages = Vec::new();
    let mut message_id_claimed = false;
    for block in &message.content {
        match block {
            Content::Text(content) => {
                // The upstream outputId belongs only to the first message
                // item — several text blocks sharing one id would collide
                // output item identifiers; the rest get synthesized msg_.
                let mut message_id = message.output_id.clone();
                if message_id.is_empty() || message_id_claimed {
                    message_id = randid::prefixed("msg_");
                }
                message_id_claimed = true;
                messages.push(json!({
                    "id": message_id, "type": "message", "status": "completed", "role": "assistant",
                    "content": [{"type": "output_text", "text": content.text, "annotations": []}],
                }));
            }
            Content::Thinking(content) => {
                let mut item_id = randid::prefixed("rs_");
                if content.signature_type == "openai"
                    && let Some(items) = openai_reasoning_items(&content.thinking_signature)
                    && !items[0].id.is_empty()
                {
                    item_id.clone_from(&items[0].id);
                }
                let mut item = json!({
                    "id": item_id, "type": "reasoning", "status": "completed",
                    "summary": [{"type": "summary_text", "text": content.thinking}],
                });
                if !content.thinking_signature.is_empty() {
                    item["encrypted_content"] = Value::String(content.thinking_signature.clone());
                }
                reasonings.push(item);
            }
            Content::ToolCall(content) => {
                if content.custom {
                    tool_calls.push(json!({
                        "id": randid::prefixed("fc_"), "type": "custom_tool_call", "status": "completed",
                        "call_id": content.id, "name": content.name, "input": content.arguments,
                    }));
                } else {
                    tool_calls.push(json!({
                        "id": randid::prefixed("fc_"), "type": "function_call", "status": "completed",
                        "call_id": content.id, "name": content.name, "arguments": content.arguments,
                    }));
                }
            }
            Content::Image(_) => {
                return Err(Failure::plain(format!(
                    "unsupported response content type {}",
                    go_content_type_name(block)
                )));
            }
        }
    }
    let mut output = Vec::with_capacity(reasonings.len() + tool_calls.len() + messages.len());
    output.extend(reasonings);
    output.extend(tool_calls);
    output.extend(messages);
    Ok(output)
}

/// Go `%T` names for content blocks in error messages.
fn go_content_type_name(content: &Content) -> &'static str {
    match content {
        Content::Text(_) => "llm.TextContent",
        Content::Thinking(_) => "llm.ThinkingContent",
        Content::Image(_) => "llm.ImageContent",
        Content::ToolCall(_) => "llm.ToolCall",
    }
}

/// Maps the Response object's `status` enum.
fn response_status(reason: Option<StopReason>) -> &'static str {
    if matches!(reason, Some(StopReason::Length | StopReason::ContentFilter)) {
        return "incomplete";
    }
    if matches!(reason, Some(StopReason::Error | StopReason::Aborted)) {
        return "failed";
    }
    "completed"
}

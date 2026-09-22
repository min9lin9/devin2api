//! Anthropic Messages API (`/v1/messages`) request decoder.
//!
//! Port of `G/internal/api/anthropic/messages/request.go` — the decoder
//! half; the JSON/SSE encoder half lands with the output-protocol task.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::domain::{
    AssistantMessage, Content, Failure, ImageContent, Message, RequestMessages, TextContent,
    ThinkingContent, ToolCall, ToolDefinition, ToolResultMessage, UserMessage,
};

use super::common::{
    classify_signature_type, de_go_bool, de_go_raw, de_go_string, de_go_vec, decode_image_part,
    decode_single_json, normalize_tool_arguments, now_unix_ms, parse_anthropic_tool_choice,
    unconsumed_fields,
};

/// The subset of Anthropic Messages request fields this adapter supports.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Request {
    /// Model identifier to use.
    #[serde(default, deserialize_with = "de_go_string")]
    pub model: String,
    /// Conversation messages.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub messages: Vec<AnthropicMessage>,
    /// System prompt: a string or a block array.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub system: Option<Box<RawValue>>,
    /// `max_tokens` is a pointer in Go to distinguish "not provided" from
    /// "explicitly <=0": the latter is dropped client input that must be
    /// observable in `dropped`.
    pub max_tokens: Option<i64>,
    /// Tool definitions.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub tools: Vec<Tool>,
    /// Tool-call behavior control.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub tool_choice: Option<Box<RawValue>>,
    /// Whether a streaming response was requested.
    #[serde(default, deserialize_with = "de_go_bool")]
    pub stream: bool,
    /// Optional sampling temperature.
    pub temperature: Option<f64>,
    /// Optional nucleus sampling parameter.
    pub top_p: Option<f64>,
    /// Optional top-k sampling parameter.
    pub top_k: Option<i64>,
    /// Optional stop sequences.
    #[serde(default, deserialize_with = "de_go_vec")]
    pub stop_sequences: Vec<String>,
    /// Optional request metadata (`user_id` feeds the session key).
    #[serde(default, deserialize_with = "de_go_raw")]
    pub metadata: Option<Box<RawValue>>,
}

/// An Anthropic message entry.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AnthropicMessage {
    /// Message role.
    #[serde(default, deserialize_with = "de_go_string")]
    pub role: String,
    /// String or block-array content.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub content: Option<Box<RawValue>>,
}

/// An Anthropic tool definition.
///
/// An absent/`custom` `type` is a client function tool; client-executed
/// types like `bash_*`/`text_editor_*` are forwarded too (a missing
/// `input_schema` gets a `{"type":"object"}` placeholder); server-hosted
/// types like `web_search_*`/`web_fetch_*`/`code_execution_*` are not
/// forwarded.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Tool {
    /// Tool type.
    #[serde(rename = "type", default, deserialize_with = "de_go_string")]
    pub kind: String,
    /// Tool name.
    #[serde(default, deserialize_with = "de_go_string")]
    pub name: String,
    /// Tool-purpose description.
    #[serde(default, deserialize_with = "de_go_string")]
    pub description: String,
    /// Input JSON Schema.
    #[serde(default, deserialize_with = "de_go_raw")]
    pub input_schema: Option<Box<RawValue>>,
}

/// Top-level fields `decode_request` consumes; the rest
/// (`thinking`/`service_tier`/`context_management`/`mcp_servers` etc.) have
/// no upstream counterpart and are recorded in `dropped` rather than
/// swallowed.
static ANTHROPIC_REQUEST_FIELDS: LazyLock<BTreeSet<&'static str>> = LazyLock::new(|| {
    BTreeSet::from([
        "model",
        "messages",
        "system",
        "max_tokens",
        "tools",
        "tool_choice",
        "stream",
        "temperature",
        "top_p",
        "top_k",
        "stop_sequences",
        "metadata",
    ])
});

/// An adapted Anthropic request: the intermediate request plus generation
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
}

/// Converts an Anthropic Messages JSON request to the intermediate request.
///
/// `collect_dropped` controls a second full scan of the body collecting
/// top-level unconsumed fields (`field:*` markers); when false the scan is
/// skipped — `dropped`'s only reader is the debuglog request projection, so
/// with debug off the whole field tree would be wasted work. The other
/// `dropped` write sites are all low-frequency branches and are not gated.
pub fn decode_request(data: &[u8], collect_dropped: bool) -> Result<AdaptedRequest, Failure> {
    let request: Request = decode_single_json(data).map_err(|err| {
        err.into_failure(
            "decode anthropic request",
            // Content after the top-level JSON means the body is not a
            // single request object — most likely a client bug or a proxy
            // mis-concatenation; silently ignoring it would mask
            // truncation/framing bugs.
            "anthropic request has trailing data after JSON body",
        )
    })?;
    if request.model.is_empty() {
        return Err(Failure::plain("anthropic request model is required"));
    }
    if request.messages.is_empty() {
        return Err(Failure::plain("anthropic request messages are required"));
    }

    let mut context = RequestMessages {
        model: request.model,
        ..RequestMessages::default()
    };
    if collect_dropped {
        context
            .dropped
            .extend(unconsumed_fields(data, &ANTHROPIC_REQUEST_FIELDS));
    }
    if let Some(max_tokens) = request.max_tokens {
        if max_tokens > 0 {
            context.max_tokens = Some(max_tokens);
        } else {
            context.dropped.push("field:max_tokens".to_string());
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
    context.stop_sequences = request.stop_sequences;
    let (tool_choice, disable_parallel) =
        parse_anthropic_tool_choice(request.tool_choice.as_deref())?;
    context.tool_choice = tool_choice;
    context.disable_parallel_tool_calls = disable_parallel;
    if let Some(metadata) = &request.metadata
        && !metadata.get().trim().is_empty()
    {
        #[derive(Default, Deserialize)]
        struct Metadata {
            #[serde(default, deserialize_with = "de_go_string")]
            user_id: String,
        }
        // Go's json.Unmarshal leaves a zero struct on `null` and ignores
        // non-object metadata silently (unmarshal error → no session key).
        if let Ok(metadata) = serde_json::from_str::<Option<Metadata>>(metadata.get()) {
            context.session_key = metadata.unwrap_or_default().user_id;
        }
    }
    if let Some(system) = &request.system {
        let trimmed = system.get().trim();
        if !trimmed.is_empty() && trimmed != "null" {
            append_system(&mut context, system.get())?;
        }
    }
    append_messages(&mut context, &request.messages)?;
    for tool in &request.tools {
        if !client_executed_tool_type(&tool.kind) {
            // Server tools (web_search_*/web_fetch_*/code_execution_* etc.)
            // are provider-hosted; upstream Devin has no counterpart and
            // forwarding would only create dead tools.
            context.dropped.push(format!("tool:{}", tool.kind));
            continue;
        }
        let schema = tool
            .input_schema
            .as_deref()
            .map_or_else(|| "{\"type\":\"object\"}".to_string(), RawValue::to_string);
        context.tools.push(ToolDefinition {
            name: tool.name.clone(),
            description: tool.description.clone(),
            input_schema: schema,
            custom: false,
        });
    }
    // Adjacent assistant turns merge first (same IR-layer shared
    // implementation as the chat/responses faces): fake turn boundaries on
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
        },
    })
}

/// Anthropic client-executed tool `type` shapes: `bash/text_editor/computer`/
/// memory are executed by the caller's environment (Claude Code's local
/// tools look like this) and carry no `input_schema` — forwarding them with
/// a `{"type":"object"}` placeholder lets the model issue calls whose
/// arguments the client interprets per the type version's established
/// schema.
const CLIENT_TOOL_TYPE_PREFIXES: &[&str] = &[
    "bash_",
    "text_editor_",
    "computer_",
    "memory_",
    "str_replace_based_edit_tool",
];

/// Whether `tool.type` is client-executable: empty/`custom` is an ordinary
/// function tool; known client-type prefixes pass; everything else is
/// treated as a server-hosted tool.
fn client_executed_tool_type(tool_type: &str) -> bool {
    if tool_type.is_empty() || tool_type == "custom" {
        return true;
    }
    CLIENT_TOOL_TYPE_PREFIXES
        .iter()
        .any(|prefix| tool_type.starts_with(prefix))
}

/// Merges the `system` field (string or block array) into `system_prompt`.
fn append_system(context: &mut RequestMessages, raw: &str) -> Result<(), Failure> {
    // Go's json.Unmarshal accepts `null` into a string (leaving ""); the
    // caller already filtered a literal null system.
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw) {
        context.system_prompt = text.unwrap_or_default();
        return Ok(());
    }
    let parts: Vec<Box<RawValue>> = serde_json::from_str(raw)
        .map_err(|err| Failure::plain(format!("decode anthropic system: {err}")))?;
    for part in &parts {
        #[derive(Default, Deserialize)]
        struct SystemBlock {
            #[serde(rename = "type", default, deserialize_with = "de_go_string")]
            kind: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
        }
        // Go's json.Unmarshal leaves a zero struct on `null`.
        let block: SystemBlock = serde_json::from_str::<Option<SystemBlock>>(part.get())
            .map_err(|err| Failure::plain(err.to_string()))?
            .unwrap_or_default();
        if block.kind != "text" {
            context.dropped.push(format!("system_block:{}", block.kind));
            continue;
        }
        if !context.system_prompt.is_empty() && !block.text.is_empty() {
            context.system_prompt.push('\n');
        }
        context.system_prompt.push_str(&block.text);
    }
    Ok(())
}

/// Decodes the message stream in order.
fn append_messages(
    context: &mut RequestMessages,
    messages: &[AnthropicMessage],
) -> Result<(), Failure> {
    for (index, message) in messages.iter().enumerate() {
        append_message(context, message)
            .map_err(|err| err.prefixed(format_args!("message[{index}]")))?;
    }
    Ok(())
}

/// Decodes one message into the conversation by role.
fn append_message(
    context: &mut RequestMessages,
    message: &AnthropicMessage,
) -> Result<(), Failure> {
    match message.role.as_str() {
        "user" | "system" => {
            // Claude Code injects role:system mid-stream (agent lists, task
            // reminders, system notifications). The content is
            // position-sensitive — decoding to a UserMessage preserves the
            // timeline; it must not fold into the system prompt.
            let mut produced = decode_anthropic_user_messages(context, message.content.as_deref())?;
            if produced.is_empty()
                && message
                    .content
                    .as_deref()
                    .is_some_and(|raw| !raw.get().trim().is_empty())
            {
                // A content:[] message must not vanish: same policy as
                // content:null — an empty-text placeholder keeps the turn
                // structure, with the gap accounted for.
                context
                    .dropped
                    .push(format!("empty_message:{}", message.role));
                produced.push(Message::User(UserMessage {
                    content: vec![Content::Text(TextContent::default())],
                    timestamp_ms: now_unix_ms(),
                }));
            }
            context.messages.append(&mut produced);
        }
        "assistant" => {
            let blocks = decode_assistant_content(context, message.content.as_deref())?;
            if blocks.is_empty()
                && message
                    .content
                    .as_deref()
                    .is_some_and(|raw| !raw.get().trim().is_empty())
            {
                context.dropped.push("empty_message:assistant".to_string());
            }
            context.messages.push(Message::Assistant(AssistantMessage {
                content: blocks,
                timestamp_ms: now_unix_ms(),
                ..AssistantMessage::default()
            }));
        }
        other => context.dropped.push(format!("role:{other}")),
    }
    Ok(())
}

/// Splits an Anthropic user message's content into one or more intermediate
/// messages — `tool_result` blocks produce standalone `ToolResultMessage`s.
fn decode_anthropic_user_messages(
    context: &mut RequestMessages,
    raw: Option<&RawValue>,
) -> Result<Vec<Message>, Failure> {
    let Some(raw) = raw else {
        return Ok(vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent::default())],
            timestamp_ms: now_unix_ms(),
        })]);
    };
    let trimmed = raw.get().trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent::default())],
            timestamp_ms: now_unix_ms(),
        })]);
    }
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw.get()) {
        return Ok(vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: text.unwrap_or_default(),
            })],
            timestamp_ms: now_unix_ms(),
        })]);
    }
    let parts: Vec<Box<RawValue>> = serde_json::from_str(raw.get())
        .map_err(|err| Failure::plain(format!("decode user content: {err}")))?;

    let mut result = Vec::new();
    let mut current_user_content: Vec<Content> = Vec::new();
    macro_rules! flush_user {
        () => {
            if !current_user_content.is_empty() {
                result.push(Message::User(UserMessage {
                    content: std::mem::take(&mut current_user_content),
                    timestamp_ms: now_unix_ms(),
                }));
            }
        };
    }

    for (index, part) in parts.iter().enumerate() {
        #[derive(Default, Deserialize)]
        struct PartHeader {
            #[serde(rename = "type", default, deserialize_with = "de_go_string")]
            kind: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
            #[serde(default, deserialize_with = "de_go_string")]
            tool_use_id: String,
            #[serde(default, deserialize_with = "de_go_raw")]
            content: Option<Box<RawValue>>,
            #[serde(default, deserialize_with = "de_go_bool")]
            is_error: bool,
        }
        // Go's json.Unmarshal leaves a zero struct on `null`.
        let header: PartHeader = serde_json::from_str::<Option<PartHeader>>(part.get())
            .map_err(|err| Failure::plain(format!("content[{index}]: {err}")))?
            .unwrap_or_default();
        match header.kind.as_str() {
            "text" => {
                current_user_content.push(Content::Text(TextContent { text: header.text }));
            }
            "image" => {
                let image = decode_image_part(part.get())
                    .map_err(|err| err.prefixed(format_args!("content[{index}]")))?;
                current_user_content.push(Content::Image(image));
            }
            "document" | "file" => {
                // Document blocks have no upstream channel and their content
                // is necessarily lost; silent dropping would let the model
                // answer without context and nobody would notice — a
                // placeholder text at least makes the omission visible.
                context.dropped.push(format!("user_block:{}", header.kind));
                current_user_content.push(Content::Text(TextContent {
                    text: format!("[content omitted: {} block not supported]", header.kind),
                }));
            }
            "tool_result" => {
                // Results with a missing tool_use_id or one matching no
                // earlier call enter the IR as-is;
                // `demote_orphan_tool_results` at the decode tail demotes
                // them to USER text uniformly.
                flush_user!();
                let tool = decode_tool_result(
                    context,
                    &header.tool_use_id,
                    header.content.as_deref(),
                    header.is_error,
                )
                .map_err(|err| err.prefixed(format_args!("content[{index}]")))?;
                result.push(Message::ToolResult(tool));
            }
            _ => context.dropped.push(format!("user_block:{}", header.kind)),
        }
    }
    flush_user!();
    Ok(result)
}

/// Decodes an assistant message's `text/thinking/tool_use` blocks.
fn decode_assistant_content(
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
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw.get()) {
        return Ok(vec![Content::Text(TextContent {
            text: text.unwrap_or_default(),
        })]);
    }
    let parts: Vec<Box<RawValue>> = serde_json::from_str(raw.get())
        .map_err(|err| Failure::plain(format!("decode assistant content: {err}")))?;
    let mut blocks = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        #[derive(Default, Deserialize)]
        struct PartHeader {
            #[serde(rename = "type", default, deserialize_with = "de_go_string")]
            kind: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
            #[serde(default, deserialize_with = "de_go_string")]
            thinking: String,
            #[serde(default, deserialize_with = "de_go_string")]
            signature: String,
            #[serde(default, deserialize_with = "de_go_string")]
            data: String,
            #[serde(default, deserialize_with = "de_go_string")]
            id: String,
            #[serde(default, deserialize_with = "de_go_string")]
            name: String,
            #[serde(default, deserialize_with = "de_go_raw")]
            input: Option<Box<RawValue>>,
        }
        // Go's json.Unmarshal leaves a zero struct on `null`.
        let header: PartHeader = serde_json::from_str::<Option<PartHeader>>(part.get())
            .map_err(|err| Failure::plain(format!("content[{index}]: {err}")))?
            .unwrap_or_default();
        match header.kind.as_str() {
            "text" => blocks.push(Content::Text(TextContent { text: header.text })),
            "thinking" => blocks.push(Content::Thinking(ThinkingContent {
                thinking: header.thinking,
                thinking_signature: header.signature.clone(),
                signature_type: guess_signature_type(&header.signature).to_string(),
                redacted: false,
            })),
            "redacted_thinking" => {
                // A redacted block's data is the sealed thinking body; on
                // the Devin wire it maps to signature+redacted.
                blocks.push(Content::Thinking(ThinkingContent {
                    thinking_signature: header.data.clone(),
                    signature_type: guess_signature_type(&header.data).to_string(),
                    redacted: true,
                    ..ThinkingContent::default()
                }));
            }
            "tool_use" => {
                let (arguments, custom) =
                    normalize_tool_arguments(header.input.as_deref().map_or("", RawValue::get));
                blocks.push(Content::ToolCall(ToolCall {
                    id: header.id,
                    name: header.name,
                    arguments,
                    custom,
                }));
            }
            _ => context
                .dropped
                .push(format!("assistant_block:{}", header.kind)),
        }
    }
    Ok(blocks)
}

/// Decodes a `tool_result` block into a `ToolResultMessage`; a missing
/// `tool_use_id` or one matching no known call enters the IR as-is and is
/// demoted by `demote_orphan_tool_results` at the decode tail.
fn decode_tool_result(
    context: &mut RequestMessages,
    tool_use_id: &str,
    raw: Option<&RawValue>,
    is_error: bool,
) -> Result<ToolResultMessage, Failure> {
    let mut blocks = decode_anthropic_content(context, raw)?;
    if blocks.is_empty() {
        blocks = vec![Content::Text(TextContent::default())];
    }
    Ok(ToolResultMessage {
        tool_call_id: tool_use_id.to_string(),
        content: blocks,
        is_error,
        timestamp_ms: now_unix_ms(),
    })
}

/// Decodes raw JSON into text/image content blocks.
fn decode_anthropic_content(
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
    if let Ok(text) = serde_json::from_str::<Option<String>>(raw.get()) {
        return Ok(vec![Content::Text(TextContent {
            text: text.unwrap_or_default(),
        })]);
    }
    let parts: Vec<Box<RawValue>> = serde_json::from_str(raw.get())
        .map_err(|err| Failure::plain(format!("decode content: {err}")))?;
    let mut blocks = Vec::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        #[derive(Default, Deserialize)]
        struct PartHeader {
            #[serde(rename = "type", default, deserialize_with = "de_go_string")]
            kind: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
            #[serde(default)]
            resource: Option<Resource>,
        }
        #[derive(Default, Deserialize)]
        struct Resource {
            #[serde(default, deserialize_with = "de_go_string")]
            uri: String,
            #[serde(rename = "mimeType", default, deserialize_with = "de_go_string")]
            mime_type: String,
            #[serde(default, deserialize_with = "de_go_string")]
            text: String,
            #[serde(default, deserialize_with = "de_go_string")]
            blob: String,
        }
        // Go's json.Unmarshal leaves a zero struct on `null`.
        let header: PartHeader = serde_json::from_str::<Option<PartHeader>>(part.get())
            .map_err(|err| Failure::plain(format!("content[{index}]: {err}")))?
            .unwrap_or_default();
        match header.kind.as_str() {
            "text" => blocks.push(Content::Text(TextContent { text: header.text })),
            "image" => {
                let image = decode_image_part(part.get())
                    .map_err(|err| err.prefixed(format_args!("content[{index}]")))?;
                blocks.push(Content::Image(image));
            }
            "resource" => {
                // MCP tool_result resource blocks: text expands directly;
                // blobs degrade to images or a placeholder.
                let Some(resource) = header.resource else {
                    continue;
                };
                if !resource.text.is_empty() {
                    blocks.push(Content::Text(TextContent {
                        text: resource.text,
                    }));
                } else if !resource.blob.is_empty() && resource.mime_type.starts_with("image/") {
                    blocks.push(Content::Image(ImageContent {
                        data: resource.blob,
                        mime_type: resource.mime_type,
                    }));
                } else {
                    blocks.push(Content::Text(TextContent {
                        text: format!("[resource: {}]", resource.uri),
                    }));
                }
            }
            _ => {
                // Blocks inside tool_result that cannot project to the IR
                // (document etc.) only record `dropped` — they do not enter
                // the content — otherwise the model would answer without
                // knowing content was omitted; same placeholder convention
                // as user-level document/file so the gap stays visible.
                context
                    .dropped
                    .push(format!("content_block:{}", header.kind));
                blocks.push(Content::Text(TextContent {
                    text: format!("[content omitted: {} block not supported]", header.kind),
                }));
            }
        }
    }
    Ok(blocks)
}

/// Tags a replayed thinking signature with the upstream `signature_type`.
/// Shape classification is shared with the responses frontend via
/// [`classify_signature_type`] — `signature_type` is an upstream-regime
/// property, not an entry-protocol property, so an openai-regime signature
/// (a serialized reasoning-item blob) replayed cross-frontend must not be
/// tagged anthropic or upstream returns `invalid_argument`. Other opaque
/// blobs are tagged anthropic (signatures arriving over the Anthropic
/// protocol come either from this proxy's claude models or from the real
/// Anthropic API — both anthropic regime). A missing type is tolerated by
/// upstream; a wrong type triggers `invalid_argument`.
fn guess_signature_type(signature: &str) -> &'static str {
    if let Some(signature_type) = classify_signature_type(signature) {
        return signature_type;
    }
    if signature.is_empty() {
        return "";
    }
    "anthropic"
}

// ===========================================================================
// response.go — final JSON and SSE event encoding
// ===========================================================================

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::{ResponseEvent, ResponseEventType, StopReason, Usage};
use crate::randid;

use super::common::{SseEvent, content_at, go_marshal, stream_error};

/// Per-request Anthropic Messages SSE encoding state.
pub struct StreamEncoder {
    model: String,
    message_id: String,
    finished: bool,
    blocks: Vec<ContentBlockState>,
    usage: Usage,
}

// The bool set mirrors the Go stream encoder's per-block state flags.
#[allow(clippy::struct_excessive_bools)]
struct ContentBlockState {
    index: i32,
    kind: &'static str,
    signature: String,
    /// The thinking body ended but `content_block_stop` was not sent yet —
    /// waiting for a possibly trailing signature frame so the signature
    /// does not land as a separate malformed thinking block.
    pending_sig: bool,
    /// The block's End event arrived but an earlier-index `pending_sig`
    /// thinking block has not closed: pending blocks wait for stream-end
    /// signatures, and letting a later block stop first would make the
    /// thinking block the last to close — spec clients aggregate assistant
    /// snapshots by close order, and Claude Code -p's result takes the
    /// text of the last assistant snapshot, yielding a thinking-only
    /// snapshot and an empty string. Stops are uniformly deferred and
    /// replayed in index order at close-out so the highest-index block
    /// always closes last.
    stop_deferred: bool,
    /// The thinking body was hidden upstream (`ThinkingRedacted`); close-out
    /// emits a `redacted_thinking` block instead of a thinking block.
    redacted: bool,
    /// Redacted was already known when the block opened: spec's
    /// `redacted_thinking` is a `content_block_start` carrying `data` in one
    /// shot, and the sealed signature only completes at close-out, so the
    /// start is deferred to close-out and sent with the data.
    start_deferred: bool,
}

impl StreamEncoder {
    /// Creates encoding state for one Anthropic Messages stream.
    pub fn new(model: &str) -> Self {
        Self {
            model: model.to_string(),
            message_id: randid::prefixed("msg_"),
            finished: false,
            blocks: Vec::new(),
            usage: Usage::default(),
        }
    }

    /// Expands one intermediate response event into ordered Anthropic SSE
    /// events.
    pub fn encode(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if let Err(err) = event.validate() {
            return Err(Failure::plain(format!("validate response event: {err}")));
        }
        if self.finished {
            return Err(Failure::plain("anthropic message stream is already done"));
        }
        match event.kind {
            ResponseEventType::Start => Ok(self.start(event)),
            ResponseEventType::TextStart => Ok(self.start_text(event)),
            ResponseEventType::TextDelta => self.text_delta(event),
            ResponseEventType::TextEnd => self.end_text(event),
            ResponseEventType::ThinkingStart => Ok(self.start_thinking(event)),
            ResponseEventType::ThinkingDelta => self.thinking_delta(event),
            ResponseEventType::ThinkingEnd => self.end_thinking(event),
            ResponseEventType::ThinkingSignature => self.thinking_signature(event),
            ResponseEventType::ToolCallStart => Ok(self.start_tool_use(event)),
            ResponseEventType::ToolCallDelta => self.tool_use_delta(event),
            ResponseEventType::ToolCallEnd => self.end_tool_use(event),
            ResponseEventType::Done => Ok(self.finish(event)),
            ResponseEventType::Error => Ok(self.failed(event)),
        }
    }

    /// Emits `message_start`, carrying the first partial's usage snapshot.
    fn start(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        // The start event's contract field is `partial` (require_partial);
        // `message` belongs to done — reading the wrong field would pin
        // message_start's usage at zero.
        self.usage = event
            .partial
            .as_ref()
            .map_or_else(Usage::default, |partial| partial.usage.clone());
        vec![Self::event(
            "message_start",
            &json!({
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": self.model,
                    "stop_reason": null,
                    "usage": anthropic_usage(&self.usage),
                },
            }),
        )]
    }

    /// Registers text block state and emits `content_block_start`.
    fn start_text(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        self.blocks.push(ContentBlockState {
            index: event.content_index,
            kind: "text",
            signature: String::new(),
            pending_sig: false,
            stop_deferred: false,
            redacted: false,
            start_deferred: false,
        });
        vec![Self::event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": event.content_index,
                "content_block": {"type": "text", "text": ""},
            }),
        )]
    }

    /// Errors explicitly on a missing `text_start`: the decoder contract
    /// guarantees start before delta, and a missing one is a decoder bug —
    /// silently dropping would disguise a shifted sequence as a normal
    /// stream.
    fn text_delta(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if self.block(event.content_index, "text").is_none() {
            return Err(Failure::plain(format!(
                "text delta at content index {} without text_start",
                event.content_index
            )));
        }
        Ok(vec![Self::emit_block_delta(
            event.content_index,
            BlockDelta {
                kind: "text_delta",
                text: event.delta.clone(),
                ..BlockDelta::default()
            },
        )])
    }

    /// Emits `content_block_stop`; the body was fully delivered via
    /// `text_delta` and spec's stop frame carries only type/index — no
    /// re-reading `event.content` for an echo.
    fn end_text(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if self.block(event.content_index, "text").is_none() {
            return Err(Failure::plain(format!(
                "text end at content index {} without text_start",
                event.content_index
            )));
        }
        if self.earlier_pending(event.content_index) {
            self.block_mut(event.content_index, "text")
                .expect("block found")
                .stop_deferred = true;
            return Ok(Vec::new());
        }
        Ok(vec![Self::event(
            "content_block_stop",
            &json!({
                "type": "content_block_stop",
                "index": event.content_index,
            }),
        )])
    }

    /// Registers thinking block state and emits a `thinking`-type
    /// `content_block_start`.
    fn start_thinking(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        let mut state = ContentBlockState {
            index: event.content_index,
            kind: "thinking",
            signature: String::new(),
            pending_sig: false,
            stop_deferred: false,
            redacted: false,
            start_deferred: false,
        };
        if let Some(thinking) =
            content_at(event.partial.as_deref(), event.content_index).and_then(Content::as_thinking)
            && thinking.redacted
        {
            state.redacted = true;
        }
        let redacted = state.redacted;
        self.blocks.push(state);
        if redacted {
            // Spec's redacted_thinking is a start frame carrying `data` in
            // one shot; emitting {thinking,""} now would show spec clients
            // a payloadless empty thinking block with no legal channel for
            // the data ever. Defer the start to close-out.
            self.blocks.last_mut().expect("just pushed").start_deferred = true;
            return Vec::new();
        }
        vec![Self::event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": event.content_index,
                "content_block": {"type": "thinking", "thinking": "", "signature": ""},
            }),
        )]
    }

    /// Emits a `thinking_delta` increment; redacted blocks never emit body.
    fn thinking_delta(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        let Some(state) = self.block(event.content_index, "thinking") else {
            return Err(Failure::plain(format!(
                "thinking delta at content index {} without thinking_start",
                event.content_index
            )));
        };
        if state.redacted {
            // Hidden thinking must not leak increment bodies (upstream
            // gives none anyway — belt-and-suspenders).
            return Ok(Vec::new());
        }
        Ok(vec![Self::emit_block_delta(
            event.content_index,
            BlockDelta {
                kind: "thinking_delta",
                thinking: event.delta.clone(),
                ..BlockDelta::default()
            },
        )])
    }

    /// Closes a thinking block: with a signature it emits
    /// `content_block_stop`, otherwise stays pending for the signature.
    fn end_thinking(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if self.block(event.content_index, "thinking").is_none() {
            return Err(Failure::plain(format!(
                "thinking end at content index {} without thinking_start",
                event.content_index
            )));
        }
        if let Some(thinking) =
            content_at(event.partial.as_deref(), event.content_index).and_then(Content::as_thinking)
        {
            let state = self
                .block_mut(event.content_index, "thinking")
                .expect("block found");
            state.signature.push_str(&thinking.thinking_signature);
            state.redacted = state.redacted || thinking.redacted;
        }
        // Upstream sends the signature as a trailing frame after the body
        // (swe-2 observed it at stream end after all text): with no
        // signature yet, defer content_block_stop; flush_pending_thinking
        // closes the block uniformly at stream end.
        let (has_signature, redacted) = {
            let state = self
                .block(event.content_index, "thinking")
                .expect("block found");
            (!state.signature.is_empty(), state.redacted)
        };
        if !has_signature {
            self.block_mut(event.content_index, "thinking")
                .expect("block found")
                .pending_sig = true;
            return Ok(Vec::new());
        }
        if redacted {
            if self.earlier_pending(event.content_index) {
                self.block_mut(event.content_index, "thinking")
                    .expect("block found")
                    .stop_deferred = true;
                return Ok(Vec::new());
            }
            return Ok(self.stop_thinking(event.content_index));
        }
        if self.earlier_pending(event.content_index) {
            // The signature is ready but an earlier thinking block is still
            // pending: closing now would let the higher-index block finish
            // first, producing the same thinking-last block order — defer
            // to the flush for ordered replay.
            self.block_mut(event.content_index, "thinking")
                .expect("block found")
                .stop_deferred = true;
            return Ok(Vec::new());
        }
        // The signature arrived complete with thinking_end (including the
        // decodeLateSignature synthesized block's Start+End path — the
        // openai-regime signature is the only thinking product): spec
        // clients accumulate signatures only from signature_delta, so
        // stopping directly would drop the signature into thin air.
        let signature = self
            .block(event.content_index, "thinking")
            .expect("block found")
            .signature
            .clone();
        let mut events = vec![Self::emit_block_delta(
            event.content_index,
            BlockDelta {
                kind: "signature_delta",
                signature,
                ..BlockDelta::default()
            },
        )];
        events.extend(self.stop_thinking(event.content_index));
        Ok(events)
    }

    /// Only accumulates trailing signature increments without emitting per
    /// frame: both official SDKs treat signature as assignment semantics
    /// (content.signature = delta.signature, not append), so incremental
    /// `signature_delta` frames would leave clients with only the last
    /// fragment. The complete signature goes out once inside
    /// `flush_pending_thinking` at close-out; a late frame for an
    /// already-closed block (`pending_sig` cleared) lands in the dead buffer
    /// and is naturally dropped — distinct from a missing block (decoder
    /// bug).
    fn thinking_signature(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        let Some(state) = self.block_mut(event.content_index, "thinking") else {
            return Err(Failure::plain(format!(
                "thinking signature at content index {} without thinking_start",
                event.content_index
            )));
        };
        state.signature.push_str(&event.delta);
        Ok(Vec::new())
    }

    /// Emits pending block close-outs before stream termination
    /// (finish/failed): `pending_sig` thinking blocks wait for trailing
    /// signatures, `stop_deferred` later blocks wait for those to close.
    /// Signature frames may arrive after later content blocks (observed
    /// `thinking_end -> toolcall_* -> signature`); calling mid-stream would
    /// close blocks early and silently drop late signatures. Replay is
    /// strictly in index order: spec clients aggregate assistant snapshots
    /// by close order and the last-closing block decides the streaming
    /// result text — out-of-order closes (text first, thinking last) make
    /// Claude Code -p pick a thinking-only snapshot and return an empty
    /// string. Signatures accumulated while pending go out as a single
    /// `signature_delta` (complete string) before `content_block_stop` —
    /// spec clients assign signatures, so multiple fragments equal keeping
    /// only the last.
    fn flush_pending_thinking(&mut self) -> Vec<SseEvent> {
        let mut pending: Vec<i32> = self
            .blocks
            .iter()
            .filter(|state| state.pending_sig || state.stop_deferred)
            .map(|state| state.index)
            .collect();
        pending.sort_unstable();
        let mut events = Vec::new();
        for index in pending {
            let (kind, redacted, signature) = {
                let state = self
                    .blocks
                    .iter_mut()
                    .find(|state| state.index == index)
                    .expect("pending block");
                state.pending_sig = false;
                state.stop_deferred = false;
                (state.kind, state.redacted, state.signature.clone())
            };
            if kind == "thinking" {
                if !redacted && !signature.is_empty() {
                    events.push(Self::emit_block_delta(
                        index,
                        BlockDelta {
                            kind: "signature_delta",
                            signature,
                            ..BlockDelta::default()
                        },
                    ));
                }
                events.extend(self.stop_thinking(index));
                continue;
            }
            events.push(Self::event(
                "content_block_stop",
                &json!({
                    "type": "content_block_stop",
                    "index": index,
                }),
            ));
        }
        events
    }

    /// Whether a lower-index pending thinking block has not closed — if so
    /// this block's stop defers to the uniform close-out, keeping
    /// `content_block_stop` landing in index order.
    fn earlier_pending(&self, index: i32) -> bool {
        self.blocks
            .iter()
            .any(|state| state.index < index && state.pending_sig)
    }

    /// Emits a thinking block's close-out events. Spec's stop frame carries
    /// only type/index; the only exception is a redacted thinking block —
    /// the upstream sealed signature (`data`) has no delta shape and can
    /// only go out whole at the block boundary: when the start was deferred
    /// (redacted known at open) emit spec's
    /// `start{redacted_thinking,data}+stop`; when the start already went out
    /// as thinking (redacted arrived late) the data embeds in the stop
    /// frame, the only channel left.
    fn stop_thinking(&mut self, index: i32) -> Vec<SseEvent> {
        let (redacted, signature, start_deferred) = {
            let state = self
                .blocks
                .iter()
                .find(|state| state.index == index && state.kind == "thinking")
                .expect("thinking block");
            (
                state.redacted,
                state.signature.clone(),
                state.start_deferred,
            )
        };
        if redacted && !signature.is_empty() {
            let block = json!({"type": "redacted_thinking", "data": signature});
            if start_deferred {
                return vec![
                    Self::event(
                        "content_block_start",
                        &json!({
                            "type": "content_block_start",
                            "index": index,
                            "content_block": block,
                        }),
                    ),
                    Self::event(
                        "content_block_stop",
                        &json!({
                            "type": "content_block_stop",
                            "index": index,
                        }),
                    ),
                ];
            }
            return vec![Self::event(
                "content_block_stop",
                &json!({
                    "type": "content_block_stop",
                    "index": index,
                    "content_block": block,
                }),
            )];
        }
        if start_deferred {
            // The start was deferred and no signature ever arrived: the
            // block never opened on the wire and has no data to send — an
            // empty-data redacted_thinking is malformed, so emitting
            // nothing is more compliant (an unused index hole is legal).
            return Vec::new();
        }
        vec![Self::event(
            "content_block_stop",
            &json!({
                "type": "content_block_stop",
                "index": index,
            }),
        )]
    }

    /// Registers tool block state and emits a `tool_use`-type
    /// `content_block_start`.
    fn start_tool_use(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        self.blocks.push(ContentBlockState {
            index: event.content_index,
            kind: "tool_use",
            signature: String::new(),
            pending_sig: false,
            stop_deferred: false,
            redacted: false,
            start_deferred: false,
        });
        vec![Self::event(
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": event.content_index,
                "content_block": {"type": "tool_use", "id": event.tool_call_id, "name": event.tool_name, "input": {}},
            }),
        )]
    }

    /// Emits an argument increment as `input_json_delta`.
    fn tool_use_delta(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if self.block(event.content_index, "tool_use").is_none() {
            return Err(Failure::plain(format!(
                "tool use delta at content index {} without toolcall_start",
                event.content_index
            )));
        }
        Ok(vec![Self::emit_block_delta(
            event.content_index,
            BlockDelta {
                kind: "input_json_delta",
                partial_json: event.delta.clone(),
                ..BlockDelta::default()
            },
        )])
    }

    /// Emits the tool block's `content_block_stop`; the complete input
    /// already arrived via `input_json_delta` increments and spec's stop
    /// frame carries only type/index.
    fn end_tool_use(&mut self, event: &ResponseEvent) -> Result<Vec<SseEvent>, Failure> {
        if self.block(event.content_index, "tool_use").is_none() {
            return Err(Failure::plain(format!(
                "tool use end at content index {} without toolcall_start",
                event.content_index
            )));
        }
        if self.earlier_pending(event.content_index) {
            self.block_mut(event.content_index, "tool_use")
                .expect("block found")
                .stop_deferred = true;
            return Ok(Vec::new());
        }
        Ok(vec![Self::event(
            "content_block_stop",
            &json!({
                "type": "content_block_stop",
                "index": event.content_index,
            }),
        )])
    }

    /// Closes out all pending thinking blocks, then emits `message_delta`
    /// and `message_stop`.
    fn finish(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        self.finished = true;
        self.usage = event
            .message
            .as_ref()
            .map_or_else(Usage::default, |message| message.usage.clone());
        let stop_sequence = event.message.as_ref().map_or(Value::Null, |message| {
            if message.stop_sequence.is_empty() {
                Value::Null
            } else {
                Value::String(message.stop_sequence.clone())
            }
        });
        let delta = json!({
            "stop_reason": anthropic_stop_reason(event.reason),
            "stop_sequence": stop_sequence,
        });
        let mut events = self.flush_pending_thinking();
        events.extend([
            Self::event(
                "message_delta",
                &json!({
                    "type": "message_delta",
                    "delta": delta,
                    "usage": anthropic_usage(&self.usage),
                }),
            ),
            Self::event("message_stop", &json!({"type": "message_stop"})),
        ]);
        events
    }

    /// Closes out pending thinking blocks, then emits an Anthropic-shaped
    /// `error` event and closes the stream.
    fn failed(&mut self, event: &ResponseEvent) -> Vec<SseEvent> {
        self.finished = true;
        // Anthropic's official streaming error format:
        //   event: error
        //   data: {"type":"error","error":{"type":"...","message":"..."}}
        // The top-level status lets downstream gateways classify by real
        // HTTP semantics; error.code lets context overflow be recognized as
        // a request-level problem rather than a channel fault.
        let (error_payload, status) = stream_error(event, "anthropic message stream failed", false);
        let mut events = self.flush_pending_thinking();
        events.push(Self::event(
            "error",
            &json!({
                "type": "error",
                "status": status,
                "error": error_payload,
            }),
        ));
        events
    }

    /// Finds a registered content block by index and kind.
    fn block(&self, index: i32, kind: &str) -> Option<&ContentBlockState> {
        self.blocks
            .iter()
            .find(|state| state.index == index && state.kind == kind)
    }

    /// Mutable variant of [`Self::block`].
    fn block_mut(&mut self, index: i32, kind: &str) -> Option<&mut ContentBlockState> {
        self.blocks
            .iter_mut()
            .find(|state| state.index == index && state.kind == kind)
    }

    /// Marshals a payload into one SSE frame.
    fn event(name: &'static str, payload: &Value) -> SseEvent {
        SseEvent {
            name,
            data: go_marshal(payload),
        }
    }

    /// Struct-encodes the highest-frequency `content_block_delta` frame,
    /// saving one map-reflection marshal per frame.
    fn emit_block_delta(index: i32, delta: BlockDelta) -> SseEvent {
        let data = go_marshal(&BlockDeltaEvent {
            kind: "content_block_delta",
            index,
            delta,
        });
        SseEvent {
            name: "content_block_delta",
            data,
        }
    }
}

/// Covers the four delta shapes of `content_block_delta`; the shapes' keys
/// are mutually exclusive and `skip_serializing_if` keeps the wire key set
/// byte-identical with the original map encoding.
#[derive(Serialize, Default)]
struct BlockDelta {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    thinking: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    signature: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    partial_json: String,
}

/// The fixed shell of a `content_block_delta` event.
#[derive(Serialize)]
struct BlockDeltaEvent {
    #[serde(rename = "type")]
    kind: &'static str,
    index: i32,
    delta: BlockDelta,
}

/// Encodes the final assistant message as non-streaming Anthropic Messages
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
        model = "claude";
    }
    let mut response = json!({
        "id": randid::prefixed("msg_"),
        "type": "message",
        "role": "assistant",
        "content": message_to_anthropic(message),
        "model": model,
        "stop_reason": anthropic_stop_reason(message.stop_reason),
        "usage": anthropic_usage(&message.usage),
    });
    if !message.stop_sequence.is_empty() {
        response["stop_sequence"] = Value::String(message.stop_sequence.clone());
    }
    Ok(go_marshal(&response))
}

/// Converts a tool call's arguments into an Anthropic `input` object.
/// Custom calls' argument bodies are not JSON (freeform patch text /
/// replayed malformed arguments), while Anthropic input must be an object —
/// they go down in the upstream custom-tool wire wrapper shape
/// `{"input":"<verbatim>"}`: a client replaying that input restores exactly
/// the single-parameter wrapper upstream expects, losing nothing.
fn anthropic_tool_input(call: &ToolCall) -> Value {
    if call.custom {
        return json!({"input": call.arguments});
    }
    match serde_json::from_str::<Value>(&call.arguments) {
        Ok(parsed) if !parsed.is_null() => parsed,
        _ => json!({}),
    }
}

/// Converts the final message's content blocks into an Anthropic `content`
/// array.
fn message_to_anthropic(message: &AssistantMessage) -> Value {
    let mut blocks = Vec::new();
    for block in &message.content {
        match block {
            Content::Text(content) => {
                blocks.push(json!({"type": "text", "text": content.text}));
            }
            Content::Thinking(content) => {
                if content.redacted {
                    blocks.push(
                        json!({"type": "redacted_thinking", "data": content.thinking_signature}),
                    );
                    continue;
                }
                let mut value = json!({"type": "thinking", "thinking": content.thinking});
                if !content.thinking_signature.is_empty() {
                    value["signature"] = Value::String(content.thinking_signature.clone());
                }
                blocks.push(value);
            }
            Content::ToolCall(content) => {
                blocks.push(json!({
                    "type": "tool_use", "id": content.id, "name": content.name,
                    "input": anthropic_tool_input(content),
                }));
            }
            Content::Image(_) => {}
        }
    }
    if blocks.is_empty() {
        // Go's nil slice marshals as null, not [].
        return Value::Null;
    }
    Value::Array(blocks)
}

/// Projects the Anthropic usage shape.
fn anthropic_usage(usage: &Usage) -> Value {
    json!({
        "input_tokens": usage.input,
        "output_tokens": usage.output,
        "cache_creation_input_tokens": usage.cache_write,
        "cache_read_input_tokens": usage.cache_read,
    })
}

/// Maps the Anthropic `stop_reason` enum.
fn anthropic_stop_reason(reason: Option<StopReason>) -> Value {
    match reason {
        Some(StopReason::ToolUse) => Value::String("tool_use".to_string()),
        Some(StopReason::Length) => Value::String("max_tokens".to_string()),
        Some(StopReason::Stop) => Value::String("end_turn".to_string()),
        Some(StopReason::StopSequence) => Value::String("stop_sequence".to_string()),
        Some(StopReason::ContentFilter) => Value::String("refusal".to_string()),
        // "error" is not in the Anthropic stop_reason enum — the error is
        // already carried by the error event, so stop_reason falls back to
        // null for "not normally finished" rather than an invented enum.
        _ => Value::Null,
    }
}

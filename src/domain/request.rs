//! Provider-independent request context, messages, content blocks and tools.
//!
//! Port of `G/internal/llm/request.go`. Go `json.RawMessage` fields become
//! `String` here: the domain layer stores verbatim JSON text (or, for custom
//! tool calls, verbatim non-JSON payloads), never a reserialized tree.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::response::{AssistantMessage, StopReason};

/// Identifies a message's role in the conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageRole {
    /// `user`
    User,
    /// `assistant`
    Assistant,
    /// `toolResult`
    ToolResult,
}

impl MessageRole {
    /// Wire value used in Go error messages (`%s` on the role).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::ToolResult => "toolResult",
        }
    }
}

/// Identifies the kind of a message content block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    /// `text`
    Text,
    /// `thinking`
    Thinking,
    /// `image`
    Image,
    /// `toolCall`
    ToolCall,
}

impl ContentType {
    /// Wire value used in Go error messages (`%q` on the type).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Thinking => "thinking",
            Self::Image => "image",
            Self::ToolCall => "toolCall",
        }
    }
}

/// The complete request context sent to any provider adapter.
#[derive(Debug, Clone, Default)]
pub struct RequestMessages {
    /// Caller-specified model identifier; empty means the adapter default.
    pub model: String,
    /// System prompt kept separate from the ordinary message history.
    pub system_prompt: String,
    /// Full chronological conversation history, replayable across providers.
    pub messages: Vec<Message>,
    /// Tool definitions the model may call in this request.
    pub tools: Vec<ToolDefinition>,
    /// Optional output token cap; `None` uses the provider default.
    pub max_tokens: Option<i64>,
    /// Optional sampling temperature; `None` uses the provider default.
    pub temperature: Option<f64>,
    /// Optional nucleus sampling parameter; `None` uses the provider default.
    pub top_p: Option<f64>,
    /// Optional top-k sampling parameter; `None` uses the provider default.
    pub top_k: Option<i64>,
    /// Optional stop sequence list.
    pub stop_sequences: Vec<String>,
    /// Caller preference for tool calling; `None` leaves the choice to the model.
    pub tool_choice: Option<ToolChoice>,
    /// When true, asks the model not to issue parallel tool calls in one turn.
    /// The Devin upstream accepts but ignores this flag; the adapter only
    /// passes the shape through.
    pub disable_parallel_tool_calls: bool,
    /// Optional sampling seed; `None` leaves it to the provider.
    pub seed: Option<i64>,
    /// Caller-provided session key (e.g. `user` / `prompt_cache_key` /
    /// `metadata.user_id`) from which the adapter may derive a stable upstream
    /// session ID. Empty means the caller provided none.
    pub session_key: String,
    /// Fields dropped/downgraded during request decode and normalization
    /// (`"kind:detail"`), surfaced to the debug log — the silent side of
    /// "decode is filter" must be observable.
    pub dropped: Vec<String>,
}

/// Counts of silent repairs made while projecting a request to the upstream
/// wire format — a sibling of `AssistantMessageDiagnostic` (diagnostics that
/// do not change the main result). All-zero means the field is not persisted.
/// Orphan tool-result demotion happens at the IR layer
/// (`demote_orphan_tool_results`) and is audited per-item through
/// `RequestMessages::dropped`, not counted here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestRepairs {
    /// Prompts repositioned by call→result pairing reorder.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub reordered_prompts: i64,
    /// Empty assistant messages skipped (upstream degrades on empty replies).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub dropped_empty_assistant: i64,
    /// History images rewritten to text placeholders (upstream only accepts
    /// current-turn images).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub omitted_history_images: i64,
    /// Upstream content-policy fingerprint rewrites, counted by rule id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sanitize_hits: BTreeMap<String, i64>,
}

// serde's skip_serializing_if calls this with `&i64`; the reference is
// required by the attribute protocol.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(value: &i64) -> bool {
    *value == 0
}

impl RequestRepairs {
    /// Total repair actions, for log-index aggregation into a single field.
    pub fn total(&self) -> i64 {
        self.reordered_prompts
            + self.dropped_empty_assistant
            + self.omitted_history_images
            + self.sanitize_hits.values().sum::<i64>()
    }
}

/// The tool-calling mode requested by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolChoiceMode {
    /// `auto` — the model decides whether to call a tool (default).
    Auto,
    /// `none` — tool calls are forbidden.
    None,
    /// `required` — the model must call some tool this turn.
    Required,
    /// `named` — the model must call `ToolChoice::tool_name`.
    Named,
}

impl ToolChoiceMode {
    /// Wire value used in Go error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::None => "none",
            Self::Required => "required",
            Self::Named => "named",
        }
    }
}

/// Provider-independent tool-call preference.
///
/// Anthropic's `{"type":"any"}` normalizes to `Required` at this layer —
/// the Devin upstream's `option_name` accepts only none/auto/required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolChoice {
    /// Normalized call mode.
    pub mode: ToolChoiceMode,
    /// Tool name required under `Named` mode.
    pub tool_name: String,
}

/// A user, assistant or tool-result message.
// Assistant is much larger than the other variants; boxing it would
// churn every construction/pattern site for a size win that does not
// matter — messages already live behind Vec/Arc.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// User-submitted text/image content.
    User(UserMessage),
    /// Assistant-produced text/thinking/tool-call content.
    Assistant(AssistantMessage),
    /// Result of a tool call.
    ToolResult(ToolResultMessage),
}

impl Message {
    /// The message's role in the conversation.
    pub fn role(&self) -> MessageRole {
        match self {
            Self::User(_) => MessageRole::User,
            Self::Assistant(_) => MessageRole::Assistant,
            Self::ToolResult(_) => MessageRole::ToolResult,
        }
    }

    /// Checks the message against intermediate-layer constraints.
    ///
    /// Error strings mirror the Go `Validate` messages.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::User(message) => {
                validate_content(&message.content, &[ContentType::Text, ContentType::Image])
            }
            Self::Assistant(message) => message.validate(),
            Self::ToolResult(message) => {
                if message.tool_call_id.is_empty() {
                    return Err("tool result call ID is required".to_string());
                }
                validate_content(&message.content, &[ContentType::Text, ContentType::Image])
            }
        }
    }

    /// `Some(&UserMessage)` when this is a user message.
    pub fn as_user(&self) -> Option<&UserMessage> {
        match self {
            Self::User(message) => Some(message),
            _ => None,
        }
    }

    /// `Some(&AssistantMessage)` when this is an assistant message.
    pub fn as_assistant(&self) -> Option<&AssistantMessage> {
        match self {
            Self::Assistant(message) => Some(message),
            _ => None,
        }
    }

    /// `Some(&ToolResultMessage)` when this is a tool-result message.
    pub fn as_tool_result(&self) -> Option<&ToolResultMessage> {
        match self {
            Self::ToolResult(message) => Some(message),
            _ => None,
        }
    }
}

/// A text, thinking, image or tool-call content block.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    /// Ordinary text.
    Text(TextContent),
    /// Model thinking/reasoning.
    Thinking(ThinkingContent),
    /// Base64 image attachment.
    Image(ImageContent),
    /// Assistant-initiated tool call.
    ToolCall(ToolCall),
}

impl Content {
    /// The block's kind.
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Text(_) => ContentType::Text,
            Self::Thinking(_) => ContentType::Thinking,
            Self::Image(_) => ContentType::Image,
            Self::ToolCall(_) => ContentType::ToolCall,
        }
    }

    /// Checks the block against intermediate-layer constraints.
    ///
    /// Error strings mirror the Go `Validate` messages.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Text(_) => Ok(()),
            Self::Thinking(content) => {
                if content.redacted && content.thinking_signature.is_empty() {
                    return Err("redacted thinking content requires a signature".to_string());
                }
                Ok(())
            }
            Self::Image(content) => {
                if content.data.is_empty() {
                    return Err("image data is required".to_string());
                }
                if content.mime_type.is_empty() {
                    return Err("image MIME type is required".to_string());
                }
                Ok(())
            }
            Self::ToolCall(call) => call.validate(),
        }
    }

    /// `Some(&TextContent)` when this is a text block.
    pub fn as_text(&self) -> Option<&TextContent> {
        match self {
            Self::Text(content) => Some(content),
            _ => None,
        }
    }

    /// `Some(&ThinkingContent)` when this is a thinking block.
    pub fn as_thinking(&self) -> Option<&ThinkingContent> {
        match self {
            Self::Thinking(content) => Some(content),
            _ => None,
        }
    }

    /// `Some(&ImageContent)` when this is an image block.
    pub fn as_image(&self) -> Option<&ImageContent> {
        match self {
            Self::Image(content) => Some(content),
            _ => None,
        }
    }

    /// `Some(&ToolCall)` when this is a tool-call block.
    pub fn as_tool_call(&self) -> Option<&ToolCall> {
        match self {
            Self::ToolCall(call) => Some(call),
            _ => None,
        }
    }
}

/// Ordinary text content.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextContent {
    /// Text shown to the user or replayed as context.
    pub text: String,
}

/// Model thinking/reasoning content.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThinkingContent {
    /// Visible thinking text; may be empty for encrypted thinking.
    pub thinking: String,
    /// Provider signature or encrypted opaque payload, preserved verbatim on
    /// replay.
    pub thinking_signature: String,
    /// Signature payload format (`sealed`/`anthropic`/`openai` upstream
    /// `signature_type`). The signature's parse rules are decided by it —
    /// openai-type signatures are serialized Responses reasoning items, the
    /// rest are opaque blobs. Must be replayed with the signature; a mismatch
    /// triggers upstream `invalid_argument`.
    pub signature_type: String,
    /// The thinking body was hidden by the provider; the signature may hold a
    /// replayable payload.
    pub redacted: bool,
}

/// A base64-encoded image attachment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageContent {
    /// Base64 image data without a `data:` URL prefix.
    pub data: String,
    /// Image media type, e.g. `image/png`.
    pub mime_type: String,
}

/// One assistant-initiated tool call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolCall {
    /// Provider-assigned call identifier, used to pair later tool results.
    pub id: String,
    /// Name of the tool to call.
    pub name: String,
    /// Fully accumulated JSON argument object (verbatim text).
    pub arguments: String,
    /// When true, `arguments` is not a JSON object but provider-verbatim text
    /// (Devin `invalid_json_str`/`is_custom_tool_call`: custom/freeform tool
    /// argument bodies are not JSON, e.g. `apply_patch` patch text). Malformed
    /// JSON arguments replayed client-side are preserved the same way rather
    /// than swallowed into `{}`.
    pub custom: bool,
}

impl ToolCall {
    /// Checks the tool call.
    ///
    /// Error strings mirror the Go `Validate` messages.
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("tool call ID is required".to_string());
        }
        if self.name.is_empty() {
            return Err("tool call name is required".to_string());
        }
        if self.custom {
            return Ok(());
        }
        if !is_json_object(&self.arguments) {
            return Err("tool call arguments must be a JSON object".to_string());
        }
        Ok(())
    }
}

/// A user message.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserMessage {
    /// User-submitted text and image blocks.
    pub content: Vec<Content>,
    /// Unix-millisecond creation timestamp.
    pub timestamp_ms: i64,
}

/// The execution result of one tool call.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolResultMessage {
    /// Identifier of the call this result pairs with — the wire pairs on it;
    /// the tool name does not travel upstream, so it is not stored.
    pub tool_call_id: String,
    /// Text and image blocks returned to the model.
    pub content: Vec<Content>,
    /// Whether tool execution failed.
    pub is_error: bool,
    /// Unix-millisecond creation timestamp.
    pub timestamp_ms: i64,
}

/// A tool the model may call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolDefinition {
    /// Stable tool name.
    pub name: String,
    /// Tool-purpose description shown to the model.
    pub description: String,
    /// JSON Schema describing the tool input object (verbatim text).
    pub input_schema: String,
    /// The client declared this tool with freeform/custom semantics (Codex
    /// `apply_patch)`: the argument body is verbatim text, not JSON. The
    /// upstream `is_custom_tool` declaration channel is deterministically
    /// unknown, so such tools are wrapped on the wire as a function
    /// declaration with a single string parameter (`input_schema` is the
    /// wrapper schema); the response side unwraps `{"input":"<verbatim>"}`
    /// back to the raw text using this flag.
    pub custom: bool,
}

impl ToolDefinition {
    /// Checks the tool definition.
    ///
    /// The name charset matches the upstream-observed `^[A-Za-z0-9_-]+$`
    /// (dotted/spaced/non-ASCII names are rejected upstream with only a vague
    /// internal error); local validation turns that into a readable 400.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("tool name is required".to_string());
        }
        if !self
            .name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        {
            return Err(format!(
                "tool name {:?} contains characters outside [A-Za-z0-9_-] which upstream rejects",
                self.name
            ));
        }
        if !is_json_object(&self.input_schema) {
            return Err("tool input schema must be a JSON object".to_string());
        }
        Ok(())
    }
}

impl RequestMessages {
    /// Checks the full request context.
    ///
    /// Error strings mirror the Go `Validate` messages. Go's nil-message and
    /// invalid-mode arms are unreachable here: `Vec<Message>` cannot hold nil
    /// and `ToolChoiceMode` is an exhaustive enum.
    pub fn validate(&self) -> Result<(), String> {
        for (index, message) in self.messages.iter().enumerate() {
            if let Err(err) = message.validate() {
                return Err(format!(
                    "message {index} ({}): {err}",
                    message.role().as_str()
                ));
            }
        }
        for (index, tool) in self.tools.iter().enumerate() {
            if let Err(err) = tool.validate() {
                return Err(format!("tool {index}: {err}"));
            }
        }
        if let Some(choice) = &self.tool_choice {
            match choice.mode {
                ToolChoiceMode::Auto | ToolChoiceMode::None | ToolChoiceMode::Required => {}
                ToolChoiceMode::Named => {
                    if choice.tool_name.is_empty() {
                        return Err("tool_choice named mode requires a tool name".to_string());
                    }
                }
            }
        }
        Ok(())
    }

    /// Demotes orphan `ToolResultMessage`s — those with no consumable call
    /// ahead of them — in place to `UserMessage`s.
    ///
    /// Upstream pairing is positional: call→result pairs are consumed in
    /// order without checking `tool_call_id`, so a result whose id points at
    /// a nonexistent call is still accepted while unconsumed calls remain;
    /// only results appearing before any call (or after all calls were
    /// consumed by earlier results) are rejected, and demotion keeps their
    /// content so the request can proceed. Results with an empty call id
    /// cannot carry a pairing key on the wire and are demoted too. Must run
    /// before `validate`. Each demotion leaves a `missing_tool_call_id` /
    /// `unmatched_tool_call_id:<id>` marker in `dropped`.
    pub fn demote_orphan_tool_results(&mut self) {
        // pending counts earlier calls not yet consumed by a result: each
        // retained result consumes one in arrival order; results arriving
        // after exhaustion are the orphans.
        let mut pending = 0usize;
        for index in 0..self.messages.len() {
            match &self.messages[index] {
                Message::Assistant(assistant) => {
                    pending += assistant
                        .content
                        .iter()
                        .filter(|block| matches!(block, Content::ToolCall(_)))
                        .count();
                }
                Message::ToolResult(result) => {
                    if !result.tool_call_id.is_empty() && pending > 0 {
                        pending -= 1;
                        continue;
                    }
                    if result.tool_call_id.is_empty() {
                        self.dropped.push("missing_tool_call_id".to_string());
                    } else {
                        self.dropped
                            .push(format!("unmatched_tool_call_id:{}", result.tool_call_id));
                    }
                    tracing::warn!(
                        tool_call_id = result.tool_call_id.as_str(),
                        "demoted orphan tool result to user text"
                    );
                    // The prefix block carries "\n": the wire projection
                    // concatenates text blocks directly, so a separate block
                    // produces the "marker line + original text" split.
                    let mut content = Vec::with_capacity(result.content.len() + 1);
                    content.push(Content::Text(TextContent {
                        text: "[tool result, original call lost]\n".to_string(),
                    }));
                    content.extend(result.content.iter().cloned());
                    self.messages[index] = Message::User(UserMessage {
                        content,
                        timestamp_ms: result.timestamp_ms,
                    });
                }
                Message::User(_) => {}
            }
        }
    }

    /// Merges runs of adjacent `AssistantMessage`s: some client histories
    /// flatten one model turn into several adjacent assistant messages, and
    /// passing them through one by one creates fake turn boundaries on the
    /// wire that raise the declared-EOS probability (issue #2). Adjacent
    /// assistant messages always belong to one turn — turn boundaries are
    /// only ever delimited by user/tool-result messages.
    pub fn merge_adjacent_assistant_turns(&mut self) {
        let mut merged: Vec<Message> = Vec::with_capacity(self.messages.len());
        for message in std::mem::take(&mut self.messages) {
            let Message::Assistant(assistant) = message else {
                merged.push(message);
                continue;
            };
            // `merged.last_mut()` cannot be matched with a push in the else
            // arm (the scrutinee borrow outlives the match), so check first.
            if !matches!(merged.last(), Some(Message::Assistant(_))) {
                merged.push(Message::Assistant(assistant));
                continue;
            }
            let Some(Message::Assistant(last)) = merged.last_mut() else {
                unreachable!("checked above");
            };
            // Insert a newline between adjacent text runs: the wire
            // projection concatenates multiple TextContent blocks without a
            // separator, which would glue the two message bodies together.
            if !last.content.is_empty() && !assistant.content.is_empty() {
                let prev_text = matches!(last.content.last(), Some(Content::Text(_)));
                let next_text = matches!(assistant.content.first(), Some(Content::Text(_)));
                if prev_text && next_text {
                    last.content.push(Content::Text(TextContent {
                        text: "\n".to_string(),
                    }));
                }
            }
            let has_call = assistant
                .content
                .iter()
                .any(|block| matches!(block, Content::ToolCall(_)));
            last.content.extend(assistant.content);
            if !assistant.output_id.is_empty() {
                last.output_id = assistant.output_id;
            }
            if has_call {
                last.stop_reason = Some(StopReason::ToolUse);
            }
        }
        self.messages = merged;
    }
}

fn validate_content(content: &[Content], allowed: &[ContentType]) -> Result<(), String> {
    for (index, block) in content.iter().enumerate() {
        if !allowed.contains(&block.content_type()) {
            return Err(format!(
                "content block {index} has disallowed type {:?}",
                block.content_type().as_str()
            ));
        }
        if let Err(err) = block.validate() {
            return Err(format!(
                "content block {index} ({}): {err}",
                block.content_type().as_str()
            ));
        }
    }
    Ok(())
}

/// Reports whether `value` is a JSON object (`{}` included). First-byte
/// prescreen plus a full validity scan without building a tree — non-object,
/// invalid text and `null` all return false. Shared argument sanity check
/// for the protocol frontends and the adapter.
pub fn is_json_object(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with('{') && serde_json::from_str::<serde::de::IgnoredAny>(trimmed).is_ok()
}

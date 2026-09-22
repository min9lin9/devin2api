//! Stable debug-log projections of the intermediate LLM request and
//! response events — port of `G/internal/debuglog/projection.go`.

use crate::domain::{
    AssistantMessage, Content, Message, RequestMessages, ResponseEvent, ResponseEventType,
};

use super::gojson::{JVal, Obj};
use super::stages::STAGE_RESPONSE_EVENTS;
use super::{LogValue, Recorder};

/// `RequestMessagesProjection` — readable JSON shape of the intermediate
/// request model (a Go `map[string]any`, so keys emit sorted).
pub fn request_messages_projection(request: &RequestMessages) -> JVal {
    let messages: Vec<JVal> = request.messages.iter().map(message_projection).collect();
    let tools: Vec<JVal> = request
        .tools
        .iter()
        .map(|tool| {
            Obj::default()
                .set("name", JVal::Str(tool.name.clone()))
                .set("description", JVal::Str(tool.description.clone()))
                // Go's InputSchema is json.RawMessage — verbatim bytes.
                .set("input_schema", JVal::Raw(tool.input_schema.clone().into_bytes()))
                .build()
        })
        .collect();
    let mut result = Obj::default()
        .set("model", JVal::Str(request.model.clone()))
        .set("system_prompt", JVal::Str(request.system_prompt.clone()))
        .set("messages", JVal::Arr(messages))
        .set("tools", JVal::Arr(tools))
        .set(
            "stop_sequences",
            JVal::Arr(
                request
                    .stop_sequences
                    .iter()
                    .map(|s| JVal::Str(s.clone()))
                    .collect(),
            ),
        );
    if let Some(choice) = &request.tool_choice {
        result = result.set(
            "tool_choice",
            Obj::default()
                .set("mode", JVal::Str(choice.mode.as_str().to_string()))
                .set("tool_name", JVal::Str(choice.tool_name.clone()))
                .build(),
        );
    }
    if request.disable_parallel_tool_calls {
        result = result.set("disable_parallel_tool_calls", JVal::Bool(true));
    }
    if let Some(max_tokens) = request.max_tokens {
        result = result.set("max_tokens", JVal::Int(max_tokens));
    }
    if let Some(temperature) = request.temperature {
        result = result.set("temperature", JVal::Float(temperature));
    }
    if !request.session_key.is_empty() {
        result = result.set("session_key", JVal::Str(request.session_key.clone()));
    }
    if !request.dropped.is_empty() {
        result = result.set(
            "dropped_items",
            JVal::Arr(
                request
                    .dropped
                    .iter()
                    .map(|d| JVal::Str(d.clone()))
                    .collect(),
            ),
        );
    }
    result.build()
}

impl Recorder {
    /// `RecordResponseEvent` — log one upstream response event to the 05
    /// stage JSONL. The projection is packaged as a deferred thunk: the
    /// event's `partial` is a per-frame snapshot owned by the decoder and
    /// `message`/`error`/`tool_call` are terminal values, so moving the
    /// event into the worker is race-free by construction — the hot path
    /// pays only one enqueue.
    pub fn record_response_event(&self, event: ResponseEvent) {
        if !self.is_active() {
            return;
        }
        let event_name = event.kind.as_str().to_string();
        self.append_jsonl(
            STAGE_RESPONSE_EVENTS,
            &event_name,
            LogValue::deferred(move || response_event_projection(&event)),
        );
    }
}

/// `ResponseEventProjection` — log shape avoiding a repeated full `partial`.
fn response_event_projection(event: &ResponseEvent) -> LogValue {
    let mut result = Obj::default().set("type", JVal::Str(event.kind.as_str().to_string()));
    match event.kind {
        ResponseEventType::TextStart
        | ResponseEventType::TextDelta
        | ResponseEventType::TextEnd
        | ResponseEventType::ThinkingStart
        | ResponseEventType::ThinkingDelta
        | ResponseEventType::ThinkingEnd
        | ResponseEventType::ThinkingSignature
        | ResponseEventType::ToolCallStart
        | ResponseEventType::ToolCallDelta
        | ResponseEventType::ToolCallEnd => {
            result = result.set("content_index", JVal::Int(i64::from(event.content_index)));
        }
        _ => {}
    }
    if !event.delta.is_empty() || event.kind == ResponseEventType::ToolCallDelta {
        result = result.set("delta", JVal::Str(event.delta.clone()));
    }
    if !event.content.is_empty() {
        result = result.set("content", JVal::Str(event.content.clone()));
    }
    if !event.tool_call_id.is_empty() {
        result = result.set("tool_call_id", JVal::Str(event.tool_call_id.clone()));
    }
    if !event.tool_name.is_empty() {
        result = result.set("tool_name", JVal::Str(event.tool_name.clone()));
    }
    if let Some(call) = &event.tool_call {
        result = result.set(
            "tool_call",
            content_projection(&Content::ToolCall(call.clone())),
        );
    }
    if let Some(reason) = event.reason {
        result = result.set("reason", JVal::Str(reason.as_str().to_string()));
    }
    if event.kind == ResponseEventType::Start
        && let Some(partial) = &event.partial
    {
        result = result.set("message", assistant_projection(partial));
    }
    if let Some(message) = &event.message {
        result = result.set("message", assistant_projection(message));
    }
    if let Some(error) = &event.error {
        result = result.set("error", assistant_projection(error));
    }
    LogValue::Tree(result.build())
}

/// `messageProjection` — one intermediate message; assistant messages reuse
/// `assistant_projection`'s full field set, other roles take their
/// debuggable fields.
fn message_projection(message: &Message) -> JVal {
    let mut result = Obj::default().set("role", JVal::Str(message.role().as_str().to_string()));
    match message {
        Message::User(user) => {
            result = result
                .set("content", content_list_projection(&user.content))
                .set("timestamp_ms", JVal::Int(user.timestamp_ms));
        }
        Message::Assistant(assistant) => {
            // Go merges the assistant map into the message map — same keys.
            if let JVal::Obj(fields) = assistant_projection(assistant) {
                for (key, value) in fields {
                    result = result.set(&key, value);
                }
            }
        }
        Message::ToolResult(tool_result) => {
            result = result
                .set("tool_call_id", JVal::Str(tool_result.tool_call_id.clone()))
                .set("content", content_list_projection(&tool_result.content))
                .set("is_error", JVal::Bool(tool_result.is_error))
                .set("timestamp_ms", JVal::Int(tool_result.timestamp_ms));
        }
    }
    result.build()
}

/// `assistantProjection` — the assistant message log shape; shared by the
/// 02 message list and 05's per-event `message` field so both agree.
fn assistant_projection(message: &AssistantMessage) -> JVal {
    Obj::default()
        .set("role", JVal::Str("assistant".to_string()))
        .set("content", content_list_projection(&message.content))
        .set("api", JVal::Str(message.api.clone()))
        .set("provider", JVal::Str(message.provider.clone()))
        .set("model", JVal::Str(message.model.clone()))
        .set("response_model", JVal::Str(message.response_model.clone()))
        .set("response_id", JVal::Str(message.response_id.clone()))
        .set("output_id", JVal::Str(message.output_id.clone()))
        .set(
            "upstream_request_id",
            JVal::Str(message.upstream_request_id.clone()),
        )
        .set(
            "diagnostics",
            JVal::Arr(
                message
                    .diagnostics
                    .iter()
                    .map(|d| {
                        // Go marshals the struct in field order:
                        // Type, TimestampMS, Details (RawMessage).
                        let mut w = super::gojson::ObjWriter::new();
                        w.field_str("Type", &d.kind)
                            .field_int("TimestampMS", d.timestamp_ms)
                            .field_raw("Details", d.details.as_bytes());
                        JVal::Raw(w.finish().unwrap_or_else(|_| b"{}".to_vec()))
                    })
                    .collect(),
            ),
        )
        .set("usage", usage_projection(&message.usage))
        .set(
            "stop_reason",
            message
                .stop_reason
                .map_or(JVal::Null, |r| JVal::Str(r.as_str().to_string())),
        )
        .set("stop_sequence", JVal::Str(message.stop_sequence.clone()))
        .set("error_message", JVal::Str(message.error_message.clone()))
        .set("timestamp_ms", JVal::Int(message.timestamp_ms))
        .build()
}

/// `llm.Usage` has no JSON tags — Go marshals field names verbatim.
fn usage_projection(usage: &crate::domain::Usage) -> JVal {
    let mut w = super::gojson::ObjWriter::new();
    w.field_int("Input", usage.input)
        .field_int("Output", usage.output)
        .field_int("CacheRead", usage.cache_read)
        .field_int("CacheWrite", usage.cache_write)
        .field_opt_int("Reasoning", usage.reasoning)
        .field_int("TotalTokens", usage.total_tokens);
    JVal::Raw(w.finish().unwrap_or_else(|_| b"{}".to_vec()))
}

/// `contentListProjection` — block-by-block projection.
fn content_list_projection(content: &[Content]) -> JVal {
    JVal::Arr(content.iter().map(content_projection).collect())
}

/// `contentProjection` — one content block; the `type` field is the block's
/// own identifier and unknown blocks log `{"type":"unknown"}` rather than
/// being dropped — the log reflects the real shape.
fn content_projection(content: &Content) -> JVal {
    match content {
        Content::Text(text) => Obj::default()
            .set("type", JVal::Str("text".to_string()))
            .set("text", JVal::Str(text.text.clone()))
            .build(),
        Content::Thinking(thinking) => Obj::default()
            .set("type", JVal::Str("thinking".to_string()))
            .set("thinking", JVal::Str(thinking.thinking.clone()))
            .set(
                "thinking_signature",
                JVal::Str(thinking.thinking_signature.clone()),
            )
            .set("signature_type", JVal::Str(thinking.signature_type.clone()))
            .set("redacted", JVal::Bool(thinking.redacted))
            .build(),
        Content::Image(image) => Obj::default()
            .set("type", JVal::Str("image".to_string()))
            .set("data", JVal::Str(image.data.clone()))
            .set("mime_type", JVal::Str(image.mime_type.clone()))
            .build(),
        Content::ToolCall(call) => {
            // Custom calls carry provider-verbatim non-JSON arguments —
            // marshaling them as raw JSON would corrupt the output, so they
            // log as a string flagged `custom`.
            if call.custom {
                Obj::default()
                    .set("type", JVal::Str("toolCall".to_string()))
                    .set("id", JVal::Str(call.id.clone()))
                    .set("name", JVal::Str(call.name.clone()))
                    .set("arguments", JVal::Str(call.arguments.clone()))
                    .set("custom", JVal::Bool(true))
                    .build()
            } else {
                Obj::default()
                    .set("type", JVal::Str("toolCall".to_string()))
                    .set("id", JVal::Str(call.id.clone()))
                    .set("name", JVal::Str(call.name.clone()))
                    // Go's Arguments is json.RawMessage — verbatim bytes.
                    .set("arguments", JVal::Raw(call.arguments.clone().into_bytes()))
                    .build()
            }
        }
    }
}

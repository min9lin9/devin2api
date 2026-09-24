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
                .set("input_schema", JVal::Raw(tool.input_schema.clone().into_bytes().into()))
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
    ///
    /// The thunk captures only the fields the projection reads, not a
    /// whole-event clone: `partial` is only projected for `start`
    /// events, so delta events do not pin the decoder's shared `Arc` — a
    /// pinned snapshot would force a deep clone at the decoder's next
    /// `Arc::make_mut`.
    pub fn record_response_event(&self, event: &ResponseEvent) {
        if !self.is_active() {
            return;
        }
        let event = ResponseEvent {
            kind: event.kind,
            content_index: event.content_index,
            delta: event.delta.clone(),
            content: event.content.clone(),
            partial: if event.kind == ResponseEventType::Start {
                event.partial.clone()
            } else {
                None
            },
            tool_call_id: event.tool_call_id.clone(),
            tool_name: event.tool_name.clone(),
            tool_call: event.tool_call.clone(),
            reason: event.reason,
            message: event.message.clone(),
            error: event.error.clone(),
        };
        self.append_jsonl(
            STAGE_RESPONSE_EVENTS,
            event.kind.as_str(),
            LogValue::deferred(move || match response_event_projection_bytes(&event) {
                Ok(bytes) => LogValue::Raw(bytes.into()),
                // A marshal failure must fail the record like Go's
                // `json.Marshal` error path: emitting the error text as a
                // raw payload makes `compact_escape` reject it, so the
                // worker skips the write exactly as on marshal error.
                Err(err) => LogValue::Raw(err.into_bytes().into()),
            }),
        );
    }
}

/// `ResponseEventProjection` emitted directly as compact JSON bytes — the
/// worker's sanitize prescreen then passes it through untouched, so the
/// hot path never builds a `JVal` tree. Field order is the sorted order
/// Go's `json.Marshal` gives the projection map; nested projections
/// (`message`/`error`/`tool_call`) marshal through `JVal` for identical
/// bytes without duplicating their field lists.
fn response_event_projection_bytes(event: &ResponseEvent) -> Result<Vec<u8>, String> {
    let mut w = super::gojson::ObjWriter::new();
    if !event.content.is_empty() {
        w.field_str("content", &event.content);
    }
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
            w.field_int("content_index", i64::from(event.content_index));
        }
        _ => {}
    }
    if !event.delta.is_empty() || event.kind == ResponseEventType::ToolCallDelta {
        w.field_str("delta", &event.delta);
    }
    if let Some(error) = &event.error {
        w.field("error", &assistant_projection(error));
    }
    // Go's map literal assigns `message` twice — `event.Message` wins over
    // the start-event `partial`; the single emit below mirrors the winner.
    let message = event.message.as_ref().or_else(|| {
        if event.kind == ResponseEventType::Start {
            event.partial.as_ref()
        } else {
            None
        }
    });
    if let Some(message) = message {
        w.field("message", &assistant_projection(message));
    }
    if let Some(reason) = event.reason {
        w.field_str("reason", reason.as_str());
    }
    if let Some(call) = &event.tool_call {
        w.field("tool_call", &tool_call_projection(call));
    }
    if !event.tool_call_id.is_empty() {
        w.field_str("tool_call_id", &event.tool_call_id);
    }
    if !event.tool_name.is_empty() {
        w.field_str("tool_name", &event.tool_name);
    }
    w.field_str("type", event.kind.as_str());
    w.finish()
}

/// `ResponseEventProjection` — log shape avoiding a repeated full `partial`.
/// Retained as the tree-form reference: the parity test pins
/// `response_event_projection_bytes` to its marshaled output byte-for-byte.
#[cfg(test)]
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
        result = result.set("tool_call", tool_call_projection(call));
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
                        JVal::Raw(w.finish().unwrap_or_else(|_| b"{}".to_vec()).into())
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
    JVal::Raw(w.finish().unwrap_or_else(|_| b"{}".to_vec()).into())
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
        Content::ToolCall(call) => tool_call_projection(call),
    }
}

/// The `toolCall` content-block projection, shared by the content list and
/// the per-event `tool_call` field.
fn tool_call_projection(call: &crate::domain::ToolCall) -> JVal {
    // Custom calls carry provider-verbatim non-JSON arguments — marshaling
    // them as raw JSON would corrupt the output, so they log as a string
    // flagged `custom`.
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
            .set("arguments", JVal::Raw(call.arguments.clone().into_bytes().into()))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        AssistantMessage, AssistantMessageDiagnostic, Content, StopReason, TextContent,
        ThinkingContent, ToolCall, Usage,
    };
    use std::sync::Arc;

    fn assistant() -> AssistantMessage {
        AssistantMessage {
            content: vec![
                Content::Text(TextContent {
                    text: "hello <world> & \"friends\"\u{2028}".to_string(),
                }),
                Content::Thinking(ThinkingContent {
                    thinking: "pondering".to_string(),
                    thinking_signature: "sig".to_string(),
                    signature_type: "type".to_string(),
                    redacted: false,
                }),
                Content::ToolCall(ToolCall {
                    id: "call_1".to_string(),
                    name: "shell".to_string(),
                    arguments: "{\"cmd\":\"ls\"}".to_string(),
                    custom: false,
                }),
                Content::ToolCall(ToolCall {
                    id: "call_2".to_string(),
                    name: "apply_patch".to_string(),
                    arguments: "*** patch text".to_string(),
                    custom: true,
                }),
            ],
            api: "devin".to_string(),
            provider: "devin".to_string(),
            model: "m".to_string(),
            response_model: "m-2".to_string(),
            response_id: "resp_1".to_string(),
            output_id: "out_1".to_string(),
            upstream_request_id: "req_1".to_string(),
            diagnostics: vec![AssistantMessageDiagnostic {
                kind: "warn".to_string(),
                timestamp_ms: 7,
                details: "{\"a\":1}".to_string(),
            }],
            usage: Usage {
                input: 3,
                output: 5,
                cache_read: 1,
                cache_write: 2,
                reasoning: Some(4),
                total_tokens: 15,
            },
            stop_reason: Some(StopReason::Stop),
            stop_sequence: String::new(),
            error_message: String::new(),
            failure: None,
            debug_ref: String::new(),
            timestamp_ms: 42,
        }
    }

    fn base_event(kind: ResponseEventType) -> ResponseEvent {
        ResponseEvent {
            kind,
            content_index: 2,
            delta: String::new(),
            content: String::new(),
            partial: None,
            tool_call_id: String::new(),
            tool_name: String::new(),
            tool_call: None,
            reason: None,
            message: None,
            error: None,
        }
    }

    /// The direct-emit writer must produce byte-identical output to the
    /// tree projection's `json.Marshal` across every field combination the
    /// encoder can produce — the write path changed, the bytes must not.
    #[test]
    fn direct_emit_matches_tree_projection() {
        let mut events = vec![
            base_event(ResponseEventType::Start),
            base_event(ResponseEventType::TextDelta),
            base_event(ResponseEventType::ToolCallDelta),
            base_event(ResponseEventType::Done),
            base_event(ResponseEventType::Error),
        ];
        events[0].partial = Some(Arc::new(assistant()));
        events[1].delta = "chunk <&>\u{2029}".to_string();
        events[2].delta = "{\"a\":".to_string();
        events[3].reason = Some(StopReason::Stop);
        events[3].message = Some(Arc::new(assistant()));
        events[4].reason = Some(StopReason::Error);
        events[4].error = Some(Arc::new(assistant()));
        let mut full = base_event(ResponseEventType::ToolCallEnd);
        full.content = "result".to_string();
        full.delta = "tail".to_string();
        full.tool_call_id = "call_9".to_string();
        full.tool_name = "shell".to_string();
        full.tool_call = Some(Box::new(ToolCall {
            id: "call_9".to_string(),
            name: "shell".to_string(),
            arguments: "{\"x\": [1, 2]}".to_string(),
            custom: false,
        }));
        full.message = Some(Arc::new(assistant()));
        full.error = Some(Arc::new(assistant()));
        events.push(full);
        let mut custom = base_event(ResponseEventType::ToolCallEnd);
        custom.tool_call = Some(Box::new(ToolCall {
            id: "c".to_string(),
            name: "apply_patch".to_string(),
            arguments: "not json".to_string(),
            custom: true,
        }));
        events.push(custom);

        for event in &events {
            let LogValue::Tree(tree) = response_event_projection(event) else {
                panic!("tree projection must yield a tree");
            };
            let want = super::super::gojson::marshal(&tree).expect("tree marshals");
            let got = response_event_projection_bytes(event).expect("direct emit");
            assert_eq!(
                String::from_utf8_lossy(&got),
                String::from_utf8_lossy(&want),
                "kind {:?}",
                event.kind
            );
        }
    }
}

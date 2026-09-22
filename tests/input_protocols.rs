//! Input-protocol contract tests for the devin2api Rust port (plan task 5).
//!
//! Ports `G/internal/llm/{request,response,failure}_test.go`,
//! `G/internal/api/common/toolchoice_test.go` and the three
//! `request_test.go` decoder suites against `devin2api::domain` and
//! `devin2api::protocol::{responses,chat,messages}`. Test names map to the
//! `rust_case` entries in `tests/contracts.json` (`owner_task` 5); each test
//! notes its Go source.

use std::sync::Arc;

use devin2api::domain::{
    self, AssistantMessage, Content, Failure, ImageContent, Message, RequestMessages,
    ResponseEvent, ResponseEventType, StopReason, TextContent, ThinkingContent, ToolCall,
    ToolChoiceMode, ToolDefinition, ToolResultMessage, Usage, UserMessage,
};
use devin2api::protocol::{chat, common, messages, responses};

fn text_block(text: &str) -> Content {
    Content::Text(TextContent {
        text: text.to_string(),
    })
}

fn user_text(text: &str) -> Message {
    Message::User(UserMessage {
        content: vec![text_block(text)],
        timestamp_ms: 0,
    })
}

fn tool_call(id: &str, name: &str, arguments: &str) -> Content {
    Content::ToolCall(ToolCall {
        id: id.to_string(),
        name: name.to_string(),
        arguments: arguments.to_string(),
        custom: false,
    })
}

fn tool_result(call_id: &str, text: &str) -> Message {
    Message::ToolResult(ToolResultMessage {
        tool_call_id: call_id.to_string(),
        content: vec![text_block(text)],
        is_error: false,
        timestamp_ms: 0,
    })
}

fn dropped_contains(dropped: &[String], marker: &str) -> bool {
    dropped.iter().any(|entry| entry == marker)
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/llm/request_test.go
// ---------------------------------------------------------------------------

/// Port of `TestRequestMessagesSupportsProviderIndependentHistory`.
#[test]
fn request_messages_supports_provider_independent_history() {
    let request = RequestMessages {
        system_prompt: "你是一个谨慎的编程助手。".to_string(),
        messages: vec![
            Message::User(UserMessage {
                content: vec![
                    text_block("读取配置并解释图片。"),
                    Content::Image(ImageContent {
                        data: "iVBORw0KGgo=".to_string(),
                        mime_type: "image/png".to_string(),
                    }),
                ],
                timestamp_ms: 1,
            }),
            Message::Assistant(AssistantMessage {
                content: vec![
                    Content::Thinking(ThinkingContent {
                        thinking: "需要先读取文件。".to_string(),
                        thinking_signature: "thinking-signature".to_string(),
                        ..ThinkingContent::default()
                    }),
                    text_block("我先读取配置。"),
                    tool_call("call-1", "read_file", r#"{"path":"config.json"}"#),
                ],
                api: "anthropic-messages".to_string(),
                provider: "anthropic".to_string(),
                model: "claude-test".to_string(),
                usage: Usage {
                    input: 20,
                    output: 12,
                    reasoning: Some(8),
                    total_tokens: 32,
                    ..Usage::default()
                },
                stop_reason: Some(StopReason::ToolUse),
                timestamp_ms: 2,
                ..AssistantMessage::default()
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-1".to_string(),
                content: vec![
                    text_block(r#"{"debug":true}"#),
                    Content::Image(ImageContent {
                        data: "iVBORw0KGgo=".to_string(),
                        mime_type: "image/png".to_string(),
                    }),
                ],
                is_error: false,
                timestamp_ms: 3,
            }),
        ],
        tools: vec![ToolDefinition {
            name: "read_file".to_string(),
            description: "读取文件内容".to_string(),
            input_schema: r#"{
                "type":"object",
                "properties":{"path":{"type":"string"}},
                "required":["path"]
            }"#
            .to_string(),
            custom: false,
        }],
        ..RequestMessages::default()
    };

    request.validate().expect("Validate() error");
}

/// Port of `TestRequestMessagesRejectsInvalidToolArguments`.
#[test]
fn request_messages_rejects_invalid_tool_arguments() {
    let request = RequestMessages {
        messages: vec![Message::Assistant(AssistantMessage {
            content: vec![tool_call("call-1", "read_file", r#"{"path:"#)],
            stop_reason: Some(StopReason::ToolUse),
            ..AssistantMessage::default()
        })],
        ..RequestMessages::default()
    };

    assert!(
        request.validate().is_err(),
        "Validate() = nil, want invalid tool arguments error"
    );
}

/// Port of `TestDemoteOrphanToolResults` — positional semantics: results with
/// an unconsumed call ahead stay TOOL even when the id mismatches; results
/// before any call, after exhaustion, or with no id demote to USER text
/// with dropped markers.
#[test]
fn demote_orphan_tool_results() {
    let mut request = RequestMessages {
        messages: vec![
            // Orphan: a result before any call.
            tool_result("call-early", "early"),
            Message::Assistant(AssistantMessage {
                content: vec![
                    tool_call("call-1", "read", "{}"),
                    tool_call("call-2", "read", "{}"),
                ],
                ..AssistantMessage::default()
            }),
            // Normal pairing: consumes call-1.
            tool_result("call-1", "ok"),
            // Id mismatch but call-2 still unconsumed: upstream consumes
            // positionally, kept as TOOL.
            tool_result("call-mismatch", "positional"),
            // Orphan: pending exhausted.
            tool_result("call-gone", "lost"),
            // Orphan: missing id.
            tool_result("", "noid"),
        ],
        ..RequestMessages::default()
    };
    request.demote_orphan_tool_results();

    assert!(
        matches!(request.messages[0], Message::User(_)),
        "result-before-any-call not demoted: {:?}",
        request.messages[0]
    );
    for index in [2usize, 3] {
        assert!(
            matches!(request.messages[index], Message::ToolResult(_)),
            "consumable result was demoted: {:?}",
            request.messages[index]
        );
    }
    for index in [0usize, 4, 5] {
        let Message::User(demoted) = &request.messages[index] else {
            panic!("message {index} not demoted: {:?}", request.messages[index]);
        };
        let Some(Content::Text(text)) = demoted.content.first() else {
            panic!("message {index} prefix = {:?}", demoted.content[0]);
        };
        assert_eq!(text.text, "[tool result, original call lost]\n");
    }
    let want = [
        "unmatched_tool_call_id:call-early",
        "unmatched_tool_call_id:call-gone",
        "missing_tool_call_id",
    ];
    assert_eq!(request.dropped, want, "dropped markers");
    request.validate().expect("post-demote Validate() error");
}

/// Port of `TestMergeAdjacentAssistantTurns` — adjacent assistant messages
/// merge into one turn: a newline separates text runs, `OutputID` takes the
/// last non-empty, a `ToolCall` sets `toolUse`; assistants separated by
/// user/`tool_result` do not merge.
#[test]
fn merge_adjacent_assistant_turns() {
    let mut request = RequestMessages {
        messages: vec![
            user_text("问"),
            Message::Assistant(AssistantMessage {
                content: vec![text_block("先读")],
                output_id: "msg_a".to_string(),
                ..AssistantMessage::default()
            }),
            Message::Assistant(AssistantMessage {
                content: vec![text_block("再改"), tool_call("c1", "edit", "{}")],
                ..AssistantMessage::default()
            }),
            tool_result("c1", "done"),
            Message::Assistant(AssistantMessage {
                content: vec![text_block("收尾")],
                output_id: "msg_c".to_string(),
                ..AssistantMessage::default()
            }),
        ],
        ..RequestMessages::default()
    };
    request.merge_adjacent_assistant_turns();

    assert_eq!(request.messages.len(), 4, "merged message count");
    let Some(merged) = request.messages[1].as_assistant() else {
        panic!(
            "message[1] = {:?}, want AssistantMessage",
            request.messages[1]
        );
    };
    assert_eq!(merged.content.len(), 4, "want text+\\n+text+call");
    let Some(Content::Text(separator)) = merged.content.get(1) else {
        panic!(
            "content[1] = {:?}, want newline separator",
            merged.content[1]
        );
    };
    assert_eq!(separator.text, "\n");
    assert_eq!(merged.output_id, "msg_a");
    assert_eq!(merged.stop_reason, Some(StopReason::ToolUse));
    let Some(last) = request.messages[3].as_assistant() else {
        panic!(
            "message[3] = {:?}, want separate assistant turn",
            request.messages[3]
        );
    };
    assert_eq!(last.content.len(), 1);
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/llm/response_test.go
// ---------------------------------------------------------------------------

/// Port of `TestResponseEventProtocolCarriesPartialUsageAndFinalMessage`.
#[test]
fn response_event_protocol_carries_partial_usage_and_final_message() {
    let partial = AssistantMessage {
        content: vec![text_block("正在处理")],
        usage: Usage {
            input: 10,
            output: 2,
            total_tokens: 12,
            ..Usage::default()
        },
        stop_reason: Some(StopReason::Pending),
        ..AssistantMessage::default()
    };
    let call = ToolCall {
        id: "call-1".to_string(),
        name: "read_file".to_string(),
        arguments: r#"{"path":"config.json"}"#.to_string(),
        custom: false,
    };
    let final_message = AssistantMessage {
        content: vec![Content::ToolCall(call.clone())],
        usage: Usage {
            input: 10,
            output: 8,
            total_tokens: 18,
            ..Usage::default()
        },
        stop_reason: Some(StopReason::ToolUse),
        ..AssistantMessage::default()
    };
    let with_partial = |kind| ResponseEvent {
        kind,
        partial: Some(Arc::new(partial.clone())),
        ..ResponseEvent::default()
    };

    let events = vec![
        with_partial(ResponseEventType::Start),
        with_partial(ResponseEventType::TextStart),
        ResponseEvent {
            delta: "处理中".to_string(),
            ..with_partial(ResponseEventType::TextDelta)
        },
        ResponseEvent {
            content: "正在处理".to_string(),
            ..with_partial(ResponseEventType::TextEnd)
        },
        ResponseEvent {
            content_index: 1,
            ..with_partial(ResponseEventType::ThinkingStart)
        },
        ResponseEvent {
            content_index: 1,
            delta: "需要工具".to_string(),
            ..with_partial(ResponseEventType::ThinkingDelta)
        },
        ResponseEvent {
            content_index: 1,
            content: "需要工具".to_string(),
            ..with_partial(ResponseEventType::ThinkingEnd)
        },
        ResponseEvent {
            content_index: 2,
            tool_call_id: call.id.clone(),
            tool_name: call.name.clone(),
            ..with_partial(ResponseEventType::ToolCallStart)
        },
        ResponseEvent {
            content_index: 2,
            tool_call_id: call.id.clone(),
            delta: r#"{"path:"#.to_string(),
            ..with_partial(ResponseEventType::ToolCallDelta)
        },
        ResponseEvent {
            content_index: 2,
            tool_call: Some(call),
            ..with_partial(ResponseEventType::ToolCallEnd)
        },
        ResponseEvent {
            kind: ResponseEventType::Done,
            reason: Some(StopReason::ToolUse),
            message: Some(final_message),
            ..ResponseEvent::default()
        },
    ];

    for event in &events {
        event
            .validate()
            .unwrap_or_else(|err| panic!("{} Validate() error = {err}", event.kind.as_str()));
    }
}

/// Port of `TestResponseEventRejectsDoneWithoutFinalMessage`.
#[test]
fn response_event_rejects_done_without_final_message() {
    let event = ResponseEvent {
        kind: ResponseEventType::Done,
        reason: Some(StopReason::Stop),
        ..ResponseEvent::default()
    };
    assert!(
        event.validate().is_err(),
        "Validate() = nil, want missing final message error"
    );
}

/// Port of `TestResponseEventAllowsEmptyToolCallDelta` — providers may send
/// empty argument fragments as long as the call association id is kept.
#[test]
fn response_event_allows_empty_tool_call_delta() {
    let event = ResponseEvent {
        kind: ResponseEventType::ToolCallDelta,
        content_index: 0,
        tool_call_id: "call-1".to_string(),
        delta: String::new(),
        partial: Some(Arc::new(AssistantMessage {
            stop_reason: Some(StopReason::Pending),
            ..AssistantMessage::default()
        })),
        ..ResponseEvent::default()
    };
    event
        .validate()
        .expect("Validate() error, want empty tool call delta to be valid");
}

/// Port of `TestResponseEventRejectsToolCallDeltaWithoutID` — parallel tool
/// argument streams must not lose their stable association.
#[test]
fn response_event_rejects_tool_call_delta_without_id() {
    let event = ResponseEvent {
        kind: ResponseEventType::ToolCallDelta,
        content_index: 0,
        delta: "{}".to_string(),
        partial: Some(Arc::new(AssistantMessage {
            stop_reason: Some(StopReason::Pending),
            ..AssistantMessage::default()
        })),
        ..ResponseEvent::default()
    };
    assert!(
        event.validate().is_err(),
        "Validate() = nil, want missing tool call ID error"
    );
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/llm/failure_test.go
// ---------------------------------------------------------------------------

/// A plain error with a source chain — the Rust stand-in for Go's
/// `fmt.Errorf("outer: %w", errors.New("plain boom"))`.
#[derive(Debug)]
struct ChainedError {
    message: String,
    source: Option<Box<ChainedError>>,
}

impl std::fmt::Display for ChainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ChainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|err| err as &(dyn std::error::Error + 'static))
    }
}

/// Port of `TestClassifyConnectError` — a non-connect error classifies by
/// text, not by code-prefix guessing.
#[test]
fn classify_connect_error() {
    let err = ChainedError {
        message: "outer: plain boom".to_string(),
        source: Some(Box::new(ChainedError {
            message: "plain boom".to_string(),
            source: None,
        })),
    };
    let failure = domain::classify(&err);
    assert_eq!(failure.code, "", "want empty code for non-connect error");
    assert_eq!(failure.message, "outer: plain boom");
}

/// Port of `TestClassifyTypedFailure` — a typed record is taken directly and
/// derivation is idempotent; producer-set fields are not reset.
#[test]
fn classify_typed_failure() {
    let produced = Failure {
        code: "resource_exhausted".to_string(),
        message: "local gate".to_string(),
        local_gate: true,
        retry_after_seconds: 30,
        ..Failure::default()
    };
    let got = domain::classify(&produced);
    assert!(
        got.rate_limited && got.local_gate && got.retry_after_seconds == 30,
        "producer fields must survive derive: {got:?}"
    );
    // Re-classification is idempotent.
    let again = domain::classify(&got);
    assert!(
        again.retry_after_seconds == 30 && again.rate_limited,
        "re-classify must be idempotent: {again:?}"
    );
}

/// Port of `TestClassifyTextCodePrefix` — the "<code>: <msg>" dialect only
/// accepts known Connect code prefixes; local text is not a regime marker.
#[test]
fn classify_text_code_prefix() {
    assert_eq!(
        domain::classify_text("resource_exhausted: quota").code,
        "resource_exhausted"
    );
    assert_eq!(
        domain::classify_text("read request: failed").code,
        "",
        "want empty code for non-code prefix"
    );
}

/// Port of `TestUpstreamFault` — upstream-responsibility detection overrides
/// a code's fixable semantics: connect-wrapped transport breaks and the
/// "an internal error occurred" template are not the caller's problem.
#[test]
fn upstream_fault() {
    let got = domain::classify_text("invalid_argument: an internal error occurred (trace ID: x)");
    assert!(
        got.upstream_fault && !got.client_fixable,
        "masqueraded internal error must be UpstreamFault, not ClientFixable: {got:?}"
    );
    let got = domain::classify_text(
        "invalid_argument: protocol error: incomplete envelope: read: connection reset by peer",
    );
    assert!(
        got.upstream_fault,
        "frame truncation must be UpstreamFault: {got:?}"
    );
    let got = domain::classify_text(
        "unavailable: stream error: stream ID 1; REFUSED_STREAM; received from peer",
    );
    assert!(
        got.upstream_fault,
        "http2 RST_STREAM must be UpstreamFault: {got:?}"
    );
    // ENHANCE_YOUR_CALM mapped to resource_exhausted by connect-go — a
    // transport event, not an upstream rate limit: UpstreamFault set and
    // RateLimited suppressed (no latch, no rate_limit_exceeded downstream).
    let got = domain::classify_text(
        "resource_exhausted: bandwidth exhausted: stream error: stream ID 5; ENHANCE_YOUR_CALM; received from peer",
    );
    assert!(
        got.upstream_fault && !got.rate_limited,
        "transport-masqueraded resource_exhausted must be UpstreamFault, not RateLimited: {got:?}"
    );
    let got = domain::classify_text(
        "unavailable: http2: server sent GOAWAY and closed the connection; LastStreamID=9, ErrCode=NO_ERROR",
    );
    assert!(
        got.upstream_fault,
        "http2 GOAWAY must be UpstreamFault: {got:?}"
    );
    // Bare EOF (Go's io.EOF) ≈ Rust's UnexpectedEof io error.
    let eof = std::io::Error::from(std::io::ErrorKind::UnexpectedEof);
    let got = domain::classify(&eof);
    assert!(
        got.upstream_fault,
        "bare EOF must be UpstreamFault: {got:?}"
    );
    let got = domain::classify(&domain::Canceled);
    assert!(
        !got.upstream_fault && got.canceled,
        "canceled must not be UpstreamFault: {got:?}"
    );
    // A peer RST_STREAM CANCEL mapped to the canceled code (local ctx not
    // cancelled) is an upstream transport break, not a client cancel.
    let got =
        domain::classify_text("canceled: stream error: stream ID 3; CANCEL; received from peer");
    assert!(
        !got.canceled && got.upstream_fault,
        "peer RST_STREAM CANCEL must be UpstreamFault, not Canceled: {got:?}"
    );
    // Local cancel semantics unaffected: a canceled without transport
    // wording still classifies as client disconnect.
    let got = domain::classify_text("canceled: context canceled");
    assert!(
        got.canceled && !got.upstream_fault,
        "local cancel must stay Canceled, not UpstreamFault: {got:?}"
    );
    let got = domain::classify_text("invalid_argument: bad request");
    assert!(
        !got.upstream_fault && got.client_fixable,
        "plain invalid_argument must stay ClientFixable: {got:?}"
    );
    // A real upstream rate limit (EndStream-trailer semantic refusal) is
    // unaffected by transport wording.
    let got = domain::classify_text(
        "resource_exhausted: Reached overall message rate limit. Your limit will reset in 3 minutes.",
    );
    assert!(
        !got.upstream_fault && got.rate_limited,
        "real rate limit must stay RateLimited, not UpstreamFault: {got:?}"
    );
}

/// Port of `TestRetryAfterSeconds` — the reset window is parsed from upstream
/// rate-limit text (no Retry-After/RetryInfo upstream; "reset in N seconds"
/// is the only actionable hint).
#[test]
fn retry_after_seconds() {
    let failure = domain::classify_text(
        "resource_exhausted: rate limited. Your limit will reset in 42 seconds.",
    );
    assert_eq!(failure.retry_after_seconds, 42);
    let failure = domain::classify_text("resource_exhausted: quota exceeded");
    assert!(
        failure.retry_after_seconds == 0 && !failure.reset_hint,
        "no reset hint must report 0 and no hint"
    );
    // Explicit 0 and no-hint are two states: same 0 seconds, but the hint
    // marker records the declaration's presence.
    let failure = domain::classify_text("reset in 0 seconds");
    assert!(
        failure.retry_after_seconds == 0 && failure.reset_hint,
        "explicit zero reset must report 0 with hint present"
    );
}

/// Port of `TestRateLimitResetZero` — entry-point zero-value behavior and the
/// explicit-0 declaration (the minute-bucket alignment cases live with the
/// rate-gate task).
#[test]
fn rate_limit_reset_zero() {
    let now = std::time::SystemTime::now();
    assert!(
        domain::classify_text("plain error")
            .rate_limit_reset(now)
            .is_none(),
        "no reset hint must report false"
    );
    // Explicit 0 seconds: the declared reset instant is now — the latch
    // expires on it rather than a fallback duration.
    let reset = domain::classify_text("reset in 0 seconds").rate_limit_reset(now);
    assert_eq!(reset, Some(now), "explicit zero reset want now");
}

/// Port of `TestUpstreamTraceID` — the "(trace ID: …)" tail is extracted;
/// upstream errors are uniformly vague "internal error" text and the trace
/// ID is the only diagnostic anchor.
#[test]
fn upstream_trace_id() {
    assert_eq!(
        domain::classify_text("internal: an internal error occurred (trace ID: abc-def)").trace_id,
        "abc-def"
    );
    assert_eq!(domain::classify_text("internal: boom").trace_id, "");
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/api/common/toolchoice_test.go
// ---------------------------------------------------------------------------

/// Port of `TestParseOpenAIToolChoice`.
#[test]
fn parse_openai_tool_choice() {
    let raw = |text: &str| -> Box<serde_json::value::RawValue> {
        serde_json::value::RawValue::from_string(text.to_string()).unwrap()
    };
    let cases: &[(&str, Option<ToolChoiceMode>, &str)] = &[
        (r#""auto""#, None, ""),
        (r#""none""#, Some(ToolChoiceMode::None), ""),
        (r#""required""#, Some(ToolChoiceMode::Required), ""),
        (
            r#"{"type":"function","function":{"name":"read_file"}}"#,
            Some(ToolChoiceMode::Named),
            "read_file",
        ),
        // The Responses API's flat shape.
        (
            r#"{"type":"function","name":"exec"}"#,
            Some(ToolChoiceMode::Named),
            "exec",
        ),
        ("null", None, ""),
    ];
    for (text, want_mode, want_name) in cases {
        let mut dropped = Vec::new();
        let choice = common::parse_openai_tool_choice(Some(raw(text).as_ref()), &mut dropped)
            .unwrap_or_else(|err| panic!("{text}: {err}"));
        assert!(
            dropped.is_empty(),
            "{text}: dropped={dropped:?}, want empty"
        );
        match want_mode {
            None => assert!(choice.is_none(), "{text}: got {choice:?}, want nil"),
            Some(mode) => {
                let choice = choice.unwrap_or_else(|| panic!("{text}: got nil"));
                assert_eq!(choice.mode, *mode, "{text}");
                assert_eq!(choice.tool_name, *want_name, "{text}");
            }
        }
    }
    let mut dropped = Vec::new();
    assert!(
        common::parse_openai_tool_choice(Some(raw(r#""bogus""#).as_ref()), &mut dropped).is_err(),
        "bogus option should error"
    );
    // Hosted-tool constraints (file_search/allowed_tools etc.) are
    // unsatisfiable: recorded as dropped and passed through as auto.
    let mut dropped = Vec::new();
    let choice = common::parse_openai_tool_choice(
        Some(raw(r#"{"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"exec"}]}"#).as_ref()),
        &mut dropped,
    )
    .expect("allowed_tools");
    assert!(choice.is_none(), "allowed_tools => {choice:?}, want nil");
    assert_eq!(dropped, ["tool_choice:allowed_tools"]);
}

/// Port of `TestParseAnthropicToolChoice`.
#[test]
fn parse_anthropic_tool_choice() {
    let raw = |text: &str| -> Box<serde_json::value::RawValue> {
        serde_json::value::RawValue::from_string(text.to_string()).unwrap()
    };
    // "any" normalizes to required (upstream option_name rejects "any");
    // the canonical field is disable_parallel_tool_use.
    let (choice, disable) = common::parse_anthropic_tool_choice(Some(
        raw(r#"{"type":"any","disable_parallel_tool_use":true}"#).as_ref(),
    ))
    .expect("any");
    let choice = choice.expect("any => want required");
    assert_eq!(choice.mode, ToolChoiceMode::Required);
    assert!(disable, "any => want disable");
    // The legacy disable_parallel_tool_calls spelling stays compatible.
    let (_, disable) = common::parse_anthropic_tool_choice(Some(
        raw(r#"{"type":"auto","disable_parallel_tool_calls":true}"#).as_ref(),
    ))
    .expect("legacy spelling");
    assert!(disable, "legacy spelling => want disable");
    let (choice, _) =
        common::parse_anthropic_tool_choice(Some(raw(r#"{"type":"tool","name":"Bash"}"#).as_ref()))
            .expect("tool");
    let choice = choice.expect("tool => want named");
    assert_eq!(choice.mode, ToolChoiceMode::Named);
    assert_eq!(choice.tool_name, "Bash");
    let (choice, _) = common::parse_anthropic_tool_choice(Some(raw(r#"{"type":"none"}"#).as_ref()))
        .expect("none");
    assert_eq!(
        choice.expect("none => want none").mode,
        ToolChoiceMode::None
    );
    let (choice, _) = common::parse_anthropic_tool_choice(Some(raw(r#"{"type":"auto"}"#).as_ref()))
        .expect("auto");
    assert!(choice.is_none(), "auto => want nil");
    assert!(
        common::parse_anthropic_tool_choice(Some(raw(r#"{"type":"bogus"}"#).as_ref())).is_err(),
        "bogus type should error"
    );
    assert!(
        common::parse_anthropic_tool_choice(Some(raw(r#"{"type":"tool"}"#).as_ref())).is_err(),
        "tool without name should error"
    );
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/api/openai/chat/request_test.go
// ---------------------------------------------------------------------------

/// Port of chat `TestDecodeRequestBuildsConversationContext` — system prompt,
/// multimodal input, `tools` and tool results are preserved.
#[test]
fn chat_decode_request_builds_conversation_context() {
    let data = r#"{
  "model": "gpt-test",
  "messages": [
    {"role": "system", "content": "你是一个谨慎的助手。"},
    {"role": "user", "content": [
      {"type": "text", "text": "读取这个文件"},
      {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}}
    ]},
    {"role": "assistant", "content": null, "tool_calls": [{"id": "call-1", "type": "function", "function": {"name": "read_file", "arguments": "{\"path\":\"a.txt\"}"}}]},
    {"role": "tool", "tool_call_id": "call-1", "content": "内容"}
  ],
  "tools": [{"type": "function", "function": {"name": "read_file", "description": "读取文件", "parameters": {"type": "object"}}}]
}"#;

    let request = chat::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.model, "gpt-test");
    assert_eq!(request.context.system_prompt, "你是一个谨慎的助手。");
    assert_eq!(request.context.messages.len(), 3, "message count");
    assert!(matches!(request.context.messages[0], Message::User(_)));
    assert!(matches!(request.context.messages[1], Message::Assistant(_)));
    assert!(matches!(
        request.context.messages[2],
        Message::ToolResult(_)
    ));
    assert_eq!(request.context.tools.len(), 1);
    assert_eq!(request.context.tools[0].name, "read_file");
    let user = request.context.messages[0].as_user().unwrap();
    assert!(
        matches!(user.content[1], Content::Image(_)),
        "user content[1] = {:?}, want ImageContent",
        user.content[1]
    );
    request.context.validate().expect("context validation");
}

/// Port of chat `TestDecodeRequestAcceptsPlainString`.
#[test]
fn chat_decode_request_accepts_plain_string() {
    let request = chat::decode_request(
        r#"{"model":"gpt-test","messages":[{"role":"user","content":"hello"}]}"#.as_bytes(),
        true,
    )
    .expect("decode");
    assert_eq!(request.context.messages.len(), 1);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("message content = {:?}", user.content);
    };
    assert_eq!(text.text, "hello");
}

/// Port of chat `TestDecodeRequestAcceptsFunctionCallArguments` — tool-call
/// arguments are preserved as a `JSON` object.
#[test]
fn chat_decode_request_accepts_function_call_arguments() {
    let request = chat::decode_request(
        r#"{"model":"gpt-test","messages":[{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"a.txt\"}"}}]}]}"#.as_bytes(),
        true,
    )
    .expect("decode");
    let assistant = request.context.messages[0].as_assistant().unwrap();
    let Some(Content::ToolCall(call)) = assistant.content.first() else {
        panic!("assistant content[0] = {:?}", assistant.content);
    };
    assert_eq!(call.name, "read_file");
    assert!(
        serde_json::from_str::<serde::de::IgnoredAny>(&call.arguments).is_ok()
            && call.arguments.contains("\"path\""),
        "tool call arguments = {}",
        call.arguments
    );
}

/// Port of chat `TestDecodeRequestAcceptsLegacyFunctionDialect` — the
/// pre-2023-06 function-calling shape: `functions` declarations, an
/// assistant `function_call` with a synthesized id, `role:"function"`
/// results reconciled by name, and the request-level `function_call`
/// selector.
#[test]
fn chat_decode_request_accepts_legacy_function_dialect() {
    let data = r#"{
  "model": "gpt-test",
  "messages": [
    {"role": "user", "content": "读文件"},
    {"role": "assistant", "content": null, "function_call": {"name": "read_file", "arguments": "{\"path\":\"a.txt\"}"}},
    {"role": "function", "name": "read_file", "content": "内容"}
  ],
  "functions": [{"name": "read_file", "description": "读取文件", "parameters": {"type": "object"}}],
  "function_call": {"name": "read_file"}
}"#;

    let request = chat::decode_request(data.as_bytes(), true).expect("decode");
    let assistant = request.context.messages[1].as_assistant().unwrap();
    let Some(Content::ToolCall(call)) = assistant.content.first() else {
        panic!("assistant content[0] = {:?}", assistant.content);
    };
    assert_eq!(call.name, "read_file");
    let result = request.context.messages[2].as_tool_result().unwrap();
    assert_eq!(
        result.tool_call_id, call.id,
        "tool result must pair by synthesized id"
    );
    assert_eq!(request.context.tools.len(), 1);
    assert_eq!(request.context.tools[0].name, "read_file");
    let choice = request.context.tool_choice.as_ref().expect("ToolChoice");
    assert_eq!(choice.mode, ToolChoiceMode::Named);
    assert_eq!(choice.tool_name, "read_file");
    request.context.validate().expect("context validation");
}

/// Port of chat `TestDecodeRequestTrailingData`.
#[test]
fn chat_decode_request_trailing_data() {
    let data = r#"{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]} extra"#;
    assert!(
        chat::decode_request(data.as_bytes(), true).is_err(),
        "trailing data should error"
    );
}

/// Port of chat `TestDecodeRequestMergesAdjacentAssistants` — consecutive
/// assistant messages merge into one turn (same IR-layer implementation as
/// the responses face), preventing fake wire turn boundaries.
#[test]
fn chat_decode_request_merges_adjacent_assistants() {
    let data = r#"{"model":"gpt-test","messages":[
	{"role":"assistant","content":"第一段"},
	{"role":"assistant","content":"第二段","tool_calls":[{"id":"call-1","type":"function","function":{"name":"read","arguments":"{}"}}]},
	{"role":"tool","tool_call_id":"call-1","content":"结果"},
	{"role":"assistant","content":"下一回合"}
]}"#;
    let request = chat::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 3, "want 3 (merged)");
    let merged = request.context.messages[0].as_assistant().unwrap();
    assert_eq!(
        merged.stop_reason,
        Some(StopReason::ToolUse),
        "want merged assistant with toolUse"
    );
    assert!(matches!(
        request.context.messages[1],
        Message::ToolResult(_)
    ));
    assert!(
        matches!(request.context.messages[2], Message::Assistant(_)),
        "want separate assistant turn"
    );
}

/// Port of chat `TestDecodeRequestEmptyContent` — same convention as the
/// anthropic face: a user's content:[] lands as an empty-text placeholder
/// with `empty_message:user`; an assistant with no product at all records
/// `empty_message:assistant`.
#[test]
fn chat_decode_request_empty_content() {
    let data = r#"{"model":"gpt-test","messages":[
	{"role":"user","content":[]},
	{"role":"assistant"},
	{"role":"user","content":"hi"}
]}"#;
    let request = chat::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 3);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("empty user placeholder = {:?}", user.content);
    };
    assert_eq!(text.text, "");
    for want in ["empty_message:user", "empty_message:assistant"] {
        assert!(
            dropped_contains(&request.context.dropped, want),
            "dropped = {:?}, want {want}",
            request.context.dropped
        );
    }
}

/// Port of chat `TestDecodeRequestSkipsFieldScan` — `collect_dropped`=false
/// skips the top-level unconsumed-field scan (the production path with
/// debuglog off); other dropped accounting is unaffected.
#[test]
fn chat_decode_request_skips_field_scan() {
    let data = r#"{"model":"gpt-test","messages":[{"role":"user","content":[]}],"store":true,"reasoning_effort":"high"}"#;
    let with_scan = chat::decode_request(data.as_bytes(), true).expect("decode with scan");
    let without_scan = chat::decode_request(data.as_bytes(), false).expect("decode without scan");
    for want in ["field:store", "field:reasoning_effort"] {
        assert!(
            dropped_contains(&with_scan.context.dropped, want),
            "collectDropped=true dropped = {:?}, want {want}",
            with_scan.context.dropped
        );
    }
    for marker in &without_scan.context.dropped {
        assert!(
            !marker.starts_with("field:"),
            "collectDropped=false still collected field marker {marker:?}"
        );
    }
    assert!(
        dropped_contains(&without_scan.context.dropped, "empty_message:user"),
        "collectDropped=false dropped = {:?}, want empty_message:user",
        without_scan.context.dropped
    );
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/api/openai/responses/request_test.go
// ---------------------------------------------------------------------------

/// Port of responses `TestDecodeRequestBuildsConversationContext`.
#[test]
fn responses_decode_request_builds_conversation_context() {
    let data = r#"{
  "model": "gpt-test",
  "instructions": "你是一个谨慎的助手。",
  "input": [
    {"type":"message","role":"user","content":[
      {"type":"input_text","text":"读取这个文件"},
      {"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}
    ]},
    {"type":"function_call","call_id":"call-1","name":"read_file","arguments":"{\"path\":\"a.txt\"}"},
    {"type":"function_call_output","call_id":"call-1","output":"内容"}
  ],
  "tools": [{"type":"function","name":"read_file","description":"读取文件","parameters":{"type":"object"}}]
}"#;

    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.model, "gpt-test");
    assert_eq!(request.context.system_prompt, "你是一个谨慎的助手。");
    assert_eq!(request.context.messages.len(), 3, "message count");
    assert!(matches!(request.context.messages[0], Message::User(_)));
    assert!(matches!(request.context.messages[1], Message::Assistant(_)));
    assert!(matches!(
        request.context.messages[2],
        Message::ToolResult(_)
    ));
    assert_eq!(request.context.tools.len(), 1);
    assert_eq!(request.context.tools[0].name, "read_file");
    request.context.validate().expect("context validation");
}

/// Port of responses `TestDecodeRequestAcceptsStringInput`.
#[test]
fn responses_decode_request_accepts_string_input() {
    let request =
        responses::decode_request(r#"{"model":"gpt-test","input":"hello"}"#.as_bytes(), true)
            .expect("decode");
    assert_eq!(request.context.messages.len(), 1);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("message content = {:?}", user.content);
    };
    assert_eq!(text.text, "hello");
}

/// Port of responses `TestDecodeRequestAcceptsImageURLObject` — the `IDE`'s
/// common `image_url` object shape decodes.
#[test]
fn responses_decode_request_accepts_image_url_object() {
    let data = r#"{
  "model":"gpt-test",
  "input":[{"role":"user","content":[
    {"type":"input_text","text":"see"},
    {"type":"input_image","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}
  ]}]
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let user = request.context.messages[0].as_user().unwrap();
    assert_eq!(user.content.len(), 2);
    let Some(Content::Image(image)) = user.content.get(1) else {
        panic!("content[1] = {:?}, want ImageContent", user.content[1]);
    };
    assert_eq!(image.mime_type, "image/png");
    assert!(
        !image.data.is_empty() && !image.data.starts_with("data:"),
        "image = {image:?}"
    );
}

/// Port of responses `TestDecodeRequestAcceptsChatCompletionsImagePart` —
/// the type=`image_url` Chat-style part.
#[test]
fn responses_decode_request_accepts_chat_completions_image_part() {
    let data = r#"{
  "model":"gpt-test",
  "input":[{"role":"user","content":[
    {"type":"text","text":"see"},
    {"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}
  ]}]
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let user = request.context.messages[0].as_user().unwrap();
    assert!(
        matches!(user.content[1], Content::Image(_)),
        "content[1] = {:?}, want ImageContent",
        user.content[1]
    );
}

/// Port of responses `TestDecodeRequestPreservesMalformedToolArguments` —
/// non-object tool arguments travel the Custom channel verbatim (same as
/// chat/anthropic); swallowing them into {} or 400ing would silently empty
/// or lose the call semantics.
#[test]
fn responses_decode_request_preserves_malformed_tool_arguments() {
    let data = r#"{"model":"gpt-test","input":[{"type":"function_call","call_id":"call-1","name":"tool","arguments":"[]"}]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let assistant = request.context.messages[0].as_assistant().unwrap();
    let Some(Content::ToolCall(call)) = assistant.content.first() else {
        panic!("assistant content[0] = {:?}", assistant.content);
    };
    assert!(
        call.custom && call.arguments == "[]",
        "malformed arguments must be preserved as Custom, got {call:?}"
    );
}

/// Port of responses `TestDecodeRequestRetainsRawSchema` — tool schemas are
/// kept as raw `JSON`.
#[test]
fn responses_decode_request_retains_raw_schema() {
    let request = responses::decode_request(
        r#"{"model":"gpt-test","input":"hi","tools":[{"type":"function","name":"tool","parameters":{"type":"object","additionalProperties":false}}]}"#.as_bytes(),
        true,
    )
    .expect("decode");
    let schema: serde_json::Value =
        serde_json::from_str(&request.context.tools[0].input_schema).expect("schema json");
    assert_eq!(schema["additionalProperties"], serde_json::json!(false));
}

/// Port of responses `TestDecodeRequestCustomToolDeclaration` — type:"custom"
/// `tools` are wrapped as a single-input-parameter function declaration with
/// the Custom flag; format.definition (the only grammar spec the model
/// sees) is injected into the description; unknown tool types record
/// dropped.
#[test]
fn responses_decode_request_custom_tool_declaration() {
    let request = responses::decode_request(
        r#"{"model":"gpt-test","input":"hi","tools":[
		{"type":"custom","name":"apply_patch","description":"Patch files","format":{"syntax":"lark","definition":"patch_grammar"}},
		{"type":"custom","name":"no_grammar"},
		{"type":"mystery","name":"dropped_tool"}
	]}"#.as_bytes(),
        true,
    )
    .expect("decode");
    assert_eq!(request.context.tools.len(), 2, "want 2 decoded");
    let tool = &request.context.tools[0];
    assert!(
        tool.custom && tool.name == "apply_patch",
        "tool = {tool:?}, want custom apply_patch"
    );
    assert_eq!(
        tool.input_schema,
        r#"{"type":"object","properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}"#
    );
    assert_eq!(
        tool.description,
        "Patch files\n\nInput grammar (lark):\npatch_grammar"
    );
    assert_eq!(
        request.context.tools[1].description, "",
        "no-format custom tool description"
    );
    assert!(
        dropped_contains(&request.context.dropped, "tool:mystery"),
        "dropped = {:?}, want tool:mystery",
        request.context.dropped
    );
}

/// Port of responses `TestDecodeRequestAcceptsMessageWithoutType` — the
/// role+content shorthand sent by official `OpenAI` examples and `SDKs`.
#[test]
fn responses_decode_request_accepts_message_without_type() {
    let data = r#"{
  "model": "glm-5.2",
  "input": [{
    "role": "user",
    "content": [{"type":"input_text","text":"hello"}]
  }],
  "stream": true
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 1);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("message content = {:?}", user.content);
    };
    assert_eq!(text.text, "hello");
}

/// Port of responses `TestDecodeRequestAttachesReasoningSummary` — a
/// reasoning item's summary text attaches to the next assistant product as
/// a leading `ThinkingContent` block.
#[test]
fn responses_decode_request_attaches_reasoning_summary() {
    let data = r#"{
  "model": "gpt-test",
  "input": [
    {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},
    {"type":"reasoning","summary":[{"type":"summary_text","text":"计划：先读文件再改"}]},
    {"type":"message","role":"assistant","content":[{"type":"output_text","text":"好的"}]},
    {"type":"reasoning","summary":[{"type":"summary_text","text":"需要调用 read_file"}],"encrypted_content":"sealed.v1.xyz"},
    {"type":"function_call","call_id":"call-1","name":"read_file","arguments":"{}"},
    {"type":"function_call_output","call_id":"call-1","output":"内容"}
  ]
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    // The same turn's assistant message and function_call merge into one
    // message: thinking/text blocks keep input order in the merged content.
    assert_eq!(request.context.messages.len(), 3, "message count");
    let assistant = request.context.messages[1].as_assistant().unwrap();
    let Some(Content::Thinking(thinking)) = assistant.content.first() else {
        panic!(
            "assistant content[0] = {:?}, want ThinkingContent",
            assistant.content[0]
        );
    };
    assert_eq!(thinking.thinking, "计划：先读文件再改");
    let Some(Content::Thinking(thinking)) = assistant.content.get(2) else {
        panic!(
            "assistant content[2] = {:?}, want ThinkingContent",
            assistant.content[2]
        );
    };
    assert_eq!(thinking.thinking, "需要调用 read_file");
    assert_eq!(
        thinking.thinking_signature, "sealed.v1.xyz",
        "want sealed.v1.xyz replay"
    );
    assert!(
        matches!(assistant.content[3], Content::ToolCall(_)),
        "assistant content[3] = {:?}, want ToolCall",
        assistant.content[3]
    );
}

/// Port of responses `TestDecodeRequestMergesAssistantTurnItems` — multiple
/// input items flattened from one turn (assistant message / reasoning /
/// `function_call`) merge into one `AssistantMessage`; per-item messages would
/// create fake wire turn boundaries raising premature `end_turn` probability
/// (issue #2). Negative case: assistant products separated by
/// `function_call_output` belong to different turns and must not merge.
#[test]
fn responses_decode_request_merges_assistant_turn_items() {
    let data = r#"{
	  "model": "gpt-test",
	  "input": [
	    {"type":"message","role":"user","content":[{"type":"input_text","text":"看看项目结构"}]},
	    {"type":"reasoning","summary":[{"type":"summary_text","text":"先看 README"}]},
	    {"type":"message","role":"assistant","id":"msg_1","content":[{"type":"output_text","text":"我先读 README"}]},
	    {"type":"reasoning","summary":[{"type":"summary_text","text":"需要 read_file"}],"encrypted_content":"sealed.v1.sig"},
	    {"type":"function_call","call_id":"c1","name":"read_file","arguments":"{\"path\":\"README.md\"}"},
	    {"type":"function_call_output","call_id":"c1","output":"readme 内容"},
	    {"type":"function_call","call_id":"c2","name":"list_dir","arguments":"{}"},
	    {"type":"function_call_output","call_id":"c2","output":"file list"},
	    {"type":"message","role":"user","content":[{"type":"input_text","text":"继续"}]}
	  ]
	}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 6, "message count");
    let assistant = request.context.messages[1].as_assistant().unwrap();
    assert_eq!(assistant.content.len(), 4, "want 4 blocks");
    let Some(Content::Thinking(thinking)) = assistant.content.first() else {
        panic!(
            "content[0] = {:?}, want turn reasoning",
            assistant.content[0]
        );
    };
    assert_eq!(thinking.thinking, "先看 README");
    let Some(Content::Text(text)) = assistant.content.get(1) else {
        panic!(
            "content[1] = {:?}, want announcement text",
            assistant.content[1]
        );
    };
    assert_eq!(text.text, "我先读 README");
    let Some(Content::Thinking(thinking)) = assistant.content.get(2) else {
        panic!(
            "content[2] = {:?}, want signed reasoning",
            assistant.content[2]
        );
    };
    assert_eq!(thinking.thinking_signature, "sealed.v1.sig");
    let Some(Content::ToolCall(call)) = assistant.content.get(3) else {
        panic!(
            "content[3] = {:?}, want read_file ToolCall",
            assistant.content[3]
        );
    };
    assert_eq!(call.id, "c1");
    assert_eq!(call.name, "read_file");
    assert_eq!(
        assistant.stop_reason,
        Some(StopReason::ToolUse),
        "want toolUse for merged turn with call"
    );
    assert_eq!(assistant.output_id, "msg_1", "want last non-empty msg_1");
    assert!(matches!(
        request.context.messages[2],
        Message::ToolResult(_)
    ));
    // Negative case: the function_call after the output separator belongs
    // to the next turn and must be its own message.
    let next = request.context.messages[3].as_assistant().unwrap();
    assert_eq!(next.content.len(), 1, "want separate AssistantMessage");
    let Some(Content::ToolCall(call)) = next.content.first() else {
        panic!(
            "message[3] content = {:?}, want list_dir ToolCall",
            next.content
        );
    };
    assert_eq!(call.id, "c2");

    // Two assistant messages in one turn join their texts with "\n";
    // OutputID takes the run's last non-empty.
    let request = responses::decode_request(
        r#"{"model":"m","input":[
		{"type":"message","role":"assistant","id":"msg_a","content":[{"type":"output_text","text":"第一段"}]},
		{"type":"message","role":"assistant","id":"msg_b","content":[{"type":"output_text","text":"第二段"}]}
	]}"#.as_bytes(),
        true,
    )
    .expect("decode");
    assert_eq!(request.context.messages.len(), 1, "want 1 merged");
    let joined = request.context.messages[0].as_assistant().unwrap();
    let texts: Vec<&str> = joined
        .content
        .iter()
        .map(|block| match block {
            Content::Text(text) => text.text.as_str(),
            other => panic!("content = {other:?}, want text blocks only"),
        })
        .collect();
    assert_eq!(texts.concat(), "第一段\n第二段", "want newline-separated");
    assert_eq!(joined.output_id, "msg_b", "want last non-empty msg_b");
}

/// Port of responses `TestDecodeRequestDropsOrphanReasoning` — reasoning not
/// followed by an assistant product must not attach to a later user
/// message.
#[test]
fn responses_decode_request_drops_orphan_reasoning() {
    let data = r#"{
  "model": "gpt-test",
  "input": [
    {"type":"reasoning","summary":[{"type":"summary_text","text":"orphan"}]},
    {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}
  ]
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let user = request.context.messages[0].as_user().unwrap();
    assert_eq!(user.content.len(), 1, "want single text block");
}

/// Port of responses `TestDecodeRequestToleratesOrphanToolOutput` — an
/// isolated `function_call_output` (its `function_call` lost to compaction)
/// demotes to USER text at the decode tail with a dropped marker instead of
/// failing the request.
#[test]
fn responses_decode_request_tolerates_orphan_tool_output() {
    let data = r#"{
  "model": "gpt-test",
  "input": [
    {"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]},
    {"type":"function_call_output","call_id":"call-gone","output":"残留结果"}
  ]
}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let Some(user) = request.context.messages[1].as_user() else {
        panic!(
            "message[1] = {:?}, want demoted UserMessage",
            request.context.messages[1]
        );
    };
    assert_eq!(user.content.len(), 2, "want prefix + body");
    assert!(
        dropped_contains(&request.context.dropped, "unmatched_tool_call_id:call-gone"),
        "dropped = {:?}, want unmatched_tool_call_id:call-gone",
        request.context.dropped
    );
}

/// Port of responses `TestDecodeRequestIgnoresUnsupportedExtensions` —
/// upstream-added fields and types must not block recognizable conversation
/// content.
#[test]
fn responses_decode_request_ignores_unsupported_extensions() {
    let request = responses::decode_request(
        r#"{
  "model":"model",
  "client_metadata":{"client":"codex"},
  "input":[
    {"type":"additional_tools","role":"developer","tools":[{"name":"unknown"}]},
    {"content":"untyped extension"},
    {"type":"message","role":"future_role","content":"ignored"},
    {"type":"message","role":"user","content":[
      {"type":"future_content","value":"ignored"},
      {"type":"input_text","text":"hello"}
    ]}
  ],
  "tools":[
    {"type":"web_search_preview"},
    {"type":"function","name":"known","parameters":{"type":"object"}}
  ]
}"#
        .as_bytes(),
        true,
    )
    .expect("decode");
    // Unknown items demote to USER text keeping content: additional_tools
    // and the type-less item each produce a demoted message, the
    // future_role message is dropped, recognizable content is unaffected.
    assert_eq!(request.context.messages.len(), 3, "message count");
    for index in 0..2 {
        let Some(message) = request.context.messages[index].as_user() else {
            panic!(
                "message {index} = {:?}, want UserMessage",
                request.context.messages[index]
            );
        };
        let Some(Content::Text(text)) = message.content.first() else {
            panic!("demoted message {index} = {:?}", message.content);
        };
        assert!(
            text.text.starts_with("[input item type="),
            "demoted message {index} = {:?}",
            text.text
        );
    }
    let message = request.context.messages[2].as_user().unwrap();
    let Some(Content::Text(text)) = message.content.first() else {
        panic!("message content = {:?}", message.content);
    };
    assert_eq!(message.content.len(), 1);
    assert_eq!(text.text, "hello");
    assert_eq!(request.context.tools.len(), 1);
    assert_eq!(request.context.tools[0].name, "known");
}

/// Port of responses `TestDecodeRequestAcceptsCallIDVariants` — all four call
/// id field names on `function_call_output` are accepted: `call_id` is
/// canonical; the rest come from Chat habits / camelCase / id-as-call-id.
#[test]
fn responses_decode_request_accepts_call_id_variants() {
    for field in ["call_id", "tool_call_id", "callId", "id"] {
        let data = format!(
            r#"{{"model":"m","input":[
			{{"type":"function_call","call_id":"c1","name":"t","arguments":"{{}}"}},
			{{"type":"function_call_output","{field}":"c1","output":"ok"}}
		]}}"#
        );
        let request = responses::decode_request(data.as_bytes(), true)
            .unwrap_or_else(|err| panic!("{field}: {err}"));
        let Some(result) = request.context.messages[1].as_tool_result() else {
            panic!("{field}: message[1] = {:?}", request.context.messages[1]);
        };
        assert_eq!(result.tool_call_id, "c1", "{field}");
    }
}

/// Port of responses `TestDecodeRequestReplaysOpenAIReasoningSignature` — an
/// `openai`-type signature (serialized `reasoning-item` array) replays verbatim
/// from `encrypted_content` as signature+`signature_type` — the Responses
/// multi-turn reasoning replay channel.
#[test]
fn responses_decode_request_replays_openai_reasoning_signature() {
    let blob = r#"[{"id":"rs_9","type":"reasoning","encrypted_content":"gAAA","summary":[],"content":[],"status":""}]"#;
    let data = format!(
        r#"{{"model":"m","input":[
		{{"type":"reasoning","id":"rs_9","summary":[],"encrypted_content":"{}"}},
		{{"type":"message","role":"assistant","id":"msg_7","content":[{{"type":"output_text","text":"done"}}]}}
	]}}"#,
        blob.replace('"', "\\\"")
    );
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let Some(assistant) = request.context.messages[0].as_assistant() else {
        panic!("message[0] = {:?}", request.context.messages[0]);
    };
    let Some(Content::Thinking(thinking)) = assistant.content.first() else {
        panic!("thinking block = {:?}", assistant.content[0]);
    };
    assert_eq!(thinking.signature_type, "openai");
    assert_eq!(thinking.thinking_signature, blob);
    assert!(
        thinking.redacted,
        "signature-only reasoning must be marked redacted"
    );
    assert_eq!(assistant.output_id, "msg_7");
}

/// Port of responses `TestDecodeRequestDropsForeignReasoningPayload` — a
/// foreign opaque `encrypted_content` (neither `sealed.*` nor `reasoning-item`
/// `JSON`) is not passed through.
#[test]
fn responses_decode_request_drops_foreign_reasoning_payload() {
    let data = r#"{"model":"m","input":[
		{"type":"reasoning","summary":[],"encrypted_content":"gAAAAB-foreign"},
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let assistant = request.context.messages[0].as_assistant().unwrap();
    for block in &assistant.content {
        if let Content::Thinking(thinking) = block {
            assert!(
                thinking.thinking_signature.is_empty(),
                "foreign signature must be dropped, got {thinking:?}"
            );
        }
    }
    assert!(
        dropped_contains(&request.context.dropped, "reasoning:encrypted_content"),
        "dropped = {:?}, want reasoning:encrypted_content",
        request.context.dropped
    );
}

/// Port of responses `TestDecodeRequestCustomToolCall` — a `custom_tool_call`
/// item's input travels the Custom channel verbatim — freeform argument
/// bodies are not `JSON`.
#[test]
fn responses_decode_request_custom_tool_call() {
    let data = r#"{"model":"m","input":[
		{"type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"*** Begin Patch\n+x"},
		{"type":"custom_tool_call_output","call_id":"c1","output":"patched"}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let assistant = request.context.messages[0].as_assistant().unwrap();
    let Some(Content::ToolCall(call)) = assistant.content.first() else {
        panic!("custom tool call = {:?}", assistant.content[0]);
    };
    assert!(
        call.custom && call.arguments == "*** Begin Patch\n+x",
        "custom tool call = {call:?}"
    );
    let result = request.context.messages[1].as_tool_result().unwrap();
    assert_eq!(result.tool_call_id, "c1");
}

/// Port of responses `TestDecodeRequestToolOutputPartArray` — a
/// `function_call_output` part array (with `input_image`) decodes to content
/// blocks rather than literal `JSON` text.
#[test]
fn responses_decode_request_tool_output_part_array() {
    let data = r#"{"model":"m","input":[
		{"type":"function_call","call_id":"c1","name":"shot","arguments":"{}"},
		{"type":"function_call_output","call_id":"c1","output":[
			{"type":"input_text","text":"see"},
			{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgo="}
		]}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let result = request.context.messages[1].as_tool_result().unwrap();
    assert_eq!(
        result.content.len(),
        2,
        "tool result content = {:?}",
        result.content
    );
    assert!(
        matches!(result.content[1], Content::Image(_)),
        "content[1] = {:?}, want ImageContent",
        result.content[1]
    );
}

/// Port of responses `TestDecodeRequestTrailingData`.
#[test]
fn responses_decode_request_trailing_data() {
    let data = r#"{"model":"gpt-test","input":"hi"} trailing"#;
    assert!(
        responses::decode_request(data.as_bytes(), true).is_err(),
        "trailing data should error"
    );
}

/// Port of responses `TestDecodeRequestKeepsEmptyMessage` — empty-content
/// messages do not silently vanish: `empty_message:<role>` is recorded, a
/// user gets an empty-text placeholder, an assistant keeps the empty
/// message — same convention as the anthropic face.
#[test]
fn responses_decode_request_keeps_empty_message() {
    let data = r#"{"model":"m","input":[
		{"type":"message","role":"user","content":[]},
		{"type":"message","role":"assistant","content":[]},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 3);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("empty user placeholder = {:?}", user.content);
    };
    assert_eq!(text.text, "");
    assert!(
        matches!(request.context.messages[1], Message::Assistant(_)),
        "empty assistant message dropped: {:?}",
        request.context.messages[1]
    );
    for want in ["empty_message:user", "empty_message:assistant"] {
        assert!(
            dropped_contains(&request.context.dropped, want),
            "dropped = {:?}, want {want}",
            request.context.dropped
        );
    }
}

/// Port of responses `TestDecodeRequestMalformedToolOutputParts` — a
/// part-looking `function_call_output` that fails to decode (e.g. a bad image
/// part) degrades to literal `JSON` text with a dropped marker — the
/// tolerance is deliberate but must reconcile.
#[test]
fn responses_decode_request_malformed_tool_output_parts() {
    let data = r#"{"model":"m","input":[
		{"type":"function_call","call_id":"c1","name":"shot","arguments":"{}"},
		{"type":"function_call_output","call_id":"c1","output":[
			{"type":"input_image","image_url":"http://example.com/x.png"}
		]}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    let result = request.context.messages[1].as_tool_result().unwrap();
    let Some(Content::Text(text)) = result.content.first() else {
        panic!(
            "malformed output should fall back to literal text, got {:?}",
            result.content
        );
    };
    assert!(
        text.text.contains("input_image"),
        "literal fallback = {:?}",
        text.text
    );
    assert!(
        dropped_contains(&request.context.dropped, "tool_output:malformed_parts"),
        "dropped = {:?}, want tool_output:malformed_parts",
        request.context.dropped
    );
}

/// Port of responses `TestDecodeRequestPositionalToolResults` — orphan
/// detection is positional: an output whose id matches nothing but which
/// still has an unconsumed call ahead stays TOOL (upstream consumes in
/// order without checking ids); only orphans ahead of all calls demote to
/// USER text.
#[test]
fn responses_decode_request_positional_tool_results() {
    let data = r#"{"model":"m","input":[
		{"type":"function_call_output","call_id":"call-early","output":"孤儿"},
		{"type":"function_call","call_id":"c1","name":"read","arguments":"{}"},
		{"type":"function_call_output","call_id":"call-mismatch","output":"按位置消化"}
	]}"#;
    let request = responses::decode_request(data.as_bytes(), true).expect("decode");
    assert!(
        matches!(request.context.messages[0], Message::User(_)),
        "orphan output not demoted: {:?}",
        request.context.messages[0]
    );
    let Some(result) = request.context.messages[2].as_tool_result() else {
        panic!(
            "positionally-consumable output was demoted: {:?}",
            request.context.messages[2]
        );
    };
    assert_eq!(result.tool_call_id, "call-mismatch");
}

// ---------------------------------------------------------------------------
// Ported Go cases: internal/api/anthropic/messages/request_test.go
// ---------------------------------------------------------------------------

/// Port of anthropic `TestDecodeRequestBuildsConversationContext` — system,
/// image, `tool_use` and `tool_result` are preserved.
#[test]
fn anthropic_decode_request_builds_conversation_context() {
    let data = r#"{
  "model": "claude-test",
  "system": "你是一个谨慎的助手。",
  "messages": [
    {"role": "user", "content": [
      {"type": "text", "text": "读取这个文件"},
      {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "iVBORw0KGgo="}}
    ]},
    {"role": "assistant", "content": [{"type": "tool_use", "id": "call-1", "name": "read_file", "input": {"path": "a.txt"}}]},
    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call-1", "content": "内容"}]}
  ],
  "max_tokens": 256,
  "tools": [{"name": "read_file", "description": "读取文件", "input_schema": {"type": "object"}}]
}"#;

    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.model, "claude-test");
    assert_eq!(request.context.max_tokens, Some(256));
    assert_eq!(request.context.system_prompt, "你是一个谨慎的助手。");
    assert_eq!(request.context.messages.len(), 3, "message count");
    assert!(matches!(request.context.messages[0], Message::User(_)));
    let assistant = request.context.messages[1].as_assistant().unwrap();
    let Some(Content::ToolCall(call)) = assistant.content.first() else {
        panic!("assistant content = {:?}", assistant.content);
    };
    assert_eq!(call.name, "read_file");
    assert!(matches!(
        request.context.messages[2],
        Message::ToolResult(_)
    ));
    assert_eq!(request.context.tools.len(), 1);
    assert_eq!(request.context.tools[0].name, "read_file");
    request.context.validate().expect("context validation");
}

/// Port of anthropic `TestDecodeRequestPreservesMidConversationSystem` —
/// Claude Code's mid-stream role:system injections (agent lists, task
/// reminders) stay in position as user messages instead of being silently
/// dropped.
#[test]
fn anthropic_decode_request_preserves_mid_conversation_system() {
    let data = r#"{
  "model": "claude-test",
  "messages": [
    {"role": "user", "content": "hello"},
    {"role": "system", "content": "Available agent types for the Agent tool: explore"},
    {"role": "assistant", "content": [{"type": "text", "text": "done"}]}
  ],
  "max_tokens": 256
}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(request.context.messages.len(), 3);
    let Some(mid) = request.context.messages[1].as_user() else {
        panic!(
            "message[1] = {:?}, want UserMessage",
            request.context.messages[1]
        );
    };
    let Some(Content::Text(text)) = mid.content.first() else {
        panic!("message[1] content = {:?}", mid.content);
    };
    assert_eq!(
        text.text,
        "Available agent types for the Agent tool: explore"
    );
}

/// Port of anthropic `TestDecodeRequestReplaysThinkingSignature` — a thinking
/// block's body reads from the thinking field (not text), the signature is
/// preserved, and `redacted_thinking`'s data passes through as a replayable
/// signature.
#[test]
fn anthropic_decode_request_replays_thinking_signature() {
    let data = r#"{
  "model": "claude-test",
  "messages": [
    {"role": "user", "content": "hi"},
    {"role": "assistant", "content": [
      {"type": "thinking", "thinking": "先想清楚再答", "signature": "sig-1"},
      {"type": "redacted_thinking", "data": "sealed-data-2"},
      {"type": "text", "text": "好的"}
    ]},
    {"role": "user", "content": "next"}
  ],
  "max_tokens": 256
}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    let assistant = request.context.messages[1].as_assistant().unwrap();
    let Some(Content::Thinking(first)) = assistant.content.first() else {
        panic!(
            "content[0] = {:?}, want thinking+signature",
            assistant.content[0]
        );
    };
    assert_eq!(first.thinking, "先想清楚再答");
    assert_eq!(first.thinking_signature, "sig-1");
    let Some(Content::Thinking(second)) = assistant.content.get(1) else {
        panic!(
            "content[1] = {:?}, want redacted thinking with data as signature",
            assistant.content[1]
        );
    };
    assert!(second.redacted);
    assert_eq!(second.thinking_signature, "sealed-data-2");
}

/// Port of anthropic `TestDecodeRequestAcceptsStringContent`.
#[test]
fn anthropic_decode_request_accepts_string_content() {
    let request = messages::decode_request(
        r#"{"model":"claude-test","messages":[{"role":"user","content":"hello"}],"max_tokens":256}"#.as_bytes(),
        true,
    )
    .expect("decode");
    assert_eq!(request.context.messages.len(), 1);
    let user = request.context.messages[0].as_user().unwrap();
    let Some(Content::Text(text)) = user.content.first() else {
        panic!("message content = {:?}", user.content);
    };
    assert_eq!(text.text, "hello");
}

/// Port of anthropic `TestDecodeRequestClientTypedTools` — client-executed
/// `tools` (`bash_*`/`text_editor_*`) pass through with a {"type":"object"}
/// placeholder schema; server-hosted types (`web_search_*`) drop with a
/// marker.
#[test]
fn anthropic_decode_request_client_typed_tools() {
    let data = r#"{
  "model": "claude-test",
  "messages": [{"role": "user", "content": "hi"}],
  "tools": [
    {"type": "bash_20250124", "name": "bash"},
    {"type": "text_editor_20250429", "name": "str_replace_editor"},
    {"type": "web_search_20250305", "name": "web_search"},
    {"name": "plain_custom", "input_schema": {"type": "object", "properties": {"x": {"type": "string"}}}}
  ]
}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    assert_eq!(
        request.context.tools.len(),
        3,
        "tools = {:?}",
        request.context.tools
    );
    assert_eq!(request.context.tools[0].name, "bash");
    assert_eq!(
        request.context.tools[0].input_schema,
        r#"{"type":"object"}"#
    );
    assert!(
        dropped_contains(&request.context.dropped, "tool:web_search_20250305"),
        "dropped = {:?}, want tool:web_search_20250305",
        request.context.dropped
    );
}

/// Port of anthropic `TestDecodeRequestDroppedFields` — invalid field values
/// and empty message bodies enter dropped accounting.
#[test]
fn anthropic_decode_request_dropped_fields() {
    let data = r#"{
  "model": "claude-test",
  "max_tokens": 0,
  "top_k": -1,
  "messages": [{"role": "user", "content": []}, {"role": "user", "content": "hi"}]
}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    for want in ["field:max_tokens", "field:top_k", "empty_message:user"] {
        assert!(
            dropped_contains(&request.context.dropped, want),
            "dropped = {:?}, want {want}",
            request.context.dropped
        );
    }
    // A content:[] user message lands as an empty-text placeholder; the
    // turn structure is not lost.
    assert_eq!(request.context.messages.len(), 2);
}

/// Port of anthropic `TestDecodeRequestTrailingData`.
#[test]
fn anthropic_decode_request_trailing_data() {
    let data = r#"{"model":"claude-test","messages":[{"role":"user","content":"hi"}]} extra"#;
    assert!(
        messages::decode_request(data.as_bytes(), true).is_err(),
        "trailing data should error"
    );
}

/// Port of anthropic `TestDecodeRequestMergesAdjacentAssistants` — same
/// IR-layer merge as the responses/chat faces.
#[test]
fn anthropic_decode_request_merges_adjacent_assistants() {
    let data = r#"{"model":"claude-test","max_tokens":256,"messages":[
	{"role":"assistant","content":[{"type":"text","text":"第一段"}]},
	{"role":"assistant","content":[{"type":"text","text":"第二段"},{"type":"tool_use","id":"call-1","name":"read","input":{}}]},
	{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"结果"}]},
	{"role":"assistant","content":[{"type":"text","text":"下一回合"}]}
]}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    // The tool_result inside the user message splits into a standalone
    // ToolResultMessage — separating the assistants on both sides, so the
    // result is merged / result / assistant.
    assert_eq!(request.context.messages.len(), 3, "message count");
    let merged = request.context.messages[0].as_assistant().unwrap();
    assert_eq!(
        merged.stop_reason,
        Some(StopReason::ToolUse),
        "want merged assistant with toolUse"
    );
    assert!(matches!(
        request.context.messages[1],
        Message::ToolResult(_)
    ));
    assert!(
        matches!(request.context.messages[2], Message::Assistant(_)),
        "want separate assistant turn"
    );
}

/// Port of anthropic `TestDecodeRequestPositionalToolResults` — `tool_result`
/// consumption is positional: a result whose `tool_use_id` matches nothing
/// but which still has an unconsumed call ahead stays TOOL; only orphans
/// ahead of all calls demote.
#[test]
fn anthropic_decode_request_positional_tool_results() {
    let data = r#"{"model":"claude-test","max_tokens":256,"messages":[
	{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-early","content":"孤儿"}]},
	{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"read","input":{}}]},
	{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-mismatch","content":"按位置消化"}]}
]}"#;
    let request = messages::decode_request(data.as_bytes(), true).expect("decode");
    assert!(
        matches!(request.context.messages[0], Message::User(_)),
        "orphan result not demoted: {:?}",
        request.context.messages[0]
    );
    let Some(result) = request.context.messages[2].as_tool_result() else {
        panic!(
            "positionally-consumable result was demoted: {:?}",
            request.context.messages[2]
        );
    };
    assert_eq!(result.tool_call_id, "call-mismatch");
}

// ---------------------------------------------------------------------------
// QA failure scenario: invalid_tool_choice_and_payload — invalid payloads
// are rejected at decode time, before any upstream call.
// ---------------------------------------------------------------------------

/// The plan's failure scenario: invalid `tool_choice` values and malformed
/// payloads fail decode with a client-fixable error on all three faces.
// One sequential failure-matrix scenario; splitting would scatter it.
#[allow(clippy::too_many_lines)]
#[test]
fn invalid_tool_choice_and_payload() {
    // Invalid tool_choice strings/objects are decode errors everywhere.
    assert!(
        chat::decode_request(
            r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"tool_choice":"bogus"}"#
                .as_bytes(),
            true,
        )
        .is_err(),
        "chat: bogus tool_choice string must error"
    );
    assert!(
        responses::decode_request(
            r#"{"model":"m","input":"hi","tool_choice":{"type":"function"}}"#.as_bytes(),
            true,
        )
        .is_err(),
        "responses: tool_choice object without a function name must error"
    );
    assert!(
        messages::decode_request(
            r#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":"hi"}],"tool_choice":{"type":"bogus"}}"#.as_bytes(),
            true,
        )
        .is_err(),
        "anthropic: bogus tool_choice type must error"
    );
    assert!(
        messages::decode_request(
            r#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":"hi"}],"tool_choice":{"type":"tool"}}"#.as_bytes(),
            true,
        )
        .is_err(),
        "anthropic: tool_choice type=tool without name must error"
    );

    // Malformed JSON bodies fail on every face.
    for (name, result) in [
        (
            "responses",
            responses::decode_request(b"{not json", true).map(|_| ()),
        ),
        ("chat", chat::decode_request(b"{not json", true).map(|_| ())),
        (
            "messages",
            messages::decode_request(b"{not json", true).map(|_| ()),
        ),
    ] {
        assert!(result.is_err(), "{name}: malformed JSON must error");
    }

    // Missing required fields fail on every face.
    assert!(
        responses::decode_request(r#"{"input":"hi"}"#.as_bytes(), true).is_err(),
        "responses: missing model must error"
    );
    assert!(
        responses::decode_request(r#"{"model":"m"}"#.as_bytes(), true).is_err(),
        "responses: missing input must error"
    );
    assert!(
        chat::decode_request(
            r#"{"messages":[{"role":"user","content":"hi"}]}"#.as_bytes(),
            true
        )
        .is_err(),
        "chat: missing model must error"
    );
    assert!(
        chat::decode_request(r#"{"model":"m","messages":[]}"#.as_bytes(), true).is_err(),
        "chat: empty messages must error"
    );
    assert!(
        messages::decode_request(
            r#"{"messages":[{"role":"user","content":"hi"}]}"#.as_bytes(),
            true
        )
        .is_err(),
        "anthropic: missing model must error"
    );
    assert!(
        messages::decode_request(r#"{"model":"m","messages":[]}"#.as_bytes(), true).is_err(),
        "anthropic: empty messages must error"
    );

    // A tool name outside the upstream-accepted charset is a validation
    // error classified invalid_argument — a readable 400, not an upstream
    // internal error.
    let err = chat::decode_request(
        r#"{"model":"m","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"a.b","parameters":{"type":"object"}}}]}"#.as_bytes(),
        true,
    )
    .expect_err("invalid tool name must error");
    assert_eq!(err.code, "invalid_argument");
    assert!(
        err.message.contains("validate adapted request"),
        "message = {}",
        err.message
    );

    // Unsupported image shapes (http URLs, file_id) fail at decode time.
    assert!(
        chat::decode_request(
            r#"{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.com/x.png"}}]}]}"#.as_bytes(),
            true,
        )
        .is_err(),
        "chat: http image URL must error"
    );
    assert!(
        messages::decode_request(
            r#"{"model":"m","max_tokens":1,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"!!!"}}]}]}"#.as_bytes(),
            true,
        )
        .is_err(),
        "anthropic: invalid base64 image must error"
    );
}

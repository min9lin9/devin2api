//! Upstream response-decoder contract tests for the devin2api Rust port
//! (plan task 9).
//!
//! Ports `G/internal/adapter/devin/replay_test.go` (golden-frame replay) and
//! the `ResponseDecoder*` cases of `devin_test.go` against
//! `devin2api::upstream::response::ResponseDecoder`. The `GoldenFrames*`
//! names map to `rust_case` entries in `tests/contracts.json` (`owner_task` 9);
//! the remaining tests cover the task's QA scenarios: late usage/signature
//! round-trip and `eof_without_stop_and_unknown_fields`.

use std::collections::HashSet;
use std::path::PathBuf;

use devin_proto::buffa::{DecodeOptions, UnknownField, UnknownFieldData};
use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::domain::{Content, ResponseEvent, ResponseEventType, StopReason, ToolDefinition};
use devin2api::upstream::response::{ResponseDecoder, custom_tool_names};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn frames_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/task9/frames")
}

/// Replays one golden-frame file through the decoder exactly like Go's
/// `replayFixture`: proto-`JSON` lines decode to `GetChatMessageResponse`,
/// `start` + per-frame `decode` + `finish(None)` events concatenate.
fn replay_fixture(
    name: &str,
    stop_patterns: &[String],
    custom_tools: HashSet<String>,
) -> Vec<ResponseEvent> {
    let text = std::fs::read_to_string(frames_dir().join(name)).expect("read fixture");
    let mut decoder = ResponseDecoder::new("swe-2-max", stop_patterns, custom_tools);
    let mut events = decoder.start();
    for (line_no, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let frame: pb::GetChatMessageResponse =
            serde_json::from_str(line).unwrap_or_else(|err| panic!("{name}:{line_no}: {err}"));
        events.extend(decoder.decode(&frame));
    }
    events.extend(decoder.finish(None));
    events
}

fn event_kinds(events: &[ResponseEvent]) -> Vec<ResponseEventType> {
    events.iter().map(|event| event.kind).collect()
}

fn assert_event_sequence(events: &[ResponseEvent], want: &[ResponseEventType]) {
    let got = event_kinds(events);
    assert_eq!(got, want, "event sequence mismatch");
}

fn final_message(events: &[ResponseEvent]) -> &devin2api::domain::AssistantMessage {
    let last = events.last().expect("non-empty event list");
    assert_eq!(
        last.kind,
        ResponseEventType::Done,
        "last event must be done"
    );
    last.message.as_ref().expect("done carries final message")
}

fn usage_frame(input: u64, output: u64, cache_read: u64) -> pb::GetChatMessageResponse {
    let usage = pb::ExaCodeiumCommonPb_ModelUsageStats {
        input_tokens: Some(input),
        output_tokens: Some(output),
        cache_read_tokens: Some(cache_read),
        ..Default::default()
    };
    pb::GetChatMessageResponse {
        usage: usage.into(),
        ..Default::default()
    }
}

fn stop_frame(reason: pb::ExaCodeiumCommonPb_StopReason) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        stop_reason: Some(reason),
        ..Default::default()
    }
}

fn tool_delta(
    id: Option<&str>,
    name: Option<&str>,
    arguments_json: Option<&str>,
) -> pb::ExaCodeiumCommonPb_ChatToolCall {
    pb::ExaCodeiumCommonPb_ChatToolCall {
        id: id.map(str::to_string),
        name: name.map(str::to_string),
        arguments_json: arguments_json.map(str::to_string),
        ..Default::default()
    }
}

fn text_frame(text: &str) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_text: Some(text.to_string()),
        ..Default::default()
    }
}

fn thinking_frame(text: &str) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_thinking: Some(text.to_string()),
        ..Default::default()
    }
}

fn signature_frame(signature: &str, signature_type: Option<&str>) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_signature: Some(signature.to_string()),
        delta_signature_type: signature_type.map(str::to_string),
        ..Default::default()
    }
}

fn tool_frame(deltas: Vec<pb::ExaCodeiumCommonPb_ChatToolCall>) -> pb::GetChatMessageResponse {
    pb::GetChatMessageResponse {
        delta_tool_calls: deltas,
        ..Default::default()
    }
}

const STOP_PATTERN: pb::ExaCodeiumCommonPb_StopReason =
    pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN;
const FUNCTION_CALL: pb::ExaCodeiumCommonPb_StopReason =
    pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_FUNCTION_CALL;
const PARTIAL: pb::ExaCodeiumCommonPb_StopReason =
    pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_PARTIAL;

// ---------------------------------------------------------------------------
// Golden-frame replay (contracts.json owner_task 9)
// ---------------------------------------------------------------------------

/// Port of `TestGoldenFramesToolCallTurn`: metadata-only usage frames produce
/// no events, tool id+name leads argument fragments, `FUNCTION_CALL` stop is
/// followed by trailing usage/dimension frames that still land in the final
/// aggregate (stop alone does not discard trailing usage).
#[test]
fn golden_frames_tool_call_turn() {
    let events = replay_fixture("tool-call.jsonl", &[], HashSet::new());
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::Start,
            ResponseEventType::ToolCallStart,
            ResponseEventType::ToolCallDelta,
            ResponseEventType::ToolCallDelta,
            ResponseEventType::ToolCallDelta,
            ResponseEventType::ToolCallEnd,
            ResponseEventType::Done,
        ],
    );
    let message = final_message(&events);
    assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
    assert_eq!(message.content.len(), 1);
    let Content::ToolCall(call) = &message.content[0] else {
        panic!("content[0] = {:?}, want ToolCall", message.content[0]);
    };
    assert_eq!(call.id, "Bash_75");
    assert_eq!(call.name, "Bash");
    let arguments: serde_json::Value =
        serde_json::from_str(&call.arguments).expect("arguments JSON");
    assert_eq!(arguments["command"], "ls -la");
    assert_eq!(message.usage.input, 1618);
    assert_eq!(message.usage.output, 88);
    assert_eq!(message.usage.cache_read, 182_211);
    assert_eq!(message.response_model, "swe-2-max");
    assert_eq!(message.upstream_request_id, "req-golden-t1");
}

/// Port of `TestGoldenFramesTextAnswerTurn`: pure `deltaText` sequence ending
/// with upstream `STOP_PATTERN` (natural `EOS`).
#[test]
fn golden_frames_text_answer_turn() {
    let events = replay_fixture("text-answer.jsonl", &[], HashSet::new());
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::Start,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextDelta,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ],
    );
    let message = final_message(&events);
    assert_eq!(message.stop_reason, Some(StopReason::Stop));
    let Content::Text(text) = &message.content[0] else {
        panic!("content[0] = {:?}, want Text", message.content[0]);
    };
    assert_eq!(text.text, "All three checks passed — nothing left to do.");
    assert_eq!(message.usage.input, 1073);
    assert_eq!(message.usage.cache_read, 60118);
}

/// Port of `TestGoldenFramesThinkingLateSignature`: the signature arrives as a
/// trailing frame after all body content and must merge back into the closed
/// thinking block instead of opening a new one.
#[test]
fn golden_frames_thinking_late_signature() {
    let events = replay_fixture("thinking-late-signature.jsonl", &[], HashSet::new());
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::Start,
            ResponseEventType::ThinkingStart,
            ResponseEventType::ThinkingDelta,
            ResponseEventType::ThinkingDelta,
            ResponseEventType::ThinkingEnd,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::ThinkingSignature,
            ResponseEventType::TextEnd,
            ResponseEventType::Done,
        ],
    );
    let message = final_message(&events);
    assert_eq!(message.content.len(), 2);
    let Content::Thinking(thinking) = &message.content[0] else {
        panic!("content[0] = {:?}, want Thinking", message.content[0]);
    };
    assert_eq!(
        thinking.thinking,
        "Comparing the two code paths, the merged form wins."
    );
    assert_eq!(
        thinking.thinking_signature, "sealed.v1.goldenfixturesignature",
        "late signature merged into the closed thinking block"
    );
    assert_eq!(thinking.signature_type, "sealed");
    let Content::Text(text) = &message.content[1] else {
        panic!("content[1] = {:?}, want Text", message.content[1]);
    };
    assert_eq!(text.text, "Verified: the merged history form fixes it.");
}

// ---------------------------------------------------------------------------
// Ordered-event reduction (devin_test.go ResponseDecoder* cases)
// ---------------------------------------------------------------------------

/// Port of `TestResponseDecoderMapsOneFrameToOrderedEvents`: one frame carrying
/// thinking+signature+text+tool reduces to the fixed semantic order.
#[test]
fn maps_one_frame_to_ordered_events() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    let events = decoder.start();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ResponseEventType::Start);

    let mut frame = thinking_frame("think");
    frame.delta_signature = Some("sig".to_string());
    frame.delta_text = Some("answer".to_string());
    frame.delta_tool_calls = vec![tool_delta(
        Some("call"),
        Some("exec"),
        Some(r#"{"command":"ls"}"#),
    )];
    let events = decoder.decode(&frame);

    assert_event_sequence(
        &events,
        &[
            ResponseEventType::ThinkingStart,
            ResponseEventType::ThinkingDelta,
            ResponseEventType::ThinkingEnd,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::ToolCallStart,
            ResponseEventType::ToolCallDelta,
        ],
    );
    let partial = events.last().and_then(|event| event.partial.as_ref());
    let partial = partial.expect("partial on last event");
    assert_eq!(partial.content.len(), 3);
    let Content::Thinking(thinking) = &partial.content[0] else {
        panic!("content[0] = {:?}, want Thinking", partial.content[0]);
    };
    assert_eq!(thinking.thinking, "think");
    assert_eq!(thinking.thinking_signature, "sig");
}

/// Port of `TestResponseDecoderMergesLateSignature`.
#[test]
fn merges_late_signature() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&thinking_frame("think"));
    decoder.decode(&text_frame("answer"));
    let events = decoder.decode(&signature_frame("sig", None));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ResponseEventType::ThinkingSignature);
    assert_eq!(events[0].content_index, 0);
    assert_eq!(events[0].delta, "sig");
    let partial = events[0].partial.as_ref().expect("partial");
    let Content::Thinking(thinking) = &partial.content[0] else {
        panic!("content[0] = {:?}, want Thinking", partial.content[0]);
    };
    assert_eq!(thinking.thinking, "think");
    assert_eq!(thinking.thinking_signature, "sig");
}

/// Port of `TestResponseDecoderSynthesizesThinkingForBareSignature`: `openai`
/// regime signatures with no thinking body synthesize a complete
/// start+end block; later bare signatures merge into it.
#[test]
fn synthesizes_thinking_for_bare_signature() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let events = decoder.decode(&signature_frame("sig", Some("openai")));
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::ThinkingStart,
            ResponseEventType::ThinkingEnd,
        ],
    );
    assert_eq!(events[0].content_index, 0);
    assert_eq!(events[1].content_index, 0);
    let partial = events[1].partial.as_ref().expect("partial");
    let Content::Thinking(thinking) = &partial.content[0] else {
        panic!("content[0] = {:?}, want Thinking", partial.content[0]);
    };
    assert_eq!(thinking.thinking_signature, "sig");
    assert_eq!(thinking.signature_type, "openai");

    let events = decoder.decode(&signature_frame("2", None));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ResponseEventType::ThinkingSignature);
    assert_eq!(events[0].delta, "2");
    let partial = events[0].partial.as_ref().expect("partial");
    let Content::Thinking(thinking) = &partial.content[0] else {
        panic!("content[0] = {:?}, want Thinking", partial.content[0]);
    };
    assert_eq!(thinking.thinking_signature, "sig2");
}

/// Port of `TestResponseDecoderAggregatesToolArgumentFragments`: deltas keep
/// raw fragments while the final call carries the complete arguments.
#[test]
fn aggregates_tool_argument_fragments() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let first = decoder.decode(&tool_frame(vec![tool_delta(
        Some("call"),
        Some("exec"),
        None,
    )]));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind, ResponseEventType::ToolCallStart);
    assert_eq!(first[0].tool_call_id, "call");
    assert_eq!(first[0].tool_name, "exec");

    let second = decoder.decode(&tool_frame(vec![tool_delta(
        None,
        None,
        Some("{\"command\":\""),
    )]));
    second[0]
        .validate()
        .expect("incomplete tool delta validates");
    let partial = second[0].partial.as_ref().expect("partial");
    let Content::ToolCall(call) = &partial.content[0] else {
        panic!("content[0] = {:?}, want ToolCall", partial.content[0]);
    };
    assert_eq!(
        call.arguments, "{}",
        "incomplete partial arguments stay {{}}"
    );

    let third = decoder.decode(&tool_frame(vec![tool_delta(None, None, Some(r#"ls"}"#))]));
    let stop_events = decoder.decode(&stop_frame(FUNCTION_CALL));
    assert_eq!(second[0].tool_call_id, "call");
    assert_eq!(third[0].tool_call_id, "call");
    assert_eq!(second[0].delta, "{\"command\":\"");
    assert_eq!(third[0].delta, r#"ls"}"#);
    assert!(stop_events.is_empty(), "no final event before EOF");

    let events = decoder.finish(None);
    let done = events.last().expect("done");
    assert_eq!(done.kind, ResponseEventType::Done);
    let message = done.message.as_ref().expect("done message");
    let Content::ToolCall(call) = &message.content[0] else {
        panic!("content[0] = {:?}, want ToolCall", message.content[0]);
    };
    assert_eq!(call.arguments, r#"{"command":"ls"}"#);
    assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
}

/// Port of `TestResponseDecoderBindsLateToolCallID`: a placeholder id stays on
/// events while the real id backfills the final content block.
#[test]
fn binds_late_tool_call_id() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let first = decoder.decode(&tool_frame(vec![tool_delta(
        None,
        Some("exec"),
        Some("{\"command\":\""),
    )]));
    assert!(!first.is_empty());
    assert_eq!(first[0].kind, ResponseEventType::ToolCallStart);
    assert_eq!(first[0].tool_call_id, "call_0");

    let second = decoder.decode(&tool_frame(vec![tool_delta(
        Some("real-id"),
        None,
        Some(r#"ls"}"#),
    )]));
    for event in &second {
        assert_eq!(
            event.tool_call_id, "call_0",
            "delta ids stay pinned to the placeholder"
        );
    }
    let Content::ToolCall(call) = &decoder.partial().content[0] else {
        panic!(
            "content[0] = {:?}, want ToolCall",
            decoder.partial().content[0]
        );
    };
    assert_eq!(call.id, "real-id", "real id backfills the content block");

    decoder.decode(&stop_frame(FUNCTION_CALL));
    let done = decoder.finish(None);
    let message = done.last().and_then(|event| event.message.as_ref());
    let Content::ToolCall(call) = &message.expect("done message").content[0] else {
        panic!("final content[0] is not a tool call");
    };
    assert_eq!(call.id, "real-id");
    assert_eq!(call.arguments, r#"{"command":"ls"}"#);
}

/// Port of `TestResponseDecoderSplitsSecondIdlessCall`: an id-less frame with a
/// different name opens a new call instead of merging into the last one.
#[test]
fn splits_second_idless_call() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&tool_frame(vec![tool_delta(Some("a"), Some("exec"), None)]));
    let second = decoder.decode(&tool_frame(vec![tool_delta(None, Some("read"), None)]));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].kind, ResponseEventType::ToolCallStart);
    assert_eq!(second[0].tool_call_id, "call_1");
    assert_eq!(decoder.tool_count(), 2, "two separate calls");
}

/// Port of `TestResponseDecoderUnwrapsCustomToolArguments`: wrapped custom-tool
/// fragments buffer silently and unwrap to one verbatim delta at end.
#[test]
fn unwraps_custom_tool_arguments() {
    let custom_tools = custom_tool_names(&[ToolDefinition {
        name: "apply_patch".to_string(),
        custom: true,
        ..ToolDefinition::default()
    }]);
    let mut decoder = ResponseDecoder::new("model", &[], custom_tools);
    decoder.start();
    let first = decoder.decode(&tool_frame(vec![tool_delta(
        Some("apply_patch_0"),
        Some("apply_patch"),
        None,
    )]));
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].kind, ResponseEventType::ToolCallStart);
    let partial = first[0].partial.as_ref().expect("partial");
    let Content::ToolCall(call) = &partial.content[0] else {
        panic!("content[0] = {:?}, want ToolCall", partial.content[0]);
    };
    assert!(call.custom, "wrapped call is custom at start");

    let second = decoder.decode(&tool_frame(vec![tool_delta(
        None,
        None,
        Some(r#"{"input": "*** Begin Patch"#),
    )]));
    assert!(second.is_empty(), "wrapped fragments buffer without deltas");
    decoder.decode(&tool_frame(vec![tool_delta(
        None,
        None,
        Some("\\n*** End Patch\"}"),
    )]));
    decoder.decode(&stop_frame(FUNCTION_CALL));
    let events = decoder.finish(None);

    let mut saw_full_delta = false;
    let mut call = None;
    for event in &events {
        if event.kind == ResponseEventType::ToolCallDelta
            && event.delta == "*** Begin Patch\n*** End Patch"
        {
            saw_full_delta = true;
        }
        if event.kind == ResponseEventType::ToolCallEnd {
            call = event.tool_call.clone();
        }
    }
    assert!(
        saw_full_delta,
        "unwrapped input delta emitted for wrapped call"
    );
    let call = call.expect("tool call end");
    assert!(call.custom);
    assert_eq!(call.arguments, "*** Begin Patch\n*** End Patch");
}

/// Port of `TestResponseDecoderConsumesUsageAfterStopReason`: the stop frame
/// emits nothing and trailing usage frames still land in the aggregate.
#[test]
fn consumes_usage_after_stop_reason() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&text_frame("complete"));
    let stop_events = decoder.decode(&stop_frame(STOP_PATTERN));
    assert!(stop_events.is_empty());
    assert!(
        !decoder.is_finished(),
        "stop alone does not finish the stream"
    );
    assert!(decoder.has_stop_reason());

    let usage_events = decoder.decode(&usage_frame(167, 61, 12195));
    assert!(usage_events.is_empty(), "usage frame is metadata-only");

    let events = decoder.finish(None);
    let done = events.last().expect("done");
    assert_eq!(done.kind, ResponseEventType::Done);
    assert_eq!(done.reason, Some(StopReason::Stop));
    let usage = &done.message.as_ref().expect("done message").usage;
    assert_eq!(usage.input, 167);
    assert_eq!(usage.output, 61);
    assert_eq!(usage.cache_read, 12195);
    assert_eq!(usage.cache_write, 0);
    assert_eq!(usage.total_tokens, 12423);
}

/// Port of `TestResponseDecoderCompletesPartialWithThinking`: `PARTIAL` stop
/// keeps generated thinking/text and maps to length.
#[test]
fn completes_partial_with_thinking() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&thinking_frame("think"));
    let mut frame = text_frame("hello");
    frame.stop_reason = Some(PARTIAL);
    decoder.decode(&frame);
    let events = decoder.finish(None);
    let done = events.last().expect("done");
    assert_eq!(done.kind, ResponseEventType::Done);
    assert_eq!(done.reason, Some(StopReason::Length));
    let message = done.message.as_ref().expect("done message");
    let Content::Thinking(thinking) = &message.content[0] else {
        panic!("content[0] = {:?}, want Thinking", message.content[0]);
    };
    assert_eq!(thinking.thinking, "think");
    let Content::Text(text) = &message.content[1] else {
        panic!("content[1] = {:?}, want Text", message.content[1]);
    };
    assert_eq!(text.text, "hello");
}

/// Port of `TestResponseDecoderLocalStopSequence`: local stop-pattern
/// truncation emits the prefix, ends the text block, and later frames only
/// update metadata.
#[test]
fn local_stop_sequence() {
    let mut decoder = ResponseDecoder::new("model", &["STOP".to_string()], HashSet::new());
    decoder.start();
    let events = decoder.decode(&text_frame("hello STOP world"));
    let mut text = String::new();
    for event in &events {
        if event.kind == ResponseEventType::TextDelta {
            text.push_str(&event.delta);
        }
    }
    assert_eq!(text, "hello ");
    let last = events.last().expect("text end");
    assert_eq!(last.kind, ResponseEventType::TextEnd);
    assert_eq!(last.content, "hello ");

    let later = decoder.decode(&text_frame(" more"));
    assert!(later.is_empty(), "post-truncation frames emit nothing");

    let done = decoder.finish(None);
    assert_eq!(done.len(), 1);
    assert_eq!(done[0].kind, ResponseEventType::Done);
    assert_eq!(done[0].reason, Some(StopReason::StopSequence));
    assert_eq!(
        done[0]
            .message
            .as_ref()
            .expect("done message")
            .stop_sequence,
        "STOP"
    );
}

/// Port of `TestResponseDecoderStopSequenceAcrossDeltas`: a pattern split
/// across frames holds the suspicious tail back until the match completes.
#[test]
fn stop_sequence_across_deltas() {
    let mut decoder = ResponseDecoder::new("model", &["XYZ".to_string()], HashSet::new());
    decoder.start();
    let events = decoder.decode(&text_frame("abc XY"));
    let mut emitted = String::new();
    for event in &events {
        if event.kind == ResponseEventType::TextDelta {
            emitted.push_str(&event.delta);
        }
    }
    assert_eq!(emitted, "abc ", "incomplete prefix tail is withheld");

    let events = decoder.decode(&text_frame("Z tail"));
    let mut emitted = String::new();
    let mut end_content = String::new();
    for event in &events {
        if event.kind == ResponseEventType::TextDelta {
            emitted.push_str(&event.delta);
        }
        if event.kind == ResponseEventType::TextEnd {
            end_content = event.content.clone();
        }
    }
    assert_eq!(emitted, "");
    assert_eq!(end_content, "abc ");
    let done = decoder.finish(None);
    assert_eq!(done[0].reason, Some(StopReason::StopSequence));
}

/// Port of `TestResponseDecoderStopSequenceRuneBoundary`: the holdback window
/// never splits a multi-byte rune.
#[test]
fn stop_sequence_rune_boundary() {
    let mut decoder = ResponseDecoder::new("model", &["STOP".to_string()], HashSet::new());
    decoder.start();
    let mut emitted = String::new();
    for event in decoder.decode(&text_frame("ab中文cd")) {
        if event.kind == ResponseEventType::TextDelta {
            emitted.push_str(&event.delta);
        }
    }
    assert_eq!(emitted, "ab中");
    assert!(!emitted.contains('\u{FFFD}'));

    for event in decoder.decode(&tool_frame(vec![tool_delta(
        Some("c"),
        Some("exec"),
        Some("{}"),
    )])) {
        if event.kind == ResponseEventType::TextDelta {
            emitted.push_str(&event.delta);
        }
    }
    assert_eq!(emitted, "ab中文cd");
}

/// Port of `TestResponseDecoderNoStopMatchFlushesTail`: the withheld tail
/// flushes as the last delta when the text block closes.
#[test]
fn no_stop_match_flushes_tail() {
    let mut decoder = ResponseDecoder::new("model", &["STOP".to_string()], HashSet::new());
    decoder.start();
    let events = decoder.decode(&text_frame("hi"));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ResponseEventType::TextStart);

    let events = decoder.decode(&tool_frame(vec![tool_delta(
        Some("c"),
        Some("exec"),
        Some("{}"),
    )]));
    let mut emitted = String::new();
    let mut end_content = String::new();
    for event in &events {
        if event.kind == ResponseEventType::TextDelta {
            emitted.push_str(&event.delta);
        }
        if event.kind == ResponseEventType::TextEnd {
            end_content = event.content.clone();
        }
    }
    assert_eq!(emitted, "hi");
    assert_eq!(end_content, "hi");

    decoder.decode(&stop_frame(FUNCTION_CALL));
    let done = decoder.finish(None);
    let last = done.last().expect("done");
    assert_eq!(last.kind, ResponseEventType::Done);
    assert_eq!(last.reason, Some(StopReason::ToolUse));
}

/// Port of `TestResponseDecoderStoresSignatureTypeAndOutputID`.
#[test]
fn stores_signature_type_and_output_id() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let mut frame = thinking_frame("think");
    frame.delta_signature = Some("sig-payload".to_string());
    frame.delta_signature_type = Some("anthropic".to_string());
    frame.output_id = Some("msg_123".to_string());
    let events = decoder.decode(&frame);
    assert!(!events.is_empty());
    let Content::Thinking(thinking) = &decoder.partial().content[0] else {
        panic!(
            "content[0] = {:?}, want Thinking",
            decoder.partial().content[0]
        );
    };
    assert_eq!(thinking.signature_type, "anthropic");
    assert_eq!(thinking.thinking_signature, "sig-payload");
    assert_eq!(decoder.partial().output_id, "msg_123");
}

/// Port of `TestResponseDecoderLateSignatureSynthesizesBlock`.
#[test]
fn late_signature_synthesizes_block() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let events = decoder.decode(&signature_frame(
        r#"[{"id":"rs_9","type":"reasoning","encrypted_content":"gAAA"}]"#,
        Some("openai"),
    ));
    let kinds = event_kinds(&events);
    assert!(kinds.contains(&ResponseEventType::ThinkingStart));
    assert!(kinds.contains(&ResponseEventType::ThinkingEnd));
    let Content::Thinking(thinking) = &decoder.partial().content[0] else {
        panic!(
            "content[0] = {:?}, want Thinking",
            decoder.partial().content[0]
        );
    };
    assert_eq!(thinking.signature_type, "openai");
    assert!(!thinking.thinking_signature.is_empty());
}

/// Port of `TestResponseDecoderCustomToolCall`: `is_custom_tool_call` +
/// `invalid_json_str` pass the verbatim non-`JSON` body through as a custom call.
#[test]
fn custom_tool_call_passthrough() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let mut delta = tool_delta(Some("call-1"), Some("apply_patch"), None);
    delta.is_custom_tool_call = Some(true);
    delta.invalid_json_str = Some("*** Begin Patch\n+hello".to_string());
    decoder.decode(&tool_frame(vec![delta]));
    decoder.decode(&stop_frame(FUNCTION_CALL));
    let events = decoder.finish(None);
    let done = events
        .iter()
        .find(|event| event.kind == ResponseEventType::Done)
        .expect("done event");
    let message = done.message.as_ref().expect("done message");
    let Content::ToolCall(call) = &message.content[0] else {
        panic!("content[0] = {:?}, want ToolCall", message.content[0]);
    };
    assert!(call.custom);
    assert_eq!(call.arguments, "*** Begin Patch\n+hello");
}

/// Port of `TestMapStopReason` plus the remaining mapped variants.
#[test]
fn maps_stop_reason() {
    use pb::ExaCodeiumCommonPb_StopReason as R;
    let cases = [
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MAX_TOKENS,
            StopReason::Length,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_INCOMPLETE,
            StopReason::Length,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_PARTIAL,
            StopReason::Length,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MAX_NEWLINES,
            StopReason::Length,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_FUNCTION_CALL,
            StopReason::ToolUse,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_ERROR,
            StopReason::Error,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_NONFINITE_LOGIT_OR_PROB,
            StopReason::Error,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_CONTENT_FILTER,
            StopReason::ContentFilter,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN,
            StopReason::Stop,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_MIN_LOG_PROB,
            StopReason::Stop,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_EXIT_SCOPE,
            StopReason::Stop,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_FIRST_NON_WHITESPACE_LINE,
            StopReason::Stop,
        ),
        (
            R::ExaCodeiumCommonPb_StopReason_STOP_REASON_NON_INSERTION,
            StopReason::Stop,
        ),
    ];
    for (input, want) in cases {
        assert_eq!(
            devin2api::upstream::response::map_stop_reason(input),
            want,
            "{input:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// QA failure case: eof_without_stop_and_unknown_fields
// ---------------------------------------------------------------------------

/// Malformed termination fails instead of fabricating success, and schema
/// drift stays observable on the decoder.
#[test]
fn eof_without_stop_and_unknown_fields() {
    // EOF after content but without a stop reason is a truncation, not a
    // clean end — the decoder must not fabricate a successful done.
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&text_frame("partial"));
    let events = decoder.finish(None);
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.kind, ResponseEventType::Error);
    let error = event.error.as_ref().expect("error message");
    assert_eq!(
        error.error_message,
        "devin stream ended without stop reason"
    );
    assert_eq!(error.stop_reason, Some(StopReason::Error));

    // A completely empty stream is not a successful empty turn either.
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let events = decoder.finish(None);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, ResponseEventType::Error);
    assert_eq!(
        events[0].error.as_ref().expect("error").error_message,
        "devin stream ended without generated content"
    );

    // Unknown fields on the frame, usage and tool-call scopes are recorded
    // as drift evidence (Go logs one warning per stream; the record keeps
    // every observation for the debug log).
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let mut frame = text_frame("hi");
    frame.__buffa_unknown_fields.push(UnknownField {
        number: 99,
        data: UnknownFieldData::Varint(1),
    });
    let mut usage = pb::ExaCodeiumCommonPb_ModelUsageStats::default();
    usage.__buffa_unknown_fields.push(UnknownField {
        number: 77,
        data: UnknownFieldData::Fixed32(7),
    });
    frame.usage = usage.into();
    let mut delta = tool_delta(Some("c"), Some("exec"), Some("{}"));
    delta.__buffa_unknown_fields.push(UnknownField {
        number: 42,
        data: UnknownFieldData::LengthDelimited(vec![0xAA]),
    });
    frame.delta_tool_calls = vec![delta];
    decoder.decode(&frame);

    let drift = decoder.drift();
    assert_eq!(drift.len(), 3, "frame+usage+tool_call scopes recorded");
    assert_eq!(drift[0].scope, "frame");
    assert_eq!(drift[0].field_numbers, vec![99]);
    assert_eq!(drift[1].scope, "usage");
    assert_eq!(drift[1].field_numbers, vec![77]);
    assert_eq!(drift[2].scope, "tool_call");
    assert_eq!(drift[2].field_numbers, vec![42]);

    // An undeclared stop_reason enum value on the wire (proto3-open enum
    // semantics: Go keeps it in the field, buffa routes it to unknown
    // fields) still counts as a declared stop and maps to stop — the same
    // outcome the Go decoder's stopReasonWarned branch produces.
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    decoder.decode(&text_frame("body"));
    let wire = [0x28u8, 0xE7, 0x07]; // field 5 varint 999 — undeclared stop_reason
    let undeclared: pb::GetChatMessageResponse = DecodeOptions::new()
        .decode_from_slice(&wire)
        .expect("decode undeclared stop_reason frame");
    decoder.decode(&undeclared);
    assert!(
        decoder.has_stop_reason(),
        "undeclared stop_reason still registers a stop"
    );
    let events = decoder.finish(None);
    let done = events.last().expect("done");
    assert_eq!(done.kind, ResponseEventType::Done);
    assert_eq!(done.reason, Some(StopReason::Stop));
}

// ---------------------------------------------------------------------------
// QA happy case: late usage/signature + multi-kind frames round-trip
// ---------------------------------------------------------------------------

/// One frame carrying thinking+signature+text+tool deltas emits the fixed
/// semantic order, a bare signature after the text block merges into the
/// closed thinking block, and a trailing usage frame after the stop reason
/// still lands in the final aggregate.
#[test]
fn late_usage_and_signature_round_trip() {
    let mut decoder = ResponseDecoder::new("swe-2-max", &[], HashSet::new());
    let mut events = decoder.start();

    let mut frame = thinking_frame("reasoning ");
    frame.delta_signature = Some("sig-a".to_string());
    frame.delta_signature_type = Some("sealed".to_string());
    frame.delta_text = Some("answer".to_string());
    frame.delta_tool_calls = vec![tool_delta(
        Some("call-1"),
        Some("exec"),
        Some(r#"{"command":"ls"}"#),
    )];
    events.extend(decoder.decode(&frame));
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::Start,
            ResponseEventType::ThinkingStart,
            ResponseEventType::ThinkingDelta,
            ResponseEventType::ThinkingEnd,
            ResponseEventType::TextStart,
            ResponseEventType::TextDelta,
            ResponseEventType::TextEnd,
            ResponseEventType::ToolCallStart,
            ResponseEventType::ToolCallDelta,
        ],
    );

    // Bare signature after the thinking block closed merges back.
    events.extend(decoder.decode(&signature_frame("-b", None)));
    // Stop reason, then trailing usage — both metadata-only frames.
    let mut stop = stop_frame(FUNCTION_CALL);
    stop.usage = pb::ExaCodeiumCommonPb_ModelUsageStats {
        input_tokens: Some(10),
        ..Default::default()
    }
    .into();
    assert!(decoder.decode(&stop).is_empty());
    assert!(decoder.decode(&usage_frame(0, 42, 0)).is_empty());
    events.extend(decoder.finish(None));

    assert_event_sequence(
        &events[9..],
        &[
            ResponseEventType::ThinkingSignature,
            ResponseEventType::ToolCallEnd,
            ResponseEventType::Done,
        ],
    );
    let message = final_message(&events);
    let Content::Thinking(thinking) = &message.content[0] else {
        panic!("content[0] = {:?}, want Thinking", message.content[0]);
    };
    assert_eq!(thinking.thinking, "reasoning ");
    assert_eq!(thinking.thinking_signature, "sig-a-b");
    assert_eq!(thinking.signature_type, "sealed");
    assert_eq!(message.usage.input, 10);
    assert_eq!(message.usage.output, 42);
    assert_eq!(message.usage.total_tokens, 52);
    assert_eq!(message.stop_reason, Some(StopReason::ToolUse));
}

/// `proto2` presence distinctions survive decode: an explicitly empty
/// `argumentsJson` still emits a delta (present != absent), an explicitly
/// false `thinkingRedacted` does not open a thinking block, and a zero
/// usage value does not overwrite an already-recorded non-zero value.
#[test]
fn presence_metadata_distinctions() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    // Present-but-empty argumentsJson is a real fragment.
    let events = decoder.decode(&tool_frame(vec![tool_delta(
        Some("c"),
        Some("exec"),
        Some(""),
    )]));
    assert_event_sequence(
        &events,
        &[
            ResponseEventType::ToolCallStart,
            ResponseEventType::ToolCallDelta,
        ],
    );
    assert_eq!(events[1].delta, "");

    // Explicit false thinkingRedacted does not open a thinking block.
    let frame = pb::GetChatMessageResponse {
        thinking_redacted: Some(false),
        ..Default::default()
    };
    assert!(decoder.decode(&frame).is_empty());
    // Explicit true does.
    let frame = pb::GetChatMessageResponse {
        thinking_redacted: Some(true),
        ..Default::default()
    };
    let events = decoder.decode(&frame);
    assert_eq!(events[0].kind, ResponseEventType::ThinkingStart);
    let Content::Thinking(thinking) = &decoder.partial().content[1] else {
        panic!(
            "content[1] = {:?}, want Thinking",
            decoder.partial().content[1]
        );
    };
    assert!(thinking.redacted);

    // Non-zero usage lands; an explicit zero on a later frame does not
    // overwrite it.
    decoder.decode(&usage_frame(100, 0, 0));
    decoder.decode(&usage_frame(0, 0, 0));
    assert_eq!(decoder.partial().usage.input, 100);
}

/// The `upstream_provider` diagnostic is recorded once from the first usage
/// frame carrying provider identity, with the Go field set.
#[test]
fn provider_diagnostic_recorded_once() {
    let mut decoder = ResponseDecoder::new("model", &[], HashSet::new());
    decoder.start();
    let mut usage = pb::ExaCodeiumCommonPb_ModelUsageStats {
        api_provider: Some(pb::ExaCodeiumCommonPb_APIProvider::ExaCodeiumCommonPb_APIProvider_API_PROVIDER_FIREWORKS_DEVIN),
        message_id: Some("provider-msg-1".to_string()),
        billing_model_uid: Some("billing-uid".to_string()),
        ..Default::default()
    };
    usage
        .response_header
        .insert("x-request-id".to_string(), "chatcmpl-1".to_string());
    let frame = pb::GetChatMessageResponse {
        usage: usage.into(),
        ..Default::default()
    };
    decoder.decode(&frame);
    decoder.decode(&frame);

    let diagnostics = &decoder.partial().diagnostics;
    assert_eq!(diagnostics.len(), 1, "provider diagnostic logged once");
    assert_eq!(diagnostics[0].kind, "upstream_provider");
    let details: serde_json::Value =
        serde_json::from_str(&diagnostics[0].details).expect("details JSON");
    assert_eq!(
        details["api_provider"],
        "ExaCodeiumCommonPb_APIProvider_API_PROVIDER_FIREWORKS_DEVIN"
    );
    assert_eq!(details["provider_request_id"], "chatcmpl-1");
    assert_eq!(details["provider_message_id"], "provider-msg-1");
    assert_eq!(details["billing_model_uid"], "billing-uid");
}

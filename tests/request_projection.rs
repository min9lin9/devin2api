//! Upstream request-projection contract tests (plan task 8).
//!
//! Ports `G/internal/adapter/devin/{request_encoder,sanitize,tool_definition}.go`
//! behavior against `devin2api::upstream::{request,sanitize,tool_definition}`.
//!
//! `tests/fixtures/task8/corpus.json` is the shared input corpus;
//! `expected.json` was produced by the REAL Go functions (`buildRequest`,
//! `sanitizeRequest`, `sanitizeUpstreamText`, `withToolDescriptions`,
//! `convertToolDefinition`, `deriveSessionIDs`, `nextStepIndex`) via a
//! `go test -overlay` harness injected into `internal/adapter/devin` —
//! see `.omo/evidence/devin2api-rust-parity/task-8/oracle/`. Random fields
//! (messageId/executionId/metadata.f) and the process-local stepIndex are
//! canonicalized on both sides; everything else compares exactly.

use std::collections::BTreeMap;
use std::path::PathBuf;

use buffa::{DecodeOptions, Message as _};
use devin_proto::generated::exa::api_server_pb as pb;
use devin2api::domain::request::{
    Content, ImageContent, Message, RequestMessages, TextContent, ThinkingContent, ToolCall,
    ToolChoice, ToolChoiceMode, ToolDefinition, ToolResultMessage, UserMessage,
};
use devin2api::domain::response::AssistantMessage;
use devin2api::upstream::request::{
    CallBinding, ClientIdentity, build_request, derive_session_ids,
};
use devin2api::upstream::sanitize::{sanitize_request, sanitize_upstream_text};
use devin2api::upstream::tool_definition::{convert_tool_definition, with_tool_descriptions};
use serde::Deserialize;
use serde_json::Value;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/task8")
}

fn corpus() -> Value {
    serde_json::from_slice(&std::fs::read(fixtures_dir().join("corpus.json")).expect("corpus"))
        .expect("parse corpus")
}

fn expected() -> Value {
    serde_json::from_slice(&std::fs::read(fixtures_dir().join("expected.json")).expect("expected"))
        .expect("parse expected")
}

// --- corpus DSL ---

#[derive(Debug, Deserialize)]
struct CorpusRequest {
    name: String,
    #[serde(default)]
    sanitize: bool,
    #[serde(default)]
    demote_orphans: bool,
    #[serde(default)]
    binding_jwt: String,
    #[serde(default)]
    system_prompt: String,
    #[serde(default)]
    session_key: String,
    #[serde(default)]
    model: String,
    max_tokens: Option<i64>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i64>,
    seed: Option<i64>,
    #[serde(default)]
    stop_sequences: Vec<String>,
    tool_choice: Option<CorpusChoice>,
    #[serde(default)]
    disable_parallel_tool_calls: bool,
    #[serde(default)]
    messages: Vec<CorpusMessage>,
    #[serde(default)]
    tools: Vec<CorpusTool>,
}

#[derive(Debug, Deserialize)]
struct CorpusChoice {
    mode: String,
    #[serde(default)]
    tool_name: String,
}

#[derive(Debug, Deserialize)]
struct CorpusMessage {
    role: String,
    #[serde(default)]
    tool_call_id: String,
    #[serde(default)]
    is_error: bool,
    #[serde(default)]
    output_id: String,
    #[serde(default)]
    content: Vec<CorpusContent>,
}

#[derive(Debug, Deserialize)]
struct CorpusContent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    thinking: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    signature_type: String,
    #[serde(default)]
    redacted: bool,
    #[serde(default)]
    data: String,
    #[serde(default)]
    mime_type: String,
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    arguments: String,
    #[serde(default)]
    custom: bool,
}

#[derive(Debug, Deserialize)]
struct CorpusTool {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    input_schema: String,
    #[serde(default)]
    custom: bool,
}

fn to_request(c: &CorpusRequest) -> RequestMessages {
    let mut request = RequestMessages {
        system_prompt: c.system_prompt.clone(),
        session_key: c.session_key.clone(),
        model: c.model.clone(),
        max_tokens: c.max_tokens,
        temperature: c.temperature,
        top_p: c.top_p,
        top_k: c.top_k,
        seed: c.seed,
        stop_sequences: c.stop_sequences.clone(),
        disable_parallel_tool_calls: c.disable_parallel_tool_calls,
        ..Default::default()
    };
    if let Some(choice) = &c.tool_choice {
        let mode = match choice.mode.as_str() {
            "auto" => ToolChoiceMode::Auto,
            "none" => ToolChoiceMode::None,
            "required" => ToolChoiceMode::Required,
            "named" => ToolChoiceMode::Named,
            other => panic!("unknown tool_choice mode {other}"),
        };
        request.tool_choice = Some(ToolChoice {
            mode,
            tool_name: choice.tool_name.clone(),
        });
    }
    for m in &c.messages {
        let content: Vec<Content> = m
            .content
            .iter()
            .map(|b| match b.kind.as_str() {
                "text" => Content::Text(TextContent {
                    text: b.text.clone(),
                }),
                "thinking" => Content::Thinking(ThinkingContent {
                    thinking: b.thinking.clone(),
                    thinking_signature: b.signature.clone(),
                    signature_type: b.signature_type.clone(),
                    redacted: b.redacted,
                }),
                "image" => Content::Image(ImageContent {
                    data: b.data.clone(),
                    mime_type: b.mime_type.clone(),
                }),
                "tool_call" => Content::ToolCall(ToolCall {
                    id: b.id.clone(),
                    name: b.name.clone(),
                    arguments: b.arguments.clone(),
                    custom: b.custom,
                }),
                other => panic!("unknown content type {other}"),
            })
            .collect();
        match m.role.as_str() {
            "user" => request.messages.push(Message::User(UserMessage {
                content,
                timestamp_ms: 0,
            })),
            "assistant" => request.messages.push(Message::Assistant(AssistantMessage {
                content,
                output_id: m.output_id.clone(),
                ..Default::default()
            })),
            "tool_result" => request
                .messages
                .push(Message::ToolResult(ToolResultMessage {
                    tool_call_id: m.tool_call_id.clone(),
                    is_error: m.is_error,
                    content,
                    timestamp_ms: 0,
                })),
            other => panic!("unknown role {other}"),
        }
    }
    for t in &c.tools {
        request.tools.push(ToolDefinition {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
            custom: t.custom,
        });
    }
    request
}

// --- canonicalization (mirrors the Go oracle harness) ---

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, child) in map {
                let canon = match key.as_str() {
                    "messageId" | "executionId" | "f" => Value::from("<rand>"),
                    "stepIndex" => Value::from("<step>"),
                    "numCompletions" | "maxTokens" | "maxNewlines" | "topK" | "seed"
                    | "requestId" => match child {
                        Value::String(s) => s
                            .parse::<f64>()
                            .map_or_else(|_| canonicalize(child), Value::from),
                        _ => canonicalize(child),
                    },
                    _ => canonicalize(child),
                };
                out.insert(key.clone(), canon);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        _ => value.clone(),
    }
}

/// Semantic `JSON` equality: objects/arrays recursively, all numbers as f64
/// (Go `protojson` prints `1` where serde emits `1.0` for float fields).
fn json_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| json_eq(v, w)))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(v, w)| json_eq(v, w))
        }
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

fn binding(jwt: &str) -> CallBinding {
    CallBinding {
        token: "token".to_string(),
        model: "model".to_string(),
        model_assignment_jwt: jwt.to_string(),
    }
}

/// Encodes, re-decodes and serializes a built request — the canonical
/// comparison runs on the wire round-trip, not just the in-memory message.
fn wire_roundtrip_json(request: &pb::GetChatMessageRequest) -> Value {
    let mut buf = Vec::new();
    request.encode(&mut buf);
    let decoded = DecodeOptions::new()
        .decode_from_slice::<pb::GetChatMessageRequest>(&buf)
        .expect("re-decode built request");
    serde_json::to_value(&decoded).expect("serialize decoded request")
}

/// Every corpus request: sanitize/demote flags applied like the Go oracle,
/// then the canonical decoded protobuf and the repairs counters must match
/// the Go-produced expectation exactly.
#[test]
fn corpus_requests_match_go_oracle() {
    let corpus = corpus();
    let expected = expected();
    let cases: Vec<CorpusRequest> =
        serde_json::from_value(corpus["requests"].clone()).expect("requests dsl");
    let expected_requests = expected["requests"].as_object().expect("expected requests");
    let mut ran = 0usize;
    for case in &cases {
        let mut request = to_request(case);
        let mut sanitize_hits = BTreeMap::new();
        if case.sanitize {
            sanitize_hits = sanitize_request(&mut request);
        }
        if case.demote_orphans {
            request.demote_orphan_tool_results();
        }
        let want = &expected_requests[&case.name];
        match build_request(
            &request,
            &ClientIdentity::default(),
            &binding(&case.binding_jwt),
        ) {
            Err(err) => {
                let want_error = want["error"].as_str().unwrap_or_else(|| {
                    panic!(
                        "{}: build_request failed: {err}; oracle expected success",
                        case.name
                    )
                });
                assert_eq!(
                    err.to_string(),
                    want_error,
                    "{}: error text differs from Go",
                    case.name
                );
            }
            Ok((built, mut repairs)) => {
                assert!(
                    want.get("error").is_none(),
                    "{}: oracle expected error {:?}, got success",
                    case.name,
                    want["error"]
                );
                repairs.sanitize_hits = sanitize_hits;
                let got = canonicalize(&wire_roundtrip_json(&built));
                let want_request = &want["request"];
                assert!(
                    json_eq(&got, want_request),
                    "{}: canonical request differs from Go oracle\ngot:  {}\nwant: {}",
                    case.name,
                    serde_json::to_string_pretty(&got).unwrap(),
                    serde_json::to_string_pretty(want_request).unwrap()
                );
                let got_repairs = serde_json::to_value(&repairs).expect("repairs json");
                assert!(
                    json_eq(&got_repairs, &want["repairs"]),
                    "{}: repairs differ\ngot:  {got_repairs}\nwant: {}",
                    case.name,
                    want["repairs"]
                );
            }
        }
        ran += 1;
    }
    assert!(ran >= 25, "expected the full corpus, ran {ran}");
}

/// Every sanitize corpus text: rewritten output and per-rule hit counts
/// must equal the Go oracle.
#[test]
fn sanitize_texts_match_go_oracle() {
    let corpus = corpus();
    let expected = expected();
    let cases = corpus["sanitize_texts"]
        .as_array()
        .expect("sanitize corpus");
    let want_map = expected["sanitize_texts"]
        .as_object()
        .expect("expected sanitize");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let prompt_only = case["prompt_only"].as_bool().unwrap();
        let text = case["text"].as_str().unwrap();
        let mut hits = BTreeMap::new();
        let got = sanitize_upstream_text(text, prompt_only, &mut hits);
        let want = &want_map[name];
        assert_eq!(
            got.as_ref(),
            want["text"].as_str().unwrap(),
            "{name}: sanitized text differs"
        );
        let got_hits = serde_json::to_value(&hits).unwrap();
        assert!(
            json_eq(&got_hits, &want["hits"]),
            "{name}: hits differ\ngot:  {got_hits}\nwant: {}",
            want["hits"]
        );
    }
}

/// Tool-description injection into the system prompt matches Go,
/// including XML escaping and description reformatting.
#[test]
fn tool_descriptions_match_go_oracle() {
    let corpus = corpus();
    let expected = expected();
    let cases = corpus["tool_descriptions"].as_array().expect("desc corpus");
    let want_map = expected["tool_descriptions"]
        .as_object()
        .expect("expected descs");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let tools: Vec<CorpusTool> =
            serde_json::from_value(case["tools"].clone()).expect("tools dsl");
        let tools: Vec<ToolDefinition> = tools
            .iter()
            .map(|t| ToolDefinition {
                name: t.name.clone(),
                description: t.description.clone(),
                input_schema: t.input_schema.clone(),
                custom: t.custom,
            })
            .collect();
        let got = with_tool_descriptions(case["system_prompt"].as_str().unwrap(), &tools);
        assert_eq!(
            got,
            want_map[name].as_str().unwrap(),
            "{name}: tool description section differs"
        );
    }
}

/// Schema strip+normalize through `convert_tool_definition`: the emitted
/// `json_schema_string` (Go-marshal-exact: sorted keys, HTML escapes, ES6
/// floats) or the wrapped error text must equal the Go oracle.
#[test]
fn tool_definitions_match_go_oracle() {
    let corpus = corpus();
    let expected = expected();
    let cases = corpus["schemas"].as_array().expect("schema corpus");
    let want_map = expected["tool_definitions"]
        .as_object()
        .expect("expected tool_definitions");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let tool = ToolDefinition {
            name: format!("tool_{name}"),
            description: String::new(),
            input_schema: case["schema"].as_str().unwrap().to_string(),
            custom: false,
        };
        let Some(want) = want_map.get(name) else {
            // Oracle skips cases whose Go error text isn't ported
            // (encoding/json parse diagnostics); error presence is the contract.
            assert!(
                convert_tool_definition(&tool).is_err(),
                "{name}: expected convert to fail"
            );
            continue;
        };
        match convert_tool_definition(&tool) {
            Err(err) => {
                let want_error = want["error"].as_str().unwrap_or_else(|| {
                    panic!("{name}: convert failed: {err}; oracle expected success")
                });
                assert_eq!(err.message, want_error, "{name}: error text differs");
            }
            Ok(converted) => {
                assert!(
                    want.get("error").is_none(),
                    "{name}: oracle expected error {:?}",
                    want["error"]
                );
                assert_eq!(
                    converted.json_schema_string.as_deref(),
                    want["schema"].as_str(),
                    "{name}: schema differs"
                );
                assert_eq!(converted.name.as_deref(), want["name"].as_str());
                assert_eq!(
                    converted.description.as_deref(),
                    want["description"].as_str()
                );
            }
        }
    }
}

/// Session-ID derivation: session-key seeding and the byte-truncated
/// content-hash fallback (4KB system head + 1KB first text, mid-rune cuts
/// included) must produce the Go-derived UUIDs.
// One sequential oracle sweep; splitting would scatter the cases.
#[allow(clippy::too_many_lines)]
#[test]
fn session_ids_match_go_oracle() {
    let expected = expected();
    let want_map = expected["session_ids"].as_object().expect("session_ids");

    let user = |text: &str| {
        Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: text.to_string(),
            })],
            timestamp_ms: 0,
        })
    };
    let mut long_system = "x".repeat(4090);
    long_system.push_str("世界");
    long_system.push('y');
    let mut long_text = "a".repeat(1020);
    long_text.push_str("界世");
    long_text.push('z');

    let cases: Vec<(&str, RequestMessages)> = vec![
        (
            "session_key",
            RequestMessages {
                session_key: "user-1".to_string(),
                system_prompt: "system".to_string(),
                messages: vec![user("task")],
                ..Default::default()
            },
        ),
        (
            "keyless_simple",
            RequestMessages {
                system_prompt: "system".to_string(),
                messages: vec![user("task A")],
                ..Default::default()
            },
        ),
        (
            "keyless_other",
            RequestMessages {
                system_prompt: "system".to_string(),
                messages: vec![user("task B")],
                ..Default::default()
            },
        ),
        (
            "keyless_long_system",
            RequestMessages {
                system_prompt: long_system,
                messages: vec![user("hi")],
                ..Default::default()
            },
        ),
        (
            "keyless_long_text",
            RequestMessages {
                system_prompt: "sys".to_string(),
                messages: vec![user(&long_text)],
                ..Default::default()
            },
        ),
        (
            "keyless_no_text",
            RequestMessages {
                system_prompt: "sys".to_string(),
                messages: vec![Message::User(UserMessage {
                    content: vec![Content::Image(ImageContent {
                        data: "AA".to_string(),
                        mime_type: "image/png".to_string(),
                    })],
                    timestamp_ms: 0,
                })],
                ..Default::default()
            },
        ),
        ("keyless_empty", RequestMessages::default()),
        (
            "keyless_first_text_skips_empty",
            RequestMessages {
                system_prompt: "sys".to_string(),
                messages: vec![
                    user(""),
                    Message::Assistant(AssistantMessage {
                        content: vec![Content::Text(TextContent {
                            text: "first real".to_string(),
                        })],
                        ..Default::default()
                    }),
                ],
                ..Default::default()
            },
        ),
    ];
    for (name, request) in cases {
        let (trajectory, cascade) = derive_session_ids(&request);
        let want = want_map[name].as_array().expect("sid pair");
        assert_eq!(
            trajectory,
            want[0].as_str().unwrap(),
            "{name}: trajectory id differs"
        );
        assert_eq!(
            cascade,
            want[1].as_str().unwrap(),
            "{name}: cascade id differs"
        );
    }
}

/// `step_index` is process-local and monotonically increasing per
/// `trajectory_id`; a fresh session key starts at 1.
#[test]
fn step_index_is_monotonic_per_trajectory() {
    let request = RequestMessages {
        session_key: "task8-step-index-unique".to_string(),
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: "hi".to_string(),
            })],
            timestamp_ms: 0,
        })],
        ..Default::default()
    };
    let mut steps = Vec::new();
    for _ in 0..3 {
        let (built, _) =
            build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
        steps.push(
            built
                .trajectory_reference
                .as_option()
                .and_then(|t| t.step_index)
                .expect("step_index set"),
        );
    }
    assert_eq!(steps, vec![1, 2, 3], "step_index must increment per send");
}

/// Session IDs stay stable across turns of one conversation and differ
/// across sessions — Go `TestDeriveSessionIDsStableForSamePrefix` /
// DifferAcrossConversations.
#[test]
fn session_ids_stable_across_turns() {
    let base = RequestMessages {
        system_prompt: "system".to_string(),
        session_key: "user-1".to_string(),
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: "task".to_string(),
            })],
            timestamp_ms: 0,
        })],
        ..Default::default()
    };
    let (first, _) = build_request(&base, &ClientIdentity::default(), &binding("")).expect("first");
    let mut second_req = base.clone();
    second_req
        .messages
        .push(Message::Assistant(AssistantMessage {
            content: vec![Content::Text(TextContent {
                text: "answer".to_string(),
            })],
            ..Default::default()
        }));
    second_req.messages.push(Message::User(UserMessage {
        content: vec![Content::Text(TextContent {
            text: "follow up".to_string(),
        })],
        timestamp_ms: 0,
    }));
    let (second, _) =
        build_request(&second_req, &ClientIdentity::default(), &binding("")).expect("second");
    let first_traj = first
        .trajectory_reference
        .as_option()
        .and_then(|t| t.trajectory_id.clone())
        .unwrap();
    let second_traj = second
        .trajectory_reference
        .as_option()
        .and_then(|t| t.trajectory_id.clone())
        .unwrap();
    assert_eq!(first_traj, second_traj, "trajectory must be stable");
    assert_eq!(
        first.cascade_id, second.cascade_id,
        "cascade must be stable"
    );
    assert_ne!(
        first.execution_id, second.execution_id,
        "execution id must stay unique per request"
    );
}

/// Exact opaque bytes: custom tool-call argument bodies and thinking
/// signatures must appear verbatim inside the encoded wire bytes (not
/// re-encoded, not `JSON`-escaped).
#[test]
fn opaque_bytes_pass_through_verbatim() {
    let patch = "*** Begin Patch\n*** Update File: a.py\n@@\n+hello\n*** End Patch";
    let signature = "sealed.v1.abc==opaque\u{0001}bytes";
    let request = RequestMessages {
        messages: vec![
            Message::User(UserMessage {
                content: vec![Content::Text(TextContent {
                    text: "hi".to_string(),
                })],
                timestamp_ms: 0,
            }),
            Message::Assistant(AssistantMessage {
                content: vec![
                    Content::Thinking(ThinkingContent {
                        thinking: "t".to_string(),
                        thinking_signature: signature.to_string(),
                        signature_type: "sealed".to_string(),
                        redacted: false,
                    }),
                    Content::ToolCall(ToolCall {
                        id: "c1".to_string(),
                        name: "apply_patch".to_string(),
                        arguments: patch.to_string(),
                        custom: true,
                    }),
                ],
                ..Default::default()
            }),
        ],
        ..Default::default()
    };
    let (built, _) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    let mut wire = Vec::new();
    built.encode(&mut wire);
    // The raw patch text and signature must appear byte-for-byte inside
    // the wire encoding.
    assert!(
        wire.windows(patch.len()).any(|w| w == patch.as_bytes()),
        "custom tool-call body must pass through verbatim"
    );
    assert!(
        wire.windows(signature.len())
            .any(|w| w == signature.as_bytes()),
        "signature must pass through verbatim"
    );
    // And the decoded message carries them in the right fields.
    let decoded = DecodeOptions::new()
        .decode_from_slice::<pb::GetChatMessageRequest>(&wire)
        .expect("decode");
    let prompt = &decoded.chat_message_prompts[1];
    let call = &prompt.tool_calls[0];
    assert_eq!(call.is_custom_tool_call, Some(true));
    assert_eq!(call.invalid_json_str.as_deref(), Some(patch));
    assert!(
        call.arguments_json.is_none(),
        "custom call must not set arguments_json"
    );
    assert_eq!(prompt.signature.as_deref(), Some(signature));
    assert_eq!(prompt.signature_type.as_deref(), Some("sealed"));
}

/// QA failure case: orphan tool results are repaired (demoted to user
/// text at the IR layer, or kept unpaired at the wire layer), named
/// `tool_choice` outside the `tools` list is rejected with a readable error,
/// invalid tool names are rejected at validation, and repair counters
/// reflect every action.
// One sequential repair-matrix scenario; splitting would scatter it.
#[allow(clippy::too_many_lines)]
#[test]
fn orphan_tools_and_invalid_names() {
    // Orphan result demoted at the IR layer keeps its content as user text.
    let mut request = RequestMessages {
        messages: vec![
            Message::User(UserMessage {
                content: vec![Content::Text(TextContent {
                    text: "hi".to_string(),
                })],
                timestamp_ms: 0,
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-lost".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "orphan".to_string(),
                })],
                timestamp_ms: 0,
            }),
            Message::Assistant(AssistantMessage {
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".to_string(),
                    name: "read".to_string(),
                    arguments: "{\"path\":\"a\"}".to_string(),
                    custom: false,
                })],
                ..Default::default()
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-1".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "a-body".to_string(),
                })],
                timestamp_ms: 0,
            }),
        ],
        ..Default::default()
    };
    request.demote_orphan_tool_results();
    assert!(
        request
            .dropped
            .iter()
            .any(|d| d == "unmatched_tool_call_id:call-lost"),
        "demotion must be audited in dropped: {:?}",
        request.dropped
    );
    let (built, _repairs) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    let demoted = &built.chat_message_prompts[1];
    assert_eq!(
        demoted.source,
        Some(pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER)
    );
    assert!(
        demoted
            .prompt
            .as_deref()
            .unwrap_or_default()
            .contains("[tool result, original call lost]"),
        "demoted prompt keeps marker + content: {:?}",
        demoted.prompt
    );

    // An orphan result left undemoted keeps its position unpaired — the
    // wire layer never drops messages.
    let undemoted = RequestMessages {
        messages: vec![
            Message::Assistant(AssistantMessage {
                content: vec![Content::ToolCall(ToolCall {
                    id: "call-1".to_string(),
                    name: "read".to_string(),
                    arguments: "{}".to_string(),
                    custom: false,
                })],
                ..Default::default()
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-1".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "a-body".to_string(),
                })],
                timestamp_ms: 0,
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "call-lost".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "orphan".to_string(),
                })],
                timestamp_ms: 0,
            }),
        ],
        ..Default::default()
    };
    let (built, _) =
        build_request(&undemoted, &ClientIdentity::default(), &binding("")).expect("build");
    assert_eq!(
        built.chat_message_prompts.len(),
        3,
        "orphan result must be kept"
    );
    assert_eq!(
        built.chat_message_prompts[2].tool_call_id.as_deref(),
        Some("call-lost")
    );

    // Named tool_choice outside the tools list: readable local rejection.
    let named = RequestMessages {
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: "hi".to_string(),
            })],
            timestamp_ms: 0,
        })],
        tools: vec![ToolDefinition {
            name: "read_file".to_string(),
            description: String::new(),
            input_schema: "{\"type\":\"object\"}".to_string(),
            custom: false,
        }],
        tool_choice: Some(ToolChoice {
            mode: ToolChoiceMode::Named,
            tool_name: "missing_tool".to_string(),
        }),
        ..Default::default()
    };
    let err = build_request(&named, &ClientIdentity::default(), &binding(""))
        .expect_err("named tool_choice outside tools must fail");
    assert_eq!(err.code, "invalid_argument");
    assert!(
        err.message.contains("missing_tool"),
        "error must name the missing tool: {err}"
    );

    // Invalid tool names are rejected at domain validation (upstream's
    // charset gate is declaration-only).
    for bad in ["a.b", "mcp::x", "has space", "工具"] {
        let tool = ToolDefinition {
            name: bad.to_string(),
            description: String::new(),
            input_schema: "{}".to_string(),
            custom: false,
        };
        assert!(
            tool.validate().is_err(),
            "tool name {bad:?} must fail validation"
        );
    }
    for good in ["read_file", "exec-2", "A9_"] {
        let tool = ToolDefinition {
            name: good.to_string(),
            description: String::new(),
            input_schema: "{}".to_string(),
            custom: false,
        };
        assert!(tool.validate().is_ok(), "tool name {good:?} must pass");
    }

    // Repair counters: the repairs_full corpus case counts reorder,
    // dropped-empty-assistant, omitted images and sanitize hits — checked
    // exactly against the oracle in corpus_requests_match_go_oracle; here
    // assert the total aggregation.
    let mut repairs = devin2api::domain::request::RequestRepairs {
        reordered_prompts: 2,
        dropped_empty_assistant: 1,
        omitted_history_images: 1,
        sanitize_hits: BTreeMap::from([("a1-cc-full".to_string(), 1i64)]),
    };
    assert_eq!(repairs.total(), 5);
    repairs.sanitize_hits.clear();
    assert_eq!(repairs.total(), 4);
}

/// Duplicate call-ids bind positionally: the second call with the same id
/// does not reuse the same result (Go
/// `TestPairToolCallsWithResultsConsumesDuplicateID`).
#[test]
fn duplicate_call_ids_pair_positionally() {
    let request = RequestMessages {
        messages: vec![
            Message::Assistant(AssistantMessage {
                content: vec![Content::ToolCall(ToolCall {
                    id: "dup".to_string(),
                    name: "x".to_string(),
                    arguments: "{}".to_string(),
                    custom: false,
                })],
                ..Default::default()
            }),
            Message::Assistant(AssistantMessage {
                content: vec![Content::ToolCall(ToolCall {
                    id: "dup".to_string(),
                    name: "x".to_string(),
                    arguments: "{}".to_string(),
                    custom: false,
                })],
                ..Default::default()
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "dup".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "r".to_string(),
                })],
                timestamp_ms: 0,
            }),
        ],
        ..Default::default()
    };
    let (built, _) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    let prompts = &built.chat_message_prompts;
    assert_eq!(prompts.len(), 3);
    let system = pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_SYSTEM;
    let tool = pb::ExaCodeiumCommonPb_ChatMessageSource::ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL;
    assert_eq!(prompts[0].source, Some(system));
    assert_eq!(prompts[1].source, Some(tool));
    assert_eq!(prompts[2].source, Some(system));
}

/// Multiple results with the same id pair in arrival order and none is
/// dropped (Go `TestPairToolCallsWithResultsKeepsDuplicateResults`).
#[test]
fn duplicate_results_all_kept() {
    let request = RequestMessages {
        messages: vec![
            Message::Assistant(AssistantMessage {
                content: vec![Content::ToolCall(ToolCall {
                    id: "dup".to_string(),
                    name: "x".to_string(),
                    arguments: "{}".to_string(),
                    custom: false,
                })],
                ..Default::default()
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "dup".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "r1".to_string(),
                })],
                timestamp_ms: 0,
            }),
            Message::ToolResult(ToolResultMessage {
                tool_call_id: "dup".to_string(),
                is_error: false,
                content: vec![Content::Text(TextContent {
                    text: "r2".to_string(),
                })],
                timestamp_ms: 0,
            }),
        ],
        ..Default::default()
    };
    let (built, _) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    let prompts = &built.chat_message_prompts;
    assert_eq!(prompts.len(), 3, "no result may be dropped");
    assert_eq!(prompts[1].prompt.as_deref(), Some("r1"));
    assert_eq!(prompts[2].prompt.as_deref(), Some("r2"));
}

/// Metadata shape: 366-byte `fingerprint` → 732 hex chars, captured `CLI`
/// identity fields, locale and `os` (Go `TestBuildRequestMapsLoopMessages`
/// metadata assertions).
#[test]
fn metadata_carries_client_identity() {
    let request = RequestMessages {
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: "hi".to_string(),
            })],
            timestamp_ms: 0,
        })],
        ..Default::default()
    };
    let (built, _) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    let metadata = built.metadata.as_option().expect("metadata");
    let fingerprint = metadata.f.as_deref().expect("fingerprint");
    assert_eq!(fingerprint.len(), 732, "366-byte hex fingerprint");
    assert!(fingerprint.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(metadata.extension_version.as_deref(), Some("3000.2.17"));
    assert_eq!(metadata.ide_version.as_deref(), Some("3000.2.17"));
    assert_eq!(metadata.extension_name.as_deref(), Some("chisel"));
    assert_eq!(metadata.ide_name.as_deref(), Some("chisel"));
    assert_eq!(metadata.os.as_deref(), Some("mac"));
    assert_eq!(metadata.locale.as_deref(), Some("en"));
    assert_eq!(metadata.api_key.as_deref(), Some("token"));
    // Request-level enums and absent fields.
    assert_eq!(
        built.request_type,
        Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_CASCADE)
    );
    assert!(
        built.provider_source.is_none(),
        "provider source must be absent"
    );
    assert_eq!(
        built.planner_mode,
        Some(pb::ExaCodeiumCommonPb_ConversationalPlannerMode::ExaCodeiumCommonPb_ConversationalPlannerMode_CONVERSATIONAL_PLANNER_MODE_DEFAULT)
    );
    let trajectory = built.trajectory_reference.as_option().expect("trajectory");
    assert_eq!(
        trajectory.trajectory_type,
        Some(pb::ExaCortexPb_CortexTrajectoryType::ExaCortexPb_CortexTrajectoryType_CORTEX_TRAJECTORY_TYPE_CASCADE)
    );
    assert_eq!(
        trajectory.step_type,
        Some(
            pb::ExaCortexPb_CortexStepType::ExaCortexPb_CortexStepType_CORTEX_STEP_TYPE_USER_INPUT
        )
    );
    let configuration = built.configuration.as_option().expect("configuration");
    assert_eq!(configuration.max_newlines, Some(400));
    // Custom identity overrides resolve through ClientIdentity::resolve.
    let identity = ClientIdentity {
        name: "  custom-ide ".to_string(),
        version: "9.9".to_string(),
        os: "win".to_string(),
    };
    let (built, _) = build_request(&request, &identity, &binding("")).expect("build");
    let metadata = built.metadata.as_option().expect("metadata");
    assert_eq!(metadata.ide_name.as_deref(), Some("custom-ide"));
    assert_eq!(metadata.ide_version.as_deref(), Some("9.9"));
    assert_eq!(metadata.os.as_deref(), Some("win"));
}

/// `tool_choice`=none drops both the `tools` array and the description
/// injection while still sending `option_name=none` (upstream's real
/// execution disable).
#[test]
fn tool_choice_none_strips_tools_from_wire() {
    let request = RequestMessages {
        system_prompt: "sys".to_string(),
        tools: vec![ToolDefinition {
            name: "read".to_string(),
            description: "read a file".to_string(),
            input_schema: "{\"type\":\"object\"}".to_string(),
            custom: false,
        }],
        tool_choice: Some(ToolChoice {
            mode: ToolChoiceMode::None,
            tool_name: String::new(),
        }),
        messages: vec![Message::User(UserMessage {
            content: vec![Content::Text(TextContent {
                text: "hi".to_string(),
            })],
            timestamp_ms: 0,
        })],
        ..Default::default()
    };
    let (built, _) =
        build_request(&request, &ClientIdentity::default(), &binding("")).expect("build");
    assert!(
        built.tools.is_empty(),
        "no tools on the wire for tool_choice=none"
    );
    assert_eq!(
        built.prompt.as_deref(),
        Some("sys"),
        "no description injection"
    );
    let choice = built.tool_choice.as_option().expect("tool_choice");
    match &choice.choice {
        Some(devin_proto::generated::exa::api_server_pb::__buffa::oneof::exa_chat_pb_chat_tool_choice::Choice::OptionName(name)) => {
            assert_eq!(name, "none");
        }
        other => panic!("expected option_name, got {other:?}"),
    }
}

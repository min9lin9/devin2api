//! Wire-compatibility proof: Go-produced protobuf fixtures cross-decode in
//! Rust and Rust output matches the Go `protojson` oracle.
//!
//! Fixtures under `tests/fixtures/` were produced by the Go reference's own
//! generated module (`outputs/devin-proto-go`, protoc-gen-go v1.36.11) via
//! `tests/fixtures/gen` — see that directory for regeneration. `.bin` files
//! are `proto.Marshal` output; `.json` files are `protojson.Marshal` output.

use std::collections::BTreeMap;
use std::path::PathBuf;

use buffa::serde_json::{self, Value};
use buffa::{DecodeOptions, Message, UnknownFieldData};
use devin_proto::generated::exa::api_server_pb as pb;
use devin_proto::generated::exa::api_server_pb::google_protobuf_extension_range_options;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read(name: &str, ext: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(format!("{name}.{ext}"))).expect("read fixture")
}

fn decode<T: Message>(data: &[u8]) -> T {
    DecodeOptions::new()
        .decode_from_slice::<T>(data)
        .expect("decode fixture")
}

fn encode<T: Message>(msg: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    msg.encode(&mut buf);
    buf
}

/// Semantic `JSON` equality: objects/arrays recursively, all numbers as f64
/// (Go `protojson` emits `1` where `serde_json` emits `1.0` for float fields).
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

fn oracle_json(name: &str) -> Value {
    serde_json::from_slice(&read(name, "json")).expect("parse oracle json")
}

fn rust_json<T: Message + serde::Serialize>(msg: &T) -> Value {
    serde_json::from_str(&serde_json::to_string(msg).expect("serialize")).expect("reparse")
}

fn manifest() -> Value {
    serde_json::from_slice(&read("manifest", "json")).expect("parse manifest")
}

/// Every fixture: Rust decodes the Go bytes, re-encodes, decodes again to an
/// equal message, and serializes to the same proto-`JSON` Go produced.
#[test]
fn go_fixtures_cross_decode() {
    let mut count = 0usize;
    for f in manifest()["fixtures"].as_array().expect("fixtures") {
        let name = f["name"].as_str().expect("name");
        let deterministic = f["deterministic_bytes"].as_bool().expect("flag");
        let go_bin = read(name, "bin");
        let mut oracle = oracle_json(name);
        // Go protojson prints an unknown enum value kept in the field as a
        // bare number ({"stopReason":999}); buffa instead routes it to
        // unknown_fields, so the field is absent from Rust proto-JSON. The
        // wire bytes are byte-identical (asserted below and by
        // unknown_enum_value_routed_to_unknown_fields); only the JSON
        // projection of the unknown value differs, so drop it from the
        // oracle for this fixture.
        if name == "response_unknown_enum_value" {
            oracle.as_object_mut().expect("object").remove("stopReason");
        }

        macro_rules! case {
            ($ty:ty) => {{
                let msg = decode::<$ty>(&go_bin);
                let re = encode(&msg);
                // Deterministic (map-free) fixtures must re-encode to the
                // exact Go bytes — the strongest wire-equality proof.
                if deterministic {
                    assert_eq!(re, go_bin, "{name}: re-encoded bytes differ from Go");
                }
                let msg2 = decode::<$ty>(&re);
                assert_eq!(msg, msg2, "{name}: decode(encode(m)) != m");
                assert!(
                    json_eq(&rust_json(&msg), &oracle),
                    "{name}: Rust proto-JSON != Go protojson oracle\noracle: {oracle}"
                );
            }};
        }

        match name {
            "get_chat_message_request_full"
            | "get_chat_message_request_minimal"
            | "get_chat_message_request_explicit_defaults" => case!(pb::GetChatMessageRequest),
            "get_chat_message_response_stream_delta"
            | "get_chat_message_response_stream_thinking"
            | "get_chat_message_response_stream_tool"
            | "get_chat_message_response_stream_stop"
            | "response_unknown_wire_tag"
            | "response_unknown_enum_value" => case!(pb::GetChatMessageResponse),
            "query_result_map" => case!(pb::QueryResult),
            "deploy_request_oneof_metadata" | "deploy_request_oneof_file_chunk" => {
                case!(pb::DeployWindsurfJSAppRequest);
            }
            "chat_mentions_search_packed_enums" => {
                case!(pb::ExaChatPb_ChatMentionsSearchRequest);
            }
            "plugin_bundle_map_headers" => case!(pb::GetAccountManagedPluginBundleResponse),
            "extension_range_options_default_absent"
            | "extension_range_options_default_explicit" => {
                case!(pb::GoogleProtobuf_ExtensionRangeOptions);
            }
            other => panic!("unmapped fixture {other}"),
        }
        count += 1;
    }
    assert!(count >= 15, "expected the full fixture set, ran {count}");
}

/// `proto2` presence: absent optional fields decode as `None`, not defaults.
#[test]
fn absent_optional_fields_are_none() {
    let msg = decode::<pb::GetChatMessageRequest>(&read("get_chat_message_request_minimal", "bin"));
    assert!(msg.prompt.is_some());
    assert!(msg.metadata.is_set());
    assert!(msg.use_internal_chat_model.is_none());
    assert!(msg.request_type.is_none());
    assert!(!msg.configuration.is_set());
    assert!(msg.tools.is_empty());
    assert!(!msg.tool_choice.is_set());
    assert!(msg.chat_message_prompts.is_empty());
}

/// `proto2` presence: explicitly-encoded default values stay `Some`, which is
/// what distinguishes `optional bool x = N` set-to-false from absent.
#[test]
fn explicit_default_values_keep_presence() {
    let msg = decode::<pb::GetChatMessageRequest>(&read(
        "get_chat_message_request_explicit_defaults",
        "bin",
    ));
    assert_eq!(msg.prompt.as_deref(), Some(""));
    assert_eq!(msg.use_internal_chat_model, Some(false));
    assert_eq!(msg.disable_parallel_tool_calls, Some(false));
    assert_eq!(msg.arena_converge_count, Some(0));
    assert_eq!(
        msg.request_type,
        Some(pb::ChatMessageRequestType::CHAT_MESSAGE_REQUEST_TYPE_UNSPECIFIED)
    );
    // The proto-JSON oracle must also show the explicit defaults.
    let oracle = oracle_json("get_chat_message_request_explicit_defaults");
    assert_eq!(oracle["prompt"], Value::from(""));
    assert_eq!(oracle["useInternalChatModel"], Value::from(false));
}

/// `proto2` `[default = UNVERIFIED]`: absent vs explicitly-encoded UNVERIFIED
/// are different wire states and must stay distinguishable.
#[test]
fn proto2_default_value_presence() {
    let absent = decode::<pb::GoogleProtobuf_ExtensionRangeOptions>(&read(
        "extension_range_options_default_absent",
        "bin",
    ));
    assert!(absent.verification.is_none());
    assert_eq!(absent.declaration.len(), 1);

    let explicit = decode::<pb::GoogleProtobuf_ExtensionRangeOptions>(&read(
        "extension_range_options_default_explicit",
        "bin",
    ));
    assert_eq!(
        explicit.verification,
        Some(google_protobuf_extension_range_options::VerificationState::UNVERIFIED)
    );
    // Re-encoding must preserve the distinction: explicit stays on the wire.
    assert_eq!(
        encode(&explicit),
        read("extension_range_options_default_explicit", "bin")
    );
}

/// Oneof variants decode to the right arm and re-encode identically.
#[test]
fn oneof_variants() {
    use devin_proto::generated::exa::api_server_pb::__buffa::oneof;
    let meta =
        decode::<pb::DeployWindsurfJSAppRequest>(&read("deploy_request_oneof_metadata", "bin"));
    match meta.data {
        Some(oneof::deploy_windsurf_js_app_request::Data::DeploymentMetadata(ref m)) => {
            assert_eq!(m.project_path.as_deref(), Some("/tmp/app"));
            assert_eq!(m.framework.as_deref(), Some("nextjs"));
        }
        other => panic!("expected DeploymentMetadata, got {other:?}"),
    }
    let chunk =
        decode::<pb::DeployWindsurfJSAppRequest>(&read("deploy_request_oneof_file_chunk", "bin"));
    match chunk.data {
        Some(oneof::deploy_windsurf_js_app_request::Data::FileChunk(ref c)) => {
            assert_eq!(c.file_path.as_deref(), Some("pages/index.tsx"));
            assert_eq!(
                c.file_contents.as_deref(),
                Some(&[0x00, 0x01, 0x02, 0xff, 0x7f][..])
            );
        }
        other => panic!("expected FileChunk, got {other:?}"),
    }
}

/// `Map` fields decode with all entries and re-encode semantically equal.
#[test]
fn map_fields() {
    let msg = decode::<pb::QueryResult>(&read("query_result_map", "bin"));
    let want: BTreeMap<&str, &str> = [("alpha", "1"), ("beta", "two"), ("gamma", "")]
        .into_iter()
        .collect();
    let got: BTreeMap<&str, &str> = msg
        .record
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    assert_eq!(got, want);
    // Re-encode -> decode round-trip (byte order is not guaranteed for maps).
    let msg2 = decode::<pb::QueryResult>(&encode(&msg));
    assert_eq!(msg, msg2);
}

/// Packed repeated closed enums decode in order.
#[test]
fn packed_repeated_enums() {
    use pb::ExaCodeiumCommonPb_CodeContextType as T;
    let msg = decode::<pb::ExaChatPb_ChatMentionsSearchRequest>(&read(
        "chat_mentions_search_packed_enums",
        "bin",
    ));
    assert_eq!(
        msg.allowed_types,
        vec![
            T::ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_FILE,
            T::ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_FUNCTION,
            T::ExaCodeiumCommonPb_CodeContextType_CODE_CONTEXT_TYPE_REFERENCE_FUNCTION,
        ]
    );
    assert_eq!(msg.include_repo_info, Some(true));
}

/// Unknown wire tags are preserved in `unknown_fields` and re-encoded
/// verbatim — the same drift tolerance Go's `unknownFields` provides.
#[test]
fn unknown_wire_tag_preserved() {
    let go_bin = read("response_unknown_wire_tag", "bin");
    let msg = decode::<pb::GetChatMessageResponse>(&go_bin);
    assert_eq!(msg.delta_text.as_deref(), Some("x"));
    let unknown: Vec<_> = msg.__buffa_unknown_fields.iter().collect();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].number, 90);
    assert_eq!(unknown[0].data, UnknownFieldData::Varint(7));
    assert_eq!(
        encode(&msg),
        go_bin,
        "unknown field must re-encode verbatim"
    );
}

/// Unknown `proto2` enum values: Go keeps them in the field (`protojson` prints
/// the number); buffa routes them to `unknown_fields` — identical wire bytes,
/// and the value is still observable for drift detection.
#[test]
fn unknown_enum_value_routed_to_unknown_fields() {
    let go_bin = read("response_unknown_enum_value", "bin");
    let msg = decode::<pb::GetChatMessageResponse>(&go_bin);
    assert!(
        msg.stop_reason.is_none(),
        "unknown enum must not decode as a known variant"
    );
    let unknown: Vec<_> = msg.__buffa_unknown_fields.iter().collect();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].number, 5);
    assert_eq!(unknown[0].data, UnknownFieldData::Varint(999));
    assert_eq!(
        encode(&msg),
        go_bin,
        "unknown enum wire value must round-trip"
    );
}

/// Nested message fields (metadata, usage, tool calls, timestamps) decode
/// with full fidelity.
#[test]
fn nested_messages() {
    let msg = decode::<pb::GetChatMessageRequest>(&read("get_chat_message_request_full", "bin"));
    let md = msg.metadata.as_option().expect("metadata");
    assert_eq!(md.ide_name.as_deref(), Some("vscode"));
    assert_eq!(md.request_id, Some(42));
    assert_eq!(md.supported_model_displays.len(), 2);
    assert_eq!(msg.chat_message_prompts.len(), 2);
    let p0 = &msg.chat_message_prompts[0];
    assert_eq!(p0.tool_calls.len(), 1);
    assert_eq!(p0.tool_calls[0].name.as_deref(), Some("read_file"));
    assert_eq!(p0.images.len(), 1);
    assert_eq!(p0.images[0].mime_type.as_deref(), Some("image/png"));
    assert_eq!(msg.tools.len(), 2);

    let stop =
        decode::<pb::GetChatMessageResponse>(&read("get_chat_message_response_stream_stop", "bin"));
    let usage = stop.usage.as_option().expect("usage");
    assert_eq!(usage.input_tokens, Some(1024));
    assert_eq!(usage.output_tokens, Some(256));
    assert_eq!(
        usage.response_header.get("x-upstream").map(String::as_str),
        Some("devin")
    );
    assert_eq!(stop.committed_quota_cost_basis_points, Some(12345));
    let delta = decode::<pb::GetChatMessageResponse>(&read(
        "get_chat_message_response_stream_delta",
        "bin",
    ));
    let ts = delta.timestamp.as_option().expect("timestamp");
    assert_eq!(ts.seconds, Some(1_758_000_000));
    assert_eq!(ts.nanos, Some(123_456_789));
}

/// The generated Connect service surface keeps the original service name and
/// per-method paths/stream types used by the Go client.
#[test]
fn connect_service_names_and_specs() {
    assert_eq!(
        pb::API_SERVER_SERVICE_SERVICE_NAME,
        "exa.api_server_pb.ApiServerService"
    );
    // The two RPCs the Go adapter actually calls.
    assert_eq!(
        pb::API_SERVER_SERVICE_GET_CHAT_MESSAGE_SPEC.procedure,
        "/exa.api_server_pb.ApiServerService/GetChatMessage"
    );
    assert_eq!(
        pb::API_SERVER_SERVICE_GET_CHAT_MESSAGE_SPEC.stream_type,
        connectrpc::StreamType::ServerStream
    );
    assert_eq!(
        pb::API_SERVER_SERVICE_ASSIGN_MODEL_SPEC.procedure,
        "/exa.api_server_pb.ApiServerService/AssignModel"
    );
    assert_eq!(
        pb::API_SERVER_SERVICE_ASSIGN_MODEL_SPEC.stream_type,
        connectrpc::StreamType::Unary
    );
}

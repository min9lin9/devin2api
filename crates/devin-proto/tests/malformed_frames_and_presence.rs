//! `Failure`-path QA: malformed wire data is rejected and `proto2` presence
//! semantics distinguish absent from erased/explicit-default fields.

use std::path::PathBuf;

use buffa::{DecodeOptions, Message};
use devin_proto::generated::exa::api_server_pb as pb;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(format!("{name}.bin"))).expect("read fixture")
}

fn decode<T: Message>(data: &[u8]) -> Result<T, buffa::DecodeError> {
    DecodeOptions::new().decode_from_slice::<T>(data)
}

/// A truncated protobuf payload must fail to decode, never silently
/// produce a partial message.
#[test]
fn truncated_protobuf_rejected() {
    let go_bin = read("get_chat_message_request_full");
    // Cut mid-field: keep the first 40 bytes (inside the metadata submessage).
    let truncated = &go_bin[..40];
    assert!(
        decode::<pb::GetChatMessageRequest>(truncated).is_err(),
        "truncated message must not decode"
    );

    // Truncated varint: tag for field 4 (delta_tokens) then a varint
    // continuation byte with no successor.
    let bad_varint = [0x20u8, 0x80];
    assert!(decode::<pb::GetChatMessageResponse>(&bad_varint).is_err());

    // Length-delimited field whose declared length overruns the buffer:
    // field 3 (delta_text) claims 10 bytes, supplies 2.
    let overrun = [0x1au8, 0x0a, b'h', b'i'];
    assert!(decode::<pb::GetChatMessageResponse>(&overrun).is_err());
}

/// An intentionally erased optional field must decode as absent — this is
/// the negative control proving presence detection actually works (a codec
/// that defaulted `prompt` to `Some("")` would pass the decode but fail
/// this assertion).
#[test]
fn erased_optional_field_detected() {
    let go_bin = read("get_chat_message_request_full");
    let mut msg = decode::<pb::GetChatMessageRequest>(&go_bin).expect("decode");
    assert!(msg.prompt.is_some(), "fixture must start with prompt set");

    // Erase the field and re-encode: the wire bytes no longer carry tag 2.
    msg.prompt = None;
    let mut buf = Vec::new();
    msg.encode(&mut buf);
    let round = decode::<pb::GetChatMessageRequest>(&buf).expect("re-decode");
    assert!(
        round.prompt.is_none(),
        "erased optional field must decode as absent, not as a default"
    );

    // And the JSON form must not contain the key at all.
    let json = buffa::serde_json::to_string(&round).expect("serialize");
    let value: buffa::serde_json::Value = buffa::serde_json::from_str(&json).expect("parse");
    assert!(value.get("prompt").is_none());
}

/// Empty payloads decode to a fully-absent message (`proto2`: every field
/// optional). Presence must not be invented.
#[test]
fn empty_message_has_no_present_fields() {
    let msg = decode::<pb::GetChatMessageResponse>(&[]).expect("empty decodes");
    assert!(msg.message_id.is_none());
    assert!(msg.delta_text.is_none());
    assert!(msg.delta_tokens.is_none());
    assert!(msg.stop_reason.is_none());
    assert!(!msg.timestamp.is_set());
    assert!(!msg.usage.is_set());
    assert!(msg.delta_tool_calls.is_empty());
    assert!(msg.__buffa_unknown_fields.is_empty());
    // Re-encoding an all-absent message yields zero bytes.
    let mut buf = Vec::new();
    msg.encode(&mut buf);
    assert!(buf.is_empty());
}

/// Invalid wire types are rejected: a field declared varint on the schema
/// but arriving as start-group must not decode.
#[test]
fn wrong_wire_type_rejected() {
    // field 4 (delta_tokens, varint) encoded as start-group (wire type 3).
    let bad = [0x23u8, 0x00];
    assert!(decode::<pb::GetChatMessageResponse>(&bad).is_err());
}

/// `proto2` `required` fields keep explicit presence: a missing required
/// field decodes as absent (`None`) without error — matching Go's
/// `proto.Unmarshal`, which does not enforce required on read.
/// (`GoogleProtobuf_FileDescriptorProto.name` is `required`.)
#[test]
fn missing_required_field_decodes_absent() {
    let msg = decode::<pb::GoogleProtobuf_FileDescriptorProto>(&[]).expect("decodes");
    assert!(msg.name.is_none(), "missing required field stays absent");
    let only_package = [0x12u8, 0x03, b'f', b'o', b'o'];
    let msg2 = decode::<pb::GoogleProtobuf_FileDescriptorProto>(&only_package).expect("decodes");
    assert!(msg2.name.is_none());
    assert_eq!(msg2.package.as_deref(), Some("foo"));
}

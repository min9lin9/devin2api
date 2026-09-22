//! Emit Rust-produced wire fixtures for cross-verification by the Go oracle.
//!
//! For every checked-in Go fixture `tests/fixtures/<name>.bin` this decodes
//! the bytes with the generated buffa types, re-encodes them to
//! `<out>/<name>.bin`, and serializes the decoded message to
//! `<out>/<name>.json` (proto-`JSON` via serde). `scripts/verify-wire-fixtures.sh`
//! then asks the Go oracle to decode the Rust bytes and compare `protojson`.
//!
//! Usage: `cargo run -p devin-proto --example emit_fixtures -- <out-dir>`

use std::path::PathBuf;

use buffa::{DecodeOptions, Message};
use devin_proto::buffa::serde_json;
use devin_proto::generated::exa::api_server_pb as pb;

fn decode<T: Message>(data: &[u8]) -> T {
    DecodeOptions::new()
        .decode_from_slice::<T>(data)
        .expect("decode fixture")
}

// name -> decode+re-encode+serialize closure. The set must match
// tests/fixtures/manifest.json; a missing entry fails loudly below.
type Emit = fn(&[u8]) -> (Vec<u8>, String);

fn main() {
    let out_dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: emit_fixtures <out-dir>"),
    );
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    std::fs::create_dir_all(&out_dir).expect("create out dir");

    let cases: Vec<(&str, Emit)> = vec![
        (
            "get_chat_message_request_full",
            emit::<pb::GetChatMessageRequest>,
        ),
        (
            "get_chat_message_request_minimal",
            emit::<pb::GetChatMessageRequest>,
        ),
        (
            "get_chat_message_request_explicit_defaults",
            emit::<pb::GetChatMessageRequest>,
        ),
        (
            "get_chat_message_response_stream_delta",
            emit::<pb::GetChatMessageResponse>,
        ),
        (
            "get_chat_message_response_stream_thinking",
            emit::<pb::GetChatMessageResponse>,
        ),
        (
            "get_chat_message_response_stream_tool",
            emit::<pb::GetChatMessageResponse>,
        ),
        (
            "get_chat_message_response_stream_stop",
            emit::<pb::GetChatMessageResponse>,
        ),
        ("query_result_map", emit::<pb::QueryResult>),
        (
            "deploy_request_oneof_metadata",
            emit::<pb::DeployWindsurfJSAppRequest>,
        ),
        (
            "deploy_request_oneof_file_chunk",
            emit::<pb::DeployWindsurfJSAppRequest>,
        ),
        (
            "chat_mentions_search_packed_enums",
            emit::<pb::ExaChatPb_ChatMentionsSearchRequest>,
        ),
        (
            "plugin_bundle_map_headers",
            emit::<pb::GetAccountManagedPluginBundleResponse>,
        ),
        (
            "extension_range_options_default_absent",
            emit::<pb::GoogleProtobuf_ExtensionRangeOptions>,
        ),
        (
            "extension_range_options_default_explicit",
            emit::<pb::GoogleProtobuf_ExtensionRangeOptions>,
        ),
        (
            "response_unknown_wire_tag",
            emit::<pb::GetChatMessageResponse>,
        ),
        (
            "response_unknown_enum_value",
            emit::<pb::GetChatMessageResponse>,
        ),
    ];

    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(fixtures.join("manifest.json")).expect("read manifest"),
    )
    .expect("parse manifest");
    let listed: std::collections::BTreeSet<&str> = manifest["fixtures"]
        .as_array()
        .expect("fixtures array")
        .iter()
        .map(|f| f["name"].as_str().expect("name"))
        .collect();
    let covered: std::collections::BTreeSet<&str> = cases.iter().map(|(n, _)| *n).collect();
    assert_eq!(listed, covered, "emitter coverage must match manifest");

    for (name, f) in &cases {
        let go_bin = std::fs::read(fixtures.join(format!("{name}.bin"))).expect("read go bin");
        let (rust_bin, rust_json) = f(&go_bin);
        std::fs::write(out_dir.join(format!("{name}.bin")), &rust_bin).expect("write bin");
        std::fs::write(out_dir.join(format!("{name}.json")), &rust_json).expect("write json");
        eprintln!("emitted {name} ({} bytes)", rust_bin.len());
    }
}

fn emit<T: Message + serde::Serialize>(go_bin: &[u8]) -> (Vec<u8>, String) {
    let msg = decode::<T>(go_bin);
    let mut buf = Vec::new();
    msg.encode(&mut buf);
    let json = serde_json::to_string(&msg).expect("serialize");
    (buf, json)
}

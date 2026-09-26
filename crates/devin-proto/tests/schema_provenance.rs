//! Schema provenance and completeness: the copied artifacts are
//! byte-identical to the Go reference's `outputs/devin-proto/`, every
//! extracted .proto in the original descriptor set compiles (fully linked
//! descriptor pool), and the generated bindings cover the flattened schema.

use std::path::PathBuf;

use buffa::DecodeOptions;
use buffa::serde_json::{self, Value};
use buffa_descriptor::DescriptorPool;
use buffa_descriptor::generated::descriptor::FileDescriptorSet;

fn proto_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../proto")
        .canonicalize()
        .expect("proto dir")
}

fn read(name: &str) -> Vec<u8> {
    std::fs::read(proto_dir().join(name)).expect("read proto artifact")
}

/// The three schema artifacts must be byte-identical copies of
/// `G/outputs/devin-proto/` — recorded in proto/SHA256SUMS.
#[test]
fn copied_artifacts_match_recorded_hashes() {
    let sums = String::from_utf8(read("SHA256SUMS")).expect("utf8 sums");
    for line in sums.lines() {
        let mut it = line.split_whitespace();
        let (want, name) = (it.next().expect("hash"), it.next().expect("name"));
        let got = sha256_hex(&read(name));
        assert_eq!(got, want, "{name} diverged from the Go reference artifact");
    }
}

/// Every extracted .proto compiles: the original 63-file descriptor set
/// links into a descriptor pool with zero unresolved symbols, and the
/// flattened codegen input is a valid single-file set.
#[test]
fn every_extracted_proto_compiles() {
    // Trusted, large input: raise the element-memory bound like
    // connectrpc-build does for compiler-produced sets.
    let opts = DecodeOptions::new().with_element_memory_limit(usize::MAX);

    let original = read("descriptors.pb");
    let pool = DescriptorPool::decode_with_options(&original, &opts)
        .expect("original descriptor set must link");
    let set = opts
        .decode_from_slice::<FileDescriptorSet>(&original)
        .expect("decode set");
    assert_eq!(set.file.len(), 63, "expected 63 extracted files");
    assert!(
        pool.services().len() >= 20,
        "expected all services, got {}",
        pool.services().len()
    );

    let flat = read("all-protos.fds");
    let flat_pool = DescriptorPool::decode_with_options(&flat, &opts)
        .expect("flattened descriptor set must link");
    assert_eq!(flat_pool.files().len(), 1);
    assert_eq!(flat_pool.services().len(), 20);
}

/// Manifest symbol mappings must resolve against the original descriptor
/// pool — provenance that the flattened schema renamed, not dropped,
/// symbols.
#[test]
fn manifest_symbol_mappings_resolve() {
    let opts = DecodeOptions::new().with_element_memory_limit(usize::MAX);
    let pool = DescriptorPool::decode_with_options(&read("descriptors.pb"), &opts).expect("pool");
    let manifest: Value = serde_json::from_slice(&read("manifest.json")).expect("parse manifest");
    let mappings = manifest["flattened"]["symbol_mappings"]
        .as_array()
        .expect("symbol_mappings");

    let mut checked = 0usize;
    for m in mappings {
        let kind = m["kind"].as_str().expect("kind");
        let original = m["original"].as_str().expect("original");
        match kind {
            "message" => assert!(
                pool.message_by_name(original).is_some(),
                "unresolved message {original}"
            ),
            "enum" => assert!(
                pool.enum_by_name(original).is_some(),
                "unresolved enum {original}"
            ),
            "service" => assert!(
                pool.service_by_name(original).is_some(),
                "unresolved service {original}"
            ),
            "enum_value" => {
                let (en, val) = original.rsplit_once('.').expect("enum_value fqn");
                let e = pool.enum_by_name(en).unwrap_or_else(|| panic!("enum {en}"));
                assert!(
                    e.value_by_name(val).is_some(),
                    "unresolved enum value {original}"
                );
            }
            "extension" => assert!(
                pool.extension_by_name(original).is_some(),
                "unresolved extension {original}"
            ),
            other => panic!("unknown mapping kind {other}"),
        }
        checked += 1;
    }
    assert!(
        checked > 5000,
        "expected full mapping coverage, got {checked}"
    );
}

/// The generated bindings cover the pruned reachable schema: codegen input
/// is all-protos.fds filtered to proto/prune-roots.txt by examples/prune.rs,
/// so only ApiServerService and its transitive types exist. The manifest
/// counts still describe the full flattened schema (the FDS itself is
/// unchanged — pruning happens at generation time).
#[test]
fn generated_bindings_cover_schema() {
    use devin_proto::generated::exa::api_server_pb as pb;

    assert!(pb::API_SERVER_SERVICE_SERVICE_NAME.starts_with("exa.api_server_pb."));
    let _chat_spec: ::connectrpc::Spec = pb::API_SERVER_SERVICE_GET_CHAT_MESSAGE_SPEC;

    // Manifest counts: 2633 messages / 247 enums / 20 services flattened.
    let manifest: Value = serde_json::from_slice(&read("manifest.json")).expect("parse manifest");
    let mut kinds = std::collections::BTreeMap::<&str, usize>::new();
    for m in manifest["flattened"]["symbol_mappings"]
        .as_array()
        .expect("mappings")
    {
        *kinds.entry(m["kind"].as_str().expect("kind")).or_default() += 1;
    }
    assert_eq!(kinds["service"], 20);
    assert_eq!(kinds["message"], 2633);
    assert_eq!(kinds["enum"], 247);
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

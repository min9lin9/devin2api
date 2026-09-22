use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::thread;

use buffa::{DecodeOptions, MessageField};
use buffa_descriptor::DescriptorPool;
use buffa_descriptor::generated::descriptor::{
    FileDescriptorSet, SourceCodeInfo, source_code_info::Location,
};
use devin2api::auxiliary::{census, flatten};

fn bin(name: &str) -> PathBuf {
    PathBuf::from(
        std::env::var(format!("CARGO_BIN_EXE_{name}")).unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target/debug")
                .join(name)
                .display()
                .to_string()
        }),
    )
}

fn temp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("devin2api-aux-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(name: &str, args: &[&str]) -> std::process::Output {
    Command::new(bin(name)).args(args).output().unwrap()
}

#[test]
fn unknown_commands_are_usage_errors_before_credentials() {
    let out = Command::new(bin("probe"))
        .arg("not-a-command")
        .env_remove("DEVIN_TOKEN")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("subcommands:"));
    let out = run("protocensus", &["nope"]);
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn protoextract_scans_and_census_diff_reports_change() {
    // FileDescriptorProto { name:"fixture.proto", package:"qa", message_type:
    // DescriptorProto { name:"Ping", field:{name:"text",number:1,label:1,type:9} }, syntax:"proto3" }
    let descriptor = [
        0x0a, 0x0d, b'f', b'i', b'x', b't', b'u', b'r', b'e', b'.', b'p', b'r', b'o', b't', b'o',
        0x12, 0x02, b'q', b'a', 0x22, 0x12, 0x0a, 0x04, b'P', b'i', b'n', b'g', 0x12, 0x0a, 0x0a,
        0x04, b't', b'e', b'x', b't', 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x62, 0x06, b'p', b'r',
        b'o', b't', b'o', b'3',
    ];
    let root = temp("schema");
    let binary = root.join("fixture.bin");
    std::fs::write(
        &binary,
        [b"prefix".as_slice(), descriptor.as_slice(), b"suffix"].concat(),
    )
    .unwrap();
    let output = root.join("out");
    let out = run(
        "protoextract",
        &[binary.to_str().unwrap(), output.to_str().unwrap()],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(output.join("descriptors.pb").is_file());
    assert!(
        std::fs::read_to_string(output.join("all-protos.proto"))
            .unwrap()
            .contains("message Ping")
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["descriptor_count"], 1);

    let old = output.join("descriptors.pb");
    let mut changed_descriptor = descriptor;
    let number = changed_descriptor
        .windows(2)
        .position(|w| w == [0x18, 0x01])
        .unwrap()
        + 1;
    changed_descriptor[number] = 2;
    let binary2 = root.join("fixture2.bin");
    std::fs::write(&binary2, changed_descriptor).unwrap();
    let output2 = root.join("out2");
    assert!(
        run(
            "protoextract",
            &[binary2.to_str().unwrap(), output2.to_str().unwrap()]
        )
        .status
        .success()
    );
    let diff = run(
        "protocensus",
        &[
            "diff",
            old.to_str().unwrap(),
            output2.join("descriptors.pb").to_str().unwrap(),
        ],
    );
    assert!(diff.status.success());
    let report: serde_json::Value = serde_json::from_slice(&diff.stdout).unwrap();
    assert!(
        report["added"].as_array().unwrap().len()
            + report["removed"].as_array().unwrap().len()
            + report["changed"].as_array().unwrap().len()
            > 0
    );
}

#[test]
fn flattened_real_schema_links_and_preserves_streaming_and_comments() {
    let raw = std::fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto/descriptors.pb"))
        .unwrap();
    let mut set: FileDescriptorSet = DecodeOptions::new()
        .with_element_memory_limit(usize::MAX)
        .decode_from_slice(&raw)
        .unwrap();
    set.file[0].source_code_info = MessageField::some(SourceCodeInfo {
        location: vec![Location {
            path: vec![],
            span: vec![0, 0, 0],
            leading_comments: Some(" preserved extraction comment".to_string()),
            ..Default::default()
        }],
        ..Default::default()
    });
    let original_has_streaming = set
        .file
        .iter()
        .flat_map(|f| &f.service)
        .flat_map(|s| &s.method)
        .any(|m| m.server_streaming == Some(true) || m.client_streaming == Some(true));
    assert!(original_has_streaming);
    let (flat, metadata) =
        flatten::flatten_descriptors(&set.file, flatten::PREFERRED_ROOT_PACKAGE).unwrap();
    DescriptorPool::new(FileDescriptorSet {
        file: vec![flat.clone()],
        ..Default::default()
    })
    .expect("flattened descriptor must link as one file");
    assert!(
        flat.service
            .iter()
            .flat_map(|s| &s.method)
            .any(|m| m.server_streaming == Some(true) || m.client_streaming == Some(true))
    );
    assert!(metadata.symbol_mappings.len() > 5_000);
    let rendered = flatten::render_flattened(&flat, set.file.len());
    assert!(rendered.contains("preserved extraction comment"));
    assert!(
        rendered.contains("stream ")
            || flat
                .service
                .iter()
                .flat_map(|s| &s.method)
                .any(|m| m.server_streaming == Some(true))
    );
}

#[test]
fn census_walks_nested_lists_and_reports_enum_drift() {
    let pool = census::schema_pool().unwrap();
    let md = pool
        .message_by_name(census::REQUEST_TYPE_NAME)
        .unwrap()
        .clone();
    let mut scan = census::Census::new(&pool);
    scan.set_current_dir("fixture-dir");
    let value = serde_json::json!({
        "metadata": {"unknownNested": true},
        "chatMessagePrompts": [{"source": 2_147_483_647, "unknownMessageKey": 1}],
        "requestType": 2_147_483_647
    });
    scan.walk(&md, value.as_object().unwrap());
    let report = census::census_section(&scan);
    assert!(
        report["messages"].as_object().unwrap().len() >= 3,
        "{report}"
    );
    assert!(
        report["unknown_keys"].as_array().unwrap().len() >= 2,
        "{report}"
    );
    assert!(
        report["enum_anomalies"].as_array().unwrap().len() >= 2,
        "{report}"
    );
}

#[test]
fn corrupted_descriptor_is_nonzero() {
    let root = temp("bad");
    let old = root.join("old.pb");
    let new = root.join("new.pb");
    std::fs::write(&old, [0xff, 0xff]).unwrap();
    std::fs::write(&new, []).unwrap();
    assert_eq!(
        run(
            "protocensus",
            &["diff", old.to_str().unwrap(), new.to_str().unwrap()]
        )
        .status
        .code(),
        Some(1)
    );
}

#[test]
fn loadtest_reports_wire_and_semantic_latency() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 4096];
        let _ = stream.read(&mut request).unwrap();
        let body = "data: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"pong\"}}]}\n\ndata: [DONE]\n\n";
        write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
    });
    let out = run(
        "loadtest",
        &[
            "-url",
            &format!("http://{addr}/v1/chat/completions"),
            "-c",
            "1",
            "-n",
            "1",
        ],
    );
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    server.join().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("ttfb_ms")
            && text.contains("wire_first_byte_ms")
            && text.contains("semantic_first_content_ms"),
        "{text}"
    );
}

#[test]
fn upstreamstub_rejects_unknown_scenario() {
    let out = run(
        "upstreamstub",
        &["-scenario", "typo", "-listen", "127.0.0.1:0"],
    );
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown scenario"));
}

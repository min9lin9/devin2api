//! `qa cli` — auxiliary-command parity harness. Builds the Go reference
//! binaries (`probe`, `protoextract`, `protocensus`, `loadtest`,
//! `upstreamstub`) from `--go-root` and the Rust equivalents from this
//! crate, then runs both against identical inputs:
//!
//! - **malformed**: usage errors, missing args, bad flags, unsafe output
//!   dirs, unknown scenarios — exit codes compared.
//! - **protoextract**: a synthetic binary with embedded descriptors is
//!   extracted by both; `descriptors.pb` must be byte-identical, the
//!   manifest equal, and the complete flattened proto token stream equal.
//! - **protocensus**: `diff` and `census` stdout compared byte-for-byte
//!   over a fixture log tree that carries unknown keys and an
//!   out-of-range enum.
//! - **upstreamstub**: every scenario is driven with a real Connect
//!   streaming request; response frames are decoded and compared (the
//!   timestamp field is normalized), hang scenarios are verified to
//!   deliver their prefix then hold the connection open.
//! - **loadtest**: both binaries fire at a local SSE fixture; the
//!   request/ok/error counts must match and every Go metric line must
//!   exist in the Rust output (which adds `wire_first_byte_ms` and
//!   `semantic_first_content_ms`).
//! - **probe**: a loopback Connect server records each request; every
//!   subcommand runs against both implementations and the decoded
//!   request messages are compared with volatile fields (ids, keys,
//!   timestamps) normalized — the strongest available check that the
//!   Rust probe sends the same wire shape.
//! - **example**: `examples/devin_client.rs` is built and run against
//!   the Rust stub in `stream` mode.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use buffa::{Message, MessageField};
use devin_proto::generated::exa::api_server_pb as pb;
use serde::Serialize;

const AUX_TOOLS: &[&str] = &[
    "probe",
    "protoextract",
    "protocensus",
    "loadtest",
    "upstreamstub",
];

const CHAT_PATH: &str = "/exa.api_server_pb.ApiServerService/GetChatMessage";
const ASSIGN_PATH: &str = "/exa.api_server_pb.ApiServerService/AssignModel";
const EXTCHAT_PATH: &str =
    "/exa.api_server_pb.ApiServerService/GetStreamingExternalChatCompletions";

const PROBE_UNARY_PATHS: &[&str] = &[
    ASSIGN_PATH,
    "/exa.api_server_pb.ApiServerService/CheckChatCapacity",
    "/exa.api_server_pb.ApiServerService/CheckUserMessageRateLimit",
    "/exa.api_server_pb.ApiServerService/GetModelStatuses",
    "/exa.api_server_pb.ApiServerService/GetModelProviders",
    "/exa.api_server_pb.ApiServerService/GetCliModelConfigs",
    "/exa.api_server_pb.ApiServerService/GetCommandModelConfigs",
    "/exa.api_server_pb.ApiServerService/GetStatus",
    "/exa.api_server_pb.ApiServerService/GetConfig",
    "/exa.api_server_pb.ApiServerService/GetEmbeddings",
];

fn known_probe_path(path: &str) -> bool {
    path == CHAT_PATH || path == EXTCHAT_PATH || PROBE_UNARY_PATHS.contains(&path)
}

/// The `qa cli` report written to `<evidence>/qa.json`.
#[derive(Serialize)]
pub struct CliReport {
    pub subcommand: &'static str,
    pub passed: bool,
    pub targets: Vec<TargetResult>,
}

/// One compared case.
#[derive(Serialize)]
pub struct TargetResult {
    pub name: String,
    pub status: &'static str,
    pub details: serde_json::Value,
}

fn pass(name: &str, details: serde_json::Value) -> TargetResult {
    TargetResult {
        name: name.to_string(),
        status: "pass",
        details,
    }
}

fn fail(name: &str, details: serde_json::Value) -> TargetResult {
    TargetResult {
        name: name.to_string(),
        status: "fail",
        details,
    }
}

/// `run` — execute the `qa cli` harness. `case` filters to one target
/// group (`malformed`, `protoextract`, `protocensus`, `upstreamstub`,
/// `loadtest`, `probe`, `example`) or one target name.
///
/// # Errors
///
/// `anyhow` for build failures or unreadable evidence directories.
pub fn run(go_root: &Path, evidence: &Path, case: Option<&str>) -> anyhow::Result<CliReport> {
    let case = match case {
        Some("malformed-input-and-unknown-command") => Some("malformed"),
        other => other,
    };
    let work = evidence.join("work");
    let malformed_dir = evidence.join("malformed");
    std::fs::create_dir_all(&work)?;
    std::fs::create_dir_all(&malformed_dir)?;

    // Build both implementations up front so a compile failure is one
    // error, not forty.
    let rust_bins = build_rust(&work)?;
    let go_bins = build_go(go_root, &work)?;

    let wanted = |group: &str, name: &str| -> bool {
        match case {
            None => true,
            Some(c) => c == group || c == name,
        }
    };

    let mut targets = Vec::new();
    if wanted("malformed", "malformed") {
        targets.push(malformed_cases(&rust_bins, &go_bins, &malformed_dir));
    }
    if wanted("protoextract", "protoextract") {
        targets.push(protoextract_case(&rust_bins, &go_bins, &work));
    }
    if wanted("protocensus", "protocensus") {
        targets.push(protocensus_case(&rust_bins, &go_bins, &work));
    }
    if case.is_none_or(|c| c == "upstreamstub" || c.starts_with("upstreamstub-")) {
        targets.extend(
            upstreamstub_cases(&rust_bins, &go_bins, &work)
                .into_iter()
                .filter(|t| wanted("upstreamstub", &t.name)),
        );
    }
    if case.is_none_or(|c| c == "loadtest" || c.starts_with("loadtest-")) {
        targets.extend(
            loadtest_cases(&rust_bins, &go_bins, &work)
                .into_iter()
                .filter(|t| wanted("loadtest", &t.name)),
        );
    }
    if case.is_none_or(|c| {
        c == "probe"
            || (c != "malformed"
                && c != "protoextract"
                && c != "protocensus"
                && c != "example"
                && c != "example-devin-client"
                && !c.starts_with("upstreamstub-")
                && !c.starts_with("loadtest-"))
    }) {
        targets.extend(
            probe_cases(&rust_bins, &go_bins, &work)
                .into_iter()
                .filter(|t| wanted("probe", &t.name)),
        );
    }
    if wanted("example", "example-devin-client") {
        targets.push(example_case(&rust_bins, &work));
    }
    if targets.is_empty() {
        anyhow::bail!("unknown cli case {}", case.unwrap_or_default());
    }

    Ok(CliReport {
        subcommand: "cli",
        passed: targets.iter().all(|t| t.status == "pass"),
        targets,
    })
}

// ---------- build helpers -------------------------------------------------------

/// Build the Rust aux binaries + example with `cargo --locked`.
fn build_rust(work: &Path) -> anyhow::Result<BTreeMap<String, PathBuf>> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut command = Command::new("cargo");
    command
        .args(["build", "--locked", "--bins", "--example", "devin_client"])
        .current_dir(manifest_dir);
    let (output, timed_out) = bounded_output(&mut command, Duration::from_mins(15))?;
    if timed_out {
        anyhow::bail!("cargo build --locked --bins exceeded 900 seconds");
    }
    std::fs::write(work.join("cargo-build.log"), &output.stderr)?;
    if !output.status.success() {
        anyhow::bail!(
            "cargo build --locked --bins failed; see {}",
            work.join("cargo-build.log").display()
        );
    }
    let dir = manifest_dir.join("target/debug");
    let mut map = BTreeMap::new();
    for name in AUX_TOOLS {
        map.insert((*name).to_string(), dir.join(name));
    }
    map.insert(
        "devin_client".to_string(),
        dir.join("examples/devin_client"),
    );
    Ok(map)
}

fn build_go(go_root: &Path, work: &Path) -> anyhow::Result<BTreeMap<String, PathBuf>> {
    let bin_dir = work.join("bin");
    std::fs::create_dir_all(&bin_dir)?;
    let configured = std::env::var_os("QA_GO_BIN").map(PathBuf::from);
    let sdk = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("sdk/go/bin/go");
    let go = configured.unwrap_or_else(|| {
        if sdk.is_file() {
            sdk
        } else {
            PathBuf::from("go")
        }
    });
    let mut bins = BTreeMap::new();
    for tool in AUX_TOOLS {
        let output_path = bin_dir.join(tool);
        let mut command = Command::new(&go);
        command
            .args(["build", "-o"])
            .arg(&output_path)
            .arg(format!("./cmd/{tool}"))
            .current_dir(go_root)
            .env("GOFLAGS", "-mod=readonly")
            .env("GOTOOLCHAIN", "local");
        let (output, timed_out) = bounded_output(&mut command, Duration::from_secs(600))?;
        std::fs::write(work.join(format!("go-build-{tool}.log")), &output.stderr)?;
        if timed_out || !output.status.success() {
            anyhow::bail!("go build {tool} failed or timed out; see evidence build log");
        }
        bins.insert((*tool).to_string(), output_path);
    }
    Ok(bins)
}

// ---------- process helpers ------------------------------------------------------

struct RunOut {
    code: i32,
    stdout: String,
    stderr: String,
}

fn bounded_output(
    command: &mut Command,
    timeout: Duration,
) -> anyhow::Result<(std::process::Output, bool)> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn()?;
    let pid = child.id();
    let (done_tx, done_rx) = mpsc::channel();
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_watchdog = Arc::clone(&timed_out);
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(timeout).is_err() {
            timed_out_watchdog.store(true, Ordering::SeqCst);
            let _ = Command::new("/bin/kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    });
    let output = child.wait_with_output()?;
    let _ = done_tx.send(());
    let _ = watchdog.join();
    Ok((output, timed_out.load(Ordering::SeqCst)))
}

/// Run a binary with an env-scrubbed environment: no inherited
/// credentials or config can leak into the aux tools. `HOME` and
/// `XDG_DATA_HOME` point at empty dirs under `cwd` so the credentials
/// discovery chain finds nothing unless a case opts in.
fn run_scrubbed(
    bin: &Path,
    args: &[&str],
    cwd: &Path,
    extra_env: &[(&str, &str)],
    timeout: Duration,
) -> anyhow::Result<RunOut> {
    let mut cmd = Command::new(bin);
    cmd.args(args)
        .current_dir(cwd)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("HOME", cwd.join("home"))
        .env("XDG_DATA_HOME", cwd.join("xdg"))
        .env("DEVIN2API_CONFIG", cwd.join("config.yaml"))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let child = cmd.spawn()?;
    let pid = child.id();
    let (done_tx, done_rx) = mpsc::channel();
    let timed_out = Arc::new(AtomicBool::new(false));
    let timed_out_watchdog = Arc::clone(&timed_out);
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(timeout).is_err() {
            timed_out_watchdog.store(true, Ordering::SeqCst);
            let _ = Command::new("/bin/kill")
                .args(["-KILL", &pid.to_string()])
                .status();
        }
    });
    let out = child.wait_with_output()?;
    let _ = done_tx.send(());
    let _ = watchdog.join();
    if timed_out.load(Ordering::SeqCst) {
        return Ok(RunOut {
            code: -1,
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: "qa: timed out".to_string(),
        });
    }
    Ok(RunOut {
        code: out.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

// ---------- malformed cases -------------------------------------------------------

/// Exit-code parity for usage errors and invalid inputs. Stderr wording
/// is recorded in artifacts but not byte-compared (OS error strings
/// differ in casing across runtimes).
// One table-driven case list; splitting it would scatter the
// exit-code parity matrix.
#[allow(clippy::too_many_lines)]
fn malformed_cases(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    dir: &Path,
) -> TargetResult {
    struct Case {
        name: &'static str,
        tool: &'static str,
        args: Vec<String>,
        env: Vec<(String, String)>,
    }
    let cwd = dir.join("cwd");
    let _ = std::fs::create_dir_all(cwd.join("home"));
    let _ = std::fs::create_dir_all(cwd.join("xdg"));
    // A tokenless config so the no-token path is reached deterministically.
    let _ = std::fs::write(cwd.join("config.yaml"), "server:\n  listen: :1\n");

    let token = || ("DEVIN_TOKEN".to_string(), "qa-token".to_string());
    let cases = vec![
        Case {
            name: "probe-no-args",
            tool: "probe",
            args: vec![],
            env: vec![],
        },
        Case {
            name: "probe-unknown",
            tool: "probe",
            args: vec!["nope".into()],
            env: vec![],
        },
        Case {
            name: "probe-no-token",
            tool: "probe",
            args: vec!["status".into()],
            env: vec![],
        },
        Case {
            name: "probe-chat-bad-flag",
            tool: "probe",
            args: vec!["chat".into(), "-bogus".into()],
            env: vec![token()],
        },
        Case {
            name: "probe-edge-no-case",
            tool: "probe",
            args: vec!["edge".into()],
            env: vec![token()],
        },
        Case {
            name: "probe-edge-unknown",
            tool: "probe",
            args: vec!["edge".into(), "nope".into()],
            env: vec![token()],
        },
        Case {
            name: "probe-assign-no-uid",
            tool: "probe",
            args: vec!["assign".into()],
            env: vec![token()],
        },
        Case {
            name: "probe-rerun-missing-file",
            tool: "probe",
            args: vec!["rerun".into(), "-file".into(), "nonexistent.json".into()],
            env: vec![token()],
        },
        Case {
            name: "probe-hist-bad-shape",
            tool: "probe",
            args: vec!["hist".into(), "-shape".into(), "nope".into()],
            env: vec![token()],
        },
        Case {
            name: "protoextract-no-args",
            tool: "protoextract",
            args: vec![],
            env: vec![],
        },
        Case {
            name: "protoextract-missing-binary",
            tool: "protoextract",
            args: vec!["/nonexistent/binary".into(), "out".into()],
            env: vec![],
        },
        Case {
            name: "protoextract-unsafe-out",
            tool: "protoextract",
            args: vec!["/nonexistent/binary".into(), "/".into()],
            env: vec![],
        },
        Case {
            name: "protocensus-no-args",
            tool: "protocensus",
            args: vec![],
            env: vec![],
        },
        Case {
            name: "protocensus-unknown",
            tool: "protocensus",
            args: vec!["nope".into()],
            env: vec![],
        },
        Case {
            name: "protocensus-diff-missing",
            tool: "protocensus",
            args: vec!["diff".into(), "nope.pb".into(), "nope2.pb".into()],
            env: vec![],
        },
        Case {
            name: "upstreamstub-bad-scenario",
            tool: "upstreamstub",
            args: vec![
                "-scenario".into(),
                "typo".into(),
                "-listen".into(),
                "127.0.0.1:0".into(),
            ],
            env: vec![],
        },
        Case {
            name: "loadtest-bad-flag",
            tool: "loadtest",
            args: vec!["-bogus".into()],
            env: vec![],
        },
    ];

    let mut diffs = Vec::new();
    let mut results = serde_json::Map::new();
    for case in &cases {
        let mut pair = serde_json::Map::new();
        let mut codes = [0i32; 2];
        for (idx, bins) in [go, rust].iter().enumerate() {
            let args: Vec<&str> = case.args.iter().map(String::as_str).collect();
            let env: Vec<(&str, &str)> = case
                .env
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            match run_scrubbed(&bins[case.tool], &args, &cwd, &env, Duration::from_secs(15)) {
                Ok(out) => {
                    codes[idx] = out.code;
                    pair.insert(
                        if idx == 0 { "go" } else { "rust" }.to_string(),
                        serde_json::json!({
                            "code": out.code,
                            "stdout": out.stdout,
                            "stderr": out.stderr,
                        }),
                    );
                }
                Err(e) => {
                    codes[idx] = -1;
                    pair.insert(
                        if idx == 0 { "go" } else { "rust" }.to_string(),
                        serde_json::json!({ "error": e.to_string() }),
                    );
                }
            }
        }
        if codes[0] != codes[1] {
            diffs.push(format!("{}: go={} rust={}", case.name, codes[0], codes[1]));
        }
        results.insert(case.name.to_string(), serde_json::Value::Object(pair));
    }
    let _ = std::fs::write(
        dir.join("malformed.json"),
        serde_json::to_string_pretty(&results).unwrap_or_default(),
    );
    if diffs.is_empty() {
        pass(
            "malformed",
            serde_json::json!({ "cases": cases.len(), "artifact": "malformed/malformed.json" }),
        )
    } else {
        fail(
            "malformed",
            serde_json::json!({ "exit_code_diffs": diffs, "artifact": "malformed/malformed.json" }),
        )
    }
}

// ---------- protoextract ----------------------------------------------------------

/// Two `FileDescriptorProtos` embedded in a synthetic binary: `a.proto`
/// (message Ping) and `b.proto` (imports a.proto, message Pong, enum E,
/// service S), plus a duplicate copy of `a.proto` to exercise dedup.
fn synthetic_binary() -> Vec<u8> {
    use buffa_descriptor::generated::descriptor as d;
    let file_a = d::FileDescriptorProto {
        name: Some("a.proto".to_string()),
        package: Some("qa".to_string()),
        syntax: Some("proto3".to_string()),
        message_type: vec![d::DescriptorProto {
            name: Some("Ping".to_string()),
            field: vec![d::FieldDescriptorProto {
                name: Some("text".to_string()),
                number: Some(1),
                label: Some(d::field_descriptor_proto::Label::LABEL_OPTIONAL),
                r#type: Some(d::field_descriptor_proto::Type::TYPE_STRING),
                json_name: Some("text".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let file_b = d::FileDescriptorProto {
        name: Some("b.proto".to_string()),
        package: Some("qa.b".to_string()),
        dependency: vec!["a.proto".to_string()],
        syntax: Some("proto3".to_string()),
        message_type: vec![d::DescriptorProto {
            name: Some("Pong".to_string()),
            field: vec![d::FieldDescriptorProto {
                name: Some("ping".to_string()),
                number: Some(1),
                label: Some(d::field_descriptor_proto::Label::LABEL_OPTIONAL),
                r#type: Some(d::field_descriptor_proto::Type::TYPE_MESSAGE),
                type_name: Some(".qa.Ping".to_string()),
                json_name: Some("ping".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        enum_type: vec![d::EnumDescriptorProto {
            name: Some("E".to_string()),
            value: vec![
                d::EnumValueDescriptorProto {
                    name: Some("E_UNSPECIFIED".to_string()),
                    number: Some(0),
                    ..Default::default()
                },
                d::EnumValueDescriptorProto {
                    name: Some("E_ON".to_string()),
                    number: Some(1),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
        service: vec![d::ServiceDescriptorProto {
            name: Some("S".to_string()),
            method: vec![d::MethodDescriptorProto {
                name: Some("M".to_string()),
                input_type: Some(".qa.Ping".to_string()),
                output_type: Some(".qa.b.Pong".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut bin = b"random-prefix-bytes".to_vec();
    bin.extend_from_slice(&file_a.encode_to_vec());
    bin.extend_from_slice(b"middle");
    bin.extend_from_slice(&file_b.encode_to_vec());
    bin.extend_from_slice(b"tail");
    bin.extend_from_slice(&file_a.encode_to_vec()); // duplicate → dedup
    bin
}

/// Tokenize an entire proto source while ignoring printer-only whitespace
/// and comments. Unlike the old declaration inventory, this retains every
/// semantic token: labels and types, nesting braces, enum numbers, oneof
/// membership, options, reserved declarations, extensions and RPC modifiers.
fn proto_tokens(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if bytes[i..].starts_with(b"//") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if bytes[i..].starts_with(b"/*") {
            i += 2;
            while i + 1 < bytes.len() && !bytes[i..].starts_with(b"*/") {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if bytes[i] == b'"' || bytes[i] == b'\'' {
            let quote = bytes[i];
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == quote {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            continue;
        }
        if bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            continue;
        }
        tokens.push(char::from(bytes[i]).to_string());
        i += 1;
    }
    tokens
}

fn protoextract_case(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    work: &Path,
) -> TargetResult {
    let name = "protoextract";
    let dir = work.join("protoextract");
    let _ = std::fs::create_dir_all(&dir);
    let binary = dir.join("fixture.bin");
    if let Err(e) = std::fs::write(&binary, synthetic_binary()) {
        return fail(name, serde_json::json!({ "setup": e.to_string() }));
    }
    let mut diffs = Vec::new();
    let mut outputs = serde_json::Map::new();
    for (label, bins) in [("go", go), ("rust", rust)] {
        let out_dir = dir.join(label);
        let out = match run_scrubbed(
            &bins["protoextract"],
            &[
                binary.to_str().unwrap_or_default(),
                out_dir.to_str().unwrap_or_default(),
            ],
            &dir,
            &[],
            Duration::from_secs(30),
        ) {
            Ok(o) => o,
            Err(e) => return fail(name, serde_json::json!({ label: e.to_string() })),
        };
        outputs.insert(
            label.to_string(),
            serde_json::json!({ "code": out.code, "stdout": out.stdout, "stderr": out.stderr }),
        );
        if out.code != 0 {
            diffs.push(format!("{label} exited {}", out.code));
        }
    }
    let go_out = dir.join("go");
    let rust_out = dir.join("rust");
    // descriptors.pb must be byte-identical.
    match (
        std::fs::read(go_out.join("descriptors.pb")),
        std::fs::read(rust_out.join("descriptors.pb")),
    ) {
        (Ok(g), Ok(r)) if g != r => diffs.push("descriptors.pb differs".to_string()),
        (Err(e), _) | (_, Err(e)) => diffs.push(format!("descriptors.pb: {e}")),
        _ => {}
    }
    // manifest.json must be equal (same input path, same counts).
    match (
        std::fs::read_to_string(go_out.join("manifest.json")),
        std::fs::read_to_string(rust_out.join("manifest.json")),
    ) {
        (Ok(g), Ok(r)) => {
            let gv: serde_json::Value = serde_json::from_str(&g).unwrap_or_default();
            let rv: serde_json::Value = serde_json::from_str(&r).unwrap_or_default();
            if gv != rv {
                diffs.push("manifest.json differs".to_string());
            }
        }
        _ => diffs.push("manifest.json missing".to_string()),
    }
    // Compare every semantic token in the flattened bundles. Comment wording
    // and whitespace are printer-specific, but no schema construct is omitted.
    match (
        std::fs::read_to_string(go_out.join("all-protos.proto")),
        std::fs::read_to_string(rust_out.join("all-protos.proto")),
    ) {
        (Ok(g), Ok(r)) => {
            let go_tokens = proto_tokens(&g);
            let rust_tokens = proto_tokens(&r);
            if go_tokens != rust_tokens {
                let first = go_tokens
                    .iter()
                    .zip(&rust_tokens)
                    .position(|(go, rust)| go != rust)
                    .unwrap_or_else(|| go_tokens.len().min(rust_tokens.len()));
                diffs.push(format!(
                    "flattened schema differs at token {first}: go={} rust={} tokens",
                    go_tokens.len(),
                    rust_tokens.len()
                ));
            }
        }
        _ => diffs.push("all-protos.proto missing".to_string()),
    }
    if diffs.is_empty() {
        pass(name, serde_json::json!({ "outputs": outputs }))
    } else {
        fail(
            name,
            serde_json::json!({ "diffs": diffs, "outputs": outputs }),
        )
    }
}

// ---------- protocensus ------------------------------------------------------------

// One census/diff driver; the fixture pipeline reads as a unit.
#[allow(clippy::too_many_lines)]
fn protocensus_case(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    work: &Path,
) -> TargetResult {
    let name = "protocensus";
    let dir = work.join("protocensus");
    let _ = std::fs::create_dir_all(&dir);
    // Fixture log tree: one clean request dir, one carrying an unknown
    // key and an out-of-range enum value.
    let logs = dir.join("logs");
    let d1 = logs.join("2026-09-19T10-00-00_abcd");
    let d2 = logs.join("2026-09-19T11-00-00_ef01");
    let setup = || -> std::io::Result<()> {
        std::fs::create_dir_all(&d1)?;
        std::fs::create_dir_all(&d2)?;
        std::fs::write(
            d1.join("03-devin-request.json"),
            r#"{"chatModelUid":"swe-2-max","chatMessagePrompts":[{"prompt":"hi"}]}"#,
        )?;
        std::fs::write(
            d1.join("04-devin-response.jsonl"),
            "{\"deltaText\":\"hi\"}\n{\"stopReason\":\"STOP_REASON_STOP_PATTERN\"}\n",
        )?;
        std::fs::write(
            d2.join("03-devin-request.json"),
            r#"{"chatModelUid":"swe-2-max","requestType":99,"bogusField":1}"#,
        )?;
        std::fs::write(
            d2.join("04-devin-response.jsonl"),
            "{\"deltaText\":\"x\",\"mystery\":true}\n",
        )?;
        Ok(())
    };
    if let Err(e) = setup() {
        return fail(name, serde_json::json!({ "setup": e.to_string() }));
    }
    // Descriptor sets for diff come from each impl's own extractor —
    // cross-impl diff must be empty.
    let binary = dir.join("fixture.bin");
    if let Err(e) = std::fs::write(&binary, synthetic_binary()) {
        return fail(name, serde_json::json!({ "setup": e.to_string() }));
    }
    let mut diffs = Vec::new();
    let mut outputs = serde_json::Map::new();
    for (label, bins) in [("go", go), ("rust", rust)] {
        let out_dir = dir.join(format!("extract-{label}"));
        let _ = run_scrubbed(
            &bins["protoextract"],
            &[
                binary.to_str().unwrap_or_default(),
                out_dir.to_str().unwrap_or_default(),
            ],
            &dir,
            &[],
            Duration::from_secs(30),
        );
        let census = run_scrubbed(
            &bins["protocensus"],
            &["census", "-logs", logs.to_str().unwrap_or_default()],
            &dir,
            &[],
            Duration::from_secs(30),
        );
        let self_diff = run_scrubbed(
            &bins["protocensus"],
            &[
                "diff",
                out_dir.join("descriptors.pb").to_str().unwrap_or_default(),
                out_dir.join("descriptors.pb").to_str().unwrap_or_default(),
            ],
            &dir,
            &[],
            Duration::from_secs(30),
        );
        outputs.insert(
            label.to_string(),
            serde_json::json!({
                "census": census.map(|o| serde_json::json!({"code": o.code, "stdout": o.stdout, "stderr": o.stderr})).unwrap_or_default(),
                "self_diff": self_diff.map(|o| serde_json::json!({"code": o.code, "stdout": o.stdout})).unwrap_or_default(),
            }),
        );
    }
    let go_census = outputs["go"]["census"]["stdout"]
        .as_str()
        .unwrap_or_default();
    let rust_census = outputs["rust"]["census"]["stdout"]
        .as_str()
        .unwrap_or_default();
    let go_census_json = serde_json::from_str::<serde_json::Value>(go_census);
    let rust_census_json = serde_json::from_str::<serde_json::Value>(rust_census);
    match (go_census_json, rust_census_json) {
        (Ok(go_value), Ok(rust_value)) if go_value == rust_value => {}
        _ => diffs.push("census JSON differs".to_string()),
    }
    if outputs["go"]["census"]["code"] != outputs["rust"]["census"]["code"] {
        diffs.push("census exit code differs".to_string());
    }
    // Cross-impl diff: go-extracted vs rust-extracted must be empty.
    let cross = run_scrubbed(
        &rust["protocensus"],
        &[
            "diff",
            dir.join("extract-go/descriptors.pb")
                .to_str()
                .unwrap_or_default(),
            dir.join("extract-rust/descriptors.pb")
                .to_str()
                .unwrap_or_default(),
        ],
        &dir,
        &[],
        Duration::from_secs(30),
    );
    match cross {
        Ok(o) => {
            let v: serde_json::Value = serde_json::from_str(&o.stdout).unwrap_or_default();
            let empty = v["added"].as_array().is_some_and(std::vec::Vec::is_empty)
                && v["removed"].as_array().is_some_and(std::vec::Vec::is_empty)
                && v["changed"].as_array().is_some_and(std::vec::Vec::is_empty);
            if !empty {
                diffs.push(format!("cross-impl diff non-empty: {}", o.stdout));
            }
        }
        Err(e) => diffs.push(format!("cross diff: {e}")),
    }
    if diffs.is_empty() {
        pass(name, serde_json::json!({ "outputs": outputs }))
    } else {
        fail(
            name,
            serde_json::json!({ "diffs": diffs, "outputs": outputs }),
        )
    }
}

// ---------- upstreamstub -------------------------------------------------------------

fn complete_envelope_count(bytes: &[u8]) -> usize {
    let mut count = 0usize;
    let mut offset = 0usize;
    while offset + 5 <= bytes.len() {
        let len =
            u32::from_be_bytes(bytes[offset + 1..offset + 5].try_into().unwrap_or([0; 4])) as usize;
        if offset + 5 + len > bytes.len() {
            break;
        }
        count += 1;
        offset += 5 + len;
    }
    count
}

fn dechunk_response(
    stream: &mut TcpStream,
    mut raw: Vec<u8>,
    max_frames: usize,
) -> std::io::Result<Vec<u8>> {
    let mut decoded = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let line_end = loop {
            if let Some(pos) = raw.windows(2).position(|w| w == b"\r\n") {
                break pos;
            }
            match stream.read(&mut chunk) {
                Ok(0) => return Ok(decoded),
                Ok(n) => raw.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(decoded);
                }
                Err(e) => return Err(e),
            }
        };
        let size_text = String::from_utf8_lossy(&raw[..line_end]);
        let size = usize::from_str_radix(size_text.split(';').next().unwrap_or("").trim(), 16)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad chunk size"))?;
        let needed = line_end + 2 + size + 2;
        while raw.len() < needed {
            match stream.read(&mut chunk) {
                Ok(0) => return Ok(decoded),
                Ok(n) => raw.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(decoded);
                }
                Err(e) => return Err(e),
            }
        }
        raw.drain(..line_end + 2);
        if size == 0 {
            return Ok(decoded);
        }
        decoded.extend(raw.drain(..size));
        raw.drain(..2);
        if complete_envelope_count(&decoded) >= max_frames {
            return Ok(decoded);
        }
    }
}

/// Read a Connect streaming response into (headers, frames, trailing
/// bytes). Frames are (flag, payload); trailing bytes are whatever
/// followed the last complete envelope. `max_frames` bounds the read so
/// hang scenarios return after their prefix.
/// (head, frames, trailers) decoded from one Connect stream response.
type StreamResponse = (String, Vec<(u8, Vec<u8>)>, Vec<u8>);

fn read_stream_response(
    stream: &mut TcpStream,
    max_frames: usize,
    read_timeout: Duration,
) -> std::io::Result<StreamResponse> {
    stream.set_read_timeout(Some(read_timeout))?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        match stream.read(&mut chunk) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            }
            Err(e) => return Err(e),
        }
    };
    let headers = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut rest = buf.split_off(head_end);
    let chunked = headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked");
    if chunked {
        rest = dechunk_response(stream, rest, max_frames)?;
    }
    let mut frames = Vec::new();
    loop {
        if frames.len() >= max_frames {
            return Ok((headers, frames, rest));
        }
        while rest.len() < 5 {
            if chunked {
                return Ok((headers, frames, rest));
            }
            match stream.read(&mut chunk) {
                Ok(0) => return Ok((headers, frames, rest)),
                Ok(n) => rest.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Ok((headers, frames, rest));
                }
                Err(e) => return Err(e),
            }
        }
        let flag = rest[0];
        let len = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]) as usize;
        while rest.len() < 5 + len {
            if chunked {
                return Ok((headers, frames, rest));
            }
            match stream.read(&mut chunk) {
                Ok(0) => return Ok((headers, frames, rest)),
                Ok(n) => rest.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    return Ok((headers, frames, rest));
                }
                Err(e) => return Err(e),
            }
        }
        let payload: Vec<u8> = rest.drain(..5 + len).skip(5).collect();
        frames.push((flag, payload));
    }
}

/// Send one Connect streaming request to a stub and read the response.
fn stub_request(
    addr: &str,
    max_frames: usize,
    read_timeout: Duration,
) -> std::io::Result<StreamResponse> {
    let mut stream = TcpStream::connect(addr)?;
    let msg = pb::GetChatMessageRequest::default().encode_to_vec();
    let mut body = vec![0u8];
    body.extend_from_slice(&u32::try_from(msg.len()).unwrap_or(u32::MAX).to_be_bytes());
    body.extend_from_slice(&msg);
    stream.write_all(
        format!(
            "POST {CHAT_PATH} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/connect+proto\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(&body)?;
    read_stream_response(&mut stream, max_frames, read_timeout)
}

/// Decode a response into a comparable shape: frame flags + protojson
/// bodies with volatile fields normalized, plus trailing raw bytes.
fn comparable_stream(frames: &[(u8, Vec<u8>)], trailing: &[u8]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = frames
        .iter()
        .map(|(flag, payload)| {
            if *flag == 0x02 {
                let v: serde_json::Value = serde_json::from_slice(payload).unwrap_or_default();
                serde_json::json!({ "flag": flag, "end": normalize_json(&v) })
            } else {
                match pb::GetChatMessageResponse::decode(&mut payload.as_slice()) {
                    Ok(msg) => {
                        let v = serde_json::to_value(&msg).unwrap_or_default();
                        serde_json::json!({ "flag": flag, "msg": normalize_json(&v) })
                    }
                    Err(_) => serde_json::json!({ "flag": flag, "raw": payload }),
                }
            }
        })
        .collect();
    serde_json::json!({ "frames": items, "trailing": trailing })
}

/// Spawn a stub on a fresh port, wait for it to accept, run `body`,
/// then kill it.
fn with_stub<T>(
    bin: &Path,
    args: &[&str],
    cwd: &Path,
    body: impl FnOnce(&str) -> T,
) -> anyhow::Result<T> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let mut full_args: Vec<String> = args.iter().map(std::string::ToString::to_string).collect();
    full_args.push("-listen".to_string());
    full_args.push(format!("127.0.0.1:{port}"));
    let rust_target = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
    let traced = !bin.starts_with(&rust_target);
    let mut command = Command::new("/usr/bin/setsid");
    if traced {
        command
            .args(["/usr/bin/strace", "-f", "-e", "trace=bind,listen", "--"])
            .arg(bin);
    } else {
        command.arg(bin);
    }
    let mut child = command
        .args(&full_args)
        .current_dir(cwd)
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("stub stderr was not piped"))?;
    let (ready_tx, ready_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut lines = Vec::new();
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let ready = if traced {
                line.contains("listen(") && line.trim_end().ends_with("= 0")
            } else {
                line.contains("listening on")
            };
            lines.push(line);
            if ready {
                let _ = ready_tx.send(());
            }
        }
        lines
    });
    let addr = format!("127.0.0.1:{port}");
    if ready_rx.recv_timeout(Duration::from_secs(10)).is_err() {
        let _ = Command::new("/bin/kill")
            .args(["-KILL", "--", &format!("-{}", child.id())])
            .status();
        let _ = child.wait();
        let _ = reader.join();
        anyhow::bail!("stub {} never signaled readiness", bin.display());
    }
    let result = body(&addr);
    let _ = Command::new("/bin/kill")
        .args(["-KILL", "--", &format!("-{}", child.id())])
        .status();
    let _ = child.wait();
    let _ = reader.join();
    Ok(result)
}

// One scenario table per upstreamstub mode; splitting scatters the
// flag-parity matrix.
#[allow(clippy::too_many_lines)]
fn upstreamstub_cases(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    work: &Path,
) -> Vec<TargetResult> {
    let dir = work.join("upstreamstub");
    let _ = std::fs::create_dir_all(&dir);
    let mut out = Vec::new();

    // Buffered scenarios: read to EOF, compare decoded streams.
    for scenario in [
        "precontent",
        "midcontent",
        "cleaneof",
        "cleaneof-content",
        "bare-end",
        "endstream-error",
        "badframe",
        "badflags",
    ] {
        let name = format!("upstreamstub-{scenario}");
        let mut meta = serde_json::Map::new();
        let mut comparable = Vec::new();
        for (label, bins) in [("go", go), ("rust", rust)] {
            let res = with_stub(
                &bins["upstreamstub"],
                &["-scenario", scenario],
                &dir,
                |addr| stub_request(addr, 64, Duration::from_secs(5)),
            );
            match res {
                Ok(Ok((headers, frames, trailing))) => {
                    meta.insert(
                        label.to_string(),
                        serde_json::json!({
                            "status": headers.lines().next().unwrap_or(""),
                            "frames": frames.len(),
                            "trailing": trailing.len(),
                        }),
                    );
                    comparable.push(comparable_stream(&frames, &trailing));
                }
                other => {
                    meta.insert(
                        label.to_string(),
                        serde_json::json!({ "error": format!("{other:?}") }),
                    );
                    comparable.push(serde_json::Value::Null);
                }
            }
        }
        if comparable[0] == comparable[1] && !comparable[0].is_null() {
            out.push(pass(&name, serde_json::Value::Object(meta)));
        } else {
            out.push(fail(
                &name,
                serde_json::json!({ "go": comparable[0], "rust": comparable[1], "meta": meta }),
            ));
        }
    }

    // stream: full deterministic stream, compare decoded frames.
    {
        let name = "upstreamstub-stream";
        let mut comparable = Vec::new();
        for bins in [go, rust] {
            let res = with_stub(
                &bins["upstreamstub"],
                &["-scenario", "stream", "-deltas", "5", "-delta-bytes", "4"],
                &dir,
                |addr| stub_request(addr, 64, Duration::from_secs(5)),
            );
            comparable.push(match res {
                Ok(Ok((_, frames, trailing))) => comparable_stream(&frames, &trailing),
                other => serde_json::json!({ "error": format!("{other:?}") }),
            });
        }
        // 1 meta + 5 deltas + 1 stop + 1 endstream = 8 frames.
        let ok = comparable[0] == comparable[1]
            && comparable[0]["frames"]
                .as_array()
                .is_some_and(|f| f.len() == 8);
        if ok {
            out.push(pass(name, serde_json::json!({ "frames": 8 })));
        } else {
            out.push(fail(
                name,
                serde_json::json!({ "go": comparable[0], "rust": comparable[1] }),
            ));
        }
    }

    // recover: first request truncated, second complete.
    {
        let name = "upstreamstub-recover";
        let mut comparable = Vec::new();
        for bins in [go, rust] {
            let res = with_stub(
                &bins["upstreamstub"],
                &["-scenario", "recover", "-recover-after", "1"],
                &dir,
                |addr| {
                    let first = stub_request(addr, 64, Duration::from_secs(5));
                    let second = stub_request(addr, 64, Duration::from_secs(5));
                    (first, second)
                },
            );
            comparable.push(match res {
                Ok((Ok((_, f1, t1)), Ok((_, f2, t2)))) => serde_json::json!({
                    "first": comparable_stream(&f1, &t1),
                    "second": comparable_stream(&f2, &t2),
                }),
                other => serde_json::json!({ "error": format!("{other:?}") }),
            });
        }
        let ok = comparable[0] == comparable[1]
            && comparable[0]["first"]["trailing"]
                .as_array()
                .is_some_and(|t| t.len() == 2)
            && comparable[0]["second"]["frames"]
                .as_array()
                .is_some_and(|f| f.len() == 4);
        if ok {
            out.push(pass(name, serde_json::json!({})));
        } else {
            out.push(fail(
                name,
                serde_json::json!({ "go": comparable[0], "rust": comparable[1] }),
            ));
        }
    }

    // Hang scenarios: verify the prefix arrives and the connection holds.
    for (scenario, expect_frames) in [("stall", 0usize), ("end-hang", 4), ("heartbeat", 2)] {
        let name = format!("upstreamstub-{scenario}");
        let mut comparable = Vec::new();
        for bins in [go, rust] {
            let res = with_stub(
                &bins["upstreamstub"],
                &["-scenario", scenario],
                &dir,
                |addr| stub_request(addr, expect_frames.max(1), Duration::from_secs(5)),
            );
            comparable.push(match res {
                Ok(Ok((headers, frames, _))) => serde_json::json!({
                    "status": headers.lines().next().unwrap_or(""),
                    "frames": frames.len(),
                }),
                other => serde_json::json!({ "error": format!("{other:?}") }),
            });
        }
        let ok = comparable[0] == comparable[1]
            && comparable[0]["frames"].as_u64().unwrap_or(999) == expect_frames as u64;
        if ok {
            out.push(pass(&name, comparable[0].clone()));
        } else {
            out.push(fail(
                &name,
                serde_json::json!({ "go": comparable[0], "rust": comparable[1] }),
            ));
        }
    }
    out
}

// ---------- loadtest ------------------------------------------------------------------

/// A fixture HTTP server: serves `body` once per connection, forever.
fn serve_fixture(body: &'static str, content_type: &'static str) -> (String, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
    let addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            if flag.load(Ordering::SeqCst) {
                return;
            }
            let Ok(mut stream) = conn else { continue };
            std::thread::spawn(move || {
                let mut req = [0u8; 8192];
                let _ = stream.read(&mut req);
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            });
        }
    });
    (addr, stop)
}

// One scenario table per loadtest mode; splitting scatters the
// flag-parity matrix.
#[allow(clippy::too_many_lines)]
fn loadtest_cases(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    work: &Path,
) -> Vec<TargetResult> {
    let dir = work.join("loadtest");
    let _ = std::fs::create_dir_all(&dir);
    let mut out = Vec::new();
    let fixtures: [(&str, &str, &str); 2] = [
        (
            "sse",
            "data: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"pong\"}}]}\n\ndata: [DONE]\n\n",
            "text/event-stream",
        ),
        (
            "json",
            "{\"choices\":[{\"message\":{\"content\":\"pong\"}}]}",
            "application/json",
        ),
    ];
    for (label, body, content_type) in fixtures {
        let name = format!("loadtest-{label}");
        let (addr, stop) = serve_fixture(body, content_type);
        let url = format!("http://{addr}/v1/chat/completions");
        let mut summaries = Vec::new();
        let mut raw = serde_json::Map::new();
        for (tag, bins) in [("go", go), ("rust", rust)] {
            let res = run_scrubbed(
                &bins["loadtest"],
                &["-url", &url, "-c", "2", "-n", "4"],
                &dir,
                &[],
                Duration::from_secs(30),
            );
            match res {
                Ok(o) => {
                    raw.insert(
                        tag.to_string(),
                        serde_json::json!({ "code": o.code, "stdout": o.stdout, "stderr": o.stderr }),
                    );
                    summaries.push((o.code, o.stdout));
                }
                Err(e) => {
                    raw.insert(
                        tag.to_string(),
                        serde_json::json!({ "error": e.to_string() }),
                    );
                    summaries.push((-1, String::new()));
                }
            }
        }
        stop.store(true, Ordering::SeqCst);
        let mut diffs = Vec::new();
        if summaries[0].0 != summaries[1].0 {
            diffs.push(format!(
                "exit: go={} rust={}",
                summaries[0].0, summaries[1].0
            ));
        }
        let counts = |s: &str| {
            let mut m = BTreeMap::new();
            for tok in s.lines().next().unwrap_or("").split_whitespace() {
                if let Some((k, v)) = tok.split_once('=') {
                    m.insert(k.to_string(), v.to_string());
                }
            }
            m
        };
        let (gc, rc) = (counts(&summaries[0].1), counts(&summaries[1].1));
        for key in ["requests", "ok", "errors"] {
            if gc.get(key) != rc.get(key) {
                diffs.push(format!(
                    "{key}: go={:?} rust={:?}",
                    gc.get(key),
                    rc.get(key)
                ));
            }
        }
        let metric_names = |s: &str| -> Vec<String> {
            s.lines()
                .filter_map(|l| l.split_whitespace().next().map(String::from))
                .filter(|l| l.ends_with("_ms"))
                .collect()
        };
        let go_metrics = metric_names(&summaries[0].1);
        let rust_metrics = metric_names(&summaries[1].1);
        for m in &go_metrics {
            if !rust_metrics.contains(m) {
                diffs.push(format!("missing metric {m}"));
            }
        }
        if !rust_metrics.contains(&"wire_first_byte_ms".to_string())
            || !rust_metrics.contains(&"semantic_first_content_ms".to_string())
        {
            diffs.push("rust missing new metrics".to_string());
        }
        if diffs.is_empty() {
            out.push(pass(&name, serde_json::json!({ "metrics": rust_metrics })));
        } else {
            out.push(fail(
                &name,
                serde_json::json!({ "diffs": diffs, "raw": raw }),
            ));
        }
    }
    out
}

// ---------- probe ----------------------------------------------------------------------

/// A recorded request: path, content type, raw body.
struct Recorded {
    path: String,
    content_type: String,
    body: Vec<u8>,
}

/// The loopback Connect server: answers every `ApiServerService` RPC with
/// a canned response and records each request for comparison.
struct Loopback {
    addr: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    stop: Arc<AtomicBool>,
}

impl Loopback {
    fn start() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let reqs = Arc::clone(&requests);
        let flag = Arc::clone(&stop);
        std::thread::spawn(move || {
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let reqs = Arc::clone(&reqs);
                        std::thread::spawn(move || {
                            let _ = serve_connect(&mut stream, &reqs);
                        });
                    }
                    Err(_) if flag.load(Ordering::SeqCst) => return,
                    Err(_) => {}
                }
            }
        });
        Ok(Self {
            addr,
            requests,
            stop,
        })
    }

    fn take(&self) -> Vec<Recorded> {
        std::mem::take(
            &mut self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the blocking accept so the thread exits promptly.
        let _ = TcpStream::connect(&self.addr);
    }
}

fn envelope(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(flag);
    out.extend_from_slice(
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    out.extend_from_slice(payload);
    out
}

/// The canned chat stream: meta → thinking → signature → text → stop →
/// endstream, with fixed ids so both implementations see identical
/// bytes.
fn canned_chat_stream(json_wire: bool) -> Vec<u8> {
    let frame = |msg: &pb::GetChatMessageResponse| {
        let payload = if json_wire {
            serde_json::to_vec(msg).unwrap_or_default()
        } else {
            msg.encode_to_vec()
        };
        envelope(0x00, &payload)
    };
    let mut out = Vec::new();
    out.extend_from_slice(&frame(&pb::GetChatMessageResponse {
        message_id: Some("bot-qa".to_string()),
        request_id: Some("qa-req".to_string()),
        output_id: Some("qa-output".to_string()),
        thinking_id: Some("qa-thinking".to_string()),
        phase: Some("qa-phase".to_string()),
        usage: MessageField::some(pb::ExaCodeiumCommonPb_ModelUsageStats {
            model_uid: Some("swe-2-max".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }));
    out.extend_from_slice(&frame(&pb::GetChatMessageResponse {
        delta_thinking: Some("qa thinking".to_string()),
        ..Default::default()
    }));
    out.extend_from_slice(&frame(&pb::GetChatMessageResponse {
        delta_signature: Some("qa-sig".to_string()),
        delta_signature_type: Some("qa-sig-type".to_string()),
        ..Default::default()
    }));
    out.extend_from_slice(&frame(&pb::GetChatMessageResponse {
        delta_text: Some("pong".to_string()),
        ..Default::default()
    }));
    out.extend_from_slice(&frame(&pb::GetChatMessageResponse {
        stop_reason: Some(pb::ExaCodeiumCommonPb_StopReason::ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN),
        ..Default::default()
    }));
    out.extend_from_slice(&envelope(0x02, b"{}"));
    out
}

/// Read one request, record it, write the canned response for its path.
// One request/response exchange per connection; the canned-reply
// table reads as a unit.
#[allow(clippy::too_many_lines)]
fn serve_connect(
    stream: &mut TcpStream,
    requests: &Arc<Mutex<Vec<Recorded>>>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 1 << 20 {
            return Ok(());
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let path = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .unwrap_or("")
        .to_string();
    let mut content_length = 0usize;
    let mut content_type = String::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => content_length = value.trim().parse().unwrap_or(0),
            "content-type" => content_type = value.trim().to_string(),
            _ => {}
        }
    }
    let mut rest = buf.split_off(head_end);
    while rest.len() < content_length {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        rest.extend_from_slice(&chunk[..n]);
    }
    let body: Vec<u8> = rest.drain(..content_length.min(rest.len())).collect();
    if !known_probe_path(&path) {
        stream.write_all(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("unexpected probe RPC path {path}"),
        ));
    }
    requests
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(Recorded {
            path: path.clone(),
            content_type: content_type.clone(),
            body,
        });

    let json_wire = content_type.contains("json");
    if path == CHAT_PATH || path == EXTCHAT_PATH {
        let ct = if json_wire {
            "application/connect+json"
        } else {
            "application/connect+proto"
        };
        stream.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nX-Stub: qa\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )?;
        let stream_body = if path == CHAT_PATH {
            canned_chat_stream(json_wire)
        } else {
            let f = envelope(0x00, b"{}");
            let mut b = f.clone();
            b.extend_from_slice(&f);
            b.extend_from_slice(&envelope(0x02, b"{}"));
            b
        };
        stream.write_all(&stream_body)?;
        return Ok(());
    }
    // Unary RPC: bare message body.
    let (ct, payload) = if path == ASSIGN_PATH {
        let msg = pb::AssignModelResponse {
            assignment: MessageField::some(pb::ModelAssignment {
                model_uid: Some("swe-2-max".to_string()),
                assignment_jwt: Some("qa-jwt".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        if json_wire {
            (
                "application/json",
                serde_json::to_vec(&msg).unwrap_or_default(),
            )
        } else {
            ("application/proto", msg.encode_to_vec())
        }
    } else if json_wire {
        ("application/json", b"{}".to_vec())
    } else {
        ("application/proto", Vec::new())
    };
    stream.write_all(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {ct}\r\nX-Stub: qa\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            payload.len()
        )
        .as_bytes(),
    )?;
    stream.write_all(&payload)
}

/// Decode a recorded request body into comparable protojson.
fn decode_request(rec: &Recorded) -> serde_json::Value {
    let json_wire = rec.content_type.contains("json");
    // Streaming requests carry one envelope; unary carry a bare message.
    let payload: &[u8] = if rec.path == CHAT_PATH || rec.path == EXTCHAT_PATH {
        if rec.body.len() >= 5 {
            let len =
                u32::from_be_bytes([rec.body[1], rec.body[2], rec.body[3], rec.body[4]]) as usize;
            &rec.body[5..5 + len.min(rec.body.len() - 5)]
        } else {
            &rec.body
        }
    } else {
        &rec.body
    };
    macro_rules! decode {
        ($ty:ty) => {
            if json_wire {
                serde_json::from_slice::<serde_json::Value>(payload).unwrap_or_default()
            } else {
                <$ty>::decode(&mut { payload })
                    .map(|m| serde_json::to_value(&m).unwrap_or_default())
                    .unwrap_or_default()
            }
        };
    }
    let value = match rec.path.as_str() {
        CHAT_PATH => decode!(pb::GetChatMessageRequest),
        ASSIGN_PATH => decode!(pb::AssignModelRequest),
        "/exa.api_server_pb.ApiServerService/CheckChatCapacity" => {
            decode!(pb::CheckChatCapacityRequest)
        }
        "/exa.api_server_pb.ApiServerService/CheckUserMessageRateLimit" => {
            decode!(pb::CheckUserMessageRateLimitRequest)
        }
        "/exa.api_server_pb.ApiServerService/GetModelStatuses" => {
            decode!(pb::GetModelStatusesRequest)
        }
        "/exa.api_server_pb.ApiServerService/GetModelProviders" => {
            decode!(pb::GetModelProvidersRequest)
        }
        "/exa.api_server_pb.ApiServerService/GetCliModelConfigs" => {
            decode!(pb::GetCliModelConfigsRequest)
        }
        "/exa.api_server_pb.ApiServerService/GetCommandModelConfigs" => {
            decode!(pb::GetCommandModelConfigsRequest)
        }
        "/exa.api_server_pb.ApiServerService/GetStatus" => decode!(pb::GetStatusRequest),
        "/exa.api_server_pb.ApiServerService/GetConfig" => decode!(pb::GetConfigRequest),
        "/exa.api_server_pb.ApiServerService/GetEmbeddings" => {
            decode!(pb::GetEmbeddingsRequest)
        }
        EXTCHAT_PATH => decode!(pb::GetChatCompletionsRequest),
        _ => serde_json::Value::Null,
    };
    serde_json::json!({
        "path": rec.path,
        "body": normalize_json(&value),
    })
}

/// Replace volatile field values (ids, keys, timestamps) with a fixed
/// placeholder so Go/Rust requests compare semantically.
fn normalize_json(value: &serde_json::Value) -> serde_json::Value {
    const VOLATILE: &[&str] = &[
        "apiKey",
        "f",
        "messageId",
        "trajectoryId",
        "cascadeId",
        "executionId",
        "sessionId",
        "deviceFingerprint",
        "timestamp",
        "modelAssignmentJwt",
    ];
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, v) in map {
                if VOLATILE.contains(&k.as_str()) {
                    out.insert(k.clone(), serde_json::json!("<v>"));
                } else {
                    out.insert(k.clone(), normalize_json(v));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(normalize_json).collect())
        }
        other => other.clone(),
    }
}

/// Normalize volatile tokens in stdout text: UUIDs, fingerprints, the
/// qa token and jwt lengths.
fn normalize_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // UUID: 8-4-4-4-12 hex with dashes.
        if i + 36 <= bytes.len() && is_uuid(&bytes[i..i + 36]) {
            out.push_str("<uuid>");
            i += 36;
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    for key in ["apiKey", "deviceFingerprint", "modelAssignmentJwt"] {
        let pat = format!("\"{key}\":\"");
        let mut search_from = 0usize;
        while let Some(relative) = out[search_from..].find(&pat) {
            let start = search_from + relative;
            let val_start = start + pat.len();
            let Some(end) = out[val_start..].find('"') else {
                break;
            };
            let replacement = format!("\"{key}\":\"<v>\"");
            out.replace_range(start..=(val_start + end), &replacement);
            search_from = start + replacement.len();
        }
    }
    // jwt_len=<digits>
    let mut cleaned = String::with_capacity(out.len());
    let mut rest = out.as_str();
    while let Some(pos) = rest.find("jwt_len=") {
        cleaned.push_str(&rest[..pos + 8]);
        let tail = &rest[pos + 8..];
        let digits = tail.len() - tail.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        cleaned.push_str("<n>");
        rest = &tail[digits..];
    }
    cleaned.push_str(rest);
    cleaned
}

/// Preserve user-facing probe meaning while removing transport/library
/// formatting that differs between connect-go and connectrpc. Typed request
/// bodies are compared independently and exactly below.
fn comparable_probe_output(text: &str) -> String {
    normalize_text(text)
        .lines()
        .filter_map(|line| {
            if line.starts_with("== request:")
                || line.starts_with("== headers:")
                || line.starts_with("== trailers:")
                || line.starts_with("extchat stream err:")
            {
                None
            } else if line.starts_with("== usage:") {
                Some("== usage:".to_string())
            } else {
                Some(line.to_string())
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_uuid(b: &[u8]) -> bool {
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                *c == b'-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}

/// One probe invocation against a fresh loopback; returns (exit code,
/// normalized stdout, decoded requests, stderr).
fn probe_run(
    bin: &Path,
    args: &[&str],
    cwd: &Path,
) -> anyhow::Result<(i32, String, Vec<serde_json::Value>, String)> {
    let server = Loopback::start()?;
    // Point devin.base_url at the loopback via DEVIN2API_CONFIG (the
    // scrubbed env already points at cwd/config.yaml).
    std::fs::write(
        cwd.join("config.yaml"),
        format!(
            "server:\n  listen: 127.0.0.1:1\ndevin:\n  base_url: \"http://{}\"\n",
            server.addr
        ),
    )?;
    let out = run_scrubbed(
        bin,
        args,
        cwd,
        &[("DEVIN_TOKEN", "qa-token")],
        Duration::from_secs(30),
    )?;
    let recorded = server.take();
    let decoded = recorded.iter().map(decode_request).collect();
    Ok((
        out.code,
        comparable_probe_output(&out.stdout),
        decoded,
        out.stderr,
    ))
}

// One case table per probe subcommand; splitting scatters the
// flag-parity matrix.
#[allow(clippy::too_many_lines)]
fn probe_cases(
    rust: &BTreeMap<String, PathBuf>,
    go: &BTreeMap<String, PathBuf>,
    work: &Path,
) -> Vec<TargetResult> {
    let dir = work.join("probe");
    let _ = std::fs::create_dir_all(&dir);
    let cwd = dir.join("cwd");
    let _ = std::fs::create_dir_all(cwd.join("home"));
    let _ = std::fs::create_dir_all(cwd.join("xdg"));

    // A captured request for `rerun`: fixed protojson both sides replay.
    let rerun_file = dir.join("captured.json");
    let _ = std::fs::write(
        &rerun_file,
        r#"{"metadata":{"apiKey":"x"},"chatModelUid":"swe-2-max","chatMessagePrompts":[{"source":"ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_USER","prompt":"hi"}]}"#,
    );

    let mut all_cases: Vec<(String, Vec<String>)> = vec![
        ("status".to_string(), vec!["status".into()]),
        (
            "assign".to_string(),
            vec!["assign".into(), "swe-2-max".into(), "other".into()],
        ),
        ("misc".to_string(), vec!["misc".into()]),
        ("chat-default".to_string(), vec!["chat".into()]),
        (
            "chat-flags".to_string(),
            vec![
                "chat".into(),
                "-frames".into(),
                "-system-as-message".into(),
                "-tool".into(),
                "a".into(),
                "-tool".into(),
                "b".into(),
                "-tool-schema".into(),
                r#"{"type":"object"}"#.into(),
                "-tool-choice".into(),
                "opt:auto".into(),
                "-disable-parallel".into(),
                "-provider-source".into(),
                "PROVIDER_SOURCE_CASCADE".into(),
                "-prompt-id".into(),
                "pid".into(),
                "-num-tokens".into(),
                "7".into(),
                "-planner-mode".into(),
                "CONVERSATIONAL_PLANNER_MODE_PLANNING".into(),
                "-step-type".into(),
                "CORTEX_STEP_TYPE_USER_INPUT".into(),
                "-step-index".into(),
                "3".into(),
                "-request-type".into(),
                "CHAT_MESSAGE_REQUEST_TYPE_CASCADE".into(),
                "-language".into(),
                "LANGUAGE_TYPESCRIPT".into(),
                "-chat-model-name".into(),
                "x".into(),
                "-no-fingerprint".into(),
                "-trajectory-id".into(),
                "t1".into(),
                "-cascade-id".into(),
                "c1".into(),
                "-max-tokens".into(),
                "99".into(),
                "-num-completions".into(),
                "2".into(),
                "-stop-pattern".into(),
                "END".into(),
                "-temperature".into(),
                "0.5".into(),
                "-top-p".into(),
                "0.9".into(),
                "-top-k".into(),
                "5".into(),
                "-images".into(),
                "2".into(),
                "-meta-extras".into(),
            ],
        ),
        (
            "chat-resolve".to_string(),
            vec![
                "chat".into(),
                "-resolve".into(),
                "-router".into(),
                "swe-2-max".into(),
            ],
        ),
        (
            "chat-resolve-only".to_string(),
            vec!["chat".into(), "-resolve-only".into()],
        ),
        (
            "chat-misc-flags".to_string(),
            vec![
                "chat".into(),
                "-system-empty".into(),
                "-raw-schema".into(),
                "-tool".into(),
                "x".into(),
                "-custom-tool".into(),
                "apply".into(),
                "-tool-extras".into(),
                "-no-ids".into(),
            ],
        ),
        (
            "replay-with-sig".to_string(),
            vec!["replay".into(), "-variant".into(), "with-sig".into()],
        ),
        (
            "replay-bogus-typed".to_string(),
            vec!["replay".into(), "-variant".into(), "bogus-sig-typed".into()],
        ),
        (
            "hist-merged".to_string(),
            vec!["hist".into(), "-shape".into(), "merged".into()],
        ),
        (
            "hist-split".to_string(),
            vec!["hist".into(), "-shape".into(), "split".into()],
        ),
        (
            "hist-merged-single".to_string(),
            vec!["hist".into(), "-shape".into(), "merged-single".into()],
        ),
        (
            "hist-split-single".to_string(),
            vec!["hist".into(), "-shape".into(), "split-single".into()],
        ),
        (
            "bigctx".to_string(),
            vec!["bigctx".into(), "-kb".into(), "4".into()],
        ),
        (
            "rerun".to_string(),
            vec![
                "rerun".into(),
                "-file".into(),
                rerun_file.to_str().unwrap_or_default().to_string(),
                "-n".into(),
                "2".into(),
            ],
        ),
    ];
    // Every edge case (flags must precede the positional case name).
    for case in [
        "orphan-tool-result",
        "unknown-source",
        "dup-message-id",
        "empty-user-prompt",
        "empty-assistant",
        "experiment",
        "trailing-assistant",
        "trailing-tool-result",
        "thinking-only-assistant",
        "thinking-empty-sig",
        "interleaved-calls",
        "grouped-calls-results",
        "trailing-call-no-result",
        "dup-tool-result",
        "tool-result-mismatch-call",
        "orphan-result-with-id",
        "tool-call-invalid-json-arg",
        "gap-tool-result",
        "dup-call-id",
        "tool-result-image",
        "user-image-prompt",
        "pdf-as-image",
        "custom-tool-call-flag",
        "parallel-call-id-frames",
        "history-tool-name",
    ] {
        let mut args = vec!["edge".to_string()];
        if case == "user-image-prompt" {
            args.push("-prompt".to_string());
            args.push("describe it".to_string());
        }
        args.push(case.to_string());
        all_cases.push((format!("edge-{case}"), args));
    }
    all_cases.push((
        "edge-tool-name".to_string(),
        vec!["edge".into(), "tool-name".into(), "x".into()],
    ));
    all_cases.push((
        "edge-n-tools-limit".to_string(),
        vec!["edge".into(), "n-tools-limit".into(), "3".into()],
    ));

    let expected_request_count = |name: &str| -> usize {
        match name {
            "status" => 4,
            "assign" | "rerun" | "replay-with-sig" | "replay-bogus-typed" | "chat-resolve" => 2,
            "misc" => 5,
            _ => 1,
        }
    };

    let mut results = Vec::new();
    for (name, args) in &all_cases {
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let go = probe_run(&go["probe"], &arg_refs, &cwd);
        let rust = probe_run(&rust["probe"], &arg_refs, &cwd);
        let mut diffs = Vec::new();
        match (&go, &rust) {
            (Ok((gc, gout, greqs, _)), Ok((rc, rout, rreqs, _))) => {
                if gc != rc {
                    diffs.push(format!("exit: go={gc} rust={rc}"));
                }
                if *gc != 0 || *rc != 0 {
                    diffs.push(format!("happy-path target must succeed: go={gc} rust={rc}"));
                }
                let expected = expected_request_count(name);
                if greqs.len() != expected || rreqs.len() != expected {
                    diffs.push(format!(
                        "happy-path request count: expected={expected} go={} rust={}",
                        greqs.len(),
                        rreqs.len()
                    ));
                }
                if gout != rout {
                    diffs.push("stdout differs".to_string());
                }
                if greqs != rreqs {
                    diffs.push(format!(
                        "requests differ: go={} rust={} recorded",
                        greqs.len(),
                        rreqs.len()
                    ));
                }
            }
            (Err(e), _) | (_, Err(e)) => diffs.push(format!("run: {e}")),
        }
        let detail = serde_json::json!({
            "go": go.map(|(c, o, r, e)| serde_json::json!({"code": c, "stdout": o, "requests": r, "stderr": e})).unwrap_or_default(),
            "rust": rust.map(|(c, o, r, e)| serde_json::json!({"code": c, "stdout": o, "requests": r, "stderr": e})).unwrap_or_default(),
        });
        if diffs.is_empty() {
            results.push(pass(name, detail));
        } else {
            results.push(fail(
                name,
                serde_json::json!({ "diffs": diffs, "detail": detail }),
            ));
        }
    }

    // `configs` writes outputs/probe/ under cwd — isolate each impl.
    {
        let name = "configs".to_string();
        let mut diffs = Vec::new();
        let mut detail = serde_json::Map::new();
        for (label, bins) in [("go", go), ("rust", rust)] {
            let case_cwd = dir.join(format!("configs-{label}"));
            let _ = std::fs::create_dir_all(case_cwd.join("home"));
            let _ = std::fs::create_dir_all(case_cwd.join("xdg"));
            match probe_run(&bins["probe"], &["configs"], &case_cwd) {
                Ok((code, stdout, reqs, stderr)) => {
                    let request_count = reqs.len();
                    let written = case_cwd
                        .join("outputs/probe/cli-model-configs.json")
                        .metadata()
                        .map_or(0, |m| m.len());
                    detail.insert(
                        label.to_string(),
                        serde_json::json!({ "code": code, "stdout": stdout, "requests": reqs, "stderr": stderr, "written": written }),
                    );
                    if code != 0 || written == 0 || request_count != 1 {
                        diffs.push(format!(
                            "{label}: code={code} written={written} requests={request_count}"
                        ));
                    }
                }
                Err(e) => diffs.push(format!("{label}: {e}")),
            }
        }
        if diffs.is_empty() {
            results.push(pass(&name, serde_json::Value::Object(detail)));
        } else {
            results.push(fail(
                &name,
                serde_json::json!({ "diffs": diffs, "detail": detail }),
            ));
        }
    }
    results
}

// ---------- example ---------------------------------------------------------------------

fn example_case(rust: &BTreeMap<String, PathBuf>, work: &Path) -> TargetResult {
    let name = "example-devin-client";
    let dir = work.join("example");
    let _ = std::fs::create_dir_all(&dir);
    let res = with_stub(
        &rust["upstreamstub"],
        &["-scenario", "stream", "-deltas", "3"],
        &dir,
        |addr| {
            run_scrubbed(
                &rust["devin_client"],
                &[&format!("http://{addr}")],
                &dir,
                &[],
                Duration::from_secs(15),
            )
        },
    );
    match res {
        Ok(Ok(out)) if out.code == 0 && !out.stdout.is_empty() => {
            pass(name, serde_json::json!({ "stdout": out.stdout }))
        }
        Ok(Ok(out)) => fail(
            name,
            serde_json::json!({ "code": out.code, "stdout": out.stdout, "stderr": out.stderr }),
        ),
        Ok(Err(e)) => fail(name, serde_json::json!({ "run": e.to_string() })),
        Err(e) => fail(name, serde_json::json!({ "stub": e.to_string() })),
    }
}

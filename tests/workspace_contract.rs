//! Workspace contract tests for the devin2api Rust port (plan task 1).
//!
//! Pins the source/output separation between the read-only Go reference tree
//! `G` (`$W/devin2api`) and the Rust output root `R` (`$W/rust`), plus the
//! structural workspace contract from the plan's Scope section.

use std::path::{Path, PathBuf};

/// Absolute, canonicalized path of the Rust workspace root (R).
fn rust_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("workspace root must canonicalize")
}

/// Absolute, canonicalized path of the read-only Go reference root (G),
/// or `None` when the checkout is absent (e.g. CI clones only R).
fn go_root() -> Option<PathBuf> {
    let candidate = if let Some(dir) = std::env::var_os("DEVIN2API_GO_ROOT") {
        PathBuf::from(dir)
    } else {
        rust_root()
            .parent()
            .expect("R must have a parent W")
            .join("devin2api")
    };
    if !candidate.join("go.mod").is_file() {
        return None;
    }
    Some(candidate.canonicalize().expect("G must canonicalize"))
}

/// An output root is valid only when it neither contains nor is contained by
/// the Go reference tree: the two trees must be disjoint.
fn is_valid_output_root(candidate: &Path, go_root: &Path) -> bool {
    let candidate = candidate
        .canonicalize()
        .unwrap_or_else(|_| candidate.to_path_buf());
    !candidate.starts_with(go_root) && !go_root.starts_with(&candidate)
}

fn manifest() -> toml::Value {
    let text = std::fs::read_to_string(rust_root().join("Cargo.toml"))
        .expect("Cargo.toml must be readable");
    toml::from_str(&text).expect("Cargo.toml must parse")
}

#[test]
fn rejects_reference_overlap() {
    let r = rust_root();
    let Some(g) = go_root() else {
        eprintln!("SKIP: Go reference checkout not present (CI clone of R only)");
        return;
    };
    let w = r.parent().expect("R must have a parent W");

    assert!(
        g.join("go.mod").is_file(),
        "G must be the Go reference checkout"
    );
    assert!(
        !is_valid_output_root(&g, &g),
        "G itself must be rejected as an output root"
    );
    assert!(
        !is_valid_output_root(&g.join("internal"), &g),
        "a subdirectory of G must be rejected as an output root"
    );
    assert!(
        !is_valid_output_root(w, &g),
        "the parent W containing G must be rejected as an output root"
    );
    assert!(
        is_valid_output_root(&r, &g),
        "R must be accepted as an output root disjoint from G"
    );

    for artifact in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "target"] {
        assert!(
            !g.join(artifact).exists(),
            "G must not contain Rust artifact {artifact}"
        );
    }
}

#[test]
fn package_identity() {
    let m = manifest();
    let pkg = &m["package"];
    assert_eq!(pkg["name"].as_str().unwrap(), "devin2api");
    assert!(pkg["repository"]["workspace"].as_bool().unwrap());
    assert_eq!(
        m["workspace"]["package"]["repository"].as_str().unwrap(),
        "https://github.com/min9lin9/devin2api"
    );
    assert!(pkg["license"]["workspace"].as_bool().unwrap());
    assert!(pkg["edition"]["workspace"].as_bool().unwrap());
    assert_eq!(
        m["workspace"]["package"]["license"].as_str().unwrap(),
        "MIT"
    );
    assert_eq!(
        m["workspace"]["package"]["edition"].as_str().unwrap(),
        "2024"
    );
}

#[test]
fn workspace_members_and_targets() {
    let m = manifest();
    let members: Vec<&str> = m["workspace"]["members"]
        .as_array()
        .expect("workspace.members must exist")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(
        members.contains(&"crates/devin-proto"),
        "generated-message member missing: {members:?}"
    );

    let bins = m["bin"].as_array().expect("[[bin]] targets must exist");
    let names: Vec<&str> = bins.iter().map(|b| b["name"].as_str().unwrap()).collect();
    for expected in [
        "devin-2api",
        "probe",
        "protoextract",
        "protocensus",
        "loadtest",
        "upstreamstub",
        "qa",
    ] {
        assert!(names.contains(&expected), "missing bin target {expected}");
    }

    let qa = bins
        .iter()
        .find(|b| b["name"].as_str() == Some("qa"))
        .unwrap();
    let required: Vec<&str> = qa["required-features"]
        .as_array()
        .expect("qa bin must be feature-gated")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(required.contains(&"qa"), "qa bin must require feature `qa`");
    assert!(
        m["features"]["qa"].is_array(),
        "feature `qa` must be declared"
    );
}

#[test]
fn module_roots_exist() {
    let r = rust_root();
    let files = [
        "src/lib.rs",
        "src/config.rs",
        "src/domain.rs",
        "src/debuglog.rs",
        "src/metrics.rs",
        "src/dashboard.rs",
        "src/protocol/mod.rs",
        "src/protocol/responses.rs",
        "src/protocol/chat.rs",
        "src/protocol/messages.rs",
        "src/upstream/mod.rs",
        "src/upstream/transport.rs",
        "src/upstream/catalog.rs",
        "src/upstream/request.rs",
        "src/upstream/response.rs",
        "src/upstream/retry.rs",
        "src/upstream/gate.rs",
        "src/server/mod.rs",
        "src/server/http.rs",
        "src/server/stream.rs",
        "src/server/websocket.rs",
        "src/server/lifecycle.rs",
        "src/bin/devin-2api.rs",
        "src/bin/probe.rs",
        "src/bin/protoextract.rs",
        "src/bin/protocensus.rs",
        "src/bin/loadtest.rs",
        "src/bin/upstreamstub.rs",
        "src/bin/qa.rs",
        "crates/devin-proto/Cargo.toml",
        "crates/devin-proto/src/lib.rs",
    ];
    for f in files {
        assert!(r.join(f).is_file(), "missing required file {f}");
    }
}

#[test]
fn license_and_toolchain() {
    let r = rust_root();
    let license = std::fs::read_to_string(r.join("LICENSE")).expect("LICENSE must exist");
    assert!(license.contains("MIT License"));
    assert!(
        license.contains("leookun"),
        "original copyright notice must be retained"
    );

    let tc_text = std::fs::read_to_string(r.join("rust-toolchain.toml")).unwrap();
    let tc: toml::Value = toml::from_str(&tc_text).unwrap();
    let channel = tc["toolchain"]["channel"]
        .as_str()
        .expect("toolchain channel must be set");
    assert!(
        channel.split('.').count() == 3 && channel.chars().all(|c| c.is_ascii_digit() || c == '.'),
        "channel must pin an exact release, got {channel}"
    );
    assert!(r.join("Cargo.lock").is_file(), "Cargo.lock must exist");
}

#[test]
fn locked_pins() {
    let lock = std::fs::read_to_string(rust_root().join("Cargo.lock")).unwrap();
    for (name, version) in [
        ("connectrpc", "0.9.0"),
        ("connectrpc-build", "0.9.0"),
        ("buffa", "0.9.2"),
    ] {
        let needle = format!("name = \"{name}\"\nversion = \"{version}\"");
        assert!(
            lock.contains(&needle),
            "Cargo.lock must pin {name} {version}"
        );
    }
}

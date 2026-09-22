//! CI/release matrix contract tests for the devin2api Rust port (plan task 22).
//!
//! `packaging/targets.tsv` is the single source of truth for the six release
//! targets. These tests statically verify that `.github/workflows/ci.yml` and
//! `.github/workflows/release.yml` build exactly those targets on the right
//! hosted runners, keep the asset naming/packaging contract, gate the publish
//! job on verified artifacts, and reject Linux binaries with a dynamic
//! interpreter. No workflow job is executed here; foreign-platform builds are
//! verified by CI, not by this test.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use serde_yaml_ng::Value;

fn rust_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .expect("workspace root must canonicalize")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{} must be readable: {e}", path.display()))
}

fn load_workflow(name: &str) -> Value {
    let path = rust_root().join(".github/workflows").join(name);
    serde_yaml_ng::from_str(&read(&path))
        .unwrap_or_else(|e| panic!("{} must parse as YAML: {e}", path.display()))
}

/// One row of `packaging/targets.tsv`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    triple: String,
    asset: String,
    kind: String,
}

fn targets() -> Vec<Target> {
    let text = read(&rust_root().join("packaging/targets.tsv"));
    text.lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .map(|l| {
            let mut c = l.split('\t');
            Target {
                triple: c.next().expect("tsv column 1").to_string(),
                asset: c.next().expect("tsv column 2").to_string(),
                kind: c.next().expect("tsv column 3").to_string(),
            }
        })
        .collect()
}

fn job<'a>(wf: &'a Value, name: &str) -> &'a Value {
    wf.get("jobs")
        .and_then(|j| j.get(name))
        .unwrap_or_else(|| panic!("workflow must define job {name}"))
}

fn steps(job: &Value) -> Vec<&Value> {
    job.get("steps")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().collect())
        .unwrap_or_default()
}

fn step_runs(job: &Value) -> Vec<String> {
    steps(job)
        .iter()
        .filter_map(|s| s.get("run").and_then(Value::as_str).map(str::to_string))
        .collect()
}

fn step_ifs(job: &Value) -> Vec<String> {
    steps(job)
        .iter()
        .filter_map(|s| s.get("if").and_then(Value::as_str).map(str::to_string))
        .collect()
}

fn step_uses(job: &Value) -> Vec<String> {
    steps(job)
        .iter()
        .filter_map(|s| s.get("uses").and_then(Value::as_str).map(str::to_string))
        .collect()
}

/// Matrix `include` rows of a job as a list of key→string maps.
fn matrix_include(job: &Value) -> Vec<Vec<(String, String)>> {
    job.get("strategy")
        .and_then(|s| s.get("matrix"))
        .and_then(|m| m.get("include"))
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("job must define strategy.matrix.include"))
        .iter()
        .map(|row| {
            row.as_mapping()
                .expect("matrix row must be a mapping")
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().expect("matrix key").to_string(),
                        v.as_str().expect("matrix value").to_string(),
                    )
                })
                .collect()
        })
        .collect()
}

fn field(row: &[(String, String)], key: &str) -> Option<String> {
    row.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
}

/// GitHub-hosted runner labels this project may use. Anything outside this
/// list is either nonexistent or a paid/larger runner the plan forbids by
/// default.
const FREE_RUNNERS: &[&str] = &[
    "ubuntu-latest",
    "ubuntu-24.04",
    "ubuntu-22.04",
    "ubuntu-24.04-arm",
    "ubuntu-22.04-arm",
    "macos-latest",
    "macos-15",
    "macos-15-intel",
    "macos-14",
    "macos-14-intel",
    "macos-13",
    "windows-latest",
    "windows-2025",
    "windows-2022",
];

fn runner_os(runner: &str) -> &'static str {
    if runner.starts_with("ubuntu") {
        "linux"
    } else if runner.starts_with("macos") {
        "darwin"
    } else if runner.starts_with("windows") {
        "windows"
    } else {
        "unknown"
    }
}

fn runner_arch(runner: &str) -> &'static str {
    if runner.ends_with("-arm") {
        "aarch64"
    } else if runner.contains("intel") {
        "x86_64"
    } else if runner.starts_with("macos-1") && !runner.contains("intel") {
        // macos-13/14/15 without an -intel suffix are Apple Silicon images.
        "aarch64"
    } else {
        "x86_64"
    }
}

/// Validate a build-matrix job (`include` rows with runner/target/asset/kind)
/// against the canonical target table. Returns the list of violations; an
/// empty list means the matrix is exactly the release contract.
fn validate_matrix(rows: &[Vec<(String, String)>], expected: &[Target]) -> Vec<String> {
    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        let triple = field(row, "target").unwrap_or_default();
        let asset = field(row, "asset").unwrap_or_default();
        let runner = field(row, "runner").unwrap_or_default();
        let kind = field(row, "kind").unwrap_or_default();
        if !seen.insert(triple.clone()) {
            violations.push(format!("duplicate matrix target {triple}"));
        }
        let Some(t) = expected.iter().find(|t| t.triple == triple) else {
            violations.push(format!(
                "matrix target {triple} not in packaging/targets.tsv"
            ));
            continue;
        };
        if asset != t.asset {
            violations.push(format!(
                "{}: asset {asset} != targets.tsv {}",
                t.triple, t.asset
            ));
        }
        if kind != t.kind {
            violations.push(format!(
                "{}: kind {kind} != targets.tsv {}",
                t.triple, t.kind
            ));
        }
        if !FREE_RUNNERS.contains(&runner.as_str()) {
            violations.push(format!("{}: non-free or unknown runner {runner}", t.triple));
        }
        let os = runner_os(&runner);
        let arch = runner_arch(&runner);
        if t.triple.contains("linux") && os != "linux" {
            violations.push(format!("{}: linux target on {runner}", t.triple));
        }
        // Darwin targets need the Apple SDK: the runner must be macOS of the
        // same architecture (no cross-compiling between Intel and Silicon).
        if t.triple.contains("apple-darwin")
            && (os != "darwin" || arch != t.triple.split('-').next().unwrap_or_default())
        {
            violations.push(format!(
                "{}: darwin target needs same-arch macOS runner, got {runner}",
                t.triple
            ));
        }
        if t.triple.contains("windows-msvc") && os != "windows" {
            violations.push(format!("{}: windows target on {runner}", t.triple));
        }
        // Windows arm64 may cross-build on a Windows amd64 runner; every other
        // target must run on a runner of its own architecture.
        if t.triple == "aarch64-pc-windows-msvc" && arch != "x86_64" && arch != "aarch64" {
            violations.push(format!("{}: unexpected runner arch {runner}", t.triple));
        }
        if t.triple != "aarch64-pc-windows-msvc"
            && arch != t.triple.split('-').next().unwrap_or_default()
        {
            violations.push(format!("{}: runner arch mismatch ({runner})", t.triple));
        }
    }
    for t in expected {
        if !seen.contains(&t.triple) {
            violations.push(format!("missing matrix target {}", t.triple));
        }
    }
    violations
}

/// Minimal `ELF` program-header scan: does the file declare `PT_INTERP` (a
/// dynamic program interpreter)? Linux release assets must not.
fn elf_has_interp(bytes: &[u8]) -> Option<bool> {
    if bytes.len() < 0x40 || &bytes[0..4] != b"\x7fELF" {
        return None;
    }
    let is64 = bytes[4] == 2;
    let le = bytes[5] != 2; // ELFDATA2LSB or unset; treat MSB as unsupported→None below
    if !le {
        return None;
    }
    let u16 = |o: usize| -> Option<u16> {
        bytes
            .get(o..o + 2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]))
    };
    let u32 = |o: usize| -> Option<u32> {
        bytes
            .get(o..o + 4)
            .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    };
    let u64 = |o: usize| -> Option<u64> {
        bytes
            .get(o..o + 8)
            .map(|b| u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    };
    let (phoff, phentsize, phnum) = if is64 {
        (
            usize::try_from(u64(0x20)?).unwrap_or(usize::MAX),
            u16(0x36)? as usize,
            u16(0x38)? as usize,
        )
    } else {
        (
            u32(0x1c)? as usize,
            u16(0x2a)? as usize,
            u16(0x2c)? as usize,
        )
    };
    for i in 0..phnum {
        let off = phoff + i * phentsize;
        // p_type is the first field of Elf32/Elf64_Phdr alike.
        if u32(off)? == 3 {
            // PT_INTERP
            return Some(true);
        }
    }
    Some(false)
}

/// Build a minimal ELF64 image with the given program-header types.
fn elf64_with_phdrs(p_types: &[u32]) -> Vec<u8> {
    let mut b = vec![0u8; 0x40 + p_types.len() * 56];
    b[0..4].copy_from_slice(b"\x7fELF");
    b[4] = 2; // ELFCLASS64
    b[5] = 1; // little-endian
    b[0x20..0x28].copy_from_slice(&0x40u64.to_le_bytes()); // e_phoff
    b[0x36..0x38].copy_from_slice(&56u16.to_le_bytes()); // e_phentsize
    b[0x38..0x3a].copy_from_slice(
        &u16::try_from(p_types.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for (i, t) in p_types.iter().enumerate() {
        b[0x40 + i * 56..0x44 + i * 56].copy_from_slice(&t.to_le_bytes());
    }
    b
}

// ---------------------------------------------------------------------------
// Canonical target table
// ---------------------------------------------------------------------------

#[test]
fn targets_tsv_is_the_six_asset_contract() {
    let t = targets();
    assert_eq!(t.len(), 6, "release contract is exactly six targets");
    let assets: BTreeSet<_> = t.iter().map(|t| t.asset.as_str()).collect();
    assert_eq!(assets.len(), 6, "asset names must be unique");
    for row in &t {
        match row.kind.as_str() {
            "binary" => assert!(
                !row.asset.to_ascii_lowercase().ends_with(".zip"),
                "{}: unix assets are bare binaries",
                row.triple
            ),
            "windows-zip" => assert!(
                row.asset.to_ascii_lowercase().ends_with(".zip"),
                "{}: windows assets are zip archives",
                row.triple
            ),
            other => panic!("{}: unknown kind {other}", row.triple),
        }
        assert!(
            row.asset.starts_with("devin-2api-"),
            "{}: asset name must keep the devin-2api-* contract",
            row.triple
        );
    }
    // The six-target contract: linux/darwin/windows × amd64/arm64.
    for (os, arch) in [
        ("linux", "amd64"),
        ("linux", "arm64"),
        ("darwin", "amd64"),
        ("darwin", "arm64"),
        ("windows", "amd64"),
        ("windows", "arm64"),
    ] {
        let want = format!("devin-2api-{os}-{arch}");
        assert!(
            t.iter()
                .any(|r| r.asset == want || r.asset == format!("{want}.zip")),
            "missing release asset for {os}/{arch}"
        );
    }
}

// ---------------------------------------------------------------------------
// CI workflow
// ---------------------------------------------------------------------------

#[test]
fn ci_workflow_covers_required_checks() {
    let wf = load_workflow("ci.yml");
    let all_runs: String = wf
        .get("jobs")
        .and_then(Value::as_mapping)
        .expect("ci.yml must define jobs")
        .values()
        .flat_map(step_runs)
        .collect::<Vec<_>>()
        .join("\n");

    for needle in [
        "cargo fmt --all -- --check",
        "cargo clippy --locked --workspace --all-targets --all-features -- -D warnings",
        "cargo test --locked --workspace --all-features",
        "generate-proto.sh --check",
        "check-licenses.sh",
        "check-secrets.sh",
        "deploy-assets.test.sh",
        "release-selftest.sh",
    ] {
        assert!(all_runs.contains(needle), "ci.yml missing check: {needle}");
    }
    // No job may request repository secrets (the implicit GITHUB_TOKEN
    // needs no secrets.* expression).
    let text = read(&rust_root().join(".github/workflows/ci.yml"));
    assert!(
        !text.contains("${{ secrets."),
        "ci.yml must not reference secrets.* expressions"
    );
}

#[test]
fn ci_matrix_matches_targets_tsv() {
    let wf = load_workflow("ci.yml");
    let expected = targets();
    let mut checked = 0;
    for (name, job) in wf
        .get("jobs")
        .and_then(Value::as_mapping)
        .expect("jobs")
        .iter()
        .map(|(k, v)| (k.as_str().unwrap_or_default().to_string(), v))
    {
        if job.get("strategy").and_then(|s| s.get("matrix")).is_none() {
            continue;
        }
        let violations = validate_matrix(&matrix_include(job), &expected);
        assert!(
            violations.is_empty(),
            "ci.yml job {name} matrix violations: {violations:?}"
        );
        checked += 1;
    }
    assert!(
        checked >= 1,
        "ci.yml must contain a six-target build matrix"
    );
}

#[test]
fn ci_matrix_builds_and_asserts_static_linux() {
    let wf = load_workflow("ci.yml");
    let jobs = wf.get("jobs").and_then(Value::as_mapping).expect("jobs");
    let mut found = false;
    for job in jobs.values() {
        if job.get("strategy").and_then(|s| s.get("matrix")).is_none() {
            continue;
        }
        let runs = step_runs(job).join("\n");
        let ifs = step_ifs(job).join("\n");
        assert!(
            runs.contains("cargo build --locked") && runs.contains("--target"),
            "matrix job must build with cargo --locked --target"
        );
        assert!(
            runs.contains("readelf") && runs.contains("interpreter"),
            "matrix job must reject Linux binaries with a dynamic interpreter"
        );
        assert!(
            ifs.contains("linux") || ifs.contains("Linux"),
            "static-binary check must be gated on Linux matrix legs"
        );
        let uses = step_uses(job).join("\n");
        assert!(
            uses.contains("upload-artifact"),
            "matrix job must upload named artifacts"
        );
        found = true;
    }
    assert!(found, "ci.yml must contain a matrix build job");
}

// ---------------------------------------------------------------------------
// Release workflow
// ---------------------------------------------------------------------------

#[test]
fn release_matrix_matches_targets_tsv() {
    let wf = load_workflow("release.yml");
    let job = job(&wf, "binaries");
    let violations = validate_matrix(&matrix_include(job), &targets());
    assert!(
        violations.is_empty(),
        "release.yml binaries matrix violations: {violations:?}"
    );
}

#[test]
fn release_gating_and_packaging_contract() {
    let wf = load_workflow("release.yml");
    // Tag-only trigger: the publish pipeline needs explicit tag authorization.
    let text = read(&rust_root().join(".github/workflows/release.yml"));
    assert!(
        text.contains("tags:") && text.contains("v*"),
        "release.yml must trigger on v* tags only"
    );

    let binaries = job(&wf, "binaries");
    let rows = matrix_include(binaries);
    for row in &rows {
        let target = field(row, "target").expect("release matrix target");
        let smoke = field(row, "smoke").expect("release matrix smoke policy");
        let expected = match target.as_str() {
            "aarch64-pc-windows-msvc" => "none",
            "x86_64-pc-windows-msvc" => "version",
            _ => "native",
        };
        assert_eq!(
            smoke, expected,
            "{target}: release smoke policy must label executable coverage"
        );
    }
    let runs = step_runs(binaries).join("\n");
    let ifs = step_ifs(binaries).join("\n");
    assert!(
        text.contains("DEVIN2API_BUILD_VERSION"),
        "release build must inject the tag as DEVIN2API_BUILD_VERSION"
    );
    assert!(
        runs.contains("readelf") && runs.contains("interpreter"),
        "release must reject Linux binaries with a dynamic interpreter"
    );
    assert!(
        runs.contains("package-release.sh") || runs.contains("Compress-Archive"),
        "release must package assets via the packaging contract"
    );
    assert!(
        runs.contains("scripts/smoke.sh --no-upstream")
            && runs.contains("devin-2api.exe' -version"),
        "tagged native artifacts must pass release smoke before upload"
    );
    assert!(
        ifs.contains("matrix.smoke == 'native'")
            && ifs.contains("matrix.smoke == 'version'")
            && ifs.contains("matrix.smoke == 'none'"),
        "release must label native, version-only, and unavailable smoke legs"
    );

    let publish = job(&wf, "publish");
    let needs: Vec<String> = publish
        .get("needs")
        .and_then(|n| match n {
            Value::String(s) => Some(vec![s.clone()]),
            Value::Sequence(s) => Some(
                s.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();
    assert!(
        needs.iter().any(|n| n == "binaries") && needs.iter().any(|n| n == "test"),
        "publish must need both tests and the verified binaries job"
    );
    let pruns = step_runs(publish).join("\n");
    assert!(
        pruns.contains("targets.tsv"),
        "publish must verify all six assets against packaging/targets.tsv"
    );
    assert!(
        pruns.contains("sha256sum"),
        "publish must produce and verify checksums"
    );
    let puses = step_uses(publish).join("\n");
    assert!(
        puses.contains("download-artifact"),
        "publish must consume the verified uploaded artifacts"
    );
    assert!(
        puses.contains("action-gh-release"),
        "publish must create the GitHub release"
    );
    assert!(
        text.contains("dist/devin-2api-*") && text.contains("dist/checksums.txt"),
        "release must publish the six assets plus checksums.txt"
    );
    assert!(
        text.contains("linux/amd64,linux/arm64"),
        "Docker image must publish linux/amd64+linux/arm64"
    );
    // Windows zip contents are part of the asset contract.
    for needle in ["devin-2api.exe", "config.example.yaml", "LICENSE"] {
        assert!(text.contains(needle), "windows zip must contain {needle}");
    }
}

// ---------------------------------------------------------------------------
// Security workflow
// ---------------------------------------------------------------------------

#[test]
fn security_workflow_runs_cargo_audit() {
    let wf = load_workflow("security.yml");
    let text = read(&rust_root().join(".github/workflows/security.yml"));
    assert!(
        text.contains("cron:"),
        "security.yml must keep the scheduled scan"
    );
    let all: String = wf
        .get("jobs")
        .and_then(Value::as_mapping)
        .expect("jobs")
        .values()
        .flat_map(|j| {
            step_runs(j)
                .into_iter()
                .chain(step_uses(j))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        all.contains("cargo audit") || all.contains("audit-check"),
        "security.yml must run a Rust advisory scan (cargo audit)"
    );
}

// ---------------------------------------------------------------------------
// Failure cases (plan: missing_target_and_dynamic_linux_rejected)
// ---------------------------------------------------------------------------

#[test]
fn missing_target_and_dynamic_linux_rejected() {
    let expected = targets();
    let wf = load_workflow("release.yml");
    let rows = matrix_include(job(&wf, "binaries"));

    // A matrix that drops one target must be rejected.
    let missing: Vec<_> = rows
        .iter()
        .filter(|r| field(r, "target").as_deref() != Some("aarch64-apple-darwin"))
        .cloned()
        .collect();
    let violations = validate_matrix(&missing, &expected);
    assert!(
        violations
            .iter()
            .any(|v| v.contains("missing matrix target")),
        "dropping a target must be rejected, got {violations:?}"
    );

    // A matrix that adds a platform outside the contract must be rejected.
    let mut extra = rows.clone();
    extra.push(vec![
        ("runner".into(), "ubuntu-latest".into()),
        ("target".into(), "riscv64gc-unknown-linux-musl".into()),
        ("asset".into(), "devin-2api-linux-riscv64".into()),
        ("kind".into(), "binary".into()),
    ]);
    let violations = validate_matrix(&extra, &expected);
    assert!(
        violations
            .iter()
            .any(|v| v.contains("not in packaging/targets.tsv")),
        "adding a platform must be rejected, got {violations:?}"
    );

    // The static-binary gate: an ELF with PT_INTERP is dynamic and must be
    // rejected; one without it is the static musl artifact we ship.
    let dynamic = elf64_with_phdrs(&[1, 3, 1]); // PT_LOAD, PT_INTERP, PT_LOAD
    let static_bin = elf64_with_phdrs(&[1, 1, 4]); // PT_LOAD, PT_LOAD, PT_NOTE
    assert_eq!(elf_has_interp(&dynamic), Some(true));
    assert_eq!(elf_has_interp(&static_bin), Some(false));
    assert_eq!(elf_has_interp(b"not an elf"), None);

    // And the workflow must actually contain that gate for Linux legs.
    let runs = step_runs(job(&wf, "binaries")).join("\n");
    assert!(
        runs.contains("readelf") && runs.contains("interpreter"),
        "release binaries job must reject dynamic Linux binaries"
    );
}

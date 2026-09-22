//! Shared helpers for integration test targets. Lives in a subdirectory so
//! it is not compiled as its own test target; each test file opts in with
//! `mod support;`.

use std::path::PathBuf;

use devin2api::qa::oracle::GoOracle;

/// Go reference root (`QA_GO_ROOT` or `../devin2api` next to the repo).
// Shared helpers: not every test binary uses every helper.
#[allow(dead_code)]
pub fn go_root() -> PathBuf {
    GoOracle::default_go_root()
}

/// An oracle bound to the Go reference, or `None` (with a SKIP note on
/// stderr) when the checkout or Go toolchain is unavailable.
#[allow(dead_code)]
pub fn require_go_oracle() -> Option<GoOracle> {
    let root = go_root();
    if !root.join("go.mod").is_file() {
        eprintln!("SKIP: Go reference not found at {}", root.display());
        return None;
    }
    let oracle = GoOracle::new(root);
    if let Err(err) = oracle.go_version() {
        eprintln!("SKIP: Go toolchain unavailable: {err}");
        return None;
    }
    Some(oracle)
}

/// Fresh per-test work directory under the OS temp dir.
pub fn work_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("devin2api-qa-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create work dir");
    dir
}

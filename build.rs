use std::path::Path;
use std::process::Command;

fn non_dev(value: Option<String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty() && v != "dev")
}

fn vcs_version(root: &Path) -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let revision = String::from_utf8(output.stdout).ok()?;
    let revision = revision.trim();
    if revision.is_empty() {
        return None;
    }
    let mut value = format!("dev-{}", &revision[..revision.len().min(12)]);
    let dirty = Command::new("git")
        .args(["status", "--porcelain", "--untracked-files=no"])
        .current_dir(root)
        .output()
        .is_ok_and(|out| out.status.success() && !out.stdout.is_empty());
    if dirty {
        value.push_str("-dirty");
    }
    Some(value)
}

fn main() {
    println!("cargo:rerun-if-env-changed=DEVIN2API_BUILD_VERSION");
    println!("cargo:rerun-if-env-changed=DEVIN2API_PACKAGE_VERSION");
    println!("cargo:rerun-if-changed=VERSION");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    let root = std::env::var_os("CARGO_MANIFEST_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_default();
    let version = non_dev(std::env::var("DEVIN2API_BUILD_VERSION").ok())
        .or_else(|| non_dev(std::env::var("DEVIN2API_PACKAGE_VERSION").ok()))
        .or_else(|| vcs_version(&root))
        .or_else(|| {
            std::fs::read_to_string(root.join("VERSION"))
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        })
        .unwrap_or_else(|| "dev".to_string());
    println!("cargo:rustc-env=DEVIN2API_RESOLVED_VERSION={version}");
}

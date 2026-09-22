//! Auxiliary binary `protoextract` — extract embedded
//! `FileDescriptorProto` values from a compiled binary into
//! `descriptors.pb` (raw set), `all-protos.proto` (flattened compilable
//! bundle) and `manifest.json` (provenance + caveats). Port of
//! `G/cmd/`protoextract`/main.go`.
//!
//! The output directory is deleted and recreated; the destructive-path
//! guards (root, home, cwd-containing, source-containing) are enforced
//! before anything is removed. Extraction only ever targets directories
//! the caller names — tests redirect it to temp dirs.

use std::path::Path;
use std::process::ExitCode;

use buffa_descriptor::generated::descriptor::FileDescriptorSet;
use devin2api::auxiliary::extract::{
    self, DEFAULT_BUNDLE_NAME, encode_set, prepare_fresh_output, resolve_descriptors,
    scan_file_descriptors,
};
use devin2api::auxiliary::flatten::{
    PREFERRED_ROOT_PACKAGE, flatten_descriptors, render_flattened,
};

fn main() -> ExitCode {
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        eprintln!(
            "Usage: {} <source-binary> <output-directory>",
            Path::new(&std::env::args_os().next().unwrap_or_default())
                .file_name()
                .map_or_else(
                    || "protoextract".to_string(),
                    |s| s.to_string_lossy().into_owned()
                )
        );
        return ExitCode::from(2);
    }
    match run(Path::new(&args[0]), Path::new(&args[1])) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(binary_path: &Path, output_dir: &Path) -> anyhow::Result<()> {
    let (binary_path, output_dir) = prepare_fresh_output(binary_path, output_dir)?;
    let binary = std::fs::read(&binary_path)?;
    let (files, stats) = scan_file_descriptors(&binary);
    if files.is_empty() {
        anyhow::bail!("no FileDescriptorProto values found");
    }

    let descriptor_set = FileDescriptorSet {
        file: files.clone(),
        ..Default::default()
    };
    std::fs::write(output_dir.join("descriptors.pb"), encode_set(files.clone()))?;

    if let Err(resolution_err) = resolve_descriptors(&descriptor_set) {
        eprintln!("warning: descriptor set is not fully resolvable: {resolution_err}");
    }

    let (flattened, flattening) =
        flatten_descriptors(&files, PREFERRED_ROOT_PACKAGE).map_err(anyhow::Error::msg)?;
    let bundle = render_flattened(&flattened, files.len());
    std::fs::write(output_dir.join(DEFAULT_BUNDLE_NAME), bundle)?;

    let manifest =
        extract::build_manifest(&binary_path, DEFAULT_BUNDLE_NAME, &files, stats, flattening);
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    manifest_bytes.push(b'\n');
    std::fs::write(output_dir.join("manifest.json"), manifest_bytes)?;

    println!(
        "extracted {} unique descriptors ({} duplicate candidates)",
        files.len(),
        stats.duplicates
    );
    println!(
        "compilable flattened proto: {}",
        output_dir.join(DEFAULT_BUNDLE_NAME).display()
    );
    println!(
        "comments: {} locations across {} files",
        manifest.comment_location_count, manifest.files_with_comments
    );
    if !manifest.missing_dependencies.is_empty() {
        println!(
            "warning: {} referenced dependencies were not embedded; see manifest.json",
            manifest.missing_dependencies.len()
        );
    }
    Ok(())
}

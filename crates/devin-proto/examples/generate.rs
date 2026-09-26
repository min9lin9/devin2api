//! Regenerate the checked-in Devin protobuf bindings.
//!
//! Driven by `scripts/generate-proto.sh`; reads the precompiled
//! `proto/all-protos.fds` `FileDescriptorSet` (produced from
//! `proto/all-protos.proto` — see that script for the compiler chain) and
//! writes generated sources via connectrpc-build's documented
//! descriptor-set interface.
//!
//! Usage: `cargo run -p devin-proto --example generate -- [--out DIR]`
//! Default output is `src/generated/` inside this crate.

use std::path::PathBuf;

fn main() {
    let mut out: Option<PathBuf> = None;
    let mut fds: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out" => {
                out = Some(PathBuf::from(
                    args.next().expect("--out requires a directory"),
                ));
            }
            "--fds" => {
                fds = Some(PathBuf::from(args.next().expect("--fds requires a file")));
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }

    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace = crate_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let fds = fds.unwrap_or_else(|| workspace.join("proto/all-protos.fds"));
    let out = out.unwrap_or_else(|| crate_dir.join("src/generated"));

    connectrpc_build::Config::new()
        .descriptor_set(&fds)
        .files(&["all-protos.proto"])
        .out_dir(&out)
        .include_file("mod.rs")
        .emit_rerun_directives(false)
        .compile()
        .expect("connectrpc-build codegen failed");
    eprintln!("generated into {}", out.display());
}

//! Auxiliary binary `protocensus` — protocol census over debuglog
//! traffic and descriptor-set drift diff. Port of
//! `G/cmd/`protocensus`/main.go`:
//!
//!   census [-logs DIR] [-max-dirs N]   field coverage, unknown keys,
//!                                    enum anomalies (default ./logs)
//!   diff OLD.pb NEW.pb                 added/removed/changed symbols

use std::path::PathBuf;
use std::process::ExitCode;

use devin2api::auxiliary::census::{
    self, Census, census_report, load_symbols, request_dirs, scan_request_dir, schema_pool,
};
use devin2api::auxiliary::goflag::{ErrorHandling, FlagSet};

fn usage() {
    eprintln!(
        "subcommands:
  census [-logs DIR] [-max-dirs N]
                        scan request dirs, report field coverage, unknown keys,
                        enum anomalies (default logs dir: ./logs)
  diff OLD.pb NEW.pb    compare two FileDescriptorSets (descriptors.pb),
                        report added/removed/changed symbols"
    );
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        usage();
        return ExitCode::from(2);
    };
    let result = match command.as_str() {
        "census" => cmd_census(&args[1..]),
        "diff" => cmd_diff(&args[1..]),
        _ => {
            usage();
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ERR: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// `messageDesc` — resolve a message type in the embedded flattened
/// registry (Go's generated-code global registry equivalent).
fn message_desc(
    pool: &buffa_descriptor::DescriptorPool,
    name: &str,
) -> anyhow::Result<buffa_descriptor::MessageDescriptor> {
    pool.message_by_name(name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("resolve {name}: not found (run task generate first)"))
}

fn cmd_census(args: &[String]) -> anyhow::Result<()> {
    let mut fs = FlagSet::new("census", ErrorHandling::ContinueOnError);
    let logs = fs.string("logs", "logs", "debuglog directory");
    let max_dirs = fs.int("max-dirs", 0, "only scan newest N request dirs (0 = all)");
    fs.parse(args).map_err(|e| anyhow::anyhow!("{e}"))?;

    let pool = schema_pool().map_err(|e| anyhow::anyhow!("{e}"))?;
    let req_md = message_desc(&pool, census::REQUEST_TYPE_NAME)?;
    let resp_md = message_desc(&pool, census::RESPONSE_TYPE_NAME)?;
    let dirs = request_dirs(
        &PathBuf::from(fs.str(logs)),
        usize::try_from(fs.get_int(max_dirs).max(0)).unwrap_or(0),
    )?;
    let mut req = Census::new(&pool);
    let mut resp = Census::new(&pool);
    let mut frames = 0usize;
    for dir in &dirs {
        frames += scan_request_dir(
            &PathBuf::from(fs.str(logs)),
            dir,
            &mut req,
            &mut resp,
            &req_md,
            &resp_md,
        );
    }
    let report = census_report(dirs.len(), frames, &req, &resp);
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn cmd_diff(args: &[String]) -> anyhow::Result<()> {
    if args.len() != 2 {
        anyhow::bail!("usage: diff OLD.pb NEW.pb");
    }
    let old = load_symbols(&PathBuf::from(&args[0]))?;
    let new = load_symbols(&PathBuf::from(&args[1]))?;
    let report = serde_json::json!({
        "added": census::diff_added(&old, &new),
        "removed": census::diff_added(&new, &old),
        "changed": census::diff_changed(&old, &new),
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

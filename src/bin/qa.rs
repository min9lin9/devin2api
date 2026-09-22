//! QA-only binary `qa` behind feature `qa`: process-level comparisons and
//! benchmarks. Task 2 implements `baseline` and `manifest`; every other
//! planned subcommand is registered and fails with exit 2 until its owning
//! task lands — placeholders never succeed.

use std::path::PathBuf;
use std::time::Duration;

use devin2api::qa::compare::{self, CanonRules};
use devin2api::qa::contracts;
use devin2api::qa::oracle::{BaselineRequest, GoOracle, OracleConfig};
use devin2api::qa::{SUBCOMMANDS, find_subcommand};

const USAGE: &str = "\
qa — devin2api parity QA driver (not a released daemon)

usage: qa <subcommand> [options]

common options:
  --evidence DIR    directory for JSON results and artifacts (required by runnable commands)
  --go-root DIR     Go reference checkout (default: ../devin2api or $QA_GO_ROOT)
  --case NAME       run one explicit case
  --allow-live      permit live upstream traffic (live only; never default)

subcommands:";

fn print_help() {
    println!("{USAGE}");
    for sub in SUBCOMMANDS {
        let status = if sub.implemented { "" } else { "  [planned]" };
        println!(
            "  {:<18} task {:<2} {}{}",
            sub.name, sub.owner_task, sub.summary, status
        );
    }
}

struct Args {
    subcommand: String,
    evidence: Option<PathBuf>,
    go_root: Option<PathBuf>,
    case: Option<String>,
    out: Option<PathBuf>,
    allow_live: bool,
    requests: Option<u64>,
}

fn parse_args(raw_args: &[String]) -> Result<Args, String> {
    let mut args = Args {
        subcommand: String::new(),
        evidence: None,
        go_root: None,
        case: None,
        out: None,
        allow_live: false,
        requests: None,
    };
    let mut i = 0;
    while i < raw_args.len() {
        let arg = &raw_args[i];
        match arg.as_str() {
            "--evidence" => {
                i += 1;
                args.evidence = Some(PathBuf::from(
                    raw_args.get(i).ok_or("--evidence needs a value")?,
                ));
            }
            "--go-root" => {
                i += 1;
                args.go_root = Some(PathBuf::from(
                    raw_args.get(i).ok_or("--go-root needs a value")?,
                ));
            }
            "--case" => {
                i += 1;
                args.case = Some(raw_args.get(i).ok_or("--case needs a value")?.clone());
            }
            "--out" => {
                i += 1;
                args.out = Some(PathBuf::from(raw_args.get(i).ok_or("--out needs a value")?));
            }
            "--requests" => {
                i += 1;
                args.requests = Some(
                    raw_args
                        .get(i)
                        .ok_or("--requests needs a value")?
                        .parse()
                        .map_err(|_| "--requests needs a number")?,
                );
            }
            "--allow-live" => args.allow_live = true,
            other if other.starts_with('-') => return Err(format!("unknown flag {other}")),
            other if args.subcommand.is_empty() => args.subcommand = other.to_string(),
            other => return Err(format!("unexpected argument {other}")),
        }
        i += 1;
    }
    if args.subcommand.is_empty() {
        return Err("missing subcommand".into());
    }
    Ok(args)
}

fn require_evidence(args: &Args) -> Result<PathBuf, String> {
    let dir = args
        .evidence
        .clone()
        .ok_or_else(|| format!("qa {} requires --evidence DIR", args.subcommand))?;
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(dir)
}

fn go_root(args: &Args) -> PathBuf {
    args.go_root
        .clone()
        .unwrap_or_else(GoOracle::default_go_root)
}

/// The baseline request set: every protocol plus `auth`/error/panel surface.
fn baseline_requests() -> Vec<BaselineRequest> {
    let mut unauthorized = BaselineRequest::get("unauthorized", "/v1/models");
    unauthorized.auth = false;
    vec![
        BaselineRequest::get("healthz", "/healthz"),
        unauthorized,
        BaselineRequest::get("models", "/v1/models"),
        BaselineRequest::get("model-detail", "/v1/models/stub-model"),
        BaselineRequest::post_json(
            "chat-stream",
            "/v1/chat/completions",
            r#"{"model":"stub-model","stream":true,"messages":[{"role":"user","content":"Reply exactly: pong"}]}"#,
        ),
        BaselineRequest::post_json(
            "chat-json",
            "/v1/chat/completions",
            r#"{"model":"stub-model","stream":false,"messages":[{"role":"user","content":"Reply exactly: pong"}]}"#,
        ),
        BaselineRequest::post_json(
            "responses-stream",
            "/v1/responses",
            r#"{"model":"stub-model","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Reply exactly: pong"}]}]}"#,
        ),
        BaselineRequest::post_json(
            "messages-stream",
            "/v1/messages",
            r#"{"model":"stub-model","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"Reply exactly: pong"}]}"#,
        ),
        BaselineRequest::post_json("chat-bad-json", "/v1/chat/completions", "{not json"),
        BaselineRequest::get("unknown-route", "/v1/nope"),
        BaselineRequest::get("panel-api", "/panel/api"),
        BaselineRequest::get("panel-api-status", "/panel/api/status"),
    ]
}

async fn cmd_baseline(args: &Args) -> anyhow::Result<i32> {
    let evidence = require_evidence(args).map_err(anyhow::Error::msg)?;
    let oracle = GoOracle::new(go_root(args));
    let work = evidence.join("oracle-work");
    let config = OracleConfig {
        work_dir: work.clone(),
        startup_timeout: Duration::from_secs(30),
        build_packages: devin2api::qa::oracle::GO_BINARIES
            .iter()
            .map(|b| format!("./cmd/{b}"))
            .collect(),
        test_packages: vec!["./...".into()],
        requests: baseline_requests(),
    };
    let baseline = oracle.capture_baseline(&config).await?;
    let raw = serde_json::to_string_pretty(&baseline)?;
    std::fs::write(evidence.join("baseline.json"), &raw)?;

    // Negative control: the comparator must reject a mutated status.
    let rules = oracle.canon_rules(&work);
    let mut mutated = baseline.cases.clone();
    if let Some(first) = mutated.first_mut() {
        first.response.status = if first.response.status == 200 {
            503
        } else {
            200
        };
    }
    let negative = compare::compare_case_sets(&baseline.cases, &mutated, &rules);
    let negative_detected = !negative.matched();
    let self_check = compare::compare_case_sets(&baseline.cases, &baseline.cases.clone(), &rules);

    let report = serde_json::json!({
        "cases": baseline.cases.len(),
        "go_version": baseline.go_version,
        "go_commit": baseline.go_commit,
        "git_clean_before": baseline.git_clean_before,
        "git_clean_after": baseline.git_clean_after,
        "daemon_version": baseline.daemon_version,
        "daemon_exit": baseline.daemon_exit,
        "go_test": baseline.go_test_summary,
        "self_check_matched": self_check.matched(),
        "negative_control_detected": negative_detected,
        "work_dir": work.display().to_string(),
    });
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_string_pretty(&report)?,
    )?;

    let mut failures = Vec::new();
    if baseline.cases.is_empty() {
        failures.push("zero baseline cases executed");
    }
    if !baseline.git_clean_before || !baseline.git_clean_after {
        failures.push("Go reference tree is dirty");
    }
    if !self_check.matched() {
        failures.push("comparator self-check failed");
    }
    if !negative_detected {
        failures.push("negative control was not detected");
    }
    if failures.is_empty() {
        println!(
            "qa baseline: {} cases captured, negative control detected",
            baseline.cases.len()
        );
        Ok(0)
    } else {
        for f in &failures {
            eprintln!("qa baseline FAIL: {f}");
        }
        Ok(1)
    }
}

fn cmd_cli(args: &Args) -> anyhow::Result<i32> {
    let evidence = require_evidence(args).map_err(anyhow::Error::msg)?;
    let report = devin2api::qa::cli::run(&go_root(args), &evidence, args.case.as_deref())?;
    std::fs::write(
        evidence.join("qa.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    let failed: Vec<&str> = report
        .targets
        .iter()
        .filter(|t| t.status != "pass")
        .map(|t| t.name.as_str())
        .collect();
    if failed.is_empty() {
        println!("qa cli: {} targets passed", report.targets.len());
        Ok(0)
    } else {
        for f in &failed {
            eprintln!("qa cli FAIL: {f}");
        }
        Ok(1)
    }
}

fn cmd_manifest(args: &Args) -> anyhow::Result<i32> {
    let root = go_root(args);
    let manifest = contracts::scan_go_reference(&root)?;
    let json = serde_json::to_string_pretty(&manifest)? + "\n";
    let mut wrote = Vec::new();
    if let Some(out) = &args.out {
        std::fs::write(out, &json)?;
        wrote.push(out.display().to_string());
    }
    if let Some(dir) = &args.evidence {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("contracts.json");
        std::fs::write(&path, &json)?;
        wrote.push(path.display().to_string());
    }
    if wrote.is_empty() {
        // Default: refresh the committed manifest in the repo.
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/contracts.json");
        std::fs::write(&path, &json)?;
        wrote.push(path.display().to_string());
    }
    println!(
        "qa manifest: {} routes, {} config keys, {} aux commands, {} assets, {} go cases -> {}",
        manifest.routes.len(),
        manifest.config_keys.len(),
        manifest.aux_commands.len(),
        manifest.platform_assets.len(),
        manifest.go_test_cases.len(),
        wrote.join(", ")
    );
    Ok(0)
}

// The qa driver is one sequential scenario dispatcher.
#[allow(clippy::too_many_lines)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let raw_args: Vec<String> = std::env::args().skip(1).collect();
    if raw_args.first().is_some_and(|arg| arg == "__http-upstream") {
        let mode = raw_args.get(1).map_or("normal", String::as_str);
        if let Err(error) = devin2api::qa::http::upstream_child(mode).await {
            eprintln!("qa http upstream child: {error:#}");
            std::process::exit(1);
        }
        return;
    }
    if raw_args.first().is_some_and(|arg| arg == "__http-server") {
        let parsed = (|| -> anyhow::Result<_> {
            Ok((
                raw_args
                    .get(1)
                    .ok_or_else(|| anyhow::anyhow!("missing upstream"))?
                    .as_str(),
                PathBuf::from(
                    raw_args
                        .get(2)
                        .ok_or_else(|| anyhow::anyhow!("missing logs"))?,
                ),
                raw_args
                    .get(3)
                    .ok_or_else(|| anyhow::anyhow!("missing api key"))?
                    .as_str(),
                raw_args
                    .get(4)
                    .ok_or_else(|| anyhow::anyhow!("missing max concurrency"))?
                    .parse::<usize>()?,
                raw_args
                    .get(5)
                    .ok_or_else(|| anyhow::anyhow!("missing draining"))?
                    .parse::<bool>()?,
            ))
        })();
        match parsed {
            Ok((upstream, logs, api_key, max, draining)) => {
                if let Err(error) =
                    devin2api::qa::http::server_child(upstream, &logs, api_key, max, draining).await
                {
                    eprintln!("qa http server child: {error:#}");
                    std::process::exit(1);
                }
            }
            Err(error) => {
                eprintln!("qa http server child args: {error:#}");
                std::process::exit(2);
            }
        }
        return;
    }
    if raw_args.is_empty()
        || raw_args
            .iter()
            .any(|a| a == "--help" || a == "-h" || a == "help")
    {
        print_help();
        std::process::exit(if raw_args.is_empty() { 2 } else { 0 });
    }
    let args = match parse_args(&raw_args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("qa: {e}");
            print_help();
            std::process::exit(2);
        }
    };
    let Some(sub) = find_subcommand(&args.subcommand) else {
        eprintln!("qa: unknown subcommand {}", args.subcommand);
        print_help();
        std::process::exit(2);
    };
    if !sub.implemented {
        eprintln!(
            "qa: subcommand {} is planned (task {}) but not implemented yet",
            sub.name, sub.owner_task
        );
        std::process::exit(2);
    }
    let result = match sub.name {
        "baseline" => cmd_baseline(&args).await,
        "manifest" => cmd_manifest(&args),
        "http" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => devin2api::qa::http::run(&evidence, args.case.as_deref()).await,
                Err(error) => Err(error),
            }
        }
        "websocket" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => devin2api::qa::ws::run(&evidence, args.case.as_deref()).await,
                Err(error) => Err(error),
            }
        }
        "dashboard-api" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::dashboard::run(&evidence, args.case.as_deref()).await
                }
                Err(error) => Err(error),
            }
        }
        "diagnostics" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::diagnostics::run(&evidence, args.case.as_deref()).await
                }
                Err(error) => Err(error),
            }
        }
        "panel" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::panel::run(&evidence, &go_root(&args), args.case.as_deref())
                        .await
                }
                Err(error) => Err(error),
            }
        }
        "lifecycle" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => devin2api::qa::lifecycle::run(&evidence, args.case.as_deref()),
                Err(error) => Err(error),
            }
        }
        "cli" => cmd_cli(&args),
        "parity" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => devin2api::qa::parity::run_parity(&go_root(&args), &evidence).await,
                Err(error) => Err(error),
            }
        }
        "faults" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::parity::run_faults(&evidence, args.case.as_deref()).await
                }
                Err(error) => Err(error),
            }
        }
        "bench" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::bench::run(&go_root(&args), &evidence, args.case.as_deref())
                        .await
                }
                Err(error) => Err(error),
            }
        }
        "stress" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::stress::run(&evidence, args.requests.unwrap_or(100_000)).await
                }
                Err(error) => Err(error),
            }
        }
        "documented-smoke" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => devin2api::qa::docs::run(&evidence, args.case.as_deref()).await,
                Err(error) => Err(error),
            }
        }
        "packaging" | "package" => {
            let evidence = require_evidence(&args).map_err(anyhow::Error::msg);
            match evidence {
                Ok(evidence) => {
                    devin2api::qa::packaging::run(&evidence, args.case.as_deref()).await
                }
                Err(error) => Err(error),
            }
        }
        _ => unreachable!("implemented subcommand without a handler"),
    };
    match result {
        Ok(code) => std::process::exit(code),
        Err(err) => {
            eprintln!("qa {}: {err:#}", sub.name);
            std::process::exit(1);
        }
    }
}

// Keep CanonRules referenced for future subcommand wiring.
#[allow(dead_code)]
fn _rules() -> CanonRules {
    CanonRules::default()
}

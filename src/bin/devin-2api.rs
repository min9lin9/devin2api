//! Primary devin2api daemon.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use devin2api::config::{FlagError, Platform};
use devin2api::debuglog::{Manager, RetentionPolicy};
use devin2api::server::http::{App, HttpConfig};
use devin2api::server::lifecycle::{self, RuntimeConfig};
use devin2api::upstream::catalog::{Adapter, AdapterConfig};
use tokio_util::sync::CancellationToken;

fn absolute(path: &str) -> std::io::Result<PathBuf> {
    let path = Path::new(path);
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn retention(config: &devin2api::config::Config) -> RetentionPolicy {
    RetentionPolicy {
        days: config.debug.retention_days.unwrap_or_default(),
        max_total_mb: config.debug.max_total_mb.unwrap_or_default(),
        payload_hours: config.debug.payload_hours.unwrap_or_default(),
        keep_error_dirs: config.debug.keep_error_dirs.unwrap_or_default(),
    }
}

/// Warm the 60-minute trend buckets from `index.jsonl` so restart does not
/// zero the traffic/health timeline (Go seeds Metrics from `ListRequests`).
fn seed_trends(app: &App, manager: Arc<Manager>) {
    let metrics = app.metrics();
    tokio::task::spawn_blocking(move || {
        for entry in manager
            .list_requests(50000, &devin2api::debuglog::RequestFilter::default())
            .entries
        {
            let Ok(started) = entry.started_at.parse::<jiff::Timestamp>() else {
                continue;
            };
            let finished = started
                .checked_add(jiff::SignedDuration::from_millis(entry.duration_ms))
                .unwrap_or(started);
            let at = std::time::UNIX_EPOCH
                + std::time::Duration::new(
                    u64::try_from(finished.as_second()).unwrap_or(0),
                    u32::try_from(finished.subsec_nanosecond()).unwrap_or(0),
                );
            metrics.seed_trend(
                at,
                entry.status_code >= 400
                    || (!entry.result.is_empty() && entry.result != "completed"),
            );
        }
    });
}

/// Optional diagnostics listener (Go `debug.pprof_listen`): loopback-only,
/// non-fatal on bind failure, follows the reuseport handoff switch.
async fn start_diagnostics(
    config: &devin2api::config::Config,
    app: &App,
    reuse_port: bool,
    shutdown: CancellationToken,
) {
    if config.debug.pprof_listen.is_empty() {
        return;
    }
    match devin2api::metrics::DiagnosticsListener::bind_with(
        &config.debug.pprof_listen,
        app.metrics(),
        reuse_port,
    )
    .await
    {
        Ok(diagnostic) => {
            tracing::info!(addr=%config.debug.pprof_listen, "diagnostic endpoints listening");
            tokio::spawn(async move {
                if let Err(error) = diagnostic.serve(shutdown).await {
                    tracing::error!(%error, "diagnostic listener failed");
                }
            });
        }
        Err(error) => {
            tracing::error!(addr=%config.debug.pprof_listen, %error, "diagnostic listen failed");
        }
    }
}

/// Resolve and bind the configured listen address; on `EADDRINUSE` probe
/// the occupant's `/healthz` and record the holder in `bind-failure.json`
/// before returning the error (Go `reportListenFailure`).
async fn bind_or_report(
    listen: &str,
    log_root: &Path,
    reuse_port: bool,
) -> anyhow::Result<tokio::net::TcpListener> {
    // Go's net.Listen accepts ":PORT" (empty host = wildcard); Rust's
    // lookup_host rejects an empty host, so normalize it to 0.0.0.0 first.
    let normalized = if listen.starts_with(':') {
        format!("0.0.0.0{listen}")
    } else {
        listen.to_string()
    };
    let address = tokio::net::lookup_host(&normalized)
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("listen address resolved to no endpoints"))?;
    match lifecycle::bind_listener(address, reuse_port) {
        Ok(listener) => Ok(listener),
        Err(error) => {
            if error.kind() == std::io::ErrorKind::AddrInUse {
                let holder = lifecycle::probe_existing_instance(listen).await;
                let marker = lifecycle::record_bind_failure(log_root, listen, &holder)?;
                tracing::error!(addr=%marker.addr, count=marker.count, holder=%marker.holder, "port already in use");
            }
            Err(error.into())
        }
    }
}

/// Resolve `-config`/`-state-dir` into absolute paths and the logs root.
fn resolve_paths(flags: &devin2api::config::Flags) -> anyhow::Result<(PathBuf, PathBuf, PathBuf)> {
    let env = |name: &str| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned());
    let cwd = std::env::current_dir()?;
    let config_path = devin2api::config::resolve_config_path_with(
        &flags.config,
        &env,
        &cwd,
        Platform::current(),
    )?;
    let state_dir =
        devin2api::config::resolve_state_dir_with(&flags.state_dir, &env, Platform::current())?;
    let config_path = absolute(&config_path)?;
    let state_dir = absolute(&state_dir)?;
    let log_root = state_dir.join("logs");
    std::fs::create_dir_all(&log_root)?;
    Ok((config_path, state_dir, log_root))
}

async fn run() -> anyhow::Result<()> {
    let raw: Vec<String> = std::env::args().collect();
    let program = raw.first().map_or("devin-2api", String::as_str);
    let flags = match devin2api::config::parse_flags(&raw[1..]) {
        Ok(flags) => flags,
        Err(FlagError::Help) => {
            print!("{}", devin2api::config::usage_text(program));
            return Ok(());
        }
        Err(error) => {
            eprintln!("{error}");
            eprint!("{}", devin2api::config::usage_text(program));
            anyhow::bail!("invalid command line");
        }
    };
    if flags.version {
        println!("{}", lifecycle::BUILD_VERSION);
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let (config_path, state_dir, log_root) = resolve_paths(&flags)?;
    let config = devin2api::config::load(config_path.to_string_lossy().as_ref())?;
    tracing::info!(config=%config_path.display(), state_dir=%state_dir.display(), "paths resolved");

    // Bind before adapter/log replay work so handoff connections enter the backlog.
    let reuse_port = lifecycle::reuse_port_enabled();
    let listener = bind_or_report(&config.server.listen, &log_root, reuse_port).await?;
    let address = listener.local_addr()?;
    if let Some(marker) = lifecycle::mark_bind_recovered(&log_root)? {
        tracing::warn!(addr=%marker.addr, count=marker.count, holder=%marker.holder, "port contention recovered");
    }

    let manager = Arc::new(Manager::new(&log_root, &retention(&config)));
    manager.set_enabled(config.debug.enabled);
    let reload_state = Arc::new(RuntimeConfig::new(
        config_path.clone(),
        log_root.clone(),
        config.clone(),
    ));
    let token_path = config_path.clone();
    let token_source = Arc::new(move || {
        devin2api::config::load(token_path.to_string_lossy().as_ref())
            .map(|cfg| cfg.devin.token)
            .unwrap_or_default()
    });
    let adapter = Adapter::new(AdapterConfig::from_devin(
        &config.devin,
        Some(token_source),
        Some(log_root.join("gate-state.json")),
    ))?;
    let current_state = reload_state.clone();
    let reload_target = reload_state.clone();
    let app = App::with_backend(
        adapter.clone(),
        HttpConfig::from_config(
            &config,
            lifecycle::BUILD_VERSION.to_string(),
            Some(manager.clone()),
            Arc::new(move || current_state.view()),
            Arc::new(move || reload_target.reload()),
        ),
    );
    reload_state.attach(adapter, app.clone(), manager.clone());
    seed_trends(&app, manager.clone());

    let shutdown = CancellationToken::new();
    start_diagnostics(&config, &app, reuse_port, shutdown.clone()).await;

    tracing::info!(addr=%address, listen=%lifecycle::listen_url(&config.server.listen), version=lifecycle::BUILD_VERSION, reuseport=reuse_port, "HTTP server listening");

    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(error) = lifecycle::termination_signal().await {
            tracing::error!(%error, "signal listener failed");
        }
        signal_shutdown.cancel();
    });
    // SIGHUP (terminal disconnect) is not part of drain semantics.
    lifecycle::ignore_sighup();
    let drain_timeout = lifecycle::drain_timeout();
    let forced =
        lifecycle::serve_until_shutdown(app, listener, shutdown, reuse_port, drain_timeout).await?;
    if forced {
        tracing::warn!(
            timeout_seconds = drain_timeout.as_secs(),
            "shutdown drain deadline reached"
        );
    }
    // Flush the index handle and stop the cleaner before exit (Go
    // `defer debugManager.Close()`).
    manager.close();
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("devin-2api: {error:#}");
        std::process::exit(1);
    }
}

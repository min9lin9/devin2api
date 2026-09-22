//! Process lifecycle: atomic reload, bind diagnostics, signals and graceful drain.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::config::{Config, Platform, reuse_port_enabled_for};
use crate::dashboard::ConfigReloadReport;
use crate::debuglog::{Manager, RetentionPolicy, gotime};
use crate::server::http::App;
use crate::upstream::catalog::{Adapter, AdapterConfig};

/// Build-time version shared by `-version`, startup logs and `/healthz`.
pub const BUILD_VERSION: &str = env!("DEVIN2API_RESOLVED_VERSION");

/// Startup-log description for a configured listen address. Wildcard binds
/// retain the bind value and add the localhost URL operators can open; concrete
/// hosts render directly as an HTTP URL (Go `listenURL` parity).
#[must_use]
pub fn listen_url(listen: &str) -> String {
    let parsed = listen.parse::<std::net::SocketAddr>();
    if let Ok(address) = parsed {
        if address.ip().is_unspecified() {
            return format!("{listen} (http://localhost:{})", address.port());
        }
        return format!("http://{listen}");
    }
    if let Some(port) = listen
        .strip_prefix(':')
        .filter(|port| port.parse::<u16>().is_ok())
    {
        return format!("{listen} (http://localhost:{port})");
    }
    listen.to_string()
}
/// Hard upper bound for graceful request draining.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(600);
/// Persistent bind-contention marker file.
pub const BIND_FAILURE_FILE: &str = "bind-failure.json";

/// Resolve version metadata with the release/distribution/VCS/archive precedence.
/// This pure form is also used by isolated metadata-fixture QA.
#[must_use]
pub fn resolve_version(
    build: Option<&str>,
    package: Option<&str>,
    revision: Option<&str>,
    dirty: bool,
    embedded: &str,
) -> String {
    fn non_dev(value: Option<&str>) -> Option<&str> {
        value
            .map(str::trim)
            .filter(|value| !value.is_empty() && *value != "dev")
    }
    if let Some(value) = non_dev(build) {
        return value.to_string();
    }
    if let Some(value) = non_dev(package) {
        return value.to_string();
    }
    if let Some(revision) = revision.map(str::trim).filter(|value| !value.is_empty()) {
        let short: String = revision.chars().take(12).collect();
        return format!("dev-{short}{}", if dirty { "-dirty" } else { "" });
    }
    let embedded = embedded.trim();
    if embedded.is_empty() {
        "dev".to_string()
    } else {
        embedded.to_string()
    }
}

/// Whether the current process requested Unix `SO_REUSEPORT` handoff.
#[must_use]
pub fn reuse_port_enabled() -> bool {
    reuse_port_enabled_for(
        &|name| std::env::var_os(name).map(|v| v.to_string_lossy().into_owned()),
        Platform::current(),
    )
}

/// Graceful-drain deadline. The Go constant is 600s (plist
/// `ExitTimeOut`/`systemd TimeoutStopSec` are 660s, leaving ~60s for close
/// and exit). `DEVIN2API_DRAIN_TIMEOUT_MS` is a QA/ops override so the
/// bound is exercisable against a real process without waiting 10 minutes;
/// unset or unparseable keeps the 600s default.
#[must_use]
pub fn drain_timeout() -> Duration {
    std::env::var("DEVIN2API_DRAIN_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(DRAIN_TIMEOUT, Duration::from_millis)
}

/// `probeExistingInstance` — ask whoever holds `listen` whether it is a
/// devin2api instance, so an `EADDRINUSE` exit records a useful holder
/// string. Wildcard/bare-port listens are probed via loopback (Go falls
/// back to 127.0.0.1 the same way).
pub async fn probe_existing_instance(listen: &str) -> String {
    let (host, port) = listen.rsplit_once(':').unwrap_or(("", listen));
    let Ok(port) = port.parse::<u16>() else {
        return "unknown".to_string();
    };
    let host = match host.trim_matches(['[', ']']) {
        "" | "0.0.0.0" | "::" => "127.0.0.1".to_string(),
        other => other.to_string(),
    };
    let probe = async move {
        let mut stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
        stream
            .write_all(
                format!(
                    "GET /healthz HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await?;
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await?;
        Ok::<_, std::io::Error>(raw)
    };
    let Ok(raw) = tokio::time::timeout(Duration::from_secs(2), probe).await else {
        return "unresponsive".to_string();
    };
    let Ok(raw) = raw else {
        return "unresponsive".to_string();
    };
    let body = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| &raw[pos + 4..])
        .unwrap_or_default();
    let Ok(health) = serde_json::from_slice::<Value>(body) else {
        return "not devin-2api".to_string();
    };
    let version = health["version"].as_str().unwrap_or_default();
    if version.is_empty() {
        return "not devin-2api".to_string();
    }
    format!(
        "devin-2api pid={} version={} uptime={}s draining={}",
        health["pid"].as_u64().unwrap_or_default(),
        version,
        health["uptime_seconds"].as_u64().unwrap_or_default(),
        health["draining"].as_bool().unwrap_or_default(),
    )
}

/// `signal.Ignore(syscall.SIGHUP)` — terminal hangup must not kill a
/// foreground run mid-drain. Unix only; Windows has no SIGHUP.
#[cfg(unix)]
pub fn ignore_sighup() {
    if let Ok(mut hangup) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        tokio::spawn(async move { while hangup.recv().await.is_some() {} });
    }
}

/// Windows has no SIGHUP; the Ctrl+C drain path is the equivalent.
#[cfg(not(unix))]
pub fn ignore_sighup() {}

/// Create, configure and bind the production TCP listener.
pub fn bind_listener(
    addr: SocketAddr,
    reuse_port: bool,
) -> std::io::Result<tokio::net::TcpListener> {
    let socket = new_tcp_socket(addr)?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(windows)]
    let _ = reuse_port;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    tokio::net::TcpListener::from_std(socket.into())
}

/// Create a TCP socket of the correct address family.
pub fn new_tcp_socket(addr: SocketAddr) -> std::io::Result<socket2::Socket> {
    let domain = if addr.is_ipv6() {
        socket2::Domain::IPV6
    } else {
        socket2::Domain::IPV4
    };
    socket2::Socket::new(domain, socket2::Type::STREAM, Some(socket2::Protocol::TCP))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindFailureMarker {
    pub first_at: String,
    pub last_at: String,
    pub count: u64,
    pub addr: String,
    pub holder: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub recovered_at: String,
}

/// `time.Now().UTC().Format(time.RFC3339)` — marker timestamps.
fn now() -> String {
    gotime::rfc3339(&gotime::now().with_time_zone(jiff::tz::TimeZone::UTC))
}

/// `time.Now().Format(time.RFC3339)` — reload report and load timestamps
/// keep the local zone like the Go service.
fn now_local() -> String {
    gotime::rfc3339(&gotime::now())
}

/// `fileMtime.Format(time.RFC3339)`; a failed stat is the Go zero time.
fn format_mtime(mtime: Option<SystemTime>) -> String {
    mtime.map_or_else(
        || "0001-01-01T00:00:00Z".to_string(),
        |mtime| {
            let timestamp = jiff::Timestamp::try_from(mtime).unwrap_or(jiff::Timestamp::UNIX_EPOCH);
            gotime::rfc3339(&timestamp.to_zoned(jiff::tz::TimeZone::system()))
        },
    )
}

/// Atomically update the cumulative bind collision record.
pub fn record_bind_failure(
    log_root: &Path,
    addr: &str,
    holder: &str,
) -> std::io::Result<BindFailureMarker> {
    std::fs::create_dir_all(log_root)?;
    let path = log_root.join(BIND_FAILURE_FILE);
    let mut marker: BindFailureMarker = std::fs::read(&path)
        .ok()
        .and_then(|raw| serde_json::from_slice(&raw).ok())
        .unwrap_or_default();
    let at = now();
    if marker.count == 0 {
        marker.first_at.clone_from(&at);
    }
    marker.last_at = at;
    marker.count += 1;
    marker.addr = addr.to_string();
    marker.holder = holder.to_string();
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(&marker)?)?;
    std::fs::rename(temporary, path)?;
    Ok(marker)
}

/// Mark previously recorded contention as recovered, once per collision series.
pub fn mark_bind_recovered(log_root: &Path) -> std::io::Result<Option<BindFailureMarker>> {
    let path = log_root.join(BIND_FAILURE_FILE);
    let Ok(raw) = std::fs::read(&path) else {
        return Ok(None);
    };
    let mut marker: BindFailureMarker = serde_json::from_slice(&raw)?;
    if marker.count == 0
        || (!marker.recovered_at.is_empty() && marker.last_at <= marker.recovered_at)
    {
        return Ok(None);
    }
    marker.recovered_at = now();
    let temporary = path.with_extension("json.tmp");
    std::fs::write(&temporary, serde_json::to_vec(&marker)?)?;
    std::fs::rename(temporary, path)?;
    Ok(Some(marker))
}

fn policy(config: &Config) -> RetentionPolicy {
    RetentionPolicy {
        days: config.debug.retention_days.unwrap_or_default(),
        max_total_mb: config.debug.max_total_mb.unwrap_or_default(),
        payload_hours: config.debug.payload_hours.unwrap_or_default(),
        keep_error_dirs: config.debug.keep_error_dirs.unwrap_or_default(),
    }
}

struct RuntimeState {
    config: Config,
    loaded_at: String,
    file_mtime: Option<SystemTime>,
    last_reload: Option<ConfigReloadReport>,
}

/// Validate-then-commit owner for every hot-reloadable runtime field.
pub struct RuntimeConfig {
    path: PathBuf,
    log_root: PathBuf,
    state: Mutex<RuntimeState>,
    targets: Mutex<Option<(Adapter, App, Arc<Manager>)>>,
}

impl RuntimeConfig {
    #[must_use]
    pub fn new(path: PathBuf, log_root: PathBuf, config: Config) -> Self {
        let file_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        Self {
            path,
            log_root,
            state: Mutex::new(RuntimeState {
                config,
                loaded_at: now_local(),
                file_mtime,
                last_reload: None,
            }),
            targets: Mutex::new(None),
        }
    }

    pub fn attach(&self, adapter: Adapter, app: App, manager: Arc<Manager>) {
        *self
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((adapter, app, manager));
    }

    #[must_use]
    pub fn config(&self) -> Config {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .config
            .clone()
    }

    /// Load and fully validate first; no runtime holder changes on any
    /// error. The whole load→validate→commit sequence runs under `state`
    /// (Go `reloadMu`): two concurrent reloads cannot interleave a stale
    /// snapshot over a newer commit.
    pub fn reload(&self) -> Result<ConfigReloadReport, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next =
            crate::config::load(self.path.to_string_lossy().as_ref()).map_err(|e| e.to_string())?;
        if next.devin.model.trim().is_empty() || next.devin.base_url.trim().is_empty() {
            return Err("devin.model and devin.base_url must be non-empty".to_string());
        }
        let targets = self
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (adapter, app, manager) = targets.as_ref().ok_or("runtime targets not attached")?;
        let previous = &state.config;
        let gate_path = Some(self.log_root.join("gate-state.json"));
        // The credential re-read callback reloads this same file, matching
        // `devinConfigFrom`'s TokenSource — dropping it here would sever
        // the unauthenticated self-heal chain after the first reload.
        let token_path = self.path.clone();
        let token_source = std::sync::Arc::new(move || {
            crate::config::load(token_path.to_string_lossy().as_ref())
                .map(|cfg| cfg.devin.token)
                .unwrap_or_default()
        });
        let (mut applied, mut requires_restart) = adapter.apply_config(AdapterConfig::from_devin(
            &next.devin,
            Some(token_source),
            gate_path,
        ));
        if previous.auth.api_key != next.auth.api_key {
            app.set_api_key(next.auth.api_key.clone());
            applied.push("auth.api_key".to_string());
        }
        if previous.dashboard.password != next.dashboard.password {
            app.set_dashboard_password(next.dashboard.password.clone());
            applied.push("dashboard.password".to_string());
        }
        if previous.debug.enabled != next.debug.enabled {
            manager.set_enabled(next.debug.enabled);
            applied.push("debug.enabled".to_string());
        }
        let old_policy = policy(previous);
        let new_policy = policy(&next);
        if old_policy.days != new_policy.days
            || old_policy.max_total_mb != new_policy.max_total_mb
            || old_policy.payload_hours != new_policy.payload_hours
            || old_policy.keep_error_dirs != new_policy.keep_error_dirs
        {
            manager.set_policy(new_policy);
            applied.push("debug.retention".to_string());
        }
        if previous.debug.quota_interval_minutes != next.debug.quota_interval_minutes {
            requires_restart.push("debug.quota_interval_minutes".to_string());
        }
        if previous.debug.pprof_listen != next.debug.pprof_listen {
            requires_restart.push("debug.pprof_listen".to_string());
        }
        if previous.server.listen != next.server.listen {
            requires_restart.push("server.listen".to_string());
        }
        if previous.server.max_concurrency != next.server.max_concurrency {
            requires_restart.push("server.max_concurrency".to_string());
        }
        let report = ConfigReloadReport {
            at: now_local(),
            applied,
            requires_restart,
        };
        state.config = next;
        state.loaded_at = now_local();
        state.file_mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        state.last_reload = Some(report.clone());
        Ok(report)
    }

    #[must_use]
    pub fn view(&self) -> Value {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let current_mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        json!({
            "path": self.path,
            "config": state.config.redacted_view(),
            "loaded_at": state.loaded_at,
            "file_mtime": format_mtime(state.file_mtime),
            "stale": current_mtime > state.file_mtime,
            "last_reload": state.last_reload,
        })
    }
}

/// Run until shutdown, then refuse new inference and await active requests.
/// In reuseport mode accept stops immediately, releasing the socket to the
/// handoff peer; otherwise it remains open so new requests receive HTTP 503.
pub async fn serve_until_shutdown(
    app: App,
    listener: tokio::net::TcpListener,
    shutdown: CancellationToken,
    reuse_port: bool,
    drain_timeout: Duration,
) -> std::io::Result<bool> {
    let accept_stop = CancellationToken::new();
    let serve_app = app.clone();
    let serve_stop = accept_stop.clone();
    let mut serving = tokio::spawn(async move { serve_app.serve(listener, serve_stop).await });
    tokio::select! {
        result = &mut serving => return result.map_err(std::io::Error::other)?.map(|()| false),
        () = shutdown.cancelled() => {}
    }
    // Publish the draining state before the readiness log. QA and operators
    // use this line as the transition signal, so logging first creates a
    // race where /healthz can still report draining=false.
    app.begin_drain();
    tracing::info!(
        timeout_seconds = drain_timeout.as_secs(),
        "shutdown: draining in-flight requests"
    );
    if reuse_port {
        // Handoff: release the socket immediately so every new connection
        // lands on the reuseport peer (Go closes the listener here). The
        // accept task owns the listener, so the log line waits for its
        // exit — only then is the socket actually closed.
        accept_stop.cancel();
        match (&mut serving).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(std::io::Error::other(error)),
        }
        tracing::info!("shutdown: listener released for handoff");
    }
    let forced = tokio::time::timeout(drain_timeout, app.wait_idle())
        .await
        .is_err();
    if forced {
        tracing::warn!("shutdown: drain timed out, closing remaining connections");
    }
    accept_stop.cancel();
    // Go `server.Close()`: whatever is still open after the drain window
    // is force-closed, cancelling in-flight upstream work with it.
    app.close_connections();
    if !reuse_port {
        serving.await.map_err(std::io::Error::other)??;
    }
    Ok(forced)
}

/// Arm the platform termination source. Unix handles SIGTERM and Ctrl+C;
/// Windows uses Ctrl+C without claiming reuseport support.
pub async fn termination_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { _ = term.recv() => {}, result = tokio::signal::ctrl_c() => result? }
    }
    #[cfg(windows)]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

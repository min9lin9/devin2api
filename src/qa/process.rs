//! Managed child processes for QA runs: log capture, readiness waits with
//! bounded timeouts, and guaranteed cleanup (SIGTERM then SIGKILL).

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

/// Poll interval for readiness probes. This is a poll cadence for an
/// external process signal, not a correctness sleep: the wait returns as
/// soon as the endpoint answers or the child exits.
const READY_POLL: Duration = Duration::from_millis(50);

/// A spawned child whose stdout/stderr are captured to files under the QA
/// work directory and which is always reaped.
pub struct ManagedChild {
    name: String,
    child: Child,
    pid: u32,
    /// Directory holding `<name>.stdout.log` / `<name>.stderr.log`.
    log_dir: PathBuf,
}

impl ManagedChild {
    /// OS pid of the child.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Path of the captured stderr log.
    pub fn stderr_log(&self) -> PathBuf {
        self.log_dir.join(format!("{}.stderr.log", self.name))
    }

    /// Path of the captured stdout log.
    pub fn stdout_log(&self) -> PathBuf {
        self.log_dir.join(format!("{}.stdout.log", self.name))
    }

    /// Graceful shutdown: SIGTERM, wait up to `grace`, escalate to SIGKILL.
    /// Always reaps the child. Returns the exit description.
    pub async fn shutdown(&mut self, grace: Duration) -> std::io::Result<String> {
        send_signal(self.pid, "TERM");
        let deadline = Instant::now() + grace;
        loop {
            match self.child.try_wait()? {
                Some(status) => return Ok(format!("{status}")),
                None if Instant::now() >= deadline => break,
                None => tokio::time::sleep(READY_POLL).await,
            }
        }
        self.child.kill()?;
        let status = self.child.wait()?;
        Ok(format!("{status} (killed)"))
    }

    /// Kill immediately and reap.
    pub fn kill(&mut self) -> std::io::Result<()> {
        self.child.kill()?;
        self.child.wait().map(|_| ())
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        // Best-effort kill+reap; explicit shutdown() is the normal path —
        // this is the panic safety net.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn `command` with stdout/stderr appended to `<log_dir>/<name>.{stdout,stderr}.log`.
/// The command's stdin is null. Returns the managed child.
pub fn spawn_logged(
    log_dir: &Path,
    name: &str,
    command: &mut std::process::Command,
) -> std::io::Result<ManagedChild> {
    std::fs::create_dir_all(log_dir)?;
    let stdout = std::fs::File::create(log_dir.join(format!("{name}.stdout.log")))?;
    let stderr = std::fs::File::create(log_dir.join(format!("{name}.stderr.log")))?;
    let child = command
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()?;
    let pid = child.id();
    Ok(ManagedChild {
        name: name.to_string(),
        child,
        pid,
        log_dir: log_dir.to_path_buf(),
    })
}

/// Wait until `url` answers any HTTP response, the child exits, or
/// `timeout` elapses. Errors are descriptive and always bounded.
pub async fn wait_ready(
    child: &mut ManagedChild,
    url: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.child.try_wait()? {
            let stderr = std::fs::read_to_string(child.stderr_log()).unwrap_or_default();
            anyhow::bail!(
                "child {} exited before ready ({status}); stderr tail: {}",
                child.name,
                tail(&stderr, 2000)
            );
        }
        match client.get(url).send().await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() >= deadline => {
                let stderr = std::fs::read_to_string(child.stderr_log()).unwrap_or_default();
                anyhow::bail!(
                    "timeout after {timeout:?} waiting for {} at {url}; stderr tail: {}",
                    child.name,
                    tail(&stderr, 2000)
                );
            }
            Err(_) => tokio::time::sleep(READY_POLL).await,
        }
    }
}

/// Wait until a TCP connect to `127.0.0.1:port` succeeds, the child exits,
/// or `timeout` elapses. Used for services without an HTTP readiness route.
pub async fn wait_tcp(
    child: &mut ManagedChild,
    port: u16,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.child.try_wait()? {
            let stderr = std::fs::read_to_string(child.stderr_log()).unwrap_or_default();
            anyhow::bail!(
                "child {} exited before ready ({status}); stderr tail: {}",
                child.name,
                tail(&stderr, 2000)
            );
        }
        match tokio::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() >= deadline => {
                let stderr = std::fs::read_to_string(child.stderr_log()).unwrap_or_default();
                anyhow::bail!(
                    "timeout after {timeout:?} waiting for {} on port {port}; stderr tail: {}",
                    child.name,
                    tail(&stderr, 2000)
                );
            }
            Err(_) => tokio::time::sleep(READY_POLL).await,
        }
    }
}

/// Allocate an ephemeral loopback port via bind(0). The caller spawns the
/// service immediately after; the brief release window is inherent to
/// port-by-probe allocation.
pub fn free_port() -> std::io::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Whether a process with `pid` currently exists (Unix: `kill -0`).
pub fn pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

fn send_signal(pid: u32, signal: &str) {
    let _ = std::process::Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn tail(text: &str, max: usize) -> &str {
    if text.len() <= max {
        text
    } else {
        &text[text.len() - max..]
    }
}

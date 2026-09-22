//! Disposable lifecycle process and metadata QA.
//!
//! Drives the real `devin-2api` binary: config hot reload through the
//! panel endpoint, graceful drain of a gated in-flight stream, `SO_REUSEPORT`
//! handoff between two daemons, bind-conflict holder recording and the
//! bounded drain deadline. All synchronization is event-driven — stderr
//! log lines, upstream gate events and process exits; nothing sleeps.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

fn write_json(path: &Path, value: &Value) -> anyhow::Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

fn checked(command: &mut Command) -> anyhow::Result<std::process::Output> {
    let display = format!("{command:?}");
    let output = command.output()?;
    anyhow::ensure!(
        output.status.success(),
        "command failed: {display}\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output)
}

fn go_binary() -> PathBuf {
    if let Ok(path) = std::env::var("QA_GO_BIN") {
        return PathBuf::from(path);
    }
    let sdk = PathBuf::from(std::env::var("HOME").unwrap_or_default()).join("sdk/go/bin/go");
    if sdk.is_file() {
        sdk
    } else {
        PathBuf::from("go")
    }
}

fn git(dir: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = checked(Command::new("git").args(args).current_dir(dir))?;
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

// One fixture writer per version-precedence branch; the table reads
// as a unit.
#[allow(clippy::too_many_lines)]
fn metadata_fixture(
    root: &Path,
    name: &str,
    build: Option<&str>,
    package: Option<&str>,
    vcs: bool,
    dirty: bool,
    embedded: Option<&str>,
) -> anyhow::Result<Value> {
    let dir = root.join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("build.rs"),
        dir.join("build.rs"),
    )?;
    if let Some(value) = embedded {
        std::fs::write(dir.join("VERSION"), value)?;
    }
    let revision = if vcs {
        git(&dir, &["init", "-q"])?;
        git(&dir, &["config", "user.email", "qa@example.invalid"])?;
        git(&dir, &["config", "user.name", "Lifecycle QA"])?;
        std::fs::write(dir.join("tracked"), "clean\n")?;
        git(&dir, &["add", "tracked"])?;
        git(&dir, &["commit", "-q", "-m", "fixture"])?;
        let revision = git(&dir, &["rev-parse", "HEAD"])?;
        if dirty {
            std::fs::write(dir.join("tracked"), "dirty\n")?;
        }
        Some(revision)
    } else {
        None
    };

    checked(
        Command::new("rustc")
            .args(["build.rs", "-o", "build-script"])
            .current_dir(&dir),
    )?;
    let mut script = Command::new(dir.join("build-script"));
    script
        .current_dir(&dir)
        .env("CARGO_MANIFEST_DIR", &dir)
        .env_remove("DEVIN2API_BUILD_VERSION")
        .env_remove("DEVIN2API_PACKAGE_VERSION");
    if let Some(value) = build {
        script.env("DEVIN2API_BUILD_VERSION", value);
    }
    if let Some(value) = package {
        script.env("DEVIN2API_PACKAGE_VERSION", value);
    }
    let output = String::from_utf8(checked(&mut script)?.stdout)?;
    let resolved = output
        .lines()
        .find_map(|line| line.strip_prefix("cargo:rustc-env=DEVIN2API_RESOLVED_VERSION="))
        .ok_or_else(|| anyhow::anyhow!("build.rs emitted no version"))?
        .to_string();
    std::fs::write(
        dir.join("main.rs"),
        "fn main(){println!(\"{}\",env!(\"DEVIN2API_RESOLVED_VERSION\"));}\n",
    )?;
    checked(
        Command::new("rustc")
            .args(["main.rs", "-o", "rust-resolver"])
            .env("DEVIN2API_RESOLVED_VERSION", &resolved)
            .current_dir(&dir),
    )?;
    let rust = String::from_utf8(checked(&mut Command::new(dir.join("rust-resolver")))?.stdout)?
        .trim()
        .to_string();

    std::fs::write(
        dir.join("oracle.go"),
        r#"package main
import("fmt";"strings")
var buildVersion="dev"; var packageVersion,revision,modified,embedded string
func main(){if buildVersion!=""&&buildVersion!="dev"{fmt.Println(buildVersion);return};if packageVersion!=""&&packageVersion!="(devel)"&&packageVersion!="dev"{fmt.Println(packageVersion);return};if revision!=""{if len(revision)>12{revision=revision[:12]};if modified=="true"{revision+="-dirty"};fmt.Println("dev-"+revision);return};if v:=strings.TrimSpace(embedded);v!=""{fmt.Println(v);return};fmt.Println("dev")}
"#,
    )?;
    let mut flags = Vec::new();
    if let Some(value) = build {
        flags.push(format!("-X main.buildVersion={value}"));
    }
    if let Some(value) = package {
        flags.push(format!("-X main.packageVersion={value}"));
    }
    if let Some(value) = &revision {
        flags.push(format!("-X main.revision={value}"));
    }
    if dirty {
        flags.push("-X main.modified=true".into());
    }
    if let Some(value) = embedded {
        flags.push(format!("-X main.embedded={}", value.trim()));
    }
    checked(
        Command::new(go_binary())
            .args([
                "build",
                "-o",
                "go-resolver",
                "-ldflags",
                &flags.join(" "),
                "oracle.go",
            ])
            .current_dir(&dir),
    )?;
    let go = String::from_utf8(checked(&mut Command::new(dir.join("go-resolver")))?.stdout)?
        .trim()
        .to_string();
    let result = json!({"name":name,"inputs":{"build":build,"package":package,"revision":revision,"dirty":dirty,"embedded":embedded},"rust":rust,"go":go,"matched":rust==go,"rust_binary":dir.join("rust-resolver"),"go_binary":dir.join("go-resolver")});
    write_json(&dir.join("result.json"), &result)?;
    Ok(result)
}

fn version_cases(evidence: &Path) -> anyhow::Result<Value> {
    let dir = evidence.join("version");
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;
    let cases = vec![
        metadata_fixture(
            &dir,
            "explicit",
            Some("v9.0.0"),
            Some("v8.0.0"),
            true,
            false,
            Some("v7\n"),
        )?,
        metadata_fixture(
            &dir,
            "distribution-over-vcs",
            Some("dev"),
            Some("v8.0.0"),
            true,
            false,
            Some("v7\n"),
        )?,
        metadata_fixture(&dir, "dirty-vcs", None, None, true, true, Some("v7\n"))?,
        metadata_fixture(
            &dir,
            "source-archive",
            None,
            None,
            false,
            false,
            Some(" v7.0.0\n"),
        )?,
        metadata_fixture(&dir, "bare-dev", None, None, false, false, None)?,
    ];
    let report = json!({"cases":cases,"all_matched":cases.iter().all(|v|v["matched"]==true)});
    write_json(&dir.join("summary.json"), &report)?;
    Ok(report)
}

fn build_daemon(evidence: &Path) -> anyhow::Result<PathBuf> {
    // `cargo run --bin qa` only guarantees that the QA binary is current.
    // Build the product binary in this invocation before any process test so
    // stale target/debug/devin-2api bytes can never produce a false pass.
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    checked(
        Command::new("cargo")
            .args(["build", "--locked", "--bin", "devin-2api"])
            .env("CARGO_BUILD_JOBS", "2")
            .env_remove("DEVIN2API_BUILD_VERSION")
            .env_remove("DEVIN2API_PACKAGE_VERSION")
            .current_dir(manifest),
    )?;
    let binary = manifest.join("target/debug").join(if cfg!(windows) {
        "devin-2api.exe"
    } else {
        "devin-2api"
    });
    let metadata = std::fs::metadata(&binary)?;
    anyhow::ensure!(metadata.is_file(), "fresh daemon build is not a file");
    write_json(
        &evidence.join("daemon-build.json"),
        &json!({
            "command":"cargo build --locked --bin devin-2api",
            "path":binary,
            "bytes":metadata.len(),
            "freshly_built":true
        }),
    )?;
    Ok(binary)
}

#[cfg(unix)]
mod imp {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::sync::mpsc::{self, Receiver};
    use std::time::{Duration, Instant};

    use anyhow::Context as _;
    use serde_json::Value;

    const EVENT_TIMEOUT: Duration = Duration::from_secs(20);

    pub fn free_port() -> anyhow::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        Ok(listener.local_addr()?.port())
    }

    pub fn signal(pid: u32, sig: &str) -> anyhow::Result<()> {
        let status = Command::new("/bin/kill")
            .args([format!("-{sig}"), pid.to_string()])
            .status()?;
        anyhow::ensure!(status.success(), "kill -{sig} {pid} failed");
        Ok(())
    }

    /// A spawned child with piped stdout+stderr folded into one line
    /// channel (stderr also mirrored to a log file). Events are pre-armed
    /// waits on lines, never sleeps.
    pub struct Proc {
        child: Child,
        pub pid: u32,
        lines: Receiver<String>,
        stdin: Option<std::process::ChildStdin>,
    }

    impl Proc {
        /// Spawn `cmd` with a scrubbed environment (no inherited
        /// credentials or config paths) plus `env` overrides.
        pub fn spawn(
            cmd: &Path,
            args: &[String],
            env: &[(&str, &str)],
            stderr_log: &Path,
        ) -> anyhow::Result<Self> {
            let mut command = Command::new(cmd);
            command
                .args(args)
                .env_clear()
                .env("PATH", std::env::var("PATH").unwrap_or_default())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            for (key, value) in env {
                command.env(key, value);
            }
            let mut child = command.spawn()?;
            let pid = child.id();
            let stdin = child.stdin.take();
            let (tx, lines) = mpsc::sync_channel(256);
            let stdout = child.stdout.take().expect("piped stdout");
            let stderr = child.stderr.take().expect("piped stderr");
            let log_path = stderr_log.to_path_buf();
            let log_tx = tx.clone();
            std::thread::spawn(move || {
                let mut log = std::fs::File::create(&log_path).ok();
                for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                    if let Some(file) = &mut log {
                        let _ = writeln!(file, "{line}");
                    }
                    if log_tx.send(line).is_err() {
                        break;
                    }
                }
            });
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
            });
            Ok(Self {
                child,
                pid,
                lines,
                stdin,
            })
        }

        /// Wait for one line containing `needle` (bounded).
        pub fn wait_line(&self, needle: &str) -> anyhow::Result<String> {
            let deadline = Instant::now() + EVENT_TIMEOUT;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                anyhow::ensure!(!remaining.is_zero(), "timed out waiting for {needle:?}");
                let line = self
                    .lines
                    .recv_timeout(remaining)
                    .context("child output ended while waiting")?;
                if line.contains(needle) {
                    return Ok(line);
                }
            }
        }

        pub fn send_line(&mut self, line: &str) -> anyhow::Result<()> {
            let stdin = self.stdin.as_mut().expect("child stdin");
            stdin.write_all(line.as_bytes())?;
            stdin.write_all(b"\n")?;
            stdin.flush()?;
            Ok(())
        }

        /// Wait for process exit (bounded).
        pub fn wait_exit(&mut self, bound: Duration) -> anyhow::Result<ExitStatus> {
            let deadline = Instant::now() + bound;
            loop {
                if let Some(status) = self.child.try_wait()? {
                    return Ok(status);
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "process {} did not exit within {bound:?}",
                    self.pid
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        pub fn kill(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    impl Drop for Proc {
        fn drop(&mut self) {
            self.kill();
        }
    }

    pub struct Daemon {
        pub proc: Proc,
        pub config: PathBuf,
    }

    pub fn daemon_config(
        dir: &Path,
        port: u16,
        upstream: &str,
        api_key: &str,
        password: &str,
    ) -> PathBuf {
        std::fs::create_dir_all(dir.join("state")).expect("daemon dir");
        let config = dir.join("config.yaml");
        std::fs::write(
            &config,
            format!(
                "server:\n  listen: '127.0.0.1:{port}'\ndevin:\n  base_url: '{upstream}'\n  token: synthetic\n  model: qa-model\ndebug:\n  enabled: false\ndashboard:\n  password: {password}\nauth:\n  api_key: {api_key}\n"
            ),
        )
        .expect("write config");
        config
    }

    pub fn spawn_daemon(
        binary: &Path,
        dir: &Path,
        name: &str,
        reuseport: bool,
        extra_env: &[(&str, &str)],
    ) -> anyhow::Result<Daemon> {
        let config = dir.join("config.yaml");
        let state = dir.join("state");
        let mut env = vec![("DEVIN2API_REUSEPORT", if reuseport { "1" } else { "0" })];
        env.extend_from_slice(extra_env);
        let proc = Proc::spawn(
            binary,
            &[
                "-config".to_string(),
                config.to_str().unwrap().to_string(),
                "-state-dir".to_string(),
                state.to_str().unwrap().to_string(),
            ],
            &env,
            &dir.join(format!("{name}.stderr.log")),
        )?;
        Ok(Daemon { proc, config })
    }

    /// Blocking HTTP/1.1 request; reads until EOF, a terminator substring,
    /// or the bound — whichever comes first.
    pub fn raw_request(
        port: u16,
        request: &str,
        terminators: &[&str],
        read_bound: Duration,
    ) -> anyhow::Result<String> {
        let mut stream = std::net::TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(read_bound))?;
        stream.write_all(request.as_bytes())?;
        let mut data = Vec::new();
        let mut buf = [0_u8; 8192];
        loop {
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    data.extend_from_slice(&buf[..n]);
                    if terminators
                        .iter()
                        .any(|t| data.windows(t.len()).any(|w| w == t.as_bytes()))
                    {
                        break;
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(String::from_utf8_lossy(&data).into_owned())
    }

    pub fn get_healthz(port: u16) -> anyhow::Result<Value> {
        let response = raw_request(
            port,
            "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
            &[],
            Duration::from_secs(5),
        )?;
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .ok_or_else(|| anyhow::anyhow!("malformed healthz response: {response}"))?;
        Ok(serde_json::from_str(body)?)
    }

    pub fn post_reload(port: u16, password: &str) -> anyhow::Result<String> {
        raw_request(
            port,
            &format!(
                "POST /panel/api/config/reload HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {password}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
            &[],
            Duration::from_secs(5),
        )
    }

    fn chat_request(api_key: &str) -> String {
        let body =
            r#"{"model":"qa-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#;
        format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {api_key}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// A streaming chat request on its own thread; the raw response is
    /// delivered on the channel once `[DONE]` arrives or the connection
    /// closes.
    pub fn spawn_chat(port: u16, api_key: &str) -> Receiver<String> {
        let request = chat_request(api_key);
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let result = raw_request(
                port,
                &request,
                &["[DONE]", "server_draining"],
                Duration::from_secs(30),
            )
            .unwrap_or_else(|e| format!("error: {e}"));
            let _ = tx.send(result);
        });
        rx
    }

    pub fn recv_bounded<T>(rx: &Receiver<T>, bound: Duration) -> anyhow::Result<T> {
        rx.recv_timeout(bound)
            .map_err(|_| anyhow::anyhow!("timed out waiting for channel event"))
    }

    /// The gated upstream child (`qa __http-upstream gate`): every chat
    /// request prints REQUEST then holds the response until RELEASE.
    pub struct Upstream {
        pub proc: Proc,
        pub addr: String,
    }

    /// `current_exe` after an in-place rebuild reports `<path> (deleted)`;
    /// the fresh binary lives at the same path, so strip the marker.
    fn self_binary() -> anyhow::Result<PathBuf> {
        let exe = std::env::current_exe()?;
        let text = exe.to_string_lossy();
        Ok(text
            .strip_suffix(" (deleted)")
            .map_or_else(|| exe.clone(), PathBuf::from))
    }

    pub fn spawn_gated_upstream(dir: &Path, name: &str) -> anyhow::Result<Upstream> {
        let qa = self_binary()?;
        let proc = Proc::spawn(
            &qa,
            &["__http-upstream".to_string(), "gate".to_string()],
            &[],
            &dir.join(format!("{name}.stderr.log")),
        )?;
        let ready = proc.wait_line("READY ")?;
        let addr = ready
            .strip_prefix("READY ")
            .ok_or_else(|| anyhow::anyhow!("bad upstream readiness line: {ready}"))?
            .trim()
            .to_string();
        Ok(Upstream { proc, addr })
    }

    /// Happy path: reload applies config, drain completes the in-flight
    /// stream, handoff transfers the port to a second daemon.
    // One sequential lifecycle drill; splitting scatters the
    // reload/drain/handoff narrative.
    #[allow(clippy::too_many_lines)]
    pub fn happy_path(binary: &Path, evidence: &Path) -> anyhow::Result<Value> {
        let dir = evidence.join("happy");
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let mut upstream = spawn_gated_upstream(&dir, "upstream")?;
        let upstream_url = format!("http://{}", upstream.addr);
        let port = free_port()?;

        // --- Daemon A: start, healthz, gated in-flight stream. ---
        let dir_a = dir.join("a");
        daemon_config(&dir_a, port, &upstream_url, "key-old", "pw-old");
        let mut a = spawn_daemon(binary, &dir_a, "a", false, &[])?;
        a.proc.wait_line("HTTP server listening")?;
        let health_a = get_healthz(port)?;
        let pid_a = health_a["pid"].as_u64().unwrap_or_default();
        anyhow::ensure!(pid_a == u64::from(a.proc.pid), "healthz pid mismatch");
        let flag_version =
            String::from_utf8(Command::new(binary).arg("-version").output()?.stdout)?
                .trim()
                .to_string();
        let versions_agree = health_a["version"].as_str() == Some(flag_version.as_str());

        let inflight = spawn_chat(port, "key-old");
        upstream.proc.wait_line("REQUEST")?;

        // --- Hot reload: new api key + panel password take effect live.
        // The reload request itself still authenticates with the OLD
        // password (rotation lands inside the commit).
        daemon_config(&dir_a, port, &upstream_url, "key-new", "pw-new");
        let reload = post_reload(port, "pw-old")?;
        anyhow::ensure!(reload.contains("200"), "reload failed: {reload}");
        let reload_body = reload.split_once("\r\n\r\n").map_or("", |(_, b)| b);
        let report: Value = serde_json::from_str(reload_body)?;
        anyhow::ensure!(
            report["applied"]
                .as_array()
                .is_some_and(|a| a.iter().any(|v| v == "auth.api_key")),
            "auth.api_key not applied: {report}"
        );
        // Old key rejected, new key admitted — auth updated without restart.
        let old_key = raw_request(
            port,
            "GET /v1/models HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer key-old\r\nConnection: close\r\n\r\n",
            &[],
            Duration::from_secs(5),
        )?;
        let new_key = raw_request(
            port,
            "GET /v1/models HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer key-new\r\nConnection: close\r\n\r\n",
            &[],
            Duration::from_secs(5),
        )?;
        anyhow::ensure!(old_key.contains("401"), "old key still works: {old_key}");
        anyhow::ensure!(new_key.contains("200"), "new key rejected: {new_key}");

        // --- Graceful drain: SIGTERM, in-flight completes, new work 503s.
        signal(a.proc.pid, "TERM")?;
        a.proc.wait_line("draining in-flight requests")?;
        let health_draining = get_healthz(port)?;
        anyhow::ensure!(
            health_draining["draining"] == true,
            "healthz not draining: {health_draining}"
        );
        let refused = raw_request(
            port,
            &chat_request("key-new"),
            &["server_draining"],
            Duration::from_secs(5),
        )?;
        anyhow::ensure!(refused.contains("503"), "drain did not refuse: {refused}");
        anyhow::ensure!(
            refused.contains("server_draining"),
            "missing drain code: {refused}"
        );
        upstream.proc.send_line("RELEASE")?;
        upstream.proc.wait_line("RELEASED")?;
        let inflight_response = recv_bounded(&inflight, EVENT_TIMEOUT)?;
        anyhow::ensure!(
            inflight_response.contains("200") && inflight_response.contains("[DONE]"),
            "in-flight request dropped: {inflight_response}"
        );
        let exit_a = a.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(exit_a.success(), "daemon A exit: {exit_a}");

        // --- Reuseport handoff: A holds a gated stream, B binds the same
        // port, SIGTERM releases A's listener and B takes new connections
        // while the old stream still completes.
        let dir_h = dir.join("handoff");
        let port_h = free_port()?;
        let dir_old = dir_h.join("a");
        daemon_config(&dir_old, port_h, &upstream_url, "key-new", "pw-new");
        let mut old = spawn_daemon(binary, &dir_old, "handoff-a", true, &[])?;
        old.proc.wait_line("HTTP server listening")?;
        let pid_old = get_healthz(port_h)?["pid"].as_u64().unwrap_or_default();
        // The in-flight stream starts while A is the only listener, so it
        // is guaranteed to land on A.
        let handoff_inflight = spawn_chat(port_h, "key-new");
        upstream.proc.wait_line("REQUEST")?;
        let dir_new = dir_h.join("b");
        daemon_config(&dir_new, port_h, &upstream_url, "key-new", "pw-new");
        let mut new = spawn_daemon(binary, &dir_new, "handoff-b", true, &[])?;
        new.proc.wait_line("HTTP server listening")?;

        signal(old.proc.pid, "TERM")?;
        old.proc.wait_line("listener released for handoff")?;
        let health_b = get_healthz(port_h)?;
        let pid_b = health_b["pid"].as_u64().unwrap_or_default();
        anyhow::ensure!(
            pid_b == u64::from(new.proc.pid) && pid_b != pid_old,
            "handoff did not transfer: {health_b}"
        );
        upstream.proc.send_line("RELEASE")?;
        upstream.proc.wait_line("RELEASED")?;
        let handoff_response = recv_bounded(&handoff_inflight, EVENT_TIMEOUT)?;
        anyhow::ensure!(
            handoff_response.contains("[DONE]"),
            "handoff dropped the in-flight stream: {handoff_response}"
        );
        let exit_old = old.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(exit_old.success(), "handoff A exit: {exit_old}");
        // B keeps serving after A is gone.
        let health_b_after = get_healthz(port_h)?;
        anyhow::ensure!(
            health_b_after["pid"].as_u64() == Some(u64::from(new.proc.pid)),
            "B not serving after handoff: {health_b_after}"
        );
        signal(new.proc.pid, "TERM")?;
        let exit_new = new.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(exit_new.success(), "handoff B exit: {exit_new}");

        upstream.proc.kill();
        Ok(serde_json::json!({
            "reload": report,
            "old_key_rejected": old_key.contains("401"),
            "new_key_admitted": new_key.contains("200"),
            "health_a": health_a,
            "health_draining": health_draining,
            "drain_refusal_status": refused.lines().next().unwrap_or(""),
            "inflight_completed": inflight_response.contains("[DONE]"),
            "exit_a": format!("{exit_a}"),
            "handoff": {
                "pid_a": pid_old,
                "pid_b": pid_b,
                "health_after": health_b_after,
                "inflight_completed": handoff_response.contains("[DONE]"),
                "exit_a": format!("{exit_old}"),
                "exit_b": format!("{exit_new}"),
            },
            "flag_version": flag_version,
            "versions_agree": versions_agree,
        }))
    }

    /// Failure path: invalid reload keeps old config (422), bind collision
    /// records the holder pid, and the drain deadline bounds a hung
    /// request.
    pub fn failure_path(binary: &Path, evidence: &Path) -> anyhow::Result<Value> {
        let dir = evidence.join("failure");
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        std::fs::create_dir_all(&dir)?;
        let mut upstream = spawn_gated_upstream(&dir, "upstream")?;
        let upstream_url = format!("http://{}", upstream.addr);
        let port = free_port()?;
        let dir_a = dir.join("a");
        daemon_config(&dir_a, port, &upstream_url, "key-a", "pw-a");
        let mut a = spawn_daemon(binary, &dir_a, "a", false, &[])?;
        a.proc.wait_line("HTTP server listening")?;

        // Invalid reload (empty model) → 422, old config keeps serving.
        std::fs::write(
            &a.config,
            "server:\n  listen: '127.0.0.1:1'\ndevin:\n  base_url: 'http://127.0.0.1:9'\n  token: x\n  model: ''\n",
        )?;
        let rejected = post_reload(port, "pw-a")?;
        anyhow::ensure!(
            rejected.contains("422"),
            "invalid reload not 422: {rejected}"
        );
        let still_ok = raw_request(
            port,
            "GET /v1/models HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer key-a\r\nConnection: close\r\n\r\n",
            &[],
            Duration::from_secs(5),
        )?;
        anyhow::ensure!(
            still_ok.contains("200"),
            "old config not serving: {still_ok}"
        );

        // Bind conflict: a second daemon on the same port exits nonzero
        // and the marker records the holder's pid (probed via /healthz).
        let dir_b = dir.join("b");
        daemon_config(&dir_b, port, &upstream_url, "key-b", "pw-b");
        let mut b = spawn_daemon(binary, &dir_b, "b", false, &[])?;
        let exit_b = b.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(
            !exit_b.success(),
            "conflicting daemon unexpectedly ran: {exit_b}"
        );
        let marker_path = dir_b.join("state/logs/bind-failure.json");
        let marker: Value = serde_json::from_str(&std::fs::read_to_string(&marker_path)?)?;
        let holder = marker["holder"].as_str().unwrap_or_default();
        anyhow::ensure!(
            holder.contains(&format!("pid={}", a.proc.pid)),
            "holder did not record daemon A pid: {marker}"
        );
        anyhow::ensure!(marker["count"].as_u64() == Some(1), "marker: {marker}");

        // Bounded drain: a hung in-flight request cannot stall shutdown
        // past the configured deadline (DEVIN2API_DRAIN_TIMEOUT_MS seam).
        let dir_c = dir.join("c");
        let port_c = free_port()?;
        daemon_config(&dir_c, port_c, &upstream_url, "key-c", "pw-c");
        let mut c = spawn_daemon(
            binary,
            &dir_c,
            "c",
            false,
            &[("DEVIN2API_DRAIN_TIMEOUT_MS", "400")],
        )?;
        c.proc.wait_line("HTTP server listening")?;
        let hung = spawn_chat(port_c, "key-c");
        upstream.proc.wait_line("REQUEST")?;
        signal(c.proc.pid, "TERM")?;
        c.proc.wait_line("drain timed out")?;
        let exit_c = c.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(exit_c.success(), "bounded drain exit: {exit_c}");
        // The hung client connection was force-closed at the deadline.
        let hung_response = recv_bounded(&hung, EVENT_TIMEOUT)?;
        anyhow::ensure!(
            !hung_response.contains("[DONE]"),
            "hung request unexpectedly completed: {hung_response}"
        );

        signal(a.proc.pid, "TERM")?;
        let exit_a = a.proc.wait_exit(EVENT_TIMEOUT)?;
        anyhow::ensure!(exit_a.success(), "daemon A exit: {exit_a}");
        upstream.proc.kill();
        Ok(serde_json::json!({
            "invalid_reload_status": rejected.lines().next().unwrap_or(""),
            "old_config_kept": still_ok.contains("200"),
            "conflict_exit": format!("{exit_b}"),
            "bind_failure_marker": marker,
            "bounded_drain_exit": format!("{exit_c}"),
            "hung_request_cut": !hung_response.contains("[DONE]"),
            "exit_a": format!("{exit_a}"),
        }))
    }
}

#[cfg(unix)]
use imp::{failure_path, happy_path};

#[cfg(not(unix))]
fn happy_path(_: &Path, _: &Path) -> anyhow::Result<Value> {
    Ok(json!({"skipped":"process lifecycle QA requires unix signals"}))
}

#[cfg(not(unix))]
fn failure_path(_: &Path, _: &Path) -> anyhow::Result<Value> {
    Ok(json!({"skipped":"process lifecycle QA requires unix signals"}))
}

pub fn run(evidence: &Path, case: Option<&str>) -> anyhow::Result<i32> {
    std::fs::create_dir_all(evidence)?;
    // Fail closed: no prior success artifact may survive a new invocation.
    // Per-scenario directories are rebuilt below; top-level reports are only
    // published after the corresponding scenario completes successfully.
    for file in ["qa.json", "happy.json", "failure.json", "daemon-build.json"] {
        let path = evidence.join(file);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    for directory in ["happy", "failure", "version"] {
        let path = evidence.join(directory);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
    }
    let versions = version_cases(evidence)?;
    if case == Some("version-resolution") {
        write_json(&evidence.join("qa.json"), &versions)?;
        return Ok(i32::from(versions["all_matched"] != true));
    }
    let binary = build_daemon(evidence)?;
    match case {
        Some("invalid-reload-bind-and-forced-deadline" | "drain-timeout-and-handoff-conflict") => {
            let failure = failure_path(&binary, evidence)?;
            write_json(&evidence.join("failure.json"), &failure)?;
            write_json(&evidence.join("qa.json"), &failure)?;
            Ok(0)
        }
        Some(other) => anyhow::bail!("unknown lifecycle case {other}"),
        None => {
            let happy = happy_path(&binary, evidence)?;
            let failure = failure_path(&binary, evidence)?;
            write_json(&evidence.join("happy.json"), &happy)?;
            write_json(&evidence.join("failure/result.json"), &failure)?;
            let report = json!({"version_resolution":versions,"happy":happy,"failure":failure});
            write_json(&evidence.join("qa.json"), &report)?;
            let passed = versions["all_matched"] == true
                && happy["versions_agree"] == true
                && happy["inflight_completed"] == true
                && happy["handoff"]["inflight_completed"] == true
                && failure["old_config_kept"] == true
                && failure["bind_failure_marker"]["count"] == 1;
            Ok(i32::from(!passed))
        }
    }
}

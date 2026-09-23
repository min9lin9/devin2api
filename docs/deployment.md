# Deployment (launchd / systemd / bare process)

Three platform topologies — binary, config and state/logs live in separate per-platform directories:

| Platform | Supervisor                                 | Binary                                              | Config                                                 | State/logs                                                                  | Deploy command               |
| -------- | ------------------------------------------ | --------------------------------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------------- | ---------------------------- |
| macOS    | launchd user agent `com.$USER.devin-2api`  | `~/.local/bin/devin-2api`                           | `~/Library/Application Support/devin-2api/config.yaml` | same dir as config (`logs/` subdir; macOS has no separate state convention) | `scripts/deploy.sh`          |
| Linux    | systemd `--user` unit `devin-2api.service` | `~/.local/bin/devin-2api`                           | `${XDG_CONFIG_HOME:-~/.config}/devin-2api/config.yaml` | `${XDG_STATE_HOME:-~/.local/state}/devin-2api`                              | `scripts/deploy-linux.sh`    |
| Windows  | none — bare exe in the foreground          | `%LOCALAPPDATA%\Programs\devin-2api\devin-2api.exe` | `%APPDATA%\devin-2api\config.yaml`                     | `%LOCALAPPDATA%\devin-2api`                                                 | `scripts/deploy-windows.ps1` |

Path resolution chain (service definitions pass explicit flags; the chain only matters for bare runs): config = `-config` flag → `DEVIN2API_CONFIG` env → `./config.yaml` (only if it exists — repo checkout / unzip-and-run on Windows) → the platform default above; state = `-state-dir` flag → `DEVIN2API_STATE_DIR` env → platform default. The startup log line `paths resolved` prints both effective paths.

The three deploy scripts (macOS/Linux share `scripts/lib-deploy.sh`) have identical flag semantics: `--release <tag|latest>` installs a prebuilt binary (sha256-verified), `--no-restart` swaps without restarting, `--check` compares installed/running/latest release versions, `--uninstall` stops and removes the service and binary (keeping config and logs). First install generates the service definition and starts it; a missing `config.yaml` is generated from `config.example.yaml` (random `auth.api_key`/`dashboard.password`, interactive token prompt on a tty). Preflight blocks sudo, missing dependencies, placeholder tokens and port conflicts; after `/healthz` reports the new version the script probes `/v1/models` to verify upstream auth. Minimal install path: clone the repo → `deploy*.sh --release latest`.

`scripts/deploy-remote.sh` is a dev-machine remote driver: passwordless SSH to the deploy target (`DEVIN2API_HOST`) running `deploy.sh` — worktree mode pushes the local working tree (incl. uncommitted changes) plus `.git` to a remote staging dir and builds there; `--ref`/`--release` deploy pushed state or prebuilt assets; `--check` compares both instances side by side.

## Cross-platform invariants

- **`logs/` always lives under the state dir**: per-request debug dirs, `index.jsonl`, `quota.jsonl`, `gate-state.json` are all under `<state-dir>/logs/`; `stdout.log`/`stderr.log` are process output. A `logs/` symlink in the repo just points at the local state dir (dev convenience, not required). The legacy single-runtime-dir layout is migrated automatically by the deploy scripts (`migrate_legacy_runtime`): config/logs move to the platform dirs and the old binary is removed; existing targets are not overwritten.
- **Graceful drain is a hard requirement**: `SIGTERM` puts the process into draining — `/healthz` keeps answering with `draining: true`, new `/v1/*` get `503 + Retry-After: 1`, in-flight requests run to completion; the drain cap is 600s, after which remaining connections are closed. Both service definitions allow 660s to cover the cap plus margin. Restarts only send SIGTERM — never `kill -9` to save time (Ctrl+C on a Windows console triggers the same drain). Listener behavior during drain depends on `DEVIN2API_REUSEPORT`: without it the listener stays open (new requests get an application-level 503 rather than a kernel refusal); with it the listener closes immediately so the reuseport group's other sockets take over — the old instance must yield the port for the pre-staged handoff process to take over. At drain start keep-alives are disabled: connections accepted before drain would otherwise pin to the old instance eating 503s for the whole window; with keep-alive off they finish with `Connection: close` and the client's reconnect lands on the successor — a stale connection eats at most one 503.
- **One writer per state directory**: never run two daemons against the same state dir. The deploy scripts enforce a single supervised instance; bind failures during restart storms are recorded in `logs/bind-failure.json` and surfaced at `/panel/api/stats` under `last_bind_failure`.

## Rollback via a backed-up state copy

The supported rollback path restores a **separate backup copy** — never roll back onto a live state dir:

```bash
# before upgrading (Linux paths shown)
systemctl --user stop devin-2api
cp -a ~/.local/state/devin-2api ~/.local/state/devin-2api.bak
systemctl --user start devin-2api        # new version runs

# to roll back
systemctl --user stop devin-2api
scripts/deploy-linux.sh --release v<old>  # or reuse the kept devin-2api.previous
rsync -a --delete ~/.local/state/devin-2api.bak/ ~/.local/state/devin-2api/
systemctl --user start devin-2api
```

The restored copy contains only pre-upgrade history; requests served during the evaluation window are absent — expected. `qa documented-smoke` executes this workflow end to end (backup → post-upgrade run → restore → panel serves pre-upgrade history).

## Migrating from the Go daemon

The config file, state directory and log formats are shared with the Go implementation ([WncFht/devin2api](https://github.com/WncFht/devin2api)) — an existing `config.yaml`, `credentials.toml` and `logs/` history are read as-is, no migration step.

1. Stop the Go service (`systemctl --user stop devin-2api` / `launchctl bootout` / Ctrl+C).
2. Install this binary (`scripts/deploy-linux.sh --release latest`, `scripts/deploy.sh`, or unpack the release asset).
3. Keep the existing `config.yaml` and state dir — both are read as-is.
4. Start the service; `/healthz` reports the new version and `logs/` continues appending in the same format.

For side-by-side evaluation, run this instance with a **separate** `-state-dir` (and a different `server.listen` port); copy the existing state dir first if you want the history visible. Rollback uses the backed-up-copy procedure above.

Known intentional differences vs the Go daemon (all deliberate fixes or honest capability boundaries): token-generation auth repair, bounded shared catalog fetch, cancellation rechecked before rate admission, drip probes obey `max_rpm` inside a latch, and Go-only runtime metrics (pprof/fgprof) are replaced by Rust diagnostics (`/debug/diagnostics/*`; the legacy routes return 501).

## Offline smoke (no upstream token)

The documented quickstart can be exercised end to end without a real Devin account by pointing `devin.base_url` at a local stub. The QA suite runs exactly this (`cargo run --locked --features qa --bin qa -- documented-smoke --evidence <dir>`): it packages a release asset, copies `config.example.yaml`, applies the documented edits (listen port, `base_url` → stub, synthetic `token`, `model`), then checks `/healthz`, `/v1/models` (401 without `auth.api_key`, 200 with), a streaming `/v1/chat/completions`, a buffered `/v1/responses`, `/panel` + `/panel/api` auth, the rollback-copy workflow and a clean SIGTERM exit.

`scripts/smoke.sh` does the lighter-weight version against a source build: `--no-upstream` asserts `/v1/models` fails cleanly without a token instead of probing a live upstream.

## macOS (launchd)

### Current plist

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key><string>com.$USER.devin-2api</string>
	<key>ProgramArguments</key>
	<array>
		<string>/Users/<user>/.local/bin/devin-2api</string>
		<string>-config</string>
		<string>/Users/<user>/Library/Application Support/devin-2api/config.yaml</string>
		<string>-state-dir</string>
		<string>/Users/<user>/Library/Application Support/devin-2api</string>
	</array>
	<key>WorkingDirectory</key><string>/Users/<user>/Library/Application Support/devin-2api</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>DEVIN2API_REUSEPORT</key><string>1</string>
	</dict>
	<key>RunAtLoad</key><true/>
	<key>KeepAlive</key><true/>
	<key>ThrottleInterval</key><integer>5</integer>
	<key>ExitTimeOut</key><integer>660</integer>
	<key>StandardOutPath</key><string>/Users/<user>/Library/Application Support/devin-2api/logs/stdout.log</string>
	<key>StandardErrorPath</key><string>/Users/<user>/Library/Application Support/devin-2api/logs/stderr.log</string>
</dict>
</plist>
```

Key semantics:

| Key                    | Value                   | Notes                                                                                                                                       |
| ---------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `RunAtLoad`            | true                    | start at login                                                                                                                              |
| `KeepAlive`            | true                    | relaunch on any exit — including a manual `kill`. For "clean exit stays dead" use `<dict><key>SuccessfulExit</key><false/></dict>`          |
| `ThrottleInterval`     | 5                       | crash-loop backoff                                                                                                                          |
| `ExitTimeOut`          | 660                     | SIGTERM → wait ≤660s → SIGKILL; covers the binary's 600s drain cap + margin                                                                 |
| `EnvironmentVariables` | `DEVIN2API_REUSEPORT=1` | enables SO_REUSEPORT — the precondition for overlapping handoff deploys; a bare binary without it still hits the single-instance port guard |
| `StandardErrorPath`    | logs/stderr.log         | tracing output lands on disk; rotated daily by the companion logrotate agent (below)                                                        |

### stderr/stdout log rotation

Request debug logs have retention, but `stderr.log` (tracing process log) and `stdout.log` only grow. The write fds are held by launchd and the process cannot reopen them — rename-style rotation would keep writing to the old inode and lose new output — so rotation uses copytruncate: `scripts/rotate-logs.sh` copies then truncates `*.log` over 50MB in place and gzips, keeping `.1`–`.3.gz` (a line written mid-truncate can still be lost — the minimal cost of that semantic).

`deploy.sh` installs it as `~/.local/bin/devin-2api-logrotate` and loads the companion agent `com.$USER.devin-2api.logrotate` (`StartInterval=86400`, daily; output to `logs/logrotate.out`/`.err`); plist drift is rewritten and bootout+bootstrap applied, `--uninstall` removes both. Fully decoupled from the main service — rotation does not touch in-flight requests. The system newsyslog is rename+signal and does not apply to launchd-held fds; there is no equivalent substitute, use this script.

### Common commands

```bash
launchctl print gui/$(id -u)/com.$USER.devin-2api | grep -E 'state|pid'   # status
launchctl kickstart -k gui/$(id -u)/com.$USER.devin-2api                 # restart (SIGTERM then relaunch)
launchctl bootout gui/$(id -u)/com.$USER.devin-2api                      # stop
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.$USER.devin-2api.plist  # reload
tail -f logs/stderr.log                                                  # process log
```

### Optional extras

- **Restart on config change**: add `WatchPaths` pointing at the config dir's `config.yaml` — saving triggers a restart. Cost: any mtime bump (editor touch, `deploy.sh` sync) restarts.
- **Freshly built binary + immediate kickstart**: overwriting the binary then kickstarting can leave dyld stuck in a Gatekeeper check (process `S` state, no listener, no logs). Confirm with `sample <pid>`, then `kill -9` and let KeepAlive relaunch; safer order is build first, stop the old process after.

## Linux (systemd --user)

The unit generated by `scripts/deploy-linux.sh` (`~/.config/systemd/user/devin-2api.service`):

```ini
[Unit]
Description=devin-2api — OpenAI/Anthropic-compatible proxy for Devin
After=network-online.target

[Service]
ExecStart=~/.local/bin/devin-2api -config ~/.config/devin-2api/config.yaml -state-dir ~/.local/state/devin-2api
WorkingDirectory=~/.local/state/devin-2api
Environment=DEVIN2API_REUSEPORT=1
Restart=always
RestartSec=5
TimeoutStopSec=660
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=~/.local/state/devin-2api
StandardOutput=append:~/.local/state/devin-2api/logs/stdout.log
StandardError=append:~/.local/state/devin-2api/logs/stderr.log

[Install]
WantedBy=default.target
```

Correspondence with the macOS version: `Restart=always` + `RestartSec=5` ≈ `KeepAlive` + `ThrottleInterval`, `TimeoutStopSec=660` ≈ `ExitTimeOut`, `Environment=DEVIN2API_REUSEPORT=1` ≈ `EnvironmentVariables`; stdout/stderr land in state-dir files the same way (not the journal — the debugging path is identical to macOS). Note that `XDG_CONFIG_HOME`/`XDG_STATE_HOME` are usually unset in the systemd `--user` context; the unit uses absolute paths expanded at deploy time.

stderr/stdout rotation is the same script with the same semantics: `deploy-linux.sh` installs `~/.local/bin/devin-2api-logrotate` plus a `devin-2api-logrotate.service` (oneshot) + `devin-2api-logrotate.timer` (`OnCalendar=daily`, `Persistent=true`) pair; `--uninstall` removes them.

Common commands:

```bash
systemctl --user status devin-2api          # status (MainPID, memory)
systemctl --user restart devin-2api         # restart (SIGTERM → TimeoutStopSec=660 drain window → kill)
systemctl --user stop devin-2api            # stop
journalctl --user -u devin-2api -f          # unit event log (tracing is in logs/stderr.log)
tail -f logs/stderr.log                     # process log
```

Two Linux-specific notes:

- **User manager lifecycle**: `systemctl --user` services exit with the last login session by default. To run while logged out: `loginctl enable-linger $USER` (no root needed; some distros prompt via polkit; `deploy-linux.sh` probes and reminds you after each run).
- **No systemd** (containers, WSL1, …): run the binary in the foreground — equivalent to the Windows mode.

## Windows (bare process)

No service wrapper: `devin-2api.exe` runs in the foreground; Ctrl+C triggers the same graceful drain as SIGTERM elsewhere; closing the window, `taskkill /F` and `Stop-Process` are hard kills. The layout follows Microsoft conventions: exe in `%LOCALAPPDATA%\Programs\devin-2api`, `config.yaml` in `%APPDATA%\devin-2api` (roaming), `logs\` in `%LOCALAPPDATA%\devin-2api` (machine-local). Running the zip's exe directly also works — `./config.yaml` is picked up when present (resolution chain above), while the state dir still falls back to `%LOCALAPPDATA%\devin-2api`. The release zip contains exe + `config.example.yaml` + LICENSE.

`scripts/deploy-windows.ps1` has the same semantics as the bash scripts: `-Release latest` downloads the zip and verifies sha256, generates `config.yaml` when missing (random `auth.api_key`/`dashboard.password`, `127.0.0.1` + free port, interactive token paste), starts a separate console window, and smokes healthz + `/v1/models`; `-Check`/`-Uninstall`/`-NoStart`/`-Force` (permits killing a running instance, equivalent to closing the window)/`-RuntimeDir` (override the exe install dir; config/state dirs are overridable via `DEVIN2API_CONFIG_DIR`/`DEVIN2API_STATE_DIR` env). The legacy "exe + config + logs in one dir" layout is migrated automatically (`Move-LegacyLayout`). Instances started over SSH are reaped when the session ends — the script targets local interactive sessions.

## Panel and agent access

`/panel` is the human entry point; the API under it is also meant for programmatic agent consumption. With `dashboard.password` set, besides cookie login you can pass `Authorization: Bearer <password>` directly (skips the cookie dance):

```bash
curl -s -H 'Authorization: Bearer <password>' localhost:<port>/panel/api/stats
curl -s -H 'Authorization: Bearer <password>' 'localhost:<port>/panel/api/requests?limit=20&q=failed'
curl -s -H 'Authorization: Bearer <password>' localhost:<port>/panel/api/requests/active
curl -s -H 'Authorization: Bearer <password>' localhost:<port>/panel/api/requests/<dir>
curl -s -H 'Authorization: Bearer <password>' localhost:<port>/panel/api/requests/<dir>/file/04-devin-response.jsonl
```

An empty `password` leaves the panel and API open — acceptable for local-only use; set one before exposing beyond loopback.

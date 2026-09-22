# Go ↔ Rust compatibility

This document is the contract between the Rust port and the Go reference implementation it replaces. Everything not listed under [Approved exceptions](#approved-exceptions) is intended to behave identically; any other observed difference is a regression.

## Shared surfaces

| Surface         | Compatibility                                                                                                                                                                                                                                                                             |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `config.yaml`   | Same keys, defaults, validation and unknown-field rejection. An existing Go config loads unchanged; `config.example.yaml` carries the same keys and defaults (comments updated where the Go-specific behavior changed, e.g. `debug.pprof_listen`).                                        |
| Credentials     | Same discovery chain: `devin.token` → `DEVIN_TOKEN`/`WINDSURF_API_KEY` → Devin CLI `credentials.toml` (same per-platform paths).                                                                                                                                                          |
| CLI flags       | `-config`, `--config`, `-state-dir`, `--state-dir`, `-version`/`--version` with the same exit codes. `DEVIN2API_CONFIG`, `DEVIN2API_STATE_DIR`, `DEVIN2API_REUSEPORT` keep their meaning.                                                                                                 |
| State directory | `logs/index.jsonl`, `logs/quota.jsonl`, `logs/gate-state.json`, `logs/bind-failure.json`, `logs/stderr.log`, per-request `meta.json`/`error.json`/stage files and `attachments/` are written in the same formats. Existing Go history is readable by the Rust daemon — no migration step. |
| HTTP surface    | Same routes, status codes, error shapes, SSE event sequences, WebSocket subprotocol (`responses_websockets=2026-02-06`) and panel API.                                                                                                                                                    |
| Panel           | Same embedded UI; only the runtime-diagnostics rendering differs (see exception 5).                                                                                                                                                                                                       |

## The one-writer state rule

**Never run two daemons — Go and Rust, or two of either — as writers of one state directory.** `index.jsonl`, `quota.jsonl` and `gate-state.json` are append/rewrite files with no cross-process locking; concurrent writers corrupt history and can double-consume rate-gate latch state. The deploy scripts enforce this by managing exactly one supervised instance per state dir.

Safe patterns:

- Sequential swap: stop Go, start Rust on the same state dir (or vice versa).
- Side-by-side evaluation: run the Rust instance with a **separate** `-state-dir` (and a different `server.listen` port). Copy the Go state dir first if you want the history visible in the new instance.

## Migration: Go → Rust

1. Stop the Go service (`systemctl --user stop devin-2api` / `launchctl bootout` / Ctrl+C).
2. Install the Rust binary (`scripts/deploy-linux.sh --release latest`, `scripts/deploy.sh`, or unpack the release asset).
3. Keep the existing `config.yaml` and state dir — both are read as-is.
4. Start the Rust service; `/healthz` reports the new version and `logs/` continues appending in the same format.

## Rollback: Rust → Go

Rollback uses a **separate backed-up copy** of the state directory — never roll back onto a live dir:

1. Before upgrading, copy the state dir: `cp -a ~/.local/state/devin-2api ~/.local/state/devin-2api.bak` (paths per platform, see `deployment.md`).
2. To roll back: stop the Rust service, reinstall the previous binary (deploy scripts keep `devin-2api.previous`, or re-download the old release asset), restore the backup over the state dir (`rsync -a --delete backup/ state/` or remove + copy), then start the service.
3. The Go daemon reads the restored state as if the Rust run never happened. Requests logged by the Rust build during the evaluation window are absent from the restored copy — that is expected; merge them manually only if you need the records.

The QA suite executes this exact workflow (`qa documented-smoke`): backup → run → restore → verify the panel serves the pre-upgrade history.

## Approved exceptions

Five behavioral differences were reviewed and approved; each is a deliberate fix or an honest capability boundary, not drift:

1. **Concurrent authentication repair uses token generations.** A request that fails `unauthenticated` may retry once against a newer token generation even if another request already refreshed it. No extra retry when no newer token exists. (Go could issue redundant repair reads under races.)
2. **Catalog fetch is one bounded shared task.** Leader and followers independently observe their own cancellation; a cancelled leader does not strand the fetch or the followers. Cache/cooldown semantics are otherwise equivalent.
3. **Retry backoff completes before final rate admission.** Cancellation is rechecked immediately before every actual send. (Go admitted before the backoff, so a cancelled request could still consume a send slot.)
4. **Latched drip probes obey the configured window quota** as well as drip spacing. (Go's drip path could exceed `max_rpm` inside a latch.)
5. **Go-only runtime metrics/profiles become Rust diagnostics.** RSS, CPU, active requests, rates and operational counters are retained; Go-only fields report `null` rather than invented values. Snapshots carry `runtime: "rust"` plus capability metadata. The `debug.pprof_listen` setting keeps its name and restart classification but serves the Rust diagnostic endpoints (`/debug/diagnostics/*`); the legacy `/debug/pprof/*` and `/debug/fgprof` routes return an explicit `501` pointing at the replacement. See `perf.md` for the capability matrix and profiling workflow.

## Known non-exceptions (verified equal)

- Streaming event order, thinking/text/tool interleave, signature merging, `stop_reason` handling, truncation classification.
- Rate-gate latch persistence (`gate-state.json`) across restarts, including unexpired latches.
- Reload semantics: the same fields apply live vs. `requires_restart`; invalid reloads keep the old config and return `422`.
- Drain behavior: `SIGTERM`/Ctrl+C → `draining` healthz, new `/v1/*` get `503 + Retry-After`, in-flight requests finish, 600s cap.
- Version resolution: `DEVIN2API_BUILD_VERSION` → `DEVIN2API_PACKAGE_VERSION` → `dev-<vcs12>[-dirty]` → checked-in `VERSION` → `dev`; `-version` and `/healthz` agree.

## Performance relationship

Not a compatibility surface, but recorded here so the claim lives next to the contract: the Rust build is **slower on SSE streaming** (0.45–1.03× Go throughput on the measured Chat SSE cells), faster on buffered JSON (1.28×) and WebSocket (1.47×), and uses 0.22–0.75× the peak RSS. Full data in `perf.md`.

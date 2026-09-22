# Command reference

All binaries are built with `cargo build --locked [--release] --bin <name>`. Flags use Go-style single-dash spelling; double-dash forms (`--config`) are accepted identically.

## `devin-2api` — the daemon

```text
devin-2api -config <path> -state-dir <dir> -version
```

| Flag               | Meaning                                                                                     |
| ------------------ | ------------------------------------------------------------------------------------------- |
| `-config <path>`   | YAML config path; default resolves `$DEVIN2API_CONFIG` → `./config.yaml` → platform default |
| `-state-dir <dir>` | log/state root; default resolves `$DEVIN2API_STATE_DIR` → platform default                  |
| `-version`         | print the build version and exit                                                            |

Environment variables:

| Variable                                          | Meaning                                                                                                   |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `DEVIN2API_CONFIG`                                | config path (below `-config`, above `./config.yaml`)                                                      |
| `DEVIN2API_STATE_DIR`                             | state dir (below `-state-dir`)                                                                            |
| `DEVIN2API_REUSEPORT`                             | `1`/`true` (case-insensitive) enables `SO_REUSEPORT` zero-downtime handoff where the platform supports it |
| `DEVIN_TOKEN`, `WINDSURF_API_KEY`                 | upstream token sources (above `credentials.toml`, below `devin.token`)                                    |
| `HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY`/`NO_PROXY` | upstream proxy env chain; `devin.proxy` in config takes precedence                                        |
| `DEVIN2API_BUILD_VERSION`                         | build-time only: stamps the release version into `-version`/`/healthz`                                    |
| `DEVIN2API_PACKAGE_VERSION`                       | build-time only: distribution-injected package version (second precedence)                                |

Signals: `SIGTERM`/Ctrl+C → graceful drain (healthz reports `draining`, new `/v1/*` get 503, in-flight finish, 600s cap). `SIGHUP`-free reload goes through `POST /panel/api/config/reload`.

## `probe` — upstream experiment tool

```text
probe <subcommand> [flags]
```

Token resolution: `DEVIN_TOKEN` → `devin.token` in config → Devin CLI credentials chain. Unknown subcommands print usage and exit 2 before any credential check.

| Subcommand                     | Purpose                                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------------------- |
| `configs`                      | dump `GetCliModelConfigs` (raw + router/feature summary)                                     |
| `status`                       | `CheckChatCapacity` + `CheckUserMessageRateLimit` + `GetModelStatuses` + `GetModelProviders` |
| `assign <uid> [uid...]`        | `AssignModel` for each router uid                                                            |
| `chat [flags]`                 | one `GetChatMessage` stream, dump all frames                                                 |
| `replay [flags]`               | two-step: call once, then replay the assistant message with variants                         |
| `hist [flags]`                 | synthetic text+call+result history in a chosen wire shape                                    |
| `rerun -file <03.json> [-n N]` | replay a captured `GetChatMessageRequest` N times                                            |
| `bigctx [flags]`               | send a ~N KB single user message, observe the error code                                     |
| `misc`                         | adjacent endpoints: embeddings/extchat/status/config/command configs                         |
| `edge <case> [flags]`          | targeted edge-case histories (case names in the source switch)                               |

`chat` flags (selection — run `probe` with no args for the full list): `-model`, `-prompt`, `-system`, `-system-as-message`, `-system-empty`, `-tool`/`-tool-schema`, `-custom-tool`, `-raw-schema`, `-tool-extras`, `-tool-choice`, `-disable-parallel`, `-provider-source`, `-prompt-id`, `-num-tokens`, `-planner-mode`, `-step-type`, `-step-index`, `-request-type`, `-language`, `-chat-model-name`, `-no-fingerprint`, `-no-ids`, `-trajectory-id`, `-cascade-id`, `-max-tokens`, `-num-completions`, `-stop-pattern`, `-temperature`, `-top-p`, `-top-k`, `-images`, `-internal-model`, `-assign-jwt`, `-resolve`/`-resolve-only`/`-router`, `-meta-extras`, `-frames`, `-dump <dir>`.

`replay -variant`: `with-sig`/`with-ids`/`no-sig`/`bogus-sig`/`bogus-sig-typed`/`sig-only`/`mutated-thinking`/`no-thinking`. `hist -shape`: `merged`/`merged-single`/`split`/`split-single`. `edge` flags: `-model`, `-image-file`, `-prompt`.

## `loadtest` — HTTP load generator

```text
loadtest -url <endpoint> -key <api_key> -c <concurrency> [-n <total> | -duration <dur>] [-model <m>] [-stream] [-body <file>]
```

Defaults: `-url http://localhost:3003/v1/chat/completions`, `-c 8`, `-n 100`, `-stream true`. `-duration` (e.g. `30s`) overrides `-n`. `-body` replaces the built-in chat request payload.

## `upstreamstub` — upstream fault-injection stub

```text
upstreamstub -listen <addr> -scenario <name> [-recover-after N] [-deltas N] [-delta-bytes N] [-interval D] [-ttfb D]
```

A minimal HTTP/1.1 Connect server that answers `GetChatMessage` with real streaming envelopes and cuts the stream per scenario. Scenarios: `precontent`, `midcontent`, `recover`, `cleaneof`, `cleaneof-content`, `bare-end`, `endstream-error`, `badframe`, `badflags`, `end-hang`, `heartbeat`, `stall`, `stream` (deterministic full stream for perf work). Unimplemented RPCs get the connection hijacked and closed — the catalog-miss path. See `troubleshooting.md` for the scenario→classification table.

## `protoextract` — schema extraction

```text
protoextract <source-binary> <output-directory>
```

Scans a compiled binary for embedded `FileDescriptorProto`s and rebuilds `proto/`: `descriptors.pb` (raw descriptor set), `all-protos.proto` (flattened single file), `manifest.json` (symbol mapping). **Clears the output directory before writing** — never point it at a tree you care about. Not reproducible (needs the upstream binary + captures); the products are committed.

## `protocensus` — schema census and diff

```text
protocensus census [-logs DIR] [-max-dirs N]   # scan request dirs, report field coverage, unknown keys, enum anomalies
protocensus diff OLD.pb NEW.pb                 # compare two FileDescriptorSets, report added/removed/changed symbols
```

`census` reads `03-devin-request.json`/`04-devin-response.jsonl` under `logs/` (default `./logs`). New upstream enum members surface as numeric anomalies; new fields are invisible in logs (protojson drops unknown fields) — use `protoextract` + `diff` for those.

## `qa` — QA driver (not shipped)

Feature-gated (`--features qa`), dev tooling only — not a released daemon:

```text
qa <subcommand> --evidence <dir> [--go-root <dir>] [--case <name>] [--requests N] [--allow-live]
```

Subcommands: `baseline`, `manifest`, `http`, `websocket`, `dashboard-api`, `diagnostics`, `panel`, `lifecycle`, `cli`, `parity`, `faults`, `bench`, `stress`, `packaging`, `documented-smoke`, `coverage`, `live`, `final-surface`. Every runnable subcommand writes JSON results under `--evidence` and exits nonzero on failure. `live` requires `--allow-live` plus real credentials and is never run automatically.

## Scripts

| Script                                                                  | Purpose                                                                                                                                                 |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/smoke.sh [--port N] [--config F] [--no-upstream] [--binary P]` | build → free-port instance → healthz + `/v1/models` probe → SIGTERM clean-exit check. `--no-upstream` asserts a clean failure without a token (CI mode) |
| `scripts/deploy.sh` / `deploy-linux.sh` / `deploy-windows.ps1`          | platform install/upgrade/check/uninstall; `--release <tag\|latest>`, `--no-restart`, `--check`, `--uninstall`                                           |
| `scripts/deploy-remote.sh`                                              | drive a remote `deploy.sh` over passwordless SSH (`DEVIN2API_HOST`); worktree/`--ref`/`--release`/`--check` modes                                       |
| `scripts/rotate-logs.sh`                                                | copytruncate rotation for `stderr.log`/`stdout.log` over 50MB, keeps `.1`–`.3.gz`                                                                       |
| `scripts/release.sh [--publish] [--version vX.Y.Z]`                     | Conventional-Commits version bump + tag; dry-run by default                                                                                             |
| `scripts/release-selftest.sh`                                           | offline full-flow rehearsal of release.sh (local bare origin + stubbed curl/gh) — mandatory after editing release.sh                                    |
| `scripts/package-release.sh --target T --binary P --out D --version V`  | package one prebuilt binary into the release asset contract + `checksums.txt`                                                                           |
| `scripts/deploy-assets.test.sh`                                         | offline assertion suite over the deploy/release assets                                                                                                  |
| `scripts/generate-proto.sh [--check]`                                   | regenerate `crates/devin-proto` bindings from `proto/all-protos.fds`; `--check` verifies the committed output matches a fresh regen                     |
| `scripts/perf-snapshot.sh`                                              | Rust perf snapshot via the ported `loadtest` + Linux `perf`                                                                                             |
| `scripts/bench.sh [label]`                                              | capture the Rust test/bench timing baseline into `outputs/bench/`                                                                                       |
| `scripts/check-licenses.sh`                                             | verify every dependency has a license entry                                                                                                             |
| `scripts/check-secrets.sh`                                              | scan for committed secrets                                                                                                                              |
| `scripts/verify-wire-fixtures.sh`                                       | verify the checked-in wire fixtures                                                                                                                     |

## Taskfile tasks

```bash
task extract BINARY=<upstream-binary>   # binary → proto/ (destructive to proto/, not reproducible)
task generate                           # proto/all-protos.fds → crates/devin-proto (reproducible)
task generate-check                     # committed bindings == fresh regen
task census [MAX_DIRS=500]              # protocensus census over logs/
```

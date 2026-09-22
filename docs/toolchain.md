# Toolchain (operator's manual)

This repository's engineering infrastructure has four layers: **local verification** (fmt/lint/test/selftests), **CI** (three workflows), **release** (release.sh + release.yml), and **deploy** (the deploy script family). Every rule that can be mechanized lives in a script or CI job, not in someone's memory.

## 1. Toolchain pin

- **Rust 1.98.1**, pinned in `rust-toolchain.toml` (minimal profile + rustfmt + clippy). The floor is 1.88 (required by `connectrpc`/`connectrpc-build` 0.9.0); the pin records the exact resolved release.
- **Dependencies**: resolved once into `Cargo.lock`; every build/test/lint runs with `--locked`. No mid-flight upgrades.
- **Go is not needed** to build or run anything in this repo. The Go reference tree lives outside this repository and is only a test oracle for the QA harness.
- **Node** is needed only for docs lint (`npm run lint:md`/`format:check`) and the panel e2e (`npm run test:panel`, Playwright).

## 2. Proto codegen

The upstream schema is extracted, not written by hand:

```text
upstream binary → protoextract → proto/{descriptors.pb, all-protos.proto, manifest.json}
proto/all-protos.fds → connectrpc-build + buffa → crates/devin-proto/src/generated/
```

- `proto/` holds the three schema artifacts with SHA-256 provenance in `proto/SHA256SUMS`. They are immutable inputs — census/diff run against them.
- `crates/devin-proto` contains the **checked-in** generated bindings (~80MB of Rust). Normal builds never need upstream binaries or remote codegen.
- `task generate` / `scripts/generate-proto.sh` regenerates deterministically; `task generate-check` (`--check`) verifies committed output matches a fresh regen byte-for-byte.
- `task extract BINARY=<path>` re-extracts from a new upstream binary — destructive to `proto/` (it clears and rewrites the directory) and not reproducible; afterwards `all-protos.fds` and `SHA256SUMS` must be rebuilt before `generate` will pass its verification step.
- `protocensus census` scans `logs/` wire traffic against the extracted schema for field coverage and drift; `protocensus diff` compares descriptor sets.

## 3. Version resolution (4-level fallback)

`-version` output source, in precedence order (`build.rs`):

1. **`DEVIN2API_BUILD_VERSION`** — explicit non-dev stamp; release/deploy builds inject it.
2. **`DEVIN2API_PACKAGE_VERSION`** — distribution-injected package version (the Go installed-module equivalent; never the ordinary Cargo `package.version`).
3. **VCS**: `dev-` + first 12 revision chars, plus `-dirty` when the tree is dirty.
4. **Checked-in `VERSION`** — release.sh rewrites this file at publish so tarball builds report correctly. Fallback `"dev"`.

`-version` and `/healthz` always agree.

## 4. Local verification

| Command                                                          | Coverage                                                                                |
| ---------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `cargo fmt --all -- --check`                                     | formatting                                                                              |
| `cargo clippy --locked --workspace --all-targets --all-features` | lints (pedantic on; see workspace `Cargo.toml` for the relaxed set)                     |
| `cargo test --locked --workspace --all-features`                 | unit + integration tests                                                                |
| `cargo test --locked --test ci_contract`                         | CI matrix / asset-name / static-link contracts                                          |
| `bash scripts/deploy-assets.test.sh`                             | deploy-asset assertions (plist/unit keys, no `kill -9`, `bash -n` on all scripts)       |
| `bash scripts/release-selftest.sh`                               | release.sh full-flow rehearsal — **mandatory after editing release.sh**                 |
| `npm run format:check` / `npm run lint:md`                       | markdown format/rules (prettier + markdownlint-cli2, versions pinned in `package.json`) |
| `cargo run --locked --features qa --bin qa -- <sub>`             | process-level QA: parity, faults, lifecycle, bench, stress, packaging, documented-smoke |

## 5. CI (`.github/workflows/`)

- **`ci.yml`**: fmt, clippy, tests, proto-regeneration check, license check (`check-licenses.sh`), secret scan (`check-secrets.sh`), deploy-asset selftests, and the six-target build matrix.
- **`release.yml`**: consumes verified artifacts on tag push; builds the six assets (`devin-2api-{linux,darwin}-{amd64,arm64}` + `devin-2api-windows-{amd64,arm64}.zip`), asserts Linux assets have no dynamic interpreter (`readelf` — a musl artifact with an INTERP segment means an accidental dynamic dep), builds the Docker image from the same verified release binary (`Dockerfile.release` COPYs the asset — image bytes = release bytes), generates `checksums.txt`, publishes to GHCR (`ghcr.io/min9lin9/devin2api`). The publish job requires tag/publish authorization.
- **`security.yml`**: dependency audit job (push/PR/weekly).

### Platform build status

| Target                       | Asset                          | Status                                                           |
| ---------------------------- | ------------------------------ | ---------------------------------------------------------------- |
| `x86_64-unknown-linux-musl`  | `devin-2api-linux-amd64`       | **verified on this host**: static-pie, zero INTERP, smoke-passed |
| `aarch64-unknown-linux-musl` | `devin-2api-linux-arm64`       | CI runner only — not built on this host                          |
| `x86_64-apple-darwin`        | `devin-2api-darwin-amd64`      | CI runner only                                                   |
| `aarch64-apple-darwin`       | `devin-2api-darwin-arm64`      | CI runner only                                                   |
| `x86_64-pc-windows-msvc`     | `devin-2api-windows-amd64.zip` | CI runner only                                                   |
| `aarch64-pc-windows-msvc`    | `devin-2api-windows-arm64.zip` | cross-built on Windows amd64 runner; no emulated smoke           |

Foreign-target builds are delegated to native hosted runners; unrun targets are explicitly unverified, not claimed green.

## 6. Release (`scripts/release.sh` + `release.yml`)

Two-phase:

- **dry-run** (default): computes the next version from Conventional Commits (0.x: feat/breaking → minor, else patch; `--version` overrides) and prints a classified changelog (Features / Fixes / Other; `chore(release):` bookkeeping commits filtered).
- **`--publish`**: refuses dirty worktree/unpushed HEAD → rewrites `VERSION`, commits `chore(release): bump`, pushes → polls `workflow_runs?head_sha=` until that commit's CI is green (`CI_WAIT_SECONDS` default 1200s, `CI_POLL_INTERVAL` default 20s) → re-fetches and re-checks `HEAD == origin/main` (TOCTOU guard) → `git tag -a -F notes --cleanup=verbatim` → pushes the tag.

**Iron rule: a pushed tag is never re-cut.** A broken release body is fixed in place with `gh release edit vX.Y.Z --notes-file <tag annotation>`.

`scripts/release-selftest.sh` rehearses the whole flow offline: temp work repo + local bare origin, `url."file://<bare>".insteadOf` rewrites the fake GitHub URL so fetch/push stay local, PATH-stubbed `curl`/`gh` return canned `workflow_runs` JSON. Covers dry-run version math, the green publish path, pending→green polling, CI-failure rejection, unpushed rejection, and the mid-wait TOCTOU rejection.

## 7. Deploy script family + asset assertions

- `scripts/deploy.sh` (macOS launchd `com.$USER.devin-2api`) and `scripts/deploy-linux.sh` (systemd `--user`) share `scripts/lib-deploy.sh`: release-asset download + `checksums.txt` verification, post-deploy `wait_healthz_version` polling, stray-process check (`pgrep -x` exact-name match — `pgrep -f` would false-positive on unrelated processes with devin-2api in their command line). Both install `scripts/rotate-logs.sh` as `~/.local/bin/devin-2api-logrotate` plus a daily driver (launchd StartInterval agent / systemd timer) rotating stderr/stdout.log. Platform details in `deployment.md`.
- `scripts/deploy-windows.ps1` is the same-semantics PowerShell variant (no service wrapper — console process).
- `scripts/deploy-assets.test.sh` is the string-assertion suite over those assets: the plist must carry KeepAlive/ExitTimeOut/`kickstart -k`, the unit must carry Restart=always/TimeoutStopSec, progress output must go `>&2`, `kill -9` is forbidden, plus `bash -n` on every shell script. New assertions follow the same `check`/`has` pattern with a single exit code at the end.

## 8. Quick reference

```bash
# before committing
cargo fmt --all && cargo clippy --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
npm run format:check && npm run lint:md

# release (see scripts/release.sh header)
scripts/release.sh                       # dry-run
scripts/release.sh --publish             # VERSION rewrite → wait for green CI → tag
bash scripts/release-selftest.sh         # mandatory after editing release.sh

# deploy & troubleshoot
scripts/deploy-linux.sh [--release vX.Y.Z]   # Linux local upgrade (macOS: deploy.sh)
scripts/deploy-remote.sh [--check|--release vX.Y.Z]  # drive a remote deploy from a dev machine
bash scripts/deploy-assets.test.sh           # deploy-asset assertions
```

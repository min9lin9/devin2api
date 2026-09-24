#!/usr/bin/env bash
# pgo-build.sh — profile-guided optimization (PGO) pipeline for the
# devin-2api release binary.
#
# Why: the Go baseline ships a checked-in PGO profile; the Rust port had
# none. rustc exposes the same machinery via -Cprofile-generate /
# -Cprofile-use. Expected effect on this workload: +5-12% throughput
# (branch layout + inlining on the hot path: SSE encode, proto decode,
# tokio poll). Measured results: see the task-24 PGO evidence and the
# docs section quoted in the claim that introduced this script.
#
# Flow (./scripts/pgo-build.sh all runs every phase in order):
#
#   1. instrument  cargo build --release --bin devin-2api with
#                  RUSTFLAGS="-Cprofile-generate=$PGO_DIR/profraw" into
#                  $TARGET_INSTRUMENTED. Instrumented binaries are ~2-5x
#                  slower — never ship them.
#   2. profile     run a representative workload against the instrumented
#                  daemon: the shipped `upstreamstub` (scenario=stream,
#                  200x32B deltas — the qa bench shape) plus `loadtest`
#                  legs over chat-SSE (c64 + c256), chat-JSON, responses
#                  and messages SSE. If a `qa` binary and the recorded
#                  oracle baseline are available, a `qa stress` leg adds
#                  the WS + debug-on mix. LLVM_PROFILE_FILE uses buffered
#                  mode (no `%c`): continuous mode is broken on this
#                  toolchain (llvm-profdata rejects the mmap'd files as
#                  truncated even with -runtime-counter-relocation), so the
#                  profile relies on the daemon's graceful SIGTERM drain,
#                  which runs atexit and flushes counters. Teardown must
#                  signal the daemon PID itself — a shell wrapper PID
#                  orphans the server and loses the profile.
#   3. merge       llvm-profdata merge -sparse all *.profraw into
#                  $PGO_DIR/merged.profdata; records git sha + timestamp
#                  in $PGO_DIR/profile.meta for staleness checks.
#   4. build       cargo build --release --bin devin-2api with
#                  RUSTFLAGS="-Cprofile-use=$PGO_DIR/merged.profdata
#                  -Cllvm-args=-pgo-warn-missing-function" into
#                  $TARGET_OPTIMIZED. rustc/llvm warnings are tee'd to
#                  $PGO_DIR/pgo-warnings.log — a large count means the
#                  profile is stale (source drifted since `profile`).
#
# Staleness: PGO profiles degrade gracefully — a stale profile silently
# regresses toward ~0 gain, never breaks correctness. `build` warns when
# the recorded git sha differs from HEAD or the profile is older than 14
# days; re-run `profile` + `merge` after landing hot-path changes.
#
# Requirements: rustup component llvm-tools-preview (the script resolves
# llvm-profdata from `rustc --print sysroot`; it does not need to be on
# PATH), python3 (free-port + readiness probes), cargo, bash.
#
# Env knobs:
#   PGO_DIR                 profile artifacts   (default: <repo>/pgo)
#   PGO_TARGET_INSTRUMENTED instrumented target (default: target/pgo-instrumented)
#   PGO_TARGET_OPTIMIZED    optimized target    (default: target/pgo-optimized)
#   PGO_PROFILE_SECS        seconds per loadtest leg (default: 45)
#   PGO_STRESS_REQUESTS     qa stress leg size, 0 skips (default: 40000)
#
# target-cpu=native: orthogonal lever — add -Ctarget-cpu=native to the
# phase-4 RUSTFLAGS for a combined binary, or alone for a native-only
# build. Fine for QA/bench; a shipped binary needs a documented baseline
# (x86-64-v3) instead of native.
#
# Usage: pgo-build.sh {instrument|profile|merge|build|all}
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PGO_DIR="${PGO_DIR:-$ROOT/pgo}"
TARGET_INSTRUMENTED="${PGO_TARGET_INSTRUMENTED:-$ROOT/target/pgo-instrumented}"
TARGET_OPTIMIZED="${PGO_TARGET_OPTIMIZED:-$ROOT/target/pgo-optimized}"
PROFILE_SECS="${PGO_PROFILE_SECS:-45}"
STRESS_REQUESTS="${PGO_STRESS_REQUESTS:-40000}"
PROFDATA="$PGO_DIR/merged.profdata"
META="$PGO_DIR/profile.meta"

log() { printf 'pgo: %s\n' "$*"; }
die() { printf 'pgo: ERROR %s\n' "$*" >&2; exit 1; }

llvm_profdata() {
    local sysroot bin
    sysroot="$(rustc --print sysroot)"
    bin="$sysroot/lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-profdata"
    if [ ! -x "$bin" ]; then
        die "llvm-profdata not found at $bin — run: rustup component add llvm-tools-preview"
    fi
    printf '%s' "$bin"
}

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

wait_tcp() { # port, timeout_secs, label
    python3 - "$1" "$2" "$3" <<'PY'
import socket, sys, time
port, timeout, label = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
deadline = time.time() + timeout
while time.time() < deadline:
    try:
        socket.create_connection(("127.0.0.1", port), 0.5).close()
        sys.exit(0)
    except OSError:
        time.sleep(0.1)
sys.exit(f"pgo: {label} port {port} never opened")
PY
}

wait_healthz() { # url, timeout_secs
    python3 - "$1" "$2" <<'PY'
import sys, time, urllib.request
url, timeout = sys.argv[1], float(sys.argv[2])
deadline = time.time() + timeout
while time.time() < deadline:
    try:
        if urllib.request.urlopen(url, timeout=2).status == 200:
            sys.exit(0)
    except Exception:
        time.sleep(0.2)
sys.exit(f"pgo: {url} never returned 200")
PY
}

phase_instrument() {
    mkdir -p "$PGO_DIR/profraw" "$PGO_DIR/profraw-build"
    log "instrumented build -> $TARGET_INSTRUMENTED (RUSTFLAGS=-Cprofile-generate=$PGO_DIR/profraw)"
    # Build-time profraws (build scripts, proc macros) are diverted to
    # profraw-build/ so `merge` only sees runtime workload data.
    LLVM_PROFILE_FILE="$PGO_DIR/profraw-build/build-%p.profraw" \
    RUSTFLAGS="-Cprofile-generate=$PGO_DIR/profraw" \
    CARGO_TARGET_DIR="$TARGET_INSTRUMENTED" \
        timeout 5400 cargo build --locked --release --bin devin-2api
    log "instrumented daemon: $TARGET_INSTRUMENTED/release/devin-2api"
}

phase_profile() {
    local daemon="$TARGET_INSTRUMENTED/release/devin-2api"
    [ -x "$daemon" ] || die "missing $daemon — run '$0 instrument' first"
    # Workload drivers are the NORMAL release builds: an instrumented
    # stub/loadtest would throttle the load and skew the profile.
    log "ensuring normal release stub+loadtest exist"
    timeout 5400 cargo build --locked --release --bin upstreamstub --bin loadtest
    local stub="$ROOT/target/release/upstreamstub"
    local loadtest="$ROOT/target/release/loadtest"
    [ -x "$stub" ] && [ -x "$loadtest" ] || die "stub/loadtest build failed"

    local work="$PGO_DIR/work"
    mkdir -p "$work"
    local stub_port daemon_port
    stub_port="$(free_port)"; daemon_port="$(free_port)"

    cat > "$work/pgo.yaml" <<EOF
server:
  listen: "127.0.0.1:$daemon_port"
  max_concurrency: 1024
devin:
  base_url: "http://127.0.0.1:$stub_port"
  token: "qa-bench-token"
  model: "stub-model"
  force_http1: true
  max_rpm: 0
auth:
  api_key: "qa-pgo-key"
debug:
  enabled: false
EOF
    printf '%s' '{"model":"stub-model","stream":true,"input":"load test"}' \
        > "$work/responses.json"
    printf '%s' '{"model":"stub-model","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"load test"}]}' \
        > "$work/messages.json"

    "$stub" -listen "127.0.0.1:$stub_port" -scenario stream \
        -deltas 200 -delta-bytes 32 -interval 0ms -ttfb 0ms \
        > "$work/stub.log" 2>&1 &
    local stub_pid=$!
    # Buffered mode: the profraw flushes on the daemon's graceful SIGTERM
    # drain (atexit). Launch directly (no wrapper) so $daemon_pid is the
    # real server PID — killing a wrapper orphans the server and the
    # profile is lost.
    LLVM_PROFILE_FILE="$PGO_DIR/profraw/loadtest-%p.profraw" \
        "$daemon" -config "$work/pgo.yaml" -state-dir "$work/state" \
        > "$work/daemon.log" 2>&1 &
    local daemon_pid=$!
    trap 'kill $daemon_pid $stub_pid 2>/dev/null || true' EXIT INT TERM

    wait_tcp "$stub_port" 15 stub
    wait_healthz "http://127.0.0.1:$daemon_port/healthz" 30
    log "daemon+stub up; running loadtest legs (${PROFILE_SECS}s each)"

    local base="http://127.0.0.1:$daemon_port"
    # Bool flags are Go-style: `-stream=false`, never `-stream false`
    # (a separate arg would set stream=true and leave a stray positional).
    timeout $((PROFILE_SECS + 60)) "$loadtest" \
        -url "$base/v1/chat/completions" -c 64 -duration "${PROFILE_SECS}s" \
        -key qa-pgo-key -model stub-model -stream=true 2>&1 | tail -4
    timeout $((PROFILE_SECS + 60)) "$loadtest" \
        -url "$base/v1/chat/completions" -c 256 -duration "${PROFILE_SECS}s" \
        -key qa-pgo-key -model stub-model -stream=true 2>&1 | tail -4
    timeout $((PROFILE_SECS / 2 + 60)) "$loadtest" \
        -url "$base/v1/chat/completions" -c 64 -duration "$((PROFILE_SECS / 2))s" \
        -key qa-pgo-key -model stub-model -stream=false 2>&1 | tail -4
    timeout $((PROFILE_SECS / 2 + 60)) "$loadtest" \
        -url "$base/v1/responses" -c 16 -duration "$((PROFILE_SECS / 2))s" \
        -key qa-pgo-key -body "$work/responses.json" 2>&1 | tail -4
    timeout $((PROFILE_SECS / 2 + 60)) "$loadtest" \
        -url "$base/v1/messages" -c 16 -duration "$((PROFILE_SECS / 2))s" \
        -key qa-pgo-key -body "$work/messages.json" 2>&1 | tail -4

    # Optional qa stress leg: adds WS turns + debug-on pipeline coverage.
    # Skipped (with a warning) when the qa binary or the recorded oracle
    # baseline is unavailable.
    if [ "$STRESS_REQUESTS" -gt 0 ]; then
        if [ ! -x "$ROOT/target/release/qa" ]; then
            log "building qa binary for the stress leg"
            timeout 5400 cargo build --locked --release --features qa --bin qa \
                || log "qa build failed — skipping stress leg"
        fi
        if [ -x "$ROOT/target/release/qa" ]; then
            cp "$ROOT/target/release/qa" "$TARGET_INSTRUMENTED/release/qa"
            log "qa stress leg: $STRESS_REQUESTS requests (SSE/JSON/WS, debug-on)"
            LLVM_PROFILE_FILE="$PGO_DIR/profraw/qa-stress-%p.profraw" \
                timeout 2400 "$TARGET_INSTRUMENTED/release/qa" stress \
                --requests "$STRESS_REQUESTS" --evidence "$PGO_DIR/qa-stress" \
                || log "qa stress leg failed — continuing with loadtest data only"
        fi
    fi

    # SIGTERM triggers the daemon's graceful drain; atexit flushes the
    # buffered profraw. Wait for exit (bounded), escalate to SIGKILL only
    # as a last resort — a SIGKILLed daemon writes no profile.
    kill "$daemon_pid" 2>/dev/null || true
    for _ in $(seq 1 300); do
        kill -0 "$daemon_pid" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$daemon_pid" 2>/dev/null; then
        log "WARNING: daemon did not exit within 30s of SIGTERM — SIGKILL (profile for this leg lost)"
        kill -9 "$daemon_pid" 2>/dev/null || true
    fi
    wait "$daemon_pid" 2>/dev/null || true
    kill "$stub_pid" 2>/dev/null || true
    trap - EXIT INT TERM
    local n
    n=$(find "$PGO_DIR/profraw" -name '*.profraw' | wc -l)
    log "profile phase done: $n profraw file(s) in $PGO_DIR/profraw"
    [ "$n" -gt 0 ] || die "no profraw produced — instrumentation not taking effect?"
}

phase_merge() {
    local profdata_bin; profdata_bin="$(llvm_profdata)"
    shopt -s nullglob
    local files=("$PGO_DIR"/profraw/*.profraw)
    [ "${#files[@]}" -gt 0 ] || die "no profraw in $PGO_DIR/profraw — run '$0 profile' first"
    log "merging ${#files[@]} profraw -> $PROFDATA"
    "$profdata_bin" merge -sparse -o "$PROFDATA" "${files[@]}"
    {
        printf 'git_sha=%s\n' "$(git rev-parse HEAD 2>/dev/null || echo unknown)"
        printf 'created=%s\n' "$(date -Is)"
        printf 'profraw_files=%s\n' "${#files[@]}"
    } > "$META"
    log "wrote $PROFDATA ($(du -h "$PROFDATA" | cut -f1)) + $META"
}

phase_build() {
    [ -f "$PROFDATA" ] || die "missing $PROFDATA — run '$0 merge' first"
    # Staleness warnings: a stale profile degrades toward ~0 gain, never
    # breaks the build — but silent staleness is how PGO "stops working".
    if [ -f "$META" ]; then
        local recorded current
        recorded="$(grep '^git_sha=' "$META" | cut -d= -f2)"
        current="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
        [ "$recorded" = "$current" ] || \
            log "WARNING: profile built at $recorded, HEAD is $current — re-run profile+merge"
        local age_days
        age_days=$(( ($(date +%s) - $(stat -c %Y "$PROFDATA")) / 86400 ))
        [ "$age_days" -le 14 ] || \
            log "WARNING: profile is ${age_days}d old (>14d) — re-run profile+merge"
    else
        log "WARNING: no $META — cannot check profile freshness"
    fi
    log "PGO-optimized build -> $TARGET_OPTIMIZED"
    RUSTFLAGS="-Cprofile-use=$PROFDATA -Cllvm-args=-pgo-warn-missing-function" \
    CARGO_TARGET_DIR="$TARGET_OPTIMIZED" \
        timeout 5400 cargo build --locked --release --bin devin-2api \
        2> >(tee "$PGO_DIR/pgo-warnings.log" >&2)
    local warns
    warns=$(grep -c 'warning' "$PGO_DIR/pgo-warnings.log" || true)
    log "optimized daemon: $TARGET_OPTIMIZED/release/devin-2api ($warns warning lines; see $PGO_DIR/pgo-warnings.log)"
    [ "$warns" -lt 200 ] || \
        log "WARNING: high warning count — profile likely stale, re-run profile+merge"
}

case "${1:-}" in
    instrument) phase_instrument ;;
    profile)    phase_profile ;;
    merge)      phase_merge ;;
    build)      phase_build ;;
    all)        phase_instrument; phase_profile; phase_merge; phase_build ;;
    *)          grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac

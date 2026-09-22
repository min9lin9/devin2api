#!/usr/bin/env bash
# Rust performance snapshot. Uses the ported loadtest and Linux perf rather
# than Go pprof; all processes and state are disposable.
set -euo pipefail
cd "$(dirname "$0")/.."
REQUESTS=100 CONCURRENCY=8 OUT="" PORT=3005 STUB_PORT=48190
while [[ $# -gt 0 ]]; do
  case "$1" in
    --requests) REQUESTS="$2"; shift 2;;
    --concurrency) CONCURRENCY="$2"; shift 2;;
    --out) OUT="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --stub-port) STUB_PORT="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[[ -n "$OUT" ]] || OUT="outputs/perf/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
WORK="$(mktemp -d -t devin2api-perf.XXXXXX)"; PIDS=()
cleanup(){ for p in "${PIDS[@]}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT
cargo build --locked --release --bin devin-2api --bin upstreamstub --bin loadtest
cat > "$WORK/config.yaml" <<EOF
server:
  listen: '127.0.0.1:$PORT'
devin:
  base_url: 'http://127.0.0.1:$STUB_PORT'
  token: synthetic
  model: stub
  force_http1: true
debug:
  enabled: true
dashboard:
  password: ''
auth:
  api_key: ''
EOF
target/release/upstreamstub -listen "127.0.0.1:$STUB_PORT" -scenario stream >"$WORK/stub.log" 2>&1 & PIDS+=("$!")
target/release/devin-2api -config "$WORK/config.yaml" -state-dir "$WORK/state" >"$WORK/server.log" 2>&1 & PIDS+=("$!")
# Follow the exact readiness log instead of sleeping/polling. grep exits as
# soon as the event arrives, which intentionally gives tail SIGPIPE; scope
# pipefail off around only this pipeline so grep's status remains authoritative.
set +o pipefail
if ! timeout 30 tail -n +1 -F "$WORK/server.log" | grep -m1 'HTTP server listening' >"$WORK/ready.log"; then
  set -o pipefail
  echo 'server readiness event not observed within 30s' >&2
  tail -20 "$WORK/server.log" >&2
  exit 1
fi
set -o pipefail
target/release/loadtest -url "http://127.0.0.1:$PORT/v1/chat/completions" -c "$CONCURRENCY" -n "$REQUESTS" | tee "$OUT/report.txt"
cp "$WORK/server.log" "$WORK/stub.log" "$OUT/"
if command -v perf >/dev/null && perf stat -e task-clock true >/dev/null 2>&1; then
  perf stat -p "${PIDS[1]}" -o "$OUT/perf-stat.txt" -- timeout 5 target/release/loadtest -url "http://127.0.0.1:$PORT/v1/chat/completions" -c "$CONCURRENCY" -duration 4s || true
else
  echo 'perf unavailable or not permitted' > "$OUT/perf-stat.txt"
fi
echo "snapshot: $OUT"

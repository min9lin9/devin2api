#!/usr/bin/env bash
# Capture the Rust benchmark/test timing baseline without weakening features.
set -euo pipefail
cd "$(dirname "$0")/.."
LABEL="${1:-bench-$(git rev-parse --short HEAD 2>/dev/null || echo local)}"
mkdir -p outputs/bench
OUT="outputs/bench/${LABEL}.txt"
echo "== cargo test --locked --release --workspace --all-features =="
/usr/bin/time -f 'wall_seconds=%e max_rss_kb=%M' \
  cargo test --locked --release --workspace --all-features 2>&1 | tee "$OUT"
echo "== saved $OUT =="

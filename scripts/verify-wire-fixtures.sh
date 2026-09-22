#!/usr/bin/env bash
# Cross-verify wire fixtures: Rust-produced bytes must decode identically
# under the Go protobuf oracle.
#
#   1. cargo run -p devin-proto --example emit_fixtures -- <tmp>/rust
#      decodes every checked-in Go .bin fixture with the generated buffa
#      types, re-encodes to <tmp>/rust/<name>.bin and writes serde proto-JSON
#      to <tmp>/rust/<name>.json.
#   2. The Go oracle (tests/fixtures/gen, mode "verify") decodes each Rust
#      .bin, marshals protojson, and compares it against the checked-in
#      Go .json oracle; it also compares the Rust .json output directly.
#
# Usage: scripts/verify-wire-fixtures.sh [work-dir]
# Requires: Go toolchain on PATH (test oracle only — never shipped).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="${1:-$(mktemp -d)}"
mkdir -p "$WORK/rust"

cargo run --locked -p devin-proto --example emit_fixtures -- "$WORK/rust"

GEN_DIR="crates/devin-proto/tests/fixtures/gen"
(cd "$GEN_DIR" && go build -o "$WORK/fixturegen" .)
"$WORK/fixturegen" verify crates/devin-proto/tests/fixtures "$WORK/rust" | tee "$WORK/verify.log"

echo "verify-wire-fixtures: all fixtures cross-verified (artifacts in $WORK)"

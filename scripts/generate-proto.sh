#!/usr/bin/env bash
# Regenerate the checked-in Devin protobuf bindings in crates/devin-proto.
#
# Pipeline (deterministic, no network):
#   proto/all-protos.fds  --(connectrpc-build descriptor_set + buffa codegen)-->
#   crates/devin-proto/src/generated/*.rs
#
# Inputs under proto/ are byte-identical copies of the Go reference's
# outputs/devin-proto/ artifacts (see proto/SHA256SUMS):
#   all-protos.proto  flattened proto2 schema (single package exa.api_server_pb)
#   descriptors.pb    original 63-file FileDescriptorSet (packages preserved)
#   manifest.json     extraction provenance + symbol mappings
#   all-protos.fds    FileDescriptorSet compiled from all-protos.proto
#
# all-protos.fds is the documented "precompiled descriptor set" input of
# connectrpc-build (Config::descriptor_set). It was produced from
# all-protos.proto with bufbuild/protocompile v0.14.1 (the compiler library
# already vendored in the Go reference's module graph for protoextract),
# because no protoc binary is installed on this machine. To rebuild it:
#   * with protoc:  protoc -I proto --descriptor_set_out=proto/all-protos.fds \
#                     --include_imports proto/all-protos.proto
#   * or rerun the protocompile helper recorded in the task-3 evidence
#     ($E/task-3/protocc/). Either input must yield name "all-protos.proto".
#
# Usage:
#   scripts/generate-proto.sh           regenerate in place
#   scripts/generate-proto.sh --check   verify checked-in output is identical
#                                       to a fresh regeneration (exit 1 on diff)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GEN_DIR="crates/devin-proto/src/generated"

# 1. Provenance: the three copied artifacts must match the Go reference bytes.
#    SHA256SUMS lists bare filenames relative to proto/.
(cd proto && sha256sum --check SHA256SUMS)

# 2. Regenerate into a temp dir, then swap in (or diff in --check mode).
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cargo run --locked -p devin-proto --example generate -- --out "$TMP/gen"

if [[ "${1:-}" == "--check" ]]; then
    diff -r --exclude=mod.rs "$GEN_DIR" "$TMP/gen" >/dev/null && {
        echo "generate-proto --check: generated sources are up to date"
        exit 0
    }
    echo "generate-proto --check: FAIL — generated sources differ; rerun scripts/generate-proto.sh" >&2
    diff -r --exclude=mod.rs "$GEN_DIR" "$TMP/gen" | head -50 >&2 || true
    exit 1
elif [[ $# -gt 0 ]]; then
    echo "usage: $0 [--check]" >&2
    exit 2
fi

rm -rf "$GEN_DIR"
mv "$TMP/gen" "$GEN_DIR"
echo "generate-proto: regenerated $GEN_DIR"

#!/usr/bin/env bash
# Build a version-stamped fixture, then run two deploy sandboxes concurrently.
# No caller-provided daemon and no pre-existing target/debug bytes are trusted.
set -euo pipefail
cd "$(dirname "$0")/.."

TAG=v9.9.9
if [[ -n "${1:-}" ]]; then
	WORK="$(mkdir -p "$1" && cd "$1" && pwd)"
else
	WORK="$(mktemp -d -t devin2api-deploy-concurrent.XXXXXX)"
	trap 'rm -rf "$WORK"' EXIT
fi
rm -rf "$WORK/bin" "$WORK/sandbox-a" "$WORK/sandbox-b"
mkdir -p "$WORK/bin"

# Build both executables in this invocation. Copy the tagged daemon out of
# target immediately, then restore a normal daemon build before testing: this
# proves the sandboxes consume the copied fixture rather than stale target/.
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" cargo build --locked --features qa --bin qa
cp target/debug/qa "$WORK/bin/qa"
DEVIN2API_BUILD_VERSION="$TAG" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
	cargo build --locked --bin devin-2api
cp target/debug/devin-2api "$WORK/bin/devin-2api-$TAG"
[[ "$("$WORK/bin/devin-2api-$TAG" -version)" == "$TAG" ]] || {
	echo "fixture binary was not stamped $TAG" >&2
	exit 1
}
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" cargo build --locked --bin devin-2api
NORMAL_VERSION="$(target/debug/devin-2api -version)"
[[ "$NORMAL_VERSION" != "$TAG" ]] || {
	echo "normal target unexpectedly still reports fixture tag $TAG" >&2
	exit 1
}
echo "fixture_version=$TAG"
echo "current_target_version=$NORMAL_VERSION"

bash scripts/deploy-sandbox.test.sh \
	"$WORK/bin/devin-2api-$TAG" "$WORK/bin/qa" "$WORK/sandbox-a" \
	>"$WORK/deploy-sandbox-a.log" 2>&1 &
A=$!
bash scripts/deploy-sandbox.test.sh \
	"$WORK/bin/devin-2api-$TAG" "$WORK/bin/qa" "$WORK/sandbox-b" \
	>"$WORK/deploy-sandbox-b.log" 2>&1 &
B=$!
rc=0
wait "$A" || rc=1
wait "$B" || rc=1
cat "$WORK/deploy-sandbox-a.log"
cat "$WORK/deploy-sandbox-b.log"
[[ "$rc" == 0 ]] || exit "$rc"

port_of() {
	sed -n "s/.*listen: '127.0.0.1:\([0-9]*\)'.*/\1/p" "$1/repo/config.yaml"
}
PA="$(port_of "$WORK/sandbox-a")"
PB="$(port_of "$WORK/sandbox-b")"
[[ -n "$PA" && -n "$PB" && "$PA" != "$PB" ]] || {
	echo "sandboxes did not receive distinct ephemeral ports: a=$PA b=$PB" >&2
	exit 1
}
printf 'sandbox_a_port=%s\nsandbox_b_port=%s\n' "$PA" "$PB" | tee "$WORK/ephemeral-ports.log"
echo 'deploy-sandbox-concurrent: all scenarios passed'

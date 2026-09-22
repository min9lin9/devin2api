#!/usr/bin/env bash
# deploy-sandbox.test.sh — drive the REAL scripts/deploy-linux.sh end-to-end
# in a disposable sandbox: stubbed systemctl --user (spawns/kills real
# processes per the generated unit), stubbed curl serving a local fixture
# "GitHub release", stubbed gh. No real service is installed; HOME/XDG dirs,
# BIN_DIR, CONFIG_DIR and STATE_DIR all live under the sandbox work dir.
#
# Scenarios:
#   install   — fresh install of the fixture release: unit generated,
#               service "started" (real process), healthz version polled,
#               /v1/models upstream smoke against a local stub upstream.
#   badsum    — upgrade attempt whose checksums.txt disagrees: deploy must
#               refuse, installed binary must remain the old one.
#   badhealth — upgrade to a NEW tag whose binary never serves healthz:
#               deploy must fail and roll back to the .previous binary (old
#               version serving again). A distinct tag is required: with a
#               same-tag redeploy the draining old instance still answers
#               healthz with the wanted version and the check cannot
#               distinguish it from the new instance (Go semantics).
#
# usage: deploy-sandbox.test.sh <daemon-binary> <qa-binary> [workdir]
#   <daemon-binary>  devin-2api built with DEVIN2API_BUILD_VERSION=v9.9.9
#                    (any profile; deploy semantics are profile-independent)
#   <qa-binary>      qa binary — its hidden `__http-upstream normal` child is
#                    the Connect stub upstream
#   [workdir]        sandbox root (default: mktemp -d, removed on exit)
set -euo pipefail

DAEMON="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
QA_BIN="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
REPO_R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REAL_CURL="$(command -v curl)"
TAG="v9.9.9"
BAD_TAG="v9.9.10"

if [[ -n "${3:-}" ]]; then
	SBX="$(mkdir -p "$3" && cd "$3" && pwd)"
else
	SBX="$(mktemp -d -t devin2api-deploy-sandbox.XXXXXX)"
	trap 'rm -rf "$SBX"' EXIT
fi

FIX="$SBX/fixtures"
STUB_SRC="$SBX/stubsrc"
mkdir -p "$SBX"/{home,bin,config,state,xdg-config,xdg-state,repo,stubbin,sysd} "$FIX" "$STUB_SRC"
cd "$SBX/repo"
cp "$REPO_R/config.example.yaml" .
mkdir -p scripts
cp "$REPO_R/scripts/"{deploy-linux.sh,lib-deploy.sh,rotate-logs.sh} scripts/

# ---------- fixtures (per-tag release dirs) ----------
mkdir -p "$FIX/$TAG" "$FIX/$BAD_TAG"
cp "$DAEMON" "$FIX/$TAG/devin-2api-linux-amd64"
sha256sum "$FIX/$TAG/devin-2api-linux-amd64" | awk '{print $1"  devin-2api-linux-amd64"}' >"$FIX/$TAG/checksums.txt"
# badhealth: -version answers the new tag but the process never binds.
printf '#!/usr/bin/env bash\n[[ "${1:-}" == "-version" ]] && { echo %s; exit 0; }\nexec sleep 3600\n' "$BAD_TAG" >"$FIX/$BAD_TAG/devin-2api-linux-amd64"
chmod +x "$FIX/$BAD_TAG/devin-2api-linux-amd64"
sha256sum "$FIX/$BAD_TAG/devin-2api-linux-amd64" | awk '{print $1"  devin-2api-linux-amd64"}' >"$FIX/$BAD_TAG/checksums.txt"
# badsum: corrupt bytes + a manifest that names a wrong sha for the asset.
printf 'corrupt-bytes\n' >"$FIX/badsum-asset"
echo "0000000000000000000000000000000000000000000000000000000000000000  devin-2api-linux-amd64" >"$FIX/checksums-badsum.txt"

# ---------- stub: systemctl ----------
# Fidelity notes vs real systemd:
#   * `cat UNIT` fails until the unit is "loaded" (daemon-reload/enable mark
#     it) — real systemctl cat only sees units in the loaded set, which is
#     what makes the deploy script's first-install branch fire.
#   * `enable --now UNIT` loads+starts services; timers are only marked
#     enabled (their oneshot does not run here).
#   * `start`/`restart` spawn the unit's ExecStart detached with its
#     Environment= and WorkingDirectory= honored, stdout/stderr appended to
#     the unit's StandardOutput=append:/StandardError=append: paths.
cat >"$SBX/stubbin/systemctl" <<'EOF'
#!/usr/bin/env bash
set -u
SYS="${SBX_SYS:?}"
UNITD="${SBX_UNITD:?}"
[[ "${1:-}" == "--user" ]] && shift
cmd="${1:-}"; shift || true
unit_file() { echo "$UNITD/$1"; }
pid_of() { cat "$SYS/${1%.service}.pid" 2>/dev/null || true; }
mark_loaded() { touch "$SYS/${1}.loaded"; }
is_loaded() { [[ -f "$SYS/${1}.loaded" ]]; }
unit_prop() { sed -n "s/^$2=//p" "$(unit_file "$1")" 2>/dev/null | head -1; }
start_unit() {
	local unit="$1" uf exec_line env_kv wd out err
	uf="$(unit_file "$unit")"
	[[ -f "$uf" ]] || { echo "stub: no unit $unit" >&2; return 1; }
	exec_line="$(unit_prop "$unit" ExecStart)"
	env_kv="$(sed -n 's/^Environment=//p' "$uf")"
	wd="$(unit_prop "$unit" WorkingDirectory)"
	out="$(unit_prop "$unit" StandardOutput)"; out="${out#append:}"
	err="$(unit_prop "$unit" StandardError)"; err="${err#append:}"
	[[ -n "$out" ]] || out="$SYS/${unit%.service}.stdout.log"
	[[ -n "$err" ]] || err="$SYS/${unit%.service}.stderr.log"
	[[ -n "$wd" ]] && cd "$wd"
	mkdir -p "$(dirname "$out")" "$(dirname "$err")"
	# shellcheck disable=SC2086
	env $env_kv nohup $exec_line >>"$out" 2>>"$err" &
	echo $! > "$SYS/${unit%.service}.pid"
	mark_loaded "$unit"
}
stop_unit() {
	local pid; pid="$(pid_of "$1")"
	[[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
	rm -f "$SYS/${1%.service}.pid"
}
case "$cmd" in
	daemon-reload)
		for f in "$UNITD"/*; do [[ -f "$f" ]] && mark_loaded "$(basename "$f")"; done
		exit 0 ;;
	cat)
		is_loaded "${1:-}" && cat "$(unit_file "$1")" || exit 1 ;;
	show)
		local_u="${@: -1}"
		pid="$(pid_of "$local_u")"
		if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then echo "$pid"; else echo 0; fi ;;
	enable)
		local now=0 u=""
		for a in "$@"; do [[ "$a" == "--now" ]] && now=1 || u="$a"; done
		mark_loaded "$u"
		touch "$SYS/${u%.service}.enabled"
		[[ "$now" == 1 && "$u" == *.service ]] && start_unit "$u"
		exit 0 ;;
	disable)
		local now=0 u=""
		for a in "$@"; do [[ "$a" == "--now" ]] && now=1 || u="$a"; done
		rm -f "$SYS/${u%.service}.enabled" "$SYS/${u}.loaded"
		[[ "$now" == 1 && "$u" == *.service ]] && stop_unit "$u"
		exit 0 ;;
	restart) stop_unit "${1:?}"; start_unit "$1" ;;
	start) start_unit "${1:?}" ;;
	stop) stop_unit "${1:?}" ;;
	is-active) pid="$(pid_of "${1:-}")"; [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && { echo active; exit 0; }; echo inactive; exit 3 ;;
	*) echo "stub systemctl: unhandled $cmd $*" >&2; exit 0 ;;
esac
EOF

# ---------- stub: curl ----------
# GitHub release/API URLs are served from $FIX; everything else (localhost
# healthz/models) is delegated to the real curl.
cat >"$SBX/stubbin/curl" <<EOF
#!/usr/bin/env bash
FIX="$FIX"; REAL_CURL="$REAL_CURL"; TAG="$TAG"
url="" out="" head=0 writeout=""
prev=""
for a in "\$@"; do
	case "\$prev" in
		-o) out="\$a" ;;
		-w) writeout="\$a" ;;
	esac
	prev="\$a"
	case "\$a" in
		-I) head=1 ;;
		http*://*) url="\$a" ;;
	esac
done
case "\$url" in
	*github.com*|*api.github.com*)
		case "\$url" in
			*/releases/latest)
				if [[ "\$head" == 1 ]]; then
					[[ "\$writeout" == *redirect_url* ]] && printf 'https://github.com/min9lin9/devin2api/releases/tag/%s' "\$TAG"
					exit 0
				fi
				printf '{"tag_name":"%s"}' "\$TAG"
				exit 0 ;;
			*/releases/download/*)
				asset="\${url##*/}"
				tag="\$(printf '%s' "\$url" | sed -n 's#.*/download/\([^/]*\)/.*#\1#p')"
				file="\$FIX/\$tag/\$asset"
				# FIX_MODE=badsum serves corrupt bytes and a wrong-sha manifest.
				if [[ "\${FIX_MODE:-}" == badsum ]]; then
					if [[ "\$asset" == "checksums.txt" ]]; then
						file="\$FIX/checksums-badsum.txt"
					else
						file="\$FIX/badsum-asset"
					fi
				fi
				[[ -f "\$file" ]] || exit 22
				if [[ -n "\$out" ]]; then cp "\$file" "\$out"; else cat "\$file"; fi
				exit 0 ;;
			*api.github.com*)
				printf '{"tag_name":"%s"}' "\$TAG"
				exit 0 ;;
		esac ;;
	"")
		exit 2 ;;
	*)
		exec "\$REAL_CURL" "\$@" ;;
esac
exit 0
EOF

# ---------- stub: gh ----------
cat >"$SBX/stubbin/gh" <<'EOF'
#!/usr/bin/env bash
[[ "${1:-} ${2:-}" == "auth token" ]] && { echo fake-token; exit 0; }
exit 1
EOF
chmod +x "$SBX/stubbin/"*

# ---------- sandbox env ----------
export HOME="$SBX/home"
export XDG_CONFIG_HOME="$SBX/xdg-config"
export XDG_STATE_HOME="$SBX/xdg-state"
export SBX_SYS="$SBX/sysd"
export SBX_UNITD="$XDG_CONFIG_HOME/systemd/user"
export PATH="$SBX/stubbin:$PATH"
export DEVIN2API_BIN_DIR="$SBX/bin"
export DEVIN2API_CONFIG_DIR="$SBX/config"
export DEVIN2API_STATE_DIR="$SBX/state"
export DEVIN2API_HEALTH_TIMEOUT_SECS="${DEVIN2API_HEALTH_TIMEOUT_SECS:-60}"
mkdir -p "$SBX_UNITD"

# ---------- upstream stub (qa __http-upstream normal; prints READY <addr>) ----------
mkfifo "$SBX/upstream.ready"
"$QA_BIN" __http-upstream normal >"$SBX/upstream.ready" 2>"$SBX/upstream.log" &
UPSTUB=$!
cleanup() {
	kill "$UPSTUB" 2>/dev/null || true
	for p in "$SBX/sysd"/*.pid; do
		[[ -f "$p" ]] && kill "$(cat "$p")" 2>/dev/null || true
	done
}
trap cleanup EXIT
STUB_ADDR="$(timeout 15 head -1 "$SBX/upstream.ready")"
STUB_ADDR="${STUB_ADDR#READY }"
[[ -n "$STUB_ADDR" ]] || { echo "upstream stub failed:"; cat "$SBX/upstream.log"; exit 1; }
STUB_PORT="${STUB_ADDR##*:}"

# ---------- fixture config: loopback port + stub upstream ----------
# Ask the kernel for an ephemeral port instead of using a fixed test port.
# FIX_PORT remains available for callers that deliberately reserve one.
allocate_port() {
	python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}
PORT="${FIX_PORT:-$(allocate_port)}"
cat >"$SBX/repo/config.yaml" <<EOF
server:
  listen: '127.0.0.1:$PORT'
devin:
  base_url: 'http://127.0.0.1:$STUB_PORT'
  token: synthetic
  model: stub-model
  force_http1: true
debug:
  enabled: true
dashboard:
  password: ''
auth:
  api_key: ''
EOF

FAILED=0
pass() { echo "ok    $1"; }
fail() { echo "FAIL  $1"; FAILED=1; }
healthz_version() { curl -sf "http://127.0.0.1:$PORT/healthz" 2>/dev/null | sed -n 's/.*"version" *: *"\([^"]*\)".*/\1/p'; }

echo "== scenario: fresh install of fixture release $TAG =="
if bash scripts/deploy-linux.sh --release "$TAG" >"$SBX/install.log" 2>&1; then
	pass "deploy --release $TAG exit 0"
else
	fail "deploy --release $TAG"; sed 's/^/    /' "$SBX/install.log" | tail -20
fi
grep -q "installed $SBX/bin/devin-2api" "$SBX/install.log" && pass "binary installed to BIN_DIR" || fail "binary installed"
[[ -f "$SBX_UNITD/devin-2api.service" ]] && pass "unit generated" || fail "unit generated"
grep -q "DEVIN2API_REUSEPORT=1" "$SBX_UNITD/devin-2api.service" && pass "unit carries reuseport env" || fail "reuseport env"
[[ -f "$SBX_UNITD/devin-2api-logrotate.timer" ]] && pass "logrotate timer generated" || fail "logrotate timer"
v="$(healthz_version)"
[[ "$v" == "$TAG" ]] && pass "healthz serves $TAG" || fail "healthz version ($v)"
grep -q "upstream auth verified" "$SBX/install.log" && pass "upstream smoke passed" || fail "upstream smoke"

echo "== scenario: bad checksum refuses, old install retained =="
old_sha="$(sha256sum "$SBX/bin/devin-2api" | awk '{print $1}')"
if FIX_MODE=badsum bash scripts/deploy-linux.sh --release "$TAG" >"$SBX/badsum.log" 2>&1; then
	fail "bad checksum deploy unexpectedly succeeded"
else
	pass "bad checksum deploy refused (nonzero)"
fi
grep -q "sha256 mismatch" "$SBX/badsum.log" && pass "mismatch reported" || fail "mismatch reported"
new_sha="$(sha256sum "$SBX/bin/devin-2api" | awk '{print $1}')"
[[ "$new_sha" == "$old_sha" ]] && pass "installed binary unchanged" || fail "installed binary changed"
[[ ! -e "$SBX/repo/devin-2api.new" ]] && pass "temp artifact removed" || fail "temp artifact left"
v="$(healthz_version)"
[[ "$v" == "$TAG" ]] && pass "old instance still serving" || fail "old instance serving ($v)"

echo "== scenario: failed health check rolls back ($TAG -> $BAD_TAG) =="
if DEVIN2API_HEALTH_TIMEOUT_SECS=15 bash scripts/deploy-linux.sh --release "$BAD_TAG" >"$SBX/badhealth.log" 2>&1; then
	fail "failed-health deploy unexpectedly succeeded"
else
	pass "failed-health deploy failed (nonzero)"
fi
grep -q "healthz 未出现新版本" "$SBX/badhealth.log" && pass "health timeout reported" || fail "health timeout reported"
grep -q "rolled back" "$SBX/badhealth.log" && pass "rollback executed" || fail "rollback executed"
[[ "$("$SBX/bin/devin-2api" -version 2>/dev/null)" == "$TAG" ]] && pass "installed binary is old version" || fail "installed binary version"
v="$(healthz_version)"
[[ "$v" == "$TAG" ]] && pass "old version serving after rollback" || fail "old version serving ($v)"

echo
if [[ "$FAILED" == 1 ]]; then
	echo "deploy-sandbox: FAILURES" >&2
	exit 1
fi
echo "deploy-sandbox: all scenarios passed"

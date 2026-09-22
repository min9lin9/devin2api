#!/usr/bin/env bash
# Offline deployment/release contract checks. Never installs a real service.
set -euo pipefail
cd "$(dirname "$0")/.."
FAILED=0
check(){ local d="$1"; shift; if "$@"; then echo "ok    $d"; else echo "FAIL  $d"; FAILED=1; fi; }
has(){ grep -qF -- "$2" "$1"; }

echo '== syntax =='
for script in scripts/*.sh; do check "bash -n $script" bash -n "$script"; done

echo '== Rust build/deploy contract =='
check 'source deploy uses locked Cargo release build' has scripts/lib-deploy.sh 'cargo build --locked --release --bin devin-2api'
check 'release source is min9lin9' has scripts/lib-deploy.sh 'min9lin9/devin2api'
check 'checksums required before install' has scripts/lib-deploy.sh 'checksums.txt'
check 'checksum helper exists' has scripts/lib-deploy.sh 'verify_checksum'
check 'previous binary retained' has scripts/lib-deploy.sh 'devin-2api.previous'
check 'Linux failed health rolls back' has scripts/deploy-linux.sh 'rollback_binary'
check 'macOS failed health rolls back' has scripts/deploy.sh 'rollback_binary'
check 'stray matching uses executable name' has scripts/lib-deploy.sh 'pgrep -x'
check 'no pgrep -f false positives' bash -c "! grep -vE '^\\s*#' scripts/lib-deploy.sh | grep -qF 'pgrep -f'"

echo '== service definitions =='
check 'launchd only on Darwin' has scripts/deploy.sh 'Darwin'
check 'launchd KeepAlive' has scripts/deploy.sh 'KeepAlive'
check 'launchd graceful timeout' has scripts/deploy.sh 'ExitTimeOut'
check 'systemd user service' has scripts/deploy-linux.sh 'systemctl --user'
check 'systemd restart policy' has scripts/deploy-linux.sh 'Restart=always'
check 'systemd graceful timeout' has scripts/deploy-linux.sh 'TimeoutStopSec=660'
check 'daily Linux log rotation' has scripts/deploy-linux.sh 'OnCalendar=daily'
check 'daily macOS log rotation' has scripts/deploy.sh 'StartInterval'
check 'uninstall preserves config/logs' has scripts/deploy-linux.sh '保留 ${CONFIG_DIR}/config.yaml 与 ${STATE_DIR}/logs/'

echo '== Windows =='
check 'PowerShell asset exists' test -f scripts/deploy-windows.ps1
check 'PowerShell UTF-8 BOM' bash -c "head -c3 scripts/deploy-windows.ps1 | od -An -tx1 | tr -d ' ' | grep -qx efbbbf"
check 'Windows Rust build' has scripts/deploy-windows.ps1 'cargo build --locked --release --bin devin-2api'
check 'Windows ZIP asset' has scripts/deploy-windows.ps1 'devin-2api-windows-amd64.zip'
check 'Windows checksum' has scripts/deploy-windows.ps1 'Get-FileHash'
check 'Windows rollback' has scripts/deploy-windows.ps1 'devin-2api.previous.exe'

echo '== release/container =='
check 'six target mappings' bash -c "[[ $(wc -l < packaging/targets.tsv) -eq 6 ]]"
check 'release VERSION at Rust root' has scripts/release.sh 'VERSION_FILE="VERSION"'
check 'publish remains explicit' has scripts/release.sh '--publish'
check 'publish waits for CI' has scripts/release.sh 'workflow_runs'
check 'Docker locked release build' has Dockerfile 'cargo build --locked --release --bin devin-2api'
check 'Docker verifies embedded version' has Dockerfile '/tmp/devin-2api -version'
check 'release image copies matrix binary' has Dockerfile.release 'dist/devin-2api-linux-${TARGETARCH}'
check 'GHCR canonical target documented' bash -c "grep -qF 'ghcr.io/min9lin9/devin2api' scripts/release-selftest.sh"
check 'perf readiness scopes expected SIGPIPE out of pipefail' has scripts/perf-snapshot.sh 'set +o pipefail'
check 'deploy sandbox asks kernel for ephemeral port' has scripts/deploy-sandbox.test.sh 'sock.bind(("127.0.0.1", 0))'
check 'deploy sandbox has no fixed legacy port' bash -c "! grep -qF '43971' scripts/deploy-sandbox.test.sh"
check 'concurrent sandbox builds stamped fixture itself' has scripts/deploy-sandbox-concurrent.test.sh 'DEVIN2API_BUILD_VERSION="$TAG"'
check 'concurrent sandbox copies fixture out of target' has scripts/deploy-sandbox-concurrent.test.sh 'cp target/debug/devin-2api "$WORK/bin/devin-2api-$TAG"'
check 'concurrent sandbox restores ordinary target before run' has scripts/deploy-sandbox-concurrent.test.sh 'NORMAL_VERSION="$(target/debug/devin-2api -version)"'

if [[ "$FAILED" == 1 ]]; then echo 'deployment asset checks failed' >&2; exit 1; fi
echo 'all deployment asset checks passed'

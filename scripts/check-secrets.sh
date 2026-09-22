#!/usr/bin/env bash
# check-secrets.sh — reject committed credentials in the repository tree.
#
# High-signal patterns only (private-key blocks, well-known token prefixes,
# quoted secret assignments); fixture placeholders are excluded by the
# allowlist below. Build artifacts, vendored deps and generated output are
# not scanned. Exit 1 on any hit.
set -euo pipefail
cd "$(dirname "$0")/.."

PATTERNS=(
    'BEGIN [A-Z ]*PRIVATE KEY'
    'ghp_[A-Za-z0-9]{20,}'
    'github_pat_[A-Za-z0-9_]{20,}'
    'xox[baprs]-[A-Za-z0-9-]{10,}'
    'AKIA[0-9A-Z]{16}'
    'AIza[0-9A-Za-z_-]{35}'
    'sk-[A-Za-z0-9]{20,}'
    'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'
    '(password|secret|api[_-]?key|token)["'"'"']?[[:space:]]*[:=][[:space:]]*["'"'"'][A-Za-z0-9+/=_.-]{20,}["'"'"']'
)

ALLOW='synthetic|fixture|example|dummy|placeholder|your[_-]|xxxxx|changeme|test-|header\.payload|<[a-z_-]+>'

hits=0
for pat in "${PATTERNS[@]}"; do
    out="$(grep -rInE --binary-files=without-match \
        --exclude-dir=target --exclude-dir='target-*' \
        --exclude-dir=node_modules --exclude-dir=test-results \
        --exclude-dir=dist --exclude-dir=outputs --exclude-dir=.git \
        --exclude=check-secrets.sh \
        -- "$pat" . 2>/dev/null | grep -vEi "$ALLOW" || true)"
    if [[ -n "$out" ]]; then
        echo "possible committed secret (pattern: $pat):" >&2
        echo "$out" | head -20 >&2
        hits=$((hits + 1))
    fi
done

if [[ "$hits" -gt 0 ]]; then
    echo "secret scan failed: $hits pattern(s) matched" >&2
    exit 1
fi
echo "secret scan ok: no committed credentials found"

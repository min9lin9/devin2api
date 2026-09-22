#!/usr/bin/env bash
# check-licenses.sh — verify THIRD_PARTY_NOTICES.md covers exactly the
# third-party crate set locked in Cargo.lock (name+version multiset).
#
# The plan requires every dependency to carry a license entry verified from
# the resolved manifests; this script is the CI gate for that rule. It is
# offline: Cargo.lock is the source of truth for the resolved set, and the
# notices table is the recorded license attribution.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 - <<'PY'
import re
import sys
import tomllib
from collections import Counter
from pathlib import Path

lock = tomllib.loads(Path("Cargo.lock").read_text())
# Workspace members have no `source`; only external packages need notices.
locked = Counter(
    (p["name"], p["version"]) for p in lock["package"] if "source" in p
)

listed = Counter()
for line in Path("THIRD_PARTY_NOTICES.md").read_text().splitlines():
    m = re.match(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*$", line)
    if not m:
        continue
    name, version, license_ = (c.strip() for c in m.groups())
    if name in ("Crate", "---") or set(name) <= {"-"}:
        continue
    if not license_ or set(license_) <= {"-"}:
        print(f"notices row for {name} {version} has no license", file=sys.stderr)
        sys.exit(1)
    listed[(name, version)] += 1

missing = locked - listed   # locked but not listed
stale = listed - locked     # listed but no longer locked

for name, version in sorted(missing):
    print(f"missing notice: {name} {version}", file=sys.stderr)
for name, version in sorted(stale):
    print(f"stale notice (not in Cargo.lock): {name} {version}", file=sys.stderr)

if missing or stale:
    print(
        f"license check failed: {sum(missing.values())} missing, "
        f"{sum(stale.values())} stale",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"license check ok: {sum(locked.values())} locked third-party crates all noticed")
PY

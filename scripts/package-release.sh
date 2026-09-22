#!/usr/bin/env bash
# Package one prebuilt Rust target using the stable six-asset release contract.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="" BINARY="" OUT=dist VERSION="${DEVIN2API_BUILD_VERSION:-}"
usage() { echo "usage: $0 --target RUST_TARGET --binary PATH [--out DIR] [--version vX.Y.Z]"; }
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target) TARGET="${2:?--target needs a value}"; shift 2 ;;
    --binary) BINARY="${2:?--binary needs a value}"; shift 2 ;;
    --out) OUT="${2:?--out needs a value}"; shift 2 ;;
    --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$TARGET" && -n "$BINARY" ]] || { usage >&2; exit 2; }
[[ -f "$BINARY" ]] || { echo "binary not found: $BINARY" >&2; exit 1; }

row="$(awk -F '\t' -v t="$TARGET" '$1==t {print $2 "\t" $3}' packaging/targets.tsv)"
[[ -n "$row" ]] || { echo "unsupported release target: $TARGET" >&2; exit 1; }
asset="${row%%$'\t'*}"; kind="${row#*$'\t'}"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
rm -f "$OUT/$asset"
if [[ "$kind" == binary ]]; then
  install -m 0755 "$BINARY" "$OUT/$asset"
else
  work="$(mktemp -d -t devin2api-package.XXXXXX)"
  trap 'rm -rf "$work"' EXIT
  install -m 0755 "$BINARY" "$work/devin-2api.exe"
  cp config.example.yaml LICENSE "$work/"
  python3 - "$work" "$OUT/$asset" <<'PY'
import pathlib, sys, zipfile
root, output = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for name in ("devin-2api.exe", "config.example.yaml", "LICENSE"):
        info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (0o100755 if name.endswith(".exe") else 0o100644) << 16
        archive.writestr(info, (root / name).read_bytes())
PY
fi
# Recompute a deterministic, sorted manifest for every asset currently present.
(
  cd "$OUT"
  : > checksums.txt.new
  for file in devin-2api-*; do
    [[ "$file" == *.new ]] && continue
    [[ -f "$file" ]] || continue
    if command -v sha256sum >/dev/null; then sha256sum "$file"; else shasum -a 256 "$file"; fi
  done | sort -k2 > checksums.txt.new
  mv checksums.txt.new checksums.txt
)
if [[ -n "$VERSION" ]]; then printf '%s\n' "$VERSION" > "$OUT/VERSION"; fi
echo "$OUT/$asset"

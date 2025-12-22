#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DB="$DIR/northwind.sqlite"

if [[ -f "$DB" ]]; then
  echo "OK: DB already exists: $DB"
  exit 0
fi

cat <<'MSG'
Northwind SQLite DB not found at:
  examples/northwind/northwind.sqlite

This script will try to download a known Northwind SQLite file and save it there.
If you prefer, you can place any Northwind SQLite DB at that path yourself.
MSG

URL="https://github.com/jpwhite3/northwind-SQLite3/raw/master/Northwind_small.sqlite"
EXPECTED_SHA256="${NW_DB_SHA256:-}"

echo "Downloading: $URL"
tmp="$(mktemp)"
cleanup() { rm -f "$tmp"; }
trap cleanup EXIT

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$URL" -o "$tmp"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$tmp" "$URL"
else
  echo "ERROR: need curl or wget to download; alternatively place the DB at $DB"
  exit 2
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha="$(sha256sum "$tmp" | awk '{print $1}')"
  if [[ -n "$EXPECTED_SHA256" && "$actual_sha" != "$EXPECTED_SHA256" ]]; then
    echo "ERROR: sha256 mismatch for $DB" >&2
    echo "Expected: $EXPECTED_SHA256" >&2
    echo "Actual:   $actual_sha" >&2
    exit 3
  fi
  if [[ -z "$EXPECTED_SHA256" ]]; then
    echo "SHA256: $actual_sha"
    echo "Tip: pin this download by setting: NW_DB_SHA256=$actual_sha"
  fi
else
  echo "WARN: sha256sum not found; skipping checksum verification."
fi

mv "$tmp" "$DB"
trap - EXIT

echo "Saved: $DB"
echo "Next: python3 examples/northwind/01_ingest.py"

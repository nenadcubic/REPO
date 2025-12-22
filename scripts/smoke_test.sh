#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

ER_CLI="${ER_CLI:-$ROOT/build/cli/er_cli}"
ER_REDIS_HOST="${ER_REDIS_HOST:-localhost}"
ER_REDIS_PORT="${ER_REDIS_PORT:-6379}"
ER_PREFIX="${ER_PREFIX:-er}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing required command: $1" >&2; exit 2; }; }
need redis-cli

if [[ ! -x "$ER_CLI" ]]; then
  echo "ERROR: er_cli not found/executable: $ER_CLI" >&2
  echo "Build it (docker dev):" >&2
  echo "  env UID=\$(id -u) GID=\$(id -g) docker compose -f docker/docker-compose.yml up -d --build" >&2
  echo "  docker compose -f docker/docker-compose.yml exec dev bash" >&2
  echo "  cmake -S /work -B /work/build -G Ninja && cmake --build /work/build -j" >&2
  exit 2
fi

if [[ -z "$ER_PREFIX" || ! "$ER_PREFIX" =~ ^[A-Za-z0-9][A-Za-z0-9:_-]*$ ]]; then
  echo "ERROR: unsafe ER_PREFIX: ${ER_PREFIX@Q}" >&2
  exit 2
fi

redis() { redis-cli -h "$ER_REDIS_HOST" -p "$ER_REDIS_PORT" "$@"; }
redis_raw() { redis-cli --raw -h "$ER_REDIS_HOST" -p "$ER_REDIS_PORT" "$@"; }

if ! redis PING >/dev/null 2>&1; then
  echo "ERROR: Redis not reachable at $ER_REDIS_HOST:$ER_REDIS_PORT" >&2
  exit 2
fi

echo "Redis: $ER_REDIS_HOST:$ER_REDIS_PORT"
echo "er_cli: $ER_CLI"
echo "Prefix: $ER_PREFIX:"
echo

echo "Resetting keys: $ER_PREFIX:*"
if redis-cli -h "$ER_REDIS_HOST" -p "$ER_REDIS_PORT" --scan --pattern "$ER_PREFIX:*" >/dev/null 2>&1; then
  while IFS= read -r k; do
    [[ -z "$k" ]] && continue
    redis DEL "$k" >/dev/null
  done < <(redis-cli -h "$ER_REDIS_HOST" -p "$ER_REDIS_PORT" --scan --pattern "$ER_PREFIX:*")
else
  echo "WARN: redis-cli --scan not available; skipping reset" >&2
fi

echo "Seeding elements..."
"$ER_CLI" put alice 1 42 >/dev/null
"$ER_CLI" put bob 1 7 >/dev/null
"$ER_CLI" put carol 7 42 >/dev/null

assert_count() {
  local out="$1" want="$2" label="$3"
  local got
  got="$(printf '%s\n' "$out" | awk -F': ' '/^Count: /{print $2; exit}')"
  if [[ "$got" != "$want" ]]; then
    echo "ERROR: $label: expected Count=$want, got=${got:-<missing>}" >&2
    echo "$out" >&2
    exit 1
  fi
}

echo "Query: find 42 (expect alice, carol)"
OUT="$("$ER_CLI" find 42)"
assert_count "$OUT" "2" "find 42"

echo "Query: find_all 1 42 (expect alice)"
OUT="$("$ER_CLI" find_all 1 42)"
assert_count "$OUT" "1" "find_all 1 42"

echo "Query: find_any 7 42 (expect alice, bob, carol)"
OUT="$("$ER_CLI" find_any 7 42)"
assert_count "$OUT" "3" "find_any 7 42"

echo "Query: find_not 42 7 (expect alice)"
OUT="$("$ER_CLI" find_not 42 7)"
assert_count "$OUT" "1" "find_not 42 7"

echo "Store+TTL: find_all_store 30 1 42"
TMP="$("$ER_CLI" --keys-only find_all_store 30 1 42)"
if [[ -z "$TMP" ]]; then
  echo "ERROR: expected tmp key output from find_all_store" >&2
  exit 1
fi
TTL="$(redis TTL "$TMP")"
if [[ "$TTL" -le 0 ]]; then
  echo "ERROR: expected TTL > 0 for $TMP, got $TTL" >&2
  exit 1
fi
if [[ "$(redis SCARD "$TMP")" -le 0 ]]; then
  echo "ERROR: expected non-empty set for $TMP" >&2
  exit 1
fi
"$ER_CLI" show "$TMP" >/dev/null

echo "Store+TTL: find_all_not_store 30 42 7 (expect alice)"
TMP2="$("$ER_CLI" --keys-only find_all_not_store 30 42 7)"
TTL2="$(redis TTL "$TMP2")"
if [[ "$TTL2" -le 0 ]]; then
  echo "ERROR: expected TTL > 0 for $TMP2, got $TTL2" >&2
  exit 1
fi
if [[ "$(redis SCARD "$TMP2")" -le 0 ]]; then
  echo "ERROR: expected non-empty set for $TMP2" >&2
  exit 1
fi
"$ER_CLI" show "$TMP2" >/dev/null

echo "OK: smoke test passed"

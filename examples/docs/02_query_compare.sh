#!/usr/bin/env bash
set -euo pipefail

DOCS_REDIS_HOST="${DOCS_REDIS_HOST:-localhost}"
DOCS_REDIS_PORT="${DOCS_REDIS_PORT:-6379}"
PREFIX="${DOCS_PREFIX:-docs}"
PREFIX="${PREFIX%:}"
TTL_SEC="${DOCS_TTL_SEC:-600}"
DOCS_CLEAN_TMP="${DOCS_CLEAN_TMP:-0}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing required command: $1" >&2; exit 2; }; }
need redis-cli
need python3

if [[ -z "$PREFIX" || ! "$PREFIX" =~ ^[A-Za-z0-9][A-Za-z0-9:_-]*$ ]]; then
  echo "ERROR: unsafe DOCS_PREFIX: ${PREFIX@Q}" >&2
  exit 2
fi

redis() { redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" "$@"; }
redis_raw() { redis-cli --raw -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" "$@"; }

tmp_key() {
  local tag="$1"
  printf '%s:tmp:%s:%s:%s' "$PREFIX" "$tag" "$(date +%s)" "$RANDOM$RANDOM"
}

set_store_with_ttl() {
  local op="$1" dest="$2" ttl="$3"
  shift 3
  local out
  out="$({
    printf 'MULTI\n'
    printf '%s %s' "$op" "$dest"
    for k in "$@"; do printf ' %s' "$k"; done
    printf '\n'
    printf 'EXPIRE %s %s\n' "$dest" "$ttl"
    printf 'EXEC\n'
  } | redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" 2>&1)" || {
    echo "ERROR: redis-cli failed for: $op $dest" >&2
    echo "$out" >&2
    exit 1
  }
  if printf '%s\n' "$out" | grep -qiE '\\(error\\)|-ERR|-WRONGTYPE|-NOAUTH|-READONLY|-MOVED|-ASK|EXECABORT'; then
    echo "ERROR: Redis error for: $op $dest" >&2
    echo "$out" >&2
    exit 1
  fi
}

py_expected() {
  local query="$1"
  python3 - "$query" <<'PY'
import re, sys

DOCS = [
    ("d1", "redis atomic store ttl multi exec"),
    ("d2", "redis lua store ttl atomic"),
    ("d3", "sqlite sql join where orderdate"),
    ("d4", "redis sets union inter diff presjek"),
    ("d5", "lua script atomic expire"),
    ("d6", "northwind orders customers germany"),
]

def toks(s: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", s.lower()) if t}

query = sys.argv[1]
by_id = {doc_id: toks(text) for doc_id, text in DOCS}

if query == "redis_and_atomic":
    ids = {i for i, ts in by_id.items() if {"redis","atomic"} <= ts}
elif query == "redis_and_atomic_not_lua":
    ids = {i for i, ts in by_id.items() if {"redis","atomic"} <= ts and "lua" not in ts}
elif query == "not_redis":
    ids = {i for i, ts in by_id.items() if "redis" not in ts}
else:
    raise SystemExit(f"unknown query: {query}")

for i in sorted(ids):
    print(i)
PY
}

compare_py_vs_set() {
  local label="$1" query="$2" setkey="$3"
  local a b
  a="$(mktemp)"
  b="$(mktemp)"
  cleanup() { rm -f "$a" "$b"; }
  trap cleanup RETURN

  py_expected "$query" >"$a"
  redis_raw SMEMBERS "$setkey" | sed '/^$/d' | LC_ALL=C sort >"$b"
  LC_ALL=C sort -o "$a" "$a"

  local n1 n2
  n1="$(wc -l <"$a" | tr -d ' ')"
  n2="$(wc -l <"$b" | tr -d ' ')"
  echo "== $label =="
  echo "Py count:   $n1"
  echo "Redis count:$n2 (key: $setkey)"
  if ! diff -u "$a" "$b" >/dev/null; then
    echo "ERROR: mismatch: $label" >&2
    diff -u "$a" "$b" | head -n 80 >&2 || true
    exit 1
  fi
  echo "OK: match"
  echo
}

if [[ "$DOCS_CLEAN_TMP" == "1" ]]; then
  echo "Cleaning tmp keys: $PREFIX:tmp:*"
  if redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*" >/dev/null 2>&1; then
    while IFS= read -r k; do
      [[ -z "$k" ]] && continue
      redis DEL "$k" >/dev/null
    done < <(redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*")
  fi
  echo
fi

redis PING >/dev/null

K_ALL="$PREFIX:all"
K_REDIS="$PREFIX:term:redis"
K_ATOMIC="$PREFIX:term:atomic"
K_LUA="$PREFIX:term:lua"

TMP_REDIS_ATOMIC="$(tmp_key redis_and_atomic)"
set_store_with_ttl SINTERSTORE "$TMP_REDIS_ATOMIC" "$TTL_SEC" "$K_REDIS" "$K_ATOMIC"

TMP_REDIS_ATOMIC_NOT_LUA="$(tmp_key redis_and_atomic_not_lua)"
set_store_with_ttl SDIFFSTORE "$TMP_REDIS_ATOMIC_NOT_LUA" "$TTL_SEC" "$TMP_REDIS_ATOMIC" "$K_LUA"

compare_py_vs_set "docs: redis AND atomic" "redis_and_atomic" \
  "$TMP_REDIS_ATOMIC"

compare_py_vs_set "docs: (redis AND atomic) NOT lua" "redis_and_atomic_not_lua" \
  "$TMP_REDIS_ATOMIC_NOT_LUA"

TMP_NOT_REDIS="$(tmp_key not_redis)"
set_store_with_ttl SDIFFSTORE "$TMP_NOT_REDIS" "$TTL_SEC" "$K_ALL" "$K_REDIS"
compare_py_vs_set "docs: NOT redis (set-diff)" "not_redis" \
  "$TMP_NOT_REDIS"

echo "DONE"

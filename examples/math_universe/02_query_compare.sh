#!/usr/bin/env bash
set -euo pipefail

MU_REDIS_HOST="${MU_REDIS_HOST:-localhost}"
MU_REDIS_PORT="${MU_REDIS_PORT:-6379}"
PREFIX="${MU_PREFIX:-mu}"
PREFIX="${PREFIX%:}"
TTL_SEC="${MU_TTL_SEC:-600}"
MAX_N="${MU_MAX_N:-100}"
MU_CLEAN_TMP="${MU_CLEAN_TMP:-0}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing required command: $1" >&2; exit 2; }; }
need redis-cli
need python3

if [[ -z "$PREFIX" || ! "$PREFIX" =~ ^[A-Za-z0-9][A-Za-z0-9:_-]*$ ]]; then
  echo "ERROR: unsafe MU_PREFIX: ${PREFIX@Q}" >&2
  exit 2
fi

redis() { redis-cli -h "$MU_REDIS_HOST" -p "$MU_REDIS_PORT" "$@"; }
redis_raw() { redis-cli --raw -h "$MU_REDIS_HOST" -p "$MU_REDIS_PORT" "$@"; }

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
  } | redis-cli -h "$MU_REDIS_HOST" -p "$MU_REDIS_PORT" 2>&1)" || {
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

compare_py_vs_set() {
  local label="$1" query="$2" setkey="$3"
  local a b
  a="$(mktemp)"
  b="$(mktemp)"
  cleanup() { rm -f "$a" "$b"; }
  trap cleanup RETURN

  python3 - "$query" "$MAX_N" >"$a" <<'PY'
import sys, math
query = sys.argv[1]
max_n = int(sys.argv[2])

def is_prime(n: int) -> bool:
    if n < 2: return False
    if n % 2 == 0: return n == 2
    d = 3
    while d * d <= n:
        if n % d == 0: return False
        d += 2
    return True

U = set(range(1, max_n + 1))
even = {n for n in U if n % 2 == 0}
mod3 = {n for n in U if n % 3 == 0}
prime = {n for n in U if is_prime(n)}
gt50 = {n for n in U if n > 50}

if query == "even_and_mod3":
    S = even & mod3
elif query == "even_and_mod3_not_prime":
    S = (even & mod3) - prime
elif query == "gt50_or_prime":
    S = gt50 | prime
elif query == "not_mod3":
    S = U - mod3
else:
    raise SystemExit(f"unknown query: {query}")

for n in sorted(S):
    print(n)
PY

  redis_raw SMEMBERS "$setkey" | sed '/^$/d' | LC_ALL=C sort -n >"$b"
  LC_ALL=C sort -n -o "$a" "$a"

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

if [[ "$MU_CLEAN_TMP" == "1" ]]; then
  echo "Cleaning tmp keys: $PREFIX:tmp:*"
  if redis-cli -h "$MU_REDIS_HOST" -p "$MU_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*" >/dev/null 2>&1; then
    while IFS= read -r k; do
      [[ -z "$k" ]] && continue
      redis DEL "$k" >/dev/null
    done < <(redis-cli -h "$MU_REDIS_HOST" -p "$MU_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*")
  fi
  echo
fi

redis PING >/dev/null

K_EVEN="$PREFIX:idx:even"
K_MOD3="$PREFIX:idx:mod3"
K_PRIME="$PREFIX:idx:prime"
K_GT50="$PREFIX:idx:gt50"
K_ALL="$PREFIX:all"

TMP_EVEN_MOD3="$(tmp_key even_and_mod3)"
set_store_with_ttl SINTERSTORE "$TMP_EVEN_MOD3" "$TTL_SEC" "$K_EVEN" "$K_MOD3"
compare_py_vs_set "even AND mod3" "even_and_mod3" "$TMP_EVEN_MOD3"

TMP_EVEN_MOD3_NOT_PRIME="$(tmp_key even_and_mod3_not_prime)"
set_store_with_ttl SDIFFSTORE "$TMP_EVEN_MOD3_NOT_PRIME" "$TTL_SEC" "$TMP_EVEN_MOD3" "$K_PRIME"
compare_py_vs_set "(even AND mod3) NOT prime" "even_and_mod3_not_prime" "$TMP_EVEN_MOD3_NOT_PRIME"

TMP_GT50_OR_PRIME="$(tmp_key gt50_or_prime)"
set_store_with_ttl SUNIONSTORE "$TMP_GT50_OR_PRIME" "$TTL_SEC" "$K_GT50" "$K_PRIME"
compare_py_vs_set "gt50 OR prime" "gt50_or_prime" "$TMP_GT50_OR_PRIME"

TMP_NOT_MOD3="$(tmp_key not_mod3)"
set_store_with_ttl SDIFFSTORE "$TMP_NOT_MOD3" "$TTL_SEC" "$K_ALL" "$K_MOD3"
compare_py_vs_set "NOT mod3 (set-diff)" "not_mod3" "$TMP_NOT_MOD3"

echo "DONE"


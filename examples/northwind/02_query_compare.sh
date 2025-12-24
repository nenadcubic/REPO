#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
DB="${NW_DB_PATH:-$DIR/northwind.sqlite}"
PREFIX="${NW_PREFIX:-nw}"
PREFIX="${PREFIX%:}"
NW_REDIS_HOST="${NW_REDIS_HOST:-localhost}"
NW_REDIS_PORT="${NW_REDIS_PORT:-6379}"
TTL_SEC="${NW_TTL_SEC:-600}"
PRODUCT_ID="${NW_PRODUCT_ID:-11}"
NW_CLEAN_TMP="${NW_CLEAN_TMP:-0}"
YEAR="${NW_YEAR:-1997}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: missing required command: $1" >&2; exit 2; }; }
need redis-cli
need python3

HAVE_SQLITE3_CLI=0
if command -v sqlite3 >/dev/null 2>&1; then
  HAVE_SQLITE3_CLI=1
else
  echo "WARN: sqlite3 CLI not found; using python sqlite3 for SQL execution."
fi

if [[ ! -f "$DB" ]]; then
  echo "ERROR: DB not found: $DB"
  echo "Run: $DIR/00_get_db.sh"
  exit 2
fi

redis() { redis-cli -h "$NW_REDIS_HOST" -p "$NW_REDIS_PORT" "$@"; }
redis_raw() { redis-cli --raw -h "$NW_REDIS_HOST" -p "$NW_REDIS_PORT" "$@"; }

tmp_key() {
  local tag="$1"
  printf '%s:tmp:%s:%s:%s' "$PREFIX" "$tag" "$(date +%s)" "$RANDOM$RANDOM"
}

union_store_chunked() {
  local dest="$1" ttl="$2" batch_size="$3"
  shift 3
  local -a keys=( "$@" )

  if ((${#keys[@]} == 0)); then
    redis DEL "$dest" >/dev/null
    redis SADD "$dest" "__empty__" >/dev/null
    redis SREM "$dest" "__empty__" >/dev/null
    redis EXPIRE "$dest" "$ttl" >/dev/null
    return 0
  fi

  local i=0
  local -a batch=()
  batch=( "${keys[@]:i:batch_size}" )
  set_store_with_ttl SUNIONSTORE "$dest" "$ttl" "${batch[@]}"
  i=$((i + batch_size))

  # Rolling union to avoid huge argv/protocol payloads.
  # Redis allows the destination key to be among the source keys for *STORE operations.
  while (( i < ${#keys[@]} )); do
    batch=( "${keys[@]:i:batch_size}" )
    set_store_with_ttl SUNIONSTORE "$dest" "$ttl" "$dest" "${batch[@]}"
    i=$((i + batch_size))
  done
}

set_store_with_ttl() {
  local op="$1" dest="$2" ttl="$3"
  shift 3
  # Atomic store+expire (single Redis transaction).
  local out
  out="$({
    printf 'MULTI\n'
    printf '%s %s' "$op" "$dest"
    for k in "$@"; do printf ' %s' "$k"; done
    printf '\n'
    printf 'EXPIRE %s %s\n' "$dest" "$ttl"
    printf 'EXEC\n'
  } | redis-cli -h "$NW_REDIS_HOST" -p "$NW_REDIS_PORT" 2>&1)" || {
    echo "ERROR: redis-cli failed for: $op $dest (ttl=$ttl)" >&2
    echo "$out" >&2
    exit 1
  }

  # redis-cli may emit errors as `(error) ...` (sometimes prefixed by `1) `), `-ERR ...`, `-WRONGTYPE ...`,
  # `EXECABORT ...`, and cluster redirects (`MOVED`, `ASK`) among others.
  if printf '%s\n' "$out" | grep -qiE '\\(error\\)|-ERR|-WRONGTYPE|-NOAUTH|-READONLY|-MOVED|-ASK|EXECABORT'; then
    echo "ERROR: Redis error for: $op $dest (ttl=$ttl)" >&2
    echo "$out" >&2
    exit 1
  fi
}

sql_quote_ident() {
  local name="$1"
  name="${name//\"/\"\"}"
  printf '"%s"' "$name"
}

sql() {
  local query="$1"
  if [[ "$HAVE_SQLITE3_CLI" == "1" ]]; then
    sqlite3 -noheader -batch "$DB" "$query"
    return
  fi
  python3 - "$DB" "$query" <<'PY'
import sqlite3, sys
db, query = sys.argv[1], sys.argv[2]
conn = sqlite3.connect(db)
try:
    cur = conn.execute(query)
    for row in cur.fetchall():
        if not row:
            print("")
        else:
            v = row[0]
            print("" if v is None else v)
finally:
    conn.close()
PY
}

OD_TABLE="$(
  sql "
  SELECT name
  FROM sqlite_master
  WHERE type='table'
    AND lower(name) IN ('order details','orderdetails','order_details','order detail','orderdetail','order_detail')
  LIMIT 1;
  "
)"
if [[ -z "${OD_TABLE:-}" ]]; then
  echo "ERROR: could not find Order Details table in $DB" >&2
  exit 2
fi
OD_T="$(sql_quote_ident "$OD_TABLE")"

C_TABLE="$(
  sql "
  SELECT name
  FROM sqlite_master
  WHERE type='table'
    AND lower(name) IN ('customers','customer')
  LIMIT 1;
  "
)"
O_TABLE="$(
  sql "
  SELECT name
  FROM sqlite_master
  WHERE type='table'
    AND lower(name) IN ('orders','order')
  LIMIT 1;
  "
)"
if [[ -z "${C_TABLE:-}" || -z "${O_TABLE:-}" ]]; then
  echo "ERROR: could not find Customers/Orders tables in $DB" >&2
  echo "Found: Customers=${C_TABLE:-<none>} Orders=${O_TABLE:-<none>} OrderDetails=${OD_TABLE:-<none>}" >&2
  exit 2
fi
C_T="$(sql_quote_ident "$C_TABLE")"
O_T="$(sql_quote_ident "$O_TABLE")"

BITS_JSON="$DIR/schema_bits.json"
if [[ ! -f "$BITS_JSON" ]]; then
  echo "ERROR: schema_bits.json not found: $BITS_JSON" >&2
  exit 2
fi

read_bit() {
  local token="$1"
  python3 - "$BITS_JSON" "$token" <<'PY'
import json, sys
path, token = sys.argv[1], sys.argv[2]
obj = json.load(open(path, "r", encoding="utf-8"))
cur = obj
for part in token.split("."):
    if not isinstance(cur, dict) or part not in cur:
        raise SystemExit(2)
    cur = cur[part]
print(int(cur))
PY
}

BIT_DE="$(read_bit "customers.country.Germany")"
BIT_FR="$(read_bit "customers.country.France")"
BIT_UK="$(read_bit "customers.country.UK")"

K_CUSTOMERS_ALL="$PREFIX:customers:all"
K_ORDERS_ALL="$PREFIX:orders:all"
K_DE="$PREFIX:idx:customers:bit:$BIT_DE"
K_FR="$PREFIX:idx:customers:bit:$BIT_FR"
K_UK="$PREFIX:idx:customers:bit:$BIT_UK"

if ! redis PING >/dev/null; then
  echo "ERROR: Redis not reachable at $NW_REDIS_HOST:$NW_REDIS_PORT" >&2
  exit 2
fi

if [[ "$NW_CLEAN_TMP" == "1" ]]; then
  echo "Cleaning tmp keys: $PREFIX:tmp:*"
  if redis-cli -h "$NW_REDIS_HOST" -p "$NW_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*" >/dev/null 2>&1; then
    deleted=0
    while IFS= read -r k; do
      [[ -z "$k" ]] && continue
      redis DEL "$k" >/dev/null
      deleted=$((deleted + 1))
    done < <(redis-cli -h "$NW_REDIS_HOST" -p "$NW_REDIS_PORT" --scan --pattern "$PREFIX:tmp:*")
    echo "Deleted tmp keys: $deleted"
  else
    echo "WARN: redis-cli --scan not supported; skipping NW_CLEAN_TMP cleanup." >&2
  fi
  echo
fi

echo "DB: $DB"
echo "Redis: $NW_REDIS_HOST:$NW_REDIS_PORT"
echo "Prefix: $PREFIX:"
echo "TTL for derived keys: ${TTL_SEC}s"
echo

compare_sql_vs_set() {
  local label="$1" sql="$2" setkey="$3"
  local a b
  a="$(mktemp)"
  b="$(mktemp)"
  cleanup() { rm -f "$a" "$b"; }
  trap cleanup RETURN

  sql "$sql" | sed '/^$/d' >"$a" || { echo "ERROR: SQL execution failed for: $label" >&2; return 1; }
  # Some redis-cli versions output a single blank line for empty sets; filter empties to avoid false counts.
  redis_raw SMEMBERS "$setkey" | sed '/^$/d' | LC_ALL=C sort >"$b" || {
    echo "ERROR: redis SMEMBERS failed for key: $setkey" >&2
    return 1
  }
  LC_ALL=C sort -o "$a" "$a" || { echo "ERROR: sort failed for: $label" >&2; return 1; }

  local sql_n redis_n
  sql_n="$(wc -l <"$a" | tr -d ' ')"
  redis_n="$(wc -l <"$b" | tr -d ' ')"

  echo "== $label =="
  echo "SQL count:   $sql_n"
  echo "Redis count: $redis_n   (key: $setkey)"
  echo "Sample (up to 10):"
  head -n 10 "$a" | sed 's/^/ - /'

  if ! diff -u "$a" "$b" >/dev/null; then
    echo "ERROR: mismatch for: $label" >&2
    diff -u "$a" "$b" | head -n 80 >&2 || true
    return 1
  fi
  echo "OK: match"
  echo
}

echo "Bits:"
echo " - Germany: bit $BIT_DE (key: $K_DE)"
echo " - France:  bit $BIT_FR (key: $K_FR)"
echo " - UK:      bit $BIT_UK (key: $K_UK)"
echo

# 1) Customers in Germany
compare_sql_vs_set \
  "Customers in Germany" \
  "SELECT CustomerID FROM $C_T WHERE Country='Germany' ORDER BY CustomerID;" \
  "$K_DE"

# 2) Customers in Germany OR France
TMP_C_DE_FR="$(tmp_key customers_de_or_fr)"
set_store_with_ttl SUNIONSTORE "$TMP_C_DE_FR" "$TTL_SEC" "$K_DE" "$K_FR"
compare_sql_vs_set \
  "Customers in Germany OR France" \
  "SELECT CustomerID FROM $C_T WHERE Country IN ('Germany','France') ORDER BY CustomerID;" \
  "$TMP_C_DE_FR"

# 3) Customers NOT in Germany
TMP_C_NOT_DE="$(tmp_key customers_not_de)"
set_store_with_ttl SDIFFSTORE "$TMP_C_NOT_DE" "$TTL_SEC" "$K_CUSTOMERS_ALL" "$K_DE"
# SQL note: `Country!='Germany'` filters out NULLs (3-valued logic). Set-diff includes them, so include NULL explicitly.
compare_sql_vs_set \
  "Customers NOT in Germany" \
  "SELECT CustomerID FROM $C_T WHERE Country IS NULL OR Country!='Germany' ORDER BY CustomerID;" \
  "$TMP_C_NOT_DE"

# 4) Orders from German customers (union over nw:orders:customer:<CustomerID>)
TMP_O_DE="$(tmp_key orders_de)"
mapfile -t DE_CUSTOMERS < <(redis_raw SMEMBERS "$K_DE")
ORDER_KEYS=()
for cid in "${DE_CUSTOMERS[@]}"; do
  ORDER_KEYS+=("$PREFIX:orders:customer:$cid")
done
if ((${#ORDER_KEYS[@]} == 0)); then
  redis DEL "$TMP_O_DE" >/dev/null
  redis SADD "$TMP_O_DE" "__empty__" >/dev/null
  redis SREM "$TMP_O_DE" "__empty__" >/dev/null
  redis EXPIRE "$TMP_O_DE" "$TTL_SEC" >/dev/null
else
  union_store_chunked "$TMP_O_DE" "$TTL_SEC" 200 "${ORDER_KEYS[@]}"
fi
compare_sql_vs_set \
  "Orders from German customers" \
  "SELECT o.OrderID FROM $O_T o JOIN $C_T c ON c.CustomerID=o.CustomerID WHERE c.Country='Germany' ORDER BY o.OrderID;" \
  "$TMP_O_DE"

# 5) Orders from German customers AND containing ProductID=$PRODUCT_ID
# Uses the precomputed ingest index: <prefix>:orders:has_product:<ProductID> -> OrderIDs
K_O_HAS_P="$PREFIX:orders:has_product:$PRODUCT_ID"

TMP_O_DE_AND_P="$(tmp_key orders_de_and_product_${PRODUCT_ID})"
set_store_with_ttl SINTERSTORE "$TMP_O_DE_AND_P" "$TTL_SEC" "$TMP_O_DE" "$K_O_HAS_P"
compare_sql_vs_set \
  "Orders from German customers containing ProductID=$PRODUCT_ID" \
  "SELECT DISTINCT o.OrderID FROM $O_T o JOIN $C_T c ON c.CustomerID=o.CustomerID JOIN $OD_T od ON od.OrderID=o.OrderID WHERE c.Country='Germany' AND od.ProductID=$PRODUCT_ID ORDER BY o.OrderID;" \
  "$TMP_O_DE_AND_P"

# 6) Orders in year $YEAR
K_O_YEAR="$PREFIX:idx:orders:year:$YEAR"
compare_sql_vs_set \
  "Orders in year $YEAR" \
  "SELECT OrderID FROM $O_T WHERE OrderDate IS NOT NULL AND substr(OrderDate,1,4)='${YEAR}' ORDER BY OrderID;" \
  "$K_O_YEAR"

# 7) Orders in $YEAR Q1
K_O_Q1="$PREFIX:idx:orders:quarter:Q1"
TMP_O_YEAR_Q1="$(tmp_key orders_${YEAR}_q1)"
set_store_with_ttl SINTERSTORE "$TMP_O_YEAR_Q1" "$TTL_SEC" "$K_O_YEAR" "$K_O_Q1"
compare_sql_vs_set \
  "Orders in ${YEAR} Q1" \
  "SELECT OrderID FROM $O_T WHERE OrderDate IS NOT NULL AND OrderDate >= '${YEAR}-01-01' AND OrderDate < '${YEAR}-04-01' ORDER BY OrderID;" \
  "$TMP_O_YEAR_Q1"

# 8) Orders in $YEAR Q1 from German customers
TMP_O_DE_YEAR_Q1="$(tmp_key orders_de_${YEAR}_q1)"
set_store_with_ttl SINTERSTORE "$TMP_O_DE_YEAR_Q1" "$TTL_SEC" "$TMP_O_DE" "$K_O_YEAR" "$K_O_Q1"
compare_sql_vs_set \
  "Orders in ${YEAR} Q1 from German customers" \
  "SELECT o.OrderID FROM $O_T o JOIN $C_T c ON c.CustomerID=o.CustomerID WHERE c.Country='Germany' AND o.OrderDate IS NOT NULL AND o.OrderDate >= '${YEAR}-01-01' AND o.OrderDate < '${YEAR}-04-01' ORDER BY o.OrderID;" \
  "$TMP_O_DE_YEAR_Q1"

# 9) Orders NOT in year $YEAR (set-diff semantics include NULL OrderDate)
TMP_O_NOT_YEAR="$(tmp_key orders_not_${YEAR})"
set_store_with_ttl SDIFFSTORE "$TMP_O_NOT_YEAR" "$TTL_SEC" "$K_ORDERS_ALL" "$K_O_YEAR"
compare_sql_vs_set \
  "Orders NOT in year $YEAR" \
  "SELECT OrderID FROM $O_T WHERE OrderDate IS NULL OR substr(OrderDate,1,4)!='${YEAR}' ORDER BY OrderID;" \
  "$TMP_O_NOT_YEAR"

ER_CLI="$DIR/../../build/cli/er_cli"
if [[ -x "$ER_CLI" ]]; then
  echo "er_cli (optional): show one derived key"
  if ! ER_REDIS_HOST="$NW_REDIS_HOST" ER_REDIS_PORT="$NW_REDIS_PORT" "$ER_CLI" show "$TMP_O_DE_YEAR_Q1" | head -n 40; then
    echo "WARN: er_cli failed (missing runtime deps?); skipping." >&2
  fi
  echo
fi

echo "DONE"

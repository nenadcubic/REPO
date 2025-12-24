#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

json_get() { curl -fsS "$API_URL$1"; }
json_post() { curl -fsS -H 'Content-Type: application/json' -X POST "$API_URL$1" -d "$2"; }

assert_ok_json() {
  python3 -c 'import json,sys
raw=sys.stdin.read()
if not raw.strip():
  print("empty response", file=sys.stderr)
  raise SystemExit(2)
p=json.loads(raw)
if not p.get("ok"):
  print(json.dumps(p, indent=2), file=sys.stderr)
  raise SystemExit(2)
' "$@"
}

wait_for_health() {
  for _ in $(seq 1 50); do
    if json_get "/api/v1/health" >/tmp/er_gui_health.json 2>/dev/null; then
      if python3 -c 'import json,sys; p=json.load(open("/tmp/er_gui_health.json","r",encoding="utf-8")); sys.exit(0 if p.get("ok") else 1)' >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 0.2
  done
  echo "Last health response:" >&2
  cat /tmp/er_gui_health.json >&2 || true
  return 1
}

echo "[1/6] health"
wait_for_health || fail "backend not healthy at $API_URL"
json_get "/api/v1/health" | assert_ok_json

echo "[2/6] namespaces"
NS="$(json_get "/api/v1/namespaces" | python3 -c 'import json,sys;p=json.load(sys.stdin);d=p.get("data",{});print(d.get("default") or "er")')"
test -n "$NS" || NS="er"

echo "[2b/6] namespaces discover"
json_get "/api/v1/namespaces/discover?max_keys=2000&sample_per_prefix=50&scan_count=500" | assert_ok_json

echo "[3/6] config"
json_get "/api/v1/config" | assert_ok_json

echo "[4/6] bitmaps"
json_get "/api/v1/bitmaps?ns=$NS" | python3 -c '
import json,sys
p=json.load(sys.stdin)
if not p.get("ok"):
  raise SystemExit(2)
schema=p.get("data",{}).get("schema")
if schema != "er.gui.bitmaps.v1":
  print("unexpected schema:", schema, file=sys.stderr)
  raise SystemExit(3)
'

echo "[5/6] put"
json_post "/api/v1/elements/put" '{"ns":"'"$NS"'","name":"selftest_elem","bits":[0,1,7,8,4095]}' | assert_ok_json

echo "[6/6] get + assert bits"
json_get "/api/v1/elements/get?ns=$NS&name=selftest_elem&limit=4096" | python3 -c '
import json,sys
p=json.load(sys.stdin)
if not p.get("ok"):
  print(json.dumps(p, indent=2), file=sys.stderr)
  raise SystemExit(2)
bits=set(p["data"]["bits"])
want={0,1,7,8,4095}
missing=sorted(want-bits)
if missing:
  print("missing bits:", missing, file=sys.stderr)
  raise SystemExit(3)
print("PASS")
'

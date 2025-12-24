#!/usr/bin/env bash
set -euo pipefail

PREFIX="${DOCS_PREFIX:-docs}"
PREFIX="${PREFIX%:}"
DOCS_REDIS_HOST="${DOCS_REDIS_HOST:-localhost}"
DOCS_REDIS_PORT="${DOCS_REDIS_PORT:-6379}"

if [[ -z "$PREFIX" || ! "$PREFIX" =~ ^[A-Za-z0-9][A-Za-z0-9:_-]*$ ]]; then
  echo "ERROR: unsafe DOCS_PREFIX: ${PREFIX@Q}" >&2
  exit 2
fi

echo "Reset: deleting keys match: $PREFIX:* (Redis: $DOCS_REDIS_HOST:$DOCS_REDIS_PORT)"
deleted=0

if redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" --scan --pattern "$PREFIX:*" >/dev/null 2>&1; then
  while IFS= read -r k; do
    [[ -z "$k" ]] && continue
    redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" DEL "$k" >/dev/null
    deleted=$((deleted + 1))
  done < <(redis-cli -h "$DOCS_REDIS_HOST" -p "$DOCS_REDIS_PORT" --scan --pattern "$PREFIX:*")
else
  echo "ERROR: redis-cli --scan not supported" >&2
  exit 2
fi

echo "Deleted: $deleted"


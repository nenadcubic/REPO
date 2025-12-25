from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Iterable, cast

import redis

from .errors import ApiError
from .examples import list_examples
from .northwind_compare import TOKEN_TO_SQL_TABLE_CANDIDATES, _find_table, _iter_rows, _pk_columns, _row_pk, resolve_sqlite_path
from .northwind_data_bits import (
    SUPPORTED_TABLE_TOKENS,
    BitCondition,
    bit_conditions_for,
    data_key,
    data_registry_key,
    encode_row_bits,
    sql_expr_for,
)


def _example_dir_and_sqlite_path() -> tuple[Path, Path, str]:
    ex = next((x for x in list_examples() if x.id == "northwind_compare"), None)
    if not ex or not ex.dir or not ex.reference or ex.reference.kind != "sqlite" or not ex.reference.path:
        raise ApiError("NOT_FOUND", "northwind_compare example is not available", status_code=404)
    example_dir = ex.dir
    sqlite_path = resolve_sqlite_path(example_dir=example_dir, ref_path=ex.reference.path)
    return example_dir, sqlite_path, ex.reference.path


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in row.keys():
        out[str(k)] = row[k]
    return out


def _sql_table_for_token(conn: sqlite3.Connection, token: str) -> str:
    t = (token or "").strip()
    if t not in SUPPORTED_TABLE_TOKENS:
        raise ApiError("INVALID_INPUT", "unsupported table", status_code=422, details={"table": t})
    cand = TOKEN_TO_SQL_TABLE_CANDIDATES.get(t, [t])
    sql_table = _find_table(conn, cand)
    if not sql_table:
        raise ApiError("NOT_FOUND", "sqlite table not found", status_code=404, details={"token": t, "candidates": cand})
    return sql_table


def reset_data_ingest(*, r: redis.Redis, prefix: str) -> dict[str, Any]:
    reg = data_registry_key(prefix)
    members = r.smembers(reg)
    keys = [m.decode("utf-8", errors="replace") if isinstance(m, (bytes, bytearray)) else str(m) for m in members]
    keys = [k for k in keys if k]

    deleted = 0
    pipe = r.pipeline(transaction=False)
    batch = 0
    for k in keys:
        pipe.delete(k)
        batch += 1
        if batch >= 500:
            res = pipe.execute()
            deleted += sum(1 for x in res if isinstance(x, (int, float)) and int(x) > 0)
            pipe = r.pipeline(transaction=False)
            batch = 0
    if batch:
        res = pipe.execute()
        deleted += sum(1 for x in res if isinstance(x, (int, float)) and int(x) > 0)
    r.delete(reg)
    return {"deleted": deleted, "registry_key": reg}


def ingest_data_rows(
    *,
    r: redis.Redis,
    prefix: str,
    tables: list[str] | None = None,
    reset: bool = False,
    max_rows_per_table: int = 0,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    if reset:
        reset_data_ingest(r=r, prefix=pfx)

    _, sqlite_path, ref_path = _example_dir_and_sqlite_path()
    conn = sqlite3.connect(str(sqlite_path))

    requested = [(t or "").strip() for t in (tables or SUPPORTED_TABLE_TOKENS)]
    requested = [t for t in requested if t in SUPPORTED_TABLE_TOKENS]
    if not requested:
        raise ApiError("INVALID_INPUT", "no supported tables requested", status_code=422, details={"tables": tables})

    reg = data_registry_key(pfx)
    created_by_table: dict[str, int] = {t: 0 for t in requested}
    processed_by_table: dict[str, int] = {t: 0 for t in requested}

    pipe = r.pipeline(transaction=False)
    pending = 0

    for token in requested:
        sql_table = _sql_table_for_token(conn, token)
        pk_cols = _pk_columns(conn, sql_table)
        count = 0
        for row in _iter_rows(conn, sql_table):
            processed_by_table[token] += 1
            if max_rows_per_table and processed_by_table[token] > max_rows_per_table:
                break

            row_id = _row_pk(row, pk_cols)
            bits_int = encode_row_bits(table=token, row=_row_to_dict(row))
            k = data_key(pfx, token, row_id)
            pipe.set(k, str(bits_int))
            pipe.sadd(reg, k)
            pending += 2
            count += 1
            if pending >= 2000:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                pending = 0
        created_by_table[token] = count

    if pending:
        pipe.execute()

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "sqlite": {"ref_path": ref_path, "path": str(sqlite_path)},
        "registry_key": reg,
        "created_by_table": created_by_table,
        "processed_by_table": processed_by_table,
        "elapsed_ms": elapsed_ms,
    }


def data_info(*, r: redis.Redis, prefix: str, max_keys: int = 50_000) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    reg = data_registry_key(pfx)
    counts: dict[str, int] = {}
    total = 0

    cursor = 0
    scanned = 0
    while True:
        cursor, batch = r.sscan(reg, cursor=cursor, count=2000)
        for raw_key in batch:
            scanned += 1
            if scanned > max_keys:
                cursor = 0
                break
            k = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
            # {pfx}:data:<Table>:<RowId>
            want = f"{pfx}:data:"
            if not k.startswith(want):
                continue
            rest = k[len(want) :]
            if ":" not in rest:
                continue
            table, _ = rest.split(":", 1)
            if not table:
                continue
            counts[table] = counts.get(table, 0) + 1
            total += 1
        if cursor == 0:
            break

    return {"registry_key": reg, "total": total, "counts_by_table": dict(sorted(counts.items(), key=lambda kv: kv[0]))}


def _sql_where_and_params(*, table: str, conditions: list[dict[str, Any]]) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for raw in conditions:
        if not isinstance(raw, dict):
            continue
        col = str(raw.get("column") or "").strip()
        op = str(raw.get("op") or "=").strip()
        val = raw.get("value")
        if not col:
            raise ApiError("INVALID_INPUT", "condition column is required", status_code=422)
        if op not in ("=", "<", "<=", ">", ">="):
            raise ApiError("INVALID_INPUT", "unsupported operator", status_code=422, details={"op": op})
        expr = sql_expr_for(table=table, column=col)
        clauses.append(f"{expr} {op} ?")
        if table == "Orders" and col == "OrderYear":
            try:
                params.append(int(str(val).strip(), 10))
            except Exception:
                raise ApiError("INVALID_INPUT", "OrderYear must be an integer", status_code=422, details={"value": val})
        else:
            params.append(val if not isinstance(val, (bytes, bytearray)) else bytes(val))
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def _scan_data_keys(
    *,
    r: redis.Redis,
    prefix: str,
    table: str,
    max_keys: int,
) -> list[str]:
    pfx = (prefix or "").strip(":")
    t = (table or "").strip()
    match = f"{pfx}:data:{t}:*"
    out: list[str] = []
    for raw in r.scan_iter(match=match, count=2000):
        k = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        if not k:
            continue
        out.append(k)
        if len(out) >= max_keys:
            break
    out.sort()
    return out


def _row_id_from_data_key(*, key: str, prefix: str, table: str) -> str | None:
    pfx = (prefix or "").strip(":")
    t = (table or "").strip()
    want = f"{pfx}:data:{t}:"
    if not key.startswith(want):
        return None
    rid = key[len(want) :]
    return rid if rid else None


def compare_sql_vs_bitsets(
    *,
    r: redis.Redis,
    prefix: str,
    table: str,
    predicate_type: str,
    conditions: list[dict[str, Any]],
    sample: int,
    max_scan_keys: int = 200_000,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    pfx = (prefix or "").strip(":")
    token = (table or "").strip()
    if token not in SUPPORTED_TABLE_TOKENS:
        raise ApiError("INVALID_INPUT", "unsupported table", status_code=422, details={"table": token})
    if predicate_type != "and":
        raise ApiError("INVALID_INPUT", "only predicate type 'and' is supported", status_code=422, details={"type": predicate_type})
    if not conditions:
        raise ApiError("INVALID_INPUT", "at least one condition is required", status_code=422)

    # SQL side
    _, sqlite_path, ref_path = _example_dir_and_sqlite_path()
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row
    sql_table = _sql_table_for_token(conn, token)
    pk_cols = _pk_columns(conn, sql_table)
    if not pk_cols:
        raise ApiError("INVALID_INPUT", "table has no primary key", status_code=422, details={"table": token})

    where, params = _sql_where_and_params(table=token, conditions=conditions)
    sel_cols = ", ".join([f'"{c}"' for c in pk_cols])
    sql_query = f'SELECT {sel_cols} FROM "{sql_table}" WHERE {where}'
    sql_rows = conn.execute(sql_query, params).fetchall()
    sql_ids = [_row_pk(row, pk_cols) for row in cast(list[sqlite3.Row], sql_rows)]

    # Bitset side
    bit_conds, bit_ref = bit_conditions_for(table=token, conditions=conditions)
    data_keys = _scan_data_keys(r=r, prefix=pfx, table=token, max_keys=max_scan_keys)

    bitset_ids: list[str] = []
    batch_size = 400
    for i in range(0, len(data_keys), batch_size):
        chunk = data_keys[i : i + batch_size]
        vals = r.mget(chunk)
        for j, raw_val in enumerate(vals):
            if raw_val is None:
                continue
            s = raw_val.decode("utf-8", errors="replace") if isinstance(raw_val, (bytes, bytearray)) else str(raw_val)
            s = (s or "").strip()
            if not s:
                continue
            try:
                x = int(s, 10)
            except ValueError:
                continue
            ok = True
            for bc in bit_conds:
                if bc.kind == "all":
                    if (x & bc.mask) != bc.mask:
                        ok = False
                        break
                else:
                    if (x & bc.mask) == 0:
                        ok = False
                        break
            if not ok:
                continue
            rid = _row_id_from_data_key(key=chunk[j], prefix=pfx, table=token)
            if rid:
                bitset_ids.append(rid)

    sql_set = set(sql_ids)
    bit_set = set(bitset_ids)
    intersection = sorted(sql_set & bit_set)
    only_sql = sorted(sql_set - bit_set)
    only_bitset = sorted(bit_set - sql_set)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    def _sample(lst: list[str]) -> list[str]:
        return lst[: max(0, int(sample))]

    return {
        "sqlite": {"ref_path": ref_path, "path": str(sqlite_path)},
        "table": token,
        "sql": {"query": sql_query, "params": [str(p) for p in params]},
        "bitset_filter": {"conditions": [bc.label for bc in bit_conds], "bits": bit_ref},
        "results": {
            "sql": {"count": len(sql_ids), "ids": _sample(sorted(sql_set))},
            "bitset": {"count": len(bitset_ids), "ids": _sample(sorted(bit_set))},
            "intersection": {"count": len(intersection), "ids": _sample(intersection)},
            "only_sql": {"count": len(only_sql), "ids": _sample(only_sql)},
            "only_bitset": {"count": len(only_bitset), "ids": _sample(only_bitset)},
        },
        "scan": {"data_keys_scanned": len(data_keys), "max_scan_keys": max_scan_keys},
        "elapsed_ms": elapsed_ms,
    }


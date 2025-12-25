from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, cast

import redis

from .errors import ApiError
from .redis_bits import element_key_with_prefix
from .schema_meta import PROFILE_ID, bits_for_column, bits_for_relation, bits_for_table, encode_flags_bin

TABLE_TOKENS: list[str] = [
    "Customers",
    "Orders",
    "OrderDetails",
    "Products",
    "Employees",
    "Suppliers",
    "Categories",
    "Shippers",
    "Regions",
    "Territories",
    "EmployeeTerritories",
    "CustomerDemographics",
    "CustomerCustomerDemo",
]

TOKEN_TO_SQL_TABLE_CANDIDATES: dict[str, list[str]] = {
    "Customers": ["Customers"],
    "Orders": ["Orders"],
    "OrderDetails": ["Order Details", "OrderDetails"],
    "Products": ["Products"],
    "Employees": ["Employees"],
    "Suppliers": ["Suppliers"],
    "Categories": ["Categories"],
    "Shippers": ["Shippers"],
    "Regions": ["Region", "Regions"],
    "Territories": ["Territories"],
    "EmployeeTerritories": ["EmployeeTerritories"],
    "CustomerDemographics": ["CustomerDemographics"],
    "CustomerCustomerDemo": ["CustomerCustomerDemo", "CustomerCustomerDemo "],
}


@dataclass(frozen=True)
class OrLayoutTemplates:
    universe_key: str
    table_set_key: str
    object_key: str
    import_registry_key: str
    order_details_by_order_key: str


def _tpl(tpl: str, **vars: str) -> str:
    out = tpl
    for k, v in vars.items():
        out = out.replace("{" + k + "}", v)
    return out


def _require_str(d: dict[str, Any], key: str) -> str:
    v = d.get(key)
    if not isinstance(v, str) or not v.strip():
        raise ApiError("INVALID_LAYOUT", f"missing layout template: {key}", status_code=500)
    return v


def resolve_or_layout(*, namespaces_doc: dict[str, Any], layout_id: str) -> OrLayoutTemplates:
    layouts = namespaces_doc.get("layouts")
    if not isinstance(layouts, dict):
        raise ApiError("INVALID_LAYOUT", "namespaces.json layouts missing", status_code=500)
    layout = layouts.get(layout_id)
    if not isinstance(layout, dict):
        raise ApiError("INVALID_LAYOUT", "unknown layout", status_code=500, details={"layout": layout_id})
    kt = layout.get("key_templates")
    if not isinstance(kt, dict):
        raise ApiError("INVALID_LAYOUT", "layout key_templates missing", status_code=500, details={"layout": layout_id})
    return OrLayoutTemplates(
        universe_key=_require_str(kt, "universe_key"),
        table_set_key=_require_str(kt, "table_set_key"),
        object_key=_require_str(kt, "object_key"),
        import_registry_key=_require_str(kt, "import_registry_key"),
        order_details_by_order_key=_require_str(kt, "order_details_by_order_key"),
    )


def resolve_sqlite_path(*, example_dir: Path, ref_path: str) -> Path:
    assets_dir = (example_dir / "assets").resolve()
    if not assets_dir.is_dir():
        raise ApiError(
            "NOT_FOUND",
            "example assets directory missing",
            status_code=404,
            details={"assets_dir": str(assets_dir)},
        )

    rel = (ref_path or "").strip()
    if not rel:
        raise ApiError("INVALID_INPUT", "sqlite path is required", status_code=422)

    p = Path(rel)
    if p.is_absolute():
        resolved = p.resolve()
    else:
        resolved = (example_dir / p).resolve()

    if not resolved.is_relative_to(assets_dir):
        raise ApiError(
            "INVALID_INPUT",
            "sqlite path must be within this example's assets directory",
            status_code=422,
            details={"assets_dir": str(assets_dir)},
        )
    if not resolved.exists():
        raise ApiError(
            "NOT_FOUND",
            "sqlite database not found; place the file under this example's assets directory",
            status_code=404,
            details={"path": str(resolved)},
        )
    return resolved


def _find_table(conn: sqlite3.Connection, candidates: list[str]) -> str | None:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    by_lower = {str(r[0]).lower(): str(r[0]) for r in rows}
    for c in candidates:
        hit = by_lower.get(c.lower())
        if hit:
            return hit
    return None


def _pk_columns(conn: sqlite3.Connection, sql_table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({sql_table!r})").fetchall()
    cols = []
    for row in rows:
        # row: cid,name,type,notnull,dflt_value,pk
        name = row[1]
        pk = row[5]
        if pk and int(pk) > 0:
            cols.append(str(name))
    return cols


def _table_info(conn: sqlite3.Connection, sql_table: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return cast(list[sqlite3.Row], conn.execute(f"PRAGMA table_info({sql_table!r})").fetchall())


def _index_list(conn: sqlite3.Connection, sql_table: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return cast(list[sqlite3.Row], conn.execute(f"PRAGMA index_list({sql_table!r})").fetchall())


def _index_info(conn: sqlite3.Connection, index_name: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return cast(list[sqlite3.Row], conn.execute(f"PRAGMA index_info({index_name!r})").fetchall())


def _fk_list(conn: sqlite3.Connection, sql_table: str) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return cast(list[sqlite3.Row], conn.execute(f"PRAGMA foreign_key_list({sql_table!r})").fetchall())


def _row_pk(row: sqlite3.Row, pk_cols: list[str]) -> str:
    if not pk_cols:
        raise ApiError("INVALID_INPUT", "table has no primary key", status_code=422)
    parts: list[str] = []
    for c in pk_cols:
        v = row[c]
        if v is None:
            raise ApiError("INVALID_INPUT", "primary key is NULL", status_code=422, details={"column": c})
        parts.append(str(v))
    return ":".join(parts)


def _iter_rows(conn: sqlite3.Connection, sql_table: str) -> Iterable[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(f'SELECT * FROM "{sql_table}"')
    for row in cur:
        yield row


def _hset_mapping(pipe: redis.client.Pipeline, key: str, mapping: dict[str, Any]) -> None:
    # redis-py typing differs across versions; keep a small wrapper.
    pipe.hset(key, mapping=mapping)


def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", errors="replace")
    return str(v)


def _decimal(v: Any) -> Decimal:
    s = _to_str(v).strip()
    if not s:
        return Decimal("0")
    return Decimal(s)


def _round_2(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


RESET_LUA = r"""
local registry = KEYS[1]
local universe = KEYS[2]

local table_set_tpl = ARGV[1]
local obj_key_tpl = ARGV[2]
local order_details_by_order_tpl = ARGV[3]

local function fmt(tpl, vars)
  local out = tpl
  for k,v in pairs(vars) do
    out = string.gsub(out, "{"..k.."}", v)
  end
  return out
end

local members = redis.call("SMEMBERS", registry)
local deleted_objects = 0
local tables = {}
local orders = {}

for _, name in ipairs(members) do
  local table, id = string.match(name, "^([^:]+):(.+)$")
  if table and id then
    tables[table] = true
    local obj_key = fmt(obj_key_tpl, { table = table, id = id })
    redis.call("DEL", obj_key)
    redis.call("SREM", fmt(table_set_tpl, { table = table }), name)
    redis.call("SREM", universe, name)
    if table == "OrderDetails" then
      local order_id = string.match(id, "^([^:]+):")
      if order_id then
        orders[order_id] = true
        redis.call("SREM", fmt(order_details_by_order_tpl, { order_id = order_id }), name)
      end
    end
    deleted_objects = deleted_objects + 1
  end
end

for t,_ in pairs(tables) do
  redis.call("DEL", fmt(table_set_tpl, { table = t }))
end
for o,_ in pairs(orders) do
  redis.call("DEL", fmt(order_details_by_order_tpl, { order_id = o }))
end

redis.call("DEL", registry)
redis.call("DEL", universe)

return { deleted_objects, tostring(#members) }
"""


def reset_import(*, r: redis.Redis, prefix: str, tpl: OrLayoutTemplates) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    registry_key = _tpl(tpl.import_registry_key, pfx=pfx)
    universe_key = _tpl(tpl.universe_key, pfx=pfx)

    try:
        deleted, scanned = r.eval(
            RESET_LUA,
            2,
            registry_key,
            universe_key,
            _tpl(tpl.table_set_key, pfx=pfx, table="{table}"),
            _tpl(tpl.object_key, pfx=pfx, table="{table}", id="{id}"),
            _tpl(tpl.order_details_by_order_key, pfx=pfx, order_id="{order_id}"),
        )
    except Exception as e:
        raise ApiError("REDIS_ERROR", "reset failed", status_code=502, details={"error": str(e)})

    return {"scanned": int(scanned), "deleted_objects": int(deleted)}


def _schema_meta_registry_key(*, prefix: str) -> str:
    pfx = (prefix or "").strip(":")
    return f"{pfx}:import:northwind_compare:schema_meta"


def reset_schema_meta(*, r: redis.Redis, prefix: str) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    reg = _schema_meta_registry_key(prefix=pfx)
    names_raw = r.smembers(reg)
    names = [n.decode("utf-8", errors="replace") if isinstance(n, (bytes, bytearray)) else str(n) for n in names_raw]
    names = [n for n in names if n]

    pipe = r.pipeline(transaction=False)
    deleted = 0
    for name in names:
        pipe.delete(element_key_with_prefix(pfx, name))
        deleted += 1
    pipe.delete(reg)

    # Safety cleanup for older runs (or interrupted resets): scan known patterns.
    extra_deleted = 0
    for pat in (f"{pfx}:element:tbl:*", f"{pfx}:element:col:*", f"{pfx}:element:rel:*"):
        for raw_key in r.scan_iter(match=pat, count=1000):
            k = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
            pipe.delete(k)
            extra_deleted += 1
            if extra_deleted >= 50_000:
                break
        if extra_deleted >= 50_000:
            break

    pipe.execute()
    return {"registry_scanned": len(names), "deleted": deleted + extra_deleted}


def _safe_element_name(name: str) -> str | None:
    s = (name or "").strip()
    if not s or len(s) > 100:
        return None
    return s


def import_schema_meta(
    *,
    r: redis.Redis,
    prefix: str,
    conn: sqlite3.Connection,
    table_map: dict[str, str],
    logger: Any,
) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    sql_to_token = {sql.lower(): token for token, sql in table_map.items()}
    reg = _schema_meta_registry_key(prefix=pfx)

    pipe = r.pipeline(transaction=False)
    queued = 0
    max_queued = 8000

    created = 0
    skipped = 0

    def write_meta(name: str, bits: set[int]) -> None:
        nonlocal created, queued, pipe
        nm = _safe_element_name(name)
        if not nm:
            return
        key = element_key_with_prefix(pfx, nm)
        flags_bin = encode_flags_bin(bits)
        _hset_mapping(pipe, key, {"name": nm, "meta_profile": PROFILE_ID, "flags_bin": flags_bin})
        pipe.sadd(reg, nm)
        created += 1
        queued += 2
        if queued >= max_queued:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            queued = 0

    for token, sql_table in table_map.items():
        write_meta(f"tbl:{token}", bits_for_table())

        ti = _table_info(conn, sql_table)
        col_notnull: dict[str, bool] = {}
        col_default: dict[str, bool] = {}
        col_pk: set[str] = set()
        declared_type: dict[str, str] = {}
        for row in ti:
            col = str(row["name"])
            declared_type[col] = str(row["type"] or "")
            col_notnull[col] = bool(int(row["notnull"] or 0))
            col_default[col] = row["dflt_value"] is not None
            if int(row["pk"] or 0) > 0:
                col_pk.add(col)

        idx_rows = _index_list(conn, sql_table)
        indexed_cols: set[str] = set()
        unique_sets: list[set[str]] = []
        for idx in idx_rows:
            idx_name = str(idx["name"] or "")
            if not idx_name:
                continue
            unique = bool(int(idx["unique"] or 0))
            origin = str(idx["origin"] or "") if "origin" in idx.keys() else ""
            cols: set[str] = set()
            for r0 in _index_info(conn, idx_name):
                if "name" in r0.keys() and r0["name"]:
                    cols.add(str(r0["name"]))
            if unique and cols:
                unique_sets.append(cols)
            if origin == "c" and cols:
                indexed_cols |= cols

        fk_rows = _fk_list(conn, sql_table)
        fks_by_id: dict[int, list[sqlite3.Row]] = {}
        for rw in fk_rows:
            fk_id = int(rw["id"])
            fks_by_id.setdefault(fk_id, []).append(rw)

        fk_cols_all: set[str] = set()
        for fk_id, rows in fks_by_id.items():
            ref_sql = str(rows[0]["table"] or "")
            to_token = sql_to_token.get(ref_sql.lower()) or ref_sql.replace(" ", "")
            fk_cols = [str(rw["from"] or "") for rw in rows]
            fk_cols = [c for c in fk_cols if c]
            fk_cols_set = set(fk_cols)
            fk_cols_all |= fk_cols_set
            child_mandatory = all(col_notnull.get(c, False) for c in fk_cols)

            pk_cols = set(_pk_columns(conn, sql_table))
            is_unique_child = (fk_cols_set and fk_cols_set == pk_cols) or any(fk_cols_set == u for u in unique_sets)

            on_update = str(rows[0]["on_update"] or "")
            on_delete = str(rows[0]["on_delete"] or "")
            rel_bits = bits_for_relation(
                is_unique_child=is_unique_child,
                child_mandatory=child_mandatory,
                on_delete=on_delete,
                on_update=on_update,
            )
            write_meta(f"rel:{token}:{to_token}:fk{fk_id}", rel_bits)

        for col, decl in declared_type.items():
            is_fk = col in fk_cols_all
            c_bits = bits_for_column(
                declared_type=decl,
                not_null=col_notnull.get(col, False) or (col in col_pk),
                has_default=col_default.get(col, False),
                is_pk=(col in col_pk),
                is_fk=is_fk,
                has_index=(col in indexed_cols),
            )
            nm = f"col:{token}:{col}"
            if _safe_element_name(nm) is None:
                skipped += 1
                continue
            write_meta(nm, c_bits)

    if queued:
        pipe.execute()

    logger.info("northwind_compare schema_meta ns_prefix=%s created=%d skipped=%d", pfx, created, skipped)
    return {"profile": PROFILE_ID, "created": created, "skipped": skipped, "registry_key": reg}


def import_northwind(
    *,
    r: redis.Redis,
    prefix: str,
    tpl: OrLayoutTemplates,
    sqlite_path: Path,
    reset: bool,
    logger: Any,
) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    t0 = time.perf_counter()
    reset_info = None
    reset_schema_info = None
    if reset:
        reset_info = reset_import(r=r, prefix=pfx, tpl=tpl)
        reset_schema_info = reset_schema_meta(r=r, prefix=pfx)

    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row

    table_map: dict[str, str] = {}
    for token in TABLE_TOKENS:
        sql_name = _find_table(conn, TOKEN_TO_SQL_TABLE_CANDIDATES.get(token, [token]))
        if sql_name:
            table_map[token] = sql_name

    if not table_map.get("Customers") or not table_map.get("Orders") or not table_map.get("OrderDetails"):
        raise ApiError(
            "INVALID_INPUT",
            "expected Northwind tables not found (need Customers, Orders, Order Details)",
            status_code=422,
            details={"found": sorted(table_map.keys())},
        )

    pipe = r.pipeline(transaction=False)
    queued = 0
    max_queued = 8000

    registry_key = _tpl(tpl.import_registry_key, pfx=pfx)
    universe_key = _tpl(tpl.universe_key, pfx=pfx)

    table_counts: dict[str, int] = {}
    imported_tables: list[str] = []

    for token, sql_table in table_map.items():
        pk_cols = _pk_columns(conn, sql_table)
        table_set_key = _tpl(tpl.table_set_key, pfx=pfx, table=token)
        rows = 0
        for row in _iter_rows(conn, sql_table):
            pk = _row_pk(row, pk_cols)
            object_name = f"{token}:{pk}"
            object_key = _tpl(tpl.object_key, pfx=pfx, table=token, id=pk)

            mapping: dict[str, str] = {"__table": token, "__id": pk, "__name": object_name}
            for k in row.keys():
                mapping[str(k)] = _to_str(row[k])

            _hset_mapping(pipe, object_key, mapping)
            pipe.sadd(table_set_key, object_name)
            pipe.sadd(registry_key, object_name)
            pipe.sadd(universe_key, object_name)

            inc = 4
            if token == "OrderDetails":
                order_id = pk.split(":", 1)[0]
                od_key = _tpl(tpl.order_details_by_order_key, pfx=pfx, order_id=order_id)
                pipe.sadd(od_key, object_name)
                inc += 1

            queued += inc
            rows += 1
            if queued >= max_queued:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                queued = 0

        if queued:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
            queued = 0

        table_counts[token] = rows
        imported_tables.append(token)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    logger.info("northwind_compare import ns_prefix=%s tables=%d elapsed_ms=%d", pfx, len(imported_tables), elapsed_ms)

    schema_meta = import_schema_meta(r=r, prefix=pfx, conn=conn, table_map=table_map, logger=logger)

    out: dict[str, Any] = {
        "table_counts": table_counts,
        "imported_tables": imported_tables,
        "elapsed_ms": elapsed_ms,
        "rounding": "2dp_half_up",
        "schema_meta": schema_meta,
    }
    if reset_info is not None:
        out["reset"] = reset_info
    if reset_schema_info is not None:
        out["reset_schema_meta"] = reset_schema_info
    return out


def report_row_counts(*, r: redis.Redis, prefix: str, tpl: OrLayoutTemplates, sqlite_path: Path) -> list[dict[str, Any]]:
    pfx = (prefix or "").strip(":")
    conn = sqlite3.connect(str(sqlite_path))
    out: list[dict[str, Any]] = []

    for token in TABLE_TOKENS:
        sql_table = _find_table(conn, TOKEN_TO_SQL_TABLE_CANDIDATES.get(token, [token]))
        if not sql_table:
            continue
        sqlite_count = int(conn.execute(f'SELECT COUNT(*) FROM "{sql_table}"').fetchone()[0])
        redis_count = int(r.scard(_tpl(tpl.table_set_key, pfx=pfx, table=token)))
        out.append(
            {
                "table": token,
                "sqlite_count": sqlite_count,
                "redis_count": redis_count,
                "match": sqlite_count == redis_count,
            }
        )
    return out


def report_order_totals_sample(
    *,
    r: redis.Redis,
    prefix: str,
    tpl: OrLayoutTemplates,
    sqlite_path: Path,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if limit <= 0 or limit > 100:
        raise ApiError("INVALID_INPUT", "limit out of range", status_code=422)

    pfx = (prefix or "").strip(":")
    conn = sqlite3.connect(str(sqlite_path))
    conn.row_factory = sqlite3.Row

    orders_table = _find_table(conn, TOKEN_TO_SQL_TABLE_CANDIDATES["Orders"])
    od_table = _find_table(conn, TOKEN_TO_SQL_TABLE_CANDIDATES["OrderDetails"])
    if not orders_table or not od_table:
        raise ApiError("INVALID_INPUT", "missing Orders or Order Details", status_code=422)

    order_ids = [str(rw["OrderID"]) for rw in conn.execute(f'SELECT OrderID FROM "{orders_table}" ORDER BY OrderID LIMIT ?', (limit,))]
    if not order_ids:
        return []

    # SQLite totals (rounded)
    sqlite_totals: dict[str, Decimal] = {}
    for oid in order_ids:
        total = Decimal("0")
        rows = conn.execute(
            f'SELECT UnitPrice, Quantity, Discount FROM "{od_table}" WHERE OrderID = ?',
            (oid,),
        ).fetchall()
        for rw in rows:
            up = _decimal(rw[0])
            qty = _decimal(rw[1])
            disc = _decimal(rw[2])
            total += up * qty * (Decimal("1") - disc)
        total = _round_2(total)
        sqlite_totals[oid] = total

    # Redis totals (rounded with same rule)
    redis_totals: dict[str, Decimal] = {}
    for oid in order_ids:
        idx_key = _tpl(tpl.order_details_by_order_key, pfx=pfx, order_id=oid)
        members = r.smembers(idx_key)
        total = Decimal("0")
        for raw_name in members:
            name = _to_str(raw_name)
            # "OrderDetails:<OrderID>:<ProductID>"
            _, pk = name.split(":", 1)
            obj_key = _tpl(tpl.object_key, pfx=pfx, table="OrderDetails", id=pk)
            unit_price, quantity, discount = r.hmget(obj_key, "UnitPrice", "Quantity", "Discount")
            up = _decimal(unit_price)
            qty = _decimal(quantity)
            disc = _decimal(discount)
            total += up * qty * (Decimal("1") - disc)
        redis_totals[oid] = _round_2(total)

    out: list[dict[str, Any]] = []
    for oid in order_ids:
        st = sqlite_totals.get(oid, Decimal("0"))
        rt = redis_totals.get(oid, Decimal("0"))
        diff = _round_2(rt - st)
        out.append(
            {
                "order_id": oid,
                "sqlite_total": str(st),
                "redis_total": str(rt),
                "diff": str(diff),
            }
        )
    return out

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import redis
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .cli_adapter import er_cli_put, er_cli_query_with_count, er_cli_store_key
from .errors import ApiError, err, ok
from .models import (
    AssocCheckRequest,
    AssocHintRequest,
    ExamplesRunRequest,
    NorthwindCompareRequest,
    NorthwindDataIngestRequest,
    PutRequest,
    QueryRequest,
    StoreRequest,
)
from .redis_bits import decode_flags_bin, element_key_with_prefix
from .settings import load_settings
from .bitmaps import load_bitmaps_from_preset, save_bitmaps_to_preset
from .namespaces import NamespaceEntry, load_namespaces_from_preset, namespaces_to_map
from .namespace_discovery import DiscoveryLimits, discover_namespaces, write_namespaces_generated
from .examples import get_example_readme, list_examples, run_example, run_reports
from .schema_meta import decode_column_meta, decode_relation_meta
from .assoc_wordnet import (
    check_guess,
    generate_board,
    get_board,
    get_or_build_explain,
    hint_for,
    seed_demo,
 )
from .northwind_data import compare_sql_vs_bitsets, data_info as northwind_data_info, ingest_data_rows


BACKEND_VERSION = "0.1.0"

settings, preset_path = load_settings()

logger = logging.getLogger("er_gui_backend")
logger.setLevel(getattr(logging, settings.log_level.upper(), logging.INFO))
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

_log_path = Path(settings.log_path)
_log_path.parent.mkdir(parents=True, exist_ok=True)
_fh = logging.FileHandler(_log_path)
_fh.setFormatter(_formatter)
logger.addHandler(_fh)

_sh = logging.StreamHandler()
_sh.setFormatter(_formatter)
logger.addHandler(_sh)


def redis_client() -> redis.Redis:
    return redis.Redis(host=settings.redis_host, port=settings.redis_port, decode_responses=False)


def _redis_used_memory(r: redis.Redis) -> int | None:
    info = r.info(section="memory")
    used = info.get("used_memory") or info.get(b"used_memory")
    if used is None:
        return None
    if isinstance(used, (int, float)):
        return int(used)
    if isinstance(used, bytes):
        try:
            return int(used.decode("utf-8", errors="ignore") or "0")
        except ValueError:
            return None
    return None


def _ensure_store_key_safe(store_key: str, *, prefix: str) -> None:
    pfx = (prefix or "er").strip(":")
    if not store_key.startswith(f"{pfx}:tmp:"):
        raise ApiError("INVALID_STORE_KEY", f"store_key must start with {pfx}:tmp:", status_code=400)


def _resolve_ns_entry(ns: str | None) -> tuple[str, NamespaceEntry, dict[str, Any]]:
    doc = load_namespaces_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, logger=logger)
    default_id, mp = namespaces_to_map(doc)
    ns_id = (ns or "").strip() or default_id
    ent = mp.get(ns_id)
    if not ent:
        raise ApiError("INVALID_INPUT", "unknown namespace", status_code=422, details={"ns": ns_id})
    return ns_id, ent, doc


def _resolve_ns(ns: str | None) -> tuple[str, str]:
    ns_id, ent, _ = _resolve_ns_entry(ns)
    return ns_id, ent.prefix


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _count_set_bits(flags_bin: bytes) -> int:
    if len(flags_bin) != 512:
        raise ApiError("INVALID_FLAGS", "flags_bin must be 512 bytes", status_code=502, details={"len": len(flags_bin)})
    return int(int.from_bytes(flags_bin, byteorder="big", signed=False).bit_count())


def _decode_element_key(*, key: str, prefix: str) -> str:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)
    want = f"{pfx}:element:"
    if not key.startswith(want):
        raise ApiError("INVALID_INPUT", "not an element key", status_code=422, details={"key": key, "prefix": pfx})
    name = key[len(want) :]
    if not name or len(name) > 100:
        raise ApiError("INVALID_NAME", "name must be 1..100 chars", status_code=422, details={"name": name})
    return name


def _decode_or_object_key(*, key: str, prefix: str) -> tuple[str, str]:
    pfx = (prefix or "").strip(":")
    want = f"{pfx}:obj:"
    if not key.startswith(want):
        raise ApiError("INVALID_INPUT", "not an OR object key", status_code=422, details={"key": key, "prefix": pfx})
    rest = key[len(want) :]
    if ":" not in rest:
        raise ApiError("INVALID_INPUT", "invalid OR object key", status_code=422, details={"key": key})
    table, obj_id = rest.split(":", 1)
    table = table.strip()
    obj_id = obj_id.strip()
    if not table or not obj_id:
        raise ApiError("INVALID_INPUT", "invalid OR object key", status_code=422, details={"key": key})
    return table, obj_id


def _or_object_key_from_name(*, prefix: str, object_name: str) -> str:
    # object_name is "{table}:{id}" where id can contain ":" (composite PK)
    pfx = (prefix or "").strip(":")
    if ":" not in object_name:
        raise ApiError("INVALID_INPUT", "invalid OR object name", status_code=422, details={"name": object_name})
    table, obj_id = object_name.split(":", 1)
    table = table.strip()
    obj_id = obj_id.strip()
    if not table or not obj_id:
        raise ApiError("INVALID_INPUT", "invalid OR object name", status_code=422, details={"name": object_name})
    return f"{pfx}:obj:{table}:{obj_id}"


app = FastAPI(title="element-redis GUI API", version=BACKEND_VERSION)


@app.exception_handler(ApiError)
async def _api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=err(exc.code, exc.message, details=exc.details))


@app.exception_handler(RequestValidationError)
async def _validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content=err("VALIDATION_ERROR", "request validation failed", details={"errors": exc.errors()}),
    )


@app.exception_handler(Exception)
async def _unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(status_code=500, content=err("INTERNAL", "internal error"))


@app.get("/api/v1/health")
async def health() -> dict[str, Any]:
    r = redis_client()
    t0 = time.perf_counter()
    try:
        pong = r.ping()
        ok_redis = bool(pong)
    except Exception as e:
        logger.warning("Redis ping failed: %s", e)
        ok_redis = False
    ping_ms = int((time.perf_counter() - t0) * 1000)

    used_memory = None
    if ok_redis:
        try:
            used_memory = _redis_used_memory(r)
        except Exception:
            used_memory = None

    return ok(
        {
            "backend_version": BACKEND_VERSION,
            "preset": settings.gui_preset,
            "preset_path": str(preset_path) if preset_path else None,
            "redis": {"ok": ok_redis, "ping_ms": ping_ms, "used_memory": used_memory},
        }
    )


@app.get("/api/v1/config")
async def config() -> dict[str, Any]:
    return ok(
        {
            "backend_version": BACKEND_VERSION,
            "er_prefix": settings.er_prefix,
            "ttl_max_sec": int(settings.ttl_max_sec),
            "default_limit": 200,
            "max_query_limit": 5000,
            "store_preview_limit": int(settings.store_preview_limit),
        }
    )

@app.get("/api/v1/namespaces")
async def namespaces() -> dict[str, Any]:
    data = load_namespaces_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, logger=logger)
    return ok(data)

@app.get("/api/v1/namespaces/discover")
async def namespaces_discover(
    max_keys: int = 50000,
    sample_per_prefix: int = 200,
    scan_count: int = 1000,
    write: int = 0,
) -> dict[str, Any]:
    r = redis_client()
    discovery = discover_namespaces(
        r=r, limits=DiscoveryLimits(max_keys=max_keys, sample_per_prefix=sample_per_prefix, scan_count=scan_count)
    )
    out: dict[str, Any] = dict(discovery)
    if int(write) == 1:
        out["export"] = write_namespaces_generated(
            presets_dir=settings.presets_dir, preset=settings.gui_preset, discovery=discovery, logger=logger
        )
    return ok(out)

@app.get("/api/v1/examples")
async def examples() -> dict[str, Any]:
    ex = []
    for e in sorted(list_examples(logger=logger), key=lambda it: it.id):
        item: dict[str, Any] = {
            "id": e.id,
            "title": e.title,
            "type": e.type,
            "description": e.description,
            "default_namespace": e.default_namespace,
            "tags": e.tags,
        }
        if e.type == "seed":
            item["element_count_estimate"] = len(e.elements or [])
        if e.type == "dataset_compare":
            item["compare_reports"] = [{"id": r.id, "title": r.title} for r in (e.compare_reports or [])]
            item["reference"] = {"kind": (e.reference.kind if e.reference else None), "path": (e.reference.path if e.reference else None)}
        ex.append(item)
    return ok({"examples": ex})

@app.get("/api/v1/examples/{id}/readme")
async def examples_readme(id: str) -> dict[str, Any]:
    return ok(get_example_readme(example_id=id))


@app.post("/api/v1/examples/{id}/run")
async def examples_run(id: str, body: ExamplesRunRequest) -> dict[str, Any]:
    ex = next((x for x in list_examples(logger=logger) if x.id == id), None)
    if not ex:
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": id})
    ns_to_use = (body.ns or "").strip() or ex.default_namespace
    ns_id, ent, namespaces_doc = _resolve_ns_entry(ns_to_use)
    if ex.type == "dataset_compare" and ent.layout != "or_layout_v2":
        raise ApiError("INVALID_INPUT", "example requires OR layout", status_code=422, details={"ns": ns_id, "layout": ent.layout})

    r = redis_client()
    data = run_example(
        example_id=id,
        ns=ns_id,
        prefix=ent.prefix,
        layout_id=ent.layout,
        namespaces_doc=namespaces_doc,
        reset=(bool(body.reset) if body.reset is not None else False),
        r=r,
        er_cli_path=settings.er_cli_path,
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        logger=logger,
    )
    return ok(data)


@app.get("/api/v1/examples/{id}/reports")
async def examples_reports(id: str, ns: str | None = None) -> dict[str, Any]:
    if not isinstance(id, str) or not id.strip():
        raise ApiError("INVALID_INPUT", "id is required", status_code=422)
    ex = next((x for x in list_examples(logger=logger) if x.id == id), None)
    if not ex:
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": id})
    ns_to_use = (ns or "").strip() or ex.default_namespace
    ns_id, ent, namespaces_doc = _resolve_ns_entry(ns_to_use)
    if ex.type == "dataset_compare" and ent.layout != "or_layout_v2":
        raise ApiError("INVALID_INPUT", "example requires OR layout", status_code=422, details={"ns": ns_id, "layout": ent.layout})
    r = redis_client()
    data = run_reports(example_id=id, ns=ns_id, prefix=ent.prefix, layout_id=ent.layout, namespaces_doc=namespaces_doc, r=r, logger=logger)
    return ok(data)


@app.get("/api/v1/explorer/namespaces")
async def explorer_namespaces() -> list[dict[str, Any]]:
    doc = load_namespaces_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, logger=logger)
    ns_list = doc.get("namespaces") if isinstance(doc.get("namespaces"), list) else []
    r = redis_client()

    out: list[dict[str, Any]] = []
    now = _utc_now_iso()
    for raw in ns_list:
        if not isinstance(raw, dict):
            continue
        ns_id = str(raw.get("id") or "").strip()
        prefix = str(raw.get("prefix") or "").strip().strip(":")
        layout_id = str(raw.get("layout") or "").strip()
        if not ns_id or not prefix:
            continue
        try:
            element_count = int(r.scard(f"{prefix}:all"))
        except Exception:
            element_count = 0
        out.append({"name": ns_id, "key_count": element_count, "updated_at": now, "layout": layout_id})
    return out


@app.get("/api/v1/explorer/namespaces/{namespace}/elements")
async def explorer_namespace_elements(
    namespace: str,
    search: str = "",
    page: int = 1,
    page_size: int = 50,
) -> dict[str, Any]:
    q = (search or "").strip().lower()
    if page <= 0:
        raise ApiError("INVALID_INPUT", "page must be >= 1", status_code=422)
    if page_size <= 0 or page_size > 200:
        raise ApiError("INVALID_INPUT", "page_size must be 1..200", status_code=422)

    _, ent, _ = _resolve_ns_entry(namespace)
    prefix = ent.prefix.strip(":")

    r = redis_client()
    universe_key = f"{prefix}:all"

    total: int
    if not q:
        try:
            total = int(r.scard(universe_key))
        except Exception:
            total = 0
    else:
        total = -1  # unknown without scanning full set

    start = (page - 1) * page_size
    end = start + page_size

    cursor = 0
    scanned = 0
    matched_seen = 0
    names: list[str] = []
    max_scan = 200_000
    truncated = False

    while True:
        cursor, batch = r.sscan(universe_key, cursor=cursor, count=1000)
        for raw_name in batch:
            scanned += 1
            if scanned > max_scan:
                truncated = True
                cursor = 0
                break
            name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, (bytes, bytearray)) else str(raw_name)
            if not name:
                continue
            if q and q not in name.lower():
                continue

            if matched_seen >= start and matched_seen < end:
                names.append(name)
            matched_seen += 1

            if matched_seen >= end:
                cursor = 0
                break
        if cursor == 0:
            break

    # Build keys + per-row metrics.
    keys: list[str] = []
    if ent.layout == "er_layout_v1":
        keys = [element_key_with_prefix(prefix, n) for n in names]
    elif ent.layout == "or_layout_v2":
        keys = [_or_object_key_from_name(prefix=prefix, object_name=n) for n in names]
    else:
        # Unknown layout: still return names, but skip key inspection.
        keys = [f"{prefix}:{n}" for n in names]

    pipe = r.pipeline(transaction=False)
    if ent.layout == "er_layout_v1":
        for k in keys:
            pipe.hget(k, "flags_bin")
            pipe.ttl(k)
    elif ent.layout == "or_layout_v2":
        for k in keys:
            pipe.hlen(k)
            pipe.ttl(k)
    raw = pipe.execute() if keys else []

    items: list[dict[str, Any]] = []
    for i, name in enumerate(names):
        ttl = raw[i * 2 + 1] if i * 2 + 1 < len(raw) else None
        ttl_out = None
        if isinstance(ttl, (int, float)) and int(ttl) >= 0:
            ttl_out = int(ttl)

        metric = raw[i * 2] if i * 2 < len(raw) else None
        if ent.layout == "er_layout_v1":
            flags_bin = metric
            if isinstance(flags_bin, str):
                flags_bin = flags_bin.encode("utf-8")
            if isinstance(flags_bin, (bytes, bytearray)) and len(flags_bin) == 512:
                try:
                    count = _count_set_bits(bytes(flags_bin))
                except Exception:
                    count = 0
            else:
                count = 0
        elif ent.layout == "or_layout_v2":
            count = int(metric) if isinstance(metric, (int, float)) else 0  # field count
        else:
            count = 0

        items.append({"key": keys[i], "short_name": name, "set_bits_count": count, "ttl": ttl_out})

    out = {"items": items, "page": page, "page_size": page_size, "total": int(total)}
    if truncated:
        out["truncated"] = True
    return out


@app.get("/api/v1/explorer/elements/{encodedKey}")
async def explorer_element(encodedKey: str) -> dict[str, Any]:
    raw_key = unquote(encodedKey or "").strip()
    if not raw_key:
        raise ApiError("INVALID_INPUT", "encodedKey is required", status_code=422)

    ns_doc = load_namespaces_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, logger=logger)
    _, mp = namespaces_to_map(ns_doc)
    ns_id = None
    prefix = None
    for k, ent in mp.items():
        pfx = str(ent.prefix or "").strip(":")
        if pfx and raw_key.startswith(f"{pfx}:"):
            ns_id = k
            prefix = pfx
            break
    if not ns_id or not prefix:
        raise ApiError("INVALID_INPUT", "unknown namespace for key", status_code=422, details={"key": raw_key})

    r = redis_client()
    ttl = r.ttl(raw_key)
    ttl_out = None
    if isinstance(ttl, (int, float)) and int(ttl) >= 0:
        ttl_out = int(ttl)

    if raw_key.startswith(f"{prefix}:element:"):
        name = _decode_element_key(key=raw_key, prefix=prefix)
        flags_bin = r.hget(raw_key, "flags_bin")
        if flags_bin is None:
            raise ApiError("NOT_FOUND", "element not found", status_code=404, details={"key": raw_key})
        if isinstance(flags_bin, str):
            flags_bin = flags_bin.encode("utf-8")
        if not isinstance(flags_bin, (bytes, bytearray)):
            raise ApiError("INVALID_FLAGS", "flags_bin must be bytes", status_code=502)
        bits = decode_flags_bin(bytes(flags_bin))
        return {
            "key": raw_key,
            "short_name": name,
            "namespace": ns_id,
            "kind": "bitset",
            "bits": 4096,
            "set_bits": bits,
            "ttl": ttl_out,
        }

    if raw_key.startswith(f"{prefix}:obj:"):
        table, obj_id = _decode_or_object_key(key=raw_key, prefix=prefix)
        # Limit fields to avoid huge responses.
        fields = r.hgetall(raw_key)
        out_fields: dict[str, str] = {}
        max_fields = 200
        i = 0
        for k, v in (fields or {}).items():
            if i >= max_fields:
                break
            kk = k.decode("utf-8", errors="replace") if isinstance(k, (bytes, bytearray)) else str(k)
            vv = v.decode("utf-8", errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)
            out_fields[kk] = vv
            i += 1
        return {
            "key": raw_key,
            "short_name": f"{table}:{obj_id}",
            "namespace": ns_id,
            "kind": "hash",
            "ttl": ttl_out,
            "hash": {"field_count": int(r.hlen(raw_key)), "fields": out_fields, "truncated": bool(len(out_fields) < int(r.hlen(raw_key)))},
        }

    raise ApiError("INVALID_INPUT", "unsupported key type", status_code=422, details={"key": raw_key})


@app.get("/api/v1/explorer/namespaces/{namespace}/bitmap")
async def explorer_namespace_bitmap(namespace: str, limit: int = 75, offset: int = 0) -> dict[str, Any]:
    if limit <= 0 or limit > 200:
        raise ApiError("INVALID_INPUT", "limit must be 1..200", status_code=422)
    if offset < 0:
        raise ApiError("INVALID_INPUT", "offset must be >= 0", status_code=422)

    ns_id, ent, _ = _resolve_ns_entry(namespace)
    if ent.layout != "er_layout_v1":
        return {"namespace": ns_id, "bits": 0, "elements": [], "note": "bitmap supported only for bitset namespaces"}
    prefix = ent.prefix.strip(":")
    r = redis_client()
    universe_key = f"{prefix}:all"

    cursor = 0
    scanned = 0
    max_scan = 300_000
    wanted_end = offset + limit
    names: list[str] = []
    while True:
        cursor, batch = r.sscan(universe_key, cursor=cursor, count=1000)
        for raw_name in batch:
            scanned += 1
            if scanned > max_scan:
                cursor = 0
                break
            if scanned <= offset:
                continue
            name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, (bytes, bytearray)) else str(raw_name)
            if not name:
                continue
            names.append(name)
            if len(names) >= limit:
                cursor = 0
                break
        if cursor == 0 or scanned >= wanted_end:
            if len(names) >= limit:
                break
            if cursor == 0:
                break

    keys = [element_key_with_prefix(prefix, n) for n in names]
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.hget(k, "flags_bin")
    raw = pipe.execute() if keys else []

    elements: list[dict[str, Any]] = []
    for i, name in enumerate(names):
        flags_bin = raw[i] if i < len(raw) else None
        if isinstance(flags_bin, str):
            flags_bin = flags_bin.encode("utf-8")
        bits: list[int] = []
        if isinstance(flags_bin, (bytes, bytearray)) and len(flags_bin) == 512:
            try:
                bits = decode_flags_bin(bytes(flags_bin))
            except Exception:
                bits = []
        elements.append({"key": element_key_with_prefix(prefix, name), "short_name": name, "set_bits": bits})

    return {"namespace": ns_id, "bits": 4096, "elements": elements}


@app.get("/api/v1/bitmaps")
async def bitmaps(ns: str | None = None) -> dict[str, Any]:
    ns_id, prefix = _resolve_ns(ns)
    data = load_bitmaps_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, ns=ns_id, logger=logger)
    data.setdefault("meta", {})
    if isinstance(data["meta"], dict):
        data["meta"].setdefault("ns", ns_id)
        data["meta"].setdefault("prefix", prefix)
    return ok(data)


@app.put("/api/v1/bitmaps")
async def bitmaps_put(document: dict[str, Any], ns: str | None = None) -> dict[str, Any]:
    ns_id, prefix = _resolve_ns(ns)
    save_bitmaps_to_preset(
        presets_dir=settings.presets_dir, preset=settings.gui_preset, ns=ns_id, logger=logger, document=document
    )
    data = load_bitmaps_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, ns=ns_id, logger=logger)
    logger.info("bitmaps saved preset=%s ns=%s", settings.gui_preset, ns_id)
    data.setdefault("meta", {})
    if isinstance(data["meta"], dict):
        data["meta"].setdefault("ns", ns_id)
        data["meta"].setdefault("prefix", prefix)
    return ok(data)


@app.post("/api/v1/elements/put")
async def elements_put(req: PutRequest, ns: str | None = None) -> dict[str, Any]:
    bits = sorted(set(req.bits))
    ns_id, prefix = _resolve_ns(ns or req.ns)
    er_cli_put(
        er_cli_path=settings.er_cli_path,
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        redis_prefix=prefix,
        name=req.name,
        bits=bits,
    )
    logger.info("put ns=%s name=%s bits=%d", ns_id, req.name, len(bits))
    return ok({"name": req.name, "written_bits": len(bits)})


@app.get("/api/v1/elements/get")
async def elements_get(name: str, limit: int = 200, ns: str | None = None) -> dict[str, Any]:
    if not name or len(name) > 100:
        raise ApiError("INVALID_NAME", "name must be 1..100 chars", status_code=422)
    if limit <= 0 or limit > 4096:
        raise ApiError("INVALID_LIMIT", "limit must be 1..4096", status_code=422)
    _, prefix = _resolve_ns(ns)

    r = redis_client()
    key = element_key_with_prefix(prefix, name)
    flags_bin = r.hget(key, "flags_bin")
    if flags_bin is None:
        raise ApiError("NOT_FOUND", "element not found", status_code=404, details={"name": name})
    if isinstance(flags_bin, str):
        flags_bin = flags_bin.encode("utf-8")
    if not isinstance(flags_bin, (bytes, bytearray)):
        raise ApiError("INVALID_FLAGS", "flags_bin must be bytes", status_code=502)

    bits = decode_flags_bin(bytes(flags_bin))
    bits_limited = bits[: int(limit)]
    return ok({"name": name, "bits": bits_limited, "count": len(bits), "returned": len(bits_limited), "limit": limit})


@app.post("/api/v1/query")
async def query(req: QueryRequest, ns: str | None = None) -> dict[str, Any]:
    ns_id, prefix = _resolve_ns(ns or req.ns)
    if req.type == "find":
        args = ["find", str(req.bit)]
    elif req.type == "find_all":
        args = ["find_all", *[str(b) for b in req.bits]]
    elif req.type == "find_any":
        args = ["find_any", *[str(b) for b in req.bits]]
    elif req.type == "find_not":
        args = ["find_not", str(req.include_bit), *[str(b) for b in req.exclude_bits]]
    else:  # find_universe_not
        args = ["find_universe_not", *[str(b) for b in req.exclude_bits]]

    count_from_cli, names = er_cli_query_with_count(
        er_cli_path=settings.er_cli_path,
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        redis_prefix=prefix,
        args=args,
    )
    limit = int(req.limit)
    limited = names[:limit]
    count = int(count_from_cli) if count_from_cli is not None else len(names)
    return ok({"ns": ns_id, "type": req.type, "count": count, "returned": len(limited), "limit": limit, "names": limited})


@app.post("/api/v1/store")
async def store(req: StoreRequest, ns: str | None = None) -> dict[str, Any]:
    ns_id, prefix = _resolve_ns(ns or req.ns)
    if req.ttl_sec > int(settings.ttl_max_sec):
        raise ApiError(
            "INVALID_TTL",
            "ttl_sec exceeds max",
            status_code=422,
            details={"ttl_sec": req.ttl_sec, "max_ttl_sec": int(settings.ttl_max_sec)},
        )
    if req.type == "find_all_store":
        args = ["find_all_store", str(req.ttl_sec), *[str(b) for b in req.bits]]
    elif req.type == "find_any_store":
        args = ["find_any_store", str(req.ttl_sec), *[str(b) for b in req.bits]]
    else:  # find_not_store
        args = ["find_not_store", str(req.ttl_sec), str(req.include_bit), *[str(b) for b in req.exclude_bits]]

    store_key = er_cli_store_key(
        er_cli_path=settings.er_cli_path,
        redis_host=settings.redis_host,
        redis_port=settings.redis_port,
        redis_prefix=prefix,
        args=args,
    )
    _ensure_store_key_safe(store_key, prefix=prefix)

    r = redis_client()
    ttl = r.ttl(store_key)
    ttl_remaining = int(ttl) if isinstance(ttl, (int, float)) else -1
    count = int(r.scard(store_key))

    preview: list[str] = []
    cursor = 0
    limit = max(0, int(settings.store_preview_limit))
    while len(preview) < limit:
        cursor, batch = r.sscan(store_key, cursor=cursor, count=min(500, limit - len(preview)))
        for raw in batch:
            if isinstance(raw, bytes):
                preview.append(raw.decode("utf-8", errors="replace"))
            else:
                preview.append(str(raw))
            if len(preview) >= limit:
                break
        if cursor == 0:
            break

    return ok(
        {
            "ns": ns_id,
            "store_key": store_key,
            "ttl_remaining": ttl_remaining,
            "count": count,
            "preview_limit": limit,
            "preview": preview,
        }
    )


@app.get("/api/v1/store/inspect")
async def store_inspect(store_key: str, limit: int = 200, ns: str | None = None) -> dict[str, Any]:
    ns_id, prefix = _resolve_ns(ns)
    _ensure_store_key_safe(store_key, prefix=prefix)
    if limit <= 0 or limit > 5000:
        raise ApiError("INVALID_LIMIT", "limit must be 1..5000", status_code=422)

    r = redis_client()
    ttl = r.ttl(store_key)
    ttl_remaining = int(ttl) if isinstance(ttl, (int, float)) else -1
    count = int(r.scard(store_key))

    names: list[str] = []
    cursor = 0
    while len(names) < limit:
        cursor, batch = r.sscan(store_key, cursor=cursor, count=min(500, limit - len(names)))
        for raw in batch:
            if isinstance(raw, bytes):
                names.append(raw.decode("utf-8", errors="replace"))
            else:
                names.append(str(raw))
            if len(names) >= limit:
                break
        if cursor == 0:
            break

    return ok(
        {
            "ns": ns_id,
            "store_key": store_key,
            "ttl_remaining": ttl_remaining,
            "count": count,
            "returned": len(names),
            "limit": limit,
            "names": names,
        }
    )


@app.delete("/api/v1/store")
async def store_delete(store_key: str, ns: str | None = None) -> dict[str, Any]:
    _, prefix = _resolve_ns(ns)
    _ensure_store_key_safe(store_key, prefix=prefix)
    r = redis_client()
    deleted = bool(r.delete(store_key))
    return ok({"deleted": deleted})


@app.get("/api/v1/logs")
async def logs(tail: int = 200) -> dict[str, Any]:
    if tail <= 0 or tail > 2000:
        raise ApiError("INVALID_TAIL", "tail must be 1..2000", status_code=422)

    path = Path(settings.log_path)
    if not path.exists():
        return ok({"lines": [], "returned": 0, "tail": tail})

    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        raise ApiError("LOG_READ_ERROR", "failed to read log file", status_code=500, details={"error": str(e)})

    out_lines = lines[-tail:]
    return ok({"lines": out_lines, "returned": len(out_lines), "tail": tail})


@app.get("/api/v1/assoc/board/random")
async def assoc_board_random(seed: str | None = None, mode: str | None = None) -> dict[str, Any]:
    r = redis_client()
    if (mode or "").strip().lower() == "demo":
        seed_demo(r=r)
        board = get_board(r=r, board_id="demo_v1")
        return ok(board)

    try:
        board = generate_board(r=r, seed=(seed or None))
        return ok(board)
    except ApiError as e:
        # Out-of-the-box UX: if WordNet isn't ingested (or only demo-sized data exists),
        # fall back to a demo board instead of surfacing an opaque 500 to the user.
        if e.code in ("WORDNET_NOT_INGESTED", "NO_BOARD"):
            try:
                wn_count = int(r.scard("wn:all"))
            except Exception:
                wn_count = 0
            if e.code == "WORDNET_NOT_INGESTED" or wn_count < 1000:
                seed_demo(r=r)
                board = get_board(r=r, board_id="demo_v1")
                board_out = dict(board)
                board_out["note"] = "Using demo board because full WordNet is not available; ingest WordNet to enable random boards."
                return ok(board_out)
        raise


@app.get("/api/v1/assoc/status")
async def assoc_status() -> dict[str, Any]:
    r = redis_client()
    try:
        wn_all = int(r.scard("wn:all"))
    except Exception:
        wn_all = 0
    try:
        wn_nouns = int(r.scard("wn:idx:pos:n"))
    except Exception:
        wn_nouns = 0
    try:
        demo_present = bool(r.exists("assoc:board:demo_v1"))
    except Exception:
        demo_present = False

    # Heuristic: demo seed is ~19 synsets; full WordNet is 100k+.
    kind = "none"
    if wn_all >= 1000:
        kind = "full_or_partial"
    elif wn_all > 0:
        kind = "demo_or_small"

    return ok(
        {
            "wordnet": {
                "kind": kind,
                "wn_all_count": wn_all,
                "wn_noun_count": wn_nouns,
                "demo_board_present": demo_present,
            },
            "ingest_commands": {
                "host_python": "pip install -r tools/wn_ingest/requirements.txt && python tools/wn_ingest/wordnet_to_bitset.py --reset",
                "docker_network": 'docker run --rm --network <compose_network> -v "$PWD":/work -w /work python:3.12-slim bash -lc "pip install -r tools/wn_ingest/requirements.txt && python tools/wn_ingest/wordnet_to_bitset.py --redis-host redis --redis-port 6379 --reset"',
                "note": "Replace <compose_network> with your Docker network (e.g. gui_default).",
            },
        }
    )


@app.get("/api/v1/assoc/board/{id}")
async def assoc_board(id: str) -> dict[str, Any]:
    board_id = (id or "").strip()
    if not board_id:
        raise ApiError("INVALID_INPUT", "id is required", status_code=422)
    r = redis_client()
    return ok(get_board(r=r, board_id=board_id))


@app.post("/api/v1/assoc/board/{id}/check")
async def assoc_check(id: str, body: AssocCheckRequest) -> dict[str, Any]:
    board_id = (id or "").strip()
    if not board_id:
        raise ApiError("INVALID_INPUT", "id is required", status_code=422)
    r = redis_client()
    board = get_board(r=r, board_id=board_id)
    res = check_guess(r=r, board=board, cell=body.cell, guess=body.guess)
    return ok({"id": board_id, "cell": body.cell, **res})


@app.post("/api/v1/assoc/board/{id}/hint")
async def assoc_hint(id: str, body: AssocHintRequest) -> dict[str, Any]:
    board_id = (id or "").strip()
    if not board_id:
        raise ApiError("INVALID_INPUT", "id is required", status_code=422)
    r = redis_client()
    board = get_board(r=r, board_id=board_id)
    res = hint_for(board=board, cell=body.cell, kind=body.kind)
    return ok({"id": board_id, **res})


@app.get("/api/v1/assoc/board/{id}/explain")
async def assoc_explain(id: str) -> dict[str, Any]:
    board_id = (id or "").strip()
    if not board_id:
        raise ApiError("INVALID_INPUT", "id is required", status_code=422)
    r = redis_client()
    board = get_board(r=r, board_id=board_id)
    exp = get_or_build_explain(r=r, board=board)
    return ok(exp)


def _scan_keys(*, r: redis.Redis, match: str, max_keys: int) -> list[str]:
    out: list[str] = []
    for raw in r.scan_iter(match=match, count=1000):
        k = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        if not k:
            continue
        out.append(k)
        if len(out) >= max_keys:
            break
    out.sort()
    return out


def _strip_element_key_prefix(*, key: str, prefix: str) -> str:
    pfx = (prefix or "").strip(":")
    want = f"{pfx}:element:"
    if not key.startswith(want):
        raise ApiError("INVALID_INPUT", "not an element key", status_code=422, details={"key": key})
    return key[len(want) :]


@app.post("/api/v1/explorer/northwind/data_ingest")
async def northwind_data_ingest(body: NorthwindDataIngestRequest) -> dict[str, Any]:
    ns_to_use = (body.ns or "").strip() or "or"
    ns_id, ent, _ = _resolve_ns_entry(ns_to_use)
    if ent.layout != "or_layout_v2":
        raise ApiError(
            "INVALID_INPUT",
            "northwind data ingest requires OR layout",
            status_code=422,
            details={"ns": ns_id, "layout": ent.layout},
        )
    r = redis_client()
    data = ingest_data_rows(
        r=r,
        prefix=ent.prefix,
        tables=body.tables,
        reset=(bool(body.reset) if body.reset is not None else False),
        max_rows_per_table=int(body.max_rows_per_table or 0),
    )
    return ok({"ns": ns_id, "prefix": ent.prefix.strip(":"), **data})


@app.get("/api/v1/explorer/northwind/data_info")
async def northwind_data_info_route(ns: str | None = None) -> dict[str, Any]:
    ns_id, ent, _ = _resolve_ns_entry((ns or "").strip() or "or")
    if ent.layout != "or_layout_v2":
        raise ApiError(
            "INVALID_INPUT",
            "northwind data info requires OR layout",
            status_code=422,
            details={"ns": ns_id, "layout": ent.layout},
        )
    r = redis_client()
    info = northwind_data_info(r=r, prefix=ent.prefix)
    return ok({"ns": ns_id, "prefix": ent.prefix.strip(":"), **info})


@app.post("/api/v1/explorer/northwind/compare")
async def northwind_compare(body: NorthwindCompareRequest) -> dict[str, Any]:
    ns_to_use = (body.ns or "").strip() or "or"
    ns_id, ent, _ = _resolve_ns_entry(ns_to_use)
    if ent.layout != "or_layout_v2":
        raise ApiError(
            "INVALID_INPUT",
            "northwind compare requires OR layout",
            status_code=422,
            details={"ns": ns_id, "layout": ent.layout},
        )
    r = redis_client()
    data = compare_sql_vs_bitsets(
        r=r,
        prefix=ent.prefix,
        table=body.table,
        predicate_type=body.predicate.type,
        conditions=[c.model_dump() for c in body.predicate.conditions],
        sample=int(body.sample or 0),
    )
    return ok({"ns": ns_id, "prefix": ent.prefix.strip(":"), **data})


@app.get("/api/v1/schema/tables")
async def schema_tables(ns: str | None = None) -> dict[str, Any]:
    ns_id, ent, _ = _resolve_ns_entry(ns)
    pfx = ent.prefix.strip(":")
    r = redis_client()
    keys = _scan_keys(r=r, match=f"{pfx}:element:tbl:*", max_keys=5000)
    tables: list[dict[str, Any]] = []
    for k in keys:
        name = _strip_element_key_prefix(key=k, prefix=pfx)
        if not name.startswith("tbl:"):
            continue
        table = name[len("tbl:") :]
        if not table:
            continue
        tables.append({"table": table, "key": k})
    return ok({"ns": ns_id, "prefix": pfx, "tables": tables})


@app.get("/api/v1/schema/tables/{table}")
async def schema_table(table: str, ns: str | None = None) -> dict[str, Any]:
    table = (table or "").strip()
    if not table or len(table) > 100:
        raise ApiError("INVALID_INPUT", "table is required", status_code=422)

    ns_id, ent, _ = _resolve_ns_entry(ns)
    pfx = ent.prefix.strip(":")
    r = redis_client()

    col_keys = _scan_keys(r=r, match=f"{pfx}:element:col:{table}:*", max_keys=50_000)
    rel_from_keys = _scan_keys(r=r, match=f"{pfx}:element:rel:{table}:*", max_keys=50_000)
    rel_to_keys = _scan_keys(r=r, match=f"{pfx}:element:rel:*:{table}:*", max_keys=50_000)
    rel_keys = sorted(set(rel_from_keys) | set(rel_to_keys))

    pipe = r.pipeline(transaction=False)
    for k in col_keys:
        pipe.hget(k, "flags_bin")
    for k in rel_keys:
        pipe.hget(k, "flags_bin")
    raw = pipe.execute()

    cols: list[dict[str, Any]] = []
    for i, k in enumerate(col_keys):
        flags_bin = raw[i] if i < len(raw) else None
        if isinstance(flags_bin, str):
            flags_bin = flags_bin.encode("utf-8")
        if not isinstance(flags_bin, (bytes, bytearray)) or len(flags_bin) != 512:
            continue
        bits = set(decode_flags_bin(bytes(flags_bin)))

        name = _strip_element_key_prefix(key=k, prefix=pfx)
        if not name.startswith(f"col:{table}:"):
            continue
        col_name = name[len(f"col:{table}:") :]
        meta = decode_column_meta(bits)
        cols.append(
            {
                "name": col_name,
                "type_family": meta.type_family,
                "not_null": meta.not_null,
                "has_default": meta.has_default,
                "is_pk": meta.is_pk,
                "is_fk": meta.is_fk,
                "has_index": meta.has_index,
                "length_bucket": meta.length_bucket,
                "key": k,
            }
        )
    cols.sort(key=lambda it: it.get("name") or "")

    rels: list[dict[str, Any]] = []
    base = len(col_keys)
    for j, k in enumerate(rel_keys):
        flags_bin = raw[base + j] if base + j < len(raw) else None
        if isinstance(flags_bin, str):
            flags_bin = flags_bin.encode("utf-8")
        if not isinstance(flags_bin, (bytes, bytearray)) or len(flags_bin) != 512:
            continue
        bits = set(decode_flags_bin(bytes(flags_bin)))

        name = _strip_element_key_prefix(key=k, prefix=pfx)
        parts = name.split(":", 3)
        if len(parts) != 4 or parts[0] != "rel":
            continue
        from_table, to_table, fk = parts[1], parts[2], parts[3]
        direction = "from" if from_table == table else "to" if to_table == table else "other"
        meta = decode_relation_meta(bits)
        rels.append(
            {
                "from_table": from_table,
                "to_table": to_table,
                "fk": fk,
                "direction": direction,
                "cardinality": meta.cardinality,
                "child_required": meta.child_required,
                "on_delete": meta.on_delete,
                "on_update": meta.on_update,
                "key": k,
            }
        )

    rels.sort(key=lambda it: (it.get("direction") or "", it.get("from_table") or "", it.get("to_table") or "", it.get("fk") or ""))

    return ok({"ns": ns_id, "prefix": pfx, "table": table, "columns": cols, "relations": rels})

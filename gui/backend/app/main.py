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
from .models import ExamplesRunRequest, PutRequest, QueryRequest, StoreRequest
from .redis_bits import decode_flags_bin, element_key_with_prefix
from .settings import load_settings
from .bitmaps import load_bitmaps_from_preset, save_bitmaps_to_preset
from .namespaces import NamespaceEntry, load_namespaces_from_preset, namespaces_to_map
from .namespace_discovery import DiscoveryLimits, discover_namespaces, write_namespaces_generated
from .examples import get_example_readme, list_examples, run_example, run_reports


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
        if layout_id != "er_layout_v1":
            continue
        try:
            element_count = int(r.scard(f"{prefix}:all"))
        except Exception:
            element_count = 0
        out.append({"name": ns_id, "key_count": element_count, "updated_at": now})
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
    if ent.layout != "er_layout_v1":
        return {"items": [], "page": page, "page_size": page_size, "total": 0}
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
        total = 0

    start = (page - 1) * page_size
    end = start + page_size

    cursor = 0
    matched: list[str] = []
    scanned = 0
    max_scan = 200_000
    while True:
        cursor, batch = r.sscan(universe_key, cursor=cursor, count=1000)
        for raw_name in batch:
            scanned += 1
            if scanned > max_scan:
                cursor = 0
                break
            name = raw_name.decode("utf-8", errors="replace") if isinstance(raw_name, (bytes, bytearray)) else str(raw_name)
            if not name:
                continue
            if q and q not in name.lower():
                continue
            if q:
                total += 1
            matched.append(name)
        if cursor == 0:
            break

    if not q and total == 0 and matched:
        total = len(matched)

    page_names = matched[start:end]
    keys = [element_key_with_prefix(prefix, n) for n in page_names]

    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.hget(k, "flags_bin")
        pipe.ttl(k)
    raw = pipe.execute() if keys else []

    items: list[dict[str, Any]] = []
    for i, name in enumerate(page_names):
        flags_bin = raw[i * 2] if i * 2 < len(raw) else None
        ttl = raw[i * 2 + 1] if i * 2 + 1 < len(raw) else None
        if isinstance(flags_bin, str):
            flags_bin = flags_bin.encode("utf-8")
        if not isinstance(flags_bin, (bytes, bytearray)) or len(flags_bin) != 512:
            set_bits_count = 0
        else:
            try:
                set_bits_count = _count_set_bits(bytes(flags_bin))
            except Exception:
                set_bits_count = 0
        ttl_out = None
        if isinstance(ttl, (int, float)) and int(ttl) >= 0:
            ttl_out = int(ttl)
        items.append(
            {
                "key": element_key_with_prefix(prefix, name),
                "short_name": name,
                "set_bits_count": set_bits_count,
                "ttl": ttl_out,
            }
        )

    return {"items": items, "page": page, "page_size": page_size, "total": int(total)}


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

    name = _decode_element_key(key=raw_key, prefix=prefix)

    r = redis_client()
    flags_bin = r.hget(raw_key, "flags_bin")
    if flags_bin is None:
        raise ApiError("NOT_FOUND", "element not found", status_code=404, details={"key": raw_key})
    if isinstance(flags_bin, str):
        flags_bin = flags_bin.encode("utf-8")
    if not isinstance(flags_bin, (bytes, bytearray)):
        raise ApiError("INVALID_FLAGS", "flags_bin must be bytes", status_code=502)
    bits = decode_flags_bin(bytes(flags_bin))
    ttl = r.ttl(raw_key)
    ttl_out = None
    if isinstance(ttl, (int, float)) and int(ttl) >= 0:
        ttl_out = int(ttl)

    return {
        "key": raw_key,
        "short_name": name,
        "namespace": ns_id,
        "bits": 4096,
        "set_bits": bits,
        "ttl": ttl_out,
    }


@app.get("/api/v1/explorer/namespaces/{namespace}/bitmap")
async def explorer_namespace_bitmap(namespace: str, limit: int = 75, offset: int = 0) -> dict[str, Any]:
    if limit <= 0 or limit > 200:
        raise ApiError("INVALID_INPUT", "limit must be 1..200", status_code=422)
    if offset < 0:
        raise ApiError("INVALID_INPUT", "offset must be >= 0", status_code=422)

    ns_id, ent, _ = _resolve_ns_entry(namespace)
    if ent.layout != "er_layout_v1":
        return {"namespace": ns_id, "bits": 4096, "elements": []}
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

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import redis
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .cli_adapter import er_cli_put, er_cli_query_with_count, er_cli_store_key
from .errors import ApiError, err, ok
from .models import PutRequest, QueryRequest, StoreRequest
from .redis_bits import decode_flags_bin, element_key_with_prefix
from .settings import load_settings
from .bitmaps import load_bitmaps_from_preset, save_bitmaps_to_preset
from .namespaces import load_namespaces_from_preset, namespaces_to_map


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


def _resolve_ns(ns: str | None) -> tuple[str, str]:
    doc = load_namespaces_from_preset(presets_dir=settings.presets_dir, preset=settings.gui_preset, logger=logger)
    default_id, mp = namespaces_to_map(doc)
    ns_id = (ns or "").strip() or default_id
    ent = mp.get(ns_id)
    if not ent:
        raise ApiError("INVALID_INPUT", "unknown namespace", status_code=422, details={"ns": ns_id})
    return ns_id, ent.prefix


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

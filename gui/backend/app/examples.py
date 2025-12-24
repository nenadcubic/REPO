from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import redis

from .cli_adapter import er_cli_put
from .errors import ApiError
from .northwind_compare import (
    import_northwind,
    report_order_totals_sample,
    report_row_counts,
    resolve_or_layout,
    resolve_sqlite_path,
)
from .redis_bits import decode_flags_bin, element_key_with_prefix


_EXAMPLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_MAX_EXAMPLE_JSON_BYTES = 200_000
_MAX_README_BYTES = 300_000


@dataclass(frozen=True)
class ExampleElement:
    name: str
    bits: list[int]


@dataclass(frozen=True)
class ExampleReference:
    kind: Literal["none", "sqlite", "external"]
    path: str | None


@dataclass(frozen=True)
class ExampleCompareReport:
    id: str
    title: str


@dataclass(frozen=True)
class ExampleDef:
    id: str
    title: str
    type: Literal["seed", "dataset_compare"]
    description: str
    default_namespace: str
    tags: list[str]
    elements: list[ExampleElement] | None = None
    queries: list[dict[str, Any]] | None = None
    reference: ExampleReference | None = None
    compare_reports: list[ExampleCompareReport] | None = None
    dir: Path | None = None


def _discover_examples_dir() -> Path | None:
    env = (os.getenv("ER_GUI_EXAMPLES_DIR") or "").strip()
    if env:
        p = Path(env)
        if p.is_dir():
            return p

    here = Path(__file__).resolve()
    for parent in here.parents:
        cand = parent / "examples"
        if cand.is_dir():
            return cand
    return None


def _validate_example_id(example_id: str) -> str:
    ex_id = (example_id or "").strip()
    if not ex_id or not _EXAMPLE_ID_RE.fullmatch(ex_id):
        raise ApiError("INVALID_INPUT", "invalid example id", status_code=422, details={"id": ex_id})
    return ex_id


def _example_dir_for(*, base: Path, example_id: str) -> Path:
    ex_id = _validate_example_id(example_id)
    p = (base / ex_id).resolve()
    base_resolved = base.resolve()
    if not p.is_relative_to(base_resolved):
        raise ApiError("INVALID_INPUT", "invalid example path", status_code=422, details={"id": ex_id})
    if not p.is_dir():
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": ex_id})
    return p


def _read_small_text(path: Path, *, max_bytes: int) -> str:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        raise ApiError("NOT_FOUND", "file not found", status_code=404, details={"path": str(path)})
    if len(data) > max_bytes:
        raise ApiError("INVALID_INPUT", "file too large", status_code=422, details={"path": str(path), "max_bytes": max_bytes})
    return data.decode("utf-8", errors="replace")


def _require_str_field(doc: dict[str, Any], key: str) -> str:
    v = doc.get(key)
    if not isinstance(v, str) or not v.strip():
        raise ApiError("INVALID_INPUT", f"missing required field: {key}", status_code=422)
    return v.strip()


def _parse_tags(doc: dict[str, Any]) -> list[str]:
    raw = doc.get("tags")
    if not isinstance(raw, list):
        raise ApiError("INVALID_INPUT", "tags must be a list", status_code=422)
    out: list[str] = []
    seen: set[str] = set()
    for it in raw:
        if not isinstance(it, str):
            continue
        s = it.strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _parse_reference(doc: dict[str, Any]) -> ExampleReference:
    raw = doc.get("reference")
    if not isinstance(raw, dict):
        raise ApiError("INVALID_INPUT", "reference must be an object", status_code=422)
    kind = str(raw.get("kind") or "").strip()
    if kind not in ("none", "sqlite", "external"):
        raise ApiError("INVALID_INPUT", "invalid reference.kind", status_code=422, details={"kind": kind})
    path = raw.get("path")
    if path is None:
        return ExampleReference(kind=kind, path=None)
    if not isinstance(path, str):
        raise ApiError("INVALID_INPUT", "invalid reference.path", status_code=422)
    p = path.strip()
    return ExampleReference(kind=kind, path=(p if p else None))


def _parse_compare_reports(doc: dict[str, Any]) -> list[ExampleCompareReport]:
    raw = doc.get("compare_reports")
    if not isinstance(raw, list):
        raise ApiError("INVALID_INPUT", "compare_reports must be a list", status_code=422)
    out: list[ExampleCompareReport] = []
    seen: set[str] = set()
    for it in raw:
        if not isinstance(it, dict):
            continue
        rid = str(it.get("id") or "").strip()
        title = str(it.get("title") or "").strip()
        if not rid or not title:
            continue
        if not _EXAMPLE_ID_RE.fullmatch(rid):
            continue
        if rid in seen:
            continue
        seen.add(rid)
        out.append(ExampleCompareReport(id=rid, title=title))
    if not out:
        raise ApiError("INVALID_INPUT", "example has no valid compare_reports", status_code=422)
    return out


def _load_example_from_dir(*, base: Path, example_id: str) -> ExampleDef:
    p = _example_dir_for(base=base, example_id=example_id)
    if not (p / "README.md").is_file():
        raise ApiError("INVALID_INPUT", "missing README.md", status_code=422, details={"id": example_id})

    raw = _read_small_text(p / "example.json", max_bytes=_MAX_EXAMPLE_JSON_BYTES)
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ApiError("INVALID_INPUT", "invalid example.json", status_code=422, details={"id": example_id, "error": str(e)})
    if not isinstance(doc, dict):
        raise ApiError("INVALID_INPUT", "invalid example.json", status_code=422, details={"id": example_id})

    ex_id = _validate_example_id(str(doc.get("id") or ""))
    if ex_id != example_id:
        raise ApiError(
            "INVALID_INPUT",
            "example.json id mismatch",
            status_code=422,
            details={"dir": example_id, "id": ex_id},
        )
    title = _require_str_field(doc, "title")
    desc = _require_str_field(doc, "description")
    ex_type = str(doc.get("type") or "").strip() or "seed"
    if ex_type not in ("seed", "dataset_compare"):
        raise ApiError("INVALID_INPUT", "invalid type", status_code=422, details={"type": ex_type})

    default_ns = _require_str_field(doc, "default_namespace")
    tags = _parse_tags(doc)
    ref = _parse_reference(doc)

    if ex_type == "dataset_compare":
        if ref.kind != "sqlite" or not ref.path:
            raise ApiError(
                "INVALID_INPUT",
                "dataset_compare examples require reference.kind=sqlite and a non-empty reference.path",
                status_code=422,
                details={"id": ex_id, "reference": {"kind": ref.kind, "path": ref.path}},
            )
        reports = _parse_compare_reports(doc)
        return ExampleDef(
            id=ex_id,
            title=title,
            type="dataset_compare",
            description=desc,
            default_namespace=default_ns,
            tags=tags,
            reference=ref,
            compare_reports=reports,
            dir=p,
        )

    if ref.kind != "none" or ref.path is not None:
        raise ApiError(
            "INVALID_INPUT",
            'seed examples require reference.kind="none" and reference.path=null',
            status_code=422,
            details={"id": ex_id, "reference": {"kind": ref.kind, "path": ref.path}},
        )

    if not isinstance(doc.get("elements"), list):
        raise ApiError("INVALID_INPUT", "elements must be a list", status_code=422, details={"id": ex_id})

    elements: list[ExampleElement] = []
    for it in doc["elements"]:
        if not isinstance(it, dict):
            continue
        name = str(it.get("name") or "").strip()
        bits_raw = it.get("bits")
        if not name or len(name) > 100:
            continue
        if not isinstance(bits_raw, list):
            continue
        bits: list[int] = []
        ok_bits = True
        for b in bits_raw:
            if not isinstance(b, int):
                ok_bits = False
                break
            if b < 0 or b > 4095:
                ok_bits = False
                break
            bits.append(b)
        if not ok_bits:
            continue
        uniq = sorted(set(bits))
        if not uniq:
            continue
        elements.append(ExampleElement(name=name, bits=uniq))

    if not elements:
        raise ApiError("INVALID_INPUT", "example has no valid elements", status_code=422, details={"id": ex_id})

    queries = doc.get("queries")
    if queries is not None and not isinstance(queries, list):
        queries = None

    return ExampleDef(
        id=ex_id,
        title=title,
        type="seed",
        description=desc,
        default_namespace=default_ns,
        tags=tags,
        elements=elements,
        queries=queries,
        reference=ref,
        dir=p,
    )


_REGISTRY: dict[str, ExampleDef] | None = None
_REGISTRY_BASE: Path | None = None


def list_examples(*, logger: Any | None = None) -> list[ExampleDef]:
    global _REGISTRY, _REGISTRY_BASE
    if _REGISTRY is not None:
        return list(_REGISTRY.values())

    base = _discover_examples_dir()
    if not base:
        _REGISTRY = {}
        _REGISTRY_BASE = None
        return []

    registry: dict[str, ExampleDef] = {}
    _REGISTRY_BASE = base
    for child in sorted(base.iterdir(), key=lambda p: p.name):
        if not child.is_dir():
            continue
        ex_id = child.name
        if not _EXAMPLE_ID_RE.fullmatch(ex_id):
            continue
        if not (child / "example.json").is_file():
            continue
        try:
            ex = _load_example_from_dir(base=base, example_id=ex_id)
        except ApiError as e:
            if logger:
                logger.warning("Skipping example id=%s: %s", ex_id, e.message)
            continue
        if ex.id in registry:
            if logger:
                logger.warning("Skipping duplicate example id=%s", ex.id)
            continue
        registry[ex.id] = ex

    _REGISTRY = registry
    return list(registry.values())


def get_example_readme(*, example_id: str) -> dict[str, Any]:
    ex = _get_example_def(example_id=example_id, logger=None)
    if not ex.dir:
        raise ApiError("NOT_FOUND", "examples directory not available", status_code=404)
    md = _read_small_text(ex.dir / "README.md", max_bytes=_MAX_README_BYTES)
    return {"markdown": md}


def _get_example_def(*, example_id: str, logger: Any | None) -> ExampleDef:
    ex_id = _validate_example_id(example_id)
    ex = next((x for x in list_examples(logger=logger) if x.id == ex_id), None)
    if not ex:
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": ex_id})
    return ex


def _seed_registry_key(*, prefix: str, example_id: str) -> str:
    pfx = (prefix or "").strip(":")
    return f"{pfx}:example:{example_id}:created"


def reset_seed_example(*, r: redis.Redis, prefix: str, example_id: str) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    reg = _seed_registry_key(prefix=pfx, example_id=example_id)
    universe_key = f"{pfx}:all"

    members = r.smembers(reg)
    names = [m.decode("utf-8", errors="replace") if isinstance(m, bytes) else str(m) for m in members]
    names = [n for n in names if n]

    pipe = r.pipeline(transaction=False)
    deleted_elements = 0
    for name in names:
        el_key = element_key_with_prefix(pfx, name)
        flags = r.get(el_key)
        bits: list[int] = []
        if isinstance(flags, (bytes, bytearray)) and len(flags) == 512:
            try:
                bits = decode_flags_bin(bytes(flags))
            except Exception:
                bits = []
        pipe.delete(el_key)
        pipe.srem(universe_key, name)
        for b in bits:
            pipe.srem(f"{pfx}:idx:bit:{b}", name)
        deleted_elements += 1

    pipe.delete(reg)
    pipe.execute()
    return {"mode": "example_registry", "scanned": len(names), "deleted_elements": deleted_elements}


def run_example(
    *,
    example_id: str,
    ns: str,
    prefix: str,
    layout_id: str,
    namespaces_doc: dict[str, Any],
    reset: bool,
    r: redis.Redis,
    er_cli_path: str,
    redis_host: str,
    redis_port: int,
    logger: Any,
) -> dict[str, Any]:
    ex = _get_example_def(example_id=example_id, logger=logger)

    if ex.type == "dataset_compare":
        if ex.id != "northwind_compare":
            raise ApiError("INVALID_INPUT", "unknown dataset_compare example", status_code=422, details={"id": ex.id})
        if not ex.reference or ex.reference.kind != "sqlite" or not ex.reference.path:
            raise ApiError("INVALID_INPUT", "invalid reference", status_code=422, details={"id": ex.id})
        if not ex.dir:
            raise ApiError("NOT_FOUND", "example directory not available", status_code=404, details={"id": ex.id})
        sqlite_path = resolve_sqlite_path(example_dir=ex.dir, ref_path=ex.reference.path)
        tpl = resolve_or_layout(namespaces_doc=namespaces_doc, layout_id=layout_id)
        data = import_northwind(r=r, prefix=prefix, tpl=tpl, sqlite_path=sqlite_path, reset=reset, logger=logger)
        return {"id": example_id, "type": "dataset_compare", "ns": ns, **data}

    reset_info = None
    if reset:
        reset_info = reset_seed_example(r=r, prefix=prefix, example_id=example_id)

    created_total = 0
    updated_total = 0
    skipped_total = 0
    created: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []
    sample_cap = 200

    seen_names: set[str] = set()
    for el in ex.elements or []:
        name = (el.name or "").strip()
        if not name or len(name) > 100:
            skipped_total += 1
            if len(skipped) < sample_cap:
                skipped.append(name or "<empty>")
            continue
        if name in seen_names:
            skipped_total += 1
            if len(skipped) < sample_cap:
                skipped.append(name)
            continue
        seen_names.add(name)

        key = f"{prefix}:element:{name}"
        existed = bool(r.exists(key))
        er_cli_put(
            er_cli_path=er_cli_path,
            redis_host=redis_host,
            redis_port=redis_port,
            redis_prefix=prefix,
            name=name,
            bits=sorted(set(el.bits)),
        )
        if existed:
            updated_total += 1
            if len(updated) < sample_cap:
                updated.append(name)
        else:
            created_total += 1
            if len(created) < sample_cap:
                created.append(name)
            r.sadd(_seed_registry_key(prefix=prefix, example_id=example_id), name)

    logger.info(
        "examples run id=%s ns=%s created=%d updated=%d skipped=%d",
        example_id,
        ns,
        created_total,
        updated_total,
        skipped_total,
    )

    data: dict[str, Any] = {
        "id": example_id,
        "type": "seed",
        "ns": ns,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "counts": {"created": created_total, "updated": updated_total, "skipped": skipped_total},
        "samples_truncated": {
            "created": created_total > len(created),
            "updated": updated_total > len(updated),
            "skipped": skipped_total > len(skipped),
        },
    }
    if reset_info is not None:
        data["reset"] = reset_info
    return data


def run_reports(
    *,
    example_id: str,
    ns: str,
    prefix: str,
    layout_id: str,
    namespaces_doc: dict[str, Any],
    r: redis.Redis,
    logger: Any,
) -> dict[str, Any]:
    ex = _get_example_def(example_id=example_id, logger=logger)
    if ex.type != "dataset_compare":
        raise ApiError("INVALID_INPUT", "reports supported only for dataset_compare examples", status_code=422)
    if ex.id != "northwind_compare":
        raise ApiError("INVALID_INPUT", "unknown dataset_compare example", status_code=422, details={"id": ex.id})
    if not ex.reference or ex.reference.kind != "sqlite" or not ex.reference.path:
        raise ApiError("INVALID_INPUT", "invalid reference", status_code=422, details={"id": ex.id})
    if not ex.dir:
        raise ApiError("NOT_FOUND", "example directory not available", status_code=404, details={"id": ex.id})
    sqlite_path = resolve_sqlite_path(example_dir=ex.dir, ref_path=ex.reference.path)
    tpl = resolve_or_layout(namespaces_doc=namespaces_doc, layout_id=layout_id)

    row_counts = report_row_counts(r=r, prefix=prefix, tpl=tpl, sqlite_path=sqlite_path)
    order_totals = report_order_totals_sample(r=r, prefix=prefix, tpl=tpl, sqlite_path=sqlite_path, limit=20)
    return {
        "id": example_id,
        "type": "dataset_compare",
        "ns": ns,
        "rounding": "2dp_half_up",
        "reports": {"row_counts": row_counts, "order_totals_sample": order_totals},
    }

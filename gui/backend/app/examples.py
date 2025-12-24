from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis

from .cli_adapter import er_cli_put
from .errors import ApiError
from .northwind_compare import import_northwind, report_order_totals_sample, report_row_counts, resolve_or_layout, resolve_sqlite_path


_EXAMPLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_MAX_EXAMPLE_JSON_BYTES = 200_000
_MAX_README_BYTES = 300_000


@dataclass(frozen=True)
class ExampleElement:
    name: str
    bits: list[int]


@dataclass(frozen=True)
class ExampleDef:
    id: str
    title: str
    description: str
    type: str  # "seed" | "dataset_compare"
    namespace: str | None = None
    elements: list[ExampleElement] | None = None
    queries: list[dict[str, Any]] | None = None
    reference: dict[str, Any] | None = None
    targets: list[dict[str, Any]] | None = None
    compare_reports: list[dict[str, Any]] | None = None


def _builtin_examples() -> list[ExampleDef]:
    return [
        ExampleDef(
            id="basic_seed",
            title="Basic seed",
            description="Small deterministic dataset for quick manual testing of Elements, Queries, Store+TTL, and Matrix.",
            type="seed",
            namespace="er",
            elements=[
                ExampleElement("alice", [1, 7, 42]),
                ExampleElement("bob", [7, 9, 1024]),
                ExampleElement("carol", [1, 9, 13, 2048]),
                ExampleElement("dave", [0, 1, 2, 3, 4]),
                ExampleElement("eve", [4095]),
            ],
            queries=[
                {"type": "find", "bit": 7, "note": "Elements containing bit 7"},
                {"type": "find_all", "bits": [1, 9], "note": "Elements containing both 1 and 9"},
            ],
        ),
        ExampleDef(
            id="math_universe",
            title="Math universe (mini)",
            description="Mini version inspired by /examples/math_universe (no scripts executed).",
            type="seed",
            namespace="er",
            elements=[
                ExampleElement("U", [0, 1, 2, 3, 4, 5]),
                ExampleElement("A", [0, 2, 4]),
                ExampleElement("B", [1, 2, 3]),
                ExampleElement("C", [3, 5]),
            ],
        ),
        ExampleDef(
            id="northwind",
            title="Northwind (mini)",
            description="Mini dataset inspired by /examples/northwind (no DB/scripts; names are illustrative).",
            type="seed",
            namespace="er",
            elements=[
                ExampleElement("cust:ALFKI", [0, 10, 20]),
                ExampleElement("cust:ANATR", [0, 11, 21]),
                ExampleElement("order:10248", [1, 10, 30]),
                ExampleElement("order:10249", [1, 11, 31]),
            ],
        ),
    ]


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


def _load_example_from_dir(*, base: Path, example_id: str) -> ExampleDef:
    p = _example_dir_for(base=base, example_id=example_id)
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
    title = str(doc.get("title") or "").strip()
    desc = str(doc.get("description") or "").strip()
    ex_type = str(doc.get("type") or "seed").strip() or "seed"
    if not title or not desc:
        raise ApiError("INVALID_INPUT", "missing required fields in example.json", status_code=422, details={"id": ex_id})

    if ex_type == "dataset_compare":
        ref = doc.get("reference") if isinstance(doc.get("reference"), dict) else None
        targets = doc.get("targets") if isinstance(doc.get("targets"), list) else None
        reports = doc.get("compare_reports") if isinstance(doc.get("compare_reports"), list) else None
        if not ref or not targets or not reports:
            raise ApiError(
                "INVALID_INPUT",
                "missing required fields in example.json",
                status_code=422,
                details={"id": ex_id, "required": ["id", "title", "type", "description", "reference", "targets", "compare_reports"]},
            )
        return ExampleDef(
            id=ex_id,
            title=title,
            description=desc,
            type="dataset_compare",
            reference=ref,
            targets=targets,
            compare_reports=reports,
        )

    ns = str(doc.get("namespace") or "").strip()
    if not ns:
        raise ApiError(
            "INVALID_INPUT",
            "missing required fields in example.json",
            status_code=422,
            details={"id": ex_id, "required": ["id", "title", "namespace", "description", "elements"]},
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

    return ExampleDef(id=ex_id, title=title, description=desc, type="seed", namespace=ns, elements=elements, queries=queries)


def list_examples(*, logger: Any | None = None) -> list[ExampleDef]:
    base = _discover_examples_dir()
    if not base:
        return _builtin_examples()

    out: list[ExampleDef] = []
    seen: set[str] = set()
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
        if ex.id in seen:
            continue
        seen.add(ex.id)
        out.append(ex)

    return out if out else _builtin_examples()


def get_example_readme(*, example_id: str) -> dict[str, Any]:
    base = _discover_examples_dir()
    if not base:
        raise ApiError("NOT_FOUND", "examples directory not available", status_code=404)
    ex_id = _validate_example_id(example_id)
    p = _example_dir_for(base=base, example_id=ex_id)
    readme_path = p / "README.md"
    md = _read_small_text(readme_path, max_bytes=_MAX_README_BYTES)
    return {"id": ex_id, "readme": md}


def _get_example_def(*, example_id: str, logger: Any | None) -> ExampleDef:
    ex_id = _validate_example_id(example_id)
    base = _discover_examples_dir()
    if base:
        return _load_example_from_dir(base=base, example_id=ex_id)
    ex = next((e for e in _builtin_examples() if e.id == ex_id), None)
    if not ex:
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": ex_id})
    if logger:
        logger.warning("examples dir not found; using builtin example id=%s", ex_id)
    return ex


def _canonical_key_allowed(prefix: str, key: str) -> bool:
    pfx = (prefix or "").strip(":")
    if not pfx:
        return False
    return (
        key == f"{pfx}:all"
        or key.startswith(f"{pfx}:element:")
        or key.startswith(f"{pfx}:idx:bit:")
        or key.startswith(f"{pfx}:tmp:")
    )


def reset_namespace(*, r: redis.Redis, prefix: str, max_scan: int = 50000) -> dict[str, Any]:
    pfx = (prefix or "").strip(":")
    if not pfx:
        raise ApiError("INVALID_INPUT", "invalid namespace prefix", status_code=422)

    scanned = 0
    deleted = 0
    skipped = 0
    unknown = 0

    cursor = 0
    while True:
        cursor, batch = r.scan(cursor=cursor, match=f"{pfx}:*", count=1000)
        keys = []
        for raw in batch:
            k = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
            scanned += 1
            if _canonical_key_allowed(pfx, k):
                keys.append(k)
            else:
                unknown += 1
            if scanned >= max_scan:
                break
        if keys:
            deleted += int(r.delete(*keys))
        if cursor == 0 or scanned >= max_scan:
            break

    # "skipped" are keys we saw but refused to delete due to unknown pattern
    skipped = unknown
    return {"scanned": scanned, "deleted": deleted, "skipped": skipped}


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
        ref = ex.reference or {}
        if str(ref.get("kind") or "").strip() != "sqlite":
            raise ApiError("INVALID_INPUT", "unsupported reference kind", status_code=422, details={"kind": ref.get("kind")})
        sqlite_path = resolve_sqlite_path(str(ref.get("path") or ""))
        tpl = resolve_or_layout(namespaces_doc=namespaces_doc, layout_id=layout_id)
        data = import_northwind(r=r, prefix=prefix, tpl=tpl, sqlite_path=sqlite_path, reset=reset, logger=logger)
        return {"id": example_id, "type": "dataset_compare", "ns": ns, **data}

    if ex.type != "seed":
        raise ApiError("INVALID_INPUT", "unknown example type", status_code=422, details={"type": ex.type})

    reset_info = None
    if reset:
        reset_info = reset_namespace(r=r, prefix=prefix)

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
    ref = ex.reference or {}
    if str(ref.get("kind") or "").strip() != "sqlite":
        raise ApiError("INVALID_INPUT", "unsupported reference kind", status_code=422, details={"kind": ref.get("kind")})

    sqlite_path = resolve_sqlite_path(str(ref.get("path") or ""))
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

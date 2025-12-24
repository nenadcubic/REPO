from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ApiError


BITMAPS_SCHEMA_V1 = "er.gui.bitmaps.v1"


@dataclass(frozen=True)
class BitmapItem:
    bit: int
    key: str | None
    name: str | None
    group: str | None
    description: str | None
    source: str  # "item" | "range"


def _as_int(v: Any) -> int | None:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float) and v.is_integer():
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s, 10)
        except ValueError:
            return None
    return None


def _bit_ok(bit: int) -> bool:
    return 0 <= bit <= 4095


def _sanitize_ns(ns: str) -> str:
    s = (ns or "").strip()
    if not s:
        return "er"
    safe = []
    for ch in s:
        if ch.isalnum() or ch in ("_", "-", "."):
            safe.append(ch)
    return "".join(safe) or "er"


def _bitmaps_path(*, presets_dir: str, preset: str, ns: str) -> Path:
    ns2 = _sanitize_ns(ns)
    return Path(presets_dir) / preset / "bitmaps" / f"{ns2}.json"


def _legacy_bitmaps_path(*, presets_dir: str, preset: str) -> Path:
    return Path(presets_dir) / preset / "bitmaps.json"


def load_bitmaps_from_preset(
    *, presets_dir: str, preset: str, ns: str, logger: Any
) -> dict[str, Any]:
    ns2 = _sanitize_ns(ns)
    path = _bitmaps_path(presets_dir=presets_dir, preset=preset, ns=ns2)
    legacy_path = _legacy_bitmaps_path(presets_dir=presets_dir, preset=preset)

    read_path: Path | None = path if path.exists() else (legacy_path if legacy_path.exists() else None)
    legacy_used = read_path == legacy_path
    if read_path is None:
        doc = {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {},
            "groups": {},
            "labels": {},
            "defaults": {},
            "items": [],
            "ranges": [],
        }
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "ns": ns2, "missing": True},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
            "document": doc,
        }

    try:
        doc = json.loads(read_path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("bitmaps.json parse failed: %s (path=%s)", e, str(read_path))
        doc_out = {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {},
            "groups": {},
            "labels": {},
            "defaults": {},
            "items": [],
            "ranges": [],
        }
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "ns": ns2, "path": str(read_path), "invalid_json": True, "legacy": legacy_used},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
            "document": doc_out,
        }

    if not isinstance(doc, dict):
        logger.warning("bitmaps.json root must be object (path=%s)", str(read_path))
        doc_out = {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {},
            "groups": {},
            "labels": {},
            "defaults": {},
            "items": [],
            "ranges": [],
        }
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "ns": ns2, "path": str(read_path), "invalid_root": True, "legacy": legacy_used},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
            "document": doc_out,
        }

    schema = doc.get("schema")
    if schema != BITMAPS_SCHEMA_V1:
        logger.warning("bitmaps.json schema mismatch: %s (path=%s)", schema, str(read_path))
        doc_out = {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {},
            "groups": {},
            "labels": {},
            "defaults": {},
            "items": [],
            "ranges": [],
        }
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {
                "preset": preset,
                "ns": ns2,
                "path": str(read_path),
                "invalid_schema": True,
                "found_schema": schema,
                "legacy": legacy_used,
            },
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
            "document": doc_out,
        }

    meta = doc.get("meta") if isinstance(doc.get("meta"), dict) else {}
    groups = doc.get("groups") if isinstance(doc.get("groups"), dict) else {}
    labels = doc.get("labels") if isinstance(doc.get("labels"), dict) else {}
    defaults = doc.get("defaults") if isinstance(doc.get("defaults"), dict) else {}

    default_group = defaults.get("group") if isinstance(defaults.get("group"), str) else None

    document_items: list[dict[str, Any]] = []
    items_in = doc.get("items") if isinstance(doc.get("items"), list) else []

    explicit: dict[int, BitmapItem] = {}
    for raw in items_in:
        if not isinstance(raw, dict):
            logger.warning("bitmaps item ignored (not object): %r", raw)
            continue
        bit = _as_int(raw.get("bit"))
        if bit is None or not _bit_ok(bit):
            logger.warning("bitmaps item ignored (invalid bit): %r", raw.get("bit"))
            continue
        if bit in explicit:
            logger.warning("bitmaps item ignored (duplicate bit=%d)", bit)
            continue
        document_items.append(
            {
                "bit": bit,
                "key": raw.get("key") if isinstance(raw.get("key"), str) else None,
                "name": raw.get("name") if isinstance(raw.get("name"), str) else None,
                "group": raw.get("group") if isinstance(raw.get("group"), str) else default_group,
                "description": raw.get("description") if isinstance(raw.get("description"), str) else None,
            }
        )
        explicit[bit] = BitmapItem(
            bit=bit,
            key=raw.get("key") if isinstance(raw.get("key"), str) else None,
            name=raw.get("name") if isinstance(raw.get("name"), str) else None,
            group=raw.get("group") if isinstance(raw.get("group"), str) else default_group,
            description=raw.get("description") if isinstance(raw.get("description"), str) else None,
            source="item",
        )

    ranged: dict[int, BitmapItem] = {}
    document_ranges: list[dict[str, Any]] = []
    ranges_in = doc.get("ranges") if isinstance(doc.get("ranges"), list) else []
    for raw in ranges_in:
        if not isinstance(raw, dict):
            logger.warning("bitmaps range ignored (not object): %r", raw)
            continue
        f = _as_int(raw.get("from"))
        t = _as_int(raw.get("to"))
        if f is None or t is None:
            logger.warning("bitmaps range ignored (invalid from/to): %r", raw)
            continue
        if f > t:
            logger.warning("bitmaps range ignored (from>to): from=%d to=%d", f, t)
            continue
        if not _bit_ok(f) or not _bit_ok(t):
            logger.warning("bitmaps range ignored (out of range 0..4095): from=%d to=%d", f, t)
            continue

        name_prefix = raw.get("name_prefix") if isinstance(raw.get("name_prefix"), str) else ""
        group = raw.get("group") if isinstance(raw.get("group"), str) else default_group
        description = raw.get("description") if isinstance(raw.get("description"), str) else None
        fmt = raw.get("format") if isinstance(raw.get("format"), str) else None

        document_range: dict[str, Any] = {"from": f, "to": t}
        if group is not None:
            document_range["group"] = group
        if name_prefix:
            document_range["name_prefix"] = name_prefix
        if fmt:
            document_range["format"] = fmt
        if description:
            document_range["description"] = description
        document_ranges.append(document_range)

        for bit in range(f, t + 1):
            if bit in explicit or bit in ranged:
                continue
            gen_key = f"{name_prefix}{bit}" if name_prefix else None
            gen_name = f"{name_prefix}{bit}" if name_prefix else None
            ranged[bit] = BitmapItem(
                bit=bit,
                key=gen_key,
                name=gen_name,
                group=group,
                description=description,
                source="range",
            )

    merged: dict[int, BitmapItem] = {}
    merged.update(ranged)
    merged.update(explicit)  # explicit wins

    items_out: list[dict[str, Any]] = []
    for bit in sorted(merged.keys()):
        it = merged[bit]
        items_out.append(
            {
                "bit": it.bit,
                "key": it.key,
                "name": it.name,
                "group": it.group,
                "description": it.description,
                "source": it.source,
            }
        )

    meta_out = dict(meta)
    meta_out.setdefault("preset", preset)
    meta_out.setdefault("ns", ns2)
    meta_out.setdefault("path", str(read_path))
    if legacy_used:
        meta_out.setdefault("legacy", True)

    return {
        "schema": BITMAPS_SCHEMA_V1,
        "meta": meta_out,
        "groups": groups,
        "defaults": defaults,
        "count": len(items_out),
        "items": items_out,
        "document": {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": dict(meta),
            "groups": dict(groups),
            "labels": dict(labels),
            "defaults": dict(defaults),
            "items": document_items,
            "ranges": document_ranges,
        },
    }


def save_bitmaps_to_preset(*, presets_dir: str, preset: str, ns: str, logger: Any, document: Any) -> None:
    if not isinstance(document, dict):
        raise ApiError("INVALID_BITMAPS", "bitmaps document must be an object", status_code=422)

    schema = document.get("schema")
    if schema != BITMAPS_SCHEMA_V1:
        raise ApiError(
            "INVALID_SCHEMA",
            "bitmaps schema mismatch",
            status_code=422,
            details={"expected": BITMAPS_SCHEMA_V1, "found": schema},
        )

    meta = document.get("meta") if isinstance(document.get("meta"), dict) else {}
    groups_in = document.get("groups") if isinstance(document.get("groups"), dict) else {}
    labels = document.get("labels") if isinstance(document.get("labels"), dict) else {}
    defaults_in = document.get("defaults") if isinstance(document.get("defaults"), dict) else {}

    groups: dict[str, dict[str, Any]] = {}
    for gid, graw in groups_in.items():
        if not isinstance(gid, str):
            continue
        gid2 = gid.strip()
        if not gid2:
            continue
        if not isinstance(graw, dict):
            continue
        g: dict[str, Any] = {}
        if isinstance(graw.get("label"), str):
            g["label"] = graw["label"]
        if isinstance(graw.get("order"), (int, float)) and float(graw["order"]).is_integer():
            g["order"] = int(graw["order"])
        if isinstance(graw.get("color"), str) and graw.get("color").strip():
            g["color"] = graw["color"].strip()
        groups[gid2] = g

    default_group = defaults_in.get("group") if isinstance(defaults_in.get("group"), str) else None
    if default_group is not None and default_group not in groups:
        raise ApiError(
            "INVALID_DEFAULT_GROUP",
            "defaults.group must reference an existing group",
            status_code=422,
            details={"defaults_group": default_group, "known_groups": sorted(groups.keys())},
        )

    def _norm_group(raw_group: Any) -> str | None:
        g = raw_group if isinstance(raw_group, str) else None
        if g is None or not g.strip():
            g = default_group
        if g is None:
            return None
        g = g.strip()
        if g not in groups:
            raise ApiError(
                "INVALID_GROUP",
                "group must reference an existing group",
                status_code=422,
                details={"group": g, "known_groups": sorted(groups.keys())},
            )
        return g

    items_out: list[dict[str, Any]] = []
    seen_bits: set[int] = set()
    items_in = document.get("items") if isinstance(document.get("items"), list) else []
    for raw in items_in:
        if not isinstance(raw, dict):
            logger.warning("bitmaps item ignored on save (not object): %r", raw)
            continue
        bit = _as_int(raw.get("bit"))
        if bit is None or not _bit_ok(bit):
            logger.warning("bitmaps item ignored on save (invalid bit): %r", raw.get("bit"))
            continue
        if bit in seen_bits:
            logger.warning("bitmaps item ignored on save (duplicate bit=%d)", bit)
            continue
        seen_bits.add(bit)
        item: dict[str, Any] = {"bit": bit}
        if isinstance(raw.get("key"), str) and raw.get("key").strip():
            item["key"] = raw.get("key").strip()
        if isinstance(raw.get("name"), str) and raw.get("name").strip():
            item["name"] = raw.get("name").strip()
        if isinstance(raw.get("description"), str) and raw.get("description").strip():
            item["description"] = raw.get("description").strip()
        group = _norm_group(raw.get("group"))
        if group is not None:
            item["group"] = group
        items_out.append(item)

    ranges_out: list[dict[str, Any]] = []
    ranges_in = document.get("ranges") if isinstance(document.get("ranges"), list) else []
    for raw in ranges_in:
        if not isinstance(raw, dict):
            logger.warning("bitmaps range ignored on save (not object): %r", raw)
            continue
        f = _as_int(raw.get("from"))
        t = _as_int(raw.get("to"))
        if f is None or t is None or f > t or not _bit_ok(f) or not _bit_ok(t):
            logger.warning("bitmaps range ignored on save (invalid from/to): %r", raw)
            continue
        rng: dict[str, Any] = {"from": f, "to": t}
        group = _norm_group(raw.get("group"))
        if group is not None:
            rng["group"] = group
        if isinstance(raw.get("name_prefix"), str) and raw.get("name_prefix").strip():
            rng["name_prefix"] = raw.get("name_prefix").strip()
        if isinstance(raw.get("format"), str) and raw.get("format").strip():
            rng["format"] = raw.get("format").strip()
        if isinstance(raw.get("description"), str) and raw.get("description").strip():
            rng["description"] = raw.get("description").strip()
        ranges_out.append(rng)

    defaults: dict[str, Any] = {}
    for k in ["group", "format", "missing_name_format"]:
        if isinstance(defaults_in.get(k), str) and defaults_in.get(k).strip():
            defaults[k] = defaults_in.get(k).strip()

    doc_out: dict[str, Any] = {
        "schema": BITMAPS_SCHEMA_V1,
        "meta": meta,
        "groups": groups,
        "labels": labels,
        "defaults": defaults,
        "items": items_out,
        "ranges": ranges_out,
    }

    path = _bitmaps_path(presets_dir=presets_dir, preset=preset, ns=_sanitize_ns(ns))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(doc_out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
    except ApiError:
        raise
    except Exception as e:
        raise ApiError(
            "BITMAPS_WRITE_FAILED",
            "failed to write bitmaps.json",
            status_code=500,
            details={"error": str(e), "path": str(path)},
        )

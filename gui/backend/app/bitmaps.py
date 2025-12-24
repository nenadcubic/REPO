from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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


def load_bitmaps_from_preset(
    *, presets_dir: str, preset: str, logger: Any
) -> dict[str, Any]:
    path = Path(presets_dir) / preset / "bitmaps.json"
    if not path.exists():
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "missing": True},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
        }

    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("bitmaps.json parse failed: %s (path=%s)", e, str(path))
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "path": str(path), "invalid_json": True},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
        }

    if not isinstance(doc, dict):
        logger.warning("bitmaps.json root must be object (path=%s)", str(path))
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "path": str(path), "invalid_root": True},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
        }

    schema = doc.get("schema")
    if schema != BITMAPS_SCHEMA_V1:
        logger.warning("bitmaps.json schema mismatch: %s (path=%s)", schema, str(path))
        return {
            "schema": BITMAPS_SCHEMA_V1,
            "meta": {"preset": preset, "path": str(path), "invalid_schema": True, "found_schema": schema},
            "groups": {},
            "defaults": {},
            "count": 0,
            "items": [],
        }

    meta = doc.get("meta") if isinstance(doc.get("meta"), dict) else {}
    groups = doc.get("groups") if isinstance(doc.get("groups"), dict) else {}
    defaults = doc.get("defaults") if isinstance(doc.get("defaults"), dict) else {}

    default_group = defaults.get("group") if isinstance(defaults.get("group"), str) else None

    explicit: dict[int, BitmapItem] = {}
    items_in = doc.get("items") if isinstance(doc.get("items"), list) else []
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
        explicit[bit] = BitmapItem(
            bit=bit,
            key=raw.get("key") if isinstance(raw.get("key"), str) else None,
            name=raw.get("name") if isinstance(raw.get("name"), str) else None,
            group=raw.get("group") if isinstance(raw.get("group"), str) else default_group,
            description=raw.get("description") if isinstance(raw.get("description"), str) else None,
            source="item",
        )

    ranged: dict[int, BitmapItem] = {}
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
    meta_out.setdefault("path", str(path))

    return {
        "schema": BITMAPS_SCHEMA_V1,
        "meta": meta_out,
        "groups": groups,
        "defaults": defaults,
        "count": len(items_out),
        "items": items_out,
    }


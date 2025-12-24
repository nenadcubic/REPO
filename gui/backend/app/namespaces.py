from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


NAMESPACES_SCHEMA_V1 = "er.gui.namespaces.v1"


@dataclass(frozen=True)
class NamespaceEntry:
    id: str
    label: str
    prefix: str
    layout: str


def _norm_id(v: Any) -> str | None:
    if not isinstance(v, str):
        return None
    s = v.strip()
    if not s:
        return None
    return s


def _norm_prefix(v: Any) -> str | None:
    if not isinstance(v, str):
        return None
    s = v.strip()
    if not s:
        return None
    return s.strip(":")


def load_namespaces_from_preset(*, presets_dir: str, preset: str, logger: Any) -> dict[str, Any]:
    path = Path(presets_dir) / preset / "namespaces.json"
    if not path.exists():
        return {
            "schema": NAMESPACES_SCHEMA_V1,
            "default": "er",
            "namespaces": [{"id": "er", "label": "Element-Redis", "prefix": "er", "layout": "er_layout_v1"}],
            "meta": {"preset": preset, "missing": True},
        }

    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("namespaces.json parse failed: %s (path=%s)", e, str(path))
        return {
            "schema": NAMESPACES_SCHEMA_V1,
            "default": "er",
            "namespaces": [{"id": "er", "label": "Element-Redis", "prefix": "er", "layout": "er_layout_v1"}],
            "meta": {"preset": preset, "path": str(path), "invalid_json": True},
        }

    if not isinstance(doc, dict) or doc.get("schema") != NAMESPACES_SCHEMA_V1:
        logger.warning("namespaces.json schema mismatch (path=%s)", str(path))
        return {
            "schema": NAMESPACES_SCHEMA_V1,
            "default": "er",
            "namespaces": [{"id": "er", "label": "Element-Redis", "prefix": "er", "layout": "er_layout_v1"}],
            "meta": {"preset": preset, "path": str(path), "invalid_schema": True},
        }

    raw_default = _norm_id(doc.get("default")) or "er"
    raw_list = doc.get("namespaces") if isinstance(doc.get("namespaces"), list) else []
    raw_layouts = doc.get("layouts") if isinstance(doc.get("layouts"), dict) else {}

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_list:
        if not isinstance(raw, dict):
            continue
        ns_id = _norm_id(raw.get("id"))
        if not ns_id or ns_id in seen:
            continue
        prefix = _norm_prefix(raw.get("prefix"))
        if not prefix:
            continue
        label = raw.get("label") if isinstance(raw.get("label"), str) else ns_id
        layout = raw.get("layout") if isinstance(raw.get("layout"), str) and raw.get("layout").strip() else "er_layout_v1"
        out.append({"id": ns_id, "label": label, "prefix": prefix, "layout": layout})
        seen.add(ns_id)

    if not out:
        out = [{"id": "er", "label": "Element-Redis", "prefix": "er", "layout": "er_layout_v1"}]
        raw_default = "er"

    if raw_default not in {x["id"] for x in out}:
        raw_default = out[0]["id"]

    return {
        "schema": NAMESPACES_SCHEMA_V1,
        "default": raw_default,
        "namespaces": out,
        "layouts": raw_layouts,
        "meta": {"preset": preset, "path": str(path)},
    }


def namespaces_to_map(namespaces_doc: dict[str, Any]) -> tuple[str, dict[str, NamespaceEntry]]:
    default_id = namespaces_doc.get("default") if isinstance(namespaces_doc.get("default"), str) else "er"
    out: dict[str, NamespaceEntry] = {}
    for raw in namespaces_doc.get("namespaces", []):
        if not isinstance(raw, dict):
            continue
        ns_id = _norm_id(raw.get("id"))
        prefix = _norm_prefix(raw.get("prefix"))
        if not ns_id or not prefix:
            continue
        label = raw.get("label") if isinstance(raw.get("label"), str) else ns_id
        layout = raw.get("layout") if isinstance(raw.get("layout"), str) and raw.get("layout").strip() else "er_layout_v1"
        out[ns_id] = NamespaceEntry(id=ns_id, label=label, prefix=prefix, layout=layout)
    if default_id not in out and out:
        default_id = sorted(out.keys())[0]
    return default_id, out


def resolve_layout(namespaces_doc: dict[str, Any], layout_id: str) -> dict[str, Any] | None:
    layouts = namespaces_doc.get("layouts")
    if not isinstance(layouts, dict):
        return None
    raw = layouts.get(layout_id)
    if not isinstance(raw, dict):
        return None
    return raw

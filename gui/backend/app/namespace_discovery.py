from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redis

from .errors import ApiError


@dataclass(frozen=True)
class DiscoveryLimits:
    max_keys: int = 50000
    sample_per_prefix: int = 200
    scan_count: int = 1000


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _decode_key(k: Any) -> str:
    if isinstance(k, bytes):
        return k.decode("utf-8", errors="replace")
    return str(k)


def _decode_type(t: Any) -> str:
    if isinstance(t, bytes):
        return t.decode("utf-8", errors="replace")
    return str(t)


def _prefix_of(key: str) -> str:
    if ":" not in key:
        return key
    return key.split(":", 1)[0]


def _infer_for_prefix(*, prefix: str, keys: list[str], r: redis.Redis) -> dict[str, Any]:
    evidence_counts: dict[str, Any] = {
        "total_keys": len(keys),
        "types": {"string": 0, "hash": 0, "set": 0, "zset": 0, "list": 0, "none": 0, "other": 0},
        "patterns": {"element": 0, "idx_bit": 0, "tmp": 0, "universe": 0},
    }
    samples: dict[str, Any] = {"keys": [], "by_class": {"element": [], "idx_bit": [], "tmp": [], "universe": []}}

    pipe = r.pipeline()
    for k in keys:
        pipe.type(k)
    types_raw = pipe.execute()

    for k, t_raw in zip(keys, types_raw):
        t = _decode_type(t_raw)
        if t in evidence_counts["types"]:
            evidence_counts["types"][t] += 1
        elif t == "none":
            evidence_counts["types"]["none"] += 1
        else:
            evidence_counts["types"]["other"] += 1

        cls = None
        if k == f"{prefix}:all":
            cls = "universe"
            evidence_counts["patterns"]["universe"] += 1
        elif k.startswith(f"{prefix}:element:"):
            cls = "element"
            evidence_counts["patterns"]["element"] += 1
        elif k.startswith(f"{prefix}:idx:bit:"):
            cls = "idx_bit"
            evidence_counts["patterns"]["idx_bit"] += 1
        elif k.startswith(f"{prefix}:tmp:"):
            cls = "tmp"
            evidence_counts["patterns"]["tmp"] += 1

        if cls:
            bucket = samples["by_class"][cls]
            if len(bucket) < 20:
                bucket.append({"key": k, "type": t})
        if len(samples["keys"]) < 20:
            samples["keys"].append({"key": k, "type": t})

    confidence = 0.0
    if evidence_counts["patterns"]["element"] > 0 and evidence_counts["types"]["hash"] > 0:
        confidence += 0.45
    if evidence_counts["patterns"]["idx_bit"] > 0 and evidence_counts["types"]["set"] > 0:
        confidence += 0.30
    if evidence_counts["patterns"]["universe"] > 0 and evidence_counts["types"]["set"] > 0:
        confidence += 0.15
    if evidence_counts["patterns"]["tmp"] > 0 and evidence_counts["types"]["set"] > 0:
        confidence += 0.10
    confidence = float(min(1.0, max(0.0, confidence)))

    suggested_layout: dict[str, Any] = {"status": "unknown"}
    if confidence >= 0.50:
        suggested_layout = {
            "status": "candidate",
            "prefix": prefix,
            "key_templates": {
                "element": f"{prefix}:element:{{name}}",
                "idx_bit": f"{prefix}:idx:bit:{{bit}}",
                "universe": f"{prefix}:all",
                "tmp_store": f"{prefix}:tmp:{{tag}}:{{ns}}",
            },
            "delete_policy": {"tmp_only": True} if confidence >= 0.70 else {"tmp_only": False, "reason": "low_confidence"},
        }

    return {
        "prefix": prefix,
        "confidence": round(confidence, 3),
        "evidence": {"counts": evidence_counts, "samples": samples},
        "suggested_layout": suggested_layout,
    }


def discover_namespaces(
    *,
    r: redis.Redis,
    limits: DiscoveryLimits,
) -> dict[str, Any]:
    max_keys = int(limits.max_keys)
    sample_per_prefix = int(limits.sample_per_prefix)
    scan_count = int(limits.scan_count)
    if max_keys <= 0 or max_keys > 500000:
        raise ApiError("INVALID_LIMIT", "max_keys must be 1..500000", status_code=422)
    if sample_per_prefix <= 0 or sample_per_prefix > 2000:
        raise ApiError("INVALID_LIMIT", "sample_per_prefix must be 1..2000", status_code=422)
    if scan_count <= 0 or scan_count > 10000:
        raise ApiError("INVALID_LIMIT", "scan_count must be 1..10000", status_code=422)

    prefixes: dict[str, dict[str, Any]] = {}
    cursor = 0
    seen = 0
    while True:
        cursor, batch = r.scan(cursor=cursor, count=scan_count)
        for raw in batch:
            key = _decode_key(raw)
            seen += 1
            pfx = _prefix_of(key)
            entry = prefixes.setdefault(pfx, {"count": 0, "samples": []})
            entry["count"] += 1
            if len(entry["samples"]) < sample_per_prefix:
                entry["samples"].append(key)
            if seen >= max_keys:
                break
        if cursor == 0 or seen >= max_keys:
            break

    inferred: list[dict[str, Any]] = []
    for pfx, info in prefixes.items():
        inferred.append(_infer_for_prefix(prefix=pfx, keys=info["samples"], r=r))

    inferred.sort(key=lambda x: (-float(x.get("confidence") or 0.0), str(x.get("prefix") or "")))
    return {
        "schema": "er.gui.namespaces.discover.v1",
        "generated_utc": _now_utc(),
        "limits": {"max_keys": max_keys, "sample_per_prefix": sample_per_prefix, "scan_count": scan_count},
        "seen_keys": seen,
        "prefixes": inferred,
    }


def write_namespaces_generated(
    *, presets_dir: str, preset: str, discovery: dict[str, Any], logger: Any
) -> dict[str, Any]:
    prefixes = discovery.get("prefixes") if isinstance(discovery.get("prefixes"), list) else []
    ns_entries = []
    for p in prefixes:
        if not isinstance(p, dict):
            continue
        prefix = p.get("prefix")
        if not isinstance(prefix, str) or not prefix:
            continue
        ns_entries.append({"id": prefix, "label": prefix, "prefix": prefix})

    default_ns = ns_entries[0]["id"] if ns_entries else "er"
    out = {
        "schema": "er.gui.namespaces.v1",
        "default": default_ns,
        "namespaces": ns_entries,
        "meta": {
            "generated_utc": discovery.get("generated_utc"),
            "source": "discover",
            "limits": discovery.get("limits"),
            "seen_keys": discovery.get("seen_keys"),
        },
    }

    path = Path(presets_dir) / preset / "namespaces.generated.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    try:
        tmp.write_text(__import__("json").dumps(out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        tmp.replace(path)
    except Exception as e:
        logger.warning("namespaces.generated.json write failed: %s (path=%s)", e, str(path))
        raise ApiError("WRITE_FAILED", "failed to write namespaces.generated.json", status_code=500, details={"path": str(path)})

    return {"ok": True, "path": str(path), "document": out}


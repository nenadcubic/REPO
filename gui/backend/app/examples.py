from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import redis

from .cli_adapter import er_cli_put
from .errors import ApiError


@dataclass(frozen=True)
class ExampleElement:
    name: str
    bits: list[int]


@dataclass(frozen=True)
class ExampleDef:
    id: str
    title: str
    description: str
    ns_hint: str
    elements: list[ExampleElement]
    queries: list[dict[str, Any]] | None = None


def list_examples() -> list[ExampleDef]:
    return [
        ExampleDef(
            id="basic_seed",
            title="Basic seed",
            description="Small deterministic dataset for quick manual testing of Elements, Queries, Store+TTL, and Matrix.",
            ns_hint="er",
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
            id="math_universe_mini",
            title="Math universe (mini)",
            description="Mini version inspired by /examples/math_universe (no scripts executed).",
            ns_hint="er",
            elements=[
                ExampleElement("U", [0, 1, 2, 3, 4, 5]),
                ExampleElement("A", [0, 2, 4]),
                ExampleElement("B", [1, 2, 3]),
                ExampleElement("C", [3, 5]),
            ],
        ),
        ExampleDef(
            id="northwind_mini",
            title="Northwind (mini)",
            description="Mini dataset inspired by /examples/northwind (no DB/scripts; names are illustrative).",
            ns_hint="er",
            elements=[
                ExampleElement("cust:ALFKI", [0, 10, 20]),
                ExampleElement("cust:ANATR", [0, 11, 21]),
                ExampleElement("order:10248", [1, 10, 30]),
                ExampleElement("order:10249", [1, 11, 31]),
            ],
        ),
    ]


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
    reset: bool,
    r: redis.Redis,
    er_cli_path: str,
    redis_host: str,
    redis_port: int,
    logger: Any,
) -> dict[str, Any]:
    ex = next((e for e in list_examples() if e.id == example_id), None)
    if not ex:
        raise ApiError("INVALID_INPUT", "unknown example id", status_code=422, details={"id": example_id})

    reset_info = None
    if reset:
        reset_info = reset_namespace(r=r, prefix=prefix)

    created: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []

    seen_names: set[str] = set()
    for el in ex.elements:
        name = (el.name or "").strip()
        if not name or len(name) > 100:
            skipped.append(name or "<empty>")
            continue
        if name in seen_names:
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
            updated.append(name)
        else:
            created.append(name)

    logger.info("examples run id=%s ns=%s created=%d updated=%d skipped=%d", example_id, ns, len(created), len(updated), len(skipped))

    data: dict[str, Any] = {
        "id": example_id,
        "ns": ns,
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "counts": {"created": len(created), "updated": len(updated), "skipped": len(skipped)},
    }
    if reset_info is not None:
        data["reset"] = reset_info
    return data


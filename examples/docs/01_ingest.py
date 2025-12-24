#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
from typing import Dict, List, Tuple


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def require_cmd(name: str) -> None:
    p = subprocess.run(["bash", "-lc", f"command -v {name} >/dev/null 2>&1"], check=False)
    if p.returncode != 0:
        raise SystemExit(f"Missing required command: {name}")


def redis_cli(host: str, port: int, argv: List[str]) -> None:
    p = subprocess.run(
        ["redis-cli", "-h", host, "-p", str(port), *argv],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0 or re.search(r"(\\(error\\))|(-ERR)|(-WRONGTYPE)|(-NOAUTH)|(-READONLY)|(EXECABORT)", out, re.I):
        raise SystemExit(f"redis-cli failed: {' '.join(argv)}\n{out}")


DOCS: List[Tuple[str, str]] = [
    ("d1", "redis atomic store ttl multi exec"),
    ("d2", "redis lua store ttl atomic"),
    ("d3", "sqlite sql join where orderdate"),
    ("d4", "redis sets union inter diff presjek"),
    ("d5", "lua script atomic expire"),
    ("d6", "northwind orders customers germany"),
]


def tokenize(text: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if t]


def main() -> int:
    host = env_str("DOCS_REDIS_HOST", "localhost")
    port = env_int("DOCS_REDIS_PORT", 6379)
    prefix = env_str("DOCS_PREFIX", "docs").rstrip(":")
    if not prefix or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]*", prefix):
        raise SystemExit(f"Unsafe DOCS_PREFIX: {prefix!r}")

    require_cmd("redis-cli")
    redis_cli(host, port, ["PING"])

    all_key = f"{prefix}:all"
    term_map: Dict[str, List[str]] = {}

    for doc_id, text in DOCS:
        redis_cli(host, port, ["SADD", all_key, doc_id])
        for term in set(tokenize(text)):
            term_map.setdefault(term, []).append(doc_id)

    for term, ids in term_map.items():
        redis_cli(host, port, ["SADD", f"{prefix}:term:{term}", *sorted(ids)])

    print("OK: ingested docs → Redis")
    print(f"Redis: {host}:{port}")
    print(f"Prefix: {prefix}:")
    print(f"Docs: {len(DOCS)} (key: {all_key})")
    print(f"Terms: {len(term_map)} (key pattern: {prefix}:term:<term>)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import subprocess
from typing import List


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


def is_prime(n: int) -> bool:
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    d = 3
    while d * d <= n:
        if n % d == 0:
            return False
        d += 2
    return True


def main() -> int:
    host = env_str("MU_REDIS_HOST", "localhost")
    port = env_int("MU_REDIS_PORT", 6379)
    prefix = env_str("MU_PREFIX", "mu").rstrip(":")
    max_n = env_int("MU_MAX_N", 100)
    if max_n < 1:
        raise SystemExit("MU_MAX_N must be >= 1")
    if not prefix or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]*", prefix):
        raise SystemExit(f"Unsafe MU_PREFIX: {prefix!r}")

    require_cmd("redis-cli")
    redis_cli(host, port, ["PING"])

    k_all = f"{prefix}:all"
    k_even = f"{prefix}:idx:even"
    k_odd = f"{prefix}:idx:odd"
    k_prime = f"{prefix}:idx:prime"
    k_mod3 = f"{prefix}:idx:mod3"
    k_gt50 = f"{prefix}:idx:gt50"

    for n in range(1, max_n + 1):
        s = str(n)
        redis_cli(host, port, ["SADD", k_all, s])
        redis_cli(host, port, ["SADD", k_even if (n % 2 == 0) else k_odd, s])
        if n % 3 == 0:
            redis_cli(host, port, ["SADD", k_mod3, s])
        if n > 50:
            redis_cli(host, port, ["SADD", k_gt50, s])
        if is_prime(n):
            redis_cli(host, port, ["SADD", k_prime, s])

    print("OK: ingested math universe → Redis")
    print(f"Redis: {host}:{port}")
    print(f"Prefix: {prefix}:")
    print(f"Universe: 1..{max_n} (key: {k_all})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


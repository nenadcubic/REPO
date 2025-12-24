from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from typing import Sequence

from .errors import ApiError


@dataclass(frozen=True)
class CliResult:
    stdout: str
    stderr: str


def _run_er_cli(
    *,
    er_cli_path: str,
    args: Sequence[str],
    redis_host: str,
    redis_port: int,
    keys_only: bool = False,
    timeout_sec: int = 10,
) -> CliResult:
    env = os.environ.copy()
    env["ER_REDIS_HOST"] = redis_host
    env["ER_REDIS_PORT"] = str(redis_port)
    if keys_only:
        env["ER_KEYS_ONLY"] = "1"

    try:
        proc = subprocess.run(
            [er_cli_path, *args],
            check=False,
            capture_output=True,
            text=True,
            env=env,
            timeout=timeout_sec,
        )
    except FileNotFoundError as e:
        raise ApiError("ER_CLI_MISSING", f"er_cli not found at {er_cli_path}", status_code=500) from e
    except subprocess.TimeoutExpired as e:
        raise ApiError("ER_CLI_TIMEOUT", "er_cli timed out", status_code=504) from e

    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        if not msg:
            msg = f"er_cli failed with exit code {proc.returncode}"
        raise ApiError("ER_CLI_ERROR", msg, status_code=502, details={"exit_code": proc.returncode})

    return CliResult(stdout=proc.stdout, stderr=proc.stderr)


def _parse_er_cli_members(output: str) -> list[str]:
    names: list[str] = []
    for line in output.splitlines():
        line = line.rstrip("\n")
        if line.startswith(" - "):
            names.append(line[3:])
    return names


def _parse_er_cli_count(output: str) -> int | None:
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("Count:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except Exception:
                return None
    return None


def er_cli_put(*, er_cli_path: str, redis_host: str, redis_port: int, name: str, bits: list[int]) -> None:
    _run_er_cli(
        er_cli_path=er_cli_path,
        args=["put", name, *[str(b) for b in bits]],
        redis_host=redis_host,
        redis_port=redis_port,
    )


def er_cli_query(*, er_cli_path: str, redis_host: str, redis_port: int, args: Sequence[str]) -> list[str]:
    res = _run_er_cli(
        er_cli_path=er_cli_path,
        args=args,
        redis_host=redis_host,
        redis_port=redis_port,
    )
    return _parse_er_cli_members(res.stdout)


def er_cli_query_with_count(
    *, er_cli_path: str, redis_host: str, redis_port: int, args: Sequence[str]
) -> tuple[int | None, list[str]]:
    res = _run_er_cli(
        er_cli_path=er_cli_path,
        args=args,
        redis_host=redis_host,
        redis_port=redis_port,
    )
    return _parse_er_cli_count(res.stdout), _parse_er_cli_members(res.stdout)


def er_cli_store_key(*, er_cli_path: str, redis_host: str, redis_port: int, args: Sequence[str]) -> str:
    res = _run_er_cli(
        er_cli_path=er_cli_path,
        args=args,
        redis_host=redis_host,
        redis_port=redis_port,
        keys_only=True,
    )
    store_key = res.stdout.strip()
    if not store_key:
        raise ApiError("ER_CLI_ERROR", "er_cli returned empty store key", status_code=502)
    return store_key

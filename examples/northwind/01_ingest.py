#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import shutil
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def find_table(conn: sqlite3.Connection, candidates: List[str]) -> Optional[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    by_lower = {r[0].lower(): r[0] for r in rows}
    for c in candidates:
        hit = by_lower.get(c.lower())
        if hit:
            return hit
    return None


def load_schema_bits(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _require_cmd(name: str) -> None:
    if shutil.which(name) is None:
        raise SystemExit(f"Missing required command: {name}")


def _encode_redis_cmd(argv: List[str]) -> bytes:
    # RESP (Redis Serialization Protocol), used by `redis-cli --pipe`.
    out = [f"*{len(argv)}\r\n".encode("utf-8")]
    for a in argv:
        b = a.encode("utf-8")
        out.append(f"${len(b)}\r\n".encode("utf-8"))
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


def _redis_cli(host: str, port: int, argv: List[str]) -> str:
    p = subprocess.run(
        ["redis-cli", "-h", host, "-p", str(port), *argv],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if p.returncode != 0:
        raise SystemExit(f"redis-cli failed: {' '.join(argv)}\n{p.stderr}{p.stdout}")
    return p.stdout


def _redis_pipe(host: str, port: int, commands: List[List[str]]) -> None:
    payload = b"".join(_encode_redis_cmd(cmd) for cmd in commands)
    p = subprocess.run(
        ["redis-cli", "-h", host, "-p", str(port), "--pipe"],
        input=payload,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if p.returncode != 0:
        out = p.stdout.decode("utf-8", "replace")
        err = p.stderr.decode("utf-8", "replace")
        raise SystemExit(f"redis-cli --pipe failed\n{err}{out}")


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Ingest Northwind SQLite → Redis sets (nw:*)")
    ap.add_argument(
        "--db",
        default=env_str("NW_DB_PATH", str(here / "northwind.sqlite")),
        help="Path to northwind.sqlite (default: examples/northwind/northwind.sqlite)",
    )
    ap.add_argument(
        "--schema-bits",
        default=env_str("NW_SCHEMA_BITS", str(here / "schema_bits.json")),
        help="Path to schema_bits.json (default: examples/northwind/schema_bits.json)",
    )
    ap.add_argument(
        "--redis-host",
        default=env_str("NW_REDIS_HOST", "localhost"),
        help="Redis host (default: NW_REDIS_HOST or localhost)",
    )
    ap.add_argument(
        "--redis-port",
        type=int,
        default=env_int("NW_REDIS_PORT", 6379),
        help="Redis port (default: NW_REDIS_PORT or 6379)",
    )
    ap.add_argument(
        "--prefix",
        default=env_str("NW_PREFIX", "nw"),
        help="Redis key prefix (default: nw)",
    )
    ap.add_argument(
        "--reset",
        action="store_true",
        help="Delete existing keys under <prefix>:* before ingest (uses SCAN, not KEYS)",
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path} (run examples/northwind/00_get_db.sh)")

    schema_bits = load_schema_bits(Path(args.schema_bits))
    customers_country_bits: Dict[str, int] = (
        schema_bits.get("customers", {}).get("country", {}) if isinstance(schema_bits, dict) else {}
    )

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        customers_table = find_table(conn, ["Customers", "Customer", "customers", "customer"])
        orders_table = find_table(conn, ["Orders", "Order", "orders", "order"])
        order_details_table = find_table(
            conn,
            [
                "Order Details",
                "OrderDetails",
                "Order_Details",
                "order_details",
                "Order Detail",
                "OrderDetail",
                "orderdetail",
            ],
        )

        if not customers_table or not orders_table or not order_details_table:
            raise SystemExit(
                "Expected Northwind tables not found. Need Customers, Orders, and Order Details.\n"
                f"Found: Customers={customers_table}, Orders={orders_table}, OrderDetails={order_details_table}"
            )

        rows = conn.execute(f'SELECT CustomerID, Country FROM "{customers_table}"').fetchall()
        customers: List[Tuple[str, str]] = [(str(rw["CustomerID"]), str(rw["Country"] or "")) for rw in rows]

        order_rows = conn.execute(f'SELECT OrderID, CustomerID, OrderDate FROM "{orders_table}"').fetchall()
        orders: List[Tuple[str, str, Optional[str]]] = [
            (str(rw["OrderID"]), str(rw["CustomerID"]), (str(rw["OrderDate"]) if rw["OrderDate"] is not None else None))
            for rw in order_rows
        ]

        od_rows = conn.execute(f'SELECT OrderID, ProductID FROM "{order_details_table}"').fetchall()
        order_details: List[Tuple[str, str]] = [(str(rw["OrderID"]), str(rw["ProductID"])) for rw in od_rows]
    finally:
        conn.close()

    prefix = args.prefix.rstrip(":")
    reset_pattern: Optional[str] = None
    if args.reset:
        if not prefix:
            raise SystemExit("Refusing to --reset with empty prefix (set --prefix or NW_PREFIX)")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9:_-]*", prefix):
            raise SystemExit(
                f"Refusing to --reset with unsafe prefix: {prefix!r} (allowed: [A-Za-z0-9][A-Za-z0-9:_-]*)"
            )
        reset_pattern = f"{prefix}:*"
    k_customers_all = f"{prefix}:customers:all"
    k_orders_all = f"{prefix}:orders:all"
    customer_ids = [cid for cid, _ in customers]
    order_ids = [oid for oid, _, _ in orders]

    if redis is None:
        _require_cmd("redis-cli")
        pong = _redis_cli(args.redis_host, args.redis_port, ["PING"]).strip()
        if pong != "PONG":
            raise SystemExit(f"Redis PING failed: {pong!r}")

        if reset_pattern:
            keys = _redis_cli(args.redis_host, args.redis_port, ["--scan", "--pattern", reset_pattern]).splitlines()
            deleted = len(keys)
            for batch in chunked(keys, 1000):
                _redis_pipe(args.redis_host, args.redis_port, [["DEL", *batch]])
            print(f"Reset done: deleted {deleted} keys (match: {reset_pattern})")

        commands: List[List[str]] = []
        def flush() -> None:
            nonlocal commands
            if not commands:
                return
            _redis_pipe(args.redis_host, args.redis_port, commands)
            commands = []

        for ch in chunked(customer_ids, 1000):
            commands.append(["SADD", k_customers_all, *ch])
        for cid, country in customers:
            bit = customers_country_bits.get(country.strip())
            if bit is None:
                continue
            if not (0 <= int(bit) < 4096):
                raise SystemExit(f"Invalid bit for customers.country.{country}: {bit} (expected 0..4095)")
            commands.append(["SADD", f"{prefix}:idx:customers:bit:{int(bit)}", cid])

        for ch in chunked(order_ids, 1000):
            commands.append(["SADD", k_orders_all, *ch])

        for oid, cid, order_date in orders:
            commands.append(["SADD", f"{prefix}:orders:customer:{cid}", oid])
            if order_date:
                s = order_date[:10]
                parts = s.split("-")
                if len(parts) == 3:
                    try:
                        year = int(parts[0])
                        month = int(parts[1])
                    except ValueError:
                        year = 0
                        month = 0
                    if 1 <= month <= 12 and year:
                        quarter = (month - 1) // 3 + 1
                        commands.append(["SADD", f"{prefix}:idx:orders:year:{year}", oid])
                        commands.append(["SADD", f"{prefix}:idx:orders:quarter:Q{quarter}", oid])

        for oid, pid in order_details:
            commands.append(["SADD", f"{prefix}:order_items:order:{oid}", pid])
            commands.append(["SADD", f"{prefix}:orders:has_product:{pid}", oid])

        flush()
    else:
        r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
        r.ping()
        if reset_pattern:
            cursor = 0
            deleted = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=reset_pattern, count=1000)
                if keys:
                    dpipe = r.pipeline(transaction=False)
                    for k in keys:
                        dpipe.delete(k)
                    res = dpipe.execute()
                    deleted += sum(int(x) for x in res)
                if int(cursor) == 0:
                    break
            print(f"Reset done: deleted {deleted} keys (match: {reset_pattern})")
        pipe = r.pipeline(transaction=False)

        for ch in chunked(customer_ids, 1000):
            pipe.sadd(k_customers_all, *ch)

        for cid, country in customers:
            bit = customers_country_bits.get(country.strip())
            if bit is None:
                continue
            if not (0 <= int(bit) < 4096):
                raise SystemExit(f"Invalid bit for customers.country.{country}: {bit} (expected 0..4095)")
            pipe.sadd(f"{prefix}:idx:customers:bit:{int(bit)}", cid)

        for ch in chunked(order_ids, 1000):
            pipe.sadd(k_orders_all, *ch)
        for oid, cid, order_date in orders:
            pipe.sadd(f"{prefix}:orders:customer:{cid}", oid)
            if not order_date:
                continue
            # Northwind variants commonly use `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`; use the first 10 chars.
            s = order_date[:10]
            parts = s.split("-")
            if len(parts) != 3:
                continue
            try:
                year = int(parts[0])
                month = int(parts[1])
            except ValueError:
                continue
            if month < 1 or month > 12:
                continue
            quarter = (month - 1) // 3 + 1
            pipe.sadd(f"{prefix}:idx:orders:year:{year}", oid)
            pipe.sadd(f"{prefix}:idx:orders:quarter:Q{quarter}", oid)

        for oid, pid in order_details:
            pipe.sadd(f"{prefix}:order_items:order:{oid}", pid)
            pipe.sadd(f"{prefix}:orders:has_product:{pid}", oid)

        pipe.execute()

    print("OK: ingested Northwind → Redis")
    print(f"DB: {db_path}")
    print(f"Redis: {args.redis_host}:{args.redis_port}")
    print(f"Prefix: {prefix}:")
    print(f"Customers: {len(customers)} (key: {k_customers_all})")
    print(f"Orders: {len(orders)} (key: {k_orders_all})")
    print(f"OrderDetails: {len(order_details)} (key pattern: {prefix}:order_items:order:<OrderID>)")
    if customers_country_bits:
        print("Customer country bits:")
        for token, bit in sorted(customers_country_bits.items(), key=lambda kv: int(kv[1])):
            print(f" - bit {int(bit)} = {token} (key: {prefix}:idx:customers:bit:{int(bit)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

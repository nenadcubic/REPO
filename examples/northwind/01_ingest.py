#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None


REDIS_ERROR_RE = re.compile(
    r"(\(error\))|(-ERR)|(-WRONGTYPE)|(-NOAUTH)|(-READONLY)|(-MOVED)|(-ASK)|(EXECABORT)",
    re.I,
)


def env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v else default


def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def chunked(items: List[str], n: int) -> Iterator[List[str]]:
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


def parse_order_date(order_date: Optional[str]) -> Optional[Tuple[int, int]]:
    if not order_date:
        return None
    # Northwind variants commonly use `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`; use the first 10 chars.
    s = order_date[:10]
    parts = s.split("-")
    if len(parts) != 3:
        return None
    try:
        year = int(parts[0])
        month = int(parts[1])
    except ValueError:
        return None
    if month < 1 or month > 12:
        return None
    quarter = (month - 1) // 3 + 1
    return year, quarter


def _require_cmd(name: str) -> None:
    if shutil.which(name) is None:
        raise SystemExit(f"Missing required command: {name}")


def _encode_redis_cmd(argv: List[str]) -> bytes:
    out = [f"*{len(argv)}\r\n".encode("utf-8")]
    for a in argv:
        b = a.encode("utf-8")
        out.append(f"${len(b)}\r\n".encode("utf-8"))
        out.append(b)
        out.append(b"\r\n")
    return b"".join(out)


class RedisWriter:
    def ping(self) -> None:
        raise NotImplementedError

    def scan_iter(self, match: str, count: int = 1000) -> Iterator[str]:
        raise NotImplementedError

    def delete_keys(self, keys: List[str]) -> None:
        raise NotImplementedError

    def sadd(self, key: str, members: List[str]) -> None:
        raise NotImplementedError

    def flush(self) -> None:
        raise NotImplementedError


class RedisPyWriter(RedisWriter):
    def __init__(self, host: str, port: int) -> None:
        assert redis is not None
        self._r = redis.Redis(host=host, port=port, decode_responses=True)
        self._pipe = self._r.pipeline(transaction=False)
        self._queued = 0
        self._max_queued = 5000

    def ping(self) -> None:
        if not self._r.ping():
            raise SystemExit("Redis PING failed")

    def scan_iter(self, match: str, count: int = 1000) -> Iterator[str]:
        yield from self._r.scan_iter(match=match, count=count)

    def delete_keys(self, keys: List[str]) -> None:
        for k in keys:
            self._pipe.delete(k)
            self._queued += 1
        if self._queued >= self._max_queued:
            self.flush()

    def sadd(self, key: str, members: List[str]) -> None:
        for ch in chunked(members, 1000):
            self._pipe.sadd(key, *ch)
            self._queued += 1
            if self._queued >= self._max_queued:
                self.flush()

    def flush(self) -> None:
        if self._queued == 0:
            return
        self._pipe.execute()
        self._pipe = self._r.pipeline(transaction=False)
        self._queued = 0


class RedisCliWriter(RedisWriter):
    def __init__(self, host: str, port: int) -> None:
        _require_cmd("redis-cli")
        self._host = host
        self._port = port
        self._buf: List[List[str]] = []
        self._max_buf = 5000

    def _run(self, argv: List[str], raw: bool = False) -> str:
        cmd = ["redis-cli", "-h", self._host, "-p", str(self._port)]
        if raw:
            cmd.append("--raw")
        cmd.extend(argv)
        p = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        combined = (p.stdout or "") + (p.stderr or "")
        if p.returncode != 0 or REDIS_ERROR_RE.search(combined):
            raise SystemExit(f"redis-cli failed: {' '.join(argv)}\n{combined}")
        return p.stdout

    def ping(self) -> None:
        pong = self._run(["PING"]).strip()
        if pong != "PONG":
            raise SystemExit(f"Redis PING failed: {pong!r}")

    def scan_iter(self, match: str, count: int = 1000) -> Iterator[str]:
        cursor = "0"
        while True:
            out = self._run(["SCAN", cursor, "MATCH", match, "COUNT", str(count)], raw=True)
            lines = [ln for ln in out.splitlines() if ln != ""]
            if not lines:
                break
            cursor = lines[0].strip()
            for k in lines[1:]:
                if k:
                    yield k
            if cursor == "0":
                break

    def delete_keys(self, keys: List[str]) -> None:
        if not keys:
            return
        self._buf.append(["DEL", *keys])
        if len(self._buf) >= self._max_buf:
            self.flush()

    def sadd(self, key: str, members: List[str]) -> None:
        for ch in chunked(members, 1000):
            self._buf.append(["SADD", key, *ch])
            if len(self._buf) >= self._max_buf:
                self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        payload = b"".join(_encode_redis_cmd(cmd) for cmd in self._buf)
        p = subprocess.run(
            ["redis-cli", "-h", self._host, "-p", str(self._port), "--pipe"],
            input=payload,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        out = p.stdout.decode("utf-8", "replace")
        err = p.stderr.decode("utf-8", "replace")
        combined = out + err
        m = re.search(r"errors:\s*(\d+)", combined)
        if p.returncode != 0 or (m and int(m.group(1)) != 0) or REDIS_ERROR_RE.search(combined):
            raise SystemExit(f"redis-cli --pipe failed\n{combined}")
        self._buf = []


def make_writer(host: str, port: int) -> RedisWriter:
    if redis is None:
        return RedisCliWriter(host, port)
    return RedisPyWriter(host, port)


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

    schema_bits = load_schema_bits(Path(args.schema_bits))
    customers_country_bits: Dict[str, int] = (
        schema_bits.get("customers", {}).get("country", {}) if isinstance(schema_bits, dict) else {}
    )

    w = make_writer(args.redis_host, args.redis_port)
    w.ping()

    if reset_pattern:
        deleted = 0
        batch: List[str] = []
        for k in w.scan_iter(reset_pattern):
            batch.append(k)
            if len(batch) >= 1000:
                w.delete_keys(batch)
                deleted += len(batch)
                batch = []
        if batch:
            w.delete_keys(batch)
            deleted += len(batch)
        w.flush()
        print(f"Reset done: deleted {deleted} keys (match: {reset_pattern})")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    customers_count = 0
    orders_count = 0
    order_details_count = 0
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

        # --- Customers ---
        customer_ids: List[str] = []
        for rw in conn.execute(f'SELECT CustomerID, Country FROM "{customers_table}"'):
            cid = str(rw["CustomerID"])
            country = str(rw["Country"] or "")
            customer_ids.append(cid)
            customers_count += 1
            bit = customers_country_bits.get(country.strip())
            if bit is not None:
                if not (0 <= int(bit) < 4096):
                    raise SystemExit(f"Invalid bit for customers.country.{country}: {bit} (expected 0..4095)")
                w.sadd(f"{prefix}:idx:customers:bit:{int(bit)}", [cid])

        k_customers_all = f"{prefix}:customers:all"
        for ch in chunked(customer_ids, 1000):
            w.sadd(k_customers_all, ch)

        # --- Orders + time indexes ---
        k_orders_all = f"{prefix}:orders:all"
        orders_by_customer: Dict[str, List[str]] = {}
        orders_by_year: Dict[int, List[str]] = {}
        orders_by_quarter: Dict[int, List[str]] = {}
        order_ids: List[str] = []
        for rw in conn.execute(f'SELECT OrderID, CustomerID, OrderDate FROM "{orders_table}"'):
            oid = str(rw["OrderID"])
            cid = str(rw["CustomerID"])
            od = str(rw["OrderDate"]) if rw["OrderDate"] is not None else None
            order_ids.append(oid)
            orders_count += 1
            orders_by_customer.setdefault(cid, []).append(oid)
            parsed = parse_order_date(od)
            if parsed:
                year, quarter = parsed
                orders_by_year.setdefault(year, []).append(oid)
                orders_by_quarter.setdefault(quarter, []).append(oid)

        for ch in chunked(order_ids, 1000):
            w.sadd(k_orders_all, ch)
        for cid, oids in orders_by_customer.items():
            w.sadd(f"{prefix}:orders:customer:{cid}", oids)
        for year, oids in orders_by_year.items():
            w.sadd(f"{prefix}:idx:orders:year:{year}", oids)
        for quarter, oids in orders_by_quarter.items():
            w.sadd(f"{prefix}:idx:orders:quarter:Q{quarter}", oids)

        # --- Order details ---
        # Reduce command count by buffering a window and emitting grouped SADDs.
        window = 20000
        buf_order_items: Dict[str, set[str]] = {}
        buf_has_product: Dict[str, set[str]] = {}
        buf_n = 0

        def flush_details() -> None:
            nonlocal buf_order_items, buf_has_product, buf_n
            for oid, pids in buf_order_items.items():
                w.sadd(f"{prefix}:order_items:order:{oid}", sorted(pids))
            for pid, oids in buf_has_product.items():
                w.sadd(f"{prefix}:orders:has_product:{pid}", sorted(oids))
            w.flush()
            buf_order_items = {}
            buf_has_product = {}
            buf_n = 0

        for rw in conn.execute(f'SELECT OrderID, ProductID FROM "{order_details_table}"'):
            oid = str(rw["OrderID"])
            pid = str(rw["ProductID"])
            order_details_count += 1
            buf_order_items.setdefault(oid, set()).add(pid)
            buf_has_product.setdefault(pid, set()).add(oid)
            buf_n += 1
            if buf_n >= window:
                flush_details()
        if buf_n:
            flush_details()

    finally:
        conn.close()

    w.flush()

    print("OK: ingested Northwind → Redis")
    print(f"DB: {db_path}")
    print(f"Redis: {args.redis_host}:{args.redis_port}")
    print(f"Prefix: {prefix}:")
    print(f"Customers: {customers_count} (key: {prefix}:customers:all)")
    print(f"Orders: {orders_count} (key: {prefix}:orders:all)")
    print(f"OrderDetails: {order_details_count} (key pattern: {prefix}:order_items:order:<OrderID>)")
    if customers_country_bits:
        print("Customer country bits:")
        for token, bit in sorted(customers_country_bits.items(), key=lambda kv: int(kv[1])):
            print(f" - bit {int(bit)} = {token} (key: {prefix}:idx:customers:bit:{int(bit)})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

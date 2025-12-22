# Northwind → element-redis (minimal, no GUI)

This example ingests a Northwind SQLite DB into Redis using simple Redis `SET` indexes, in a namespaced way (`nw:*`), then compares a few SQL queries with equivalent set operations (union / intersection / difference).

## Files

- `00_get_db.sh` downloads (or instructs you to place) `northwind.sqlite`
- `01_ingest.py` ingests SQLite → Redis sets
- `02_query_compare.sh` runs SQL vs Redis set-ops and compares results
- `schema_bits.json` minimal “bit” mapping used for index sets
- `requirements.txt` Python deps for ingestion

## Setup

1) Get the DB:

- `./examples/northwind/00_get_db.sh`

2) Install Python deps (recommended in a venv):

- `python3 -m venv .venv && . .venv/bin/activate`
- `pip install -r examples/northwind/requirements.txt`

3) Start Redis locally (or point at an existing Redis):

- `export NW_REDIS_HOST=localhost`
- `export NW_REDIS_PORT=6379`

4) Ingest:

- `python3 examples/northwind/01_ingest.py`
- For iterative runs (clean `nw:*` first): `python3 examples/northwind/01_ingest.py --reset`

5) Compare queries:

- `./examples/northwind/02_query_compare.sh`

Optional env vars:
- `NW_PREFIX` (default `nw`)
- `NW_DB_PATH` (default `examples/northwind/northwind.sqlite`)
- `NW_DB_SHA256` (optional, pins `00_get_db.sh` download)
- `NW_TTL_SEC` (default `600`, for `nw:tmp:*`)
- `NW_PRODUCT_ID` (default `11`, for the “orders containing product” example)
- `NW_CLEAN_TMP=1` (optional, delete `nw:tmp:*` before compare)
- `NW_YEAR` (default `1997`, for “orders in year/quarter” examples)

## Atomicity note

This example uses Redis `MULTI/EXEC` to make “store + expire” atomic in `02_query_compare.sh`.
The core project implements the same guarantee via Lua; keep semantics aligned (don’t let the two drift).

## Redis keys created

All keys are prefixed with `nw:` (configurable via `NW_PREFIX`).

Base (no TTL):

- `nw:customers:all` (SET of `CustomerID`)
- `nw:orders:all` (SET of `OrderID`)
- `nw:orders:customer:<CustomerID>` (SET of `OrderID`)
- `nw:order_items:order:<OrderID>` (SET of `ProductID`)
- `nw:orders:has_product:<ProductID>` (SET of `OrderID`)
- `nw:idx:orders:year:<YYYY>` (SET of `OrderID`)
- `nw:idx:orders:quarter:<Q1|Q2|Q3|Q4>` (SET of `OrderID`)
- `nw:idx:customers:bit:<bit>` (SET of `CustomerID`) for bits defined in `schema_bits.json`

Derived (created by `02_query_compare.sh`, with TTL, default 600s):

- `nw:tmp:*` (intermediate result sets)

## Bits (what they mean)

`schema_bits.json` defines a minimal, fixed mapping of tokens → bit numbers, currently for customer country:

- `customers.country.Germany` → bit `0`
- `customers.country.France` → bit `1`
- `customers.country.UK` → bit `2`

In Redis, each bit is represented as a `SET`:

- `nw:idx:customers:bit:0` contains all customers in Germany, etc.

## SQL → set operations (step-by-step)

`02_query_compare.sh` demonstrates:

1) Customers in Germany

- SQL: `Customers WHERE Country='Germany'`
- Redis: use the “bit set” directly: `SMEMBERS nw:idx:customers:bit:<GermanyBit>`

2) Customers in Germany OR France

- SQL: `Customers WHERE Country IN ('Germany','France')`
- Redis: `SUNIONSTORE tmp deBitSet frBitSet` (+ `EXPIRE tmp 600`)

3) Customers NOT in Germany

- SQL: `Customers WHERE Country!='Germany'`
- Redis: `SDIFFSTORE tmp nw:customers:all deBitSet` (+ TTL)
  - Note: SQL needs `Country IS NULL OR Country!='Germany'` to match set-diff semantics when `Country` can be NULL.

4) Orders from German customers

- SQL: `Orders JOIN Customers WHERE Customers.Country='Germany'`
- Redis:
  - start from German customer IDs (`SMEMBERS deBitSet`)
  - map each to `nw:orders:customer:<CustomerID>`
  - `SUNIONSTORE tmp` over those per-customer order sets (+ TTL)

5) Orders from German customers that include ProductID=11

- SQL: `Orders JOIN Customers JOIN OrderDetails WHERE Country='Germany' AND ProductID=11`
- Redis:
  - use `nw:orders:has_product:11` (built during ingest)
  - `SINTERSTORE tmp germanOrders nw:orders:has_product:11` (+ TTL)

6) Orders in year 1997 / quarter Q1 / Q1 AND Germany

- SQL: `OrderDate` range predicates (ISO string compare) or `substr(OrderDate,1,4)='1997'`
- Redis:
  - base indexes: `nw:idx:orders:year:1997`, `nw:idx:orders:quarter:Q1`
  - `SINTERSTORE tmp yearSet quarterSet` (+ TTL)
  - `SINTERSTORE tmp tmp_german_orders yearSet quarterSet` (+ TTL)

## Notes

- The SQLite DB file is intentionally ignored: `examples/northwind/northwind.sqlite`.
- If you built `build/cli/er_cli`, `02_query_compare.sh` will use `er_cli show <key>` for nicer set dumps when available.

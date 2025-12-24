# Northwind Compare (SQLite vs Redis)

This example imports Northwind data from a local SQLite file into Redis using the **OR namespace layout** (per-table sets + per-row objects), then compares Redis-side metrics against SQLite.

Security notes:
- No shell execution.
- No arbitrary file access: the SQLite file must be under `examples/northwind_compare/assets/`.
- No `KEYS` wildcard deletes; reset uses an import registry set of exact object names.

## Prerequisite: provide the SQLite DB

Place the DB file at:
- `examples/northwind_compare/assets/northwind.sqlite`

If you already have it locally (often present as `examples/northwind/northwind.sqlite`), you can copy it:
- `cp examples/northwind/northwind.sqlite examples/northwind_compare/assets/northwind.sqlite`

## How to run in the GUI

1) Open the GUI: `http://localhost:18080`
2) Go to `Examples`
3) Select: `Northwind: Redis vs SQLite comparison (northwind_compare)`
4) Pick namespace: `or`
5) Click `Run Import` (optional: enable reset first)
6) Click `Compare`

## What to expect

- **Row counts** per table match between SQLite and Redis.
- **Order totals (sample)** shows a deterministic list of OrderIDs and totals; diffs should be `0.00` when rounding to 2 decimals.

## OR naming rules (this example)

- `table` is a stable token like `Customers`, `Orders`, `OrderDetails`, ...
- `id` is the table primary key (composite keys are joined with `:` in PK order).
- `object_name` is `"{table}:{id}"` (stored in per-table sets)
- `object_key` is `"{pfx}:obj:{table}:{id}"` (hash with row fields)

If you see a mismatch:
- Ensure you imported into `or` (OR layout is required for this example).
- Ensure the SQLite DB is the expected Northwind variant (must contain `Customers`, `Orders`, and `Order Details`).
- Re-run with `Reset` enabled, then `Run Import`, then `Compare`.

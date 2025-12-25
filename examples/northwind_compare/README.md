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

## Schema metadata (bitset Elements)

In addition to importing OR objects, this example also ingests SQLite schema metadata as **4096-bit Element hashes** (profile: `northwind_meta_v0`) under the same `or` prefix.

Element names:
- Table: `tbl:<TableName>` → Redis key `or:element:tbl:<TableName>`
- Column: `col:<TableName>:<ColumnName>` → Redis key `or:element:col:<TableName>:<ColumnName>`
- Relation (FK): `rel:<FromTable>:<ToTable>:fk<id>` → Redis key `or:element:rel:<FromTable>:<ToTable>:fk<id>`

These are stored as standard element hashes:
- `name` (string)
- `flags_bin` (512 bytes, big-endian 4096-bit bitmap)
- `meta_profile` = `northwind_meta_v0`

Bit-profile summary (`northwind_meta_v0`):
- Identification: `0=TABLE`, `1=COLUMN`, `5=FK_REL`, `17/18/19` meta scope bits
- Column type family: `256..261` (TEXT/INTEGER/REAL/NUMERIC/DATETIME/BLOB)
- Column attributes: `272..278` (NOT NULL, DEFAULT, PK, FK, IDX)
- Length buckets: `288..291` (small/medium/large/huge)
- Relation bits: `1024` relation, `1030/1031` (1:1 / 1:N), `1040/1041` (mandatory/optional), `1050..1055` FK actions

GUI:
- Open `http://localhost:18080/explorer/schema/` to browse these decoded metadata records.

## Row data (bitset integers) + SQL vs Bitsets demo

In addition to OR objects and schema metadata, the GUI can ingest **row data** as 4096-bit integers and compare SQL filtering vs bitset filtering.

Keys:
- Row bitsets: `or:data:<TableName>:<RowId>` → decimal string integer (4096-bit)
- Reset registry: `or:import:northwind_compare:data_bits`

UI:
- Open `http://localhost:18080/explorer/data/`
- Click `Run data ingest…`
- Select a preset and run `Run comparison`

Backend API:
- `POST /api/v1/explorer/northwind/data_ingest`
- `GET /api/v1/explorer/northwind/data_info`
- `POST /api/v1/explorer/northwind/compare`

Bit-profile summary (row data v1; bucketed/approximate):
- Customers:
  - Country buckets: `256..260` (USA/UK/Germany/France/Other)
  - City buckets: `264..268` (London/Paris/Berlin/Seattle/Mexico D.F.)
- Products/Categories:
  - CategoryID buckets: `1024..1055` (CategoryID 1..32)
  - UnitPrice buckets: `1060..1063` (<10 / 10–20 / 20–50 / >=50)
- Orders:
  - OrderYear buckets: `1792..1795` (1996/1997/1998/Other)
- OrderDetails:
  - Quantity buckets: `1856..1859` (<5 / 5–10 / 11–20 / >20)
  - Discount: `1864` (Discount > 0)

If you see a mismatch:
- Ensure you imported into `or` (OR layout is required for this example).
- Ensure the SQLite DB is the expected Northwind variant (must contain `Customers`, `Orders`, and `Order Details`).
- Re-run with `Reset` enabled, then `Run Import`, then `Compare`.

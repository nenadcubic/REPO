# Query patterns (GUI)

This example is built to make query semantics obvious by using a dataset with predictable, easy-to-check results.

No scripts are executed. Data comes only from `example.json`.

## How to run in the GUI

1) Open the GUI (`http://localhost:18080`)
2) Go to `Examples`
3) Select: `Query patterns (query_patterns)`
4) Click `Run example`

## What it seeds

- `and_exact`      → bits `10 11`
- `and_superset`   → bits `10 11 12`
- `or_only_10`     → bit  `10`
- `or_only_11`     → bit  `11`
- `neither_10_11`  → bit  `12`

## Expected results (copy/paste friendly)

### AND (Find ALL)

Bits: `10 11` → `and_exact`, `and_superset`

### OR (Find ANY)

Bits: `10 11` → `and_exact`, `and_superset`, `or_only_10`, `or_only_11`

### NOT (Find NOT)

Include bit: `10`  
Exclude bits: `12`  
Result → `and_exact`, `or_only_10`

### Universe NOT

Exclude bits: `12` → `and_exact`, `or_only_10`, `or_only_11`

## Interpretation hints

- `Find NOT` is “include X but exclude Y/Z…”.
- `Universe NOT` ignores an include bit and starts from the universe (`<prefix>:all`).

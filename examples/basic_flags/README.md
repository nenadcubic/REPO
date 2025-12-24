# Basic flags (GUI)

This example seeds a tiny dataset into Redis so the GUI has known data to display.

No scripts are executed. Data comes only from `example.json`.

## How to run in the GUI

1) Open the GUI (`http://localhost:18080`)
2) Go to `Examples`
3) Select: `Basic flags (basic_flags)`
4) Keep the namespace as `er` (default), or choose another namespace
5) Click `Run example` (optional: enable `Reset` to remove prior runs of this example)

## What it seeds

- `alpha` → bits `1 2 3`
- `beta`  → bits `2 4`
- `gamma` → bits `3 5`

## What to expect

- `Queries → Find ALL` with bits `2` returns: `alpha`, `beta`
- `Queries → Find ANY` with bits `4 5` returns: `beta`, `gamma`
- `Queries → Universe NOT` with exclude bits `2` returns: `gamma`
- `Elements → Matrix` for `alpha` highlights exactly 3 cells (bits `1`, `2`, `3`)

## Interpretation hints

- `Find ALL` (AND) means *every requested bit must be present*.
- `Find ANY` (OR) means *at least one requested bit must be present*.
- `Universe NOT` returns elements that have *none* of the excluded bits.

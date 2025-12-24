# Bit-map groups (GUI)

This example is meant to be used together with the `Bit-maps` screen. The seeded elements use bits that have names and groups in the preset bit dictionary.

No scripts are executed. Data comes only from `example.json`.

## How to run in the GUI

1) Open the GUI (`http://localhost:18080`)
2) Go to `Examples`
3) Select: `Bit-map groups (bitmap_groups)`
4) Click `Run example`

## What it seeds

Elements are named like `acct:<risk>_<region>_<feature>` and set one bit from each group:

- Risk: `100` (Low), `101` (Medium), `102` (High)
- Region: `200` (US), `201` (EU), `202` (APAC)
- Feature: `300` (Core), `301` (Plus), `302` (Beta)

## What to expect

- `Bit-maps`: search for `Risk` / `Region` / `Feature` and confirm the bits are grouped.
- `Elements → Matrix`: fetch `acct:high_eu_plus` and hover the highlighted cells to see the named bits.
- `Queries`: run `Find ALL` with `102 201` to find “High risk in EU”.

## Interpretation hints

- The Bit-maps dictionary is preset metadata; it does not create Redis keys.
- The Matrix hover uses the already-fetched bit dictionary (no per-hover backend requests).

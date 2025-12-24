# Multi-namespace demo (GUI)

This example demonstrates namespace isolation. In the GUI, a “namespace” selects a Redis prefix family (for example `er:*` vs `er2:*`).

No scripts are executed. Data comes only from `example.json`.

## How to run in the GUI

1) Open the GUI (`http://localhost:18080`)
2) Go to `Examples`
3) Select: `Multi-namespace demo (multi_namespace_demo)`
4) Run it once with namespace `er`
5) Switch the namespace selector to a different namespace (for example `er2`), then run it again

## What to expect

- In `Elements → Get`, `shared:one` exists in both namespaces, but under different Redis keys:
  - `er:element:shared:one`
  - `er2:element:shared:one`
- In `Queries`, results are scoped to the selected namespace.

## Suggested check (hands-on)

1) Run this example in `er`.
2) Go to `Elements → Put` and change `shared:one` bits (still in `er`).
3) Switch namespace to `er2` and fetch `shared:one` again — it remains unchanged.

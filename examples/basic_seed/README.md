# Basic seed (GUI)

This example is designed for the GUI.

The `Examples` screen can load this dataset into Redis under the selected GUI namespace (e.g. `er:*`). No scripts are executed.

## What it seeds

Elements:
- `alice` → bits: `1 7 42`
- `bob` → bits: `7 9 1024`
- `carol` → bits: `1 9 13 2048`
- `dave` → bits: `0 1 2 3 4`
- `eve` → bits: `4095`

## Suggested checks (GUI)

- `Elements → Get`: fetch `alice`
- `Elements → Matrix`: fetch `dave`
- `Queries → Find`: bit `7` should match `alice` + `bob`
- `Queries → Find ALL`: bits `1 9` should match `carol`
- `Store + TTL`: store any query and inspect/delete the `store_key`


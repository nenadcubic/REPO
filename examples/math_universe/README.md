# Math universe (no DB)

Minimal “universe + indexes + set algebra” demo using integers and Redis `SET`s, with a Python reference evaluator.

## GUI seed (safe)

The GUI's `Examples` screen can load a small, predefined dataset from `example.json` into Redis under the selected GUI namespace (e.g. `er:*`). No scripts are executed.

Suggested checks (GUI):
- `Elements → Get`: fetch `U`, `A`, `B`, `C`
- `Queries → Find`: bit `2` should match `U` + `A` + `B`
- `Queries → Find NOT`: include bit `3`, exclude bit `5` should match `B` (from `B = {1,2,3}`, `C = {3,5}`)

## Keys (prefix `mu`, configurable via `MU_PREFIX`)

Base (no TTL):

- `mu:all` (SET of integers as strings)
- `mu:idx:even`, `mu:idx:odd`, `mu:idx:prime`, `mu:idx:mod3`, `mu:idx:gt50`

Derived (TTL, default 600s):

- `mu:tmp:*`

## Run

- `./examples/math_universe/00_reset.sh`
- `python3 examples/math_universe/01_ingest.py`
- `MU_CLEAN_TMP=1 ./examples/math_universe/02_query_compare.sh`

Env:

- `MU_REDIS_HOST` (default `localhost`)
- `MU_REDIS_PORT` (default `6379`)
- `MU_PREFIX` (default `mu`)
- `MU_TTL_SEC` (default `600`)
- `MU_MAX_N` (default `100`)
- `MU_CLEAN_TMP=1` deletes `mu:tmp:*` before compare

# Math universe (no DB)

Minimal “universe + indexes + set algebra” demo using integers and Redis `SET`s, with a Python reference evaluator.

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


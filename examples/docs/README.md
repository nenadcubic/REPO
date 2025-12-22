# Docs example (no DB)

Minimal inverted-index example using Redis `SET`s, with a reference evaluator in Python.

## Keys (prefix `docs`, configurable via `DOCS_PREFIX`)

Base (no TTL):

- `docs:all` (SET of doc IDs)
- `docs:term:<term>` (SET of doc IDs)

Derived (TTL, default 600s):

- `docs:tmp:*`

## Run

- `./examples/docs/00_reset.sh`
- `python3 examples/docs/01_ingest.py`
- `DOCS_CLEAN_TMP=1 ./examples/docs/02_query_compare.sh`

Env:

- `DOCS_REDIS_HOST` (default `localhost`)
- `DOCS_REDIS_PORT` (default `6379`)
- `DOCS_PREFIX` (default `docs`)
- `DOCS_TTL_SEC` (default `600`)
- `DOCS_CLEAN_TMP=1` deletes `docs:tmp:*` before compare


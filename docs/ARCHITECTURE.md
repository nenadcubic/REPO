# Architecture — element-redis

## Goal
One source of truth in C++ (the engine). Everything else (CLI, examples, future GUI) must be thin adapters.

Redis is the source of truth for persisted state and indexes.

## Layers and Boundaries

### `core/` (C++ library)
Responsibilities:
- Redis I/O (hiredis), Lua `EVAL`, reply decoding
- Atomic store+TTL primitives (Lua) and set operations
- Bitmap/flags utilities
- Key naming (single place)
- Error model (`Result<T>` / `Error`)

Non-responsibilities:
- No CLI argument parsing
- No filesystem I/O (except explicit caller-provided paths in ingest)
- No “magic” environment variables
- No UI concerns

### `ingest/` (data → Redis indexes)
Responsibilities:
- Convert external datasets into Redis indexes using the `core` API
- Treat data sources as interchangeable providers of rows (Strategy)

Non-responsibilities:
- No query semantics implemented here
- No Redis key naming outside `core::keys`

### `cli/` (thin adapter)
Responsibilities:
- Parse arguments
- Call engine/core functions
- Print results and map errors to exit codes

Non-responsibilities:
- No set logic or Lua duplication

## Patterns (max 2)

### Facade: `Engine`
The only entry point for query operations used by CLI and future GUI.

### Strategy: `IDataSource`
Provides rows for ingest (e.g. `NorthwindSqliteSource`, `JsonDatasetSource`).

## Redis Keys (Predictable)
All Redis key strings are constructed in one place: `include/er/keys.hpp`.

Rules:
- No ad-hoc string concatenation for keys outside `keys.*`
- Keys are stable, human-readable, and namespaced
- Temporary keys use a predictable template:
  - `${prefix}:tmp:<tag>:<epoch_or_ns>:<rand>`

## Query Semantics
Set algebra is the primary query model:
- `ALL`: intersection (`SINTER*`)
- `ANY`: union (`SUNION*`)
- `NOT`: set difference (`SDIFF*`) over a defined universe

Note on SQL `NULL` vs set-diff:
- SQL uses 3-valued logic; `col != 'X'` excludes `NULL`.
- Set difference includes items not present in the excluded set (including “unknowns”).
Examples must be explicit about this when comparing SQL ↔ Redis.

## Atomicity
The canonical atomic “store+ttl” primitive is implemented via Lua in `core`.
Examples may use `MULTI/EXEC` as a convenience, but must document that `core` is canonical and semantics must match.

## Error Model
`core` uses `Result<T>` with `Errc` + message:
- No exceptions in `core` (other than truly-fatal like `std::bad_alloc`)
- All fallible operations return `Result<T>` and are `[[nodiscard]]`

CLI policy:
- Convert `Result` failures into readable stderr output and stable exit codes

## Threading Model
`RedisClient` is not thread-safe by default (hiredis contexts are not thread-safe).
If a multi-threaded consumer appears (GUI/server), use “one client per thread” or a pool at the application layer.

## Lua Script Policy
- No duplicated script logic across layers
- Scripts live/are embedded in one place and are versioned with the core
- Prefer calling the canonical core primitive from adapters


element-redis
=============

Minimal C++ demo of storing 4096-bit flags per element in Redis, maintaining per-bit inverted indexes, and running composite set queries (AND/OR/NOT), optionally storing results with TTL atomically.

**Run (Docker dev)**
- `env UID=$(id -u) GID=$(id -g) docker compose -f docker/docker-compose.yml up -d --build`
- `docker compose -f docker/docker-compose.yml exec dev bash`

**Build (inside `dev`)**
- `cmake -S . -B build -G Ninja`
- `cmake --build build -j`

**CLI**
- Binary: `build/cli/er_cli`
- Redis connection:
  - `ER_REDIS_HOST` (default `localhost`)
  - `ER_REDIS_PORT` (default `6379`)
- Store output:
  - `ER_KEYS_ONLY=1` or `--keys-only` prints only the tmp key for `*_store` commands.

Examples (inside `dev`):
- `./build/cli/er_cli put alice 1 7 42`
- `./build/cli/er_cli find 42`
- `./build/cli/er_cli find_all 1 42`
- `./build/cli/er_cli --keys-only find_all_store 30 1 42`
- `./build/cli/er_cli show <tmp_key>`
- `./build/cli/er_cli del alice`

**Smoke test**
- `./scripts/smoke_test.sh` (deletes only `er:*` keys by default; configurable via `ER_PREFIX`, `ER_REDIS_HOST`, `ER_REDIS_PORT`, `ER_CLI`)

Next:
- Implementation checklist: `docs/TODO.md`

**Host build (Ubuntu/Debian)**
- Deps: `cmake`, `pkg-config`, `libhiredis-dev`, `libboost-all-dev`, a C++20 compiler
- `sudo apt-get update`
- `sudo apt-get install -y build-essential cmake pkg-config libhiredis-dev libboost-all-dev`
- `cmake -S . -B build_local`
- `cmake --build build_local -j`

Notes:
- If you previously built inside the Docker `dev` container, your existing `build/` cache may point to `/work`; use a fresh directory like `build_local/`.
- Docker dev uses `ER_REDIS_HOST=redis` via `docker/docker-compose.yml`; host default is `localhost`.

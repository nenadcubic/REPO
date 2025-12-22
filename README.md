element-redis
=============

Minimal C++ demo of storing 4096-bit flags per element in Redis, maintaining per-bit inverted indexes, and running composite set queries (AND/OR/NOT), optionally storing results with TTL atomically.

**Run (Docker dev)**
- `docker compose -f docker/docker-compose.yml up -d --build`
- `docker compose -f docker/docker-compose.yml exec dev bash`

**Build (inside `dev`)**
- `cmake -S . -B build -G Ninja`
- `cmake --build build -j`

**CLI**
- Binary: `build/cli/er_cli`
- Redis connection:
  - `ER_REDIS_HOST` (default `redis`)
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

Next:
- Implementation checklist: `docs/TODO.md`

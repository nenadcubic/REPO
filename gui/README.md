# element-redis GUI sandbox

Thin WEB GUI + REST API layer for `element-redis`, packaged as a portable Docker Compose sandbox (frontend + backend + Redis).

Architecture contract (GUI-freeze-v1):
- `gui/ARHITECTURE_GUI.md`

UI copy contract (GUI-freeze-v1):
- `gui/GUI_COPY.md`

Bit dictionary (preset metadata):
- `gui/presets/<GUI_PRESET>/bitmaps.json`

## Run

From repo root:
- `docker compose -f gui/docker-compose.yml up -d --build`

Open:
- GUI: `http://localhost:${GUI_HTTP_PORT:-8080}`
- API: `http://localhost:${GUI_API_PORT:-8000}/api/v1/health`

## Config: `.env` + presets

- Presets live in `gui/presets/*.env` and are selected via `GUI_PRESET` (`default`, `dev`, `demo`).
- Optional: copy `gui/.env.example` to `gui/.env` (or export env vars in your shell). `docker compose` reads `gui/.env` automatically for variable interpolation.

Common variables:
- `GUI_PRESET` (default: `default`)
- `GUI_HTTP_PORT` (default: `8080`)
- `GUI_API_PORT` (default: `8000`)

Backend/Redis variables (rarely needed; preset defaults are fine):
- `ER_REDIS_HOST` (default preset: `redis`)
- `ER_REDIS_PORT` (default preset: `6379`)
- `ER_PREFIX` (default preset: `er`)

## Endpoints

All endpoints are versioned under `/api/v1` and return:
- Success: `{ "ok": true, "data": { ... } }`
- Error: `{ "ok": false, "error": { "code": "...", "message": "...", "details": { ... } } }`

OpenAPI 3.1:
- `gui/backend/openapi.yaml`

## Bit-maps

- Backend loads `bitmaps.json` from the selected preset directory and exposes it via `GET /api/v1/bitmaps`.
- The GUI caches bit-maps in memory (no per-hover requests).

## Safety defaults

- List endpoints use `limit` (default `200`) to avoid huge responses/UI freezes.
- Store TTL has a v1 cap: `ER_GUI_TTL_MAX_SEC` (default `86400`).

## Troubleshooting

- Check health: `curl -sS http://localhost:${GUI_API_PORT:-8000}/api/v1/health | jq`
- View logs: `docker compose -f gui/docker-compose.yml logs -f --tail=200 backend`
- If ports are busy, set `GUI_HTTP_PORT` / `GUI_API_PORT` in `gui/.env`.

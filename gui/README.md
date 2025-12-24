# element-redis GUI sandbox

Thin WEB GUI + REST API layer for `element-redis`, packaged as a portable Docker Compose sandbox (frontend + backend + Redis).

Architecture contract (GUI-freeze-v1):
- `gui/ARHITECTURE_GUI.md`

UI copy contract (GUI-freeze-v1):
- `gui/GUI_COPY.md`

Bit dictionary (preset metadata):
- `gui/presets/<GUI_PRESET>/bitmaps/<ns>.json`

Namespaces (preset metadata):
- `gui/presets/<GUI_PRESET>/namespaces.json`

## Run

From repo root:
- `docker compose -f gui/docker-compose.yml up -d --build`

Open:
- GUI: `http://localhost:${GUI_HTTP_PORT:-18080}`
- API: `http://localhost:${GUI_API_PORT:-18000}/api/v1/health`

## Config: `.env` + presets

- Presets live in `gui/presets/*.env` and are selected via `GUI_PRESET` (`default`, `dev`, `demo`).
- Optional: copy `gui/.env.example` to `gui/.env` (or export env vars in your shell). `docker compose` reads `gui/.env` automatically for variable interpolation.

Common variables:
- `GUI_PRESET` (default: `default`)
- `GUI_HTTP_PORT` (default: `18080`)
- `GUI_API_PORT` (default: `18000`)

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

Namespace selection:
- Allowed namespaces are exposed via `GET /api/v1/namespaces`.
- Element/query/store endpoints accept `ns` (namespace id) to select a Redis prefix family (for example `er:*` vs `or:*`).

## Bit-maps

- Backend loads namespace-scoped bit-maps from `gui/presets/<GUI_PRESET>/bitmaps/<ns>.json` and exposes it via `GET /api/v1/bitmaps?ns=...`.
- Edit mode saves updates back to `gui/presets/<GUI_PRESET>/bitmaps/<ns>.json` via `PUT /api/v1/bitmaps?ns=...` (metadata-only; no Redis operations).
- `bitmaps.json` `defaults.format` is reserved for future use (not active in v1); hover tooltip text is fixed to `NAME: 0` / `NAME: 1`.
- The GUI caches bit-maps in memory (no per-hover requests).

## Explorer

The GUI includes an `Explorer` screen at:
- `http://localhost:${GUI_HTTP_PORT:-18080}/explorer/`

Purpose:
- Quick overview of namespaces (left panel) and elements in a namespace (middle panel).
- Detailed view of a single element’s bits (right panel): `Details` and 64×64 `Matrix`.
- Namespace bitmap overview (right panel): renders many elements at once; click a row to open that element.

How to use:
1) Open `Explorer` from the sidebar.
2) Pick a namespace.
3) Search/sort elements, then click an element to inspect it.
4) Switch `Element` / `Namespace bitmap` depending on whether you want a single element or a multi-element overview.

Notes:
- Explorer fully supports `er_layout_v1` namespaces (bitset elements: `Details`, 64×64 `Matrix`, and `Namespace bitmap`).
- `or_layout_v2` namespaces are shown as a read-only object browser (hash view). Matrix/bitmap views are disabled because OR objects are not bitsets.
- If you have no data, seed a dataset via `Examples` (seed-type examples).

API (Explorer):
- `GET /api/v1/explorer/namespaces`
- `GET /api/v1/explorer/namespaces/{namespace}/elements?search=&page=&page_size=`
- `GET /api/v1/explorer/elements/{encodedKey}`
- `GET /api/v1/explorer/namespaces/{namespace}/bitmap?limit=&offset=`

## Examples

- The GUI provides an `Examples` screen that can seed Redis with predefined datasets so the existing `Elements`, `Queries`, and `Matrix` views have known data to display.
- Examples are loaded from the repo’s `examples/<id>/example.json` and `examples/<id>/README.md` (no scripts are executed).
- Example types:
  - `seed`: loads predefined elements into the selected namespace
  - `dataset_compare`: imports a dataset (for example from SQLite) and exposes comparison reports
- API:
  - `GET /api/v1/examples`
  - `GET /api/v1/examples/{id}/readme`
  - `POST /api/v1/examples/{id}/run` with `{ "ns": "...", "reset": false }` (both fields optional; defaults come from the example)
  - `GET /api/v1/examples/{id}/reports?ns=...` (dataset compare examples only; `ns` defaults to the example namespace)

Built-in dataset compare example:
- `northwind_compare`: imports Northwind from `examples/northwind_compare/assets/northwind.sqlite` into the `or` namespace (OR layout), then compares row counts and sample order totals.

## Safety defaults

- List endpoints use `limit` (default `200`) to avoid huge responses/UI freezes.
- Store TTL has a v1 cap: `ER_GUI_TTL_MAX_SEC` (default `86400`).

## Selftest (backend container)

After the sandbox is up, run:
- `docker compose -f gui/docker-compose.yml exec -T backend bash -lc /app/scripts/selftest.sh`

## Troubleshooting

- Check health: `curl -sS http://localhost:${GUI_API_PORT:-18000}/api/v1/health` (optionally: `| jq`)
- View logs: `docker compose -f gui/docker-compose.yml logs -f --tail=200 backend`
- If ports are busy, set `GUI_HTTP_PORT` / `GUI_API_PORT` in `gui/.env`.

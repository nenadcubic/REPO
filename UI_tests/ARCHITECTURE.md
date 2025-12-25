# ARCHITECTURE.md (UI_tests)
Version: Playwright-suite-v1
Scope: End-to-end tests for `gui/frontend/www` (main GUI) and `gui/frontend/explorer` (Explorer app)

## 0) Goals

- Validate user-visible UI behavior and core user flows.
- Validate UI-side input normalization and validation rules (empty, invalid values, limits, boundary values).
- Validate backend “contract assumptions” that the UI relies on (error envelopes, limit enforcement, health/availability).
- Keep tests deterministic, readable, and low-flake.

Non-goals:
- Performance benchmarking
- Redis correctness beyond what is observable through the UI/API (covered by core tests elsewhere)

## 1) Runtime contract

The suite runs against an already running sandbox:
- GUI: `http://localhost:18080` (default)
- API: `http://localhost:18000/api/v1` (default)

Configuration:
- `GUI_BASE_URL` controls Playwright `baseURL`
- `API_BASE_URL` controls API helper base URL for seeding and direct contract tests

## 2) Test organization

Location: `UI_tests/tests/`

High-level grouping:
- `00_navigation.spec.ts`: smoke + navigation + namespace persistence
- `01_status.spec.ts`: backend/redis status + namespace discovery
- `02_elements_put_get_matrix.spec.ts`: Elements Put/Get/Matrix, copy buttons, double-click behavior, boundaries
- `03_queries.spec.ts`: all query modes + negative validations, with deterministic test data
- `04_store.spec.ts`: Store+TTL create/inspect/delete + negative validations
- `05_examples_logs.spec.ts`: Examples run + Logs view validation
- `06_bitmaps.spec.ts`: Bit-maps browse + edit-mode validation + group CRUD + bulk assign (with restore)
- `07_explorer.spec.ts`: Explorer browse + quick create validation + layout-specific constraints
- `08_api_contract.spec.ts`: API envelope + limits + health assumptions
- `09_schema_explorer.spec.ts`: Schema Explorer UI + decoded metadata after `northwind_compare` import
- `10_assoc_wordnet.spec.ts`: Associations (WordNet) UI (demo mode)
- `11_northwind_data_vs_bitsets.spec.ts`: Northwind row-bitset ingest + SQL vs bitset comparison UI

Shared helpers: `UI_tests/tests/_helpers.ts`

## 3) Non-negotiable rules (anti-flake)

1) Prefer stable locators:
   - `#id` selectors for main GUI (`gui/frontend/www/index.html`)
   - `data-testid` for Explorer (`gui/frontend/explorer/src/Explorer.tsx`)

2) No arbitrary sleeps.
   - Use `expect(locator).toBeVisible()` / `toHaveText()` / `toContainText()` as synchronization.

3) Avoid cross-test coupling.
   - Each spec file seeds or creates what it needs.

4) Keep state changes reversible.
   - Any test that mutates preset metadata must restore it (see Bit-maps test).

## 4) Data strategy (determinism)

### 4.1 Seeded datasets
When possible, tests seed known data using the backend Examples API:
- `POST /api/v1/examples/{id}/run` (typically `basic_flags`)

This keeps UI tests stable and avoids manually constructing many elements via the UI.

### 4.2 Deterministic unique elements for assertions
Where the seeded dataset is not strict enough (e.g., Queries returning many matches), tests create their own unique elements with “high bit” values unlikely to collide with existing data:
- Example: bits `3991..3995`
- Names include a timestamp suffix to avoid collisions between reruns

## 5) Concurrency model

Default is serial execution (`workers: 1`) because:
- The suite shares one Redis + backend instance
- Global state (namespaces, examples, bitmaps) is shared

If isolation is introduced later (separate Redis per worker, unique prefixes per worker, or container-per-job), the suite can be parallelized.

## 6) UI validation coverage

The suite explicitly tests:
- Required fields (empty values)
- Invalid input types (non-numeric where numeric is required)
- Boundary ranges (`0..4095`, `limit` maxes, TTL max)
- Length constraints (`name` max 100)
- Double-click behavior on “Save Element” (should not send duplicate requests)

Validation is verified primarily via the UI banner (`#banner`) and/or view-specific outputs.

## 7) Backend contract coverage

The suite verifies key assumptions the UI depends on:
- Health endpoint is reachable and reports Redis state.
- Validation failures return HTTP 422 with `{ ok:false, error:{ code, message, details } }`.
- Limit caps are enforced (example: `logs?tail=...`, Explorer `page_size`).

## 8) Debugging workflow

Preferred:
- UI runner: `npx playwright test --ui --headed`
- Stop-and-inspect: add `await page.pause()` temporarily or run with `--debug`

Artifacts:
- `UI_tests/test-results/` contains screenshots/videos/traces on failures
- `UI_tests/playwright-report/` contains the HTML report

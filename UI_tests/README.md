# UI Tests (Playwright)

End-to-end UI test suite for the `element-redis` GUI, using Playwright.

Prereqs:
- The GUI stack is running (GUI + backend API + Redis)
- Node.js >= 18 (required by Playwright)

## 1) Start the GUI stack

From repo root:

```bash
docker compose -f gui/docker-compose.yml up -d --build
```

Optional sanity check:

```bash
curl -sS http://localhost:18000/api/v1/health
```

## 2) Install dependencies

From repo root:

```bash
cd UI_tests
npm ci
npx playwright install chromium
```

## 3) Run tests

```bash
cd UI_tests
npm test
```

Run a single file:

```bash
npx playwright test tests/02_elements_put_get_matrix.spec.ts
```

Run by title grep:

```bash
npx playwright test -g "Store ANY"
```

## 4) Run with UI runner (watch + debug)

UI runner opens a dashboard; the browser may still be headless unless you enable headed mode.

```bash
npx playwright test --ui --headed
```

To stop inside a running test and inspect steps:

- add `await page.pause();` temporarily in the test, or
- run: `npx playwright test --ui --debug`

## 5) Reports, traces, screenshots, videos

- HTML report: `UI_tests/playwright-report/` (open with `npm run report` or `npx playwright show-report`)
- Artifacts: `UI_tests/test-results/` (screenshots/videos/traces on failures or retries)

## 6) Environment variables

- `GUI_BASE_URL` (default `http://localhost:18080`)
- `API_BASE_URL` (default `http://localhost:18000/api/v1`)

Example:

```bash
GUI_BASE_URL=http://localhost:18081 API_BASE_URL=http://localhost:18001/api/v1 npm test
```

## Notes

- Tests assume a shared running sandbox; by default they run serially (single worker) to avoid Redis/test-data collisions.
- The Bit-maps edit-mode test writes via `PUT /api/v1/bitmaps` but restores the original document at the end; if a run is interrupted mid-test, reset with `git checkout -- gui/presets/default/bitmaps/*.json` (or rerun the suite).
- The Schema Explorer test (`tests/09_schema_explorer.spec.ts`) requires `examples/northwind_compare/assets/northwind.sqlite` to be present (as expected by the `northwind_compare` example).
- The Northwind Data vs Bitsets test is `tests/11_northwind_data_vs_bitsets.spec.ts` (ingests row bitsets via `/api/v1/explorer/northwind/data_ingest`).
- The Associations demo test is `tests/10_assoc_wordnet.spec.ts` (uses demo mode; no full WordNet ingest required).

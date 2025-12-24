# ARHITECTURE_GUI.md
Version: GUI-freeze-v1  
Scope: User GUI + Backend API (Admin GUI is out of scope for v1)

## 0) Core premise
GUI is a **thin UI**. It must not re-implement core logic, query semantics, or Redis logic.  
All heavy work is done by **C++ core + Redis + Lua** (via backend adapter).

**Goal:** portable, zero-setup, sandboxed system launched with:
- `docker compose up -d --build`

---

## 1) Non-negotiable project rules
1. **Everything GUI-related lives in a new top-level folder:** `gui/`  
   No scattering across the repository.

2. **Zero-setup sandbox**  
   GUI + Backend + Redis (and any supporting services) must run in Docker Compose.

3. **Web GUI**  
   No desktop UI.

4. **Config by presets, not YAML edits**  
   - `.env` + preset profiles (e.g. `GUI_PRESET=default|dev|demo`)
   - Presets are stored under `gui/presets/`
   - Users must NOT edit docker-compose.yml for configuration.

5. **API standard**  
   REST + JSON, versioned under `/api/v1` with **OpenAPI 3.1** spec.

---

## 2) Navigation & layout standard (Sidebar)
### 2.1 Primary navigation
Use a **left sidebar** with these sections (MVP):
- Status
- Elements
- Queries
- Store + TTL
- Examples
- Logs (optional, preferred)
- Bit-maps (preset metadata)

### 2.2 Screen structure rule
Every screen follows this structure:
- Short description: “what it does”
- Form inputs
- Single primary action button: **Run**
- Results panel (monospace for technical output)

### 2.3 Layout constraints
- Max **two columns** on desktop; **single column** on small screens
- 8px spacing grid (8/16/24/32)

---

## 3) Thin UI contract (no duplicated logic)
GUI responsibilities:
- Accept user input
- Perform client-side validation & normalization
- Call backend endpoints
- Render results and errors clearly

GUI must NOT:
- Compute query results
- Implement Redis set operations
- Construct Redis keys (store_key comes from backend)
- Maintain hidden state beyond the current view

Backend responsibilities:
- Validate input (defensive)
- Call existing core/CLI semantics
- Return consistent JSON envelopes
- Provide OpenAPI spec

---

## 4) Redis must be addressed in every future change request (MANDATORY)
**Rule:** Every future GUI/API change request must explicitly address Redis implications:
1. **Key naming** (prefix conventions, e.g. `er:`)
2. **TTL behavior** (creation, inspection, expiry)
3. **Result size control** (limits to avoid huge transfers/UI freeze)
4. **Atomicity** (Lua usage when needed)
5. **Operational safety** (health, restart, failure modes)

If a future request does not mention Redis impacts, it is considered **incomplete** and must be updated.

Bit-maps note (v1):
- Bit-maps are preset metadata only (no Redis keys, no TTL, no atomicity).
- Matrix hover must use already-fetched element flags (no extra Redis calls per hover).
- Bit-maps are namespace-scoped: `gui/presets/<GUI_PRESET>/bitmaps/<ns>.json`.
- `bitmaps.json` `defaults.format` is reserved for future use (not active in v1); hover tooltip text is fixed to `NAME: 0` / `NAME: 1`.

Examples note (v1):
- Examples seed known elements via backend (implemented as a whitelist, no script execution).
- Optional reset must only delete canonical keys within the selected namespace prefix (no wildcard delete across prefixes).

---

## 5) Input standards (canonical normalization)
### 5.1 Bits input
GUI accepts:
- space-separated: `1 2 3`
- comma-separated: `1,2,3`
- mixed: `1, 2 3`

Normalization (GUI side, before sending):
- trim
- split on comma/space
- parse int
- **dedupe**
- **sort ascending**

Validation:
- each bit must be `0..4095`
- invalid tokens must be surfaced (example: `12a`)

Backend must still validate `0..4095` defensively.

### 5.2 Name input
- non-empty
- trimmed
- max length 100

### 5.3 TTL input
- integer > 0
- default: 60 seconds
- (v1) recommended max: 86400 seconds (1 day) unless explicitly overridden later

---

## 6) Result rendering standards
### 6.1 Lists
Any endpoint returning lists must support `limit`:
- default limit: 200
- GUI always uses a limit (never unbounded)

Render:
- `count`
- items list (monospace or clean list)
- copy actions:
  - Copy list
  - Copy CSV (where applicable)

### 6.2 Store results
Always render:
- `store_key` (monospace)
- `ttl_remaining`
- `count`
- `preview` first N names

Provide action:
- Inspect store_key (fetch names + ttl_remaining + count)

Optional (v1):
- Delete store_key

---

## 7) Error and success envelope (strict)
All responses follow one of these shapes.

### 7.1 Success
```json
{ "ok": true, "data": { ... } }
```

7.2 Error
```json
{
  "ok": false,
  "error": {
    "code": "INVALID_BIT",
    "message": "bit must be in range 0..4095",
    "details": { "bit": 5000 }
  }
}
```

GUI rendering rules:
- show `error.code` as title
- show `error.message` as body
- `details` collapsible

---

## 8) MVP screens (User GUI only)
### 8.1 Status
- backend ok
- redis ok
- ping ms, used_memory
- backend_version
- refresh

### 8.2 Elements
Put
- name
- bits list
- output: written_bits, name

Get
- name
- output: sorted bits + copy list/csv

### 8.3 Queries (tabs)
- find
- find_all
- find_any
- find_not
- find_universe_not

Output: names + count (respect limit)

### 8.4 Store + TTL (tabs)
- find_all_store (ttl + bits[])
- find_any_store (ttl + bits[])
- find_not_store (ttl + include_bit + exclude_bits[])

Output: store_key + ttl_remaining + count + preview  
Inspect endpoint is mandatory.

### 8.5 Logs (optional but preferred)
- read-only tail N lines
- filter info/warn/error (optional)

---

## 9) Operational standards (Compose)
Docker Compose must include:
- healthchecks (backend + redis)
- restart policy
- sensible defaults
- no host dependencies beyond Docker

---

## 10) Versioning & drift control
- This file defines GUI-freeze-v1.
- Breaking changes require bump to v2 or explicit “break notice”.
- API is versioned under /api/v1.

---

## 11) Quick checklist for every PR (GUI/API)
- Change lives under gui/
- Thin UI preserved (no query logic moved into UI)
- Redis implications explicitly addressed (keys/TTL/limits/atomicity)
- Limit enforced for list outputs
- Errors follow the envelope
- OpenAPI updated
- Compose still one-command runnable

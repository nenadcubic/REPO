# GUI_COPY.md
Version: GUI-freeze-v1  
Scope: User GUI copy (labels, help text, messages)

---

## 1) Navigation (Sidebar Labels)
- **Status**
- **Elements**
- **Queries**
- **Store + TTL**
- **Logs**

---

## 2) Status Panel
### Section title: `System Status`
### Description:
> Overview of backend, Redis, and preset configuration.

### Fields:
- `Backend:` ✅ / ❌
- `Redis:` ✅ / ❌
- `Ping:` `3 ms`
- `Memory Used:` `12.3 MB`
- `Backend Version:` `v1.0.0`
- `Preset:` `default`

### Button:
- `Refresh Status`

### Failure message:
> ⚠️ Unable to connect to backend. Please ensure the sandbox is running.

---

## 3) Elements Panel
### Section title: `Element Operations`

---

### 3.1 Put Element
**Header:** `Create or Update Element`

**Input fields:**
- `Element Name` (max 100 characters)
- `Bits` (comma or space separated, e.g. `1, 5, 1023`)

**Button:**
- `Save Element`

**Tooltip (Bits input):**
> Enter bit indices from 0 to 4095. Separate by space or comma. Duplicates and invalid entries will be removed.

**Success message:**
> ✅ Element saved. 4 bits written.

**Error (bit out of range):**
> ❌ Bit value must be between 0 and 4095.

---

### 3.2 Get Element
**Header:** `Retrieve Element by Name`

**Input:**
- `Element Name`

**Button:**
- `Fetch Element`

**Output:**
- `Bits: [1, 5, 1023]`
- Buttons:
  - `Copy as CSV`
  - `Copy as List`

**Error (not found):**
> ⚠️ No element found with that name.

---

## 4) Queries Panel
### Section title: `Find Matching Elements`

---

### Tab: Find (SINGLE)
- **Label:** `Bit`
- **Description:** Find all elements that contain this bit.

---

### Tab: Find ALL (AND)
- **Label:** `Bits`
- **Description:** Find elements that contain **all** of the specified bits.

---

### Tab: Find ANY (OR)
- **Label:** `Bits`
- **Description:** Find elements that contain **at least one** of the specified bits.

---

### Tab: Find NOT
- **Labels:**
  - `Include Bit`
  - `Exclude Bits`
- **Description:** Find elements that contain `Include Bit`, but none of the `Exclude Bits`.

---

### Tab: Universe NOT
- **Label:** `Exclude Bits`
- **Description:** Find all elements that do **not** contain any of the given bits.

---

### Button (all tabs):
- `Run Query`

**Result:**
- `12 matching elements found`
- Buttons:
  - `Copy Results`
  - `Copy as CSV`

**Error (empty result):**
> No elements matched your query.

**Error (bit validation):**
> ❌ One or more bits are out of the 0–4095 range.

---

## 5) Store + TTL Panel
### Section title: `Store Results with Expiry`

### Shared fields:
- `TTL (seconds)` – tooltip: `How long to keep this result in Redis. Default is 60 seconds.`

---

### Tabs (same as in Queries, plus TTL):
- `Find ALL + TTL`
- `Find ANY + TTL`
- `Find NOT + TTL`

---

**Result:**
- `✅ Stored as: er:tmp:nonce:abcd123`
- `42 elements stored`
- `Time remaining: 58s`
- `Preview: x, y, z...`

**Buttons:**
- `Inspect Store Key`
- `Delete Store Key` (optional)

**Error (store fail):**
> ⚠️ Failed to store result. Please check backend logs.

---

## 6) Inspect Store Key
### Panel title: `Inspect Stored Result`

**Fields:**
- `Store Key` (input or auto-populated)
- `Limit (optional)` (default: 200)

**Output:**
- `42 elements`
- `Time remaining: 23s`

**Button:**
- `Refresh`

---

## 7) Logs Panel (optional)
### Section title: `Backend Logs (read-only)`

**Fields:**
- `Tail last N lines:` (default 200)
- Filter dropdown (optional): `Info | Warning | Error`

**Live stream toggle:** `⏵ Live`

**Error:**
> ⚠️ Could not retrieve logs. Check backend status.

---

## 8) Validation & Error Messages

### Shared
- `This field is required.`
- `Invalid value.`
- `Must be a number.`
- `Must be between 0 and 4095.`

---

## 9) General buttons and labels

| Label          | Purpose                          |
|----------------|----------------------------------|
| `Run`          | Execute query/store operation    |
| `Save`         | Save an Element                  |
| `Fetch`        | Get Element by name              |
| `Refresh`      | Reload system status             |
| `Inspect`      | Load contents of store_key       |
| `Delete`       | Delete a store_key (optional)    |
| `Copy`         | Copy output                      |
| `Copy CSV`     | Copy as comma-separated          |
| `Copy List`    | Copy as newline-separated        |

---

## 10) UX notes (for consistency)
- Always disable action buttons during loading
- Show spinner in result panel
- Use green banner for success, red for error
- Render counts explicitly (`Found: 27 elements`)
- Use monospace for Redis keys, bit lists, and logs

---

## 11) Language and tone
- Technical but approachable
- No jargon for users (“bitmask”, “set op”, etc.)
- Use “Element” consistently instead of “record” or “item”
- Avoid passive voice: prefer `Saved`, not `Was saved`


const $ = (id) => document.getElementById(id);

const runtimeConfig = {
  ttlMaxSec: 86400,
  defaultLimit: 200,
  maxQueryLimit: 5000,
  storePreviewLimit: 25,
  erPrefix: "er",
  backendVersion: "unknown",
};

const nsState = {
  doc: null, // namespaces response data
  options: [], // [{id,label,prefix}]
  selectedId: "er",
  selectedLabel: "Element-Redis",
  selectedPrefix: "er",
};

const state = {
  error: null, // { type: "validation"|"request", message: string }
  locked: false, // true only while request in-flight
};

let bitmapsCache = null; // { ns, meta, items, byBit: Map<number, item> }
let bitmapsDoc = null; // raw document for editing (bitmaps.json), scoped by ns

const bitmapsUi = {
  editMode: false,
  selectedBit: null,
  editingGroupOldId: null,
};

const matrixState = {
  bitSet: null, // Set<number>
};

let nsDiscoverCache = null;
let examplesCache = null;
const examplesReadmeCache = new Map(); // id -> markdown string

function disableAllInputs() {
  for (const el of document.querySelectorAll(".content input, .content select, .content textarea")) {
    el.disabled = true;
  }
  const ns = $("nsSelect");
  if (ns) ns.disabled = true;
}

function enableAllInputs() {
  for (const el of document.querySelectorAll(".content input, .content select, .content textarea")) {
    el.disabled = false;
  }
  const ns = $("nsSelect");
  if (ns) ns.disabled = false;
}

function disableAllButtons() {
  for (const el of document.querySelectorAll("button")) {
    el.disabled = true;
  }
}

function enableAllButtons() {
  for (const el of document.querySelectorAll("button")) {
    el.disabled = false;
  }
}

function clearBanner() {
  const b = $("banner");
  if (!b) return;
  b.classList.add("hidden");
  b.classList.remove("success", "error");
  b.textContent = "";
}

function clearErrorState() {
  if (!state.error && !state.locked) return;

  clearBanner();

  const activeView = document.querySelector(".view.active");
  if (activeView) {
    for (const pre of activeView.querySelectorAll("pre.out")) pre.textContent = "";
  }
  const queryNames = $("queryNames");
  if (queryNames && document.querySelector("#view-queries")?.classList.contains("active")) queryNames.textContent = "";

  state.error = null;
  state.locked = false;

  enableAllInputs();
  enableAllButtons();
}

function showBanner(kind, text) {
  const b = $("banner");
  if (!b) return;
  b.classList.remove("hidden");
  b.classList.toggle("success", kind === "success");
  b.classList.toggle("error", kind === "error");
  b.textContent = text;
}

function setOutLoading(outEl, loading) {
  if (!outEl) return;
  outEl.classList.toggle("loading", !!loading);
  if (loading) outEl.textContent = "Loading...";
}

async function withRequest({ buttonEl, outEl, fn }) {
  clearBanner();
  const prevText = buttonEl ? buttonEl.textContent : "";
  state.locked = true;
  disableAllButtons();
  disableAllInputs();
  if (buttonEl) buttonEl.disabled = true;
  setOutLoading(outEl, true);
  try {
    await fn();
  } finally {
    setOutLoading(outEl, false);
    if (buttonEl) {
      buttonEl.disabled = false;
      buttonEl.textContent = prevText;
    }
    enableAllInputs();
    enableAllButtons();
    state.locked = false;
  }
}

function normalizeName(input) {
  const name = (input || "").trim();
  if (!name) throw new Error("This field is required.");
  if (name.length > 100) throw new Error("Element Name must be max 100 characters.");
  return name;
}

function normalizeInt(input, label) {
  const s = String(input ?? "").trim();
  if (!s) throw new Error("This field is required.");
  const n = Number(s);
  if (!Number.isFinite(n)) throw new Error("Must be a number.");
  if (!Number.isInteger(n)) throw new Error("Invalid value.");
  return n;
}

function normalizeBits(input) {
  const s = (input || "").trim();
  if (!s) return [];
  const parts = s.split(/[\s,]+/).filter(Boolean);
  const bits = [];
  for (const p of parts) {
    if (!/^-?\d+$/.test(p)) throw new Error("Invalid value.");
    const n = Number(p);
    if (!Number.isInteger(n)) throw new Error("Must be a number.");
    if (n < 0 || n > 4095) throw new Error("Must be between 0 and 4095.");
    bits.push(n);
  }
  const uniq = Array.from(new Set(bits));
  uniq.sort((a, b) => a - b);
  return uniq;
}

function normalizeTTL(input) {
  const ttl = normalizeInt(input, "TTL");
  if (ttl <= 0) throw new Error("Invalid value.");
  if (ttl > Number(runtimeConfig.ttlMaxSec || 86400)) throw new Error("Invalid value.");
  return ttl;
}

function normalizeLimit(input, label, max) {
  const limit = normalizeInt(input, label);
  if (limit <= 0) throw new Error("Invalid value.");
  if (limit > max) throw new Error("Invalid value.");
  return limit;
}

function readLimitOrDefault(inputEl, label, max, defaultVal) {
  const s = String(inputEl?.value ?? "").trim();
  if (!s) return defaultVal;
  return normalizeLimit(s, label, max);
}

async function apiJson(path, opts = {}) {
  try {
    const res = await fetch(path, {
      headers: { "Content-Type": "application/json", ...(opts.headers || {}) },
      ...opts,
    });
    const data = await res.json().catch(() => null);
    if (data && typeof data === "object") return data;
    if (!res.ok) return { ok: false, error: { code: "HTTP_ERROR", message: `HTTP ${res.status}`, details: {} } };
    return { ok: true, data: data };
  } catch (e) {
    return { ok: false, error: { code: "NETWORK_ERROR", message: String(e), details: {} } };
  }
}

function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}

function selectedNsId() {
  return String(nsState.selectedId || "er").trim() || "er";
}

function selectedNsPrefix() {
  return String(nsState.selectedPrefix || "er").trim() || "er";
}

function withNsQuery(path) {
  const ns = selectedNsId();
  if (!ns) return path;
  if (!path.startsWith("/api/v1/")) return path;
  if (path.includes("ns=")) return path;
  return path.includes("?") ? `${path}&ns=${encodeURIComponent(ns)}` : `${path}?ns=${encodeURIComponent(ns)}`;
}

function setActiveView(name) {
  for (const el of document.querySelectorAll(".nav-item")) {
    el.classList.toggle("active", el.dataset.view === name);
  }
  for (const el of document.querySelectorAll(".view")) {
    el.classList.toggle("active", el.id === `view-${name}`);
  }
  const titleMap = {
    status: "System Status",
    elements: "Element Operations",
    queries: "Find Matching Elements",
    store: "Store Results with Expiry",
    examples: "Examples",
    logs: "Backend Logs (read-only)",
    bitmaps: "Bit-maps",
  };
  $("viewTitle").textContent = titleMap[name] || name;
  clearBanner();
  state.error = null;
  state.locked = false;
  enableAllInputs();
  enableAllButtons();
}

function setActiveTabGroup(tabGroup, tabId) {
  for (const el of document.querySelectorAll(`.tab[data-tabgroup="${tabGroup}"]`)) {
    el.classList.toggle("active", el.dataset.tab === tabId);
  }
  for (const el of document.querySelectorAll(`.tabpane[data-tabgroup="${tabGroup}"]`)) {
    el.classList.toggle("active", el.id === tabId);
  }
}

function setupNav() {
  for (const el of document.querySelectorAll(".nav-item")) {
    el.addEventListener("click", async () => {
      if (state.locked) return;
      setActiveView(el.dataset.view);
      if (el.dataset.view === "bitmaps") {
        await fetchBitmaps();
        renderBitmapsTable();
        renderBitmapsGroups();
      }
	      if (el.dataset.view === "examples") {
	        await withRequest({
	          buttonEl: null,
	          outEl: $("examplesOut"),
	          fn: async () => {
	            await fetchExamples({ force: false });
	            await renderExamples();
	          },
	        });
	      }
	    });
	  }
	}

function setupTabGroups() {
  for (const el of document.querySelectorAll(".tab[data-tabgroup]")) {
    el.addEventListener("click", () => {
      if (state.locked) return;
      setActiveTabGroup(el.dataset.tabgroup, el.dataset.tab);
    });
  }
}

async function fetchBitmaps({ force = false, silent = false } = {}) {
  const ns = selectedNsId();
  if (bitmapsCache && bitmapsCache.ns === ns && !force) return bitmapsCache;
  const res = await apiJson(withNsQuery("/api/v1/bitmaps"));
  if (!res.ok) {
    if (!silent) {
      state.error = { type: "request", message: res?.error?.message || "Request failed" };
      showBanner("error", res?.error?.message || "Request failed");
    }
    return null;
  }
  applyBitmapsResponse(res);
  return bitmapsCache;
}

function bitNameFor(bit) {
  const entry = bitmapsCache?.byBit?.get(bit);
  const name = entry?.name;
  if (typeof name === "string" && name.trim()) return name.trim();
  return `Bit ${bit}`;
}

function applyBitmapsResponse(res) {
  const ns = selectedNsId();
  const byBit = new Map();
  const items = Array.isArray(res?.data?.items) ? res.data.items : [];
  for (const it of items) {
    const bit = Number(it.bit);
    if (Number.isInteger(bit)) byBit.set(bit, it);
  }
  bitmapsCache = { ns, meta: res?.data?.meta || {}, items, byBit, raw: res?.data || {} };

  const doc = res?.data?.document;
  if (doc && typeof doc === "object") {
    bitmapsDoc = doc;
  } else {
    bitmapsDoc = { schema: "er.gui.bitmaps.v1", meta: {}, groups: {}, labels: {}, defaults: {}, items: [], ranges: [] };
  }
}

function getGroupsSorted() {
  const groups = bitmapsDoc?.groups && typeof bitmapsDoc.groups === "object" ? bitmapsDoc.groups : {};
  const out = Object.entries(groups)
    .filter(([id, g]) => typeof id === "string" && id.trim() && g && typeof g === "object")
    .map(([id, g]) => ({
      id: id.trim(),
      label: typeof g.label === "string" ? g.label : "",
      order: Number.isFinite(Number(g.order)) ? Number(g.order) : 0,
      color: typeof g.color === "string" ? g.color : "",
    }));
  out.sort((a, b) => (a.order || 0) - (b.order || 0) || a.id.localeCompare(b.id));
  return out;
}

function groupExists(id) {
  if (!id || typeof id !== "string") return false;
  const groups = bitmapsDoc?.groups && typeof bitmapsDoc.groups === "object" ? bitmapsDoc.groups : {};
  return Object.prototype.hasOwnProperty.call(groups, id);
}

function firstGroupId() {
  const gs = getGroupsSorted();
  return gs.length ? gs[0].id : "";
}

function getDefaultGroupId() {
  const g = bitmapsDoc?.defaults?.group;
  if (typeof g === "string" && g && groupExists(g)) return g;
  return firstGroupId();
}

function populateGroupSelect(selectEl, { value = "", includeEmpty = false } = {}) {
  if (!selectEl) return;
  const gs = getGroupsSorted();
  const opts = [];
  if (includeEmpty) opts.push(`<option value=""></option>`);
  for (const g of gs) {
    const label = g.label ? `${g.label} (${g.id})` : g.id;
    opts.push(`<option value="${escapeHtml(g.id)}">${escapeHtml(label)}</option>`);
  }
  selectEl.innerHTML = opts.join("");
  const v = value && groupExists(value) ? value : getDefaultGroupId();
  if (v) selectEl.value = v;
}

function setBitmapsEditMode(on) {
  bitmapsUi.editMode = !!on;
  $("bitmapsEditOnly")?.classList.toggle("hidden", !bitmapsUi.editMode);
  if (!bitmapsUi.editMode) {
    bitmapsUi.selectedBit = null;
    $("bitmapsItemPanel")?.classList.add("hidden");
  }
  renderBitmapsTable();
  renderBitmapsGroups();
}

function openBitmapsItemEditor(bit) {
  if (!bitmapsUi.editMode) return;
  if (!Number.isInteger(bit) || bit < 0 || bit > 4095) return;
  if (!bitmapsDoc) return;

  bitmapsUi.selectedBit = bit;
  const panel = $("bitmapsItemPanel");
  if (panel) panel.classList.remove("hidden");

  $("bmEditBit").value = String(bit);
  populateGroupSelect($("bmEditGroup"), { value: "" });

  const explicit = Array.isArray(bitmapsDoc.items) ? bitmapsDoc.items.find((it) => Number(it?.bit) === bit) : null;
  const resolved = bitmapsCache?.byBit?.get(bit) || null;
  const src = explicit || resolved || {};

  $("bmEditName").value = typeof src.name === "string" ? src.name : "";
  $("bmEditKey").value = typeof src.key === "string" ? src.key : "";
  $("bmEditDesc").value = typeof src.description === "string" ? src.description : "";

  const desiredGroup = typeof src.group === "string" ? src.group : "";
  populateGroupSelect($("bmEditGroup"), { value: desiredGroup });
}

function closeBitmapsItemEditor() {
  bitmapsUi.selectedBit = null;
  $("bitmapsItemPanel")?.classList.add("hidden");
  $("bmEditBit").value = "";
  $("bmEditName").value = "";
  $("bmEditKey").value = "";
  $("bmEditDesc").value = "";
  renderBitmapsTable();
}

function upsertDocItem(bit, patch) {
  if (!bitmapsDoc) return;
  if (!Array.isArray(bitmapsDoc.items)) bitmapsDoc.items = [];
  const idx = bitmapsDoc.items.findIndex((it) => Number(it?.bit) === bit);
  const next = { bit, ...(idx >= 0 ? bitmapsDoc.items[idx] : {}), ...patch };
  if (!next.name) delete next.name;
  if (!next.key) delete next.key;
  if (!next.description) delete next.description;
  if (!next.group) delete next.group;
  if (idx >= 0) bitmapsDoc.items[idx] = next;
  else bitmapsDoc.items.push(next);
}

async function putBitmapsDoc({ buttonEl, outEl }) {
  if (!bitmapsDoc) throw new Error("No bit-maps document loaded.");
  await withRequest({
    buttonEl,
    outEl,
    fn: async () => {
      const out = await apiJson(withNsQuery("/api/v1/bitmaps"), { method: "PUT", body: JSON.stringify(bitmapsDoc) });
      if (!out.ok) {
        const msg = out?.error?.message || "Request failed";
        state.error = { type: "request", message: msg };
        showBanner("error", msg);
        return;
      }
      state.error = null;
      applyBitmapsResponse(out);
      showBanner("success", "Saved");
      renderBitmapsGroups();
      renderBitmapsTable();
    },
  });
}

function hideMatrixTooltip() {
  const tip = $("matrixTooltip");
  if (!tip) return;
  tip.classList.add("hidden");
  tip.textContent = "";
}

function showMatrixTooltip(text, clientX, clientY) {
  const tip = $("matrixTooltip");
  if (!tip) return;
  tip.textContent = text;
  tip.style.left = `${clientX + 12}px`;
  tip.style.top = `${clientY + 12}px`;
  tip.classList.remove("hidden");
}

function drawMatrix() {
  const canvas = $("matrixCanvas");
  const meta = $("matrixMeta");
  if (!canvas || !(canvas instanceof HTMLCanvasElement)) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const cols = 64;
  const cell = canvas.width / cols;

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = "rgba(4, 9, 20, 0.65)";
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const bitSet = matrixState.bitSet || new Set();
  ctx.fillStyle = "rgba(109, 167, 255, 0.85)";
  for (const bit of bitSet) {
    if (!Number.isInteger(bit) || bit < 0 || bit > 4095) continue;
    const x = bit % cols;
    const y = Math.floor(bit / cols);
    ctx.fillRect(x * cell, y * cell, cell, cell);
  }

  if (meta) {
    const count = bitSet.size;
    meta.textContent = `Found: ${count} set bits`;
  }
}

function setupMatrixHover() {
  const canvas = $("matrixCanvas");
  if (!canvas || !(canvas instanceof HTMLCanvasElement)) return;

  canvas.addEventListener("mouseleave", () => hideMatrixTooltip());
  canvas.addEventListener("mousemove", (ev) => {
    if (!matrixState.bitSet) return;

    const rect = canvas.getBoundingClientRect();
    const relX = ev.clientX - rect.left;
    const relY = ev.clientY - rect.top;
    if (relX < 0 || relY < 0 || relX >= rect.width || relY >= rect.height) {
      hideMatrixTooltip();
      return;
    }

    const size = 64;
    const x = Math.floor((relX / rect.width) * size);
    const y = Math.floor((relY / rect.height) * size);
    const bit = y * size + x;
    if (bit < 0 || bit > 4095) {
      hideMatrixTooltip();
      return;
    }

    const value = matrixState.bitSet.has(bit) ? 1 : 0;
    const name = bitNameFor(bit);
    showMatrixTooltip(`${name}: ${value}`, ev.clientX, ev.clientY);
  });
}

function renderBitmapsTable() {
  const tbody = $("bitmapsTbody");
  const metaEl = $("bitmapsMeta");
  if (!tbody || !metaEl) return;

  const q = String($("bitmapsSearch")?.value || "").trim().toLowerCase();
  if (!bitmapsCache) {
    metaEl.textContent = "No bit-maps loaded.";
    tbody.innerHTML = "";
    return;
  }

  const items = bitmapsCache.items || [];
  const filtered = items.filter((it) => {
    if (!q) return true;
    const hay = [
      String(it.bit ?? ""),
      String(it.key ?? ""),
      String(it.name ?? ""),
      String(it.group ?? ""),
      String(it.description ?? ""),
    ]
      .join(" ")
      .toLowerCase();
    return hay.includes(q);
  });

  filtered.sort((a, b) => Number(a.bit) - Number(b.bit));

  const meta = bitmapsCache.meta || {};
  const missing = meta.missing ? " (missing)" : "";
  const schema = String(bitmapsCache.raw?.schema || "").trim();
  const preset = String(meta.preset || "").trim();
  const ns = String(meta.ns || bitmapsCache.ns || "").trim();
  const prefix = String(meta.prefix || selectedNsPrefix() || "").trim();
  const parts = [`Loaded: ${filtered.length}/${items.length}${missing}`];
  if (preset) parts.push(`Preset: ${preset}`);
  if (ns) parts.push(`NS: ${ns}${prefix ? ` (${prefix}:*)` : ""}`);
  if (schema) parts.push(`Schema: ${schema}`);
  metaEl.textContent = parts.join(" • ");

  tbody.innerHTML = filtered
    .map((it) => {
      const bit = Number(it.bit);
      const key = it.key ? `<code>${escapeHtml(String(it.key))}</code>` : "";
      const name = it.name ? escapeHtml(String(it.name)) : "";
      const group = it.group ? escapeHtml(String(it.group)) : "";
      const desc = it.description ? escapeHtml(String(it.description)) : "";
      const clickable = bitmapsUi.editMode ? "clickable" : "";
      const selected = bitmapsUi.editMode && bitmapsUi.selectedBit === bit ? "selected" : "";
      return `<tr class="${clickable} ${selected}" data-bit="${bit}"><td>${bit}</td><td>${key}</td><td>${name}</td><td>${group}</td><td>${desc}</td></tr>`;
    })
    .join("");
}

function renderBitmapsGroups() {
  const groupsTbody = $("bmGroupsTbody");
  const defaultSel = $("bmDefaultGroup");
  const itemGroupSel = $("bmEditGroup");
  if (!groupsTbody || !defaultSel || !itemGroupSel) return;

  populateGroupSelect(defaultSel, { value: getDefaultGroupId(), includeEmpty: false });
  populateGroupSelect(itemGroupSel, { value: itemGroupSel.value || getDefaultGroupId(), includeEmpty: false });

  const gs = getGroupsSorted();
  if (!gs.length) {
    groupsTbody.innerHTML = `<tr><td colspan="5" class="muted">No groups defined.</td></tr>`;
    return;
  }

  groupsTbody.innerHTML = gs
    .map((g) => {
      const id = escapeHtml(g.id);
      const label = escapeHtml(g.label || "");
      const order = Number.isFinite(g.order) ? String(g.order) : "";
      const color = escapeHtml(g.color || "");
      return (
        `<tr data-group="${id}">` +
        `<td><code>${id}</code></td>` +
        `<td>${label}</td>` +
        `<td>${order}</td>` +
        `<td>${color}</td>` +
        `<td>` +
        `<button class="btn" data-action="edit">Edit</button> ` +
        `<button class="btn danger" data-action="delete">Delete</button>` +
        `</td>` +
        `</tr>`
      );
    })
    .join("");
}

function resetGroupForm() {
  bitmapsUi.editingGroupOldId = null;
  $("bmGroupId").value = "";
  $("bmGroupLabel").value = "";
  $("bmGroupOrder").value = "";
  $("bmGroupColor").value = "";
}

function readGroupForm() {
  const id = String($("bmGroupId").value || "").trim();
  const label = String($("bmGroupLabel").value || "").trim();
  const order = normalizeInt($("bmGroupOrder").value, "Order");
  const color = String($("bmGroupColor").value || "").trim();
  if (!id) throw new Error("This field is required.");
  if (!/^[A-Za-z0-9_.-]+$/.test(id)) throw new Error("Invalid value.");
  if (!label) throw new Error("This field is required.");
  return { id, label, order, color };
}

async function saveDefaultGroup() {
  if (!bitmapsDoc) return;
  const next = String($("bmDefaultGroup").value || "").trim();
  if (!next || !groupExists(next)) {
    const msg = "Invalid value.";
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    return;
  }
  if (!bitmapsDoc.defaults || typeof bitmapsDoc.defaults !== "object") bitmapsDoc.defaults = {};
  bitmapsDoc.defaults.group = next;
  await putBitmapsDoc({ buttonEl: $("btnBmDefaultGroupSave"), outEl: $("bitmapsMeta") });
}

function renameGroupRefs(oldId, newId) {
  if (!bitmapsDoc) return;
  if (bitmapsDoc.defaults?.group === oldId) bitmapsDoc.defaults.group = newId;
  if (Array.isArray(bitmapsDoc.items)) {
    for (const it of bitmapsDoc.items) {
      if (it && typeof it === "object" && it.group === oldId) it.group = newId;
    }
  }
  if (Array.isArray(bitmapsDoc.ranges)) {
    for (const r of bitmapsDoc.ranges) {
      if (r && typeof r === "object" && r.group === oldId) r.group = newId;
    }
  }
}

async function saveGroupFromForm() {
  if (!bitmapsDoc) return;
  const { id, label, order, color } = readGroupForm();

  if (!bitmapsDoc.groups || typeof bitmapsDoc.groups !== "object") bitmapsDoc.groups = {};

  const oldId = bitmapsUi.editingGroupOldId;
  if (oldId && oldId !== id) {
    if (groupExists(id)) throw new Error("Invalid value.");
    const g = bitmapsDoc.groups[oldId];
    delete bitmapsDoc.groups[oldId];
    bitmapsDoc.groups[id] = g;
    renameGroupRefs(oldId, id);
  }

  bitmapsDoc.groups[id] = { label, order, ...(color ? { color } : {}) };
  if (!bitmapsDoc.defaults || typeof bitmapsDoc.defaults !== "object") bitmapsDoc.defaults = {};
  if (!bitmapsDoc.defaults.group || !groupExists(bitmapsDoc.defaults.group)) bitmapsDoc.defaults.group = id;

  await putBitmapsDoc({ buttonEl: $("btnBmGroupSave"), outEl: $("bitmapsMeta") });
  resetGroupForm();
}

async function deleteGroupById(id) {
  if (!bitmapsDoc) return;
  if (!id || !groupExists(id)) return;

  const okDel = window.confirm(`Delete group "${id}"?`);
  if (!okDel) return;

  const groups = bitmapsDoc.groups || {};
  const remainingKeys = Object.keys(groups).filter((k) => k !== id);
  const hasItems = Array.isArray(bitmapsDoc.items) && bitmapsDoc.items.length > 0;
  const hasRanges = Array.isArray(bitmapsDoc.ranges) && bitmapsDoc.ranges.length > 0;
  if (remainingKeys.length === 0 && (hasItems || hasRanges)) {
    const msg = "Invalid value.";
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    return;
  }

  delete groups[id];

  const defaultId = getDefaultGroupId();
  const nextDefault = groupExists(defaultId) ? defaultId : firstGroupId();
  if (!bitmapsDoc.defaults || typeof bitmapsDoc.defaults !== "object") bitmapsDoc.defaults = {};
  bitmapsDoc.defaults.group = nextDefault || "";

  const reassign = nextDefault || "";
  if (Array.isArray(bitmapsDoc.items)) {
    for (const it of bitmapsDoc.items) {
      if (it && typeof it === "object" && it.group === id) it.group = reassign;
    }
  }
  if (Array.isArray(bitmapsDoc.ranges)) {
    for (const r of bitmapsDoc.ranges) {
      if (r && typeof r === "object" && r.group === id) r.group = reassign;
    }
  }

  await putBitmapsDoc({ buttonEl: null, outEl: $("bitmapsMeta") });
  resetGroupForm();
}

function parseBulkAssign(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const assignments = new Map(); // bit -> groupId
  const duplicates = [];
  const invalidTokens = [];
  const invalidGroups = [];

  for (const line of lines) {
    const idx = line.indexOf(":");
    if (idx <= 0) {
      invalidTokens.push(line);
      continue;
    }
    const group = line.slice(0, idx).trim();
    const rhs = line.slice(idx + 1).trim();
    if (!group || !groupExists(group)) {
      invalidGroups.push(group || line);
      continue;
    }
    if (!rhs) continue;

    const tokens = rhs.split(/[\s,]+/).filter(Boolean);
    for (const tok of tokens) {
      const m = tok.match(/^(\d+)(?:-|\.\.)(\d+)$/);
      if (m) {
        const a = Number(m[1]);
        const b = Number(m[2]);
        if (!Number.isInteger(a) || !Number.isInteger(b)) {
          invalidTokens.push(tok);
          continue;
        }
        const lo = Math.min(a, b);
        const hi = Math.max(a, b);
        for (let bit = lo; bit <= hi; bit++) {
          if (bit < 0 || bit > 4095) {
            invalidTokens.push(String(bit));
            continue;
          }
          if (assignments.has(bit)) duplicates.push(bit);
          else assignments.set(bit, group);
        }
        continue;
      }

      if (!/^\d+$/.test(tok)) {
        invalidTokens.push(tok);
        continue;
      }
      const bit = Number(tok);
      if (!Number.isInteger(bit) || bit < 0 || bit > 4095) {
        invalidTokens.push(tok);
        continue;
      }
      if (assignments.has(bit)) duplicates.push(bit);
      else assignments.set(bit, group);
    }
  }

  return { assignments, duplicates, invalidTokens, invalidGroups };
}

function writeBulkSummary(summary) {
  const outEl = $("bmBulkOut");
  if (!outEl) return;
  outEl.textContent = summary;
}

async function applyBulkAssign() {
  if (!bitmapsDoc) return;
  const { assignments, duplicates, invalidTokens, invalidGroups } = parseBulkAssign($("bmBulkText").value || "");
  const autoname = !!$("bmBulkAutoname").checked;

  if (invalidGroups.length || invalidTokens.length) {
    const msg = "Invalid value. See Summary.";
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
  }

  const items = Array.isArray(bitmapsDoc.items) ? bitmapsDoc.items : [];
  const explicitByBit = new Map(items.map((it) => [Number(it.bit), it]));
  let created = 0;
  let updated = 0;

  const prefix = typeof bitmapsDoc.labels?.UNNAMED_PREFIX === "string" && bitmapsDoc.labels.UNNAMED_PREFIX.trim()
    ? bitmapsDoc.labels.UNNAMED_PREFIX.trim()
    : "Bit";

  for (const [bit, group] of assignments.entries()) {
    const existing = explicitByBit.get(bit);
    if (existing) {
      if (existing.group !== group) {
        existing.group = group;
        updated++;
      }
      continue;
    }
    const next = { bit, group };
    if (autoname) next.name = `${prefix} ${bit}`;
    items.push(next);
    created++;
  }
  bitmapsDoc.items = items;

  writeBulkSummary(
    [
      `Applied: ${assignments.size}`,
      `Created: ${created}`,
      `Updated: ${updated}`,
      `Duplicates ignored: ${duplicates.length}`,
      `Invalid groups: ${invalidGroups.length}`,
      `Invalid tokens: ${invalidTokens.length}`,
      invalidGroups.length ? `\nInvalid groups:\n${invalidGroups.slice(0, 50).join("\n")}` : "",
      invalidTokens.length ? `\nInvalid tokens:\n${invalidTokens.slice(0, 50).join("\n")}` : "",
    ]
      .filter(Boolean)
      .join("\n")
  );

  if (!assignments.size) return;
  await putBitmapsDoc({ buttonEl: $("btnBmBulkApply"), outEl: $("bmBulkOut") });
}

function escapeHtml(s) {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderEnvelope(outEl, env) {
  if (!env || typeof env !== "object") {
    outEl.textContent = "Invalid response";
    return;
  }
  if (env.ok) {
    outEl.textContent = pretty(env);
    return;
  }
  const code = env?.error?.code || "ERROR";
  const message = env?.error?.message || "Request failed";
  const details = env?.error?.details || {};
  outEl.textContent = `${code}\n${message}\n\nDetails:\n${pretty(details)}`;
}

function formatBytes(bytes) {
  if (bytes == null) return "n/a";
  const n = Number(bytes);
  if (!Number.isFinite(n)) return "n/a";
  const mb = n / (1024 * 1024);
  if (mb < 10) return `${mb.toFixed(1)} MB`;
  return `${mb.toFixed(0)} MB`;
}

function downloadJson(filename, obj) {
  const text = JSON.stringify(obj, null, 2) + "\n";
  const blob = new Blob([text], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function renderNsDiscover() {
  const meta = $("nsDiscoverMeta");
  const tbody = $("nsDiscoverTbody");
  if (!meta || !tbody) return;
  if (!nsDiscoverCache) {
    meta.textContent = "";
    tbody.innerHTML = "";
    return;
  }

  meta.textContent = `Seen keys: ${nsDiscoverCache.seen_keys} • Prefixes: ${(nsDiscoverCache.prefixes || []).length}`;

  const prefixes = Array.isArray(nsDiscoverCache.prefixes) ? nsDiscoverCache.prefixes : [];
  tbody.innerHTML = prefixes
    .map((p) => {
      const prefix = escapeHtml(String(p.prefix || ""));
      const conf = Number(p.confidence ?? 0);
      const pct = `${Math.round(conf * 100)}%`;
      const total = p?.evidence?.counts?.total_keys ?? "";
      const patterns = p?.evidence?.counts?.patterns || {};
      const ev =
        `element:${patterns.element ?? 0} ` +
        `idx_bit:${patterns.idx_bit ?? 0} ` +
        `tmp:${patterns.tmp ?? 0} ` +
        `universe:${patterns.universe ?? 0}`;
      const tpl = p?.suggested_layout?.key_templates || null;
      const layout = tpl ? `<code>${escapeHtml(`${tpl.element}`)}</code>` : `<span class="muted">unknown</span>`;
      return `<tr><td><code>${prefix}</code></td><td>${pct}</td><td>${escapeHtml(String(total))}</td><td>${escapeHtml(ev)}</td><td>${layout}</td></tr>`;
    })
    .join("");
}

async function fetchExamples({ force = false } = {}) {
  if (examplesCache && !force) return examplesCache;
  const out = await apiJson("/api/v1/examples");
  if (!out.ok) return null;
  examplesCache = out.data;
  if (force) examplesReadmeCache.clear();
  return examplesCache;
}

async function fetchExampleReadme(id, { force = false } = {}) {
  const exId = String(id || "").trim();
  if (!exId) return null;
  if (examplesReadmeCache.has(exId) && !force) {
    return { ok: true, data: { id: exId, readme: examplesReadmeCache.get(exId) } };
  }
  const out = await apiJson(`/api/v1/examples/${encodeURIComponent(exId)}/readme`);
  if (out.ok) {
    const md = String(out?.data?.readme ?? "");
    examplesReadmeCache.set(exId, md);
  }
  return out;
}

function envelopeToText(env) {
  if (!env || typeof env !== "object") return "Invalid response";
  if (env.ok) return pretty(env);
  const code = env?.error?.code || "ERROR";
  const message = env?.error?.message || "Request failed";
  const details = env?.error?.details || {};
  return `${code}\n${message}\n\nDetails:\n${pretty(details)}`;
}

function isSafeHref(href) {
  const h = String(href || "").trim();
  if (!h) return false;
  if (h.startsWith("#") || h.startsWith("/")) return true;
  return h.startsWith("http://") || h.startsWith("https://");
}

function renderInlineMd(raw) {
  const codes = [];
  const links = [];
  let s = String(raw || "");
  s = s.replace(/`([^`]+)`/g, (_, code) => {
    const idx = codes.length;
    codes.push(code);
    return `@@CODE${idx}@@`;
  });
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, text, url) => {
    const idx = links.length;
    links.push({ text, url });
    return `@@LINK${idx}@@`;
  });
  s = escapeHtml(s);
  s = s.replace(/@@LINK(\d+)@@/g, (_, idxStr) => {
    const idx = Number(idxStr);
    const ent = links[idx] || { text: "", url: "" };
    const href = String(ent.url || "").trim();
    const label = String(ent.text || "").trim() || href;
    if (!isSafeHref(href)) return escapeHtml(label);
    return `<a href="${escapeHtml(href)}" target="_blank" rel="noreferrer noopener">${escapeHtml(label)}</a>`;
  });
  s = s.replace(/@@CODE(\d+)@@/g, (_, idxStr) => {
    const idx = Number(idxStr);
    const code = codes[idx] ?? "";
    return `<code>${escapeHtml(String(code))}</code>`;
  });
  return s;
}

function renderMarkdown(md) {
  const src = String(md ?? "").replaceAll("\r\n", "\n");
  if (!src.trim()) return `<div class="muted">No README available.</div>`;

  const lines = src.split("\n");
  const out = [];
  let inFence = false;
  let fenceLines = [];
  let para = [];
  let listItems = [];

  function flushPara() {
    if (!para.length) return;
    const text = para.join(" ").trim();
    if (text) out.push(`<p>${renderInlineMd(text)}</p>`);
    para = [];
  }

  function flushList() {
    if (!listItems.length) return;
    out.push(`<ul>${listItems.map((t) => `<li>${renderInlineMd(t)}</li>`).join("")}</ul>`);
    listItems = [];
  }

  for (const line of lines) {
    const fence = line.trim().startsWith("```");
    if (fence) {
      if (inFence) {
        out.push(`<pre><code>${escapeHtml(fenceLines.join("\n"))}</code></pre>`);
        fenceLines = [];
      } else {
        flushPara();
        flushList();
      }
      inFence = !inFence;
      continue;
    }
    if (inFence) {
      fenceLines.push(line);
      continue;
    }

    const h = line.match(/^(#{1,6})\s+(.*)$/);
    if (h) {
      flushPara();
      flushList();
      const level = Math.min(6, h[1].length);
      const text = (h[2] || "").trim();
      out.push(`<h${level}>${renderInlineMd(text)}</h${level}>`);
      continue;
    }

    const li = line.match(/^\s*[-*]\s+(.*)$/);
    if (li) {
      flushPara();
      listItems.push((li[1] || "").trim());
      continue;
    }

    if (!line.trim()) {
      flushPara();
      flushList();
      continue;
    }

    para.push(line.trim());
  }

  flushPara();
  flushList();
  if (inFence && fenceLines.length) out.push(`<pre><code>${escapeHtml(fenceLines.join("\n"))}</code></pre>`);
  return out.join("");
}

function getSelectedExample() {
  const list = Array.isArray(examplesCache?.examples) ? examplesCache.examples : [];
  const exSel = $("examplesSelect");
  const exId = String(exSel?.value || "").trim();
  if (!list.length) return null;
  const first = list[0];
  return list.find((e) => e.id === exId) || first;
}

function clearExamplesReports() {
  const rowWrap = $("examplesRowCountsWrap");
  const totalsWrap = $("examplesTotalsWrap");
  const rowTbody = $("examplesRowCountsTbody");
  const totalsTbody = $("examplesTotalsTbody");
  if (rowWrap) rowWrap.classList.add("hidden");
  if (totalsWrap) totalsWrap.classList.add("hidden");
  if (rowTbody) rowTbody.innerHTML = "";
  if (totalsTbody) totalsTbody.innerHTML = "";
}

function setExamplesMode(selected) {
  const runBtn = $("btnExamplesRun");
  const compareBtn = $("btnExamplesCompare");
  if (!runBtn || !compareBtn) return;
  const type = String(selected?.type || "seed");
  runBtn.textContent = type === "dataset_compare" ? "Run Import" : "Load into Redis";
  compareBtn.classList.toggle("hidden", type !== "dataset_compare");
  compareBtn.disabled = type !== "dataset_compare";
  clearExamplesReports();
}

async function updateExamplesMetaAndReadme({ forceReadme = false } = {}) {
  const meta = $("examplesMeta");
  const readmeEl = $("examplesReadme");
  const selected = getSelectedExample();
  if (!meta || !readmeEl) return;
  if (!selected) {
    meta.textContent = "";
    readmeEl.innerHTML = `<div class="muted">No README available.</div>`;
    return;
  }

  const type = String(selected.type || "seed");
  const base =
    `Active example: ${selected.title} (${selected.id})\n` +
    `${selected.description}\n` +
    `Type: ${type}`;
  if (type === "dataset_compare") {
    const targetNs = Array.isArray(selected.targets) && selected.targets.length ? String(selected.targets[0]?.ns || "") : "";
    meta.textContent = `${base}\nTarget namespace: ${targetNs || "n/a"}`;
  } else {
    meta.textContent = `${base}\nDefault namespace: ${selected.namespace || "n/a"} • Estimated elements: ${selected.element_count_estimate ?? "n/a"}`;
  }

  readmeEl.innerHTML = `<div class="muted">Loading README...</div>`;
  const out = await fetchExampleReadme(selected.id, { force: forceReadme });
  if (!out) {
    readmeEl.innerHTML = `<div class="muted">No README available.</div>`;
    return;
  }
  if (!out.ok) {
    readmeEl.innerHTML = `<pre><code>${escapeHtml(envelopeToText(out))}</code></pre>`;
    return;
  }
  readmeEl.innerHTML = renderMarkdown(out?.data?.readme || "");
}

async function renderExamples({ forceReadme = false } = {}) {
  const nsSel = $("examplesNs");
  const exSel = $("examplesSelect");
  const outEl = $("examplesOut");
  const links = $("examplesLinks");
  if (!nsSel || !exSel || !outEl || !links) return;

  nsSel.innerHTML = (nsState.options || [])
    .map((o) => `<option value="${escapeHtml(o.id)}">${escapeHtml(`${o.label} (${o.prefix}:*)`)}</option>`)
    .join("");
  nsSel.value = nsState.selectedId;

  const list = Array.isArray(examplesCache?.examples) ? examplesCache.examples : [];
  const prevId = String(exSel.value || "").trim();
  exSel.innerHTML = list
    .map((e) => `<option value="${escapeHtml(e.id)}">${escapeHtml(`${e.title} (${e.id})`)}</option>`)
    .join("");
  if (prevId && list.some((e) => e.id === prevId)) exSel.value = prevId;

  if (!list.length) {
    outEl.textContent = "No examples available.";
    links.innerHTML = "";
    const meta = $("examplesMeta");
    const readmeEl = $("examplesReadme");
    if (meta) meta.textContent = "";
    if (readmeEl) readmeEl.innerHTML = `<div class="muted">No README available.</div>`;
    return;
  }
  const first = list[0];
  if (!String(exSel.value || "").trim()) exSel.value = first.id;
  const selected = getSelectedExample();
  if (String(selected?.type || "seed") === "dataset_compare") {
    outEl.textContent = "Click “Run Import”, then “Compare”.";
  } else {
    outEl.textContent = "Click “Load into Redis”.";
  }
  links.innerHTML = "";
  setExamplesMode(selected);
  if (String(selected?.type || "seed") === "dataset_compare") {
    const targetNs = Array.isArray(selected.targets) && selected.targets.length ? String(selected.targets[0]?.ns || "") : "";
    if (targetNs && (nsState.options || []).some((o) => o.id === targetNs)) nsSel.value = targetNs;
  }
  await updateExamplesMetaAndReadme({ forceReadme });
}

function renderExamplesRunResult(res) {
  const outEl = $("examplesOut");
  const links = $("examplesLinks");
  if (!outEl || !links) return;
  if (!res || !res.ok) {
    renderEnvelope(outEl, res);
    links.innerHTML = "";
    clearExamplesReports();
    return;
  }
  const d = res.data || {};
  clearExamplesReports();

  const type = String(d.type || "");
  if (type === "dataset_compare") {
    outEl.textContent =
      `Example: ${d.id}\n` +
      `Namespace: ${d.ns}\n` +
      `Imported tables: ${(d.imported_tables || []).length}\n` +
      `Elapsed: ${d.elapsed_ms ?? "n/a"} ms\n` +
      (d.reset ? `\nReset:\n- scanned: ${d.reset.scanned}\n- deleted_objects: ${d.reset.deleted_objects}\n` : "");
    links.innerHTML = "";
    return;
  }

  const trunc = d.samples_truncated || {};
  const truncNote =
    trunc.created || trunc.updated || trunc.skipped
      ? "\n\nNote: name lists are samples (truncated to keep the UI responsive)."
      : "";
  outEl.textContent =
    `Example: ${d.id}\n` +
    `Namespace: ${d.ns}\n` +
    `Created: ${d.counts?.created ?? 0}\n` +
    `Updated: ${d.counts?.updated ?? 0}\n` +
    `Skipped: ${d.counts?.skipped ?? 0}\n` +
    (d.reset ? `\nReset:\n- scanned: ${d.reset.scanned}\n- deleted: ${d.reset.deleted}\n- skipped: ${d.reset.skipped}\n` : "") +
    truncNote;

  const names = [...(d.created || []), ...(d.updated || [])].slice(0, 20);
  if (!names.length) {
    links.innerHTML = "";
    return;
  }
  links.innerHTML = names
    .map((n) => `<button class="btn" data-name="${escapeHtml(n)}">Open ${escapeHtml(n)}</button>`)
    .join("");
}

function renderExamplesReports(res) {
  const rowWrap = $("examplesRowCountsWrap");
  const totalsWrap = $("examplesTotalsWrap");
  const rowTbody = $("examplesRowCountsTbody");
  const totalsTbody = $("examplesTotalsTbody");
  if (!rowWrap || !totalsWrap || !rowTbody || !totalsTbody) return;

  clearExamplesReports();

  if (!res || !res.ok) {
    return;
  }
  const d = res.data || {};
  const reports = d.reports || {};

  const rowCounts = Array.isArray(reports.row_counts) ? reports.row_counts : [];
  const totals = Array.isArray(reports.order_totals_sample) ? reports.order_totals_sample : [];

  if (rowCounts.length) {
    rowTbody.innerHTML = rowCounts
      .map((r) => {
        const table = escapeHtml(String(r.table ?? ""));
        const sc = escapeHtml(String(r.sqlite_count ?? ""));
        const rc = escapeHtml(String(r.redis_count ?? ""));
        const match = !!r.match;
        const m = match ? "OK" : "Mismatch";
        return `<tr><td><code>${table}</code></td><td>${sc}</td><td>${rc}</td><td>${m}</td></tr>`;
      })
      .join("");
    rowWrap.classList.remove("hidden");
  }

  if (totals.length) {
    totalsTbody.innerHTML = totals
      .map((r) => {
        const oid = escapeHtml(String(r.order_id ?? ""));
        const st = escapeHtml(String(r.sqlite_total ?? ""));
        const rt = escapeHtml(String(r.redis_total ?? ""));
        const diff = escapeHtml(String(r.diff ?? ""));
        return `<tr><td><code>${oid}</code></td><td>${st}</td><td>${rt}</td><td>${diff}</td></tr>`;
      })
      .join("");
    totalsWrap.classList.remove("hidden");
  }
}

async function loadNamespaces() {
  const selectEl = $("nsSelect");
  if (!selectEl) return;

  const res = await apiJson("/api/v1/namespaces");
  if (!res.ok) {
    showBanner("error", "⚠️ Unable to load namespaces. Using safe default.");
    nsState.doc = { schema: "er.gui.namespaces.v1", default: "er", namespaces: [{ id: "er", label: "Element-Redis", prefix: "er" }] };
  } else {
    nsState.doc = res.data;
  }

  const list = Array.isArray(nsState.doc?.namespaces) ? nsState.doc.namespaces : [];
  const def = String(nsState.doc?.default || "er").trim() || "er";

  const opts = list
    .filter((x) => x && typeof x === "object" && typeof x.id === "string" && typeof x.prefix === "string")
    .map((x) => ({ id: String(x.id).trim(), label: String(x.label || x.id).trim(), prefix: String(x.prefix).trim() }))
    .filter((x) => x.id && x.prefix);

  if (!opts.length) opts.push({ id: "er", label: "Element-Redis", prefix: "er" });
  nsState.options = opts;

  const saved = String(localStorage.getItem("er_gui_ns") || "").trim();
  const selected = opts.some((o) => o.id === saved) ? saved : (opts.some((o) => o.id === def) ? def : opts[0].id);

  selectEl.innerHTML = opts
    .map((o) => `<option value="${escapeHtml(o.id)}">${escapeHtml(`${o.label} (${o.prefix}:*)`)}</option>`)
    .join("");
  selectEl.value = selected;

  const ent = opts.find((o) => o.id === selected) || opts[0];
  nsState.selectedId = ent.id;
  nsState.selectedLabel = ent.label;
  nsState.selectedPrefix = ent.prefix;
}

async function loadConfig() {
  const cfg = await apiJson("/api/v1/config");
  if (!cfg.ok) {
    showBanner("error", "⚠️ Unable to load config. Using safe defaults.");
    state.error = { type: "request", message: "Unable to load config." };
    return;
  }

  runtimeConfig.ttlMaxSec = Number(cfg.data.ttl_max_sec ?? runtimeConfig.ttlMaxSec) || runtimeConfig.ttlMaxSec;
  runtimeConfig.defaultLimit = Number(cfg.data.default_limit ?? runtimeConfig.defaultLimit) || runtimeConfig.defaultLimit;
  runtimeConfig.maxQueryLimit =
    Number(cfg.data.max_query_limit ?? runtimeConfig.maxQueryLimit) || runtimeConfig.maxQueryLimit;
  runtimeConfig.storePreviewLimit =
    Number(cfg.data.store_preview_limit ?? runtimeConfig.storePreviewLimit) || runtimeConfig.storePreviewLimit;
  runtimeConfig.erPrefix = String(cfg.data.er_prefix ?? runtimeConfig.erPrefix) || runtimeConfig.erPrefix;
  runtimeConfig.backendVersion = String(cfg.data.backend_version ?? runtimeConfig.backendVersion) || runtimeConfig.backendVersion;

  const defaultLimit = String(runtimeConfig.defaultLimit);
  for (const id of ["getLimit", "queryLimit", "inspectLimit", "logsTail"]) {
    const el = $(id);
    if (el && !String(el.value || "").trim()) el.value = defaultLimit;
  }
}

async function refreshHealth() {
  await withRequest({
    buttonEl: $("btnStatusRefresh"),
    outEl: $("backendStatus"),
    fn: async () => {
      const h = await apiJson("/api/v1/health");
      if (!h.ok) {
        showBanner("error", "⚠️ Unable to connect to backend. Please ensure the sandbox is running.");
        state.error = { type: "request", message: "Unable to connect to backend." };
        renderEnvelope($("backendStatus"), h);
        $("redisStatus").textContent = "";
        return;
      }

      const r = h.data.redis || {};
      const backendOk = h.ok ? "✅" : "❌";
      const redisOk = r.ok ? "✅" : "❌";

      $("backendStatus").textContent =
        `Backend: ${backendOk}\n` +
        `Redis: ${redisOk}\n` +
        `Ping: ${r.ping_ms} ms\n` +
        `Memory Used: ${formatBytes(r.used_memory)}\n` +
        `Namespace: ${nsState.selectedId} (${selectedNsPrefix()}:*)\n` +
        `Backend Version: v${h.data.backend_version}\n` +
        `Preset: ${h.data.preset}\n`;

      $("redisStatus").textContent = "";
      state.error = null;
    },
  });
}

async function doPut() {
  let name;
  let bits;
  try {
    name = normalizeName($("putName").value);
    bits = normalizeBits($("putBits").value);
    if (bits.length === 0) throw new Error("This field is required.");
  } catch (e) {
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    if (msg.includes("between 0 and 4095")) showBanner("error", "❌ Bit value must be between 0 and 4095.");
    else showBanner("error", msg);
    renderEnvelope($("putOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnPut"),
    outEl: $("putOut"),
    fn: async () => {
      const out = await apiJson(withNsQuery("/api/v1/elements/put"), {
        method: "POST",
        body: JSON.stringify({ name, bits }),
      });
      if (!out.ok) {
        const msg = out?.error?.message || "Request failed";
        state.error = { type: "request", message: msg };
        showBanner("error", msg);
        renderEnvelope($("putOut"), out);
        return;
      }
      state.error = null;
      showBanner("success", `✅ Element saved. ${out.data.written_bits} bits written.`);
      $("putOut").textContent = `Saved: ${out.data.name}\nBits written: ${out.data.written_bits}\n`;
    },
  });
}

let lastGetBits = [];

async function doGet() {
  let name;
  let limit;
  try {
    name = normalizeName($("getName").value);
    limit = readLimitOrDefault($("getLimit"), "Limit", 4096, runtimeConfig.defaultLimit);
  } catch (e) {
    lastGetBits = [];
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    renderEnvelope($("getOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnGet"),
    outEl: $("getOut"),
    fn: async () => {
      const out = await apiJson(
        withNsQuery(`/api/v1/elements/get?name=${encodeURIComponent(name)}&limit=${encodeURIComponent(limit)}`)
      );
      if (!out.ok) {
        lastGetBits = [];
        state.error = { type: "request", message: out?.error?.message || "Request failed" };
        if (out?.error?.code === "NOT_FOUND") showBanner("error", "⚠️ No element found with that name.");
        else showBanner("error", out?.error?.message || "Request failed");
        renderEnvelope($("getOut"), out);
        return;
      }
      state.error = null;
      lastGetBits = out.data.bits || [];
      const bitsText = `[${lastGetBits.join(", ")}]`;
      $("getOut").textContent =
        `Bits: ${bitsText}\n` +
        `Found: ${out.data.count} bits\n` +
        `Returned: ${out.data.returned}/${out.data.limit}\n`;
    },
  });
}

async function doQuery() {
  let limit;
  let body;
  try {
    limit = readLimitOrDefault($("queryLimit"), "Limit", runtimeConfig.maxQueryLimit, runtimeConfig.defaultLimit);
    const active = document.querySelector('.tab[data-tabgroup="queries"].active')?.dataset?.tab || "q-find";

    body = { limit };
    if (active === "q-find") {
      body.type = "find";
      const bit = normalizeInt($("qBit").value, "Bit");
      if (bit < 0 || bit > 4095) throw new Error("Must be between 0 and 4095.");
      body.bit = bit;
    } else if (active === "q-find-all") {
      body.type = "find_all";
      body.bits = normalizeBits($("qBitsAll").value);
      if (body.bits.length < 2) throw new Error("Invalid value.");
    } else if (active === "q-find-any") {
      body.type = "find_any";
      body.bits = normalizeBits($("qBitsAny").value);
      if (body.bits.length < 2) throw new Error("Invalid value.");
    } else if (active === "q-find-not") {
      body.type = "find_not";
      const include = normalizeInt($("qIncludeBit").value, "Include Bit");
      if (include < 0 || include > 4095) throw new Error("Must be between 0 and 4095.");
      body.include_bit = include;
      body.exclude_bits = normalizeBits($("qExcludeBits").value);
      if (body.exclude_bits.length < 1) throw new Error("Invalid value.");
    } else {
      body.type = "find_universe_not";
      body.exclude_bits = normalizeBits($("qExcludeUniverseBits").value);
      if (body.exclude_bits.length < 1) throw new Error("Invalid value.");
    }
  } catch (e) {
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    if (msg.includes("between 0 and 4095")) showBanner("error", "❌ One or more bits are out of the 0–4095 range.");
    else showBanner("error", msg);
    $("queryNames").textContent = "";
    renderEnvelope($("queryOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnQuery"),
    outEl: $("queryOut"),
    fn: async () => {
      const out = await apiJson(withNsQuery("/api/v1/query"), { method: "POST", body: JSON.stringify(body) });
      if (!out.ok) {
        const msg = out?.error?.message || "Request failed";
        state.error = { type: "request", message: msg };
        showBanner("error", msg);
        $("queryNames").textContent = "";
        renderEnvelope($("queryOut"), out);
        return;
      }

      state.error = null;
      const count = out.data.count ?? 0;
      const returned = out.data.returned ?? 0;
      const lim = out.data.limit ?? limit;

      if (count === 0) showBanner("error", "No elements matched your query.");
      else showBanner("success", `${count} matching elements found`);

      $("queryOut").textContent = `Found: ${count} elements\nReturned: ${returned}/${lim}\n`;
      $("queryNames").textContent = (out.data.names || []).join("\n");
    },
  });
}

async function doStore() {
  let body;
  try {
    const ttl_sec = normalizeTTL($("storeTtl").value);
    const active = document.querySelector('.tab[data-tabgroup="store-create"].active')?.dataset?.tab || "sc-all";
    body = { ttl_sec };
    if (active === "sc-all") {
      body.type = "find_all_store";
      body.bits = normalizeBits($("storeBitsAll").value);
      if (body.bits.length < 2) throw new Error("Invalid value.");
    } else if (active === "sc-any") {
      body.type = "find_any_store";
      body.bits = normalizeBits($("storeBitsAny").value);
      if (body.bits.length < 2) throw new Error("Invalid value.");
    } else {
      body.type = "find_not_store";
      const include = normalizeInt($("storeIncludeBit").value, "Include Bit");
      if (include < 0 || include > 4095) throw new Error("Must be between 0 and 4095.");
      body.include_bit = include;
      body.exclude_bits = normalizeBits($("storeExcludeBits").value);
      if (body.exclude_bits.length < 1) throw new Error("Invalid value.");
    }
  } catch (e) {
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    renderEnvelope($("storeOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnStore"),
    outEl: $("storeOut"),
    fn: async () => {
      const out = await apiJson(withNsQuery("/api/v1/store"), { method: "POST", body: JSON.stringify(body) });
      if (!out.ok) {
        state.error = { type: "request", message: out?.error?.message || "Request failed" };
        showBanner("error", "⚠️ Failed to store result. Please check backend logs.");
        renderEnvelope($("storeOut"), out);
        return;
      }

      state.error = null;
      $("inspectKey").value = out.data.store_key || "";
      showBanner("success", `✅ Stored as: ${out.data.store_key}`);
      const preview = (out.data.preview || []).join(", ");
      $("storeOut").textContent =
        `✅ Stored as: ${out.data.store_key}\n` +
        `${out.data.count} elements stored\n` +
        `Time remaining: ${out.data.ttl_remaining}s\n` +
        `Preview: ${preview}${out.data.count > out.data.preview_limit ? "..." : ""}\n`;
    },
  });
}

async function doInspect() {
  let store_key;
  let limit;
  try {
    store_key = ($("inspectKey").value || "").trim();
    if (!store_key) throw new Error("This field is required.");
    limit = readLimitOrDefault($("inspectLimit"), "Limit", 5000, runtimeConfig.defaultLimit);
  } catch (e) {
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    renderEnvelope($("inspectOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnInspect"),
    outEl: $("inspectOut"),
    fn: async () => {
      const out = await apiJson(
        withNsQuery(`/api/v1/store/inspect?store_key=${encodeURIComponent(store_key)}&limit=${encodeURIComponent(limit)}`)
      );
      if (!out.ok) {
        state.error = { type: "request", message: out?.error?.message || "Request failed" };
        showBanner("error", out?.error?.message || "Request failed");
        renderEnvelope($("inspectOut"), out);
        return;
      }
      state.error = null;
      const names = out.data.names || [];
      $("inspectOut").textContent =
        `${out.data.count} elements\n` +
        `Time remaining: ${out.data.ttl_remaining}s\n` +
        `Returned: ${out.data.returned}/${out.data.limit}\n\n` +
        names.join("\n");
    },
  });
}

async function doDeleteStore() {
  const store_key = ($("inspectKey").value || "").trim();
  if (!store_key) {
    const msg = "This field is required.";
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    renderEnvelope($("inspectOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnDeleteStore"),
    outEl: $("inspectOut"),
    fn: async () => {
      const out = await apiJson(withNsQuery(`/api/v1/store?store_key=${encodeURIComponent(store_key)}`), {
        method: "DELETE",
      });
      if (!out.ok) {
        state.error = { type: "request", message: out?.error?.message || "Request failed" };
        showBanner("error", out?.error?.message || "Request failed");
        renderEnvelope($("inspectOut"), out);
        return;
      }
      state.error = null;
      showBanner("success", "Deleted");
      $("inspectOut").textContent = `Deleted: ${out.data.deleted}\n`;
    },
  });
}

async function doLogs() {
  let tail;
  try {
    tail = readLimitOrDefault($("logsTail"), "Tail", 2000, runtimeConfig.defaultLimit);
  } catch (e) {
    const msg = String(e);
    state.error = { type: "validation", message: msg };
    showBanner("error", msg);
    renderEnvelope($("logsOut"), { ok: false, error: { code: "VALIDATION_ERROR", message: msg, details: {} } });
    return;
  }

  await withRequest({
    buttonEl: $("btnLogs"),
    outEl: $("logsOut"),
    fn: async () => {
      const out = await apiJson(`/api/v1/logs?tail=${encodeURIComponent(tail)}`);
      if (!out.ok) {
        state.error = { type: "request", message: out?.error?.message || "Request failed" };
        showBanner("error", "⚠️ Could not retrieve logs. Check backend status.");
        renderEnvelope($("logsOut"), out);
        return;
      }
      state.error = null;
      $("logsOut").textContent = (out.data.lines || []).join("\n");
    },
  });
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    // fallback
    const t = document.createElement("textarea");
    t.value = text;
    document.body.appendChild(t);
    t.select();
    document.execCommand("copy");
    t.remove();
  }
}

function setupActions() {
  $("nsSelect").addEventListener("change", async () => {
    if (state.locked) return;
    clearErrorState();
    const next = String($("nsSelect").value || "").trim();
    const ent = (nsState.options || []).find((o) => o.id === next);
    if (!ent) {
      showBanner("error", "Invalid value.");
      return;
    }
    nsState.selectedId = ent.id;
    nsState.selectedLabel = ent.label;
    nsState.selectedPrefix = ent.prefix;
    localStorage.setItem("er_gui_ns", ent.id);

    bitmapsCache = null;
    bitmapsDoc = null;
    closeBitmapsItemEditor();
    renderBitmapsTable();
    renderBitmapsGroups();
    await fetchBitmaps({ silent: true });
    renderBitmapsTable();
    await refreshHealth();
  });

  $("btnStatusRefresh").addEventListener("click", refreshHealth);

  $("btnNsDiscover").addEventListener("click", async () => {
    if (state.locked) return;
    await withRequest({
      buttonEl: $("btnNsDiscover"),
      outEl: $("backendStatus"),
      fn: async () => {
        const out = await apiJson("/api/v1/namespaces/discover?max_keys=50000&sample_per_prefix=200&scan_count=1000");
        if (!out.ok) {
          const msg = out?.error?.message || "Request failed";
          state.error = { type: "request", message: msg };
          showBanner("error", msg);
          return;
        }
        nsDiscoverCache = out.data;
        renderNsDiscover();
        showBanner("success", "Discovery completed");
      },
    });
  });

  $("btnNsExport").addEventListener("click", async () => {
    if (state.locked) return;
    await withRequest({
      buttonEl: $("btnNsExport"),
      outEl: $("backendStatus"),
      fn: async () => {
        const out = await apiJson("/api/v1/namespaces/discover?max_keys=50000&sample_per_prefix=200&scan_count=1000&write=1");
        if (!out.ok) {
          const msg = out?.error?.message || "Request failed";
          state.error = { type: "request", message: msg };
          showBanner("error", msg);
          return;
        }
        nsDiscoverCache = out.data;
        renderNsDiscover();
        const doc = out?.data?.export?.document || null;
        if (doc) downloadJson("namespaces.generated.json", doc);
        showBanner("success", "Exported namespaces.generated.json");
      },
    });
  });

	  $("btnExamplesRefresh").addEventListener("click", async () => {
	    if (state.locked) return;
	    await withRequest({
	      buttonEl: $("btnExamplesRefresh"),
	      outEl: $("examplesOut"),
	      fn: async () => {
	        await fetchExamples({ force: true });
	        await renderExamples({ forceReadme: true });
	      },
	    });
	  });

	  $("examplesSelect").addEventListener("change", async () => {
	    if (state.locked) return;
	    setExamplesMode(getSelectedExample());
	    clearExamplesReports();
	    await updateExamplesMetaAndReadme({ forceReadme: false });
	  });

	  $("btnExamplesCompare").addEventListener("click", async () => {
	    if (state.locked) return;
	    const selected = getSelectedExample();
	    if (String(selected?.type || "") !== "dataset_compare") return;
	    const id = String($("examplesSelect")?.value || "").trim();
	    const ns = String($("examplesNs")?.value || "").trim() || selectedNsId();
	    if (!id || !ns) {
	      const msg = "This field is required.";
	      state.error = { type: "validation", message: msg };
	      showBanner("error", msg);
	      return;
	    }
	    await withRequest({
	      buttonEl: $("btnExamplesCompare"),
	      outEl: $("examplesOut"),
	      fn: async () => {
	        const out = await apiJson(`/api/v1/examples/${encodeURIComponent(id)}/reports?ns=${encodeURIComponent(ns)}`);
	        if (!out.ok) {
	          state.error = { type: "request", message: out?.error?.message || "Request failed" };
	          showBanner("error", out?.error?.message || "Request failed");
	          renderEnvelope($("examplesOut"), out);
	          clearExamplesReports();
	          return;
	        }
	        state.error = null;
	        showBanner("success", "Compare completed");
	        renderExamplesReports(out);
	      },
	    });
	  });

	  $("btnExamplesRun").addEventListener("click", async () => {
	    if (state.locked) return;
	    const id = String($("examplesSelect")?.value || "").trim();
	    const ns = String($("examplesNs")?.value || "").trim() || selectedNsId();
	    const reset = !!$("examplesReset")?.checked;
    if (!id || !ns) {
      const msg = "This field is required.";
      state.error = { type: "validation", message: msg };
      showBanner("error", msg);
      return;
    }

	    await withRequest({
	      buttonEl: $("btnExamplesRun"),
	      outEl: $("examplesOut"),
	      fn: async () => {
	        const out = await apiJson(`/api/v1/examples/${encodeURIComponent(id)}/run`, {
	          method: "POST",
	          body: JSON.stringify({ ns, reset }),
	        });
	        if (!out.ok) {
	          state.error = { type: "request", message: out?.error?.message || "Request failed" };
	          showBanner("error", out?.error?.message || "Request failed");
	          renderExamplesRunResult(out);
	          return;
	        }
	        state.error = null;
	        const t = String(out?.data?.type || "");
	        showBanner("success", t === "dataset_compare" ? "✅ Import completed." : "✅ Loaded example into Redis.");
	        renderExamplesRunResult(out);
	      },
	    });
	  });

  $("examplesLinks").addEventListener("click", (ev) => {
    if (state.locked) return;
    const btn = ev.target.closest("button");
    const name = btn?.dataset?.name;
    if (!name) return;
    setActiveView("elements");
    setActiveTabGroup("elements", "elements-get");
    $("getName").value = name;
    $("matrixName").value = name;
  });
  $("btnPut").addEventListener("click", doPut);
  $("btnGet").addEventListener("click", doGet);
  $("btnQuery").addEventListener("click", doQuery);
  $("btnStore").addEventListener("click", doStore);
  $("btnInspect").addEventListener("click", doInspect);
  $("btnDeleteStore").addEventListener("click", doDeleteStore);
  $("btnLogs").addEventListener("click", doLogs);
  $("btnBitmapsRefresh").addEventListener("click", async () => {
    if (state.locked) return;
    await withRequest({
      buttonEl: $("btnBitmapsRefresh"),
      outEl: $("bitmapsMeta"),
      fn: async () => {
        await fetchBitmaps({ force: true });
        renderBitmapsTable();
        renderBitmapsGroups();
      },
    });
  });

  $("bitmapsEditMode").addEventListener("change", () => {
    setBitmapsEditMode(!!$("bitmapsEditMode").checked);
  });

  $("bitmapsTbody").addEventListener("click", (ev) => {
    if (state.locked) return;
    if (!bitmapsUi.editMode) return;
    const tr = ev.target.closest("tr");
    const bit = Number(tr?.dataset?.bit);
    if (!Number.isInteger(bit)) return;
    openBitmapsItemEditor(bit);
    renderBitmapsTable();
  });

  $("btnBmItemCancel").addEventListener("click", () => {
    if (state.locked) return;
    closeBitmapsItemEditor();
  });

  $("btnBmItemSave").addEventListener("click", async () => {
    if (state.locked) return;
    let bit;
    let group;
    try {
      bit = normalizeInt($("bmEditBit").value, "Bit");
      if (bit < 0 || bit > 4095) throw new Error("Must be between 0 and 4095.");
      group = String($("bmEditGroup").value || "").trim();
      if (!group || !groupExists(group)) throw new Error("Invalid value.");
    } catch (e) {
      const msg = String(e);
      state.error = { type: "validation", message: msg };
      showBanner("error", msg);
      return;
    }

    const name = String($("bmEditName").value || "").trim();
    const key = String($("bmEditKey").value || "").trim();
    const description = String($("bmEditDesc").value || "").trim();
    upsertDocItem(bit, { group, name, key, description });
    await putBitmapsDoc({ buttonEl: $("btnBmItemSave"), outEl: $("bitmapsMeta") });
  });

  $("btnBmDefaultGroupSave").addEventListener("click", async () => {
    if (state.locked) return;
    await saveDefaultGroup();
  });

  $("btnBmGroupCancel").addEventListener("click", () => {
    if (state.locked) return;
    resetGroupForm();
  });

  $("btnBmGroupSave").addEventListener("click", async () => {
    if (state.locked) return;
    try {
      await saveGroupFromForm();
    } catch (e) {
      const msg = String(e);
      state.error = { type: "validation", message: msg };
      showBanner("error", msg);
    }
  });

  $("bmGroupsTbody").addEventListener("click", async (ev) => {
    if (state.locked) return;
    const btn = ev.target.closest("button");
    const tr = ev.target.closest("tr");
    const action = btn?.dataset?.action;
    const id = tr?.dataset?.group;
    if (!action || !id) return;

    if (action === "edit") {
      const g = bitmapsDoc?.groups?.[id];
      if (!g) return;
      bitmapsUi.editingGroupOldId = id;
      $("bmGroupId").value = id;
      $("bmGroupLabel").value = typeof g.label === "string" ? g.label : "";
      $("bmGroupOrder").value = String(g.order ?? "");
      $("bmGroupColor").value = typeof g.color === "string" ? g.color : "";
      return;
    }

    if (action === "delete") {
      await deleteGroupById(id);
    }
  });

  $("btnBmBulkApply").addEventListener("click", async () => {
    if (state.locked) return;
    await applyBulkAssign();
  });

  $("btnMatrixFetch").addEventListener("click", async () => {
    let name;
    try {
      name = normalizeName($("matrixName").value);
    } catch (e) {
      const msg = String(e);
      state.error = { type: "validation", message: msg };
      showBanner("error", msg);
      $("matrixMeta").textContent = "";
      return;
    }

    await withRequest({
      buttonEl: $("btnMatrixFetch"),
      outEl: $("matrixMeta"),
      fn: async () => {
        await fetchBitmaps({ silent: true });
        const out = await apiJson(withNsQuery(`/api/v1/elements/get?name=${encodeURIComponent(name)}&limit=4096`));
        if (!out.ok) {
          state.error = { type: "request", message: out?.error?.message || "Request failed" };
          showBanner("error", out?.error?.code === "NOT_FOUND" ? "⚠️ No element found with that name." : (out?.error?.message || "Request failed"));
          $("matrixMeta").textContent = "";
          matrixState.bitSet = null;
          drawMatrix();
          return;
        }

        state.error = null;
        matrixState.bitSet = new Set(out.data.bits || []);
        drawMatrix();
      },
    });
  });

  $("btnCopyBitsList").addEventListener("click", async () => {
    await copyText((lastGetBits || []).join("\n"));
  });
  $("btnCopyBitsCsv").addEventListener("click", async () => {
    await copyText((lastGetBits || []).join(","));
  });
  $("btnCopyNamesList").addEventListener("click", async () => {
    await copyText($("queryNames").textContent || "");
  });
  $("btnCopyNamesCsv").addEventListener("click", async () => {
    const names = $("queryNames").textContent || "";
    await copyText(names.split("\n").filter(Boolean).join(","));
  });
}

function setupErrorResetOnInput() {
  for (const el of document.querySelectorAll(".content input, .content select, .content textarea")) {
    el.addEventListener("input", () => {
      clearErrorState();
    });
  }
  const ns = $("nsSelect");
  if (ns) {
    ns.addEventListener("change", () => {
      clearErrorState();
    });
  }
}

setupNav();
setupTabGroups();
setupActions();
setupErrorResetOnInput();
setActiveView("status");

	async function init() {
	  await loadNamespaces();
	  await loadConfig();
	  await fetchExamples({ force: false });
	  await fetchBitmaps({ silent: true });
  renderBitmapsGroups();
  setBitmapsEditMode(!!$("bitmapsEditMode")?.checked);
	  await refreshHealth();
	  renderNsDiscover();
	  await renderExamples();
	  const search = $("bitmapsSearch");
	  if (search) search.addEventListener("input", () => renderBitmapsTable());
	  setupMatrixHover();
	  drawMatrix();
	}

init();

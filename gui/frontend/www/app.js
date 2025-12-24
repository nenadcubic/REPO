const $ = (id) => document.getElementById(id);

const runtimeConfig = {
  ttlMaxSec: 86400,
  defaultLimit: 200,
  maxQueryLimit: 5000,
  storePreviewLimit: 25,
  erPrefix: "er",
  backendVersion: "unknown",
};

const state = {
  error: null, // { type: "validation"|"request", message: string }
  locked: false, // true only while request in-flight
};

let bitmapsCache = null; // { meta, items, byBit: Map<number, item> }

const matrixState = {
  bitSet: null, // Set<number>
};

function disableAllInputs() {
  for (const el of document.querySelectorAll(".content input, .content select, .content textarea")) {
    el.disabled = true;
  }
}

function enableAllInputs() {
  for (const el of document.querySelectorAll(".content input, .content select, .content textarea")) {
    el.disabled = false;
  }
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
  if (bitmapsCache && !force) return bitmapsCache;
  const res = await apiJson("/api/v1/bitmaps");
  if (!res.ok) {
    if (!silent) {
      state.error = { type: "request", message: res?.error?.message || "Request failed" };
      showBanner("error", res?.error?.message || "Request failed");
    }
    return null;
  }
  const byBit = new Map();
  const items = Array.isArray(res.data.items) ? res.data.items : [];
  for (const it of items) {
    const bit = Number(it.bit);
    if (Number.isInteger(bit)) byBit.set(bit, it);
  }
  bitmapsCache = { meta: res.data.meta || {}, items, byBit, raw: res.data };
  return bitmapsCache;
}

function bitNameFor(bit) {
  const entry = bitmapsCache?.byBit?.get(bit);
  const name = entry?.name;
  if (typeof name === "string" && name.trim()) return name.trim();
  return `Bit ${bit}`;
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
  const rows = 64;
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

    const cols = 64;
    const rows = 64;
    const x = Math.floor((relX / rect.width) * cols);
    const y = Math.floor((relY / rect.height) * rows);
    const bit = y * cols + x;
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
  metaEl.textContent = `Loaded: ${filtered.length}/${items.length}${missing}`;

  tbody.innerHTML = filtered
    .map((it) => {
      const bit = Number(it.bit);
      const key = it.key ? `<code>${escapeHtml(String(it.key))}</code>` : "";
      const name = it.name ? escapeHtml(String(it.name)) : "";
      const group = it.group ? escapeHtml(String(it.group)) : "";
      const desc = it.description ? escapeHtml(String(it.description)) : "";
      return `<tr><td>${bit}</td><td>${key}</td><td>${name}</td><td>${group}</td><td>${desc}</td></tr>`;
    })
    .join("");
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
      const out = await apiJson("/api/v1/elements/put", { method: "POST", body: JSON.stringify({ name, bits }) });
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
      const out = await apiJson(`/api/v1/elements/get?name=${encodeURIComponent(name)}&limit=${encodeURIComponent(limit)}`);
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
      const out = await apiJson("/api/v1/query", { method: "POST", body: JSON.stringify(body) });
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
      const out = await apiJson("/api/v1/store", { method: "POST", body: JSON.stringify(body) });
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
        `/api/v1/store/inspect?store_key=${encodeURIComponent(store_key)}&limit=${encodeURIComponent(limit)}`
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
      const out = await apiJson(`/api/v1/store?store_key=${encodeURIComponent(store_key)}`, { method: "DELETE" });
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
  $("btnStatusRefresh").addEventListener("click", refreshHealth);
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
      },
    });
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
        const out = await apiJson(`/api/v1/elements/get?name=${encodeURIComponent(name)}&limit=4096`);
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
}

setupNav();
setupTabGroups();
setupActions();
setupErrorResetOnInput();
setActiveView("status");

async function init() {
  await loadConfig();
  await fetchBitmaps({ silent: true });
  await refreshHealth();
  const search = $("bitmapsSearch");
  if (search) search.addEventListener("input", () => renderBitmapsTable());
  setupMatrixHover();
  drawMatrix();
}

init();

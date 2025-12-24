const $ = (id) => document.getElementById(id);

function clearBanner() {
  const b = $("banner");
  if (!b) return;
  b.classList.add("hidden");
  b.classList.remove("success", "error");
  b.textContent = "";
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

async function withLoading({ buttonEl, outEl, fn }) {
  clearBanner();
  const prevText = buttonEl ? buttonEl.textContent : "";
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
  if (ttl > 86400) throw new Error("Invalid value.");
  return ttl;
}

function normalizeLimit(input, label, max) {
  const limit = normalizeInt(input, label);
  if (limit <= 0) throw new Error("Invalid value.");
  if (limit > max) throw new Error("Invalid value.");
  return limit;
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
  };
  $("viewTitle").textContent = titleMap[name] || name;
  clearBanner();
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
    el.addEventListener("click", () => setActiveView(el.dataset.view));
  }
}

function setupTabGroups() {
  for (const el of document.querySelectorAll(".tab[data-tabgroup]")) {
    el.addEventListener("click", () => setActiveTabGroup(el.dataset.tabgroup, el.dataset.tab));
  }
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

async function refreshHealth() {
  await withLoading({
    buttonEl: $("btnStatusRefresh"),
    outEl: $("backendStatus"),
    fn: async () => {
      const h = await apiJson("/api/v1/health");
      if (!h.ok) {
        showBanner("error", "⚠️ Unable to connect to backend. Please ensure the sandbox is running.");
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
    },
  });
}

async function doPut() {
  await withLoading({
    buttonEl: $("btnPut"),
    outEl: $("putOut"),
    fn: async () => {
      try {
        const name = normalizeName($("putName").value);
        const bits = normalizeBits($("putBits").value);
        if (bits.length === 0) throw new Error("This field is required.");

        const out = await apiJson("/api/v1/elements/put", { method: "POST", body: JSON.stringify({ name, bits }) });
        if (!out.ok) {
          showBanner("error", out?.error?.message || "Invalid value.");
          renderEnvelope($("putOut"), out);
          return;
        }
        showBanner("success", `✅ Element saved. ${out.data.written_bits} bits written.`);
        $("putOut").textContent = `Saved: ${out.data.name}\nBits written: ${out.data.written_bits}\n`;
      } catch (e) {
        const msg = String(e);
        if (msg.includes("between 0 and 4095")) showBanner("error", "❌ Bit value must be between 0 and 4095.");
        else showBanner("error", msg);
        renderEnvelope($("putOut"), { ok: false, error: { code: "INVALID_INPUT", message: msg, details: {} } });
      }
    },
  });
}

let lastGetBits = [];

async function doGet() {
  await withLoading({
    buttonEl: $("btnGet"),
    outEl: $("getOut"),
    fn: async () => {
      try {
        const name = normalizeName($("getName").value);
        const limit = normalizeLimit($("getLimit").value, "Limit", 4096);
        const out = await apiJson(
          `/api/v1/elements/get?name=${encodeURIComponent(name)}&limit=${encodeURIComponent(limit)}`
        );
        if (!out.ok) {
          lastGetBits = [];
          if (out?.error?.code === "NOT_FOUND") showBanner("error", "⚠️ No element found with that name.");
          else showBanner("error", out?.error?.message || "Invalid value.");
          renderEnvelope($("getOut"), out);
          return;
        }
        lastGetBits = out.data.bits || [];
        const bitsText = `[${lastGetBits.join(", ")}]`;
        $("getOut").textContent =
          `Bits: ${bitsText}\n` +
          `Found: ${out.data.count} bits\n` +
          `Returned: ${out.data.returned}/${out.data.limit}\n`;
      } catch (e) {
        lastGetBits = [];
        showBanner("error", String(e));
        renderEnvelope($("getOut"), { ok: false, error: { code: "INVALID_INPUT", message: String(e), details: {} } });
      }
    },
  });
}

async function doQuery() {
  await withLoading({
    buttonEl: $("btnQuery"),
    outEl: $("queryOut"),
    fn: async () => {
      try {
        const limit = normalizeLimit($("queryLimit").value, "Limit", 5000);
        const active = document.querySelector('.tab[data-tabgroup="queries"].active')?.dataset?.tab || "q-find";

        let body = { limit };
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

        const out = await apiJson("/api/v1/query", { method: "POST", body: JSON.stringify(body) });
        if (!out.ok) {
          const msg = out?.error?.message || "Invalid value.";
          showBanner("error", msg);
          $("queryNames").textContent = "";
          renderEnvelope($("queryOut"), out);
          return;
        }

        const count = out.data.count ?? 0;
        const returned = out.data.returned ?? 0;
        const lim = out.data.limit ?? limit;

        if (count === 0) showBanner("error", "No elements matched your query.");
        else showBanner("success", `${count} matching elements found`);

        $("queryOut").textContent = `Found: ${count} elements\nReturned: ${returned}/${lim}\n`;
        $("queryNames").textContent = (out.data.names || []).join("\n");
      } catch (e) {
        const msg = String(e);
        if (msg.includes("between 0 and 4095")) showBanner("error", "❌ One or more bits are out of the 0–4095 range.");
        else showBanner("error", msg);
        $("queryNames").textContent = "";
        renderEnvelope($("queryOut"), { ok: false, error: { code: "INVALID_INPUT", message: msg, details: {} } });
      }
    },
  });
}

async function doStore() {
  await withLoading({
    buttonEl: $("btnStore"),
    outEl: $("storeOut"),
    fn: async () => {
      try {
        const ttl_sec = normalizeTTL($("storeTtl").value);
        const active = document.querySelector('.tab[data-tabgroup="store-create"].active')?.dataset?.tab || "sc-all";
        let body = { ttl_sec };
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

        const out = await apiJson("/api/v1/store", { method: "POST", body: JSON.stringify(body) });
        if (!out.ok) {
          showBanner("error", "⚠️ Failed to store result. Please check backend logs.");
          renderEnvelope($("storeOut"), out);
          return;
        }

        $("inspectKey").value = out.data.store_key || "";
        showBanner("success", `✅ Stored as: ${out.data.store_key}`);
        const preview = (out.data.preview || []).join(", ");
        $("storeOut").textContent =
          `✅ Stored as: ${out.data.store_key}\n` +
          `${out.data.count} elements stored\n` +
          `Time remaining: ${out.data.ttl_remaining}s\n` +
          `Preview: ${preview}${out.data.count > out.data.preview_limit ? "..." : ""}\n`;
      } catch (e) {
        const msg = String(e);
        showBanner("error", msg);
        renderEnvelope($("storeOut"), { ok: false, error: { code: "INVALID_INPUT", message: msg, details: {} } });
      }
    },
  });
}

async function doInspect() {
  await withLoading({
    buttonEl: $("btnInspect"),
    outEl: $("inspectOut"),
    fn: async () => {
      try {
        const store_key = ($("inspectKey").value || "").trim();
        if (!store_key) throw new Error("This field is required.");
        const limit = normalizeLimit($("inspectLimit").value || "200", "Limit", 5000);
        const out = await apiJson(
          `/api/v1/store/inspect?store_key=${encodeURIComponent(store_key)}&limit=${encodeURIComponent(limit)}`
        );
        if (!out.ok) {
          showBanner("error", out?.error?.message || "Invalid value.");
          renderEnvelope($("inspectOut"), out);
          return;
        }
        const names = out.data.names || [];
        $("inspectOut").textContent =
          `${out.data.count} elements\n` +
          `Time remaining: ${out.data.ttl_remaining}s\n` +
          `Returned: ${out.data.returned}/${out.data.limit}\n\n` +
          names.join("\n");
      } catch (e) {
        showBanner("error", String(e));
        renderEnvelope($("inspectOut"), { ok: false, error: { code: "INVALID_INPUT", message: String(e), details: {} } });
      }
    },
  });
}

async function doDeleteStore() {
  await withLoading({
    buttonEl: $("btnDeleteStore"),
    outEl: $("inspectOut"),
    fn: async () => {
      try {
        const store_key = ($("inspectKey").value || "").trim();
        if (!store_key) throw new Error("This field is required.");
        const out = await apiJson(`/api/v1/store?store_key=${encodeURIComponent(store_key)}`, { method: "DELETE" });
        if (!out.ok) {
          showBanner("error", out?.error?.message || "Invalid value.");
          renderEnvelope($("inspectOut"), out);
          return;
        }
        showBanner("success", "Deleted");
        $("inspectOut").textContent = `Deleted: ${out.data.deleted}\n`;
      } catch (e) {
        showBanner("error", String(e));
        renderEnvelope($("inspectOut"), { ok: false, error: { code: "INVALID_INPUT", message: String(e), details: {} } });
      }
    },
  });
}

async function doLogs() {
  await withLoading({
    buttonEl: $("btnLogs"),
    outEl: $("logsOut"),
    fn: async () => {
      try {
        const tail = normalizeLimit($("logsTail").value || "200", "Tail", 2000);
        const out = await apiJson(`/api/v1/logs?tail=${encodeURIComponent(tail)}`);
        if (!out.ok) {
          showBanner("error", "⚠️ Could not retrieve logs. Check backend status.");
          renderEnvelope($("logsOut"), out);
          return;
        }
        $("logsOut").textContent = (out.data.lines || []).join("\n");
      } catch (e) {
        showBanner("error", String(e));
        renderEnvelope($("logsOut"), { ok: false, error: { code: "INVALID_INPUT", message: String(e), details: {} } });
      }
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

setupNav();
setupTabGroups();
setupActions();
setActiveView("status");
refreshHealth();

import React, { useEffect, useMemo, useRef, useState } from "react";

type NamespaceInfo = {
  name: string; // namespace id (preset-backed)
  key_count: number; // element count (universe set size)
  updated_at: string;
  layout: string;
};

type ElementsListItem = {
  key: string; // full redis key, e.g. "er:element:alice"
  short_name: string; // element name, e.g. "alice"
  set_bits_count: number;
  ttl: number | null;
};

type ElementsListResponse = {
  items: ElementsListItem[];
  page: number;
  page_size: number;
  total: number;
};

type ElementDetailsResponse = {
  key: string;
  short_name: string;
  namespace: string;
  kind: "bitset" | "hash";
  bits?: number;
  set_bits?: number[];
  ttl: number | null;
  hash?: { field_count?: number; fields?: Record<string, string>; truncated?: boolean };
};

type NamespaceBitmapRow = {
  key: string;
  short_name: string;
  set_bits: number[];
};

type NamespaceBitmapResponse = {
  namespace: string;
  bits: number; // expected 4096
  elements: NamespaceBitmapRow[];
};

type ApiErrorEnvelope = {
  ok: false;
  error?: { code?: string; message?: string; details?: unknown };
};

type ApiOkEnvelope<T> = {
  ok: true;
  data: T;
};

type ViewMode = "element" | "bitmap";
type ElementTab = "details" | "matrix";
type SortKey = "short_name" | "set_bits_count" | "ttl";
type SortDir = "asc" | "desc";

function isEnvelope(v: unknown): v is ApiOkEnvelope<unknown> | ApiErrorEnvelope {
  return !!v && typeof v === "object" && "ok" in (v as Record<string, unknown>);
}

async function apiGetJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, { headers: { Accept: "application/json" }, signal });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}${txt ? ` — ${txt}` : ""}`);
  }
  const raw: unknown = await res.json();
  if (isEnvelope(raw)) {
    if (!raw.ok) {
      const msg = raw?.error?.message || "Request failed";
      const code = raw?.error?.code || "ERROR";
      throw new Error(`${code}: ${msg}`);
    }
    return raw.data as T;
  }
  return raw as T;
}

async function apiPostJson<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText}${txt ? ` — ${txt}` : ""}`);
  }
  const raw: unknown = await res.json();
  if (isEnvelope(raw)) {
    if (!raw.ok) {
      const msg = raw?.error?.message || "Request failed";
      const code = raw?.error?.code || "ERROR";
      throw new Error(`${code}: ${msg}`);
    }
    return raw.data as T;
  }
  return raw as T;
}

function fmtTime(iso: string) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

function fmtTtl(ttl: number | null) {
  if (ttl == null) return "∞";
  if (!Number.isFinite(ttl)) return String(ttl);
  if (ttl < 0) return "∞";
  return `${ttl}s`;
}

function encodePathSegment(s: string) {
  return encodeURIComponent(s);
}

function encodeElementKey(key: string) {
  return encodeURIComponent(key);
}

function parseBitsInput(raw: string): number[] {
  const s = String(raw || "").trim();
  if (!s) return [];
  const parts = s.split(/[,\s]+/g).map((t) => t.trim()).filter(Boolean);
  const bits: number[] = [];
  for (const p of parts) {
    const n = Number.parseInt(p, 10);
    if (!Number.isInteger(n) || n < 0 || n > 4095) continue;
    bits.push(n);
  }
  return Array.from(new Set(bits)).sort((a, b) => a - b);
}

async function fetchNamespaces(signal?: AbortSignal) {
  return apiGetJson<NamespaceInfo[]>("/api/v1/explorer/namespaces", signal);
}

async function fetchNamespaceElements(
  namespace: string,
  { search, page, pageSize }: { search: string; page: number; pageSize: number },
  signal?: AbortSignal,
) {
  const qs = new URLSearchParams();
  if (search.trim()) qs.set("search", search.trim());
  qs.set("page", String(page));
  qs.set("page_size", String(pageSize));
  return apiGetJson<ElementsListResponse>(
    `/api/v1/explorer/namespaces/${encodePathSegment(namespace)}/elements?${qs.toString()}`,
    signal,
  );
}

async function fetchElementDetails(key: string, signal?: AbortSignal) {
  return apiGetJson<ElementDetailsResponse>(`/api/v1/explorer/elements/${encodeElementKey(key)}`, signal);
}

async function fetchNamespaceBitmap(
  namespace: string,
  { limit, offset }: { limit: number; offset: number },
  signal?: AbortSignal,
) {
  const qs = new URLSearchParams();
  qs.set("limit", String(limit));
  qs.set("offset", String(offset));
  return apiGetJson<NamespaceBitmapResponse>(
    `/api/v1/explorer/namespaces/${encodePathSegment(namespace)}/bitmap?${qs.toString()}`,
    signal,
  );
}

export default function Explorer() {
  const [namespaces, setNamespaces] = useState<NamespaceInfo[]>([]);
  const [nsFilter, setNsFilter] = useState("");
  const [selectedNamespace, setSelectedNamespace] = useState<NamespaceInfo | null>(null);
  const [nsReloadNonce, setNsReloadNonce] = useState(0);

  const [elementsResp, setElementsResp] = useState<ElementsListResponse | null>(null);
  const [elementsSearch, setElementsSearch] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [elementsReloadNonce, setElementsReloadNonce] = useState(0);
  const [sortKey, setSortKey] = useState<SortKey>("short_name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const [selectedElementKey, setSelectedElementKey] = useState<string | null>(null);
  const [selectedElement, setSelectedElement] = useState<ElementDetailsResponse | null>(null);

  const [viewMode, setViewMode] = useState<ViewMode>("element");
  const [elementTab, setElementTab] = useState<ElementTab>("details");

  const [bitmapLimit, setBitmapLimit] = useState(75);
  const [bitmapOffset, setBitmapOffset] = useState(0);
  const [bitmap, setBitmap] = useState<NamespaceBitmapResponse | null>(null);

  const [loadingNamespaces, setLoadingNamespaces] = useState(false);
  const [loadingElements, setLoadingElements] = useState(false);
  const [loadingElement, setLoadingElement] = useState(false);
  const [loadingBitmap, setLoadingBitmap] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [createName, setCreateName] = useState("example");
  const [createBits, setCreateBits] = useState("1 7 42");
  const [createStatus, setCreateStatus] = useState<string | null>(null);

  useEffect(() => {
    const ac = new AbortController();
    setLoadingNamespaces(true);
    setError(null);
    fetchNamespaces(ac.signal)
      .then((data) => setNamespaces(Array.isArray(data) ? data : []))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingNamespaces(false));
    return () => ac.abort();
  }, [nsReloadNonce]);

  useEffect(() => {
    if (!selectedNamespace) {
      setElementsResp(null);
      return;
    }
    const ac = new AbortController();
    setLoadingElements(true);
    setError(null);
    fetchNamespaceElements(
      selectedNamespace.name,
      { search: elementsSearch, page, pageSize },
      ac.signal,
    )
      .then((data) => setElementsResp(data))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingElements(false));
    return () => ac.abort();
  }, [selectedNamespace?.name, elementsSearch, page, pageSize, elementsReloadNonce]);

  useEffect(() => {
    if (!selectedElementKey) {
      setSelectedElement(null);
      return;
    }
    const ac = new AbortController();
    setLoadingElement(true);
    setError(null);
    fetchElementDetails(selectedElementKey, ac.signal)
      .then((data) => setSelectedElement(data))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingElement(false));
    return () => ac.abort();
  }, [selectedElementKey]);

  useEffect(() => {
    if (viewMode !== "bitmap") return;
    if (!selectedNamespace) {
      setBitmap(null);
      return;
    }
    const ac = new AbortController();
    setLoadingBitmap(true);
    setError(null);
    fetchNamespaceBitmap(selectedNamespace.name, { limit: bitmapLimit, offset: bitmapOffset }, ac.signal)
      .then((data) => setBitmap(data))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingBitmap(false));
    return () => ac.abort();
  }, [viewMode, selectedNamespace?.name, bitmapLimit, bitmapOffset]);

  const filteredNamespaces = useMemo(() => {
    const q = nsFilter.trim().toLowerCase();
    const list = Array.isArray(namespaces) ? namespaces : [];
    if (!q) return list;
    return list.filter((ns) => String(ns?.name || "").toLowerCase().includes(q));
  }, [namespaces, nsFilter]);

  const elementsSorted = useMemo(() => {
    const items = Array.isArray(elementsResp?.items) ? [...elementsResp!.items] : [];
    const dir = sortDir === "asc" ? 1 : -1;
    items.sort((a, b) => {
      if (sortKey === "short_name") {
        return dir * String(a.short_name || "").localeCompare(String(b.short_name || ""));
      }
      if (sortKey === "set_bits_count") {
        return dir * (Number(a.set_bits_count || 0) - Number(b.set_bits_count || 0));
      }
      const at = a.ttl == null ? Number.POSITIVE_INFINITY : Number(a.ttl);
      const bt = b.ttl == null ? Number.POSITIVE_INFINITY : Number(b.ttl);
      return dir * (at - bt);
    });
    return items;
  }, [elementsResp, sortKey, sortDir]);

  const totalPages = useMemo(() => {
    const total = Number(elementsResp?.total ?? 0);
    return Math.max(1, Math.ceil(total / pageSize));
  }, [elementsResp?.total, pageSize]);

  function onSelectNamespace(ns: NamespaceInfo) {
    setSelectedNamespace(ns);
    setPage(1);
    setSelectedElementKey(null);
    setSelectedElement(null);
    setBitmap(null);
    setViewMode("element");
    setElementTab("details");
    setBitmapOffset(0);
  }

  function toggleSort(nextKey: SortKey) {
    if (sortKey !== nextKey) {
      setSortKey(nextKey);
      setSortDir("asc");
      return;
    }
    setSortDir((d) => (d === "asc" ? "desc" : "asc"));
  }

  async function createElement() {
    if (!selectedNamespace) return;
    if (selectedNamespace.layout !== "er_layout_v1") {
      setCreateStatus("Quick create is supported only for bitset (er_layout_v1) namespaces.");
      return;
    }
    const name = String(createName || "").trim();
    if (!name || name.length > 100) {
      setCreateStatus("Name must be 1..100 chars.");
      return;
    }
    const bits = parseBitsInput(createBits);
    if (!bits.length) {
      setCreateStatus("Bits must include at least one valid bit (0..4095).");
      return;
    }
    setCreateStatus("Saving…");
    try {
      await apiPostJson<{ name: string; written_bits: number }>("/api/v1/elements/put", {
        ns: selectedNamespace.name,
        name,
        bits,
      });
      setCreateStatus(`Saved "${name}" (${bits.length} bits).`);
      setElementsReloadNonce((n) => n + 1);
      setNsReloadNonce((n) => n + 1);
    } catch (e) {
      setCreateStatus(e instanceof Error ? e.message : String(e));
    }
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="sidebar-title">element-redis</div>
          <div className="sidebar-sub">Explorer</div>
        </div>
        <nav className="nav">
          <a className="nav-item" href="/" title="Back to the main GUI">
            Back to GUI
          </a>
          <span className="nav-item active">Explorer</span>
        </nav>
        <div className="sidebar-foot">
          <div className="muted">
            API: <code>/api/v1</code>
          </div>
        </div>
      </aside>

      <div className="main">
        <header className="header">
          <div>
            <div className="header-title">Explorer</div>
            <div className="header-sub">Namespaces • Elements • Matrix • Bitmap</div>
          </div>
        </header>

        <main className="content">
          <div className="desc">
            Browse namespaces and elements, inspect per-element bits (0..4095), or render a namespace bitmap overview.
          </div>

          {error ? <div className="banner error">{error}</div> : null}

          <div className="grid3">
            <div className="panel" style={{ marginTop: 0 }}>
              <div className="panel-title">Namespaces</div>
              <label className="label">Filter</label>
              <input
                className="input"
                placeholder="Substring filter"
                value={nsFilter}
                onChange={(e) => setNsFilter(e.target.value)}
              />
              <div className="row">
                <button className="btn" onClick={() => setNsReloadNonce((n) => n + 1)} disabled={loadingNamespaces}>
                  Refresh list
                </button>
              </div>
              <NamespaceTable
                items={filteredNamespaces}
                selected={selectedNamespace?.name ?? null}
                loading={loadingNamespaces}
                onSelect={onSelectNamespace}
              />
            </div>

            <div className="panel" style={{ marginTop: 0 }}>
              <div className="panel-title">Elements</div>
              <div className="help">
                {selectedNamespace ? (
                  <>
                    Selected namespace: <code>{selectedNamespace.name}</code>
                    {" "}
                    <span className="muted">
                      (layout: <code>{selectedNamespace.layout}</code>)
                    </span>
                  </>
                ) : (
                  "Select a namespace to load its elements."
                )}
              </div>

              {selectedNamespace?.layout === "er_layout_v1" ? (
                <div className="panel" style={{ marginTop: 12 }}>
                  <div className="panel-title">Quick create (debug)</div>
                  <div className="help" style={{ marginTop: 0 }}>
                    Creates a new element in the selected namespace using the existing API.
                  </div>
                  <label className="label">Name</label>
                  <input
                    className="input"
                    value={createName}
                    onChange={(e) => setCreateName(e.target.value)}
                    disabled={!selectedNamespace}
                  />
                  <label className="label">Bits</label>
                  <input
                    className="input"
                    value={createBits}
                    onChange={(e) => setCreateBits(e.target.value)}
                    disabled={!selectedNamespace}
                    placeholder="e.g. 1 2 3"
                  />
                  <div className="row">
                    <button className="btn primary" onClick={createElement} disabled={!selectedNamespace}>
                      Save element
                    </button>
                    {createStatus ? <span className="muted">{createStatus}</span> : null}
                  </div>
                </div>
              ) : null}

              <label className="label">Search</label>
              <input
                className="input"
                placeholder="Search element names"
                value={elementsSearch}
                onChange={(e) => {
                  setElementsSearch(e.target.value);
                  setPage(1);
                }}
                disabled={!selectedNamespace}
              />

              <div className="row">
                <label className="label" style={{ margin: 0 }}>
                  Page size
                </label>
                <select
                  className="input"
                  style={{ width: 120 }}
                  value={pageSize}
                  onChange={(e) => {
                    setPageSize(Number(e.target.value));
                    setPage(1);
                  }}
                  disabled={!selectedNamespace}
                >
                  <option value={25}>25</option>
                  <option value={50}>50</option>
                  <option value={100}>100</option>
                </select>
                <button
                  className="btn"
                  onClick={() => setElementsReloadNonce((n) => n + 1)}
                  disabled={!selectedNamespace || loadingElements}
                >
                  Refresh
                </button>
                <button
                  className="btn"
                  onClick={() => {
                    if (!selectedNamespace) return;
                    setPage(1);
                    setElementsSearch("");
                  }}
                  disabled={!selectedNamespace}
                >
                  Clear
                </button>
                {!selectedNamespace ? null : (
                  <a className="btn" href="/" title="Open Examples in the main GUI">
                    Seed via Examples…
                  </a>
                )}
              </div>

              <ElementsTable
                items={elementsSorted}
                loading={loadingElements}
                selectedKey={selectedElementKey}
                sortKey={sortKey}
                sortDir={sortDir}
                onSort={toggleSort}
                onSelect={(it) => {
                  setSelectedElementKey(it.key);
                  setSelectedElement(null);
                  setViewMode("element");
                  setElementTab("details");
                }}
              />

              <Pagination
                disabled={!selectedNamespace || loadingElements}
                page={page}
                totalPages={totalPages}
                total={elementsResp?.total ?? 0}
                onPrev={() => setPage((p) => Math.max(1, p - 1))}
                onNext={() => setPage((p) => Math.min(totalPages, p + 1))}
              />
            </div>

            <div className="panel" style={{ marginTop: 0 }}>
              <div className="panel-title">Inspect</div>

              <div className="tabs tabs-inline" style={{ marginTop: 0 }}>
                <button
                  className={"tab " + (viewMode === "element" ? "active" : "")}
                  onClick={() => setViewMode("element")}
                  disabled={!selectedNamespace}
                >
                  Element
                </button>
                <button
                  className={"tab " + (viewMode === "bitmap" ? "active" : "")}
                  onClick={() => setViewMode("bitmap")}
                  disabled={!selectedNamespace || selectedNamespace.layout !== "er_layout_v1"}
                >
                  Namespace bitmap
                </button>
              </div>

              {!selectedNamespace ? (
                <div className="muted">Select a namespace to continue.</div>
              ) : viewMode === "bitmap" ? (
                <>
                  <div className="row" style={{ marginTop: 0 }}>
                    <label className="label" style={{ margin: 0 }}>
                      Rows
                    </label>
                    <input
                      className="input"
                      style={{ width: 120 }}
                      type="number"
                      min={10}
                      max={200}
                      value={bitmapLimit}
                      onChange={(e) => {
                        const v = Number(e.target.value) || 75;
                        setBitmapLimit(Math.max(10, Math.min(200, v)));
                        setBitmapOffset(0);
                      }}
                    />
                    <button
                      className="btn"
                      onClick={() => setBitmapOffset((o) => Math.max(0, o - bitmapLimit))}
                      disabled={bitmapOffset <= 0 || loadingBitmap}
                    >
                      Prev
                    </button>
                    <button className="btn" onClick={() => setBitmapOffset((o) => o + bitmapLimit)} disabled={loadingBitmap}>
                      Next
                    </button>
                  </div>
                  <NamespaceBitmap
                    data={bitmap}
                    loading={loadingBitmap}
                    onSelectElement={(key) => {
                      setViewMode("element");
                      setSelectedElementKey(key);
                      setElementTab("matrix");
                    }}
                  />
                </>
              ) : !selectedElementKey ? (
                <div className="muted">
                  Select an element to view its bit matrix, or switch to Namespace bitmap to see multiple elements at once.
                </div>
              ) : loadingElement && !selectedElement ? (
                <div className="muted">Loading element…</div>
              ) : selectedElement ? (
                <ElementPanel element={selectedElement} tab={elementTab} onTabChange={setElementTab} />
              ) : (
                <div className="muted">No element loaded.</div>
              )}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}

function NamespaceTable({
  items,
  selected,
  loading,
  onSelect,
}: {
  items: NamespaceInfo[];
  selected: string | null;
  loading: boolean;
  onSelect: (ns: NamespaceInfo) => void;
}) {
  if (loading) return <div className="muted">Loading namespaces…</div>;
  if (!items.length) return <div className="muted">No namespaces found.</div>;

  return (
    <div className="table-wrap" style={{ marginTop: 12 }}>
      <table className="table" style={{ minWidth: 0 }}>
        <thead>
          <tr>
            <th>Namespace</th>
            <th>Elements</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          {items.map((ns) => {
            const isSel = selected === ns.name;
            return (
              <tr
                key={ns.name}
                className={"clickable " + (isSel ? "selected" : "")}
                onClick={() => onSelect(ns)}
              >
                <td>
                  <code>{ns.name}</code>
                </td>
                <td>{ns.key_count ?? 0}</td>
                <td className="muted">{fmtTime(ns.updated_at)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ElementsTable({
  items,
  loading,
  selectedKey,
  sortKey,
  sortDir,
  onSort,
  onSelect,
}: {
  items: ElementsListItem[];
  loading: boolean;
  selectedKey: string | null;
  sortKey: SortKey;
  sortDir: SortDir;
  onSort: (k: SortKey) => void;
  onSelect: (it: ElementsListItem) => void;
}) {
  return (
    <div className="table-wrap" style={{ marginTop: 12 }}>
      <table className="table" style={{ minWidth: 0 }}>
        <thead>
          <tr>
            <th style={{ cursor: "pointer" }} onClick={() => onSort("short_name")}>
              short_name{sortKey === "short_name" ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
            </th>
            <th style={{ cursor: "pointer" }} onClick={() => onSort("set_bits_count")}>
              set_bits_count{sortKey === "set_bits_count" ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
            </th>
            <th style={{ cursor: "pointer" }} onClick={() => onSort("ttl")}>
              ttl{sortKey === "ttl" ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
            </th>
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <tr>
              <td colSpan={3} className="muted">
                Loading elements…
              </td>
            </tr>
          ) : !items.length ? (
            <tr>
              <td colSpan={3} className="muted">
                No elements found. Use “Seed via Examples…” to load a dataset.
              </td>
            </tr>
          ) : (
            items.map((it) => {
              const isSel = selectedKey === it.key;
              return (
                <tr
                  key={it.key}
                  className={"clickable " + (isSel ? "selected" : "")}
                  onClick={() => onSelect(it)}
                  title={it.key}
                >
                  <td>
                    <code>{it.short_name}</code>
                  </td>
                  <td>{it.set_bits_count ?? 0}</td>
                  <td>{fmtTtl(it.ttl)}</td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    </div>
  );
}

function Pagination({
  disabled,
  page,
  totalPages,
  total,
  onPrev,
  onNext,
}: {
  disabled: boolean;
  page: number;
  totalPages: number;
  total: number;
  onPrev: () => void;
  onNext: () => void;
}) {
  return (
    <div className="row" style={{ justifyContent: "space-between" }}>
      <div className="muted">
        Page <code>{page}</code> / <code>{totalPages}</code> • Total <code>{total}</code>
      </div>
      <div className="row" style={{ marginTop: 0 }}>
        <button className="btn" onClick={onPrev} disabled={disabled || page <= 1}>
          Prev
        </button>
        <button className="btn" onClick={onNext} disabled={disabled || page >= totalPages}>
          Next
        </button>
      </div>
    </div>
  );
}

function ElementPanel({
  element,
  tab,
  onTabChange,
}: {
  element: ElementDetailsResponse;
  tab: ElementTab;
  onTabChange: (t: ElementTab) => void;
}) {
  const canMatrix = element.kind === "bitset";
  return (
    <>
      <div className="tabs tabs-inline" style={{ marginTop: 0 }}>
        <button className={"tab " + (tab === "details" ? "active" : "")} onClick={() => onTabChange("details")}>
          Details
        </button>
        {canMatrix ? (
          <button className={"tab " + (tab === "matrix" ? "active" : "")} onClick={() => onTabChange("matrix")}>
            Matrix
          </button>
        ) : null}
      </div>

      {tab === "details" || !canMatrix ? <ElementDetails element={element} /> : <ElementMatrix element={element} />}
    </>
  );
}

function ElementDetails({ element }: { element: ElementDetailsResponse }) {
  const setBits = Array.isArray(element.set_bits) ? element.set_bits : [];
  return (
    <>
      <div className="help" style={{ marginTop: 0 }}>
        Key: <code>{element.key}</code>
        <br />
        Namespace: <code>{element.namespace}</code> • Type: <code>{element.kind}</code> • TTL: <code>{fmtTtl(element.ttl)}</code>
        {element.kind === "bitset" ? (
          <>
            {" "}
            • Set bits: <code>{setBits.length}</code>
          </>
        ) : null}
      </div>
      {element.kind === "bitset" ? (
        <pre className="out" style={{ maxHeight: 320 }}>
          {setBits.length ? setBits.join(", ") : "No set bits."}
        </pre>
      ) : (
        <pre className="out" style={{ maxHeight: 320 }}>
          {JSON.stringify(element.hash?.fields || {}, null, 2)}
          {element.hash?.truncated ? "\n\n(note: fields truncated)" : ""}
        </pre>
      )}
    </>
  );
}

function ElementMatrix({ element }: { element: ElementDetailsResponse }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const tooltipRef = useRef<HTMLDivElement | null>(null);
  const setBits = useMemo(
    () => new Set<number>(((element.set_bits as number[] | undefined) || []).filter((b) => Number.isInteger(b))),
    [element.set_bits],
  );

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const size = 64;
    const px = 512;
    const cell = px / size;

    canvas.width = px;
    canvas.height = px;
    ctx.clearRect(0, 0, px, px);

    ctx.fillStyle = "rgba(4, 9, 20, 0.65)";
    ctx.fillRect(0, 0, px, px);

    ctx.fillStyle = "rgba(109, 167, 255, 0.85)";
    for (const bit of setBits) {
      if (bit < 0 || bit > 4095) continue;
      const x = bit % size;
      const y = Math.floor(bit / size);
      ctx.fillRect(x * cell, y * cell, cell, cell);
    }
  }, [setBits]);

  function hideTip() {
    const tip = tooltipRef.current;
    if (!tip) return;
    tip.classList.add("hidden");
    tip.textContent = "";
  }

  function showTip(text: string, clientX: number, clientY: number) {
    const tip = tooltipRef.current;
    if (!tip) return;
    tip.textContent = text;
    tip.style.left = `${clientX + 12}px`;
    tip.style.top = `${clientY + 12}px`;
    tip.classList.remove("hidden");
  }

  function handleMove(ev: React.MouseEvent) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const relX = ev.clientX - rect.left;
    const relY = ev.clientY - rect.top;
    if (relX < 0 || relY < 0 || relX >= rect.width || relY >= rect.height) {
      hideTip();
      return;
    }
    const size = 64;
    const x = Math.floor((relX / rect.width) * size);
    const y = Math.floor((relY / rect.height) * size);
    const bit = y * size + x;
    if (bit < 0 || bit > 4095) {
      hideTip();
      return;
    }
    const value = setBits.has(bit) ? 1 : 0;
    showTip(`BIT ${bit}: ${value}`, ev.clientX, ev.clientY);
  }

  return (
    <>
      <div className="help" style={{ marginTop: 0 }}>
        4096 bits rendered as a 64×64 matrix. Hover a cell for the bit index.
      </div>
      <div className="matrix-wrap">
        <canvas
          ref={canvasRef}
          className="matrix-canvas"
          onMouseMove={handleMove}
          onMouseLeave={hideTip}
        />
      </div>
      <div ref={tooltipRef} className="tooltip hidden" />
    </>
  );
}

function NamespaceBitmap({
  data,
  loading,
  onSelectElement,
}: {
  data: NamespaceBitmapResponse | null;
  loading: boolean;
  onSelectElement: (key: string) => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const tipRef = useRef<HTMLDivElement | null>(null);
  const rows = Array.isArray(data?.elements) ? data!.elements : [];

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const widthPx = 512;
    const rowH = 8;
    const heightPx = Math.max(1, rows.length * rowH);
    canvas.width = widthPx;
    canvas.height = heightPx;

    ctx.fillStyle = "rgba(4, 9, 20, 0.65)";
    ctx.fillRect(0, 0, widthPx, heightPx);

    ctx.fillStyle = "rgba(255, 255, 255, 0.06)";
    for (let i = 0; i < rows.length; i++) {
      if (i % 2 === 1) ctx.fillRect(0, i * rowH, widthPx, rowH);
    }

    ctx.fillStyle = "rgba(109, 167, 255, 0.85)";
    for (let i = 0; i < rows.length; i++) {
      const sb = rows[i]?.set_bits || [];
      for (const bit of sb) {
        const b = Number(bit);
        if (!Number.isInteger(b) || b < 0 || b >= 4096) continue;
        const x = Math.floor((b / 4096) * widthPx);
        ctx.fillRect(x, i * rowH, 1, rowH);
      }
    }
  }, [rows]);

  function hideTip() {
    const tip = tipRef.current;
    if (!tip) return;
    tip.classList.add("hidden");
    tip.textContent = "";
  }

  function showTip(text: string, clientX: number, clientY: number) {
    const tip = tipRef.current;
    if (!tip) return;
    tip.textContent = text;
    tip.style.left = `${clientX + 12}px`;
    tip.style.top = `${clientY + 12}px`;
    tip.classList.remove("hidden");
  }

  function handleMove(ev: React.MouseEvent) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const relX = ev.clientX - rect.left;
    const relY = ev.clientY - rect.top;
    if (relX < 0 || relY < 0 || relX >= rect.width || relY >= rect.height) {
      hideTip();
      return;
    }
    const rowH = 8;
    const idx = Math.floor((relY / rect.height) * (rows.length || 1));
    const row = rows[idx];
    if (!row) {
      hideTip();
      return;
    }
    const bitApprox = Math.floor((relX / rect.width) * 4096);
    showTip(`${row.short_name} • bit≈${bitApprox}`, ev.clientX, ev.clientY);
  }

  function handleClick(ev: React.MouseEvent) {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const relY = ev.clientY - rect.top;
    const idx = Math.floor((relY / rect.height) * (rows.length || 1));
    const row = rows[idx];
    if (!row) return;
    onSelectElement(row.key);
  }

  if (loading && !data) return <div className="muted">Loading bitmap…</div>;
  if (!rows.length) return <div className="muted">No rows returned.</div>;

  return (
    <>
      <div className="help" style={{ marginTop: 0 }}>
        X = bit index (0..4095) • Y = element row. Click a row to open it in Element view.
      </div>
      <div className="matrix-wrap" style={{ width: "100%" }}>
        <canvas
          ref={canvasRef}
          className="matrix-canvas"
          style={{ width: "100%", height: Math.max(120, rows.length * 8) }}
          onMouseMove={handleMove}
          onMouseLeave={hideTip}
          onClick={handleClick}
        />
      </div>
      <div ref={tipRef} className="tooltip hidden" />
    </>
  );
}

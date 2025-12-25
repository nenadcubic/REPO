import React, { useEffect, useMemo, useState } from "react";

type ApiErrorEnvelope = {
  ok: false;
  error?: { code?: string; message?: string; details?: unknown };
};

type ApiOkEnvelope<T> = {
  ok: true;
  data: T;
};

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

type SchemaTable = { table: string; key: string };
type SchemaTablesResponse = { ns: string; prefix: string; tables: SchemaTable[] };

type SchemaColumn = {
  name: string;
  type_family: string | null;
  not_null: boolean | null;
  has_default: boolean;
  is_pk: boolean;
  is_fk: boolean;
  has_index: boolean;
  length_bucket: string | null;
  key: string;
};

type SchemaRelation = {
  from_table: string;
  to_table: string;
  fk: string;
  direction: "from" | "to" | "other";
  cardinality: string | null;
  child_required: boolean | null;
  on_delete: string | null;
  on_update: string | null;
  key: string;
};

type SchemaTableResponse = { ns: string; prefix: string; table: string; columns: SchemaColumn[]; relations: SchemaRelation[] };

function safeId(s: string) {
  return String(s || "").replace(/[^a-z0-9_-]/gi, "-");
}

function yesNoUnknown(v: boolean | null) {
  if (v === true) return "yes";
  if (v === false) return "no";
  return "unknown";
}

export default function SchemaExplorer() {
  const [error, setError] = useState<string | null>(null);
  const [loadingTables, setLoadingTables] = useState(false);
  const [loadingTable, setLoadingTable] = useState(false);

  const [tables, setTables] = useState<SchemaTable[]>([]);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [tableResp, setTableResp] = useState<SchemaTableResponse | null>(null);

  const filteredTables = useMemo(() => {
    const q = (filter || "").trim().toLowerCase();
    if (!q) return tables;
    return tables.filter((t) => t.table.toLowerCase().includes(q));
  }, [tables, filter]);

  useEffect(() => {
    const ctrl = new AbortController();
    setLoadingTables(true);
    setError(null);
    apiGetJson<SchemaTablesResponse>("/api/v1/schema/tables?ns=or", ctrl.signal)
      .then((data) => {
        setTables(Array.isArray(data.tables) ? data.tables : []);
        setSelected((prev) => prev || (data.tables?.length ? data.tables[0].table : null));
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingTables(false));
    return () => ctrl.abort();
  }, []);

  useEffect(() => {
    if (!selected) {
      setTableResp(null);
      return;
    }
    const ctrl = new AbortController();
    setLoadingTable(true);
    setError(null);
    apiGetJson<SchemaTableResponse>(`/api/v1/schema/tables/${encodeURIComponent(selected)}?ns=or`, ctrl.signal)
      .then((data) => setTableResp(data))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingTable(false));
    return () => ctrl.abort();
  }, [selected]);

  return (
    <div className="app" data-testid="schema-root">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="sidebar-title">element-redis</div>
          <div className="sidebar-sub">Explorer</div>
        </div>
        <nav className="nav">
          <a className="nav-item" href="/" title="Back to the main GUI">
            Back to GUI
          </a>
          <a className="nav-item" href="/explorer/" title="Explore elements and namespaces">
            Explorer
          </a>
          <span className="nav-item active">Schema</span>
          <a className="nav-item" href="/explorer/data/" title="Northwind: Data vs Bitsets">
            Data
          </a>
          <a className="nav-item" href="/explorer/assoc/" title="Associations (WordNet)">
            Associations
          </a>
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
            <div className="header-title">Schema Explorer</div>
            <div className="header-sub">Northwind meta (bitset elements)</div>
          </div>
        </header>

        <main className="content">
          <div className="desc">
            Shows SQLite schema metadata ingested as 4096-bit Elements (tables, columns, relations) under the <code>or</code>{" "}
            prefix.
          </div>

          {error ? <div className="banner error">{error}</div> : null}

          <div className="grid3">
            <div className="panel" style={{ marginTop: 0 }} data-testid="schema-tables">
              <div className="panel-title">Tables</div>
              <label className="label">Filter</label>
              <input
                className="input"
                placeholder="Substring filter"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
              />
              <div className="row">
                <button
                  className="btn"
                  onClick={() => {
                    setLoadingTables(true);
                    setError(null);
                    apiGetJson<SchemaTablesResponse>("/api/v1/schema/tables?ns=or")
                      .then((data) => setTables(Array.isArray(data.tables) ? data.tables : []))
                      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
                      .finally(() => setLoadingTables(false));
                  }}
                  disabled={loadingTables}
                >
                  Refresh list
                </button>
                <a className="btn" href="/" title="Open Examples in the main GUI">
                  Run Northwind import…
                </a>
              </div>

              {loadingTables ? (
                <div className="muted">Loading tables…</div>
              ) : !filteredTables.length ? (
                <div className="muted">
                  No schema metadata found. Run the <code>northwind_compare</code> import in <code>Examples</code> with namespace{" "}
                  <code>or</code>.
                </div>
              ) : (
                <div className="table-wrap" style={{ marginTop: 12 }}>
                  <table className="table" style={{ minWidth: 0 }}>
                    <thead>
                      <tr>
                        <th>Table</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filteredTables.map((t) => {
                        const isSel = selected === t.table;
                        return (
                          <tr
                            key={t.table}
                            data-testid={`schema-table-row-${safeId(t.table)}`}
                            className={"clickable " + (isSel ? "selected" : "")}
                            onClick={() => setSelected(t.table)}
                          >
                            <td>
                              <code>{t.table}</code>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div className="panel" style={{ marginTop: 0 }} data-testid="schema-columns">
              <div className="panel-title">Columns</div>
              {!selected ? (
                <div className="muted">Select a table to view columns.</div>
              ) : loadingTable && !tableResp ? (
                <div className="muted">Loading…</div>
              ) : tableResp ? (
                <>
                  <div className="help" style={{ marginTop: 0 }}>
                    Selected table: <code>{tableResp.table}</code>
                  </div>
                  {!tableResp.columns.length ? (
                    <div className="muted">No columns found.</div>
                  ) : (
                    <div className="table-wrap" style={{ marginTop: 12 }}>
                      <table className="table" style={{ minWidth: 0 }}>
                        <thead>
                          <tr>
                            <th>Column</th>
                            <th>Type</th>
                            <th>NULL</th>
                            <th>PK</th>
                            <th>FK</th>
                            <th>Idx</th>
                          </tr>
                        </thead>
                        <tbody>
                          {tableResp.columns.map((c) => (
                            <tr key={c.name} data-testid={`schema-column-row-${safeId(c.name)}`}>
                              <td>
                                <code>{c.name}</code>
                              </td>
                              <td>{c.type_family || "—"}</td>
                              <td>{c.not_null == null ? "—" : c.not_null ? "NOT NULL" : "NULL"}</td>
                              <td>{c.is_pk ? "PK" : ""}</td>
                              <td>{c.is_fk ? "FK" : ""}</td>
                              <td>{c.has_index ? "IDX" : ""}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              ) : (
                <div className="muted">No data.</div>
              )}
            </div>

            <div className="panel" style={{ marginTop: 0 }} data-testid="schema-relations">
              <div className="panel-title">Relations</div>
              {!selected ? (
                <div className="muted">Select a table to view relations.</div>
              ) : loadingTable && !tableResp ? (
                <div className="muted">Loading…</div>
              ) : tableResp ? (
                <>
                  {!tableResp.relations.length ? (
                    <div className="muted">No relations found.</div>
                  ) : (
                    <div className="table-wrap" style={{ marginTop: 12 }}>
                      <table className="table" style={{ minWidth: 0 }}>
                        <thead>
                          <tr>
                            <th>From</th>
                            <th>To</th>
                            <th>Card</th>
                            <th>Child required</th>
                            <th>ON DELETE</th>
                            <th>ON UPDATE</th>
                          </tr>
                        </thead>
                        <tbody>
                          {tableResp.relations.map((r) => (
                            <tr
                              key={r.key}
                              data-testid={`schema-relation-row-${safeId(r.from_table)}-${safeId(r.to_table)}-${safeId(r.fk)}`}
                            >
                              <td>
                                <code>{r.from_table}</code>
                              </td>
                              <td>
                                <code>{r.to_table}</code>
                              </td>
                              <td>{r.cardinality || "—"}</td>
                              <td>{yesNoUnknown(r.child_required)}</td>
                              <td>{r.on_delete || "—"}</td>
                              <td>{r.on_update || "—"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </>
              ) : (
                <div className="muted">No data.</div>
              )}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}

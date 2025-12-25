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

type DataInfoResponse = {
  ns: string;
  prefix: string;
  registry_key: string;
  total: number;
  counts_by_table: Record<string, number>;
};

type CompareCondition = { column: string; op: "=" | "<" | "<=" | ">" | ">="; value: string };

type CompareResponse = {
  ns: string;
  prefix: string;
  table: string;
  sql: { query: string; params: string[] };
  bitset_filter: { conditions: string[]; bits: number[] };
  results: {
    sql: { count: number; ids: string[] };
    bitset: { count: number; ids: string[] };
    intersection: { count: number; ids: string[] };
    only_sql: { count: number; ids: string[] };
    only_bitset: { count: number; ids: string[] };
  };
  elapsed_ms: number;
};

type IngestResponse = {
  ns: string;
  prefix: string;
  created_by_table: Record<string, number>;
  processed_by_table: Record<string, number>;
  elapsed_ms: number;
};

const TABLES = ["Customers", "Products", "Orders", "OrderDetails", "Categories"] as const;
type TableToken = (typeof TABLES)[number];

const TABLE_COLUMNS: Record<TableToken, string[]> = {
  Customers: ["Country", "City"],
  Products: ["CategoryID", "UnitPrice"],
  Categories: ["CategoryID"],
  Orders: ["OrderYear"],
  OrderDetails: ["Quantity", "Discount"],
};

function safeId(s: string) {
  return String(s || "").replace(/[^a-z0-9_-]/gi, "-");
}

function defaultConditions(table: TableToken): CompareCondition[] {
  if (table === "Customers") return [{ column: "Country", op: "=", value: "USA" }];
  if (table === "Products") return [{ column: "CategoryID", op: "=", value: "1" }];
  if (table === "Orders") return [{ column: "OrderYear", op: "=", value: "1997" }];
  if (table === "OrderDetails") return [{ column: "Quantity", op: ">", value: "20" }];
  return [{ column: "CategoryID", op: "=", value: "1" }];
}

function presets(table: TableToken): { id: string; title: string; conditions: CompareCondition[] }[] {
  if (table === "Customers") {
    return [
      { id: "cust-country-usa", title: "Country = USA", conditions: [{ column: "Country", op: "=", value: "USA" }] },
      { id: "cust-country-uk", title: "Country = UK", conditions: [{ column: "Country", op: "=", value: "UK" }] },
      { id: "cust-city-london", title: "City = London", conditions: [{ column: "City", op: "=", value: "London" }] },
      {
        id: "cust-uk-london",
        title: "Country = UK AND City = London",
        conditions: [
          { column: "Country", op: "=", value: "UK" },
          { column: "City", op: "=", value: "London" },
        ],
      },
    ];
  }
  if (table === "Products") {
    return [
      { id: "prod-cat-1", title: "CategoryID = 1", conditions: [{ column: "CategoryID", op: "=", value: "1" }] },
      { id: "prod-price-lt10", title: "UnitPrice < 10", conditions: [{ column: "UnitPrice", op: "<", value: "10" }] },
      { id: "prod-price-ge50", title: "UnitPrice >= 50", conditions: [{ column: "UnitPrice", op: ">=", value: "50" }] },
    ];
  }
  if (table === "Orders") {
    return [
      { id: "ord-1996", title: "OrderYear = 1996", conditions: [{ column: "OrderYear", op: "=", value: "1996" }] },
      { id: "ord-1997", title: "OrderYear = 1997", conditions: [{ column: "OrderYear", op: "=", value: "1997" }] },
      { id: "ord-1998", title: "OrderYear = 1998", conditions: [{ column: "OrderYear", op: "=", value: "1998" }] },
    ];
  }
  if (table === "OrderDetails") {
    return [
      { id: "od-qty-gt20", title: "Quantity > 20", conditions: [{ column: "Quantity", op: ">", value: "20" }] },
      { id: "od-discount", title: "Discount > 0", conditions: [{ column: "Discount", op: ">", value: "0" }] },
    ];
  }
  return [{ id: "cat-1", title: "CategoryID = 1", conditions: [{ column: "CategoryID", op: "=", value: "1" }] }];
}

export default function NorthwindDataCompare() {
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<DataInfoResponse | null>(null);
  const [loadingInfo, setLoadingInfo] = useState(false);
  const [loadingIngest, setLoadingIngest] = useState(false);
  const [ingestStatus, setIngestStatus] = useState<string | null>(null);

  const [table, setTable] = useState<TableToken>("Customers");
  const [conds, setConds] = useState<CompareCondition[]>(() => defaultConditions("Customers"));
  const [loadingCompare, setLoadingCompare] = useState(false);
  const [compare, setCompare] = useState<CompareResponse | null>(null);

  const availableColumns = useMemo(() => TABLE_COLUMNS[table], [table]);
  const tableCount = useMemo(() => (info?.counts_by_table ? info.counts_by_table[table] : 0) || 0, [info, table]);

  function refreshInfo() {
    const ctrl = new AbortController();
    setLoadingInfo(true);
    setError(null);
    apiGetJson<DataInfoResponse>("/api/v1/explorer/northwind/data_info?ns=or", ctrl.signal)
      .then((d) => setInfo(d))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoadingInfo(false));
    return () => ctrl.abort();
  }

  useEffect(() => {
    return refreshInfo();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setConds(defaultConditions(table));
    setCompare(null);
  }, [table]);

  async function runIngest(reset: boolean) {
    setLoadingIngest(true);
    setIngestStatus(null);
    setError(null);
    try {
      const data = await apiPostJson<IngestResponse>("/api/v1/explorer/northwind/data_ingest", {
        ns: "or",
        reset,
        tables: TABLES,
      });
      setIngestStatus(
        `Ingested rows: ${Object.entries(data.created_by_table)
          .map(([k, v]) => `${k}=${v}`)
          .join(", ")} (elapsed ${data.elapsed_ms}ms)`,
      );
      refreshInfo();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadingIngest(false);
    }
  }

  async function runCompare() {
    setLoadingCompare(true);
    setError(null);
    setCompare(null);
    try {
      const data = await apiPostJson<CompareResponse>("/api/v1/explorer/northwind/compare", {
        ns: "or",
        table,
        predicate: { type: "and", conditions: conds },
        sample: 25,
      });
      setCompare(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoadingCompare(false);
    }
  }

  return (
    <div className="app" data-testid="data-root">
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
          <a className="nav-item" href="/explorer/schema/" title="Schema Explorer">
            Schema
          </a>
          <span className="nav-item active">Data</span>
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
            <div className="header-title">Northwind: Data vs Bitsets</div>
            <div className="header-sub">Compare SQL results vs bucketed row bitsets</div>
          </div>
        </header>

        <main className="content">
          <div className="desc">
            Ingests row data into Redis as <code>or:data:&lt;Table&gt;:&lt;RowId&gt;</code> → 4096-bit integers, then compares
            SQL filtering vs bitset filtering.
          </div>

          {error ? <div className="banner error">{error}</div> : null}
          {ingestStatus ? <div className="banner">{ingestStatus}</div> : null}

          <div className="grid3">
            <div className="panel" style={{ marginTop: 0 }} data-testid="data-left">
              <div className="panel-title">Ingest + Tables</div>
              <div className="row" style={{ marginTop: 0 }}>
                <button className="btn primary" data-testid="data-ingest" onClick={() => void runIngest(false)} disabled={loadingIngest}>
                  Run data ingest…
                </button>
                <button className="btn" onClick={() => void runIngest(true)} disabled={loadingIngest}>
                  Reset + ingest
                </button>
                <button className="btn" onClick={() => refreshInfo()} disabled={loadingInfo}>
                  Refresh
                </button>
              </div>
              <div className="help">
                Registry: <code>{info?.registry_key || "—"}</code>
                <br />
                Total ingested: <code>{info ? info.total : "—"}</code>
              </div>

              <div className="table-wrap" style={{ marginTop: 12 }}>
                <table className="table" style={{ minWidth: 0 }}>
                  <thead>
                    <tr>
                      <th>Table</th>
                      <th style={{ width: 90 }}>Rows</th>
                    </tr>
                  </thead>
                  <tbody>
                    {TABLES.map((t) => {
                      const isSel = table === t;
                      const cnt = (info?.counts_by_table || {})[t] || 0;
                      return (
                        <tr
                          key={t}
                          data-testid={`data-table-row-${safeId(t)}`}
                          className={"clickable " + (isSel ? "selected" : "")}
                          onClick={() => setTable(t)}
                        >
                          <td>
                            <code>{t}</code>
                          </td>
                          <td>{cnt || "—"}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              <div className="panel" style={{ marginTop: 12 }}>
                <div className="panel-title">Presets</div>
                <div className="help" style={{ marginTop: 0 }}>
                  Table: <code>{table}</code>
                </div>
                <div className="row">
                  {presets(table).map((p) => (
                    <button
                      key={p.id}
                      className="btn"
                      data-testid={`data-preset-${safeId(p.id)}`}
                      onClick={() => setConds(p.conditions)}
                      disabled={loadingCompare}
                      title={p.title}
                    >
                      {p.title}
                    </button>
                  ))}
                </div>
                {!tableCount ? (
                  <div className="muted" style={{ marginTop: 8 }}>
                    No <code>or:data:{table}:*</code> keys found yet. Run ingest first.
                  </div>
                ) : null}
              </div>
            </div>

            <div className="panel" style={{ marginTop: 0 }} data-testid="data-predicate">
              <div className="panel-title">Predicate (AND)</div>
              <div className="help" style={{ marginTop: 0 }}>
                Only a small, bucketed subset is supported in v1 to keep the demo deterministic.
              </div>

              {conds.length ? (
                <div className="table-wrap" style={{ marginTop: 12 }}>
                  <table className="table" style={{ minWidth: 0 }}>
                    <thead>
                      <tr>
                        <th style={{ width: 180 }}>Column</th>
                        <th style={{ width: 90 }}>Op</th>
                        <th>Value</th>
                        <th style={{ width: 60 }} />
                      </tr>
                    </thead>
                    <tbody>
                      {conds.map((c, idx) => (
                        <tr key={idx} data-testid={`data-cond-row-${idx}`}>
                          <td>
                            <select
                              className="input"
                              value={c.column}
                              onChange={(e) => {
                                const next = [...conds];
                                next[idx] = { ...c, column: e.target.value };
                                setConds(next);
                              }}
                            >
                              {availableColumns.map((col) => (
                                <option key={col} value={col}>
                                  {col}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td>
                            <select
                              className="input"
                              value={c.op}
                              onChange={(e) => {
                                const op = e.target.value as CompareCondition["op"];
                                const next = [...conds];
                                next[idx] = { ...c, op };
                                setConds(next);
                              }}
                            >
                              <option value="=">=</option>
                              <option value="<">&lt;</option>
                              <option value="<=">&lt;=</option>
                              <option value=">">&gt;</option>
                              <option value=">=">&gt;=</option>
                            </select>
                          </td>
                          <td>
                            <input
                              className="input"
                              value={c.value}
                              onChange={(e) => {
                                const next = [...conds];
                                next[idx] = { ...c, value: e.target.value };
                                setConds(next);
                              }}
                              placeholder="Value"
                            />
                          </td>
                          <td>
                            <button
                              className="btn"
                              onClick={() => setConds(conds.filter((_, j) => j !== idx))}
                              disabled={conds.length <= 1}
                              title="Remove condition"
                            >
                              ×
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="muted">Add at least one condition.</div>
              )}

              <div className="row">
                <button
                  className="btn"
                  onClick={() =>
                    setConds([
                      ...conds,
                      { column: availableColumns[0] || "Country", op: "=", value: "" },
                    ])
                  }
                  disabled={conds.length >= 10}
                >
                  Add condition
                </button>
                <button className="btn primary" data-testid="data-run-compare" onClick={() => void runCompare()} disabled={loadingCompare}>
                  Run comparison
                </button>
              </div>

              {compare ? (
                <div className="help">
                  SQL: <code>{compare.sql.query}</code>
                  <br />
                  Bit filter bits: <code>{compare.bitset_filter.bits.join(", ") || "—"}</code>
                </div>
              ) : null}
            </div>

            <div className="panel" style={{ marginTop: 0 }} data-testid="data-results">
              <div className="panel-title">Results</div>
              {loadingCompare ? (
                <div className="muted">Running comparison…</div>
              ) : !compare ? (
                <div className="muted">Run a comparison to see SQL vs bitset results.</div>
              ) : (
                <>
                  <div className="grid2" style={{ marginTop: 0 }}>
                    <div className="panel" style={{ marginTop: 0 }}>
                      <div className="panel-title">Counts</div>
                      <div className="help" data-testid="data-counts">
                        SQL: <code data-testid="data-count-sql">{compare.results.sql.count}</code>
                        <br />
                        Bitset: <code data-testid="data-count-bitset">{compare.results.bitset.count}</code>
                        <br />
                        Intersection: <code data-testid="data-count-intersection">{compare.results.intersection.count}</code>
                        <br />
                        Only SQL: <code data-testid="data-count-only-sql">{compare.results.only_sql.count}</code>
                        <br />
                        Only Bitset: <code data-testid="data-count-only-bitset">{compare.results.only_bitset.count}</code>
                        <br />
                        Elapsed: <code>{compare.elapsed_ms}ms</code>
                      </div>
                    </div>
                    <div className="panel" style={{ marginTop: 0 }}>
                      <div className="panel-title">Intersection (sample)</div>
                      {!compare.results.intersection.ids.length ? (
                        <div className="muted">No matches.</div>
                      ) : (
                        <div className="out" data-testid="data-intersection">
                          {compare.results.intersection.ids.join("\n")}
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="grid3" style={{ marginTop: 12 }}>
                    <div className="panel" style={{ marginTop: 0 }}>
                      <div className="panel-title">Only SQL</div>
                      {!compare.results.only_sql.ids.length ? <div className="muted">—</div> : <div className="out">{compare.results.only_sql.ids.join("\n")}</div>}
                    </div>
                    <div className="panel" style={{ marginTop: 0 }}>
                      <div className="panel-title">Only Bitset</div>
                      {!compare.results.only_bitset.ids.length ? (
                        <div className="muted">—</div>
                      ) : (
                        <div className="out">{compare.results.only_bitset.ids.join("\n")}</div>
                      )}
                    </div>
                    <div className="panel" style={{ marginTop: 0 }}>
                      <div className="panel-title">Samples</div>
                      <div className="help" style={{ marginTop: 0 }}>
                        SQL sample IDs: <code>{compare.results.sql.ids.slice(0, 6).join(", ") || "—"}</code>
                        <br />
                        Bitset sample IDs: <code>{compare.results.bitset.ids.slice(0, 6).join(", ") || "—"}</code>
                      </div>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}


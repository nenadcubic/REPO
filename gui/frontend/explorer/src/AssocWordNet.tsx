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

type BoardCell = { synset: string; lemma: string; domain: string };
type BoardColumn = { id: "A" | "B" | "C" | "D"; synset: string; lemma: string; domain: string; clues: BoardCell[] };
type Board = { id: string; final: BoardCell; columns: BoardColumn[]; note?: string };

type ExplainRel = { type: string; via: string[]; shared_bits: { relation_bits: string[]; domain_bits: string[] } };
type ExplainClue = { lemma: string; synset: string; relation_to_column: ExplainRel };
type ExplainColumn = { id: string; lemma: string; synset: string; relation_to_final: ExplainRel; clues: ExplainClue[] };
type Explain = { id: string; final: { synset: string; lemma: string }; columns: ExplainColumn[] };

type AssocStatus = {
  wordnet: {
    kind: "none" | "demo_or_small" | "full_or_partial";
    wn_all_count: number;
    wn_noun_count: number;
    demo_board_present: boolean;
  };
  ingest_commands: { host_python: string; docker_network: string; note: string };
};

function safeId(s: string) {
  return String(s || "").replace(/[^a-z0-9_-]/gi, "-");
}

type CellKey = "final" | "A" | "B" | "C" | "D" | `${"A" | "B" | "C" | "D"}${1 | 2 | 3 | 4}`;

function cellLabel(cell: CellKey) {
  return cell === "final" ? "Final" : cell;
}

function cellFromRow(col: BoardColumn, idx: number): CellKey {
  return `${col.id}${(idx + 1) as 1 | 2 | 3 | 4}`;
}

function boardCellByKey(board: Board, cell: CellKey): BoardCell | null {
  if (cell === "final") return board.final;
  if (cell.length === 1) {
    const col = board.columns.find((c) => c.id === cell);
    return col ? { synset: col.synset, lemma: col.lemma, domain: col.domain } : null;
  }
  const colId = cell[0] as "A" | "B" | "C" | "D";
  const n = Number(cell.slice(1)) - 1;
  const col = board.columns.find((c) => c.id === colId);
  return col?.clues?.[n] || null;
}

export default function AssocWordNet() {
  const [error, setError] = useState<string | null>(null);
  const [note, setNote] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<AssocStatus | null>(null);

  const [board, setBoard] = useState<Board | null>(null);
  const [explain, setExplain] = useState<Explain | null>(null);
  const [inputs, setInputs] = useState<Record<string, string>>({});
  const [correct, setCorrect] = useState<Record<string, boolean | null>>({});
  const [selected, setSelected] = useState<CellKey>("final");

  const mode = useMemo(() => {
    const p = new URLSearchParams(window.location.search);
    return (p.get("mode") || "").trim();
  }, []);

  async function loadStatus() {
    try {
      const s = await apiGetJson<AssocStatus>("/api/v1/assoc/status");
      setStatus(s);
    } catch {
      setStatus(null);
    }
  }

  async function loadBoard(nextMode?: string) {
    const ctrl = new AbortController();
    setLoading(true);
    setError(null);
    setNote(null);
    setExplain(null);
    setInputs({});
    setCorrect({});
    try {
      const url =
        nextMode === "demo" || mode === "demo"
          ? "/api/v1/assoc/board/random?mode=demo"
          : "/api/v1/assoc/board/random";
      const data = await apiGetJson<Board>(url, ctrl.signal);
      setBoard(data);
      setNote((data as Board).note || null);
      setSelected("final");
      void loadStatus();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
    return () => ctrl.abort();
  }

  useEffect(() => {
    void loadBoard();
    void loadStatus();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function ensureExplain() {
    if (!board) return;
    if (explain) return;
    try {
      const exp = await apiGetJson<Explain>(`/api/v1/assoc/board/${encodeURIComponent(board.id)}/explain`);
      setExplain(exp);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function checkCell(cell: CellKey) {
    if (!board) return;
    const val = inputs[cell] || "";
    try {
      const out = await apiPostJson<{ correct: boolean }>(`/api/v1/assoc/board/${encodeURIComponent(board.id)}/check`, {
        cell,
        guess: val,
      });
      setCorrect((m) => ({ ...m, [cell]: !!out.correct }));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  async function hint(cell: CellKey, kind: "first_letter" | "reveal") {
    if (!board) return;
    try {
      const out = await apiPostJson<{ hint: string }>(`/api/v1/assoc/board/${encodeURIComponent(board.id)}/hint`, { cell, kind });
      if (kind === "reveal") setInputs((m) => ({ ...m, [cell]: out.hint }));
      else setError(`Hint (${cellLabel(cell)}): ${out.hint}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  function solveAll() {
    if (!board) return;
    const next: Record<string, string> = {};
    const ok: Record<string, boolean> = {};
    next.final = board.final.lemma;
    ok.final = true;
    for (const col of board.columns) {
      next[col.id] = col.lemma;
      ok[col.id] = true;
      for (let i = 0; i < col.clues.length; i++) {
        const cell = cellFromRow(col, i);
        next[cell] = col.clues[i].lemma;
        ok[cell] = true;
      }
    }
    setInputs((m) => ({ ...m, ...next }));
    setCorrect((m) => ({ ...m, ...ok }));
  }

  async function copyText(label: string, text: string) {
    try {
      await navigator.clipboard.writeText(text);
      setError(`${label} copied to clipboard.`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  const explainText = useMemo(() => {
    if (!board || !explain) return null;
    if (selected === "final") return `Final: ${explain.final.lemma} (${explain.final.synset})`;
    if (selected.length === 1) {
      const col = explain.columns.find((c) => c.id === selected);
      if (!col) return null;
      const r = col.relation_to_final;
      return `Column ${selected} → Final: ${r.type}${r.via.length ? ` via ${r.via.join(" → ")}` : ""} • shared domains: ${r.shared_bits.domain_bits.join(", ") || "—"}`;
    }
    const colId = selected[0];
    const idx = Number(selected.slice(1)) - 1;
    const col = explain.columns.find((c) => c.id === colId);
    const clue = col?.clues?.[idx];
    if (!col || !clue) return null;
    const r = clue.relation_to_column;
    return `${selected} → ${colId}: ${r.type}${r.via.length ? ` via ${r.via.join(" → ")}` : ""} • shared domains: ${r.shared_bits.domain_bits.join(", ") || "—"}`;
  }, [board, explain, selected]);

  function CellInput({ cell }: { cell: CellKey }) {
    const val = inputs[cell] || "";
    const c = correct[cell];
    const cls = "input assoc-input " + (c === true ? "correct" : c === false ? "wrong" : "");
    return (
      <div data-testid={`assoc-cell-${safeId(cell)}`} data-correct={c == null ? "" : c ? "true" : "false"}>
        <label className="label" style={{ marginTop: 0 }}>
          {cellLabel(cell)}
        </label>
        <input
          className={cls}
          value={val}
          onFocus={() => {
            setSelected(cell);
            void ensureExplain();
          }}
          onChange={(e) => setInputs((m) => ({ ...m, [cell]: e.target.value }))}
          onKeyDown={(e) => {
            if (e.key === "Enter") void checkCell(cell);
          }}
        />
        <div className="row" style={{ marginTop: 8 }}>
          <button className="btn" onClick={() => checkCell(cell)} disabled={!board}>
            Check
          </button>
          <button className="btn" onClick={() => hint(cell, "first_letter")} disabled={!board}>
            Hint
          </button>
          <button className="btn" onClick={() => hint(cell, "reveal")} disabled={!board}>
            Reveal
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="app" data-testid="assoc-root">
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
          <a className="nav-item" href="/explorer/data/" title="Northwind: Data vs Bitsets">
            Data
          </a>
          <span className="nav-item active">Associations</span>
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
            <div className="header-title">Associations (WordNet)</div>
            <div className="header-sub">WordNet-backed bitsets + explanations</div>
          </div>
        </header>

        <main className="content">
          <div className="desc">4×4 clues → 4 column solutions → final solution (WordNet synsets).</div>

          {error ? <div className="banner error">{error}</div> : null}
          {note ? <div className="banner">{note}</div> : null}

          <div className="panel" style={{ marginTop: 0 }} data-testid="assoc-status">
            <div className="panel-title">WordNet status</div>
            {!status ? (
              <div className="muted">Loading…</div>
            ) : (
              <>
                <div className="help" style={{ marginTop: 0 }}>
                  wn:all: <code>{status.wordnet.wn_all_count}</code> • nouns: <code>{status.wordnet.wn_noun_count}</code> • kind:{" "}
                  <code>{status.wordnet.kind}</code>
                </div>
                <div className="help" style={{ marginTop: 0 }}>
                  {status.wordnet.kind === "full_or_partial"
                    ? "Full/partial WordNet detected; “New board” should generate randomly."
                    : status.wordnet.kind === "demo_or_small"
                      ? "Only demo/small WordNet detected; “New board” may fall back to demo."
                      : "No WordNet detected; use “Load demo” or ingest WordNet."}
                </div>
                <div className="row" style={{ marginTop: 0 }}>
                  <button
                    className="btn"
                    data-testid="assoc-copy-ingest-docker"
                    onClick={() => void copyText("Docker ingest command", status.ingest_commands.docker_network)}
                  >
                    Copy Docker ingest command
                  </button>
                  <button
                    className="btn"
                    data-testid="assoc-copy-ingest-host"
                    onClick={() => void copyText("Host ingest command", status.ingest_commands.host_python)}
                  >
                    Copy host ingest command
                  </button>
                </div>
                <div className="muted">{status.ingest_commands.note}</div>
              </>
            )}
          </div>

          <div className="row" style={{ marginTop: 0 }}>
            <button className="btn primary" onClick={() => void loadBoard()} disabled={loading}>
              New board
            </button>
            <button className="btn" onClick={() => void loadBoard("demo")} disabled={loading}>
              Load demo
            </button>
            <button className="btn" onClick={solveAll} disabled={!board}>
              Solve all
            </button>
            <button className="btn" onClick={() => void ensureExplain()} disabled={!board}>
              Explain
            </button>
          </div>

          {!board ? (
            <div className="muted">{loading ? "Loading…" : "No board loaded."}</div>
          ) : (
            <div className="grid3">
              <div className="panel" style={{ marginTop: 0 }} data-testid="assoc-grid">
                <div className="panel-title">Board</div>
                <div className="grid2" style={{ gridTemplateColumns: "repeat(2, minmax(0, 1fr))" }}>
                  <CellInput cell="A1" />
                  <CellInput cell="B1" />
                  <CellInput cell="A2" />
                  <CellInput cell="B2" />
                  <CellInput cell="A3" />
                  <CellInput cell="B3" />
                  <CellInput cell="A4" />
                  <CellInput cell="B4" />
                  <CellInput cell="A" />
                  <CellInput cell="B" />
                </div>
                <div className="grid2" style={{ gridTemplateColumns: "repeat(2, minmax(0, 1fr))", marginTop: 16 }}>
                  <CellInput cell="C1" />
                  <CellInput cell="D1" />
                  <CellInput cell="C2" />
                  <CellInput cell="D2" />
                  <CellInput cell="C3" />
                  <CellInput cell="D3" />
                  <CellInput cell="C4" />
                  <CellInput cell="D4" />
                  <CellInput cell="C" />
                  <CellInput cell="D" />
                </div>
                <div className="panel" style={{ marginTop: 16 }}>
                  <div className="panel-title">Final</div>
                  <CellInput cell="final" />
                </div>
              </div>

              <div className="panel" style={{ marginTop: 0 }} data-testid="assoc-info">
                <div className="panel-title">Selected</div>
                <div className="help" style={{ marginTop: 0 }}>
                  Selected cell: <code>{cellLabel(selected)}</code>
                </div>
                <div className="help">
                  Target:{" "}
                  <code>
                    {(() => {
                      const c = boardCellByKey(board, selected);
                      return c ? `${c.lemma} (${c.synset})` : "—";
                    })()}
                  </code>
                </div>
              </div>

              <div className="panel" style={{ marginTop: 0 }} data-testid="assoc-explain">
                <div className="panel-title">Explanation</div>
                {!explain ? (
                  <div className="muted">Click “Explain” or focus a cell to load explanations.</div>
                ) : (
                  <pre className="out" style={{ whiteSpace: "pre-wrap", maxHeight: 520 }}>
                    {explainText || "No explanation."}
                  </pre>
                )}
              </div>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

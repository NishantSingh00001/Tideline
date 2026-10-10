import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { SqlEditor } from "../components/SqlEditor.tsx";
import { api, type AiSql, type SavedQuery } from "../lib/api.ts";
import { live, useLiveVersion } from "../lib/live.ts";
import { clock, dayTime, duration, int } from "../lib/format.ts";
import { EXAMPLES, PANELS } from "../../shared/queries.ts";
import { COLUMNS, COLUMN_DOCS } from "../../shared/table.ts";
import { FUNCTION_DOCS } from "../../shared/sql/functions.ts";
import type { Column, Row, Value } from "../../shared/engine/types.ts";

const ALL = [...Object.values(PANELS), ...EXAMPLES].reduce<Record<string, { title: string; sql: string; about: string; tags: string[] }>>((m, q) => ((m[q.id] = q), m), {});

const typeName = (t: string) => (t === "TIMESTAMP_LTZ" ? "TIMESTAMP_LTZ(3)" : t);

function cell(v: Value, col: Column) {
  if (v === null) return <span className="null">NULL</span>;
  if (col.type === "TIMESTAMP_LTZ") return clock(v as number);
  if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
  if (typeof v === "number") return col.type === "DOUBLE" || col.type === "DECIMAL" ? v.toLocaleString("en-IN", { maximumFractionDigits: 4 }) : v.toLocaleString("en-IN");
  return v;
}

const numeric = (c: Column) => ["INT", "BIGINT", "DOUBLE", "DECIMAL"].includes(c.type);

function ResultTable({ schema, rows, breakOn, fresh }: { schema: Column[]; rows: { row: Row; kind?: string; fresh?: boolean }[]; breakOn?: number; fresh?: boolean }) {
  return (
    <div className="table-wrap">
      <table className="data">
        <thead>
          <tr>
            {rows.some((r) => r.kind) && <th>op</th>}
            {schema.map((c, i) => (
              <th key={i} className={numeric(c) ? "n" : undefined} style={numeric(c) ? { textAlign: "right" } : undefined}>
                {c.name}
                <small>{typeName(c.type)}</small>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className={[breakOn !== undefined && i > 0 && rows[i - 1].row[breakOn] !== r.row[breakOn] ? "window-break" : "", fresh && r.fresh ? "fresh" : ""].join(" ")}>
              {r.kind && (
                <td>
                  <span className={`kind ${r.kind === "+I" ? "ins" : r.kind === "-U" || r.kind === "-D" ? "ub" : "ua"}`}>{r.kind}</span>
                </td>
              )}
              {schema.map((c, j) => (
                <td key={j} className={numeric(c) ? "n" : undefined} title={typeof r.row[j] === "string" ? (r.row[j] as string) : undefined}>
                  {cell(r.row[j], c)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Results() {
  const s = useLiveVersion();
  const q = s.sql;
  const [mode, setMode] = useState<"table" | "log">("table");
  const c = q.compiled;
  if (!c || c.explainOnly) return null;
  const t = q.tick;
  const elapsed = Date.now() - q.startedAt;
  const startCol = c.schema.findIndex((x) => x.name === "window_start");
  let body: ReactNode;
  let count = 0;
  if (c.changelog) {
    const list = q.view.list().sort((a, b) => b.updated - a.updated);
    count = list.length;
    const latest = list[0]?.updated ?? 0;
    body =
      mode === "table" ? (
        <ResultTable schema={c.schema} rows={list.slice(0, 300).map((r) => ({ row: r.row, fresh: r.updated > latest - 20 }))} fresh />
      ) : (
        <ResultTable schema={c.schema} rows={[...q.changelog].reverse().slice(0, 200).map((m) => ({ row: m.row, kind: m.kind }))} />
      );
  } else {
    // Newest window first, but rows inside a window keep their order (Top-N ranks).
    let rows: Row[];
    if (startCol >= 0) {
      const groups: Row[][] = [];
      for (const r of q.rows) {
        const g = groups[groups.length - 1];
        if (g && g[0][startCol] === r[startCol]) g.push(r);
        else groups.push([r]);
      }
      rows = groups.reverse().flat();
    } else rows = [...q.rows].reverse();
    count = rows.length;
    body = <ResultTable schema={c.schema} rows={rows.slice(0, 300).map((row) => ({ row }))} breakOn={startCol >= 0 ? startCol : undefined} />;
  }
  return (
    <section className="results" aria-live="polite">
      <div className="results-head">
        <h2>{c.changelog ? "Live table" : c.windowed ? "Window results" : "Rows"}</h2>
        <span className="meta">
          {q.running ? "Running for" : t?.failed ? "Failed after" : "Stopped after"} {duration(elapsed)}. {int(t?.received ?? 0)} changes read
          {t?.watermark ? `, watermark at ${clock(t.watermark)}` : ""}, {int(count)} {c.changelog ? "rows" : "results"}
          {c.changelog ? ` from ${int(q.messages)} changelog messages` : ""}.
        </span>
        {c.changelog && (
          <div className="seg" style={{ marginLeft: "auto" }}>
            <button aria-pressed={mode === "table"} onClick={() => setMode("table")}>
              Table
            </button>
            <button aria-pressed={mode === "log"} onClick={() => setMode("log")}>
              Changelog
            </button>
          </div>
        )}
      </div>
      {t?.failed && <div className="err-box">{t.failed}</div>}
      {t?.status?.state === "connecting" && <div className="info-box">Replaying from Kafka…</div>}
      {count === 0 && !t?.failed ? (
        <p className="waiting">
          {c.windowed
            ? `Window results appear when the watermark passes the end of each window (watermark delay ${Math.round(s.delay / 1000)} s). Start from an earlier time to get results at once.`
            : "Waiting for matching changes…"}
        </p>
      ) : (
        body
      )}
      {c.changelog && (
        <p className="panel-note">
          A GROUP BY without a window never closes, so Flink emits a changelog: <span className="kind ins">+I</span> inserts a row, then each change retracts the old row (<span className="kind ub">-U</span>) and adds the new one (
          <span className="kind ua">+U</span>).
        </p>
      )}
    </section>
  );
}

export default function Sql() {
  const [params] = useSearchParams();
  const { id } = useParams();
  const nav = useNavigate();
  const s = useLiveVersion();
  const q = s.sql;
  const initial = ALL[params.get("example") || ""]?.sql || PANELS.pulse.sql;
  const [text, setText] = useState(initial);
  const [from, setFrom] = useState<"now" | number>(10);
  const [showPlan, setShowPlan] = useState(false);
  const [ask, setAsk] = useState("");
  const [aiBusy, setAiBusy] = useState(false);
  const [note, setNote] = useState<{ kind: "info" | "err"; text: string } | null>(null);
  const [shared, setShared] = useState<string | null>(null);

  const run = (sql = text, start = from) => {
    setNote((n) => (n?.kind === "err" ? null : n));
    setShared(null);
    live.runSql(sql, start);
  };

  // Load an example or a shared query, then run it.
  useEffect(() => {
    let cancelled = false;
    if (id) {
      api<SavedQuery>(`/api/queries/${encodeURIComponent(id)}`)
        .then((saved) => {
          if (cancelled) return;
          setText(saved.sql);
          setNote({ kind: "info", text: `Shared query “${saved.title}”, saved ${dayTime(Date.parse(saved.created))}. Viewed ${int(saved.views)} times.` });
          run(saved.sql);
        })
        .catch((e) => !cancelled && setNote({ kind: "err", text: e.message }));
    } else {
      const ex = params.get("example");
      const sql = ALL[ex || ""]?.sql || initial;
      setText(sql);
      run(sql);
    }
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id, params.get("example")]);

  useEffect(() => () => live.stopSql(), []);

  const write = async () => {
    if (!ask.trim()) return;
    setAiBusy(true);
    setNote(null);
    try {
      const r = await api<AiSql>("/api/ai/sql", { method: "POST", json: { question: ask.trim() } });
      setText(r.sql);
      setNote({ kind: "info", text: `${r.explanation} Written by Gemini, then checked by Tideline's SQL parser${r.attempts > 1 ? ` (fixed after ${r.attempts - 1} rejected draft${r.attempts > 2 ? "s" : ""})` : ""}.` });
      run(r.sql);
    } catch (e) {
      setNote({ kind: "err", text: (e as Error).message });
    } finally {
      setAiBusy(false);
    }
  };

  const share = async () => {
    try {
      const title = (text.match(/--\s*(.+)/)?.[1] || Object.values(ALL).find((x) => x.sql === text)?.title || "Flink SQL query").slice(0, 80);
      const r = await api<{ id: string }>("/api/queries", { method: "POST", json: { sql: text, title } });
      const url = `${location.origin}/q/${r.id}`;
      setShared(url);
      try {
        await navigator.clipboard.writeText(url);
      } catch {
        /* clipboard may be blocked */
      }
      nav(`/q/${r.id}`, { replace: true });
    } catch (e) {
      setNote({ kind: "err", text: (e as Error).message });
    }
  };

  const err = q.error;
  const c = q.compiled;
  const fromLabel = useMemo(() => (from === "now" ? "now" : from === 10 ? "10 minutes ago" : "1 hour ago"), [from]);

  return (
    <div className="wrap">
      <header className="page-head">
        <div>
          <h1>Query the stream with Flink SQL</h1>
          <p>
            The <code>recentchange</code> table is the live edit stream. Queries run continuously in your browser on the same engine as the live page, in Apache Flink's dialect: window table functions, window Top-N, changelogs.
          </p>
        </div>
      </header>
      <div className="sql-layout">
        <div>
          <div className="ask">
            <input
              value={ask}
              onChange={(e) => setAsk(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && write()}
              placeholder="Ask in English, e.g. which wikis get the most new articles per minute?"
              aria-label="Describe a query in English"
              maxLength={300}
            />
            <button className="btn" onClick={write} disabled={aiBusy || !ask.trim()}>
              {aiBusy ? "Writing…" : "Write SQL"}
            </button>
          </div>
          <SqlEditor value={text} onChange={setText} onRun={() => run()} error={err} />
          <div className="editor-bar">
            {q.running ? (
              <button className="btn primary" onClick={() => live.stopSql()}>
                Stop
              </button>
            ) : (
              <button className="btn primary" onClick={() => run()}>
                Run
              </button>
            )}
            <label className="field" style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
              Start from
              <select value={String(from)} onChange={(e) => setFrom(e.target.value === "now" ? "now" : Number(e.target.value))} style={{ height: 36 }}>
                <option value="now">now (live)</option>
                <option value="10">10 minutes ago</option>
                <option value="60">1 hour ago</option>
              </select>
            </label>
            <button className="btn" onClick={() => setShowPlan((p) => !p)} aria-pressed={showPlan} disabled={!c}>
              {showPlan ? "Hide plan" : "Show plan"}
            </button>
            <button className="btn" onClick={share} disabled={!text.trim()}>
              Share
            </button>
            <span className="spacer" />
            <span className="kbd">Ctrl + Enter</span>
          </div>
          {shared && (
            <div className="info-box">
              Link copied: <a href={shared}>{shared}</a>
            </div>
          )}
          {note && <div className={note.kind === "err" ? "err-box" : "info-box"}>{note.text}</div>}
          {err && (
            <div className="err-box" role="alert">
              {err.message}
            </div>
          )}
          {c && !err && (showPlan || c.explainOnly) && (
            <div style={{ marginTop: 16 }}>
              <pre className="plan">{c.explain}</pre>
            </div>
          )}
          {c && !err && !c.explainOnly && (
            <p className="muted" style={{ fontSize: 14, margin: "12px 0 0" }}>
              {c.changelog ? "An updating result (changelog)" : c.windowed ? "Append-only: one row per window and key" : "Append-only: rows as they arrive"}, reading from {fromLabel}
              {from !== "now" ? " over a second connection that replays Kafka history" : ""}.
            </p>
          )}
          <Results />
        </div>
        <aside className="side">
          <section>
            <h3>Examples</h3>
            <ul className="examples">
              {Object.entries(ALL).map(([key, ex]) => (
                <li key={key}>
                  <button
                    onClick={() => {
                      if (id) nav(`/sql?example=${key}`);
                      else nav(`/sql?example=${key}`, { replace: true });
                    }}
                  >
                    <b>{ex.title}</b>
                    <span>{ex.about}</span>
                    <span className="chips">
                      {ex.tags.map((t) => (
                        <span key={t} className="chip">
                          {t}
                        </span>
                      ))}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          </section>
          <section>
            <h3>recentchange</h3>
            <ul className="cols">
              {COLUMNS.map((col) => (
                <li key={col.name}>
                  <code>{col.name}</code>
                  <span className="ty">
                    {typeName(col.type)}
                    {col.rowtime ? " rowtime" : ""}
                  </span>
                  <p>{COLUMN_DOCS[col.name]}</p>
                </li>
              ))}
            </ul>
            <p className="panel-note">
              Watermark: <code>event_time - INTERVAL '{Math.round(s.delay / 1000)}' SECOND</code>. The same table definition runs on real Kafka and Flink in the <Link to="/how">pipeline</Link>.
            </p>
          </section>
          <section>
            <h3>Functions</h3>
            <p className="panel-note" style={{ marginTop: 0 }}>
              COUNT, SUM, AVG, MIN, MAX (with DISTINCT and FILTER), ROW_NUMBER for window Top-N, CASE, CAST, and:
            </p>
            <p style={{ font: "400 12px/1.7 var(--mono)", color: "var(--ink-2)", margin: 0 }}>{FUNCTION_DOCS.join(", ")}</p>
          </section>
        </aside>
      </div>
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import { api } from "../lib/api.ts";
import { live, useLiveVersion } from "../lib/live.ts";
import { bytes, clock, duration, int } from "../lib/format.ts";
import type { RecoverMode } from "../worker/protocol.ts";

const MODES: { id: RecoverMode; title: string; about: string }[] = [
  { id: "exactly-once", title: "Restore and replay with deduplication", about: "Load the checkpoint, re-read Kafka from its position, drop events already counted. Exactly once." },
  { id: "at-least-once", title: "Restore and replay without deduplication", about: "Same checkpoint, but replay a little early and keep every event. Some are counted twice: at least once." },
  { id: "at-most-once", title: "Start again from the latest data", about: "No state, no replay. Everything in flight and everything during the outage is gone: at most once." },
];

interface Stored {
  id: string;
  bytes: number;
  at: number;
}

export default function Recovery() {
  const s = useLiveVersion();
  const r = s.rec;
  const [mode, setMode] = useState<RecoverMode>("exactly-once");
  const [stored, setStored] = useState<Stored | null>(null);
  const [storeError, setStoreError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const lastUploaded = useRef<string | null>(null);

  useEffect(() => {
    if (!live.get().rec) live.recReset();
  }, []);

  // Every checkpoint is written to durable storage (Netlify Blobs), like Flink's checkpoint directory.
  useEffect(() => {
    const snap = s.lastSnapshot;
    if (!snap || snap === lastUploaded.current) return;
    lastUploaded.current = snap;
    api<{ id: string; bytes: number }>("/api/checkpoints", { method: "POST", body: snap, headers: { "content-type": "application/json" } })
      .then((res) => {
        setStored({ id: res.id, bytes: res.bytes, at: Date.now() });
        setStoreError(null);
      })
      .catch((e) => setStoreError(e.message));
  }, [s.lastSnapshot]);

  const recover = async () => {
    setBusy(true);
    let snapshot: string | null = null;
    if (mode !== "at-most-once" && stored) {
      try {
        snapshot = JSON.stringify(await api<unknown>(`/api/checkpoints/${stored.id}`));
      } catch (e) {
        setStoreError(`Couldn't read the checkpoint back from the server (${(e as Error).message}); using the copy in this tab.`);
      }
    }
    live.recRecover(mode, snapshot);
    setBusy(false);
  };

  if (!r) return <div className="wrap waiting">Starting the two jobs…</div>;
  const latest = r.checkpoints[0];
  const crashed = r.phase === "crashed";
  const recovering = r.phase === "recovering" || r.phase === "caught-up";
  const verdictOk = r.mode !== null && r.differences === 0 && r.missing === 0 && r.compared > 0;
  const maxCount = Math.max(1, ...r.windows.map((w) => Math.max(w.ref, w.primary ?? 0)));

  return (
    <div className="wrap" style={{ paddingBottom: 60 }}>
      <header className="page-head">
        <div>
          <h1>Crash it, and get the same answer</h1>
          <p>
            Two copies of one job count the live stream in 10-second windows. The reference never fails. The primary checkpoints its state every 15 seconds: its watermark, its open windows, the ids it has seen and its Kafka position. Crash the primary, pick how it recovers, then compare the two.
          </p>
        </div>
      </header>

      <div className="rec-steps">
        <div>
          <h3>1. Checkpoint</h3>
          <p>{latest ? `Checkpoint ${latest.id} at ${clock(latest.at)}: ${int(latest.events)} events in, Kafka position ${clock(latest.position)}.` : "The first checkpoint is taken 15 seconds after the job starts."}</p>
          <p className="muted" style={{ fontSize: 13 }}>
            {stored ? `Stored on the server as ${stored.id} (${bytes(stored.bytes)}).` : storeError ? storeError : "Waiting to store the first checkpoint on the server."}
          </p>
          <button className="btn" onClick={() => live.recCheckpoint()} disabled={crashed}>
            Checkpoint now
          </button>
        </div>
        <div>
          <h3>2. Crash</h3>
          <p>{crashed ? `Crashed at ${clock(r.crashedAt)}. Its state is gone, and output written since the last checkpoint is rolled back. The reference keeps running.` : "Kills the primary: its in-memory state is thrown away."}</p>
          <button className="btn danger" onClick={() => live.recCrash()} disabled={crashed || !latest}>
            Crash the primary
          </button>
        </div>
        <div>
          <h3>3. Recover</h3>
          <div className="opts">
            {MODES.map((m) => (
              <label key={m.id} className="radio">
                <input type="radio" name="mode" checked={mode === m.id} onChange={() => setMode(m.id)} />
                <span>
                  {m.title}
                  <small>{m.about}</small>
                </span>
              </label>
            ))}
          </div>
          <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
            <button className="btn primary" onClick={recover} disabled={!crashed || busy}>
              Recover
            </button>
            <button className="btn" onClick={() => live.recReset()}>
              Start over
            </button>
          </div>
        </div>
      </div>

      {r.mode && (
        <div className={`verdict ${verdictOk ? "ok" : r.compared ? "bad" : ""}`} role="status">
          <strong>{r.phase === "recovering" ? "Catching up…" : verdictOk ? "Identical" : `${r.differences + r.missing} windows differ`}</strong>
          <span>
            {r.phase === "recovering" && `Replaying from Kafka: ${int(r.primaryEvents)} events read so far. `}
            Compared {int(r.compared)} windows with the reference.
            {r.extra ? ` ${int(r.extra)} events were counted twice.` : ""}
            {r.missing ? ` ${int(r.missing)} windows are missing from the primary.` : ""}
            {r.mode === "exactly-once" && r.primaryDuplicates ? ` ${int(r.primaryDuplicates)} replayed events were recognised and dropped.` : ""}
          </span>
        </div>
      )}

      <section className="panel">
        <div className="panel-head">
          <h2>Window counts, reference against primary</h2>
          <span className="aside">
            {crashed ? "Primary down" : recovering ? (r.phase === "caught-up" ? "Primary recovered" : "Primary replaying") : "Both running"}
          </span>
        </div>
        {!r.windows.length ? (
          <p className="waiting">The first windows close about 15 seconds after the job starts.</p>
        ) : (
          <div className="table-wrap" style={{ maxHeight: 560 }}>
            <table className="data">
              <thead>
                <tr>
                  <th>Window</th>
                  <th style={{ width: "40%" }}>Changes</th>
                  <th style={{ textAlign: "right" }}>Reference</th>
                  <th style={{ textAlign: "right" }}>Primary</th>
                  <th style={{ textAlign: "right" }}>Difference</th>
                </tr>
              </thead>
              <tbody>
                {[...r.windows].reverse().map((w) => {
                  const diff = w.primary === null ? null : w.primary - w.ref;
                  return (
                    <tr key={w.start}>
                      <td className="num">
                        {clock(w.start)}–{clock(w.start + 10_000)}
                      </td>
                      <td>
                        <svg width="100%" height="18" viewBox="0 0 100 18" preserveAspectRatio="none" aria-hidden="true">
                          <rect x="0" y="2" height="6" width={(w.ref / maxCount) * 100} fill="var(--s1)" rx="1" />
                          {w.primary !== null && <rect x="0" y="10" height="6" width={(w.primary / maxCount) * 100} fill={diff ? "var(--magenta)" : "var(--s2)"} rx="1" />}
                        </svg>
                      </td>
                      <td className="n">{int(w.ref)}</td>
                      <td className="n">{w.primary === null ? <span className="null">missing</span> : int(w.primary)}</td>
                      <td className="n" style={{ color: diff ? "var(--magenta)" : "var(--ink-3)" }}>
                        {diff === null ? "–" : diff === 0 ? "same" : (diff > 0 ? "+" : "") + diff}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
        <ul className="legend" style={{ marginTop: 10 }}>
          <li>
            <i style={{ background: "var(--s1)" }} />
            Reference
          </li>
          <li>
            <i style={{ background: "var(--s2)" }} />
            Primary, same count
          </li>
          <li>
            <i style={{ background: "var(--magenta)" }} />
            Primary, different count
          </li>
        </ul>
      </section>

      <section className="prose" style={{ marginTop: 30 }}>
        <h2>How exactly-once works here</h2>
        <p>
          A checkpoint is a consistent snapshot taken between two events: the source's Kafka position, the watermark, every open window's partial counts, and the ids of the most recent events. It is written to durable storage on the server, the way Flink writes checkpoints to S3 or HDFS.
        </p>
        <p>
          Wikimedia's stream can only resume from a timestamp, not an exact offset, so recovery re-reads a couple of seconds before the checkpoint. Events in that overlap were already counted; the restored ids recognise them and drop them. Window results are only published when a checkpoint completes, so anything the primary produced after its last checkpoint is rolled back when it crashes and produced again after recovery, as with Flink's two-phase-commit sinks.
        </p>
        <p>
          Recovery time here was {r.restoredFrom && r.crashedAt ? duration(r.crashedAt - r.restoredFrom) : "a few seconds"} of stream to replay. It depends on how long ago the last checkpoint was, which is why checkpoint intervals are a trade-off between recovery time and overhead.
        </p>
      </section>
    </div>
  );
}

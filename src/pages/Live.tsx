import { useState } from "react";
import { Link } from "react-router-dom";
import { AnimatePresence, motion } from "framer-motion";
import { TideChart } from "../components/TideChart.tsx";
import { HBars, LineChart, PulseBars } from "../components/charts.tsx";
import { Pipeline } from "../components/Pipeline.tsx";
import { live, useLiveVersion } from "../lib/live.ts";
import { clock, duration, hm, int, pct, signed, siteLabel, wikiUrl } from "../lib/format.ts";

function Cartouche() {
  const s = useLiveVersion();
  const st = s.source;
  const lag = s.maxTs !== null ? Math.max(0, s.now - s.maxTs) : null;
  const lateShare = st && st.received ? st.behindWatermark / st.received : 0;
  const catchingUp = lag !== null && lag > 60_000 && !s.paused;
  return (
    <dl className="cartouche">
      <div>
        <dt>Throughput</dt>
        <dd>
          {s.rate.toFixed(1)} <small>changes/s</small>
        </dd>
      </div>
      <div>
        <dt>{catchingUp ? "Catching up" : "Behind real time"}</dt>
        <dd>{lag === null ? "–" : duration(lag)}</dd>
      </div>
      <div>
        <dt>Watermark delay</dt>
        <dd>
          <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <input
              type="range"
              min={1}
              max={60}
              step={1}
              value={Math.round(s.delay / 1000)}
              onChange={(e) => live.setDelay(Number(e.target.value) * 1000)}
              aria-label="Watermark delay in seconds"
              style={{ width: "100%", minWidth: 60 }}
            />
            <span>{Math.round(s.delay / 1000)} s</span>
          </label>
        </dd>
      </div>
      <div>
        <dt>Arrived late</dt>
        <dd>
          {int(st?.behindWatermark)} <small>{pct(lateShare, 1)}</small>
        </dd>
      </div>
      <div>
        <dt>Kafka position</dt>
        <dd title="Timestamp of the last record read from eqiad.mediawiki.recentchange">{s.status?.position ? clock(s.status.position) : "–"}</dd>
      </div>
      <div className="controls">
        {s.paused ? (
          <button className="btn primary" onClick={() => live.resume()}>
            Resume
          </button>
        ) : (
          <button className="btn" onClick={() => live.pause()}>
            Pause
          </button>
        )}
        <button className="btn" onClick={() => live.replay(15)} title="Re-read the last 15 minutes from Kafka">
          Replay 15 min
        </button>
      </div>
    </dl>
  );
}

function PeopleBots() {
  const s = useLiveVersion();
  const w = s.editors;
  const latest = w[w.length - 1];
  const share = latest ? latest.people / Math.max(1, latest.people + latest.bots) : null;
  return (
    <section className="panel span-4">
      <div className="panel-head">
        <h2>People and bots</h2>
        <Link className="sql-link" to="/sql?example=editors">
          SQL
        </Link>
      </div>
      {share === null ? (
        <p className="waiting">The first 10-second window closes soon.</p>
      ) : (
        <>
          <div className="big-stat">
            <strong>{pct(share)}</strong>
            <span>
              of changes were made by people
              <br />
              {int(latest.accounts)} accounts in the last window
            </span>
          </div>
          <LineChart points={w.map((x) => ({ x: x.start, y: x.people / Math.max(1, x.people + x.bots) }))} format={(v) => pct(v)} label={(x) => `${clock(x)} window`} domain={[0, 1]} />
          <p className="panel-note">Share of each window's changes made by accounts without a bot flag.</p>
        </>
      )}
    </section>
  );
}

function Late() {
  const s = useLiveVersion();
  const st = s.source;
  const byType = Object.entries(st?.lateByType || {}).sort((a, b) => b[1] - a[1]);
  return (
    <section className="panel span-4">
      <div className="panel-head">
        <h2>Late events</h2>
        <Link className="sql-link" to="/sql?example=late">
          SQL
        </Link>
      </div>
      {!st || !st.behindWatermark ? (
        <p className="waiting">No change has arrived behind the watermark yet.</p>
      ) : (
        <>
          <div className="big-stat">
            <strong>{int(st.behindWatermark)}</strong>
            <span>changes arrived after the watermark had passed their event time</span>
          </div>
          <h3 style={{ font: "500 15px/1.3 var(--sans)", margin: "12px 0 8px", color: "var(--ink-2)" }}>By kind of change</h3>
          <HBars items={byType.map(([k, v]) => ({ label: k, value: v }))} color="var(--magenta)" />
          <h3 style={{ font: "500 15px/1.3 var(--sans)", margin: "14px 0 8px", color: "var(--ink-2)" }}>How far behind</h3>
          <HBars
            items={["under 1 min", "1–5 min", "5–30 min", "over 30 min"].map((label, i) => ({ label, value: st.lateness[i] }))}
            color="var(--magenta)"
          />
          <p className="panel-note">
            Category changes are written by MediaWiki's job queue, often minutes after the edit that caused them, so they are the usual late arrivals. Window queries drop them; raise the watermark delay to keep more.
          </p>
        </>
      )}
    </section>
  );
}

function Trending() {
  const s = useLiveVersion();
  const t = s.trending;
  return (
    <section className="panel span-4">
      <div className="panel-head">
        <h2>Trending articles</h2>
        <Link className="sql-link" to="/sql?example=trending">
          SQL
        </Link>
      </div>
      {!t ? (
        <p className="waiting">Ranks the most-edited articles over a sliding 10-minute window. The first one closes within a minute.</p>
      ) : (
        <>
          <p className="muted" style={{ margin: "0 0 4px", fontSize: 13 }}>
            Edits by people, {hm(t.start)}–{hm(t.end)}
          </p>
          <ol className="ranks">
            <AnimatePresence initial={false}>
              {t.rows.map((r) => {
                const prev = t.previous.get(r.server + "|" + r.title);
                const move = prev === undefined ? "new" : prev > r.rank ? `▲${prev - r.rank}` : prev < r.rank ? `▼${r.rank - prev}` : "";
                return (
                  <motion.li key={r.server + "|" + r.title} layout initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.35 }}>
                    <span className="r">{r.rank}</span>
                    <span className="t">
                      <a href={wikiUrl(r.server, r.title)} target="_blank" rel="noreferrer">
                        {r.title}
                      </a>
                      <small>
                        {siteLabel(r.server)}
                        {move && <span className={`move ${move === "new" ? "new" : move[0] === "▲" ? "up" : "down"}`}>{move}</span>}
                      </small>
                    </span>
                    <span className="n">
                      {r.edits}
                      <small>
                        {r.editors} {r.editors === 1 ? "editor" : "editors"}
                      </small>
                    </span>
                  </motion.li>
                );
              })}
            </AnimatePresence>
          </ol>
        </>
      )}
    </section>
  );
}

function WreckIcon() {
  // The nautical chart symbol for a dangerous wreck.
  return (
    <svg width="26" height="18" viewBox="0 0 26 18" aria-hidden="true">
      <ellipse cx="13" cy="9" rx="12" ry="7.5" fill="none" stroke="var(--magenta)" strokeDasharray="2 2" />
      <path d="M5 9 H21 M10 5 V13 M13 5 V13 M16 5 V13" stroke="var(--ink)" strokeWidth="1.6" />
    </svg>
  );
}

function EditWars() {
  const s = useLiveVersion();
  return (
    <section className="panel span-4">
      <div className="panel-head">
        <h2>Edit wars</h2>
        <span className="aside">MATCH_RECOGNIZE</span>
      </div>
      {!s.wars.length ? (
        <p className="waiting">Watching for three reverts in a row on one page, by at least two people, within 15 minutes.</p>
      ) : (
        <ul className="wars">
          {s.wars.slice(0, 6).map((w) => (
            <li key={w.server_name + w.title + w.start}>
              <WreckIcon />
              <div>
                <a href={wikiUrl(w.server_name, w.title)} target="_blank" rel="noreferrer">
                  {w.title}
                </a>
                <p>
                  {siteLabel(w.server_name)}: three reverts in {duration(w.end - w.start)} by {w.users.slice(0, 3).join(" and ")}
                </p>
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function Feeds() {
  const s = useLiveVersion();
  const [tab, setTab] = useState<"new" | "big">("new");
  const rows = tab === "new" ? s.newPages : s.bigChanges;
  return (
    <section className="panel span-8">
      <div className="panel-head">
        <div className="tabs" role="tablist">
          <button role="tab" aria-selected={tab === "new"} onClick={() => setTab("new")}>
            New articles
          </button>
          <button role="tab" aria-selected={tab === "big"} onClick={() => setTab("big")}>
            Big changes
          </button>
        </div>
        <Link className="sql-link" to={`/sql?example=${tab === "new" ? "newPages" : "bigChanges"}`}>
          SQL
        </Link>
      </div>
      {!rows.length ? (
        <p className="waiting">{tab === "new" ? "New articles appear here the moment they're created." : "Edits that add or remove 10 KB or more appear here."}</p>
      ) : (
        <ul className="feed">
          {rows.map((r) => (
            <li key={r.server + r.title + r.ts + r.value}>
              <time>{clock(r.ts)}</time>
              <span style={{ minWidth: 0 }}>
                <a href={wikiUrl(r.server, r.title)} target="_blank" rel="noreferrer">
                  {r.title}
                </a>
                <span className="who">
                  {siteLabel(r.server)}, by {r.user}
                </span>
              </span>
              <span className="v" style={{ color: tab === "big" && (r.value ?? 0) < 0 ? "var(--magenta)" : "var(--ink)" }}>
                {tab === "new" ? `${int(r.value)} B` : `${signed(r.value)} B`}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export default function Live() {
  const s = useLiveVersion();
  return (
    <>
      <div className="wrap">
        <header className="page-head">
          <div>
            <h1>Wikipedia, edited right now</h1>
            <p>
              Every dot is a change on a Wikimedia wiki, placed at the time it happened. Tideline counts them in 10-second windows and closes each window once the{" "}
              <span className="lede-key">watermark</span> passes it, the way Apache Flink does. Dots behind the line arrived late.
            </p>
          </div>
        </header>
        <div className="neatline">
          <div className="inner">
            <TideChart />
            <Cartouche />
          </div>
        </div>
        {s.status?.state === "retrying" && s.maxTs !== null && (
          <div className="info-box" role="status">
            {s.status.message}. Reconnecting and resuming from the last Kafka position, so nothing is missed.
          </div>
        )}
      </div>
      <div className="wrap grid">
        <section className="panel span-8">
          <div className="panel-head">
            <h2>Changes per 10 seconds</h2>
            <Link className="sql-link" to="/sql?example=pulse">
              SQL
            </Link>
          </div>
          {s.pulse.length ? <PulseBars windows={s.pulse} /> : <p className="waiting">Each bar appears when its window closes.</p>}
          <p className="panel-note">
            Each bar is one 10-second tumbling window. It's drawn once the watermark reaches the window's end minus a millisecond; a change that turns up after that is late and isn't counted. Raise the watermark delay above to wait longer and count more of them.
          </p>
        </section>
        <Trending />
        <PeopleBots />
        <Late />
        <EditWars />
        <Feeds />
        <section className="panel span-4">
          <div className="panel-head">
            <h2>Dead letters</h2>
          </div>
          <div className="big-stat">
            <strong>{int(s.pipeline?.dlq ?? 0)}</strong>
            <span>events failed schema validation</span>
          </div>
          <p className="panel-note">
            Each event is checked against the parts of Wikimedia's <code>mediawiki/recentchange</code> schema the queries rely on. Events that fail go to a dead-letter queue instead of breaking the pipeline.
            {s.pipeline?.dlqSample.length ? ` Latest reason: ${s.pipeline.dlqSample[0]}.` : ""}
          </p>
        </section>
        <section className="panel span-12">
          <div className="panel-head">
            <h2>The running pipeline</h2>
            <span className="aside">{int(s.source?.received)} changes processed</span>
          </div>
          <Pipeline m={s.pipeline} source={s.source} rate={s.rate} delay={s.delay} />
        </section>
      </div>
    </>
  );
}

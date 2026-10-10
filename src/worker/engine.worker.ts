// The engine runs here, off the main thread: it reads the stream, runs every
// job and sends the page a compact update four times a second.

import { toChange, type Change } from "../../shared/change.ts";
import { EditWarDetector, type EditWar } from "../../shared/engine/cep.ts";
import { Runtime, type RuntimeSnapshot } from "../../shared/engine/runtime.ts";
import type { Row } from "../../shared/engine/types.ts";
import { WindowAggOp } from "../../shared/engine/operators.ts";
import { KafkaSim } from "../../shared/kafka/sim.ts";
import { PANELS } from "../../shared/queries.ts";
import { SqlError } from "../../shared/sql/lexer.ts";
import { QueryJob } from "../../shared/sql/job.ts";
import { compile } from "../../shared/sql/planner.ts";
import { projectLane } from "../lib/projects.ts";
import type { Dot, FromWorker, OpMetric, RecoverMode, RecoveryView, ResultBatch, SqlTick, Tick, ToWorker } from "./protocol.ts";
import { StreamSource, type SourceStatus } from "./source.ts";

const ctx = self as unknown as { postMessage(m: unknown): void; onmessage: ((e: MessageEvent<ToWorker>) => void) | null };
const post = (m: FromWorker) => ctx.postMessage(m);
const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

let url = "";
let rt = new Runtime(5000);
let epoch = 0;
let paused = false;
let source: StreamSource | null = null;
let results: Record<string, ResultBatch[]> = {};
let dots: Dot[] = [];
let wars: EditWar[] = [];
let dlq = 0;
const dlqSample: string[] = [];
const arrivals: number[] = [];
let kafka: KafkaSim | null = null;

const PANEL_IDS = ["pulse", "editors", "trending", "newPages", "bigChanges"] as const;
const panelJobs = new Map<string, QueryJob>();
let warDetector: EditWarDetector;

function hash01(s: string) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return ((h >>> 0) % 10000) / 10000;
}

function buildRuntime(delay: number) {
  rt = new Runtime(delay);
  panelJobs.clear();
  for (const id of PANEL_IDS) {
    const q = compile(PANELS[id].sql, { timeZone, watermarkDelay: delay });
    const job = new QueryJob(id, q, (kind, row) => (results[id] ||= []).push({ kind, row }));
    panelJobs.set(id, job);
    rt.add(job);
  }
  warDetector = new EditWarDetector(15 * 60_000, (w) => wars.push(w));
  const det = warDetector;
  rt.add({ id: "editWars", push: (r) => det.push(r), watermark: (w) => det.watermark(w), snapshot: () => det.snapshot(), restore: (s) => det.restore(s) });
  // The recovery demo's reference job lives in the main pipeline.
  rt.add(new QueryJob("recRef", compile(REC_SQL, { timeZone, watermarkDelay: delay }), (_k, row) => rec.onRef(row)));
  if (sql.job && sql.from === "now") rt.add(sql.job);
}

function onMessage(data: string, _pos: number | null) {
  let raw: unknown;
  try {
    raw = JSON.parse(data);
  } catch {
    deadLetter("invalid JSON");
    return;
  }
  const v = toChange(raw);
  if (!v.ok) {
    deadLetter(v.reason);
    return;
  }
  const c = v.change;
  const late = c.ts_ms <= rt.wm;
  if (!rt.ingest(c)) return;
  arrivals.push(Date.now());
  if (dots.length < 600) {
    dots.push({ ts: c.ts_ms, lane: projectLane(c.wiki, c.server_name), late, y: hash01(c.id), title: c.title, server: c.server_name, user: c.user_name, type: c.change_type, bot: c.is_bot });
  }
  kafka?.produce(c.wiki, `${c.wiki}:${c.title}`);
  rec.onMain(c);
}

function deadLetter(reason: string) {
  dlq++;
  dlqSample.unshift(reason);
  if (dlqSample.length > 5) dlqSample.length = 5;
}

function startStream(since: number | null) {
  source?.stop();
  epoch++;
  results = {};
  dots = [];
  wars = [];
  dlq = 0;
  buildRuntime(rt.delay);
  source = new StreamSource(url, onMessage);
  source.start(since);
  paused = false;
}

// ---------------------------------------------------------------- user SQL

const sql: { id: number; job: QueryJob | null; from: "now" | number; rt: Runtime | null; source: StreamSource | null; rows: ResultBatch[]; received: number } = {
  id: 0,
  job: null,
  from: "now",
  rt: null,
  source: null,
  rows: [],
  received: 0,
};

function stopSql() {
  if (sql.job && sql.from === "now") rt.remove(sql.job.id);
  sql.source?.stop();
  sql.source = null;
  sql.rt = null;
  sql.job = null;
  sql.rows = [];
}

function runSql(id: number, text: string, from: "now" | number, tz: string) {
  stopSql();
  sql.id = id;
  try {
    const q = compile(text, { timeZone: tz, watermarkDelay: rt.delay });
    post({ type: "sql.compiled", id, schema: q.schema, changelog: q.changelog, windowed: q.windowed, explain: q.explain, nodes: q.nodes, explainOnly: q.explainOnly });
    if (q.explainOnly) return;
    const job = new QueryJob(`sql-${id}`, q, (kind, row) => {
      sql.rows.push({ kind, row });
      if (sql.rows.length > 3000) sql.rows.splice(0, sql.rows.length - 3000);
    });
    sql.job = job;
    sql.from = from;
    if (from === "now") {
      rt.add(job);
    } else {
      // Replay from Kafka on a separate connection so the live page isn't disturbed.
      const own = new Runtime(rt.delay);
      own.add(job);
      sql.rt = own;
      sql.received = 0;
      sql.source = new StreamSource(url, (data) => {
        try {
          const v = toChange(JSON.parse(data));
          if (v.ok && own.ingest(v.change)) sql.received++;
        } catch {
          /* dead letter */
        }
      });
      sql.source.start(Date.now() - from * 60_000);
    }
  } catch (e) {
    const err = e as SqlError;
    post({ type: "sql.error", id, message: err.message, line: err.pos?.line, col: err.pos?.col, start: err.pos?.offset, end: err.endOffset });
  }
}

function sqlTick(): SqlTick | undefined {
  if (!sql.job) return undefined;
  const rows = sql.rows;
  sql.rows = [];
  const own = sql.rt;
  return {
    id: sql.id,
    rows,
    failed: sql.job.failed,
    stats: sql.job.stats().map((s, i) => ({ ...s, detail: sql.job!.query.ops[i].detail })),
    received: own ? sql.received : rt.stats.received,
    watermark: finite(own ? own.wm : rt.wm),
    status: sql.source?.status ?? null,
    done: sql.job.done,
  };
}

// ---------------------------------------------------------------- recovery demo

const REC_SQL = `SELECT window_start, COUNT(*) AS changes
FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end`;

interface Checkpoint {
  id: number;
  at: number;
  position: number | null;
  json: string;
  events: number;
}

const rec = {
  active: false,
  phase: "running" as RecoveryView["phase"],
  mode: null as RecoverMode | null,
  primary: null as Runtime | null,
  primarySource: null as StreamSource | null,
  ref: new Map<number, number>(),
  /** Window results the primary has committed (survive a crash). */
  committed: new Map<number, number>(),
  /** Results since the last checkpoint (discarded by a crash: two-phase commit). */
  pending: new Map<number, number>(),
  checkpoints: [] as Checkpoint[],
  lastPosition: null as number | null,
  crashedAt: null as number | null,
  restoredFrom: null as number | null,
  lastAuto: 0,
  events: 0,
  sentSnapshot: 0,

  newPrimary(): Runtime {
    const p = new Runtime(rt.delay);
    p.add(new QueryJob("rec", compile(REC_SQL, { timeZone, watermarkDelay: rt.delay }), (_k, row) => this.pending.set(row[0] as number, row[1] as number)));
    return p;
  },
  start() {
    this.reset();
    this.active = true;
  },
  reset() {
    this.primarySource?.stop();
    this.primarySource = null;
    this.primary = this.newPrimary();
    this.phase = "running";
    this.mode = null;
    this.committed.clear();
    this.pending.clear();
    this.checkpoints = [];
    this.crashedAt = null;
    this.restoredFrom = null;
    this.events = 0;
    this.lastAuto = Date.now();
  },
  onRef(row: Row) {
    this.ref.set(row[0] as number, row[1] as number);
    if (this.ref.size > 200) this.ref.delete(this.ref.keys().next().value!);
  },
  onMain(c: Change) {
    if (!this.active || this.phase !== "running" || !this.primary) return;
    if (this.primary.ingest(c)) this.events++;
    this.lastPosition = source?.status.position ?? null;
  },
  checkpoint(): Checkpoint | null {
    if (!this.primary || this.phase === "crashed") return null;
    const position = this.phase === "running" ? source?.status.position ?? null : this.primarySource?.status.position ?? null;
    const snap = this.primary.snapshot(500);
    const json = JSON.stringify({ position, snapshot: snap, committed: [...this.committed, ...this.pending] });
    // The checkpoint completes: pending results are committed (two-phase commit).
    for (const [k, v] of this.pending) this.committed.set(k, v);
    this.pending.clear();
    const cp = { id: (this.checkpoints[0]?.id || 0) + 1, at: Date.now(), position, json, events: this.events };
    this.checkpoints.unshift(cp);
    if (this.checkpoints.length > 6) this.checkpoints.length = 6;
    return cp;
  },
  crash() {
    if (this.phase === "crashed") return;
    this.primarySource?.stop();
    this.primarySource = null;
    this.primary = null;
    this.pending.clear(); // uncommitted output is rolled back
    this.phase = "crashed";
    this.crashedAt = Date.now();
  },
  recover(mode: RecoverMode, external?: string | null) {
    const cp = external ? { json: external } : this.checkpoints[0];
    this.mode = mode;
    this.primary = this.newPrimary();
    this.pending.clear();
    let from: number | null = null;
    if (mode !== "at-most-once" && cp) {
      const saved = JSON.parse(cp.json) as { position: number | null; snapshot: RuntimeSnapshot; committed: [number, number][] };
      this.primary.restore(saved.snapshot);
      this.committed = new Map(saved.committed);
      if (mode === "at-least-once") this.primary.dedup = false;
      from = saved.position !== null ? saved.position - (mode === "at-least-once" ? 15_000 : 2000) : null;
      this.restoredFrom = saved.position;
    } else {
      // No state: start again from the newest data.
      this.restoredFrom = null;
    }
    const p = this.primary;
    this.primarySource = new StreamSource(url, (data) => {
      try {
        const v = toChange(JSON.parse(data));
        if (v.ok && p.ingest(v.change)) this.events++;
      } catch {
        /* ignore */
      }
    });
    this.primarySource.start(from);
    this.phase = "recovering";
  },
  tick() {
    if (!this.active) return;
    if (this.phase === "recovering" && this.primarySource && source && (this.primarySource.status.position ?? 0) >= (source.status.position ?? Infinity) - 1500) this.phase = "caught-up";
    if ((this.phase === "running" || this.phase === "caught-up") && Date.now() - this.lastAuto > 15_000) {
      this.lastAuto = Date.now();
      this.checkpoint();
    }
  },
  view(): RecoveryView {
    const out = new Map<number, number>([...this.committed, ...this.pending]);
    const refWm = finite(rt.wm);
    const primWm = this.primary ? finite(this.primary.wm) : null;
    const starts = [...this.ref.keys()].sort((a, b) => a - b).slice(-30);
    let compared = 0;
    let differences = 0;
    let missing = 0;
    let extra = 0;
    for (const s of starts) {
      const mine = out.get(s);
      if (mine === undefined) {
        // Missing only if the primary's watermark is past this window.
        if (primWm !== null && primWm >= s + 10_000 - 1 && (this.phase !== "running")) missing++;
        continue;
      }
      compared++;
      const ref = this.ref.get(s)!;
      if (mine !== ref) {
        differences++;
        if (mine > ref) extra += mine - ref;
      }
    }
    return {
      phase: this.phase,
      mode: this.mode,
      checkpoints: this.checkpoints.map(({ id, at, position, json, events }) => ({ id, at, position, bytes: json.length, events })),
      primaryEvents: this.events,
      primaryDuplicates: this.primary?.stats.duplicates ?? 0,
      primaryWatermark: primWm,
      refWatermark: refWm,
      windows: starts.map((s) => ({ start: s, ref: this.ref.get(s)!, primary: out.get(s) ?? null })),
      compared,
      differences,
      missing,
      extra,
      // The newest checkpoint's state, sent once so the page can store it durably.
      lastSnapshot: this.checkpoints[0] && this.checkpoints[0].id !== this.sentSnapshot ? ((this.sentSnapshot = this.checkpoints[0].id), this.checkpoints[0].json) : null,
      crashedAt: this.crashedAt,
      restoredFrom: this.restoredFrom,
    };
  },
};

// ---------------------------------------------------------------- ticks

const finite = (n: number) => (Number.isFinite(n) ? n : null);

function opMetrics(job: QueryJob): OpMetric[] {
  return job.stats().map((s, i) => ({ ...s, detail: job.query.ops[i].detail }));
}

let lastKafkaStep = Date.now();

function tick() {
  const now = Date.now();
  while (arrivals.length && arrivals[0] < now - 5000) arrivals.shift();
  const jobs: Record<string, OpMetric[]> = {};
  for (const [id, job] of panelJobs) jobs[id] = opMetrics(job);
  const w = warDetector.stats();
  jobs.editWars = [{ name: warDetector.name, detail: warDetector.detail, ...w }];
  const pulse = panelJobs.get("pulse")?.query.ops.find((o) => o instanceof WindowAggOp) as WindowAggOp | undefined;
  kafka?.step(now - lastKafkaStep);
  lastKafkaStep = now;
  rec.tick();
  const status: SourceStatus = source?.status ?? { state: "stopped", message: "", reconnects: 0, position: null, startedFrom: null, openedAt: null, bytes: 0 };
  const t: Tick = {
    type: "tick",
    now,
    status,
    paused,
    delay: rt.delay,
    maxTs: finite(rt.maxTs),
    watermark: finite(rt.wm),
    source: { ...rt.stats },
    rate: arrivals.length / 5,
    dots,
    results,
    openWindows: pulse ? pulse.openWindows() : [],
    wars,
    pipeline: { jobs, dlq, dlqSample },
    sql: sqlTick(),
    kafka: kafka?.view(),
    rec: rec.active ? rec.view() : undefined,
    epoch,
  };
  post(t);
  dots = [];
  results = {};
  wars = [];
}

setInterval(tick, 250);

ctx.onmessage = (e: MessageEvent<ToWorker>) => {
  const m = e.data;
  switch (m.type) {
    case "start":
      url = m.url;
      startStream(m.since ?? null);
      break;
    case "pause":
      source?.stop();
      paused = true;
      break;
    case "resume":
      source?.resume();
      paused = false;
      break;
    case "replay":
      startStream(Date.now() - m.minutes * 60_000);
      break;
    case "delay":
      rt.setDelay(m.ms);
      break;
    case "sql.run":
      runSql(m.id, m.sql, m.from, m.timeZone);
      break;
    case "sql.stop":
      stopSql();
      break;
    case "kafka.config":
      if (!kafka) kafka = new KafkaSim(m.config);
      else kafka.configure(m.config);
      break;
    case "kafka.kill":
      kafka?.kill(m.consumer);
      break;
    case "kafka.add":
      kafka?.add();
      break;
    case "rec.reset":
      rec.start();
      break;
    case "rec.checkpoint": {
      if (!rec.active) rec.start();
      rec.checkpoint();
      break;
    }
    case "rec.crash":
      rec.crash();
      break;
    case "rec.recover":
      rec.recover(m.mode, m.snapshot);
      break;
  }
};

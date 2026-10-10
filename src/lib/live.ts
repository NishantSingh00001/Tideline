// The page-side half of the engine: starts the worker, keeps what the panels
// need, and lets React components subscribe to it.

import { useSyncExternalStore } from "react";
import type { EditWar } from "../../shared/engine/cep.ts";
import type { Column, Row, RowKind } from "../../shared/engine/types.ts";
import type { SourceStats } from "../../shared/engine/runtime.ts";
import type { KafkaConfig, KafkaView } from "../../shared/kafka/sim.ts";
import { MaterializedView } from "../../shared/sql/job.ts";
import type { PlanNode } from "../../shared/sql/planner.ts";
import type { Dot, FromWorker, PipelineMetrics, RecoverMode, RecoveryView, SqlTick, Tick, ToWorker } from "../worker/protocol.ts";
import type { SourceStatus } from "../worker/source.ts";
import { PROJECTS, projectIndex } from "./projects.ts";

export const DEFAULT_STREAM = "https://stream.wikimedia.org/v2/stream/recentchange";

/** The stream URL: Wikimedia's, or a local mock (?stream=http://localhost:8787/...) for development. */
export function streamUrl(): string {
  const env = (import.meta.env.VITE_STREAM_URL as string | undefined) || "";
  try {
    const q = new URLSearchParams(location.search).get("stream");
    if (q) {
      const u = new URL(q);
      if (["localhost", "127.0.0.1"].includes(u.hostname)) return u.toString();
    }
  } catch {
    /* ignore a bad parameter */
  }
  return env || DEFAULT_STREAM;
}

export interface TimedDot extends Dot {
  arrived: number;
}

export interface PulseWindow {
  start: number;
  end: number;
  counts: number[];
  total: number;
}

export interface EditorsWindow {
  start: number;
  people: number;
  bots: number;
  accounts: number;
}

export interface TrendingRow {
  server: string;
  title: string;
  edits: number;
  editors: number;
  bytes: number | null;
  rank: number;
}

export interface FeedRow {
  ts: number;
  server: string;
  title: string;
  user: string;
  value: number | null;
}

export interface SqlState {
  id: number;
  running: boolean;
  compiled: { schema: Column[]; changelog: boolean; windowed: boolean; explain: string; nodes: PlanNode[]; explainOnly: boolean } | null;
  error: { message: string; line?: number; col?: number; start?: number; end?: number } | null;
  /** Append / window results, newest last. */
  rows: Row[];
  /** For changelog queries: the materialised table and the raw messages. */
  view: MaterializedView;
  changelog: { kind: RowKind; row: Row; at: number }[];
  messages: number;
  tick: SqlTick | null;
  startedAt: number;
}

export interface LiveState {
  version: number;
  epoch: number;
  url: string;
  now: number;
  status: SourceStatus | null;
  paused: boolean;
  delay: number;
  maxTs: number | null;
  watermark: number | null;
  source: SourceStats | null;
  rate: number;
  rateHistory: number[];
  dots: TimedDot[];
  pulse: PulseWindow[];
  editors: EditorsWindow[];
  trending: { start: number; end: number; rows: TrendingRow[]; previous: Map<string, number> } | null;
  newPages: FeedRow[];
  bigChanges: FeedRow[];
  wars: EditWar[];
  openWindows: Tick["openWindows"];
  pipeline: PipelineMetrics | null;
  sql: SqlState;
  kafka: KafkaView | null;
  rec: RecoveryView | null;
  lastSnapshot: string | null;
}

const emptySql = (): SqlState => ({ id: 0, running: false, compiled: null, error: null, rows: [], view: new MaterializedView(), changelog: [], messages: 0, tick: null, startedAt: 0 });

const state: LiveState = {
  version: 0,
  epoch: 0,
  url: "",
  now: Date.now(),
  status: null,
  paused: false,
  delay: 5000,
  maxTs: null,
  watermark: null,
  source: null,
  rate: 0,
  rateHistory: [],
  dots: [],
  pulse: [],
  editors: [],
  trending: null,
  newPages: [],
  bigChanges: [],
  wars: [],
  openWindows: [],
  pipeline: null,
  sql: emptySql(),
  kafka: null,
  rec: null,
  lastSnapshot: null,
};

const listeners = new Set<() => void>();
let worker: Worker | null = null;

function send(m: ToWorker) {
  ensure();
  worker!.postMessage(m);
}

function clearPanels() {
  state.dots = [];
  state.pulse = [];
  state.editors = [];
  state.trending = null;
  state.newPages = [];
  state.bigChanges = [];
  state.wars = [];
  state.rateHistory = [];
}

function onTick(t: Tick) {
  if (t.epoch !== state.epoch) {
    state.epoch = t.epoch;
    clearPanels();
  }
  const arrived = performance.now();
  state.now = t.now;
  state.status = t.status;
  state.paused = t.paused;
  state.delay = t.delay;
  state.maxTs = t.maxTs;
  state.watermark = t.watermark;
  state.source = t.source;
  state.rate = t.rate;
  state.rateHistory = [...state.rateHistory.slice(-119), t.rate];
  state.openWindows = t.openWindows;
  state.pipeline = t.pipeline;
  if (t.dots.length) {
    const add = t.dots.map((d, i) => ({ ...d, arrived: arrived + (i / t.dots.length) * 250 }));
    state.dots = [...state.dots, ...add].slice(-5000);
  }
  for (const { row } of t.results.pulse || []) {
    const [start, end, project, changes] = row as [number, number, string, number];
    let w = state.pulse.find((x) => x.start === start);
    if (!w) {
      w = { start, end, counts: PROJECTS.map(() => 0), total: 0 };
      state.pulse = [...state.pulse, w].sort((a, b) => a.start - b.start).slice(-36);
    }
    const i = projectIndex(project);
    if (i >= 0) w.counts[i] = changes;
    w.total = w.counts.reduce((s, x) => s + x, 0);
  }
  for (const { row } of t.results.editors || []) {
    const [start, , people, bots, accounts] = row as number[];
    state.editors = [...state.editors, { start, people, bots, accounts }].slice(-36);
  }
  const tr = t.results.trending || [];
  if (tr.length) {
    // Rows of the newest closed window.
    const byWindow = new Map<number, Row[]>();
    for (const { row } of tr) {
      const ws = row[0] as number;
      byWindow.set(ws, [...(byWindow.get(ws) || []), row]);
    }
    const latest = Math.max(...byWindow.keys());
    const rows = byWindow.get(latest)!;
    const previous = new Map<string, number>();
    for (const r of state.trending?.rows || []) previous.set(r.server + "|" + r.title, r.rank);
    state.trending = {
      start: latest,
      end: rows[0][1] as number,
      rows: rows
        .map((r) => ({ server: r[2] as string, title: r[3] as string, edits: r[4] as number, editors: r[5] as number, bytes: r[6] as number | null, rank: r[7] as number }))
        .sort((a, b) => a.rank - b.rank),
      previous,
    };
  }
  const feed = (rows: { row: Row }[]) => rows.map(({ row }) => ({ ts: row[0] as number, server: row[1] as string, title: row[2] as string, user: row[3] as string, value: row[4] as number | null }));
  if (t.results.newPages?.length) state.newPages = [...feed(t.results.newPages).reverse(), ...state.newPages].slice(0, 40);
  if (t.results.bigChanges?.length) state.bigChanges = [...feed(t.results.bigChanges).reverse(), ...state.bigChanges].slice(0, 40);
  if (t.wars.length) state.wars = [...[...t.wars].reverse(), ...state.wars].slice(0, 20);
  if (t.sql && t.sql.id === state.sql.id) {
    const s = state.sql;
    s.tick = t.sql;
    for (const b of t.sql.rows) {
      s.messages++;
      if (s.compiled?.changelog) {
        s.view.apply(b.kind, b.row);
        s.changelog.push({ kind: b.kind, row: b.row, at: t.now });
      } else s.rows.push(b.row);
    }
    if (s.rows.length > 2000) s.rows = s.rows.slice(-2000);
    if (s.changelog.length > 400) s.changelog = s.changelog.slice(-400);
    if (t.sql.failed) s.running = false;
  }
  state.kafka = t.kafka ?? state.kafka;
  if (t.rec) {
    state.rec = t.rec;
    if (t.rec.lastSnapshot) state.lastSnapshot = t.rec.lastSnapshot;
  }
  emit();
}

function emit() {
  state.version++;
  for (const l of listeners) l();
}

function ensure() {
  if (worker) return;
  worker = new Worker(new URL("../worker/engine.worker.ts", import.meta.url), { type: "module" });
  worker.onmessage = (e: MessageEvent<FromWorker>) => {
    const m = e.data;
    if (m.type === "tick") onTick(m);
    else if (m.type === "sql.compiled" && m.id === state.sql.id) {
      state.sql.compiled = { schema: m.schema, changelog: m.changelog, windowed: m.windowed, explain: m.explain, nodes: m.nodes, explainOnly: m.explainOnly };
      state.sql.running = !m.explainOnly;
      emit();
    } else if (m.type === "sql.error" && m.id === state.sql.id) {
      state.sql.error = { message: m.message, line: m.line, col: m.col, start: m.start, end: m.end };
      state.sql.running = false;
      emit();
    }
  };
  state.url = streamUrl();
  worker.postMessage({ type: "start", url: state.url } satisfies ToWorker);
}

let sqlSeq = 0;

export const live = {
  start: ensure,
  get: () => state,
  pause: () => send({ type: "pause" }),
  resume: () => send({ type: "resume" }),
  replay: (minutes: number) => send({ type: "replay", minutes }),
  setDelay: (ms: number) => send({ type: "delay", ms }),
  runSql(sql: string, from: "now" | number) {
    const id = ++sqlSeq;
    state.sql = { ...emptySql(), id, running: true, startedAt: Date.now() };
    emit();
    send({ type: "sql.run", id, sql, from, timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC" });
  },
  stopSql() {
    state.sql.running = false;
    send({ type: "sql.stop" });
    emit();
  },
  kafka: (config: Partial<KafkaConfig>) => send({ type: "kafka.config", config }),
  kafkaKill: (consumer: string) => send({ type: "kafka.kill", consumer }),
  kafkaAdd: () => send({ type: "kafka.add" }),
  recReset: () => send({ type: "rec.reset" }),
  recCheckpoint: () => send({ type: "rec.checkpoint" }),
  recCrash: () => send({ type: "rec.crash" }),
  recRecover: (mode: RecoverMode, snapshot?: string | null) => send({ type: "rec.recover", mode, snapshot }),
};

export function useLive<T>(select: (s: LiveState) => T): T {
  ensure();
  return useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => select(state),
  );
}

/** Re-renders on every update (selectors that return fresh objects would loop). */
export function useLiveVersion(): LiveState {
  useSyncExternalStore(
    (l) => {
      listeners.add(l);
      return () => listeners.delete(l);
    },
    () => state.version,
  );
  return state;
}

// Messages between the page and the engine worker.

import type { Column, OpStats, Row, RowKind } from "../../shared/engine/types.ts";
import type { SourceStats } from "../../shared/engine/runtime.ts";
import type { EditWar } from "../../shared/engine/cep.ts";
import type { PlanNode } from "../../shared/sql/planner.ts";
import type { SourceStatus } from "./source.ts";
import type { KafkaConfig, KafkaView } from "../../shared/kafka/sim.ts";

export type ToWorker =
  | { type: "start"; url: string; since?: number | null }
  | { type: "pause" }
  | { type: "resume" }
  | { type: "replay"; minutes: number }
  | { type: "delay"; ms: number }
  | { type: "sql.run"; id: number; sql: string; from: "now" | number; timeZone: string }
  | { type: "sql.stop" }
  | { type: "kafka.config"; config: Partial<KafkaConfig> }
  | { type: "kafka.kill"; consumer: string }
  | { type: "kafka.add" }
  | { type: "rec.checkpoint" }
  | { type: "rec.crash" }
  | { type: "rec.recover"; mode: RecoverMode; snapshot?: string | null }
  | { type: "rec.reset" };

export type RecoverMode = "exactly-once" | "at-least-once" | "at-most-once";

/** A change shown as a dot on the tide chart. */
export interface Dot {
  ts: number;
  lane: number;
  late: boolean;
  /** 0..1, stable per event: vertical position inside the lane. */
  y: number;
  title: string;
  server: string;
  user: string;
  type: string;
  bot: boolean;
}

export interface OpMetric extends OpStats {
  name: string;
  detail: string;
}

export interface PipelineMetrics {
  /** Per job: operator chain with counters. */
  jobs: Record<string, OpMetric[]>;
  dlq: number;
  dlqSample: string[];
}

export interface ResultBatch {
  kind: RowKind;
  row: Row;
}

export interface Tick {
  type: "tick";
  now: number;
  status: SourceStatus;
  paused: boolean;
  delay: number;
  maxTs: number | null;
  watermark: number | null;
  source: SourceStats;
  rate: number;
  dots: Dot[];
  /** Window results that closed since the last tick, per panel. */
  results: Record<string, ResultBatch[]>;
  openWindows: { start: number; end: number; keys: number; rows: number }[];
  wars: EditWar[];
  pipeline: PipelineMetrics;
  sql?: SqlTick;
  kafka?: KafkaView;
  rec?: RecoveryView;
  /** Set when the stream restarted (replay): the page clears what it has. */
  epoch: number;
}

export interface SqlCompiled {
  type: "sql.compiled";
  id: number;
  schema: Column[];
  changelog: boolean;
  windowed: boolean;
  explain: string;
  nodes: PlanNode[];
  explainOnly: boolean;
}

export interface SqlError {
  type: "sql.error";
  id: number;
  message: string;
  line?: number;
  col?: number;
  start?: number;
  end?: number;
}

export interface SqlTick {
  id: number;
  rows: ResultBatch[];
  failed: string | null;
  stats: OpMetric[];
  received: number;
  watermark: number | null;
  status: SourceStatus | null;
  done: boolean;
}

export interface RecoveryWindow {
  start: number;
  ref: number;
  primary: number | null;
}

export interface RecoveryView {
  phase: "running" | "crashed" | "recovering" | "caught-up";
  mode: RecoverMode | null;
  checkpoints: { id: number; at: number; position: number | null; bytes: number; events: number }[];
  primaryEvents: number;
  primaryDuplicates: number;
  primaryWatermark: number | null;
  refWatermark: number | null;
  windows: RecoveryWindow[];
  compared: number;
  differences: number;
  missing: number;
  extra: number;
  lastSnapshot: string | null;
  crashedAt: number | null;
  restoredFrom: number | null;
}

export type FromWorker = Tick | SqlCompiled | SqlError;

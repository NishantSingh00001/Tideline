// Core types of the Tideline stream engine.
//
// The engine follows Apache Flink's semantics where it matters for results:
// event time, watermarks emitted after each record (Flink's
// `pipeline.auto-watermark-interval = 0`), window firing at
// `watermark >= window_end - 1`, late-record dropping, and retract
// changelogs (+I / -U / +U / -D) for unbounded aggregations.

export type Value = string | number | boolean | null;
export type Row = Value[];
export type RowKind = "+I" | "-U" | "+U" | "-D";

export type SqlType = "STRING" | "INT" | "BIGINT" | "DOUBLE" | "DECIMAL" | "BOOLEAN" | "TIMESTAMP_LTZ" | "NULL";

export interface Column {
  name: string;
  type: SqlType;
  nullable: boolean;
  /** Event-time attribute (Flink's *ROWTIME*). */
  rowtime?: boolean;
}

export type Emit = (kind: RowKind, row: Row) => void;

export interface OpStats {
  in: number;
  out: number;
  late: number;
  /** Keys, windows or buffered rows currently held in state. */
  state: number;
}

export interface Operator {
  readonly name: string;
  /** One-line description for EXPLAIN, e.g. "window=[TUMBLE(event_time, 10 s)], groupBy=[wiki]". */
  readonly detail: string;
  readonly schema: Column[];
  readonly changelog: boolean;
  next: Emit;
  push(kind: RowKind, row: Row): void;
  watermark(wm: number): void;
  snapshot(): unknown;
  restore(state: unknown): void;
  stats(): OpStats;
}

/** Shared settings for a running query. */
export interface EngineContext {
  /** IANA time zone used for TIMESTAMP_LTZ display and window alignment (Flink's table.local-time-zone). */
  timeZone: string;
  /** Current watermark seen by the source (for CURRENT_WATERMARK()). */
  currentWatermark: number;
}

export const MIN_WATERMARK = -Infinity;
export const MAX_WATERMARK = Number.MAX_SAFE_INTEGER;

export const isIntegral = (t: SqlType) => t === "INT" || t === "BIGINT";
export const isNumeric = (t: SqlType) => t === "INT" || t === "BIGINT" || t === "DOUBLE" || t === "DECIMAL";

/** A stable string key for a row of values (distinguishes 1, "1", true and null). */
export function keyOf(values: readonly Value[]): string {
  let out = "";
  for (const v of values) {
    if (v === null) out += "\u0000n";
    else if (typeof v === "string") out += "\u0000s" + v;
    else if (typeof v === "number") out += "\u0000d" + v;
    else out += v ? "\u0000t" : "\u0000f";
  }
  return out;
}

export const valueKey = (v: Value) => keyOf([v]);

export function sameRow(a: Row | undefined, b: Row | undefined): boolean {
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

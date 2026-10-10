// Stream operators. Each one is push-based: rows arrive through push(), event
// time advances through watermark(), and results go to `next`.

import { accFromJson, accToJson, accumulate, mergeAcc, newAcc, result, type Acc, type AccJson, type AggCall } from "./aggs.ts";
import { assignWindows, describeWindow, isFired, type WindowSpec } from "./windows.ts";
import { keyOf, sameRow, type Column, type Emit, type OpStats, type Operator, type Row, type RowKind, type Value } from "./types.ts";

const noop: Emit = () => {};

abstract class Base implements Operator {
  abstract readonly name: string;
  abstract readonly detail: string;
  readonly schema: Column[];
  readonly changelog: boolean;
  next: Emit = noop;
  protected nIn = 0;
  protected nOut = 0;
  protected nLate = 0;
  constructor(schema: Column[], changelog: boolean) {
    this.schema = schema;
    this.changelog = changelog;
  }
  abstract push(kind: RowKind, row: Row): void;
  watermark(_wm: number): void {}
  snapshot(): unknown {
    return null;
  }
  restore(_state: unknown): void {}
  protected stateSize(): number {
    return 0;
  }
  protected emit(kind: RowKind, row: Row) {
    this.nOut++;
    this.next(kind, row);
  }
  stats(): OpStats {
    return { in: this.nIn, out: this.nOut, late: this.nLate, state: this.stateSize() };
  }
  protected counters() {
    return [this.nIn, this.nOut, this.nLate];
  }
  protected setCounters(c: unknown) {
    if (Array.isArray(c)) [this.nIn, this.nOut, this.nLate] = c as number[];
  }
}

/** Filter and/or projection (Flink's Calc). Stateless; keeps the row kind. */
export class CalcOp extends Base {
  readonly name = "Calc";
  readonly detail: string;
  constructor(
    schema: Column[],
    changelog: boolean,
    private filter: ((row: Row) => boolean) | null,
    private project: ((row: Row) => Value)[] | null,
    detail: string,
  ) {
    super(schema, changelog);
    this.detail = detail;
  }
  push(kind: RowKind, row: Row) {
    this.nIn++;
    if (this.filter && !this.filter(row)) return;
    if (!this.project) return this.emit(kind, row);
    const out = new Array(this.project.length);
    for (let i = 0; i < out.length; i++) out[i] = this.project[i](row);
    this.emit(kind, out);
  }
  snapshot() {
    return { c: this.counters() };
  }
  restore(s: any) {
    this.setCounters(s?.c);
  }
}

/** A window TVF used on its own: appends window_start, window_end, window_time. */
export class WindowTableOp extends Base {
  readonly name = "WindowTableFunction";
  readonly detail: string;
  private wm = -Infinity;
  constructor(schema: Column[], private spec: Exclude<WindowSpec, { kind: "SESSION" }>, private timeIdx: number, private tz: string) {
    super(schema, false);
    this.detail = `window=[${describeWindow(spec)}]`;
  }
  push(_kind: RowKind, row: Row) {
    this.nIn++;
    const ts = row[this.timeIdx] as number;
    // Like Flink's WindowTableFunctionOperator, rows are never dropped here,
    // even when their window has already closed downstream (counted as late).
    let late = true;
    for (const w of assignWindows(this.spec, ts, this.tz)) {
      if (!isFired(w.end, this.wm)) late = false;
      this.emit("+I", [...row, w.start, w.end, w.end - 1]);
    }
    if (late) this.nLate++;
  }
  watermark(wm: number) {
    this.wm = Math.max(this.wm, wm);
  }
  snapshot() {
    return { wm: this.wm, c: this.counters() };
  }
  restore(s: any) {
    this.wm = s?.wm ?? -Infinity;
    this.setCounters(s?.c);
  }
}

interface Group {
  key: Row;
  accs: Acc[];
}

interface AlignedWindow {
  start: number;
  end: number;
  groups: Map<string, Group>;
}

interface Session {
  start: number;
  end: number;
  key: Row;
  accs: Acc[];
}

/**
 * Window aggregation (Flink's WindowAggregate). Output rows are
 * [window_start, window_end, window_time, ...groupKeys, ...aggregates],
 * emitted once per window and key when the watermark passes the window end.
 */
export class WindowAggOp extends Base {
  readonly name = "WindowAggregate";
  readonly detail: string;
  private wm = -Infinity;
  private aligned = new Map<string, AlignedWindow>();
  private sessions = new Map<string, Session[]>();
  constructor(
    schema: Column[],
    readonly spec: WindowSpec,
    private timeIdx: number,
    private keys: ((row: Row) => Value)[],
    private aggs: AggCall[],
    private tz: string,
    keyNames: string[],
  ) {
    super(schema, false);
    this.detail = `window=[${describeWindow(spec)}], groupBy=[${keyNames.join(", ")}], select=[${aggs.map((a) => a.text).join(", ")}]`;
  }

  push(_kind: RowKind, row: Row) {
    this.nIn++;
    const ts = row[this.timeIdx] as number;
    const key = this.keys.map((k) => k(row));
    if (this.spec.kind === "SESSION") return this.pushSession(row, ts, key, this.spec.gap);
    const kk = keyOf(key);
    let added = false;
    for (const w of assignWindows(this.spec, ts, this.tz)) {
      if (isFired(w.end, this.wm)) continue; // late for this window
      const wk = w.start + "|" + w.end;
      let win = this.aligned.get(wk);
      if (!win) this.aligned.set(wk, (win = { start: w.start, end: w.end, groups: new Map() }));
      let g = win.groups.get(kk);
      if (!g) win.groups.set(kk, (g = { key, accs: this.aggs.map(newAcc) }));
      for (let i = 0; i < this.aggs.length; i++) accumulate(this.aggs[i], g.accs[i], row);
      added = true;
    }
    if (!added) this.nLate++;
  }

  private pushSession(row: Row, ts: number, key: Row, gap: number) {
    const kk = keyOf(key);
    const list = this.sessions.get(kk) || [];
    let start = ts;
    let end = ts + gap;
    // TimeWindow.intersects is inclusive: [0,3) and [3,6) merge.
    const touching = list.filter((s) => s.start <= end && s.end >= start);
    if (!touching.length && isFired(end, this.wm)) {
      this.nLate++;
      return;
    }
    const accs = this.aggs.map(newAcc);
    for (const s of touching) {
      start = Math.min(start, s.start);
      end = Math.max(end, s.end);
      for (let i = 0; i < this.aggs.length; i++) mergeAcc(this.aggs[i], accs[i], s.accs[i]);
    }
    for (let i = 0; i < this.aggs.length; i++) accumulate(this.aggs[i], accs[i], row);
    const rest = list.filter((s) => !touching.includes(s));
    rest.push({ start, end, key, accs });
    this.sessions.set(kk, rest);
  }

  watermark(wm: number) {
    if (wm <= this.wm) return;
    this.wm = wm;
    const fired: { start: number; end: number; key: Row; accs: Acc[] }[] = [];
    for (const [wk, win] of this.aligned) {
      if (!isFired(win.end, wm)) continue;
      this.aligned.delete(wk);
      for (const g of win.groups.values()) fired.push({ start: win.start, end: win.end, key: g.key, accs: g.accs });
    }
    for (const [kk, list] of this.sessions) {
      const keep: Session[] = [];
      for (const s of list) (isFired(s.end, wm) ? fired : keep).push(s);
      if (keep.length) this.sessions.set(kk, keep);
      else this.sessions.delete(kk);
    }
    fired.sort((a, b) => a.end - b.end || a.start - b.start);
    for (const f of fired) this.emit("+I", [f.start, f.end, f.end - 1, ...f.key, ...this.aggs.map((a, i) => result(a, f.accs[i]))]);
  }

  /** Open windows and the count of keys in each (for the live view). */
  openWindows(): { start: number; end: number; keys: number; rows: number }[] {
    const out: { start: number; end: number; keys: number; rows: number }[] = [];
    for (const w of this.aligned.values()) {
      let rows = 0;
      for (const g of w.groups.values()) rows += g.accs.length ? g.accs[0].n : 0;
      out.push({ start: w.start, end: w.end, keys: w.groups.size, rows });
    }
    return out.sort((a, b) => a.start - b.start);
  }

  protected stateSize() {
    let n = 0;
    for (const w of this.aligned.values()) n += w.groups.size;
    for (const l of this.sessions.values()) n += l.length;
    return n;
  }

  snapshot() {
    return {
      wm: this.wm,
      c: this.counters(),
      a: [...this.aligned.values()].map((w) => [w.start, w.end, [...w.groups.values()].map((g) => [g.key, g.accs.map(accToJson)])]),
      s: [...this.sessions.values()].flat().map((s) => [s.start, s.end, s.key, s.accs.map(accToJson)]),
    };
  }

  restore(st: any) {
    this.wm = st?.wm ?? -Infinity;
    this.setCounters(st?.c);
    this.aligned.clear();
    this.sessions.clear();
    for (const [start, end, groups] of st?.a || []) {
      const m = new Map<string, Group>();
      for (const [key, accs] of groups) m.set(keyOf(key), { key, accs: (accs as AccJson[]).map(accFromJson) });
      this.aligned.set(start + "|" + end, { start, end, groups: m });
    }
    for (const [start, end, key, accs] of st?.s || []) {
      const kk = keyOf(key);
      const list = this.sessions.get(kk) || [];
      list.push({ start, end, key, accs: (accs as AccJson[]).map(accFromJson) });
      this.sessions.set(kk, list);
    }
  }
}

/**
 * Unbounded GROUP BY (Flink's GroupAggregate). Emits a retract changelog:
 * +I for a new key, then -U/+U pairs whenever its result changes.
 */
export class GroupAggOp extends Base {
  readonly name = "GroupAggregate";
  readonly detail: string;
  private groups = new Map<string, Group & { last: Row }>();
  constructor(schema: Column[], private keys: ((row: Row) => Value)[], private aggs: AggCall[], keyNames: string[]) {
    super(schema, true);
    this.detail = `groupBy=[${keyNames.join(", ")}], select=[${aggs.map((a) => a.text).join(", ")}]`;
  }
  push(_kind: RowKind, row: Row) {
    this.nIn++;
    const key = this.keys.map((k) => k(row));
    const kk = keyOf(key);
    let g = this.groups.get(kk);
    const isNew = !g;
    if (!g) this.groups.set(kk, (g = { key, accs: this.aggs.map(newAcc), last: [] }));
    for (let i = 0; i < this.aggs.length; i++) accumulate(this.aggs[i], g.accs[i], row);
    const out = [...key, ...this.aggs.map((a, i) => result(a, g!.accs[i]))];
    if (isNew) {
      g.last = out;
      return this.emit("+I", out);
    }
    if (sameRow(out, g.last)) return; // unchanged: Flink emits nothing
    const prev = g.last;
    g.last = out;
    this.emit("-U", prev);
    this.emit("+U", out);
  }
  protected stateSize() {
    return this.groups.size;
  }
  snapshot() {
    return { c: this.counters(), g: [...this.groups.values()].map((g) => [g.key, g.accs.map(accToJson), g.last]) };
  }
  restore(st: any) {
    this.setCounters(st?.c);
    this.groups.clear();
    for (const [key, accs, last] of st?.g || []) this.groups.set(keyOf(key), { key, accs: (accs as AccJson[]).map(accFromJson), last });
  }
}

export interface SortKey {
  idx: number;
  desc: boolean;
}

export function compareValues(a: Value, b: Value): number {
  if (a === b) return 0;
  // Flink sorts NULL as the smallest value.
  if (a === null) return -1;
  if (b === null) return 1;
  if (typeof a === "boolean" || typeof b === "boolean") return Number(a) - Number(b);
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Window Top-N (Flink's WindowRank): buffers rows per window (and partition
 * key) and emits the first N by the ORDER BY when the window closes.
 */
export class WindowRankOp extends Base {
  readonly name = "WindowRank";
  readonly detail: string;
  private wm = -Infinity;
  private buckets = new Map<string, { end: number; rows: Row[] }>();
  constructor(
    schema: Column[],
    private startIdx: number,
    private endIdx: number,
    private partition: number[],
    private order: SortKey[],
    private limit: number,
    private withRownum: boolean,
    detail: string,
  ) {
    super(schema, false);
    this.detail = detail;
  }
  push(_kind: RowKind, row: Row) {
    this.nIn++;
    const end = row[this.endIdx] as number;
    if (isFired(end, this.wm)) {
      this.nLate++;
      return;
    }
    const bk = keyOf([row[this.startIdx], end, ...this.partition.map((i) => row[i])]);
    let b = this.buckets.get(bk);
    if (!b) this.buckets.set(bk, (b = { end, rows: [] }));
    b.rows.push(row);
  }
  watermark(wm: number) {
    if (wm <= this.wm) return;
    this.wm = wm;
    const due = [...this.buckets.entries()].filter(([, b]) => isFired(b.end, wm)).sort((a, b) => a[1].end - b[1].end);
    for (const [bk, b] of due) {
      this.buckets.delete(bk);
      const sorted = [...b.rows].sort((x, y) => {
        for (const k of this.order) {
          const c = compareValues(x[k.idx], y[k.idx]);
          if (c) return k.desc ? -c : c;
        }
        return 0;
      });
      for (let i = 0; i < Math.min(this.limit, sorted.length); i++) this.emit("+I", this.withRownum ? [...sorted[i], i + 1] : sorted[i]);
    }
  }
  protected stateSize() {
    let n = 0;
    for (const b of this.buckets.values()) n += b.rows.length;
    return n;
  }
  snapshot() {
    return { wm: this.wm, c: this.counters(), b: [...this.buckets.entries()] };
  }
  restore(st: any) {
    this.wm = st?.wm ?? -Infinity;
    this.setCounters(st?.c);
    this.buckets = new Map(st?.b || []);
  }
}

/** LIMIT n on an append-only stream. */
export class LimitOp extends Base {
  readonly name = "Limit";
  readonly detail: string;
  constructor(schema: Column[], private n: number) {
    super(schema, false);
    this.detail = `fetch=[${n}]`;
  }
  push(kind: RowKind, row: Row) {
    this.nIn++;
    if (this.nOut < this.n) this.emit(kind, row);
  }
  get done() {
    return this.nOut >= this.n;
  }
  snapshot() {
    return { c: this.counters() };
  }
  restore(s: any) {
    this.setCounters(s?.c);
  }
}

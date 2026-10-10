// A compiled query running as a job on the Runtime: wires the operator chain
// together and collects results (a materialised view for changelogs).

import type { Job } from "../engine/runtime.ts";
import { keyOf, type Row, type RowKind } from "../engine/types.ts";
import { LimitOp } from "../engine/operators.ts";
import { SqlError } from "./lexer.ts";
import type { CompiledQuery } from "./planner.ts";

export type Sink = (kind: RowKind, row: Row) => void;

export class QueryJob implements Job {
  failed: string | null = null;
  constructor(
    readonly id: string,
    readonly query: CompiledQuery,
    sink: Sink,
  ) {
    const ops = query.ops;
    for (let i = 0; i < ops.length - 1; i++) {
      const nextOp = ops[i + 1];
      ops[i].next = (k, r) => nextOp.push(k, r);
    }
    if (ops.length) ops[ops.length - 1].next = sink;
    this.sink = sink;
  }
  private sink: Sink;

  get done() {
    const last = this.query.ops[this.query.ops.length - 1];
    return last instanceof LimitOp && last.done;
  }

  push(row: Row) {
    if (this.failed || this.done) return;
    try {
      if (this.query.ops.length) this.query.ops[0].push("+I", row);
      else this.sink("+I", row);
    } catch (e) {
      this.fail(e);
    }
  }

  watermark(wm: number) {
    if (this.failed) return;
    this.query.ctx.currentWatermark = wm;
    try {
      for (const op of this.query.ops) op.watermark(wm);
    } catch (e) {
      this.fail(e);
    }
  }

  private fail(e: unknown) {
    this.failed = e instanceof SqlError ? e.message : `Job failed: ${(e as Error).message}`;
  }

  snapshot() {
    return { ops: this.query.ops.map((o) => o.snapshot()), wm: this.query.ctx.currentWatermark };
  }

  restore(s: any) {
    if (!s) return;
    this.query.ops.forEach((o, i) => o.restore(s.ops?.[i]));
    this.query.ctx.currentWatermark = s.wm ?? -Infinity;
  }

  stats() {
    return this.query.ops.map((o) => ({ name: o.name, ...o.stats() }));
  }
}

/**
 * Applies a retract changelog to a table, like Flink's SQL client in
 * "table" result mode: +I/+U add a row, -U/-D remove one.
 */
export class MaterializedView {
  rows = new Map<string, { row: Row; count: number; updated: number }>();
  version = 0;
  apply(kind: RowKind, row: Row) {
    const k = keyOf(row);
    const hit = this.rows.get(k);
    this.version++;
    if (kind === "+I" || kind === "+U") {
      if (hit) {
        hit.count++;
        hit.updated = this.version;
      } else this.rows.set(k, { row, count: 1, updated: this.version });
    } else if (hit) {
      if (--hit.count <= 0) this.rows.delete(k);
    }
  }
  list(): { row: Row; updated: number }[] {
    const out: { row: Row; updated: number }[] = [];
    for (const v of this.rows.values()) for (let i = 0; i < v.count; i++) out.push({ row: v.row, updated: v.updated });
    return out;
  }
}

// The source side of the engine: deduplication, watermark generation and
// fan-out of each record to every running job. Also owns checkpoints.

import type { Change } from "../change.ts";
import { changeToRow } from "../table.ts";
import { MAX_WATERMARK, type Row } from "./types.ts";

export interface Job {
  readonly id: string;
  push(row: Row): void;
  watermark(wm: number): void;
  snapshot(): unknown;
  restore(state: unknown): void;
}

export interface SourceStats {
  received: number;
  duplicates: number;
  /** Records that arrived with an event time at or behind the current watermark. */
  behindWatermark: number;
  lateByType: Record<string, number>;
  /** How far behind the watermark late records were: <1 min, 1–5 min, 5–30 min, >30 min. */
  lateness: [number, number, number, number];
  maxTs: number;
  watermark: number;
}

export interface RuntimeSnapshot {
  v: 1;
  delay: number;
  maxTs: number;
  wm: number;
  stats: SourceStats;
  dedup: [string, number][];
  jobs: Record<string, unknown>;
}

const DEDUP_TTL = 10 * 60_000;

export class Runtime {
  private jobs = new Map<string, Job>();
  private seen = new Map<string, number>();
  private sinceEvict = 0;
  maxTs = -Infinity;
  wm = -Infinity;
  /** Drop events whose id was already seen (exactly-once over an at-least-once source). */
  dedup = true;
  stats: SourceStats = Runtime.emptyStats();

  /** `WATERMARK FOR event_time AS event_time - INTERVAL 'delay'`, in ms. */
  constructor(public delay = 5000) {}

  static emptyStats(): SourceStats {
    return { received: 0, duplicates: 0, behindWatermark: 0, lateByType: {}, lateness: [0, 0, 0, 0], maxTs: -Infinity, watermark: -Infinity };
  }

  add(job: Job) {
    this.jobs.set(job.id, job);
    // A job added mid-stream starts at the current watermark.
    if (this.wm > -Infinity) job.watermark(this.wm);
  }

  remove(id: string) {
    this.jobs.delete(id);
  }

  get(id: string) {
    return this.jobs.get(id);
  }

  /** Feeds one change through every job. Returns false for a duplicate. */
  ingest(c: Change): boolean {
    if (this.seen.has(c.id)) {
      this.stats.duplicates++;
      if (this.dedup) return false;
    }
    this.seen.set(c.id, c.ts_ms);
    if (++this.sinceEvict >= 2000) this.evict();
    this.stats.received++;
    if (c.ts_ms <= this.wm) {
      this.stats.behindWatermark++;
      this.stats.lateByType[c.change_type] = (this.stats.lateByType[c.change_type] || 0) + 1;
      const behind = this.wm - c.ts_ms;
      this.stats.lateness[behind < 60_000 ? 0 : behind < 300_000 ? 1 : behind < 1_800_000 ? 2 : 3]++;
    }
    const row = changeToRow(c);
    for (const job of this.jobs.values()) job.push(row);
    // Flink's WatermarkAssignerOperator: forward the record first, then the watermark.
    if (c.ts_ms > this.maxTs) this.maxTs = c.ts_ms;
    const next = this.maxTs - this.delay;
    if (next > this.wm) {
      this.wm = next;
      for (const job of this.jobs.values()) job.watermark(next);
    }
    this.stats.maxTs = this.maxTs;
    this.stats.watermark = this.wm;
    return true;
  }

  /** End of a bounded input: Flink emits Long.MAX_VALUE so every window fires. */
  finish() {
    this.wm = MAX_WATERMARK;
    for (const job of this.jobs.values()) job.watermark(MAX_WATERMARK);
  }

  setDelay(ms: number) {
    this.delay = ms;
  }

  private evict() {
    this.sinceEvict = 0;
    const cutoff = this.maxTs - DEDUP_TTL;
    for (const [id, ts] of this.seen) if (ts < cutoff) this.seen.delete(id);
  }

  /**
   * A consistent snapshot of the source position's state: watermark, dedup ids
   * and every job's operator state. `dedupLimit` keeps only the newest ids
   * (enough to cover a replay overlap) so checkpoints stay small.
   */
  snapshot(dedupLimit = Infinity): RuntimeSnapshot {
    const jobs: Record<string, unknown> = {};
    for (const [id, job] of this.jobs) jobs[id] = job.snapshot();
    const ids = [...this.seen.entries()];
    return { v: 1, delay: this.delay, maxTs: this.maxTs, wm: this.wm, stats: structuredClone(this.stats), dedup: Number.isFinite(dedupLimit) ? ids.slice(-dedupLimit) : ids, jobs };
  }

  restore(s: RuntimeSnapshot) {
    this.delay = s.delay;
    this.maxTs = s.maxTs ?? -Infinity;
    this.wm = s.wm ?? -Infinity;
    this.stats = { ...Runtime.emptyStats(), ...s.stats };
    this.seen = new Map(s.dedup);
    for (const [id, job] of this.jobs) if (id in s.jobs) job.restore(s.jobs[id]);
  }
}

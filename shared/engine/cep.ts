// Edit-war detection: complex event processing over the change stream.
//
// Equivalent Flink SQL (pipeline/flink/sql/04_edit_wars.sql):
//
//   MATCH_RECOGNIZE (
//     PARTITION BY wiki, title ORDER BY event_time
//     MEASURES R1.event_time AS first_revert, R3.event_time AS third_revert, ...
//     ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW
//     PATTERN (R1 O*? R2 O*? R3) WITHIN INTERVAL '15' MINUTE
//     DEFINE R1 AS is_revert, R2 AS is_revert,
//            R3 AS is_revert AND (R2.user_name <> R1.user_name OR R3.user_name <> R1.user_name),
//            O AS NOT is_revert)
//
// i.e. three reverts in a row on the same page, by at least two people,
// with less than 15 minutes between the first and the third.

import { REVERT_RE } from "../change.ts";
import { COL } from "../table.ts";
import { isFired } from "./windows.ts";
import type { Row } from "./types.ts";

export interface EditWar {
  wiki: string;
  server_name: string;
  title: string;
  start: number;
  end: number;
  users: string[];
  comments: string[];
}

interface Revert {
  ts: number;
  user: string;
  comment: string;
}

export const isRevert = (comment: string) => REVERT_RE.test(comment);

export class EditWarDetector {
  readonly name = "MatchRecognize";
  readonly detail = "partitionBy=[wiki, title], pattern=[R1 O*? R2 O*? R3 WITHIN 15 min]";
  private wm = -Infinity;
  /** Rows waiting for the watermark, per page (CEP sorts by event time first). */
  private buffer = new Map<string, Row[]>();
  /** Reverts since the last match, per page (at most two). */
  private partial = new Map<string, Revert[]>();
  private nIn = 0;
  private nOut = 0;
  private nLate = 0;
  constructor(private within = 15 * 60_000, private onMatch: (w: EditWar) => void = () => {}) {}

  push(row: Row) {
    this.nIn++;
    const ts = row[COL.ts_ms] as number;
    if (ts <= this.wm) {
      this.nLate++; // CEP drops elements behind the watermark
      return;
    }
    const k = row[COL.wiki] + "\u0000" + row[COL.title];
    const b = this.buffer.get(k);
    if (b) b.push(row);
    else this.buffer.set(k, [row]);
  }

  watermark(wm: number) {
    if (wm <= this.wm) return;
    this.wm = wm;
    for (const [k, rows] of this.buffer) {
      const ready = rows.filter((r) => (r[COL.ts_ms] as number) <= wm);
      if (!ready.length) continue;
      const rest = rows.filter((r) => (r[COL.ts_ms] as number) > wm);
      if (rest.length) this.buffer.set(k, rest);
      else this.buffer.delete(k);
      ready.sort((a, b) => (a[COL.ts_ms] as number) - (b[COL.ts_ms] as number));
      for (const r of ready) this.step(k, r);
    }
    // Forget reverts that can no longer start a match (any later revert would
    // be at least `within` after them), keeping the ones that still can.
    for (const [k, p] of this.partial) {
      let i = 0;
      while (i < p.length && isFired(p[i].ts + this.within, wm)) i++;
      if (i === p.length) this.partial.delete(k);
      else if (i) this.partial.set(k, p.slice(i));
    }
  }

  private step(k: string, row: Row) {
    const comment = row[COL.edit_comment] as string;
    if (!isRevert(comment)) return;
    const r: Revert = { ts: row[COL.ts_ms] as number, user: row[COL.user_name] as string, comment };
    let p = this.partial.get(k) || [];
    // Drop reverts that are now too old to be the first of a match.
    while (p.length && r.ts - p[0].ts >= this.within) p = p.slice(1);
    p = [...p, r];
    if (p.length === 3) {
      const [r1, r2, r3] = p;
      if (r2.user !== r1.user || r3.user !== r1.user) {
        this.nOut++;
        this.partial.delete(k);
        this.onMatch({
          wiki: row[COL.wiki] as string,
          server_name: row[COL.server_name] as string,
          title: row[COL.title] as string,
          start: r1.ts,
          end: r3.ts,
          users: [...new Set(p.map((x) => x.user))],
          comments: p.map((x) => x.comment),
        });
        return;
      }
      p = p.slice(1);
    }
    this.partial.set(k, p);
  }

  stats() {
    let state = 0;
    for (const b of this.buffer.values()) state += b.length;
    return { in: this.nIn, out: this.nOut, late: this.nLate, state: state + this.partial.size };
  }

  snapshot() {
    return { wm: this.wm, c: [this.nIn, this.nOut, this.nLate], b: [...this.buffer.entries()], p: [...this.partial.entries()] };
  }

  restore(s: any) {
    this.wm = s?.wm ?? -Infinity;
    [this.nIn, this.nOut, this.nLate] = s?.c || [0, 0, 0];
    this.buffer = new Map(s?.b || []);
    this.partial = new Map(s?.p || []);
  }
}

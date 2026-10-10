// Aggregate functions with serialisable accumulators (so state can be checkpointed).

import { isIntegral, valueKey, type Row, type SqlType, type Value } from "./types.ts";

export type AggFn = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";

export interface AggCall {
  fn: AggFn;
  /** COUNT(*) */
  star: boolean;
  distinct: boolean;
  arg?: (row: Row) => Value;
  argType?: SqlType;
  /** `FILTER (WHERE ...)` */
  filter?: (row: Row) => boolean;
  /** Result type, following Flink: COUNT → BIGINT, SUM/AVG/MIN/MAX keep the argument type. */
  type: SqlType;
  text: string;
}

/** Accumulator: count, running sum, current min/max, and distinct values seen. */
export interface Acc {
  n: number;
  s: number;
  v: Value;
  d?: Set<string>;
}

export function resultType(fn: AggFn, argType: SqlType | undefined): SqlType {
  if (fn === "COUNT") return "BIGINT";
  if (!argType || argType === "NULL") return "INT";
  if (argType === "DECIMAL") return "DECIMAL";
  return argType;
}

export function newAcc(call: AggCall): Acc {
  return call.distinct ? { n: 0, s: 0, v: null, d: new Set() } : { n: 0, s: 0, v: null };
}

/** Adds one input row to an accumulator. Returns true if the accumulator changed. */
export function accumulate(call: AggCall, acc: Acc, row: Row): boolean {
  if (call.filter && !call.filter(row)) return false;
  if (call.star) {
    acc.n++;
    return true;
  }
  const v = call.arg!(row);
  if (v === null) return false;
  if (acc.d) {
    const k = valueKey(v);
    if (acc.d.has(k)) return false;
    acc.d.add(k);
  }
  switch (call.fn) {
    case "COUNT":
      acc.n++;
      return true;
    case "SUM":
    case "AVG":
      acc.n++;
      acc.s += Number(v);
      return true;
    case "MIN":
      acc.n++;
      if (acc.v === null || v < acc.v) {
        acc.v = v;
        return true;
      }
      return false;
    case "MAX":
      acc.n++;
      if (acc.v === null || v > acc.v) {
        acc.v = v;
        return true;
      }
      return false;
  }
}

export function result(call: AggCall, acc: Acc): Value {
  switch (call.fn) {
    case "COUNT":
      return acc.n;
    case "SUM":
      return acc.n === 0 ? null : acc.s;
    case "AVG":
      if (acc.n === 0) return null;
      // Flink keeps the argument type: AVG over an INT column is an INT (truncated).
      return isIntegral(call.type) ? Math.trunc(acc.s / acc.n) : acc.s / acc.n;
    case "MIN":
    case "MAX":
      return acc.v;
  }
}

/** Decodes a key made by valueKey() back into its value. */
export function decodeValueKey(k: string): Value {
  const tag = k[1];
  const body = k.slice(2);
  if (tag === "n") return null;
  if (tag === "s") return body;
  if (tag === "d") return Number(body);
  return tag === "t";
}

/** Merges accumulator `b` into `a` (session windows merge when they touch). */
export function mergeAcc(call: AggCall, a: Acc, b: Acc): void {
  if (a.d && b.d) {
    for (const k of b.d) {
      if (a.d.has(k)) continue;
      a.d.add(k);
      const v = decodeValueKey(k);
      a.n++;
      if (call.fn === "SUM" || call.fn === "AVG") a.s += Number(v);
      if ((call.fn === "MIN" && (a.v === null || (v as never) < (a.v as never))) || (call.fn === "MAX" && (a.v === null || (v as never) > (a.v as never)))) a.v = v;
    }
    return;
  }
  a.n += b.n;
  a.s += b.s;
  if (b.v !== null && (a.v === null || (call.fn === "MIN" ? b.v < a.v : b.v > a.v))) a.v = b.v;
}

export type AccJson = [number, number, Value, string[]?];

export const accToJson = (a: Acc): AccJson => (a.d ? [a.n, a.s, a.v, [...a.d]] : [a.n, a.s, a.v]);
export const accFromJson = (j: AccJson): Acc => (j[3] ? { n: j[0], s: j[1], v: j[2], d: new Set(j[3]) } : { n: j[0], s: j[1], v: j[2] });

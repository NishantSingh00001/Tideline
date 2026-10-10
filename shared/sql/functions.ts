// Built-in scalar functions, with Flink's names, argument counts and result types.

import { dateFormat, wallClock } from "../engine/time.ts";
import { isIntegral, isNumeric, type EngineContext, type SqlType, type Value } from "../engine/types.ts";

export interface FnDef {
  min: number;
  max: number;
  /** Result type from argument types, or an error message. */
  type: (args: SqlType[]) => SqlType | string;
  impl: (args: Value[], ctx: EngineContext) => Value;
  /** Most functions return NULL when any argument is NULL. */
  nullSafe?: boolean;
  doc: string;
}

const str = (v: Value) => String(v);
const num = (v: Value) => Number(v);
const needString = (name: string) => (a: SqlType[]) =>
  a.every((t) => t === "STRING" || t === "NULL") ? "STRING" : `${name} expects STRING arguments, got ${a.join(", ")}.`;
const needNumber = (name: string, out?: SqlType) => (a: SqlType[]) =>
  a.every((t) => isNumeric(t) || t === "NULL") ? out || (a[0] === "NULL" ? "INT" : a[0]) : `${name} expects numeric arguments, got ${a.join(", ")}.`;
const needTime = (name: string, out: SqlType) => (a: SqlType[]) =>
  a[0] === "TIMESTAMP_LTZ" || a[0] === "NULL" ? out : `${name} expects a TIMESTAMP_LTZ argument, got ${a[0]}.`;

/** HALF_UP rounding, as Java's BigDecimal does it. */
export function roundHalfUp(x: number, digits: number): number {
  const f = Math.pow(10, digits);
  const r = Math.round(Math.abs(x) * f + 1e-9) / f;
  return x < 0 ? -r : r;
}

/** Java's String.split-free SPLIT_INDEX: splits on a literal separator. */
function splitIndex(s: string, sep: string, idx: number): Value {
  if (idx < 0) return null;
  const parts = sep === "" ? [s] : s.split(sep);
  return idx < parts.length ? parts[idx] : null;
}

const reCache = new Map<string, RegExp>();
/** Translates the common Java regex dialect to JavaScript. */
export function javaRegex(p: string, global = false): RegExp {
  const key = (global ? "g" : "") + p;
  let re = reCache.get(key);
  if (!re) {
    let flags = global ? "gu" : "u";
    let src = p;
    const inline = /^\(\?([a-z]+)\)/.exec(src);
    if (inline) {
      src = src.slice(inline[0].length);
      if (inline[1].includes("i")) flags += "i";
      if (inline[1].includes("s")) flags += "s";
      if (inline[1].includes("m")) flags += "m";
    }
    try {
      re = new RegExp(src, flags);
    } catch {
      re = new RegExp(src, flags.replace("u", ""));
    }
    if (reCache.size > 200) reCache.clear();
    reCache.set(key, re);
  }
  return re;
}

function trim(s: string, chars: string, mode: string): string {
  const set = new Set(chars.split(""));
  let a = 0;
  let b = s.length;
  if (mode !== "TRAILING") while (a < b && set.has(s[a])) a++;
  if (mode !== "LEADING") while (b > a && set.has(s[b - 1])) b--;
  return s.slice(a, b);
}

export const FUNCTIONS: Record<string, FnDef> = {
  LOWER: { min: 1, max: 1, type: needString("LOWER"), impl: ([s]) => str(s).toLowerCase(), doc: "LOWER(s)" },
  UPPER: { min: 1, max: 1, type: needString("UPPER"), impl: ([s]) => str(s).toUpperCase(), doc: "UPPER(s)" },
  INITCAP: { min: 1, max: 1, type: needString("INITCAP"), impl: ([s]) => str(s).toLowerCase().replace(/(^|[^\p{L}\p{N}])(\p{L})/gu, (_, a, b) => a + b.toUpperCase()), doc: "INITCAP(s)" },
  CHAR_LENGTH: { min: 1, max: 1, type: (a) => (needString("CHAR_LENGTH")(a) === "STRING" ? "INT" : needString("CHAR_LENGTH")(a)), impl: ([s]) => str(s).length, doc: "CHAR_LENGTH(s)" },
  CONCAT: { min: 1, max: 99, type: needString("CONCAT"), impl: (a) => a.map(str).join(""), doc: "CONCAT(a, b, …)" },
  CONCAT_WS: {
    min: 2,
    max: 99,
    type: needString("CONCAT_WS"),
    nullSafe: true,
    impl: ([sep, ...rest]) => (sep === null ? null : rest.filter((x) => x !== null).map(str).join(str(sep))),
    doc: "CONCAT_WS(sep, a, b, …)",
  },
  SUBSTRING: {
    min: 2,
    max: 3,
    type: (a) => (a[0] === "STRING" || a[0] === "NULL" ? "STRING" : `SUBSTRING expects a STRING, got ${a[0]}.`),
    impl: ([s, start, len]) => {
      const t = str(s);
      let from = num(start);
      from = from > 0 ? from - 1 : from < 0 ? Math.max(0, t.length + from) : 0;
      return len === undefined ? t.slice(from) : num(len) < 0 ? null : t.slice(from, from + num(len));
    },
    doc: "SUBSTRING(s FROM start [FOR len])",
  },
  REPLACE: { min: 3, max: 3, type: needString("REPLACE"), impl: ([s, a, b]) => str(s).split(str(a)).join(str(b)), doc: "REPLACE(s, find, replacement)" },
  TRIM: { min: 3, max: 3, type: () => "STRING", impl: ([s, c, m]) => trim(str(s), str(c), str(m)), doc: "TRIM([BOTH|LEADING|TRAILING] [chars] FROM s)" },
  LTRIM: { min: 1, max: 1, type: needString("LTRIM"), impl: ([s]) => trim(str(s), " ", "LEADING"), doc: "LTRIM(s)" },
  RTRIM: { min: 1, max: 1, type: needString("RTRIM"), impl: ([s]) => trim(str(s), " ", "TRAILING"), doc: "RTRIM(s)" },
  POSITION: { min: 2, max: 2, type: (a) => (needString("POSITION")(a) === "STRING" ? "INT" : needString("POSITION")(a)), impl: ([n, h]) => str(h).indexOf(str(n)) + 1, doc: "POSITION(needle IN s)" },
  REGEXP: { min: 2, max: 2, type: (a) => (needString("REGEXP")(a) === "STRING" ? "BOOLEAN" : needString("REGEXP")(a)), impl: ([s, p]) => javaRegex(str(p)).test(str(s)), doc: "REGEXP(s, pattern): true if the pattern matches part of s" },
  REGEXP_EXTRACT: {
    min: 2,
    max: 3,
    type: (a) => (a[0] === "STRING" && a[1] === "STRING" ? "STRING" : "REGEXP_EXTRACT expects (STRING, STRING[, INT])."),
    impl: ([s, p, i]) => {
      const m = javaRegex(str(p)).exec(str(s));
      if (!m) return null;
      const g = m[i === undefined ? 1 : num(i)];
      return g === undefined ? null : g;
    },
    doc: "REGEXP_EXTRACT(s, pattern[, group])",
  },
  REGEXP_REPLACE: { min: 3, max: 3, type: needString("REGEXP_REPLACE"), impl: ([s, p, r]) => str(s).replace(javaRegex(str(p), true), str(r).replace(/\$(\d)/g, "$$$1")), doc: "REGEXP_REPLACE(s, pattern, replacement)" },
  SPLIT_INDEX: {
    min: 3,
    max: 3,
    type: (a) => (a[0] === "STRING" && a[1] === "STRING" && isIntegral(a[2]) ? "STRING" : "SPLIT_INDEX expects (STRING, STRING, INT)."),
    impl: ([s, sep, i]) => splitIndex(str(s), str(sep), num(i)),
    doc: "SPLIT_INDEX(s, separator, index)",
  },
  ABS: { min: 1, max: 1, type: needNumber("ABS"), impl: ([x]) => Math.abs(num(x)), doc: "ABS(x)" },
  CEIL: { min: 1, max: 1, type: needNumber("CEIL"), impl: ([x]) => Math.ceil(num(x)), doc: "CEIL(x)" },
  CEILING: { min: 1, max: 1, type: needNumber("CEILING"), impl: ([x]) => Math.ceil(num(x)), doc: "CEILING(x)" },
  FLOOR: { min: 1, max: 1, type: needNumber("FLOOR"), impl: ([x]) => Math.floor(num(x)), doc: "FLOOR(x)" },
  ROUND: {
    min: 1,
    max: 2,
    type: needNumber("ROUND"),
    impl: ([x, d]) => roundHalfUp(num(x), d === undefined ? 0 : num(d)),
    doc: "ROUND(x[, digits])",
  },
  MOD: {
    min: 2,
    max: 2,
    type: (a) => (a.every((t) => isIntegral(t) || t === "NULL") ? (a.includes("BIGINT") ? "BIGINT" : "INT") : "MOD expects integer arguments."),
    impl: ([a, b]) => {
      if (num(b) === 0) throw new Error("/ by zero");
      return num(a) % num(b);
    },
    doc: "MOD(a, b)",
  },
  POWER: { min: 2, max: 2, type: needNumber("POWER", "DOUBLE"), impl: ([a, b]) => Math.pow(num(a), num(b)), doc: "POWER(a, b)" },
  SQRT: { min: 1, max: 1, type: needNumber("SQRT", "DOUBLE"), impl: ([x]) => Math.sqrt(num(x)), doc: "SQRT(x)" },
  LN: { min: 1, max: 1, type: needNumber("LN", "DOUBLE"), impl: ([x]) => Math.log(num(x)), doc: "LN(x)" },
  LOG10: { min: 1, max: 1, type: needNumber("LOG10", "DOUBLE"), impl: ([x]) => Math.log10(num(x)), doc: "LOG10(x)" },
  LOG2: { min: 1, max: 1, type: needNumber("LOG2", "DOUBLE"), impl: ([x]) => Math.log2(num(x)), doc: "LOG2(x)" },
  EXP: { min: 1, max: 1, type: needNumber("EXP", "DOUBLE"), impl: ([x]) => Math.exp(num(x)), doc: "EXP(x)" },
  SIGN: { min: 1, max: 1, type: needNumber("SIGN"), impl: ([x]) => Math.sign(num(x)), doc: "SIGN(x)" },
  GREATEST: { min: 1, max: 99, type: (a) => a.find((t) => t !== "NULL") || "NULL", impl: (a) => a.reduce((m, v) => ((v as never) > (m as never) ? v : m)), doc: "GREATEST(a, b, …)" },
  LEAST: { min: 1, max: 99, type: (a) => a.find((t) => t !== "NULL") || "NULL", impl: (a) => a.reduce((m, v) => ((v as never) < (m as never) ? v : m)), doc: "LEAST(a, b, …)" },
  COALESCE: { min: 1, max: 99, nullSafe: true, type: (a) => a.find((t) => t !== "NULL") || "NULL", impl: (a) => a.find((v) => v !== null) ?? null, doc: "COALESCE(a, b, …)" },
  IFNULL: { min: 2, max: 2, nullSafe: true, type: (a) => a.find((t) => t !== "NULL") || "NULL", impl: ([a, b]) => (a === null ? b : a), doc: "IFNULL(a, b)" },
  NULLIF: { min: 2, max: 2, nullSafe: true, type: (a) => a[0], impl: ([a, b]) => (a === b ? null : a), doc: "NULLIF(a, b)" },
  IF: {
    min: 3,
    max: 3,
    nullSafe: true,
    type: (a) => (a[0] === "BOOLEAN" || a[0] === "NULL" ? (a[1] !== "NULL" ? a[1] : a[2]) : "IF expects a BOOLEAN condition."),
    impl: ([c, a, b]) => (c === true ? a : b),
    doc: "IF(condition, then, else)",
  },
  DATE_FORMAT: {
    min: 2,
    max: 2,
    type: (a) => (a[0] === "TIMESTAMP_LTZ" && a[1] === "STRING" ? "STRING" : "DATE_FORMAT expects (TIMESTAMP_LTZ, STRING)."),
    impl: ([t, f], ctx) => dateFormat(num(t), str(f), ctx.timeZone),
    doc: "DATE_FORMAT(ts, 'HH:mm:ss')",
  },
  HOUR: { min: 1, max: 1, type: needTime("HOUR", "BIGINT"), impl: ([t], ctx) => wallClock(num(t), ctx.timeZone).hour, doc: "HOUR(ts)" },
  MINUTE: { min: 1, max: 1, type: needTime("MINUTE", "BIGINT"), impl: ([t], ctx) => wallClock(num(t), ctx.timeZone).minute, doc: "MINUTE(ts)" },
  SECOND: { min: 1, max: 1, type: needTime("SECOND", "BIGINT"), impl: ([t], ctx) => wallClock(num(t), ctx.timeZone).second, doc: "SECOND(ts)" },
  TO_TIMESTAMP_LTZ: {
    min: 1,
    max: 2,
    type: (a) => (isIntegral(a[0]) || a[0] === "NULL" ? "TIMESTAMP_LTZ" : "TO_TIMESTAMP_LTZ expects a BIGINT epoch value."),
    impl: ([v, p]) => (p === undefined || num(p) === 3 ? num(v) : num(v) * 1000),
    doc: "TO_TIMESTAMP_LTZ(epoch, 3)",
  },
  CURRENT_WATERMARK: {
    min: 1,
    max: 1,
    nullSafe: true,
    type: (a) => (a[0] === "TIMESTAMP_LTZ" ? "TIMESTAMP_LTZ" : "CURRENT_WATERMARK expects the rowtime attribute."),
    impl: (_a, ctx) => (Number.isFinite(ctx.currentWatermark) ? ctx.currentWatermark : null),
    doc: "CURRENT_WATERMARK(event_time): NULL until the first watermark",
  },
};

export const AGGREGATES = new Set(["COUNT", "SUM", "AVG", "MIN", "MAX"]);

/** Names offered by the editor's autocomplete and the docs panel. */
export const FUNCTION_DOCS = Object.values(FUNCTIONS).map((f) => f.doc);

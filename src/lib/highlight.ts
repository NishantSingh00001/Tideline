// A forgiving SQL highlighter for the editor (works on half-typed queries).

import { FLINK_RESERVED } from "../../shared/sql/reserved.ts";

const EXTRA = new Set(["TUMBLE", "HOP", "CUMULATE", "SESSION", "DESCRIPTOR", "ASC", "DESC", "NULLS", "FIRST", "LAST", "PLAN"]);

export interface Span {
  text: string;
  cls?: string;
}

export function highlight(src: string): Span[] {
  const out: Span[] = [];
  const re = /(--[^\n]*|\/\*[\s\S]*?(?:\*\/|$))|('(?:[^']|'')*'?)|(`[^`]*`?)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_][A-Za-z0-9_$]*)(\s*\()?|([\s\S])/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(src))) {
    if (m[1]) out.push({ text: m[1], cls: "tok-cm" });
    else if (m[2]) out.push({ text: m[2], cls: "tok-str" });
    else if (m[3]) out.push({ text: m[3] });
    else if (m[4]) out.push({ text: m[4], cls: "tok-num" });
    else if (m[5]) {
      const up = m[5].toUpperCase();
      if (m[6]) {
        out.push({ text: m[5], cls: EXTRA.has(up) || FLINK_RESERVED.has(up) ? "tok-fn" : "tok-fn" });
        out.push({ text: m[6] });
      } else out.push({ text: m[5], cls: FLINK_RESERVED.has(up) || EXTRA.has(up) ? "tok-kw" : undefined });
    } else out.push({ text: m[7] });
  }
  // Merge neighbours with the same class to keep the DOM small.
  const merged: Span[] = [];
  for (const s of out) {
    const last = merged[merged.length - 1];
    if (last && last.cls === s.cls) last.text += s.text;
    else merged.push({ ...s });
  }
  return merged;
}

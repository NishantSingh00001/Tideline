// A plain textarea with a highlighted copy underneath: fast, accessible, and
// it never fights the browser's own editing, undo or IME.

import { useMemo, useRef } from "react";
import { highlight } from "../lib/highlight.ts";

export function SqlEditor({
  value,
  onChange,
  onRun,
  error,
  label = "Flink SQL query",
}: {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  error?: { start?: number; end?: number } | null;
  label?: string;
}) {
  const ta = useRef<HTMLTextAreaElement>(null);
  const spans = useMemo(() => {
    const s = error?.start;
    if (s === undefined || s === null || s >= value.length) return [{ part: highlight(value), err: false }];
    let e = Math.max(s + 1, error?.end ?? s + 1);
    if (e > value.length) e = value.length;
    return [
      { part: highlight(value.slice(0, s)), err: false },
      { part: highlight(value.slice(s, e)), err: true },
      { part: highlight(value.slice(e)), err: false },
    ];
  }, [value, error?.start, error?.end]);
  const lines = value.split("\n").length;
  return (
    <div className="editor">
      <div className="editor-inner">
        <div className="gutter" aria-hidden="true">
          {Array.from({ length: lines }, (_, i) => (
            <div key={i}>{i + 1}</div>
          ))}
        </div>
        <pre aria-hidden="true">
          {spans.map((g, gi) => {
            const inner = g.part.map((sp, i) => (
              <span key={i} className={sp.cls}>
                {sp.text}
              </span>
            ));
            return g.err ? (
              <span key={gi} className="tok-err">
                {inner}
              </span>
            ) : (
              <span key={gi}>{inner}</span>
            );
          })}
          {"\n "}
        </pre>
        <textarea
          ref={ta}
          value={value}
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
          aria-label={label}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
              e.preventDefault();
              onRun();
            }
          }}
        />
      </div>
    </div>
  );
}

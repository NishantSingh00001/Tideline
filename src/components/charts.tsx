// Small SVG charts, built to the dataviz rules: thin marks, 2px gaps between
// stacked segments, rounded data-ends, recessive axes, hover tooltips and a
// legend for multi-series charts.

import { useMemo, useRef, useState, type ReactNode } from "react";
import { PROJECTS, PROJECT_COLORS } from "../lib/projects.ts";
import { clock, int } from "../lib/format.ts";
import type { PulseWindow } from "../lib/live.ts";

function niceMax(v: number) {
  if (v <= 0) return 10;
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}

function useWidth(initial = 600) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(initial);
  const obs = useMemo(
    () =>
      typeof ResizeObserver === "undefined"
        ? null
        : new ResizeObserver((e) => {
            const cw = Math.round(e[0].contentRect.width);
            if (cw) setW(cw);
          }),
    [],
  );
  const set = (el: HTMLDivElement | null) => {
    (ref as { current: HTMLDivElement | null }).current = el;
    if (el && obs) obs.observe(el);
  };
  return [set, w, ref] as const;
}

export function Tip({ x, y, width, children }: { x: number; y: number; width: number; children: ReactNode }) {
  const left = x + 170 > width ? Math.max(0, x - 180) : x + 12;
  return (
    <div className="chart-tip" style={{ left, top: Math.max(0, y - 10) }}>
      {children}
    </div>
  );
}

/** Stacked bars: changes per 10-second window, by project. */
export function PulseBars({ windows }: { windows: PulseWindow[] }) {
  const [setRef, width] = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  // Taller on wide screens, where the panel sits beside the trending list.
  const H = width > 600 ? 400 : 220;
  const m = { l: 40, r: 6, t: 8, b: 24 };
  const pw = width - m.l - m.r;
  const ph = H - m.t - m.b;
  const n = 30;
  const shown = windows.slice(-n);
  const max = niceMax(Math.max(10, ...shown.map((w) => w.total)));
  const slot = pw / n;
  const bw = Math.max(3, slot - 3);
  const y = (v: number) => m.t + ph - (v / max) * ph;
  const ticks = [0, max / 2, max];
  const hw = hover !== null ? shown[hover] : null;
  return (
    <div ref={setRef} style={{ position: "relative" }}>
      <ul className="legend" aria-label="Projects">
        {PROJECTS.map((p, i) => (
          <li key={p}>
            <i style={{ background: PROJECT_COLORS[i] }} />
            {p}
          </li>
        ))}
      </ul>
      <svg width={width} height={H} role="img" aria-label="Changes per 10-second window, stacked by project" onMouseLeave={() => setHover(null)}>
        {ticks.map((t) => (
          <g key={t}>
            <line x1={m.l} x2={m.l + pw} y1={y(t)} y2={y(t)} stroke="var(--rule-2)" />
            <text x={m.l - 8} y={y(t) + 4} textAnchor="end" fontSize="12" fill="var(--ink-3)" fontFamily="var(--cond)">
              {int(t)}
            </text>
          </g>
        ))}
        {shown.map((w, i) => {
          const x0 = m.l + (n - shown.length + i) * slot + (slot - bw) / 2;
          let acc = 0;
          const topIdx = w.counts.reduce((last, c, k) => (c > 0 ? k : last), -1);
          return (
            <g key={w.start} onMouseEnter={() => setHover(i)}>
              <rect x={x0 - 1} y={m.t} width={bw + 2} height={ph} fill="transparent" />
              {w.counts.map((c, k) => {
                if (!c) return null;
                const y1 = y(acc);
                acc += c;
                // A 2px gap of surface above every segment except the top one.
                const top = y(acc) + (k === topIdx ? 0 : 2);
                const h = Math.max(0, y1 - top);
                if (h < 0.5) return null;
                const r = k === topIdx ? Math.min(3, bw / 2, h) : 0;
                return (
                  <path
                    key={k}
                    d={`M${x0},${y1} V${top + r} Q${x0},${top} ${x0 + r},${top} H${x0 + bw - r} Q${x0 + bw},${top} ${x0 + bw},${top + r} V${y1} Z`}
                    fill={PROJECT_COLORS[k]}
                    opacity={hover === null || hover === i ? 1 : 0.55}
                  />
                );
              })}
            </g>
          );
        })}
        {shown.length > 0 && (
          <>
            <text x={m.l} y={H - 6} fontSize="12" fill="var(--ink-3)" fontFamily="var(--cond)">
              {clock(shown[0].start)}
            </text>
            <text x={m.l + pw} y={H - 6} fontSize="12" textAnchor="end" fill="var(--ink-3)" fontFamily="var(--cond)">
              {clock(shown[shown.length - 1].end)}
            </text>
          </>
        )}
      </svg>
      {hw && hover !== null && (
        <Tip x={m.l + (n - shown.length + hover) * slot} y={40} width={width}>
          <strong>
            {clock(hw.start)}–{clock(hw.end)}
          </strong>
          <table>
            <tbody>
              {PROJECTS.map((p, k) => (
                <tr key={p}>
                  <td>
                    <span className="sw" style={{ background: PROJECT_COLORS[k] }} />
                    {p}
                  </td>
                  <td>{int(hw.counts[k])}</td>
                </tr>
              ))}
              <tr>
                <td>
                  <b>Total</b>
                </td>
                <td>
                  <b>{int(hw.total)}</b>
                </td>
              </tr>
            </tbody>
          </table>
        </Tip>
      )}
    </div>
  );
}

/** A single-series line with a crosshair tooltip. */
export function LineChart({
  points,
  format,
  label,
  height = 120,
  domain,
  color = "var(--s1)",
}: {
  points: { x: number; y: number }[];
  format: (v: number) => string;
  label: (x: number) => string;
  height?: number;
  domain?: [number, number];
  color?: string;
}) {
  const [setRef, width] = useWidth(300);
  const [hover, setHover] = useState<number | null>(null);
  const m = { l: 36, r: 8, t: 10, b: 18 };
  const pw = width - m.l - m.r;
  const ph = height - m.t - m.b;
  if (points.length < 2)
    return (
      <div ref={setRef} className="muted" style={{ height, display: "grid", placeItems: "center", fontSize: 14 }}>
        Waiting for two windows to close…
      </div>
    );
  const [lo, hi] = domain || [0, niceMax(Math.max(...points.map((p) => p.y)))];
  const x = (i: number) => m.l + (i / (points.length - 1)) * pw;
  const y = (v: number) => m.t + ph - ((v - lo) / (hi - lo || 1)) * ph;
  const d = points.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.y).toFixed(1)}`).join("");
  const hp = hover !== null ? points[hover] : null;
  return (
    <div ref={setRef} style={{ position: "relative" }}>
      <svg
        width={width}
        height={height}
        onMouseMove={(e) => {
          const r = (e.currentTarget as SVGSVGElement).getBoundingClientRect();
          const i = Math.round(((e.clientX - r.left - m.l) / pw) * (points.length - 1));
          setHover(Math.max(0, Math.min(points.length - 1, i)));
        }}
        onMouseLeave={() => setHover(null)}
        role="img"
        aria-label="Line chart"
      >
        {[lo, (lo + hi) / 2, hi].map((t) => (
          <g key={t}>
            <line x1={m.l} x2={m.l + pw} y1={y(t)} y2={y(t)} stroke="var(--rule-2)" />
            <text x={m.l - 6} y={y(t) + 4} textAnchor="end" fontSize="11" fill="var(--ink-3)" fontFamily="var(--cond)">
              {format(t)}
            </text>
          </g>
        ))}
        <path d={d} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
        {hp && hover !== null && (
          <>
            <line x1={x(hover)} x2={x(hover)} y1={m.t} y2={m.t + ph} stroke="var(--ink-3)" strokeDasharray="2 3" />
            <circle cx={x(hover)} cy={y(hp.y)} r="4" fill={color} stroke="var(--paper)" strokeWidth="2" />
          </>
        )}
      </svg>
      {hp && hover !== null && (
        <Tip x={x(hover)} y={y(hp.y)} width={width}>
          <strong>{label(hp.x)}</strong>
          <div>{format(hp.y)}</div>
        </Tip>
      )}
    </div>
  );
}

/** Horizontal bars with direct labels (few categories). */
export function HBars({ items, color = "var(--s1)", format = int }: { items: { label: string; value: number; color?: string }[]; color?: string; format?: (n: number) => string }) {
  const max = Math.max(1, ...items.map((i) => i.value));
  return (
    <div style={{ display: "grid", gap: 6 }}>
      {items.map((it) => (
        <div key={it.label} style={{ display: "grid", gridTemplateColumns: "110px minmax(0,1fr) 54px", gap: 10, alignItems: "center", fontSize: 14 }}>
          <span style={{ color: "var(--ink-2)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{it.label}</span>
          <span style={{ height: 12, background: "var(--rule-2)", position: "relative" }}>
            <span style={{ position: "absolute", inset: 0, width: `${(it.value / max) * 100}%`, background: it.color || color, borderRadius: "0 3px 3px 0" }} />
          </span>
          <span className="num" style={{ textAlign: "right", fontWeight: 600 }}>
            {format(it.value)}
          </span>
        </div>
      ))}
    </div>
  );
}

// The running job graph, drawn like Flink's web UI but on the chart: the
// source, the shared stages, and one branch per live panel with its
// operators' counters and state size.

import type { PipelineMetrics } from "../worker/protocol.ts";
import { int } from "../lib/format.ts";
import type { SourceStats } from "../../shared/engine/runtime.ts";

const BRANCHES: { id: string; title: string; op: string }[] = [
  { id: "pulse", title: "Changes per 10 s", op: "WindowAggregate" },
  { id: "editors", title: "People and bots", op: "WindowAggregate" },
  { id: "trending", title: "Trending articles", op: "WindowRank" },
  { id: "newPages", title: "New articles", op: "Calc" },
  { id: "bigChanges", title: "Big changes", op: "Calc" },
  { id: "editWars", title: "Edit wars", op: "MatchRecognize" },
];

export function Pipeline({ m, source, rate, delay }: { m: PipelineMetrics | null; source: SourceStats | null; rate: number; delay: number }) {
  const W = 1180;
  const rowH = 46;
  const top = 18;
  const H = top + BRANCHES.length * rowH + 8;
  const midY = top + (BRANCHES.length * rowH) / 2 - 4;
  const stages = [
    { x: 0, title: "EventStreams", sub: `${rate.toFixed(1)} changes/s`, note: "a Kafka topic, over SSE" },
    { x: 192, title: "Validate", sub: `${int(m?.dlq ?? 0)} to dead letters`, note: "schema check, flatten" },
    { x: 384, title: "Deduplicate", sub: `${int(source?.duplicates ?? 0)} dropped`, note: "by meta.id" },
    { x: 576, title: "Watermarks", sub: `${int(source?.behindWatermark ?? 0)} late`, note: `event_time − ${delay / 1000} s` },
  ];
  const speed = Math.max(0.4, Math.min(6, 30 / Math.max(rate, 1)));
  const bx = 784;
  return (
    <>
    {/* Narrow screens: the same graph as a vertical list. */}
    <div className="dag-v" aria-label="Pipeline">
      <ol className="dag-v-stages">
        {stages.map((s) => (
          <li key={s.title}>
            <b>{s.title}</b>
            <span>{s.sub}</span>
            <i>{s.note}</i>
          </li>
        ))}
      </ol>
      <ul className="dag-v-branches">
        {BRANCHES.map((b) => {
          const ops = m?.jobs[b.id] || [];
          const key = ops.find((o) => o.name === b.op) || ops[ops.length - 1];
          const first = ops[0];
          const last = ops[ops.length - 1];
          return (
            <li key={b.id}>
              <div className="op">
                <b>{key?.name || b.op}</b>
                <span>{key ? `state ${int(key.state)}${key.late ? ` · late ${int(key.late)}` : ""}` : "starting"}</span>
              </div>
              <div className="sink">
                <em>{b.title}</em>
                <span>{first ? `${int(first.in)} in → ${int(last.out)} out` : ""}</span>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
    <div className="dag">
      <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Pipeline: source, validation, deduplication, watermarks, then one branch per panel">
        <style>{`.flow{stroke-dasharray:3 7;animation:flow ${speed}s linear infinite}@keyframes flow{to{stroke-dashoffset:-20}}@media (prefers-reduced-motion:reduce){.flow{animation:none}}`}</style>
        {stages.map((s, i) => (
          <g key={s.title} transform={`translate(${s.x},${midY - 30})`}>
            {i > 0 && <line x1={-32} x2={0} y1={30} y2={30} stroke="var(--ink)" strokeWidth="1.5" className="flow" />}
            <rect width="160" height="62" fill="#fff" stroke="var(--ink)" />
            <text x="10" y="21" fontFamily="var(--sans)" fontWeight="600" fontSize="15" fill="var(--ink)">
              {s.title}
            </text>
            <text x="10" y="39" fontFamily="var(--cond)" fontSize="13" fill="var(--ink-2)">
              {s.sub}
            </text>
            <text x="10" y="54" fontFamily="var(--serif)" fontStyle="italic" fontSize="12" fill="var(--ink-3)">
              {s.note}
            </text>
          </g>
        ))}
        {BRANCHES.map((b, i) => {
          const y = top + i * rowH + rowH / 2 - 4;
          const ops = m?.jobs[b.id] || [];
          const key = ops.find((o) => o.name === b.op) || ops[ops.length - 1];
          const first = ops[0];
          const last = ops[ops.length - 1];
          return (
            <g key={b.id}>
              <path d={`M736,${midY + 1} C755,${midY + 1} 755,${y} ${bx},${y}`} fill="none" stroke="var(--ink)" strokeWidth="1.2" className="flow" />
              <rect x={bx} y={y - 17} width="200" height="34" fill="#fff" stroke="var(--ink)" />
              <text x={bx + 10} y={y - 1} fontFamily="var(--sans)" fontWeight="600" fontSize="13" fill="var(--ink)">
                {key?.name || b.op}
              </text>
              <text x={bx + 10} y={y + 12} fontFamily="var(--cond)" fontSize="12" fill="var(--ink-3)">
                {key ? `state ${int(key.state)}${key.late ? ` · late ${int(key.late)}` : ""}` : "starting"}
              </text>
              <line x1={bx + 200} x2={bx + 240} y1={y} y2={y} stroke="var(--ink)" strokeWidth="1.2" />
              <text x={bx + 250} y={y - 1} fontFamily="var(--serif)" fontSize="15" fill="var(--ink)">
                {b.title}
              </text>
              <text x={bx + 250} y={y + 13} fontFamily="var(--cond)" fontSize="12" fill="var(--ink-3)">
                {first ? `${int(first.in)} in → ${int(last.out)} out` : ""}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
    </>
  );
}

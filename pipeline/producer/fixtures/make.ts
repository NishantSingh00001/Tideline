// Writes fixtures for the Python producer's tests from the TypeScript side, so
// both languages are held to the same validation and flattening rules.
//   node --experimental-transform-types pipeline/producer/fixtures/make.ts
import { writeFileSync } from "node:fs";
import { eventsBetween } from "../../../dev/generator.ts";
import { toChange } from "../../../shared/change.ts";

const start = Date.UTC(2026, 9, 9, 12, 0, 0);
const out: string[] = [];
let n = 0;
for (const g of eventsBetween(start, start + 30_000)) {
  const v = toChange(g.event);
  if (!v.ok) continue;
  const { topic: _t, partition: _p, offset: _o, ...row } = v.change;
  out.push(JSON.stringify({ raw: g.event, expected: row }));
  if (++n >= 400) break;
}
const bad: unknown[] = [
  "just a string",
  { meta: { id: "not-a-uuid", dt: "2026-10-09T12:00:00Z" }, wiki: "enwiki", type: "edit", title: "X" },
  { meta: { id: "590988aa-4cd7-48bb-b414-ff4cbf65fd5e", dt: "yesterday" }, wiki: "enwiki", type: "edit", title: "X" },
  { meta: { id: "590988aa-4cd7-48bb-b414-ff4cbf65fd5e", dt: "2026-10-09T12:00:00Z" }, type: "edit", title: "X" },
  { meta: { id: "590988aa-4cd7-48bb-b414-ff4cbf65fd5e", dt: "2026-10-09T12:00:00Z", stream: "mediawiki.page-create" }, wiki: "enwiki", type: "edit", title: "X" },
  { wiki: "enwiki" },
];
for (const b of bad) {
  const v = toChange(b);
  out.push(JSON.stringify({ raw: b, reason: v.ok ? null : v.reason }));
}
writeFileSync(new URL("./events.jsonl", import.meta.url), out.join("\n") + "\n");
console.log(`wrote ${out.length} fixtures`);

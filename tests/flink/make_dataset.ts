// Writes tests/flink/data/events.jsonl: 20 minutes of generated recentchange
// events, flattened to the recentchange table's columns, in Kafka order.
import { mkdirSync, writeFileSync } from "node:fs";
import { eventsBetween } from "../../dev/generator.ts";
import { toChange } from "../../shared/change.ts";

const start = Date.UTC(2026, 9, 9, 12, 0, 0);
const lines: string[] = [];
for (const g of eventsBetween(start, start + 20 * 60_000)) {
  const v = toChange(g.event);
  if (!v.ok) continue;
  const { topic: _t, partition: _p, offset: _o, ...row } = v.change;
  lines.push(JSON.stringify(row));
}
mkdirSync(new URL("./data/", import.meta.url), { recursive: true });
writeFileSync(new URL("./data/events.jsonl", import.meta.url), lines.join("\n") + "\n");
console.log(`wrote ${lines.length} events`);

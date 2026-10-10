// Parity with Apache Flink: runs every query in tests/flink/queries.json on the
// Tideline engine and compares the results, row for row, with what real Flink
// produced for the same events (tests/flink/expected.json, written by
// tests/flink/run_flink.py). Also writes the summary shown on the site.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import type { Change } from "../shared/change.ts";
import { EditWarDetector } from "../shared/engine/cep.ts";
import { Runtime } from "../shared/engine/runtime.ts";
import { formatTimestamp } from "../shared/engine/time.ts";
import type { Value } from "../shared/engine/types.ts";
import { MaterializedView, QueryJob } from "../shared/sql/job.ts";
import { compile } from "../shared/sql/planner.ts";
import { EXAMPLES, PANELS, PARITY_EXTRA } from "../shared/queries.ts";
import { check, section } from "./assert.ts";

const dir = new URL("./flink/", import.meta.url);
const dataFile = new URL("./data/events.jsonl", dir);
if (!existsSync(dataFile)) {
  execFileSync(process.execPath, ["--experimental-transform-types", "--disable-warning=ExperimentalWarning", new URL("./make_dataset.ts", dir).pathname], { stdio: "inherit" });
}
const expected = JSON.parse(readFileSync(new URL("./expected.json", dir), "utf8"));
const spec = JSON.parse(readFileSync(new URL("./queries.json", dir), "utf8")) as { queries: { id: string; sql: string }[] };
const events: Change[] = readFileSync(dataFile, "utf8")
  .split("\n")
  .filter(Boolean)
  .map((l) => ({ ...JSON.parse(l), topic: "", partition: 0, offset: 0 }));

section(`Parity with Apache Flink ${expected.flink} on ${events.length.toLocaleString("en")} events`);

const norm = (v: Value, type: string): Value => {
  if (v === null) return null;
  if (type === "TIMESTAMP_LTZ") return formatTimestamp(v as number, "UTC");
  if (typeof v === "number") return Number(v.toPrecision(12));
  return v;
};
const normFlink = (v: unknown): Value => (typeof v === "number" ? Number(v.toPrecision(12)) : (v as Value));

const rt = new Runtime(5000);
const views = new Map<string, { view: MaterializedView; messages: number; types: string[]; job: QueryJob }>();
for (const q of spec.queries) {
  const cq = compile(q.sql, { timeZone: "UTC", watermarkDelay: 5000 });
  const entry = { view: new MaterializedView(), messages: 0, types: cq.schema.map((c) => c.type), job: null as unknown as QueryJob };
  entry.job = new QueryJob(q.id, cq, (k, r) => {
    entry.messages++;
    entry.view.apply(k, r);
  });
  views.set(q.id, entry);
  rt.add(entry.job);
}
const wars: string[] = [];
const det = new EditWarDetector(15 * 60_000, (w) => wars.push(JSON.stringify([w.wiki, w.title, formatTimestamp(w.start, "UTC"), formatTimestamp(w.end, "UTC")])));
rt.add({ id: "editWars", push: (r) => det.push(r), watermark: (w) => det.watermark(w), snapshot: () => null, restore: () => {} });

const t0 = performance.now();
for (const e of events) rt.ingest(e);
rt.finish();
const ms = performance.now() - t0;
console.log(`  Tideline processed ${events.length} events through ${views.size + 1} jobs in ${ms.toFixed(0)} ms`);

const titles: Record<string, string> = Object.fromEntries([...Object.values(PANELS), ...EXAMPLES, ...PARITY_EXTRA].map((q) => [q.id, q.title]));
titles.editWars = "Edit wars (MATCH_RECOGNIZE)";
const summary: { id: string; title: string; rows: number; messages: number; flinkMessages: number; match: boolean }[] = [];

function compare(id: string, mine: string[], theirs: string[], messages: number, flinkMessages: number) {
  mine.sort();
  theirs.sort();
  let same = mine.length === theirs.length;
  let firstDiff = "";
  if (same)
    for (let i = 0; i < mine.length; i++)
      if (mine[i] !== theirs[i]) {
        same = false;
        firstDiff = `\n      tideline: ${mine[i]}\n      flink:    ${theirs[i]}`;
        break;
      }
  check(same, `${id}: ${mine.length} rows vs Flink's ${theirs.length}${firstDiff}`);
  check(messages === flinkMessages, `${id}: ${messages} changelog messages vs Flink's ${flinkMessages}`);
  summary.push({ id, title: titles[id] || id, rows: theirs.length, messages, flinkMessages, match: same && messages === flinkMessages });
}

for (const q of spec.queries) {
  const exp = expected.queries[q.id];
  if (!exp) {
    check(false, `${q.id}: no Flink result in expected.json (run tests/flink/run_flink.py)`);
    continue;
  }
  const v = views.get(q.id)!;
  check(!v.job.failed, `${q.id} ran without errors ${v.job.failed || ""}`);
  const mine = v.view.list().map(({ row }) => JSON.stringify(row.map((x, i) => norm(x, v.types[i]))));
  const theirs = (exp.rows as unknown[][]).map((r) => JSON.stringify(r.map(normFlink)));
  compare(q.id, mine, theirs, v.messages, exp.messages);
}
{
  const exp = expected.queries.editWars;
  const theirs = (exp.rows as unknown[][]).map((r) => JSON.stringify(r));
  compare("editWars", wars, theirs, wars.length, exp.messages);
}

const allMatch = summary.every((s) => s.match);
writeFileSync(
  new URL("../public/parity.json", import.meta.url),
  JSON.stringify(
    {
      flink: expected.flink,
      events: events.length,
      checked: summary.reduce((s, q) => s + q.rows, 0),
      queries: summary,
      allMatch,
      settings: "parallelism 1, table.local-time-zone = UTC, pipeline.auto-watermark-interval = 0 ms, watermark = event_time - 5 s",
      editWarsSql: expected.editWarsSql,
    },
    null,
    1,
  ) + "\n",
);

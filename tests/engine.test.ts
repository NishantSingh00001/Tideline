// Engine semantics on hand-made streams, with the expected answers worked out
// from Flink's documented rules.
import type { Change } from "../shared/change.ts";
import { EditWarDetector, type EditWar } from "../shared/engine/cep.ts";
import { Runtime } from "../shared/engine/runtime.ts";
import { formatTimestamp } from "../shared/engine/time.ts";
import type { Row, RowKind } from "../shared/engine/types.ts";
import { MaterializedView, QueryJob } from "../shared/sql/job.ts";
import { compile } from "../shared/sql/planner.ts";
import { check, section } from "./assert.ts";

const T0 = Date.UTC(2026, 0, 1, 0, 0, 0);
let seq = 0;
export function ch(ts: number, p: Partial<Change> = {}): Change {
  seq++;
  return {
    id: `00000000-0000-4000-8000-${String(seq).padStart(12, "0")}`,
    change_type: "edit",
    wiki: "enwiki",
    server_name: "en.wikipedia.org",
    title: "Mumbai",
    ns: 0,
    user_name: "Asha",
    is_bot: false,
    is_minor: false,
    len_old: 100,
    len_new: 110,
    edit_comment: "",
    log_type: null,
    rev_id: seq,
    ts_ms: T0 + ts,
    topic: "eqiad.mediawiki.recentchange",
    partition: 0,
    offset: seq,
    ...p,
  };
}

interface Run {
  rt: Runtime;
  job: QueryJob;
  view: MaterializedView;
  log: { kind: RowKind; row: Row }[];
}

function start(sql: string, delay = 2000, tz = "UTC"): Run {
  const rt = new Runtime(delay);
  const view = new MaterializedView();
  const log: Run["log"] = [];
  const q = compile(sql, { timeZone: tz, watermarkDelay: delay });
  const job = new QueryJob("q", q, (kind, row) => {
    view.apply(kind, row);
    log.push({ kind, row });
  });
  rt.add(job);
  return { rt, job, view, log };
}

const fmt = (row: Row) => row.map((v) => (typeof v === "number" && v > 1e12 ? formatTimestamp(v, "UTC").slice(11, 19) : v)).join(" ");
const s = (sec: number) => sec * 1000;

section("Tumbling windows and late data");
{
  const r = start(`SELECT window_start, window_end, COUNT(*) AS n FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '10' SECOND)) GROUP BY window_start, window_end`);
  for (const t of [1, 3]) r.rt.ingest(ch(s(t)));
  check(r.log.length === 0, "nothing is emitted before the watermark passes the window end");
  r.rt.ingest(ch(s(12))); // watermark 10 s >= 9.999 s: [0,10) fires
  check(r.log.length === 1 && fmt(r.log[0].row) === "00:00:00 00:00:10 2", `[0,10) fires with 2 events once the watermark reaches 10 s (${r.log.map((l) => fmt(l.row))})`);
  r.rt.ingest(ch(s(4))); // behind the watermark: late
  check(r.rt.stats.behindWatermark === 1, "an event behind the watermark is counted as late at the source");
  check(r.log.length === 1, "the late event doesn't change or re-fire the closed window");
  r.rt.ingest(ch(s(25)));
  r.rt.finish();
  const all = r.view.list().map((x) => fmt(x.row)).sort();
  check(JSON.stringify(all) === JSON.stringify(["00:00:00 00:00:10 2", "00:00:10 00:00:20 1", "00:00:20 00:00:30 1"]), `all windows after end of input: ${all.join(" | ")}`);
  const st = r.job.stats().find((o) => o.name === "WindowAggregate")!;
  check(st.late === 1, `window operator dropped exactly one late record (${st.late})`);
}

section("Window fires exactly when watermark >= window_end - 1");
{
  const r = start(`SELECT window_start, COUNT(*) AS n FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '10' SECOND)) GROUP BY window_start, window_end`, 2000);
  r.rt.ingest(ch(0));
  r.rt.ingest(ch(11_998)); // wm = 9,998
  check(r.log.length === 0, "watermark 9.998 s: [0,10) still open");
  r.rt.ingest(ch(11_999)); // wm = 9,999
  check(r.log.length === 1, "watermark 9.999 s: [0,10) fires");
}

section("Sliding (HOP) windows");
{
  const r = start(`SELECT window_start, window_end, COUNT(*) AS n FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '10' SECOND)) GROUP BY window_start, window_end`, 0);
  r.rt.ingest(ch(s(7)));
  r.rt.ingest(ch(s(11))); // wm 11 s: [0,10) fired
  r.rt.ingest(ch(s(8))); // late for [0,10) but [5,15) is still open
  r.rt.finish();
  const rows = Object.fromEntries(r.view.list().map((x) => { const f = fmt(x.row).split(" "); return [f[0] + "-" + f[1], Number(f[2])]; }));
  check(rows["00:00:00-00:00:10"] === 1, `[0,10) has only the 7 s event (${rows["00:00:00-00:00:10"]})`);
  check(rows["00:00:05-00:00:15"] === 3, `[5,15) still accepts the late 8 s event (${rows["00:00:05-00:00:15"]})`);
  check(rows["00:00:10-00:00:20"] === 1, "[10,20) has the 11 s event");
}

section("Session windows");
{
  const r = start(`SELECT window_start, window_end, user_name, COUNT(*) AS n FROM TABLE(SESSION(TABLE recentchange PARTITION BY user_name, DESCRIPTOR(event_time), INTERVAL '3' SECOND)) GROUP BY window_start, window_end, user_name`, 1000);
  for (const t of [0, 3, 7]) r.rt.ingest(ch(s(t)));
  r.rt.ingest(ch(s(20)));
  r.rt.ingest(ch(s(26))); // wm 25: session [20,23) fired
  r.rt.ingest(ch(s(22))); // [22,25) touches nothing open -> late
  r.rt.ingest(ch(s(24))); // [24,27) touches [26,29) -> merges, not late
  r.rt.finish();
  const rows = r.view.list().map((x) => fmt(x.row)).sort();
  check(JSON.stringify(rows) === JSON.stringify(["00:00:00 00:00:06 Asha 2", "00:00:07 00:00:10 Asha 1", "00:00:20 00:00:23 Asha 1", "00:00:24 00:00:29 Asha 2"]), `sessions: ${rows.join(" | ")}`);
}

section("Cumulating windows");
{
  const r = start(`SELECT window_start, window_end, COUNT(*) AS n FROM TABLE(CUMULATE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '2' SECOND, INTERVAL '6' SECOND)) GROUP BY window_start, window_end`, 0);
  r.rt.ingest(ch(s(1)));
  r.rt.ingest(ch(s(3)));
  r.rt.finish();
  const rows = r.view.list().map((x) => fmt(x.row)).sort();
  check(JSON.stringify(rows) === JSON.stringify(["00:00:00 00:00:02 1", "00:00:00 00:00:04 2", "00:00:00 00:00:06 2"]), `cumulate: ${rows.join(" | ")}`);
}

section("Unbounded GROUP BY emits a retract changelog");
{
  const r = start(`SELECT wiki, COUNT(*) AS n, MAX(delta) AS biggest FROM recentchange GROUP BY wiki`);
  r.rt.ingest(ch(0, { len_old: 0, len_new: 50 }));
  r.rt.ingest(ch(1, { len_old: 0, len_new: 10 }));
  const kinds = r.log.map((l) => l.kind).join(" ");
  check(kinds === "+I -U +U", `first row inserts, the next updates: ${kinds}`);
  const r2 = start(`SELECT wiki, MAX(delta) AS biggest FROM recentchange GROUP BY wiki`);
  r2.rt.ingest(ch(0, { len_old: 0, len_new: 50 }));
  r2.rt.ingest(ch(1, { len_old: 0, len_new: 10 }));
  check(r2.log.length === 1, "no update is emitted when the result doesn't change");
}

section("Window Top-N");
{
  const r = start(`SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY n DESC, title) AS rownum FROM (SELECT window_start, window_end, title, COUNT(*) AS n FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end, title)) WHERE rownum <= 2`, 0);
  const titles = ["Pune", "Goa", "Pune", "Delhi", "Goa", "Pune"];
  titles.forEach((t, i) => r.rt.ingest(ch(s(i), { title: t })));
  r.rt.finish();
  const rows = r.view.list().map((x) => `${x.row[2]}:${x.row[3]}:${x.row[4]}`).sort();
  check(JSON.stringify(rows) === JSON.stringify(["Goa:2:2", "Pune:3:1"]), `top 2: ${rows.join(" ")}`);
}

section("Time zones (TIMESTAMP_LTZ windows follow the session time zone)");
{
  const r = start(`SELECT window_start, COUNT(*) AS n FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' HOUR)) GROUP BY window_start, window_end`, 0, "Asia/Kolkata");
  r.rt.ingest(ch(s(4 * 3600 + 45 * 60))); // 04:45 UTC = 10:15 IST
  r.rt.finish();
  const startUtc = new Date(r.view.list()[0].row[0] as number).toISOString();
  check(startUtc === "2026-01-01T04:30:00.000Z", `an hourly window in IST starts at 10:00 local = 04:30 UTC (${startUtc})`);
}

section("Checkpoint, crash and recovery: exactly-once");
{
  const sql = `SELECT window_start, wiki, COUNT(*) AS n, COUNT(DISTINCT user_name) AS u FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '5' SECOND, INTERVAL '20' SECOND)) GROUP BY window_start, window_end, wiki`;
  const wikis = ["enwiki", "dewiki", "hiwiki"];
  const users = ["A", "B", "C", "D"];
  const events: Change[] = [];
  for (let i = 0; i < 400; i++) events.push(ch(i * 450 + ((i * 7919) % 1300) - 600, { wiki: wikis[i % 3], user_name: users[(i * 31) % 4] }));
  // Reference: never fails.
  const ref = start(sql, 1500);
  events.forEach((e) => ref.rt.ingest(e));
  ref.rt.finish();
  // Primary: checkpoint after 250 events, crash after 320, restore, replay from 230 (overlap).
  const a = start(sql, 1500);
  const before: Row[] = [];
  for (let i = 0; i < 250; i++) a.rt.ingest(events[i]);
  const snapshot = JSON.parse(JSON.stringify(a.rt.snapshot()));
  const emittedAtCheckpoint = a.log.length;
  for (let i = 250; i < 320; i++) a.rt.ingest(events[i]);
  before.push(...a.log.slice(0, emittedAtCheckpoint).map((l) => l.row)); // results committed by the checkpoint
  const b = start(sql, 1500);
  b.rt.restore(snapshot);
  for (let i = 230; i < events.length; i++) b.rt.ingest(events[i]);
  b.rt.finish();
  check(b.rt.stats.duplicates === 20, `the 20 replayed events before the checkpoint were deduplicated (${b.rt.stats.duplicates})`);
  const got = [...before, ...b.log.map((l) => l.row)].map((r) => JSON.stringify(r)).sort();
  const want = ref.log.map((l) => JSON.stringify(l.row)).sort();
  check(got.length === want.length && got.every((x, i) => x === want[i]), `recovered output equals the uninterrupted run (${got.length} vs ${want.length} rows)`);
}

section("Edit-war detection (MATCH_RECOGNIZE semantics)");
{
  const run = (evs: Change[]) => {
    const rt = new Runtime(1000);
    const wars: EditWar[] = [];
    const det = new EditWarDetector(15 * 60_000, (w) => wars.push(w));
    rt.add({ id: "wars", push: (r) => det.push(r), watermark: (w) => det.watermark(w), snapshot: () => det.snapshot(), restore: (x) => det.restore(x) });
    evs.forEach((e) => rt.ingest(e));
    rt.finish();
    return wars;
  };
  const rv = (t: number, user: string, title = "Goa") => ch(s(t), { title, user_name: user, edit_comment: `Undid revision 1 by X` });
  check(run([rv(0, "A"), ch(s(10)), rv(60, "B"), rv(120, "A")]).length === 1, "three reverts by two people within 15 minutes → one war");
  check(run([rv(0, "A"), rv(60, "A"), rv(120, "A")]).length === 0, "one person reverting three times isn't a war");
  check(run([rv(0, "A"), rv(500, "B"), rv(900, "A")]).length === 0, "the third revert 15 minutes after the first doesn't match (WITHIN is exclusive)");
  check(run([rv(0, "A"), rv(60, "B", "Pune"), rv(120, "A")]).length === 0, "reverts on different pages don't combine");
  const w = run([rv(0, "A"), rv(30, "A"), rv(60, "A"), rv(90, "B")]);
  check(w.length === 1 && w[0].start === T0 + s(30), "after a failed start, matching resumes from the next revert");
  check(run([ch(0, { edit_comment: "Reverted edits by [[Special:Contributions/X|X]] (talk) to last version by Y", user_name: "A" }), ch(s(5), { edit_comment: "Отмена правки 1 участника X", user_name: "B" }), ch(s(9), { edit_comment: "rv vandalism", user_name: "A" })]).length === 1, "revert summaries are recognised in several languages");
}

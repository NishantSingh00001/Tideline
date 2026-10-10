// The SQL front end: errors that read like Flink's, typing rules, functions.
import { Runtime } from "../shared/engine/runtime.ts";
import { MaterializedView, QueryJob } from "../shared/sql/job.ts";
import { compile } from "../shared/sql/planner.ts";
import { EXAMPLES, PANELS, PARITY_EXTRA } from "../shared/queries.ts";
import { check, section, throws } from "./assert.ts";
import { ch } from "./engine.test.ts";

const opts = { timeZone: "UTC", watermarkDelay: 5000 };
const c = (sql: string) => compile(sql, opts);

section("Every built-in query compiles");
for (const q of [...Object.values(PANELS), ...EXAMPLES, ...PARITY_EXTRA]) {
  try {
    const cq = c(q.sql);
    check(cq.ops.length > 0 && cq.explain.includes("TableSourceScan"), `${q.id} compiles to ${cq.ops.length} operators`);
  } catch (e) {
    check(false, `${q.id}: ${(e as Error).message}`);
  }
}

section("Errors read like Flink's");
throws(() => c("SELEC wiki FROM recentchange"), /SQL parse failed\. Encountered "SELEC" at line 1, column 1/, "typo in SELECT");
throws(() => c("SELECT wiki FORM recentchange"), /Encountered "recentchange" at line 1, column 18/, "typo in FROM (FORM is read as an alias, as in Flink)");
throws(() => c("SELECT COUNT(*) AS bytes FROM recentchange"), /Encountered "bytes"/, "reserved word as alias (Flink reserves BYTES)");
check(c("SELECT COUNT(*) AS `bytes` FROM recentchange").schema[0].name === "bytes", "reserved word in backticks is fine");
throws(() => c("SELECT Wiki FROM recentchange"), /Column 'Wiki' not found in any table; did you mean 'wiki'\?/, "identifiers are case-sensitive, with a hint");
throws(() => c("SELECT wiki FROM RecentChange"), /Object 'RecentChange' not found.*did you mean 'recentchange'/, "table names are case-sensitive");
throws(() => c("SELECT wiki FROM recentchange WHERE ns = 'zero'"), /Cannot apply '=' to arguments of type '<INTEGER> = <STRING>'/, "type mismatch in comparison");
throws(() => c("SELECT wiki, COUNT(*) FROM recentchange"), /Expression 'wiki' is not being grouped/, "column outside GROUP BY");
throws(() => c("SELECT * FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(ts_ms), INTERVAL '1' MINUTE))"), /requires the timecol is a time attribute type, but is BIGINT/, "window on a non-time column");
throws(() => c("SELECT window_start, COUNT(*) FROM TABLE(HOP(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '7' SECOND, INTERVAL '30' SECOND)) GROUP BY window_start, window_end"), /integral multiple of slide/, "HOP size must be a multiple of the slide");
throws(() => c("SELECT title FROM recentchange ORDER BY title"), /Sort on a non-time-attribute field is not supported/, "ORDER BY in a stream");
throws(() => c("SELECT COUNT(*) FROM recentchange WHERE COUNT(*) > 1"), /not allowed in WHERE/, "aggregate in WHERE");
throws(() => c("SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY delta DESC) AS rn FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)))"), /needs a filter on the rank.*rn <= 10/, "Top-N without a rank filter");
throws(() => c("SELECT LOWR(title) FROM recentchange"), /No match found for function signature LOWR/, "unknown function");
throws(() => c("INSERT INTO sink SELECT * FROM recentchange"), /Only SELECT queries run here/, "INSERT is explained");
throws(() => c("SELECT title FROM recentchange WHERE title = 'unterminated"), /Unterminated string literal/, "unterminated string");

section("Typing follows Flink");
const types = (sql: string) => c(sql).schema.map((s) => s.type).join(",");
check(types("SELECT 7 / 2, 7.0 / 2, CAST(7 AS DOUBLE) / 2 FROM recentchange") === "INT,DECIMAL,DOUBLE", "integer division stays INT");
check(types("SELECT COUNT(*), SUM(ns), AVG(delta), MAX(title) FROM recentchange") === "BIGINT,INT,INT,STRING", "aggregate result types (AVG of INT is INT)");
check(c("SELECT COUNT(*) FROM recentchange").schema[0].name === "EXPR$0", "unnamed expressions are EXPR$n");
check(c("SELECT wiki, COUNT(*) FROM recentchange GROUP BY wiki").changelog, "unbounded GROUP BY produces a changelog");
check(!c(PANELS.pulse.sql).changelog && c(PANELS.pulse.sql).windowed, "window aggregation is append-only");
check(c("EXPLAIN " + PANELS.trending.sql).explainOnly, "EXPLAIN is recognised");
check(/WindowRank\(rankType=\[ROW_NUMBER\], rankRange=\[rankStart=1, rankEnd=10\]/.test(c(PANELS.trending.sql).explain), "Top-N shows up as WindowRank in EXPLAIN");

section("Expressions evaluate like Flink");
{
  const rt = new Runtime(0);
  const view = new MaterializedView();
  const q = c(`SELECT delta / 7, delta % 7, -7 / 2, ROUND(-2.5, 0), ROUND(2.345, 2), SPLIT_INDEX(server_name, '.', 0), SPLIT_INDEX(server_name, '.', 9),
    CAST(is_minor AS STRING), CAST('42' AS INT) + 1, COALESCE(NULL, len_old), NULLIF(ns, 0), title LIKE 'M%a_',
    REGEXP_EXTRACT(edit_comment, 'revision (\\d+)', 1), IF(delta > 0, 'up', 'down'), CHAR_LENGTH(title), INITCAP('hello wORLD'),
    TRIM(LEADING 'x' FROM 'xxabcxx'), CONCAT('a', NULL), CONCAT_WS('-', 'a', NULL, 'b'), DATE_FORMAT(event_time, 'yyyy-MM-dd HH:mm')
    FROM recentchange`);
  rt.add(new QueryJob("x", q, (k, r) => view.apply(k, r)));
  rt.ingest(ch(0, { len_old: 100, len_new: 110, title: "Mumbai", is_minor: true, edit_comment: "Undid revision 12345 by X" }));
  const row = view.list()[0].row;
  const want = [1, 3, -3, -3, 2.35, "en", null, "TRUE", 43, 100, null, true, "12345", "up", 6, "Hello World", "abcxx", null, "a-b", "2026-01-01 00:00"];
  want.forEach((w, i) => check(row[i] === w, `column ${i}: expected ${JSON.stringify(w)}, got ${JSON.stringify(row[i])}`));
}

section("Three-valued logic");
{
  const rt = new Runtime(0);
  const view = new MaterializedView();
  const q = c(`SELECT len_old > 5 AND FALSE, len_old > 5 OR TRUE, len_old > 5 OR FALSE, NOT (len_old > 5), len_old IN (1, 2), len_old IS NULL FROM recentchange`);
  rt.add(new QueryJob("x", q, (k, r) => view.apply(k, r)));
  rt.ingest(ch(0, { len_old: null, change_type: "new" }));
  const row = view.list()[0].row;
  check(JSON.stringify(row) === JSON.stringify([false, true, null, null, null, true]), `NULL logic: ${JSON.stringify(row)}`);
}

section("Runtime errors fail the job, like Flink");
{
  const rt = new Runtime(0);
  const job = new QueryJob("x", c("SELECT 10 / (ns - ns) FROM recentchange"), () => {});
  rt.add(job);
  rt.ingest(ch(0));
  check(/ArithmeticException: \/ by zero/.test(job.failed || ""), `division by zero: ${job.failed}`);
}

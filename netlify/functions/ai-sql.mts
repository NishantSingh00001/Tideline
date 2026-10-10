// English → Flink SQL with Gemini.
//   POST /api/ai/sql { question } → { sql, explanation, model, attempts }
// The model's SQL is compiled by Tideline's own parser and planner before it
// is returned; if it doesn't compile, the error goes back to the model (up to
// three drafts). Rate-limited per visitor and per day, to protect the API quota.

import type { Config } from "@netlify/functions";
import { z } from "zod";
import { compile } from "../../shared/sql/planner.ts";
import { EXAMPLES, PANELS } from "../../shared/queries.ts";
import { COLUMNS, COLUMN_DOCS } from "../../shared/table.ts";
import { FUNCTION_DOCS } from "../../shared/sql/functions.ts";
import { env } from "../lib/env.ts";
import { generate } from "../lib/gemini.ts";
import { body, clientKey, handle, HttpError, json, methodNotAllowed } from "../lib/http.ts";
import { rateLimit, update } from "../lib/store.ts";

const typeName = (t: string) => (t === "TIMESTAMP_LTZ" ? "TIMESTAMP_LTZ(3)" : t);

const SYSTEM = `You write Apache Flink SQL (Flink 2.x dialect) for exactly one streaming table, and nothing else.

Table recentchange: Wikimedia's live stream of changes to Wikipedia, Wikidata, Commons and other wikis.
${COLUMNS.map((c) => `- ${c.name} ${typeName(c.type)}${c.rowtime ? " (the rowtime attribute; watermark = event_time - INTERVAL '5' SECOND)" : ""}: ${COLUMN_DOCS[c.name]}`).join("\n")}

Rules:
- Return one SELECT statement. Never INSERT, CREATE, SET or several statements.
- Time-based grouping must use a window table function in FROM, then GROUP BY window_start, window_end and the keys:
  TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
  TABLE(HOP(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '10' MINUTES))  -- slide, then size; size must be a multiple of slide
  TABLE(CUMULATE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '1' HOUR))  -- step, then max size
  TABLE(SESSION(TABLE recentchange PARTITION BY user_name, DESCRIPTOR(event_time), INTERVAL '5' MINUTES))  -- GROUP BY must be exactly the PARTITION BY columns plus window_start, window_end
- "Top N per window" uses window Top-N: SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY edits DESC, title) AS rownum FROM (<window aggregation>)) WHERE rownum <= N. Always add a tie-breaker column to the ORDER BY.
- No ORDER BY outside OVER(...), no joins, no UNION, no subqueries in WHERE, no OVER windows with ROWS/RANGE.
- A GROUP BY without a window is allowed: it produces an updating result (a changelog).
- Column aliases must not be SQL reserved words: never count, sum, value, user, bytes, timestamp, window, result, rank, language, position, year, hour, minute. Use names like changes, edits, editors, pages, net_bytes.
- Identifiers are case-sensitive and lower case. Strings use single quotes.
- Aggregates: COUNT, SUM, AVG, MIN, MAX, with DISTINCT and FILTER (WHERE ...). Scalar functions: ${FUNCTION_DOCS.join("; ")}. Also CASE, CAST, LIKE, IN, BETWEEN, IS NULL.
- Useful filters: Wikipedia articles are ns = 0 AND server_name LIKE '%.wikipedia.org'. English Wikipedia is wiki = 'enwiki'; Hindi is 'hiwiki', Marathi 'mrwiki', Wikidata 'wikidatawiki', Commons 'commonswiki'. People: NOT is_bot. New pages: change_type = 'new'. Bytes added or removed: delta.
- If no window size is asked for, pick a sensible one (1 minute for counts, 10 minutes sliding every minute for rankings).
- Format the SQL readably: one clause per line, two-space indentation.
If the question can't be answered from this table, return an empty sql and say why in the explanation.

Reply as JSON: {"sql": "...", "explanation": "One or two plain sentences on what the result shows."}

Examples of good queries:
${[PANELS.pulse, PANELS.trending, EXAMPLES.find((e) => e.id === "sessions")!].map((q) => `-- ${q.title}\n${q.sql}`).join("\n\n")}`;

const SCHEMA = {
  type: "OBJECT",
  properties: { sql: { type: "STRING" }, explanation: { type: "STRING" } },
  required: ["sql", "explanation"],
};

export default handle(async (req, context) => {
  if (req.method !== "POST") return methodNotAllowed(["POST"]);
  const { question } = await body(req, z.object({ question: z.string().trim().min(3, "Ask a question first.").max(300, "Keep the question under 300 characters.") }));
  await rateLimit(`ai:${await clientKey(context, "ai")}`, 15, 3600, "You've asked a lot of questions this hour. Write the SQL by hand for a bit, or try again later.");
  const cap = Number(env("DAILY_AI_LIMIT") || 300);
  const day = new Date().toISOString().slice(0, 10);
  let over = false;
  await update<number>(`ai-day/${day}`, (n) => {
    if ((n || 0) >= cap) {
      over = true;
      return undefined;
    }
    return (n || 0) + 1;
  });
  if (over) throw new HttpError(429, "The site's daily AI allowance is used up. Try again tomorrow, or start from an example.");

  let prompt = `Question: ${question}`;
  let lastError = "";
  for (let attempt = 1; attempt <= 3; attempt++) {
    const r = await generate<{ sql: string; explanation: string }>({ system: SYSTEM, prompt, schema: SCHEMA, temperature: 0.2, maxOutputTokens: 1500, deadlineMs: 20000 });
    const sql = (r.data?.sql || "").trim().replace(/;\s*$/, "");
    const explanation = (r.data?.explanation || "").trim();
    if (!sql) throw new HttpError(422, explanation || "That question can't be answered from the edit stream.");
    try {
      const q = compile(sql, { timeZone: "UTC", watermarkDelay: 5000 });
      if (q.explainOnly) throw new Error("Return a SELECT query, not EXPLAIN.");
      return json({ sql, explanation, model: r.model, attempts: attempt });
    } catch (e) {
      lastError = (e as Error).message;
      prompt = `Question: ${question}\n\nYour previous answer was rejected by the Flink SQL parser.\nSQL:\n${sql}\n\nError: ${lastError}\n\nReturn a corrected query.`;
    }
  }
  throw new HttpError(422, `The AI couldn't write a query that runs (${lastError}). Try rephrasing, or start from an example.`);
});

export const config: Config = { path: "/api/ai/sql" };

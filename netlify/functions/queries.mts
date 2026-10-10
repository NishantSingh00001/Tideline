// Shared queries.
//   POST /api/queries      { sql, title }  → { id }   (only SQL that compiles is saved)
//   GET  /api/queries/:id                   → { id, sql, title, created, views }

import type { Config } from "@netlify/functions";
import { z } from "zod";
import { compile } from "../../shared/sql/planner.ts";
import { body, clientKey, handle, HttpError, json, methodNotAllowed } from "../lib/http.ts";
import { getJSON, rateLimit, store, update } from "../lib/store.ts";
import { ID_RE, randomId } from "../lib/ids.ts";

interface Saved {
  sql: string;
  title: string;
  created: string;
  views: number;
}

const Body = z.object({
  sql: z.string().trim().min(1, "Write a query first.").max(8000, "That query is too long to share."),
  title: z.string().trim().max(100).optional(),
});

export default handle(async (req, context) => {
  const id = context.params?.id;
  if (!id) {
    if (req.method !== "POST") return methodNotAllowed(["POST"]);
    const { sql, title } = await body(req, Body);
    try {
      compile(sql, { timeZone: "UTC", watermarkDelay: 5000 });
    } catch (e) {
      throw new HttpError(422, `Only queries that run can be shared. ${(e as Error).message}`);
    }
    await rateLimit(`q:${await clientKey(context, "q")}`, 30, 3600, "That's a lot of shared queries. Try again in an hour.");
    for (let i = 0; i < 5; i++) {
      const qid = randomId(8);
      const rec: Saved = { sql, title: title || "Flink SQL query", created: new Date().toISOString(), views: 0 };
      const res = await store().setJSON(`q/${qid}`, rec, { onlyIfNew: true });
      if (res.modified) return json({ id: qid }, 201);
    }
    throw new HttpError(503, "Couldn't save the query. Please try again.");
  }
  if (req.method !== "GET") return methodNotAllowed(["GET"]);
  if (!ID_RE(8).test(id)) throw new HttpError(400, "That link isn't valid.");
  const rec = await getJSON<Saved>(`q/${id}`);
  if (!rec) throw new HttpError(410, "This shared query doesn't exist any more.");
  await update<Saved>(`q/${id}`, (cur) => (cur ? { ...cur, views: cur.views + 1 } : undefined)).catch(() => null);
  return json({ id, ...rec, views: rec.views + 1 });
});

export const config: Config = { path: ["/api/queries", "/api/queries/:id"] };

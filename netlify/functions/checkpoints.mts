// Durable storage for the recovery demo's checkpoints, like Flink's
// checkpoint directory on S3 or HDFS.
//   POST /api/checkpoints        (the checkpoint JSON)  → { id, bytes }
//   GET  /api/checkpoints/:id                           → the checkpoint
// Checkpoints are deleted after a day by the cleanup job.

import type { Config } from "@netlify/functions";
import { clientKey, handle, HttpError, json, methodNotAllowed } from "../lib/http.ts";
import { rateLimit, store } from "../lib/store.ts";
import { randomId } from "../lib/ids.ts";

const MAX_BYTES = 300_000;
const ID = /^cp[0-9a-z]{8,10}[2-9a-hjkmnp-z]{6}$/;

function looksLikeCheckpoint(x: any): boolean {
  return (
    x &&
    typeof x === "object" &&
    (x.position === null || typeof x.position === "number") &&
    Array.isArray(x.committed) &&
    x.snapshot &&
    x.snapshot.v === 1 &&
    typeof x.snapshot.jobs === "object" &&
    Array.isArray(x.snapshot.dedup)
  );
}

export default handle(async (req, context) => {
  const id = context.params?.id;
  if (!id) {
    if (req.method !== "POST") return methodNotAllowed(["POST"]);
    const text = await req.text();
    if (text.length > MAX_BYTES) throw new HttpError(413, "That checkpoint is too large.");
    let data: unknown;
    try {
      data = JSON.parse(text);
    } catch {
      throw new HttpError(400, "The checkpoint isn't valid JSON.");
    }
    if (!looksLikeCheckpoint(data)) throw new HttpError(400, "That doesn't look like a Tideline checkpoint.");
    await rateLimit(`cp:${await clientKey(context, "cp")}`, 20, 60, "Too many checkpoints in a minute.");
    // The timestamp in the id lets the cleanup job expire old checkpoints without reading them.
    const cid = `cp${Date.now().toString(36)}${randomId(6)}`;
    await store().set(`cp/${cid}`, text);
    return json({ id: cid, bytes: text.length }, 201);
  }
  if (req.method !== "GET") return methodNotAllowed(["GET"]);
  if (!ID.test(id)) throw new HttpError(400, "That checkpoint id isn't valid.");
  const text = await store().get(`cp/${id}`, { type: "text" });
  if (!text) throw new HttpError(410, "This checkpoint has expired.");
  return new Response(text, { status: 200, headers: { "content-type": "application/json", "cache-control": "private, max-age=3600" } });
});

export const config: Config = { path: ["/api/checkpoints", "/api/checkpoints/:id"] };

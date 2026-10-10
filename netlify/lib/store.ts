// Netlify Blobs helpers: JSON documents, optimistic updates, dedupe keys and rate limits.

import { getStore } from "@netlify/blobs";
import { HttpError } from "./env.ts";

export const store = () => getStore({ name: "tideline", consistency: "strong" });

export async function getJSON<T>(key: string): Promise<T | null> {
  return (await store().get(key, { type: "json" })) as T | null;
}
export const putJSON = (key: string, value: unknown) => store().setJSON(key, value);

/** Read-modify-write with an ETag check, retried on conflicts. Return undefined to skip writing. */
export async function update<T>(key: string, fn: (current: T | null) => T | undefined, tries = 8): Promise<T | null> {
  const s = store();
  for (let i = 0; i < tries; i++) {
    const cur = await s.getWithMetadata(key, { type: "json" });
    const next = fn(cur ? (cur.data as T) : null);
    if (next === undefined) return cur ? (cur.data as T) : null;
    const res = cur?.etag
      ? await s.setJSON(key, next, { onlyIfMatch: cur.etag })
      : cur
        ? await s.setJSON(key, next) // local sandbox: no ETags
        : await s.setJSON(key, next, { onlyIfNew: true });
    if (res.modified) return next;
    await new Promise((r) => setTimeout(r, 25 + Math.random() * 60));
  }
  throw new HttpError(503, "Busy right now. Please try again.");
}

/** True the first time a key is seen (webhook retries and duplicates are ignored). */
export async function firstTime(key: string): Promise<boolean> {
  const res = await store().setJSON(`seen/${key}`, { at: Date.now() }, { onlyIfNew: true });
  return res.modified;
}

export async function rateLimit(key: string, limit: number, windowSec: number, message = "Too many requests. Please slow down."): Promise<void> {
  const start = Math.floor(Date.now() / 1000 / windowSec) * windowSec;
  let over = false;
  await update<number>(`limit/${key.replace(/[^a-zA-Z0-9:_+-]/g, "")}/${start}`, (n) => {
    const v = n || 0;
    if (v >= limit) { over = true; return undefined; }
    return v + 1;
  });
  if (over) throw new HttpError(429, message);
}

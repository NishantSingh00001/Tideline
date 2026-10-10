// Daily housekeeping: expired checkpoints (older than a day), old rate-limit
// windows and AI day counters.

import type { Config } from "@netlify/functions";
import { store } from "../lib/store.ts";

export default async () => {
  const s = store();
  const now = Date.now();
  let removed = 0;
  for (const { key } of (await s.list({ prefix: "cp/" })).blobs) {
    const ts = parseInt(key.slice(5, key.length - 6), 36);
    if (!Number.isFinite(ts) || ts < now - 86_400_000) {
      await s.delete(key);
      removed++;
    }
  }
  for (const { key } of (await s.list({ prefix: "limit/" })).blobs) {
    const start = Number(key.split("/").pop());
    if (Number.isFinite(start) && start * 1000 < now - 2 * 86_400_000) {
      await s.delete(key);
      removed++;
    }
  }
  for (const { key } of (await s.list({ prefix: "ai-day/" })).blobs) {
    if (Date.parse(key.slice(7)) < now - 7 * 86_400_000) {
      await s.delete(key);
      removed++;
    }
  }
  console.log("cleanup: removed", removed, "keys");
};

export const config: Config = { schedule: "@daily" };

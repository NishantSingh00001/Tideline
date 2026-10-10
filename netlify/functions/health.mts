// GET /api/health: is the API up, and is AI switched on.

import type { Config } from "@netlify/functions";
import { aiConfigured } from "../lib/gemini.ts";
import { handle, json } from "../lib/http.ts";

export default handle(async () => json({ ok: true, ai: aiConfigured(), service: "tideline" }, 200, { "cache-control": "public, max-age=30" }));

export const config: Config = { path: "/api/health" };

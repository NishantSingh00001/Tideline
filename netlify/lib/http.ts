// Small helpers shared by the API functions: JSON responses, errors, body parsing, client keys.

import type { Context } from "@netlify/functions";
import type { z, ZodTypeAny } from "zod";
import { HttpError } from "./env.ts";

export { HttpError };

export const json = (data: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...headers } });

/** Wraps a handler so thrown HttpErrors become JSON responses and anything else a 500. */
export function handle(fn: (req: Request, context: Context) => Promise<Response>) {
  return async (req: Request, context: Context): Promise<Response> => {
    try {
      return await fn(req, context);
    } catch (err) {
      if (err instanceof HttpError) return json({ error: err.message }, err.status);
      console.error(err);
      return json({ error: "Something went wrong on our side. Please try again." }, 500);
    }
  };
}

export async function body<S extends ZodTypeAny>(req: Request, schema: S, maxBytes = 64_000): Promise<z.output<S>> {
  const text = await req.text();
  if (text.length > maxBytes) throw new HttpError(413, "That request is too large.");
  let raw: unknown;
  try { raw = JSON.parse(text || "{}"); } catch { throw new HttpError(400, "The request body isn't valid JSON."); }
  const parsed = schema.safeParse(raw);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    throw new HttpError(400, `${issue.path.join(".") || "body"}: ${issue.message}`);
  }
  return parsed.data;
}

/** A short, non-reversible key for rate limits (the IP itself is never stored). */
export async function clientKey(context: Context, salt: string): Promise<string> {
  const data = new TextEncoder().encode(`${salt}:${context.ip || "local"}`);
  const hash = new Uint8Array(await crypto.subtle.digest("SHA-256", data));
  return Array.from(hash.slice(0, 8), (b) => b.toString(16).padStart(2, "0")).join("");
}

export function methodNotAllowed(allowed: string[]) {
  return json({ error: `Use ${allowed.join(" or ")}.` }, 405, { allow: allowed.join(", ") });
}

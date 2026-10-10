// Minimal Gemini API client (REST, no SDK) with a model fallback chain.
// GEMINI_API_KEY is required for AI features; GEMINI_MODEL (tried first) and
// GEMINI_ENDPOINT (e.g. an API gateway) are optional.

import { env, HttpError } from "./env.ts";

const DEFAULT_MODELS = ["gemini-3.8-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite", "gemini-3-flash-preview", "gemini-2.5-flash"];
const endpoint = () => (env("GEMINI_ENDPOINT") || "https://generativelanguage.googleapis.com/v1beta/models").replace(/\/+$/, "");

// Models that answered "not found" are skipped for a while by this function instance.
const missing = new Map<string, number>();

export const aiConfigured = () => Boolean(env("GEMINI_API_KEY"));

export function modelChain(): string[] {
  const all = [...new Set([env("GEMINI_MODEL"), ...DEFAULT_MODELS].filter(Boolean) as string[])];
  const live = all.filter((m) => !((missing.get(m) || 0) > Date.now()));
  return live.length ? live : all;
}

type Err = Error & { status?: number };

async function call(model: string, body: unknown, key: string, timeoutMs: number): Promise<string> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(`${endpoint()}/${encodeURIComponent(model)}:generateContent`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-goog-api-key": key },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    const text = await res.text();
    let data: any = null;
    try { data = JSON.parse(text); } catch { /* non-JSON error body */ }
    if (!res.ok) {
      const err: Err = new Error(data?.error?.message || `Gemini returned ${res.status}`);
      err.status = res.status;
      throw err;
    }
    const cand = data?.candidates?.[0];
    const out = (cand?.content?.parts || []).filter((p: any) => !p.thought).map((p: any) => p.text || "").join("");
    if (!out) {
      const err: Err = new Error(`Empty response (${cand?.finishReason || data?.promptFeedback?.blockReason || "unknown"})`);
      err.status = 502;
      throw err;
    }
    return out;
  } finally {
    clearTimeout(timer);
  }
}

export interface GenerateOptions {
  system: string;
  prompt: string;
  schema?: unknown;
  file?: { mimeType: string; data: string }; // base64
  temperature?: number;
  maxOutputTokens?: number;
  deadlineMs?: number;
}

export async function generate<T = unknown>(opts: GenerateOptions): Promise<{ text?: string; data?: T; model: string }> {
  const { system, prompt, schema, file, temperature = 0.6, maxOutputTokens = 4096, deadlineMs = 50000 } = opts;
  const key = env("GEMINI_API_KEY");
  if (!key) throw new HttpError(503, "AI isn't set up on this deployment yet. Add GEMINI_API_KEY in Netlify.");
  const started = Date.now();
  let lastErr: Err | undefined;
  for (const model of modelChain()) {
    const left = deadlineMs - (Date.now() - started);
    if (left < 4000) break;
    const generationConfig: Record<string, unknown> = { temperature, maxOutputTokens };
    if (schema) Object.assign(generationConfig, { responseMimeType: "application/json", responseSchema: schema });
    const parts: unknown[] = [...(file ? [{ inlineData: file }] : []), { text: prompt }];
    const body = { systemInstruction: { parts: [{ text: system }] }, contents: [{ role: "user", parts }], generationConfig };
    try {
      let text: string;
      try {
        text = await call(model, body, key, Math.min(left, 45000));
      } catch (err) {
        const e = err as Err;
        // Some models reject a schema feature: retry once in plain JSON mode.
        if (schema && e.status === 400 && /schema/i.test(e.message)) {
          delete generationConfig.responseSchema;
          (parts[parts.length - 1] as { text: string }).text += `\n\nReply with JSON only, matching this schema:\n${JSON.stringify(schema)}`;
          text = await call(model, body, key, Math.min(deadlineMs - (Date.now() - started), 45000));
        } else throw err;
      }
      if (!schema) return { text: text.trim(), model };
      return { data: parseJson(text) as T, model };
    } catch (err) {
      const e = err as Err;
      lastErr = e;
      const notFound = e.status === 404 || (e.status === 400 && /not found|not supported|unsupported|invalid model/i.test(e.message));
      if (notFound) missing.set(model, Date.now() + 30 * 60 * 1000);
      const retryable = notFound || [429, 500, 502, 503, 504].includes(e.status || 0) || e.name === "AbortError" || e instanceof SyntaxError;
      console.warn(`Gemini ${model} failed: ${e.status || ""} ${e.message}`);
      if (!retryable) break;
    }
  }
  const status = lastErr?.status === 429 ? 429 : 502;
  const msg = lastErr?.status === 429
    ? "The Gemini API quota is used up for now. Try again later."
    : lastErr?.status === 400 || lastErr?.status === 401 || lastErr?.status === 403
      ? "The Gemini API key was rejected. Check GEMINI_API_KEY in Netlify."
      : "The AI didn't respond in time. Please try again.";
  throw new HttpError(status, msg);
}

export function parseJson(text: string): unknown {
  const cleaned = text.trim().replace(/^```(?:json)?\s*/i, "").replace(/```\s*$/, "");
  const start = cleaned.search(/[[{]/);
  return JSON.parse(start > 0 ? cleaned.slice(start) : cleaned);
}

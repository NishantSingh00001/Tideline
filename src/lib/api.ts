// Calls to the site's own API (Netlify Functions).

export class ApiError extends Error {
  constructor(message: string, public status: number) {
    super(message);
  }
}

export async function api<T>(path: string, init?: RequestInit & { json?: unknown }): Promise<T> {
  const { json, ...rest } = init || {};
  const res = await fetch(path, {
    ...rest,
    headers: { ...(json !== undefined ? { "content-type": "application/json" } : {}), ...(rest.headers || {}) },
    body: json !== undefined ? JSON.stringify(json) : rest.body,
  });
  let data: any = null;
  try {
    data = await res.json();
  } catch {
    /* not JSON */
  }
  if (!res.ok) throw new ApiError(data?.error || `The server answered ${res.status}.`, res.status);
  return data as T;
}

export interface SavedQuery {
  id: string;
  sql: string;
  title: string;
  created: string;
  views: number;
}

export interface AiSql {
  sql: string;
  explanation: string;
  model: string;
  attempts: number;
}

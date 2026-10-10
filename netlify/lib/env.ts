// Environment access that works in Netlify Functions (Netlify.env) and plain Node.
declare const Netlify: { env: { get(name: string): string | undefined } } | undefined;

export function env(name: string): string | undefined {
  try {
    if (typeof Netlify !== "undefined" && Netlify?.env) {
      const v = Netlify.env.get(name);
      if (v) return v;
    }
  } catch { /* fall through */ }
  try {
    return process.env[name] || undefined;
  } catch {
    return undefined;
  }
}

export class HttpError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

/** The public URL of this site, used in webhook, media and login links. */
export function siteUrl(req?: Request): string {
  const fromEnv = env("URL") || env("SITE_URL");
  if (fromEnv) return fromEnv.replace(/\/+$/, "");
  if (req) {
    const u = new URL(req.url);
    const host = req.headers.get("x-forwarded-host") || u.host;
    const proto = req.headers.get("x-forwarded-proto") || u.protocol.replace(":", "");
    return `${proto}://${host}`;
  }
  return "http://localhost:8888";
}

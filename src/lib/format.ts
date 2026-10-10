// Formatting helpers for the page (the viewer's own time zone).

const fmtCache = new Map<string, Intl.DateTimeFormat>();
function dtf(opts: Intl.DateTimeFormatOptions) {
  const k = JSON.stringify(opts);
  let f = fmtCache.get(k);
  if (!f) fmtCache.set(k, (f = new Intl.DateTimeFormat("en-GB", opts)));
  return f;
}

export const clock = (ms: number | null | undefined) => (ms == null || !Number.isFinite(ms) ? "–" : dtf({ hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(ms));
export const hm = (ms: number) => dtf({ hour: "2-digit", minute: "2-digit", hour12: false }).format(ms);
export const dayTime = (ms: number) => dtf({ day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(ms);

export const int = (n: number | null | undefined) => (n == null || !Number.isFinite(n) ? "–" : Math.round(n).toLocaleString("en-IN"));
export const signed = (n: number | null | undefined) => (n == null ? "–" : (n > 0 ? "+" : n < 0 ? "−" : "") + Math.abs(n).toLocaleString("en-IN"));
export const pct = (x: number, digits = 0) => `${(x * 100).toFixed(digits)}%`;

export function duration(ms: number | null | undefined): string {
  if (ms == null || !Number.isFinite(ms)) return "–";
  const a = Math.abs(ms);
  if (a < 1000) return `${Math.round(a)} ms`;
  if (a < 60_000) return `${(a / 1000).toFixed(a < 10_000 ? 1 : 0)} s`;
  if (a < 3_600_000) return `${Math.floor(a / 60_000)} min ${Math.round((a % 60_000) / 1000)} s`;
  return `${Math.floor(a / 3_600_000)} h ${Math.round((a % 3_600_000) / 60_000)} min`;
}

export const bytes = (n: number) => (n < 1024 ? `${n} B` : n < 1024 * 1024 ? `${(n / 1024).toFixed(1)} KB` : `${(n / 1024 / 1024).toFixed(1)} MB`);

export function wikiUrl(server: string, title: string) {
  return `https://${server}/wiki/${encodeURIComponent(title.replace(/ /g, "_"))}`;
}

/** "en.wikipedia.org" → "English Wikipedia"-ish short label. */
export function siteLabel(server: string) {
  const m = /^([a-z-]+)\.(wikipedia|wiktionary|wikisource|wikiquote|wikibooks|wikinews|wikivoyage)\.org$/.exec(server);
  if (m) {
    let name = m[1];
    try {
      name = new Intl.DisplayNames(["en"], { type: "language" }).of(m[1]) || m[1];
    } catch {
      /* unknown code */
    }
    return `${name} ${m[2][0].toUpperCase() + m[2].slice(1)}`;
  }
  if (server === "www.wikidata.org") return "Wikidata";
  if (server === "commons.wikimedia.org") return "Commons";
  return server;
}

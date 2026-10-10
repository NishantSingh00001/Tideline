// Time helpers: time-zone offsets for TIMESTAMP_LTZ, Flink-style window
// alignment, interval formatting.

const dtfCache = new Map<string, Intl.DateTimeFormat>();
const offsetCache = new Map<string, number>();

function dtf(tz: string): Intl.DateTimeFormat {
  let f = dtfCache.get(tz);
  if (!f) {
    f = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
    dtfCache.set(tz, f);
  }
  return f;
}

/** Offset of `tz` from UTC at the instant `epochMs`, in milliseconds (IST = +19,800,000). */
export function tzOffset(tz: string, epochMs: number): number {
  if (tz === "UTC" || tz === "Etc/UTC") return 0;
  // Offsets only change on whole quarter-hours; cache per 15 minutes.
  const bucket = Math.floor(epochMs / 900_000);
  const ck = tz + "|" + bucket;
  const hit = offsetCache.get(ck);
  if (hit !== undefined) return hit;
  const probe = bucket * 900_000;
  const p: Record<string, number> = {};
  for (const part of dtf(tz).formatToParts(new Date(probe))) if (part.type !== "literal") p[part.type] = Number(part.value);
  const asUtc = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
  const off = asUtc - probe;
  if (offsetCache.size > 5000) offsetCache.clear();
  offsetCache.set(ck, off);
  return off;
}

/** Flink's TimeWindow.getWindowStartWithOffset. */
export function windowStart(ts: number, offset: number, size: number): number {
  return ts - ((((ts - offset) % size) + size) % size);
}

/**
 * Window start for TIMESTAMP_LTZ attributes. Flink aligns windows to the
 * session time zone's wall clock (an hourly window in IST runs 10:00–11:00
 * local), so the alignment happens on the shifted timestamp.
 */
export function alignedStart(ts: number, offset: number, size: number, tz: string): number {
  const shift = tzOffset(tz, ts);
  return windowStart(ts + shift, offset, size) - shift;
}

const pad = (n: number, w = 2) => String(n).padStart(w, "0");

/** Wall-clock parts of an instant in a time zone. */
export function wallClock(epochMs: number, tz: string) {
  const d = new Date(epochMs + tzOffset(tz, epochMs));
  return {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth() + 1,
    day: d.getUTCDate(),
    hour: d.getUTCHours(),
    minute: d.getUTCMinutes(),
    second: d.getUTCSeconds(),
    ms: d.getUTCMilliseconds(),
    dow: d.getUTCDay(),
  };
}

/** How Flink prints TIMESTAMP_LTZ(3): `2026-10-09 17:04:30.000`. */
export function formatTimestamp(epochMs: number, tz: string): string {
  const w = wallClock(epochMs, tz);
  return `${w.year}-${pad(w.month)}-${pad(w.day)} ${pad(w.hour)}:${pad(w.minute)}:${pad(w.second)}.${pad(w.ms, 3)}`;
}

export function formatClock(epochMs: number, tz: string, withMs = false): string {
  const w = wallClock(epochMs, tz);
  return `${pad(w.hour)}:${pad(w.minute)}:${pad(w.second)}${withMs ? "." + pad(w.ms, 3) : ""}`;
}

/** Java SimpleDateFormat subset used by DATE_FORMAT. */
export function dateFormat(epochMs: number, pattern: string, tz: string): string {
  const w = wallClock(epochMs, tz);
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  return pattern.replace(/'([^']*)'|yyyy|yy|MMM|MM|M|dd|d|HH|H|mm|m|ss|s|SSS|EEE|E/g, (tok, lit) => {
    if (lit !== undefined) return lit;
    switch (tok) {
      case "yyyy": return String(w.year);
      case "yy": return pad(w.year % 100);
      case "MMM": return MONTHS[w.month - 1];
      case "MM": return pad(w.month);
      case "M": return String(w.month);
      case "dd": return pad(w.day);
      case "d": return String(w.day);
      case "HH": return pad(w.hour);
      case "H": return String(w.hour);
      case "mm": return pad(w.minute);
      case "m": return String(w.minute);
      case "ss": return pad(w.second);
      case "s": return String(w.second);
      case "SSS": return pad(w.ms, 3);
      case "EEE":
      case "E": return DAYS[w.dow];
      default: return tok;
    }
  });
}

/** "10 s", "1 min", "1 h 30 min" */
export function formatDuration(ms: number): string {
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000} h`;
  if (ms >= 3_600_000) return `${Math.floor(ms / 3_600_000)} h ${Math.round((ms % 3_600_000) / 60_000)} min`;
  if (ms % 60_000 === 0) return `${ms / 60_000} min`;
  if (ms % 1000 === 0) return `${ms / 1000} s`;
  return `${ms} ms`;
}

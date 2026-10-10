// The recentchange event from Wikimedia EventStreams, and the flat row that
// every query in Tideline reads (the `recentchange` table).
//
// Flattening mirrors what a Kafka Connect Single Message Transform (Flatten +
// ReplaceField) would do before the data lands in a topic: nested `meta`,
// `length` and `revision` objects become plain columns, and a few columns are
// renamed so they aren't SQL keywords (`type`, `user`, `comment`, `namespace`).

export type ChangeType = "edit" | "new" | "log" | "categorize" | "external";

/** Raw event as sent by stream.wikimedia.org (schema /mediawiki/recentchange/1.0.0). */
export interface RecentChangeEvent {
  $schema?: string;
  meta: {
    id: string;
    dt: string;
    stream?: string;
    domain?: string;
    uri?: string;
    request_id?: string;
    topic?: string;
    partition?: number;
    offset?: number;
  };
  id?: number | null;
  type?: string;
  title?: string;
  namespace?: number;
  comment?: string;
  timestamp?: number;
  user?: string;
  bot?: boolean;
  server_url?: string;
  server_name?: string;
  wiki?: string;
  minor?: boolean;
  patrolled?: boolean;
  length?: { old?: number | null; new?: number | null };
  revision?: { old?: number | null; new?: number | null };
  log_type?: string | null;
  log_action?: string;
}

/** One row of the `recentchange` table. */
export interface Change {
  id: string; // meta.id (uuid), the dedup key
  change_type: string;
  wiki: string;
  server_name: string;
  title: string;
  ns: number;
  user_name: string;
  is_bot: boolean;
  is_minor: boolean;
  len_old: number | null;
  len_new: number | null;
  edit_comment: string;
  log_type: string | null;
  rev_id: number | null;
  ts_ms: number; // event time (meta.dt), milliseconds since epoch
  // Where the event sat in Wikimedia's Kafka cluster (not part of the SQL table).
  topic: string;
  partition: number;
  offset: number;
}

export type ValidationResult = { ok: true; change: Change } | { ok: false; reason: string };

const UUID = /^[a-fA-F0-9]{8}(-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}$/;
const intOrNull = (v: unknown) => (typeof v === "number" && Number.isFinite(v) ? Math.trunc(v) : null);

/**
 * Validates an event against the parts of the recentchange schema Tideline
 * relies on, and flattens it. Invalid events go to the dead-letter queue.
 */
export function toChange(raw: unknown): ValidationResult {
  if (!raw || typeof raw !== "object") return { ok: false, reason: "not a JSON object" };
  const e = raw as RecentChangeEvent;
  if (!e.meta || typeof e.meta !== "object") return { ok: false, reason: "missing meta" };
  if (typeof e.meta.id !== "string" || !UUID.test(e.meta.id)) return { ok: false, reason: "meta.id is not a UUID" };
  if (e.meta.stream && e.meta.stream !== "mediawiki.recentchange") return { ok: false, reason: `unexpected stream ${e.meta.stream}` };
  const ts = Date.parse(e.meta.dt);
  if (!Number.isFinite(ts)) return { ok: false, reason: "meta.dt is not a date-time" };
  if (typeof e.wiki !== "string" || !e.wiki) return { ok: false, reason: "missing wiki" };
  if (typeof e.type !== "string") return { ok: false, reason: "missing type" };
  if (typeof e.title !== "string") return { ok: false, reason: "missing title" };
  return {
    ok: true,
    change: {
      id: e.meta.id,
      change_type: e.type,
      wiki: e.wiki,
      server_name: typeof e.server_name === "string" ? e.server_name : e.meta.domain || "",
      title: e.title,
      ns: typeof e.namespace === "number" ? e.namespace : 0,
      user_name: typeof e.user === "string" ? e.user : "",
      is_bot: e.bot === true,
      is_minor: e.minor === true,
      len_old: intOrNull(e.length?.old),
      len_new: intOrNull(e.length?.new),
      edit_comment: typeof e.comment === "string" ? e.comment : "",
      log_type: typeof e.log_type === "string" ? e.log_type : null,
      rev_id: intOrNull(e.revision?.new),
      ts_ms: ts,
      topic: e.meta.topic || "",
      partition: typeof e.meta.partition === "number" ? e.meta.partition : 0,
      offset: typeof e.meta.offset === "number" ? e.meta.offset : -1,
    },
  };
}

/** Where a change can be read on the wiki itself. */
export function changeUrl(c: Pick<Change, "server_name" | "title" | "rev_id">): string {
  const base = `https://${c.server_name}`;
  if (c.rev_id) return `${base}/w/index.php?diff=${c.rev_id}`;
  return `${base}/wiki/${encodeURIComponent(c.title.replace(/ /g, "_"))}`;
}

/** Human names for the wikis that dominate the stream. */
export const WIKI_NAMES: Record<string, string> = {
  wikidatawiki: "Wikidata",
  commonswiki: "Commons",
  enwiki: "English Wikipedia",
  dewiki: "German Wikipedia",
  frwiki: "French Wikipedia",
  eswiki: "Spanish Wikipedia",
  itwiki: "Italian Wikipedia",
  ruwiki: "Russian Wikipedia",
  jawiki: "Japanese Wikipedia",
  zhwiki: "Chinese Wikipedia",
  ptwiki: "Portuguese Wikipedia",
  plwiki: "Polish Wikipedia",
  nlwiki: "Dutch Wikipedia",
  arwiki: "Arabic Wikipedia",
  hiwiki: "Hindi Wikipedia",
  mrwiki: "Marathi Wikipedia",
  bnwiki: "Bengali Wikipedia",
  tawiki: "Tamil Wikipedia",
  fawiki: "Persian Wikipedia",
  ukwiki: "Ukrainian Wikipedia",
  enwiktionary: "English Wiktionary",
  metawiki: "Meta-Wiki",
  mediawikiwiki: "MediaWiki.org",
};

export const wikiName = (wiki: string) => WIKI_NAMES[wiki] || wiki;

/**
 * Comments that MediaWiki and common tools write when a change is undone.
 * Used by the edit-war detector (and the MATCH_RECOGNIZE job in pipeline/).
 */
export const REVERT_PATTERN =
  "(?iu)(undid revision|undo revision|reverted edits? by|reverted to revision|restored revision|^rvv?\\b|^revert|rückgängig|annulation|revertid|annullat|отмена правки|отменены правки|取り消し|ロールバック)";

/** The same pattern for JavaScript (no inline (?i) flag). */
export const REVERT_RE = new RegExp(REVERT_PATTERN.replace("(?iu)", ""), "i");

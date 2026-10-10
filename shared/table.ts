// The `recentchange` table: columns, the matching Flink DDL, and the
// conversion from a Change to a row.

import type { Change } from "./change.ts";
import type { Column, Row } from "./engine/types.ts";

export const TABLE_NAME = "recentchange";

export const COLUMNS: Column[] = [
  { name: "id", type: "STRING", nullable: false },
  { name: "change_type", type: "STRING", nullable: false },
  { name: "wiki", type: "STRING", nullable: false },
  { name: "server_name", type: "STRING", nullable: false },
  { name: "title", type: "STRING", nullable: false },
  { name: "ns", type: "INT", nullable: false },
  { name: "user_name", type: "STRING", nullable: false },
  { name: "is_bot", type: "BOOLEAN", nullable: false },
  { name: "is_minor", type: "BOOLEAN", nullable: false },
  { name: "len_old", type: "INT", nullable: true },
  { name: "len_new", type: "INT", nullable: true },
  { name: "edit_comment", type: "STRING", nullable: false },
  { name: "log_type", type: "STRING", nullable: true },
  { name: "rev_id", type: "BIGINT", nullable: true },
  { name: "ts_ms", type: "BIGINT", nullable: false },
  // Computed columns
  { name: "delta", type: "INT", nullable: true },
  { name: "event_time", type: "TIMESTAMP_LTZ", nullable: false, rowtime: true },
];

export const COL = Object.fromEntries(COLUMNS.map((c, i) => [c.name, i])) as Record<string, number>;

export const COLUMN_DOCS: Record<string, string> = {
  id: "Event id (meta.id, a UUID). Unique per event.",
  change_type: "edit, new (page created), log (deletions, blocks, uploads…), categorize",
  wiki: "Database name, e.g. enwiki, wikidatawiki, commonswiki, hiwiki",
  server_name: "Host, e.g. en.wikipedia.org",
  title: "Page title, with namespace prefix (\"Talk:Mumbai\")",
  ns: "Namespace: 0 = articles, 1 = talk, 2 = user, 6 = files, 14 = categories",
  user_name: "Username or IP address",
  is_bot: "Made by an account flagged as a bot",
  is_minor: "Marked as a minor edit",
  len_old: "Page size in bytes before the change (NULL for new pages)",
  len_new: "Page size in bytes after the change",
  edit_comment: "Edit summary",
  log_type: "For log events: delete, block, upload, move, …",
  rev_id: "New revision id",
  ts_ms: "Event time in milliseconds since epoch",
  delta: "len_new - len_old (computed column)",
  event_time: "TO_TIMESTAMP_LTZ(ts_ms, 3), the event-time attribute with the watermark",
};

export const changeToRow = (c: Change): Row => [
  c.id,
  c.change_type,
  c.wiki,
  c.server_name,
  c.title,
  c.ns,
  c.user_name,
  c.is_bot,
  c.is_minor,
  c.len_old,
  c.len_new,
  c.edit_comment,
  c.log_type,
  c.rev_id,
  c.ts_ms,
  c.len_new === null || c.len_old === null ? null : c.len_new - c.len_old,
  c.ts_ms,
];

/** The Flink DDL for this table. `connector` options are filled in by the caller. */
export function flinkDdl(watermarkDelaySeconds: number, withClause: string): string {
  return `CREATE TABLE recentchange (
  id            STRING,
  change_type   STRING,
  wiki          STRING,
  server_name   STRING,
  title         STRING,
  ns            INT,
  user_name     STRING,
  is_bot        BOOLEAN,
  is_minor      BOOLEAN,
  len_old       INT,
  len_new       INT,
  edit_comment  STRING,
  log_type      STRING,
  rev_id        BIGINT,
  ts_ms         BIGINT,
  delta AS len_new - len_old,
  event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '${watermarkDelaySeconds}' SECOND
) WITH (
${withClause}
)`;
}

// The Flink SQL behind each live panel, plus the example gallery. Every query
// here is also run on real Apache Flink by tests/flink/run_flink.py and the
// results compared row for row (tests/parity.test.ts).

export interface NamedQuery {
  id: string;
  title: string;
  about: string;
  sql: string;
  /** Concepts this query shows, for the gallery. */
  tags: string[];
}

const PROJECT = `CASE
    WHEN wiki = 'wikidatawiki' THEN 'Wikidata'
    WHEN wiki = 'commonswiki' THEN 'Commons'
    WHEN wiki = 'enwiki' THEN 'English Wikipedia'
    WHEN server_name LIKE '%.wikipedia.org' THEN 'Other Wikipedias'
    ELSE 'Other projects'
  END`;

/** Queries that power the Live page. */
export const PANELS = {
  pulse: {
    id: "pulse",
    title: "Changes per 10 seconds, by project",
    about: "A tumbling event-time window: each 10-second bucket is emitted once the watermark passes its end.",
    tags: ["TUMBLE", "CASE"],
    sql: `SELECT window_start, window_end,
  ${PROJECT} AS project,
  COUNT(*) AS changes
FROM TABLE(
  TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end,
  ${PROJECT}`,
  },
  editors: {
    id: "editors",
    title: "People and bots",
    about: "Aggregate FILTER clauses split one window into human and bot activity.",
    tags: ["TUMBLE", "FILTER"],
    sql: `SELECT window_start, window_end,
  COUNT(*) FILTER (WHERE NOT is_bot) AS by_people,
  COUNT(*) FILTER (WHERE is_bot) AS by_bots,
  COUNT(DISTINCT user_name) AS accounts
FROM TABLE(
  TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end`,
  },
  trending: {
    id: "trending",
    title: "Trending articles",
    about: "Window Top-N: the 10 most-edited Wikipedia articles over a sliding 10-minute window that advances every minute.",
    tags: ["HOP", "Top-N", "COUNT DISTINCT"],
    sql: `SELECT *
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end
      ORDER BY edits DESC, editors DESC, title, server_name) AS rownum
  FROM (
    SELECT window_start, window_end, server_name, title,
      COUNT(*) AS edits,
      COUNT(DISTINCT user_name) AS editors,
      SUM(delta) AS net_bytes
    FROM TABLE(
      HOP(TABLE recentchange, DESCRIPTOR(event_time),
          INTERVAL '1' MINUTE, INTERVAL '10' MINUTES))
    WHERE ns = 0 AND NOT is_bot
      AND change_type IN ('edit', 'new')
      AND server_name LIKE '%.wikipedia.org'
    GROUP BY window_start, window_end, server_name, title
  )
)
WHERE rownum <= 10`,
  },
  newPages: {
    id: "newPages",
    title: "New articles",
    about: "A stateless filter: rows flow straight through as they arrive.",
    tags: ["filter"],
    sql: `SELECT event_time, server_name, title, user_name, len_new
FROM recentchange
WHERE change_type = 'new' AND ns = 0 AND NOT is_bot`,
  },
  bigChanges: {
    id: "bigChanges",
    title: "Big changes",
    about: "Edits that add or remove 10 KB or more in one go.",
    tags: ["filter", "ABS"],
    sql: `SELECT event_time, server_name, title, user_name, delta
FROM recentchange
WHERE change_type = 'edit' AND ABS(delta) >= 10000`,
  },
} satisfies Record<string, NamedQuery>;

/** The example gallery on the SQL page. */
export const EXAMPLES: NamedQuery[] = [
  PANELS.pulse,
  PANELS.trending,
  {
    id: "sessions",
    title: "Editing sessions",
    about: "SESSION windows group each person's edits until they pause for 5 minutes, so sessions have different lengths.",
    tags: ["SESSION", "PARTITION BY"],
    sql: `SELECT window_start, window_end, user_name,
  COUNT(*) AS changes,
  COUNT(DISTINCT title) AS pages,
  SUM(ABS(delta)) AS bytes_touched
FROM TABLE(
  SESSION(TABLE recentchange PARTITION BY user_name,
          DESCRIPTOR(event_time), INTERVAL '5' MINUTES))
WHERE NOT is_bot
GROUP BY window_start, window_end, user_name
HAVING COUNT(*) >= 3`,
  },
  {
    id: "cumulate",
    title: "Running total per hour",
    about: "CUMULATE windows share a start and grow every minute, like a dashboard counter that resets each hour.",
    tags: ["CUMULATE"],
    sql: `SELECT window_start, window_end,
  COUNT(*) AS changes,
  COUNT(*) FILTER (WHERE change_type = 'new') AS new_pages,
  SUM(delta) AS net_bytes
FROM TABLE(
  CUMULATE(TABLE recentchange, DESCRIPTOR(event_time),
           INTERVAL '1' MINUTE, INTERVAL '1' HOUR))
GROUP BY window_start, window_end`,
  },
  {
    id: "changelog",
    title: "Running totals per wiki (changelog)",
    about: "A GROUP BY without a window never closes, so Flink emits updates: +I for a new wiki, then -U/+U pairs as counts change.",
    tags: ["changelog", "GROUP BY"],
    sql: `SELECT wiki,
  COUNT(*) AS changes,
  COUNT(*) FILTER (WHERE is_bot) AS bot_changes,
  MAX(event_time) AS last_change
FROM recentchange
GROUP BY wiki
HAVING COUNT(*) >= 20`,
  },
  {
    id: "biggestPerWindow",
    title: "Biggest change every 5 minutes",
    about: "Top-N directly on a window table function, without aggregating first.",
    tags: ["TUMBLE", "Top-N"],
    sql: `SELECT *
FROM (
  SELECT window_start, window_end, server_name, title, user_name, delta,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end
      ORDER BY delta DESC, id) AS rownum
  FROM TABLE(
    TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '5' MINUTES))
  WHERE change_type = 'edit' AND delta IS NOT NULL
)
WHERE rownum <= 3`,
  },
  {
    id: "indic",
    title: "Indian-language Wikipedias",
    about: "Activity on Hindi, Marathi, Bengali and Tamil Wikipedia, per minute.",
    tags: ["TUMBLE", "IN"],
    sql: `SELECT window_start, wiki,
  COUNT(*) AS changes,
  COUNT(DISTINCT user_name) AS editors
FROM TABLE(
  TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE wiki IN ('hiwiki', 'mrwiki', 'bnwiki', 'tawiki')
GROUP BY window_start, window_end, wiki`,
  },
  {
    id: "late",
    title: "Spot late events",
    about: "CURRENT_WATERMARK shows which rows arrive behind the watermark. Window queries drop these rows.",
    tags: ["watermark", "late data"],
    sql: `SELECT event_time,
  CURRENT_WATERMARK(event_time) AS wm,
  change_type, server_name, title
FROM recentchange
WHERE event_time <= CURRENT_WATERMARK(event_time)`,
  },
  PANELS.bigChanges,
];

/** Extra queries used only to check parity with Flink (they cover the corners). */
export const PARITY_EXTRA: NamedQuery[] = [
  {
    id: "tumbleOffset",
    title: "Tumble with offset",
    about: "",
    tags: [],
    sql: `SELECT window_start, window_end, change_type, COUNT(*) AS n, MIN(delta) AS min_delta, MAX(delta) AS max_delta, AVG(delta) AS avg_delta
FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '15' SECOND))
GROUP BY window_start, window_end, change_type`,
  },
  {
    id: "tvfRows",
    title: "Window TVF rows",
    about: "",
    tags: [],
    sql: `SELECT id, window_start, window_end, change_type
FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '30' SECOND))
WHERE change_type IN ('new', 'categorize')`,
  },
  {
    id: "expressions",
    title: "Expressions",
    about: "",
    tags: [],
    sql: `SELECT id,
  LOWER(wiki) AS w, UPPER(SUBSTRING(title FROM 1 FOR 3)) AS t3, CHAR_LENGTH(title) AS len,
  delta / 7 AS idiv, delta % 7 AS imod, CAST(delta AS DOUBLE) / 4 AS ddiv, ROUND(CAST(delta AS DOUBLE) / 3, 2) AS r2,
  COALESCE(len_old, -1) AS lo, NULLIF(ns, 0) AS nz, IF(is_bot, 'bot', 'person') AS who,
  CASE WHEN delta > 0 THEN 'grew' WHEN delta < 0 THEN 'shrank' ELSE 'same' END AS dir,
  REGEXP(edit_comment, '(?i)revert|undid') AS rv, SPLIT_INDEX(server_name, '.', 0) AS lang,
  CONCAT(wiki, ':', CAST(ns AS STRING)) AS wk, DATE_FORMAT(event_time, 'HH:mm:ss') AS hms,
  title LIKE 'Q%' AS q, ns BETWEEN 1 AND 6 AS ns16, EXTRACT(MINUTE FROM event_time) AS mi,
  REPLACE(change_type, 'e', 'E') AS ct, POSITION('.' IN server_name) AS dot_at, ABS(delta) AS ad,
  CAST(is_minor AS STRING) AS ms, TRIM(BOTH ' ' FROM '  x  ') AS tr, rev_id % 1000 AS rv3
FROM recentchange
WHERE MOD(CAST(ts_ms / 1000 AS INT), 7) = 0`,
  },
  {
    id: "distinctPairs",
    title: "Distinct",
    about: "",
    tags: [],
    sql: `SELECT DISTINCT wiki, change_type FROM recentchange WHERE wiki LIKE '%wiki'`,
  },
  {
    id: "globalAgg",
    title: "Global aggregate",
    about: "",
    tags: [],
    sql: `SELECT COUNT(*) AS n, COUNT(DISTINCT wiki) AS wikis, SUM(CASE WHEN is_bot THEN 1 ELSE 0 END) AS bots, MAX(len_new) AS biggest, MIN(title) AS first_title
FROM recentchange`,
  },
  {
    id: "having",
    title: "Having",
    about: "",
    tags: [],
    sql: `SELECT window_start, window_end, wiki, COUNT(*) AS n, SUM(delta) AS net
FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, wiki
HAVING COUNT(*) > 100 AND SUM(delta) IS NOT NULL`,
  },
];

CREATE TABLE recentchange (
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
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = '__DATA__',
  'format' = 'json'
)

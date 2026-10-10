#!/usr/bin/env python3
"""Runs every parity query on real Apache Flink (PyFlink, local mini-cluster)
over tests/flink/data/events.jsonl and writes expected.json.

    npm run parity:data        # writes the dataset and queries.json
    python3 tests/flink/run_flink.py

Settings that make Flink's output deterministic so it can be compared row for
row: parallelism 1, session time zone UTC, and pipeline.auto-watermark-interval
= 0 so the watermark advances after every record.
"""
import collections, decimal, json, os, sys, time
from pyflink.table import EnvironmentSettings, TableEnvironment
import pyflink

HERE = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(HERE, "data")
spec = json.load(open(os.path.join(HERE, "queries.json")))

env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
cfg = env.get_config()
cfg.set("parallelism.default", "1")
cfg.set("table.local-time-zone", "UTC")
cfg.set("pipeline.auto-watermark-interval", "0 ms")
env.execute_sql(open(os.path.join(HERE, "ddl.sql")).read().replace("__DATA__", data_dir))

def plain(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v

def run(qid, sql):
    env.execute_sql(f"CREATE TEMPORARY VIEW v_{qid} AS {sql}")
    cols = []
    with env.execute_sql(f"DESCRIBE v_{qid}").collect() as it:
        for r in it:
            cols.append((r[0], r[1]))
    sel = ", ".join(
        f"CAST(`{n}` AS STRING) AS `{n}`" if t.upper().startswith("TIMESTAMP") else f"`{n}`" for n, t in cols
    )
    view = collections.Counter()
    messages = collections.Counter()
    t0 = time.time()
    with env.execute_sql(f"SELECT {sel} FROM v_{qid}").collect() as it:
        for r in it:
            kind = str(r.get_row_kind())
            values = tuple(plain(v) for v in r)
            messages[kind] += 1
            if kind in ("RowKind.INSERT", "RowKind.UPDATE_AFTER", "+I", "+U"):
                view[values] += 1
            else:
                view[values] -= 1
                if view[values] <= 0:
                    del view[values]
    rows = sorted([list(v) for v, c in view.items() for _ in range(c)], key=lambda x: json.dumps(x))
    print(f"  {qid}: {len(rows)} rows, {sum(messages.values())} messages ({time.time() - t0:.1f} s)", flush=True)
    return {"columns": [n for n, _ in cols], "types": [t for _, t in cols], "rows": rows, "messages": sum(messages.values())}

only = set(sys.argv[1:])
path = os.environ.get("OUT") or os.path.join(HERE, "expected.json")
out = json.load(open(path)) if only and os.path.exists(path) else {"queries": {}}
from importlib.metadata import version
out["flink"] = version("apache-flink")
out["events"] = sum(1 for _ in open(os.path.join(data_dir, "events.jsonl")))
print("Running parity queries on Apache Flink", out["flink"])
for q in spec["queries"]:
    if only and q["id"] not in only:
        continue
    out["queries"][q["id"]] = run(q["id"], q["sql"])

# Edit wars with MATCH_RECOGNIZE (complex event processing)
P = spec["revertPattern"].replace("'", "''")
mr = f"""
SELECT wiki, title, first_revert, third_revert
FROM recentchange
MATCH_RECOGNIZE (
  PARTITION BY wiki, title
  ORDER BY event_time
  MEASURES
    R1.event_time AS first_revert,
    R3.event_time AS third_revert
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (R1 O1*? R2 O2*? R3) WITHIN INTERVAL '15' MINUTE
  DEFINE
    R1 AS REGEXP(R1.edit_comment, '{P}'),
    R2 AS REGEXP(R2.edit_comment, '{P}'),
    R3 AS REGEXP(R3.edit_comment, '{P}') AND (R2.user_name <> R1.user_name OR R3.user_name <> R1.user_name),
    O1 AS NOT REGEXP(O1.edit_comment, '{P}'),
    O2 AS NOT REGEXP(O2.edit_comment, '{P}')
)"""
if not only or "editWars" in only:
    out["queries"]["editWars"] = run("editWars", mr)
out["editWarsSql"] = mr.strip()

json.dump(out, open(path, "w"), indent=1, ensure_ascii=False)
print("wrote expected.json")

#!/usr/bin/env python3
"""Checks the Flink job scripts with Flink's own planner, without Kafka.

Swaps the Kafka connectors for local ones (the change table reads the test
dataset from tests/flink/data, results go to blackhole sinks), then plans the
whole statement set with EXPLAIN and runs it to the end of the data.

    pip install apache-flink==1.20.3      # or 2.x
    python3 pipeline/flink/check_sql.py
"""
import os, re, sys, time
from pyflink.table import EnvironmentSettings, TableEnvironment
from importlib.metadata import version

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "..", "tests", "flink", "data")
if not os.path.exists(os.path.join(DATA, "events.jsonl")):
    sys.exit("Run `npm test` first: it generates tests/flink/data/events.jsonl")

def statements(path):
    text = re.sub(r"--[^\n]*", "", open(path).read())
    return [s.strip() for s in re.split(r";\s*\n", text) if s.strip()]

env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
cfg = env.get_config()
cfg.set("parallelism.default", "1")
for stmt in statements(os.path.join(HERE, "sql", "00_tables.sql")):
    m = re.match(r"SET\s+'([^']+)'\s*=\s*'([^']+)'", stmt)
    if m:
        cfg.set(m.group(1), m.group(2))
        continue
    name = re.search(r"CREATE TABLE (\w+)", stmt).group(1)
    local = f"'connector' = 'filesystem', 'path' = '{DATA}', 'format' = 'json'" if name == "recentchange" else "'connector' = 'blackhole'"
    env.execute_sql(re.sub(r"WITH \((.|\n)*\)\s*$", f"WITH ({local})", stmt))
    print(f"created {name}")

jobs = open(os.path.join(HERE, "sql", "10_jobs.sql")).read()
body = re.search(r"EXECUTE STATEMENT SET\s+BEGIN(.*)END;", jobs, re.S).group(1)
inserts = [s.strip() for s in re.split(r";\s*\n", re.sub(r"--[^\n]*", "", body)) if s.strip()]
print(f"{len(inserts)} INSERT statements")
plan = env.execute_sql("EXPLAIN STATEMENT SET BEGIN " + ";\n".join(inserts) + "; END").collect()
with plan as it:
    text = "\n".join(str(r[0]) for r in it)
ops = sorted(set(re.findall(r"^\s*(?:\+- )?([A-Z][A-Za-z]+)\(", text, re.M)))
print("planned operators:", ", ".join(ops))
ss = env.create_statement_set()
for s in inserts:
    ss.add_insert_sql(s)
t0 = time.time()
ss.execute().wait()
print(f"ran all jobs to the end of the data on Flink {version('apache-flink')} in {time.time() - t0:.1f} s")

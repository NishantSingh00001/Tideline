#!/usr/bin/env python3
"""End-to-end API checks against a local dev server with the mock Gemini.

    python3 tests/mock_gemini.py &
    cp tests/e2e.env .env && netlify dev
    python3 tests/api_test.py
"""
import json, sys, time, urllib.request

B = "http://localhost:8888"
FAILED = []

def ok(cond, msg):
    print(("PASS " if cond else "FAIL ") + msg)
    if not cond:
        FAILED.append(msg)

def req(method, path, body=None, raw=None, headers=None):
    data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
    h = {"content-type": "application/json"} if data else {}
    h.update(headers or {})
    r = urllib.request.Request(B + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=60) as res:
            t = res.read()
            return res.status, json.loads(t) if t else None
    except urllib.error.HTTPError as e:
        t = e.read()
        try:
            return e.code, json.loads(t)
        except Exception:
            return e.code, t

urllib.request.urlopen(urllib.request.Request("http://127.0.0.1:9996/reset", data=b"{}", method="POST"))

s, h = req("GET", "/api/health")
ok(s == 200 and h["ok"] and h["ai"], f"health: ok, AI on ({h})")

# ---- shared queries
SQL = "SELECT window_start, COUNT(*) AS changes FROM TABLE(TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end"
s, r = req("POST", "/api/queries", {"sql": SQL, "title": "Per minute"}); qid = (r or {}).get("id", "")
ok(s == 201 and len(qid) == 8, f"query saved: /q/{qid}")
s, g = req("GET", f"/api/queries/{qid}")
ok(s == 200 and g["sql"] == SQL and g["title"] == "Per minute" and g["views"] == 1, "shared query loads and counts a view")
s, g = req("GET", f"/api/queries/{qid}"); ok(g["views"] == 2, "second view counted")
s, r = req("POST", "/api/queries", {"sql": "SELECT nope FROM recentchange"}); ok(s == 422 and "Column 'nope' not found" in r["error"], f"SQL that doesn't compile isn't saved → {s}: {r.get('error','')[:80]}")
s, r = req("POST", "/api/queries", {"sql": ""}); ok(s == 400, f"empty query → {s}")
s, r = req("POST", "/api/queries", {"sql": "SELECT 1 FROM recentchange -- " + "x" * 9000}); ok(s == 400, f"oversized query → {s}")
s, r = req("GET", "/api/queries/zzzzzzzz"); ok(s == 410, f"missing query → {s}")
s, r = req("GET", "/api/queries/bad!"); ok(s == 400, f"malformed id → {s}")
s, r = req("PUT", "/api/queries"); ok(s == 405, f"PUT → {s}")

# ---- checkpoints
cp = {"position": 1791547200000, "snapshot": {"v": 1, "delay": 5000, "maxTs": 1, "wm": 0, "stats": {}, "dedup": [["a", 1]], "jobs": {"rec": {"ops": []}}}, "committed": [[1, 2]]}
s, r = req("POST", "/api/checkpoints", cp); cid = (r or {}).get("id", "")
ok(s == 201 and cid.startswith("cp") and r["bytes"] > 50, f"checkpoint stored: {cid} ({r.get('bytes')} bytes)")
s, g = req("GET", f"/api/checkpoints/{cid}")
ok(s == 200 and g == cp, "checkpoint reads back byte for byte")
s, r = req("POST", "/api/checkpoints", {"hello": "world"}); ok(s == 400, f"not a checkpoint → {s}")
s, r = req("POST", "/api/checkpoints", raw=b"{nope"); ok(s == 400, f"broken JSON → {s}")
s, r = req("POST", "/api/checkpoints", raw=b'{"x":"' + b"a" * 310000 + b'"}'); ok(s == 413, f"oversized checkpoint → {s}")
s, r = req("GET", "/api/checkpoints/cpzzzzzzzz222222"); ok(s == 410, f"missing checkpoint → {s}")
s, r = req("GET", "/api/checkpoints/cp..%2F..%2Fetc"); ok(s == 400, f"bad checkpoint id → {s}")

# ---- English → SQL
s, r = req("POST", "/api/ai/sql", {"question": "which wikis get the most new articles per minute?"})
ok(s == 200 and "TUMBLE" in r["sql"] and r["attempts"] == 1 and r["explanation"], f"AI writes SQL that compiles (attempts {r.get('attempts')})")
s, r = req("POST", "/api/ai/sql", {"question": "broken: count per wiki"})
ok(s == 200 and r["attempts"] == 2 and "count" not in r["sql"].split("AS")[-1], f"a draft with a reserved-word alias is rejected and fixed (attempts {r.get('attempts')})")
calls = json.loads(urllib.request.urlopen("http://127.0.0.1:9996/calls").read())
ok(any("rejected by the Flink SQL parser" in c["prompt"] and "Encountered" in c["prompt"] for c in calls), "the parser's error is sent back to the model")
ok(all(c["system_has_schema"] for c in calls), "the system prompt carries the table schema")
s, r = req("POST", "/api/ai/sql", {"question": "what's the weather in Pune"}); ok(s == 422 and "weather" in r["error"], f"unanswerable question → {s}: {r.get('error')}")
s, r = req("POST", "/api/ai/sql", {"question": "always wrong please"}); ok(s == 422 and "couldn't write a query" in r["error"], f"three bad drafts → {s}")
s, r = req("POST", "/api/ai/sql", {"question": "x"}); ok(s == 400, f"too-short question → {s}")
codes = []
for i in range(12):
    s, r = req("POST", "/api/ai/sql", {"question": f"new articles per minute {i}"})
    codes.append(s)
ok(429 in codes, f"per-visitor / daily AI limits kick in → {codes}")

print()
print(f"{len(FAILED)} failed" if FAILED else "All API checks passed.")
sys.exit(1 if FAILED else 0)

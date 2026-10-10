#!/usr/bin/env python3
"""A stand-in for the Gemini API, for local tests of English → SQL.

    python3 tests/mock_gemini.py      # listens on 127.0.0.1:9996
    # .env: GEMINI_API_KEY=mock  GEMINI_ENDPOINT=http://127.0.0.1:9996/gemini/v1beta/models

Questions containing "broken" get a query with a mistake first (to test the
retry with the parser's error); "weather" gets an empty answer (can't be
answered from the table).
"""
import http.server, json, re

CALLS = []
GOOD = """SELECT window_start, window_end, wiki,
  COUNT(*) AS new_articles
FROM TABLE(
  TUMBLE(TABLE recentchange, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE change_type = 'new' AND ns = 0
GROUP BY window_start, window_end, wiki"""
BAD = "SELECT wiki, COUNT(*) AS count FROM recentchange GROUP BY wiki"


class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/calls":
            return self.send(200, CALLS)
        self.send(404, {})

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length", 0)))
        if self.path == "/reset":
            CALLS.clear()
            return self.send(200, {"ok": True})
        m = re.match(r"^/gemini/v1beta/models/([^:]+):generateContent$", self.path)
        if not m:
            return self.send(404, {"error": {"message": "no mock"}})
        body = json.loads(raw)
        prompt = body["contents"][0]["parts"][-1]["text"]
        system = body["systemInstruction"]["parts"][0]["text"]
        CALLS.append({"model": m.group(1), "prompt": prompt[:600], "system_has_schema": "recentchange" in system and "event_time" in system})
        if "weather" in prompt:
            out = {"sql": "", "explanation": "The edit stream has no weather data."}
        elif "broken" in prompt and "rejected" not in prompt:
            out = {"sql": BAD, "explanation": "Changes per wiki."}
        elif "always wrong" in prompt:
            out = {"sql": "SELEC nonsense", "explanation": "Oops."}
        else:
            out = {"sql": GOOD, "explanation": "New articles per wiki, per minute."}
        self.send(200, {"candidates": [{"content": {"parts": [{"text": json.dumps(out)}]}, "finishReason": "STOP"}]})


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("127.0.0.1", 9996), H).serve_forever()

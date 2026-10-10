// A local stand-in for https://stream.wikimedia.org/v2/stream/recentchange,
// speaking the same wire format as Wikimedia's KafkaSSE service:
//
//   :ok
//
//   event: message
//   id: [{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1791547200001},...]
//   data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{...},...}
//
// Supports ?since=<ms or ISO date> for historical replay (served as fast as the
// client reads, like Kafka catching up) and then continues live in real time.
//
//   node --experimental-transform-types dev/mock-eventstreams.ts [port]

import { createServer } from "node:http";
import { eventsForSecond } from "./generator.ts";

const port = Number(process.argv[2] || process.env.MOCK_PORT || 8787);
// MOCK_DROP_AFTER_S closes each connection after that many seconds, like the
// real service does from time to time (clients must reconnect with since=).
const dropAfter = Number(process.env.MOCK_DROP_AFTER_S || 0);

const server = createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://localhost:${port}`);
  const cors = { "access-control-allow-origin": "*", "access-control-allow-headers": "last-event-id" };
  if (req.method === "OPTIONS") {
    res.writeHead(204, cors).end();
    return;
  }
  if (url.pathname !== "/v2/stream/recentchange") {
    res.writeHead(404, { "content-type": "application/json", ...cors }).end(JSON.stringify({ status: 404, type: "not_found", title: "Stream Not Found" }));
    return;
  }
  let since = Date.now();
  const raw = url.searchParams.get("since");
  if (raw) {
    since = /^\d+$/.test(raw) ? Number(raw) : Date.parse(raw);
    if (!Number.isFinite(since)) {
      res.writeHead(400, { "content-type": "application/json", ...cors }).end(JSON.stringify({ status: 400, type: "invalid_timestamp", title: "Invalid timestamp" }));
      return;
    }
  }
  const lastId = req.headers["last-event-id"];
  if (typeof lastId === "string") {
    try {
      const ids = JSON.parse(lastId) as { timestamp?: number }[];
      const ts = Math.min(...ids.map((i) => i.timestamp ?? Infinity));
      if (Number.isFinite(ts)) since = ts;
    } catch {
      /* ignore a malformed header */
    }
  }
  res.writeHead(200, { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache", connection: "keep-alive", ...cors });
  res.write(":ok\n\n");
  let closed = false;
  req.on("close", () => (closed = true));
  const opened = Date.now();
  const write = (s: string) => new Promise<void>((resolve) => (res.write(s) ? resolve() : res.once("drain", () => resolve())));
  let sec = Math.floor(since / 1000);
  while (!closed) {
    if (dropAfter && Date.now() - opened > dropAfter * 1000) break;
    const batch = eventsForSecond(sec).filter((g) => g.kafkaTs >= since);
    const frame = (g: (typeof batch)[number]) => {
      const id = JSON.stringify([
        { topic: "eqiad.mediawiki.recentchange", partition: 0, timestamp: g.kafkaTs + 1 },
        { topic: "codfw.mediawiki.recentchange", partition: 0, offset: -1 },
      ]);
      return `event: message\nid: ${id}\ndata: ${JSON.stringify(g.event)}\n\n`;
    };
    if (sec * 1000 + 1000 <= Date.now()) {
      // History: as fast as the client reads.
      const chunk = batch.map(frame).join("");
      if (chunk) await write(chunk);
    } else {
      // Live: each event at its own time.
      for (const g of batch) {
        const wait = g.kafkaTs - Date.now();
        if (wait > 0) await new Promise((r) => setTimeout(r, wait));
        if (closed) break;
        await write(frame(g));
      }
      const rest = sec * 1000 + 1000 - Date.now();
      if (rest > 0) await new Promise((r) => setTimeout(r, rest));
    }
    sec++;
    if (sec % 50 === 0) await new Promise((r) => setImmediate(r));
  }
  res.end();
});

server.listen(port, () => console.log(`Mock EventStreams on http://localhost:${port}/v2/stream/recentchange`));

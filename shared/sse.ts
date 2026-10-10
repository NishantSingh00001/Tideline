// Server-Sent Events over fetch(): a small, spec-following parser
// (https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation).
// Using fetch instead of EventSource lets the client choose where to resume
// (?since=) and cancel cleanly, which checkpoint recovery needs.

export interface SseMessage {
  event: string;
  data: string;
  id: string;
}

export class SseParser {
  private buf = "";
  private data: string[] = [];
  private event = "";
  private id = "";
  /** Comment lines seen (":ok" heartbeats). */
  comments = 0;

  /** Feeds a chunk of text; returns the complete messages it finished. */
  push(chunk: string): SseMessage[] {
    this.buf += chunk;
    const out: SseMessage[] = [];
    let nl: number;
    while ((nl = this.buf.search(/\r\n|\r|\n/)) >= 0) {
      const line = this.buf.slice(0, nl);
      const sepLen = this.buf[nl] === "\r" && this.buf[nl + 1] === "\n" ? 2 : 1;
      // A lone \r at the very end might be the first half of \r\n: wait for more.
      if (this.buf[nl] === "\r" && nl + 1 === this.buf.length) break;
      this.buf = this.buf.slice(nl + sepLen);
      if (line === "") {
        if (this.data.length) out.push({ event: this.event || "message", data: this.data.join("\n"), id: this.id });
        this.data = [];
        this.event = "";
        continue;
      }
      if (line[0] === ":") {
        this.comments++;
        continue;
      }
      const colon = line.indexOf(":");
      const field = colon < 0 ? line : line.slice(0, colon);
      let value = colon < 0 ? "" : line.slice(colon + 1);
      if (value[0] === " ") value = value.slice(1);
      if (field === "data") this.data.push(value);
      else if (field === "event") this.event = value;
      else if (field === "id" && !value.includes("\u0000")) this.id = value;
    }
    return out;
  }
}

/** The Kafka position in an EventStreams id: the latest timestamp across topics. */
export function positionFromId(id: string): { timestamp: number | null; offsets: { topic: string; partition: number; timestamp?: number; offset?: number }[] } {
  try {
    const parts = JSON.parse(id) as { topic: string; partition: number; timestamp?: number; offset?: number }[];
    const ts = parts.map((p) => p.timestamp).filter((t): t is number => typeof t === "number" && t > 0);
    return { timestamp: ts.length ? Math.max(...ts) : null, offsets: parts };
  } catch {
    return { timestamp: null, offsets: [] };
  }
}

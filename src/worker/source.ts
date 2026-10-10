// Reads Wikimedia EventStreams with fetch(), resuming with ?since= after a
// disconnect so no event is lost (duplicates from the overlap are removed by
// the runtime's dedup on meta.id).

import { SseParser, positionFromId } from "../../shared/sse.ts";

export type SourceState = "connecting" | "open" | "retrying" | "stopped";

export interface SourceStatus {
  state: SourceState;
  message: string;
  reconnects: number;
  /** Kafka timestamp of the last event read (from the SSE id). */
  position: number | null;
  /** Where the current connection started reading. */
  startedFrom: number | null;
  openedAt: number | null;
  bytes: number;
}

const OVERLAP_MS = 2000;

export class StreamSource {
  status: SourceStatus = { state: "stopped", message: "", reconnects: 0, position: null, startedFrom: null, openedAt: null, bytes: 0 };
  private ctrl: AbortController | null = null;
  private generation = 0;

  constructor(
    private url: string,
    private onMessage: (data: string, kafkaTs: number | null) => void,
    private onStatus: (s: SourceStatus) => void = () => {},
  ) {}

  /** Starts reading. `since` (ms) replays history from Kafka; omitted = from now. */
  start(since?: number | null) {
    this.stop();
    this.status = { ...this.status, reconnects: 0, position: null, bytes: 0 };
    const gen = ++this.generation;
    void this.loop(gen, since ?? null);
  }

  /** Continues from where the last event left off (after a pause). */
  resume() {
    const from = this.status.position !== null ? this.status.position - OVERLAP_MS : null;
    this.stop();
    const gen = ++this.generation;
    void this.loop(gen, from);
  }

  stop() {
    this.generation++;
    this.ctrl?.abort();
    this.ctrl = null;
    this.set({ state: "stopped", message: "" });
  }

  private set(p: Partial<SourceStatus>) {
    this.status = { ...this.status, ...p };
    this.onStatus(this.status);
  }

  private async loop(gen: number, since: number | null) {
    let delay = 1000;
    let from = since;
    while (gen === this.generation) {
      const ctrl = new AbortController();
      this.ctrl = ctrl;
      const url = from !== null ? `${this.url}${this.url.includes("?") ? "&" : "?"}since=${Math.floor(from)}` : this.url;
      this.set({ state: "connecting", message: from !== null ? "Replaying from Kafka" : "Connecting", startedFrom: from });
      try {
        const res = await fetch(url, { signal: ctrl.signal, headers: { accept: "text/event-stream" }, cache: "no-store" });
        if (!res.ok || !res.body) throw new Error(`The stream answered ${res.status}`);
        this.set({ state: "open", message: "", openedAt: Date.now() });
        delay = 1000;
        const parser = new SseParser();
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader();
        for (;;) {
          const { value, done } = await reader.read();
          if (done || gen !== this.generation) break;
          this.status.bytes += value.length;
          for (const m of parser.push(value)) {
            if (m.event !== "message") continue;
            const pos = m.id ? positionFromId(m.id).timestamp : null;
            if (pos !== null) this.status.position = pos;
            this.onMessage(m.data, pos);
          }
        }
        if (gen !== this.generation) return;
        throw new Error("The stream closed the connection");
      } catch (e) {
        if (gen !== this.generation || (e as Error).name === "AbortError") return;
        const msg = (e as Error).message || "Network error";
        this.set({ state: "retrying", message: /Failed to fetch|NetworkError|Load failed/i.test(msg) ? "Can't reach stream.wikimedia.org" : msg, reconnects: this.status.reconnects + 1 });
        await new Promise((r) => setTimeout(r, delay));
        delay = Math.min(delay * 2, 15000);
        // Pick up exactly where we were (minus a small overlap that dedup removes).
        if (this.status.position !== null) from = this.status.position - OVERLAP_MS;
      }
    }
  }
}

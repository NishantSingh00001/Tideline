// Window table functions: TUMBLE, HOP, CUMULATE and SESSION, with the same
// assignment rules as Apache Flink.

import { alignedStart, formatDuration } from "./time.ts";

export type WindowSpec =
  | { kind: "TUMBLE"; size: number; offset: number }
  | { kind: "HOP"; slide: number; size: number; offset: number }
  | { kind: "CUMULATE"; step: number; size: number; offset: number }
  | { kind: "SESSION"; gap: number; partitionBy: string[] };

export interface TimeWindow {
  start: number;
  end: number; // exclusive
}

/** A window has fired (and later records for it are late) once the watermark reaches end - 1. */
export const isFired = (end: number, watermark: number) => watermark >= end - 1;

/** All aligned windows an event-time timestamp belongs to, oldest first. */
export function assignWindows(spec: Exclude<WindowSpec, { kind: "SESSION" }>, ts: number, tz: string): TimeWindow[] {
  switch (spec.kind) {
    case "TUMBLE": {
      const start = alignedStart(ts, spec.offset, spec.size, tz);
      return [{ start, end: start + spec.size }];
    }
    case "HOP": {
      // SlidingEventTimeWindows.assignWindows
      const out: TimeWindow[] = [];
      const last = alignedStart(ts, spec.offset, spec.slide, tz);
      for (let start = last; start > ts - spec.size; start -= spec.slide) out.push({ start, end: start + spec.size });
      return out.reverse();
    }
    case "CUMULATE": {
      // Windows share a start and grow by `step` until `size`.
      const start = alignedStart(ts, spec.offset, spec.size, tz);
      const out: TimeWindow[] = [];
      const first = start + (Math.floor((ts - start) / spec.step) + 1) * spec.step;
      for (let end = first; end <= start + spec.size; end += spec.step) out.push({ start, end });
      return out;
    }
  }
}

export function describeWindow(spec: WindowSpec, timeCol = "event_time"): string {
  switch (spec.kind) {
    case "TUMBLE": return `TUMBLE(${timeCol}, ${formatDuration(spec.size)})`;
    case "HOP": return `HOP(${timeCol}, slide ${formatDuration(spec.slide)}, size ${formatDuration(spec.size)})`;
    case "CUMULATE": return `CUMULATE(${timeCol}, step ${formatDuration(spec.step)}, max ${formatDuration(spec.size)})`;
    case "SESSION": return `SESSION(${timeCol}, gap ${formatDuration(spec.gap)}${spec.partitionBy.length ? `, partition by ${spec.partitionBy.join(", ")}` : ""})`;
  }
}

/** Checks the same constraints Flink's planner enforces on window TVF arguments. */
export function validateWindow(spec: WindowSpec): string | null {
  switch (spec.kind) {
    case "TUMBLE":
      if (spec.size <= 0) return "TUMBLE window size must be positive.";
      if (Math.abs(spec.offset) >= spec.size) return "TUMBLE window offset must be smaller than the window size.";
      return null;
    case "HOP":
      if (spec.size <= 0 || spec.slide <= 0) return "HOP window size and slide must be positive.";
      if (spec.size % spec.slide !== 0) return `HOP window requires size (${formatDuration(spec.size)}) to be an integral multiple of slide (${formatDuration(spec.slide)}).`;
      return null;
    case "CUMULATE":
      if (spec.size <= 0 || spec.step <= 0) return "CUMULATE window step and max size must be positive.";
      if (spec.size % spec.step !== 0) return `CUMULATE window requires max size (${formatDuration(spec.size)}) to be an integral multiple of step (${formatDuration(spec.step)}).`;
      return null;
    case "SESSION":
      if (spec.gap <= 0) return "SESSION window gap must be positive.";
      return null;
  }
}

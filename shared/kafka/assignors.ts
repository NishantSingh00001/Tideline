// Consumer-group partition assignment strategies.
//
// range and roundrobin follow Kafka's RangeAssignor and RoundRobinAssignor
// exactly (for one topic). sticky keeps every partition where it was when it
// can and only moves what's needed to stay balanced, which is the goal of
// Kafka's (Cooperative)StickyAssignor; Kafka's algorithm also handles many
// topics and subscriptions, which this single-topic version doesn't need.

export type Strategy = "range" | "roundrobin" | "sticky";
export type Assignment = Record<string, number[]>;

const byId = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

export function assign(strategy: Strategy, consumers: string[], partitions: number, previous: Assignment = {}): Assignment {
  const members = [...consumers].sort(byId);
  const out: Assignment = Object.fromEntries(members.map((m) => [m, [] as number[]]));
  if (!members.length) return out;
  if (strategy === "range") {
    const per = Math.floor(partitions / members.length);
    const extra = partitions % members.length;
    members.forEach((m, i) => {
      const start = per * i + Math.min(i, extra);
      const len = per + (i + 1 > extra ? 0 : 1);
      for (let p = start; p < start + len; p++) out[m].push(p);
    });
    return out;
  }
  if (strategy === "roundrobin") {
    for (let p = 0; p < partitions; p++) out[members[p % members.length]].push(p);
    return out;
  }
  // sticky
  const owned = new Set<number>();
  for (const m of members)
    for (const p of previous[m] || [])
      if (p < partitions && !owned.has(p)) {
        out[m].push(p);
        owned.add(p);
      }
  const base = Math.floor(partitions / members.length);
  let extras = partitions % members.length;
  // Members that already hold more keep the +1 slots, so fewer partitions move.
  const order = [...members].sort((a, b) => out[b].length - out[a].length || byId(a, b));
  const target: Record<string, number> = {};
  for (const m of order) target[m] = base + (extras-- > 0 ? 1 : 0);
  const free: number[] = [];
  for (let p = 0; p < partitions; p++) if (!owned.has(p)) free.push(p);
  for (const m of members) {
    out[m].sort((a, b) => a - b);
    while (out[m].length > target[m]) free.push(out[m].pop()!);
  }
  free.sort((a, b) => a - b);
  for (const p of free) {
    const m = members.filter((x) => out[x].length < target[x]).sort((a, b) => out[a].length - out[b].length || byId(a, b))[0];
    out[m].push(p);
  }
  for (const m of members) out[m].sort((a, b) => a - b);
  return out;
}

/** Partitions that changed owner between two assignments. */
export function moved(before: Assignment, after: Assignment): number {
  const owner = new Map<number, string>();
  for (const [m, ps] of Object.entries(before)) for (const p of ps) owner.set(p, m);
  let n = 0;
  for (const [m, ps] of Object.entries(after)) for (const p of ps) if (owner.has(p) && owner.get(p) !== m) n++;
  return n;
}

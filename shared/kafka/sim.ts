// A model of a Kafka topic and consumer group, fed by the live change stream.
//
// Producing uses the real default partitioner (murmur2 for keyed records,
// sticky batches for records without a key). The consumer group uses the real
// assignment strategies, offsets, auto-commit and rebalances, so you can see
// key skew, consumer lag, idle consumers, and what crashes do under
// at-most-once, at-least-once and exactly-once delivery.

import { assign, type Assignment, type Strategy } from "./assignors.ts";
import { partitionFor } from "./partitioner.ts";

export type Delivery = "at-most-once" | "at-least-once" | "exactly-once";
export type KeyMode = "wiki" | "page" | "none";

export interface KafkaConfig {
  partitions: number;
  key: KeyMode;
  strategy: Strategy;
  delivery: Delivery;
  /** Records per second one consumer can process. */
  capacity: number;
  /** auto.commit.interval.ms */
  commitInterval: number;
  /** How long the group waits for a crashed consumer (session.timeout.ms, scaled down). */
  sessionTimeout: number;
}

export interface PartitionView {
  id: number;
  leo: number;
  committed: number;
  position: number;
  lag: number;
  rate: number;
  owner: string | null;
  topKeys: [string, number][];
  paused: boolean;
}

export interface ConsumerView {
  id: string;
  alive: boolean;
  assigned: number[];
  rate: number;
  processed: number;
}

export interface KafkaView {
  config: KafkaConfig;
  partitions: PartitionView[];
  consumers: ConsumerView[];
  group: "Stable" | "PreparingRebalance";
  rebalanceIn: number | null;
  generation: number;
  produced: number;
  processed: number;
  duplicates: number;
  lost: number;
  log: { at: number; text: string }[];
  skew: number;
}

interface Part {
  leo: number;
  committed: number;
  position: number;
  /** Highest offset ever processed + 1 (to detect reprocessing). */
  done: number;
  rate: number;
  inWindow: number;
  keys: Map<string, number>;
  pausedUntil: number;
}

interface Consumer {
  id: string;
  alive: boolean;
  processed: number;
  rate: number;
  inWindow: number;
  carry: number;
  rr: number;
}

export const DEFAULT_KAFKA: KafkaConfig = {
  partitions: 6,
  key: "wiki",
  strategy: "sticky",
  delivery: "at-least-once",
  capacity: 12,
  commitInterval: 5000,
  sessionTimeout: 6000,
};

export class KafkaSim {
  config: KafkaConfig;
  private parts: Part[] = [];
  private consumers: Consumer[] = [];
  private assignment: Assignment = {};
  private generation = 0;
  private produced = 0;
  private duplicates = 0;
  private lost = 0;
  private log: { at: number; text: string }[] = [];
  private stickyPartition = 0;
  private stickyLeft = 0;
  private lastCommit = 0;
  private rebalanceAt: number | null = null;
  private nextId = 1;
  private lastRateAt = 0;
  private rng = 1;

  constructor(config: Partial<KafkaConfig> = {}, consumers = 3, private now: () => number = Date.now) {
    this.config = { ...DEFAULT_KAFKA, ...config };
    this.reset(consumers);
  }

  private say(text: string) {
    this.log.unshift({ at: this.now(), text });
    if (this.log.length > 30) this.log.length = 30;
  }

  private rand() {
    // xorshift: deterministic, so the model behaves the same in tests.
    let x = this.rng || 1;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.rng = x >>> 0;
    return this.rng / 4294967296;
  }

  reset(consumers = this.consumers.filter((c) => c.alive).length || 3) {
    this.parts = Array.from({ length: this.config.partitions }, () => ({ leo: 0, committed: 0, position: 0, done: 0, rate: 0, inWindow: 0, keys: new Map(), pausedUntil: 0 }));
    this.consumers = [];
    this.nextId = 1;
    this.assignment = {};
    this.generation = 0;
    this.produced = this.duplicates = this.lost = 0;
    this.log = [];
    for (let i = 0; i < consumers; i++) this.consumers.push(this.newConsumer());
    this.rebalance("group created", true);
  }

  private newConsumer(): Consumer {
    return { id: `consumer-${this.nextId++}`, alive: true, processed: 0, rate: 0, inWindow: 0, carry: 0, rr: 0 };
  }

  configure(p: Partial<KafkaConfig>) {
    const before = this.config;
    this.config = { ...this.config, ...p };
    if (p.partitions !== undefined && p.partitions !== before.partitions) {
      this.say(`Topic recreated with ${this.config.partitions} partitions`);
      this.reset();
    } else if (p.strategy && p.strategy !== before.strategy) {
      this.rebalance(`strategy changed to ${p.strategy}`);
    }
    if (p.key && p.key !== before.key) this.say(p.key === "none" ? "Producing without a key: sticky batches" : `Producing with key = ${p.key}`);
    if (p.delivery && p.delivery !== before.delivery) this.say(`Delivery: ${p.delivery}`);
  }

  /** The producer: one record per change. */
  produce(wiki: string, page: string) {
    const P = this.parts.length;
    let p: number;
    let key: string | null = null;
    if (this.config.key === "none") {
      // Sticky partitioner: fill a batch on one partition, then pick another.
      if (this.stickyLeft <= 0) {
        this.stickyPartition = Math.floor(this.rand() * P);
        this.stickyLeft = 32;
      }
      this.stickyLeft--;
      p = this.stickyPartition;
    } else {
      key = this.config.key === "wiki" ? wiki : page;
      p = partitionFor(key, P);
    }
    const part = this.parts[p];
    part.leo++;
    part.inWindow++;
    this.produced++;
    const k = key ?? "(no key)";
    part.keys.set(k, (part.keys.get(k) || 0) + 1);
    if (part.keys.size > 400) {
      // Keep the map small: drop the rarest keys.
      const sorted = [...part.keys.entries()].sort((a, b) => b[1] - a[1]).slice(0, 200);
      part.keys = new Map(sorted);
    }
  }

  private owner(p: number): Consumer | undefined {
    for (const c of this.consumers) if (c.alive && (this.assignment[c.id] || []).includes(p)) return c;
    return undefined;
  }

  /** Advances consumers by dt milliseconds. */
  step(dt: number) {
    const t = this.now();
    if (this.rebalanceAt !== null && t >= this.rebalanceAt) {
      this.rebalanceAt = null;
      this.rebalance("session timeout expired");
    }
    for (const c of this.consumers) {
      if (!c.alive) continue;
      const mine = (this.assignment[c.id] || []).filter((p) => this.parts[p].pausedUntil <= t);
      if (!mine.length) continue;
      c.carry += (this.config.capacity * dt) / 1000;
      let budget = Math.floor(c.carry);
      c.carry -= budget;
      // Round-robin over this consumer's partitions, one record at a time.
      let idle = 0;
      while (budget > 0 && idle < mine.length) {
        const p = mine[c.rr++ % mine.length];
        const part = this.parts[p];
        if (part.position >= part.leo) {
          idle++;
          continue;
        }
        idle = 0;
        if (part.position < part.done) this.duplicates++;
        part.position++;
        part.done = Math.max(part.done, part.position);
        c.processed++;
        c.inWindow++;
        budget--;
        if (this.config.delivery === "exactly-once") part.committed = part.position; // committed in the same transaction
      }
      if (this.config.delivery === "at-most-once") {
        // Offsets are committed when records are fetched, before processing:
        // a whole fetch (one second of work) ahead of what is done.
        for (const p of mine) this.parts[p].committed = Math.min(this.parts[p].leo, Math.max(this.parts[p].committed, this.parts[p].position + this.config.capacity));
      }
    }
    if (this.config.delivery === "at-least-once" && t - this.lastCommit >= this.config.commitInterval) {
      this.lastCommit = t;
      for (const c of this.consumers) if (c.alive) for (const p of this.assignment[c.id] || []) this.parts[p].committed = this.parts[p].position;
    }
    if (t - this.lastRateAt >= 1000) {
      const secs = (t - this.lastRateAt) / 1000;
      if (this.lastRateAt) {
        for (const p of this.parts) {
          p.rate = p.rate * 0.6 + (p.inWindow / secs) * 0.4;
          p.inWindow = 0;
        }
        for (const c of this.consumers) {
          c.rate = c.rate * 0.6 + (c.inWindow / secs) * 0.4;
          c.inWindow = 0;
        }
      }
      this.lastRateAt = t;
    }
  }

  /** A consumer process dies without leaving the group. */
  kill(id: string) {
    const c = this.consumers.find((x) => x.id === id && x.alive);
    if (!c) return;
    c.alive = false;
    c.rate = 0;
    this.say(`${id} crashed. Its partitions stop until the group notices (session timeout)`);
    if (this.rebalanceAt === null) this.rebalanceAt = this.now() + this.config.sessionTimeout;
  }

  /** A new consumer joins the group (a JoinGroup request triggers a rebalance at once). */
  add() {
    if (this.consumers.filter((c) => c.alive).length >= 12) return;
    const c = this.newConsumer();
    this.consumers.push(c);
    this.rebalance(`${c.id} joined`);
  }

  private rebalance(reason: string, initial = false) {
    const alive = this.consumers.filter((c) => c.alive);
    const before = this.assignment;
    const after = assign(this.config.strategy, alive.map((c) => c.id), this.parts.length, before);
    const t = this.now();
    const prevOwner = new Map<number, string>();
    for (const [m, ps] of Object.entries(before)) for (const p of ps) prevOwner.set(p, m);
    const cooperative = this.config.strategy === "sticky";
    let moved = 0;
    for (const [m, ps] of Object.entries(after))
      for (const p of ps) {
        const from = prevOwner.get(p);
        const changed = from !== m;
        if (changed) moved++;
        const part = this.parts[p];
        const crashed = from && !this.consumers.find((c) => c.id === from)?.alive;
        if (changed || !cooperative) {
          if (crashed) {
            // The new owner starts from the last committed offset.
            if (part.committed < part.position) this.say(`Partition ${p}: ${part.position - part.committed} records after the last commit will be processed again`);
            if (part.committed > part.position) {
              this.lost += part.committed - part.position;
              this.say(`Partition ${p}: ${part.committed - part.position} fetched records were never processed (lost)`);
            }
            part.position = part.committed;
          } else if (from) {
            // A clean handover: the old owner commits on revoke.
            part.committed = part.position;
          }
          // Eager protocols stop every partition during the rebalance; cooperative only the moved ones.
          if (!initial) part.pausedUntil = t + 700;
        }
      }
    this.assignment = after;
    this.generation++;
    if (!initial) this.say(`Rebalance (generation ${this.generation}): ${reason}. ${cooperative ? `${moved} partition${moved === 1 ? "" : "s"} moved; the rest kept working` : "every partition was revoked and reassigned"}`);
  }

  view(): KafkaView {
    const t = this.now();
    const rates = this.parts.map((p) => p.rate);
    const avg = rates.reduce((s, r) => s + r, 0) / (rates.length || 1);
    return {
      config: this.config,
      partitions: this.parts.map((p, i) => ({
        id: i,
        leo: p.leo,
        committed: p.committed,
        position: p.position,
        lag: p.leo - p.position,
        rate: p.rate,
        owner: this.owner(i)?.id ?? null,
        topKeys: [...p.keys.entries()].sort((a, b) => b[1] - a[1]).slice(0, 3),
        paused: p.pausedUntil > t,
      })),
      consumers: this.consumers.filter((c) => c.alive || t - 0 < Infinity).map((c) => ({ id: c.id, alive: c.alive, assigned: this.assignment[c.id] || [], rate: c.rate, processed: c.processed })),
      group: this.rebalanceAt !== null ? "PreparingRebalance" : "Stable",
      rebalanceIn: this.rebalanceAt !== null ? Math.max(0, this.rebalanceAt - t) : null,
      generation: this.generation,
      produced: this.produced,
      processed: this.consumers.reduce((s, c) => s + c.processed, 0),
      duplicates: this.duplicates,
      lost: this.lost,
      log: this.log,
      skew: avg > 0 ? Math.max(...rates) / avg : 1,
    };
  }
}

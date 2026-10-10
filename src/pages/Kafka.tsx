import { useEffect } from "react";
import { live, useLiveVersion } from "../lib/live.ts";
import { int } from "../lib/format.ts";
import type { Delivery, KeyMode } from "../../shared/kafka/sim.ts";
import type { Strategy } from "../../shared/kafka/assignors.ts";

const WINDOW = 240; // offsets shown per partition bar

function Seg<T extends string>({ value, options, onChange, label }: { value: T; options: [T, string][]; onChange: (v: T) => void; label: string }) {
  return (
    <div className="field">
      <span>{label}</span>
      <div className="seg" role="group" aria-label={label}>
        {options.map(([v, l]) => (
          <button key={v} aria-pressed={value === v} onClick={() => onChange(v)}>
            {l}
          </button>
        ))}
      </div>
    </div>
  );
}

const DELIVERY_NOTES: Record<Delivery, string> = {
  "at-most-once": "Offsets are committed when records are fetched, before they're processed. A crash loses the fetched records nobody finished.",
  "at-least-once": "Offsets are committed every 5 seconds after processing (enable.auto.commit). A crash means the next owner re-reads everything since the last commit.",
  "exactly-once": "Results and offsets are committed together in one transaction (read-process-write with transactions), so a crash neither loses nor repeats a record.",
};

export default function Kafka() {
  const s = useLiveVersion();
  const k = s.kafka;
  useEffect(() => {
    live.kafka({});
  }, []);
  if (!k) return <div className="wrap waiting">Creating the topic…</div>;
  const c = k.config;
  const lag = k.partitions.reduce((a, p) => a + p.lag, 0);
  const alive = k.consumers.filter((x) => x.alive);
  const idle = alive.filter((x) => !x.assigned.length);
  const hot = [...k.partitions].sort((a, b) => b.rate - a.rate)[0];
  return (
    <div className="wrap" style={{ paddingBottom: 60 }}>
      <header className="page-head">
        <div>
          <h1>Partitions, keys and consumer groups</h1>
          <p>
            Every change from the live stream is produced to a model of a Kafka topic with Kafka's own partitioner (murmur2 of the key), then read by a consumer group using Kafka's assignment strategies. Change the key, crash a consumer, or switch the delivery guarantee and watch lag, rebalances, duplicates and losses.
          </p>
        </div>
      </header>

      <div className="kafka-controls">
        <label className="field">
          Partitions
          <select value={c.partitions} onChange={(e) => live.kafka({ partitions: Number(e.target.value) })}>
            {[1, 2, 3, 4, 6, 8, 12].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <Seg<KeyMode> label="Record key" value={c.key} onChange={(key) => live.kafka({ key })} options={[["wiki", "wiki"], ["page", "wiki + page"], ["none", "no key"]]} />
        <Seg<Strategy> label="Assignment" value={c.strategy} onChange={(strategy) => live.kafka({ strategy })} options={[["range", "Range"], ["roundrobin", "Round robin"], ["sticky", "Cooperative sticky"]]} />
        <label className="field">
          Delivery
          <select value={c.delivery} onChange={(e) => live.kafka({ delivery: e.target.value as Delivery })}>
            <option value="at-most-once">At most once</option>
            <option value="at-least-once">At least once</option>
            <option value="exactly-once">Exactly once</option>
          </select>
        </label>
        <label className="field">
          Each consumer handles {c.capacity} records/s
          <input type="range" min={2} max={40} value={c.capacity} onChange={(e) => live.kafka({ capacity: Number(e.target.value) })} />
        </label>
        <button className="btn" onClick={() => live.kafkaAdd()} disabled={alive.length >= 12}>
          Add a consumer
        </button>
      </div>
      <p className="panel-note">{DELIVERY_NOTES[c.delivery]}</p>

      <dl className="counters">
        <div>
          <dt>Produced</dt>
          <dd>{int(k.produced)}</dd>
        </div>
        <div>
          <dt>Processed</dt>
          <dd>{int(k.processed)}</dd>
        </div>
        <div>
          <dt>Consumer lag</dt>
          <dd>{int(lag)}</dd>
        </div>
        <div className={k.duplicates ? "bad" : ""}>
          <dt>Processed twice</dt>
          <dd>{int(k.duplicates)}</dd>
        </div>
        <div className={k.lost ? "bad" : ""}>
          <dt>Never processed</dt>
          <dd>{int(k.lost)}</dd>
        </div>
      </dl>

      <div className="grid" style={{ paddingTop: 30 }}>
        <section className="panel span-8">
          <div className="panel-head">
            <h2>Topic wiki.changes</h2>
            <span className="aside">{k.group === "Stable" ? `Group stable, generation ${k.generation}` : `Rebalancing in ${((k.rebalanceIn ?? 0) / 1000).toFixed(1)} s`}</span>
          </div>
          <div className="partitions">
            {k.partitions.map((p) => {
              const x = (off: number) => Math.max(0, Math.min(100, 100 - ((p.leo - off) / WINDOW) * 100));
              return (
                <div className="part" key={p.id}>
                  <div className="label">
                    Partition {p.id}
                    <small>{p.rate.toFixed(1)} records/s</small>
                  </div>
                  <div className="log-bar" title={`Log end ${p.leo}, consumer position ${p.position}, committed ${p.committed}`}>
                    <div className="fill" style={{ left: `${x(p.position)}%`, right: 0, width: "auto" }} />
                    <div className="com" style={{ left: `calc(${x(p.committed)}% - 1px)` }} />
                    <div className="pos" style={{ left: `calc(${x(p.position)}% - 1px)` }} />
                    <span className="keys">{p.topKeys.map(([key, n]) => `${key} ${n}`).join(", ") || "empty"}</span>
                  </div>
                  <div className="owner">
                    {p.owner ? p.owner : <span style={{ color: "var(--magenta)" }}>no owner</span>}
                    {p.paused && <span className="muted"> (paused by rebalance)</span>}
                    <small>
                      lag {int(p.lag)}, offset {int(p.position)} of {int(p.leo)}
                    </small>
                  </div>
                </div>
              );
            })}
          </div>
          <p className="panel-note">
            Each bar is the end of the partition's log, newest record on the right. The black tick is where the owner has read to, the magenta tick the last committed offset; shaded records are waiting (lag).
            {c.key === "wiki" && hot && hot.rate > 0 && k.skew > 1.6
              ? ` Keyed by wiki, every Wikidata change lands on one partition, so partition ${hot.id} takes ${k.skew.toFixed(1)}× the average load: a hot partition.`
              : c.key === "none"
                ? " Without a key the producer fills a batch on one partition, then moves on (the sticky partitioner): load evens out but per-wiki order is lost."
                : ""}
          </p>
        </section>
        <section className="panel span-4">
          <div className="panel-head">
            <h2>Consumer group</h2>
            <span className="aside">{alive.length} running</span>
          </div>
          <div className="consumers" style={{ marginTop: 0 }}>
            {k.consumers.map((x) => (
              <div key={x.id} className={`consumer${x.alive ? "" : " dead"}`}>
                <h4>
                  {x.id}
                  {x.alive && (
                    <button className="btn small danger" onClick={() => live.kafkaKill(x.id)}>
                      Crash
                    </button>
                  )}
                </h4>
                <p>
                  {!x.alive ? "Crashed" : x.assigned.length ? `Partitions ${x.assigned.join(", ")}` : "Idle: more consumers than partitions"}
                  <br />
                  {x.rate.toFixed(1)} records/s, {int(x.processed)} processed
                </p>
              </div>
            ))}
          </div>
          {idle.length > 0 && <p className="panel-note">A partition is read by one consumer of a group at a time, so consumers beyond the partition count sit idle.</p>}
          <h3 style={{ font: "500 19px/1.2 var(--serif)", margin: "24px 0 8px" }}>Group log</h3>
          <ul className="events-log">
            {k.log.map((l, i) => (
              <li key={l.at + ":" + i}>
                <time>{new Date(l.at).toLocaleTimeString("en-GB")}</time>
                {l.text}
              </li>
            ))}
          </ul>
        </section>
      </div>
    </div>
  );
}

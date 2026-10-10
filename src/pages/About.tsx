import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { int } from "../lib/format.ts";

interface Parity {
  flink: string;
  events: number;
  checked: number;
  allMatch: boolean;
  settings: string;
  queries: { id: string; title: string; rows: number; messages: number; flinkMessages: number; match: boolean }[];
}

const COURSES: { name: string; issuer: string; used: string }[] = [
  {
    name: "Data Streaming Engineer Foundations",
    issuer: "Confluent",
    used: "Topics, partitions and keys with Kafka's murmur2 partitioner; consumer groups with range, round-robin and cooperative-sticky assignment; offsets, auto-commit and delivery guarantees; an idempotent producer with Avro in Schema Registry and a dead-letter topic in the Kafka pipeline.",
  },
  {
    name: "Data Streaming Engineer: Apache Flink",
    issuer: "Confluent",
    used: "Event time and watermarks, TUMBLE/HOP/CUMULATE/SESSION window table functions, window Top-N, changelogs from unbounded aggregations, MATCH_RECOGNIZE, EXPLAIN plans, and checkpoint-based recovery, all checked against real Flink.",
  },
  {
    name: "Serverless Data Analysis with BigQuery and Cloud Dataflow",
    issuer: "Google Cloud",
    used: "The streaming model behind Dataflow and Beam: fixed, sliding and session windows, watermarks and late data, and SQL over a stream.",
  },
  {
    name: "AWS Data Engineering",
    issuer: "Amazon Web Services",
    used: "Stream ingestion, validation and a dead-letter queue, a pipeline that is tested and automated end to end, and durable state for recovery.",
  },
  {
    name: "DevOps Basics",
    issuer: "Udemy",
    used: "The Kafka, Schema Registry and Flink pipeline is a Docker Compose stack. A GitHub Actions workflow type-checks, runs the tests, builds the site, and re-runs the parity check on real Flink on every push.",
  },
];

export default function About() {
  const [p, setP] = useState<Parity | null>(null);
  useEffect(() => {
    fetch("/parity.json")
      .then((r) => r.json())
      .then(setP)
      .catch(() => setP(null));
  }, []);
  return (
    <div className="wrap" style={{ paddingBottom: 70 }}>
      <header className="page-head">
        <div>
          <h1>How Tideline works</h1>
          <p>
            A stream processor written in TypeScript that runs in your browser, follows Apache Flink's rules, and is tested against real Flink. Next to it in the repository is the same pipeline on real Kafka and Flink.
          </p>
        </div>
      </header>

      <section className="prose">
        <h2>Checked against Apache Flink</h2>
        {p ? (
          <p>
            The same {int(p.events)} events went through Apache Flink {p.flink} (PyFlink on a local mini-cluster) and through Tideline. {p.allMatch ? "Every query" : "Not every query"} produced the same result{p.allMatch ? `: ${int(p.checked)} rows across ${p.queries.length} queries, including the number of changelog messages and the edit-war matches from MATCH_RECOGNIZE.` : "."}
          </p>
        ) : (
          <p>Loading the parity report…</p>
        )}
      </section>
      {p && (
        <div className="table-wrap" style={{ maxHeight: "none", maxWidth: 880, marginTop: 8 }}>
          <table className="data parity-table">
            <thead>
              <tr>
                <th>Query</th>
                <th style={{ textAlign: "right" }}>Result rows</th>
                <th style={{ textAlign: "right" }}>Messages (Tideline / Flink)</th>
                <th>Same as Flink</th>
              </tr>
            </thead>
            <tbody>
              {p.queries.map((q) => (
                <tr key={q.id}>
                  <td>{q.title}</td>
                  <td className="n">{int(q.rows)}</td>
                  <td className="n">
                    {int(q.messages)} / {int(q.flinkMessages)}
                  </td>
                  <td className={q.match ? "ok" : "no"}>{q.match ? "Yes" : "No"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <section className="prose">
        <p>
          Settings that make Flink's output repeatable: {p?.settings || "parallelism 1, UTC, a watermark after every record"}. The test data is a deterministic generator modelled on the real stream, including late category changes and edit wars, because the live stream can't be replayed in CI.
        </p>
        <p>
          The same suite on Flink 1.20.3, the version the Kafka pipeline runs, agrees on 17 of the 18 queries. The odd one out is a tumbling window with an offset, where Flink 1.20.3 itself emits only every other window, with lower counts. Flink 2.3.0 and Tideline emit all of them, with the same numbers.
        </p>
        <p>
          The check earned its keep: it caught two real bugs. A window table function used without aggregation must pass late rows through rather than drop them, and the edit-war matcher was forgetting a revert that could still start a match. Both now behave exactly like Flink.
        </p>

        <h2>The pieces</h2>
        <h3>In your browser</h3>
        <p>
          A Web Worker reads <a href="https://stream.wikimedia.org/?doc">Wikimedia EventStreams</a> over server-sent events. That service is a window onto Kafka: every event carries its topic, partition and offset, and <code>?since=</code> replays history, which the replay buttons and recovery use. Each event is validated against the <code>mediawiki/recentchange</code> schema (failures go to a dead-letter count), deduplicated by id, given a watermark of event time minus the delay, and fed to every running query.
        </p>
        <p>
          The SQL front end parses Flink's dialect, type-checks it with Flink's rules (integer division, AVG of an INT is an INT, Flink's 431 reserved words, case-sensitive names), plans it into operators (Calc, WindowAggregate, GroupAggregate, WindowRank, Limit) and prints Flink-style EXPLAIN output. Window operators fire when the watermark reaches the window's end minus one millisecond, and drop late records the same way Flink does.
        </p>
        <h3>On the server</h3>
        <p>
          Netlify Functions store recovery checkpoints and shared queries in Netlify Blobs, and turn questions in English into Flink SQL with Gemini. The generated SQL is parsed and planned by the same engine before it's returned, and sent back to the model with the error if it doesn't compile.
        </p>
        <h3>On real Kafka and Flink</h3>
        <p>
          The <code>pipeline/</code> folder runs the same thing for real with Docker Compose: a Python producer reads EventStreams, validates and flattens each event, and produces it to Kafka keyed by wiki: idempotent, murmur2 partitioning, Avro with the schema in Schema Registry, and a dead-letter topic for bad events. One Flink SQL job runs the same queries as this site, with RocksDB state and checkpoints every 10 seconds, and writes the results back to Kafka exactly once, in a transaction per checkpoint.
        </p>

        <h2>What this project demonstrates</h2>
        <p>Built to put into practice what these courses teach.</p>
      </section>
      <div className="course-map">
        {COURSES.map((c) => (
          <div key={c.name}>
            <h3>{c.name}</h3>
            <span className="issuer">{c.issuer}</span>
            <p>{c.used}</p>
          </div>
        ))}
      </div>
      <section className="prose">
        <h2>Try it</h2>
        <p>
          Watch the watermark on the <Link to="/">live chart</Link> and drag its delay to see late events change. Write a window query on the <Link to="/sql">SQL page</Link>, or ask for one in English. Make a hot partition or crash a consumer on the <Link to="/kafka">Kafka page</Link>, then crash a job on the <Link to="/recovery">recovery page</Link> and watch it come back with the same numbers.
        </p>
      </section>
    </div>
  );
}

# Tideline on real Kafka and Flink

The website runs its own stream engine in your browser. This folder runs the same
queries on the real thing: Apache Kafka, Confluent Schema Registry and Apache Flink,
fed by Wikipedia's live edit stream.

```
stream.wikimedia.org ──SSE──▶ producer (Python) ──Avro──▶ Kafka: wikimedia.recentchange (6 partitions, keyed by wiki)
                                   │                              │
                                   └─ bad events ─▶ …recentchange.dlq   Flink SQL (one job, six results)
                                                                  │
                     tideline.pulse · tideline.editors · tideline.trending · tideline.new-pages
                     tideline.edit-wars · tideline.wiki-totals (compacted, upserts)
```

## Run it

You need Docker with Compose (Docker Desktop is fine) and about 4 GB of free memory.

```bash
cd pipeline
docker compose up -d --build          # Kafka, Schema Registry, Flink and the producer
docker compose logs -f producer       # "produced 2410, delivered 2410, ... partitions {0: 391, 1: 450, ...}"
docker compose run --rm sql-submit    # submit the Flink SQL job
```

Then:

- **Flink dashboard:** http://localhost:8082. You'll see one job, its operators (window
  aggregates, window Top-N, MATCH_RECOGNIZE), watermarks and checkpoints every 10 s.
- **Results:** read a result topic. The job writes in Kafka transactions, so read
  committed data only:

  ```bash
  docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic tideline.trending \
    --isolation-level read_committed --from-beginning
  ```

- **Schema:** `curl localhost:8081/subjects/wikimedia.recentchange-value/versions/latest`.
- **Consumer lag:** `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --all-groups`.

Stop everything with `docker compose down` (add `-v` to delete the data too).

## What each part does

**`producer/producer.py`** does the job of a Kafka Connect source connector with a
few single-message transforms:

- Reads the server-sent events and resumes from its last position after a restart
  (`?since=`, stored on a volume).
- Validates each event and flattens it, with the same rules as `shared/change.ts`
  on the site. Events that fail go to the dead-letter topic, with the reason in a header.
- Serialises to Avro and registers `recentchange.avsc` in Schema Registry. The
  registry is set to `FULL_TRANSITIVE` compatibility, so a schema change has to stay
  readable both ways.
- Produces idempotently (`enable.idempotence`, `acks=all`) with zstd compression and
  50 ms batching. It keys by wiki with Kafka's murmur2 partitioner, so each wiki's
  changes stay in order on one partition and land where the Java client would put them.

**`flink/sql/`** is generated from `shared/queries.ts` by `npm run flink:sql`, so the
cluster runs exactly the queries the website runs and the parity test checks:

- `00_tables.sql`: the source table (Avro from Schema Registry, event-time
  watermark 5 s behind the newest event) and one Kafka sink per result. The updating
  result (all-time totals per wiki) goes through `upsert-kafka` to a compacted topic.
  Every sink is exactly-once: results are written in a Kafka transaction per checkpoint.
- `10_jobs.sql`: one `STATEMENT SET`, so Kafka is read once for all six results.
  The queries are tumbling windows, a window Top-N, an unbounded `GROUP BY`
  (changelog), and `MATCH_RECOGNIZE` for edit wars.

**Flink** runs with the RocksDB state backend and checkpoints every 10 s to a volume
shared by the JobManager and TaskManager. Stop the TaskManager
(`docker compose restart taskmanager`) and watch the job restore from the last
checkpoint and carry on without duplicating anything in the result topics.

## How it's tested

You don't need Docker for the tests. CI (`.github/workflows/ci.yml`) runs them on every push:

| What | How |
|---|---|
| Producer | `pytest` in `producer/`. Avro round trips through a mock Schema Registry. Partitioning is checked on librdkafka's built-in mock Kafka cluster against the Java client's murmur2. Also covers the dead-letter path, SSE parsing under any chunking, and validation identical to the TypeScript side. |
| Job SQL | `flink/check_sql.py` plans and runs both SQL files on real Flink (1.20.3 and 2.3.0) with the Kafka tables swapped for local files. |
| Query results | `tests/flink/run_flink.py` runs every query on real Flink, and `npm test` compares the browser engine's output with it row by row. |

I couldn't start containers in the environment where this was built, so the Compose
stack itself hasn't been run end to end there. The connector options follow the
Flink 1.20 Kafka connector documentation. If something doesn't start, `docker compose
logs <service>` shows why.

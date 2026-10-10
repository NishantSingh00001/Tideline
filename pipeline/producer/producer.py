#!/usr/bin/env python3
"""Reads Wikimedia EventStreams and produces every change to Kafka.

It does the job of a Kafka Connect source connector with a couple of single
message transforms:

* reads https://stream.wikimedia.org/v2/stream/recentchange (server-sent events)
* validates each event and flattens it to the recentchange schema
  (invalid events go to a dead-letter topic with the reason in a header)
* serialises it as Avro, registering the schema in Schema Registry
* produces it keyed by wiki, idempotently, with Java-compatible murmur2
  partitioning, so all changes to one wiki stay in order on one partition
* stores the stream position after each flush and resumes from it after a
  restart (?since=), so a restart neither loses nor duplicates much: Flink
  deduplicates the small overlap by id

Configuration (environment variables):
  BOOTSTRAP_SERVERS     default localhost:29092
  SCHEMA_REGISTRY_URL   default http://localhost:8081
  TOPIC                 default wikimedia.recentchange
  DLQ_TOPIC             default wikimedia.recentchange.dlq
  STREAM_URL            default https://stream.wikimedia.org/v2/stream/recentchange
  POSITION_FILE         default ./position.json
  CONTACT               an email or URL for the User-Agent (Wikimedia asks for one)
"""
from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

log = logging.getLogger("producer")

UUID = re.compile(r"^[a-fA-F0-9]{8}(-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}$")
SCHEMA = (Path(__file__).parent / "recentchange.avsc").read_text()


class Invalid(Exception):
    pass


def _int_or_none(v):
    return int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def parse_dt(s: str) -> int:
    """meta.dt (ISO 8601) to epoch milliseconds."""
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception as e:  # noqa: BLE001
        raise Invalid("meta.dt is not a date-time") from e


def flatten(e: object) -> dict:
    """Validates a recentchange event and flattens it. Same rules as shared/change.ts."""
    if not isinstance(e, dict):
        raise Invalid("not a JSON object")
    meta = e.get("meta")
    if not isinstance(meta, dict):
        raise Invalid("missing meta")
    mid = meta.get("id")
    if not isinstance(mid, str) or not UUID.match(mid):
        raise Invalid("meta.id is not a UUID")
    stream = meta.get("stream")
    if stream and stream != "mediawiki.recentchange":
        raise Invalid(f"unexpected stream {stream}")
    if not isinstance(meta.get("dt"), str):
        raise Invalid("meta.dt is not a date-time")
    ts = parse_dt(meta["dt"])
    wiki = e.get("wiki")
    if not isinstance(wiki, str) or not wiki:
        raise Invalid("missing wiki")
    if not isinstance(e.get("type"), str):
        raise Invalid("missing type")
    if not isinstance(e.get("title"), str):
        raise Invalid("missing title")
    length = e.get("length") if isinstance(e.get("length"), dict) else {}
    revision = e.get("revision") if isinstance(e.get("revision"), dict) else {}
    return {
        "id": mid,
        "change_type": e["type"],
        "wiki": wiki,
        "server_name": e["server_name"] if isinstance(e.get("server_name"), str) else (meta.get("domain") or ""),
        "title": e["title"],
        "ns": e["namespace"] if isinstance(e.get("namespace"), int) else 0,
        "user_name": e["user"] if isinstance(e.get("user"), str) else "",
        "is_bot": e.get("bot") is True,
        "is_minor": e.get("minor") is True,
        "len_old": _int_or_none(length.get("old")),
        "len_new": _int_or_none(length.get("new")),
        "edit_comment": e["comment"] if isinstance(e.get("comment"), str) else "",
        "log_type": e["log_type"] if isinstance(e.get("log_type"), str) else None,
        "rev_id": _int_or_none(revision.get("new")),
        "ts_ms": ts,
    }


class SseParser:
    """Server-sent events, per the HTML spec (same as shared/sse.ts)."""

    def __init__(self) -> None:
        self.buf = ""
        self.data: list[str] = []
        self.event = ""
        self.id = ""

    def push(self, chunk: str) -> list[tuple[str, str, str]]:
        self.buf += chunk
        out = []
        while True:
            m = re.search(r"\r\n|\r|\n", self.buf)
            if not m:
                break
            if m.group() == "\r" and m.end() == len(self.buf):
                break  # maybe half of \r\n
            line, self.buf = self.buf[: m.start()], self.buf[m.end():]
            if line == "":
                if self.data:
                    out.append((self.event or "message", "\n".join(self.data), self.id))
                self.data, self.event = [], ""
                continue
            if line.startswith(":"):
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "data":
                self.data.append(value)
            elif field == "event":
                self.event = value
            elif field == "id" and "\0" not in value:
                self.id = value
        return out


def position_from_id(sse_id: str) -> int | None:
    try:
        ts = [p["timestamp"] for p in json.loads(sse_id) if isinstance(p.get("timestamp"), int) and p["timestamp"] > 0]
        return max(ts) if ts else None
    except Exception:  # noqa: BLE001
        return None


def read_stream(url: str, since: int | None, user_agent: str, stop) -> Iterator[tuple[str, str]]:
    """Yields (data, id) forever, reconnecting from the last position."""
    import requests

    delay = 1.0
    position = since
    while not stop():
        params = {"since": str(position - 2000)} if position else {}
        try:
            with requests.get(url, params=params, stream=True, timeout=(10, 60), headers={"User-Agent": user_agent, "Accept": "text/event-stream"}) as r:
                r.raise_for_status()
                log.info("connected to %s%s", url, f" from {position}" if position else "")
                delay = 1.0
                parser = SseParser()
                for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                    if stop():
                        return
                    for event, data, sse_id in parser.push(chunk):
                        if event != "message":
                            continue
                        pos = position_from_id(sse_id)
                        if pos:
                            position = pos
                        yield data, sse_id
        except Exception as e:  # noqa: BLE001
            log.warning("stream error: %s; reconnecting in %.0f s", e, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def producer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": "tideline-eventstreams-producer",
        # No duplicates or reordering on retries.
        "enable.idempotence": True,
        "acks": "all",
        # Throughput: batch for up to 50 ms and compress (Confluent's tuning guide).
        "linger.ms": 50,
        "batch.size": 256 * 1024,
        "compression.type": "zstd",
        # Same key -> same partition as the Java client and Flink's Kafka sink.
        "partitioner": "murmur2_random",
    }


class Pipeline:
    def __init__(self, producer, serialize_value, topic: str, dlq_topic: str):
        self.producer = producer
        self.serialize_value = serialize_value
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.produced = 0
        self.delivered = 0
        self.failed = 0
        self.dead = 0
        self.partitions: dict[int, int] = {}

    def _on_delivery(self, err, msg):
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1
            self.partitions[msg.partition()] = self.partitions.get(msg.partition(), 0) + 1

    def handle(self, data: str, sse_id: str = "") -> bool:
        """Validates, serialises and produces one event. Returns False if it went to the DLQ."""
        try:
            row = flatten(json.loads(data))
        except (Invalid, json.JSONDecodeError) as e:
            reason = str(e) if isinstance(e, Invalid) else "invalid JSON"
            self.producer.produce(self.dlq_topic, value=data.encode(), headers=[("error.reason", reason.encode()), ("source.id", sse_id.encode())])
            self.dead += 1
            return False
        from confluent_kafka.serialization import MessageField, SerializationContext

        value = self.serialize_value(row, SerializationContext(self.topic, MessageField.VALUE))
        while True:
            try:
                self.producer.produce(self.topic, key=row["wiki"].encode(), value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(0.5)  # local queue full: let it drain
        self.produced += 1
        self.producer.poll(0)
        return True


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer

    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "localhost:29092")
    registry = SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    topic = os.environ.get("TOPIC", "wikimedia.recentchange")
    dlq = os.environ.get("DLQ_TOPIC", "wikimedia.recentchange.dlq")
    url = os.environ.get("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
    pos_file = Path(os.environ.get("POSITION_FILE", "position.json"))
    contact = os.environ.get("CONTACT", "https://github.com/NishantSingh00001/Tideline")
    ua = f"TidelineProducer/1.0 ({contact}) python-requests"

    serializer = AvroSerializer(registry, SCHEMA, conf={"auto.register.schemas": True})
    producer = Producer(producer_config(bootstrap))
    pipe = Pipeline(producer, serializer, topic, dlq)

    stopping = {"now": False}
    signal.signal(signal.SIGTERM, lambda *_: stopping.update(now=True))
    signal.signal(signal.SIGINT, lambda *_: stopping.update(now=True))

    since = None
    if pos_file.exists():
        since = json.loads(pos_file.read_text()).get("position")
        log.info("resuming from stored position %s", since)
    last_report = time.time()
    last_pos = since
    for data, sse_id in read_stream(url, since, ua, lambda: stopping["now"]):
        pipe.handle(data, sse_id)
        last_pos = position_from_id(sse_id) or last_pos
        if time.time() - last_report > 10:
            # Commit the position only after Kafka acknowledged everything before it.
            producer.flush(30)
            if last_pos:
                pos_file.write_text(json.dumps({"position": last_pos}))
            log.info("produced %d, delivered %d, failed %d, dead letters %d, partitions %s", pipe.produced, pipe.delivered, pipe.failed, pipe.dead, dict(sorted(pipe.partitions.items())))
            last_report = time.time()
    producer.flush(30)
    if last_pos:
        pos_file.write_text(json.dumps({"position": last_pos}))
    log.info("stopped after %d records", pipe.produced)
    return 0


if __name__ == "__main__":
    sys.exit(main())

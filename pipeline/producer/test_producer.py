"""Tests for the producer: validation parity with the TypeScript engine, the
SSE parser, Avro round trips through a mock Schema Registry, Java-compatible
partitioning on librdkafka's built-in mock Kafka cluster, and the dead-letter path.

    pip install -r requirements.txt -r requirements-dev.txt
    pytest -q
"""
import json
import random
from pathlib import Path

import pytest

from producer import SCHEMA, Invalid, Pipeline, SseParser, flatten, position_from_id, producer_config

FIXTURES = [json.loads(l) for l in (Path(__file__).parent / "fixtures" / "events.jsonl").read_text().splitlines() if l]


def test_flatten_matches_typescript():
    good = [f for f in FIXTURES if "expected" in f]
    assert len(good) >= 300
    for f in good:
        assert flatten(f["raw"]) == f["expected"]


def test_invalid_events_rejected_for_the_same_reason():
    bad = [f for f in FIXTURES if "reason" in f]
    assert bad
    for f in bad:
        with pytest.raises(Invalid) as e:
            flatten(f["raw"])
        assert str(e.value) == f["reason"]


def frames(events):
    out = ":ok\n\n"
    for i, e in enumerate(events):
        sse_id = json.dumps([{"topic": "eqiad.mediawiki.recentchange", "partition": 0, "timestamp": 1791547200000 + i}, {"topic": "codfw.mediawiki.recentchange", "partition": 0, "offset": -1}])
        out += f"event: message\nid: {sse_id}\ndata: {json.dumps(e)}\n\n"
    return out


def test_sse_parser_handles_any_chunking():
    events = [f["raw"] for f in FIXTURES[:50]]
    text = frames(events).replace("\n", "\r\n")
    rnd = random.Random(7)
    for _ in range(20):
        p = SseParser()
        got = []
        i = 0
        while i < len(text):
            n = rnd.randint(1, 300)
            got += p.push(text[i:i + n])
            i += n
        assert [json.loads(d) for _, d, _ in got] == events
        assert position_from_id(got[-1][2]) == 1791547200000 + 49


def test_avro_round_trip_through_schema_registry():
    from confluent_kafka.schema_registry._sync.mock_schema_registry_client import MockSchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    reg = MockSchemaRegistryClient({"url": "mock://registry"})
    ser = AvroSerializer(reg, SCHEMA)
    de = AvroDeserializer(reg)
    ctx = SerializationContext("wikimedia.recentchange", MessageField.VALUE)
    for f in FIXTURES[:100]:
        if "expected" not in f:
            continue
        data = ser(f["expected"], ctx)
        assert data[0] == 0  # Confluent wire format: magic byte, then the 4-byte schema id
        assert de(data, ctx) == f["expected"]
    assert reg.get_subjects() == ["wikimedia.recentchange-value"]


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.sent.append({"topic": topic, "key": key, "value": value, "headers": dict(headers or [])})

    def poll(self, _t):
        return 0


def test_invalid_events_go_to_the_dead_letter_topic():
    fp = FakeProducer()
    pipe = Pipeline(fp, lambda row, ctx: json.dumps(row).encode(), "wikimedia.recentchange", "wikimedia.recentchange.dlq")
    assert pipe.handle(json.dumps(FIXTURES[0]["raw"])) is True
    assert pipe.handle("{not json", "id-1") is False
    assert pipe.handle(json.dumps({"meta": {"id": "x"}}), "id-2") is False
    assert [s["topic"] for s in fp.sent] == ["wikimedia.recentchange", "wikimedia.recentchange.dlq", "wikimedia.recentchange.dlq"]
    assert fp.sent[0]["key"] == FIXTURES[0]["expected"]["wiki"].encode()
    assert fp.sent[1]["headers"]["error.reason"] == b"invalid JSON"
    assert fp.sent[2]["headers"]["error.reason"] == b"meta.id is not a UUID"
    assert pipe.dead == 2 and pipe.produced == 1


def test_keys_land_on_the_same_partitions_as_the_java_client():
    """Produces to librdkafka's in-process mock cluster and checks every
    delivery report against kafka-python's port of Java's murmur2 partitioner."""
    from confluent_kafka import Producer
    from kafka.partitioner.default import murmur2

    conf = producer_config("")
    del conf["bootstrap.servers"]
    conf["test.mock.num.brokers"] = 3
    conf["topic.metadata.refresh.interval.ms"] = 200
    p = Producer(conf)
    p.produce("warmup", key=b"x", value=b"x")
    p.flush(10)
    nparts = len(p.list_topics("wikimedia.recentchange", timeout=10).topics["wikimedia.recentchange"].partitions)
    assert nparts > 1
    reports = []
    keys = sorted({f["expected"]["wiki"] for f in FIXTURES if "expected" in f})
    for k in keys:
        p.produce("wikimedia.recentchange", key=k.encode(), value=b"v", on_delivery=lambda err, msg: reports.append((err, msg.key().decode(), msg.partition())))
    p.flush(20)
    assert len(reports) == len(keys) and all(err is None for err, _, _ in reports)
    for _, key, part in reports:
        assert part == (murmur2(key.encode()) & 0x7FFFFFFF) % nparts, key

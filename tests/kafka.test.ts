// Kafka client logic: the default partitioner and group assignment strategies.
import { assign, moved } from "../shared/kafka/assignors.ts";
import { murmur2, partitionFor, toPositive } from "../shared/kafka/partitioner.ts";
import { check, section } from "./assert.ts";

section("murmur2 matches Kafka's Java client");
const enc = new TextEncoder();
// From Kafka's own UtilsTest.testMurmur2 (signed Java ints).
const java: Record<string, number> = {
  "21": -973932308,
  foobar: -790332482,
  "a-little-bit-long-string": -985981536,
  "a-little-bit-longer-string": -1486304829,
  lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8: -58897971,
  abc: 479470107,
};
for (const [k, v] of Object.entries(java)) check(murmur2(enc.encode(k)) === v, `murmur2("${k}") = ${v}`);
// Unsigned values from kafka-python's kafka.partitioner.default.murmur2 (tests/kafka_vectors.py).
const py: Record<string, number> = { enwiki: 1338253941, wikidatawiki: 3087528659, commonswiki: 938807123, hiwiki: 4022695599, "मुंबई": 1384912785, "": 275646681, a: 2731586172, ab: 316155434, abcd: 2971317748, "Apache Flink": 2237518500 };
for (const [k, v] of Object.entries(py)) check(murmur2(enc.encode(k)) >>> 0 === v, `murmur2("${k}") = ${v} (unsigned)`);
check(partitionFor("enwiki", 6) === toPositive(1338253941) % 6, "partitionFor uses toPositive(...) % partitions");

section("Range and round-robin assignment");
const r = assign("range", ["c2", "c1"], 5);
check(JSON.stringify(r) === JSON.stringify({ c1: [0, 1, 2], c2: [3, 4] }), `range: ${JSON.stringify(r)}`);
const rr = assign("roundrobin", ["c1", "c2"], 5);
check(JSON.stringify(rr) === JSON.stringify({ c1: [0, 2, 4], c2: [1, 3] }), `roundrobin: ${JSON.stringify(rr)}`);
const idle = assign("range", ["a", "b", "c", "d"], 3);
check(idle.d.length === 0, "more consumers than partitions leaves a consumer idle");

section("Sticky assignment keeps partitions in place");
const s3 = assign("sticky", ["c1", "c2", "c3"], 12);
check(Object.values(s3).every((p) => p.length === 4), "12 partitions over 3 consumers: 4 each");
const s2 = assign("sticky", ["c1", "c3"], 12, s3);
check(Object.values(s2).every((p) => p.length === 6) && moved(s3, s2) === 4, `when c2 leaves only its 4 partitions move (moved=${moved(s3, s2)})`);
const r3 = assign("range", ["c1", "c2", "c3"], 12);
const r2 = assign("range", ["c2", "c3"], 12);
const s2b = assign("sticky", ["c2", "c3"], 12, s3);
check(moved(r3, r2) === 6 && moved(s3, s2b) === 4, `when c1 leaves, range moves ${moved(r3, r2)} partitions and sticky only ${moved(s3, s2b)}`);
const s4 = assign("sticky", ["c1", "c2", "c3", "c4"], 12, s3);
check(Object.values(s4).every((p) => p.length === 3) && moved(s3, s4) === 3, `a new consumer takes one partition from each, nothing else moves (moved=${moved(s3, s4)})`);

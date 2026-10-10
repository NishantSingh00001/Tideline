// Kafka's default partitioner for keyed records:
//   partition = toPositive(murmur2(keyBytes)) % numPartitions
// A line-by-line port of org.apache.kafka.common.utils.Utils.murmur2, so keys
// land on the same partitions as with the Java client (and librdkafka's
// `partitioner=murmur2_random`).

const encoder = new TextEncoder();

export function murmur2(data: Uint8Array): number {
  const length = data.length;
  const seed = 0x9747b28c;
  const m = 0x5bd1e995;
  const r = 24;
  let h = (seed ^ length) | 0;
  const length4 = length >> 2;
  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m);
    h ^= k;
  }
  const tail = length & ~3;
  const rem = length % 4;
  if (rem >= 3) h ^= (data[tail + 2] & 0xff) << 16;
  if (rem >= 2) h ^= (data[tail + 1] & 0xff) << 8;
  if (rem >= 1) {
    h ^= data[tail] & 0xff;
    h = Math.imul(h, m);
  }
  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;
  return h | 0;
}

export const toPositive = (n: number) => n & 0x7fffffff;

/** The partition a keyed record goes to. */
export function partitionFor(key: string, numPartitions: number): number {
  return toPositive(murmur2(encoder.encode(key))) % numPartitions;
}

import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BatchBuilder,
  BatchKind,
  type BatchRecord,
  BrokerResponseError,
  ClientAuthenticationError,
  codecAvailable,
  Compression,
  ErrorCode,
  GaveUpError,
  InsecureTransportError,
  LotseClient,
  OffsetAt,
  OffsetOutOfRangeError,
  ResourceExistsError,
  UnknownStreamError,
} from "../src/index.js";
import { crc32c } from "../src/record/crc32c.js";
import { putU32 } from "../src/record/bytes.js";
import { formatAddress } from "../src/net/address.js";
import { PlainCluster } from "./support/cluster.js";
import { pinLeader, retryIfLeaderMoved, stable } from "./support/failover.js";
import { binary, Certs, NodeProcess, retry, skipWithoutBinary } from "./support/lotse.js";

/**
 * Three real `lotse` processes on loopback (plain TCP, no authentication) and a client that creates streams, produces,
 * fetches, and keeps going when the leader of a partition is killed; and one TLS node. Skipped, with the reason, when no
 * `lotse` executable is found (set LOTSE_BIN).
 */

const skip = skipWithoutBinary();
const text = (r: BatchRecord): string => Buffer.from(r.value as Uint8Array).toString();

describe("a client against three real nodes", { skip }, () => {
  let cluster: PlainCluster;
  let client: LotseClient;

  before(async () => {
    cluster = await PlainCluster.started(binary(), 3);
    client = new LotseClient({
      bootstrap: cluster.bootstrap,
      plaintextOnLoopback: true,
      requestTimeoutMs: 5_000,
      retryForMs: 60_000,
    });
  });

  after(async () => {
    client?.close();
    await cluster?.stop();
  });

  const probe = async (node: number): Promise<void> => {
    // Serving again: the node answers metadata for itself.
    const p = new LotseClient({
      bootstrap: [formatAddress(cluster.address(node))],
      plaintextOnLoopback: true,
      requestTimeoutMs: 5_000,
      retryForMs: 60_000,
    });
    try {
      await p.metadata();
    } finally {
      p.close();
    }
  };

  async function leaderOf(stream: string, partition: number): Promise<number> {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      const m = await client.metadata(stream);
      for (const r of m.resources) {
        for (const p of r.partitions) {
          if (r.name === stream && p.partition === partition && p.leader !== 0) {
            return p.leader;
          }
        }
      }
      await sleep(100);
    }
    throw new Error(`no leader for ${stream}/${partition}\n${cluster.log(1)}`);
  }

  /** Reads a partition from its start until `count` records are seen. */
  async function readAll(c: LotseClient, stream: string, partition: number, count: number): Promise<BatchRecord[]> {
    const out: BatchRecord[] = [];
    let offset = 0n;
    const deadline = Date.now() + 60_000;
    while (out.length < count && Date.now() < deadline) {
      const f = await c.fetch(stream, partition, offset, 200);
      out.push(...f.committedMessages());
      offset = f.nextOffset(offset);
    }
    return out;
  }

  test("a stream is created, produced to and read back", async () => {
    const id = await client.createStream("orders", 2, 3);
    assert.notEqual(id, 0n);
    await assert.rejects(client.createStream("orders", 2, 3), (e: unknown) => {
      assert.ok(e instanceof ResourceExistsError);
      assert.equal(e.resourceId, id, "creating it again says which one exists");
      return true;
    });

    const before = BigInt(Date.now());
    const offsets: bigint[] = [];
    for (let i = 0; i < 10; i++) {
      const r = await client.produce("orders", i % 2, {
        value: `order-${i}`,
        key: `k${i}`,
        headers: [{ name: "n", value: String(i) }],
      });
      offsets.push(r.baseOffset);
    }
    const after = BigInt(Date.now());

    for (let partition = 0; partition < 2; partition++) {
      const got = await readAll(client, "orders", partition, 5);
      assert.equal(got.length, 5);
      for (let j = 0; j < 5; j++) {
        const i = 2 * j + partition;
        const rec = got[j] as BatchRecord;
        assert.equal(text(rec), `order-${i}`);
        assert.equal(Buffer.from(rec.key as Uint8Array).toString(), `k${i}`);
        assert.equal(Buffer.from(rec.headers[0]?.value as Uint8Array).toString(), String(i));
        assert.ok(rec.timestamp >= before && rec.timestamp <= after, "stamped with the time of the produce");
        assert.equal(rec.offset, offsets[i], "the offset the produce returned");
      }
    }
  });

  test("a long poll waits at the broker and an offset past the end is out of range", async () => {
    // The log holds control entries as well as records, so the end is found by reading, not by counting.
    let end = 0n;
    for (let f = await client.fetch("orders", 0, 0n, 50); f.records.length > 0; f = await client.fetch("orders", 0, end, 50)) {
      end = f.nextOffset(end);
    }
    const start = performance.now();
    const f = await client.fetch("orders", 0, end, 300);
    const ms = performance.now() - start;
    // The broker may append an entry of its own meanwhile (a membership change), which ends the wait early: either
    // something came, or the broker held the request for the whole wait.
    assert.ok(f.records.length > 0 || ms >= 200, `returned empty after only ${ms} ms`);
    if (f.records.length === 0) {
      assert.equal(f.nextOffset(end), end, "nothing read: stays where it was");
    }
    await assert.rejects(client.fetch("orders", 0, 1_000_000n, 10), (e: unknown) => {
      assert.ok(e instanceof OffsetOutOfRangeError);
      assert.equal(e.logStart, 0n);
      return true;
    });
  });

  test("an unknown stream is reported at once", async () => {
    const start = performance.now();
    await assert.rejects(client.produce("nope", 0, { value: "x" }), UnknownStreamError);
    assert.ok(performance.now() - start < 10_000);
    await assert.rejects(client.listOffset("nope", 0, OffsetAt.LATEST), UnknownStreamError);
  });

  test("ListOffsets finds the start, the end and a time", async () => {
    // The end of a log moves when a new leader appends a note of its own term, so the scenario runs again, with a new
    // stream, if a leader moved while it ran; with every leader where it was, a failure is a failure.
    await retryIfLeaderMoved(client, async (steady, attempt) => {
      const times = `times${attempt === 1 ? "" : `-${attempt}`}`;
      await client.createStream(times, 1, 3);
      await pinLeader(client, times);
      await steady.watch(times);
      const a = await client.produce(times, 0, [{ value: "a1", timestamp: 1_000 }, { value: "a2", timestamp: 1_500 }]);
      const b = await client.produce(times, 0, { value: "b", timestamp: 2_000 });
      const c = await client.produce(times, 0, { value: "c", timestamp: 3_000 });

      // The log starts with entries of the broker's own, so the first offset is not 0 and not a message.
      const earliest = await client.listOffset(times, 0, OffsetAt.EARLIEST);
      assert.ok(earliest <= a.baseOffset, `earliest ${earliest} before the first batch ${a.baseOffset}`);
      // A new leader appends a note of its own term, which can fall between listing the end and reading to it: the two
      // are compared once the end has stopped moving.
      const readEnd = async (): Promise<bigint> => {
        let end = 0n;
        for (let f = await client.fetch(times, 0, 0n, 50); f.records.length > 0; f = await client.fetch(times, 0, end, 50)) {
          end = f.nextOffset(end);
        }
        return end;
      };
      const latest = await stable(() => client.listOffset(times, 0, OffsetAt.LATEST));
      assert.ok(latest > c.baseOffset);
      assert.equal(await stable(readEnd), latest, "the end a reader sees");

      // By batches: the first batch with a record at or after the time.
      assert.equal(await client.listOffset(times, 0, OffsetAt.time(0)), a.baseOffset);
      assert.equal(await client.listOffset(times, 0, OffsetAt.time(1_000)), a.baseOffset);
      assert.equal(await client.listOffset(times, 0, OffsetAt.time(1_501)), b.baseOffset, "a's newest is 1500");
      assert.equal(await client.listOffset(times, 0, OffsetAt.time(2_000)), b.baseOffset);
      assert.equal(await client.listOffset(times, 0, OffsetAt.time(2_001)), c.baseOffset);
      assert.equal(await stable(() => client.listOffset(times, 0, OffsetAt.time(3_001))), latest, "none yet: the latest offset");
    });
  });

  test("readRecords reads a partition to its end as an async iterator", async () => {
    await client.createStream("iter", 1, 3);
    for (let i = 0; i < 5; i++) {
      await client.produce("iter", 0, { value: `i-${i}` });
    }
    const got: string[] = [];
    for await (const r of client.readRecords("iter", 0, { untilEnd: true, maxWaitMs: 50 })) {
      got.push(text(r));
    }
    assert.deepEqual(got, ["i-0", "i-1", "i-2", "i-3", "i-4"]);
    // From a given offset on.
    const second = (await readAll(client, "iter", 0, 2))[1] as BatchRecord;
    const rest: string[] = [];
    for await (const r of client.readRecords("iter", 0, { from: second.offset, untilEnd: true, maxWaitMs: 50 })) {
      rest.push(text(r));
    }
    assert.deepEqual(rest, ["i-1", "i-2", "i-3", "i-4"]);
  });

  for (const [codec, name] of [
    [Compression.LZ4, "lz4"],
    [Compression.ZSTD, "zstd"],
  ] as const) {
    test(`compressed batches (${name}) are accepted by the node and read back`, { skip: codecAvailable(codec) ? false : `SKIPPED: no ${name} in this Node.js` }, async () => {
      const stream = `packed-${name}`;
      await client.createStream(stream, 1, 3);
      const make = (n: number): Uint8Array => {
        const b = new BatchBuilder({
          attributes: { compression: codec, transactional: false, control: false, kind: BatchKind.DATA },
          producerId: 0n,
          producerEpoch: 0,
          baseSequence: -1,
        });
        for (let i = 0; i < 20; i++) {
          b.push(BigInt(Date.now()), Buffer.from(`k${n}-${i}`), Buffer.from(`value ${n} ${i} `.repeat(40)));
        }
        return b.finish();
      };
      // The node decodes a compressed batch once when it is produced and refuses one that does not decode: so this
      // is the real broker's libzstd or lz4_flex reading what this SDK wrote.
      const first = await client.produceBatch(stream, 0, make(0));
      await client.produceBatch(stream, 0, make(1));
      const f = await client.fetch(stream, 0, first.baseOffset, 500);
      const batches = f.committedBatches();
      assert.ok(batches.length >= 1);
      assert.equal(batches[0]?.attributes.compression, codec, "stored and served compressed, as sent");
      const got = (await readAll(client, stream, 0, 40)).map(text);
      assert.equal(got.length, 40);
      assert.equal(got[0], `value 0 0 `.repeat(40));
      assert.equal(got[39], `value 1 19 `.repeat(40));
    });
  }

  test("a compressed batch that does not decode is refused by the node", { skip: false }, async () => {
    await client.createStream("garbled", 1, 3);
    const b = new BatchBuilder({
      attributes: { compression: Compression.LZ4, transactional: false, control: false, kind: BatchKind.DATA },
      producerId: 0n,
      producerEpoch: 0,
      baseSequence: -1,
    });
    b.push(1n, null, new Uint8Array(5_000).fill(7));
    const bytes = b.finish();
    bytes.fill(0x5a, 65 + 8); // the frame's blocks are overwritten; the checksum is made to fit
    putU32(bytes, 21, crc32c(bytes, 25, bytes.length));
    assert.ok(Batch.parse(bytes).batch.attributes.compression === Compression.LZ4);
    await assert.rejects(client.produceBatch("garbled", 0, bytes), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError, String(e));
      assert.equal(e.code, ErrorCode.CORRUPT_BATCH);
      return true;
    });
  });

  test("killing the leader mid-stream neither loses nor duplicates an idempotent produce", async () => {
    await client.createStream("idem", 1, 3);
    const leader = await leaderOf("idem", 0);
    const total = 150;
    let done = 0;
    const killer = (async () => {
      while (done < 40) {
        await sleep(2);
      }
      await cluster.kill(leader);
    })();
    const offsets: bigint[] = [];
    for (let i = 0; i < total; i++) {
      offsets.push((await client.produce("idem", 0, { value: `m-${i}` })).baseOffset);
      done++;
    }
    await killer;

    const got = await readAll(client, "idem", 0, total);
    assert.deepEqual(
      got.map(text),
      Array.from({ length: total }, (_, i) => `m-${i}`),
      "every message once, in order",
    );
    assert.equal(new Set(offsets).size, total, "each produce got its own offset");
    for (let i = 1; i < total; i++) {
      assert.ok((offsets[i] as bigint) > (offsets[i - 1] as bigint), "offsets rise");
    }
    assert.notEqual(await leaderOf("idem", 0), leader, "another node leads now");
    await cluster.restart(leader);
    await probe(leader);
  });

  test("a client follows the new leader after a failover", async () => {
    await client.createStream("failover", 1, 3);
    await client.produce("failover", 0, { value: "before" });
    const old = await leaderOf("failover", 0);
    assert.equal(client.routeOf("failover", 0)?.leader, old);

    await cluster.kill(old);
    await client.produce("failover", 0, { value: "after" });
    assert.notEqual(client.routeOf("failover", 0)?.leader, old, "the route moved to the new leader");

    assert.deepEqual((await readAll(client, "failover", 0, 2)).map(text), ["before", "after"]);
    await cluster.restart(old);
    await probe(old);
    await client.produce("failover", 0, { value: "again" });
    assert.equal((await readAll(client, "failover", 0, 3)).length, 3);
  });

  test("a client without idempotence writes too and never writes twice", async () => {
    await client.createStream("plain", 1, 3);
    const plain = new LotseClient({
      bootstrap: cluster.bootstrap,
      plaintextOnLoopback: true,
      idempotent: false,
      retryForMs: 60_000,
    });
    try {
      for (let i = 0; i < 5; i++) {
        await plain.produce("plain", 0, { value: `p-${i}` });
      }
      assert.deepEqual((await readAll(plain, "plain", 0, 5)).map(text), ["p-0", "p-1", "p-2", "p-3", "p-4"]);
    } finally {
      plain.close();
    }
  });

  test("a named producer is fenced by the next instance with its name", async () => {
    await client.createStream("named", 1, 3);
    const open = (): LotseClient =>
      new LotseClient({ bootstrap: cluster.bootstrap, plaintextOnLoopback: true, producerName: "writer", retryForMs: 60_000 });
    const first = open();
    const second = open();
    try {
      await first.produce("named", 0, { value: "one" });
      await second.produce("named", 0, { value: "two" }); // raises the epoch
      await assert.rejects(first.produce("named", 0, { value: "stale" }), (e: unknown) => {
        assert.ok(e instanceof BrokerResponseError);
        assert.equal(e.code, ErrorCode.PRODUCER_FENCED);
        return true;
      });
      await second.produce("named", 0, { value: "three" });
      assert.deepEqual((await readAll(second, "named", 0, 3)).map(text), ["one", "two", "three"]);
    } finally {
      first.close();
      second.close();
    }
  });
});

describe("a client against one real node with TLS 1.3 and a required login", { skip }, () => {
  let certs: Certs;
  let node: NodeProcess;

  before(async () => {
    certs = await Certs.make(binary());
    node = NodeProcess.start(binary(), certs);
  });

  after(async () => {
    await node?.stop();
  });

  test("an admin with a certificate creates, writes and reads", async () => {
    const admin = new LotseClient({
      bootstrap: [formatAddress(certs.address)],
      tls: await certs.adminTls(),
      retryForMs: 60_000,
    });
    try {
      await retry("the node to create a stream", node, () => admin.createStream("secure", 1, 1));
      await admin.produce("secure", 0, { value: "over tls" });
      // The log starts with entries of the broker's own, which a read returns without any message in them.
      const got: BatchRecord[] = [];
      let offset = 0n;
      for (let i = 0; i < 50 && got.length === 0; i++) {
        const f = await admin.fetch("secure", 0, offset, 100);
        got.push(...f.committedMessages());
        offset = f.nextOffset(offset);
      }
      assert.equal(text(got[0] as BatchRecord), "over tls");
    } finally {
      admin.close();
    }
  });

  test("a client without a certificate is refused at once, not retried until the time runs out", async () => {
    const stranger = new LotseClient({
      bootstrap: [formatAddress(certs.address)],
      tls: await certs.anonymousTls(),
      retryForMs: 60_000,
    });
    try {
      const start = performance.now();
      await assert.rejects(stranger.produce("secure", 0, { value: "x" }), ClientAuthenticationError);
      assert.ok(performance.now() - start < 20_000, "not retried for a minute");
    } finally {
      stranger.close();
    }
  });

  test("a client that asks for no transport is refused before anything is sent", async () => {
    const bare = new LotseClient({ bootstrap: [formatAddress(certs.address)] });
    try {
      await assert.rejects(bare.metadata("secure"), InsecureTransportError);
    } finally {
      bare.close();
    }
  });

  test("a client gives up when no node is there", async () => {
    const lost = new LotseClient({ bootstrap: ["127.0.0.1:1"], plaintextOnLoopback: true, retryForMs: 200 });
    try {
      await assert.rejects(lost.metadata(), GaveUpError);
    } finally {
      lost.close();
    }
  });
});

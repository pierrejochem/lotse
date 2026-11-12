import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  type AbortedTxn,
  Batch,
  BatchBuilder,
  BatchKind,
  BrokerResponseError,
  Compression,
  concat,
  ControlType,
  ErrorCode,
  Fetched,
  GaveUpError,
  Isolation,
  LotseClient,
  marker,
  OffsetAt,
  OffsetOutOfRangeError,
  type Request,
} from "../src/index.js";
import { fromHex } from "../src/record/bytes.js";
import * as fx from "./support/fx.js";
import { closeAll, type FakeNode, startNodes } from "./support/fake-node.js";
import { field, lines } from "./vectors.js";

/** Fetch requests, retries, ListOffsets, and what a read_committed reader sees. */

// ---- batches as the broker stores them ----

/** A batch of data records at `baseOffset`; producer 0 is not idempotent. */
function data(baseOffset: number, producer: number, transactional: boolean, ...values: string[]): Uint8Array {
  const b = new BatchBuilder();
  for (const v of values) {
    b.push(1_000n, null, Buffer.from(v));
  }
  const bytes = b.finish();
  if (producer !== 0) {
    Batch.setProducer(bytes, BigInt(producer), 1, 0);
  }
  if (transactional) {
    Batch.setTransactional(bytes, true);
  }
  Batch.stamp(bytes, BigInt(baseOffset), 1n);
  return bytes;
}

function mark(baseOffset: number, producer: number, type: ControlType): Uint8Array {
  const bytes = marker(BigInt(producer), 1, type, 1, 1_000n);
  Batch.stamp(bytes, BigInt(baseOffset), 1n);
  return bytes;
}

const aborted = (producerId: number, firstOffset: number): AbortedTxn => ({
  producerId: BigInt(producerId),
  firstOffset: BigInt(firstOffset),
});

const fetchedOf = (records: Uint8Array, abortedTxns: readonly AbortedTxn[] = []): Fetched =>
  new Fetched(records, 100n, 100n, 0n, abortedTxns);

const values = (f: Fetched): string[] => f.committedMessages().map((r) => Buffer.from(r.value as Uint8Array).toString());

describe("what a read_committed reader sees", () => {
  test("it skips aborted transactions and markers", () => {
    const records = concat([
      data(0, 0, false, "plain"),
      data(1, 7, true, "aborted-1"),
      data(2, 8, true, "committed"),
      data(3, 7, true, "aborted-2"),
      mark(4, 8, ControlType.COMMIT),
      mark(5, 7, ControlType.ABORT),
      data(6, 7, true, "later transaction of the same producer"),
      mark(7, 7, ControlType.COMMIT),
    ]);
    const f = fetchedOf(records, [aborted(7, 1)]);
    assert.deepEqual(values(f), ["plain", "committed", "later transaction of the same producer"]);
  });

  test("the aborted list need not arrive in order", () => {
    // The broker lists aborted transactions by producer; the reader must take them by first offset.
    const records = concat([
      data(1, 7, true, "aborted-7"),
      mark(2, 7, ControlType.ABORT),
      data(10, 9, true, "aborted-9"),
      mark(11, 9, ControlType.ABORT),
      data(12, 9, true, "kept"),
    ]);
    assert.deepEqual(values(fetchedOf(records, [aborted(9, 10), aborted(7, 1)])), ["kept"]);
  });

  test("an aborted producer is skipped only from its first offset", () => {
    // The same producer wrote a committed transaction before the aborted one began.
    const records = concat([
      data(0, 7, true, "earlier, committed"),
      mark(1, 7, ControlType.COMMIT),
      data(2, 7, true, "aborted"),
      mark(3, 7, ControlType.ABORT),
    ]);
    assert.deepEqual(values(fetchedOf(records, [aborted(7, 2)])), ["earlier, committed"]);
  });

  test("only transactional batches of an aborted producer are skipped", () => {
    const records = concat([
      data(1, 7, true, "aborted"),
      data(2, 7, false, "not transactional"),
      mark(3, 7, ControlType.ABORT),
    ]);
    assert.deepEqual(values(fetchedOf(records, [aborted(7, 1)])), ["not transactional"]);
  });

  test("an abort of one producer does not end the skipping of another", () => {
    const records = concat([
      data(1, 7, true, "a7"),
      data(2, 8, true, "a8"),
      mark(3, 7, ControlType.ABORT),
      data(4, 8, true, "a8 again"),
      mark(5, 8, ControlType.ABORT),
    ]);
    assert.deepEqual(values(fetchedOf(records, [aborted(7, 1), aborted(8, 2)])), []);
  });

  test("a commit marker does not end the skipping", () => {
    // Cannot happen for one producer in one transaction, but the rule is "up to its ABORT marker".
    const records = concat([
      data(1, 7, true, "a"),
      mark(2, 7, ControlType.COMMIT),
      data(3, 7, true, "b"),
      mark(4, 7, ControlType.ABORT),
    ]);
    assert.deepEqual(values(fetchedOf(records, [aborted(7, 1)])), []);
  });

  test("without aborted transactions every data batch is seen", () => {
    const records = concat([data(0, 8, true, "x", "y"), mark(2, 8, ControlType.COMMIT), data(3, 0, false, "z")]);
    assert.deepEqual(values(fetchedOf(records)), ["x", "y", "z"]);
    assert.equal(fetchedOf(records).committedBatches().length, 2, "the marker is not a batch of data");
  });

  test("a control batch of a Raft kind is skipped, not an error", () => {
    const b = new BatchBuilder({
      attributes: { compression: Compression.NONE, transactional: false, control: true, kind: BatchKind.DATA },
      producerId: 0n,
      producerEpoch: 0,
      baseSequence: -1,
    });
    b.push(1n, new Uint8Array([0, 0, 2, 0]), new Uint8Array([1, 2, 3]));
    const raft = b.finish();
    Batch.stamp(raft, 0n, 1n);
    assert.deepEqual(values(fetchedOf(concat([raft, data(1, 0, false, "v")]))), ["v"]);
  });

  test("records that do not parse are a corrupt batch", () => {
    const bytes = data(0, 0, false, "x").slice();
    bytes[bytes.length - 1] = (bytes[bytes.length - 1] as number) ^ 1;
    assert.throws(
      () => fetchedOf(bytes).committedBatches(),
      (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.CORRUPT_BATCH,
    );
  });

  test("the next offset follows the last batch", () => {
    const records = concat([data(0, 0, false, "a", "b", "c"), data(3, 0, false, "d")]);
    assert.equal(fetchedOf(records).nextOffset(0n), 4n);
    assert.equal(fetchedOf(new Uint8Array(0)).nextOffset(9n), 9n, "nothing read: where it was");
    assert.equal(fetchedOf(concat([records, new Uint8Array([1, 2, 3])])).nextOffset(0n), 4n, "stops at a batch that does not parse");
  });

  test("the fetched records are available without filtering", () => {
    const records = concat([data(0, 7, true, "t"), mark(1, 7, ControlType.ABORT)]);
    const f = fetchedOf(records, [aborted(7, 0)]);
    assert.deepEqual(f.records, records);
    assert.equal(f.committedBatches().length, 0);
    assert.equal(Batch.parseAll(f.records).length, 2);
  });

  // ---- batches the Rust side wrote ----

  const vector = (label: string): Uint8Array => {
    for (const v of lines("batches.hex")) {
      if (field(v, 0) === label) {
        return fromHex(field(v, 1));
      }
    }
    throw new Error(`no vector ${label}`);
  };

  test("control batches written by Rust are skipped and its transactional data is kept", () => {
    const records = concat([vector("transactional"), vector("raft-noop"), vector("raft-config"), vector("control-commit")]);
    const batches = fetchedOf(records).committedBatches();
    assert.equal(batches.length, 1);
    assert.equal(batches[0]?.baseOffset, 200n);
  });

  test("a transaction of Rust's producer stays hidden until its own abort marker", () => {
    // The abort marker in the vectors belongs to producer 18, so producer 17's transaction is still open.
    const records = concat([vector("transactional"), vector("control-abort")]);
    assert.equal(fetchedOf(records, [aborted(17, 200)]).committedBatches().length, 0);
    assert.equal(fetchedOf(records).committedBatches().length, 1);
  });
});

describe("fetch and list offsets against scripted brokers", () => {
  let a: FakeNode;
  let b: FakeNode;
  let client: LotseClient | undefined;

  beforeEach(async () => {
    [a, b] = (await startNodes(2)) as [FakeNode, FakeNode];
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll([a, b]);
  });

  const only = (n: FakeNode) =>
    fx.metadata([n], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1)));

  test("the fetch request asks for committed data from the route's leader term", async () => {
    a.on((node, request) =>
      request.type === "Metadata"
        ? only(node)
        : fx.fetched(fx.STREAM_ID, 0, {
            records: data(10, 0, false, "v"),
            highWatermark: 12,
            lastStable: 11,
            logStart: 3,
            aborted: [aborted(4, 5)],
          }),
    );
    client = new LotseClient(fx.config([a]));
    const f = await client.fetch("s", 0, 10n, 250);

    const sent = a.requests("Fetch")[0] as Extract<Request, { type: "Fetch" }>;
    assert.equal(sent.maxWaitMs, 250);
    assert.equal(sent.minBytes, 1);
    assert.equal(sent.maxBytes, 4 << 20);
    assert.equal(sent.isolation, Isolation.COMMITTED);
    assert.equal(sent.resources[0]?.resourceId, fx.STREAM_ID);
    const p = sent.resources[0]?.partitions[0];
    assert.equal(p?.partition, 0);
    assert.equal(p?.fetchOffset, 10n);
    assert.equal(p?.leaderTerm, 5n, "the term of the route");
    assert.equal(p?.maxBytes, 1 << 20);

    assert.equal(f.highWatermark, 12n);
    assert.equal(f.lastStableOffset, 11n);
    assert.equal(f.logStartOffset, 3n);
    assert.deepEqual(f.aborted, [aborted(4, 5)]);
    assert.deepEqual(values(f), ["v"]);
  });

  test("fetchUncommitted asks for the high watermark", async () => {
    a.on((node, request) => (request.type === "Metadata" ? only(node) : fx.fetched(fx.STREAM_ID, 0)));
    client = new LotseClient(fx.config([a]));
    await client.fetchUncommitted("s", 0, 0n, 1);
    assert.equal(a.requests("Fetch")[0]?.isolation, Isolation.UNCOMMITTED);
  });

  test("an offset out of range says where the partition starts and is not retried", async () => {
    a.on((node, request) =>
      request.type === "Metadata"
        ? only(node)
        : fx.fetched(fx.STREAM_ID, 0, { code: ErrorCode.OFFSET_OUT_OF_RANGE, highWatermark: 50, lastStable: 50, logStart: 40 }),
    );
    client = new LotseClient(fx.config([a]));
    await assert.rejects(client.fetch("s", 0, 3n, 1), (e: unknown) => {
      assert.ok(e instanceof OffsetOutOfRangeError);
      assert.equal(e.logStart, 40n);
      assert.equal(e.offset, 3n);
      assert.equal(e.stream, "s");
      return true;
    });
    assert.equal(a.count("Fetch"), 1, "not retried");
  });

  test("a retriable fetch error follows the leader hint", async () => {
    a.on((_node, request) =>
      request.type === "Metadata"
        ? fx.metadata([a, b], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2)))
        : fx.fetched(fx.STREAM_ID, 0, { code: ErrorCode.NOT_LEADER, hint: { leader: 2, term: 6n } }),
    );
    b.on(() => fx.fetched(fx.STREAM_ID, 0, { records: data(0, 0, false, "from b"), highWatermark: 1, lastStable: 1 }));
    client = new LotseClient(fx.config([a]));
    assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from b"]);
    assert.equal(a.count("Metadata"), 1, "the hint was enough");
    assert.equal(b.requests("Fetch")[0]?.resources[0]?.partitions[0]?.leaderTerm, 6n);
  });

  /** Node 1 leads in zone a, node 2 follows in zone b; node 2 answers with `followerCode`. */
  function zones(followerCode: number): void {
    const meta = {
      type: "Metadata" as const,
      brokers: [a, b].map((n, i) => ({ nodeId: n.id, host: "127.0.0.1", port: n.address.port, rack: ["a", "b"][i] ?? "" })),
      resources: [fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2))],
    };
    const handler = (node: FakeNode, request: Request) => {
      if (request.type === "Metadata") {
        return meta;
      }
      const code = node.id === 2 ? followerCode : ErrorCode.NONE;
      const records = code === ErrorCode.NONE ? data(0, 0, false, `from ${node.id}`) : undefined;
      return fx.fetched(fx.STREAM_ID, 0, { code, highWatermark: 1, lastStable: 1, ...(records ? { records } : {}) });
    };
    a.on(handler);
    b.on(handler);
  }

  test("a client in a zone fetches from the replica there", async () => {
    zones(ErrorCode.NONE);
    client = new LotseClient(fx.config([a], { zone: "b" }));
    assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from 2"]);
    assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from 2"]);
    assert.equal(a.count("Fetch"), 0, "the leader is in another zone");
    assert.equal((b.requests("Fetch")[0] as Extract<Request, { type: "Fetch" }>).allowFollower, true);
    await client.close();
    // In the leader's zone, or in one without a replica, the leader serves, as without a zone.
    for (const zone of ["a", "c", undefined]) {
      client = new LotseClient(fx.config([a], zone === undefined ? {} : { zone }));
      assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from 1"], `zone ${zone}`);
      await client.close();
    }
    assert.ok(a.requests("Fetch").every((r) => (r as Extract<Request, { type: "Fetch" }>).allowFollower !== true));
    client = undefined;
  });

  test("a replica that fails is left for the leader", async () => {
    // An older node answers NOT_LEADER; one whose log starts later, OFFSET_OUT_OF_RANGE.
    for (const code of [ErrorCode.NOT_LEADER, ErrorCode.OFFSET_OUT_OF_RANGE]) {
      zones(code);
      const before = b.count("Fetch");
      client = new LotseClient(fx.config([a], { zone: "b" }));
      assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from 1"]);
      assert.deepEqual(values(await client.fetch("s", 0, 0n, 1)), ["from 1"]);
      assert.equal(b.count("Fetch"), before + 1, `asked once, then left: ${code}`);
      await client.close();
    }
    client = undefined;
  });

  test("a fetch that never gets a leader gives up", async () => {
    a.on((node, request) =>
      request.type === "Metadata" ? fx.metadata([node], fx.stream("s", fx.STREAM_ID, fx.partition(0, 0, 0))) : null,
    );
    client = new LotseClient(fx.config([a], { retryForMs: 150 }));
    await assert.rejects(client.fetch("s", 0, 0n, 1), GaveUpError);
  });

  test("a fetch given up while waiting at the broker ends with GaveUp", async () => {
    a.on((node, request) => (request.type === "Metadata" ? only(node) : null));
    client = new LotseClient(fx.config([a], { retryForMs: 5_000 }));
    const abort = new AbortController();
    const pending = client.fetch("s", 0, 0n, 1_000, { signal: abort.signal });
    setTimeout(() => abort.abort(), 50);
    await assert.rejects(pending, GaveUpError);
  });

  test("readRecords yields the committed records and ends at the end when asked", async () => {
    let calls = 0;
    a.on((node, request) => {
      if (request.type === "Metadata") {
        return only(node);
      }
      if (request.type === "ListOffsets") {
        return fx.listed(fx.STREAM_ID, 0, 10);
      }
      calls++;
      const r = request as Extract<Request, { type: "Fetch" }>;
      const at = r.resources[0]?.partitions[0]?.fetchOffset;
      if (at === 10n) {
        return fx.fetched(fx.STREAM_ID, 0, { records: concat([data(10, 0, false, "a", "b"), mark(12, 7, ControlType.COMMIT)]) });
      }
      if (at === 13n) {
        return fx.fetched(fx.STREAM_ID, 0, { records: data(13, 0, false, "c") });
      }
      return fx.fetched(fx.STREAM_ID, 0);
    });
    client = new LotseClient(fx.config([a]));
    const got: string[] = [];
    for await (const r of client.readRecords("s", 0, { untilEnd: true, maxWaitMs: 1 })) {
      got.push(Buffer.from(r.value as Uint8Array).toString());
    }
    assert.deepEqual(got, ["a", "b", "c"]);
    assert.equal(calls, 3, "it went on from the offset after the last batch, the marker included");
  });

  test("readRecords ends quietly when its signal fires", async () => {
    a.on((node, request) => {
      if (request.type === "Metadata") {
        return only(node);
      }
      return fx.fetched(fx.STREAM_ID, 0);
    });
    client = new LotseClient(fx.config([a]));
    const abort = new AbortController();
    setTimeout(() => abort.abort(), 50);
    let n = 0;
    for await (const _ of client.readRecords("s", 0, { from: 0n, maxWaitMs: 5, signal: abort.signal })) {
      n++;
    }
    assert.equal(n, 0);
  });

  // ---- list offsets ----

  test("ListOffsets asks the leader with the committed isolation and the timestamp of the position", async () => {
    a.on((node, request) => (request.type === "Metadata" ? only(node) : fx.listed(fx.STREAM_ID, 0, 42)));
    client = new LotseClient(fx.config([a]));
    assert.equal(await client.listOffset("s", 0, OffsetAt.EARLIEST), 42n);
    await client.listOffset("s", 0, OffsetAt.LATEST);
    await client.listOffset("s", 0, OffsetAt.time(1_700_000_000_000));
    await client.listOffset("s", 0, OffsetAt.time(-5));
    const sent = a.requests("ListOffsets");
    assert.deepEqual(
      sent.map((r) => r.resources[0]?.partitions[0]?.timestamp),
      [-2n, -1n, 1_700_000_000_000n, 0n],
    );
    for (const r of sent) {
      assert.equal(r.isolation, Isolation.COMMITTED);
      assert.equal(r.resources[0]?.resourceId, fx.STREAM_ID);
    }
  });

  test("the offsets of ListOffsets are u64 bit patterns", async () => {
    a.on((node, request) => (request.type === "Metadata" ? only(node) : fx.listed(fx.STREAM_ID, 0, -1n)));
    client = new LotseClient(fx.config([a]));
    assert.equal(await client.listOffset("s", 0, OffsetAt.LATEST), -1n);
  });

  test("a retriable ListOffsets error follows the hint, another error is reported", async () => {
    a.on((_node, request) =>
      request.type === "Metadata"
        ? fx.metadata([a, b], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2)))
        : fx.listed(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER, { leader: 2, term: 5n }),
    );
    b.on(() => fx.listed(fx.STREAM_ID, 0, 7));
    client = new LotseClient(fx.config([a]));
    assert.equal(await client.listOffset("s", 0, OffsetAt.LATEST), 7n);
    assert.equal(a.count("Metadata"), 1, "the hint was enough");

    b.on(() => fx.listed(fx.STREAM_ID, 0, 0, ErrorCode.INVALID_REQUEST));
    await assert.rejects(client.listOffset("s", 0, OffsetAt.LATEST), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.INVALID_REQUEST);
      return true;
    });
  });

  test("OffsetAt prints its position", () => {
    assert.equal(String(OffsetAt.EARLIEST), "EARLIEST");
    assert.equal(String(OffsetAt.LATEST), "LATEST");
    assert.equal(String(OffsetAt.time(5)), "time(5)");
  });
});

import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  type AbortedTxn,
  Batch,
  BatchBuilder,
  BatchKind,
  BrokerResponseError,
  ClientError,
  Compression,
  concat,
  ControlType,
  ErrorCode,
  type ErrorCodeValue,
  Fetched,
  GaveUpError,
  type GroupIndex,
  Isolation,
  type LeaderHint,
  LotseClient,
  marker,
  OffsetAt,
  OffsetOutOfRangeError,
  type Request,
  type Response,
} from "../src/index.js";
import { Indexes, refused, zoneNode } from "../src/client/zone.js";
import { fromHex } from "../src/record/bytes.js";
import * as fx from "./support/fx.js";
import { closeAll, type FakeNode, type Heard, startNodes } from "./support/fake-node.js";
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
      if (request.type === "ListOffsets") {
        const partition = { partition: 0, error: code, offset: BigInt(node.id * 10), timestamp: -1n, leaderHint: null };
        return { type: "ListOffsets" as const, resources: [{ resourceId: fx.STREAM_ID, partitions: [partition] }] };
      }
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

  test("a client in a zone lists offsets at the replica there, and leaves one that lags", async () => {
    zones(ErrorCode.NONE);
    client = new LotseClient(fx.config([a], { zone: "b" }));
    assert.equal(await client.listOffset("s", 0, OffsetAt.LATEST), 20n, "the follower in zone b answers");
    assert.equal(a.count("ListOffsets"), 0);
    assert.equal((b.requests("ListOffsets")[0] as Extract<Request, { type: "ListOffsets" }>).allowFollower, true);
    await client.close();
    // A follower that lags too far answers NOT_LEADER (ADR 0116): asked once, then left for the leader.
    zones(ErrorCode.NOT_LEADER);
    const before = b.count("ListOffsets");
    client = new LotseClient(fx.config([a], { zone: "b" }));
    assert.equal(await client.listOffset("s", 0, OffsetAt.LATEST), 10n);
    assert.equal(await client.listOffset("s", 0, OffsetAt.LATEST), 10n);
    assert.equal(b.count("ListOffsets"), before + 1);
    assert.ok(a.requests("ListOffsets").every((r) => (r as Extract<Request, { type: "ListOffsets" }>).allowFollower !== true));
    await client.close();
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

// ---- zone reads: offsets and admin state in the client's zone, and back to the zone ----
//
// Node 1 (a) is in zone x and leads everything; nodes 2 (b) and 3 (c) are in zone y. Stream s (id 7) lives on node 1
// alone, stream t (id 8) on nodes 1 and 2. Node 1 coordinates, with all three voting.

describe("zone reads against scripted nodes", () => {
  let a: FakeNode;
  let b: FakeNode;
  let c: FakeNode;
  let client: LotseClient | undefined;
  /** What every node heard, in order across the nodes. */
  let heard: Heard[];

  beforeEach(async () => {
    [a, b, c] = (await startNodes(3)) as [FakeNode, FakeNode, FakeNode];
    heard = [];
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll([a, b, c]);
  });

  const HINT: LeaderHint = { leader: 1, term: 1n };
  const ZONES: Record<number, string> = { 1: "x", 2: "y", 3: "y" };

  /** Indexes from pairs of group and index. */
  const at = (...pairs: (number | bigint)[]): GroupIndex[] => {
    const out: GroupIndex[] = [];
    for (let i = 0; i < pairs.length; i += 2) {
      out.push({ group: BigInt(pairs[i] as number | bigint), index: BigInt(pairs[i + 1] as number | bigint) });
    }
    return out;
  };

  /** Indexes as text, "1:5 2:3", so what was heard compares as plain strings. */
  const text = (indexes: readonly GroupIndex[]): string => indexes.map((e) => `${e.group}:${e.index}`).join(" ");

  /** The metadata of the zone tests, listing the nodes `listed`. */
  const zoneMetadata = (...listed: FakeNode[]): Extract<Response, { type: "Metadata" }> => ({
    type: "Metadata",
    brokers: listed.map((n) => ({ nodeId: n.id, host: "127.0.0.1", port: n.address.port, rack: ZONES[n.id] ?? "" })),
    resources: [
      fx.stream("s", 7, fx.partition(0, 1, 1, 1)),
      fx.stream("t", 8, fx.partition(0, 1, 1, 1, 2)),
    ],
  });

  const zoneFetched = (error: ErrorCodeValue, preferredReplica?: number): Extract<Response, { type: "Fetch" }> => ({
    type: "Fetch",
    throttleMs: 0,
    resources: [
      {
        resourceId: 0n,
        partitions: [
          {
            partition: 0,
            error,
            highWatermark: 0n,
            lastStableOffset: 0n,
            logStartOffset: 0n,
            abortedTxns: [],
            records: new Uint8Array(0),
            leaderHint: null,
            ...(preferredReplica === undefined ? {} : { preferredReplica }),
          },
        ],
      },
    ],
  });

  /** The answer to a zone read, with `error` (and then the leader's hint) or without. */
  const read = (req: Request, error: ErrorCodeValue): Response => {
    const leaderHint = error === ErrorCode.NONE ? null : HINT;
    switch (req.type) {
      case "DescribeConfigs":
        return {
          type: "DescribeConfigs",
          error,
          leaderHint,
          resources: req.resourceIds.map((resourceId) => ({ resourceId, error: ErrorCode.NONE, configs: [] })),
        };
      case "DescribeNodes":
        return { type: "DescribeNodes", error, leaderHint, nodes: [] };
      case "DescribeUsers":
        return { type: "DescribeUsers", error, leaderHint, users: [] };
      case "DescribeTokens":
        return { type: "DescribeTokens", error, leaderHint, tokens: [] };
      case "DescribeCertificates":
        return { type: "DescribeCertificates", error, leaderHint, nowMs: 0n, certificates: [] };
      case "DescribeCertificateRequests":
        return { type: "DescribeCertificateRequests", error, leaderHint, nowMs: 0n, requests: [] };
      case "DescribeUpgrade":
        return { type: "DescribeUpgrade", error, leaderHint, clusterVersion: null, held: false, turn: null, nodes: [] };
      case "DescribeTenants":
        return { type: "DescribeTenants", error, leaderHint, tenants: [] };
      case "DescribeRoleBindings":
        return { type: "DescribeRoleBindings", error, leaderHint, bindings: [] };
      case "OffsetFetch":
        return {
          type: "OffsetFetch",
          resources: req.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((partition) => ({ partition, error, offset: 0n, leaderTerm: 1n, metadata: null })),
          })),
        };
      default:
        throw new Error(`not a zone read: ${req.type}`);
    }
  };

  /** What every node answers, unless a test says otherwise. */
  const common = (req: Request): Response => {
    switch (req.type) {
      case "Metadata":
        return zoneMetadata(a, b, c);
      case "FindCoordinator":
        return { ...fx.coordinator(a), voters: [1, 2, 3] };
      case "GroupHeartbeat":
        return {
          type: "GroupHeartbeat",
          throttleMs: 0,
          error: ErrorCode.NONE,
          memberId: "m",
          memberEpoch: 1,
          heartbeatIntervalMs: 1_000,
          assignment: [{ resourceId: 7n, partitions: [0] }],
        };
      case "Fetch":
        return zoneFetched(ErrorCode.NONE);
      case "ListOffsets":
        return fx.listed(0, 0, 0);
      case "CommitOffsets":
        return {
          type: "CommitOffsets",
          resources: req.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((p) => ({ partition: p.partition, error: ErrorCode.NONE })),
          })),
        };
      default:
        return read(req, ErrorCode.NONE);
    }
  };

  /** Every node answers `common` and records what it hears in `heard`. */
  const zoneNodes = (): void => {
    for (const n of [a, b, c]) {
      n.heardIn(heard).on((_node, req) => common(req));
    }
  };

  const zoneClient = (zone: string | null): LotseClient => {
    client = new LotseClient(fx.config([a], zone === null ? {} : { zone }));
    return client;
  };

  /** The requests of `type` that were heard, as "node [readAtLeast]". */
  const heardOf = (type: Request["type"]): string[] =>
    heard.filter((h) => h.request.type === type).map((h) => `${h.node} [${text(h.readAtLeast)}]`);

  /** The nodes that heard the requests of `type`, in order. */
  const nodesOf = (type: Request["type"]): number[] => heard.filter((h) => h.request.type === type).map((h) => h.node);

  /** The reads of the streams, as "node kind allowFollower". */
  const streamReads = (): string[] => {
    const out: string[] = [];
    for (const h of heard) {
      if (h.request.type === "Fetch") {
        out.push(`${h.node} fetch ${h.request.allowFollower === true}`);
      } else if (h.request.type === "ListOffsets") {
        out.push(`${h.node} list ${h.request.allowFollower === true}`);
      }
    }
    return out;
  };

  /** Indexes only rise, and every connection's answers raise the one set. */
  test("anIndexOnlyRises", () => {
    const i = new Indexes();
    assert.deepEqual(i.snapshot(), at(1, 0, 2, 0));
    i.raise(at(1, 5, 2, 3));
    i.raise(at(1, 2));
    assert.deepEqual(i.snapshot(), at(1, 5, 2, 3));
    i.raise(at(2, -1n));
    assert.deepEqual(i.snapshot(), at(1, 5, 2, -1n), "indexes are u64");
    i.raise(at(2, 4));
    assert.deepEqual(i.snapshot(), at(1, 5, 2, -1n), "u64::MAX is the highest");
  });

  test("theZoneNodeIsTheLowestIdInTheZone", () => {
    const zones = new Map([
      [1, "x"],
      [2, "y"],
      [3, "y"],
    ]);
    assert.equal(zoneNode("y", [3, 2, 1], zones), 2);
    assert.equal(zoneNode("x", [2, 3], zones), null);
    assert.equal(zoneNode("z", zones.keys(), zones), null);
  });

  /**
   * Every `applied` the client hears raises its index of that group; a lower one (from a follower further behind) leaves
   * it as it is.
   */
  test("appliedRaisesTheIndexAndNeverLowersIt", async () => {
    zoneNodes();
    a.applied((r) => (r.type === "Metadata" ? at(1, 5, 2, 3) : []));
    const answers = [at(1, 2), at(1, 9)];
    b.applied(() => answers.shift() ?? []);
    await zoneClient("y").metadata();
    for (let i = 0; i < 3; i++) {
      await client!.describeUsers();
    }
    assert.deepEqual(
      heardOf("DescribeUsers"),
      ["2 [1:5 2:3]", "2 [1:5 2:3]", "2 [1:9 2:3]"],
      "2 is below 5 and leaves it; 9 raises it",
    );
  });

  /** Each of the nine admin reads goes to the lowest-id node in the client's zone with both groups' indexes. */
  test("anAdminReadGoesToTheZoneWithReadAtLeast", async () => {
    zoneNodes();
    a.applied((r) => (r.type === "Metadata" ? at(1, 4) : []));
    const cl = zoneClient("y");
    await cl.metadata();
    await cl.describeConfigs("s");
    await cl.describeNodes();
    await cl.describeUsers();
    await cl.describeTokens(null);
    await cl.describeCertificates();
    await cl.describeCertificateRequests();
    await cl.describeUpgrade();
    await cl.describeTenants();
    await cl.describeRoleBindings();
    const reads = heard.filter((h) => h.request.type !== "Metadata");
    assert.equal(reads.length, 9, reads.map((h) => h.request.type).join(" "));
    for (const h of reads) {
      assert.equal(`${h.node} [${text(h.readAtLeast)}]`, "2 [1:4 2:0]", h.request.type);
    }
  });

  /**
   * A zone node that has not applied what the client saw refuses with NOT_LEADER: that read goes to the leader with the
   * same tag, which a leader deposed without knowing it refuses (ADR 0141), and the next read tries the zone again.
   */
  test("aRefusedZoneReadGoesToTheLeaderAndTheNextTriesTheZoneAgain", async () => {
    zoneNodes();
    a.applied(() => at(1, 6));
    let refusedOnce = false;
    b.on((_node, r) => {
      if (r.type === "DescribeTenants" && !refusedOnce) {
        refusedOnce = true;
        return read(r, ErrorCode.NOT_LEADER);
      }
      return common(r);
    });
    const cl = zoneClient("y");
    await cl.metadata();
    await cl.describeTenants();
    await cl.describeTenants();
    assert.deepEqual(heardOf("DescribeTenants"), ["2 [1:6 2:0]", "1 [1:6 2:0]", "2 [1:6 2:0]"]);
  });

  /**
   * A node of an older release ignores the tag and refuses the leader-only reads with NOT_LEADER: every such read ends
   * at the leader.
   */
  test("anOlderZoneNodeSendsTheReadToTheLeader", async () => {
    zoneNodes();
    b.on((_node, r) => (r.type === "DescribeUpgrade" ? read(r, ErrorCode.NOT_LEADER) : common(r)));
    const cl = zoneClient("y");
    await cl.metadata();
    await cl.describeUpgrade();
    await cl.describeUpgrade();
    assert.deepEqual(nodesOf("DescribeUpgrade"), [2, 1, 2, 1]);
  });

  /** A group member reads its committed offsets from the lowest-id coordinator voter in its zone. */
  test("offsetFetchGoesToACoordinatorVoterInTheZone", async () => {
    zoneNodes();
    a.applied((r) =>
      r.type === "Metadata" ? at(1, 4) : r.type === "FindCoordinator" || r.type === "GroupHeartbeat" ? at(2, 6) : [],
    );
    const group = await zoneClient("y").joinGroup("g", ["s"]);
    await group.poll(10);
    assert.deepEqual(group.positions(), new Map([["s", new Map([[0, 0n]])]]), "the committed offset");
    assert.deepEqual(heardOf("OffsetFetch"), ["2 [1:4 2:6]"]);
  });

  /** A fetch that goes to the leader names the client's zone, so the leader can point back to it; one to the zone does not. */
  test("aFetchToTheLeaderNamesTheZone", async () => {
    zoneNodes();
    const cl = zoneClient("y");
    await cl.fetch("s", 0, 0n, 0);
    await cl.fetch("t", 0, 0n, 0);
    const fetches = heard.flatMap((h) =>
      h.request.type === "Fetch" ? [`${h.node} ${h.request.zone ?? "none"} ${h.request.allowFollower === true}`] : [],
    );
    assert.deepEqual(fetches, ["1 y false", "2 none true"]);
  });

  /**
   * After a replica in the zone failed, the leader serves; once it names that replica as current again, the next fetch
   * and offset lookup go back there, without asking for metadata.
   */
  test("preferredReplicaSendsTheNextFetchBackToTheZone", async () => {
    zoneNodes();
    a.on((_node, r) => (r.type === "Fetch" ? zoneFetched(ErrorCode.NONE, 2) : common(r)));
    let refusedOnce = false;
    b.on((_node, r) => {
      if (r.type === "Fetch" && !refusedOnce) {
        refusedOnce = true;
        return zoneFetched(ErrorCode.NOT_LEADER);
      }
      return common(r);
    });
    const cl = zoneClient("y");
    await cl.fetch("t", 0, 0n, 0);
    await cl.fetch("t", 0, 0n, 0);
    await cl.listOffset("t", 0, OffsetAt.LATEST);
    assert.deepEqual(streamReads(), ["2 fetch true", "1 fetch false", "2 fetch true", "2 list true"]);
    assert.equal(nodesOf("Metadata").length, 1, "metadata only for the first route");
  });

  /** A client without a zone sends neither tag, and its reads go to the leaders. */
  test("noZoneNoTags", async () => {
    zoneNodes();
    a.applied(() => at(1, 4, 2, 6));
    for (const n of [b, c]) {
      n.on((_node, r) => {
        if (r.type === "Metadata" || r.type === "FindCoordinator") {
          return common(r);
        }
        throw new Error(`only the leader is asked, not for ${r.type}`);
      });
    }
    const cl = zoneClient(null);
    await cl.describeUpgrade();
    await cl.describeUsers();
    await cl.fetch("t", 0, 0n, 0);
    await cl.listOffset("t", 0, OffsetAt.LATEST);
    await (await cl.joinGroup("g", ["s"])).poll(10);
    assert.ok(heard.some((h) => h.request.type === "OffsetFetch"));
    for (const h of heard) {
      assert.deepEqual(h.readAtLeast, [], h.request.type);
      if (h.request.type === "Fetch") {
        assert.equal(h.request.zone, undefined);
        assert.notEqual(h.request.allowFollower, true);
      }
      if (h.request.type !== "Metadata" && h.request.type !== "FindCoordinator") {
        assert.equal(h.node, 1, h.request.type);
      }
    }
  });

  /**
   * A node the latest metadata no longer lists is no zone node, for admin reads and offset reads alike: here node 2
   * left, and the reads go to node 3 without trying node 2 first. Node 2 still serves, so a read sent there would show.
   */
  test("zoneReadsSkipANodeTheLatestMetadataLeftOut", async () => {
    zoneNodes();
    let answers = 0;
    a.on((_node, r) => (r.type === "Metadata" ? (++answers === 1 ? zoneMetadata(a, b, c) : zoneMetadata(a, c)) : common(r)));
    const cl = zoneClient("y");
    await cl.metadata();
    await cl.metadata();
    await cl.describeUsers();
    await (await cl.joinGroup("g", ["s"])).poll(10);
    assert.deepEqual(nodesOf("DescribeUsers"), [3]);
    assert.deepEqual(nodesOf("OffsetFetch"), [3]);
  });

  /** Node 2 refuses the first fetch of t, then serves; node 1 answers its n-th fetch with what `leader` gives. */
  const afterAFallback = async (leader: (n: number) => Response): Promise<string[]> => {
    zoneNodes();
    let count = 0;
    a.on((_node, r) => (r.type === "Fetch" ? leader(++count) : common(r)));
    let refusedOnce = false;
    b.on((_node, r) => {
      if (r.type === "Fetch" && !refusedOnce) {
        refusedOnce = true;
        return zoneFetched(ErrorCode.NOT_LEADER);
      }
      return common(r);
    });
    const cl = zoneClient("y");
    try {
      await cl.fetch("t", 0, 0n, 0);
    } catch (e) {
      // the leader's answer to it is the test's to choose
      assert.ok(e instanceof ClientError, String(e));
    }
    await cl.fetch("t", 0, 0n, 0);
    return streamReads();
  };

  /** A leader answer with an error says nothing about the replicas: its preferred replica leaves the next fetch with the leader. */
  test("preferredReplicaOnAnErrorIsIgnored", async () => {
    const seen = await afterAFallback((n) =>
      n === 1 ? zoneFetched(ErrorCode.OFFSET_OUT_OF_RANGE, 2) : zoneFetched(ErrorCode.NONE),
    );
    assert.deepEqual(seen, ["2 fetch true", "1 fetch false", "1 fetch false"]);
  });

  /**
   * A replica the client has no address for cannot be asked: the fallback stays, rather than sending the next fetch to
   * the replica that failed.
   */
  test("preferredReplicaWithoutAnAddressLeavesTheFetchOnTheLeader", async () => {
    const seen = await afterAFallback((n) => zoneFetched(ErrorCode.NONE, n === 1 ? 4 : undefined));
    assert.deepEqual(seen, ["2 fetch true", "1 fetch false", "1 fetch false"]);
  });

  /**
   * The zone voter refuses an `OffsetFetch`, or the call to it fails: that read goes to the coordinator with the same
   * tag, and the next tries the zone again.
   */
  test("aRefusedOffsetFetchGoesToTheCoordinatorAndTheNextTriesTheZoneAgain", async () => {
    for (const failedCall of [false, true]) {
      heard.length = 0;
      zoneNodes();
      a.applied((r) => (r.type === "Metadata" ? at(1, 4) : r.type === "FindCoordinator" ? at(2, 6) : []));
      let refusedOnce = false;
      b.on((_node, r) => {
        if (r.type === "OffsetFetch" && !refusedOnce) {
          refusedOnce = true;
          return failedCall ? { type: "Error", code: ErrorCode.INVALID_REQUEST } : read(r, ErrorCode.NOT_LEADER);
        }
        return common(r);
      });
      const cl = zoneClient("y");
      await cl.metadata();
      const req: Request = {
        type: "OffsetFetch",
        groupId: "g",
        requireStable: true,
        resources: [{ resourceId: 7n, partitions: [0] }],
        tenant: "default",
      };
      const internal = cl as unknown as { callCoordinatorRead(key: string, req: Request): Promise<Response> };
      for (let i = 0; i < 2; i++) {
        const r = await internal.callCoordinatorRead("g", req);
        assert.ok(r.type === "OffsetFetch" && !refused(r), JSON.stringify(r, (_k, v: unknown) => (typeof v === "bigint" ? String(v) : v)));
      }
      assert.deepEqual(heardOf("OffsetFetch"), ["2 [1:4 2:6]", "1 [1:4 2:6]", "2 [1:4 2:6]"], `failed call: ${failedCall}`);
      cl.close();
    }
    client = undefined;
  });
});

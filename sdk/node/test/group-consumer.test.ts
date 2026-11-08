import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  BrokerResponseError,
  ConsumerClosedError,
  ErrorCode,
  type ErrorCodeValue,
  type GroupConfig,
  type GroupConsumer,
  type GroupRecord,
  LotseClient,
  MustAbortError,
  NO_OFFSET,
  OffsetAt,
  type Request,
  type Response,
} from "../src/index.js";
import { closeAll, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";

/** A consumer group member against a scripted broker that is coordinator, leader and group coordinator at once. */

const STREAM_ID = 7n;
const WAIT = 20;

type Heartbeat = Extract<Request, { type: "GroupHeartbeat" }>;
type HeartbeatAnswer = Extract<Response, { type: "GroupHeartbeat" }>;

describe("a consumer group member against a scripted broker", () => {
  let node: FakeNode;
  let all: FakeNode[];
  let client: LotseClient | undefined;
  /** The answer to the n-th heartbeat, counted from 1. */
  let heartbeat: (n: number, r: Heartbeat) => HeartbeatAnswer;
  let heartbeats: number;
  /** Committed offsets per partition; absent: none. */
  let committed: Map<number, bigint>;
  /** Fetch answers per partition; absent: two records from the offset asked for. */
  let fetch: Map<number, (offset: bigint) => Response | Promise<Response>>;
  let commitError: ErrorCodeValue;

  const assign = (...partitions: number[]): HeartbeatAnswer => ({
    type: "GroupHeartbeat",
    throttleMs: 0,
    error: ErrorCode.NONE,
    memberId: "m1",
    memberEpoch: 3,
    // An interval of 0: every poll heartbeats.
    heartbeatIntervalMs: 0,
    assignment: [{ resourceId: STREAM_ID, partitions }],
  });
  const assignEvery = (ms: number, ...partitions: number[]): HeartbeatAnswer => ({ ...assign(...partitions), heartbeatIntervalMs: ms });
  const failed = (error: ErrorCodeValue): HeartbeatAnswer => ({
    type: "GroupHeartbeat",
    throttleMs: 0,
    error,
    memberId: "",
    memberEpoch: 0,
    heartbeatIntervalMs: 0,
    assignment: null,
  });

  beforeEach(async () => {
    all = await startNodes(1);
    node = all[0] as FakeNode;
    heartbeat = () => assign(0);
    heartbeats = 0;
    committed = new Map();
    fetch = new Map();
    commitError = ErrorCode.NONE;
    node.on((n, request) => answer(n, request));
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll(all);
  });

  function commitAnswer(resources: readonly { resourceId: bigint; partitions: readonly { partition: number }[] }[]): Response {
    return {
      type: "CommitOffsets",
      resources: resources.map((r) => ({
        resourceId: r.resourceId,
        partitions: r.partitions.map((p) => ({ partition: p.partition, error: commitError })),
      })),
    };
  }

  function answer(n: FakeNode, request: Request): Response | Promise<Response> {
    switch (request.type) {
      case "Metadata":
        return fx.metadata(all, fx.stream("s", STREAM_ID, fx.partition(0, 1, 1, 1), fx.partition(1, 1, 1, 1)));
      case "FindCoordinator":
        return fx.coordinator(n);
      case "GroupHeartbeat":
        if (request.memberEpoch === -1) {
          return failed(ErrorCode.NONE);
        }
        return heartbeat(++heartbeats, request);
      case "OffsetFetch":
        return {
          type: "OffsetFetch",
          resources: request.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((p) => ({
              partition: p,
              error: ErrorCode.NONE,
              offset: committed.get(p) ?? NO_OFFSET,
              leaderTerm: 0n,
              metadata: null,
            })),
          })),
        };
      case "Fetch": {
        const p = request.resources[0]?.partitions[0] as { partition: number; fetchOffset: bigint };
        const script = fetch.get(p.partition);
        if (script !== undefined) {
          return script(p.fetchOffset);
        }
        return fx.fetched(STREAM_ID, p.partition, {
          records: fx.data(p.fetchOffset, `r${p.fetchOffset}`, `r${p.fetchOffset + 1n}`),
          highWatermark: 100,
          lastStable: 100,
        });
      }
      case "CommitOffsets":
      case "AddOffsetsToTxn":
        return commitAnswer(request.resources);
      case "ListOffsets": {
        const p = request.resources[0]?.partitions[0] as { partition: number; timestamp: bigint };
        return {
          type: "ListOffsets",
          resources: [
            {
              resourceId: STREAM_ID,
              partitions: [{ partition: p.partition, error: ErrorCode.NONE, offset: p.timestamp === -1n ? 42n : 30n, timestamp: -1n, leaderHint: null }],
            },
          ],
        };
      }
      case "InitProducer":
        return fx.producer(100, 1);
      case "AddPartitionsToTxn":
        return {
          type: "AddPartitionsToTxn",
          resources: [{ resourceId: request.resources[0]?.resourceId as bigint, partitions: [{ partition: 0, error: ErrorCode.NONE }] }],
        };
      case "Produce":
        return fx.produced(request.resources[0]?.resourceId as bigint, 0, 0);
      case "EndTxn":
        return { type: "EndTxn", error: ErrorCode.NONE, producerId: request.producerId, producerEpoch: request.producerEpoch + 1 };
      default:
        throw new Error(`unexpected ${request.type}`);
    }
  }

  const open = (): LotseClient => {
    client = new LotseClient(fx.config(all, { transactionalId: "t", retryForMs: 400 }));
    return client;
  };
  const join = (config: GroupConfig = {}, streams: string[] = ["s"]): Promise<GroupConsumer> =>
    open().joinGroup("g", streams, config);
  const values = (records: readonly GroupRecord[]): string[] => records.map((r) => Buffer.from(r.record.value as Uint8Array).toString());
  const sent = () => node.requests("GroupHeartbeat");

  test("a member joins, reads from the committed offset and commits where it got to", async () => {
    committed.set(0, 5n);
    const g = await join({ autoCommit: false });
    assert.deepEqual(values(await g.poll(WAIT)), ["r5", "r6"]);
    const j = sent()[0];
    assert.equal(j?.memberEpoch, 0);
    assert.equal(j?.memberId, "");
    assert.deepEqual(j?.subscribed, [STREAM_ID]);
    assert.ok(j?.subscribedPattern === null || j?.subscribedPattern === undefined);
    assert.equal(g.memberId, "m1");
    assert.equal(g.memberEpoch, 3);
    assert.deepEqual(g.positions(), new Map([["s", new Map([[0, 7n]])]]));
    await g.commit();
    const c = node.requests("CommitOffsets")[0];
    assert.equal(c?.memberId, "m1");
    assert.equal(c?.memberEpoch, 3);
    assert.equal(c?.resources[0]?.partitions[0]?.offset, 7n);
    assert.equal(node.requests("OffsetFetch")[0]?.requireStable, true, "waits for open transactions");
  });

  test("a member heartbeats again only when due and reports what it owns", async () => {
    heartbeat = () => ({ ...assign(0), heartbeatIntervalMs: 200 });
    const g = await join();
    await g.poll(WAIT);
    await g.poll(WAIT);
    assert.equal(sent().length, 1, "not due yet");
    await sleep(250);
    await g.poll(WAIT);
    assert.equal(sent().length, 2);
    const second = sent()[1];
    assert.equal(second?.memberEpoch, 3);
    assert.equal(second?.memberId, "m1");
    assert.equal(second?.subscribed, null, "the subscription goes with the join only");
    assert.deepEqual(second?.owned, [{ resourceId: STREAM_ID, partitions: [0] }]);
  });

  test("a revoked partition is committed, then reported released at once", async () => {
    // The revoking answer schedules the next heartbeat 10 s later; the release must not wait for it.
    heartbeat = (n) => (n === 1 ? assign(0, 1) : assignEvery(10_000, 1));
    const g = await join();
    await g.poll(WAIT);
    await g.poll(WAIT);
    assert.deepEqual(g.assignment(), new Map([["s", [1]]]));
    await g.poll(WAIT);
    assert.equal(sent().length, 3, "released with the next poll");
    const all = node.requests() as Request[];
    let commit = -1;
    let release = -1;
    all.forEach((r, i) => {
      if (r.type === "CommitOffsets" && commit < 0) {
        assert.equal(r.resources[0]?.partitions[0]?.partition, 0);
        commit = i;
      }
      if (r.type === "GroupHeartbeat" && release < 0 && r.owned?.length === 1 && r.owned[0]?.partitions.join() === "1" && i > 0) {
        release = i;
      }
    });
    assert.ok(commit >= 0, "the revoked partition's position is committed");
    assert.ok(release > commit, "and only then reported released");
  });

  test("without auto-commit a revoked partition is released without a commit", async () => {
    heartbeat = (n) => (n === 1 ? assign(0, 1) : assign(1));
    const g = await join({ autoCommit: false });
    await g.poll(WAIT);
    await g.poll(WAIT);
    await g.poll(WAIT);
    assert.equal(node.count("CommitOffsets"), 0);
  });

  test("a member without a heartbeat for the session timeout drops everything and joins again", async () => {
    heartbeat = (n) => (n === 1 ? assign(0) : n < 100 ? failed(ErrorCode.COORDINATOR_NOT_AVAILABLE) : assign(0));
    const g = await join({ sessionTimeoutMs: 300 });
    await g.poll(WAIT);
    assert.deepEqual(g.assignment(), new Map([["s", [0]]]));
    await sleep(350);
    await g.poll(WAIT);
    assert.deepEqual(g.assignment(), new Map(), "the coordinator may have given it away");
    assert.equal(g.memberId, "");
    const last = sent()[sent().length - 1];
    assert.equal(last?.memberEpoch, 0, "joins again as a new member");
    assert.equal(node.count("CommitOffsets"), 0, "no commit for what was lost");
  });

  test("a fetch that outlasts the session hands out nothing", async () => {
    // The fetch answer comes after the session timeout: the coordinator may have given the partition away.
    fetch.set(0, async (offset) => {
      await sleep(400);
      return fx.fetched(STREAM_ID, 0, { records: fx.data(offset, "late"), highWatermark: 100, lastStable: 100 });
    });
    const g = await join({ sessionTimeoutMs: 300 });
    assert.deepEqual(await g.poll(50), []);
    assert.deepEqual(g.assignment(), new Map());
    assert.equal(g.memberId, "");
  });

  test("an unknown member joins again", async () => {
    heartbeat = (n) => (n === 2 ? failed(ErrorCode.UNKNOWN_MEMBER_ID) : assign(0));
    const g = await join();
    await g.poll(WAIT);
    await g.poll(WAIT);
    assert.deepEqual(g.assignment(), new Map());
    await g.poll(WAIT);
    const third = sent()[2];
    assert.equal(third?.memberEpoch, 0);
    assert.equal(third?.memberId, "");
    assert.deepEqual(third?.subscribed, [STREAM_ID]);
  });

  test("a fenced instance stays out", async () => {
    heartbeat = () => failed(ErrorCode.FENCED_INSTANCE_ID);
    const g = await join({ instanceId: "i1" });
    await assert.rejects(g.poll(WAIT), (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.FENCED_INSTANCE_ID);
    assert.equal(sent()[0]?.instanceId, "i1");
    await assert.rejects(g.poll(WAIT), BrokerResponseError);
    assert.equal(sent().length, 1, "it does not join again");
  });

  test("a partition without a committed offset starts where the configuration says", async () => {
    const latest = await join({ startFrom: OffsetAt.LATEST });
    await latest.poll(WAIT);
    assert.deepEqual(latest.positions(), new Map([["s", new Map([[0, 44n]])]]), "42, then the two records read");
    const asked = node.requests("ListOffsets")[0];
    assert.equal(asked?.resources[0]?.partitions[0]?.timestamp, -1n);
    assert.equal(asked?.isolation, 1, "committed");
    const earliest = await (client as LotseClient).joinGroup("g2", ["s"]);
    await earliest.poll(WAIT);
    assert.deepEqual(earliest.positions(), new Map([["s", new Map([[0, 2n]])]]), "from 0, without asking");
    assert.equal(node.count("ListOffsets"), 1);
  });

  test("a position retention deleted goes on from the log start", async () => {
    committed.set(0, 3n);
    fetch.set(0, (offset) =>
      offset < 10n
        ? fx.fetched(STREAM_ID, 0, { code: ErrorCode.OFFSET_OUT_OF_RANGE, logStart: 10, highWatermark: 100, lastStable: 100 })
        : fx.fetched(STREAM_ID, 0, { records: fx.data(offset, "x"), logStart: 10, highWatermark: 100, lastStable: 100 }),
    );
    const g = await join();
    assert.deepEqual(await g.poll(WAIT), []);
    assert.deepEqual(g.positions(), new Map([["s", new Map([[0, 10n]])]]));
    assert.deepEqual(values(await g.poll(WAIT)), ["x"]);
  });

  test("rewind forgets the positions", async () => {
    committed.set(0, 5n);
    const g = await join({ autoCommit: false });
    await g.poll(WAIT);
    g.rewind();
    assert.deepEqual(g.positions(), new Map());
    assert.deepEqual(values(await g.poll(WAIT)), ["r5", "r6"], "from the committed offset again");
  });

  test("closing commits and leaves", async () => {
    const g = await join();
    await g.poll(WAIT);
    await g.close();
    assert.equal(node.requests("CommitOffsets")[0]?.resources[0]?.partitions[0]?.offset, 2n);
    const leave = sent()[sent().length - 1];
    assert.equal(leave?.memberEpoch, -1);
    assert.equal(leave?.memberId, "m1");
    await g.close();
    assert.equal(node.count("CommitOffsets"), 1, "closing twice does nothing more");
    await assert.rejects(g.poll(WAIT), ConsumerClosedError);
  });

  test("a pattern goes with the join instead of streams", async () => {
    const g = await open().joinGroupMatching("g", "s.*");
    await g.poll(WAIT);
    const j = sent()[0];
    assert.equal(j?.subscribedPattern, "s.*");
    assert.equal(j?.subscribed, null);
    assert.deepEqual(g.assignment(), new Map([["s", [0]]]));
  });

  test("a signal ends a poll that waits", async () => {
    heartbeat = () => ({ ...assign(), heartbeatIntervalMs: 10_000 });
    const g = await join();
    const ac = new AbortController();
    const started = Date.now();
    const pending = g.poll(5_000, { signal: ac.signal });
    await sleep(30);
    ac.abort();
    assert.deepEqual(await pending, []);
    assert.ok(Date.now() - started < 1_000);
  });

  test("the iterator hands out records until the consumer is closed or the signal fires", async () => {
    const g = await join({ autoCommit: false });
    const seen: string[] = [];
    for await (const r of g) {
      seen.push(...values([r]));
      if (seen.length >= 5) {
        break;
      }
    }
    assert.deepEqual(seen, ["r0", "r1", "r2", "r3", "r4"]);
    const ac = new AbortController();
    const records: string[] = [];
    for await (const r of g.records({ signal: ac.signal, waitMs: WAIT })) {
      records.push(...values([r]));
      ac.abort();
    }
    assert.equal(records.length, 2, "the batch in hand is handed out, then the iterator ends");
    setTimeout(() => void g.close(), 20);
    for await (const _ of g.records({ waitMs: WAIT })) {
      // runs until close
    }
  });

  test("calls to poll take turns and a position moves once per record", async () => {
    const g = await join({ autoCommit: false });
    const [a, b] = await Promise.all([g.poll(WAIT), g.poll(WAIT)]);
    assert.deepEqual(values(a), ["r0", "r1"]);
    assert.deepEqual(values(b), ["r2", "r3"]);
  });

  test("a partition rewound or lost while its fetch is in flight hands out nothing", async () => {
    fetch.set(0, async (offset) => {
      await sleep(80);
      return fx.fetched(STREAM_ID, 0, { records: fx.data(offset, "late"), highWatermark: 100, lastStable: 100 });
    });
    const g = await join({ autoCommit: false });
    const pending = g.poll(500);
    await sleep(40);
    g.rewind();
    assert.deepEqual(await pending, [], "the position moved under the fetch: nothing is handed out");
    assert.deepEqual(g.positions(), new Map(), "and the position stays forgotten");
  });

  test("a partition is read from only while it is assigned", async () => {
    heartbeat = (n) => (n === 1 ? assign(0) : assignEvery(10_000, 1));
    const g = await join({ autoCommit: false });
    await g.poll(WAIT);
    await g.poll(WAIT);
    const fetchedFrom = node.requests("Fetch").map((f) => f.resources[0]?.partitions[0]?.partition);
    assert.deepEqual(fetchedFrom.slice(0, 1), [0]);
    await g.poll(WAIT);
    await g.poll(WAIT);
    const after = node.requests("Fetch").slice(1).map((f) => f.resources[0]?.partitions[0]?.partition);
    assert.ok(after.every((p) => p === 1), `only partition 1 is read once it is revoked: ${after.join()}`);
    assert.deepEqual(g.assignment(), new Map([["s", [1]]]));
  });

  // ---- offsets in a transaction ----

  test("offsets sent with a transaction carry the member and are not committed on their own", async () => {
    const g = await join({ autoCommit: false });
    await g.poll(WAIT);
    const txn = await (client as LotseClient).beginTransaction();
    await txn.produce("s", 1, { value: "out" });
    await txn.sendOffsets(g);
    await txn.commit();
    const a = node.requests("AddOffsetsToTxn")[0];
    assert.equal(a?.groupId, "g");
    assert.equal(a?.memberId, "m1");
    assert.equal(a?.memberEpoch, 3);
    assert.equal(a?.txnId, "t");
    assert.equal(a?.resources[0]?.partitions[0]?.offset, 2n);
    assert.equal(node.count("CommitOffsets"), 0, "not committed on their own");
  });

  test("refused offsets leave the transaction only an abort", async () => {
    const g = await join({ autoCommit: false });
    await g.poll(WAIT);
    commitError = ErrorCode.FENCED_MEMBER_EPOCH;
    const txn = await (client as LotseClient).beginTransaction();
    await txn.produce("s", 1, { value: "out" });
    await assert.rejects(txn.sendOffsets(g), BrokerResponseError);
    await assert.rejects(txn.commit(), MustAbortError);
    await txn.abort();
  });
});

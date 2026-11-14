import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BrokerResponseError,
  ClientClosedError,
  type DeliverPush,
  ErrorCode,
  type ErrorCodeValue,
  ExchangeType,
  GaveUpError,
  LotseClient,
  MustAbortError,
  NotTransactionalError,
  type Request,
  type ResourceMetadata,
  type Response,
  ResourceKind,
  TransactionEndedError,
  UnknownOutcomeError,
  UnknownStreamError,
  UnroutableError,
} from "../src/index.js";
import type { Peer } from "./support/fake-broker.js";
import { closeAll, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";
import { eventually } from "./support/lotse.js";

/** Transactions against a scripted broker that is coordinator and leader at once, as the Rust client does them. */

const STREAM_ID = 7n;
const EXCHANGE_ID = 20n;
const QUEUE_A = 21n;
const QUEUE_B = 22n;

const SILENT = Symbol("no answer");

describe("transactions against a scripted broker", () => {
  let node: FakeNode;
  let all: FakeNode[];
  let client: LotseClient | undefined;
  let nextProducer: bigint;
  let nextOffset: bigint;
  /** What a produce to a resource id gets; absent: written at the next offset. */
  let produce: Map<bigint, (r: Extract<Request, { type: "Produce" }>) => Response | typeof SILENT>;
  let endTxn: (r: Extract<Request, { type: "EndTxn" }>) => Response;
  let ackError: ErrorCodeValue;
  /** The consumer id the node gives (ADR 0109); 0 as a node from before it. */
  let consumerId: bigint;
  /** Acks to refuse as NOT_LEADER before taking them. */
  let acksNotLed: number;
  /** The connection the last subscription was made on. */
  let subPeer: Peer | undefined;

  beforeEach(async () => {
    all = await startNodes(1);
    node = all[0] as FakeNode;
    nextProducer = 100n;
    nextOffset = 0n;
    produce = new Map();
    endTxn = (r) => ({ type: "EndTxn", error: ErrorCode.NONE, producerId: r.producerId, producerEpoch: r.producerEpoch + 1 });
    ackError = ErrorCode.NONE;
    consumerId = 0n;
    acksNotLed = 0;
    subPeer = undefined;
    node.on((n, request, peer) => {
      const answer = answerOf(n, request, peer);
      return answer === SILENT ? null : answer;
    });
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll(all);
  });

  const queue = (name: string, id: bigint): ResourceMetadata => fx.queue(name, id, fx.partition(0, 1, 1, 1));

  function answerOf(n: FakeNode, request: Request, peer: Peer): Response | typeof SILENT {
    switch (request.type) {
      case "Metadata": {
        // A topic exchange: "a.#" to queue qa, "*.b" to queue qb.
        const x: ResourceMetadata = {
          error: ErrorCode.NONE,
          name: "x",
          tenant: "default",
          resourceId: EXCHANGE_ID,
          kind: ResourceKind.EXCHANGE,
          partitions: [],
          exchange: {
            exchangeType: ExchangeType.TOPIC,
            bindings: [
              { queueId: QUEUE_A, bindingKey: "a.#", headerMatch: [], matchAll: true },
              { queueId: QUEUE_B, bindingKey: "*.b", headerMatch: [], matchAll: true },
            ],
          },
        };
        const asked = request.names;
        const known = [
          fx.stream("s", STREAM_ID, fx.partition(0, 1, 1, 1), fx.partition(1, 1, 1, 1)),
          x,
          queue("qa", QUEUE_A),
          queue("qb", QUEUE_B),
        ];
        return fx.metadata(all, ...known, ...asked.filter((a) => !known.some((r) => r.name === a)).map(fx.unknown));
      }
      case "FindCoordinator":
        return fx.coordinator(n);
      case "InitProducer":
        return fx.producer(nextProducer++, 4);
      case "AddPartitionsToTxn":
        return {
          type: "AddPartitionsToTxn",
          resources: request.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((p) => ({ partition: p, error: ErrorCode.NONE })),
          })),
        };
      case "Produce": {
        const id = (request.resources[0] as { resourceId: bigint }).resourceId;
        const f = produce.get(id);
        if (f !== undefined) {
          return f(request);
        }
        const part = request.resources[0]?.partitions[0]?.partition ?? 0;
        return fx.produced(id, part, nextOffset++);
      }
      case "EndTxn":
        return endTxn(request);
      case "AddOffsetsToTxn":
        return {
          type: "CommitOffsets",
          resources: request.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((p) => ({ partition: p.partition, error: ErrorCode.NONE })),
          })),
        };
      case "Subscribe":
        subPeer = peer;
        return { type: "Subscribe", error: ErrorCode.NONE, subscriptionId: 500, leaderHint: null, standby: false, consumerId };
      case "Ack":
        if (acksNotLed > 0) {
          acksNotLed--;
          return { type: "Outcome", error: ErrorCode.NOT_LEADER, leaderHint: null };
        }
        return { type: "Outcome", error: ackError, leaderHint: null };
      case "Unsubscribe":
      case "Nack":
        return { type: "Outcome", error: ErrorCode.NONE, leaderHint: null };
      default:
        throw new Error(`unexpected ${request.type}`);
    }
  }

  const open = (extra: object = {}): LotseClient => {
    client = new LotseClient(fx.config(all, { transactionalId: "t", retryForMs: 400, ...extra }));
    return client;
  };

  const produced = (resourceId: bigint): Batch[] =>
    node
      .requests("Produce")
      .filter((p) => p.resources[0]?.resourceId === resourceId)
      .map((p) => Batch.parse(fx.bytesOf(p)).batch);

  const ends = () => node.requests("EndTxn");

  test("a client without a transactional id cannot begin", async () => {
    client = new LotseClient(fx.config(all));
    await assert.rejects(client.beginTransaction(), NotTransactionalError);
  });

  test("batches carry the transaction and each partition is added once", async () => {
    const txn = await open().beginTransaction();
    await txn.produce("s", 0, [{ value: "a" }, { value: "b" }]);
    await txn.produce("s", 0, { value: "c" });
    await txn.produce("s", 1, { value: "d" });
    await txn.commit();
    const adds = node.requests("AddPartitionsToTxn");
    assert.equal(adds.length, 2, "each partition once");
    assert.equal(adds[0]?.txnId, "t");
    const batches = produced(STREAM_ID);
    assert.equal(batches.length, 3);
    for (const b of batches) {
      assert.ok(b.attributes.transactional);
      assert.equal(b.producerId, 100n);
      assert.equal(b.producerEpoch, 4);
    }
    assert.equal(batches[0]?.baseSequence, 0);
    assert.equal(batches[1]?.baseSequence, 2, "counted by records");
    assert.equal(batches[2]?.baseSequence, 0, "per partition");
    for (const p of node.requests("Produce")) {
      assert.equal(p.txnId, "t");
    }
    const end = ends()[0];
    assert.equal(end?.commit, true);
    assert.equal(end?.producerId, 100n);
    assert.equal(txn.isOpen, false);
  });

  test("the next transaction goes on with the raised epoch", async () => {
    const lotse = open();
    const first = await lotse.beginTransaction();
    await first.produce("s", 0, { value: "a" });
    await first.commit();
    const second = await lotse.beginTransaction();
    await second.produce("s", 0, { value: "b" });
    await second.abort();
    assert.equal(node.count("InitProducer"), 1, "the producer id is kept");
    const batches = produced(STREAM_ID);
    assert.equal(batches[1]?.producerEpoch, 5, "the epoch EndTxn raised");
    assert.equal(batches[1]?.baseSequence, 0, "a new epoch starts at 0");
    assert.equal(ends()[1]?.commit, false);
  });

  test("a transaction that wrote nothing ends without the coordinator", async () => {
    const lotse = open();
    await (await lotse.beginTransaction()).commit();
    assert.equal(ends().length, 0);
    await (await lotse.beginTransaction()).abort();
    assert.equal(node.count("InitProducer"), 1);
  });

  test("a transaction left open is abandoned for a fresh producer id", async () => {
    const lotse = open();
    const first = await lotse.beginTransaction();
    await first.produce("s", 0, { value: "a" });
    first.abandon();
    const second = await lotse.beginTransaction();
    await second.produce("s", 0, { value: "b" });
    await second.commit();
    assert.equal(node.count("InitProducer"), 2, "a fresh producer id aborts the first");
    assert.equal(produced(STREAM_ID)[1]?.producerId, 101n);
  });

  test("an unknown write leaves only an abort", async () => {
    produce.set(STREAM_ID, () => fx.produced(STREAM_ID, 0, 0, ErrorCode.REQUEST_TIMED_OUT));
    const txn = await open().beginTransaction();
    await assert.rejects(txn.produce("s", 0, { value: "a" }), UnknownOutcomeError);
    await assert.rejects(txn.produce("s", 1, { value: "b" }), MustAbortError);
    await assert.rejects(txn.commit(), MustAbortError);
    assert.equal(ends().length, 0, "the coordinator was never asked to commit");
    assert.equal(txn.isOpen, true, "open for the abort");
    await txn.abort();
    assert.equal(txn.isOpen, false);
    assert.equal(ends()[0]?.commit, false);
  });

  test("a produce given up by its signal after it was sent leaves only an abort", async () => {
    produce.set(STREAM_ID, () => SILENT);
    const txn = await open({ retryForMs: 5_000 }).beginTransaction();
    const ac = new AbortController();
    const pending = txn.produce("s", 0, { value: "a" }, { signal: ac.signal });
    await eventually("the produce", () => node.count("Produce") > 0);
    ac.abort();
    await assert.rejects(pending, UnknownOutcomeError);
    await assert.rejects(txn.commit(), MustAbortError);
    await txn.abort();
    assert.equal(ends()[0]?.commit, false);
  });

  test("a produce that ends without an outcome because the client closed leaves only an abort", async () => {
    produce.set(STREAM_ID, () => SILENT);
    const lotse = open({ retryForMs: 5_000, requestTimeoutMs: 5_000 });
    const txn = await lotse.beginTransaction();
    const pending = txn.produce("s", 0, { value: "a" });
    await eventually("the produce", () => node.count("Produce") > 0);
    lotse.close();
    await assert.rejects(pending);
    await assert.rejects(txn.produce("s", 0, { value: "b" }), MustAbortError);
    await assert.rejects(txn.commit(), MustAbortError);
  });

  test("a refused write leaves only an abort", async () => {
    produce.set(STREAM_ID, () => fx.produced(STREAM_ID, 0, 0, ErrorCode.INVALID_REQUEST));
    const txn = await open().beginTransaction();
    await assert.rejects(txn.produce("s", 0, { value: "a" }), BrokerResponseError);
    await assert.rejects(txn.commit(), MustAbortError);
    await txn.abort();
  });

  test("a write that certainly failed leaves the transaction open", async () => {
    const txn = await open().beginTransaction();
    await assert.rejects(txn.produce("nope", 0, { value: "a" }), UnknownStreamError);
    await txn.produce("s", 0, { value: "b" });
    await txn.commit();
    assert.equal(ends()[0]?.commit, true);
  });

  test("an add that gave up does not end the transaction while the last one is completing", async () => {
    // The coordinator as the server has it (coord.rs): after EndTxn the decision is being completed and the epoch is
    // already raised. Until then it refuses AddPartitionsToTxn with CONCURRENT_TRANSACTIONS (retriable) and an EndTxn
    // of the next transaction with INVALID_TXN_STATE.
    let completing = false;
    endTxn = (r) => {
      if (completing) {
        return { type: "EndTxn", error: ErrorCode.INVALID_TXN_STATE, producerId: 0n, producerEpoch: 0 };
      }
      completing = true;
      return { type: "EndTxn", error: ErrorCode.NONE, producerId: r.producerId, producerEpoch: r.producerEpoch + 1 };
    };
    node.on((n, request, peer) => {
      if (request.type === "AddPartitionsToTxn" && completing) {
        return {
          type: "AddPartitionsToTxn",
          resources: request.resources.map((r) => ({
            resourceId: r.resourceId,
            partitions: r.partitions.map((p) => ({ partition: p, error: ErrorCode.CONCURRENT_TRANSACTIONS })),
          })),
        };
      }
      const answer = answerOf(n, request, peer);
      return answer === SILENT ? null : answer;
    });
    const lotse = open();
    const first = await lotse.beginTransaction();
    await first.produce("s", 0, { value: "a" });
    await first.commit();
    const second = await lotse.beginTransaction();
    // Nothing was written to the partition: the add gave up, so the transaction stays open ...
    await assert.rejects(second.produce("s", 0, { value: "b" }), GaveUpError);
    assert.equal(produced(STREAM_ID).length, 1);
    // ... and commits without asking the coordinator.
    await second.commit();
    assert.equal(ends().length, 1);
  });

  test("a lost end is asked again and counts as decided", async () => {
    let n = 0;
    endTxn = (r) =>
      ++n === 1
        ? { type: "EndTxn", error: ErrorCode.COORDINATOR_NOT_AVAILABLE, producerId: 0n, producerEpoch: 0 }
        : { type: "EndTxn", error: ErrorCode.NONE, producerId: r.producerId, producerEpoch: r.producerEpoch + 1 };
    const txn = await open().beginTransaction();
    await txn.produce("s", 0, { value: "a" });
    await txn.commit();
    assert.equal(n, 2);
  });

  test("an end whose answer never comes is unknown, and the next transaction starts with a fresh producer id", async () => {
    let silent = true;
    node.on((n, request, peer) => {
      if (request.type === "EndTxn" && silent) {
        return null;
      }
      const a = answerOf(n, request, peer);
      return a === SILENT ? null : a;
    });
    const lotse = open({ retryForMs: 300, requestTimeoutMs: 100 });
    const txn = await lotse.beginTransaction();
    await txn.produce("s", 0, { value: "a" });
    await assert.rejects(txn.commit(), UnknownOutcomeError);
    silent = false;
    await (await lotse.beginTransaction()).abort();
    assert.equal(node.count("InitProducer"), 2);
  });

  test("a failed end starts the next transaction with a fresh producer id", async () => {
    endTxn = () => ({ type: "EndTxn", error: ErrorCode.PRODUCER_FENCED, producerId: 0n, producerEpoch: 0 });
    const lotse = open();
    const txn = await lotse.beginTransaction();
    await txn.produce("s", 0, { value: "a" });
    await assert.rejects(txn.commit(), BrokerResponseError);
    assert.equal(txn.isOpen, false);
    await (await lotse.beginTransaction()).abort();
    assert.equal(node.count("InitProducer"), 2);
  });

  test("a transaction that has ended refuses every call", async () => {
    const txn = await open().beginTransaction();
    await txn.abort();
    await assert.rejects(txn.produce("s", 0, { value: "a" }), TransactionEndedError);
    await assert.rejects(txn.commit(), TransactionEndedError);
    await assert.rejects(txn.abort(), TransactionEndedError);
  });

  test("one transaction at a time", async () => {
    const lotse = open();
    const first = await lotse.beginTransaction();
    let second = false;
    const waiting = lotse.beginTransaction().then((t) => {
      second = true;
      return t;
    });
    await sleep(100);
    assert.equal(second, false, "waits for the first");
    await first.abort();
    await (await waiting).abort();
  });

  test("a signal gives up the wait for the open transaction", async () => {
    const lotse = open();
    const first = await lotse.beginTransaction();
    const ac = new AbortController();
    const waiting = lotse.beginTransaction({ signal: ac.signal });
    await sleep(30);
    ac.abort();
    await assert.rejects(waiting, GaveUpError);
    await first.abort();
    await (await lotse.beginTransaction()).abort();
  });

  test("calls on one transaction take turns in the order they were made", async () => {
    const txn = await open().beginTransaction();
    const [a, b, c] = await Promise.all([
      txn.produce("s", 0, [{ value: "1" }, { value: "2" }]),
      txn.produce("s", 0, { value: "3" }),
      txn.produce("s", 0, { value: "4" }),
    ]);
    assert.ok(a.baseOffset < b.baseOffset && b.baseOffset < c.baseOffset);
    assert.deepEqual(
      produced(STREAM_ID).map((x) => x.baseSequence),
      [0, 2, 3],
    );
    await txn.commit();
  });

  // ---- await using ----

  test("leaving an await using scope aborts a transaction that was not ended", async () => {
    const lotse = open();
    await assert.rejects(async () => {
      await using txn = await lotse.beginTransaction();
      await txn.produce("s", 0, { value: "a" });
      throw new Error("the work failed");
    }, /the work failed/);
    assert.equal(ends().length, 1);
    assert.equal(ends()[0]?.commit, false);
    // The turn is free again and the producer id is the one the abort raised.
    const next = await lotse.beginTransaction();
    await next.produce("s", 0, { value: "b" });
    await next.commit();
    assert.equal(node.count("InitProducer"), 1);
    assert.equal(produced(STREAM_ID)[1]?.producerEpoch, 5);
  });

  test("leaving an await using scope after a commit does nothing more", async () => {
    const lotse = open();
    {
      await using txn = await lotse.beginTransaction();
      await txn.produce("s", 0, { value: "a" });
      await txn.commit();
    }
    assert.equal(ends().length, 1);
    assert.equal(ends()[0]?.commit, true);
  });

  test("a transaction that must abort is aborted by the end of its scope, and the error stays the work's", async () => {
    produce.set(STREAM_ID, () => fx.produced(STREAM_ID, 0, 0, ErrorCode.INVALID_REQUEST));
    const lotse = open();
    await assert.rejects(async () => {
      await using txn = await lotse.beginTransaction();
      await txn.produce("s", 0, { value: "a" });
    }, BrokerResponseError);
    assert.equal(ends()[0]?.commit, false);
  });

  test("a disposal whose abort does not get through abandons the transaction without throwing", async () => {
    node.on((n, request, peer) => {
      if (request.type === "EndTxn") {
        return { type: "EndTxn", error: ErrorCode.PRODUCER_FENCED, producerId: 0n, producerEpoch: 0 };
      }
      const a = answerOf(n, request, peer);
      return a === SILENT ? null : a;
    });
    const lotse = open();
    {
      await using txn = await lotse.beginTransaction();
      await txn.produce("s", 0, { value: "a" });
    }
    await (await lotse.beginTransaction()).abort();
    assert.equal(node.count("InitProducer"), 2, "a fresh producer id after the failed abort");
  });

  test("the signal of a transaction aborts it, giving up the call in progress", async () => {
    produce.set(STREAM_ID, () => SILENT);
    const ac = new AbortController();
    const txn = await open({ retryForMs: 5_000 }).beginTransaction({ signal: ac.signal });
    const pending = txn.produce("s", 0, { value: "a" });
    await eventually("the produce", () => node.count("Produce") > 0);
    ac.abort();
    await assert.rejects(pending, UnknownOutcomeError);
    await eventually("the abort", () => ends().length === 1);
    assert.equal(ends()[0]?.commit, false);
    assert.equal(txn.isOpen, false);
    await assert.rejects(txn.produce("s", 0, { value: "b" }), TransactionEndedError);
  });

  // ---- publishing: routed by this client ----

  test("a publish is routed here and written to every queue in the transaction", async () => {
    const txn = await open().beginTransaction();
    const p = await txn.publish("x", "a.b", { value: "m" });
    assert.deepEqual(
      p.routed.map((r) => r.queue),
      ["qa", "qb"],
    );
    const q = await txn.publish("x", "a.c", { value: "n" }, { mandatory: true });
    assert.deepEqual(
      q.routed.map((r) => r.queue),
      ["qa"],
    );
    assert.deepEqual((await txn.publish("x", "c", { value: "o" })).routed, []);
    await assert.rejects(txn.publish("x", "c", { value: "o" }, { mandatory: true }), UnroutableError);
    await txn.commit();
    assert.equal(produced(QUEUE_A).length, 2);
    assert.equal(produced(QUEUE_B).length, 1);
    assert.ok(produced(QUEUE_B)[0]?.attributes.transactional);
    assert.equal(node.count("Publish"), 0, "not routed by a node");
  });

  test("a publish to an exchange nobody knows is an unknown stream and leaves the transaction open", async () => {
    const txn = await open().beginTransaction();
    await assert.rejects(txn.publish("nope", "k", { value: "m" }), UnknownStreamError);
    await txn.commit();
  });

  test("a publish that reached some queues only leaves only an abort", async () => {
    produce.set(QUEUE_B, () => fx.produced(QUEUE_B, 0, 0, ErrorCode.NOT_LEADER));
    const txn = await open().beginTransaction();
    await assert.rejects(txn.publish("x", "a.b", { value: "m" }), GaveUpError);
    await assert.rejects(txn.commit(), MustAbortError);
    await txn.abort();
    assert.equal(produced(QUEUE_A).length, 1);
  });

  // ---- queue acks in a transaction (ADR 0075) ----

  const deliver = (offset: number): DeliverPush => ({
    subscriptionId: 500,
    error: ErrorCode.NONE,
    batch: fx.data(offset, "m"),
    deliveries: [{ offset: BigInt(offset), deliveryCount: 1 }],
    leaderHint: null,
  });

  test("an ack in a transaction goes on the subscription's connection, after the queue joined", async () => {
    const lotse = open();
    const c = await lotse.subscribe("qa");
    const txn = await lotse.beginTransaction();
    (subPeer as Peer).push(deliver(3));
    const d = await c.poll(3_000);
    assert.ok(d !== null);
    await c.ackIn(txn, d);
    await txn.commit();
    const acks = (subPeer as Peer).received.map((r) => r.request).filter((r) => r.type === "Ack");
    assert.equal(acks.length, 1, "on the subscription's connection");
    const ack = acks[0] as Extract<Request, { type: "Ack" }>;
    assert.equal(ack.txnId, "t");
    assert.equal(ack.producerId, 100n);
    assert.equal(ack.producerEpoch, 4);
    assert.deepEqual(ack.ranges, [{ from: 3n, to: 4n }]);
    assert.equal(node.requests("Ack").length, 1, "and on no other");
    const add = node.requests("AddPartitionsToTxn")[0];
    assert.equal(add?.resources[0]?.resourceId, QUEUE_A, "the queue joins the transaction first");
    c.close();
  });

  test("ADR 0109: an ack refused as NOT_LEADER goes on a subscription that resumes", async () => {
    consumerId = 77n;
    acksNotLed = 1;
    const lotse = open();
    const c = await lotse.subscribe("qa");
    const txn = await lotse.beginTransaction();
    (subPeer as Peer).push(deliver(3));
    const d = await c.poll(3_000);
    assert.ok(d !== null);
    await c.ackIn(txn, d);
    await txn.commit();
    const subs = node.requests("Subscribe");
    assert.equal(subs.length, 2, "subscribed again");
    assert.equal(subs[1]?.resume, 77n, "as the consumer it was");
    assert.equal(node.requests("Ack").length, 2, "sent again on the new subscription");
    c.close();
  });

  test("an ack the leader refuses leaves only an abort", async () => {
    ackError = ErrorCode.MESSAGE_NOT_HELD;
    const lotse = open();
    const c = await lotse.subscribe("qa");
    const txn = await lotse.beginTransaction();
    (subPeer as Peer).push(deliver(3));
    const d = await c.poll(3_000);
    assert.ok(d !== null);
    await assert.rejects(c.ackIn(txn, d), (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.MESSAGE_NOT_HELD);
    await assert.rejects(txn.commit(), MustAbortError);
    await txn.abort();
    c.close();
  });

  test("an ack after the subscription ended is MESSAGE_NOT_HELD", async () => {
    const lotse = open();
    const c = await lotse.subscribe("qa");
    const txn = await lotse.beginTransaction();
    (subPeer as Peer).push(deliver(3));
    const d = await c.poll(3_000);
    assert.ok(d !== null);
    c.close();
    await assert.rejects(c.ackIn(txn, d), (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.MESSAGE_NOT_HELD);
    await txn.abort();
  });

  test("closing the client is reported to a transaction's next call", async () => {
    const lotse = open();
    const txn = await lotse.beginTransaction();
    lotse.close();
    await assert.rejects(txn.produce("s", 0, { value: "a" }), ClientClosedError);
  });
});

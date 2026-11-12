import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BatchBuilder,
  BrokerResponseError,
  ConsumerClosedError,
  type Delivery,
  type DeliverPush,
  ErrorCode,
  type ErrorCodeValue,
  GaveUpError,
  type LeaderHint,
  LotseClient,
  type QueueConsumer,
  type Request,
  type Response,
  UnknownStreamError,
} from "../src/index.js";
import type { Peer } from "./support/fake-broker.js";
import { closeAll, DROP, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";
import { eventually } from "./support/lotse.js";

/** A queue consumer against scripted brokers: what it sends, and what it does with what it is sent. */

const QUEUE_ID = 9n;
const SHORT = 300;

/** A batch of data records at `base`, as the broker stores it. */
function data(base: number, ...values: string[]): Uint8Array {
  const b = new BatchBuilder();
  for (const v of values) {
    b.push(1_000n, null, Buffer.from(v));
  }
  const bytes = b.finish();
  Batch.stamp(bytes, BigInt(base), 1n);
  return bytes;
}

function push(sub: number, base: number, count: number, ...delivered: number[]): DeliverPush {
  const values = Array.from({ length: count }, (_, i) => `m-${base + i}`);
  return {
    subscriptionId: sub,
    error: ErrorCode.NONE,
    batch: data(base, ...values),
    deliveries: delivered.map((o) => ({ offset: BigInt(o), deliveryCount: 1 })),
    leaderHint: null,
  };
}

function pushAll(sub: number, base: number, count: number): DeliverPush {
  return push(sub, base, count, ...Array.from({ length: count }, (_, i) => base + i));
}

function failure(sub: number, error: ErrorCodeValue, hint: LeaderHint | null = null): DeliverPush {
  return { subscriptionId: sub, error, batch: new Uint8Array(0), deliveries: [], leaderHint: hint };
}

const activation = (sub: number): DeliverPush => failure(sub, ErrorCode.NONE);

const outcome = (error: ErrorCodeValue = ErrorCode.NONE, hint: LeaderHint | null = null): Response => ({
  type: "Outcome",
  error,
  leaderHint: hint,
});

const subscribed = (id: number, standby = false, error: ErrorCodeValue = ErrorCode.NONE, hint: LeaderHint | null = null): Response => ({
  type: "Subscribe",
  error,
  subscriptionId: id,
  leaderHint: hint,
  standby,
});

const requestsOf = <T extends Request["type"]>(peer: Peer, type: T): Extract<Request, { type: T }>[] =>
  peer.received.map((r) => r.request).filter((r): r is Extract<Request, { type: T }> => r.type === type);

describe("a queue consumer against scripted brokers", () => {
  let a: FakeNode;
  let b: FakeNode;
  let all: FakeNode[];
  let client: LotseClient | undefined;
  let leader: FakeNode;
  let nextSub = 100;
  /** Every subscription a node answered, in order: the connection it is on and its id. */
  let subs: { node: number; peer: Peer; id: number }[];

  beforeEach(async () => {
    all = await startNodes(2);
    [a, b] = all as [FakeNode, FakeNode];
    leader = a;
    nextSub = 100;
    subs = [];
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll(all);
  });

  type Extra = (request: Request, peer: Peer, node: FakeNode) => Response | null | typeof DROP | undefined;

  /** A healthy queue leader that pushes nothing of its own; `extra` may answer first (`undefined` passes). */
  const serve = (node: FakeNode, extra?: Extra): void => {
    node.on((n, request, peer) => {
      const special = extra?.(request, peer, n);
      if (special !== undefined) {
        return special;
      }
      switch (request.type) {
        case "Metadata":
          return fx.metadata(all, fx.queue("jobs", QUEUE_ID, fx.partition(0, leader.id, 1, 1, 2)));
        case "Subscribe": {
          const id = ++nextSub;
          subs.push({ node: n.id, peer, id });
          return subscribed(id);
        }
        case "Credit":
          return null;
        default:
          return outcome();
      }
    });
  };

  const connect = (...nodes: FakeNode[]): LotseClient => {
    client = new LotseClient(fx.config(nodes, { requestTimeoutMs: 2_000, retryForMs: 3_000 }));
    return client;
  };

  async function take(c: QueueConsumer): Promise<Delivery> {
    const d = await c.poll(3_000);
    assert.notEqual(d, null, "a message should have come");
    return d as Delivery;
  }

  // ---- subscribing ----

  describe("subscribing", () => {
    test("it sends the queue, the credit and asks to be told about activation", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 8 });
      const s = a.requests("Subscribe")[0];
      assert.ok(s);
      assert.equal(s.queueId, QUEUE_ID);
      assert.equal(s.consumerTag, "");
      assert.equal(s.creditMsgs, 8);
      assert.equal(s.creditBytes, 0xffff_ffff);
      assert.equal(s.exclusive, false);
      assert.equal(s.notifyActive, true);
      c.close();
    });

    test("the default credit is 64 and one below 1 is 1", async () => {
      serve(a);
      const lotse = connect(a);
      (await lotse.subscribe("jobs")).close();
      (await lotse.subscribe("jobs", { credit: 0 })).close();
      (await lotse.subscribe("jobs", { credit: -5 })).close();
      (await lotse.subscribe("jobs", { credit: 2.9 })).close();
      assert.deepEqual(
        a.requests("Subscribe").map((s) => s.creditMsgs),
        [64, 1, 1, 2],
      );
      await assert.rejects(lotse.subscribe("jobs", { credit: Number.NaN }), RangeError);
    });

    test("an unknown queue is reported", async () => {
      serve(a, (r) => (r.type === "Metadata" ? fx.metadata([a], fx.unknown("jobs")) : undefined));
      await assert.rejects(connect(a).subscribe("jobs"), UnknownStreamError);
    });

    test("a refused subscription fails at once", async () => {
      serve(a, (r) => (r.type === "Subscribe" ? subscribed(0, false, ErrorCode.INVALID_REQUEST) : undefined));
      await assert.rejects(connect(a).subscribe("jobs"), (e: unknown) => {
        assert.ok(e instanceof BrokerResponseError);
        assert.equal(e.code, ErrorCode.INVALID_REQUEST);
        return true;
      });
      assert.equal(a.count("Subscribe"), 1, "not tried again");
    });

    test("a node that is not the leader sends the subscription on with its hint", async () => {
      serve(a, (r) => (r.type === "Subscribe" ? subscribed(0, false, ErrorCode.NOT_LEADER, { leader: 2, term: 2n }) : undefined));
      serve(b);
      const c = await connect(a).subscribe("jobs");
      assert.equal(b.count("Subscribe"), 1);
      assert.equal(a.count("Subscribe"), 1);
      assert.equal(c.subscribedAddress()?.port, b.address.port);
      c.close();
    });

    test("an aborted signal gives up a subscription that cannot be made", async () => {
      serve(a, (r) => (r.type === "Subscribe" ? subscribed(0, false, ErrorCode.NOT_LEADER) : undefined));
      const ac = new AbortController();
      const pending = connect(a).subscribe("jobs", { signal: ac.signal });
      await sleep(50);
      ac.abort();
      await assert.rejects(pending, GaveUpError);
    });
  });

  // ---- receiving ----

  describe("receiving", () => {
    test("pushes that arrive before the answer to Subscribe are not lost", async () => {
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          // Sent before the answer, so the client has no subscription to give them to yet.
          peer.push(pushAll(id, 10, 2));
          peer.push(pushAll(id, 12, 1));
          return subscribed(id);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      assert.equal((await take(c)).offset, 10n);
      assert.equal((await take(c)).offset, 11n);
      assert.equal((await take(c)).offset, 12n);
      assert.equal(await c.poll(SHORT), null);
      c.close();
    });

    test("only the offsets in the deliveries list are handed out", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(push(s.id, 10, 4, 11, 13));
      const first = await take(c);
      const second = await take(c);
      assert.equal(first.offset, 11n);
      assert.equal(second.offset, 13n);
      assert.equal(first.text(), "m-11");
      assert.equal(second.text(), "m-13");
      assert.equal(await c.poll(SHORT), null, "the records 10 and 12 are somebody else's");
      c.close();
    });

    test("the delivery count and the timestamp come from the push", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push({ ...push(s.id, 5, 1), deliveries: [{ offset: 5n, deliveryCount: 4 }] });
      const d = await take(c);
      assert.equal(d.deliveryCount, 4);
      assert.equal(d.timestamp, 1_000n);
      c.close();
    });

    test("poll returns null when nothing comes and does not subscribe again", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const start = performance.now();
      assert.equal(await c.poll(200), null);
      assert.ok(performance.now() - start >= 150);
      assert.equal(a.count("Subscribe"), 1);
      c.close();
    });

    test("a consumer is an async iterator that ends when it is closed", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      const seen: bigint[] = [];
      const loop = (async () => {
        for await (const d of c) {
          seen.push(d.offset);
        }
      })();
      await eventually("three messages", () => seen.length === 3);
      c.close();
      await loop; // ends without an error
      assert.deepEqual(seen, [10n, 11n, 12n]);
    });

    test("leaving a loop does not close the consumer", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      for await (const d of c) {
        assert.equal(d.offset, 10n);
        break;
      }
      assert.equal((await take(c)).offset, 11n, "the rest is still there");
      assert.equal(a.count("Unsubscribe"), 0);
      c.close();
    });

    test("a signal ends the iterator, and next with a signal gives up", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const ac = new AbortController();
      const loop = (async () => {
        let n = 0;
        for await (const _ of c.deliveries({ signal: ac.signal })) {
          n++;
        }
        return n;
      })();
      await sleep(50);
      ac.abort();
      assert.equal(await loop, 0);
      const ac2 = new AbortController();
      const pending = c.next({ signal: ac2.signal });
      await sleep(30);
      ac2.abort();
      await assert.rejects(pending, GaveUpError);
      assert.equal(await c.poll(100, { signal: ac2.signal }), null, "an aborted signal returns at once");
      c.close();
    });

    test("two reads at once take turns and no message is handed out twice", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      const both = Promise.all([c.poll(3_000), c.poll(3_000)]);
      await sleep(50);
      s.peer.push(pushAll(s.id, 10, 2));
      const got = (await both).map((d) => d?.offset);
      assert.deepEqual(got, [10n, 11n]);
      c.close();
    });
  });

  // ---- credit ----

  describe("credit", () => {
    /** Credit `credit`: pushes `credit` messages in one batch and hands out `taken` of them. */
    async function creditsAfter(credit: number, taken: number): Promise<{ credits: Extract<Request, { type: "Credit" }>[]; bytes: number; sub: number }> {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit });
      const s = subs[0] as (typeof subs)[number];
      const p = pushAll(s.id, 10, credit);
      s.peer.push(p);
      for (let i = 0; i < taken; i++) {
        await take(c);
      }
      await sleep(150); // credit is fire-and-forget: give it time to arrive
      const credits = requestsOf(s.peer, "Credit");
      c.close();
      return { credits, bytes: p.batch.length, sub: s.id };
    }

    test("credit is added once half of it is used", async () => {
      assert.equal((await creditsAfter(5, 2)).credits.length, 0, "2 of 5 used: not yet");
    });

    test("it carries the messages and the bytes used", async () => {
      const r = await creditsAfter(5, 3);
      assert.equal(r.credits.length, 1, "3 of 5 used: now");
      assert.equal(r.credits[0]?.subscriptionId, r.sub);
      assert.equal(r.credits[0]?.addMsgs, 3);
      assert.equal(r.credits[0]?.addBytes, r.bytes, "the bytes of the batch that was received");
    });

    test("an even credit does not refill below half", async () => {
      assert.equal((await creditsAfter(4, 1)).credits.length, 0);
    });

    test("an even credit refills at exactly half", async () => {
      const r = await creditsAfter(4, 2);
      assert.equal(r.credits.length, 1);
      assert.equal(r.credits[0]?.addMsgs, 2);
    });

    test("a credit of one refills after every message", async () => {
      const r = await creditsAfter(1, 1);
      assert.equal(r.credits.length, 1);
      assert.equal(r.credits[0]?.addMsgs, 1);
      assert.equal(r.credits[0]?.addBytes, r.bytes);
    });

    test("the counts start over after a refill", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 4 });
      const s = subs[0] as (typeof subs)[number];
      const p1 = pushAll(s.id, 10, 2);
      const p2 = pushAll(s.id, 12, 2);
      s.peer.push(p1);
      await take(c);
      await take(c);
      s.peer.push(p2);
      await take(c);
      await take(c);
      await eventually("two credits", () => requestsOf(s.peer, "Credit").length >= 2);
      await sleep(100);
      const credits = requestsOf(s.peer, "Credit");
      assert.equal(credits.length, 2, "two refills for four messages");
      assert.equal(credits[0]?.addMsgs, 2);
      assert.equal(credits[0]?.addBytes, p1.batch.length);
      assert.equal(credits[1]?.addMsgs, 2);
      assert.equal(credits[1]?.addBytes, p2.batch.length, "bytes of the first batch are not counted twice");
      c.close();
    });

    test("credit goes on the subscription's connection", async () => {
      const r = await creditsAfter(2, 1);
      assert.equal(r.credits.length, 1);
      assert.equal(a.requests("Credit").length, 1);
      assert.equal(requestsOf((subs[0] as (typeof subs)[number]).peer, "Credit").length, 1);
    });
  });

  // ---- settling ----

  describe("settling", () => {
    test("an ack names the offset as a half-open range", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      const d10 = await take(c);
      await take(c);
      const d12 = await take(c);
      await c.ack(d10);
      const ack = a.requests("Ack")[0];
      assert.ok(ack);
      assert.equal(ack.queueId, QUEUE_ID);
      assert.deepEqual(ack.ranges, [{ from: 10n, to: 11n }]);
      assert.ok(ack.txnId === undefined || ack.txnId === null);
      await c.ack([d12, d10, d12]);
      assert.deepEqual(a.requests("Ack")[1]?.ranges, [
        { from: 10n, to: 11n },
        { from: 12n, to: 13n },
      ]);
      c.close();
    });

    test("acked neighbours are one range", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      const all3 = [await take(c), await take(c), await take(c)];
      await c.ack(all3);
      assert.deepEqual(a.requests("Ack")[0]?.ranges, [{ from: 10n, to: 13n }]);
      await c.ack([]);
      assert.equal(a.count("Ack"), 1, "nothing to ack: nothing sent");
      c.close();
    });

    test("an ack refused as not leader is retried at the new leader", async () => {
      serve(a, (r) => (r.type === "Ack" ? outcome(ErrorCode.NOT_LEADER, { leader: 2, term: 2n }) : undefined));
      serve(b);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      await c.ack(await take(c));
      assert.equal(a.count("Ack"), 1);
      assert.deepEqual(b.requests("Ack")[0]?.ranges, [{ from: 10n, to: 11n }]);
      c.close();
    });

    test("an ack whose answer is lost is sent again", async () => {
      let dropped = false;
      serve(a, (r) => {
        if (r.type === "Ack" && !dropped) {
          dropped = true;
          return DROP;
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      const d = await take(c);
      await c.ack(d);
      assert.equal(a.count("Ack"), 2, "idempotent: sent again");
      c.close();
    });

    test("a refused ack is an error", async () => {
      serve(a, (r) => (r.type === "Ack" ? outcome(ErrorCode.INVALID_REQUEST) : undefined));
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      const d = await take(c);
      await assert.rejects(c.ack(d), (e: unknown) => {
        assert.ok(e instanceof BrokerResponseError);
        assert.equal(e.code, ErrorCode.INVALID_REQUEST);
        return true;
      });
      assert.equal(a.count("Ack"), 1);
      c.close();
    });

    test("a nack without requeue discards at the leader", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      await c.nack(await take(c), false);
      const nack = a.requests("Nack")[0];
      assert.ok(nack);
      assert.equal(nack.requeue, false);
      assert.equal(nack.delayMs, 0);
      assert.equal(nack.queueId, QUEUE_ID);
      assert.deepEqual(nack.ranges, [{ from: 10n, to: 11n }]);
      assert.equal(nack.subscriptionId, undefined, "a discard takes nothing back: no subscription named");
      c.close();
    });

    test("a nack with requeue goes on the subscription's connection", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 2));
      await c.nack(await take(c), true);
      const mine = requestsOf(s.peer, "Nack");
      assert.equal(mine.length, 1, "on the connection of the subscription");
      assert.equal(mine[0]?.requeue, true);
      assert.equal(mine[0]?.delayMs, 0);
      assert.deepEqual(mine[0]?.ranges, [{ from: 10n, to: 11n }]);
      assert.equal(mine[0]?.subscriptionId, s.id, "names its subscription (ADR 0106)");
      c.close();
    });

    test("a delayed requeue carries its delay on the subscription's connection", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      assert.equal(c.isStandby, false);
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 2));
      await c.nackAfter(await take(c), 1_500);
      const first = requestsOf(s.peer, "Nack")[0];
      assert.equal(first?.delayMs, 1_500);
      assert.equal(first?.requeue, true);
      assert.equal(first?.subscriptionId, s.id, "names its subscription (ADR 0106)");
      assert.deepEqual(first?.ranges, [{ from: 10n, to: 11n }]);
      await c.nack(await take(c), true);
      assert.equal(requestsOf(s.peer, "Nack")[1]?.delayMs, 0, "a plain requeue waits not");
      c.close();
    });

    test("a delay is cut to what the field holds", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      await c.nackAfter(await take(c), 1e12);
      await c.nackAfter(await take(c), -5);
      await c.nackAfter(await take(c), 2.9);
      assert.deepEqual(
        requestsOf(s.peer, "Nack").map((n) => n.delayMs),
        [0xffff_ffff, 0, 2],
      );
      c.close();
    });

    test("a requeue the node refuses is an error, a retriable answer is not", async () => {
      serve(a, (r) => (r.type === "Nack" ? outcome(ErrorCode.INVALID_REQUEST) : undefined));
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      const d = await take(c);
      await assert.rejects(c.nack(d, true), BrokerResponseError);
      c.close();
    });

    test("a message of an ended subscription is not requeued on the new one, but an ack still settles it", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      const old = await take(c);
      s.peer.push(failure(s.id, ErrorCode.ACK_TIMED_OUT));
      assert.equal(await c.poll(SHORT), null); // subscribes again
      assert.equal(a.count("Subscribe"), 2);
      await c.nack(old, true);
      assert.equal(a.count("Nack"), 0, "the broker took it back when the subscription ended");
      await c.ack(old); // it names an offset
      assert.equal(a.count("Ack"), 1);
      c.close();
    });
  });

  // ---- standbys ----

  describe("single active consumer", () => {
    test("a standby is told when its turn comes", async () => {
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          return subscribed(id, true);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      assert.equal(c.isStandby, true);
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(activation(s.id));
      assert.equal(await c.poll(SHORT), null, "the activation is no message");
      assert.equal(c.isStandby, false);
      s.peer.push(pushAll(s.id, 10, 1));
      assert.equal((await take(c)).offset, 10n);
      c.close();
    });

    test("a standby that gets a message is active", async () => {
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          return subscribed(id, true);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 1));
      await take(c);
      assert.equal(c.isStandby, false);
      c.close();
    });

    test("a standby that subscribes again after an end is a standby again", async () => {
      let standby = true;
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          const s = standby;
          standby = false;
          return subscribed(id, s);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      assert.equal(c.isStandby, true);
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(failure(s.id, ErrorCode.ACK_TIMED_OUT));
      assert.equal(await c.poll(SHORT), null);
      assert.equal(a.count("Subscribe"), 2);
      assert.equal(c.isStandby, false, "the new subscription is the active one");
      c.close();
    });
  });

  // ---- ending and subscribing again ----

  describe("ending and subscribing again", () => {
    test("a push with NOT_LEADER moves the subscription to the hinted leader", async () => {
      serve(a);
      serve(b, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 2, peer, id });
          peer.push(pushAll(id, 30, 1));
          return subscribed(id);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(failure(s.id, ErrorCode.NOT_LEADER, { leader: 2, term: 2n }));
      const d = await take(c);
      assert.equal(d.offset, 30n);
      assert.equal(b.count("Subscribe"), 1);
      assert.equal(a.count("Subscribe"), 1);
      assert.equal(a.count("Unsubscribe"), 0, "the push ended it: nothing to unsubscribe");
      c.close();
    });

    test("a push with UNKNOWN_RESOURCE drops the route", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      const before = a.count("Metadata");
      s.peer.push(failure(s.id, ErrorCode.UNKNOWN_RESOURCE));
      assert.equal(await c.poll(SHORT), null);
      assert.ok(a.count("Metadata") > before, "the route was looked up again");
      assert.equal(a.count("Subscribe"), 2);
      c.close();
    });

    test("an ack timeout ends the subscription and the next read subscribes again", async () => {
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          if (subs.length > 1) {
            peer.push(pushAll(id, 50, 1));
          }
          return subscribed(id);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(failure(s.id, ErrorCode.ACK_TIMED_OUT));
      const d = await take(c);
      assert.equal(d.offset, 50n, "the message of the new subscription");
      assert.equal(a.count("Subscribe"), 2);
      assert.equal(a.connections, 1, "the connection is still good: the same one is used");
      c.close();
    });

    test("a broken connection ends the subscription and the next read subscribes again", async () => {
      serve(a, (r, peer) => {
        if (r.type === "Subscribe") {
          const id = ++nextSub;
          subs.push({ node: 1, peer, id });
          if (subs.length > 1) {
            peer.push(pushAll(id, 50, 1));
          }
          return subscribed(id);
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      (subs[0] as (typeof subs)[number]).peer.closeSocket();
      const d = await take(c);
      assert.equal(d.offset, 50n);
      assert.equal(a.count("Subscribe"), 2);
      assert.equal(a.connections, 2, "on a new connection");
      c.close();
    });

    test("messages already received are still handed out when the subscription ends", async () => {
      // The consumer reads its end only after it has handed out everything it holds; the broker gives those messages
      // to others (the delivery count then rises), so they may come twice but not zero times.
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 3));
      assert.equal((await take(c)).offset, 10n);
      s.peer.closeSocket();
      await sleep(100);
      assert.equal((await take(c)).offset, 11n);
      assert.equal((await take(c)).offset, 12n);
      assert.equal(a.count("Subscribe"), 1, "not subscribed again while messages are held");
      assert.equal(await c.poll(SHORT), null);
      assert.equal(a.count("Subscribe"), 2);
      c.close();
    });

    test("credit after an end goes to the new subscription, with the counts started over", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 2 });
      const s0 = subs[0] as (typeof subs)[number];
      s0.peer.push(failure(s0.id, ErrorCode.ACK_TIMED_OUT));
      assert.equal(await c.poll(SHORT), null);
      const s1 = subs[1] as (typeof subs)[number];
      s1.peer.push(pushAll(s1.id, 20, 2));
      await take(c);
      await eventually("a credit", () => requestsOf(s1.peer, "Credit").length === 1);
      const credit = requestsOf(s1.peer, "Credit")[0];
      assert.equal(credit?.subscriptionId, s1.id);
      assert.equal(credit?.addMsgs, 1);
      c.close();
    });
  });

  // ---- closing ----

  describe("closing", () => {
    test("close unsubscribes and gives back what was not handed out", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 4));
      await take(c); // offset 10 is handed out and stays with the consumer
      c.close();
      await eventually("the unsubscribe", () => a.count("Unsubscribe") === 1);
      await sleep(150);
      const sent = s.peer.received.map((r) => r.request);
      const un = requestsOf(s.peer, "Unsubscribe");
      const nacks = requestsOf(s.peer, "Nack");
      assert.equal(nacks.length, 1, "the three not handed out go back");
      assert.equal(un[0]?.subscriptionId, s.id);
      assert.equal(nacks[0]?.requeue, true);
      assert.equal(nacks[0]?.queueId, QUEUE_ID);
      assert.equal(nacks[0]?.subscriptionId, s.id, "names the subscription it ends (ADR 0106)");
      assert.deepEqual(nacks[0]?.ranges, [{ from: 11n, to: 14n }], "the three not handed out, as one range");
      assert.ok(sent.indexOf(un[0] as Request) < sent.indexOf(nacks[0] as Request), "unsubscribe first");
      c.close(); // twice is harmless
      await sleep(50);
      assert.equal(a.count("Unsubscribe"), 1);
    });

    test("close also gives back pushes that were not read yet", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs", { credit: 10 });
      const s = subs[0] as (typeof subs)[number];
      s.peer.push(pushAll(s.id, 10, 2));
      assert.equal((await take(c)).offset, 10n);
      // The second push arrives while nothing reads: it waits unread behind the buffered 11.
      s.peer.push(pushAll(s.id, 12, 2));
      await sleep(100);
      c.close();
      await eventually("the nack", () => requestsOf(s.peer, "Nack").length === 1);
      assert.deepEqual(requestsOf(s.peer, "Nack")[0]?.ranges, [{ from: 11n, to: 14n }], "11 was buffered, 12 and 13 unread");
    });

    test("close with nothing buffered only unsubscribes", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      c.close();
      await eventually("the unsubscribe", () => a.count("Unsubscribe") === 1);
      await sleep(100);
      assert.equal(a.count("Nack"), 0);
    });

    test("a closed consumer refuses further reads", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      c.close();
      await assert.rejects(c.poll(SHORT), ConsumerClosedError);
      await assert.rejects(c.next(), ConsumerClosedError);
    });

    test("close wakes a read that waits for a message", async () => {
      serve(a);
      const c = await connect(a).subscribe("jobs");
      const waiting = c.poll(30_000);
      await sleep(100);
      const start = performance.now();
      c.close();
      await assert.rejects(waiting, ConsumerClosedError);
      assert.ok(performance.now() - start < 5_000);
    });

    test("close while a read is subscribing again ends the new subscription", async () => {
      let release!: () => void;
      const gate = new Promise<void>((r) => {
        release = r;
      });
      let n = 0;
      serve(a, (r, peer) => {
        if (r.type === "Subscribe" && ++n === 2) {
          return gate.then(() => {
            const id = ++nextSub;
            subs.push({ node: 1, peer, id });
            return subscribed(id);
          }) as unknown as Response;
        }
        return undefined;
      });
      const c = await connect(a).subscribe("jobs");
      const s0 = subs[0] as (typeof subs)[number];
      s0.peer.push(failure(s0.id, ErrorCode.ACK_TIMED_OUT));
      const reading = c.poll(5_000);
      await eventually("the second subscribe", () => a.count("Subscribe") === 2);
      c.close();
      release();
      await assert.rejects(reading, ConsumerClosedError);
      await eventually("the second unsubscribe", () => a.count("Unsubscribe") >= 1);
      assert.equal(requestsOf((subs[1] as (typeof subs)[number]).peer, "Unsubscribe").at(-1)?.subscriptionId, (subs[1] as (typeof subs)[number]).id);
    });

    test("closing the client ends a consumer's connection and its reads fail", async () => {
      serve(a);
      const lotse = connect(a);
      const c = await lotse.subscribe("jobs");
      lotse.close();
      await assert.rejects(c.poll(SHORT));
    });
  });

  // ---- createQueue ----

  describe("createQueue", () => {
    const created = (error: ErrorCodeValue = ErrorCode.NONE): Response => ({
      type: "Create",
      error,
      resourceId: 77n,
      errorMsg: null,
      leaderHint: null,
    });

    test("it sends the tenant, the replication, the dead-letter queue and the settings", async () => {
      a.on((_n, r) => (r.type === "CreateQueue" ? created() : fx.metadata([a])));
      const lotse = new LotseClient(fx.config([a], { tenant: "acme" }));
      client = lotse;
      const id = await lotse.createQueue("jobs", {
        replication: 5,
        ackTimeoutMs: 700,
        maxDeliveries: 3,
        messageTtlMs: 60_000,
        deadLetterQueue: "dead",
        singleActiveConsumer: true,
        priorityLevels: 4,
        settings: { "x.other": "1" },
      });
      assert.equal(id, 77n);
      const req = a.requests("CreateQueue")[0];
      assert.ok(req);
      assert.equal(req.name, "jobs");
      assert.equal(req.tenant, "acme");
      assert.equal(req.replication, 5);
      assert.equal(req.dlq, "dead");
      assert.equal(req.priorityLevels, 1);
      assert.equal(req.singleActive, false);
      assert.deepEqual(req.configs, [
        { key: "ack.timeout.ms", value: "700" },
        { key: "max.deliveries", value: "3" },
        { key: "message.ttl.ms", value: "60000" },
        { key: "priority.levels", value: "4" },
        { key: "single.active.consumer", value: "true" },
        { key: "x.other", value: "1" },
      ]);
    });

    test("the defaults are three nodes and no settings", async () => {
      a.on((_n, r) => (r.type === "CreateQueue" ? created() : fx.metadata([a])));
      await connect(a).createQueue("jobs");
      const req = a.requests("CreateQueue")[0];
      assert.equal(req?.replication, 3);
      assert.equal(req?.dlq, null);
      assert.deepEqual(req?.configs, []);
      assert.equal(req?.tenant, "default");
    });

    test("a queue that exists says which one", async () => {
      a.on((_n, r) => (r.type === "CreateQueue" ? created(ErrorCode.RESOURCE_EXISTS) : fx.metadata([a])));
      await assert.rejects(connect(a).createQueue("jobs"), (e: unknown) => {
        assert.equal((e as { resourceId?: bigint }).resourceId, 77n);
        return true;
      });
    });

    test("settings out of range are refused before anything is sent", async () => {
      a.on(() => {
        throw new Error("nothing should be sent");
      });
      const lotse = connect(a);
      await assert.rejects(lotse.createQueue("q", { replication: 0 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { replication: 256 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { priorityLevels: 11 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { priorityLevels: 0 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { maxDeliveries: -1 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { ackTimeoutMs: 1.5 }), RangeError);
      await assert.rejects(lotse.createQueue("q", { maxDeliveries: 2, settings: { "max.deliveries": "3" } }), RangeError);
      assert.equal(a.requests().length, 0);
    });
  });
});

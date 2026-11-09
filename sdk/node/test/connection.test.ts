import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  address,
  BrokerError,
  Connection,
  ConnectionError,
  ErrorCode,
  MAX_FRAME,
  ORPHAN_SUBSCRIPTIONS,
  RequestTimeoutError,
  encodePush,
  encodeResponse,
  withLengthPrefix,
  Writer,
  type DecodedRequest,
  type DeliverPush,
  type PushListener,
  type Request,
  type Response,
} from "../src/index.js";
import { eventually } from "./support/lotse.js";
import { type FakeBroker, type Peer, withBroker } from "./support/fake-broker.js";

const plain = { plaintextOnLoopback: true, requestTimeoutMs: 5_000 } as const;

/** A request whose answer tells which one it answers. */
const ask = (n: number): Request => ({ type: "Unsubscribe", subscriptionId: n });
const answer = (n: number): Response => ({ type: "Outcome", error: n, leaderHint: null });
const code = (r: Response): number => (r as Extract<Response, { type: "Outcome" }>).error;
const subscription = (r: DecodedRequest): number => (r.request as Extract<Request, { type: "Unsubscribe" }>).subscriptionId;

function push(subscriptionId: number, offset: bigint): DeliverPush {
  return { subscriptionId, error: ErrorCode.NONE, batch: new Uint8Array(0), deliveries: [{ offset, deliveryCount: 1 }], leaderHint: null };
}

/** Collects what a subscription is told. */
class Inbox implements PushListener {
  readonly pushes: DeliverPush[] = [];
  readonly closed: ConnectionError[] = [];
  onPush(p: DeliverPush): void {
    this.pushes.push(p);
  }
  onClose(r: ConnectionError): void {
    this.closed.push(r);
  }
  async nextOffset(): Promise<bigint> {
    await eventually("a push", () => this.pushes.length > 0);
    return (this.pushes.shift() as DeliverPush).deliveries[0]?.offset as bigint;
  }
}

async function using(
  handler: (peer: Peer, req: DecodedRequest) => void | Promise<void>,
  body: (c: Connection, broker: FakeBroker) => Promise<void>,
  options: Parameters<typeof Connection.open>[1] = plain,
): Promise<void> {
  await withBroker(handler, async (broker) => {
    const c = await Connection.open(broker.address, options);
    try {
      await body(c, broker);
    } finally {
      c.close();
    }
  });
}

test("requests are pipelined and answered in any order", async () => {
  const held: DecodedRequest[] = [];
  await using(
    (peer, req) => {
      held.push(req);
      if (held.length === 3) {
        // All three are on the wire before any answer: pipelined. Answer the last first.
        for (let i = 2; i >= 0; i--) {
          const r = held[i] as DecodedRequest;
          peer.reply(r.header.correlationId, answer(subscription(r)));
        }
      }
    },
    async (c) => {
      const [a, b, d] = [c.send(ask(10)), c.send(ask(20)), c.send(ask(30))];
      assert.equal(code(await a), 10);
      assert.equal(code(await b), 20);
      assert.equal(code(await d), 30);
      assert.deepEqual(
        held.map((r) => r.header.correlationId),
        [1, 2, 3],
        "correlation ids start at 1; 0 is for pushes",
      );
    },
  );
});

test("a response is matched to its request by correlation id, not by order", async () => {
  // The node answers 1, 2, 3 in the order 3, 1, 2, each saying which request it is for.
  const held: DecodedRequest[] = [];
  await using(
    (peer, req) => {
      held.push(req);
      if (held.length === 3) {
        for (const i of [2, 0, 1]) {
          const r = held[i] as DecodedRequest;
          peer.reply(r.header.correlationId, answer(subscription(r) + 1));
        }
      }
    },
    async (c) => {
      const results = await Promise.all([1, 2, 3].map((n) => c.call(ask(n))));
      assert.deepEqual(results.map(code), [2, 3, 4]);
    },
  );
});

test("many callers may share one connection", async () => {
  await using(
    (peer, req) => peer.reply(req.header.correlationId, answer(subscription(req))),
    async (c) => {
      const calls: Promise<void>[] = [];
      let wrong = 0;
      for (let t = 0; t < 8; t++) {
        calls.push(
          (async () => {
            for (let i = 0; i < 100; i++) {
              if (code(await c.call(ask(t * 1000 + i))) !== t * 1000 + i) {
                wrong++;
              }
            }
          })(),
        );
      }
      await Promise.all(calls);
      assert.equal(wrong, 0);
    },
  );
});

test("credit is never answered and has no response slot", async () => {
  const seen: DecodedRequest[] = [];
  await using(
    (peer, req) => {
      seen.push(req);
      if (req.request.type !== "Credit") {
        peer.reply(req.header.correlationId, answer(1));
      }
    },
    async (c) => {
      await assert.rejects(c.send({ type: "Credit", subscriptionId: 5, addMsgs: 1, addBytes: 1 }), TypeError);
      c.sendOnly({ type: "Credit", subscriptionId: 5, addMsgs: 10, addBytes: 1000 });
      assert.equal(code(await c.call(ask(1))), 1);
      assert.equal(seen.length, 2);
      assert.equal(seen[0]?.request.type, "Credit");
      assert.equal(seen[0]?.header.correlationId, 1);
      assert.equal(seen[1]?.header.correlationId, 2, "Credit used an id, and nothing waits for it");
    },
  );
});

test("a response nobody waits for is dropped", async () => {
  await using(
    (peer, req) => {
      peer.reply(777, answer(0)); // no such request
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      assert.equal(code(await c.call(ask(1))), 1);
      assert.equal(code(await c.call(ask(1))), 1);
      assert.ok(c.isOpen);
    },
  );
});

test("pushes before the subscription is registered are kept and delivered in order", async () => {
  await using(
    (peer, req) => {
      const n = subscription(req);
      if (n === 1) {
        // The subscription's pushes come before anyone registered for it.
        peer.push(push(7, 100n));
        peer.push(push(7, 101n));
        peer.push(push(8, 200n));
        peer.push(push(7, 102n));
      }
      if (n === 2) {
        peer.push(push(7, 103n));
      }
      peer.reply(req.header.correlationId, answer(n));
    },
    async (c) => {
      await c.call(ask(1)); // when this returns, the reader has routed the four pushes before it
      const seven = new Inbox();
      const eight = new Inbox();
      c.register(7, seven);
      assert.equal(await seven.nextOffset(), 100n);
      assert.equal(await seven.nextOffset(), 101n);
      assert.equal(await seven.nextOffset(), 102n);
      c.register(8, eight);
      assert.equal(await eight.nextOffset(), 200n);
      await c.call(ask(2));
      assert.equal(await seven.nextOffset(), 103n, "later pushes follow the kept ones");
      await sleep(50);
      assert.equal(seven.pushes.length, 0);
      c.unregister(7);
      c.register(7, new Inbox());
    },
  );
});

test("only sixty-four subscriptions are kept waiting", async () => {
  assert.equal(ORPHAN_SUBSCRIPTIONS, 64);
  await using(
    (peer, req) => {
      for (let sub = 1; sub <= 70; sub++) {
        peer.push(push(sub, BigInt(sub)));
      }
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      await c.call(ask(1));
      const evicted = new Inbox();
      const oldestKept = new Inbox();
      const newest = new Inbox();
      c.register(6, evicted);
      c.register(7, oldestKept);
      c.register(70, newest);
      await sleep(50);
      assert.equal(evicted.pushes.length, 0, "the lowest ids made room");
      assert.equal(await oldestKept.nextOffset(), 7n);
      assert.equal(await newest.nextOffset(), 70n);
    },
  );
});

test("unregistering forgets the kept pushes", async () => {
  await using(
    (peer, req) => {
      peer.push(push(5, 1n));
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      await c.call(ask(1));
      c.unregister(5);
      const late = new Inbox();
      c.register(5, late);
      await sleep(50);
      assert.equal(late.pushes.length, 0);
    },
  );
});

test("pushes can be read as an async iterable, which ends with the connection's error", async () => {
  await using(
    (peer, req) => {
      peer.push(push(9, 1n));
      peer.push(push(9, 2n));
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      const stream = c.pushes(9);
      await c.call(ask(1));
      const seen: bigint[] = [];
      const reading = (async () => {
        for await (const p of stream) {
          seen.push(p.deliveries[0]?.offset as bigint);
        }
      })();
      await eventually("two pushes", () => seen.length === 2);
      c.close();
      await assert.rejects(reading, ConnectionError);
      assert.deepEqual(seen, [1n, 2n]);
    },
  );
});

test("leaving an async iterable early unregisters the subscription", async () => {
  await using(
    (peer, req) => {
      peer.push(push(9, 1n));
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      const stream = c.pushes(9);
      await c.call(ask(1));
      for await (const p of stream) {
        assert.equal(p.deliveries[0]?.offset, 1n);
        break;
      }
      const again = new Inbox();
      c.register(9, again); // free again
      await c.call(ask(1)); // this one pushes to the new listener
      assert.equal(await again.nextOffset(), 1n);
    },
  );
});

test("a response with the error flag becomes a BrokerError before any typed decoding", async () => {
  await using(
    (peer, req) => {
      if (req.request.type === "Metadata") {
        // A body that is only an error code, for a request whose answer has a long typed body.
        peer.reply(req.header.correlationId, { type: "Error", code: ErrorCode.UNAUTHORIZED });
      } else {
        peer.reply(req.header.correlationId, { type: "Refused", code: ErrorCode.INVALID_REQUEST, message: "no such thing" });
      }
    },
    async (c) => {
      await assert.rejects(c.send({ type: "Metadata", names: ["x"] }), (e: unknown) => {
        assert.ok(e instanceof BrokerError);
        assert.equal(e.code, ErrorCode.UNAUTHORIZED);
        assert.equal(e.reason, null);
        return true;
      });
      await assert.rejects(c.send(ask(1)), (e: unknown) => {
        assert.ok(e instanceof BrokerError);
        assert.equal(e.code, ErrorCode.INVALID_REQUEST);
        assert.equal(e.reason, "no such thing");
        assert.ok(e.message.includes("no such thing"));
        return true;
      });
      assert.ok(c.isOpen, "a refused request does not end the connection");
    },
  );
});

test("a timed out request fails and the late answer changes nothing", async () => {
  const peers: Peer[] = [];
  const ids: number[] = [];
  await using(
    (peer, req) => {
      peers.push(peer);
      ids.push(req.header.correlationId);
      if (ids.length > 1) {
        peer.reply(req.header.correlationId, answer(2));
      }
    },
    async (c) => {
      const start = Date.now();
      await assert.rejects(c.send(ask(1)), (e: unknown) => {
        assert.ok(e instanceof RequestTimeoutError);
        assert.ok(e.requestSent, "the outcome of a timed out request is unknown");
        return true;
      });
      assert.ok(Date.now() - start >= 140);
      peers[0]?.reply(ids[0] as number, answer(1)); // too late
      assert.equal(code(await c.call(ask(2))), 2);
      assert.ok(c.isOpen);
    },
    { plaintextOnLoopback: true, requestTimeoutMs: 150 },
  );
});

test("closing fails every pending request and notifies subscriptions", async () => {
  await withBroker(
    () => {},
    async (broker) => {
      const c = await Connection.open(broker.address, plain);
      const inbox = new Inbox();
      c.register(3, inbox);
      const a = c.send(ask(1));
      const b = c.send(ask(2));
      c.close();
      for (const f of [a, b]) {
        await assert.rejects(f, (e: unknown) => e instanceof ConnectionError && e.requestSent);
      }
      assert.equal(inbox.closed.length, 1);
      assert.ok(!c.isOpen);
      await assert.rejects(c.send(ask(3)), (e: unknown) => e instanceof ConnectionError && !e.requestSent, "nothing was sent on a closed connection");
      assert.throws(() => c.register(4, new Inbox()), ConnectionError);
      assert.throws(() => c.sendOnly({ type: "Credit", subscriptionId: 1, addMsgs: 1, addBytes: 1 }), ConnectionError);
      c.close(); // closing twice is fine
      assert.ok((await c.closed) instanceof ConnectionError);
    },
  );
});

test("the broker closing the connection fails pending requests", async () => {
  await using(
    (peer) => peer.closeSocket(),
    async (c) => {
      const inbox = new Inbox();
      c.register(3, inbox);
      await assert.rejects(c.send(ask(1)), (e: unknown) => e instanceof ConnectionError && e.message.includes("closed"));
      await eventually("the subscription to hear", () => inbox.closed.length === 1);
      await eventually("the connection to end", () => !c.isOpen);
    },
  );
});

test("a frame over the limit ends the connection", async () => {
  await using(
    (peer) => peer.rawBytes(new Uint8Array([1, 0, 0, 1])),
    async (c) => {
      await assert.rejects(c.send(ask(1)), (e: unknown) => e instanceof ConnectionError && e.message.includes("limit"));
      assert.ok(!c.isOpen);
    },
  );
});

test("a request over the limit is not sent and the connection stays usable", async () => {
  await using(
    (peer, req) => peer.reply(req.header.correlationId, answer(1)),
    async (c) => {
      const big = new Uint8Array(MAX_FRAME + 1);
      await assert.rejects(c.send({ type: "SaslAuthenticate", authBytes: big }), (e: unknown) => e instanceof ConnectionError && !e.requestSent);
      assert.equal(code(await c.call(ask(1))), 1);
    },
  );
});

test("an undecodable response ends the connection", async () => {
  await using(
    (peer, req) => {
      const w = new Writer();
      w.u32(req.header.correlationId).u16(0).noTags();
      w.u8(9); // far too short for a Metadata response
      peer.raw(w.toBytes());
    },
    async (c) => {
      await assert.rejects(c.send({ type: "Metadata", names: [] }), (e: unknown) => e instanceof ConnectionError && e.message.includes("undecodable"));
      await eventually("the connection to end", () => !c.isOpen);
    },
  );
});

test("a frame that arrives in pieces, and frames that arrive together, are put right", async () => {
  await using(
    (peer, req) => {
      const id = req.header.correlationId;
      const one = withLengthPrefix(encodeResponse(id, answer(1)));
      if (id === 1) {
        // One byte at a time, so the length prefix itself arrives in pieces.
        for (const b of one) {
          peer.rawBytes(new Uint8Array([b]));
        }
      } else {
        // Three whole frames in one write: a push, this answer, and a push that follows it.
        peer.rawBytes(
          Buffer.concat([withLengthPrefix(encodePush(push(4, 1n))), one, withLengthPrefix(encodePush(push(4, 2n)))]),
        );
      }
    },
    async (c) => {
      const inbox = new Inbox();
      c.register(4, inbox);
      assert.equal(code(await c.call(ask(1))), 1);
      assert.equal(code(await c.call(ask(1))), 1);
      assert.equal(await inbox.nextOffset(), 1n);
      assert.equal(await inbox.nextOffset(), 2n);
    },
  );
});

test("api versions is optional but available", async () => {
  await using(
    (peer, req) => {
      assert.equal(req.request.type, "ApiVersions");
      peer.reply(req.header.correlationId, { type: "ApiVersions", versions: [{ apiKey: 3, min: 0, max: 0 }], features: ["streams"] });
    },
    async (c) => {
      const v = await c.apiVersions();
      assert.deepEqual(v.features, ["streams"]);
      assert.equal(v.versions.length, 1);
    },
  );
});

test("a wrong type of response is reported", async () => {
  await using(
    (peer, req) => peer.reply(req.header.correlationId, answer(1)),
    async (c) => {
      await assert.rejects(c.call(ask(1), "Metadata"), ConnectionError);
    },
  );
});

test("connecting to nothing fails without hanging", async () => {
  await assert.rejects(
    Connection.open(address("127.0.0.1", 1), plain),
    (e: unknown) => e instanceof ConnectionError && !e.requestSent,
  );
});

test("correlation ids wrap past zero", async () => {
  const ids: number[] = [];
  await using(
    (peer, req) => {
      ids.push(req.header.correlationId);
      peer.reply(req.header.correlationId, answer(1));
    },
    async (c) => {
      // Reach the end of the u32 range without sending four billion requests.
      (c as unknown as { lastId: number }).lastId = 0xffff_fffe;
      await c.call(ask(1));
      await c.call(ask(1));
      await c.call(ask(1));
      assert.deepEqual(ids, [0xffff_ffff, 1, 2], "0 marks pushes and is skipped");
    },
  );
});

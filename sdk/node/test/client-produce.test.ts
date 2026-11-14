import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BrokerResponseError,
  ClientAuthenticationError,
  CoordinatorType,
  ErrorCode,
  type ErrorCodeValue,
  GaveUpError,
  LotseClient,
  type OutMessage,
  type Response,
  UnknownOutcomeError,
  UnknownStreamError,
} from "../src/index.js";
import { eventually } from "./support/lotse.js";
import * as fx from "./support/fx.js";
import { closeAll, DROP, FakeNode } from "./support/fake-node.js";

/** Idempotent and plain produce against a scripted broker. */

const messages = (n: number): OutMessage[] => Array.from({ length: n }, (_, i) => ({ value: `m${i}` }));

const batchOf = (r: Parameters<typeof fx.bytesOf>[0]): Batch => Batch.parse(fx.bytesOf(r)).batch;

describe("produce against a scripted broker", () => {
  let node: FakeNode;
  const clients: LotseClient[] = [];
  let nextProducerId = 100n;
  let offsets = 0n;

  beforeEach(async () => {
    node = await FakeNode.start(1);
    nextProducerId = 100n;
    offsets = 0n;
  });

  afterEach(async () => {
    for (const c of clients.splice(0)) {
      c.close();
    }
    await closeAll([node]);
  });

  /** The broker serves metadata (partitions 0 and 1, both led by node 1), a coordinator, and `produce`. */
  function serve(produce: (n: number) => Response | typeof DROP): void {
    let produces = 0;
    node.on((n, request) => {
      switch (request.type) {
        case "Metadata":
          return fx.metadata([n], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1), fx.partition(1, 1, 1, 1)));
        case "FindCoordinator":
          return fx.coordinator(n);
        case "InitProducer":
          return fx.producer(nextProducerId++, 3);
        case "Produce":
          return produce(++produces);
        default:
          throw new Error(`unexpected ${request.type}`);
      }
    });
  }

  const ok = (): Response => fx.produced(fx.STREAM_ID, 0, offsets++);

  function client(idempotent: boolean, extra: object = {}): LotseClient {
    const c = new LotseClient(fx.config([node], { idempotent, retryForMs: 400, ...extra }));
    clients.push(c);
    return c;
  }

  test("an idempotent producer asks for its id once and counts records per partition", async () => {
    serve(() => ok());
    const lotse = client(true);
    await lotse.produce("s", 0, messages(2));
    await lotse.produce("s", 0, messages(1));
    await lotse.produce("s", 0, messages(3));
    await lotse.produce("s", 1, messages(1));

    const find = node.requests("FindCoordinator");
    assert.equal(find.length, 1);
    assert.equal(find[0]?.keyType, CoordinatorType.PRODUCER);
    assert.equal(find[0]?.key, "");
    const inits = node.requests("InitProducer");
    assert.equal(inits.length, 1);
    assert.equal(inits[0]?.txnId, null, "an idempotent producer has no transactional id");

    const sent = node.requests("Produce");
    assert.deepEqual([0, 1, 2].map((i) => batchOf(sent[i] as never).baseSequence), [0, 2, 3]);
    assert.equal(batchOf(sent[3] as never).baseSequence, 0, "another partition has its own sequence");
    for (const p of sent) {
      const b = batchOf(p);
      assert.equal(b.producerId, 100n);
      assert.equal(b.producerEpoch, 3);
      assert.equal(b.attributes.transactional, false);
    }
    assert.equal(sent[0]?.timeoutMs, 5000, "produce_timeout goes to the leader");
  });

  test("a batch whose outcome is unknown is sent again with the same sequence", async () => {
    // The first answer is lost with the connection, the second says the quorum timed out, the third works.
    serve((n) => (n === 1 ? DROP : n === 2 ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.REQUEST_TIMED_OUT) : ok()));
    const lotse = client(true);
    await lotse.produce("s", 0, messages(2));
    await lotse.produce("s", 0, messages(1));

    const sent = node.requests("Produce");
    assert.equal(sent.length, 4);
    assert.deepEqual(fx.bytesOf(sent[0] as never), fx.bytesOf(sent[1] as never), "the same bytes again");
    assert.deepEqual(fx.bytesOf(sent[0] as never), fx.bytesOf(sent[2] as never), "the same bytes again");
    assert.equal(batchOf(sent[2] as never).baseSequence, 0);
    assert.equal(batchOf(sent[3] as never).baseSequence, 2, "advanced once, by the two records");
    assert.equal(node.count("InitProducer"), 1, "the producer id stayed");
  });

  /**
   * Stream `s` has the resource id in `script.current`. A produce to another id is refused with NOT_LEADER (nothing
   * written), as a node that applied the delete would; with `swallowThen` set, the next produce's answer is lost and the
   * stream moves to that id meanwhile (ADR 0107).
   */
  function serveIncarnations(script: { current: bigint; swallowThen: bigint }): void {
    node.on((n, request) => {
      switch (request.type) {
        case "Metadata":
          return fx.metadata([n], fx.stream("s", script.current, fx.partition(0, 1, 1, 1)));
        case "FindCoordinator":
          return fx.coordinator(n);
        case "InitProducer":
          return fx.producer(nextProducerId++, 3);
        case "Produce": {
          const id = request.resources[0]?.resourceId ?? 0n;
          if (script.swallowThen !== 0n) {
            script.current = script.swallowThen;
            script.swallowThen = 0n;
            return DROP;
          }
          return id === script.current ? fx.produced(id, 0, offsets++) : fx.produced(id, 0, 0, ErrorCode.NOT_LEADER);
        }
        default:
          throw new Error(`unexpected ${request.type}`);
      }
    });
  }

  const incarnationsSent = (): string[] =>
    node.requests("Produce").map((p) => {
      const b = batchOf(p);
      return `${p.resources[0]?.resourceId}/${b.producerId}/${b.baseSequence}`;
    });

  test("each incarnation of a stream counts its own sequences", async () => {
    const script = { current: 7n, swallowThen: 0n };
    serveIncarnations(script);
    const lotse = client(true);
    await lotse.produce("s", 0, messages(1));
    await lotse.produce("s", 0, messages(1));
    // Deleted and created again: the cached route still names 7, which is refused; the produce does not follow the
    // name to 9.
    script.current = 9n;
    await assert.rejects(lotse.produce("s", 0, messages(1)), UnknownStreamError);
    await lotse.produce("s", 0, messages(1));
    // A node behind still serves 7: back there, its sequences go on.
    script.current = 7n;
    await assert.rejects(lotse.produce("s", 0, messages(1)), UnknownStreamError);
    await lotse.produce("s", 0, messages(1));
    assert.deepEqual(
      incarnationsSent(),
      ["7/100/0", "7/100/1", "7/100/2", "9/100/0", "9/100/1", "7/100/2"],
      "a refused batch keeps its sequence; 9 starts at 0; 7 goes on at 2",
    );
  });

  test("a produce in doubt when its stream moves starts a new producer", async () => {
    const script = { current: 7n, swallowThen: 0n };
    serveIncarnations(script);
    const lotse = client(true, { requestTimeoutMs: 200 });
    await lotse.produce("s", 0, messages(1));
    // The next produce's answer is lost while the stream is created again: it may be in 7.
    script.swallowThen = 9n;
    await assert.rejects(lotse.produce("s", 0, messages(1)), UnknownStreamError);
    await lotse.produce("s", 0, messages(1));
    const sent = incarnationsSent();
    assert.equal(sent[1], "7/100/1");
    assert.equal(sent.at(-1), "9/101/0", "a new producer id");
  });

  test("an outcome still unknown when time runs out is reported, and the next produce gets a new producer id", async () => {
    let phase = 0;
    serve(() => (phase === 0 ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.REQUEST_TIMED_OUT) : ok()));
    const lotse = client(true);

    await assert.rejects(lotse.produce("s", 0, messages(2)), (e: unknown) => {
      assert.ok(e instanceof UnknownOutcomeError);
      assert.match(e.message, /still unknown/);
      return true;
    });
    const sent = node.requests("Produce");
    assert.ok(sent.length >= 2, "it retried while it could");
    for (const p of sent) {
      assert.deepEqual(fx.bytesOf(p), fx.bytesOf(sent[0] as never), "always the same sequence");
    }
    assert.equal(lotse.producerId(), 0n, "the session was discarded");

    phase = 1;
    await lotse.produce("s", 0, messages(1));
    assert.equal(node.count("InitProducer"), 2, "a new producer id");
    const next = batchOf(node.requests("Produce")[sent.length] as never);
    assert.equal(next.producerId, 101n);
    assert.equal(next.baseSequence, 0, "the new id starts at 0");
  });

  for (const code of [ErrorCode.OUT_OF_ORDER_SEQUENCE, ErrorCode.PRODUCER_FENCED]) {
    test(`error ${code} is reported and starts over with a new producer id`, async () => {
      let phase = 0;
      serve(() => (phase === 0 ? fx.produced(fx.STREAM_ID, 0, 0, code) : ok()));
      const lotse = client(true);
      await assert.rejects(lotse.produce("s", 0, messages(1)), (e: unknown) => {
        assert.ok(e instanceof BrokerResponseError);
        assert.equal(e.code, code);
        return true;
      });
      assert.equal(lotse.producerId(), 0n, "discarded");
      phase = 1;
      const before = nextProducerId;
      await lotse.produce("s", 0, messages(1));
      assert.equal(nextProducerId, before + 1n, "a new producer id");
    });
  }

  test("a named producer asks under its name and stays fenced once a newer instance took over", async () => {
    let phase = 0;
    serve(() => (phase === 0 ? ok() : fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.PRODUCER_FENCED)));
    const lotse = client(true, { producerName: "orders-writer" });
    await lotse.produce("s", 0, messages(1));
    assert.equal(node.requests("InitProducer")[0]?.txnId, "orders-writer", "asked by name");
    assert.equal(node.requests("FindCoordinator")[0]?.key, "orders-writer");
    phase = 1;
    for (let i = 0; i < 2; i++) {
      await assert.rejects(lotse.produce("s", 0, messages(1)), (e: unknown) => {
        assert.ok(e instanceof BrokerResponseError);
        assert.equal(e.code, ErrorCode.PRODUCER_FENCED);
        return true;
      });
    }
    assert.equal(node.count("InitProducer"), 1, "no new epoch that would fence the newer one");
    assert.throws(() => new LotseClient(fx.config([node], { producerName: "a\0b" })), RangeError);
    assert.throws(() => new LotseClient(fx.config([node], { producerName: "" })), RangeError);
  });

  test("a definite failure keeps the session and the sequence", async () => {
    let phase = 0;
    serve(() => (phase === 0 ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.QUOTA_EXCEEDED) : ok()));
    const lotse = client(true);
    await assert.rejects(lotse.produce("s", 0, messages(2)), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.QUOTA_EXCEEDED);
      return true;
    });
    assert.equal(lotse.producerId(), 100n, "the producer id is kept");
    phase = 1;
    await lotse.produce("s", 0, messages(1));
    assert.equal(node.count("InitProducer"), 1);
    assert.equal(batchOf(node.requests("Produce")[1] as never).baseSequence, 0, "the sequence of a batch that was not written is free");
  });

  test("a partition full of producers is asked again with the same bytes until it takes this one", async () => {
    // ADR 0110: nothing was written, and the partition takes the producer once it forgot an idle one.
    serve((n) => (n <= 2 ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.PRODUCER_LIMIT) : ok()));
    const lotse = client(true);
    await lotse.produce("s", 0, messages(2));
    const sent = node.requests("Produce");
    assert.equal(sent.length, 3);
    assert.deepEqual(fx.bytesOf(sent[0] as never), fx.bytesOf(sent[2] as never), "the same bytes again");
    assert.equal(node.count("InitProducer"), 1, "the producer id stayed");
  });

  test("produce to the same partition is serialised and to others is not", async () => {
    let release!: () => void;
    const released = new Promise<void>((r) => {
      release = r;
    });
    let inFlight = 0;
    let maxInFlightOnPartition0 = 0;
    node.on(async (n, request) => {
      if (request.type === "Metadata") {
        return fx.metadata([n], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1), fx.partition(1, 1, 1, 1)));
      }
      if (request.type === "FindCoordinator") {
        return fx.coordinator(n);
      }
      if (request.type === "InitProducer") {
        return fx.producer(100, 1);
      }
      const p = fx.produceOf(request).resources[0]?.partitions[0];
      const partition = p?.partition ?? -1;
      if (partition === 0) {
        inFlight++;
        maxInFlightOnPartition0 = Math.max(maxInFlightOnPartition0, inFlight);
        await released;
        inFlight--;
      }
      return fx.produced(fx.STREAM_ID, partition, 0);
    });
    const lotse = client(true, { retryForMs: 5_000 });
    await lotse.metadata("s");
    await lotse.produce("s", 1, messages(1));
    const sentBefore = node.count("Produce");

    const first = lotse.produce("s", 0, messages(1));
    await eventually("the first produce to arrive", () => node.count("Produce") === sentBefore + 1);
    const second = lotse.produce("s", 0, messages(1));
    // Another partition is not held up by the first.
    await lotse.produce("s", 1, messages(1));
    await sleep(300);
    assert.equal(node.count("Produce"), sentBefore + 2, "the second produce to partition 0 waits its turn");

    release();
    await Promise.all([first, second]);
    assert.equal(maxInFlightOnPartition0, 1, "never two in flight to one partition");
    const seqs = node
      .requests("Produce")
      .filter((p) => p.resources[0]?.partitions[0]?.partition === 0)
      .map((p) => batchOf(p).baseSequence);
    assert.deepEqual(seqs, [0, 1], "sequences reach the broker in order");
  });

  test("without idempotence there is no producer id and an unknown outcome is never retried", async () => {
    serve(() => fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.REQUEST_TIMED_OUT));
    const lotse = client(false);
    await assert.rejects(lotse.produce("s", 0, messages(1)), UnknownOutcomeError);
    assert.equal(node.count("Produce"), 1, "not sent again");
    assert.equal(node.count("FindCoordinator") + node.count("InitProducer"), 0);
    assert.equal(batchOf(node.requests("Produce")[0] as never).producerId, 0n, "not stamped");
  });

  test("without idempotence a lost connection after sending is never retried", async () => {
    serve(() => DROP);
    const lotse = client(false);
    await assert.rejects(lotse.produce("s", 0, messages(1)), UnknownOutcomeError);
    assert.equal(node.count("Produce"), 1);
  });

  test("without idempotence a leader error is still retried because nothing was written", async () => {
    serve((n) => (n === 1 ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER) : ok()));
    await client(false).produce("s", 0, messages(1));
    assert.equal(node.count("Produce"), 2);
  });

  test("a connection that could not be opened is retried even without idempotence", async () => {
    const dead = await FakeNode.start(2);
    await dead.close();
    // The metadata names a leader that is down for the first attempt: nothing is sent, so a retry is safe.
    let asked = 0;
    node.on((n, request) => {
      if (request.type === "Metadata") {
        const first = ++asked === 1;
        return fx.metadata([n, dead], fx.stream("s", fx.STREAM_ID, fx.partition(0, first ? 2 : 1, 1, 1, 2)));
      }
      return fx.produced(fx.STREAM_ID, 0, 11);
    });
    assert.equal((await client(false).produce("s", 0, messages(1))).baseOffset, 11n);
    assert.equal(node.count("Produce"), 1);
  });

  test("an authentication failure after a lost attempt is reported as unknown", async () => {
    let n = 0;
    node.on((self, request) => {
      switch (request.type) {
        case "Metadata":
          return fx.metadata([self], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1)));
        case "FindCoordinator":
          return fx.coordinator(self);
        case "InitProducer":
          return fx.producer(100, 1);
        default:
          return ++n === 1 ? DROP : { type: "Error", code: ErrorCode.UNAUTHORIZED };
      }
    });
    await assert.rejects(client(true).produce("s", 0, messages(1)), (e: unknown) => {
      assert.ok(e instanceof UnknownOutcomeError);
      assert.match(e.message, /may have been carried out/);
      return true;
    });
  });

  test("a produce whose login is refused fails at once as authentication, not after the retry time", async () => {
    node.on(() => ({ type: "Error", code: ErrorCode.UNAUTHORIZED }));
    const lotse = client(true, { retryForMs: 30_000 });
    const start = performance.now();
    await assert.rejects(lotse.produce("s", 0, messages(1)), ClientAuthenticationError);
    assert.ok(performance.now() - start < 5_000);
  });

  test("the timestamp defaults to now and an explicit one is kept", async () => {
    serve(() => ok());
    const lotse = client(true);
    const before = BigInt(Date.now());
    await lotse.produce("s", 0, [{ value: "a" }, { value: "b", timestamp: 1234 }]);
    const after = BigInt(Date.now());
    const records = batchOf(node.requests("Produce")[0] as never).records();
    assert.ok((records[0]?.timestamp as bigint) >= before && (records[0]?.timestamp as bigint) <= after, "now, not 0");
    assert.equal(records[1]?.timestamp, 1234n);
  });

  test("keys, values and headers arrive in the batch, null apart from empty", async () => {
    serve(() => ok());
    await client(true).produce("s", 0, {
      value: "v",
      key: "k",
      headers: [
        { name: "h", value: "x" },
        { name: "n", value: null },
        { name: "e", value: new Uint8Array(0) },
      ],
    });
    const rec = batchOf(node.requests("Produce")[0] as never).records()[0];
    assert.equal(Buffer.from(rec?.key as Uint8Array).toString(), "k");
    assert.equal(Buffer.from(rec?.value as Uint8Array).toString(), "v");
    assert.equal(rec?.headers.length, 3);
    assert.equal(Buffer.from(rec?.headers[0]?.value as Uint8Array).toString(), "x");
    assert.equal(rec?.headers[1]?.value, null);
    assert.equal(rec?.headers[2]?.value?.length, 0, "empty, not null");
  });

  test("an empty list is refused", async () => {
    serve(() => ok());
    await assert.rejects(client(true).produce("s", 0, []), RangeError);
  });

  test("a rejected promise carries the client error", async () => {
    serve(() => fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.QUOTA_EXCEEDED));
    await assert.rejects(client(true).produce("s", 0, messages(1)), BrokerResponseError);
  });

  test("a batch that does not parse is refused as a corrupt batch before anything is sent", async () => {
    serve(() => ok());
    const lotse = client(true);
    await assert.rejects(lotse.produceBatch("s", 0, new Uint8Array(10)), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.CORRUPT_BATCH);
      return true;
    });
    assert.equal(node.count("Produce"), 0);
  });

  test("the batch the caller passed is not changed by stamping", async () => {
    serve(() => ok());
    const lotse = client(true);
    const batch = (await import("../src/index.js")).buildBatch([{ value: "x" }]);
    const copy = batch.slice();
    await lotse.produceBatch("s", 0, batch);
    assert.deepEqual(batch, copy);
  });

  test("a produce given up while its answer is outstanding is unknown and the client starts over", async () => {
    // Reusing the sequence of an abandoned batch could let the leader take the next one for it (PROTOCOL.md).
    let hold = true;
    node.on(async (n, request) => {
      switch (request.type) {
        case "Metadata":
          return fx.metadata([n], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1)));
        case "FindCoordinator":
          return fx.coordinator(n);
        case "InitProducer":
          return fx.producer(nextProducerId++, 1);
        default:
          return hold ? null : fx.produced(fx.STREAM_ID, 0, 5);
      }
    });
    const lotse = client(true, { retryForMs: 5_000, requestTimeoutMs: 5_000 });
    const abort = new AbortController();
    const pending = lotse.produce("s", 0, messages(1), { signal: abort.signal });
    await eventually("the produce to arrive", () => node.count("Produce") === 1);
    abort.abort();
    await assert.rejects(pending, UnknownOutcomeError);
    assert.equal(lotse.producerId(), 0n, "the session was dropped");
    hold = false;
    await lotse.produce("s", 0, messages(1));
    assert.equal(node.count("InitProducer"), 2, "a new producer id");
    assert.equal(batchOf(node.requests("Produce")[1] as never).producerId, 101n);
    assert.equal(batchOf(node.requests("Produce")[1] as never).baseSequence, 0);
  });

  test("a produce aborted before it was sent gives up without a producer session change", async () => {
    serve(() => ok());
    const lotse = client(true);
    const abort = new AbortController();
    abort.abort();
    await assert.rejects(lotse.produce("s", 0, messages(1), { signal: abort.signal }), GaveUpError);
    assert.equal(node.count("Produce"), 0);
  });

  test("a call waiting for the partition's turn can be abandoned, and the turn passes on", async () => {
    let release!: () => void;
    const released = new Promise<void>((r) => {
      release = r;
    });
    let produces = 0;
    node.on(async (n, request) => {
      switch (request.type) {
        case "Metadata":
          return fx.metadata([n], fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1)));
        case "FindCoordinator":
          return fx.coordinator(n);
        case "InitProducer":
          return fx.producer(100, 1);
        default:
          if (++produces === 1) {
            await released;
          }
          return fx.produced(fx.STREAM_ID, 0, produces);
      }
    });
    const lotse = client(true, { retryForMs: 5_000 });
    const first = lotse.produce("s", 0, messages(1));
    await eventually("the first produce to arrive", () => node.count("Produce") === 1);
    const abort = new AbortController();
    const waiting = lotse.produce("s", 0, messages(1), { signal: abort.signal });
    abort.abort();
    await assert.rejects(waiting, GaveUpError);
    const third = lotse.produce("s", 0, messages(1));
    release();
    await first;
    await third;
    assert.equal(node.count("Produce"), 2, "the abandoned call never sent anything");
    assert.equal(batchOf(node.requests("Produce")[1] as never).baseSequence, 1);
  });

  test("a produce to an unknown error code of the broker is reported with its code", async () => {
    serve(() => fx.produced(fx.STREAM_ID, 0, 0, 999 as ErrorCodeValue));
    await assert.rejects(client(true).produce("s", 0, messages(1)), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, 999);
      return true;
    });
  });
});

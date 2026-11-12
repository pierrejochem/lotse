import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BrokerResponseError,
  ErrorCode,
  type ErrorCodeValue,
  ExchangeType,
  GaveUpError,
  LotseClient,
  NotAllWrittenError,
  type PublishResult,
  type Request,
  type ResourceMetadata,
  type Response,
  ResourceExistsError,
  ResourceKind,
  type RoutedMessage,
  UnknownOutcomeError,
  UnknownStreamError,
  UnroutableError,
} from "../src/index.js";
import { closeAll, DROP, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";
import { eventually } from "./support/lotse.js";

/** Publishing through exchanges (a node routes, ADR 0079) and declaring and binding them, against scripted brokers. */

const EXCHANGE = 20n;

const resource = (name: string, id: bigint, kind: number): ResourceMetadata => ({
  error: ErrorCode.NONE,
  name,
  tenant: "default",
  resourceId: id,
  kind,
  partitions: [],
  exchange: kind === ResourceKind.EXCHANGE ? { exchangeType: ExchangeType.DIRECT, bindings: [] } : null,
});

const routed = (...queues: RoutedMessage[]): Response => ({
  type: "Publish",
  throttleMs: 0,
  entries: [{ error: ErrorCode.NONE, routed: queues }],
});

const written = (id: number, name: string, offset: number): RoutedMessage => ({
  queueId: BigInt(id),
  name,
  error: ErrorCode.NONE,
  baseOffset: BigInt(offset),
});

describe("publishing through exchanges and binding them", () => {
  let a: FakeNode;
  let b: FakeNode;
  let all: FakeNode[];
  let client: LotseClient | undefined;
  let publishes: Extract<Request, { type: "Publish" }>[];
  let publishAnswer: (p: Extract<Request, { type: "Publish" }>) => Response | null | typeof DROP;

  beforeEach(async () => {
    all = await startNodes(2);
    [a, b] = all as [FakeNode, FakeNode];
    publishes = [];
    publishAnswer = () => routed();
    for (const n of all) {
      n.on((_n, request) => {
        switch (request.type) {
          case "Metadata":
            return fx.metadata(
              all,
              resource("x", EXCHANGE, ResourceKind.EXCHANGE),
              resource("a", 7n, ResourceKind.QUEUE),
              resource("b", 8n, ResourceKind.QUEUE),
            );
          case "Publish":
            publishes.push(request);
            return publishAnswer(request);
          default:
            throw new Error(`unexpected ${request.type}`);
        }
      });
    }
    client = new LotseClient(fx.config([a], { requestTimeoutMs: 500, retryForMs: 1_000 }));
  });

  afterEach(async () => {
    client?.close();
    await closeAll(all);
  });

  const lotse = (): LotseClient => client as LotseClient;

  test("a routed message names its queues and offsets, by queue name", async () => {
    // 9 was bound and deleted between this client's lookups: only the node can name it. An empty name falls back to the
    // name this client knows.
    publishAnswer = () => routed(written(8, "b", 3), written(9, "gone", 5), written(7, "", 41));
    const got = await lotse().publish("x", "k", { value: "hello" }, { mandatory: true });
    assert.deepEqual(got.routed, [
      { queue: "a", offset: 41n },
      { queue: "b", offset: 3n },
      { queue: "gone", offset: 5n },
    ]);
    const e = publishes[0]?.entries[0];
    assert.equal(e?.exchangeId, EXCHANGE);
    assert.equal(e?.routingKey, "k");
    assert.equal(e?.mandatory, true);
    assert.equal(publishes[0]?.txnId, null);
    const batch = Batch.parse(e?.batch as Uint8Array).batch;
    assert.equal(batch.recordCount, 1);
    assert.equal(batch.attributes.transactional, false);
  });

  test("a message no queue takes is dropped without mandatory", async () => {
    assert.deepEqual((await lotse().publish("x", "nobody", { value: "m" })).routed, []);
    assert.equal(publishes[0]?.entries[0]?.mandatory, false);
  });

  test("some queues only are reported as such, with the queues that hold the message", async () => {
    publishAnswer = () =>
      routed(written(7, "a", 41), { queueId: 8n, name: "b", error: ErrorCode.REQUEST_TIMED_OUT, baseOffset: 0n }, {
        queueId: 9n,
        name: "c",
        error: ErrorCode.UNAUTHORIZED,
        baseOffset: 0n,
      });
    await assert.rejects(lotse().publish("x", "k", { value: "m" }), (e: unknown) => {
      assert.ok(e instanceof NotAllWrittenError);
      assert.deepEqual(e.written, ["a"]);
      assert.ok(e.failures[0] instanceof UnknownOutcomeError, "a timed out write is unknown");
      assert.ok(e.failures[1] instanceof BrokerResponseError, "a refused one is not");
      return true;
    });
  });

  test("a mandatory message that nothing takes is unroutable", async () => {
    publishAnswer = () => ({ type: "Publish", throttleMs: 0, entries: [{ error: ErrorCode.UNROUTABLE, routed: [] }] });
    await assert.rejects(lotse().publish("x", "nobody", { value: "m" }, { mandatory: true }), UnroutableError);
  });

  test("a refusal is a broker error", async () => {
    publishAnswer = () => ({ type: "Publish", throttleMs: 0, entries: [{ error: ErrorCode.UNAUTHORIZED, routed: [] }] });
    await assert.rejects(lotse().publish("x", "k", { value: "m" }), BrokerResponseError);
  });

  test("a refusal from a node that has not heard of the exchange yet is sent again", async () => {
    // Refused before anything was written; the next node, or a later try, takes it.
    let refusals = 2;
    publishAnswer = () =>
      refusals-- > 0
        ? { type: "Publish", throttleMs: 0, entries: [{ error: ErrorCode.NOT_LEADER, routed: [] }] }
        : routed(written(7, "a", 41));
    const got = await lotse().publish("x", "k", { value: "m" });
    assert.deepEqual(got.routed, [{ queue: "a", offset: 41n }]);
    assert.equal(publishes.length, 3, "two refused, then taken");
  });

  test("a lost answer is not sent again: the outcome is unknown", async () => {
    publishAnswer = () => DROP;
    await assert.rejects(lotse().publish("x", "k", { value: "m" }), UnknownOutcomeError);
    assert.equal(publishes.length, 1, "the node may have written it");
  });

  test("a publish nothing answers is unknown after the request timeout, and not sent again", async () => {
    publishAnswer = () => null;
    await assert.rejects(lotse().publish("x", "k", { value: "m" }), UnknownOutcomeError);
    assert.equal(publishes.length, 1);
  });

  test("a publish given up by its signal after it was sent is unknown", async () => {
    publishAnswer = () => null;
    const ac = new AbortController();
    const pending = lotse().publish("x", "k", { value: "m" }, { signal: ac.signal });
    await eventually("the publish", () => publishes.length === 1);
    ac.abort();
    await assert.rejects(pending, UnknownOutcomeError);
  });

  test("a node that cannot be reached is tried in turn and then given up", async () => {
    client?.close();
    await a.close();
    client = new LotseClient(fx.config([a, b], { requestTimeoutMs: 500, retryForMs: 300 }));
    const got = await lotse().publish("x", "k", { value: "m" });
    assert.deepEqual(got.routed, []);
    assert.equal(publishes.length, 1, "the second node took it");
    await b.close();
    publishes = [];
    client.close();
    client = new LotseClient(fx.config([a, b], { requestTimeoutMs: 500, retryForMs: 300 }));
    await assert.rejects(lotse().publish("x", "k", { value: "m" }), GaveUpError);
  });

  test("an exchange nobody knows is an unknown stream, and a queue is not an exchange", async () => {
    await assert.rejects(lotse().publish("nope", "k", { value: "m" }), UnknownStreamError);
    await assert.rejects(lotse().publish("a", "k", { value: "m" }), UnknownStreamError);
    assert.equal(publishes.length, 0);
  });

  test("an exchange deleted and made again is looked up once more", async () => {
    let id = EXCHANGE;
    for (const n of all) {
      n.on((_n, request) => {
        if (request.type === "Metadata") {
          return fx.metadata(all, resource("x", id, ResourceKind.EXCHANGE));
        }
        if (request.type === "Publish") {
          publishes.push(request);
          const e = request.entries[0] as { exchangeId: bigint };
          return e.exchangeId === id
            ? routed(written(7, "a", 1))
            : { type: "Publish", throttleMs: 0, entries: [{ error: ErrorCode.UNKNOWN_RESOURCE, routed: [] } as PublishResult] };
        }
        throw new Error(`unexpected ${request.type}`);
      });
    }
    await lotse().publish("x", "k", { value: "m" });
    id = 30n;
    const got = await lotse().publish("x", "k", { value: "m" });
    assert.equal(got.routed.length, 1);
    assert.deepEqual(
      publishes.map((p) => p.entries[0]?.exchangeId),
      [EXCHANGE, EXCHANGE, 30n],
    );
  });

  // ---- declaring and binding ----

  test("declareExchange sends the type and the tenant, and a repeat finds the first", async () => {
    let created = 0;
    a.on((_n, request) => {
      if (request.type === "DeclareExchange") {
        created++;
        return created === 1
          ? { type: "Create", error: ErrorCode.NONE, resourceId: 77n, errorMsg: null, leaderHint: null }
          : { type: "Create", error: ErrorCode.RESOURCE_EXISTS, resourceId: 77n, errorMsg: null, leaderHint: null };
      }
      throw new Error(`unexpected ${request.type}`);
    });
    assert.equal(await lotse().declareExchange("x", ExchangeType.TOPIC), 77n);
    const sent = a.requests("DeclareExchange")[0];
    assert.equal(sent?.exchangeType, ExchangeType.TOPIC);
    assert.equal(sent?.tenant, "default");
    await assert.rejects(lotse().declareExchange("x", ExchangeType.TOPIC), (e: unknown) => e instanceof ResourceExistsError && e.resourceId === 77n);
    await assert.rejects(lotse().declareExchange("y", 9), RangeError);
  });

  test("bind, bindHeaders and unbind name the exchange and the queue by id, and follow a leader hint", async () => {
    let first = true;
    const outcome = (error: ErrorCodeValue = ErrorCode.NONE, hint: { leader: number; term: bigint } | null = null): Response => ({
      type: "Outcome",
      error,
      leaderHint: hint,
    });
    a.on((_n, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, resource("x", EXCHANGE, ResourceKind.EXCHANGE), resource("a", 7n, ResourceKind.QUEUE));
      }
      if (request.type === "Bind" && first) {
        first = false;
        return outcome(ErrorCode.NOT_LEADER, { leader: 2, term: 3n });
      }
      throw new Error(`unexpected ${request.type}`);
    });
    b.on((_n, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, resource("x", EXCHANGE, ResourceKind.EXCHANGE), resource("a", 7n, ResourceKind.QUEUE));
      }
      if (request.type === "Bind" || request.type === "Unbind") {
        return outcome();
      }
      throw new Error(`unexpected ${request.type}`);
    });
    await lotse().bind("x", "a", "orders.#");
    assert.equal(a.count("Bind"), 1);
    const bind = b.requests("Bind")[0];
    assert.deepEqual(
      [bind?.exchangeId, bind?.queueId, bind?.bindingKey, bind?.matchAll, bind?.headerMatch.length],
      [EXCHANGE, 7n, "orders.#", true, 0],
    );
    await lotse().bindHeaders("x", "a", [{ name: "region", value: "eu" }, { name: "flag", value: null }], false);
    const hb = b.requests("Bind")[1];
    assert.equal(hb?.bindingKey, "");
    assert.equal(hb?.matchAll, false);
    assert.deepEqual(
      hb?.headerMatch.map((m) => [Buffer.from(m.key).toString(), m.value === null ? null : Buffer.from(m.value).toString()]),
      [
        ["region", "eu"],
        ["flag", null],
      ],
    );
    await lotse().unbind("x", "a", "orders.#");
    assert.equal(b.requests("Unbind")[0]?.bindingKey, "orders.#");
    await assert.rejects(lotse().bind("x", "nope", "k"), UnknownStreamError);
  });

  test("a refused bind fails at once", async () => {
    a.on((_n, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, resource("x", EXCHANGE, ResourceKind.EXCHANGE), resource("a", 7n, ResourceKind.QUEUE));
      }
      return { type: "Outcome", error: ErrorCode.UNAUTHORIZED, leaderHint: null };
    });
    await assert.rejects(lotse().bind("x", "a", "k"), BrokerResponseError);
    assert.equal(a.count("Bind"), 1);
    await sleep(0);
  });
});

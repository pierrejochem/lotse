import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  BrokerResponseError,
  ClientAuthenticationError,
  ErrorCode,
  type ErrorCodeValue,
  GaveUpError,
  InsecureTransportError,
  type LeaderHint,
  LotseClient,
  mergeNewest,
  ResourceExistsError,
  type ResourceMetadata,
  type Response,
  UnknownStreamError,
} from "../src/index.js";
import * as fx from "./support/fx.js";
import { closeAll, DROP, type FakeNode, type NodeHandler, startNodes } from "./support/fake-node.js";

/** How the client finds a partition's leader and what it does with leader hints (PROTOCOL.md, Routes). */

describe("routing against scripted brokers", () => {
  let a: FakeNode;
  let b: FakeNode;
  let c: FakeNode;
  let all: FakeNode[];
  let client: LotseClient | undefined;

  beforeEach(async () => {
    all = await startNodes(3);
    [a, b, c] = all as [FakeNode, FakeNode, FakeNode];
  });

  afterEach(async () => {
    client?.close();
    client = undefined;
    await closeAll(all);
  });

  const open = (bootstrap: FakeNode[], extra: object = {}): LotseClient => {
    client = new LotseClient(fx.config(bootstrap, { idempotent: false, ...extra }));
    return client;
  };

  /** A node that serves metadata from `view` and acknowledges every produce at `offset`. */
  const serving =
    (view: ResourceMetadata, offset: number): NodeHandler =>
    (_node, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, view);
      }
      if (request.type === "Produce") {
        return fx.produced(fx.STREAM_ID, 0, offset);
      }
      throw new Error(`unexpected ${request.type}`);
    };

  const x = { value: "x" };

  test("a route is used until something fails", async () => {
    const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
    a.on(serving(view, 10));
    const lotse = open([a]);
    for (let i = 0; i < 3; i++) {
      assert.equal((await lotse.produce("s", 0, x)).baseOffset, 10n);
    }
    assert.equal(a.count("Metadata"), 1, "metadata once, then the route");
    assert.equal(a.count("Produce"), 3);
  });

  test("a hint with the same or a newer term and another leader is followed without new metadata", async () => {
    const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
    a.on((_n, request) =>
      request.type === "Metadata"
        ? fx.metadata(all, view)
        : fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER, { leader: 2, term: 5n }),
    );
    b.on(serving(view, 42));
    const lotse = open([a]);
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 42n);
    assert.equal(a.count("Produce"), 1);
    assert.equal(b.count("Produce"), 1);
    assert.equal(a.count("Metadata") + b.count("Metadata"), 1, "no new metadata");
    assert.deepEqual(lotse.routeOf("s", 0), { resourceId: fx.STREAM_ID, leader: 2, term: 5n });
  });

  for (const term of [6n, -1n]) {
    test(`a newer term is followed (${term === -1n ? "u64::MAX, newer than any term" : term})`, async () => {
      const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
      a.on((_n, request) =>
        request.type === "Metadata"
          ? fx.metadata(all, view)
          : fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER, { leader: 2, term }),
      );
      b.on(serving(view, 1));
      const lotse = open([a]);
      await lotse.produce("s", 0, x);
      assert.equal(b.count("Produce"), 1);
      assert.equal(a.count("Metadata"), 1);
      assert.equal(lotse.routeOf("s", 0)?.term, term);
    });
  }

  for (const code of [ErrorCode.NOT_LEADER, ErrorCode.FENCED_LEADER_TERM]) {
    test(`both leader errors are handled alike (${code})`, async () => {
      const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
      a.on((_n, request) =>
        request.type === "Metadata"
          ? fx.metadata(all, view)
          : fx.produced(fx.STREAM_ID, 0, 0, code as ErrorCodeValue, { leader: 2, term: 5n }),
      );
      b.on(serving(view, 1));
      await open([a]).produce("s", 0, x);
      assert.equal(b.count("Produce"), 1);
      assert.equal(a.count("Metadata"), 1);
    });
  }

  test("a hint with an older term is not followed and metadata is asked again", async () => {
    const stale = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
    const fresh = fx.stream("s", fx.STREAM_ID, fx.partition(0, 3, 9, 1, 2, 3));
    a.on((node, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, node.count("Metadata") === 1 ? stale : fresh);
      }
      // Node 2 claims to lead in term 4, which is older than the route's 5: it is lagging.
      return fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER, { leader: 2, term: 4n });
    });
    c.on(serving(fresh, 77));
    const lotse = open([a]);
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 77n);
    assert.equal(b.count("Produce"), 0, "the old hint was not followed");
    assert.equal(a.count("Metadata"), 2, "the route was dropped and metadata asked again");
  });

  const noFollow: [string, LeaderHint | null][] = [
    ["same leader, same term", { leader: 1, term: 5n }],
    ["same leader, newer term", { leader: 1, term: 99n }],
    ["no hint", null],
  ];
  for (const [kind, hint] of noFollow) {
    test(`a hint naming the same leader, or no hint, drops the route (${kind})`, async () => {
      const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 5, 1, 2, 3));
      a.on((node, request) => {
        if (request.type === "Metadata") {
          return fx.metadata(all, view);
        }
        // The first produce is refused; the second works.
        return node.count("Produce") === 1
          ? fx.produced(fx.STREAM_ID, 0, 0, ErrorCode.NOT_LEADER, hint)
          : fx.produced(fx.STREAM_ID, 0, 3);
      });
      assert.equal((await open([a]).produce("s", 0, x)).baseOffset, 3n);
      assert.equal(a.count("Metadata"), 2, "the route was dropped and metadata asked again");
      assert.equal(a.count("Produce"), 2);
    });
  }

  test("leader zero asks the partition's voters directly", async () => {
    const none = fx.stream("s", fx.STREAM_ID, fx.partition(0, 0, 0, 2, 3));
    const knows = fx.stream("s", fx.STREAM_ID, fx.partition(0, 3, 8, 2, 3));
    a.on(() => fx.metadata(all, none));
    // The first voter does not know either, the second does.
    b.on(() => fx.metadata(all, none));
    c.on(serving(knows, 5));
    const lotse = open([a]);
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 5n);
    assert.equal(b.count("Metadata"), 1, "voter 2 was asked");
    assert.equal(c.count("Metadata"), 1, "voter 3 was asked once");
    assert.equal(c.count("Produce"), 1);
    assert.deepEqual(lotse.routeOf("s", 0), { resourceId: fx.STREAM_ID, leader: 3, term: 8n });
  });

  test("a leader of zero that metadata reported is not used as a route", async () => {
    // metadata() learns the partition with leader 0; a produce must ask the voters at once, not try node 0 and wait.
    const none = fx.stream("s", fx.STREAM_ID, fx.partition(0, 0, 0, 2, 3));
    const knows = fx.stream("s", fx.STREAM_ID, fx.partition(0, 3, 8, 2, 3));
    a.on(() => fx.metadata(all, none));
    b.on(() => fx.metadata(all, none));
    c.on(serving(knows, 5));
    const lotse = open([a], { backoffMs: 3_000, retryForMs: 10_000 });
    await lotse.metadata("s");
    const start = performance.now();
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 5n);
    assert.ok(performance.now() - start < 1_500, "no retry pause");
  });

  test("leader zero everywhere is retried and then given up", async () => {
    const none = fx.stream("s", fx.STREAM_ID, fx.partition(0, 0, 0, 2, 3));
    for (const n of all) {
      n.on(() => fx.metadata(all, none));
    }
    const lotse = open([a], { retryForMs: 200 });
    await assert.rejects(lotse.produce("s", 0, x), (e: unknown) => {
      assert.ok(e instanceof GaveUpError);
      assert.match(e.message, /no leader/);
      return true;
    });
    assert.equal(a.count("Produce") + b.count("Produce") + c.count("Produce"), 0);
  });

  test("a name one broker does not know is asked of all and the highest resource id wins", async () => {
    // Node 1 has not heard of the stream; 2 knows an old incarnation, 3 the newest.
    const old = fx.stream("s", 10, fx.partition(0, 2, 1, 2));
    const newest = fx.stream("s", 20, fx.partition(0, 3, 1, 3));
    a.on(() => fx.metadata(all, fx.unknown("s")));
    b.on(() => fx.metadata(all, old));
    c.on((_n, request) => (request.type === "Metadata" ? fx.metadata(all, newest) : fx.produced(20, 0, 9)));
    const lotse = open([a]);
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 9n);
    const sent = c.requests("Produce")[0];
    assert.equal(sent?.resources[0]?.resourceId, 20n, "the newest incarnation");
    assert.equal(b.count("Produce"), 0);
  });

  test("a name nobody knows is an unknown stream and is not retried", async () => {
    for (const n of all) {
      n.on(() => fx.metadata(all, fx.unknown("s")));
    }
    const lotse = open([a]);
    const start = performance.now();
    await assert.rejects(lotse.produce("s", 0, x), (e: unknown) => {
      assert.ok(e instanceof UnknownStreamError);
      assert.equal(e.streamName, "s");
      return true;
    });
    assert.ok(performance.now() - start < 1_500, "not retried");
  });

  test("a partition that does not exist is an unknown stream", async () => {
    a.on(() => fx.metadata(all, fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1))));
    await assert.rejects(open([a]).produce("s", 5, x), (e: unknown) => {
      assert.ok(e instanceof UnknownStreamError);
      assert.equal(e.partition, 5);
      return true;
    });
  });

  test("a leader whose address is unknown is retried with fresh metadata", async () => {
    // The first answer names node 9, which no broker list contains; the second names node 1.
    const ghost = fx.stream("s", fx.STREAM_ID, fx.partition(0, 9, 1, 9));
    const real = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 2, 1));
    a.on((node, request) =>
      request.type === "Metadata" ? fx.metadata(all, node.count("Metadata") === 1 ? ghost : real) : fx.produced(fx.STREAM_ID, 0, 4),
    );
    assert.equal((await open([a]).produce("s", 0, x)).baseOffset, 4n);
    assert.equal(a.count("Metadata"), 2);
  });

  test("UNKNOWN_RESOURCE in a produce drops the route, so a name that comes back is found again", async () => {
    const first = fx.stream("s", 10, fx.partition(0, 1, 1, 1));
    const second = fx.stream("s", 20, fx.partition(0, 1, 1, 1));
    a.on((node, request) => {
      if (request.type === "Metadata") {
        return fx.metadata(all, node.count("Metadata") === 1 ? first : second);
      }
      const id = request.type === "Produce" ? (request.resources[0]?.resourceId ?? 0n) : 0n;
      return id === 10n ? fx.produced(10, 0, 0, ErrorCode.UNKNOWN_RESOURCE) : fx.produced(20, 0, 6);
    });
    const lotse = open([a]);
    await assert.rejects(lotse.produce("s", 0, x), (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.UNKNOWN_RESOURCE);
    assert.equal(lotse.routeOf("s", 0), undefined, "dropped");
    assert.equal((await lotse.produce("s", 0, x)).baseOffset, 6n);
  });

  // ---- mergeNewest ----

  const resource = (name: string, id: bigint): ResourceMetadata => fx.stream(name, id);

  test("the newest resource of a name wins in every order", () => {
    for (const order of [
      [100n, 200n],
      [200n, 100n],
    ]) {
      const merged: ResourceMetadata[] = [];
      for (const id of order) {
        mergeNewest(merged, [resource("s", id), resource(`only${id}`, id)]);
      }
      assert.equal(merged.length, 3);
      assert.equal(merged.filter((r) => r.name === "s").length, 1);
      assert.equal(merged.find((r) => r.name === "s")?.resourceId, 200n);
    }
  });

  test("an error names nothing", () => {
    const merged: ResourceMetadata[] = [resource("s", 200n)];
    mergeNewest(merged, [{ ...fx.unknown("s"), resourceId: 300n }]);
    assert.equal(merged[0]?.resourceId, 200n);
    mergeNewest(merged, [{ ...fx.unknown("t"), resourceId: 300n }]);
    assert.equal(merged.length, 1);
  });

  test("resource ids are compared as unsigned", () => {
    const merged: ResourceMetadata[] = [resource("s", 5n)];
    mergeNewest(merged, [resource("s", -(2n ** 63n))]);
    assert.equal(merged[0]?.resourceId, -(2n ** 63n), "2^63 is newer than 5");
  });

  // ---- metadata and login ----

  test("metadata gives up when no broker answers", async () => {
    await a.close();
    const lotse = open([a], { retryForMs: 100 });
    await assert.rejects(lotse.metadata("s"), GaveUpError);
  });

  test("a broker that refuses login fails at once as authentication", async () => {
    a.on(() => ({ type: "Error", code: ErrorCode.UNAUTHORIZED }));
    const lotse = open([a], { retryForMs: 30_000 });
    const start = performance.now();
    await assert.rejects(lotse.produce("s", 0, x), ClientAuthenticationError);
    assert.ok(performance.now() - start < 5_000);
  });

  test("a client that asks for no transport is refused before anything is sent", async () => {
    const lotse = new LotseClient({ bootstrap: [a.hostPort], retryForMs: 30_000 });
    try {
      await assert.rejects(lotse.metadata("s"), InsecureTransportError);
      assert.equal(a.connections, 0);
    } finally {
      lotse.close();
    }
  });

  test("the options are checked", () => {
    assert.throws(() => new LotseClient({ bootstrap: [], plaintextOnLoopback: true }), RangeError);
    assert.throws(() => new LotseClient({ bootstrap: [a.hostPort], plaintextOnLoopback: true, retryForMs: 0 }), RangeError);
    assert.throws(() => new LotseClient({ bootstrap: ["nohost"], plaintextOnLoopback: true }), RangeError);
  });

  test("a closed client refuses calls", async () => {
    const lotse = open([a]);
    lotse.close();
    await assert.rejects(lotse.metadata(), /closed/);
  });

  // ---- createStream ----

  const withAdmin = (admin: (n: number) => Response | typeof DROP): void => {
    const view = fx.stream("s", fx.STREAM_ID, fx.partition(0, 1, 1, 1));
    let calls = 0;
    a.on((_n, request) => (request.type === "Metadata" ? fx.metadata(all, view) : pointToB(request)));
    b.on((_n, request) => (request.type === "Metadata" ? fx.metadata(all, view) : admin(++calls)));
  };
  const pointToB = (r: { type: string }): Response => {
    assert.equal(r.type, "CreateStream");
    return { type: "Create", error: ErrorCode.NOT_LEADER, resourceId: 0n, errorMsg: null, leaderHint: { leader: 2, term: 1n } };
  };
  const created = (error: ErrorCodeValue, id: bigint): Response => ({ type: "Create", error, resourceId: id, errorMsg: null, leaderHint: null });

  test("a create follows the hint to the metadata leader and asks it directly afterwards", async () => {
    withAdmin(() => created(ErrorCode.NONE, 31n));
    const lotse = open([a], { retryForMs: 1_000, tenant: "acme" });
    assert.equal(await lotse.createStream("t", 3, 2, { "retention.ms": "1000", "a.first": "x" }), 31n);
    const sent = b.requests("CreateStream")[0];
    assert.equal(sent?.name, "t");
    assert.equal(sent?.tenant, "acme");
    assert.equal(sent?.partitions, 3);
    assert.equal(sent?.replication, 2);
    assert.deepEqual(sent?.configs, [
      { key: "a.first", value: "x" },
      { key: "retention.ms", value: "1000" },
    ]);
    await lotse.createStream("u", 1, 1);
    assert.equal(a.count("CreateStream"), 1, "the leader, once known, is asked directly");
    assert.equal(b.requests("CreateStream")[1]?.tenant, "acme");
  });

  test("a create whose answer was lost finds it and says so", async () => {
    withAdmin((n) => (n === 1 ? DROP : created(ErrorCode.RESOURCE_EXISTS, 9n)));
    await assert.rejects(open([a], { retryForMs: 1_000 }).createStream("t", 1, 1), (e: unknown) => {
      assert.ok(e instanceof ResourceExistsError);
      assert.equal(e.resourceId, 9n);
      return true;
    });
  });

  test("a create that is refused for another reason is reported with its code", async () => {
    withAdmin(() => ({ type: "Create", error: ErrorCode.INVALID_REQUEST, resourceId: 0n, errorMsg: "replication 5 is more than the 3 nodes", leaderHint: null }));
    await assert.rejects(open([a], { retryForMs: 1_000 }).createStream("t", 1, 5), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.INVALID_REQUEST);
      assert.match(e.message, /replication 5/);
      return true;
    });
  });
});

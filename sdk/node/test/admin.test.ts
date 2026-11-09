import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, test } from "node:test";
import {
  BrokerResponseError,
  ClientClosedError,
  CertificateState,
  type CertificateEntry,
  ConfigSource,
  ErrorCode,
  type ErrorCodeValue,
  ExchangeType,
  GaveUpError,
  type LeaderHint,
  LotseClient,
  NoAnswerError,
  type Request,
  type ResourceMetadata,
  ResourceKind,
  type Response,
  Role,
  type RoleBinding,
  ScramCredential,
  SubjectKind,
  UnknownOutcomeError,
  UnknownStreamError,
} from "../src/index.js";
import { closeAll, DROP, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";
import { eventually } from "./support/lotse.js";

/** Admin calls against scripted brokers: node 2 leads the metadata group, node 1 points to it. */

const EXCHANGE_ID = 20n;
const QUEUE_ID = 21n;
const STREAM_ID = 7n;

const HINT: LeaderHint = { leader: 2, term: 1n };

type Handler = (request: Request) => Response | null | typeof DROP;

const ok = (): Response => ({ type: "Admin", error: ErrorCode.NONE, errorMsg: null, leaderHint: null });
const admin = (error: ErrorCodeValue, errorMsg: string | null = null, leaderHint: LeaderHint | null = null): Response => ({
  type: "Admin",
  error,
  errorMsg,
  leaderHint,
});

/** What a node that does not lead the metadata group answers to each admin request: NOT_LEADER with a hint. */
function notLeader(r: Request): Response {
  const error = ErrorCode.NOT_LEADER;
  const leaderHint = HINT;
  switch (r.type) {
    case "DescribeConfigs":
      return { type: "DescribeConfigs", error, leaderHint, resources: [] };
    case "DescribeUsers":
      return { type: "DescribeUsers", error, leaderHint, users: [] };
    case "DescribeCertificates":
      return { type: "DescribeCertificates", error, leaderHint, nowMs: 0n, certificates: [] };
    case "DescribeTenants":
      return { type: "DescribeTenants", error, leaderHint, tenants: [] };
    case "DescribeRoleBindings":
      return { type: "DescribeRoleBindings", error, leaderHint, bindings: [] };
    case "DescribeNodes":
      return { type: "DescribeNodes", error, leaderHint, nodes: [] };
    case "QueueAction":
      return { type: "QueueAction", error, errorMsg: null, count: 0n, upTo: 0n, more: false, leaderHint };
    default:
      return admin(error, null, leaderHint);
  }
}

const sample = (name: string, kind: number): ResourceMetadata => ({
  error: ErrorCode.NONE,
  name,
  tenant: "default",
  resourceId: kind === ResourceKind.EXCHANGE ? EXCHANGE_ID : QUEUE_ID,
  kind,
  partitions: kind === ResourceKind.QUEUE ? [fx.partition(0, 2, 1, 2)] : [],
  exchange:
    kind === ResourceKind.EXCHANGE
      ? { exchangeType: ExchangeType.DIRECT, bindings: [{ queueId: QUEUE_ID, bindingKey: "k", headerMatch: [], matchAll: true }] }
      : null,
});

describe("admin calls against scripted brokers", () => {
  let a: FakeNode;
  let b: FakeNode;
  let all: FakeNode[];
  let client: LotseClient;
  let calls: number;

  const metadata = (): Response =>
    fx.metadata(
      all,
      fx.stream("s", STREAM_ID, fx.partition(0, 2, 1, 2)),
      sample("x", ResourceKind.EXCHANGE),
      sample("q", ResourceKind.QUEUE),
    );

  /** Node 1 answers metadata and sends admin requests to node 2; node 2 answers with `handler`. */
  function serve(handler: Handler, extra: { retryForMs?: number } = {}): void {
    a.on((_n, r) => (r.type === "Metadata" ? metadata() : notLeader(r)));
    b.on((_n, r) => (r.type === "Metadata" ? metadata() : handler(r)));
    client = new LotseClient(fx.config(all, { retryForMs: 1_000, ...extra }));
  }

  beforeEach(async () => {
    all = await startNodes(2);
    [a, b] = all as [FakeNode, FakeNode];
    calls = 0;
  });

  afterEach(async () => {
    client?.close();
    await closeAll(all);
  });

  test("a delete is done once the resource is gone, and keeps the broker's reason when it is refused", async () => {
    serve((r) => {
      assert.equal(r.type, "DeleteResource");
      if (r.type === "DeleteResource" && r.ifEmpty) {
        return admin(ErrorCode.RESOURCE_NOT_EMPTY, "2 messages");
      }
      return admin(ErrorCode.UNKNOWN_RESOURCE);
    });
    assert.equal(await client.delete("q"), QUEUE_ID, "deleted by someone else: done");
    const sent = b.requests("DeleteResource");
    assert.deepEqual(sent, [{ type: "DeleteResource", resourceId: QUEUE_ID, ifEmpty: false }]);
    assert.equal(a.count("DeleteResource"), 1, "the first ask went to node 1, which pointed to node 2");

    await assert.rejects(client.deleteIfEmpty("q"), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.RESOURCE_NOT_EMPTY);
      assert.equal(e.brokerMessage, "2 messages");
      return true;
    });
    assert.deepEqual(b.requests("DeleteResource")[1], { type: "DeleteResource", resourceId: QUEUE_ID, ifEmpty: true });
    assert.equal(a.count("DeleteResource"), 1, "the leader, once known, is asked directly");

    await assert.rejects(client.delete("nothing"), UnknownStreamError);
    assert.equal(b.count("DeleteResource"), 2, "an unknown name sends no delete");
  });

  test("a delete that is answered NONE returns the id and forgets what the client held of the name", async () => {
    serve(() => ok());
    await client.metadata("q");
    assert.notEqual(client.routeOf("q", 0), undefined);
    assert.equal(await client.deleteIfEmpty("q"), QUEUE_ID);
    assert.equal(client.routeOf("q", 0), undefined, "a queue made again has a new id");
  });

  test("a delete of an exchange drops the bindings a transaction's publish had read", async () => {
    serve(() => ok());
    const metadataRequests = (): number => a.count("Metadata") + b.count("Metadata");
    const exchange = (name: string): Promise<unknown> => (client as unknown as { exchange(n: string): Promise<unknown> }).exchange(name);
    await exchange("x");
    const cached = metadataRequests();
    await exchange("x");
    assert.equal(metadataRequests(), cached, "kept for metadataMaxAgeMs");
    await client.delete("x");
    const afterDelete = metadataRequests();
    await exchange("x");
    assert.ok(metadataRequests() > afterDelete, "asked again after the delete");
  });

  test("a delete whose answer was lost is carried out again and is done", async () => {
    serve(() => (++calls === 1 ? DROP : admin(ErrorCode.UNKNOWN_RESOURCE)));
    assert.equal(await client.delete("q"), QUEUE_ID);
    assert.equal(b.count("DeleteResource"), 2);
  });

  test("purge goes on until the leader says there is no more, and requeue says which it is", async () => {
    const action = (count: bigint, upTo: bigint, more: boolean): Response => ({
      type: "QueueAction",
      error: ErrorCode.NONE,
      errorMsg: null,
      count,
      upTo,
      more,
      leaderHint: null,
    });
    a.on((_n, r) => (r.type === "Metadata" ? metadata() : notLeader(r)));
    b.on((_n, r) => {
      if (r.type === "Metadata") {
        return metadata();
      }
      assert.equal(r.type, "QueueAction");
      return r.type === "QueueAction" && r.upTo === 0n ? action(3n, 10n, true) : action(2n, 12n, false);
    });
    client = new LotseClient(fx.config(all));
    assert.equal(await client.purge("q"), 5n);
    const sent = b.requests("QueueAction");
    assert.deepEqual(sent, [
      { type: "QueueAction", requeue: false, queueId: QUEUE_ID, upTo: 0n },
      { type: "QueueAction", requeue: false, queueId: QUEUE_ID, upTo: 10n },
    ]);
    assert.equal(await client.requeue("q"), 5n);
    assert.deepEqual(
      b.requests("QueueAction").slice(2).map((q) => [q.requeue, q.upTo]),
      [
        [true, 0n],
        [true, 10n],
      ],
    );
  });

  test("a purge follows the queue's new leader and a refusal keeps its reason", async () => {
    const nodeHint: LeaderHint = { leader: 1, term: 2n };
    b.on((_n, r) => {
      if (r.type === "Metadata") {
        return metadata();
      }
      return { type: "QueueAction", error: ErrorCode.NOT_LEADER, errorMsg: null, count: 0n, upTo: 0n, more: false, leaderHint: nodeHint };
    });
    a.on((_n, r) => {
      if (r.type === "Metadata") {
        return metadata();
      }
      assert.equal(r.type, "QueueAction");
      return ++calls === 1
        ? { type: "QueueAction", error: ErrorCode.NONE, errorMsg: null, count: 4n, upTo: 0n, more: false, leaderHint: null }
        : { type: "QueueAction", error: ErrorCode.UNAUTHORIZED, errorMsg: "not a tenant admin", count: 0n, upTo: 0n, more: false, leaderHint: null };
    });
    client = new LotseClient(fx.config(all));
    assert.equal(await client.purge("q"), 4n);
    assert.equal(b.count("QueueAction"), 1, "tried the old leader once");
    assert.equal(client.routeOf("q", 0)?.leader, 1, "the hint was followed");
    await assert.rejects(client.requeue("q"), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.UNAUTHORIZED);
      assert.equal(e.brokerMessage, "not a tenant admin");
      return true;
    });
    await assert.rejects(client.purge("nothing"), UnknownStreamError);
  });

  test("settings are changed and read", async () => {
    serve((r) => {
      if (r.type === "DescribeConfigs") {
        return {
          type: "DescribeConfigs",
          error: ErrorCode.NONE,
          leaderHint: null,
          resources: [
            {
              resourceId: QUEUE_ID,
              error: ErrorCode.NONE,
              configs: [{ key: "ack.timeout.ms", value: "500", source: ConfigSource.DYNAMIC }],
            },
          ],
        };
      }
      return ok();
    });
    await client.alterConfigs("q", { "max.deliveries": "3", "ack.timeout.ms": "500" }, ["message.ttl.ms"]);
    assert.deepEqual(b.requests("AlterConfigs"), [
      {
        type: "AlterConfigs",
        resourceId: QUEUE_ID,
        set: [
          { key: "ack.timeout.ms", value: "500" },
          { key: "max.deliveries", value: "3" },
        ],
        delete: ["message.ttl.ms"],
      },
    ]);
    await client.alterConfigs("q", {}, ["max.deliveries"]);
    const second = b.requests("AlterConfigs")[1];
    assert.deepEqual([second?.set, second?.delete], [[], ["max.deliveries"]]);
    assert.deepEqual(await client.describeConfigs("q"), [{ key: "ack.timeout.ms", value: "500", source: ConfigSource.DYNAMIC }]);
    assert.deepEqual(b.requests("DescribeConfigs"), [{ type: "DescribeConfigs", resourceIds: [QUEUE_ID] }]);
  });

  test("a setting a resource refuses is the broker's answer with its reason", async () => {
    serve(() => admin(ErrorCode.INVALID_REQUEST, "unknown setting nope"));
    await assert.rejects(client.alterConfigs("q", { nope: "1" }), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.INVALID_REQUEST);
      assert.equal(e.brokerMessage, "unknown setting nope");
      return true;
    });
    assert.equal(b.count("AlterConfigs"), 1, "a refusal is not retried");
  });

  test("describeConfigs reports a resource the broker could not describe, and waits for one it has not got", async () => {
    serve(() => ({
      type: "DescribeConfigs",
      error: ErrorCode.NONE,
      leaderHint: null,
      resources: [{ resourceId: QUEUE_ID, error: ErrorCode.UNKNOWN_RESOURCE, configs: [] }],
    }));
    await assert.rejects(client.describeConfigs("q"), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.UNKNOWN_RESOURCE);
      return true;
    });
    client.close();
    client = new LotseClient(fx.config(all, { retryForMs: 150 }));
    b.on((_n, r) =>
      r.type === "Metadata" ? metadata() : { type: "DescribeConfigs", error: ErrorCode.NONE, leaderHint: null, resources: [] },
    );
    await assert.rejects(client.describeConfigs("q"), GaveUpError);
    assert.ok(b.count("DescribeConfigs") > 1, "an empty answer is asked again");
  });

  test("a password leaves the client only as a salted credential", async () => {
    serve(() => ok());
    await client.setUserPassword("alice", "s3cret");
    const sent = b.requests("AlterUser")[0];
    assert.equal(sent?.user, "alice");
    const c = sent?.credential;
    assert.ok(c !== null && c !== undefined);
    assert.equal(c.salt.length, 16);
    assert.ok(c.iterations >= 4096);
    assert.equal(c.storedKey.length, 64);
    assert.equal(c.serverKey.length, 64);
    const expected = ScramCredential.of("s3cret", c.salt, c.iterations);
    assert.deepEqual(Buffer.from(c.storedKey), Buffer.from(expected.storedKey));
    assert.deepEqual(Buffer.from(c.serverKey), Buffer.from(expected.serverKey));
    assert.equal(JSON.stringify([...c.salt, ...c.storedKey, ...c.serverKey]).includes("s3cret"), false);
    for (const request of b.requests()) {
      assert.ok(!Buffer.from(JSON.stringify(request, (_k, v) => (typeof v === "bigint" ? String(v) : v))).includes("s3cret"));
    }

    await client.setUserPassword("alice", "s3cret");
    const again = b.requests("AlterUser")[1]?.credential;
    assert.notDeepEqual(Buffer.from(again?.salt ?? []), Buffer.from(c.salt), "a new salt each time");

    await client.deleteUser("alice");
    assert.deepEqual(b.requests("AlterUser")[2], { type: "AlterUser", user: "alice", credential: null });
  });

  test("users, tenants, role bindings and nodes are read from the metadata leader", async () => {
    const binding: RoleBinding = { subjectKind: SubjectKind.GROUP, subject: "ops", role: Role.VIEWER, tenant: "acme", prefix: "orders-" };
    const entry: CertificateEntry = {
      issuer: Buffer.alloc(32, 7),
      serial: Buffer.from([1, 0xab]),
      principal: "bob",
      notBeforeMs: 1_690_000_000_000n,
      notAfterMs: 1_800_000_000_000n,
      firstSeenMs: 1_695_000_000_000n,
      lastSeenMs: 1_699_000_000_000n,
      state: CertificateState.REVOKED,
      reason: "laptop lost",
      revokedAtMs: 1_698_000_000_000n,
    };
    serve((r) => {
      switch (r.type) {
        case "DescribeUsers":
          return { type: "DescribeUsers", error: ErrorCode.NONE, leaderHint: null, users: [{ name: "bob", mechanism: "SCRAM-SHA-512", iterations: 4096, source: ConfigSource.DYNAMIC }] };
        case "DescribeCertificates":
          return { type: "DescribeCertificates", error: ErrorCode.NONE, leaderHint: null, nowMs: 1_700_000_000_000n, certificates: [entry] };
        case "DescribeTenants":
          return { type: "DescribeTenants", error: ErrorCode.NONE, leaderHint: null, tenants: [{ name: "acme", source: ConfigSource.DYNAMIC, resources: 3 }] };
        case "DescribeRoleBindings":
          return { type: "DescribeRoleBindings", error: ErrorCode.NONE, leaderHint: null, bindings: [{ binding, source: ConfigSource.DYNAMIC }] };
        case "DescribeNodes":
          return { type: "DescribeNodes", error: ErrorCode.NONE, leaderHint: null, nodes: [{ nodeId: 2n, peer: "127.0.0.1:9101", client: "127.0.0.1:9001", state: 0, replicas: 5, moving: 1 }] };
        default:
          throw new Error(`unexpected ${r.type}`);
      }
    });
    assert.deepEqual(await client.describeUsers(), [{ name: "bob", mechanism: "SCRAM-SHA-512", iterations: 4096, source: ConfigSource.DYNAMIC }]);
    assert.deepEqual(await client.describeCertificates(), { certificates: [entry], nowMs: 1_700_000_000_000n });
    assert.deepEqual(await client.describeTenants(), [{ name: "acme", source: ConfigSource.DYNAMIC, resources: 3 }]);
    assert.deepEqual(await client.describeRoleBindings(), [{ binding, source: ConfigSource.DYNAMIC }]);
    assert.deepEqual(await client.describeNodes(), [{ nodeId: 2n, peer: "127.0.0.1:9101", client: "127.0.0.1:9001", state: 0, replicas: 5, moving: 1 }]);
    assert.equal(a.requests().filter((r) => r.type.startsWith("Describe")).length, 1, "node 1 was asked once, then node 2 directly");
  });

  test("a role binding goes out as every field, removals beside additions", async () => {
    serve(() => ok());
    const add: RoleBinding[] = [
      { subjectKind: SubjectKind.USER, subject: "carol", role: Role.TENANT_ADMIN, tenant: "acme", prefix: "" },
      { subjectKind: SubjectKind.GROUP, subject: "ops", role: Role.CLUSTER_VIEWER, tenant: "default", prefix: "x-" },
    ];
    const remove: RoleBinding[] = [{ subjectKind: SubjectKind.USER, subject: "dave", role: Role.PRODUCER, tenant: "acme", prefix: "orders-" }];
    await client.alterRoleBindings(add, remove);
    assert.deepEqual(b.requests("AlterRoleBindings"), [{ type: "AlterRoleBindings", add, remove }]);
    await client.alterRoleBindings(add);
    assert.deepEqual(b.requests("AlterRoleBindings")[1]?.remove, []);
  });

  test("whoAmI asks until a node answers it", async () => {
    // Node 1 answers WhoAmI with a different answer (a confused node, undecodable for this request): the client asks again, and the next ask lands on node 2.
    serve((r) => {
      assert.equal(r.type, "WhoAmI");
      return { type: "WhoAmI", kind: 2, name: "alice", groups: ["ops"], bindings: [] };
    });
    const who = await client.whoAmI();
    assert.deepEqual([who.kind, who.name, who.groups], [2, "alice", ["ops"]]);
  });

  test("a retriable answer is retried, and an answer that keeps being retriable ends as unknown", async () => {
    serve(() => (++calls === 1 ? admin(ErrorCode.NOT_ENOUGH_REPLICAS, null, HINT) : admin(ErrorCode.INVALID_REQUEST, "no such tenant")));
    await assert.rejects(client.createTenant("t"), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.INVALID_REQUEST);
      assert.equal(e.brokerMessage, "no such tenant");
      return true;
    });
    assert.equal(b.count("CreateTenant"), 2);

    client.close();
    b.on((_n, r) =>
      r.type === "DescribeNodes"
        ? { type: "DescribeNodes", error: ErrorCode.NOT_ENOUGH_REPLICAS, leaderHint: HINT, nodes: [] }
        : r.type === "Metadata"
          ? metadata()
          : admin(ErrorCode.NOT_ENOUGH_REPLICAS, null, HINT),
    );
    client = new LotseClient(fx.config(all, { retryForMs: 150 }));
    await assert.rejects(client.deleteTenant("t"), UnknownOutcomeError);
    await assert.rejects(client.describeNodes(), GaveUpError, "a read has nothing unknown about it");
  });

  test("cluster calls carry what they name", async () => {
    serve(() => ok());
    await client.moveLeader("s", 1, 3);
    await client.moveLeader("s", 0, null);
    assert.deepEqual(b.requests("MoveLeader"), [
      { type: "MoveLeader", resourceId: STREAM_ID, partition: 1, nodeId: 3n },
      { type: "MoveLeader", resourceId: STREAM_ID, partition: 0, nodeId: 0n },
    ]);
    await client.revokeSessions("ops", true);
    await client.revokeSessions("bob");
    await client.revokeSessions("carol", false);
    assert.deepEqual(
      b.requests("RevokeSessions"),
      [
        { type: "RevokeSessions", subjectKind: SubjectKind.GROUP, subject: "ops", beforeMs: 0n },
        { type: "RevokeSessions", subjectKind: SubjectKind.USER, subject: "bob", beforeMs: 0n },
        { type: "RevokeSessions", subjectKind: SubjectKind.USER, subject: "carol", beforeMs: 0n },
      ],
    );
    const issuer = new Uint8Array(32).fill(7);
    await client.revokeCertificate(issuer, Uint8Array.of(1, 0xab), 1_800_000_000_000, "laptop lost");
    await client.revokeCertificate(issuer, Uint8Array.of(0x7f), 0n, "");
    // Decoded byte fields are Buffers; compare them as such.
    assert.deepEqual(b.requests("RevokeCertificate"), [
      {
        type: "RevokeCertificate",
        issuer: Buffer.from(issuer),
        serial: Buffer.from([1, 0xab]),
        notAfterMs: 1_800_000_000_000n,
        reason: "laptop lost",
      },
      { type: "RevokeCertificate", issuer: Buffer.from(issuer), serial: Buffer.from([0x7f]), notAfterMs: 0n, reason: "" },
    ]);
    await assert.rejects(client.revokeCertificate(new Uint8Array(31), Uint8Array.of(1), 0, ""), RangeError);
    await client.createPartitions("s", 4);
    assert.deepEqual(b.requests("CreatePartitions"), [{ type: "CreatePartitions", resourceId: STREAM_ID, count: 4 }]);
    await client.addNode(4, "n4:9100", "n4:9000");
    assert.deepEqual(b.requests("AddNode"), [{ type: "AddNode", nodeId: 4n, peer: "n4:9100", client: "n4:9000" }]);
    await client.removeNode(4n);
    assert.deepEqual(b.requests("RemoveNode"), [{ type: "RemoveNode", nodeId: 4n }]);
    await client.createTenant("acme");
    await client.deleteTenant("acme");
    assert.deepEqual(b.requests("CreateTenant"), [{ type: "CreateTenant", name: "acme" }]);
    assert.deepEqual(b.requests("DeleteTenant"), [{ type: "DeleteTenant", name: "acme" }]);
    await assert.rejects(client.moveLeader("nothing", 0, 1), UnknownStreamError);
    await assert.rejects(client.createPartitions("nothing", 2), UnknownStreamError);
    await assert.rejects(client.alterConfigs("nothing", {}, []), UnknownStreamError);
    await assert.rejects(client.describeConfigs("nothing"), UnknownStreamError);
  });

  test("an argument out of range is refused before anything is sent", async () => {
    serve(() => ok());
    await assert.rejects(client.createPartitions("s", 0), RangeError);
    await assert.rejects(client.createPartitions("s", 1.5), RangeError);
    await assert.rejects(client.createPartitions("s", 2 ** 32), RangeError);
    await assert.rejects(client.moveLeader("s", -1, 1), RangeError);
    await assert.rejects(client.moveLeader("s", 2 ** 32, 1), RangeError);
    await assert.rejects(client.moveLeader("s", 0, 0), RangeError, "node 0 would unpin: say null");
    await assert.rejects(client.addNode(-1, "p", "c"), RangeError);
    assert.deepEqual(a.requests().concat(b.requests()).filter((r) => r.type !== "Metadata"), []);
  });

  test("every node is asked for its metrics, and one that is down or refuses is reported", async () => {
    const m: Extract<Response, { type: "DescribeMetrics" }> = {
      type: "DescribeMetrics",
      error: ErrorCode.NONE,
      nodeId: 1,
      version: "0.0.1",
      uptimeMs: 10n,
      metadataGroup: 1n,
      coordinatorGroup: 2n,
      connections: 0n,
      clientInBytes: 0n,
      clientOutBytes: 0n,
      peerInBytes: 0n,
      peerOutBytes: 0n,
      commitLatency: [],
      host: { cpuPermille: null, memoryBytes: null, memoryLimitBytes: null, diskFreeBytes: null, diskTotalBytes: null },
      partitions: [],
      queues: [],
      groups: [],
    };
    a.on((_n, r) => (r.type === "Metadata" ? metadata() : m));
    b.on((_n, r) => (r.type === "Metadata" ? metadata() : { ...m, error: ErrorCode.UNAUTHORIZED, nodeId: 2 }));
    client = new LotseClient(fx.config(all));
    const refused = await client.nodeMetrics();
    assert.deepEqual(refused.map((x) => x.nodeId), [1, 2]);
    assert.equal(refused[0]?.metrics?.nodeId, 1);
    assert.equal(refused[0]?.error, null);
    assert.equal(refused[1]?.metrics, null);
    assert.ok(refused[1]?.error instanceof BrokerResponseError);
    assert.equal((refused[1]?.error as BrokerResponseError).code, ErrorCode.UNAUTHORIZED);

    await b.close();
    const got = await client.nodeMetrics();
    assert.equal(got.length, 2);
    assert.equal(got[1]?.metrics, null);
    assert.ok(got[1]?.error instanceof NoAnswerError);
    assert.equal((got[1]?.error as NoAnswerError).nodeId, 2);
    assert.equal(got[0]?.metrics?.version, "0.0.1");
  });

  test("a change that is given up while in flight is unknown, a read is only given up", async () => {
    serve(() => null);
    const change = new AbortController();
    const pending = client.createTenant("t", { signal: change.signal });
    pending.catch(() => {});
    await eventually("the request to arrive", () => b.count("CreateTenant") === 1);
    change.abort();
    await assert.rejects(pending, UnknownOutcomeError);

    const read = new AbortController();
    const reading = client.describeUsers({ signal: read.signal });
    reading.catch(() => {});
    await eventually("the request to arrive", () => b.count("DescribeUsers") === 1);
    read.abort();
    await assert.rejects(reading, GaveUpError);

    const gone = new AbortController();
    gone.abort();
    await assert.rejects(client.whoAmI({ signal: gone.signal }), GaveUpError);
    await assert.rejects(client.setUserPassword("u", "p", { signal: gone.signal }), UnknownOutcomeError);
  });

  test("a closed client refuses every admin call", async () => {
    serve(() => ok());
    client.close();
    const calls: Promise<unknown>[] = [
      client.delete("q"),
      client.deleteIfEmpty("q"),
      client.purge("q"),
      client.requeue("q"),
      client.alterConfigs("q", {}, []),
      client.describeConfigs("q"),
      client.setUserPassword("u", "p"),
      client.deleteUser("u"),
      client.describeUsers(),
      client.createTenant("t"),
      client.deleteTenant("t"),
      client.describeTenants(),
      client.alterRoleBindings([], []),
      client.describeRoleBindings(),
      client.whoAmI(),
      client.revokeSessions("u"),
      client.revokeCertificate(new Uint8Array(32), Uint8Array.of(1), 0, ""),
      client.describeCertificates(),
      client.addNode(5, "p", "c"),
      client.removeNode(5),
      client.describeNodes(),
      client.moveLeader("s", 0, null),
      client.createPartitions("s", 2),
      client.nodeMetrics(),
    ];
    for (const c of calls) {
      await assert.rejects(c, ClientClosedError);
    }
  });
});

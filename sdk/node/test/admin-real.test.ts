import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  AuthenticationError,
  BrokerError,
  BrokerResponseError,
  ClientAuthenticationError,
  type ClientOptions,
  ConfigSource,
  Connection,
  ConnectionError,
  ErrorCode,
  ExchangeType,
  LotseClient,
  NodeState,
  type QueueConsumer,
  Role,
  type RoleBinding,
  scramCredentials,
  SubjectKind,
  type TlsConfig,
  UnknownStreamError,
} from "../src/index.js";
import { formatAddress } from "../src/net/address.js";
import { PlainCluster } from "./support/cluster.js";
import { leaderOf as currentLeader, pinLeader } from "./support/failover.js";
import { binary, Certs, NodeProcess, retry, skipWithoutBinary } from "./support/lotse.js";

/**
 * The admin calls against real `lotse` processes: three nodes on plain TCP for streams, queues, settings, partitions,
 * pins, nodes and metrics, and one node with TLS and authentication for users, tenants, role bindings and revoked
 * sessions. Whatever the nodes apply asynchronously (a delete, a new partition, a pinned leader) is waited for before it
 * is asserted, on every node where it matters. Skipped, with the reason, when no `lotse` executable is found (set
 * LOTSE_BIN).
 */

const skip = skipWithoutBinary();

const never = (what: string): never => assert.fail(what);

/** Awaits `action` until it stops throwing, for up to `ms`; the last error is the failure. */
async function until<T>(what: string, action: () => Promise<T>, ms = 30_000): Promise<T> {
  const deadline = Date.now() + ms;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      return await action();
    } catch (e) {
      last = e;
      await sleep(100);
    }
  }
  throw new Error(`timed out waiting for ${what}: ${String(last)}`, { cause: last });
}

function refused(code: number): (e: unknown) => boolean {
  return (e: unknown) => {
    assert.ok(e instanceof BrokerResponseError, `a refusal, not ${String(e)}`);
    assert.equal(e.code, code, e.message);
    return true;
  };
}

describe("admin calls against three real nodes", { skip }, () => {
  let cluster: PlainCluster;
  let client: LotseClient;
  const extra: LotseClient[] = [];

  const options = (bootstrap: string[]): ClientOptions => ({
    bootstrap,
    plaintextOnLoopback: true,
    requestTimeoutMs: 5_000,
    retryForMs: 60_000,
  });
  /** A client that knows one node only: what that node has applied. */
  const at = (node: number): LotseClient => {
    const c = new LotseClient(options([formatAddress(cluster.address(node))]));
    extra.push(c);
    return c;
  };
  const eachNode = [1, 2, 3];

  before(async () => {
    cluster = await PlainCluster.make(binary(), 3);
    cluster.startAll();
    client = new LotseClient(options(cluster.bootstrap));
    await until("the cluster to answer", () => client.metadata());
    await until("three nodes", async () => assert.equal((await client.describeNodes()).length, 3));
  });

  after(async () => {
    client?.close();
    for (const c of extra) {
      c.close();
    }
    await cluster?.stop();
  });

  /** Waits until every node has applied that `name` exists (or no longer does). */
  async function everyNodeKnows(name: string, exists: boolean): Promise<void> {
    for (const n of eachNode) {
      const c = at(n);
      await until(`node ${n} to ${exists ? "know" : "forget"} ${name}`, async () => {
        const m = await c.metadata(name);
        const known = m.resources.some((r) => r.name === name && r.error === ErrorCode.NONE);
        assert.equal(known, exists);
      });
    }
  }

  async function leaderOf(name: string, partition = 0): Promise<{ leader: number; voters: readonly number[] }> {
    return until(`a leader of ${name}`, async () => {
      const m = await client.metadata(name);
      const p = m.resources.find((r) => r.name === name)?.partitions.find((x) => x.partition === partition);
      assert.ok(p !== undefined && p.leader !== 0, "no leader yet");
      return { leader: p.leader, voters: p.voters };
    });
  }

  const configOf = async (name: string): Promise<Map<string, { value: string; source: number }>> =>
    new Map((await client.describeConfigs(name)).map((e) => [e.key, { value: e.value, source: e.source }]));

  /** Purges until `count` messages are gone: a message is purged once the leader has applied it. */
  async function purgeAll(queue: string, count: bigint): Promise<void> {
    let total = 0n;
    await until(`${count} messages to be purged from ${queue}`, async () => {
      total += await client.purge(queue);
      assert.equal(total >= count, true, `purged ${total}`);
    });
    assert.equal(total, count, "purge never settles more than the queue held");
  }

  async function take(c: QueueConsumer): Promise<string> {
    const d = await c.poll(30_000);
    assert.notEqual(d, null, "a message should have come");
    return (d as NonNullable<typeof d>).text() as string;
  }

  test("a stream, a queue and an exchange are deleted, on every node, and a deleted name can be made again", async () => {
    const streamId = await client.createStream("del-s", 2, 3);
    const queueId = await client.createQueue("del-q");
    await client.declareExchange("del-x", ExchangeType.DIRECT);
    await client.bind("del-x", "del-q", "k");
    await client.produce("del-s", 0, { value: "v" });
    await everyNodeKnows("del-q", true);

    const exchangeId = (await client.metadata("del-x")).resources.find((r) => r.name === "del-x")?.resourceId ?? never("no exchange");
    assert.equal(await client.delete("del-x"), exchangeId);
    assert.equal(await client.delete("del-q"), queueId);
    assert.equal(await client.delete("del-s"), streamId);
    for (const name of ["del-x", "del-q", "del-s"]) {
      await everyNodeKnows(name, false);
    }
    await assert.rejects(client.delete("del-q"), UnknownStreamError, "the name is gone from every broker");
    await assert.rejects(client.produce("del-s", 0, { value: "late" }), UnknownStreamError);

    // The same name again is a new resource: a new id, and routes that do not lead to the old one.
    const again = await client.createQueue("del-q");
    assert.ok(again > queueId, `${again} follows ${queueId}`);
    await client.produce("del-q", 0, { value: "fresh" });
    const c = await client.subscribe("del-q");
    try {
      assert.equal(await take(c), "fresh");
    } finally {
      c.close();
    }
  });

  test("a resource that holds something is not deleted with deleteIfEmpty, one that holds nothing is", async () => {
    await client.createQueue("ne-q");
    await client.produce("ne-q", 0, [{ value: "a" }, { value: "b" }]);
    await assert.rejects(client.deleteIfEmpty("ne-q"), refused(ErrorCode.RESOURCE_NOT_EMPTY));
    await everyNodeKnows("ne-q", true);
    assert.ok((await client.metadata("ne-q")).resources.some((r) => r.name === "ne-q" && r.error === ErrorCode.NONE), "nothing was deleted");
    await purgeAll("ne-q", 2n);
    await client.deleteIfEmpty("ne-q");
    await everyNodeKnows("ne-q", false);

    await client.createStream("ne-s", 1, 3);
    await client.produce("ne-s", 0, { value: "a" });
    await assert.rejects(client.deleteIfEmpty("ne-s"), refused(ErrorCode.RESOURCE_NOT_EMPTY));
    await client.delete("ne-s");
    await client.createStream("ne-empty", 1, 3);
    await client.deleteIfEmpty("ne-empty");
    await client.declareExchange("ne-x", ExchangeType.FANOUT);
    await client.deleteIfEmpty("ne-x");
    for (const name of ["ne-s", "ne-empty", "ne-x"]) {
      await everyNodeKnows(name, false);
    }
  });

  test("a purge settles the messages that are ready and leaves what a consumer holds", async () => {
    // The id from the answer: a node asked for metadata right after may not know the queue yet, and the id it gives
    // then matches nothing in the metrics.
    const queueId = await client.createQueue("purge-q");
    await pinLeader(client, "purge-q");
    await client.produce("purge-q", 0, [{ value: "m0" }, { value: "m1" }, { value: "m2" }, { value: "m3" }]);
    const c = await client.subscribe("purge-q", { credit: 1 });
    const kept: string[] = [];
    let purged = 0n;
    try {
      const held = await c.poll(30_000);
      assert.notEqual(held, null);
      kept.push((held as NonNullable<typeof held>).text() as string);
      // Until the leader has nothing ready: what the leader has pushed to the consumer's credit is held, not ready.
      await until("the queue to have nothing ready", async () => {
        purged += await client.purge("purge-q");
        // Only the node the metadata names leader counts: a leader that was deposed goes on reporting the queue, with
        // what it last knew, until it hears of the new term.
        const leader = await currentLeader(client, "purge-q");
        const ready = (await client.nodeMetrics())
          .filter((m) => m.nodeId === leader)
          .flatMap((m) => m.metrics?.queues ?? [])
          .filter((q) => q.resourceId === queueId)
          .map((q) => q.ready);
        assert.deepEqual(ready, [0n]);
      });
      await c.ack(held as NonNullable<typeof held>);
      for (let d = await c.poll(500); d !== null; d = await c.poll(500)) {
        kept.push(d.text() as string);
        await c.ack(d);
      }
    } finally {
      c.close();
    }
    assert.ok(kept.length >= 1, "the held message was not purged");
    assert.equal(BigInt(kept.length) + purged, 4n, "every message was purged or reached the consumer, none twice");
    assert.deepEqual(kept, ["m0", "m1", "m2", "m3"].slice(0, kept.length), "the consumer got the first ones; purge took the rest");
    assert.equal(await client.purge("purge-q"), 0n, "nothing is left");
    await assert.rejects(client.purge("no-such-queue"), UnknownStreamError);
  });

  test("a purge in many rounds settles every message", async () => {
    await client.createQueue("rounds-q");
    await pinLeader(client, "rounds-q");
    const count = 1_200;
    for (let i = 0; i < count; i += 400) {
      await client.produce(
        "rounds-q",
        0,
        Array.from({ length: 400 }, (_, j) => ({ value: `m${i + j}` })),
      );
    }
    await purgeAll("rounds-q", BigInt(count));
  });

  test("a dead letter is requeued to the queue it came from", async () => {
    await client.createQueue("rq-dead");
    await client.createQueue("rq-src", { maxDeliveries: 1, deadLetterQueue: "rq-dead" });
    await pinLeader(client, "rq-dead");
    await pinLeader(client, "rq-src");
    await client.produce("rq-src", 0, { value: "poison" });
    const c = await client.subscribe("rq-src");
    try {
      const d = await c.poll(30_000);
      assert.notEqual(d, null);
      await c.nack(d as NonNullable<typeof d>, true);
    } finally {
      c.close();
    }
    // The leader forwards the retired message asynchronously: requeue until it has arrived.
    let moved = 0n;
    await until("the dead letter to be requeued", async () => {
      moved += await client.requeue("rq-dead");
      assert.equal(moved, 1n);
    });
    const again = await client.subscribe("rq-src", { credit: 1 });
    try {
      const d = await again.poll(30_000);
      assert.notEqual(d, null);
      assert.equal((d as NonNullable<typeof d>).text(), "poison", "back where it came from");
      await again.ack(d as NonNullable<typeof d>);
    } finally {
      again.close();
    }
    assert.equal(await client.requeue("rq-dead"), 0n, "moved once, not again");
    await assert.rejects(client.requeue("no-such-queue"), UnknownStreamError);
  });

  test("a stream's settings are described with their sources, changed and reset", async () => {
    await client.createStream("cfg-s", 1, 3, { "retention.ms": "600000", "cleanup.policy": "compact" });
    let c = await configOf("cfg-s");
    assert.deepEqual(c.get("retention.ms"), { value: "600000", source: ConfigSource.DYNAMIC });
    assert.deepEqual(c.get("cleanup.policy"), { value: "compact", source: ConfigSource.DYNAMIC });
    assert.equal(c.get("retention.bytes")?.source, ConfigSource.DEFAULT, "not set: the node's default");

    await client.alterConfigs("cfg-s", { "retention.bytes": "1048576", "retention.ms": "1200000" });
    c = await configOf("cfg-s");
    assert.deepEqual(c.get("retention.bytes"), { value: "1048576", source: ConfigSource.DYNAMIC });
    assert.deepEqual(c.get("retention.ms"), { value: "1200000", source: ConfigSource.DYNAMIC });

    await client.alterConfigs("cfg-s", {}, ["retention.ms", "retention.bytes"]);
    c = await configOf("cfg-s");
    assert.equal(c.get("retention.ms")?.source, ConfigSource.DEFAULT);
    assert.equal(c.get("retention.bytes")?.source, ConfigSource.DEFAULT);
    assert.deepEqual(c.get("cleanup.policy"), { value: "compact", source: ConfigSource.DYNAMIC });

    await assert.rejects(client.createStream("cfg-bad", 1, 3, { "cleanup.policy": "bogus" }), refused(ErrorCode.INVALID_REQUEST));
    await assert.rejects(client.alterConfigs("cfg-s", { "no.such.setting": "1" }), refused(ErrorCode.INVALID_REQUEST));
  });

  test("a queue's settings are described and its ack timeout changed", async () => {
    await client.createQueue("cfg-q", { ackTimeoutMs: 5_000, maxDeliveries: 4, messageTtlMs: 60_000 });
    let c = await configOf("cfg-q");
    assert.deepEqual(c.get("ack.timeout.ms"), { value: "5000", source: ConfigSource.DYNAMIC });
    assert.deepEqual(c.get("max.deliveries"), { value: "4", source: ConfigSource.DYNAMIC });
    assert.deepEqual(c.get("message.ttl.ms"), { value: "60000", source: ConfigSource.DYNAMIC });

    await client.alterConfigs("cfg-q", { "ack.timeout.ms": "1500" });
    c = await configOf("cfg-q");
    assert.deepEqual(c.get("ack.timeout.ms"), { value: "1500", source: ConfigSource.DYNAMIC });
    await assert.rejects(client.alterConfigs("cfg-q", { "ack.timeout.ms": "not a number" }), refused(ErrorCode.INVALID_REQUEST));
    await assert.rejects(client.alterConfigs("no-such-queue", {}), UnknownStreamError);
  });

  test("a stream gets more partitions, which every node serves, and never fewer", async () => {
    await client.createStream("parts-s", 1, 3);
    await client.createPartitions("parts-s", 3);
    for (const n of eachNode) {
      const c = at(n);
      await until(`node ${n} to show three partitions with leaders`, async () => {
        const r = (await c.metadata("parts-s")).resources.find((x) => x.name === "parts-s");
        assert.equal(r?.partitions.length, 3);
        assert.ok(r?.partitions.every((p) => p.leader !== 0));
      });
    }
    const first = await until("a produce to the new partition", () => client.produce("parts-s", 2, { value: "on two" }));
    assert.ok(first.baseOffset >= 0n);
    const read: string[] = [];
    for await (const r of client.readRecords("parts-s", 2, { untilEnd: true, maxWaitMs: 50 })) {
      read.push(Buffer.from(r.value as Uint8Array).toString());
    }
    assert.deepEqual(read, ["on two"]);
    await assert.rejects(client.createPartitions("parts-s", 2), refused(ErrorCode.INVALID_REQUEST));
    await assert.rejects(client.createPartitions("no-such-stream", 2), UnknownStreamError);
  });

  test("describeNodes lists the three nodes as the metadata group knows them", async () => {
    const nodes = await client.describeNodes();
    assert.deepEqual(nodes.map((n) => n.nodeId), [1n, 2n, 3n]);
    for (const [i, n] of nodes.entries()) {
      assert.equal(n.client, cluster.bootstrap[i]);
      assert.match(n.peer, /^127\.0\.0\.1:\d+$/);
      assert.equal(n.state, NodeState.ACTIVE);
      assert.ok(n.replicas > 0, "every node holds replicas");
    }
  });

  test("a node that is not in the cluster, or already is, is refused", async () => {
    await assert.rejects(client.removeNode(99), refused(ErrorCode.UNKNOWN_RESOURCE));
    await assert.rejects(client.addNode(1, "127.0.0.1:1", "127.0.0.1:2"), refused(ErrorCode.RESOURCE_EXISTS));
    assert.equal((await client.describeNodes()).length, 3, "nothing changed");
  });

  test("every node reports its metrics, and what was produced shows on the leader", async () => {
    const id = await client.createStream("metrics-s", 1, 3);
    // Produce counters are the leader's, in memory: the leader stays where it took the produce.
    await pinLeader(client, "metrics-s");
    await client.produce("metrics-s", 0, [{ value: "a" }, { value: "b" }, { value: "c" }]);
    await until("the leader's counters", async () => {
      const all = await client.nodeMetrics();
      assert.deepEqual(all.map((m) => m.nodeId), [1, 2, 3]);
      for (const m of all) {
        assert.equal(m.error, null);
        assert.equal(m.metrics?.nodeId, m.nodeId);
        assert.ok((m.metrics?.version.length ?? 0) > 0);
        assert.ok((m.metrics?.uptimeMs ?? 0n) > 0n);
      }
      const records = all
        .flatMap((m) => m.metrics?.partitions ?? [])
        .filter((p) => p.resourceId === id && p.leader)
        .reduce((sum, p) => sum + p.produceRecords, 0n);
      assert.equal(records, 3n);
    });
  });

  test("a leader is pinned to a voter and unpinned, and a node that is no voter is refused", async () => {
    await client.createStream("pin-s", 1, 3);
    const { leader, voters } = await leaderOf("pin-s");
    const target = voters.find((v) => v !== leader) ?? never("no other voter");
    await client.moveLeader("pin-s", 0, target);
    await until(`node ${target} to lead pin-s`, async () => {
      assert.equal((await leaderOf("pin-s")).leader, target);
    });
    await client.moveLeader("pin-s", 0, null);
    await assert.rejects(client.moveLeader("pin-s", 0, 99), refused(ErrorCode.INVALID_REQUEST));
    await assert.rejects(client.moveLeader("pin-s", 7, target), refused(ErrorCode.UNKNOWN_RESOURCE));
    await assert.rejects(client.moveLeader("no-such-stream", 0, target), UnknownStreamError);
    // The cluster still serves it, whoever leads now.
    await until("a produce after the pin was removed", () => client.produce("pin-s", 0, { value: "ok" }));
  });
});

describe("admin calls against a node that requires authentication", { skip }, () => {
  let certs: Certs;
  let node: NodeProcess;
  let adminTls: TlsConfig;
  let anonymousTls: TlsConfig;
  let alice: LotseClient;
  const extra: LotseClient[] = [];
  const connections: Connection[] = [];

  const bootstrap = (): string[] => [formatAddress(certs.address)];
  const user = (name: string, password: string, tenant?: string): LotseClient => {
    const c = new LotseClient({
      bootstrap: bootstrap(),
      tls: anonymousTls,
      credentials: scramCredentials(name, password),
      requestTimeoutMs: 5_000,
      retryForMs: 8_000,
      ...(tenant === undefined ? {} : { tenant }),
    });
    extra.push(c);
    return c;
  };

  before(async () => {
    if (skip !== false) {
      return;
    }
    certs = await Certs.make(binary());
    adminTls = await certs.adminTls();
    anonymousTls = await certs.anonymousTls();
    node = NodeProcess.start(binary(), certs);
    alice = new LotseClient({ bootstrap: bootstrap(), tls: adminTls, requestTimeoutMs: 5_000, retryForMs: 60_000 });
    await retry("the node to start", node, () => alice.metadata());
  });

  after(async () => {
    alice?.close();
    for (const c of extra) {
      c.close();
    }
    for (const c of connections) {
      c.close();
    }
    await node?.stop();
  });

  const bindingOf = (subject: string, role: number, tenant: string, prefix = ""): RoleBinding => ({
    subjectKind: SubjectKind.USER,
    subject,
    role,
    tenant,
    prefix,
  });

  test("the admin is the principal its certificate names", async () => {
    const who = await alice.whoAmI();
    assert.equal(who.name, "alice");
    assert.deepEqual(who.groups, []);
  });

  test("a password set by the admin logs the user in, and it can be changed and removed", async () => {
    await alice.setUserPassword("bob", "first password");
    const users = await alice.describeUsers();
    assert.deepEqual(users.find((u) => u.name === "bob"), {
      name: "bob",
      mechanism: "SCRAM-SHA-512",
      iterations: 4096,
      source: ConfigSource.DYNAMIC,
    });
    const bob = user("bob", "first password");
    assert.equal((await bob.whoAmI()).name, "bob");

    // A wrong password is a failed login, said so, and not retried until the time is up.
    const wrong = user("bob", "wrong");
    await assert.rejects(wrong.whoAmI(), ClientAuthenticationError);

    await alice.setUserPassword("bob", "second password");
    await until("the old password to stop working", async () => {
      const old = new LotseClient({ bootstrap: bootstrap(), tls: anonymousTls, credentials: scramCredentials("bob", "first password"), retryForMs: 3_000 });
      extra.push(old);
      await assert.rejects(old.whoAmI(), ClientAuthenticationError);
    });
    assert.equal((await user("bob", "second password").whoAmI()).name, "bob");

    await alice.setUserPassword("dora", "doras password");
    await alice.deleteUser("dora");
    assert.equal((await alice.describeUsers()).some((u) => u.name === "dora"), false);
    await assert.rejects(user("dora", "doras password").whoAmI(), ClientAuthenticationError);
    await assert.rejects(user("bob", "second password").setUserPassword("eve", "x"), refused(ErrorCode.UNAUTHORIZED));
  });

  test("a user without a role may not administer anything, and sees no tenant", async () => {
    const bob = user("bob", "second password");
    await assert.rejects(bob.createStream("bob-s", 1, 1), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.createTenant("bobs"), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.alterRoleBindings([bindingOf("bob", Role.CLUSTER_ADMIN, "default")]), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.revokeSessions("alice"), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.removeNode(1), refused(ErrorCode.UNAUTHORIZED));
    assert.deepEqual(await bob.describeTenants(), []);
  });

  test("tenants and role bindings: a tenant admin may in their tenant and not outside it", async () => {
    await alice.createTenant("acme");
    const tenants = await alice.describeTenants();
    assert.deepEqual(tenants.find((t) => t.name === "acme"), { name: "acme", source: ConfigSource.DYNAMIC, resources: 0 });
    assert.ok(tenants.some((t) => t.name === "default"));
    await assert.rejects(alice.createTenant("acme"), refused(ErrorCode.RESOURCE_EXISTS));

    const binding = bindingOf("bob", Role.TENANT_ADMIN, "acme");
    await alice.alterRoleBindings([binding]);
    const listed = (await alice.describeRoleBindings()).find((b) => b.binding.subject === "bob");
    assert.deepEqual(listed, { binding, source: ConfigSource.DYNAMIC });

    const bob = user("bob", "second password", "acme");
    const who = await bob.whoAmI();
    assert.deepEqual(who.bindings.filter((b) => b.subject === "bob"), [binding]);
    assert.deepEqual((await bob.describeTenants()).map((t) => t.name), ["acme"], "only a tenant they have a role in");

    // Inside the tenant.
    await bob.createStream("acme-s", 1, 1);
    await bob.createQueue("acme-q", { replication: 1 });
    await bob.createPartitions("acme-s", 2);
    await bob.alterConfigs("acme-q", { "ack.timeout.ms": "2500" });
    assert.equal((await bob.describeConfigs("acme-q")).find((e) => e.key === "ack.timeout.ms")?.value, "2500");
    await until("a produce to the new partition", () => bob.produce("acme-q", 0, { value: "x" }));
    assert.equal(await until("the purge", async () => {
      const n = await bob.purge("acme-q");
      assert.equal(n, 1n);
      return n;
    }), 1n);

    // Outside it: the default tenant, the cluster's own business, and a resource of another tenant.
    const outside = user("bob", "second password");
    await assert.rejects(outside.createStream("default-s", 1, 1), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.createTenant("evil"), refused(ErrorCode.UNAUTHORIZED));
    await assert.rejects(bob.moveLeader("acme-s", 0, 1), refused(ErrorCode.UNAUTHORIZED));
    await alice.createStream("default-only", 1, 1);
    await assert.rejects(bob.deleteIfEmpty("default-only"), UnknownStreamError, "what a principal may not see is unknown to it");
    await assert.rejects(bob.createPartitions("default-only", 2), UnknownStreamError);

    // A tenant that holds resources is not deleted; once they are gone, it is, with its bindings.
    await assert.rejects(alice.deleteTenant("acme"), refused(ErrorCode.INVALID_REQUEST));
    await bob.delete("acme-s");
    await bob.delete("acme-q");
    await alice.deleteTenant("acme");
    assert.equal((await alice.describeTenants()).some((t) => t.name === "acme"), false);
    assert.equal((await alice.describeRoleBindings()).some((b) => b.binding.tenant === "acme"), false, "bindings go with the tenant");
  });

  test("removing a binding takes the right away, and a prefix limits it", async () => {
    await alice.createTenant("globex");
    const all = bindingOf("bob", Role.TENANT_ADMIN, "globex");
    await alice.alterRoleBindings([all]);
    const bob = user("bob", "second password", "globex");
    await until("the binding to apply", () => bob.createStream("globex-s", 1, 1));
    await alice.alterRoleBindings([], [all]);
    await until("the removal to apply", async () => {
      await assert.rejects(bob.createStream("globex-t", 1, 1), refused(ErrorCode.UNAUTHORIZED));
    });

    // The same role limited to a prefix: names outside it are not the user's to create.
    const orders = bindingOf("bob", Role.TENANT_ADMIN, "globex", "orders-");
    await alice.alterRoleBindings([orders]);
    await until("the prefixed binding to apply", () => bob.createStream("orders-eu", 1, 1));
    await assert.rejects(bob.createStream("billing", 1, 1), refused(ErrorCode.UNAUTHORIZED));
    // The same list twice is the same list: removal first, then addition.
    await alice.alterRoleBindings([orders], [orders]);
    assert.equal((await alice.describeRoleBindings()).filter((b) => b.binding.prefix === "orders-").length, 1);
    await alice.alterRoleBindings([], [orders]);
    await alice.delete("globex-s");
    await alice.delete("orders-eu");
    await alice.deleteTenant("globex");
  });

  test("revoked sessions end, and the user logs in again with the password", async () => {
    await alice.setUserPassword("rita", "ritas password");
    const login = async (): Promise<Connection> => {
      const c = await Connection.open(certs.address, { tls: anonymousTls, credentials: scramCredentials("rita", "ritas password"), requestTimeoutMs: 5_000 });
      connections.push(c);
      return c;
    };
    const open = await login();
    assert.equal((await open.call({ type: "WhoAmI" }, "WhoAmI")).name, "rita");
    await sleep(100); // the revocation names "now": the session must predate it
    await alice.revokeSessions("rita");
    await until("the session to end", async () => {
      await assert.rejects(
        open.call({ type: "WhoAmI" }),
        (e: unknown) => (e instanceof BrokerError && e.code === ErrorCode.UNAUTHORIZED) || e instanceof ConnectionError,
      );
    });
    await sleep(100);
    const fresh = await login();
    assert.equal((await fresh.call({ type: "WhoAmI" }, "WhoAmI")).name, "rita", "a new login after the revocation is a new session");

    // A client reconnects by itself and keeps working.
    const rita = user("rita", "ritas password");
    assert.equal((await rita.whoAmI()).name, "rita");
    await sleep(100);
    await alice.revokeSessions("rita");
    await sleep(100);
    assert.equal((await rita.whoAmI()).name, "rita");

    // A revoked group is accepted whether or not anyone is in it; removing the password keeps the user out.
    await alice.revokeSessions("nobody-group", true);
    await alice.deleteUser("rita");
    await until("the login to be refused", async () => {
      await assert.rejects(Connection.open(certs.address, { tls: anonymousTls, credentials: scramCredentials("rita", "ritas password") }), AuthenticationError);
    });
  });

  test("a node that requires authentication reports the admin's node metrics", async () => {
    const [m] = await alice.nodeMetrics();
    assert.equal(m?.nodeId, 1);
    assert.equal(m?.error, null);
    assert.equal(m?.metrics?.nodeId, 1);
    // An anonymous client is told no more than that it is not authenticated.
    const anonymous = new LotseClient({ bootstrap: bootstrap(), tls: anonymousTls, retryForMs: 2_000 });
    extra.push(anonymous);
    await assert.rejects(anonymous.describeNodes(), (e: unknown) => e instanceof ClientAuthenticationError);
  });
});

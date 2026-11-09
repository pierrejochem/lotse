import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  Batch,
  BrokerResponseError,
  type BatchRecord,
  ErrorCode,
  ExchangeType,
  type GroupConsumer,
  type GroupRecord,
  LotseClient,
  MustAbortError,
  OffsetAt,
  type Delivery,
  type QueueConsumer,
  UnknownOutcomeError,
  UnroutableError,
} from "../src/index.js";
import { Connection } from "../src/index.js";
import { PlainCluster } from "./support/cluster.js";
import { binary, skipWithoutBinary } from "./support/lotse.js";

/**
 * Transactions, publishing and consumer groups against three real `lotse` processes on loopback (plain TCP, no
 * authentication): commit and abort as read_committed readers see them, a producer fenced by a newer epoch, exactly-once
 * consume-transform-produce from a stream and from a queue, `MESSAGE_NOT_HELD` between two transactions, groups with two
 * members and no partition twice, a static member taken over, patterns, publishing through a topic exchange to two queues
 * and a publish whose answer never comes. Skipped, with the reason, when no `lotse` executable is found (set LOTSE_BIN).
 */

const skip = skipWithoutBinary();

const text = (r: BatchRecord): string => Buffer.from(r.value ?? []).toString();

describe("transactions, publishing and groups against three real nodes", { skip }, () => {
  let cluster: PlainCluster;
  let client: LotseClient;
  const extra: LotseClient[] = [];

  const options = (more: object = {}) => ({
    bootstrap: cluster.bootstrap,
    plaintextOnLoopback: true,
    requestTimeoutMs: 5_000,
    retryForMs: 60_000,
    ...more,
  });
  const newClient = (more: object = {}): LotseClient => {
    const c = new LotseClient(options(more));
    extra.push(c);
    return c;
  };

  before(async () => {
    cluster = await PlainCluster.make(binary(), 3);
    cluster.startAll();
    client = new LotseClient(options());
  });

  after(async () => {
    client?.close();
    for (const c of extra) {
      c.close();
    }
    await cluster?.stop();
  });

  /** What a read_committed reader sees in a partition, as text. */
  async function committed(c: LotseClient, stream: string, partition = 0): Promise<string[]> {
    const out: string[] = [];
    for await (const r of c.readRecords(stream, partition, { untilEnd: true, maxWaitMs: 50 })) {
      out.push(text(r));
    }
    return out;
  }

  /**
   * What a read_committed reader sees once the markers are in. EndTxn returns when the decision is durable; the markers
   * reach the partitions after that (ADR 0029), so a reader may see too little for a moment. Wait for the expected
   * records up to a bound, then compare, so a wrong result still fails with what was read.
   */
  async function assertCommitted(
    c: LotseClient,
    stream: string,
    expected: string[],
    message?: string,
    partition = 0,
  ): Promise<void> {
    const deadline = Date.now() + 15_000;
    let got = await committed(c, stream, partition);
    while (Date.now() < deadline && JSON.stringify(got) !== JSON.stringify(expected)) {
      await new Promise((r) => setTimeout(r, 100));
      got = await committed(c, stream, partition);
    }
    if (message === undefined) assert.deepEqual(got, expected);
    else assert.deepEqual(got, expected, message);
  }

  /** The data records, transactional or not, that are in the partition whatever happened to them. */
  async function everything(c: LotseClient, stream: string, partition = 0): Promise<string[]> {
    const out: string[] = [];
    let offset = await c.listOffset(stream, partition, OffsetAt.EARLIEST);
    for (;;) {
      const f = await c.fetchUncommitted(stream, partition, offset, 50);
      if (f.records.length === 0) {
        return out;
      }
      for (const b of Batch.parseAll(f.records)) {
        if (!b.attributes.control) {
          out.push(...b.records().map(text));
        }
      }
      offset = f.nextOffset(offset);
    }
  }

  /** Waits until every node reports `want` bindings on the exchange: a node routes by the bindings it has applied. */
  async function bindingsEverywhere(exchange: string, want: number): Promise<void> {
    const deadline = Date.now() + 30_000;
    for (let id = 1; id <= 3; id++) {
      for (;;) {
        const c = await Connection.open(cluster.address(id), { plaintextOnLoopback: true });
        const m = await c.call({ type: "Metadata", names: [exchange] }, "Metadata");
        c.close();
        if (m.resources.find((r) => r.name === exchange)?.exchange?.bindings.length === want) {
          break;
        }
        assert.ok(Date.now() < deadline, `node ${id} never showed ${want} bindings on ${exchange}`);
        await sleep(50);
      }
    }
  }

  /** Waits until the queue's leader has stayed the same for half a second (a new group elects one in its first moments). */
  async function settled(queue: string): Promise<void> {
    const leaderOf = async (): Promise<number> => {
      for (;;) {
        const m = await client.metadata(queue);
        const l = m.resources.find((r) => r.name === queue)?.partitions[0]?.leader ?? 0;
        if (l !== 0) {
          return l;
        }
        await sleep(100);
      }
    };
    let leader = await leaderOf();
    let same = 0;
    while (same < 5) {
      await sleep(100);
      const now = await leaderOf();
      same = now === leader ? same + 1 : 0;
      leader = now;
    }
  }

  // ---- commit and abort ----

  test("a committed transaction is visible together, an aborted one never", async () => {
    await client.createStream("vis", 2, 3);
    const lotse = newClient({ transactionalId: "vis-writer" });

    const t1 = await lotse.beginTransaction();
    await t1.produce("vis", 0, [{ value: "c0-a" }, { value: "c0-b" }]);
    await t1.produce("vis", 1, { value: "c1-a" });
    assert.deepEqual(await committed(client, "vis", 0), [], "nothing before the commit");
    assert.deepEqual(await committed(client, "vis", 1), []);
    assert.deepEqual(await everything(client, "vis", 0), ["c0-a", "c0-b"], "the batches are there, hidden");
    await t1.commit();
    await assertCommitted(client, "vis", ["c0-a", "c0-b"], undefined, 0);
    await assertCommitted(client, "vis", ["c1-a"], undefined, 1);

    const t2 = await lotse.beginTransaction();
    await t2.produce("vis", 0, { value: "gone-0" });
    await t2.produce("vis", 1, { value: "gone-1" });
    await t2.abort();
    const t3 = await lotse.beginTransaction();
    await t3.produce("vis", 0, { value: "c0-c" });
    await t3.commit();
    await assertCommitted(client, "vis", ["c0-a", "c0-b", "c0-c"], "the aborted batch is skipped", 0);
    await assertCommitted(client, "vis", ["c1-a"], undefined, 1);
    assert.deepEqual(await everything(client, "vis", 0), ["c0-a", "c0-b", "gone-0", "c0-c"], "and still in the log");
  });

  test("a transaction left to await using is aborted and its data never shows", async () => {
    await client.createStream("disposed", 1, 3);
    const lotse = newClient({ transactionalId: "disposer" });
    await assert.rejects(async () => {
      await using txn = await lotse.beginTransaction();
      await txn.produce("disposed", 0, { value: "lost" });
      throw new Error("the work failed");
    }, /the work failed/);
    // An abandoned transaction would hold the last stable offset back: a plain record after it stays hidden until it ends.
    await client.produce("disposed", 0, { value: "plain" });
    await assertCommitted(client, "disposed", ["plain"], "the transaction was ended, not left open");
    {
      await using txn = await lotse.beginTransaction();
      await txn.produce("disposed", 0, { value: "kept" });
      await txn.commit();
    }
    await assertCommitted(client, "disposed", ["plain", "kept"]);
  });

  test("a producer fenced by a newer epoch cannot write or commit any more", async () => {
    await client.createStream("fenced", 1, 3);
    const old = newClient({ transactionalId: "same-id" });
    const stale = await old.beginTransaction();
    await stale.produce("fenced", 0, { value: "stale-1" });

    // A new client with the same transactional id raises the epoch: that fences the old one and aborts what it left open.
    const fresh = newClient({ transactionalId: "same-id" });
    const current = await fresh.beginTransaction();
    await current.produce("fenced", 0, { value: "fresh-1" });

    let fencedWith: unknown;
    try {
      await stale.produce("fenced", 0, { value: "stale-2" });
      await stale.commit();
    } catch (e) {
      fencedWith = e;
    }
    assert.ok(fencedWith instanceof BrokerResponseError, `fenced, not ${String(fencedWith)}`);
    assert.ok(
      [ErrorCode.PRODUCER_FENCED, ErrorCode.INVALID_TXN_STATE].includes(fencedWith.code as never),
      `the code was ${fencedWith.code}`,
    );
    await current.commit();
    await assertCommitted(client, "fenced", ["fresh-1"], "only the newer producer's data is committed");
    // The stale transaction can only be aborted, and the abort does not get through either.
    await assert.rejects(stale.commit(), MustAbortError);
    await stale.abort().catch(() => {});
  });

  // ---- exactly once ----

  /** Reads one record at a time from a group member until `want` have been read, with a bound on the time. */
  async function drain(g: GroupConsumer, want: number, ms = 60_000): Promise<GroupRecord[]> {
    const out: GroupRecord[] = [];
    const deadline = Date.now() + ms;
    while (out.length < want && Date.now() < deadline) {
      out.push(...(await g.poll(200)));
    }
    return out;
  }

  test("consume-transform-produce from a stream is exactly once, also across an abort", async () => {
    await client.createStream("eos-in", 1, 3);
    await client.createStream("eos-out", 1, 3);
    for (let i = 0; i < 12; i++) {
      await client.produce("eos-in", 0, { value: `in-${i}` });
    }
    const lotse = newClient({ transactionalId: "eos-1" });
    const g = await lotse.joinGroup("eos-group", ["eos-in"], { autoCommit: false });
    let processed = 0;
    let failedOnce = false;
    const batchesSeen: number[] = [];
    const deadline = Date.now() + 90_000;
    while (processed < 12 && Date.now() < deadline) {
      const records = await g.poll(200);
      if (records.length === 0) {
        continue;
      }
      batchesSeen.push(records.length);
      const txn = await lotse.beginTransaction();
      try {
        for (const r of records) {
          await txn.produce("eos-out", 0, { value: text(r.record).toUpperCase() });
        }
        await txn.sendOffsets(g);
        if (!failedOnce) {
          failedOnce = true;
          // The first attempt fails after it wrote and sent the offsets: neither may show.
          assert.deepEqual(await committed(client, "eos-out"), [], "not yet");
          throw new Error("the work failed after it wrote");
        }
        await txn.commit();
        processed += records.length;
      } catch (e) {
        if (!(e instanceof Error) || e.message !== "the work failed after it wrote") {
          throw e;
        }
        await txn.abort();
        g.rewind(); // read again from the committed offset
      }
    }
    assert.ok(failedOnce, "the abort path ran");
    assert.equal(batchesSeen.length, 2, "the same records were read again after the abort");
    await assertCommitted(
      client,
      "eos-out",
      Array.from({ length: 12 }, (_, i) => `IN-${i}`),
      "every input once, in order",
    );
    // The group's committed offset is the end of what was processed: a new member has nothing left to read.
    await g.close();
    const again = await newClient().joinGroup("eos-group", ["eos-in"], { autoCommit: false });
    assert.deepEqual(await again.poll(500), []);
    assert.deepEqual(await again.poll(500), []);
    await again.close();
  });

  async function take(c: QueueConsumer, ms = 30_000): Promise<Delivery> {
    const d = await c.poll(ms);
    assert.notEqual(d, null, "a message should have come");
    return d as Delivery;
  }

  test("offsets sent without any write are committed with the transaction", async () => {
    await client.createStream("offsets-only-in", 1, 3);
    await client.produce("offsets-only-in", 0, { value: "m0" });
    await client.produce("offsets-only-in", 0, { value: "m1" });
    const lotse = newClient({ transactionalId: "offsets-only-worker" });
    const g = await lotse.joinGroup("offsets-only-group", ["offsets-only-in"], { autoCommit: false });
    assert.equal((await drain(g, 2)).length, 2);
    // No produce, no partition added: only the offsets. The commit must still reach the coordinator, or the offsets
    // stay pending and the next member waits until the transaction times out.
    const txn = await lotse.beginTransaction();
    await txn.sendOffsets(g);
    await txn.commit();
    await g.close();
    await client.produce("offsets-only-in", 0, { value: "m2" });
    const next = await newClient().joinGroup("offsets-only-group", ["offsets-only-in"], { autoCommit: false });
    const got = await drain(next, 1, 30_000);
    assert.deepEqual(got.map((r) => text(r.record)), ["m2"]);
    await next.close();
  });

  test("consume-transform-produce from a queue is exactly once; an aborted ack gives the message back", async () => {
    await client.createQueue("eq-in");
    await client.createStream("eq-out", 1, 3);
    await settled("eq-in");
    for (let i = 0; i < 6; i++) {
      await client.produce("eq-in", 0, { value: `job-${i}` });
    }
    const lotse = newClient({ transactionalId: "eq-1" });
    const c = await lotse.subscribe("eq-in", { credit: 3 });
    let aborted = 0;
    const done = new Set<string>();
    while (done.size < 6) {
      const d = await take(c);
      const v = d.text() as string;
      const txn = await lotse.beginTransaction();
      await txn.produce("eq-out", 0, { value: `done-${v}` });
      await c.ackIn(txn, d);
      if (aborted === 0 && done.size === 2) {
        aborted++;
        await txn.abort(); // the message goes back, and the output with it is hidden
        continue;
      }
      await txn.commit();
      done.add(v);
    }
    assert.equal(aborted, 1);
    // The markers follow the commits (ADR 0029): wait until the reader sees all six, then check each came once.
    for (let i = 0; i < 150 && (await committed(client, "eq-out")).length < 6; i++) {
      await new Promise((r) => setTimeout(r, 100));
    }
    const out = await committed(client, "eq-out");
    assert.equal(out.length, 6, `each exactly once: ${out.join()}`);
    assert.deepEqual([...out].sort(), Array.from({ length: 6 }, (_, i) => `done-job-${i}`).sort());
    // Everything is settled: nothing comes again.
    assert.equal(await c.poll(1_500), null);
    c.close();
  });

  test("of two transactions the first holds a message and the other gets MESSAGE_NOT_HELD", async () => {
    await client.createQueue("held");
    await settled("held");
    await client.produce("held", 0, { value: "only" });
    const one = newClient({ transactionalId: "held-1" });
    const two = newClient({ transactionalId: "held-2" });
    const c = await one.subscribe("held");
    const d = await take(c);
    const a = await one.beginTransaction();
    const b = await two.beginTransaction();
    await c.ackIn(a, d);
    await assert.rejects(c.ackIn(b, d), (e: unknown) => e instanceof BrokerResponseError && e.code === ErrorCode.MESSAGE_NOT_HELD);
    await assert.rejects(b.commit(), MustAbortError);
    await b.abort();
    await a.commit();
    assert.equal(await c.poll(1_000), null, "settled by the first");
    c.close();

    // And when the holder aborts, the message is back.
    await client.produce("held", 0, { value: "again" });
    const c2 = await one.subscribe("held");
    const d2 = await take(c2);
    const t = await one.beginTransaction();
    await c2.ackIn(t, d2);
    await t.abort();
    const back = await take(c2);
    assert.equal(back.text(), "again");
    assert.ok(back.deliveryCount >= 1);
    await c2.ack(back);
    c2.close();
  });

  // ---- groups ----

  test("two members share the partitions, no partition twice, no record twice", async () => {
    await client.createStream("share", 4, 3);
    let next = 0;
    const produceMore = async (n: number): Promise<void> => {
      for (let i = 0; i < n; i++, next++) {
        await client.produce("share", next % 4, { value: `r-${next}` });
      }
    };
    await produceMore(40);
    const m1 = await newClient().joinGroup("share-group", ["share"]);
    const m2 = await newClient().joinGroup("share-group", ["share"]);
    const members = [m1, m2];
    const seen = new Map<string, number>();
    const counts = [0, 0];
    const overlaps: string[] = [];
    const note = (i: number, rs: readonly GroupRecord[]): void => {
      for (const r of rs) {
        seen.set(text(r.record), (seen.get(text(r.record)) ?? 0) + 1);
        counts[i] = (counts[i] ?? 0) + 1;
      }
    };
    // m1 starts alone and owns every partition; the 40 records are its.
    const first = await drain(m1, 40);
    note(0, first);
    assert.deepEqual(m1.assignment().get("share"), [0, 1, 2, 3], "alone, it owns every partition");
    // m2 joins: the partitions move over, at no instant with two owners.
    const balanced = (): boolean => (m1.assignment().get("share")?.length ?? 0) === 2 && (m2.assignment().get("share")?.length ?? 0) === 2;
    const until = Date.now() + 90_000;
    while (Date.now() < until && !balanced()) {
      const got = await Promise.all(members.map((m) => m.poll(100)));
      got.forEach((rs, i) => note(i, rs));
      const x = new Set(m1.assignment().get("share") ?? []);
      for (const p of m2.assignment().get("share") ?? []) {
        if (x.has(p)) {
          overlaps.push(`partition ${p}`);
        }
      }
    }
    assert.deepEqual(overlaps, [], "a partition had two owners");
    assert.ok(balanced(), `two partitions each: ${JSON.stringify([...m1.assignment()])} ${JSON.stringify([...m2.assignment()])}`);
    const a1 = m1.assignment().get("share") ?? [];
    const a2 = m2.assignment().get("share") ?? [];
    assert.deepEqual([...a1, ...a2].sort(), [0, 1, 2, 3]);
    // More records, read by both.
    await produceMore(40);
    const deadline = Date.now() + 60_000;
    while (seen.size < next && Date.now() < deadline) {
      const got = await Promise.all(members.map((m) => m.poll(100)));
      got.forEach((rs, i) => note(i, rs));
    }
    assert.equal(seen.size, next, "every record was read");
    assert.deepEqual([...seen.entries()].filter(([, n]) => n !== 1), [], "and each once, across the hand-over");
    assert.ok((counts[0] ?? 0) > 0 && (counts[1] ?? 0) > 0, `both members read: ${counts.join()}`);
    await m1.close();
    await m2.close();
  });

  test("a static member comes back to its partitions, and the instance it replaced is fenced", async () => {
    await client.createStream("static", 2, 3);
    for (let i = 0; i < 4; i++) {
      await client.produce("static", i % 2, { value: `s-${i}` });
    }
    const first = await newClient().joinGroup("static-group", ["static"], { instanceId: "inst-1" });
    await drain(first, 4);
    assert.deepEqual(first.assignment().get("static"), [0, 1]);
    const idBefore = first.memberId;

    // A second instance with the same id takes over with its partitions and no rebalance.
    const second = await newClient().joinGroup("static-group", ["static"], { instanceId: "inst-1" });
    const until = Date.now() + 30_000;
    while (Date.now() < until && (second.assignment().get("static")?.length ?? 0) < 2) {
      await second.poll(100);
    }
    assert.deepEqual(second.assignment().get("static"), [0, 1], "the same partitions");
    console.log(`      (member ids ${idBefore} -> ${second.memberId})`);

    // The first instance learns with its next heartbeat that it was replaced.
    let fenced: unknown;
    const limit = Date.now() + 30_000;
    while (Date.now() < limit && fenced === undefined) {
      try {
        await first.poll(100);
      } catch (e) {
        fenced = e;
      }
    }
    assert.ok(fenced instanceof BrokerResponseError, `fenced, not ${String(fenced)}`);
    assert.equal((fenced as BrokerResponseError).code, ErrorCode.FENCED_INSTANCE_ID);
    await assert.rejects(first.poll(100), BrokerResponseError, "and it stays out");
    await second.close();
  });

  test("a pattern takes the streams that match, also the ones created later", async () => {
    await client.createStream("pat-a", 1, 3);
    await client.createStream("pat-b", 1, 3);
    await client.createStream("other", 1, 3);
    const g = await newClient().joinGroupMatching("pat-group", "pat-.*");
    const until = Date.now() + 30_000;
    while (Date.now() < until && (g.assignment().size < 2)) {
      await g.poll(100);
    }
    assert.deepEqual([...g.assignment().keys()].sort(), ["pat-a", "pat-b"]);
    await client.createStream("pat-c", 1, 3);
    const later = Date.now() + 60_000;
    while (Date.now() < later && !g.assignment().has("pat-c")) {
      await g.poll(100);
    }
    assert.deepEqual([...g.assignment().keys()].sort(), ["pat-a", "pat-b", "pat-c"]);
    await g.close();
  });

  test("a group without a committed offset starts where it is told to", async () => {
    await client.createStream("from", 1, 3);
    for (let i = 0; i < 5; i++) {
      await client.produce("from", 0, { value: `old-${i}` });
    }
    const g = await newClient().joinGroup("from-latest", ["from"], { startFrom: OffsetAt.LATEST });
    const until = Date.now() + 30_000;
    while (Date.now() < until && g.positions().size === 0) {
      await g.poll(100);
    }
    assert.equal(g.positions().get("from")?.size, 1, "it has a position, and nothing read");
    await client.produce("from", 0, { value: "new-0" });
    const got = await drain(g, 1);
    assert.deepEqual(got.map((r) => text(r.record)), ["new-0"]);
    await g.close();
    const all = await newClient().joinGroup("from-earliest", ["from"]);
    assert.deepEqual((await drain(all, 6)).map((r) => text(r.record)), ["old-0", "old-1", "old-2", "old-3", "old-4", "new-0"]);
    await all.close();
  });

  test("a group member does not read what an open transaction holds", async () => {
    await client.createStream("held-back", 1, 3);
    const lotse = newClient({ transactionalId: "holder" });
    const txn = await lotse.beginTransaction();
    await txn.produce("held-back", 0, { value: "pending" });
    const g = await newClient().joinGroup("hb-group", ["held-back"]);
    assert.deepEqual(await g.poll(300), []);
    assert.deepEqual(await g.poll(300), []);
    await txn.commit();
    assert.deepEqual((await drain(g, 1)).map((r) => text(r.record)), ["pending"]);
    await g.close();
  });

  // ---- publishing ----

  test("a publish through a topic exchange reaches the queues its key matches", async () => {
    await client.declareExchange("orders", ExchangeType.TOPIC);
    await client.createQueue("orders-all");
    await client.createQueue("orders-eu");
    await client.bind("orders", "orders-all", "orders.#");
    await client.bind("orders", "orders-eu", "*.eu");
    await bindingsEverywhere("orders", 2);
    await settled("orders-all");
    await settled("orders-eu");

    const both = await client.publish("orders", "orders.eu", { value: "m-eu" });
    assert.deepEqual(
      both.routed.map((r) => r.queue),
      ["orders-all", "orders-eu"],
    );
    const one = await client.publish("orders", "orders.us", { value: "m-us" });
    assert.deepEqual(
      one.routed.map((r) => r.queue),
      ["orders-all"],
    );
    assert.deepEqual((await client.publish("orders", "nothing", { value: "m-none" })).routed, []);
    await assert.rejects(client.publish("orders", "nothing", { value: "m-none" }, { mandatory: true }), UnroutableError);

    const all = await client.subscribe("orders-all");
    const eu = await client.subscribe("orders-eu");
    assert.deepEqual([(await take(all)).text(), (await take(all)).text()], ["m-eu", "m-us"]);
    assert.equal((await take(eu)).text(), "m-eu");
    assert.equal(await eu.poll(500), null);
    all.close();
    eu.close();

    // Unbound, a key no longer reaches the queue.
    await client.unbind("orders", "orders-eu", "*.eu");
    await bindingsEverywhere("orders", 1);
    assert.deepEqual(
      (await client.publish("orders", "orders.eu", { value: "m-eu-2" })).routed.map((r) => r.queue),
      ["orders-all"],
    );
  });

  test("a publish in a transaction is routed by the client and lands in every queue or none", async () => {
    await client.declareExchange("txn-x", ExchangeType.FANOUT);
    await client.createQueue("txn-q1");
    await client.createQueue("txn-q2");
    await client.bind("txn-x", "txn-q1", "");
    await bindingsEverywhere("txn-x", 1);
    await settled("txn-q1");
    await settled("txn-q2");
    const lotse = newClient({ transactionalId: "publisher" });

    const t1 = await lotse.beginTransaction();
    await t1.publish("txn-x", "", { value: "p1" });
    await t1.abort();
    // Bound after the first routing: the client sees the new binding at once, because the bind came from it.
    await lotse.bind("txn-x", "txn-q2", "");
    await bindingsEverywhere("txn-x", 2);
    const t2 = await lotse.beginTransaction();
    const routed = await t2.publish("txn-x", "", { value: "p2" });
    assert.deepEqual(
      routed.routed.map((r) => r.queue),
      ["txn-q1", "txn-q2"],
    );
    await t2.commit();

    for (const q of ["txn-q1", "txn-q2"]) {
      const c = await client.subscribe(q);
      assert.equal((await take(c)).text(), "p2", q);
      assert.equal(await c.poll(500), null, `${q} holds nothing of the aborted publish`);
      c.close();
    }
  });

  test("a publish whose answer never comes is unknown and is not sent again", async () => {
    await client.declareExchange("slow-x", ExchangeType.FANOUT);
    await client.createQueue("slow-q");
    await client.bind("slow-x", "slow-q", "");
    await bindingsEverywhere("slow-x", 1);
    await settled("slow-q");
    const lotse = newClient({ requestTimeoutMs: 700, retryForMs: 20_000 });
    await lotse.publish("slow-x", "", { value: "first" }); // learns the exchange and the brokers
    for (const id of [1, 2, 3]) {
      cluster.pause(id);
    }
    let outcome: unknown;
    try {
      await lotse.publish("slow-x", "", { value: "second" });
    } catch (e) {
      outcome = e;
    } finally {
      for (const id of [1, 2, 3]) {
        cluster.resume(id);
      }
    }
    assert.ok(outcome instanceof UnknownOutcomeError, `unknown, not ${String(outcome)}`);
    // The node got the request and may have written it. Wait for the cluster, then count: at most once.
    await sleep(4_000);
    const c = await client.subscribe("slow-q");
    const seen: string[] = [];
    for (;;) {
      const d = await c.poll(3_000);
      if (d === null) {
        break;
      }
      seen.push(d.text() as string);
      await c.ack(d);
    }
    c.close();
    assert.equal(seen[0], "first");
    assert.ok(seen.filter((v) => v === "second").length <= 1, `sent again: ${seen.join()}`);
    console.log(`      (the unknown publish was ${seen.includes("second") ? "written once" : "not written"})`);
  });
});

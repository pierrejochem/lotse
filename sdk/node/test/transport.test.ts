import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  address,
  Connection,
  ConnectionPool,
  formatAddress,
  InsecureTransportError,
  isLoopbackAddress,
  parseAddress,
} from "../src/net/index.js";
import { ErrorCode } from "../src/proto/index.js";
import { withBroker } from "./support/fake-broker.js";

/** Plain TCP only on loopback and only when asked for (AGENTS.md, rule 7). */

test("nothing is connected until the transport is chosen", async () => {
  await withBroker(
    () => {},
    async (broker) => {
      await assert.rejects(Connection.open(broker.address, {}), InsecureTransportError);
      await assert.rejects(new ConnectionPool({}).get(broker.address), InsecureTransportError);
      await sleep(100);
      assert.equal(broker.accepted, 0, "not even a TCP connection was made");
    },
  );
});

test("plain TCP is refused off loopback before anything is sent", async () => {
  const start = Date.now();
  for (const host of ["192.0.2.1", "10.1.2.3", "0.0.0.0", "8.8.8.8", "2001:db8::1", "::ffff:10.0.0.1"]) {
    await assert.rejects(
      Connection.open(address(host, 9092), { plaintextOnLoopback: true, connectTimeoutMs: 30_000 }),
      (e: unknown) => e instanceof InsecureTransportError && e.message.includes("loopback") && !e.requestSent,
      host,
    );
  }
  assert.ok(Date.now() - start < 5_000, "refused without trying to connect");
});

test("loopback addresses are recognised", () => {
  for (const ok of ["127.0.0.1", "127.255.0.3", "::1", "::ffff:127.0.0.1"]) {
    assert.ok(isLoopbackAddress(ok), ok);
  }
  for (const no of ["128.0.0.1", "10.0.0.1", "0.0.0.0", "::", "::2", "::ffff:128.0.0.1", "1127.0.0.1"]) {
    assert.ok(!isLoopbackAddress(no), no);
  }
});

test("plain TCP to a loopback name works", async () => {
  await withBroker(
    (peer, req) => peer.reply(req.header.correlationId, { type: "Outcome", error: ErrorCode.NONE, leaderHint: null }),
    async (broker) => {
      const c = await Connection.open(address("localhost", broker.address.port), { plaintextOnLoopback: true });
      try {
        assert.equal((await c.call({ type: "Unsubscribe", subscriptionId: 1 })).type, "Outcome");
        assert.equal(c.isSecure, false);
      } finally {
        c.close();
      }
    },
  );
});

test("addresses parse", () => {
  assert.deepEqual(parseAddress("broker-1.example:9092"), address("broker-1.example", 9092));
  assert.deepEqual(parseAddress("127.0.0.1:7"), address("127.0.0.1", 7));
  assert.deepEqual(parseAddress("[::1]:9092"), address("::1", 9092));
  assert.equal(formatAddress(parseAddress("[::1]:9092")), "[::1]:9092");
  for (const bad of ["nohost", "::1:9092", "host:", "host:abc", "host:0", "host:70000", ":9092", "host:-1"]) {
    assert.throws(() => parseAddress(bad), RangeError, bad);
  }
});

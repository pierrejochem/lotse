import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  ApiKey,
  AuthenticationError,
  BrokerError,
  Connection,
  ConnectionError,
  ConnectionPool,
  ErrorCode,
  oauthBearerCredentials,
  scramCredentials,
  ScramCredential,
  type ConnectionOptions,
  type TlsConfig,
} from "../src/index.js";
import { binary, Certs, makePlainCluster, NodeProcess, retry, skipWithoutBinary } from "./support/lotse.js";

/**
 * Real `lotse` nodes, started from generated cluster files: one with TLS, `alice` as cluster admin and sessions limited
 * to 4 s, one with plain TCP on loopback and no authentication. The admin connects with a client certificate and
 * creates a SCRAM user; the user then logs in over SCRAM, renews its session, and loses it when it does not. Skipped
 * when no executable is found (set `LOTSE_BIN`).
 */

const skip = skipWithoutBinary();
const SESSION_MS = 4_000;

let certs: Certs;
let node: NodeProcess;
let adminTls: TlsConfig;
let anonymousTls: TlsConfig;
let plainNode: NodeProcess;
let plainAddress: Awaited<ReturnType<typeof makePlainCluster>>["address"];

const options = (tls: TlsConfig, credentials?: ConnectionOptions["credentials"]): ConnectionOptions => ({
  tls,
  ...(credentials === undefined ? {} : { credentials }),
  requestTimeoutMs: 10_000,
});

before(async () => {
  if (skip !== false) {
    return;
  }
  const bin = binary();
  certs = await Certs.make(bin, `max_session_s = ${SESSION_MS / 1000}\n`);
  adminTls = await certs.adminTls();
  anonymousTls = await certs.anonymousTls();
  node = NodeProcess.start(bin, certs);
  const plain = await makePlainCluster();
  plainAddress = plain.address;
  plainNode = NodeProcess.start(bin, plain);
  // Ready when the node answers Metadata.
  await retry("the TLS node to start", node, async () => {
    const c = await Connection.open(certs.address, options(adminTls));
    try {
      return await c.call({ type: "Metadata", names: [] }, "Metadata");
    } finally {
      c.close();
    }
  });
  await retry("the plain node to start", plainNode, async () => {
    const c = await Connection.open(plainAddress, { plaintextOnLoopback: true });
    try {
      return await c.call({ type: "Metadata", names: [] }, "Metadata");
    } finally {
      c.close();
    }
  });
});

after(async () => {
  await node?.stop();
  await plainNode?.stop();
});

test("plain TCP on loopback with authentication off", { skip }, async () => {
  const c = await Connection.open(plainAddress, { plaintextOnLoopback: true });
  try {
    assert.equal(c.isSecure, false);
    assert.equal(c.sessionState(), "UNLIMITED");
    const m = await c.call({ type: "Metadata", names: [] }, "Metadata");
    assert.equal(m.brokers.length, 1);
    assert.equal(m.brokers[0]?.port, plainAddress.port);
  } finally {
    c.close();
  }
  // The transport is chosen by the options: a client that asks for TLS does not talk plain TCP to this node.
  await assert.rejects(Connection.open(plainAddress, { ...options(anonymousTls), connectTimeoutMs: 3_000 }), ConnectionError);
});

test("a node answers ApiVersions and Metadata over TLS", { skip }, async () => {
  const c = await Connection.open(certs.address, options(adminTls));
  try {
    assert.ok(c.isSecure);
    const versions = await c.apiVersions();
    assert.ok(versions.versions.some((v) => v.apiKey === ApiKey.METADATA && v.min === 0));
    assert.ok(versions.versions.some((v) => v.apiKey === ApiKey.SASL_AUTHENTICATE));
    const metadata = await c.call({ type: "Metadata", names: [] }, "Metadata");
    assert.equal(metadata.brokers.length, 1);
    assert.equal(metadata.brokers[0]?.nodeId, 1);
    assert.equal(metadata.brokers[0]?.port, certs.clientPort);
    const who = await c.call({ type: "WhoAmI" }, "WhoAmI");
    assert.equal(who.name, "alice", "the principal comes from the certificate's common name");
    assert.equal(c.sessionState(), "UNLIMITED", "a certificate's session lasts the connection");
  } finally {
    c.close();
  }
});

test("pipelined requests on a real connection are all answered", { skip }, async () => {
  const c = await Connection.open(certs.address, options(adminTls));
  try {
    const answers = await Promise.all(
      Array.from({ length: 50 }, (_, i) => (i % 2 === 0 ? c.call({ type: "WhoAmI" }) : c.call({ type: "ApiVersions" }))),
    );
    answers.forEach((a, i) => assert.equal(a.type, i % 2 === 0 ? "WhoAmI" : "ApiVersions"));
  } finally {
    c.close();
  }
});

test("a node that requires authentication refuses an anonymous client with the error flag", { skip }, async () => {
  const c = await Connection.open(certs.address, options(anonymousTls));
  try {
    await assert.rejects(c.call({ type: "Metadata", names: [] }), (e: unknown) => {
      assert.ok(e instanceof BrokerError);
      assert.equal(e.reason, null, "an unauthenticated connection is told no more than the code");
      assert.equal(e.code, ErrorCode.UNAUTHORIZED);
      return true;
    });
    assert.ok(c.isOpen, "a refused request does not end the connection");
    // ApiVersions is answered before anyone has authenticated: a client may look before it logs in.
    assert.ok((await c.apiVersions()).versions.length > 10);
  } finally {
    c.close();
  }
});

test("the admin creates a SCRAM user and the user logs in", { skip }, async () => {
  const credential = ScramCredential.forPassword("correct horse");
  await retry("AlterUser to be accepted", node, async () => {
    const admin = await Connection.open(certs.address, options(adminTls));
    try {
      const r = await admin.call(credential.alterUser("bob"), "Admin");
      if (r.error !== ErrorCode.NONE) {
        throw new Error(`AlterUser answered ${r.error} ${r.errorMsg}`);
      }
      return r;
    } finally {
      admin.close();
    }
  });

  const pool = new ConnectionPool(options(anonymousTls, scramCredentials("bob", "correct horse")));
  try {
    const bob = await pool.get(certs.address);
    const who = await bob.call({ type: "WhoAmI" }, "WhoAmI");
    assert.equal(who.name, "bob");
    assert.equal(bob.sessionState(), "ACTIVE");
    assert.equal(await pool.get(certs.address), bob);
  } finally {
    pool.close();
  }
});

test("a wrong password and an unknown user are refused the same way", { skip }, async () => {
  const refuse = (user: string, password: string): Promise<AuthenticationError> =>
    Connection.open(certs.address, options(anonymousTls, scramCredentials(user, password))).then(
      () => assert.fail("logged in"),
      (e: unknown) => {
        assert.ok(e instanceof AuthenticationError);
        return e;
      },
    );
  const wrong = await refuse("bob", "wrong");
  const unknown = await refuse("nobody", "wrong");
  assert.equal(wrong.code, ErrorCode.AUTHENTICATION_FAILED);
  assert.equal(unknown.code, ErrorCode.AUTHENTICATION_FAILED);
  assert.equal(wrong.message, unknown.message, "the node does not say which was wrong");
});

test("a mechanism the node does not offer is named with the ones it does", { skip }, async () => {
  await assert.rejects(
    Connection.open(certs.address, options(anonymousTls, oauthBearerCredentials(() => "token"))),
    (e: unknown) => {
      assert.ok(e instanceof AuthenticationError);
      assert.equal(e.code, ErrorCode.UNSUPPORTED_SASL_MECHANISM);
      assert.ok(e.message.includes("OAUTHBEARER is not enabled"), e.message);
      assert.ok(e.message.includes("SCRAM-SHA-512"), e.message);
      return true;
    },
  );
});

test("the session is renewed on the same connection and ends when it is not", { skip }, async () => {
  const bob = scramCredentials("bob", "correct horse");
  const pool = new ConnectionPool(options(anonymousTls, bob));
  try {
    const c = await pool.get(certs.address);
    await sleep((SESSION_MS * 4) / 5 + 200);
    assert.equal(c.sessionState(), "RENEW_DUE");
    assert.equal(await pool.get(certs.address), c, "renewed, same connection");
    assert.equal(c.sessionState(), "ACTIVE");
    // Past the first session's end, the renewed one carries the connection.
    await sleep(SESSION_MS / 5 + 300);
    assert.equal((await c.call({ type: "WhoAmI" }, "WhoAmI")).name, "bob");
  } finally {
    pool.close();
  }

  // Without renewal the node ends the session and refuses what follows.
  const c = await Connection.open(certs.address, options(anonymousTls, bob));
  try {
    assert.equal((await c.call({ type: "WhoAmI" }, "WhoAmI")).name, "bob");
    await sleep(SESSION_MS + 300);
    assert.equal(c.sessionState(), "ENDED");
    await assert.rejects(c.call({ type: "WhoAmI" }), (e: unknown) => e instanceof BrokerError && e.code === ErrorCode.UNAUTHORIZED);
  } finally {
    c.close();
  }
});

test("a pool replaces a connection that was dropped", { skip }, async () => {
  const pool = new ConnectionPool(options(anonymousTls, scramCredentials("bob", "correct horse")));
  try {
    const first = await pool.get(certs.address);
    first.close(); // as if the node had dropped it
    const second = await pool.get(certs.address);
    assert.notEqual(first, second);
    assert.equal((await second.call({ type: "WhoAmI" }, "WhoAmI")).name, "bob");
  } finally {
    pool.close();
  }
});

test("a connection ends when the node stops", { skip }, async () => {
  const bin = binary();
  const own = await Certs.make(bin);
  const ownNode = NodeProcess.start(bin, own);
  try {
    const admin = await retry("a second node to start", ownNode, async () => {
      const c = await Connection.open(own.address, options(await own.adminTls()));
      await c.call({ type: "Metadata", names: [] }, "Metadata");
      return c;
    });
    await admin.call({ type: "WhoAmI" });
    await ownNode.stop();
    const why = await admin.closed;
    assert.ok(why.requestSent);
    assert.ok(!admin.isOpen);
  } finally {
    await ownNode.stop();
  }
});

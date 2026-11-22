import assert from "node:assert/strict";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  BrokerResponseError,
  ClientAuthenticationError,
  ErrorCode,
  LotseClient,
  SessionMechanism,
  type TlsConfig,
  TokenLifetime,
  tokenCredentials,
} from "../src/index.js";
import { formatAddress } from "../src/net/address.js";
import { binary, Certs, NodeProcess, retry, skipWithoutBinary } from "./support/lotse.js";

/**
 * Access tokens (ADR 0133) against one real node with TLS: the admin, signed in with a certificate, makes a token; a
 * client with no certificate signs in with it as the admin, the node lists the session with the token's id, and once
 * the token is revoked the open session ends and the token signs no one in. Skipped, with the reason, when no `lotse`
 * executable is found (set LOTSE_BIN).
 */

const skip = skipWithoutBinary();

/** Cluster version 0.1, which tokens need, as `major << 16 | minor`. */
const TOKENS_SINCE = 1;

describe("access tokens against a real node", { skip }, () => {
  let certs: Certs;
  let node: NodeProcess;
  let adminTls: TlsConfig;
  let anonymousTls: TlsConfig;
  let admin: LotseClient;
  const extra: LotseClient[] = [];

  const withToken = (text: string): LotseClient => {
    const c = new LotseClient({
      bootstrap: [formatAddress(certs.address)],
      tls: anonymousTls,
      credentials: tokenCredentials(text),
      requestTimeoutMs: 5_000,
      retryForMs: 10_000,
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
    admin = new LotseClient({ bootstrap: [formatAddress(certs.address)], tls: adminTls, requestTimeoutMs: 5_000, retryForMs: 60_000 });
    await retry("the node to start", node, () => admin.metadata());
    // A fresh cluster takes the version its nodes run once each has reported it (ADR 0127).
    await retry("cluster version 0.1", node, async () => {
      const v = (await admin.describeUpgrade()).clusterVersion;
      assert.ok(v !== null && v >= TOKENS_SINCE, `the cluster never took version 0.1: ${v}`);
    });
  });

  after(async () => {
    admin?.close();
    for (const c of extra) {
      c.close();
    }
    await node?.stop();
  });

  const signedOut = (e: unknown): boolean =>
    e instanceof ClientAuthenticationError || (e instanceof BrokerResponseError && e.code === ErrorCode.UNAUTHORIZED);

  test("a token signs in as its owner until it is revoked", async () => {
    const created = await admin.createToken("laptop", null, TokenLifetime.WEEK);
    assert.ok(created.id !== 0n && created.text().startsWith("lotse_tok_"), created.toString());
    const days = Math.floor((Number(created.expiresAtMs) - Date.now()) / 86_400_000);
    assert.equal(days, 6, "7 days from now");

    const viaToken = withToken(created.text());
    assert.equal((await viaToken.whoAmI()).name, "alice");
    const listed = await viaToken.describeTokens();
    assert.equal(listed.length, 1);
    assert.equal(listed[0]?.id, created.id);
    assert.equal(listed[0]?.name, "laptop");
    assert.equal(listed[0]?.owner, "alice");
    // A session signed in with a token makes no tokens.
    await assert.rejects(viaToken.createToken("more", null, TokenLifetime.WEEK), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError, String(e));
      assert.equal(e.code, ErrorCode.UNAUTHORIZED);
      return true;
    });

    // The node lists the session by its token, never the secret.
    let seen = false;
    for (const n of await admin.nodeSessions()) {
      assert.ok(n.sessions !== null, `node ${n.nodeId}: ${String(n.error)}`);
      for (const s of n.sessions.sessions) {
        if (s.mechanism === SessionMechanism.TOKEN) {
          assert.equal(s.tokenId, created.id);
          assert.equal(s.name, "alice");
          seen = true;
        }
      }
    }
    assert.ok(seen, "the token's session is listed");

    // The same id with another secret is not the token.
    const text = created.text();
    const forged = text.slice(0, -1) + (text.endsWith("A") ? "Q" : "A");
    await assert.rejects(withToken(forged).whoAmI(), ClientAuthenticationError);

    // Revoked, it ends the open session and signs no one in.
    await admin.revokeToken(created.id);
    await assert.rejects(admin.revokeToken(created.id), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError, String(e));
      assert.equal(e.code, ErrorCode.UNKNOWN_RESOURCE);
      return true;
    });
    const deadline = Date.now() + 30_000;
    for (let ended = false; !ended; ) {
      assert.ok(Date.now() < deadline, "the revoked token's session goes on");
      try {
        await viaToken.whoAmI();
        await sleep(100);
      } catch (e) {
        assert.ok(signedOut(e), String(e));
        ended = true;
      }
    }
    await assert.rejects(withToken(created.text()).whoAmI(), ClientAuthenticationError);
    assert.deepEqual(await admin.describeTokens(), [], "a revoked token is not listed");
  });
});

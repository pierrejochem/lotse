import assert from "node:assert/strict";
import { X509Certificate } from "node:crypto";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, test } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import {
  BrokerResponseError,
  ErrorCode,
  LotseClient,
  mayRenew,
  newRenewalKey,
  renewalWindowOpensMs,
  type TlsConfig,
  writeRenewed,
} from "../src/index.js";
import { formatAddress } from "../src/net/address.js";
import { binary, Certs, NodeProcess, retry, skipWithoutBinary } from "./support/lotse.js";

/**
 * Renewing a client certificate over a connection made with it (ADR 0123): the window, new keys and writing the files
 * without a node; then, against one real node with a renewal CA, the call writes the new files, a `TlsConfig` read from
 * them presents the new certificate at its next connection (outside its window, so it cannot be renewed again), and a
 * client configured to renew by itself does so once its certificate is in its window.
 */

const DAY = 86_400_000n;

describe("certificate renewal without a node", () => {
  test("the window opens in the last 30 days or after two thirds", () => {
    // A year: two thirds (day 243 and a third) come before the last 30 days.
    assert.equal(renewalWindowOpensMs(0n, 365n * DAY), ((365n * DAY) / 3n) * 2n);
    // 60 days: the last 30 days come first.
    assert.equal(renewalWindowOpensMs(0n, 60n * DAY), 30n * DAY);
    assert.equal(renewalWindowOpensMs(100n, 103n), 0n);
    assert.ok(mayRenew(0n, 60n * DAY, 30n * DAY));
    assert.ok(!mayRenew(0n, 60n * DAY, 30n * DAY - 1n));
    assert.ok(!mayRenew(0n, 60n * DAY, 60n * DAY), "not once it expired");
  });

  test("a new key is a P-256 key with a signing request", () => {
    const k = newRenewalKey();
    assert.match(k.keyPem, /BEGIN PRIVATE KEY/);
    assert.equal(k.csr[0], 0x30);
    assert.ok(k.csr.length > 150 && k.csr.length < 4096, `about 200 bytes: ${k.csr.length}`);
  });

  test("the key is written, then the chain, each whole", () => {
    const dir = mkdtempSync(join(tmpdir(), "lotse-renewal-"));
    const cert = join(dir, "c.pem");
    const key = join(dir, "c.key");
    writeFileSync(cert, "old cert");
    writeFileSync(key, "old key");
    writeRenewed({ keyPem: "new key", chainPem: "new chain", notAfterMs: 1n }, cert, key);
    assert.equal(readFileSync(cert, "utf8"), "new chain");
    assert.equal(readFileSync(key, "utf8"), "new key");
    assert.ok(!existsSync(`${cert}.new`) && !existsSync(`${key}.new`));
  });
});

const skip = skipWithoutBinary();

describe("certificate renewal against a real node", { skip }, () => {
  let certs: Certs;
  let node: NodeProcess;
  const clients: LotseClient[] = [];

  const client = (tls: TlsConfig, more: Partial<ConstructorParameters<typeof LotseClient>[0]> = {}): LotseClient => {
    const c = new LotseClient({ bootstrap: [formatAddress(certs.address)], tls, retryForMs: 60_000, ...more });
    clients.push(c);
    return c;
  };
  const leaf = (file: string): X509Certificate => new X509Certificate(readFileSync(file));
  const blocks = (file: string): number => readFileSync(file, "utf8").split("BEGIN CERTIFICATE").length - 1;

  before(async () => {
    if (skip !== false) {
      return;
    }
    certs = await Certs.make(binary());
    node = NodeProcess.start(binary(), certs);
    const alice = client(await certs.adminTls());
    await retry("the node to start", node, () => alice.metadata());
  });

  after(async () => {
    for (const c of clients) {
      c.close();
    }
    await node?.stop();
  });

  test("a certificate in its window is renewed and the new files are used at the next connection", async () => {
    const files = certs.filesOf("carol");
    const tls = await certs.userTls("carol");
    const before = leaf(files.certPemFile).serialNumber;
    const renewed = await client(tls).renewCertificate(files);
    const now = leaf(files.certPemFile);
    assert.equal(blocks(files.certPemFile), 2, "the new certificate and the renewal CA's");
    assert.notEqual(now.serialNumber, before);
    assert.equal(now.subject, "CN=carol");
    assert.equal(renewed.notAfterMs / 1000n, BigInt(Date.parse(now.validTo)) / 1000n);
    assert.equal((renewed.notAfterMs - BigInt(Date.now())) / DAY, 89n, "90 days from now");
    // The same TlsConfig, on a new client's connections, shows the renewed certificate: 90 days long, it is not in
    // its window, so renewing again is refused.
    const again = client(tls);
    assert.equal((await again.whoAmI()).name, "carol");
    await assert.rejects(again.renewCertificate(), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError, String(e));
      assert.equal(e.code, ErrorCode.INVALID_REQUEST);
      assert.match(e.message, /two thirds/);
      return true;
    });
  });

  test("a client configured to renew by itself renews in the window", async () => {
    const files = certs.filesOf("dave");
    const before = leaf(files.certPemFile).serialNumber;
    const dave = client(await certs.userTls("dave"), { renewCertificate: files, renewCheckMs: 200 });
    const deadline = Date.now() + 60_000;
    while (leaf(files.certPemFile).serialNumber === before) {
      assert.ok(Date.now() < deadline, `never renewed: ${String(dave.renewalFailure)}`);
      await sleep(100);
    }
    assert.equal(blocks(files.certPemFile), 2);
    // Renewed, it is out of its window: the next looks change nothing.
    const renewed = leaf(files.certPemFile).serialNumber;
    await sleep(1_000);
    assert.equal(leaf(files.certPemFile).serialNumber, renewed);
    assert.equal(dave.renewalFailure, null);
  });
});

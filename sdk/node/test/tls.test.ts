import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { address, Connection, ConnectionError, TlsConfig } from "../src/index.js";
import { binary, Certs, skipWithoutBinary } from "./support/lotse.js";
import { TlsTestServer } from "./support/tls-server.js";

/**
 * TLS 1.3 against a Node.js server with certificates made by the real `lotse certs` (a CA, a node certificate with the
 * IP entry 127.0.0.1 and a client certificate for "alice"). Skipped when no `lotse` executable is found.
 */

const skip = skipWithoutBinary();
const options = (tls: TlsConfig) => ({ tls, requestTimeoutMs: 5_000 });
const at = (host: string, server: TlsTestServer) => address(host, server.port);

let certs: Certs;
let otherCa: Certs;
const servers: TlsTestServer[] = [];

async function serve(c: Certs, o: Parameters<typeof TlsTestServer.start>[1] = {}): Promise<TlsTestServer> {
  const s = await TlsTestServer.start(c, o);
  servers.push(s);
  return s;
}

before(async () => {
  if (skip === false) {
    certs = await Certs.make(binary());
    otherCa = await Certs.make(binary());
  }
});

after(async () => {
  await Promise.all(servers.map((s) => s.close()));
});

async function ping(c: Connection): Promise<readonly string[]> {
  return (await c.apiVersions()).features;
}

test("a node with a certificate from the cluster's CA is reached at its IP address", { skip }, async () => {
  const server = await serve(certs);
  const c = await Connection.open(at("127.0.0.1", server), options(await certs.anonymousTls()));
  try {
    assert.ok(c.isSecure);
    assert.deepEqual(await ping(c), ["tls-test"]);
  } finally {
    c.close();
  }
});

test("the name must be one the certificate is valid for", { skip }, async () => {
  // The certificate lists the IP address 127.0.0.1 and the node's URI, but not the name "localhost".
  const server = await serve(certs);
  await assert.rejects(
    Connection.open(at("localhost", server), options(await certs.anonymousTls())),
    (e: unknown) => e instanceof ConnectionError && !e.requestSent,
  );
});

test("a certificate from another CA is refused", { skip }, async () => {
  const server = await serve(certs);
  await assert.rejects(
    Connection.open(at("127.0.0.1", server), options(await otherCa.anonymousTls())),
    (e: unknown) => e instanceof ConnectionError && !e.requestSent && /certificate|issuer|self.signed/i.test(e.message),
  );
});

test("the roots Node.js ships with are not trusted", { skip }, async () => {
  // Only the CA that was given counts: giving another one must not fall back to the default list.
  const server = await serve(certs);
  const onlyOther = TlsConfig.fromPem({ ca: readFileSync(otherCa.ca, "utf8") });
  await assert.rejects(Connection.open(at("127.0.0.1", server), options(onlyOther)), ConnectionError);
});

test("TLS 1.2 is not spoken", { skip }, async () => {
  const server = await serve(certs, { version: "TLSv1.2" });
  await assert.rejects(
    Connection.open(at("127.0.0.1", server), options(await certs.anonymousTls())),
    (e: unknown) => e instanceof ConnectionError && /protocol|version/i.test(e.message),
  );
});

test("a client certificate is presented when the node asks for one", { skip }, async () => {
  const server = await serve(certs, { requireClient: true });
  const c = await Connection.open(at("127.0.0.1", server), options(await certs.adminTls()));
  try {
    assert.deepEqual(await ping(c), ["tls-test"]);
  } finally {
    c.close();
  }
  assert.deepEqual(server.clientSubjects, ["CN=alice"]);
});

test("without a client certificate a node that requires one refuses", { skip }, async () => {
  const server = await serve(certs, { requireClient: true });
  // In TLS 1.3 the client finishes its handshake before the server has checked the client's certificate, so the
  // refusal can arrive with the first request instead of the handshake.
  await assert.rejects(async () => {
    const c = await Connection.open(at("127.0.0.1", server), options(await certs.anonymousTls()));
    try {
      await ping(c);
    } finally {
      c.close();
    }
  }, ConnectionError);
});

test("a client certificate from another CA is refused", { skip }, async () => {
  const server = await serve(certs, { requireClient: true });
  const wrong = await TlsConfig.fromFiles(certs.ca, otherCa.clientCert, otherCa.clientKey);
  await assert.rejects(async () => {
    const c = await Connection.open(at("127.0.0.1", server), options(wrong));
    try {
      await ping(c);
    } finally {
      c.close();
    }
  }, ConnectionError);
});

test("a TLS config needs a CA, and a client identity both its parts", () => {
  assert.throws(() => TlsConfig.fromPem({ ca: "" }), RangeError);
  assert.throws(() => TlsConfig.fromPem({ ca: "nothing" }), RangeError);
  assert.throws(() => TlsConfig.fromPem({ ca: "-----BEGIN CERTIFICATE-----", cert: "x" }), RangeError);
});

test("requests over TLS go through the same framing", { skip }, async () => {
  const server = await serve(certs);
  const c = await Connection.open(at("127.0.0.1", server), options(await certs.anonymousTls()));
  try {
    for (let i = 0; i < 20; i++) {
      assert.equal((await c.call({ type: "ApiVersions" })).type, "ApiVersions");
    }
  } finally {
    c.close();
  }
});

// ---- ADR 0068: a connection ends when the certificate its other end showed expires ----

function stamp(ms: number): string {
  return new Date(ms).toISOString().replace(/[-:T]/g, "").replace(/\.\d+Z$/, "Z");
}

function openssl(...args: string[]): boolean {
  try {
    execFileSync("openssl", args, { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

test("a connection ends when the node's certificate expires", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "lotse-sdk-node-expiry-"));
  const cert = join(dir, "node.pem");
  const key = join(dir, "node.key");
  const lifetimeMs = 3_000;
  const made = openssl(
    "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
    "-keyout", key, "-out", cert, "-subj", "/CN=expiring", "-addext", "subjectAltName=IP:127.0.0.1",
    "-not_before", stamp(Date.now() - 3_600_000), "-not_after", stamp(Date.now() + lifetimeMs),
  );
  if (!made) {
    t.skip("SKIPPED: openssl 3.4 or later is needed to make a certificate that expires in seconds");
    return;
  }
  const server = await TlsTestServer.start({ ca: cert, nodeCert: cert, nodeKey: key });
  try {
    const c = await Connection.open(at("127.0.0.1", server), options(await TlsConfig.fromFiles(cert)));
    assert.deepEqual(await ping(c), ["tls-test"]);
    const start = Date.now();
    const why = await Promise.race([
      c.closed,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("the connection did not end")), lifetimeMs + 5_000).unref()),
    ]).finally(() => c.close());
    assert.ok(why.message.includes("certificate expired"), why.message);
    assert.ok(Date.now() - start < lifetimeMs + 2_000, "ended at the deadline, not much later");
    assert.ok(!c.isOpen);
    await assert.rejects(c.call({ type: "ApiVersions" }), (e: unknown) => e instanceof ConnectionError && !e.requestSent);
  } finally {
    await server.close();
  }
});

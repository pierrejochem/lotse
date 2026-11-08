import assert from "node:assert/strict";
import { test } from "node:test";
import { MIN_ITERATIONS, ScramClient, ScramCredential, ScramError, type ScramErrorKind } from "../src/auth/index.js";
import { field, hex, lines, toHex } from "./vectors.js";
import { TestScramServer, Failed } from "./support/scram-server.js";

const SALT = Buffer.from("0123456789abcdef", "ascii");
const text = (b: Uint8Array): string => Buffer.from(b).toString("utf8");
const bytes = (s: string): Uint8Array => Buffer.from(s, "utf8");

function serverFirst(nonce: string, iterations: number | string): Uint8Array {
  return bytes(`r=${nonce},s=${SALT.toString("base64")},i=${iterations}`);
}

function afterFirst(nonce: string): ScramClient {
  const c = new ScramClient("alice", "pw", nonce);
  c.first();
  return c;
}

async function rejectsWith(p: Promise<unknown>, kind: ScramErrorKind, what: string): Promise<void> {
  await assert.rejects(p, (e: unknown) => e instanceof ScramError && e.kind === kind, what);
}

test("every message of the broker's exchange is reproduced byte for byte", async () => {
  const cases = lines("scram.txt");
  assert.equal(cases.length, 5);
  for (const c of cases) {
    const label = field(c, 0);
    const user = text(hex(field(c, 1)));
    const pw = field(c, 2);
    const password = pw.length === 1 ? "" : text(hex(pw.slice(1)));
    const salt = hex(field(c, 3));
    const iterations = Number(field(c, 4));
    const serverNonce = field(c, 6);

    const client = new ScramClient(user, password, field(c, 5));
    assert.equal(toHex(client.first()), field(c, 7), `${label}: client first`);
    const clientFinal = await client.respond(hex(field(c, 8)));
    assert.equal(toHex(clientFinal), field(c, 9), `${label}: client final (the proof)`);
    client.verify(hex(field(c, 10)));

    const credential = ScramCredential.of(password, salt, iterations);
    assert.equal(toHex(credential.storedKey), field(c, 11), `${label}: StoredKey`);
    assert.equal(toHex(credential.serverKey), field(c, 12), `${label}: ServerKey`);

    // The test server, written from the RFC, agrees with the broker's bytes too.
    const server = new TestScramServer(() => credential, serverNonce);
    assert.equal(toHex(server.first(hex(field(c, 7)))), field(c, 8), `${label}: server first`);
    assert.equal(toHex(server.last(clientFinal).serverFinal), field(c, 10), `${label}: server final`);
  }
});

test("the user name is escaped", () => {
  assert.equal(text(new ScramClient("a,b=c", "pw", "x").first()), "n,,n=a=2Cb=3Dc,r=x");
});

test("the server nonce must extend the client's strictly", async () => {
  // equal: a server that adds nothing is not contributing a nonce
  await rejectsWith(afterFirst("abc").respond(serverFirst("abc", 4096)), "MALFORMED", "equal");
  // does not start with the client's nonce
  await rejectsWith(afterFirst("abc").respond(serverFirst("xbcdef", 4096)), "MALFORMED", "other prefix");
  // shorter
  await rejectsWith(afterFirst("abc").respond(serverFirst("ab", 4096)), "MALFORMED", "shorter");
  // a nonce with a character outside printable ASCII
  await rejectsWith(afterFirst("abc").respond(serverFirst("abc d", 4096)), "MALFORMED", "space");
  // accepted
  await afterFirst("abc").respond(serverFirst("abcd", 4096));
});

test("the iteration count is bounded", async () => {
  for (const bad of [4095, 0, (1 << 20) + 1, 2147483647, "99999999999", "-4096"]) {
    await rejectsWith(afterFirst("abc").respond(serverFirst("abcd", bad)), "MALFORMED", `i=${bad}`);
  }
  await afterFirst("abc").respond(serverFirst("abcd", 4096));
  // The largest count is accepted, and costs a second or two of PBKDF2.
  await afterFirst("abc").respond(serverFirst("abcd", 1 << 20));
});

test("a malformed server first message is refused", async () => {
  for (const bad of ["", "r=abcd", "r=abcd,s=AAAA", "s=AAAA,r=abcd,i=4096", "r=abcd,s=!!!!,i=4096", "r=abcd,s=AAAA,i=x", "r=abcd,s=AAA,i=4096"]) {
    await rejectsWith(afterFirst("abc").respond(bytes(bad)), "MALFORMED", bad);
  }
  await rejectsWith(afterFirst("abc").respond(new Uint8Array([0xff, 0xfe])), "MALFORMED", "not UTF-8");
});

test("the server's signature is checked", async () => {
  const credential = ScramCredential.of("pw", SALT, 4096);
  for (const mode of ["good", "wrong signature", "no v=", "not base64", "empty", "error message"]) {
    const client = new ScramClient("alice", "pw", "cn");
    const server = new TestScramServer(() => credential, "sn");
    const first = server.first(client.first());
    const last = await client.respond(first);
    const goodFinal = Buffer.from(server.last(last).serverFinal);
    let given: Uint8Array;
    switch (mode) {
      case "good":
        given = goodFinal;
        break;
      case "wrong signature": {
        given = Buffer.from(goodFinal);
        (given as Buffer)[10] = goodFinal[10] === 0x41 ? 0x42 : 0x41;
        break;
      }
      case "no v=":
        given = bytes(text(goodFinal).slice(2));
        break;
      case "not base64":
        given = bytes("v=%%%%");
        break;
      case "empty":
        given = new Uint8Array(0);
        break;
      default:
        given = bytes("e=invalid-proof");
    }
    if (mode === "good") {
      client.verify(given);
    } else {
      assert.throws(
        () => client.verify(given),
        (e: unknown) => e instanceof ScramError && e.kind === "SERVER_NOT_VERIFIED",
        mode,
      );
    }
  }
});

test("a server that does not know the credential cannot prove itself", async () => {
  const client = new ScramClient("alice", "pw", "cn");
  const server = new TestScramServer(() => ScramCredential.of("another password", SALT, 4096), "sn");
  const last = await client.respond(server.first(client.first()));
  assert.throws(() => server.last(last), Failed, "the proof is wrong");
  // A server that skips the check and answers anything is caught by the client.
  assert.throws(() => client.verify(bytes(`v=${Buffer.alloc(64).toString("base64")}`)), ScramError);
});

test("the steps must come in order", async () => {
  const c = new ScramClient("alice", "pw", "cn");
  await rejectsWith(c.respond(serverFirst("cnx", 4096)), "OUT_OF_ORDER", "respond before first");
  assert.throws(() => new ScramClient("alice", "pw", "cn").verify(bytes("v=")), (e: unknown) => e instanceof ScramError && e.kind === "OUT_OF_ORDER");
  const d = afterFirst("cn");
  assert.throws(() => d.verify(bytes("v=")), (e: unknown) => e instanceof ScramError && e.kind === "OUT_OF_ORDER", "verify before respond");
  await d.respond(serverFirst("cnx", 4096));
  await rejectsWith(d.respond(serverFirst("cnx", 4096)), "OUT_OF_ORDER", "respond twice");
});

test("a credential is made with a fresh salt, and AlterUser carries no password", () => {
  const a = ScramCredential.forPassword("pw");
  const b = ScramCredential.forPassword("pw");
  assert.equal(a.salt.length, 16);
  assert.equal(a.iterations, MIN_ITERATIONS);
  assert.notDeepEqual(a.salt, b.salt);
  const req = a.alterUser("bob");
  assert.equal(req.type, "AlterUser");
  assert.equal(req.user, "bob");
  assert.deepEqual(req.credential, { iterations: 4096, salt: a.salt, storedKey: a.storedKey, serverKey: a.serverKey });
  assert.equal(req.credential?.storedKey.length, 64);
  assert.equal(req.credential?.serverKey.length, 64);
  assert.equal(JSON.stringify(req).includes("pw"), false);
  assert.deepEqual(ScramCredential.removal("bob"), { type: "AlterUser", user: "bob", credential: null });
  assert.throws(() => ScramCredential.of("pw", SALT, 4095), RangeError);
  assert.throws(() => ScramCredential.of("pw", SALT, (1 << 20) + 1), RangeError);
});

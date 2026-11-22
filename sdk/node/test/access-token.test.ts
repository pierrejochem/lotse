import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { afterEach, beforeEach, describe, test } from "node:test";
import { inspect } from "node:util";
import {
  AccessToken,
  AuthenticationError,
  authenticate,
  BrokerResponseError,
  type CreatedToken,
  ErrorCode,
  type ErrorCodeValue,
  type LeaderHint,
  LotseClient,
  mechanismOf,
  type Request,
  type Response,
  type TokenEntry,
  TokenLifetime,
  tokenCredentials,
  tokenLifetimeDays,
  decodeRequest,
  encodeRequest,
} from "../src/index.js";
import { closeAll, type FakeNode, startNodes } from "./support/fake-node.js";
import * as fx from "./support/fx.js";
import { toHex } from "./vectors.js";

/** An access token's text and hash (ADR 0133), checked against texts and hashes computed outside this code. */

/** The secret of `lotse-auth`'s own token tests: byte `i` is `i * 37 + 11`. */
const sampleSecret = (): Uint8Array => Uint8Array.from({ length: 32 }, (_, i) => (i * 37 + 11) & 0xff);
const SAMPLE_ID = 0x0123_4567_89ab_cdefn;
// Written by Python's base64.urlsafe_b64encode and hashlib.sha256 for the sample secret, not by this code.
const SAMPLE_TEXT = "lotse_tok_0123456789abcdef_CzBVep_E6Q4zWH2ix-wRNluApcrvFDleg6jN8hc8YYY";
const SAMPLE_HASH = "83b7a8ed859053c81d818870fab1f8b1ae44d06a98a9665d369a8fd7d2838ded";
const SECRET_CHARS = SAMPLE_TEXT.slice(-43);

const sample = (): AccessToken => new AccessToken(SAMPLE_ID, sampleSecret());

test("the token text round-trips and trims", () => {
  const t = sample();
  assert.equal(t.text(), SAMPLE_TEXT);
  assert.ok(AccessToken.parse(SAMPLE_TEXT)?.equals(t));
  // A pasted token often carries a newline; Rust's trim_ascii takes space, tab, LF, FF and CR.
  assert.ok(AccessToken.parse(`  ${SAMPLE_TEXT}\n`)?.equals(t));
  assert.ok(AccessToken.parse(`\t\r\n\f${SAMPLE_TEXT} \r\n`)?.equals(t));
  // Not whitespace to the node: a vertical tab or a no-break space stays and spoils the text.
  assert.equal(AccessToken.parse(`\u000b${SAMPLE_TEXT}`), null);
  assert.equal(AccessToken.parse(` ${SAMPLE_TEXT}`), null);
  assert.equal(AccessToken.parse(`${SAMPLE_TEXT}\u000b`), null);
  assert.equal(AccessToken.parse(`${SAMPLE_TEXT} x`), null);
  assert.deepEqual(AccessToken.parse(SAMPLE_TEXT)?.secret(), sampleSecret());
  assert.equal(AccessToken.parse(SAMPLE_TEXT)?.id, SAMPLE_ID);
});

test("the highest id and any random secret round-trip", () => {
  for (const id of [0n, 1n, 0x2an, (1n << 63n) - 1n, (1n << 64n) - 1n]) {
    const secret = Uint8Array.from({ length: 32 }, () => Math.floor(Math.random() * 256));
    const t = new AccessToken(id, secret);
    assert.equal(t.text().length, 10 + 16 + 1 + 43, t.text());
    assert.ok(AccessToken.parse(t.text())?.equals(t), t.text());
  }
  // Ids from the wire are signed bit patterns: the top half of the range reads back as negative, and its text is the same.
  const top = new AccessToken(-2n, sampleSecret());
  assert.ok(top.text().startsWith("lotse_tok_fffffffffffffffe_"), top.text());
  assert.equal(AccessToken.parse(top.text())?.id, -2n);
  assert.ok(new AccessToken((1n << 64n) - 2n, sampleSecret()).equals(top));
  assert.equal(top.toString(), "AccessToken[id=fffffffffffffffe]");
  assert.ok(new AccessToken((1n << 64n) - 1n, new Uint8Array(32)).text().startsWith("lotse_tok_ffffffffffffffff_"));
  assert.throws(() => new AccessToken(1n << 64n, new Uint8Array(32)), RangeError);
  assert.throws(() => new AccessToken(-(1n << 63n) - 1n, new Uint8Array(32)), RangeError);
});

test("malformed texts do not parse", () => {
  const secret = SECRET_CHARS;
  const bad = [
    SAMPLE_TEXT.replace("lotse_tok_", "lotse_key_"),
    `lotse_tok_0123456789abcde_${secret}`,
    `lotse_tok_0123456789abcdef_${secret.slice(0, 42)}`,
    `lotse_tok_0123456789abcdef_${secret.slice(0, 42)}!`,
    // 'Z' ends in non-zero spare bits: not the canonical text of any secret.
    `lotse_tok_0123456789abcdef_${secret.slice(0, 42)}Z`,
    `lotse_tok_0123456789abcdef_${secret.slice(0, 42)}=`,
    `lotse_tok_0123456789ABCDEF_${secret}`,
    `lotse_tok_+123456789abcdef_${secret}`,
    `lotse_tok_0123456789abcdef_${secret}x`,
    `lotse_tok_0123456789abcdef${secret}`,
    `LOTSE_TOK_0123456789abcdef_${secret}`,
    "",
  ];
  for (const b of bad) {
    assert.equal(AccessToken.parse(b), null, b);
  }
});

test("the hash is the SHA-256 of the secret bytes", () => {
  const t = sample();
  assert.equal(Buffer.from(t.hash()).toString("hex"), SAMPLE_HASH);
  assert.equal(Buffer.from(t.hash()).toString("hex"), createHash("sha256").update(sampleSecret()).digest("hex"));
  assert.notDeepEqual(t.secret(), t.hash());
  assert.throws(() => new AccessToken(1, new Uint8Array(31)), RangeError);
  // The token holds a copy: changing the caller's array or the returned one changes nothing.
  const s = sampleSecret();
  const u = new AccessToken(1, s);
  s.fill(0);
  u.secret().fill(0);
  assert.equal(u.text().slice(-43), SECRET_CHARS);
});

test("the secret is in no string form, JSON or inspect output", () => {
  const forms = (v: unknown): string[] => [String(v), JSON.stringify(v), inspect(v, { depth: 5, showHidden: true }), `${String(v)}`];
  for (const form of forms(sample())) {
    assert.ok(!form.includes(SECRET_CHARS) && !form.includes("CzBVep"), form);
  }
  assert.ok(sample().toString().includes("0123456789abcdef"));
  const credentials = tokenCredentials(SAMPLE_TEXT);
  assert.equal(credentials.text, SAMPLE_TEXT, "the library can still read it");
  for (const form of [...forms(credentials), JSON.stringify({ credentials }), inspect({ credentials }, { showHidden: true })]) {
    assert.ok(!form.includes(SECRET_CHARS) && !form.includes("CzBVep"), form);
  }
  assert.deepEqual(Object.keys(credentials), ["type"]);
});

test("lifetimes are the five steps", () => {
  const days = [7, 30, 90, 180, 365];
  const all = [TokenLifetime.WEEK, TokenLifetime.MONTH, TokenLifetime.QUARTER, TokenLifetime.HALF_YEAR, TokenLifetime.YEAR];
  all.forEach((l, i) => {
    assert.equal(l, i + 1);
    assert.equal(tokenLifetimeDays(l), days[i]);
  });
  for (const bad of [0, 6, -1, 1.5, Number.NaN]) {
    assert.throws(() => tokenLifetimeDays(bad), RangeError, String(bad));
  }
});

test("LOTSE-TOKEN is one round whose message is the token's text", async () => {
  const sent: Request[] = [];
  const lifetime = await authenticate(async (r): Promise<Response> => {
    sent.push(r);
    if (r.type === "SaslHandshake") {
      return { type: "SaslHandshake", error: ErrorCode.NONE, mechanisms: ["LOTSE-TOKEN"] };
    }
    return { type: "SaslAuthenticate", error: ErrorCode.NONE, errorMsg: null, authBytes: new Uint8Array(0), sessionLifetimeMs: 60_000n };
  }, tokenCredentials(SAMPLE_TEXT));
  assert.equal(lifetime, 60_000n);
  assert.equal(mechanismOf(tokenCredentials(SAMPLE_TEXT)), "LOTSE-TOKEN");
  assert.equal(sent.length, 2);
  assert.deepEqual(sent[0], { type: "SaslHandshake", mechanism: "LOTSE-TOKEN" });
  const auth = sent[1];
  assert.ok(auth?.type === "SaslAuthenticate");
  assert.equal(Buffer.from(auth.authBytes).toString("utf8"), SAMPLE_TEXT);
});

test("a refused token fails the login", async () => {
  await assert.rejects(
    authenticate(
      async (r): Promise<Response> =>
        r.type === "SaslHandshake"
          ? { type: "SaslHandshake", error: ErrorCode.NONE, mechanisms: ["LOTSE-TOKEN"] }
          : {
              type: "SaslAuthenticate",
              error: ErrorCode.AUTHENTICATION_FAILED,
              errorMsg: "authentication failed",
              authBytes: new Uint8Array(0),
              sessionLifetimeMs: 0n,
            },
      tokenCredentials(SAMPLE_TEXT),
    ),
    (e: unknown) => e instanceof AuthenticationError && e.message === "authentication failed",
  );
});

const HINT: LeaderHint = { leader: 2, term: 1n };

describe("token calls against scripted brokers", () => {
  let a: FakeNode;
  let b: FakeNode;
  let all: FakeNode[];
  let client: LotseClient;

  /** Node 1 answers metadata and points every token call to node 2, which answers with `handler`. */
  function serve(handler: (r: Request) => Response): void {
    const metadata = (): Response => fx.metadata(all, fx.stream("s", 7n, fx.partition(0, 2, 1, 2)));
    const error: ErrorCodeValue = ErrorCode.NOT_LEADER;
    a.on((_n, r) => {
      switch (r.type) {
        case "Metadata":
          return metadata();
        case "CreateToken":
          return { type: "CreateToken", error, errorMsg: null, id: 0n, expiresAtMs: 0n, leaderHint: HINT };
        case "DescribeTokens":
          return { type: "DescribeTokens", error, leaderHint: HINT, tokens: [] };
        default:
          return { type: "Admin", error, errorMsg: null, leaderHint: HINT };
      }
    });
    b.on((_n, r) => (r.type === "Metadata" ? metadata() : handler(r)));
    client = new LotseClient(fx.config(all, { retryForMs: 1_000 }));
  }

  const ok = (): Response => ({ type: "Admin", error: ErrorCode.NONE, errorMsg: null, leaderHint: null });

  beforeEach(async () => {
    all = await startNodes(2);
    [a, b] = all as [FakeNode, FakeNode];
  });
  afterEach(async () => {
    client?.close();
    await closeAll(all);
  });

  function contains(hay: Uint8Array, needle: Uint8Array): boolean {
    return Buffer.from(hay).includes(needle);
  }

  test("createToken sends only the hash", async () => {
    serve(() => ({ type: "CreateToken", error: ErrorCode.NONE, errorMsg: null, id: 0x2an, expiresAtMs: 5_000n, leaderHint: null }));
    const created: CreatedToken = await client.createToken("laptop", null, TokenLifetime.MONTH);
    assert.equal(created.id, 0x2an);
    assert.equal(created.expiresAtMs, 5_000n);
    const token = AccessToken.parse(created.text());
    assert.ok(token !== null);
    assert.equal(token.id, 0x2an, "the id is the cluster's");

    const sent = b.requests("CreateToken");
    assert.equal(sent.length, 1, "the leader, after node 1 pointed to it");
    assert.equal(a.requests("CreateToken").length, 1, "node 1 got the same request first");
    for (const req of [...a.requests("CreateToken"), ...sent]) {
      assert.equal(req.name, "laptop");
      assert.equal(req.owner, "", "no owner: the caller");
      assert.equal(req.lifetime, TokenLifetime.MONTH);
      assert.equal(req.secretHash.length, 32);
      assert.equal(Buffer.from(req.secretHash).toString("hex"), Buffer.from(token.hash()).toString("hex"), "the hash of the secret");
      const frame = encodeRequest(1, req);
      assert.ok(!contains(frame, token.secret()), "the secret is in the frame");
      assert.ok(!contains(frame, Buffer.from(created.text(), "utf8")), "the text is in the frame");
      assert.ok(!contains(frame, Buffer.from(SECRET_CHARS, "utf8")));
      assert.equal(toHex(encodeRequest(1, decodeRequest(frame).request)), toHex(frame));
    }
    for (const form of [String(created), JSON.stringify(created), inspect(created, { showHidden: true })]) {
      assert.ok(!form.includes(created.text()) && !form.includes(created.text().slice(-43)), form);
    }

    await client.createToken("ci", "bob", TokenLifetime.YEAR);
    const forBob = b.requests("CreateToken")[1];
    assert.equal(forBob?.owner, "bob");
    assert.equal(forBob?.lifetime, 5);
    assert.notEqual(Buffer.from(forBob?.secretHash ?? []).toString("hex"), Buffer.from(token.hash()).toString("hex"), "a new secret each");
  });

  test("a refused token says why", async () => {
    const why = "tokens need cluster version 0.1; raise it with `lotse upgrade finalize`";
    serve(() => ({ type: "CreateToken", error: ErrorCode.UNSUPPORTED_VERSION, errorMsg: why, id: 0n, expiresAtMs: 0n, leaderHint: null }));
    await assert.rejects(client.createToken("early", null, TokenLifetime.WEEK), (e: unknown) => {
      assert.ok(e instanceof BrokerResponseError);
      assert.equal(e.code, ErrorCode.UNSUPPORTED_VERSION);
      assert.ok(e.message.includes(why), e.message);
      return true;
    });
    await assert.rejects(client.createToken("bad", null, 9), RangeError);
  });

  test("tokens are listed and revoked on the metadata leader", async () => {
    const tokens: TokenEntry[] = [{ id: 41n, name: "ci", owner: "alice", createdAtMs: 1n, expiresAtMs: 2n, createdBy: "admin" }];
    serve((r) => (r.type === "DescribeTokens" ? { type: "DescribeTokens", error: ErrorCode.NONE, leaderHint: null, tokens } : ok()));
    assert.deepEqual(await client.describeTokens("alice"), tokens);
    assert.equal(b.requests("DescribeTokens")[0]?.owner, "alice");
    assert.deepEqual(await client.describeTokens(), tokens);
    assert.equal(b.requests("DescribeTokens")[1]?.owner, null);
    await client.revokeToken(41n);
    await client.revokeToken(42);
    assert.deepEqual(b.requests("RevokeToken"), [
      { type: "RevokeToken", id: 41n },
      { type: "RevokeToken", id: 42n },
    ]);
  });
});

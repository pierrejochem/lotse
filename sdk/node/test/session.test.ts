import assert from "node:assert/strict";
import { test } from "node:test";
import {
  address,
  AuthenticationError,
  Connection,
  ConnectionPool,
  ErrorCode,
  oauthBearerCredentials,
  scramCredentials,
  type ConnectionOptions,
  type Credentials,
} from "../src/index.js";
import { type Handler, type Peer, withBroker } from "./support/fake-broker.js";
import { FakeSasl } from "./support/fake-sasl.js";

/** Logging in, renewing a session at four fifths of its lifetime, and replacing the connection when it ends. */

const SECOND = 1_000_000_000n;

class Clock {
  constructor(public now: bigint) {}
  readonly ticker = (): bigint => this.now;
}

function serving(sasl: FakeSasl): Handler {
  return (peer, req) => {
    if (!sasl.handle(peer, req)) {
      peer.reply(req.header.correlationId, { type: "Outcome", error: ErrorCode.NONE, leaderHint: null });
    }
  };
}

function options(clock: Clock, credentials: Credentials): ConnectionOptions {
  return { plaintextOnLoopback: true, credentials, ticker: clock.ticker, requestTimeoutMs: 5_000 };
}

test("a session is renewed on the same connection at four fifths and replaced at the end", async () => {
  const clock = new Clock(1_000_000n * SECOND); // an arbitrary origin
  const origin = clock.now;
  const sasl = new FakeSasl(10_000n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const first = await pool.get(broker.address);
      assert.equal(sasl.handshakes, 1);
      assert.equal(first.sessionState(), "ACTIVE");

      clock.now = origin + 8n * SECOND - 1n;
      assert.equal(await pool.get(broker.address), first, "not yet");
      assert.equal(first.sessionState(), "ACTIVE");
      assert.equal(sasl.handshakes, 1);

      clock.now = origin + 8n * SECOND;
      assert.equal(first.sessionState(), "RENEW_DUE");
      assert.equal(await pool.get(broker.address), first, "renewed on the same connection");
      assert.equal(sasl.handshakes, 2, "handshake and authenticate again");
      assert.equal(sasl.finals, 2);
      assert.equal(broker.accepted, 1);
      assert.equal(first.sessionState(), "ACTIVE", "a new session of 10 s from 8 s");

      clock.now = origin + 18n * SECOND - 1n;
      assert.notEqual(first.sessionState(), "ENDED");
      clock.now = origin + 18n * SECOND;
      assert.equal(first.sessionState(), "ENDED");
      const second = await pool.get(broker.address);
      assert.notEqual(second, first, "an ended session is not renewed: the node refuses it");
      assert.equal(broker.accepted, 2);
      assert.ok(!first.isOpen);
      assert.equal(sasl.handshakes, 3);
      assert.equal(second.sessionState(), "ACTIVE");
    } finally {
      pool.close();
    }
  });
});

test("the lifetime comes from the last response", async () => {
  const clock = new Clock(5n * SECOND);
  const origin = clock.now;
  const sasl = new FakeSasl(10_000n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const c = await pool.get(broker.address);
      sasl.lifetimeMs = 4_000n; // the node grants less on renewal
      clock.now = origin + 8n * SECOND;
      assert.equal(await pool.get(broker.address), c);
      clock.now = origin + 8n * SECOND + 3_199_999_999n;
      assert.equal(c.sessionState(), "ACTIVE", "renewal at 3.2 s of 4 s");
      clock.now = origin + 8n * SECOND + 3_200_000_000n;
      assert.equal(c.sessionState(), "RENEW_DUE");
      clock.now = origin + 8n * SECOND + 4n * SECOND;
      assert.equal(c.sessionState(), "ENDED");
    } finally {
      pool.close();
    }
  });
});

test("the lifetime counts from before the handshake", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(10_000n);
  // The log in takes a second: the node's clock started when it received the first message.
  sasl.onHandshake = () => {
    clock.now += SECOND;
  };
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const c = await pool.get(broker.address);
      assert.equal(clock.now, SECOND);
      clock.now = 8n * SECOND - 1n;
      assert.equal(c.sessionState(), "ACTIVE");
      clock.now = 8n * SECOND;
      assert.equal(c.sessionState(), "RENEW_DUE", "4/5 of 10 s from time 0, not from time 1 s");
      clock.now = 10n * SECOND - 1n;
      assert.equal(c.sessionState(), "RENEW_DUE");
      clock.now = 10n * SECOND;
      assert.equal(c.sessionState(), "ENDED");
    } finally {
      pool.close();
    }
  });
});

test("a lifetime with the top bit set is clamped, not taken as negative", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(-1n); // u64::MAX as its signed bit pattern
  await withBroker(serving(sasl), async (broker) => {
    const c = await Connection.open(broker.address, options(clock, scramCredentials("alice", "pw")));
    try {
      assert.equal(c.sessionState(), "ACTIVE");
      clock.now = 3600n * SECOND * 24n * 365n;
      assert.notEqual(c.sessionState(), "ENDED", "a year in, the session still stands");
    } finally {
      c.close();
    }
  });
});

test("a session without a lifetime lasts the connection", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(0n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const c = await pool.get(broker.address);
      assert.equal(c.sessionState(), "UNLIMITED");
      clock.now = 1_000_000n * SECOND;
      assert.equal(await pool.get(broker.address), c);
      assert.equal(sasl.handshakes, 1);
    } finally {
      pool.close();
    }
  });
});

test("OAUTHBEARER asks for the token on every authentication", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(10_000n);
  let issued = 0;
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, oauthBearerCredentials(async () => `token-${++issued}`)));
    try {
      const c = await pool.get(broker.address);
      assert.deepEqual(sasl.tokens, ["token-1"]);
      clock.now = 8n * SECOND;
      assert.equal(await pool.get(broker.address), c);
      assert.deepEqual(sasl.tokens, ["token-1", "token-2"], "a fresh token to renew");
    } finally {
      pool.close();
    }
  });
});

test("a wrong password fails the login and leaves nothing in the pool", async () => {
  const sasl = new FakeSasl(10_000n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(new Clock(0n), scramCredentials("alice", "wrong")));
    try {
      const refused = (e: unknown): boolean =>
        e instanceof AuthenticationError && e.code === ErrorCode.AUTHENTICATION_FAILED && e.message === "authentication failed";
      await assert.rejects(pool.get(broker.address), refused);
      await assert.rejects(pool.get(broker.address), refused);
      assert.equal(broker.accepted, 2, "each try is a new connection; the failed one was closed");
    } finally {
      pool.close();
    }
  });
});

test("a mechanism the node does not offer names the ones it does", async () => {
  const sasl = new FakeSasl(10_000n);
  sasl.mechanisms = ["OAUTHBEARER"];
  await withBroker(serving(sasl), async (broker) => {
    await assert.rejects(Connection.open(broker.address, options(new Clock(0n), scramCredentials("alice", "pw"))), (e: unknown) => {
      assert.ok(e instanceof AuthenticationError);
      assert.equal(e.code, ErrorCode.UNSUPPORTED_SASL_MECHANISM);
      assert.ok(e.message.includes("SCRAM-SHA-512 is not enabled"), e.message);
      assert.ok(e.message.includes("OAUTHBEARER"), e.message);
      return true;
    });
  });
});

test("a server that cannot prove itself is refused", async () => {
  // A broker that accepts any proof and answers with a made up signature.
  await withBroker(
    (peer, req) => {
      const id = req.header.correlationId;
      if (req.request.type === "SaslHandshake") {
        peer.reply(id, { type: "SaslHandshake", error: ErrorCode.NONE, mechanisms: ["SCRAM-SHA-512"] });
        return;
      }
      const first = Buffer.from((req.request as { authBytes: Uint8Array }).authBytes).toString("utf8");
      let out: string;
      if (first.startsWith("n,,")) {
        const nonce = first.slice(first.indexOf("r=") + 2);
        out = `r=${nonce}xyz,s=MDEyMzQ1Njc4OWFiY2RlZg==,i=4096`;
      } else {
        out = `v=${Buffer.alloc(64).toString("base64")}`;
      }
      peer.reply(id, { type: "SaslAuthenticate", error: ErrorCode.NONE, errorMsg: null, authBytes: Buffer.from(out), sessionLifetimeMs: 10_000n });
    },
    async (broker) => {
      await assert.rejects(
        Connection.open(broker.address, options(new Clock(0n), scramCredentials("alice", "pw"))),
        (e: unknown) => e instanceof AuthenticationError && e.message.includes("could not prove"),
      );
    },
  );
});

test("a connection that breaks during renewal is replaced with a fresh login", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(10_000n);
  let breakNext = false;
  sasl.onHandshake = (n) => {
    if (breakNext && n === 2) {
      throw new Error("closes the connection"); // the fake broker closes the peer
    }
  };
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const first = await pool.get(broker.address);
      breakNext = true;
      clock.now = 8n * SECOND;
      const second = await pool.get(broker.address);
      assert.notEqual(second, first);
      assert.equal(broker.accepted, 2);
      assert.ok(!first.isOpen);
      assert.equal(second.sessionState(), "ACTIVE");
    } finally {
      pool.close();
    }
  });
});

test("a refused renewal is reported and the connection dropped", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(10_000n);
  await withBroker(
    (peer: Peer, req) => {
      // On renewal the node has forgotten the user.
      if (req.request.type === "SaslHandshake" && sasl.handshakes >= 1) {
        sasl.handshakes++;
        peer.reply(req.header.correlationId, { type: "SaslHandshake", error: ErrorCode.UNSUPPORTED_SASL_MECHANISM, mechanisms: [] });
        return;
      }
      serving(sasl)(peer, req);
    },
    async (broker) => {
      const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
      try {
        const first = await pool.get(broker.address);
        clock.now = 8n * SECOND;
        await assert.rejects(pool.get(broker.address), AuthenticationError);
        assert.ok(!first.isOpen);
      } finally {
        pool.close();
      }
    },
  );
});

test("callers that ask while a connection is opening share the attempt", async () => {
  const sasl = new FakeSasl(10_000n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(new Clock(0n), scramCredentials("alice", "pw")));
    try {
      const all = await Promise.all([1, 2, 3, 4].map(() => pool.get(broker.address)));
      assert.ok(all.every((c) => c === all[0]));
      assert.equal(broker.accepted, 1);
      assert.equal(sasl.handshakes, 1);
    } finally {
      pool.close();
    }
  });
});

test("callers that find a renewal due share the renewal", async () => {
  const clock = new Clock(0n);
  const sasl = new FakeSasl(10_000n);
  await withBroker(serving(sasl), async (broker) => {
    const pool = new ConnectionPool(options(clock, scramCredentials("alice", "pw")));
    try {
      const c = await pool.get(broker.address);
      clock.now = 8n * SECOND;
      const all = await Promise.all([1, 2, 3].map(() => pool.get(broker.address)));
      assert.ok(all.every((x) => x === c));
      assert.equal(sasl.handshakes, 2, "one renewal, not three");
    } finally {
      pool.close();
    }
  });
});

test("a closed pool refuses", async () => {
  const pool = new ConnectionPool({ plaintextOnLoopback: true });
  pool.close();
  await assert.rejects(pool.get(address("127.0.0.1", 9)), /closed/);
});

test("the password is not in what a log line would show", () => {
  const c = scramCredentials("alice", "hunter2");
  assert.ok(!String(c).includes("hunter2"));
  assert.ok(!JSON.stringify(c).includes("hunter2"));
});

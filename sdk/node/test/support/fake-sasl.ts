import { ErrorCode } from "../../src/proto/error-code.js";
import type { DecodedRequest } from "../../src/proto/index.js";
import { ScramCredential } from "../../src/auth/scram-credential.js";
import type { Peer } from "./fake-broker.js";
import { Failed, TestScramServer } from "./scram-server.js";

export const ALICE = ScramCredential.of("pw", Buffer.from("0123456789abcdef", "ascii"), 4096);

/** The authentication half of a fake broker: SCRAM against a user table, or OAUTHBEARER accepting any token. */
export class FakeSasl {
  readonly users = new Map<string, ScramCredential>([["alice", ALICE]]);
  handshakes = 0;
  finals = 0;
  readonly tokens: string[] = [];
  /** Called with the number of handshakes so far, when a handshake arrives. */
  onHandshake: (n: number) => void = () => {};
  /** Mechanisms the broker offers. */
  mechanisms = ["SCRAM-SHA-512", "OAUTHBEARER"];

  private readonly servers = new Map<Peer, TestScramServer>();
  private readonly chosen = new Map<Peer, string>();

  constructor(public lifetimeMs: bigint) {}

  /** Handles SASL requests; returns false for any other. */
  handle(peer: Peer, decoded: DecodedRequest): boolean {
    const id = decoded.header.correlationId;
    const req = decoded.request;
    if (req.type === "SaslHandshake") {
      this.onHandshake(++this.handshakes);
      if (this.mechanisms.includes(req.mechanism)) {
        this.chosen.set(peer, req.mechanism);
        peer.reply(id, { type: "SaslHandshake", error: ErrorCode.NONE, mechanisms: this.mechanisms });
      } else {
        peer.reply(id, { type: "SaslHandshake", error: ErrorCode.UNSUPPORTED_SASL_MECHANISM, mechanisms: this.mechanisms });
      }
      return true;
    }
    if (req.type === "SaslAuthenticate") {
      if (this.chosen.get(peer) === "SCRAM-SHA-512") {
        this.scram(peer, id, req.authBytes);
      } else {
        const message = Buffer.from(req.authBytes).toString("utf8");
        this.tokens.push(message.slice("n,,\u0001auth=Bearer ".length, message.length - 2));
        this.finals++;
        peer.reply(id, {
          type: "SaslAuthenticate",
          error: ErrorCode.NONE,
          errorMsg: null,
          authBytes: new Uint8Array(0),
          sessionLifetimeMs: this.lifetimeMs,
        });
      }
      return true;
    }
    return false;
  }

  private scram(peer: Peer, id: number, bytes: Uint8Array): void {
    try {
      let server = this.servers.get(peer);
      if (server === undefined) {
        server = new TestScramServer((u) => this.users.get(u), "servernonce");
        this.servers.set(peer, server);
        const first = server.first(bytes);
        peer.reply(id, { type: "SaslAuthenticate", error: ErrorCode.NONE, errorMsg: null, authBytes: first, sessionLifetimeMs: 0n });
      } else {
        this.servers.delete(peer);
        const r = server.last(bytes);
        this.finals++;
        peer.reply(id, {
          type: "SaslAuthenticate",
          error: ErrorCode.NONE,
          errorMsg: null,
          authBytes: r.serverFinal,
          sessionLifetimeMs: this.lifetimeMs,
        });
      }
    } catch (e) {
      if (!(e instanceof Failed)) {
        throw e;
      }
      this.servers.delete(peer);
      peer.reply(id, {
        type: "SaslAuthenticate",
        error: ErrorCode.AUTHENTICATION_FAILED,
        errorMsg: "authentication failed",
        authBytes: new Uint8Array(0),
        sessionLifetimeMs: 0n,
      });
      peer.closeSocket();
    }
  }
}

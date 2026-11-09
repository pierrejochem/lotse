import { createHmac, createHash, timingSafeEqual } from "node:crypto";
import { ScramCredential } from "../../src/auth/scram-credential.js";

/** The login failed; the real broker answers AUTHENTICATION_FAILED and closes the connection. */
export class Failed extends Error {}

function unescape(name: string): string {
  return name.replaceAll("=2C", ",").replaceAll("=3D", "=");
}

function hmac(key: Uint8Array, data: Uint8Array): Buffer {
  return createHmac("sha512", key).update(data).digest();
}

/**
 * The broker's end of a SCRAM-SHA-512 exchange, written for tests from RFC 5802 and checked against the Rust
 * implementation through the vectors. It lets the fake broker of the connection tests accept or refuse a login.
 */
export class TestScramServer {
  private user = "";
  private credential: ScramCredential | undefined;
  private nonce = "";
  private authPrefix = "";

  constructor(
    private readonly lookup: (user: string) => ScramCredential | undefined,
    private readonly serverNonce: string,
  ) {}

  first(message: Uint8Array): Uint8Array {
    const msg = Buffer.from(message).toString("utf8");
    if (!msg.startsWith("n,,")) {
      throw new Failed("only n,, is supported");
    }
    const bare = msg.slice(3);
    const parts = bare.split(",");
    this.user = unescape((parts[0] ?? "").slice(2));
    const clientNonce = (parts[1] ?? "").slice(2);
    this.credential = this.lookup(this.user);
    if (this.credential === undefined) {
      throw new Failed("unknown user");
    }
    this.nonce = clientNonce + this.serverNonce;
    const serverFirst = `r=${this.nonce},s=${Buffer.from(this.credential.salt).toString("base64")},i=${this.credential.iterations}`;
    this.authPrefix = `${bare},${serverFirst}`;
    return Buffer.from(serverFirst, "utf8");
  }

  last(message: Uint8Array): { user: string; serverFinal: Uint8Array } {
    const credential = this.credential;
    if (credential === undefined) {
      throw new Failed("out of order");
    }
    const msg = Buffer.from(message).toString("utf8");
    const at = msg.lastIndexOf(",p=");
    const withoutProof = msg.slice(0, at);
    const proof = Buffer.from(msg.slice(at + 3), "base64");
    if (withoutProof !== `c=biws,r=${this.nonce}`) {
      throw new Failed("channel binding or nonce does not match");
    }
    const authMessage = Buffer.from(`${this.authPrefix},${withoutProof}`, "utf8");
    const clientSignature = hmac(credential.storedKey, authMessage);
    const clientKey = Buffer.alloc(64);
    for (let i = 0; i < 64; i++) {
      clientKey[i] = (proof[i] ?? 0) ^ (clientSignature[i] as number);
    }
    const stored = createHash("sha512").update(clientKey).digest();
    if (proof.length !== 64 || !timingSafeEqual(stored, credential.storedKey)) {
      throw new Failed("wrong password");
    }
    const signature = hmac(credential.serverKey, authMessage);
    return { user: this.user, serverFinal: Buffer.from(`v=${signature.toString("base64")}`, "utf8") };
  }
}

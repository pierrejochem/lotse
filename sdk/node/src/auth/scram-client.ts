import { ScramError } from "./errors.js";
import { CLIENT_KEY, constantTimeEquals, hmac, saltedPassword, SERVER_KEY, sha512, xor } from "./scram.js";

/** Fewest iterations a credential may have (RFC 7677 asks for at least 4096). */
export const MIN_ITERATIONS = 4096;

/** Most iterations a credential may have: a bound on the work one login costs either side. */
export const MAX_ITERATIONS = 1 << 20;

type State = "START" | "SENT_FIRST" | "SENT_FINAL" | "DONE";

const UTF8_STRICT = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

function utf8(bytes: Uint8Array): string {
  try {
    return UTF8_STRICT.decode(bytes);
  } catch {
    throw new ScramError("MALFORMED", "malformed SCRAM message: not UTF-8");
  }
}

function base64(text: string): Uint8Array | null {
  return BASE64.test(text) ? Buffer.from(text, "base64") : null;
}

/** `saslname`: `=` and `,` escaped as `=3D` and `=2C`. */
export function escapeName(name: string): string {
  return name.replaceAll("=", "=3D").replaceAll(",", "=2C");
}

function attribute(parts: readonly string[], index: number, key: string): string {
  const part = parts[index];
  if (part === undefined) {
    throw new ScramError("MALFORMED", `malformed SCRAM message: missing attribute ${key}`);
  }
  if (part.length < 2 || part[0] !== key || part[1] !== "=") {
    throw new ScramError("MALFORMED", `malformed SCRAM message: expected attribute ${key}`);
  }
  return part.slice(2);
}

function validNonce(n: string): boolean {
  if (n.length === 0) {
    return false;
  }
  for (let i = 0; i < n.length; i++) {
    const c = n.charCodeAt(i);
    if (c < 0x21 || c > 0x7e || c === 0x2c) {
      return false;
    }
  }
  return true;
}

/**
 * A client's end of one SCRAM-SHA-512 exchange (RFC 5802, RFC 7677), without I/O.
 *
 * Three calls in this order: `first`, `respond` with the server's first message, `verify` with the server's final
 * message. A call out of order fails. Channel binding is not used (`n,,`): TLS ties the session to the server's
 * certificate. The password is the PBKDF2 input as its UTF-8 bytes, with no SASLprep; a password that SASLprep would
 * change gives a different key than other SCRAM clients compute.
 *
 * The client refuses a server whose nonce does not strictly extend its own, and an iteration count outside
 * `MIN_ITERATIONS` to `MAX_ITERATIONS`, which would let a hostile server make it spin. `respond` runs PBKDF2 off the
 * event loop, so it returns a promise.
 */
export class ScramClient {
  private readonly password: Uint8Array;
  private state: State = "START";
  private bare = "";
  private serverSignature: Uint8Array = new Uint8Array(0);

  /**
   * @param nonce fresh and unpredictable: printable ASCII without a comma. `authenticate` uses 24 random bytes in hex.
   */
  constructor(
    private readonly user: string,
    password: string,
    private readonly nonce: string,
  ) {
    this.password = Buffer.from(password, "utf8");
  }

  /** The client's first message: `n,,n=<user>,r=<nonce>`. */
  first(): Uint8Array {
    this.bare = `n=${escapeName(this.user)},r=${this.nonce}`;
    this.state = "SENT_FIRST";
    return Buffer.from(`n,,${this.bare}`, "utf8");
  }

  /** Answers the server's first message with the client's final one. */
  async respond(serverFirstBytes: Uint8Array): Promise<Uint8Array> {
    if (this.state !== "SENT_FIRST") {
      throw new ScramError("OUT_OF_ORDER", "unexpected SCRAM step");
    }
    this.state = "DONE";
    const serverFirst = utf8(serverFirstBytes);
    const parts = serverFirst.split(",");
    const serverNonce = attribute(parts, 0, "r");
    const salt = base64(attribute(parts, 1, "s"));
    if (salt === null) {
      throw new ScramError("MALFORMED", "malformed SCRAM message: salt is not base64");
    }
    const count = attribute(parts, 2, "i");
    if (!/^[0-9]{1,10}$/.test(count)) {
      throw new ScramError("MALFORMED", "malformed SCRAM message: bad iterations");
    }
    const iterations = Number(count);
    // The server's nonce extends ours; a bound on the iterations keeps a hostile server from making the client spin.
    if (
      !serverNonce.startsWith(this.nonce) ||
      serverNonce.length === this.nonce.length ||
      !validNonce(serverNonce) ||
      iterations < MIN_ITERATIONS ||
      iterations > MAX_ITERATIONS
    ) {
      throw new ScramError("MALFORMED", "malformed SCRAM message: server nonce or iteration count not acceptable");
    }
    const salted = await saltedPassword(this.password, salt, iterations);
    const clientKey = hmac(salted, CLIENT_KEY);
    const storedKey = sha512(clientKey);
    const withoutProof = `c=biws,r=${serverNonce}`;
    const authMessage = Buffer.from(`${this.bare},${serverFirst},${withoutProof}`, "utf8");
    const proof = xor(clientKey, hmac(storedKey, authMessage));
    this.serverSignature = hmac(hmac(salted, SERVER_KEY), authMessage);
    this.state = "SENT_FINAL";
    return Buffer.from(`${withoutProof},p=${Buffer.from(proof).toString("base64")}`, "utf8");
  }

  /** Checks the server's final message: the server proves it knew the credential. */
  verify(serverFinal: Uint8Array): void {
    if (this.state !== "SENT_FINAL") {
      throw new ScramError("OUT_OF_ORDER", "unexpected SCRAM step");
    }
    this.state = "DONE";
    const msg = utf8(serverFinal);
    if (!msg.startsWith("v=")) {
      throw notVerified();
    }
    const signature = base64(msg.slice(2));
    if (signature === null || !constantTimeEquals(signature, this.serverSignature)) {
      throw notVerified();
    }
  }
}

function notVerified(): ScramError {
  return new ScramError("SERVER_NOT_VERIFIED", "the server could not prove it knows the credential");
}

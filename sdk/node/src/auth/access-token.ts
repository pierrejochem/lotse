import { createHash } from "node:crypto";
import { inspect } from "node:util";

/**
 * An access token (ADR 0133): its id and its secret, as `lotse_tok_<id>_<secret>`. The id is 16 lower-case hex digits and
 * names the token in the cluster's state; the secret is 32 random bytes in unpadded base64url (43 characters). The
 * cluster keeps only the SHA-256 of the secret bytes, which is all a client sends when it creates a token. Nothing here
 * draws randomness: the caller passes the secret in. The secret never shows in `toString`, `toJSON` or
 * `util.inspect`.
 */

const PREFIX = "lotse_tok_";
const SECRET_BYTES = 32;
const SECRET_CHARS = 43;

/** The characters Rust's `trim_ascii` strips: space, tab, line feed, form feed and carriage return. */
function isAsciiWhitespace(c: number): boolean {
  return c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0c || c === 0x0d;
}

function trimAscii(s: string): string {
  let start = 0;
  let end = s.length;
  while (start < end && isAsciiWhitespace(s.charCodeAt(start))) {
    start++;
  }
  while (end > start && isAsciiWhitespace(s.charCodeAt(end - 1))) {
    end--;
  }
  return s.slice(start, end);
}

function hexId(id: bigint): string {
  return BigInt.asUintN(64, id).toString(16).padStart(16, "0");
}

export class AccessToken {
  /** The token's id, a `u64` as its signed 64-bit bit pattern like every 64-bit value of this SDK (`u64ToString` prints it). */
  readonly id: bigint;
  readonly #secret: Uint8Array;

  /**
   * A token from an id and a secret the caller drew.
   *
   * @param id the token's id, a `u64`: either as itself or as its signed bit pattern
   * @param secret the 32 secret bytes; copied
   */
  constructor(id: bigint | number, secret: Uint8Array) {
    const wide = BigInt(id);
    if (wide < -(1n << 63n) || wide >= 1n << 64n) {
      throw new RangeError("a token's id is a u64");
    }
    if (secret.length !== SECRET_BYTES) {
      throw new RangeError(`a token's secret is 32 bytes, got ${secret.length}`);
    }
    this.id = BigInt.asIntN(64, wide);
    this.#secret = Uint8Array.from(secret);
  }

  /** A copy of the 32 secret bytes. */
  secret(): Uint8Array {
    return Uint8Array.from(this.#secret);
  }

  /** The text the user holds: `lotse_tok_<id>_<secret>`. */
  text(): string {
    return `${PREFIX}${hexId(this.id)}_${Buffer.from(this.#secret).toString("base64url")}`;
  }

  /** The SHA-256 of the secret bytes: all the cluster keeps, and all a client sends to create the token. */
  hash(): Uint8Array {
    return createHash("sha256").update(this.#secret).digest();
  }

  /** Whether `other` has the same id and the same secret. */
  equals(other: AccessToken): boolean {
    return this.id === other.id && Buffer.from(this.#secret).equals(other.#secret);
  }

  /**
   * Reads a token's text, ignoring ASCII whitespace around it (a pasted token often carries a newline), as the node
   * does. Anything that is not exactly a token's canonical text is refused: upper-case hex, a sign, another length,
   * padding, or a last character whose spare bits are not zero, so one secret has one text.
   *
   * @returns the token, or `null` if `text` is not one
   */
  static parse(text: string): AccessToken | null {
    const t = trimAscii(text);
    if (!t.startsWith(PREFIX)) {
      return null;
    }
    const rest = t.slice(PREFIX.length);
    const sep = rest.indexOf("_");
    if (sep < 0) {
      return null;
    }
    const id = rest.slice(0, sep);
    const secret = rest.slice(sep + 1);
    if (!/^[0-9a-f]{16}$/.test(id) || !/^[A-Za-z0-9_-]{43}$/.test(secret) || secret.length !== SECRET_CHARS) {
      return null;
    }
    const bytes = Buffer.from(secret, "base64url");
    // Node's decoder ignores the 2 spare bits of the last character; only the canonical text is a token.
    if (bytes.length !== SECRET_BYTES || bytes.toString("base64url") !== secret) {
      return null;
    }
    return new AccessToken(BigInt(`0x${id}`), bytes);
  }

  /** Names the token by its id only: the secret stays out of logs. */
  toString(): string {
    return `AccessToken[id=${hexId(this.id)}]`;
  }

  toJSON(): { id: string } {
    return { id: hexId(this.id) };
  }

  [inspect.custom](): string {
    return this.toString();
  }
}

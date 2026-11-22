import { inspect } from "node:util";
import { u64ToString } from "../record/unsigned.js";

/**
 * An access token just created (ADR 0133), from `LotseClient.createToken`. The text is shown once, since the cluster
 * keeps only the hash of the secret; it never shows in `toString`, `toJSON` or `util.inspect`.
 */
export class CreatedToken {
  readonly #text: string;

  constructor(
    /** Names the token; listed with `describeTokens` and revoked with `revokeToken`, a `u64` as its signed bit pattern. */
    readonly id: bigint,
    text: string,
    /** When the token stops being valid, ms since the Unix epoch. */
    readonly expiresAtMs: bigint,
  ) {
    this.#text = text;
  }

  /** The token's text, `lotse_tok_...`: sign in with it through `tokenCredentials`. */
  text(): string {
    return this.#text;
  }

  /** Names the id and the end only: the text is the secret and stays out of logs. */
  toString(): string {
    return `CreatedToken[id=${u64ToString(this.id)}, expiresAtMs=${this.expiresAtMs}]`;
  }

  toJSON(): { id: string; expiresAtMs: string } {
    return { id: u64ToString(this.id), expiresAtMs: this.expiresAtMs.toString() };
  }

  [inspect.custom](): string {
    return this.toString();
  }
}

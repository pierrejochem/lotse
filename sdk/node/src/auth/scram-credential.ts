import { randomBytes } from "node:crypto";
import type { AlterUserRequest } from "../proto/request.js";
import { MAX_ITERATIONS, MIN_ITERATIONS } from "./scram-client.js";
import { CLIENT_KEY, hmac, saltedPasswordSync, SERVER_KEY, sha512 } from "./scram.js";

/** The salt length `forPassword` uses. */
export const SALT_BYTES = 16;

/**
 * What a node keeps for one SCRAM user: salt, iteration count, `StoredKey` and `ServerKey`. Neither the password nor
 * anything that lets one log in without it is stored or sent: a stolen credential lets an attacker impersonate the
 * server, not the user.
 */
export class ScramCredential {
  readonly salt: Uint8Array;
  readonly iterations: number;
  /** `H(ClientKey)`, 64 bytes. */
  readonly storedKey: Uint8Array;
  /** `HMAC(SaltedPassword, "Server Key")`, 64 bytes. */
  readonly serverKey: Uint8Array;

  constructor(salt: Uint8Array, iterations: number, storedKey: Uint8Array, serverKey: Uint8Array) {
    this.salt = Uint8Array.from(salt);
    this.iterations = iterations;
    this.storedKey = Uint8Array.from(storedKey);
    this.serverKey = Uint8Array.from(serverKey);
  }

  /**
   * The credential for `password` with `salt` and `iterations`. The password is used as its UTF-8 bytes; there is no
   * SASLprep. Blocks for the iterations (4096 take a few milliseconds).
   *
   * @throws RangeError if `iterations` is outside the bounds a node accepts
   */
  static of(password: string, salt: Uint8Array, iterations: number): ScramCredential {
    if (!Number.isInteger(iterations) || iterations < MIN_ITERATIONS || iterations > MAX_ITERATIONS) {
      throw new RangeError(`iterations must be from ${MIN_ITERATIONS} to ${MAX_ITERATIONS}, not ${iterations}`);
    }
    const salted = saltedPasswordSync(Buffer.from(password, "utf8"), salt, iterations);
    const clientKey = hmac(salted, CLIENT_KEY);
    return new ScramCredential(salt, iterations, sha512(clientKey), hmac(salted, SERVER_KEY));
  }

  /** A credential for `password` with a fresh 16-byte salt and `MIN_ITERATIONS` iterations, as `lotse-client` makes it. */
  static forPassword(password: string, random: (n: number) => Uint8Array = randomBytes): ScramCredential {
    return ScramCredential.of(password, random(SALT_BYTES), MIN_ITERATIONS);
  }

  /** The `AlterUser` request that sets this credential for `user` (admin). */
  alterUser(user: string): AlterUserRequest {
    return {
      type: "AlterUser",
      user,
      credential: {
        iterations: this.iterations,
        salt: this.salt,
        storedKey: this.storedKey,
        serverKey: this.serverKey,
      },
    };
  }

  /** The `AlterUser` request that removes `user`'s credential (admin). */
  static removal(user: string): AlterUserRequest {
    return { type: "AlterUser", user, credential: null };
  }

  toString(): string {
    return `ScramCredential[iterations=${this.iterations}]`;
  }
}

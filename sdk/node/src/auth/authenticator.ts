import { randomBytes } from "node:crypto";
import { ErrorCode, errorName } from "../proto/error-code.js";
import type { Request } from "../proto/request.js";
import type { Response, SaslAuthenticateResponse } from "../proto/response.js";
import { type Credentials, mechanismOf, oauthBearerInitialResponse } from "./credentials.js";
import { AuthenticationError } from "./errors.js";
import { ScramClient } from "./scram-client.js";

/** Sends one request and returns its response, or rejects if there is none. */
export type Exchange = (request: Request) => Promise<Response>;

/** A fresh client nonce: 24 random bytes in hex. */
export function makeNonce(random: (n: number) => Uint8Array = randomBytes): string {
  return Buffer.from(random(24)).toString("hex");
}

function unexpected(r: Response): AuthenticationError {
  return new AuthenticationError(`unexpected response ${r.type} during authentication`);
}

async function step(exchange: Exchange, bytes: Uint8Array): Promise<SaslAuthenticateResponse> {
  const r = await exchange({ type: "SaslAuthenticate", authBytes: bytes });
  if (r.type !== "SaslAuthenticate") {
    throw unexpected(r);
  }
  if (r.error !== ErrorCode.NONE) {
    throw new AuthenticationError(r.errorMsg ?? errorName(r.error), r.error);
  }
  return r;
}

/**
 * Runs the SASL exchange of a connection: `SaslHandshake` with the mechanism, then `SaslAuthenticate` rounds. Calling
 * it again on a connection that is already authenticated renews the session; the node then insists on the same
 * principal.
 *
 * @returns the session's lifetime in milliseconds as the node granted it with its last answer (a `u64` as its signed
 *   bit pattern, like every 64-bit value); `0n` if the session lasts as long as the connection. It is taken from the
 *   last response: a renewal can grant another lifetime than the first login.
 * @throws AuthenticationError if the node refuses, does not offer the mechanism (the message names the mechanisms it
 *   offers), or a SCRAM server fails the checks of `ScramClient`
 */
export async function authenticate(
  exchange: Exchange,
  credentials: Credentials,
  random: (n: number) => Uint8Array = randomBytes,
): Promise<bigint> {
  const mechanism = mechanismOf(credentials);
  const handshake = await exchange({ type: "SaslHandshake", mechanism });
  if (handshake.type !== "SaslHandshake") {
    throw unexpected(handshake);
  }
  if (handshake.error !== ErrorCode.NONE) {
    const offered = handshake.mechanisms.length === 0 ? "none" : handshake.mechanisms.join(", ");
    throw new AuthenticationError(`${mechanism} is not enabled on this node; it offers ${offered}`, handshake.error);
  }
  if (credentials.type === "SCRAM") {
    const client = new ScramClient(credentials.user, credentials.password, makeNonce(random));
    const first = await step(exchange, client.first());
    const last = await client.respond(first.authBytes);
    const fin = await step(exchange, last);
    client.verify(fin.authBytes);
    return fin.sessionLifetimeMs;
  }
  const token = await credentials.token();
  return (await step(exchange, oauthBearerInitialResponse(token))).sessionLifetimeMs;
}

/**
 * How a client proves who it is to a node. A client certificate needs no entry here: it is part of the TLS
 * configuration.
 */
export type Credentials = ScramCredentials | OAuthBearerCredentials;

export const SCRAM_MECHANISM = "SCRAM-SHA-512";
export const OAUTHBEARER_MECHANISM = "OAUTHBEARER";

/** SASL SCRAM-SHA-512 with a user name and a password. The password never leaves the client. */
export interface ScramCredentials {
  readonly type: "SCRAM";
  readonly user: string;
  readonly password: string;
}

/**
 * SASL OAUTHBEARER. `token` is called for *every* authentication, on connecting and before each session ends, and
 * returns the current access token; keep it cheap and let it refresh the token when needed.
 */
export interface OAuthBearerCredentials {
  readonly type: "OAUTHBEARER";
  readonly token: () => string | Promise<string>;
}

export function scramCredentials(user: string, password: string): ScramCredentials {
  if (user.length === 0) {
    throw new RangeError("the user name is empty");
  }
  // toString and toJSON name the user and never the password, so a log line cannot leak it.
  const credentials = { type: "SCRAM" as const, user, password };
  Object.defineProperty(credentials, "password", { enumerable: false });
  Object.defineProperty(credentials, "toString", { value: () => `Scram[user=${user}]`, enumerable: false });
  return credentials;
}

export function oauthBearerCredentials(token: () => string | Promise<string>): OAuthBearerCredentials {
  return { type: "OAUTHBEARER", token };
}

export function mechanismOf(credentials: Credentials): string {
  return credentials.type === "SCRAM" ? SCRAM_MECHANISM : OAUTHBEARER_MECHANISM;
}

/** The initial client response for `token`: `n,,\x01auth=Bearer <token>\x01\x01` (RFC 7628). */
export function oauthBearerInitialResponse(token: string): Uint8Array {
  return Buffer.from(`n,,\u0001auth=Bearer ${token}\u0001\u0001`, "utf8");
}

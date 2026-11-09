import type { ErrorCodeValue } from "../proto/error-code.js";

/**
 * The node did not accept the credentials, or the node could not prove it knows the user's credential. The
 * connection must not be used afterwards: the node closes it after a failed attempt.
 *
 * A node says no more than "authentication failed" when the user or the password is wrong; the two cannot be told
 * apart, by design.
 */
export class AuthenticationError extends Error {
  /** The error code the node answered with, or `null` if the failure was found on this side. */
  readonly code: ErrorCodeValue | null;

  constructor(message: string, code: ErrorCodeValue | null = null) {
    super(message);
    this.name = "AuthenticationError";
    this.code = code;
  }
}

export type ScramErrorKind =
  /** A message does not follow the mechanism's grammar, or carries a nonce or iteration count that is not acceptable. */
  | "MALFORMED"
  /** The server's signature is missing or wrong: it does not know the user's credential. */
  | "SERVER_NOT_VERIFIED"
  /** The step does not fit the exchange's state. */
  | "OUT_OF_ORDER";

/** A SCRAM exchange that cannot go on: a message that breaks the grammar, or a server that fails its checks. */
export class ScramError extends AuthenticationError {
  readonly kind: ScramErrorKind;

  constructor(kind: ScramErrorKind, message: string) {
    super(message, null);
    this.name = "ScramError";
    this.kind = kind;
  }
}

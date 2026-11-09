import type { ErrorCodeValue } from "../proto/error-code.js";
import { BrokerResponseError, ClientAuthenticationError, type ClientError } from "./errors.js";

/**
 * Why one attempt failed, thrown inside the retry loop only. `RETRY` may be tried again at once, after metadata is
 * refreshed; `RETRY_MAYBE_DONE` may be tried again, but the request may have been carried out, so only an idempotent
 * produce does; `FAIL` ends the call with an error.
 */
export type AttemptKind = "RETRY" | "RETRY_MAYBE_DONE" | "FAIL";

export class Attempt extends Error {
  private constructor(
    readonly kind: AttemptKind,
    why: string,
    readonly error: ClientError | null,
  ) {
    super(why);
    this.name = "Attempt";
  }

  static retry(why: string): Attempt {
    return new Attempt("RETRY", why, null);
  }

  static retryMaybeDone(why: string): Attempt {
    return new Attempt("RETRY_MAYBE_DONE", why, null);
  }

  static fail(error: ClientError): Attempt {
    return new Attempt("FAIL", error.message, error);
  }
}

export type CallFailureKind =
  /** It never left: retrying cannot duplicate anything. */
  | "NOT_SENT"
  /** It may have reached the broker. */
  | "LOST"
  /** The connection could not authenticate: the request never left, and trying again will not help. */
  | "DENIED"
  /** The node refused the request (a role is missing): not carried out, and trying again will not help. */
  | "REFUSED";

/** Why a request got no response. */
export class CallFailure extends Error {
  private constructor(
    readonly kind: CallFailureKind,
    why: string,
    readonly code: ErrorCodeValue | null,
  ) {
    super(why);
    this.name = "CallFailure";
  }

  static notSent(why: string): CallFailure {
    return new CallFailure("NOT_SENT", why, null);
  }

  static lost(why: string): CallFailure {
    return new CallFailure("LOST", why, null);
  }

  static denied(why: string): CallFailure {
    return new CallFailure("DENIED", why, null);
  }

  static refused(code: ErrorCodeValue, why: string): CallFailure {
    return new CallFailure("REFUSED", why, code);
  }

  /** A connection that could not authenticate, or a refusal, fails at once; anything else is tried again. */
  toAttempt(): Attempt {
    switch (this.kind) {
      case "DENIED":
        return Attempt.fail(new ClientAuthenticationError(this.message));
      case "REFUSED":
        return Attempt.fail(new BrokerResponseError(this.code as ErrorCodeValue, this.message));
      default:
        return Attempt.retry(this.describe());
    }
  }

  describe(): string {
    return this.kind === "REFUSED" ? `${this.code}: ${this.message}` : this.message;
  }
}

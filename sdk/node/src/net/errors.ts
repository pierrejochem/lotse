import type { ErrorCodeValue } from "../proto/error-code.js";
import { errorName } from "../proto/error-code.js";

/**
 * A request got no answer because the connection was not there, broke, was closed, or the node did not reply in time.
 *
 * `requestSent` says whether the request may have reached the node. When it is `false` the request was certainly not
 * carried out and may be sent again. When it is `true` the outcome is unknown: resend only what is idempotent.
 */
export class ConnectionError extends Error {
  readonly requestSent: boolean;

  constructor(message: string, requestSent: boolean, cause?: unknown) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "ConnectionError";
    this.requestSent = requestSent;
  }
}

/** No answer arrived within the request timeout. The outcome is unknown. */
export class RequestTimeoutError extends ConnectionError {
  constructor(timeoutMs: number) {
    super(`no response within ${timeoutMs} ms`, true);
    this.name = "RequestTimeoutError";
  }
}

/** Plain TCP was asked for where it is not allowed, or no transport was chosen. Nothing was sent. */
export class InsecureTransportError extends ConnectionError {
  constructor(message: string) {
    super(message, false);
    this.name = "InsecureTransportError";
  }
}

/**
 * The node refused a request as a whole: the response had the `ERROR` flag, so it carries only an error code and
 * perhaps a reason, whatever the request was (an unsupported version, a connection that is not authenticated, a
 * request the principal may not make). The request was not carried out.
 *
 * Errors inside a typed response, such as a partition's error in `Produce`, are not exceptions; they stay in the
 * response.
 */
export class BrokerError extends Error {
  readonly code: ErrorCodeValue;
  /** The reason the node gave (tagged field 0), or `null`. */
  readonly reason: string | null;

  constructor(code: ErrorCodeValue, reason: string | null) {
    super(reason === null ? `refused: ${errorName(code)}` : `refused: ${errorName(code)}: ${reason}`);
    this.name = "BrokerError";
    this.code = code;
    this.reason = reason;
  }
}

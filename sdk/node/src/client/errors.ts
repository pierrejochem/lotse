import { describeError, type ErrorCodeValue } from "../proto/error-code.js";
import { u64ToString } from "../record/unsigned.js";

/**
 * What went wrong in a client call. The subclasses mirror the Rust client's `ClientError` and the Java client's
 * `ClientException`; each says what the caller can do next. `UnknownOutcomeError` is the one that needs a decision,
 * because a write may or may not have happened.
 */
export class ClientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The broker refused the request with an error code. */
export class BrokerResponseError extends ClientError {
  readonly code: ErrorCodeValue;
  /** The broker's explanation, or `null`. */
  readonly brokerMessage: string | null;

  constructor(code: ErrorCodeValue, message: string | null) {
    super(`broker error ${describeError(code)}${message === null ? "" : `: ${message}`}`);
    this.code = code;
    this.brokerMessage = message;
  }
}

/** The offset is not in the partition: retention deleted it (it is below the log start), or it is past the end. */
export class OffsetOutOfRangeError extends ClientError {
  constructor(
    readonly stream: string,
    readonly partition: number,
    /** The offset asked for. */
    readonly offset: bigint,
    /** The first offset the partition still holds. Reading on from there skips what retention deleted. */
    readonly logStart: bigint,
  ) {
    super(
      `offset ${u64ToString(offset)} is not in ${stream} partition ${partition}, which starts at ${u64ToString(logStart)}`,
    );
  }
}

/**
 * A produce may or may not have been written. Read the partition to find out, or produce again and accept a possible
 * duplicate. The client has started over with a new producer id, so later produces do not wait for a gap that may
 * never fill.
 */
export class UnknownOutcomeError extends ClientError {
  constructor(why: string) {
    super(`outcome unknown: ${why}`);
  }
}

/** No broker could serve the request before the retry time ran out. Nothing was written. */
export class GaveUpError extends ClientError {
  constructor(afterMs: number, why: string) {
    super(`gave up after ${Math.round(afterMs)} ms: ${why}`);
  }
}

/** The stream does not exist, or has no such partition. */
export class UnknownStreamError extends ClientError {
  constructor(
    readonly streamName: string,
    readonly partition: number,
  ) {
    super(`unknown stream ${streamName} or partition ${partition}`);
  }
}

/** The node did not accept the credentials, or does not offer the mechanism. Retrying with the same credentials will not help. */
export class ClientAuthenticationError extends ClientError {
  constructor(why: string) {
    super(`authentication failed: ${why}`);
  }
}

/** A resource with that name exists already (perhaps created by an earlier attempt of this same request). */
export class ResourceExistsError extends ClientError {
  constructor(
    readonly resourceName: string,
    /** The existing resource's id. */
    readonly resourceId: bigint,
  ) {
    super(`${resourceName} exists already (id ${u64ToString(resourceId)})`);
  }
}

/** The client was closed. */
export class ClientClosedError extends ClientError {
  constructor() {
    super("the client is closed");
  }
}

/** The queue consumer was closed. */
export class ConsumerClosedError extends ClientError {
  constructor() {
    super("the consumer is closed");
  }
}

/** The transaction can no longer be committed (a write's outcome is unknown, or the producer was fenced): abort it. */
export class MustAbortError extends ClientError {
  constructor(why: string) {
    super(`the transaction must be aborted: ${why}`);
  }
}

/** The transaction has ended, by `commit`, `abort` or `abandon`, or was ended by its signal. */
export class TransactionEndedError extends ClientError {
  constructor() {
    super("the transaction has ended");
  }
}

/** No transactional id is configured (`ClientOptions.transactionalId`). */
export class NotTransactionalError extends ClientError {
  constructor() {
    super("no transactional id is configured");
  }
}

/** A mandatory message matched no binding. */
export class UnroutableError extends ClientError {
  constructor(
    readonly exchange: string,
    readonly routingKey: string,
  ) {
    super(`no queue is bound to ${exchange} for routing key "${routingKey}"`);
  }
}

/**
 * A published message was written to some of its queues only. Each failure says whether that write is unknown
 * (`UnknownOutcomeError`) or did not happen.
 */
export class NotAllWrittenError extends ClientError {
  constructor(
    /** The queues that hold the message. */
    readonly written: readonly string[],
    /** Why the other queues do not. */
    readonly failures: readonly ClientError[],
  ) {
    super(
      `published to some queues only: written to [${written.join(", ")}], failed [${failures.map((f) => f.message).join("; ")}]`,
    );
  }
}

/** A node did not answer a request made of it alone (`LotseClient.nodeMetrics`): it is down, unreachable or answered something else. */
export class NoAnswerError extends ClientError {
  constructor(
    readonly nodeId: number,
    why: string,
  ) {
    super(`node ${nodeId} gave no answer: ${why}`);
  }
}

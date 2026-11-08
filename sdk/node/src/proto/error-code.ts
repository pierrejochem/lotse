/** Error codes of the native protocol. A code a client does not know is kept as it came and named `UNKNOWN`. */
export const ErrorCode = {
  NONE: 0,
  NOT_LEADER: 1,
  FENCED_LEADER_TERM: 2,
  UNKNOWN_RESOURCE: 3,
  OFFSET_OUT_OF_RANGE: 4,
  NOT_ENOUGH_REPLICAS: 5,
  REQUEST_TIMED_OUT: 6,
  OUT_OF_ORDER_SEQUENCE: 7,
  PRODUCER_FENCED: 8,
  INVALID_TXN_STATE: 9,
  QUOTA_EXCEEDED: 10,
  STORAGE_FULL: 11,
  CORRUPT_BATCH: 12,
  UNAUTHORIZED: 13,
  UNSUPPORTED_VERSION: 14,
  FENCED_MEMBER_EPOCH: 15,
  UNKNOWN_MEMBER_ID: 16,
  FRAME_TOO_LARGE: 17,
  COORDINATOR_NOT_AVAILABLE: 18,
  CONCURRENT_TRANSACTIONS: 19,
  COORDINATOR_FENCED: 20,
  INVALID_REQUEST: 21,
  UNKNOWN_SUBSCRIPTION: 22,
  ACK_TIMED_OUT: 23,
  UNSTABLE_OFFSET_COMMIT: 24,
  CONCURRENT_GROUP_CHANGE: 25,
  RESOURCE_EXISTS: 26,
  AUTHENTICATION_FAILED: 27,
  UNSUPPORTED_SASL_MECHANISM: 28,
  RESOURCE_NOT_EMPTY: 29,
  FENCED_INSTANCE_ID: 30,
  MESSAGE_NOT_HELD: 31,
  UNROUTABLE: 32,
} as const;

/** An error code as it travels: a `u16`. Known codes are the values of `ErrorCode`. */
export type ErrorCodeValue = number;

const NAMES: readonly string[] = Object.keys(ErrorCode);

/** The code's name, or `UNKNOWN` for one this SDK does not list. */
export function errorName(code: ErrorCodeValue): string {
  return NAMES[code] ?? "UNKNOWN";
}

/** Whether the same request may succeed later. `REQUEST_TIMED_OUT` also means the outcome is unknown. */
export function isRetriable(code: ErrorCodeValue): boolean {
  switch (code) {
    case ErrorCode.NOT_LEADER:
    case ErrorCode.FENCED_LEADER_TERM:
    case ErrorCode.NOT_ENOUGH_REPLICAS:
    case ErrorCode.REQUEST_TIMED_OUT:
    case ErrorCode.COORDINATOR_NOT_AVAILABLE:
    case ErrorCode.CONCURRENT_TRANSACTIONS:
    case ErrorCode.UNSTABLE_OFFSET_COMMIT:
    case ErrorCode.CONCURRENT_GROUP_CHANGE:
      return true;
    default:
      return false;
  }
}

/** `NOT_LEADER (1)` */
export function describeError(code: ErrorCodeValue): string {
  return `${errorName(code)} (${code})`;
}

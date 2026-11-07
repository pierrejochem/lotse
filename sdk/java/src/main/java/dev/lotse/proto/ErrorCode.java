package dev.lotse.proto;

import java.util.List;

/**
 * An error code, a {@code u16}; every partition in a response carries its own. Codes this library does not know
 * are kept as they came and named {@code UNKNOWN}.
 *
 * @param code the code as sent
 */
public record ErrorCode(int code) {
    /** Success. */
    public static final ErrorCode NONE = new ErrorCode(0);
    /** Wrong node; the response carries a {@link LeaderHint} if one is known. */
    public static final ErrorCode NOT_LEADER = new ErrorCode(1);
    /** The client's leader term is outdated: reload metadata. */
    public static final ErrorCode FENCED_LEADER_TERM = new ErrorCode(2);
    /** The id does not exist (any more). */
    public static final ErrorCode UNKNOWN_RESOURCE = new ErrorCode(3);
    /** Offset before the log start or after the leader's log end. */
    public static final ErrorCode OFFSET_OUT_OF_RANGE = new ErrorCode(4);
    /** No quorum reachable. */
    public static final ErrorCode NOT_ENOUGH_REPLICAS = new ErrorCode(5);
    /** No quorum confirmation within the timeout; the batch may or may not have been written. */
    public static final ErrorCode REQUEST_TIMED_OUT = new ErrorCode(6);
    /** Gap in the producer sequence. */
    public static final ErrorCode OUT_OF_ORDER_SEQUENCE = new ErrorCode(7);
    /** A newer producer epoch is active. */
    public static final ErrorCode PRODUCER_FENCED = new ErrorCode(8);
    /** The operation does not fit the transaction's state. */
    public static final ErrorCode INVALID_TXN_STATE = new ErrorCode(9);
    /** The tenant's storage quota is used up. */
    public static final ErrorCode QUOTA_EXCEEDED = new ErrorCode(10);
    /** The node's hard storage limit is reached. */
    public static final ErrorCode STORAGE_FULL = new ErrorCode(11);
    /** CRC or format invalid. */
    public static final ErrorCode CORRUPT_BATCH = new ErrorCode(12);
    /** Access denied. */
    public static final ErrorCode UNAUTHORIZED = new ErrorCode(13);
    /** API version outside the node's range. */
    public static final ErrorCode UNSUPPORTED_VERSION = new ErrorCode(14);
    /** The member's epoch is outdated: join again. */
    public static final ErrorCode FENCED_MEMBER_EPOCH = new ErrorCode(15);
    /** The member is not known: join again. */
    public static final ErrorCode UNKNOWN_MEMBER_ID = new ErrorCode(16);
    /** The frame is larger than the limit; the connection is closed. */
    public static final ErrorCode FRAME_TOO_LARGE = new ErrorCode(17);
    /** No coordinator is configured, or it has no leader right now. */
    public static final ErrorCode COORDINATOR_NOT_AVAILABLE = new ErrorCode(18);
    /** Another change to the same transaction is in progress: retry. */
    public static final ErrorCode CONCURRENT_TRANSACTIONS = new ErrorCode(19);
    /** A newer coordinator has taken over this producer's markers. */
    public static final ErrorCode COORDINATOR_FENCED = new ErrorCode(20);
    /** The request is not valid; retrying it unchanged will not help. */
    public static final ErrorCode INVALID_REQUEST = new ErrorCode(21);
    /** The subscription does not exist (any more) on this connection. */
    public static final ErrorCode UNKNOWN_SUBSCRIPTION = new ErrorCode(22);
    /** The subscription held a message unacknowledged longer than the ack timeout; it has ended. */
    public static final ErrorCode ACK_TIMED_OUT = new ErrorCode(23);
    /** A transaction holds an uncommitted offset for the partition: retry once it ends. */
    public static final ErrorCode UNSTABLE_OFFSET_COMMIT = new ErrorCode(24);
    /** Another change to the same consumer group is in progress: retry. */
    public static final ErrorCode CONCURRENT_GROUP_CHANGE = new ErrorCode(25);
    /** A resource with that name exists already; the answer names its id. */
    public static final ErrorCode RESOURCE_EXISTS = new ErrorCode(26);
    /** The credentials or token were not accepted. The node says no more than that. */
    public static final ErrorCode AUTHENTICATION_FAILED = new ErrorCode(27);
    /** The SASL mechanism is not enabled on this node; the answer lists the ones that are. */
    public static final ErrorCode UNSUPPORTED_SASL_MECHANISM = new ErrorCode(28);
    /** A delete asked for only if the resource is empty, and it is not (ADR 0063). */
    public static final ErrorCode RESOURCE_NOT_EMPTY = new ErrorCode(29);
    /** Another member joined the group with this member's instance id and took its place (ADR 0072). */
    public static final ErrorCode FENCED_INSTANCE_ID = new ErrorCode(30);
    /** A transactional ack names a message the connection does not hold; the transaction must abort (ADR 0075). */
    public static final ErrorCode MESSAGE_NOT_HELD = new ErrorCode(31);
    /** A mandatory published message routes to no queue (ADR 0079). */
    public static final ErrorCode UNROUTABLE = new ErrorCode(32);

    private static final List<String> NAMES = List.of(
            "NONE", "NOT_LEADER", "FENCED_LEADER_TERM", "UNKNOWN_RESOURCE", "OFFSET_OUT_OF_RANGE",
            "NOT_ENOUGH_REPLICAS", "REQUEST_TIMED_OUT", "OUT_OF_ORDER_SEQUENCE", "PRODUCER_FENCED",
            "INVALID_TXN_STATE", "QUOTA_EXCEEDED", "STORAGE_FULL", "CORRUPT_BATCH", "UNAUTHORIZED",
            "UNSUPPORTED_VERSION", "FENCED_MEMBER_EPOCH", "UNKNOWN_MEMBER_ID", "FRAME_TOO_LARGE",
            "COORDINATOR_NOT_AVAILABLE", "CONCURRENT_TRANSACTIONS", "COORDINATOR_FENCED", "INVALID_REQUEST",
            "UNKNOWN_SUBSCRIPTION", "ACK_TIMED_OUT", "UNSTABLE_OFFSET_COMMIT", "CONCURRENT_GROUP_CHANGE",
            "RESOURCE_EXISTS", "AUTHENTICATION_FAILED", "UNSUPPORTED_SASL_MECHANISM",
            "RESOURCE_NOT_EMPTY", "FENCED_INSTANCE_ID", "MESSAGE_NOT_HELD", "UNROUTABLE");

    /** Creates a code. */
    public ErrorCode {
        if (code < 0 || code > 0xFFFF) {
            throw new IllegalArgumentException("not a u16: " + code);
        }
    }

    /** Whether this is {@link #NONE}. */
    public boolean isOk() {
        return code == 0;
    }

    /** The name used in documentation and logs; {@code UNKNOWN} for a code this library does not know. */
    public String codeName() {
        return code < NAMES.size() ? NAMES.get(code) : "UNKNOWN";
    }

    /**
     * Whether the same request may succeed later, after reloading metadata. Retrying a request whose outcome is
     * unknown ({@link #REQUEST_TIMED_OUT}) is only safe where the request is idempotent.
     */
    public boolean retriable() {
        return switch (code) {
            case 1, 2, 5, 6, 18, 19, 24, 25 -> true;
            default -> false;
        };
    }

    @Override
    public String toString() {
        return codeName() + " (" + code + ")";
    }
}

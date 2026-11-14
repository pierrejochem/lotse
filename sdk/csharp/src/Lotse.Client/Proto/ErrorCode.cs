namespace Lotse.Proto;

/// <summary>
/// An error code, a <c>u16</c>; every partition in a response carries its own. Codes this library does not know are
/// kept as they came and named <c>UNKNOWN</c>.
/// </summary>
/// <param name="Code">The code as sent.</param>
public readonly record struct ErrorCode(ushort Code)
{
    /// <summary>Success.</summary>
    public static readonly ErrorCode None = new(0);

    /// <summary>Wrong node; the response carries a <see cref="LeaderHint"/> if one is known.</summary>
    public static readonly ErrorCode NotLeader = new(1);

    /// <summary>The client's leader term is outdated: reload metadata.</summary>
    public static readonly ErrorCode FencedLeaderTerm = new(2);

    /// <summary>The id does not exist (any more).</summary>
    public static readonly ErrorCode UnknownResource = new(3);

    /// <summary>Offset before the log start or after the leader's log end.</summary>
    public static readonly ErrorCode OffsetOutOfRange = new(4);

    /// <summary>No quorum reachable.</summary>
    public static readonly ErrorCode NotEnoughReplicas = new(5);

    /// <summary>No quorum confirmation within the timeout; the batch may or may not have been written.</summary>
    public static readonly ErrorCode RequestTimedOut = new(6);

    /// <summary>Gap in the producer sequence.</summary>
    public static readonly ErrorCode OutOfOrderSequence = new(7);

    /// <summary>A newer producer epoch is active.</summary>
    public static readonly ErrorCode ProducerFenced = new(8);

    /// <summary>The operation does not fit the transaction's state.</summary>
    public static readonly ErrorCode InvalidTxnState = new(9);

    /// <summary>The tenant's storage quota is used up.</summary>
    public static readonly ErrorCode QuotaExceeded = new(10);

    /// <summary>The node's hard storage limit is reached.</summary>
    public static readonly ErrorCode StorageFull = new(11);

    /// <summary>CRC or format invalid.</summary>
    public static readonly ErrorCode CorruptBatch = new(12);

    /// <summary>Access denied.</summary>
    public static readonly ErrorCode Unauthorized = new(13);

    /// <summary>API version outside the node's range.</summary>
    public static readonly ErrorCode UnsupportedVersion = new(14);

    /// <summary>The member's epoch is outdated: join again.</summary>
    public static readonly ErrorCode FencedMemberEpoch = new(15);

    /// <summary>The member is not known: join again.</summary>
    public static readonly ErrorCode UnknownMemberId = new(16);

    /// <summary>The frame is larger than the limit; the connection is closed.</summary>
    public static readonly ErrorCode FrameTooLarge = new(17);

    /// <summary>No coordinator is configured, or it has no leader right now.</summary>
    public static readonly ErrorCode CoordinatorNotAvailable = new(18);

    /// <summary>Another change to the same transaction is in progress: retry.</summary>
    public static readonly ErrorCode ConcurrentTransactions = new(19);

    /// <summary>A newer coordinator has taken over this producer's markers.</summary>
    public static readonly ErrorCode CoordinatorFenced = new(20);

    /// <summary>The request is not valid; retrying it unchanged will not help.</summary>
    public static readonly ErrorCode InvalidRequest = new(21);

    /// <summary>The subscription does not exist (any more) on this connection.</summary>
    public static readonly ErrorCode UnknownSubscription = new(22);

    /// <summary>The subscription held a message unacknowledged longer than the ack timeout; it has ended.</summary>
    public static readonly ErrorCode AckTimedOut = new(23);

    /// <summary>A transaction holds an uncommitted offset for the partition: retry once it ends.</summary>
    public static readonly ErrorCode UnstableOffsetCommit = new(24);

    /// <summary>Another change to the same consumer group is in progress: retry.</summary>
    public static readonly ErrorCode ConcurrentGroupChange = new(25);

    /// <summary>A resource with that name exists already; the answer names its id.</summary>
    public static readonly ErrorCode ResourceExists = new(26);

    /// <summary>The credentials or token were not accepted. The node says no more than that.</summary>
    public static readonly ErrorCode AuthenticationFailed = new(27);

    /// <summary>The SASL mechanism is not enabled on this node; the answer lists the ones that are.</summary>
    public static readonly ErrorCode UnsupportedSaslMechanism = new(28);

    /// <summary>A delete asked for only if the resource is empty, and it is not (ADR 0063).</summary>
    public static readonly ErrorCode ResourceNotEmpty = new(29);

    /// <summary>Another member joined the group with this member's instance id and took its place (ADR 0072).</summary>
    public static readonly ErrorCode FencedInstanceId = new(30);

    /// <summary>A transactional ack names a message the connection does not hold; the transaction must abort (ADR 0075).</summary>
    public static readonly ErrorCode MessageNotHeld = new(31);

    /// <summary>A mandatory published message routes to no queue (ADR 0079).</summary>
    public static readonly ErrorCode Unroutable = new(32);

    /// <summary>A new producer, and the partition holds as many as it admits: retry once an idle one is forgotten (ADR 0110).</summary>
    public static readonly ErrorCode ProducerLimit = new(33);

    private static readonly string[] Names =
    {
        "NONE", "NOT_LEADER", "FENCED_LEADER_TERM", "UNKNOWN_RESOURCE", "OFFSET_OUT_OF_RANGE",
        "NOT_ENOUGH_REPLICAS", "REQUEST_TIMED_OUT", "OUT_OF_ORDER_SEQUENCE", "PRODUCER_FENCED",
        "INVALID_TXN_STATE", "QUOTA_EXCEEDED", "STORAGE_FULL", "CORRUPT_BATCH", "UNAUTHORIZED",
        "UNSUPPORTED_VERSION", "FENCED_MEMBER_EPOCH", "UNKNOWN_MEMBER_ID", "FRAME_TOO_LARGE",
        "COORDINATOR_NOT_AVAILABLE", "CONCURRENT_TRANSACTIONS", "COORDINATOR_FENCED", "INVALID_REQUEST",
        "UNKNOWN_SUBSCRIPTION", "ACK_TIMED_OUT", "UNSTABLE_OFFSET_COMMIT", "CONCURRENT_GROUP_CHANGE",
        "RESOURCE_EXISTS", "AUTHENTICATION_FAILED", "UNSUPPORTED_SASL_MECHANISM",
        "RESOURCE_NOT_EMPTY", "FENCED_INSTANCE_ID", "MESSAGE_NOT_HELD", "UNROUTABLE", "PRODUCER_LIMIT",
    };

    /// <summary>Whether this is <see cref="None"/>.</summary>
    public bool IsOk => Code == 0;

    /// <summary>The name used in documentation and logs; <c>UNKNOWN</c> for a code this library does not know.</summary>
    public string CodeName => Code < Names.Length ? Names[Code] : "UNKNOWN";

    /// <summary>
    /// Whether the same request may succeed later, after reloading metadata. Retrying a request whose outcome is
    /// unknown (<see cref="RequestTimedOut"/>) is only safe where the request is idempotent.
    /// </summary>
    public bool Retriable => Code is 1 or 2 or 5 or 6 or 18 or 19 or 24 or 25 or 33;

    /// <inheritdoc/>
    public override string ToString() => $"{CodeName} ({Code})";
}

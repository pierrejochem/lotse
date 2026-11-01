package dev.lotse.record;

/**
 * What a control batch is. A control batch holds one record whose key is {@code version u16 | type u16}, both
 * little-endian. Consumers skip every kind; only the transaction markers matter to a client.
 */
public enum ControlKind {
    /** A transaction marker: abort. The value is the coordinator epoch, a {@code u32}. */
    ABORT_MARKER(0),
    /** A transaction marker: commit. The value is the coordinator epoch, a {@code u32}. */
    COMMIT_MARKER(1),
    /** The empty entry a new Raft leader appends. */
    RAFT_NOOP(2),
    /** A Raft membership change. */
    RAFT_CONFIG(3),
    /** A producer id allocation in the coordinator group. */
    PRODUCER_ID(4),
    /** The state of one transactional id. */
    TXN_STATE(5),
    /** The membership and assignment of one consumer group. */
    GROUP_STATE(6),
    /** Offsets committed for a consumer group. */
    GROUP_OFFSETS(7),
    /** A change to the cluster's resources, in the metadata group. */
    METADATA(8);

    private final int code;

    ControlKind(int code) {
        this.code = code;
    }

    /** The type code in the control record's key. */
    public int code() {
        return code;
    }

    static ControlKind fromCode(int code) {
        for (ControlKind k : values()) {
            if (k.code == code) {
                return k;
            }
        }
        throw RecordFormatException.malformed("unknown control type");
    }
}

package dev.lotse.record;

/** What a batch carries. Queue state lives in the same log as the data. */
public enum BatchKind {
    /** Messages. */
    DATA(0),
    /** Queue acknowledgements as offset ranges. */
    ACK(1),
    /** Delivery counts and other persisted queue state. */
    QUEUE_STATE(2),
    /** Due times for TTL and delayed delivery. */
    TIMER(3),
    /** Messages moved here from another queue's dead-lettering (ADR 0055); deliverable like data. */
    DEAD_LETTER(4),
    /** Messages leaving a queue because of its limits: the claim that settles them, and the note that they were forwarded (ADR 0055). */
    RETIRE(5);

    private final int code;

    BatchKind(int code) {
        this.code = code;
    }

    /** The value in bits 5 to 7 of the attributes. */
    public int code() {
        return code;
    }

    static BatchKind fromCode(int code) {
        for (BatchKind k : values()) {
            if (k.code == code) {
                return k;
            }
        }
        throw RecordFormatException.malformed("unknown batch kind");
    }
}

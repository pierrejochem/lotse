package dev.lotse.record;

/** The marker that ends a transaction in one partition. */
public enum ControlType {
    /** The producer's open transaction in this partition is aborted. */
    ABORT(0),
    /** The producer's open transaction in this partition is committed. */
    COMMIT(1);

    private final int code;

    ControlType(int code) {
        this.code = code;
    }

    /** The type code in a control record's key. */
    public int code() {
        return code;
    }
}

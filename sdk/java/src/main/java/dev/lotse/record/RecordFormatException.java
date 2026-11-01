package dev.lotse.record;

/** A batch, record or varint that cannot be read or written. The {@link #kind()} says why. */
public final class RecordFormatException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** Why a batch was refused; the same cases the broker distinguishes. */
    public enum Kind {
        /** The buffer ends before the batch or field does. */
        TRUNCATED,
        /** A varint is longer than allowed or its value does not fit. */
        VARINT_OVERFLOW,
        /** An unknown {@code magic} byte. */
        UNSUPPORTED_MAGIC,
        /** The CRC does not match the content. */
        CHECKSUM,
        /** A batch is larger than {@link Batch#MAX_BATCH_LEN}. */
        TOO_LARGE,
        /** Header fields contradict each other or the records. */
        MALFORMED,
        /** The batch uses a compression codec this library does not read. */
        UNSUPPORTED_COMPRESSION
    }

    private final Kind kind;

    /** Creates the exception. */
    public RecordFormatException(Kind kind, String message) {
        super(message);
        this.kind = kind;
    }

    /** Why. */
    public Kind kind() {
        return kind;
    }

    static RecordFormatException truncated() {
        return new RecordFormatException(Kind.TRUNCATED, "buffer ends inside a batch");
    }

    static RecordFormatException malformed(String what) {
        return new RecordFormatException(Kind.MALFORMED, "malformed batch: " + what);
    }
}

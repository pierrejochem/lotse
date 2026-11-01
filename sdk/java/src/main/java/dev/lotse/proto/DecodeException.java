package dev.lotse.proto;

/** A frame that cannot be decoded. */
public final class DecodeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** Why a frame was refused. */
    public enum Kind {
        /** The frame ends inside a field. */
        TRUNCATED,
        /** A field holds something no version allows. */
        MALFORMED,
        /** The request header is fine, but the key or version is not known; see {@link #header()}. */
        UNSUPPORTED
    }

    private final Kind kind;
    private final transient RequestHeader header;

    /** Creates the exception. */
    public DecodeException(Kind kind, String message) {
        this(kind, message, null);
    }

    /** Creates the exception for an unsupported request, keeping its header so it can still be answered. */
    public DecodeException(Kind kind, String message, RequestHeader header) {
        super(message);
        this.kind = kind;
        this.header = header;
    }

    /** Why. */
    public Kind kind() {
        return kind;
    }

    /** The header of an {@link Kind#UNSUPPORTED} request, otherwise {@code null}. */
    public RequestHeader header() {
        return header;
    }

    static DecodeException truncated() {
        return new DecodeException(Kind.TRUNCATED, "frame is truncated");
    }

    static DecodeException malformed(String what) {
        return new DecodeException(Kind.MALFORMED, "malformed frame: " + what);
    }
}

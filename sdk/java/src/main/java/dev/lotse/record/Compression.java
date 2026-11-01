package dev.lotse.record;

/** Compression of the record block. Only {@link #NONE} can be read or written so far. */
public enum Compression {
    /** Records are stored as is. */
    NONE(0),
    /** zstd (not supported yet). */
    ZSTD(1),
    /** lz4 frame format (not supported yet). */
    LZ4(2);

    private final int code;

    Compression(int code) {
        this.code = code;
    }

    /** The value in the low three bits of the attributes. */
    public int code() {
        return code;
    }

    static Compression fromCode(int code) {
        for (Compression c : values()) {
            if (c.code == code) {
                return c;
            }
        }
        throw RecordFormatException.malformed("unknown compression");
    }
}

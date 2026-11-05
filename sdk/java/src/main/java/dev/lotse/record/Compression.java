package dev.lotse.record;

/**
 * Compression of the record block. {@link #ZSTD} and {@link #LZ4} need their libraries on the class path
 * ({@code com.github.luben:zstd-jni}, {@code at.yawk.lz4:lz4-java}); without them such batches are refused with
 * {@link RecordFormatException.Kind#UNSUPPORTED_COMPRESSION}.
 */
public enum Compression {
    /** Records are stored as is. */
    NONE(0),
    /** One zstd frame. */
    ZSTD(1),
    /** One LZ4 frame (the frame format, not the bare block format). */
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

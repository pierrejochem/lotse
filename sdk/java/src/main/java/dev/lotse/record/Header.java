package dev.lotse.record;

import java.util.Objects;

/**
 * A record header: key and value metadata, such as a schema id.
 *
 * @param key the name; never null
 * @param value the value, or {@code null}
 */
public record Header(ByteString key, ByteString value) {
    /** Creates a header. */
    public Header {
        Objects.requireNonNull(key);
    }
}

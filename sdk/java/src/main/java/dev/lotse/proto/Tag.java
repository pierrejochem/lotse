package dev.lotse.proto;

import dev.lotse.record.ByteString;
import java.util.Objects;

/**
 * One tagged field: a tag number and an opaque value. Later versions add fields as tags; readers skip the ones
 * they do not know.
 *
 * @param tag the tag number, never negative
 * @param value its bytes
 */
public record Tag(long tag, ByteString value) {
    /** Creates a tag. */
    public Tag {
        if (tag < 0) {
            throw new IllegalArgumentException("negative tag");
        }
        Objects.requireNonNull(value);
    }
}

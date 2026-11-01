package dev.lotse.proto;

/**
 * The versions a node speaks for one API key.
 *
 * @param apiKey the key
 * @param min oldest version, a {@code u16}
 * @param max newest version, a {@code u16}
 */
public record ApiVersionRange(ApiKey apiKey, int min, int max) {
    /** Creates a range. */
    public ApiVersionRange {
        java.util.Objects.requireNonNull(apiKey);
    }

    /** Whether {@code version} is inside the range. */
    public boolean contains(int version) {
        return version >= min && version <= max;
    }
}

package dev.lotse.proto;

import java.util.Objects;

/**
 * The header of a request frame: {@code api_key u16 | api_version u16 | correlation_id u32 | flags u16 |
 * tagged_fields}.
 *
 * @param apiKey what is asked
 * @param apiVersion in which version, a {@code u16}
 * @param correlationId echoed in the response, a {@code u32}; {@code 0} is reserved for push frames
 * @param flags see {@link Flags}, a {@code u16}
 */
public record RequestHeader(ApiKey apiKey, int apiVersion, long correlationId, int flags) {
    /** Creates a header. */
    public RequestHeader {
        Objects.requireNonNull(apiKey);
    }
}

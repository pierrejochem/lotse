package dev.lotse.client;

import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A message to produce: an optional key, a value, headers and a timestamp.
 *
 * <p><b>The timestamp.</b> When none is set, the client uses the time of the produce call, in milliseconds since
 * the epoch. The Rust helper's default is 0, but a stream's retention judges a batch's age by the producer's
 * timestamps, so a message stamped 0 is old at once and may be deleted at the next retention pass. Set a timestamp
 * yourself only when it means something, such as the time of the event.
 *
 * @param key the key, or {@code null}
 * @param value the value, or {@code null} for a tombstone
 * @param headers headers in order
 * @param timestamp milliseconds since the epoch, or {@code null} for the time of the produce call
 */
public record OutMessage(ByteString key, ByteString value, List<Header> headers, Long timestamp) {
    /** Creates the message. */
    public OutMessage {
        headers = List.copyOf(Objects.requireNonNull(headers));
    }

    /** A message with a value only. */
    public static OutMessage value(byte[] value) {
        return new OutMessage(null, ByteString.copyOf(value), List.of(), null);
    }

    /** A message whose value is {@code value} in UTF-8. */
    public static OutMessage value(String value) {
        return value(value.getBytes(StandardCharsets.UTF_8));
    }

    /** This message with a key. */
    public OutMessage withKey(byte[] newKey) {
        return new OutMessage(ByteString.copyOf(newKey), value, headers, timestamp);
    }

    /** This message with a key, in UTF-8. */
    public OutMessage withKey(String newKey) {
        return withKey(newKey.getBytes(StandardCharsets.UTF_8));
    }

    /** This message with one more header. */
    public OutMessage withHeader(String name, byte[] headerValue) {
        List<Header> all = new ArrayList<>(headers);
        all.add(new Header(ByteString.utf8(name), headerValue == null ? null : ByteString.copyOf(headerValue)));
        return new OutMessage(key, value, all, timestamp);
    }

    /** This message with one more header whose value is text, in UTF-8. */
    public OutMessage withHeader(String name, String headerValue) {
        return withHeader(name, headerValue == null ? null : headerValue.getBytes(StandardCharsets.UTF_8));
    }

    /** This message with a timestamp, in milliseconds since the epoch. */
    public OutMessage withTimestamp(long millis) {
        return new OutMessage(key, value, headers, millis);
    }
}

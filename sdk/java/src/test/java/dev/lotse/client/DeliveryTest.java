package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.BatchRecord;
import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeliveryTest {
    private static Header h(String k, String v) {
        return new Header(ByteString.utf8(k), v == null ? null : ByteString.utf8(v));
    }

    private static Delivery with(Header... headers) {
        return new Delivery(
                new BatchRecord(5, 1_000, null, ByteString.utf8("body"), List.of(headers)), 3, new Object());
    }

    @Test
    void aMessageWithoutDeadLetterHeadersIsNotDeadLettered() {
        assertTrue(with(h("x-other", "1")).deadLetter().isEmpty());
        assertTrue(with().deadLetter().isEmpty());
    }

    @Test
    void theDeadLetterHeadersAreReadAsTyped() {
        Delivery d = with(
                h("app", "yes"),
                h("x-lotse-dead-letter-reason", "max-deliveries"),
                h("x-lotse-dead-letter-queue", "jobs"),
                h("x-lotse-dead-letter-queue-id", "18446744073709551615"),
                h("x-lotse-dead-letter-offset", "42"),
                h("x-lotse-dead-letter-deliveries", "3"),
                h("x-lotse-dead-letter-floor", "40"));
        DeadLetter dl = d.deadLetter().orElseThrow();
        assertEquals("max-deliveries", dl.reason());
        assertEquals("jobs", dl.queue());
        assertEquals(-1L, dl.queueId(), "a u64 above i64::MAX");
        assertEquals(42, dl.offset());
        assertEquals(3, dl.deliveries());
        assertEquals(40, dl.floor());
        assertEquals(5, d.offset());
        assertEquals(3, d.deliveryCount());
    }

    @Test
    void anUnreadableNumberReadsAsZero() {
        DeadLetter dl = with(h("x-lotse-dead-letter-reason", "expired"), h("x-lotse-dead-letter-offset", "nope"))
                .deadLetter()
                .orElseThrow();
        assertEquals("expired", dl.reason());
        assertEquals(0, dl.offset());
        assertEquals("", dl.queue());
    }

    @Test
    void headersKeepTheirOrder() {
        List<String> keys = new ArrayList<>();
        for (Header x : with(h("a", "1"), h("b", "2")).headers()) {
            keys.add(x.key().toUtf8());
        }
        assertEquals(List.of("a", "b"), keys);
    }
}

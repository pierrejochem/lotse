package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Batches compaction rewrote (ADR 0067): what a fetch of a compacted stream returns. They read with gaps between
 * their records' offsets, may hold no record at all, and keep every header field but the length, the checksum, the
 * record count and attribute bit 8.
 */
class CompactedConformanceTest {
    private record Vector(long count, long span, byte[] bytes) {}

    private static Map<String, Vector> vectors() {
        Map<String, Vector> out = new LinkedHashMap<>();
        for (String[] v : Vectors.lines("compacted.hex")) {
            out.put(v[0], new Vector(Long.parseLong(v[1]), Long.parseLong(v[2]), Vectors.hex(v[3])));
        }
        return out;
    }

    private static String nullable(ByteString b) {
        if (b == null) {
            return "-";
        }
        if (b.size() > 64) {
            return "#" + b.size() + ":" + String.format("%08x", Batch.crc(b.toByteArray(), 0, b.size()));
        }
        return "=" + Vectors.hex(b.toByteArray());
    }

    @Test
    void compactedBatchesReadWithTheirGaps() {
        Map<String, List<String>> expected = new HashMap<>();
        for (String[] v : Vectors.lines("compacted-records.txt")) {
            expected.computeIfAbsent(v[0], k -> new ArrayList<>()).add(String.join(" ", v));
        }
        Map<String, Vector> all = vectors();
        assertEquals(5, all.size());
        for (Map.Entry<String, Vector> e : all.entrySet()) {
            String label = e.getKey();
            Batch b = Batch.parse(ByteString.copyOf(e.getValue().bytes())).batch();
            assertTrue(b.isCompacted(), label);
            assertEquals(e.getValue().count(), b.recordCount(), label);
            assertEquals(e.getValue().span(), b.span(), label);
            List<String> lines = new ArrayList<>();
            List<BatchRecord> records = b.records();
            for (int i = 0; i < records.size(); i++) {
                BatchRecord r = records.get(i);
                String headers = r.headers().isEmpty()
                        ? "-"
                        : String.join(
                                ",",
                                r.headers().stream()
                                        .map(h -> Vectors.hex(h.key().toByteArray()) + ":" + nullable(h.value()))
                                        .toList());
                lines.add(String.join(
                        " ",
                        label,
                        Integer.toString(i),
                        Long.toUnsignedString(r.offset()),
                        Long.toString(r.timestamp()),
                        nullable(r.key()),
                        nullable(r.value()),
                        headers));
            }
            assertEquals(expected.getOrDefault(label, List.of()), lines, label);
            assertEquals(b.baseOffset() + b.span(), b.nextOffset(), label);
        }
    }

    @Test
    void theHeaderOfTheOriginalStays() {
        Map<String, byte[]> originals = new HashMap<>();
        for (String[] v : Vectors.lines("batches.hex")) {
            originals.put(v[0], Vectors.hex(v[1]));
        }
        for (String[] v : Vectors.lines("compressed.hex")) {
            originals.put(v[0], Vectors.hex(v[1]));
        }
        Map<String, String> from = Map.of(
                "gap", "data-stamped",
                "last-only", "idempotent",
                "emptied", "transactional",
                "zstd-gap", "zstd-stamped",
                "lz4-emptied", "lz4-large");
        for (Map.Entry<String, Vector> e : vectors().entrySet()) {
            Batch c = Batch.parse(ByteString.copyOf(e.getValue().bytes())).batch();
            Batch o = Batch.parse(ByteString.copyOf(originals.get(from.get(e.getKey())))).batch();
            assertFalse(o.isCompacted());
            String label = e.getKey();
            assertEquals(o.baseOffset(), c.baseOffset(), label);
            assertEquals(o.lastOffsetDelta(), c.lastOffsetDelta(), label);
            assertEquals(o.term(), c.term(), label);
            assertEquals(o.attributes(), c.attributes(), label);
            assertEquals(o.producerId(), c.producerId(), label);
            assertEquals(o.producerEpoch(), c.producerEpoch(), label);
            assertEquals(o.baseSequence(), c.baseSequence(), label);
            assertEquals(o.baseTimestamp(), c.baseTimestamp(), label);
            assertEquals(o.maxTimestamp(), c.maxTimestamp(), label);
        }
    }

    /** {@code b} with attribute bit 8 set and the checksum computed again. */
    private static byte[] markedCompacted(byte[] b) {
        byte[] out = b.clone();
        out[26] |= 1;
        long crc = Batch.crc(out, 25, out.length);
        for (int i = 0; i < 4; i++) {
            out[21 + i] = (byte) (crc >>> (8 * i));
        }
        return out;
    }

    @Test
    void onlyDataBatchesAreCompactedAndNeverHoldMoreRecordsThanOffsets() {
        Map<String, byte[]> batches = new HashMap<>();
        for (String[] v : Vectors.lines("batches.hex")) {
            batches.put(v[0], Vectors.hex(v[1]));
        }
        // A whole batch marked compacted still reads: nothing was left out.
        Batch whole = Batch.parse(ByteString.copyOf(markedCompacted(batches.get("data-stamped")))).batch();
        assertEquals(3, whole.records().size());
        assertThrows(
                RecordFormatException.class,
                () -> Batch.parse(ByteString.copyOf(markedCompacted(batches.get("control-commit")))),
                "a marker is never compacted");
        // More records than the batch spans.
        byte[] over = markedCompacted(batches.get("data-stamped")).clone();
        over[27] = 1; // last_offset_delta 1: two offsets for three records
        long crc = Batch.crc(over, 25, over.length);
        for (int i = 0; i < 4; i++) {
            over[21 + i] = (byte) (crc >>> (8 * i));
        }
        assertThrows(RecordFormatException.class, () -> Batch.parse(ByteString.copyOf(over)));
        // Not compacted, the strict rules stay: an empty batch is refused.
        byte[] emptied = null;
        for (String[] v : Vectors.lines("compacted.hex")) {
            if (v[0].equals("emptied")) {
                emptied = Vectors.hex(v[3]);
            }
        }
        byte[] plain = emptied.clone();
        plain[26] &= ~1;
        crc = Batch.crc(plain, 25, plain.length);
        for (int i = 0; i < 4; i++) {
            plain[21 + i] = (byte) (crc >>> (8 * i));
        }
        assertThrows(RecordFormatException.class, () -> Batch.parse(ByteString.copyOf(plain)));
    }
}

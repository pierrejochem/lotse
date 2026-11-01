package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/** Every batch the broker's code wrote is read here, checked field by field and written again byte for byte. */
class BatchConformanceTest {
    private static Map<String, byte[]> batches() {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (String[] v : Vectors.lines("batches.hex")) {
            out.put(v[0], Vectors.hex(v[1]));
        }
        return out;
    }

    private static Map<String, Map<String, String>> fields() {
        Map<String, Map<String, String>> out = new HashMap<>();
        for (String[] v : Vectors.lines("batch-fields.txt")) {
            Map<String, String> m = new HashMap<>();
            for (int i = 1; i < v.length; i++) {
                String[] kv = v[i].split("=", 2);
                m.put(kv[0], kv[1]);
            }
            out.put(v[0], m);
        }
        return out;
    }

    @Test
    void headerFieldsMatch() {
        Map<String, Map<String, String>> expected = fields();
        for (Map.Entry<String, byte[]> e : batches().entrySet()) {
            Batch.Split s = Batch.parse(ByteString.copyOf(e.getValue()));
            assertTrue(s.rest().isEmpty(), e.getKey());
            Batch b = s.batch();
            Map<String, String> f = expected.get(e.getKey());
            String label = e.getKey();
            assertEquals(Long.parseUnsignedLong(f.get("base_offset")), b.baseOffset(), label);
            assertEquals(Long.parseUnsignedLong(f.get("term")), b.term(), label);
            assertEquals(Integer.parseInt(f.get("attributes")), b.attributes().toBits(), label);
            assertEquals(Integer.parseInt(f.get("compression")), b.attributes().compression().code(), label);
            assertEquals(Boolean.parseBoolean(f.get("transactional")), b.attributes().transactional(), label);
            assertEquals(Boolean.parseBoolean(f.get("control")), b.attributes().control(), label);
            assertEquals(Integer.parseInt(f.get("kind")), b.attributes().kind().code(), label);
            assertEquals(Long.parseLong(f.get("last_offset_delta")), b.lastOffsetDelta(), label);
            assertEquals(Long.parseLong(f.get("base_ts")), b.baseTimestamp(), label);
            assertEquals(Long.parseLong(f.get("max_ts")), b.maxTimestamp(), label);
            assertEquals(Long.parseUnsignedLong(f.get("producer_id")), b.producerId(), label);
            assertEquals(Integer.parseInt(f.get("producer_epoch")), b.producerEpoch(), label);
            assertEquals(Integer.parseInt(f.get("base_sequence")), b.baseSequence(), label);
            assertEquals(Long.parseLong(f.get("record_count")), b.recordCount(), label);
            assertEquals(Integer.parseInt(f.get("len")), b.length(), label);
        }
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
    void recordsMatch() {
        Map<String, List<String>> expected = new HashMap<>();
        for (String[] v : Vectors.lines("batch-records.txt")) {
            expected.computeIfAbsent(v[0], k -> new ArrayList<>()).add(String.join(" ", v));
        }
        for (Map.Entry<String, byte[]> e : batches().entrySet()) {
            List<BatchRecord> records = Batch.parse(ByteString.copyOf(e.getValue())).batch().records();
            List<String> lines = new ArrayList<>();
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
                        e.getKey(),
                        Integer.toString(i),
                        Long.toUnsignedString(r.offset()),
                        Long.toString(r.timestamp()),
                        nullable(r.key()),
                        nullable(r.value()),
                        headers));
            }
            assertEquals(expected.get(e.getKey()), lines, e.getKey());
        }
    }

    @Test
    void parsedBatchesAreWrittenAgainByteForByte() {
        for (Map.Entry<String, byte[]> e : batches().entrySet()) {
            Batch b = Batch.parse(ByteString.copyOf(e.getValue())).batch();
            BatchBuilder builder = new BatchBuilder(
                    new BatchOptions(b.attributes(), b.producerId(), b.producerEpoch(), b.baseSequence()));
            for (BatchRecord r : b.records()) {
                builder.push(r.timestamp(), r.key(), r.value(), r.headers());
            }
            byte[] again = builder.finish().toByteArray();
            Batch.stamp(again, b.baseOffset(), b.term());
            assertEquals(Vectors.hex(e.getValue()), Vectors.hex(again), e.getKey());
        }
    }

    // The broker's own construction, repeated here without the reader: the oracle is written in the test.
    private static final long TS = 1_700_000_000_000L;

    private static byte[] bytes(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void batchesBuiltHereMatchTheBrokersBytes() {
        Map<String, byte[]> expected = batches();

        BatchBuilder b = new BatchBuilder();
        b.push(TS, bytes("k1"), bytes("v1"), List.of(new Header(ByteString.utf8("h"), ByteString.utf8("x")),
                new Header(ByteString.utf8("n"), null)));
        b.push(TS + 5, (byte[]) null, null);
        b.push(TS - 1, new byte[0], new byte[0], List.of(new Header(ByteString.EMPTY, ByteString.EMPTY)));
        byte[] built = b.finish().toByteArray();
        Batch.stamp(built, 100, 7);
        assertEquals(Vectors.hex(expected.get("data-stamped")), Vectors.hex(built));

        b = new BatchBuilder(new BatchOptions(Attributes.DATA, 17, 2, 5));
        for (int i = 0; i < 3; i++) {
            b.push(TS + i, new byte[] {(byte) i}, new byte[] {(byte) i, (byte) i});
        }
        built = b.finish().toByteArray();
        Batch.stamp(built, 3, 1);
        assertEquals(Vectors.hex(expected.get("idempotent")), Vectors.hex(built));

        b = new BatchBuilder(new BatchOptions(new Attributes(Compression.NONE, true, false, BatchKind.DATA), 17, 2, 8));
        b.push(TS, bytes("t"), bytes("transactional"));
        built = b.finish().toByteArray();
        Batch.stamp(built, 200, 2);
        assertEquals(Vectors.hex(expected.get("transactional")), Vectors.hex(built));

        built = ControlBatches.marker(17, 3, ControlType.COMMIT, 9, TS).toByteArray();
        Batch.stamp(built, 204, 2);
        assertEquals(Vectors.hex(expected.get("control-commit")), Vectors.hex(built));
        built = ControlBatches.marker(18, 0, ControlType.ABORT, 0, TS).toByteArray();
        Batch.stamp(built, 205, 2);
        assertEquals(Vectors.hex(expected.get("control-abort")), Vectors.hex(built));

        byte[] key = new byte[200];
        byte[] value = new byte[20_000];
        java.util.Arrays.fill(key, (byte) 0xab);
        java.util.Arrays.fill(value, (byte) 0xcd);
        b = new BatchBuilder();
        b.push(TS, key, value);
        b.push(-TS, bytes("negative"), null);
        assertEquals(Vectors.hex(expected.get("data-large")), Vectors.hex(b.finish().toByteArray()));
    }

    @Test
    void inPlaceEditsMatch() {
        for (String[] v : Vectors.lines("batch-ops.txt")) {
            byte[] batch = Vectors.hex(v[v.length - 2]);
            String[] args = v[1].split(",");
            switch (v[0]) {
                case "set_producer" -> Batch.setProducer(
                        batch, Long.parseUnsignedLong(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
                case "set_transactional" -> Batch.setTransactional(batch, Boolean.parseBoolean(args[0]));
                case "set_base_timestamp" -> Batch.setBaseTimestamp(batch, Long.parseLong(args[0]));
                default -> throw new AssertionError(v[0]);
            }
            assertEquals(v[v.length - 1], Vectors.hex(batch), v[0] + " " + v[1]);
            // The edited batch is still valid: the checksum was recomputed.
            Batch.parse(ByteString.copyOf(batch));
        }
    }

    @Test
    void concatenatedBatchesAreSplitByTheirLength() {
        for (String[] v : Vectors.lines("fetch-records.hex")) {
            byte[] all = Vectors.hex(v[1]);
            List<Batch> parsed = Batch.parseAll(ByteString.copyOf(all));
            assertTrue(parsed.size() >= 4, v[0]);
            int total = 0;
            long at = 0;
            for (Batch b : parsed) {
                assertEquals(b.length(), Batch.peekLen(java.util.Arrays.copyOfRange(all, (int) at, all.length)));
                at += b.length();
                total += b.length();
            }
            assertEquals(all.length, total, v[0]);
            // Cutting the last batch short is a truncation, not a shorter list.
            byte[] cut = java.util.Arrays.copyOf(all, all.length - 1);
            assertEquals(
                    RecordFormatException.Kind.TRUNCATED,
                    org.junit.jupiter.api.Assertions.assertThrows(
                                    RecordFormatException.class, () -> Batch.parseAll(ByteString.copyOf(cut)))
                            .kind());
        }
    }

    @Test
    void checksumsAreVerifiedOnTheWholeBatchFromByte25() {
        for (Map.Entry<String, byte[]> e : batches().entrySet()) {
            byte[] b = e.getValue();
            CRC32C crc = new CRC32C();
            crc.update(b, 25, b.length - 25);
            assertEquals(crc.getValue(), Batch.getU32(b, 21), e.getKey());
            // Stamping changes bytes 0..8 and 12..20 only, so the checksum stays valid.
            byte[] stamped = b.clone();
            Batch.stamp(stamped, 123456789L, 42);
            Batch.parse(ByteString.copyOf(stamped));
            assertArrayEquals(java.util.Arrays.copyOfRange(b, 21, b.length), java.util.Arrays.copyOfRange(stamped, 21, b.length));
        }
    }

    @Test
    void controlMarkersAreRead() {
        Batch commit = Batch.parse(ByteString.copyOf(batches().get("control-commit"))).batch();
        assertEquals(new ControlMarker(ControlType.COMMIT, 9), ControlBatches.readMarker(commit));
        assertEquals(ControlKind.COMMIT_MARKER, ControlBatches.kind(commit));
        Batch abort = Batch.parse(ByteString.copyOf(batches().get("control-abort"))).batch();
        assertEquals(new ControlMarker(ControlType.ABORT, 0), ControlBatches.readMarker(abort));
        assertEquals(17, Batch.parse(ByteString.copyOf(batches().get("control-commit"))).batch().producerId());
        assertEquals(ControlKind.RAFT_NOOP, ControlBatches.kind(Batch.parse(ByteString.copyOf(batches().get("raft-noop"))).batch()));
        assertEquals(ControlKind.RAFT_CONFIG, ControlBatches.kind(Batch.parse(ByteString.copyOf(batches().get("raft-config"))).batch()));
        assertEquals(ControlKind.METADATA, ControlBatches.kind(Batch.parse(ByteString.copyOf(batches().get("internal-metadata"))).batch()));
        Batch data = Batch.parse(ByteString.copyOf(batches().get("data-stamped"))).batch();
        assertEquals(null, ControlBatches.kind(data));
        assertFalse(data.attributes().control());
    }

    @Test
    void theBatchKindsOfTheQueueLog() {
        assertEquals(BatchKind.ACK, Batch.parse(ByteString.copyOf(batches().get("ack"))).batch().attributes().kind());
        assertEquals(BatchKind.QUEUE_STATE, Batch.parse(ByteString.copyOf(batches().get("queue-state"))).batch().attributes().kind());
        assertEquals(BatchKind.DEAD_LETTER, Batch.parse(ByteString.copyOf(batches().get("dead-letter"))).batch().attributes().kind());
        assertEquals(BatchKind.RETIRE, Batch.parse(ByteString.copyOf(batches().get("retire-out"))).batch().attributes().kind());
        assertEquals(BatchKind.RETIRE, Batch.parse(ByteString.copyOf(batches().get("retire-done"))).batch().attributes().kind());
    }
}

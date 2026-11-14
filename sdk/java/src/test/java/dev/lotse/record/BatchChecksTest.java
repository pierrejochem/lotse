package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.RecordFormatException.Kind;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Each check of {@link Batch#parse} and {@link Batch#records()} has a test that fails without it. */
class BatchChecksTest {
    private static byte[] sample() {
        BatchBuilder b = new BatchBuilder();
        b.push(1000, new byte[] {1}, new byte[] {2, 3});
        b.push(1001, null, new byte[] {4});
        return b.finish().toByteArray();
    }

    /** Recomputes the checksum, so a test changes one thing and the checksum is not what is refused. */
    private static byte[] sum(byte[] b) {
        Batch.putU32(b, 21, Batch.crc(b, 25, b.length));
        return b;
    }

    private static Kind parseFails(byte[] b) {
        return assertThrows(RecordFormatException.class, () -> Batch.parse(ByteString.copyOf(b))).kind();
    }

    private static Kind recordsFail(byte[] b) {
        Batch batch = Batch.parse(ByteString.copyOf(b)).batch();
        return assertThrows(RecordFormatException.class, batch::records).kind();
    }

    @Test
    void aValidBatchParses() {
        Batch b = Batch.parse(ByteString.copyOf(sample())).batch();
        assertEquals(2, b.recordCount());
        assertEquals(2, b.records().size());
    }

    @Test
    void aFlippedByteAnywhereAfterTheChecksumFieldIsRefused() {
        byte[] good = sample();
        for (int i = 25; i < good.length; i++) {
            byte[] bad = good.clone();
            bad[i] ^= 0x01;
            assertEquals(Kind.CHECKSUM, parseFails(bad), "byte " + i);
        }
    }

    @Test
    void aWrongStoredChecksumIsRefused() {
        byte[] bad = sample();
        bad[21] ^= 0x01;
        assertEquals(Kind.CHECKSUM, parseFails(bad));
    }

    @Test
    void theChecksumIsNotComputedOverTheBytesBefore25() {
        byte[] b = sample();
        b[3] ^= 0x55; // base_offset
        b[14] ^= 0x55; // term
        Batch.parse(ByteString.copyOf(b));
    }

    @Test
    void aWrongMagicIsRefusedBeforeTheChecksum() {
        byte[] b = sample();
        b[20] = 2;
        assertEquals(Kind.UNSUPPORTED_MAGIC, parseFails(b));
    }

    @Test
    void aBufferShorterThanTheLengthPrefixOrTheBatchIsTruncated() {
        assertEquals(Kind.TRUNCATED, parseFails(new byte[11]));
        byte[] b = sample();
        assertEquals(Kind.TRUNCATED, parseFails(Arrays.copyOf(b, b.length - 1)));
    }

    @Test
    void aLengthShorterThanTheHeaderIsMalformed() {
        byte[] b = sample();
        Batch.putU32(b, 8, 10);
        assertEquals(Kind.MALFORMED, parseFails(b));
    }

    @Test
    void aLengthOverTheLimitIsRefusedWithoutOverflowing() {
        byte[] b = sample();
        Batch.putU32(b, 8, Batch.MAX_BATCH_LEN - Batch.LEN_PREFIX + 1);
        assertEquals(Kind.TOO_LARGE, parseFails(b));
        Batch.putU32(b, 8, 0xFFFF_FFFFL);
        assertEquals(Kind.TOO_LARGE, parseFails(b));
        assertEquals(0xFFFF_FFFFL + 12, Batch.peekLen(b));
    }

    @Test
    void attributesAreValidated() {
        byte[] b = sample();
        Batch.putU16(b, 25, 3); // compression 3
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
        b = sample();
        Batch.putU16(b, 25, 7 << 5); // kind 7
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
        b = sample();
        Batch.putU16(b, 25, 1 << 9); // a reserved bit (bit 8 marks a compacted batch)
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
        b = sample();
        Batch.putU16(b, 25, 1 << 15);
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
    }

    @Test
    void everyKnownAttributeCombinationRoundTrips() {
        for (Compression c : Compression.values()) {
            for (BatchKind k : BatchKind.values()) {
                for (int flags = 0; flags < 4; flags++) {
                    Attributes a = new Attributes(c, (flags & 1) != 0, (flags & 2) != 0, k);
                    assertEquals(a, Attributes.fromBits(a.toBits()));
                }
            }
        }
    }

    @Test
    void anEmptyRecordCountIsRefused() {
        byte[] b = sample();
        Batch.putU32(b, 61, 0);
        Batch.putU32(b, 27, 0xFFFF_FFFFL);
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
    }

    @Test
    void lastOffsetDeltaMustMatchTheRecordCount() {
        byte[] b = sample();
        Batch.putU32(b, 27, 5);
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
    }

    @Test
    void maxTimestampBeforeBaseTimestampIsRefused() {
        byte[] b = sample();
        Batch.putI64(b, 39, 999);
        assertEquals(Kind.MALFORMED, parseFails(sum(b)));
    }

    @Test
    void aControlBatchHoldsOneRecord() {
        BatchBuilder b = new BatchBuilder(new BatchOptions(new Attributes(Compression.NONE, true, true, BatchKind.DATA), 1, 0, -1));
        b.push(1, new byte[4], new byte[4]);
        b.push(2, new byte[4], new byte[4]);
        assertEquals(Kind.MALFORMED, parseFails(b.finish().toByteArray()));
    }

    @Test
    void recordsMustHaveConsecutiveOffsetDeltas() {
        byte[] b = sample();
        // First record: length, attributes, ts_delta, offset_delta... offset_delta is the 3rd byte after the length.
        int firstRecord = Batch.HEADER_LEN;
        b[firstRecord + 3] = (byte) Varint.zigzag(1); // offset_delta 1 instead of 0
        assertEquals(Kind.MALFORMED, recordsFail(sum(b)));
    }

    @Test
    void bytesAfterTheLastRecordAreRefused() {
        byte[] b = Arrays.copyOf(sample(), sample().length + 1);
        Batch.putU32(b, 8, b.length - Batch.LEN_PREFIX);
        assertEquals(Kind.MALFORMED, recordsFail(sum(b)));
    }

    @Test
    void bytesInsideARecordAreRefused() {
        byte[] good = sample();
        // Grow the first record by one byte: its length varint says 1 more, and one more byte follows it.
        byte[] b = new byte[good.length + 1];
        System.arraycopy(good, 0, b, 0, Batch.HEADER_LEN + 1 + 7);
        int lenAt = Batch.HEADER_LEN;
        int recordLen = Varint.getI64(good, lenAt, good.length).value() > 0 ? (int) Varint.unzigzag(good[lenAt] & 0xFF) : 0;
        b[lenAt] = (byte) Varint.zigzag(recordLen + 1);
        b[Batch.HEADER_LEN + 1 + recordLen] = 0x7f;
        System.arraycopy(good, Batch.HEADER_LEN + 1 + recordLen, b, Batch.HEADER_LEN + 2 + recordLen, good.length - Batch.HEADER_LEN - 1 - recordLen);
        Batch.putU32(b, 8, b.length - Batch.LEN_PREFIX);
        assertEquals(Kind.MALFORMED, recordsFail(sum(b)));
    }

    @Test
    void aRecordLongerThanTheBatchIsTruncated() {
        byte[] b = sample();
        b[Batch.HEADER_LEN] = (byte) Varint.zigzag(100);
        assertEquals(Kind.TRUNCATED, recordsFail(sum(b)));
    }

    @Test
    void aNegativeRecordLengthIsRefused() {
        byte[] b = sample();
        b[Batch.HEADER_LEN] = (byte) Varint.zigzag(-5);
        assertEquals(Kind.MALFORMED, recordsFail(sum(b)));
    }

    @Test
    void aCompressedBatchParsesBeforeItsBlockIsDecoded() {
        // The header is checked without the codec; the block only when the records are read.
        byte[] b = sample();
        Batch.putU16(b, 25, 1);
        Batch batch = Batch.parse(ByteString.copyOf(sum(b))).batch();
        assertEquals(Compression.ZSTD, batch.attributes().compression());
        assertEquals(Kind.MALFORMED, assertThrows(RecordFormatException.class, batch::records).kind());
    }

    @Test
    void anEmptyBuilderAndAnOversizedBatchAreRefused() {
        assertEquals(Kind.MALFORMED, assertThrows(RecordFormatException.class, () -> new BatchBuilder().finish()).kind());
        BatchBuilder b = new BatchBuilder();
        b.push(1, null, new byte[Batch.MAX_BATCH_LEN]);
        assertEquals(Kind.TOO_LARGE, assertThrows(RecordFormatException.class, b::finish).kind());
    }

    @Test
    void editsRefuseBuffersThatAreNotWholeBatches() {
        byte[] b = sample();
        byte[] cut = Arrays.copyOf(b, b.length - 1);
        assertEquals(Kind.TRUNCATED, assertThrows(RecordFormatException.class, () -> Batch.setProducer(cut, 1, 1, 1)).kind());
        assertEquals(Kind.TRUNCATED, assertThrows(RecordFormatException.class, () -> Batch.setTransactional(cut, true)).kind());
        assertEquals(Kind.TRUNCATED, assertThrows(RecordFormatException.class, () -> Batch.setBaseTimestamp(cut, 1)).kind());
        assertEquals(Kind.TRUNCATED, assertThrows(RecordFormatException.class, () -> Batch.stamp(new byte[10], 1, 1)).kind());
        assertEquals(-1, Batch.peekLen(new byte[11]));
    }

    @Test
    void nullAndEmptyAreDifferentInKeysValuesAndHeaders() {
        BatchBuilder b = new BatchBuilder();
        b.push(5, (byte[]) null, new byte[0], List.of(new Header(ByteString.EMPTY, null)));
        BatchRecord r = Batch.parse(b.finish()).batch().records().get(0);
        assertEquals(null, r.key());
        assertTrue(r.value().isEmpty());
        assertEquals(null, r.headers().get(0).value());
    }

    @Test
    void u64OffsetsAreUnsigned() {
        byte[] b = sample();
        Batch.stamp(b, -2L, 1); // 2^64 - 2
        Batch batch = Batch.parse(ByteString.copyOf(b)).batch();
        assertEquals("18446744073709551614", Unsigned.toString(batch.baseOffset()));
        assertEquals(-1L, batch.lastOffset());
    }
}

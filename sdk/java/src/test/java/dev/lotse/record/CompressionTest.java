package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.RecordFormatException.Kind;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * zstd and LZ4 record blocks (ADR 0058, ADR 0061): the broker's compressed batches read like their plain twins,
 * batches compressed here read back, and a block that does not decode or decodes to too much is refused. These
 * tests run with both codec libraries on the class path, as the build puts them there.
 */
class CompressionTest {
    private static final long TS = 1_700_000_000_000L;

    private static String nullable(ByteString b) {
        if (b == null) {
            return "-";
        }
        if (b.size() > 64) {
            return "#" + b.size() + ":" + String.format("%08x", Batch.crc(b.toByteArray(), 0, b.size()));
        }
        return "=" + Vectors.hex(b.toByteArray());
    }

    private static List<String> lines(String label, Batch batch) {
        List<String> out = new ArrayList<>();
        List<BatchRecord> records = batch.records();
        for (int i = 0; i < records.size(); i++) {
            BatchRecord r = records.get(i);
            String headers = r.headers().isEmpty()
                    ? "-"
                    : String.join(
                            ",",
                            r.headers().stream()
                                    .map(h -> Vectors.hex(h.key().toByteArray()) + ":" + nullable(h.value()))
                                    .toList());
            out.add(String.join(
                    " ",
                    label,
                    Integer.toString(i),
                    Long.toUnsignedString(r.offset()),
                    Long.toString(r.timestamp()),
                    nullable(r.key()),
                    nullable(r.value()),
                    headers));
        }
        return out;
    }

    private static byte[] sum(byte[] b) {
        Batch.putU32(b, 21, Batch.crc(b, 25, b.length));
        return b;
    }

    @Test
    void theBrokersCompressedBatchesReadAsWritten() {
        Map<String, List<String>> expected = new HashMap<>();
        for (String[] v : Vectors.lines("compressed-records.txt")) {
            expected.computeIfAbsent(v[0], k -> new ArrayList<>()).add(String.join(" ", v));
        }
        int seen = 0;
        for (String[] v : Vectors.lines("compressed.hex")) {
            Batch b = Batch.parse(ByteString.copyOf(Vectors.hex(v[1]))).batch();
            assertEquals(v[0].startsWith("zstd") ? Compression.ZSTD : Compression.LZ4, b.attributes().compression());
            assertEquals(expected.get(v[0]), lines(v[0], b), v[0]);
            seen++;
        }
        assertEquals(expected.size(), seen, "every compressed vector has its records");
    }

    private static BatchBuilder large(Compression codec) {
        byte[] key = new byte[200];
        byte[] value = new byte[20_000];
        Arrays.fill(key, (byte) 0xab);
        Arrays.fill(value, (byte) 0xcd);
        BatchBuilder b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.DATA), 9, 1, 4));
        b.push(TS, key, value, List.of(new Header(ByteString.utf8("h"), ByteString.utf8("x"))));
        b.push(TS + 3, (byte[]) null, null);
        b.push(-TS, "negative".getBytes(java.nio.charset.StandardCharsets.UTF_8), new byte[0]);
        return b;
    }

    @Test
    void batchesCompressedHereReadLikeTheirPlainTwin() {
        Batch plain = Batch.parse(large(Compression.NONE).finish()).batch();
        for (Compression codec : List.of(Compression.ZSTD, Compression.LZ4)) {
            Batch c = Batch.parse(large(codec).finish()).batch();
            assertEquals(codec, c.attributes().compression());
            assertTrue(c.length() < plain.length() / 10, codec + ": " + c.length() + " of " + plain.length());
            assertEquals(lines("x", plain), lines("x", c), codec.toString());
            assertEquals(plain.recordCount(), c.recordCount());
            assertEquals(plain.maxTimestamp(), c.maxTimestamp());
            assertEquals(plain.producerId(), c.producerId());
        }
    }

    @Test
    void aBlockThatDoesNotDecodeIsRefused() {
        for (Compression codec : List.of(Compression.ZSTD, Compression.LZ4)) {
            byte[] good = large(codec).finish().toByteArray();
            // A frame cut short, and a frame with its body overwritten.
            byte[] cut = Arrays.copyOf(good, good.length - 4);
            Batch.putU32(cut, 8, cut.length - Batch.LEN_PREFIX);
            byte[] garbled = good.clone();
            Arrays.fill(garbled, Batch.HEADER_LEN + 6, garbled.length, (byte) 0x5a);
            for (byte[] bad : List.of(cut, garbled)) {
                Batch b = Batch.parse(ByteString.copyOf(sum(bad))).batch();
                assertEquals(Kind.MALFORMED, assertThrows(RecordFormatException.class, b::records).kind(), codec.toString());
            }
            // A plain block marked compressed is no frame at all.
            byte[] plain = large(Compression.NONE).finish().toByteArray();
            Batch.putU16(plain, 25, codec.code());
            Batch b = Batch.parse(ByteString.copyOf(sum(plain))).batch();
            assertEquals(Kind.MALFORMED, assertThrows(RecordFormatException.class, b::records).kind(), codec.toString());
        }
    }

    /** A batch whose block is {@code block}, compressed with {@code codec}, claiming one record. */
    private static byte[] withBlock(Compression codec, byte[] block) {
        byte[] head = new BatchBuilder().push(1, null, null).finish().toByteArray();
        byte[] out = Arrays.copyOf(head, Batch.HEADER_LEN + block.length);
        System.arraycopy(block, 0, out, Batch.HEADER_LEN, block.length);
        Batch.putU32(out, 8, out.length - Batch.LEN_PREFIX);
        Batch.putU16(out, 25, codec.code());
        return sum(out);
    }

    @Test
    void aBlockThatDecompressesPastTheLimitIsRefused() {
        byte[] atLimit = new byte[Batch.MAX_RECORDS_LEN];
        byte[] pastIt = new byte[Batch.MAX_RECORDS_LEN + 1];
        for (Compression codec : List.of(Compression.ZSTD, Compression.LZ4)) {
            byte[] big = withBlock(codec, Codecs.compress(codec, pastIt, 0, pastIt.length));
            assertTrue(big.length < Batch.MAX_BATCH_LEN, "a small batch: " + big.length);
            Batch b = Batch.parse(ByteString.copyOf(big)).batch();
            assertEquals(Kind.TOO_LARGE, assertThrows(RecordFormatException.class, b::records).kind(), codec.toString());
            // Exactly at the limit is decompressed; zeros are no valid records, which is refused otherwise.
            Batch ok = Batch.parse(ByteString.copyOf(withBlock(codec, Codecs.compress(codec, atLimit, 0, atLimit.length))))
                    .batch();
            assertNotEquals(Kind.TOO_LARGE, assertThrows(RecordFormatException.class, ok::records).kind());
        }
    }

    @Test
    void theBuilderRefusesToCompressMoreThanTheLimit() {
        byte[] mib = new byte[1024 * 1024];
        BatchBuilder b = new BatchBuilder(new BatchOptions(new Attributes(Compression.ZSTD, false, false, BatchKind.DATA), 0, 0, -1));
        for (int i = 0; i < 16; i++) {
            b.push(TS, null, mib);
        }
        assertEquals(Kind.TOO_LARGE, assertThrows(RecordFormatException.class, b::finish).kind());
        // Plain, the same records are simply too large a batch.
        BatchBuilder p = new BatchBuilder();
        p.push(TS, null, mib);
        assertEquals(Kind.TOO_LARGE, assertThrows(RecordFormatException.class, p::finish).kind());
    }

    @Test
    void aMissingLibraryIsNamed() {
        RecordFormatException e = assertThrows(
                RecordFormatException.class,
                () -> Codecs.require("com.example.NoSuchCodec", Codecs.ZSTD_ARTIFACT, Compression.ZSTD));
        assertEquals(Kind.UNSUPPORTED_COMPRESSION, e.kind());
        assertTrue(e.getMessage().contains("add com.github.luben:zstd-jni"), e.getMessage());
        // Both libraries are here.
        Codecs.require(Codecs.ZSTD_CLASS, Codecs.ZSTD_ARTIFACT, Compression.ZSTD);
        Codecs.require(Codecs.LZ4_CLASS, Codecs.LZ4_ARTIFACT, Compression.LZ4);
    }
}

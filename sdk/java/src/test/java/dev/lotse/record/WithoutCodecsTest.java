package dev.lotse.record;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.RecordFormatException.Kind;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Runs only in {@code testWithoutCodecs}, with zstd-jni and lz4-java off the class path: compressed batches still
 * parse, and reading or building one fails with {@link Kind#UNSUPPORTED_COMPRESSION} naming the library, never with
 * a {@link NoClassDefFoundError}. Plain batches are unaffected.
 */
@Tag("without-codecs")
class WithoutCodecsTest {
    @Test
    void compressedBatchesAreRefusedWithTheLibraryToAdd() {
        for (String[] v : Vectors.lines("compressed.hex")) {
            Batch b = Batch.parse(ByteString.copyOf(Vectors.hex(v[1]))).batch();
            RecordFormatException e = assertThrows(RecordFormatException.class, b::records);
            assertEquals(Kind.UNSUPPORTED_COMPRESSION, e.kind(), v[0]);
            String artifact = b.attributes().compression() == Compression.LZ4 ? Codecs.LZ4_ARTIFACT : Codecs.ZSTD_ARTIFACT;
            assertTrue(e.getMessage().contains("add " + artifact), e.getMessage());
        }
        for (Compression codec : List.of(Compression.ZSTD, Compression.LZ4)) {
            BatchBuilder b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.DATA), 0, 0, -1));
            b.push(1, null, new byte[] {1});
            assertEquals(Kind.UNSUPPORTED_COMPRESSION, assertThrows(RecordFormatException.class, b::finish).kind());
        }
    }

    @Test
    void plainBatchesNeedNoCodec() {
        for (String[] v : Vectors.lines("batches.hex")) {
            Batch.parse(ByteString.copyOf(Vectors.hex(v[1]))).batch().records();
        }
    }
}

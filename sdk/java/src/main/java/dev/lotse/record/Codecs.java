package dev.lotse.record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * The zstd and LZ4 codecs of the record block (ADR 0058, ADR 0061).
 *
 * <p>Both come from libraries that are optional dependencies of this one: {@code com.github.luben:zstd-jni} and
 * {@code at.yawk.lz4:lz4-java}. A codec is used only once its library is found on the class path, so a client
 * that never meets a compressed batch needs neither; one that does gets {@link
 * RecordFormatException.Kind#UNSUPPORTED_COMPRESSION} with the library to add. The classes that call the libraries
 * are loaded only after that check, so a missing library never surfaces as a {@link NoClassDefFoundError}.
 *
 * <p>The block is one zstd frame or one LZ4 frame (the frame format, not the bare block format), as the broker
 * writes and reads it. A block decompresses to at most {@link Batch#MAX_RECORDS_LEN} bytes: decompression stops
 * one byte past it, so a small batch cannot make the client allocate without bound.
 */
final class Codecs {
    /** The zstd level the broker's own builder uses. */
    static final int ZSTD_LEVEL = 3;

    static final String ZSTD_CLASS = "com.github.luben.zstd.ZstdInputStream";
    static final String ZSTD_ARTIFACT = "com.github.luben:zstd-jni";
    static final String LZ4_CLASS = "net.jpountz.lz4.LZ4FrameInputStream";
    static final String LZ4_ARTIFACT = "at.yawk.lz4:lz4-java";

    private Codecs() {}

    /** Decompresses a record block. */
    static byte[] decompress(Compression codec, byte[] a, int off, int len) {
        InputStream in = new ByteArrayInputStream(a, off, len);
        try {
            InputStream decoded = switch (codec) {
                case NONE -> in;
                case ZSTD -> {
                    require(ZSTD_CLASS, ZSTD_ARTIFACT, codec);
                    yield ZstdCodec.decoder(in);
                }
                case LZ4 -> {
                    require(LZ4_CLASS, LZ4_ARTIFACT, codec);
                    yield Lz4Codec.decoder(in);
                }
            };
            try (decoded) {
                return readBounded(decoded, codec);
            }
        } catch (IOException | RuntimeException e) {
            if (e instanceof RecordFormatException r) {
                throw r;
            }
            throw RecordFormatException.malformed(codec + " block does not decode: " + e.getMessage());
        } catch (LinkageError e) {
            // zstd-jni loads a native library; a platform it has none for ends here.
            throw unsupported(codec, artifact(codec) + " cannot be loaded here: " + e);
        }
    }

    /** Compresses a record block. */
    static byte[] compress(Compression codec, byte[] a, int off, int len) {
        try {
            return switch (codec) {
                case NONE -> java.util.Arrays.copyOfRange(a, off, off + len);
                case ZSTD -> {
                    require(ZSTD_CLASS, ZSTD_ARTIFACT, codec);
                    yield ZstdCodec.compress(a, off, len);
                }
                case LZ4 -> {
                    require(LZ4_CLASS, LZ4_ARTIFACT, codec);
                    yield Lz4Codec.compress(a, off, len);
                }
            };
        } catch (IOException e) {
            throw RecordFormatException.malformed(codec + " compression failed: " + e.getMessage());
        } catch (LinkageError e) {
            throw unsupported(codec, artifact(codec) + " cannot be loaded here: " + e);
        }
    }

    private static byte[] readBounded(InputStream in, Compression codec) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[64 * 1024];
        long limit = (long) Batch.MAX_RECORDS_LEN + 1;
        long total = 0;
        while (total < limit) {
            int n = in.read(buf, 0, (int) Math.min(buf.length, limit - total));
            if (n < 0) {
                break;
            }
            out.write(buf, 0, n);
            total += n;
        }
        if (total > Batch.MAX_RECORDS_LEN) {
            throw new RecordFormatException(
                    RecordFormatException.Kind.TOO_LARGE,
                    codec + " block decompresses to more than " + Batch.MAX_RECORDS_LEN + " bytes");
        }
        return out.toByteArray();
    }

    /** Fails unless {@code className} can be loaded, naming the library that has it. */
    static void require(String className, String artifact, Compression codec) {
        try {
            Class.forName(className, false, Codecs.class.getClassLoader());
        } catch (ClassNotFoundException | LinkageError e) {
            throw unsupported(codec, "add " + artifact + " to the class path to read and write " + codec + " batches");
        }
    }

    private static String artifact(Compression codec) {
        return codec == Compression.LZ4 ? LZ4_ARTIFACT : ZSTD_ARTIFACT;
    }

    private static RecordFormatException unsupported(Compression codec, String detail) {
        return new RecordFormatException(
                RecordFormatException.Kind.UNSUPPORTED_COMPRESSION, "compression codec " + codec + ": " + detail);
    }

    /** Calls zstd-jni; loaded only once the library is known to be there. */
    private static final class ZstdCodec {
        static InputStream decoder(InputStream in) throws IOException {
            return new com.github.luben.zstd.ZstdInputStream(in);
        }

        static byte[] compress(byte[] a, int off, int len) {
            byte[] block = off == 0 && len == a.length ? a : java.util.Arrays.copyOfRange(a, off, off + len);
            return com.github.luben.zstd.Zstd.compress(block, ZSTD_LEVEL);
        }
    }

    /** Calls lz4-java; loaded only once the library is known to be there. */
    private static final class Lz4Codec {
        static InputStream decoder(InputStream in) throws IOException {
            return new net.jpountz.lz4.LZ4FrameInputStream(in);
        }

        static byte[] compress(byte[] a, int off, int len) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream(len / 2 + 64);
            // Independent 64 KiB blocks, as the broker writes them.
            try (net.jpountz.lz4.LZ4FrameOutputStream enc = new net.jpountz.lz4.LZ4FrameOutputStream(
                    out, net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB)) {
                enc.write(a, off, len);
            }
            return out.toByteArray();
        }
    }
}

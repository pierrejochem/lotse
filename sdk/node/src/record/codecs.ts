import { Compression, compressionName } from "./attributes.js";
import { RecordFormatError } from "./errors.js";

/**
 * zstd and lz4 are optional (ADR 0083, ADR 0061). Phase 1 reads and writes uncompressed batches only: a batch that
 * needs a codec parses (its header is checked), and reading or building its record block fails with
 * `UNSUPPORTED_COMPRESSION`, naming the codec and what is missing.
 */

function unsupported(codec: Compression, detail: string): RecordFormatError {
  return new RecordFormatError("UNSUPPORTED_COMPRESSION", `compression codec ${compressionName(codec)}: ${detail}`);
}

function missing(codec: Compression): RecordFormatError {
  const what = codec === Compression.LZ4 ? "an LZ4 frame" : "a zstd frame";
  return unsupported(
    codec,
    `this version of @lotse/client has no ${compressionName(codec)} support yet, so it cannot read or write ${what}; ` +
      "read the stream with uncompressed producers or wait for the optional codec",
  );
}

/** The decompressed record block of a compressed batch. */
export function decompress(codec: Compression, _block: Uint8Array): Uint8Array {
  throw missing(codec);
}

/** The compressed form of a record block. */
export function compress(codec: Compression, _block: Uint8Array): Uint8Array {
  throw missing(codec);
}

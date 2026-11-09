import zlib from "node:zlib";
import { Compression, compressionName } from "./attributes.js";
import { RecordFormatError } from "./errors.js";
import { compressFrame, decompressFrame } from "./lz4.js";

/**
 * zstd and lz4 record blocks (ADR 0058, ADR 0083, ADR 0085).
 *
 * - **lz4** is written out in `lz4.ts`: always available.
 * - **zstd** comes from `node:zlib` (`zstdCompressSync`, Node.js 22.15, 23.8 and 24 and later). It is looked up at
 *   every call, so a Node.js without it fails with `UNSUPPORTED_COMPRESSION` and says what to do, never with a
 *   `TypeError`. Anything else can be plugged in with `registerCodec`.
 *
 * A missing codec is not damage: it is reported as `UNSUPPORTED_COMPRESSION`, as the Java SDK does (ADR 0061).
 */

/** The record block limit once decompressed; the same as `MAX_RECORDS_LEN` in `batch.ts`. */
const MAX_RECORDS_LEN = 16 * 1024 * 1024;
const ZSTD_LEVEL = 3;

/** A codec an application supplies, for a Node.js whose `node:zlib` has no zstd. */
export interface BlockCodec {
  /** One whole frame in, one whole frame out. */
  compress(block: Uint8Array): Uint8Array;
  /** Must stop and throw once the output would pass `maxOutput` bytes. The caller refuses any output longer than that. */
  decompress(block: Uint8Array, maxOutput: number): Uint8Array;
}

const registered = new Map<Compression, BlockCodec>();

/** Uses `codec` for `which` instead of the built-in one; `undefined` goes back to the built-in. */
export function registerCodec(which: Compression, codec: BlockCodec | undefined): void {
  if (which === Compression.NONE) {
    throw new RangeError("there is nothing to register for uncompressed blocks");
  }
  if (codec === undefined) {
    registered.delete(which);
  } else {
    registered.set(which, codec);
  }
}

interface ZstdZlib {
  zstdCompressSync?: (b: Uint8Array, o?: object) => Buffer;
  zstdDecompressSync?: (b: Uint8Array, o?: object) => Buffer;
}

function zstdZlib(): ZstdZlib {
  return zlib as unknown as ZstdZlib;
}

/** Whether blocks of `which` can be read and written by this process right now. */
export function codecAvailable(which: Compression): boolean {
  if (which === Compression.NONE || registered.has(which)) {
    return true;
  }
  if (which === Compression.LZ4) {
    return true;
  }
  const z = zstdZlib();
  return typeof z.zstdCompressSync === "function" && typeof z.zstdDecompressSync === "function";
}

function unsupported(codec: Compression): RecordFormatError {
  const name = compressionName(codec);
  return new RecordFormatError(
    "UNSUPPORTED_COMPRESSION",
    `compression codec ${name}: this Node.js (${process.version}) has no zstd in node:zlib, so @lotse/client cannot read or write ` +
      "a zstd frame; use Node.js 22.15, 23.8 or later (24 is tested), or give the SDK a zstd of your own with registerCodec(Compression.ZSTD, ...)",
  );
}

function isTooLarge(e: unknown): boolean {
  return (e instanceof RangeError || e instanceof Error) && (e as { code?: unknown }).code === "ERR_BUFFER_TOO_LARGE";
}

/** The decompressed record block of a compressed batch: at most one byte more than the limit, which the caller refuses. */
export function decompress(codec: Compression, block: Uint8Array): Uint8Array {
  const maxOutput = MAX_RECORDS_LEN + 1;
  const custom = registered.get(codec);
  try {
    if (custom !== undefined) {
      return custom.decompress(block, maxOutput);
    }
    if (codec === Compression.LZ4) {
      return decompressFrame(block, MAX_RECORDS_LEN);
    }
    if (!codecAvailable(codec)) {
      throw unsupported(codec);
    }
    return (zstdZlib().zstdDecompressSync as NonNullable<ZstdZlib["zstdDecompressSync"]>)(block, {
      maxOutputLength: maxOutput,
    });
  } catch (e) {
    if (e instanceof RecordFormatError) {
      throw e;
    }
    if (isTooLarge(e)) {
      throw new RecordFormatError("TOO_LARGE", `block decompresses to more than ${MAX_RECORDS_LEN} bytes`);
    }
    throw RecordFormatError.malformed(`${compressionName(codec)} block does not decode: ${(e as Error).message}`);
  }
}

/** The compressed form of a record block: zstd at level 3, or LZ4 as a frame of independent 64 KiB blocks. */
export function compress(codec: Compression, block: Uint8Array): Uint8Array {
  const custom = registered.get(codec);
  if (custom !== undefined) {
    return custom.compress(block);
  }
  if (codec === Compression.LZ4) {
    return compressFrame(block);
  }
  if (!codecAvailable(codec)) {
    throw unsupported(codec);
  }
  const level = zlib.constants["ZSTD_c_compressionLevel" as keyof typeof zlib.constants] as number;
  return (zstdZlib().zstdCompressSync as NonNullable<ZstdZlib["zstdCompressSync"]>)(block, {
    params: { [level]: ZSTD_LEVEL },
  });
}

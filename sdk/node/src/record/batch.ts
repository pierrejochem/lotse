import { type Attributes, attributesFromBits, Compression } from "./attributes.js";
import { getI32, getI64, getU16, getU32, putI64, putU16, putU32 } from "./bytes.js";
import { decompress } from "./codecs.js";
import { crc32c } from "./crc32c.js";
import { RecordFormatError } from "./errors.js";
import type { BatchRecord, Header } from "./types.js";
import * as varint from "./varint.js";

/** The fixed header of a batch, in bytes. */
export const HEADER_LEN = 65;
/** Bytes before the part `batch_len` counts: base_offset (8) and batch_len (4). */
export const LEN_PREFIX = 12;
export const MAGIC = 1;
/** A batch is at most this long, compressed. */
export const MAX_BATCH_LEN = 1024 * 1024;
/** A record block is at most this long once decompressed. */
export const MAX_RECORDS_LEN = 16 * 1024 * 1024;
/** The checksum covers the batch from this byte on. */
export const CRC_START = 25;

const COMPACTED_BIT = 1 << 8;

/**
 * One validated batch: a view of its bytes, never a copy. Fields that are `u64` on the wire are `bigint` bit
 * patterns (see `unsigned.ts`); counts and lengths are `number`.
 */
export class Batch {
  private constructor(
    private readonly bytes: Uint8Array,
    readonly attributes: Attributes,
  ) {}

  /**
   * Reads the first batch of `buf` and returns it with the bytes after it. The checks run in the order of
   * `sdk/PROTOCOL.md`: prefix, length limit, minimum length, whole batch present, magic, checksum, attributes, record
   * count rules, timestamps, control batch size.
   */
  static parse(buf: Uint8Array): { batch: Batch; rest: Uint8Array } {
    const len = Batch.peekLen(buf);
    if (len < 0) {
      throw RecordFormatError.truncated();
    }
    if (len > MAX_BATCH_LEN) {
      throw new RecordFormatError("TOO_LARGE", `batch of ${len} bytes exceeds the limit`);
    }
    if (len < HEADER_LEN) {
      throw RecordFormatError.malformed("batch shorter than its header");
    }
    if (buf.length < len) {
      throw RecordFormatError.truncated();
    }
    const bytes = buf.subarray(0, len);
    const magic = bytes[20] as number;
    if (magic !== MAGIC) {
      throw new RecordFormatError("UNSUPPORTED_MAGIC", `unsupported batch format version ${magic}`);
    }
    const stored = getU32(bytes, 21);
    const computed = crc32c(bytes, CRC_START, len);
    if (stored !== computed) {
      throw new RecordFormatError(
        "CHECKSUM",
        `checksum mismatch: stored 0x${stored.toString(16).padStart(8, "0")}, computed 0x${computed
          .toString(16)
          .padStart(8, "0")}`,
      );
    }
    const attributes = attributesFromBits(getU16(bytes, 25));
    const batch = new Batch(bytes, attributes);
    const count = batch.recordCount;
    if (batch.isCompacted) {
      // Compaction keeps a batch's offsets and drops records from it, down to none (ADR 0067).
      if (attributes.control) {
        throw RecordFormatError.malformed("a control batch is never compacted");
      }
      if (count > batch.span) {
        throw RecordFormatError.malformed("more records than offsets");
      }
    } else {
      if (count === 0) {
        throw RecordFormatError.malformed("empty batch");
      }
      if (batch.lastOffsetDelta !== count - 1) {
        throw RecordFormatError.malformed("last_offset_delta does not match record_count");
      }
    }
    if (batch.maxTimestamp < batch.baseTimestamp) {
      throw RecordFormatError.malformed("max_ts before base_ts");
    }
    if (attributes.control && count !== 1) {
      throw RecordFormatError.malformed("control batch must hold one record");
    }
    return { batch, rest: buf.subarray(len) };
  }

  /** Reads consecutive batches, as in the records of a `Fetch` partition. A cut batch at the end is an error. */
  static parseAll(buf: Uint8Array): Batch[] {
    const out: Batch[] = [];
    let rest = buf;
    while (rest.length > 0) {
      const s = Batch.parse(rest);
      out.push(s.batch);
      rest = s.rest;
    }
    return out;
  }

  /**
   * The length a batch claims, prefix included, or -1 when `buf` is shorter than the 12-byte prefix. The length is
   * a `u32`, so the result may exceed 2^32 and is a `number` for that reason.
   */
  static peekLen(buf: Uint8Array): number {
    if (buf.length < LEN_PREFIX) {
      return -1;
    }
    return LEN_PREFIX + getU32(buf, 8);
  }

  /** The batch as stored and sent. */
  asBytes(): Uint8Array {
    return this.bytes;
  }

  get length(): number {
    return this.bytes.length;
  }

  get baseOffset(): bigint {
    return getI64(this.bytes, 0);
  }

  /** `base_offset + last_offset_delta`, wrapping like the broker's `u64`. */
  get lastOffset(): bigint {
    return BigInt.asIntN(64, this.baseOffset + BigInt(this.lastOffsetDelta));
  }

  get nextOffset(): bigint {
    return BigInt.asIntN(64, this.lastOffset + 1n);
  }

  get term(): bigint {
    return getI64(this.bytes, 12);
  }

  /** Whether compaction rewrote this batch (attribute bit 8, ADR 0067). */
  get isCompacted(): boolean {
    return (getU16(this.bytes, 25) & COMPACTED_BIT) !== 0;
  }

  /** The offsets the batch covers, whether records remain for all of them or not. */
  get span(): number {
    return this.lastOffsetDelta + 1;
  }

  get lastOffsetDelta(): number {
    return getU32(this.bytes, 27);
  }

  get baseTimestamp(): bigint {
    return getI64(this.bytes, 31);
  }

  get maxTimestamp(): bigint {
    return getI64(this.bytes, 39);
  }

  get producerId(): bigint {
    return getI64(this.bytes, 47);
  }

  get producerEpoch(): number {
    return getU16(this.bytes, 55);
  }

  get baseSequence(): number {
    return getI32(this.bytes, 57);
  }

  get recordCount(): number {
    return getU32(this.bytes, 61);
  }

  /**
   * Decodes the records. Throws `UNSUPPORTED_COMPRESSION` for a compressed batch whose codec this SDK does not have
   * (phase 1 has none).
   */
  records(): BatchRecord[] {
    const compacted = this.isCompacted;
    if (compacted && this.recordCount === 0) {
      // Compaction writes an emptied batch's block plain, as nothing, whatever its codec.
      return [];
    }
    let a: Uint8Array;
    let pos: number;
    let end: number;
    if (this.attributes.compression === Compression.NONE) {
      a = this.bytes;
      pos = HEADER_LEN;
      end = this.bytes.length;
    } else {
      a = decompress(this.attributes.compression, this.bytes.subarray(HEADER_LEN));
      if (a.length > MAX_RECORDS_LEN) {
        throw new RecordFormatError("TOO_LARGE", `block decompresses to more than ${MAX_RECORDS_LEN} bytes`);
      }
      pos = 0;
      end = a.length;
    }
    const baseOffset = this.baseOffset;
    const baseTs = this.baseTimestamp;
    const count = this.recordCount;
    const lastOffsetDelta = this.lastOffsetDelta;
    const out: BatchRecord[] = [];
    let lastDelta = -1;
    for (let expectedDelta = 0; expectedDelta < count; expectedDelta++) {
      const len = varint.getI64(a, pos, end);
      pos += len.length;
      if (len.value < 0n) {
        throw RecordFormatError.malformed("negative record length");
      }
      if (BigInt(end - pos) < len.value) {
        throw RecordFormatError.truncated();
      }
      const recEnd = pos + Number(len.value);
      const c = new Cursor(a, pos, recEnd);
      c.byte(); // attributes, reserved
      const tsDelta = c.i64();
      const offsetDelta = c.i32();
      if (compacted) {
        // Gaps where records went, but in order and inside the batch.
        if (offsetDelta <= lastDelta || offsetDelta > lastOffsetDelta) {
          throw RecordFormatError.malformed("record offset deltas out of order");
        }
        lastDelta = offsetDelta;
      } else if (offsetDelta !== expectedDelta) {
        throw RecordFormatError.malformed("record offset deltas are not consecutive");
      }
      const key = c.nullable();
      const value = c.nullable();
      const headerCount = c.i64();
      if (headerCount < 0n) {
        throw RecordFormatError.malformed("negative header count");
      }
      const headers: Header[] = [];
      for (let i = 0n; i < headerCount; i++) {
        const hk = c.nullable();
        if (hk === null) {
          throw RecordFormatError.malformed("null header key");
        }
        headers.push({ key: hk, value: c.nullable() });
      }
      if (c.pos !== recEnd) {
        throw RecordFormatError.malformed("trailing bytes in record");
      }
      out.push({
        offset: BigInt.asIntN(64, baseOffset + BigInt(offsetDelta)),
        timestamp: BigInt.asIntN(64, baseTs + tsDelta),
        key,
        value,
        headers,
      });
      pos = recEnd;
    }
    if (pos !== end) {
      throw RecordFormatError.malformed("trailing bytes after records");
    }
    return out;
  }

  // ---- editing encoded batches in place ----

  /** Sets `base_offset` and `term`, as the leader does. Both lie before the checksummed range. */
  static stamp(batch: Uint8Array, baseOffset: bigint, term: bigint): void {
    if (batch.length < HEADER_LEN) {
      throw RecordFormatError.truncated();
    }
    putI64(batch, 0, baseOffset);
    putI64(batch, 12, term);
  }

  /** Moves `base_ts` and `max_ts` together, as the leader does when it stamps a TTL, and recomputes the checksum. */
  static setBaseTimestamp(batch: Uint8Array, baseTs: bigint): void {
    const len = wholeLength(batch);
    const oldBase = getI64(batch, 31);
    const oldMax = getI64(batch, 39);
    putI64(batch, 31, baseTs);
    putI64(batch, 39, baseTs + (oldMax - oldBase));
    resum(batch, len);
  }

  /** Sets the producer fields of an idempotent or transactional batch and recomputes the checksum. */
  static setProducer(batch: Uint8Array, producerId: bigint, producerEpoch: number, baseSequence: number): void {
    if (!Number.isInteger(producerEpoch) || producerEpoch < 0 || producerEpoch > 0xffff) {
      throw new RangeError(`producer epoch is not a u16: ${producerEpoch}`);
    }
    if (!Number.isInteger(baseSequence) || baseSequence < -2147483648 || baseSequence > 2147483647) {
      throw new RangeError(`base sequence is not an i32: ${baseSequence}`);
    }
    const len = wholeLength(batch);
    putI64(batch, 47, producerId);
    putU16(batch, 55, producerEpoch);
    putU32(batch, 57, baseSequence);
    resum(batch, len);
  }

  /** Sets or clears the transactional bit and recomputes the checksum. */
  static setTransactional(batch: Uint8Array, transactional: boolean): void {
    const len = wholeLength(batch);
    let bits = getU16(batch, 25);
    bits = transactional ? bits | (1 << 3) : bits & ~(1 << 3);
    putU16(batch, 25, bits);
    resum(batch, len);
  }
}

function wholeLength(batch: Uint8Array): number {
  const len = Batch.peekLen(batch);
  if (len < 0 || len < HEADER_LEN || batch.length < len) {
    throw RecordFormatError.truncated();
  }
  return len;
}

function resum(batch: Uint8Array, len: number): void {
  putU32(batch, 21, crc32c(batch, CRC_START, len));
}

/** The checksum a batch of these bytes must carry. */
export function batchChecksum(batch: Uint8Array): number {
  return crc32c(batch, CRC_START, batch.length);
}

/** Reads the fields of one record. */
class Cursor {
  pos: number;

  constructor(
    private readonly a: Uint8Array,
    pos: number,
    private readonly end: number,
  ) {
    this.pos = pos;
  }

  byte(): number {
    if (this.pos >= this.end) {
      throw RecordFormatError.truncated();
    }
    return this.a[this.pos++] as number;
  }

  i64(): bigint {
    const d = varint.getI64(this.a, this.pos, this.end);
    this.pos += d.length;
    return d.value;
  }

  i32(): number {
    const d = varint.getI32(this.a, this.pos, this.end);
    this.pos += d.length;
    return d.value;
  }

  nullable(): Uint8Array | null {
    const len = this.i64();
    if (len === -1n) {
      return null;
    }
    if (len < 0n) {
      throw RecordFormatError.malformed("negative length");
    }
    if (BigInt(this.end - this.pos) < len) {
      throw RecordFormatError.truncated();
    }
    const n = Number(len);
    const out = this.a.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
}

import { type Attributes, attributesToBits, Compression, DATA_ATTRIBUTES } from "./attributes.js";
import { putI64, putU16, putU32, GrowableBytes } from "./bytes.js";
import { compress } from "./codecs.js";
import { CRC_START, HEADER_LEN, LEN_PREFIX, MAGIC, MAX_BATCH_LEN, MAX_RECORDS_LEN } from "./batch.js";
import { crc32c } from "./crc32c.js";
import { RecordFormatError } from "./errors.js";
import type { Header } from "./types.js";
import * as varint from "./varint.js";

/** Everything in a batch's header that is not about its records. A client sends `base_offset` and `term` as zero. */
export interface BatchOptions {
  readonly attributes: Attributes;
  readonly producerId: bigint;
  readonly producerEpoch: number;
  readonly baseSequence: number;
}

export const DEFAULT_BATCH_OPTIONS: BatchOptions = {
  attributes: DATA_ATTRIBUTES,
  producerId: 0n,
  producerEpoch: 0,
  baseSequence: -1,
};

/**
 * Builds the bytes of a batch. The first record's timestamp is `base_ts` and the largest is `max_ts`; nothing here
 * reads a clock.
 */
export class BatchBuilder {
  private readonly options: BatchOptions;
  private baseTs = 0n;
  private maxTs = 0n;
  private count = 0;
  private readonly body = new GrowableBytes(256);

  constructor(options: BatchOptions = DEFAULT_BATCH_OPTIONS) {
    if (!Number.isInteger(options.producerEpoch) || options.producerEpoch < 0 || options.producerEpoch > 0xffff) {
      throw new RangeError(`producer epoch is not a u16: ${options.producerEpoch}`);
    }
    this.options = options;
  }

  /** The number of records so far. */
  get size(): number {
    return this.count;
  }

  get isEmpty(): boolean {
    return this.count === 0;
  }

  /** The length of the batch as it stands, uncompressed. */
  get encodedLength(): number {
    return HEADER_LEN + this.body.size;
  }

  /** Adds a record. `null` and empty are different for keys, values and header values. */
  push(
    timestamp: bigint,
    key: Uint8Array | null,
    value: Uint8Array | null,
    headers: readonly Header[] = [],
  ): this {
    if (this.count === 0) {
      this.baseTs = timestamp;
      this.maxTs = timestamp;
    } else if (timestamp > this.maxTs) {
      this.maxTs = timestamp;
    }
    const rec = new GrowableBytes(16 + (key?.length ?? 0) + (value?.length ?? 0));
    rec.putByte(0); // attributes
    // The delta wraps like the broker's signed 64-bit subtraction.
    putVarint(rec, BigInt.asIntN(64, timestamp - this.baseTs));
    putVarint(rec, BigInt(this.count));
    putNullable(rec, key);
    putNullable(rec, value);
    putVarint(rec, BigInt(headers.length));
    for (const h of headers) {
      putNullable(rec, h.key);
      putNullable(rec, h.value);
    }
    putVarint(this.body, BigInt(rec.size));
    this.body.put(rec.bytes());
    this.count++;
    return this;
  }

  /** The finished batch with `base_offset` and `term` zero. Throws if empty, over the limit, or needing a codec. */
  finish(): Uint8Array {
    if (this.count === 0) {
      throw RecordFormatError.malformed("empty batch");
    }
    const codec = this.options.attributes.compression;
    let block = this.body.bytes();
    if (codec !== Compression.NONE) {
      if (block.length > MAX_RECORDS_LEN) {
        throw new RecordFormatError(
          "TOO_LARGE",
          `records of ${block.length} bytes exceed what a compressed batch may hold`,
        );
      }
      block = compress(codec, block);
    }
    const total = HEADER_LEN + block.length;
    if (total > MAX_BATCH_LEN) {
      throw new RecordFormatError("TOO_LARGE", `batch of ${total} bytes exceeds the limit`);
    }
    const buf = new Uint8Array(total);
    buf.set(block, HEADER_LEN);
    putU32(buf, 8, total - LEN_PREFIX);
    buf[20] = MAGIC;
    putU16(buf, 25, attributesToBits(this.options.attributes));
    putU32(buf, 27, this.count - 1);
    putI64(buf, 31, this.baseTs);
    putI64(buf, 39, this.maxTs);
    putI64(buf, 47, this.options.producerId);
    putU16(buf, 55, this.options.producerEpoch);
    putU32(buf, 57, this.options.baseSequence);
    putU32(buf, 61, this.count);
    putU32(buf, 21, crc32c(buf, CRC_START, buf.length));
    return buf;
  }
}

function putVarint(buf: GrowableBytes, v: bigint): void {
  buf.putWith(varint.MAX_LEN_64, (dst, off) => varint.putI64(dst, off, v));
}

function putNullable(buf: GrowableBytes, v: Uint8Array | null): void {
  if (v === null) {
    putVarint(buf, -1n);
  } else {
    putVarint(buf, BigInt(v.length));
    buf.put(v);
  }
}

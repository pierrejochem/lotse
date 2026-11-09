import { RecordFormatError } from "../record/errors.js";
import * as varint from "../record/varint.js";
import { DecodeError } from "./error.js";

const NULL_STRING = 0xffff;
const UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/** A tagged field: a number and opaque bytes. */
export interface Tag {
  readonly tag: number;
  readonly value: Uint8Array;
}

/**
 * Reads the field types of the native protocol from a frame. Every read checks the bytes that are left and throws a
 * `DecodeError` of kind `TRUNCATED` or `MALFORMED`. 64-bit reads are `bigint` bit patterns, as in `record/unsigned.ts`.
 */
export class Reader {
  private readonly buf: Uint8Array;
  private readonly view: DataView;
  private readonly end: number;
  private pos: number;

  constructor(frame: Uint8Array, offset = 0, length: number = frame.length - offset) {
    if (offset < 0 || length < 0 || offset + length > frame.length) {
      throw new RangeError("reader window outside the buffer");
    }
    this.buf = frame;
    this.view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength);
    this.pos = offset;
    this.end = offset + length;
  }

  get remaining(): number {
    return this.end - this.pos;
  }

  /** Fails if bytes are left: a frame is read to its end and no further. */
  finish(): void {
    if (this.pos !== this.end) {
      throw DecodeError.malformed("trailing bytes");
    }
  }

  private need(n: number): void {
    if (this.end - this.pos < n) {
      throw DecodeError.truncated();
    }
  }

  u8(): number {
    this.need(1);
    return this.buf[this.pos++] as number;
  }

  bool(): boolean {
    return this.u8() !== 0;
  }

  u16(): number {
    this.need(2);
    const v = this.view.getUint16(this.pos, true);
    this.pos += 2;
    return v;
  }

  u32(): number {
    this.need(4);
    const v = this.view.getUint32(this.pos, true);
    this.pos += 4;
    return v;
  }

  i32(): number {
    this.need(4);
    const v = this.view.getInt32(this.pos, true);
    this.pos += 4;
    return v;
  }

  /** A `u64` as its signed bit pattern: `u64::MAX` is `-1n`. */
  u64(): bigint {
    return this.i64();
  }

  i64(): bigint {
    this.need(8);
    const v = this.view.getBigInt64(this.pos, true);
    this.pos += 8;
    return v;
  }

  /** A ZigZag varint. A short varint and an overlong one are both "truncated" in a frame, as in the broker. */
  varint(): bigint {
    try {
      const d = varint.getI64(this.buf, this.pos, this.end);
      this.pos += d.length;
      return d.value;
    } catch (e) {
      if (e instanceof RecordFormatError) {
        throw DecodeError.truncated();
      }
      throw e;
    }
  }

  /** An array count, checked against the bytes left before anything is allocated. */
  private count(minSize: number): number {
    const n = this.varint();
    if (n < 0n) {
      throw DecodeError.malformed("negative count");
    }
    if (n > BigInt(Math.floor(this.remaining / Math.max(minSize, 1)))) {
      throw DecodeError.malformed("count exceeds the frame");
    }
    return Number(n);
  }

  string(): string {
    const s = this.optString();
    if (s === null) {
      throw DecodeError.malformed("null string");
    }
    return s;
  }

  optString(): string | null {
    const len = this.u16();
    if (len === NULL_STRING) {
      return null;
    }
    this.need(len);
    try {
      const s = UTF8.decode(this.buf.subarray(this.pos, this.pos + len));
      this.pos += len;
      return s;
    } catch {
      throw DecodeError.malformed("string is not UTF-8");
    }
  }

  /** A `u32` length and that many bytes, as a view of the frame (copy it to keep it). */
  bytes(): Uint8Array {
    const len = this.u32();
    this.need(len);
    const out = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }

  /** `minSize` is the fewest bytes one element can take; it bounds the count. */
  array<T>(minSize: number, f: (r: Reader) => T): T[] {
    const n = this.count(minSize);
    const out: T[] = [];
    for (let i = 0; i < n; i++) {
      out.push(f(this));
    }
    return out;
  }

  tags(): Tag[] {
    return this.array(5, (r) => {
      const tag = r.varint();
      if (tag < 0n) {
        throw DecodeError.malformed("negative tag");
      }
      return { tag: Number(tag), value: r.bytes() };
    });
  }
}

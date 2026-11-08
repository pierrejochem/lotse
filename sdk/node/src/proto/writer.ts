import { putI64 as putVarintI64, MAX_LEN_64 } from "../record/varint.js";
import { U16_MAX, U32_MAX, U8_MAX } from "../record/unsigned.js";
import type { Tag } from "./reader.js";

const NULL_STRING = 0xffff;
const UTF8 = new TextEncoder();

/** True if the string holds a lone surrogate, which `TextEncoder` would silently replace. */
function hasLoneSurrogate(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = s.charCodeAt(i + 1);
      if (d >= 0xdc00 && d <= 0xdfff) {
        i++;
        continue;
      }
      return true;
    }
    if (c >= 0xdc00 && c <= 0xdfff) {
      return true;
    }
  }
  return false;
}

/** Writes the field types of the native protocol. Values out of range throw a `RangeError`. */
export class Writer {
  private buf = new Uint8Array(64);
  private view = new DataView(this.buf.buffer);
  private size = 0;

  toBytes(): Uint8Array {
    return this.buf.slice(0, this.size);
  }

  private reserve(extra: number): void {
    const needed = this.size + extra;
    if (needed > this.buf.length) {
      const grown = new Uint8Array(Math.max(needed, this.buf.length * 2));
      grown.set(this.buf.subarray(0, this.size));
      this.buf = grown;
      this.view = new DataView(grown.buffer);
    }
  }

  private checkInt(v: number, min: number, max: number, what: string): void {
    if (!Number.isInteger(v) || v < min || v > max) {
      throw new RangeError(`not ${what}: ${v}`);
    }
  }

  u8(v: number): this {
    this.checkInt(v, 0, U8_MAX, "a u8");
    this.reserve(1);
    this.buf[this.size++] = v;
    return this;
  }

  bool(v: boolean): this {
    return this.u8(v ? 1 : 0);
  }

  u16(v: number): this {
    this.checkInt(v, 0, U16_MAX, "a u16");
    this.reserve(2);
    this.view.setUint16(this.size, v, true);
    this.size += 2;
    return this;
  }

  u32(v: number): this {
    this.checkInt(v, 0, U32_MAX, "a u32");
    this.reserve(4);
    this.view.setUint32(this.size, v, true);
    this.size += 4;
    return this;
  }

  i32(v: number): this {
    this.checkInt(v, -2147483648, 2147483647, "an i32");
    this.reserve(4);
    this.view.setInt32(this.size, v, true);
    this.size += 4;
    return this;
  }

  /** A `u64`: any bigint from -2^63 to 2^64 - 1, written as its low 64 bits (so `-1n` is `u64::MAX`). */
  u64(v: bigint): this {
    if (typeof v !== "bigint" || v < -(1n << 63n) || v >= 1n << 64n) {
      throw new RangeError(`not a u64: ${String(v)}`);
    }
    this.reserve(8);
    this.view.setBigUint64(this.size, BigInt.asUintN(64, v), true);
    this.size += 8;
    return this;
  }

  i64(v: bigint): this {
    if (typeof v !== "bigint" || v < -(1n << 63n) || v >= 1n << 63n) {
      throw new RangeError(`not an i64: ${String(v)}`);
    }
    return this.u64(v);
  }

  /** A ZigZag varint. */
  varint(v: bigint | number): this {
    this.reserve(MAX_LEN_64);
    this.size += putVarintI64(this.buf, this.size, BigInt(v));
    return this;
  }

  string(s: string): this {
    if (typeof s !== "string") {
      throw new TypeError("null string where the protocol needs one; use optString");
    }
    if (hasLoneSurrogate(s)) {
      throw new RangeError("string is not valid Unicode");
    }
    const utf8 = UTF8.encode(s);
    if (utf8.length >= NULL_STRING) {
      throw new RangeError(`a string of at most 65,534 bytes, got ${utf8.length}`);
    }
    this.u16(utf8.length);
    this.reserve(utf8.length);
    this.buf.set(utf8, this.size);
    this.size += utf8.length;
    return this;
  }

  optString(s: string | null | undefined): this {
    if (s === null || s === undefined) {
      return this.u16(NULL_STRING);
    }
    return this.string(s);
  }

  bytes(b: Uint8Array): this {
    this.u32(b.length);
    this.reserve(b.length);
    this.buf.set(b, this.size);
    this.size += b.length;
    return this;
  }

  array<T>(items: readonly T[], f: (w: Writer, item: T) => void): this {
    this.varint(items.length);
    for (const item of items) {
      f(this, item);
    }
    return this;
  }

  tags(tags: readonly Tag[]): this {
    this.varint(tags.length);
    for (const t of tags) {
      this.varint(t.tag);
      this.bytes(t.value);
    }
    return this;
  }

  noTags(): this {
    return this.varint(0);
  }
}

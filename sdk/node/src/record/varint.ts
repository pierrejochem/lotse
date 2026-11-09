import { RecordFormatError } from "./errors.js";

/** ZigZag and LEB128 varints, up to 64 bits. */

export const MAX_LEN_64 = 10;

export interface Decoded {
  readonly value: bigint;
  readonly length: number;
}

const MASK64 = 0xffff_ffff_ffff_ffffn;

export function zigzag(value: bigint): bigint {
  return BigInt.asUintN(64, (value << 1n) ^ (value >> 63n));
}

export function unzigzag(value: bigint): bigint {
  const v = BigInt.asUintN(64, value);
  return BigInt.asIntN(64, (v >> 1n) ^ -(v & 1n));
}

/** Writes the plain LEB128 form of a `u64` at `off` and returns the number of bytes. */
export function putU64(dst: Uint8Array, off: number, value: bigint): number {
  let v = value & MASK64;
  let n = 0;
  while (v >= 0x80n) {
    dst[off + n++] = Number(v & 0x7fn) | 0x80;
    v >>= 7n;
  }
  dst[off + n++] = Number(v);
  return n;
}

/** Writes a ZigZag varint of an `i64`. */
export function putI64(dst: Uint8Array, off: number, value: bigint): number {
  return putU64(dst, off, zigzag(value));
}

/** The bytes of a ZigZag varint. */
export function encodeI64(value: bigint): Uint8Array {
  const buf = new Uint8Array(MAX_LEN_64);
  return buf.subarray(0, putI64(buf, 0, value));
}

/** Reads a plain LEB128 `u64` from `buf[off, end)`. Overlong forms such as `80 00` are accepted, as the broker does. */
export function getU64(buf: Uint8Array, off: number, end: number): Decoded {
  let value = 0n;
  const available = Math.min(MAX_LEN_64, end - off);
  for (let i = 0; i < available; i++) {
    const b = buf[off + i] as number;
    if (i === MAX_LEN_64 - 1 && b > 0x01) {
      throw new RecordFormatError("VARINT_OVERFLOW", "varint overflow");
    }
    value |= BigInt(b & 0x7f) << BigInt(7 * i);
    if ((b & 0x80) === 0) {
      return { value, length: i + 1 };
    }
  }
  if (end - off < MAX_LEN_64) {
    throw RecordFormatError.truncated();
  }
  throw new RecordFormatError("VARINT_OVERFLOW", "varint overflow");
}

/** Reads a ZigZag `i64`. */
export function getI64(buf: Uint8Array, off: number, end: number): Decoded {
  const raw = getU64(buf, off, end);
  return { value: unzigzag(raw.value), length: raw.length };
}

/** Reads a ZigZag `i32`; a value outside that range is an overflow. */
export function getI32(buf: Uint8Array, off: number, end: number): { value: number; length: number } {
  const v = getI64(buf, off, end);
  if (v.value < -2147483648n || v.value > 2147483647n) {
    throw new RecordFormatError("VARINT_OVERFLOW", "varint overflow");
  }
  return { value: Number(v.value), length: v.length };
}

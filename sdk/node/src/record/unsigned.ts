/**
 * 64-bit values are `bigint`. As in the Java SDK, a `u64` travels as its bit pattern in a signed 64-bit value, so
 * `NO_OFFSET` (`u64::MAX`) is `-1n`. Compare, add and print offsets with the helpers here, never with `<` or
 * `String`, whenever a value may be that large.
 */

export const U8_MAX = 0xff;
export const U16_MAX = 0xffff;
export const U32_MAX = 0xffff_ffff;

/** The signed 64-bit view of a `u64` bit pattern. */
export function asI64(v: bigint): bigint {
  return BigInt.asIntN(64, v);
}

/** The unsigned value of a `u64` bit pattern. */
export function asU64(v: bigint): bigint {
  return BigInt.asUintN(64, v);
}

/** Orders two `u64` bit patterns as unsigned numbers. */
export function compareU64(a: bigint, b: bigint): -1 | 0 | 1 {
  const x = asU64(a);
  const y = asU64(b);
  return x < y ? -1 : x > y ? 1 : 0;
}

/** Prints a `u64` bit pattern in decimal. */
export function u64ToString(v: bigint): string {
  return asU64(v).toString();
}

/** `a + b` for `u64` values; throws when the sum does not fit. */
export function addU64Exact(a: bigint, b: bigint): bigint {
  const sum = asU64(a) + asU64(b);
  if (sum > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError("u64 overflow");
  }
  return asI64(sum);
}

export function fitsU32(v: number): boolean {
  return Number.isInteger(v) && v >= 0 && v <= U32_MAX;
}

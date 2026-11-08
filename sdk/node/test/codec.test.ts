import assert from "node:assert/strict";
import { test } from "node:test";
import { DecodeError, Reader, Writer } from "../src/proto/index.js";

function kind(f: () => unknown): string {
  try {
    f();
  } catch (e) {
    assert.ok(e instanceof DecodeError, String(e));
    return e.kind;
  }
  assert.fail("expected a DecodeError");
}

test("fixed-width integers are little-endian", () => {
  const b = new Writer().u16(0x0102).u32(0x03040506).u64(0x0708090a0b0c0d0en).i32(-2).toBytes();
  assert.deepEqual([...b], [2, 1, 6, 5, 4, 3, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 9, 8, 7, 254, 255, 255, 255]);
  const r = new Reader(b);
  assert.equal(r.u16(), 0x0102);
  assert.equal(r.u32(), 0x03040506);
  assert.equal(r.u64(), 0x0708090a0b0c0d0en);
  assert.equal(r.i32(), -2);
  r.finish();
});

test("unsigned ranges are checked", () => {
  assert.throws(() => new Writer().u8(256), RangeError);
  assert.throws(() => new Writer().u8(-1), RangeError);
  assert.throws(() => new Writer().u8(1.5), RangeError);
  assert.throws(() => new Writer().u16(65536), RangeError);
  assert.throws(() => new Writer().u32(2 ** 32), RangeError);
  assert.throws(() => new Writer().u32(-1), RangeError);
  assert.throws(() => new Writer().u64(1n << 64n), RangeError);
  assert.equal(new Reader(new Writer().u32(0xffff_ffff).toBytes()).u32(), 0xffff_ffff);
});

test("a u64 above 2^53 keeps every bit, and u64::MAX is -1n", () => {
  const big = 9007199254740993n; // 2^53 + 1
  assert.equal(new Reader(new Writer().u64(big).toBytes()).u64(), big);
  const max = new Writer().u64(0xffff_ffff_ffff_ffffn).toBytes();
  assert.deepEqual([...max], [255, 255, 255, 255, 255, 255, 255, 255]);
  assert.equal(new Reader(max).u64(), -1n);
  assert.deepEqual([...new Writer().u64(-1n).toBytes()], [...max]);
});

test("strings are length-prefixed UTF-8 with a null marker", () => {
  const b = new Writer().string("päckchen").optString(null).optString("").toBytes();
  assert.deepEqual([...b], [9, 0, 112, 0xc3, 0xa4, 99, 107, 99, 104, 101, 110, 255, 255, 0, 0]);
  const r = new Reader(b);
  assert.equal(r.string(), "päckchen");
  assert.equal(r.optString(), null);
  assert.equal(r.string(), "");
});

test("a null string where one is required is malformed", () => {
  assert.equal(kind(() => new Reader(new Uint8Array([255, 255])).string()), "MALFORMED");
});

test("invalid UTF-8 is malformed", () => {
  assert.equal(kind(() => new Reader(new Uint8Array([2, 0, 0xc3, 0x28])).string()), "MALFORMED");
  // A lone surrogate half, encoded as UTF-8, is not UTF-8 either.
  assert.equal(kind(() => new Reader(new Uint8Array([3, 0, 0xed, 0xa0, 0x80])).string()), "MALFORMED");
});

test("a byte order mark is part of the string", () => {
  const r = new Reader(new Uint8Array([3, 0, 0xef, 0xbb, 0xbf]));
  assert.equal(r.string(), "﻿");
});

test("the longest string is 65,534 bytes", () => {
  const ok = "a".repeat(65534);
  assert.equal(new Reader(new Writer().string(ok).toBytes()).string(), ok);
  assert.throws(() => new Writer().string("a".repeat(65535)), RangeError);
  assert.throws(() => new Writer().string("\ud800"), RangeError);
  assert.throws(() => new Writer().string("a\udc00b"), RangeError);
  assert.equal(new Reader(new Writer().string("\u{1f600}").toBytes()).string(), "\u{1f600}");
});

test("bytes are length-prefixed by a u32", () => {
  const b = new Writer().bytes(new Uint8Array([1, 2, 3])).toBytes();
  assert.deepEqual([...b], [3, 0, 0, 0, 1, 2, 3]);
  assert.deepEqual([...new Reader(b).bytes()], [1, 2, 3]);
  // A length of 2^32 - 1 is a truncation, not a negative number.
  assert.equal(kind(() => new Reader(new Uint8Array([255, 255, 255, 255])).bytes()), "TRUNCATED");
});

test("counts are ZigZag varints", () => {
  const b = new Writer().array(["a", "b"], (w, s) => w.string(s)).toBytes();
  assert.equal(b[0], 4, "count 2 is ZigZag 4");
  assert.deepEqual(new Reader(b).array(2, (r) => r.string()), ["a", "b"]);
});

test("a negative count is malformed", () => {
  const b = new Writer().varint(-1).toBytes();
  assert.equal(kind(() => new Reader(b).array(1, (r) => r.u8())), "MALFORMED");
});

test("a count the frame cannot hold is refused before allocating", () => {
  const b = new Writer().varint((1n << 62n) - 1n).toBytes();
  assert.equal(kind(() => new Reader(b).array(1, (r) => r.u8())), "MALFORMED");
  const two = new Writer().varint(3).u8(1).u8(2).toBytes();
  assert.equal(kind(() => new Reader(two).array(1, (r) => r.u8())), "MALFORMED");
  assert.deepEqual(new Reader(new Writer().varint(2).u8(1).u8(2).toBytes()).array(1, (r) => r.u8()), [1, 2]);
});

test("an overlong varint is truncated", () => {
  assert.equal(kind(() => new Reader(new Uint8Array(11).fill(0xff)).varint()), "TRUNCATED");
});

test("tags are a count and tag-value pairs", () => {
  const b = new Writer().tags([{ tag: 0, value: new Uint8Array([7]) }]).toBytes();
  assert.deepEqual([...b], [2, 0, 1, 0, 0, 0, 7]);
  const tags = new Reader(b).tags();
  assert.equal(tags.length, 1);
  assert.equal(tags[0]?.tag, 0);
  const negative = new Writer().varint(1).varint(-1).u32(0).toBytes();
  assert.equal(kind(() => new Reader(negative).tags()), "MALFORMED");
});

test("finish refuses trailing bytes", () => {
  const r = new Reader(new Uint8Array([1, 2]));
  r.u8();
  assert.equal(kind(() => r.finish()), "MALFORMED");
});

test("a reader over a window of a larger buffer sees only the window", () => {
  const r = new Reader(new Uint8Array([9, 9, 1, 0, 9]), 2, 2);
  assert.equal(r.u16(), 1);
  r.finish();
});

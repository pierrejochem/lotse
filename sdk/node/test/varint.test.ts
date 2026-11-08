import assert from "node:assert/strict";
import { test } from "node:test";
import { RecordFormatError } from "../src/record/errors.js";
import * as varint from "../src/record/varint.js";
import { field, hex, lines, toHex } from "./vectors.js";

function encodeU64(v: bigint): Uint8Array {
  const buf = new Uint8Array(varint.MAX_LEN_64);
  return buf.subarray(0, varint.putU64(buf, 0, v));
}

function failure(input: Uint8Array): string {
  try {
    varint.getU64(input, 0, input.length);
  } catch (e) {
    assert.ok(e instanceof RecordFormatError);
    return e.kind;
  }
  assert.fail("expected a failure");
}

test("varints: the broker's vectors", () => {
  for (const v of lines("varints.txt")) {
    const bytes = hex(field(v, v.length - 1));
    const label = v.join(" ");
    switch (v[0]) {
      case "zigzag": {
        const value = BigInt(field(v, 1));
        assert.equal(toHex(varint.encodeI64(value)), toHex(bytes), label);
        const d = varint.getI64(bytes, 0, bytes.length);
        assert.equal(d.value, value, label);
        assert.equal(d.length, bytes.length, label);
        break;
      }
      case "unsigned": {
        const value = BigInt.asUintN(64, BigInt(field(v, 1)));
        assert.equal(toHex(encodeU64(value)), toHex(bytes), label);
        assert.equal(BigInt.asUintN(64, varint.getU64(bytes, 0, bytes.length).value), value, label);
        break;
      }
      case "lenient": {
        const d = varint.getU64(bytes, 0, bytes.length);
        assert.equal(BigInt.asUintN(64, d.value), BigInt(field(v, 1)), label);
        assert.equal(d.length, bytes.length, label);
        break;
      }
      case "bad":
        assert.equal(failure(bytes), v[1] === "truncated" ? "TRUNCATED" : "VARINT_OVERFLOW", label);
        break;
      default:
        assert.fail(`unknown vector kind ${v[0]}`);
    }
  }
});

test("varints: the tenth byte may only be one", () => {
  const ok = new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255, 255, 0x01]);
  assert.equal(BigInt.asUintN(64, varint.getU64(ok, 0, ok.length).value), 0xffff_ffff_ffff_ffffn);
  const tooBig = new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255, 255, 0x02]);
  assert.equal(failure(tooBig), "VARINT_OVERFLOW");
});

test("varints: eleven bytes are an overflow, not a truncation", () => {
  assert.equal(failure(new Uint8Array(11).fill(0xff)), "VARINT_OVERFLOW");
});

test("varints: a short open varint is truncated", () => {
  assert.equal(failure(new Uint8Array([0x80, 0x80])), "TRUNCATED");
  assert.equal(failure(new Uint8Array(0)), "TRUNCATED");
});

test("varints: an i32 must fit", () => {
  const big = varint.encodeI64(1n << 31n);
  assert.throws(
    () => varint.getI32(big, 0, big.length),
    (e: unknown) => e instanceof RecordFormatError && e.kind === "VARINT_OVERFLOW",
  );
  const min = varint.encodeI64(-2147483648n);
  assert.equal(varint.getI32(min, 0, min.length).value, -2147483648);
});

test("varints: ZigZag is its own inverse, at the edges of i64", () => {
  for (const v of [0n, -1n, 1n, (1n << 63n) - 1n, -(1n << 63n), 123456789n, -987654321n]) {
    assert.equal(varint.unzigzag(varint.zigzag(v)), v);
  }
});

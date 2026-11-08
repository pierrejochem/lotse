import assert from "node:assert/strict";
import { test } from "node:test";
import { type Attributes, attributesFromBits, attributesToBits, BatchKind, Compression } from "../src/record/attributes.js";
import { Batch, HEADER_LEN, LEN_PREFIX, MAX_BATCH_LEN } from "../src/record/batch.js";
import { BatchBuilder } from "../src/record/builder.js";
import { codecAvailable } from "../src/record/codecs.js";
import { putI64, putU16, putU32 } from "../src/record/bytes.js";
import { crc32c } from "../src/record/crc32c.js";
import { RecordFormatError, type RecordFormatKind } from "../src/record/errors.js";
import { u64ToString } from "../src/record/unsigned.js";
import { zigzag } from "../src/record/varint.js";

/** Each check of `Batch.parse` and `Batch.records` has a test that fails without it. */

function sample(): Uint8Array {
  const b = new BatchBuilder();
  b.push(1000n, new Uint8Array([1]), new Uint8Array([2, 3]));
  b.push(1001n, null, new Uint8Array([4]));
  return b.finish();
}

/** Recomputes the checksum, so a test changes one thing and the checksum is not what is refused. */
function sum(b: Uint8Array): Uint8Array {
  putU32(b, 21, crc32c(b, 25, b.length));
  return b;
}

function kindOf(f: () => unknown): RecordFormatKind {
  try {
    f();
  } catch (e) {
    assert.ok(e instanceof RecordFormatError, String(e));
    return e.kind;
  }
  assert.fail("expected a RecordFormatError");
}

const parseFails = (b: Uint8Array): RecordFormatKind => kindOf(() => Batch.parse(b));
const recordsFail = (b: Uint8Array): RecordFormatKind => {
  const batch = Batch.parse(b).batch;
  return kindOf(() => batch.records());
};

test("a valid batch parses", () => {
  const b = Batch.parse(sample()).batch;
  assert.equal(b.recordCount, 2);
  assert.equal(b.records().length, 2);
});

test("a flipped byte anywhere after the checksum field is refused", () => {
  const good = sample();
  for (let i = 25; i < good.length; i++) {
    const bad = good.slice();
    bad[i] = (bad[i] as number) ^ 0x01;
    assert.equal(parseFails(bad), "CHECKSUM", `byte ${i}`);
  }
});

test("a wrong stored checksum is refused", () => {
  const bad = sample();
  bad[21] = (bad[21] as number) ^ 0x01;
  assert.equal(parseFails(bad), "CHECKSUM");
});

test("the checksum does not cover base_offset and term", () => {
  const b = sample();
  b[3] = (b[3] as number) ^ 0x55;
  b[14] = (b[14] as number) ^ 0x55;
  Batch.parse(b);
});

test("a wrong magic is refused before the checksum", () => {
  const b = sample();
  b[20] = 2;
  assert.equal(parseFails(b), "UNSUPPORTED_MAGIC");
});

test("a buffer shorter than the prefix or the batch is truncated", () => {
  assert.equal(parseFails(new Uint8Array(11)), "TRUNCATED");
  const b = sample();
  assert.equal(parseFails(b.subarray(0, b.length - 1)), "TRUNCATED");
});

test("a length shorter than the header is malformed", () => {
  const b = sample();
  putU32(b, 8, 10);
  assert.equal(parseFails(b), "MALFORMED");
});

test("a length over the limit is refused without overflowing", () => {
  const b = sample();
  putU32(b, 8, MAX_BATCH_LEN - LEN_PREFIX + 1);
  assert.equal(parseFails(b), "TOO_LARGE");
  putU32(b, 8, 0xffff_ffff);
  assert.equal(parseFails(b), "TOO_LARGE");
  assert.equal(Batch.peekLen(b), 0xffff_ffff + 12);
});

test("attributes are validated", () => {
  const withBits = (bits: number): Uint8Array => {
    const b = sample();
    putU16(b, 25, bits);
    return sum(b);
  };
  assert.equal(parseFails(withBits(3)), "MALFORMED", "compression 3");
  assert.equal(parseFails(withBits(6 << 5)), "MALFORMED", "kind 6");
  assert.equal(parseFails(withBits(7 << 5)), "MALFORMED", "kind 7");
  assert.equal(parseFails(withBits(1 << 9)), "MALFORMED", "a reserved bit (bit 8 marks a compacted batch)");
  assert.equal(parseFails(withBits(1 << 15)), "MALFORMED", "the top bit");
});

test("every known attribute combination round-trips", () => {
  for (const compression of [Compression.NONE, Compression.ZSTD, Compression.LZ4]) {
    for (const kind of [0, 1, 2, 3, 4, 5] as const) {
      for (let flags = 0; flags < 4; flags++) {
        const a: Attributes = { compression, transactional: (flags & 1) !== 0, control: (flags & 2) !== 0, kind };
        assert.deepEqual(attributesFromBits(attributesToBits(a)), a);
      }
    }
  }
});

test("an empty record count is refused", () => {
  const b = sample();
  putU32(b, 61, 0);
  putU32(b, 27, 0xffff_ffff);
  assert.equal(parseFails(sum(b)), "MALFORMED");
});

test("last_offset_delta must match the record count", () => {
  const b = sample();
  putU32(b, 27, 5);
  assert.equal(parseFails(sum(b)), "MALFORMED");
});

test("max_ts before base_ts is refused", () => {
  const b = sample();
  putI64(b, 39, 999n);
  assert.equal(parseFails(sum(b)), "MALFORMED");
});

test("a control batch holds one record", () => {
  const b = new BatchBuilder({
    attributes: { compression: Compression.NONE, transactional: true, control: true, kind: BatchKind.DATA },
    producerId: 1n,
    producerEpoch: 0,
    baseSequence: -1,
  });
  b.push(1n, new Uint8Array(4), new Uint8Array(4));
  b.push(2n, new Uint8Array(4), new Uint8Array(4));
  assert.equal(parseFails(b.finish()), "MALFORMED");
});

test("records must have consecutive offset deltas", () => {
  const b = sample();
  // First record: length, attributes, ts_delta, offset_delta; offset_delta is the 3rd byte after the length.
  b[HEADER_LEN + 3] = Number(zigzag(1n));
  assert.equal(recordsFail(sum(b)), "MALFORMED");
});

test("bytes after the last record are refused", () => {
  const good = sample();
  const b = new Uint8Array(good.length + 1);
  b.set(good);
  putU32(b, 8, b.length - LEN_PREFIX);
  assert.equal(recordsFail(sum(b)), "MALFORMED");
});

test("bytes inside a record are refused", () => {
  const good = sample();
  // Grow the first record by one byte: its length varint says one more, and one more byte follows it.
  const recordLen = (good[HEADER_LEN] as number) >> 1; // the length is a one-byte ZigZag varint here
  const b = new Uint8Array(good.length + 1);
  b.set(good.subarray(0, HEADER_LEN + 1 + recordLen));
  b[HEADER_LEN] = Number(zigzag(BigInt(recordLen + 1)));
  b[HEADER_LEN + 1 + recordLen] = 0x7f;
  b.set(good.subarray(HEADER_LEN + 1 + recordLen), HEADER_LEN + 2 + recordLen);
  putU32(b, 8, b.length - LEN_PREFIX);
  assert.equal(recordsFail(sum(b)), "MALFORMED");
});

test("a record longer than the batch is truncated", () => {
  const b = sample();
  b[HEADER_LEN] = Number(zigzag(100n));
  assert.equal(recordsFail(sum(b)), "TRUNCATED");
});

test("a negative record length is refused", () => {
  const b = sample();
  b[HEADER_LEN] = Number(zigzag(-5n));
  assert.equal(recordsFail(sum(b)), "MALFORMED");
});

test("a compressed batch parses before its block is decoded", () => {
  // The header is checked without the codec; the block only when the records are read. A plain block marked as
  // compressed is no frame: damage where the codec is there, a missing codec where it is not.
  const b = sample();
  putU16(b, 25, 1);
  const batch = Batch.parse(sum(b)).batch;
  assert.equal(batch.attributes.compression, Compression.ZSTD);
  assert.equal(kindOf(() => batch.records()), codecAvailable(Compression.ZSTD) ? "MALFORMED" : "UNSUPPORTED_COMPRESSION");
  const lz4 = sample();
  putU16(lz4, 25, 2);
  assert.equal(kindOf(() => Batch.parse(sum(lz4)).batch.records()), "MALFORMED");
});

test("an empty builder and an oversized batch are refused", () => {
  assert.equal(kindOf(() => new BatchBuilder().finish()), "MALFORMED");
  const b = new BatchBuilder();
  b.push(1n, null, new Uint8Array(MAX_BATCH_LEN));
  assert.equal(kindOf(() => b.finish()), "TOO_LARGE");
});

test("edits refuse buffers that are not whole batches", () => {
  const b = sample();
  const cut = b.subarray(0, b.length - 1);
  assert.equal(kindOf(() => Batch.setProducer(cut, 1n, 1, 1)), "TRUNCATED");
  assert.equal(kindOf(() => Batch.setTransactional(cut, true)), "TRUNCATED");
  assert.equal(kindOf(() => Batch.setBaseTimestamp(cut, 1n)), "TRUNCATED");
  assert.equal(kindOf(() => Batch.stamp(new Uint8Array(10), 1n, 1n)), "TRUNCATED");
  assert.equal(Batch.peekLen(new Uint8Array(11)), -1);
});

test("a producer epoch and a sequence out of range are refused", () => {
  const b = sample();
  assert.throws(() => Batch.setProducer(b, 1n, 65536, 0), RangeError);
  assert.throws(() => Batch.setProducer(b, 1n, -1, 0), RangeError);
  assert.throws(() => Batch.setProducer(b, 1n, 0, 2 ** 31), RangeError);
  Batch.setProducer(b, 1n, 65535, -1);
  assert.equal(Batch.parse(b).batch.producerEpoch, 65535);
  assert.equal(Batch.parse(b).batch.baseSequence, -1);
});

test("null and empty are different in keys, values and headers", () => {
  const b = new BatchBuilder();
  b.push(5n, null, new Uint8Array(0), [{ key: new Uint8Array(0), value: null }]);
  const r = Batch.parse(b.finish()).batch.records()[0];
  assert.ok(r !== undefined);
  assert.equal(r.key, null);
  assert.equal(r.value?.length, 0);
  assert.equal(r.headers[0]?.value, null);
});

test("u64 offsets are unsigned and tolerate more than 2^53", () => {
  const b = sample();
  Batch.stamp(b, -2n, 1n); // 2^64 - 2
  const batch = Batch.parse(b).batch;
  assert.equal(u64ToString(batch.baseOffset), "18446744073709551614");
  assert.equal(batch.lastOffset, -1n);
  Batch.stamp(b, 9007199254740993n, 1n); // 2^53 + 1 is not a double
  assert.equal(Batch.parse(b).batch.baseOffset, 9007199254740993n);
  assert.equal(Batch.parse(b).batch.records()[1]?.offset, 9007199254740994n);
});

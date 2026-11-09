import assert from "node:assert/strict";
import { test } from "node:test";
import zlib from "node:zlib";
import { type Attributes, BatchKind, Compression } from "../src/record/attributes.js";
import { Batch, HEADER_LEN, LEN_PREFIX, MAX_BATCH_LEN, MAX_RECORDS_LEN } from "../src/record/batch.js";
import { equalBytes, putU16, putU32 } from "../src/record/bytes.js";
import { BatchBuilder } from "../src/record/builder.js";
import { type BlockCodec, codecAvailable, compress, decompress, registerCodec } from "../src/record/codecs.js";
import { crc32c } from "../src/record/crc32c.js";
import { RecordFormatError, type RecordFormatKind } from "../src/record/errors.js";
import type { BatchRecord } from "../src/record/types.js";
import { field, hex, lines, recordLine } from "./vectors.js";

/**
 * zstd and lz4 record blocks (ADR 0058, ADR 0085): the broker's compressed batches read like their plain twins, batches
 * compressed here read back, a block that does not decode or decodes to too much is refused, and a codec this Node.js
 * lacks is named. lz4 is written in the SDK and always runs; zstd needs `node:zlib` to have it (Node.js 22.15, 23.8, 24),
 * and its tests say so when they are skipped.
 */

const ZSTD_SKIP = codecAvailable(Compression.ZSTD) ? false : `SKIPPED: node ${process.version} has no zstd in node:zlib`;
const TS = 1_700_000_000_000n;
const CODECS: readonly (readonly [Compression, string, string | false])[] = [
  [Compression.ZSTD, "ZSTD", ZSTD_SKIP],
  [Compression.LZ4, "LZ4", false],
];

const attrs = (compression: Compression): Attributes => ({ compression, transactional: false, control: false, kind: BatchKind.DATA });
const options = (compression: Compression) => ({ attributes: attrs(compression), producerId: 9n, producerEpoch: 1, baseSequence: 4 });

function kindOf(f: () => unknown): RecordFormatKind | "NONE" {
  try {
    f();
    return "NONE";
  } catch (e) {
    if (e instanceof RecordFormatError) {
      return e.kind;
    }
    throw e;
  }
}

function sum(b: Uint8Array): Uint8Array {
  putU32(b, 21, crc32c(b, 25, b.length));
  return b;
}

const lines2 = (label: string, batch: Batch): string[] => batch.records().map((r, i) => recordLine(label, i, r));

test("compressed batches parse and their headers match their plain twins", () => {
  const twin: Record<string, string> = {
    "zstd-stamped": "data-stamped",
    "zstd-large": "data-large",
    "lz4-stamped": "data-stamped",
    "lz4-large": "data-large",
  };
  const plain = new Map(lines("batches.hex").map((v) => [field(v, 0), Batch.parse(hex(field(v, 1))).batch]));
  const compressed = lines("compressed.hex");
  assert.equal(compressed.length, 4);
  for (const v of compressed) {
    const label = field(v, 0);
    const b = Batch.parse(hex(field(v, 1))).batch;
    const o = plain.get(twin[label] as string);
    assert.ok(o !== undefined, label);
    assert.equal(b.attributes.compression, label.startsWith("zstd") ? Compression.ZSTD : Compression.LZ4, label);
    assert.equal(b.isCompacted, false, label);
    assert.equal(b.baseOffset, o.baseOffset, label);
    assert.equal(b.term, o.term, label);
    assert.equal(b.recordCount, o.recordCount, label);
    assert.equal(b.lastOffsetDelta, o.lastOffsetDelta, label);
    assert.equal(b.baseTimestamp, o.baseTimestamp, label);
    assert.equal(b.maxTimestamp, o.maxTimestamp, label);
    assert.equal(b.producerId, o.producerId, label);
    assert.equal(b.producerEpoch, o.producerEpoch, label);
    assert.equal(b.baseSequence, o.baseSequence, label);
    assert.equal(b.attributes.transactional, o.attributes.transactional, label);
    assert.equal(b.attributes.kind, o.attributes.kind, label);
  }
});

test("the broker's compressed batches read as written", () => {
  const expected = new Map<string, string[]>();
  for (const v of lines("compressed-records.txt")) {
    expected.set(field(v, 0), [...(expected.get(field(v, 0)) ?? []), v.join(" ")]);
  }
  let seen = 0;
  for (const v of lines("compressed.hex")) {
    const label = field(v, 0);
    seen++;
    const b = Batch.parse(hex(field(v, 1))).batch;
    if (label.startsWith("zstd") && ZSTD_SKIP !== false) {
      // Without zstd the failure is the named one, not damage.
      assert.equal(kindOf(() => b.records()), "UNSUPPORTED_COMPRESSION", label);
      continue;
    }
    assert.deepEqual(lines2(label, b), expected.get(label), label);
  }
  assert.equal(seen, expected.size, "every compressed vector has its records");
});

function large(codec: Compression): BatchBuilder {
  const b = new BatchBuilder(options(codec));
  b.push(TS, new Uint8Array(200).fill(0xab), new Uint8Array(20_000).fill(0xcd), [
    { key: Buffer.from("h"), value: Buffer.from("x") },
  ]);
  b.push(TS + 3n, null, null);
  b.push(-TS, Buffer.from("negative"), new Uint8Array(0));
  return b;
}

for (const [codec, name, skip] of CODECS) {
  test(`${name}: a batch compressed here reads like its plain twin and is much smaller`, { skip }, () => {
    const plain = Batch.parse(large(Compression.NONE).finish()).batch;
    const c = Batch.parse(large(codec).finish()).batch;
    assert.equal(c.attributes.compression, codec);
    assert.ok(c.length < plain.length / 10, `${c.length} of ${plain.length}`);
    assert.deepEqual(lines2("x", c), lines2("x", plain));
    assert.equal(c.recordCount, plain.recordCount);
    assert.equal(c.maxTimestamp, plain.maxTimestamp);
    assert.equal(c.producerId, plain.producerId);
  });

  test(`${name}: a block that does not decode is refused as malformed`, { skip }, () => {
    const good = large(codec).finish();
    // A frame cut short, and a frame with its body overwritten.
    const cut = good.slice(0, good.length - 4);
    putU32(cut, 8, cut.length - LEN_PREFIX);
    const garbled = good.slice();
    garbled.fill(0x5a, HEADER_LEN + 6);
    for (const bad of [cut, garbled]) {
      assert.equal(kindOf(() => Batch.parse(sum(bad)).batch.records()), "MALFORMED", name);
    }
    // A plain block marked compressed is no frame at all.
    const plain = large(Compression.NONE).finish();
    putU16(plain, 25, codec);
    assert.equal(kindOf(() => Batch.parse(sum(plain)).batch.records()), "MALFORMED", name);
  });

  /** A batch whose block is `block`, compressed with `codec`, claiming one record. */
  const withBlock = (block: Uint8Array): Uint8Array => {
    const head = new BatchBuilder().push(1n, null, null).finish();
    const out = new Uint8Array(HEADER_LEN + block.length);
    out.set(head.subarray(0, HEADER_LEN));
    out.set(block, HEADER_LEN);
    putU32(out, 8, out.length - LEN_PREFIX);
    putU16(out, 25, codec);
    return sum(out);
  };

  test(`${name}: a block that decompresses past the limit is refused, one at the limit is not for its size`, { skip }, () => {
    const pastIt = withBlock(compress(codec, new Uint8Array(MAX_RECORDS_LEN + 1)));
    assert.ok(pastIt.length < MAX_BATCH_LEN, `a small batch: ${pastIt.length}`);
    assert.equal(kindOf(() => Batch.parse(pastIt).batch.records()), "TOO_LARGE", name);
    // Exactly at the limit it is decompressed; zeros are no valid records, which is refused otherwise.
    const atLimit = withBlock(compress(codec, new Uint8Array(MAX_RECORDS_LEN)));
    const kind = kindOf(() => Batch.parse(atLimit).batch.records());
    assert.notEqual(kind, "TOO_LARGE");
    assert.notEqual(kind, "NONE");
  });

  test(`${name}: the codec itself stops at the limit instead of producing what a bomb holds`, { skip }, () => {
    const bomb = compress(codec, new Uint8Array(2 * MAX_RECORDS_LEN));
    assert.ok(bomb.length < 1_000_000, `a small frame: ${bomb.length}`);
    // The batch reads one byte past the limit and refuses; the codec must not hand back all 32 MiB first.
    assert.throws(() => decompress(codec, bomb), (e: unknown) => e instanceof RecordFormatError && e.kind === "TOO_LARGE");
    assert.equal(decompress(codec, compress(codec, new Uint8Array(1000))).length, 1000);
  });

  test(`${name}: a batch with every record kind round-trips`, { skip }, () => {
    const b = new BatchBuilder(options(codec));
    const r = (i: number): Uint8Array => Uint8Array.from({ length: 300 + i }, (_, k) => (k * 7 + i) & 255);
    for (let i = 0; i < 50; i++) {
      b.push(TS + BigInt(i), i % 3 === 0 ? null : r(i).subarray(0, 20), i % 5 === 0 ? null : r(i), [
        { key: r(i).subarray(0, 3), value: i % 2 === 0 ? null : r(i).subarray(0, 40) },
      ]);
    }
    const got: BatchRecord[] = Batch.parse(b.finish()).batch.records();
    assert.equal(got.length, 50);
    for (let i = 0; i < 50; i++) {
      assert.ok(equalBytes(got[i]?.value ?? null, i % 5 === 0 ? null : r(i)), `value ${i}`);
      assert.ok(equalBytes(got[i]?.key ?? null, i % 3 === 0 ? null : r(i).subarray(0, 20)), `key ${i}`);
    }
  });
}

test("the builder refuses to compress more than the limit", () => {
  const mib = new Uint8Array(1024 * 1024);
  const b = new BatchBuilder(options(Compression.LZ4));
  for (let i = 0; i < 16; i++) {
    b.push(TS, null, mib);
  }
  assert.equal(kindOf(() => b.finish()), "TOO_LARGE");
  // Plain, the same records are simply too large a batch.
  const p = new BatchBuilder();
  p.push(TS, null, mib);
  assert.equal(kindOf(() => p.finish()), "TOO_LARGE");
});

/** Runs `body` as a Node.js without zstd in `node:zlib` would see it, then restores what was there. */
function withoutZlibZstd(body: () => void): void {
  const z = zlib as unknown as Record<string, unknown>;
  const kept = { c: z["zstdCompressSync"], d: z["zstdDecompressSync"] };
  delete z["zstdCompressSync"];
  delete z["zstdDecompressSync"];
  try {
    body();
  } finally {
    z["zstdCompressSync"] = kept.c;
    z["zstdDecompressSync"] = kept.d;
  }
}

test("a Node.js without zstd names what is missing, and lz4 and plain batches still work", () => {
  const zstdBatch = Batch.parse(hex(field(lines("compressed.hex").find((v) => field(v, 0) === "zstd-stamped") as string[], 1))).batch;
  const lz4Batch = Batch.parse(hex(field(lines("compressed.hex").find((v) => field(v, 0) === "lz4-stamped") as string[], 1))).batch;
  withoutZlibZstd(() => {
    assert.equal(codecAvailable(Compression.ZSTD), false);
    assert.equal(codecAvailable(Compression.LZ4), true);
    assert.throws(
      () => zstdBatch.records(),
      (e: unknown) =>
        e instanceof RecordFormatError &&
        e.kind === "UNSUPPORTED_COMPRESSION" &&
        /ZSTD/.test(e.message) &&
        /node:zlib/.test(e.message) &&
        /22\.15/.test(e.message) &&
        /registerCodec/.test(e.message),
    );
    assert.throws(
      () => new BatchBuilder(options(Compression.ZSTD)).push(1n, null, new Uint8Array([1])).finish(),
      (e: unknown) => e instanceof RecordFormatError && e.kind === "UNSUPPORTED_COMPRESSION" && e.message.includes("ZSTD"),
    );
    assert.equal(lz4Batch.records().length, 3, "lz4 needs nothing");
    for (const v of lines("batches.hex")) {
      Batch.parse(hex(field(v, 1))).batch.records();
    }
  });
});

test("a codec the application registers is used, and removing it goes back to the built-in one", () => {
  const calls: string[] = [];
  const reversed: BlockCodec = {
    compress: (b) => {
      calls.push("compress");
      return Uint8Array.from(b).reverse();
    },
    decompress: (b, max) => {
      calls.push(`decompress ${max}`);
      return Uint8Array.from(b).reverse();
    },
  };
  withoutZlibZstd(() => {
    registerCodec(Compression.ZSTD, reversed);
    try {
      assert.equal(codecAvailable(Compression.ZSTD), true);
      const batch = new BatchBuilder(options(Compression.ZSTD)).push(5n, null, Buffer.from("own codec")).finish();
      const rec = Batch.parse(batch).batch.records()[0];
      assert.equal(Buffer.from(rec?.value as Uint8Array).toString(), "own codec");
      assert.deepEqual(calls, ["compress", `decompress ${MAX_RECORDS_LEN + 1}`]);
    } finally {
      registerCodec(Compression.ZSTD, undefined);
    }
    assert.equal(codecAvailable(Compression.ZSTD), false);
  });
  assert.throws(() => registerCodec(Compression.NONE, reversed), RangeError);
  // A codec that throws is damage, not a missing codec.
  registerCodec(Compression.LZ4, {
    compress: (b) => b,
    decompress: () => {
      throw new Error("boom");
    },
  });
  try {
    const b = Batch.parse(hex(field(lines("compressed.hex").find((v) => field(v, 0) === "lz4-stamped") as string[], 1))).batch;
    assert.equal(kindOf(() => b.records()), "MALFORMED");
  } finally {
    registerCodec(Compression.LZ4, undefined);
  }
});

test("plain batches need no codec", () => {
  for (const v of lines("batches.hex")) {
    Batch.parse(hex(field(v, 1))).batch.records();
  }
});

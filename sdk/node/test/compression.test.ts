import assert from "node:assert/strict";
import { test } from "node:test";
import { Compression } from "../src/record/attributes.js";
import { Batch } from "../src/record/batch.js";
import { BatchBuilder } from "../src/record/builder.js";
import { RecordFormatError } from "../src/record/errors.js";
import { field, hex, lines } from "./vectors.js";

/**
 * Phase 1 has no zstd and no lz4. Compressed batches still parse and their header agrees with the broker's; reading
 * or building a record block fails with `UNSUPPORTED_COMPRESSION`, naming the codec. Plain batches are unaffected.
 */

const twin: Record<string, string> = {
  "zstd-stamped": "data-stamped",
  "zstd-large": "data-large",
  "lz4-stamped": "data-stamped",
  "lz4-large": "data-large",
};

test("compressed batches parse and their headers match their plain twins", () => {
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

test("reading a compressed block names the codec that is missing", () => {
  for (const v of lines("compressed.hex")) {
    const label = field(v, 0);
    const b = Batch.parse(hex(field(v, 1))).batch;
    assert.throws(
      () => b.records(),
      (e: unknown) =>
        e instanceof RecordFormatError &&
        e.kind === "UNSUPPORTED_COMPRESSION" &&
        e.message.includes(label.startsWith("lz4") ? "LZ4" : "ZSTD"),
      label,
    );
  }
});

test("building a compressed batch is refused with the codec named", () => {
  for (const [codec, name] of [
    [Compression.ZSTD, "ZSTD"],
    [Compression.LZ4, "LZ4"],
  ] as const) {
    const b = new BatchBuilder({
      attributes: { compression: codec, transactional: false, control: false, kind: 0 },
      producerId: 0n,
      producerEpoch: 0,
      baseSequence: -1,
    });
    b.push(1n, null, new Uint8Array([1]));
    assert.throws(
      () => b.finish(),
      (e: unknown) => e instanceof RecordFormatError && e.kind === "UNSUPPORTED_COMPRESSION" && e.message.includes(name),
    );
  }
});

test("plain batches need no codec", () => {
  for (const v of lines("batches.hex")) {
    Batch.parse(hex(field(v, 1))).batch.records();
  }
});

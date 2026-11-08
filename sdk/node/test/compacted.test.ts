import assert from "node:assert/strict";
import { test } from "node:test";
import { Batch } from "../src/record/batch.js";
import { putU32 } from "../src/record/bytes.js";
import { crc32c } from "../src/record/crc32c.js";
import { RecordFormatError } from "../src/record/errors.js";
import { field, hex, lines, recordLine } from "./vectors.js";

/**
 * Batches compaction rewrote (ADR 0067): what a fetch of a compacted stream returns. They read with gaps between
 * their records' offsets, may hold no record at all, and keep every header field but the length, the checksum, the
 * record count and attribute bit 8.
 */

interface Vector {
  readonly count: number;
  readonly span: number;
  readonly bytes: Uint8Array;
}

function vectors(): Map<string, Vector> {
  return new Map(
    lines("compacted.hex").map((v) => [
      field(v, 0),
      { count: Number(field(v, 1)), span: Number(field(v, 2)), bytes: hex(field(v, 3)) },
    ]),
  );
}

function isUnsupportedCodec(e: unknown): boolean {
  return e instanceof RecordFormatError && e.kind === "UNSUPPORTED_COMPRESSION";
}

test("compacted batches read with their gaps", () => {
  const expected = new Map<string, string[]>();
  for (const v of lines("compacted-records.txt")) {
    const list = expected.get(field(v, 0)) ?? [];
    list.push(v.join(" "));
    expected.set(field(v, 0), list);
  }
  const all = vectors();
  assert.equal(all.size, 5);
  for (const [label, vec] of all) {
    const b = Batch.parse(vec.bytes).batch;
    assert.ok(b.isCompacted, label);
    assert.equal(b.recordCount, vec.count, label);
    assert.equal(b.span, vec.span, label);
    assert.equal(b.nextOffset, b.baseOffset + BigInt(b.span), label);
    if (b.attributes.compression !== 0 && vec.count > 0) {
      // The record block is one zstd or lz4 frame, which phase 1 cannot open.
      assert.throws(() => b.records(), isUnsupportedCodec, label);
      continue;
    }
    const got = b.records().map((r, i) => recordLine(label, i, r));
    assert.deepEqual(got, expected.get(label) ?? [], label);
  }
});

test("an emptied batch holds no block, whatever its codec", () => {
  const lz4Emptied = vectors().get("lz4-emptied");
  assert.ok(lz4Emptied !== undefined);
  const b = Batch.parse(lz4Emptied.bytes).batch;
  assert.equal(b.attributes.compression, 2);
  assert.deepEqual(b.records(), []);
});

test("the header of the original stays", () => {
  const originals = new Map<string, Uint8Array>();
  for (const v of [...lines("batches.hex"), ...lines("compressed.hex")]) {
    originals.set(field(v, 0), hex(field(v, 1)));
  }
  const from: Record<string, string> = {
    gap: "data-stamped",
    "last-only": "idempotent",
    emptied: "transactional",
    "zstd-gap": "zstd-stamped",
    "lz4-emptied": "lz4-large",
  };
  for (const [label, vec] of vectors()) {
    const c = Batch.parse(vec.bytes).batch;
    const o = Batch.parse(originals.get(from[label] as string) as Uint8Array).batch;
    assert.equal(o.isCompacted, false);
    assert.equal(c.baseOffset, o.baseOffset, label);
    assert.equal(c.lastOffsetDelta, o.lastOffsetDelta, label);
    assert.equal(c.term, o.term, label);
    assert.deepEqual(c.attributes, o.attributes, label);
    assert.equal(c.producerId, o.producerId, label);
    assert.equal(c.producerEpoch, o.producerEpoch, label);
    assert.equal(c.baseSequence, o.baseSequence, label);
    assert.equal(c.baseTimestamp, o.baseTimestamp, label);
    assert.equal(c.maxTimestamp, o.maxTimestamp, label);
  }
});

/** `b` with the checksum computed again. */
function resum(b: Uint8Array): Uint8Array {
  putU32(b, 21, crc32c(b, 25, b.length));
  return b;
}

test("only data batches are compacted, and never hold more records than offsets", () => {
  const batches = new Map(lines("batches.hex").map((v) => [field(v, 0), hex(field(v, 1))]));
  const marked = (b: Uint8Array): Uint8Array => {
    const out = b.slice();
    out[26] = (out[26] as number) | 1;
    return resum(out);
  };
  // A whole batch marked compacted still reads: nothing was left out.
  const whole = Batch.parse(marked(batches.get("data-stamped") as Uint8Array)).batch;
  assert.equal(whole.records().length, 3);
  assert.throws(() => Batch.parse(marked(batches.get("control-commit") as Uint8Array)), RecordFormatError);
  // More records than the batch spans.
  const over = marked(batches.get("data-stamped") as Uint8Array);
  over[27] = 1; // last_offset_delta 1: two offsets for three records
  assert.throws(() => Batch.parse(resum(over)), RecordFormatError);
  // Not compacted, the strict rules stay: an empty batch is refused.
  const emptied = vectors().get("emptied");
  assert.ok(emptied !== undefined);
  const plain = emptied.bytes.slice();
  plain[26] = (plain[26] as number) & ~1;
  assert.throws(() => Batch.parse(resum(plain)), RecordFormatError);
});

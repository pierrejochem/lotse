import assert from "node:assert/strict";
import { test } from "node:test";
import { Batch, HEADER_LEN } from "../src/record/batch.js";
import { BatchBuilder } from "../src/record/builder.js";
import { Compression, BatchKind } from "../src/record/attributes.js";
import { attributesToBits } from "../src/record/attributes.js";
import { getU32 } from "../src/record/bytes.js";
import { ControlKind, ControlType, controlKind, marker, readMarker } from "../src/record/control.js";
import { crc32c } from "../src/record/crc32c.js";
import { RecordFormatError } from "../src/record/errors.js";
import { u64ToString } from "../src/record/unsigned.js";
import { field, hex, lines, recordLine, toHex } from "./vectors.js";

/** Every batch the broker's code wrote is read here, checked field by field and written again byte for byte. */

function batches(): Map<string, Uint8Array> {
  return new Map(lines("batches.hex").map((v) => [field(v, 0), hex(field(v, 1))]));
}

function parse(bytes: Uint8Array): Batch {
  const s = Batch.parse(bytes);
  assert.equal(s.rest.length, 0);
  return s.batch;
}

test("batches: header fields match the broker's", () => {
  const expected = new Map<string, Map<string, string>>();
  for (const v of lines("batch-fields.txt")) {
    const m = new Map<string, string>();
    for (const kv of v.slice(1)) {
      const [k, ...rest] = kv.split("=");
      m.set(k as string, rest.join("="));
    }
    expected.set(field(v, 0), m);
  }
  const get = (f: Map<string, string>, k: string): string => {
    const v = f.get(k);
    assert.ok(v !== undefined, k);
    return v;
  };
  for (const [label, bytes] of batches()) {
    const b = parse(bytes);
    const f = expected.get(label);
    assert.ok(f !== undefined, label);
    assert.equal(u64ToString(b.baseOffset), get(f, "base_offset"), label);
    assert.equal(u64ToString(b.term), get(f, "term"), label);
    assert.equal(attributesToBits(b.attributes), Number(get(f, "attributes")), label);
    assert.equal(b.attributes.compression, Number(get(f, "compression")), label);
    assert.equal(String(b.attributes.transactional), get(f, "transactional"), label);
    assert.equal(String(b.attributes.control), get(f, "control"), label);
    assert.equal(b.attributes.kind, Number(get(f, "kind")), label);
    assert.equal(b.lastOffsetDelta, Number(get(f, "last_offset_delta")), label);
    assert.equal(b.baseTimestamp, BigInt(get(f, "base_ts")), label);
    assert.equal(b.maxTimestamp, BigInt(get(f, "max_ts")), label);
    assert.equal(u64ToString(b.producerId), get(f, "producer_id"), label);
    assert.equal(b.producerEpoch, Number(get(f, "producer_epoch")), label);
    assert.equal(b.baseSequence, Number(get(f, "base_sequence")), label);
    assert.equal(b.recordCount, Number(get(f, "record_count")), label);
    assert.equal(b.length, Number(get(f, "len")), label);
  }
});

test("batches: records match the broker's", () => {
  const expected = new Map<string, string[]>();
  for (const v of lines("batch-records.txt")) {
    const list = expected.get(field(v, 0)) ?? [];
    list.push(v.join(" "));
    expected.set(field(v, 0), list);
  }
  for (const [label, bytes] of batches()) {
    const got = parse(bytes)
      .records()
      .map((r, i) => recordLine(label, i, r));
    assert.deepEqual(got, expected.get(label), label);
  }
});

test("batches: a parsed batch is written again byte for byte", () => {
  for (const [label, bytes] of batches()) {
    const b = parse(bytes);
    const builder = new BatchBuilder({
      attributes: b.attributes,
      producerId: b.producerId,
      producerEpoch: b.producerEpoch,
      baseSequence: b.baseSequence,
    });
    for (const r of b.records()) {
      builder.push(r.timestamp, r.key, r.value, r.headers);
    }
    const again = builder.finish();
    Batch.stamp(again, b.baseOffset, b.term);
    assert.equal(toHex(again), toHex(bytes), label);
  }
});

// The broker's own construction, repeated here without the reader: the oracle is written in the test.
const TS = 1_700_000_000_000n;
const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s);

test("batches: built here they match the broker's bytes", () => {
  const expected = batches();
  const want = (label: string): string => toHex(expected.get(label) as Uint8Array);

  let b = new BatchBuilder();
  b.push(TS, utf8("k1"), utf8("v1"), [
    { key: utf8("h"), value: utf8("x") },
    { key: utf8("n"), value: null },
  ]);
  b.push(TS + 5n, null, null);
  b.push(TS - 1n, new Uint8Array(0), new Uint8Array(0), [{ key: new Uint8Array(0), value: new Uint8Array(0) }]);
  let built = b.finish();
  Batch.stamp(built, 100n, 7n);
  assert.equal(toHex(built), want("data-stamped"));

  b = new BatchBuilder({
    attributes: { compression: Compression.NONE, transactional: false, control: false, kind: BatchKind.DATA },
    producerId: 17n,
    producerEpoch: 2,
    baseSequence: 5,
  });
  for (let i = 0; i < 3; i++) {
    b.push(TS + BigInt(i), new Uint8Array([i]), new Uint8Array([i, i]));
  }
  built = b.finish();
  Batch.stamp(built, 3n, 1n);
  assert.equal(toHex(built), want("idempotent"));

  b = new BatchBuilder({
    attributes: { compression: Compression.NONE, transactional: true, control: false, kind: BatchKind.DATA },
    producerId: 17n,
    producerEpoch: 2,
    baseSequence: 8,
  });
  b.push(TS, utf8("t"), utf8("transactional"));
  built = b.finish();
  Batch.stamp(built, 200n, 2n);
  assert.equal(toHex(built), want("transactional"));

  built = marker(17n, 3, ControlType.COMMIT, 9, TS);
  Batch.stamp(built, 204n, 2n);
  assert.equal(toHex(built), want("control-commit"));

  built = marker(18n, 0, ControlType.ABORT, 0, TS);
  Batch.stamp(built, 205n, 2n);
  assert.equal(toHex(built), want("control-abort"));

  b = new BatchBuilder();
  b.push(TS, new Uint8Array(200).fill(0xab), new Uint8Array(20_000).fill(0xcd));
  b.push(-TS, utf8("negative"), null);
  assert.equal(toHex(b.finish()), want("data-large"));
});

test("batches: in-place edits match and keep the batch valid", () => {
  for (const v of lines("batch-ops.txt")) {
    const batch = hex(field(v, v.length - 2)).slice();
    const args = field(v, 1).split(",");
    const arg = (i: number): string => args[i] as string;
    switch (v[0]) {
      case "set_producer":
        Batch.setProducer(batch, BigInt(arg(0)), Number(arg(1)), Number(arg(2)));
        break;
      case "set_transactional":
        Batch.setTransactional(batch, arg(0) === "true");
        break;
      case "set_base_timestamp":
        Batch.setBaseTimestamp(batch, BigInt(arg(0)));
        break;
      default:
        assert.fail(`unknown op ${v[0]}`);
    }
    assert.equal(toHex(batch), field(v, v.length - 1), `${v[0]} ${v[1]}`);
    // The checksum was recomputed, so it parses.
    Batch.parse(batch);
  }
});

test("batches: several in a row are split by their lengths", () => {
  for (const v of lines("fetch-records.hex")) {
    const all = hex(field(v, 1));
    const parsed = Batch.parseAll(all);
    assert.ok(parsed.length >= 4, field(v, 0));
    let at = 0;
    for (const b of parsed) {
      assert.equal(b.length, Batch.peekLen(all.subarray(at)));
      at += b.length;
    }
    assert.equal(at, all.length, field(v, 0));
    // Cutting the last batch short is a truncation, not a shorter list.
    assert.throws(
      () => Batch.parseAll(all.subarray(0, all.length - 1)),
      (e: unknown) => e instanceof RecordFormatError && e.kind === "TRUNCATED",
    );
  }
});

test("batches: every vector cut at every length is refused, and one byte more is not a batch", () => {
  const all = [
    ...lines("batches.hex").map((v) => [field(v, 0), field(v, 1)] as const),
    ...lines("compressed.hex").map((v) => [field(v, 0), field(v, 1)] as const),
    ...lines("compacted.hex").map((v) => [field(v, 0), field(v, 3)] as const),
    ...lines("fetch-records.hex").map((v) => [field(v, 0), field(v, 1)] as const),
  ];
  for (const [label, h] of all) {
    const bytes = hex(h);
    const isRow = label.startsWith("fetch-");
    // A row of batches cut inside a batch fails; cut exactly between batches it is a shorter, valid row.
    const boundaries = new Set<number>([0]);
    if (isRow) {
      let sum = 0;
      for (const b of Batch.parseAll(bytes)) {
        sum += b.length;
        boundaries.add(sum);
      }
    }
    for (let cut = 0; cut < bytes.length; cut++) {
      const part = bytes.subarray(0, cut);
      if (isRow) {
        if (boundaries.has(cut)) {
          Batch.parseAll(part);
        } else {
          assert.throws(() => Batch.parseAll(part), RecordFormatError, `${label} cut at ${cut}`);
        }
      } else {
        assert.throws(() => Batch.parse(part), RecordFormatError, `${label} cut at ${cut}`);
      }
    }
    const longer = new Uint8Array(bytes.length + 1);
    longer.set(bytes);
    assert.throws(() => Batch.parseAll(longer), RecordFormatError, `${label} plus one byte`);
  }
});

test("batches: the checksum is over the whole batch from byte 25", () => {
  for (const [label, b] of batches()) {
    assert.equal(crc32c(b, 25, b.length), getU32(b, 21), label);
    // Stamping changes bytes 0..8 and 12..20 only, so the checksum stays valid.
    const stamped = b.slice();
    Batch.stamp(stamped, 123456789n, 42n);
    Batch.parse(stamped);
    assert.deepEqual(stamped.subarray(21), b.subarray(21), label);
  }
});

test("batches: control markers are read", () => {
  const b = batches();
  const commit = parse(b.get("control-commit") as Uint8Array);
  assert.deepEqual(readMarker(commit), { type: ControlType.COMMIT, coordinatorEpoch: 9 });
  assert.equal(controlKind(commit), ControlKind.COMMIT_MARKER);
  assert.equal(commit.producerId, 17n);
  const abort = parse(b.get("control-abort") as Uint8Array);
  assert.deepEqual(readMarker(abort), { type: ControlType.ABORT, coordinatorEpoch: 0 });
  assert.equal(controlKind(parse(b.get("raft-noop") as Uint8Array)), ControlKind.RAFT_NOOP);
  assert.equal(controlKind(parse(b.get("raft-config") as Uint8Array)), ControlKind.RAFT_CONFIG);
  assert.equal(controlKind(parse(b.get("internal-metadata") as Uint8Array)), ControlKind.METADATA);
  const data = parse(b.get("data-stamped") as Uint8Array);
  assert.equal(controlKind(data), null);
  assert.equal(data.attributes.control, false);
});

test("batches: the kinds of the queue log", () => {
  const b = batches();
  const kind = (label: string): number => parse(b.get(label) as Uint8Array).attributes.kind;
  assert.equal(kind("ack"), BatchKind.ACK);
  assert.equal(kind("queue-state"), BatchKind.QUEUE_STATE);
  assert.equal(kind("dead-letter"), BatchKind.DEAD_LETTER);
  assert.equal(kind("retire-out"), BatchKind.RETIRE);
  assert.equal(kind("retire-done"), BatchKind.RETIRE);
});

test("batches: the header is 65 bytes", () => {
  assert.equal(HEADER_LEN, 65);
});

import assert from "node:assert/strict";
import { test } from "node:test";
import { RecordFormatError } from "../src/record/errors.js";
import { compressFrame, decompressFrame } from "../src/record/lz4.js";
import { xxh32 } from "../src/record/xxhash.js";

/**
 * The LZ4 frame codec written for ADR 0085. The decoder is also checked against the frames the broker writes in
 * compression.test.ts; here it meets frames written by hand with every option the format has.
 */

const LIMIT = 16 * 1024 * 1024;
const text = (s: string): Uint8Array => Uint8Array.from(Buffer.from(s, "latin1"));

/** A small deterministic generator, so a failure reproduces. */
function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s;
  };
}

function bytes(n: number, seed: number, alphabet = 256): Uint8Array {
  const r = rng(seed);
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i++) {
    out[i] = r() % alphabet;
  }
  return out;
}

const u32le = (v: number): number[] => [v & 255, (v >>> 8) & 255, (v >>> 16) & 255, (v >>> 24) & 255];

interface FrameSpec {
  readonly flg?: number;
  readonly bd?: number;
  readonly contentSize?: bigint;
  /** Blocks as `[bytes, stored]`. */
  readonly blocks: readonly (readonly [Uint8Array, boolean])[];
  readonly blockChecksums?: boolean;
  readonly content?: Uint8Array;
  readonly badHeaderChecksum?: boolean;
  readonly badBlockChecksum?: boolean;
  readonly badContentChecksum?: boolean;
  readonly trailing?: Uint8Array;
}

/** Writes a frame the way the format says, with the options of `spec`. */
function frame(spec: FrameSpec): Uint8Array {
  const out: number[] = [0x04, 0x22, 0x4d, 0x18];
  let flg = spec.flg ?? 0b0110_0000;
  if (spec.blockChecksums === true) {
    flg |= 0b0001_0000;
  }
  if (spec.contentSize !== undefined) {
    flg |= 0b0000_1000;
  }
  if (spec.content !== undefined) {
    flg |= 0b0000_0100;
  }
  const descriptor: number[] = [flg, spec.bd ?? 0x40];
  if (spec.contentSize !== undefined) {
    const lo = Number(spec.contentSize & 0xffff_ffffn);
    const hi = Number(spec.contentSize >> 32n);
    descriptor.push(...u32le(lo), ...u32le(hi));
  }
  const hc = (xxh32(Uint8Array.from(descriptor)) >>> 8) & 255;
  out.push(...descriptor, spec.badHeaderChecksum === true ? hc ^ 1 : hc);
  for (const [data, stored] of spec.blocks) {
    out.push(...u32le((stored ? 0x8000_0000 : 0) | data.length), ...data);
    if (spec.blockChecksums === true) {
      const c = xxh32(data);
      out.push(...u32le(spec.badBlockChecksum === true ? c ^ 1 : c));
    }
  }
  out.push(0, 0, 0, 0);
  if (spec.content !== undefined) {
    const c = xxh32(spec.content);
    out.push(...u32le(spec.badContentChecksum === true ? c ^ 1 : c));
  }
  out.push(...(spec.trailing ?? []));
  return Uint8Array.from(out);
}

function malformed(f: () => unknown, what = ""): void {
  assert.throws(
    f,
    (e: unknown) => e instanceof RecordFormatError && e.kind === "MALFORMED",
    `${what}: expected MALFORMED`,
  );
}

test("xxHash32 gives the reference values", () => {
  assert.equal(xxh32(new Uint8Array(0)), 0x02cc5d05);
  assert.equal(xxh32(text("abc")), 0x32d153ff);
  assert.equal(xxh32(text("Nobody inspects the spammish repetition")), 0xe2293b2f);
  // The 16-byte stripe path, and a seed.
  assert.equal(xxh32(text("0123456789abcdef0123456789abcdef")), xxh32(text("0123456789abcdef0123456789abcdef"), 0, 32));
  assert.equal(xxh32(new Uint8Array(0), 0, 0, 1), 0x0b2cb792);
});

test("frames written here read back, for sizes at every edge", () => {
  for (const n of [0, 1, 4, 11, 12, 13, 17, 255, 256, 270, 4096, 65535, 65536, 65537, 131072, 200_000]) {
    for (const [name, data] of [
      ["random", bytes(n, n)],
      ["few symbols", bytes(n, n + 1, 4)],
      ["zeros", new Uint8Array(n)],
    ] as const) {
      const packed = compressFrame(data);
      assert.deepEqual(decompressFrame(packed, LIMIT), data, `${name} ${n}`);
    }
  }
});

test("repetitive data shrinks and random data is stored with little overhead", () => {
  const zeros = compressFrame(new Uint8Array(100_000));
  assert.ok(zeros.length < 1_000, `zeros: ${zeros.length}`);
  const noise = bytes(100_000, 3);
  const packed = compressFrame(noise);
  assert.ok(packed.length <= noise.length + 7 + 2 * 4 + 4, `noise: ${packed.length}`);
  const words = text("the quick brown fox jumps over the lazy dog ".repeat(2_000));
  assert.ok(compressFrame(words).length < words.length / 10);
});

test("a frame written here is a version 1 frame of independent 64 KiB blocks with no checksums", () => {
  const f = compressFrame(text("hello hello hello hello hello hello"));
  assert.deepEqual([...f.subarray(0, 4)], [0x04, 0x22, 0x4d, 0x18]);
  assert.equal(f[4], 0b0110_0000);
  assert.equal(f[5], 0x40);
  assert.equal(f[6], (xxh32(f, 4, 6) >>> 8) & 255);
  assert.deepEqual([...f.subarray(f.length - 4)], [0, 0, 0, 0], "ends with the end mark");
  assert.equal(compressFrame(new Uint8Array(0)).length, 7 + 4, "an empty input is a header and an end mark");
});

test("the decoder takes the options the format has", () => {
  // "abcd" then a match of offset 4, length 8, then five literals.
  const block = Uint8Array.from([0x44, ...text("abcd"), 4, 0, 0x50, ...text("hello")]);
  const expected = text("abcdabcdabcdhello");
  assert.deepEqual(decompressFrame(frame({ blocks: [[block, false]] }), LIMIT), expected, "plain");
  assert.deepEqual(decompressFrame(frame({ blocks: [[block, false]], blockChecksums: true }), LIMIT), expected, "block checksums");
  assert.deepEqual(decompressFrame(frame({ blocks: [[block, false]], content: expected }), LIMIT), expected, "content checksum");
  assert.deepEqual(decompressFrame(frame({ blocks: [[block, false]], contentSize: BigInt(expected.length) }), LIMIT), expected, "content size");
  assert.deepEqual(decompressFrame(frame({ blocks: [[expected, true]] }), LIMIT), expected, "stored block");
  assert.deepEqual(decompressFrame(frame({ blocks: [[text("abc"), true], [text("def"), true]] }), LIMIT), text("abcdef"), "two blocks");
  for (const bd of [0x50, 0x60, 0x70]) {
    assert.deepEqual(decompressFrame(frame({ blocks: [[block, false]], bd }), LIMIT), expected, `block size ${bd}`);
  }
});

test("a linked block may reach into the block before it, an independent one may not", () => {
  const first = text("abcdefgh");
  // Literals only: the last sequence of a block has no match.
  const firstBlock = Uint8Array.from([0x80, ...first]);
  // One literal "Z", then a match of offset 9 (back into the first block), length 4, then five literals.
  const second = Uint8Array.from([0x10, 0x5a, 9, 0, 0x50, ...text("12345")]);
  const linked = frame({ flg: 0b0100_0000, blocks: [[firstBlock, false], [second, false]] });
  // "abcdefgh", "Z", then 4 bytes from 9 back: "bcde"? output length is 9 at the match, offset 9 -> index 0 -> "abcd".
  assert.deepEqual(decompressFrame(linked, LIMIT), text("abcdefghZabcd12345"));
  malformed(() => decompressFrame(frame({ blocks: [[firstBlock, false], [second, false]] }), LIMIT), "independent");
});

test("a frame that breaks a rule is refused as malformed", () => {
  const block = Uint8Array.from([0x50, ...text("hello")]);
  const good = frame({ blocks: [[block, false]] });
  assert.deepEqual(decompressFrame(good, LIMIT), text("hello"));
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], badHeaderChecksum: true }), LIMIT), "header checksum");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], blockChecksums: true, badBlockChecksum: true }), LIMIT), "block checksum");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], content: text("hello"), badContentChecksum: true }), LIMIT), "content checksum");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], contentSize: 6n }), LIMIT), "content size");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], trailing: Uint8Array.from([0]) }), LIMIT), "trailing byte");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], flg: 0b1010_0000 }), LIMIT), "version 2");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], flg: 0b0110_0010 }), LIMIT), "reserved FLG bit");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], bd: 0x41 }), LIMIT), "reserved BD bit");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], bd: 0x30 }), LIMIT), "block size 3");
  malformed(() => decompressFrame(frame({ blocks: [[block, false]], flg: 0b0110_0001 }), LIMIT), "dictionary");
  malformed(() => decompressFrame(frame({ blocks: [[text("abcd".repeat(20_000)), true]] }), LIMIT), "block larger than the frame allows");
  const bad = good.slice();
  bad[0] = 0;
  malformed(() => decompressFrame(bad, LIMIT), "magic");
  // Block rules: offset 0, offset before the data, a length that runs on, literals that run past the block.
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0x40, ...text("abcd"), 0, 0, 0x50, ...text("hello")]), false]] }), LIMIT), "offset 0");
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0x40, ...text("abcd"), 5, 0, 0x50, ...text("hello")]), false]] }), LIMIT), "offset past the start");
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0x40, ...text("abcd"), 4]), false]] }), LIMIT), "block ends inside an offset");
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0x4f, ...text("abcd"), 4, 0, 255]), false]] }), LIMIT), "length runs on");
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0x90, ...text("abcd")]), false]] }), LIMIT), "literals run past the block");
  malformed(() => decompressFrame(frame({ blocks: [[Uint8Array.from([0xf0, 255]), false]] }), LIMIT), "literal length runs on");
});

test("a frame cut at any length is refused, never mistaken for a shorter frame", () => {
  const data = text("a frame to cut, a frame to cut, a frame to cut, a frame to cut");
  for (const f of [compressFrame(data), frame({ blocks: [[data, true]], blockChecksums: true, content: data, contentSize: BigInt(data.length) })]) {
    for (let n = 0; n < f.length; n++) {
      malformed(() => decompressFrame(f.subarray(0, n), LIMIT), `cut at ${n} of ${f.length}`);
    }
  }
});

test("mutated frames are refused or read, and nothing else can happen", () => {
  const r = rng(7);
  const data = text("fuzz fuzz fuzz fuzz ".repeat(50));
  const f = compressFrame(data);
  for (let i = 0; i < 3_000; i++) {
    const m = f.slice();
    const flips = 1 + (r() % 3);
    for (let k = 0; k < flips; k++) {
      m[r() % m.length] = r() & 255;
    }
    try {
      decompressFrame(m, 4096);
    } catch (e) {
      assert.ok(e instanceof RecordFormatError, `iteration ${i}: ${String(e)}`);
    }
  }
});

test("output past the limit is refused as too large, exactly at the limit is not", () => {
  const limit = 100_000;
  const at = compressFrame(new Uint8Array(limit));
  assert.equal(decompressFrame(at, limit).length, limit);
  assert.throws(
    () => decompressFrame(compressFrame(new Uint8Array(limit + 1)), limit),
    (e: unknown) => e instanceof RecordFormatError && e.kind === "TOO_LARGE",
  );
  // A declared size cannot make the decoder allocate more than the limit allows.
  const lying = frame({ blocks: [[text("hi"), true]], contentSize: 0xffff_ffff_ffffn });
  malformed(() => decompressFrame(lying, limit), "a content size that is not the content");
  // A match chain far beyond the limit stops at the limit and does not grow without bound.
  assert.throws(
    () => decompressFrame(compressFrame(new Uint8Array(3_000_000)), 1_000_000),
    (e: unknown) => e instanceof RecordFormatError && e.kind === "TOO_LARGE",
  );
});

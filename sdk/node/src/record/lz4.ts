import { RecordFormatError } from "./errors.js";
import { xxh32 } from "./xxhash.js";

/**
 * The LZ4 frame format (lz4_Frame_format.md) and the LZ4 block format inside it, written for record blocks
 * (ADR 0085). Nothing here depends on a library.
 *
 * The decoder reads what lz4_flex, the broker's library, and lz4-java write: any block size, independent or linked
 * blocks, optional content size and block and content checksums (all verified). It refuses a dictionary, a
 * legacy frame, a frame that does not end where the input does, and output past `limit`.
 *
 * The encoder writes one frame of independent 64 KiB blocks with no checksums, as ADR 0061 does for Java, and a
 * block that does not shrink is stored as it is.
 */

const MAGIC = 0x184d2204;
const BLOCK_SIZE = 64 * 1024;
const MIN_MATCH = 4;
/** A match must start at least this many bytes before the end of a block, and leave this many literals after it. */
const MF_LIMIT = 12;
const LAST_LITERALS = 5;
const HASH_BITS = 14;
const MAX_OFFSET = 0xffff;
const STORED = 0x8000_0000;

function malformed(what: string): RecordFormatError {
  return RecordFormatError.malformed(`LZ4 ${what}`);
}

function tooLarge(limit: number): RecordFormatError {
  return new RecordFormatError("TOO_LARGE", `block decompresses to more than ${limit} bytes`);
}

function u32(b: Uint8Array, at: number): number {
  return ((b[at] as number) | ((b[at + 1] as number) << 8) | ((b[at + 2] as number) << 16) | ((b[at + 3] as number) << 24)) >>> 0;
}

const BLOCK_SIZES: Readonly<Record<number, number>> = {
  4: 64 * 1024,
  5: 256 * 1024,
  6: 1024 * 1024,
  7: 4 * 1024 * 1024,
};

/** Growable output with a ceiling, so a hostile frame cannot make the decoder allocate without bound. */
class Out {
  buf: Uint8Array;
  len = 0;

  constructor(
    initial: number,
    readonly limit: number,
  ) {
    this.buf = new Uint8Array(Math.max(16, Math.min(initial, limit + 1)));
  }

  /** Room for `extra` more bytes; fails once the output would pass `limit`. */
  reserve(extra: number): void {
    const needed = this.len + extra;
    if (needed > this.limit) {
      throw tooLarge(this.limit);
    }
    if (needed > this.buf.length) {
      const grown = new Uint8Array(Math.min(Math.max(needed, this.buf.length * 2), this.limit + 1));
      grown.set(this.buf.subarray(0, this.len));
      this.buf = grown;
    }
  }
}

/** Decodes one LZ4 block into `out`. `base` is where the block's own output starts (linked blocks may reach before it). */
function decodeBlock(src: Uint8Array, from: number, to: number, out: Out, base: number, independent: boolean): void {
  let ip = from;
  while (ip < to) {
    const token = src[ip++] as number;
    let literals = token >> 4;
    if (literals === 15) {
      let b: number;
      do {
        if (ip >= to) {
          throw malformed("block ends inside a length");
        }
        b = src[ip++] as number;
        literals += b;
        if (literals > out.limit + 1) {
          throw tooLarge(out.limit);
        }
      } while (b === 255);
    }
    if (ip + literals > to) {
      throw malformed("literals run past the block");
    }
    out.reserve(literals);
    out.buf.set(src.subarray(ip, ip + literals), out.len);
    out.len += literals;
    ip += literals;
    if (ip === to) {
      return; // the last sequence has literals only
    }
    if (ip + 2 > to) {
      throw malformed("block ends inside an offset");
    }
    const offset = (src[ip] as number) | ((src[ip + 1] as number) << 8);
    ip += 2;
    if (offset === 0) {
      throw malformed("match offset 0");
    }
    const reach = independent ? out.len - base : out.len;
    if (offset > reach) {
      throw malformed("match reaches before the data it may use");
    }
    let length = token & 15;
    if (length === 15) {
      let b: number;
      do {
        if (ip >= to) {
          throw malformed("block ends inside a length");
        }
        b = src[ip++] as number;
        length += b;
        if (length > out.limit + 1) {
          throw tooLarge(out.limit);
        }
      } while (b === 255);
    }
    length += MIN_MATCH;
    out.reserve(length);
    // A match may overlap what it writes, so it is copied byte by byte.
    let from2 = out.len - offset;
    const buf = out.buf;
    for (let i = 0; i < length; i++) {
      buf[out.len++] = buf[from2++] as number;
    }
  }
  if (ip !== to) {
    throw malformed("block does not end after a literal run");
  }
}

/** Decodes a frame. `limit` is the most the output may hold; one byte more fails with `TOO_LARGE`. */
export function decompressFrame(src: Uint8Array, limit: number): Uint8Array {
  if (src.length < 7 || u32(src, 0) !== MAGIC) {
    throw malformed("frame does not start with the frame magic number");
  }
  const flg = src[4] as number;
  const bd = src[5] as number;
  if (flg >> 6 !== 1) {
    throw malformed("frame version is not 1");
  }
  if ((flg & 0b10) !== 0 || (bd & 0b1000_1111) !== 0) {
    throw malformed("frame sets reserved bits");
  }
  if ((flg & 1) !== 0) {
    throw malformed("frame needs a dictionary, which record blocks never use");
  }
  const independent = (flg & 0b0010_0000) !== 0;
  const blockChecksums = (flg & 0b0001_0000) !== 0;
  const hasContentSize = (flg & 0b0000_1000) !== 0;
  const contentChecksum = (flg & 0b0000_0100) !== 0;
  const maxBlock = BLOCK_SIZES[(bd >> 4) & 7];
  if (maxBlock === undefined) {
    throw malformed("frame has an unknown maximum block size");
  }
  let p = 6;
  let declared = -1n;
  if (hasContentSize) {
    if (src.length < p + 8) {
      throw malformed("frame ends inside its header");
    }
    declared = BigInt.asUintN(64, (BigInt(u32(src, p + 4)) << 32n) | BigInt(u32(src, p)));
    p += 8;
  }
  if (src.length < p + 1) {
    throw malformed("frame ends inside its header");
  }
  if (src[p] !== ((xxh32(src, 4, p) >>> 8) & 0xff)) {
    throw malformed("header checksum does not match");
  }
  p++;
  const out = new Out(declared >= 0n && declared <= BigInt(limit) ? Number(declared) : 4096, limit);
  for (;;) {
    if (p + 4 > src.length) {
      throw malformed("frame ends before its end mark");
    }
    const size = u32(src, p);
    p += 4;
    if (size === 0) {
      break;
    }
    const stored = (size & STORED) !== 0;
    const n = size & ~STORED;
    if (n > maxBlock) {
      throw malformed("block is larger than the frame allows");
    }
    if (p + n + (blockChecksums ? 4 : 0) > src.length) {
      throw malformed("frame ends inside a block");
    }
    if (blockChecksums && u32(src, p + n) !== xxh32(src, p, p + n)) {
      throw malformed("block checksum does not match");
    }
    const base = out.len;
    if (stored) {
      out.reserve(n);
      out.buf.set(src.subarray(p, p + n), out.len);
      out.len += n;
    } else {
      decodeBlock(src, p, p + n, out, base, independent);
      if (out.len - base > maxBlock) {
        throw malformed("block decodes to more than the frame allows");
      }
    }
    p += n + (blockChecksums ? 4 : 0);
  }
  const result = out.buf.subarray(0, out.len);
  if (contentChecksum) {
    if (p + 4 > src.length) {
      throw malformed("frame ends before its content checksum");
    }
    if (u32(src, p) !== xxh32(result)) {
      throw malformed("content checksum does not match");
    }
    p += 4;
  }
  if (p !== src.length) {
    throw malformed("bytes follow the frame");
  }
  if (declared >= 0n && declared !== BigInt(out.len)) {
    throw malformed("content size does not match");
  }
  return result;
}

/** Compresses `src[from, to)` into one LZ4 block, appended to `dst` at `at`; returns the new end. `dst` must have room for `bound`. */
function encodeBlock(src: Uint8Array, from: number, to: number, dst: Uint8Array, at: number, table: Int32Array): number {
  let op = at;
  let anchor = from;
  const length = to - from;
  const writeLength = (n: number): void => {
    while (n >= 255) {
      dst[op++] = 255;
      n -= 255;
    }
    dst[op++] = n;
  };
  const emit = (litEnd: number, matchLength: number, offset: number): void => {
    const lit = litEnd - anchor;
    const ml = matchLength === 0 ? 0 : matchLength - MIN_MATCH;
    dst[op++] = ((lit >= 15 ? 15 : lit) << 4) | (matchLength === 0 ? 0 : ml >= 15 ? 15 : ml);
    if (lit >= 15) {
      writeLength(lit - 15);
    }
    dst.set(src.subarray(anchor, litEnd), op);
    op += lit;
    if (matchLength !== 0) {
      dst[op++] = offset & 0xff;
      dst[op++] = offset >> 8;
      if (ml >= 15) {
        writeLength(ml - 15);
      }
    }
  };
  if (length >= MF_LIMIT + 1) {
    table.fill(-1);
    const lastStart = to - MF_LIMIT;
    const matchEnd = to - LAST_LITERALS;
    let ip = from;
    while (ip <= lastStart) {
      const seq = u32(src, ip);
      const h = Math.imul(seq, 0x9e3779b1) >>> (32 - HASH_BITS);
      const ref = table[h] as number;
      table[h] = ip;
      if (ref >= 0 && ip - ref <= MAX_OFFSET && u32(src, ref) === seq) {
        let m = MIN_MATCH;
        while (ip + m < matchEnd && src[ref + m] === src[ip + m]) {
          m++;
        }
        emit(ip, m, ip - ref);
        ip += m;
        anchor = ip;
      } else {
        ip++;
      }
    }
  }
  emit(to, 0, 0);
  return op;
}

/** The worst case for one block's output: the block grows by at most 1/255 plus a few bytes. */
function bound(n: number): number {
  return n + Math.floor(n / 255) + 16;
}

/** Compresses `src` into one frame of independent 64 KiB blocks. */
export function compressFrame(src: Uint8Array): Uint8Array {
  const blocks = Math.ceil(src.length / BLOCK_SIZE);
  const out = new Uint8Array(7 + blocks * (4 + bound(BLOCK_SIZE)) + 4);
  let op = 0;
  out[op++] = MAGIC & 0xff;
  out[op++] = (MAGIC >>> 8) & 0xff;
  out[op++] = (MAGIC >>> 16) & 0xff;
  out[op++] = (MAGIC >>> 24) & 0xff;
  out[op++] = 0b0110_0000; // version 1, independent blocks, no checksums, no content size
  out[op++] = 4 << 4; // 64 KiB blocks
  out[op] = (xxh32(out, 4, 6) >>> 8) & 0xff;
  op++;
  const table = new Int32Array(1 << HASH_BITS);
  for (let at = 0; at < src.length; at += BLOCK_SIZE) {
    const end = Math.min(src.length, at + BLOCK_SIZE);
    const sizeAt = op;
    op += 4;
    let written = encodeBlock(src, at, end, out, op, table) - op;
    let header = written;
    if (written >= end - at) {
      // It did not shrink: store the bytes as they are.
      out.set(src.subarray(at, end), op);
      written = end - at;
      header = written | STORED;
    }
    out[sizeAt] = header & 0xff;
    out[sizeAt + 1] = (header >>> 8) & 0xff;
    out[sizeAt + 2] = (header >>> 16) & 0xff;
    out[sizeAt + 3] = (header >>> 24) & 0xff;
    op += written;
  }
  out[op++] = 0;
  out[op++] = 0;
  out[op++] = 0;
  out[op++] = 0;
  return out.subarray(0, op);
}

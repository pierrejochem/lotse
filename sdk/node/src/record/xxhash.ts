/**
 * xxHash32, which the LZ4 frame format uses for its header, block and content checksums. Written out here so that
 * the SDK needs no dependency for it; checked against the reference values and against the frames the broker writes.
 */

const P1 = 0x9e3779b1;
const P2 = 0x85ebca77;
const P3 = 0xc2b2ae3d;
const P4 = 0x27d4eb2f;
const P5 = 0x165667b1;

function rotl(x: number, r: number): number {
  return ((x << r) | (x >>> (32 - r))) | 0;
}

function read32(b: Uint8Array, at: number): number {
  return ((b[at] as number) | ((b[at + 1] as number) << 8) | ((b[at + 2] as number) << 16) | ((b[at + 3] as number) << 24)) | 0;
}

function round(acc: number, input: number): number {
  return Math.imul(rotl((acc + Math.imul(input, P2)) | 0, 13), P1);
}

/** The 32-bit hash of `data[start, end)` as an unsigned number. */
export function xxh32(data: Uint8Array, start = 0, end: number = data.length, seed = 0): number {
  let p = start;
  let h: number;
  if (end - start >= 16) {
    let v1 = (seed + P1 + P2) | 0;
    let v2 = (seed + P2) | 0;
    let v3 = seed | 0;
    let v4 = (seed - P1) | 0;
    const limit = end - 16;
    while (p <= limit) {
      v1 = round(v1, read32(data, p));
      v2 = round(v2, read32(data, p + 4));
      v3 = round(v3, read32(data, p + 8));
      v4 = round(v4, read32(data, p + 12));
      p += 16;
    }
    h = (rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)) | 0;
  } else {
    h = (seed + P5) | 0;
  }
  h = (h + (end - start)) | 0;
  while (p + 4 <= end) {
    h = Math.imul(rotl((h + Math.imul(read32(data, p), P3)) | 0, 17), P4);
    p += 4;
  }
  while (p < end) {
    h = Math.imul(rotl((h + Math.imul(data[p] as number, P5)) | 0, 11), P1);
    p++;
  }
  h ^= h >>> 15;
  h = Math.imul(h, P2);
  h ^= h >>> 13;
  h = Math.imul(h, P3);
  h ^= h >>> 16;
  return h >>> 0;
}

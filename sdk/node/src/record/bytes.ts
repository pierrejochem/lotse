/** Fixed-width little-endian access to bytes, and a buffer that grows. */

export function view(b: Uint8Array): DataView {
  return new DataView(b.buffer, b.byteOffset, b.byteLength);
}

export function getU16(b: Uint8Array, at: number): number {
  return view(b).getUint16(at, true);
}

export function getU32(b: Uint8Array, at: number): number {
  return view(b).getUint32(at, true);
}

export function getI32(b: Uint8Array, at: number): number {
  return view(b).getInt32(at, true);
}

/** A 64-bit field as a signed bit pattern. */
export function getI64(b: Uint8Array, at: number): bigint {
  return view(b).getBigInt64(at, true);
}

export function putU16(b: Uint8Array, at: number, v: number): void {
  view(b).setUint16(at, v, true);
}

export function putU32(b: Uint8Array, at: number, v: number): void {
  view(b).setUint32(at, v >>> 0, true);
}

/** Accepts any bigint; the low 64 bits are written. */
export function putI64(b: Uint8Array, at: number, v: bigint): void {
  view(b).setBigUint64(at, BigInt.asUintN(64, v), true);
}

export function toHex(b: Uint8Array): string {
  let s = "";
  for (const x of b) {
    s += x.toString(16).padStart(2, "0");
  }
  return s;
}

export function fromHex(s: string): Uint8Array {
  if (s.length % 2 !== 0 || /[^0-9a-fA-F]/.test(s)) {
    throw new TypeError("not a hex string");
  }
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(s.slice(2 * i, 2 * i + 2), 16);
  }
  return out;
}

export function concat(parts: readonly Uint8Array[]): Uint8Array {
  let n = 0;
  for (const p of parts) {
    n += p.length;
  }
  const out = new Uint8Array(n);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

export function equalBytes(a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === null || b === null) {
    return a === b;
  }
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/** A byte buffer that grows by doubling. */
export class GrowableBytes {
  private buf: Uint8Array;
  private len = 0;

  constructor(capacity = 64) {
    this.buf = new Uint8Array(Math.max(capacity, 16));
  }

  get size(): number {
    return this.len;
  }

  /** The bytes written so far, as a view of the internal buffer (valid until the next write). */
  bytes(): Uint8Array {
    return this.buf.subarray(0, this.len);
  }

  reserve(extra: number): void {
    const needed = this.len + extra;
    if (needed > this.buf.length) {
      const grown = new Uint8Array(Math.max(needed, this.buf.length * 2));
      grown.set(this.buf.subarray(0, this.len));
      this.buf = grown;
    }
  }

  put(b: Uint8Array): void {
    this.reserve(b.length);
    this.buf.set(b, this.len);
    this.len += b.length;
  }

  putByte(v: number): void {
    this.reserve(1);
    this.buf[this.len++] = v;
  }

  /** Writes a ZigZag varint through `put`, a function from `varint.ts`. */
  putWith(max: number, write: (dst: Uint8Array, off: number) => number): void {
    this.reserve(max);
    this.len += write(this.buf, this.len);
  }
}

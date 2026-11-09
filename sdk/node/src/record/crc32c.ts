/** CRC32C (Castagnoli, polynomial 0x1EDC6F41, reflected). `node:zlib` only has CRC-32, which is another code. */

const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) {
      c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
    }
    t[n] = c >>> 0;
  }
  return t;
})();

/** The CRC32C of `buf[from, to)`, as an unsigned 32-bit number. */
export function crc32c(buf: Uint8Array, from = 0, to: number = buf.length): number {
  let c = 0xffff_ffff;
  for (let i = from; i < to; i++) {
    c = (TABLE[(c ^ (buf[i] as number)) & 0xff] as number) ^ (c >>> 8);
  }
  return (c ^ 0xffff_ffff) >>> 0;
}

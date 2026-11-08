import { RecordFormatError } from "./errors.js";

/** How a batch's record block is compressed (ADR 0058). */
export const Compression = { NONE: 0, ZSTD: 1, LZ4: 2 } as const;
export type Compression = (typeof Compression)[keyof typeof Compression];

/** What a batch holds. Everything but DATA belongs to the queue log. */
export const BatchKind = { DATA: 0, ACK: 1, QUEUE_STATE: 2, TIMER: 3, DEAD_LETTER: 4, RETIRE: 5 } as const;
export type BatchKind = (typeof BatchKind)[keyof typeof BatchKind];

/** The packed attributes of a batch (bit 8, compacted, is read from the batch itself). */
export interface Attributes {
  readonly compression: Compression;
  readonly transactional: boolean;
  readonly control: boolean;
  readonly kind: BatchKind;
}

export const DATA_ATTRIBUTES: Attributes = {
  compression: Compression.NONE,
  transactional: false,
  control: false,
  kind: BatchKind.DATA,
};

export function attributesToBits(a: Attributes): number {
  return a.compression | (a.transactional ? 1 << 3 : 0) | (a.control ? 1 << 4 : 0) | (a.kind << 5);
}

/** Reads the packed attributes. Bit 8 (compacted) is allowed and ignored here; bits 9 to 15 must be zero. */
export function attributesFromBits(bits: number): Attributes {
  const compression = bits & 0b111;
  if (compression > Compression.LZ4) {
    throw RecordFormatError.malformed("unknown compression");
  }
  const kind = (bits >> 5) & 0b111;
  if (kind > BatchKind.RETIRE) {
    throw RecordFormatError.malformed("unknown batch kind");
  }
  if (bits >> 9 !== 0) {
    throw RecordFormatError.malformed("reserved attribute bits set");
  }
  return {
    compression: compression as Compression,
    transactional: (bits & (1 << 3)) !== 0,
    control: (bits & (1 << 4)) !== 0,
    kind: kind as BatchKind,
  };
}

export function compressionName(c: Compression): string {
  return c === Compression.ZSTD ? "ZSTD" : c === Compression.LZ4 ? "LZ4" : "NONE";
}

/**
 * A place in a partition, as a read_committed reader sees it (ADR 0059): `EARLIEST` the first offset still stored,
 * `LATEST` the offset after the last one it may read, and `OffsetAt.time` the first offset of the first batch with a
 * record at or after a time, or the latest offset if there is none yet.
 */
export class OffsetAt {
  /** The first offset still stored. */
  static readonly EARLIEST = new OffsetAt(-2n);
  /** The offset after the last one a read_committed reader may see. */
  static readonly LATEST = new OffsetAt(-1n);

  private constructor(
    /** What `ListOffsets` asks for: -2, -1 or milliseconds since the epoch. */
    readonly timestamp: bigint,
  ) {}

  /** The first batch with a record at or after `epochMillis`; a negative time counts as 0. */
  static time(epochMillis: number | bigint): OffsetAt {
    const t = typeof epochMillis === "bigint" ? epochMillis : BigInt(Math.trunc(epochMillis));
    return new OffsetAt(t < 0n ? 0n : t);
  }

  toString(): string {
    return this.timestamp === -2n ? "EARLIEST" : this.timestamp === -1n ? "LATEST" : `time(${this.timestamp})`;
  }
}

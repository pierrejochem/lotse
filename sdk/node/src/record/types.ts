/** One header of a record. A key is never null; a value may be. */
export interface Header {
  readonly key: Uint8Array;
  readonly value: Uint8Array | null;
}

/** A record as a reader sees it. `offset` and `timestamp` are absolute. */
export interface BatchRecord {
  readonly offset: bigint;
  readonly timestamp: bigint;
  readonly key: Uint8Array | null;
  readonly value: Uint8Array | null;
  readonly headers: readonly Header[];
}

import { BatchBuilder } from "../record/builder.js";
import type { Header } from "../record/types.js";

/** Bytes, or text that is sent as UTF-8. */
export type Bytes = Uint8Array | string;

/** A header of an outgoing message. */
export interface OutHeader {
  readonly name: string;
  /** `null` is a header with no value, which differs from an empty one. */
  readonly value: Bytes | null;
}

/**
 * A message to produce: an optional key, a value, headers and a timestamp.
 *
 * The timestamp defaults to the time of the produce call, in milliseconds since the epoch. The Rust helper's default is
 * 0, but a stream's retention judges a batch's age by the producer's timestamps, so a message stamped 0 is old at
 * once and may be deleted at the next retention pass. Set a timestamp yourself only when it means something, such as
 * the time of the event.
 */
export interface OutMessage {
  /** The key, or `null`/absent for none. */
  readonly key?: Bytes | null;
  /** The value; `null`/absent is a tombstone. */
  readonly value?: Bytes | null;
  readonly headers?: readonly OutHeader[];
  /** Milliseconds since the epoch. */
  readonly timestamp?: number | bigint;
}

function bytesOf(b: Bytes): Uint8Array {
  return typeof b === "string" ? Buffer.from(b, "utf8") : b;
}

function nullable(b: Bytes | null | undefined): Uint8Array | null {
  return b === null || b === undefined ? null : bytesOf(b);
}

/** One batch of `messages`. Throws `RangeError` for none. */
export function buildBatch(messages: readonly OutMessage[], now: bigint = BigInt(Date.now())): Uint8Array {
  if (messages.length === 0) {
    throw new RangeError("there is no message to produce");
  }
  const b = new BatchBuilder();
  for (const m of messages) {
    const headers: Header[] = (m.headers ?? []).map((h) => ({ key: bytesOf(h.name), value: nullable(h.value) }));
    const ts = m.timestamp === undefined ? now : BigInt(m.timestamp);
    b.push(ts, nullable(m.key), nullable(m.value), headers);
  }
  return b.finish();
}

/** Where a produced batch went. */
export interface ProduceResult {
  /** The offset of the batch's first record; the batch is committed. */
  readonly baseOffset: bigint;
  /** The leader's append time in milliseconds since the epoch, or -1. */
  readonly appendTimestamp: bigint;
}

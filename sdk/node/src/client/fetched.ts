import type { AbortedTxn } from "../proto/messages.js";
import { ErrorCode } from "../proto/error-code.js";
import { Batch } from "../record/batch.js";
import { ControlType, readMarker } from "../record/control.js";
import { RecordFormatError } from "../record/errors.js";
import type { BatchRecord } from "../record/types.js";
import { compareU64 } from "../record/unsigned.js";
import { BrokerResponseError } from "./errors.js";

/**
 * Records read from one partition.
 *
 * `records` holds whole batches exactly as the broker sent them, control batches and aborted ones included. A reader
 * that wants read_committed semantics uses `committedBatches()` or `committedMessages()`, which leave out what it must
 * not see. Offsets are `bigint` bit patterns of `u64` (see `unsigned.ts`).
 */
export class Fetched {
  constructor(
    /** Whole record batches. */
    readonly records: Uint8Array,
    /** Offsets below this are committed. */
    readonly highWatermark: bigint,
    /** Offsets below this are committed and in no open transaction; reads stop here. */
    readonly lastStableOffset: bigint,
    /** The first offset still in the partition. */
    readonly logStartOffset: bigint,
    /** Aborted transactions with data in `records`: producer id and first offset. */
    readonly aborted: readonly AbortedTxn[],
  ) {}

  /** The offset to fetch next: after the last batch, or `from` if there is none. Reading stops at the first batch that does not parse. */
  nextOffset(from: bigint): bigint {
    let next = from;
    let rest = this.records;
    while (rest.length > 0) {
      let split: { batch: Batch; rest: Uint8Array };
      try {
        split = Batch.parse(rest);
      } catch (e) {
        if (e instanceof RecordFormatError) {
          break;
        }
        throw e;
      }
      next = split.batch.nextOffset;
      rest = split.rest;
    }
    return next;
  }

  /**
   * The data batches a read_committed consumer sees: control batches and batches of aborted transactions left out. An
   * aborted transaction's batches are those of its producer from its first offset up to the producer's ABORT marker.
   *
   * @throws BrokerResponseError with `CORRUPT_BATCH` if the records do not parse
   */
  committedBatches(): Batch[] {
    const pending = [...this.aborted].sort((a, b) => compareU64(a.firstOffset, b.firstOffset));
    let next = 0;
    const skipping = new Set<bigint>();
    const out: Batch[] = [];
    let rest = this.records;
    while (rest.length > 0) {
      let split: { batch: Batch; rest: Uint8Array };
      try {
        split = Batch.parse(rest);
      } catch (e) {
        if (e instanceof RecordFormatError) {
          throw new BrokerResponseError(ErrorCode.CORRUPT_BATCH, e.message);
        }
        throw e;
      }
      const b = split.batch;
      rest = split.rest;
      while (next < pending.length && compareU64((pending[next] as AbortedTxn).firstOffset, b.baseOffset) <= 0) {
        skipping.add((pending[next] as AbortedTxn).producerId);
        next++;
      }
      if (b.attributes.control) {
        if (isAbortMarker(b)) {
          skipping.delete(b.producerId);
        }
        continue;
      }
      if (b.attributes.transactional && skipping.has(b.producerId)) {
        continue;
      }
      out.push(b);
    }
    return out;
  }

  /**
   * The records of `committedBatches()`, in offset order.
   *
   * @throws RecordFormatError of kind `UNSUPPORTED_COMPRESSION` if a batch is compressed with a codec this Node.js
   *   does not have (ADR 0085)
   */
  committedMessages(): BatchRecord[] {
    const out: BatchRecord[] = [];
    for (const b of this.committedBatches()) {
      try {
        out.push(...b.records());
      } catch (e) {
        if (e instanceof RecordFormatError) {
          // A codec missing here is no damage: say which one (ADR 0085).
          if (e.kind === "UNSUPPORTED_COMPRESSION") {
            throw e;
          }
          throw new BrokerResponseError(ErrorCode.CORRUPT_BATCH, e.message);
        }
        throw e;
      }
    }
    return out;
  }
}

function isAbortMarker(b: Batch): boolean {
  try {
    return readMarker(b).type === ControlType.ABORT;
  } catch (e) {
    if (e instanceof RecordFormatError) {
      return false; // not a transaction marker: Raft's or a group's own entry
    }
    throw e;
  }
}

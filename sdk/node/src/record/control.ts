import { BatchKind, Compression } from "./attributes.js";
import { Batch } from "./batch.js";
import { BatchBuilder } from "./builder.js";
import { getU32, putU16, putU32 } from "./bytes.js";
import { RecordFormatError } from "./errors.js";
import type { BatchRecord } from "./types.js";
import { fitsU32 } from "./unsigned.js";

/** What a control batch's key says it is. Types 0 and 1 are transaction markers; the rest belong to the broker. */
export const ControlKind = {
  ABORT_MARKER: 0,
  COMMIT_MARKER: 1,
  RAFT_NOOP: 2,
  RAFT_CONFIG: 3,
  PRODUCER_ID: 4,
  TXN_STATE: 5,
  GROUP_STATE: 6,
  GROUP_OFFSETS: 7,
  METADATA: 8,
  FORGET: 9,
  EXPIRE_PRODUCERS: 10,
} as const;
export type ControlKind = (typeof ControlKind)[keyof typeof ControlKind];

/** The two kinds of transaction marker. */
export const ControlType = { ABORT: 0, COMMIT: 1 } as const;
export type ControlType = (typeof ControlType)[keyof typeof ControlType];

export interface ControlMarker {
  readonly type: ControlType;
  readonly coordinatorEpoch: number;
}

/** Builds a transaction marker batch, as a node does after `EndTxn`. */
export function marker(
  producerId: bigint,
  producerEpoch: number,
  type: ControlType,
  coordinatorEpoch: number,
  timestamp: bigint,
): Uint8Array {
  if (!fitsU32(coordinatorEpoch)) {
    throw new RangeError(`coordinator epoch is not a u32: ${coordinatorEpoch}`);
  }
  const b = new BatchBuilder({
    attributes: { compression: Compression.NONE, transactional: true, control: true, kind: BatchKind.DATA },
    producerId,
    producerEpoch,
    baseSequence: -1,
  });
  const key = new Uint8Array(4);
  putU16(key, 2, type);
  const value = new Uint8Array(4);
  putU32(value, 0, coordinatorEpoch);
  b.push(timestamp, key, value);
  return b.finish();
}

function firstRecord(batch: Batch): BatchRecord {
  const rec = batch.records()[0];
  if (rec === undefined) {
    throw RecordFormatError.malformed("empty control batch");
  }
  return rec;
}

function controlCode(key: Uint8Array): number {
  return (key[2] as number) | ((key[3] as number) << 8);
}

/** The kind of a control batch, or `null` for a batch that is not one. */
export function controlKind(batch: Batch): ControlKind | null {
  if (!batch.attributes.control) {
    return null;
  }
  const rec = firstRecord(batch);
  if (rec.key === null) {
    throw RecordFormatError.malformed("control record without key");
  }
  if (rec.key.length !== 4) {
    throw RecordFormatError.malformed("control record has the wrong size");
  }
  const code = controlCode(rec.key);
  if (code > ControlKind.EXPIRE_PRODUCERS) {
    throw RecordFormatError.malformed("unknown control type");
  }
  if (code === ControlKind.ABORT_MARKER || code === ControlKind.COMMIT_MARKER) {
    if ((rec.value ?? new Uint8Array(0)).length !== 4) {
      throw RecordFormatError.malformed("marker value has the wrong size");
    }
  }
  return code as ControlKind;
}

/** Reads a commit or abort marker. */
export function readMarker(batch: Batch): ControlMarker {
  if (!batch.attributes.control) {
    throw RecordFormatError.malformed("not a control batch");
  }
  const rec = firstRecord(batch);
  if (rec.key === null) {
    throw RecordFormatError.malformed("control record without key");
  }
  if (rec.value === null) {
    throw RecordFormatError.malformed("control record without value");
  }
  if (rec.key.length !== 4 || rec.value.length !== 4) {
    throw RecordFormatError.malformed("control record has the wrong size");
  }
  const code = controlCode(rec.key);
  if (code !== ControlType.ABORT && code !== ControlType.COMMIT) {
    throw RecordFormatError.malformed("unknown control type");
  }
  return { type: code, coordinatorEpoch: getU32(rec.value, 0) };
}

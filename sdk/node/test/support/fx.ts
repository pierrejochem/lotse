import { type ClientOptions } from "../../src/client/index.js";
import {
  type AbortedTxn,
  ErrorCode,
  type ErrorCodeValue,
  type LeaderHint,
  type PartitionMetadata,
  type Request,
  type ResourceMetadata,
  type Response,
  ResourceKind,
} from "../../src/index.js";
import { Batch, BatchBuilder } from "../../src/index.js";
import type { FakeNode } from "./fake-node.js";

type Of<T extends Response["type"]> = Extract<Response, { type: T }>;

export const STREAM_ID = 7n;

/** A client configuration for fake brokers: plain TCP, short timeouts so that a test that should fail fails soon. */
export function config(nodes: readonly FakeNode[], extra: Partial<ClientOptions> = {}): ClientOptions {
  return {
    bootstrap: nodes.map((n) => n.hostPort),
    plaintextOnLoopback: true,
    requestTimeoutMs: 2_000,
    retryForMs: 2_000,
    backoffMs: 5,
    ...extra,
  };
}

export function metadata(nodes: readonly FakeNode[], ...resources: ResourceMetadata[]): Of<"Metadata"> {
  return {
    type: "Metadata",
    brokers: nodes.map((n) => ({ nodeId: n.id, host: "127.0.0.1", port: n.address.port, rack: "" })),
    resources,
  };
}

export function partition(number: number, leader: number, term: bigint | number, ...voters: number[]): PartitionMetadata {
  return { partition: number, leader, leaderTerm: BigInt(term), voters };
}

export function stream(name: string, id: bigint | number, ...partitions: PartitionMetadata[]): ResourceMetadata {
  return {
    error: ErrorCode.NONE,
    name,
    tenant: "default",
    resourceId: BigInt(id),
    kind: ResourceKind.STREAM,
    partitions,
    exchange: null,
  };
}

export function unknown(name: string): ResourceMetadata {
  return {
    error: ErrorCode.UNKNOWN_RESOURCE,
    name,
    tenant: "",
    resourceId: 0n,
    kind: ResourceKind.STREAM,
    partitions: [],
    exchange: null,
  };
}

export function produced(
  resourceId: bigint | number,
  partition: number,
  baseOffset: bigint | number,
  code: ErrorCodeValue = ErrorCode.NONE,
  hint: LeaderHint | null = null,
): Of<"Produce"> {
  return {
    type: "Produce",
    throttleMs: 0,
    resources: [
      {
        resourceId: BigInt(resourceId),
        partitions: [{ partition, error: code, baseOffset: BigInt(baseOffset), appendTs: -1n, errorMsg: null, leaderHint: hint }],
      },
    ],
  };
}

export interface FetchedParts {
  readonly records?: Uint8Array;
  readonly highWatermark?: bigint | number;
  readonly lastStable?: bigint | number;
  readonly logStart?: bigint | number;
  readonly aborted?: readonly AbortedTxn[];
  readonly code?: ErrorCodeValue;
  readonly hint?: LeaderHint | null;
}

export function fetched(resourceId: bigint | number, partition: number, parts: FetchedParts = {}): Of<"Fetch"> {
  return {
    type: "Fetch",
    throttleMs: 0,
    resources: [
      {
        resourceId: BigInt(resourceId),
        partitions: [
          {
            partition,
            error: parts.code ?? ErrorCode.NONE,
            highWatermark: BigInt(parts.highWatermark ?? 0),
            lastStableOffset: BigInt(parts.lastStable ?? 0),
            logStartOffset: BigInt(parts.logStart ?? 0),
            abortedTxns: parts.aborted ?? [],
            records: parts.records ?? new Uint8Array(0),
            leaderHint: parts.hint ?? null,
          },
        ],
      },
    ],
  };
}

export function listed(resourceId: bigint | number, partition: number, offset: bigint | number, code: ErrorCodeValue = ErrorCode.NONE, hint: LeaderHint | null = null): Of<"ListOffsets"> {
  return {
    type: "ListOffsets",
    resources: [
      { resourceId: BigInt(resourceId), partitions: [{ partition, error: code, offset: BigInt(offset), timestamp: -1n, leaderHint: hint }] },
    ],
  };
}

export function coordinator(n: FakeNode): Of<"FindCoordinator"> {
  return { type: "FindCoordinator", error: ErrorCode.NONE, nodeId: n.id, host: "127.0.0.1", port: n.address.port, errorMsg: null };
}

export function producer(id: bigint | number, epoch: number): Of<"InitProducer"> {
  return { type: "InitProducer", error: ErrorCode.NONE, producerId: BigInt(id), producerEpoch: epoch, leaderHint: null };
}

export function produceOf(r: Request): Extract<Request, { type: "Produce" }> {
  if (r.type !== "Produce") {
    throw new Error(`not a produce: ${r.type}`);
  }
  return r;
}

/** The batch bytes of a produce request's only partition. */
export function bytesOf(r: Request): Uint8Array {
  const p = produceOf(r).resources[0]?.partitions[0];
  if (p === undefined) {
    throw new Error("no partition");
  }
  return p.batch;
}

/** A queue: a resource of kind queue with one partition, 0. */
export function queue(name: string, id: bigint | number, ...partitions: PartitionMetadata[]): ResourceMetadata {
  return { ...stream(name, id, ...partitions), kind: ResourceKind.QUEUE };
}

/** A batch of data records at `base`, as the broker stores it. */
export function data(base: number | bigint, ...values: string[]): Uint8Array {
  const b = new BatchBuilder();
  for (const v of values) {
    b.push(1_000n, null, Buffer.from(v));
  }
  const bytes = b.finish();
  Batch.stamp(bytes, BigInt(base), 1n);
  return bytes;
}

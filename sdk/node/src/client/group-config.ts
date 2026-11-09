import { OffsetAt } from "./offset-at.js";

/** How a consumer group member behaves; see `LotseClient.joinGroup`. Durations are milliseconds. */
export interface GroupConfig {
  /**
   * After this long without a successful heartbeat, counted from when the last successful one was sent, the member drops
   * every partition and joins again as a new member. It must not exceed the brokers' group session timeout. The member
   * heartbeats only from `poll`: call it more often than this. Default 45 000.
   */
  readonly sessionTimeoutMs?: number;
  /** How long the member may take to release a revoked partition. Default 60 000. */
  readonly rebalanceTimeoutMs?: number;
  /**
   * Commit a revoked partition's position before releasing it, and every position on `close`. Turn it off for
   * exactly-once processing, where offsets go with each transaction (`Transaction.sendOffsets`). Default true.
   */
  readonly autoCommit?: boolean;
  /** Where to start a partition the group has committed no offset for (ADR 0059). Default the log start. */
  readonly startFrom?: OffsetAt;
  /**
   * A stable id for this member across restarts (ADR 0072). A member that comes back with it within the session timeout
   * takes over its partitions without a rebalance; a second instance with the same id takes over too, and the first
   * then gets `FENCED_INSTANCE_ID` and stops. Leaving keeps the partitions for the session timeout. Default none.
   */
  readonly instanceId?: string;
}

/** `GroupConfig` with every default filled in and every value checked. */
export interface ResolvedGroupConfig {
  readonly sessionTimeoutMs: number;
  readonly rebalanceTimeoutMs: number;
  readonly autoCommit: boolean;
  readonly startFrom: OffsetAt;
  readonly instanceId: string | null;
}

function positive(name: string, v: number | undefined, fallback: number): number {
  const x = v ?? fallback;
  if (!Number.isFinite(x) || x <= 0) {
    throw new RangeError(`${name} must be positive, not ${x}`);
  }
  return x;
}

export function resolveGroupConfig(c: GroupConfig = {}): ResolvedGroupConfig {
  if (c.instanceId !== undefined && c.instanceId.length === 0) {
    throw new RangeError("an instance id is not empty");
  }
  return {
    sessionTimeoutMs: positive("sessionTimeoutMs", c.sessionTimeoutMs, 45_000),
    rebalanceTimeoutMs: positive("rebalanceTimeoutMs", c.rebalanceTimeoutMs, 60_000),
    autoCommit: c.autoCommit ?? true,
    startFrom: c.startFrom ?? OffsetAt.EARLIEST,
    instanceId: c.instanceId ?? null,
  };
}

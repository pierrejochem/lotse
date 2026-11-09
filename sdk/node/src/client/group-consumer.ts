import { setTimeout as sleep } from "node:timers/promises";
import { ErrorCode, isRetriable } from "../proto/error-code.js";
import { NO_OFFSET, type OffsetCommitResource, type TxnResource } from "../proto/messages.js";
import type { Request } from "../proto/request.js";
import type { Response } from "../proto/response.js";
import type { BatchRecord } from "../record/types.js";
import { compareU64 } from "../record/unsigned.js";
import { Attempt } from "./attempt.js";
import type { CallOptions } from "./client.js";
import { BrokerResponseError, ClientError, ConsumerClosedError, GaveUpError, OffsetOutOfRangeError } from "./errors.js";
import type { Fetched } from "./fetched.js";
import { type GroupConfig, type ResolvedGroupConfig, resolveGroupConfig } from "./group-config.js";
import { Mutex } from "./mutex.js";
import { OffsetAt } from "./offset-at.js";

/** What a group member needs from the client that made it. */
export interface GroupHost {
  readonly tenant: string;
  readonly backoffMs: number;
  retrying<T>(attempt: () => Promise<T>, signal?: AbortSignal, mayHaveChanged?: boolean): Promise<T>;
  callCoordinator(key: string, request: Request): Promise<Response>;
  forgetCoordinator(): void;
  streamId(name: string): bigint | undefined;
  streamName(id: bigint): string | undefined;
  metadata(): Promise<unknown>;
  listOffset(stream: string, partition: number, at: OffsetAt): Promise<bigint>;
  fetch(stream: string, partition: number, offset: bigint, maxWaitMs: number, options: CallOptions): Promise<Fetched>;
}

/** One record read by a group member. */
export interface GroupRecord {
  readonly stream: string;
  readonly partition: number;
  /** The record, its offset included. */
  readonly record: BatchRecord;
}

/** What `poll` and the iterator take besides the wait. */
export interface GroupPollOptions {
  /** Ends the wait: `poll` returns nothing, and the iterator ends. */
  readonly signal?: AbortSignal;
}

/** A partition and, for the entries of `positionEntries`, the next offset to read. */
export interface PartitionPosition {
  readonly stream: string;
  readonly partition: number;
  readonly offset: bigint;
}

interface Owned {
  readonly stream: string;
  readonly partition: number;
  /** The next offset to read, or `null` until the committed offset is known. */
  position: bigint | null;
}

function nowMs(): number {
  return performance.now();
}

function keyOf(stream: string, partition: number): string {
  return `${stream}\0${partition}`;
}

function byPartition(a: { stream: string; partition: number }, b: { stream: string; partition: number }): number {
  return a.stream < b.stream ? -1 : a.stream > b.stream ? 1 : a.partition - b.partition;
}

function fencedInstance(): BrokerResponseError {
  return new BrokerResponseError(ErrorCode.FENCED_INSTANCE_ID, "another member joined with this instance id");
}

/**
 * A member of a consumer group, from `LotseClient.joinGroup` (ADR 0033). The coordinator assigns partitions; the member
 * reads them with read_committed from the group's committed offsets. It follows the Rust client's `GroupConsumer` rule
 * for rule:
 *
 * - it heartbeats only from `poll`, when one is due: call `poll` more often than the session timeout, or the member
 *   loses its partitions;
 * - a revoked partition is no longer read from the moment the answer that revokes it arrives; with auto-commit its
 *   position is committed, then the next heartbeat, at once, reports it released, and only then may another member get it;
 * - when no heartbeat has succeeded for the session timeout, counted from when the last successful one was sent, every
 *   partition is dropped at once, since the coordinator may give them to others by then, and the member joins again as a
 *   new member;
 * - a partition whose committed offset retention has deleted goes on from the oldest record left.
 *
 * Calls to `poll` (and the iterator) take turns. `commit`, `rewind` and `close` may be called at any time; `close` waits
 * for the poll in progress.
 */
export class GroupConsumer implements AsyncIterable<GroupRecord> {
  private readonly cfg: ResolvedGroupConfig;
  private id = "";
  private epoch = 0;
  private readonly owned = new Map<string, Owned>();
  /** When the last heartbeat that succeeded was sent, or `null`. */
  private lastOk: number | null = null;
  private nextHeartbeat = nowMs();
  /** Reads the partitions round robin. */
  private turn = 0;
  /** A newer instance with the same instance id took this member's place (ADR 0072): it stays out. */
  private fenced = false;
  private closed = false;
  private closing: Promise<void> | null = null;
  private readonly polling = new Mutex();

  /** @internal Made by `LotseClient.joinGroup`. */
  constructor(
    private readonly host: GroupHost,
    /** The group's name. */
    readonly group: string,
    private readonly streams: readonly string[],
    /** Subscribed by pattern instead (ADR 0073), or `null`. */
    private readonly pattern: string | null,
    config: GroupConfig | undefined,
  ) {
    this.cfg = resolveGroupConfig(config);
  }

  /** The member id, empty until joined. */
  get memberId(): string {
    return this.id;
  }

  /** The member epoch. */
  get memberEpoch(): number {
    return this.epoch;
  }

  /** The partitions this member owns now, as stream name to ascending partitions. */
  assignment(): Map<string, number[]> {
    const out = new Map<string, number[]>();
    for (const o of [...this.owned.values()].sort(byPartition)) {
      const list = out.get(o.stream);
      if (list === undefined) {
        out.set(o.stream, [o.partition]);
      } else {
        list.push(o.partition);
      }
    }
    return out;
  }

  /** The next offset to read per owned partition whose position is known, as stream name to partition to offset. */
  positions(): Map<string, Map<number, bigint>> {
    const out = new Map<string, Map<number, bigint>>();
    for (const p of this.positionEntries()) {
      let m = out.get(p.stream);
      if (m === undefined) {
        m = new Map();
        out.set(p.stream, m);
      }
      m.set(p.partition, p.offset);
    }
    return out;
  }

  /** The known positions in stream and partition order. */
  positionEntries(): PartitionPosition[] {
    const out: PartitionPosition[] = [];
    for (const o of [...this.owned.values()].sort(byPartition)) {
      if (o.position !== null) {
        out.push({ stream: o.stream, partition: o.partition, offset: o.position });
      }
    }
    return out;
  }

  /**
   * Heartbeats when one is due, then reads what one owned partition has, waiting up to `waitMs`. Returns the records in
   * offset order; empty if there is nothing yet, or when `signal` fired.
   *
   * @throws BrokerResponseError with `FENCED_INSTANCE_ID` once a newer instance took this member's place
   * @throws ConsumerClosedError after `close`
   */
  async poll(waitMs: number, options: GroupPollOptions = {}): Promise<GroupRecord[]> {
    if (this.closed) {
      throw new ConsumerClosedError();
    }
    const release = await this.polling.acquire();
    try {
      return await this.pollLocked(waitMs, options.signal);
    } finally {
      release();
    }
  }

  /**
   * The records as an async iterator: `for await (const r of group.records())`. It polls without end, and ends when the
   * consumer is closed or `signal` fires; errors of `poll` are thrown from the loop.
   */
  async *records(options: GroupPollOptions & { readonly waitMs?: number } = {}): AsyncGenerator<GroupRecord, void> {
    const waitMs = options.waitMs ?? 500;
    while (!this.closed && options.signal?.aborted !== true) {
      let batch: GroupRecord[];
      try {
        batch = await this.poll(waitMs, options);
      } catch (e) {
        if (this.closed && e instanceof ConsumerClosedError) {
          return;
        }
        throw e;
      }
      for (const r of batch) {
        yield r;
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<GroupRecord> {
    return this.records();
  }

  private async pollLocked(waitMs: number, signal: AbortSignal | undefined): Promise<GroupRecord[]> {
    if (this.closed) {
      throw new ConsumerClosedError();
    }
    if (this.fenced) {
      throw fencedInstance();
    }
    const now = nowMs();
    if (this.lastOk !== null && now - this.lastOk >= this.cfg.sessionTimeoutMs) {
      // The coordinator may have given everything away by now.
      this.lost();
    }
    if (now >= this.nextHeartbeat || this.id === "") {
      await this.heartbeat();
    }
    if (this.closed) {
      return [];
    }
    await this.loadPositions();
    if (this.closed) {
      return [];
    }
    const ready = this.positionEntries();
    const untilHeartbeat = Math.max(0, this.nextHeartbeat - now);
    if (ready.length === 0) {
      const pause = Math.min(Math.max(0, this.nextHeartbeat - nowMs()), waitMs);
      await nap(Math.max(pause, 1), signal);
      return [];
    }
    this.turn++;
    const pick = ready[this.turn % ready.length] as PartitionPosition;
    const key = keyOf(pick.stream, pick.partition);
    const offset = pick.offset;
    const fetchWait = Math.max(Math.min(waitMs, untilHeartbeat), 1);
    let fetched: Fetched;
    try {
      fetched = await this.host.fetch(pick.stream, pick.partition, offset, fetchWait, signal === undefined ? {} : { signal });
    } catch (e) {
      if (e instanceof GaveUpError) {
        return [];
      }
      if (e instanceof OffsetOutOfRangeError) {
        // Retention deleted what the group had not read: go on from the oldest record left, as Kafka's earliest reset does.
        if (compareU64(e.logStart, offset) <= 0) {
          throw e;
        }
        const o = this.owned.get(key);
        if (o !== undefined && o.position === offset) {
          o.position = e.logStart;
        }
        return [];
      }
      throw e;
    }
    if (this.lastOk === null || nowMs() - this.lastOk >= this.cfg.sessionTimeoutMs) {
      this.lost();
      return [];
    }
    // Closed, revoked or lost meanwhile: nothing of it is handed out.
    const o = this.owned.get(key);
    if (this.closed || o === undefined || o.position !== offset) {
      return [];
    }
    const out: GroupRecord[] = fetched
      .committedMessages()
      .map((record) => ({ stream: pick.stream, partition: pick.partition, record }));
    o.position = fetched.nextOffset(offset);
    return out;
  }

  /** Everything is gone: no commits, join again as a new member. */
  private lost(): void {
    this.owned.clear();
    this.id = "";
    this.epoch = 0;
    this.lastOk = null;
  }

  /** One heartbeat. Only what retrying cannot fix (a refusal, a login that fails) is an error. */
  private async heartbeat(): Promise<void> {
    const sent = nowMs();
    const joining = this.id === "";
    const ids: bigint[] = [];
    for (const s of this.streams) {
      const id = this.host.streamId(s);
      if (id !== undefined) {
        ids.push(id);
      }
    }
    const req: Request = {
      type: "GroupHeartbeat",
      groupId: this.group,
      memberId: this.id,
      memberEpoch: joining ? 0 : this.epoch,
      rebalanceTimeoutMs: Math.min(this.cfg.rebalanceTimeoutMs, 0xffff_ffff),
      subscribed: joining && this.pattern === null ? ids : null,
      owned: this.ownedResources(),
      tenant: this.host.tenant,
      instanceId: this.cfg.instanceId,
      subscribedPattern: joining ? this.pattern : null,
    };
    const retrySoon = sent + this.host.backoffMs;
    let resp: Extract<Response, { type: "GroupHeartbeat" }>;
    try {
      const r = await this.host.callCoordinator(this.group, req);
      if (r.type !== "GroupHeartbeat") {
        this.host.forgetCoordinator();
        this.nextHeartbeat = retrySoon;
        return;
      }
      resp = r;
    } catch (a) {
      if (a instanceof Attempt) {
        if (a.kind === "FAIL") {
          throw a.error;
        }
        this.host.forgetCoordinator();
        this.nextHeartbeat = retrySoon;
        return;
      }
      throw a;
    }
    const code = resp.error;
    if (code === ErrorCode.UNKNOWN_MEMBER_ID || code === ErrorCode.FENCED_MEMBER_EPOCH) {
      this.lost();
      this.nextHeartbeat = retrySoon;
      return;
    }
    if (code === ErrorCode.FENCED_INSTANCE_ID) {
      // A newer instance with this instance id took the place: stop, rather than join again and take it back.
      this.lost();
      this.fenced = true;
      throw fencedInstance();
    }
    if (code !== ErrorCode.NONE) {
      if (isRetriable(code)) {
        this.host.forgetCoordinator();
      }
      this.nextHeartbeat = retrySoon;
      return;
    }
    this.lastOk = sent;
    this.id = resp.memberId;
    this.epoch = resp.memberEpoch;
    this.nextHeartbeat = sent + Math.min(resp.heartbeatIntervalMs, this.cfg.sessionTimeoutMs / 3);
    const assignment = resp.assignment;
    if (assignment === null) {
      return;
    }
    // A pattern may bring streams this client has not looked up yet.
    if (assignment.some((r) => this.host.streamName(r.resourceId) === undefined)) {
      try {
        await this.host.metadata();
      } catch (e) {
        if (!(e instanceof ClientError)) {
          throw e;
        }
        // the names come with a later heartbeat
      }
    }
    const target = new Map<string, { stream: string; partition: number }>();
    for (const r of assignment) {
      const name = this.host.streamName(r.resourceId);
      if (name === undefined) {
        continue;
      }
      for (const p of r.partitions) {
        target.set(keyOf(name, p), { stream: name, partition: p });
      }
    }
    const revoked: Owned[] = [];
    for (const [k, o] of this.owned) {
      if (!target.has(k)) {
        revoked.push(o);
      }
    }
    if (revoked.length > 0) {
      // Stop reading them now; commit where they got to, then report them released with the next heartbeat, which comes at
      // once.
      const positions: PartitionPosition[] = [];
      for (const o of revoked.sort(byPartition)) {
        this.owned.delete(keyOf(o.stream, o.partition));
        if (o.position !== null) {
          positions.push({ stream: o.stream, partition: o.partition, offset: o.position });
        }
      }
      if (this.cfg.autoCommit && positions.length > 0) {
        try {
          await this.commitPositions(positions);
        } catch (e) {
          if (!(e instanceof ClientError)) {
            throw e;
          }
          // the next owner starts from the last commit
        }
      }
      this.nextHeartbeat = nowMs();
    }
    for (const [k, t] of target) {
      if (!this.owned.has(k)) {
        this.owned.set(k, { stream: t.stream, partition: t.partition, position: null });
      }
    }
  }

  private ownedResources(): TxnResource[] {
    const by = new Map<bigint, number[]>();
    for (const o of [...this.owned.values()].sort(byPartition)) {
      const id = this.host.streamId(o.stream);
      if (id !== undefined) {
        const list = by.get(id);
        if (list === undefined) {
          by.set(id, [o.partition]);
        } else {
          list.push(o.partition);
        }
      }
    }
    return [...by.entries()].sort((a, b) => compareU64(a[0], b[0])).map(([resourceId, partitions]) => ({ resourceId, partitions }));
  }

  /**
   * Learns the committed offsets of partitions whose position is not known yet; a partition whose offset an open
   * transaction holds waits for it.
   */
  private async loadPositions(): Promise<void> {
    const by = new Map<bigint, number[]>();
    for (const o of [...this.owned.values()].sort(byPartition)) {
      if (o.position !== null) {
        continue;
      }
      const id = this.host.streamId(o.stream);
      if (id !== undefined) {
        const list = by.get(id);
        if (list === undefined) {
          by.set(id, [o.partition]);
        } else {
          list.push(o.partition);
        }
      }
    }
    if (by.size === 0) {
      return;
    }
    const resources = [...by.entries()].sort((a, b) => compareU64(a[0], b[0])).map(([resourceId, partitions]) => ({ resourceId, partitions }));
    const req: Request = { type: "OffsetFetch", groupId: this.group, requireStable: true, resources, tenant: this.host.tenant };
    let resp: Extract<Response, { type: "OffsetFetch" }>;
    try {
      const r = await this.host.callCoordinator(this.group, req);
      if (r.type !== "OffsetFetch") {
        this.host.forgetCoordinator();
        return;
      }
      resp = r;
    } catch (a) {
      if (a instanceof Attempt) {
        this.host.forgetCoordinator();
        return;
      }
      throw a;
    }
    for (const r of resp.resources) {
      const name = this.host.streamName(r.resourceId);
      if (name === undefined) {
        continue;
      }
      for (const p of r.partitions) {
        if (p.error !== ErrorCode.NONE) {
          continue;
        }
        let start: bigint;
        if (p.offset !== NO_OFFSET) {
          start = p.offset;
        } else if (this.cfg.startFrom === OffsetAt.EARLIEST) {
          // From the start: a position below the log start goes on from there anyway.
          start = 0n;
        } else {
          try {
            start = await this.host.listOffset(name, p.partition, this.cfg.startFrom);
          } catch (e) {
            if (!(e instanceof ClientError)) {
              throw e;
            }
            // asked again with the next fetch of positions
            continue;
          }
        }
        const o = this.owned.get(keyOf(name, p.partition));
        if (o !== undefined) {
          o.position = start;
        }
      }
    }
  }

  /**
   * Commits the position of every owned partition whose position is known.
   *
   * @throws BrokerResponseError for a refusal, such as `FENCED_MEMBER_EPOCH`
   */
  async commit(options: CallOptions = {}): Promise<void> {
    const positions = this.positionEntries();
    if (positions.length > 0) {
      await this.commitPositions(positions, options.signal);
    }
  }

  private async commitPositions(positions: readonly PartitionPosition[], signal?: AbortSignal): Promise<void> {
    const req: Request = {
      type: "CommitOffsets",
      groupId: this.group,
      memberId: this.id,
      memberEpoch: this.epoch,
      resources: offsetResources(this.host, positions),
      tenant: this.host.tenant,
    };
    await this.host.retrying(() => commitOnce(this.host, this.group, req), signal, true);
  }

  /** Forgets the read positions: every owned partition starts again from the group's committed offset, as after an aborted transaction. */
  rewind(): void {
    for (const o of this.owned.values()) {
      o.position = null;
    }
  }

  /** Leaves the group, committing positions first when auto-commit is on. Closing twice is harmless. */
  close(): Promise<void> {
    if (this.closing === null) {
      this.closed = true;
      this.closing = this.leave();
    }
    return this.closing;
  }

  private async leave(): Promise<void> {
    // The poll in progress ends first: it hands out nothing once the consumer is closed.
    const release = await this.polling.acquire();
    try {
      if (this.cfg.autoCommit) {
        try {
          await this.commit();
        } catch (e) {
          if (!(e instanceof ClientError)) {
            throw e;
          }
          // leaving matters more
        }
      }
      if (this.id === "") {
        return;
      }
      const req: Request = {
        type: "GroupHeartbeat",
        groupId: this.group,
        memberId: this.id,
        memberEpoch: -1,
        rebalanceTimeoutMs: 0,
        subscribed: null,
        owned: null,
        tenant: this.host.tenant,
        instanceId: this.cfg.instanceId,
        subscribedPattern: null,
      };
      try {
        await this.host.callCoordinator(this.group, req);
      } catch (e) {
        if (!(e instanceof Attempt) && !(e instanceof ClientError)) {
          throw e;
        }
        // the coordinator drops the member after the session timeout
      }
      this.owned.clear();
    } finally {
      release();
    }
  }
}

/** Waits `ms`, or until `signal` fires. */
async function nap(ms: number, signal: AbortSignal | undefined): Promise<void> {
  try {
    await sleep(ms, undefined, signal === undefined ? {} : { signal });
  } catch (e) {
    if ((e as { name?: string }).name !== "AbortError") {
      throw e;
    }
  }
}

/** The positions as commit resources, by stream id. */
export function offsetResources(host: Pick<GroupHost, "streamId">, positions: readonly PartitionPosition[]): OffsetCommitResource[] {
  const by = new Map<bigint, { partition: number; offset: bigint; leaderTerm: bigint; metadata: null }[]>();
  for (const p of positions) {
    const id = host.streamId(p.stream);
    if (id === undefined) {
      continue;
    }
    const entry = { partition: p.partition, offset: p.offset, leaderTerm: 0n, metadata: null };
    const list = by.get(id);
    if (list === undefined) {
      by.set(id, [entry]);
    } else {
      list.push(entry);
    }
  }
  return [...by.entries()].sort((a, b) => compareU64(a[0], b[0])).map(([resourceId, partitions]) => ({ resourceId, partitions }));
}

/** One `CommitOffsets` or `AddOffsetsToTxn` to the coordinator, keyed by `key`. */
export async function commitOnce(
  host: Pick<GroupHost, "callCoordinator" | "forgetCoordinator">,
  key: string,
  req: Request,
): Promise<void> {
  const r = await host.callCoordinator(key, req);
  if (r.type !== "CommitOffsets") {
    throw Attempt.retry("unexpected response");
  }
  for (const res of r.resources) {
    for (const p of res.partitions) {
      if (p.error === ErrorCode.NONE) {
        continue;
      }
      if (isRetriable(p.error)) {
        host.forgetCoordinator();
        throw Attempt.retryMaybeDone(String(p.error));
      }
      throw Attempt.fail(new BrokerResponseError(p.error, null));
    }
  }
}

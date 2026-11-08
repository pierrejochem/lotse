import { setTimeout as sleep } from "node:timers/promises";
import { AuthenticationError } from "../auth/errors.js";
import { type Address, addressKey, formatAddress } from "../net/address.js";
import type { Connection } from "../net/connection.js";
import { BrokerError, ConnectionError, InsecureTransportError } from "../net/errors.js";
import { ConnectionPool } from "../net/pool.js";
import { describeError, ErrorCode, isRetriable } from "../proto/error-code.js";
import {
  CoordinatorType,
  Isolation,
  type LeaderHint,
  type ResourceMetadata,
} from "../proto/messages.js";
import type { Request } from "../proto/request.js";
import type { Response } from "../proto/response.js";
import { Batch } from "../record/batch.js";
import { RecordFormatError } from "../record/errors.js";
import type { BatchRecord } from "../record/types.js";
import { compareU64, U32_MAX } from "../record/unsigned.js";
import { Attempt, CallFailure } from "./attempt.js";
import { type ClientConfig, type ClientOptions, resolveClientOptions } from "./config.js";
import {
  BrokerResponseError,
  ClientAuthenticationError,
  ClientClosedError,
  ClientError,
  GaveUpError,
  OffsetOutOfRangeError,
  ResourceExistsError,
  UnknownOutcomeError,
  UnknownStreamError,
} from "./errors.js";
import { Fetched } from "./fetched.js";
import { buildBatch, type OutMessage, type ProduceResult } from "./messages.js";
import { Mutex } from "./mutex.js";
import { OffsetAt } from "./offset-at.js";

/** One partition's place in the cluster, as metadata last said. */
interface Route {
  readonly resourceId: bigint;
  readonly leader: number;
  readonly term: bigint;
}

interface Routed {
  readonly route: Route;
  readonly address: Address;
}

/** The producer id this client writes with. */
interface Session {
  readonly producerId: bigint;
  readonly epoch: number;
  readonly next: Map<string, number>;
}

interface ProducerIds {
  readonly producerId: bigint;
  readonly epoch: number;
}

interface Stamp {
  readonly producerId: bigint;
  readonly epoch: number;
  readonly sequence: number;
}

/** What a call may be given besides its arguments. */
export interface CallOptions {
  /**
   * Abandons the call. A call that is given up while its outcome may be unknown (a produce that was sent) ends with
   * `UnknownOutcomeError`, and the client starts over with a new producer id, because reusing the sequence could make
   * the leader take the next batch for the abandoned one. Any other call ends with `GaveUpError`.
   */
  readonly signal?: AbortSignal;
}

/** The settings `readRecords` takes. */
export interface ReadOptions extends CallOptions {
  /** Where to start. Default: the first offset still stored. */
  readonly from?: bigint;
  /** How long the broker holds a fetch with nothing to return. Default 500. */
  readonly maxWaitMs?: number;
  /** End when a fetch comes back with nothing to read, instead of waiting for more. */
  readonly untilEnd?: boolean;
}

const ABORTED = Symbol("aborted");

/** Settles with `ABORTED` as a rejection once `signal` fires. `dispose` removes the listener. */
function abortGuard(signal: AbortSignal | undefined): { promise: Promise<never>; dispose: () => void } | null {
  if (signal === undefined) {
    return null;
  }
  let dispose = (): void => {};
  const promise = new Promise<never>((_, reject) => {
    if (signal.aborted) {
      reject(ABORTED);
      return;
    }
    const onAbort = (): void => reject(ABORTED);
    signal.addEventListener("abort", onAbort, { once: true });
    dispose = () => signal.removeEventListener("abort", onAbort);
  });
  promise.catch(() => {}); // nobody may be listening when it fires
  return { promise, dispose };
}

function nowMs(): number {
  return performance.now();
}

/**
 * A client for Lotse streams, written to behave like the Rust client (`lotse-client`) and the Java client.
 *
 * It looks up streams with `Metadata`, sends `Produce` and `Fetch` straight to each partition's leader, and follows
 * `NOT_LEADER` hints and metadata changes on its own. It connects on first use and keeps one connection per broker
 * address.
 *
 * **Produce is idempotent by default** (`ClientOptions.idempotent`). The client gets a producer id from the coordinator
 * (`FindCoordinator`, `InitProducer`), stamps every batch with it and a sequence number per partition, and retries a
 * batch whose outcome is unknown (the connection broke after sending, or `REQUEST_TIMED_OUT`) with the *same* sequence;
 * the leader recognises a batch it already has and answers with its offset. Produces to one partition go out one at a
 * time, so sequences arrive in order. Only when retrying runs out of time with the outcome still unknown does the caller
 * get `UnknownOutcomeError`; the client then starts over with a new producer id, so later batches do not wait for a gap
 * that may never fill. Without idempotence an unknown outcome is never retried, since a retry could write the batch
 * twice.
 *
 * Two produces to one partition run one after the other in the order they were called; produces to different
 * partitions run in parallel. Offsets, ids and terms are `bigint`.
 *
 * An `InsecureTransportError` (no TLS configured, or plain TCP to an address that is not loopback) is a mistake in the
 * configuration and is thrown as it is, not retried.
 */
export class LotseClient {
  private readonly cfg: ClientConfig;
  private readonly pool: ConnectionPool;
  private isClosed = false;

  private readonly brokers = new Map<number, Address>();
  private readonly routes = new Map<string, Route>();

  private session: Session | null = null;
  private coordinatorAddress: Address | null = null;
  private readonly partitionLocks = new Map<string, Mutex>();

  private metadataLeader: number | null = null;
  private adminTurn = 0;
  private fatal: ClientError | null = null;
  private readonly seen = new WeakSet<Connection>();

  /** Creates a client. It connects on first use. */
  constructor(options: ClientOptions) {
    this.cfg = resolveClientOptions(options);
    this.pool = new ConnectionPool(this.cfg.connection);
  }

  /** Closes every connection. Calls in progress fail. */
  close(): void {
    this.isClosed = true;
    this.pool.close();
  }

  // ---- metadata ----

  /**
   * Asks any reachable broker for metadata and remembers the leaders.
   *
   * @param names the resources to ask for; none asks for all
   * @throws GaveUpError if no broker answers within `retryForMs`
   * @throws ClientAuthenticationError if a broker does not accept the credentials
   */
  async metadata(...names: string[]): Promise<Extract<Response, { type: "Metadata" }>> {
    return this.metadataWith(undefined, names);
  }

  private async metadataWith(
    signal: AbortSignal | undefined,
    names: readonly string[],
  ): Promise<Extract<Response, { type: "Metadata" }>> {
    this.checkOpen();
    const start = nowMs();
    let why = "no broker known";
    for (;;) {
      for (const addr of this.candidates()) {
        try {
          const r = await this.call(addr, { type: "Metadata", names });
          if (r.type === "Metadata") {
            this.learn(r);
            return r;
          }
          why = `unexpected response ${r.type}`;
        } catch (e) {
          if (!(e instanceof CallFailure)) {
            throw e;
          }
          if (e.kind === "DENIED") {
            throw new ClientAuthenticationError(e.message);
          }
          why = `${formatAddress(addr)}: ${e.describe()}`;
        }
      }
      if (nowMs() - start >= this.cfg.retryForMs) {
        throw new GaveUpError(this.cfg.retryForMs, why);
      }
      if (!(await this.pause(signal))) {
        throw new GaveUpError(nowMs() - start, "aborted");
      }
    }
  }

  /** Brokers known so far, then the bootstrap list, without repeats. */
  private candidates(): Address[] {
    const out = new Map<string, Address>();
    for (const a of this.brokers.values()) {
      out.set(addressKey(a), a);
    }
    for (const a of this.cfg.bootstrap) {
      if (!out.has(addressKey(a))) {
        out.set(addressKey(a), a);
      }
    }
    return [...out.values()];
  }

  /** Asks every known broker and merges what they know: per name, the newest resource any of them knows. */
  private async metadataFromAll(asked: readonly string[]): Promise<ResourceMetadata[]> {
    const sorted = this.candidates().sort((a, b) => {
      const x = formatAddress(a);
      const y = formatAddress(b);
      return x < y ? -1 : x > y ? 1 : 0;
    });
    const merged: ResourceMetadata[] = [];
    for (const addr of sorted) {
      try {
        const m = await this.call(addr, { type: "Metadata", names: asked });
        if (m.type === "Metadata") {
          this.learn(m);
          mergeNewest(merged, m.resources);
        }
      } catch (e) {
        if (!(e instanceof CallFailure)) {
          throw e;
        }
        // a broker that does not answer knows nothing
      }
    }
    return merged;
  }

  private learn(m: Extract<Response, { type: "Metadata" }>): void {
    for (const b of m.brokers) {
      this.brokers.set(b.nodeId, { host: b.host, port: b.port });
    }
    for (const r of m.resources) {
      if (r.error !== ErrorCode.NONE) {
        continue;
      }
      for (const p of r.partitions) {
        this.routes.set(routeKey(r.name, p.partition), { resourceId: r.resourceId, leader: p.leader, term: p.leaderTerm });
      }
    }
  }

  // ---- routing ----

  /** The leader's address, refreshing metadata if none is known. */
  private async route(stream: string, partition: number, signal?: AbortSignal): Promise<Routed> {
    const known = this.routes.get(routeKey(stream, partition));
    const route = known !== undefined && known.leader !== 0 ? known : await this.resolve(stream, partition, signal);
    const address = this.brokers.get(route.leader);
    if (address === undefined) {
      this.forget(stream, partition);
      throw Attempt.retry(`no address for node ${route.leader}`);
    }
    return { route, address };
  }

  private async resolve(stream: string, partition: number, signal?: AbortSignal): Promise<Route> {
    let m: Extract<Response, { type: "Metadata" }>;
    try {
      m = await this.metadataWith(signal, [stream]);
    } catch (e) {
      if (e instanceof ClientError) {
        throw Attempt.fail(e);
      }
      throw e;
    }
    let resources: readonly ResourceMetadata[] = m.resources;
    if (find(resources, stream) === undefined) {
      // A resource created a moment ago may not have reached that broker yet: any broker that knows it will do.
      resources = await this.metadataFromAll([stream]);
    }
    const res = find(resources, stream);
    const p = res?.partitions.find((c) => c.partition === partition);
    if (res === undefined || p === undefined) {
      throw Attempt.fail(new UnknownStreamError(stream, partition));
    }
    let leader = p.leader;
    let term = p.leaderTerm;
    if (leader === 0) {
      // Partitions created at runtime live on some nodes only: their voters know the leader.
      for (const voter of p.voters) {
        const addr = this.brokers.get(voter);
        if (addr === undefined) {
          continue;
        }
        try {
          const vm = await this.call(addr, { type: "Metadata", names: [stream] });
          if (vm.type !== "Metadata") {
            continue;
          }
          this.learn(vm);
          const vr = find(vm.resources, stream, false);
          if (vr === undefined) {
            continue;
          }
          for (const vp of vr.partitions) {
            if (vp.partition === partition && vp.leader !== 0) {
              leader = vp.leader;
              term = vp.leaderTerm;
            }
          }
          if (leader !== 0) {
            break;
          }
        } catch (e) {
          if (!(e instanceof CallFailure)) {
            throw e;
          }
          // the next voter may know
        }
      }
    }
    if (leader === 0) {
      throw Attempt.retry("the partition has no leader");
    }
    return { resourceId: res.resourceId, leader, term };
  }

  private forget(stream: string, partition: number): void {
    this.routes.delete(routeKey(stream, partition));
  }

  /**
   * Follows a leader hint, or forgets the route so metadata is reloaded. A hint is followed only if its term is not
   * older than the route's and it names another leader: an older hint comes from a node that lags.
   */
  private redirect(stream: string, partition: number, hint: LeaderHint | null): void {
    const key = routeKey(stream, partition);
    const r = this.routes.get(key);
    if (hint !== null && r !== undefined && compareU64(hint.term, r.term) >= 0 && hint.leader !== r.leader) {
      this.routes.set(key, { resourceId: r.resourceId, leader: hint.leader, term: hint.term });
    } else {
      this.routes.delete(key);
    }
  }

  // ---- the retry loop ----

  /**
   * Runs `attempt` until it succeeds, fails for good, or `retryForMs` has passed. `mayHaveChanged` says that giving up
   * a call that is in flight leaves its outcome unknown (a produce, a create).
   */
  private async retrying<T>(attempt: () => Promise<T>, signal?: AbortSignal, mayHaveChanged = false): Promise<T> {
    const start = nowMs();
    let maybeDone = false;
    const guard = abortGuard(signal);
    try {
      for (;;) {
        this.checkOpen();
        let why: string;
        try {
          if (signal?.aborted === true) {
            throw ABORTED;
          }
          const running = attempt();
          if (guard === null) {
            return await running;
          }
          running.catch(() => {}); // an abandoned attempt may still fail, and nobody is waiting for it
          return await Promise.race([running, guard.promise]);
        } catch (e) {
          if (e === ABORTED) {
            throw maybeDone || mayHaveChanged
              ? new UnknownOutcomeError("the call was given up while its outcome was unknown")
              : new GaveUpError(nowMs() - start, "aborted");
          }
          if (!(e instanceof Attempt)) {
            throw e;
          }
          switch (e.kind) {
            case "FAIL": {
              // A failed login or a refusal now says nothing of an earlier attempt whose answer was lost: that one may
              // have been carried out. (An answer about the request itself, such as RESOURCE_EXISTS, is the caller's
              // to read.)
              const err = e.error as ClientError;
              const refusal =
                err instanceof ClientAuthenticationError ||
                (err instanceof BrokerResponseError && err.code === ErrorCode.UNAUTHORIZED);
              if (maybeDone && refusal) {
                throw new UnknownOutcomeError(`an earlier attempt may have been carried out; then ${err.message}`);
              }
              throw err;
            }
            case "RETRY":
              why = e.message;
              break;
            case "RETRY_MAYBE_DONE":
              maybeDone = true;
              why = e.message;
              break;
          }
        }
        // A connection that could not authenticate: the same credentials will not do better on the next try.
        const failedLogin = this.fatal;
        this.fatal = null;
        if (failedLogin !== null) {
          throw maybeDone
            ? new UnknownOutcomeError(`an earlier attempt may have been carried out; then ${failedLogin.message}`)
            : failedLogin;
        }
        if (nowMs() - start >= this.cfg.retryForMs) {
          throw maybeDone
            ? new UnknownOutcomeError(`still unknown after ${this.cfg.retryForMs} ms: ${why}`)
            : new GaveUpError(this.cfg.retryForMs, why);
        }
        if (!(await this.pause(signal))) {
          throw maybeDone
            ? new UnknownOutcomeError(`given up while the outcome was unknown: ${why}`)
            : new GaveUpError(nowMs() - start, "aborted");
        }
      }
    } finally {
      guard?.dispose();
    }
  }

  /** Sleeps one backoff; `false` if `signal` fired meanwhile. */
  private async pause(signal?: AbortSignal): Promise<boolean> {
    try {
      await sleep(this.cfg.backoffMs, undefined, signal === undefined ? {} : { signal });
      return true;
    } catch (e) {
      if ((e as { name?: string }).name === "AbortError") {
        return false;
      }
      throw e;
    }
  }

  // ---- produce ----

  /**
   * Appends messages to a partition as one batch and returns where it went once it is committed.
   *
   * @param partition the partition; Lotse's client does not choose one for you
   * @throws UnknownOutcomeError the outcome is unknown after retrying; see the class description
   * @throws GaveUpError nothing was written and no broker could be reached in time
   * @throws UnknownStreamError the stream or partition does not exist
   */
  async produce(
    stream: string,
    partition: number,
    messages: OutMessage | readonly OutMessage[],
    options: CallOptions = {},
  ): Promise<ProduceResult> {
    const list = Array.isArray(messages) ? (messages as readonly OutMessage[]) : [messages as OutMessage];
    return this.produceBatch(stream, partition, buildBatch(list), options);
  }

  /**
   * Appends a record batch built with `BatchBuilder` to a partition. Returns the offset of its first record once the
   * batch is committed. The batch is sent as it is, with the producer fields of this client written into a copy.
   */
  async produceBatch(
    stream: string,
    partition: number,
    batch: Uint8Array,
    options: CallOptions = {},
  ): Promise<ProduceResult> {
    this.checkOpen();
    const signal = options.signal;
    if (!this.cfg.idempotent) {
      return this.retrying(() => this.produceOnce(stream, partition, batch, false, signal), signal, true);
    }
    let records: number;
    try {
      records = Batch.parse(batch).batch.recordCount;
    } catch (e) {
      if (e instanceof RecordFormatError) {
        throw new BrokerResponseError(ErrorCode.CORRUPT_BATCH, e.message);
      }
      throw e;
    }
    const key = routeKey(stream, partition);
    // One produce per partition at a time, so sequence numbers reach the leader in order.
    let lock = this.partitionLocks.get(key);
    if (lock === undefined) {
      lock = new Mutex();
      this.partitionLocks.set(key, lock);
    }
    const guard = abortGuard(signal);
    let release: () => void;
    try {
      try {
        release = await lock.acquire(guard?.promise);
      } catch (e) {
        if (e === ABORTED) {
          throw new GaveUpError(0, "aborted while waiting for the partition's earlier produces");
        }
        throw e;
      }
    } finally {
      guard?.dispose();
    }
    try {
      const stamp = await this.nextSequence(key, signal);
      const stamped = batch.slice();
      Batch.setProducer(stamped, stamp.producerId, stamp.epoch, stamp.sequence);
      let result: ProduceResult;
      try {
        // Every attempt sends the same bytes: the same producer id and the same sequence.
        result = await this.retrying(() => this.produceOnce(stream, partition, stamped, true, signal), signal, true);
      } catch (e) {
        // Written or not, nobody can tell: later batches would wait behind a gap that may never fill. Start over with
        // a new producer id. Anything else was definitely not written, and the sequence is free for the next batch.
        // A named producer that is fenced has a newer instance: it keeps its fenced id rather than start over, which
        // would fence that one in turn (ADR 0071).
        const fencedByName =
          this.cfg.producerName !== null && e instanceof BrokerResponseError && e.code === ErrorCode.PRODUCER_FENCED;
        const startOver =
          !fencedByName &&
          (e instanceof UnknownOutcomeError ||
            (e instanceof BrokerResponseError &&
              (e.code === ErrorCode.OUT_OF_ORDER_SEQUENCE || e.code === ErrorCode.PRODUCER_FENCED)) ||
            !(e instanceof ClientError));
        if (startOver && this.session !== null && this.session.producerId === stamp.producerId) {
          this.session = null;
        }
        throw e;
      }
      if (this.session !== null && this.session.producerId === stamp.producerId) {
        this.session.next.set(key, (stamp.sequence + records) | 0);
      }
      return result;
    } finally {
      release();
    }
  }

  /** The producer id, epoch and next sequence for a partition, getting a producer id first if there is none. */
  private async nextSequence(key: string, signal?: AbortSignal): Promise<Stamp> {
    if (this.session !== null) {
      return stampOf(this.session, key);
    }
    const ids = await this.retrying(() => this.initProducerOnce(this.cfg.producerName), signal);
    // Another partition's produce may have started a session meanwhile.
    if (this.session === null) {
      this.session = { producerId: ids.producerId, epoch: ids.epoch, next: new Map() };
    }
    return stampOf(this.session, key);
  }

  private async produceOnce(
    stream: string,
    partition: number,
    batch: Uint8Array,
    idempotent: boolean,
    signal?: AbortSignal,
  ): Promise<ProduceResult> {
    // An idempotent batch may be sent again whatever happened to it.
    const routed = await this.route(stream, partition, signal);
    const req: Request = {
      type: "Produce",
      txnId: null,
      timeoutMs: Math.min(this.cfg.produceTimeoutMs, U32_MAX),
      resources: [{ resourceId: routed.route.resourceId, partitions: [{ partition, batch }] }],
      tenant: this.cfg.tenant,
    };
    let resp: Response;
    try {
      resp = await this.call(routed.address, req);
    } catch (e) {
      if (!(e instanceof CallFailure)) {
        throw e;
      }
      switch (e.kind) {
        case "NOT_SENT":
          // Not sent at all: safe to try again.
          this.forget(stream, partition);
          throw Attempt.retry(e.message);
        case "LOST":
          this.forget(stream, partition);
          throw unknown(idempotent, e.message);
        default:
          throw e.toAttempt();
      }
    }
    if (resp.type !== "Produce") {
      throw unknown(idempotent, `unexpected response ${resp.type}`);
    }
    const first = resp.resources[0]?.partitions[0];
    if (first === undefined) {
      throw unknown(idempotent, "empty produce response");
    }
    const code = first.error;
    if (code === ErrorCode.NONE) {
      return { baseOffset: first.baseOffset, appendTimestamp: first.appendTs };
    }
    if (code === ErrorCode.NOT_LEADER || code === ErrorCode.FENCED_LEADER_TERM) {
      // Refused before it was proposed, or definitely not written.
      this.redirect(stream, partition, first.leaderHint);
      throw Attempt.retry(describeError(code));
    }
    if (code === ErrorCode.REQUEST_TIMED_OUT) {
      this.forget(stream, partition);
      throw unknown(idempotent, first.errorMsg ?? describeError(code));
    }
    if (code === ErrorCode.UNKNOWN_RESOURCE) {
      // Deleted: the name may come back as a new resource.
      this.forget(stream, partition);
    }
    throw Attempt.fail(new BrokerResponseError(code, first.errorMsg));
  }

  // ---- the producer id ----

  /** The coordinator's address, asking any broker for it if needed. */
  private async coordinator(key: string): Promise<Address> {
    if (this.coordinatorAddress !== null) {
      return this.coordinatorAddress;
    }
    const find: Request = { type: "FindCoordinator", keyType: CoordinatorType.PRODUCER, key };
    let why = "no broker answered";
    for (const addr of this.candidates()) {
      try {
        const r = await this.call(addr, find);
        if (r.type === "FindCoordinator") {
          if (r.error === ErrorCode.NONE) {
            this.coordinatorAddress = { host: r.host, port: r.port };
            return this.coordinatorAddress;
          }
          why = r.errorMsg ?? describeError(r.error);
          if (!isRetriable(r.error)) {
            throw Attempt.fail(new BrokerResponseError(r.error, why));
          }
        } else {
          why = `unexpected response ${r.type}`;
        }
      } catch (e) {
        if (!(e instanceof CallFailure)) {
          throw e;
        }
        why = e.describe();
      }
    }
    throw Attempt.retry(why);
  }

  /** One request to the coordinator; failures become `Attempt`s (every coordinator request may be repeated). */
  private async callCoordinator(key: string, req: Request): Promise<Response> {
    const addr = await this.coordinator(key);
    try {
      return await this.call(addr, req);
    } catch (e) {
      if (e instanceof CallFailure) {
        this.coordinatorAddress = null;
        throw e.toAttempt();
      }
      throw e;
    }
  }

  private async initProducerOnce(txn: string | null): Promise<ProducerIds> {
    const init: Request = {
      type: "InitProducer",
      txnId: txn,
      txnTimeoutMs: Math.min(this.cfg.txnTimeoutMs, U32_MAX),
      tenant: this.cfg.tenant,
    };
    const resp = await this.callCoordinator(txn ?? "", init);
    if (resp.type !== "InitProducer") {
      throw Attempt.retry(`unexpected response ${resp.type}`);
    }
    if (resp.error === ErrorCode.NONE) {
      return { producerId: resp.producerId, epoch: resp.producerEpoch };
    }
    if (isRetriable(resp.error)) {
      // An id that was handed out but never received is just unused.
      this.coordinatorAddress = null;
      throw Attempt.retry(describeError(resp.error));
    }
    throw Attempt.fail(new BrokerResponseError(resp.error, null));
  }

  // ---- list offsets (ADR 0059) ----

  /**
   * Where `at` falls in a partition, as a read_committed reader sees it (ADR 0059): `OffsetAt.EARLIEST` is the first
   * offset still stored, `OffsetAt.LATEST` the offset after the last one it may read, and `OffsetAt.time` the first
   * offset of the first batch with a record at or after that time, or the latest offset if there is none yet.
   */
  async listOffset(stream: string, partition: number, at: OffsetAt, options: CallOptions = {}): Promise<bigint> {
    this.checkOpen();
    return this.retrying(() => this.listOffsetOnce(stream, partition, at, options.signal), options.signal);
  }

  private async listOffsetOnce(stream: string, partition: number, at: OffsetAt, signal?: AbortSignal): Promise<bigint> {
    const routed = await this.route(stream, partition, signal);
    const req: Request = {
      type: "ListOffsets",
      isolation: Isolation.COMMITTED,
      resources: [
        { resourceId: routed.route.resourceId, partitions: [{ partition, timestamp: at.timestamp }] },
      ],
    };
    let resp: Response;
    try {
      resp = await this.call(routed.address, req);
    } catch (e) {
      if (e instanceof CallFailure) {
        this.forget(stream, partition);
        throw e.toAttempt();
      }
      throw e;
    }
    if (resp.type !== "ListOffsets") {
      throw Attempt.retry(`unexpected response ${resp.type}`);
    }
    const p = resp.resources[0]?.partitions[0];
    if (p === undefined) {
      throw Attempt.retry("empty list-offsets response");
    }
    if (p.error === ErrorCode.NONE) {
      return p.offset;
    }
    if (isRetriable(p.error)) {
      this.redirect(stream, partition, p.leaderHint);
      throw Attempt.retry(describeError(p.error));
    }
    if (p.error === ErrorCode.UNKNOWN_RESOURCE) {
      this.forget(stream, partition);
    }
    throw Attempt.fail(new BrokerResponseError(p.error, null));
  }

  // ---- fetch ----

  /**
   * Reads committed batches from `offset`, waiting up to `maxWaitMs` at the broker for some to arrive (a long poll). Keep
   * `maxWaitMs` well below `requestTimeoutMs`.
   *
   * This is read_committed: reading stops at the last stable offset. Use `Fetched.committedBatches()` or
   * `Fetched.committedMessages()` to leave out control batches and aborted transactions.
   *
   * @throws OffsetOutOfRangeError retention deleted the offset, or it is past the end; the error says where the
   *   partition starts
   */
  fetch(stream: string, partition: number, offset: bigint, maxWaitMs: number, options: CallOptions = {}): Promise<Fetched> {
    return this.fetchWith(stream, partition, offset, maxWaitMs, Isolation.COMMITTED, options.signal);
  }

  /**
   * Like `fetch`, but also past the last stable offset: batches of open and aborted transactions and their markers, up
   * to the high watermark. For inspecting a log, not for processing it.
   */
  fetchUncommitted(
    stream: string,
    partition: number,
    offset: bigint,
    maxWaitMs: number,
    options: CallOptions = {},
  ): Promise<Fetched> {
    return this.fetchWith(stream, partition, offset, maxWaitMs, Isolation.UNCOMMITTED, options.signal);
  }

  private async fetchWith(
    stream: string,
    partition: number,
    offset: bigint,
    maxWaitMs: number,
    isolation: number,
    signal?: AbortSignal,
  ): Promise<Fetched> {
    this.checkOpen();
    return this.retrying(() => this.fetchOnce(stream, partition, offset, maxWaitMs, isolation, signal), signal);
  }

  private async fetchOnce(
    stream: string,
    partition: number,
    offset: bigint,
    maxWaitMs: number,
    isolation: number,
    signal?: AbortSignal,
  ): Promise<Fetched> {
    const routed = await this.route(stream, partition, signal);
    const req: Request = {
      type: "Fetch",
      maxWaitMs: Math.min(Math.max(0, Math.trunc(maxWaitMs)), U32_MAX),
      minBytes: 1,
      maxBytes: 4 << 20,
      isolation,
      resources: [
        {
          resourceId: routed.route.resourceId,
          partitions: [{ partition, fetchOffset: offset, leaderTerm: routed.route.term, maxBytes: 1 << 20 }],
        },
      ],
    };
    let resp: Response;
    try {
      resp = await this.call(routed.address, req);
    } catch (e) {
      if (e instanceof CallFailure) {
        this.forget(stream, partition);
        throw e.toAttempt();
      }
      throw e;
    }
    if (resp.type !== "Fetch") {
      throw Attempt.retry(`unexpected response ${resp.type}`);
    }
    const p = resp.resources[0]?.partitions[0];
    if (p === undefined) {
      throw Attempt.retry("empty fetch response");
    }
    if (p.error === ErrorCode.NONE) {
      return new Fetched(p.records, p.highWatermark, p.lastStableOffset, p.logStartOffset, p.abortedTxns);
    }
    if (isRetriable(p.error)) {
      this.redirect(stream, partition, p.leaderHint);
      throw Attempt.retry(describeError(p.error));
    }
    if (p.error === ErrorCode.OFFSET_OUT_OF_RANGE) {
      throw Attempt.fail(new OffsetOutOfRangeError(stream, partition, offset, p.logStartOffset));
    }
    if (p.error === ErrorCode.UNKNOWN_RESOURCE) {
      this.forget(stream, partition);
    }
    throw Attempt.fail(new BrokerResponseError(p.error, null));
  }

  /**
   * The committed records of a partition as an async iterator: `for await (const record of client.readRecords(...))`.
   * It fetches with `fetch`, hands out `committedMessages()` and goes on from `nextOffset`. It ends when `untilEnd` is
   * set and a fetch returns nothing, or when `signal` fires (then it ends quietly); errors of `fetch` are thrown from
   * the loop, among them `OffsetOutOfRangeError`, which says where to continue.
   */
  async *readRecords(stream: string, partition: number, options: ReadOptions = {}): AsyncGenerator<BatchRecord, void> {
    const maxWaitMs = options.maxWaitMs ?? 500;
    let offset = options.from ?? (await this.listOffset(stream, partition, OffsetAt.EARLIEST, options));
    const aborted = (): boolean => options.signal?.aborted === true;
    for (;;) {
      if (aborted()) {
        return;
      }
      let fetched: Fetched;
      try {
        fetched = await this.fetch(stream, partition, offset, maxWaitMs, options);
      } catch (e) {
        if (aborted() && e instanceof GaveUpError) {
          return;
        }
        throw e;
      }
      const next = fetched.nextOffset(offset);
      const records = fetched.committedMessages();
      for (const r of records) {
        yield r;
      }
      if (fetched.records.length === 0 && options.untilEnd === true) {
        return;
      }
      offset = next;
    }
  }

  // ---- admin (ADR 0034) ----
  //
  // Admin requests go to the metadata group's leader. Any broker answers NOT_LEADER with a hint to it, which the client
  // follows. A create whose outcome is unknown is retried; if the first attempt did create the resource, the retry finds
  // it and the call fails with ResourceExistsError with its id.

  /**
   * Creates a stream in the configured tenant and returns its id.
   *
   * A create whose outcome is unknown is retried. If the first attempt did create the stream, the retry finds it and the
   * call fails with `ResourceExistsError`, which carries the id.
   *
   * @param partitions the number of partitions
   * @param replication the number of nodes holding each partition
   * @param configs settings such as `retention.ms` and `retention.bytes`
   */
  async createStream(
    name: string,
    partitions: number,
    replication: number,
    configs: Readonly<Record<string, string>> = {},
    options: CallOptions = {},
  ): Promise<bigint> {
    this.checkOpen();
    const list = Object.keys(configs)
      .sort()
      .map((key) => ({ key, value: configs[key] as string }));
    return this.create(
      name,
      { type: "CreateStream", name, tenant: this.cfg.tenant, partitions, replication, configs: list },
      options.signal,
    );
  }

  /** Sends a create request to the metadata leader and returns the id of what was created. */
  private create(name: string, req: Request, signal?: AbortSignal): Promise<bigint> {
    return this.retrying(
      async () => {
        const r = await this.callAdmin(req);
        if (r.type !== "Create") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        if (r.error === ErrorCode.NONE) {
          return r.resourceId;
        }
        if (r.error === ErrorCode.RESOURCE_EXISTS) {
          throw Attempt.fail(new ResourceExistsError(name, r.resourceId));
        }
        if (isRetriable(r.error)) {
          this.metadataLeader = r.leaderHint === null ? null : r.leaderHint.leader;
          throw Attempt.retryMaybeDone(describeError(r.error));
        }
        throw Attempt.fail(new BrokerResponseError(r.error, r.errorMsg));
      },
      signal,
      true,
    );
  }

  /** Sends an admin request to the metadata leader, or any broker if it is not known. */
  private async callAdmin(req: Request): Promise<Response> {
    const leader = this.metadataLeader;
    // A leader whose address this client has not seen (a node added since it last asked): learn the cluster's nodes
    // first, or it keeps asking the others.
    if (leader !== null && !this.brokers.has(leader)) {
      try {
        await this.metadata();
      } catch (e) {
        if (!(e instanceof ClientError)) {
          throw e;
        }
        // the retry loop reports what is still wrong
      }
    }
    let addr = leader === null ? undefined : this.brokers.get(leader);
    if (addr === undefined) {
      let targets = [...this.brokers.values()];
      if (targets.length === 0) {
        targets = [...this.cfg.bootstrap];
      }
      addr = targets[this.adminTurn++ % targets.length] as Address;
    }
    try {
      return await this.call(addr, req);
    } catch (e) {
      if (!(e instanceof CallFailure)) {
        throw e;
      }
      if (e.kind === "DENIED" || e.kind === "REFUSED") {
        throw e.toAttempt();
      }
      this.metadataLeader = null;
      // The request may have arrived: a retry finds what it did.
      throw Attempt.retryMaybeDone(e.describe());
    }
  }

  // ---- one request to one broker ----

  /**
   * Sends one request to `addr` and waits for its response. A connection that failed once is not trusted again: it is
   * closed and the next call opens a new one.
   *
   * @throws CallFailure when there is no response
   */
  private async call(addr: Address, req: Request): Promise<Response> {
    return this.callOn(await this.connect(addr), req);
  }

  /** The pooled connection to `addr`, logged in. */
  private async connect(addr: Address): Promise<Connection> {
    this.checkOpen();
    let c: Connection;
    try {
      c = await this.pool.get(addr);
    } catch (e) {
      if (e instanceof InsecureTransportError) {
        throw e;
      }
      if (e instanceof AuthenticationError) {
        const f = CallFailure.denied(e.message);
        this.noteFatal(f);
        throw f;
      }
      if (e instanceof ConnectionError) {
        throw CallFailure.notSent(e.message);
      }
      throw e;
    }
    if (!this.seen.has(c)) {
      // A new connection logged in (if it has credentials): an earlier failed login no longer holds.
      this.seen.add(c);
      this.fatal = null;
    }
    return c;
  }

  /** Sends one request on a given connection and waits for its response; failures are handled as in `call`. */
  private async callOn(c: Connection, req: Request): Promise<Response> {
    try {
      return await c.send(req);
    } catch (e) {
      if (e instanceof BrokerError) {
        this.pool.discard(c);
        // Not authenticated, or the session ended: sending again with the same credentials gets the same answer.
        // Refused as a whole with a reason: not carried out, and the same request would be refused again.
        let f: CallFailure;
        if (e.code === ErrorCode.UNAUTHORIZED && e.reason === null) {
          f = CallFailure.denied("the node requires authentication");
        } else if (e.reason === null) {
          f = CallFailure.notSent(`refused: ${describeError(e.code)}`);
        } else {
          f = CallFailure.refused(e.code, e.reason);
        }
        this.noteFatal(f);
        throw f;
      }
      if (e instanceof ConnectionError) {
        this.pool.discard(c);
        throw e.requestSent ? CallFailure.lost(e.message) : CallFailure.notSent(e.message);
      }
      throw e;
    }
  }

  /**
   * Keeps a failed login for the retry loop to report: it is the client's credentials that fail, so it holds for every
   * request, also one whose attempt only saw a metadata lookup fail. A refused request is not kept: its own caller gets
   * the refusal, and keeping it would report it for another request.
   */
  private noteFatal(f: CallFailure): void {
    if (f.kind === "DENIED") {
      this.fatal = new ClientAuthenticationError(f.message);
    }
  }

  private checkOpen(): void {
    if (this.isClosed) {
      throw new ClientClosedError();
    }
  }

  // ---- for tests ----

  /** The route this client holds for a partition, or `undefined`. */
  routeOf(stream: string, partition: number): { resourceId: bigint; leader: number; term: bigint } | undefined {
    return this.routes.get(routeKey(stream, partition));
  }

  /** The producer id of the current session, or 0n if there is none. */
  producerId(): bigint {
    return this.session === null ? 0n : this.session.producerId;
  }
}

function routeKey(stream: string, partition: number): string {
  return `${stream}\0${partition}`;
}

function stampOf(s: Session, key: string): Stamp {
  return { producerId: s.producerId, epoch: s.epoch, sequence: s.next.get(key) ?? 0 };
}

function unknown(idempotent: boolean, why: string): Attempt {
  return idempotent ? Attempt.retryMaybeDone(why) : Attempt.fail(new UnknownOutcomeError(why));
}

function find(resources: readonly ResourceMetadata[], name: string, mustBeOk = true): ResourceMetadata | undefined {
  return resources.find((r) => r.name === name && (!mustBeOk || r.error === ErrorCode.NONE));
}

/**
 * Adds what one broker knows to `merged`, keeping per name the newest resource: a name deleted and created again has a
 * new, higher id (ids grow with the metadata log), and a broker that lags may still report the old one. A resource with
 * an error names nothing.
 */
export function mergeNewest(merged: ResourceMetadata[], more: readonly ResourceMetadata[]): void {
  for (const r of more) {
    if (r.error !== ErrorCode.NONE) {
      continue;
    }
    const at = merged.findIndex((m) => m.name === r.name);
    if (at < 0) {
      merged.push(r);
    } else if (compareU64((merged[at] as ResourceMetadata).resourceId, r.resourceId) < 0) {
      merged[at] = r;
    }
  }
}

import { setTimeout as sleep } from "node:timers/promises";
import { AuthenticationError } from "../auth/errors.js";
import { ScramCredential } from "../auth/scram-credential.js";
import { type Address, addressKey, formatAddress } from "../net/address.js";
import type { Connection } from "../net/connection.js";
import { BrokerError, ConnectionError, InsecureTransportError } from "../net/errors.js";
import { ConnectionPool } from "../net/pool.js";
import { describeError, ErrorCode, type ErrorCodeValue, isRetriable } from "../proto/error-code.js";
import {
  type ConfigEntry,
  CoordinatorType,
  ExchangeType,
  Isolation,
  type HeaderMatch,
  type LeaderHint,
  type NodeEntry,
  type RoleBinding,
  type SourcedBinding,
  SubjectKind,
  type TenantEntry,
  type UserEntry,
  type CertificateEntry,
  type CertificateRequestEntry,
  type PublishEntry,
  type PublishResult,
  type ResourceMetadata,
  ResourceKind,
} from "../proto/messages.js";
import type { Request } from "../proto/request.js";
import type { DescribeMetricsResponse, Response, WhoAmIResponse } from "../proto/response.js";
import { Batch } from "../record/batch.js";
import { RecordFormatError } from "../record/errors.js";
import type { BatchRecord, Header } from "../record/types.js";
import { compareU64, U32_MAX, u64ToString } from "../record/unsigned.js";
import { Attempt, CallFailure } from "./attempt.js";
import { type ClientConfig, type ClientOptions, resolveClientOptions } from "./config.js";
import {
  BrokerResponseError,
  ClientAuthenticationError,
  ClientClosedError,
  ClientError,
  GaveUpError,
  NoAnswerError,
  NotAllWrittenError,
  NotTransactionalError,
  OffsetOutOfRangeError,
  ResourceExistsError,
  UnknownOutcomeError,
  UnknownStreamError,
  UnroutableError,
} from "./errors.js";
import { Exchange } from "./exchange.js";
import { Fetched } from "./fetched.js";
import { type GroupConfig } from "./group-config.js";
import { type GroupHost, GroupConsumer } from "./group-consumer.js";
import { buildBatch, type OutHeader, type OutMessage, type ProduceResult } from "./messages.js";
import { Mutex } from "./mutex.js";
import { OffsetAt } from "./offset-at.js";
import { type QueueHost, QueueConsumer } from "./queue-consumer.js";
import type { Published, PublishOptions, RoutedQueue } from "./published.js";
import { type ProducerIds, Transaction, type TransactionHost, type TransactionOptions } from "./transaction.js";
import { queueConfigs, type QueueOptions, resolveCredit, type SubscribeOptions } from "./queue-options.js";

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

interface Stamp {
  readonly producerId: bigint;
  readonly epoch: number;
  readonly sequence: number;
}

/** One node's answer to `DescribeMetrics`, from `LotseClient.nodeMetrics`: its metrics, or why there are none. */
export interface NodeMetrics {
  readonly nodeId: number;
  /** What it answered, or `null`. */
  readonly metrics: DescribeMetricsResponse | null;
  /** Why it did not answer, or `null`: `NoAnswerError` for a node that could not be reached. */
  readonly error: ClientError | null;
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
  /** The names of the resources this client has looked up, by id, and their newest ids by name. */
  private readonly names = new Map<bigint, string>();
  private readonly idsByName = new Map<string, bigint>();
  /** Exchanges by name, and when metadata said so (ADR 0032). */
  private readonly exchanges = new Map<string, { exchange: Exchange; at: number }>();
  private readonly exchangeIds = new Map<string, bigint>();

  /** One transaction at a time; the producer id and epoch for the next one, `null` to ask anew. */
  private readonly txnTurn = new Mutex();
  private txnProducer: ProducerIds | null = null;

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
      this.names.set(r.resourceId, r.name);
      // A name deleted and made again has a higher id, and a broker that lags may still report the old one.
      const known = this.idsByName.get(r.name);
      if (known === undefined || compareU64(known, r.resourceId) <= 0) {
        this.idsByName.set(r.name, r.resourceId);
      }
      if (r.exchange !== null) {
        this.exchanges.set(r.name, { exchange: Exchange.of(r.name, r.resourceId, r.exchange), at: nowMs() });
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
    txnId: string | null = null,
  ): Promise<ProduceResult> {
    // An idempotent batch may be sent again whatever happened to it.
    const routed = await this.route(stream, partition, signal);
    const req: Request = {
      type: "Produce",
      txnId,
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

  /**
   * Creates a queue in the configured tenant and returns its id. A create whose outcome is unknown is retried and may then
   * fail with `ResourceExistsError`, as for `createStream`. A queue is a resource with one partition, 0: produce to it
   * with `produce(name, 0, ...)`.
   *
   * @throws RangeError for a setting out of range
   */
  async createQueue(name: string, options: QueueOptions & CallOptions = {}): Promise<bigint> {
    this.checkOpen();
    const { replication, configs } = queueConfigs(options);
    return this.create(
      name,
      {
        type: "CreateQueue",
        name,
        tenant: this.cfg.tenant,
        priorityLevels: 1,
        singleActive: false,
        dlq: options.deadLetterQueue ?? null,
        replication,
        configs,
      },
      options.signal,
    );
  }

  // ---- queues ----

  /**
   * Subscribes to a queue. The consumer is connected to the queue's leader when this resolves; messages are produced to a
   * queue with `produce` to partition 0.
   *
   * @throws UnknownStreamError if the queue does not exist
   * @throws GaveUpError if no leader can be reached within `retryForMs`
   */
  async subscribe(queue: string, options: SubscribeOptions & CallOptions = {}): Promise<QueueConsumer> {
    this.checkOpen();
    const consumer = new QueueConsumer(this.queueHost(), queue, resolveCredit(options));
    await consumer.start(options.signal);
    return consumer;
  }

  private queueHost(): QueueHost {
    return {
      tenant: this.cfg.tenant,
      retrying: (attempt, signal) => this.retrying(attempt, signal),
      route: (queue, partition, signal) => this.route(queue, partition, signal),
      connect: (address) => this.connect(address),
      call: (address, request) => this.call(address, request),
      callOn: (connection, request) => this.callOn(connection, request),
      forget: (queue, partition) => this.forget(queue, partition),
      redirect: (queue, partition, hint) => this.redirect(queue, partition, hint),
    };
  }

  // ---- transactions (ADR 0029) ----

  /**
   * Starts a transaction for the configured transactional id (`ClientOptions.transactionalId`), getting its producer id
   * from the coordinator first, which fences any older client with the same id. One transaction at a time per client: a
   * second call waits until the first ends. End it with `commit` or `abort`; a transaction left open is aborted by
   * `await using`, or, with `abandon`, by the coordinator, and the next transaction then gets a fresh producer id.
   *
   * @throws NotTransactionalError no transactional id is configured
   * @throws GaveUpError no coordinator answered in time, or `signal` fired while waiting for the open transaction
   */
  async beginTransaction(options: TransactionOptions = {}): Promise<Transaction> {
    this.checkOpen();
    const txnId = this.cfg.transactionalId;
    if (txnId === null) {
      throw new NotTransactionalError();
    }
    const signal = options.signal;
    const guard = abortGuard(signal);
    let release: () => void;
    try {
      try {
        release = await this.txnTurn.acquire(guard?.promise);
      } catch (e) {
        if (e === ABORTED) {
          throw new GaveUpError(0, "aborted while waiting for the open transaction");
        }
        throw e;
      }
    } finally {
      guard?.dispose();
    }
    try {
      let ids = this.txnProducer;
      if (ids === null) {
        ids = await this.retrying(() => this.initProducerOnce(txnId), signal);
        this.txnProducer = ids;
      }
      if (signal?.aborted === true) {
        throw new GaveUpError(0, "aborted");
      }
      return new Transaction(this.transactionHost(), txnId, ids.producerId, ids.epoch, release, signal);
    } catch (e) {
      release();
      throw e;
    }
  }

  private transactionHost(): TransactionHost {
    return {
      tenant: this.cfg.tenant,
      retrying: (attempt, signal, mayHaveChanged) => this.retrying(attempt, signal, mayHaveChanged),
      produceOnce: (stream, partition, batch, idempotent, signal, txnId) =>
        this.produceOnce(stream, partition, batch, idempotent, signal, txnId),
      route: (name, partition, signal) => this.route(name, partition, signal),
      callCoordinator: (key, request) => this.callCoordinator(key, request),
      forgetCoordinator: () => {
        this.coordinatorAddress = null;
      },
      transactionEnded: (next) => {
        this.txnProducer = next;
      },
      routeMessage: (exchange, routingKey, headers, mandatory) => this.routeMessage(exchange, routingKey, headers, mandatory),
      streamId: (name) => this.idsByName.get(name),
    };
  }

  // ---- exchanges routed by the client, in a transaction (ADR 0032) ----

  /** The exchange as metadata last said within `metadataMaxAgeMs`, asking for it otherwise. */
  private async exchange(name: string): Promise<Exchange> {
    const known = this.exchanges.get(name);
    if (known !== undefined) {
      if (nowMs() - known.at < this.cfg.metadataMaxAgeMs) {
        return known.exchange;
      }
      this.exchanges.delete(name);
    }
    // learn() keeps the exchange and the queues bound to it.
    const m = await this.metadataWith(undefined, [name]);
    if (find(m.resources, name) === undefined) {
      // Declared a moment ago: not every broker knows it yet.
      await this.metadataFromAll([name]);
    }
    const x = this.exchanges.get(name);
    if (x === undefined) {
      throw new UnknownStreamError(name, 0);
    }
    return x.exchange;
  }

  /** The queues a message goes to, by name, sorted. With `mandatory`, a message no queue takes is refused. */
  private async routeMessage(
    exchange: string,
    routingKey: string,
    headers: readonly Header[],
    mandatory: boolean,
  ): Promise<string[]> {
    const x = await this.exchange(exchange);
    const ids = x.route(routingKey, headers);
    if (ids.length === 0 && mandatory) {
      throw new UnroutableError(exchange, routingKey);
    }
    const queues: string[] = [];
    for (const id of ids) {
      const n = this.names.get(id);
      if (n === undefined) {
        throw new UnknownStreamError(`queue ${u64ToString(id)} bound to ${exchange}`, 0);
      }
      queues.push(n);
    }
    return queues.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  }

  // ---- consumer groups (ADR 0033) ----

  /**
   * Joins `group` to read `streams`; partitions arrive with the first calls to `GroupConsumer.poll`.
   *
   * @throws GaveUpError if no broker answers within `retryForMs`
   */
  async joinGroup(
    group: string,
    streams: readonly string[],
    config?: GroupConfig,
    options: CallOptions = {},
  ): Promise<GroupConsumer> {
    this.checkOpen();
    // Learn the streams' ids (and names) first.
    await this.metadataWith(options.signal, streams);
    return new GroupConsumer(this.groupHost(), group, [...streams], null, config);
  }

  /**
   * Joins `group` to read every stream of the tenant whose whole name matches the regular expression `pattern` and that
   * this principal may read (ADR 0073). The coordinator matches it with every heartbeat, so streams created later join in.
   */
  async joinGroupMatching(
    group: string,
    pattern: string,
    config?: GroupConfig,
    options: CallOptions = {},
  ): Promise<GroupConsumer> {
    this.checkOpen();
    await this.metadataWith(options.signal, []);
    return new GroupConsumer(this.groupHost(), group, [], pattern, config);
  }

  private groupHost(): GroupHost {
    return {
      tenant: this.cfg.tenant,
      backoffMs: this.cfg.backoffMs,
      retrying: (attempt, signal, mayHaveChanged) => this.retrying(attempt, signal, mayHaveChanged),
      callCoordinator: (key, request) => this.callCoordinator(key, request),
      forgetCoordinator: () => {
        this.coordinatorAddress = null;
      },
      streamId: (name) => this.idsByName.get(name),
      streamName: (id) => this.names.get(id),
      metadata: () => this.metadataWith(undefined, []),
      listOffset: (stream, partition, at) => this.listOffset(stream, partition, at),
      fetch: (stream, partition, offset, maxWaitMs, options) => this.fetch(stream, partition, offset, maxWaitMs, options),
    };
  }

  // ---- publishing through exchanges (ADR 0079) ----

  /**
   * Publishes a message to an exchange, which routes it to queues by `routingKey` (and headers). A node routes it by the
   * bindings it knows and writes it to each queue, if this client's roles allow every one of them. Resolves once every
   * routed queue holds it; a message no queue takes is dropped, or refused with `UnroutableError` when `mandatory`. If
   * some queues could not be written, `NotAllWrittenError` says which hold it. If the node's answer is lost, the call
   * rejects with `UnknownOutcomeError`: the publish is not sent again, since the node may have written it. A publish
   * given up by its signal after it was sent is unknown the same way.
   */
  async publish(
    exchange: string,
    routingKey: string,
    message: OutMessage,
    options: PublishOptions = {},
  ): Promise<Published> {
    this.checkOpen();
    const batch = buildBatch([message]);
    const mandatory = options.mandatory === true;
    // A name deleted and made again has a new id: ask once more.
    for (let attempt = 0; attempt < 2; attempt++) {
      if (attempt > 0) {
        this.exchangeIds.delete(exchange);
      }
      const id = await this.exchangeId(exchange, options.signal);
      const r = await this.publishOnce({ exchangeId: id, routingKey, mandatory, batch }, options.signal);
      const code = r.error;
      if (code === ErrorCode.NONE) {
        return this.published(r);
      }
      if (code === ErrorCode.UNKNOWN_RESOURCE && attempt === 0) {
        continue;
      }
      if (code === ErrorCode.UNKNOWN_RESOURCE) {
        throw new UnknownStreamError(exchange, 0);
      }
      if (code === ErrorCode.UNROUTABLE) {
        throw new UnroutableError(exchange, routingKey);
      }
      throw new BrokerResponseError(code, null);
    }
    throw new Error("the second attempt returns");
  }

  /** The exchange's id, from metadata the first time. */
  private async exchangeId(exchange: string, signal?: AbortSignal): Promise<bigint> {
    const known = this.exchangeIds.get(exchange);
    if (known !== undefined) {
      return known;
    }
    const m = await this.metadataWith(signal, [exchange]);
    const r = find(m.resources, exchange, false);
    if (r === undefined || r.error !== ErrorCode.NONE || r.kind !== ResourceKind.EXCHANGE) {
      throw new UnknownStreamError(exchange, 0);
    }
    this.exchangeIds.set(exchange, r.resourceId);
    return r.resourceId;
  }

  /** One `Publish` to the first node that takes it. It is never sent twice once a node may have it. */
  private async publishOnce(entry: PublishEntry, signal?: AbortSignal): Promise<PublishResult> {
    const req: Request = {
      type: "Publish",
      txnId: null,
      timeoutMs: Math.min(this.cfg.requestTimeoutMs, U32_MAX),
      entries: [entry],
    };
    const start = nowMs();
    let why = "no broker known";
    const guard = abortGuard(signal);
    try {
      for (;;) {
        for (const addr of this.candidates()) {
          if (signal?.aborted === true) {
            throw new GaveUpError(nowMs() - start, "aborted");
          }
          try {
            const sending = this.call(addr, req);
            let r: Response;
            if (guard === null) {
              r = await sending;
            } else {
              sending.catch(() => {}); // abandoned, it may still fail
              r = await Promise.race([sending, guard.promise]);
            }
            if (r.type === "Publish" && r.entries.length === 1) {
              return r.entries[0] as PublishResult;
            }
            why = `unexpected response ${r.type}`;
          } catch (e) {
            if (e === ABORTED) {
              throw new UnknownOutcomeError("the publish was given up after it was sent");
            }
            if (!(e instanceof CallFailure)) {
              throw e;
            }
            switch (e.kind) {
              case "DENIED":
                throw new ClientAuthenticationError(e.message);
              case "REFUSED":
                throw new BrokerResponseError(e.code as ErrorCodeValue, e.message);
              case "LOST":
                // The node may have written it: not sent again.
                throw new UnknownOutcomeError(`${formatAddress(addr)}: ${e.message}`);
              case "NOT_SENT":
                why = `${formatAddress(addr)}: ${e.describe()}`;
                break;
            }
          }
        }
        if (nowMs() - start >= this.cfg.retryForMs) {
          throw new GaveUpError(this.cfg.retryForMs, why);
        }
        if (!(await this.pause(signal))) {
          throw new GaveUpError(nowMs() - start, "aborted");
        }
      }
    } finally {
      guard?.dispose();
    }
  }

  /** A routed message's outcome by queue name. */
  private published(result: PublishResult): Published {
    // The node names each queue: this client may never have seen one bound and deleted between two of its lookups. Only a
    // queue the node no longer knows comes without a name.
    const written: RoutedQueue[] = [];
    const failures: ClientError[] = [];
    for (const q of result.routed) {
      const name = q.name !== "" ? q.name : (this.names.get(q.queueId) ?? `queue ${u64ToString(q.queueId)}`);
      if (q.error === ErrorCode.NONE) {
        written.push({ queue: name, offset: q.baseOffset });
      } else if (q.error === ErrorCode.REQUEST_TIMED_OUT) {
        failures.push(new UnknownOutcomeError(`${name}: the write's outcome is unknown`));
      } else {
        failures.push(new BrokerResponseError(q.error, name));
      }
    }
    written.sort((a, b) => (a.queue < b.queue ? -1 : a.queue > b.queue ? 1 : 0));
    if (failures.length > 0) {
      throw new NotAllWrittenError(
        written.map((w) => w.queue),
        failures,
      );
    }
    return { routed: written };
  }

  // ---- exchanges and bindings (ADR 0034) ----

  /**
   * Declares an exchange in the configured tenant and returns its id. A declare whose outcome is unknown is retried and may
   * then fail with `ResourceExistsError`, as for `createStream`.
   *
   * @param type one of `ExchangeType`: `DIRECT`, `TOPIC`, `FANOUT` or `HEADERS`
   * @throws RangeError for an unknown type
   */
  async declareExchange(name: string, type: number, options: CallOptions = {}): Promise<bigint> {
    this.checkOpen();
    if (!Object.values(ExchangeType).includes(type as 0)) {
      throw new RangeError(`unknown exchange type ${type}`);
    }
    return this.create(
      name,
      { type: "DeclareExchange", name, tenant: this.cfg.tenant, exchangeType: type, configs: [] },
      options.signal,
    );
  }

  /** Binds `queue` to `exchange` with a binding key (direct and topic exchanges). */
  bind(exchange: string, queue: string, key: string, options: CallOptions = {}): Promise<void> {
    return this.binding(exchange, queue, key, [], true, false, options.signal);
  }

  /**
   * Binds `queue` to a headers exchange: all of `headers` must match (`matchAll`), or any one. A header with a `null` value
   * matches only a header whose value is null.
   */
  bindHeaders(
    exchange: string,
    queue: string,
    headers: readonly OutHeader[],
    matchAll: boolean,
    options: CallOptions = {},
  ): Promise<void> {
    const matches: HeaderMatch[] = headers.map((h) => ({
      key: typeof h.name === "string" ? Buffer.from(h.name, "utf8") : h.name,
      value: h.value === null ? null : typeof h.value === "string" ? Buffer.from(h.value, "utf8") : h.value,
    }));
    return this.binding(exchange, queue, "", matches, matchAll, false, options.signal);
  }

  /** Removes a binding made by `bind`. */
  unbind(exchange: string, queue: string, key: string, options: CallOptions = {}): Promise<void> {
    return this.binding(exchange, queue, key, [], true, true, options.signal);
  }

  private async binding(
    exchange: string,
    queue: string,
    key: string,
    headerMatch: HeaderMatch[],
    matchAll: boolean,
    unbind: boolean,
    signal?: AbortSignal,
  ): Promise<void> {
    this.checkOpen();
    const m = await this.metadataFromAll([exchange, queue]);
    const x = find(m, exchange);
    const q = find(m, queue);
    if (x === undefined || q === undefined) {
      throw new UnknownStreamError(`${exchange} or ${queue}`, 0);
    }
    const body = { exchangeId: x.resourceId, queueId: q.resourceId, bindingKey: key, headerMatch, matchAll };
    const req: Request = unbind ? { type: "Unbind", ...body } : { type: "Bind", ...body };
    await this.retrying(
      async () => {
        const r = await this.callAdmin(req);
        if (r.type !== "Outcome") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint);
      },
      signal,
      true,
    );
    // A transaction's publish routes by the bindings this client has seen: see the new ones.
    this.exchanges.delete(exchange);
  }

  /** An admin answer: retriable errors follow the leader hint and may have been carried out; others fail. */
  private adminAnswer(
    error: ErrorCodeValue,
    hint: LeaderHint | null,
    message: string | null = null,
    changes = true,
  ): void {
    if (error === ErrorCode.NONE) {
      return;
    }
    if (isRetriable(error)) {
      this.metadataLeader = hint === null ? null : hint.leader;
      // A read that was refused as retriable changed nothing: running out of time is "gave up", not "unknown".
      throw changes ? Attempt.retryMaybeDone(describeError(error)) : Attempt.retry(describeError(error));
    }
    throw Attempt.fail(new BrokerResponseError(error, message));
  }

  // ---- deleting, settings, users, tenants, roles, nodes (ADR 0035, 0039, 0048, 0063 to 0065, 0070, 0074) ----

  /**
   * Deletes a stream, queue or exchange created at runtime, with its data and bindings, and returns its id. Resources
   * from the node configuration cannot be deleted. Done once the resource is gone, even if someone else deleted it first,
   * so a delete whose answer was lost succeeds when it is retried.
   *
   * @throws UnknownStreamError if no broker knows the name
   */
  delete(name: string, options: CallOptions = {}): Promise<bigint> {
    return this.deleteWith(name, false, options.signal);
  }

  /**
   * `delete`, but only if the stream or queue holds nothing (ADR 0063): no unsettled message in a queue, no record in a
   * stream. Otherwise it fails with `BrokerResponseError` `RESOURCE_NOT_EMPTY` and nothing is deleted. An exchange holds no
   * messages and is deleted. The check guards against mistakes, not against a producer: a message produced while the
   * delete is under way goes with it.
   */
  deleteIfEmpty(name: string, options: CallOptions = {}): Promise<bigint> {
    return this.deleteWith(name, true, options.signal);
  }

  private async deleteWith(name: string, ifEmpty: boolean, signal?: AbortSignal): Promise<bigint> {
    this.checkOpen();
    const id = await this.resourceId(name);
    const req: Request = { type: "DeleteResource", resourceId: id, ifEmpty };
    await this.retrying(
      async () => {
        const r = await this.callAdmin(req);
        if (r.type !== "Admin") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        // Gone already, deleted by this request's earlier attempt or by someone else: done.
        if (r.error === ErrorCode.UNKNOWN_RESOURCE) {
          return;
        }
        this.adminAnswer(r.error, r.leaderHint, r.errorMsg);
      },
      signal,
      true,
    );
    this.exchanges.delete(name);
    this.exchangeIds.delete(name);
    // A name made again gets a new id: a route to the old one would lead nowhere.
    for (const key of [...this.routes.keys()]) {
      if (key.startsWith(`${name}\0`)) {
        this.routes.delete(key);
      }
    }
    return id;
  }

  /**
   * Settles every message of `queue` that is ready now: not held by a consumer, not in an open transaction (ADR 0064).
   * Messages produced while it runs are left alone. Returns how many were purged. Needs the tenant-admin role.
   */
  purge(queue: string, options: CallOptions = {}): Promise<bigint> {
    return this.queueAction(queue, false, options.signal);
  }

  /**
   * Moves every ready message of the dead-letter queue `queue` back to the queue its dead-letter headers name, once each
   * across failovers (ADR 0064). Messages without such headers, or whose queue is gone, stay. Returns how many were moved.
   * Needs the tenant-admin role.
   */
  requeue(queue: string, options: CallOptions = {}): Promise<bigint> {
    return this.queueAction(queue, true, options.signal);
  }

  /** Rounds: `up_to` 0 first, then the bound each answer gave, until the leader says there is no more. */
  private async queueAction(queue: string, requeue: boolean, signal?: AbortSignal): Promise<bigint> {
    this.checkOpen();
    let total = 0n;
    let upTo = 0n;
    for (;;) {
      const bound = upTo;
      const step = await this.retrying(() => this.queueActionOnce(queue, requeue, bound, signal), signal, true);
      total += step.count;
      upTo = step.upTo;
      if (!step.more) {
        return total;
      }
    }
  }

  private async queueActionOnce(
    queue: string,
    requeue: boolean,
    upTo: bigint,
    signal?: AbortSignal,
  ): Promise<{ count: bigint; upTo: bigint; more: boolean }> {
    const routed = await this.route(queue, 0, signal);
    let r: Response;
    try {
      r = await this.call(routed.address, { type: "QueueAction", requeue, queueId: routed.route.resourceId, upTo });
    } catch (e) {
      if (!(e instanceof CallFailure)) {
        throw e;
      }
      this.forget(queue, 0);
      throw e.toAttempt();
    }
    if (r.type !== "QueueAction") {
      throw Attempt.retry(`unexpected response ${r.type}`);
    }
    if (r.error === ErrorCode.NONE) {
      return { count: r.count, upTo: r.upTo, more: r.more };
    }
    if (isRetriable(r.error)) {
      this.redirect(queue, 0, r.leaderHint);
      // Acting again is harmless: what was done is settled.
      throw Attempt.retryMaybeDone(describeError(r.error));
    }
    throw Attempt.fail(new BrokerResponseError(r.error, r.errorMsg));
  }

  /**
   * Changes settings of a resource created at runtime: `set` gives keys new values, `reset` returns keys to their default.
   *
   * @throws UnknownStreamError if no broker knows the name
   */
  async alterConfigs(
    name: string,
    set: Readonly<Record<string, string>> = {},
    reset: readonly string[] = [],
    options: CallOptions = {},
  ): Promise<void> {
    this.checkOpen();
    const id = await this.resourceId(name);
    const configs = Object.keys(set)
      .sort()
      .map((key) => ({ key, value: set[key] as string }));
    return this.adminChange({ type: "AlterConfigs", resourceId: id, set: configs, delete: reset }, options.signal);
  }

  /**
   * A resource's settings, each with where its value comes from (`ConfigSource`). Read from the metadata leader, so
   * changes this client made are in it.
   *
   * @throws UnknownStreamError if no broker knows the name
   */
  async describeConfigs(name: string, options: CallOptions = {}): Promise<readonly ConfigEntry[]> {
    this.checkOpen();
    const id = await this.resourceId(name);
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeConfigs", resourceIds: [id] });
        if (r.type !== "DescribeConfigs") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        const first = r.resources[0];
        if (first === undefined) {
          throw Attempt.retry("no resource in the answer");
        }
        if (first.error !== ErrorCode.NONE) {
          throw Attempt.fail(new BrokerResponseError(first.error, null));
        }
        return first.configs;
      },
      options.signal,
    );
  }

  /**
   * Sets `user`'s SCRAM-SHA-512 password, creating the user if needed. Only a salted credential leaves the client (a
   * fresh 16-byte salt each time); the password does not. Needs the cluster-admin role.
   */
  async setUserPassword(user: string, password: string, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    return this.adminChange(ScramCredential.forPassword(password).alterUser(user), options.signal);
  }

  /**
   * Removes `user`'s SCRAM credential. Sessions it opened end at the latest when the node's maximum session time is up;
   * `revokeSessions` ends them now.
   */
  async deleteUser(user: string, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    return this.adminChange(ScramCredential.removal(user), options.signal);
  }

  /** The users with a SCRAM credential, configured and set at runtime. */
  async describeUsers(options: CallOptions = {}): Promise<readonly UserEntry[]> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeUsers" });
        if (r.type !== "DescribeUsers") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return r.users;
      },
      options.signal,
    );
  }

  /**
   * The client certificates the cluster has seen or had revoked (ADR 0091), with their state at `nowMs`, the answering
   * node's clock. Last-seen times are at most an hour late. Needs the cluster-admin role.
   */
  async describeCertificates(
    options: CallOptions = {},
  ): Promise<{ readonly certificates: readonly CertificateEntry[]; readonly nowMs: bigint }> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeCertificates" });
        if (r.type !== "DescribeCertificates") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return { certificates: r.certificates, nowMs: r.nowMs };
      },
      options.signal,
    );
  }

  /** Creates a tenant. Needs the cluster-admin role. */
  async createTenant(name: string, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    return this.adminChange({ type: "CreateTenant", name }, options.signal);
  }

  /** Deletes a tenant that holds no resources, and its role bindings. */
  async deleteTenant(name: string, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    return this.adminChange({ type: "DeleteTenant", name }, options.signal);
  }

  /** The tenants this client has a role in; all of them for a cluster admin. */
  async describeTenants(options: CallOptions = {}): Promise<readonly TenantEntry[]> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeTenants" });
        if (r.type !== "DescribeTenants") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return r.tenants;
      },
      options.signal,
    );
  }

  /** Adds and removes role bindings; removals first. */
  async alterRoleBindings(
    add: readonly RoleBinding[],
    remove: readonly RoleBinding[] = [],
    options: CallOptions = {},
  ): Promise<void> {
    this.checkOpen();
    return this.adminChange({ type: "AlterRoleBindings", add, remove }, options.signal);
  }

  /** Every role binding, each with where it comes from (`ConfigSource`). */
  async describeRoleBindings(options: CallOptions = {}): Promise<readonly SourcedBinding[]> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeRoleBindings" });
        if (r.type !== "DescribeRoleBindings") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return r.bindings;
      },
      options.signal,
    );
  }

  /** Who this client acts as, and the role bindings that apply. */
  async whoAmI(options: CallOptions = {}): Promise<WhoAmIResponse> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "WhoAmI" });
        if (r.type !== "WhoAmI") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        return r;
      },
      options.signal,
    );
  }

  /**
   * Revokes the sessions of a user, or of every member of an IdP group (`group`) (ADR 0070): each node ends their
   * sessions that authenticated before now, and refuses their tokens issued before now. A SCRAM user who still has the
   * password can sign in again; change or delete the password to keep them out. Needs the cluster-admin role.
   */
  async revokeSessions(subject: string, group = false, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    const subjectKind = group ? SubjectKind.GROUP : SubjectKind.USER;
    return this.adminChange({ type: "RevokeSessions", subjectKind, subject, beforeMs: 0n }, options.signal);
  }

  /**
   * Revokes one client certificate (ADR 0090): each node ends the connections that showed it and refuses it from
   * then on, until it expires at `notAfterMs` (milliseconds since the Unix epoch). `issuer` is the SHA-256 of the
   * issuer's distinguished name in DER form (32 bytes) and `serial` the serial number, big-endian, as the certificate
   * has them. The CRL file stays as it is. Needs the cluster-admin role.
   */
  async revokeCertificate(
    issuer: Uint8Array,
    serial: Uint8Array,
    notAfterMs: bigint | number,
    reason: string,
    options: CallOptions = {},
  ): Promise<void> {
    this.checkOpen();
    if (issuer.length !== 32) {
      throw new RangeError(`the issuer hash is 32 bytes, not ${issuer.length}`);
    }
    return this.adminChange(
      { type: "RevokeCertificate", issuer, serial, notAfterMs: BigInt(notAfterMs), reason },
      options.signal,
    );
  }

  /**
   * Asks for a certificate with a PKCS#10 signing request in DER form, made by the requester: its private key never
   * reaches the cluster (ADR 0092). The request's id is the SHA-256 of `csr`, so asking twice is one request. Needs the
   * cluster-admin role.
   */
  async requestCertificate(principal: string, csr: Uint8Array, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    return this.adminChange({ type: "RequestCertificate", principal, csr }, options.signal);
  }

  /** The certificate requests, newest first, with `nowMs`, the answering node's clock (ADR 0092). Cluster admin. */
  async describeCertificateRequests(
    options: CallOptions = {},
  ): Promise<{ readonly requests: readonly CertificateRequestEntry[]; readonly nowMs: bigint }> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeCertificateRequests" });
        if (r.type !== "DescribeCertificateRequests") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return { requests: r.requests, nowMs: r.nowMs };
      },
      options.signal,
    );
  }

  /**
   * The CA holder records the certificate it signed for the request `id` (the SHA-256 of its signing request). The
   * broker does not read the certificate: `issuer` (SHA-256 of the issuer's name in DER form, 32 bytes), `serial`
   * (big-endian) and the validity times, in ms since the Unix epoch, are the CA holder's word (ADR 0092). Cluster
   * admin.
   */
  async recordCertificate(
    id: Uint8Array,
    certificate: Uint8Array,
    issuer: Uint8Array,
    serial: Uint8Array,
    notBeforeMs: bigint | number,
    notAfterMs: bigint | number,
    options: CallOptions = {},
  ): Promise<void> {
    this.checkOpen();
    return this.adminChange(
      {
        type: "RecordCertificate",
        id,
        certificate,
        issuer,
        serial,
        notBeforeMs: BigInt(notBeforeMs),
        notAfterMs: BigInt(notAfterMs),
      },
      options.signal,
    );
  }

  /**
   * Adds node `nodeId` to the cluster, reached by nodes at `peer` and by clients at `client` (each `host:port`); start it
   * with `join` afterwards. Ids are never used twice. Needs the cluster-admin role.
   */
  async addNode(nodeId: number | bigint, peer: string, client: string, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    checkNodeId(nodeId);
    return this.adminChange({ type: "AddNode", nodeId: BigInt(nodeId), peer, client }, options.signal);
  }

  /** Starts draining a node: its replicas move to other nodes, then it leaves. `describeNodes` shows how far. */
  async removeNode(nodeId: number | bigint, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    checkNodeId(nodeId);
    return this.adminChange({ type: "RemoveNode", nodeId: BigInt(nodeId) }, options.signal);
  }

  /** The cluster's nodes, as the metadata group knows them. */
  async describeNodes(options: CallOptions = {}): Promise<readonly NodeEntry[]> {
    this.checkOpen();
    return this.retrying(
      async () => {
        const r = await this.callAdmin({ type: "DescribeNodes" });
        if (r.type !== "DescribeNodes") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, null, false);
        return r.nodes;
      },
      options.signal,
    );
  }

  /**
   * Pins the leader of `partition` of the stream or queue `name` to `node`, one of its voters, or unpins it with `null` so
   * that leaders spread by themselves again (ADR 0065). The group's leader hands over shortly after; the pin stays until
   * it is removed or the node leaves. Needs the cluster-admin role.
   *
   * @throws RangeError for a partition that is not a u32, or a node id that is not a positive u64
   */
  async moveLeader(
    name: string,
    partition: number,
    node: number | bigint | null,
    options: CallOptions = {},
  ): Promise<void> {
    this.checkOpen();
    if (!Number.isInteger(partition) || partition < 0 || partition > U32_MAX) {
      throw new RangeError(`partition must be a u32, not ${partition}`);
    }
    if (node !== null) {
      checkNodeId(node);
    }
    const id = await this.resourceId(name);
    return this.adminChange(
      { type: "MoveLeader", resourceId: id, partition, nodeId: node === null ? 0n : BigInt(node) },
      options.signal,
    );
  }

  /**
   * Raises the partition count of the stream `name`, created at runtime, to `count` (ADR 0074). Existing partitions stay as
   * they are, so a key may map to another partition afterwards; consumer groups spread over the new ones on their next
   * heartbeat. Needs the tenant-admin role.
   *
   * @throws RangeError for a count that is not a positive u32
   */
  async createPartitions(name: string, count: number, options: CallOptions = {}): Promise<void> {
    this.checkOpen();
    if (!Number.isInteger(count) || count < 1 || count > U32_MAX) {
      throw new RangeError(`count must be a positive u32, not ${count}`);
    }
    const id = await this.resourceId(name);
    return this.adminChange({ type: "CreatePartitions", resourceId: id, count }, options.signal);
  }

  /**
   * Every node's metrics, asked of each node directly, for every node the cluster's metadata names: each with its answer,
   * or why there is none. Counters grow from each node's start; see `DescribeMetrics`. A node that does not answer is
   * reported, not thrown.
   */
  async nodeMetrics(options: CallOptions = {}): Promise<NodeMetrics[]> {
    this.checkOpen();
    const m = await this.metadataWith(options.signal, []);
    const out: NodeMetrics[] = [];
    for (const b of m.brokers) {
      if (options.signal?.aborted === true) {
        throw new GaveUpError(0, "aborted");
      }
      const addr: Address = { host: b.host, port: b.port };
      try {
        const r = await this.call(addr, { type: "DescribeMetrics" });
        if (r.type !== "DescribeMetrics") {
          out.push({ nodeId: b.nodeId, metrics: null, error: new NoAnswerError(b.nodeId, `unexpected response ${r.type}`) });
        } else if (r.error === ErrorCode.NONE) {
          out.push({ nodeId: b.nodeId, metrics: r, error: null });
        } else {
          out.push({ nodeId: b.nodeId, metrics: null, error: new BrokerResponseError(r.error, null) });
        }
      } catch (e) {
        if (!(e instanceof CallFailure)) {
          throw e;
        }
        out.push({ nodeId: b.nodeId, metrics: null, error: new NoAnswerError(b.nodeId, `${formatAddress(addr)}: ${e.describe()}`) });
      }
    }
    return out;
  }

  /** The id of the resource called `name`, asking every broker before calling it unknown. */
  private async resourceId(name: string): Promise<bigint> {
    const r = find(await this.metadataFromAll([name]), name);
    if (r === undefined) {
      throw new UnknownStreamError(name, 0);
    }
    return r.resourceId;
  }

  /** An admin change answered with `AdminResponse`. */
  private adminChange(req: Request, signal?: AbortSignal): Promise<void> {
    return this.retrying(
      async () => {
        const r = await this.callAdmin(req);
        if (r.type !== "Admin") {
          throw Attempt.retry(`unexpected response ${r.type}`);
        }
        this.adminAnswer(r.error, r.leaderHint, r.errorMsg);
      },
      signal,
      true,
    );
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

/** A node id is a positive `u64`; `0` means "none" on the wire. */
function checkNodeId(nodeId: number | bigint): void {
  if ((typeof nodeId === "number" && !Number.isInteger(nodeId)) || BigInt(nodeId) <= 0n || BigInt(nodeId) >= 1n << 64n) {
    throw new RangeError(`a node id is a positive u64, not ${String(nodeId)}`);
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

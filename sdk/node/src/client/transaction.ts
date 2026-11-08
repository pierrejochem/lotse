import type { Address } from "../net/address.js";
import { ErrorCode, isRetriable } from "../proto/error-code.js";
import type { Request } from "../proto/request.js";
import type { Response } from "../proto/response.js";
import { Batch } from "../record/batch.js";
import { RecordFormatError } from "../record/errors.js";
import type { Header } from "../record/types.js";
import { Attempt } from "./attempt.js";
import type { CallOptions } from "./client.js";
import {
  BrokerResponseError,
  GaveUpError,
  MustAbortError,
  TransactionEndedError,
  UnknownStreamError,
} from "./errors.js";
import type { GroupConsumer } from "./group-consumer.js";
import { commitOnce, offsetResources } from "./group-consumer.js";
import { buildBatch, type OutMessage, type ProduceResult } from "./messages.js";
import { Mutex } from "./mutex.js";
import type { Published, PublishOptions, RoutedQueue } from "./published.js";

/** A producer id and the epoch it is at. */
export interface ProducerIds {
  readonly producerId: bigint;
  readonly epoch: number;
}

/** What a transaction needs from the client that began it. */
export interface TransactionHost {
  readonly tenant: string;
  retrying<T>(attempt: () => Promise<T>, signal?: AbortSignal, mayHaveChanged?: boolean): Promise<T>;
  produceOnce(
    stream: string,
    partition: number,
    batch: Uint8Array,
    idempotent: boolean,
    signal: AbortSignal | undefined,
    txnId: string,
  ): Promise<ProduceResult>;
  route(name: string, partition: number, signal?: AbortSignal): Promise<{ route: { resourceId: bigint }; address: Address }>;
  callCoordinator(key: string, request: Request): Promise<Response>;
  forgetCoordinator(): void;
  /** `next` is the producer id for the next transaction, `null` to ask anew. Hands the turn on. */
  transactionEnded(next: ProducerIds | null): void;
  routeMessage(exchange: string, routingKey: string, headers: readonly Header[], mandatory: boolean): Promise<string[]>;
  streamId(name: string): bigint | undefined;
}

/** What `beginTransaction` takes. */
export interface TransactionOptions {
  /**
   * While waiting for the turn (one transaction at a time per client), it gives up the wait with `GaveUpError`. Once the
   * transaction has begun, it aborts the transaction when it fires: calls in progress are given up as their own signals
   * would, and the transaction is aborted after them. A transaction that was aborted this way is ended.
   */
  readonly signal?: AbortSignal;
}

/**
 * An open transaction, from `LotseClient.beginTransaction` (ADR 0029). Everything written in it becomes visible to
 * read_committed readers together when it commits, and never if it aborts. It follows the Rust client's `Transaction` rule
 * for rule:
 *
 * - a partition (or queue) is added to the transaction with `AddPartitionsToTxn` before the first batch is written to it;
 * - batches carry the transaction's producer id and epoch, a sequence per partition, and the transactional flag; a batch
 *   whose outcome is unknown is retried with the same bytes;
 * - after an error that may have left something written, or a write that never got its outcome, only `abort` is left:
 *   every other call rejects with `MustAbortError`. Errors that prove nothing was written (`GaveUpError`,
 *   `UnknownStreamError`) leave the transaction open;
 * - `commit` resolves once the decision is durable. A transaction that wrote nothing ends without asking the coordinator.
 *
 * Calls on one transaction take turns in the order they were made. Leave the scope with `await using` and a transaction
 * that was neither committed nor aborted is aborted; or call `abandon()`, which leaves it to the coordinator: it aborts
 * the transaction after the transaction timeout, or at once when this client begins the next one with a fresh producer
 * id.
 */
export class Transaction {
  private readonly added = new Set<string>();
  private readonly next = new Map<string, number>();
  private readonly turn = new Mutex();
  /** Why only an abort is left, or `null`. */
  private poisoned: string | null = null;
  private ended = false;
  private releaseTurn: (() => void) | null;
  private readonly signal: AbortSignal | undefined;
  private readonly onSignal: (() => void) | null;

  /** @internal Made by `LotseClient.beginTransaction`. */
  constructor(
    private readonly host: TransactionHost,
    /** The transactional id. */
    readonly txnId: string,
    /** The producer id the coordinator gave. */
    readonly producerId: bigint,
    /** The producer epoch. */
    readonly epoch: number,
    releaseTurn: () => void,
    signal: AbortSignal | undefined,
  ) {
    this.releaseTurn = releaseTurn;
    this.signal = signal;
    if (signal === undefined) {
      this.onSignal = null;
    } else {
      this.onSignal = () => {
        if (!this.ended) {
          void this.abort().catch(() => {});
        }
      };
      signal.addEventListener("abort", this.onSignal, { once: true });
    }
  }

  /**
   * Whether the transaction may still be committed or aborted: neither has been called, and `abandon` has not left it to
   * the coordinator. A commit refused with `MustAbortError` leaves it open for an abort.
   */
  get isOpen(): boolean {
    return !this.ended;
  }

  /** Why only an abort is left, or `null`. */
  get mustAbortBecause(): string | null {
    return this.poisoned;
  }

  private checkUsable(): void {
    if (this.ended) {
      throw new TransactionEndedError();
    }
    if (this.poisoned !== null) {
      throw new MustAbortError(this.poisoned);
    }
  }

  /** The signal of one call: its own and the transaction's, whichever fires first. */
  private signalFor(own: AbortSignal | undefined): AbortSignal | undefined {
    if (own === undefined) {
      return this.signal;
    }
    return this.signal === undefined ? own : AbortSignal.any([own, this.signal]);
  }

  private async exclusive<T>(body: () => Promise<T>): Promise<T> {
    const release = await this.turn.acquire();
    try {
      return await body();
    } finally {
      release();
    }
  }

  /**
   * Appends messages to a partition as one batch within the transaction; see `produceBatch`.
   *
   * @throws MustAbortError an earlier call left the transaction to be aborted
   */
  produce(
    stream: string,
    partition: number,
    messages: OutMessage | readonly OutMessage[],
    options: CallOptions = {},
  ): Promise<ProduceResult> {
    const list = Array.isArray(messages) ? (messages as readonly OutMessage[]) : [messages as OutMessage];
    return this.produceBatch(stream, partition, buildBatch(list), options);
  }

  /**
   * Appends a batch to a partition within the transaction and returns the offset of its first record. Read_committed
   * readers see it only once the transaction commits. A call given up by its signal after the batch was sent rejects with
   * `UnknownOutcomeError`, and only an abort is left.
   *
   * @throws MustAbortError an earlier call left the transaction to be aborted
   */
  produceBatch(stream: string, partition: number, batch: Uint8Array, options: CallOptions = {}): Promise<ProduceResult> {
    return this.exclusive(() => this.produceLocked(stream, partition, batch, this.signalFor(options.signal)));
  }

  private async produceLocked(
    stream: string,
    partition: number,
    batch: Uint8Array,
    signal: AbortSignal | undefined,
  ): Promise<ProduceResult> {
    this.checkUsable();
    await this.add(stream, partition, signal);
    let records: number;
    try {
      records = Batch.parse(batch).batch.recordCount;
    } catch (e) {
      if (e instanceof RecordFormatError) {
        throw new BrokerResponseError(ErrorCode.CORRUPT_BATCH, e.message);
      }
      throw e;
    }
    const key = partKey(stream, partition);
    const seq = this.next.get(key) ?? 0;
    const stamped = batch.slice();
    Batch.setProducer(stamped, this.producerId, this.epoch, seq);
    Batch.setTransactional(stamped, true);
    // Until the outcome is known the sequence is in doubt: a call that ends some other way (the client closed, an error
    // that is not a ClientError) leaves the transaction to be aborted, rather than let the next batch take the same
    // sequence and be taken for this one.
    this.poisoned = "a produce ended before its outcome was known";
    let result: ProduceResult;
    try {
      result = await this.host.retrying(
        () => this.host.produceOnce(stream, partition, stamped, true, signal, this.txnId),
        signal,
        true,
      );
    } catch (e) {
      this.poisoned = null;
      this.poison(e);
      throw e;
    }
    this.poisoned = null;
    this.next.set(key, (seq + records) | 0);
    return result;
  }

  /**
   * Publishes a message within the transaction (ADR 0032): this client routes it by the exchange's bindings, as metadata
   * gave them within `metadataMaxAgeMs`, and produces it to every queue it goes to, so it reaches them all if the
   * transaction commits and none if it aborts. A message no queue takes is dropped, or refused with `UnroutableError` when
   * `mandatory`. If some queues hold it already when a write fails, only an abort is left.
   */
  publish(exchange: string, routingKey: string, message: OutMessage, options: PublishOptions = {}): Promise<Published> {
    return this.exclusive(async () => {
      this.checkUsable();
      const signal = this.signalFor(options.signal);
      const headers: Header[] = (message.headers ?? []).map((h) => ({
        key: typeof h.name === "string" ? Buffer.from(h.name, "utf8") : h.name,
        value: h.value === null ? null : typeof h.value === "string" ? Buffer.from(h.value, "utf8") : h.value,
      }));
      const queues = await this.host.routeMessage(exchange, routingKey, headers, options.mandatory === true);
      const batch = buildBatch([message]);
      const routed: RoutedQueue[] = [];
      for (const q of queues) {
        try {
          routed.push({ queue: q, offset: (await this.produceLocked(q, 0, batch, signal)).baseOffset });
        } catch (e) {
          // Some queues may hold it already: committing would deliver it to those only.
          if (routed.length > 0 && this.poisoned === null) {
            this.poisoned = `published to [${routed.map((r) => r.queue).join(", ")}] only: ${(e as Error).message}`;
          }
          throw e;
        }
      }
      return { routed };
    });
  }

  /**
   * Adds a partition (a queue: partition 0) to the transaction once.
   *
   * @internal
   */
  async add(name: string, partition: number, signal?: AbortSignal): Promise<void> {
    this.checkUsable();
    const key = partKey(name, partition);
    if (this.added.has(key)) {
      return;
    }
    try {
      await this.host.retrying(() => this.addPartitionOnce(name, partition, signal), signal);
    } catch (e) {
      this.poison(e);
      throw e;
    }
    this.added.add(key);
  }

  /** After an error only an abort is left, unless nothing was written and nothing else went wrong. */
  private poison(e: unknown): void {
    if (e instanceof GaveUpError || e instanceof UnknownStreamError) {
      return;
    }
    this.poisoned = e instanceof Error ? e.message : String(e);
  }

  /**
   * Leaves only an abort, for `why`.
   *
   * @internal
   */
  mustAbort(why: string): void {
    this.poisoned = why;
  }

  /**
   * Runs `body` with this transaction's turn, as `ackIn` does.
   *
   * @internal
   */
  inTurn<T>(body: () => Promise<T>): Promise<T> {
    return this.exclusive(body);
  }

  private async addPartitionOnce(name: string, partition: number, signal?: AbortSignal): Promise<void> {
    const routed = await this.host.route(name, partition, signal);
    const req: Request = {
      type: "AddPartitionsToTxn",
      txnId: this.txnId,
      producerId: this.producerId,
      producerEpoch: this.epoch,
      resources: [{ resourceId: routed.route.resourceId, partitions: [partition] }],
      tenant: this.host.tenant,
    };
    const resp = await this.host.callCoordinator(this.txnId, req);
    if (resp.type !== "AddPartitionsToTxn") {
      throw Attempt.retry("unexpected response");
    }
    const error = resp.resources[0]?.partitions[0]?.error ?? ErrorCode.NONE;
    if (error === ErrorCode.NONE) {
      return;
    }
    if (isRetriable(error)) {
      this.host.forgetCoordinator();
      throw Attempt.retry(String(error));
    }
    throw Attempt.fail(new BrokerResponseError(error, null));
  }

  /**
   * Commits the consumer's read positions with this transaction (exactly-once consume-transform-produce): they become the
   * group's committed offsets if it commits, and nothing changes if it aborts. A consumer that no longer owns a partition
   * is fenced, and the transaction can only be aborted.
   */
  sendOffsets(consumer: GroupConsumer, options: CallOptions = {}): Promise<void> {
    return this.exclusive(async () => {
      this.checkUsable();
      const signal = this.signalFor(options.signal);
      const req: Request = {
        type: "AddOffsetsToTxn",
        txnId: this.txnId,
        producerId: this.producerId,
        producerEpoch: this.epoch,
        groupId: consumer.group,
        memberId: consumer.memberId,
        memberEpoch: consumer.memberEpoch,
        resources: offsetResources(this.host, consumer.positionEntries()),
        tenant: this.host.tenant,
      };
      try {
        await this.host.retrying(() => commitOnce(this.host, this.txnId, req), signal, true);
      } catch (e) {
        // Unknown or refused: committing could commit offsets nobody knows, or a zombie's. Only an abort is left.
        this.poisoned = `offsets not added: ${(e as Error).message}`;
        throw e;
      }
    });
  }

  /**
   * Commits: once this resolves, the decision is durable and every partition will show the transaction to read_committed
   * readers. A commit given up after the request was sent rejects with `UnknownOutcomeError`.
   *
   * @throws MustAbortError an earlier call left the transaction to be aborted; it is still open
   */
  commit(options: CallOptions = {}): Promise<void> {
    return this.exclusive(async () => {
      this.checkUsable();
      await this.end(true, this.signalFor(options.signal));
    });
  }

  /** Aborts: none of the transaction's batches will be visible to read_committed readers. */
  abort(options: CallOptions = {}): Promise<void> {
    return this.exclusive(async () => {
      if (this.ended) {
        throw new TransactionEndedError();
      }
      // The transaction's own signal is what may have called for this abort: only the call's signal counts.
      await this.end(false, options.signal);
    });
  }

  private async end(commit: boolean, signal: AbortSignal | undefined): Promise<void> {
    this.ended = true;
    this.detach();
    if (this.added.size === 0) {
      this.finish({ producerId: this.producerId, epoch: this.epoch });
      return;
    }
    const req: Request = {
      type: "EndTxn",
      txnId: this.txnId,
      producerId: this.producerId,
      producerEpoch: this.epoch,
      commit,
      tenant: this.host.tenant,
    };
    let next: ProducerIds | null = null;
    try {
      next = await this.host.retrying(
        async () => {
          let resp: Response;
          try {
            resp = await this.host.callCoordinator(this.txnId, req);
          } catch (e) {
            // A lost request may have been decided: retry as maybe done.
            if (e instanceof Attempt && e.kind === "RETRY") {
              throw Attempt.retryMaybeDone(e.message);
            }
            throw e;
          }
          if (resp.type !== "EndTxn") {
            throw Attempt.retry(`unexpected response ${resp.type}`);
          }
          if (resp.error === ErrorCode.NONE) {
            return { producerId: resp.producerId, epoch: resp.producerEpoch };
          }
          if (isRetriable(resp.error)) {
            this.host.forgetCoordinator();
            // The coordinator may have decided before the answer was lost.
            throw Attempt.retryMaybeDone(String(resp.error));
          }
          throw Attempt.fail(new BrokerResponseError(resp.error, null));
        },
        signal,
        true,
      );
    } finally {
      // The next transaction uses the raised epoch; after a failure, a fresh producer id (which also aborts whatever is
      // left open).
      this.finish(next);
    }
  }

  private finish(next: ProducerIds | null): void {
    const release = this.releaseTurn;
    this.releaseTurn = null;
    this.host.transactionEnded(next);
    release?.();
  }

  private detach(): void {
    if (this.signal !== undefined && this.onSignal !== null) {
      this.signal.removeEventListener("abort", this.onSignal);
    }
  }

  /**
   * Ends this handle without deciding: if neither `commit` nor `abort` was called, the next transaction of this client
   * starts with a fresh producer id, and the coordinator aborts this one.
   */
  abandon(): void {
    if (!this.ended) {
      this.ended = true;
      this.detach();
      this.finish(null);
    }
  }

  /**
   * The end of an `await using` scope: a transaction that was neither committed nor aborted is aborted. If the abort does
   * not get through, the transaction is abandoned as by `abandon`, and the error is not thrown, so that it does not hide
   * the one that left the scope; call `abort` yourself to see it.
   */
  async dispose(): Promise<void> {
    if (this.ended) {
      return;
    }
    try {
      await this.abort();
    } catch {
      this.abandon();
    }
  }
}

// `await using` calls this at the end of its scope.
export interface Transaction extends AsyncDisposable {
  [Symbol.asyncDispose](): Promise<void>;
}

if (typeof Symbol.asyncDispose === "symbol") {
  Object.defineProperty(Transaction.prototype, Symbol.asyncDispose, {
    value: function (this: Transaction): Promise<void> {
      return this.dispose();
    },
    writable: true,
    configurable: true,
  });
}

function partKey(name: string, partition: number): string {
  return `${name}\0${partition}`;
}

import type { Address } from "../net/address.js";
import type { Connection } from "../net/connection.js";
import { ConnectionError } from "../net/errors.js";
import { describeError, ErrorCode, isRetriable } from "../proto/error-code.js";
import type { LeaderHint } from "../proto/messages.js";
import type { DeliverPush } from "../proto/index.js";
import { isActivation } from "../proto/frames.js";
import type { Request } from "../proto/request.js";
import type { Response } from "../proto/response.js";
import { Batch } from "../record/batch.js";
import { RecordFormatError } from "../record/errors.js";
import { U32_MAX } from "../record/unsigned.js";
import { Attempt, CallFailure } from "./attempt.js";
import { Delivery } from "./delivery.js";
import { BrokerResponseError, ConsumerClosedError, GaveUpError } from "./errors.js";
import { Mutex } from "./mutex.js";
import type { Transaction } from "./transaction.js";
import { offsetRanges } from "./offset-ranges.js";

/** What a consumer needs from the client that made it. */
export interface QueueHost {
  readonly tenant: string;
  retrying<T>(attempt: () => Promise<T>, signal?: AbortSignal): Promise<T>;
  route(queue: string, partition: number, signal?: AbortSignal): Promise<{ route: { resourceId: bigint }; address: Address }>;
  connect(address: Address): Promise<Connection>;
  call(address: Address, request: Request): Promise<Response>;
  callOn(connection: Connection, request: Request): Promise<Response>;
  forget(queue: string, partition: number): void;
  redirect(queue: string, partition: number, hint: LeaderHint | null): void;
}

/** What a call of a consumer may be given. */
export interface ConsumerCallOptions {
  /** Gives up the call; see `LotseClient`. */
  readonly signal?: AbortSignal;
}

const CLOSED = Symbol("closed");

type SubEvent = DeliverPush | ConnectionError | typeof CLOSED;

/** One subscription on one connection; its events are pushes and the end of the connection. */
class Sub {
  /** Waits as a standby of the queue's single active consumer (ADR 0076). */
  standby = false;
  readonly events: SubEvent[] = [];
  private wake: (() => void) | null = null;

  constructor(
    readonly conn: Connection,
    readonly id: number,
  ) {}

  add(event: SubEvent): void {
    this.events.push(event);
    const w = this.wake;
    if (w !== null) {
      w();
    }
  }

  /** Settles once an event is waiting, `ms` have passed, or `signal` fires. */
  wait(ms: number, signal?: AbortSignal): Promise<void> {
    if (this.events.length > 0 || signal?.aborted === true) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", done);
        this.wake = null;
        resolve();
      };
      const timer = setTimeout(done, ms);
      signal?.addEventListener("abort", done, { once: true });
      this.wake = done;
    });
  }
}

interface Subscribed {
  readonly sub: Sub;
  readonly resourceId: bigint;
}

function sendQuietly(conn: Connection, request: Request): void {
  try {
    conn.sendOnly(request);
  } catch (e) {
    if (!(e instanceof ConnectionError)) {
      throw e;
    }
    // the subscription ends with the connection
  }
}

/**
 * A subscription to a queue, from `LotseClient.subscribe`. Read messages with `next`, `poll` or `for await`, and settle
 * each with `ack` or `nack`.
 *
 * A subscription lives on one connection to the queue's leader. The leader pushes batches within the consumer's credit;
 * the consumer hands the messages out one at a time and adds credit once half of it is used up, so at most `credit`
 * messages wait unprocessed on this side.
 *
 * When the subscription ends (the leader changed, the connection broke, or the consumer held a message longer than the
 * queue's ack timeout) the leader gives its unacknowledged messages to other consumers, and the consumer subscribes
 * again on its next read once it has handed out the messages it already holds. Those may meanwhile have gone to someone
 * else, so a message can arrive more than once (`Delivery.deliveryCount` then exceeds 1); it is not lost.
 * Acknowledging a message that went to someone else still settles it: an ack names an offset, not a delivery.
 *
 * Reads (`next`, `poll`, the iterator) take turns when called together, in the order they were called. `ack`, `nack` and
 * `close` may be called at any time, also while a read waits.
 */
export class QueueConsumer implements AsyncIterable<Delivery> {
  private sub: Sub | null = null;
  private resourceId = 0n;
  private readonly buffered: Delivery[] = [];
  private usedMsgs = 0;
  private usedBytes = 0;
  private isClosed = false;
  private readonly reading = new Mutex();

  constructor(
    private readonly host: QueueHost,
    private readonly queue: string,
    private readonly credit: number,
  ) {}

  /** Subscribes for the first time, so that a queue that cannot be reached fails `subscribe`. */
  async start(signal?: AbortSignal): Promise<void> {
    await this.ensureSubscribed(signal);
  }

  // ---- subscribing ----

  private async ensureSubscribed(signal?: AbortSignal): Promise<void> {
    this.checkOpen();
    if (this.sub !== null) {
      return;
    }
    const made = await this.host.retrying(() => this.subscribeOnce(signal), signal);
    if (this.isClosed) {
      // Closed while subscribing: the broker has the subscription, so end it.
      made.sub.conn.unregister(made.sub.id);
      sendQuietly(made.sub.conn, { type: "Unsubscribe", subscriptionId: made.sub.id });
      throw new ConsumerClosedError();
    }
    this.sub = made.sub;
    this.resourceId = made.resourceId;
    this.usedMsgs = 0;
    this.usedBytes = 0;
  }

  private async subscribeOnce(signal?: AbortSignal): Promise<Subscribed> {
    const routed = await this.host.route(this.queue, 0, signal);
    let conn: Connection;
    try {
      conn = await this.host.connect(routed.address);
    } catch (e) {
      if (e instanceof CallFailure) {
        throw e.toAttempt();
      }
      throw e;
    }
    // Asks to be told when it becomes the single active consumer, should the queue have one (ADR 0076).
    const req: Request = {
      type: "Subscribe",
      queueId: routed.route.resourceId,
      consumerTag: "",
      creditMsgs: this.credit,
      creditBytes: U32_MAX,
      exclusive: false,
      notifyActive: true,
    };
    let resp: Response;
    try {
      resp = await this.host.callOn(conn, req);
    } catch (e) {
      if (e instanceof CallFailure) {
        // A subscription the broker may have made ends with the connection, which callOn has closed.
        this.host.forget(this.queue, 0);
        throw e.toAttempt();
      }
      throw e;
    }
    if (resp.type !== "Subscribe") {
      throw Attempt.retry(`unexpected response ${resp.type}`);
    }
    if (resp.error === ErrorCode.NONE) {
      const made = new Sub(conn, resp.subscriptionId);
      made.standby = resp.standby;
      try {
        conn.register(made.id, {
          onPush: (push) => made.add(push),
          onClose: (reason) => made.add(reason),
        });
      } catch (e) {
        if (e instanceof ConnectionError) {
          throw Attempt.retry("the connection broke");
        }
        throw e;
      }
      return { sub: made, resourceId: routed.route.resourceId };
    }
    if (isRetriable(resp.error)) {
      this.host.redirect(this.queue, 0, resp.leaderHint);
      throw Attempt.retry(describeError(resp.error));
    }
    if (resp.error === ErrorCode.UNKNOWN_RESOURCE) {
      // Deleted: the name may come back as a new queue.
      this.host.forget(this.queue, 0);
    }
    throw Attempt.fail(new BrokerResponseError(resp.error, null));
  }

  // ---- receiving ----

  /**
   * The next message, waiting for one as long as it takes. Subscribes again first if the subscription has ended.
   *
   * @throws ConsumerClosedError if the consumer is closed, also while waiting
   * @throws GaveUpError if the queue's leader cannot be reached within the client's `retryForMs`, or `signal` fires
   */
  async next(options: ConsumerCallOptions = {}): Promise<Delivery> {
    for (;;) {
      const d = await this.poll(1_000, options);
      if (d !== null) {
        return d;
      }
      if (options.signal?.aborted === true) {
        throw new GaveUpError(0, "aborted");
      }
    }
  }

  /**
   * The next message, waiting up to `waitMs` for one; `null` if none came, or if `signal` fired. Subscribes again first
   * if the subscription has ended.
   *
   * @throws ConsumerClosedError if the consumer is closed
   */
  async poll(waitMs: number, options: ConsumerCallOptions = {}): Promise<Delivery | null> {
    const signal = options.signal;
    const release = await this.reading.acquire();
    try {
      return await this.pollNow(waitMs, signal);
    } finally {
      release();
    }
  }

  private async pollNow(waitMs: number, signal?: AbortSignal): Promise<Delivery | null> {
    const deadline = performance.now() + Math.max(0, waitMs);
    for (;;) {
      this.checkOpen();
      const out = this.buffered.shift();
      if (out !== undefined) {
        this.usedMsgs++;
        const credit = this.refill();
        if (credit !== null && this.sub !== null) {
          // A lost credit is noticed as a broken connection by the next wait.
          sendQuietly(this.sub.conn, credit);
        }
        return out;
      }
      try {
        await this.ensureSubscribed(signal);
      } catch (e) {
        if (signal?.aborted === true && e instanceof GaveUpError) {
          return null;
        }
        throw e;
      }
      const left = deadline - performance.now();
      if (left <= 0 || signal?.aborted === true) {
        return null;
      }
      this.checkOpen();
      const waitingOn = this.sub;
      if (waitingOn === null) {
        continue;
      }
      await waitingOn.wait(left, signal);
      const event = waitingOn.events.shift();
      if (event === undefined) {
        return null; // nothing came in time, or the signal fired
      }
      this.handle(waitingOn, event);
    }
  }

  private handle(from: Sub, event: SubEvent): void {
    if (event === CLOSED) {
      this.checkOpen();
      return;
    }
    if (event instanceof ConnectionError) {
      this.ended(from);
      return;
    }
    const push = event;
    if (push.error !== ErrorCode.NONE) {
      if (push.error === ErrorCode.NOT_LEADER) {
        this.host.redirect(this.queue, 0, push.leaderHint);
      } else if (push.error === ErrorCode.UNKNOWN_RESOURCE) {
        this.host.forget(this.queue, 0);
      }
      this.ended(from);
      return;
    }
    // Its turn as the single active consumer, or messages, which come to the active consumer only.
    from.standby = false;
    if (isActivation(push)) {
      return;
    }
    let records;
    try {
      records = Batch.parse(push.batch).batch.records();
    } catch (e) {
      if (e instanceof RecordFormatError) {
        // A codec missing here is no damage: say which one to add (ADR 0085).
        if (e.kind === "UNSUPPORTED_COMPRESSION") {
          throw e;
        }
        throw new BrokerResponseError(ErrorCode.CORRUPT_BATCH, e.message);
      }
      throw e;
    }
    // The batch is as stored: it can hold messages that are not for this subscription.
    const mine = new Map<bigint, number>();
    for (const d of push.deliveries) {
      mine.set(d.offset, d.deliveryCount);
    }
    if (this.sub !== from) {
      return; // the subscription ended meanwhile (closed)
    }
    for (const r of records) {
      const count = mine.get(r.offset);
      if (count !== undefined) {
        this.buffered.push(new Delivery(r, count, from));
      }
    }
    this.usedBytes = Math.min(U32_MAX, this.usedBytes + push.batch.length);
  }

  /**
   * The subscription is over. Called only when nothing is buffered (the buffer is read before the events), so the
   * clearing is for a consumer closed meanwhile.
   */
  private ended(which: Sub): void {
    if (this.sub !== which) {
      return;
    }
    this.sub = null;
    this.buffered.length = 0;
    which.conn.unregister(which.id);
  }

  /** The request that adds credit once half of it is used up, or `null`. */
  private refill(): Request | null {
    if (this.usedMsgs < Math.floor((this.credit + 1) / 2) || this.sub === null) {
      return null;
    }
    const req: Request = {
      type: "Credit",
      subscriptionId: this.sub.id,
      addMsgs: Math.min(this.usedMsgs, U32_MAX),
      addBytes: this.usedBytes,
    };
    this.usedMsgs = 0;
    this.usedBytes = 0;
    return req;
  }

  /**
   * Whether the subscription waits as a standby of its queue's single active consumer (ADR 0076): subscribed, and not
   * its turn yet. The broker tells a standby when it becomes active; this is false from the next read that sees it.
   */
  get isStandby(): boolean {
    return this.sub !== null && this.sub.standby;
  }

  /**
   * The messages as an async iterator: `for await (const d of consumer)`. It waits for each message, subscribes again
   * when the subscription ended, and ends when the consumer is closed or `signal` fires. Leaving the loop does not
   * close the consumer.
   */
  async *deliveries(options: ConsumerCallOptions = {}): AsyncGenerator<Delivery, void> {
    for (;;) {
      let d: Delivery | null;
      try {
        d = await this.poll(1_000, options);
      } catch (e) {
        if (e instanceof ConsumerClosedError) {
          return;
        }
        throw e;
      }
      if (d !== null) {
        yield d;
      } else if (options.signal?.aborted === true) {
        return;
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<Delivery> {
    return this.deliveries();
  }

  // ---- settling ----

  /**
   * Acknowledges messages in as few ranges as their offsets allow: once this resolves, they are never delivered
   * again. The outcome of an ack that was lost is retried: settling twice is harmless.
   */
  async ack(deliveries: Delivery | readonly Delivery[], options: ConsumerCallOptions = {}): Promise<void> {
    const list = asList(deliveries);
    if (list.length > 0) {
      await this.settle(offsetRanges(list.map((d) => d.offset)), false, options.signal);
    }
  }

  /**
   * Acknowledges a message within a transaction (ADR 0075): the queue joins the transaction, the leader takes the message
   * out of flight and the transaction holds it, so that the commit settles it and an abort gives it back; no one else
   * receives it meanwhile. Only a message this subscription holds can be acknowledged so, and the request goes on the
   * subscription's own connection. Any error leaves the transaction to be aborted. `MESSAGE_NOT_HELD` (a
   * `BrokerResponseError`) means the message has gone back, or another transaction holds it.
   */
  async ackIn(txn: Transaction, delivery: Delivery, options: ConsumerCallOptions = {}): Promise<void> {
    await txn.inTurn(async () => {
      await txn.add(this.queue, 0, options.signal);
      try {
        await this.ackInOnce(txn, delivery, options.signal);
      } catch (e) {
        txn.mustAbort((e as Error).message);
        throw e;
      }
    });
  }

  private async ackInOnce(txn: Transaction, delivery: Delivery, signal?: AbortSignal): Promise<void> {
    // The messages are the subscription's connection's: on another one they are not held.
    const on = this.sub;
    if (on === null) {
      throw new BrokerResponseError(ErrorCode.MESSAGE_NOT_HELD, "the subscription ended; the message went back");
    }
    const req: Request = {
      type: "Ack",
      queueId: this.resourceId,
      txnId: txn.txnId,
      ranges: offsetRanges([delivery.offset]),
      tenant: this.host.tenant,
      producerId: txn.producerId,
      producerEpoch: txn.epoch,
    };
    await this.host.retrying(async () => {
      let resp: Response;
      try {
        resp = await this.host.callOn(on.conn, req);
      } catch (e) {
        if (e instanceof CallFailure) {
          switch (e.kind) {
            // Sent again on the same connection: the leader takes a retry for what the transaction holds.
            case "LOST":
              throw Attempt.retryMaybeDone(e.message);
            case "NOT_SENT":
              throw Attempt.fail(
                new BrokerResponseError(ErrorCode.MESSAGE_NOT_HELD, "the connection broke; the message went back"),
              );
            default:
              throw e.toAttempt();
          }
        }
        throw e;
      }
      if (resp.type === "Outcome") {
        if (resp.error === ErrorCode.NONE) {
          return;
        }
        throw Attempt.fail(new BrokerResponseError(resp.error, null));
      }
      throw Attempt.retryMaybeDone(`unexpected response ${resp.type}`);
    }, signal);
  }

  /**
   * Rejects messages. With `requeue` they are delivered again, to any consumer; without it, they are discarded like
   * acknowledged ones (or dead-lettered, if the queue has a dead-letter queue and the limits say so). A requeue is one
   * request on the subscription's own connection and is not retried, so it takes no signal.
   */
  async nack(deliveries: Delivery | readonly Delivery[], requeue: boolean, options: ConsumerCallOptions = {}): Promise<void> {
    const list = asList(deliveries);
    if (list.length === 0) {
      return;
    }
    if (!requeue) {
      await this.settle(offsetRanges(list.map((d) => d.offset)), true, options.signal);
      return;
    }
    await this.requeue(list, 0);
  }

  /**
   * Gives a message back to be delivered again, to any consumer, once `delayMs` has passed (ADR 0077): a retry with
   * backoff. Rounded down to whole milliseconds, at most `u32::MAX` (about 49 days); a delay of 0 is `nack` with
   * requeue. If the subscription has ended, the message went back with it, without a delay.
   */
  async nackAfter(delivery: Delivery, delayMs: number): Promise<void> {
    const ms = Math.min(Math.max(Math.trunc(Number.isNaN(delayMs) ? 0 : delayMs), 0), U32_MAX);
    await this.requeue([delivery], ms);
  }

  private async requeue(deliveries: readonly Delivery[], delayMs: number): Promise<void> {
    // Only the subscription's connection can give a message back; if it is gone, so is the message (it went back with
    // the subscription). A message of an earlier subscription is gone with it.
    const on = this.sub;
    if (on === null) {
      return;
    }
    const held = deliveries.filter((d) => d.subscription === on);
    if (held.length === 0) {
      return;
    }
    const req: Request = {
      type: "Nack",
      queueId: this.resourceId,
      ranges: offsetRanges(held.map((d) => d.offset)),
      requeue: true,
      delayMs,
      subscriptionId: on.id, // ADR 0106
    };
    let resp: Response;
    try {
      resp = await this.host.callOn(on.conn, req);
    } catch (e) {
      if (e instanceof CallFailure) {
        // Not done (no leader, broken connection): the subscription ends with it and the message goes back anyway.
        return;
      }
      throw e;
    }
    if (resp.type === "Outcome" && resp.error !== ErrorCode.NONE && !isRetriable(resp.error)) {
      throw new BrokerResponseError(resp.error, null);
    }
  }

  /** An `Ack` or a discarding `Nack` to the queue's leader, retried. */
  private async settle(ranges: ReturnType<typeof offsetRanges>, nack: boolean, signal?: AbortSignal): Promise<void> {
    await this.host.retrying(async () => {
      const routed = await this.host.route(this.queue, 0, signal);
      const queueId = routed.route.resourceId;
      const req: Request = nack
        ? { type: "Nack", queueId, ranges, requeue: false, delayMs: 0 }
        : { type: "Ack", queueId, ranges };
      let resp: Response;
      try {
        resp = await this.host.call(routed.address, req);
      } catch (e) {
        if (e instanceof CallFailure) {
          this.host.forget(this.queue, 0);
          if (e.kind === "DENIED" || e.kind === "REFUSED") {
            throw e.toAttempt();
          }
          throw Attempt.retryMaybeDone(e.describe());
        }
        throw e;
      }
      if (resp.type !== "Outcome") {
        throw Attempt.retry(`unexpected response ${resp.type}`);
      }
      if (resp.error === ErrorCode.NONE) {
        return;
      }
      if (isRetriable(resp.error)) {
        this.host.redirect(this.queue, 0, resp.leaderHint);
        throw Attempt.retryMaybeDone(describeError(resp.error));
      }
      throw Attempt.fail(new BrokerResponseError(resp.error, null));
    }, signal);
  }

  // ---- closing ----

  /**
   * Ends the subscription. Messages handed out stay with this consumer until acknowledged (or until this client's
   * connection closes); the ones not handed out yet, also those pushed and not read, go back to the queue. A read that waits rejects with
   * `ConsumerClosedError`; an iterator ends. Closing twice is harmless.
   */
  close(): void {
    if (this.isClosed) {
      return;
    }
    this.isClosed = true;
    const ending = this.sub;
    this.sub = null;
    const requests: Request[] = [];
    if (ending !== null) {
      requests.push({ type: "Unsubscribe", subscriptionId: ending.id });
      // Received means buffered here or pushed and not read yet: the broker holds both for this subscription.
      const unread = this.buffered.map((d) => d.offset);
      for (const e of ending.events) {
        if (typeof e === "object" && !(e instanceof ConnectionError) && e.error === ErrorCode.NONE) {
          unread.push(...e.deliveries.map((d) => d.offset));
        }
      }
      if (unread.length > 0) {
        requests.push({
          type: "Nack",
          queueId: this.resourceId,
          ranges: offsetRanges(unread),
          requeue: true,
          delayMs: 0,
          // Named (ADR 0106): sent late, it must not take what a newer subscription on the connection holds.
          subscriptionId: ending.id,
        });
      }
    }
    this.buffered.length = 0;
    if (ending !== null) {
      ending.conn.unregister(ending.id);
      ending.add(CLOSED);
      for (const r of requests) {
        sendQuietly(ending.conn, r);
      }
    }
  }

  private checkOpen(): void {
    if (this.isClosed) {
      throw new ConsumerClosedError();
    }
  }

  /** The address this consumer's subscription is on, or `undefined`; for tests. */
  subscribedAddress(): Address | undefined {
    return this.sub?.conn.address;
  }
}

function asList(d: Delivery | readonly Delivery[]): readonly Delivery[] {
  return d instanceof Delivery ? [d] : d;
}

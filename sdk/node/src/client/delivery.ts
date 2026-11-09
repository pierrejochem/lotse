import type { BatchRecord, Header } from "../record/types.js";
import { asI64 } from "../record/unsigned.js";

/**
 * Where a dead-lettered message came from, read from the `x-lotse-dead-letter-*` headers the broker adds when it moves a
 * message to a dead-letter queue. They describe the latest move.
 */
export interface DeadLetter {
  /** Why: `max-deliveries` or `expired`. */
  readonly reason: string;
  /** The name of the queue the message came from. */
  readonly queue: string;
  /** That queue's resource id. */
  readonly queueId: bigint;
  /** The message's offset there. */
  readonly offset: bigint;
  /** How often it had been delivered there. */
  readonly deliveries: number;
  /** That queue's ack floor when the message was sent: every offset below it is settled. */
  readonly floor: bigint;
}

const REASON = "x-lotse-dead-letter-reason";
const QUEUE = "x-lotse-dead-letter-queue";
const QUEUE_ID = "x-lotse-dead-letter-queue-id";
const OFFSET = "x-lotse-dead-letter-offset";
const DELIVERIES = "x-lotse-dead-letter-deliveries";
const FLOOR = "x-lotse-dead-letter-floor";

const utf8 = new TextDecoder();

/**
 * One message from a queue, from `QueueConsumer.next`, `poll` or the consumer's async iterator. Settle it with
 * `QueueConsumer.ack` or `nack`.
 */
export class Delivery {
  /** @internal The subscription this message came on; a requeue is honoured only by that one. */
  readonly subscription: object;

  constructor(
    /** The record as stored. */
    readonly record: BatchRecord,
    /** How often the message has been delivered, this time included. More than 1 means an earlier delivery was not acknowledged. */
    readonly deliveryCount: number,
    subscription: object,
  ) {
    this.subscription = subscription;
  }

  /** The message's offset in the queue, a `u64` bit pattern; it identifies the message for acks. */
  get offset(): bigint {
    return this.record.offset;
  }

  /** The producer's timestamp, in milliseconds since the epoch. */
  get timestamp(): bigint {
    return this.record.timestamp;
  }

  /** The key, or `null`. */
  get key(): Uint8Array | null {
    return this.record.key;
  }

  /** The value, or `null`. */
  get value(): Uint8Array | null {
    return this.record.value;
  }

  /** The headers, in the order the producer set them. */
  get headers(): readonly Header[] {
    return this.record.headers;
  }

  /** The value as UTF-8 text, or `null`. */
  text(): string | null {
    return this.record.value === null ? null : utf8.decode(this.record.value);
  }

  /**
   * Where this message came from, if a dead-letter move brought it to this queue: present when the
   * `x-lotse-dead-letter-reason` header is. A missing or unreadable number reads as 0, a missing name as the empty
   * string.
   */
  deadLetter(): DeadLetter | null {
    const reason = this.header(REASON);
    if (reason === null) {
      return null;
    }
    return {
      reason,
      queue: this.header(QUEUE) ?? "",
      queueId: this.number(QUEUE_ID),
      offset: this.number(OFFSET),
      deliveries: Number(BigInt.asIntN(32, this.number(DELIVERIES))),
      floor: this.number(FLOOR),
    };
  }

  private header(name: string): string | null {
    // The broker replaces the headers of an earlier move, so each appears once; the last wins if not.
    let found: string | null = null;
    for (const h of this.record.headers) {
      if (h.value !== null && utf8.decode(h.key) === name) {
        found = utf8.decode(h.value);
      }
    }
    return found;
  }

  private number(name: string): bigint {
    const v = this.header(name);
    if (v === null || !/^\+?\d+$/.test(v)) {
      return 0n;
    }
    const n = BigInt(v);
    return n > 0xffff_ffff_ffff_ffffn ? 0n : asI64(n);
  }

  toString(): string {
    return `Delivery{offset=${BigInt.asUintN(64, this.offset)}, deliveryCount=${this.deliveryCount}}`;
  }
}

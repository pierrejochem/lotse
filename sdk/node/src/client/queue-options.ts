import type { Config } from "../proto/messages.js";
import { U32_MAX } from "../record/unsigned.js";

/** The credit used when none is given: 64 messages. */
export const DEFAULT_CREDIT = 64;

/**
 * How a queue consumer is subscribed.
 *
 * Credit is how many messages the queue's leader may have pushed to the consumer and not yet counted as used. At most
 * that many messages wait unprocessed in the client. A consumer that works through a message slowly wants a small
 * credit, so that the messages it is not working on can go to others. A value below 1 is taken as 1.
 *
 * Exclusive subscriptions are not offered: the broker refuses them. A queue with `singleActiveConsumer` (ADR 0076) is
 * set when it is created, not here.
 */
export interface SubscribeOptions {
  /** Messages of credit. Default 64. */
  readonly credit?: number;
}

/** The credit `options` ask for, at least 1 and at most `u32::MAX`. */
export function resolveCredit(options: SubscribeOptions): number {
  const c = options.credit ?? DEFAULT_CREDIT;
  if (!Number.isFinite(c)) {
    throw new RangeError(`credit must be a number, not ${c}`);
  }
  return Math.min(Math.max(1, Math.trunc(c)), U32_MAX);
}

/**
 * Settings of a queue for `LotseClient.createQueue`. The queue is replicated on 3 nodes unless told otherwise.
 * Durations are milliseconds.
 */
export interface QueueOptions {
  /** The number of nodes holding the queue (1 to 255). Default 3. */
  readonly replication?: number;
  /** How long a consumer may hold a message unacknowledged before it goes to others (`ack.timeout.ms`). */
  readonly ackTimeoutMs?: number;
  /** How often a message is delivered before the queue retires it (`max.deliveries`); 0 is no limit. */
  readonly maxDeliveries?: number;
  /** How long a message may wait in the queue before it is retired (`message.ttl.ms`). */
  readonly messageTtlMs?: number;
  /** A queue of the same tenant that receives the messages the limits retire; without one they are dropped. */
  readonly deadLetterQueue?: string;
  /** Deliver to one subscription at a time, the others wait as standbys (`single.active.consumer`, ADR 0076). */
  readonly singleActiveConsumer?: boolean;
  /** Priority levels, 2 to 10 (`priority.levels`, ADR 0078); 1 means none, the default. */
  readonly priorityLevels?: number;
  /** Any other queue settings, by key. A key that a field above also sets is a `RangeError`. */
  readonly settings?: Readonly<Record<string, string>>;
}

function whole(name: string, v: number, min: number, max: number): number {
  if (!Number.isInteger(v) || v < min || v > max) {
    throw new RangeError(`${name} must be a whole number from ${min} to ${max}, not ${v}`);
  }
  return v;
}

/** The replication and the settings pairs (sorted by key) a `CreateQueue` request carries. */
export function queueConfigs(o: QueueOptions): { replication: number; configs: Config[] } {
  const replication = whole("replication", o.replication ?? 3, 1, 255);
  const map = new Map<string, string>();
  const set = (key: string, value: string): void => {
    if (map.has(key)) {
      throw new RangeError(`setting ${key} is given twice`);
    }
    map.set(key, value);
  };
  if (o.ackTimeoutMs !== undefined) {
    set("ack.timeout.ms", String(whole("ackTimeoutMs", o.ackTimeoutMs, 1, Number.MAX_SAFE_INTEGER)));
  }
  if (o.maxDeliveries !== undefined) {
    set("max.deliveries", String(whole("maxDeliveries", o.maxDeliveries, 0, U32_MAX)));
  }
  if (o.messageTtlMs !== undefined) {
    set("message.ttl.ms", String(whole("messageTtlMs", o.messageTtlMs, 1, Number.MAX_SAFE_INTEGER)));
  }
  if (o.singleActiveConsumer !== undefined) {
    set("single.active.consumer", o.singleActiveConsumer ? "true" : "false");
  }
  if (o.priorityLevels !== undefined) {
    set("priority.levels", String(whole("priorityLevels", o.priorityLevels, 1, 10)));
  }
  // Explicit fields were set first, so a clash with `settings` shows as a duplicate.
  const explicit = new Set(map.keys());
  for (const [k, v] of Object.entries(o.settings ?? {})) {
    if (explicit.has(k)) {
      throw new RangeError(`setting ${k} is given twice`);
    }
    map.set(k, v);
  }
  const configs = [...map.keys()].sort().map((key) => ({ key, value: map.get(key) as string }));
  return { replication, configs };
}

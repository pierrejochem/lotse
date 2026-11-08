import type { BindingMetadata, ExchangeMetadata } from "../proto/messages.js";
import { ExchangeType } from "../proto/messages.js";
import type { Header } from "../record/types.js";
import { compareU64 } from "../record/unsigned.js";

/**
 * An exchange and its bindings, as metadata gives them, and which queues a message goes to. The rules are those of
 * `lotse-route`, which the broker and the Rust client use, checked against `conformance/routing.txt`:
 *
 * - **direct**: the binding key equals the routing key;
 * - **topic**: keys are words split at `.`; in a binding key `*` matches exactly one word and `#` zero or more; an empty
 *   key has no words;
 * - **fanout**: every binding;
 * - **headers**: the binding's headers must be present with equal values (a null value equals only null), all of them
 *   or any one; a binding without headers matches everything when it asks for all and nothing when it asks for any.
 *
 * A queue bound twice gets a message once. An exchange of a type this library does not know routes nowhere.
 */
export class Exchange {
  constructor(
    readonly name: string,
    readonly resourceId: bigint,
    readonly type: number,
    readonly bindings: readonly BindingMetadata[],
  ) {}

  static of(name: string, resourceId: bigint, x: ExchangeMetadata): Exchange {
    return new Exchange(name, resourceId, x.exchangeType, x.bindings);
  }

  /** The ids of the queues a message with `routingKey` and `headers` goes to, ascending as `u64`. */
  route(routingKey: string, headers: readonly Header[]): bigint[] {
    const out = new Set<bigint>();
    for (const b of this.bindings) {
      let hit: boolean;
      switch (this.type) {
        case ExchangeType.DIRECT:
          hit = b.bindingKey === routingKey;
          break;
        case ExchangeType.TOPIC:
          hit = topicMatches(b.bindingKey, routingKey);
          break;
        case ExchangeType.FANOUT:
          hit = true;
          break;
        case ExchangeType.HEADERS:
          hit = headersMatch(b, headers);
          break;
        default:
          hit = false;
      }
      if (hit) {
        out.add(b.queueId);
      }
    }
    return [...out].sort(compareU64);
  }
}

/** Whether a topic binding key matches a routing key. */
export function topicMatches(pattern: string, key: string): boolean {
  return matches(words(pattern), 0, words(key), 0);
}

/** An empty key has no words, not one empty word. */
function words(s: string): string[] {
  return s === "" ? [] : s.split(".");
}

function matches(pattern: readonly string[], p: number, key: readonly string[], k: number): boolean {
  if (p === pattern.length) {
    return k === key.length;
  }
  const w = pattern[p];
  if (w === "#") {
    // # takes zero words, or one and stays.
    return matches(pattern, p + 1, key, k) || (k < key.length && matches(pattern, p, key, k + 1));
  }
  if (k === key.length) {
    return false;
  }
  return (w === "*" || w === key[k]) && matches(pattern, p + 1, key, k + 1);
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && Buffer.compare(a, b) === 0;
}

function headersMatch(b: BindingMetadata, present: readonly Header[]): boolean {
  if (b.headerMatch.length === 0) {
    return b.matchAll;
  }
  const hits = b.headerMatch.map((m) =>
    present.some(
      (h) =>
        bytesEqual(h.key, m.key) &&
        (h.value === null || m.value === null ? h.value === m.value : bytesEqual(h.value, m.value)),
    ),
  );
  return b.matchAll ? !hits.includes(false) : hits.includes(true);
}

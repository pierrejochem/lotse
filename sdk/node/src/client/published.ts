/** One queue that holds a published message. */
export interface RoutedQueue {
  /** The queue's name. */
  readonly queue: string;
  /** The message's offset there. */
  readonly offset: bigint;
}

/** Where a published message went: each queue it was routed to and the message's offset there, in queue name order. */
export interface Published {
  readonly routed: readonly RoutedQueue[];
}

/** What `publish` takes besides the message. */
export interface PublishOptions {
  /** Refuse (with `UnroutableError`) a message that no queue takes, instead of dropping it. Default false. */
  readonly mandatory?: boolean;
  /** Gives up the call; see `CallOptions`. */
  readonly signal?: AbortSignal;
}

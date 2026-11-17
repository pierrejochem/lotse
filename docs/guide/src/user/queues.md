# Queues

A queue delivers each message to one consumer at a time and keeps it until a consumer acknowledges it. Use it for work that must be done once by one of several workers, and retried when a worker fails.

## A work queue with a dead-letter queue

The example creates two queues, writes five jobs, works them off, and lets one job fail until the queue retires it.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/QueueExample.java}}
```

Points to note:

- You write to a queue with `produce(queue, 0, message)`: a queue is partition 0. Messages can also arrive through an [exchange](exchanges.md).
- `subscribe(queue, SubscribeOptions.withCredit(16))` opens a subscription. **Credit** is how many messages the broker may have out with you at once. Each ack gives credit back; the default is 64.
- `poll(wait)` returns the next delivery, or `null` after `wait`. `next()` waits without limit.
- `ack(delivery)` settles a message. `nack(delivery, true)` gives it back to be delivered again; `nack(delivery, false)` discards it. `nackAfter(delivery, delay)` gives it back after a delay, for a retry with backoff.
- `delivery.deliveryCount()` says how many times the broker has sent the message.
- A message you neither ack nor nack goes out again when the **ack timeout** passes (`ack.timeout.ms`) or when you close the consumer. After a leader change or a broken connection the consumer subscribes again and keeps its messages, also within a transaction; if it does not come back within the **resume window** (`resume.window.ms`, 30 s by default), they go to others ([ADR 0109](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0109-deliveries-are-written-to-the-log.md)). Process messages so that doing one twice is harmless, or use a [transaction](transactions.md).

## Settings

Set at creation with `QueueOptions`, or later with `alterConfigs(name, set, reset)`. All limits are off by default. `describeConfigs(name)` reads them.

| Key | Meaning | Values | `QueueOptions` |
| --- | --- | --- | --- |
| `ack.timeout.ms` | Time a delivered message may stay unacknowledged | Milliseconds above 0 | `ackTimeout` |
| `resume.window.ms` | After a leader change, how long a consumer's messages wait for it to subscribe again | Milliseconds above 0; default 30,000 | `alterConfigs` |
| `max.deliveries` | A message goes out at most this often; the next time it is retired | 1 to 65,535 | `maxDeliveries` |
| `message.ttl.ms` | A message not delivered within this time after the broker stored it is retired | Milliseconds above 0 | `messageTtl` |
| `dead.letter.queue` | Where retired messages go | Name of a queue in the same tenant | `deadLetterQueue` |
| `single.active.consumer` | Only one subscription gets messages | `true` or `false` | `alterConfigs` |
| `priority.levels` | Number of priority levels | 1 to 10; 1 means none | `alterConfigs` |
| `max.length` | Messages that may wait for delivery | Messages above 0 | `maxLength` |
| `max.length.bytes` | Bytes of keys, values and headers that may wait for delivery | Bytes above 0 | `maxLengthBytes` |
| `overflow` | What a full queue does | `reject-publish` (default) or `drop-head` | `overflow` |

`resume.window.ms` and the last two are set with `alterConfigs` in the Java SDK:

```java
// Not from the compiled examples.
client.alterConfigs("example-jobs", Map.of("single.active.consumer", "true", "priority.levels", "3"), List.of());
```

Changing settings, purging and requeueing need the tenant-admin role on the queue.

## Retries and dead letters

A **delivery attempt** is a delivery that did not end in an ack: a nack with requeue, an ack timeout, or a lost connection. With `max.deliveries` set, the broker retires a message when it would otherwise go out once more. With `message.ttl.ms` set, it retires a message that waited too long. A message a consumer holds does not expire.

Retired messages move to the `dead.letter.queue` if there is one. **Without one, they are dropped.** Set a dead-letter queue when messages matter.

The move happens once, also across failovers. The copy keeps key, value and headers, and gets these headers, which `delivery.deadLetter()` reads:

| Header | Content |
| --- | --- |
| `x-lotse-dead-letter-reason` | `max-deliveries`, `expired`, `requeued` or `max-length` |
| `x-lotse-dead-letter-queue` | The queue it came from |
| `x-lotse-dead-letter-queue-id` | That queue's id |
| `x-lotse-dead-letter-offset` | Its offset there |
| `x-lotse-dead-letter-deliveries` | Deliveries before it was retired |
| `x-lotse-dead-letter-floor` | The source queue's retention floor when it moved |

A queue cannot be its own dead-letter queue, a chain of dead-letter queues that leads back is refused, and a queue that is someone's dead-letter queue cannot be deleted. Delivery counts are kept in the queue's log, so they go on across failovers ([ADR 0109](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0109-deliveries-are-written-to-the-log.md)). See [ADR 0055](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0055-dead-letter-queues-and-message-ttl.md).

## Length limits

A queue may limit what waits for delivery ([ADR 0118](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0118-queue-length-limits.md)): `max.length` messages, `max.length.bytes` bytes of keys, values and headers, or both. Every message a consumer could still get counts, also one not yet committed or in an open transaction. A message out with a consumer does not count, as in RabbitMQ, so a consumer with a large credit lets the queue hold more.

What a full queue does is its `overflow`:

- `reject-publish`, the default: a produce, a publish or a transaction's write that would pass the limit fails with `QUEUE_FULL`, and nothing is written. The client does not retry it; try again once consumers have made room. A dead letter for a full queue waits in its source queue until there is room.
- `drop-head`: the queue takes every write and retires its oldest waiting messages until it fits, into its dead-letter queue with the reason `max-length` if it has one. **Without one, they are dropped.**

If the answer to an earlier attempt was lost, `QUEUE_FULL` does not say whether that attempt was written. The client then reports the outcome as unknown.

The limits can change at any time with `alterConfigs`; a queue already over a new limit refuses, or drops down to it. After a leader change, the new leader reads the queue's backlog first and may refuse while it does.

```java
// Not from the compiled examples.
client.createQueue("orders", QueueOptions.builder().maxLength(100_000).maxLengthBytes(1L << 30).build());
client.alterConfigs("orders", Map.of("overflow", "drop-head"), List.of());
```

## Purge and requeue

`purge(queue)` settles every message that is ready: not held by a consumer, not in an open transaction. Messages written while it runs are left alone. It returns the number purged.

`requeue(deadLetterQueue)` moves the ready messages of a dead-letter queue back to the queue named in each message's dead-letter headers, once each. A message without those headers, or whose queue is gone, stays. It returns the number moved. The copy gets the reason `requeued`. See [ADR 0064](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0064-purge-and-requeue.md). The [console](console.md) has both as buttons.

## Delayed delivery

Two ways to make a message wait ([ADR 0077](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0077-delayed-delivery.md)):

- **On publish**, set the header `x-lotse-delay-ms` to a decimal number of milliseconds, 0 to 4,294,967,295 (about 49 days). The delay counts from the time the queue's leader stored the message, so the producer's clock does not matter. Any other value means no delay.
- **On nack**, `nackAfter(delivery, delay)`. It gives the message back to any consumer once the delay has passed.

```java
// Not from the compiled examples: a reminder in ten minutes.
client.produce("reminders", 0,
        OutMessage.value("call back").withHeader("x-lotse-delay-ms", "600000"));
```

A delayed message never blocks the queue: others keep flowing, and the delayed one is delivered first once it is due. A dead-lettered or requeued copy does not wait again.

## Priorities

With `priority.levels` from 2 to 10, set the header `x-lotse-priority` to a decimal number from 0. Higher goes first. A value above the queue's highest level counts as the highest; anything that is not a number from 0 to 255 is level 0. Within a level, the oldest message goes first. Dead letters and requeued messages are level 0 ([ADR 0078](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0078-queue-priorities.md)). The console's queue page and `DescribeMetrics` show how many messages are ready at each level; after a failover, messages whose level the new leader has not read yet are counted apart until it has. They also show which subscription of a single active consumer queue is active and which ones are standbys ([ADR 0120](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0120-queue-metrics-per-priority-level-and-consumer-state.md)).

```java
// Not from the compiled examples.
client.produce("jobs", 0, OutMessage.value("urgent").withHeader("x-lotse-priority", "2"));
```

## Single active consumer

With `single.active.consumer = true`, one subscription gets all messages and the others wait as **standbys** in the order they subscribed. `consumer.isStandby()` says whether yours still waits. When the active one ends, the oldest standby takes over only once nothing is out with anyone, so the queue's order holds across the handover.

This holds per leader term. After a leader change, a deposed leader that has not noticed may still deliver to the old active consumer for a short time; that consumer's acks then fail ([ADR 0076](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0076-single-active-consumer.md)).

## Kotlin

`lotse.subscribe(queue)` returns a `LotseQueue` with `ack`, `nack`, `nackAfter` as `suspend` functions, and `deliveries()` as a `Flow`:

```kotlin
// Not from the compiled examples.
lotse.subscribe("example-jobs").use { jobs ->
    jobs.deliveries().collect { job ->
        process(job.value().toUtf8())
        jobs.ack(job)
    }
}
```

## Errors you may meet

| Error | When |
| --- | --- |
| `ClientException.ResourceExists` | The queue name is taken in your tenant |
| `ClientException.Broker` with `INVALID_REQUEST` | A setting is out of range, or would make a dead-letter loop |
| `ClientException.Broker` with `MESSAGE_NOT_HELD` | You acked a message inside a transaction that is not in flight with your subscription, or another transaction holds it. Abort |
| `ClientException.Broker` with `UNAUTHORIZED` | Your roles do not allow it |

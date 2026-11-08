# Transactions and exactly-once

Delivery over a network is at least once: if an answer is lost, the sender asks again. Lotse turns that into **exactly once** in two steps. Idempotent producers remove duplicates on the way in. Transactions make a read, a change and a write happen all together or not at all.

## Idempotent producers

Every producer has an id from the cluster and numbers its batches. A leader that sees a number twice answers with the earlier result and writes nothing. The Java and Rust clients do this by default, so a retried `produce` does not duplicate. A producer with a name (`producerName`) keeps its identity across restarts, and a second instance with the same name fences the first ([ADR 0028](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0028-idempotent-producers.md), [ADR 0071](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0071-named-idempotent-producers.md)).

When the outcome of a write is unknown and the client cannot find out, it throws `ClientException.Unknown`. That is the one case where you decide.

## Consume, transform, produce

The example reads orders as a consumer group member and writes one invoice per order. Each poll's invoices and the group's offsets commit in one transaction. If the process stops in between, neither the invoices nor the new offsets exist, and the next instance reads the same orders again.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/ExactlyOnceExample.java}}
```

Points to note:

- **One client per transactional id.** `transactionalId("example-billing-1")` names the client's identity. Starting a second client with the same id fences the first.
- **Turn auto-commit off** for the group (`autoCommit(false)`). The offsets belong to the transaction: `txn.sendOffsets(group)`.
- **Call `group.poll` often.** It sends the group heartbeat.
- `commit()` ends the transaction. `close()` without `commit()` or `abort()` leaves the decision to the coordinator, which aborts it, and the client's next transaction starts with a fresh producer id; call `abort()` when you want it gone at once. On `ClientException` the example calls `group.rewind()` so the next poll reads from the committed offsets again.
- `ClientException.MustAbort` says the transaction can no longer commit; abort it.

## The same in Kotlin

`transaction { }` commits when its block returns and aborts on an exception or cancellation ([ADR 0080](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0080-kotlin-layer-on-dispatchers-io.md)).

```kotlin
{{#include ../../../../sdk/kotlin/src/examples/kotlin/dev/lotse/examples/KotlinExample.kt}}
```

## What a transaction can hold

Inside one transaction you can:

- `produce` to any number of streams and queues;
- `publish` through an exchange (the client routes, and the message reaches all its queues or none);
- `sendOffsets(group)` to commit a consumer group's offsets;
- `queue.ackIn(txn, delivery)` to acknowledge queue messages.

A transaction that is neither committed nor aborted is aborted by the coordinator after `txnTimeout`.

## Consume from a queue, produce, exactly once

`ackIn` ties a queue acknowledgement to the transaction. The commit settles the message; an abort gives it back to the queue.

```java
// Not from the compiled examples.
Delivery job = jobs.poll(Duration.ofSeconds(5));
try (Transaction txn = client.beginTransaction()) {
    txn.produce("results", 0, OutMessage.value(compute(job.value().toUtf8())));
    jobs.ackIn(txn, job);
    txn.commit();
}
```

Rules to know ([ADR 0075](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0075-queue-acks-in-transactions.md)):

- Ack on the connection the message came on. A message that is not in flight with your subscription gets `MESSAGE_NOT_HELD`; abort.
- Of two transactions that ack the same message, the first in the log holds it; the other gets `MESSAGE_NOT_HELD`.
- A held message is not ready: purge leaves it, and expiry and the delivery limit do not touch it.

## What readers see

A fetch is **read_committed** by default: records of open and aborted transactions are not returned. A reader never sees a half-written transaction. Messages in a queue that a transaction wrote become deliverable when it commits.

## Limits of the guarantee

- Exactly once holds for effects inside Lotse: streams, queues, offsets, acks. A call to a database or an e-mail service inside your code is outside it; make that call idempotent.
- A plain `publish` outside a transaction that ends in `Unknown` is not retried by the client (see [Exchanges and routing](exchanges.md)).
- A stream consumer that commits offsets outside a transaction gives at-least-once.

See also [ADR 0029](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0029-transactions.md) and [ADR 0057](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0057-partitions-verify-transactions-with-the-coordinator.md).

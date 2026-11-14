# Exactly-once recipes

Lotse gives exactly-once effects from three pieces that build on each other. Use the smallest one that covers your case.

| Need | Use |
| --- | --- |
| A retry must not write a message twice | An idempotent producer |
| Several writes must appear together or not at all | A transaction |
| Read, transform and write must happen once as a unit | A transaction that also commits the consumer's position |

## 1. Idempotent producer

Idempotent produce is on by default in both clients. The client gets a producer id from the coordinator and stamps each batch with a sequence per partition. If a connection breaks after sending, or a request times out, it sends the batch again with the same sequence. The leader checks the sequence against the whole log and answers with the offset of the batch it already has ([ADR 0028](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0028-idempotent-producers.md)).

What you must handle:

- **Unknown outcome.** When retrying runs out of `retry_for` (Rust) or `retryFor` (Java) with the result unknown, the call fails with `ClientError::Unknown` or `ClientException.Unknown`. The client drops its producer id. Read the partition to see whether the batch is there, or produce again and accept a possible duplicate.
- **Restarts.** Without a name, each client instance gets a fresh producer id, so a retry from before a restart is a different producer. Set a name (`producer_name` in Rust, `producerName` in Java) and the coordinator hands out the same id with a raised epoch on every start. The earlier instance is fenced: its retries are refused instead of written after the new instance's batches ([ADR 0071](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0071-named-idempotent-producers.md)). Names share a namespace with transactional ids, and one client instance may use a name at a time.
- **Idle producers.** A partition forgets a producer that has written nothing to it for a day (`[producers] expiration_ms`); a retry after that is a new batch. It never forgets a live producer to make room. While a partition knows `[producers] max` producers (10,000), it refuses new ones with `PRODUCER_LIMIT`, and the client retries until one is forgotten or `retry_for` runs out ([ADR 0110](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0110-idle-producers-expire-and-a-full-partition-refuses-new-ones.md)). Many short-lived producers without a name fill that table; give long-running ones a name.
- **Publishing through an exchange.** A node writes each routed queue with an idempotent producer of its own. If its answer is lost, you get `Unknown`, not a second publish ([ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md)).

## 2. Transactions

Set a transactional id. One client instance per id: starting a transaction fences any older instance with the same id.

A transaction can write to several partitions and queues atomically. A read_committed reader (the default for `fetch`) sees all of a committed transaction and nothing of an aborted one ([ADR 0029](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0029-transactions.md)).

```rust,ignore
let mut txn = client.begin_transaction().await?;
txn.produce("payments", 0, payment_batch).await?;
txn.produce("audit", 1, audit_batch).await?;
txn.commit().await?;       // or txn.abort().await?
```

```java
try (Transaction txn = client.beginTransaction()) {
    txn.produce("payments", 0, OutMessage.value("paid"));
    txn.produce("audit", 1, OutMessage.value("payment recorded"));
    txn.commit();
}
```

```kotlin
lotse.transaction {
    produce("payments", 0, message("paid"))
    produce("audit", 1, message("payment recorded"))
}   // commits when the block returns; aborts on an exception or cancellation
```

Rules to keep in mind:

- A transaction that fails part way reports `MustAbort` (Rust) and can only be aborted. Rust's `Transaction::publish` does this when some queues hold a message and others do not.
- A transaction left open times out after `txn_timeout` (60 s by default) and the coordinator aborts it. The next transaction of the same client starts with a fresh producer id and aborts it sooner.
- An open transaction holds back the last stable offset of its partitions, so read_committed readers wait behind it. Keep transactions short.
- A commit that fails with `Unknown` may have committed. The outcome stays unknown to the caller; check the data before you repeat the work.

## 3. Consume, transform, produce

### From a stream

A consumer group member reads records; the transaction writes the results and commits the member's positions together. A crash between the two leaves neither.

1. Join the group with `auto_commit` off. Offsets then go with each transaction, not on a timer.
2. `poll` for records. It also heartbeats, so call it often.
3. Begin a transaction, produce the results, call `send_offsets` (Rust) or `sendOffsets` (Java) with the group member, commit.
4. If the transaction fails, call `rewind()`. The member reads the same records again from the group's committed offsets.

```rust,ignore
let cfg = GroupConfig { auto_commit: false, ..GroupConfig::default() };
let mut group = client.join_group("billing", &["orders"], cfg).await?;
loop {
    let orders = group.poll(Duration::from_millis(500)).await?;
    if orders.is_empty() { continue; }
    let mut txn = client.begin_transaction().await?;
    for order in &orders {
        txn.produce("invoices", 0, invoice_batch(order)).await?;
    }
    txn.send_offsets(&group).await?;
    match txn.commit().await {
        Ok(()) => {}
        Err(_) => group.rewind(),
    }
}
```

A member that no longer owns a partition is fenced: `send_offsets` fails and the transaction can only abort. That is the point: the new owner reads from the last committed offset, and nothing is written twice.

This is the complete Java version, compiled by `gradlew check`:

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/ExactlyOnceExample.java}}
```

And the same in Kotlin with `transaction { }`:

```kotlin
{{#include ../../../../sdk/kotlin/src/examples/kotlin/dev/lotse/examples/KotlinExample.kt}}
```

### From a queue

A consumer acknowledges messages inside the transaction ([ADR 0075](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0075-queue-acks-in-transactions.md)). If it commits, the messages are settled; if it aborts, they are delivered again.

```rust,ignore
let mut jobs = client.subscribe("jobs", 16).await?;
while let Some(job) = jobs.next(Duration::from_secs(5)).await? {
    let mut txn = client.begin_transaction().await?;
    txn.produce("results", 0, result_batch(&job)).await?;
    jobs.ack_in(&mut txn, &job).await?;      // adds the queue to the transaction first
    txn.commit().await?;
}
```

In Java, `queue.ackIn(txn, delivery)`; in Kotlin, `ack(queue, delivery)` inside `transaction { }`.

- Acknowledge on the subscription that holds the message. The ack goes on the connection the message came on.
- Of two transactions that try to hold one message, the first in the log holds it and the other gets `MESSAGE_NOT_HELD`. Any error from `ack_in` leaves the transaction to abort.
- Acks alone do not hold the last stable offset back.
- Messages that stay unsettled past the queue's ack timeout are delivered again; keep the transaction shorter than that.

## What exactly-once does not cover

The guarantee holds for effects inside Lotse: the log, the queue ack index and the group offsets. A side effect outside it, such as a payment or an e-mail, can run twice if your process crashes after the effect and before the commit. Make such effects idempotent on their own side, with a key derived from the stream, partition and offset or from the queue message.

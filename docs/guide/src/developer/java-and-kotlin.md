# Java and Kotlin

The Java client is in `sdk/java` (Java 17, Gradle). It follows `lotse-client` rule for rule; the Kotlin layer in `sdk/kotlin` wraps it for coroutines. The design is in [ADR 0056](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0056-client-sdks-for-the-jvm.md) and [ADR 0080](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0080-kotlin-layer-on-dispatchers-io.md). The artifact group is `dev.lotse`, version 0.0.1; nothing is published to a repository yet, so build it from the source tree.

## Packages

| Package | What |
| --- | --- |
| `dev.lotse.proto` | Field codec, frames, every client-facing request and response, error codes. |
| `dev.lotse.record` | Record batch, `BatchBuilder`, in-place edits, control batches. |
| `dev.lotse.net` | Connections, TLS 1.3 (`TlsConfig`), pipelining, pushes, session renewal. |
| `dev.lotse.auth` | `Credentials`: SCRAM-SHA-512 and OAUTHBEARER. |
| `dev.lotse.client` | `LotseClient`, `QueueConsumer`, `GroupConsumer`, `Transaction` and the admin calls. |

The client has no runtime dependencies. Reading and writing zstd and lz4 batches needs zstd-jni and lz4-java, which are optional; their absence is named in the error ([ADR 0061](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0061-optional-codecs-in-the-java-sdk.md)).

## Connecting

`ClientConfig.builder(bootstrap...)` builds a configuration. There is no transport default: call `tls(TlsConfig.fromFiles(ca, cert, key))` or `plaintextOnLoopback()`.

```java
var config = ClientConfig.builder("127.0.0.1:9092", "127.0.0.1:9093")
        .plaintextOnLoopback()
        .build();
try (var client = new LotseClient(config)) {
    // ...
}
```

Other builder methods: `requestTimeout`, `retryFor`, `backoff`, `produceTimeout`, `idempotent`, `txnTimeout`, `producerName`, `transactionalId`, `metadataMaxAge`, `tenant`, `credentials`. They mean what the Rust fields mean (see [Rust client](rust-client.md)).

## Streams

`LotseClient` offers `createStream`, `produce`, `fetch`, `fetchUncommitted` and `listOffset`. Every call has an `...Async` form that returns a `CompletableFuture`. Two async produces to one partition run one after the other in no promised order: await the first if order matters.

- You choose the partition; the client does not hash keys.
- `u64` values such as offsets travel in a `long` as their bit pattern. Print and compare them with `Long.toUnsignedString` and the other `Long.*Unsigned` methods.
- A message without a timestamp is stamped with the time of the call. Retention judges data by producers' timestamps, so a message stamped 0 would be deleted at the next pass.
- `ClientException.Unknown` is the one error that needs a decision: after `retryFor` the client cannot tell whether the batch was written. Read the partition, or produce again and accept a possible duplicate.
- `ClientException.OffsetOutOfRange` carries `logStart()`.

The example below creates a stream, writes ten messages and reads them back. It is compiled by `gradlew check`.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/StreamExample.java}}
```

## Queues

`createQueue(name, QueueOptions)` and `subscribe(queue, SubscribeOptions)` return a `QueueConsumer`. `poll(Duration)` returns a `Delivery` or `null`; settle with `ack`, `nack(delivery, requeue)` or `nackAfter(delivery, delay)`. `isStandby()` is true while a subscription waits behind another on a single active consumer queue. The consumer resubscribes by itself after a leader change.

`QueueOptions.builder()` sets `maxDeliveries`, `deadLetterQueue`, `ackTimeout` and the other queue settings. A retired message arrives in the dead-letter queue with `Delivery.deadLetter()` describing where it came from and why.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/QueueExample.java}}
```

## Publishing through an exchange

`declareExchange`, `bind`, `bindHeaders` and `unbind` set up routing; `LotseClient.publish(exchange, routingKey, message, mandatory)` goes to a node, which routes it ([ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md)). Inside a transaction `Transaction.publish` routes in the client, checked against `sdk/conformance/routing.txt`.

## Transactions and groups

`beginTransaction()` returns a `Transaction`; set `transactionalId` first. It has `produce`, `produceBatch`, `publish`, `sendOffsets(GroupConsumer)`, `commit` and `abort`; `QueueConsumer.ackIn(txn, delivery)` acknowledges within it. `close()` without a decision ends the handle; the coordinator aborts what is left when the next transaction starts with a fresh producer id.

`joinGroup(group, streams, GroupConfig)` and `joinGroupMatching(group, pattern, config)` return a `GroupConsumer`. `poll(Duration)` heartbeats too, so call it often. `commit()`, `rewind()`, `assignment()`, `positions()` and `close()` match the Rust client. [Exactly-once recipes](exactly-once.md) shows the combination.

```java
{{#include ../../../../sdk/java/src/examples/java/dev/lotse/examples/ExactlyOnceExample.java}}
```

## Admin

The admin calls are methods of `LotseClient`: `delete`, `deleteIfEmpty`, `purge`, `requeue`, `alterConfigs`, `describeConfigs`, `setUserPassword`, `deleteUser`, `describeUsers`, `createTenant`, `deleteTenant`, `describeTenants`, `alterRoleBindings`, `describeRoleBindings`, `whoAmI`, `revokeSessions`, `addNode`, `removeNode`, `describeNodes`, `moveLeader`, `createPartitions` and `nodeMetrics`.

## Kotlin

`dev.lotse.kotlin` wraps the Java client: every call is a `suspend` function that runs the blocking Java call on `Dispatchers.IO`.

- `lotse(vararg bootstrap) { ... }` builds a `Lotse` (the block configures `ClientConfig.Builder`). Builders `groupConfig { }` and `queueOptions { }` do the same for their types, and `message(value, key, headers)` makes an `OutMessage`.
- `Lotse.records(stream, partition, from)` and `LotseQueue.deliveries()` and `LotseGroup.records()` are `Flow`s.
- `Lotse.transaction { }` begins a transaction, runs the block with a `LotseTransaction` receiver, commits when the block returns, and aborts on an exception or cancellation.
- `LotseTransaction`, `LotseQueue` and `LotseGroup` expose the Java object as `java`.

```kotlin
{{#include ../../../../sdk/kotlin/src/examples/kotlin/dev/lotse/examples/KotlinExample.kt}}
```

## Running the examples

The examples read `LOTSE_BOOTSTRAP` (default `127.0.0.1:9092`). The Java examples use plain TCP on loopback unless `LOTSE_CA` is set, with optional `LOTSE_CERT`, `LOTSE_KEY`, `LOTSE_USER` and `LOTSE_PASSWORD`. `make dev` starts a local three-node cluster.

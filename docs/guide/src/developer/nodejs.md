# Node.js

The Node.js client is in `sdk/node` (TypeScript, Node.js 20 and later). It follows `lotse-client` and the Java client rule for rule. The design is in [ADR 0083](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0083-client-sdks-for-csharp-and-nodejs.md); the codecs are in [ADR 0085](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0085-codecs-in-the-nodejs-sdk.md). The package is named `@lotse/client` and is private: nothing is published to npm yet, so build it from the source tree. It has no runtime dependencies. `sdk/node/README.md` lists every rule in full.

## Build and test

```sh
make sdk-node-test                                   # cd sdk/node && npm ci && npm test
LOTSE_BIN=/path/to/lotse make sdk-node-test          # adds the TLS and real-node tests
```

`npm test` compiles to `dist/` and runs `node:test`. Without `LOTSE_BIN` the tests that need a `lotse` executable are skipped with a message. `make sdk-test` runs this after the Java and Kotlin tests.

## Modules

| Module | What |
| --- | --- |
| `src/proto` | Field codec, frames, every client-facing request and response, error codes. |
| `src/record` | Record batch: parse, build, in-place edits, control batches, CRC32C, varints, the codecs. |
| `src/net` | `Connection`, `ConnectionPool`, `TlsConfig`, `Address`, the errors. |
| `src/auth` | `scramCredentials`, `oauthBearerCredentials`, `ScramCredential`. |
| `src/client` | `LotseClient`, `QueueConsumer`, `GroupConsumer`, `Transaction` and the admin calls. |

Values of 64 bits are `bigint`. A `u64` is held as its signed 64-bit bit pattern, as in Java, so "no offset" is `-1n`. Compare, add and print such values with `compareU64`, `addU64Exact` and `u64ToString`, not with `<` or `String`.

## Connecting

`new LotseClient(options)` takes `bootstrap` and a transport. There is no transport default: give `tls` or `plaintextOnLoopback: true`, or the client refuses with `InsecureTransportError`, which is never retried.

```ts
import { LotseClient, TlsConfig, scramCredentials } from "@lotse/client";

const client = new LotseClient({
  bootstrap: ["10.0.0.1:9092", "10.0.0.2:9092"],
  tls: await TlsConfig.fromFiles("ca.pem", "alice.pem", "alice.key"),   // the certificate is optional
  credentials: scramCredentials("bob", "secret"),                        // optional
});
// ... client.close() when done
```

Durations are milliseconds: `requestTimeoutMs` 10 000, `retryForMs` 30 000, `backoffMs` 50, `produceTimeoutMs` 5 000, `txnTimeoutMs` 60 000, `metadataMaxAgeMs` 30 000. Other options: `idempotent`, `producerName`, `transactionalId`, `tenant`, `zone`. They mean what the Rust fields mean (see [Rust client](rust-client.md)).

## Streams

```ts
await client.createStream("orders", 2, 3, { "retention.ms": "86400000" });   // 2 partitions, 3 replicas each
const { baseOffset } = await client.produce("orders", 0, { key: "k1", value: "hello" });

for await (const record of client.readRecords("orders", 0, { from: baseOffset, untilEnd: true })) {
  console.log(record.offset, Buffer.from(record.value ?? []).toString());
}
const end = await client.listOffset("orders", 0, OffsetAt.LATEST);
```

- You choose the partition; the client does not hash keys.
- `fetch` is read_committed. Continue from `nextOffset(from)`, not from the number of records. `readRecords` does this for you and yields committed records.
- Keys, values and header values are `string` (UTF-8) or `Uint8Array`; `null` and empty differ.
- A message without a timestamp gets the time of the call.
- `UnknownOutcomeError` is the one error that needs a decision: after `retryForMs` the client cannot tell whether the batch was written. Read the partition, or produce again and accept a possible duplicate.
- Calls take `{ signal }` (an `AbortSignal`) last. A produce given up while its answer may still come rejects with `UnknownOutcomeError` and drops the producer id.
- All errors extend `ClientError`; `OffsetOutOfRangeError` says where the partition starts now.

## Queues

`createQueue(name, options)` and `subscribe(queue, options)` return a `QueueConsumer`. `next()` waits for a message, `poll(waitMs)` resolves `null` when none came, and the consumer is an async iterable. Settle with `ack`, `nack(delivery, requeue)` or `nackAfter(delivery, delayMs)`.

```ts
await client.createQueue("jobs", { ackTimeoutMs: 30_000, maxDeliveries: 5, deadLetterQueue: "jobs-dead" });

const consumer = await client.subscribe("jobs", { credit: 16 });
for await (const d of consumer) {
  try {
    await handle(d.text());
    await consumer.ack(d);
  } catch {
    await consumer.nackAfter(d, 5_000);       // back in the queue after 5 s
  }
}
consumer.close();
```

- Credit defaults to 64 messages. The consumer subscribes again by itself after a leader change, as the same consumer, and keeps the messages it received (ADR 0109); after an ack timeout they may arrive twice, with a higher `deliveryCount`.
- Leaving a `for await` loop does not close the consumer.
- `consumer.isStandby` is true while a subscription waits behind another on a single active consumer queue.
- `delivery.deadLetter()` says where a retired message came from and why.
- `close()` returns the messages received and not handed out, with one requeueing `Nack`; it does not wait for answers.

## Publishing through an exchange

`declareExchange(name, ExchangeType.TOPIC)`, `bind`, `bindHeaders` and `unbind` set up routing. `publish(exchange, routingKey, message, { mandatory })` goes to a node, which routes it ([ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md)); inside a transaction `txn.publish` routes in the client, checked against `sdk/conformance/routing.txt`.

```ts
await client.declareExchange("orders", ExchangeType.TOPIC);
await client.bind("orders", "orders-eu", "*.eu");
const { routed } = await client.publish("orders", "orders.eu", { value: "m" }, { mandatory: true });
```

If the node's answer is lost the call rejects with `UnknownOutcomeError` and is not sent again, since the node may have written the message.

## Transactions and groups

Set `transactionalId` first. `beginTransaction()` returns a `Transaction` with `produce`, `produceBatch`, `publish`, `sendOffsets(group)`, `commit` and `abort`; `consumer.ackIn(txn, delivery)` acknowledges within it. With `await using`, leaving the scope aborts a transaction that was not ended; if the abort does not get through, the transaction is abandoned and the error that left the scope is not hidden.

```ts
{
  await using txn = await client.beginTransaction();
  await txn.produce("ledger", 0, { key: "acct-7", value: "debit 40" });
  await txn.publish("events", "ledger.debit", { value: "debit 40" });
  await txn.commit();
}
```

`await using` needs TypeScript 5.2 or later (the SDK builds with 5.9). Without it, call `abort()` yourself in a `finally` block.

`joinGroup(group, streams, config)` and `joinGroupMatching(group, pattern, config)` return a `GroupConsumer`. `poll(waitMs, { signal })` heartbeats when one is due, so call it more often than the session timeout; `for await (const { stream, partition, record } of group)` polls without end. `commit()`, `rewind()` and `close()` match the Rust client.

```ts
const group = await client.joinGroup("billing", ["orders"], { autoCommit: true, startFrom: OffsetAt.EARLIEST });
for await (const { record } of group) {
  await handle(record);
}
await group.close();                          // commits the positions and leaves
```

[Exactly-once recipes](exactly-once.md) shows the rules. Runnable programs are in [`sdk/node/examples`](https://github.com/pierrejochem/lotse/tree/main/sdk/node/examples).

## Admin

The admin calls are methods of `LotseClient`: `delete`, `deleteIfEmpty`, `purge`, `requeue`, `alterConfigs`, `describeConfigs`, `setUserPassword`, `deleteUser`, `describeUsers`, `createTenant`, `deleteTenant`, `describeTenants`, `alterRoleBindings`, `describeRoleBindings`, `whoAmI`, `revokeSessions`, `addNode`, `removeNode`, `describeNodes`, `moveLeader`, `createPartitions` and `nodeMetrics`. Each needs the role the broker asks for. A create whose answer was lost is retried; if the first attempt did create the resource, the call rejects with `ResourceExistsError` and the id.

## Compressed batches

zstd and lz4 are read and written without a package to install. Set `attributes.compression` on a `BatchBuilder`; nothing compresses by default.

- lz4 is implemented in the SDK and works on every supported Node.js.
- zstd is `node:zlib`'s, which has it from Node.js 22.15 and 23.8, and in 24. On Node.js 20 and earlier 22 versions, reading or building a zstd batch rejects with `RecordFormatError` of kind `UNSUPPORTED_COMPRESSION`. Give the SDK a codec of your own with `registerCodec(Compression.ZSTD, { compress, decompress })` to read zstd there.
- A decompressed block may hold at most 16 MiB.

## Differences from Java

- Promises and async iterators replace `CompletableFuture` and blocking calls; there is no blocking form.
- `AbortSignal` replaces thread interruption. A call given up after it was sent rejects with `UnknownOutcomeError` where the outcome may be unknown.
- Values of 64 bits are `bigint`.
- The SDK has no runtime dependencies at all: lz4 is its own, zstd is Node's. Java needs two optional libraries.

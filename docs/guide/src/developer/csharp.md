# C#

The C# client is in `sdk/csharp` (.NET 10). It follows `lotse-client` and the Java client rule for rule. The design is in [ADR 0083](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0083-client-sdks-for-csharp-and-nodejs.md); the optional codecs are in [ADR 0084](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0084-codecs-in-the-csharp-sdk.md). Nothing is published to NuGet yet, so build it from the source tree. `sdk/csharp/README.md` lists every rule in full.

## Build and test

`global.json` pins the SDK to 10.0.403 and lets it roll forward to a later feature band. From the repository root:

```sh
make sdk-csharp-test                                  # cd sdk/csharp && dotnet test Lotse.sln
LOTSE_BIN=/path/to/lotse make sdk-csharp-test         # adds the TLS and real-node tests
```

Without `LOTSE_BIN` the tests that need a `lotse` executable are skipped with a message. `make sdk-test` runs this after the Java and Kotlin tests.

## Projects and namespaces

| Project | What |
| --- | --- |
| `src/Lotse.Client` | Everything below. No dependencies beyond the base class library. |
| `src/Lotse.Client.Codecs` | zstd and LZ4 for compressed batches, optional. |

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response, error codes. |
| `Lotse.Record` | Record batch parser and builder, in-place edits, control batches. |
| `Lotse.Net` | `Connection`, `ConnectionPool`, `ConnectionOptions`, `TlsConfig`, `Address`, the exceptions. |
| `Lotse.Auth` | `Credentials` (SCRAM-SHA-512 and OAUTHBEARER), `ScramCredential`. |
| `Lotse.Client` | `LotseClient`, `QueueConsumer`, `GroupConsumer`, `Transaction` and the admin calls. |

Unsigned wire types keep their width: offsets are `ulong`, so no unsigned-compare helpers are needed as in Java.

## Connecting

`ClientConfig.CreateBuilder(bootstrap...)` builds a configuration. There is no transport default: call `Tls(TlsConfig.FromFiles(ca, cert, key))` or `PlaintextOnLoopback()`, or the client refuses with `InsecureTransportException` before it sends a byte.

```csharp
ClientConfig config = ClientConfig.CreateBuilder("127.0.0.1:9092", "127.0.0.1:9093")
    .PlaintextOnLoopback()
    .Build();
using var client = new LotseClient(config);
```

Other builder methods include `WithCredentials(Credentials.Scram(user, password))`, `WithTransactionalId`, `WithProducerName`, `WithIdempotence`, `WithTenant`, `WithZone`, `WithRetryFor` and `WithMetadataMaxAge`. Defaults are the Rust client's: 10 s per request, 30 s of retrying, 50 ms between attempts, 5 s for the leader's quorum (see [Rust client](rust-client.md)).

## Streams

```csharp
await client.CreateStreamAsync("orders", partitions: 2, replication: 3);

ProduceResult sent = await client.ProduceAsync("orders", 0,
    OutMessage.FromValue("order-1").WithKey("k1"));

Fetched fetched = await client.FetchAsync("orders", 0, sent.BaseOffset, TimeSpan.FromMilliseconds(500));
foreach (BatchRecord record in fetched.CommittedMessages())
{
    Console.WriteLine($"{record.Offset} {record.Value!.ToUtf8()}");
}
ulong next = fetched.NextOffset(sent.BaseOffset);
```

- You choose the partition; the client does not hash keys.
- `FetchAsync` is read_committed. Continue from `NextOffset`, not from the number of records: a response may hold only control batches.
- A message without a timestamp gets the time of the call. A message stamped 0 would be deleted at the next retention pass.
- `OutcomeUnknownException` is the one error that needs a decision: after `RetryFor` the client cannot tell whether the batch was written. Read the partition, or produce again and accept a possible duplicate.
- `OffsetOutOfRangeException` says where the partition starts now.
- Every call takes a `CancellationToken` and throws `OperationCanceledException`. A produce cancelled while its outcome is unknown drops the producer id, as an unknown outcome does.

## Queues

`CreateQueueAsync(name, QueueOptions)` and `SubscribeAsync(queue, SubscribeOptions)` return a `QueueConsumer`. `NextAsync` waits for a message, `PollAsync(wait)` returns `null` when none came, and `DeliveriesAsync()` is an `IAsyncEnumerable<Delivery>`. Settle with `AckAsync`, `NackAsync(delivery, requeue)` or `NackAfterAsync(delivery, delay)`.

```csharp
await client.CreateQueueAsync("jobs", QueueOptions.CreateBuilder()
    .WithAckTimeout(TimeSpan.FromSeconds(30))
    .WithMaxDeliveries(5)
    .WithDeadLetterQueue("jobs-dead")
    .Build());

await using QueueConsumer consumer = await client.SubscribeAsync("jobs", SubscribeOptions.WithCredit(16));
await foreach (Delivery delivery in consumer.DeliveriesAsync(cancellationToken))
{
    try
    {
        Handle(delivery.Value!.ToUtf8());
        await consumer.AckAsync(delivery);
    }
    catch (TransientException)
    {
        await consumer.NackAfterAsync(delivery, TimeSpan.FromSeconds(10));
    }
}
```

- Credit defaults to 64 messages. The consumer resubscribes by itself after a leader change; messages it held may arrive twice, with a higher `DeliveryCount`.
- `IsStandby` is true while a subscription waits behind another on a single active consumer queue.
- `Delivery.DeadLetter` says where a retired message came from and why.
- `DisposeAsync` sends `Unsubscribe` and then hands back what the consumer holds and has not handed out. The Java consumer leaves those messages with the broker until the ack timeout.

## Publishing through an exchange

`DeclareExchangeAsync`, `BindAsync`, `BindHeadersAsync` and `UnbindAsync` set up routing. `PublishAsync(exchange, routingKey, message, mandatory)` goes to a node, which routes it ([ADR 0079](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0079-publish-routed-by-the-node.md)); inside a transaction `Transaction.PublishAsync` routes in the client, checked against `sdk/conformance/routing.txt`.

```csharp
await client.DeclareExchangeAsync("events", Messages.ExchangeType.Topic);
await client.BindAsync("events", "audit", "order.#");
Published published = await client.PublishAsync("events", "order.paid", OutMessage.FromValue("o-1"), mandatory: true);
```

If the node's answer is lost the call throws `OutcomeUnknownException` and is not sent again, since the node may have written the message.

## Transactions and groups

Set `WithTransactionalId` first. `BeginTransactionAsync()` returns a `Transaction` with `ProduceAsync`, `PublishAsync`, `SendOffsetsAsync(GroupConsumer)`, `CommitAsync` and `AbortAsync`; `QueueConsumer.AckInAsync(txn, delivery)` acknowledges within it. `Transaction` is `IAsyncDisposable` only: `await using` aborts a transaction that was neither committed nor aborted, also when the block ends with an exception.

`JoinGroupAsync(group, streams, GroupConfig)` and `JoinGroupMatchingAsync(group, pattern, config)` return a `GroupConsumer`. `PollAsync` heartbeats too, so call it more often than the session timeout; `RecordsAsync()` repeats it. `CommitAsync()`, `Rewind()` and `DisposeAsync()` match the Rust client.

```csharp
GroupConfig groupConfig = GroupConfig.CreateBuilder().WithAutoCommit(false).Build();
await using GroupConsumer group = await client.JoinGroupAsync("pipeline", new[] { "orders" }, groupConfig);
while (!stop.IsCancellationRequested)
{
    IReadOnlyList<GroupRecord> records = await group.PollAsync(TimeSpan.FromMilliseconds(500), stop);
    if (records.Count == 0)
    {
        continue;
    }
    await using Transaction txn = await client.BeginTransactionAsync(stop);
    foreach (GroupRecord r in records)
    {
        await txn.ProduceAsync("shipments", 0, Transform(r.Record), stop);
    }
    await txn.SendOffsetsAsync(group, stop);
    await txn.CommitAsync(stop);
}
```

After an exception that aborted the transaction, call `group.Rewind()` to read the records again. [Exactly-once recipes](exactly-once.md) shows the rules. Runnable programs are in [`sdk/csharp/examples`](https://github.com/pierrejochem/lotse/tree/main/sdk/csharp/examples).

## Admin

The admin calls are methods of `LotseClient`: `DeleteAsync`, `DeleteIfEmptyAsync`, `PurgeAsync`, `RequeueAsync`, `AlterConfigsAsync`, `DescribeConfigsAsync`, `SetUserPasswordAsync`, `DeleteUserAsync`, `DescribeUsersAsync`, `CreateTenantAsync`, `DeleteTenantAsync`, `DescribeTenantsAsync`, `AlterRoleBindingsAsync`, `DescribeRoleBindingsAsync`, `WhoAmIAsync`, `RevokeSessionsAsync`, `AddNodeAsync`, `RemoveNodeAsync`, `DescribeNodesAsync`, `MoveLeaderAsync`, `CreatePartitionsAsync` and `NodeMetricsAsync`. Each needs the role the broker asks for. A create whose answer was lost is retried; if the first attempt did create the resource, the call throws `ResourceExistsException` with the id.

## Compressed batches

Add the `Lotse.Client.Codecs` package to read and write zstd and LZ4 batches. It brings ZstdSharp.Port and K4os.Compression.LZ4.Streams, and is found by name the first time a compressed batch is read or built. A program that is trimmed or compiled ahead of time calls `LotseCodecs.Register()` once.

Without the package a compressed batch still parses, but reading its records or building one throws `RecordFormatException` of kind `UnsupportedCompression`, naming the package to add. Nothing compresses by default. A decompressed block may hold at most 16 MiB.

## Differences from Java

- `async`/`await` and `CancellationToken` replace `CompletableFuture`; there is no blocking form.
- `ulong` replaces the `long` bit pattern for offsets.
- `Transaction`, `QueueConsumer` and `GroupConsumer` are `IAsyncDisposable`. Closing a queue consumer also returns the messages it received and did not read, which the Java consumer does not.
- The client ends its own side of a connection when the node's certificate expires ([ADR 0068](https://github.com/pierrejochem/lotse/blob/main/docs/adr/0068-connections-end-when-their-certificate-expires.md)); the Java SDK leaves that to the node.
- The codecs are a separate package, found by name, where Java uses capabilities of the Gradle module.

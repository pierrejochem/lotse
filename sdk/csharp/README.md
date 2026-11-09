# Lotse client for .NET

A C# client for the Lotse native protocol. It follows the Java SDK rule for rule ([ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md), [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md)). The Rust code is the authority; the golden vectors in [`sdk/conformance`](../conformance) enforce it. The protocol as an implementer needs it is in [`sdk/PROTOCOL.md`](../PROTOCOL.md).

## Status

Phase 6 of 8 is done: the protocol codec and record batch, connections and authentication, the stream client (`LotseClient`) with the optional zstd and LZ4 codecs, the queue consumer (`QueueConsumer`, `CreateQueueAsync`), transactions (`Transaction`), publishing through exchanges, consumer groups (`GroupConsumer`), and now the admin calls.

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response (one codec per message), error codes |
| `Lotse.Record` | Record batch parser and builder, in-place edits (producer, transactional bit, base timestamp), control batches, varints, CRC32C |
| `Lotse.Net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, a read loop, session state), `ConnectionPool` (login, renewal, replacement), `ConnectionOptions`, `TlsConfig`, `Address`, the exceptions |
| `Lotse.Auth` | SCRAM-SHA-512 (`ScramClient`, both ends are tested), OAUTHBEARER messages, `Credentials`, `Authenticator`, `ScramCredential` for `AlterUser` |
| `Lotse.Client` | `LotseClient` (metadata routing, idempotent produce, read_committed fetch, `ListOffsets`, `CreateStream`, `CreateQueue`, `Subscribe`, `BeginTransaction`, `Publish`, `DeclareExchange`, `Bind`, `Unbind`, `JoinGroup`, and the admin calls below), `NodeMetrics`, `Transaction`, `QueueConsumer`, `GroupConsumer`, `GroupConfig`, `GroupRecord`, `Published`, `Delivery`, `DeadLetter`, `QueueOptions`, `SubscribeOptions`, `ClientConfig`, `OutMessage`, `Fetched`, `OffsetAt`, the exceptions |

Two packages, two projects:

| Project | What | Dependencies |
| --- | --- | --- |
| `src/Lotse.Client` (`Lotse.Client`) | Everything above | none beyond the base class library |
| `src/Lotse.Client.Codecs` (`Lotse.Client.Codecs`) | zstd and LZ4 for compressed batches, optional ([ADR 0084](../../docs/adr/0084-codecs-in-the-csharp-sdk.md)) | ZstdSharp.Port 0.8.6, K4os.Compression.LZ4.Streams 1.3.8 |

Not yet: examples.

## Streams

```csharp
using Lotse.Auth;
using Lotse.Client;
using Lotse.Net;

ClientConfig config = ClientConfig.CreateBuilder("127.0.0.1:9092")
    .Tls(TlsConfig.FromFiles("ca.pem"))                     // or .PlaintextOnLoopback() while developing
    .WithCredentials(Credentials.Scram("bob", "secret"))
    .Build();
using var client = new LotseClient(config);

await client.CreateStreamAsync("orders", partitions: 2, replication: 3,
    new Dictionary<string, string> { ["retention.ms"] = "604800000" });

ProduceResult sent = await client.ProduceAsync("orders", 0,
    OutMessage.FromValue("order-1").WithKey("k1").WithHeader("source", "web"));

ulong offset = sent.BaseOffset;
Fetched fetched = await client.FetchAsync("orders", 0, offset, TimeSpan.FromMilliseconds(500));
foreach (BatchRecord record in fetched.CommittedMessages())
{
    Console.WriteLine($"{record.Offset} {record.Value!.ToUtf8()}");
}
offset = fetched.NextOffset(offset);                          // not offset + count: a response may hold only control batches

ulong end = await client.ListOffsetAsync("orders", 0, OffsetAt.Latest);
```

The client follows the Java SDK and `lotse-client` rule for rule ([`sdk/PROTOCOL.md`](../PROTOCOL.md), "Client behaviour: streams"). In short:

- **Routes.** `(stream, partition)` maps to the leader, learnt from `Metadata`, and is used until a request fails; it does not expire by age. A `NOT_LEADER` or `FENCED_LEADER_TERM` answer with a hint moves the route only if the hint's term is not older than the route's and it names another leader; otherwise the route is dropped and metadata is asked again. A name unknown to the first broker is asked of every broker, and the resource with the highest id wins. A partition without a known leader is asked of its voters.
- **Produce is idempotent by default.** The first produce gets a producer id from the coordinator; every batch is stamped with it and a sequence per partition. A batch whose outcome is unknown is sent again with the same bytes until `RetryFor` has passed; then the call throws `OutcomeUnknownException` and the client starts over with a new producer id. So does a produce whose token is cancelled before the outcome is known. Produces to one partition go out one at a time. `WithIdempotence(false)` stamps nothing and never sends an unknown outcome again; errors that prove nothing was written are still retried. `WithProducerName` asks the coordinator under a stable name, which fences an earlier instance ([ADR 0071](../../docs/adr/0071-named-idempotent-producers.md)).
- **Reading.** `FetchAsync` is read_committed: it reads up to the last stable offset and returns whole batches. `Fetched.CommittedBatches()` and `CommittedMessages()` leave out control batches and the batches of aborted transactions. `FetchUncommittedAsync` reads up to the high watermark and is for looking at a log. An offset that retention deleted throws `OffsetOutOfRangeException`, which says where the partition starts now.
- **Timestamps.** A message without a timestamp gets the time of the call, not 0, because retention judges a batch by its producers' timestamps.
- **Tenants.** `WithTenant` names the tenant of created resources and of the producer's transactional id; the default is `default`.
- **Time and failures.** Defaults are the Rust client's: 10 s per request, 30 s of retrying, 50 ms between attempts, 5 s for the leader's quorum. A broker that cannot be reached or does not serve in time gives `GaveUpException`; a login that fails gives `ClientAuthenticationException` at once, or `OutcomeUnknownException` if an earlier attempt of the same call may have been carried out. `InsecureTransportException` is thrown as it is.
- **Cancellation.** Every call takes a `CancellationToken` and throws `OperationCanceledException`.

`CreateStreamAsync` retries a create whose outcome is unknown; if the first attempt did create the stream, the retry throws `ResourceExistsException` with the id.

## Queues

```csharp
await client.CreateQueueAsync("jobs", QueueOptions.CreateBuilder()
    .WithAckTimeout(TimeSpan.FromSeconds(30))
    .WithMaxDeliveries(5)
    .WithDeadLetterQueue("jobs-dead")
    .Build());

await client.ProduceAsync("jobs", 0, OutMessage.FromValue("resize image 17"));   // a queue is partition 0

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
        await consumer.NackAfterAsync(delivery, TimeSpan.FromSeconds(10));       // back to the queue, in 10 s
    }
}
```

`SubscribeAsync` connects to the queue's leader; the leader pushes deliveries within the consumer's credit and the consumer hands them out one at a time. `NextAsync` waits for a message, `PollAsync(wait)` returns `null` when none came, and `DeliveriesAsync()` is the same as an `IAsyncEnumerable<Delivery>`: it ends when the consumer is closed and throws `OperationCanceledException` when its token is cancelled. One task reads at a time; acking, nacking and closing may be done from any task. The rules are those of `lotse-client` and the Java SDK ([`sdk/PROTOCOL.md`](../PROTOCOL.md), "Client behaviour: queues"):

- **Credit.** `SubscribeOptions.DefaultCredit` is 64 messages. After handing out a message, once at least half the credit (rounded up) is used, the consumer sends the messages and the bytes used since the last time. At most the credit's worth of messages wait unprocessed on the client.
- **What is the consumer's.** A push holds a batch as stored; only the offsets in its delivery list are handed out. `Delivery.DeliveryCount` counts this delivery; more than 1 means an earlier delivery was not acked. Counts are a lower bound after a failover (ADR 0031).
- **Ack and nack.** An ack names an offset, goes to the leader as half-open ranges and is retried, also when its answer was lost: settling twice is harmless. It also settles a message that went to someone else meanwhile. `NackAsync(delivery, requeue: false)` discards the message, or dead-letters it if the queue says so. A requeueing nack and `NackAfterAsync(delivery, delay)` go over the subscription's own connection, because the broker takes back only what that connection holds; for a message of an ended subscription they do nothing, since it has gone back already. The delay is rounded down to whole milliseconds and capped at about 49 days (ADR 0077).
- **Ending and subscribing again.** A push with an error (`NOT_LEADER`, `ACK_TIMED_OUT`, `UNKNOWN_RESOURCE`) or a broken connection ends the subscription. The next read subscribes again, after handing out what the consumer already holds, following a leader hint or dropping the route. Those messages may meanwhile have gone to others and then arrive twice.
- **Single active consumer.** On a queue created with `WithSingleActiveConsumer()` (ADR 0076) the consumer always asks to be told when its turn comes. `IsStandby` is true from a standby answer until the activation push or the first message; the activation is not a message. Standbys take over in the order they subscribed, once nothing is out with anyone.
- **Dead letters.** `Delivery.DeadLetter` reads the `x-lotse-dead-letter-*` headers of a message that a queue moved (ADR 0055): reason (`max-deliveries` or `expired`), the source queue, its id, the offset there, the deliveries and the ack floor.
- **Closing.** `Dispose` and `DisposeAsync` send `Unsubscribe` and then one requeueing `Nack` for what the consumer holds and has not handed out, on the subscription's connection and without waiting for an answer; `DisposeAsync` completes when both are written. This also covers what was pushed and not read yet, which the Java and Rust consumers leave with the broker until the ack timeout. Messages already handed out stay with the consumer until acked or until the client's connection closes. A task waiting in `PollAsync` or `NextAsync` gets `ObjectDisposedException`; acking after closing still works.
- **Cancelling a subscribe.** If a token is cancelled while the answer to `Subscribe` is outstanding, the client closes that connection: the broker may have subscribed, and nobody would read the pushes.

`QueueOptions` sets the replication (3 by default), `ack.timeout.ms`, `max.deliveries`, `message.ttl.ms`, the dead-letter queue, `single.active.consumer` (ADR 0076) and `priority.levels` (1 to 10, ADR 0078); `WithSetting(key, value)` passes any other setting through. A message gets its priority from the header `x-lotse-priority` and a delay from `x-lotse-delay-ms` (ADR 0077); both are plain headers on `OutMessage`. Acking within a transaction (ADR 0075) is `AckInAsync`, below.

## Transactions

```csharp
ClientConfig config = ClientConfig.CreateBuilder("127.0.0.1:9092")
    .PlaintextOnLoopback()
    .WithTransactionalId("order-worker-1")                  // lives in the configured tenant (ADR 0062)
    .Build();
using var client = new LotseClient(config);

await using Transaction txn = await client.BeginTransactionAsync();
await txn.ProduceAsync("orders", 0, OutMessage.FromValue("order-1"));
await txn.ProduceAsync("audit", 0, OutMessage.FromValue("order-1 seen"));
await txn.CommitAsync();                                    // both become visible to read_committed readers, or neither
```

`BeginTransactionAsync` asks the coordinator for the producer id of the transactional id, which fences an earlier client with the same id and aborts what it left open. A client runs one transaction at a time: a second call waits for the first to end. The rules are those of `lotse-client` and the Java SDK ([`sdk/PROTOCOL.md`](../PROTOCOL.md), "Client behaviour: transactions"):

- **Adding.** A partition (a queue: partition 0) is added with `AddPartitionsToTxn` once, before the first batch to it. Batches carry the producer id, the epoch, a sequence per partition counted by records, and the transactional bit; a batch whose outcome is unknown is sent again with the same bytes.
- **Only an abort is left.** After any error except `GaveUpException` and `UnknownStreamException`, and after a call that ended without an outcome (a cancelled token, a closed client), every call but `AbortAsync` throws `MustAbortException`, and `CommitAsync` does not ask the coordinator. `IsOpen` stays true until the abort.
- **Ending.** `CommitAsync` returns when the decision is durable. A lost request or a retriable error is asked again; if time runs out the call throws `OutcomeUnknownException`. The next transaction uses the epoch the answer raised; after a failed end it asks for a fresh producer id, which also aborts what is left open. A transaction that added nothing ends without asking the coordinator.
- **Disposing aborts.** `await using` aborts a transaction that was neither committed nor aborted, also when the block ends with an exception or a cancellation. The abort ignores the token of the call that failed, is bounded by `RetryFor`, and does not throw: if it fails, the next transaction asks for a fresh producer id. A `Transaction` is `IAsyncDisposable` only, so a plain `using` does not compile. Not for use by several tasks at once.
- **Publishing in a transaction** is routed by this client (ADR 0032): `txn.PublishAsync(exchange, routingKey, message, mandatory)` reads the exchange's bindings from metadata (kept for `MetadataMaxAge`; a `BindAsync` through this client refreshes them) and produces the message to every queue it goes to. The routing follows `lotse-route` and is checked against `sdk/conformance/routing.txt`. If a write fails after some queues have the message, only an abort is left.
- **Queue acks** (ADR 0075): `consumer.AckInAsync(txn, delivery)` adds the queue, then sends the ack with the transaction's producer id and epoch over the subscription's own connection. A lost answer is sent again on that connection; a connection that broke, or another transaction that holds the message, gives `BrokerErrorException` with `MESSAGE_NOT_HELD`. Any error leaves only an abort. A commit settles the message; an abort gives it back.
- **Offsets** (ADR 0033): `txn.SendOffsetsAsync(groupConsumer)` sends the member's positions with `AddOffsetsToTxn`. They become the group's committed offsets when the transaction commits. A refusal or an unknown outcome leaves only an abort. A transaction that only sent offsets (no partition added) is still ended at the coordinator, also if the answer to `AddOffsetsToTxn` was lost: otherwise the offsets stay pending until the transaction times out and every reader of the group's offsets waits for them. An `AddPartitionsToTxn` that only gave up (`GaveUpException`) does not count: nothing was written, and an `EndTxn` can be refused with `INVALID_TXN_STATE` while the previous transaction is still completing, so such a transaction commits without asking the coordinator. (A call cancelled in the middle of an add still leaves the transaction to be ended, since the coordinator may hold the partition.)

Exactly-once consume-transform-produce from a stream:

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
    await txn.CommitAsync(stop);                            // an exception above aborts; call group.Rewind() to read the records again
}
```

From a queue, `await consumer.AckInAsync(txn, delivery)` replaces `SendOffsetsAsync`.

## Publishing through exchanges

```csharp
await client.DeclareExchangeAsync("events", Messages.ExchangeType.Topic);
await client.BindAsync("events", "audit", "order.#");
await client.BindAsync("events", "billing", "*.paid");

Published published = await client.PublishAsync("events", "order.paid", OutMessage.FromValue("o-1"), mandatory: true);
foreach (RoutedQueue q in published.Routed)                 // in queue name order
{
    Console.WriteLine($"{q.Queue} {q.Offset}");
}
```

Outside a transaction a node routes (`Publish`, ADR 0079): it applies its bindings, checks the producer role on the exchange and on every routed queue, and writes each queue. The call returns once every routed queue holds the message. A message no queue takes is dropped, or refused with `UnroutableException` when `mandatory`. `NotAllWrittenException` names the queues that hold it and why the others do not (`OutcomeUnknownException` for a write whose outcome is unknown). If the node's answer is lost the call throws `OutcomeUnknownException` and is not sent again, since the node may have written it; a token cancelled after the request was sent does the same. A name unknown to the node is looked up once more, since a deleted and declared exchange has a new id. `BindHeadersAsync` binds a headers exchange; the other admin calls are in the next section.

## Admin

```csharp
using Lotse.Proto;

ulong id = await client.CreateStreamAsync("orders", 2, 3, new Dictionary<string, string> { ["retention.ms"] = "604800000" });
await client.AlterConfigsAsync("orders", new Dictionary<string, string> { ["retention.bytes"] = "1073741824" }, reset: new[] { "retention.ms" });
foreach (Messages.ConfigEntry e in await client.DescribeConfigsAsync("orders"))
{
    Console.WriteLine($"{e.Key} = {e.Value} (source {e.Source})");     // Messages.ConfigSource: Default, Dynamic, Static
}
await client.CreatePartitionsAsync("orders", 4);

await client.SetUserPasswordAsync("carol", "secret");                  // only a salted credential leaves the client
await client.CreateTenantAsync("acme");
await client.AlterRoleBindingsAsync(add: new[]
{
    new Messages.RoleBinding(Messages.SubjectKind.User, "carol", Messages.Role.TenantAdmin, "acme", Prefix: string.Empty),
});

ulong purged = await client.PurgeAsync("jobs");                        // ready messages only
ulong moved = await client.RequeueAsync("jobs-dead");                  // dead letters back to the queue they came from
await client.DeleteIfEmptyAsync("jobs-dead");                          // RESOURCE_NOT_EMPTY if it holds anything
```

Every call takes a `CancellationToken` and follows `lotse-client` and the Java SDK ([`sdk/PROTOCOL.md`](../PROTOCOL.md), "Client behaviour: admin"):

| Call | What |
| --- | --- |
| `DeleteAsync`, `DeleteIfEmptyAsync` | Returns the id. A resource that is gone counts as deleted, also when someone else deleted it. The conditional form asks every partition's leader first (ADR 0063). The exchange cache of the client is dropped. |
| `PurgeAsync`, `RequeueAsync` | Go to the queue's leader in rounds (`up_to` 0, then the bound the answer gave, until `more` is false) and return the sum (ADR 0064). Tenant admin. |
| `AlterConfigsAsync`, `DescribeConfigsAsync` | Streams take `retention.ms`, `retention.bytes`, `cleanup.policy`, `delete.retention.ms`; queues `ack.timeout.ms`, `max.deliveries`, `message.ttl.ms`, `dead.letter.queue`, `single.active.consumer`, `priority.levels`. Keys are sent in ordinal order. The node refuses anything else and a key given as both set and reset, with its reason in `BrokerErrorException.BrokerMessage`. Reads come from the metadata leader. |
| `SetUserPasswordAsync`, `DeleteUserAsync`, `DescribeUsersAsync` | SCRAM-SHA-512, 16 random bytes of salt, 4096 iterations. Cluster admin. |
| `CreateTenantAsync`, `DeleteTenantAsync`, `DescribeTenantsAsync` | A tenant that holds resources is not deleted. Describe lists the tenants the client has a role in. |
| `AlterRoleBindingsAsync`, `DescribeRoleBindingsAsync`, `WhoAmIAsync` | Removals are applied before additions. A principal does not see resources outside its binding's prefix: they are `UnknownStreamException` to it, not a refusal. |
| `RevokeSessionsAsync(subject, group)` | Ends the subject's sessions that began before now (ADR 0070). A SCRAM user who still has the password logs in again. Cluster admin. |
| `AddNodeAsync`, `RemoveNodeAsync`, `DescribeNodesAsync` | ADR 0048. |
| `MoveLeaderAsync(name, partition, node)` | Pins a leader to a voter; `null` unpins (ADR 0065). Cluster admin. |
| `NodeMetricsAsync` | Asks each node the metadata names for `DescribeMetrics`; a node that does not answer is a `NodeMetrics` with `Error` (`NoAnswerException` or `BrokerErrorException`), not a failure of the call. |

Names become ids through `Metadata` asked of every known broker, and a name no broker knows is `UnknownStreamException`. A change whose answer was lost is asked again; a refusal after such a lost attempt is `OutcomeUnknownException`, as the earlier attempt may have been carried out. A missing role is a `BrokerErrorException` with `UNAUTHORIZED` at once. Tenants are created by a cluster admin and a duplicate is `RESOURCE_EXISTS`; `CreateStreamAsync` and the other creates use the configured tenant.

Not in this client, as in the Java SDK: creating a queue in a tenant other than the configured one (use a client with `WithTenant`) and the node-internal `SettleVoters`.

## Consumer groups

```csharp
GroupConfig groupConfig = GroupConfig.CreateBuilder()
    .WithSessionTimeout(TimeSpan.FromSeconds(30))
    .WithStartFrom(OffsetAt.Latest)                         // where a partition without a committed offset starts
    .WithInstanceId("worker-1")                             // static membership (ADR 0072), optional
    .Build();
await using GroupConsumer group = await client.JoinGroupAsync("billing", new[] { "orders", "refunds" }, groupConfig);
// or: await client.JoinGroupMatchingAsync("billing", "order.*", groupConfig);   // a regular expression (ADR 0073)

await foreach (GroupRecord r in group.RecordsAsync(cancellationToken))
{
    Handle(r.Stream, r.Partition, r.Record);
}
```

`PollAsync(wait)` returns the records of one owned partition, in offset order, and is the unit `RecordsAsync` repeats. The member follows `lotse-client` and the Java SDK rule for rule ([`sdk/PROTOCOL.md`](../PROTOCOL.md), "Client behaviour: consumer groups"):

- **Heartbeats come from `PollAsync`.** A caller that polls less often than the session timeout loses its partitions. The next heartbeat is due after the coordinator's interval or a third of the session timeout, whichever is shorter, counted from when the last one was sent; after a failure, after one backoff.
- **Rebalancing is cooperative.** A partition that an answer revokes is no longer read from that moment. With auto-commit its position is committed, and the next heartbeat goes out at once and reports it released; only then may another member get it. So no partition has two owners.
- **Lost.** When no heartbeat has succeeded for the session timeout, counted from when the last successful one was sent, every partition is dropped at once without a commit, and the member joins again. A fetch that returns after that hands out nothing. `UNKNOWN_MEMBER_ID` and `FENCED_MEMBER_EPOCH` mean the same.
- **Static members.** A member with an instance id keeps its partitions when it leaves, for the session timeout; the next instance with that id takes them over with a new member id and no rebalance. The earlier instance gets `BrokerErrorException` with `FENCED_INSTANCE_ID` from `PollAsync` and stays out.
- **Positions.** A partition without one asks `OffsetFetch` with `require_stable`, so an offset an open transaction holds waits. `NO_OFFSET` starts at the log start, or where `WithStartFrom` says (`ListOffsets`, ADR 0059). A committed offset that retention deleted goes on from the log start. `Rewind()` forgets the positions, as after an aborted transaction.
- **Committing.** `CommitAsync()` commits the positions; with auto-commit (the default) a revoked partition and, on dispose, every partition is committed. Turn auto-commit off when offsets go with a transaction. `DisposeAsync` commits, leaves with epoch -1, and does not throw.
- **Tenants.** The group id lives in the configured tenant (ADR 0062).

## Connections

```csharp
using Lotse.Auth;
using Lotse.Net;
using Lotse.Proto;

TlsConfig tls = TlsConfig.FromFiles("ca.pem");                       // add the client certificate and key for certificate logins
ConnectionOptions options = ConnectionOptions.CreateBuilder()
    .Tls(tls)
    .WithCredentials(Credentials.Scram("bob", "secret"))
    .Build();

using var pool = new ConnectionPool(options);
Connection connection = await pool.GetAsync(Address.Parse("127.0.0.1:9092"));
Response.WhoAmI who = await connection.CallAsync<Response.WhoAmI>(new Request.WhoAmI());
```

- There is no default transport. A connection is refused with `InsecureTransportException`, and nothing is sent, until the options say `Tls(...)` or `PlaintextOnLoopback()`. Plain TCP is refused for any address that resolves to a non-loopback address.
- TLS is 1.3 only. The node's certificate must be signed by the CA given and valid for the host dialled, which includes IP entries: dial the address the cluster file lists. The system's roots are not used, and no revocation list is fetched; a node ends the connections of revoked client certificates itself ([ADR 0060](../../docs/adr/0060-certificate-renewal-and-revocation.md)).
- Keys are read from PEM. `lotse certs` writes PKCS#8 (`BEGIN PRIVATE KEY`); the traditional RSA and EC forms load too. An encrypted key is refused with the `openssl` command that decrypts it.
- Requests are pipelined: `SendAsync` queues the request and returns a task, responses are matched by correlation id in any order, and requests leave in the order they were sent. A response with the error flag fails the task with `BrokerException` before any typed decoding. A request that times out fails with `RequestTimeoutException` and has an unknown outcome. Cancelling the token stops the wait only.
- `ConnectionException.RequestSent` says whether a failed request may have reached the node. A frame over 16 MiB, a frame that does not decode, or the end of the stream ends the connection and fails everything pending.
- Pushes go to the `IPushListener` registered for the subscription, on the read loop: return quickly. A push that arrives before its subscription is registered is kept (at most 64 subscriptions) and delivered on registration.
- A connection ends when the certificate its node showed expires ([ADR 0068](../../docs/adr/0068-connections-end-when-their-certificate-expires.md)): pending requests fail with "the node's certificate expired", and the pool opens a new connection on its next `GetAsync`. The Java SDK leaves this to the node; this one also ends its own side.

## Sessions

`Connection.SessionState` is `Unlimited` (a certificate login, or no authentication), `Active`, `RenewDue` or `Ended`. The lifetime is taken from the last `SaslAuthenticate` response and counted from before the first request. The pool authenticates again on the same connection at four fifths of the lifetime, one caller at a time, asking OAUTHBEARER credentials for a fresh token each time. An ended session, a broken connection or a failed renewal gets a new connection with a new login. A wrong password gives `AuthenticationException` with `AUTHENTICATION_FAILED`; the node does not say whether the user or the password was wrong. A SCRAM server that cannot prove it knows the credential gives `ScramException` with kind `ServerNotVerified`.

## Types

- .NET 10, nullable reference types, warnings as errors, no runtime dependencies.
- Unsigned wire types keep their width: `u64` is `ulong`, `u32` is `uint`, `u16` is `ushort`, `u8` is `byte`. Timestamps and `i64` fields are `long`.
- Offsets are `ulong`. The offset of "no committed offset" is `Messages.NoOffset` (`ulong.MaxValue`); `Messages.NoOffsetSigned` is the same bits as `-1L`.
- Fixed-width fields are little-endian. The frame length prefix is big-endian (`Frames.WithLengthPrefix`, `Frames.ReadLength`).
- A response with the error flag decodes to `Response.Failed` or `Response.Refused`, whatever request it answers.

## Compressed batches

Add the package `Lotse.Client.Codecs` to read and write zstd and LZ4 batches ([ADR 0084](../../docs/adr/0084-codecs-in-the-csharp-sdk.md), [ADR 0058](../../docs/adr/0058-zstd-and-lz4-compression.md)). It is found by name, once, the first time a compressed batch is read or built, so nothing needs to be called. A program that is trimmed or published ahead of time calls `LotseCodecs.Register()` once, or registers its own `IBlockCodec` per codec with `CompressionCodecs.Register`.

```csharp
var builder = new BatchBuilder(new BatchOptions(new Attributes(Compression.Zstd, false, false, BatchKind.Data), 0, 0, -1));
builder.Push(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), keyBytes, valueBytes);
await client.ProduceBatchAsync("orders", 0, builder.Finish());      // the node keeps and serves the batch compressed
```

Without the package a compressed batch still parses: its header and checksum are checked. Reading its records, or building a compressed batch, throws `RecordFormatException` of kind `UnsupportedCompression` that names the package to add; it is never reported as damage, and `Fetched.CommittedMessages()` passes it on as it is. A library that is found but cannot be loaded is named the same way. Plain batches need no package. Nothing compresses by default; the client sends a batch as it was built.

Limits as in the broker: a record block is one zstd frame or one LZ4 frame; decompression stops one byte past 16 MiB and the batch is refused (`TooLarge`); a block that does not decode is `Malformed`; the builder writes zstd at level 3 and LZ4 as independent 64 KiB blocks, and refuses records over 16 MiB.

## Build and test

```sh
export DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH   # wherever the .NET 10 SDK is
make sdk-csharp-test                                         # or: cd sdk/csharp && dotnet test
LOTSE_BIN=/tmp/cargo-target/debug/lotse dotnet test          # adds the TLS and real-node tests
```

`global.json` pins the SDK. The tests read the vectors from `sdk/conformance` in place and find them from the test binary's location; `LOTSE_CONFORMANCE` names another directory. Every vector is decoded and written again to identical bytes, and refused when cut at any length or with a byte added. The SCRAM vectors are replayed message by message against `ScramClient` and a test server written from the RFC.

Tests that need the `lotse` executable (certificates from `lotse certs`, a node started from a generated cluster file with TLS, a SCRAM user created through `AlterUser`, sessions of 4 s, and a three-node cluster on plain loopback TCP for the stream client and the queue consumer) are skipped, with a message, when `LOTSE_BIN` does not name one. The rest run against servers in the test process: a fake broker on loopback that answers as each test tells it to, and TLS servers whose certificates are made by the test.

There are two test assemblies. `Lotse.Client.Tests` references the codecs package and reads the broker's compressed vectors for real. `Lotse.Client.NoCodecs.Tests` does not: it checks the named error, plain batches, a hand-registered codec and the limits the SDK keeps for itself, and it changes the process's codec registry, so none of its tests runs beside another.

The queue tests on three nodes drain a queue with a small credit, requeue with a count of 2, discard, hold a message back with `NackAfterAsync` and with `x-lotse-delay-ms`, let an ack time out, drop a consumer and hand its messages to the next, kill the queue's leader in the middle of consuming 60 messages, retire a message into a dead-letter queue by `max.deliveries` and by `message.ttl.ms` with its origin headers, hand a single active queue to a standby when the active consumer ends or disappears, deliver by priority, and give back on close what was pushed and not read. The unit tests run the consumer against scripted brokers that push to it (`FakeNode.Conn`).

The transaction tests on three nodes check what read_committed readers see while a transaction is open, committed and aborted (across two partitions, and after an exception in the block), a producer fenced by a newer epoch, a cancelled produce, exactly-once consume-transform-produce from a stream (with an abort in the middle) and from a queue, offsets that an aborted transaction does not commit, an abort that gives a message back, the `MESSAGE_NOT_HELD` of a second transaction, a publish routed by the client that is atomic across two queues, and a publish through a topic exchange routed by a node. A forwarding node in the test process drops the answer to a publish: the call is `OutcomeUnknownException`, and the real node holds the message once. The group tests run two members that share four partitions without ever owning one together, a static member taken over by its next instance while the earlier one is fenced, a pattern that picks up a stream created later, and `StartFrom` with a commit across members. The unit tests run both against scripted brokers.

The three-node stream tests create streams, produce and read back, look up offsets by start, end and time, kill the leader of a partition in the middle of 150 idempotent produces (every message once, in order, with its own offset), follow a failover, produce without idempotence, send the same sequence to a node twice, and round-trip zstd and LZ4 batches through a node, which refuses a block it cannot decode.

The admin tests run in three places. Unit tests against scripted brokers decode every request from the wire, check each field, and run every admin change through the same refusal, retry and cancellation cases. Three real nodes on plain loopback TCP create and delete streams, queues and exchanges, refuse a conditional delete while a queue holds an unsettled message or a stream a record, purge, requeue dead letters to the queue they came from, change, reset and read stream and queue settings with their source, raise a partition count, list nodes, read every node's metrics, and pin a leader to each node in turn. One real node with TLS and a required login (`alice` the cluster admin by certificate) sets passwords and logs the users in over SCRAM, binds a tenant admin who may work in her tenant and may not elsewhere, a consumer who may not create, change, purge or delete, and a tenant admin limited by a name prefix, shows a cluster viewer and a user without a role what each may see, ends the sessions of one user and not another, and deletes a tenant only when it holds nothing. Changes are applied by the node before it answers, but the tests wait for their effect (every node knowing a stream, a login working, a role ending) before they look anywhere but at the metadata leader.

Safety rules that the tests break on purpose (ADR 0010): the SCRAM server signature, the strict server nonce and the iteration bounds, no transport by default, plain TCP only on loopback, correlation by id, the error flag becoming `BrokerException`, the limit on kept pushes, renewal at four fifths, the lifetime counted from before the login, replacing an ended session, certificate checking, TLS 1.3 only, the client certificate and the end at certificate expiry; and, for the stream client, the sequence advancing by the record count and per partition, an unknown outcome retried with the same bytes when idempotent and never otherwise, the session started over after an unknown outcome, an out-of-order sequence, a fenced producer or a cancelled produce, produces to one partition one at a time, the leader term sent with a fetch, the isolation of fetch and `ListOffsets`, skipping aborted transactions (from their first offset, sorted, ended only by their producer's abort marker, control batches never shown), following a leader hint only when its term is not older and it names another leader, dropping the route on `UNKNOWN_RESOURCE`, the newest id winning the merge, asking a partition's voters, a refusal after a lost attempt becoming `OutcomeUnknownException`, the tenant, `OFFSET_OUT_OF_RANGE` not retried, and, for the codecs, the decompression limit (removed and off by one, and kept by the SDK against a codec that ignores it), the builder's limit, the builder compressing, decompression running, a missing codec named and not reported as damage, and a codec that is not there not being called available., and, for the queue consumer, the credit threshold (rounded up, at least, reset after each refill, bytes counted once), handing out only the offsets in a push's list with the count it gives, ranges sorted, deduplicated and joined, a requeue on the subscription's own connection and not for a message of an ended subscription, the delay, an ack retried after a lost answer and after `NOT_LEADER` but not after a refusal, asking to be told when active, the standby answer and the activation push, resubscribing after a push error or a broken connection (and not while messages are held), the leader hint and the dropped route, what close sends and in which order, giving back unread pushes, waking a waiting task, and ending the connection of a cancelled subscribe; and, for transactions, groups and publishing, the epoch the end raises and the producer id asked afresh after a failed end, aborting on dispose, only an abort left after an error, an unknown write, a refused write, a refused offset and a call that ended without an outcome, the sequence counted by records and per partition, the transactional bit, a partition added once, the commit flag, one transaction at a time, ending a transaction that only sent offsets, not ending one whose add only gave up, offsets that go with the transaction and not on their own, the ack on the subscription's own connection and its `MESSAGE_NOT_HELD`, a publish that reached some queues only, a publish whose answer is lost not being sent again, the routing of topic (`#`, the empty key), headers (all and any, null values) and the binding refresh, a revoked partition released at once and only after its commit, dropping everything at the session timeout and after a slow fetch, a member rejoining after `UNKNOWN_MEMBER_ID`, staying out after `FENCED_INSTANCE_ID`, the start offset, the log start and `require_stable`.

For the admin calls the tests break, on purpose: the reason and the code of a refusal reaching the caller and a refusal not being swallowed, a retriable answer counting as possibly carried out, the conditional flag of a delete, an already deleted resource being done, a name turning into the right id, purge and requeue not swapped and the rounds continuing from the bound the node gave, the exchange cache dropped by a delete, settings sent in key order, a new salt for every password, the add and remove lists of role bindings and the field order of a binding, the subject kind of a revocation (the real node keeps the session when the kind is wrong) and the pin of a leader being removed with `null`.

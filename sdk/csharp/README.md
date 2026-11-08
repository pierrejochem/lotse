# Lotse client for .NET

A C# client for the Lotse native protocol. It follows the Java SDK rule for rule ([ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md), [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md)). The Rust code is the authority; the golden vectors in [`sdk/conformance`](../conformance) enforce it. The protocol as an implementer needs it is in [`sdk/PROTOCOL.md`](../PROTOCOL.md).

## Status

Phase 4 of 8 is done: the protocol codec and record batch, connections and authentication, the stream client (`LotseClient`) with the optional zstd and LZ4 codecs, and now the queue consumer (`QueueConsumer`, `CreateQueueAsync`).

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response (one codec per message), error codes |
| `Lotse.Record` | Record batch parser and builder, in-place edits (producer, transactional bit, base timestamp), control batches, varints, CRC32C |
| `Lotse.Net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, a read loop, session state), `ConnectionPool` (login, renewal, replacement), `ConnectionOptions`, `TlsConfig`, `Address`, the exceptions |
| `Lotse.Auth` | SCRAM-SHA-512 (`ScramClient`, both ends are tested), OAUTHBEARER messages, `Credentials`, `Authenticator`, `ScramCredential` for `AlterUser` |
| `Lotse.Client` | `LotseClient` (metadata routing, idempotent produce, read_committed fetch, `ListOffsets`, `CreateStream`, `CreateQueue`, `Subscribe`), `QueueConsumer`, `Delivery`, `DeadLetter`, `QueueOptions`, `SubscribeOptions`, `ClientConfig`, `OutMessage`, `Fetched`, `OffsetAt`, the exceptions |

Two packages, two projects:

| Project | What | Dependencies |
| --- | --- | --- |
| `src/Lotse.Client` (`Lotse.Client`) | Everything above | none beyond the base class library |
| `src/Lotse.Client.Codecs` (`Lotse.Client.Codecs`) | zstd and LZ4 for compressed batches, optional ([ADR 0084](../../docs/adr/0084-codecs-in-the-csharp-sdk.md)) | ZstdSharp.Port 0.8.6, K4os.Compression.LZ4.Streams 1.3.8 |

Not yet: acking within a transaction, transactions, consumer groups, the other admin calls, examples.

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

`QueueOptions` sets the replication (3 by default), `ack.timeout.ms`, `max.deliveries`, `message.ttl.ms`, the dead-letter queue, `single.active.consumer` (ADR 0076) and `priority.levels` (1 to 10, ADR 0078); `WithSetting(key, value)` passes any other setting through. A message gets its priority from the header `x-lotse-priority` and a delay from `x-lotse-delay-ms` (ADR 0077); both are plain headers on `OutMessage`. Acking within a transaction (ADR 0075) comes with the transactions in phase 5.

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

The three-node stream tests create streams, produce and read back, look up offsets by start, end and time, kill the leader of a partition in the middle of 150 idempotent produces (every message once, in order, with its own offset), follow a failover, produce without idempotence, send the same sequence to a node twice, and round-trip zstd and LZ4 batches through a node, which refuses a block it cannot decode.

Safety rules that the tests break on purpose (ADR 0010): the SCRAM server signature, the strict server nonce and the iteration bounds, no transport by default, plain TCP only on loopback, correlation by id, the error flag becoming `BrokerException`, the limit on kept pushes, renewal at four fifths, the lifetime counted from before the login, replacing an ended session, certificate checking, TLS 1.3 only, the client certificate and the end at certificate expiry; and, for the stream client, the sequence advancing by the record count and per partition, an unknown outcome retried with the same bytes when idempotent and never otherwise, the session started over after an unknown outcome, an out-of-order sequence, a fenced producer or a cancelled produce, produces to one partition one at a time, the leader term sent with a fetch, the isolation of fetch and `ListOffsets`, skipping aborted transactions (from their first offset, sorted, ended only by their producer's abort marker, control batches never shown), following a leader hint only when its term is not older and it names another leader, dropping the route on `UNKNOWN_RESOURCE`, the newest id winning the merge, asking a partition's voters, a refusal after a lost attempt becoming `OutcomeUnknownException`, the tenant, `OFFSET_OUT_OF_RANGE` not retried, and, for the codecs, the decompression limit (removed and off by one, and kept by the SDK against a codec that ignores it), the builder's limit, the builder compressing, decompression running, a missing codec named and not reported as damage, and a codec that is not there not being called available., and, for the queue consumer, the credit threshold (rounded up, at least, reset after each refill, bytes counted once), handing out only the offsets in a push's list with the count it gives, ranges sorted, deduplicated and joined, a requeue on the subscription's own connection and not for a message of an ended subscription, the delay, an ack retried after a lost answer and after `NOT_LEADER` but not after a refusal, asking to be told when active, the standby answer and the activation push, resubscribing after a push error or a broken connection (and not while messages are held), the leader hint and the dropped route, what close sends and in which order, giving back unread pushes, waking a waiting task, and ending the connection of a cancelled subscribe.

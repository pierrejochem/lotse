# Lotse client for .NET

A C# client for the Lotse native protocol. It follows the Java SDK rule for rule ([ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md), [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md)). The Rust code is the authority; the golden vectors in [`sdk/conformance`](../conformance) enforce it. The protocol as an implementer needs it is in [`sdk/PROTOCOL.md`](../PROTOCOL.md).

## Status

Phase 3 of 8 is done: the protocol codec and record batch, connections and authentication, and now the stream client (`LotseClient`) with the optional zstd and LZ4 codecs. There is no queue client yet.

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response (one codec per message), error codes |
| `Lotse.Record` | Record batch parser and builder, in-place edits (producer, transactional bit, base timestamp), control batches, varints, CRC32C |
| `Lotse.Net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, a read loop, session state), `ConnectionPool` (login, renewal, replacement), `ConnectionOptions`, `TlsConfig`, `Address`, the exceptions |
| `Lotse.Auth` | SCRAM-SHA-512 (`ScramClient`, both ends are tested), OAUTHBEARER messages, `Credentials`, `Authenticator`, `ScramCredential` for `AlterUser` |
| `Lotse.Client` | `LotseClient` (metadata routing, idempotent produce, read_committed fetch, `ListOffsets`, `CreateStream`), `ClientConfig`, `OutMessage`, `Fetched`, `OffsetAt`, the exceptions |

Two packages, two projects:

| Project | What | Dependencies |
| --- | --- | --- |
| `src/Lotse.Client` (`Lotse.Client`) | Everything above | none beyond the base class library |
| `src/Lotse.Client.Codecs` (`Lotse.Client.Codecs`) | zstd and LZ4 for compressed batches, optional ([ADR 0084](../../docs/adr/0084-codecs-in-the-csharp-sdk.md)) | ZstdSharp.Port 0.8.6, K4os.Compression.LZ4.Streams 1.3.8 |

Not yet: queues, transactions, consumer groups, the other admin calls, examples.

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

Tests that need the `lotse` executable (certificates from `lotse certs`, a node started from a generated cluster file with TLS, a SCRAM user created through `AlterUser`, sessions of 4 s, and a three-node cluster on plain loopback TCP for the stream client) are skipped, with a message, when `LOTSE_BIN` does not name one. The rest run against servers in the test process: a fake broker on loopback that answers as each test tells it to, and TLS servers whose certificates are made by the test.

There are two test assemblies. `Lotse.Client.Tests` references the codecs package and reads the broker's compressed vectors for real. `Lotse.Client.NoCodecs.Tests` does not: it checks the named error, plain batches, a hand-registered codec and the limits the SDK keeps for itself, and it changes the process's codec registry, so none of its tests runs beside another.

The three-node tests create streams, produce and read back, look up offsets by start, end and time, kill the leader of a partition in the middle of 150 idempotent produces (every message once, in order, with its own offset), follow a failover, produce without idempotence, send the same sequence to a node twice, and round-trip zstd and LZ4 batches through a node, which refuses a block it cannot decode.

Safety rules that the tests break on purpose (ADR 0010): the SCRAM server signature, the strict server nonce and the iteration bounds, no transport by default, plain TCP only on loopback, correlation by id, the error flag becoming `BrokerException`, the limit on kept pushes, renewal at four fifths, the lifetime counted from before the login, replacing an ended session, certificate checking, TLS 1.3 only, the client certificate and the end at certificate expiry; and, for the stream client, the sequence advancing by the record count and per partition, an unknown outcome retried with the same bytes when idempotent and never otherwise, the session started over after an unknown outcome, an out-of-order sequence, a fenced producer or a cancelled produce, produces to one partition one at a time, the leader term sent with a fetch, the isolation of fetch and `ListOffsets`, skipping aborted transactions (from their first offset, sorted, ended only by their producer's abort marker, control batches never shown), following a leader hint only when its term is not older and it names another leader, dropping the route on `UNKNOWN_RESOURCE`, the newest id winning the merge, asking a partition's voters, a refusal after a lost attempt becoming `OutcomeUnknownException`, the tenant, `OFFSET_OUT_OF_RANGE` not retried, and, for the codecs, the decompression limit (removed and off by one, and kept by the SDK against a codec that ignores it), the builder's limit, the builder compressing, decompression running, a missing codec named and not reported as damage, and a codec that is not there not being called available.

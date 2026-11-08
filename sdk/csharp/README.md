# Lotse client for .NET

A C# client for the Lotse native protocol. It follows the Java SDK rule for rule ([ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md), [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md)). The Rust code is the authority; the golden vectors in [`sdk/conformance`](../conformance) enforce it. The protocol as an implementer needs it is in [`sdk/PROTOCOL.md`](../PROTOCOL.md).

## Status

Phase 2 of 8 is done: the protocol codec and record batch, and now connections and authentication. There is no stream or queue client yet.

| Namespace | What |
| --- | --- |
| `Lotse.Proto` | Field reader and writer, frames, every client-facing request and response (one codec per message), error codes |
| `Lotse.Record` | Record batch parser and builder, in-place edits (producer, transactional bit, base timestamp), control batches, varints, CRC32C |
| `Lotse.Net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, a read loop, session state), `ConnectionPool` (login, renewal, replacement), `ConnectionOptions`, `TlsConfig`, `Address`, the exceptions |
| `Lotse.Auth` | SCRAM-SHA-512 (`ScramClient`, both ends are tested), OAUTHBEARER messages, `Credentials`, `Authenticator`, `ScramCredential` for `AlterUser` |

Not yet: streams, queues, transactions, consumer groups, admin calls, examples.

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

The SDK has no zstd or LZ4 library yet. A compressed batch still parses: its header and checksum are checked. Reading its records, or building a compressed batch, throws `RecordFormatException` with kind `UnsupportedCompression`, naming the codec. Which library will serve the codecs is settled with the stream client; per ADR 0083 it stays optional.

## Build and test

```sh
export DOTNET_ROOT=$HOME/.dotnet PATH=$HOME/.dotnet:$PATH   # wherever the .NET 10 SDK is
make sdk-csharp-test                                         # or: cd sdk/csharp && dotnet test
LOTSE_BIN=/tmp/cargo-target/debug/lotse dotnet test          # adds the TLS and real-node tests
```

`global.json` pins the SDK. The tests read the vectors from `sdk/conformance` in place and find them from the test binary's location; `LOTSE_CONFORMANCE` names another directory. Every vector is decoded and written again to identical bytes, and refused when cut at any length or with a byte added. The SCRAM vectors are replayed message by message against `ScramClient` and a test server written from the RFC.

Tests that need the `lotse` executable (certificates from `lotse certs`, a node started from a generated cluster file with TLS, a SCRAM user created through `AlterUser`, sessions of 4 s) are skipped, with a message, when `LOTSE_BIN` does not name one. The rest run against servers in the test process: a fake broker on loopback, and TLS servers whose certificates are made by the test.

Safety rules that the tests break on purpose (ADR 0010): the SCRAM server signature, the strict server nonce and the iteration bounds, no transport by default, plain TCP only on loopback, correlation by id, the error flag becoming `BrokerException`, the limit on kept pushes, renewal at four fifths, the lifetime counted from before the login, replacing an ended session, certificate checking, TLS 1.3 only, the client certificate and the end at certificate expiry.

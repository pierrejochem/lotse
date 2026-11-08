# Lotse client for Node.js

A client for the Lotse native protocol in TypeScript, for Node.js 20 and later. It follows [ADR 0083](../../docs/adr/0083-client-sdks-for-csharp-and-nodejs.md) and [ADR 0056](../../docs/adr/0056-client-sdks-for-the-jvm.md): the Rust code is the authority, the golden vectors in [`../conformance`](../conformance) enforce it, and the [Java client](../java) is the reference for structure. The package name `@lotse/client` is a placeholder; nothing is published.

## Status

Phase 3 of 8 is done: the stream client. It creates streams, produces idempotently, reads committed records, and finds offsets. There is no queue client, transaction, consumer group or further admin call yet.

| Module | What |
| --- | --- |
| `src/proto` | Field codec (`Reader`, `Writer`), frames and pushes, every client request and response, one codec per message, error codes |
| `src/record` | Record batch: parse, build, in-place edits (`stamp`, `setProducer`, `setTransactional`, `setBaseTimestamp`), control batches, CRC32C, varints |
| `src/net` | `Connection` (TLS 1.3 or plain TCP on loopback, pipelining, pushes, session state), `ConnectionPool`, `TlsConfig`, `Address`, errors |
| `src/auth` | `ScramClient` (SCRAM-SHA-512), `ScramCredential` (the `AlterUser` credential), OAUTHBEARER messages, `authenticate` |
| `src/client` | `LotseClient`: metadata routing with leader hints, idempotent `produce`, read_committed `fetch`, `listOffset`, `createStream`, `readRecords`; `Fetched`, `OffsetAt`, `OutMessage`, the errors |

## Connections

```ts
import { Connection, ConnectionPool, TlsConfig, scramCredentials, parseAddress } from "@lotse/client";

const tls = await TlsConfig.fromFiles("ca.pem", "alice.pem", "alice.key"); // client certificate optional
const node = parseAddress("10.0.0.1:9092");

const connection = await Connection.open(node, { tls });
const metadata = await connection.call({ type: "Metadata", names: [] }, "Metadata");
connection.close();

// One authenticated connection per address, renewed and replaced as sessions end.
const pool = new ConnectionPool({ tls, credentials: scramCredentials("bob", "secret") });
const c = await pool.get(node);
pool.close();
```

- **The transport has no default.** Options without `tls` and without `plaintextOnLoopback: true` are refused with `InsecureTransportError` before a socket is opened. Plain TCP is allowed only when every address the host resolves to is loopback, and refused otherwise, again before anything is sent (rule 7 of `AGENTS.md`, [ADR 0037](../../docs/adr/0037-tls-and-node-identity.md)).
- **TLS 1.3 only**, with the CA you give and no other: the roots Node.js ships with are not consulted. The node's certificate must be valid for the host you dialled, including IP entries: dial the address the cluster file lists. The client key is a PKCS#8 PEM, as `lotse certs` writes it.
- **Requests are pipelined.** `send` returns a promise that settles when the response with its correlation id arrives, in any order. A response with the `ERROR` flag rejects with `BrokerError` (code and optional reason) before any typed decoding and leaves the connection open. No answer within `requestTimeoutMs` (default 10 000) rejects with `RequestTimeoutError`, whose outcome is unknown. A broken or closed connection rejects every pending request with `ConnectionError`; `requestSent` tells whether the request may have reached the node. The writer does not wait for the socket to drain, so bound the requests you keep in flight.
- **Pushes** go to the `PushListener` registered with `register(subscriptionId, listener)`, or are read with `for await (const push of connection.pushes(id))`. A push that arrives before its subscription is registered is kept, for at most 64 subscriptions, and delivered on registration.
- **Sessions.** After a SASL login, `sessionState()` is `ACTIVE`, `RENEW_DUE` (past four fifths of the lifetime, counted from before the first request) or `ENDED`; a certificate login is `UNLIMITED`. The pool renews a due session on the same connection and replaces an ended or broken one with a fresh login. Without a pool, call `connection.authenticate(credentials)` before the session ends.
- **A connection ends when the node's certificate expires** ([ADR 0068](../../docs/adr/0068-connections-end-when-their-certificate-expires.md)): pending and later requests fail with `ConnectionError`. The deadline timer does not keep the process alive.

## Streams

```ts
import { LotseClient, OffsetAt, TlsConfig } from "@lotse/client";

const client = new LotseClient({
  bootstrap: ["10.0.0.1:9092", "10.0.0.2:9092"],
  tls: await TlsConfig.fromFiles("ca.pem", "alice.pem", "alice.key"),
});

const id = await client.createStream("orders", 2, 3, { "retention.ms": "86400000" }); // 2 partitions on 3 nodes each
const { baseOffset } = await client.produce("orders", 0, { key: "k1", value: "hello", headers: [{ name: "n", value: "1" }] });

for await (const record of client.readRecords("orders", 0, { from: baseOffset, untilEnd: true })) {
  console.log(record.offset, Buffer.from(record.value ?? []).toString());
}
const end = await client.listOffset("orders", 0, OffsetAt.LATEST);
client.close();
```

The client follows `lotse-client` and the Java client rule for rule; [`../PROTOCOL.md`](../PROTOCOL.md) lists the rules.

- **Options.** Durations are milliseconds: `requestTimeoutMs` 10 000, `retryForMs` 30 000, `backoffMs` 50, `produceTimeoutMs` 5 000, `txnTimeoutMs` 60 000. `tenant` is `default`. Like `Connection`, the client has no default transport: give `tls` or `plaintextOnLoopback: true`, or it refuses to connect with `InsecureTransportError`, which is never retried.
- **Routes.** `Metadata` gives each partition's leader and term. The client uses a route until a request fails. A `NOT_LEADER` or `FENCED_LEADER_TERM` hint is followed only if its term is not older (compared as `u64`) and it names another leader; otherwise the route is dropped and metadata is asked again. A name one broker does not know is asked of all brokers, and the highest resource id wins. A leader of 0 asks the partition's voters.
- **Produce is idempotent by default.** The first produce gets a producer id from the coordinator; every batch is stamped with it and a sequence per partition. A batch whose outcome is unknown (the connection broke after sending, `REQUEST_TIMED_OUT`) is sent again with the same bytes, so the leader recognises it. Produces to one partition go out one at a time, in call order; different partitions run in parallel. If retrying runs out of time with the outcome still unknown, the call rejects with `UnknownOutcomeError` and the client starts over with a new producer id. With `idempotent: false` nothing is stamped and an unknown outcome is never retried. `producerName` keeps a producer id across restarts (ADR 0071): the next instance with the name fences the earlier one.
- **Give up a call with a signal.** `produce`, `fetch`, `listOffset` and `createStream` take `{ signal }`. A produce given up while its answer may still come rejects with `UnknownOutcomeError` and drops the producer id, because reusing the sequence could make the leader take the next batch for the abandoned one. A call that was not sent rejects with `GaveUpError`.
- **Fetch is read_committed.** It stops at the last stable offset. `Fetched.records` holds whole batches as the broker sent them; `committedBatches()` and `committedMessages()` leave out control batches and aborted transactions. Go on from `nextOffset(from)`, not from the number of records: a response may hold only a control batch, and the first record of a fresh partition is not at offset 0. `fetchUncommitted` reads up to the high watermark. An offset the broker no longer has rejects with `OffsetOutOfRangeError`, whose `logStart` says where to go on.
- **`readRecords`** is `fetch` as an async iterator: it yields committed records, goes on by itself, and ends when `untilEnd` is set and a fetch returns nothing, or when its `signal` fires.
- **`listOffset`** with `OffsetAt.EARLIEST`, `OffsetAt.LATEST` or `OffsetAt.time(ms)` (ADR 0059). A time finds the first batch with a record at or after it, so the batch may begin with older records.
- **`createStream`** goes to the metadata leader. A create whose answer was lost is sent again; if the first one did create the stream, the second rejects with `ResourceExistsError`, which carries the id.
- **Messages.** `OutMessage` takes `string` (UTF-8) or `Uint8Array` for keys, values and header values; `null` and empty differ. The timestamp defaults to the time of the call: a message stamped 0 is old at once to the stream's retention.
- **Errors** extend `ClientError`: `BrokerResponseError` (with `code`), `OffsetOutOfRangeError`, `UnknownOutcomeError`, `GaveUpError`, `UnknownStreamError`, `ResourceExistsError`, `ClientAuthenticationError`, `ClientClosedError`. Only `UnknownOutcomeError` needs a decision: the write may or may not have happened.

## Authentication

`scramCredentials(user, password)` and `oauthBearerCredentials(() => token)` (the function is called for every authentication, renewals included, and may be async). A client certificate is part of `TlsConfig`.

SCRAM-SHA-512 follows RFC 5802 and RFC 7677 with the particulars in [`../PROTOCOL.md`](../PROTOCOL.md): the password is its UTF-8 bytes with no SASLprep, the server nonce must strictly extend the client's, iteration counts outside 4096 to 1 048 576 are refused, and the server's final signature is always verified; a node that cannot prove it knows the credential is refused with `ScramError` of kind `SERVER_NOT_VERIFIED`. PBKDF2 runs off the event loop. A wrong password and an unknown user are refused alike by the node (`AUTHENTICATION_FAILED`), and the node closes the connection.

`ScramCredential.forPassword(password)` computes what a node stores (salt, iterations, `StoredKey`, `ServerKey`), and `.alterUser(name)` is the `AlterUser` request that sets it; the password itself is never sent.

## Conventions

- No runtime dependencies. `typescript` and `@types/node` are development dependencies only.
- A 64-bit wire value is a `bigint`. A `u64` is held as its signed 64-bit bit pattern, as in the Java client, so `NO_OFFSET` (`u64::MAX`) is `-1n`. Compare, add and print such values with `compareU64`, `addU64Exact` and `u64ToString`; do not use `<` or `String` when a value may be that large.
- Narrower integers (`u8`, `u16`, `u32`, `i32`) are `number`. A writer refuses a value out of range with a `RangeError`.
- Fixed-width fields are little-endian. The frame length prefix is big-endian.
- A frame that does not decode throws `DecodeError`; a batch that does not parse throws `RecordFormatError`. Both carry a `kind`.
- Keys, values and header values are `Uint8Array`, `null` where the protocol has a null. Null and empty differ.

## Compression

zstd and lz4 are read and written ([ADR 0085](../../docs/adr/0085-codecs-in-the-nodejs-sdk.md)). Set `attributes.compression` on a `BatchBuilder`, or read compressed batches from a fetch; nothing compresses by default.

- **lz4** is implemented in the SDK (frame format with checksums, independent and linked blocks) and works on every Node.js.
- **zstd** is `node:zlib`'s, which has it from Node.js 22.15 and 23.8 and in 24. On Node.js 20 and earlier 22 versions, reading or building a zstd batch rejects with `RecordFormatError` of kind `UNSUPPORTED_COMPRESSION`, naming the codec and what to do. To read zstd there anyway, give the SDK a codec of your own: `registerCodec(Compression.ZSTD, { compress, decompress })` with functions from a package you choose; `registerCodec(Compression.ZSTD, undefined)` removes it.
- A block is at most 16 MiB once decompressed; one byte more is `TOO_LARGE`. A block that does not decode is `MALFORMED`. A compacted batch with no records reads as empty whatever its codec, because compaction writes no block for it.

## Tests

```sh
npm ci
npm test        # compiles to dist/, then runs node:test on dist/test
```

or `make sdk-node-test` from the repository root. The tests decode every vector in `sdk/conformance` and write it again to identical bytes, refuse each vector cut at every length and with one byte added, and check the fixed cases for CRC32C, varints, error codes and in-place batch edits. Set `LOTSE_CONFORMANCE` to read the vectors from another directory.

The client tests run against scripted brokers in the test process (routing, hints, idempotent retries, read_committed, signals, `createStream`) and, with `LOTSE_BIN`, against three real nodes on loopback (create, produce, read back, `ListOffsets`, compressed batches, a leader killed in the middle of a produce, a failover, a named producer fenced) and one TLS node. The lz4 codec has its own tests (`lz4.test.ts`); the zstd tests are skipped, with the reason, on a Node.js without zstd in `node:zlib`.

The connection tests run against a fake broker in the test process (plain TCP and TLS), with a fake clock for sessions; SCRAM is checked against `conformance/scram.txt` and a server written for the tests from the RFC. The tests that need real certificates or a real node (`tls`, `real-node`, `client-real`) use the `lotse` executable: set `LOTSE_BIN`, or build `lotse-node` so that `/tmp/cargo-target/debug/lotse` (or `$CARGO_TARGET_DIR/debug/lotse`) exists. Without one they are skipped and say so. They start one TLS node with sessions limited to 4 s and one plain-TCP node with authentication off, and the stream client tests three more, so the suite takes about 25 s with `LOTSE_BIN` set. The certificate expiry test needs `openssl` 3.4 or later and is skipped without it.

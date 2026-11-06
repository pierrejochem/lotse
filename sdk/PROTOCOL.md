# The native protocol, for implementers

This is what a client in another language needs beyond reading the Rust. Where this file and [`docs/design.md`](../docs/design.md) differ, this file follows the code. Where it differs from the code, the code wins and this file is wrong: fix it. The golden vectors in [`conformance/`](conformance/) pin the layouts below to the broker's bytes.

All of it is version 0 of every API key.

## Integers and fields

Fixed-width integers are **little-endian**. The one exception is the frame length prefix.

| Type | Wire | Notes |
| --- | --- | --- |
| `u8`, `u16`, `u32`, `u64`, `i32`, `i64` | 1, 2, 4, 8, 4, 8 bytes | booleans are a `u8`: write 0 or 1, read any non-zero as true |
| `varint` | ZigZag, then LEB128, at most 10 bytes | array counts, tag numbers, record fields |
| `string` | `u16` length, UTF-8 | length `0xFFFF` is null; at most 65,534 bytes |
| `bytes` | `u32` length, data | no null form |
| `array<T>` | `varint` count, elements | the count is ZigZag: 2 elements is the byte `04` |
| optional | `u8` 0 or 1, then the value if 1 | a different form from a null string |
| tagged fields | `varint` count, then `{tag varint, bytes}` | see below |

**Unsigned types in Java.** `u64` travels in a `long` as its bit pattern; compare, divide and print it with the `Long.*Unsigned` methods. `u32` fits a `long`, `u16` an `int`. `NO_OFFSET` (a group's "no committed offset") is `u64::MAX`, which is `-1` as a `long`. Offsets are `u64` in the protocol but the first offset of a log is 0: `base_offset` is the Raft index of the first record minus one (the broker stamps `index - 1`), although the doc comment in `lotse-record` says "index of the first record".

**Counts are bounded before allocating.** A reader must refuse an array whose count times the smallest encoding of one element exceeds the bytes left in the frame. The minimum element sizes are part of the broker's decoder (the `array(min_size, ..)` arguments in `messages.rs`); a decoder that guesses a smaller minimum is merely more permissive, one that guesses a larger minimum refuses valid frames of nothing but minimal elements.

**Varints.** The reader accepts overlong encodings such as `80 00` for 0 and does not tell a truncated varint from an overlong one in a frame (both are "truncated"); inside a record they are different errors. Writers never produce overlong forms. The tenth byte of a 10-byte varint may only be `00` or `01`.

## Frames

On a connection every frame is preceded by its length, a **big-endian** `u32`, at most 16 MiB (`MAX_FRAME`). A larger length ends the connection.

```text
Request:  api_key u16 | api_version u16 | correlation_id u32 | flags u16 | tagged_fields | body
Response: correlation_id u32 | flags u16 | tagged_fields | body
Push:     correlation_id u32 (0) | flags u16 (PUSH) | tagged_fields | api_key u16 | api_version u16 | body
```

- Flags: `PUSH` = 1, `ERROR` = 2.
- Correlation ids of requests start at 1 and are never 0; 0 marks pushes. Requests are pipelined and responses may arrive in another order: match them by id.
- A response has no API key. The decoder must be told which request it answers.
- **Check `ERROR` before decoding a typed body.** A response with that flag has the body `error_code u16 | tagged_fields`, whatever the request was (unsupported version, connection not authenticated). If tagged field 0 is present, its bytes are the reason, in UTF-8. Decoding such a frame as a `Produce` response misreads it.
- **Push detection:** the first four bytes are zero and bit 0 of the flags is set. A response with correlation id 0 cannot exist.
- A push's body begins with the **subscription id**; `docs/design.md` used to say the id sat in the header.
- `Credit` is never answered. No other request is unanswered.
- After the header's tagged fields, `ApiVersions` still has a body: one more, empty block of tagged fields (`00`).

## Tagged fields: not at the same place everywhere

Most structures end with a block of tagged fields, written empty (`00`) and skipped by readers (unknown tags come from newer versions and are dropped, not preserved). That is not uniform, and a generic "every struct ends in tags" decoder is wrong. The leader hint is tag 0 of the block it sits in: `leader u32 | term u64`, present only after `NOT_LEADER`.

The tenant of a transactional or group id is tag 1 of a request's trailing block: one `string` (ADR 0062). It is written only for a tenant other than `default`; a reader that finds none takes `default`. The requests that carry it are `Produce` (it applies to its transactional id), `InitProducer`, `AddPartitionsToTxn`, `EndTxn`, `GroupHeartbeat`, `CommitOffsets`, `OffsetFetch` and `AddOffsetsToTxn`. A client sends the tenant of its configuration. `DescribeMetrics` gives each group's tenant as a plain `string` after its id.

| Where the block is | Messages |
| --- | --- |
| Hint **is** the trailing block | `InitProducer`, `Subscribe`, `QueueAction` (answer to `PurgeQueue`, `Requeue`), `Outcome` (answer to `Ack`, `Nack`, `Unsubscribe`, `Bind`, `Unbind`), `Create` (`CreateStream`, `CreateQueue`, `DeclareExchange`), `Admin` (`DeleteResource`, `AlterConfigs`, `AlterUser`, `CreateTenant`, `DeleteTenant`, `AlterRoleBindings`, `AddNode`, `RemoveNode`) |
| Hint in the **middle**, no trailing block | `DescribeConfigs`, `DescribeUsers`, `DescribeTenants`, `DescribeRoleBindings`, `DescribeNodes` (after the error code, before the array) |
| Hint is the **element's** block, one per partition | `Produce` and `Fetch` partitions (the resource around them still ends in an empty block) |
| Hint is the last field of a push | `Deliver` |
| **No** block at all | `SaslHandshake` and `SaslAuthenticate` responses, `WhoAmI` response |
| Empty block after the body fields | everything else |

Elements of arrays carry their own empty block in most requests (`TxnResource`, produce and fetch partitions, offset commits) and none in a few (`ApiVersions` entries, queue ranges, config pairs, role bindings, metrics sub-structures). Do not infer: the layouts are in `Request.java` and `Response.java` of the Java SDK, and each is covered by a vector.

## Responses worth a second look

- `Metadata`: a resource carries its own `error`. An unknown name is a resource with `UNKNOWN_RESOURCE`, not a frame error. Leader `0` means unknown.
- `OffsetFetch`: an offset of `u64::MAX` means none.
- `DescribeMetrics`: `connections` is sent as a `u64` but must fit a `u32`.
- `Produce` and `Fetch`: every partition has its own error; a request can partly succeed.

## Error codes

Codes 0 to 29, with names and retry classes, are in `ErrorCode.java` and in `conformance/error-codes.txt`. Retriable: `NOT_LEADER`, `FENCED_LEADER_TERM`, `NOT_ENOUGH_REPLICAS`, `REQUEST_TIMED_OUT`, `COORDINATOR_NOT_AVAILABLE`, `CONCURRENT_TRANSACTIONS`, `UNSTABLE_OFFSET_COMMIT`, `CONCURRENT_GROUP_CHANGE`. `REQUEST_TIMED_OUT` means the outcome is unknown: retry only what is idempotent. A code a client does not know is kept as it came and named `UNKNOWN`.

## The record batch

A batch is a 65-byte header and a block of records. All fixed-width header fields are little-endian.

```text
offset size field
     0    8 base_offset        stamped by the leader: Raft index of the first record, minus one
     8    4 batch_len          bytes after this field, so the batch is batch_len + 12 bytes
    12    8 term               stamped by the leader
    20    1 magic              1
    21    4 crc32c             CRC32C (Castagnoli) over bytes 25 up to the end of this batch
    25    2 attributes         compression(3) | transactional(1) | control(1) | kind(3) | compacted(1), bit 0 first
    27    4 last_offset_delta  record_count - 1
    31    8 base_ts            milliseconds since the epoch
    39    8 max_ts
    47    8 producer_id        0 = not idempotent
    55    2 producer_epoch
    57    4 base_sequence      i32, -1 when there is none
    61    4 record_count
    65    . records
```

`base_offset` and `term` sit before the checksummed range: the leader stamps them without invalidating the CRC. A client sends both as zero.

Kinds: 0 data, 1 ack, 2 queue state, 3 timer, 4 dead letter, 5 retire. Compression: 0 none, 1 zstd, 2 lz4. Bits 8 to 15 must be zero.

**Compression** (ADR 0058) covers the record block only; the header stays as it is, and the CRC is over the compressed bytes. With zstd the block is one zstd frame; with lz4 it is one LZ4 frame (the frame format with its magic `04 22 4d 18`, not the bare block format). Once decompressed the block reads exactly like an uncompressed one, and it is at most 16 MiB: a reader stops decompressing past that and treats the batch as damaged. The batch itself, compressed, stays within 1 MiB. A node checks a compressed batch's records when it is produced and refuses one that does not decode (`CORRUPT_BATCH`); it stores, replicates and serves the batch compressed, so a consumer must read every codec its producers use. Control batches are never compressed.

A record, every `varint` ZigZag:

```text
length varint | attributes i8 (0) | ts_delta varint | offset_delta varint
| key_len varint (-1 = null) | key | value_len varint (-1 = null) | value
| header_count varint | [key_len varint, key, value_len varint, value]*
```

`ts_delta` is relative to `base_ts` and wraps like a signed 64-bit subtraction; `offset_delta` is the record's index and must run 0, 1, 2, and so on, except in a compacted batch, where records that compaction removed leave gaps (vectors in `compacted.hex`). A header key is never null.

**Checks a parser makes**, in this order: the buffer holds the 12-byte prefix; the length is at most 1 MiB (the length is a `u32`, so test it without overflow); it is at least 65; the whole batch is present; `magic` is 1; the CRC matches; the attributes are valid; `record_count` is not 0; `last_offset_delta == record_count - 1`; `max_ts >= base_ts`; a control batch holds exactly one record. A compacted batch (bit 8, ADR 0067) replaces the two record-count checks: it holds at most `last_offset_delta + 1` records, possibly none, and is never a control batch. Decoding the records adds: the block decompresses, to at most 16 MiB; consecutive offset deltas (in a compacted batch: strictly rising and at most `last_offset_delta`; an emptied one has no block at all, whatever its codec), no bytes left inside a record, no bytes left after the last record, lengths and counts not negative, and every length within the record.

**Editing in place.** A client sets producer fields (`set_producer`), the transactional bit (`set_transactional`) and, for the leader's TTL stamping, the base timestamp, on the encoded bytes and recomputes the CRC over bytes 25 to the end of that batch. `BatchBuilder` takes the first record's timestamp as `base_ts` and the largest as `max_ts`; it does not read a clock.

**Several batches in a row** (the records of a `Fetch` partition, a log segment): read `batch_len`, add 12, cut, repeat.  A fetch response holds whole batches.

**Control batches.** One record. Key: `version u16 (0) | type u16`. Types 0 abort and 1 commit are transaction markers whose value is the coordinator epoch as a `u32`. Types 2 and 3 are Raft's own entries, 4 to 8 the coordinator and metadata groups' records. Consumers skip all of them. A `read_committed` consumer uses the fetch response's aborted-transaction list and the markers to hide aborted data.

## Connections

One TCP connection per broker address, kept open. Every connection is TLS 1.3 with the cluster's CA (ADR 0037), or plain TCP on loopback when a node was set up for development; a client never falls back from one to the other. The client checks the node's certificate against the host it dialled, including IP entries: `lotse certs` writes the node's addresses from the cluster file as IP entries, so dial the same address the file lists. A client certificate, if the node asks for one, names the principal by its common name. There is no hello, no ALPN and no version byte: the first bytes after the handshake are the first frame.

- Set `TCP_NODELAY`. Frames are written whole; writes from several threads must not interleave.
- One reader per connection decodes frames and completes requests by correlation id. A response nobody waits for (its request timed out) is dropped without ending the connection.
- A frame whose length exceeds 16 MiB, one that does not decode, or end of stream ends the connection: fail every pending request and every subscription. A refused request (`ERROR` flag) does not end it.
- A request that timed out has an unknown outcome. Retry only what is idempotent.
- `ApiVersions` is answered before authentication. The Rust client never sends it; the broker does not require it.
- A request on a connection that is not authenticated, or whose session ended, gets an `ERROR` response with `UNAUTHORIZED` and no reason. The connection stays open.
- Pushes of a subscription can arrive before the response to `Subscribe` has been handled, so the client keeps pushes for subscriptions nobody has registered yet. The Rust client keeps up to 64 such subscriptions and drops the lowest id for another.

## Authentication

Two SASL mechanisms, chosen by name in `SaslHandshake`. A node offers the ones its configuration enables; asking for another gives `UNSUPPORTED_SASL_MECHANISM` (28) with the node's list in the same response.

```text
SaslHandshake(mechanism)  ->  error u16 | mechanisms array<string>        (no tagged fields)
SaslAuthenticate(bytes)   ->  error u16 | error_msg optional string | auth_bytes bytes | session_lifetime_ms u64
```

An unsuccessful `SaslAuthenticate` carries `AUTHENTICATION_FAILED` (27) and a message, and the node **closes the connection** after sending it. It does not say whether the user or the password was wrong.

**OAUTHBEARER** is one round: `n,,\x01auth=Bearer <token>\x01\x01`. Ask for the token on every authentication, including renewals.

**SCRAM-SHA-512** (RFC 5802, RFC 7677) is two rounds, with these particulars that differ from what a library might assume:

| Step | Detail |
| --- | --- |
| client first | `n,,n=<user>,r=<nonce>`; in the user name `=` becomes `=3D` and `,` becomes `=2C`. The nonce is 24 random bytes in hex |
| server first | `r=<nonce>,s=<base64 salt>,i=<iterations>`. The nonce must **strictly** extend the client's. Refuse iterations below 4096 or above 1,048,576 |
| password | the raw UTF-8 bytes, **no SASLprep**, as PBKDF2-HMAC-SHA512 input with a 64-byte output |
| client final | `c=biws,r=<nonce>,p=<base64 proof>`; `biws` is base64 of `n,,` (no channel binding) |
| AuthMessage | `<client first without n,,>,<server first>,<client final without ,p=...>` |
| proof | `ClientKey xor HMAC(StoredKey, AuthMessage)`, where `ClientKey = HMAC(SaltedPassword, "Client Key")` and `StoredKey = SHA-512(ClientKey)` |
| server final | `v=<base64 HMAC(ServerKey, AuthMessage)>`; the client **must** verify it, it is the server's proof that it knows the credential |

An empty password is valid. HMAC pads a short key with zeros, so an implementation whose library refuses an empty key can use 128 zero bytes.

`conformance/scram.txt` holds whole exchanges with fixed nonces, written by the broker's own code, and the credential computed for each.

**Setting a password** (`AlterUser`, admin) sends a credential, never the password: a 16-byte random salt, 4096 iterations, `StoredKey` and `ServerKey = HMAC(SaltedPassword, "Server Key")` (each 64 bytes). A removal sends no credential.

**Sessions.** `session_lifetime_ms` of the **last** response is how long the session lasts, counted by the node from when it completes the login (its last answer); `0` means as long as the connection (a certificate login has no SASL session at all). The node caps it at its `max_session_s` and, for OAUTHBEARER, at the token's expiry. Authenticate again on the same connection (`SaslHandshake`, then `SaslAuthenticate`) before it ends: the Rust client does so at four fifths of the lifetime, counted from before its first request (so it never believes in a session longer than the node does), and the node insists on the same principal. Requests in flight meanwhile are served. After the end the node answers `UNAUTHORIZED` to everything but SASL and `ApiVersions`; the Rust client does not try to renew then but opens a new connection and logs in again, and so does the Java one.

## Client behaviour: streams

The bytes above are enough to talk to a node. These rules are what makes a client behave like `lotse-client` when nodes fail; they come from `crates/lotse-client/src/lib.rs` and the Java client follows them rule for rule.

### Errors of a call

Every request ends in one of four ways, and the retry rules depend on which:

| Outcome | Meaning | Retried by |
| --- | --- | --- |
| not sent | the connection was not there, or the node refused the whole frame without a reason | everything |
| lost | the request may have reached the node (timeout, broken connection after sending, an answer that does not decode) | idempotent produce, and requests that can be repeated |
| denied | the node requires authentication, or login failed | nothing: `Authentication` at once |
| refused | the node answered with `ERROR` and a reason (a role is missing) | nothing |

A connection that failed in any of these ways is closed and not trusted again. A failed login is remembered until a new connection logs in, so that a retry loop whose last attempt only saw a metadata lookup fail still reports the login. If an earlier attempt of a produce was lost and a later one is denied, the answer is `Unknown`, not `Authentication`: the first attempt may have been written.

The retry loop tries again until `retry_for` (default 30 s) has passed, pausing `backoff` (50 ms) between attempts. When time runs out it reports `GaveUp` if every attempt was certainly not carried out, and `Unknown` if one may have been.

### Routes

The client keeps `(stream, partition) -> {resource_id, leader, term}`, learnt from `Metadata`, and uses a route until a request fails. Routes do not expire by age; `metadata_max_age` applies to exchange bindings only.

- `Metadata` for a name a node does not know returns a resource with `UNKNOWN_RESOURCE`. The client then asks **every** broker it knows (sorted by address, the bootstrap list included) and keeps, per name, the resource with the **highest `resource_id`** (compared as `u64`): a name deleted and created again has a higher id, and a node that lags may still report the old one. A resource with an error names nothing and is never merged.
- `leader == 0` means unknown here, not that there is none. A partition created at runtime lives on some nodes only; the client asks the partition's voters, one after the other, for `Metadata` of that name and takes the first non-zero leader. If none knows, the attempt is retried.
- `NOT_LEADER` and `FENCED_LEADER_TERM` carry a leader hint (`leader`, `term`) when the node knows one. The client switches the route to the hint **only if** `hint.term >= route.term` (unsigned) and `hint.leader != route.leader`. Otherwise (no hint, an older term, the same leader) it drops the route, and the next attempt asks for metadata again. An older term comes from a node that lags; following it would send the client back to a leader that has been replaced.
- A leader whose node id has no known address drops the route and retries.
- `UNKNOWN_RESOURCE` in a `Produce` or `Fetch` drops the route too: the name may come back as a new resource.

### Idempotent produce

1. On the first produce the client asks any broker `FindCoordinator(key_type = PRODUCER, key = "")` and then the coordinator `InitProducer(txn_id = null)`. The answer is `producer_id` and `epoch`; both are kept for the client's session.
2. Per `(stream, partition)` the client counts the next sequence, starting at 0. It stamps the batch (`set_producer(id, epoch, sequence)`, which recomputes the CRC) and sends it.
3. **Produces to one partition go out one at a time.** A second batch is not sent until the first has an answer, so the leader sees sequences in order. Different partitions run in parallel.
4. Retrying a batch whose outcome is unknown (lost connection, `REQUEST_TIMED_OUT`) sends **the same bytes**: same id, epoch and sequence. The leader recognises a batch it has and answers with its offset.
5. On success the sequence advances by the batch's record count (a wrapping `i32`).
6. When the call ends with `Unknown`, `OUT_OF_ORDER_SEQUENCE` or `PRODUCER_FENCED`, the whole session is dropped; the next produce gets a new producer id and starts every partition at 0. A gap in the sequence would otherwise block everything behind it. Any other failure means the batch was not written, and its sequence is free.
7. `REQUEST_TIMED_OUT` means the quorum did not confirm in time: the batch may or may not be written.
8. Without idempotence nothing is stamped and an unknown outcome fails at once, because a retry could write the batch twice. Errors that prove the batch was not written (`NOT_LEADER`, a connection that was never opened) are still retried.

The timestamp of a record is the producer's. A stream's retention judges a batch's age by it, so a record stamped 0 is old at once and may be deleted at the next retention pass. The Rust helper `OutMessage` defaults to 0; the Java client uses the time of the call.

### Fetch

`Fetch` asks for `min_bytes = 1`, `max_bytes` 4 MiB, 1 MiB per partition, `isolation = 1` (committed) and `leader_term` = the route's term; the broker holds the request up to `max_wait_ms` when there is nothing to return. Keep `max_wait` well below the request timeout. The records are whole batches; a response may contain only a control batch, so a reader moves on by the offset after the last batch, not by the number of messages it got.

A read_committed reader drops what it must not show:

1. Sort the `aborted_txns` by first offset (as `u64`).
2. For each batch in order, every aborted entry whose first offset is at or below the batch's base offset puts its producer into the *skipping* set.
3. Control batches are never shown. A control batch that is an **abort** marker (type 0) takes its producer out of the skipping set; commit markers and the broker's own control entries do nothing.
4. A **transactional** data batch of a producer in the skipping set is dropped. A non-transactional batch of the same producer is kept.

The first record of a fresh partition is not at offset 0: the log begins with entries of the broker's own. Do not assume the offset of the first message.

`OFFSET_OUT_OF_RANGE` is not retried. The partition response's `log_start_offset` says where the log starts now; a reader that was overtaken by retention continues from there.

### ListOffsets

`ListOffsets` (key 5, ADR 0059) goes to the partition's leader, like `Fetch`, and answers per partition with an offset and a timestamp. `timestamp = -2` asks for the first offset still stored, `-1` for the offset after the last one the reader may see (the high watermark, or the last stable offset with `isolation = 1`), and a time in milliseconds since the epoch for the first offset of the first data batch whose newest record is at or after it; that batch's newest timestamp comes back too. If no batch qualifies, the answer is the latest offset with timestamp `-1`. Other negative timestamps are `INVALID_REQUEST`. A time lookup goes by batches: the batch it finds may begin with records older than the time asked for. A consumer group member uses it for a partition the group has committed no offset for, when it is told to start at the end or at a time.

## Client behaviour: queues

These rules come from `crates/lotse-client/src/consumer.rs` and the push handling in `lib.rs`. A queue is a resource of kind 1 with one partition, 0, so routes, leader hints and `Produce` work as for a stream; messages are produced with a normal `Produce` to partition 0.

### Subscribing

1. Look up the route of `(queue, 0)` and open (or reuse) the connection to its leader.
2. Send `Subscribe(queue_id, consumer_tag = "", credit_msgs, credit_bytes = 0xFFFFFFFF, exclusive = false)`. `exclusive` and `single_active` queues are refused with `INVALID_REQUEST`; do not offer them.
3. On `NONE`, register the `subscription_id` **on that connection**. Subscription ids are per node, and `Credit`, `Unsubscribe` and a requeueing `Nack` are honoured only on the connection that owns the subscription (`UNKNOWN_SUBSCRIPTION` otherwise). Pushes can arrive before the answer to `Subscribe` has been handled: the connection keeps them (see Connections) and hands them over on registration.
4. A retriable error (`NOT_LEADER`) follows the leader hint and retries. `UNKNOWN_RESOURCE` drops the route and fails the call. A lost answer drops the connection (a subscription the broker may have made ends with it), drops the route and retries.

### Receiving

A push holds one batch exactly as stored and `deliveries`, the offsets in it that are meant for this subscription, each with its delivery count. **Only records whose offset is in `deliveries` are the consumer's**; the rest of the batch belongs to others. Read the first batch of the push only.

The consumer hands out what it holds first and reads the next push (or the end of the subscription) only when its buffer is empty. A push with a non-zero `error` ends the subscription:

| Error | Then |
| --- | --- |
| `NOT_LEADER` | follow the leader hint (as for a request), subscribe again |
| `UNKNOWN_RESOURCE` | drop the route, subscribe again |
| `ACK_TIMED_OUT`, any other | subscribe again; the broker took the unacknowledged messages back |

A connection that breaks ends the subscription in the same way. The next read subscribes again, on the next call and not before. Messages received before the end but not yet handed out are still handed out; the broker meanwhile gives them to others, so they can arrive twice, and a delivery count that rose by one for each time says so. Counts are a lower bound (ADR 0031): after a failover they can start over.

### Credit

The consumer keeps two counters: messages handed out and bytes received (the length of each pushed batch, added when it arrives, saturating at `u32::MAX`). After handing out a message, if messages used is at least `ceil(credit / 2)`, it sends `Credit(subscription_id, add_msgs = used, add_bytes = bytes)` on the subscription's connection and sets both counters to 0. `Credit` is never answered; a lost one shows as a broken connection on the next read.

### Settling

- **Ack** goes to the queue's leader, on any connection, with ranges `[from, to)` built from the offsets: sorted as `u64`, duplicates dropped, neighbours joined. `txn_id` is null. An ack names an offset, not a delivery, so it settles a message that went to someone else meanwhile, and settling twice is harmless: a lost answer is retried, and so is `NOT_LEADER` after following the hint.
- **Nack without requeue** discards the message (or dead-letters it, if the queue's limits say so) and is sent like an ack.
- **Nack with requeue** is sent **on the subscription's own connection**: the broker takes back only what that connection holds. `delay_ms` is 0 (anything else is refused). If the subscription has ended or the connection fails, the message has gone back already, and the call does nothing. The Java client also does nothing for a message of an earlier subscription, rather than ask the new connection for something it does not hold.

### Closing

Send `Unsubscribe(subscription_id)` and then, for the messages received but not handed out, one `Nack(requeue = true)` with their offsets as ranges, both on the subscription's connection and without waiting for answers. Messages handed out stay with the consumer until it acknowledges them or the connection closes.

### Dead letters

A queue with limits (`max.deliveries`, `message.ttl.ms`) and a `dead.letter.queue` moves a message there as a one-record batch with the original key, value, headers and timestamp, and these headers (UTF-8 text, decimal numbers; they describe the latest move and replace those a message arrived with): `x-lotse-dead-letter-reason` (`max-deliveries` or `expired`), `-queue` (source queue name), `-queue-id`, `-offset`, `-deliveries` and `-floor` (the source's ack floor when it was sent). A client reads them like any header; the Java client offers them typed as `Delivery.deadLetter()`.

## Conformance vectors

`sdk/conformance/*` is written by `cargo run -p lotse-proto --features samples --example vectors` (`make sdk-vectors`). One vector per line, `#` for comments.

| File | Line |
| --- | --- |
| `requests.hex` | `label hex` (frame without the length prefix; correlation id is index + 1) |
| `responses.hex` | `label api_key hex` (correlation id is the index in the sample list) |
| `pushes.hex` | `label hex` |
| `batches.hex` | `label hex` |
| `batch-fields.txt`, `batch-records.txt` | the header fields and records of each batch, as the Rust parser reads them |
| `batch-ops.txt` | `op arguments input output`: an in-place edit and its result |
| `fetch-records.hex` | several batches in a row |
| `varints.txt`, `crc32c.txt`, `error-codes.txt` | fixed cases, including CRC32C of `123456789` = `E3069283` |
| `scram.txt` | `label user_hex password salt_hex iterations client_nonce server_nonce` and the five messages, `stored_key server_key`; the password is `=` and its hex |

A decoder passes when it reads every vector and writes it again to the same bytes, refuses each vector cut at every length and with one byte added, and agrees with the field files.

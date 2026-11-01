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

| Where the block is | Messages |
| --- | --- |
| Hint **is** the trailing block | `InitProducer`, `Subscribe`, `Outcome` (answer to `Ack`, `Nack`, `Unsubscribe`, `Bind`, `Unbind`), `Create` (`CreateStream`, `CreateQueue`, `DeclareExchange`), `Admin` (`DeleteResource`, `AlterConfigs`, `AlterUser`, `CreateTenant`, `DeleteTenant`, `AlterRoleBindings`, `AddNode`, `RemoveNode`) |
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

Codes 0 to 28, with names and retry classes, are in `ErrorCode.java` and in `conformance/error-codes.txt`. Retriable: `NOT_LEADER`, `FENCED_LEADER_TERM`, `NOT_ENOUGH_REPLICAS`, `REQUEST_TIMED_OUT`, `COORDINATOR_NOT_AVAILABLE`, `CONCURRENT_TRANSACTIONS`, `UNSTABLE_OFFSET_COMMIT`, `CONCURRENT_GROUP_CHANGE`. `REQUEST_TIMED_OUT` means the outcome is unknown: retry only what is idempotent. A code a client does not know is kept as it came and named `UNKNOWN`.

## The record batch

A batch is a 65-byte header and a block of records. All fixed-width header fields are little-endian.

```text
offset size field
     0    8 base_offset        stamped by the leader: Raft index of the first record, minus one
     8    4 batch_len          bytes after this field, so the batch is batch_len + 12 bytes
    12    8 term               stamped by the leader
    20    1 magic              1
    21    4 crc32c             CRC32C (Castagnoli) over bytes 25 up to the end of this batch
    25    2 attributes         compression(3) | transactional(1) | control(1) | kind(3), bit 0 first
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

Kinds: 0 data, 1 ack, 2 queue state, 3 timer, 4 dead letter, 5 retire. Compression: 0 none, 1 zstd, 2 lz4; only 0 is readable so far. Bits 8 to 15 must be zero.

A record, every `varint` ZigZag:

```text
length varint | attributes i8 (0) | ts_delta varint | offset_delta varint
| key_len varint (-1 = null) | key | value_len varint (-1 = null) | value
| header_count varint | [key_len varint, key, value_len varint, value]*
```

`ts_delta` is relative to `base_ts` and wraps like a signed 64-bit subtraction; `offset_delta` is the record's index and must run 0, 1, 2, and so on. A header key is never null.

**Checks a parser makes**, in this order: the buffer holds the 12-byte prefix; the length is at most 1 MiB (the length is a `u32`, so test it without overflow); it is at least 65; the whole batch is present; `magic` is 1; the CRC matches; the attributes are valid; `record_count` is not 0; `last_offset_delta == record_count - 1`; `max_ts >= base_ts`; a control batch holds exactly one record. Decoding the records adds: consecutive offset deltas, no bytes left inside a record, no bytes left after the last record, lengths and counts not negative, and every length within the record.

**Editing in place.** A client sets producer fields (`set_producer`), the transactional bit (`set_transactional`) and, for the leader's TTL stamping, the base timestamp, on the encoded bytes and recomputes the CRC over bytes 25 to the end of that batch. `BatchBuilder` takes the first record's timestamp as `base_ts` and the largest as `max_ts`; it does not read a clock.

**Several batches in a row** (the records of a `Fetch` partition, a log segment): read `batch_len`, add 12, cut, repeat.  A fetch response holds whole batches.

**Control batches.** One record. Key: `version u16 (0) | type u16`. Types 0 abort and 1 commit are transaction markers whose value is the coordinator epoch as a `u32`. Types 2 and 3 are Raft's own entries, 4 to 8 the coordinator and metadata groups' records. Consumers skip all of them. A `read_committed` consumer uses the fetch response's aborted-transaction list and the markers to hide aborted data.

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

A decoder passes when it reads every vector and writes it again to the same bytes, refuses each vector cut at every length and with one byte added, and agrees with the field files.

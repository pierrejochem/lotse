# 58. zstd and lz4 compression

- Status: Accepted
- Date: 2026-11-05

## Context

The batch format reserved three bits for the codec of the record block, zstd (1) and lz4 (2), and the design document said the header stays uncompressed so that Raft, the indexes and the transaction logic never decompress. Nothing implemented them: a compressed batch was refused when built and when read. Producers of repetitive data paid for every byte on the wire, in Raft and on disk.

## Decision

**zstd through `zstd` (libzstd), lz4 through `lz4_flex`** (maintainer's choice). libzstd is the reference implementation, built from source by `cc` and linked statically under musl; `lz4_flex` is pure Rust. The record block is one zstd frame, or one LZ4 frame (the frame format, not the bare block format), so that any implementation of either codec reads it. `BatchBuilder` compresses when its options name a codec, zstd at level 3. The header, the CRC (over the compressed bytes) and the 1 MiB limit on a batch stay as they are.

**A decompressed block is at most 16 MiB** (`MAX_RECORDS_LEN`). Readers stop decompressing past it and refuse the batch, so a small batch cannot make a node or a client allocate without bound; the builder refuses to make one.

**The leader decodes a compressed batch once when it is produced** (maintainer's choice). A block that does not decode, decodes to too much, or holds other records than the header says is refused with `CORRUPT_BATCH` and never written. Plain batches keep the checks they had. After that the batch is stored, replicated and served compressed: Raft, the log, retention, snapshots and `Fetch` never touch the block. Whatever reads single records (a consumer, every queue replica applying its log, dead-lettering, `lotse-client`) decompresses them through `Batch::records`, which handles every codec. Control batches are never compressed.

**Clients choose per batch.** `lotse-client` sends batches as the caller builds them; nothing compresses by default. A publish to an exchange is one message per batch and stays plain.

## Consequences

- Repetitive data costs a fraction on the wire, in Raft and on disk; a compressed batch may hold up to 16 MiB of records in its 1 MiB.
- Producing a compressed batch costs the leader one decompression; consumers and queue replicas decompress each time they read records.
- The build needs a C compiler for libzstd: the Alpine image has one, and cargo-zigbuild brings its own for the release targets.
- A consumer must read every codec its producers use. The Java SDK does not decompress yet (it uses no dependencies so far) and refuses compressed batches with a clear error; `sdk/conformance/compressed.hex` holds vectors for when it does.
- Tests: round trips of every codec in the batch property test; a compressed batch smaller than its plain twin and reading the same; a block that does not decode, one that decodes past 16 MiB (and a builder asked for one), and one with fewer records than its header are refused; a damaged compressed batch is refused by the leader and never written, and a good one is stored compressed. The client, queue, group and retention simulations produce a third of their batches plain, a third zstd and a third lz4. The leader's check and the size limit were broken on purpose and a test failed; bounding the decompression itself (rather than checking its size afterwards) only saves memory, which no test observes.

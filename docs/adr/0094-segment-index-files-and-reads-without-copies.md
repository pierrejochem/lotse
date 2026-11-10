# 94. Segment index files and reads without copies

- Status: Accepted
- Date: 2026-11-10

## Context
The log kept its sparse offset index in memory only. Opening a log read and checked every batch of every segment to rebuild it, so a node's start-up time grew with the data on disk. `ListOffsets` by time (ADR 0059) skipped segments whose newest record was older, but then read batch headers from the start of the first segment that might hold the time. `Log::read` read each batch twice (its length, then the batch) and copied every batch into a new buffer, and `Fetch` copied once more into the response frame; follower replication and queue delivery read the same way. The aborted transaction ranges of a partition stayed in its state and snapshots for good, also once retention had deleted their data.

The known gaps named Kafka's three files (`.index`, `.timeindex`, `.txnindex`) and `sendfile` for `Fetch`. Every client connection is TLS 1.3, encrypted in the process by rustls (ADR 0037), and `sendfile` can only send bytes as they are on disk.

Four decisions, each the maintainer's choice of the recommended option.

## Decision
**One index file per sealed segment.** `<base>.index` holds the segment's sparse index: for each entry a batch's first offset, its byte position, and the newest timestamp of the batches before it in the segment, which only grows, so the same entries answer a search by time. There is no separate `.timeindex`. The header names the segment's length, its next offset, its newest timestamp, and the position and stored CRC of its last batch; a CRC32C covers the file. The file is written when a segment is sealed (synced first, then indexed, before the next segment exists), after a compaction rewrite, and on open for a sealed segment without one; it goes to `<base>.index.tmp`, is synced and renamed into place. It is removed before its segment changes: Raft truncation, retention, a rewrite (before the rename), a reset. The active segment has none; one a crash left beside it is removed on open.

**Opening trusts a matching index file.** A sealed segment is taken from its file if the file decodes, its checksum holds, the segment has the recorded length, and at the recorded position there is a valid batch with the recorded CRC that ends at the recorded offset (offsets lie outside a batch's CRC and are compared on their own). Otherwise the segment is scanned as before and the file written again. Opening reads the active segment and one batch of each sealed one. Damage inside a sealed segment is then found when the batch is read, whose CRC every read checks, not at start-up.

**`ListOffsets` keeps answering with a batch.** Within the first segment whose newest record is at or after the time, the search starts at the last index entry whose preceding batches are all older, so it reads at most one index interval (4 KiB) of batches it skips. The answer stays the first offset of the first batch whose newest record is at or after the time: clients read and hand out whole batches, so an offset inside a batch would show the earlier records anyway.

**No `sendfile`; fewer copies.** `Log::read` finds the range from the index and the batch headers and reads it in one call where a read-ahead (one index interval plus the bytes asked for) covers it, returning a slice of that buffer with every batch checked. `Fetch`, replication and queue delivery pass it on as it is; the response frame copies it once, and TLS encrypts it. Kernel TLS, which would let `sendfile` work, is not used: Linux only, a new dependency, a file-region send in the `Net` seam and its simulated equivalent, to save one copy.

**Aborted ranges below the log start are forgotten; no `.txnindex`.** When retention cuts the log, when a snapshot restarts it and on open, a partition drops the aborted transactions whose marker lies below the log's first offset. Their data lies below it too, so no fetch or compaction meets it. A transaction whose data reaches the log start stays. The list then grows with the data kept, not with the partition's history.

## Consequences
A node starts in time that grows with the number of segments, not with their bytes. Index files are written once per segment, about 24 bytes per 4 KiB of data. A sealed segment damaged after it was indexed is reported when read (`Corrupt`), and a node no longer refuses to start over it; with its index file removed, opening finds it as before.

A stale index file can only be believed if it records the segment's exact length and its last batch, CRC included; one from a different history of the same length and the same last batch would still be taken, which is why a segment loses its index file before it changes. Tests check a stale file of the same shape with other contents, a stale file of a shorter segment, and a power loss at every step of rolling and indexing; breaking the length, CRC or last-batch check makes one fail.

`ListOffsets` by time reads a bounded number of batches before its answer. It still answers with a batch, not a record.

`Fetch` still copies the data once in user space, into the frame. Removing that copy would take frames of several parts through `Conn::send`; removing the TLS copy would take kernel TLS. Both stay open.

Aborted ranges differ between replicas below their own log starts, which no read can see. Snapshots carry only the ranges at or after the log start of the replica that took them.

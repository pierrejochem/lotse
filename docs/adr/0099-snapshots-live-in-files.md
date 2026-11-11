# 99. Snapshots live in files

- Status: Accepted
- Date: 2026-11-11

## Context

Since ADR 0053 a snapshot travels in chunks, but the follower's core collected them in a buffer and installed the whole snapshot from memory. Besides that buffer, a replica kept copies of snapshot data in memory: its latest snapshot (to send to followers), every snapshot it had replaced while a transfer of it was under way (`OlderSnapshots`), and one snapshot per segment boundary above its last cut, where retention may cut later (ADR 0046). A stream that keeps many segments therefore held as many copies of its encoded state, and a follower receiving a snapshot held the bytes it had received on top of the state it would replace. The README listed this as one of two places where memory grows with data.

The state itself (producer state, transactions, a queue's ack index, the metadata) lives in memory and is restored from a snapshot whole; that does not change here. What changes is the copies of its encoding.

## Decision

**The core keeps no snapshot bytes, and a replica keeps every snapshot in a file of its directory, read a chunk at a time when it is sent.**

- The core hands each chunk that arrives in order to the driver in `Ready::snapshot_chunks` (`SnapshotChunk { meta, offset, data }`) and keeps only the snapshot's meta and how many bytes have arrived. Offset 0 starts a snapshot: the driver drops what an earlier transfer left. With the last chunk, `Ready::snapshot` names the snapshot (its `SnapshotMeta`) to install from what the driver kept. The rules of ADR 0053 (in order, a repeat or a gap only answered, another snapshot at offset 0 starts over, installed only when complete) stay in the core.
- Chunks come first in a `Ready`. They are not synced: a node that crashes forgets the transfer in its core, and the leader starts over (ADR 0053). Installing stays durable before anything is sent, as before.
- The replica writes chunks to `snapshot.incoming`, checks that each lands at its offset, and installs by reading the file once, writing `snapshot` atomically and restoring the state. When the core gives a transfer up (the node campaigns or leads, or the log catches up), the replica removes the file.
- A replica keeps its snapshots as files: `snapshot` (the latest, as before), `snapshot.<index>.boundary` (taken where a segment begins; written synced, so retention makes it the snapshot with one atomic rename), and `snapshot.<index>.sent` (a replaced snapshot a transfer still needs; copied a megabyte at a time before `snapshot` is replaced, not synced). In memory it keeps only each one's meta, the file name and where its data lies. Boundary files at or below a cut are removed when retention cuts, all of them when a snapshot is installed, and a `.sent` copy when the next replacement finds no transfer needing it.
- Sending reads the chunk from the file. The core's choice of which snapshot to send stays in the core: `Message::snapshot_chunk_source` picks the snapshot and the offset as `attach_snapshot_chunk` did, the driver reads, and `Message::set_snapshot_chunk` puts the chunk in. `OlderSnapshots` is generic over how the driver holds a snapshot and returns what it drops, so the driver can remove files. `attach_snapshot_chunk` remains for drivers that keep bytes in memory (`testing::MemNode`).
- On open a replica removes every snapshot file but `snapshot`: boundaries are taken again as the log is applied, and transfers in either direction start over.

## Consequences

- A replica holds no copy of its snapshots' data in memory beside the state, however many boundaries it keeps, and a follower holds none of a snapshot it is receiving. Installing still reads the snapshot once and restores the state from it, so a snapshot must fit in memory at that moment, as the state does.
- Disk holds a file per boundary snapshot and per snapshot in transfer; they are of the state's size, and writing a boundary now costs a synced write where it cost a copy in memory.
- Sending a chunk opens and reads the file; a chunk is 1 MiB at most, so this is small next to the network.
- New rules are mutation-checked by unit tests: the core hands chunks over once and in order (`lotse-raft`), and a replica truncates on a new start, removes abandoned chunks and stray files, reads data at the right position, copies a replaced snapshot a transfer needs and removes copies and boundaries no longer needed (`lotse-server` replica tests).

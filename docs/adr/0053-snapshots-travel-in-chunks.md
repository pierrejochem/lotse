# 53. Snapshots travel in chunks

- Status: Accepted; in part superseded by [ADR 0099](0099-snapshots-live-in-files.md) (the core hands chunks to the driver, which keeps them and every snapshot in files)
- Date: 2026-11-01

## Context

A snapshot went to a lagging follower in one `Snapshot` message, so it had to fit one frame (`MAX_FRAME`, 16 MiB; ADR 0025). The state of a busy replica (producer state, transactions, the ack index of a queue, the whole state of the metadata group) can be larger, and then a follower could never catch up. A single message also restarts from nothing whenever the connection breaks.

## Decision

**The driver cuts the snapshot into chunks; the core keeps the offsets and the follower reassembles.** The core stays free of snapshot bytes.

- `Message::Snapshot` carries `offset` (where its bytes begin in the snapshot's data), `data` (one chunk) and `done` (the last chunk). The core sets `offset` and leaves `data` empty; the driver calls `Message::attach_snapshot_chunk` with its latest snapshot and a largest chunk size (`Replica::max_snapshot_chunk`, 1 MiB by default). If the driver's snapshot is newer than the one the core meant, the offset becomes 0.
- One chunk is in flight per follower. The follower answers every chunk that does not complete the snapshot with `AppendResult::SnapshotChunk { index, next }`: the offset it wants next. The leader keeps `(index, next)` per follower, sends from there, and ignores answers about another snapshot than its own. The answer to the last chunk is `Accepted { last: index }`, as before. A chunk that is lost is sent again after the usual in-flight timeout, from the follower's `next`.
- A transfer finishes with the snapshot it began with. The leader remembers per follower the snapshot's meta and the offset asked for; if it takes a newer snapshot meanwhile it keeps sending the old one, and sends the newer one only after the follower has installed the old. Otherwise a leader that snapshots more often than a transfer lasts (the replica simulation compacts every few dozen steps) would never get one through. The driver keeps the data of a replaced snapshot for as long as the core reports its index in `snapshots_in_transfer` (`OlderSnapshots`); if it no longer has it, it sends its latest from offset 0 and the follower starts over.
- The follower takes chunks strictly in order. A chunk at the offset it has is appended; a repeat or a gap only gets an answer with its `next`; offset 0 of another snapshot (other index, term or membership) drops what it holds and starts over; a chunk of a snapshot it knows nothing about, after a restart for instance, gets `next = 0`. Chunks of one snapshot are the same bytes whoever sends them, so a new leader or a restarted leader continues a transfer.
- The snapshot goes into `Ready::snapshot` only with the last chunk. Until then the follower's log, commit index and state are untouched; a transfer that breaks off installs nothing. The partial data is memory only: it is dropped when the node restarts, campaigns or leads, or when the log catches up on its own (commit reaches the snapshot's index).
- The wire format of `Snapshot` gets `offset` and `done`, and `AppendReply` the tag 2 (`SnapshotChunk`). The node-to-node format has no version negotiation yet, so nodes of the two formats do not mix; this is as before for every format change in the first cuts.

## Consequences

- A snapshot may be as large as the follower's memory; it no longer has to fit a frame. A chunk of 1 MiB leaves room in a frame for the other groups' messages in the same round.
- A transfer costs one round trip per chunk. Pipelining chunks would be faster over a long link and is left for later.
- The follower buffers the whole snapshot in memory before it is written; spilling it to a file is possible later without changing the messages.
- New safety rules (in order, only complete snapshots install, a different snapshot starts over, the leader's offset follows the follower's answer) are mutation-checked by unit tests and the Raft and replica simulations, which now cut snapshots into chunks of a few dozen to a few hundred bytes.

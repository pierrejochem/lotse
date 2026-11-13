# 103. Snapshot chunks travel in a window

- Status: Accepted
- Date: 2026-11-12

## Context

Since ADR 0053 a snapshot travels in chunks of at most 1 MiB, one on its way per follower: the leader sends the next only when the follower's answer to the last has arrived. A snapshot therefore takes one round trip per megabyte. Across zones that leaves the link mostly idle: at 2 ms a round trip, a 1 GiB snapshot needs at least two seconds of waiting alone.

## Decision

**A leader keeps up to `snapshot_window` chunks of a snapshot on their way to a follower (default 4), all of `snapshot_chunk_bytes` (default 1 MiB), and goes back to the follower's offset once when a chunk is lost.**

- The chunk size moves from the drivers into the core's `Config` (`snapshot_chunk_bytes`), with `snapshot_window` next to it, because the core now computes the offsets of the chunks ahead. Drivers read it with `RaftGroup::snapshot_chunk_bytes` and fill chunks of exactly that size, so the offsets match. `testing::MemNode` and `Replica` lose their own `max_snapshot_chunk`.
- Each follower's progress keeps how far chunks have been sent. Sending fills the window from the follower's offset: chunks at the offset, the offset plus one chunk, and so on, up to `snapshot_window` of them. The core does not know the snapshot's length. Chunks past the end come back from the driver empty and marked last, and a follower that has installed the snapshot accepts them. At most `snapshot_window - 1` such empty messages go out per transfer.
- The follower is unchanged (ADR 0053, ADR 0099). It takes chunks only in order and answers each with where it stands.
- An answer that moves the follower on moves the window. An answer that does not means a chunk was lost or came twice, and the leader goes back to the follower's offset once. Further answers like it, from chunks sent before, are ignored until the follower moves on, so a lost chunk is sent again once and not once per later chunk. If the chunk sent again is lost too, the in-flight timeout goes back again.
- An answer that says the follower has less than it said before moves nothing at once. With several chunks on their way, answers arrive late and out of order, and one sent before a newer answer says less. Going back for each of them made the answers to what was sent again go back in turn: the Raft simulation saw the messages multiply until memory ran out (seed 459). The leader notes the offset of the latest answer. When the chunks in flight time out, it goes back to that offset if it is lower than what it has. A follower that really restarted and lost the chunks it had says so in every answer, and is served from there after the timeout.

## Consequences

- A snapshot takes about one round trip per `snapshot_window` chunks instead of one per chunk. A follower may receive up to four chunks it then drops, after a loss or a restart.
- Memory on the follower does not grow: chunks go to its file as they arrive in order (ADR 0099). On the leader, up to `snapshot_window` chunks are read and on the wire at once.
- Tests:
  - a unit test checks that three chunks leave before any answer, that a lost chunk is sent again once, and that no chunk leaves more than the window ahead;
  - a unit test feeds the leader late and repeated answers: a lower one sends nothing, one at the follower's offset sends the window again once, and a follower that started over is served from its offset after the timeout. Going back for a lower answer, not going back on the timeout, and not sending again for a repeated answer each make it fail;
  - the Raft simulation picks the window between one and six per node, as does the replica simulation (`tests/cluster.rs`);
  - one older test keeps a window of one, because it counts how often a transfer starts from offset 0.

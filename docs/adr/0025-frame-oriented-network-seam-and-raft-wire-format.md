# 25. Frame-oriented network seam and Raft wire format

- Status: Accepted
- Date: 2026-10-27

## Context

Nodes need to exchange Raft messages, in production over TCP and in simulation over a network the test controls. [ADR 0006](0006-tokio-behind-a-runtime-seam.md) puts every contact with the outside world behind `lotse-rt`, and [ADR 0002](0002-multi-raft-with-batching-per-node-pair.md) asks for one stream of messages per node pair, not per group. The seam had no network yet.

## Decision

`lotse-rt` gets `Net`, `Listener` and `Conn`. A connection carries **whole frames** (at most `MAX_FRAME`, 16 MiB), in order, until it breaks; after a break nothing more arrives and the sender reconnects. Byte streams, partial reads and framing stay out of everything above the seam.

- `lotse-rt-tokio` implements it over TCP with a 4-byte big-endian length prefix and `TCP_NODELAY`. It is the one place that reads the real clock and OS randomness.
- `lotse-sim` implements it as `SimNet`: per-connection pipes with seeded latency that never reorders, and faults the test injects (block a pair of hosts, isolate a host, heal). A fault breaks the connection and drops what was in flight, as a real reset would.

The node-to-node format (`lotse-server::wire`) is one frame per destination and round, holding every message for every group: `version from count` followed by `group kind body`. Log entries travel as the record batches stored in the log (stamped with offset and term), so a leader passes on what it read without re-encoding and a follower parses them in place. Decoding refuses unknown versions and kinds, counts the frame cannot hold and trailing bytes; a frame that does not decode drops the connection.

Per peer, a `Node` queues at most 256 frames while the peer is unreachable and drops the oldest: Raft resends what still matters, and the queue must not grow without bound.

## Consequences

- The same `Node` runs unchanged on Tokio and in the simulation; the node simulation exercises the real main loop, senders, readers and codec.
- A snapshot must fit in one frame until chunked transfer exists.
- The format carries a version byte, so a later change can be rolled out node by node.
- No TLS or authentication between nodes yet.

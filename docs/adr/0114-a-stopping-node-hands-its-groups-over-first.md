# 114. A stopping node hands its groups over first

- Status: Accepted
- Date: 2026-11-16

## Context

On SIGTERM or Ctrl-C, `lotse run` stopped its node at once. Every group the node led then went through a failover: its followers waited an election timeout before one campaigned, and clients waited too, then met `NOT_LEADER` or a broken connection and resumed. An upgrade, a change to the cluster file or a host reboot is a planned stop, and the most common reason a leader goes away. Yet each one cost a failover for every group the node led.

Kafka and RabbitMQ both move leadership off a node before a planned stop. Kafka's controlled shutdown (`controlled.shutdown.enable`, on by default) moves every partition the broker leads to another replica before the broker exits. RabbitMQ's maintenance mode (`rabbitmq-upgrade drain`) moves quorum queue leaders off the node. A rolling restart then causes only planned moves.

Lotse already had the parts: a Raft leader transfer, leader hints in `NOT_LEADER` (as in Kafka's KIP-951), and consumers that keep their messages across a move (ADR 0109, ADR 0112).

## Decision

**A stopping node leaves first** (maintainer's choice: "do it like Kafka and RabbitMQ"). `NodeHandle::leave(within)` asks the node to stop once it has handed over the groups it leads. `lotse run` calls it on SIGTERM or Ctrl-C, with 30 s. That is within systemd's `TimeoutStopSec` of 60 s.

- **Handing over:** while leaving, the node asks, for every group it leads, another voter to take over with a Raft leader transfer. It asks those that hold the most of the log first, the lower id among equals. Every 50 ticks while it still leads the group it asks the next one, so a voter that is down is passed over. During a change of voters the transfer is refused and asked again.
- **Stopping:** the node stops as soon as it leads no group that has another voter, or once `within` has passed, whichever comes first. A group with a single voter cannot be handed over and does not hold the stop back. Neither does a group whose other voters are all down: its leader loses its quorum and steps down within an election timeout (check-quorum), so the last node of a cluster stops without waiting out `within`. `lotse run` stops the node outright 5 s after `within` if it has still not stopped.
- **No group comes back:** while leaving, the node drops every `TimeoutNow` it receives, so it takes no group over, also not one pinned to it (ADR 0065).
- **When it returns:** the restarted node gets its groups back by the hand-back of ADR 0113, a minute after it has caught up.

`NodeHandle::stop` still stops at once. A crash is still a failover.

Rejected options:

- **Kafka's imbalance threshold as well**, handing back only while a node leads more than 10% over its share. RabbitMQ has no automatic hand-back. Lotse's hand-back of ADR 0113 already waits until a node is steady. The threshold can be decided separately.
- **Telling subscribers before a planned move**, so that they move before they meet `NOT_LEADER`. It needs a new push in the protocol and in all four clients, and saves about one round trip per move.
- **Recording the move as a limit.** Neither Kafka nor RabbitMQ accepts it for planned stops.

## Consequences

- A rolling restart moves each group's leadership with a transfer, a round trip or two, instead of an election timeout per stopped node. Clients meet `NOT_LEADER` with the new leader's address and resume at once.
- Stopping a node takes as long as its handovers, at most 30 s with `lotse run`. A supervisor's stop timeout must allow that; the systemd unit's 60 s does.
- A node cut off from the other voters gives up its groups within an election timeout (check-quorum) and stops. Stopping a whole cluster node by node therefore takes no deadline per node either.
- Tests:
  - A unit test checks whom a leaving leader asks to take over.
  - The metadata simulation `a_leaving_node_hands_its_groups_over_before_it_stops` asks a stream's leader to leave and requires that, when its task ends, every group it led already has a leader on another node, well within an election timeout.
  - The metadata simulation `a_leaving_node_passes_over_a_voter_that_is_down` crashes the voter asked first, then requires the leader to stop within 5 s, and the last node too.
- Mutation-checked:
  - a node that stops without handing over fails the first simulation;
  - asking the same voter every time fails the second.
- Not covered by a test: dropping `TimeoutNow`. In the simulation a leaving node stops before a pinned group could be handed back to it.

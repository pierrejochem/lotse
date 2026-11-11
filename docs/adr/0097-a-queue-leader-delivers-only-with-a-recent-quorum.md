# 97. A queue leader delivers only with a recent quorum

- Status: Accepted
- Date: 2026-11-11

## Context
Queue deliveries are the leader's: it pushes messages within each subscription's credit once it has applied an entry of its own term (ADR 0031), and it alone decides which subscription of a single active consumer queue is active (ADR 0076). A leader cut off from the other nodes but not from its clients keeps its role until check-quorum steps it down, up to two election timeouts. Meanwhile the other nodes may elect a new leader, which delivers the same messages. The old leader went on handing out messages to its consumers and to any consumer that subscribed there, and could hand a single active consumer over; the acks of those deliveries fail, but the messages were out twice at once, and for a single active consumer there were two active consumers. The README listed this as a weakness.

ADR 0095 added `Raft::leads_with_quorum`: whether a quorum answered a round the leader sent less than an election timeout ago. It was used for reports only.

## Decision
**A queue leader that no quorum answered within an election timeout hands out nothing.** In each round of serving its queues, a leader skips the hand-over of a single active consumer, reading priority levels and delivering, for every queue whose group does not lead with a quorum (`leads_with_quorum`). It still sweeps closed connections, ends subscriptions past their ack timeout and writes what it can; writes need a quorum anyway. Subscriptions are not ended: when a quorum answers again, delivery goes on; when check-quorum steps the leader down, they end with `NOT_LEADER` as before.

The flag stays a heuristic, not a safety rule: reads and writes still go through the term and `read_index`, and an ack is still only good once committed.

## Consequences
A new leader is elected only after the followers have heard nothing from the old one for an election timeout of their own, at least as long as the window the old leader counts from the last round they answered. With ticks at the same pace, the old leader stops handing out before another can lead. Ticks follow each node's loop, not a shared clock, so a node that ticks more slowly may still overlap a little; the overlap is now that skew, not up to two election timeouts.

A leader whose followers answer late (an overloaded node, a slow disk) pauses its deliveries while no answer is newer than an election timeout, though it stays leader. That is the same bound check-quorum uses, measured from when the round was sent.

A simulation cuts a queue's leader off from the other nodes, waits until it no longer reports the queue as its own, and subscribes there from a client that reaches only that node: nothing may come. Without the rule it hands out the first message at once.

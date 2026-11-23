# 139. Nodes log their groups' elections and stalls

- Status: Accepted
- Date: 2026-11-23

## Context
Tests against real nodes fail now and then on CI runners in a way no simulation shows: the console's `CreateQueue` is answered `NOT_LEADER` for five seconds, a C# queue consumer gets nothing for 30 s, a requeue reports 0 messages instead of 1. Each looks like a group without a leader for far longer than an election takes (`election_ticks` 10 at 10 ms, so 100 to 200 ms). The likeliest cause is the node's loop falling behind on a loaded runner, through slow fsyncs or a starved CPU, so ticks come late, followers time out and leaders step down for want of a quorum. Nothing in a node's log said so: it logged neither elections nor how late its loop was, so a failed run left no trace of where the time went.

## Decision
A node logs what its groups and its loop do, for anyone who asks with `LOTSE_LOG`:

- Under the target `lotse::raft`, at `debug`, every change of a group's role, term or leader as the node's loop sees it after a tick or after the messages of one sender: node, group, the new and the old role, term and leader, the sender when a message caused it, and a reason derived from what changed and what ran, for instance "stepped down: no quorum heard within an election timeout (check-quorum)", "election timeout: no leader heard, asking for pre-votes", "stepped down: a higher term" or "election won". A group's first sight is not a change, and a group the node no longer runs is forgotten.
- Under the target `lotse::stall`, a tick that comes late and a round of persisting and sending that takes long: at `debug` from two ticks, at `warn` from half an election timeout, the delay in milliseconds.

The changes are `debug`, not `info`, because a cluster of thousands of groups would fill an `info` log at every start; a stall of half an election timeout is a `warn` because it is what makes a healthy cluster elect. CI sets `LOTSE_LOG=info,lotse::raft=debug,lotse::stall=debug` for every job; the console's test against real nodes logs through the test harness, the SDK tests' nodes write their logs under a known `TMPDIR`, and a failed SDK job prints the lines of those two targets.

## Consequences
The next failure of this kind shows which group lost its leader, on which node, why, and whether the node's loop was late before it; then the cause can be fixed instead of guessed. Nothing changes for a node at the default `info` but `warn` lines for stalls long enough to cause elections. Observing costs one comparison per group per tick and per stepped group, which is small next to the tick itself. The reason is a reading of the transition, not a report from the Raft core; a transition the table does not name says "changed".

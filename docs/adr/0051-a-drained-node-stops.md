# 51. A drained node stops

- Status: Accepted; the stop of a node the cluster forgot is superseded by [ADR 0105](0105-a-forgotten-node-stops-without-seeing-its-drain.md)
- Date: 2026-10-31

## Context

A node removed with `lotse node remove` (ADR 0048) was drained and then forgotten by the cluster, but its process kept running until someone switched it off. Its metadata replica stopped receiving entries when it was forgotten, so it went on answering clients with the metadata it last knew: streams that had since been deleted, voters that had since moved, a node list without the newest nodes. A client that reached it first could act on that for as long as it ran.

## Decision

**A drained node stops serving and exits** (maintainer's choice). Once a node's own metadata state shows it draining, with no group counting it among its voters and no move off it still under way (the same test the metadata leader uses before it forgets the node), the node stops: it closes its client and node listeners and its groups, and `lotse run` exits with status 0, so a supervisor such as systemd (`Restart=on-failure`) does not start it again. It logs that it was drained.

**A draining node the cluster forgot while it was cut off stops too.** The node is a learner of the metadata group until it is forgotten, so it normally sees the moment above. If it was down or cut off then, it never receives another entry. So a draining node asks the metadata leader for the node list every 100 ticks (`DescribeNodes`, through a node it lists, as a lagging node does in ADR 0050); when the list leaves it out, the cluster forgot it, and as ids are never used twice it cannot be back: it stops. A node that is not draining never stops for being unlisted: a joining node is not listed until it has been added.

## Consequences

- Clients no longer get stale metadata from a removed node once it has stopped; their connections to it fail and they go to the other nodes.
- A node drained by mistake cannot simply be started again: it exits at once. It has to join as a new node with a new id and certificate.
- A drained node that cannot reach any node it lists keeps running until it can, or until it is switched off.
- Tests: unit tests of when a node is drained and when it stops (not while it holds a group or a move off it is under way; not when it is unlisted but not draining); the membership simulation checks that the removed node stops by itself after the drain, even if it was down or cut off when the drain finished; the binary's test checks that the drained first node's `lotse run` ends. Breaking the guard that only a draining node stops makes a unit test fail; breaking the unlisted check makes the simulation fail. The two ways to notice the end of a drain cover each other in the simulation: either alone stops the node, as the cluster always forgets it in the end.

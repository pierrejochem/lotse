# 23. Persist the hard state after the log

- Status: Accepted (lands in code with the `lotse-server` replica, in progress)
- Date: 2026-10-27

## Context

The replica crash simulation found that a follower which persisted its hard state (commit 34) and then lost power before installing the snapshot that justified it restarted with a divergent log and applied 16 entries that were never committed.

## Decision

A `Ready` is persisted in this order: install the snapshot, truncate, append and sync entries, then persist the hard state, then apply and send. The commit index in the hard state therefore only ever refers to log contents already on disk. On restart the term is raised to the log's last term if the hard state is older (no vote in that term was sent, since messages go out only after the hard state is durable). A driver writing log and hard state in one atomic write may ignore the order.

## Consequences
- Crash recovery never applies uncommitted entries.
- Term and vote still become durable before any message leaves the node.

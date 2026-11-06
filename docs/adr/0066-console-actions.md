# 66. Console actions: purge, requeue, move leader

- Status: Accepted
- Date: 2026-11-06

## Context

The console's design names three actions that waited for broker APIs: purging a queue, requeuing dead letters, and moving a leader. ADR 0064 and ADR 0065 built the APIs. The design also says that destructive actions in production ask for the resource's name to be typed. The console has no notion of which cluster is production. The native console also refused to create queues with a dead-letter queue, a TTL or a delivery limit, which the broker has supported since ADR 0055, so nothing it created could be requeued.

## Decision

**Three routes, acting as the user.**
- `POST /clusters/{cluster}/queues/{name}/purge` (body `{confirm}`) purges a queue.
- `POST /clusters/{cluster}/queues/{name}/requeue` requeues a dead-letter queue.
- `POST /clusters/{cluster}/leaders` (body `{resource, partition, node}`, `node` null to unpin) pins or unpins a leader.

Each answers `Done` with a message and the equivalent CLI command, like the other writes. Every call goes through the signed-in user's client, so the broker's roles decide (tenant admin for purge and requeue, cluster admin for moving leaders), and a refusal comes back as 403 with the broker's reason.

**Purge always asks for the name.** With no way to tell production from anything else, the purge dialog asks for the queue's name on every cluster. The API checks it too, so a script cannot purge by mistake either. Requeue and moving a leader undo nothing and only ask for a confirmation.

**Where the buttons are.**
- The queue page has Purge beside its title.
- The dead-letter section of a queue that has one has Requeue all, acting on its dead-letter queue.
- Each partition of a stream, and each level of a queue, has Move next to its leader; the dialog offers the other replicas and Unpin.

The native console now creates queues with a dead-letter queue (creating it first when asked), a message TTL and a delivery limit, and shows these settings from `DescribeConfigs`. Priority levels and single active consumer are still refused.

## Consequences

- Operators can do the three actions from the console. The demo cluster plays them out in memory: purged queues show no ready messages, requeued dead letters go, pinned leaders show where they were put.
- The console does not show whether a partition is pinned, because the broker does not report pins yet; the leader it shows is the effect.
- A queue page offers Purge even to a user whose roles do not allow it; the broker refuses, and the console shows why.
- Tests:
  - the console against three real nodes: creating a queue with a dead-letter queue and seeing its settings; a purge refused without the right name and then emptying the queue; a message dead-lettered and requeued; a leader pinned to another node, moving there, and unpinned; a node that does not exist refused;
  - the demo cluster's actions;
  - the grouping of counts in messages.
  
  The screens were checked in a browser, at desktop and phone widths.

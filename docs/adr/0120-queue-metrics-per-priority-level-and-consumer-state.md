# 120. Queue metrics per priority level and consumer state

- Status: Accepted
- Date: 2026-11-18

## Context

`DescribeMetrics` reported a queue's ready, in-flight and transactional counts and its subscriptions (ADR 0036). It left out two things the README listed as gaps:

- On a queue with `single.active.consumer` (ADR 0076), which subscription gets the messages and which ones wait.
- On a queue with `priority.levels` (ADR 0078), how many messages wait at each level.

Only the leader knows either. The dispatcher picks the active subscription, and the leader reads each message's level from its `x-lotse-priority` header as data becomes deliverable. A new leader reads the backlog for levels, 1 MiB a round, so for a while some ready messages have no known level. The console's queue page already had places for both, filled with placeholders for a real cluster.

## Decision

**The queue's leader reports each subscription's state and, on a queue with priority levels, its ready messages per level, with those whose level it has not read yet counted apart. `/metrics`, the SDKs and the console show them** (maintainer's choices: a state per consumer; ready per level plus the unread; every surface).

- **Consumer state.** Each subscription in `DescribeMetrics` gets a state:
  - `shared`: the queue has no single active consumer.
  - `active`: the single active consumer.
  - `standby`: waits for its turn.

  During a handover nobody is active until every message out has come back (ADR 0076), so then every subscription is `standby`.
- **Per level.** On a queue with priority levels:
  - `ready_by_level` gives the ready messages of each level, level 0 first.
  - `ready_level_unread` gives the ready messages at or past where the leader has read levels.

  Ready means what `DescribeMetrics` already counted as ready: unsettled, below what may be delivered, not in flight. So the levels and the unread count add up to `ready`. A queue nobody subscribes to has no reader of levels, so all of its ready messages count as unread. A message whose header does not parse, or names no level, is level 0; one above the highest is the highest, as when delivering.
- **Wire.** The fields go into tag 3 of the `DescribeMetrics` answer, `queue_details`, which earlier readers skip. It holds an array of entries, each with:
  - the queue's id (`u64`);
  - the ready messages per level (array of `u64`);
  - the unread count (`u64`);
  - each subscription's id (`u32`) and state code (`u8`: 0 shared, 1 active, 2 standby; an unknown code reads as shared).

  Only queues with levels or a subscription that is not shared are listed. Unlike tags 1 and 2, every principal that may see the queue gets it.
- **`/metrics`.** Three new families:
  - `lotse_queue_consumers_in_state{state}` for every queue, with each state;
  - `lotse_queue_ready_by_level_messages{level}` for each level of a queue with levels;
  - `lotse_queue_ready_level_unread_messages` for each queue with levels.
- **SDKs and console.** The Java, C# and Node.js SDKs decode the tag into `QueueMetrics.readyByLevel`, `readyLevelUnread` and `ConsumerMetrics.state`. The console's queue page shows each level's ready messages, highest first, and a note while the leader still reads levels. Each consumer's chip says shared, active or standby. All levels share one log and one leader, so the page names the leader once and pins partition 0.

Rejected options:

- **The active subscription's id on the queue.** It is smaller, but a reader that does not know the queue's setting cannot tell plain consumers from standbys.
- **Counting unread messages as level 0.** It is simpler, but after a failover level 0 would look too large until the backlog is read.
- **Ready and in-flight counts per level.** That doubles the data per queue, and in flight per level is rarely what an operator acts on.
- **New fields in the queue entry instead of a tag.** Earlier readers would fail to decode the answer.

## Consequences

- An operator sees which consumer of a single active consumer queue gets the messages, and whether high-priority messages are waiting, in Prometheus and in the console.
- The answer grows by about 10 bytes per subscription and 8 bytes per level, only for queues that use either feature.
- The counts are the leader's. While a new leader reads the backlog, `ready_level_unread` falls to 0 and the levels fill in.
- The golden vectors' metrics answer carries the tag, and each SDK's conformance tests read it back.
- Tests:
  - A `lotse-queue` unit test counts ready messages per level: in flight, settled before and after a sweep, below the limit, nothing read yet.
  - A server unit test renders the new families.
  - The protocol tests decode the smallest entries and round-trip the full answer.
  - A queue scenario test reports a priority queue before anyone subscribes, while messages are out and after an ack, and a single active consumer queue with a standby that takes over.
- Mutation-checked:
  - counting in-flight messages at their level;
  - not counting the unread;
  - counting in-flight messages as unread;
  - every consumer reported as shared;
  - an unsubscribed queue's messages not counted as unread;
  - the decoder dropping the states.

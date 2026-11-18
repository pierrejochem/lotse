# 122. A node low on disk stops new data in its groups

- Status: Accepted
- Date: 2026-11-18
- Supersedes the disk limits of [ADR 0011](0011-flow-control-throttle-never-drop.md)

## Context

ADR 0011 planned two disk limits per node: a soft one at 20 % free that raises `throttle_ms`, and a hard one at 10 % free that stops writes with `STORAGE_FULL`. Neither was built. A node wrote until its volume was full, and then a replica failed on its next append. In a Raft group every voter writes what the leader takes, so one full voter is enough to stall a group, or to crash its replica.

The console was also missing figures:

- the age of a stream's oldest record;
- the open transactions per stream;
- the bytes each queue consumer holds;
- each node's zone and disk limit.

The README listed these as console gaps, together with the navigation on a phone, which sat above the page.

## Decision

**A node whose data volume has less free space than its limit says so; the metadata group records it, and the leader of every group with that node as a voter takes no new data until it has room again** (maintainer's choices: all the console gaps, disk protection with them; refuse new data below one limit; any voter of the group counts; the limit in bytes or a percentage, on by default).

- **The limit.** `[disk] free_limit` in the cluster file is bytes (`2147483648`, `"2GiB"`, `"500MB"`), a share of the volume (`"5%"`, one decimal at most), or `0` for none. The default is the larger of 1 GiB and 5 % of the volume.
- **Low and back.** A node reads its volume every 5 s, as it does for `DescribeMetrics`.
  - It is low once free space falls below the limit.
  - It is back only once free space is a tenth above the limit, so a volume that hovers at the limit does not switch new data on and off with every read.
  - A read that cannot tell changes nothing.
- **Who knows.** The node reports both, with its limit in bytes, in `DescribeMetrics` (tag 6). The metadata leader already asks every node for its metrics to weigh moves (ADR 0052), every 1,000 ticks. From those answers it proposes `DiskLow { node, low }` for each node whose answer differs from the state.
  - The set of low nodes is part of the metadata state and its snapshot.
  - A forgotten node leaves it.
  - A node that does not answer stays as it was recorded.
  - A low node also counts itself at once, before its record commits.
- **What is refused.** A leader asks only about a batch that is new to the partition, after the duplicate check, as for a queue's length limit (ADR 0118): a retry of a batch it took is still answered as written.
  - Refused while any voter of the group, in either configuration of a joint membership, is low:
    - a produce, plain or transactional;
    - what a node writes for a `Publish`;
    - a dead letter forwarded to the queue, which stays owed in its source.
  - The answer is `STORAGE_FULL` (11), now retriable, with the low nodes named.
  - Everything that keeps the log moving or makes room passes: acks, nacks, checkouts, timers, retirements, transaction markers, the coordinator's and metadata groups' records, and every metadata change.
- **Clients.** Every client retries a produce refused with `STORAGE_FULL`, with the same bytes. When its time runs out it gives up with the refusal, not a timeout, and the sequence stays free. If an earlier attempt's answer was lost, the outcome is unknown, as for any retry.
- **More figures in `DescribeMetrics`**, each a tag written only when it has something to say:
  - Tag 4: each partition's oldest record and its open transactions. The oldest record is the timestamp of the first data batch within 64 KiB of the log start: the producer's timestamp, or on a queue the leader's append time. Each replica looks again once a second, only when its log start or first segment changed.
  - Tag 5: the bytes each queue consumer holds, each message counting its share of its batch as stored. A message that a consumer resumed with after a failover counts 0 until it is delivered again.
  - Tag 6: the node's limit and whether it is low.
- **`/metrics`** gains:
  - `lotse_disk_free_limit_bytes` and `lotse_disk_low`;
  - `lotse_partition_oldest_record_timestamp_seconds` and `lotse_partition_open_transactions`;
  - `lotse_queue_in_flight_bytes`.
- **Console.**
  - Nodes show their zone from `Metadata` (ADR 0104), their limit as a tick on the disk bar, and "Low on disk" with an alert while low.
  - A stream shows its oldest record's age and its open transactions, summed over partitions.
  - A queue consumer shows the bytes it holds.
  - On a phone the navigation is a bar with a menu button. The pages open from it, and choosing one closes the menu.

Rejected options:

- **Throttling at a soft limit, then refusing at a hard one** (ADR 0011). It needs two settings and a `throttle_ms` that no client honours yet. One limit, with a margin before it clears, is enough to stop a volume from filling.
- **Refusing only on the low node.** A leader on a healthy node would go on taking data that the low voter must write too, until it fails.
- **Watching only leaders' disks.** It is simpler, but followers fill just as fast.
- **A request of its own for nodes to report.** The metadata leader already reads every node's metrics, so no new key is needed.
- **A fixed limit only, or none by default.** A volume that fills by surprise is worse than one that stops taking new data with room to spare.

## Consequences

- A node that runs low no longer fills its volume. New data waits, retried by clients, until its operator frees space or moves replicas off. Consumers drain queues meanwhile.
- On a small cluster, where every group has a voter on every node, one low node stops new data everywhere. That is the price of each voter holding a full copy.
- A node learns that another is low up to about 15 s after it is: one host read, and the metadata leader's next poll of the nodes' metrics. The low node refuses at once for the groups it leads. The default margin of 5 % leaves room for that.
- Tests on real disks set the limit to 0 or 1 byte: whatever the machine has free must not decide them.
- The golden vectors gain tags 4 to 6, and `STORAGE_FULL` is retriable in every SDK.
- `DiskLimits`, the soft and hard fractions in the console's API, is gone: each node carries its own limit.
- Tests:
  - Unit tests cover the limit and its hysteresis, the cluster file's forms, the metadata change and its snapshot section, the oldest record and open transactions of a replica, and a leader that refuses a new produce and a dead letter but answers a retry and takes an ack.
  - A simulation drives a follower's simulated volume below the limit. The leaders, which learn of it only through the metadata group, refuse; reads, acks and a metadata change go on; the node stays low just above the limit and clears a tenth above it.
  - The metrics simulation checks every new figure.
  - The SDKs read the new tags from the vectors and test the retry and the refusal at the end.
  - The console's test against real nodes checks zones, the limit, the oldest record and open transactions.
- Mutation-checked:
  - a leader that ignores low voters;
  - a dead letter taken while a voter is low;
  - the refusal asked before the duplicate check;
  - the metadata leader not recording a low node;
  - clearing at the limit instead of a tenth above it;
  - in the SDKs, a refusal given up as a timeout and `STORAGE_FULL` not retriable.

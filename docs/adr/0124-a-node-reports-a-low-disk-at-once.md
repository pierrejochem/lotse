# 124. A node reports a low disk at once

- Status: Accepted
- Date: 2026-11-19
- Changes [ADR 0122](0122-a-node-low-on-disk-stops-new-data-in-its-groups.md) in part: how the metadata group learns that a node is low, and how often a node reads its volume

## Context

Under ADR 0122 a node read its data volume every 5 s, together with the host's CPU reading. The metadata leader learned from the metrics it asks of every node every 1,000 ticks, 10 s at the default tick. So other nodes' leaders went on taking new data for up to about 15 s after a node went low. On a volume filling fast, those 15 s are what the default margin of 5 % had to absorb. The README listed this as a limit.

## Decision

**A node whose read of its volume differs from what its own view of the metadata state records tells the metadata leader at once, with the internal `ReportDisk` (key 61). It reads its volume every `[disk] check_interval_ms`, 1,000 ms by default** (maintainer's choices: the node reports at once; a setting with a default of a second).

- **The read.** A node reads only its volume's free and total bytes, through `Host::disk_space`, every `check_interval_ms` (10 to 60,000 ms). The CPU and memory reading stays at 5 s. The limit, and the margin of a tenth before a node is back, are as in ADR 0122.
- **The report.** After each read, if the node is low and the state does not say so, or the other way round, it sends `ReportDisk { node_id, low }` to the metadata leader. One report is in flight at a time. While its view still differs, the next read reports again. A lost report, a change of metadata leader, and a node that restarted all end the same way.
- **The metadata leader.**
  - It answers at once when the state already says so, or when the node left.
  - Otherwise it proposes `DiskLow` and answers once the change commits.
  - A node speaks only for itself. A report for another node, from a connection whose certificate names a node, is refused with `UNAUTHORIZED`.
  - Only broker nodes may send the request.
- **The poll no longer records.** The metadata leader still asks every node for its metrics, for sizes (ADR 0052) and tombstones (ADR 0100). It no longer proposes `DiskLow` from those answers. An answer older than a node's report could otherwise set the state back, and the node's repeated report covers what the poll covered. `DescribeMetrics` tag 6 still carries the node's limit and state for the console and `/metrics`.
- **Vectors.** `ReportDisk` is node-internal and stays out of the client vectors, like `SettleVoters`. The SDKs need no codec for it.

Rejected options:

- **Polling the disk faster.** Asking every node every second costs requests forever and still leaves up to a second plus the read interval.
- **Telling the group leaders directly.** Faster by one commit, but leaders could disagree, and a new leader would not know. The metadata state stays the one record.
- **Keeping the poll as a second source.** It races with the report, as above.
- **A fixed read interval.** One second suits most volumes. An operator with a slow or remote file system can read less often, and a test can read more often.

## Consequences

- Other nodes' leaders refuse new data about one read interval plus a commit after a node crosses its limit: within a few seconds by default, where it was up to about 15 s.
- A node reads its volume five times as often as before. A free space read is one system call.
- A low node that cannot reach the metadata leader stays as recorded until it can, as a node that was down did before. It still refuses at once for the groups it leads.
- The protocol gains key 61, internal. The client vectors are unchanged except the `ApiVersions` answer, which lists the key.
- Tests:
  - In the disk simulation, nodes run with verified identities:
    - leaders refuse within 5 s of a follower going low, and take data again within 5 s of it having room;
    - a recovery while the node is cut off from the others is recorded once it is reachable again;
    - a node reporting for another is refused.
  - The cluster file's `check_interval_ms`, its default and its bounds are unit-tested.
- Mutation-checked:
  - no report;
  - one report per change without a repeat;
  - a node reporting for another.

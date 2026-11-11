# 100. Tombstones go once every node removed the files

- Status: Accepted
- Date: 2026-11-11

## Context

Deleting a resource leaves a tombstone in the metadata state: its id and how many groups it had (ADR 0035). The tombstone lets every node close the resource's groups and remove their files, including a node that was down when the delete applied, and it marks the id as gone for good, so a node answers `UNKNOWN_RESOURCE` rather than `NOT_LEADER` and a transaction coordinator stops writing markers to the deleted partitions. Tombstones were kept for ever, so the metadata state, every snapshot of it and every node's view grew with every resource ever deleted. The README listed this as the last place where memory grows with data.

## Decision

**A tombstone is forgotten once every node the cluster lists has removed the files of its groups; an id is deleted for good without one.**

- A tombstone records the index of the record that deleted it (snapshot section 10; a snapshot from before has 0 there).
- Each node says how far it has got. When a node's metadata view is rebuilt, it closes the groups of every tombstone and removes their files first; only then does the new view count. `DescribeMetrics` answers with the metadata index that view was built from, in the tag `removed_through` (1). A node that has not built a view since it started answers without the tag. The tag is for nodes only: the SDKs ignore it, and the conformance sample leaves it out.
- The metadata leader already asks every node for `DescribeMetrics` to weigh moves (ADR 0052). It keeps each node's `removed_through`. A tombstone is forgettable once the least `removed_through` over every node listed (configured ones and those added, without those gone) is at or past the index that made it. A listed node that did not answer counts as 0 and holds them all. The leader proposes `ForgetDeleted` (metadata change 24) with those ids, from the same loop and under the same guard as the planner: only while everything it proposed has applied.
- An id is deleted when it has a tombstone or when it is a runtime id that is not live and was created at or before what the state has applied. Ids are never reused, so this holds after the tombstone has gone.
  - In the metadata state this is `is_deleted`. A change that names an id was checked against a state in which the id existed, so a runtime id that is not there any more was deleted.
  - In a node's view it is `Deleted::contains`, with the view's applied index. A runtime id above that may be a resource this node has not heard of yet, and it still gets `NOT_LEADER`.
- A node that is gone (drained and forgotten, ADR 0051) no longer holds tombstones. It does not run again with its old data: its id is never used twice.

## Consequences

- The metadata state keeps a tombstone only while some node may hold its files. A listed node that is down holds every tombstone made since it last answered, until it comes back or is removed.
- Answers about a deleted id are the same with or without its tombstone: `UNKNOWN_RESOURCE`, no markers written, no bind or alter applied.
- `DescribeMetrics` carries one more tag between nodes. A node of an older version never answers with it, so it holds the tombstones until it is upgraded.
- The rules are mutation-checked:
  - in unit tests: what is forgettable, and what counts as deleted with and without a tombstone;
  - in the metadata simulation, which now waits until every node holds no tombstone and then checks that deleted ids are still unknown everywhere. It also deletes a stream while one node is cut off from the others, and fails if a node reports an index for which it still holds the stream's files.
- Each mutation made a test fail:
  - a node that does not answer counting as having removed everything;
  - deletes without a tombstone not recognised;
  - the applied bound dropped;
  - a node reporting before it removed the files.

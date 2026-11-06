# 63. Conditional delete

- Status: Accepted
- Date: 2026-11-06

## Context

`DeleteResource` has had an `if_empty` flag since the first cut of the metadata group, but ADR 0035 refused it. Checking it needs the metadata leader, which deletes, to learn something only the leaders of the resource's own groups know. Those leaders may run on other nodes. Without the flag, an operator cleaning up a queue had no way to say "only if nothing is left in it".

## Decision

**Check, then delete** (maintainer's choice). With `if_empty`, the metadata leader first runs every check of an unconditional delete. Then it asks each partition's leader, with the new internal `CheckEmpty` (key 46, nodes only, answered with an `Outcome`), whether the partition holds anything. It asks through the same client code that routes everything else, so leader hints and retries apply. The first partition that holds something ends the asking, and the delete is refused with the new error `RESOURCE_NOT_EMPTY` (29), naming the partition. If every partition is empty, the delete goes ahead through all the checks of an unconditional one again, since leadership may have moved meanwhile. If the partitions cannot be asked, the delete is refused with that error, and nothing is deleted. An exchange holds no messages and is deleted as if unconditionally.

**What empty means.** A queue is empty when no offset is unsettled and no message is owed to a dead-letter queue. A delivered but unacknowledged message is unsettled. Entries appended but not yet applied count as not empty. A stream partition is empty when it holds no data record from its log start to its end, committed or not; control batches do not count. It is found with the same header scan as a time lookup (ADR 0059).

**A guard, not a lock.** The check and the delete are two steps on different groups. A message produced in the round trip between the last answer and the delete goes with the resource. Sealing the resource first would narrow that window without closing it, and fencing in each partition's own log would close it at the cost of a new control batch and refusing writes when they are applied. `if_empty` is meant to stop an operator deleting by mistake, not to coordinate with producers.

`lotse-client` has `Client::delete_if_empty`. The Java SDK knows the key and the error code.

## Consequences

- Operators and the console can delete a stream or queue only if nothing is left in it.
- A conditional delete costs one round trip per partition before the metadata write.
- The race above is documented and stays.
- Tests: a scenario test on a three-node simulated cluster covers:
  - an empty stream deleted;
  - an exchange deleted;
  - a stream with one record in its last partition kept, and still writable;
  - a queue with an unsettled message kept, also while the message is delivered but not acked, and deleted once it is acked;
  - an unconditional delete working as before.
  
  The access test checks that only nodes may send `CheckEmpty`.
- Mutation checks: these were broken on purpose, and a test failed for each:
  - the leader always saying empty;
  - the queue's unsettled count ignored;
  - the stream scan skipped;
  - the node ignoring a non-empty answer;
  - the metadata leader skipping the check;
  - `CheckEmpty` let through for clients.
  
  Not covered by a test: a message owed to a dead-letter queue, and entries appended but not applied. Both make a queue not empty.

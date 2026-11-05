# 59. ListOffsets

- Status: Accepted
- Date: 2026-11-05

## Context

A reader could only start where it already knew an offset. A consumer group member for a partition with no committed offset always began at the log start, so a new group read the whole history, and nothing could start at the end or at a point in time. The design reserved key 5 for `ListOffsets`; the README listed it as a known gap. Segments already record their newest record timestamp, for retention by age; there is no time index file.

## Decision

**`ListOffsets` answers at both ends and by time** (maintainer's choice). Per partition it takes a timestamp: `-2` (earliest) is the first offset still stored; `-1` (latest) is the offset after the last one the reader may see, the high watermark, or the last stable offset for read_committed; any time in milliseconds is the first offset of the first data batch (control batches and queue state left out) whose newest record is at or after it, together with that record's timestamp. If none qualifies below the reader's end, the answer is that end, with timestamp `-1`. Other negative timestamps are refused (`INVALID_REQUEST`). Only the leader answers, with the same bounds as `Fetch`; it needs the read role.

**A time is found without an index.** Segments whose newest timestamp is older than the time asked for are skipped whole; from the first that may hold it, batch headers are read in order until one qualifies. A lookup costs at most the read of one segment, usually much less, and nothing new is written to disk. The answer is batch-exact, as Kafka's is with a time index: the batch found may begin with older records.

**Consumer groups start where they are told.** `GroupConfig::start_from` (`OffsetAt::Earliest`, the default and the old behaviour; `Latest`; `Time(ms)`) decides where a member starts on a partition its group has committed no offset for. `Client::list_offset` is there for everything else.

## Consequences

- A new group can read only what comes after it, or start at a point in time; replaying a stream from a time is one call.
- A time lookup reads up to one segment from disk; frequent lookups on partitions with large segments cost I/O that a time index would avoid. That index stays possible later without changing the request.
- Timestamps are the producers' own: a producer with a wrong clock makes a time lookup land where its timestamps say.
- The Java SDK has the request and response types (its codec round-trips the new vectors); its client does not use them yet.
- Tests: a deterministic test of every answer (both ends under both isolations, times before, inside, between and after the batches, with small segments to skip and one large segment to read past the end); a scenario test in the group simulation that new groups starting at the end, at a time and at the start see exactly the expected messages. Breaking the time comparison, the control-batch filter, the stop at the reader's end, the isolation and the client's `start_from` each made a test fail.

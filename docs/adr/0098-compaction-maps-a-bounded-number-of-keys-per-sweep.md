# 98. Compaction maps a bounded number of keys per sweep

- Status: Accepted
- Date: 2026-11-11

## Context
Each replica compacts its own log (ADR 0067). It kept a map of every key of the partition to the key's newest offset, grown before each sweep and kept between sweeps, and rebuilt it from the whole log after a restart. Memory grew with the number of keys, without a bound, once per compacted partition. The README listed it as a weakness.

Two decisions, each the maintainer's choice of the recommended option.

## Decision
**A bounded map of key hashes per sweep.** A sweep maps the SHA-256 of each key, cut to 16 bytes, to the key's newest offset. It maps from where the last finished sweep's map ended (the log start after a restart) towards the end of the sealed, stable segments, whole batches at a time, and stops before a batch whose new keys would overfill the map; a first batch goes in whole, so a sweep always gets on. Then it walks the segments that begin below the map's end, oldest first, and drops each record whose key the map knows at a newer offset, as before. When it finishes, the map is dropped and the next sweep maps on from its end. Records whose newer value lies past a sweep's map stay until a later sweep: like Kafka's cleaner, compaction is complete once the maps have covered the log, and it never drops a record it should not.

Tombstones keep their rules (old enough, on a leader, below what every member holds) and go once this replica's older values of the key went: in a sweep whose map holds the tombstone as its key's newest record, since the sweep walks the older segments first, or below where a finished sweep's map reached, since that sweep dropped them.

**One sweep per node, budget per node.** A node runs one sweep at a time. The replica holding a sweep goes on with it; when none does, the compacted partitions after the last one to start get the first chance, in turn. The map may take `[compaction] map_bytes` in the cluster file (default 64 MiB), counted at 48 bytes per key, about 1.4 million keys.

## Consequences
Compaction's memory is bounded per node, not per partition and not by the data. Nothing of it is kept across a restart, as before; a restarted replica maps from the log start again, a sweep at a time.

Two keys whose hashes share 16 bytes count as one, and the older record of either may go: the odds are 2^-128 per pair of keys. A partition with more keys than the map holds takes several sweeps, each of which reads the segments from the log start, so it is compacted later than it would be with a larger map; many compacted partitions on one node wait for their turn.

Tests: a replica with a map of 1 to 4 keys compacts a log of six keys, values and tombstones as fully as a large map would, never brings back an older value of a key whose newest record is a tombstone, and never maps more keys than allowed; a replica not allowed to start waits; the compaction simulation runs half its seeds with maps of 6 to 12 keys, fewer than it writes, and checks that no node sweeps two partitions at once. Breaking the tombstone rules, the map's limit, the advance of the finished region, the turn, or the drop of superseded records makes a test fail.

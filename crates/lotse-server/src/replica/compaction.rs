//! Log compaction of a stream partition (ADR 0067).
//!
//! Each replica compacts its own log: it keeps, per key, the newest record
//! and leaves out the older ones, the records of aborted transactions, and
//! tombstones once they are old enough and safe to drop. Batches are never
//! removed, only records from them (`lotse_record::compact`): every Raft
//! entry keeps its offsets and term, so the log stays the Raft log, and a
//! batch's producer fields still feed the producer and transaction state
//! when a replica applies it again.
//!
//! Only sealed segments below the last stable offset and the applied index
//! are compacted. A sweep first maps key hashes to their newest offset,
//! from where the last finished sweep's map ended (the log start after a
//! restart) until it reaches the end or holds as many keys as it may
//! (ADR 0098); then it walks the segments below that point, oldest first,
//! at most one rewrite per step, and drops each record whose key the map
//! knows at a newer offset. The next sweep maps on from there. A record
//! whose newer value lies past the map stays until a later sweep: what is
//! dropped is never wrong, only sometimes late. The map lives for one
//! sweep; a node runs one sweep at a time, so it bounds the memory.
//!
//! Keys are mapped by 16 bytes of their SHA-256: two keys with one hash
//! (odds of 2^-128 per pair) would count as one.
//!
//! Superseded records may go on any replica at any time: a replica that
//! copies the rest of the log from another one gets the newer record with
//! it. A tombstone is different: dropping it on one replica while another
//! still holds an older value of the key, and then copying from the first
//! to the second, would bring the old value back. So tombstones go only on
//! a leader, only below what every member of the group holds, and only
//! once this replica's older values of the key went: in a sweep whose map
//! holds the tombstone as its key's newest record (the sweep walks the
//! older segments first), or below where a finished sweep's map reached.

use std::collections::BTreeMap;

use bytes::Bytes;
use lotse_log::{Log, SegmentInfo};
use lotse_record::{Batch, BatchKind};
use lotse_rt::Disk;
use sha2::{Digest, Sha256};

use super::{Error, batches};
use crate::txns::TxnState;

/// A new sweep starts this long after the last one began even if no new
/// data came, so that tombstones expire.
const SWEEP_EVERY_MS: u64 = 60_000;

/// Segments a step looks at, at most.
const SEGMENTS_PER_STEP: usize = 8;

/// Bytes read at a time.
const READ_BYTES: usize = 1 << 20;

/// Memory one key of the map takes, counted generously: 16 bytes of hash,
/// 8 of offset, and the tree around them.
pub(crate) const MAP_ENTRY_BYTES: u64 = 48;

/// One replica's compaction progress; nothing of it is persisted.
#[derive(Debug, Default)]
pub(crate) struct Compactor {
    /// The sweep in progress: each key hash's newest offset from where the
    /// last finished sweep's map ended to `map_end`.
    map: BTreeMap<u128, u64>,
    /// Records below this offset are in `map` (or were in an earlier one).
    map_end: u64,
    /// A finished sweep's map reached this offset: below it, every record
    /// a newer one replaces within the mapped ranges is gone.
    clean_to: u64,
    /// The sweep in progress: the next segment at or after this offset.
    cursor: Option<u64>,
    /// The sweep in progress rewrites segments that end at or below this.
    sweep_end: u64,
    /// When the sweep in progress, or the last one, began.
    swept_at_ms: u64,
}

/// What one step may drop.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// Offsets below this are applied and not in an open transaction.
    pub(crate) stable: u64,
    /// Tombstones below this offset may go (what every member holds, on a
    /// leader; 0 elsewhere).
    pub(crate) tombstones_below: u64,
    /// Tombstones are kept this long, by their timestamps.
    pub(crate) tombstone_ms: u64,
    /// Now, ms since the epoch.
    pub(crate) now_ms: u64,
    /// Whether a new sweep may start (the node's one sweep at a time).
    pub(crate) may_start: bool,
    /// Keys a sweep's map may hold.
    pub(crate) map_keys: usize,
}

/// A key's place in the map.
fn hash(key: &[u8]) -> u128 {
    let d = Sha256::digest(key);
    u128::from_be_bytes(d[..16].try_into().expect("16 bytes"))
}

impl Compactor {
    /// Forgets everything (the log was replaced by a snapshot).
    pub(crate) fn reset(&mut self) {
        *self = Compactor::default();
    }

    /// Keys the map holds now.
    #[cfg(test)]
    pub(crate) fn mapped(&self) -> usize {
        self.map.len()
    }

    /// Whether a sweep is in progress (it holds a map).
    pub(crate) fn sweeping(&self) -> bool {
        self.cursor.is_some()
    }

    /// Starts a sweep if one is due and allowed, and rewrites at most one
    /// segment. Returns the base of the segment rewritten, if one was.
    pub(crate) async fn step<D: Disk>(
        &mut self,
        log: &mut Log<D>,
        txns: &TxnState,
        limits: Limits,
    ) -> Result<Option<u64>, Error> {
        if self.clean_to > log.next_offset() || self.map_end > log.next_offset() {
            self.reset();
        }
        self.clean_to = self.clean_to.max(log.start_offset());
        if self.cursor.is_none() {
            if !limits.may_start {
                return Ok(None);
            }
            // Only sealed segments wholly below the stable point.
            let active = log.segments().last().map_or(0, |s| s.base_offset);
            let end = log
                .segments()
                .filter(|s| s.base_offset != active && s.next_offset <= limits.stable)
                .map(|s| s.next_offset)
                .max()
                .unwrap_or(0);
            if end <= self.clean_to && limits.now_ms < self.swept_at_ms.saturating_add(SWEEP_EVERY_MS) {
                return Ok(None);
            }
            // The map stays as it is until the sweep ends: a tombstone it
            // holds was known when every older segment was swept, so this
            // replica's older values of its key went first.
            self.map.clear();
            self.map_end = self.clean_to;
            self.scan(log, txns, end, limits.map_keys.max(1)).await?;
            self.cursor = Some(log.start_offset());
            self.sweep_end = end;
            self.swept_at_ms = limits.now_ms;
        }
        // Segments with nothing to drop cost a read only: several of them
        // per step, but at most one rewrite.
        for _ in 0..SEGMENTS_PER_STEP {
            let at = self.cursor.expect("set above");
            let Some(seg) = log
                .segments()
                .find(|s| s.next_offset > at && s.next_offset <= self.sweep_end && s.base_offset < self.map_end)
            else {
                self.cursor = None;
                self.clean_to = self.map_end;
                self.map = BTreeMap::new();
                return Ok(None);
            };
            self.cursor = Some(seg.next_offset);
            if let Some(out) = self.compacted(log, txns, seg, limits).await? {
                log.rewrite_segment(seg.base_offset, &out).await?;
                return Ok(Some(seg.base_offset));
            }
        }
        Ok(None)
    }

    /// The batches of `seg` without what may go; `None` if nothing may.
    async fn compacted<D: Disk>(
        &self,
        log: &Log<D>,
        txns: &TxnState,
        seg: SegmentInfo,
        limits: Limits,
    ) -> Result<Option<Vec<Bytes>>, Error> {
        let tombstones_below = limits.tombstones_below;
        let horizon = i64::try_from(limits.now_ms.saturating_sub(limits.tombstone_ms)).unwrap_or(i64::MAX);
        let mut out = Vec::new();
        let mut changed = false;
        let mut offset = seg.base_offset;
        while offset < seg.next_offset {
            for batch in batches(log.read(offset, READ_BYTES).await?)? {
                if batch.base_offset() >= seg.next_offset {
                    break;
                }
                offset = batch.next_offset();
                let kept = if compactable(&batch) {
                    lotse_record::compact(&batch, |r| {
                        if batch.attributes().transactional && txns.aborted_at(batch.producer_id(), r.offset) {
                            return false;
                        }
                        let Some(key) = &r.key else { return true };
                        let expired = r.offset < tombstones_below && r.timestamp < horizon;
                        match self.map.get(&hash(key)) {
                            Some(&newest) if newest > r.offset => false,
                            // The map's newest of its key, or past where a
                            // finished sweep reached: older values went.
                            Some(&newest) if newest == r.offset && r.value.is_none() => !expired,
                            None if r.value.is_none() && r.offset < self.clean_to => !expired,
                            _ => true,
                        }
                    })?
                } else {
                    None
                };
                changed |= kept.is_some();
                out.push(kept.unwrap_or_else(|| batch.as_bytes().clone()));
            }
        }
        Ok(changed.then_some(out))
    }

    /// Maps every keyed record from `map_end` towards `end`, a batch at a
    /// time, until the map holds `keys` keys (the first batch goes in
    /// whole, so a sweep always gets on).
    async fn scan<D: Disk>(&mut self, log: &Log<D>, txns: &TxnState, end: u64, keys: usize) -> Result<(), Error> {
        while self.map_end < end {
            let read = batches(log.read(self.map_end, READ_BYTES).await?)?;
            if read.is_empty() {
                break;
            }
            for batch in read {
                if batch.base_offset() >= end {
                    return Ok(());
                }
                if compactable(&batch) {
                    let aborted = |o| batch.attributes().transactional && txns.aborted_at(batch.producer_id(), o);
                    let records: Vec<(u128, u64)> = batch
                        .records()?
                        .into_iter()
                        .filter(|r| !aborted(r.offset))
                        .filter_map(|r| r.key.map(|k| (hash(&k), r.offset)))
                        .collect();
                    let new: std::collections::BTreeSet<u128> =
                        records.iter().map(|(h, _)| *h).filter(|h| !self.map.contains_key(h)).collect();
                    // Whole batches only: one that would overfill the map
                    // waits for the next sweep.
                    if !self.map.is_empty() && self.map.len() + new.len() > keys {
                        return Ok(());
                    }
                    self.map.extend(records);
                }
                self.map_end = batch.next_offset();
            }
        }
        Ok(())
    }
}

/// Data a producer wrote; markers and the broker's own batches stay whole.
fn compactable(batch: &Batch) -> bool {
    !batch.attributes().control && batch.attributes().kind == BatchKind::Data
}

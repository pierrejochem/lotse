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
//! are compacted. A sweep walks them oldest first, at most one rewrite per
//! step, with a map of each key's newest offset that grows between sweeps.
//!
//! Superseded records may go on any replica at any time: a replica that
//! copies the rest of the log from another one gets the newer record with
//! it. A tombstone is different: dropping it on one replica while another
//! still holds an older value of the key, and then copying from the first
//! to the second, would bring the old value back. So tombstones go only on
//! a leader, only below what every member of the group holds, and only in a
//! sweep that knew them from its start, so this replica's older values of
//! the key went first.

use std::collections::BTreeMap;

use bytes::Bytes;
use lotse_log::{Log, SegmentInfo};
use lotse_record::{Batch, BatchKind};
use lotse_rt::Disk;

use super::{Error, batches};
use crate::txns::TxnState;

/// A new sweep starts this long after the last one began even if no new
/// data came, so that tombstones expire.
const SWEEP_EVERY_MS: u64 = 60_000;

/// Segments a step looks at, at most.
const SEGMENTS_PER_STEP: usize = 8;

/// Bytes read at a time.
const READ_BYTES: usize = 1 << 20;

/// One replica's compaction progress; nothing of it is persisted.
#[derive(Debug, Default)]
pub(crate) struct Compactor {
    /// Each key's newest offset below `scanned_to`.
    latest: BTreeMap<Bytes, u64>,
    /// Records below this offset are in `latest`.
    scanned_to: u64,
    /// The sweep in progress: the next segment at or after this offset.
    cursor: Option<u64>,
    /// The sweep in progress covers segments that end at or below this.
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
}

impl Compactor {
    /// Forgets everything (the log was replaced by a snapshot).
    pub(crate) fn reset(&mut self) {
        *self = Compactor::default();
    }

    /// Starts a sweep if one is due, and rewrites at most one segment. Returns the base of
    /// the segment rewritten, if one was.
    pub(crate) async fn step<D: Disk>(
        &mut self,
        log: &mut Log<D>,
        txns: &TxnState,
        limits: Limits,
    ) -> Result<Option<u64>, Error> {
        if self.scanned_to > log.next_offset() {
            self.reset();
        }
        if self.scanned_to < log.start_offset() {
            let start = log.start_offset();
            self.latest.retain(|_, o| *o >= start);
            self.scanned_to = start;
        }
        if self.cursor.is_none() {
            // Only sealed segments wholly below the stable point.
            let active = log.segments().last().map_or(0, |s| s.base_offset);
            let end = log
                .segments()
                .filter(|s| s.base_offset != active && s.next_offset <= limits.stable)
                .map(|s| s.next_offset)
                .max()
                .unwrap_or(0);
            if end <= self.scanned_to && limits.now_ms < self.swept_at_ms.saturating_add(SWEEP_EVERY_MS) {
                return Ok(None);
            }
            // The map stays as it is until the sweep ends: a tombstone it
            // knows was known when every older segment was swept, so this
            // replica's older values of its key went first.
            self.scan(log, txns, end).await?;
            self.cursor = Some(log.start_offset());
            self.sweep_end = end;
            self.swept_at_ms = limits.now_ms;
        }
        // Segments with nothing to drop cost a read only: several of them
        // per step, but at most one rewrite.
        for _ in 0..SEGMENTS_PER_STEP {
            let at = self.cursor.expect("set above");
            let Some(seg) = log.segments().find(|s| s.next_offset > at && s.next_offset <= self.sweep_end) else {
                self.cursor = None;
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
                        match self.latest.get(key) {
                            Some(&newest) if newest > r.offset => false,
                            Some(_) if r.value.is_none() => r.offset >= tombstones_below || r.timestamp >= horizon,
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

    /// Adds every keyed record from `scanned_to` to `end` to the map.
    async fn scan<D: Disk>(&mut self, log: &Log<D>, txns: &TxnState, end: u64) -> Result<(), Error> {
        while self.scanned_to < end {
            let read = batches(log.read(self.scanned_to, READ_BYTES).await?)?;
            if read.is_empty() {
                break;
            }
            for batch in read {
                if batch.base_offset() >= end {
                    return Ok(());
                }
                if compactable(&batch) {
                    let aborted = |o| batch.attributes().transactional && txns.aborted_at(batch.producer_id(), o);
                    for r in batch.records()? {
                        if let Some(key) = r.key
                            && !aborted(r.offset)
                        {
                            self.latest.insert(key, r.offset);
                        }
                    }
                }
                self.scanned_to = batch.next_offset();
            }
        }
        Ok(())
    }
}

/// Data a producer wrote; markers and the broker's own batches stay whole.
fn compactable(batch: &Batch) -> bool {
    !batch.attributes().control && batch.attributes().kind == BatchKind::Data
}

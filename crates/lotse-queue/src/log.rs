//! The applied state of a queue's log: which offsets are settled, and the
//! persisted delivery counts.
//!
//! An offset is **settled** when nothing will ever deliver it: its message
//! was acknowledged, its transaction aborted, or it is not a message at all
//! (Raft's and the broker's own batches: markers, acks, queue state). The
//! lowest unsettled offset is the ack floor. Every replica applies the same
//! batches in the same order, so every replica agrees on what is settled.
//!
//! Transactional messages wait for their marker: committed, they are
//! deliverable; aborted, they are settled.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::{Batch, BatchKind, Control, ControlType};

use crate::RangeSet;

/// See the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueLog {
    settled: RangeSet,
    /// Delivery counts persisted on nack or ack timeout, for unsettled offsets.
    counts: BTreeMap<u64, u16>,
    /// Transactional messages per producer that wait for their marker.
    pending: BTreeMap<u64, Vec<(u64, u64)>>,
}

impl QueueLog {
    /// An empty queue.
    pub fn new() -> Self {
        QueueLog::default()
    }

    /// Applies a committed batch, in log order. Fails on a malformed queue
    /// batch: the log cannot be trusted then.
    pub fn apply(&mut self, batch: &Batch) -> Result<(), lotse_record::Error> {
        let (first, next) = (batch.base_offset(), batch.next_offset());
        let a = batch.attributes();
        if a.control {
            self.settled.insert(first, next);
            if let Some(Control::Marker(ty, _)) = lotse_record::control(batch)? {
                let ranges = self.pending.remove(&batch.producer_id()).unwrap_or_default();
                if ty == ControlType::Abort {
                    for (f, t) in ranges {
                        self.settle(f, t);
                    }
                }
            }
            return Ok(());
        }
        match a.kind {
            BatchKind::Data => {
                if a.transactional && batch.producer_id() != 0 {
                    self.pending.entry(batch.producer_id()).or_default().push((first, next));
                }
            }
            BatchKind::Ack => {
                self.settled.insert(first, next);
                for (f, t) in lotse_record::ack_ranges(batch)? {
                    self.settle(f, t);
                }
            }
            BatchKind::QueueState => {
                self.settled.insert(first, next);
                for (offset, count) in lotse_record::queue_state_counts(batch)? {
                    if !self.settled.contains(offset) {
                        let c = self.counts.entry(offset).or_insert(0);
                        *c = (*c).max(count);
                    }
                }
            }
            BatchKind::Timer => self.settled.insert(first, next),
        }
        Ok(())
    }

    fn settle(&mut self, from: u64, to: u64) {
        self.settled.insert(from, to);
        let gone: Vec<u64> = self.counts.range(from..to).map(|(o, _)| *o).collect();
        for o in gone {
            self.counts.remove(&o);
        }
    }

    /// Whether `offset` will never be delivered again.
    pub fn is_settled(&self, offset: u64) -> bool {
        self.settled.contains(offset)
    }

    /// The lowest unsettled offset at or after `from`.
    pub fn next_unsettled(&self, from: u64) -> u64 {
        self.settled.next_gap(from)
    }

    /// The ack floor: every offset below it is settled.
    pub fn floor(&self) -> u64 {
        self.settled.next_gap(0)
    }

    /// How many offsets in `from..to` are not settled: messages that are
    /// or will be delivered (or wait for their transaction's marker).
    pub fn unsettled_in(&self, from: u64, to: u64) -> u64 {
        to.saturating_sub(from) - self.settled.count_in(from, to)
    }

    /// Messages waiting for their transaction's marker; until it comes they
    /// are neither deliverable nor settled.
    pub fn in_open_transactions(&self) -> u64 {
        self.pending.values().flatten().map(|&(f, t)| t - f).sum()
    }

    /// The persisted delivery count of `offset`.
    pub fn delivery_count(&self, offset: u64) -> u16 {
        self.counts.get(&offset).copied().unwrap_or(0)
    }

    /// The applied state, for a snapshot.
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        self.settled.encode(&mut b);
        b.put_u32_le(self.counts.len() as u32);
        for (o, c) in &self.counts {
            b.put_u64_le(*o);
            b.put_u16_le(*c);
        }
        b.put_u32_le(self.pending.len() as u32);
        for (p, ranges) in &self.pending {
            b.put_u64_le(*p);
            b.put_u32_le(ranges.len() as u32);
            for (f, t) in ranges {
                b.put_u64_le(*f);
                b.put_u64_le(*t);
            }
        }
        b.freeze()
    }

    /// The state a snapshot holds.
    pub fn decode(mut b: Bytes) -> Result<Self, String> {
        let short = || "queue state is truncated".to_string();
        let settled = RangeSet::decode(&mut b)?;
        if b.remaining() < 4 {
            return Err(short());
        }
        let n = b.get_u32_le() as usize;
        if b.remaining() < n.saturating_mul(10) {
            return Err(short());
        }
        let counts = (0..n).map(|_| (b.get_u64_le(), b.get_u16_le())).collect();
        if b.remaining() < 4 {
            return Err(short());
        }
        let mut pending = BTreeMap::new();
        for _ in 0..b.get_u32_le() {
            if b.remaining() < 12 {
                return Err(short());
            }
            let (p, m) = (b.get_u64_le(), b.get_u32_le() as usize);
            if b.remaining() < m.saturating_mul(16) {
                return Err(short());
            }
            pending.insert(p, (0..m).map(|_| (b.get_u64_le(), b.get_u64_le())).collect());
        }
        if b.has_remaining() {
            return Err("queue state has trailing bytes".into());
        }
        Ok(QueueLog { settled, counts, pending })
    }
}

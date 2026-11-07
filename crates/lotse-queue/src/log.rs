//! The applied state of a queue's log: which offsets are settled, and the
//! persisted delivery counts.
//!
//! An offset is **settled** when nothing will ever deliver it: its message
//! was acknowledged, its transaction aborted, or it is not a message at all
//! (Raft's and the broker's own batches: markers, acks, queue state). The
//! lowest unsettled offset is the ack floor. Every replica applies the same
//! batches in the same order, so every replica agrees on what is settled.
//!
//! A message that leaves the queue because of its limits (ADR 0055) is settled
//! by a **claim** (a RETIRE batch) and, when the queue has a dead-letter
//! queue, stays **owed** to it until a second RETIRE batch says it was
//! forwarded. A queue that receives dead letters keeps, per source queue,
//! which source offsets it has taken, so that a message sent twice is
//! delivered once.
//!
//! Transactional messages wait for their marker: committed, they are
//! deliverable; aborted, they are settled.
//!
//! A transactional ack (ADR 0075) **holds** the messages it names for its
//! producer's transaction: they are neither delivered nor settled until the
//! marker comes. Committed, they are settled; aborted, they are delivered
//! again. Only an unsettled message that no other transaction holds can be
//! held, so of two transactions that ack the same message the log's order
//! picks one; the leader tells the other one's client, which must abort.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::{Batch, BatchKind, Control, ControlType, Retire, dead_letter::Reason};

use crate::RangeSet;

/// See the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueLog {
    settled: RangeSet,
    /// Delivery counts persisted on nack or ack timeout, for unsettled offsets.
    counts: BTreeMap<u64, u16>,
    /// Transactional messages per producer that wait for their marker.
    pending: BTreeMap<u64, Vec<(u64, u64)>>,
    /// Claimed messages not yet forwarded to their dead-letter queue.
    owed: BTreeMap<u64, Owed>,
    /// Per source queue: what this queue has taken from it as dead letters.
    sources: BTreeMap<u64, Source>,
    /// Messages transactional acks hold, per producer: the epoch of the
    /// ack, and the offsets.
    held: BTreeMap<u64, (u16, RangeSet)>,
}

/// A claimed message that is to be forwarded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Owed {
    /// Why it left the queue.
    pub reason: Reason,
    /// How often it had been delivered.
    pub deliveries: u16,
    /// Resource id of the queue it is owed to.
    pub target: u64,
}

/// What a queue has taken as dead letters from one source queue.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Source {
    /// Every source offset below this is done there: forwarded or
    /// acknowledged. A message below it will not come again.
    floor: u64,
    /// Source offsets taken, at or above `floor`.
    taken: RangeSet,
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
                let producer = batch.producer_id();
                let ranges = self.pending.remove(&producer).unwrap_or_default();
                if ty == ControlType::Abort {
                    for (f, t) in ranges {
                        self.settle(f, t);
                    }
                }
                // The marker ends the transaction its acks were written in,
                // which ran at an older epoch (the coordinator raises it).
                if self.held.get(&producer).is_some_and(|(epoch, _)| *epoch < batch.producer_epoch()) {
                    let (_, offsets) = self.held.remove(&producer).expect("checked");
                    if ty == ControlType::Commit {
                        for (f, t) in offsets.iter() {
                            self.settle(f, t);
                        }
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
            BatchKind::Ack if a.transactional && batch.producer_id() != 0 => {
                self.settled.insert(first, next);
                let ranges = lotse_record::ack_ranges(batch)?;
                self.hold(batch.producer_id(), batch.producer_epoch(), &ranges);
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
            BatchKind::DeadLetter => self.take_dead_letters(batch)?,
            BatchKind::Retire => {
                self.settled.insert(first, next);
                match lotse_record::retire(batch)? {
                    Retire::Out { target, entries } => {
                        for (offset, reason, deliveries) in entries {
                            // Unsettled now means no ack came first: the log
                            // orders the claim and the ack, so exactly one
                            // of them settles the message.
                            // Held, it belongs to a transaction that may
                            // still commit.
                            if !self.settled.contains(offset) && self.held_by(offset).is_none() {
                                self.settle(offset, offset + 1);
                                self.owed.insert(offset, Owed { reason, deliveries, target });
                            }
                        }
                    }
                    Retire::Done(offsets) => {
                        for offset in offsets {
                            self.owed.remove(&offset);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// A dead-letter batch: its messages are deliverable, except those
    /// taken from the same source before (a retry that reached the log twice).
    fn take_dead_letters(&mut self, batch: &Batch) -> Result<(), lotse_record::Error> {
        for record in batch.records()? {
            let origin = lotse_record::dead_letter_origin(&record)?;
            let source = self.sources.entry(origin.queue_id).or_default();
            if origin.floor > source.floor {
                source.floor = origin.floor;
                source.taken = source.taken.from(origin.floor);
            }
            if origin.offset < source.floor || source.taken.contains(origin.offset) {
                self.settle(record.offset, record.offset + 1);
            } else {
                source.taken.insert(origin.offset, origin.offset + 1);
            }
        }
        Ok(())
    }

    /// Holds for `producer` what in `ranges` is unsettled and not held by
    /// another producer's transaction.
    fn hold(&mut self, producer: u64, epoch: u16, ranges: &[(u64, u64)]) {
        for &(from, to) in ranges {
            let mut o = self.settled.next_gap(from);
            while o < to {
                if self.held_by(o).is_none() {
                    let (e, offsets) = self.held.entry(producer).or_insert_with(|| (epoch, RangeSet::new()));
                    *e = (*e).max(epoch);
                    offsets.insert(o, o + 1);
                }
                o = self.settled.next_gap(o + 1);
            }
        }
    }

    /// The producer whose open transaction holds `offset`, if one does.
    pub fn held_by(&self, offset: u64) -> Option<u64> {
        if self.settled.contains(offset) {
            return None;
        }
        self.held.iter().find(|(_, (_, offsets))| offsets.contains(offset)).map(|(p, _)| *p)
    }

    /// How many messages open transactions hold.
    pub fn held(&self) -> u64 {
        self.held.values().flat_map(|(_, o)| o.iter()).map(|(f, t)| t - f - self.settled.count_in(f, t)).sum()
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

    /// The ack floor, held back by messages still owed to a dead-letter
    /// queue: their data must stay in the log until they are forwarded.
    pub fn retention_floor(&self) -> u64 {
        self.owed.keys().next().map_or(self.floor(), |o| (*o).min(self.floor()))
    }

    /// The claimed messages that are not forwarded yet, by offset.
    pub fn owed(&self) -> impl Iterator<Item = (u64, Owed)> + '_ {
        self.owed.iter().map(|(o, w)| (*o, *w))
    }

    /// The unsettled messages whose persisted delivery count is at least
    /// `n`, by offset.
    pub fn delivered_at_least(&self, n: u32) -> impl Iterator<Item = (u64, u16)> + '_ {
        self.counts.iter().filter(move |(_, c)| u32::from(**c) >= n).map(|(o, c)| (*o, *c))
    }

    /// How many source offsets of `queue_id` this queue holds as taken, for
    /// tests and metrics.
    pub fn taken_from(&self, queue_id: u64) -> usize {
        self.sources.get(&queue_id).map_or(0, |s| s.taken.len())
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
        b.put_u32_le(self.owed.len() as u32);
        for (o, w) in &self.owed {
            b.put_u64_le(*o);
            b.put_u8(w.reason.code());
            b.put_u16_le(w.deliveries);
            b.put_u64_le(w.target);
        }
        b.put_u32_le(self.sources.len() as u32);
        for (q, s) in &self.sources {
            b.put_u64_le(*q);
            b.put_u64_le(s.floor);
            s.taken.encode(&mut b);
        }
        // Written only when there is something, so a state without held
        // messages reads as before.
        if !self.held.is_empty() {
            b.put_u32_le(self.held.len() as u32);
            for (p, (epoch, offsets)) in &self.held {
                b.put_u64_le(*p);
                b.put_u16_le(*epoch);
                offsets.encode(&mut b);
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
        if b.remaining() < 4 {
            return Err(short());
        }
        let n = b.get_u32_le() as usize;
        if b.remaining() < n.saturating_mul(19) {
            return Err(short());
        }
        let mut owed = BTreeMap::new();
        for _ in 0..n {
            let offset = b.get_u64_le();
            let reason = Reason::from_code(b.get_u8()).ok_or("unknown dead-letter reason in queue state")?;
            let deliveries = b.get_u16_le();
            owed.insert(offset, Owed { reason, deliveries, target: b.get_u64_le() });
        }
        if b.remaining() < 4 {
            return Err(short());
        }
        let mut sources = BTreeMap::new();
        for _ in 0..b.get_u32_le() {
            if b.remaining() < 16 {
                return Err(short());
            }
            let (queue, floor) = (b.get_u64_le(), b.get_u64_le());
            sources.insert(queue, Source { floor, taken: RangeSet::decode(&mut b)? });
        }
        let mut held = BTreeMap::new();
        if b.has_remaining() {
            if b.remaining() < 4 {
                return Err(short());
            }
            for _ in 0..b.get_u32_le() {
                if b.remaining() < 10 {
                    return Err(short());
                }
                let (p, epoch) = (b.get_u64_le(), b.get_u16_le());
                held.insert(p, (epoch, RangeSet::decode(&mut b)?));
            }
        }
        if b.has_remaining() {
            return Err("queue state has trailing bytes".into());
        }
        Ok(QueueLog { settled, counts, pending, owed, sources, held })
    }
}

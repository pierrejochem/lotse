//! Producer state of one replica: what makes produce idempotent.
//!
//! Per producer id the replica keeps the epoch and its last
//! [`RECENT`] batches `(base_seq, last_seq, first, last)`. Before a leader
//! appends an idempotent batch it [`check`](ProducerState::check)s it:
//!
//! - a batch already written (same epoch and sequence range) is a
//!   duplicate: the client gets the offset of the original;
//! - an older epoch is fenced;
//! - a sequence that does not continue the last one (or start a newer
//!   epoch at 0) is out of order;
//! - an unknown producer is accepted with any sequence: it is new, or it was
//!   evicted (see below), and then a retry can no longer be recognised.
//!
//! A transaction marker raises its producer's epoch ([`crate::txns`]): the
//! producer's next batch starts the new epoch at sequence 0.
//!
//! The check must see the **whole log** of the replica, not only what is
//! committed: a retry may arrive while the original is still in flight, on
//! this leader or on a new one that inherited it. So there are two parts:
//! the committed state (built from applied batches, kept in snapshots) and
//! the tail (batches appended but not applied yet, rebuilt from the log on
//! restart and cut when Raft truncates the log).
//!
//! The committed state holds at most `max_producers` producers; applying a
//! batch of one more evicts the producer whose last write is oldest in the
//! log. Every replica applies the same batches in the same order with the
//! same limit, so every replica evicts the same producers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::Batch;

/// Batches remembered per producer; a client keeps at most this many in
/// flight per partition.
pub(crate) const RECENT: usize = 5;

/// Producers kept per replica unless configured otherwise.
pub const DEFAULT_MAX_PRODUCERS: usize = 10_000;

/// One batch of a producer, by Raft index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Written {
    base_seq: i32,
    last_seq: i32,
    first: u64,
    last: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Producer {
    epoch: u16,
    recent: VecDeque<Written>,
    /// First index of its last batch or marker.
    last_write: u64,
}

impl Producer {
    fn new(epoch: u16) -> Self {
        Producer { epoch, recent: VecDeque::new(), last_write: 0 }
    }

    /// Records a batch (`Some`) or a marker (`None`) at `first`.
    fn record(&mut self, epoch: u16, w: Option<Written>, first: u64) {
        if epoch > self.epoch {
            self.epoch = epoch;
            self.recent.clear();
        }
        if let Some(w) = w {
            self.recent.push_back(w);
            if self.recent.len() > RECENT {
                self.recent.pop_front();
            }
        }
        self.last_write = first;
    }
}

/// An idempotent batch or a marker in the log that is not applied yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailEntry {
    producer_id: u64,
    epoch: u16,
    /// `None` for a marker.
    written: Option<Written>,
    first: u64,
    term: u64,
}

/// What [`ProducerState::check`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Check {
    /// Append it.
    New,
    /// Already in the log at `first..=last`; `term` is the term of that
    /// entry if it is not applied yet, `None` once it is.
    Duplicate { first: u64, last: u64, term: Option<u64> },
    /// An older epoch than the producer's current one.
    Fenced { current: u16 },
    /// Does not continue the producer's sequence.
    OutOfOrder { expected: i32 },
}

/// See the module docs.
#[derive(Debug, Clone)]
pub(crate) struct ProducerState {
    committed: BTreeMap<u64, Producer>,
    /// `(last write, producer id)`, to evict the oldest.
    by_last_write: BTreeSet<(u64, u64)>,
    tail: BTreeMap<u64, TailEntry>,
    max_producers: usize,
}

/// `(base_seq, last_seq)` of a batch.
type Seqs = (i32, i32);

fn last_seq(base_seq: i32, records: u32) -> i32 {
    base_seq.wrapping_add(records.saturating_sub(1) as i32)
}

/// The producer fields of a batch, if it is idempotent: `None` sequences
/// for a marker, which carries a producer but no records of it.
fn producer_of(batch: &Batch) -> Option<(u64, u16, Option<Seqs>)> {
    let id = batch.producer_id();
    if id == 0 {
        return None;
    }
    if batch.attributes().control {
        return batch.attributes().transactional.then_some((id, batch.producer_epoch(), None));
    }
    // A transactional ack in a queue (ADR 0075) has no sequence: it is not
    // the producer's data, and settling twice changes nothing.
    if batch.attributes().kind != lotse_record::BatchKind::Data {
        return None;
    }
    let base = batch.base_sequence();
    Some((id, batch.producer_epoch(), Some((base, last_seq(base, batch.span())))))
}

impl ProducerState {
    pub(crate) fn new(max_producers: usize) -> Self {
        ProducerState {
            committed: BTreeMap::new(),
            by_last_write: BTreeSet::new(),
            tail: BTreeMap::new(),
            max_producers: max_producers.max(1),
        }
    }

    /// The producer as the whole log knows it: committed state, then the
    /// tail's batches of it in log order.
    fn view(&self, producer_id: u64) -> Option<(Producer, BTreeMap<u64, u64>)> {
        let mut p = self.committed.get(&producer_id).cloned();
        let mut terms = BTreeMap::new();
        for e in self.tail.values().filter(|e| e.producer_id == producer_id) {
            p.get_or_insert_with(|| Producer::new(e.epoch)).record(e.epoch, e.written, e.first);
            terms.insert(e.first, e.term);
        }
        p.map(|p| (p, terms))
    }

    /// Decides whether a leader may append `batch`. Non-idempotent batches
    /// (producer id 0) are always new.
    pub(crate) fn check(&self, batch: &Batch) -> Check {
        let Some((id, epoch, Some((base_seq, last_seq)))) = producer_of(batch) else { return Check::New };
        let Some((p, tail_terms)) = self.view(id) else { return Check::New };
        if epoch < p.epoch {
            return Check::Fenced { current: p.epoch };
        }
        if epoch > p.epoch {
            return if base_seq == 0 { Check::New } else { Check::OutOfOrder { expected: 0 } };
        }
        if let Some(w) = p.recent.iter().find(|w| w.base_seq == base_seq && w.last_seq == last_seq) {
            return Check::Duplicate { first: w.first, last: w.last, term: tail_terms.get(&w.first).copied() };
        }
        let expected = p.recent.back().map_or(0, |w| w.last_seq.wrapping_add(1));
        if base_seq == expected { Check::New } else { Check::OutOfOrder { expected } }
    }

    /// Notes a batch appended to the log at `first..=last` in `term`.
    pub(crate) fn appended(&mut self, first: u64, last: u64, term: u64, batch: &Batch) {
        if let Some((producer_id, epoch, seqs)) = producer_of(batch) {
            let written = seqs.map(|(base_seq, last_seq)| Written { base_seq, last_seq, first, last });
            self.tail.insert(first, TailEntry { producer_id, epoch, written, first, term });
        }
    }

    /// Raft removed every index from `from` on.
    pub(crate) fn truncated(&mut self, from: u64) {
        self.tail.split_off(&from);
    }

    /// Applies a committed batch at `first..=last`.
    pub(crate) fn applied(&mut self, first: u64, last: u64, batch: &Batch) {
        // Everything up to `last` is applied now, idempotent or not.
        let rest = self.tail.split_off(&(last + 1));
        self.tail = rest;
        let Some((id, epoch, seqs)) = producer_of(batch) else { return };
        let written = seqs.map(|(base_seq, last_seq)| Written { base_seq, last_seq, first, last });
        match self.committed.get_mut(&id) {
            Some(p) => {
                self.by_last_write.remove(&(p.last_write, id));
                p.record(epoch, written, first);
            }
            None => {
                let mut p = Producer::new(epoch);
                p.record(epoch, written, first);
                self.committed.insert(id, p);
            }
        }
        self.by_last_write.insert((first, id));
        while self.committed.len() > self.max_producers {
            let (_, evicted) = self.by_last_write.pop_first().expect("one entry per producer");
            self.committed.remove(&evicted);
        }
    }

    /// The committed state, for a snapshot.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32_le(self.committed.len() as u32);
        for (id, p) in &self.committed {
            b.put_u64_le(*id);
            b.put_u16_le(p.epoch);
            b.put_u64_le(p.last_write);
            b.put_u8(p.recent.len() as u8);
            for w in &p.recent {
                b.put_i32_le(w.base_seq);
                b.put_i32_le(w.last_seq);
                b.put_u64_le(w.first);
                b.put_u64_le(w.last);
            }
        }
        b.freeze()
    }

    /// Replaces everything with a snapshot's committed state; the log after
    /// the snapshot is noted again with [`ProducerState::appended`].
    pub(crate) fn restore(&mut self, mut b: &[u8]) -> Result<(), String> {
        let short = || "producer state is truncated".to_string();
        let mut committed = BTreeMap::new();
        let mut by_last_write = BTreeSet::new();
        if b.remaining() < 4 {
            return Err(short());
        }
        for _ in 0..b.get_u32_le() {
            if b.remaining() < 19 {
                return Err(short());
            }
            let (id, epoch, last_write, n) = (b.get_u64_le(), b.get_u16_le(), b.get_u64_le(), b.get_u8());
            let mut recent = VecDeque::new();
            for _ in 0..n {
                if b.remaining() < 24 {
                    return Err(short());
                }
                recent.push_back(Written {
                    base_seq: b.get_i32_le(),
                    last_seq: b.get_i32_le(),
                    first: b.get_u64_le(),
                    last: b.get_u64_le(),
                });
            }
            let p = Producer { epoch, recent, last_write };
            by_last_write.insert((p.last_write, id));
            committed.insert(id, p);
        }
        if b.has_remaining() {
            return Err("producer state has trailing bytes".into());
        }
        self.committed = committed;
        self.by_last_write = by_last_write;
        self.tail.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.committed.len()
    }
}

#[cfg(test)]
mod tests;

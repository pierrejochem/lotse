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
//!   forgotten (see below), and then a retry can no longer be recognised;
//! - an unknown producer is refused while the replica holds `max_producers`
//!   already ([`Check::Full`]): it may try again once one is forgotten.
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
//! **Forgetting** (ADR 0110). A producer is forgotten only after it has
//! written nothing for the expiration time, never to make room: a live
//! producer keeps its place, so its retries are always recognised. The
//! leader times idleness on its own clock ([`IdleClock`]) and writes the
//! producers to forget in an `ExpireProducers` batch, each with the index
//! of its last write as the leader saw it; every replica applies the batch
//! and forgets a producer only if that is still its last write, so one
//! that wrote in between stays. A new leader counts every producer as
//! active from when it took over: a failover can delay forgetting, never
//! hasten it. `max_producers` is a limit on admission, not a reason to
//! forget: while the replica holds that many, the leader refuses new
//! producers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::{Batch, Control, InternalControl};
use lotse_rt::Instant;

/// Batches remembered per producer; a client keeps at most this many in
/// flight per partition.
pub(crate) const RECENT: usize = 5;

/// Producers a replica admits unless configured otherwise.
pub const DEFAULT_MAX_PRODUCERS: usize = 10_000;

/// How long a producer may write nothing before it is forgotten, unless
/// configured otherwise: a day.
pub const DEFAULT_PRODUCER_EXPIRATION: Duration = Duration::from_secs(24 * 3600);

/// Producers named in one `ExpireProducers` batch at most.
const EXPIRE_BATCH: usize = 1_000;

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
    /// An unknown producer, and the replica holds `max_producers` already.
    Full,
    /// Does not continue the producer's sequence.
    OutOfOrder { expected: i32 },
}

/// See the module docs.
#[derive(Debug, Clone)]
pub(crate) struct ProducerState {
    committed: BTreeMap<u64, Producer>,
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
        ProducerState { committed: BTreeMap::new(), tail: BTreeMap::new(), max_producers: max_producers.max(1) }
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
        let Some((p, tail_terms)) = self.view(id) else {
            return if self.known() >= self.max_producers { Check::Full } else { Check::New };
        };
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
        if let Ok(Some(Control::Internal(InternalControl::ExpireProducers, payload))) = lotse_record::control(batch) {
            // A damaged list forgets nothing: every replica reads the same
            // bytes, so every replica keeps the same producers.
            for (id, last_write) in decode_expired(&payload).unwrap_or_default() {
                if self.committed.get(&id).is_some_and(|p| p.last_write == last_write) {
                    self.committed.remove(&id);
                }
            }
            return;
        }
        let Some((id, epoch, seqs)) = producer_of(batch) else { return };
        let written = seqs.map(|(base_seq, last_seq)| Written { base_seq, last_seq, first, last });
        self.committed.entry(id).or_insert_with(|| Producer::new(epoch)).record(epoch, written, first);
    }

    /// Producers the whole log knows: committed, or only in the tail.
    fn known(&self) -> usize {
        let fresh: BTreeSet<u64> =
            self.tail.values().map(|e| e.producer_id).filter(|id| !self.committed.contains_key(id)).collect();
        self.committed.len() + fresh.len()
    }

    /// For the leader: the producers `clock` has seen idle for `after`, with
    /// their last writes, at most [`EXPIRE_BATCH`] of them; `keep` names
    /// those that must stay whatever their age (an open transaction). Each
    /// one returned starts its idle time again, so it is not named twice
    /// while its batch is on its way.
    pub(crate) fn idle(
        &self,
        clock: &mut IdleClock,
        now: Instant,
        after: Duration,
        keep: impl Fn(u64) -> bool,
    ) -> Vec<(u64, u64)> {
        clock.seen.retain(|id, _| self.committed.contains_key(id));
        let mut out = Vec::new();
        for (&id, p) in &self.committed {
            let seen = clock.seen.entry(id).or_insert((p.last_write, now));
            if seen.0 != p.last_write || keep(id) {
                *seen = (p.last_write, now);
                continue;
            }
            if out.len() < EXPIRE_BATCH && now.saturating_duration_since(seen.1) >= after {
                seen.1 = now;
                out.push((id, p.last_write));
            }
        }
        out
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
            committed.insert(id, Producer { epoch, recent, last_write });
        }
        if b.has_remaining() {
            return Err("producer state has trailing bytes".into());
        }
        self.committed = committed;
        self.tail.clear();
        Ok(())
    }

    /// Producers it admits.
    pub(crate) fn max(&self) -> usize {
        self.max_producers
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.committed.len()
    }
}

/// The leader's view of how long each producer has been idle (see the
/// module docs): when it first saw each producer's last write. Kept in
/// memory only, and started afresh in every term.
#[derive(Debug, Default)]
pub(crate) struct IdleClock {
    /// The term it counts for.
    pub(crate) term: u64,
    /// Per producer: the last write it saw and since when.
    seen: BTreeMap<u64, (u64, Instant)>,
}

impl IdleClock {
    /// Starts counting afresh for `term`, if it counted for another.
    pub(crate) fn for_term(&mut self, term: u64) {
        if self.term != term {
            *self = IdleClock { term, seen: BTreeMap::new() };
        }
    }
}

/// The payload of an `ExpireProducers` batch.
pub(crate) fn encode_expired(producers: &[(u64, u64)]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + 16 * producers.len());
    b.put_u32_le(producers.len() as u32);
    for (id, last_write) in producers {
        b.put_u64_le(*id);
        b.put_u64_le(*last_write);
    }
    b.freeze()
}

fn decode_expired(mut b: &[u8]) -> Option<Vec<(u64, u64)>> {
    if b.remaining() < 4 {
        return None;
    }
    let n = b.get_u32_le() as usize;
    if b.remaining() != n.checked_mul(16)? {
        return None;
    }
    Some((0..n).map(|_| (b.get_u64_le(), b.get_u64_le())).collect())
}

#[cfg(test)]
mod tests;

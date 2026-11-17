//! A queue's length limit at work (ADR 0118): the leader counts what waits,
//! refuses a new batch that would pass the limit (`reject-publish`), or
//! retires the oldest waiting messages (`drop-head`).
//!
//! The count is the leader's alone and lives as long as its term. A new
//! leader reads the queue's backlog, [`READ_BYTES_PER_ROUND`] a round; what
//! it has not read yet counts at most as it can be: every unread offset a
//! message, every unread segment byte a message byte. So a new leader may
//! refuse while it reads, and never takes more than the limit.
//!
//! A batch counts from when the leader takes it: a write in flight or in an
//! open transaction waits as much as a committed one. A duplicate of a
//! batch already taken is answered as written and never refused, or its
//! producer would take its sequence as free.

use std::collections::BTreeSet;

use lotse_queue::{Amount, LengthLimit, Lengths, Overflow, Unread, message_bytes};
use lotse_record::Batch;
use lotse_rt::Runtime;

use super::{Clients, QueueConfig};
use crate::replica::{Error, Replica, StateMachine};
use crate::wire::GroupId;

/// Bytes of the log a round reads for a queue's count at most.
pub(super) const READ_BYTES_PER_ROUND: usize = 4 << 20;

/// A leader's count of one queue, for one term.
pub(super) struct Counting {
    term: u64,
    lengths: Lengths,
}

/// What a write must fit into: the queue's limit and what waits now.
#[derive(Debug, Clone)]
pub(super) struct Gate {
    limit: LengthLimit,
    waiting: Amount,
    queue: String,
}

impl Gate {
    /// Whether `adding` may be written: always with `drop-head`, which
    /// makes room afterwards.
    pub(super) fn check(&self, adding: Amount) -> Result<(), String> {
        if self.limit.overflow == Overflow::DropHead || self.limit.admits(self.waiting, adding) {
            return Ok(());
        }
        let limit = |n: u64| if n == 0 { "none".to_string() } else { n.to_string() };
        Err(format!(
            "queue {} is full: {} messages ({} bytes) wait and {} ({} bytes) more would pass max.length {} or \
             max.length.bytes {}; nothing was written",
            self.queue,
            self.waiting.messages,
            self.waiting.bytes,
            adding.messages,
            adding.bytes,
            limit(self.limit.max_messages),
            limit(self.limit.max_bytes),
        ))
    }
}

/// What a batch adds to a queue: its records and their bytes. A batch
/// that does not decode adds its record count; proposing it fails anyway.
pub(super) fn amount_of(batch: &Batch) -> Amount {
    match batch.records() {
        Ok(records) => Amount { messages: records.len() as u64, bytes: records.iter().map(message_bytes).sum::<u64>() },
        Err(_) => Amount { messages: u64::from(batch.record_count()), bytes: 0 },
    }
}

/// Where a leader's count of `replica`'s queue starts reading.
fn start<R: Runtime, S: StateMachine>(replica: &Replica<R, S>) -> u64 {
    let q = replica.queue().expect("a queue group");
    q.floor().max(replica.log().start_offset())
}

/// What `lengths` has not read of `replica`'s log, at most.
fn unread<R: Runtime, S: StateMachine>(lengths: &Lengths, replica: &Replica<R, S>) -> Unread {
    let q = replica.queue().expect("a queue group");
    let segments = replica.log().segments().map(|s| (s.next_offset, s.bytes));
    unread_of(lengths.scanned(), replica.applied(), replica.log().next_offset(), q, segments)
}

/// What is not read from `from` on, at most: every unsettled offset of
/// the applied part (below `applied`) and every offset beyond it a message,
/// every byte of a segment (`(next offset, bytes)`) that ends past `from`
/// a message byte.
fn unread_of(
    from: u64,
    applied: u64,
    end: u64,
    q: &lotse_queue::QueueLog,
    segments: impl Iterator<Item = (u64, u64)>,
) -> Unread {
    if from >= end {
        return Unread::default();
    }
    let messages = q.unsettled_in(from, applied.max(from)) + end.saturating_sub(applied.max(from));
    let bytes = segments.filter(|(next, _)| *next > from).map(|(_, b)| b).sum();
    Unread { messages, bytes }
}

impl Clients {
    /// The current term's count of queue `qc`, started now if this term has
    /// none yet.
    fn counting<R: Runtime, S: StateMachine>(&mut self, qc: &QueueConfig, replica: &Replica<R, S>) -> &mut Counting {
        let term = replica.raft_ref().term();
        let c =
            self.lengths.entry(qc.group).or_insert_with(|| Counting { term, lengths: Lengths::new(start(replica)) });
        if c.term != term {
            *c = Counting { term, lengths: Lengths::new(start(replica)) };
        }
        c
    }

    /// One round for a queue this node leads: forgets what is settled and
    /// reads on. A queue without a limit, or a node that does not lead,
    /// keeps no count.
    pub(super) async fn count<R: Runtime, S: StateMachine>(
        &mut self,
        qc: &QueueConfig,
        replica: &Replica<R, S>,
    ) -> Result<(), Error> {
        if !qc.length.is_set() || replica.raft_ref().role() != lotse_raft::Role::Leader {
            self.lengths.remove(&qc.group);
            return Ok(());
        }
        let q = replica.queue().expect("a queue group");
        let c = self.counting(qc, replica);
        c.lengths.settle(q);
        // Trimmed below what it would read: nothing there waits.
        let mut at = c.lengths.scanned().max(start(replica));
        let end = replica.log().next_offset();
        let mut budget = READ_BYTES_PER_ROUND;
        while at < end && budget > 0 {
            let read = replica.log().read(at, budget).await?;
            if read.is_empty() {
                break;
            }
            budget = budget.saturating_sub(read.len());
            for batch in crate::replica::batches(read)? {
                let a = batch.attributes();
                // Messages: produced data and dead letters. Markers, acks,
                // checkouts and the rest are not.
                let message =
                    !a.control && matches!(a.kind, lotse_record::BatchKind::Data | lotse_record::BatchKind::DeadLetter);
                if message {
                    for r in batch.records()? {
                        if !q.is_settled(r.offset) {
                            c.lengths.read(r.offset, message_bytes(&r));
                        }
                    }
                }
                at = batch.next_offset();
                c.lengths.read_to(at);
            }
        }
        Ok(())
    }

    /// What a write to queue `qc` must fit into, if it has a limit and this
    /// node leads it.
    pub(super) fn gate<R: Runtime, S: StateMachine>(
        &mut self,
        qc: &QueueConfig,
        replica: &Replica<R, S>,
    ) -> Option<Gate> {
        if !qc.length.is_set() || replica.raft_ref().role() != lotse_raft::Role::Leader {
            return None;
        }
        let term = replica.raft_ref().term();
        self.counting(qc, replica);
        let lengths = &self.lengths[&qc.group].lengths;
        let dispatcher = self.queues.get(&qc.group).filter(|s| s.term == term).map(|s| &s.dispatcher);
        let q = replica.queue().expect("a queue group");
        let waiting = lengths.waiting(q, dispatcher, unread(lengths, replica));
        Some(Gate { limit: qc.length, waiting, queue: qc.name.clone() })
    }

    /// A write to group `g` was taken: it ends before offset `end` and adds
    /// `adding`.
    pub(super) fn took(&mut self, g: GroupId, end: u64, adding: Amount) {
        if let Some(c) = self.lengths.get_mut(&g) {
            c.lengths.take(end, adding);
        }
    }

    /// [`Clients::took`] for a produced batch, if it was new here (`end`).
    pub(super) fn took_batch(&mut self, g: GroupId, end: Option<u64>, batch: &bytes::Bytes) {
        if let (Some(end), true) = (end, self.lengths.contains_key(&g))
            && let Ok((b, _)) = Batch::parse(batch)
        {
            self.took(g, end, amount_of(&b));
        }
    }

    /// For `drop-head`: the oldest messages queue `qc` must lose to be
    /// within its limit, at most `n`, none of `skip`, all below `limit`
    /// (what may be delivered). Nothing while its count has not read the
    /// whole log: it would retire on a bound, not a count.
    pub(super) fn heads<R: Runtime, S: StateMachine>(
        &self,
        qc: &QueueConfig,
        replica: &Replica<R, S>,
        limit: u64,
        skip: &BTreeSet<u64>,
        n: usize,
    ) -> Vec<u64> {
        if qc.length.overflow != Overflow::DropHead || !qc.length.is_set() {
            return Vec::new();
        }
        let term = replica.raft_ref().term();
        let end = replica.log().next_offset();
        let Some(c) = self.lengths.get(&qc.group).filter(|c| c.term == term && c.lengths.complete(end)) else {
            return Vec::new();
        };
        let dispatcher = self.queues.get(&qc.group).filter(|s| s.term == term).map(|s| &s.dispatcher);
        let q = replica.queue().expect("a queue group");
        c.lengths.heads(q, dispatcher, qc.length, limit, skip, n)
    }
}

#[cfg(test)]
mod tests;

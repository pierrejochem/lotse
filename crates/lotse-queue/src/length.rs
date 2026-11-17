//! A queue's length limit (ADR 0118): how many messages, and how many bytes
//! of them, may wait for delivery, and what happens beyond.
//!
//! What waits is what a consumer could still get: every unsettled message,
//! committed or not, in an open transaction or not, except those out with
//! a consumer (in flight, held by a transactional ack, or waiting for a
//! consumer to resume). As in RabbitMQ, a message handed out and not yet
//! acknowledged does not count.
//!
//! Only the leader judges, and only for itself: [`Lengths`] is its volatile
//! picture of the queue, built by reading the log, like the priority levels
//! (ADR 0078). A message's size is the bytes of its key, value and headers.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use lotse_record::Record;

use crate::{Dispatcher, QueueLog};

/// What a full queue does with what comes next (ADR 0118).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Overflow {
    /// Refuse it: a produce or publish fails with `QUEUE_FULL`, a dead
    /// letter waits in its source queue.
    #[default]
    RejectPublish,
    /// Take it, and retire the oldest waiting messages until the queue is
    /// within its limit again: into the dead-letter queue if it has one.
    DropHead,
}

impl Overflow {
    /// As a setting's value: `reject-publish` or `drop-head`.
    pub fn name(self) -> &'static str {
        match self {
            Overflow::RejectPublish => "reject-publish",
            Overflow::DropHead => "drop-head",
        }
    }

    /// The overflow a setting's value names.
    pub fn parse(s: &str) -> Option<Overflow> {
        match s {
            "reject-publish" => Some(Overflow::RejectPublish),
            "drop-head" => Some(Overflow::DropHead),
            _ => None,
        }
    }

    /// The code in a metadata record.
    pub fn code(self) -> u8 {
        match self {
            Overflow::RejectPublish => 0,
            Overflow::DropHead => 1,
        }
    }

    /// The overflow a code stands for.
    pub fn from_code(code: u8) -> Option<Overflow> {
        match code {
            0 => Some(Overflow::RejectPublish),
            1 => Some(Overflow::DropHead),
            _ => None,
        }
    }
}

/// How much waits, or comes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Amount {
    /// Messages.
    pub messages: u64,
    /// Bytes of their keys, values and headers.
    pub bytes: u64,
}

impl Amount {
    /// The sum of two amounts.
    pub fn plus(self, other: Amount) -> Amount {
        Amount { messages: self.messages + other.messages, bytes: self.bytes + other.bytes }
    }
}

/// A queue's length limit: `max.length`, `max.length.bytes`, `overflow`;
/// 0 for no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LengthLimit {
    /// Messages that may wait at most.
    pub max_messages: u64,
    /// Bytes that may wait at most.
    pub max_bytes: u64,
    /// What a full queue does.
    pub overflow: Overflow,
}

impl LengthLimit {
    /// Whether either limit is set.
    pub fn is_set(&self) -> bool {
        self.max_messages > 0 || self.max_bytes > 0
    }

    /// Whether `adding` fits on top of what `waiting`.
    pub fn admits(&self, waiting: Amount, adding: Amount) -> bool {
        let after = waiting.plus(adding);
        (self.max_messages == 0 || after.messages <= self.max_messages)
            && (self.max_bytes == 0 || after.bytes <= self.max_bytes)
    }

    /// Whether what waits is over the limit.
    pub fn exceeded(&self, waiting: Amount) -> bool {
        !self.admits(waiting, Amount::default())
    }
}

/// The bytes a message counts for: its key, value and headers.
pub fn message_bytes(r: &Record) -> u64 {
    let len = |b: &Option<bytes::Bytes>| b.as_ref().map_or(0, |b| b.len() as u64);
    len(&r.key) + len(&r.value) + r.headers.iter().map(|h| h.key.len() as u64 + len(&h.value)).sum::<u64>()
}

/// What the leader has not read yet, as an upper bound: the caller counts
/// it from the log ([`Lengths::scanned`] up to the log's end).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Unread {
    /// Messages there at most.
    pub messages: u64,
    /// Bytes there at most.
    pub bytes: u64,
}

/// A queue leader's picture of what waits (see the module docs).
#[derive(Debug, Clone, Default)]
pub struct Lengths {
    /// Unsettled messages read so far, and their sizes.
    sizes: BTreeMap<u64, u64>,
    /// The sum of `sizes`.
    bytes: u64,
    /// Every message below this offset has been read.
    scanned: u64,
    /// Writes taken since the last read reached them: where each ends, and
    /// what it adds.
    taken: VecDeque<(u64, Amount)>,
}

impl Lengths {
    /// A picture that starts reading at `from` (the ack floor, or the log's
    /// start if that is higher).
    pub fn new(from: u64) -> Lengths {
        Lengths { scanned: from, ..Lengths::default() }
    }

    /// Where the next read starts.
    pub fn scanned(&self) -> u64 {
        self.scanned
    }

    /// A message read at `offset`, `bytes` large.
    pub fn read(&mut self, offset: u64, bytes: u64) {
        if offset < self.scanned {
            return;
        }
        if let Some(old) = self.sizes.insert(offset, bytes) {
            self.bytes -= old;
        }
        self.bytes += bytes;
    }

    /// Everything below `at` has been read; writes taken that end there are
    /// counted by now.
    pub fn read_to(&mut self, at: u64) {
        self.scanned = self.scanned.max(at);
        while self.taken.front().is_some_and(|(end, _)| *end <= self.scanned) {
            self.taken.pop_front();
        }
    }

    /// A write was taken that ends at `end` and adds `amount`; counted until
    /// a read reaches it.
    pub fn take(&mut self, end: u64, amount: Amount) {
        if end > self.scanned {
            self.taken.push_back((end, amount));
        }
    }

    /// Forgets what `queue` has settled since.
    pub fn settle(&mut self, queue: &QueueLog) {
        let floor = queue.floor();
        let kept = self.sizes.split_off(&floor);
        self.bytes -= self.sizes.values().sum::<u64>();
        self.sizes = kept;
        let Some(&last) = self.sizes.keys().next_back() else { return };
        for (from, to) in queue.settled_from(floor) {
            if from > last {
                break;
            }
            let gone: Vec<u64> = self.sizes.range(from..to).map(|(o, _)| *o).collect();
            for o in gone {
                self.bytes -= self.sizes.remove(&o).unwrap_or(0);
            }
        }
    }

    /// Whether everything up to `end` is read.
    pub fn complete(&self, end: u64) -> bool {
        self.scanned >= end
    }

    /// The messages out with a consumer, which do not count.
    fn busy(queue: &QueueLog, dispatcher: Option<&Dispatcher>) -> BTreeSet<u64> {
        let mut busy: BTreeSet<u64> = queue.held_offsets().collect();
        match dispatcher {
            Some(d) => {
                busy.extend(d.busy());
                if !d.has_adopted() {
                    busy.extend(queue.checkouts().map(|(o, _)| o));
                }
            }
            None => busy.extend(queue.checkouts().map(|(o, _)| o)),
        }
        busy
    }

    /// What waits now: the messages read, those taken since, and `unread`,
    /// less those out with a consumer.
    pub fn waiting(&self, queue: &QueueLog, dispatcher: Option<&Dispatcher>, unread: Unread) -> Amount {
        let mut w = Amount { messages: self.sizes.len() as u64, bytes: self.bytes };
        for o in Self::busy(queue, dispatcher) {
            if let Some(b) = self.sizes.get(&o) {
                w.messages -= 1;
                w.bytes -= b;
            }
        }
        for (_, a) in &self.taken {
            w = w.plus(*a);
        }
        w.plus(Amount { messages: unread.messages, bytes: unread.bytes })
    }

    /// The oldest waiting messages below `limit` (what may be delivered)
    /// the queue must lose to be within `max`, at most `n`. Those in `skip`
    /// are being retired already: they are passed over, and no longer count.
    /// For `drop-head`, once everything is read.
    pub fn heads(
        &self,
        queue: &QueueLog,
        dispatcher: Option<&Dispatcher>,
        max: LengthLimit,
        limit: u64,
        skip: &BTreeSet<u64>,
        n: usize,
    ) -> Vec<u64> {
        let mut exceeding = self.waiting(queue, dispatcher, Unread::default());
        let busy = Self::busy(queue, dispatcher);
        // What is being retired already leaves: it no longer counts.
        for o in skip {
            if let Some(b) = self.sizes.get(o).filter(|_| !busy.contains(o)) {
                exceeding.messages -= 1;
                exceeding.bytes -= b;
            }
        }
        let mut out = Vec::new();
        for (&o, &b) in self.sizes.range(..limit) {
            if !max.exceeded(exceeding) || out.len() >= n {
                break;
            }
            if busy.contains(&o) || skip.contains(&o) || queue.is_settled(o) {
                continue;
            }
            out.push(o);
            exceeding.messages -= 1;
            exceeding.bytes = exceeding.bytes.saturating_sub(b);
        }
        out
    }
}

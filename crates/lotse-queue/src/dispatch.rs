//! Who gets which message: the queue leader's volatile delivery state.
//!
//! Subscriptions have a credit window in messages and bytes, refilled with
//! `Credit`. A delivered message is **in flight** with its subscription until
//! it is settled (acknowledged, or discarded on nack) or released (nack with
//! requeue, disconnect, ack timeout; an unsubscribe is not a release).
//! Released messages are delivered again before new ones, lowest offset
//! first, so a message keeps its place.
//!
//! A nack with requeue and an ack timeout persist the delivery count before
//! the message goes out again: [`Dispatcher::take`] and
//! [`Dispatcher::timed_out`] hold the messages back, and the caller releases
//! them once the count is committed. After a disconnect the count is only
//! kept in memory.
//!
//! None of this is replicated: after a failover the new leader starts with
//! an empty dispatcher and delivers every unsettled message again. That is
//! correct because only committed acks settle a message. A subscription
//! whose oldest message in flight is older than the ack timeout is ended and
//! its messages are held back as above (RabbitMQ's consumer timeout).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lotse_rt::Instant;

use crate::QueueLog;

/// A subscription; the caller chooses ids, unique per dispatcher.
pub type SubscriptionId = u32;

/// The connection a subscription belongs to.
pub type ConnId = u64;

#[derive(Debug, Clone)]
struct Subscription {
    conn: ConnId,
    credit_msgs: u32,
    credit_bytes: u32,
    /// Unsubscribed: no new deliveries; gone once nothing is in flight.
    closing: bool,
}

#[derive(Debug, Clone, Copy)]
struct InFlight {
    sub: SubscriptionId,
    at: Instant,
}

/// See the module docs.
#[derive(Debug, Clone)]
pub struct Dispatcher {
    subs: BTreeMap<SubscriptionId, Subscription>,
    in_flight: BTreeMap<u64, InFlight>,
    /// Released, to be delivered again before anything new.
    redeliver: BTreeSet<u64>,
    /// Deliveries since the persisted count (redeliveries after disconnect).
    extra: BTreeMap<u64, u16>,
    /// Offsets below this were handed out at least once by this leader.
    cursor: u64,
    /// Where the next round of assignment starts, for fairness.
    rotate: usize,
}

/// One message handed to a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Delivery {
    /// The message's offset.
    pub offset: u64,
    /// How often it has been delivered, this time included.
    pub delivery_count: u16,
}

impl Dispatcher {
    /// A dispatcher for a new leader term: everything unsettled from the
    /// ack floor on is to be delivered.
    pub fn new(queue: &QueueLog) -> Self {
        Dispatcher {
            subs: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            redeliver: BTreeSet::new(),
            extra: BTreeMap::new(),
            cursor: queue.floor(),
            rotate: 0,
        }
    }

    /// Adds subscription `id` with an initial credit window.
    pub fn subscribe(&mut self, id: SubscriptionId, conn: ConnId, credit_msgs: u32, credit_bytes: u32) {
        self.subs.insert(id, Subscription { conn, credit_msgs, credit_bytes, closing: false });
    }

    /// Whether `id` exists and belongs to `conn`.
    pub fn owns(&self, conn: ConnId, id: SubscriptionId) -> bool {
        self.subs.get(&id).is_some_and(|s| s.conn == conn)
    }

    /// Adds credit.
    pub fn credit(&mut self, id: SubscriptionId, msgs: u32, bytes: u32) {
        if let Some(s) = self.subs.get_mut(&id) {
            s.credit_msgs = s.credit_msgs.saturating_add(msgs);
            s.credit_bytes = s.credit_bytes.saturating_add(bytes);
        }
    }

    /// Stops deliveries to `id`. Its messages stay with it until they are
    /// acknowledged or released (as RabbitMQ's cancel).
    pub fn unsubscribe(&mut self, id: SubscriptionId) {
        if let Some(s) = self.subs.get_mut(&id) {
            s.closing = true;
        }
        self.drop_finished();
    }

    /// The connection is gone: its subscriptions end and their messages are
    /// released.
    pub fn disconnect(&mut self, conn: ConnId) {
        let gone: Vec<SubscriptionId> = self.subs.iter().filter(|(_, s)| s.conn == conn).map(|(id, _)| *id).collect();
        for id in gone {
            self.end(id);
        }
    }

    /// Ends a subscription and releases its messages; returns them.
    pub fn end(&mut self, id: SubscriptionId) -> Vec<u64> {
        self.subs.remove(&id);
        let mine: Vec<u64> = self.in_flight.iter().filter(|(_, f)| f.sub == id).map(|(o, _)| *o).collect();
        for o in &mine {
            self.in_flight.remove(o);
            self.redeliver.insert(*o);
            *self.extra.entry(*o).or_insert(0) += 1;
        }
        mine
    }

    /// Takes the messages among `offsets` that are in flight with one of
    /// `conn`'s subscriptions back (nack with requeue): they are not
    /// delivered again until [`Dispatcher::release`]. Returns them.
    pub fn take(&mut self, conn: ConnId, offsets: impl IntoIterator<Item = u64>) -> Vec<u64> {
        let mut taken = Vec::new();
        for o in offsets {
            let mine = self.in_flight.get(&o).and_then(|f| self.subs.get(&f.sub)).is_some_and(|s| s.conn == conn);
            if mine {
                self.in_flight.remove(&o);
                taken.push(o);
            }
        }
        self.drop_finished();
        taken
    }

    /// Releases messages for delivery again, once the caller has persisted
    /// their counts (so no extra count is kept here).
    pub fn release(&mut self, offsets: impl IntoIterator<Item = u64>) {
        for o in offsets {
            self.extra.remove(&o);
            if self.in_flight.remove(&o).is_some() || o < self.cursor {
                self.redeliver.insert(o);
            }
        }
        self.drop_finished();
    }

    /// Forgets everything the log has settled since (acks applied, from any
    /// connection or leader), and subscriptions that are done.
    pub fn sweep(&mut self, queue: &QueueLog) {
        self.in_flight.retain(|o, _| !queue.is_settled(*o));
        self.redeliver.retain(|o| !queue.is_settled(*o));
        self.extra.retain(|o, _| !queue.is_settled(*o));
        self.drop_finished();
    }

    fn drop_finished(&mut self) {
        let in_flight = &self.in_flight;
        self.subs.retain(|id, s| !s.closing || in_flight.values().any(|f| f.sub == *id));
    }

    /// Credit for messages left over all open subscriptions.
    pub fn open_credit(&self) -> u64 {
        self.subs.values().filter(|s| !s.closing && s.credit_bytes > 0).map(|s| u64::from(s.credit_msgs)).sum()
    }

    /// Offsets to deliver next, at most `max`, all below `limit` (the last
    /// stable offset): released ones first, then new ones.
    pub fn candidates(&mut self, queue: &QueueLog, limit: u64, max: usize) -> Vec<u64> {
        let mut out: Vec<u64> = self.redeliver.iter().copied().filter(|o| *o < limit).take(max).collect();
        let mut o = queue.next_unsettled(self.cursor);
        while out.len() < max && o < limit {
            if !self.in_flight.contains_key(&o) && !self.redeliver.contains(&o) {
                out.push(o);
            }
            o = queue.next_unsettled(o + 1);
        }
        out.sort_unstable();
        out
    }

    /// Hands `offsets` (all in one batch of `batch_bytes`) to subscriptions
    /// with credit, round robin. Returns what each one gets.
    pub fn assign(
        &mut self,
        queue: &QueueLog,
        offsets: &[u64],
        batch_bytes: u32,
        now: Instant,
    ) -> Vec<(SubscriptionId, Vec<Delivery>)> {
        let ids: Vec<SubscriptionId> = self.subs.keys().copied().collect();
        let mut out = Vec::new();
        let mut rest = offsets;
        for k in 0..ids.len() {
            if rest.is_empty() {
                break;
            }
            let id = ids[(self.rotate + k) % ids.len()];
            let s = self.subs.get_mut(&id).expect("listed");
            if s.closing || s.credit_msgs == 0 || s.credit_bytes == 0 {
                continue;
            }
            let n = (s.credit_msgs as usize).min(rest.len());
            let (mine, more) = rest.split_at(n);
            rest = more;
            s.credit_msgs -= n as u32;
            s.credit_bytes = s.credit_bytes.saturating_sub(batch_bytes);
            let mut deliveries = Vec::with_capacity(n);
            for &o in mine {
                self.in_flight.insert(o, InFlight { sub: id, at: now });
                self.redeliver.remove(&o);
                self.cursor = self.cursor.max(o + 1);
                let count =
                    queue.delivery_count(o).saturating_add(self.extra.get(&o).copied().unwrap_or(0)).saturating_add(1);
                deliveries.push(Delivery { offset: o, delivery_count: count });
            }
            out.push((id, deliveries));
        }
        self.rotate = self.rotate.wrapping_add(1);
        out
    }

    /// Ends subscriptions whose oldest message in flight is older than
    /// `timeout`; returns each with its messages, which are held back until
    /// the caller has persisted their counts and releases them.
    pub fn timed_out(&mut self, now: Instant, timeout: Duration) -> Vec<(SubscriptionId, Vec<u64>)> {
        let mut oldest: BTreeMap<SubscriptionId, Instant> = BTreeMap::new();
        for f in self.in_flight.values() {
            let e = oldest.entry(f.sub).or_insert(f.at);
            if f.at < *e {
                *e = f.at;
            }
        }
        let expired: Vec<SubscriptionId> =
            oldest.into_iter().filter(|(_, at)| now - *at >= timeout).map(|(id, _)| id).collect();
        expired
            .into_iter()
            .map(|id| {
                self.subs.remove(&id);
                let held: Vec<u64> = self.in_flight.iter().filter(|(_, f)| f.sub == id).map(|(o, _)| *o).collect();
                for o in &held {
                    self.in_flight.remove(o);
                }
                (id, held)
            })
            .collect()
    }

    /// The count to persist for a message held back: its deliveries so far.
    pub fn count_to_persist(&self, queue: &QueueLog, offset: u64) -> u16 {
        queue.delivery_count(offset).saturating_add(self.extra.get(&offset).copied().unwrap_or(0)).saturating_add(1)
    }

    /// The messages in flight with `id`.
    pub fn in_flight_of(&self, id: SubscriptionId) -> Vec<u64> {
        self.in_flight.iter().filter(|(_, f)| f.sub == id).map(|(o, _)| *o).collect()
    }

    /// Whether `offset` is in flight with any subscription.
    pub fn is_in_flight(&self, offset: u64) -> bool {
        self.in_flight.contains_key(&offset)
    }

    /// The subscriptions, open or closing.
    pub fn subscriptions(&self) -> impl Iterator<Item = (SubscriptionId, ConnId)> + '_ {
        self.subs.iter().map(|(id, s)| (*id, s.conn))
    }
}

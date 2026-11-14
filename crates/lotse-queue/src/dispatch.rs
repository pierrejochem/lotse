//! Who gets which message: the queue leader's volatile delivery state.
//!
//! Subscriptions have a credit window in messages and bytes, refilled with
//! `Credit`. A delivered message is **in flight** with its subscription until
//! it is settled (acknowledged, or discarded on nack) or released (nack with
//! requeue, disconnect, ack timeout; an unsubscribe is not a release).
//! Released messages are delivered again before new ones, lowest offset
//! first, so a message keeps its place.
//!
//! Every delivery's count is in the log before the message goes out: the
//! caller writes a CHECKOUT batch for what [`Dispatcher::assign`] hands out
//! and pushes once it is committed (ADR 0109), so a count is always the
//! persisted one plus this delivery. A nack with requeue and an ack timeout
//! write that the message is back (a QUEUE_STATE batch with its count)
//! before it goes out again: [`Dispatcher::take`] and
//! [`Dispatcher::timed_out`] hold the messages back, and the caller releases
//! them once that is committed. After a disconnect a message goes out again
//! at once; its next checkout replaces the old one.
//!
//! Who holds what is also in the log (ADR 0109): the leader writes a
//! CHECKOUT batch naming the consumer before it hands messages out. A new
//! leader [adopts](Dispatcher::adopt) those as **waiting** for their
//! consumer: not delivered to anyone else, until the consumer subscribes
//! again with its id and [resumes](Dispatcher::resume) them, or the resume
//! window ends, or their ack timeout does, when they are held back as below
//! and go out again. Everything else starts afresh with a new leader; that
//! is correct because only committed acks settle a message. A subscription
//! whose oldest message in flight is older than the ack timeout is ended and
//! its messages are held back as above (RabbitMQ's consumer timeout).
//!
//! A transactional ack (ADR 0075) takes messages out of flight for good:
//! [`Dispatcher::take_for_txn`] accepts only what the connection has in
//! flight (or what the same producer holds already, for a retry), and the
//! messages are then **held**: never delivered, never timed out, until the
//! log settles them (the transaction committed) or no transaction holds
//! them any more (it aborted), when they are delivered again.
//!
//! With a **single active consumer** (ADR 0076) only one subscription gets
//! messages; the others are standbys, in the order they subscribed. The
//! active one keeps its place until it ends. The next one takes over only
//! once no message is out with anyone: none in flight, none held back for a
//! count, none held by a transaction. Messages the last one had come back
//! first, so the queue's order holds across the handover.
//!
//! A message that must wait (ADR 0077: a delay its producer asked for, or
//! a requeueing nack's) is **deferred**: left out until its time, then
//! delivered before anything new. The caller works out the time from the
//! log, so a new leader defers the same messages again.
//!
//! On a queue with **priority levels** (ADR 0078) the caller tells the
//! dispatcher each message's level above 0 as it reads the log. Messages of
//! a higher level go first, lowest offset first within a level; level 0 is
//! everything else, in offset order as before. A message of a higher level
//! does not move the cursor: the level-0 messages it overtook are still to
//! come.

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
    /// The consumer's id, which outlives the subscription (ADR 0109).
    consumer: u64,
    credit_msgs: u32,
    credit_bytes: u32,
    /// Unsubscribed: no new deliveries; gone once nothing is in flight.
    closing: bool,
    /// When it subscribed, among this dispatcher's subscriptions: standbys
    /// take over in this order.
    order: u64,
}

#[derive(Debug, Clone, Copy)]
struct InFlight {
    sub: SubscriptionId,
    at: Instant,
}

/// A message an earlier leader handed to a consumer that has not
/// subscribed again yet.
#[derive(Debug, Clone, Copy)]
struct Waiting {
    consumer: u64,
    /// When it was handed out, on this leader's clock.
    at: Instant,
}

/// See the module docs.
#[derive(Debug, Clone)]
pub struct Dispatcher {
    subs: BTreeMap<SubscriptionId, Subscription>,
    in_flight: BTreeMap<u64, InFlight>,
    /// Released, to be delivered again before anything new.
    redeliver: BTreeSet<u64>,
    /// Offsets below this were handed out at least once by this leader.
    cursor: u64,
    /// Where the next round of assignment starts, for fairness.
    rotate: usize,
    /// Messages a transaction holds or is about to.
    held: BTreeMap<u64, Hold>,
    /// Messages held back until the caller releases them (nack with
    /// requeue, ack timeout).
    out: BTreeSet<u64>,
    /// One subscription at a time gets messages.
    single_active: bool,
    /// That subscription.
    active: Option<SubscriptionId>,
    /// Subscriptions that became active since the caller last asked.
    activated: Vec<SubscriptionId>,
    next_order: u64,
    /// Messages that wait, and until when (ms since the epoch).
    deferred: BTreeMap<u64, i64>,
    /// Messages an earlier leader handed out, waiting for their consumer.
    waiting: BTreeMap<u64, Waiting>,
    /// When this dispatcher adopted them: the resume window starts then.
    adopted: Option<Instant>,
    /// Unsettled messages above level 0, and their levels.
    levels: BTreeMap<u64, u8>,
    /// The same, highest level first: `(u8::MAX - level, offset)`.
    ranked: BTreeSet<(u8, u64)>,
}

/// A message a transactional ack took.
#[derive(Debug, Clone, Copy)]
struct Hold {
    producer: u64,
    /// The ack is applied: from now on the log says who holds it.
    applied: bool,
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
            cursor: queue.floor(),
            rotate: 0,
            held: BTreeMap::new(),
            out: BTreeSet::new(),
            single_active: false,
            active: None,
            activated: Vec::new(),
            next_order: 0,
            deferred: BTreeMap::new(),
            waiting: BTreeMap::new(),
            adopted: None,
            levels: BTreeMap::new(),
            ranked: BTreeSet::new(),
        }
    }

    /// Takes over what the log says is out with consumers (ADR 0109): those
    /// messages wait for their consumer to subscribe again. `now_ms` is the
    /// time on the clock the checkouts were written with (ms since the
    /// epoch), so that each keeps its age for the ack timeout.
    pub fn adopt(&mut self, queue: &QueueLog, now: Instant, now_ms: i64) {
        for (offset, c) in queue.checkouts() {
            let age = Duration::from_millis(u64::try_from(now_ms.saturating_sub(c.at_ms)).unwrap_or(0));
            let at = Instant::from_nanos(now.as_nanos().saturating_sub(age.as_nanos() as u64));
            self.waiting.insert(offset, Waiting { consumer: c.consumer, at });
        }
        self.adopted = Some(now);
    }

    /// Adds subscription `id` of consumer `consumer` with an initial credit
    /// window.
    pub fn subscribe(&mut self, id: SubscriptionId, conn: ConnId, consumer: u64, credit_msgs: u32, credit_bytes: u32) {
        let order = self.next_order;
        self.next_order += 1;
        self.subs.insert(id, Subscription { conn, consumer, credit_msgs, credit_bytes, closing: false, order });
    }

    /// Subscription `id` takes back what waits for its consumer: those
    /// messages are in flight with it from now on, keeping their age, and
    /// its connection may ack them in a transaction. With a single active
    /// consumer, one that holds messages becomes active if no one is and
    /// nothing else is out: it was the active one. Messages that wait for
    /// another consumer keep it a standby, since a connection that went
    /// without writing to the log leaves its checkouts behind, and those
    /// come back older than what an active one would get next. Returns how
    /// many it took.
    pub fn resume(&mut self, id: SubscriptionId) -> usize {
        let Some(consumer) = self.subs.get(&id).map(|s| s.consumer) else { return 0 };
        let mine: Vec<(u64, Instant)> =
            self.waiting.iter().filter(|(_, w)| w.consumer == consumer).map(|(o, w)| (*o, w.at)).collect();
        for &(o, at) in &mine {
            self.waiting.remove(&o);
            self.in_flight.insert(o, InFlight { sub: id, at });
        }
        let alone = self.waiting.is_empty()
            && self.in_flight.values().all(|f| f.sub == id)
            && self.out.is_empty()
            && self.held.is_empty();
        if !mine.is_empty() && self.single_active && self.active.is_none() && alone {
            self.active = Some(id);
            self.activated.push(id);
        }
        mine.len()
    }

    /// Whether subscription `id` is there and takes deliveries: not ended
    /// and not unsubscribed.
    pub fn is_open(&self, id: SubscriptionId) -> bool {
        self.subs.get(&id).is_some_and(|s| !s.closing)
    }

    /// The consumer of subscription `id`.
    pub fn consumer_of(&self, id: SubscriptionId) -> Option<u64> {
        self.subs.get(&id).map(|s| s.consumer)
    }

    /// Messages that waited for their consumer in vain: the resume window
    /// since [`Dispatcher::adopt`] is over, or their ack timeout is. They
    /// are held back like timed-out ones until the caller has persisted
    /// their counts and releases them.
    pub fn unclaimed(&mut self, now: Instant, window: Duration, timeout: Duration) -> Vec<u64> {
        let over = self.adopted.is_some_and(|a| now - a >= window);
        let gone: Vec<u64> =
            self.waiting.iter().filter(|(_, w)| over || now - w.at >= timeout).map(|(o, _)| *o).collect();
        for o in &gone {
            self.waiting.remove(o);
            self.out.insert(*o);
        }
        gone
    }

    /// Whether `offset` waits for the consumer an earlier leader handed it to.
    pub fn is_waiting(&self, offset: u64) -> bool {
        self.waiting.contains_key(&offset)
    }

    /// Turns single active consumer on or off (ADR 0076). Turned on, no one
    /// is active until every message out has come back.
    pub fn set_single_active(&mut self, on: bool) {
        if self.single_active != on {
            self.single_active = on;
            self.active = None;
        }
    }

    /// Whether `id` waits as a standby: single active consumer is on and
    /// another subscription gets the messages, or will once nothing is out.
    pub fn is_standby(&self, id: SubscriptionId) -> bool {
        self.single_active && self.active != Some(id)
    }

    /// The active subscription, if single active consumer is on and one is.
    pub fn active(&self) -> Option<SubscriptionId> {
        self.active.filter(|_| self.single_active)
    }

    /// Subscriptions that became active since the last call.
    pub fn take_activated(&mut self) -> Vec<SubscriptionId> {
        std::mem::take(&mut self.activated)
    }

    /// With a single active consumer: makes the first standby active if the
    /// active one is gone or closing and nothing is out with anyone; returns
    /// the active one. [`Dispatcher::assign`] does this too; the caller does
    /// it each round so a standby hears it is active before messages come.
    pub fn hand_over(&mut self, queue: &QueueLog) -> Option<SubscriptionId> {
        if !self.single_active {
            return None;
        }
        if self.active.is_some_and(|id| self.subs.get(&id).is_none_or(|s| s.closing)) {
            self.active = None;
        }
        // The queue's count covers transactions an earlier leader saw.
        let nothing_out = self.in_flight.is_empty()
            && self.waiting.is_empty()
            && self.out.is_empty()
            && self.held.is_empty()
            && queue.held() == 0;
        if self.active.is_none() && nothing_out {
            self.active = self.subs.iter().filter(|(_, s)| !s.closing).min_by_key(|(_, s)| s.order).map(|(id, _)| *id);
            self.activated.extend(self.active);
        }
        self.active
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
                self.out.insert(o);
                taken.push(o);
            }
        }
        self.drop_finished();
        taken
    }

    /// Releases those of `offsets` that are still in flight with `id`, for
    /// delivery again: what was handed to it but never reached it (its
    /// checkout was not written, or it ended or unsubscribed first, ADR
    /// 0109). One that went back and out to another since stays there.
    pub fn release_from(&mut self, id: SubscriptionId, offsets: impl IntoIterator<Item = u64>) {
        for o in offsets {
            if self.in_flight.get(&o).is_some_and(|f| f.sub == id) {
                self.in_flight.remove(&o);
                self.redeliver.insert(o);
            }
        }
        self.drop_finished();
    }

    /// Releases messages for delivery again, once the caller has persisted
    /// their counts.
    pub fn release(&mut self, offsets: impl IntoIterator<Item = u64>) {
        for o in offsets {
            self.held.remove(&o);
            self.out.remove(&o);
            if self.in_flight.remove(&o).is_some() || o < self.cursor {
                self.redeliver.insert(o);
            }
        }
        self.drop_finished();
    }

    /// Takes `offsets` for a transactional ack of `producer`, if each is in
    /// flight with a subscription of `conn` or taken for `producer` already
    /// (a retry). Returns the ones newly taken, which the caller releases if
    /// the ack is not written, or `None`, taking nothing, if one is neither.
    pub fn take_for_txn(&mut self, conn: ConnId, producer: u64, offsets: &[u64]) -> Option<Vec<u64>> {
        let mine = |o: &u64| self.in_flight.get(o).and_then(|f| self.subs.get(&f.sub)).is_some_and(|s| s.conn == conn);
        let again = |o: &u64| self.held.get(o).is_some_and(|h| h.producer == producer);
        if !offsets.iter().all(|o| mine(o) || again(o)) {
            return None;
        }
        let taken: Vec<u64> = offsets.iter().copied().filter(|o| !again(o)).collect();
        for &o in &taken {
            self.in_flight.remove(&o);
            self.held.insert(o, Hold { producer, applied: false });
        }
        self.drop_finished();
        Some(taken)
    }

    /// The transactional ack that took `offsets` is applied.
    pub fn held_applied(&mut self, offsets: &[u64]) {
        for o in offsets {
            if let Some(h) = self.held.get_mut(o) {
                h.applied = true;
            }
        }
    }

    /// Forgets everything the log has settled since (acks applied, from any
    /// connection or leader), and subscriptions that are done.
    pub fn sweep(&mut self, queue: &QueueLog) {
        self.in_flight.retain(|o, _| !queue.is_settled(*o));
        // Settled, or a transaction holds it now: no one waits for it.
        self.waiting.retain(|o, _| !queue.is_settled(*o) && queue.held_by(*o).is_none());
        self.redeliver.retain(|o| !queue.is_settled(*o));
        self.deferred.retain(|o, _| !queue.is_settled(*o));
        let settled: Vec<(u64, u8)> =
            self.levels.iter().filter(|(o, _)| queue.is_settled(**o)).map(|(o, l)| (*o, *l)).collect();
        for (o, l) in settled {
            self.levels.remove(&o);
            self.ranked.remove(&(u8::MAX - l, o));
        }
        // A held message whose transaction aborted goes out again.
        let mut back = Vec::new();
        self.held.retain(|o, h| {
            if queue.is_settled(*o) {
                return false;
            }
            if !h.applied {
                return true;
            }
            match queue.held_by(*o) {
                Some(p) => {
                    h.producer = p;
                    true
                }
                None => {
                    back.push(*o);
                    false
                }
            }
        });
        for o in back {
            self.redeliver.insert(o);
        }
        self.drop_finished();
    }

    fn drop_finished(&mut self) {
        let in_flight = &self.in_flight;
        self.subs.retain(|id, s| !s.closing || in_flight.values().any(|f| f.sub == *id));
    }

    /// Notes the priority level of the message at `offset`; level 0 is
    /// every message the dispatcher was not told about.
    pub fn set_level(&mut self, offset: u64, level: u8) {
        if let Some(old) = self.levels.remove(&offset) {
            self.ranked.remove(&(u8::MAX - old, offset));
        }
        if level > 0 {
            self.levels.insert(offset, level);
            self.ranked.insert((u8::MAX - level, offset));
        }
    }

    /// Forgets every level: the queue has none any more.
    pub fn clear_levels(&mut self) {
        self.levels.clear();
        self.ranked.clear();
    }

    /// The level the dispatcher knows for `offset`.
    pub fn level_of(&self, offset: u64) -> u8 {
        self.levels.get(&offset).copied().unwrap_or(0)
    }

    /// Leaves `offset` out until `due_ms` (ms since the epoch).
    pub fn defer(&mut self, offset: u64, due_ms: i64) {
        self.redeliver.remove(&offset);
        self.deferred.insert(offset, due_ms);
    }

    /// Deferred messages whose time has come go out again, before anything
    /// new. Returns the earliest time one still waits for, if one does.
    pub fn wake(&mut self, now_ms: i64) -> Option<i64> {
        let due: Vec<u64> = self.deferred.iter().filter(|(_, at)| **at <= now_ms).map(|(o, _)| *o).collect();
        for o in due {
            self.deferred.remove(&o);
            self.redeliver.insert(o);
        }
        self.deferred.values().min().copied()
    }

    /// Whether `offset` waits for its time.
    pub fn is_deferred(&self, offset: u64) -> bool {
        self.deferred.contains_key(&offset)
    }

    /// Credit for messages left over all open subscriptions; with a single
    /// active consumer, the active one's.
    pub fn open_credit(&self) -> u64 {
        let open = |s: &&Subscription| !s.closing && s.credit_bytes > 0;
        if self.single_active {
            return self.active.and_then(|id| self.subs.get(&id)).filter(open).map_or(0, |s| u64::from(s.credit_msgs));
        }
        self.subs.values().filter(open).map(|s| u64::from(s.credit_msgs)).sum()
    }

    /// Offsets to deliver next, at most `max`, all below `limit` (the last
    /// stable offset), in the order to deliver them: messages above level 0,
    /// highest level first and lowest offset first within one; then level
    /// 0, released ones and new ones by offset.
    ///
    /// `skip` holds messages that must not go out: the leader has decided
    /// to retire them (dead-letter or drop them) and waits for the claim to
    /// be applied.
    pub fn candidates(&mut self, queue: &QueueLog, limit: u64, max: usize, skip: &BTreeSet<u64>) -> Vec<u64> {
        // Higher levels first, in their own order.
        let ready = |o: &u64| {
            *o < limit
                && !skip.contains(o)
                && !self.in_flight.contains_key(o)
                && !self.held.contains_key(o)
                && !self.deferred.contains_key(o)
                && !self.out.contains(o)
                && !self.waiting.contains_key(o)
                && !queue.is_settled(*o)
        };
        let first: Vec<u64> = self.ranked.iter().map(|(_, o)| *o).filter(|o| ready(o)).take(max).collect();
        let max = max - first.len();
        let mut out: Vec<u64> = self
            .redeliver
            .iter()
            .copied()
            .filter(|o| *o < limit && !skip.contains(o) && !self.held.contains_key(o) && !self.deferred.contains_key(o))
            .filter(|o| !self.levels.contains_key(o))
            .take(max)
            .collect();
        let mut o = queue.next_unsettled(self.cursor);
        while out.len() < max && o < limit {
            // Held by a transaction this leader has not seen taken (an
            // earlier leader's): noted, so its end is noticed.
            if let Some(producer) = queue.held_by(o).filter(|_| !self.held.contains_key(&o)) {
                self.held.insert(o, Hold { producer, applied: true });
            }
            let free = !self.in_flight.contains_key(&o)
                && !self.waiting.contains_key(&o)
                && !self.out.contains(&o)
                && !self.redeliver.contains(&o)
                && !self.held.contains_key(&o)
                && !self.deferred.contains_key(&o)
                && !self.levels.contains_key(&o);
            if free && !skip.contains(&o) {
                out.push(o);
            }
            o = queue.next_unsettled(o + 1);
        }
        out.sort_unstable();
        let mut all = first;
        all.extend(out);
        all
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
        let ids: Vec<SubscriptionId> = if self.single_active {
            self.hand_over(queue).into_iter().collect()
        } else {
            self.subs.keys().copied().collect()
        };
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
                // Only a message found new moves the cursor: what goes out
                // again may lie beyond it (one an earlier leader handed out,
                // ADR 0109), and the messages before it are still to come.
                // So are level-0 messages a higher one overtook.
                let again = self.redeliver.remove(&o);
                if !again && !self.levels.contains_key(&o) {
                    self.cursor = self.cursor.max(o + 1);
                }
                let count = queue.delivery_count(o).saturating_add(1);
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
                    self.out.insert(*o);
                }
                (id, held)
            })
            .collect()
    }

    /// The count to persist for a message held back: its deliveries so
    /// far, each of which its checkout wrote (ADR 0109).
    pub fn count_to_persist(&self, queue: &QueueLog, offset: u64) -> u16 {
        queue.delivery_count(offset)
    }

    /// The messages in flight with `id`.
    pub fn in_flight_of(&self, id: SubscriptionId) -> Vec<u64> {
        self.in_flight.iter().filter(|(_, f)| f.sub == id).map(|(o, _)| *o).collect()
    }

    /// Whether a transaction holds `offset`, or a transactional ack took it.
    pub fn is_held(&self, offset: u64) -> bool {
        self.held.contains_key(&offset)
    }

    /// Whether `offset` is with a consumer as far as `dispatcher` knows, or,
    /// before it has adopted the log's checkouts (or without a dispatcher),
    /// as far as the log says (ADR 0109): not to be retired or expired.
    pub fn with_consumer(queue: &QueueLog, dispatcher: Option<&Dispatcher>, offset: u64) -> bool {
        match dispatcher {
            Some(d) if d.adopted.is_some() => d.is_busy(offset),
            Some(d) => d.is_busy(offset) || queue.checked_out(offset).is_some(),
            None => queue.checked_out(offset).is_some(),
        }
    }

    /// Whether `offset` is with a consumer: in flight, held, or waiting for
    /// the consumer an earlier leader handed it to.
    pub fn is_busy(&self, offset: u64) -> bool {
        self.is_in_flight(offset) || self.is_held(offset) || self.is_waiting(offset)
    }

    /// Whether `offset` is in flight with any subscription.
    pub fn is_in_flight(&self, offset: u64) -> bool {
        self.in_flight.contains_key(&offset)
    }

    /// Messages `id` may still be sent.
    pub fn credit_of(&self, id: SubscriptionId) -> u32 {
        self.subs.get(&id).map_or(0, |s| s.credit_msgs)
    }

    /// Messages delivered and not settled or released, over all
    /// subscriptions.
    pub fn in_flight_count(&self) -> u64 {
        self.in_flight.len() as u64
    }

    /// The subscriptions, open or closing.
    pub fn subscriptions(&self) -> impl Iterator<Item = (SubscriptionId, ConnId)> + '_ {
        self.subs.iter().map(|(id, s)| (*id, s.conn))
    }
}

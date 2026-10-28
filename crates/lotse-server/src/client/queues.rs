//! Serving queues: `Subscribe`, `Credit`, `Ack`, `Nack`, `Unsubscribe`, and
//! deliveries pushed to subscribers ([`lotse_queue`]).
//!
//! Every replica of a queue group keeps the queue's ack index
//! ([`lotse_queue::QueueLog`]) as it applies the log. The leader adds a
//! [`Dispatcher`] for its term, made when the first consumer subscribes and
//! dropped when the term ends, which ends every subscription with a
//! `NOT_LEADER` push; consumers subscribe again at the new leader, which
//! delivers every unsettled message again.
//!
//! The leader delivers only once it has applied an entry of its own term:
//! everything committed before, every ack included, is applied then, so an
//! acknowledged message is never delivered again. It delivers only below
//! the last stable offset (messages of open transactions wait), and only
//! what is applied.
//!
//! An `Ack` is a batch in the queue's log, answered once committed under the
//! same rule as a produce; the message counts as settled when the batch is
//! applied. A `Nack` either discards (an ack) or requeues: the messages are
//! taken back, their delivery counts are written, and they go out again once
//! that is committed. The ack timeout does the same for a subscription that
//! held a message too long, after ending it with an `ACK_TIMED_OUT` push.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lotse_proto::{
    AckRequest, CreditRequest, DeliverPush, DeliveredOffset, ErrorCode, LeaderHint, NackRequest, OutcomeResponse,
    Response, SubscribeRequest, SubscribeResponse, UnsubscribeRequest,
};
use lotse_queue::{ConnId, Dispatcher, QueueLog, SubscriptionId};
use lotse_raft::Role;
use lotse_record::Batch;
use lotse_rt::{Instant, Runtime};

use super::{Clients, ClusterView, Outbox, hint, outcome};
use crate::replica::{Error, Proposed, Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Why a request is refused: the code, and the leader if known.
type Refusal = (ErrorCode, Option<LeaderHint>);

/// A queue and its Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    /// Name clients look it up by.
    pub name: String,
    /// Id used by data-path requests.
    pub resource_id: u64,
    /// The group that replicates the queue (its only partition, 0).
    pub group: GroupId,
    /// How long a consumer may hold a message unacknowledged before its
    /// subscription ends and the message goes to someone else.
    pub ack_timeout: Duration,
}

impl QueueConfig {
    /// RabbitMQ's default consumer timeout.
    pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30 * 60);
}

/// How long an `Ack` or `Nack` may take to commit before it is answered with
/// `REQUEST_TIMED_OUT`.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Messages handed out per queue and round at most.
const DELIVER_PER_ROUND: usize = 1024;

/// The leader's delivery state for one queue in one term.
pub(super) struct Serving {
    term: u64,
    dispatcher: Dispatcher,
    conns: BTreeMap<ConnId, Arc<Outbox>>,
}

/// An ack or queue-state batch in flight.
pub(super) struct QueueWrite {
    reply: Option<(Arc<Outbox>, u32)>,
    deadline: Instant,
    group: GroupId,
    last: u64,
    term: u64,
    /// Messages to deliver again once the batch (their counts) is applied.
    release: Vec<u64>,
}

fn conn_id(outbox: &Arc<Outbox>) -> ConnId {
    // The outbox is kept alive in `Serving::conns` while it is in use, so
    // the address is not reused meanwhile.
    Arc::as_ptr(outbox) as usize as u64
}

fn answer(outbox: &Outbox, id: u32, error: ErrorCode, leader_hint: Option<LeaderHint>) {
    outbox.push(id, &Response::Outcome(OutcomeResponse { error, leader_hint }));
}

fn ended(subscription_id: u32, error: ErrorCode, leader_hint: Option<LeaderHint>) -> DeliverPush {
    DeliverPush { subscription_id, error, batch: bytes::Bytes::new(), deliveries: Vec::new(), leader_hint }
}

/// Offsets below this may be delivered and acknowledged: applied, and no
/// open transaction before them.
fn deliverable<R: Runtime, S: StateMachine>(replica: &Replica<R, S>) -> u64 {
    replica.txns().last_stable_offset(replica.applied()).min(replica.raft_ref().commit())
}

/// Whether the ranges are non-empty, well-formed and below `limit`.
///
/// Acks are checked against the leader's log end, not against what is
/// deliverable: a new leader may not have applied (or even learnt the
/// commit of) what its predecessor delivered. Acking any offset in the log
/// is safe: the ack's entry comes after it, so if the ack commits, so did
/// the entry it settles.
fn valid(ranges: &[(u64, u64)], limit: u64) -> bool {
    !ranges.is_empty() && ranges.iter().all(|&(f, t)| f < t && t <= limit)
}

impl Clients {
    fn queue_group(view: &ClusterView<'_>, queue_id: u64) -> Option<GroupId> {
        view.queues.iter().find(|q| q.resource_id == queue_id).map(|q| q.group)
    }

    /// The serving state for the replica's current term, made if needed.
    fn serving<R: Runtime, S: StateMachine>(&mut self, g: GroupId, replica: &Replica<R, S>) -> &mut Serving {
        let term = replica.raft_ref().term();
        if self.queues.get(&g).is_some_and(|s| s.term != term) {
            self.stop_serving(g, ErrorCode::NOT_LEADER, hint(replica.raft_ref()));
        }
        self.queues.entry(g).or_insert_with(|| Serving {
            term,
            dispatcher: Dispatcher::new(replica.queue().expect("a queue group")),
            conns: BTreeMap::new(),
        })
    }

    /// Ends every subscription to the queue in `g` with `error`.
    fn stop_serving(&mut self, g: GroupId, error: ErrorCode, leader_hint: Option<LeaderHint>) {
        let Some(s) = self.queues.remove(&g) else { return };
        for (id, conn) in s.dispatcher.subscriptions() {
            self.sub_groups.remove(&id);
            if let Some(outbox) = s.conns.get(&conn) {
                outbox.push_frame(lotse_proto::encode_push(&ended(id, error, leader_hint)));
            }
        }
    }

    pub(super) fn subscribe<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: SubscribeRequest,
        view: &ClusterView<'_>,
        groups: &Groups<R, S>,
    ) {
        let reply = |error, subscription_id, leader_hint| {
            outbox.push(correlation_id, &Response::Subscribe(SubscribeResponse { error, subscription_id, leader_hint }))
        };
        let group = Self::queue_group(view, req.queue_id);
        let Some((g, replica)) = group.and_then(|g| groups.get(&g).map(|r| (g, r))) else {
            return reply(view.absent(req.queue_id, group), 0, None);
        };
        if replica.raft_ref().role() != Role::Leader {
            return reply(ErrorCode::NOT_LEADER, 0, hint(replica.raft_ref()));
        }
        if req.exclusive {
            return reply(ErrorCode::INVALID_REQUEST, 0, None);
        }
        let mut id = self.next_subscription;
        while id == 0 || self.sub_groups.contains_key(&id) {
            id = id.wrapping_add(1);
        }
        self.next_subscription = id.wrapping_add(1);
        self.sub_groups.insert(id, g);
        let conn = conn_id(&outbox);
        let s = self.serving(g, replica);
        s.dispatcher.subscribe(id, conn, req.credit_msgs, req.credit_bytes);
        s.conns.insert(conn, outbox.clone());
        reply(ErrorCode::NONE, id, None);
    }

    /// The serving state and the subscription, if `outbox`'s connection
    /// owns it.
    fn owned(&mut self, outbox: &Arc<Outbox>, id: SubscriptionId) -> Option<&mut Serving> {
        let g = *self.sub_groups.get(&id)?;
        let s = self.queues.get_mut(&g)?;
        s.dispatcher.owns(conn_id(outbox), id).then_some(s)
    }

    pub(super) fn credit(&mut self, outbox: &Arc<Outbox>, req: CreditRequest) {
        // Never answered: credit for a subscription that has ended is moot.
        if let Some(s) = self.owned(outbox, req.subscription_id) {
            s.dispatcher.credit(req.subscription_id, req.add_msgs, req.add_bytes);
        }
    }

    pub(super) fn unsubscribe(&mut self, outbox: Arc<Outbox>, correlation_id: u32, req: UnsubscribeRequest) {
        match self.owned(&outbox, req.subscription_id) {
            Some(s) => {
                s.dispatcher.unsubscribe(req.subscription_id);
                answer(&outbox, correlation_id, ErrorCode::NONE, None);
            }
            None => answer(&outbox, correlation_id, ErrorCode::UNKNOWN_SUBSCRIPTION, None),
        }
    }

    /// The leader replica of a queue, or the answer refusing the request.
    fn queue_leader<'a, R: Runtime, S: StateMachine>(
        view: &ClusterView<'_>,
        groups: &'a mut Groups<R, S>,
        queue_id: u64,
    ) -> Result<(GroupId, &'a mut Replica<R, S>), Refusal> {
        let g = Self::queue_group(view, queue_id).ok_or((view.absent(queue_id, None), None))?;
        let replica = groups.get_mut(&g).ok_or((ErrorCode::NOT_LEADER, None))?;
        if replica.raft_ref().role() != Role::Leader {
            return Err((ErrorCode::NOT_LEADER, hint(replica.raft_ref())));
        }
        Ok((g, replica))
    }

    pub(super) fn ack<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: AckRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let (g, replica) = match Self::queue_leader(view, groups, req.queue_id) {
            Ok(x) => x,
            Err((e, h)) => return answer(&outbox, correlation_id, e, h),
        };
        // Acks inside a transaction are not supported yet.
        if req.txn_id.is_some() || !valid(&req.ranges, replica.raft_ref().last_index()) {
            return answer(&outbox, correlation_id, ErrorCode::INVALID_REQUEST, None);
        }
        let batch = lotse_record::ack_batch(&req.ranges).expect("ranges fit a batch");
        self.write_queue(Some((outbox, correlation_id)), g, replica, batch, Vec::new(), view.now);
    }

    pub(super) fn nack<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: NackRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let (g, replica) = match Self::queue_leader(view, groups, req.queue_id) {
            Ok(x) => x,
            Err((e, h)) => return answer(&outbox, correlation_id, e, h),
        };
        if req.delay_ms != 0 || !valid(&req.ranges, replica.raft_ref().last_index()) {
            return answer(&outbox, correlation_id, ErrorCode::INVALID_REQUEST, None);
        }
        if !req.requeue {
            let batch = lotse_record::ack_batch(&req.ranges).expect("ranges fit a batch");
            return self.write_queue(Some((outbox, correlation_id)), g, replica, batch, Vec::new(), view.now);
        }
        // Only this connection's messages in flight go back; nothing else
        // needs doing for the rest (not delivered here, or settled).
        let conn = conn_id(&outbox);
        let taken = match self.queues.get_mut(&g).filter(|s| s.term == replica.raft_ref().term()) {
            Some(s) => {
                let d = &mut s.dispatcher;
                let mine: Vec<u64> = d
                    .subscriptions()
                    .filter(|(_, c)| *c == conn)
                    .flat_map(|(id, _)| d.in_flight_of(id))
                    .filter(|o| req.ranges.iter().any(|&(f, t)| (f..t).contains(o)))
                    .collect();
                d.take(conn, mine)
            }
            None => Vec::new(),
        };
        if taken.is_empty() {
            return answer(&outbox, correlation_id, ErrorCode::NONE, None);
        }
        let q = replica.queue().expect("a queue group");
        let d = &self.queues[&g].dispatcher;
        let counts: Vec<(u64, u16)> = taken.iter().map(|&o| (o, d.count_to_persist(q, o))).collect();
        let batch = lotse_record::queue_state_batch(&counts).expect("counts fit a batch");
        self.write_queue(Some((outbox, correlation_id)), g, replica, batch, taken, view.now);
    }

    /// Proposes an ack or queue-state batch; `release` goes out again once
    /// it is applied.
    fn write_queue<R: Runtime, S: StateMachine>(
        &mut self,
        reply: Option<(Arc<Outbox>, u32)>,
        group: GroupId,
        replica: &mut Replica<R, S>,
        batch: bytes::Bytes,
        release: Vec<u64>,
        now: Instant,
    ) {
        match replica.propose_internal(batch) {
            Ok(Proposed { last, term: Some(term), .. }) => {
                let deadline = now + WRITE_TIMEOUT;
                self.queue_writes.push(QueueWrite { reply, deadline, group, last, term, release });
            }
            Ok(_) => unreachable!("a queue batch is never a duplicate"),
            Err(_) => {
                // Not the leader any more: the dispatcher goes with the term.
                if let Some((outbox, id)) = reply {
                    answer(&outbox, id, ErrorCode::NOT_LEADER, hint(replica.raft_ref()));
                }
            }
        }
    }

    /// Answers decided queue writes and releases their messages.
    pub(super) fn settle_queue_writes<R: Runtime, S: StateMachine>(&mut self, now: Instant, groups: &Groups<R, S>) {
        let writes = std::mem::take(&mut self.queue_writes);
        for mut w in writes {
            let Some(replica) = groups.get(&w.group) else {
                if let Some((outbox, id)) = w.reply.take() {
                    answer(&outbox, id, ErrorCode::UNKNOWN_RESOURCE, None);
                }
                continue;
            };
            let decided = outcome(replica.raft_ref(), w.last, w.term);
            if decided == Some(ErrorCode::NONE) && replica.applied() < w.last {
                self.queue_writes.push(w); // committed; applied in a moment
                continue;
            }
            match decided {
                Some(error) => {
                    if let Some((outbox, id)) = w.reply.take() {
                        let h = if error == ErrorCode::NOT_LEADER { hint(replica.raft_ref()) } else { None };
                        answer(&outbox, id, error, h);
                    }
                    // Released only into the term that took them back.
                    if let Some(s) = self.queues.get_mut(&w.group).filter(|s| s.term == w.term) {
                        s.dispatcher.release(w.release);
                    }
                }
                None => {
                    if now >= w.deadline
                        && let Some((outbox, id)) = w.reply.take()
                    {
                        answer(&outbox, id, ErrorCode::REQUEST_TIMED_OUT, None);
                    }
                    self.queue_writes.push(w);
                }
            }
        }
    }

    /// Ends what has ended (terms, connections, ack timeouts) and delivers
    /// to subscribers with credit.
    pub(super) async fn serve_queues<R: Runtime, S: StateMachine>(
        &mut self,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) -> Result<(), Error> {
        // A queue whose group has gone was deleted: its subscriptions end.
        let gone: Vec<GroupId> = self.queues.keys().copied().filter(|g| !groups.contains_key(g)).collect();
        for g in gone {
            self.stop_serving(g, ErrorCode::UNKNOWN_RESOURCE, None);
        }
        for qc in view.queues {
            let Some(replica) = groups.get_mut(&qc.group) else { continue };
            let (g, raft) = (qc.group, replica.raft_ref());
            let current = raft.role() == Role::Leader && self.queues.get(&g).is_some_and(|s| s.term == raft.term());
            if !current {
                self.stop_serving(g, ErrorCode::NOT_LEADER, hint(raft));
                continue;
            }
            // Acks committed in earlier terms may not be applied before.
            if raft.log().term_at(replica.applied()) != Some(raft.term()) {
                continue;
            }
            let limit = deliverable(replica);
            let s = self.queues.get_mut(&g).expect("serving");
            let q = replica.queue().expect("a queue group");
            s.dispatcher.sweep(q);

            // Connections that closed: their messages go to others.
            let closed: Vec<ConnId> =
                s.conns.iter().filter(|(_, o)| o.closed.load(Ordering::Acquire)).map(|(c, _)| *c).collect();
            for c in closed {
                for (id, _) in s.dispatcher.subscriptions().filter(|(_, x)| *x == c).collect::<Vec<_>>() {
                    self.sub_groups.remove(&id);
                }
                s.dispatcher.disconnect(c);
                s.conns.remove(&c);
            }

            // Subscriptions past the ack timeout end; their messages wait
            // until their delivery counts are written.
            let owners: BTreeMap<SubscriptionId, ConnId> = s.dispatcher.subscriptions().collect();
            let mut held = Vec::new();
            for (id, offsets) in s.dispatcher.timed_out(view.now, qc.ack_timeout) {
                self.sub_groups.remove(&id);
                if let Some(outbox) = owners.get(&id).and_then(|c| s.conns.get(c)) {
                    outbox.push_frame(lotse_proto::encode_push(&ended(id, ErrorCode::ACK_TIMED_OUT, None)));
                }
                held.extend(offsets);
            }
            if !held.is_empty() {
                let counts: Vec<(u64, u16)> = held.iter().map(|&o| (o, s.dispatcher.count_to_persist(q, o))).collect();
                let batch = lotse_record::queue_state_batch(&counts).expect("counts fit a batch");
                self.write_queue(None, g, replica, batch, held, view.now);
            }

            let s = self.queues.get_mut(&g).expect("serving");
            deliver(s, replica, limit, view.now).await?;
            let live: Vec<ConnId> = s.dispatcher.subscriptions().map(|(_, c)| c).collect();
            s.conns.retain(|c, _| live.contains(c));
        }
        Ok(())
    }
}

/// Hands out what credit allows, one stored batch at a time.
async fn deliver<R: Runtime, S: StateMachine>(
    s: &mut Serving,
    replica: &Replica<R, S>,
    limit: u64,
    now: Instant,
) -> Result<(), Error> {
    let q: &QueueLog = replica.queue().expect("a queue group");
    let mut budget = DELIVER_PER_ROUND;
    while budget > 0 {
        let max = (s.dispatcher.open_credit() as usize).min(budget);
        let offsets = s.dispatcher.candidates(q, limit, max);
        if offsets.is_empty() {
            return Ok(());
        }
        let owners: BTreeMap<SubscriptionId, ConnId> = s.dispatcher.subscriptions().collect();
        let mut delivered = 0;
        let mut rest = &offsets[..];
        while let Some(&first) = rest.first() {
            let read = replica.log().read(first, 1).await?;
            let (batch, _) = Batch::parse(&read)?;
            let raw = read.slice(..batch.len());
            let n = rest.iter().take_while(|o| **o < batch.next_offset()).count();
            let (here, more) = rest.split_at(n);
            rest = more;
            for (id, deliveries) in s.dispatcher.assign(q, here, raw.len() as u32, now) {
                delivered += deliveries.len();
                let push = DeliverPush {
                    subscription_id: id,
                    error: ErrorCode::NONE,
                    batch: raw.clone(),
                    deliveries: deliveries
                        .iter()
                        .map(|d| DeliveredOffset { offset: d.offset, delivery_count: d.delivery_count })
                        .collect(),
                    leader_hint: None,
                };
                if let Some(outbox) = owners.get(&id).and_then(|c| s.conns.get(c)) {
                    outbox.push_frame(lotse_proto::encode_push(&push));
                }
            }
        }
        if delivered == 0 {
            return Ok(()); // credit in messages, but none in bytes
        }
        budget = budget.saturating_sub(delivered);
    }
    Ok(())
}

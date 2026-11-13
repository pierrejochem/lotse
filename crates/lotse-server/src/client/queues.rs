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
//!
//! An `Ack` with a transactional id (ADR 0075) takes the connection's
//! messages out of flight and writes a transactional ack batch; the
//! transaction's marker settles them or gives them back. It is answered once
//! applied, with `MESSAGE_NOT_HELD` if the transaction does not hold every
//! message it named.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::BytesMut;
use lotse_proto::{
    AckRequest, CreditRequest, DeliverPush, DeliveredOffset, ErrorCode, LeaderHint, NackRequest, OutcomeResponse,
    Response, SubscribeRequest, SubscribeResponse, UnsubscribeRequest, VerifyTxnRequest,
};
use lotse_queue::{ConnId, Dispatcher, QueueLog, SubscriptionId};
use lotse_raft::Role;
use lotse_record::Batch;
use lotse_rt::{Instant, Runtime};

use super::{Clients, ClusterView, Outbox, hint, ids, outcome, retire, verify};
use crate::replica::{Error, ProposeError, Proposed, Replica, StateMachine};
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
    /// What the queue does with a message it will not deliver (again).
    pub limits: lotse_queue::Limits,
    /// The queue that receives such messages (a resource id), if any; they
    /// are dropped otherwise.
    pub dead_letter: Option<u64>,
    /// Single active consumer (ADR 0076): one subscription at a time gets
    /// messages, the others wait.
    pub single_active: bool,
    /// Priority levels (ADR 0078): messages of a higher level go first;
    /// 0 or 1 for none.
    pub priority_levels: u8,
}

impl QueueConfig {
    /// RabbitMQ's default consumer timeout.
    pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30 * 60);

    /// A queue with the default ack timeout and no limits.
    pub fn new(name: impl Into<String>, resource_id: u64, group: GroupId) -> Self {
        QueueConfig {
            name: name.into(),
            resource_id,
            group,
            ack_timeout: Self::DEFAULT_ACK_TIMEOUT,
            limits: lotse_queue::Limits::default(),
            dead_letter: None,
            single_active: false,
            priority_levels: 0,
        }
    }
}

/// How long an `Ack` or `Nack` may take to commit before it is answered with
/// `REQUEST_TIMED_OUT`.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Messages one transactional ack may name at most: each is held one by one.
const MAX_TXN_ACK: u64 = 10_000;

/// Messages handed out per queue and round at most.
const DELIVER_PER_ROUND: usize = 1024;

/// The leader's delivery state for one queue in one term.
pub(super) struct Serving {
    pub(super) term: u64,
    pub(super) dispatcher: Dispatcher,
    pub(super) conns: BTreeMap<ConnId, Arc<Outbox>>,
    /// Subscriptions that asked to hear when they become the single active
    /// consumer (ADR 0076).
    pub(super) notify: std::collections::BTreeSet<SubscriptionId>,
    /// Priority levels (ADR 0078): the queue's count as last seen, and how
    /// far the log has been read for them.
    levels: u8,
    scanned: u64,
}

impl Serving {
    /// Follows the queue's single active consumer setting, hands over if
    /// that is due, and tells whoever became active and asked.
    fn hand_over(&mut self, single_active: bool, queue: &QueueLog) {
        self.dispatcher.set_single_active(single_active);
        self.dispatcher.hand_over(queue);
        let owners: BTreeMap<SubscriptionId, ConnId> = self.dispatcher.subscriptions().collect();
        for id in self.dispatcher.take_activated() {
            if self.notify.contains(&id)
                && let Some(outbox) = owners.get(&id).and_then(|c| self.conns.get(c))
            {
                outbox.push_frame(lotse_proto::encode_push(&DeliverPush::activated(id)));
            }
        }
        self.notify.retain(|id| owners.contains_key(id));
    }
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
    /// Messages it acknowledges, counted once it is written.
    acked: u64,
    /// Messages it claims for retiring, and those it notes as forwarded:
    /// no longer in flight once it is decided.
    marks: retire::Marks,
    /// For a purge or requeue: what to answer once it is decided.
    action: Option<Action>,
    /// For a transactional ack: whose, and what.
    txn: Option<TxnAck>,
}

/// A transactional ack in flight (ADR 0075).
pub(super) struct TxnAck {
    producer: u64,
    /// The messages it names.
    offsets: Vec<u64>,
    /// The ones this request took out of flight: given back if it is not
    /// written.
    taken: Vec<u64>,
}

/// What a purge or requeue answers once its batch is decided (ADR 0064).
#[derive(Debug, Clone, Copy)]
pub(super) struct Action {
    pub(super) count: u64,
    pub(super) up_to: u64,
    pub(super) more: bool,
}

/// Answers a purge or requeue.
pub(super) fn answer_action(outbox: &Outbox, id: u32, error: ErrorCode, action: Action, hint: Option<LeaderHint>) {
    let ok = error == ErrorCode::NONE;
    outbox.push(
        id,
        &Response::QueueAction(lotse_proto::QueueActionResponse {
            error,
            error_msg: None,
            count: if ok { action.count } else { 0 },
            up_to: action.up_to,
            more: ok && action.more,
            leader_hint: hint,
        }),
    );
}

/// An ack or queue-state batch, and what follows once it is applied.
pub(super) struct QueueBatch {
    pub(super) batch: bytes::Bytes,
    /// Messages to deliver again.
    pub(super) release: Vec<u64>,
    /// Messages it acknowledges, for metrics.
    pub(super) acked: u64,
    pub(super) marks: retire::Marks,
    /// For a purge or requeue, how to answer.
    pub(super) action: Option<Action>,
    /// For a transactional ack.
    pub(super) txn: Option<TxnAck>,
}

impl QueueBatch {
    pub(super) fn new(batch: bytes::Bytes) -> Self {
        QueueBatch { batch, release: Vec::new(), acked: 0, marks: retire::Marks::default(), action: None, txn: None }
    }
}

/// Answers a queue write: an outcome, or for a purge or requeue the count.
fn reply_to(outbox: &Outbox, id: u32, error: ErrorCode, hint: Option<LeaderHint>, action: Option<Action>) {
    match action {
        Some(a) => answer_action(outbox, id, error, a, hint),
        None => answer(outbox, id, error, hint),
    }
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
pub(super) fn deliverable<R: Runtime, S: StateMachine>(replica: &Replica<R, S>) -> u64 {
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
            notify: std::collections::BTreeSet::new(),
            levels: 0,
            scanned: 0,
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
            let resp = SubscribeResponse { error, subscription_id, leader_hint, standby: false };
            outbox.push(correlation_id, &Response::Subscribe(resp))
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
        let single_active = view.queues.iter().any(|q| q.group == g && q.single_active);
        let s = self.serving(g, replica);
        s.dispatcher.subscribe(id, conn, req.credit_msgs, req.credit_bytes);
        s.conns.insert(conn, outbox.clone());
        // The answer says whether it waits; a push follows when it is its
        // turn, if it asked.
        // Becoming active now, it hears so in the answer.
        s.hand_over(single_active, replica.queue().expect("a queue group"));
        if req.notify_active {
            s.notify.insert(id);
        }
        let standby = s.dispatcher.is_standby(id);
        let resp = SubscribeResponse { error: ErrorCode::NONE, subscription_id: id, leader_hint: None, standby };
        outbox.push(correlation_id, &Response::Subscribe(resp));
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
    pub(super) fn queue_leader<'a, R: Runtime, S: StateMachine>(
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
        if !valid(&req.ranges, replica.raft_ref().last_index()) {
            return answer(&outbox, correlation_id, ErrorCode::INVALID_REQUEST, None);
        }
        if req.txn_id.is_some() {
            return self.txn_ack(outbox, correlation_id, g, req, view.now, replica);
        }
        let batch = lotse_record::ack_batch(&req.ranges).expect("ranges fit a batch");
        let acked = req.ranges.iter().map(|&(f, t)| t.saturating_sub(f)).sum();
        let what = QueueBatch { acked, ..QueueBatch::new(batch) };
        self.write_queue(Some((outbox, correlation_id)), g, replica, what, view.now);
    }

    /// An ack within a transaction: asks the coordinator first if the
    /// producer has no transaction open in the queue yet (ADR 0057).
    fn txn_ack<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        g: GroupId,
        req: AckRequest,
        now: Instant,
        replica: &mut Replica<R, S>,
    ) {
        let count: u64 = req.ranges.iter().map(|&(f, t)| t - f).sum();
        let Some((producer, epoch)) = req.producer.filter(|(p, _)| *p != 0 && count <= MAX_TXN_ACK) else {
            return answer(&outbox, correlation_id, ErrorCode::INVALID_REQUEST, None);
        };
        let mut batch = BytesMut::from(&lotse_record::ack_batch(&req.ranges).expect("ranges fit a batch")[..]);
        lotse_record::set_producer(&mut batch, producer, epoch, -1).expect("a whole batch");
        lotse_record::set_transactional(&mut batch, true).expect("a whole batch");
        let batch = batch.freeze();
        let ack = verify::WaitingAck { outbox, correlation_id, group: g, producer, ranges: req.ranges, batch };
        match replica.opens_txn(&ack.batch) {
            Some((producer_id, epoch)) => {
                let key = verify::Key { group: g, producer_id, epoch };
                let (tenant, txn_id) = ids::split(req.txn_id.as_deref().expect("a transactional ack"));
                let request = VerifyTxnRequest {
                    tenant: tenant.to_string(),
                    txn_id: txn_id.to_string(),
                    producer_id,
                    producer_epoch: epoch,
                    resource_id: req.queue_id,
                    partition: 0,
                };
                self.verify.wait(key, request, ack);
            }
            None => self.write_txn_ack(ack, replica, now),
        }
    }

    /// Takes the messages a transactional ack names and writes it, or says
    /// why not.
    pub(super) fn write_txn_ack<R: Runtime, S: StateMachine>(
        &mut self,
        ack: verify::WaitingAck,
        replica: &mut Replica<R, S>,
        now: Instant,
    ) {
        let verify::WaitingAck { outbox, correlation_id, group, producer, ranges, batch } = ack;
        let offsets: Vec<u64> = ranges.iter().flat_map(|&(f, t)| f..t).collect();
        let term = replica.raft_ref().term();
        let taken = self
            .queues
            .get_mut(&group)
            .filter(|s| s.term == term && replica.raft_ref().role() == Role::Leader)
            .and_then(|s| s.dispatcher.take_for_txn(conn_id(&outbox), producer, &offsets));
        let Some(taken) = taken else {
            return answer(&outbox, correlation_id, ErrorCode::MESSAGE_NOT_HELD, None);
        };
        let what = QueueBatch { txn: Some(TxnAck { producer, offsets, taken }), ..QueueBatch::new(batch) };
        self.write_queue(Some((outbox, correlation_id)), group, replica, what, now);
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
        // A delay is for a message that goes back (ADR 0077).
        let delayed = req.delay_ms != 0 && !req.requeue;
        if delayed || !valid(&req.ranges, replica.raft_ref().last_index()) {
            return answer(&outbox, correlation_id, ErrorCode::INVALID_REQUEST, None);
        }
        if !req.requeue {
            let batch = lotse_record::ack_batch(&req.ranges).expect("ranges fit a batch");
            return self.write_queue(Some((outbox, correlation_id)), g, replica, QueueBatch::new(batch), view.now);
        }
        // Only this connection's messages in flight go back; nothing else
        // needs doing for the rest (not delivered here, or settled). A Nack
        // that names its subscription takes back only what that one holds:
        // sent late, by a consumer that ended, it must not take what a newer
        // subscription on the connection received since (ADR 0106).
        let conn = conn_id(&outbox);
        let named = req.subscription_id;
        let taken = match self.queues.get_mut(&g).filter(|s| s.term == replica.raft_ref().term()) {
            Some(s) => {
                let d = &mut s.dispatcher;
                let mine: Vec<u64> = d
                    .subscriptions()
                    .filter(|(id, c)| *c == conn && named.is_none_or(|n| n == *id))
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
        // With a delay, the time it may go out again is written with its
        // count, so that every replica, and so a new leader, knows it.
        let batch = if req.delay_ms == 0 {
            lotse_record::queue_state_batch(&counts)
        } else {
            let due_ms = (view.unix_ms as i64).saturating_add(i64::from(req.delay_ms));
            let timers: Vec<lotse_record::Timer> = counts
                .iter()
                .map(|&(offset, delivery_count)| lotse_record::Timer { offset, delivery_count, due_ms })
                .collect();
            lotse_record::timer_batch(&timers)
        }
        .expect("counts fit a batch");
        let what = QueueBatch { release: taken, ..QueueBatch::new(batch) };
        self.write_queue(Some((outbox, correlation_id)), g, replica, what, view.now);
    }

    /// Proposes an ack or queue-state batch; see [`QueueBatch`].
    pub(super) fn write_queue<R: Runtime, S: StateMachine>(
        &mut self,
        reply: Option<(Arc<Outbox>, u32)>,
        group: GroupId,
        replica: &mut Replica<R, S>,
        QueueBatch { batch, release, acked, marks, action, txn }: QueueBatch,
        now: Instant,
    ) {
        let proposed = if txn.is_some() { replica.propose_txn_ack(batch) } else { replica.propose_internal(batch) };
        match proposed {
            Ok(Proposed { last, term: Some(term), .. }) => {
                let deadline = now + WRITE_TIMEOUT;
                self.queue_writes.push(QueueWrite {
                    reply,
                    deadline,
                    group,
                    last,
                    term,
                    release,
                    acked,
                    marks,
                    action,
                    txn,
                });
            }
            Ok(_) => unreachable!("a queue batch is never a duplicate"),
            Err(e) => {
                // Not the leader any more (the dispatcher goes with the
                // term), or a fenced producer.
                let (error, h) = match e {
                    ProposeError::ProducerFenced { .. } => (ErrorCode::PRODUCER_FENCED, None),
                    _ => (ErrorCode::NOT_LEADER, hint(replica.raft_ref())),
                };
                if let Some(t) = txn
                    && let Some(s) = self.queues.get_mut(&group).filter(|s| s.term == replica.raft_ref().term())
                {
                    s.dispatcher.release(t.taken);
                }
                if let Some((outbox, id)) = reply {
                    reply_to(&outbox, id, error, h, action);
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
                    reply_to(&outbox, id, ErrorCode::UNKNOWN_RESOURCE, None, w.action);
                }
                continue;
            };
            let decided = outcome(replica.raft_ref(), w.last, w.term);
            if decided == Some(ErrorCode::NONE) && replica.applied() < w.last {
                self.queue_writes.push(w); // committed; applied in a moment
                continue;
            }
            match decided {
                Some(mut error) => {
                    if error == ErrorCode::NONE {
                        self.stats.group(w.group).acked += w.acked;
                    }
                    if let Some(t) = &w.txn {
                        let serving = self.queues.get_mut(&w.group).filter(|s| s.term == w.term);
                        if error == ErrorCode::NONE {
                            let q = replica.queue().expect("a queue group");
                            if !t.offsets.iter().all(|o| q.held_by(*o) == Some(t.producer)) {
                                error = ErrorCode::MESSAGE_NOT_HELD;
                            }
                            if let Some(s) = serving {
                                s.dispatcher.held_applied(&t.taken);
                            }
                        } else if let Some(s) = serving {
                            s.dispatcher.release(t.taken.iter().copied());
                        }
                    }
                    if let Some((outbox, id)) = w.reply.take() {
                        let h = if error == ErrorCode::NOT_LEADER { hint(replica.raft_ref()) } else { None };
                        reply_to(&outbox, id, error, h, w.action);
                    }
                    // Released only into the term that took them back.
                    if let Some(s) = self.queues.get_mut(&w.group).filter(|s| s.term == w.term) {
                        s.dispatcher.release(w.release);
                    }
                    if let Some(r) = self.retiring.get_mut(&w.group).filter(|r| r.term() == w.term) {
                        r.decided(&w.marks);
                    }
                }
                None => {
                    if now >= w.deadline
                        && let Some((outbox, id)) = w.reply.take()
                    {
                        reply_to(&outbox, id, ErrorCode::REQUEST_TIMED_OUT, None, w.action);
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
        self.retiring.retain(|g, _| groups.contains_key(g));
        for qc in view.queues {
            let Some(replica) = groups.get_mut(&qc.group) else { continue };
            let (g, raft) = (qc.group, replica.raft_ref());
            if raft.role() != Role::Leader {
                self.retiring.remove(&g);
            } else if raft.log().term_at(replica.applied()) == Some(raft.term()) {
                self.retire(qc, view, replica).await?;
            }
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
                self.write_queue(None, g, replica, QueueBatch { release: held, ..QueueBatch::new(batch) }, view.now);
            }

            // A leader that no quorum answered within an election timeout
            // may be cut off and replaced by now: it hands out nothing and
            // hands no single active consumer over until a quorum answers
            // again or check-quorum steps it down (ADR 0097).
            if !replica.raft_ref().leads_with_quorum() {
                continue;
            }
            let s = self.queues.get_mut(&g).expect("serving");
            s.hand_over(qc.single_active, replica.queue().expect("a queue group"));
            read_levels(s, replica, limit, qc.priority_levels).await?;
            let none = std::collections::BTreeSet::new();
            let skip = self.retiring.get(&g).map_or(&none, retire::Retiring::claiming);
            let due = Due { now: view.now, unix_ms: view.unix_ms, limits: qc.limits };
            let (delivered, bytes, late) = deliver(s, replica, limit, due, skip).await?;
            if late && let Some(r) = self.retiring.get_mut(&g) {
                r.scan_now();
            }
            let stats = self.stats.group(g);
            stats.delivered += delivered;
            stats.fetch_bytes += bytes;
            let live: Vec<ConnId> = s.dispatcher.subscriptions().map(|(_, c)| c).collect();
            s.conns.retain(|c, _| live.contains(c));
        }
        Ok(())
    }
}

/// Bytes of the log a round reads for priority levels at most: a new
/// leader reads the whole backlog once, over several rounds.
const LEVEL_BYTES_PER_ROUND: usize = 1 << 20;

/// Tells the dispatcher the levels of messages appended since the last
/// round, from their `x-lotse-priority` headers (ADR 0078). A level above
/// the queue's highest counts as its highest.
async fn read_levels<R: Runtime, S: StateMachine>(
    s: &mut Serving,
    replica: &Replica<R, S>,
    limit: u64,
    levels: u8,
) -> Result<(), Error> {
    let levels = if levels > 1 { levels } else { 0 };
    if s.levels != levels {
        s.dispatcher.clear_levels();
        (s.levels, s.scanned) = (levels, 0);
    }
    if levels == 0 {
        return Ok(());
    }
    let q = replica.queue().expect("a queue group");
    let mut at = s.scanned.max(q.floor()).max(replica.log().start_offset());
    let mut budget = LEVEL_BYTES_PER_ROUND;
    while at < limit && budget > 0 {
        let read = replica.log().read(at, budget).await?;
        if read.is_empty() {
            break;
        }
        budget = budget.saturating_sub(read.len());
        for batch in crate::replica::batches(read)? {
            if batch.base_offset() >= limit {
                break;
            }
            if batch.attributes().kind == lotse_record::BatchKind::Data && !batch.attributes().control {
                for r in batch.records()? {
                    let level = lotse_record::priority::of(&r).min(levels - 1);
                    if level > 0 && !q.is_settled(r.offset) {
                        s.dispatcher.set_level(r.offset, level);
                    }
                }
            }
            at = batch.next_offset();
        }
    }
    s.scanned = at;
    Ok(())
}

/// The messages among `here` (all in `batch`) that may not go out yet at
/// `now_ms`, with when they may: the delay their producer asked for,
/// counted from the leader's append time, or a requeueing nack's, whichever
/// is later (ADR 0077).
fn waiting(q: &QueueLog, batch: &Batch, here: &[u64], now_ms: i64) -> Result<Vec<(u64, i64)>, Error> {
    let mut asked = BTreeMap::new();
    // Only as produced: a dead letter, or one requeued back, does not wait
    // a second time.
    if batch.attributes().kind == lotse_record::BatchKind::Data {
        for r in batch.records()? {
            let ms = lotse_record::delay::of(&r);
            if ms > 0 && here.contains(&r.offset) {
                asked.insert(r.offset, batch.base_timestamp().saturating_add(ms as i64));
            }
        }
    }
    let mut out = Vec::new();
    for &o in here {
        let at = asked.get(&o).copied().max(q.due_at(o));
        if let Some(at) = at.filter(|at| *at > now_ms) {
            out.push((o, at));
        }
    }
    Ok(out)
}

/// When deliveries happen, and what the queue allows.
#[derive(Clone, Copy)]
struct Due {
    now: Instant,
    unix_ms: u64,
    limits: lotse_queue::Limits,
}

/// Hands out what credit allows, one stored batch at a time; returns how
/// many messages it delivered, how many bytes it sent, and whether it met
/// messages that may not be delivered (too old, or delivered as often as the
/// queue allows). Those wait for the next round's claim: a delivery never
/// happens past the limits, whatever the scan has found.
async fn deliver<R: Runtime, S: StateMachine>(
    s: &mut Serving,
    replica: &Replica<R, S>,
    limit: u64,
    due: Due,
    skip: &std::collections::BTreeSet<u64>,
) -> Result<(u64, u64, bool), Error> {
    let Due { now, unix_ms, limits } = due;
    s.dispatcher.wake(unix_ms as i64);
    let mut skip = skip.clone();
    let mut late = false;
    let q: &QueueLog = replica.queue().expect("a queue group");
    let mut budget = DELIVER_PER_ROUND;
    let mut total = 0;
    let mut bytes = 0u64;
    while budget > 0 {
        let max = (s.dispatcher.open_credit() as usize).min(budget);
        let offsets = s.dispatcher.candidates(q, limit, max, &skip);
        if offsets.is_empty() {
            return Ok((total, bytes, late));
        }
        // Delivered as often as allowed already (a release this round may
        // have counted one more): the next round's claim retires them.
        let over: Vec<u64> = offsets
            .iter()
            .copied()
            .filter(|o| limits.exhausted(u32::from(q.delivery_count(*o)) + u32::from(s.dispatcher.extra_of(*o))))
            .collect();
        if !over.is_empty() {
            skip.extend(over);
            late = true;
            budget = budget.saturating_sub(1);
            continue;
        }
        let owners: BTreeMap<SubscriptionId, ConnId> = s.dispatcher.subscriptions().collect();
        let (mut delivered, mut skipped) = (0, false);
        let mut rest = &offsets[..];
        while let Some(&first) = rest.first() {
            let read = replica.log().read(first, 1).await?;
            let (batch, _) = Batch::parse(&read)?;
            let raw = read.slice(..batch.len());
            // Candidates come in the order to deliver them, not by offset
            // (ADR 0078): the run that lies in this batch goes together.
            let n = rest.iter().take_while(|o| (batch.base_offset()..batch.next_offset()).contains(*o)).count();
            let (here, more) = rest.split_at(n);
            rest = more;
            if limits.expired(batch.max_timestamp(), unix_ms) {
                skip.extend(here.iter().copied());
                (late, skipped) = (true, true);
                continue;
            }
            // Messages that wait for their time (ADR 0077) are left out.
            let waiting = waiting(q, &batch, here, unix_ms as i64)?;
            let here: Vec<u64> = if waiting.is_empty() {
                here.to_vec()
            } else {
                for &(o, at) in &waiting {
                    s.dispatcher.defer(o, at);
                }
                skipped = true;
                here.iter().copied().filter(|o| !waiting.iter().any(|(w, _)| w == o)).collect()
            };
            if here.is_empty() {
                continue;
            }
            let here = &here[..];
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
                    bytes += raw.len() as u64;
                }
            }
        }
        if delivered == 0 && !skipped {
            return Ok((total, bytes, late)); // credit in messages, but none in bytes
        }
        total += delivered as u64;
        budget = budget.saturating_sub(delivered.max(1));
    }
    Ok((total, bytes, late))
}

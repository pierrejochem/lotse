//! A queue's limits at work (ADR 0055): messages that were delivered as
//! often as the queue allows (`max.deliveries`) or waited longer than its
//! `message.ttl.ms` leave the queue, into its dead-letter queue if it has one.
//!
//! Only the leader decides, and every decision is a batch in the queue's own
//! log, so replicas never consult a clock or a count of their own:
//!
//! 1. **Claim.** The leader proposes a RETIRE batch naming the messages
//!    ([`lotse_record::retire_out_batch`]); without a dead-letter queue it
//!    proposes an ack instead, which drops them. Applying a claim settles a
//!    message that is still unsettled and records that it is *owed* to the
//!    dead-letter queue. The log orders a claim and a consumer's ack: exactly
//!    one settles the message.
//! 2. **Forward.** The leader of a queue forwards what the applied state
//!    owes, read from its own log, to the dead-letter queue's leader with the
//!    internal `DeadLetter` request. The receiving queue recognises a message
//!    it has taken from the same source and offset, so sending again after a
//!    timeout or a failover is safe.
//! 3. **Done.** Once the dead-letter queue has committed the messages, the
//!    leader proposes a second RETIRE batch that clears the debt. A new
//!    leader starts from the applied state, so what is still owed is
//!    forwarded again, whatever the old leader got done.
//!
//! The data of an owed message stays in the log: retention stops at the
//! lowest owed offset ([`lotse_queue::QueueLog::retention_floor`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{DeadLetterRequest, ErrorCode};
use lotse_queue::Owed;
use lotse_record::dead_letter::Reason;
use lotse_record::{Batch, DeadLetterInfo};
use lotse_rt::{Instant, Runtime};

use super::queues::{QueueBatch, deliverable};
use super::{Clients, ClusterView, Outbox};
use crate::replica::{Error, Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Messages claimed in one batch at most.
const MAX_CLAIM: usize = 512;

/// Batches the TTL scan reads per round at most.
const SCAN_BATCHES: usize = 64;

/// Bytes of messages sent to a dead-letter queue in one request, about; a
/// batch may hold 1 MiB.
const FORWARD_BYTES: usize = 256 * 1024;

/// Messages sent in one request at most.
const FORWARD_MESSAGES: usize = 256;

/// How long to wait before forwarding again after a failure.
const RETRY_AFTER: Duration = Duration::from_millis(200);

/// What a queue write marks while it is in flight.
#[derive(Debug, Default, Clone)]
pub(super) struct Marks {
    /// Messages a claim names.
    pub(super) claims: Vec<u64>,
    /// Messages a done note names.
    pub(super) finishes: Vec<u64>,
}

/// The leader's retiring state for one queue in one term.
pub(super) struct Retiring {
    term: u64,
    /// Claims in flight: these messages are not delivered meanwhile.
    claiming: BTreeSet<u64>,
    /// Done notes in flight.
    finishing: BTreeSet<u64>,
    /// A forward job is running.
    forwarding: bool,
    retry_at: Option<Instant>,
    /// The Unix time (ms) before which the TTL scan has nothing to find.
    scan_at_ms: i64,
    /// The TTL the scan time was computed for.
    scan_ttl_ms: u64,
}

impl Retiring {
    pub(super) fn new(term: u64) -> Self {
        Retiring {
            term,
            claiming: BTreeSet::new(),
            finishing: BTreeSet::new(),
            forwarding: false,
            retry_at: None,
            scan_at_ms: 0,
            scan_ttl_ms: 0,
        }
    }

    pub(super) fn term(&self) -> u64 {
        self.term
    }

    /// Messages a leader has decided to retire and must not deliver.
    pub(super) fn claiming(&self) -> &BTreeSet<u64> {
        &self.claiming
    }

    /// Marks messages as decided on, so they are not delivered until the
    /// write that settles them is (a purge, a requeue).
    pub(super) fn hold(&mut self, offsets: &[u64]) {
        self.claiming.extend(offsets.iter().copied());
    }

    /// The leader saw an expired message on its way out: look for more soon.
    pub(super) fn scan_now(&mut self) {
        self.scan_at_ms = 0;
    }

    /// A write was decided: what it marked is no longer in flight.
    pub(super) fn decided(&mut self, marks: &Marks) {
        for o in &marks.claims {
            self.claiming.remove(o);
        }
        for o in &marks.finishes {
            self.finishing.remove(o);
        }
    }
}

/// Messages for the node to forward to their dead-letter queue.
#[derive(Debug, Clone)]
pub(crate) struct ForwardJob {
    /// The source queue's group and the term this leader decided in.
    pub(crate) group: GroupId,
    pub(crate) term: u64,
    /// The dead-letter queue.
    pub(crate) target: u64,
    /// The source offsets sent.
    pub(crate) offsets: Vec<u64>,
    /// One batch per message.
    pub(crate) batches: Vec<Bytes>,
}

/// Merges sorted offsets into ranges.
pub(super) fn ranges(offsets: &[u64]) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = Vec::new();
    for &o in offsets {
        match out.last_mut() {
            Some((_, to)) if *to == o => *to += 1,
            _ => out.push((o, o + 1)),
        }
    }
    out
}

impl Clients {
    /// Takes the forward jobs the leaders have prepared.
    pub(crate) fn take_forward_jobs(&mut self) -> Vec<ForwardJob> {
        std::mem::take(&mut self.forward_jobs)
    }

    /// One round for a queue this node leads, once it has applied an entry of
    /// its own term: claim what its limits retire and forward what it owes.
    pub(super) async fn retire<R: Runtime, S: StateMachine>(
        &mut self,
        qc: &super::QueueConfig,
        view: &ClusterView<'_>,
        replica: &mut Replica<R, S>,
    ) -> Result<(), Error> {
        let g = qc.group;
        let term = replica.raft_ref().term();
        if self.retiring.get(&g).is_some_and(|r| r.term != term) {
            self.retiring.remove(&g);
        }
        let st = self.retiring.entry(g).or_insert_with(|| Retiring::new(term));
        let dispatcher = self.queues.get(&g).filter(|s| s.term == term).map(|s| &s.dispatcher);
        let q = replica.queue().expect("a queue group");
        st.claiming.retain(|o| !q.is_settled(*o));
        let owed_now: BTreeSet<u64> = q.owed().map(|(o, _)| o).collect();
        st.finishing.retain(|o| owed_now.contains(o));

        // 1. Claims.
        let limits = qc.limits;
        let mut claims: BTreeMap<u64, (Reason, u16)> = BTreeMap::new();
        for (o, n) in lotse_queue::exhausted(q, dispatcher, limits, &st.claiming, MAX_CLAIM) {
            claims.insert(o, (Reason::MaxDeliveries, n));
        }
        if limits.message_ttl_ms > 0
            && (view.unix_ms as i64 >= st.scan_at_ms || st.scan_ttl_ms != limits.message_ttl_ms)
        {
            st.scan_ttl_ms = limits.message_ttl_ms;
            let now = view.unix_ms as i64;
            let (mut o, end) = (q.next_unsettled(q.floor()), deliverable(replica));
            let mut wake = now + (limits.message_ttl_ms / 4).clamp(10, 500) as i64;
            let mut budget = SCAN_BATCHES;
            while o < end && budget > 0 && claims.len() < MAX_CLAIM {
                if st.claiming.contains(&o) || dispatcher.is_some_and(|d| d.is_in_flight(o)) {
                    o = q.next_unsettled(o + 1);
                    continue;
                }
                budget -= 1;
                let read = replica.log().read(o, 1).await?;
                let (batch, _) = Batch::parse(&read)?;
                let appended = batch.max_timestamp();
                if !limits.expired(appended, view.unix_ms) {
                    // Appended in order: nothing after this is older.
                    wake = wake.min(limits.expires_at(appended));
                    break;
                }
                for x in o..batch.next_offset() {
                    let busy = st.claiming.contains(&x) || dispatcher.is_some_and(|d| d.is_in_flight(x));
                    if !q.is_settled(x) && !busy {
                        let prior = u32::from(q.delivery_count(x)) + dispatcher.map_or(0, |d| u32::from(d.extra_of(x)));
                        claims.entry(x).or_insert((Reason::Expired, prior.min(u32::from(u16::MAX)) as u16));
                    }
                }
                o = q.next_unsettled(batch.next_offset());
            }
            // More to read next round, or sleep until the head expires.
            st.scan_at_ms = if budget == 0 { now } else { wake };
        }
        if !claims.is_empty() {
            let offsets: Vec<u64> = claims.keys().copied().collect();
            let batch = match qc.dead_letter {
                None => lotse_record::ack_batch(&ranges(&offsets)).expect("ranges fit a batch"),
                Some(target) => {
                    let entries: Vec<(u64, Reason, u16)> = claims.iter().map(|(o, (r, n))| (*o, *r, *n)).collect();
                    lotse_record::retire_out_batch(target, &entries).expect("claims fit a batch")
                }
            };
            let acked = if qc.dead_letter.is_none() { offsets.len() as u64 } else { 0 };
            st.claiming.extend(offsets.iter().copied());
            let what =
                QueueBatch { acked, marks: Marks { claims: offsets, finishes: Vec::new() }, ..QueueBatch::new(batch) };
            self.write_queue(None, g, replica, what, view.now);
            return Ok(());
        }

        // 2. What is owed.
        let q = replica.queue().expect("a queue group");
        if st.forwarding || st.retry_at.is_some_and(|t| view.now < t) {
            return Ok(());
        }
        let owed: Vec<(u64, Owed)> =
            q.owed().filter(|(o, _)| !st.finishing.contains(o)).take(FORWARD_MESSAGES).collect();
        let Some(&(_, first)) = owed.first() else { return Ok(()) };
        let target = first.target;
        let floor = q.retention_floor();
        let mut batches = Vec::new();
        let mut offsets = Vec::new();
        let mut dropped = Vec::new();
        let (mut bytes, gone) = (0, view.deleted.contains(&target));
        for (o, w) in owed.iter().filter(|(_, w)| w.target == target) {
            if bytes >= FORWARD_BYTES {
                break;
            }
            if gone {
                // The queue it was owed to was deleted since: nowhere to go.
                dropped.push(*o);
                continue;
            }
            let read = replica.log().read(*o, 1).await?;
            let (batch, _) = Batch::parse(&read)?;
            let record = batch.records()?.into_iter().find(|r| r.offset == *o);
            let info = DeadLetterInfo {
                queue: &qc.name,
                queue_id: qc.resource_id,
                offset: *o,
                floor,
                reason: w.reason,
                deliveries: w.deliveries,
            };
            // A message too large to carry the extra headers cannot be sent.
            match record.and_then(|r| lotse_record::dead_letter_item(&r, info).ok()) {
                Some(item) => {
                    bytes += item.len();
                    offsets.push(*o);
                    batches.push(item);
                }
                None => dropped.push(*o),
            }
        }
        if !offsets.is_empty() {
            st.forwarding = true;
            self.forward_jobs.push(ForwardJob { group: g, term, target, offsets, batches });
        }
        if !dropped.is_empty() {
            self.finish(g, replica, dropped, view.now);
        }
        Ok(())
    }

    /// Notes in the queue's log that `offsets` are not owed any more.
    fn finish<R: Runtime, S: StateMachine>(
        &mut self,
        g: GroupId,
        replica: &mut Replica<R, S>,
        offsets: Vec<u64>,
        now: Instant,
    ) {
        let Some(st) = self.retiring.get_mut(&g) else { return };
        st.finishing.extend(offsets.iter().copied());
        let batch = lotse_record::retire_done_batch(&offsets).expect("offsets fit a batch");
        let what = QueueBatch { marks: Marks { claims: Vec::new(), finishes: offsets }, ..QueueBatch::new(batch) };
        self.write_queue(None, g, replica, what, now);
    }

    /// A forward job ended: with `ok` the dead-letter queue has the messages
    /// committed, and the debt can be cleared.
    pub(crate) fn forwarded<R: Runtime, S: StateMachine>(
        &mut self,
        group: GroupId,
        term: u64,
        offsets: Vec<u64>,
        ok: bool,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let Some(st) = self.retiring.get_mut(&group).filter(|r| r.term == term) else { return };
        st.forwarding = false;
        if !ok {
            st.retry_at = Some(view.now + RETRY_AFTER);
            return;
        }
        st.retry_at = None;
        let Some(replica) = groups.get_mut(&group) else { return };
        let q = replica.queue().expect("a queue group");
        let owed_now: BTreeSet<u64> = q.owed().map(|(o, _)| o).collect();
        let still: Vec<u64> = offsets.into_iter().filter(|o| owed_now.contains(o)).collect();
        if !still.is_empty() {
            self.finish(group, replica, still, view.now);
        }
    }

    /// `DeadLetter`: messages from another queue's leader. Answered once
    /// they are committed here.
    pub(super) fn dead_letter<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: DeadLetterRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let refuse = |code, hint| {
            outbox.push(
                id,
                &lotse_proto::Response::Outcome(lotse_proto::OutcomeResponse { error: code, leader_hint: hint }),
            )
        };
        let Some(qc) = view.queues.iter().find(|q| q.resource_id == req.queue_id) else {
            return refuse(view.absent(req.queue_id, None), None);
        };
        let Some(replica) = groups.get_mut(&qc.group) else { return refuse(ErrorCode::NOT_LEADER, None) };
        if replica.raft_ref().role() != lotse_raft::Role::Leader {
            return refuse(ErrorCode::NOT_LEADER, super::hint(replica.raft_ref()));
        }
        // What the sender says it sends: single data messages, each naming a
        // source queue of the receiving queue's own tenant.
        let tenant = view.access.resources.get(&req.queue_id).map(|(_, t)| t);
        let mut items = Vec::new();
        for raw in &req.batches {
            let ok = Batch::parse(raw).ok().filter(|(b, rest)| {
                rest.is_empty()
                    && b.record_count() == 1
                    && !b.attributes().control
                    && !b.attributes().transactional
                    && b.attributes().kind == lotse_record::BatchKind::Data
            });
            let Some((batch, _)) = ok else { return refuse(ErrorCode::INVALID_REQUEST, None) };
            let origin = batch.records().ok().and_then(|r| lotse_record::dead_letter_origin(&r[0]).ok());
            let Some(origin) = origin else { return refuse(ErrorCode::INVALID_REQUEST, None) };
            let source = view.access.resources.get(&origin.queue_id).map(|(_, t)| t);
            if source.is_some() && tenant.is_some() && source != tenant {
                return refuse(ErrorCode::INVALID_REQUEST, None);
            }
            items.push(batch);
        }
        if items.is_empty() {
            return refuse(ErrorCode::NONE, None);
        }
        match lotse_record::dead_letter_batch(&items, view.unix_ms as i64) {
            Ok(batch) => self.write_queue(Some((outbox, id)), qc.group, replica, QueueBatch::new(batch), view.now),
            Err(_) => refuse(ErrorCode::INVALID_REQUEST, None),
        }
    }
}

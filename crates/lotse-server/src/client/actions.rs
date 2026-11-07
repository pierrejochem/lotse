//! Operators' actions on a queue's ready messages (ADR 0064), served by the
//! queue's leader:
//!
//! - **Purge** settles them with one ack batch, as RabbitMQ's purge does.
//!   Messages consumers hold stay theirs to ack or nack; messages of open
//!   transactions wait for their marker.
//! - **Requeue** moves the messages of a dead-letter queue back to the queues
//!   they came from (named in their dead-letter headers). It is the dead-
//!   letter move of ADR 0055 the other way round: the leader claims the
//!   messages in a RETIRE batch owed to their source, forwards them with the
//!   internal `DeadLetter` request, and the source recognises repeats, so a
//!   message moves once across failovers.
//!
//! Each request acts on a bounded number of messages below a bound fixed by
//! the first request; the client repeats it until nothing is left, so a
//! queue that is being produced to does not keep a purge going. While the
//! batch is in flight its messages are held back from delivery, as claims
//! are.

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_proto::{ErrorCode, QueueActionRequest};
use lotse_record::Batch;
use lotse_record::dead_letter::Reason;
use lotse_rt::Runtime;

use super::queues::{Action, QueueBatch, answer_action, deliverable};
use super::retire::{Marks, Retiring, ranges};
use super::{Clients, ClusterView, Outbox, hint};
use crate::replica::{Error, Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Claims of a requeue: `(offset, reason, deliveries)` per message.
type Claims = Vec<(u64, Reason, u16)>;

/// Messages one purge settles at most.
pub(crate) const PURGE_MAX: usize = 10_000;

/// Messages one requeue moves at most: each is read from the log.
pub(crate) const REQUEUE_MAX: usize = 1_000;

/// Which action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Purge,
    Requeue,
}

impl Clients {
    /// `PurgeQueue` or `Requeue`.
    pub(super) async fn queue_action<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: QueueActionRequest,
        kind: Kind,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) -> Result<(), Error> {
        let none = Action { count: 0, up_to: req.up_to, more: false };
        let (g, replica) = match Self::queue_leader(view, groups, req.queue_id) {
            Ok(x) => x,
            Err((e, h)) => {
                answer_action(&outbox, id, e, none, h);
                return Ok(());
            }
        };
        // What earlier leaders settled must be applied first, as for
        // deliveries.
        let raft = replica.raft_ref();
        if raft.log().term_at(replica.applied()) != Some(raft.term()) {
            answer_action(&outbox, id, ErrorCode::NOT_LEADER, none, hint(raft));
            return Ok(());
        }
        let term = raft.term();
        let end = deliverable(replica);
        let up_to = if req.up_to == 0 { end } else { req.up_to.min(end) };
        let st = self.retiring.entry(g).or_insert_with(|| Retiring::new(term));
        if st.term() != term {
            *st = Retiring::new(term);
        }
        let dispatcher = self.queues.get(&g).filter(|s| s.term == term).map(|s| &s.dispatcher);
        let q = replica.queue().expect("a queue group");
        // Held by a transaction, a message is not ready: it may be committed.
        let busy =
            |o: u64| st.claiming().contains(&o) || dispatcher.is_some_and(|d| d.is_busy(o)) || q.held_by(o).is_some();
        let max = if kind == Kind::Purge { PURGE_MAX } else { REQUEUE_MAX };

        // The ready messages below the bound, oldest first.
        let mut picked: Vec<u64> = Vec::new();
        let mut o = q.next_unsettled(q.floor());
        while o < up_to && picked.len() < max {
            if !busy(o) {
                picked.push(o);
            }
            o = q.next_unsettled(o + 1);
        }
        let more = o < up_to;
        match kind {
            Kind::Purge => {
                if picked.is_empty() {
                    answer_action(&outbox, id, ErrorCode::NONE, Action { count: 0, up_to, more: false }, None);
                    return Ok(());
                }
                let batch = lotse_record::ack_batch(&ranges(&picked)).expect("ranges fit a batch");
                let action = Action { count: picked.len() as u64, up_to, more };
                st.hold(&picked);
                let what = QueueBatch {
                    acked: picked.len() as u64,
                    marks: Marks { claims: picked, finishes: Vec::new() },
                    action: Some(action),
                    ..QueueBatch::new(batch)
                };
                self.write_queue(Some((outbox, id)), g, replica, what, view.now);
            }
            Kind::Requeue => {
                // Each message goes back where its headers say it came from;
                // one that names no origin, or a queue that is gone, stays.
                let mut by_target: BTreeMap<u64, Claims> = BTreeMap::new();
                let mut cache: Option<(u64, Batch)> = None;
                for &o in &picked {
                    let batch = match &cache {
                        Some((first, b)) if *first <= o && o < b.next_offset() => b.clone(),
                        _ => {
                            let read = replica.log().read(o, 1).await?;
                            let (b, _) = Batch::parse(&read)?;
                            cache = Some((b.base_offset(), b.clone()));
                            b
                        }
                    };
                    let Some(record) = batch.records()?.into_iter().find(|r| r.offset == o) else { continue };
                    let Ok(origin) = lotse_record::dead_letter_origin(&record) else { continue };
                    let source_is_queue = view.queues.iter().any(|x| x.resource_id == origin.queue_id);
                    if origin.queue_id == req.queue_id || view.deleted.contains(&origin.queue_id) || !source_is_queue {
                        continue;
                    }
                    by_target.entry(origin.queue_id).or_default().push((o, Reason::Requeued, 0));
                }
                let count: u64 = by_target.values().map(|v| v.len() as u64).sum();
                if count == 0 {
                    answer_action(&outbox, id, ErrorCode::NONE, Action { count: 0, up_to, more }, None);
                    return Ok(());
                }
                // One claim per source queue; the answer waits for the last,
                // which the log orders after the others.
                let targets: Vec<(u64, Claims)> = by_target.into_iter().collect();
                let last = targets.len() - 1;
                for (i, (target, entries)) in targets.into_iter().enumerate() {
                    let offsets: Vec<u64> = entries.iter().map(|e| e.0).collect();
                    let batch = lotse_record::retire_out_batch(target, &entries).expect("claims fit a batch");
                    if let Some(st) = self.retiring.get_mut(&g) {
                        st.hold(&offsets);
                    }
                    let reply = (i == last).then(|| (outbox.clone(), id));
                    let action = (i == last).then_some(Action { count, up_to, more });
                    let what = QueueBatch {
                        marks: Marks { claims: offsets, finishes: Vec::new() },
                        action,
                        ..QueueBatch::new(batch)
                    };
                    self.write_queue(reply, g, replica, what, view.now);
                }
            }
        }
        Ok(())
    }
}

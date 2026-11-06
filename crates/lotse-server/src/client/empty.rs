//! Conditional deletes (ADR 0063): `DeleteResource` with `if_empty` deletes
//! a stream or queue only if it holds nothing.
//!
//! What a resource holds lives with its partitions' leaders, the delete in
//! the metadata group. The metadata leader therefore asks each partition's
//! leader first (the internal `CheckEmpty`, through the same client code as
//! everyone else) and deletes only once all of them said empty. This is a
//! guard against deleting by mistake, not a lock: a message produced in the
//! round trip between the last answer and the delete goes with the
//! resource. The maintainer chose that over sealing the resource first.
//!
//! Empty means, for a queue, that no offset is unsettled and no message is
//! owed to a dead-letter queue; for a stream, that no partition holds a
//! data record. Both count what is appended, committed or not, so a
//! message in flight makes a resource not empty.

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_proto::{CheckEmptyRequest, DeleteResourceRequest, ErrorCode, OutcomeResponse, Response};
use lotse_raft::Role;
use lotse_rt::Runtime;

use super::admin::admin_response;
use super::offsets::by_time;
use super::{Clients, ClusterView, Outbox, hint};
use crate::replica::{Error, Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Partitions for the node to ask about, for one delete.
pub(crate) struct EmptyJob {
    pub(crate) key: u64,
    pub(crate) resource_id: u64,
    pub(crate) partitions: Vec<u32>,
}

/// What the node found out: every partition empty (`Ok(None)`), the first
/// that is not, or why it could not ask.
pub(crate) type EmptyAnswer = Result<Option<u32>, ErrorCode>;

/// A delete waiting for its answers.
pub(crate) struct WaitingDelete {
    outbox: Arc<Outbox>,
    id: u32,
    resource_id: u64,
}

/// The conditional deletes in flight on this node.
#[derive(Default)]
pub(crate) struct Emptiness {
    next: u64,
    waiting: BTreeMap<u64, WaitingDelete>,
    jobs: Vec<EmptyJob>,
}

impl Clients {
    /// Holds a delete until the partitions of `resource_id` are known to be
    /// empty.
    pub(super) fn delete_if_empty(&mut self, outbox: Arc<Outbox>, id: u32, resource_id: u64, partitions: Vec<u32>) {
        let e = &mut self.emptiness;
        e.next += 1;
        e.waiting.insert(e.next, WaitingDelete { outbox, id, resource_id });
        e.jobs.push(EmptyJob { key: e.next, resource_id, partitions });
    }

    /// Questions for the node to send.
    pub(crate) fn take_empty_jobs(&mut self) -> Vec<EmptyJob> {
        std::mem::take(&mut self.emptiness.jobs)
    }

    /// The answers for a held delete: delete now, through every check of an
    /// unconditional one again (this node may no longer lead the metadata
    /// group), or refuse.
    pub(crate) fn empty_checked<R: Runtime, S: StateMachine>(
        &mut self,
        key: u64,
        answer: EmptyAnswer,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let Some(w) = self.emptiness.waiting.remove(&key) else { return };
        match answer {
            Ok(None) => {
                let req = DeleteResourceRequest { resource_id: w.resource_id, if_empty: false };
                self.delete_resource(w.outbox, w.id, req, view, groups);
            }
            Ok(Some(partition)) => {
                let why = format!("partition {partition} holds messages");
                w.outbox.push(w.id, &admin_response(ErrorCode::RESOURCE_NOT_EMPTY, Some(&why), None));
            }
            Err(code) => w.outbox.push(w.id, &admin_response(code, Some("could not ask every partition"), None)),
        }
    }

    /// `CheckEmpty`, on a partition's leader.
    pub(super) async fn check_empty<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: &Outbox,
        id: u32,
        req: CheckEmptyRequest,
        view: &ClusterView<'_>,
        groups: &Groups<R, S>,
    ) -> Result<(), Error> {
        let group = view.group_of(req.resource_id, req.partition);
        let mut out = OutcomeResponse { error: ErrorCode::NONE, leader_hint: None };
        match group.and_then(|g| groups.get(&g)) {
            None => out.error = view.absent(req.resource_id, group),
            Some(r) if r.raft_ref().role() != Role::Leader => {
                out.error = ErrorCode::NOT_LEADER;
                out.leader_hint = hint(r.raft_ref());
            }
            Some(r) => {
                if !is_empty(r).await? {
                    out.error = ErrorCode::RESOURCE_NOT_EMPTY;
                }
            }
        }
        outbox.push(id, &Response::Outcome(out));
        Ok(())
    }
}

/// Whether a partition's leader holds nothing.
async fn is_empty<R: Runtime, S: StateMachine>(replica: &Replica<R, S>) -> Result<bool, Error> {
    let end = replica.log().next_offset();
    match replica.queue() {
        Some(q) => {
            // Offsets not applied yet may be messages: not empty until known.
            let applied = replica.applied().min(end);
            let unapplied = end > applied;
            Ok(!unapplied && q.unsettled_in(q.floor(), applied) == 0 && q.owed().next().is_none())
        }
        None => Ok(by_time(replica.log(), i64::MIN, end).await?.is_none()),
    }
}

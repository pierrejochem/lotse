//! The metadata group's side of the protocol: `CreateStream`,
//! `CreateQueue`, `DeclareExchange`, `Bind` and `Unbind` (state in
//! [`crate::metadata`]).
//!
//! Only the metadata group's leader takes admin requests, and only once it
//! has applied an entry of its own term, so it checks names against
//! everything committed before. It places the new groups, proposes one
//! record and answers once that record is committed and applied here, under
//! the commit rule of the other writes: then it looks at the applied state
//! to tell the client whether this record created the resource or a
//! concurrent one did (`RESOURCE_EXISTS`, with that resource's id).
//!
//! The answer rests on the applied state, not on the record's term: a
//! create is acknowledged only if the resource with that name has exactly
//! the id this record's index gives. Even if this record was replaced, a
//! resource with that name and id exists, so the answer is true. The commit
//! rule still decides when to give up (`NOT_LEADER`, `REQUEST_TIMED_OUT`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use lotse_proto::{
    BindRequest, CreateQueueRequest, CreateResponse, CreateStreamRequest, DeclareExchangeRequest, ErrorCode,
    LeaderHint, OutcomeResponse, Response,
};
use lotse_raft::{NodeId, Role};
use lotse_route::{Binding, ExchangeKind};
use lotse_rt::{Instant, Runtime};

use super::{Clients, ClusterView, Outbox, hint, outcome};
use crate::metadata::{self, Change, MAX_PARTITIONS};
use crate::replica::{Proposed, Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// How long an admin record may take to commit.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// What the cluster looks like to the admin handlers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct AdminView<'a> {
    /// The metadata group, if there is one.
    pub(crate) metadata: Option<GroupId>,
    /// Every node.
    pub(crate) nodes: &'a [NodeId],
    /// Groups every node hosts from the configuration.
    pub(crate) configured_groups: usize,
    /// Voters of the groups created at runtime.
    pub(crate) voters: &'a BTreeMap<GroupId, Vec<NodeId>>,
}

static NO_VOTERS: BTreeMap<GroupId, Vec<NodeId>> = BTreeMap::new();

impl Default for AdminView<'_> {
    fn default() -> Self {
        AdminView { metadata: None, nodes: &[], configured_groups: 0, voters: &NO_VOTERS }
    }
}

/// What to answer once an admin record is applied.
pub(super) enum AdminThen {
    /// A create: the name and the id this record gives it.
    Created { name: String, id: u64 },
    /// A bind or unbind.
    Bound,
}

/// An admin record in flight.
pub(super) struct AdminWrite {
    reply: (Arc<Outbox>, u32),
    deadline: Instant,
    last: u64,
    term: u64,
    then: AdminThen,
}

fn created(error: ErrorCode, resource_id: u64, why: Option<&str>, leader_hint: Option<LeaderHint>) -> Response {
    Response::Create(CreateResponse { error, resource_id, error_msg: why.map(str::to_string), leader_hint })
}

fn outcome_response(error: ErrorCode, leader_hint: Option<LeaderHint>) -> Response {
    Response::Outcome(OutcomeResponse { error, leader_hint })
}

/// The id of the resource called `name`, configured or created.
fn existing(view: &ClusterView<'_>, name: &str) -> Option<u64> {
    let streams = view.streams.iter().map(|s| (&s.name, s.resource_id));
    let queues = view.queues.iter().map(|q| (&q.name, q.resource_id));
    let exchanges = view.exchanges.iter().map(|x| (&x.name, x.resource_id));
    streams.chain(queues).chain(exchanges).find(|(n, _)| n.as_str() == name).map(|(_, id)| id)
}

impl Clients {
    /// The metadata group's replica, if this node leads it and has applied
    /// an entry of its own term; otherwise the error and leader hint.
    fn metadata_leader<'a, R: Runtime, S: StateMachine>(
        view: &ClusterView<'_>,
        groups: &'a mut Groups<R, S>,
    ) -> Result<&'a mut Replica<R, S>, (ErrorCode, Option<LeaderHint>)> {
        let g = view.admin.metadata.ok_or((ErrorCode::UNSUPPORTED_VERSION, None))?;
        let replica = groups.get_mut(&g).ok_or((ErrorCode::NOT_LEADER, None))?;
        let raft = replica.raft_ref();
        if raft.role() != Role::Leader {
            return Err((ErrorCode::NOT_LEADER, hint(raft)));
        }
        if raft.log().term_at(replica.applied()) != Some(raft.term()) {
            return Err((ErrorCode::COORDINATOR_NOT_AVAILABLE, None));
        }
        Ok(replica)
    }

    /// Proposes `change`; answered by `then` once applied.
    fn propose_admin<R: Runtime, S: StateMachine>(
        &mut self,
        replica: &mut Replica<R, S>,
        now: Instant,
        change: &Change,
        reply: (Arc<Outbox>, u32),
        then: AdminThen,
    ) {
        match replica.propose_internal(metadata::record(change)) {
            Ok(Proposed { last, term: Some(term), .. }) => {
                let then = match then {
                    // The id is the record's index: known now.
                    AdminThen::Created { name, .. } => AdminThen::Created { name, id: metadata::resource_id(last) },
                    t => t,
                };
                self.admin_writes.push(AdminWrite { reply, deadline: now + WRITE_TIMEOUT, last, term, then });
            }
            Ok(_) => unreachable!("an admin record is never a duplicate"),
            Err(_) => {
                let (outbox, id) = reply;
                let h = hint(replica.raft_ref());
                let resp = match then {
                    AdminThen::Created { .. } => created(ErrorCode::NOT_LEADER, 0, None, h),
                    AdminThen::Bound => outcome_response(ErrorCode::NOT_LEADER, h),
                };
                outbox.push(id, &resp);
            }
        }
    }

    /// Checks what every create needs; on refusal answers and returns `None`.
    fn admit<'a, R: Runtime, S: StateMachine>(
        outbox: &Outbox,
        id: u32,
        view: &ClusterView<'_>,
        groups: &'a mut Groups<R, S>,
        name: &str,
        replication: Option<u8>,
    ) -> Option<&'a mut Replica<R, S>> {
        if name.is_empty() {
            outbox.push(id, &created(ErrorCode::INVALID_REQUEST, 0, Some("a name is needed"), None));
            return None;
        }
        if let Some(r) = replication
            && (r == 0 || usize::from(r) > view.admin.nodes.len())
        {
            let why = format!("replication must be between 1 and the {} nodes", view.admin.nodes.len());
            outbox.push(id, &created(ErrorCode::INVALID_REQUEST, 0, Some(&why), None));
            return None;
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => {
                outbox.push(id, &created(code, 0, None, h));
                return None;
            }
        };
        if let Some(existing) = existing(view, name) {
            outbox.push(id, &created(ErrorCode::RESOURCE_EXISTS, existing, None, None));
            return None;
        }
        Some(replica)
    }

    /// Voters for `count` new groups of `replication` replicas each.
    fn place<R: Runtime, S: StateMachine>(
        view: &ClusterView<'_>,
        replica: &Replica<R, S>,
        replication: u8,
        count: usize,
    ) -> Vec<Vec<NodeId>> {
        let mut load = metadata::load(view.admin.nodes, view.admin.configured_groups, replica.metadata());
        metadata::place(view.admin.nodes, &mut load, usize::from(replication), count)
    }

    pub(super) fn create_stream<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: CreateStreamRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        if req.partitions == 0 || req.partitions > MAX_PARTITIONS || !req.configs.is_empty() {
            let why = format!("1 to {MAX_PARTITIONS} partitions, and no settings are supported yet");
            return outbox.push(id, &created(ErrorCode::INVALID_REQUEST, 0, Some(&why), None));
        }
        let Some(replica) = Self::admit(&outbox, id, view, groups, &req.name, Some(req.replication)) else { return };
        let partitions = Self::place(view, replica, req.replication, req.partitions as usize);
        let change = Change::CreateStream { name: req.name.clone(), partitions };
        let then = AdminThen::Created { name: req.name, id: 0 };
        self.propose_admin(replica, view.now, &change, (outbox, id), then);
    }

    pub(super) fn create_queue<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: CreateQueueRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let mut ack_timeout_ms = super::QueueConfig::DEFAULT_ACK_TIMEOUT.as_millis() as u64;
        let mut refused = None;
        if req.priority_levels != 1 || req.single_active || req.dlq.is_some() {
            refused = Some("priorities, single active consumer and dead-letter queues are not supported yet".into());
        }
        for (k, v) in &req.configs {
            match (k.as_str(), v.parse::<u64>()) {
                ("ack.timeout.ms", Ok(ms)) if ms > 0 => ack_timeout_ms = ms,
                _ => refused = Some(format!("unsupported setting {k} = {v}")),
            }
        }
        if let Some(why) = refused {
            return outbox.push(id, &created(ErrorCode::INVALID_REQUEST, 0, Some(&why), None));
        }
        let Some(replica) = Self::admit(&outbox, id, view, groups, &req.name, Some(req.replication)) else { return };
        let voters = Self::place(view, replica, req.replication, 1).remove(0);
        let change = Change::CreateQueue { name: req.name.clone(), voters, ack_timeout_ms };
        let then = AdminThen::Created { name: req.name, id: 0 };
        self.propose_admin(replica, view.now, &change, (outbox, id), then);
    }

    pub(super) fn declare_exchange<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: DeclareExchangeRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let Some(kind) = ExchangeKind::from_code(req.exchange_type).filter(|_| req.configs.is_empty()) else {
            let why = "unknown exchange type, or settings (none are supported yet)";
            return outbox.push(id, &created(ErrorCode::INVALID_REQUEST, 0, Some(why), None));
        };
        let Some(replica) = Self::admit(&outbox, id, view, groups, &req.name, None) else { return };
        let change = Change::DeclareExchange { name: req.name.clone(), kind };
        let then = AdminThen::Created { name: req.name, id: 0 };
        self.propose_admin(replica, view.now, &change, (outbox, id), then);
    }

    pub(super) fn bind<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: BindRequest,
        unbind: bool,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &outcome_response(code, h)),
        };
        let known = view.exchanges.iter().any(|x| x.resource_id == req.exchange_id)
            && view.queues.iter().any(|q| q.resource_id == req.queue_id);
        if !known {
            return outbox.push(id, &outcome_response(ErrorCode::UNKNOWN_RESOURCE, None));
        }
        let binding = Binding {
            queue_id: req.queue_id,
            key: req.binding_key,
            headers: req.header_match,
            match_all: req.match_all,
        };
        let change = if unbind {
            Change::Unbind { exchange_id: req.exchange_id, binding }
        } else {
            Change::Bind { exchange_id: req.exchange_id, binding }
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Bound);
    }

    /// Answers admin records that are decided and applied.
    pub(super) fn settle_admin<R: Runtime, S: StateMachine>(&mut self, view: &ClusterView<'_>, groups: &Groups<R, S>) {
        let Some(replica) = view.admin.metadata.and_then(|g| groups.get(&g)) else { return };
        let writes = std::mem::take(&mut self.admin_writes);
        for w in writes {
            let raft = replica.raft_ref();
            let decided = outcome(raft, w.last, w.term);
            let (outbox, id) = &w.reply;
            match decided {
                // Written; the answer needs the applied state.
                Some(ErrorCode::NONE) if replica.applied() < w.last => self.admin_writes.push(w),
                Some(ErrorCode::NONE) => match &w.then {
                    AdminThen::Created { name, id: mine } => {
                        let resp = match replica.metadata().id_of(name) {
                            Some(found) if found == *mine => created(ErrorCode::NONE, found, None, None),
                            Some(found) => created(ErrorCode::RESOURCE_EXISTS, found, None, None),
                            None => created(ErrorCode::RESOURCE_EXISTS, existing(view, name).unwrap_or(0), None, None),
                        };
                        outbox.push(*id, &resp);
                    }
                    AdminThen::Bound => outbox.push(*id, &outcome_response(ErrorCode::NONE, None)),
                },
                Some(code) => {
                    let h = if code == ErrorCode::NOT_LEADER { hint(raft) } else { None };
                    let resp = match w.then {
                        AdminThen::Created { .. } => created(code, 0, None, h),
                        AdminThen::Bound => outcome_response(code, h),
                    };
                    outbox.push(*id, &resp);
                }
                None if view.now >= w.deadline => {
                    let resp = match w.then {
                        AdminThen::Created { .. } => created(ErrorCode::REQUEST_TIMED_OUT, 0, None, None),
                        AdminThen::Bound => outcome_response(ErrorCode::REQUEST_TIMED_OUT, None),
                    };
                    outbox.push(*id, &resp);
                }
                None => self.admin_writes.push(w),
            }
        }
    }
}

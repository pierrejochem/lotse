//! The consumer group coordinator's side of the protocol: `GroupHeartbeat`,
//! `CommitOffsets`, `OffsetFetch` and `AddOffsetsToTxn` (state in
//! [`crate::groups`]).
//!
//! Like the transaction coordinator it acts only once its leader has applied
//! an entry of its own term, and changes one group at a time: a heartbeat
//! that needs a change while another change to the group is in flight gets
//! `CONCURRENT_GROUP_CHANGE` and retries. A heartbeat that changes nothing
//! is answered at once from the applied state.
//!
//! Liveness is the leader's: a member is removed when it has not sent a
//! heartbeat for the session timeout, or has not released a revoked
//! partition within its rebalance timeout (and never sooner than the
//! session timeout after the revocation). Both are measured on this
//! leader's clock from when it first saw the member (or the revocation), so
//! a new leader never removes a member sooner than the old one would have.
//! Only then do the member's partitions go to others.
//!
//! Why that is safe: a member stops reading a revoked partition as soon as
//! a heartbeat answer tells it, and drops everything once no heartbeat has
//! succeeded for the session timeout, counted from when it sent the last
//! one that did. A heartbeat that succeeds after a revocation carries it.
//! So by the session timeout after a revocation, the member has either
//! learnt of it or dropped everything, and by the session timeout after its
//! last heartbeat arrived here it has dropped everything.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use lotse_proto::{
    AddOffsetsToTxnRequest, CommitOffsetsRequest, CommitOffsetsResponse, ErrorCode, GroupHeartbeatRequest,
    GroupHeartbeatResponse, NO_OFFSET, OffsetCommitResource, OffsetFetchPartition, OffsetFetchRequest,
    OffsetFetchResource, OffsetFetchResponse, Response, TxnPartitionResult, TxnResource, TxnResourceResult,
};
use lotse_rt::{Instant, Runtime};

use super::coord::{Busy, Then};
use super::{Clients, ClusterView, Outbox};
use crate::coordinator::{Phase, TxnMeta};
use crate::groups::{Committed, GroupMeta, MemberMeta, Tp, group_record, offsets_record};
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// The leader's clocks for group members.
#[derive(Default)]
pub(super) struct GroupTimers {
    /// When each member was last heard of: `(group, member)`.
    seen: BTreeMap<(String, String), Instant>,
    /// Since when each member has had partitions to release.
    revoking: BTreeMap<(String, String), Instant>,
    /// Member ids are `<term>-<n>`: unique across leaders.
    term: u64,
    next: u64,
}

fn as_resources(tps: &BTreeSet<Tp>) -> Vec<TxnResource> {
    let mut by: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
    for (r, p) in tps {
        by.entry(*r).or_default().push(*p);
    }
    by.into_iter().map(|(resource_id, partitions)| TxnResource { resource_id, partitions }).collect()
}

fn as_tps(resources: &[TxnResource]) -> BTreeSet<Tp> {
    resources.iter().flat_map(|r| r.partitions.iter().map(|p| (r.resource_id, *p))).collect()
}

fn partitions_of(view: &ClusterView<'_>) -> impl Fn(u64) -> u32 {
    let counts: BTreeMap<u64, u32> = view.streams.iter().map(|s| (s.resource_id, s.partitions.len() as u32)).collect();
    move |r| counts.get(&r).copied().unwrap_or(0)
}

/// The per-partition answer to an offset commit, all `NONE` to start with.
fn commit_answer(resources: &[OffsetCommitResource]) -> CommitOffsetsResponse {
    CommitOffsetsResponse {
        resources: resources
            .iter()
            .map(|r| TxnResourceResult {
                resource_id: r.resource_id,
                partitions: r
                    .partitions
                    .iter()
                    .map(|p| TxnPartitionResult { partition: p.partition, error: ErrorCode::NONE })
                    .collect(),
            })
            .collect(),
    }
}

fn set_all(resp: &mut CommitOffsetsResponse, error: ErrorCode) {
    resp.resources.iter_mut().flat_map(|r| r.partitions.iter_mut()).for_each(|p| p.error = error);
}

/// Checks that `member` of `group` may commit offsets for these partitions:
/// it exists and still holds each one. Marks refused partitions in `resp`
/// and returns whether any was refused.
fn check_owner(
    group: Option<&GroupMeta>,
    member: &str,
    resources: &[OffsetCommitResource],
    resp: &mut CommitOffsetsResponse,
) -> bool {
    let Some(m) = group.and_then(|g| g.members.get(member)) else {
        set_all(resp, ErrorCode::UNKNOWN_MEMBER_ID);
        return true;
    };
    let mut refused = false;
    for (r, out) in resources.iter().zip(resp.resources.iter_mut()) {
        for (p, o) in r.partitions.iter().zip(out.partitions.iter_mut()) {
            if !m.holds(&(r.resource_id, p.partition)) {
                o.error = ErrorCode::FENCED_MEMBER_EPOCH;
                refused = true;
            }
        }
    }
    refused
}

fn committed(resources: &[OffsetCommitResource]) -> Vec<(Tp, Committed)> {
    resources
        .iter()
        .flat_map(|r| {
            r.partitions.iter().map(move |p| {
                let c = Committed { offset: p.offset, leader_term: p.leader_term, metadata: p.metadata.clone() };
                ((r.resource_id, p.partition), c)
            })
        })
        .collect()
}

impl Clients {
    pub(super) fn group_heartbeat<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: GroupHeartbeatRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let interval = view.group_heartbeat.as_millis() as u32;
        let refuse = |error| {
            Response::GroupHeartbeat(GroupHeartbeatResponse {
                throttle_ms: 0,
                error,
                member_id: req.member_id.clone(),
                member_epoch: req.member_epoch,
                heartbeat_interval_ms: interval,
                assignment: None,
            })
        };
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, _)) => return outbox.push(id, &refuse(code)),
        };
        let key = Busy::Group(req.group_id.clone());
        let applied = replica.coordinator().groups().groups.get(&req.group_id);
        if req.member_epoch > 0 && applied.is_some_and(|g| g.members.contains_key(&req.member_id)) {
            // Heard of, even if nothing can change right now.
            self.group_timers.seen.insert((req.group_id.clone(), req.member_id.clone()), view.now);
        }
        if self.busy.contains(&key) {
            return outbox.push(id, &refuse(ErrorCode::CONCURRENT_GROUP_CHANGE));
        }
        let term = replica.raft_ref().term();
        let mut g = replica.coordinator().groups().groups.get(&req.group_id).cloned().unwrap_or_default();
        let known: BTreeSet<u64> = view.streams.iter().map(|s| s.resource_id).collect();
        let subscribed = req.subscribed.as_ref().map(|s| s.iter().copied().filter(|r| known.contains(r)).collect());
        let mut changed = false;
        let member_id = match req.member_epoch {
            0 => {
                if self.group_timers.term != term {
                    self.group_timers.term = term;
                    self.group_timers.next = 0;
                }
                self.group_timers.next += 1;
                let mid = format!("{term}-{}", self.group_timers.next);
                let m = MemberMeta {
                    subscribed: subscribed.clone().unwrap_or_default(),
                    rebalance_timeout_ms: req.rebalance_timeout_ms,
                    ..MemberMeta::default()
                };
                g.members.insert(mid.clone(), m);
                g.epoch += 1;
                g.retarget(partitions_of(view));
                changed = true;
                mid
            }
            -1 => {
                if g.members.remove(&req.member_id).is_none() {
                    return outbox.push(id, &refuse(ErrorCode::NONE));
                }
                g.epoch += 1;
                g.retarget(partitions_of(view));
                self.group_timers.forget(&req.group_id, &req.member_id);
                let resp = GroupHeartbeatResponse { member_epoch: -1, ..refuse_resp(&req, interval) };
                let record = group_record(&req.group_id, &g);
                return self.propose_record(replica, view.now, record, key, Some((outbox, id)), Then::Heartbeat(resp));
            }
            epoch => {
                let Some(m) = g.members.get_mut(&req.member_id) else {
                    return outbox.push(id, &refuse(ErrorCode::UNKNOWN_MEMBER_ID));
                };
                if epoch > m.epoch {
                    return outbox.push(id, &refuse(ErrorCode::FENCED_MEMBER_EPOCH));
                }
                if let Some(s) = subscribed.filter(|s| *s != m.subscribed) {
                    m.subscribed = s;
                    g.epoch += 1;
                    g.retarget(partitions_of(view));
                    changed = true;
                }
                req.member_id.clone()
            }
        };
        self.group_timers.seen.insert((req.group_id.clone(), member_id.clone()), view.now);
        let owned = req.owned.as_deref().map(as_tps);
        changed |= g.reconcile(&member_id, owned.as_ref());
        let m = &g.members[&member_id];
        let resp = GroupHeartbeatResponse {
            throttle_ms: 0,
            error: ErrorCode::NONE,
            member_id: member_id.clone(),
            member_epoch: m.epoch,
            heartbeat_interval_ms: interval,
            assignment: (req.member_epoch != m.epoch || req.member_epoch == 0).then(|| as_resources(&m.assigned)),
        };
        if !changed {
            return outbox.push(id, &Response::GroupHeartbeat(resp));
        }
        let record = group_record(&req.group_id, &g);
        self.propose_record(replica, view.now, record, key, Some((outbox, id)), Then::Heartbeat(resp));
    }

    pub(super) fn commit_offsets<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: CommitOffsetsRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let mut resp = commit_answer(&req.resources);
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, _)) => {
                set_all(&mut resp, code);
                return outbox.push(id, &Response::CommitOffsets(resp));
            }
        };
        let group = replica.coordinator().groups().groups.get(&req.group_id);
        if check_owner(group, &req.member_id, &req.resources, &mut resp) {
            // All or nothing: a commit that is refused in part is not written.
            return outbox.push(id, &Response::CommitOffsets(resp));
        }
        let record = offsets_record(&req.group_id, &committed(&req.resources));
        self.propose_record(replica, view.now, record, Busy::Free, Some((outbox, id)), Then::Offsets(resp));
    }

    pub(super) fn offset_fetch<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: OffsetFetchRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let coordinator = Self::coordinator(view, groups);
        let resources = req
            .resources
            .iter()
            .map(|r| OffsetFetchResource {
                resource_id: r.resource_id,
                partitions: r
                    .partitions
                    .iter()
                    .map(|&partition| {
                        let mut out = OffsetFetchPartition {
                            partition,
                            error: ErrorCode::NONE,
                            offset: NO_OFFSET,
                            leader_term: 0,
                            metadata: None,
                        };
                        let state = match &coordinator {
                            Ok((_, replica)) => replica.coordinator(),
                            Err((code, _)) => {
                                out.error = *code;
                                return out;
                            }
                        };
                        let key = (req.group_id.clone(), r.resource_id, partition);
                        if req.require_stable && state.offset_pending(&key) {
                            out.error = ErrorCode::UNSTABLE_OFFSET_COMMIT;
                        } else if let Some(c) = state.groups().offsets.get(&key) {
                            (out.offset, out.leader_term, out.metadata) = (c.offset, c.leader_term, c.metadata.clone());
                        }
                        out
                    })
                    .collect(),
            })
            .collect();
        outbox.push(id, &Response::OffsetFetch(OffsetFetchResponse { resources }));
    }

    pub(super) fn add_offsets_to_txn<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: AddOffsetsToTxnRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let mut resp = commit_answer(&req.resources);
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, _)) => {
                set_all(&mut resp, code);
                return outbox.push(id, &Response::CommitOffsets(resp));
            }
        };
        let current = replica.coordinator().get(&req.txn_id).cloned();
        let error = match &current {
            _ if self.busy.contains(&Busy::Txn(req.txn_id.clone())) => Some(ErrorCode::CONCURRENT_TRANSACTIONS),
            None => Some(ErrorCode::INVALID_TXN_STATE),
            Some(m) if m.producer_id != req.producer_id || m.epoch != req.producer_epoch => {
                Some(ErrorCode::PRODUCER_FENCED)
            }
            Some(m) if m.phase.preparing().is_some() => Some(ErrorCode::CONCURRENT_TRANSACTIONS),
            Some(_) => None,
        };
        if let Some(e) = error {
            set_all(&mut resp, e);
            return outbox.push(id, &Response::CommitOffsets(resp));
        }
        // A member that no longer holds a partition is fenced: its
        // transaction must abort, so a zombie cannot move the group's offsets.
        let group = replica.coordinator().groups().groups.get(&req.group_id);
        if check_owner(group, &req.member_id, &req.resources, &mut resp) {
            return outbox.push(id, &Response::CommitOffsets(resp));
        }
        let m = current.expect("checked");
        let ongoing = m.phase == Phase::Ongoing;
        let mut offsets = if ongoing { m.offsets.clone() } else { BTreeMap::new() };
        for ((r, p), c) in committed(&req.resources) {
            offsets.insert((req.group_id.clone(), r, p), c);
        }
        let partitions = if ongoing { m.partitions.clone() } else { BTreeSet::new() };
        let meta = TxnMeta { phase: Phase::Ongoing, partitions, offsets, ..m };
        let record = crate::coordinator::record(&req.txn_id, &meta);
        let key = Busy::Txn(req.txn_id.clone());
        self.propose_record(replica, view.now, record, key, Some((outbox, id)), Then::Offsets(resp));
    }

    /// Removes members past their session or rebalance timeout. Runs every
    /// round while this node coordinates.
    pub(super) fn expire_members<R: Runtime, S: StateMachine>(
        &mut self,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let Ok((_, replica)) = Self::coordinator(view, groups) else {
            self.group_timers.seen.clear();
            self.group_timers.revoking.clear();
            return;
        };
        let state: Vec<(String, GroupMeta)> =
            replica.coordinator().groups().groups.iter().map(|(id, g)| (id.clone(), g.clone())).collect();
        let t = &mut self.group_timers;
        let live: BTreeSet<(String, String)> =
            state.iter().flat_map(|(gid, g)| g.members.keys().map(move |m| (gid.clone(), m.clone()))).collect();
        t.seen.retain(|k, _| live.contains(k));
        t.revoking.retain(|k, _| live.contains(k));
        let mut expired: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (gid, g) in &state {
            for (mid, m) in &g.members {
                let k = (gid.clone(), mid.clone());
                let seen = *t.seen.entry(k.clone()).or_insert(view.now);
                let mut gone = view.now - seen >= view.group_session;
                if m.revoking.is_empty() {
                    t.revoking.remove(&k);
                } else {
                    let since = *t.revoking.entry(k).or_insert(view.now);
                    let limit = Duration::from_millis(u64::from(m.rebalance_timeout_ms)).max(view.group_session);
                    gone |= view.now - since >= limit;
                }
                if gone {
                    expired.entry(gid.clone()).or_default().push(mid.clone());
                }
            }
        }
        for (gid, members) in expired {
            let key = Busy::Group(gid.clone());
            if self.busy.contains(&key) {
                continue;
            }
            let mut g = state.iter().find(|(id, _)| *id == gid).expect("listed").1.clone();
            for mid in &members {
                g.members.remove(mid);
                self.group_timers.forget(&gid, mid);
            }
            g.epoch += 1;
            g.retarget(partitions_of(view));
            let record = group_record(&gid, &g);
            self.propose_record(replica, view.now, record, key, None, Then::Nothing);
        }
    }
}

impl GroupTimers {
    fn forget(&mut self, group: &str, member: &str) {
        let k = (group.to_string(), member.to_string());
        self.seen.remove(&k);
        self.revoking.remove(&k);
    }
}

fn refuse_resp(req: &GroupHeartbeatRequest, interval: u32) -> GroupHeartbeatResponse {
    GroupHeartbeatResponse {
        throttle_ms: 0,
        error: ErrorCode::NONE,
        member_id: req.member_id.clone(),
        member_epoch: req.member_epoch,
        heartbeat_interval_ms: interval,
        assignment: None,
    }
}

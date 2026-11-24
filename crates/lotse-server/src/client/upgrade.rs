//! Rolling upgrades (ADR 0127): the cluster version, the nodes' versions
//! and the restart turn, all in the metadata group.
//!
//! A node about to restart for an upgrade takes the cluster's single turn
//! first: `lotse turn take` on its machine sends `Turn` with
//! [`turn_action::TAKE`] to the node itself, which checks that no group it
//! votes in would lose its quorum without it and then claims the turn from
//! the metadata leader. The restarted process gives the turn back once all
//! its groups are ready. The safety of a rolling restart rests on the
//! metadata state taking one claim at a time, which it does when the
//! claims apply, not here: two claims may both pass these checks before
//! either applied.

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_proto::{
    AlterUpgradeRequest, DescribeUpgradeResponse, ErrorCode, ReportVersionRequest, Response, TurnRequest, UpgradeNode,
    UpgradeTurn, turn_action,
};
use lotse_raft::NodeId;
use lotse_rt::Runtime;

use super::admin::{AdminThen, admin_response};
use super::{Clients, ClusterView, Outbox};
use crate::auth::Principal;
use crate::metadata::{Change, MetadataState};
use crate::replica::{Replica, StateMachine};
use crate::version;
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// A `Turn` with [`turn_action::TAKE`] for the node to judge and claim
/// through the protocol, as its own client.
pub(crate) struct TurnJob {
    outbox: Arc<Outbox>,
    id: u32,
}

impl TurnJob {
    /// Answers `lotse turn take`.
    pub(crate) fn answer(self, code: ErrorCode, why: Option<&str>) {
        self.outbox.push(self.id, &admin_response(code, why, None));
    }
}

/// Why `node` cannot have the turn now, as the applied `state` stands; `None`
/// when it can, or holds it.
pub(crate) fn turn_refused(state: &MetadataState, node: NodeId) -> Option<String> {
    match state.turn {
        Some(t) if t.node == node => None,
        Some(t) => Some(format!("node {} holds the restart turn", t.node)),
        None if state.turns_held => Some("restart turns are paused; a cluster admin resumes them".into()),
        None => None,
    }
}

/// The cluster version for a cluster that has none yet, once every node
/// of `members` has reported: the lowest of their newest versions. A
/// report not yet applied counts as `pending`.
fn cluster_default(
    state: &MetadataState,
    members: impl Iterator<Item = NodeId>,
    pending: (NodeId, u32),
) -> Option<u32> {
    if state.cluster_version.is_some() {
        return None;
    }
    let mut lowest = u32::MAX;
    for n in members {
        let newest = if n == pending.0 { pending.1 } else { state.versions.get(&n)?.newest };
        lowest = lowest.min(newest);
    }
    (lowest != u32::MAX).then_some(lowest)
}

impl Clients {
    /// `Turn`: taken on the node itself, claimed from and given back to the
    /// metadata leader.
    pub(super) fn turn<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: TurnRequest,
        principal: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let node = NodeId::from(req.node_id);
        if let Principal::Node(n) = principal
            && node != *n
        {
            let why = format!("node {n} takes and gives back only its own turn, not node {node}'s");
            return outbox.push(id, &admin_response(ErrorCode::UNAUTHORIZED, Some(&why), None));
        }
        match req.action {
            turn_action::TAKE if node != view.node.id => {
                let why = format!("node {node} judges whether it may stop; ask node {node}, not node {}", view.node.id);
                outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
            }
            turn_action::TAKE => self.turn_jobs.push(TurnJob { outbox, id }),
            turn_action::CLAIM => {
                let replica = match Self::metadata_leader(view, groups) {
                    Ok(r) => r,
                    Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
                };
                if !view.admin.members.contains_key(&node) {
                    let why = format!("node {node} is not in the cluster");
                    return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
                }
                let state = replica.metadata();
                if let Some(why) = turn_refused(state, node) {
                    return outbox.push(id, &admin_response(ErrorCode::TURN_UNAVAILABLE, Some(&why), None));
                }
                if state.turn.is_some() {
                    // It holds the turn already: the instance that took it
                    // stays (ADR 0127).
                    return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
                }
                let change = Change::ClaimTurn { node, instance: req.instance, at_ms: view.unix_ms };
                self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Turn { node });
            }
            turn_action::RELEASE => self.release_turn(outbox, id, node, view, groups),
            other => {
                let why = format!("unknown turn action {other}");
                outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
            }
        }
    }

    /// Gives back the turn `node` holds; nothing to do if it holds none.
    fn release_turn<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        node: NodeId,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        if !replica.metadata().turn.is_some_and(|t| t.node == node) {
            return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
        }
        self.propose_admin(replica, view.now, &Change::ReleaseTurn { node }, (outbox, id), AdminThen::Changed);
    }

    /// `ReportVersion`: a node's release and the cluster versions it runs
    /// with; the first time every node has reported, also the cluster
    /// version for a cluster that has none.
    pub(super) fn report_version<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: ReportVersionRequest,
        principal: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let node = NodeId::from(req.node_id);
        if let Principal::Node(n) = principal
            && node != *n
        {
            let why = format!("node {n} reports only for itself, not for node {node}");
            return outbox.push(id, &admin_response(ErrorCode::UNAUTHORIZED, Some(&why), None));
        }
        if req.oldest > req.newest || req.release.is_empty() {
            let why = format!("node {node} reports no cluster versions it runs with");
            return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        let reported = state.versions.get(&node);
        let same =
            reported.is_some_and(|v| v.release == req.release && v.oldest == req.oldest && v.newest == req.newest);
        let default = cluster_default(state, view.admin.members.keys().copied(), (node, req.newest));
        if same && default.is_none() || state.gone.contains(&node) {
            return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
        }
        let change = Change::ReportVersion {
            node,
            release: req.release,
            oldest: req.oldest,
            newest: req.newest,
            cluster_default: default,
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `AlterUpgrade`: pause or resume turns, give back a node's turn, or
    /// raise the cluster version. One change per request; the first field
    /// set decides, in that order.
    pub(super) fn alter_upgrade<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: AlterUpgradeRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let set = [req.hold.is_some(), req.release_turn.is_some(), req.cluster_version.is_some()];
        if set.iter().filter(|s| **s).count() != 1 {
            let why = "an upgrade change sets one of hold, release_turn and cluster_version";
            return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(why), None));
        }
        if let Some(node) = req.release_turn {
            return self.release_turn(outbox, id, node, view, groups);
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        let change = if let Some(held) = req.hold {
            if state.turns_held == held {
                return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
            }
            Change::HoldTurns { held }
        } else {
            let target = req.cluster_version.unwrap_or_default();
            if let Err(why) = may_raise(state, view.admin.members.keys().copied(), target) {
                return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
            }
            if state.cluster_version == Some(target) {
                return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
            }
            Change::SetClusterVersion { version: target }
        };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `DescribeUpgrade`: where the upgrade stands, from the metadata leader,
    /// or for a request with `read_at_least` from a follower that may serve
    /// it ([`Clients::read_replica`]).
    pub(super) fn describe_upgrade<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: &Outbox,
        id: u32,
        at_least: Option<u64>,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_read(view, groups, at_least) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeUpgradeResponse {
                    error,
                    leader_hint,
                    cluster_version: None,
                    held: false,
                    turn: None,
                    nodes: Vec::new(),
                };
                return outbox.push(id, &Response::DescribeUpgrade(resp));
            }
        };
        let state = replica.metadata();
        let nodes = view
            .admin
            .members
            .keys()
            .map(|n| match state.versions.get(n) {
                Some(v) => UpgradeNode { node_id: *n, release: v.release.clone(), oldest: v.oldest, newest: v.newest },
                None => UpgradeNode { node_id: *n, release: String::new(), oldest: 0, newest: 0 },
            })
            .collect();
        let resp = DescribeUpgradeResponse {
            error: ErrorCode::NONE,
            leader_hint: None,
            cluster_version: state.cluster_version,
            held: state.turns_held,
            turn: state.turn.map(|t| UpgradeTurn { node_id: t.node, since_ms: t.since_ms }),
            nodes,
        };
        outbox.push(id, &Response::DescribeUpgrade(resp));
    }

    /// The `Turn` requests with [`turn_action::TAKE`] since the last call,
    /// for the node to judge and claim.
    pub(crate) fn take_turn_jobs(&mut self) -> Vec<TurnJob> {
        std::mem::take(&mut self.turn_jobs)
    }
}

/// Whether the cluster version may go to `target`: not down, and only to a
/// version every node of `members` runs with (ADR 0127).
pub(crate) fn may_raise(
    state: &MetadataState,
    members: impl Iterator<Item = NodeId>,
    target: u32,
) -> Result<(), String> {
    if let Some(current) = state.cluster_version
        && target < current
    {
        return Err(format!(
            "the cluster runs at cluster version {}; it never goes down, to {}",
            version::display(current),
            version::display(target)
        ));
    }
    let mut behind = Vec::new();
    for n in members {
        match state.versions.get(&n) {
            None => behind.push(format!("node {n} has not reported its version")),
            Some(v) if v.newest < target => behind.push(format!(
                "node {n} runs release {}, with cluster versions up to {}",
                v.release,
                version::display(v.newest)
            )),
            Some(v) if v.oldest > target => behind.push(format!(
                "node {n} runs release {}, with cluster versions from {}",
                v.release,
                version::display(v.oldest)
            )),
            Some(_) => {}
        }
    }
    if behind.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "cluster version {} needs every node to run with it: {}",
            version::display(target),
            behind.join("; ")
        ))
    }
}

#[cfg(test)]
mod tests;

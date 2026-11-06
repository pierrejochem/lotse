//! Adding and removing nodes (ADR 0048): `AddNode`, `RemoveNode` and
//! `DescribeNodes` for admins, and `SettleVoters`, by which a group's
//! leader tells the metadata group that the group's voters are in place.
//! The node list and every group's voters live in the metadata group;
//! nodes follow them (`node.rs`).

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_proto::{
    AddNodeRequest, DescribeNodesResponse, ErrorCode, MoveLeaderRequest, NodeEntry, RemoveNodeRequest, Response,
    SettleVotersRequest, node_state,
};
use lotse_raft::NodeId;
use lotse_rt::Runtime;

use super::admin::{AdminThen, admin_response};
use super::{Clients, ClusterView, Outbox};
use crate::metadata::{Change, MetadataState};
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// Every group whose voters the metadata group keeps: the metadata and
/// coordinator groups (their first voters are the configured nodes unless
/// changed since) and every runtime group.
pub(crate) fn managed_groups(
    state: &MetadataState,
    core: &[GroupId],
    configured_voters: &[NodeId],
) -> Vec<(GroupId, Vec<NodeId>)> {
    let mut out: Vec<(GroupId, Vec<NodeId>)> = core
        .iter()
        .map(|g| (*g, state.targets.get(g).cloned().unwrap_or_else(|| configured_voters.to_vec())))
        .collect();
    out.extend(state.groups().map(|(g, v, _)| (g, v.to_vec())));
    out
}

/// Whether `g`'s voters were changed at runtime and its leader has not yet
/// reported them in place.
pub(crate) fn moving(state: &MetadataState, g: GroupId, voters: &[NodeId]) -> bool {
    let changed = state.targets.contains_key(&g) || state.initial.contains_key(&g);
    changed && !state.is_settled(g, voters)
}

fn valid_address(a: &str) -> bool {
    a.rsplit_once(':').is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
}

impl Clients {
    pub(super) fn add_node<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: AddNodeRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let refuse = |code, why: &str| admin_response(code, Some(why), None);
        if req.node_id == 0 {
            return outbox.push(id, &refuse(ErrorCode::INVALID_REQUEST, "node ids start at 1"));
        }
        if !valid_address(&req.peer) || !valid_address(&req.client) {
            return outbox.push(id, &refuse(ErrorCode::INVALID_REQUEST, "addresses are host:port"));
        }
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        if let Some((peer, client)) = view.admin.members.get(&req.node_id) {
            // The same node again (a retry): done.
            let code =
                if *peer == req.peer && *client == req.client { ErrorCode::NONE } else { ErrorCode::RESOURCE_EXISTS };
            return outbox.push(id, &admin_response(code, Some("a node with this id is in the cluster"), None));
        }
        if state.gone.contains(&req.node_id) {
            let why = "this id belonged to a node that left; ids are never used twice";
            return outbox.push(id, &refuse(ErrorCode::INVALID_REQUEST, why));
        }
        let taken =
            view.admin.members.iter().any(|(_, (p, c))| [p, c].iter().any(|a| **a == req.peer || **a == req.client));
        if taken {
            return outbox.push(id, &refuse(ErrorCode::INVALID_REQUEST, "another node has this address"));
        }
        let change = Change::AddNode { id: req.node_id, peer: req.peer, client: req.client };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    pub(super) fn remove_node<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: RemoveNodeRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        if !view.admin.members.contains_key(&req.node_id) {
            return outbox.push(id, &admin_response(ErrorCode::UNKNOWN_RESOURCE, Some("no such node"), None));
        }
        if state.draining.contains(&req.node_id) {
            return outbox.push(id, &admin_response(ErrorCode::NONE, Some("draining already"), None));
        }
        // Every group must still find as many nodes as it has voters.
        let left = view.admin.nodes.iter().filter(|n| **n != req.node_id).count();
        let core: Vec<GroupId> = view.admin.metadata.into_iter().chain(view.coordinator).collect();
        let widest = managed_groups(state, &core, view.admin.configured_voters)
            .iter()
            .filter(|(g, _)| !core.contains(g))
            .map(|(_, v)| v.len())
            .max()
            .unwrap_or(1)
            .max(1);
        if left < widest {
            let why = format!("{left} nodes would be left for groups of {widest} replicas; add a node first");
            return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
        }
        let change = Change::DrainNode { id: req.node_id };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    /// `MoveLeader` (ADR 0065): pins a partition's preferred leader to one
    /// of its voters, or unpins it. The group's leader hands over in its next
    /// membership pass, as it does for any preferred voter.
    pub(super) fn move_leader<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: MoveLeaderRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        let state = replica.metadata();
        let group = match (state.streams.get(&req.resource_id), state.queues.get(&req.resource_id)) {
            (Some(s), _) => s.partitions.get(req.partition as usize).map(|(g, _)| *g),
            (None, Some(q)) => (req.partition == 0).then_some(q.group),
            (None, None) => {
                let why = "only partitions of streams and queues created at runtime have movable leaders";
                return outbox.push(id, &admin_response(ErrorCode::UNKNOWN_RESOURCE, Some(why), None));
            }
        };
        let Some(group) = group else {
            return outbox.push(id, &admin_response(ErrorCode::UNKNOWN_RESOURCE, Some("no such partition"), None));
        };
        let node = (req.node_id != 0).then_some(req.node_id);
        if let Some(n) = node {
            let voters = state.voters_of(group).unwrap_or_default();
            if !voters.contains(&n) {
                let why = format!("node {n} is not a voter of that partition (voters: {voters:?})");
                return outbox.push(id, &admin_response(ErrorCode::INVALID_REQUEST, Some(&why), None));
            }
        }
        let change = Change::PinLeader { group, node };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }

    pub(super) fn describe_nodes<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: &Outbox,
        id: u32,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeNodesResponse { error, leader_hint, nodes: Vec::new() };
                return outbox.push(id, &Response::DescribeNodes(resp));
            }
        };
        let state = replica.metadata();
        let core: Vec<GroupId> = view.admin.metadata.into_iter().chain(view.coordinator).collect();
        let managed = managed_groups(state, &core, view.admin.configured_voters);
        let nodes = view
            .admin
            .members
            .iter()
            .map(|(node, (peer, client))| {
                let mine: Vec<&(GroupId, Vec<NodeId>)> = managed.iter().filter(|(_, v)| v.contains(node)).collect();
                NodeEntry {
                    node_id: *node,
                    peer: peer.clone(),
                    client: client.clone(),
                    state: if state.draining.contains(node) { node_state::DRAINING } else { node_state::ACTIVE },
                    replicas: mine.len() as u32,
                    moving: mine.iter().filter(|(g, v)| moving(state, *g, v)).count() as u32,
                }
            })
            .collect();
        let resp = DescribeNodesResponse { error: ErrorCode::NONE, leader_hint: None, nodes };
        outbox.push(id, &Response::DescribeNodes(resp));
    }

    pub(super) fn settle_voters<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: SettleVotersRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        if replica.metadata().is_settled(req.group, &req.voters) {
            return outbox.push(id, &admin_response(ErrorCode::NONE, None, None));
        }
        let change = Change::Settled { group: req.group, voters: req.voters };
        self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed);
    }
}

//! Moving groups between nodes (ADR 0048), in two halves that never talk
//! to each other directly:
//!
//! - The metadata group's leader *plans*: it decides the voters each group
//!   is to have ([`plan`]) and records them (`SetVoters`). It moves the
//!   replicas of draining nodes, keeps the metadata and coordinator groups
//!   at up to five voters, and forgets a drained node once nothing holds
//!   it.
//! - Each group's own leader *carries it out*: it changes the group's Raft
//!   membership a step at a time towards those voters ([`next_step`]): the
//!   new nodes as learners first, then a joint change once they have caught
//!   up. When the membership is exactly the target, it reports that
//!   (`SettleVoters`), which lets the planner go on and the old replicas be
//!   removed.

use std::collections::{BTreeMap, BTreeSet};

use lotse_raft::{Membership, NodeId};

use crate::client::{managed_groups, moving};
use crate::metadata::{Change, MetadataState};
use crate::wire::GroupId;

/// Groups whose voters may be changing at once, cluster-wide: moves cost
/// a full copy of each group, and a few at a time keep the cluster
/// serving.
pub(crate) const MAX_MOVING: usize = 4;

/// The metadata and coordinator groups have at most this many voters.
pub(crate) const MAX_CORE_VOTERS: usize = 5;

/// What a group's leader does next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Step {
    /// Propose this membership.
    Propose { voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId> },
    /// The membership is the target: report it.
    Settled,
    /// A change is under way; the Raft core finishes it.
    Wait,
}

/// The next step from `current` towards `target` voters and `learners`
/// (nodes that follow the group without voting: for the metadata group,
/// every other node of the cluster).
pub(crate) fn next_step(current: &Membership, target: &BTreeSet<NodeId>, learners: &BTreeSet<NodeId>) -> Step {
    if current.is_joint() {
        return Step::Wait;
    }
    let want_learners: BTreeSet<NodeId> = learners.difference(target).copied().collect();
    // New voters start as learners, so they catch up before they count.
    let missing: BTreeSet<NodeId> =
        target.iter().filter(|n| !current.voters.contains(n) && !current.learners.contains(n)).copied().collect();
    if !missing.is_empty() {
        let learners = current.learners.union(&missing).chain(want_learners.iter()).copied();
        let learners = learners.filter(|n| !current.voters.contains(n)).collect();
        return Step::Propose { voters: current.voters.clone(), learners };
    }
    if current.voters != *target {
        return Step::Propose { voters: target.clone(), learners: want_learners };
    }
    if current.learners != want_learners {
        return Step::Propose { voters: target.clone(), learners: want_learners };
    }
    Step::Settled
}

/// What a group's leader does next, given the membership it has, the
/// voters and learners its view of the metadata group wants, and the
/// metadata index of that decision (`tag`, 0 if never changed). A group
/// that already carries out a newer decision than this view knows waits
/// for the view to catch up rather than undo it; one that already has what
/// is wanted under an older decision records the newer one, so that no
/// stale view undoes it either.
pub(crate) fn leader_step(
    current: &Membership,
    target: &BTreeSet<NodeId>,
    learners: &BTreeSet<NodeId>,
    tag: u64,
) -> Step {
    if current.tag > tag {
        return Step::Wait;
    }
    match next_step(current, target, learners) {
        Step::Settled if current.tag < tag => {
            Step::Propose { voters: current.voters.clone(), learners: current.learners.clone() }
        }
        step => step,
    }
}

/// Whether this node (`me`) may close and delete its replica of `g`: only
/// once it is no voter of what the metadata group wants and the group's
/// leader reported those voters in place. Before that the replica may still
/// be one of the copies the group's commits rest on.
pub(crate) fn replica_may_go(state: &MetadataState, g: GroupId, voters: &[NodeId], me: NodeId) -> bool {
    !voters.contains(&me) && state.is_settled(g, voters)
}

/// How many voters the metadata and coordinator groups should have with
/// `nodes` active nodes: up to five, and odd where it can be.
pub(crate) fn core_size(nodes: usize) -> usize {
    let n = nodes.min(MAX_CORE_VOTERS);
    if n > 1 && n % 2 == 0 { n - 1 } else { n.max(1) }
}

/// What the metadata group's leader proposes now, given its applied state:
/// `core` are the metadata and coordinator groups, whose first voters were
/// `configured_voters`; `active` the nodes not draining.
pub(crate) fn plan(
    state: &MetadataState,
    core: &[GroupId],
    configured_voters: &[NodeId],
    active: &[NodeId],
) -> Vec<Change> {
    let managed = managed_groups(state, core, configured_voters);
    let mut in_flight = managed.iter().filter(|(g, v)| moving(state, *g, v)).count();
    let mut load: BTreeMap<NodeId, usize> = active.iter().map(|n| (*n, 0)).collect();
    for (_, v) in &managed {
        for n in v {
            if let Some(l) = load.get_mut(n) {
                *l += 1;
            }
        }
    }
    let least_loaded = |load: &BTreeMap<NodeId, usize>, skip: &dyn Fn(NodeId) -> bool| {
        load.iter().filter(|(n, _)| !skip(**n)).min_by_key(|(n, l)| (**l, **n)).map(|(n, _)| *n)
    };
    let mut changes = Vec::new();

    // The metadata and coordinator groups: drop draining or gone voters,
    // then grow or shrink to the size the cluster calls for.
    for &g in core {
        let Some((_, voters)) = managed.iter().find(|(group, _)| *group == g) else { continue };
        if moving(state, g, voters) {
            continue;
        }
        let mut next: Vec<NodeId> = voters.iter().copied().filter(|n| load.contains_key(n)).collect();
        let want = core_size(active.len());
        while next.len() > want {
            // The busiest goes, the highest id among equals.
            let (i, _) = next.iter().enumerate().max_by_key(|(_, n)| (load[*n], **n)).expect("not empty");
            next.remove(i);
        }
        while next.len() < want {
            let Some(n) = least_loaded(&load, &|n| next.contains(&n)) else { break };
            next.push(n);
            *load.entry(n).or_default() += 1;
        }
        next.sort_unstable();
        if next != *voters && !next.is_empty() {
            changes.push(Change::SetVoters { group: g, voters: next });
            in_flight += 1;
        }
    }

    // Draining nodes: their runtime replicas move, a few groups at a time.
    for &d in &state.draining {
        for (g, voters) in managed.iter().filter(|(g, v)| v.contains(&d) && !core.contains(g)) {
            if in_flight >= MAX_MOVING {
                break;
            }
            if moving(state, *g, voters) {
                continue;
            }
            let left = state.left.get(g);
            let skip = |n: NodeId| voters.contains(&n) || left.is_some_and(|l| l.contains(&n));
            let Some(to) = least_loaded(&load, &skip) else { continue };
            let mut next: Vec<NodeId> = voters.iter().copied().filter(|n| *n != d).collect();
            next.push(to);
            next.sort_unstable();
            *load.entry(to).or_default() += 1;
            changes.push(Change::SetVoters { group: *g, voters: next });
            in_flight += 1;
        }
    }

    // A drained node is forgotten once no group counts it and no change
    // that moved it off is still under way.
    for &d in &state.draining {
        let held = managed.iter().any(|(_, v)| v.contains(&d));
        let settling = managed.iter().any(|(g, v)| {
            moving(state, *g, v) && (core.contains(g) || state.left.get(g).is_some_and(|l| l.contains(&d)))
        });
        let proposed = changes.iter().any(|c| matches!(c, Change::SetVoters { .. }));
        if !held && !settling && !proposed {
            changes.push(Change::ForgetNode { id: d });
        }
    }
    changes
}

#[cfg(test)]
mod tests;

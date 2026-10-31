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
/// be one of the copies the group's commits rest on. `replica_tag` is the
/// tag of the membership this node's replica holds: one newer than the
/// settled decision means this node's view of the metadata group lags (it
/// restarted, say) and the group has since placed it here again, so the
/// replica may be a voter whose copy commits rest on, and it stays (ADR 0050).
pub(crate) fn replica_may_go(
    state: &MetadataState,
    g: GroupId,
    voters: &[NodeId],
    me: NodeId,
    replica_tag: u64,
) -> bool {
    !voters.contains(&me) && state.is_settled(g, voters) && replica_tag <= state.target_at.get(&g).copied().unwrap_or(0)
}

/// Whether nodes holding `hi` and `lo` replicas are uneven enough to move
/// one: by more than one replica and more than a tenth of `hi`.
pub(crate) fn uneven(hi: usize, lo: usize) -> bool {
    hi.saturating_sub(lo) > 1.max(hi / 10)
}

/// Each group's preferred leader (ADR 0050): one of its voters, chosen so
/// that every node is preferred for as few groups as can be, the lowest id
/// among equals. Every node computes the same from the same metadata state,
/// so no node needs to know who leads what elsewhere.
pub(crate) fn preferred_leaders(managed: &[(GroupId, Vec<NodeId>)]) -> BTreeMap<GroupId, NodeId> {
    let mut sorted: Vec<&(GroupId, Vec<NodeId>)> = managed.iter().collect();
    sorted.sort_by_key(|(g, _)| *g);
    let mut count: BTreeMap<NodeId, usize> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (g, voters) in sorted {
        let Some(&n) = voters.iter().min_by_key(|n| (count.get(n).copied().unwrap_or(0), **n)) else { continue };
        *count.entry(n).or_default() += 1;
        out.insert(*g, n);
    }
    out
}

/// Whom the leader `me` of a group with `current` membership hands
/// leadership to: its `preferred` leader, if that is another voter that
/// holds everything the leader has (`matched` against `last`), and no
/// change of voters is under way.
pub(crate) fn hand_over_to(
    current: &Membership,
    me: NodeId,
    preferred: Option<NodeId>,
    matched: &BTreeMap<NodeId, u64>,
    last: u64,
) -> Option<NodeId> {
    let p = preferred?;
    if p == me || current.is_joint() || !current.voters.contains(&p) || !current.voters.contains(&me) {
        return None;
    }
    (matched.get(&p).copied().unwrap_or(0) >= last).then_some(p)
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
            let Some(to) = least_loaded(&load, &|n| voters.contains(&n)) else { continue };
            let mut next: Vec<NodeId> = voters.iter().copied().filter(|n| *n != d).collect();
            next.push(to);
            next.sort_unstable();
            *load.entry(to).or_default() += 1;
            changes.push(Change::SetVoters { group: *g, voters: next });
            in_flight += 1;
        }
    }

    // Rebalancing (ADR 0050): while nothing drains, one replica at a time
    // goes from the fullest node to the emptiest, until they are within
    // one replica (or a tenth of the fullest).
    let mut changed: BTreeSet<GroupId> = changes
        .iter()
        .filter_map(|c| match c {
            Change::SetVoters { group, .. } => Some(*group),
            _ => None,
        })
        .collect();
    if state.draining.is_empty() {
        while in_flight < MAX_MOVING {
            let Some((&full, &hi)) = load.iter().max_by_key(|(n, l)| (**l, **n)) else { break };
            let Some((&empty, &lo)) = load.iter().min_by_key(|(n, l)| (**l, **n)) else { break };
            if !uneven(hi, lo) {
                break;
            }
            let pick = managed.iter().find(|(g, v)| {
                !core.contains(g)
                    && v.contains(&full)
                    && !v.contains(&empty)
                    && !changed.contains(g)
                    && !moving(state, *g, v)
            });
            let Some((g, voters)) = pick else { break };
            let mut next: Vec<NodeId> = voters.iter().copied().filter(|n| *n != full).collect();
            next.push(empty);
            next.sort_unstable();
            *load.get_mut(&full).expect("active") -= 1;
            *load.get_mut(&empty).expect("active") += 1;
            changed.insert(*g);
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

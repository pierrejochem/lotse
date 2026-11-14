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
/// stale view undoes it either. A membership is settled only once it is
/// committed (`committed`): one merely appended may still be replaced by a
/// new leader that never had it, and a replica removed on the strength of
/// it would leave the group short of voters.
pub(crate) fn leader_step(
    current: &Membership,
    target: &BTreeSet<NodeId>,
    learners: &BTreeSet<NodeId>,
    tag: u64,
    committed: bool,
) -> Step {
    if current.tag > tag {
        return Step::Wait;
    }
    match next_step(current, target, learners) {
        Step::Settled if current.tag < tag => {
            Step::Propose { voters: current.voters.clone(), learners: current.learners.clone() }
        }
        Step::Settled if !committed => Step::Wait,
        step => step,
    }
}

/// Whether this node (`me`) runs a replica of `g`: as one of the voters the
/// metadata group wants, or as one it moved off while that move has not
/// settled. The group may still need that replica for a quorum: a node that
/// was down when the move began, and then started only the groups it is
/// to vote in, would leave a group that cannot elect a leader and so never
/// finishes the move.
pub(crate) fn hosts(state: &MetadataState, g: GroupId, voters: &[NodeId], me: NodeId) -> bool {
    voters.contains(&me) || (state.left.get(&g).is_some_and(|l| l.contains(&me)) && !state.is_settled(g, voters))
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

/// Whether a replica of `g` on this node (`me`), one of the voters the
/// metadata group wants, holds a log from before a settled move took the
/// group off this node, and must start over (ADR 0108). That replica was no
/// member when the move settled, so no commit rests on its log since; a
/// node that was down or cut off then never removed it, and going on from
/// it would bring back records the group has since compacted away. Its
/// membership (`replica_tag`) is newer than the move once it took part
/// again, and its log was already removed for that move if `moved_at`
/// says so: either way it stays.
pub(crate) fn starts_over(
    state: &MetadataState,
    g: GroupId,
    me: NodeId,
    replica_tag: u64,
    moved_at: Option<u64>,
) -> Option<u64> {
    let off = *state.moved_off.get(&g)?.get(&me)?;
    (replica_tag <= off && moved_at.is_none_or(|at| at < off)).then_some(off)
}

/// Whether a Raft message for runtime group `g` carries a membership newer
/// than the decision this node last refreshed its replicas with (`state`),
/// and must wait (ADR 0108). [`starts_over`] judges a replica by the
/// membership it holds: one that took the membership placing it here
/// again before its node saw the settled move off would look current and
/// go on from a log the group has since compacted. Held back, the message
/// comes again once the node has caught up with the metadata group, as
/// Raft resends what is not acknowledged.
pub(crate) fn ahead_of_view(state: &MetadataState, g: GroupId, msg: &lotse_raft::Message) -> bool {
    let seen = state.target_at.get(&g).copied().unwrap_or(0);
    let carried = match msg {
        lotse_raft::Message::Append { entries, .. } => entries
            .iter()
            .filter(|e| e.kind == lotse_raft::EntryKind::Config)
            .filter_map(|e| Membership::decode(&e.data))
            .map(|m| m.tag)
            .max(),
        lotse_raft::Message::Snapshot { meta, .. } => Some(meta.membership.tag),
        _ => None,
    };
    carried.is_some_and(|tag| tag > seen)
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
///
/// A group an operator pinned to one of its voters prefers that voter
/// (ADR 0065); pinned groups count first, so the others spread around them.
/// A pin to a node that is not a voter of the group counts for nothing.
pub(crate) fn preferred_leaders(
    managed: &[(GroupId, Vec<NodeId>)],
    pins: &BTreeMap<GroupId, NodeId>,
) -> BTreeMap<GroupId, NodeId> {
    let mut sorted: Vec<&(GroupId, Vec<NodeId>)> = managed.iter().collect();
    sorted.sort_by_key(|(g, _)| *g);
    let mut count: BTreeMap<NodeId, usize> = BTreeMap::new();
    let mut out = BTreeMap::new();
    for (g, voters) in &sorted {
        if let Some(&n) = pins.get(g).filter(|n| voters.contains(n)) {
            *count.entry(n).or_default() += 1;
            out.insert(*g, n);
        }
    }
    let pinned: BTreeSet<GroupId> = out.keys().copied().collect();
    for (g, voters) in sorted.into_iter().filter(|(g, _)| !pinned.contains(g)) {
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
    if n > 1 && n.is_multiple_of(2) { n - 1 } else { n.max(1) }
}

/// What the metadata group's leader proposes now, given its applied state:
/// `core` are the metadata and coordinator groups, whose first voters were
/// `configured_voters`; `active` the nodes not draining.
pub(crate) fn plan(
    state: &MetadataState,
    core: &[GroupId],
    configured_voters: &[NodeId],
    active: &[NodeId],
    sizes: &BTreeMap<GroupId, u64>,
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
            // Of the groups that may move, the one that evens out the two
            // nodes' bytes most (ADR 0052); without sizes, the first.
            let gap = bytes_on(&managed, sizes, full) as i128 - bytes_on(&managed, sizes, empty) as i128;
            let pick = managed
                .iter()
                .enumerate()
                .filter(|(_, (g, v))| {
                    !core.contains(g)
                        && v.contains(&full)
                        && !v.contains(&empty)
                        && !changed.contains(g)
                        && !moving(state, *g, v)
                })
                .min_by_key(|(i, (g, _))| ((gap - 2 * sizes.get(g).copied().unwrap_or(0) as i128).abs(), *i))
                .map(|(_, gv)| gv);
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
    let proposed = changes.iter().any(|c| matches!(c, Change::SetVoters { .. }));
    for &d in &state.draining {
        if drained(state, &managed, core, d) && !proposed {
            changes.push(Change::ForgetNode { id: d });
        }
    }
    changes
}

/// Whether this node (`me`) stops for good (ADR 0051): drained by its own
/// metadata state, or `forgotten` by the cluster (ADR 0105), which it may
/// learn without ever having seen that it was draining.
pub(crate) fn stops(
    state: &MetadataState,
    managed: &[(GroupId, Vec<NodeId>)],
    core: &[GroupId],
    me: NodeId,
    forgotten: bool,
) -> bool {
    forgotten || drained(state, managed, core, me)
}

/// Whether this node (`me`) is listed by its own metadata state: one of the
/// first voters (the nodes the cluster started with, kept on disk), or
/// added since, and not forgotten. A joining node is neither until its
/// addition is applied, whatever its configuration file names.
pub(crate) fn listed(state: &MetadataState, first_voters: &[NodeId], me: NodeId) -> bool {
    (first_voters.contains(&me) || state.nodes.contains_key(&me)) && !state.gone.contains(&me)
}

/// Whether the cluster forgot this node (ADR 0105), from an answer of the
/// metadata leader that is at least as new as the node's own metadata
/// state: its own state lists it ([`listed`]), the answer does not. Between
/// the two the cluster drained and forgot it, and ids are never used twice.
/// A joining node never counts as forgotten: its own state does not list it
/// until it has been added.
pub(crate) fn forgotten(listed_by_own_state: bool, listed_by_answer: bool) -> bool {
    listed_by_own_state && !listed_by_answer
}

/// Bytes of the groups `node` is a voter of, as far as `sizes` knows them.
fn bytes_on(managed: &[(GroupId, Vec<NodeId>)], sizes: &BTreeMap<GroupId, u64>, node: NodeId) -> u64 {
    managed.iter().filter(|(_, v)| v.contains(&node)).map(|(g, _)| sizes.get(g).copied().unwrap_or(0)).sum()
}

/// Whether node `d` is drained: it is draining, no group counts it among
/// its voters, and no change that moved it off is still under way. The
/// planner then forgets it, and the node itself, seeing the same in its own
/// metadata state, stops (ADR 0051).
pub(crate) fn drained(state: &MetadataState, managed: &[(GroupId, Vec<NodeId>)], core: &[GroupId], d: NodeId) -> bool {
    let held = managed.iter().any(|(_, v)| v.contains(&d));
    let settling = managed
        .iter()
        .any(|(g, v)| moving(state, *g, v) && (core.contains(g) || state.left.get(g).is_some_and(|l| l.contains(&d))));
    state.draining.contains(&d) && !held && !settling
}

#[cfg(test)]
mod tests;

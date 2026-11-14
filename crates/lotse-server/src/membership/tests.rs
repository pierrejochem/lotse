use super::*;
use crate::metadata::{NodeMeta, StreamSettings, record_payload};

fn set(n: &[NodeId]) -> BTreeSet<NodeId> {
    n.iter().copied().collect()
}

fn m(voters: &[NodeId], learners: &[NodeId]) -> Membership {
    Membership { voters: set(voters), outgoing: BTreeSet::new(), learners: set(learners), tag: 0 }
}

#[test]
fn a_leader_adds_learners_first_then_swaps_voters() {
    let target = set(&[1, 3, 4]);
    // 4 is new: a learner first; nothing else changes yet.
    assert_eq!(
        next_step(&m(&[1, 2, 3], &[]), &target, &BTreeSet::new()),
        Step::Propose { voters: set(&[1, 2, 3]), learners: set(&[4]) }
    );
    // Then the voters, in one joint change.
    assert_eq!(
        next_step(&m(&[1, 2, 3], &[4]), &target, &BTreeSet::new()),
        Step::Propose { voters: set(&[1, 3, 4]), learners: set(&[]) }
    );
    let joint = Membership { voters: set(&[1, 3, 4]), outgoing: set(&[1, 2, 3]), learners: set(&[]), tag: 0 };
    assert_eq!(next_step(&joint, &target, &BTreeSet::new()), Step::Wait, "the core finishes a joint change");
    assert_eq!(next_step(&m(&[1, 3, 4], &[]), &target, &BTreeSet::new()), Step::Settled);
    // The metadata group: every other node follows as a learner, a voter
    // that leaves stays one.
    let all = set(&[1, 2, 3, 4, 5]);
    assert_eq!(
        next_step(&m(&[1, 2, 3], &[4, 5]), &set(&[1, 3, 4]), &all),
        Step::Propose { voters: set(&[1, 3, 4]), learners: set(&[2, 5]) }
    );
    assert_eq!(
        next_step(&m(&[1, 3, 4], &[2, 5]), &set(&[1, 3, 4]), &set(&[1, 3, 4, 5])),
        Step::Propose { voters: set(&[1, 3, 4]), learners: set(&[5]) },
        "a forgotten node stops following"
    );
}

#[test]
fn core_groups_have_up_to_five_voters_odd_where_possible() {
    let sizes: Vec<usize> = (1..=12).map(core_size).collect();
    assert_eq!(sizes, vec![1, 1, 3, 3, 5, 5, 5, 5, 5, 5, 5, 5]);
}

fn with_stream(voters: &[&[NodeId]]) -> MetadataState {
    let mut s = MetadataState::default();
    let create = Change::CreateStream {
        name: "o".into(),
        tenant: "t".into(),
        partitions: voters.iter().map(|v| v.to_vec()).collect(),
        settings: StreamSettings::default(),
    };
    s.apply(10, &record_payload(&create)).unwrap();
    s
}

fn apply_all(s: &mut MetadataState, at: &mut u64, changes: &[Change]) {
    for c in changes {
        *at += 1;
        s.apply(*at, &record_payload(c)).unwrap();
    }
}

#[test]
fn a_drained_node_is_emptied_a_few_groups_at_a_time_then_forgotten() {
    let partitions: Vec<Vec<NodeId>> = (0..6).map(|_| vec![1, 2, 3]).collect();
    let refs: Vec<&[NodeId]> = partitions.iter().map(Vec::as_slice).collect();
    let mut s = with_stream(&refs);
    let core = [1, 2];
    s.nodes.insert(4, NodeMeta { peer: "n4:1".into(), client: "n4:2".into(), zone: String::new() });
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 2 }]);
    let active = [1, 3, 4];
    let first = plan(&s, &core, &[1, 2, 3], &active, &BTreeMap::new());
    // The core groups drop 2 and take 4; MAX_MOVING bounds the rest.
    let moves: Vec<&Change> = first.iter().filter(|c| matches!(c, Change::SetVoters { .. })).collect();
    assert_eq!(moves.len(), MAX_MOVING, "{first:?}");
    for c in &first {
        let Change::SetVoters { voters, .. } = c else { panic!("{c:?}") };
        assert!(!voters.contains(&2) && voters.contains(&4), "{c:?}");
    }
    apply_all(&mut s, &mut at, &first);
    assert!(
        plan(&s, &core, &[1, 2, 3], &active, &BTreeMap::new()).is_empty(),
        "nothing more while those are in flight"
    );
    // Each group's leader reports its voters in place; the planner goes on.
    let mut rounds = 0;
    loop {
        let settled: Vec<Change> = managed_groups(&s, &core, &[1, 2, 3])
            .into_iter()
            .filter(|(g, v)| moving(&s, *g, v))
            .map(|(group, voters)| Change::Settled { group, voters })
            .collect();
        apply_all(&mut s, &mut at, &settled);
        let next = plan(&s, &core, &[1, 2, 3], &active, &BTreeMap::new());
        if next.iter().any(|c| matches!(c, Change::ForgetNode { id: 2 })) {
            apply_all(&mut s, &mut at, &next);
            break;
        }
        assert!(!next.is_empty(), "the drain stalled: {s:?}");
        apply_all(&mut s, &mut at, &next);
        rounds += 1;
        assert!(rounds < 10);
    }
    assert!(s.gone.contains(&2));
    for (g, v) in managed_groups(&s, &core, &[1, 2, 3]) {
        assert!(!v.contains(&2), "group {g} still counts node 2");
        assert_eq!(v.len(), 3, "group {g} kept its replication");
    }
}

#[test]
fn a_new_node_joins_the_core_groups_while_they_are_short() {
    let s = with_stream(&[&[1, 2, 3]]);
    let core = [1, 2];
    // Five nodes: the core groups grow from three voters to five.
    let changes = plan(&s, &core, &[1, 2, 3], &[1, 2, 3, 4, 5], &BTreeMap::new());
    let core_changes: Vec<&Change> =
        changes.iter().filter(|c| matches!(c, Change::SetVoters { group: 1 | 2, .. })).collect();
    assert_eq!(
        core_changes,
        vec![
            &Change::SetVoters { group: 1, voters: vec![1, 2, 3, 4, 5] },
            &Change::SetVoters { group: 2, voters: vec![1, 2, 3, 4, 5] }
        ]
    );
}

#[test]
fn a_leader_with_an_older_view_never_undoes_a_newer_change() {
    let tagged = |voters: &[NodeId], tag| Membership { tag, ..m(voters, &[]) };
    // The group carries out decision 9; this leader's view knows only 5,
    // which wanted node 2 back.
    assert_eq!(leader_step(&tagged(&[1, 3, 4], 9), &set(&[1, 2, 3]), &BTreeSet::new(), 5, true), Step::Wait);
    // The same view, as new as the group: it goes ahead, tagging the change.
    assert_eq!(
        leader_step(&tagged(&[1, 3, 4], 9), &set(&[1, 3, 5]), &BTreeSet::new(), 9, true),
        Step::Propose { voters: set(&[1, 3, 4]), learners: set(&[5]) }
    );
    // Already the wanted voters, under an older decision: record the newer.
    assert_eq!(
        leader_step(&tagged(&[1, 3, 4], 2), &set(&[1, 3, 4]), &BTreeSet::new(), 7, true),
        Step::Propose { voters: set(&[1, 3, 4]), learners: set(&[]) }
    );
    assert_eq!(leader_step(&tagged(&[1, 3, 4], 7), &set(&[1, 3, 4]), &BTreeSet::new(), 7, true), Step::Settled);
    assert_eq!(
        leader_step(&tagged(&[1, 3, 4], 7), &set(&[1, 3, 4]), &BTreeSet::new(), 7, false),
        Step::Wait,
        "not before the membership is committed: a new leader may never have had it"
    );
}

#[test]
fn a_replica_goes_only_after_its_group_settled_without_it() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    let tag = s.target_at[&g];
    let voters = s.voters_of(g).unwrap().to_vec();
    assert!(!replica_may_go(&s, g, &voters, 2, tag), "node 2 may still be one of the copies commits rest on");
    assert!(!replica_may_go(&s, g, &voters, 1, tag), "a voter keeps its replica");
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    assert!(replica_may_go(&s, g, &voters, 2, tag));
    assert!(replica_may_go(&s, g, &voters, 2, 0), "a replica that never saw the move goes too");
    assert!(!replica_may_go(&s, g, &voters, 3, tag));
}

#[test]
fn a_replica_placed_here_again_stays_while_this_node_lags() {
    // Node 2 restarted and its metadata view reached only the settled move
    // off; its replica already holds a newer decision that placed it back,
    // under which it may have acknowledged entries.
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    let voters = s.voters_of(g).unwrap().to_vec();
    let newer = s.target_at[&g] + 5;
    assert!(!replica_may_go(&s, g, &voters, 2, newer), "the group moved on; this view is old");
}

#[test]
fn a_replica_from_before_a_settled_move_off_starts_over_when_placed_again() {
    // Node 2 holds the group, is down while it moves to node 4, and the
    // group comes back to it before it starts again (ADR 0108).
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    let before = 50; // the membership node 2's replica last held
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    let off = s.target_at[&g];
    assert_eq!(starts_over(&s, g, 2, before, None), None, "not settled: node 2 may hold copies commits rest on");
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    assert_eq!(s.moved_off[&g].get(&2), Some(&off), "the settled move off is recorded");
    assert!(!s.moved_off[&g].contains_key(&4), "a voter is not");
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 2, 3] }]);
    assert_eq!(starts_over(&s, g, 2, before, None), Some(off), "its log is from before it left");
    assert_eq!(starts_over(&s, g, 2, off, None), Some(off), "the membership of the move off itself is no newer");
    assert_eq!(starts_over(&s, g, 2, s.target_at[&g], None), None, "it took part again: commits may rest on it");
    assert_eq!(starts_over(&s, g, 2, 0, Some(off)), None, "already removed for that move: it catches up");
    assert_eq!(starts_over(&s, g, 2, 0, Some(off - 1)), Some(off), "removed for an older move only");
    assert_eq!(starts_over(&s, g, 1, before, None), None, "node 1 never left");
}

#[test]
fn only_a_report_of_the_current_decision_records_a_move_off() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 2, 3] }]);
    // A late report of the first decision: node 2 never left in fact.
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    assert!(s.moved_off.get(&g).is_none_or(|m| !m.contains_key(&2)), "an older decision's report moves nothing off");
    assert_eq!(starts_over(&s, g, 2, 0, None), None);
}

#[test]
fn a_node_that_left_a_group_may_hold_it_again() {
    // Node 5 left the group and carries nothing: it keeps the group's term
    // and vote, so it may take the group again (ADR 0050).
    let mut s = with_stream(&[&[1, 2, 5], &[6, 7, 8]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 2, 3] }]);
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 2, 3] }]);
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 3 }]);
    let next = plan(&s, &[], &[], &[1, 2, 5, 6, 7, 8], &BTreeMap::new());
    let to = next.iter().find_map(|c| match c {
        Change::SetVoters { group, voters } if *group == g => Some(voters.clone()),
        _ => None,
    });
    assert_eq!(to, Some(vec![1, 2, 5]), "the least loaded node takes it: {next:?}");
}

#[test]
fn a_new_node_gets_replicas_until_nodes_are_within_one() {
    // Twelve groups of three on nodes 1-3; node 4 joins empty.
    let partitions: Vec<Vec<NodeId>> = (0..12).map(|_| vec![1, 2, 3]).collect();
    let refs: Vec<&[NodeId]> = partitions.iter().map(Vec::as_slice).collect();
    let mut s = with_stream(&refs);
    let active = [1, 2, 3, 4];
    let mut at = 100;
    let mut rounds = 0;
    loop {
        let next = plan(&s, &[], &[], &active, &BTreeMap::new());
        assert!(next.iter().filter(|c| matches!(c, Change::SetVoters { .. })).count() <= MAX_MOVING, "{next:?}");
        if next.is_empty() {
            break;
        }
        apply_all(&mut s, &mut at, &next);
        let settled: Vec<Change> = managed_groups(&s, &[], &[])
            .into_iter()
            .filter(|(g, v)| moving(&s, *g, v))
            .map(|(group, voters)| Change::Settled { group, voters })
            .collect();
        apply_all(&mut s, &mut at, &settled);
        rounds += 1;
        assert!(rounds < 20, "rebalancing does not end");
    }
    let mut load: BTreeMap<NodeId, usize> = BTreeMap::new();
    for (_, v) in managed_groups(&s, &[], &[]) {
        assert_eq!(v.len(), 3, "a move keeps the replication");
        for n in v {
            *load.entry(n).or_default() += 1;
        }
    }
    assert_eq!(load.values().sum::<usize>(), 36);
    let (hi, lo) = (*load.values().max().unwrap(), *load.values().min().unwrap());
    assert!(hi - lo <= 1, "{load:?}");
}

#[test]
fn nothing_rebalances_while_a_node_drains_or_when_nearly_even() {
    let mut s = with_stream(&[&[1, 2, 3], &[1, 2, 3]]);
    let mut at = 100;
    // Nodes 1-3 hold two each, node 4 none: 2 - 0 is more than one.
    assert!(!plan(&s, &[], &[], &[1, 2, 3, 4], &BTreeMap::new()).is_empty());
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 3 }]);
    let next = plan(&s, &[], &[], &[1, 2, 4], &BTreeMap::new());
    assert!(
        next.iter().all(|c| match c {
            Change::SetVoters { voters, .. } => !voters.contains(&3),
            _ => true,
        }),
        "only the drain moves replicas: {next:?}"
    );
    assert!(!uneven(10, 9) && !uneven(30, 28) && uneven(30, 26) && uneven(2, 0));
}

#[test]
fn preferred_leaders_spread_over_the_voters() {
    let managed: Vec<(GroupId, Vec<NodeId>)> = (0..9).map(|g| (g, vec![1, 2, 3])).collect();
    let preferred = preferred_leaders(&managed, &BTreeMap::new());
    let mut count: BTreeMap<NodeId, usize> = BTreeMap::new();
    for n in preferred.values() {
        *count.entry(*n).or_default() += 1;
    }
    assert_eq!(count, BTreeMap::from([(1, 3), (2, 3), (3, 3)]));
    assert_eq!(preferred_leaders(&managed, &BTreeMap::new()), preferred, "every node computes the same");
    // Always one of the group's voters.
    let mixed = vec![(1, vec![4, 5, 6]), (2, vec![1, 5, 6])];
    let p = preferred_leaders(&mixed, &BTreeMap::new());
    assert!(mixed.iter().all(|(g, v)| v.contains(&p[g])));
}

#[test]
fn a_pinned_group_prefers_its_pin_while_it_is_a_voter() {
    let managed: Vec<(GroupId, Vec<NodeId>)> = (0..6).map(|g| (g, vec![1, 2, 3])).collect();
    let free = preferred_leaders(&managed, &BTreeMap::new());
    assert_eq!(free[&0], 1);
    // Group 0 pinned to node 3: it prefers node 3, and the rest still spread
    // evenly around it.
    let pinned = preferred_leaders(&managed, &BTreeMap::from([(0, 3)]));
    assert_eq!(pinned[&0], 3);
    let mut count: BTreeMap<NodeId, usize> = BTreeMap::new();
    for n in pinned.values() {
        *count.entry(*n).or_default() += 1;
    }
    assert_eq!(count, BTreeMap::from([(1, 2), (2, 2), (3, 2)]));
    // A pin to a node that is not a voter counts for nothing.
    assert_eq!(preferred_leaders(&managed, &BTreeMap::from([(0, 9)])), free);
}

#[test]
fn pins_are_kept_in_the_state_and_go_with_their_group_and_node() {
    let mut st = MetadataState::default();
    let pin = |g, n| crate::metadata::Change::PinLeader { group: g, node: n };
    st.apply(1, &record_payload(&pin(7, Some(2)))).unwrap();
    st.apply(2, &record_payload(&pin(8, Some(3)))).unwrap();
    assert_eq!(st.pins, BTreeMap::from([(7, 2), (8, 3)]));
    let back = MetadataState::decode(&st.encode()).unwrap();
    assert_eq!(back.pins, st.pins, "snapshots keep pins");
    st.apply(3, &record_payload(&pin(7, None))).unwrap();
    assert_eq!(st.pins, BTreeMap::from([(8, 3)]), "unpinned");
    st.apply(4, &record_payload(&crate::metadata::Change::ForgetNode { id: 3 })).unwrap();
    assert!(st.pins.is_empty(), "a node that left takes its pins");
    // A snapshot without pins is as it was before pins.
    let empty = MetadataState::default().encode();
    assert_eq!(MetadataState::decode(&empty).unwrap(), MetadataState::default());
}

#[test]
fn leadership_goes_only_to_a_preferred_voter_that_has_everything() {
    let current = m(&[1, 2, 3], &[4]);
    let matched = BTreeMap::from([(1, 50), (2, 50), (3, 40), (4, 50)]);
    assert_eq!(hand_over_to(&current, 1, Some(2), &matched, 50), Some(2));
    assert_eq!(hand_over_to(&current, 1, Some(3), &matched, 50), None, "node 3 lags behind");
    assert_eq!(hand_over_to(&current, 1, Some(4), &matched, 50), None, "a learner never leads");
    assert_eq!(hand_over_to(&current, 1, Some(1), &matched, 50), None, "already the leader");
    let joint = Membership { voters: set(&[1, 2, 4]), outgoing: set(&[1, 2, 3]), learners: set(&[]), tag: 0 };
    assert_eq!(hand_over_to(&joint, 1, Some(2), &matched, 50), None, "not during a change of voters");
}

#[test]
fn a_node_moved_off_a_group_runs_it_until_the_move_settled() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    let voters = s.voters_of(g).unwrap().to_vec();
    assert!(hosts(&s, g, &voters, 4), "a new voter");
    assert!(hosts(&s, g, &voters, 2), "the group may need it for a quorum until it settles");
    assert!(!hosts(&s, g, &voters, 5), "never a member");
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    assert!(!hosts(&s, g, &voters, 2), "settled without it");
}

#[test]
fn a_node_is_drained_once_no_group_needs_it() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    let managed = |s: &MetadataState| crate::client::managed_groups(s, &[], &[]);
    assert!(!drained(&s, &managed(&s), &[], 4), "never draining");
    apply_all(
        &mut s,
        &mut at,
        &[Change::AddNode { id: 4, peer: "n4:1".into(), client: "n4:2".into(), zone: String::new() }],
    );
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 2 }]);
    assert!(!drained(&s, &managed(&s), &[], 2), "still a voter");
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    assert!(!drained(&s, &managed(&s), &[], 2), "the move off it has not settled");
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    assert!(drained(&s, &managed(&s), &[], 2));
    assert!(!drained(&s, &managed(&s), &[], 1), "a node that does not drain");
    assert!(stops(&s, &managed(&s), &[], 2, false));
    assert!(!stops(&s, &managed(&s), &[], 1, false), "a node that does not drain stays");
}

#[test]
fn a_draining_node_the_cluster_forgot_stops() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 2 }]);
    let managed = crate::client::managed_groups(&s, &[], &[]);
    assert!(!stops(&s, &managed, &[], 2, false), "its view still has it holding a group");
    assert!(stops(&s, &managed, &[], 2, true), "but the cluster forgot it");
}

#[test]
fn a_node_the_cluster_forgot_stops_though_it_never_saw_its_drain() {
    // Switched off before the drain, or cut off after the drain's entry
    // reached it but not its commit (ADR 0105): its own state has it active
    // and holding a group, and still it stops once forgotten.
    let s = with_stream(&[&[1, 2, 3]]);
    let managed = crate::client::managed_groups(&s, &[], &[]);
    assert!(!s.draining.contains(&2));
    assert!(!stops(&s, &managed, &[], 2, false));
    assert!(stops(&s, &managed, &[], 2, true));
}

#[test]
fn a_node_is_listed_by_its_own_state_once_it_was_a_first_voter_or_added() {
    let mut s = MetadataState::default();
    let mut at = 1;
    assert!(listed(&s, &[1, 2, 3], 2), "a first voter");
    assert!(!listed(&s, &[1, 2, 3], 4), "a joining node its configuration names, before it is added");
    let add = Change::AddNode { id: 4, peer: "n4:7000".into(), client: "n4:9000".into(), zone: String::new() };
    apply_all(&mut s, &mut at, &[add]);
    assert!(listed(&s, &[1, 2, 3], 4), "added");
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 2 }, Change::ForgetNode { id: 2 }]);
    assert!(!listed(&s, &[1, 2, 3], 2), "forgotten");
}

#[test]
fn only_a_node_its_own_state_lists_can_be_forgotten() {
    assert!(forgotten(true, false), "listed once, left out by a newer answer");
    assert!(!forgotten(true, true), "still listed");
    assert!(!forgotten(false, false), "a joining node is not listed until added: it stays");
    assert!(!forgotten(false, true));
}

#[test]
fn a_move_evens_out_bytes_as_far_as_counts_allow() {
    // Twelve groups of three on nodes 1-3, one of them far larger; node 4
    // joins empty. The counts call for a move off a full node; of the
    // groups it may take, the large one evens out the bytes most.
    let partitions: Vec<Vec<NodeId>> = (0..12).map(|_| vec![1, 2, 3]).collect();
    let refs: Vec<&[NodeId]> = partitions.iter().map(Vec::as_slice).collect();
    let s = with_stream(&refs);
    let big = crate::metadata::group_id(10, 7);
    let sizes: BTreeMap<GroupId, u64> =
        (0..12).map(|p| (crate::metadata::group_id(10, p), if p == 7 { 1_000 } else { 10 })).collect();
    let next = plan(&s, &[], &[], &[1, 2, 3, 4], &sizes);
    let first = next.iter().find_map(|c| match c {
        Change::SetVoters { group, .. } => Some(*group),
        _ => None,
    });
    assert_eq!(first, Some(big), "{next:?}");
    // Without sizes, the same moves as by count alone.
    let by_count = plan(&s, &[], &[], &[1, 2, 3, 4], &BTreeMap::new());
    let first = by_count.iter().find_map(|c| match c {
        Change::SetVoters { group, .. } => Some(*group),
        _ => None,
    });
    assert_eq!(first, Some(crate::metadata::group_id(10, 0)));
    assert_eq!(by_count.len(), next.len(), "sizes choose between moves, they add none");
}

#[test]
fn a_membership_newer_than_this_nodes_view_waits() {
    use bytes::Bytes;
    use lotse_raft::{Entry, EntryKind, Message, SnapshotMeta};
    let mut s = with_stream(&[&[1, 2, 3]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    let seen = s.target_at[&g];
    let tagged = |tag| Membership { tag, ..m(&[1, 2, 3], &[]) };
    let append =
        |entries: Vec<Entry>| Message::Append { term: 3, prev_index: 0, prev_term: 0, entries, commit: 0, seq: 1 };
    let config = |tag| Entry { first: 7, last: 7, term: 3, kind: EntryKind::Config, data: tagged(tag).encode() };
    let data = Entry { first: 6, last: 6, term: 3, kind: EntryKind::Normal, data: Bytes::from_static(b"x") };
    assert!(!ahead_of_view(&s, g, &append(vec![data.clone()])), "data alone never waits");
    assert!(!ahead_of_view(&s, g, &append(vec![data.clone(), config(seen)])), "the decision this node saw");
    assert!(ahead_of_view(&s, g, &append(vec![data, config(seen + 1)])), "a newer one waits");
    let snapshot = |tag| Message::Snapshot {
        term: 3,
        meta: SnapshotMeta { index: 9, term: 3, membership: tagged(tag) },
        offset: 0,
        data: Bytes::new(),
        done: false,
        seq: 1,
    };
    assert!(!ahead_of_view(&s, g, &snapshot(seen)));
    assert!(ahead_of_view(&s, g, &snapshot(seen + 1)), "a snapshot carries its membership too");
    assert!(!ahead_of_view(&s, g, &Message::TimeoutNow { term: 3 }));
    let other = crate::metadata::group_id(77, 0);
    assert!(ahead_of_view(&s, other, &append(vec![config(1)])), "a group this node has seen no decision of");
}

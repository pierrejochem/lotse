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
    s.nodes.insert(4, NodeMeta { peer: "n4:1".into(), client: "n4:2".into() });
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 2 }]);
    let active = [1, 3, 4];
    let first = plan(&s, &core, &[1, 2, 3], &active);
    // The core groups drop 2 and take 4; MAX_MOVING bounds the rest.
    let moves: Vec<&Change> = first.iter().filter(|c| matches!(c, Change::SetVoters { .. })).collect();
    assert_eq!(moves.len(), MAX_MOVING, "{first:?}");
    for c in &first {
        let Change::SetVoters { voters, .. } = c else { panic!("{c:?}") };
        assert!(!voters.contains(&2) && voters.contains(&4), "{c:?}");
    }
    apply_all(&mut s, &mut at, &first);
    assert!(plan(&s, &core, &[1, 2, 3], &active).is_empty(), "nothing more while those are in flight");
    // Each group's leader reports its voters in place; the planner goes on.
    let mut rounds = 0;
    loop {
        let settled: Vec<Change> = managed_groups(&s, &core, &[1, 2, 3])
            .into_iter()
            .filter(|(g, v)| moving(&s, *g, v))
            .map(|(group, voters)| Change::Settled { group, voters })
            .collect();
        apply_all(&mut s, &mut at, &settled);
        let next = plan(&s, &core, &[1, 2, 3], &active);
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
    let changes = plan(&s, &core, &[1, 2, 3], &[1, 2, 3, 4, 5]);
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
fn a_node_that_left_a_group_may_hold_it_again() {
    // Node 5 left the group and carries nothing: it keeps the group's term
    // and vote, so it may take the group again (ADR 0050).
    let mut s = with_stream(&[&[1, 2, 5], &[6, 7, 8]]);
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 2, 3] }]);
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 2, 3] }]);
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 3 }]);
    let next = plan(&s, &[], &[], &[1, 2, 5, 6, 7, 8]);
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
        let next = plan(&s, &[], &[], &active);
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
    assert!(!plan(&s, &[], &[], &[1, 2, 3, 4]).is_empty());
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 3 }]);
    let next = plan(&s, &[], &[], &[1, 2, 4]);
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
    let preferred = preferred_leaders(&managed);
    let mut count: BTreeMap<NodeId, usize> = BTreeMap::new();
    for n in preferred.values() {
        *count.entry(*n).or_default() += 1;
    }
    assert_eq!(count, BTreeMap::from([(1, 3), (2, 3), (3, 3)]));
    assert_eq!(preferred_leaders(&managed), preferred, "every node computes the same");
    // Always one of the group's voters.
    let mixed = vec![(1, vec![4, 5, 6]), (2, vec![1, 5, 6])];
    let p = preferred_leaders(&mixed);
    assert!(mixed.iter().all(|(g, v)| v.contains(&p[g])));
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

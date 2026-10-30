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
fn a_new_node_joins_the_core_groups_while_they_are_short_and_never_returns_to_a_group_it_left() {
    let mut s = with_stream(&[&[1, 2, 3]]);
    let core = [1, 2];
    // Five nodes: the core groups grow from three voters to five.
    let changes = plan(&s, &core, &[1, 2, 3], &[1, 2, 3, 4, 5]);
    assert_eq!(
        changes,
        vec![
            Change::SetVoters { group: 1, voters: vec![1, 2, 3, 4, 5] },
            Change::SetVoters { group: 2, voters: vec![1, 2, 3, 4, 5] }
        ]
    );
    // A runtime group never goes back to a node it left.
    let g = crate::metadata::group_id(10, 0);
    let mut at = 100;
    apply_all(&mut s, &mut at, &[Change::SetVoters { group: g, voters: vec![1, 3, 4] }]);
    apply_all(&mut s, &mut at, &[Change::Settled { group: g, voters: vec![1, 3, 4] }]);
    apply_all(&mut s, &mut at, &[Change::DrainNode { id: 4 }]);
    let next = plan(&s, &core, &[1, 2, 3], &[1, 2, 3, 5]);
    let to = next.iter().find_map(|c| match c {
        Change::SetVoters { group, voters } if *group == g => Some(voters.clone()),
        _ => None,
    });
    assert_eq!(to, Some(vec![1, 3, 5]), "node 2 left this group, so node 5 takes it: {next:?}");
}

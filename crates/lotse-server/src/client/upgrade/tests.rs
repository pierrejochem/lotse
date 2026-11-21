use lotse_proto::cluster_version::of;

use super::*;
use crate::metadata::{MetadataState, Turn, record_payload};

/// Applies each change in order, as every replica does, through the record
/// format.
fn applied(changes: &[Change]) -> MetadataState {
    let mut s = MetadataState::default();
    for (i, c) in changes.iter().enumerate() {
        s.apply(i as u64 + 1, &record_payload(c)).expect("applies");
    }
    s
}

/// The state after a snapshot of it is read back.
fn round_trip(s: &MetadataState) -> MetadataState {
    MetadataState::decode(&s.encode()).expect("reads its own snapshot")
}

fn report(node: NodeId, oldest: u32, newest: u32) -> Change {
    Change::ReportVersion { node, release: format!("0.{}.0", newest & 0xffff), oldest, newest, cluster_default: None }
}

#[test]
fn one_node_holds_the_turn_at_a_time() {
    let s = applied(&[
        Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 },
        // Committed before the first applied: the leader's check let both
        // through.
        Change::ClaimTurn { node: 2, instance: 22, at_ms: 101 },
    ]);
    assert_eq!(s.turn, Some(Turn { node: 1, instance: 11, since_ms: 100 }));
    assert_eq!(turn_refused(&s, 2).as_deref(), Some("node 1 holds the restart turn"));
    assert_eq!(turn_refused(&s, 1), None);
    assert_eq!(round_trip(&s).turn, s.turn);
}

#[test]
fn a_claim_by_the_holder_keeps_the_instance_that_took_the_turn() {
    // The restarted process asks again before it gave the turn back: the
    // turn must stay with the instance that took it, or the new process
    // would never give it back.
    let s = applied(&[
        Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 },
        Change::ClaimTurn { node: 1, instance: 12, at_ms: 200 },
    ]);
    assert_eq!(s.turn, Some(Turn { node: 1, instance: 11, since_ms: 100 }));
}

#[test]
fn only_the_holders_release_frees_the_turn() {
    let s = applied(&[Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 }, Change::ReleaseTurn { node: 2 }]);
    assert!(s.turn.is_some(), "node 2 gave back a turn it did not hold");
    let s = applied(&[
        Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 },
        Change::ReleaseTurn { node: 1 },
        Change::ClaimTurn { node: 2, instance: 22, at_ms: 300 },
    ]);
    assert_eq!(s.turn.map(|t| t.node), Some(2));
}

#[test]
fn a_pause_lets_the_holder_finish_and_gives_no_next_turn() {
    let s = applied(&[Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 }, Change::HoldTurns { held: true }]);
    assert_eq!(s.turn.map(|t| t.node), Some(1), "the node holding the turn finishes");
    assert_eq!(turn_refused(&s, 1), None);
    let s = applied(&[
        Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 },
        Change::HoldTurns { held: true },
        Change::ReleaseTurn { node: 1 },
        Change::ClaimTurn { node: 2, instance: 22, at_ms: 300 },
    ]);
    assert_eq!(s.turn, None);
    assert!(turn_refused(&s, 2).is_some_and(|w| w.contains("paused")));
    assert!(round_trip(&s).turns_held);
    let s = applied(&[
        Change::HoldTurns { held: true },
        Change::HoldTurns { held: false },
        Change::ClaimTurn { node: 2, instance: 22, at_ms: 300 },
    ]);
    assert_eq!(s.turn.map(|t| t.node), Some(2));
}

#[test]
fn a_removed_node_gives_its_turn_back_and_takes_none() {
    let s = applied(&[
        report(1, of(0, 4), of(0, 5)),
        Change::ClaimTurn { node: 1, instance: 11, at_ms: 100 },
        Change::ForgetNode { id: 1 },
        Change::ClaimTurn { node: 1, instance: 12, at_ms: 200 },
    ]);
    assert_eq!(s.turn, None);
    assert!(!s.versions.contains_key(&1));
}

#[test]
fn the_cluster_version_starts_once_every_node_reported_and_never_goes_down() {
    let first = report(1, of(0, 4), of(0, 5));
    let s = applied(std::slice::from_ref(&first));
    assert_eq!(s.cluster_version, None);
    assert_eq!(cluster_default(&s, [1, 2].into_iter(), (2, of(0, 4))), Some(of(0, 4)), "the lowest newest version");
    assert_eq!(cluster_default(&s, [1, 2, 3].into_iter(), (2, of(0, 4))), None, "node 3 has not reported");
    let s = applied(&[
        first,
        Change::ReportVersion {
            node: 2,
            release: "0.4.2".into(),
            oldest: of(0, 3),
            newest: of(0, 4),
            cluster_default: Some(of(0, 4)),
        },
    ]);
    assert_eq!(s.cluster_version, Some(of(0, 4)));
    assert_eq!(cluster_default(&s, [1, 2].into_iter(), (2, of(0, 4))), None, "set once");
    let mut changes = vec![report(1, of(0, 4), of(0, 5)), Change::SetClusterVersion { version: of(0, 5) }];
    changes.push(Change::SetClusterVersion { version: of(0, 4) });
    let s = applied(&changes);
    assert_eq!(s.cluster_version, Some(of(0, 5)), "it never goes down");
    assert_eq!(round_trip(&s).cluster_version, Some(of(0, 5)));
    assert_eq!(round_trip(&s).versions, s.versions);
}

#[test]
fn the_version_goes_up_only_to_one_every_node_runs_with() {
    let s = applied(&[
        report(1, of(0, 4), of(0, 5)),
        report(2, of(0, 4), of(0, 5)),
        report(3, of(0, 3), of(0, 4)),
        Change::SetClusterVersion { version: of(0, 4) },
    ]);
    let why = may_raise(&s, [1, 2, 3].into_iter(), of(0, 5)).expect_err("node 3 is behind");
    assert!(why.contains("node 3") && why.contains("up to 0.4"), "{why}");
    assert!(!why.contains("node 1"), "{why}");
    assert_eq!(may_raise(&s, [1, 2].into_iter(), of(0, 5)), Ok(()));
    assert!(may_raise(&s, [1, 2, 4].into_iter(), of(0, 5)).is_err_and(|w| w.contains("node 4 has not reported")));
    assert!(may_raise(&s, [1, 2].into_iter(), of(0, 3)).is_err_and(|w| w.contains("never goes down")));
    let skipped = applied(&[report(1, of(0, 6), of(0, 7)), Change::SetClusterVersion { version: of(0, 4) }]);
    assert!(may_raise(&skipped, [1].into_iter(), of(0, 5)).is_err_and(|w| w.contains("from 0.6")));
}

#[test]
fn version_zero_zero_is_a_version() {
    // A 0.0.x release runs at cluster version 0.0, which is the number 0:
    // having one must not be told by the number.
    let first = Change::ReportVersion {
        node: 1,
        release: "0.0.1".into(),
        oldest: of(0, 0),
        newest: of(0, 0),
        cluster_default: Some(of(0, 0)),
    };
    let s = applied(&[first]);
    assert_eq!(s.cluster_version, Some(0));
    let back = round_trip(&s);
    assert_eq!(back.cluster_version, Some(0));
    assert_eq!(back.versions[&1].newest, 0);
    assert_eq!(may_raise(&s, [1].into_iter(), of(0, 0)), Ok(()));
    assert_eq!(cluster_default(&s, [1].into_iter(), (1, 0)), None, "set once, to 0.0");
}

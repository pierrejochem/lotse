use bytes::BytesMut;
use lotse_record::{Batch, BatchBuilder, BatchOptions, ControlType};

use super::*;

/// One transactional record from producer `id` at `epoch`.
fn data(id: u64, epoch: u16) -> Batch {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    let mut raw = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::set_producer(&mut raw, id, epoch, 0).unwrap();
    lotse_record::set_transactional(&mut raw, true).unwrap();
    Batch::parse(&raw.freeze()).unwrap().0
}

fn marker(id: u64, epoch: u16, commit: bool, coordinator_epoch: u32) -> Batch {
    let ty = if commit { ControlType::Commit } else { ControlType::Abort };
    Batch::parse(&lotse_record::control_batch(id, epoch, ty, coordinator_epoch, 0).unwrap()).unwrap().0
}

fn apply(s: &mut TxnState, at: u64, b: &Batch) {
    s.appended(at, b);
    s.applied(at, at, b);
}

#[test]
fn an_open_transaction_holds_the_last_stable_offset_back() {
    let mut s = TxnState::default();
    assert_eq!(s.last_stable_offset(10), 10, "nothing open: everything applied is stable");
    apply(&mut s, 11, &data(7, 0));
    apply(&mut s, 12, &data(8, 0));
    assert_eq!(s.last_stable_offset(12), 10, "offset 10 (index 11) opens producer 7's transaction");
    apply(&mut s, 13, &marker(7, 1, true, 1));
    assert_eq!(s.last_stable_offset(13), 11, "only producer 8 is open now");
    apply(&mut s, 14, &marker(8, 1, false, 1));
    assert_eq!(s.last_stable_offset(14), 14);
    assert_eq!(s.aborted_between(0, 20), vec![(8, 11)], "producer 8's data from offset 11 is aborted");
    assert!(s.aborted_between(14, 20).is_empty(), "nothing aborted after the marker");
}

#[test]
fn a_late_marker_cannot_close_the_next_transaction() {
    let mut s = TxnState::default();
    apply(&mut s, 1, &data(7, 0));
    assert_eq!(s.check_marker(7, 1, 1), MarkerCheck::Append);
    apply(&mut s, 2, &marker(7, 1, true, 1));
    // The next transaction runs at the epoch of the last marker.
    apply(&mut s, 3, &data(7, 1));
    assert_eq!(s.check_marker(7, 1, 1), MarkerCheck::Done, "a retry of the old marker");
    apply(&mut s, 4, &marker(7, 1, false, 1)); // were it in the log anyway
    assert_eq!(s.last_stable_offset(4), 2, "the transaction from index 3 is still open");
    assert_eq!(s.check_marker(7, 2, 1), MarkerCheck::Append);
    assert_eq!(
        s.check_marker(9, 0, 1),
        MarkerCheck::Append,
        "a partition without data gets the epoch, to fence zombies"
    );
}

#[test]
fn markers_from_an_older_coordinator_are_refused() {
    let mut s = TxnState::default();
    apply(&mut s, 1, &data(7, 0));
    apply(&mut s, 2, &marker(7, 1, true, 5));
    apply(&mut s, 3, &data(7, 1));
    assert_eq!(s.check_marker(7, 2, 4), MarkerCheck::StaleCoordinator);
    apply(&mut s, 4, &marker(7, 2, false, 4));
    assert_eq!(s.last_stable_offset(4), 2, "a stale marker changes nothing, in any replica");
}

#[test]
fn the_tail_counts_for_checks_and_is_cut_with_the_log() {
    let mut s = TxnState::default();
    s.appended(1, &data(7, 0));
    assert!(s.has_open(7));
    s.appended(2, &marker(7, 1, true, 1));
    assert!(!s.has_open(7));
    assert_eq!(s.check_marker(7, 1, 1), MarkerCheck::Done, "the marker in flight already closed it");
    s.truncated(2);
    assert_eq!(s.check_marker(7, 1, 1), MarkerCheck::Append, "truncated: the marker is needed again");
    s.truncated(1);
    assert!(!s.has_open(7));
}

#[test]
fn the_applied_state_round_trips_and_damage_is_refused() {
    let mut s = TxnState::default();
    apply(&mut s, 1, &data(7, 0));
    apply(&mut s, 2, &data(8, 3));
    apply(&mut s, 3, &marker(8, 4, false, 2));
    let bytes = s.encode();
    let mut back = TxnState::default();
    back.restore(&bytes).unwrap();
    assert_eq!(back.encode(), bytes);
    assert_eq!(back.last_stable_offset(3), 0);
    assert_eq!(back.aborted_between(0, 3), vec![(8, 1)]);
    for cut in 0..bytes.len() {
        assert!(TxnState::default().restore(&bytes[..cut]).is_err(), "cut at {cut}");
    }
}

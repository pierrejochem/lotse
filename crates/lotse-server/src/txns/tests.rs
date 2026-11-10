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
    assert_eq!(
        s.check_marker(7, 1, 1),
        MarkerCheck::Covered,
        "the marker in flight closes it, once it commits: it may yet be cut"
    );
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

/// A queue's transactional ack from producer `id` at `epoch`.
fn ack(id: u64, epoch: u16) -> Batch {
    let mut raw = BytesMut::from(&lotse_record::ack_batch(&[(0, 1)]).unwrap()[..]);
    lotse_record::set_producer(&mut raw, id, epoch, -1).unwrap();
    lotse_record::set_transactional(&mut raw, true).unwrap();
    Batch::parse(&raw.freeze()).unwrap().0
}

#[test]
fn acks_open_a_transaction_without_holding_the_last_stable_offset_back() {
    let mut s = TxnState::default();
    apply(&mut s, 5, &ack(7, 0));
    assert!(s.has_open(7), "the marker must find it open");
    assert_eq!(s.last_stable_offset(5), 5, "acks hide nothing from readers");
    apply(&mut s, 6, &data(7, 0));
    assert_eq!(s.last_stable_offset(6), 5, "its first data does hold it back");
    apply(&mut s, 7, &marker(7, 1, false, 1));
    assert_eq!(s.aborted_between(0, 10), vec![(7, 5)], "the aborted range starts at the data");
    apply(&mut s, 8, &ack(8, 0));
    apply(&mut s, 9, &marker(8, 1, false, 1));
    assert_eq!(s.aborted_between(8, 10), vec![], "acks only: no aborted range");
    assert_eq!(s.newer_epoch(8, 0), Some(1), "an ack at the old epoch is fenced");
    assert_eq!(s.newer_epoch(8, 1), None);
    // Kept in a snapshot.
    apply(&mut s, 10, &ack(9, 3));
    let mut back = TxnState::default();
    back.restore(&s.encode()).unwrap();
    assert!(back.has_open(9));
    assert_eq!(back.last_stable_offset(10), 10);
}

/// Aborted transactions whose marker lies below the log start are forgotten
/// (ADR 0094); one whose data reaches the log start stays, so a fetch from
/// there still skips it.
#[test]
fn aborted_transactions_below_the_log_start_are_forgotten_and_no_others() {
    let mut s = TxnState::default();
    // Producer 1: data at offsets 0..2, aborted at offset 2.
    apply(&mut s, 1, &data(1, 0));
    apply(&mut s, 2, &data(1, 0));
    apply(&mut s, 3, &marker(1, 1, false, 1));
    // Producer 2: data at offsets 3 and 6, aborted at offset 7; it spans the cut.
    apply(&mut s, 4, &data(2, 0));
    apply(&mut s, 7, &data(2, 0));
    apply(&mut s, 8, &marker(2, 1, false, 1));
    // Producer 3: data at offset 8, aborted at offset 9, after the cut.
    apply(&mut s, 9, &data(3, 0));
    apply(&mut s, 10, &marker(3, 1, false, 1));
    let before_cut: Vec<_> = s.aborted_between(5, 20);
    s.forget_aborted_before(5);
    assert_eq!(s.aborted_between(0, 20), before_cut, "only what a fetch from offset 5 cannot meet is gone");
    assert_eq!(s.aborted_between(0, 20), vec![(2, 3), (3, 8)]);
    assert!(s.aborted_at(2, 6), "producer 2's data at offset 6 is still aborted");
    assert!(!s.aborted_at(1, 1), "producer 1's range is forgotten");
    let mut restored = TxnState::default();
    restored.restore(&s.encode()).unwrap();
    assert_eq!(restored.aborted_between(0, 20), vec![(2, 3), (3, 8)], "and snapshots say the same");
}

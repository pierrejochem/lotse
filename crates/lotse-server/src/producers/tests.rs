use bytes::{Bytes, BytesMut};
use lotse_record::{Batch, BatchBuilder, BatchOptions};

use super::*;

/// A batch of `records` records from producer `id`.
fn batch(id: u64, epoch: u16, base_seq: i32, records: u32) -> Batch {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(b"v"), &[]);
    }
    let mut raw = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::set_producer(&mut raw, id, epoch, base_seq).unwrap();
    Batch::parse(&raw.freeze()).unwrap().0
}

/// Appends and applies `b` at `first`.
fn commit(s: &mut ProducerState, first: u64, b: &Batch) -> u64 {
    let last = first + u64::from(b.record_count()) - 1;
    s.appended(first, last, 1, b);
    s.applied(first, last, b);
    last + 1
}

#[test]
fn sequences_continue_duplicates_are_recognised_and_gaps_refused() {
    let mut s = ProducerState::new(10);
    assert_eq!(s.check(&batch(7, 0, 0, 2)), Check::New);
    let next = commit(&mut s, 1, &batch(7, 0, 0, 2));
    assert_eq!(s.check(&batch(7, 0, 0, 2)), Check::Duplicate { first: 1, last: 2, term: None });
    assert_eq!(s.check(&batch(7, 0, 2, 1)), Check::New);
    assert_eq!(s.check(&batch(7, 0, 3, 1)), Check::OutOfOrder { expected: 2 }, "a gap");
    assert_eq!(s.check(&batch(7, 0, 0, 1)), Check::OutOfOrder { expected: 2 }, "overlaps, but not a whole batch");
    commit(&mut s, next, &batch(7, 0, 2, 1));
    assert_eq!(s.check(&batch(9, 0, 5, 1)), Check::New, "an unknown producer is new, whatever its sequence");
    assert_eq!(s.check(&batch(0, 0, -1, 1)), Check::New, "not idempotent");
}

#[test]
fn epochs_fence_older_producers_and_restart_sequences() {
    let mut s = ProducerState::new(10);
    let next = commit(&mut s, 1, &batch(7, 2, 0, 1));
    assert_eq!(s.check(&batch(7, 1, 1, 1)), Check::Fenced { current: 2 });
    assert_eq!(s.check(&batch(7, 3, 4, 1)), Check::OutOfOrder { expected: 0 });
    assert_eq!(s.check(&batch(7, 3, 0, 1)), Check::New);
    commit(&mut s, next, &batch(7, 3, 0, 1));
    assert_eq!(s.check(&batch(7, 2, 1, 1)), Check::Fenced { current: 3 });
}

#[test]
fn only_the_last_five_batches_are_remembered() {
    let mut s = ProducerState::new(10);
    let mut first = 1;
    for seq in 0..7 {
        first = commit(&mut s, first, &batch(7, 0, seq, 1));
    }
    assert!(matches!(s.check(&batch(7, 0, 6, 1)), Check::Duplicate { .. }));
    assert!(matches!(s.check(&batch(7, 0, 2, 1)), Check::Duplicate { .. }));
    assert_eq!(s.check(&batch(7, 0, 1, 1)), Check::OutOfOrder { expected: 7 }, "older than the window");
}

#[test]
fn the_tail_counts_and_is_cut_with_the_log() {
    let mut s = ProducerState::new(10);
    let next = commit(&mut s, 1, &batch(7, 0, 0, 1));
    // Appended, not applied: a retry is a duplicate of the entry in flight.
    s.appended(next, next, 4, &batch(7, 0, 1, 1));
    assert_eq!(s.check(&batch(7, 0, 1, 1)), Check::Duplicate { first: next, last: next, term: Some(4) });
    assert_eq!(s.check(&batch(7, 0, 2, 1)), Check::New, "continues the tail");
    // Raft truncates it: the retry is new again.
    s.truncated(next);
    assert_eq!(s.check(&batch(7, 0, 1, 1)), Check::New);
    // Applying a later index drops the tail up to it.
    s.appended(next, next, 5, &batch(7, 0, 1, 1));
    s.applied(next, next, &batch(7, 0, 1, 1));
    assert_eq!(s.check(&batch(7, 0, 1, 1)), Check::Duplicate { first: next, last: next, term: None });
}

#[test]
fn the_producer_with_the_oldest_write_is_evicted() {
    let mut s = ProducerState::new(2);
    let mut first = commit(&mut s, 1, &batch(1, 0, 0, 1));
    first = commit(&mut s, first, &batch(2, 0, 0, 1));
    first = commit(&mut s, first, &batch(1, 0, 1, 1)); // producer 1 writes again
    commit(&mut s, first, &batch(3, 0, 0, 1)); // one too many: 2 wrote last longest ago
    assert_eq!(s.len(), 2);
    assert!(matches!(s.check(&batch(1, 0, 1, 1)), Check::Duplicate { .. }));
    assert!(matches!(s.check(&batch(3, 0, 0, 1)), Check::Duplicate { .. }));
    assert_eq!(s.check(&batch(2, 0, 0, 1)), Check::New, "evicted: its retry can no longer be recognised");
}

#[test]
fn the_committed_state_round_trips_and_damage_is_refused() {
    let mut s = ProducerState::new(10);
    let mut first = 1;
    for (id, seq) in [(1, 0), (2, 0), (1, 1)] {
        first = commit(&mut s, first, &batch(id, 3, seq, 2));
    }
    s.appended(first, first, 9, &batch(1, 3, 3, 1));
    let bytes = s.encode();
    let mut back = ProducerState::new(10);
    back.restore(&bytes).unwrap();
    assert_eq!(back.committed, s.committed);
    assert_eq!(back.by_last_write, s.by_last_write);
    assert!(back.tail.is_empty(), "the tail is rebuilt from the log, not the snapshot");
    for cut in 0..bytes.len() {
        assert!(ProducerState::new(10).restore(&bytes[..cut]).is_err(), "cut at {cut}");
    }
    let mut longer = bytes.to_vec();
    longer.push(0);
    assert!(ProducerState::new(10).restore(&longer).is_err());
    assert!(ProducerState::new(10).restore(&Bytes::from_static(&[0, 0, 0, 0])).is_ok(), "empty");
}

#[test]
fn a_marker_raises_the_epoch_and_restarts_the_sequence() {
    let mut s = ProducerState::new(10);
    let next = commit(&mut s, 1, &batch(7, 0, 0, 2));
    let m =
        Batch::parse(&lotse_record::control_batch(7, 1, lotse_record::ControlType::Commit, 1, 0).unwrap()).unwrap().0;
    s.appended(next, next, 1, &m);
    assert_eq!(s.check(&batch(7, 0, 2, 1)), Check::Fenced { current: 1 }, "the old epoch is over");
    assert_eq!(s.check(&batch(7, 1, 0, 1)), Check::New, "the new epoch starts at 0");
    s.applied(next, next, &m);
    assert_eq!(s.check(&batch(7, 1, 2, 1)), Check::OutOfOrder { expected: 0 });
    let bytes = s.encode();
    let mut back = ProducerState::new(10);
    back.restore(&bytes).unwrap();
    assert_eq!(back.committed, s.committed);
}

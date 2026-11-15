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

/// Appends and applies an `ExpireProducers` batch naming `producers` at
/// `first`.
fn expire(s: &mut ProducerState, first: u64, producers: &[(u64, u64)]) -> u64 {
    expire_raw(s, first, &encode_expired(producers))
}

fn expire_raw(s: &mut ProducerState, first: u64, payload: &[u8]) -> u64 {
    let raw = lotse_record::internal_control_batch(InternalControl::ExpireProducers, payload).unwrap();
    let b = Batch::parse(&raw).unwrap().0;
    s.appended(first, first, 1, &b);
    s.applied(first, first, &b);
    first + 1
}

fn at(secs: u64) -> Instant {
    Instant::from_nanos(secs * 1_000_000_000)
}

const DAY: Duration = Duration::from_secs(24 * 3600);

#[test]
fn a_full_partition_refuses_new_producers_and_keeps_the_live_ones() {
    let mut s = ProducerState::new(2);
    let mut first = commit(&mut s, 1, &batch(1, 0, 0, 1));
    first = commit(&mut s, first, &batch(2, 0, 0, 1));
    assert_eq!(s.check(&batch(3, 0, 0, 1)), Check::Full, "a third producer waits");
    assert!(matches!(s.check(&batch(1, 0, 0, 1)), Check::Duplicate { .. }), "the first keeps its place");
    assert!(matches!(s.check(&batch(2, 0, 0, 1)), Check::Duplicate { .. }));
    assert_eq!(s.check(&batch(1, 0, 1, 1)), Check::New, "a known producer goes on");
    assert_eq!(s.check(&batch(0, 0, -1, 1)), Check::New, "not idempotent: no limit");
    // Whatever reaches the log anyway (a marker for a transaction begun
    // before) is applied: applying never drops a producer to make room.
    commit(&mut s, first, &batch(3, 0, 0, 1));
    assert_eq!(s.len(), 3);

    // A producer only in the tail counts too.
    let mut s = ProducerState::new(2);
    let next = commit(&mut s, 1, &batch(1, 0, 0, 1));
    s.appended(next, next, 1, &batch(2, 0, 0, 1));
    assert_eq!(s.check(&batch(3, 0, 0, 1)), Check::Full);
    assert_eq!(s.check(&batch(2, 0, 1, 1)), Check::New, "continues the tail");
}

#[test]
fn producers_idle_for_the_expiration_are_named_once_and_forgotten() {
    let mut s = ProducerState::new(2);
    let mut first = commit(&mut s, 1, &batch(1, 0, 0, 1));
    first = commit(&mut s, first, &batch(2, 0, 0, 1));
    let mut clock = IdleClock::default();
    clock.for_term(1);
    assert!(s.idle(&mut clock, at(0), DAY, |_| false).is_empty(), "first seen now");
    assert!(s.idle(&mut clock, at(3600), DAY, |_| false).is_empty());
    // Producer 1 writes again: its idle time starts over.
    first = commit(&mut s, first, &batch(1, 0, 1, 1));
    assert!(s.idle(&mut clock, at(7200), DAY, |_| false).is_empty());
    let idle = s.idle(&mut clock, at(24 * 3600), DAY, |_| false);
    assert_eq!(idle, vec![(2, 2)], "only producer 2 has been quiet a day");
    assert!(s.idle(&mut clock, at(24 * 3600 + 1), DAY, |_| false).is_empty(), "named once while on its way");
    expire(&mut s, first, &idle);
    assert_eq!(s.len(), 1);
    assert_eq!(s.check(&batch(2, 0, 0, 1)), Check::New, "forgotten");
    assert_eq!(s.check(&batch(3, 0, 0, 1)), Check::New, "room for a new producer");
    assert!(matches!(s.check(&batch(1, 0, 1, 1)), Check::Duplicate { .. }), "the live one stays");
}

#[test]
fn a_producer_that_wrote_after_it_was_named_stays() {
    let mut s = ProducerState::new(10);
    let mut first = commit(&mut s, 1, &batch(1, 0, 0, 1));
    let named = [(1, 1)];
    // Its next batch reaches the log before the expiry does.
    first = commit(&mut s, first, &batch(1, 0, 1, 1));
    expire(&mut s, first, &named);
    assert_eq!(s.len(), 1);
    assert!(matches!(s.check(&batch(1, 0, 1, 1)), Check::Duplicate { .. }), "its retry is still recognised");
}

#[test]
fn an_open_transaction_and_a_new_term_keep_a_producer() {
    let mut s = ProducerState::new(10);
    commit(&mut s, 1, &batch(1, 0, 0, 1));
    commit(&mut s, 2, &batch(2, 0, 0, 1));
    let mut clock = IdleClock::default();
    clock.for_term(1);
    s.idle(&mut clock, at(0), DAY, |_| false);
    assert_eq!(s.idle(&mut clock, at(24 * 3600), DAY, |id| id == 1), vec![(2, 2)], "1 has a transaction open");
    // A new leader counts from when it took over.
    let mut clock = IdleClock::default();
    clock.for_term(1);
    s.idle(&mut clock, at(0), DAY, |_| false);
    clock.for_term(2);
    assert!(s.idle(&mut clock, at(24 * 3600), DAY, |_| false).is_empty(), "the new term saw them first now");
    assert_eq!(s.idle(&mut clock, at(48 * 3600), DAY, |_| false).len(), 2);
}

#[test]
fn a_damaged_expiry_forgets_nothing() {
    let mut s = ProducerState::new(10);
    let first = commit(&mut s, 1, &batch(1, 0, 0, 1));
    let mut payload = encode_expired(&[(1, 1)]).to_vec();
    payload.pop();
    expire_raw(&mut s, first, &payload);
    assert_eq!(s.len(), 1);
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

#[test]
fn a_transactional_ack_leaves_the_producers_sequences_alone() {
    let mut s = ProducerState::new(10);
    let next = commit(&mut s, 1, &batch(7, 0, 0, 2));
    let mut raw = BytesMut::from(&lotse_record::ack_batch(&[(0, 2)]).unwrap()[..]);
    lotse_record::set_producer(&mut raw, 7, 0, -1).unwrap();
    lotse_record::set_transactional(&mut raw, true).unwrap();
    let ack = Batch::parse(&raw.freeze()).unwrap().0;
    let next = commit(&mut s, next, &ack);
    assert_eq!(s.check(&batch(7, 0, 2, 1)), Check::New, "the ack took no sequence: the next data follows on");
    commit(&mut s, next, &batch(7, 0, 2, 1));
}

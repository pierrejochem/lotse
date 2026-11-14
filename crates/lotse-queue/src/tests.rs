use std::collections::BTreeSet;
use std::time::Duration;

use bytes::BytesMut;
use lotse_record::{Batch, BatchBuilder, BatchOptions, ControlType};
use lotse_rt::Instant;

use super::*;

/// A data batch of `n` messages at `base`, optionally transactional.
fn data(base: u64, n: u64, producer: Option<u64>) -> Batch {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..n {
        b.push(0, None, Some(b"m"), &[]);
    }
    let mut raw = BytesMut::from(&b.finish().unwrap()[..]);
    if let Some(p) = producer {
        lotse_record::set_producer(&mut raw, p, 0, 0).unwrap();
        lotse_record::set_transactional(&mut raw, true).unwrap();
    }
    lotse_record::stamp(&mut raw, base, 1).unwrap();
    Batch::parse(&raw.freeze()).unwrap().0
}

fn stamped(raw: bytes::Bytes, base: u64) -> Batch {
    let mut raw = BytesMut::from(&raw[..]);
    lotse_record::stamp(&mut raw, base, 1).unwrap();
    Batch::parse(&raw.freeze()).unwrap().0
}

fn ack(base: u64, ranges: &[(u64, u64)]) -> Batch {
    stamped(lotse_record::ack_batch(ranges).unwrap(), base)
}

fn marker(base: u64, producer: u64, commit: bool) -> Batch {
    let ty = if commit { ControlType::Commit } else { ControlType::Abort };
    stamped(lotse_record::control_batch(producer, 1, ty, 1, 0).unwrap(), base)
}

#[test]
fn ranges_merge_and_find_gaps() {
    let mut s = RangeSet::new();
    s.insert(5, 7);
    s.insert(1, 3);
    s.insert(3, 5); // joins both
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![(1, 7)]);
    s.insert(10, 12);
    s.insert(0, 1);
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![(0, 7), (10, 12)]);
    assert!(s.contains(6) && !s.contains(7) && s.contains(11));
    assert_eq!((s.next_gap(0), s.next_gap(8), s.next_gap(10)), (7, 8, 12));
    s.insert(2, 11);
    assert_eq!(s.iter().collect::<Vec<_>>(), vec![(0, 12)]);
}

#[test]
fn acks_settle_and_the_floor_moves_over_non_messages() {
    let mut q = QueueLog::new();
    q.apply(&stamped(lotse_record::raft_control_batch(lotse_record::RaftControl::Noop, &[]).unwrap(), 0)).unwrap();
    q.apply(&data(1, 3, None)).unwrap(); // messages 1, 2, 3
    assert_eq!(q.floor(), 1, "the no-op at 0 is not a message");
    q.apply(&ack(4, &[(1, 2), (3, 4)])).unwrap();
    assert_eq!(q.floor(), 2, "1 is acked; 2 is not");
    assert!(q.is_settled(3) && !q.is_settled(2) && q.is_settled(4), "the ack batch itself is settled");
    q.apply(&ack(5, &[(2, 3)])).unwrap();
    assert_eq!(q.floor(), 6);
}

#[test]
fn transactional_messages_wait_for_their_marker() {
    let mut q = QueueLog::new();
    q.apply(&data(0, 2, Some(7))).unwrap();
    q.apply(&data(2, 1, Some(8))).unwrap();
    q.apply(&marker(3, 7, false)).unwrap();
    assert!(q.is_settled(0) && q.is_settled(1), "aborted: never delivered");
    q.apply(&marker(4, 8, true)).unwrap();
    assert!(!q.is_settled(2), "committed: deliverable");
    assert_eq!(q.floor(), 2);
}

#[test]
fn delivery_counts_persist_until_the_message_is_settled() {
    let mut q = QueueLog::new();
    q.apply(&data(0, 2, None)).unwrap();
    q.apply(&stamped(lotse_record::queue_state_batch(&[(0, 2), (1, 1)]).unwrap(), 2)).unwrap();
    assert_eq!((q.delivery_count(0), q.delivery_count(1)), (2, 1));
    q.apply(&ack(3, &[(0, 1)])).unwrap();
    assert_eq!(q.delivery_count(0), 0, "settled: forgotten");
    let bytes = q.encode();
    assert_eq!(QueueLog::decode(bytes.clone()).unwrap(), q);
    for cut in 0..bytes.len() {
        assert!(QueueLog::decode(bytes.slice(..cut)).is_err(), "cut at {cut}");
    }
}

fn at(ms: u64) -> Instant {
    Instant::from_nanos(ms * 1_000_000)
}

/// Queue with messages 0..n.
/// Writes the CHECKOUT batches the leader writes for what `assign` handed
/// out (ADR 0109), each subscription's consumer its id, from `base` on.
fn check_out(q: &mut QueueLog, got: &[(SubscriptionId, Vec<Delivery>)], base: u64) {
    for (i, (sub, deliveries)) in got.iter().enumerate() {
        let d: Vec<(u64, u16)> = deliveries.iter().map(|x| (x.offset, x.delivery_count)).collect();
        q.apply(&checkout(base + i as u64, u64::from(*sub), 0, &d)).unwrap();
    }
}

fn queue(n: u64) -> QueueLog {
    let mut q = QueueLog::new();
    q.apply(&data(0, n, None)).unwrap();
    q
}

#[test]
fn credit_limits_deliveries_and_released_messages_go_first() {
    let mut q = queue(6);
    let mut d = Dispatcher::new(&q);
    let s = 1;
    d.subscribe(s, 1, 0, 2, 1 << 20);
    let c = d.candidates(&q, 6, d.open_credit() as usize, &Default::default());
    assert_eq!(c, vec![0, 1], "two credits, two messages");
    let got = d.assign(&q, &c, 100, at(0));
    assert_eq!(
        got,
        vec![(s, vec![Delivery { offset: 0, delivery_count: 1 }, Delivery { offset: 1, delivery_count: 1 }])]
    );
    assert!(d.candidates(&q, 6, d.open_credit() as usize, &Default::default()).is_empty(), "no credit left");
    d.credit(s, 3, 1 << 20);
    d.release([0]);
    let c = d.candidates(&q, 6, d.open_credit() as usize, &Default::default());
    assert_eq!(c, vec![0, 2, 3], "the released message first, in its place");
    d.assign(&q, &c, 100, at(1));
    // Acked: nothing of it is delivered again.
    q.apply(&ack(6, &[(0, 4)])).unwrap();
    d.sweep(&q);
    d.credit(s, 10, 1 << 20);
    assert_eq!(d.candidates(&q, 7, d.open_credit() as usize, &Default::default()), vec![4, 5]);
    assert!(d.candidates(&q, 5, 10, &Default::default()).iter().all(|o| *o < 5), "never at or beyond the limit");
}

#[test]
fn a_disconnect_releases_and_counts_the_redelivery() {
    let mut q = queue(2);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10, &Default::default());
    let got = d.assign(&q, &c, 100, at(0));
    check_out(&mut q, &got, 100);
    d.disconnect(1);
    let s2 = 2;
    d.subscribe(s2, 2, 0, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10, &Default::default());
    assert_eq!(c, vec![0, 1]);
    let got = d.assign(&q, &c, 100, at(1));
    assert_eq!(got[0].0, s2);
    assert!(got[0].1.iter().all(|x| x.delivery_count == 2), "second delivery");
}

#[test]
fn the_ack_timeout_ends_a_stuck_subscription() {
    let mut q = queue(3);
    let mut d = Dispatcher::new(&q);
    let stuck = 1;
    d.subscribe(stuck, 1, 0, 2, 1 << 20);
    let c = d.candidates(&q, 3, 2, &Default::default());
    let got = d.assign(&q, &c, 100, at(0));
    check_out(&mut q, &got, 100);
    assert!(d.timed_out(at(999), Duration::from_secs(1)).is_empty());
    let ended = d.timed_out(at(1_000), Duration::from_secs(1));
    assert_eq!(ended, vec![(stuck, vec![0, 1])]);
    assert_eq!(d.count_to_persist(&q, 0), 1, "the first delivery, which its checkout wrote");
    let other = 2;
    d.subscribe(other, 2, 0, 10, 1 << 20);
    assert_eq!(d.candidates(&q, 3, 10, &Default::default()), vec![2], "held back until their counts are persisted");
    d.release([0, 1]);
    let c = d.candidates(&q, 3, 10, &Default::default());
    assert_eq!(c, vec![0, 1, 2]);
    assert_eq!(d.assign(&q, &c, 100, at(1_001))[0].0, other);
}

#[test]
fn an_unsubscribed_consumer_keeps_its_messages_until_it_acks() {
    let mut q = queue(2);
    let mut d = Dispatcher::new(&q);
    let s = 1;
    d.subscribe(s, 1, 0, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10, &Default::default());
    d.assign(&q, &c, 100, at(0));
    d.unsubscribe(s);
    assert!(d.owns(1, s), "still holds its messages");
    assert!(d.candidates(&q, 2, d.open_credit() as usize, &Default::default()).is_empty());
    q.apply(&ack(2, &[(0, 2)])).unwrap();
    d.sweep(&q);
    assert!(!d.owns(1, s), "gone once everything is acked");
}

/// What a closed consumer leaves in flight is not handed to the next
/// subscription: only an ack, a nack from its connection, the connection
/// closing, the ack timeout or a new leader gives it back.
#[test]
fn a_new_subscription_does_not_get_what_an_unsubscribed_one_holds() {
    let q = queue(3);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 10, 0, 1, 1 << 20);
    let c = d.candidates(&q, 3, 10, &Default::default());
    let held = d.assign(&q, &c, 100, at(0));
    assert_eq!(held[0].1.len(), 1, "credit for one");
    d.unsubscribe(1);
    d.subscribe(2, 20, 0, 10, 1 << 20);
    let c = d.candidates(&q, 3, 10, &Default::default());
    let got = d.assign(&q, &c, 100, at(1));
    let offsets: Vec<u64> = got.iter().flat_map(|(_, m)| m.iter().map(|x| x.offset)).collect();
    assert_eq!(offsets, vec![1, 2], "everything but the message the first holds");
}

#[test]
fn a_nack_takes_back_only_the_connections_own_messages() {
    let mut q = queue(4);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 10, 0, 2, 1 << 20);
    d.subscribe(2, 20, 0, 2, 1 << 20);
    let c = d.candidates(&q, 4, 4, &Default::default());
    let got = d.assign(&q, &c, 100, at(0));
    check_out(&mut q, &got, 100);
    let theirs: Vec<u64> = got.iter().find(|(id, _)| *id == 2).unwrap().1.iter().map(|x| x.offset).collect();
    let mine: Vec<u64> = got.iter().find(|(id, _)| *id == 1).unwrap().1.iter().map(|x| x.offset).collect();
    assert_eq!(d.take(10, theirs.iter().copied()), Vec::<u64>::new(), "not this connection's");
    assert_eq!(d.take(10, mine.iter().copied()), mine);
    assert_eq!(d.count_to_persist(&q, mine[0]), 1);
    q.apply(&stamped(lotse_record::queue_state_batch(&[(mine[0], 1), (mine[1], 1)]).unwrap(), 4)).unwrap();
    d.release(mine.iter().copied());
    d.credit(2, 2, 1 << 20);
    let c = d.candidates(&q, 4, 2, &Default::default());
    assert_eq!(c, mine);
    let again = d.assign(&q, &c, 100, at(1));
    assert!(again.iter().flat_map(|(_, x)| x).all(|x| x.delivery_count == 2), "{again:?}");
}

#[test]
fn counts_of_settled_and_waiting_messages() {
    let mut s = RangeSet::new();
    s.insert(2, 5);
    s.insert(8, 10);
    assert_eq!(s.count_in(0, 20), 5);
    assert_eq!(s.count_in(3, 9), 3, "part of each range");
    assert_eq!(s.count_in(5, 8), 0, "a gap");
    assert_eq!(s.count_in(4, 4), 0);

    // Ten messages, three acknowledged, the ack batch itself settled; then
    // a transaction of four that waits for its marker.
    let mut q = QueueLog::new();
    q.apply(&data(0, 10, None)).unwrap();
    q.apply(&ack(10, &[(0, 2), (5, 6)])).unwrap();
    q.apply(&data(11, 4, Some(7))).unwrap();
    assert_eq!(q.unsettled_in(0, 15), 7 + 4);
    assert_eq!(q.in_open_transactions(), 4);
    q.apply(&marker(15, 7, false)).unwrap();
    assert_eq!((q.unsettled_in(0, 16), q.in_open_transactions()), (7, 0), "aborted: settled");
}

use lotse_record::dead_letter::Reason;

fn retire_out(base: u64, target: u64, entries: &[(u64, Reason, u16)]) -> Batch {
    stamped(lotse_record::retire_out_batch(target, entries).unwrap(), base)
}

fn retire_done(base: u64, offsets: &[u64]) -> Batch {
    stamped(lotse_record::retire_done_batch(offsets).unwrap(), base)
}

#[test]
fn a_claim_settles_a_message_and_owes_it_until_it_is_forwarded() {
    let mut q = queue(3);
    q.apply(&retire_out(3, 77, &[(1, Reason::MaxDeliveries, 5)])).unwrap();
    assert!(q.is_settled(1) && !q.is_settled(0), "the claim settles the message, not its neighbours");
    assert_eq!(
        q.owed().collect::<Vec<_>>(),
        vec![(1, Owed { reason: Reason::MaxDeliveries, deliveries: 5, target: 77 })]
    );
    assert_eq!(q.floor(), 0);
    q.apply(&ack(4, &[(0, 1)])).unwrap();
    assert_eq!(q.floor(), 2, "settled up to the next message");
    assert_eq!(q.retention_floor(), 1, "but the owed message's data stays in the log");
    q.apply(&retire_done(5, &[1])).unwrap();
    assert_eq!(q.owed().count(), 0);
    assert_eq!(q.retention_floor(), 2, "forwarded: nothing holds the log back");
}

#[test]
fn an_ack_before_a_claim_wins_and_after_it_changes_nothing() {
    // Acked first: the claim finds the message settled and owes nothing.
    let mut q = queue(2);
    q.apply(&ack(2, &[(0, 1)])).unwrap();
    q.apply(&retire_out(3, 77, &[(0, Reason::Expired, 0)])).unwrap();
    assert_eq!(q.owed().count(), 0, "a message acknowledged before the claim is not dead-lettered");

    // Claimed first: a late ack is harmless, the debt stays.
    q.apply(&retire_out(4, 77, &[(1, Reason::Expired, 0)])).unwrap();
    q.apply(&ack(5, &[(1, 2)])).unwrap();
    assert_eq!(q.owed().map(|(o, _)| o).collect::<Vec<_>>(), vec![1]);
    // A claim applied twice owes once.
    q.apply(&retire_out(6, 77, &[(1, Reason::Expired, 0)])).unwrap();
    assert_eq!(q.owed().count(), 1);
}

#[test]
fn a_claim_drops_the_persisted_count() {
    let mut q = queue(1);
    q.apply(&stamped(lotse_record::queue_state_batch(&[(0, 3)]).unwrap(), 1)).unwrap();
    assert_eq!(q.delivered_at_least(3).collect::<Vec<_>>(), vec![(0, 3)]);
    q.apply(&retire_out(2, 77, &[(0, Reason::MaxDeliveries, 3)])).unwrap();
    assert_eq!(q.delivered_at_least(1).count(), 0);
}

/// A dead-letter batch at `base` holding one message from `(queue, offset)`
/// of a source whose done-floor is `floor`.
fn dead_letter(base: u64, source: u64, offset: u64, floor: u64) -> Batch {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    let (src, _) = Batch::parse(&b.finish().unwrap()).unwrap();
    let info = lotse_record::DeadLetterInfo {
        queue: "src",
        queue_id: source,
        offset,
        floor,
        reason: Reason::Expired,
        deliveries: 0,
    };
    let item = lotse_record::dead_letter_item(&src.records().unwrap()[0], info).unwrap();
    let joined = lotse_record::dead_letter_batch(&[Batch::parse(&item).unwrap().0], 5).unwrap();
    stamped(joined, base)
}

#[test]
fn a_dead_letter_taken_twice_is_delivered_once() {
    let mut q = QueueLog::new();
    q.apply(&dead_letter(0, 9, 40, 40)).unwrap();
    assert!(!q.is_settled(0), "a dead letter is a message");
    q.apply(&dead_letter(1, 9, 40, 40)).unwrap();
    assert!(q.is_settled(1), "the same message from the same source is not delivered again");
    q.apply(&dead_letter(2, 10, 40, 40)).unwrap();
    assert!(!q.is_settled(2), "another source's offset 40 is another message");
    q.apply(&dead_letter(3, 9, 41, 40)).unwrap();
    assert!(!q.is_settled(3));
}

#[test]
fn what_the_source_has_finished_is_forgotten_and_not_taken_again() {
    let mut q = QueueLog::new();
    q.apply(&dead_letter(0, 9, 40, 40)).unwrap();
    assert_eq!(q.taken_from(9), 1);
    // The source says everything below 50 is done: no need to remember 40.
    q.apply(&dead_letter(1, 9, 55, 50)).unwrap();
    assert_eq!(q.taken_from(9), 1, "only 55 is kept");
    // A late copy of 40 from a deposed leader is recognised by the floor.
    q.apply(&dead_letter(2, 9, 40, 40)).unwrap();
    assert!(q.is_settled(2), "below the source's floor: already handled there");
    let bytes = q.encode();
    assert_eq!(QueueLog::decode(bytes.clone()).unwrap(), q);
    for cut in 0..bytes.len() {
        assert!(QueueLog::decode(bytes.slice(..cut)).is_err(), "cut at {cut}");
    }
}

#[test]
fn exhausted_messages_are_those_delivered_as_often_as_allowed_and_not_with_a_consumer() {
    let mut q = queue(5);
    // 0 and 1 were nacked three times, 2 twice; 3 is in flight with a count of 3.
    q.apply(&stamped(lotse_record::queue_state_batch(&[(0, 3), (1, 3), (2, 2), (3, 3)]).unwrap(), 5)).unwrap();
    let limits = Limits { max_deliveries: 3, message_ttl_ms: 0 };
    let none = Default::default();
    assert_eq!(exhausted(&q, None, limits, &none, 10), vec![(0, 3), (1, 3), (3, 3)]);
    assert_eq!(exhausted(&q, None, limits, &[1].into(), 10), vec![(0, 3), (3, 3)], "claims in flight are skipped");
    assert_eq!(exhausted(&q, None, limits, &none, 1), vec![(0, 3)], "at most the asked number");
    assert!(exhausted(&q, None, Limits::default(), &none, 10).is_empty(), "off by default");

    let mut d = Dispatcher::new(&q);
    d.adopt(&q, at(0), 0);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    let c = d.candidates(&q, 5, 10, &[0, 1].into());
    assert_eq!(c, vec![2, 3, 4], "retiring messages are not delivered");
    let got = d.assign(&q, &c, 10, at(0));
    check_out(&mut q, &got, 100);
    assert_eq!(exhausted(&q, Some(&d), limits, &none, 10), vec![(0, 3), (1, 3)], "message 3 is with a consumer");
    // The delivery that ended with the connection was counted by its checkout.
    d.disconnect(1);
    q.apply(&ack(6, &[(0, 1)])).unwrap();
    let got = exhausted(&q, Some(&d), limits, &none, 10);
    assert_eq!(got, vec![(1, 3), (2, 3), (3, 4)], "2 had two and one more that ended with the connection: {got:?}");
}

#[test]
fn a_message_expires_exactly_when_its_ttl_has_passed() {
    let l = Limits { max_deliveries: 0, message_ttl_ms: 300 };
    assert!(!l.expired(1_000, 1_299));
    assert!(l.expired(1_000, 1_300));
    assert!(!l.expired(5_000, 1_000), "a message from the future is young");
    assert_eq!(l.expires_at(1_000), 1_300);
    assert!(!Limits::default().expired(0, u64::MAX / 2), "no TTL, no expiry");
    assert!(!Limits { max_deliveries: 3, message_ttl_ms: 0 }.exhausted(2));
    assert!(Limits { max_deliveries: 3, message_ttl_ms: 0 }.exhausted(3));
    assert!(!Limits::default().exhausted(60_000));
}

/// A transactional ack of `producer` at `epoch`.
fn txn_ack(base: u64, producer: u64, epoch: u16, ranges: &[(u64, u64)]) -> Batch {
    let mut raw = BytesMut::from(&lotse_record::ack_batch(ranges).unwrap()[..]);
    lotse_record::set_producer(&mut raw, producer, epoch, -1).unwrap();
    lotse_record::set_transactional(&mut raw, true).unwrap();
    stamped(raw.freeze(), base)
}

#[test]
fn a_transactional_ack_holds_until_the_marker_settles_or_releases() {
    let mut q = queue(4);
    q.apply(&txn_ack(4, 7, 0, &[(0, 2)])).unwrap();
    q.apply(&txn_ack(5, 8, 0, &[(2, 3)])).unwrap();
    assert_eq!((q.held_by(0), q.held_by(1), q.held_by(2), q.held_by(3)), (Some(7), Some(7), Some(8), None));
    assert!(!q.is_settled(0) && q.is_settled(4), "held, not settled; the ack batch is no message");
    assert_eq!(q.held(), 3);
    q.apply(&marker(6, 7, true)).unwrap();
    assert!(q.is_settled(0) && q.is_settled(1), "committed: settled");
    q.apply(&marker(7, 8, false)).unwrap();
    assert!(!q.is_settled(2) && q.held_by(2).is_none(), "aborted: deliverable again");
    assert_eq!(q.held(), 0);
}

#[test]
fn of_two_transactions_the_first_ack_in_the_log_holds_a_message() {
    let mut q = queue(3);
    q.apply(&ack(3, &[(2, 3)])).unwrap();
    q.apply(&txn_ack(4, 7, 0, &[(0, 1)])).unwrap();
    q.apply(&txn_ack(5, 8, 0, &[(0, 3)])).unwrap();
    assert_eq!(q.held_by(0), Some(7), "held already: the second transaction does not get it");
    assert_eq!(q.held_by(1), Some(8));
    assert_eq!(q.held_by(2), None, "settled before: nobody holds it");
    q.apply(&marker(6, 8, true)).unwrap();
    assert!(!q.is_settled(0), "8's commit does not settle what 7 holds");
    // A plain ack settles a held message; the commit then changes nothing.
    q.apply(&ack(7, &[(0, 1)])).unwrap();
    assert!(q.is_settled(0) && q.held_by(0).is_none());
    q.apply(&marker(8, 7, false)).unwrap();
    assert!(q.is_settled(0), "an abort does not take back a plain ack");
}

#[test]
fn a_marker_ends_only_acks_of_an_older_epoch() {
    let mut q = queue(2);
    // `marker` writes epoch 1: an ack at epoch 1 belongs to the next
    // transaction, which this marker does not end.
    q.apply(&txn_ack(2, 7, 1, &[(0, 1)])).unwrap();
    q.apply(&marker(3, 7, true)).unwrap();
    assert_eq!(q.held_by(0), Some(7), "still held");
    assert!(!q.is_settled(0));
}

#[test]
fn a_claim_leaves_a_held_message() {
    let mut q = queue(2);
    q.apply(&txn_ack(2, 7, 0, &[(0, 1)])).unwrap();
    q.apply(&retire_out(3, 99, &[(0, Reason::Expired, 1), (1, Reason::Expired, 1)])).unwrap();
    assert_eq!(q.held_by(0), Some(7), "held: not claimed");
    assert_eq!(q.owed().map(|(o, _)| o).collect::<Vec<_>>(), vec![1]);
}

#[test]
fn held_messages_survive_a_snapshot_and_old_snapshots_still_read() {
    let mut q = queue(3);
    let old = q.encode();
    assert_eq!(QueueLog::decode(old).unwrap(), q, "nothing held: the state reads as before");
    q.apply(&txn_ack(3, 7, 2, &[(0, 2)])).unwrap();
    let bytes = q.encode();
    let back = QueueLog::decode(bytes.clone()).unwrap();
    assert_eq!(back, q);
    assert_eq!(back.held_by(1), Some(7));
    let mut a = back.clone();
    a.apply(&marker(4, 7, true)).unwrap();
    assert!(!a.is_settled(0), "the epoch came along: a marker of epoch 1 does not end an ack of epoch 2");
    let short = bytes.len() - 1;
    assert!(QueueLog::decode(bytes.slice(..short)).is_err());
}

#[test]
fn a_transactional_ack_takes_only_the_connections_messages_and_an_abort_gives_them_back() {
    let mut q = queue(4);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 1, 1 << 20);
    d.subscribe(2, 2, 0, 1, 1 << 20);
    let c = d.candidates(&q, 4, 2, &Default::default());
    let got = d.assign(&q, &c, 100, at(0));
    check_out(&mut q, &got, 100);
    let of = |sub| got.iter().find(|(s, _)| *s == sub).map(|(_, v)| v[0].offset).unwrap();
    let (mine, theirs) = (of(1), of(2));
    assert!(d.take_for_txn(1, 7, &[mine, theirs]).is_none(), "one of them is another connection's");
    assert!(d.is_in_flight(mine), "nothing taken");
    assert!(d.take_for_txn(1, 7, &[3]).is_none(), "never delivered");
    assert_eq!(d.take_for_txn(1, 7, &[mine]), Some(vec![mine]));
    assert_eq!(d.take_for_txn(1, 7, &[mine]), Some(vec![]), "a retry: nothing new to take");
    assert!(d.take_for_txn(1, 8, &[mine]).is_none(), "another transaction's now");
    assert!(d.is_held(mine) && !d.is_in_flight(mine));
    // Neither a disconnect nor the ack timeout gives it back.
    d.disconnect(1);
    assert!(d.timed_out(at(10_000), Duration::from_millis(1)).iter().all(|(_, o)| !o.contains(&mine)));
    d.subscribe(3, 3, 0, 10, 1 << 20);
    assert!(!d.candidates(&q, 4, 10, &Default::default()).contains(&mine), "not ready while held");
    // Applied, held by the log; the abort gives it back with one more delivery.
    q.apply(&txn_ack(4, 7, 0, &[(mine, mine + 1)])).unwrap();
    d.held_applied(&[mine]);
    d.sweep(&q);
    assert!(d.is_held(mine));
    q.apply(&marker(5, 7, false)).unwrap();
    d.sweep(&q);
    assert!(!d.is_held(mine));
    let c = d.candidates(&q, 4, 10, &Default::default());
    assert_eq!(c.first(), Some(&mine), "delivered again, first");
    let got = d.assign(&q, &c, 100, at(20));
    assert_eq!(got[0].1[0], Delivery { offset: mine, delivery_count: 2 });
}

#[test]
fn a_new_leader_learns_what_transactions_hold_from_the_log() {
    let mut q = queue(3);
    q.apply(&txn_ack(3, 7, 0, &[(1, 2)])).unwrap();
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    let c = d.candidates(&q, 3, 10, &Default::default());
    assert_eq!(c, vec![0, 2], "held by a transaction: not delivered");
    d.assign(&q, &c, 100, at(0));
    assert!(d.is_held(1));
    q.apply(&marker(4, 7, false)).unwrap();
    d.sweep(&q);
    assert_eq!(d.candidates(&q, 3, 10, &Default::default()), vec![1], "aborted: noticed and delivered");
}

#[test]
fn a_single_active_consumer_gets_everything_and_the_next_waits_until_nothing_is_out() {
    let q = queue(6);
    let mut d = Dispatcher::new(&q);
    d.set_single_active(true);
    d.subscribe(1, 1, 0, 2, 1 << 20);
    d.subscribe(2, 2, 0, 10, 1 << 20);
    assert_eq!(d.hand_over(&q), Some(1), "the first to subscribe");
    assert_eq!(d.take_activated(), vec![1]);
    assert!(d.is_standby(2) && !d.is_standby(1));
    assert_eq!(d.open_credit(), 2, "only the active one's credit counts");
    let c = d.candidates(&q, 6, 10, &Default::default());
    let got = d.assign(&q, &c, 100, at(0));
    assert_eq!(got.len(), 1, "one subscription gets messages: {got:?}");
    assert_eq!(got[0].0, 1);
    assert_eq!(got[0].1.iter().map(|x| x.offset).collect::<Vec<_>>(), vec![0, 1]);
    // Out of credit, the active one still keeps its place.
    let c = d.candidates(&q, 6, 10, &Default::default());
    assert!(d.assign(&q, &c, 100, at(1)).is_empty(), "the standby gets nothing");

    // A nack with requeue holds one back; the active one leaves; the
    // standby waits until that message is back.
    assert_eq!(d.take(1, [1]), vec![1]);
    d.disconnect(1);
    assert_eq!(d.hand_over(&q), None, "message 1 is still out");
    assert!(d.take_activated().is_empty());
    d.release([1]);
    assert_eq!(d.hand_over(&q), Some(2));
    assert_eq!(d.take_activated(), vec![2]);
    let c = d.candidates(&q, 6, 10, &Default::default());
    let got = d.assign(&q, &c, 100, at(2));
    let offsets: Vec<u64> = got[0].1.iter().map(|x| x.offset).collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5], "what the last one had comes first, in order");
}

#[test]
fn a_standby_waits_for_timed_out_and_transaction_held_messages() {
    let mut q = queue(3);
    let mut d = Dispatcher::new(&q);
    d.set_single_active(true);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    d.subscribe(2, 2, 0, 10, 1 << 20);
    let c = d.candidates(&q, 3, 10, &Default::default());
    d.assign(&q, &c, 100, at(0));
    assert_eq!(d.take_for_txn(1, 7, &[0]), Some(vec![0]));
    let ended = d.timed_out(at(10_000), Duration::from_millis(1));
    assert_eq!(ended, vec![(1, vec![1, 2])]);
    assert_eq!(d.hand_over(&q), None, "held back for their counts, and held by a transaction");
    d.release([1, 2]);
    assert_eq!(d.hand_over(&q), None, "still held by the transaction");
    // The ack is applied and the transaction aborts: message 0 comes back.
    q.apply(&txn_ack(3, 7, 0, &[(0, 1)])).unwrap();
    d.held_applied(&[0]);
    q.apply(&marker(4, 7, false)).unwrap();
    d.sweep(&q);
    assert_eq!(d.hand_over(&q), Some(2));
    let c = d.candidates(&q, 3, 10, &Default::default());
    let got = d.assign(&q, &c, 100, at(20_000));
    assert_eq!(got[0].1.iter().map(|x| x.offset).collect::<Vec<_>>(), vec![0, 1, 2]);
}

#[test]
fn a_new_leader_waits_for_what_an_earlier_one_left_held() {
    let mut q = queue(2);
    q.apply(&txn_ack(2, 7, 0, &[(0, 1)])).unwrap();
    let mut d = Dispatcher::new(&q);
    d.set_single_active(true);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    assert_eq!(d.hand_over(&q), None, "message 0 is held by a transaction the log knows");
    let c = d.candidates(&q, 2, 10, &Default::default());
    assert!(d.assign(&q, &c, 100, at(0)).is_empty(), "nothing goes out before the handover");
    q.apply(&marker(3, 7, true)).unwrap();
    d.sweep(&q);
    assert_eq!(d.hand_over(&q), Some(1));
}

#[test]
fn turning_single_active_consumer_on_waits_for_what_is_in_flight() {
    let q = queue(4);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 1, 1 << 20);
    d.subscribe(2, 2, 0, 1, 1 << 20);
    let c = d.candidates(&q, 4, 2, &Default::default());
    assert_eq!(d.assign(&q, &c, 100, at(0)).len(), 2, "two consumers while it is off");
    d.set_single_active(true);
    assert_eq!(d.hand_over(&q), None, "both still have a message");
    d.disconnect(2);
    assert_eq!(d.hand_over(&q), None, "1 still has one");
    d.disconnect(1);
    d.subscribe(3, 3, 0, 10, 1 << 20);
    assert_eq!(d.hand_over(&q), Some(3));
    d.set_single_active(false);
    assert!(!d.is_standby(3) && d.active().is_none());
}

#[test]
fn a_standby_waits_until_the_messages_of_a_timed_out_consumer_are_back() {
    let q = queue(3);
    let mut d = Dispatcher::new(&q);
    d.set_single_active(true);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    d.subscribe(2, 2, 0, 10, 1 << 20);
    let c = d.candidates(&q, 3, 10, &Default::default());
    d.assign(&q, &c, 100, at(0));
    assert_eq!(d.timed_out(at(10_000), Duration::from_millis(1)), vec![(1, vec![0, 1, 2])]);
    assert_eq!(d.hand_over(&q), None, "held back until their counts are written");
    let c = d.candidates(&q, 3, 10, &Default::default());
    assert!(d.assign(&q, &c, 100, at(10_001)).is_empty(), "nothing goes out meanwhile");
    d.release([0, 1, 2]);
    assert_eq!(d.hand_over(&q), Some(2));
}

fn timer(base: u64, timers: &[(u64, u16, i64)]) -> Batch {
    let t: Vec<lotse_record::Timer> = timers
        .iter()
        .map(|&(offset, delivery_count, due_ms)| lotse_record::Timer { offset, delivery_count, due_ms })
        .collect();
    stamped(lotse_record::timer_batch(&t).unwrap(), base)
}

#[test]
fn a_delayed_requeue_keeps_its_count_and_time_until_the_message_settles() {
    let mut q = queue(3);
    q.apply(&timer(3, &[(0, 1, 5_000), (1, 2, 7_000)])).unwrap();
    assert!(q.is_settled(3), "a timer batch is no message");
    assert_eq!((q.due_at(0), q.due_at(1), q.due_at(2)), (Some(5_000), Some(7_000), None));
    assert_eq!(q.delivery_count(1), 2);
    // A later nack's time counts, later or earlier.
    q.apply(&timer(4, &[(1, 3, 6_000)])).unwrap();
    assert_eq!((q.due_at(1), q.delivery_count(1)), (Some(6_000), 3));
    let old = QueueLog::decode(queue(3).encode()).unwrap();
    assert_eq!(old, queue(3), "a state without times reads as before");
    let back = QueueLog::decode(q.encode()).unwrap();
    assert_eq!(back, q, "times survive a snapshot");
    q.apply(&ack(5, &[(0, 1)])).unwrap();
    assert_eq!(q.due_at(0), None, "settled: no time left");
    assert_ne!(QueueLog::decode(q.encode()).unwrap().due_at(1), None);
    let bytes = q.encode();
    assert!(QueueLog::decode(bytes.slice(..bytes.len() - 1)).is_err());
}

#[test]
fn a_deferred_message_waits_and_then_goes_first() {
    let q = queue(4);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 10, 1 << 20);
    d.defer(1, 1_000);
    let c = d.candidates(&q, 4, 10, &Default::default());
    assert_eq!(c, vec![0, 2, 3], "it waits");
    d.assign(&q, &c, 100, at(0));
    assert_eq!(d.wake(999), Some(1_000));
    assert!(d.candidates(&q, 4, 10, &Default::default()).is_empty(), "not yet");
    assert_eq!(d.wake(1_000), None);
    assert!(!d.is_deferred(1));
    assert_eq!(d.candidates(&q, 4, 10, &Default::default()), vec![1], "past the cursor, it still comes");
    // Released while it waits (another write about it decided), it still
    // waits.
    d.defer(1, 2_000);
    d.release([1]);
    assert!(d.candidates(&q, 4, 10, &Default::default()).is_empty(), "deferred wins until its time");
    d.wake(2_000);
    assert_eq!(d.candidates(&q, 4, 10, &Default::default()), vec![1]);
}

#[test]
fn a_settled_message_leaves_no_time_behind() {
    let mut q = queue(3);
    q.apply(&timer(3, &[(0, 1, 5_000), (1, 1, 5_000)])).unwrap();
    q.apply(&ack(4, &[(0, 2)])).unwrap();
    let mut plain = queue(3);
    plain.apply(&timer(3, &[])).unwrap();
    plain.apply(&ack(4, &[(0, 2)])).unwrap();
    assert_eq!(q.encode(), plain.encode(), "no time of a settled message in the snapshot");
}

#[test]
fn higher_levels_go_first_and_the_messages_they_overtake_still_come() {
    let mut q = queue(8);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 0, 3, 1 << 20);
    d.set_level(6, 2);
    d.set_level(4, 1);
    d.set_level(7, 2);
    let c = d.candidates(&q, 8, 3, &Default::default());
    assert_eq!(c, vec![6, 7, 4], "level 2 by offset, then level 1");
    let got = d.assign(&q, &c, 100, at(0));
    assert_eq!(got[0].1.iter().map(|x| x.offset).collect::<Vec<_>>(), vec![6, 7, 4]);
    d.credit(1, 10, 0);
    let c = d.candidates(&q, 8, 10, &Default::default());
    assert_eq!(c, vec![0, 1, 2, 3, 5], "level 0, all of it: the cursor stayed");
    // Released, a higher-level message goes before level 0 again.
    d.end(1);
    d.subscribe(2, 2, 0, 1, 1 << 20);
    assert_eq!(d.candidates(&q, 8, 1, &Default::default()), vec![6]);
    // Settled, it is forgotten; below the limit only.
    q.apply(&ack(8, &[(6, 7)])).unwrap();
    d.sweep(&q);
    assert_eq!(d.level_of(6), 0);
    assert_eq!(d.candidates(&q, 5, 10, &Default::default()), vec![4, 0, 1, 2, 3]);
    d.clear_levels();
    assert_eq!(d.candidates(&q, 8, 10, &Default::default()), vec![0, 1, 2, 3, 4, 5, 7]);
}

fn checkout(base: u64, consumer: u64, at_ms: i64, deliveries: &[(u64, u16)]) -> Batch {
    let mut raw = BytesMut::from(&lotse_record::checkout_batch(consumer, deliveries).unwrap()[..]);
    lotse_record::set_base_timestamp(&mut raw, at_ms).unwrap();
    stamped(raw.freeze(), base)
}

#[test]
fn a_checkout_says_who_holds_a_message_until_it_settles_or_goes_back() {
    let mut q = queue(4); // messages 0..4
    q.apply(&checkout(4, 77, 1_000, &[(0, 1), (1, 1), (2, 3)])).unwrap();
    assert!(q.is_settled(4), "a checkout batch is no message");
    assert_eq!(q.checked_out(0), Some(Checkout { consumer: 77, at_ms: 1_000 }));
    assert_eq!(q.delivery_count(2), 3, "the count survives a failover");
    assert_eq!(q.checked_out(3), None);
    // Acked: settled, out with no one.
    q.apply(&ack(5, &[(0, 1)])).unwrap();
    assert_eq!(q.checked_out(0), None);
    // Given back (a requeue, an ack timeout, a consumer gone): out with no one.
    q.apply(&stamped(lotse_record::queue_state_batch(&[(1, 2)]).unwrap(), 6)).unwrap();
    assert_eq!((q.checked_out(1), q.delivery_count(1)), (None, 2));
    q.apply(&timer(7, &[(2, 3, 9_000)])).unwrap();
    assert_eq!(q.checked_out(2), None, "a delayed requeue gives it back too");
    // Handed out again, to another consumer: the newer checkout counts.
    q.apply(&checkout(8, 77, 2_000, &[(3, 1)])).unwrap();
    q.apply(&checkout(9, 88, 3_000, &[(3, 2)])).unwrap();
    assert_eq!(q.checked_out(3), Some(Checkout { consumer: 88, at_ms: 3_000 }));
    // A transaction that holds it takes it from the consumer.
    q.apply(&txn_ack(10, 5, 1, &[(3, 4)])).unwrap();
    assert_eq!((q.held_by(3), q.checked_out(3)), (Some(5), None));
    // A checkout of what is settled or held changes nothing.
    q.apply(&checkout(11, 99, 4_000, &[(0, 2), (3, 3)])).unwrap();
    assert_eq!((q.checked_out(0), q.checked_out(3)), (None, None));
}

#[test]
fn checkouts_survive_a_snapshot() {
    let mut q = queue(3);
    let before = q.encode();
    q.apply(&checkout(3, u64::MAX, -5, &[(1, 4)])).unwrap();
    assert_eq!(QueueLog::decode(before).unwrap(), queue(3), "a state without checkouts reads as before");
    let bytes = q.encode();
    let back = QueueLog::decode(bytes.clone()).unwrap();
    assert_eq!(back, q);
    assert_eq!(back.checked_out(1), Some(Checkout { consumer: u64::MAX, at_ms: -5 }));
    assert!(QueueLog::decode(bytes.slice(..bytes.len() - 1)).is_err());
}

/// Until a new leader has adopted the log's checkouts, a checked-out
/// message is with its consumer as far as the log says: not to be retired
/// or expired (ADR 0109).
#[test]
fn a_checked_out_message_is_with_its_consumer_before_the_leader_adopts() {
    let mut q = queue(3); // messages 0..3
    q.apply(&checkout(3, 7, 1_000, &[(1, 1)])).unwrap();
    let mut d = Dispatcher::new(&q);
    assert!(Dispatcher::with_consumer(&q, Some(&d), 1), "the log says so before adopting");
    assert!(Dispatcher::with_consumer(&q, None, 1));
    assert!(!Dispatcher::with_consumer(&q, Some(&d), 0), "no one holds message 0");
    d.adopt(&q, Instant::from_nanos(1_000_000_000), 1_500);
    assert!(Dispatcher::with_consumer(&q, Some(&d), 1), "waiting for its consumer");
    let gone = d.unclaimed(
        Instant::from_nanos(1_000_000_000) + Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(600),
    );
    assert_eq!(gone, vec![1]);
    d.release([1]);
    assert!(!Dispatcher::with_consumer(&q, Some(&d), 1), "given back: the leader's view counts once adopted");
}

#[test]
fn a_new_leader_keeps_a_consumers_messages_for_it_until_it_resumes() {
    let mut q = queue(5); // messages 0..5
    q.apply(&checkout(5, 7, 1_000, &[(0, 1), (1, 1)])).unwrap();
    q.apply(&checkout(6, 8, 1_000, &[(2, 2)])).unwrap();
    let t0 = Instant::from_nanos(1_000_000_000);
    let mut d = Dispatcher::new(&q);
    d.adopt(&q, t0, 1_500);
    // Someone else subscribes first: it gets only what no one holds.
    d.subscribe(1, 10, 9, 100, u32::MAX);
    let c = d.candidates(&q, 5, 10, &BTreeSet::new());
    assert_eq!(c, vec![3, 4], "messages out with consumers 7 and 8 wait for them");
    assert!(d.take_for_txn(10, 5, &[0]).is_none(), "not the connection's");
    // Consumer 7 comes back on a new connection.
    d.subscribe(2, 20, 7, 100, u32::MAX);
    assert_eq!(d.resume(2), 2);
    assert_eq!(d.in_flight_of(2), vec![0, 1]);
    assert_eq!(d.take_for_txn(20, 5, &[0, 1]), Some(vec![0, 1]), "its new connection may ack them in a transaction");
    assert!(d.is_waiting(2) && d.is_busy(2), "consumer 8's still waits");
    // Its message waits until the resume window is over, then goes back
    // once its count is written.
    assert_eq!(d.unclaimed(t0 + Duration::from_secs(29), Duration::from_secs(30), Duration::from_secs(600)), vec![]);
    let gone = d.unclaimed(t0 + Duration::from_secs(30), Duration::from_secs(30), Duration::from_secs(600));
    assert_eq!(gone, vec![2]);
    assert_eq!(d.count_to_persist(&q, 2), 2, "its count comes from the checkout");
    assert!(!d.candidates(&q, 5, 10, &BTreeSet::new()).contains(&2), "held back until the count is written");
    d.release([2]);
    assert!(d.candidates(&q, 5, 10, &BTreeSet::new()).contains(&2));
}

#[test]
fn a_waiting_message_keeps_its_age_for_the_ack_timeout() {
    let mut q = queue(2);
    q.apply(&checkout(2, 7, 1_000, &[(0, 1)])).unwrap();
    q.apply(&checkout(3, 7, 9_000, &[(1, 1)])).unwrap();
    let t0 = Instant::from_nanos(5_000_000_000);
    let mut d = Dispatcher::new(&q);
    d.adopt(&q, t0, 10_000); // handed out 9 s and 1 s ago
    let timeout = Duration::from_secs(5);
    assert_eq!(d.unclaimed(t0, Duration::from_secs(30), timeout), vec![0], "older than the ack timeout already");
    d.subscribe(1, 10, 7, 10, u32::MAX);
    assert_eq!(d.resume(1), 1);
    assert!(d.timed_out(t0 + Duration::from_secs(3), timeout).is_empty());
    let late = d.timed_out(t0 + Duration::from_secs(4), timeout);
    assert_eq!(late, vec![(1, vec![1])], "resumed, it times out from when it was handed out");
}

#[test]
fn a_resumed_single_active_consumer_stays_active() {
    let mut q = queue(3);
    q.apply(&checkout(3, 7, 0, &[(0, 1)])).unwrap();
    let t0 = Instant::from_nanos(1);
    let mut d = Dispatcher::new(&q);
    d.set_single_active(true);
    d.adopt(&q, t0, 0);
    d.subscribe(1, 10, 9, 10, u32::MAX);
    assert_eq!(d.hand_over(&q), None, "a message is out: no handover");
    d.subscribe(2, 20, 7, 10, u32::MAX);
    d.resume(2);
    assert_eq!(d.active(), Some(2), "the consumer that holds the message was the active one");
    assert_eq!(d.take_activated(), vec![2]);
    assert!(d.is_standby(1));
}

#[test]
fn a_late_release_leaves_what_another_subscription_got_since() {
    let q = queue(1);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 10, 1, 10, u32::MAX);
    let c = d.candidates(&q, 1, 10, &BTreeSet::new());
    d.assign(&q, &c, 100, at(0));
    // It ends before its checkout is written: the message goes to another.
    d.disconnect(10);
    d.subscribe(2, 20, 2, 10, u32::MAX);
    let c = d.candidates(&q, 1, 10, &BTreeSet::new());
    assert_eq!(d.assign(&q, &c, 100, at(1))[0].0, 2);
    // The first one's checkout is decided now: nothing to give back.
    d.release_from(1, [0]);
    assert_eq!(d.in_flight_of(2), vec![0], "the second one keeps it");
    assert!(d.candidates(&q, 1, 10, &BTreeSet::new()).is_empty(), "not out again");
    d.release_from(2, [0]);
    assert_eq!(d.candidates(&q, 1, 10, &BTreeSet::new()), vec![0], "its own checkout failing gives it back");
}

#[test]
fn a_message_an_earlier_leader_handed_out_does_not_move_the_cursor_past_new_ones() {
    // Message 3 was out with consumer 7 at the old leader; 0 to 2 were not.
    let mut q = queue(4);
    q.apply(&checkout(4, 7, 0, &[(3, 1)])).unwrap();
    let t0 = Instant::from_nanos(1);
    let mut d = Dispatcher::new(&q);
    d.adopt(&q, t0, 0);
    d.subscribe(1, 10, 7, 1, u32::MAX);
    d.resume(1);
    // The consumer goes; its message goes out again, ahead of the rest.
    d.disconnect(10);
    d.subscribe(2, 20, 8, 1, u32::MAX);
    let c = d.candidates(&q, 4, 1, &BTreeSet::new());
    assert_eq!(c, vec![3], "given back first");
    d.assign(&q, &c, 100, at(1));
    d.credit(2, 3, u32::MAX);
    let c = d.candidates(&q, 4, 3, &BTreeSet::new());
    assert_eq!(c, vec![0, 1, 2], "the messages before it are still to come");
}

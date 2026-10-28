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
    d.subscribe(s, 1, 2, 1 << 20);
    let c = d.candidates(&q, 6, d.open_credit() as usize);
    assert_eq!(c, vec![0, 1], "two credits, two messages");
    let got = d.assign(&q, &c, 100, at(0));
    assert_eq!(
        got,
        vec![(s, vec![Delivery { offset: 0, delivery_count: 1 }, Delivery { offset: 1, delivery_count: 1 }])]
    );
    assert!(d.candidates(&q, 6, d.open_credit() as usize).is_empty(), "no credit left");
    d.credit(s, 3, 1 << 20);
    d.release([0]);
    let c = d.candidates(&q, 6, d.open_credit() as usize);
    assert_eq!(c, vec![0, 2, 3], "the released message first, in its place");
    d.assign(&q, &c, 100, at(1));
    // Acked: nothing of it is delivered again.
    q.apply(&ack(6, &[(0, 4)])).unwrap();
    d.sweep(&q);
    d.credit(s, 10, 1 << 20);
    assert_eq!(d.candidates(&q, 7, d.open_credit() as usize), vec![4, 5]);
    assert!(d.candidates(&q, 5, 10).iter().all(|o| *o < 5), "never at or beyond the limit");
}

#[test]
fn a_disconnect_releases_and_counts_the_redelivery() {
    let q = queue(2);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 1, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10);
    d.assign(&q, &c, 100, at(0));
    d.disconnect(1);
    let s2 = 2;
    d.subscribe(s2, 2, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10);
    assert_eq!(c, vec![0, 1]);
    let got = d.assign(&q, &c, 100, at(1));
    assert_eq!(got[0].0, s2);
    assert!(got[0].1.iter().all(|x| x.delivery_count == 2), "second delivery");
}

#[test]
fn the_ack_timeout_ends_a_stuck_subscription() {
    let q = queue(3);
    let mut d = Dispatcher::new(&q);
    let stuck = 1;
    d.subscribe(stuck, 1, 2, 1 << 20);
    let c = d.candidates(&q, 3, 2);
    d.assign(&q, &c, 100, at(0));
    assert!(d.timed_out(at(999), Duration::from_secs(1)).is_empty());
    let ended = d.timed_out(at(1_000), Duration::from_secs(1));
    assert_eq!(ended, vec![(stuck, vec![0, 1])]);
    assert_eq!(d.count_to_persist(&q, 0), 1, "persisted by the caller as the first delivery");
    let other = 2;
    d.subscribe(other, 2, 10, 1 << 20);
    assert_eq!(d.candidates(&q, 3, 10), vec![2], "held back until their counts are persisted");
    d.release([0, 1]);
    let c = d.candidates(&q, 3, 10);
    assert_eq!(c, vec![0, 1, 2]);
    assert_eq!(d.assign(&q, &c, 100, at(1_001))[0].0, other);
}

#[test]
fn an_unsubscribed_consumer_keeps_its_messages_until_it_acks() {
    let mut q = queue(2);
    let mut d = Dispatcher::new(&q);
    let s = 1;
    d.subscribe(s, 1, 10, 1 << 20);
    let c = d.candidates(&q, 2, 10);
    d.assign(&q, &c, 100, at(0));
    d.unsubscribe(s);
    assert!(d.owns(1, s), "still holds its messages");
    assert!(d.candidates(&q, 2, d.open_credit() as usize).is_empty());
    q.apply(&ack(2, &[(0, 2)])).unwrap();
    d.sweep(&q);
    assert!(!d.owns(1, s), "gone once everything is acked");
}

#[test]
fn a_nack_takes_back_only_the_connections_own_messages() {
    let mut q = queue(4);
    let mut d = Dispatcher::new(&q);
    d.subscribe(1, 10, 2, 1 << 20);
    d.subscribe(2, 20, 2, 1 << 20);
    let c = d.candidates(&q, 4, 4);
    let got = d.assign(&q, &c, 100, at(0));
    let theirs: Vec<u64> = got.iter().find(|(id, _)| *id == 2).unwrap().1.iter().map(|x| x.offset).collect();
    let mine: Vec<u64> = got.iter().find(|(id, _)| *id == 1).unwrap().1.iter().map(|x| x.offset).collect();
    assert_eq!(d.take(10, theirs.iter().copied()), Vec::<u64>::new(), "not this connection's");
    assert_eq!(d.take(10, mine.iter().copied()), mine);
    assert_eq!(d.count_to_persist(&q, mine[0]), 1);
    q.apply(&stamped(lotse_record::queue_state_batch(&[(mine[0], 1), (mine[1], 1)]).unwrap(), 4)).unwrap();
    d.release(mine.iter().copied());
    d.credit(2, 2, 1 << 20);
    let c = d.candidates(&q, 4, 2);
    assert_eq!(c, mine);
    let again = d.assign(&q, &c, 100, at(1));
    assert!(again.iter().flat_map(|(_, x)| x).all(|x| x.delivery_count == 2), "{again:?}");
}

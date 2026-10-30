use std::path::Path;

use lotse_record::{BatchBuilder, BatchOptions};
use lotse_sim::{Sim, SimRuntime};

use super::*;

/// Records the first value of every applied batch.
#[derive(Debug, Default)]
struct Recorder(Vec<Bytes>);

impl StateMachine for Recorder {
    fn apply(&mut self, batch: &Batch) {
        let value = batch.records().unwrap()[0].value.clone().unwrap();
        self.0.push(value);
    }
    fn snapshot(&self) -> Bytes {
        let mut b = BytesMut::new();
        for v in &self.0 {
            b.put_u32_le(v.len() as u32);
            b.put_slice(v);
        }
        b.freeze()
    }
    fn restore(&mut self, mut data: &[u8]) -> Result<(), String> {
        self.0.clear();
        while data.has_remaining() {
            let n = data.get_u32_le() as usize;
            self.0.push(Bytes::copy_from_slice(&data[..n]));
            data.advance(n);
        }
        Ok(())
    }
}

fn batch(records: usize, value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value.as_bytes()), &[]);
    }
    b.finish().unwrap()
}

async fn open(rt: &SimRuntime) -> Replica<SimRuntime, Recorder> {
    let cfg = Config::new(1, vec![1]);
    Replica::open(
        rt.clone(),
        "g",
        cfg,
        Recorder::default(),
        LogConfig { segment_bytes: 400, index_interval_bytes: 100 },
    )
    .await
    .unwrap()
}

async fn elect(r: &mut Replica<SimRuntime, Recorder>) {
    for _ in 0..40 {
        r.raft().tick();
        r.process().await.unwrap();
    }
    assert_eq!(r.raft_ref().role(), lotse_raft::Role::Leader);
}

fn applied(r: &Replica<SimRuntime, Recorder>) -> Vec<&str> {
    r.state_machine().0.iter().map(|v| std::str::from_utf8(v).unwrap()).collect()
}

#[test]
fn a_single_replica_commits_and_reapplies_after_a_power_loss() {
    Sim::new(1).run(|rt| async move {
        let mut r = open(&rt).await;
        elect(&mut r).await;
        let Proposed { first, last, .. } = r.propose(batch(3, "a")).unwrap();
        assert_eq!((first, last), (2, 4), "no-op at 1, three records");
        r.propose(batch(1, "b")).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        assert_eq!(applied(&r), ["a", "b"]);
        assert_eq!(r.log().next_offset(), 5, "offsets 0 (no-op), 1..=3, 4");
        drop(r);
        rt.crash();
        let mut r = open(&rt).await;
        assert_eq!(r.raft_ref().last_index(), 5, "everything was synced before it was acknowledged");
        elect(&mut r).await;
        assert_eq!(applied(&r), ["a", "b"], "the state machine is rebuilt from the log");
    });
}

#[test]
fn a_replica_restarts_from_its_snapshot_and_replays_the_rest() {
    Sim::new(2).run(|rt| async move {
        let mut r = open(&rt).await;
        elect(&mut r).await;
        for v in ["a", "b", "c"] {
            r.propose(batch(2, v)).unwrap();
            r.process().await.unwrap();
            r.process().await.unwrap();
        }
        assert_eq!(r.compact().await.unwrap(), 7);
        r.propose(batch(1, "d")).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        drop(r);
        rt.crash();
        let mut r = open(&rt).await;
        assert_eq!(r.raft_ref().log().snapshot_index(), 7);
        assert_eq!(applied(&r), ["a", "b", "c"], "restored from the snapshot");
        elect(&mut r).await;
        assert_eq!(applied(&r), ["a", "b", "c", "d"], "and the rest replayed from the log");
    });
}

#[test]
fn control_batches_cannot_be_proposed_and_followers_refuse() {
    Sim::new(3).run(|rt| async move {
        let cfg = Config::new(1, vec![1, 2, 3]);
        let mut r = Replica::open(rt.clone(), "f", cfg, Recorder::default(), LogConfig::default()).await.unwrap();
        assert!(matches!(r.propose(batch(1, "x")), Err(ProposeError::NotLeader(_))));
        let control = lotse_record::raft_control_batch(RaftControl::Noop, &[]).unwrap();
        assert!(matches!(r.propose(control), Err(ProposeError::Control)));
    });
}

#[test]
fn state_files_round_trip_and_detect_damage() {
    let hs = HardState { term: 7, vote: Some(3), commit: 42 };
    assert_eq!(decode_hard_state(encode_hard_state(&hs)).unwrap(), hs);
    let none = HardState { term: 1, vote: None, commit: 0 };
    assert_eq!(decode_hard_state(encode_hard_state(&none)).unwrap(), none);
    let meta = SnapshotMeta { index: 9, term: 2, membership: Membership::of_voters([1, 2, 3]) };
    let (m, d) = decode_snapshot(encode_snapshot(&meta, b"state")).unwrap();
    assert_eq!((m, &d[..]), (meta.clone(), &b"state"[..]));
    let mut bad = BytesMut::from(&encode_snapshot(&meta, b"state")[..]);
    bad[5] ^= 1;
    assert!(matches!(decode_snapshot(bad.freeze()), Err(Error::Corrupt(_))));
}

#[test]
fn a_write_interrupted_by_a_crash_leaves_the_old_file() {
    Sim::new(4).run(|rt| async move {
        let dir = Path::new("d");
        rt.create_dir_all(dir).await.unwrap();
        write_atomic(&rt, dir, "f", Bytes::from_static(b"old")).await.unwrap();
        // The new contents are written but never renamed into place.
        let tmp = rt.open(&dir.join("f.tmp"), OpenMode::CreateOrOpen).await.unwrap();
        tmp.append(Bytes::from_static(b"new, not yet in place")).await.unwrap();
        rt.crash();
        assert_eq!(read_file(&rt, &dir.join("f")).await.unwrap().as_deref(), Some(&b"old"[..]));
    });
}

/// An idempotent batch of one record from producer 7.
fn idempotent(seq: i32, value: &str) -> Bytes {
    let mut raw = BytesMut::from(&batch(1, value)[..]);
    lotse_record::set_producer(&mut raw, 7, 0, seq).unwrap();
    raw.freeze()
}

async fn commit(r: &mut Replica<SimRuntime, Recorder>, b: Bytes) -> Proposed {
    let p = r.propose(b).unwrap();
    r.process().await.unwrap();
    r.process().await.unwrap();
    p
}

#[test]
fn producer_state_survives_a_restart_and_a_snapshot() {
    Sim::new(5).run(|rt| async move {
        let mut r = open(&rt).await;
        elect(&mut r).await;
        let a = commit(&mut r, idempotent(0, "a")).await;
        assert!(!a.duplicate);
        let again = r.propose(idempotent(0, "a")).unwrap();
        assert_eq!((again.first, again.term, again.duplicate), (a.first, None, true), "applied: written");
        assert!(matches!(r.propose(idempotent(5, "x")), Err(ProposeError::OutOfOrderSequence { expected: 1 })));

        // After a power loss the state is rebuilt from the log.
        drop(r);
        rt.crash();
        let mut r = open(&rt).await;
        elect(&mut r).await;
        assert_eq!(r.propose(idempotent(0, "a")).unwrap().first, a.first, "still known after a restart");
        let b = commit(&mut r, idempotent(1, "b")).await;

        // From a snapshot, with nothing of the producer left in the log after it.
        r.compact().await.unwrap();
        drop(r);
        rt.crash();
        let mut r = open(&rt).await;
        elect(&mut r).await;
        let again = r.propose(idempotent(1, "b")).unwrap();
        assert_eq!((again.first, again.duplicate), (b.first, true), "restored from the snapshot");
        assert!(!commit(&mut r, idempotent(2, "c")).await.duplicate);
        assert_eq!(applied(&r), ["a", "b", "c"], "every batch once");
    });
}

#[test]
fn an_unapplied_batch_is_recognised_from_the_log_tail_after_a_restart() {
    Sim::new(6).run(|rt| async move {
        let mut r = open(&rt).await;
        elect(&mut r).await;
        let a = r.propose(idempotent(0, "a")).unwrap();
        assert_eq!(a.term, Some(r.raft_ref().term()), "in flight");
        // Appended and synced, but not yet applied when the power goes.
        r.process().await.unwrap();
        drop(r);
        rt.crash();
        let mut r = open(&rt).await;
        // Lead again without processing, so nothing is applied yet: only the
        // log tail can know the batch.
        while r.raft_ref().role() != lotse_raft::Role::Leader {
            r.raft().tick();
        }
        assert_eq!(r.applied(), 0);
        let again = r.propose(idempotent(0, "a")).unwrap();
        assert_eq!((again.first, again.duplicate), (a.first, true), "the retry finds the original in the log");
        assert!(again.term.is_some(), "still in flight");
        elect(&mut r).await;
        assert_eq!(applied(&r), ["a"]);
    });
}

#[test]
fn a_queues_ack_index_survives_a_restart_and_a_snapshot() {
    Sim::new(11).run(|rt| async move {
        let open_queue = || async {
            let cfg = Config::new(1, vec![1]);
            let options = ReplicaOptions { queue: true, ..ReplicaOptions::default() };
            let log = LogConfig { segment_bytes: 400, index_interval_bytes: 100 };
            Replica::open_with(rt.clone(), "q", cfg, Recorder::default(), log, options).await.unwrap()
        };
        let mut r = open_queue().await;
        elect(&mut r).await;
        for v in ["a", "b", "c"] {
            commit(&mut r, batch(1, v)).await; // offsets 1, 2, 3 after the no-op
        }
        let ack = lotse_record::ack_batch(&[(1, 3)]).unwrap();
        assert!(
            matches!(r.propose(ack.clone()), Err(ProposeError::Invalid(_))),
            "acks are the broker's own, not produced"
        );
        r.propose_internal(ack).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        let q = r.queue().unwrap();
        assert!(q.is_settled(1) && q.is_settled(2) && !q.is_settled(3));
        let before = q.clone();

        r.compact().await.unwrap();
        drop(r);
        rt.crash();
        let r = open_queue().await;
        assert_eq!(r.queue(), Some(&before), "restored from the snapshot");
        assert_eq!(r.queue().unwrap().floor(), 3);
    });
}

const SMALL: LogConfig = LogConfig { segment_bytes: 400, index_interval_bytes: 100 };

async fn open_kept(rt: &SimRuntime, dir: &str, retention: Retention, queue: bool) -> Replica<SimRuntime, Recorder> {
    let options = ReplicaOptions { queue, retention, ..ReplicaOptions::default() };
    Replica::open_with(rt.clone(), dir, Config::new(1, vec![1]), Recorder::default(), SMALL, options).await.unwrap()
}

fn batch_at(value: &str, timestamp: i64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(timestamp, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

/// The values the log holds from its start on.
async fn kept(r: &Replica<SimRuntime, Recorder>) -> Vec<String> {
    let mut out = Vec::new();
    let mut at = r.log().start_offset();
    while at < r.log().next_offset() {
        for b in batches(r.log().read(at, 1 << 20).await.unwrap()).unwrap() {
            if !b.attributes().control && b.attributes().kind == lotse_record::BatchKind::Data {
                out.push(String::from_utf8(b.records().unwrap()[0].value.clone().unwrap().to_vec()).unwrap());
            }
            at = b.next_offset();
        }
    }
    out
}

#[test]
fn a_stream_keeps_its_newest_bytes_and_starts_at_a_snapshot() {
    Sim::new(21).run(|rt| async move {
        let retention = Retention::Stream { ms: None, bytes: Some(800) };
        let mut r = open_kept(&rt, "s", retention, false).await;
        elect(&mut r).await;
        let values: Vec<String> = (0..40).map(|i| format!("value-{i:02}")).collect();
        // Several batches per round: segments begin inside what one round
        // applies, too.
        for chunk in values.chunks(3) {
            for v in chunk {
                r.propose(batch(1, v)).unwrap();
            }
            r.process().await.unwrap();
            r.process().await.unwrap();
        }
        assert!(r.log().segment_count() > 4);
        let start = r.retain(0).await.unwrap().expect("old segments go");
        assert_eq!(r.log().start_offset(), start);
        assert_eq!(r.raft_ref().log().snapshot_index(), start, "the log starts where the snapshot ends");
        let size: u64 = r.log().segments().map(|s| s.bytes).sum();
        assert!(size >= 800, "at least the limit stays: {size}");
        let oldest = r.log().segments().next().unwrap().bytes;
        assert!(size - oldest < 800, "but no segment more than it takes: {size}, the oldest {oldest}");
        let left = kept(&r).await;
        assert_eq!(left[..], values[values.len() - left.len()..], "the newest are kept, in order");
        assert!(left.len() < values.len());
        assert!(r.log().read(start - 1, 100).await.is_err(), "below the start is gone");
        assert_eq!(r.retain(0).await.unwrap(), None, "nothing more to go");

        // After a power loss: the snapshot at the start, the rest replayed.
        drop(r);
        rt.crash();
        let mut r = open_kept(&rt, "s", retention, false).await;
        assert_eq!(r.log().start_offset(), start, "the log starts at the snapshot again");
        elect(&mut r).await;
        assert_eq!(applied(&r), values.iter().map(String::as_str).collect::<Vec<_>>(), "the state lost nothing");
        assert_eq!(kept(&r).await, left);
    });
}

#[test]
fn a_stream_drops_records_older_than_its_retention() {
    Sim::new(22).run(|rt| async move {
        let retention = Retention::Stream { ms: Some(60_000), bytes: None };
        let mut r = open_kept(&rt, "t", retention, false).await;
        elect(&mut r).await;
        let now: i64 = 10_000_000;
        for i in 0..20 {
            commit(&mut r, batch_at(&format!("old-{i:02}"), now - 120_000)).await;
        }
        for i in 0..20 {
            commit(&mut r, batch_at(&format!("new-{i:02}"), now - 1_000)).await;
        }
        assert_eq!(r.retain(now as u64 - 200_000).await.unwrap(), None, "nothing is old yet");
        r.retain(now as u64).await.unwrap().expect("the old ones go");
        let left = kept(&r).await;
        assert!(left.iter().filter(|v| v.starts_with("new")).count() == 20, "every new record stays: {left:?}");
        assert!(left.iter().filter(|v| v.starts_with("old")).count() < 20, "{left:?}");
    });
}

#[test]
fn retention_stops_at_an_open_transaction() {
    Sim::new(23).run(|rt| async move {
        let retention = Retention::Stream { ms: None, bytes: Some(0) };
        let mut r = open_kept(&rt, "x", retention, false).await;
        elect(&mut r).await;
        commit(&mut r, batch(1, "before")).await;
        let mut txn = BytesMut::from(&batch(1, "in-txn")[..]);
        lotse_record::set_producer(&mut txn, 9, 0, 0).unwrap();
        lotse_record::set_transactional(&mut txn, true).unwrap();
        let open = commit(&mut r, txn.freeze()).await;
        for i in 0..30 {
            commit(&mut r, batch(1, &format!("after-{i:02}"))).await;
        }
        r.retain(0).await.unwrap();
        assert!(r.log().start_offset() < open.first, "the open transaction is kept: {}", r.log().start_offset());
        assert!(kept(&r).await.contains(&"in-txn".to_string()));

        // Committed, it may go like the rest.
        // A marker carries the next epoch (ADR 0029).
        r.propose_marker(9, 1, true, 0).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        r.retain(0).await.unwrap().expect("now it goes");
        assert!(r.log().start_offset() > open.first - 1);
    });
}

#[test]
fn a_queue_keeps_everything_not_yet_settled() {
    Sim::new(24).run(|rt| async move {
        let mut r = open_kept(&rt, "q", Retention::Queue, true).await;
        elect(&mut r).await;
        for i in 0..30 {
            commit(&mut r, batch(1, &format!("job-{i:02}"))).await; // offsets 1..=30
        }
        assert_eq!(r.retain(0).await.unwrap(), None, "nothing is settled");
        // Every job but the 11th (offset 11) is settled.
        r.propose_internal(lotse_record::ack_batch(&[(1, 11), (12, 31)]).unwrap()).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        r.retain(0).await.unwrap();
        assert!(r.log().start_offset() <= 11, "the unsettled job stays: {}", r.log().start_offset());
        assert!(kept(&r).await.contains(&"job-10".to_string()));
        r.propose_internal(lotse_record::ack_batch(&[(11, 12)]).unwrap()).unwrap();
        r.process().await.unwrap();
        r.process().await.unwrap();
        let start = r.retain(0).await.unwrap().expect("now all of it may go");
        assert!(start > 11);
    });
}

#[test]
fn a_state_group_keeps_only_what_its_state_does_not_hold() {
    Sim::new(25).run(|rt| async move {
        let mut r = open_kept(&rt, "m", Retention::State, false).await;
        elect(&mut r).await;
        for i in 0..30 {
            commit(&mut r, batch(1, &format!("m-{i:02}"))).await;
        }
        let start = r.retain(0).await.unwrap().expect("whole segments go");
        assert_eq!(r.raft_ref().log().snapshot_index(), start);
        drop(r);
        rt.crash();
        let mut r = open_kept(&rt, "m", Retention::State, false).await;
        assert_eq!(r.log().start_offset(), start, "cut inside a segment, and again on open");
        elect(&mut r).await;
        assert_eq!(applied(&r).len(), 30, "the state holds everything");
    });
}

/// Rounds of ticks and messages among `rs` (node `i + 1` is `rs[i]`);
/// messages to or from a node not `up` are lost.
async fn rounds(rs: &mut [Replica<SimRuntime, Recorder>], up: &[bool], n: usize) {
    for _ in 0..n {
        let mut mail = Vec::new();
        for (i, r) in rs.iter_mut().enumerate() {
            if !up[i] {
                continue;
            }
            r.raft().tick();
            for s in r.process().await.unwrap().messages {
                mail.push((i as u64 + 1, s.to, s.msg));
            }
        }
        for (from, to, msg) in mail {
            if up[to as usize - 1] {
                rs[to as usize - 1].raft().step(from, msg);
            }
        }
    }
}

#[test]
fn a_follower_behind_the_start_gets_the_snapshot_and_the_rest() {
    Sim::new(26).run(|rt| async move {
        let retention = Retention::Stream { ms: None, bytes: Some(800) };
        let mut rs = Vec::new();
        for id in 1..=3u64 {
            let options = ReplicaOptions { retention, ..ReplicaOptions::default() };
            let cfg = Config::new(id, vec![1, 2, 3]);
            let host = rt.node(&format!("n{id}"));
            rs.push(Replica::open_with(host, "g", cfg, Recorder::default(), SMALL, options).await.unwrap());
        }
        // Node 3 is away while the others write and trim.
        let up = [true, true, false];
        rounds(&mut rs, &up, 80).await;
        let l = (0..2).find(|&i| rs[i].raft_ref().role() == lotse_raft::Role::Leader).expect("1 or 2 leads");
        let values: Vec<String> = (0..40).map(|i| format!("value-{i:02}")).collect();
        for v in &values {
            rs[l].propose(batch(1, v)).unwrap();
            rounds(&mut rs, &up, 3).await;
        }
        let start = rs[l].retain(0).await.unwrap().expect("old segments go");

        // Node 3 comes back with nothing: it gets the snapshot at the start,
        // then everything after it.
        rounds(&mut rs, &[true, true, true], 80).await;
        let back = &rs[2];
        assert_eq!(back.raft_ref().log().snapshot_index(), start, "the snapshot where the leader's log starts");
        assert_eq!(back.log().start_offset(), start);
        assert_eq!(back.log().next_offset(), rs[l].log().next_offset());
        assert_eq!(kept(back).await, kept(&rs[l]).await, "the same data from the start on");
        assert_eq!(applied(back), applied(&rs[l]), "and the same state");
    });
}

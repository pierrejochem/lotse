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

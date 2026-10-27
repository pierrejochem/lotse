use super::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_sim::Sim;

fn batch(base_offset: u64, values: &[&[u8]]) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for (i, v) in values.iter().enumerate() {
        b.push(i as i64, None, Some(v), &[]);
    }
    let mut owned = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut owned, base_offset, 1).unwrap();
    owned.freeze()
}

const SMALL: LogConfig = LogConfig { segment_bytes: 300, index_interval_bytes: 100 };

#[test]
fn append_assigns_contiguous_offsets_and_rejects_gaps() {
    Sim::new(1).run(|rt| async move {
        let mut log = Log::open(rt, "p0", LogConfig::default()).await.unwrap();
        assert_eq!(log.append(batch(0, &[b"a", b"b"])).await.unwrap(), Appended { base_offset: 0, next_offset: 2 });
        assert!(matches!(log.append(batch(5, &[b"c"])).await, Err(Error::OffsetMismatch { expected: 2, got: 5 })));
        assert_eq!(log.append(batch(2, &[b"c"])).await.unwrap().next_offset, 3);
    });
}

#[test]
fn read_returns_the_batch_containing_the_offset() {
    Sim::new(2).run(|rt| async move {
        let mut log = Log::open(rt, "p0", SMALL).await.unwrap();
        let mut expected = Vec::new();
        for i in 0..20u64 {
            let b = batch(i * 3, &[b"one", b"two", b"three"]);
            log.append(b.clone()).await.unwrap();
            expected.push(b);
        }
        assert!(log.segment_count() > 1, "small segments must roll");
        for offset in 0..60u64 {
            let bytes = log.read(offset, 1).await.unwrap();
            let (first, _) = Batch::parse(&bytes).unwrap();
            assert_eq!(first.as_bytes(), &expected[(offset / 3) as usize], "offset {offset}");
        }
        assert!(log.read(60, 1000).await.unwrap().is_empty());
        assert!(matches!(log.read(61, 1).await, Err(Error::OffsetOutOfRange { .. })));
    });
}

#[test]
fn read_fills_up_to_max_bytes_with_whole_batches() {
    Sim::new(3).run(|rt| async move {
        let mut log = Log::open(rt, "p0", LogConfig::default()).await.unwrap();
        let one = batch(0, &[b"x"]);
        let len = one.len();
        log.append(one).await.unwrap();
        for i in 1..5 {
            log.append(batch(i, &[b"x"])).await.unwrap();
        }
        assert_eq!(log.read(0, len * 3).await.unwrap().len(), len * 3);
        assert_eq!(log.read(0, len * 3 - 1).await.unwrap().len(), len * 2);
    });
}

#[test]
fn reopen_recovers_every_synced_batch() {
    Sim::new(4).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..10 {
            log.append(batch(i, &[b"payload"])).await.unwrap();
        }
        log.sync().await.unwrap();
        drop(log);
        let log = Log::open(rt, "p0", SMALL).await.unwrap();
        assert_eq!(log.next_offset(), 10);
        assert_eq!(log.start_offset(), 0);
    });
}

#[test]
fn truncate_cuts_at_batch_boundaries_only() {
    Sim::new(5).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..10u64 {
            log.append(batch(i * 2, &[b"a", b"b"])).await.unwrap();
        }
        assert!(matches!(log.truncate_from(7).await, Err(Error::NotBatchBoundary(7))));
        log.truncate_from(8).await.unwrap();
        assert_eq!(log.next_offset(), 8);
        log.append(batch(8, &[b"new"])).await.unwrap();
        drop(log);
        let log = Log::open(rt, "p0", SMALL).await.unwrap();
        assert_eq!(log.next_offset(), 9);
    });
}

#[test]
fn truncate_to_the_start_empties_the_log() {
    Sim::new(6).run(|rt| async move {
        let mut log = Log::open(rt, "p0", SMALL).await.unwrap();
        for i in 0..10u64 {
            log.append(batch(i, &[b"abcdefgh"])).await.unwrap();
        }
        log.truncate_from(0).await.unwrap();
        assert_eq!(log.next_offset(), 0);
        assert_eq!(log.segment_count(), 1);
        log.append(batch(0, &[b"again"])).await.unwrap();
    });
}

#[test]
fn corruption_in_a_sealed_segment_is_an_error_not_a_silent_cut() {
    Sim::new(7).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..20u64 {
            log.append(batch(i, &[b"payload"])).await.unwrap();
        }
        log.sync().await.unwrap();
        assert!(log.segment_count() > 1);
        drop(log);
        rt.disk().flip_bit(&segment_path(Path::new("p0"), 0), 40, 3).unwrap();
        assert!(matches!(Log::open(rt, "p0", SMALL).await, Err(Error::Corrupt { .. })));
    });
}

#[test]
fn reset_restarts_the_log_at_an_offset_and_survives_a_crash() {
    Sim::new(9).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..10u64 {
            log.append(batch(i * 2, &[b"aaaaaaaaaaaaaaaaaaaa", b"bbbbbbbbbbbbbbbbbbbb"])).await.unwrap();
        }
        log.sync().await.unwrap();
        assert!(log.segment_count() > 1);
        log.reset(57).await.unwrap();
        assert_eq!((log.start_offset(), log.next_offset(), log.segment_count()), (57, 57, 1));
        assert!(matches!(log.read(10, 100).await, Err(Error::OffsetOutOfRange { .. })));
        log.append(batch(57, &[b"x"])).await.unwrap();
        log.sync().await.unwrap();
        drop(log);
        rt.crash();
        let log = Log::open(rt, "p0", SMALL).await.unwrap();
        assert_eq!((log.start_offset(), log.next_offset()), (57, 58));
    });
}

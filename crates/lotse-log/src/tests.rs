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

fn batch_at(base_offset: u64, timestamp: i64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(timestamp, None, Some(b"value"), &[]);
    let mut owned = BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut owned, base_offset, 1).unwrap();
    owned.freeze()
}

#[test]
fn trimming_deletes_whole_segments_below_an_offset() {
    Sim::new(9).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..30u64 {
            log.append(batch_at(i, 1_000 + i as i64)).await.unwrap();
        }
        log.sync().await.unwrap();
        let segs: Vec<SegmentInfo> = log.segments().collect();
        assert!(segs.len() >= 4, "{segs:?}");
        assert!(segs.windows(2).all(|w| w[0].next_offset == w[1].base_offset), "contiguous: {segs:?}");
        assert!(segs.windows(2).all(|w| w[0].max_timestamp < w[1].max_timestamp), "newest per segment: {segs:?}");
        assert_eq!(segs.last().unwrap().max_timestamp, 1_029);
        assert_eq!(segs.iter().map(|s| s.bytes).sum::<u64>(), log.size_bytes());

        // Inside the third segment: the two before it go, it stays whole.
        let cut = segs[2].base_offset + 1;
        log.trim_before(cut).await.unwrap();
        assert_eq!(log.start_offset(), cut);
        assert_eq!(log.segments().next().unwrap().base_offset, segs[2].base_offset);
        assert!(matches!(log.read(cut - 1, 100).await, Err(Error::OffsetOutOfRange { .. })), "below the start");
        assert!(!log.read(cut, 100).await.unwrap().is_empty());
        assert!(matches!(log.trim_before(cut - 1).await, Err(Error::OffsetOutOfRange { .. })), "never backwards");

        // Trimming everything keeps the active segment, and the log goes on.
        let end = log.next_offset();
        log.trim_before(end).await.unwrap();
        assert_eq!((log.start_offset(), log.segment_count()), (end, 1));
        log.append(batch_at(end, 2_000)).await.unwrap();
        log.sync().await.unwrap();
        drop(log);

        // After a restart the log starts at its first segment's base.
        let log = Log::open(rt, "p0", SMALL).await.unwrap();
        assert!(log.start_offset() <= end && log.next_offset() == end + 1, "{log:?}");
        assert!(!log.read(end, 100).await.unwrap().is_empty());
    });
}

#[test]
fn a_sealed_segment_is_rewritten_whole_and_keeps_its_batches() {
    Sim::new(11).run(|rt| async move {
        let mut log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        for i in 0..10u64 {
            log.append(batch(i * 2, &[b"aaaaaaaaaaaaaaaaaaaa", b"bbbbbbbbbbbbbbbbbbbb"])).await.unwrap();
        }
        log.sync().await.unwrap();
        let first = log.segments().next().unwrap();
        let active = log.segments().last().unwrap().base_offset;
        assert_ne!(first.base_offset, active);

        let old = batches_in(&log, first).await;
        let keep_first = |b: &Bytes| {
            let (b, _) = Batch::parse(b).unwrap();
            let mut n = 0;
            lotse_record::compact(&b, |_| {
                n += 1;
                n == 1
            })
            .unwrap()
            .unwrap()
        };
        let new: Vec<Bytes> = old.iter().map(keep_first).collect();

        // Anything but the same batches with fewer records is refused.
        assert!(matches!(log.rewrite_segment(first.base_offset, &new[1..]).await, Err(Error::Rewrite(_))));
        let mut moved = new.clone();
        moved[0] = batch(first.base_offset, &[b"x"]);
        assert!(matches!(log.rewrite_segment(first.base_offset, &moved).await, Err(Error::Rewrite(_))));
        assert!(matches!(log.rewrite_segment(active, &[]).await, Err(Error::Rewrite(_))));

        log.rewrite_segment(first.base_offset, &new).await.unwrap();
        let after = log.segments().next().unwrap();
        assert_eq!((after.base_offset, after.next_offset), (first.base_offset, first.next_offset));
        assert!(after.bytes < first.bytes, "{after:?} {first:?}");
        assert_eq!(batches_in(&log, after).await, new);
        // The second record of the first batch is gone; reading it finds
        // the batch that spans it.
        let (b, _) = Batch::parse(&log.read(first.base_offset + 1, 1).await.unwrap()).unwrap();
        assert_eq!((b.base_offset(), b.record_count(), b.span()), (first.base_offset, 1, 2));
        log.append(batch(log.next_offset(), &[b"y"])).await.unwrap();
        log.sync().await.unwrap();
        drop(log);

        // A rewrite cut short by a crash leaves a file that open removes.
        rt.crash();
        let stray = cleaned_path(Path::new("p0"), active);
        let f = rt.open(&stray, OpenMode::CreateNew).await.unwrap();
        f.append(Bytes::from_static(b"half")).await.unwrap();
        f.sync().await.unwrap();
        drop(f);
        let log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        assert!(!rt.list(Path::new("p0")).await.unwrap().contains(&stray));
        assert_eq!(batches_in(&log, log.segments().next().unwrap()).await, new);
    });
}

async fn batches_in<D: Disk>(log: &Log<D>, seg: SegmentInfo) -> Vec<Bytes> {
    let mut out = Vec::new();
    let mut at = seg.base_offset;
    while at < seg.next_offset {
        let (b, _) = Batch::parse(&log.read(at, 1).await.unwrap()).unwrap();
        at = b.next_offset();
        out.push(b.as_bytes().clone());
    }
    out
}

use super::*;
use bytes::BytesMut;
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
        // The segment's index file still matches (its last batch is whole),
        // so the log opens, and the damage shows when the batch is read
        // (ADR 0094).
        let log = Log::open(rt.clone(), "p0", SMALL).await.unwrap();
        assert!(matches!(log.read(0, 1).await, Err(Error::Corrupt { .. })));
        drop(log);
        // Without the index file the segment is scanned on open, which
        // finds it.
        rt.remove(&index_path(Path::new("p0"), 0)).await.unwrap();
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

/// Every offset of the log reads back as the batch that was appended for it.
async fn assert_reads(log: &Log<lotse_sim::SimRuntime>, expected: &[Bytes]) {
    for b in expected {
        let (want, _) = Batch::parse(b).unwrap();
        if want.base_offset() < log.start_offset() || want.base_offset() >= log.next_offset() {
            continue;
        }
        for offset in want.base_offset()..want.next_offset() {
            let bytes = log.read(offset, 1).await.unwrap();
            assert_eq!(&bytes, b, "offset {offset}");
        }
    }
}

#[test]
fn sealed_segments_get_index_files_and_the_active_one_none() {
    Sim::new(20).run(|rt| async move {
        let dir = Path::new("p0");
        let mut log = Log::open(rt.clone(), dir, SMALL).await.unwrap();
        let mut expected = Vec::new();
        for i in 0..30u64 {
            let b = batch_at(i, 1_000 + i as i64);
            log.append(b.clone()).await.unwrap();
            expected.push(b);
        }
        log.sync().await.unwrap();
        let segs: Vec<SegmentInfo> = log.segments().collect();
        let (sealed, active) = segs.split_at(segs.len() - 1);
        assert!(sealed.len() >= 3);
        for s in sealed {
            assert!(rt.disk().file_len(&index_path(dir, s.base_offset)).is_some(), "segment {}", s.base_offset);
        }
        assert!(rt.disk().file_len(&index_path(dir, active[0].base_offset)).is_none());
        drop(log);
        // A missing index file is written again on open; the log reads the same.
        rt.remove(&index_path(dir, sealed[1].base_offset)).await.unwrap();
        let log = Log::open(rt.clone(), dir, SMALL).await.unwrap();
        assert!(rt.disk().file_len(&index_path(dir, sealed[1].base_offset)).is_some());
        assert_eq!(log.segments().collect::<Vec<_>>(), segs);
        assert_reads(&log, &expected).await;
    });
}

/// An index file that does not describe its segment is not believed: here
/// it is the file of a segment with the same offsets and length but other
/// timestamps and contents.
#[test]
fn an_index_file_of_other_contents_is_not_trusted() {
    Sim::new(21).run(|rt| async move {
        let fill = async |dir: &str, ts: i64| {
            let mut log = Log::open(rt.clone(), dir, SMALL).await.unwrap();
            let mut expected = Vec::new();
            for i in 0..12u64 {
                let b = batch_at(i, ts + i as i64);
                log.append(b.clone()).await.unwrap();
                expected.push(b);
            }
            log.sync().await.unwrap();
            let segs: Vec<SegmentInfo> = log.segments().collect();
            (segs, expected)
        };
        let (old, _) = fill("old", 1_000).await;
        let (new, expected) = fill("new", 5_000).await;
        assert_eq!(
            old.iter().map(|s| (s.base_offset, s.bytes)).collect::<Vec<_>>(),
            new.iter().map(|s| (s.base_offset, s.bytes)).collect::<Vec<_>>(),
            "the same shape"
        );
        // Put the old log's index file beside the new log's first segment.
        let stale = rt.open(&index_path(Path::new("old"), 0), OpenMode::Existing).await.unwrap();
        let bytes = stale.read_at(0, stale.size().await.unwrap() as usize).await.unwrap();
        let target = rt.open(&index_path(Path::new("new"), 0), OpenMode::Existing).await.unwrap();
        target.truncate(0).await.unwrap();
        target.append(bytes).await.unwrap();
        target.sync().await.unwrap();
        let log = Log::open(rt.clone(), "new", SMALL).await.unwrap();
        assert_eq!(log.segments().collect::<Vec<_>>(), new, "timestamps come from the segment, not the stale file");
        assert_eq!(log.time_search_start(0, 5_000), Some(0));
        assert_reads(&log, &expected).await;
    });
}

#[test]
fn a_crash_at_any_point_of_rolling_and_indexing_loses_no_synced_batch() {
    for ops in 1..120 {
        Sim::new(22).run(|rt| async move {
            let dir = Path::new("p0");
            let mut log = Log::open(rt.clone(), dir, SMALL).await.unwrap();
            let mut expected = Vec::new();
            for i in 0..8u64 {
                let b = batch_at(i, 1_000 + i as i64);
                log.append(b.clone()).await.unwrap();
                expected.push(b);
            }
            log.sync().await.unwrap();
            let durable = log.next_offset();
            rt.disk().arm_power_loss(ops);
            let mut synced = durable;
            for i in 8..40u64 {
                let b = batch_at(i, 1_000 + i as i64);
                if log.append(b.clone()).await.is_err() {
                    break;
                }
                expected.push(b);
                if log.sync().await.is_err() {
                    break;
                }
                synced = log.next_offset();
            }
            drop(log);
            rt.crash();
            let log = Log::open(rt.clone(), dir, SMALL).await.unwrap();
            assert!(log.next_offset() >= synced, "ops {ops}: {} < {synced}", log.next_offset());
            assert_reads(&log, &expected).await;
            drop(log);
            // And once more, now from whatever index files the first open left.
            let log = Log::open(rt, dir, SMALL).await.unwrap();
            assert_reads(&log, &expected).await;
        });
    }
}

#[test]
fn a_search_by_time_starts_where_every_batch_before_is_older() {
    Sim::new(23).run(|rt| async move {
        let config = LogConfig { segment_bytes: 2_000, index_interval_bytes: 150 };
        let mut log = Log::open(rt.clone(), "p0", config).await.unwrap();
        // Timestamps that mostly grow, with some older ones in between.
        let ts = |i: u64| 10_000 + (i as i64) * 10 - if i % 7 == 3 { 500 } else { 0 };
        let mut batches = Vec::new();
        for i in 0..120u64 {
            let b = batch_at(i, ts(i));
            log.append(b.clone()).await.unwrap();
            batches.push(b);
        }
        log.sync().await.unwrap();
        drop(log);
        let log = Log::open(rt, "p0", config).await.unwrap();
        let segs: Vec<SegmentInfo> = log.segments().collect();
        assert!(segs.len() >= 4);
        for at in (9_000..11_500).step_by(37) {
            for s in &segs {
                let start = log.time_search_start(s.base_offset, at).unwrap();
                assert!((s.base_offset..s.next_offset.max(s.base_offset + 1)).contains(&start));
                for o in s.base_offset..start {
                    assert!(ts(o) < at, "batch {o} at {} skipped in a search for {at}", ts(o));
                }
            }
        }
        // The index makes the search skip most of a long segment.
        let last = segs.iter().rev().find(|s| s.next_offset - s.base_offset > 10).unwrap();
        let late = ts(last.next_offset - 1);
        assert!(log.time_search_start(last.base_offset, late).unwrap() > last.base_offset + 3);
        assert_eq!(log.time_search_start(12_345, 0), None);
    });
}

#[test]
fn reads_beyond_the_read_ahead_return_whole_checked_batches() {
    Sim::new(24).run(|rt| async move {
        let config = LogConfig { segment_bytes: 1 << 20, index_interval_bytes: 64 };
        let mut log = Log::open(rt, "p0", config).await.unwrap();
        let mut batches = Vec::new();
        for i in 0..200u64 {
            let value = vec![b'a' + (i % 26) as u8; 1 + (i as usize * 13) % 300];
            let b = batch(i, &[&value]);
            log.append(b.clone()).await.unwrap();
            batches.push(b);
        }
        for (offset, max) in [(0, 1), (0, 5_000), (17, 20_000), (100, 300), (150, 1 << 20), (199, 10)] {
            let got = log.read(offset, max).await.unwrap();
            let mut want = batches[offset as usize].to_vec();
            for b in &batches[offset as usize + 1..] {
                if want.len() + b.len() > max {
                    break;
                }
                want.extend_from_slice(b);
            }
            assert_eq!(&got[..], &want[..], "read({offset}, {max})");
        }
    });
}

/// An index file of a shorter segment with the same first batches (a stale
/// file from before the segment held more) is not believed either.
#[test]
fn an_index_file_of_a_shorter_segment_is_not_trusted() {
    Sim::new(25).run(|rt| async move {
        let fill = async |dir: &str, segment_bytes: u64| {
            let config = LogConfig { segment_bytes, index_interval_bytes: 100 };
            let mut log = Log::open(rt.clone(), dir, config).await.unwrap();
            let mut expected = Vec::new();
            for i in 0..12u64 {
                let b = batch_at(i, 1_000 + i as i64);
                log.append(b.clone()).await.unwrap();
                expected.push(b);
            }
            log.sync().await.unwrap();
            (log.segments().collect::<Vec<_>>(), expected)
        };
        let (short, _) = fill("short", 300).await;
        let (long, expected) = fill("long", 400).await;
        assert!(short[0].next_offset < long[0].next_offset, "{short:?} {long:?}");
        let stale = rt.open(&index_path(Path::new("short"), 0), OpenMode::Existing).await.unwrap();
        let bytes = stale.read_at(0, stale.size().await.unwrap() as usize).await.unwrap();
        let target = rt.open(&index_path(Path::new("long"), 0), OpenMode::Existing).await.unwrap();
        target.truncate(0).await.unwrap();
        target.append(bytes).await.unwrap();
        target.sync().await.unwrap();
        let log =
            Log::open(rt.clone(), "long", LogConfig { segment_bytes: 400, index_interval_bytes: 100 }).await.unwrap();
        assert_eq!(log.segments().collect::<Vec<_>>(), long);
        assert_reads(&log, &expected).await;
    });
}

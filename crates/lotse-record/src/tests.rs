use super::*;
use proptest::prelude::*;

type Rec = (i64, Option<Vec<u8>>, Option<Vec<u8>>, Vec<(Vec<u8>, Option<Vec<u8>>)>);

fn build(options: BatchOptions, records: &[Rec]) -> Bytes {
    let mut b = BatchBuilder::new(options);
    for (ts, k, v, hs) in records {
        let hs: Vec<(&[u8], Option<&[u8]>)> = hs.iter().map(|(k, v)| (k.as_slice(), v.as_deref())).collect();
        b.push(*ts, k.as_deref(), v.as_deref(), &hs);
    }
    b.finish().unwrap()
}

#[test]
fn header_is_65_bytes_and_fields_land_where_documented() {
    let bytes = build(
        BatchOptions { producer_id: 7, producer_epoch: 3, base_sequence: 42, ..Default::default() },
        &[(1_000, Some(b"k".to_vec()), Some(b"v".to_vec()), vec![])],
    );
    let mut owned = BytesMut::from(&bytes[..]);
    stamp(&mut owned, 100, 9).unwrap();
    let (batch, rest) = Batch::parse(&owned.freeze()).unwrap();
    assert!(rest.is_empty());
    assert_eq!(batch.base_offset(), 100);
    assert_eq!(batch.term(), 9);
    assert_eq!(batch.producer_id(), 7);
    assert_eq!(batch.producer_epoch(), 3);
    assert_eq!(batch.base_sequence(), 42);
    assert_eq!(batch.record_count(), 1);
    assert_eq!(batch.last_offset(), 100);
    assert_eq!(batch.as_bytes()[20], MAGIC);
}

#[test]
fn stamping_keeps_the_checksum_valid() {
    let bytes = build(BatchOptions::default(), &[(5, None, Some(b"x".to_vec()), vec![])]);
    let mut owned = BytesMut::from(&bytes[..]);
    stamp(&mut owned, u64::MAX - 1, 77).unwrap();
    assert!(Batch::parse(&owned.freeze()).is_ok());
}

#[test]
fn any_flipped_bit_in_the_checksummed_range_is_caught() {
    let bytes = build(BatchOptions::default(), &[(5, Some(b"key".to_vec()), Some(b"value".to_vec()), vec![])]);
    for i in 21..bytes.len() {
        for bit in 0..8 {
            let mut corrupt = BytesMut::from(&bytes[..]);
            corrupt[i] ^= 1 << bit;
            assert!(Batch::parse(&corrupt.freeze()).is_err(), "flip at byte {i} bit {bit} not detected");
        }
    }
}

#[test]
fn truncated_batches_are_reported_as_truncated() {
    let bytes = build(BatchOptions::default(), &[(5, None, Some(b"payload".to_vec()), vec![])]);
    for cut in 0..bytes.len() {
        assert_eq!(Batch::parse(&bytes.slice(..cut)).unwrap_err(), Error::Truncated, "cut at {cut}");
    }
}

#[test]
fn a_batch_that_is_not_compacted_keeps_the_strict_rules() {
    // The same bytes as a compacted batch with a record gone, but without the
    // compacted bit: refused, as a producer may not send gaps.
    let bytes = build(BatchOptions::default(), &[(1, Some(b"k".to_vec()), None, vec![]), (2, None, None, vec![])]);
    let (batch, _) = Batch::parse(&bytes).unwrap();
    let c = compact(&batch, |r| r.offset == 1).unwrap().unwrap();
    let mut plain = BytesMut::from(&c[..]);
    let bits = get_u16(&plain, 25) & !(1 << 8);
    put_u16(&mut plain, 25, bits);
    let crc = crc32c::crc32c(&plain[CRC_START..]);
    put_u32(&mut plain, 21, crc);
    assert!(matches!(Batch::parse(&plain.freeze()), Err(Error::Malformed(_))));
    // A control batch is never compacted.
    let marker = control_batch(7, 1, ControlType::Commit, 3, 0).unwrap();
    let (m, _) = Batch::parse(&marker).unwrap();
    assert!(compact(&m, |_| false).unwrap().is_none());
    let mut forged = BytesMut::from(&marker[..]);
    let bits = get_u16(&forged, 25) | (1 << 8);
    put_u16(&mut forged, 25, bits);
    let crc = crc32c::crc32c(&forged[CRC_START..]);
    put_u32(&mut forged, 21, crc);
    assert!(matches!(Batch::parse(&forged.freeze()), Err(Error::Malformed(_))));
}

#[test]
fn empty_batches_cannot_be_built() {
    assert!(BatchBuilder::new(BatchOptions::default()).finish().is_err());
}

/// A compressed batch with `block` as its record block, the header filled
/// in as the builder would: what a careless or hostile producer could send.
fn with_block(codec: Compression, count: u32, block: &[u8]) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..count {
        b.push(0, None, Some(b"x"), &[]);
    }
    let plain = b.finish().unwrap();
    let mut buf = BytesMut::from(&plain[..HEADER_LEN]);
    buf.extend_from_slice(block);
    let len = (buf.len() - LEN_PREFIX) as u32;
    buf[8..12].copy_from_slice(&len.to_le_bytes());
    let attrs = Attributes { compression: codec, ..BatchOptions::default().attributes };
    buf[25..27].copy_from_slice(&attrs.to_bits().to_le_bytes());
    let crc = crc32c::crc32c(&buf[25..]);
    buf[21..25].copy_from_slice(&crc.to_le_bytes());
    buf.freeze()
}

#[test]
fn compressed_batches_are_smaller_and_read_the_same() {
    let records: Vec<Rec> =
        (0..200).map(|i| (i, None, Some(b"the same value, again and again".to_vec()), vec![])).collect();
    let plain = build(BatchOptions::default(), &records);
    for codec in [Compression::Zstd, Compression::Lz4] {
        let mut opts = BatchOptions::default();
        opts.attributes.compression = codec;
        let bytes = build(opts, &records);
        assert!(bytes.len() * 4 < plain.len(), "{codec:?}: {} of {} bytes", bytes.len(), plain.len());
        let (batch, _) = Batch::parse(&bytes).unwrap();
        assert_eq!(batch.attributes().compression, codec);
        let (plain_batch, _) = Batch::parse(&plain).unwrap();
        assert_eq!(batch.records().unwrap(), plain_batch.records().unwrap(), "{codec:?}");
    }
}

#[test]
fn a_compressed_block_that_does_not_decode_is_refused() {
    for codec in [Compression::Zstd, Compression::Lz4] {
        let bytes = with_block(codec, 1, b"not compressed at all");
        let (batch, _) = Batch::parse(&bytes).expect("the header and CRC are fine");
        assert!(matches!(batch.records(), Err(Error::Codec(c, _)) if c == codec), "{codec:?}");
    }
}

#[test]
fn a_compressed_block_may_not_decode_beyond_the_limit() {
    // 17 MiB of zeros compress to a few KiB: a small batch, a large block.
    let zeros = vec![0u8; MAX_RECORDS_LEN + 1024 * 1024];
    let zstd = zstd::bulk::compress(&zeros, 3).unwrap();
    let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
    std::io::Write::write_all(&mut enc, &zeros).unwrap();
    let lz4 = enc.finish().unwrap();
    for (codec, block) in [(Compression::Zstd, zstd), (Compression::Lz4, lz4)] {
        assert!(block.len() < MAX_BATCH_LEN, "{codec:?} fits a batch");
        let (batch, _) = Batch::parse(&with_block(codec, 1, &block)).unwrap();
        assert_eq!(batch.records().unwrap_err(), Error::DecompressedTooLarge, "{codec:?}");
    }
    // Nor may a builder make one.
    let mut opts = BatchOptions::default();
    opts.attributes.compression = Compression::Zstd;
    let mut b = BatchBuilder::new(opts);
    b.push(0, None, Some(&zeros), &[]);
    assert_eq!(b.finish().unwrap_err(), Error::DecompressedTooLarge);
}

#[test]
fn a_compressed_block_with_fewer_records_than_its_header_is_refused() {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"one"), &[]);
    let one = b.finish().unwrap();
    let block = zstd::bulk::compress(&one[HEADER_LEN..], 3).unwrap();
    let (batch, _) = Batch::parse(&with_block(Compression::Zstd, 2, &block)).unwrap();
    assert!(batch.records().is_err(), "the header says two");
}

#[test]
fn control_markers_roundtrip() {
    for marker in [ControlType::Commit, ControlType::Abort] {
        let bytes = control_batch(11, 2, marker, 5, 1_700).unwrap();
        let (batch, _) = Batch::parse(&bytes).unwrap();
        assert!(batch.attributes().control);
        assert!(batch.attributes().transactional);
        assert_eq!(control_marker(&batch).unwrap(), (marker, 5));
    }
}

#[test]
fn several_batches_frame_back_to_back() {
    let a = build(BatchOptions::default(), &[(1, None, Some(b"a".to_vec()), vec![])]);
    let b = build(BatchOptions::default(), &[(2, None, Some(b"bb".to_vec()), vec![]), (3, None, None, vec![])]);
    let mut joined = BytesMut::new();
    joined.extend_from_slice(&a);
    joined.extend_from_slice(&b);
    let (first, rest) = Batch::parse(&joined.freeze()).unwrap();
    let (second, rest) = Batch::parse(&rest).unwrap();
    assert!(rest.is_empty());
    assert_eq!(first.record_count(), 1);
    assert_eq!(second.record_count(), 2);
}

fn attributes() -> impl Strategy<Value = Attributes> {
    (any::<bool>(), any::<bool>(), 0u8..6, 0u8..3).prop_map(|(transactional, control, k, c)| Attributes {
        compression: [Compression::None, Compression::Zstd, Compression::Lz4][c as usize],
        transactional,
        control,
        kind: [
            BatchKind::Data,
            BatchKind::Ack,
            BatchKind::QueueState,
            BatchKind::Timer,
            BatchKind::DeadLetter,
            BatchKind::Retire,
        ][k as usize],
    })
}

fn bytes_opt() -> impl Strategy<Value = Option<Vec<u8>>> {
    proptest::option::of(proptest::collection::vec(any::<u8>(), 0..64))
}

fn record() -> impl Strategy<Value = Rec> {
    (
        any::<i64>(),
        bytes_opt(),
        bytes_opt(),
        proptest::collection::vec((proptest::collection::vec(any::<u8>(), 0..16), bytes_opt()), 0..3),
    )
}

proptest! {
    #[test]
    fn attributes_roundtrip(a in attributes()) {
        prop_assert_eq!(Attributes::from_bits(a.to_bits()), Ok(a));
    }

    #[test]
    fn batches_roundtrip(
        mut attrs in attributes(),
        records in proptest::collection::vec(record(), 1..20),
        producer_id: u64, producer_epoch: u16, base_sequence: i32,
        base_offset in 0u64..u64::MAX / 2, term: u64,
    ) {
        attrs.control = false;
        let options = BatchOptions { attributes: attrs, producer_id, producer_epoch, base_sequence };
        let bytes = build(options, &records);
        let mut owned = BytesMut::from(&bytes[..]);
        stamp(&mut owned, base_offset, term).unwrap();
        let (batch, rest) = Batch::parse(&owned.freeze()).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(batch.attributes(), attrs);
        prop_assert_eq!(batch.term(), term);
        prop_assert_eq!(batch.record_count() as usize, records.len());
        prop_assert_eq!(batch.max_timestamp(), records.iter().map(|r| r.0).max().unwrap());
        let decoded = batch.records().unwrap();
        for (i, (rec, (ts, k, v, hs))) in decoded.iter().zip(&records).enumerate() {
            prop_assert_eq!(rec.offset, base_offset + i as u64);
            prop_assert_eq!(rec.timestamp, *ts);
            prop_assert_eq!(rec.key.as_deref(), k.as_deref());
            prop_assert_eq!(rec.value.as_deref(), v.as_deref());
            prop_assert_eq!(rec.headers.len(), hs.len());
            for (h, (hk, hv)) in rec.headers.iter().zip(hs) {
                prop_assert_eq!(&h.key[..], &hk[..]);
                prop_assert_eq!(h.value.as_deref(), hv.as_deref());
            }
        }
    }

    #[test]
    fn compaction_keeps_offsets_header_and_what_it_keeps(
        mut attrs in attributes(),
        records in proptest::collection::vec(record(), 1..20),
        keep in proptest::collection::vec(any::<bool>(), 20),
        base_offset in 0u64..u64::MAX / 2, term: u64,
    ) {
        attrs.control = false;
        let options = BatchOptions { attributes: attrs, producer_id: 9, producer_epoch: 2, base_sequence: 40 };
        let mut owned = BytesMut::from(&build(options, &records)[..]);
        stamp(&mut owned, base_offset, term).unwrap();
        let (batch, _) = Batch::parse(&owned.freeze()).unwrap();
        let all = batch.records().unwrap();
        let wanted = |r: &Record| keep[(r.offset - base_offset) as usize];
        let Some(bytes) = compact(&batch, wanted).unwrap() else {
            prop_assert!(all.iter().all(wanted), "unchanged only when everything is kept");
            return Ok(());
        };
        let (c, rest) = Batch::parse(&bytes).unwrap();
        prop_assert!(rest.is_empty() && c.is_compacted());
        prop_assert_eq!(c.attributes(), attrs);
        prop_assert_eq!((c.base_offset(), c.next_offset(), c.term()), (batch.base_offset(), batch.next_offset(), term));
        prop_assert_eq!((c.producer_id(), c.producer_epoch(), c.base_sequence()), (9, 2, 40));
        prop_assert_eq!((c.base_timestamp(), c.max_timestamp()), (batch.base_timestamp(), batch.max_timestamp()));
        prop_assert_eq!(c.span(), batch.span());
        let kept: Vec<Record> = all.iter().filter(|r| wanted(r)).cloned().collect();
        prop_assert_eq!(c.records().unwrap(), kept);
        prop_assert_eq!(c.is_empty(), !all.iter().any(wanted));
        // Compacting again with the same choice changes nothing.
        prop_assert!(compact(&c, wanted).unwrap().is_none());
    }

    #[test]
    fn parse_never_panics_on_garbage(data in proptest::collection::vec(any::<u8>(), 0..300)) {
        let bytes = Bytes::from(data);
        if let Ok((batch, _)) = Batch::parse(&bytes) {
            let _ = batch.records();
        }
    }
}

#[test]
fn raft_control_batches_round_trip_and_are_not_transaction_markers() {
    for (kind, payload) in [(RaftControl::Noop, &b""[..]), (RaftControl::Config, &b"members"[..])] {
        let mut bytes = BytesMut::from(&raft_control_batch(kind, payload).unwrap()[..]);
        stamp(&mut bytes, 41, 7).unwrap();
        let (batch, rest) = Batch::parse(&bytes.freeze()).unwrap();
        assert!(rest.is_empty());
        assert_eq!((batch.base_offset(), batch.last_offset(), batch.term()), (41, 41, 7), "one index");
        assert!(batch.attributes().control && !batch.attributes().transactional);
        assert_eq!(raft_control(&batch).unwrap(), Some((kind, Bytes::copy_from_slice(payload))));
        assert!(control_marker(&batch).is_err(), "not a transaction marker");
    }
    let marker = control_batch(9, 1, ControlType::Commit, 3, 0).unwrap();
    let (batch, _) = Batch::parse(&marker).unwrap();
    assert_eq!(raft_control(&batch).unwrap(), None);
}

#[test]
fn producer_fields_can_be_set_after_building() {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    b.push(1, None, Some(b"w"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    set_producer(&mut raw, 77, 3, 41).unwrap();
    let (batch, _) = Batch::parse(&raw.clone().freeze()).expect("the checksum was recomputed");
    assert_eq!((batch.producer_id(), batch.producer_epoch(), batch.base_sequence()), (77, 3, 41));
    assert_eq!(batch.records().unwrap().len(), 2);
    assert!(set_producer(&mut raw[..20], 1, 0, 0).is_err(), "a cut batch is refused");
}

#[test]
fn control_batches_are_classified() {
    let marker = control_batch(9, 2, ControlType::Commit, 7, 0).unwrap();
    let (m, _) = Batch::parse(&marker).unwrap();
    assert_eq!(control(&m).unwrap(), Some(Control::Marker(ControlType::Commit, 7)));
    let noop = raft_control_batch(RaftControl::Noop, &[]).unwrap();
    assert_eq!(control(&Batch::parse(&noop).unwrap().0).unwrap(), Some(Control::Raft(RaftControl::Noop, Bytes::new())));
    let txn = internal_control_batch(InternalControl::TxnState, b"state").unwrap();
    let (t, _) = Batch::parse(&txn).unwrap();
    assert_eq!(control(&t).unwrap(), Some(Control::Internal(InternalControl::TxnState, Bytes::from_static(b"state"))));
    assert_eq!(raft_control(&t).unwrap(), None, "a coordinator record is a normal Raft entry");
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    let data = b.finish().unwrap();
    assert_eq!(control(&Batch::parse(&data).unwrap().0).unwrap(), None);
}

#[test]
fn a_batch_can_be_made_transactional() {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    set_transactional(&mut raw, true).unwrap();
    let (batch, _) = Batch::parse(&raw.clone().freeze()).expect("checksum recomputed");
    assert!(batch.attributes().transactional && !batch.attributes().control);
    set_transactional(&mut raw, false).unwrap();
    assert!(!Batch::parse(&raw.freeze()).unwrap().0.attributes().transactional);
}

#[test]
fn ack_and_queue_state_batches_round_trip() {
    let ranges = vec![(0, 3), (7, 8), (1_000_000, 1_000_512)];
    let b = ack_batch(&ranges).unwrap();
    let (batch, _) = Batch::parse(&b).unwrap();
    assert_eq!(batch.attributes().kind, BatchKind::Ack);
    assert!(!batch.attributes().control);
    assert_eq!(ack_ranges(&batch).unwrap(), ranges);
    assert!(ack_batch(&[(5, 5)]).is_err(), "empty ranges are refused");
    let counts = vec![(4, 2), (9, 65_535)];
    let q = queue_state_batch(&counts).unwrap();
    let (qb, _) = Batch::parse(&q).unwrap();
    assert_eq!(queue_state_counts(&qb).unwrap(), counts);
    assert!(ack_ranges(&qb).is_err(), "kinds are not confused");
}

#[test]
fn a_dead_lettered_message_keeps_its_content_and_says_where_it_came_from() {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(
        42,
        Some(b"k"),
        Some(b"v"),
        &[(b"app", Some(b"1")), (dead_letter::REASON, Some(b"stale")), (dead_letter::OFFSET, Some(b"3"))],
    );
    let (src, _) = Batch::parse(&b.finish().unwrap()).unwrap();
    let rec = &src.records().unwrap()[0];
    let info = DeadLetterInfo {
        queue: "jobs",
        queue_id: 9,
        offset: 17,
        floor: 12,
        reason: dead_letter::Reason::MaxDeliveries,
        deliveries: 5,
    };
    let item = Batch::parse(&dead_letter_item(rec, info).unwrap()).unwrap().0;
    let moved = &item.records().unwrap()[0];
    assert_eq!((moved.key.as_deref(), moved.value.as_deref(), moved.timestamp), (Some(&b"k"[..]), Some(&b"v"[..]), 42));
    let header = |name: &[u8]| {
        let all: Vec<_> = moved.headers.iter().filter(|h| h.key[..] == *name).collect();
        assert_eq!(all.len(), 1, "one {:?} header", String::from_utf8_lossy(name));
        all[0].value.clone().unwrap()
    };
    assert_eq!(&header(b"app")[..], b"1");
    assert_eq!(&header(dead_letter::REASON)[..], b"max-deliveries");
    assert_eq!(&header(dead_letter::QUEUE)[..], b"jobs");
    assert_eq!(&header(dead_letter::DELIVERIES)[..], b"5");
    assert_eq!(dead_letter_origin(moved).unwrap(), Origin { queue_id: 9, offset: 17, floor: 12 });

    let joined = Batch::parse(&dead_letter_batch(&[item.clone(), item], 1_000).unwrap()).unwrap().0;
    assert_eq!(joined.attributes().kind, BatchKind::DeadLetter);
    assert_eq!(joined.record_count(), 2);
    assert!(joined.records().unwrap().iter().all(|r| r.timestamp == 1_000));
    assert!(dead_letter_batch(&[src], 0).is_err(), "a record without an origin is refused");
}

#[test]
fn timestamps_move_together() {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(100, None, Some(b"a"), &[]);
    b.push(130, None, Some(b"b"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    set_base_timestamp(&mut raw, 5_000).unwrap();
    let (batch, _) = Batch::parse(&raw.freeze()).expect("checksum recomputed");
    assert_eq!((batch.base_timestamp(), batch.max_timestamp()), (5_000, 5_030));
    assert_eq!(batch.records().unwrap().iter().map(|r| r.timestamp).collect::<Vec<_>>(), vec![5_000, 5_030]);
}

#[test]
fn retire_batches_round_trip() {
    use dead_letter::Reason;
    let entries = vec![(4, Reason::MaxDeliveries, 3), (9, Reason::Expired, 0)];
    let out = Batch::parse(&retire_out_batch(1 << 40, &entries).unwrap()).unwrap().0;
    assert_eq!(out.attributes().kind, BatchKind::Retire);
    assert_eq!(retire(&out).unwrap(), Retire::Out { target: 1 << 40, entries });
    let done = Batch::parse(&retire_done_batch(&[4, 9]).unwrap()).unwrap().0;
    assert_eq!(retire(&done).unwrap(), Retire::Done(vec![4, 9]));
    assert!(ack_ranges(&done).is_err(), "kinds are not confused");
}

#[test]
fn timer_batches_round_trip_and_the_delay_header_reads_only_numbers() {
    let t = vec![
        Timer { offset: 4, delivery_count: 2, due_ms: 1_700_000_000_000 },
        Timer { offset: 9, delivery_count: 65_535, due_ms: -1 },
    ];
    let (batch, _) = Batch::parse(&timer_batch(&t).unwrap()).unwrap();
    assert_eq!(batch.attributes().kind, BatchKind::Timer);
    assert_eq!(timers(&batch).unwrap(), t);
    assert!(queue_state_counts(&batch).is_err(), "kinds are not confused");
    let record = |value: Option<&'static [u8]>| Record {
        offset: 0,
        timestamp: 0,
        key: None,
        value: None,
        headers: vec![Header { key: Bytes::from_static(delay::HEADER), value: value.map(Bytes::from_static) }],
    };
    assert_eq!(delay::of(&record(Some(b"1500"))), 1500);
    for bad in [&b"-5"[..], b"1.5", b"soon", b"", b"4294967296"] {
        assert_eq!(delay::of(&record(Some(bad))), 0, "{bad:?} is no delay");
    }
    assert_eq!(delay::of(&record(None)), 0);
    assert_eq!(delay::of(&record(Some(b"4294967295"))), delay::MAX_MS);
}

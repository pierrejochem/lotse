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
fn empty_batches_cannot_be_built() {
    assert!(BatchBuilder::new(BatchOptions::default()).finish().is_err());
}

#[test]
fn compressed_batches_are_refused_until_codecs_exist() {
    let mut opts = BatchOptions::default();
    opts.attributes.compression = Compression::Zstd;
    let mut b = BatchBuilder::new(opts);
    b.push(0, None, Some(b"x"), &[]);
    assert_eq!(b.finish().unwrap_err(), Error::UnsupportedCompression(Compression::Zstd));
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
    (any::<bool>(), any::<bool>(), 0u8..4).prop_map(|(transactional, control, k)| Attributes {
        compression: Compression::None,
        transactional,
        control,
        kind: [BatchKind::Data, BatchKind::Ack, BatchKind::QueueState, BatchKind::Timer][k as usize],
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

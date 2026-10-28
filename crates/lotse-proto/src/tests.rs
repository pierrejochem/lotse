use bytes::{BufMut, Bytes, BytesMut};

use super::*;

fn requests() -> Vec<Request> {
    vec![
        Request::ApiVersions,
        Request::Metadata(MetadataRequest { names: vec![] }),
        Request::Metadata(MetadataRequest { names: vec!["orders".into(), "päckchen".into()] }),
        Request::Produce(ProduceRequest {
            txn_id: None,
            timeout_ms: 5_000,
            resources: vec![ProduceResource {
                resource_id: 7,
                partitions: vec![
                    ProducePartition { partition: 0, batch: Bytes::from_static(b"batch-0") },
                    ProducePartition { partition: 3, batch: Bytes::new() },
                ],
            }],
        }),
        Request::Produce(ProduceRequest { txn_id: Some("txn-1".into()), timeout_ms: 0, resources: vec![] }),
        Request::Fetch(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            isolation: isolation::COMMITTED,
            resources: vec![FetchResource {
                resource_id: 7,
                partitions: vec![FetchPartition { partition: 1, fetch_offset: 42, leader_term: 3, max_bytes: 4096 }],
            }],
        }),
    ]
}

fn responses() -> Vec<(ApiKey, Response)> {
    vec![
        (
            ApiKey::API_VERSIONS,
            Response::ApiVersions(ApiVersionsResponse {
                versions: ApiKey::SUPPORTED.to_vec(),
                features: vec!["streams".into()],
            }),
        ),
        (ApiKey::METADATA, Response::Metadata(MetadataResponse::default())),
        (
            ApiKey::METADATA,
            Response::Metadata(MetadataResponse {
                brokers: vec![Broker { node_id: 1, host: "n1".into(), port: 9092, rack: String::new() }],
                resources: vec![
                    ResourceMetadata {
                        error: ErrorCode::NONE,
                        name: "orders".into(),
                        resource_id: 7,
                        kind: kind::STREAM,
                        partitions: vec![PartitionMetadata {
                            partition: 0,
                            leader: 1,
                            leader_term: 4,
                            voters: vec![1, 2, 3],
                        }],
                    },
                    ResourceMetadata {
                        error: ErrorCode::UNKNOWN_RESOURCE,
                        name: "missing".into(),
                        resource_id: 0,
                        kind: kind::STREAM,
                        partitions: vec![],
                    },
                ],
            }),
        ),
        (
            ApiKey::PRODUCE,
            Response::Produce(ProduceResponse {
                throttle_ms: 0,
                resources: vec![ProduceResourceResponse {
                    resource_id: 7,
                    partitions: vec![
                        ProducePartitionResponse {
                            partition: 0,
                            error: ErrorCode::NONE,
                            base_offset: 100,
                            append_ts: -1,
                            error_msg: None,
                            leader_hint: None,
                        },
                        ProducePartitionResponse {
                            partition: 1,
                            error: ErrorCode::NOT_LEADER,
                            base_offset: 0,
                            append_ts: -1,
                            error_msg: Some("not the leader".into()),
                            leader_hint: Some(LeaderHint { leader: 2, term: 9 }),
                        },
                    ],
                }],
            }),
        ),
        (
            ApiKey::FETCH,
            Response::Fetch(FetchResponse {
                throttle_ms: 10,
                resources: vec![FetchResourceResponse {
                    resource_id: 7,
                    partitions: vec![FetchPartitionResponse {
                        partition: 1,
                        error: ErrorCode::NONE,
                        high_watermark: 50,
                        last_stable_offset: 50,
                        log_start_offset: 0,
                        aborted_txns: vec![AbortedTxn { producer_id: 5, first_offset: 44 }],
                        records: Bytes::from_static(b"raw segment bytes"),
                        leader_hint: None,
                    }],
                }],
            }),
        ),
        (ApiKey::FETCH, Response::Error(ErrorCode::UNSUPPORTED_VERSION)),
        (ApiKey::METADATA, Response::Error(ErrorCode::CORRUPT_BATCH)),
    ]
}

#[test]
fn every_request_round_trips() {
    for (i, req) in requests().into_iter().enumerate() {
        let frame = encode_request(i as u32 + 1, &req);
        let (header, back) = decode_request(frame).unwrap();
        assert_eq!(back, req);
        assert_eq!((header.api_key, header.api_version, header.correlation_id), (req.api_key(), 0, i as u32 + 1));
    }
}

#[test]
fn every_response_round_trips() {
    for (i, (key, resp)) in responses().into_iter().enumerate() {
        let frame = encode_response(i as u32, &resp);
        assert_eq!(response_correlation_id(&frame).unwrap(), i as u32);
        assert_eq!(decode_response(frame, key).unwrap(), (i as u32, resp));
    }
}

#[test]
fn records_are_not_copied_on_decode() {
    let records = Bytes::from(vec![9u8; 4096]);
    let resp = Response::Fetch(FetchResponse {
        throttle_ms: 0,
        resources: vec![FetchResourceResponse {
            resource_id: 1,
            partitions: vec![FetchPartitionResponse {
                partition: 0,
                error: ErrorCode::NONE,
                high_watermark: 1,
                last_stable_offset: 1,
                log_start_offset: 0,
                aborted_txns: vec![],
                records,
                leader_hint: None,
            }],
        }],
    });
    let frame = encode_response(1, &resp);
    let start = frame.as_ptr() as usize;
    let end = start + frame.len();
    let (_, Response::Fetch(back)) = decode_response(frame, ApiKey::FETCH).unwrap() else { panic!() };
    let p = back.resources[0].partitions[0].records.as_ptr() as usize;
    assert!((start..end).contains(&p), "records point into the frame");
}

#[test]
fn an_unknown_key_or_version_is_reported_with_its_header() {
    let mut frame = BytesMut::from(&encode_request(77, &Request::ApiVersions)[..]);
    frame[2..4].copy_from_slice(&9u16.to_le_bytes()); // version 9
    match decode_request(frame.freeze()) {
        Err(DecodeError::Unsupported(h)) => assert_eq!((h.correlation_id, h.api_version), (77, 9)),
        other => panic!("{other:?}"),
    }
    let mut frame = BytesMut::from(&encode_request(5, &Request::ApiVersions)[..]);
    frame[0..2].copy_from_slice(&ApiKey::FIND_COORDINATOR.0.to_le_bytes());
    assert!(matches!(decode_request(frame.freeze()), Err(DecodeError::Unsupported(_))));
}

#[test]
fn damaged_frames_are_refused_not_misread() {
    for req in requests() {
        let frame = encode_request(1, &req);
        for cut in 0..frame.len() {
            assert!(decode_request(frame.slice(..cut)).is_err(), "{req:?} cut at {cut}");
        }
        let mut longer = BytesMut::from(&frame[..]);
        longer.put_u8(0);
        assert!(decode_request(longer.freeze()).is_err(), "trailing bytes");
    }
    for (key, resp) in responses() {
        let frame = encode_response(1, &resp);
        for cut in 0..frame.len() {
            assert!(decode_response(frame.slice(..cut), key).is_err(), "{resp:?} cut at {cut}");
        }
    }
    // A count the frame cannot hold is refused before allocating.
    let mut w = codec::Writer::new();
    w.varint(i64::MAX / 2);
    let mut r = codec::Reader::new(w.finish());
    assert!(r.array(1, codec::Reader::u8).is_err());
}

#[test]
fn unknown_tags_are_skipped() {
    let mut w = codec::Writer::new();
    w.u32(1);
    w.u16(0);
    w.tags(&[(42, Bytes::from_static(b"from the future"))]);
    MetadataResponse::default().put(&mut w);
    assert_eq!(
        decode_response(w.finish(), ApiKey::METADATA).unwrap(),
        (1, Response::Metadata(MetadataResponse::default()))
    );
}

#[test]
fn error_codes_have_names_and_retry_classes() {
    assert_eq!(ErrorCode::NOT_LEADER.name(), "NOT_LEADER");
    assert_eq!(ErrorCode::UNSUPPORTED_VERSION.name(), "UNSUPPORTED_VERSION");
    assert_eq!(ErrorCode(999).name(), "UNKNOWN");
    assert!(ErrorCode::NOT_LEADER.retriable() && ErrorCode::REQUEST_TIMED_OUT.retriable());
    assert!(!ErrorCode::CORRUPT_BATCH.retriable() && !ErrorCode::UNKNOWN_RESOURCE.retriable());
}

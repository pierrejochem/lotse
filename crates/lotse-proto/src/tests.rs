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
        Request::FindCoordinator(FindCoordinatorRequest { key_type: coordinator::PRODUCER, key: String::new() }),
        Request::InitProducer(InitProducerRequest { txn_id: None, txn_timeout_ms: 60_000 }),
        Request::InitProducer(InitProducerRequest { txn_id: Some("payments".into()), txn_timeout_ms: 0 }),
        Request::AddPartitionsToTxn(AddPartitionsToTxnRequest {
            txn_id: "payments".into(),
            producer_id: 17,
            producer_epoch: 2,
            resources: vec![TxnResource { resource_id: 7, partitions: vec![0, 1] }],
        }),
        Request::EndTxn(EndTxnRequest { txn_id: "payments".into(), producer_id: 17, producer_epoch: 2, commit: true }),
        Request::Subscribe(SubscribeRequest {
            queue_id: 9,
            consumer_tag: "worker-1".into(),
            credit_msgs: 100,
            credit_bytes: 1 << 20,
            exclusive: false,
        }),
        Request::Credit(CreditRequest { subscription_id: 3, add_msgs: 10, add_bytes: 4096 }),
        Request::Ack(AckRequest { queue_id: 9, txn_id: None, ranges: vec![(0, 3), (5, 6)] }),
        Request::Nack(NackRequest { queue_id: 9, ranges: vec![(4, 5)], requeue: true, delay_ms: 0 }),
        Request::Unsubscribe(UnsubscribeRequest { subscription_id: 3 }),
        Request::CreateStream(CreateStreamRequest {
            name: "orders".into(),
            partitions: 6,
            replication: 3,
            configs: vec![("retention.ms".into(), "1000".into())],
        }),
        Request::CreateQueue(CreateQueueRequest {
            name: "jobs".into(),
            priority_levels: 1,
            single_active: false,
            dlq: None,
            replication: 3,
            configs: vec![],
        }),
        Request::DeclareExchange(DeclareExchangeRequest {
            name: "events".into(),
            exchange_type: exchange_type::TOPIC,
            configs: vec![],
        }),
        Request::Bind(BindRequest {
            exchange_id: 4,
            queue_id: 5,
            binding_key: "eu.#".into(),
            header_match: vec![(Bytes::from_static(b"k"), None)],
            match_all: true,
        }),
        Request::Unbind(BindRequest {
            exchange_id: 4,
            queue_id: 5,
            binding_key: String::new(),
            header_match: vec![],
            match_all: false,
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            group_id: "billing".into(),
            member_id: String::new(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed: Some(vec![7, 8]),
            owned: None,
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            group_id: "billing".into(),
            member_id: "m1.2".into(),
            member_epoch: -1,
            rebalance_timeout_ms: 30_000,
            subscribed: None,
            owned: Some(vec![TxnResource { resource_id: 7, partitions: vec![0, 2] }]),
        }),
        Request::CommitOffsets(CommitOffsetsRequest {
            group_id: "billing".into(),
            member_id: "m1.2".into(),
            member_epoch: 4,
            resources: vec![OffsetCommitResource {
                resource_id: 7,
                partitions: vec![
                    OffsetCommitPartition { partition: 0, offset: 42, leader_term: 3, metadata: Some("m".into()) },
                    OffsetCommitPartition { partition: 1, offset: 0, leader_term: 0, metadata: None },
                ],
            }],
        }),
        Request::OffsetFetch(OffsetFetchRequest {
            group_id: "billing".into(),
            require_stable: true,
            resources: vec![TxnResource { resource_id: 7, partitions: vec![0] }],
        }),
        Request::AddOffsetsToTxn(AddOffsetsToTxnRequest {
            txn_id: "t".into(),
            producer_id: 9,
            producer_epoch: 2,
            group_id: "billing".into(),
            member_id: "m1.2".into(),
            member_epoch: 4,
            resources: vec![OffsetCommitResource {
                resource_id: 7,
                partitions: vec![
                    OffsetCommitPartition { partition: 0, offset: 42, leader_term: 3, metadata: Some("m".into()) },
                    OffsetCommitPartition { partition: 1, offset: 0, leader_term: 0, metadata: None },
                ],
            }],
        }),
        Request::WriteTxnMarkers(WriteTxnMarkersRequest {
            markers: vec![TxnMarker {
                producer_id: 17,
                producer_epoch: 3,
                commit: false,
                coordinator_epoch: 9,
                resources: vec![TxnResource { resource_id: 7, partitions: vec![1] }],
            }],
        }),
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
                        exchange: None,
                    },
                    ResourceMetadata {
                        error: ErrorCode::NONE,
                        name: "events".into(),
                        resource_id: 12,
                        kind: kind::EXCHANGE,
                        partitions: vec![],
                        exchange: Some(ExchangeMetadata {
                            exchange_type: exchange_type::HEADERS,
                            bindings: vec![BindingMetadata {
                                queue_id: 9,
                                binding_key: "eu.#".into(),
                                header_match: vec![
                                    (Bytes::from_static(b"region"), Some(Bytes::from_static(b"eu"))),
                                    (Bytes::from_static(b"trace"), None),
                                ],
                                match_all: false,
                            }],
                        }),
                    },
                    // The smallest bindings: empty keys, no headers.
                    ResourceMetadata {
                        error: ErrorCode::NONE,
                        name: "f".into(),
                        resource_id: 13,
                        kind: kind::EXCHANGE,
                        partitions: vec![],
                        exchange: Some(ExchangeMetadata {
                            exchange_type: exchange_type::FANOUT,
                            bindings: [10, 11]
                                .map(|queue_id| BindingMetadata {
                                    queue_id,
                                    binding_key: String::new(),
                                    header_match: vec![],
                                    match_all: true,
                                })
                                .to_vec(),
                        }),
                    },
                    ResourceMetadata {
                        error: ErrorCode::UNKNOWN_RESOURCE,
                        name: "missing".into(),
                        resource_id: 0,
                        kind: kind::STREAM,
                        partitions: vec![],
                        exchange: None,
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
        (
            ApiKey::FIND_COORDINATOR,
            Response::FindCoordinator(FindCoordinatorResponse {
                error: ErrorCode::NONE,
                node_id: 2,
                host: "n2".into(),
                port: 9000,
                error_msg: None,
            }),
        ),
        (
            ApiKey::FIND_COORDINATOR,
            Response::FindCoordinator(FindCoordinatorResponse {
                error: ErrorCode::COORDINATOR_NOT_AVAILABLE,
                node_id: 0,
                host: String::new(),
                port: 0,
                error_msg: Some("no leader".into()),
            }),
        ),
        (
            ApiKey::INIT_PRODUCER,
            Response::InitProducer(InitProducerResponse {
                error: ErrorCode::NONE,
                producer_id: 4711,
                producer_epoch: 0,
                leader_hint: None,
            }),
        ),
        (
            ApiKey::INIT_PRODUCER,
            Response::InitProducer(InitProducerResponse {
                error: ErrorCode::NOT_LEADER,
                producer_id: 0,
                producer_epoch: 0,
                leader_hint: Some(LeaderHint { leader: 3, term: 2 }),
            }),
        ),
        (
            ApiKey::ADD_PARTITIONS_TO_TXN,
            Response::AddPartitionsToTxn(AddPartitionsToTxnResponse {
                resources: vec![TxnResourceResult {
                    resource_id: 7,
                    partitions: vec![
                        TxnPartitionResult { partition: 0, error: ErrorCode::NONE },
                        TxnPartitionResult { partition: 1, error: ErrorCode::CONCURRENT_TRANSACTIONS },
                    ],
                }],
            }),
        ),
        (
            ApiKey::END_TXN,
            Response::EndTxn(EndTxnResponse { error: ErrorCode::NONE, producer_id: 17, producer_epoch: 3 }),
        ),
        (
            ApiKey::WRITE_TXN_MARKERS,
            Response::WriteTxnMarkers(WriteTxnMarkersResponse {
                markers: vec![MarkerResult {
                    producer_id: 17,
                    resources: vec![TxnResourceResult {
                        resource_id: 7,
                        partitions: vec![TxnPartitionResult { partition: 1, error: ErrorCode::COORDINATOR_FENCED }],
                    }],
                }],
            }),
        ),
        (
            ApiKey::SUBSCRIBE,
            Response::Subscribe(SubscribeResponse { error: ErrorCode::NONE, subscription_id: 3, leader_hint: None }),
        ),
        (ApiKey::ACK, Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None })),
        (
            ApiKey::CREATE_QUEUE,
            Response::Create(CreateResponse {
                error: ErrorCode::RESOURCE_EXISTS,
                resource_id: 1 << 40,
                error_msg: Some("jobs exists".into()),
                leader_hint: Some(LeaderHint { leader: 2, term: 3 }),
            }),
        ),
        (ApiKey::BIND, Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None })),
        (
            ApiKey::GROUP_HEARTBEAT,
            Response::GroupHeartbeat(GroupHeartbeatResponse {
                throttle_ms: 0,
                error: ErrorCode::NONE,
                member_id: "m1.2".into(),
                member_epoch: 5,
                heartbeat_interval_ms: 3_000,
                assignment: Some(vec![TxnResource { resource_id: 7, partitions: vec![1] }]),
            }),
        ),
        (
            ApiKey::ADD_OFFSETS_TO_TXN,
            Response::CommitOffsets(CommitOffsetsResponse {
                resources: vec![TxnResourceResult {
                    resource_id: 7,
                    partitions: vec![TxnPartitionResult { partition: 0, error: ErrorCode::UNKNOWN_MEMBER_ID }],
                }],
            }),
        ),
        (
            ApiKey::OFFSET_FETCH,
            Response::OffsetFetch(OffsetFetchResponse {
                resources: vec![OffsetFetchResource {
                    resource_id: 7,
                    partitions: vec![
                        OffsetFetchPartition {
                            partition: 0,
                            error: ErrorCode::NONE,
                            offset: 42,
                            leader_term: 3,
                            metadata: Some("m".into()),
                        },
                        OffsetFetchPartition {
                            partition: 1,
                            error: ErrorCode::UNSTABLE_OFFSET_COMMIT,
                            offset: NO_OFFSET,
                            leader_term: 0,
                            metadata: None,
                        },
                    ],
                }],
            }),
        ),
        (
            ApiKey::NACK,
            Response::Outcome(OutcomeResponse {
                error: ErrorCode::NOT_LEADER,
                leader_hint: Some(LeaderHint { leader: 2, term: 4 }),
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
    frame[0..2].copy_from_slice(&ApiKey(5).0.to_le_bytes()); // ListOffsets: not yet
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
    assert_eq!(ErrorCode::COORDINATOR_NOT_AVAILABLE.name(), "COORDINATOR_NOT_AVAILABLE");
    assert!(ErrorCode::COORDINATOR_NOT_AVAILABLE.retriable() && ErrorCode::CONCURRENT_TRANSACTIONS.retriable());
    assert_eq!(ErrorCode::COORDINATOR_FENCED.name(), "COORDINATOR_FENCED");
    assert_eq!(ErrorCode::ACK_TIMED_OUT.name(), "ACK_TIMED_OUT");
    assert_eq!(ErrorCode(999).name(), "UNKNOWN");
    assert!(ErrorCode::NOT_LEADER.retriable() && ErrorCode::REQUEST_TIMED_OUT.retriable());
    assert!(!ErrorCode::CORRUPT_BATCH.retriable() && !ErrorCode::UNKNOWN_RESOURCE.retriable());
}

#[test]
fn pushes_round_trip_and_are_told_from_responses() {
    let push = DeliverPush {
        subscription_id: 3,
        error: ErrorCode::NONE,
        batch: Bytes::from_static(b"a batch as stored"),
        deliveries: vec![
            DeliveredOffset { offset: 40, delivery_count: 1 },
            DeliveredOffset { offset: 42, delivery_count: 3 },
        ],
        leader_hint: None,
    };
    let frame = encode_push(&push);
    assert!(is_push(&frame));
    assert_eq!(decode_push(frame.clone()).unwrap(), push);
    for cut in 0..frame.len() {
        assert!(decode_push(frame.slice(..cut)).is_err(), "cut at {cut}");
    }
    let ended = DeliverPush {
        subscription_id: 3,
        error: ErrorCode::NOT_LEADER,
        batch: Bytes::new(),
        deliveries: vec![],
        leader_hint: Some(LeaderHint { leader: 1, term: 7 }),
    };
    assert_eq!(decode_push(encode_push(&ended)).unwrap(), ended);
    let resp = encode_response(5, &Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None }));
    assert!(!is_push(&resp), "a response is not a push");
}

/// Array counts are checked against the smallest encoding of an element:
/// a response of nothing but minimal bindings must still decode.
#[test]
fn the_smallest_bindings_decode() {
    let minimal =
        |queue_id| BindingMetadata { queue_id, binding_key: String::new(), header_match: vec![], match_all: true };
    let resp = Response::Metadata(MetadataResponse {
        brokers: vec![],
        resources: vec![ResourceMetadata {
            error: ErrorCode::NONE,
            name: String::new(),
            resource_id: 1,
            kind: kind::EXCHANGE,
            partitions: vec![],
            exchange: Some(ExchangeMetadata {
                exchange_type: exchange_type::FANOUT,
                bindings: vec![minimal(1), minimal(2)],
            }),
        }],
    });
    let frame = encode_response(1, &resp);
    assert_eq!(decode_response(frame, ApiKey::METADATA).unwrap().1, resp);
}

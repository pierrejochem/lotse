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
            tenant: "shop".into(),
            partitions: 6,
            replication: 3,
            configs: vec![("retention.ms".into(), "1000".into())],
        }),
        Request::CreateQueue(CreateQueueRequest {
            name: "jobs".into(),
            tenant: "default".into(),
            priority_levels: 1,
            single_active: false,
            dlq: None,
            replication: 3,
            configs: vec![],
        }),
        Request::DeclareExchange(DeclareExchangeRequest {
            name: "events".into(),
            tenant: String::new(),
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
        Request::DeleteResource(DeleteResourceRequest { resource_id: 1 << 40, if_empty: true }),
        Request::DescribeConfigs(DescribeConfigsRequest { resource_ids: vec![7, 1 << 40] }),
        Request::DescribeConfigs(DescribeConfigsRequest { resource_ids: vec![] }),
        Request::AlterConfigs(AlterConfigsRequest {
            resource_id: 1 << 40,
            set: vec![("ack.timeout.ms".into(), "5000".into())],
            delete: vec![String::new(), "x".into()],
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
        Request::AddNode(AddNodeRequest { node_id: 4, peer: "10.0.0.4:7000".into(), client: "10.0.0.4:9000".into() }),
        Request::RemoveNode(RemoveNodeRequest { node_id: 2 }),
        Request::DescribeNodes(DescribeNodesRequest),
        Request::SettleVoters(SettleVotersRequest { group: 1_099_511_627_776, voters: vec![1, 3, 4] }),
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
        Request::SaslHandshake(SaslHandshakeRequest { mechanism: "SCRAM-SHA-512".into() }),
        Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: Bytes::from_static(b"n,,n=alice,r=x") }),
        Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: Bytes::new() }),
        Request::AlterUser(AlterUserRequest {
            user: "alice".into(),
            credential: Some(ScramCredentialData {
                iterations: 4096,
                salt: Bytes::from_static(b"salt"),
                stored_key: Bytes::from(vec![1u8; 64]),
                server_key: Bytes::from(vec![2u8; 64]),
            }),
        }),
        Request::AlterUser(AlterUserRequest { user: "bob".into(), credential: None }),
        Request::DescribeUsers(DescribeUsersRequest),
        Request::CreateTenant(CreateTenantRequest { name: "shop".into() }),
        Request::DeleteTenant(DeleteTenantRequest { name: "shop".into() }),
        Request::DescribeTenants(DescribeTenantsRequest),
        Request::AlterRoleBindings(AlterRoleBindingsRequest {
            add: vec![RoleBindingData {
                subject_kind: subject_kind::GROUP,
                subject: "ops".into(),
                role: role::CONSUMER,
                tenant: "shop".into(),
                prefix: "orders-".into(),
            }],
            remove: vec![RoleBindingData {
                subject_kind: subject_kind::USER,
                subject: "eve".into(),
                role: role::CLUSTER_ADMIN,
                tenant: String::new(),
                prefix: String::new(),
            }],
        }),
        Request::DescribeRoleBindings(DescribeRoleBindingsRequest),
        Request::WhoAmI(WhoAmIRequest),
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
                        tenant: "shop".into(),
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
                        tenant: "shop".into(),
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
                        tenant: "shop".into(),
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
                        tenant: "shop".into(),
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
            ApiKey::DESCRIBE_NODES,
            Response::DescribeNodes(DescribeNodesResponse {
                error: ErrorCode::NONE,
                leader_hint: None,
                nodes: vec![NodeEntry {
                    node_id: 2,
                    peer: "10.0.0.2:7000".into(),
                    client: "10.0.0.2:9000".into(),
                    state: node_state::DRAINING,
                    replicas: 3,
                    moving: 1,
                }],
            }),
        ),
        (
            ApiKey::REMOVE_NODE,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
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
            ApiKey::DELETE_RESOURCE,
            Response::Admin(AdminResponse {
                error: ErrorCode::INVALID_REQUEST,
                error_msg: Some("configured".into()),
                leader_hint: None,
            }),
        ),
        (
            ApiKey::ALTER_CONFIGS,
            Response::Admin(AdminResponse {
                error: ErrorCode::NOT_LEADER,
                error_msg: None,
                leader_hint: Some(LeaderHint { leader: 1, term: 9 }),
            }),
        ),
        (
            ApiKey::DESCRIBE_CONFIGS,
            Response::DescribeConfigs(DescribeConfigsResponse {
                error: ErrorCode::NONE,
                leader_hint: None,
                resources: vec![
                    ResourceConfigs {
                        resource_id: 1 << 40,
                        error: ErrorCode::NONE,
                        configs: vec![ConfigEntry {
                            key: "ack.timeout.ms".into(),
                            value: "5000".into(),
                            source: config_source::DYNAMIC,
                        }],
                    },
                    ResourceConfigs { resource_id: 9, error: ErrorCode::UNKNOWN_RESOURCE, configs: vec![] },
                ],
            }),
        ),
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
        (
            ApiKey::SASL_HANDSHAKE,
            Response::SaslHandshake(SaslHandshakeResponse {
                error: ErrorCode::UNSUPPORTED_SASL_MECHANISM,
                mechanisms: vec!["SCRAM-SHA-512".into(), "OAUTHBEARER".into()],
            }),
        ),
        (
            ApiKey::SASL_AUTHENTICATE,
            Response::SaslAuthenticate(SaslAuthenticateResponse {
                error: ErrorCode::NONE,
                error_msg: None,
                auth_bytes: Bytes::from_static(b"v=abc"),
                session_lifetime_ms: 3_600_000,
            }),
        ),
        (
            ApiKey::SASL_AUTHENTICATE,
            Response::SaslAuthenticate(SaslAuthenticateResponse {
                error: ErrorCode::AUTHENTICATION_FAILED,
                error_msg: Some("authentication failed".into()),
                auth_bytes: Bytes::new(),
                session_lifetime_ms: 0,
            }),
        ),
        (
            ApiKey::ALTER_USER,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
        ),
        (
            ApiKey::DESCRIBE_USERS,
            Response::DescribeUsers(DescribeUsersResponse {
                error: ErrorCode::NONE,
                leader_hint: Some(LeaderHint { leader: 1, term: 2 }),
                users: vec![
                    UserEntry { name: "alice".into(), mechanism: "SCRAM-SHA-512".into(), iterations: 4096, source: 1 },
                    UserEntry { name: "ops".into(), mechanism: "SCRAM-SHA-512".into(), iterations: 8192, source: 2 },
                ],
            }),
        ),
        (
            ApiKey::DESCRIBE_TENANTS,
            Response::DescribeTenants(DescribeTenantsResponse {
                error: ErrorCode::NONE,
                leader_hint: None,
                tenants: vec![
                    TenantEntry { name: "default".into(), source: config_source::STATIC, resources: 3 },
                    TenantEntry { name: "shop".into(), source: config_source::DYNAMIC, resources: 0 },
                ],
            }),
        ),
        (
            ApiKey::DESCRIBE_ROLE_BINDINGS,
            Response::DescribeRoleBindings(DescribeRoleBindingsResponse {
                error: ErrorCode::NOT_LEADER,
                leader_hint: Some(LeaderHint { leader: 3, term: 9 }),
                bindings: vec![(
                    RoleBindingData {
                        subject_kind: subject_kind::USER,
                        subject: "root".into(),
                        role: role::CLUSTER_ADMIN,
                        tenant: String::new(),
                        prefix: String::new(),
                    },
                    config_source::STATIC,
                )],
            }),
        ),
        (
            ApiKey::WHO_AM_I,
            Response::WhoAmI(WhoAmIResponse {
                kind: principal_kind::USER,
                name: "bob".into(),
                groups: vec!["ops".into(), "dev".into()],
                bindings: vec![RoleBindingData {
                    subject_kind: subject_kind::GROUP,
                    subject: "ops".into(),
                    role: role::VIEWER,
                    tenant: "shop".into(),
                    prefix: String::new(),
                }],
            }),
        ),
        (
            ApiKey::CREATE_TENANT,
            Response::Admin(AdminResponse {
                error: ErrorCode::UNAUTHORIZED,
                error_msg: Some("no".into()),
                leader_hint: None,
            }),
        ),
        (ApiKey::PRODUCE, Response::Error(ErrorCode::UNAUTHORIZED)),
        (
            ApiKey::PRODUCE,
            Response::Refused { code: ErrorCode::UNAUTHORIZED, message: "no producer role for orders in shop".into() },
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
            tenant: String::new(),
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

/// The smallest described resources (no settings) and settings (empty
/// strings) must decode too: responses of nothing else.
#[test]
fn the_smallest_described_configs_decode() {
    let none = |resource_id| ResourceConfigs { resource_id, error: ErrorCode::NONE, configs: vec![] };
    let empty = ConfigEntry { key: String::new(), value: String::new(), source: config_source::DEFAULT };
    let only = |resources| {
        Response::DescribeConfigs(DescribeConfigsResponse { error: ErrorCode::NONE, leader_hint: None, resources })
    };
    let entries = ResourceConfigs { resource_id: 3, error: ErrorCode::NONE, configs: vec![empty; 4] };
    for resp in [only((1..=4).map(none).collect()), only(vec![entries])] {
        let frame = encode_response(1, &resp);
        assert_eq!(decode_response(frame, ApiKey::DESCRIBE_CONFIGS).unwrap().1, resp);
    }
}

fn metrics_round_trip(resp: DescribeMetricsResponse) {
    let resp = Response::DescribeMetrics(Box::new(resp));
    let frame = encode_response(3, &resp);
    assert_eq!(decode_response(frame.clone(), ApiKey::DESCRIBE_METRICS).unwrap().1, resp);
    for cut in 0..frame.len() {
        assert!(decode_response(frame.slice(..cut), ApiKey::DESCRIBE_METRICS).is_err(), "cut at {cut}");
    }
}

#[test]
fn metrics_round_trip_in_full() {
    let req = Request::DescribeMetrics(DescribeMetricsRequest);
    assert_eq!(decode_request(encode_request(9, &req)).unwrap().1, req);
    metrics_round_trip(DescribeMetricsResponse {
        error: ErrorCode::NONE,
        node_id: 3,
        version: "0.1.0".into(),
        uptime_ms: 60_000,
        metadata_group: 1,
        coordinator_group: 2,
        connections: 12,
        client_in_bytes: 1 << 30,
        client_out_bytes: 2 << 30,
        peer_in_bytes: 7,
        peer_out_bytes: 8,
        commit_latency: vec![0, 5, 9, 1],
        host: HostMetrics {
            cpu_permille: Some(250),
            memory_bytes: Some(1 << 28),
            memory_limit_bytes: None,
            disk_free_bytes: Some(1 << 40),
            disk_total_bytes: Some(4 << 40),
        },
        partitions: vec![PartitionMetrics {
            group: 1 << 40,
            resource_id: 7,
            partition: 2,
            leader: true,
            term: 4,
            high_watermark: 100,
            last_stable: 98,
            log_start: 0,
            log_end: 101,
            size_bytes: 4096,
            produce_bytes: 3000,
            produce_records: 30,
            fetch_bytes: 9000,
            replicas: vec![ReplicaProgress { node_id: 1, log_end: 101 }, ReplicaProgress { node_id: 2, log_end: 90 }],
        }],
        queues: vec![QueueMetrics {
            resource_id: 8,
            ready: 5,
            in_flight: 2,
            in_open_txns: 1,
            delivered: 40,
            acked: 38,
            consumers: vec![ConsumerMetrics {
                subscription_id: 1,
                peer: "10.0.0.7:51234".into(),
                in_flight: 2,
                credit: 8,
            }],
        }],
        groups: vec![GroupMetrics {
            group_id: "billing".into(),
            epoch: 5,
            members: vec![GroupMemberMetrics {
                member_id: "m1".into(),
                owned: vec![TxnResource { resource_id: 7, partitions: vec![0, 2] }],
            }],
            offsets: vec![CommittedOffset { resource_id: 7, partition: 0, offset: 42 }],
        }],
    });
}

/// Each array of a metrics answer filled with its smallest elements, one
/// array at a time: every element minimum is checked against a frame of
/// nothing else.
#[test]
fn the_smallest_metrics_decode() {
    let partition = PartitionMetrics::default();
    let queue = QueueMetrics::default();
    let group = GroupMetrics::default();
    let base = DescribeMetricsResponse::default();
    let cases = [
        DescribeMetricsResponse { commit_latency: vec![0; 32], ..base.clone() },
        DescribeMetricsResponse { partitions: vec![partition.clone(); 32], ..base.clone() },
        DescribeMetricsResponse {
            partitions: vec![PartitionMetrics {
                replicas: vec![ReplicaProgress { node_id: 0, log_end: 0 }; 32],
                ..partition
            }],
            ..base.clone()
        },
        DescribeMetricsResponse { queues: vec![queue.clone(); 32], ..base.clone() },
        DescribeMetricsResponse {
            queues: vec![QueueMetrics {
                consumers: vec![
                    ConsumerMetrics { subscription_id: 0, peer: String::new(), in_flight: 0, credit: 0 };
                    32
                ],
                ..queue
            }],
            ..base.clone()
        },
        DescribeMetricsResponse { groups: vec![group.clone(); 32], ..base.clone() },
        DescribeMetricsResponse {
            groups: vec![GroupMetrics {
                members: vec![GroupMemberMetrics { member_id: String::new(), owned: vec![] }; 32],
                ..group.clone()
            }],
            ..base.clone()
        },
        DescribeMetricsResponse {
            groups: vec![GroupMetrics {
                offsets: vec![CommittedOffset { resource_id: 0, partition: 0, offset: 0 }; 32],
                ..group
            }],
            ..base
        },
    ];
    for case in cases {
        metrics_round_trip(case);
    }
}

/// The smallest users (empty names) and mechanism lists must decode too.
#[test]
fn the_smallest_users_and_mechanisms_decode() {
    let user = UserEntry { name: String::new(), mechanism: String::new(), iterations: 0, source: 0 };
    let resp = Response::DescribeUsers(DescribeUsersResponse {
        error: ErrorCode::NONE,
        leader_hint: None,
        users: vec![user; 5],
    });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::DESCRIBE_USERS).unwrap().1, resp);
    let resp =
        Response::SaslHandshake(SaslHandshakeResponse { error: ErrorCode::NONE, mechanisms: vec![String::new(); 5] });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::SASL_HANDSHAKE).unwrap().1, resp);
    let bad = {
        let mut w = codec::Writer::new();
        w.string("alice");
        w.u8(2);
        w.no_tags();
        w.finish()
    };
    let mut frame = BytesMut::new();
    frame.put_u16(ApiKey::ALTER_USER.0);
    frame.put_u16(0);
    frame.put_u32(1);
    frame.put_u16(0);
    frame.put_slice(&bad);
    assert!(decode_request(frame.freeze()).is_err(), "a credential flag of 2");
}

/// The smallest tenants and bindings (empty strings) must decode too.
#[test]
fn the_smallest_tenants_and_bindings_decode() {
    let t = TenantEntry { name: String::new(), source: 0, resources: 0 };
    let resp = Response::DescribeTenants(DescribeTenantsResponse {
        error: ErrorCode::NONE,
        leader_hint: None,
        tenants: vec![t; 6],
    });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::DESCRIBE_TENANTS).unwrap().1, resp);
    let b = RoleBindingData {
        subject_kind: 0,
        subject: String::new(),
        role: 0,
        tenant: String::new(),
        prefix: String::new(),
    };
    let resp = Response::DescribeRoleBindings(DescribeRoleBindingsResponse {
        error: ErrorCode::NONE,
        leader_hint: None,
        bindings: vec![(b.clone(), 0); 6],
    });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::DESCRIBE_ROLE_BINDINGS).unwrap().1, resp);
    let resp = Response::WhoAmI(WhoAmIResponse {
        kind: 0,
        name: String::new(),
        groups: vec![String::new(); 4],
        bindings: vec![b.clone(); 6],
    });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::WHO_AM_I).unwrap().1, resp);
    let req = Request::AlterRoleBindings(AlterRoleBindingsRequest { add: vec![b.clone(); 5], remove: vec![b; 5] });
    assert_eq!(decode_request(encode_request(1, &req)).unwrap().1, req);
}

/// Many of the smallest resources (empty names, no partitions) decode.
#[test]
fn the_smallest_resources_decode() {
    let r = ResourceMetadata {
        error: ErrorCode::UNKNOWN_RESOURCE,
        name: String::new(),
        tenant: String::new(),
        resource_id: 0,
        kind: kind::STREAM,
        partitions: vec![],
        exchange: None,
    };
    let resp = Response::Metadata(MetadataResponse { brokers: vec![], resources: vec![r; 7] });
    assert_eq!(decode_response(encode_response(1, &resp), ApiKey::METADATA).unwrap().1, resp);
}

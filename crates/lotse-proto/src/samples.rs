//! Sample requests, responses and pushes: one of every shape the codec has.
//!
//! The round-trip tests run over these, and the `vectors` example writes
//! them as golden bytes for the client SDKs in `sdk/conformance`. Compiled
//! for tests and behind the `samples` feature only; nothing in the broker
//! uses it.

use bytes::Bytes;

use crate::*;

/// One request of every shape, including the nullable and empty cases.
pub fn requests() -> Vec<Request> {
    vec![
        Request::ApiVersions,
        Request::Metadata(MetadataRequest { names: vec![] }),
        Request::Metadata(MetadataRequest { names: vec!["orders".into(), "päckchen".into()] }),
        Request::Produce(ProduceRequest {
            tenant: DEFAULT_TENANT.into(),
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
        Request::Produce(ProduceRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: Some("txn-1".into()),
            timeout_ms: 0,
            resources: vec![],
        }),
        Request::FindCoordinator(FindCoordinatorRequest { key_type: coordinator::PRODUCER, key: String::new() }),
        Request::InitProducer(InitProducerRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: None,
            txn_timeout_ms: 60_000,
        }),
        Request::InitProducer(InitProducerRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: Some("payments".into()),
            txn_timeout_ms: 0,
        }),
        Request::AddPartitionsToTxn(AddPartitionsToTxnRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: "payments".into(),
            producer_id: 17,
            producer_epoch: 2,
            resources: vec![TxnResource { resource_id: 7, partitions: vec![0, 1] }],
        }),
        Request::EndTxn(EndTxnRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: "payments".into(),
            producer_id: 17,
            producer_epoch: 2,
            commit: true,
        }),
        Request::Subscribe(SubscribeRequest {
            queue_id: 9,
            consumer_tag: "worker-1".into(),
            credit_msgs: 100,
            credit_bytes: 1 << 20,
            exclusive: false,
            notify_active: false,
        }),
        Request::Subscribe(SubscribeRequest {
            queue_id: 9,
            consumer_tag: "worker-2".into(),
            credit_msgs: 10,
            credit_bytes: 1 << 20,
            exclusive: false,
            notify_active: true,
        }),
        Request::Credit(CreditRequest { subscription_id: 3, add_msgs: 10, add_bytes: 4096 }),
        Request::Ack(AckRequest::plain(9, vec![(0, 3), (5, 6)])),
        Request::Ack(AckRequest {
            queue_id: 9,
            txn_id: Some("payments".into()),
            tenant: "acme".into(),
            producer: Some((4000, 3)),
            ranges: vec![(7, 9)],
        }),
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
            tenant: DEFAULT_TENANT.into(),
            group_id: "billing".into(),
            member_id: String::new(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed: Some(vec![7, 8]),
            owned: None,
            instance_id: None,
            subscribed_pattern: None,
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            tenant: DEFAULT_TENANT.into(),
            group_id: "billing".into(),
            member_id: "m1.2".into(),
            member_epoch: -1,
            rebalance_timeout_ms: 30_000,
            subscribed: None,
            owned: Some(vec![TxnResource { resource_id: 7, partitions: vec![0, 2] }]),
            instance_id: None,
            subscribed_pattern: None,
        }),
        Request::CommitOffsets(CommitOffsetsRequest {
            tenant: DEFAULT_TENANT.into(),
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
            tenant: DEFAULT_TENANT.into(),
            group_id: "billing".into(),
            require_stable: true,
            resources: vec![TxnResource { resource_id: 7, partitions: vec![0] }],
        }),
        Request::AddOffsetsToTxn(AddOffsetsToTxnRequest {
            tenant: DEFAULT_TENANT.into(),
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
        Request::DeadLetter(DeadLetterRequest { queue_id: 1_099_511_627_777, batches: vec![Bytes::from_static(b"x")] }),
        Request::VerifyTxn(VerifyTxnRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: "orders-app".into(),
            producer_id: 4_000,
            producer_epoch: 3,
            resource_id: 1_099_511_627_776,
            partition: 2,
        }),
        Request::CheckEmpty(CheckEmptyRequest { resource_id: 1_099_511_627_776, partition: 3 }),
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
        Request::DescribeMetrics(DescribeMetricsRequest),
        Request::ListOffsets(ListOffsetsRequest {
            isolation: isolation::COMMITTED,
            resources: vec![ListOffsetsResource {
                resource_id: 7,
                partitions: vec![
                    ListOffsetsPartition { partition: 0, timestamp: list_offsets::EARLIEST },
                    ListOffsetsPartition { partition: 1, timestamp: list_offsets::LATEST },
                    ListOffsetsPartition { partition: 2, timestamp: 1_700_000_000_000 },
                ],
            }],
        }),
        // Ids in a tenant other than the default one carry it as a tag
        // (ADR 0062).
        Request::InitProducer(InitProducerRequest {
            txn_id: Some("payments".into()),
            tenant: "acme".into(),
            txn_timeout_ms: 60_000,
        }),
        Request::Produce(ProduceRequest {
            txn_id: Some("payments".into()),
            tenant: "acme".into(),
            timeout_ms: 5_000,
            resources: vec![],
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            tenant: "acme".into(),
            group_id: "billing".into(),
            member_id: String::new(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed: Some(vec![7]),
            owned: None,
            instance_id: None,
            subscribed_pattern: None,
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            tenant: "acme".into(),
            group_id: "billing".into(),
            member_id: String::new(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed: Some(vec![7]),
            owned: None,
            instance_id: Some("billing-worker-3".into()),
            subscribed_pattern: None,
        }),
        Request::GroupHeartbeat(GroupHeartbeatRequest {
            tenant: DEFAULT_TENANT.into(),
            group_id: "billing".into(),
            member_id: String::new(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed: Some(vec![7]),
            owned: None,
            instance_id: Some("billing-worker-4".into()),
            subscribed_pattern: Some("orders-(eu|us)-.*".into()),
        }),
        Request::AddOffsetsToTxn(AddOffsetsToTxnRequest {
            tenant: "acme".into(),
            txn_id: "payments".into(),
            producer_id: 17,
            producer_epoch: 2,
            group_id: "billing".into(),
            member_id: "m-1".into(),
            member_epoch: 3,
            resources: vec![],
        }),
        Request::PurgeQueue(QueueActionRequest { queue_id: 1_099_511_627_777, up_to: 0 }),
        Request::Requeue(QueueActionRequest { queue_id: 1_099_511_627_778, up_to: 5_000 }),
        Request::MoveLeader(MoveLeaderRequest { resource_id: 1_099_511_627_776, partition: 1, node_id: 3 }),
        Request::MoveLeader(MoveLeaderRequest { resource_id: 1_099_511_627_776, partition: 1, node_id: 0 }),
        Request::CreatePartitions(CreatePartitionsRequest { resource_id: 1_099_511_627_776, count: 12 }),
        Request::Publish(PublishRequest {
            txn_id: None,
            timeout_ms: 5_000,
            entries: vec![
                PublishEntry {
                    exchange_id: 20,
                    routing_key: "job.created".into(),
                    mandatory: true,
                    batch: Bytes::from_static(b"one-record batch"),
                },
                PublishEntry {
                    exchange_id: 21,
                    routing_key: String::new(),
                    mandatory: false,
                    batch: Bytes::from_static(b"another"),
                },
            ],
        }),
        Request::RevokeSessions(RevokeSessionsRequest {
            subject_kind: subject_kind::USER,
            subject: "alice".into(),
            before_ms: 1_700_000_000_000,
        }),
        Request::RevokeSessions(RevokeSessionsRequest {
            subject_kind: subject_kind::GROUP,
            subject: "ops".into(),
            before_ms: 0,
        }),
        Request::RevokeCertificate(RevokeCertificateRequest {
            issuer: Bytes::from_static(&[7; 32]),
            serial: Bytes::from_static(&[0x01, 0xab, 0xcd]),
            not_after_ms: 1_800_000_000_000,
            reason: "laptop lost".into(),
        }),
        Request::RevokeCertificate(RevokeCertificateRequest {
            issuer: Bytes::from_static(&[9; 32]),
            serial: Bytes::from_static(&[0x7f]),
            not_after_ms: 0,
            reason: String::new(),
        }),
        Request::DescribeCertificates(DescribeCertificatesRequest),
        Request::RecordCertificates(RecordCertificatesRequest {
            certificates: vec![
                SeenCertificate {
                    issuer: Bytes::from_static(&[7; 32]),
                    serial: Bytes::from_static(&[0x01, 0xab, 0xcd]),
                    principal: "alice".into(),
                    not_before_ms: 1_700_000_000_000,
                    not_after_ms: 1_800_000_000_000,
                    seen_ms: 1_750_000_000_000,
                },
                SeenCertificate {
                    issuer: Bytes::from_static(&[9; 32]),
                    serial: Bytes::from_static(&[0x7f]),
                    principal: String::new(),
                    not_before_ms: 0,
                    not_after_ms: 1,
                    seen_ms: 2,
                },
            ],
        }),
        Request::RecordCertificates(RecordCertificatesRequest { certificates: Vec::new() }),
        Request::RequestCertificate(RequestCertificateRequest {
            principal: "alice".into(),
            csr: Bytes::from_static(&[0x30, 0x82, 0x01, 0x02, 0x03]),
        }),
        Request::DescribeCertificateRequests(DescribeCertificateRequestsRequest),
        Request::RecordCertificate(RecordCertificateRequest {
            id: Bytes::from_static(&[5; 32]),
            certificate: Bytes::from_static(&[0x30, 0x82, 0x04, 0x05]),
            issuer: Bytes::from_static(&[7; 32]),
            serial: Bytes::from_static(&[0x01, 0xab, 0xcd]),
            not_before_ms: 1_700_000_000_000,
            not_after_ms: 1_800_000_000_000,
        }),
    ]
}

/// One response of every shape, with the API key it answers.
pub fn responses() -> Vec<(ApiKey, Response)> {
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
            Response::Subscribe(SubscribeResponse {
                error: ErrorCode::NONE,
                subscription_id: 3,
                leader_hint: None,
                standby: false,
            }),
        ),
        (
            ApiKey::SUBSCRIBE,
            Response::Subscribe(SubscribeResponse {
                error: ErrorCode::NONE,
                subscription_id: 4,
                leader_hint: None,
                standby: true,
            }),
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
            ApiKey::DESCRIBE_CERTIFICATES,
            Response::DescribeCertificates(DescribeCertificatesResponse {
                error: ErrorCode::NONE,
                leader_hint: Some(LeaderHint { leader: 1, term: 2 }),
                now_ms: 1_750_000_000_000,
                certificates: vec![
                    CertificateEntry {
                        issuer: Bytes::from_static(&[7; 32]),
                        serial: Bytes::from_static(&[0x01, 0xab, 0xcd]),
                        principal: "alice".into(),
                        not_before_ms: 1_700_000_000_000,
                        not_after_ms: 1_800_000_000_000,
                        first_seen_ms: 1_701_000_000_000,
                        last_seen_ms: 1_749_000_000_000,
                        state: certificate_state::VALID,
                        reason: String::new(),
                        revoked_at_ms: 0,
                    },
                    CertificateEntry {
                        issuer: Bytes::from_static(&[9; 32]),
                        serial: Bytes::from_static(&[0x7f]),
                        principal: String::new(),
                        not_before_ms: 0,
                        not_after_ms: 1_800_000_000_000,
                        first_seen_ms: 0,
                        last_seen_ms: 0,
                        state: certificate_state::REVOKED,
                        reason: "laptop lost".into(),
                        revoked_at_ms: 1_749_500_000_000,
                    },
                ],
            }),
        ),
        (
            ApiKey::RECORD_CERTIFICATES,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
        ),
        (
            ApiKey::REQUEST_CERTIFICATE,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
        ),
        (
            ApiKey::RECORD_CERTIFICATE,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
        ),
        (
            ApiKey::DESCRIBE_CERTIFICATE_REQUESTS,
            Response::DescribeCertificateRequests(DescribeCertificateRequestsResponse {
                error: ErrorCode::NONE,
                leader_hint: Some(LeaderHint { leader: 1, term: 2 }),
                now_ms: 1_750_000_000_000,
                requests: vec![
                    CertificateRequestEntry {
                        id: Bytes::from_static(&[5; 32]),
                        principal: "alice".into(),
                        csr: Bytes::from_static(&[0x30, 0x82, 0x01, 0x02, 0x03]),
                        requested_by: "admin".into(),
                        requested_at_ms: 1_749_000_000_000,
                        expires_at_ms: 1_749_604_800_000,
                        state: certificate_request_state::SIGNED,
                        certificate: Bytes::from_static(&[0x30, 0x82, 0x04, 0x05]),
                        signed_at_ms: 1_749_100_000_000,
                    },
                    CertificateRequestEntry {
                        id: Bytes::from_static(&[6; 32]),
                        principal: "bob".into(),
                        csr: Bytes::from_static(&[0x30, 0x01]),
                        requested_by: "admin".into(),
                        requested_at_ms: 1_749_200_000_000,
                        expires_at_ms: 1_749_804_800_000,
                        state: certificate_request_state::PENDING,
                        certificate: Bytes::new(),
                        signed_at_ms: 0,
                    },
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
        (ApiKey::DESCRIBE_METRICS, Response::DescribeMetrics(Box::new(metrics()))),
        (
            ApiKey::SUBSCRIBE,
            Response::Subscribe(SubscribeResponse {
                error: ErrorCode::NOT_LEADER,
                subscription_id: 0,
                leader_hint: Some(LeaderHint { leader: 2, term: 8 }),
                standby: false,
            }),
        ),
        (
            ApiKey::FETCH,
            Response::Fetch(FetchResponse {
                throttle_ms: 0,
                resources: vec![FetchResourceResponse {
                    resource_id: 7,
                    partitions: vec![FetchPartitionResponse {
                        partition: 0,
                        error: ErrorCode::NOT_LEADER,
                        high_watermark: 0,
                        last_stable_offset: 0,
                        log_start_offset: 0,
                        aborted_txns: vec![],
                        records: Bytes::new(),
                        leader_hint: Some(LeaderHint { leader: 3, term: 5 }),
                    }],
                }],
            }),
        ),
        (ApiKey::PRODUCE, Response::Error(ErrorCode::UNAUTHORIZED)),
        (
            ApiKey::PRODUCE,
            Response::Refused { code: ErrorCode::UNAUTHORIZED, message: "no producer role for orders in shop".into() },
        ),
        (ApiKey::FETCH, Response::Error(ErrorCode::UNSUPPORTED_VERSION)),
        (ApiKey::METADATA, Response::Error(ErrorCode::CORRUPT_BATCH)),
        (
            ApiKey::PUBLISH,
            Response::Publish(PublishResponse {
                throttle_ms: 0,
                entries: vec![
                    PublishResult {
                        error: ErrorCode::NONE,
                        routed: vec![
                            RoutedMessage {
                                queue_id: 9,
                                name: "orders".into(),
                                error: ErrorCode::NONE,
                                base_offset: 41,
                            },
                            RoutedMessage {
                                queue_id: 10,
                                name: String::new(),
                                error: ErrorCode::REQUEST_TIMED_OUT,
                                base_offset: 0,
                            },
                        ],
                    },
                    PublishResult { error: ErrorCode::UNROUTABLE, routed: vec![] },
                ],
            }),
        ),
        (
            ApiKey::PUBLISH,
            Response::Publish(PublishResponse {
                throttle_ms: 0,
                entries: vec![PublishResult { error: ErrorCode::UNAUTHORIZED, routed: vec![] }],
            }),
        ),
        (
            ApiKey::LIST_OFFSETS,
            Response::ListOffsets(ListOffsetsResponse {
                resources: vec![ListOffsetsResourceResponse {
                    resource_id: 7,
                    partitions: vec![
                        ListOffsetsPartitionResponse {
                            partition: 0,
                            error: ErrorCode::NONE,
                            offset: 12,
                            timestamp: -1,
                            leader_hint: None,
                        },
                        ListOffsetsPartitionResponse {
                            partition: 2,
                            error: ErrorCode::NONE,
                            offset: 40,
                            timestamp: 1_700_000_000_250,
                            leader_hint: None,
                        },
                        ListOffsetsPartitionResponse {
                            partition: 1,
                            error: ErrorCode::NOT_LEADER,
                            offset: 0,
                            timestamp: -1,
                            leader_hint: Some(LeaderHint { leader: 3, term: 8 }),
                        },
                    ],
                }],
            }),
        ),
        (
            ApiKey::PURGE_QUEUE,
            Response::QueueAction(QueueActionResponse {
                error: ErrorCode::NONE,
                error_msg: None,
                count: 4_096,
                up_to: 9_000,
                more: true,
                leader_hint: None,
            }),
        ),
        (
            ApiKey::REQUEUE,
            Response::QueueAction(QueueActionResponse {
                error: ErrorCode::NOT_LEADER,
                error_msg: Some("not the queue's leader".into()),
                count: 0,
                up_to: 0,
                more: false,
                leader_hint: Some(LeaderHint { leader: 2, term: 4 }),
            }),
        ),
    ]
}

/// A metrics answer with every array and every optional filled.
pub fn metrics() -> DescribeMetricsResponse {
    DescribeMetricsResponse {
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
            tenant: DEFAULT_TENANT.into(),
            group_id: "billing".into(),
            epoch: 5,
            members: vec![GroupMemberMetrics {
                member_id: "m1".into(),
                owned: vec![TxnResource { resource_id: 7, partitions: vec![0, 2] }],
            }],
            offsets: vec![CommittedOffset { resource_id: 7, partition: 0, offset: 42 }],
        }],
    }
}

/// Pushes: a delivery, a subscription that ended, and one that became its
/// queue's single active consumer.
pub fn pushes() -> Vec<DeliverPush> {
    vec![
        DeliverPush {
            subscription_id: 3,
            error: ErrorCode::NONE,
            batch: Bytes::from_static(b"a batch as stored"),
            deliveries: vec![
                DeliveredOffset { offset: 40, delivery_count: 1 },
                DeliveredOffset { offset: 42, delivery_count: 3 },
            ],
            leader_hint: None,
        },
        DeliverPush {
            subscription_id: 3,
            error: ErrorCode::NOT_LEADER,
            batch: Bytes::new(),
            deliveries: vec![],
            leader_hint: Some(LeaderHint { leader: 1, term: 7 }),
        },
        DeliverPush::activated(4),
    ]
}

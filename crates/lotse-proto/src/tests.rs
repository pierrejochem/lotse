use bytes::{BufMut, Bytes, BytesMut};

use super::*;
use crate::samples::{self, requests, responses};

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
fn a_node_says_how_much_metadata_it_has_applied_and_clients_say_nothing() {
    // The tag is internal (ADR 0105): a client's request is as it was.
    let plain = encode_request(1, &Request::DescribeNodes(DescribeNodesRequest::default()));
    assert_eq!(plain.last(), Some(&0), "a client's request ends in an empty tag block, as before");
    let plain = decode_request(plain).unwrap().1;
    assert_eq!(plain, Request::DescribeNodes(DescribeNodesRequest { applied_at_least: None }));
    let req = Request::DescribeNodes(DescribeNodesRequest { applied_at_least: Some(u64::MAX - 1) });
    assert_eq!(decode_request(encode_request(1, &req)).unwrap().1, req);
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
fn a_partition_without_a_pin_or_placement_is_written_as_before() {
    // ADR 0121: the tags appear only when set, so an earlier reader sees
    // the same bytes.
    let partition = |pinned_leader, placed| PartitionMetadata {
        partition: 0,
        leader: 1,
        leader_term: 2,
        voters: vec![1, 2],
        pinned_leader,
        placed,
    };
    let answer = |p: PartitionMetadata| {
        Response::Metadata(MetadataResponse {
            brokers: Vec::new(),
            resources: vec![ResourceMetadata {
                error: ErrorCode::NONE,
                name: "s".into(),
                tenant: "t".into(),
                resource_id: 9,
                kind: kind::STREAM,
                partitions: vec![p],
                exchange: None,
            }],
        })
    };
    let plain = encode_response(1, &answer(partition(0, false)));
    let pinned = encode_response(1, &answer(partition(3, false)));
    let placed = encode_response(1, &answer(partition(0, true)));
    // One tag each: its number (a varint) and its value's length (4 bytes),
    // then a u32 for the pin and nothing for the placement.
    assert_eq!(pinned.len(), plain.len() + 1 + 4 + 4);
    assert_eq!(placed.len(), plain.len() + 1 + 4);
    for (frame, p) in [(plain, partition(0, false)), (pinned, partition(3, false)), (placed, partition(0, true))] {
        assert_eq!(decode_response(frame, ApiKey::METADATA).unwrap().1, answer(p));
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
                preferred_replica: None,
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
    frame[0..2].copy_from_slice(&ApiKey(18).0.to_le_bytes()); // a key the design leaves free
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
    metrics_round_trip(samples::metrics());
    metrics_round_trip(DescribeMetricsResponse { removed_through: Some(1 << 40), ..samples::metrics() });
    metrics_round_trip(DescribeMetricsResponse { removed_through: Some(0), ..DescribeMetricsResponse::default() });
    let responsive = vec![
        GroupResponsive { group: 1, term: 3, nodes: vec![1, 2, 3] },
        GroupResponsive { group: 1 << 33, term: 1 << 40, nodes: Vec::new() },
    ];
    metrics_round_trip(DescribeMetricsResponse { responsive: responsive.clone(), ..samples::metrics() });
    metrics_round_trip(DescribeMetricsResponse {
        removed_through: Some(7),
        responsive,
        ..DescribeMetricsResponse::default()
    });
    // Tags of ADR 0122 only for what has something to say: a partition
    // without an oldest record or open transactions, a consumer holding no
    // bytes and a node that cannot tell its disk add nothing.
    let quiet = DescribeMetricsResponse {
        partitions: vec![PartitionMetrics::default()],
        queues: vec![QueueMetrics {
            consumers: vec![ConsumerMetrics {
                subscription_id: 1,
                peer: String::new(),
                in_flight: 0,
                credit: 0,
                state: ConsumerState::Shared,
                in_flight_bytes: 0,
            }],
            ..QueueMetrics::default()
        }],
        ..DescribeMetricsResponse::default()
    };
    let frame = encode_response(3, &Response::DescribeMetrics(Box::new(quiet)));
    assert_eq!(frame.last(), Some(&0), "no tags");
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
                    ConsumerMetrics {
                        subscription_id: 0,
                        peer: String::new(),
                        in_flight: 0,
                        credit: 0,
                        state: ConsumerState::Shared,
                        in_flight_bytes: 0,
                    };
                    32
                ],
                ..queue.clone()
            }],
            ..base.clone()
        },
        // Tag 3 (ADR 0120): its queues, levels and states.
        DescribeMetricsResponse {
            queues: (0..32).map(|i| QueueMetrics { resource_id: i, ready_level_unread: 1, ..queue.clone() }).collect(),
            ..base.clone()
        },
        DescribeMetricsResponse {
            queues: vec![QueueMetrics { ready_by_level: vec![0; 32], ..queue.clone() }],
            ..base.clone()
        },
        DescribeMetricsResponse {
            queues: vec![QueueMetrics {
                consumers: (0..32)
                    .map(|i| ConsumerMetrics {
                        subscription_id: i,
                        peer: String::new(),
                        in_flight: 0,
                        credit: 0,
                        state: ConsumerState::Standby,
                        in_flight_bytes: 0,
                    })
                    .collect(),
                ..queue.clone()
            }],
            ..base.clone()
        },
        // Tags 4 and 5 (ADR 0122): partitions and consumers by their ids.
        DescribeMetricsResponse {
            partitions: (0..32)
                .map(|i| PartitionMetrics { group: i, open_transactions: 1, ..partition.clone() })
                .collect(),
            ..base.clone()
        },
        DescribeMetricsResponse {
            partitions: vec![PartitionMetrics { oldest_timestamp_ms: Some(0), ..partition.clone() }],
            ..base.clone()
        },
        DescribeMetricsResponse {
            queues: (0..32)
                .map(|i| QueueMetrics {
                    resource_id: i,
                    consumers: vec![ConsumerMetrics {
                        subscription_id: 0,
                        peer: String::new(),
                        in_flight: 0,
                        credit: 0,
                        state: ConsumerState::Shared,
                        in_flight_bytes: 1,
                    }],
                    ..queue.clone()
                })
                .collect(),
            ..base.clone()
        },
        DescribeMetricsResponse { disk: Some(DiskMetrics::default()), ..base.clone() },
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

#[test]
fn token_messages_round_trip() {
    let create = Request::CreateToken(CreateTokenRequest {
        name: "ci".into(),
        owner: "alice".into(),
        lifetime: 2,
        secret_hash: Bytes::from(vec![0xab; 32]),
    });
    let list_all = Request::DescribeTokens(DescribeTokensRequest { owner: None });
    let list_one = Request::DescribeTokens(DescribeTokensRequest { owner: Some("alice".into()) });
    let revoke = Request::RevokeToken(RevokeTokenRequest { id: u64::MAX - 1 });
    for (i, req) in [create, list_all, list_one, revoke].into_iter().enumerate() {
        let key = req.api_key();
        assert!([ApiKey::CREATE_TOKEN, ApiKey::DESCRIBE_TOKENS, ApiKey::REVOKE_TOKEN].contains(&key));
        let (header, back) = decode_request(encode_request(i as u32, &req)).unwrap();
        assert_eq!((back, header.api_key), (req, key));
    }
    assert_eq!((ApiKey::CREATE_TOKEN.0, ApiKey::DESCRIBE_TOKENS.0, ApiKey::REVOKE_TOKEN.0), (66, 67, 68));
    for key in [ApiKey::CREATE_TOKEN, ApiKey::DESCRIBE_TOKENS, ApiKey::REVOKE_TOKEN] {
        assert!(ApiKey::SUPPORTED.iter().any(|r| r.api_key == key), "{key:?} is advertised");
    }

    let responses = [
        (
            ApiKey::CREATE_TOKEN,
            Response::CreateToken(CreateTokenResponse {
                error: ErrorCode::NONE,
                error_msg: None,
                leader_hint: None,
                id: 41,
                expires_at_ms: 1_800_000_000_000,
            }),
        ),
        (
            ApiKey::CREATE_TOKEN,
            Response::CreateToken(CreateTokenResponse {
                error: ErrorCode::NOT_LEADER,
                error_msg: Some("ask the leader".into()),
                leader_hint: Some(LeaderHint { leader: 2, term: 7 }),
                id: 0,
                expires_at_ms: 0,
            }),
        ),
        (
            ApiKey::DESCRIBE_TOKENS,
            Response::DescribeTokens(DescribeTokensResponse {
                error: ErrorCode::NONE,
                leader_hint: Some(LeaderHint { leader: 1, term: 3 }),
                tokens: vec![
                    TokenEntry {
                        id: 41,
                        name: "ci".into(),
                        owner: "alice".into(),
                        created_at_ms: 1_790_000_000_000,
                        expires_at_ms: 1_800_000_000_000,
                        created_by: "admin".into(),
                    },
                    TokenEntry {
                        id: 42,
                        name: String::new(),
                        owner: "bob".into(),
                        created_at_ms: 0,
                        expires_at_ms: u64::MAX,
                        created_by: "bob".into(),
                    },
                ],
            }),
        ),
        (
            ApiKey::DESCRIBE_TOKENS,
            Response::DescribeTokens(DescribeTokensResponse {
                error: ErrorCode::NONE,
                leader_hint: None,
                tokens: vec![],
            }),
        ),
        (
            ApiKey::REVOKE_TOKEN,
            Response::Admin(AdminResponse { error: ErrorCode::NONE, error_msg: None, leader_hint: None }),
        ),
    ];
    for (i, (key, resp)) in responses.into_iter().enumerate() {
        let frame = encode_response(i as u32, &resp);
        assert_eq!(decode_response(frame, key).unwrap(), (i as u32, resp));
    }
}

#[test]
fn a_session_names_its_token() {
    let entry = |mechanism, token_id| SessionEntry {
        principal_kind: session_principal::USER,
        name: "alice".into(),
        groups: vec![],
        mechanism,
        peer: "10.0.0.7:51234".into(),
        connected_ms: 1,
        credential_ms: 2,
        expires_ms: 3,
        token_id,
    };
    let answer = |e: SessionEntry| {
        Response::DescribeSessions(DescribeSessionsResponse {
            error: ErrorCode::NONE,
            node_id: 1,
            now_ms: 5,
            sessions: vec![e],
        })
    };
    assert_eq!(session_mechanism::TOKEN, 4);
    let plain = encode_response(1, &answer(entry(session_mechanism::SCRAM_SHA_512, None)));
    let token = encode_response(1, &answer(entry(session_mechanism::TOKEN, Some(41))));
    // Only the tag differs in size: its number, its length and a u64.
    assert_eq!(token.len(), plain.len() + 1 + 4 + 8);
    for (frame, e) in
        [(plain, entry(session_mechanism::SCRAM_SHA_512, None)), (token, entry(session_mechanism::TOKEN, Some(41)))]
    {
        assert_eq!(decode_response(frame, ApiKey::DESCRIBE_SESSIONS).unwrap().1, answer(e));
    }
}

#[test]
fn frame_tags_round_trip() {
    let indexes = vec![GroupIndex { group: 1, index: 7 }, GroupIndex { group: 2, index: u64::MAX }];
    let request = Request::DescribeNodes(DescribeNodesRequest::default());
    let tags = FrameTags { read_at_least: indexes, applied: vec![] };
    let (header, back, decoded) = decode_request_tagged(encode_request_tagged(9, &tags, &request)).unwrap();
    assert_eq!((header.correlation_id, back, decoded), (9, tags, request.clone()));

    let response = Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None });
    let tags = FrameTags { read_at_least: vec![], applied: vec![GroupIndex { group: 1, index: 9 }] };
    let frame = encode_response_tagged(4, &tags, &response);
    assert_eq!(decode_response_tagged(frame, ApiKey::BIND).unwrap(), (4, tags, response.clone()));

    // A frame without the tags decodes to the default, and the plain functions write exactly that.
    let plain = encode_request(3, &request);
    assert_eq!(plain, encode_request_tagged(3, &FrameTags::default(), &request));
    assert_eq!(decode_request_tagged(plain).unwrap().1, FrameTags::default());
    let plain = encode_response(3, &response);
    assert_eq!(decode_response_tagged(plain, ApiKey::BIND).unwrap().1, FrameTags::default());
}

#[test]
fn a_request_carries_only_read_at_least_and_a_response_only_applied() {
    let request = Request::ApiVersions;
    let tags = FrameTags {
        read_at_least: vec![GroupIndex { group: 1, index: 1 }],
        applied: vec![GroupIndex { group: 2, index: 2 }],
    };
    let back = decode_request_tagged(encode_request_tagged(1, &tags, &request)).unwrap().1;
    assert_eq!(back, FrameTags { read_at_least: tags.read_at_least.clone(), applied: vec![] });
    let response = Response::Error(ErrorCode::UNAUTHORIZED);
    let back = decode_response_tagged(encode_response_tagged(1, &tags, &response), ApiKey::BIND).unwrap().1;
    assert_eq!(back, FrameTags { read_at_least: vec![], applied: tags.applied });
}

#[test]
fn an_unknown_header_tag_is_skipped_and_a_lying_count_refused() {
    // Frame: api key, version, correlation id, flags, then two tags: 5 (unknown) and 0 (indexes).
    let build = |count: u32| {
        let mut w = Writer::new();
        w.u16(ApiKey::API_VERSIONS.0);
        w.u16(0);
        w.u32(1);
        w.u16(0);
        let mut v = Writer::new();
        v.u32(count);
        v.u64(1);
        v.u64(2);
        w.tags(&[(0, v.finish()), (5, Bytes::from_static(b"later"))]);
        w.no_tags();
        w.finish()
    };
    let (_, tags, _) = decode_request_tagged(build(1)).unwrap();
    assert_eq!(tags.read_at_least, vec![GroupIndex { group: 1, index: 2 }]);
    assert!(decode_request_tagged(build(u32::MAX)).is_err());
}

#[test]
fn fetch_zone_and_preferred_replica_round_trip() {
    let fetch = |zone: Option<&str>| {
        Request::Fetch(FetchRequest {
            max_wait_ms: 1,
            min_bytes: 1,
            max_bytes: 1024,
            isolation: isolation::COMMITTED,
            resources: vec![],
            allow_follower: true,
            zone: zone.map(str::to_string),
        })
    };
    let with = fetch(Some("eu-1"));
    assert_eq!(decode_request(encode_request(1, &with)).unwrap().1, with);
    // Without a zone the bytes are what they were before the tag existed: one tag, allow_follower.
    let without = encode_request(1, &fetch(None));
    let mut tail = Writer::new();
    tail.tags(&[(0, Bytes::from_static(&[1]))]);
    let tail = tail.finish();
    assert_eq!(&without[without.len() - tail.len()..], &tail[..], "only the allow_follower tag");
    assert_eq!(decode_request(without).unwrap().1, fetch(None));

    let partition = |hint: Option<LeaderHint>, preferred: Option<u32>| FetchPartitionResponse {
        partition: 0,
        error: ErrorCode::NONE,
        high_watermark: 0,
        last_stable_offset: 0,
        log_start_offset: 0,
        aborted_txns: vec![],
        records: Bytes::new(),
        leader_hint: hint,
        preferred_replica: preferred,
    };
    let answer = |p| {
        Response::Fetch(FetchResponse {
            throttle_ms: 0,
            resources: vec![FetchResourceResponse { resource_id: 1, partitions: vec![p] }],
        })
    };
    for p in [
        partition(None, Some(3)),
        partition(Some(LeaderHint { leader: 2, term: 4 }), Some(3)),
        partition(Some(LeaderHint { leader: 2, term: 4 }), None),
        partition(None, None),
    ] {
        let resp = answer(p);
        assert_eq!(decode_response(encode_response(1, &resp), ApiKey::FETCH).unwrap().1, resp);
    }
    let plain = encode_response(1, &answer(partition(None, None)));
    let placed = encode_response(1, &answer(partition(None, Some(3))));
    // One tag: its number (a varint), its value's length (4 bytes) and the u32 node id.
    assert_eq!(placed.len(), plain.len() + 1 + 4 + 4);
}

#[test]
fn find_coordinator_voters_round_trip() {
    let answer = |voters: Vec<u32>| {
        Response::FindCoordinator(FindCoordinatorResponse {
            error: ErrorCode::NONE,
            node_id: 2,
            host: "n2".into(),
            port: 9000,
            error_msg: None,
            voters,
        })
    };
    let with = answer(vec![1, 3, 5]);
    assert_eq!(decode_response(encode_response(1, &with), ApiKey::FIND_COORDINATOR).unwrap().1, with);
    let empty = encode_response(1, &answer(vec![]));
    assert_eq!(empty.last(), Some(&0), "empty is an empty tag block, as before");
    assert_eq!(decode_response(empty, ApiKey::FIND_COORDINATOR).unwrap().1, answer(vec![]));
}

#[test]
fn a_malformed_header_tag_on_an_unsupported_version_is_still_unsupported() {
    let mut w = Writer::new();
    w.u16(ApiKey::API_VERSIONS.0);
    w.u16(9);
    w.u32(5);
    w.u16(0);
    w.tags(&[(0, Bytes::from_static(&[1]))]); // a count that is not even whole
    w.no_tags();
    assert!(matches!(decode_request_tagged(w.finish()), Err(DecodeError::Unsupported(h)) if h.correlation_id == 5));
}

#[test]
fn quota_messages_round_trip() {
    let set = Request::AlterQuota(AlterQuotaRequest { tenant: "shop".into(), prefix: "".into(), bytes: Some(1 << 30) });
    let set_prefix =
        Request::AlterQuota(AlterQuotaRequest { tenant: "shop".into(), prefix: "orders.".into(), bytes: Some(1) });
    let remove =
        Request::AlterQuota(AlterQuotaRequest { tenant: "shop".into(), prefix: "orders.".into(), bytes: None });
    let all = Request::DescribeQuotas(DescribeQuotasRequest { tenant: None });
    let one = Request::DescribeQuotas(DescribeQuotasRequest { tenant: Some("shop".into()) });
    for (i, req) in [set, set_prefix, remove, all, one].into_iter().enumerate() {
        let key = req.api_key();
        assert!([ApiKey::ALTER_QUOTA, ApiKey::DESCRIBE_QUOTAS].contains(&key));
        let (header, back) = decode_request(encode_request(i as u32, &req)).unwrap();
        assert_eq!((back, header.api_key), (req, key));
    }
    assert_eq!((ApiKey::ALTER_QUOTA.0, ApiKey::DESCRIBE_QUOTAS.0), (69, 70));
    for key in [ApiKey::ALTER_QUOTA, ApiKey::DESCRIBE_QUOTAS] {
        assert!(ApiKey::SUPPORTED.iter().any(|r| r.api_key == key), "{key:?} is advertised");
    }
    let responses = [
        (
            ApiKey::ALTER_QUOTA,
            Response::Admin(AdminResponse {
                error: ErrorCode::INVALID_REQUEST,
                error_msg: Some("a tenant holds at most 100 prefix limits".into()),
                leader_hint: None,
            }),
        ),
        (
            ApiKey::DESCRIBE_QUOTAS,
            Response::DescribeQuotas(DescribeQuotasResponse {
                error: ErrorCode::NONE,
                leader_hint: None,
                counted_at_ms: Some(1_800_000_000_000),
                quotas: vec![
                    QuotaEntry {
                        tenant: "shop".into(),
                        prefix: "".into(),
                        bytes: Some(1 << 30),
                        set_by_cluster_admin: true,
                        full: false,
                        used_bytes: Some(4096),
                    },
                    QuotaEntry {
                        tenant: "shop".into(),
                        prefix: "orders.".into(),
                        bytes: Some(1024),
                        set_by_cluster_admin: false,
                        full: true,
                        used_bytes: Some(2048),
                    },
                    QuotaEntry {
                        tenant: "lab".into(),
                        prefix: "".into(),
                        bytes: None,
                        set_by_cluster_admin: false,
                        full: false,
                        used_bytes: None,
                    },
                ],
            }),
        ),
        (
            ApiKey::DESCRIBE_QUOTAS,
            Response::DescribeQuotas(DescribeQuotasResponse {
                error: ErrorCode::NOT_LEADER,
                leader_hint: Some(LeaderHint { leader: 2, term: 7 }),
                counted_at_ms: None,
                quotas: vec![],
            }),
        ),
    ];
    for (key, resp) in responses {
        assert_eq!(decode_response(encode_response(9, &resp), key).unwrap().1, resp);
    }
}

#[test]
fn quota_exceeded_is_not_retriable() {
    assert!(!ErrorCode::QUOTA_EXCEEDED.retriable());
    assert_eq!(ErrorCode::QUOTA_EXCEEDED.0, 10);
}

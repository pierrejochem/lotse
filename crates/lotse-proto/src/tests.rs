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
    frame[0..2].copy_from_slice(&ApiKey(9).0.to_le_bytes()); // a key the design leaves free
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

use lotse_proto::{ErrorCode, ResourceMetadata, kind};

use super::merge_newest;

fn resource(name: &str, resource_id: u64) -> ResourceMetadata {
    ResourceMetadata {
        error: ErrorCode::NONE,
        name: name.into(),
        tenant: "default".into(),
        resource_id,
        kind: kind::STREAM,
        partitions: vec![],
        exchange: None,
    }
}

/// Brokers that lag report a deleted resource under a name that now names
/// a newer one; whichever broker answers first, the newest wins.
#[test]
fn the_newest_resource_of_a_name_wins() {
    for order in [[(1, 100), (2, 200)], [(2, 200), (1, 100)]] {
        let mut merged = Vec::new();
        for (_, id) in order {
            merge_newest(&mut merged, vec![resource("s", id), resource(&format!("only{id}"), id)]);
        }
        let ids: Vec<(String, u64)> = merged.iter().map(|r| (r.name.clone(), r.resource_id)).collect();
        assert!(ids.contains(&("s".into(), 200)), "{ids:?}");
        assert_eq!(merged.iter().filter(|r| r.name == "s").count(), 1);
        assert_eq!(merged.len(), 3);
    }
    let mut merged = vec![resource("s", 200)];
    merge_newest(&mut merged, vec![ResourceMetadata { error: ErrorCode::UNKNOWN_RESOURCE, ..resource("s", 300) }]);
    assert_eq!(merged[0].resource_id, 200, "an error names nothing");
}

/// A scripted broker: answers each request with what `script` returns.
pub(crate) fn serve(
    rt: &lotse_sim::SimRuntime,
    host: &str,
    script: impl Fn(&lotse_proto::Request) -> lotse_proto::Response + Send + Sync + 'static,
) {
    use lotse_rt::{Conn as _, Listener as _, Net as _, Spawn as _};
    let node = rt.node(host);
    let addr = format!("{host}:9000");
    let script = std::sync::Arc::new(script);
    let spawner = node.clone();
    drop(node.clone().spawn(async move {
        let listener = node.listen(&addr, lotse_rt::Link::Client).await.unwrap();
        while let Ok((conn, _)) = listener.accept().await {
            let script = script.clone();
            drop(spawner.spawn(async move {
                while let Ok(Some(frame)) = conn.recv().await {
                    let (header, request) = lotse_proto::decode_request(frame).unwrap();
                    let response = script(&request);
                    if conn.send(lotse_proto::encode_response(header.correlation_id, &response)).await.is_err() {
                        break;
                    }
                }
            }));
        }
    }));
}

pub(crate) fn brokers(ids: &[(u32, &str)]) -> Vec<lotse_proto::Broker> {
    ids.iter()
        .map(|(id, host)| lotse_proto::Broker { node_id: *id, host: (*host).into(), port: 9000, rack: String::new() })
        .collect()
}

/// A leader the client has no address for is looked up with every broker,
/// not only the first that answers: here that one lags and lists neither
/// the leader nor the node that names it (seed 4638 of the membership
/// simulation, where a node cut off from the metadata group kept answering
/// first).
#[test]
fn a_leader_only_some_brokers_know_is_found() {
    use lotse_proto::{DescribeNodesResponse, LeaderHint, MetadataResponse, NodeEntry, Request, Response};
    lotse_sim::Sim::new(1).run(|rt| async move {
        let nodes =
            |error, leader_hint, nodes| Response::DescribeNodes(DescribeNodesResponse { error, leader_hint, nodes });
        serve(&rt, "a", move |r| match r {
            Request::Metadata(_) => {
                Response::Metadata(MetadataResponse { brokers: brokers(&[(1, "a")]), resources: vec![] })
            }
            _ => nodes(ErrorCode::NOT_LEADER, None, vec![]),
        });
        serve(&rt, "b", move |r| match r {
            Request::Metadata(_) => Response::Metadata(MetadataResponse {
                brokers: brokers(&[(1, "a"), (2, "b"), (3, "c")]),
                resources: vec![],
            }),
            _ => nodes(ErrorCode::NOT_LEADER, Some(LeaderHint { leader: 3, term: 1 }), vec![]),
        });
        serve(&rt, "c", move |r| match r {
            Request::Metadata(_) => Response::Metadata(MetadataResponse {
                brokers: brokers(&[(1, "a"), (2, "b"), (3, "c")]),
                resources: vec![],
            }),
            _ => nodes(
                ErrorCode::NONE,
                None,
                vec![NodeEntry {
                    node_id: 3,
                    peer: "c:7000".into(),
                    client: "c:9000".into(),
                    state: 0,
                    replicas: 0,
                    moving: 0,
                }],
            ),
        });
        let client = crate::Client::new(
            rt.node("client"),
            crate::ClientConfig {
                retry_for: std::time::Duration::from_secs(5),
                ..crate::ClientConfig::new(vec!["a:9000".into(), "b:9000".into()])
            },
        );
        let found = client.describe_nodes().await.expect("the leader is found");
        assert_eq!(found.iter().map(|n| n.node_id).collect::<Vec<_>>(), vec![3]);
    });
}

/// A client with no broker address, such as a node's own client on a node
/// that serves no clients, fails an admin request once `retry_for` runs out
/// instead of panicking.
#[test]
fn an_admin_request_without_any_address_fails() {
    lotse_sim::Sim::new(1).run(|rt| async move {
        let client = crate::Client::new(
            rt.node("client"),
            crate::ClientConfig { retry_for: std::time::Duration::from_secs(1), ..crate::ClientConfig::new(vec![]) },
        );
        let e = client.describe_nodes().await.expect_err("no broker to ask");
        assert!(e.to_string().contains("no broker address"), "{e}");
    });
}

/// A stream that the brokers which answer do not know yet, while another
/// broker does not answer, is not unknown: the silent one may be the
/// metadata leader, the only one that has applied the creation. The client
/// tries again until it hears from that one (seed 2874 of the compaction
/// simulation, where the leader was down right after creating the stream).
#[test]
fn a_stream_is_unknown_only_once_every_broker_says_so() {
    use lotse_proto::{
        MetadataResponse, PartitionMetadata, ProducePartitionResponse, ProduceResourceResponse, ProduceResponse,
        Request, Response,
    };
    use lotse_rt::{Clock as _, Spawn as _};
    lotse_sim::Sim::new(1).run(|rt| async move {
        let all = brokers(&[(1, "a"), (2, "b")]);
        let behind = all.clone();
        serve(&rt, "a", move |_| {
            Response::Metadata(MetadataResponse {
                brokers: behind.clone(),
                resources: vec![ResourceMetadata { error: ErrorCode::UNKNOWN_RESOURCE, ..resource("s", 0) }],
            })
        });
        // b, the one that knows the stream, comes up only after a second.
        let late = rt.clone();
        drop(rt.spawn(async move {
            late.sleep(std::time::Duration::from_secs(1)).await;
            serve(&late, "b", move |r| match r {
                Request::Produce(_) => Response::Produce(ProduceResponse {
                    throttle_ms: 0,
                    resources: vec![ProduceResourceResponse {
                        resource_id: 7,
                        partitions: vec![ProducePartitionResponse {
                            partition: 0,
                            error: ErrorCode::NONE,
                            base_offset: 41,
                            append_ts: -1,
                            error_msg: None,
                            leader_hint: None,
                        }],
                    }],
                }),
                _ => Response::Metadata(MetadataResponse {
                    brokers: all.clone(),
                    resources: vec![ResourceMetadata {
                        partitions: vec![PartitionMetadata {
                            partition: 0,
                            leader: 2,
                            leader_term: 1,
                            voters: vec![1, 2],
                            pinned_leader: 0,
                            placed: false,
                        }],
                        ..resource("s", 7)
                    }],
                }),
            });
        }));
        let client = crate::Client::new(
            rt.node("client"),
            crate::ClientConfig {
                retry_for: std::time::Duration::from_secs(5),
                idempotent: false,
                ..crate::ClientConfig::new(vec!["a:9000".into(), "b:9000".into()])
            },
        );
        let mut b = lotse_record::BatchBuilder::new(lotse_record::BatchOptions::default());
        b.push(0, None, Some(b"v"), &[]);
        let offset = client.produce("s", 0, b.finish().unwrap()).await.expect("b answers in time");
        assert_eq!(offset, 41);
    });
}

/// What `create_token` sends is the hash of the secret and nothing else of
/// it: the cluster never sees the secret, and the text the caller gets
/// carries the id the cluster chose.
#[test]
fn create_token_sends_only_the_hash() {
    use lotse_auth::token::{Lifetime, Token};
    use lotse_proto::{CreateTokenResponse, Request, Response};
    use std::sync::{Arc, Mutex};
    lotse_sim::Sim::new(1).run(|rt| async move {
        let seen: Arc<Mutex<Vec<lotse_proto::CreateTokenRequest>>> = Arc::default();
        let frames: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let (s, f) = (seen.clone(), frames.clone());
        serve(&rt, "a", move |r| match r {
            Request::CreateToken(c) => {
                s.lock().unwrap().push(c.clone());
                f.lock().unwrap().push(lotse_proto::encode_request(1, r).to_vec());
                Response::CreateToken(CreateTokenResponse { id: 0x2a, expires_at_ms: 5_000, ..Default::default() })
            }
            _ => Response::Error(ErrorCode::INVALID_REQUEST),
        });
        let client = crate::Client::new(
            rt.node("client"),
            crate::ClientConfig {
                retry_for: std::time::Duration::from_secs(2),
                ..crate::ClientConfig::new(vec!["a:9000".into()])
            },
        );
        let created = client.create_token("laptop", None, Lifetime::Month).await.expect("created");
        assert_eq!((created.id, created.expires_at_ms), (0x2a, 5_000));
        let token = Token::parse(&created.text).expect("the text is a token");
        assert_eq!(token.id, 0x2a, "the id is the cluster's");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let req = &seen[0];
        assert_eq!((req.name.as_str(), req.owner.as_str(), req.lifetime), ("laptop", "", Lifetime::Month.code()));
        assert_eq!(req.secret_hash.len(), 32);
        assert_eq!(&req.secret_hash[..], &token.hash()[..], "the hash of the secret");
        assert_ne!(&req.secret_hash[..], &token.secret[..]);
        let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
        for frame in frames.lock().unwrap().iter() {
            assert!(!contains(frame, &token.secret), "the secret is in the frame");
            assert!(!contains(frame, created.text.as_bytes()), "the text is in the frame");
        }
    });
}

/// A token's text is a secret: printing credentials must not show it.
#[test]
fn token_credentials_are_not_printed() {
    let text = "lotse_tok_000000000000002a_c3VwZXJzZWNyZXRzdXBlcnNlY3JldHN1cGVyc2VjcmV0MTI";
    let shown = format!("{:?}", crate::Credentials::Token(text.into()));
    assert!(!shown.contains("c3VwZXJ"), "{shown}");
    assert!(!shown.contains(text), "{shown}");
    assert!(shown.contains("Token"), "{shown}");
}

/// Reads in the client's zone (the zone reads design): scripted nodes 1
/// (`a`, zone `x`, the leader of every group), 2 (`b`, zone `y`) and 3
/// (`c`, zone `y`), each recording what it heard.
mod zone_reads {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use lotse_proto::{
        ErrorCode, FetchPartitionResponse, FetchResourceResponse, FetchResponse, FindCoordinatorResponse, GroupIndex,
        LeaderHint, ListOffsetsPartitionResponse, ListOffsetsResourceResponse, ListOffsetsResponse, MetadataResponse,
        OffsetFetchPartition, OffsetFetchResource, OffsetFetchResponse, PartitionMetadata, Request, ResourceMetadata,
        Response, kind,
    };

    /// What a node heard: its host, the request's `read_at_least` and the
    /// request.
    type Heard = Arc<Mutex<Vec<(String, Vec<GroupIndex>, Request)>>>;

    /// A scripted node whose answers carry the `applied` its script gives.
    fn serve(
        rt: &lotse_sim::SimRuntime,
        host: &'static str,
        heard: &Heard,
        script: impl Fn(&Request) -> (Vec<GroupIndex>, Response) + Send + Sync + 'static,
    ) {
        use lotse_rt::{Conn as _, Listener as _, Net as _, Spawn as _};
        let node = rt.node(host);
        let script = Arc::new(script);
        let heard = heard.clone();
        let spawner = node.clone();
        drop(node.clone().spawn(async move {
            let listener = node.listen(&format!("{host}:9000"), lotse_rt::Link::Client).await.unwrap();
            while let Ok((conn, _)) = listener.accept().await {
                let (script, heard) = (script.clone(), heard.clone());
                drop(spawner.spawn(async move {
                    while let Ok(Some(frame)) = conn.recv().await {
                        let (header, tags, request) = lotse_proto::decode_request_tagged(frame).unwrap();
                        heard.lock().unwrap().push((host.to_string(), tags.read_at_least, request.clone()));
                        let (applied, response) = script(&request);
                        let tags = lotse_proto::FrameTags { read_at_least: vec![], applied };
                        let frame = lotse_proto::encode_response_tagged(header.correlation_id, &tags, &response);
                        if conn.send(frame).await.is_err() {
                            break;
                        }
                    }
                }));
            }
        }));
    }

    fn at(entries: &[(u64, u64)]) -> Vec<GroupIndex> {
        entries.iter().map(|&(group, index)| GroupIndex { group, index }).collect()
    }

    const HINT: Option<LeaderHint> = Some(LeaderHint { leader: 1, term: 1 });

    /// Stream `s` (id 7) lives on node 1 alone, stream `t` (id 8) on nodes
    /// 1 and 2; node 1 leads both.
    fn metadata() -> Response {
        metadata_of(&[1, 2, 3])
    }

    /// [`metadata`] listing only the nodes `ids`.
    fn metadata_of(ids: &[u32]) -> Response {
        let stream = |name: &str, resource_id, voters: Vec<u32>| ResourceMetadata {
            error: ErrorCode::NONE,
            name: name.into(),
            tenant: "default".into(),
            resource_id,
            kind: kind::STREAM,
            partitions: vec![PartitionMetadata {
                partition: 0,
                leader: 1,
                leader_term: 1,
                voters,
                pinned_leader: 0,
                placed: false,
            }],
            exchange: None,
        };
        let racks = [(1, "a", "x"), (2, "b", "y"), (3, "c", "y")];
        Response::Metadata(MetadataResponse {
            brokers: racks
                .iter()
                .filter(|(id, _, _)| ids.contains(id))
                .map(|(id, host, rack)| lotse_proto::Broker {
                    node_id: *id,
                    host: (*host).into(),
                    port: 9000,
                    rack: (*rack).into(),
                })
                .collect(),
            resources: vec![stream("s", 7, vec![1]), stream("t", 8, vec![1, 2])],
        })
    }

    fn fetched(error: ErrorCode, preferred_replica: Option<u32>) -> Response {
        Response::Fetch(FetchResponse {
            throttle_ms: 0,
            resources: vec![FetchResourceResponse {
                resource_id: 0,
                partitions: vec![FetchPartitionResponse {
                    partition: 0,
                    error,
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: 0,
                    aborted_txns: vec![],
                    records: bytes::Bytes::new(),
                    leader_hint: None,
                    preferred_replica,
                }],
            }],
        })
    }

    /// The answer to a zone read, with `error` (and then the leader's
    /// hint) or without.
    fn read(req: &Request, error: ErrorCode) -> Response {
        let hint = if error == ErrorCode::NONE { None } else { HINT };
        match req {
            Request::DescribeConfigs(d) => Response::DescribeConfigs(lotse_proto::DescribeConfigsResponse {
                error,
                leader_hint: hint,
                resources: d
                    .resource_ids
                    .iter()
                    .map(|&resource_id| lotse_proto::ResourceConfigs {
                        resource_id,
                        error: ErrorCode::NONE,
                        configs: vec![],
                    })
                    .collect(),
            }),
            Request::DescribeNodes(_) => {
                Response::DescribeNodes(lotse_proto::DescribeNodesResponse { error, leader_hint: hint, nodes: vec![] })
            }
            Request::DescribeUsers(_) => {
                Response::DescribeUsers(lotse_proto::DescribeUsersResponse { error, leader_hint: hint, users: vec![] })
            }
            Request::DescribeTokens(_) => Response::DescribeTokens(lotse_proto::DescribeTokensResponse {
                error,
                leader_hint: hint,
                tokens: vec![],
            }),
            Request::DescribeCertificates(_) => {
                Response::DescribeCertificates(lotse_proto::DescribeCertificatesResponse {
                    error,
                    leader_hint: hint,
                    now_ms: 0,
                    certificates: vec![],
                })
            }
            Request::DescribeCertificateRequests(_) => {
                Response::DescribeCertificateRequests(lotse_proto::DescribeCertificateRequestsResponse {
                    error,
                    leader_hint: hint,
                    now_ms: 0,
                    requests: vec![],
                })
            }
            Request::DescribeUpgrade(_) => Response::DescribeUpgrade(lotse_proto::DescribeUpgradeResponse {
                error,
                leader_hint: hint,
                cluster_version: None,
                held: false,
                turn: None,
                nodes: vec![],
            }),
            Request::DescribeTenants(_) => Response::DescribeTenants(lotse_proto::DescribeTenantsResponse {
                error,
                leader_hint: hint,
                tenants: vec![],
            }),
            Request::DescribeRoleBindings(_) => {
                Response::DescribeRoleBindings(lotse_proto::DescribeRoleBindingsResponse {
                    error,
                    leader_hint: hint,
                    bindings: vec![],
                })
            }
            Request::OffsetFetch(o) => Response::OffsetFetch(OffsetFetchResponse {
                resources: o
                    .resources
                    .iter()
                    .map(|r| OffsetFetchResource {
                        resource_id: r.resource_id,
                        partitions: r
                            .partitions
                            .iter()
                            .map(|&partition| OffsetFetchPartition {
                                partition,
                                error,
                                offset: 0,
                                leader_term: 1,
                                metadata: None,
                            })
                            .collect(),
                    })
                    .collect(),
            }),
            other => panic!("not a zone read: {other:?}"),
        }
    }

    /// What every node answers, unless a test says otherwise: node 1 is
    /// the coordinator, with all three nodes voting.
    fn common(req: &Request) -> Response {
        match req {
            Request::Metadata(_) => metadata(),
            Request::FindCoordinator(_) => Response::FindCoordinator(FindCoordinatorResponse {
                error: ErrorCode::NONE,
                node_id: 1,
                host: "a".into(),
                port: 9000,
                error_msg: None,
                voters: vec![1, 2, 3],
            }),
            Request::GroupHeartbeat(_) => Response::GroupHeartbeat(lotse_proto::GroupHeartbeatResponse {
                throttle_ms: 0,
                error: ErrorCode::NONE,
                member_id: "m".into(),
                member_epoch: 1,
                heartbeat_interval_ms: 1_000,
                assignment: Some(vec![lotse_proto::TxnResource { resource_id: 7, partitions: vec![0] }]),
            }),
            Request::Fetch(_) => fetched(ErrorCode::NONE, None),
            Request::ListOffsets(_) => Response::ListOffsets(ListOffsetsResponse {
                resources: vec![ListOffsetsResourceResponse {
                    resource_id: 0,
                    partitions: vec![ListOffsetsPartitionResponse {
                        partition: 0,
                        error: ErrorCode::NONE,
                        offset: 0,
                        timestamp: -1,
                        leader_hint: None,
                    }],
                }],
            }),
            other => read(other, ErrorCode::NONE),
        }
    }

    fn client(rt: &lotse_sim::SimRuntime, zone: Option<&str>) -> crate::Client<lotse_sim::SimRuntime> {
        crate::Client::new(
            rt.node("client"),
            crate::ClientConfig {
                retry_for: Duration::from_secs(2),
                zone: zone.map(String::from),
                ..crate::ClientConfig::new(vec!["a:9000".into()])
            },
        )
    }

    /// The requests of kind `key` that were heard, as (host, read_at_least).
    fn heard_of(heard: &Heard, key: lotse_proto::ApiKey) -> Vec<(String, Vec<GroupIndex>)> {
        let heard = heard.lock().unwrap();
        heard.iter().filter(|(_, _, r)| r.api_key() == key).map(|(h, t, _)| (h.clone(), t.clone())).collect()
    }

    /// Indexes only rise, and every clone (a connection's) shares them.
    #[test]
    fn an_index_only_rises() {
        let i = crate::zone::Indexes::default();
        assert_eq!(i.snapshot(), at(&[(1, 0), (2, 0)]));
        i.raise(&at(&[(1, 5), (2, 3)]));
        i.raise(&at(&[(1, 2)]));
        assert_eq!(i.snapshot(), at(&[(1, 5), (2, 3)]));
        i.clone().raise(&at(&[(2, 7)]));
        assert_eq!(i.snapshot(), at(&[(1, 5), (2, 7)]), "clones share the indexes");
    }

    #[test]
    fn the_zone_node_is_the_lowest_id_in_the_zone() {
        use std::collections::BTreeMap;
        let zones = BTreeMap::from([(1, "x".to_string()), (2, "y".into()), (3, "y".into())]);
        assert_eq!(crate::zone::zone_node("y", [3, 2, 1], &zones), Some(2));
        assert_eq!(crate::zone::zone_node("x", [2, 3], &zones), None);
        assert_eq!(crate::zone::zone_node("z", zones.keys().copied(), &zones), None);
    }

    /// Every `applied` the client hears raises its index of that group; a
    /// lower one (from a follower further behind) leaves it as it is.
    #[test]
    fn applied_raises_the_index_and_never_lowers_it() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| match r {
                Request::Metadata(_) => (at(&[(1, 5), (2, 3)]), metadata()),
                r => (vec![], common(r)),
            });
            let answers = Arc::new(Mutex::new(vec![at(&[(1, 9)]), at(&[(1, 2)])]));
            serve(&rt, "b", &heard, move |r| (answers.lock().unwrap().pop().unwrap_or_default(), common(r)));
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            client.metadata(&[]).await.expect("metadata");
            for _ in 0..3 {
                client.describe_users().await.expect("users");
            }
            let b = |s: &[(u64, u64)]| ("b".to_string(), at(s));
            assert_eq!(
                heard_of(&heard, lotse_proto::ApiKey::DESCRIBE_USERS),
                [b(&[(1, 5), (2, 3)]), b(&[(1, 5), (2, 3)]), b(&[(1, 9), (2, 3)])],
                "2 is below 5 and leaves it; 9 raises it"
            );
        });
    }

    /// Each of the nine admin reads goes to the lowest-id node in the
    /// client's zone with both groups' indexes, 0 for one not heard of.
    #[test]
    fn an_admin_read_goes_to_the_zone_with_read_at_least() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| match r {
                Request::Metadata(_) => (at(&[(1, 4)]), metadata()),
                r => (vec![], common(r)),
            });
            serve(&rt, "b", &heard, |r| (vec![], common(r)));
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            // Retried until the nodes listen, unlike the lookup of a name.
            client.metadata(&[]).await.expect("metadata");
            client.describe_configs("s").await.expect("configs");
            client.describe_nodes().await.expect("nodes");
            client.describe_users().await.expect("users");
            client.describe_tokens(None).await.expect("tokens");
            client.describe_certificates().await.expect("certificates");
            client.describe_certificate_requests().await.expect("requests");
            client.describe_upgrade().await.expect("upgrade");
            client.describe_tenants().await.expect("tenants");
            client.describe_role_bindings().await.expect("bindings");
            let heard = heard.lock().unwrap();
            let reads: Vec<_> = heard.iter().filter(|(_, _, r)| !matches!(r, Request::Metadata(_))).collect();
            assert_eq!(reads.len(), 9, "{reads:?}");
            for (host, tags, r) in reads {
                assert_eq!((host.as_str(), tags), ("b", &at(&[(1, 4), (2, 0)])), "{r:?}");
            }
        });
    }

    /// A zone node that has not applied what the client saw refuses with
    /// NOT_LEADER: that read goes to the leader with the same tag, which a
    /// leader deposed without knowing it refuses (ADR 0141), and the next
    /// read tries the zone again.
    #[test]
    fn a_refused_zone_read_goes_to_the_leader_and_the_next_tries_the_zone_again() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| (at(&[(1, 6)]), common(r)));
            let refused = Arc::new(Mutex::new(false));
            serve(&rt, "b", &heard, move |r| match r {
                Request::DescribeTenants(_) if !std::mem::replace(&mut *refused.lock().unwrap(), true) => {
                    (vec![], read(r, ErrorCode::NOT_LEADER))
                }
                r => (vec![], common(r)),
            });
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            client.metadata(&[]).await.expect("metadata");
            client.describe_tenants().await.expect("from the leader");
            client.describe_tenants().await.expect("from the zone");
            let tagged = at(&[(1, 6), (2, 0)]);
            assert_eq!(
                heard_of(&heard, lotse_proto::ApiKey::DESCRIBE_TENANTS),
                [("b".into(), tagged.clone()), ("a".into(), tagged.clone()), ("b".into(), tagged)]
            );
        });
    }

    /// A node of an older release ignores the tag and refuses the
    /// leader-only reads with NOT_LEADER: every such read ends at the
    /// leader.
    #[test]
    fn an_older_zone_node_sends_the_read_to_the_leader() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| (vec![], common(r)));
            serve(&rt, "b", &heard, |r| match r {
                Request::DescribeUpgrade(_) => (vec![], read(r, ErrorCode::NOT_LEADER)),
                r => (vec![], common(r)),
            });
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            client.metadata(&[]).await.expect("metadata");
            for _ in 0..2 {
                client.describe_upgrade().await.expect("from the leader");
            }
            let hosts: Vec<String> =
                heard_of(&heard, lotse_proto::ApiKey::DESCRIBE_UPGRADE).into_iter().map(|(h, _)| h).collect();
            assert_eq!(hosts, ["b", "a", "b", "a"]);
        });
    }

    /// A group member reads its committed offsets from the lowest-id
    /// coordinator voter in its zone, naming both groups' indexes.
    #[test]
    fn offset_fetch_goes_to_a_coordinator_voter_in_the_zone() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| match r {
                Request::Metadata(_) => (at(&[(1, 4)]), metadata()),
                Request::FindCoordinator(_) | Request::GroupHeartbeat(_) => (at(&[(2, 6)]), common(r)),
                r => (vec![], common(r)),
            });
            serve(&rt, "b", &heard, |r| (vec![], common(r)));
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            let mut group = client.join_group("g", &["s"], crate::GroupConfig::default()).await.expect("joined");
            group.poll(Duration::from_millis(10)).await.expect("polled");
            assert_eq!(group.positions().get(&("s".to_string(), 0)), Some(&0), "the committed offset");
            assert_eq!(heard_of(&heard, lotse_proto::ApiKey::OFFSET_FETCH), [("b".to_string(), at(&[(1, 4), (2, 6)]))]);
        });
    }

    /// A fetch that goes to the leader names the client's zone, so the
    /// leader can point back to it; one that goes to the zone does not.
    #[test]
    fn a_fetch_to_the_leader_names_the_zone() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            for host in ["a", "b", "c"] {
                serve(&rt, host, &heard, |r| (vec![], common(r)));
            }
            let client = client(&rt, Some("y"));
            client.fetch("s", 0, 0, Duration::ZERO).await.expect("from the leader: no replica in the zone");
            client.fetch("t", 0, 0, Duration::ZERO).await.expect("from node 2, in the zone");
            let heard = heard.lock().unwrap();
            let fetches: Vec<(&str, Option<&str>, bool)> = heard
                .iter()
                .filter_map(|(h, _, r)| match r {
                    Request::Fetch(f) => Some((h.as_str(), f.zone.as_deref(), f.allow_follower)),
                    _ => None,
                })
                .collect();
            assert_eq!(fetches, [("a", Some("y"), false), ("b", None, true)]);
        });
    }

    /// After a replica in the zone failed, the leader serves; once it names
    /// that replica as current again, the next fetch and offset lookup go
    /// back there, without asking for metadata.
    #[test]
    fn preferred_replica_sends_the_next_fetch_back_to_the_zone() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| match r {
                Request::Fetch(_) => (vec![], fetched(ErrorCode::NONE, Some(2))),
                r => (vec![], common(r)),
            });
            let refused = Arc::new(Mutex::new(false));
            serve(&rt, "b", &heard, move |r| match r {
                Request::Fetch(_) if !std::mem::replace(&mut *refused.lock().unwrap(), true) => {
                    (vec![], fetched(ErrorCode::NOT_LEADER, None))
                }
                r => (vec![], common(r)),
            });
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            client.fetch("t", 0, 0, Duration::ZERO).await.expect("from the leader after node 2 refused");
            client.fetch("t", 0, 0, Duration::ZERO).await.expect("from node 2 again");
            client.list_offset("t", 0, crate::OffsetAt::Latest).await.expect("from node 2 too");
            let heard = heard.lock().unwrap();
            let reads: Vec<(&str, &str, bool)> = heard
                .iter()
                .filter_map(|(h, _, r)| match r {
                    Request::Fetch(f) => Some((h.as_str(), "fetch", f.allow_follower)),
                    Request::ListOffsets(l) => Some((h.as_str(), "list", l.allow_follower)),
                    _ => None,
                })
                .collect();
            assert_eq!(reads, [("b", "fetch", true), ("a", "fetch", false), ("b", "fetch", true), ("b", "list", true)]);
            let asked = heard.iter().filter(|(_, _, r)| matches!(r, Request::Metadata(_))).count();
            assert_eq!(asked, 1, "metadata only for the first route");
        });
    }

    /// A client without a zone sends neither tag, and its reads go to the
    /// leaders.
    #[test]
    fn no_zone_no_tags() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            serve(&rt, "a", &heard, |r| (at(&[(1, 4), (2, 6)]), common(r)));
            for host in ["b", "c"] {
                serve(&rt, host, &heard, |r| match r {
                    Request::Metadata(_) | Request::FindCoordinator(_) => (vec![], common(r)),
                    r => panic!("only the leader is asked, not for {r:?}"),
                });
            }
            let client = client(&rt, None);
            client.describe_upgrade().await.expect("upgrade");
            client.describe_users().await.expect("users");
            client.fetch("t", 0, 0, Duration::ZERO).await.expect("fetched");
            client.list_offset("t", 0, crate::OffsetAt::Latest).await.expect("listed");
            let mut group = client.join_group("g", &["s"], crate::GroupConfig::default()).await.expect("joined");
            group.poll(Duration::from_millis(10)).await.expect("polled");
            let heard = heard.lock().unwrap();
            assert!(heard.iter().any(|(_, _, r)| matches!(r, Request::OffsetFetch(_))));
            for (host, tags, r) in heard.iter() {
                assert!(tags.is_empty(), "{r:?}");
                if let Request::Fetch(f) = r {
                    assert_eq!((f.zone.as_deref(), f.allow_follower), (None, false));
                }
                if !matches!(r, Request::Metadata(_) | Request::FindCoordinator(_)) {
                    assert_eq!(host, "a", "{r:?}");
                }
            }
        });
    }

    /// A node the latest metadata no longer lists is no zone node, for
    /// admin reads and offset reads alike: here node 2 left and is gone,
    /// and the reads go to node 3 without a failed call first.
    #[test]
    fn zone_reads_skip_a_node_the_latest_metadata_left_out() {
        lotse_sim::Sim::new(1).run(|rt| async move {
            let heard = Heard::default();
            let answers = Arc::new(Mutex::new(0));
            serve(&rt, "a", &heard, move |r| match r {
                Request::Metadata(_) => {
                    let mut n = answers.lock().unwrap();
                    *n += 1;
                    (vec![], if *n == 1 { metadata() } else { metadata_of(&[1, 3]) })
                }
                r => (vec![], common(r)),
            });
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            client.metadata(&[]).await.expect("all three");
            client.metadata(&[]).await.expect("node 2 left");
            client.describe_users().await.expect("users");
            let mut group = client.join_group("g", &["s"], crate::GroupConfig::default()).await.expect("joined");
            group.poll(Duration::from_millis(10)).await.expect("polled");
            let hosts = |key| heard_of(&heard, key).into_iter().map(|(h, _)| h).collect::<Vec<_>>();
            assert_eq!(hosts(lotse_proto::ApiKey::DESCRIBE_USERS), ["c"]);
            assert_eq!(hosts(lotse_proto::ApiKey::OFFSET_FETCH), ["c"]);
        });
    }

    /// The fetches of `t`, as (host, allow_follower).
    fn fetches(heard: &Heard) -> Vec<(String, bool)> {
        let heard = heard.lock().unwrap();
        heard
            .iter()
            .filter_map(|(h, _, r)| match r {
                Request::Fetch(f) => Some((h.clone(), f.allow_follower)),
                _ => None,
            })
            .collect()
    }

    /// Node 2 refuses the first fetch, then serves; node 1 answers fetches
    /// with what `leader` gives for the n-th one.
    fn after_a_fallback(leader: impl Fn(usize) -> Response + Send + Sync + 'static) -> Vec<(String, bool)> {
        let heard = Heard::default();
        let out = heard.clone();
        lotse_sim::Sim::new(1).run(move |rt| async move {
            let count = Arc::new(Mutex::new(0));
            serve(&rt, "a", &heard, move |r| match r {
                Request::Fetch(_) => {
                    let mut n = count.lock().unwrap();
                    *n += 1;
                    (vec![], leader(*n))
                }
                r => (vec![], common(r)),
            });
            let refused = Arc::new(Mutex::new(false));
            serve(&rt, "b", &heard, move |r| match r {
                Request::Fetch(_) if !std::mem::replace(&mut *refused.lock().unwrap(), true) => {
                    (vec![], fetched(ErrorCode::NOT_LEADER, None))
                }
                r => (vec![], common(r)),
            });
            serve(&rt, "c", &heard, |r| (vec![], common(r)));
            let client = client(&rt, Some("y"));
            let _ = client.fetch("t", 0, 0, Duration::ZERO).await;
            client.fetch("t", 0, 0, Duration::ZERO).await.expect("fetched");
        });
        fetches(&out)
    }

    fn hosts(fetches: &[(&str, bool)]) -> Vec<(String, bool)> {
        fetches.iter().map(|(h, f)| (h.to_string(), *f)).collect()
    }

    /// A leader answer with an error says nothing about the replicas: its
    /// `preferred_replica` leaves the next fetch with the leader.
    #[test]
    fn preferred_replica_on_an_error_is_ignored() {
        let seen = after_a_fallback(|n| {
            if n == 1 { fetched(ErrorCode::OFFSET_OUT_OF_RANGE, Some(2)) } else { fetched(ErrorCode::NONE, None) }
        });
        assert_eq!(seen, hosts(&[("b", true), ("a", false), ("a", false)]));
    }

    /// A replica the client has no address for cannot be asked: the
    /// fallback stays, rather than sending the next fetch to the replica
    /// that failed.
    #[test]
    fn preferred_replica_without_an_address_leaves_the_fetch_on_the_leader() {
        let seen = after_a_fallback(|n| fetched(ErrorCode::NONE, if n == 1 { Some(4) } else { None }));
        assert_eq!(seen, hosts(&[("b", true), ("a", false), ("a", false)]));
    }

    /// The zone voter refuses an `OffsetFetch`, or the call to it fails:
    /// that read goes to the coordinator with the same tag, and the next
    /// tries the zone again.
    #[test]
    fn a_refused_offset_fetch_goes_to_the_coordinator_and_the_next_tries_the_zone_again() {
        for failed_call in [false, true] {
            lotse_sim::Sim::new(1).run(move |rt| async move {
                let heard = Heard::default();
                serve(&rt, "a", &heard, |r| match r {
                    Request::Metadata(_) => (at(&[(1, 4)]), metadata()),
                    Request::FindCoordinator(_) => (at(&[(2, 6)]), common(r)),
                    r => (vec![], common(r)),
                });
                let refused = Arc::new(Mutex::new(false));
                serve(&rt, "b", &heard, move |r| match r {
                    Request::OffsetFetch(_) if !std::mem::replace(&mut *refused.lock().unwrap(), true) => {
                        let answer = if failed_call {
                            Response::Error(ErrorCode::INVALID_REQUEST)
                        } else {
                            read(r, ErrorCode::NOT_LEADER)
                        };
                        (vec![], answer)
                    }
                    r => (vec![], common(r)),
                });
                serve(&rt, "c", &heard, |r| (vec![], common(r)));
                let client = client(&rt, Some("y"));
                client.metadata(&[]).await.expect("metadata");
                let req = Request::OffsetFetch(lotse_proto::OffsetFetchRequest {
                    group_id: "g".into(),
                    tenant: "default".into(),
                    require_stable: true,
                    resources: vec![lotse_proto::TxnResource { resource_id: 7, partitions: vec![0] }],
                });
                for _ in 0..2 {
                    match client.call_coordinator_read("g", &req).await {
                        Ok(Response::OffsetFetch(o)) => assert!(!crate::zone::refused(&Response::OffsetFetch(o))),
                        Ok(other) => panic!("unexpected {other:?}"),
                        Err(_) => panic!("the read failed"),
                    }
                }
                let tagged = at(&[(1, 4), (2, 6)]);
                assert_eq!(
                    heard_of(&heard, lotse_proto::ApiKey::OFFSET_FETCH),
                    [("b".into(), tagged.clone()), ("a".into(), tagged.clone()), ("b".into(), tagged)],
                    "failed call: {failed_call}"
                );
            });
        }
    }
}

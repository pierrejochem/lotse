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

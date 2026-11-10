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
fn serve(
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

fn brokers(ids: &[(u32, &str)]) -> Vec<lotse_proto::Broker> {
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

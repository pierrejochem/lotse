//! Fetching from a replica in the client's zone (ADR 0104), against
//! scripted brokers on the simulated network.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, SimRuntime};

/// What a broker saw: its node id and whether the fetch allowed a follower.
type Seen = Arc<Mutex<Vec<(u32, bool)>>>;

/// Node `id` of two, in zone `a` (node 1, the leader) or `b` (node 2, a
/// follower). Node 2 answers a fetch with `follower_error` if set.
async fn broker(rt: SimRuntime, id: u32, seen: Seen, follower_error: Option<ErrorCode>) {
    let listener = rt.listen(&format!("b{id}:9000"), lotse_rt::Link::Client).await.unwrap();
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let seen = seen.clone();
        drop(rt.spawn(async move {
            while let Ok(Some(frame)) = conn.recv().await {
                let (h, req) = decode_request(frame).unwrap();
                let resp = match req {
                    Request::Metadata(_) => Response::Metadata(MetadataResponse {
                        brokers: vec![
                            Broker { node_id: 1, host: "b1".into(), port: 9000, rack: "a".into() },
                            Broker { node_id: 2, host: "b2".into(), port: 9000, rack: "b".into() },
                        ],
                        resources: vec![ResourceMetadata {
                            error: ErrorCode::NONE,
                            name: "s".into(),
                            tenant: "default".into(),
                            resource_id: 7,
                            kind: kind::STREAM,
                            partitions: vec![PartitionMetadata {
                                partition: 0,
                                leader: 1,
                                leader_term: 1,
                                voters: vec![1, 2],
                            }],
                            exchange: None,
                        }],
                    }),
                    Request::Fetch(f) => {
                        seen.lock().unwrap().push((id, f.allow_follower));
                        let error = if id == 2 { follower_error.unwrap_or(ErrorCode::NONE) } else { ErrorCode::NONE };
                        let records = if error == ErrorCode::NONE { batch() } else { Bytes::new() };
                        Response::Fetch(FetchResponse {
                            throttle_ms: 0,
                            resources: vec![FetchResourceResponse {
                                resource_id: 7,
                                partitions: vec![FetchPartitionResponse {
                                    partition: 0,
                                    error,
                                    high_watermark: 1,
                                    last_stable_offset: 1,
                                    log_start_offset: 0,
                                    aborted_txns: vec![],
                                    records,
                                    leader_hint: None,
                                }],
                            }],
                        })
                    }
                    other => panic!("unexpected request {other:?}"),
                };
                if conn.send(encode_response(h.correlation_id, &resp)).await.is_err() {
                    return;
                }
            }
        }));
    }
}

fn batch() -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    b.finish().unwrap()
}

fn client(rt: &SimRuntime, zone: Option<&str>) -> Client<SimRuntime> {
    Client::new(
        rt.node("c1"),
        ClientConfig {
            request_timeout: Duration::from_millis(100),
            retry_for: Duration::from_millis(500),
            zone: zone.map(String::from),
            ..ClientConfig::new(vec!["b1:9000".into()])
        },
    )
}

fn run(zone: Option<&str>, follower_error: Option<ErrorCode>) -> Vec<(u32, bool)> {
    let seen: Seen = Arc::default();
    let zone = zone.map(String::from);
    let out = seen.clone();
    Sim::new(1).run(move |rt| async move {
        drop(rt.spawn(broker(rt.node("b1"), 1, seen.clone(), None)));
        drop(rt.spawn(broker(rt.node("b2"), 2, seen.clone(), follower_error)));
        let c = client(&rt, zone.as_deref());
        for _ in 0..2 {
            let f = c.fetch("s", 0, 0, Duration::ZERO).await.expect("fetched");
            assert!(!f.records.is_empty());
        }
        drop(c);
    });
    out.lock().unwrap().clone()
}

#[test]
fn a_client_in_a_zone_fetches_from_the_replica_there() {
    assert_eq!(run(Some("b"), None), [(2, true), (2, true)], "from the follower in zone b, saying so");
    assert_eq!(run(Some("a"), None), [(1, false), (1, false)], "the leader is in zone a");
    assert_eq!(run(Some("c"), None), [(1, false), (1, false)], "no replica in zone c");
    assert_eq!(run(None, None), [(1, false), (1, false)], "no zone, the leader");
}

#[test]
fn a_replica_that_fails_is_left_for_the_leader() {
    // An older node answers NOT_LEADER; one whose log starts later,
    // OFFSET_OUT_OF_RANGE. Either way the leader serves, and the
    // partition stays with it.
    for code in [ErrorCode::NOT_LEADER, ErrorCode::OFFSET_OUT_OF_RANGE] {
        assert_eq!(run(Some("b"), Some(code)), [(2, true), (1, false), (1, false)], "{code}");
    }
}

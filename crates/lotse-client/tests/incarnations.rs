//! Idempotent sequences belong to a stream's incarnation (ADR 0107): a
//! stream deleted and created again under its name is another partition.
//! Against a scripted broker on the simulated network.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, SimRuntime};

/// What the broker shares with the test.
#[derive(Default)]
struct Script {
    /// The resource id stream `s` has now.
    current: AtomicU64,
    /// Swallow the next produce and move `s` to this id meanwhile.
    swallow_then: AtomicU64,
    /// Every produce: resource id, producer id, base sequence.
    seen: Mutex<Vec<(u64, u64, i32)>>,
}

/// One node with stream `s`. A produce to the id `s` has now is written; one
/// to an older id is refused with `NOT_LEADER` (nothing written), as a node
/// that applied the delete would.
async fn broker(rt: SimRuntime, script: Arc<Script>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    let ids = Arc::new(AtomicU64::new(10));
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let (script, ids) = (script.clone(), ids.clone());
        drop(rt.spawn(async move {
            while let Ok(Some(frame)) = conn.recv().await {
                let (h, req) = decode_request(frame).unwrap();
                let resp = match req {
                    Request::Metadata(_) => Response::Metadata(MetadataResponse {
                        brokers: vec![Broker { node_id: 1, host: "b1".into(), port: 9000, rack: String::new() }],
                        resources: vec![ResourceMetadata {
                            error: ErrorCode::NONE,
                            name: "s".into(),
                            tenant: "default".into(),
                            resource_id: script.current.load(Ordering::Relaxed),
                            kind: kind::STREAM,
                            partitions: vec![PartitionMetadata {
                                partition: 0,
                                leader: 1,
                                leader_term: 1,
                                voters: vec![1],
                            }],
                            exchange: None,
                        }],
                    }),
                    Request::FindCoordinator(_) => Response::FindCoordinator(FindCoordinatorResponse {
                        error: ErrorCode::NONE,
                        node_id: 1,
                        host: "b1".into(),
                        port: 9000,
                        error_msg: None,
                    }),
                    Request::InitProducer(_) => Response::InitProducer(InitProducerResponse {
                        error: ErrorCode::NONE,
                        producer_id: ids.fetch_add(1, Ordering::Relaxed),
                        producer_epoch: 0,
                        leader_hint: None,
                    }),
                    Request::Produce(p) => {
                        let id = p.resources[0].resource_id;
                        let (b, _) = lotse_record::Batch::parse(&p.resources[0].partitions[0].batch).unwrap();
                        script.seen.lock().unwrap().push((id, b.producer_id(), b.base_sequence()));
                        let then = script.swallow_then.swap(0, Ordering::Relaxed);
                        if then != 0 {
                            script.current.store(then, Ordering::Relaxed);
                            continue; // the answer is lost: written or not, nobody knows
                        }
                        let error = if id == script.current.load(Ordering::Relaxed) {
                            ErrorCode::NONE
                        } else {
                            ErrorCode::NOT_LEADER
                        };
                        Response::Produce(ProduceResponse {
                            throttle_ms: 0,
                            resources: vec![ProduceResourceResponse {
                                resource_id: id,
                                partitions: vec![ProducePartitionResponse {
                                    partition: 0,
                                    error,
                                    base_offset: 0,
                                    append_ts: -1,
                                    error_msg: None,
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

fn setup(rt: &SimRuntime) -> (Arc<Script>, Client<SimRuntime>) {
    let script = Arc::new(Script { current: AtomicU64::new(7), ..Script::default() });
    drop(rt.spawn(broker(rt.node("b1"), script.clone())));
    let client = Client::new(
        rt.node("c1"),
        ClientConfig {
            request_timeout: Duration::from_millis(200),
            retry_for: Duration::from_secs(2),
            ..ClientConfig::new(vec!["b1:9000".into()])
        },
    );
    (script, client)
}

#[test]
fn each_incarnation_of_a_stream_counts_its_own_sequences() {
    Sim::new(1).run(|rt| async move {
        let (script, client) = setup(&rt);
        client.produce("s", 0, batch()).await.unwrap();
        client.produce("s", 0, batch()).await.unwrap();
        // Deleted and created again: the cached route still names 7, which
        // is refused; the produce does not follow the name to 9.
        script.current.store(9, Ordering::Relaxed);
        let moved = client.produce("s", 0, batch()).await;
        assert!(matches!(moved, Err(ClientError::UnknownStream(..))), "{moved:?}");
        client.produce("s", 0, batch()).await.unwrap();
        // A node behind still serves 7: back there, its sequences go on.
        script.current.store(7, Ordering::Relaxed);
        assert!(matches!(client.produce("s", 0, batch()).await, Err(ClientError::UnknownStream(..))));
        client.produce("s", 0, batch()).await.unwrap();
        let seen = script.seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            [(7, 10, 0), (7, 10, 1), (7, 10, 2), (9, 10, 0), (9, 10, 1), (7, 10, 2)],
            "a refused batch keeps its sequence; 9 starts at 0; 7 goes on at 2"
        );
        drop(client);
    });
}

#[test]
fn a_produce_in_doubt_when_its_stream_moves_starts_a_new_producer() {
    Sim::new(1).run(|rt| async move {
        let (script, client) = setup(&rt);
        client.produce("s", 0, batch()).await.unwrap();
        // The next produce's answer is lost while the stream is created
        // again: it may be in 7, so its sequence is in doubt there.
        script.swallow_then.store(9, Ordering::Relaxed);
        assert!(matches!(client.produce("s", 0, batch()).await, Err(ClientError::UnknownStream(..))));
        client.produce("s", 0, batch()).await.unwrap();
        let seen = script.seen.lock().unwrap().clone();
        assert_eq!(seen.first(), Some(&(7, 10, 0)));
        assert_eq!(seen.get(1), Some(&(7, 10, 1)));
        assert_eq!(seen.last(), Some(&(9, 11, 0)), "a new producer id: {seen:?}");
        drop(client);
    });
}

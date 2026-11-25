//! A queue at its length limit refuses with `QUEUE_FULL` (ADR 0118):
//! nothing was written, so the sequence stays free; but after an attempt
//! whose answer was lost the refusal says nothing about that attempt, which
//! may still commit, and the produce's outcome is unknown. Against a
//! scripted broker on the simulated network.

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
    /// Swallow this many produces: their answers are lost.
    swallow: AtomicU64,
    /// Answer this many produces with `QUEUE_FULL`.
    full: AtomicU64,
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
                                pinned_leader: 0,
                                placed: false,
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
                        voters: Vec::new(),
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
                        if script.swallow.load(Ordering::Relaxed) > 0 {
                            script.swallow.fetch_sub(1, Ordering::Relaxed);
                            continue; // the answer is lost: written or not, nobody knows
                        }
                        let full = script.full.load(Ordering::Relaxed) > 0;
                        if full {
                            script.full.fetch_sub(1, Ordering::Relaxed);
                        }
                        let then = script.swallow_then.swap(0, Ordering::Relaxed);
                        if then != 0 {
                            script.current.store(then, Ordering::Relaxed);
                            continue; // the answer is lost: written or not, nobody knows
                        }
                        let error = if full {
                            ErrorCode::QUEUE_FULL
                        } else if id == script.current.load(Ordering::Relaxed) {
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

fn full(r: &Result<u64, ClientError>) -> bool {
    matches!(r, Err(ClientError::Broker { code, .. }) if *code == ErrorCode::QUEUE_FULL)
}

#[test]
fn a_full_queue_refuses_and_the_sequence_stays_free() {
    Sim::new(1).run(|rt| async move {
        let (script, client) = setup(&rt);
        client.produce("s", 0, batch()).await.unwrap();
        script.full.store(1, Ordering::Relaxed);
        let r = client.produce("s", 0, batch()).await;
        assert!(full(&r), "refused at once, not retried: {r:?}");
        client.produce("s", 0, batch()).await.unwrap();
        let seen = script.seen.lock().unwrap().clone();
        assert_eq!(seen, [(7, 10, 0), (7, 10, 1), (7, 10, 1)], "the refused batch's sequence is used again");
        drop(client);
    });
}

#[test]
fn a_full_queue_after_an_attempt_in_doubt_leaves_the_outcome_unknown() {
    Sim::new(1).run(|rt| async move {
        let (script, client) = setup(&rt);
        client.produce("s", 0, batch()).await.unwrap();
        // The first attempt's answer is lost; it may commit later at a
        // leader that has it. The retry is refused as full.
        script.swallow.store(1, Ordering::Relaxed);
        script.full.store(1, Ordering::Relaxed);
        let r = client.produce("s", 0, batch()).await;
        assert!(matches!(r, Err(ClientError::Unknown(_))), "{r:?}");
        client.produce("s", 0, batch()).await.unwrap();
        let seen = script.seen.lock().unwrap().clone();
        assert_eq!(
            seen.last(),
            Some(&(7, 11, 0)),
            "a new producer id, so a sequence in doubt is not used again: {seen:?}"
        );
        drop(client);
    });
}

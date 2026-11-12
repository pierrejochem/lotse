//! The client against a scripted broker on the simulated network.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, SimRuntime};

/// A broker that answers everything a transaction needs, except produce,
/// which it swallows: every produce's outcome stays unknown.
async fn broker(rt: SimRuntime, end_called: Arc<AtomicBool>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let end_called = end_called.clone();
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
                            resource_id: 7,
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
                        producer_id: 9,
                        producer_epoch: 0,
                        leader_hint: None,
                    }),
                    Request::AddPartitionsToTxn(r) => Response::AddPartitionsToTxn(AddPartitionsToTxnResponse {
                        resources: r
                            .resources
                            .iter()
                            .map(|x| TxnResourceResult {
                                resource_id: x.resource_id,
                                partitions: x
                                    .partitions
                                    .iter()
                                    .map(|p| TxnPartitionResult { partition: *p, error: ErrorCode::NONE })
                                    .collect(),
                            })
                            .collect(),
                    }),
                    Request::EndTxn(r) => {
                        end_called.store(true, Ordering::Relaxed);
                        Response::EndTxn(EndTxnResponse {
                            error: ErrorCode::NONE,
                            producer_id: 9,
                            producer_epoch: r.producer_epoch + 1,
                        })
                    }
                    Request::Produce(_) => continue,
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

/// A write whose outcome stays unknown may or may not be in the
/// transaction: committing it would commit something nobody knows. The
/// client must refuse, without even asking the coordinator.
#[test]
fn a_transaction_with_an_unknown_write_cannot_be_committed() {
    Sim::new(1).run(|rt| async move {
        let end_called = Arc::new(AtomicBool::new(false));
        drop(rt.spawn(broker(rt.node("b1"), end_called.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_millis(100),
                retry_for: Duration::from_millis(300),
                transactional_id: Some("t".into()),
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        let mut txn = client.begin_transaction().await.unwrap();
        assert!(matches!(txn.produce("s", 0, batch()).await, Err(ClientError::Unknown(_))));
        assert!(matches!(txn.produce("s", 0, batch()).await, Err(ClientError::MustAbort(_))), "no more writes");
        assert!(matches!(txn.commit().await, Err(ClientError::MustAbort(_))));
        assert!(!end_called.load(Ordering::Relaxed), "the coordinator was never asked to commit");
        drop(client);
    });
}

/// A transaction that attempted nothing has nothing at the coordinator.
#[test]
fn an_empty_transaction_is_not_ended() {
    Sim::new(1).run(|rt| async move {
        let end_called = Arc::new(AtomicBool::new(false));
        drop(rt.spawn(broker(rt.node("b1"), end_called.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig { transactional_id: Some("t".into()), ..ClientConfig::new(vec!["b1:9000".into()]) },
        );
        let txn = client.begin_transaction().await.unwrap();
        txn.commit().await.expect("committed");
        assert!(!end_called.load(Ordering::Relaxed));
        drop(client);
    });
}

/// A broker with a fanout exchange to queues `a` (written) and `b` (never
/// written: its partition has no leader that takes the produce). A
/// `Publish` (ADR 0079) it answers the same way the first time, and
/// swallows after that, so its answer is lost; `publishes` counts them.
async fn fanout_broker(rt: SimRuntime, end_called: Arc<AtomicBool>) {
    fanout_broker_counting(rt, end_called, Arc::new(std::sync::atomic::AtomicUsize::new(0))).await
}

async fn fanout_broker_counting(
    rt: SimRuntime,
    end_called: Arc<AtomicBool>,
    publishes: Arc<std::sync::atomic::AtomicUsize>,
) {
    fanout_broker_refusing(rt, end_called, publishes, Arc::new(std::sync::atomic::AtomicUsize::new(0))).await
}

/// As [`fanout_broker_counting`], but the first `refusals` publishes are
/// refused, before anything is written, as by a node that does not know
/// the exchange yet; `publishes` counts only the others.
async fn fanout_broker_refusing(
    rt: SimRuntime,
    end_called: Arc<AtomicBool>,
    publishes: Arc<std::sync::atomic::AtomicUsize>,
    refusals: Arc<std::sync::atomic::AtomicUsize>,
) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let end_called = end_called.clone();
        let publishes = publishes.clone();
        let refusals = refusals.clone();
        drop(rt.spawn(async move {
            while let Ok(Some(frame)) = conn.recv().await {
                let (h, req) = decode_request(frame).unwrap();
                let queue = |name: &str, id: u64| ResourceMetadata {
                    error: ErrorCode::NONE,
                    name: name.into(),
                    tenant: "default".into(),
                    resource_id: id,
                    kind: kind::QUEUE,
                    partitions: vec![PartitionMetadata { partition: 0, leader: 1, leader_term: 1, voters: vec![1] }],
                    exchange: None,
                };
                let resp = match req {
                    Request::Metadata(_) => Response::Metadata(MetadataResponse {
                        brokers: vec![Broker { node_id: 1, host: "b1".into(), port: 9000, rack: String::new() }],
                        resources: vec![
                            ResourceMetadata {
                                error: ErrorCode::NONE,
                                name: "x".into(),
                                tenant: "default".into(),
                                resource_id: 3,
                                kind: kind::EXCHANGE,
                                partitions: vec![],
                                exchange: Some(ExchangeMetadata {
                                    exchange_type: exchange_type::FANOUT,
                                    bindings: [1, 2]
                                        .map(|q| BindingMetadata {
                                            queue_id: q,
                                            binding_key: String::new(),
                                            header_match: vec![],
                                            match_all: true,
                                        })
                                        .to_vec(),
                                }),
                            },
                            queue("a", 1),
                            queue("b", 2),
                        ],
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
                        producer_id: 9,
                        producer_epoch: 0,
                        leader_hint: None,
                    }),
                    Request::AddPartitionsToTxn(r) => Response::AddPartitionsToTxn(AddPartitionsToTxnResponse {
                        resources: r
                            .resources
                            .iter()
                            .map(|x| TxnResourceResult {
                                resource_id: x.resource_id,
                                partitions: vec![TxnPartitionResult { partition: 0, error: ErrorCode::NONE }],
                            })
                            .collect(),
                    }),
                    Request::EndTxn(r) => {
                        end_called.store(r.commit, Ordering::Relaxed);
                        Response::EndTxn(EndTxnResponse {
                            error: ErrorCode::NONE,
                            producer_id: 9,
                            producer_epoch: r.producer_epoch + 1,
                        })
                    }
                    Request::Produce(p) => {
                        let id = p.resources[0].resource_id;
                        let error = if id == 1 { ErrorCode::NONE } else { ErrorCode::NOT_LEADER };
                        Response::Produce(ProduceResponse {
                            throttle_ms: 0,
                            resources: vec![ProduceResourceResponse {
                                resource_id: id,
                                partitions: vec![ProducePartitionResponse {
                                    partition: 0,
                                    error,
                                    base_offset: 5,
                                    append_ts: -1,
                                    error_msg: None,
                                    leader_hint: None,
                                }],
                            }],
                        })
                    }
                    Request::Publish(p) => {
                        assert_eq!((p.entries.len(), p.entries[0].exchange_id), (1, 3));
                        let left = refusals.load(Ordering::Relaxed);
                        let refuse = left > 0
                            && refusals.compare_exchange(left, left - 1, Ordering::Relaxed, Ordering::Relaxed).is_ok();
                        if refuse {
                            let refused = PublishResult { error: ErrorCode::NOT_LEADER, routed: vec![] };
                            let resp = Response::Publish(PublishResponse { throttle_ms: 0, entries: vec![refused] });
                            if conn.send(encode_response(h.correlation_id, &resp)).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        if publishes.fetch_add(1, Ordering::Relaxed) > 0 {
                            continue;
                        }
                        let routed = vec![
                            RoutedMessage { queue_id: 1, name: "a".into(), error: ErrorCode::NONE, base_offset: 5 },
                            RoutedMessage {
                                queue_id: 2,
                                name: "b".into(),
                                error: ErrorCode::NOT_LEADER,
                                base_offset: 0,
                            },
                        ];
                        Response::Publish(PublishResponse {
                            throttle_ms: 0,
                            entries: vec![PublishResult { error: ErrorCode::NONE, routed }],
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

/// A `Publish` whose answer is lost is not sent again: the node may have
/// written it, and a second one could write it twice (ADR 0079).
#[test]
fn a_publish_whose_answer_is_lost_is_not_sent_again() {
    Sim::new(3).run(|rt| async move {
        let publishes = Arc::new(std::sync::atomic::AtomicUsize::new(1));
        drop(rt.spawn(fanout_broker_counting(rt.node("b1"), Arc::new(AtomicBool::new(false)), publishes.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_millis(100),
                retry_for: Duration::from_millis(500),
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        let res = client.publish("x", "", lotse_client::OutMessage::value("v"), false).await;
        assert!(matches!(res, Err(ClientError::Unknown(_))), "{res:?}");
        assert_eq!(publishes.load(Ordering::Relaxed), 2, "one publish, not sent again");
        drop(client);
    });
}

/// A publish refused before anything was written, by a node that has not
/// heard of the exchange yet, is sent again until a node takes it.
#[test]
fn a_publish_refused_by_a_node_behind_is_sent_again() {
    Sim::new(4).run(|rt| async move {
        let publishes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let refusals = Arc::new(std::sync::atomic::AtomicUsize::new(2));
        let broker = fanout_broker_refusing(
            rt.node("b1"),
            Arc::new(AtomicBool::new(false)),
            publishes.clone(),
            refusals.clone(),
        );
        drop(rt.spawn(broker));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_millis(100),
                retry_for: Duration::from_millis(500),
                idempotent: false,
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        let res = client.publish("x", "", lotse_client::OutMessage::value("v"), false).await;
        // The broker routes the third to queue a and refuses queue b.
        assert!(matches!(&res, Err(ClientError::NotAllWritten { written, .. }) if written.len() == 1), "{res:?}");
        assert_eq!(refusals.load(Ordering::Relaxed), 0, "both refusals were met");
        assert_eq!(publishes.load(Ordering::Relaxed), 1, "and then one publish taken");
        drop(client);
    });
}

/// A message published in a transaction reaches all its queues or none:
/// once one queue holds it and another could not be written, committing
/// would deliver it to some only.
#[test]
fn a_transaction_with_a_partial_publish_cannot_be_committed() {
    Sim::new(2).run(|rt| async move {
        let committed = Arc::new(AtomicBool::new(false));
        drop(rt.spawn(fanout_broker(rt.node("b1"), committed.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_millis(100),
                retry_for: Duration::from_millis(300),
                transactional_id: Some("t".into()),
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        let mut txn = client.begin_transaction().await.unwrap();
        let res = txn.publish("x", "", lotse_client::OutMessage::value("v"), false).await;
        assert!(matches!(res, Err(ClientError::GaveUp(..))), "queue b refuses: {res:?}");
        assert!(matches!(txn.commit().await, Err(ClientError::MustAbort(_))));
        assert!(!committed.load(Ordering::Relaxed), "the coordinator was never asked to commit");

        // Without a transaction a node routes the same publish (ADR 0079)
        // and says which queue holds it.
        let plain = Client::new(
            rt.node("c2"),
            ClientConfig {
                request_timeout: Duration::from_millis(100),
                retry_for: Duration::from_millis(300),
                idempotent: false,
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        match plain.publish("x", "", lotse_client::OutMessage::value("v"), false).await {
            Err(ClientError::NotAllWritten { written, failed }) => {
                assert_eq!(written, vec![("a".to_string(), 5)]);
                assert!(
                    matches!(&failed[..], [(q, ClientError::Broker { code: ErrorCode::NOT_LEADER, .. })] if q == "b"),
                    "{failed:?}"
                );
            }
            other => panic!("expected a partial publish, got {other:?}"),
        }
        drop((client, plain));
    });
}

/// A broker for stream `s` that hands out a new producer id each time and
/// notes each produce's producer id and sequence; the first produce it
/// swallows, the others it answers.
async fn noting_broker(rt: SimRuntime, seen: Arc<std::sync::Mutex<Vec<(u64, i32)>>>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    let ids = Arc::new(std::sync::atomic::AtomicU64::new(10));
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let (seen, ids) = (seen.clone(), ids.clone());
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
                            resource_id: 7,
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
                        let (b, _) = lotse_record::Batch::parse(&p.resources[0].partitions[0].batch).unwrap();
                        let first = {
                            let mut s = seen.lock().unwrap();
                            s.push((b.producer_id(), b.base_sequence()));
                            s.len() == 1
                        };
                        if first {
                            continue;
                        }
                        Response::Produce(ProduceResponse {
                            throttle_ms: 0,
                            resources: vec![ProduceResourceResponse {
                                resource_id: 7,
                                partitions: vec![ProducePartitionResponse {
                                    partition: 0,
                                    error: ErrorCode::NONE,
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

/// A produce dropped before its answer came may have been written. The
/// next batch must not carry the same producer id and sequence, or the
/// leader would take it for the dropped one and answer with that one's
/// offset, writing nothing (found by the queue simulation: a node drops
/// a publish's write at its deadline, ADR 0079).
#[test]
fn a_produce_dropped_before_its_answer_leaves_no_sequence_behind() {
    Sim::new(1).run(|rt| async move {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        drop(rt.spawn(noting_broker(rt.node("b1"), seen.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_secs(10),
                retry_for: Duration::from_secs(10),
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        {
            let mut produce = std::pin::pin!(client.produce("s", 0, batch()));
            let mut deadline = std::pin::pin!(rt.sleep(Duration::from_millis(100)));
            std::future::poll_fn(|cx| {
                use std::future::Future;
                assert!(produce.as_mut().poll(cx).is_pending(), "the broker never answers the first produce");
                deadline.as_mut().poll(cx)
            })
            .await;
        }
        client.produce("s", 0, batch()).await.unwrap();
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "{seen:?}");
        assert_ne!(seen[0], seen[1], "the second batch took the dropped one's producer id and sequence");
        drop(client);
    });
}

/// Within a transaction a dropped produce leaves the transaction to be
/// aborted: the next batch would otherwise take its sequence.
#[test]
fn a_transaction_whose_produce_was_dropped_cannot_go_on() {
    Sim::new(1).run(|rt| async move {
        let end_called = Arc::new(AtomicBool::new(false));
        drop(rt.spawn(broker(rt.node("b1"), end_called.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig {
                request_timeout: Duration::from_secs(10),
                retry_for: Duration::from_secs(10),
                transactional_id: Some("t".into()),
                ..ClientConfig::new(vec!["b1:9000".into()])
            },
        );
        let mut txn = client.begin_transaction().await.unwrap();
        {
            let mut produce = std::pin::pin!(txn.produce("s", 0, batch()));
            let mut deadline = std::pin::pin!(rt.sleep(Duration::from_millis(100)));
            std::future::poll_fn(|cx| {
                use std::future::Future;
                assert!(produce.as_mut().poll(cx).is_pending(), "the broker never answers a produce");
                deadline.as_mut().poll(cx)
            })
            .await;
        }
        assert!(matches!(txn.produce("s", 0, batch()).await, Err(ClientError::MustAbort(_))), "no more writes");
        assert!(matches!(txn.commit().await, Err(ClientError::MustAbort(_))));
        assert!(!end_called.load(Ordering::Relaxed), "the coordinator was never asked to commit");
        drop(client);
    });
}

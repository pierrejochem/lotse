//! The client against a scripted broker on the simulated network.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Conn, Listener, Net, Spawn};
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

/// A broker with a fanout exchange to queues `a` (written) and `b` (never
/// written: its partition has no leader that takes the produce).
async fn fanout_broker(rt: SimRuntime, end_called: Arc<AtomicBool>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let end_called = end_called.clone();
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
                    other => panic!("unexpected request {other:?}"),
                };
                if conn.send(encode_response(h.correlation_id, &resp)).await.is_err() {
                    return;
                }
            }
        }));
    }
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

        // Without a transaction the same publish says which queue holds it.
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
                assert!(matches!(&failed[..], [(q, ClientError::GaveUp(..))] if q == "b"), "{failed:?}");
            }
            other => panic!("expected a partial publish, got {other:?}"),
        }
        drop((client, plain));
    });
}

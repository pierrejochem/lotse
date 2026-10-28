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
    let listener = rt.listen("b1:9000").await.unwrap();
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
                            resource_id: 7,
                            kind: kind::STREAM,
                            partitions: vec![PartitionMetadata {
                                partition: 0,
                                leader: 1,
                                leader_term: 1,
                                voters: vec![1],
                            }],
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

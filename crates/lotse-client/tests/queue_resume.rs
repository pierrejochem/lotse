//! A queue consumer resuming at a new subscription (ADR 0109) against a
//! scripted broker on the simulated network.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_client::{Client, ClientConfig};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, SimRuntime};

/// One push of the messages at offsets 5 and 6.
fn push(subscription_id: u32) -> DeliverPush {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    b.push(0, None, Some(b"w"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut raw, 5, 1).unwrap();
    DeliverPush {
        subscription_id,
        error: ErrorCode::NONE,
        batch: raw.freeze(),
        deliveries: vec![
            DeliveredOffset { offset: 5, delivery_count: 1 },
            DeliveredOffset { offset: 6, delivery_count: 1 },
        ],
        leader_hint: None,
    }
}

/// A broker with queue `q` and a coordinator. The first subscription gets
/// consumer id 77 and one push of offsets 5 and 6; later ones get nothing
/// new. The first transactional ack is refused as `NOT_LEADER`, as after a
/// leader change; it records every subscribe and ack.
async fn broker(rt: SimRuntime, heard: Arc<Mutex<Vec<Request>>>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    let subscriptions = Arc::new(Mutex::new(0u32));
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let (heard, subscriptions) = (heard.clone(), subscriptions.clone());
        drop(rt.spawn(async move {
            while let Ok(Some(frame)) = conn.recv().await {
                let (h, req) = decode_request(frame).unwrap();
                let resp = match req {
                    Request::Metadata(_) => Response::Metadata(MetadataResponse {
                        brokers: vec![Broker { node_id: 1, host: "b1".into(), port: 9000, rack: String::new() }],
                        resources: vec![ResourceMetadata {
                            error: ErrorCode::NONE,
                            name: "q".into(),
                            tenant: "default".into(),
                            resource_id: 7,
                            kind: kind::QUEUE,
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
                    Request::EndTxn(r) => Response::EndTxn(EndTxnResponse {
                        error: ErrorCode::NONE,
                        producer_id: 9,
                        producer_epoch: r.producer_epoch + 1,
                    }),
                    Request::Subscribe(s) => {
                        heard.lock().unwrap().push(Request::Subscribe(s));
                        let id = {
                            let mut n = subscriptions.lock().unwrap();
                            *n += 1;
                            *n
                        };
                        let answer = Response::Subscribe(SubscribeResponse {
                            error: ErrorCode::NONE,
                            subscription_id: id,
                            leader_hint: None,
                            standby: false,
                            consumer_id: 77,
                        });
                        conn.send(encode_response(h.correlation_id, &answer)).await.unwrap();
                        if id == 1 {
                            conn.send(encode_push(&push(id))).await.unwrap();
                        }
                        continue;
                    }
                    Request::Ack(a) => {
                        let first = !heard.lock().unwrap().iter().any(|r| matches!(r, Request::Ack(_)));
                        heard.lock().unwrap().push(Request::Ack(a));
                        let error = if first { ErrorCode::NOT_LEADER } else { ErrorCode::NONE };
                        Response::Outcome(OutcomeResponse { error, leader_hint: None })
                    }
                    _ => Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None }),
                };
                if conn.send(encode_response(h.correlation_id, &resp)).await.is_err() {
                    return;
                }
            }
        }));
    }
}

/// A transactional ack that meets a leader change resumes the consumer,
/// which keeps what it received but has not handed out yet: the new
/// subscription holds it still.
#[test]
fn a_consumer_that_resumes_keeps_what_it_received() {
    Sim::new(1).run(|rt| async move {
        let heard = Arc::new(Mutex::new(Vec::new()));
        drop(rt.spawn(broker(rt.node("b1"), heard.clone())));
        let client = Client::new(
            rt.node("c1"),
            ClientConfig { transactional_id: Some("t".into()), ..ClientConfig::new(vec!["b1:9000".into()]) },
        );
        let mut consumer = client.subscribe("q", 10).await.unwrap();
        let first = consumer.next(Duration::from_secs(1)).await.unwrap().expect("a message");
        assert_eq!(first.offset(), 5);
        let mut txn = client.begin_transaction().await.unwrap();
        consumer.ack_in(&mut txn, &first).await.expect("sent again on the new subscription");
        let second = consumer.next(Duration::from_millis(500)).await.unwrap();
        assert_eq!(second.map(|m| m.offset()), Some(6), "kept across the resume");
        txn.commit().await.expect("committed");
        let heard = heard.lock().unwrap();
        let resumes: Vec<Option<u64>> = heard
            .iter()
            .filter_map(|r| match r {
                Request::Subscribe(s) => Some(s.resume),
                _ => None,
            })
            .collect();
        assert_eq!(resumes, vec![None, Some(77)], "resumed as the consumer it was");
        assert_eq!(heard.iter().filter(|r| matches!(r, Request::Ack(_))).count(), 2);
    });
}

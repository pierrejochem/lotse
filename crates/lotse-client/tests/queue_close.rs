//! Closing a queue consumer against a scripted broker on the simulated
//! network: what the client sends back when it goes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_client::{Client, ClientConfig};
use lotse_proto::*;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Conn, Listener, Net, Spawn};
use lotse_sim::{Sim, SimRuntime};

fn push(subscription_id: u32, offset: u64) -> DeliverPush {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(b"v"), &[]);
    let mut raw = bytes::BytesMut::from(&b.finish().unwrap()[..]);
    lotse_record::stamp(&mut raw, offset, 1).unwrap();
    DeliverPush {
        subscription_id,
        error: ErrorCode::NONE,
        batch: raw.freeze(),
        deliveries: vec![DeliveredOffset { offset, delivery_count: 1 }],
        leader_hint: None,
    }
}

/// A broker with queue `q` that pushes the messages at offsets 5 and 6, one
/// push each, right after the subscription, and records what else it hears.
async fn broker(rt: SimRuntime, heard: Arc<Mutex<Vec<Request>>>) {
    let listener = rt.listen("b1:9000", lotse_rt::Link::Client).await.unwrap();
    loop {
        let Ok((conn, _)) = listener.accept().await else { return };
        let heard = heard.clone();
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
                    Request::Subscribe(_) => {
                        let answer = Response::Subscribe(SubscribeResponse {
                            error: ErrorCode::NONE,
                            subscription_id: 3,
                            leader_hint: None,
                            standby: false,
                            consumer_id: 0,
                        });
                        conn.send(encode_response(h.correlation_id, &answer)).await.unwrap();
                        for offset in [5, 6] {
                            conn.send(encode_push(&push(3, offset))).await.unwrap();
                        }
                        continue;
                    }
                    other => {
                        heard.lock().unwrap().push(other);
                        Response::Outcome(OutcomeResponse { error: ErrorCode::NONE, leader_hint: None })
                    }
                };
                if conn.send(encode_response(h.correlation_id, &resp)).await.is_err() {
                    return;
                }
            }
        }));
    }
}

/// A push that arrived but was never read stays in flight at the node until
/// the ack timeout unless closing gives it back.
#[test]
fn closing_gives_back_what_was_pushed_but_not_read() {
    Sim::new(1).run(|rt| async move {
        let heard = Arc::new(Mutex::new(Vec::new()));
        drop(rt.spawn(broker(rt.node("b1"), heard.clone())));
        let client = Client::new(rt.node("c1"), ClientConfig::new(vec!["b1:9000".into()]));
        let mut consumer = client.subscribe("q", 10).await.unwrap();
        // The first push is read; the second waits unread in the queue.
        let first = consumer.next(Duration::from_secs(1)).await.unwrap().expect("a message");
        assert_eq!(first.offset(), 5);
        rt.sleep(Duration::from_millis(100)).await;
        consumer.close().await;
        rt.sleep(Duration::from_millis(100)).await;
        let heard = heard.lock().unwrap();
        let nacks: Vec<_> = heard
            .iter()
            .filter_map(|r| match r {
                Request::Nack(n) => Some((n.ranges.clone(), n.requeue)),
                _ => None,
            })
            .collect();
        assert_eq!(nacks, vec![(vec![(6, 7)], true)], "only the unread push goes back, with requeue; heard {heard:?}");
        assert!(matches!(heard.first(), Some(Request::Unsubscribe(_))), "unsubscribed first: {heard:?}");
    });
}

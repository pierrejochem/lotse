//! Queue consumers against a faulty cluster, checked over many seeds.
//!
//! Three or five nodes host two queues (one Raft group each), a coordinator
//! group, and two exchanges: a topic exchange routing `job.*` to `jobs` and
//! everything to `audit`, and a direct exchange routing `jobs` to `jobs`. A
//! producer writes messages through `lotse-client`, straight to a queue or
//! published through an exchange, a second one in transactions it commits,
//! aborts or abandons; consumers
//! subscribe with small credit windows and acknowledge most messages, but
//! also requeue some, discard some, hold some past the queue's ack timeout,
//! and drop their subscription now and then. Meanwhile pairs of hosts are
//! partitioned and nodes lose power and restart. Afterwards the network
//! heals and a last consumer drains each queue.
//!
//! Checked:
//! - nothing is lost: every message whose produce was acknowledged was
//!   either settled (acknowledged or discarded) or is drained at the end;
//! - nothing settled comes back: the drain never sees a message whose ack
//!   or discard was confirmed;
//! - a message carries what was produced at its offset, and a delivery
//!   count of at least 1;
//! - no message of an aborted transaction is ever delivered, and every
//!   message of a committed one is (it is settled or drained);
//! - a published message goes to exactly the queues its routing key
//!   selects, a mandatory one that matches nothing is refused, and a queue
//!   a partial publish reports as not written does not hold it.
//!
//! Delivery counts are not checked further here: they are written only on
//! a requeue or an ack timeout, so after a failover a count may start over
//! (the dispatcher's unit tests check counts).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::OutMessage;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_route::{Binding, Exchange, ExchangeKind};
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, QueueConfig, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_seeds};

#[derive(Debug, Default)]
struct Nothing;

impl StateMachine for Nothing {
    fn apply(&mut self, _: &Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

const TICK: Duration = Duration::from_millis(10);
const QUEUES: [&str; 2] = ["audit", "jobs"];
const ACK_TIMEOUT: Duration = Duration::from_millis(400);

fn message(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn client_config() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(800),
        retry_for: Duration::from_millis(1_500),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(600),
        ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
    }
}

/// A message: its queue and offset.
type At = (String, u64);

/// A transaction's messages, `(where, value)`, and how it ended.
type Txn = (Vec<(At, String)>, Option<bool>);

/// The queues a message published through `events` with `key` goes to.
fn expected_route(key: &str) -> Vec<String> {
    if key.starts_with("job.") && key.matches('.').count() == 1 {
        vec!["audit".into(), "jobs".into()]
    } else {
        vec!["audit".into()]
    }
}

#[derive(Default)]
struct Outcomes {
    /// Where each acknowledged produce or publish put its message.
    produced: BTreeMap<At, String>,
    /// Values a publish reported as definitely not written to a queue.
    not_written: Vec<(String, String)>,
    /// Acknowledged or discarded, confirmed.
    settled: BTreeSet<At>,
    /// Acks and discards whose outcome is unknown.
    maybe_settled: BTreeSet<At>,
    /// Every delivery a consumer saw: (value, count).
    seen: BTreeMap<At, Vec<(String, u16)>>,
    /// Transactions: `(offset, value)` of their messages, and whether they
    /// committed (`Some(true)`), aborted (`Some(false)`) or nobody knows.
    txns: Vec<Txn>,
}

struct World {
    rt: SimRuntime,
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
    down_until: BTreeMap<NodeId, lotse_rt::Instant>,
}

impl World {
    async fn new(rt: SimRuntime) -> World {
        let n = if rt.chance(1, 2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
        let queues = vec![
            QueueConfig { name: "jobs".into(), resource_id: 9, group: 1, ack_timeout: ACK_TIMEOUT },
            QueueConfig { name: "audit".into(), resource_id: 10, group: 3, ack_timeout: ACK_TIMEOUT },
        ];
        let exchanges = vec![
            Exchange {
                name: "events".into(),
                resource_id: 20,
                kind: ExchangeKind::Topic,
                bindings: vec![Binding::key(9, "job.*"), Binding::key(10, "#")],
            },
            Exchange {
                name: "direct".into(),
                resource_id: 21,
                kind: ExchangeKind::Direct,
                bindings: vec![Binding::key(9, "jobs")],
            },
        ];
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        // Groups 1 and 3 are queues, group 2 coordinates producers.
                        groups: vec![1, 2, 3],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        streams: Vec::new(),
                        queues: queues.clone(),
                        exchanges: exchanges.clone(),
                        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
                        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
                        metadata: None,
                        coordinator: Some(2),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        auth: lotse_server::AuthConfig::none(),
                    },
                )
            })
            .collect();
        let mut w = World {
            hosts: ids.iter().map(|&id| (id, rt.node(&format!("n{id}")))).collect(),
            cfgs,
            running: BTreeMap::new(),
            down_until: BTreeMap::new(),
            rt,
        };
        for id in ids {
            w.start(id).await;
        }
        w
    }

    async fn start(&mut self, id: NodeId) {
        let node = Node::start(self.hosts[&id].clone(), self.cfgs[&id].clone(), |_| Nothing)
            .await
            .unwrap_or_else(|e| panic!("node {id} cannot start from its disk: {e}"));
        let handle = node.handle();
        self.running.insert(id, Some((node, handle)));
    }

    async fn crash(&mut self, id: NodeId, mid_write: bool) {
        let Some(Some((node, handle))) = self.running.insert(id, None) else { return };
        let host = self.hosts[&id].clone();
        if mid_write {
            host.disk().arm_power_loss(self.rt.below(6));
            host.sleep(TICK * 3).await;
        }
        handle.stop();
        let _ = node.join().await; // an error is the power loss itself
        host.crash();
        let down = TICK * (5 + self.rt.below(60) as u32);
        self.down_until.insert(id, self.rt.now() + down);
    }
}

fn produced(out: &Mutex<Outcomes>, at: At, value: String) {
    let before = out.lock().unwrap().produced.insert(at.clone(), value);
    assert!(before.is_none(), "{at:?} acknowledged twice");
}

async fn producer(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let value = format!("m{seq}");
        seq += 1;
        match rt.below(4) {
            // Straight to a queue.
            0 => {
                let q = QUEUES[rt.below(2) as usize];
                match client.produce(q, 0, message(&value)).await {
                    Ok(offset) => produced(&out, (q.into(), offset), value),
                    Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                    Err(e) => panic!("unexpected produce error: {e}"),
                }
            }
            // A mandatory message for a key nothing is bound to.
            1 if rt.chance(1, 5) => {
                match client.publish("direct", "nobody", OutMessage::value(value.clone()), true).await {
                    Err(ClientError::Unroutable { .. }) => {}
                    other => panic!("a mandatory message for no queue was not refused: {other:?}"),
                }
                out.lock().unwrap().not_written.extend(QUEUES.map(|q| (q.to_string(), value.clone())));
            }
            // Through the topic exchange: to audit, and job.* to jobs too.
            _ => {
                let key = if rt.chance(1, 2) { format!("job.{seq}") } else { format!("log.{seq}") };
                match client.publish("events", &key, OutMessage::value(value.clone()), false).await {
                    Ok(p) => {
                        let queues: Vec<String> = p.routed.iter().map(|(q, _)| q.clone()).collect();
                        assert_eq!(queues, expected_route(&key), "{key} was routed wrongly");
                        for (q, offset) in p.routed {
                            produced(&out, (q, offset), value.clone());
                        }
                    }
                    Err(ClientError::NotAllWritten { written, failed }) => {
                        let mut queues: Vec<String> = written.iter().map(|(q, _)| q.clone()).collect();
                        queues.extend(failed.iter().map(|(q, _)| q.clone()));
                        queues.sort();
                        assert_eq!(queues, expected_route(&key), "{key} was routed wrongly");
                        for (q, offset) in written {
                            produced(&out, (q, offset), value.clone());
                        }
                        for (q, e) in failed {
                            match e {
                                ClientError::GaveUp(..) => out.lock().unwrap().not_written.push((q, value.clone())),
                                ClientError::Unknown(_) => {}
                                e => panic!("unexpected publish error for {q}: {e}"),
                            }
                        }
                    }
                    Err(e) => panic!("unexpected publish error: {e}"),
                }
            }
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
}

/// Writes transactions of one to three messages and commits, aborts or
/// abandons each.
async fn transactional(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(
        rt.clone(),
        ClientConfig {
            transactional_id: Some("tx".into()),
            txn_timeout: Duration::from_millis(800),
            ..client_config()
        },
    );
    let mut n = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let mut txn = match client.begin_transaction().await {
            Ok(t) => t,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected error beginning a transaction: {e}"),
        };
        n += 1;
        let mut written = Vec::new();
        let mut failed = false;
        for i in 0..1 + rt.below(3) {
            let value = format!("tx{n}-{i}");
            let res = if rt.chance(1, 2) {
                let q = QUEUES[rt.below(2) as usize];
                txn.produce(q, 0, message(&value)).await.map(|o| vec![(q.to_string(), o)])
            } else {
                let key = if rt.chance(1, 2) { "job.tx" } else { "log.tx" };
                txn.publish("events", key, OutMessage::value(value.clone()), false).await.map(|p| p.routed)
            };
            match res {
                Ok(routed) => written.extend(routed.into_iter().map(|at| (at, value.clone()))),
                Err(ClientError::GaveUp(..)) => {}
                // Maybe written, at an unknown offset: the transaction aborts.
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        let end = match rt.below(6) {
            0 => {
                drop(txn);
                Some(false)
            }
            1 => {
                let _ = txn.abort().await;
                Some(false)
            }
            _ if failed => {
                let _ = txn.abort().await;
                Some(false)
            }
            _ => match txn.commit().await {
                Ok(()) => Some(true),
                Err(ClientError::Unknown(_)) => None,
                Err(ClientError::GaveUp(..)) => Some(false),
                Err(ClientError::Broker { code: lotse_proto::ErrorCode::PRODUCER_FENCED, .. }) => Some(false),
                // A publish reached some of its queues only: nothing is
                // committed, and the client aborts on the next begin.
                Err(ClientError::MustAbort(_)) => Some(false),
                Err(e) => panic!("unexpected error committing: {e}"),
            },
        };
        out.lock().unwrap().txns.push((written, end));
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

fn value_of(m: &lotse_client::Message) -> String {
    String::from_utf8(m.record.value.clone().unwrap().to_vec()).unwrap()
}

/// Settles a message, noting whether the outcome is known.
fn note_settled(out: &Mutex<Outcomes>, at: At, res: Result<(), ClientError>) {
    let mut o = out.lock().unwrap();
    match res {
        Ok(()) => {
            o.settled.insert(at);
        }
        Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {
            o.maybe_settled.insert(at);
        }
        Err(e) => panic!("unexpected settle error: {e}"),
    }
}

async fn consumer(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    while !stop.load(Ordering::Relaxed) {
        let credit = 1 + rt.below(5) as u32;
        let queue = QUEUES[rt.below(2) as usize];
        let mut consumer = match client.subscribe(queue, credit).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected subscribe error: {e}"),
        };
        // Some messages, then (sometimes) start over with a new subscription.
        for _ in 0..1 + rt.below(30) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let m = match consumer.next(TICK * 20).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(ClientError::GaveUp(..)) => continue,
                Err(e) => panic!("unexpected error receiving: {e}"),
            };
            let at = (queue.to_string(), m.offset());
            out.lock().unwrap().seen.entry(at.clone()).or_default().push((value_of(&m), m.delivery_count));
            match rt.below(20) {
                // Requeue: delivered again later.
                0 => {
                    let _ = consumer.nack(&m, true).await;
                }
                // Discard.
                1 => note_settled(&out, at, consumer.nack(&m, false).await),
                // Stuck past the ack timeout: the subscription ends and the
                // message goes to someone else; this ack still settles it.
                2 => {
                    rt.sleep(ACK_TIMEOUT + TICK * 10).await;
                    note_settled(&out, at, consumer.ack(&m).await);
                }
                // Never settled here.
                3 => {}
                _ => note_settled(&out, at, consumer.ack(&m).await),
            }
        }
        if rt.chance(1, 2) {
            consumer.close().await;
        }
    }
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = vec![
        rt.spawn(producer(rt.node("p1"), out.clone(), stop.clone())),
        rt.spawn(transactional(rt.node("t1"), out.clone(), stop.clone())),
    ];
    for c in 1..=2 {
        tasks.push(rt.spawn(consumer(rt.node(&format!("k{c}")), out.clone(), stop.clone())));
    }
    let n = w.cfgs.len() as u64;
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["p1", "t1", "k1", "k2"].map(String::from));
    let end = rt.now() + Duration::from_secs(6);
    let mut partition_until = None;
    while rt.now() < end {
        rt.sleep(TICK * 2).await;
        let back: Vec<NodeId> = w.down_until.iter().filter(|(_, t)| **t <= rt.now()).map(|(id, _)| *id).collect();
        for id in back {
            w.down_until.remove(&id);
            w.start(id).await;
        }
        if partition_until.is_none() && rt.chance(1, 30) {
            let (a, b) = (&hosts[rt.below(hosts.len() as u64) as usize], &hosts[rt.below(hosts.len() as u64) as usize]);
            if a != b {
                rt.net().block(a, b);
                partition_until = Some(rt.now() + TICK * (10 + rt.below(80) as u32));
            }
        }
        if partition_until.is_some_and(|t| t <= rt.now()) {
            rt.net().heal();
            partition_until = None;
        }
        let down = w.running.values().filter(|r| r.is_none()).count() as u64;
        if down < (n - 1) / 2 && rt.chance(1, 30) {
            let mid_write = rt.chance(1, 2);
            w.crash(1 + rt.below(n), mid_write).await;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // Drain: whatever is not settled comes out, nothing settled does.
    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(!out.settled.is_empty(), "no message was settled: the run checked nothing");
    let drainer =
        Client::new(rt.node("drainer"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let mut drained: BTreeMap<At, String> = BTreeMap::new();
    for queue in QUEUES {
        let mut consumer = drainer.subscribe(queue, 50).await.expect("the healed cluster takes a subscription");
        // Messages of ended subscriptions come back once their counts are
        // written; give that a few ack timeouts.
        let mut quiet_since = rt.now();
        while rt.now() - quiet_since < ACK_TIMEOUT * 4 {
            let Some(m) = consumer.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
            quiet_since = rt.now();
            let o = (queue.to_string(), m.offset());
            assert!(!out.settled.contains(&o), "message {o:?} came back after it was settled");
            assert!(m.delivery_count >= 1, "message {o:?} has delivery count 0");
            drained.insert(o, value_of(&m));
            consumer.ack(&m).await.expect("the healed cluster takes acks");
        }
    }
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        let redelivered = out.seen.values().filter(|d| d.len() > 1).count();
        eprintln!(
            "stats: produced {} settled {} maybe {} seen {} redelivered {} drained {}",
            out.produced.len(),
            out.settled.len(),
            out.maybe_settled.len(),
            out.seen.len(),
            redelivered,
            drained.len()
        );
    }
    for (o, v) in &out.produced {
        if let Some(d) = drained.get(o) {
            assert_eq!(d, v, "message {o:?} holds {d}, but {v} was produced there");
        }
        assert!(
            out.settled.contains(o) || out.maybe_settled.contains(o) || drained.contains_key(o),
            "message {o:?} ({v}) was produced, never settled, and is not in the queue"
        );
    }
    for (q, v) in &out.not_written {
        let found = |m: &BTreeMap<At, String>| m.iter().any(|((mq, _), mv)| mq == q && mv == v);
        let seen = out.seen.iter().any(|((sq, _), d)| sq == q && d.iter().any(|(sv, _)| sv == v));
        assert!(!found(&drained) && !seen, "{v} was reported as not written to {q}, but is there");
    }
    let delivered = |o: &At| out.seen.contains_key(o) || drained.contains_key(o);
    for (written, end) in &out.txns {
        for (o, v) in written {
            match end {
                Some(false) => assert!(!delivered(o), "{v} at {o:?} was delivered, but its transaction aborted"),
                Some(true) => assert!(
                    out.settled.contains(o) || out.maybe_settled.contains(o) || drained.contains_key(o),
                    "{v} at {o:?} was committed, never settled, and is not in the queue"
                ),
                None => {}
            }
            if let Some(d) = drained.get(o) {
                assert_eq!(d, v, "message {o:?} holds {d}, but {v} was produced there");
            }
        }
    }
    assert!(out.txns.iter().any(|(w, e)| *e == Some(true) && !w.is_empty()), "no transaction committed");
    for (o, deliveries) in &out.seen {
        for (v, count) in deliveries {
            assert!(*count >= 1, "message {o:?} was delivered with count 0");
            if let Some(p) = out.produced.get(o) {
                assert_eq!(v, p, "message {o:?} was delivered as {v}, but {p} was produced there");
            }
        }
    }
    drop(drainer);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn queues_lose_nothing_and_never_redeliver_a_settled_message() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

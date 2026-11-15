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
//! Delivery counts are not checked further under random faults; the
//! deterministic failover scenario checks that they go on across a
//! failover (ADR 0109), and the dispatcher's unit tests check them too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::OutMessage;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_queue::Limits;
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
    // Batches come plain, zstd and lz4 (ADR 0058): replicas, snapshots,
    // retention and readers handle them all alike.
    let mut options = BatchOptions::default();
    options.attributes.compression =
        [lotse_record::Compression::None, lotse_record::Compression::Zstd, lotse_record::Compression::Lz4]
            [value.len() % 3];
    let mut b = BatchBuilder::new(options);
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

/// What a run hosts: queues, exchanges and the groups behind them.
struct Scenario {
    queues: Vec<QueueConfig>,
    exchanges: Vec<Exchange>,
    /// Group 2 coordinates producers.
    groups: Vec<u64>,
}

impl Scenario {
    /// Two queues without limits and two exchanges routing to them.
    fn classic() -> Scenario {
        let queues = vec![
            QueueConfig { ack_timeout: ACK_TIMEOUT, ..QueueConfig::new("jobs", 9, 1) },
            QueueConfig { ack_timeout: ACK_TIMEOUT, ..QueueConfig::new("audit", 10, 3) },
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
        // Groups 1 and 3 are queues, group 2 coordinates producers.
        Scenario { queues, exchanges, groups: vec![1, 2, 3] }
    }

    /// Queues with limits (ADR 0055): `poison` lets a message be delivered
    /// three times and then dead-letters it into `graveyard`; `drop` lets it
    /// be delivered twice and then drops it; `short` dead-letters what waits
    /// longer than `TTL` into `graveyard` too.
    fn limits() -> Scenario {
        let queue = |name: &str, id, group, limits: Limits, dead_letter| QueueConfig {
            ack_timeout: ACK_TIMEOUT,
            limits,
            dead_letter,
            ..QueueConfig::new(name, id, group)
        };
        let max = |n| Limits { max_deliveries: n, ..Limits::default() };
        let ttl = Limits { message_ttl_ms: TTL.as_millis() as u64, ..Limits::default() };
        let queues = vec![
            queue("poison", POISON, 11, max(MAX_DELIVERIES), Some(GRAVEYARD)),
            queue("drop", DROP, 12, max(DROP_DELIVERIES), None),
            queue("short", SHORT, 13, ttl, Some(GRAVEYARD)),
            queue("graveyard", GRAVEYARD, 14, Limits::default(), None),
        ];
        Scenario { queues, exchanges: Vec::new(), groups: vec![2, 11, 12, 13, 14] }
    }

    /// One queue with four priority levels (ADR 0078).
    fn priorities() -> Scenario {
        let ranked = QueueConfig { ack_timeout: ACK_TIMEOUT, priority_levels: 4, ..QueueConfig::new("ranked", 40, 1) };
        Scenario { queues: vec![ranked], exchanges: Vec::new(), groups: vec![1, 2] }
    }

    /// One queue with a single active consumer (ADR 0076).
    fn single_active() -> Scenario {
        let line = QueueConfig { ack_timeout: ACK_TIMEOUT, single_active: true, ..QueueConfig::new("line", 30, 1) };
        Scenario { queues: vec![line], exchanges: Vec::new(), groups: vec![1, 2] }
    }
}

impl World {
    async fn new(rt: SimRuntime, scenario: Scenario) -> World {
        let n = if rt.chance(1, 2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
        let Scenario { queues, exchanges, groups } = scenario;
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        groups: groups.clone(),
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        zones: Default::default(),
                        streams: Vec::new(),
                        queues: queues.clone(),
                        exchanges: exchanges.clone(),
                        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
                        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
                        metadata: None,
                        coordinator: Some(2),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
                        retention: lotse_server::RetentionConfig::keep(),
                        join: false,
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
                    // Refused, or no node answered in time (ADR 0079).
                    Err(ClientError::Unroutable { .. } | ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
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
                                // Definitely not written: the node gave up
                                // or a leader refused it (ADR 0079).
                                ClientError::GaveUp(..) | ClientError::Broker { .. } => {
                                    out.lock().unwrap().not_written.push((q, value.clone()))
                                }
                                ClientError::Unknown(_) => {}
                                e => panic!("unexpected publish error for {q}: {e}"),
                            }
                        }
                    }
                    // No node took the publish: written nowhere.
                    Err(ClientError::GaveUp(..)) => {
                        let mut o = out.lock().unwrap();
                        o.not_written.extend(expected_route(&key).into_iter().map(|q| (q, value.clone())));
                    }
                    // A node may have written it to any of its queues.
                    Err(ClientError::Unknown(_)) => {}
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

/// Partitions pairs of hosts and crashes nodes for `lasts`, restarting the
/// ones that were down.
async fn faults(rt: &SimRuntime, w: &mut World, clients: &[&str], lasts: Duration) {
    let n = w.cfgs.len() as u64;
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(clients.iter().map(|c| c.to_string()));
    let end = rt.now() + lasts;
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
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone(), Scenario::classic()).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = vec![
        rt.spawn(producer(rt.node("p1"), out.clone(), stop.clone())),
        rt.spawn(transactional(rt.node("t1"), out.clone(), stop.clone())),
    ];
    for c in 1..=2 {
        tasks.push(rt.spawn(consumer(rt.node(&format!("k{c}")), out.clone(), stop.clone())));
    }
    faults(&rt, &mut w, &["p1", "t1", "k1", "k2"], Duration::from_secs(6)).await;

    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }
    // The transactional producer may have left a transaction open. Its
    // messages, and every message after them, wait for its end, which the
    // coordinator decides at the timeout and, under faults, may finish
    // later than any quiet spell the drain waits for (seed 931). A new
    // instance, as a restarted application starts one, fences the old one
    // and gets its producer only once that transaction is aborted and its
    // markers are written.
    let restarted = Client::new(
        rt.node("t2"),
        ClientConfig {
            transactional_id: Some("tx".into()),
            txn_timeout: Duration::from_millis(800),
            retry_for: Duration::from_secs(20),
            ..client_config()
        },
    );
    drop(restarted.begin_transaction().await.expect("the healed cluster ends the open transaction"));

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

// ---------------------------------------------------------------------------
// Limits: max deliveries, message TTL and dead-letter queues (ADR 0055).
//
// Producers write "good" and "bad" messages to `poison`, `drop` and `short`.
// A consumer acknowledges the good ones at once and never acknowledges a bad
// one: it requeues it or sits on it. One consumer of `short` comes and goes,
// so that messages wait and expire. A consumer of `graveyard` reads what the
// other queues dead-lettered. Nodes crash and hosts are partitioned
// meanwhile; at the end the network heals and every queue is drained.
//
// Checked:
// - a dead letter exists once: one message in `graveyard` per source queue
//   and offset, however often the move was retried across failovers;
// - it carries what was produced there (value), says where it came from, and
//   why: `max-deliveries` only from `poison` with a delivery count of at
//   least the limit, `expired` only from `short` and only after the TTL had
//   passed since the produce began;
// - nothing is lost where a dead-letter queue exists: every message that was
//   produced is acknowledged, in the queue at the end, or in `graveyard`;
// - a message acknowledged at its first delivery is never dead-lettered for
//   its deliveries (for its age it may be, if a failover came between its
//   delivery and the ack: the new leader does not know it is out), and a
//   dead-lettered message is not in its queue at the end;
// - `short` hands out nothing much older than its TTL: at most
//   `TTL_SLACK_MS` over, which covers the leader's scan interval and the
//   network.
//
// - no message goes out more often than its queue allows (the delivery count
//   a consumer sees, which is never above the real number of deliveries).
//
// `drop` has no dead-letter queue: what exhausts its deliveries is dropped,
// so it may lose messages (also good ones, whose deliveries end when a
// connection does); nothing else loses any.

const POISON: u64 = 30;
const DROP: u64 = 31;
const SHORT: u64 = 32;
const GRAVEYARD: u64 = 33;
const MAX_DELIVERIES: u32 = 3;
const DROP_DELIVERIES: u32 = 2;
const TTL: Duration = Duration::from_millis(300);
const TTL_SLACK_MS: i64 = 250;
const LIMITED: [&str; 3] = ["poison", "drop", "short"];

/// A message in the graveyard.
#[derive(Debug, Clone)]
struct Dead {
    /// Its offset in the graveyard.
    at: u64,
    reason: String,
    queue: String,
    deliveries: u16,
    value: String,
    /// The graveyard leader's clock when it appended the copy, in ms.
    appended_ms: i64,
}

#[derive(Default)]
struct LimitOutcomes {
    /// Where each acknowledged produce put its message: value, and the clock
    /// in ms when the produce began.
    produced: BTreeMap<At, (String, i64)>,
    settled: BTreeSet<At>,
    maybe_settled: BTreeSet<At>,
    /// Acknowledged when it was delivered for the first time.
    acked_first: BTreeSet<At>,
    /// Graveyard messages by origin `(queue, offset)`.
    dead: BTreeMap<At, Vec<Dead>>,
    /// The greatest age in ms at which `short` handed a message out.
    oldest_short_ms: i64,
}

fn header(m: &lotse_client::Message, name: &[u8]) -> Option<String> {
    let h = m.record.headers.iter().find(|h| h.key[..] == *name)?;
    Some(String::from_utf8(h.value.clone()?.to_vec()).unwrap())
}

fn now_ms(rt: &SimRuntime) -> i64 {
    rt.unix_time().as_millis() as i64
}

async fn limits_producer(rt: SimRuntime, out: Arc<Mutex<LimitOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut n = 0u64;
    while !stop.load(Ordering::Relaxed) {
        n += 1;
        let queue = LIMITED[rt.below(3) as usize];
        let value = format!("{}-{n}", if rt.chance(1, 3) { "bad" } else { "good" });
        let began = now_ms(&rt);
        match client.produce(queue, 0, message(&value)).await {
            Ok(offset) => {
                let before = out.lock().unwrap().produced.insert((queue.into(), offset), (value, began));
                assert!(before.is_none(), "{queue} offset {offset} acknowledged twice");
            }
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
            Err(e) => panic!("unexpected produce error: {e}"),
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
}

/// Handles one message of a limited queue the way the scenario says: good
/// ones are acknowledged, bad ones never.
async fn handle(
    rt: &SimRuntime,
    out: &Mutex<LimitOutcomes>,
    consumer: &mut lotse_client::Consumer<'_, SimRuntime>,
    queue: &str,
    m: &lotse_client::Message,
) {
    let at = (queue.to_string(), m.offset());
    let value = value_of(m);
    // Counts only ever fall short of the real number of deliveries, so one
    // above the limit means a message went out once too often.
    let limit = match queue {
        "poison" => MAX_DELIVERIES as u16,
        "drop" => DROP_DELIVERIES as u16,
        _ => u16::MAX,
    };
    assert!(
        m.delivery_count <= limit,
        "{value} at {at:?} was delivered a {}. time; the limit is {limit}",
        m.delivery_count
    );
    if queue == "short" {
        let age = now_ms(rt) - m.record.timestamp;
        let mut o = out.lock().unwrap();
        o.oldest_short_ms = o.oldest_short_ms.max(age);
        assert!(
            age <= TTL.as_millis() as i64 + TTL_SLACK_MS,
            "short handed out {value} at {at:?} {age} ms after it was appended; the TTL is {} ms",
            TTL.as_millis()
        );
    }
    if value.starts_with("good") {
        let res = consumer.ack(m).await;
        let mut o = out.lock().unwrap();
        match res {
            Ok(()) => {
                if m.delivery_count == 1 {
                    o.acked_first.insert(at.clone());
                }
                o.settled.insert(at);
            }
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {
                o.maybe_settled.insert(at);
            }
            Err(e) => panic!("unexpected settle error: {e}"),
        }
    } else if rt.below(3) != 0 {
        let _ = consumer.nack(m, true).await;
    }
    // Otherwise it is sat on until the subscription ends.
}

async fn limits_consumer(
    rt: SimRuntime,
    out: Arc<Mutex<LimitOutcomes>>,
    stop: Arc<AtomicBool>,
    queues: &'static [&'static str],
) {
    let client = Client::new(rt.clone(), client_config());
    while !stop.load(Ordering::Relaxed) {
        let queue = queues[rt.below(queues.len() as u64) as usize];
        if queue == "short" {
            // Away for a while: what is produced meanwhile waits, and expires.
            rt.sleep(TICK * (10 + rt.below(80) as u32)).await;
        }
        // `short` is read one message per subscription, taken as soon as it
        // arrives: a message waiting in the client's buffer would age there
        // and make its age at the consumer say nothing about the leader.
        let (credit, most) = if queue == "short" { (1, 1) } else { (1 + rt.below(4) as u32, 1 + rt.below(20)) };
        let mut consumer = match client.subscribe(queue, credit).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected subscribe error: {e}"),
        };
        for _ in 0..most {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let m = match consumer.next(TICK * 15).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(ClientError::GaveUp(..)) => continue,
                Err(e) => panic!("unexpected error receiving: {e}"),
            };
            handle(&rt, &out, &mut consumer, queue, &m).await;
        }
        if queue == "short" || rt.chance(1, 2) {
            consumer.close().await;
        }
    }
}

/// Notes a graveyard message once, however often it is delivered.
fn note_dead(out: &Mutex<LimitOutcomes>, m: &lotse_client::Message) {
    let queue = header(m, lotse_record::dead_letter::QUEUE).expect("a dead letter names its queue");
    let offset: u64 = header(m, lotse_record::dead_letter::OFFSET).expect("and its offset").parse().unwrap();
    let dead = Dead {
        at: m.offset(),
        reason: header(m, lotse_record::dead_letter::REASON).expect("and a reason"),
        queue: queue.clone(),
        deliveries: header(m, lotse_record::dead_letter::DELIVERIES).expect("and its deliveries").parse().unwrap(),
        value: value_of(m),
        appended_ms: m.record.timestamp,
    };
    let mut o = out.lock().unwrap();
    let copies = o.dead.entry((queue, offset)).or_default();
    if !copies.iter().any(|d| d.at == dead.at) {
        copies.push(dead);
    }
}

async fn graveyard_consumer(rt: SimRuntime, out: Arc<Mutex<LimitOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    while !stop.load(Ordering::Relaxed) {
        let mut consumer = match client.subscribe("graveyard", 3).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected subscribe error: {e}"),
        };
        for _ in 0..1 + rt.below(30) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match consumer.next(TICK * 15).await {
                Ok(Some(m)) => {
                    note_dead(&out, &m);
                    let _ = consumer.ack(&m).await;
                }
                Ok(None) | Err(ClientError::GaveUp(..)) => {}
                Err(e) => panic!("unexpected error receiving: {e}"),
            }
        }
        consumer.close().await;
    }
}

/// Counted over the seeds of a run, to see that the scenario does what it is
/// for.
static DEAD_LETTERED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static EXPIRED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static DROPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

async fn run_limits(rt: SimRuntime) {
    let mut w = World::new(rt.clone(), Scenario::limits()).await;
    let out = Arc::new(Mutex::new(LimitOutcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = vec![
        rt.spawn(limits_producer(rt.node("p1"), out.clone(), stop.clone())),
        rt.spawn(limits_consumer(rt.node("k1"), out.clone(), stop.clone(), &["poison", "drop", "poison"])),
        rt.spawn(limits_consumer(rt.node("k2"), out.clone(), stop.clone(), &["short"])),
        rt.spawn(graveyard_consumer(rt.node("g1"), out.clone(), stop.clone())),
    ];
    faults(&rt, &mut w, &["p1", "k1", "k2", "g1"], Duration::from_secs(6)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // Drain the limited queues, then the graveyard: moves that were under
    // way finish once the cluster is whole.
    let drainer =
        Client::new(rt.node("drainer"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let mut drained: BTreeMap<At, String> = BTreeMap::new();
    for queue in LIMITED {
        // Credit the drainer reads well within the ack timeout: a message
        // left in its buffer past it times out, and one at its last
        // delivery is then dead-lettered while the drainer still takes it.
        let mut consumer = drainer.subscribe(queue, 10).await.expect("the healed cluster takes a subscription");
        let mut quiet_since = rt.now();
        while rt.now() - quiet_since < ACK_TIMEOUT * 4 {
            let Some(m) = consumer.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
            quiet_since = rt.now();
            drained.insert((queue.to_string(), m.offset()), value_of(&m));
            consumer.ack(&m).await.expect("the healed cluster takes acks");
        }
    }
    let mut consumer = drainer.subscribe("graveyard", 50).await.expect("the healed cluster takes a subscription");
    let mut quiet_since = rt.now();
    while rt.now() - quiet_since < ACK_TIMEOUT * 6 {
        let Some(m) = consumer.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
        quiet_since = rt.now();
        note_dead(&out, &m);
        consumer.ack(&m).await.expect("the healed cluster takes acks");
    }
    drop(consumer);

    let out = std::mem::take(&mut *out.lock().unwrap());
    let accounted = |o: &At| {
        out.settled.contains(o) || out.maybe_settled.contains(o) || drained.contains_key(o) || out.dead.contains_key(o)
    };
    let mut expired = 0;
    for (origin, copies) in &out.dead {
        assert_eq!(copies.len(), 1, "{origin:?} was dead-lettered {} times: {copies:?}", copies.len());
        let d = &copies[0];
        assert_eq!(d.queue, origin.0);
        if let Some((value, began)) = out.produced.get(origin) {
            assert_eq!(&d.value, value, "{origin:?} was dead-lettered as {}, but {value} was produced there", d.value);
            if d.reason == "expired" {
                assert!(
                    d.appended_ms >= began + TTL.as_millis() as i64,
                    "{origin:?} was dead-lettered as expired {} ms after its produce began; the TTL is {} ms",
                    d.appended_ms - began,
                    TTL.as_millis()
                );
            }
        }
        match (d.reason.as_str(), origin.0.as_str()) {
            ("max-deliveries", "poison") => {
                assert!(
                    d.deliveries >= MAX_DELIVERIES as u16,
                    "{origin:?} was dead-lettered after {} deliveries; the limit is {MAX_DELIVERIES}",
                    d.deliveries
                );
            }
            ("expired", "short") => expired += 1,
            other => panic!("{origin:?} was dead-lettered for {other:?}"),
        }
        // A new leader does not know what its predecessor had delivered, so
        // it may expire a message a consumer holds and acknowledges late;
        // an exhausted count is persisted, so it cannot lead to this.
        assert!(
            d.reason != "max-deliveries" || !out.acked_first.contains(origin),
            "{origin:?} was acknowledged at its first delivery, then dead-lettered"
        );
        assert!(!drained.contains_key(origin), "{origin:?} was dead-lettered and is still in its queue");
    }
    let mut dropped = 0;
    for (o, (v, _)) in &out.produced {
        if accounted(o) {
            continue;
        }
        // Only `drop` has nowhere to put what it retires. Good messages go
        // that way too when their deliveries end without an ack, as they do
        // when connections break.
        assert!(o.0 == "drop", "message {o:?} ({v}) was produced and lost");
        dropped += 1;
    }
    for (o, v) in &drained {
        if let Some((p, _)) = out.produced.get(o) {
            assert_eq!(v, p, "message {o:?} holds {v}, but {p} was produced there");
        }
    }
    DEAD_LETTERED.fetch_add(out.dead.len(), Ordering::Relaxed);
    EXPIRED.fetch_add(expired, Ordering::Relaxed);
    DROPPED.fetch_add(dropped, Ordering::Relaxed);
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        eprintln!(
            "stats: produced {} settled {} drained {} dead {} (expired {expired}) dropped {dropped} oldest short {} ms",
            out.produced.len(),
            out.settled.len(),
            drained.len(),
            out.dead.len(),
            out.oldest_short_ms
        );
    }
    drop(drainer);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn queues_lose_nothing_with_dead_letters_and_message_ttl() {
    check_seeds(10, |seed| Sim::new(seed).run(run_limits));
    // Over all seeds the scenario must have done what it is for.
    assert!(DEAD_LETTERED.load(Ordering::Relaxed) > 0, "no message was dead-lettered: the run checked nothing");
    assert!(EXPIRED.load(Ordering::Relaxed) > 0, "no message expired: the TTL was not checked");
    assert!(DROPPED.load(Ordering::Relaxed) > 0, "no message was dropped: the drop rule was not checked");
}

#[test]
fn queues_lose_nothing_and_never_redeliver_a_settled_message() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

// ---------------------------------------------------------------------------
// Acks in transactions (ADR 0075).
//
// A scenario first: a transformer reads `jobs` and writes to `audit` in one
// transaction with the ack. Then a simulation: a producer writes numbered
// messages to `jobs`; transformers move each to `audit`, writing it and
// acknowledging the original in one transaction that they commit, abort or
// abandon, and some hold a message past the ack timeout first. Nodes crash
// and hosts are partitioned meanwhile. Afterwards the network heals, a last
// transformer moves what is left, and `audit` is drained.
//
// Checked: every message `jobs` acknowledged is in `audit` exactly once, a
// message whose produce outcome is unknown at most once, and nothing else
// is; `jobs` is empty at the end.

fn transformer_config(id: &str) -> ClientConfig {
    ClientConfig { transactional_id: Some(id.into()), txn_timeout: Duration::from_millis(800), ..client_config() }
}

#[test]
fn queue_acks_commit_and_abort_with_their_transaction() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let w = World::new(rt.clone(), Scenario::classic()).await;
            let quiet =
                Client::new(rt.node("p"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
            quiet.produce("jobs", 0, message("first")).await.expect("written");
            let tf = Client::new(
                rt.node("t"),
                ClientConfig {
                    retry_for: Duration::from_secs(10),
                    // Longer than the waits below: the coordinator would
                    // abort it otherwise.
                    txn_timeout: Duration::from_secs(10),
                    ..transformer_config("tf")
                },
            );
            let mut jobs = tf.subscribe("jobs", 1).await.expect("subscribed");
            let m = jobs.next(Duration::from_secs(5)).await.unwrap().expect("delivered");

            // Another connection cannot ack it in a transaction.
            let other = Client::new(
                rt.node("o"),
                ClientConfig { retry_for: Duration::from_secs(10), ..transformer_config("other") },
            );
            let mut theirs = other.subscribe("jobs", 1).await.expect("subscribed");
            let mut txn = other.begin_transaction().await.expect("begun");
            let r = theirs.ack_in(&mut txn, &m).await;
            assert!(
                matches!(r, Err(ClientError::Broker { code: lotse_proto::ErrorCode::MESSAGE_NOT_HELD, .. })),
                "{r:?}"
            );
            assert!(matches!(txn.commit().await, Err(ClientError::MustAbort(_))), "only an abort is left");

            // Aborted: the output is not delivered and the message comes back.
            let mut txn = tf.begin_transaction().await.expect("begun");
            txn.produce("audit", 0, message("made of first")).await.expect("written");
            jobs.ack_in(&mut txn, &m).await.expect("held");
            // Held: the ack timeout passes and nobody gets it.
            if let Some(x) = theirs.next(ACK_TIMEOUT * 3).await.unwrap() {
                panic!(
                    "a held message was delivered: {} at {} count {} (held {})",
                    value_of(&x),
                    x.offset(),
                    x.delivery_count,
                    m.offset()
                );
            }
            txn.abort().await.expect("aborted");
            let again = match jobs.next(Duration::from_secs(5)).await.unwrap() {
                Some(m) => m,
                None => theirs.next(Duration::from_secs(5)).await.unwrap().expect("given back after the abort"),
            };
            assert_eq!((again.offset(), value_of(&again)), (m.offset(), "first".to_string()));
            assert!(again.delivery_count >= 2, "delivered again: {}", again.delivery_count);
            drop(theirs);

            // Committed: the output is there and the message never comes back.
            let mut jobs = tf.subscribe("jobs", 1).await.expect("subscribed");
            let m = jobs.next(Duration::from_secs(5)).await.unwrap().expect("delivered");
            let mut txn = tf.begin_transaction().await.expect("begun");
            txn.produce("audit", 0, message("made of first")).await.expect("written");
            jobs.ack_in(&mut txn, &m).await.expect("held");
            txn.commit().await.expect("committed");
            let mut audit = quiet.subscribe("audit", 10).await.expect("subscribed");
            let out = audit.next(Duration::from_secs(5)).await.unwrap().expect("the committed output");
            assert_eq!(value_of(&out), "made of first");
            assert!(audit.next(ACK_TIMEOUT).await.unwrap().is_none(), "the aborted output stays hidden");
            drop(jobs);
            let mut fresh = quiet.subscribe("jobs", 10).await.expect("subscribed");
            assert!(fresh.next(ACK_TIMEOUT * 3).await.unwrap().is_none(), "the committed ack settled it");
            drop((fresh, audit));
            for (node, handle) in w.running.into_values().flatten() {
                handle.stop();
                let _ = node.join().await;
            }
        })
    });
}

/// The node that leads queue `name`, as the metadata says.
async fn queue_leader(rt: &SimRuntime, c: &Client<SimRuntime>, name: &str) -> NodeId {
    loop {
        if let Ok(meta) = c.metadata(&[name]).await
            && let Some(p) = meta.resources.iter().find(|r| r.name == name).and_then(|r| r.partitions.first())
            && p.leader != 0
        {
            return NodeId::from(p.leader);
        }
        rt.sleep(TICK * 10).await;
    }
}

/// ADR 0109: what a leader handed to a consumer is in the log, so after a
/// failover the consumer keeps it and acks it in a transaction at the new
/// leader, and a delivery count holds across leaders. A consumer that does
/// not come back loses its message after the queue's resume window, to
/// another consumer, with its count.
#[test]
fn a_transactional_ack_survives_a_failover() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            // An ack timeout longer than the failovers below: only the
            // resume window gives a message back here.
            let mut scenario = Scenario::classic();
            for q in &mut scenario.queues {
                q.ack_timeout = Duration::from_secs(120);
            }
            let mut w = World::new(rt.clone(), scenario).await;
            let patient = || ClientConfig { retry_for: Duration::from_secs(20), ..client_config() };
            let quiet = Client::new(rt.node("p"), patient());
            quiet.produce("jobs", 0, message("first")).await.expect("written");
            let tf = Client::new(
                rt.node("t"),
                ClientConfig { transactional_id: Some("tf".into()), txn_timeout: Duration::from_secs(30), ..patient() },
            );
            let mut jobs = tf.subscribe("jobs", 1).await.expect("subscribed");
            let m = jobs.next(Duration::from_secs(5)).await.unwrap().expect("delivered");
            assert_eq!(m.delivery_count, 1);

            // The leader goes after the delivery, before the ack.
            let mut txn = tf.begin_transaction().await.expect("begun");
            txn.produce("audit", 0, message("made of first")).await.expect("written");
            let leader = queue_leader(&rt, &quiet, "jobs").await;
            w.crash(leader, false).await;
            jobs.ack_in(&mut txn, &m).await.expect("held at the new leader");
            txn.commit().await.expect("committed");
            w.start(leader).await;

            let mut audit = quiet.subscribe("audit", 10).await.expect("subscribed");
            let out = audit.next(Duration::from_secs(5)).await.unwrap().expect("the committed output");
            assert_eq!(value_of(&out), "made of first");
            assert!(audit.next(Duration::from_secs(1)).await.unwrap().is_none(), "written once");
            drop(audit);

            // A consumer cut off for good: its message waits for it at the
            // new leader, then goes to another one with its count.
            drop(jobs);
            let gone = Client::new(rt.node("g"), patient());
            let mut lost = gone.subscribe("jobs", 1).await.expect("subscribed");
            quiet.produce("jobs", 0, message("second")).await.expect("written");
            let second = lost.next(Duration::from_secs(5)).await.unwrap().expect("delivered");
            assert_eq!((value_of(&second), second.delivery_count), ("second".to_string(), 1));
            for n in 1..=3 {
                rt.net().block("g", &format!("n{n}"));
            }
            let leader = queue_leader(&rt, &quiet, "jobs").await;
            w.crash(leader, false).await;
            let other = Client::new(rt.node("o"), patient());
            let mut theirs = other.subscribe("jobs", 1).await.expect("subscribed");
            let t0 = rt.now();
            let again = theirs.next(Duration::from_secs(60)).await.unwrap().expect("given back after the window");
            assert_eq!((value_of(&again), again.offset()), ("second".to_string(), second.offset()));
            let waited = rt.now() - t0;
            assert!(waited >= QueueConfig::DEFAULT_RESUME_WINDOW / 2, "kept for its consumer a while: {waited:?}");
            assert_eq!(again.delivery_count, 2, "the first delivery counts across leaders");
            theirs.ack(&again).await.expect("acked");
            drop(lost);
            drop(gone);
            w.start(leader).await;

            // A requeue and a failover: the count still holds.
            quiet.produce("jobs", 0, message("third")).await.expect("written");
            let m = theirs.next(Duration::from_secs(5)).await.unwrap().expect("delivered");
            theirs.nack(&m, true).await.expect("given back");
            let m = theirs.next(Duration::from_secs(5)).await.unwrap().expect("delivered again");
            assert_eq!(m.delivery_count, 2);
            let leader = queue_leader(&rt, &quiet, "jobs").await;
            w.crash(leader, false).await;
            theirs.nack(&m, true).await.expect("given back at the new leader");
            let m = theirs.next(Duration::from_secs(10)).await.unwrap().expect("delivered a third time");
            assert_eq!(m.delivery_count, 3);
            theirs.ack(&m).await.expect("acked");
            drop(theirs);
            w.start(leader).await;
            for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
                handle.stop();
                let _ = node.join().await;
            }
        })
    });
}

#[derive(Default)]
struct MoveOutcomes {
    /// Values `jobs` acknowledged.
    produced: BTreeSet<String>,
    /// Values whose produce outcome is unknown.
    maybe: BTreeSet<String>,
    /// Transactions that committed, as their clients saw it.
    committed: u64,
}

async fn jobs_producer(rt: SimRuntime, name: &'static str, out: Arc<Mutex<MoveOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        let value = format!("{name}-{n}");
        n += 1;
        let res = client.produce("jobs", 0, message(&value)).await;
        {
            let mut o = out.lock().unwrap();
            match res {
                Ok(_) => o.produced.insert(value),
                Err(_) => o.maybe.insert(value),
            };
        }
        // Slower than the transformers move them, so the last one has
        // little left.
        rt.sleep(TICK * (6 + rt.below(10) as u32)).await;
    }
}

/// Moves one message from `jobs` to `audit` in a transaction. `Ok(None)`
/// when none came.
async fn move_one(
    rt: &SimRuntime,
    client: &Client<SimRuntime>,
    jobs: &mut lotse_client::Consumer<'_, SimRuntime>,
    flaky: bool,
) -> Result<Option<bool>, ClientError> {
    let Some(m) = jobs.next(TICK * 20).await? else { return Ok(None) };
    let mut txn = client.begin_transaction().await?;
    let work = async {
        txn.produce("audit", 0, message(&value_of(&m))).await?;
        if flaky && rt.chance(1, 15) {
            rt.sleep(ACK_TIMEOUT + TICK * 10).await;
        }
        jobs.ack_in(&mut txn, &m).await
    };
    if work.await.is_err() {
        let _ = txn.abort().await;
        return Ok(Some(false));
    }
    if flaky {
        match rt.below(8) {
            0 => {
                drop(txn);
                return Ok(Some(false));
            }
            1 => {
                let _ = txn.abort().await;
                return Ok(Some(false));
            }
            _ => {}
        }
    }
    Ok(Some(txn.commit().await.is_ok()))
}

async fn transformer(rt: SimRuntime, id: &'static str, out: Arc<Mutex<MoveOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), transformer_config(id));
    while !stop.load(Ordering::Relaxed) {
        let Ok(mut jobs) = client.subscribe("jobs", 1 + rt.below(3) as u32).await else { continue };
        for _ in 0..1 + rt.below(20) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match move_one(&rt, &client, &mut jobs, true).await {
                Ok(Some(true)) => out.lock().unwrap().committed += 1,
                Ok(_) => {}
                Err(_) => break,
            }
        }
        if rt.chance(1, 2) {
            jobs.close().await;
        }
    }
}

async fn run_moves(rt: SimRuntime) {
    let mut w = World::new(rt.clone(), Scenario::classic()).await;
    let out = Arc::new(Mutex::new(MoveOutcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = vec![
        rt.spawn(jobs_producer(rt.node("p1"), "p1", out.clone(), stop.clone())),
        rt.spawn(transformer(rt.node("t1"), "t1", out.clone(), stop.clone())),
        rt.spawn(transformer(rt.node("t2"), "t2", out.clone(), stop.clone())),
    ];
    faults(&rt, &mut w, &["p1", "t1", "t2"], Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // What is left in `jobs` moves now; held messages come back once their
    // transactions time out.
    let last =
        Client::new(rt.node("last"), ClientConfig { retry_for: Duration::from_secs(20), ..transformer_config("last") });
    let mut jobs = last.subscribe("jobs", 10).await.expect("the healed cluster takes a subscription");
    let mut quiet_since = rt.now();
    while rt.now() - quiet_since < Duration::from_secs(4) {
        match move_one(&rt, &last, &mut jobs, false).await {
            Ok(None) => {}
            Ok(Some(_)) => quiet_since = rt.now(),
            Err(e) => panic!("the healed cluster moves messages: {e}"),
        }
    }
    drop(jobs);

    let drainer =
        Client::new(rt.node("drainer"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let mut audit = drainer.subscribe("audit", 50).await.expect("subscribed");
    let mut found: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    let mut quiet_since = rt.now();
    while rt.now() - quiet_since < ACK_TIMEOUT * 4 {
        let Some(m) = audit.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
        quiet_since = rt.now();
        found.entry(value_of(&m)).or_default().push(m.offset());
        audit.ack(&m).await.expect("the healed cluster takes acks");
    }
    let mut left = drainer.subscribe("jobs", 50).await.expect("subscribed");
    let rest = left.next(ACK_TIMEOUT * 2).await.expect("the healed cluster delivers");
    assert!(
        rest.is_none(),
        "{:?} is still in jobs after the last transformer moved everything",
        rest.map(|m| value_of(&m))
    );

    let out = std::mem::take(&mut *out.lock().unwrap());
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        eprintln!(
            "stats: produced {} maybe {} committed by transformers {} in audit {}",
            out.produced.len(),
            out.maybe.len(),
            out.committed,
            found.len()
        );
    }
    assert!(out.committed > 0, "no transaction committed: the run checked little");
    for v in &out.produced {
        let at = found.get(v).map_or(&[][..], |o| &o[..]);
        assert_eq!(at.len(), 1, "{v} was produced once and is in audit at {at:?}");
    }
    for (v, at) in &found {
        assert!(out.produced.contains(v) || out.maybe.contains(v), "{v} is in audit but was never produced");
        assert_eq!(at.len(), 1, "{v} is in audit {} times, at {at:?}", at.len());
    }
    drop((left, audit));
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_message_moves_from_a_queue_exactly_once_under_faults() {
    check_seeds(10, |seed| Sim::new(seed).run(run_moves));
}

// ---------------------------------------------------------------------------
// Single active consumer (ADR 0076).
//
// A producer writes numbered messages to `line`, a queue with a single
// active consumer. Three consumers subscribe, take messages, ack them, now
// and then hold one past the ack timeout, unsubscribe, or drop their client
// (a disconnect), while nodes crash and hosts are partitioned. Afterwards
// the network heals and a last consumer takes what is left.
//
// Checked: each subscription receives offsets in increasing order. A
// subscription gets its messages before newer ones only if no one else had
// them when it took over, which is what the handover promises; without it,
// a consumer that takes over while another still holds older messages, or
// two that receive at once, see an older message after a newer one. And
// nothing is lost: every message the queue acknowledged was acknowledged by
// a consumer or is delivered at the end.

#[derive(Default)]
struct LineOutcomes {
    produced: BTreeSet<String>,
    maybe_produced: BTreeSet<String>,
    acked: BTreeSet<String>,
    maybe_acked: BTreeSet<String>,
    /// Per subscription (consumer, its client's round, the subscription's
    /// number): the offsets it received, in order.
    received: BTreeMap<(String, u64, u64), Vec<u64>>,
    /// A consumer found itself a standby.
    standby_seen: bool,
}

async fn line_producer(rt: SimRuntime, out: Arc<Mutex<LineOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        let value = format!("m-{n}");
        n += 1;
        let res = client.produce("line", 0, message(&value)).await;
        {
            let mut o = out.lock().unwrap();
            match res {
                Ok(_) => o.produced.insert(value),
                Err(_) => o.maybe_produced.insert(value),
            };
        }
        rt.sleep(TICK * (2 + rt.below(6) as u32)).await;
    }
}

async fn line_consumer(rt: SimRuntime, name: &'static str, out: Arc<Mutex<LineOutcomes>>, stop: Arc<AtomicBool>) {
    let mut client = Client::new(rt.clone(), client_config());
    let mut round = 0;
    while !stop.load(Ordering::Relaxed) {
        round += 1;
        let Ok(mut c) = client.subscribe("line", 1 + rt.below(3) as u32).await else { continue };
        for _ in 0..1 + rt.below(30) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match c.next(TICK * 20).await {
                Ok(Some(m)) => {
                    let key = (name.to_string(), round, c.subscriptions());
                    out.lock().unwrap().received.entry(key).or_default().push(m.offset());
                    if rt.chance(1, 25) {
                        rt.sleep(ACK_TIMEOUT + TICK * 10).await;
                    }
                    let res = c.ack(&m).await;
                    let mut o = out.lock().unwrap();
                    match res {
                        Ok(()) => o.acked.insert(value_of(&m)),
                        Err(_) => o.maybe_acked.insert(value_of(&m)),
                    };
                }
                Ok(None) => {
                    if c.is_standby() {
                        out.lock().unwrap().standby_seen = true;
                    }
                }
                Err(_) => break,
            }
        }
        match rt.below(3) {
            0 => c.close().await,
            1 => drop(c),
            _ => {
                // The connection goes: whatever it held is released.
                drop(c);
                client = Client::new(rt.clone(), client_config());
            }
        }
    }
}

async fn run_line(rt: SimRuntime) {
    let mut w = World::new(rt.clone(), Scenario::single_active()).await;
    let out = Arc::new(Mutex::new(LineOutcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = vec![
        rt.spawn(line_producer(rt.node("p"), out.clone(), stop.clone())),
        rt.spawn(line_consumer(rt.node("c1"), "c1", out.clone(), stop.clone())),
        rt.spawn(line_consumer(rt.node("c2"), "c2", out.clone(), stop.clone())),
        rt.spawn(line_consumer(rt.node("c3"), "c3", out.clone(), stop.clone())),
    ];
    faults(&rt, &mut w, &["p", "c1", "c2", "c3"], Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    let last = Client::new(rt.node("last"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let mut c = last.subscribe("line", 50).await.expect("the healed cluster takes a subscription");
    let mut drained = BTreeSet::new();
    let mut quiet_since = rt.now();
    while rt.now() - quiet_since < ACK_TIMEOUT * 4 {
        let Some(m) = c.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
        quiet_since = rt.now();
        let key = ("last".to_string(), 0, c.subscriptions());
        out.lock().unwrap().received.entry(key).or_default().push(m.offset());
        c.ack(&m).await.expect("the healed cluster takes acks");
        drained.insert(value_of(&m));
    }
    assert!(!c.is_standby(), "the last consumer took over");
    drop(c);

    let out = std::mem::take(&mut *out.lock().unwrap());
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        let handovers = out.received.len();
        eprintln!(
            "stats: produced {} acked {} drained {} subscriptions that received {} standby seen {}",
            out.produced.len(),
            out.acked.len(),
            drained.len(),
            handovers,
            out.standby_seen
        );
    }
    for (who, offsets) in &out.received {
        assert!(
            offsets.windows(2).all(|p| p[0] < p[1]),
            "{who:?} received {offsets:?}: an older message after a newer one"
        );
    }
    for v in &out.produced {
        assert!(
            out.acked.contains(v) || out.maybe_acked.contains(v) || drained.contains(v),
            "{v} was produced and neither acknowledged nor delivered at the end"
        );
    }
    for (_, n) in std::mem::take(&mut w.running) {
        if let Some((node, handle)) = n {
            handle.stop();
            let _ = node.join().await;
        }
    }
}

#[test]
fn a_single_active_consumer_hands_over_in_order_under_faults() {
    check_seeds(10, |seed| Sim::new(seed).run(run_line));
}

// ---------------------------------------------------------------------------
// Delayed delivery (ADR 0077).

/// A message whose producer asks the queue to wait `ms` before delivering it.
fn delayed(value: &str, ms: u64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    let delay = ms.to_string();
    b.push(0, None, Some(value.as_bytes()), &[(lotse_record::delay::HEADER, Some(delay.as_bytes()))]);
    b.finish().unwrap()
}

#[test]
fn delayed_messages_wait_for_their_time_across_a_failover() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), Scenario::classic()).await;
            let c = Client::new(rt.node("p"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
            // A publish delay holds one message back while the next goes.
            let t0 = rt.now();
            c.produce("jobs", 0, delayed("later", 2_000)).await.expect("written");
            c.produce("jobs", 0, message("now")).await.expect("written");
            let mut sub = c.subscribe("jobs", 10).await.expect("subscribed");
            let m = sub.next(Duration::from_secs(1)).await.unwrap().expect("the undelayed one");
            assert_eq!(value_of(&m), "now");
            sub.ack(&m).await.unwrap();
            let m = sub.next(Duration::from_secs(5)).await.unwrap().expect("the delayed one, in time");
            assert_eq!(value_of(&m), "later");
            assert!(rt.now() - t0 >= Duration::from_secs(2), "delivered after {:?}", rt.now() - t0);
            sub.ack(&m).await.unwrap();

            // A delayed requeue survives a failover: the new leader waits too.
            c.produce("jobs", 0, message("retry")).await.expect("written");
            let m = sub.next(Duration::from_secs(5)).await.unwrap().expect("delivered");
            let t1 = rt.now();
            sub.nack_after(&m, Duration::from_secs(3)).await.expect("given back for later");
            let meta = c.metadata(&["jobs"]).await.unwrap();
            let leader = meta.resources.iter().find(|r| r.name == "jobs").unwrap().partitions[0].leader;
            w.crash(u64::from(leader), false).await;
            let again = sub.next(Duration::from_secs(15)).await.unwrap().expect("delivered again");
            assert_eq!(value_of(&again), "retry");
            assert!(rt.now() - t1 >= Duration::from_secs(3), "redelivered after {:?}", rt.now() - t1);
            assert_eq!(again.delivery_count, 2);
            sub.ack(&again).await.unwrap();
            drop(sub);
            w.start(u64::from(leader)).await;
            for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
                handle.stop();
                let _ = node.join().await;
            }
        })
    });
}

// A producer writes messages to `jobs`, a queue with three priority levels,
// many asking for a delay of up to 1.5 s and each for a level; consumers
// take them, ack most, give some back with a delay or
// without, and drop their subscriptions, while nodes crash and hosts are
// partitioned. Afterwards the network heals and a last consumer takes what
// is left.
//
// Checked: no message is received before the time its producer asked for
// (its produce request was sent before the leader appended it, so its due
// time is later than that plus the delay); and nothing is lost. Times are
// whole ms of the wall clock, as the leader stamps them: a delay is a
// number of ms from an append time in ms.

const MAX_DELAY_MS: u64 = 1_500;

#[derive(Default)]
struct DelayOutcomes {
    /// Value -> when the produce request was sent (Unix ms) and the delay
    /// it asked for.
    sent: BTreeMap<String, (u64, u64)>,
    produced: BTreeSet<String>,
    acked: BTreeSet<String>,
    maybe_acked: BTreeSet<String>,
    /// Receipts before their time: (value, how many ms early).
    early: Vec<(String, u64)>,
    delayed_received: u64,
}

async fn delay_producer(rt: SimRuntime, out: Arc<Mutex<DelayOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut n = 0;
    while !stop.load(Ordering::Relaxed) {
        let value = format!("d-{n}");
        n += 1;
        let ms = if rt.chance(1, 2) { rt.below(MAX_DELAY_MS) } else { 0 };
        out.lock().unwrap().sent.insert(value.clone(), (unix_ms(&rt), ms));
        // Priorities mixed in (ADR 0078): they change the order, never
        // what arrives or when it may.
        let level = rt.below(4).to_string();
        let mut b = BatchBuilder::new(BatchOptions::default());
        let delay = ms.to_string();
        let mut headers = vec![(lotse_record::priority::HEADER, Some(level.as_bytes()))];
        if ms > 0 {
            headers.push((lotse_record::delay::HEADER, Some(delay.as_bytes())));
        }
        b.push(0, None, Some(value.as_bytes()), &headers);
        let batch = b.finish().unwrap();
        if client.produce("jobs", 0, batch).await.is_ok() {
            out.lock().unwrap().produced.insert(value);
        }
        rt.sleep(TICK * (2 + rt.below(6) as u32)).await;
    }
}

fn received(out: &Mutex<DelayOutcomes>, rt: &SimRuntime, m: &lotse_client::Message) {
    let v = value_of(m);
    let mut o = out.lock().unwrap();
    let (sent, ms) = o.sent[&v];
    let (due, now) = (sent + ms, unix_ms(rt));
    if ms > 0 {
        o.delayed_received += 1;
    }
    if now < due {
        o.early.push((v, due - now));
    }
}

fn unix_ms(rt: &SimRuntime) -> u64 {
    rt.unix_time().as_millis() as u64
}

async fn delay_consumer(rt: SimRuntime, out: Arc<Mutex<DelayOutcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    while !stop.load(Ordering::Relaxed) {
        let Ok(mut c) = client.subscribe("jobs", 1 + rt.below(4) as u32).await else { continue };
        for _ in 0..1 + rt.below(30) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match c.next(TICK * 20).await {
                Ok(Some(m)) => {
                    received(&out, &rt, &m);
                    let v = value_of(&m);
                    match rt.below(10) {
                        0 => {
                            let _ = c.nack_after(&m, Duration::from_millis(rt.below(1_000))).await;
                        }
                        1 => {
                            let _ = c.nack(&m, true).await;
                        }
                        _ => {
                            let res = c.ack(&m).await;
                            let mut o = out.lock().unwrap();
                            match res {
                                Ok(()) => o.acked.insert(v),
                                Err(_) => o.maybe_acked.insert(v),
                            };
                        }
                    }
                }
                Ok(None) => {}
                Err(_) => break,
            }
        }
        if rt.chance(1, 2) {
            c.close().await;
        }
    }
}

async fn run_delays(rt: SimRuntime) {
    let mut scenario = Scenario::classic();
    scenario.queues[0].priority_levels = 3;
    let mut w = World::new(rt.clone(), scenario).await;
    let out = Arc::new(Mutex::new(DelayOutcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let tasks = vec![
        rt.spawn(delay_producer(rt.node("p"), out.clone(), stop.clone())),
        rt.spawn(delay_consumer(rt.node("c1"), out.clone(), stop.clone())),
        rt.spawn(delay_consumer(rt.node("c2"), out.clone(), stop.clone())),
    ];
    faults(&rt, &mut w, &["p", "c1", "c2"], Duration::from_secs(5)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }
    let last = Client::new(rt.node("last"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let mut c = last.subscribe("jobs", 50).await.expect("the healed cluster takes a subscription");
    let mut drained = BTreeSet::new();
    let mut quiet_since = rt.now();
    // Longer than any delay, so a message that waits is not taken for lost.
    while rt.now() - quiet_since < Duration::from_millis(MAX_DELAY_MS) + ACK_TIMEOUT * 4 {
        let Some(m) = c.next(TICK * 20).await.expect("the healed cluster delivers") else { continue };
        quiet_since = rt.now();
        received(&out, &rt, &m);
        c.ack(&m).await.expect("the healed cluster takes acks");
        drained.insert(value_of(&m));
    }
    drop(c);
    let out = std::mem::take(&mut *out.lock().unwrap());
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        eprintln!(
            "stats: produced {} acked {} drained {} delayed received {}",
            out.produced.len(),
            out.acked.len(),
            drained.len(),
            out.delayed_received
        );
    }
    assert!(out.early.is_empty(), "received before their time: {:?}", out.early);
    for v in &out.produced {
        assert!(
            out.acked.contains(v) || out.maybe_acked.contains(v) || drained.contains(v),
            "{v} was produced and neither acknowledged nor delivered at the end"
        );
    }
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn delayed_messages_never_arrive_early_under_faults() {
    check_seeds(10, |seed| Sim::new(seed).run(run_delays));
}

// ---------------------------------------------------------------------------
// Priorities (ADR 0078).

/// A message at priority `level`.
fn ranked(value: &str, level: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[(lotse_record::priority::HEADER, Some(level.as_bytes()))]);
    b.finish().unwrap()
}

#[test]
fn higher_priorities_go_first_also_after_a_failover() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), Scenario::priorities()).await;
            let c = Client::new(rt.node("p"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
            let fill = async |tag: &str| {
                for i in 0..3 {
                    c.produce("ranked", 0, message(&format!("{tag}low{i}"))).await.unwrap();
                }
                c.produce("ranked", 0, ranked(&format!("{tag}mid"), "1")).await.unwrap();
                c.produce("ranked", 0, ranked(&format!("{tag}high0"), "3")).await.unwrap();
                // Above the highest level: the highest.
                c.produce("ranked", 0, ranked(&format!("{tag}high1"), "9")).await.unwrap();
                c.produce("ranked", 0, ranked(&format!("{tag}junk"), "soon")).await.unwrap();
            };
            let expected = |tag: &str| {
                ["high0", "high1", "mid", "low0", "low1", "low2", "junk"].map(|v| format!("{tag}{v}")).to_vec()
            };
            // Credit 1 takes one message per round; more takes runs of
            // several levels at once, across batches.
            let take = async |n: usize, credit: u32| {
                let mut sub = c.subscribe("ranked", credit).await.expect("subscribed");
                let mut got = Vec::new();
                while got.len() < n {
                    let m = sub.next(Duration::from_secs(10)).await.unwrap().expect("delivered");
                    got.push(value_of(&m));
                    sub.ack(&m).await.unwrap();
                }
                got
            };
            fill("a-").await;
            assert_eq!(take(7, 1).await, expected("a-"));

            // A new leader reads the levels from the log again.
            fill("b-").await;
            let meta = c.metadata(&["ranked"]).await.unwrap();
            let leader = meta.resources.iter().find(|r| r.name == "ranked").unwrap().partitions[0].leader;
            w.crash(u64::from(leader), false).await;
            assert_eq!(take(7, 7).await, expected("b-"));
            w.start(u64::from(leader)).await;
            for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
                handle.stop();
                let _ = node.join().await;
            }
        })
    });
}

// A queue's leader is cut off from every other node while a client can
// still reach it. Until check-quorum steps it down it goes on saying it
// leads; once no quorum has answered it for an election timeout it no
// longer reports the queue in `DescribeMetrics` (ADR 0095), and from then
// on it hands out nothing, also to a consumer that subscribes there now
// (ADR 0097): another leader may already deliver the same messages.
#[test]
fn a_cut_off_leader_hands_out_nothing_once_no_quorum_answers() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), Scenario::classic()).await;
            let c = Client::new(rt.node("p"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
            for i in 0..3 {
                c.produce("jobs", 0, message(&format!("j{i}"))).await.expect("written");
            }
            let meta = c.metadata(&["jobs"]).await.unwrap();
            let jobs = meta.resources.iter().find(|r| r.name == "jobs").unwrap();
            let (queue_id, leader) = (jobs.resource_id, u64::from(jobs.partitions[0].leader));
            assert_ne!(leader, 0);
            let cut = format!("n{leader}");
            for other in w.cfgs.keys().filter(|id| **id != leader) {
                rt.net().block(&cut, &format!("n{other}"));
            }

            // The cut-off node stops listing the queue as its own.
            let deadline = rt.now() + Duration::from_secs(5);
            loop {
                assert!(rt.now() < deadline, "node {leader} still lists the queue");
                let answers = c.node_metrics().await.expect("the nodes");
                let mine = answers.into_iter().find(|(n, _)| u64::from(*n) == leader).and_then(|(_, r)| r.ok());
                if mine.is_some_and(|m| !m.queues.iter().any(|q| q.resource_id == queue_id)) {
                    break;
                }
                rt.sleep(TICK).await;
            }

            // A consumer that reaches only that node gets nothing from it:
            // it is refused, or its subscription ends, or nothing comes.
            let only = ClientConfig {
                retry_for: Duration::from_millis(300),
                ..ClientConfig::new(vec![format!("{cut}:9000")])
            };
            for other in w.cfgs.keys().filter(|id| **id != leader) {
                rt.net().block("q", &format!("n{other}"));
            }
            let alone = Client::new(rt.node("q"), only);
            if let Ok(mut sub) = alone.subscribe("jobs", 10).await {
                let deadline = rt.now() + TICK * 40;
                while rt.now() < deadline {
                    match sub.next(TICK * 5).await {
                        Ok(Some(m)) => panic!("node {leader} handed out {} without a quorum", value_of(&m)),
                        Ok(None) => {}
                        Err(_) => break,
                    }
                }
            }

            rt.net().heal();
            for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
                handle.stop();
                let _ = node.join().await;
            }
        })
    });
}

//! Resources created at runtime against a faulty cluster, checked over many
//! seeds.
//!
//! Four or five nodes start with a metadata group, a coordinator group and
//! one configured stream, `base`. Two admin clients create the same streams, queues and an exchange
//! concurrently and bind the queues, while a producer writes to whatever
//! streams exist and a publisher publishes through the exchange; pairs of
//! hosts are partitioned and nodes lose power. After healing every node is
//! asked what exists.
//!
//! Checked:
//! - every node reports the same resources with the same ids, and one id
//!   per name: concurrent creates of a name agree on its id;
//! - a configured name cannot be created again;
//! - an acknowledged create is there, with the id it was acknowledged with,
//!   its partitions and as many voters per partition as asked for;
//! - each node runs exactly the groups it is a voter of;
//! - every acknowledged produce to a created stream is there at its offset;
//! - a published message goes only to queues that were bound to the
//!   exchange.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, OutMessage};
use lotse_log::LogConfig;
use lotse_proto::kind;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine};
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

fn message(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn client_config(n: u64) -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(500),
        retry_for: Duration::from_millis(1_500),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(400),
        metadata_max_age: Duration::from_millis(300),
        ..ClientConfig::new((1..=n).map(|i| format!("n{i}:9000")).collect())
    }
}

/// What a create should give: kind, partitions, replication.
fn spec(name: &str) -> (u8, u32, u8) {
    let i: u32 = name[1..].parse().unwrap();
    match &name[..1] {
        "s" => (kind::STREAM, 1 + i % 3, 1 + (i % 3) as u8),
        "q" => (kind::QUEUE, 1, 3),
        _ => (kind::EXCHANGE, 0, 0),
    }
}

#[derive(Default)]
struct Outcomes {
    /// Acknowledged creates (or `ResourceExists`): name → id.
    ids: BTreeMap<String, BTreeSet<u64>>,
    /// Queues ever bound to the exchange (or tried).
    bound: BTreeSet<String>,
    /// Acknowledged produces: (stream, partition, offset) → value.
    produced: BTreeMap<(String, u32, u64), String>,
    published: u64,
}

struct World {
    rt: SimRuntime,
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
    down_until: BTreeMap<NodeId, lotse_rt::Instant>,
}

impl World {
    async fn new(rt: SimRuntime, n: u64) -> World {
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        // Group 1 is the metadata group, 2 coordinates, 3 is `base`.
                        groups: vec![1, 2, 3],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        streams: vec![lotse_server::StreamConfig {
                            name: "base".into(),
                            resource_id: 7,
                            partitions: vec![3],
                        }],
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
                        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
                        metadata: Some(1),
                        coordinator: Some(2),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
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

/// What one node reports: name → (id, kind, voters per partition).
type View = BTreeMap<String, (u64, u8, Vec<Vec<u32>>)>;

const NAMES: [&str; 7] = ["s1", "s2", "s3", "q1", "q2", "q3", "x1"];

async fn admin(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    while !stop.load(Ordering::Relaxed) {
        if rt.chance(1, 10) {
            // The configured stream exists already, everywhere.
            match client.create_stream("base", 1, 1).await {
                Err(ClientError::ResourceExists { resource_id: 7, .. }) => {}
                Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                other => panic!("a configured name was created again: {other:?}"),
            }
        }
        let name = NAMES[rt.below(NAMES.len() as u64) as usize];
        let (k, partitions, replication) = spec(name);
        let res = match k {
            kind::STREAM => client.create_stream(name, partitions, replication).await,
            kind::QUEUE => client.create_queue(name, replication, None).await,
            _ => client.declare_exchange(name, ExchangeKind::Fanout).await,
        };
        match res {
            Ok(id) | Err(ClientError::ResourceExists { resource_id: id, .. }) => {
                out.lock().unwrap().ids.entry(name.into()).or_default().insert(id);
            }
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
            Err(e) => panic!("unexpected error creating {name}: {e}"),
        }
        if rt.chance(1, 3) {
            let q = ["q1", "q2"][rt.below(2) as usize];
            out.lock().unwrap().bound.insert(q.into());
            match client.bind("x1", q, "").await {
                Ok(()) | Err(ClientError::GaveUp(..) | ClientError::Unknown(_) | ClientError::UnknownStream(..)) => {}
                Err(e) => panic!("unexpected error binding {q}: {e}"),
            }
        }
        rt.sleep(TICK * (5 + rt.below(20) as u32)).await;
    }
}

async fn producer(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    let mut seq = 0;
    while !stop.load(Ordering::Relaxed) {
        let stream = ["s1", "s2", "s3"][rt.below(3) as usize];
        let partition = rt.below(u64::from(spec(stream).1)) as u32;
        let value = format!("m{seq}");
        seq += 1;
        match client.produce(stream, partition, message(&value)).await {
            Ok(offset) => {
                out.lock().unwrap().produced.insert((stream.into(), partition, offset), value.clone());
            }
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_) | ClientError::UnknownStream(..)) => {}
            Err(e) => panic!("unexpected produce error: {e}"),
        }
        if rt.chance(1, 3) {
            match client.publish("x1", "", OutMessage::value(value.clone()), false).await {
                Ok(p) => {
                    let o = out.lock().unwrap();
                    for (q, _) in &p.routed {
                        assert!(o.bound.contains(q), "{value} was routed to {q}, which was never bound");
                    }
                    drop(o);
                    out.lock().unwrap().published += 1;
                }
                Err(ClientError::NotAllWritten { written, .. }) => {
                    let o = out.lock().unwrap();
                    for (q, _) in &written {
                        assert!(o.bound.contains(q), "{value} was routed to {q}, which was never bound");
                    }
                }
                Err(ClientError::GaveUp(..) | ClientError::Unknown(_) | ClientError::UnknownStream(..)) => {}
                Err(e) => panic!("unexpected publish error: {e}"),
            }
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
}

async fn run(rt: SimRuntime) {
    let n = 4 + rt.below(2);
    let mut w = World::new(rt.clone(), n).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for a in 1..=2 {
        tasks.push(rt.spawn(admin(rt.node(&format!("a{a}")), n, out.clone(), stop.clone())));
    }
    tasks.push(rt.spawn(producer(rt.node("p1"), n, out.clone(), stop.clone())));
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["a1", "a2", "p1"].map(String::from));
    let end = rt.now() + Duration::from_secs(5);
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
        // A minority of the metadata group, and never more than one node:
        // a group of two or three replicas must keep a majority.
        if down == 0 && rt.chance(1, 40) {
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
    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(!out.ids.is_empty(), "nothing was created: the run checked nothing");
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        eprintln!(
            "stats: nodes {n} created {} produced {} published {} bound {:?}",
            out.ids.len(),
            out.produced.len(),
            out.published,
            out.bound
        );
    }

    // Every node reports the same resources, once caught up.
    let checker =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(n) });
    let deadline = rt.now() + Duration::from_secs(30);
    let mut views: BTreeMap<NodeId, View>;
    loop {
        views = BTreeMap::new();
        for id in 1..=n {
            let c = Client::new(
                rt.node("checker"),
                ClientConfig { bootstrap: vec![format!("n{id}:9000")], ..client_config(n) },
            );
            let m = c.metadata(&[]).await.expect("the healed cluster answers metadata");
            let view = m
                .resources
                .iter()
                .map(|r| {
                    let voters = r.partitions.iter().map(|p| p.voters.clone()).collect();
                    (r.name.clone(), (r.resource_id, r.kind, voters))
                })
                .collect();
            views.insert(id, view);
        }
        let first = views[&1].clone();
        let created = out.ids.keys().all(|name| first.contains_key(name));
        if views.values().all(|v| *v == first) && created {
            break;
        }
        assert!(rt.now() < deadline, "the nodes do not agree on what exists: {views:?}");
        rt.sleep(TICK * 20).await;
    }
    let view = &views[&1];
    for (name, ids) in &out.ids {
        assert_eq!(ids.len(), 1, "{name} was acknowledged with different ids: {ids:?}");
        let (id, k, voters) = &view[name];
        assert!(ids.contains(id), "{name} exists as {id}, but was acknowledged as {ids:?}");
        let (want_kind, partitions, replication) = spec(name);
        assert_eq!(*k, want_kind, "{name} has the wrong kind");
        if want_kind != kind::EXCHANGE {
            assert_eq!(voters.len() as u32, partitions, "{name} has the wrong number of partitions");
            for v in voters {
                assert_eq!(v.len(), usize::from(replication), "{name}: a partition with voters {v:?}");
            }
        }
    }

    // Each node runs exactly the groups it is a voter of.
    let deadline = rt.now() + Duration::from_secs(10);
    loop {
        let mut wrong = Vec::new();
        for (id, entry) in &w.running {
            let (_, handle) = entry.as_ref().expect("all nodes are up");
            for (name, (rid, _, voters)) in view.iter().filter(|(_, (rid, _, _))| *rid >= lotse_server::DYNAMIC_BASE) {
                for (p, v) in voters.iter().enumerate() {
                    let group = group_of(*rid, view, name, p);
                    let hosted = handle.status(group).is_some();
                    if hosted != v.contains(&(*id as u32)) {
                        wrong.push((id, name.clone(), p, hosted));
                    }
                }
            }
        }
        if wrong.is_empty() {
            break;
        }
        assert!(rt.now() < deadline, "groups run where they should not, or not where they should: {wrong:?}");
        rt.sleep(TICK * 20).await;
    }

    // Every acknowledged produce is where it was acknowledged.
    for ((stream, partition, offset), value) in &out.produced {
        let f = checker.fetch(stream, *partition, *offset, TICK * 20).await.expect("the healed cluster serves reads");
        let (b, _) = Batch::parse(&f.records).expect("the batch is there");
        let got = String::from_utf8(b.records().unwrap()[0].value.clone().unwrap().to_vec()).unwrap();
        assert_eq!((b.base_offset(), &got), (*offset, value), "{stream}/{partition}: acknowledged {value} at {offset}");
    }
    drop(checker);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

/// The group of partition `p` of a runtime resource: see `lotse_server::DYNAMIC_BASE`.
fn group_of(resource_id: u64, _view: &View, _name: &str, p: usize) -> u64 {
    let index = resource_id - lotse_server::DYNAMIC_BASE;
    lotse_server::DYNAMIC_BASE + index * u64::from(lotse_server::MAX_PARTITIONS) + p as u64
}

#[test]
fn resources_created_at_runtime_agree_everywhere() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

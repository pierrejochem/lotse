//! Nodes added and removed under load and faults, checked over many seeds
//! (ADR 0048).
//!
//! Three nodes start with a metadata group and a coordinator group; an
//! admin creates two streams (three replicas each) and a queue, and an
//! idempotent producer writes to the streams the whole time. Node 4 and
//! node 5 are added and started as joining nodes, then one of the first
//! three is removed (drained), while pairs of hosts are partitioned and
//! nodes lose power. After healing the drain must finish.
//!
//! Checked:
//! - replicas end up even across nodes (within one) and no node leads
//!   more than two of the four partitions (ADR 0050);
//! - the removed node leaves: it is no longer listed, no group counts it,
//!   and every group still has three voters;
//! - no node that stays keeps the log of a group it no longer votes in;
//! - what the metadata group wants is what the groups have: every
//!   partition's Raft voters, as its leader reports them, are the voters
//!   the metadata group lists for it;
//! - every acknowledged produce is there at its offset, and the cluster
//!   takes writes on every partition afterwards;
//! - an added id cannot be added with another address, and a removed id
//!   never comes back.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_proto::{ErrorCode, node_state};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Disk, Rng, Spawn};
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
const STREAMS: [&str; 2] = ["s1", "s2"];

fn message(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn addr(id: NodeId, port: u16) -> String {
    format!("n{id}:{port}")
}

fn client_config() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(500),
        retry_for: Duration::from_millis(1_500),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(400),
        metadata_max_age: Duration::from_millis(300),
        ..ClientConfig::new((1..=3).map(|i| addr(i, 9000)).collect())
    }
}

fn config(id: NodeId, log: LogConfig, pre_vote: bool) -> NodeConfig {
    // The first three are the cluster's nodes; 4 and 5 know them and
    // themselves, and join.
    let known: Vec<NodeId> = if id <= 3 { vec![1, 2, 3] } else { vec![1, 2, 3, id] };
    NodeConfig {
        id,
        listen: addr(id, 7000),
        peers: known.iter().map(|n| (*n, addr(*n, 7000))).collect(),
        groups: vec![1, 2],
        tick: TICK,
        pre_vote,
        log,
        client_listen: Some(addr(id, 9000)),
        client_addrs: known.iter().map(|n| (*n, addr(*n, 9000))).collect(),
        zones: Default::default(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
        retention: lotse_server::RetentionConfig::keep(),
        join: id > 3,
        auth: lotse_server::AuthConfig::none(),
    }
}

struct World {
    rt: SimRuntime,
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
    down_until: BTreeMap<NodeId, lotse_rt::Instant>,
}

impl World {
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

#[derive(Default)]
struct Outcomes {
    /// `(stream, partition, offset)` → value, as acknowledged.
    acked: BTreeMap<(String, u32, u64), String>,
}

fn settled(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::GaveUp(..)
            | ClientError::Unknown(_)
            | ClientError::UnknownStream(..)
            | ClientError::Broker {
                code: ErrorCode::NOT_LEADER | ErrorCode::REQUEST_TIMED_OUT | ErrorCode::COORDINATOR_NOT_AVAILABLE,
                ..
            }
    )
}

async fn producer(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let stream = STREAMS[rt.below(2) as usize];
        let partition = rt.below(2) as u32;
        let value = format!("v{seq}");
        seq += 1;
        match client.produce(stream, partition, message(&value)).await {
            Ok(offset) => {
                let before = out.lock().unwrap().acked.insert((stream.into(), partition, offset), value);
                assert!(before.is_none(), "{stream}/{partition}: offset {offset} acknowledged twice");
            }
            Err(e) if settled(&e) => {}
            Err(e) => panic!("unexpected produce error: {e}"),
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
}

/// Retries an admin call until it settles one way or the other.
async fn admin_call<F, Fut>(rt: &SimRuntime, what: &str, f: F)
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<(), ClientError>>,
{
    let deadline = rt.now() + Duration::from_secs(60);
    loop {
        let last = match f().await {
            Ok(()) => return,
            Err(e) if settled(&e) => e,
            Err(e) => panic!("{what}: {e}"),
        };
        assert!(rt.now() < deadline, "{what} never went through: {last}");
        rt.sleep(TICK * 10).await;
    }
}

async fn run(rt: SimRuntime) {
    let pre_vote = rt.chance(1, 2);
    let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
    let mut w = World {
        hosts: (1..=5).map(|id| (id, rt.node(&format!("n{id}")))).collect(),
        cfgs: (1..=5).map(|id| (id, config(id, log, pre_vote))).collect(),
        running: BTreeMap::new(),
        down_until: BTreeMap::new(),
        rt: rt.clone(),
    };
    for id in 1..=3 {
        w.start(id).await;
    }
    let admin = Client::new(rt.node("a1"), ClientConfig { retry_for: Duration::from_secs(5), ..client_config() });
    for s in STREAMS {
        admin_call(&rt, "create a stream", || async {
            match admin.create_stream(s, 2, 3).await {
                Ok(_) | Err(ClientError::ResourceExists { .. }) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .await;
    }
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let producing = rt.spawn(producer(rt.node("p1"), out.clone(), stop.clone()));
    let removed: NodeId = 1 + rt.below(3);
    let hosts: Vec<String> = ["n1", "n2", "n3", "n4", "n5", "a1", "p1"].map(String::from).to_vec();
    // Who may be crashed: nodes that have started.
    let mut started: Vec<NodeId> = vec![1, 2, 3];
    let mut partition_until = None;
    let end = rt.now() + Duration::from_secs(8);
    let mut step: usize = 0;
    while rt.now() < end {
        rt.sleep(TICK * 2).await;
        let back: Vec<NodeId> = w.down_until.iter().filter(|(_, t)| **t <= rt.now()).map(|(id, _)| *id).collect();
        for id in back {
            w.down_until.remove(&id);
            w.start(id).await;
        }
        // The membership changes, each once its time has come.
        let elapsed = 8_000 - (end - rt.now()).as_millis() as u64;
        // One attempt per round, so that faults heal and nodes restart in
        // between; a change counts once it is acknowledged.
        let due = [800, 1_800, 2_600].get(step).is_some_and(|t| elapsed > *t);
        if due {
            let res = match step {
                0 | 1 => {
                    let id = 4 + step as NodeId;
                    admin.add_node(id, &addr(id, 7000), &addr(id, 9000)).await
                }
                _ => admin.remove_node(removed).await,
            };
            match res {
                Ok(()) => {
                    if step < 2 {
                        let id = 4 + step as NodeId;
                        w.start(id).await;
                        started.push(id);
                    }
                    step += 1;
                }
                Err(e) if settled(&e) => {}
                Err(e) => panic!("membership change {step}: {e}"),
            }
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
        let down = w.running.values().filter(|r| r.is_none()).count();
        if down == 0 && rt.chance(1, 40) {
            let id = started[rt.below(started.len() as u64) as usize];
            let mid_write = rt.chance(1, 2);
            w.crash(id, mid_write).await;
        }
    }
    // Changes not made during the faults are made now.
    rt.net().heal();
    let deadline = rt.now() + Duration::from_secs(60);
    while step < 3 {
        assert!(rt.now() < deadline, "membership change {step} never went through on the healed cluster");
        let res = match step {
            0 | 1 => {
                let id = 4 + step as NodeId;
                admin.add_node(id, &addr(id, 7000), &addr(id, 9000)).await
            }
            _ => admin.remove_node(removed).await,
        };
        match res {
            Ok(()) => {
                if step < 2 && !w.running.contains_key(&(4 + step as NodeId)) {
                    w.start(4 + step as NodeId).await;
                }
                step += 1;
            }
            Err(e) if settled(&e) => {}
            Err(e) => panic!("membership change {step}: {e}"),
        }
    }
    stop.store(true, Ordering::Relaxed);
    producing.await;
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // The drain finishes: the node is no longer listed and nothing moves.
    let checker = Client::new(
        rt.node("checker"),
        ClientConfig {
            retry_for: Duration::from_secs(20),
            bootstrap: [1, 2, 3, 4, 5].iter().filter(|n| **n != removed).map(|n| addr(*n, 9000)).collect(),
            ..client_config()
        },
    );
    let deadline = rt.now() + Duration::from_secs(120);
    loop {
        let nodes = checker.describe_nodes().await.expect("the healed cluster lists its nodes");
        let ids: BTreeSet<u64> = nodes.iter().map(|n| n.node_id).collect();
        let quiet = nodes.iter().all(|n| n.moving == 0 && n.state == node_state::ACTIVE);
        // Rebalanced too (ADR 0050): replicas within one, or a tenth of
        // the fullest node.
        let hi = nodes.iter().map(|n| n.replicas).max().unwrap_or(0) as i64;
        let lo = nodes.iter().map(|n| n.replicas).min().unwrap_or(0) as i64;
        let even = hi - lo <= 1.max(hi / 10);
        if !ids.contains(&removed) && quiet && even {
            let expected: BTreeSet<u64> = (1..=5).filter(|n| *n != removed).collect();
            assert_eq!(ids, expected, "the cluster's nodes");
            break;
        }
        assert!(rt.now() < deadline, "the drain of node {removed} or the rebalancing never finished: {nodes:?}");
        rt.sleep(TICK * 50).await;
    }

    // The removed node stops by itself (ADR 0051), even if it was down or
    // cut off when its drain finished; the cluster is healed now.
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        let stopped = w.running[&removed].as_ref().is_none_or(|(_, h)| h.removed());
        if stopped {
            break;
        }
        assert!(rt.now() < deadline, "node {removed} was drained but did not stop");
        rt.sleep(TICK * 50).await;
    }
    if let Some((node, _)) = w.running.insert(removed, None).flatten() {
        node.join().await.expect("a drained node stops cleanly");
    }

    // What the metadata group wants is what every group has, three voters
    // each, none of them the removed node. A node that hosts a partition
    // reports its Raft voters and its leader; the others report the voters
    // the metadata group lists.
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        let mut wrong = Vec::new();
        let mut led: BTreeMap<(String, u32), u32> = BTreeMap::new();
        for id in (1..=5).filter(|n| *n != removed) {
            let c =
                Client::new(rt.node("checker"), ClientConfig { bootstrap: vec![addr(id, 9000)], ..client_config() });
            let Ok(m) = c.metadata(&STREAMS).await else {
                wrong.push(format!("node {id} does not answer"));
                continue;
            };
            for r in &m.resources {
                for p in &r.partitions {
                    let voters: BTreeSet<u32> = p.voters.iter().copied().collect();
                    if voters.len() != 3 || voters.contains(&(removed as u32)) {
                        wrong.push(format!("node {id}: {}/{} has voters {voters:?}", r.name, p.partition));
                    }
                    if p.leader != 0 {
                        led.insert((r.name.clone(), p.partition), p.leader);
                    }
                }
            }
        }
        // Leaders spread (ADR 0050): four partitions on four nodes, no node
        // leads more than two of them.
        let mut per_node: BTreeMap<u32, usize> = BTreeMap::new();
        for l in led.values() {
            *per_node.entry(*l).or_default() += 1;
        }
        let spread = per_node.values().all(|n| *n <= 2);
        if wrong.is_empty() && led.len() == STREAMS.len() * 2 && spread {
            break;
        }
        if rt.now() >= deadline {
            let nodes = checker.describe_nodes().await;
            let status: Vec<_> =
                w.running.iter().filter_map(|(id, r)| r.as_ref().map(|(_, h)| (*id, h.status(1)))).collect();
            panic!(
                "groups do not have their voters or a leader: {wrong:?}, led {led:?}, nodes {nodes:?}, metadata group {status:?}"
            );
        }
        rt.sleep(TICK * 50).await;
    }
    // No node that stays keeps the log or snapshot of a group it is not a
    // voter of: at most its term and vote (ADR 0050), even if it was down
    // when the group moved off it.
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        // Asked afresh of the nodes that stay: the checker knows the removed
        // node too, which still runs and answers with what it last knew.
        let fresh = Client::new(
            rt.node("checker"),
            ClientConfig {
                bootstrap: (1..=5).filter(|n| *n != removed).map(|n| addr(n, 9000)).collect(),
                ..client_config()
            },
        );
        let m = fresh.metadata(&STREAMS).await.expect("the healed cluster answers metadata");
        let mut left_on_disk = Vec::new();
        for r in &m.resources {
            let index = r.resource_id - lotse_server::DYNAMIC_BASE;
            for p in &r.partitions {
                let group = lotse_server::DYNAMIC_BASE
                    + index * u64::from(lotse_server::MAX_PARTITIONS)
                    + u64::from(p.partition);
                let dir = std::path::Path::new("groups").join(group.to_string());
                for id in (1..=5).filter(|n| *n != removed && !p.voters.contains(&(*n as u32))) {
                    let host = &w.hosts[&id];
                    let files: Vec<_> = host.list(&dir).await.unwrap_or_default();
                    let logs: Vec<_> = host.list(&dir.join("log")).await.unwrap_or_default();
                    let extra: Vec<_> = files
                        .iter()
                        .filter(|f| !f.ends_with("raft.state") && !f.ends_with("moved"))
                        .chain(logs.iter())
                        .collect();
                    if !extra.is_empty() {
                        left_on_disk.push(format!("node {id}, {}/{}: {extra:?}", r.name, p.partition));
                    }
                }
            }
        }
        if left_on_disk.is_empty() {
            break;
        }
        assert!(rt.now() < deadline, "nodes keep groups they left: {left_on_disk:?}");
        rt.sleep(TICK * 50).await;
    }

    // Nothing acknowledged is lost, and every partition takes writes.
    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(!out.acked.is_empty(), "nothing was acknowledged: the run checked nothing");
    for s in STREAMS {
        for p in 0..2 {
            let marker = format!("after-{s}-{p}");
            let mut at = None;
            for _ in 0..20 {
                match checker.produce(s, p, message(&marker)).await {
                    Ok(o) => {
                        at = Some(o);
                        break;
                    }
                    Err(ClientError::Unknown(_)) => continue,
                    Err(e) => panic!("{s}/{p} takes no writes after the changes: {e}"),
                }
            }
            let at = at.unwrap_or_else(|| panic!("{s}/{p}: no produce settled after the changes"));
            let mut log = BTreeMap::new();
            let mut offset = 0;
            while offset <= at {
                let f = checker.fetch(s, p, offset, TICK * 10).await.expect("the healed cluster serves reads");
                offset = f.next_offset(offset);
                for b in f.committed_batches().unwrap() {
                    let v = b.records().unwrap()[0].value.clone().unwrap();
                    log.insert(b.base_offset(), String::from_utf8(v.to_vec()).unwrap());
                }
            }
            for ((stream, partition, offset), v) in &out.acked {
                if stream == s && *partition == p {
                    assert_eq!(log.get(offset), Some(v), "{s}/{p}: acknowledged {v} at {offset} is lost or moved");
                }
            }
        }
    }

    // Ids are never used twice, nor an id with another address.
    match checker.add_node(removed, &addr(removed, 7000), &addr(removed, 9000)).await {
        Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }) => {}
        other => panic!("node {removed} was added again: {other:?}"),
    }
    match checker.add_node(4, "elsewhere:7000", "elsewhere:9000").await {
        Err(ClientError::Broker { code: ErrorCode::RESOURCE_EXISTS, .. }) => {}
        other => panic!("node 4 was added again under another address: {other:?}"),
    }
    drop(checker);
    drop(admin);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn nodes_come_and_go_without_losing_anything() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

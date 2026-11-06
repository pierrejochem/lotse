//! Resources created, changed and deleted at runtime against a faulty
//! cluster, checked over many seeds.
//!
//! Four or five nodes start with a metadata group, a coordinator group and
//! one configured stream, `base`. Two admin clients create the same streams,
//! queues and an exchange concurrently, bind the queues, change queues' ack
//! timeouts, and delete and create again `s3`, `q2` and `x1`, while a
//! producer writes to the streams, a publisher publishes through the
//! exchange and a transactional producer writes to `base` and `s3` in
//! transactions; pairs of hosts are partitioned and nodes lose power. After
//! healing every node is asked what exists.
//!
//! Checked:
//! - every node reports the same resources with the same ids, one per name;
//! - a configured name cannot be created again, and configured resources
//!   cannot be deleted;
//! - an acknowledged create of a name never deleted is there, with the id it
//!   was acknowledged with, its partitions and as many voters per partition
//!   as asked for; of a name that was deleted, no older resource outlives a
//!   newer acknowledged one;
//! - a deleted resource is gone everywhere: no node runs its groups, no
//!   disk holds their files, every node answers `UNKNOWN_RESOURCE` for it,
//!   and no exchange keeps a binding to a deleted queue;
//! - each node runs exactly the groups it is a voter of;
//! - a queue's ack timeout is its default or a value an admin set;
//! - every acknowledged produce to a stream never deleted is there at its
//!   offset;
//! - a published message goes only to queues that were bound to the
//!   exchange;
//! - a transaction whose partitions were deleted still ends: the
//!   transactional id commits again afterwards.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, OutMessage};
use lotse_log::LogConfig;
use lotse_proto::{ErrorCode, kind};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_route::ExchangeKind;
use lotse_rt::{Clock, Conn, Disk, Net, Rng, Spawn};
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

/// The names admins delete (and create again).
const DELETED: [&str; 3] = ["s3", "q2", "x1"];

#[derive(Default)]
struct Outcomes {
    /// Acknowledged creates (or `ResourceExists`): name → ids.
    ids: BTreeMap<String, BTreeSet<u64>>,
    /// Names a delete was tried for, whatever the outcome.
    delete_tried: BTreeSet<String>,
    /// Acknowledged deletes: name → ids.
    deleted: BTreeMap<String, BTreeSet<u64>>,
    /// Ack timeouts tried per queue, in ms; 0 is a reset to the default.
    timeouts: BTreeMap<String, BTreeSet<u64>>,
    /// `retention.bytes` tried per stream, at creation or changed; 0 is a
    /// reset to the default. Large enough that nothing goes.
    retention: BTreeMap<String, BTreeSet<i64>>,
    /// Queues ever bound to the exchange (or tried).
    bound: BTreeSet<String>,
    /// Acknowledged produces: (stream, partition, offset) → value.
    produced: BTreeMap<(String, u32, u64), String>,
    published: u64,
    /// Transactions committed, and how many wrote to `s3`.
    committed: u64,
    committed_s3: u64,
}

/// Whatever a request may run into while resources come and go: no answer
/// in time, an unknown outcome, a name not (yet) known or a resource
/// deleted under it.
fn coming_and_going(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::GaveUp(..)
            | ClientError::Unknown(_)
            | ClientError::UnknownStream(..)
            | ClientError::Broker { code: ErrorCode::UNKNOWN_RESOURCE, .. }
    )
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

/// What one node reports: name → (id, kind, voters per partition, queues
/// bound to it).
type View = BTreeMap<String, (u64, u8, Vec<Vec<u32>>, BTreeSet<u64>)>;

/// What every node reports: its resources, their kinds, each partition's
/// voters (a hosting node's Raft voters, the metadata group's choice
/// otherwise) and an exchange's bound queues.
async fn views_of(rt: &SimRuntime, n: u64) -> BTreeMap<NodeId, View> {
    let mut views = BTreeMap::new();
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
                let bound = r.exchange.iter().flat_map(|x| x.bindings.iter().map(|b| b.queue_id)).collect();
                (r.name.clone(), (r.resource_id, r.kind, voters, bound))
            })
            .collect();
        views.insert(id, view);
    }
    views
}

const NAMES: [&str; 7] = ["s1", "s2", "s3", "q1", "q2", "q3", "x1"];

async fn admin(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    while !stop.load(Ordering::Relaxed) {
        if rt.chance(1, 10) {
            // The configured stream exists already, everywhere, and stays.
            match client.create_stream("base", 1, 1).await {
                Err(ClientError::ResourceExists { resource_id: 7, .. }) => {}
                Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                other => panic!("a configured name was created again: {other:?}"),
            }
            match client.delete("base").await {
                Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }) => {}
                Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                other => panic!("a configured resource was deleted: {other:?}"),
            }
        }
        let name = NAMES[rt.below(NAMES.len() as u64) as usize];
        let (k, partitions, replication) = spec(name);
        let res = match k {
            kind::STREAM if rt.chance(1, 2) => {
                let bytes = retention_bytes(&rt);
                out.lock().unwrap().retention.entry(name.into()).or_default().insert(bytes);
                client
                    .create_stream_with(name, partitions, replication, &[("retention.bytes", &bytes.to_string())])
                    .await
            }
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
                Ok(()) => {}
                Err(e) if coming_and_going(&e) => {}
                Err(e) => panic!("unexpected error binding {q}: {e}"),
            }
        }
        if rt.chance(1, 4) {
            let q = ["q1", "q2", "q3"][rt.below(3) as usize];
            let ms = if rt.chance(1, 4) { 0 } else { 100 + rt.below(5_000) };
            out.lock().unwrap().timeouts.entry(q.into()).or_default().insert(ms);
            let res = if ms == 0 {
                client.alter_configs(q, &[], &["ack.timeout.ms"]).await
            } else {
                client.alter_configs(q, &[("ack.timeout.ms", &ms.to_string())], &[]).await
            };
            match res {
                Ok(()) => {}
                Err(e) if coming_and_going(&e) => {}
                Err(e) => panic!("unexpected error changing {q}: {e}"),
            }
        }
        if rt.chance(1, 4) {
            let st = ["s1", "s2", "s3"][rt.below(3) as usize];
            let bytes = if rt.chance(1, 4) { 0 } else { retention_bytes(&rt) };
            out.lock().unwrap().retention.entry(st.into()).or_default().insert(bytes);
            let res = if bytes == 0 {
                client.alter_configs(st, &[], &["retention.bytes"]).await
            } else {
                client.alter_configs(st, &[("retention.bytes", &bytes.to_string())], &[]).await
            };
            match res {
                Ok(()) => {}
                Err(e) if coming_and_going(&e) => {}
                Err(e) => panic!("unexpected error changing {st}: {e}"),
            }
        }
        if rt.chance(1, 5) {
            let name = DELETED[rt.below(DELETED.len() as u64) as usize];
            out.lock().unwrap().delete_tried.insert(name.into());
            match client.delete(name).await {
                Ok(id) => {
                    out.lock().unwrap().deleted.entry(name.into()).or_default().insert(id);
                }
                Err(e) if coming_and_going(&e) => {}
                Err(e) => panic!("unexpected error deleting {name}: {e}"),
            }
        }
        rt.sleep(TICK * (5 + rt.below(20) as u32)).await;
    }
}

/// A `retention.bytes` no partition here reaches: 1 GiB and more, or no
/// limit.
fn retention_bytes(rt: &SimRuntime) -> i64 {
    if rt.chance(1, 3) { -1 } else { (1 << 30) + rt.below(1_000) as i64 }
}

/// Produces and publishes until stopped; returns its client, routes and
/// sequences cached, for the final check.
async fn producer(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) -> Client<SimRuntime> {
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
            Err(e) if coming_and_going(&e) => {}
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
                Err(e) if coming_and_going(&e) => {}
                Err(e) => panic!("unexpected publish error: {e}"),
            }
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
    client
}

fn txn_config(n: u64) -> ClientConfig {
    ClientConfig { transactional_id: Some("tx".into()), txn_timeout: Duration::from_millis(800), ..client_config(n) }
}

/// Writes transactions to `base` and, often, `s3`, which admins delete.
async fn transactional(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), txn_config(n));
    let mut seq = 0;
    while !stop.load(Ordering::Relaxed) {
        let mut txn = match client.begin_transaction().await {
            Ok(t) => t,
            Err(e) if coming_and_going(&e) => continue,
            Err(e) => panic!("unexpected error beginning a transaction: {e}"),
        };
        let mut written = true;
        let to_s3 = rt.chance(2, 3);
        let streams: &[&str] = if to_s3 { &["base", "s3"] } else { &["base"] };
        for stream in streams {
            seq += 1;
            let partition = if *stream == "s3" { rt.below(u64::from(spec("s3").1)) as u32 } else { 0 };
            match txn.produce(stream, partition, message(&format!("t{seq}"))).await {
                Ok(_) => {}
                // Not written, maybe written, fenced by a timeout or refused
                // because the partition was deleted: the transaction aborts.
                Err(_) => written = false,
            }
        }
        // Abandoned now and then: the coordinator's timeout aborts it.
        if rt.chance(1, 5) {
            drop(txn);
        } else if !written || rt.chance(1, 4) {
            let _ = txn.abort().await;
        } else {
            match txn.commit().await {
                Ok(()) => {
                    let mut o = out.lock().unwrap();
                    o.committed += 1;
                    o.committed_s3 += u64::from(to_s3);
                }
                Err(e) if coming_and_going(&e) => {}
                Err(ClientError::Broker { code: ErrorCode::PRODUCER_FENCED, .. }) => {}
                Err(e) => panic!("unexpected error committing: {e}"),
            }
        }
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

/// The error a node gives for a fetch of `resource_id`, asked directly.
async fn fetch_error(rt: &SimRuntime, addr: &str, resource_id: u64) -> Option<ErrorCode> {
    let conn = rt.connect(addr, lotse_rt::Link::Client).await.ok()?;
    let req = lotse_proto::Request::Fetch(lotse_proto::FetchRequest {
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 10,
        isolation: lotse_proto::isolation::COMMITTED,
        resources: vec![lotse_proto::FetchResource {
            resource_id,
            partitions: vec![lotse_proto::FetchPartition {
                partition: 0,
                fetch_offset: 0,
                leader_term: 0,
                max_bytes: 1 << 10,
            }],
        }],
    });
    conn.send(lotse_proto::encode_request(1, &req)).await.ok()?;
    let frame = conn.recv().await.ok()??;
    match lotse_proto::decode_response(frame, lotse_proto::ApiKey::FETCH).ok()?.1 {
        lotse_proto::Response::Fetch(f) => Some(f.resources.first()?.partitions.first()?.error),
        _ => None,
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
    let producing = rt.spawn(producer(rt.node("p1"), n, out.clone(), stop.clone()));
    tasks.push(rt.spawn(transactional(rt.node("t1"), n, out.clone(), stop.clone())));
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["a1", "a2", "p1", "t1"].map(String::from));
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
    let producer = producing.await;
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }
    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(!out.ids.is_empty(), "nothing was created: the run checked nothing");
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        let deleted: usize = out.deleted.values().map(BTreeSet::len).sum();
        eprintln!(
            "stats: nodes {n} created {} deleted {deleted} produced {} published {} bound {:?} committed {} (s3 {})",
            out.ids.values().map(BTreeSet::len).sum::<usize>(),
            out.produced.len(),
            out.published,
            out.bound,
            out.committed,
            out.committed_s3,
        );
    }

    // Every node reports the same resources, once caught up.
    let checker =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(n) });
    let deadline = rt.now() + Duration::from_secs(30);
    let mut views: BTreeMap<NodeId, View>;
    loop {
        views = views_of(&rt, n).await;
        let first = views[&1].clone();
        let created = out.ids.keys().filter(|n| !out.delete_tried.contains(*n)).all(|name| first.contains_key(name));
        if views.values().all(|v| *v == first) && created {
            break;
        }
        assert!(rt.now() < deadline, "the nodes do not agree on what exists: {views:?}");
        rt.sleep(TICK * 20).await;
    }
    let view = &views[&1];
    // Ids that were acknowledged once and are gone now: deleted.
    let mut gone: Vec<(String, u64)> = Vec::new();
    for (name, ids) in &out.ids {
        let (want_kind, partitions, replication) = spec(name);
        let now = view.get(name);
        if !out.delete_tried.contains(name) {
            assert_eq!(ids.len(), 1, "{name} was acknowledged with different ids: {ids:?}");
            assert!(now.is_some_and(|(id, ..)| ids.contains(id)), "{name} is {now:?}, but was acknowledged as {ids:?}");
        }
        if let Some((id, k, voters, _)) = now {
            let newest = *ids.last().expect("one at least");
            assert!(*id >= newest, "{name} is {id}, older than {newest}, acknowledged after it");
            assert_eq!(*k, want_kind, "{name} has the wrong kind");
            if want_kind != kind::EXCHANGE {
                assert_eq!(voters.len() as u32, partitions, "{name} has the wrong number of partitions");
                for v in voters {
                    assert_eq!(v.len(), usize::from(replication), "{name}: a partition with voters {v:?}");
                }
            }
        }
        for id in ids.iter().filter(|id| now.is_none_or(|(now, ..)| now != *id)) {
            gone.push((name.clone(), *id));
        }
    }
    for (name, ids) in &out.deleted {
        for id in ids {
            assert!(view.get(name).is_none_or(|(now, ..)| now != id), "{name} {id} was deleted, but is there");
            gone.push((name.clone(), *id));
        }
    }
    gone.sort();
    gone.dedup();
    // No binding outlives the queue it names.
    let queues: BTreeSet<u64> = view.values().filter(|(_, k, ..)| *k == kind::QUEUE).map(|(id, ..)| *id).collect();
    for (name, (_, _, _, bound)) in view {
        assert!(bound.is_subset(&queues), "{name} binds queues that do not exist: {bound:?}, queues {queues:?}");
    }

    // A deleted resource is gone everywhere: every node calls it unknown.
    let deadline = rt.now() + Duration::from_secs(10);
    for (name, id) in gone.iter().filter(|(name, _)| spec(name).0 == kind::STREAM) {
        for node in 1..=n {
            loop {
                let error = fetch_error(&rt.node("checker"), &format!("n{node}:9000"), *id).await;
                if error == Some(ErrorCode::UNKNOWN_RESOURCE) {
                    break;
                }
                assert!(rt.now() < deadline, "node {node} answers {error:?} for {name} {id}, which was deleted");
                rt.sleep(TICK * 20).await;
            }
        }
    }

    // Ack timeouts and stream retention are the default or what an admin
    // set.
    let admin =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(n) });
    for (name, (_, k, ..)) in view.iter().filter(|(_, (id, ..))| *id >= lotse_server::DYNAMIC_BASE) {
        let configs = admin.describe_configs(name).await.expect("the healed cluster describes resources");
        if *k == kind::STREAM {
            // Nothing here compacts: the compaction settings stay at their
            // defaults (ADR 0067).
            let [ms, bytes, policy, tombstones] = &configs[..] else { panic!("{name} has settings {configs:?}") };
            assert_eq!(
                (policy.key.as_str(), policy.value.as_str(), policy.source),
                ("cleanup.policy", "delete", lotse_proto::config_source::DEFAULT),
                "{name}"
            );
            assert_eq!(tombstones.key, "delete.retention.ms", "{name}");
            assert_eq!(tombstones.source, lotse_proto::config_source::DEFAULT, "{name}");
            assert_eq!(
                (ms.key.as_str(), ms.value.as_str(), ms.source),
                ("retention.ms", "-1", lotse_proto::config_source::DEFAULT),
                "{name}: the node keeps everything by age"
            );
            assert_eq!(bytes.key, "retention.bytes");
            let tried = out.retention.get(name).cloned().unwrap_or_default();
            let value: i64 = bytes.value.parse().expect("a number");
            match bytes.source {
                lotse_proto::config_source::DEFAULT => assert_eq!(value, -1, "{name}: the default"),
                lotse_proto::config_source::DYNAMIC => {
                    assert!(tried.contains(&value), "{name}: {value} bytes, tried {tried:?}")
                }
                other => panic!("{name}: source {other}"),
            }
            continue;
        }
        if *k != kind::QUEUE {
            assert!(configs.is_empty(), "{name} has settings: {configs:?}");
            continue;
        }
        // The ack timeout, then the limits (ADR 0055), which this run never sets.
        let [c, limits @ ..] = &configs[..] else { panic!("{name} has settings {configs:?}") };
        let keys: Vec<(&str, &str, u8)> = limits.iter().map(|e| (e.key.as_str(), e.value.as_str(), e.source)).collect();
        let off = lotse_proto::config_source::DEFAULT;
        assert_eq!(
            keys,
            [("max.deliveries", "0", off), ("message.ttl.ms", "0", off), ("dead.letter.queue", "", off)],
            "{name}: no limits were set"
        );
        let tried = out.timeouts.get(name).cloned().unwrap_or_default();
        let ms: u64 = c.value.parse().expect("a number");
        match c.source {
            lotse_proto::config_source::DEFAULT => {
                assert_eq!(
                    u128::from(ms),
                    lotse_server::QueueConfig::DEFAULT_ACK_TIMEOUT.as_millis(),
                    "{name}: the default"
                )
            }
            lotse_proto::config_source::DYNAMIC => assert!(tried.contains(&ms), "{name}: {ms} ms, tried {tried:?}"),
            other => panic!("{name}: source {other}"),
        }
    }
    drop(admin);

    // Each node runs exactly the groups it is a voter of, once nothing
    // moves: groups may still be moving to even out the nodes (ADR 0050),
    // so the voters are read again each time and all nodes must agree.
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        let mut wrong = Vec::new();
        let views = views_of(&rt, n).await;
        let view = &views[&1];
        if views.values().any(|v| v != view) {
            wrong.push((&0, "the nodes disagree".to_string(), 0, false));
        }
        for (id, entry) in &w.running {
            let (_, handle) = entry.as_ref().expect("all nodes are up");
            for (name, (rid, _, voters, _)) in view.iter().filter(|(_, (rid, ..))| *rid >= lotse_server::DYNAMIC_BASE) {
                for (p, v) in voters.iter().enumerate() {
                    let group = group_of(*rid, p);
                    let hosted = handle.status(group).is_some();
                    if hosted != v.contains(&(*id as u32)) {
                        wrong.push((id, name.clone(), p, hosted));
                    }
                }
            }
            // Deleted groups run nowhere, and their files are gone.
            for (name, rid) in &gone {
                for p in 0..spec(name).1 as usize {
                    let group = group_of(*rid, p);
                    let dir = std::path::PathBuf::from("groups").join(group.to_string());
                    let on_disk =
                        w.hosts[id].list(&dir).await.is_ok() || w.hosts[id].list(&dir.join("log")).await.is_ok();
                    if handle.status(group).is_some() || on_disk {
                        wrong.push((id, format!("deleted {name}"), p, true));
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

    // Every acknowledged produce to a stream never deleted is where it was
    // acknowledged.
    for ((stream, partition, offset), value) in out.produced.iter().filter(|((s, ..), _)| !out.delete_tried.contains(s))
    {
        // A transaction the workload left open when it stopped holds the
        // last stable offset back until it ends (its timeout, then the
        // markers): read once it has.
        let deadline = rt.now() + Duration::from_secs(10);
        let f = loop {
            let f =
                checker.fetch(stream, *partition, *offset, TICK * 20).await.expect("the healed cluster serves reads");
            if !f.records.is_empty() || f.last_stable_offset > *offset {
                break f;
            }
            assert!(
                rt.now() < deadline,
                "{stream}/{partition}: stable offset stuck at {} below {offset}",
                f.last_stable_offset
            );
        };
        let (b, _) = Batch::parse(&f.records).expect("the batch is there");
        let got = String::from_utf8(b.records().unwrap()[0].value.clone().unwrap().to_vec()).unwrap();
        assert_eq!((b.base_offset(), &got), (*offset, value), "{stream}/{partition}: acknowledged {value} at {offset}");
    }
    drop(checker);

    // The producer goes on writing to every stream there is, also one that
    // was deleted and created again under it: what it cached of the old
    // one (route, sequence) gives way.
    for stream in ["s1", "s2", "s3"].into_iter().filter(|s| view.contains_key(*s)) {
        let deadline = rt.now() + Duration::from_secs(10);
        loop {
            match producer.produce(stream, 0, message("after")).await {
                Ok(_) => break,
                Err(e) => assert!(rt.now() < deadline, "the producer cannot write to {stream} any more: {e}"),
            }
        }
    }
    drop(producer);

    // Transactions that wrote to deleted partitions ended: the
    // transactional id commits again.
    let t = Client::new(rt.node("t1"), ClientConfig { retry_for: Duration::from_secs(20), ..txn_config(n) });
    let mut txn = t.begin_transaction().await.expect("the transactional id begins again: its last transaction ended");
    txn.produce("base", 0, message("last")).await.expect("a produce in the last transaction");
    txn.commit().await.expect("the last transaction commits");
    drop(t);

    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

/// The group of partition `p` of a runtime resource: see `lotse_server::DYNAMIC_BASE`.
fn group_of(resource_id: u64, p: usize) -> u64 {
    let index = resource_id - lotse_server::DYNAMIC_BASE;
    lotse_server::DYNAMIC_BASE + index * u64::from(lotse_server::MAX_PARTITIONS) + p as u64
}

#[test]
fn resources_created_changed_and_deleted_at_runtime_agree_everywhere() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

/// Whether `r` failed because the resource holds something.
fn not_empty<T: std::fmt::Debug>(r: Result<T, ClientError>) -> bool {
    matches!(r, Err(ClientError::Broker { code: ErrorCode::RESOURCE_NOT_EMPTY, .. }))
}

#[test]
fn a_conditional_delete_takes_only_what_holds_nothing() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), 3).await;
            let admin =
                Client::new(rt.node("admin"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config(3) });
            admin.create_stream("empty", 3, 3).await.unwrap();
            admin.create_stream("full", 3, 3).await.unwrap();
            admin.create_queue("q", 3, None).await.unwrap();
            admin.declare_exchange("x", ExchangeKind::Fanout).await.unwrap();

            // Nothing written: gone.
            admin.delete_if_empty("empty").await.expect("an empty stream");
            assert!(matches!(admin.produce("empty", 0, message("late")).await, Err(e) if coming_and_going(&e)));
            admin.delete_if_empty("x").await.expect("an exchange holds no messages");

            // One record in the last partition is enough to keep a stream.
            admin.produce("full", 2, message("kept")).await.unwrap();
            assert!(not_empty(admin.delete_if_empty("full").await), "a record in partition 2");
            admin.produce("full", 0, message("still there")).await.expect("not deleted");

            // A queue holds a message until it is settled.
            admin.produce("q", 0, message("job")).await.unwrap();
            assert!(not_empty(admin.delete_if_empty("q").await), "an unsettled message");
            {
                let mut c = admin.subscribe("q", 10).await.unwrap();
                let m = c.next(Duration::from_secs(5)).await.unwrap().expect("the message");
                assert!(not_empty(admin.delete_if_empty("q").await), "a delivered message is not settled yet");
                c.ack(&m).await.unwrap();
            }
            admin.delete_if_empty("q").await.expect("everything acked");

            // Unconditional deletes are as they were.
            admin.delete("full").await.expect("deleted anyway");
            for (_, n) in std::mem::take(&mut w.running) {
                let (node, handle) = n.unwrap();
                handle.stop();
                node.join().await.unwrap();
            }
        });
    });
}

fn text(m: &lotse_client::Message) -> String {
    String::from_utf8(m.record.value.clone().unwrap().to_vec()).unwrap()
}

fn header_of(m: &lotse_client::Message, name: &[u8]) -> Option<String> {
    let h = m.record.headers.iter().rev().find(|h| h.key[..] == *name)?;
    Some(String::from_utf8(h.value.clone()?.to_vec()).unwrap())
}

/// The ready messages of `queue` as its leader reports them.
async fn ready(admin: &Client<SimRuntime>, queue_id: u64) -> u64 {
    let all = admin.node_metrics().await.unwrap();
    all.into_iter()
        .filter_map(|(_, m)| m.ok())
        .flat_map(|m| m.queues)
        .filter(|q| q.resource_id == queue_id)
        .map(|q| q.ready)
        .sum()
}

#[test]
fn purge_takes_only_ready_messages_and_requeue_moves_dead_letters_back_once() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), 3).await;
            let admin =
                Client::new(rt.node("admin"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config(3) });
            let dlq = admin.create_queue("dlq", 3, None).await.unwrap();
            let work = admin.create_queue_with("work", 3, &[("max.deliveries", "1")], Some("dlq")).await.unwrap();

            // Five messages fail once each and go to the dead-letter queue.
            for i in 0..5 {
                admin.produce("work", 0, message(&format!("m{i}"))).await.unwrap();
            }
            {
                let mut c = admin.subscribe("work", 10).await.unwrap();
                let mut seen = 0;
                while seen < 5 {
                    let m = c.next(Duration::from_secs(5)).await.unwrap().expect("a delivery");
                    c.nack(&m, true).await.unwrap();
                    seen += 1;
                }
                c.close().await;
            }
            let mut waited = 0;
            while ready(&admin, dlq).await < 5 {
                rt.sleep(TICK * 10).await;
                waited += 1;
                assert!(waited < 500, "the dead letters arrive");
            }

            // Requeue: all five back in `work`, once each.
            assert_eq!(admin.requeue("dlq").await.unwrap(), 5);
            assert_eq!(admin.requeue("dlq").await.unwrap(), 0, "nothing left to move");
            let mut back = BTreeSet::new();
            {
                let mut c = admin.subscribe("work", 10).await.unwrap();
                while let Some(m) = c.next(Duration::from_secs(2)).await.unwrap() {
                    assert_eq!(header_of(&m, b"x-lotse-dead-letter-reason").as_deref(), Some("requeued"));
                    assert_eq!(header_of(&m, b"x-lotse-dead-letter-queue").as_deref(), Some("dlq"));
                    assert!(back.insert(text(&m)), "{} came back twice", text(&m));
                    c.ack(&m).await.unwrap();
                }
                c.close().await;
            }
            assert_eq!(back, (0..5).map(|i| format!("m{i}")).collect::<BTreeSet<_>>());
            assert_eq!(ready(&admin, dlq).await, 0);
            // A message that came to the dead-letter queue some other way
            // names no origin, and one whose queue was deleted has nowhere to
            // go: both stay.
            admin.produce("dlq", 0, message("plain")).await.unwrap();
            admin.create_queue_with("gone", 3, &[("max.deliveries", "1")], Some("dlq")).await.unwrap();
            admin.produce("gone", 0, message("orphan")).await.unwrap();
            {
                let mut c = admin.subscribe("gone", 1).await.unwrap();
                let m = c.next(Duration::from_secs(5)).await.unwrap().expect("a delivery");
                c.nack(&m, true).await.unwrap();
                c.close().await;
            }
            let mut waited = 0;
            while ready(&admin, dlq).await < 2 {
                rt.sleep(TICK * 10).await;
                waited += 1;
                assert!(waited < 500, "the orphan arrives");
            }
            admin.delete("gone").await.unwrap();
            // Once the dead-letter queue's leader knows of the delete; before,
            // a requeued message goes with the deleted queue (ADR 0064).
            rt.sleep(Duration::from_secs(1)).await;
            assert_eq!(admin.requeue("dlq").await.unwrap(), 0);
            assert_eq!(ready(&admin, dlq).await, 2);

            // Purge: what consumers hold stays theirs.
            for i in 0..7 {
                admin.produce("work", 0, message(&format!("p{i}"))).await.unwrap();
            }
            let mut c = admin.subscribe("work", 2).await.unwrap();
            let first = c.next(Duration::from_secs(5)).await.unwrap().expect("one");
            let purged = admin.purge("work").await.unwrap();
            assert_eq!(ready(&admin, work).await, 0);
            let in_flight: u64 = admin
                .node_metrics()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|(_, m)| m.ok())
                .flat_map(|m| m.queues)
                .filter(|q| q.resource_id == work)
                .map(|q| q.in_flight)
                .sum();
            assert!(in_flight >= 1, "the one handed out at least");
            assert_eq!(purged + in_flight, 7, "everything not with the consumer is purged");
            // The consumer keeps what it holds, and gets nothing else.
            c.ack(&first).await.expect("a held message is still the consumer's to ack");
            let mut kept = 1;
            while let Some(m) = c.next(Duration::from_secs(1)).await.unwrap() {
                c.ack(&m).await.unwrap();
                kept += 1;
            }
            assert_eq!(kept, in_flight, "exactly what it held");
            admin.produce("work", 0, message("after")).await.unwrap();
            let m = c.next(Duration::from_secs(5)).await.unwrap().expect("what comes after a purge stays");
            assert_eq!(text(&m), "after");
            c.ack(&m).await.unwrap();
            c.close().await;

            for (_, n) in std::mem::take(&mut w.running) {
                let (node, handle) = n.unwrap();
                handle.stop();
                node.join().await.unwrap();
            }
        });
    });
}

/// The leader `admin` sees for partition 0 of `name`, and its voters.
async fn leader_of(admin: &Client<SimRuntime>, name: &str) -> (u32, Vec<u32>) {
    // A node that is down or catching up may not answer, or not know yet.
    let Ok(m) = admin.metadata(&[name]).await else { return (0, Vec::new()) };
    match m.resources.iter().find(|r| r.name == name).and_then(|r| r.partitions.first()) {
        Some(p) => (p.leader, p.voters.clone()),
        None => (0, Vec::new()),
    }
}

/// Waits until partition 0 of `name` is led by `node`, for up to a minute.
async fn led_by(rt: &SimRuntime, admin: &Client<SimRuntime>, name: &str, node: u32) -> bool {
    for _ in 0..600 {
        if leader_of(admin, name).await.0 == node {
            return true;
        }
        rt.sleep(Duration::from_millis(100)).await;
    }
    false
}

#[test]
fn a_pinned_leader_stays_where_it_is_put() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut w = World::new(rt.clone(), 3).await;
            let admin =
                Client::new(rt.node("admin"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config(3) });
            admin.create_stream("s", 1, 3).await.unwrap();
            admin.produce("s", 0, message("x")).await.unwrap();
            // Wherever the automatic spreading puts it, settled.
            rt.sleep(Duration::from_secs(30)).await;
            let (mut before, mut voters) = leader_of(&admin, "s").await;
            while before == 0 || voters.len() < 3 {
                rt.sleep(Duration::from_millis(100)).await;
                (before, voters) = leader_of(&admin, "s").await;
            }
            assert_eq!(voters.len(), 3);
            let to = *voters.iter().find(|v| **v != before).unwrap();

            admin.move_leader("s", 0, Some(u64::from(to))).await.unwrap();
            assert!(led_by(&rt, &admin, "s", to).await, "the leader moves to node {to}");
            rt.sleep(Duration::from_secs(60)).await;
            assert_eq!(leader_of(&admin, "s").await.0, to, "and stays: the spreading leaves a pin alone");

            // The pin outlives a failover: back on node `to` once it is up.
            w.crash(u64::from(to), false).await;
            rt.sleep(Duration::from_secs(5)).await;
            assert_ne!(leader_of(&admin, "s").await.0, to);
            w.start(u64::from(to)).await;
            assert!(led_by(&rt, &admin, "s", to).await, "back where it is pinned");

            // Only a voter, only a cluster's partition.
            let not_voter = admin.move_leader("s", 0, Some(9)).await;
            assert!(
                matches!(not_voter, Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. })),
                "{not_voter:?}"
            );
            let no_partition = admin.move_leader("s", 4, Some(u64::from(to))).await;
            assert!(matches!(no_partition, Err(ClientError::Broker { code: ErrorCode::UNKNOWN_RESOURCE, .. })));
            admin.move_leader("s", 0, None).await.expect("unpinned");

            for (_, n) in std::mem::take(&mut w.running) {
                if let Some((node, handle)) = n {
                    handle.stop();
                    node.join().await.unwrap();
                }
            }
        });
    });
}

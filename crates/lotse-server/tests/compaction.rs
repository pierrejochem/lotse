//! Log compaction against a faulty cluster, checked over many seeds.
//!
//! Three or five nodes host a stream created with `cleanup.policy =
//! compact` (ADR 0067), on small segments. An idempotent producer writes
//! values and tombstones for a few keys, a transactional one writes other
//! keys and commits or aborts, while pairs of hosts are partitioned and
//! nodes lose power and restart. Afterwards the network heals, a filler
//! key pushes everything into sealed segments, and compaction is given
//! time to sweep.
//!
//! Checked, on every replica in turn (leadership is moved to each):
//! - no batch is lost: offsets run without a gap from 0;
//! - each key's newest record is at or after its newest acknowledged write
//!   and is one that was written; at that write's offset it is that write;
//!   a key is gone only if its newest write may have been a tombstone;
//! - nothing of an aborted transaction is left, not even for a reader past
//!   the last stable offset;
//! - everything older than a key's newest record is gone;
//! - all replicas agree on each key's value (a tombstone counts as gone).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, RetentionConfig, StateMachine};
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
const STREAM: &str = "table";
const PARTITIONS: u32 = 2;
/// Tombstones are kept this long.
const TOMBSTONE_MS: u64 = 400;

fn now_ms(rt: &SimRuntime) -> i64 {
    rt.unix_time().as_millis() as i64
}

fn keyed(key: &str, value: Option<&str>, ts: i64) -> Bytes {
    // Plain, zstd and lz4: a compacted batch keeps its codec.
    let mut options = BatchOptions::default();
    options.attributes.compression =
        [lotse_record::Compression::None, lotse_record::Compression::Zstd, lotse_record::Compression::Lz4]
            [key.len() % 3];
    let mut b = BatchBuilder::new(options);
    b.push(ts, Some(key.as_bytes()), value.map(str::as_bytes), &[]);
    b.finish().unwrap()
}

fn client_config(n: u64) -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(800),
        retry_for: Duration::from_millis(1_500),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(600),
        ..ClientConfig::new((1..=n).map(|i| format!("n{i}:9000")).collect())
    }
}

/// One write: key, value (`None`: a tombstone), and where it landed if
/// that is known.
#[derive(Debug, Clone)]
struct Write {
    key: String,
    value: Option<String>,
    offset: Option<u64>,
}

#[derive(Default)]
struct Outcomes {
    /// Per partition, every write whose outcome is acknowledged or unknown.
    writes: BTreeMap<u32, Vec<Write>>,
    /// Values of aborted transactions.
    aborted: BTreeSet<String>,
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
        let log = LogConfig { segment_bytes: 400 + rt.below(1_600), index_interval_bytes: 64 };
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        // Group 1 holds the metadata, group 2 coordinates.
                        groups: vec![1, 2],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        streams: Vec::new(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: Duration::from_millis(600),
                        group_heartbeat_interval: TICK * 5,
                        metadata: Some(1),
                        coordinator: Some(2),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        retention: RetentionConfig::keep(),
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
        let down = TICK * (5 + self.rt.below(150) as u32);
        self.down_until.insert(id, self.rt.now() + down);
    }
}

async fn producer(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let partition = rt.below(u64::from(PARTITIONS)) as u32;
        let key = format!("k{}", rt.below(5));
        let value = (!rt.chance(1, 6)).then(|| format!("p-{seq}"));
        seq += 1;
        let offset = match client.produce(STREAM, partition, keyed(&key, value.as_deref(), now_ms(&rt))).await {
            Ok(offset) => Some(offset),
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => None,
            Err(e) => panic!("unexpected produce error: {e}"),
        };
        out.lock().unwrap().writes.entry(partition).or_default().push(Write { key, value, offset });
        rt.sleep(TICK * (1 + rt.below(3) as u32)).await;
    }
}

async fn transactional(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(
        rt.clone(),
        ClientConfig {
            transactional_id: Some("tx".into()),
            txn_timeout: Duration::from_millis(800),
            ..client_config(n)
        },
    );
    let mut k = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let mut txn = match client.begin_transaction().await {
            Ok(t) => t,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected error beginning a transaction: {e}"),
        };
        k += 1;
        let mut writes = Vec::new();
        let mut failed = false;
        for i in 0..1 + rt.below(3) {
            let partition = rt.below(u64::from(PARTITIONS)) as u32;
            let key = format!("t{}", rt.below(3));
            let value = format!("tx{k}-{i}");
            match txn.produce(STREAM, partition, keyed(&key, Some(&value), now_ms(&rt))).await {
                Ok(o) => writes.push((partition, Write { key, value: Some(value), offset: Some(o) })),
                Err(ClientError::GaveUp(..)) => {
                    writes.push((partition, Write { key, value: Some(value), offset: None }));
                }
                Err(_) => {
                    writes.push((partition, Write { key, value: Some(value), offset: None }));
                    failed = true;
                    break;
                }
            }
            rt.sleep(TICK * rt.below(20) as u32).await;
        }
        let committed = if failed || rt.chance(1, 3) {
            let _ = txn.abort().await;
            Some(false)
        } else {
            match txn.commit().await {
                Ok(()) => Some(true),
                Err(ClientError::Unknown(_)) => None,
                Err(ClientError::GaveUp(..)) => Some(false),
                Err(ClientError::Broker { code: lotse_proto::ErrorCode::PRODUCER_FENCED, .. }) => Some(false),
                Err(e) => panic!("unexpected error committing: {e}"),
            }
        };
        {
            let mut o = out.lock().unwrap();
            for (p, mut w) in writes {
                match committed {
                    Some(true) => o.writes.entry(p).or_default().push(w),
                    Some(false) => {
                        o.aborted.insert(w.value.clone().unwrap());
                    }
                    None => {
                        // Committed or not: where it is is not known.
                        w.offset = None;
                        o.writes.entry(p).or_default().push(w);
                    }
                }
            }
        }
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

/// A partition as one replica serves it: each record as `(offset, key,
/// value)`, everything up to `end`, read past the last stable offset.
async fn read_all(client: &Client<SimRuntime>, p: u32, end: u64) -> Vec<(u64, String, Option<String>)> {
    let rt_text = |b: Option<Bytes>| b.map(|b| String::from_utf8(b.to_vec()).unwrap());
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < end {
        let f = match client.fetch_uncommitted(STREAM, p, offset, TICK * 10).await {
            Ok(f) => f,
            Err(ClientError::GaveUp(..) | ClientError::OffsetOutOfRange { .. }) => continue,
            Err(e) => panic!("partition {p}: the healed cluster does not serve reads: {e}"),
        };
        let mut rest = f.records.clone();
        while !rest.is_empty() {
            let (b, r) = Batch::parse(&rest).expect("whole valid batches");
            assert!(b.base_offset() <= offset, "partition {p}: a gap before {}", b.base_offset());
            assert!(b.next_offset() > offset, "partition {p}: a batch before {offset} again");
            if !b.attributes().control {
                for rec in b.records().expect("records that decode") {
                    if rec.offset >= offset {
                        out.push((rec.offset, rt_text(rec.key).expect("every record has a key"), rt_text(rec.value)));
                    }
                }
            }
            offset = b.next_offset();
            rest = r;
        }
    }
    out
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let n = w.cfgs.len() as u64;
    let admin = Client::new(rt.node("admin"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(n) });
    let tombstones = TOMBSTONE_MS.to_string();
    let configs = [("cleanup.policy", "compact"), ("delete.retention.ms", tombstones.as_str())];
    let stream_id = loop {
        match admin.create_stream_with(STREAM, PARTITIONS, n.min(3) as u8, &configs).await {
            Ok(id) | Err(ClientError::ResourceExists { resource_id: id, .. }) => break id,
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => continue,
            Err(e) => panic!("the stream is not created: {e}"),
        }
    };
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = vec![
        rt.spawn(producer(rt.node("c1"), n, out.clone(), stop.clone())),
        rt.spawn(transactional(rt.node("t1"), n, out.clone(), stop.clone())),
    ];
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["c1", "t1"].map(String::from));
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
    for t in tasks.drain(..) {
        t.await;
    }
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // A filler key seals everything written so far.
    let mut ends = BTreeMap::new();
    for p in 0..PARTITIONS {
        let mut last = 0;
        for i in 0..60 {
            let value = format!("filler-{i}-{}", "x".repeat(60));
            loop {
                match admin.produce(STREAM, p, keyed("filler", Some(&value), now_ms(&rt))).await {
                    Ok(o) => {
                        last = o;
                        break;
                    }
                    Err(ClientError::Unknown(_) | ClientError::GaveUp(..)) => continue,
                    Err(e) => panic!("partition {p} made no progress after healing: {e}"),
                }
            }
        }
        ends.insert(p, last + 1);
    }

    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(out.writes.values().flatten().any(|w| w.offset.is_some()), "nothing was acknowledged: checked nothing");
    let mut views: BTreeMap<u32, BTreeMap<NodeId, BTreeMap<String, String>>> = BTreeMap::new();
    for p in 0..PARTITIONS {
        let writes = out.writes.get(&p).cloned().unwrap_or_default();
        let voters: Vec<NodeId> = {
            let deadline = rt.now() + Duration::from_secs(30);
            loop {
                let mut v = Vec::new();
                for (node, m) in admin.node_metrics().await.expect("the healed cluster describes itself") {
                    let Ok(m) = m else { continue };
                    if m.partitions.iter().any(|pm| pm.resource_id == stream_id && pm.partition == p) {
                        v.push(NodeId::from(node));
                    }
                }
                if v.len() as u64 == n.min(3) {
                    break v;
                }
                assert!(rt.now() < deadline, "partition {p}: replicas missing: {v:?}");
                rt.sleep(TICK * 20).await;
            }
        };
        for node in voters {
            // Each replica in turn leads and is read.
            let deadline = rt.now() + Duration::from_secs(60);
            loop {
                match admin.move_leader(STREAM, p, Some(node)).await {
                    Ok(()) => break,
                    Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                    Err(e) => panic!("partition {p}: leadership does not move to {node}: {e}"),
                }
                assert!(rt.now() < deadline, "partition {p}: leadership does not move to {node}");
            }
            let deadline = rt.now() + Duration::from_secs(120);
            let records = loop {
                let leads = admin.node_metrics().await.ok().is_some_and(|ms| {
                    ms.into_iter().any(|(id, m)| {
                        NodeId::from(id) == node
                            && m.is_ok_and(|m| {
                                m.partitions
                                    .iter()
                                    .any(|pm| pm.resource_id == stream_id && pm.partition == p && pm.leader)
                            })
                    })
                });
                if leads {
                    let records = read_all(&admin, p, ends[&p]).await;
                    let mut seen = BTreeSet::new();
                    let compacted = records.iter().all(|(_, k, _)| seen.insert(k.clone()) || k == "filler");
                    let clean = !records.iter().any(|(_, _, v)| v.as_ref().is_some_and(|v| out.aborted.contains(v)));
                    if compacted && clean {
                        break records;
                    }
                }
                assert!(
                    rt.now() < deadline,
                    "partition {p} on node {node}: not compacted after two minutes (leads: {leads})"
                );
                rt.sleep(Duration::from_secs(1)).await;
            };
            let mut view = BTreeMap::new();
            for (key, newest) in records.iter().map(|(o, k, v)| (k, (o, v))).collect::<BTreeMap<_, _>>() {
                if key == "filler" {
                    continue;
                }
                let (o, v) = newest;
                let acked = writes.iter().filter(|w| &w.key == key && w.offset.is_some()).max_by_key(|w| w.offset);
                if let Some(a) = acked {
                    assert!(
                        *o >= a.offset.unwrap(),
                        "partition {p} on node {node}: {key} at {o}, acknowledged at {a:?}"
                    );
                    if *o == a.offset.unwrap() {
                        assert_eq!(v, &a.value, "partition {p} on node {node}: {key} at {o}");
                    }
                }
                assert!(
                    writes.iter().any(|w| &w.key == key && &w.value == v),
                    "partition {p} on node {node}: {key} = {v:?} at {o} was never written"
                );
                if let Some(v) = v {
                    view.insert(key.clone(), v.clone());
                }
            }
            for key in writes.iter().map(|w| &w.key).collect::<BTreeSet<_>>() {
                if records.iter().any(|(_, k, _)| k == key) {
                    continue;
                }
                let acked = writes.iter().filter(|w| &w.key == key && w.offset.is_some()).max_by_key(|w| w.offset);
                let maybe_tombstone = acked.is_none_or(|a| a.value.is_none())
                    || writes.iter().any(|w| &w.key == key && w.value.is_none() && w.offset.is_none());
                assert!(maybe_tombstone, "partition {p} on node {node}: {key} is gone, its newest write {acked:?}");
            }
            views.entry(p).or_default().insert(node, view);
        }
    }
    for (p, by_node) in &views {
        let first = by_node.values().next().unwrap();
        for (node, view) in by_node {
            assert_eq!(view, first, "partition {p}: node {node} disagrees");
        }
    }
    drop(admin);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn compaction_keeps_each_keys_newest_record_on_every_replica() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

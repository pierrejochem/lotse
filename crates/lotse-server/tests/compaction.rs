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
    /// The nodes' `[compaction] map_bytes`.
    map_bytes: u64,
}

impl World {
    async fn new(rt: SimRuntime) -> World {
        let n = if rt.chance(1, 2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let log = LogConfig { segment_bytes: 400 + rt.below(1_600), index_interval_bytes: 64 };
        let map_bytes =
            if rt.chance(1, 2) { 48 * (6 + rt.below(7)) } else { lotse_server::DEFAULT_COMPACTION_MAP_BYTES };
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
                        zones: Default::default(),
                        streams: Vec::new(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: Duration::from_millis(600),
                        group_heartbeat_interval: TICK * 5,
                        metadata: Some(1),
                        coordinator: Some(2),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        // Half the runs map 6 to 12 keys per sweep (ADR 0098),
                        // fewer than the run writes now and then, so a
                        // partition takes several sweeps; fewer still would
                        // take longer than the run waits.
                        retention: RetentionConfig { compaction_map_bytes: map_bytes, ..RetentionConfig::keep() },
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
            map_bytes,
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
async fn read_all(client: &Client<SimRuntime>, p: u32, end: u64) -> (Vec<(u64, String, Option<String>)>, bool) {
    let rt_text = |b: Option<Bytes>| b.map(|b| String::from_utf8(b.to_vec()).unwrap());
    let mut out = Vec::new();
    let mut offset = 0;
    // Producers with transactional data since their last marker; an ABORT
    // marker after such data means the replica still holds aborted data,
    // also of a transaction whose outcome the writer never learned.
    let mut open = BTreeSet::new();
    let mut aborted_left = false;
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
            if b.attributes().control {
                if let Ok(Some(lotse_record::Control::Marker(ty, _))) = lotse_record::control(&b)
                    && open.remove(&b.producer_id())
                    && ty == lotse_record::ControlType::Abort
                {
                    aborted_left = true;
                }
            } else {
                let records = b.records().expect("records that decode");
                // A batch compaction emptied keeps its offsets, not its data.
                if b.attributes().transactional && !records.is_empty() {
                    open.insert(b.producer_id());
                }
                for rec in records {
                    if rec.offset >= offset {
                        out.push((rec.offset, rt_text(rec.key).expect("every record has a key"), rt_text(rec.value)));
                    }
                }
            }
            offset = b.next_offset();
            rest = r;
        }
    }
    (out, aborted_left)
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let n = w.cfgs.len() as u64;
    let map_bytes = w.map_bytes;
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
        // A node runs one compaction sweep at a time (ADR 0098).
        for (id, running) in &w.running {
            if let Some((_, handle)) = running {
                let sweeping: Vec<u64> =
                    handle.statuses().into_iter().filter(|(_, s)| s.sweeping).map(|(g, _)| g).collect();
                assert!(sweeping.len() <= 1, "node {id} sweeps groups {sweeping:?} at once");
            }
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
            // A map of a few keys takes a sweep per few keys, each one
            // rewriting at most a segment a second, and a node sweeps one
            // partition at a time: that takes longer than one sweep of all.
            let settle = if map_bytes < lotse_server::DEFAULT_COMPACTION_MAP_BYTES { 600 } else { 120 };
            let deadline = rt.now() + Duration::from_secs(settle);
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
                    let (records, aborted_left) = read_all(&admin, p, ends[&p]).await;
                    let mut seen = BTreeSet::new();
                    let compacted = records.iter().all(|(_, k, _)| seen.insert(k.clone()) || k == "filler");
                    let clean = !records.iter().any(|(_, _, v)| v.as_ref().is_some_and(|v| out.aborted.contains(v)));
                    if compacted && clean && !aborted_left {
                        break records;
                    }
                }
                assert!(
                    rt.now() < deadline,
                    "partition {p} on node {node}: not compacted after {settle} s (leads: {leads})"
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

/// A node's configuration for [`returned`]: nodes 1 to 3 are the cluster,
/// node 4 joins it.
fn returned_config(id: NodeId, log: LogConfig) -> NodeConfig {
    let known: Vec<NodeId> = if id <= 3 { vec![1, 2, 3] } else { vec![1, 2, 3, id] };
    NodeConfig {
        id,
        listen: format!("n{id}:7000"),
        peers: known.iter().map(|n| (*n, format!("n{n}:7000"))).collect(),
        groups: vec![1, 2],
        tick: TICK,
        pre_vote: true,
        log,
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: known.iter().map(|n| (*n, format!("n{n}:9000"))).collect(),
        zones: Default::default(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: Duration::from_millis(600),
        group_heartbeat_interval: TICK * 5,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        retention: RetentionConfig::keep(),
        join: id > 3,
        auth: lotse_server::AuthConfig::none(),
    }
}

/// The nodes that report a replica of the stream's partition 0, and which
/// of them leads; `None` while the cluster does not answer.
async fn replicas(admin: &Client<SimRuntime>, stream_id: u64) -> Option<(BTreeMap<NodeId, u64>, Option<NodeId>)> {
    let mut ends = BTreeMap::new();
    let mut leader = None;
    for (node, m) in admin.node_metrics().await.ok()? {
        let Ok(m) = m else { continue };
        for pm in m.partitions.iter().filter(|pm| pm.resource_id == stream_id && pm.partition == 0) {
            ends.insert(NodeId::from(node), pm.log_end);
            if pm.leader {
                leader = Some(NodeId::from(node));
            }
        }
    }
    Some((ends, leader))
}

/// A replica that left a compacted stream's group and is placed on its node
/// again must not bring back a value whose tombstone the leader dropped
/// while it was gone (ADR 0067 counts only current members).
///
/// Node 3 holds `k = old` and goes down. Node 4 joins, and spreading moves
/// the stream from node 3 (the fullest, highest id) to node 4; that move
/// settles without node 3. `k` gets a tombstone, which the leader drops
/// once it is old enough and every member holds it. Node 4 is drained,
/// and the stream goes back to node 3, the emptiest node, which then
/// starts. Read from node 3 as leader, `k` must stay gone.
///
/// How node 3 misses the move off in [`returned`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Away {
    /// It is down, and starts once the stream is back.
    Down,
    /// It keeps running but hears no one until the stream is back: its
    /// replica is open all along, and its view of the metadata group jumps
    /// from before the move off to after the return.
    CutOff,
    /// It is down, and starts hearing the stream's leader but not the
    /// metadata group's for a while: the stream would bring its old log up
    /// to the membership that placed it there again before the node learns
    /// that the group had left it.
    MetadataBehind,
}

/// The node that leads group `g` (resource 0: the metadata group is 1).
async fn leader_of(admin: &Client<SimRuntime>, g: u64) -> Option<NodeId> {
    let ms = admin.node_metrics().await.ok()?;
    ms.into_iter().find_map(|(node, m)| {
        m.ok()?.partitions.iter().any(|pm| pm.group == g && pm.leader).then_some(NodeId::from(node))
    })
}

async fn returned(rt: SimRuntime, away: Away) {
    let log = LogConfig { segment_bytes: 400, index_interval_bytes: 64 };
    let hosts: BTreeMap<NodeId, SimRuntime> = (1..=4).map(|id| (id, rt.node(&format!("n{id}")))).collect();
    let mut w = World {
        cfgs: (1..=4).map(|id| (id, returned_config(id, log))).collect(),
        hosts,
        running: BTreeMap::new(),
        down_until: BTreeMap::new(),
        rt: rt.clone(),
        map_bytes: lotse_server::DEFAULT_COMPACTION_MAP_BYTES,
    };
    for id in 1..=3 {
        w.start(id).await;
    }
    let admin = Client::new(rt.node("admin"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(3) });
    let configs = [("cleanup.policy", "compact"), ("delete.retention.ms", "100")];
    let stream_id = loop {
        match admin.create_stream_with(STREAM, 1, 3, &configs).await {
            Ok(id) | Err(ClientError::ResourceExists { resource_id: id, .. }) => break id,
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => continue,
            Err(e) => panic!("the stream is not created: {e}"),
        }
    };
    let produce = async |key: &str, value: Option<&str>| loop {
        match admin.produce(STREAM, 0, keyed(key, value, now_ms(&rt))).await {
            Ok(o) => break o,
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => continue,
            Err(e) => panic!("produce: {e}"),
        }
    };
    let fill = async |from: u32| {
        let mut last = 0;
        for i in from..from + 30 {
            last = produce("filler", Some(&format!("filler-{i}-{}", "x".repeat(60)))).await;
        }
        last
    };
    let old = produce("k", Some("old")).await;
    fill(0).await;
    // Node 3 holds the old value, sealed, before it goes down.
    let deadline = rt.now() + Duration::from_secs(30);
    while !replicas(&admin, stream_id).await.is_some_and(|(ends, _)| ends.get(&3).is_some_and(|e| *e > old + 30)) {
        assert!(rt.now() < deadline, "node 3 never holds the old value");
        rt.sleep(TICK * 10).await;
    }
    let others = ["n1", "n2", "n4", "admin"];
    if away == Away::CutOff {
        for o in others {
            rt.net().block("n3", o);
        }
    } else {
        w.crash(3, false).await;
    }

    // Node 4 joins; the stream moves from node 3 to node 4.
    loop {
        match admin.add_node(4, "n4:7000", "n4:9000").await {
            Ok(()) => break,
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => continue,
            Err(e) => panic!("node 4 is not added: {e}"),
        }
    }
    w.start(4).await;
    let deadline = rt.now() + Duration::from_secs(60);
    while !replicas(&admin, stream_id).await.is_some_and(|(ends, _)| ends.keys().copied().eq([1, 2, 4])) {
        assert!(rt.now() < deadline, "the stream does not move to node 4");
        rt.sleep(TICK * 20).await;
    }

    // The tombstone, sealed, then dropped by the leader.
    produce("k", None).await;
    let mut round = 30;
    let end = loop {
        let end = fill(round).await + 1;
        round += 30;
        let (records, _) = read_all(&admin, 0, end).await;
        if !records.iter().any(|(_, k, _)| k == "k") {
            break end;
        }
        assert!(round < 30 * 40, "the leader never drops the tombstone");
        rt.sleep(Duration::from_secs(2)).await;
    };

    // Node 3 knows nodes 1 and 2 only until it learns of node 4: for
    // the stream to reach it first, one of them leads it, not the one that
    // leads the metadata group.
    if away == Away::MetadataBehind {
        let deadline = rt.now() + Duration::from_secs(60);
        loop {
            if let Some(meta) = leader_of(&admin, 1).await.filter(|m| [1, 2].contains(m)) {
                let other = 3 - meta;
                if admin.move_leader(STREAM, 0, Some(other)).await.is_ok()
                    && replicas(&admin, stream_id).await.is_some_and(|(_, l)| l == Some(other))
                {
                    break;
                }
            }
            assert!(rt.now() < deadline, "the stream cannot be led apart from the metadata group");
            rt.sleep(TICK * 20).await;
        }
    }

    // Node 4 drains; the stream goes back to node 3, which starts.
    loop {
        match admin.remove_node(4).await {
            Ok(()) => break,
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => continue,
            Err(e) => panic!("node 4 is not removed: {e}"),
        }
    }
    rt.sleep(Duration::from_secs(2)).await;
    match away {
        Away::CutOff => rt.net().heal(),
        Away::Down => w.start(3).await,
        Away::MetadataBehind => {
            // Cut off from the metadata leader only, while the stream's
            // leader (pinned above) is another node it knows.
            let deadline = rt.now() + Duration::from_secs(60);
            let meta = loop {
                let meta = leader_of(&admin, 1).await;
                let stream = replicas(&admin, stream_id).await.and_then(|(_, l)| l);
                if let (Some(meta), Some(stream)) = (meta, stream)
                    && meta != stream
                    && [1, 2].contains(&stream)
                {
                    break meta;
                }
                assert!(rt.now() < deadline, "the stream is not led apart from the metadata group: {meta:?}");
                rt.sleep(TICK * 20).await;
            };
            rt.net().block("n3", &format!("n{meta}"));
            w.start(3).await;
            rt.sleep(Duration::from_secs(5)).await;
            rt.net().heal();
        }
    }
    let deadline = rt.now() + Duration::from_secs(120);
    loop {
        let placed = replicas(&admin, stream_id).await.is_some_and(|(ends, _)| ends.keys().copied().eq([1, 2, 3]));
        if placed {
            match admin.move_leader(STREAM, 0, Some(3)).await {
                Ok(()) => {
                    if replicas(&admin, stream_id).await.is_some_and(|(_, leader)| leader == Some(3)) {
                        break;
                    }
                }
                Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
                Err(e) => panic!("leadership does not move to node 3: {e}"),
            }
        }
        assert!(rt.now() < deadline, "the stream does not come back to node 3");
        rt.sleep(TICK * 20).await;
    }
    let (records, _) = read_all(&admin, 0, end).await;
    // A tombstone may still be there (only a leader drops one, and the
    // stream may have had another since), but no value of `k`.
    let back: Vec<_> = records.iter().filter(|(_, k, v)| k == "k" && v.is_some()).collect();
    assert!(back.is_empty(), "node 3 brought back what the tombstone deleted: {back:?}");
    drop(admin);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_replica_placed_again_does_not_bring_back_a_deleted_key() {
    lotse_sim::check_few_seeds(3, |seed| Sim::new(seed).run(|rt| returned(rt, Away::Down)));
}

#[test]
fn a_replica_cut_off_and_placed_again_does_not_bring_back_a_deleted_key() {
    lotse_sim::check_few_seeds(3, |seed| Sim::new(seed).run(|rt| returned(rt, Away::CutOff)));
}

#[test]
fn a_replica_placed_again_before_its_node_hears_the_metadata_group_does_not_bring_back_a_deleted_key() {
    lotse_sim::check_few_seeds(3, |seed| Sim::new(seed).run(|rt| returned(rt, Away::MetadataBehind)));
}

//! Retention against a faulty cluster, checked over many seeds.
//!
//! Three or five nodes host a stream with two partitions on small segments
//! and a coordinator group, and keep records for a short time, a few
//! kilobytes per partition, or both (ADR 0046). An idempotent producer and
//! a transactional one write records stamped with the time; a reader
//! follows each partition from the start, and a consumer group member reads
//! both, while pairs of hosts are partitioned and nodes lose power and
//! restart. Retention deletes under the readers' feet. Afterwards the
//! network heals and every partition is read from where it now starts.
//!
//! Checked:
//! - every acknowledged batch at or after the partition's start is there,
//!   at its offset; one before it was old enough to go (or, with a size
//!   limit, may have gone for size);
//! - every committed transaction's records are there unless old enough to
//!   go, and no aborted one is visible;
//! - whatever a reader or the group member read is there at that offset
//!   unless deleted since;
//! - the group member never sees an offset out of range: it goes on from
//!   the oldest record left, and reads what is written after healing;
//! - every replica catches up with its leader, from a snapshot where the
//!   leader no longer has what it lacks, and keeps at least the size limit.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, GroupConfig};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, RetentionConfig, StateMachine, StreamConfig};
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
const STREAM: &str = "orders";
const PARTITIONS: u32 = 2;
const SESSION: Duration = Duration::from_millis(600);

fn now_ms(rt: &SimRuntime) -> i64 {
    rt.unix_time().as_millis() as i64
}

fn batch(value: &str, ts: i64) -> Bytes {
    // Batches come plain, zstd and lz4 (ADR 0058): replicas, snapshots,
    // retention and readers handle them all alike.
    let mut options = BatchOptions::default();
    options.attributes.compression =
        [lotse_record::Compression::None, lotse_record::Compression::Zstd, lotse_record::Compression::Lz4]
            [value.len() % 3];
    let mut b = BatchBuilder::new(options);
    b.push(ts, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn committed(f: &lotse_client::Fetched) -> Vec<(u64, String)> {
    f.committed_batches()
        .expect("whole valid batches")
        .into_iter()
        .map(|b| {
            let v = b.records().unwrap()[0].value.clone().unwrap();
            (b.base_offset(), String::from_utf8(v.to_vec()).unwrap())
        })
        .collect()
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

/// A value and the timestamp it was written with.
type Written = (String, i64);

/// One replica of a partition: log start, log end, bytes, whether it leads.
type ReplicaView = (u64, u64, u64, bool);

#[derive(Default)]
struct Outcomes {
    /// Per partition: offset → (value, timestamp), as acknowledged.
    acked: BTreeMap<u32, BTreeMap<u64, (String, i64)>>,
    /// Per partition: what readers saw, `(offset, value)`.
    read: BTreeMap<u32, Vec<(u64, String)>>,
    /// Transactions: `(value, timestamp)` each, and whether committed
    /// (`Some(true)`), aborted (`Some(false)`) or unknown.
    txns: Vec<(Vec<Written>, Option<bool>)>,
    /// Times a reader found its offset deleted and went on.
    skipped: u64,
}

struct World {
    rt: SimRuntime,
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
    down_until: BTreeMap<NodeId, lotse_rt::Instant>,
}

impl World {
    async fn new(rt: SimRuntime, retention: RetentionConfig) -> World {
        let n = if rt.chance(1, 2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let log = LogConfig { segment_bytes: 300 + rt.below(1_200), index_interval_bytes: 64 };
        let streams = vec![StreamConfig { name: STREAM.into(), resource_id: 7, partitions: vec![1, 2] }];
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        // Group 3 coordinates, group 4 holds the metadata.
                        groups: vec![1, 2, 3, 4],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        streams: streams.clone(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: SESSION,
                        group_heartbeat_interval: TICK * 5,
                        metadata: Some(4),
                        coordinator: Some(3),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        retention,
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
        // Long enough, at times, for the others to delete what it lacks.
        let down = TICK * (5 + self.rt.below(150) as u32);
        self.down_until.insert(id, self.rt.now() + down);
    }
}

async fn producer(rt: SimRuntime, n: u64, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let partition = rt.below(u64::from(PARTITIONS)) as u32;
        let value = format!("p-{seq}");
        seq += 1;
        let ts = now_ms(&rt);
        match client.produce(STREAM, partition, batch(&value, ts)).await {
            Ok(offset) => {
                let before = out.lock().unwrap().acked.entry(partition).or_default().insert(offset, (value, ts));
                assert!(before.is_none(), "offset {offset} acknowledged twice in partition {partition}");
            }
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
            Err(e) => panic!("unexpected produce error: {e}"),
        }
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
        let mut values = Vec::new();
        let mut failed = false;
        for i in 0..1 + rt.below(3) {
            let partition = rt.below(u64::from(PARTITIONS)) as u32;
            let value = format!("tx{k}-{i}");
            let ts = now_ms(&rt);
            match txn.produce(STREAM, partition, batch(&value, ts)).await {
                Ok(_) => values.push((value, ts)),
                Err(ClientError::GaveUp(..)) => {}
                Err(_) => {
                    values.push((value, ts));
                    failed = true;
                    break;
                }
            }
            // Open for a while: retention must wait for it.
            rt.sleep(TICK * rt.below(20) as u32).await;
        }
        let end = if failed || rt.chance(1, 4) {
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
        out.lock().unwrap().txns.push((values, end));
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

/// Follows a partition from the start; where retention deleted its next
/// offset, goes on from the oldest record left.
async fn reader(rt: SimRuntime, n: u64, partition: u32, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config(n));
    let mut offset = 0u64;
    while !stop.load(Ordering::Relaxed) {
        match client.fetch(STREAM, partition, offset, TICK * 20).await {
            Ok(f) => {
                let mut records = f.records.clone();
                while !records.is_empty() {
                    let (b, rest) = Batch::parse(&records).expect("whole valid batches");
                    assert_eq!(b.base_offset(), offset, "partition {partition}: a gap or overlap in what was read");
                    offset = b.next_offset();
                    records = rest;
                }
                out.lock().unwrap().read.entry(partition).or_default().extend(committed(&f));
            }
            Err(ClientError::OffsetOutOfRange { log_start, .. }) if log_start > offset => {
                offset = log_start;
                out.lock().unwrap().skipped += 1;
            }
            // A new leader that has not learnt its commit index yet.
            Err(ClientError::OffsetOutOfRange { .. } | ClientError::GaveUp(..)) => {}
            Err(e) => panic!("unexpected fetch error: {e}"),
        }
        // Slow, and now and then stalled long enough for retention to
        // overtake it.
        let pause = if rt.chance(1, 40) { 50 + rt.below(250) } else { rt.below(30) };
        rt.sleep(TICK * pause as u32).await;
    }
}

/// One member of a group reading both partitions, slowly, committing as it
/// goes; then, once `stop` is set, until it has read `until` everywhere.
async fn member(
    rt: SimRuntime,
    n: u64,
    out: Arc<Mutex<Outcomes>>,
    stop: Arc<AtomicBool>,
    until: Arc<Mutex<BTreeMap<u32, String>>>,
) {
    let client = Client::new(rt.clone(), client_config(n));
    let cfg = GroupConfig {
        session_timeout: SESSION,
        rebalance_timeout: SESSION,
        auto_commit: true,
        ..GroupConfig::default()
    };
    let mut seen_after: BTreeSet<String> = BTreeSet::new();
    let deadline = || rt.now() + Duration::from_secs(60);
    let mut give_up = None;
    loop {
        let mut consumer = match client.join_group("g", &[STREAM], cfg.clone()).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected join error: {e}"),
        };
        loop {
            if stop.load(Ordering::Relaxed) {
                let want = until.lock().unwrap().clone();
                if want.len() == PARTITIONS as usize && want.values().all(|v| seen_after.contains(v)) {
                    consumer.close().await;
                    return;
                }
                let at = *give_up.get_or_insert_with(deadline);
                assert!(rt.now() < at, "the group never read what was written after healing: {want:?}");
            }
            let records = match consumer.poll(TICK * 10).await {
                Ok(r) => r,
                Err(ClientError::GaveUp(..)) => continue,
                Err(e) => panic!("unexpected poll error: {e}"),
            };
            {
                let mut o = out.lock().unwrap();
                for r in records {
                    let v = String::from_utf8(r.record.value.clone().unwrap().to_vec()).unwrap();
                    seen_after.insert(v.clone());
                    o.read.entry(r.partition).or_default().push((r.record.offset, v));
                }
            }
            if !stop.load(Ordering::Relaxed) {
                rt.sleep(TICK * rt.below(20) as u32).await;
                if rt.chance(1, 200) {
                    // Leaves and joins again: the committed offset may be
                    // deleted meanwhile.
                    break;
                }
            }
        }
        consumer.close().await;
    }
}

async fn run(rt: SimRuntime) {
    let ms = 300 + rt.below(1_200);
    let bytes = 1_000 + rt.below(3_000);
    let retention = match rt.below(3) {
        0 => RetentionConfig { ms: Some(ms), bytes: None, ..RetentionConfig::keep() },
        1 => RetentionConfig { ms: None, bytes: Some(bytes), ..RetentionConfig::keep() },
        _ => RetentionConfig { ms: Some(ms), bytes: Some(bytes), ..RetentionConfig::keep() },
    };
    let mut w = World::new(rt.clone(), retention).await;
    let n = w.cfgs.len() as u64;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let until = Arc::new(Mutex::new(BTreeMap::new()));
    let mut tasks = vec![
        rt.spawn(producer(rt.node("c1"), n, out.clone(), stop.clone())),
        rt.spawn(transactional(rt.node("t1"), n, out.clone(), stop.clone())),
        rt.spawn(reader(rt.node("r1"), n, 0, out.clone(), stop.clone())),
        rt.spawn(reader(rt.node("r2"), n, 1, out.clone(), stop.clone())),
    ];
    let group = rt.spawn(member(rt.node("g1"), n, out.clone(), stop.clone(), until.clone()));
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["c1", "t1", "r1", "r2", "g1"].map(String::from));
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

    let reader =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config(n) });
    // A marker per partition, for the group member to reach.
    let mut markers = BTreeMap::new();
    for p in 0..PARTITIONS {
        let marker = format!("after-heal-{p}");
        let mut at = None;
        for _ in 0..20 {
            match reader.produce(STREAM, p, batch(&marker, now_ms(&rt))).await {
                Ok(o) => {
                    at = Some(o);
                    break;
                }
                Err(ClientError::Unknown(_)) => continue,
                Err(e) => panic!("partition {p} made no progress after healing: {e}"),
            }
        }
        markers.insert(p, at.unwrap_or_else(|| panic!("partition {p}: no produce settled after healing")));
        until.lock().unwrap().insert(p, marker);
    }
    group.await;

    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(out.acked.values().map(BTreeMap::len).sum::<usize>() > 0, "nothing was acknowledged: checked nothing");
    let mut visible: BTreeSet<String> = BTreeSet::new();
    for p in 0..PARTITIONS {
        // Read the partition from where it starts now.
        let (mut offset, mut start) = (0, 0);
        let mut log: BTreeMap<u64, String> = BTreeMap::new();
        let deadline = rt.now() + Duration::from_secs(30);
        while offset <= markers[&p] {
            assert!(rt.now() < deadline, "partition {p}: the last stable offset is stuck below {}", markers[&p]);
            match reader.fetch(STREAM, p, offset, TICK * 10).await {
                Ok(f) => {
                    offset = f.next_offset(offset);
                    log.extend(committed(&f));
                }
                Err(ClientError::OffsetOutOfRange { log_start, .. }) if log_start > offset => {
                    // Retention may still be deleting: whatever was read
                    // below the new start goes too.
                    log.retain(|o, _| *o >= log_start);
                    (offset, start) = (log_start, log_start);
                }
                Err(e) => panic!("partition {p}: the healed cluster does not serve reads: {e}"),
            }
        }
        // What the check itself saw deleted happened before now.
        let checked_at = now_ms(&rt);
        for (o, (v, ts)) in out.acked.get(&p).into_iter().flatten() {
            if *o >= start {
                assert_eq!(
                    log.get(o),
                    Some(v),
                    "partition {p}: acknowledged {v} at {o} is lost or moved (start {start})"
                );
            } else if let (Some(ms), None) = (retention.ms, retention.bytes) {
                assert!(
                    *ts < checked_at - ms as i64,
                    "partition {p}: {v} at {o}, written at {ts}, was deleted younger than {ms} ms (now {checked_at})"
                );
            }
        }
        for (o, v) in out.read.get(&p).into_iter().flatten() {
            if *o >= start {
                assert_eq!(log.get(o), Some(v), "partition {p}: {v} was read at {o} but is not there");
            }
        }
        visible.extend(log.into_values());
    }
    let checked_at = now_ms(&rt);
    for (values, end) in &out.txns {
        let shown = values.iter().filter(|(v, _)| visible.contains(v)).count();
        match end {
            Some(true) => {
                if let (Some(ms), None) = (retention.ms, retention.bytes) {
                    for (v, ts) in values {
                        assert!(
                            visible.contains(v) || *ts < checked_at - ms as i64,
                            "committed {v}, written at {ts}, is gone younger than {ms} ms"
                        );
                    }
                }
            }
            Some(false) => assert_eq!(shown, 0, "aborted {values:?}, but {shown} visible"),
            None => {}
        }
    }

    // Every replica catches up with its leader and keeps the size limit.
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        let mut behind = Vec::new();
        let answers = reader.node_metrics().await.expect("the healed cluster describes itself");
        let mut ends: BTreeMap<u64, BTreeMap<u32, ReplicaView>> = BTreeMap::new();
        for (node, m) in answers {
            let Ok(m) = m else {
                behind.push(format!("node {node} did not answer"));
                continue;
            };
            for pm in m.partitions.iter().filter(|pm| pm.resource_id == 7) {
                ends.entry(u64::from(pm.partition))
                    .or_default()
                    .insert(node, (pm.log_start, pm.log_end, pm.size_bytes, pm.leader));
            }
        }
        for (p, replicas) in &ends {
            let leader_end = replicas.values().find(|r| r.3).map(|r| r.1);
            for (node, (log_start, log_end, size, _)) in replicas {
                if Some(*log_end) != leader_end {
                    behind.push(format!("partition {p} on node {node}: end {log_end}, the leader's {leader_end:?}"));
                } else if let (None, Some(limit)) = (retention.ms, retention.bytes) {
                    assert!(
                        *log_start == 0 || *size >= limit,
                        "partition {p} on node {node} keeps {size} bytes from {log_start}, below {limit}"
                    );
                }
            }
            if replicas.len() as u64 != n {
                behind.push(format!("partition {p}: {} replicas answered", replicas.len()));
            }
        }
        if behind.is_empty() {
            break;
        }
        assert!(rt.now() < deadline, "replicas do not catch up: {behind:?}");
        rt.sleep(TICK * 20).await;
    }
    // A stream created at runtime with a size limit of its own, where the
    // node keeps more (or keeps by age only): its partition goes by its own
    // setting.
    let own = 1_500 + rt.below(1_000);
    let limits = [("retention.bytes", own.to_string()), ("retention.ms", "-1".to_string())];
    let limits: Vec<(&str, &str)> = limits.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let events = match reader.create_stream_with("events", 1, n.min(3) as u8, &limits).await {
        Ok(id) | Err(ClientError::ResourceExists { resource_id: id, .. }) => id,
        Err(e) => panic!("the healed cluster does not create a stream: {e}"),
    };
    for i in 0..80 {
        let value = format!("event-{i}-{}", "x".repeat(80));
        loop {
            match reader.produce("events", 0, batch(&value, now_ms(&rt))).await {
                Ok(_) => break,
                Err(ClientError::Unknown(_) | ClientError::UnknownStream(..)) => rt.sleep(TICK * 5).await,
                Err(e) => panic!("events: {e}"),
            }
        }
    }
    let deadline = rt.now() + Duration::from_secs(30);
    loop {
        let mut replicas = Vec::new();
        for (_, m) in reader.node_metrics().await.expect("the healed cluster describes itself") {
            let Ok(m) = m else { continue };
            replicas.extend(
                m.partitions.iter().filter(|pm| pm.resource_id == events).map(|pm| (pm.log_start, pm.size_bytes)),
            );
        }
        if !replicas.is_empty() && replicas.iter().all(|(start, _)| *start > 0) {
            for (start, size) in replicas {
                assert!(size >= own, "events keeps {size} bytes from {start}, below its own limit {own}");
            }
            break;
        }
        assert!(rt.now() < deadline, "events keeps everything, its own limit is {own} bytes: {replicas:?}");
        rt.sleep(TICK * 20).await;
    }
    drop(reader);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn retention_deletes_only_what_it_may_and_readers_go_on() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

/// Without faults, the coordinator group's log is cut again and again
/// while several transactional producers keep writing to it: every
/// transaction begins, writes and commits, none is lost to a cut.
async fn quiet(rt: SimRuntime) {
    let mut w = World::new(rt.clone(), RetentionConfig::keep()).await;
    let n = w.cfgs.len() as u64;
    let end = rt.now() + Duration::from_secs(5);
    let mut tasks = Vec::new();
    for t in 0..6 {
        let host = rt.node(&format!("t{t}"));
        tasks.push(rt.spawn(async move {
            let client = Client::new(
                host.clone(),
                ClientConfig {
                    transactional_id: Some(format!("tx{t}")),
                    retry_for: Duration::from_secs(5),
                    ..client_config(n)
                },
            );
            let mut committed = 0;
            while host.now() < end {
                let mut txn = client.begin_transaction().await.unwrap_or_else(|e| panic!("tx{t} did not begin: {e}"));
                let p = host.below(u64::from(PARTITIONS)) as u32;
                txn.produce(STREAM, p, batch(&format!("tx{t}-{committed}"), now_ms(&host)))
                    .await
                    .unwrap_or_else(|e| panic!("tx{t}: a quiet cluster lost a transactional write: {e}"));
                txn.commit().await.unwrap_or_else(|e| panic!("tx{t}: a quiet cluster left a commit open: {e}"));
                committed += 1;
            }
            assert!(committed > 20, "tx{t}: only {committed} transactions in 5 s");
        }));
    }
    for t in tasks {
        t.await;
    }
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn requests_learn_their_outcome_while_retention_cuts_the_coordinator_log() {
    lotse_sim::check_few_seeds(5, |seed| Sim::new(seed).run(quiet));
}

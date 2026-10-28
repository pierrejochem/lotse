//! Clients against a faulty cluster, checked over many seeds.
//!
//! Three or five nodes host a stream with two partitions (two Raft groups;
//! five voters allow histories three cannot, such as an entry that is
//! truncated on its leader and still committed later) and a coordinator
//! group. Two producers write through `lotse-client`, one idempotent and one
//! not, two consumers read every
//! partition from the start, all over the simulated network, while pairs of
//! hosts (clients included) are partitioned and nodes lose power and
//! restart. Afterwards the network heals and the partitions are read in full.
//!
//! Checked:
//! - every acknowledged batch is in the partition, at the offset the
//!   acknowledgement named;
//! - every batch a consumer read during the faults is still there at that
//!   offset (nothing uncommitted left a node);
//! - no batch a producer gave up on is there (a definite "not written"
//!   means not written);
//! - no batch is there twice (the idempotent producer retries unknown
//!   outcomes, the other never does);
//! - after healing, a new batch commits in each partition.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine, StreamConfig};
use lotse_sim::{Sim, SimRuntime, check_seeds};

/// The test reads partitions through `Fetch`; the state machine keeps nothing.
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

fn batch(value: &str, records: u64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value.as_bytes()), &[]);
    }
    b.finish().unwrap()
}

/// What a read_committed consumer sees in `f`, as `(base_offset, value)`:
/// no control batches, no aborted transactions.
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

fn client_config() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(800),
        retry_for: Duration::from_millis(1_500),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(600),
        ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
    }
}

#[derive(Default)]
struct Outcomes {
    /// Per partition: base offset → value, as acknowledged.
    acked: BTreeMap<u32, BTreeMap<u64, String>>,
    /// Values a producer gave up on: definitely not written.
    not_written: Vec<String>,
    /// Per partition: what consumers read, `(offset, value)`.
    read: BTreeMap<u32, Vec<(u64, String)>>,
    unknown: u64,
    /// Transactions: their values and how they ended.
    txns: Vec<(Vec<String>, TxnEnd)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnEnd {
    /// `commit` returned: all of it is visible.
    Committed,
    /// Aborted, or left open (the coordinator aborts it): none of it.
    Aborted,
    /// Unknown: all of it or none.
    Unknown,
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
                        // Group 3 coordinates producers.
                        groups: vec![1, 2, 3],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        streams: streams.clone(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
                        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
                        metadata: None,
                        coordinator: Some(3),
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

/// Producer `c1` is idempotent and retries unknown outcomes; `c2` is not
/// and leaves them to the caller. Neither may cause a duplicate.
async fn producer(rt: SimRuntime, name: String, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let idempotent = name == "c1";
    let client = Client::new(rt.clone(), ClientConfig { idempotent, ..client_config() });
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let partition = rt.below(u64::from(PARTITIONS)) as u32;
        let value = format!("{name}-{seq}");
        seq += 1;
        let res = client.produce(STREAM, partition, batch(&value, 1 + rt.below(3))).await;
        {
            let mut o = out.lock().unwrap();
            match res {
                Ok(offset) => {
                    let before = o.acked.entry(partition).or_default().insert(offset, value.clone());
                    assert!(before.is_none(), "offset {offset} acknowledged twice in partition {partition}");
                }
                Err(ClientError::GaveUp(..)) => o.not_written.push(value),
                Err(ClientError::Unknown(_)) => o.unknown += 1,
                Err(e) => panic!("unexpected produce error: {e}"),
            }
        }
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

/// Writes transactions of one to three batches across the partitions and
/// commits, aborts or abandons each.
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
        let mut values = Vec::new();
        let mut failed = false;
        for i in 0..1 + rt.below(3) {
            let partition = rt.below(u64::from(PARTITIONS)) as u32;
            let value = format!("tx{n}-{i}");
            match txn.produce(STREAM, partition, batch(&value, 1)).await {
                Ok(_) => values.push(value),
                // Definitely not written.
                Err(ClientError::GaveUp(..)) => {}
                // Maybe written: it counts with the transaction, which must abort.
                Err(_) => {
                    values.push(value);
                    failed = true;
                    break;
                }
            }
        }
        let end = match rt.below(6) {
            // Abandoned: the next begin, or the timeout, aborts it.
            0 => {
                drop(txn);
                TxnEnd::Aborted
            }
            1 => {
                let _ = txn.abort().await;
                TxnEnd::Aborted
            }
            _ if failed => {
                let _ = txn.abort().await;
                TxnEnd::Aborted
            }
            _ => match txn.commit().await {
                Ok(()) => TxnEnd::Committed,
                Err(ClientError::Unknown(_)) => TxnEnd::Unknown,
                // Not decided (it stays open and is aborted later), or fenced
                // because the coordinator timed it out and aborted it.
                Err(ClientError::GaveUp(..)) => TxnEnd::Aborted,
                Err(ClientError::Broker { code: lotse_proto::ErrorCode::PRODUCER_FENCED, .. }) => TxnEnd::Aborted,
                Err(e) => panic!("unexpected error committing: {e}"),
            },
        };
        out.lock().unwrap().txns.push((values, end));
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
}

async fn consumer(rt: SimRuntime, partition: u32, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
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
                // Only a response with records says something: a new leader's
                // high watermark lags what its predecessor served until an
                // entry of its own term commits, so an empty answer may come
                // with one below `offset`.
                if !f.records.is_empty() {
                    assert!(offset <= f.high_watermark, "read past the high watermark");
                    assert!(offset <= f.last_stable_offset, "read past the last stable offset");
                }
                out.lock().unwrap().read.entry(partition).or_default().extend(committed(&f));
            }
            Err(ClientError::GaveUp(..)) => {}
            Err(e) => panic!("unexpected fetch error: {e}"),
        }
    }
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for c in 1..=2 {
        let host = rt.node(&format!("c{c}"));
        tasks.push(rt.spawn(producer(host.clone(), format!("c{c}"), out.clone(), stop.clone())));
        tasks.push(rt.spawn(consumer(host, c - 1, out.clone(), stop.clone())));
    }
    tasks.push(rt.spawn(transactional(rt.node("t1"), out.clone(), stop.clone())));
    let n = w.cfgs.len() as u64;
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["c1".to_string(), "c2".to_string(), "t1".to_string()]);
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
        // At most a minority down at once: one of three, two of five.
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

    // Progress after healing, then the whole of each partition.
    let reader =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let out = std::mem::take(&mut *out.lock().unwrap());
    assert!(!out.acked.is_empty(), "no batch was acknowledged: the run checked nothing");
    // Every committed value, across partitions.
    let mut visible: BTreeMap<String, u32> = BTreeMap::new();
    for p in 0..PARTITIONS {
        let marker = format!("after-heal-{p}");
        let mut marker_at = None;
        for _ in 0..20 {
            match reader.produce(STREAM, p, batch(&marker, 1)).await {
                Ok(o) => {
                    marker_at = Some(o);
                    break;
                }
                Err(ClientError::Unknown(_)) => continue,
                Err(e) => panic!("partition {p} made no progress after healing: {e}"),
            }
        }
        let marker_at = marker_at.unwrap_or_else(|| panic!("partition {p}: no produce settled after healing"));
        let mut log: BTreeMap<u64, String> = BTreeMap::new();
        let mut offset = 0;
        // Leftover transactions are aborted by their timeout, so the last
        // stable offset catches up with the marker.
        let deadline = rt.now() + Duration::from_secs(30);
        while offset <= marker_at {
            assert!(rt.now() < deadline, "partition {p}: the last stable offset is stuck below {marker_at}");
            let f = reader.fetch(STREAM, p, offset, TICK * 10).await.expect("the healed cluster serves reads");
            offset = f.next_offset(offset);
            log.extend(committed(&f));
        }
        assert_eq!(log.get(&marker_at), Some(&marker), "partition {p}: the marker is where it was acknowledged");

        let mut seen = BTreeMap::new();
        for (o, v) in &log {
            if let Some(other) = seen.insert(v.clone(), *o) {
                panic!("partition {p}: {v} written twice, at {other} and {o}");
            }
        }
        for (o, v) in out.acked.get(&p).into_iter().flatten() {
            assert_eq!(log.get(o), Some(v), "partition {p}: acknowledged {v} at {o} is lost or moved");
        }
        for (o, v) in out.read.get(&p).into_iter().flatten() {
            assert_eq!(log.get(o), Some(v), "partition {p}: {v} was read at {o} but is not committed there");
        }
        for v in &out.not_written {
            assert!(!seen.contains_key(v), "partition {p}: {v} was reported as not written but is there");
        }
        for v in log.into_values() {
            assert!(visible.insert(v.clone(), p).is_none(), "{v} is visible in two partitions");
        }
    }
    for (values, end) in &out.txns {
        let shown = values.iter().filter(|v| visible.contains_key(*v)).count();
        match end {
            TxnEnd::Committed => assert_eq!(shown, values.len(), "committed {values:?}, but only {shown} visible"),
            TxnEnd::Aborted => assert_eq!(shown, 0, "aborted {values:?}, but {shown} visible"),
            TxnEnd::Unknown => assert!(shown == 0 || shown == values.len(), "{values:?} is half visible"),
        }
    }
    assert!(out.txns.iter().any(|(_, e)| *e == TxnEnd::Committed), "no transaction committed: checked nothing");
    drop(out);
    drop(reader);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn clients_see_only_committed_data_and_lose_no_acknowledged_batch() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

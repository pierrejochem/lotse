//! A consumer group processing a stream exactly once, against a faulty
//! cluster, checked over many seeds.
//!
//! Three or five nodes host a stream `in` with three partitions, a stream
//! `out`, and the coordinator group. A producer writes numbered messages to
//! `in`. Three processors share the consumer group `pipe`: each reads its
//! partitions, writes every message to `out` and commits its read offsets in
//! the same transaction (consume-transform-produce). Processors crash now
//! and then (leaving a zombie behind until the coordinator removes it) and
//! start again, or stall past their session while holding messages they read
//! (a zombie that wakes up and tries to commit them); pairs of hosts are
//! partitioned and nodes lose power. After
//! healing, the processors run until everything is through.
//!
//! Checked: every message acknowledged to the producer appears in `out`
//! exactly once, and nothing else appears twice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, GroupConfig};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine, StreamConfig};
use lotse_sim::{Sim, SimRuntime, check_few_seeds, check_seeds};

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
const PARTITIONS: u32 = 3;
const SESSION: Duration = Duration::from_millis(1_000);

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
        request_timeout: Duration::from_millis(400),
        retry_for: Duration::from_millis(1_200),
        backoff: Duration::from_millis(20),
        produce_timeout: Duration::from_millis(300),
        ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
    }
}

#[derive(Default)]
struct Outcomes {
    /// Input values whose produce was acknowledged.
    acked: Vec<String>,
    /// Transactions committed by processors.
    commits: u64,
    /// Transactions aborted, and of those, refused offsets (fenced members).
    aborts: u64,
    fenced: u64,
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
        let streams = vec![
            StreamConfig { name: "in".into(), resource_id: 7, partitions: vec![1, 2, 3] },
            StreamConfig { name: "out".into(), resource_id: 8, partitions: vec![4] },
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
                        // Groups 1-3 are `in`, 4 is `out`, 5 coordinates.
                        groups: vec![1, 2, 3, 4, 5],
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        zones: Default::default(),
                        streams: streams.clone(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: SESSION,
                        group_heartbeat_interval: TICK * 10,
                        metadata: None,
                        coordinator: Some(5),
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

async fn producer(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let client = Client::new(rt.clone(), client_config());
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let value = format!("i{seq}");
        seq += 1;
        let partition = rt.below(u64::from(PARTITIONS)) as u32;
        match client.produce("in", partition, message(&value)).await {
            Ok(_) => out.lock().unwrap().acked.push(value),
            Err(ClientError::GaveUp(..) | ClientError::Unknown(_)) => {}
            Err(e) => panic!("unexpected produce error: {e}"),
        }
        rt.sleep(TICK * (1 + rt.below(4) as u32)).await;
    }
}

/// Reads its partitions and writes each message to `out`, committing the
/// read offsets in the same transaction. Crashes (abandons everything) now
/// and then, unless `steady`, and starts over.
async fn processor(rt: SimRuntime, name: String, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>, steady: bool) {
    while !stop.load(Ordering::Relaxed) {
        let client = Client::new(
            rt.clone(),
            ClientConfig {
                transactional_id: Some(format!("tx-{name}")),
                // Longer than a session: a member can lose its partitions
                // while its transaction is still open.
                txn_timeout: Duration::from_millis(3_000),
                ..client_config()
            },
        );
        let cfg = GroupConfig {
            session_timeout: SESSION,
            rebalance_timeout: SESSION,
            auto_commit: false,
            ..GroupConfig::default()
        };
        let mut consumer = match client.join_group("pipe", &["in"], cfg).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected join error: {e}"),
        };
        let lifetime = if steady { u64::MAX } else { 20 + rt.below(200) };
        for _ in 0..lifetime {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let records = match consumer.poll(TICK * 10).await {
                Ok(r) => r,
                Err(ClientError::GaveUp(..)) => continue,
                Err(e) => panic!("unexpected poll error: {e}"),
            };
            if records.is_empty() {
                continue;
            }
            if !steady && rt.chance(1, 15) {
                // Stalls past its session (a long pause): the group gives
                // its partitions to others, who read the same messages and
                // have time to write them. Its offsets must be refused, or
                // they would be written twice.
                rt.sleep(SESSION * 2).await;
            }
            let mut txn = match client.begin_transaction().await {
                Ok(t) => t,
                Err(ClientError::GaveUp(..)) => {
                    consumer.rewind();
                    continue;
                }
                Err(e) => panic!("unexpected error beginning a transaction: {e}"),
            };
            let mut ok = true;
            for r in &records {
                let v = String::from_utf8(r.record.value.clone().unwrap().to_vec()).unwrap();
                if txn.produce("out", 0, message(&v)).await.is_err() {
                    ok = false;
                    break;
                }
            }
            if ok && let Err(e) = txn.send_offsets(&consumer).await {
                ok = false;
                if matches!(e, ClientError::Broker { .. }) {
                    out.lock().unwrap().fenced += 1;
                }
                if rt.chance(1, 2) {
                    // A careless processor commits anyway: refused.
                    match txn.commit().await {
                        Err(ClientError::MustAbort(_)) => {}
                        other => panic!("a transaction whose offsets were refused committed: {other:?}"),
                    }
                    out.lock().unwrap().aborts += 1;
                    consumer.rewind();
                    continue;
                }
            }
            if ok && !steady && rt.chance(1, 15) {
                // Stalls with its offsets in an open transaction: the group
                // gives the partitions away, and the new owner must wait for
                // this transaction to end before it reads the offsets.
                rt.sleep(SESSION + TICK * 20).await;
            }
            let committed = if ok {
                match txn.commit().await {
                    Ok(()) => true,
                    // Unknown: all of it or nothing, offsets included.
                    Err(_) => false,
                }
            } else {
                let _ = txn.abort().await;
                false
            };
            if committed {
                out.lock().unwrap().commits += 1;
            } else {
                out.lock().unwrap().aborts += 1;
                // Start again from what the group committed.
                consumer.rewind();
            }
        }
        if rt.chance(1, 2) {
            // Leaves cleanly; otherwise a zombie stays until its session ends.
            consumer.close().await;
        }
    }
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop_input = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let producer_task = rt.spawn(producer(rt.node("p1"), out.clone(), stop_input.clone()));
    let mut tasks = Vec::new();
    for k in 1..=3 {
        let name = format!("k{k}");
        tasks.push(rt.spawn(processor(rt.node(&name), name, out.clone(), stop.clone(), k == 1)));
    }
    let n = w.cfgs.len() as u64;
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["p1", "k1", "k2", "k3"].map(String::from));
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
    stop_input.store(true, Ordering::Relaxed);
    producer_task.await;
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }

    // The processors finish the input; read `out` until every acknowledged
    // input is there, or time runs out.
    let acked = out.lock().unwrap().acked.clone();
    assert!(!acked.is_empty(), "nothing was produced: the run checked nothing");
    let reader =
        Client::new(rt.node("checker"), ClientConfig { retry_for: Duration::from_secs(20), ..client_config() });
    let deadline = rt.now() + Duration::from_secs(60);
    let mut seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut offset = 0;
    loop {
        let f = reader.fetch("out", 0, offset, TICK * 20).await.expect("the healed cluster serves reads");
        offset = f.next_offset(offset);
        for b in f.committed_batches().unwrap() {
            for r in b.records().unwrap() {
                let v = String::from_utf8(r.value.unwrap().to_vec()).unwrap();
                *seen.entry(v).or_default() += 1;
            }
        }
        if let Some((v, n)) = seen.iter().find(|(_, n)| **n > 1) {
            panic!("{v} was written to out {n} times");
        }
        if acked.iter().all(|v| seen.contains_key(v)) {
            break;
        }
        let missing = acked.iter().filter(|v| !seen.contains_key(*v)).count();
        assert!(rt.now() < deadline, "{missing} acknowledged inputs never reached out");
    }
    assert!(out.lock().unwrap().commits > 0);
    if std::env::var_os("LOTSE_SIM_STATS").is_some() {
        let o = out.lock().unwrap();
        eprintln!("stats: acked {} commits {} aborts {} fenced {}", o.acked.len(), o.commits, o.aborts, o.fenced);
    }
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await;
    }
    drop(reader);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_consumer_group_processes_every_message_exactly_once() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

/// A group with no committed offsets starts where `start_from` says (ADR
/// 0059): at the end, only what comes after; at a time, from the first batch
/// with a record at or after it; at the start, everything.
#[test]
fn a_new_group_starts_where_it_is_told() {
    check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let _w = World::new(rt.clone()).await;
            let client = Client::new(rt.clone(), client_config());
            // Old messages at 1,000 ms and 2,000 ms after the epoch, by the
            // records' own timestamps, all into partition 0.
            for (value, ts) in [("old-a", 1_000), ("old-b", 2_000)] {
                let mut b = BatchBuilder::new(BatchOptions::default());
                b.push(ts, None, Some(value.as_bytes()), &[]);
                client.produce("in", 0, b.finish().unwrap()).await.expect("produced");
            }
            let read_all = async |group: &str, start_from, then: &[&str], want: usize| {
                let cfg = GroupConfig { start_from, ..GroupConfig::default() };
                let mut consumer = client.join_group(group, &["in"], cfg).await.expect("joined");
                // Own partition 0 and know where to start before more comes.
                let mut seen = Vec::new();
                for _ in 0..50 {
                    seen.extend(consumer.poll(TICK * 20).await.expect("polled").into_iter().map(value_of));
                    if consumer.positions().contains_key(&("in".to_string(), 0)) {
                        break;
                    }
                }
                for value in then {
                    let mut b = BatchBuilder::new(BatchOptions::default());
                    b.push(3_000, None, Some(value.as_bytes()), &[]);
                    client.produce("in", 0, b.finish().unwrap()).await.expect("produced");
                }
                for _ in 0..100 {
                    if seen.len() >= want {
                        break;
                    }
                    seen.extend(consumer.poll(TICK * 20).await.expect("polled").into_iter().map(value_of));
                }
                seen
            };
            let latest = read_all("from-latest", lotse_client::OffsetAt::Latest, &["new-1"], 1).await;
            assert_eq!(latest, ["new-1"], "from the end: only what came after");
            let timed = read_all("from-time", lotse_client::OffsetAt::Time(1_500), &["new-2"], 3).await;
            assert_eq!(timed, ["old-b", "new-1", "new-2"], "from 1,500 ms: old-b and later");
            let all = read_all("from-start", lotse_client::OffsetAt::Earliest, &[], 4).await;
            assert_eq!(all, ["old-a", "old-b", "new-1", "new-2"], "from the start: everything");
        })
    });
}

fn value_of(r: lotse_client::GroupRecord) -> String {
    String::from_utf8(r.record.value.expect("a value").to_vec()).unwrap()
}

/// A transaction that only sent group offsets still has to be ended: the
/// coordinator holds them as pending until then, and the next member of the
/// group waits for the end (or the transaction's timeout, far away here)
/// before it learns where to start.
#[test]
fn a_transaction_that_only_sent_offsets_commits_them() {
    check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let _w = World::new(rt.clone()).await;
            let producer = Client::new(rt.clone(), client_config());
            let processor = Client::new(
                rt.clone(),
                ClientConfig {
                    transactional_id: Some("tx-offsets".into()),
                    txn_timeout: Duration::from_secs(60),
                    ..client_config()
                },
            );
            for value in ["a", "b"] {
                producer.produce("in", 0, message(value)).await.expect("produced");
            }
            let cfg = GroupConfig { auto_commit: false, ..GroupConfig::default() };
            let mut first = processor.join_group("offsets-only", &["in"], cfg.clone()).await.expect("joined");
            let mut seen = Vec::new();
            for _ in 0..100 {
                seen.extend(first.poll(TICK * 20).await.expect("polled").into_iter().map(value_of));
                if seen.len() >= 2 {
                    break;
                }
            }
            assert_eq!(seen, ["a", "b"]);
            // No produce, no partition added: only the offsets.
            let mut txn = processor.begin_transaction().await.expect("began");
            txn.send_offsets(&first).await.expect("offsets sent");
            txn.commit().await.expect("committed");
            first.close().await;
            producer.produce("in", 0, message("c")).await.expect("produced");
            let mut second = producer.join_group("offsets-only", &["in"], cfg).await.expect("joined");
            let mut next = Vec::new();
            for _ in 0..250 {
                next.extend(second.poll(TICK * 20).await.expect("polled").into_iter().map(value_of));
                if !next.is_empty() {
                    break;
                }
            }
            assert_eq!(next, ["c"], "the committed offsets are the group's: only what came after");
        })
    });
}

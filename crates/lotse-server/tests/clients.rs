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
//!
//! In half the runs a partition admits only a few producers and forgets one
//! after 3 s without a write (ADR 0110), while short-lived producers come
//! and go: they wait for a place or give up, and the live producers keep
//! theirs, so their retries are still recognised.
//!
//! In the half of the runs where the nodes sit in two zones, a metadata
//! group joins, and clients in a zone read admin state and committed offsets
//! from a node there (the zone reads design, section 2). Checked as well:
//! - every `DescribeConfigs` and `DescribeNodes` answer an admin gets shows
//!   every change it was told succeeded before it asked;
//! - a group member that takes a partition over never starts below the last
//!   offset whose commit was acknowledged before.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, GroupConfig, OffsetAt};
use lotse_log::LogConfig;
use lotse_proto::ApiKey;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Net, Rng, Spawn};
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine, StreamConfig};
use lotse_sim::{Sim, SimRuntime, check_few_seeds, check_seeds};

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
/// A group member's session in the zone runs: short, so partitions change
/// owners within a run.
const SESSION: Duration = Duration::from_millis(1_000);

fn batch(value: &str, records: u64) -> Bytes {
    // Batches come plain, zstd and lz4 (ADR 0058): replicas, snapshots,
    // retention and readers handle them all alike.
    let mut options = BatchOptions::default();
    options.attributes.compression =
        [lotse_record::Compression::None, lotse_record::Compression::Zstd, lotse_record::Compression::Lz4]
            [value.len() % 3];
    let mut b = BatchBuilder::new(options);
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

/// How the partitions keep producers: how many, and for how long idle.
#[derive(Clone, Copy)]
struct Producers {
    max: usize,
    expiration: Duration,
}

impl Producers {
    const DEFAULT: Producers =
        Producers { max: lotse_server::DEFAULT_MAX_PRODUCERS, expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION };
}

/// How a world differs from the plain one.
#[derive(Clone, Copy)]
struct Setup {
    /// The metadata group, if any: clients can then create streams and
    /// change settings, and group sessions are short.
    metadata: Option<u64>,
    /// The group that coordinates producers and consumer groups.
    coordinator: u64,
    /// Pre-vote on or off; `None`: as the seed decides.
    pre_vote: Option<bool>,
    /// The nodes' Raft tick; an election timeout is ten.
    tick: Duration,
}

impl Setup {
    /// The stream's partitions on groups 1 and 2, the coordinator on 3.
    const DEFAULT: Setup = Setup { metadata: None, coordinator: 3, pre_vote: None, tick: TICK };
    /// The metadata group 1 and the coordinator 2, as a cluster file has
    /// them, which a client's read indexes start from (the zone reads
    /// design, section 2); the stream's partitions on 3 and 4.
    const PRODUCTION: Setup = Setup { metadata: Some(1), coordinator: 2, pre_vote: None, tick: TICK };

    /// Every group, and of them the stream's two partitions: the first two
    /// that are neither the metadata group nor the coordinator.
    fn groups(&self) -> (Vec<u64>, Vec<u64>) {
        let core = [self.metadata, Some(self.coordinator)];
        let partitions: Vec<u64> = (1..).filter(|g| !core.contains(&Some(*g))).take(2).collect();
        let mut all: Vec<u64> = core.into_iter().flatten().chain(partitions.iter().copied()).collect();
        all.sort();
        (all, partitions)
    }
}

impl World {
    async fn new(rt: SimRuntime, producers: Producers) -> World {
        let n = if rt.chance(1, 2) { 3 } else { 5 };
        // In half the runs nodes sit in two zones, and consumers in a zone
        // fetch from a follower there (ADR 0104).
        let zones: BTreeMap<NodeId, String> = if rt.chance(1, 2) {
            (1..=n).map(|id| (id, if id % 2 == 0 { "a" } else { "b" }.to_string())).collect()
        } else {
            BTreeMap::new()
        };
        let zoned = !zones.is_empty();
        World::build(rt, producers, n, zones, if zoned { Setup::PRODUCTION } else { Setup::DEFAULT }).await
    }

    async fn build(
        rt: SimRuntime,
        producers: Producers,
        n: NodeId,
        zones: BTreeMap<NodeId, String>,
        setup: Setup,
    ) -> World {
        let (groups, partitions) = setup.groups();
        let Setup { metadata, coordinator, pre_vote, tick } = setup;
        let ids: Vec<NodeId> = (1..=n).collect();
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let pre_vote = pre_vote.unwrap_or_else(|| rt.chance(1, 2));
        let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
        let streams = vec![StreamConfig { name: STREAM.into(), resource_id: 7, partitions }];
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
                        tick,
                        pre_vote,
                        log,
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        zones: zones.clone(),
                        streams: streams.clone(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: if metadata.is_some() {
                            SESSION
                        } else {
                            lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT
                        },
                        group_heartbeat_interval: if metadata.is_some() {
                            TICK * 10
                        } else {
                            lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL
                        },
                        metadata,
                        coordinator: Some(coordinator),
                        max_producers: producers.max,
                        producer_expiration: producers.expiration,
                        leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
                        follower_max_lag: lotse_server::DEFAULT_FOLLOWER_MAX_LAG,
                        disk_free_limit: lotse_server::FreeLimit::default(),
                        disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
                        versions: Default::default(),
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
    let zone = [None, Some("a"), Some("b")][rt.below(3) as usize].map(String::from);
    let client = Client::new(rt.clone(), ClientConfig { zone, ..client_config() });
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

/// Short-lived idempotent producers: each a new client with a new producer
/// id that writes one batch and goes. Where few producers are admitted
/// they wait for a place, or give up: definitely not written.
async fn churn(rt: SimRuntime, out: Arc<Mutex<Outcomes>>, stop: Arc<AtomicBool>) {
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let client = Client::new(rt.clone(), client_config());
        let partition = rt.below(u64::from(PARTITIONS)) as u32;
        let value = format!("s-{seq}");
        seq += 1;
        let res = client.produce(STREAM, partition, batch(&value, 1)).await;
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
        rt.sleep(TICK * (5 + rt.below(20) as u32)).await;
    }
}

/// What the zone runs judged, so a green run can be seen to have checked
/// something.
#[derive(Debug, Default)]
struct ZoneChecks {
    /// `DescribeConfigs` answers that had a setting to show.
    configs: u64,
    /// `DescribeNodes` answers after at least one stream was created.
    nodes: u64,
    /// Partitions a member took over, and of those, ones with a commit
    /// acknowledged before.
    takeovers: u64,
    after_commit: u64,
    /// The admin's reads: frames sent and calls made. A call sends one, or
    /// two when the node in its zone refused and the leader was asked.
    read_frames: u64,
    read_calls: u64,
    /// `OffsetFetch` frames the members sent.
    offset_fetches: u64,
}

/// How many requests of these kinds a tapped client sent.
fn count(sent: &Sent, kinds: &[ApiKey]) -> u64 {
    sent.lock().unwrap().iter().filter(|(_, k)| kinds.contains(k)).count() as u64
}

/// What the admin knows of a stream it created.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Setting {
    /// Created; `retention.ms` never changed.
    Created,
    /// The `retention.ms` it was last told was set.
    Set(String),
    /// A change whose outcome it does not know: not judged any more.
    Unknown,
}

/// An admin in a zone: creates streams and changes their `retention.ms`
/// while faults run, and reads both back from a node in its zone. The
/// oracle is this task's own record of what it was told succeeded.
async fn admin(rt: SimRuntime, nodes: u64, checks: Arc<Mutex<ZoneChecks>>, stop: Arc<AtomicBool>) {
    let zone = ["a", "b"][rt.below(2) as usize].to_string();
    let sent: Sent = Arc::default();
    let client = Client::new(
        Tapped { rt: rt.clone(), sent: sent.clone() },
        ClientConfig { zone: Some(zone), ..client_config() },
    );
    let mut streams: BTreeMap<String, Setting> = BTreeMap::new();
    // Streams created and acknowledged: one partition of three replicas each.
    let mut created = 0u64;
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        let pick = |streams: &BTreeMap<String, Setting>, rt: &SimRuntime, want: &dyn Fn(&Setting) -> bool| {
            let names: Vec<String> = streams.iter().filter(|(_, s)| want(s)).map(|(n, _)| n.clone()).collect();
            (!names.is_empty()).then(|| names[rt.below(names.len() as u64) as usize].clone())
        };
        match rt.below(4) {
            0 => {
                let name = format!("adm-{seq}");
                // An unknown outcome may still have created it: it is not
                // judged, and the node count below is only a lower bound.
                if client.create_stream(&name, 1, 3).await.is_ok() {
                    streams.insert(name, Setting::Created);
                    created += 1;
                }
            }
            1 => {
                let Some(name) = pick(&streams, &rt, &|s| *s != Setting::Unknown) else { continue };
                let value = (60_000 + seq).to_string();
                let after = match client.alter_configs(&name, &[("retention.ms", &value)], &[]).await {
                    Ok(()) => Setting::Set(value),
                    Err(_) => Setting::Unknown,
                };
                streams.insert(name, after);
            }
            2 => {
                let Some(name) = pick(&streams, &rt, &|s| matches!(s, Setting::Set(_))) else { continue };
                let Setting::Set(want) = streams[&name].clone() else { unreachable!() };
                // No answer (faults, or `Metadata` that has not shown the
                // stream yet) says nothing; an answer must be this new.
                let answer = client.describe_configs(&name).await;
                checks.lock().unwrap().read_calls += 1;
                if let Ok(configs) = answer {
                    let shown = configs.iter().find(|c| c.key == "retention.ms").map(|c| c.value.as_str());
                    assert_eq!(
                        shown,
                        Some(want.as_str()),
                        "{name}: DescribeConfigs misses a change it was told succeeded"
                    );
                    checks.lock().unwrap().configs += 1;
                }
            }
            _ => {
                let answer = client.describe_nodes().await;
                checks.lock().unwrap().read_calls += 1;
                if let Ok(entries) = answer {
                    // The metadata and coordinator groups have every node as
                    // a voter; each stream created adds three replicas.
                    let replicas: u64 = entries.iter().map(|e| u64::from(e.replicas)).sum();
                    assert!(
                        replicas >= 2 * nodes + 3 * created,
                        "DescribeNodes counts {replicas} replicas, fewer than the {created} streams created need"
                    );
                    if created > 0 {
                        checks.lock().unwrap().nodes += 1;
                    }
                }
            }
        }
        rt.sleep(TICK * (1 + rt.below(5) as u32)).await;
    }
    checks.lock().unwrap().read_frames += count(&sent, &[ApiKey::DESCRIBE_CONFIGS, ApiKey::DESCRIBE_NODES]);
}

/// A member of group `g` in a zone: reads `orders`, commits now and then,
/// and leaves cleanly or not at all, so partitions change owners while
/// faults run. `floors` holds, per partition, the highest offset whose
/// commit was acknowledged to any member: written here, never read from the
/// client.
async fn member(
    rt: SimRuntime,
    floors: Arc<Mutex<BTreeMap<u32, u64>>>,
    checks: Arc<Mutex<ZoneChecks>>,
    stop: Arc<AtomicBool>,
) {
    while !stop.load(Ordering::Relaxed) {
        let zone = ["a", "b"][rt.below(2) as usize].to_string();
        let sent: Sent = Arc::default();
        let client = Client::new(
            Tapped { rt: rt.clone(), sent: sent.clone() },
            ClientConfig { zone: Some(zone), ..client_config() },
        );
        let cfg = GroupConfig {
            session_timeout: SESSION,
            rebalance_timeout: SESSION,
            auto_commit: rt.chance(1, 2),
            ..GroupConfig::default()
        };
        let mut consumer = match client.join_group("g", &[STREAM], cfg).await {
            Ok(c) => c,
            Err(ClientError::GaveUp(..)) => continue,
            Err(e) => panic!("unexpected join error: {e}"),
        };
        // Partitions whose first position was judged, while still owned.
        let mut judged: BTreeSet<u32> = BTreeSet::new();
        for _ in 0..10 + rt.below(60) {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            // Commits acknowledged before the poll were written before the
            // partition was handed to this member: no other member may
            // commit it once it moved, and a member drops what it owns
            // before its session could end at the coordinator.
            let before = floors.lock().unwrap().clone();
            let records = match consumer.poll(TICK * 10).await {
                Ok(r) => r,
                Err(ClientError::GaveUp(..)) => continue,
                Err(e) => panic!("unexpected poll error: {e}"),
            };
            let owned: BTreeSet<u32> = consumer.assignment().into_iter().map(|(_, p)| p).collect();
            judged.retain(|p| owned.contains(p));
            for ((_, p), position) in consumer.positions() {
                if !judged.insert(p) {
                    continue;
                }
                // The first offset read, or where it will read from.
                let first = records.iter().find(|r| r.partition == p).map_or(position, |r| r.record.offset);
                let floor = before.get(&p).copied().unwrap_or(0);
                assert!(
                    first >= floor,
                    "partition {p}: a member took it over at {first}, below the commit acknowledged at {floor}"
                );
                let mut c = checks.lock().unwrap();
                c.takeovers += 1;
                c.after_commit += u64::from(floor > 0);
            }
            if rt.chance(1, 4) {
                let positions = consumer.positions();
                if consumer.commit().await.is_ok() {
                    let mut f = floors.lock().unwrap();
                    for ((_, p), o) in positions {
                        let e = f.entry(p).or_default();
                        *e = (*e).max(o);
                    }
                }
            }
        }
        if rt.chance(1, 2) {
            // Leaves cleanly; otherwise a zombie stays until its session ends.
            consumer.close().await;
        }
        checks.lock().unwrap().offset_fetches += count(&sent, &[ApiKey::OFFSET_FETCH]);
    }
}

async fn run(rt: SimRuntime) {
    // Longer idle than any retry of the live producers takes (`retry_for`
    // plus a request), so they are never forgotten while they retry.
    let few = rt.chance(1, 2);
    let producers = if few { Producers { max: 4, expiration: Duration::from_secs(3) } } else { Producers::DEFAULT };
    let mut w = World::new(rt.clone(), producers).await;
    let out = Arc::new(Mutex::new(Outcomes::default()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = Vec::new();
    for c in 1..=2 {
        let host = rt.node(&format!("c{c}"));
        tasks.push(rt.spawn(producer(host.clone(), format!("c{c}"), out.clone(), stop.clone())));
        tasks.push(rt.spawn(consumer(host, c - 1, out.clone(), stop.clone())));
    }
    tasks.push(rt.spawn(transactional(rt.node("t1"), out.clone(), stop.clone())));
    if few {
        tasks.push(rt.spawn(churn(rt.node("s1"), out.clone(), stop.clone())));
    }
    let n = w.cfgs.len() as u64;
    let mut hosts: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
    hosts.extend(["c1".to_string(), "c2".to_string(), "t1".to_string(), "s1".to_string()]);
    let zoned = !w.cfgs[&1].zones.is_empty();
    let checks = Arc::new(Mutex::new(ZoneChecks::default()));
    if zoned {
        tasks.push(rt.spawn(admin(rt.node("a1"), n, checks.clone(), stop.clone())));
        let floors = Arc::new(Mutex::new(BTreeMap::new()));
        for name in ["g1", "g2", "g3"] {
            tasks.push(rt.spawn(member(rt.node(name), floors.clone(), checks.clone(), stop.clone())));
        }
        hosts.extend(["a1", "g1", "g2", "g3"].map(String::from));
    }
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
    if zoned && std::env::var_os("LOTSE_SIM_STATS").is_some() {
        eprintln!("zone checks: {:?}", checks.lock().unwrap());
    }
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

/// A partition that admits two producers holds a third back until one of
/// the first two has been idle for the expiration, then takes it: the
/// leader forgets through the log, and the client waits on
/// `PRODUCER_LIMIT` (ADR 0110).
async fn wait_for_a_place(rt: SimRuntime) {
    let expiration = Duration::from_secs(1);
    let mut w = World::new(rt.clone(), Producers { max: 2, expiration }).await;
    for name in ["a", "b"] {
        let client = Client::new(rt.node(name), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
        client.produce(STREAM, 0, batch(name, 1)).await.unwrap_or_else(|e| panic!("{name} found no place: {e}"));
    }
    let started = rt.now();
    let c = Client::new(rt.node("c"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
    c.produce(STREAM, 0, batch("c", 1)).await.expect("a place once an idle producer is forgotten");
    let waited = rt.now() - started;
    assert!(waited >= expiration / 2, "the third producer got in after {waited:?}, before anyone was idle long");
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_new_producer_waits_for_an_idle_one_to_be_forgotten() {
    check_few_seeds(3, |seed| Sim::new(seed).run(wait_for_a_place));
}

/// A follower cut off from its leader stops serving reads once it no longer
/// hears it (ADR 0116): a reader in its zone then asks the leader, and finds
/// what was written after the cut, through `ListOffsets` and `Fetch` alike.
async fn leave_a_cut_off_follower(rt: SimRuntime) {
    let zones: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("z{id}"))).collect();
    let mut w = World::build(rt.clone(), Producers::DEFAULT, 3, zones, Setup::DEFAULT).await;
    let writer = Client::new(rt.node("writer"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
    let first = writer.produce(STREAM, 1, batch("before", 1)).await.expect("written");
    let (leader, voters) = loop {
        let m = writer.metadata(&[STREAM]).await.expect("metadata");
        let p = &m.resources[0].partitions[1];
        if p.leader != 0 && p.voters.len() == 3 {
            break (p.leader, p.voters.clone());
        }
        rt.sleep(TICK * 10).await;
    };
    let follower = voters.into_iter().find(|v| *v != leader).expect("a follower");
    // Two readers in the follower's zone, so that each kind of read finds
    // its own way to the leader.
    let in_zone = |name: &str| {
        Client::new(
            rt.node(name),
            ClientConfig { zone: Some(format!("z{follower}")), retry_for: Duration::from_secs(10), ..client_config() },
        )
    };
    let (lister, fetcher) = (in_zone("lister"), in_zone("fetcher"));
    // The follower serves while it keeps up.
    let deadline = rt.now() + Duration::from_secs(5);
    while lister.list_offset(STREAM, 1, OffsetAt::Latest).await.expect("an end") <= first {
        assert!(rt.now() < deadline, "the follower never caught up");
        rt.sleep(TICK * 5).await;
    }
    let f = fetcher.fetch(STREAM, 1, first, TICK * 20).await.expect("a fetch");
    assert_eq!(committed(&f).first().map(|(o, _)| *o), Some(first), "the follower serves what it has");
    for other in (1..=3u64).filter(|n| *n != u64::from(follower)) {
        rt.net().block(&format!("n{follower}"), &format!("n{other}"));
    }
    let after = writer.produce(STREAM, 1, batch("after", 1)).await.expect("written");
    let cut = rt.now();
    let fetched = loop {
        let f = fetcher.fetch(STREAM, 1, after, TICK * 20).await.expect("a fetch");
        if !f.records.is_empty() {
            break f;
        }
        assert!(rt.now() - cut < Duration::from_secs(3), "the reader still fetches from a follower that is cut off");
    };
    assert_eq!(committed(&fetched).first().map(|(o, _)| *o), Some(after));
    loop {
        let end = lister.list_offset(STREAM, 1, OffsetAt::Latest).await.expect("an end");
        if end > after {
            break;
        }
        assert!(rt.now() - cut < Duration::from_secs(3), "the reader still asks a follower that is cut off: end {end}");
        rt.sleep(TICK * 5).await;
    }
    rt.net().heal();
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_reader_leaves_a_follower_that_is_cut_off() {
    check_few_seeds(3, |seed| Sim::new(seed).run(leave_a_cut_off_follower));
}

/// Requests a client sent, as `(host, what)`, in order: seen where its
/// frames leave for the simulated network.
type Sent = Arc<Mutex<Vec<(String, ApiKey)>>>;

/// A client's runtime that records every request it sends, so a test counts
/// where a client's requests went without help from the client.
#[derive(Clone)]
struct Tapped {
    rt: SimRuntime,
    sent: Sent,
}

impl Clock for Tapped {
    fn now(&self) -> lotse_rt::Instant {
        self.rt.now()
    }
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.rt.sleep(duration)
    }
    fn unix_time(&self) -> Duration {
        self.rt.unix_time()
    }
}

impl Spawn for Tapped {
    type JoinHandle<T: Send + 'static> = <SimRuntime as Spawn>::JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.rt.spawn(future)
    }
}

impl Rng for Tapped {
    fn next_u64(&self) -> u64 {
        self.rt.next_u64()
    }
    fn fill_secret(&self, buf: &mut [u8]) {
        self.rt.fill_secret(buf)
    }
}

struct TappedConn {
    conn: <SimRuntime as Net>::Conn,
    host: String,
    sent: Sent,
}

impl lotse_rt::Conn for TappedConn {
    fn send(&self, frame: Bytes) -> impl Future<Output = std::io::Result<()>> + Send {
        if let Ok((header, _)) = lotse_proto::decode_request(frame.clone()) {
            self.sent.lock().unwrap().push((self.host.clone(), header.api_key));
        }
        self.conn.send(frame)
    }
    fn recv(&self) -> impl Future<Output = std::io::Result<Option<Bytes>>> + Send {
        self.conn.recv()
    }
    fn peer_identity(&self) -> Option<lotse_rt::PeerIdentity> {
        self.conn.peer_identity()
    }
}

/// A client never listens; the type is only there to satisfy [`Net`].
struct TappedListener(<SimRuntime as Net>::Listener, Sent);

impl lotse_rt::Listener for TappedListener {
    type Conn = TappedConn;
    fn local_addr(&self) -> String {
        lotse_rt::Listener::local_addr(&self.0)
    }
    async fn accept(&self) -> std::io::Result<(TappedConn, String)> {
        let (conn, peer) = lotse_rt::Listener::accept(&self.0).await?;
        Ok((TappedConn { conn, host: peer.clone(), sent: self.1.clone() }, peer))
    }
}

impl Net for Tapped {
    type Conn = TappedConn;
    type Listener = TappedListener;

    async fn listen(&self, addr: &str, link: lotse_rt::Link) -> std::io::Result<TappedListener> {
        Ok(TappedListener(self.rt.listen(addr, link).await?, self.sent.clone()))
    }
    async fn connect(&self, addr: &str, link: lotse_rt::Link) -> std::io::Result<TappedConn> {
        let conn = self.rt.connect(addr, link).await?;
        let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h).to_string();
        Ok(TappedConn { conn, host, sent: self.sent.clone() })
    }
    fn verifies_peers(&self) -> bool {
        self.rt.verifies_peers()
    }
}

/// A reader that fell back to the leader while its zone's follower was cut
/// off goes back to that follower once the cut heals: the leader names it
/// as soon as it is current again (the zone reads design, section 1), with
/// no `Metadata` request in between.
async fn return_to_the_zone(rt: SimRuntime) {
    let zones: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("z{id}"))).collect();
    // With pre-vote, the follower coming back does not depose the leader
    // with the terms it raised while cut off: the leader stays, and the
    // reader has no reason of its own to ask for metadata.
    let setup = Setup { pre_vote: Some(true), ..Setup::DEFAULT };
    let mut w = World::build(rt.clone(), Producers::DEFAULT, 3, zones, setup).await;
    let writer = Client::new(rt.node("writer"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
    let first = writer.produce(STREAM, 1, batch("before", 1)).await.expect("written");
    let (leader, voters) = loop {
        let m = writer.metadata(&[STREAM]).await.expect("metadata");
        let p = &m.resources[0].partitions[1];
        if p.leader != 0 && p.voters.len() == 3 {
            break (p.leader, p.voters.clone());
        }
        rt.sleep(TICK * 10).await;
    };
    let follower = voters.into_iter().find(|v| *v != leader).expect("a follower");
    let (leader_host, follower_host) = (format!("n{leader}"), format!("n{follower}"));
    let sent: Sent = Arc::default();
    let reader = Client::new(
        Tapped { rt: rt.node("reader"), sent: sent.clone() },
        ClientConfig { zone: Some(format!("z{follower}")), retry_for: Duration::from_secs(10), ..client_config() },
    );
    let fetches_to = |host: &str, since: usize| {
        sent.lock().unwrap()[since..].iter().filter(|(h, k)| h == host && *k == ApiKey::FETCH).count()
    };
    // The reader starts on the follower, which serves while it keeps up.
    let deadline = rt.now() + Duration::from_secs(5);
    loop {
        let f = reader.fetch(STREAM, 1, first, TICK * 20).await.expect("a fetch");
        if !f.records.is_empty() {
            break;
        }
        assert!(rt.now() < deadline, "the follower never served what was written");
    }
    assert!(fetches_to(&follower_host, 0) > 0, "the reader never fetched from its zone's follower");

    // Cut off, the follower stops serving, and the reader goes to the leader.
    for other in (1..=3u64).filter(|n| *n != u64::from(follower)) {
        rt.net().block(&follower_host, &format!("n{other}"));
    }
    let cut_at = sent.lock().unwrap().len();
    let after = writer.produce(STREAM, 1, batch("after", 1)).await.expect("written");
    let cut = rt.now();
    let mut next = loop {
        let f = reader.fetch(STREAM, 1, after, TICK * 20).await.expect("a fetch");
        if !f.records.is_empty() {
            break f.next_offset(after);
        }
        assert!(rt.now() - cut < Duration::from_secs(3), "the reader still fetches from a follower that is cut off");
    };
    assert!(
        fetches_to(&leader_host, cut_at) > 0,
        "the reader read what was written after the cut, not from the leader"
    );

    // Healed, the follower catches up, the leader names it, and the reader
    // fetches there again, without asking for metadata.
    rt.net().heal();
    let healed = rt.now();
    let healed_at = sent.lock().unwrap().len();
    while fetches_to(&follower_host, healed_at) == 0 {
        assert!(
            rt.now() - healed < Duration::from_secs(3),
            "3 s after the cut healed the reader still fetches from the leader"
        );
        let f = reader.fetch(STREAM, 1, next, TICK * 20).await.expect("a fetch");
        next = f.next_offset(next);
    }
    let metadata = sent.lock().unwrap()[healed_at..].iter().filter(|(_, k)| *k == ApiKey::METADATA).count();
    assert_eq!(metadata, 0, "the reader asked for metadata to find its way back");
    drop(reader);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_reader_returns_to_its_zone_after_the_follower_heals() {
    check_few_seeds(3, |seed| Sim::new(seed).run(return_to_the_zone));
}

/// A member that takes a partition over reads its predecessor's commit even
/// from a coordinator follower in its zone that has not applied it yet: the
/// heartbeat that hands it the partition names an index at or above the
/// commit, so the follower refuses and the leader answers (the zone reads
/// design, section 2). The follower is cut off just before the commit and
/// still counts as current for an election timeout (ADR 0116), which is when
/// the new member asks. Random faults rarely line this up, so it is set up
/// here.
async fn take_over_from_a_follower_behind(rt: SimRuntime) {
    let zones: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("z{id}"))).collect();
    // A long tick gives the cut-off follower an election timeout of 1 s in
    // which it still counts as current, time enough for the takeover.
    // The coordinator is group 2, as a cluster file has it, which a client
    // names in its reads from the start.
    let setup = Setup { coordinator: 2, pre_vote: Some(true), tick: TICK * 10, ..Setup::DEFAULT };
    let mut w = World::build(rt.clone(), Producers::DEFAULT, 3, zones, setup).await;
    let writer = Client::new(rt.node("writer"), ClientConfig { retry_for: Duration::from_secs(10), ..client_config() });
    for p in 0..PARTITIONS {
        for i in 0..3 {
            writer.produce(STREAM, p, batch(&format!("m{p}-{i}"), 1)).await.expect("written");
        }
    }
    // The coordinator group with a leader every node agrees on.
    let status = |id: NodeId| w.running[&id].as_ref().and_then(|(_, h)| h.status(setup.coordinator));
    let (leader, follower) = loop {
        let leaders: BTreeSet<Option<NodeId>> = (1..=3).map(|id| status(id).and_then(|s| s.leader)).collect();
        if let [Some(l)] = leaders.into_iter().collect::<Vec<_>>()[..] {
            break (l, (1..=3).find(|id| *id != l).expect("a follower"));
        }
        rt.sleep(TICK * 5).await;
    };
    let follower_host = format!("n{follower}");

    // The first member reads everything there is.
    let first = Client::new(rt.node("first"), client_config());
    let cfg = || GroupConfig { auto_commit: false, ..GroupConfig::default() };
    let mut a = first.join_group("g", &[STREAM], cfg()).await.expect("joined");
    let deadline = rt.now() + Duration::from_secs(5);
    while a.positions().len() < PARTITIONS as usize || a.positions().values().any(|o| *o < 3) {
        assert!(rt.now() < deadline, "the first member never read both partitions: {:?}", a.positions());
        a.poll(TICK * 5).await.expect("polled");
    }
    // The second, in the follower's zone, knows the cluster already.
    let sent: Sent = Arc::default();
    let second = Client::new(
        Tapped { rt: rt.node("second"), sent: sent.clone() },
        ClientConfig { zone: Some(format!("z{follower}")), ..client_config() },
    );
    second.metadata(&[STREAM]).await.expect("metadata");

    // The follower hears nothing more, and the commit passes it by.
    for other in (1..=3u64).filter(|n| *n != follower) {
        rt.net().block(&follower_host, &format!("n{other}"));
    }
    let cut = rt.now();
    let committed = a.positions();
    a.commit().await.expect("committed");
    a.close().await;
    let mut b = second.join_group("g", &[STREAM], cfg()).await.expect("joined");
    // Where it starts: the first record it reads of a partition, or its
    // position while it has read none there (a poll reads right after it
    // learns where to start).
    let mut starts: BTreeMap<(String, u32), u64> = BTreeMap::new();
    while starts.len() < PARTITIONS as usize {
        assert!(rt.now() - cut < Duration::from_secs(5), "the second member never learned where to start");
        let records = b.poll(TICK * 5).await.expect("polled");
        for (part, position) in b.positions() {
            let first = records.iter().find(|r| r.partition == part.1).map_or(position, |r| r.record.offset);
            starts.entry(part).or_insert(first);
        }
    }
    let leader_applied = status(leader).expect("the leader runs").applied;
    let behind = status(follower).expect("the follower runs").applied < leader_applied;
    let asked = sent.lock().unwrap().iter().any(|(h, k)| *h == follower_host && *k == ApiKey::OFFSET_FETCH);
    assert!(behind && asked, "the follower in the zone was not asked while behind: the test checked nothing");
    for (part, at) in &committed {
        assert!(
            starts[part] >= *at,
            "{part:?}: the second member starts at {}, below the commit at {at}",
            starts[part]
        );
    }
    rt.net().heal();
    drop(b);
    for (node, handle) in std::mem::take(&mut w.running).into_values().flatten() {
        handle.stop();
        let _ = node.join().await;
    }
}

#[test]
fn a_member_taking_over_reads_its_predecessors_commit_past_a_follower_behind() {
    check_few_seeds(3, |seed| Sim::new(seed).run(take_over_from_a_follower_behind));
}

//! Whole nodes on a simulated network, checked over many seeds.
//!
//! Three nodes, each with its own disk and host name, run three Raft groups
//! over the simulated network: real `Node`s with their main loop, senders,
//! readers and the wire format. Clients propose through `NodeHandle`s. The
//! network partitions pairs of hosts; nodes lose power (sometimes in the
//! middle of a write) and restart from their disk.
//!
//! Checked: in every group, all nodes apply the same batches in the same
//! order, across restarts; after healing, a new batch commits in every group
//! and reaches every node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_log::LogConfig;
use lotse_raft::{NodeId, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng};
use lotse_server::wire::GroupId;
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_seeds};

/// Records applied values into a list the test can watch.
#[derive(Debug, Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Bytes>>>);

impl StateMachine for Recorder {
    fn apply(&mut self, batch: &Batch) {
        let v = batch.records().expect("valid")[0].value.clone().expect("a value");
        self.0.lock().unwrap().push(v);
    }
    fn snapshot(&self) -> Bytes {
        let mut b = BytesMut::new();
        for v in self.0.lock().unwrap().iter() {
            b.put_u32_le(v.len() as u32);
            b.put_slice(v);
        }
        b.freeze()
    }
    fn restore(&mut self, mut data: &[u8]) -> Result<(), String> {
        let mut list = self.0.lock().unwrap();
        list.clear();
        while data.has_remaining() {
            let n = data.get_u32_le() as usize;
            list.push(Bytes::copy_from_slice(&data[..n]));
            data.advance(n);
        }
        Ok(())
    }
}

const GROUPS: [GroupId; 3] = [1, 2, 3];
const TICK: Duration = Duration::from_millis(10);

struct Running {
    node: Node<SimRuntime>,
    handle: NodeHandle,
    recorders: BTreeMap<GroupId, Recorder>,
}

struct World {
    rt: SimRuntime,
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<Running>>,
    down_until: BTreeMap<NodeId, lotse_rt::Instant>,
    /// Per group, every applied value in order, as first seen.
    applied: BTreeMap<GroupId, Vec<Bytes>>,
    leaders: BTreeMap<(GroupId, u64), NodeId>,
}

/// One node's settings in the simulation.
fn node_config(
    id: NodeId,
    peers: &BTreeMap<NodeId, String>,
    pre_vote: bool,
    log: LogConfig,
    election: Duration,
    heartbeat: Duration,
) -> NodeConfig {
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers: peers.clone(),
        groups: GROUPS.to_vec(),
        tick: TICK,
        election_timeout: election,
        heartbeat_interval: heartbeat,
        pre_vote,
        log,
        client_listen: None,
        client_addrs: BTreeMap::new(),
        zones: Default::default(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: None,
        coordinator: None,
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
        leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
        follower_max_lag: lotse_server::DEFAULT_FOLLOWER_MAX_LAG,
        disk_free_limit: lotse_server::FreeLimit::default(),
        disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
        versions: Default::default(),
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth: lotse_server::AuthConfig::none(),
    }
}

fn batch(value: &[u8], records: u64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value), &[]);
    }
    b.finish().unwrap()
}

impl World {
    async fn new(rt: SimRuntime) -> World {
        // Tiny segments, so sealing, indexes and snapshots happen all the time.
        let log = LogConfig { segment_bytes: 300 + rt.below(3_000), index_interval_bytes: 64 };
        World::with_log(rt, log).await
    }

    async fn with_log(rt: SimRuntime, log: LogConfig) -> World {
        // The timing the simulation was tuned for: elections after 10 ticks.
        World::with_timing(rt, log, TICK * 10, TICK * 2).await
    }

    async fn with_timing(rt: SimRuntime, log: LogConfig, election: Duration, heartbeat: Duration) -> World {
        let ids: Vec<NodeId> = vec![1, 2, 3];
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let pre_vote = rt.chance(1, 2);
        let cfgs = ids.iter().map(|&id| (id, node_config(id, &peers, pre_vote, log, election, heartbeat))).collect();
        let mut w = World {
            // Half the seeds run as with TLS: every node proves its id.
            hosts: {
                let verify = rt.chance(1, 2);
                rt.net().verify_peers(verify);
                ids.iter()
                    .map(|&id| {
                        let host = rt.node(&format!("n{id}"));
                        (id, if verify { host.with_identity(Some(lotse_rt::PeerIdentity::Node(id))) } else { host })
                    })
                    .collect()
            },
            cfgs,
            running: BTreeMap::new(),
            down_until: BTreeMap::new(),
            applied: GROUPS.iter().map(|g| (*g, Vec::new())).collect(),
            leaders: BTreeMap::new(),
            rt,
        };
        for id in ids {
            w.start(id).await;
        }
        w
    }

    async fn start(&mut self, id: NodeId) {
        let recorders: BTreeMap<GroupId, Recorder> = GROUPS.iter().map(|g| (*g, Recorder::default())).collect();
        let made = recorders.clone();
        let node = Node::start(self.hosts[&id].clone(), self.cfgs[&id].clone(), move |g| made[&g].clone())
            .await
            .unwrap_or_else(|e| panic!("node {id} cannot start from its disk: {e}"));
        let handle = node.handle();
        self.running.insert(id, Some(Running { node, handle, recorders }));
    }

    /// Power loss: `mid_write` arms the disk to fail after a few writes, so
    /// the node dies inside a round; otherwise it stops between rounds.
    async fn crash(&mut self, id: NodeId, mid_write: bool) {
        let Some(Some(r)) = self.running.insert(id, None) else { return };
        let host = self.hosts[&id].clone();
        if mid_write {
            host.disk().arm_power_loss(self.rt.below(6));
            host.sleep(TICK * 3).await;
        }
        r.handle.stop();
        let _ = r.node.join().await; // an error is the power loss itself
        host.crash();
        let down = if self.rt.chance(1, 3) { TICK } else { TICK * (5 + self.rt.below(60) as u32) };
        self.down_until.insert(id, self.rt.now() + down);
    }

    fn check(&mut self) {
        for (id, r) in &self.running {
            let Some(r) = r else { continue };
            for g in GROUPS {
                if let Some(s) = r.handle.status(g)
                    && s.role == Role::Leader
                {
                    let other = *self.leaders.entry((g, s.term)).or_insert(*id);
                    assert_eq!(other, *id, "group {g}: two leaders in term {}", s.term);
                }
                let seen = self.applied.get_mut(&g).unwrap();
                for (i, v) in r.recorders[&g].0.lock().unwrap().iter().enumerate() {
                    match seen.get(i) {
                        Some(s) => assert_eq!(s, v, "group {g}: node {id} applied {v:?} at {i}, others {s:?}"),
                        None => seen.push(v.clone()),
                    }
                }
            }
        }
    }

    fn leader(&self, g: GroupId) -> Option<NodeHandle> {
        self.running
            .values()
            .flatten()
            .find(|r| r.handle.status(g).is_some_and(|s| s.role == Role::Leader))
            .map(|r| r.handle.clone())
    }
}

async fn run(rt: SimRuntime) {
    let mut w = World::new(rt.clone()).await;
    let mut proposals = 0u64;
    let end = rt.now() + Duration::from_secs(6);
    let mut partition_until = None;
    while rt.now() < end {
        rt.sleep(TICK * 2).await;
        // Restarts.
        let back: Vec<NodeId> = w.down_until.iter().filter(|(_, t)| **t <= rt.now()).map(|(id, _)| *id).collect();
        for id in back {
            w.down_until.remove(&id);
            w.start(id).await;
        }
        // Clients.
        for _ in 0..rt.below(3) {
            let g = GROUPS[rt.below(3) as usize];
            let id = 1 + rt.below(3);
            if let Some(Some(r)) = w.running.get(&id) {
                let value = format!("g{g}-p{proposals}");
                if r.handle.propose(g, batch(value.as_bytes(), 1 + rt.below(3))).await.is_ok() {
                    proposals += 1;
                }
            }
        }
        // Faults.
        if partition_until.is_none() && rt.chance(1, 40) {
            let (a, b) = (1 + rt.below(3), 1 + rt.below(3));
            if a != b {
                rt.net().block(&format!("n{a}"), &format!("n{b}"));
                partition_until = Some(rt.now() + TICK * (10 + rt.below(80) as u32));
            }
        }
        if partition_until.is_some_and(|t| t <= rt.now()) {
            rt.net().heal();
            partition_until = None;
        }
        let up = w.running.values().filter(|r| r.is_some()).count();
        if up == 3 && rt.chance(1, 25) {
            let mid_write = rt.chance(1, 2);
            w.crash(1 + rt.below(3), mid_write).await;
        }
        w.check();
    }

    // Heal and check that every group still makes progress everywhere.
    rt.net().heal();
    for id in std::mem::take(&mut w.down_until).into_keys() {
        w.start(id).await;
    }
    for g in GROUPS {
        let marker = Bytes::from(format!("after-heal-{g}"));
        let deadline = rt.now() + Duration::from_secs(20);
        let mut proposed = false;
        loop {
            assert!(rt.now() < deadline, "group {g} made no progress after healing ({proposals} proposals before)");
            if !proposed && let Some(l) = w.leader(g) {
                proposed = l.propose(g, batch(&marker, 1)).await.is_ok();
            }
            rt.sleep(TICK * 5).await;
            w.check();
            let everywhere = w.running.values().flatten().all(|r| r.recorders[&g].0.lock().unwrap().contains(&marker));
            if everywhere {
                break;
            }
            // A leader that was deposed may have lost the marker: propose again.
            if proposed && rt.chance(1, 20) {
                proposed = false;
            }
        }
    }
    for r in w.running.into_values().flatten() {
        r.handle.stop();
        let _ = r.node.join().await;
    }
}

#[test]
fn nodes_keep_their_groups_consistent_through_partitions_and_power_loss() {
    check_seeds(20, |seed| Sim::new(seed).run(run));
}

fn term(w: &World, g: GroupId) -> u64 {
    w.running.values().flatten().filter_map(|r| r.handle.status(g)).map(|s| s.term).max().unwrap_or(0)
}

/// A busy disk: every sync on the node that leads the most groups takes
/// 20 ms, while the shortest election timeout is 100 ms (10 ticks of 10 ms).
/// Each group's write is a log sync and the hard state's two (the file and
/// its directory). Done one group after the other, a round that writes to
/// all three groups waits for nine syncs, 180 ms, and the fast followers hear
/// no heartbeat for longer than an election timeout. A node persists the
/// groups of a round at the same time (ADR 0143), so a round costs about one
/// group's syncs and no group holds an election while all of them take
/// writes.
#[test]
fn groups_keep_their_leaders_while_a_slow_disk_takes_writes_to_all_of_them() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let w = World::with_timing(rt.clone(), LogConfig::default(), TICK * 10, TICK * 2).await;
            slow_disk(rt, w, Duration::from_millis(20)).await;
        })
    });
}

/// A disk on CI whose syncs take 100 ms makes a round take 300 ms: longer
/// than an election timeout of 10 ticks, so the simulation's own timing
/// would elect, but well within the default of a second (ADR 0144), which a
/// node gets unless its cluster file says otherwise.
#[test]
fn with_the_default_election_timeout_rounds_of_several_hundred_ms_keep_their_leaders() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let (election, heartbeat) =
                (lotse_server::DEFAULT_ELECTION_TIMEOUT, lotse_server::DEFAULT_HEARTBEAT_INTERVAL);
            let w = World::with_timing(rt.clone(), LogConfig::default(), election, heartbeat).await;
            slow_disk(rt, w, Duration::from_millis(100)).await;
        })
    });
}

/// Waits for leaders, makes every sync of the node leading the most groups
/// take `delay`, writes to all groups at once for three seconds, and checks
/// that no group elected meanwhile.
async fn slow_disk(rt: SimRuntime, mut w: World, delay: Duration) {
    use lotse_rt::Spawn;
    let deadline = rt.now() + Duration::from_secs(20);
    while GROUPS.iter().any(|g| w.leader(*g).is_none()) {
        assert!(rt.now() < deadline, "the groups found no leaders");
        rt.sleep(TICK * 5).await;
    }
    // The elections of the start settle first.
    rt.sleep(Duration::from_secs(2)).await;
    // The disk of the node that leads the most groups gets busy, the
    // others' do not: their followers count ticks on time while its
    // loop waits for its syncs.
    let leads = |id: NodeId| {
        GROUPS
            .iter()
            .filter(|g| {
                w.running[&id].as_ref().and_then(|r| r.handle.status(**g)).is_some_and(|s| s.role == Role::Leader)
            })
            .count()
    };
    let slow = *w.hosts.keys().max_by_key(|id| (leads(**id), std::cmp::Reverse(**id))).expect("three nodes");
    w.hosts[&slow].set_sync_delay(delay);
    let terms: BTreeMap<GroupId, u64> = GROUPS.iter().map(|g| (*g, term(&w, *g))).collect();
    let end = rt.now() + Duration::from_secs(3);
    let mut n = 0u64;
    while rt.now() < end {
        // All three groups at once, so their writes share rounds.
        let mut proposals = Vec::new();
        for g in GROUPS {
            if let Some(leader) = w.leader(g) {
                let value = format!("g{g}-{n}");
                proposals.push(rt.spawn(async move { leader.propose(g, batch(value.as_bytes(), 1)).await }));
                n += 1;
            }
        }
        for p in proposals {
            let _ = p.await;
        }
        // Without a leader there is nothing to wait for: time must still pass.
        rt.sleep(TICK).await;
        w.check();
    }
    for g in GROUPS {
        assert_eq!(term(&w, g), terms[&g], "group {g} held an election while the slow disk took writes");
    }
    assert!(
        w.hosts[&slow].disk().most_syncs_at_once() >= 2,
        "no node had two syncs under way at once: the groups of a round were persisted one after the other"
    );
    for r in std::mem::take(&mut w.running).into_values().flatten() {
        r.handle.stop();
        let _ = r.node.join().await;
    }
}

/// A cluster of one node has nobody to wait for: its groups elect at the
/// first tick, at a first start and after a restart, not after the default
/// election timeout of a second (ADR 0144), so a single node is ready at
/// once, as the packages' checks expect.
#[test]
fn a_one_node_cluster_elects_at_its_first_tick() {
    Sim::new(1).run(|rt| async move {
        let host = rt.node("n1");
        let peers = BTreeMap::from([(1, "n1:7000".to_string())]);
        let (election, heartbeat) = (lotse_server::DEFAULT_ELECTION_TIMEOUT, lotse_server::DEFAULT_HEARTBEAT_INTERVAL);
        let cfg = node_config(1, &peers, true, LogConfig::default(), election, heartbeat);
        for start in ["a first start", "a restart"] {
            let began = rt.now();
            let node = Node::start(host.clone(), cfg.clone(), |_| Recorder::default()).await.unwrap();
            let handle = node.handle();
            while GROUPS.iter().any(|g| handle.status(*g).is_none_or(|s| s.role != Role::Leader)) {
                assert!(rt.now() - began < TICK * 10, "after {start} a group had no leader within 10 ticks");
                rt.sleep(TICK).await;
            }
            handle.stop();
            let _ = node.join().await;
        }
    });
}

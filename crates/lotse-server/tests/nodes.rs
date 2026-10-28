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

fn batch(value: &[u8], records: u64) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value), &[]);
    }
    b.finish().unwrap()
}

impl World {
    async fn new(rt: SimRuntime) -> World {
        let ids: Vec<NodeId> = vec![1, 2, 3];
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
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
                        groups: GROUPS.to_vec(),
                        tick: TICK,
                        pre_vote,
                        log,
                        client_listen: None,
                        client_addrs: BTreeMap::new(),
                        streams: Vec::new(),
                    },
                )
            })
            .collect();
        let mut w = World {
            hosts: ids.iter().map(|&id| (id, rt.node(&format!("n{id}")))).collect(),
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

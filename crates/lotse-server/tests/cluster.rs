//! Replicas on crashing disks, checked over many seeds.
//!
//! Three or five nodes, each a [`Replica`] on its own simulated disk, run a
//! Raft group through lost, delayed, duplicated and reordered messages and
//! partitions. Nodes fail by power loss: sometimes between two rounds,
//! sometimes in the middle of one (the disk stops after a random number of
//! writes), and the disk then keeps only what was synced plus a random,
//! possibly torn, prefix of the rest. A failed node restarts from whatever
//! its disk holds. Nodes snapshot at random, so followers also catch up
//! through installed snapshots.
//!
//! Checked throughout:
//! - **Election safety:** at most one leader per term.
//! - **State machine safety:** every node applies the same batches in the
//!   same order, across restarts (the state machine is rebuilt from the
//!   snapshot and the log).
//!
//! A committed batch lost by a crash would show up as a different batch at
//! its position on some node, so the prefix check also covers durability.
//! After healing, **liveness:** a new batch commits and every node applies
//! it, and with it everything applied anywhere before.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_log::LogConfig;
use lotse_raft::{Config, Message, NodeId, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Rng;
use lotse_server::{Replica, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_seeds};

/// Records the value of every applied batch.
#[derive(Debug, Default)]
struct Recorder(Vec<Bytes>);

impl StateMachine for Recorder {
    fn apply(&mut self, batch: &Batch) {
        self.0.push(batch.records().expect("valid batch")[0].value.clone().expect("a value"));
    }
    fn snapshot(&self) -> Bytes {
        let mut b = BytesMut::new();
        for v in &self.0 {
            b.put_u32_le(v.len() as u32);
            b.put_slice(v);
        }
        b.freeze()
    }
    fn restore(&mut self, mut data: &[u8]) -> Result<(), String> {
        self.0.clear();
        while data.has_remaining() {
            let n = data.get_u32_le() as usize;
            if data.len() < n {
                return Err("truncated".into());
            }
            self.0.push(Bytes::copy_from_slice(&data[..n]));
            data.advance(n);
        }
        Ok(())
    }
}

type Node = Replica<SimRuntime, Recorder>;

struct InFlight {
    at: u64,
    from: NodeId,
    to: NodeId,
    msg: Message,
}

struct World {
    rng: SimRuntime,
    rts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, Config>,
    nodes: BTreeMap<NodeId, Option<Node>>,
    down_until: BTreeMap<NodeId, u64>,
    partition: Option<(BTreeSet<NodeId>, u64)>,
    net: Vec<InFlight>,
    now: u64,
    faults: bool,
    crash_every: u64,
    drop_percent: u64,
    log_config: LogConfig,
    proposals: u64,
    leaders: BTreeMap<u64, NodeId>,
    /// Every applied value, in order, as the first node to apply it saw it.
    applied: Vec<Bytes>,
}

fn batch(records: u64, value: &[u8]) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value), &[]);
    }
    b.finish().expect("a valid batch")
}

impl World {
    async fn new(rt: SimRuntime) -> World {
        let n: u64 = if rt.chance(1, 2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let pre_vote = rt.chance(1, 2);
        // Small segments, so rolls, multi-segment reads and resets happen.
        let log_config = LogConfig { segment_bytes: 200 + rt.below(2_000), index_interval_bytes: 64 };
        let mut w = World {
            rts: ids.iter().map(|&id| (id, rt.with_own_disk())).collect(),
            cfgs: ids
                .iter()
                .map(|&id| (id, Config { seed: rt.next_u64(), pre_vote, ..Config::new(id, ids.clone()) }))
                .collect(),
            nodes: BTreeMap::new(),
            down_until: BTreeMap::new(),
            partition: None,
            net: Vec::new(),
            now: 0,
            faults: true,
            crash_every: [25, 80, 300][rt.below(3) as usize],
            drop_percent: [1, 5, 15][rt.below(3) as usize],
            log_config,
            proposals: 0,
            leaders: BTreeMap::new(),
            applied: Vec::new(),
            rng: rt,
        };
        for id in ids {
            let node = w.open(id).await;
            w.nodes.insert(id, Some(node));
        }
        w
    }

    async fn open(&self, id: NodeId) -> Node {
        let mut node =
            Replica::open(self.rts[&id].clone(), "group", self.cfgs[&id].clone(), Recorder::default(), self.log_config)
                .await
                .unwrap_or_else(|e| panic!("node {id} cannot recover from its disk: {e}"));
        node.max_fill_bytes = 1 + self.rng.below(400) as usize;
        node
    }

    fn up(&self) -> Vec<NodeId> {
        self.nodes.iter().filter(|(_, n)| n.is_some()).map(|(id, _)| *id).collect()
    }

    fn linked(&self, a: NodeId, b: NodeId) -> bool {
        self.partition.as_ref().is_none_or(|(side, _)| side.contains(&a) == side.contains(&b))
    }

    /// Power loss on node `id`: its disk keeps what was synced, maybe a torn
    /// prefix of the rest.
    fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
        self.rts[&id].crash();
        let downtime = if self.rng.chance(1, 3) { 1 } else { 5 + self.rng.below(100) };
        self.down_until.insert(id, self.now + downtime);
    }

    async fn step(&mut self) {
        self.now += 1;
        let r = self.rng.clone();

        if self.faults && self.partition.is_none() && r.chance(1, 200) {
            let side: BTreeSet<NodeId> = self.nodes.keys().copied().filter(|_| r.chance(1, 2)).collect();
            self.partition = Some((side, self.now + 30 + r.below(200)));
        }
        if self.partition.as_ref().is_some_and(|(_, until)| *until <= self.now) {
            self.partition = None;
        }
        let back: Vec<NodeId> = self.down_until.iter().filter(|(_, t)| **t <= self.now).map(|(id, _)| *id).collect();
        for id in back {
            self.down_until.remove(&id);
            let node = self.open(id).await;
            self.nodes.insert(id, Some(node));
        }

        for id in self.up() {
            let node = self.nodes.get_mut(&id).unwrap().as_mut().unwrap();
            if !r.chance(1, 10) {
                node.raft().tick();
            }
        }

        if r.chance(1, 3) {
            let up = self.up();
            let id = up[r.below(up.len() as u64) as usize];
            let value = format!("p{}", self.proposals);
            let node = self.nodes.get_mut(&id).unwrap().as_mut().unwrap();
            if node.propose(batch(1 + r.below(3), value.as_bytes())).is_ok() {
                self.proposals += 1;
            }
        }

        // Deliver what is due, in random order.
        let (mut due, later): (Vec<InFlight>, Vec<InFlight>) = self.net.drain(..).partition(|m| m.at <= self.now);
        self.net = later;
        while !due.is_empty() {
            let m = due.swap_remove(r.below(due.len() as u64) as usize);
            if !self.linked(m.from, m.to) {
                continue;
            }
            if let Some(Some(node)) = self.nodes.get_mut(&m.to) {
                node.raft().step(m.from, m.msg);
            }
        }

        // Persist, apply, send. Sometimes the power goes in the middle.
        for id in self.up() {
            let mid_round = self.faults && self.up().len() > 1 && r.chance(1, self.crash_every);
            if mid_round {
                self.rts[&id].disk().arm_power_loss(r.below(8));
            }
            let node = self.nodes.get_mut(&id).unwrap().as_mut().unwrap();
            if r.chance(1, 30) {
                let _ = node.compact().await;
            }
            let result = node.process().await;
            if mid_round {
                // Whatever went out before the power went is lost with it.
                self.crash(id);
                continue;
            }
            let out = result.unwrap_or_else(|e| panic!("node {id} failed without a crash: {e}"));
            for s in out.messages {
                if self.faults && r.below(100) < self.drop_percent {
                    continue;
                }
                let copies = if self.faults && r.chance(1, 50) { 2 } else { 1 };
                for _ in 0..copies {
                    let at = self.now + if self.faults { r.below(6) } else { r.below(2) };
                    self.net.push(InFlight { at, from: id, to: s.to, msg: s.msg.clone() });
                }
            }
        }
        if self.faults && self.up().len() > 1 && r.chance(1, self.crash_every * 2) {
            let up = self.up();
            self.crash(up[r.below(up.len() as u64) as usize]);
        }
        self.check();
    }

    fn check(&mut self) {
        for (id, node) in &self.nodes {
            let Some(node) = node else { continue };
            let raft = node.raft_ref();
            if raft.role() == Role::Leader {
                let other = *self.leaders.entry(raft.term()).or_insert(*id);
                assert_eq!(other, *id, "two leaders in term {}: {other} and {id}", raft.term());
            }
            for (i, v) in node.state_machine().0.iter().enumerate() {
                match self.applied.get(i) {
                    Some(seen) => assert_eq!(seen, v, "node {id} applied {v:?} at position {i}, others {seen:?}"),
                    None => self.applied.push(v.clone()),
                }
            }
        }
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter_map(|(id, n)| n.as_ref().filter(|n| n.raft_ref().role() == Role::Leader).map(|_| *id))
            .collect();
        (leaders.len() == 1).then(|| leaders[0])
    }

    async fn heal(&mut self) {
        self.faults = false;
        self.partition = None;
        for id in std::mem::take(&mut self.down_until).into_keys() {
            let node = self.open(id).await;
            self.nodes.insert(id, Some(node));
        }
    }
}

async fn run(rt: SimRuntime, steps: u64) {
    let mut w = World::new(rt).await;
    for _ in 0..steps {
        w.step().await;
    }
    w.heal().await;
    let marker = Bytes::from_static(b"after-heal");
    let mut proposed_to = None;
    for _ in 0..1_500 {
        w.step().await;
        if let Some(l) = w.leader() {
            let term = w.nodes[&l].as_ref().unwrap().raft_ref().term();
            if proposed_to != Some((l, term)) {
                let node = w.nodes.get_mut(&l).unwrap().as_mut().unwrap();
                if node.propose(batch(1, &marker)).is_ok() {
                    proposed_to = Some((l, term));
                }
            }
        }
        let done = w.nodes.values().all(|n| n.as_ref().is_some_and(|n| n.state_machine().0.contains(&marker)));
        if done {
            // Every node's applied batches are a prefix of one sequence and
            // hold the marker, so each has everything applied anywhere
            // before it: a lost committed batch would have shown up as a
            // different batch at its position.
            return;
        }
    }
    panic!(
        "no progress after healing: leader {:?}, terms {:?}, commits {:?}, applied {:?}",
        w.leader(),
        w.nodes.values().map(|n| n.as_ref().map(|n| n.raft_ref().term())).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.as_ref().map(|n| n.raft_ref().commit())).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.as_ref().map(|n| n.state_machine().0.len())).collect::<Vec<_>>(),
    );
}

#[test]
fn replicas_survive_power_loss() {
    check_seeds(60, |seed| Sim::new(seed).run(|rt| run(rt, 1_200)));
}

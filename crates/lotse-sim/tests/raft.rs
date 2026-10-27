//! Raft safety and liveness under faults, checked over many seeds.
//!
//! Each run builds a 3- or 5-node group and, for a few thousand steps,
//! drops, delays, duplicates and reorders messages, lets clocks drift,
//! partitions the network and crashes nodes (they restart from what they
//! persisted). Throughout, it checks the Raft guarantees:
//!
//! - **Vote safety:** a node grants at most one vote per term, even across
//!   crashes.
//! - **Election safety:** at most one leader per term.
//! - **State machine safety:** every node applies the same entries in the
//!   same order.
//! - **Durability:** once a node holds a committed entry, it never loses it.
//! - **Leader completeness:** a leader holds every committed entry.
//!
//! Then it heals everything and checks **liveness**: a leader emerges, a new
//! proposal commits and every node applies it.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use lotse_raft::testing::MemNode;
use lotse_raft::{Config, Entry, EntryKind, Message, NodeId, Role};
use lotse_rt::Rng;
use lotse_sim::{SimRng, check_seeds};

struct InFlight {
    at: u64,
    from: NodeId,
    to: NodeId,
    msg: Message,
}

/// How hostile one run is. Drawn per seed, so some runs are calm and some
/// lose a node every few dozen steps.
struct Faults {
    /// A node crashes with probability 1 in `crash` per step.
    crash: u64,
    /// Percent of messages lost.
    drop: u64,
    /// Messages arrive within this many steps.
    delay: u64,
}

struct World {
    rng: SimRng,
    f: Faults,
    now: u64,
    nodes: BTreeMap<NodeId, MemNode>,
    /// Down nodes and the step they come back.
    down: BTreeMap<NodeId, u64>,
    /// One side of a partition and when it heals.
    partition: Option<(BTreeSet<NodeId>, u64)>,
    net: Vec<InFlight>,
    faults: bool,
    // what the checks remember
    leaders: BTreeMap<u64, NodeId>,
    /// (voter, term) -> candidate, from every granted vote ever sent.
    votes: BTreeMap<(NodeId, u64), NodeId>,
    committed: Vec<Entry>,
    /// Per committed entry: the highest term any node had when it was first
    /// seen applied. It committed in that term or earlier.
    committed_by: Vec<u64>,
    kept: BTreeMap<NodeId, usize>,
    proposals: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let rng = SimRng::new(seed);
        let n = if seed % 2 == 0 { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let f = Faults {
            crash: [30, 120, 500][rng.below(3) as usize],
            drop: [1, 5, 20][rng.below(3) as usize],
            delay: [2, 4, 12][rng.below(3) as usize],
        };
        // Both protections are optional in the core; each guards the other's
        // gaps, so runs mix them.
        let (pre_vote, check_quorum) = (rng.chance(1, 2), rng.chance(1, 2));
        let nodes = ids
            .iter()
            .map(|&id| {
                let cfg = Config { seed: rng.next_u64(), pre_vote, check_quorum, ..Config::new(id, ids.clone()) };
                let mut node = MemNode::new(cfg);
                node.max_fill_bytes = 1 + rng.below(250) as usize; // often one entry per append
                (id, node)
            })
            .collect();
        World {
            rng,
            f,
            now: 0,
            nodes,
            down: BTreeMap::new(),
            partition: None,
            net: Vec::new(),
            faults: true,
            leaders: BTreeMap::new(),
            votes: BTreeMap::new(),
            committed: Vec::new(),
            committed_by: Vec::new(),
            kept: BTreeMap::new(),
            proposals: 0,
        }
    }

    fn up(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().filter(|id| !self.down.contains_key(id)).collect()
    }

    fn linked(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            Some((side, _)) => side.contains(&a) == side.contains(&b),
            None => true,
        }
    }

    fn step(&mut self) {
        self.now += 1;
        let r = self.rng.clone();

        if self.faults && self.partition.is_none() && r.chance(1, 250) {
            let side: BTreeSet<NodeId> = self.nodes.keys().copied().filter(|_| r.chance(1, 2)).collect();
            self.partition = Some((side, self.now + 50 + r.below(300)));
        }
        let back: Vec<NodeId> = self.down.iter().filter(|(_, at)| **at <= self.now).map(|(id, _)| *id).collect();
        for id in back {
            self.down.remove(&id);
            self.nodes.get_mut(&id).unwrap().restart();
        }
        if self.partition.as_ref().is_some_and(|(_, until)| *until <= self.now) {
            self.partition = None;
        }

        // Time, with drift: some nodes skip a tick, some take two.
        for id in self.up() {
            let ticks = if r.chance(1, 10) {
                0
            } else if r.chance(1, 20) {
                2
            } else {
                1
            };
            for _ in 0..ticks {
                self.nodes.get_mut(&id).unwrap().raft.tick();
            }
        }

        // Clients propose to whichever node they reach.
        if r.chance(1, 3) {
            let up = self.up();
            let id = up[r.below(up.len() as u64) as usize];
            let data = Bytes::from(format!("p{}", self.proposals));
            if self.nodes.get_mut(&id).unwrap().propose(1 + r.below(4), data).is_ok() {
                self.proposals += 1;
            }
        }

        // Deliver what is due, in random order.
        let mut due: Vec<InFlight> = Vec::new();
        let mut later = Vec::new();
        for m in self.net.drain(..) {
            if m.at <= self.now { due.push(m) } else { later.push(m) }
        }
        self.net = later;
        while !due.is_empty() {
            let m = due.swap_remove(r.below(due.len() as u64) as usize);
            if self.down.contains_key(&m.to) || self.down.contains_key(&m.from) || !self.linked(m.from, m.to) {
                continue;
            }
            self.nodes.get_mut(&m.to).unwrap().raft.step(m.from, m.msg);
        }

        // Persist, apply and send. A node may crash here: before persisting
        // (what it just learned is gone) or after persisting but before
        // sending (its messages are gone).
        for id in self.up() {
            if self.faults && self.up().len() > 1 && r.chance(1, self.f.crash) {
                let before_persist = r.chance(1, 2);
                if !before_persist {
                    self.nodes.get_mut(&id).unwrap().process();
                }
                self.down.insert(id, self.now + 5 + r.below(150));
                continue;
            }
            for _ in 0..4 {
                let out = self.nodes.get_mut(&id).unwrap().process();
                if out.is_empty() && !self.nodes[&id].raft.has_ready() {
                    break;
                }
                for s in out {
                    // Vote safety, checked on the wire: one vote per node and
                    // term, even across crashes.
                    if let Message::VoteReply { term, granted: true, pre: false } = s.msg
                        && let Some(c) = self.votes.insert((id, term), s.to)
                    {
                        assert_eq!(c, s.to, "node {id} voted twice in term {term}: for {c} and {}", s.to);
                    }
                    if self.faults && r.below(100) < self.f.drop {
                        continue; // lost
                    }
                    let copies = if self.faults && r.chance(1, 50) { 2 } else { 1 };
                    for _ in 0..copies {
                        let at = self.now + if self.faults { r.below(self.f.delay) } else { r.below(2) };
                        self.net.push(InFlight { at, from: id, to: s.to, msg: s.msg.clone() });
                    }
                }
            }
        }
        self.check();
    }

    fn check(&mut self) {
        // Election safety.
        for id in self.up() {
            let n = &self.nodes[&id];
            if n.raft.role() == Role::Leader {
                let term = n.raft.term();
                if let Some(other) = self.leaders.insert(term, id) {
                    assert_eq!(other, id, "two leaders in term {term}: {other} and {id}");
                }
            }
        }
        // State machine safety: every node's applied entries are a prefix of
        // one shared sequence.
        let max_term = self.nodes.values().map(|n| n.raft.term()).max().unwrap_or(0);
        for (id, n) in &self.nodes {
            for (i, e) in n.applied.iter().enumerate() {
                match self.committed.get(i) {
                    Some(c) => assert_eq!(c, e, "node {id} applied a different entry at position {i}"),
                    None => {
                        self.committed.push(e.clone());
                        self.committed_by.push(max_term);
                    }
                }
            }
        }
        // Durability and leader completeness.
        for (id, n) in &self.nodes {
            let keeps = self.committed.iter().zip(&n.log).take_while(|(c, l)| c == l).count();
            let before = self.kept.insert(*id, keeps).unwrap_or(0);
            assert!(keeps >= before, "node {id} lost committed entries: held {before}, now {keeps}");
            if n.raft.role() == Role::Leader && !self.down.contains_key(id) {
                // Only leaders of later terms must hold an entry; a deposed
                // leader that has not noticed yet may lag.
                let term = n.raft.term();
                for (c, _) in self.committed.iter().zip(&self.committed_by).filter(|(_, by)| term > **by) {
                    assert_eq!(
                        n.raft.log().term_at(c.last),
                        Some(c.term),
                        "leader {id} (term {}) lacks committed index {}",
                        n.raft.term(),
                        c.last
                    );
                }
            }
        }
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> =
            self.up().into_iter().filter(|id| self.nodes[id].raft.role() == Role::Leader).collect();
        if leaders.len() == 1 { Some(leaders[0]) } else { None }
    }

    fn heal(&mut self) {
        self.faults = false;
        self.partition = None;
        for id in std::mem::take(&mut self.down).into_keys() {
            self.nodes.get_mut(&id).unwrap().restart();
        }
    }
}

fn run(seed: u64, steps: u64) {
    let mut w = World::new(seed);
    for _ in 0..steps {
        w.step();
    }
    w.heal();
    // Liveness: within 1,000 steps a leader commits a new entry and every
    // node applies it. Like a real client, propose again whenever leadership
    // changes: a leader that is deposed may lose writes it had not committed.
    let marker = Bytes::from_static(b"after-heal");
    let mut proposed_to = None;
    for _ in 0..1_000 {
        w.step();
        if let Some(l) = w.leader() {
            let term = w.nodes[&l].raft.term();
            if proposed_to != Some((l, term)) && w.nodes.get_mut(&l).unwrap().propose(1, marker.clone()).is_ok() {
                proposed_to = Some((l, term));
            }
        }
        if w.nodes.values().all(|n| n.applied_data().contains(&marker)) {
            let any_normal = w.committed.iter().filter(|e| e.kind == EntryKind::Normal).count();
            assert!(any_normal > 0);
            return;
        }
    }
    panic!(
        "no progress after healing (proposed to {proposed_to:?}): leader {:?}, last {:?}, applied {:?}, terms {:?}, commits {:?}",
        w.leader(),
        w.nodes.values().map(|n| n.raft.last_index()).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.applied.last().map(|e| (e.last, e.data.clone()))).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.raft.term()).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.raft.commit()).collect::<Vec<_>>()
    );
}

#[test]
fn raft_is_safe_and_live_under_faults() {
    check_seeds(150, |seed| run(seed, 2_500));
}

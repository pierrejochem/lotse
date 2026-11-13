//! Raft safety and liveness under faults, checked over many seeds.
//!
//! Each run builds a 3- or 5-node group and, for a few thousand steps,
//! drops, delays, duplicates and reorders messages, lets clocks drift,
//! partitions the network and crashes nodes (they restart from what they
//! persisted). Meanwhile nodes snapshot and compact their logs at random, and
//! the leader is asked to change members at random: add and remove learners,
//! promote them, remove voters (the leader too) and replace several voters in
//! one joint change. Removed nodes keep running. Leadership is handed over
//! at random, clients send linearizable reads to random nodes, and some runs
//! serve reads from a leader lease. Throughout, it checks the Raft
//! guarantees:
//!
//! - **Vote safety:** a node grants at most one vote per term, even across
//!   crashes.
//! - **Election safety:** at most one leader per term.
//! - **State machine safety:** every node applies the same entries in the
//!   same order.
//! - **Durability:** once a node holds a committed entry, it never loses it.
//! - **Leader completeness:** a leader holds every committed entry.
//! - **Linearizable reads:** a confirmed read is served at an index at least
//!   as high as anything committed before the read was asked.
//!
//! Then it heals everything and checks **liveness**: a leader emerges, a new
//! proposal commits and every member applies it.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use lotse_raft::testing::MemNode;
use lotse_raft::{AppendResult, Config, Entry, EntryKind, Membership, Message, NodeId, Role};
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
    pre_vote: bool,
    lease_ticks: Option<u32>,
    /// Ids that may ever be members: the initial voters and two spares.
    pool: Vec<NodeId>,
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
    /// Read ctx -> the highest index committed anywhere when it was asked.
    reads: BTreeMap<u64, u64>,
    /// Per node: how many of its served reads were checked.
    read_seen: BTreeMap<NodeId, usize>,
    next_read: u64,
    stats: Stats,
}

/// What a run actually did, so that a green run can be told from one that never
/// reached the code under test.
#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    /// Chunks of a snapshot delivered after the first.
    later_chunks: u64,
    /// Snapshots whose last chunk was delivered although it was not also the first.
    chunked_installs: u64,
    /// Answers delivered that ask the leader to go on from the middle.
    resumes: u64,
    /// Answers delivered that ask the leader to start over.
    restarts: u64,
    /// Nodes that went down in the middle of receiving a snapshot.
    crashes_mid_transfer: u64,
}

impl World {
    fn new(seed: u64) -> Self {
        let rng = SimRng::new(seed);
        let n = if seed.is_multiple_of(2) { 3 } else { 5 };
        let ids: Vec<NodeId> = (1..=n).collect();
        let f = Faults {
            crash: [30, 120, 500][rng.below(3) as usize],
            drop: [1, 5, 20][rng.below(3) as usize],
            delay: [2, 4, 12][rng.below(3) as usize],
        };
        // Pre-vote is optional in the core and hides bugs in the vote rules,
        // so runs mix it. (Check-quorum is always on.)
        let pre_vote = rng.chance(1, 2);
        // A third of the runs read from leases. The lease leaves room for
        // the clock drift below: a follower may tick twice as fast.
        let lease_ticks = rng.chance(1, 3).then_some(3);
        let nodes = ids.iter().map(|&id| (id, fresh(&rng, id, ids.clone(), pre_vote, lease_ticks))).collect();
        World {
            pool: (1..=n + 2).collect(),
            pre_vote,
            lease_ticks,
            reads: BTreeMap::new(),
            read_seen: BTreeMap::new(),
            next_read: 1,
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
            stats: Stats::default(),
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

        // Nodes snapshot whenever they like.
        for id in self.up() {
            if r.chance(1, 40) {
                self.nodes.get_mut(&id).unwrap().compact();
            }
        }

        // An operator changes members now and then, or moves the leader.
        if self.faults && r.chance(1, 60) {
            self.change_members();
        }
        if self.faults
            && r.chance(1, 80)
            && let Some(l) = self.leader()
        {
            let voters: Vec<NodeId> = self.nodes[&l].raft.membership().voters.iter().copied().collect();
            let to = voters[r.below(voters.len() as u64) as usize];
            let _ = self.nodes.get_mut(&l).unwrap().raft.transfer_leadership(to);
        }

        // Clients read from whichever node they reach.
        if r.chance(1, 4) {
            let up = self.up();
            let id = up[r.below(up.len() as u64) as usize];
            let ctx = self.next_read;
            if self.nodes.get_mut(&id).unwrap().raft.read_index(ctx).is_ok() {
                self.next_read += 1;
                let committed = self.nodes.values().map(|n| n.raft.commit()).max().unwrap_or(0);
                self.reads.insert(ctx, committed);
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
            match &m.msg {
                Message::Snapshot { offset, done, .. } => {
                    self.stats.later_chunks += u64::from(*offset > 0);
                    self.stats.chunked_installs += u64::from(*offset > 0 && *done);
                }
                Message::AppendReply { result: AppendResult::SnapshotChunk { next, .. }, .. } => {
                    self.stats.resumes += u64::from(*next > 0);
                    self.stats.restarts += u64::from(*next == 0);
                }
                _ => {}
            }
            self.nodes.get_mut(&m.to).unwrap().raft.step(m.from, m.msg);
        }

        // Persist, apply and send. A node may crash here: before persisting
        // (what it just learned is gone) or after persisting but before
        // sending (its messages are gone).
        for id in self.up() {
            if self.faults && self.up().len() > 1 && r.chance(1, self.f.crash) {
                self.stats.crashes_mid_transfer += u64::from(self.nodes[&id].raft.receiving_snapshot());
                let before_persist = r.chance(1, 2);
                if !before_persist {
                    self.nodes.get_mut(&id).unwrap().process();
                }
                // Some restarts are immediate, as a process restart can be.
                let downtime = if r.chance(1, 4) { 1 } else { 5 + r.below(150) };
                self.down.insert(id, self.now + downtime);
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
        // Linearizable reads.
        for (id, n) in &self.nodes {
            let seen = self.read_seen.entry(*id).or_default();
            for (read, _) in &n.reads[*seen..] {
                let bound = self.reads[&read.ctx];
                assert!(
                    read.index >= bound,
                    "node {id} served read {} at index {}, but {bound} was committed before it was asked",
                    read.ctx,
                    read.index
                );
            }
            *seen = n.reads.len();
        }
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
            let keeps = self.committed.iter().zip(n.durable()).take_while(|(c, l)| c == l).count();
            let before = self.kept.insert(*id, keeps).unwrap_or(0);
            assert!(keeps >= before, "node {id} lost committed entries: held {before}, now {keeps}");
            if n.raft.role() == Role::Leader && !self.down.contains_key(id) {
                // Only leaders of later terms must hold an entry; a deposed
                // leader that has not noticed yet may lag.
                let term = n.raft.term();
                let compacted = n.raft.log().snapshot_index();
                for (c, _) in self.committed.iter().zip(&self.committed_by).filter(|(_, by)| term > **by) {
                    if c.last <= compacted {
                        continue; // in its snapshot
                    }
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

    /// Asks the leader for a random membership change. Most are refused
    /// (not caught up, a change in progress), which is part of the test.
    fn change_members(&mut self) {
        let Some(l) = self.leader() else { return };
        let r = self.rng.clone();
        let m = self.nodes[&l].raft.membership().clone();
        let outside: Vec<NodeId> = self.pool.iter().copied().filter(|id| !m.contains(*id)).collect();
        let learners: Vec<NodeId> = m.learners.iter().copied().collect();
        let voters: Vec<NodeId> = m.voters.iter().copied().collect();
        let pick = |v: &[NodeId]| v[r.below(v.len() as u64) as usize];
        let (mut new_voters, mut new_learners) = (m.voters.clone(), m.learners.clone());
        match r.below(6) {
            0 if !outside.is_empty() => {
                let id = pick(&outside);
                // Re-added nodes keep whatever state they had.
                if !self.nodes.contains_key(&id) {
                    let node = fresh(&r, id, Vec::new(), self.pre_vote, self.lease_ticks);
                    self.nodes.insert(id, node);
                }
                new_learners.insert(id);
            }
            1 if !learners.is_empty() && voters.len() < 5 => {
                let id = pick(&learners);
                new_learners.remove(&id);
                new_voters.insert(id);
            }
            2 if voters.len() > 3 => {
                new_voters.remove(&pick(&voters));
            }
            3 if !learners.is_empty() => {
                // Replace one or two voters at once.
                for _ in 0..(1 + r.below(2)) {
                    let (out, inn) = (pick(&voters), pick(&learners));
                    new_voters.remove(&out);
                    new_learners.remove(&inn);
                    new_voters.insert(inn);
                }
            }
            4 if !learners.is_empty() => {
                new_learners.remove(&pick(&learners));
            }
            5 if voters.len() >= 5 => {
                // Remove two voters at once: the old and the new majorities
                // need not overlap, only the joint configuration keeps it safe.
                new_voters.remove(&pick(&voters));
                let rest: Vec<NodeId> = new_voters.iter().copied().collect();
                new_voters.remove(&pick(&rest));
            }
            _ => return,
        }
        let _ = self.nodes.get_mut(&l).unwrap().raft.propose_membership(new_voters, new_learners);
    }

    /// The members as the leader sees them, if there is one.
    fn members(&self) -> Option<Membership> {
        self.leader().map(|l| self.nodes[&l].raft.membership().clone())
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

fn fresh(rng: &SimRng, id: NodeId, voters: Vec<NodeId>, pre_vote: bool, lease_ticks: Option<u32>) -> MemNode {
    let cfg = Config {
        seed: rng.next_u64(),
        pre_vote,
        lease_ticks,
        snapshot_chunk_bytes: 32 + rng.below(1_500), // snapshots in several chunks
        snapshot_window: 1 + rng.below(6) as u32,    // one to six of them on the way
        ..Config::new(id, voters)
    };
    let mut node = MemNode::new(cfg);
    node.max_fill_bytes = 1 + rng.below(250) as usize; // often one entry per append
    node
}

fn run(seed: u64, steps: u64) -> Stats {
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
        let applied = |id: &NodeId| w.nodes[id].applied_data().contains(&marker);
        if let Some(m) = w.members()
            && !m.is_joint()
            && m.all().iter().all(applied)
        {
            let any_normal = w.committed.iter().filter(|e| e.kind == EntryKind::Normal).count();
            assert!(any_normal > 0);
            return w.stats;
        }
    }
    panic!(
        "no progress after healing (proposed to {proposed_to:?}): leader {:?}, members {:?}, last {:?}, applied {:?}, terms {:?}, commits {:?}",
        w.leader(),
        w.members(),
        w.nodes.values().map(|n| n.raft.last_index()).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.applied.last().map(|e| (e.last, e.data.clone()))).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.raft.term()).collect::<Vec<_>>(),
        w.nodes.values().map(|n| n.raft.commit()).collect::<Vec<_>>()
    );
}

#[test]
fn raft_is_safe_and_live_under_faults() {
    check_seeds(150, |seed| {
        run(seed, 2_500);
    });
}

/// Snapshots cut into chunks of a few dozen bytes cross a network that drops,
/// duplicates and reorders them, to nodes that crash with a transfer half done
/// and leaders that change. The checks of `run` hold: every node applies what
/// was committed, whatever it installed, and it converges after healing. The
/// counters keep the test honest: transfers really span several chunks, are
/// resumed in the middle, are restarted, and nodes really crash during one.
#[test]
fn chunked_snapshots_converge_under_faults() {
    let total = std::sync::Mutex::new(Stats::default());
    check_seeds(150, |seed| {
        let s = run(seed, 2_500);
        let mut t = total.lock().unwrap();
        t.later_chunks += s.later_chunks;
        t.chunked_installs += s.chunked_installs;
        t.resumes += s.resumes;
        t.restarts += s.restarts;
        t.crashes_mid_transfer += s.crashes_mid_transfer;
    });
    let t = *total.lock().unwrap();
    eprintln!("chunked snapshots: {t:?}");
    assert!(t.later_chunks > 0 && t.chunked_installs > 0, "no snapshot ever spanned chunks: {t:?}");
    assert!(t.resumes > 0 && t.restarts > 0, "no transfer was resumed and restarted: {t:?}");
    assert!(t.crashes_mid_transfer > 0, "no node crashed mid-transfer: {t:?}");
}

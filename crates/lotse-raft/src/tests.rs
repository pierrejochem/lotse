//! Deterministic scenarios on a perfect network. Faults, crashes and random
//! schedules are in `lotse-sim/tests/raft.rs`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::*;
use crate::testing::MemNode;

struct Cluster {
    nodes: BTreeMap<NodeId, MemNode>,
    net: VecDeque<(NodeId, Send)>,
    cut: BTreeSet<NodeId>,
}

impl Cluster {
    fn new(n: u64) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let nodes = ids
            .iter()
            .map(|&id| (id, MemNode::new(Config { seed: id * 7919, ..Config::new(id, ids.clone()) })))
            .collect();
        Cluster { nodes, net: VecDeque::new(), cut: BTreeSet::new() }
    }

    fn pump(&mut self) {
        for _ in 0..1000 {
            let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
            for id in ids {
                let out = self.nodes.get_mut(&id).unwrap().process();
                self.net.extend(out.into_iter().map(|s| (id, s)));
            }
            let Some((from, s)) = self.net.pop_front() else {
                // Advancing can produce more work (a leader commits once its
                // own entries are durable), so stop only when nothing is left.
                if self.nodes.values().any(|n| n.raft.has_ready()) {
                    continue;
                }
                return;
            };
            if self.cut.contains(&from) || self.cut.contains(&s.to) {
                continue;
            }
            self.nodes.get_mut(&s.to).unwrap().raft.step(from, s.msg);
        }
        panic!("messages never settled");
    }

    fn ticks(&mut self, n: u32) {
        for _ in 0..n {
            for node in self.nodes.values_mut() {
                node.raft.tick();
            }
            self.pump();
        }
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .values()
            .filter(|n| n.raft.role() == Role::Leader && !self.cut.contains(&n.raft.id()))
            .map(|n| n.raft.id())
            .collect();
        assert!(leaders.len() <= 1, "two leaders: {leaders:?}");
        leaders.first().copied()
    }

    fn elect(&mut self) -> NodeId {
        for _ in 0..100 {
            self.ticks(1);
            if let Some(l) = self.leader() {
                return l;
            }
        }
        panic!("no leader after 100 ticks");
    }
}

#[test]
fn a_single_node_commits_alone() {
    let mut c = Cluster::new(1);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(3, "a").unwrap();
    c.pump();
    assert_eq!(c.nodes[&l].applied_data(), vec![Bytes::from("a")]);
    assert_eq!(c.nodes[&l].raft.commit(), 4, "no-op at 1, records at 2..=4");
}

#[test]
fn three_nodes_elect_one_leader_and_replicate_ranges() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let (first, last) = c.nodes.get_mut(&l).unwrap().propose(5, "batch-1").unwrap();
    assert_eq!((first, last), (2, 6), "the batch covers one index per record");
    c.nodes.get_mut(&l).unwrap().propose(1, "batch-2").unwrap();
    c.ticks(3);
    for n in c.nodes.values() {
        assert_eq!(n.applied_data(), vec![Bytes::from("batch-1"), Bytes::from("batch-2")], "node {}", n.raft.id());
        assert_eq!(n.raft.commit(), 7);
    }
}

#[test]
fn followers_reject_proposals_and_name_the_leader() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    assert_eq!(c.nodes.get_mut(&f).unwrap().propose(1, "x"), Err(NotLeader { leader: Some(l) }));
}

#[test]
fn a_new_leader_overwrites_uncommitted_entries_of_an_isolated_old_leader() {
    let mut c = Cluster::new(3);
    let old = c.elect();
    c.ticks(2);
    // The old leader is cut off and keeps accepting writes nobody else sees.
    c.cut.insert(old);
    c.nodes.get_mut(&old).unwrap().propose(4, "lost").unwrap();
    c.nodes.get_mut(&old).unwrap().process();
    let new = c.elect();
    assert_ne!(new, old);
    c.nodes.get_mut(&new).unwrap().propose(2, "kept").unwrap();
    c.ticks(2);
    c.cut.clear();
    c.ticks(30);
    for n in c.nodes.values() {
        assert_eq!(n.applied_data(), vec![Bytes::from("kept")], "node {}", n.raft.id());
    }
}

#[test]
fn a_leader_without_a_quorum_steps_down() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let others: Vec<NodeId> = c.nodes.keys().copied().filter(|&id| id != l).collect();
    c.cut.extend(others);
    c.ticks(25);
    assert_ne!(c.nodes[&l].raft.role(), Role::Leader);
}

#[test]
fn pre_vote_keeps_an_isolated_node_from_raising_its_term() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    let term = c.nodes[&f].raft.term();
    c.cut.insert(f);
    c.ticks(200);
    assert_eq!(c.nodes[&f].raft.term(), term, "pre-votes do not change the term");
    c.cut.clear();
    c.ticks(5);
    assert_eq!(c.leader(), Some(l), "the rejoining node does not disrupt the leader");
}

#[test]
fn a_restarted_follower_catches_up_from_its_log() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(2, "one").unwrap();
    c.ticks(3);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(f);
    for i in 0..20 {
        c.nodes.get_mut(&l).unwrap().propose(1 + i % 3, format!("v{i}")).unwrap();
        c.ticks(1);
    }
    c.nodes.get_mut(&f).unwrap().restart();
    c.cut.clear();
    c.ticks(20);
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data());
    assert_eq!(c.nodes[&f].applied_data().len(), 21);
}

#[test]
fn multi_raft_batches_messages_per_destination() {
    let ids = vec![1, 2, 3];
    let mut m = MultiRaft::new();
    for g in 0..50 {
        m.insert(g, RaftGroup::new(Config { seed: g, ..Config::new(1, ids.clone()) }, Restored::default()));
    }
    // Timeouts are 10..20 ticks, so after 19 every group campaigned once.
    for _ in 0..19 {
        m.tick();
    }
    let r = m.ready();
    // Every group asks both other nodes for a (pre-)vote; that is two
    // frames, not a hundred.
    assert_eq!(r.outbound.len(), 2);
    assert_eq!(r.outbound[&2].len(), 50);
    assert!(r.outbound[&2].iter().all(|(_, s)| matches!(s.msg, Message::Vote { pre: true, .. })));
}

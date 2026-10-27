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

    /// A node that joins an existing group: no voters of its own.
    fn add_node(&mut self, id: NodeId) {
        self.nodes.insert(id, MemNode::new(Config { seed: id * 7919, ..Config::new(id, Vec::new()) }));
    }

    fn change(&mut self, voters: &[NodeId], learners: &[NodeId]) -> Result<(u64, u64), ChangeError> {
        let l = self.leader().expect("a leader");
        let r = self
            .nodes
            .get_mut(&l)
            .unwrap()
            .raft
            .propose_membership(voters.iter().copied().collect(), learners.iter().copied().collect());
        self.pump();
        r
    }

    fn membership(&self, id: NodeId) -> Membership {
        self.nodes[&id].raft.membership().clone()
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

#[test]
fn a_follower_behind_the_snapshot_catches_up_from_it() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(f);
    for i in 0..10 {
        c.nodes.get_mut(&l).unwrap().propose(1 + i % 3, format!("v{i}")).unwrap();
        c.ticks(1);
    }
    let at = c.nodes.get_mut(&l).unwrap().compact();
    assert!(at > 10 && c.nodes[&l].log.is_empty(), "everything applied is compacted");
    c.cut.clear();
    c.ticks(10);
    assert_eq!(c.nodes[&f].snapshot_index(), at, "caught up through the snapshot");
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data());
    c.nodes.get_mut(&l).unwrap().propose(1, "after").unwrap();
    c.ticks(3);
    assert_eq!(c.nodes[&f].applied_data().last(), Some(&Bytes::from("after")));
}

#[test]
fn a_follower_with_the_snapshot_entry_keeps_its_log() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(2, "a").unwrap();
    c.ticks(3);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    let before = c.nodes[&f].log.clone();
    let at = c.nodes.get_mut(&l).unwrap().compact();
    // Force a snapshot to f by pretending it is behind.
    let meta = c.nodes[&l].raft.snapshot_meta();
    assert_eq!(meta.index, at);
    let term = c.nodes[&l].raft.term();
    c.nodes.get_mut(&f).unwrap().raft.step(l, Message::Snapshot { term, meta, data: Bytes::new() });
    c.nodes.get_mut(&f).unwrap().process();
    assert_eq!(c.nodes[&f].log, before, "nothing dropped, nothing installed");
    assert_eq!(c.nodes[&f].snapshot_index(), 0);
}

#[test]
fn a_node_restarts_from_its_snapshot() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(3, "a").unwrap();
    c.ticks(3);
    for n in c.nodes.values_mut() {
        n.compact();
    }
    for n in c.nodes.values_mut() {
        n.restart();
    }
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(1, "b").unwrap();
    c.ticks(3);
    for n in c.nodes.values() {
        assert_eq!(n.applied_data(), vec![Bytes::from("a"), Bytes::from("b")], "node {}", n.raft.id());
    }
}

#[test]
fn a_learner_is_added_caught_up_and_promoted() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(2, "a").unwrap();
    c.ticks(2);
    c.nodes.get_mut(&l).unwrap().compact();
    c.add_node(4);
    assert_eq!(c.change(&[1, 2, 3, 4], &[]), Err(ChangeError::NotCaughtUp(4)), "not a learner yet");
    c.change(&[1, 2, 3], &[4]).unwrap();
    c.ticks(5);
    assert_eq!(c.nodes[&4].applied_data(), vec![Bytes::from("a")], "the learner got the log, snapshot included");
    assert!(!c.membership(4).is_voter(4));
    c.change(&[1, 2, 3, 4], &[]).unwrap();
    c.ticks(3);
    for id in 1..=4 {
        let m = c.membership(id);
        assert!(!m.is_joint(), "node {id} left the joint configuration");
        assert_eq!(m.voters, [1, 2, 3, 4].into());
    }
}

#[test]
fn a_removed_leader_hands_over_after_the_change_commits() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    let rest: Vec<NodeId> = c.nodes.keys().copied().filter(|&id| id != l).collect();
    assert_eq!(c.change(&rest, &[]).map(|_| ()), Ok(()));
    assert!(c.membership(l).is_joint() || c.nodes[&l].raft.role() != Role::Leader);
    c.ticks(1);
    assert_ne!(c.nodes[&l].raft.role(), Role::Leader, "a leader that is no longer a voter steps down");
    let new = c.elect();
    assert!(rest.contains(&new));
    c.nodes.get_mut(&new).unwrap().propose(1, "x").unwrap();
    c.ticks(3);
    for id in &rest {
        assert_eq!(c.nodes[id].applied_data(), vec![Bytes::from("x")]);
    }
    // The removed node keeps running and times out, but nobody listens.
    c.ticks(60);
    assert_eq!(c.leader(), Some(new), "the removed node does not disrupt the group");
}

#[test]
fn two_voters_are_replaced_in_one_joint_change() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.add_node(4);
    c.add_node(5);
    c.change(&[1, 2, 3], &[4, 5]).unwrap();
    c.ticks(4);
    let keep = l;
    let voters = [keep, 4, 5];
    let raft = &mut c.nodes.get_mut(&l).unwrap().raft;
    raft.propose_membership(voters.into(), [].into()).unwrap();
    assert_eq!(raft.propose_membership(voters.into(), [].into()), Err(ChangeError::InProgress), "one change at a time");
    c.pump();
    c.ticks(4);
    let m = c.membership(keep);
    assert_eq!(m.voters, voters.into());
    assert!(!m.is_joint());
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(1, "y").unwrap();
    c.ticks(3);
    for id in voters {
        assert_eq!(c.nodes[&id].applied_data().last(), Some(&Bytes::from("y")), "node {id}");
    }
}

#[test]
fn a_voter_grants_its_vote_to_a_node_it_does_not_know_yet() {
    // Node 2 lags: its log predates the promotion of node 4. During the joint
    // change it may be the vote that decides, so it must not ignore 4.
    let mut n = MemNode::new(Config::new(2, vec![1, 2, 3]));
    n.raft.step(4, Message::Vote { term: 1, last_index: 10, last_term: 1, pre: false });
    let out = n.process();
    assert!(
        out.iter().any(|s| s.to == 4 && matches!(s.msg, Message::VoteReply { granted: true, pre: false, .. })),
        "vote requests are not filtered by membership: {out:?}"
    );
}

//! Deterministic scenarios on a perfect network. Faults, crashes and random
//! schedules are in `lotse-sim/tests/raft.rs`.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::*;
use crate::testing::MemNode;

struct Cluster {
    nodes: BTreeMap<NodeId, MemNode>,
    net: VecDeque<(NodeId, Send)>,
    cut: BTreeSet<NodeId>,
    /// Offsets of snapshot chunks to lose, each once.
    lose_chunks: BTreeSet<u64>,
    /// Every snapshot chunk sent: the snapshot's index and the chunk's offset.
    chunks_sent: Vec<(u64, u64)>,
    /// Cuts the receiver off once this many chunks have been sent.
    cut_after_chunks: Option<usize>,
}

impl Cluster {
    fn new(n: u64) -> Self {
        Self::with(n, None)
    }

    fn with(n: u64, lease_ticks: Option<u32>) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let nodes = ids
            .iter()
            .map(|&id| (id, MemNode::new(Config { seed: id * 7919, lease_ticks, ..Config::new(id, ids.clone()) })))
            .collect();
        Cluster {
            nodes,
            net: VecDeque::new(),
            cut: BTreeSet::new(),
            lose_chunks: BTreeSet::new(),
            chunks_sent: Vec::new(),
            cut_after_chunks: None,
        }
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
            if let Message::Snapshot { meta, offset, .. } = &s.msg {
                self.chunks_sent.push((meta.index, *offset));
                if self.cut_after_chunks == Some(self.chunks_sent.len()) {
                    self.cut.insert(s.to);
                }
                if self.lose_chunks.remove(offset) {
                    continue;
                }
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
    c.nodes
        .get_mut(&f)
        .unwrap()
        .raft
        .step(l, Message::Snapshot { term, meta, offset: 0, data: Bytes::new(), done: true, seq: 0 });
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
    n.raft.step(4, Message::Vote { term: 1, last_index: 10, last_term: 1, pre: false, transfer: false });
    let out = n.process();
    assert!(
        out.iter().any(|s| s.to == 4 && matches!(s.msg, Message::VoteReply { granted: true, pre: false, .. })),
        "vote requests are not filtered by membership: {out:?}"
    );
}

#[test]
fn leadership_moves_to_the_chosen_voter() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    let to = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.nodes.get_mut(&l).unwrap().raft.transfer_leadership(to).unwrap();
    assert_eq!(
        c.nodes.get_mut(&l).unwrap().propose(1, "x"),
        Err(NotLeader { leader: Some(to) }),
        "no proposals while handing over"
    );
    c.pump();
    assert_eq!(c.leader(), Some(to));
    c.nodes.get_mut(&to).unwrap().propose(1, "after").unwrap();
    c.ticks(3);
    for n in c.nodes.values() {
        assert_eq!(n.applied_data(), vec![Bytes::from("after")]);
    }
}

#[test]
fn a_lagging_target_is_brought_up_to_date_first() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let to = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(to);
    for i in 0..5 {
        c.nodes.get_mut(&l).unwrap().propose(2, format!("v{i}")).unwrap();
        c.ticks(1);
    }
    c.cut.clear();
    c.nodes.get_mut(&l).unwrap().raft.transfer_leadership(to).unwrap();
    c.ticks(3);
    assert_eq!(c.leader(), Some(to), "it could only win with the whole log");
    assert_eq!(c.nodes[&to].applied_data().len(), 5);
}

#[test]
fn a_transfer_that_does_not_happen_is_given_up() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let to = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(to);
    c.nodes.get_mut(&l).unwrap().raft.transfer_leadership(to).unwrap();
    c.ticks(10);
    assert_eq!(c.leader(), Some(l));
    assert!(c.nodes.get_mut(&l).unwrap().propose(1, "x").is_ok(), "the leader carries on");
    assert_eq!(c.nodes.get_mut(&l).unwrap().raft.transfer_leadership(99), Err(TransferError::NotVoter));
}

#[test]
fn reads_are_confirmed_by_a_quorum_and_batched() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().propose(1, "a").unwrap();
    c.ticks(2);
    let commit = c.nodes[&l].raft.commit();
    for ctx in 1..=3 {
        c.nodes.get_mut(&l).unwrap().raft.read_index(ctx).unwrap();
    }
    // The round goes out with the next ready; nothing is confirmed before.
    let out = c.nodes.get_mut(&l).unwrap().process();
    assert!(c.nodes[&l].reads.is_empty());
    let seqs: BTreeSet<u64> = out
        .iter()
        .filter_map(|s| match s.msg {
            Message::Append { seq, .. } => Some(seq),
            _ => None,
        })
        .collect();
    assert_eq!(seqs.len(), 1, "one round for all three reads");
    c.net.extend(out.into_iter().map(|s| (l, s)));
    c.pump();
    let served: Vec<(u64, u64)> = c.nodes[&l].reads.iter().map(|(r, _)| (r.ctx, r.index)).collect();
    assert_eq!(served, vec![(1, commit), (2, commit), (3, commit)]);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    assert_eq!(c.nodes.get_mut(&f).unwrap().raft.read_index(9), Err(NotLeader { leader: Some(l) }));
}

#[test]
fn a_single_node_confirms_reads_alone() {
    let mut c = Cluster::new(1);
    let l = c.elect();
    c.nodes.get_mut(&l).unwrap().raft.read_index(7).unwrap();
    c.pump();
    assert_eq!(c.nodes[&l].reads.len(), 1);
}

#[test]
fn an_isolated_leader_never_confirms_a_read() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    c.cut.insert(l);
    c.nodes.get_mut(&l).unwrap().raft.read_index(5).unwrap();
    c.ticks(25);
    assert!(c.nodes[&l].reads.is_empty(), "no quorum answered");
    assert_eq!(c.nodes[&l].reads_aborted, vec![5], "aborted when it stepped down");
}

#[test]
fn a_lease_serves_reads_without_a_round_until_it_runs_out() {
    let mut c = Cluster::with(3, Some(4));
    let l = c.elect();
    c.ticks(2);
    c.nodes.get_mut(&l).unwrap().raft.read_index(1).unwrap();
    c.nodes.get_mut(&l).unwrap().process();
    assert_eq!(c.nodes[&l].reads.len(), 1, "served from the lease, no round needed");
    // Cut off, the lease runs out and reads need a quorum again.
    c.cut.insert(l);
    c.ticks(5);
    c.nodes.get_mut(&l).unwrap().raft.read_index(2).unwrap();
    c.ticks(1);
    assert_eq!(c.nodes[&l].reads.len(), 1, "no lease, no quorum: not served");
}

#[test]
fn with_leases_a_restarted_node_waits_before_voting() {
    let mut c = Cluster::with(3, Some(4));
    let l = c.elect();
    c.ticks(2);
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.nodes.get_mut(&f).unwrap().restart();
    let term = c.nodes[&f].raft.term();
    let last = c.nodes[&f].raft.last_index();
    let last_term = c.nodes[&f].raft.log().last_term();
    let vote = Message::Vote { term: term + 1, last_index: last + 10, last_term, pre: false, transfer: false };
    c.nodes.get_mut(&f).unwrap().raft.step(99, vote);
    assert!(c.nodes.get_mut(&f).unwrap().process().is_empty(), "quiet right after a restart");
}

#[test]
fn a_read_is_not_confirmed_by_a_round_sent_before_it() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(2);
    // A round goes out; hold back the replies.
    for _ in 0..2 {
        c.nodes.get_mut(&l).unwrap().raft.tick();
    }
    let round: Vec<(NodeId, Send)> = c.nodes.get_mut(&l).unwrap().process().into_iter().map(|s| (l, s)).collect();
    let mut replies = Vec::new();
    for (from, s) in round {
        let to = s.to;
        c.nodes.get_mut(&to).unwrap().raft.step(from, s.msg);
        replies.extend(c.nodes.get_mut(&to).unwrap().process().into_iter().map(|s| (to, s)));
    }
    // The read arrives after that round was sent. Its replies come in, but
    // they prove leadership only up to before the read.
    c.nodes.get_mut(&l).unwrap().raft.read_index(1).unwrap();
    for (from, s) in replies {
        c.nodes.get_mut(&l).unwrap().raft.step(from, s.msg);
    }
    let next_round = c.nodes.get_mut(&l).unwrap().process();
    assert!(c.nodes[&l].reads.is_empty(), "confirmed by a round older than the read");
    c.net.extend(next_round.into_iter().map(|s| (l, s)));
    c.pump();
    assert_eq!(c.nodes[&l].reads.len(), 1, "confirmed by the next round");
}

#[test]
fn a_leader_that_tried_a_transfer_gives_up_its_lease() {
    let mut c = Cluster::with(3, Some(4));
    let l = c.elect();
    c.ticks(2);
    let to = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(to);
    c.nodes.get_mut(&l).unwrap().raft.transfer_leadership(to).unwrap();
    c.ticks(10); // the transfer is given up
    c.cut.clear();
    c.ticks(1);
    assert_eq!(c.leader(), Some(l));
    c.nodes.get_mut(&l).unwrap().raft.read_index(1).unwrap();
    let round = c.nodes.get_mut(&l).unwrap().process();
    assert!(c.nodes[&l].reads.is_empty(), "a late TimeoutNow could still elect the target: no lease reads");
    c.net.extend(round.into_iter().map(|s| (l, s)));
    c.pump();
    assert_eq!(c.nodes[&l].reads.len(), 1, "served through ReadIndex instead");
}

#[test]
fn a_restart_never_keeps_a_term_below_the_log() {
    // The driver writes the log before the hard state, so a crash can leave
    // entries of term 3 next to a hard state that only knows term 1.
    let restored = Restored {
        hard_state: HardState { term: 1, vote: Some(2), commit: 0 },
        log: TermRuns::from_entries([(1, 1, 1), (2, 4, 3)]),
        ..Restored::default()
    };
    let g = RaftGroup::new(Config::new(1, vec![1, 2, 3]), restored);
    assert_eq!(g.term(), 3, "currentTerm is never below the last log term");
    let mut n = MemNode::new(Config::new(1, vec![1, 2, 3]));
    n.raft = g;
    // The vote belonged to term 1; in term 3 this node has not voted.
    n.raft.step(3, Message::Vote { term: 3, last_index: 4, last_term: 3, pre: false, transfer: false });
    let out = n.process();
    assert!(out.iter().any(|s| matches!(s.msg, Message::VoteReply { granted: true, term: 3, .. })), "{out:?}");
}

#[test]
fn an_answer_from_before_a_node_rejoined_does_not_count() {
    // Node 2 is a learner, catches up and answers; it is removed and its
    // replica wiped; one of its old answers arrives after it was added
    // again. The leader must not take the new, empty replica for caught up.
    let mut c = Cluster::new(1);
    let l = c.elect();
    c.add_node(2);
    c.change(&[1], &[2]).unwrap();
    for i in 0..5 {
        c.nodes.get_mut(&l).unwrap().propose(1, format!("v{i}")).unwrap();
        c.pump();
    }
    let caught_up = c.nodes[&l].raft.matched().unwrap()[&2];
    assert!(caught_up >= 6, "node 2 caught up to {caught_up}");
    // An answer node 2 sent then, held back by the network.
    let term = c.nodes[&l].raft.term();
    let stale = Message::AppendReply { term, result: AppendResult::Accepted { last: caught_up }, seq: 1 };
    c.change(&[1], &[]).unwrap();
    c.nodes.remove(&2);
    c.add_node(2);
    c.cut.insert(2);
    c.change(&[1], &[2]).unwrap();
    c.nodes.get_mut(&l).unwrap().raft.step(2, stale);
    assert_eq!(c.nodes[&l].raft.matched().unwrap()[&2], 0, "an answer from before it rejoined");
    assert_eq!(
        c.nodes.get_mut(&l).unwrap().raft.propose_membership([1, 2].into(), BTreeSet::new()),
        Err(ChangeError::NotCaughtUp(2)),
        "so it is no voter yet"
    );
    // Once it really answers, it catches up and may become one.
    c.cut.clear();
    c.ticks(30);
    let now = c.nodes[&l].raft.matched().unwrap()[&2];
    assert!(now >= caught_up, "caught up to {now} of {caught_up}");
    c.change(&[1, 2], &[]).unwrap();
}

#[test]
fn a_learner_that_lost_its_log_catches_up_again() {
    // Node 2 is a learner and has acknowledged entries; its node then
    // removes the replica and makes it anew, empty. The leader must not
    // keep sending from what it acknowledged.
    let mut c = Cluster::new(1);
    let l = c.elect();
    c.add_node(2);
    c.change(&[1], &[2]).unwrap();
    for i in 0..5 {
        c.nodes.get_mut(&l).unwrap().propose(1, format!("v{i}")).unwrap();
        c.pump();
    }
    let acked = c.nodes[&l].raft.matched().unwrap()[&2];
    assert!(acked >= 6);
    c.nodes.remove(&2);
    c.add_node(2);
    c.nodes.get_mut(&l).unwrap().propose(1, "after").unwrap();
    c.ticks(30);
    let last = c.nodes[&l].raft.last_index();
    assert_eq!(c.nodes[&2].raft.last_index(), last, "the new replica has everything again");
    assert_eq!(c.nodes[&l].raft.matched().unwrap()[&2], last);
}

/// A snapshot a leader holds and a cut-off follower lacks.
struct Snap {
    leader: NodeId,
    follower: NodeId,
    term: u64,
    meta: SnapshotMeta,
    data: Bytes,
}

impl Snap {
    /// Hands the follower the bytes `from..to` as one chunk and returns its answer.
    fn give(&self, c: &mut Cluster, from: usize, to: usize) -> AppendResult {
        self.give_of(c, &self.meta, from, to)
    }

    /// The same for the chunk of another snapshot, described by `meta`.
    fn give_of(&self, c: &mut Cluster, meta: &SnapshotMeta, from: usize, to: usize) -> AppendResult {
        let msg = Message::Snapshot {
            term: self.term,
            meta: meta.clone(),
            offset: from as u64,
            data: self.data.slice(from..to),
            done: to == self.data.len(),
            seq: 1,
        };
        let f = self.follower;
        c.nodes.get_mut(&f).unwrap().raft.step(self.leader, msg);
        let out = c.nodes.get_mut(&f).unwrap().process();
        let mut replies = out.into_iter().filter_map(|s| match s.msg {
            Message::AppendReply { result, .. } => Some(result),
            _ => None,
        });
        let r = replies.next().expect("an answer");
        assert!(replies.next().is_none(), "exactly one answer");
        r
    }
}

/// A leader with a snapshot a follower lacks, and the follower, cut off.
fn snapshot_to_send() -> (Cluster, Snap) {
    let mut c = Cluster::new(3);
    let l = c.elect();
    let f = *c.nodes.keys().find(|&&id| id != l).unwrap();
    c.cut.insert(f);
    for i in 0..12 {
        c.nodes.get_mut(&l).unwrap().propose(1 + i % 3, format!("value-{i}")).unwrap();
        c.ticks(1);
    }
    c.nodes.get_mut(&l).unwrap().compact();
    let (meta, data) = c.nodes[&l].snapshot.clone().unwrap();
    assert!(data.len() > 100, "enough data for several chunks");
    let term = c.nodes[&l].raft.term();
    (c, Snap { leader: l, follower: f, term, meta, data })
}

#[test]
fn a_snapshot_installs_only_when_its_last_chunk_arrives() {
    let (mut c, sn) = snapshot_to_send();
    let (l, f, meta, data) = (sn.leader, sn.follower, sn.meta.clone(), sn.data.clone());
    let n = data.len();
    let (a, b) = (n / 3, 2 * n / 3);
    let r = sn.give(&mut c, 0, a);
    assert_eq!(r, AppendResult::SnapshotChunk { index: meta.index, next: a as u64 });
    let r = sn.give(&mut c, a, b);
    assert_eq!(r, AppendResult::SnapshotChunk { index: meta.index, next: b as u64 });
    assert_eq!(c.nodes[&f].snapshot_index(), 0, "nothing installed before the end");
    assert!(c.nodes[&f].raft.receiving_snapshot());
    let r = sn.give(&mut c, b, n);
    assert_eq!(r, AppendResult::Accepted { last: meta.index });
    assert_eq!(c.nodes[&f].snapshot_index(), meta.index);
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data());
    assert!(!c.nodes[&f].raft.receiving_snapshot());
}

#[test]
fn repeated_and_skipped_chunks_only_say_where_the_follower_stands() {
    let (mut c, sn) = snapshot_to_send();
    let (l, f, meta, data) = (sn.leader, sn.follower, sn.meta.clone(), sn.data.clone());
    let n = data.len();
    let (a, b) = (n / 3, 2 * n / 3);
    let at = |next: usize| AppendResult::SnapshotChunk { index: meta.index, next: next as u64 };
    assert_eq!(sn.give(&mut c, 0, a), at(a));
    // A copy of the first chunk: not taken twice.
    assert_eq!(sn.give(&mut c, 0, a), at(a));
    // The last chunk with the middle one missing: not taken, no install.
    assert_eq!(sn.give(&mut c, b, n), at(a));
    assert_eq!(c.nodes[&f].snapshot_index(), 0);
    // A copy of the first chunk after the second: also not taken.
    assert_eq!(sn.give(&mut c, a, b), at(b));
    assert_eq!(sn.give(&mut c, 0, a), at(b));
    assert_eq!(sn.give(&mut c, b, n), AppendResult::Accepted { last: meta.index });
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data(), "reassembled exactly");
}

#[test]
fn a_newer_snapshot_replaces_a_partial_one_and_an_unknown_one_must_start_over() {
    let (mut c, sn) = snapshot_to_send();
    let (f, meta, data) = (sn.follower, sn.meta.clone(), sn.data.clone());
    let n = data.len();
    let a = n / 2;
    sn.give(&mut c, 0, a);
    // Another snapshot starts at 0: the partial one is dropped.
    let other = SnapshotMeta { term: meta.term + 1, ..meta.clone() };
    let r = sn.give_of(&mut c, &other, 0, 10);
    assert_eq!(r, AppendResult::SnapshotChunk { index: other.index, next: 10 });
    // The first one's second half now finds nothing to continue: start over.
    let r = sn.give(&mut c, a, n);
    assert_eq!(r, AppendResult::SnapshotChunk { index: meta.index, next: 0 });
    assert_eq!(c.nodes[&f].snapshot_index(), 0, "an unknown continuation installs nothing");
    // A node that restarted lost its chunks and says so.
    sn.give(&mut c, 0, a);
    c.nodes.get_mut(&f).unwrap().restart();
    let r = sn.give(&mut c, a, n);
    assert_eq!(r, AppendResult::SnapshotChunk { index: meta.index, next: 0 });
    assert_eq!(c.nodes[&f].snapshot_index(), 0);
    assert_eq!(sn.give(&mut c, 0, n), AppendResult::Accepted { last: meta.index });
}

#[test]
fn a_leader_sends_a_snapshot_in_chunks_and_resumes_where_the_follower_stands() {
    let (mut c, sn) = snapshot_to_send();
    let (l, f, meta, data) = (sn.leader, sn.follower, sn.meta.clone(), sn.data.clone());
    c.nodes.get_mut(&l).unwrap().max_snapshot_chunk = 40;
    let chunks = data.len().div_ceil(40);
    assert!(chunks >= 4);
    // Lose the second and the fourth chunk once each.
    c.lose_chunks.extend([40, 120]);
    c.cut.clear();
    c.ticks(3 * chunks as u32 + 60);
    assert_eq!(c.nodes[&f].snapshot_index(), meta.index, "caught up from chunks");
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data());
    assert!(c.lose_chunks.is_empty(), "the chunks were lost as planned");
}

#[test]
fn a_replaced_snapshot_is_sent_from_its_start_and_the_last_chunk_is_marked() {
    let (_c, sn) = snapshot_to_send();
    let (latest, data) = (sn.meta.clone(), sn.data.clone());
    let older = SnapshotMeta { index: latest.index - 1, ..latest.clone() };
    let msg = |meta: &SnapshotMeta, offset| Message::Snapshot {
        term: 1,
        meta: meta.clone(),
        offset,
        data: Bytes::new(),
        done: false,
        seq: 1,
    };
    // The core meant an older snapshot at offset 50: the driver starts over.
    let mut m = msg(&older, 50);
    m.attach_snapshot_chunk(&latest, &data, &OlderSnapshots::default(), 30);
    let Message::Snapshot { meta, offset, data: chunk, done, .. } = &m else { unreachable!() };
    assert_eq!((meta, *offset, done), (&latest, 0, &false));
    assert_eq!(chunk[..], data[..30]);
    // The same snapshot goes on at the offset; the chunk reaching the end is the last.
    let mut m = msg(&latest, data.len() as u64 - 10);
    m.attach_snapshot_chunk(&latest, &data, &OlderSnapshots::default(), 30);
    let Message::Snapshot { offset, data: chunk, done, .. } = &m else { unreachable!() };
    assert_eq!((*offset, chunk.len(), *done), (data.len() as u64 - 10, 10, true));
    // A snapshot the driver kept for a transfer under way is sent as it was,
    // not replaced by the latest.
    let kept = Bytes::from_static(b"an older snapshot's bytes");
    let kept_at = OlderSnapshots(vec![(older.clone(), kept.clone())]);
    let mut m = msg(&older, 9);
    m.attach_snapshot_chunk(&latest, &data, &kept_at, 8);
    let Message::Snapshot { meta, offset, data: chunk, done, .. } = &m else { unreachable!() };
    assert_eq!((meta, *offset, &chunk[..], *done), (&older, 9, &kept[9..17], false));
}

#[test]
fn a_transfer_finishes_with_its_snapshot_although_the_leader_snapshots_again() {
    let (mut c, sn) = snapshot_to_send();
    let (l, f, first) = (sn.leader, sn.follower, sn.meta.index);
    c.nodes.get_mut(&l).unwrap().max_snapshot_chunk = 40;
    // Let a few chunks through, then the follower is cut off and the leader
    // snapshots again.
    c.cut_after_chunks = Some(2);
    c.cut.clear();
    for _ in 0..100 {
        if c.cut.contains(&f) {
            break;
        }
        c.ticks(1);
    }
    c.cut_after_chunks = None;
    assert!(c.cut.contains(&f));
    assert!(c.nodes[&l].raft.snapshots_in_transfer().contains(&first), "a transfer is under way");
    c.nodes.get_mut(&l).unwrap().propose(1, "newer").unwrap();
    c.ticks(1);
    let second = c.nodes.get_mut(&l).unwrap().compact();
    assert!(second > first);
    assert!(c.nodes[&f].snapshot_index() < first, "not done yet");
    c.cut.clear();
    c.ticks(400);
    let starts: Vec<u64> = c.chunks_sent.iter().filter(|c| c.1 == 0).map(|c| c.0).collect();
    assert_eq!(
        starts.iter().filter(|&&i| i == first).count(),
        1,
        "the first snapshot was not started over: {starts:?}"
    );
    let order: Vec<u64> = c.chunks_sent.iter().map(|c| c.0).collect();
    let last_first = order.iter().rposition(|&i| i == first).unwrap();
    let first_second = order.iter().position(|&i| i == second).expect("the newer one follows");
    let reached = c.chunks_sent.iter().filter(|c| c.0 == first).map(|c| c.1).max().unwrap();
    assert!(reached + 40 >= sn.data.len() as u64, "the first snapshot was sent to its end, not abandoned");
    assert!(last_first < first_second, "the first snapshot is finished before the newer starts: {order:?}");
    assert_eq!(c.nodes[&f].applied_data(), c.nodes[&l].applied_data());
    assert!(c.nodes[&f].snapshot_index() >= first);
    assert!(c.nodes[&l].raft.snapshots_in_transfer().is_empty(), "nothing is kept once it arrived");
}

/// A node asked to campaign soon is elected at the next tick, whatever the
/// seeds would have chosen; the others' delayed elections never start first
/// (ADR 0095). Every node gets each role in turn, so the seeds cannot pick it.
#[test]
fn a_node_that_campaigns_soon_is_elected_first() {
    for preferred in 1..=3 {
        let mut c = Cluster::new(3);
        for (&id, node) in &mut c.nodes {
            if id == preferred {
                node.raft.campaign_soon();
            } else {
                node.raft.delay_election();
            }
        }
        c.ticks(1);
        assert_eq!(c.leader(), Some(preferred), "elected at the first tick");
        assert_eq!(c.nodes[&preferred].raft.term(), 1);
    }
}

/// Without the preferred node the others still elect a leader, one election
/// timeout later than they would have.
#[test]
fn a_delayed_election_still_happens_without_the_preferred_node() {
    let mut c = Cluster::new(3);
    c.cut.insert(1);
    for (&id, node) in &mut c.nodes {
        if id != 1 {
            node.raft.delay_election();
        }
    }
    c.ticks(19);
    assert_eq!(c.leader(), None, "nobody before the delayed timeout");
    let l = c.elect();
    assert_ne!(l, 1);
}

/// A leader reports itself as leading only while a quorum answers it: cut
/// off, it stops saying so within an election timeout, before check-quorum
/// makes it a follower; a new leader says so once its first round is
/// answered.
#[test]
fn a_leader_cut_off_stops_claiming_a_quorum_before_it_steps_down() {
    let mut c = Cluster::new(3);
    let l = c.elect();
    c.ticks(3);
    assert!(c.nodes[&l].raft.leads_with_quorum());
    for (&id, node) in &c.nodes {
        if id != l {
            assert!(!node.raft.leads_with_quorum(), "a follower does not lead");
        }
    }
    let others: Vec<NodeId> = c.nodes.keys().copied().filter(|&id| id != l).collect();
    c.cut.extend(others);
    let mut claimed_until = 0;
    for t in 1..=30 {
        c.ticks(1);
        let raft = &c.nodes[&l].raft;
        if raft.role() != Role::Leader {
            assert!(claimed_until < t, "it stopped claiming before it stepped down");
            break;
        }
        if raft.leads_with_quorum() {
            claimed_until = t;
        }
    }
    assert!(claimed_until <= 10, "claimed for {claimed_until} ticks without a quorum");
}

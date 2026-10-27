//! Who votes, who only listens, and how majorities work during a change.
//!
//! A change of voters goes through a joint configuration: while `outgoing`
//! is not empty, every election and every commit needs a majority of
//! `voters` *and* a majority of `outgoing`. Learners receive the log but
//! never vote and never count.

use std::collections::BTreeSet;

use bytes::{BufMut, Bytes, BytesMut};

use crate::NodeId;

/// Members of a group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Membership {
    /// Voters (the incoming voters during a joint change).
    pub voters: BTreeSet<NodeId>,
    /// Voters of the old configuration while a change is joint; empty
    /// otherwise.
    pub outgoing: BTreeSet<NodeId>,
    /// Receive the log, do not vote.
    pub learners: BTreeSet<NodeId>,
}

impl Membership {
    /// Only these voters, no learners.
    pub fn of_voters(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Membership { voters: voters.into_iter().collect(), ..Self::default() }
    }

    /// `true` while a change of voters is in progress.
    pub fn is_joint(&self) -> bool {
        !self.outgoing.is_empty()
    }

    /// Votes in at least one of the configurations.
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.outgoing.contains(&id)
    }

    /// Voter or learner.
    pub fn contains(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }

    /// Everyone who votes, in either configuration.
    pub fn all_voters(&self) -> BTreeSet<NodeId> {
        self.voters.union(&self.outgoing).copied().collect()
    }

    /// Everyone: voters of both configurations and learners.
    pub fn all(&self) -> BTreeSet<NodeId> {
        let mut all = self.all_voters();
        all.extend(&self.learners);
        all
    }

    /// `true` if `yes` holds a majority of every configuration in force.
    pub fn has_quorum(&self, yes: &BTreeSet<NodeId>) -> bool {
        let majority = |set: &BTreeSet<NodeId>| set.is_empty() || set.intersection(yes).count() > set.len() / 2;
        !self.voters.is_empty() && majority(&self.voters) && majority(&self.outgoing)
    }

    /// Highest index that a majority of every configuration has, given what
    /// each voter has (`matched`).
    pub fn committed(&self, matched: impl Fn(NodeId) -> u64) -> u64 {
        let one = |set: &BTreeSet<NodeId>| {
            if set.is_empty() {
                return u64::MAX;
            }
            let mut m: Vec<u64> = set.iter().map(|id| matched(*id)).collect();
            m.sort_unstable_by(|a, b| b.cmp(a));
            m[set.len() / 2]
        };
        if self.voters.is_empty() {
            return 0;
        }
        one(&self.voters).min(one(&self.outgoing))
    }

    /// Encoded for a config entry: three counted lists of little-endian ids.
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        for set in [&self.voters, &self.outgoing, &self.learners] {
            b.put_u32_le(set.len() as u32);
            for id in set {
                b.put_u64_le(*id);
            }
        }
        b.freeze()
    }

    /// Decodes [`Membership::encode`]. `None` if malformed.
    pub fn decode(mut data: &[u8]) -> Option<Self> {
        let mut sets: [BTreeSet<NodeId>; 3] = Default::default();
        for set in &mut sets {
            let n = u32::from_le_bytes(data.get(..4)?.try_into().ok()?) as usize;
            data = &data[4..];
            for _ in 0..n {
                set.insert(u64::from_le_bytes(data.get(..8)?.try_into().ok()?));
                data = &data[8..];
            }
        }
        let [voters, outgoing, learners] = sets;
        data.is_empty().then_some(Membership { voters, outgoing, learners })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn joint_majorities_need_both_sides() {
        let m = Membership { voters: set(&[1, 2, 4]), outgoing: set(&[1, 2, 3]), learners: set(&[5]) };
        assert!(m.has_quorum(&set(&[1, 2])));
        assert!(!m.has_quorum(&set(&[1, 4])), "majority of the new voters only");
        assert!(!m.has_quorum(&set(&[1, 3])), "majority of the old voters only");
        assert!(!m.has_quorum(&set(&[5, 1])), "learners do not count");
        // matched: 1→9, 2→5, 3→8, 4→2
        let matched = |id| [0, 9, 5, 8, 2][id as usize];
        assert_eq!(m.committed(matched), 5, "new side: 9,5,2 → 5; old side: 9,8,5 → 8; both → 5");
        assert_eq!(Membership::of_voters([1, 2, 3]).committed(matched), 8);
    }

    #[test]
    fn encoding_round_trips() {
        let m = Membership { voters: set(&[1, 2, 4]), outgoing: set(&[1, 2, 3]), learners: set(&[5]) };
        assert_eq!(Membership::decode(&m.encode()), Some(m.clone()));
        assert_eq!(Membership::decode(&m.encode()[..10]), None);
    }
}

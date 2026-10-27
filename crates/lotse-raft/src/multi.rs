//! Many Raft groups on one node, with messages batched per node pair.
//!
//! A broker hosts one group per partition, queue and priority level, so
//! thousands of groups. Sending each group's heartbeat on its own would cost
//! a network write per group and tick; [`MultiRaft::ready`] instead collects
//! the output of every group and bundles all messages for the same node into
//! one list, so the driver sends one frame per destination per round.

use std::collections::BTreeMap;

use crate::{Message, NodeId, RaftGroup, Ready, Send};

/// A partition, queue or priority level: one Raft group.
pub type GroupId = u64;

/// Ready of one group, without its messages (those are batched).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupReady {
    /// Group.
    pub group: GroupId,
    /// Persist and apply this; `messages` is always empty here.
    pub ready: Ready,
}

/// Output of one round over all groups.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MultiReady {
    /// Per group: what to persist and apply. Do all of it before sending.
    pub groups: Vec<GroupReady>,
    /// Per destination node: every message for it, tagged with its group.
    pub outbound: BTreeMap<NodeId, Vec<(GroupId, Send)>>,
}

/// All groups of one node.
#[derive(Debug, Default)]
pub struct MultiRaft {
    groups: BTreeMap<GroupId, RaftGroup>,
}

impl MultiRaft {
    /// No groups yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or replaces a group.
    pub fn insert(&mut self, id: GroupId, group: RaftGroup) {
        self.groups.insert(id, group);
    }

    /// Removes a group.
    pub fn remove(&mut self, id: GroupId) -> Option<RaftGroup> {
        self.groups.remove(&id)
    }

    /// One group.
    pub fn get(&self, id: GroupId) -> Option<&RaftGroup> {
        self.groups.get(&id)
    }

    /// One group, mutable (to propose).
    pub fn get_mut(&mut self, id: GroupId) -> Option<&mut RaftGroup> {
        self.groups.get_mut(&id)
    }

    /// Number of groups.
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// `true` without groups.
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Ticks every group.
    pub fn tick(&mut self) {
        self.groups.values_mut().for_each(RaftGroup::tick);
    }

    /// Delivers a batch received from `from`. Messages for unknown groups
    /// are dropped (the group may have been removed).
    pub fn step_batch(&mut self, from: NodeId, batch: impl IntoIterator<Item = (GroupId, Message)>) {
        for (g, msg) in batch {
            if let Some(group) = self.groups.get_mut(&g) {
                group.step(from, msg);
            }
        }
    }

    /// Collects the work of every group that has some.
    pub fn ready(&mut self) -> MultiReady {
        let mut out = MultiReady::default();
        for (id, g) in self.groups.iter_mut() {
            if !g.has_ready() {
                continue;
            }
            let mut ready = g.ready();
            for s in std::mem::take(&mut ready.messages) {
                out.outbound.entry(s.to).or_default().push((*id, s));
            }
            if !(ready.is_empty()) {
                out.groups.push(GroupReady { group: *id, ready });
            }
        }
        out
    }

    /// Tells every group that its last ready is durable.
    pub fn advance(&mut self) {
        self.groups.values_mut().for_each(RaftGroup::advance);
    }
}

//! What a node's Raft groups do and when its loop falls behind, for the log
//! (ADR 0139). A group that loses its leader for seconds, as seen on loaded
//! CI runners, shows here: which node stepped down or campaigned, why, and
//! whether the node's loop was late with its ticks or slow to persist.
//!
//! Role, term and leader changes are logged at `debug` under the target
//! `lotse::raft` (a cluster with thousands of groups would fill an `info` log
//! at every start), the loop at `debug` under `lotse::stall`, and at `warn`
//! once it is late by half an election timeout; `LOTSE_LOG` chooses, for
//! instance `info,lotse::raft=debug,lotse::stall=debug`.

use std::collections::BTreeMap;
use std::time::Duration;

use lotse_raft::{NodeId, Role};

use crate::wire::GroupId;

/// A group's role, term and leader as this node sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    pub role: Role,
    pub term: u64,
    pub leader: Option<NodeId>,
}

/// What ran just before a change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    /// The node's tick.
    Tick,
    /// Messages from this node.
    Message(NodeId),
}

/// Why a group changed from `before` to `after`, as far as `cause` tells.
pub(crate) fn reason(before: Seen, after: Seen, cause: Cause) -> &'static str {
    use Role::*;
    match (cause, before.role, after.role) {
        (Cause::Tick, Leader, Follower) => "stepped down: no quorum heard within an election timeout (check-quorum)",
        (Cause::Tick, Follower, PreCandidate) => "election timeout: no leader heard, asking for pre-votes",
        (Cause::Tick, Follower | PreCandidate | Candidate, Candidate) => "election timeout: campaigning",
        (Cause::Tick, PreCandidate | Candidate, PreCandidate) => "election timeout: no majority, asking again",
        (_, PreCandidate, Candidate) => "pre-vote won: campaigning",
        (_, Candidate, Leader) => "election won",
        (Cause::Message(_), Leader, Follower) if after.term > before.term => "stepped down: a higher term",
        (Cause::Message(_), Leader, Follower) => "stepped down: leadership handed over",
        (Cause::Message(_), PreCandidate | Candidate, Follower) if after.leader.is_some() => "a leader was heard",
        (Cause::Message(_), PreCandidate | Candidate, Follower) => "a higher term: back to following",
        (Cause::Message(_), Follower, Candidate) => "asked to campaign at once (leadership handed over)",
        (_, Follower, Follower) if after.term > before.term => "a higher term",
        (_, Follower, Follower) if after.leader.is_some() => "a leader was heard",
        (_, Follower, Follower) => "lost its leader",
        _ => "changed",
    }
}

/// The groups as last logged, to log each change once.
#[derive(Default)]
pub(crate) struct Elections {
    seen: BTreeMap<GroupId, Seen>,
}

impl Elections {
    /// Logs what changed in `group` since it was last seen.
    pub(crate) fn observe(&mut self, node: NodeId, group: GroupId, now: Seen, cause: Cause) {
        let Some(before) = self.seen.insert(group, now) else { return };
        if before == now {
            return;
        }
        let from = match cause {
            Cause::Tick => None,
            Cause::Message(n) => Some(n),
        };
        tracing::debug!(
            target: "lotse::raft",
            node,
            group,
            role = ?now.role,
            term = now.term,
            leader = ?now.leader,
            was_role = ?before.role,
            was_term = before.term,
            was_leader = ?before.leader,
            after_message_from = ?from,
            "{}",
            reason(before, now, cause)
        );
    }

    /// Forgets the groups this node no longer runs.
    pub(crate) fn retain(&mut self, keep: impl Fn(GroupId) -> bool) {
        self.seen.retain(|g, _| keep(*g));
    }
}

/// Logs that the node's loop came late to its tick: by `late` beyond the
/// tick it should have run, against an election timeout of `election`.
pub(crate) fn tick_late(node: NodeId, late: Duration, tick: Duration, election: Duration) {
    if late >= election / 2 {
        tracing::warn!(
            target: "lotse::stall",
            node,
            late_ms = late.as_millis() as u64,
            election_ms = election.as_millis() as u64,
            "the node's loop is late by half an election timeout or more: its groups may lose their leaders"
        );
    } else if late >= tick * 2 {
        tracing::debug!(target: "lotse::stall", node, late_ms = late.as_millis() as u64, "tick late");
    }
}

/// Logs a slow step of the node's loop, `what` taking `took`.
pub(crate) fn slow(node: NodeId, what: &'static str, took: Duration, tick: Duration, election: Duration) {
    if took >= election / 2 {
        tracing::warn!(
            target: "lotse::stall",
            node,
            took_ms = took.as_millis() as u64,
            election_ms = election.as_millis() as u64,
            "{what} took half an election timeout or more"
        );
    } else if took >= tick * 2 {
        tracing::debug!(target: "lotse::stall", node, took_ms = took.as_millis() as u64, "{what} slow");
    }
}

#[cfg(test)]
mod tests;

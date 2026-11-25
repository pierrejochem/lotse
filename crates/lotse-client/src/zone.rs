//! Reads in this client's zone (the zone reads design): the indexes of the
//! metadata and coordinator groups this client has seen, which a node in
//! the zone must have applied before it answers a read, and the choice of
//! that node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use lotse_proto::{ErrorCode, GroupIndex, LeaderHint, Response};

/// The metadata group's id.
const METADATA: u64 = 1;
/// The coordinator group's id.
const COORDINATOR: u64 = 2;

/// The highest index of each group named in an answer's `applied`, shared
/// by a client and its connections. An index only rises: a follower
/// further behind than an answer seen before must not make a later read
/// accept older state.
#[derive(Clone, Debug)]
pub(crate) struct Indexes(Arc<Mutex<BTreeMap<u64, u64>>>);

impl Default for Indexes {
    /// Both groups at 0: a read names them even before any answer did, so
    /// a node can tell a client that saw nothing from one that sends no
    /// tag.
    fn default() -> Self {
        Indexes(Arc::new(Mutex::new(BTreeMap::from([(METADATA, 0), (COORDINATOR, 0)]))))
    }
}

impl Indexes {
    /// Takes in what an answer says its node had applied.
    pub(crate) fn raise(&self, applied: &[GroupIndex]) {
        if applied.is_empty() {
            return;
        }
        let mut known = self.0.lock().expect("indexes lock");
        for e in applied {
            let index = known.entry(e.group).or_insert(0);
            *index = (*index).max(e.index);
        }
    }

    /// What a zone read asks for: every group's index, in group order.
    pub(crate) fn snapshot(&self) -> Vec<GroupIndex> {
        let known = self.0.lock().expect("indexes lock");
        known.iter().map(|(&group, &index)| GroupIndex { group, index }).collect()
    }
}

/// The lowest-id node of `candidates` in `zone`, as `zones` has it: one
/// fixed choice, so a client's reads meet the same node's connection.
pub(crate) fn zone_node(
    zone: &str,
    candidates: impl IntoIterator<Item = u32>,
    zones: &BTreeMap<u32, String>,
) -> Option<u32> {
    candidates.into_iter().filter(|n| zones.get(n).is_some_and(|z| z == zone)).min()
}

/// Whether a zone node's answer to a zone read is no answer: an error, of
/// the whole read or of a part. The leader is asked instead, since a zone
/// node refuses what it has not caught up on, and an older node refuses
/// leader-only reads, with the same NOT_LEADER the leader never gives.
pub(crate) fn refused(resp: &Response) -> bool {
    let error = match resp {
        Response::DescribeConfigs(d) => {
            return d.error != ErrorCode::NONE || d.resources.iter().any(|r| r.error != ErrorCode::NONE);
        }
        Response::OffsetFetch(o) => {
            return o.resources.iter().flat_map(|r| &r.partitions).any(|p| p.error != ErrorCode::NONE);
        }
        Response::DescribeNodes(d) => d.error,
        Response::DescribeUsers(d) => d.error,
        Response::DescribeTokens(d) => d.error,
        Response::DescribeCertificates(d) => d.error,
        Response::DescribeCertificateRequests(d) => d.error,
        Response::DescribeUpgrade(d) => d.error,
        Response::DescribeTenants(d) => d.error,
        Response::DescribeRoleBindings(d) => d.error,
        _ => return true,
    };
    error != ErrorCode::NONE
}

/// The metadata leader a refused admin read names, if any.
pub(crate) fn leader_hint(resp: &Response) -> Option<LeaderHint> {
    match resp {
        Response::DescribeConfigs(d) => d.leader_hint,
        Response::DescribeNodes(d) => d.leader_hint,
        Response::DescribeUsers(d) => d.leader_hint,
        Response::DescribeTokens(d) => d.leader_hint,
        Response::DescribeCertificates(d) => d.leader_hint,
        Response::DescribeCertificateRequests(d) => d.leader_hint,
        Response::DescribeUpgrade(d) => d.leader_hint,
        Response::DescribeTenants(d) => d.leader_hint,
        Response::DescribeRoleBindings(d) => d.leader_hint,
        _ => None,
    }
}

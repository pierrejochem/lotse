//! Storage quotas (ADR 0147): what each limit of the metadata state uses,
//! counted by the metadata group's leader from the group sizes it collects
//! from every node, and which limits that makes full or clear.

use std::collections::BTreeMap;

use crate::metadata::{Change, MetadataState};
use crate::wire::GroupId;

/// What one count found, in bytes, every voter replica counted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Usage {
    /// Each limit of the state, by tenant and prefix.
    pub(crate) per_limit: BTreeMap<(String, String), u64>,
    /// Each tenant with runtime resources, with or without a limit.
    pub(crate) per_tenant: BTreeMap<String, u64>,
}

/// What each limit and each tenant uses: for every runtime stream
/// partition and queue, its group's size times the number of voters the
/// state lists for it. Learners and replicas a node runs until a move off
/// it settles are not voters, so the cluster's own moves never count. A
/// resource counts against its tenant and every prefix limit its name
/// starts with. `None` while some group has no size: a count from missing
/// data could clear a full limit.
pub(crate) fn count(state: &MetadataState, sizes: &BTreeMap<GroupId, u64>) -> Option<Usage> {
    let mut usage = Usage::default();
    let mut add = |tenant: &str, name: &str, bytes: u64| {
        *usage.per_tenant.entry(tenant.to_string()).or_default() += bytes;
        for (t, p) in state.quotas.keys() {
            if t == tenant && name.starts_with(p.as_str()) {
                *usage.per_limit.entry((t.clone(), p.clone())).or_default() += bytes;
            }
        }
    };
    for s in state.streams.values() {
        let mut bytes = 0u64;
        for (g, voters) in &s.partitions {
            bytes = bytes.saturating_add(sizes.get(g)?.saturating_mul(voters.len() as u64));
        }
        add(&s.tenant, &s.name, bytes);
    }
    for q in state.queues.values() {
        add(&q.tenant, &q.name, sizes.get(&q.group)?.saturating_mul(q.voters.len() as u64));
    }
    for key in state.quotas.keys() {
        usage.per_limit.entry(key.clone()).or_default();
    }
    Some(usage)
}

/// The records a count calls for: a limit becomes full once its usage
/// reaches it, and clear once usage is below 95 % of it, so a tenant at
/// the edge does not flip at every count. Nothing for a limit that stays
/// as it is.
pub(crate) fn changes(state: &MetadataState, usage: &Usage) -> Vec<Change> {
    let mut out = Vec::new();
    for ((tenant, prefix), q) in &state.quotas {
        let used = usage.per_limit.get(&(tenant.clone(), prefix.clone())).copied().unwrap_or(0);
        let full = if q.full { u128::from(used) * 100 >= u128::from(q.bytes) * 95 } else { used >= q.bytes };
        if full != q.full {
            out.push(Change::QuotaFull { tenant: tenant.clone(), prefix: prefix.clone(), full });
        }
    }
    out
}

#[cfg(test)]
mod tests;

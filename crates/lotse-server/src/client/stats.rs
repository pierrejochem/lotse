//! What a node counts for `DescribeMetrics`: traffic and connections (in
//! the tasks that own them, so atomics), and per group what the main loop
//! sees: acknowledged produces, fetches, deliveries and acks, and how long
//! produces took to be acknowledged.
//!
//! Counters only grow while the node runs; rates are the difference of two
//! answers, which the reader (the console) takes. A group's counters grow
//! only while this node leads it, so summing the answers of all nodes
//! counts every produce once.
//!
//! Nothing here decides anything: metrics are read, never acted on.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use lotse_proto::{
    CommittedOffset, ConsumerMetrics, DescribeMetricsResponse, GroupMemberMetrics, GroupMetrics, HostMetrics,
    LATENCY_BUCKETS, PartitionMetrics, QueueMetrics, ReplicaProgress, Response, TxnResource,
};
use lotse_raft::NodeId;
use lotse_rt::{HostStats, Instant, Runtime};

use super::{Clients, ClusterView, Outbox, ids, queues};
use crate::auth::Principal;
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;
use lotse_auth::roles::Operation;

/// Traffic and connections, counted by the tasks that own them.
#[derive(Debug, Default)]
pub(crate) struct NetStats {
    pub(crate) client_in: AtomicU64,
    pub(crate) client_out: AtomicU64,
    pub(crate) peer_in: AtomicU64,
    pub(crate) peer_out: AtomicU64,
    pub(crate) connections: AtomicU64,
}

impl NetStats {
    /// Counts `n` more of `counter`.
    pub(crate) fn add(counter: &AtomicU64, n: usize) {
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// What the node itself is, for `DescribeMetrics`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NodeView<'a> {
    pub(crate) id: NodeId,
    pub(crate) started: Instant,
    pub(crate) net: &'a NetStats,
    /// The host as last read (every few seconds).
    pub(crate) host: HostStats,
}

static NO_NET: NetStats = NetStats {
    client_in: AtomicU64::new(0),
    client_out: AtomicU64::new(0),
    peer_in: AtomicU64::new(0),
    peer_out: AtomicU64::new(0),
    connections: AtomicU64::new(0),
};

impl Default for NodeView<'_> {
    fn default() -> Self {
        NodeView { id: 0, started: Instant::ZERO, net: &NO_NET, host: HostStats::default() }
    }
}

/// Counted per group while this node leads it.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct GroupCounters {
    pub(crate) produce_bytes: u64,
    pub(crate) produce_records: u64,
    pub(crate) fetch_bytes: u64,
    pub(crate) delivered: u64,
    pub(crate) acked: u64,
}

/// Everything the main loop counts.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    pub(crate) groups: BTreeMap<GroupId, GroupCounters>,
    latency: [u64; LATENCY_BUCKETS],
}

impl Stats {
    /// A produce of `bytes` and `records` to `group`, acknowledged `took`
    /// after it arrived.
    pub(crate) fn produced(&mut self, group: GroupId, bytes: u64, records: u64, took: Duration) {
        let c = self.groups.entry(group).or_default();
        c.produce_bytes += bytes;
        c.produce_records += records;
        self.latency[bucket(took)] += 1;
    }

    pub(crate) fn group(&mut self, group: GroupId) -> &mut GroupCounters {
        self.groups.entry(group).or_default()
    }
}

/// The histogram bucket of `took`: the first `i` with `took < 2^i` ms.
fn bucket(took: Duration) -> usize {
    let ms = took.as_millis();
    (0..LATENCY_BUCKETS - 1).find(|&i| ms < 1u128 << i).unwrap_or(LATENCY_BUCKETS - 1)
}

impl Clients {
    /// Answers `DescribeMetrics` from what this node has and counts.
    pub(super) fn describe_metrics<R: Runtime, S: StateMachine>(
        &self,
        outbox: &Outbox,
        id: u32,
        view: &ClusterView<'_>,
        groups: &BTreeMap<GroupId, Replica<R, S>>,
        who: &Principal,
    ) {
        let node = &view.node;
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let h = node.host;
        let host = HostMetrics {
            cpu_permille: h.cpu.map(|c| (c.clamp(0.0, 1.0) * 1000.0).round() as u16),
            memory_bytes: h.memory_bytes,
            memory_limit_bytes: h.memory_limit_bytes,
            disk_free_bytes: h.disk_free_bytes,
            disk_total_bytes: h.disk_total_bytes,
        };
        let mut resp = DescribeMetricsResponse {
            node_id: node.id as u32,
            version: env!("CARGO_PKG_VERSION").into(),
            uptime_ms: (view.now - node.started).as_millis() as u64,
            metadata_group: view.admin.metadata.unwrap_or(0),
            coordinator_group: view.coordinator.unwrap_or(0),
            connections: load(&node.net.connections) as u32,
            client_in_bytes: load(&node.net.client_in),
            client_out_bytes: load(&node.net.client_out),
            peer_in_bytes: load(&node.net.peer_in),
            peer_out_bytes: load(&node.net.peer_out),
            commit_latency: self.stats.latency.to_vec(),
            host,
            ..DescribeMetricsResponse::default()
        };
        for (&g, replica) in groups {
            let raft = replica.raft_ref();
            let (resource_id, partition) = view.resource_of(g).unwrap_or((0, 0));
            let c = self.stats.groups.get(&g).copied().unwrap_or_default();
            // Raft index = offset + 1: a replica holding index i holds offsets below i.
            let replicas = raft
                .matched()
                .map(|m| m.into_iter().map(|(n, i)| ReplicaProgress { node_id: n as u32, log_end: i }).collect())
                .unwrap_or_default();
            let hw = raft.commit();
            resp.partitions.push(PartitionMetrics {
                group: g,
                resource_id,
                partition,
                // Led only while a quorum answers: a deposed leader that has
                // not heard of the new term yet does not claim it (ADR 0095).
                leader: raft.leads_with_quorum(),
                term: raft.term(),
                high_watermark: hw,
                last_stable: replica.txns().last_stable_offset(replica.applied()).min(hw),
                log_start: replica.log().start_offset(),
                log_end: replica.log().next_offset(),
                size_bytes: replica.log().size_bytes(),
                produce_bytes: c.produce_bytes,
                produce_records: c.produce_records,
                fetch_bytes: c.fetch_bytes,
                replicas,
            });
        }
        // Every queue this node leads with a quorum behind it, served
        // (subscribed to) or not.
        for qc in view.queues {
            let Some(replica) = groups.get(&qc.group).filter(|r| r.raft_ref().leads_with_quorum()) else { continue };
            let Some(q) = replica.queue() else { continue };
            let serving = self.queues.get(&qc.group).filter(|s| s.term == replica.raft_ref().term());
            let in_flight = serving.map_or(0, |s| s.dispatcher.in_flight_count());
            let limit = queues::deliverable(replica);
            let ready = q.unsettled_in(q.floor(), limit.max(q.floor())).saturating_sub(in_flight);
            let c = self.stats.groups.get(&qc.group).copied().unwrap_or_default();
            let consumers = serving.map_or_else(Vec::new, |s| {
                s.dispatcher
                    .subscriptions()
                    .map(|(sub, conn)| ConsumerMetrics {
                        subscription_id: sub,
                        peer: s.conns.get(&conn).map(|o| o.peer.clone()).unwrap_or_default(),
                        in_flight: s.dispatcher.in_flight_of(sub).len() as u32,
                        credit: s.dispatcher.credit_of(sub),
                    })
                    .collect()
            });
            resp.queues.push(QueueMetrics {
                resource_id: qc.resource_id,
                ready,
                in_flight,
                in_open_txns: q.in_open_transactions(),
                delivered: c.delivered,
                acked: c.acked,
                consumers,
            });
        }
        // Consumer groups, from the coordinator's leader.
        if let Some(coord) = view.coordinator.and_then(|g| groups.get(&g))
            && coord.raft_ref().leads_with_quorum()
        {
            let state = coord.coordinator().groups();
            for (group_id, g) in &state.groups {
                let members = g
                    .members
                    .iter()
                    .map(|(member_id, m)| {
                        let mut owned: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
                        for (r, p) in &m.assigned {
                            owned.entry(*r).or_default().push(*p);
                        }
                        let owned =
                            owned.into_iter().map(|(resource_id, partitions)| TxnResource { resource_id, partitions });
                        GroupMemberMetrics { member_id: member_id.clone(), owned: owned.collect() }
                    })
                    .collect();
                let offsets = state
                    .offsets
                    .iter()
                    .filter(|((g, _, _), _)| g == group_id)
                    .map(|((_, r, p), c)| CommittedOffset { resource_id: *r, partition: *p, offset: c.offset })
                    .collect();
                let (tenant, group_id) = ids::split(group_id);
                resp.groups.push(GroupMetrics {
                    group_id: group_id.to_string(),
                    tenant: tenant.to_string(),
                    epoch: g.epoch,
                    members,
                    offsets,
                });
            }
        }
        // Only what `who` may see: resources by their tenant and role, and
        // a consumer group only if every resource it touches is visible
        // (one that touches none, only to who sees the whole cluster).
        // The cluster's own groups (metadata, coordinator) belong to no
        // resource: only cluster admins and viewers see them.
        let visible = |r: u64| match view.access.resources.get(&r) {
            Some((name, tenant)) => view.access.may(who, Operation::Describe, tenant, name),
            None => view.access.sees_cluster(who),
        };
        resp.partitions.retain(|p| visible(p.resource_id));
        resp.queues.retain(|q| visible(q.resource_id));
        resp.groups.retain(|g| {
            // The group itself, by its tenant and id (ADR 0062).
            if !view.access.may(who, Operation::Describe, &g.tenant, &g.group_id) {
                return false;
            }
            let owned = g.members.iter().flat_map(|m| m.owned.iter().map(|t| t.resource_id));
            let mut touched = owned.chain(g.offsets.iter().map(|o| o.resource_id)).peekable();
            if touched.peek().is_none() {
                return view.access.sees_cluster(who);
            }
            touched.all(visible)
        });
        outbox.push(id, &Response::DescribeMetrics(Box::new(resp)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latencies_fall_into_power_of_two_buckets() {
        let ms = Duration::from_millis;
        assert_eq!(bucket(ms(0)), 0);
        assert_eq!(bucket(ms(1)), 1, "1 ms is not under 1 ms");
        assert_eq!(bucket(ms(3)), 2);
        assert_eq!(bucket(ms(4)), 3);
        assert_eq!(bucket(ms(1 << 15)), LATENCY_BUCKETS - 1);
        assert_eq!(bucket(Duration::from_secs(3_600)), LATENCY_BUCKETS - 1);
    }
}

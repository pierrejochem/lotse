//! What a node tells the people who run it, outside the native protocol
//! (ADR 0117): its metrics as OpenMetrics text for Prometheus, whether it
//! is ready to serve, and whether it may be stopped without leaving a group
//! without a quorum.
//!
//! Free of I/O: the node gathers an [`Observation`] in its main loop and
//! asks the other nodes for their answers; this module only judges and
//! renders them. Nothing here decides anything inside the cluster.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use lotse_proto::{ConsumerState, DescribeMetricsResponse, LATENCY_BUCKETS};
use lotse_raft::{Membership, NodeId};
use lotse_rt::{CertInfo, PeerIdentity};

use crate::wire::GroupId;

/// Who asks for a node's metrics (ADR 0117).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scraper {
    /// Plain HTTP on a loopback address: whoever can reach it is on the
    /// node's machine.
    Local,
    /// HTTPS: what the other end's certificate proved, if it showed one.
    Tls {
        /// Who the certificate names.
        identity: Option<PeerIdentity>,
        /// The certificate, to check against revocations (ADR 0090).
        cert: Option<CertInfo>,
    },
}

/// What a resource is called, for labels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceName {
    /// `stream`, `queue`, `metadata` or `coordinator`.
    pub kind: &'static str,
    /// Its tenant; empty for the cluster's own groups.
    pub tenant: String,
    /// Its name; empty for the cluster's own groups.
    pub name: String,
}

/// Where one group this node hosts stands, as the node sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupState {
    /// This node leads it, and a quorum answered lately.
    Leads,
    /// This node follows a leader it hears and keeps up with (ADR 0116).
    Follows,
    /// This node leads it without a quorum behind it.
    LeadsWithoutQuorum,
    /// This node hears no leader.
    NoLeader,
    /// This node hears its leader but has not caught up with what it said
    /// was committed within `follower_max_lag` (ADR 0116).
    Lagging,
}

impl GroupState {
    /// Where a replica stands: whether this node leads the group, leads it
    /// with a quorum that answered lately, hears its leader, and lags
    /// behind it (ADR 0116).
    pub fn of(leads: bool, with_quorum: bool, hears_leader: bool, lagging: bool) -> GroupState {
        match () {
            () if leads && with_quorum => GroupState::Leads,
            () if leads => GroupState::LeadsWithoutQuorum,
            () if !hears_leader => GroupState::NoLeader,
            () if lagging => GroupState::Lagging,
            () => GroupState::Follows,
        }
    }

    /// Whether this replica can serve what it is there for.
    pub fn ready(&self) -> bool {
        matches!(self, GroupState::Leads | GroupState::Follows)
    }

    fn reason(&self) -> &'static str {
        match self {
            GroupState::Leads | GroupState::Follows => "ready",
            GroupState::LeadsWithoutQuorum => "leads without a quorum",
            GroupState::NoLeader => "hears no leader",
            GroupState::Lagging => "has not caught up with its leader",
        }
    }
}

/// One group this node hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHealth {
    /// Its members, as this node's replica knows them.
    pub membership: Membership,
    /// Where it stands.
    pub state: GroupState,
}

/// What a node sees of itself at one moment (ADR 0117).
#[derive(Debug, Clone, Default)]
pub struct Observation {
    /// The node.
    pub node_id: NodeId,
    /// Its `DescribeMetrics` answer, unfiltered: everything it has.
    pub metrics: DescribeMetricsResponse,
    /// Names of the resources it knows, by id.
    pub resources: BTreeMap<u64, ResourceName>,
    /// Every group it hosts.
    pub groups: BTreeMap<GroupId, GroupHealth>,
    /// Client certificate requests waiting for the CA holder, as this
    /// node's replica of the metadata group has applied them (ADR 0119);
    /// `None` on a node without one.
    pub certificate_requests_pending: Option<u64>,
    /// Access tokens that have not expired, as this node's replica of the
    /// metadata group has applied them (ADR 0133); `None` on a node without
    /// one.
    pub tokens: Option<u64>,
    /// Sign-ins with an access token on this node since it started that
    /// succeeded (ADR 0133).
    pub token_sign_ins_ok: u64,
    /// Those that failed, whatever the reason.
    pub token_sign_ins_failed: u64,
    /// Whether the scraper that asked may read the metrics: they name every
    /// tenant's resources (ADR 0021).
    pub may_scrape: bool,
}

impl Observation {
    /// The groups this node hosts that are not ready, with why; empty when
    /// the node is ready to serve (`/readyz`).
    pub fn not_ready(&self) -> Vec<(GroupId, &'static str)> {
        self.groups.iter().filter(|(_, h)| !h.state.ready()).map(|(g, h)| (*g, h.state.reason())).collect()
    }
}

/// A group that stopping a node would leave without a quorum (ADR 0117).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Critical {
    /// The group.
    pub group: GroupId,
    /// Its voters.
    pub voters: BTreeSet<NodeId>,
    /// The voters besides the node that its leader heard lately; `None`
    /// when no node reported leading it with a quorum.
    pub responsive: Option<BTreeSet<NodeId>>,
}

/// The groups that would lose their quorum if node `me` stopped, judged
/// from the members `me`'s replicas know (`mine`) and the answers of the
/// nodes (`reports`): for every group `me` votes in, its leader's view of
/// who answered lately, without `me`, must still be a quorum. A group none
/// of the answers leads with a quorum counts as critical too: stopping
/// makes it no better, and a rolling restart should wait for it.
///
/// A group whose only voter is `me` is left out: it stops with the node
/// whenever that happens, and waiting would not help (ADR 0117).
pub fn quorum_critical(
    me: NodeId,
    mine: &BTreeMap<GroupId, Membership>,
    reports: &[DescribeMetricsResponse],
) -> Vec<Critical> {
    let mut critical = Vec::new();
    for (&g, membership) in mine {
        let voters = membership.all_voters();
        if !voters.contains(&me) || voters.len() == 1 {
            continue;
        }
        // The newest term's leader, should a deposed one still answer.
        let leader =
            reports.iter().filter_map(|r| r.responsive.iter().find(|x| x.group == g)).max_by_key(|heard| heard.term);
        let responsive = leader.map(|heard| {
            heard.nodes.iter().map(|n| NodeId::from(*n)).filter(|n| *n != me && voters.contains(n)).collect()
        });
        if !responsive.as_ref().is_some_and(|r| membership.has_quorum(r)) {
            critical.push(Critical { group: g, voters, responsive });
        }
    }
    critical
}

/// The content type of [`render`]'s output.
pub const OPENMETRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// A node's observation as OpenMetrics text, for Prometheus (ADR 0117).
/// Every name starts with `lotse_`; counters run since the node started,
/// and a group's counters grow only while this node leads it, as in
/// `DescribeMetrics` (ADR 0036).
pub fn render(obs: &Observation) -> String {
    let m = &obs.metrics;
    let mut out = Out::default();
    let node = [("node", obs.node_id.to_string())];
    out.family("lotse_node", "info", "This node and its version.", "");
    out.sample("lotse_node_info", &[("node", obs.node_id.to_string()), ("version", m.version.clone())], 1);
    out.family("lotse_node_uptime_seconds", "gauge", "Time since the node started.", "seconds");
    out.sample_f("lotse_node_uptime_seconds", &node, m.uptime_ms as f64 / 1000.0);
    out.family("lotse_node_ready", "gauge", "1 while every group on the node is ready (/readyz).", "");
    out.sample("lotse_node_ready", &node, u64::from(obs.not_ready().is_empty()));
    out.family("lotse_node_connections", "gauge", "Open client connections.", "");
    out.sample("lotse_node_connections", &node, u64::from(m.connections));
    out.family(
        "lotse_certificate_requests_pending",
        "gauge",
        "Client certificate requests waiting for the CA holder, as this node has applied them.",
        "",
    );
    if let Some(n) = obs.certificate_requests_pending {
        out.sample("lotse_certificate_requests_pending", &node, n);
    }
    out.family("lotse_tokens", "gauge", "Access tokens that have not expired, as this node has applied them.", "");
    if let Some(n) = obs.tokens {
        out.sample("lotse_tokens", &node, n);
    }
    out.family("lotse_token_sign_ins", "counter", "Sign-ins with an access token on this node, by result.", "");
    for (result, n) in [("ok", obs.token_sign_ins_ok), ("failed", obs.token_sign_ins_failed)] {
        out.sample("lotse_token_sign_ins_total", &[("node", obs.node_id.to_string()), ("result", result.into())], n);
    }
    for (name, help, v) in [
        ("lotse_client_received_bytes", "Bytes received from clients.", m.client_in_bytes),
        ("lotse_client_sent_bytes", "Bytes sent to clients.", m.client_out_bytes),
        ("lotse_peer_received_bytes", "Bytes received from other nodes.", m.peer_in_bytes),
        ("lotse_peer_sent_bytes", "Bytes sent to other nodes.", m.peer_out_bytes),
    ] {
        out.family(name, "counter", help, "bytes");
        out.sample(&format!("{name}_total"), &node, v);
    }
    // Bucket i counts produces acknowledged in under 2^i ms, the last all
    // slower ones: cumulative, with the last as +Inf.
    out.family(
        "lotse_commit_latency_seconds",
        "histogram",
        "Time from a produce's arrival to its acknowledgement, at the leader.",
        "seconds",
    );
    let mut sum = 0;
    for (i, n) in m.commit_latency.iter().enumerate().take(LATENCY_BUCKETS) {
        sum += n;
        let le = if i + 1 == m.commit_latency.len().min(LATENCY_BUCKETS) {
            "+Inf".to_string()
        } else {
            seconds((1u64 << i) as f64 / 1000.0)
        };
        out.sample("lotse_commit_latency_seconds_bucket", &[("node", obs.node_id.to_string()), ("le", le)], sum);
    }
    let h = &m.host;
    let host: [(&str, &str, &str, Option<f64>); 5] = [
        (
            "lotse_host_cpu_ratio",
            "CPU use of the host over the last 5 s.",
            "ratio",
            h.cpu_permille.map(|c| f64::from(c) / 1000.0),
        ),
        ("lotse_host_memory_bytes", "Memory the node uses.", "bytes", h.memory_bytes.map(|v| v as f64)),
        (
            "lotse_host_memory_limit_bytes",
            "Memory available to the node.",
            "bytes",
            h.memory_limit_bytes.map(|v| v as f64),
        ),
        ("lotse_host_disk_free_bytes", "Free space of the data volume.", "bytes", h.disk_free_bytes.map(|v| v as f64)),
        ("lotse_host_disk_total_bytes", "Size of the data volume.", "bytes", h.disk_total_bytes.map(|v| v as f64)),
    ];
    for (name, help, unit, v) in host {
        out.family(name, "gauge", help, unit);
        if let Some(v) = v {
            out.sample_f(name, &node, v);
        }
    }
    // The free space limit (ADR 0122): 0 for none.
    out.family(
        "lotse_disk_free_limit_bytes",
        "gauge",
        "Below this much free space on its data volume the node takes no new data in the groups it votes in.",
        "bytes",
    );
    if let Some(d) = m.disk {
        out.sample("lotse_disk_free_limit_bytes", &node, d.free_limit_bytes);
    }
    out.family("lotse_disk_low", "gauge", "1 while the node is below its free space limit.", "");
    if let Some(d) = m.disk {
        out.sample("lotse_disk_low", &node, u64::from(d.low));
    }

    let labels = |group: GroupId, resource_id: u64, partition: u32| -> Vec<(&'static str, String)> {
        let r = obs.resources.get(&resource_id);
        // The cluster's own groups belong to no resource.
        let kind = match r {
            Some(r) => r.kind,
            None if resource_id == 0 && group == m.metadata_group => "metadata",
            None if resource_id == 0 && group == m.coordinator_group => "coordinator",
            None => "",
        };
        vec![
            ("node", obs.node_id.to_string()),
            ("group", group.to_string()),
            ("kind", kind.to_string()),
            ("tenant", r.map(|r| r.tenant.clone()).unwrap_or_default()),
            ("resource", r.map(|r| r.name.clone()).unwrap_or_default()),
            ("partition", partition.to_string()),
        ]
    };
    let responsive: BTreeMap<u64, &[u32]> = m.responsive.iter().map(|r| (r.group, r.nodes.as_slice())).collect();
    type PartitionGauge = (&'static str, &'static str, &'static str, fn(&lotse_proto::PartitionMetrics) -> u64);
    let gauges: [PartitionGauge; 7] = [
        ("lotse_partition_leader", "gauge", "1 while this node leads the group with a quorum.", |p| {
            u64::from(p.leader)
        }),
        ("lotse_partition_term", "gauge", "The group's Raft term on this node.", |p| p.term),
        ("lotse_partition_high_watermark", "gauge", "Offset after the last committed one.", |p| p.high_watermark),
        ("lotse_partition_last_stable_offset", "gauge", "Offset below the first open transaction.", |p| p.last_stable),
        ("lotse_partition_log_start_offset", "gauge", "First offset stored on this node.", |p| p.log_start),
        ("lotse_partition_log_end_offset", "gauge", "Offset after the last one in this node's log.", |p| p.log_end),
        ("lotse_partition_size_bytes", "gauge", "Bytes of the log on this node.", |p| p.size_bytes),
    ];
    for (name, kind, help, f) in gauges {
        let unit = if name.ends_with("_bytes") { "bytes" } else { "" };
        out.family(name, kind, help, unit);
        for p in &m.partitions {
            out.sample(name, &labels(p.group, p.resource_id, p.partition), f(p));
        }
    }
    let counters: [PartitionGauge; 3] = [
        ("lotse_partition_produced_bytes", "bytes", "Bytes of acknowledged produces, while this node led.", |p| {
            p.produce_bytes
        }),
        ("lotse_partition_produced_records", "", "Records of acknowledged produces, while this node led.", |p| {
            p.produce_records
        }),
        ("lotse_partition_fetched_bytes", "bytes", "Bytes fetched, while this node led.", |p| p.fetch_bytes),
    ];
    for (name, unit, help, f) in counters {
        out.family(name, "counter", help, unit);
        for p in &m.partitions {
            out.sample(&format!("{name}_total"), &labels(p.group, p.resource_id, p.partition), f(p));
        }
    }
    out.family(
        "lotse_partition_open_transactions",
        "gauge",
        "Transactions open on the partition, as this node has applied them.",
        "",
    );
    for p in &m.partitions {
        out.sample(
            "lotse_partition_open_transactions",
            &labels(p.group, p.resource_id, p.partition),
            u64::from(p.open_transactions),
        );
    }
    out.family(
        "lotse_partition_oldest_record_timestamp_seconds",
        "gauge",
        "When the oldest record stored on this node was written, seconds since the epoch.",
        "seconds",
    );
    for p in &m.partitions {
        if let Some(ms) = p.oldest_timestamp_ms {
            let l = labels(p.group, p.resource_id, p.partition);
            out.sample_f("lotse_partition_oldest_record_timestamp_seconds", &l, ms as f64 / 1000.0);
        }
    }
    out.family("lotse_partition_voters", "gauge", "Voters of the group, as this node knows them.", "");
    for p in &m.partitions {
        let n = obs.groups.get(&p.group).map_or(0, |h| h.membership.all_voters().len());
        out.sample("lotse_partition_voters", &labels(p.group, p.resource_id, p.partition), n as u64);
    }
    // On the leader: how many voters answered within an election timeout.
    let answering: Vec<(&lotse_proto::PartitionMetrics, usize, usize)> = m
        .partitions
        .iter()
        .filter(|p| p.leader)
        .filter_map(|p| {
            let (heard, h) = (responsive.get(&p.group)?, obs.groups.get(&p.group)?);
            let voters = h.membership.all_voters();
            Some((p, heard.iter().filter(|n| voters.contains(&NodeId::from(**n))).count(), voters.len()))
        })
        .collect();
    out.family(
        "lotse_partition_responsive_voters",
        "gauge",
        "On the leader: voters, itself included, that answered within an election timeout.",
        "",
    );
    for (p, n, _) in &answering {
        out.sample("lotse_partition_responsive_voters", &labels(p.group, p.resource_id, p.partition), *n as u64);
    }
    out.family(
        "lotse_partition_under_replicated",
        "gauge",
        "On the leader: 1 while a voter has not answered within an election timeout.",
        "",
    );
    for (p, n, voters) in &answering {
        let l = labels(p.group, p.resource_id, p.partition);
        out.sample("lotse_partition_under_replicated", &l, u64::from(n < voters));
    }
    out.family(
        "lotse_replica_log_end_offset",
        "gauge",
        "On the leader: offset after the last one each replica holds.",
        "",
    );
    for p in m.partitions.iter().filter(|p| p.leader) {
        for r in &p.replicas {
            let mut l = labels(p.group, p.resource_id, p.partition);
            l.push(("replica", r.node_id.to_string()));
            out.sample("lotse_replica_log_end_offset", &l, r.log_end);
        }
    }

    let queue_labels = |resource_id: u64| -> Vec<(&'static str, String)> {
        let r = obs.resources.get(&resource_id);
        vec![
            ("node", obs.node_id.to_string()),
            ("tenant", r.map(|r| r.tenant.clone()).unwrap_or_default()),
            ("queue", r.map(|r| r.name.clone()).unwrap_or_default()),
        ]
    };
    type QueueValue = (&'static str, &'static str, &'static str, fn(&lotse_proto::QueueMetrics) -> u64);
    let queue: [QueueValue; 6] = [
        ("lotse_queue_ready_messages", "gauge", "Messages ready for delivery.", |q| q.ready),
        ("lotse_queue_in_flight_messages", "gauge", "Messages delivered and not yet acknowledged.", |q| q.in_flight),
        ("lotse_queue_in_open_transactions_messages", "gauge", "Messages waiting for their transaction to end.", |q| {
            q.in_open_txns
        }),
        ("lotse_queue_consumers", "gauge", "Subscriptions to the queue.", |q| q.consumers.len() as u64),
        ("lotse_queue_delivered_messages", "counter", "Messages delivered, while this node led the queue.", |q| {
            q.delivered
        }),
        ("lotse_queue_acked_messages", "counter", "Messages acknowledged, while this node led the queue.", |q| q.acked),
    ];
    for (name, kind, help, f) in queue {
        out.family(name, kind, help, "");
        let sample = if kind == "counter" { format!("{name}_total") } else { name.to_string() };
        for q in &m.queues {
            out.sample(&sample, &queue_labels(q.resource_id), f(q));
        }
    }
    out.family(
        "lotse_queue_in_flight_bytes",
        "gauge",
        "Bytes of the messages delivered and not yet acknowledged, each its share of its batch.",
        "bytes",
    );
    for q in &m.queues {
        let bytes = q.consumers.iter().map(|c| c.in_flight_bytes).sum();
        out.sample("lotse_queue_in_flight_bytes", &queue_labels(q.resource_id), bytes);
    }
    out.family(
        "lotse_queue_consumers_in_state",
        "gauge",
        "Subscriptions to the queue by state: shared, or a single active consumer's active and standby ones.",
        "",
    );
    for q in &m.queues {
        for state in [ConsumerState::Shared, ConsumerState::Active, ConsumerState::Standby] {
            let mut l = queue_labels(q.resource_id);
            l.push(("state", state.name().to_string()));
            out.sample(
                "lotse_queue_consumers_in_state",
                &l,
                q.consumers.iter().filter(|c| c.state == state).count() as u64,
            );
        }
    }
    out.family(
        "lotse_queue_ready_by_level_messages",
        "gauge",
        "On a queue with priority levels: messages ready for delivery per level.",
        "",
    );
    for q in &m.queues {
        for (level, n) in q.ready_by_level.iter().enumerate() {
            let mut l = queue_labels(q.resource_id);
            l.push(("level", level.to_string()));
            out.sample("lotse_queue_ready_by_level_messages", &l, *n);
        }
    }
    out.family(
        "lotse_queue_ready_level_unread_messages",
        "gauge",
        "On a queue with priority levels: messages ready for delivery whose level the leader has not read yet.",
        "",
    );
    for q in m.queues.iter().filter(|q| !q.ready_by_level.is_empty()) {
        out.sample("lotse_queue_ready_level_unread_messages", &queue_labels(q.resource_id), q.ready_level_unread);
    }

    out.family(
        "lotse_consumer_group_members",
        "gauge",
        "Members of a consumer group, on the coordinator's leader.",
        "",
    );
    out.family("lotse_consumer_group_epoch", "gauge", "A consumer group's epoch, on the coordinator's leader.", "");
    out.family(
        "lotse_consumer_group_committed_offset",
        "gauge",
        "A consumer group's committed offset per partition, on the coordinator's leader.",
        "",
    );
    for g in &m.groups {
        let base = vec![
            ("node", obs.node_id.to_string()),
            ("tenant", g.tenant.clone()),
            ("consumer_group", g.group_id.clone()),
        ];
        out.sample("lotse_consumer_group_members", &base, g.members.len() as u64);
        out.sample_f("lotse_consumer_group_epoch", &base, f64::from(g.epoch));
        for o in &g.offsets {
            let mut l = base.clone();
            l.push(("resource", obs.resources.get(&o.resource_id).map(|r| r.name.clone()).unwrap_or_default()));
            l.push(("partition", o.partition.to_string()));
            out.sample("lotse_consumer_group_committed_offset", &l, o.offset);
        }
    }
    out.text.push_str("# EOF\n");
    out.text
}

/// A number of seconds as OpenMetrics writes it.
fn seconds(v: f64) -> String {
    let s = format!("{v}");
    if s.contains('.') || s.contains('e') { s } else { format!("{s}.0") }
}

#[derive(Default)]
struct Out {
    text: String,
}

impl Out {
    fn family(&mut self, name: &str, kind: &str, help: &str, unit: &str) {
        let _ = writeln!(self.text, "# TYPE {name} {kind}");
        if !unit.is_empty() {
            let _ = writeln!(self.text, "# UNIT {name} {unit}");
        }
        let _ = writeln!(self.text, "# HELP {name} {}", escape(help));
    }

    fn labels(&mut self, name: &str, labels: &[(&str, String)]) {
        self.text.push_str(name);
        if !labels.is_empty() {
            self.text.push('{');
            for (i, (k, v)) in labels.iter().enumerate() {
                if i > 0 {
                    self.text.push(',');
                }
                let _ = write!(self.text, "{k}=\"{}\"", escape(v));
            }
            self.text.push('}');
        }
    }

    fn sample(&mut self, name: &str, labels: &[(&str, String)], v: u64) {
        self.labels(name, labels);
        let _ = writeln!(self.text, " {v}");
    }

    fn sample_f(&mut self, name: &str, labels: &[(&str, String)], v: f64) {
        self.labels(name, labels);
        let _ = writeln!(self.text, " {v}");
    }
}

/// A label value or help text with backslashes, quotes and line breaks
/// escaped, as OpenMetrics asks.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests;

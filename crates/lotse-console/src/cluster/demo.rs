//! A built-in cluster with sample data, until the native protocol has its
//! admin API keys.
//!
//! It serves the scene from the mockups: a rolling upgrade from 0.4.0 to
//! 0.4.1 is under way, broker-3 is catching up after its restart, broker-2
//! is past its soft disk limit, and tenant `analytics` has hit its quota.
//! Rates drift over time and broker-3 finishes catching up after about nine
//! minutes, so live updates have something to show. Queues created through
//! the API are kept in memory.

use std::collections::{BTreeMap, BTreeSet};
use std::f64::consts::TAU;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use super::Cluster;
use crate::auth::Identity;
use crate::error::{ApiError, ApiResult};
use crate::metrics::{ChartSample, MetricsStore, Visibility};
use crate::model::*;
use crate::time::{now_ms, rfc3339};

const MIB: f64 = 1024.0 * 1024.0;
const GIB: u64 = 1 << 30;
const TIB: u64 = 1 << 40;
const RAFT_GROUPS: u32 = 1_284;
const CATCH_UP_GROUPS: u32 = 37;
const CATCH_UP_SECS_PER_GROUP: f64 = 15.0;

/// Sample cluster. See the module docs.
#[derive(Debug)]
pub struct DemoCluster {
    id: String,
    started: Instant,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    created: BTreeMap<String, (CreateQueue, String)>,
    paused: bool,
    /// Queues purged since the console started: no ready messages.
    purged: BTreeSet<String>,
    /// Dead-letter queues requeued: their dead letters are gone.
    requeued: BTreeSet<String>,
    /// Partitions pinned to a node, by resource and partition.
    pins: BTreeMap<(String, u32), String>,
    /// Users created or changed here, by name, with their iterations; the
    /// demo's sample users are `SAMPLE_USERS`. Deleted names are in `gone`.
    users: BTreeMap<String, u32>,
    gone: BTreeSet<String>,
    /// Bindings added and removed here, over `sample_bindings`.
    added: Vec<BindingRow>,
    removed: Vec<BindingRow>,
    /// Subjects whose sessions were revoked: `(group, name)`.
    revoked: BTreeSet<(bool, String)>,
}

/// Users the demo cluster starts with, as `(name, configured)`.
const SAMPLE_USERS: [(&str, bool); 3] = [("pierre", true), ("ingest-service", false), ("billing-reader", false)];

const ROLES: [&str; 6] = ["cluster-admin", "cluster-viewer", "tenant-admin", "producer", "consumer", "viewer"];

fn binding(kind: &str, subject: &str, role: &str, tenant: &str, prefix: &str, source: &str) -> BindingRow {
    BindingRow {
        subject_kind: kind.into(),
        subject: subject.into(),
        role: role.into(),
        tenant: tenant.into(),
        prefix: prefix.into(),
        source: source.into(),
    }
}

fn sample_bindings() -> Vec<BindingRow> {
    vec![
        binding("user", "pierre", "cluster-admin", "", "", "configuration"),
        binding("group", "platform-team", "cluster-viewer", "", "", "runtime"),
        binding("user", "ingest-service", "producer", "payments", "events.", "runtime"),
        binding("user", "billing-reader", "consumer", "payments", "", "runtime"),
    ]
}

/// A binding as the broker keys it: cluster-wide roles have no tenant.
fn normalised(b: &BindingRow) -> ApiResult<BindingRow> {
    if !["user", "group"].contains(&b.subject_kind.as_str()) {
        return Err(ApiError::BadRequest(format!("\"{}\" is not a subject kind: use user or group.", b.subject_kind)));
    }
    if !ROLES.contains(&b.role.as_str()) {
        return Err(ApiError::BadRequest(format!("\"{}\" is not a role.", b.role)));
    }
    let wide = b.role.starts_with("cluster-");
    if !wide && b.tenant.is_empty() {
        return Err(ApiError::BadRequest(format!("The role {} needs a tenant.", b.role)));
    }
    let mut n = b.clone();
    n.source = "runtime".into();
    if wide {
        n.tenant.clear();
        n.prefix.clear();
    }
    Ok(n)
}

impl DemoCluster {
    /// A demo cluster called `id`.
    pub fn new(id: impl Into<String>) -> Self {
        DemoCluster { id: id.into(), started: Instant::now(), state: Mutex::new(State::default()) }
    }

    /// The sample bindings and what was added or removed since.
    fn bindings_of(&self, st: &State) -> Vec<BindingRow> {
        let key = |x: &BindingRow| {
            (x.subject_kind.clone(), x.subject.clone(), x.role.clone(), x.tenant.clone(), x.prefix.clone())
        };
        let mut all: Vec<BindingRow> =
            sample_bindings().into_iter().filter(|b| !st.removed.iter().any(|r| key(r) == key(b))).collect();
        all.extend(st.added.iter().cloned());
        all
    }

    fn secs(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    fn catching_up(&self) -> u32 {
        let done = (self.secs() / CATCH_UP_SECS_PER_GROUP) as u32;
        CATCH_UP_GROUPS.saturating_sub(done)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// What purges did to a queue, and requeues to its dead letters.
    fn overlay_queue(&self, d: &mut QueueDetail) {
        let st = self.lock();
        if st.purged.contains(&d.name) {
            d.ready = 0;
            for l in &mut d.levels {
                l.ready = 0;
                l.oldest_ms = None;
            }
        }
        if d.dlq.as_ref().is_some_and(|q| st.requeued.contains(q)) {
            d.dead_letters.clear();
        }
        for l in &mut d.levels {
            if let Some(n) = st.pins.get(&(d.name.clone(), u32::from(l.level))) {
                l.leader = n.clone();
            }
        }
    }

    fn overlay_queues(&self, rows: &mut [QueueRow]) {
        let st = self.lock();
        for r in rows.iter_mut().filter(|r| st.purged.contains(&r.name)) {
            r.ready = 0;
        }
    }

    /// Where pins put a stream's leaders.
    fn overlay_stream(&self, d: &mut StreamDetail) {
        let st = self.lock();
        for p in &mut d.partitions {
            if let Some(n) = st.pins.get(&(d.name.clone(), p.id)) {
                p.leader = n.clone();
            }
        }
    }

    fn in_rate(&self) -> f64 {
        in_rate_at(self.secs())
    }

    /// Fills `store` with synthetic history for the last `horizon`, drawn
    /// from the same curves as the live numbers, so charts are not empty
    /// right after start. Only the demo does this.
    pub fn backfill(&self, store: &MetricsStore, horizon: Duration, step: Duration) {
        let now = now_ms();
        let step_s = step.as_secs().max(1);
        for i in (1..=horizon.as_secs() / step_s).rev() {
            let ago = i * step_s;
            let s = -(ago as f64);
            let t = now - ago * 1000;
            store.record(&self.id, t, &sample_at(s));
        }
    }

    fn node_list(&self) -> Vec<Node> {
        let s = self.secs();
        let catching = self.catching_up();
        let total_in = self.in_rate();
        let b3_leads = if catching == 0 { 120 } else { 0 };
        let b1_leads = 634 - b3_leads / 2;
        let b2_leads = RAFT_GROUPS - b1_leads - b3_leads;
        let share = |leads: u32| total_in * f64::from(leads) / f64::from(RAFT_GROUPS);
        let b2_free = (0.19 - s * 0.000_002).max(0.11);
        vec![
            Node {
                id: "broker-1".into(),
                role: "Metadata leader".into(),
                zone: Some("hh-a".into()),
                uptime_secs: 24 * 60 + s as u64,
                leads: b1_leads,
                replicas: RAFT_GROUPS,
                disk_free_bytes: Some((4.0 * TIB as f64 * 0.61) as u64),
                disk_total_bytes: Some(4 * TIB),
                produce_bytes_per_sec: Some(share(b1_leads)),
                net_in_bytes_per_sec: Some(share(b1_leads) + total_in * 0.5),
                net_out_bytes_per_sec: Some(share(b1_leads) * 2.76),
                connections: 1_731,
                cpu: Some(0.38 * wave(s, 41.0, 0.08)),
                memory_bytes: Some((19.2 * GIB as f64) as u64),
                memory_limit_bytes: Some(32 * GIB),
                version: "0.4.1".into(),
                status: chip(Tone::Ok, "Healthy"),
            },
            Node {
                id: "broker-2".into(),
                role: "Voter".into(),
                zone: Some("hh-b".into()),
                uptime_secs: 41 * 86_400 + s as u64,
                leads: b2_leads,
                replicas: RAFT_GROUPS,
                disk_free_bytes: Some((4.0 * TIB as f64 * b2_free) as u64),
                disk_total_bytes: Some(4 * TIB),
                produce_bytes_per_sec: Some(share(b2_leads)),
                net_in_bytes_per_sec: Some(share(b2_leads) + total_in * 0.5),
                net_out_bytes_per_sec: Some(share(b2_leads) * 2.71),
                connections: 1_461,
                cpu: Some(0.46 * wave(s, 37.0, 0.08)),
                memory_bytes: Some((21.8 * GIB as f64) as u64),
                memory_limit_bytes: Some(32 * GIB),
                version: "0.4.0".into(),
                status: chip(Tone::Warn, "Soft disk limit"),
            },
            Node {
                id: "broker-3".into(),
                role: if catching > 0 { "Voter, upgrading".into() } else { "Voter".into() },
                zone: Some("hh-c".into()),
                uptime_secs: 7 * 60 + s as u64,
                leads: b3_leads,
                replicas: RAFT_GROUPS,
                disk_free_bytes: Some((4.0 * TIB as f64 * 0.58) as u64),
                disk_total_bytes: Some(4 * TIB),
                produce_bytes_per_sec: Some(share(b3_leads)),
                net_in_bytes_per_sec: if catching > 0 {
                    Some(61.5 * MIB)
                } else {
                    Some(share(b3_leads) + total_in * 0.5)
                },
                net_out_bytes_per_sec: Some(share(b3_leads) * 2.7 + 0.2 * MIB),
                connections: if catching > 0 { 0 } else { 402 },
                cpu: Some(if catching > 0 { 0.71 } else { 0.33 } * wave(s, 29.0, 0.06)),
                memory_bytes: Some(14 * GIB),
                memory_limit_bytes: Some(32 * GIB),
                version: "0.4.1".into(),
                status: if catching > 0 { chip(Tone::Warn, "Catching up") } else { chip(Tone::Ok, "Healthy") },
            },
        ]
    }

    fn upgrade(&self) -> Upgrade {
        let catching = self.catching_up();
        let paused = self.lock().paused;
        let p = |name: &str, state| UpgradePhase { name: name.into(), state };
        let phases = |states: [PhaseState; 4]| {
            vec![
                p("Move leaders away", states[0]),
                p("Restart on 0.4.1", states[1]),
                p("Catch up all Raft groups", states[2]),
                p("Hand leaders back", states[3]),
            ]
        };
        use PhaseState::{Done as D, Pending as P, Running as R};
        let slowest = u64::from(catching) * 3_412 / u64::from(CATCH_UP_GROUPS);
        let b3 = if catching > 0 {
            UpgradeStep {
                node: "broker-3".into(),
                status: chip(Tone::Warn, "Catching up"),
                phases: phases([D, D, R, P]),
                note: format!(
                    "{catching} Raft groups behind; slowest is orders P3 at {} entries. Writes keep their 2-of-3 quorum meanwhile.",
                    group(slowest)
                ),
            }
        } else {
            UpgradeStep {
                node: "broker-3".into(),
                status: chip(Tone::Info, "Taking leaders back"),
                phases: phases([D, D, D, R]),
                note: "Caught up. The operator hands back leaderships a few at a time.".into(),
            }
        };
        Upgrade {
            from: "0.4.0".into(),
            to: "0.4.1".into(),
            started_by: "pierre".into(),
            started_at: rfc3339(now_ms() - (self.secs() as u64 + 24 * 60) * 1000),
            nodes_done: 1,
            nodes_total: 3,
            groups_catching_up: catching,
            steps: vec![
                UpgradeStep {
                    node: "broker-1".into(),
                    status: chip(Tone::Ok, "Done"),
                    phases: phases([D, D, D, D]),
                    note: "Caught up in 3 min 12 s.".into(),
                },
                b3,
                UpgradeStep {
                    node: "broker-2".into(),
                    status: chip(Tone::Info, if paused { "Paused" } else { "Waiting" }),
                    phases: phases([P, P, P, P]),
                    note: if paused {
                        "The upgrade stops after broker-3. Resume it to continue with broker-2.".into()
                    } else {
                        "Starts when broker-3 has caught up. Soft disk limit reached; the upgrade does not change that."
                            .into()
                    },
                },
            ],
            paused_after_current: paused,
        }
    }

    fn created_rows(&self) -> Vec<QueueRow> {
        self.lock()
            .created
            .values()
            .map(|(q, _)| QueueRow {
                name: q.name.clone(),
                tenant: q.tenant.clone(),
                ready: 0,
                dead_lettered: Some(0),
                attention: None,
            })
            .collect()
    }
}

#[async_trait]
impl Cluster for DemoCluster {
    fn id(&self) -> &str {
        &self.id
    }

    async fn summary(&self, _who: &Identity) -> ApiResult<ClusterRef> {
        let upgrading = self.catching_up() > 0 || !self.lock().paused;
        Ok(ClusterRef {
            id: self.id.clone(),
            nodes: 3,
            status: if upgrading { "upgrading to 0.4.1".into() } else { "upgrade paused".into() },
            tone: Tone::Warn,
            demo: true,
            tenants: true,
            queue_options: true,
        })
    }

    async fn overview(&self, who: &Identity) -> ApiResult<Overview> {
        let nodes = self.node_list();
        let catching = self.catching_up();
        let b2 = &nodes[1];
        let b2_free = b2.disk_free_bytes.unwrap_or(0) as f64 / b2.disk_total_bytes.unwrap_or(1) as f64;
        let in_rate = self.in_rate();
        let mut queues = self.queues(who).await?;
        queues.retain(|q| q.attention.is_some());
        let mut streams = self.streams(who).await?;
        streams.truncate(5);
        Ok(Overview {
            cluster: self.id.clone(),
            raft_groups: RAFT_GROUPS,
            metadata_leader: "broker-1".into(),
            metadata_term: 18,
            alerts: vec![Alert {
                id: "disk-soft-broker-2".into(),
                tone: Tone::Warn,
                title: format!("Soft disk limit on broker-2: {:.0} % free", b2_free * 100.0),
                detail: format!(
                    "Producers writing to the {} partitions and queues it leads are throttled, throttle_ms up to 180. \
                     Writes stop at 10 % free. Move leaders away or free space.",
                    b2.leads
                ),
                node: Some("broker-2".into()),
            }],
            throughput: Throughput {
                in_bytes_per_sec: Some(in_rate),
                out_bytes_per_sec: Some(in_rate * 2.73),
                records_per_sec: Some(in_rate / 238.0),
                commit_p99_ms: Some(7.8 * wave(self.secs(), 900.0, 0.08)),
                stored_bytes: (3.94 * TIB as f64) as u64,
                consumers: 1_940,
            },
            health: vec![
                HealthItem {
                    label: "Raft groups".into(),
                    value: group(u64::from(RAFT_GROUPS)),
                    detail: if catching > 0 {
                        format!("{catching} catching up on broker-3")
                    } else {
                        "all in sync".into()
                    },
                    chip: Some(if catching > 0 { chip(Tone::Warn, "Catching up") } else { chip(Tone::Ok, "In sync") }),
                },
                HealthItem {
                    label: "Rolling upgrade".into(),
                    value: "0.4.0 → 0.4.1".into(),
                    detail: "1 of 3 nodes done".into(),
                    chip: Some(chip(Tone::Info, if self.lock().paused { "Pausing" } else { "Running" })),
                },
                HealthItem {
                    label: "Throttled producers".into(),
                    value: "23".into(),
                    detail: "soft disk limit on broker-2".into(),
                    chip: Some(chip(Tone::Warn, "Throttled")),
                },
                HealthItem {
                    label: "Open transactions".into(),
                    value: "52".into(),
                    detail: "oldest 4.1 s".into(),
                    chip: None,
                },
                HealthItem {
                    label: "Largest consumer lag".into(),
                    value: format!("{} records", group(billing_lag(self.secs()))),
                    detail: "billing-ledger on orders".into(),
                    chip: None,
                },
                HealthItem {
                    label: "Dead-lettered messages".into(),
                    value: "318".into(),
                    detail: "in 4 queues".into(),
                    chip: None,
                },
            ],
            nodes,
            disk_limits: Some(DISK_LIMITS),
            busiest_streams: streams,
            attention_queues: queues,
            stream_count: 214,
            queue_count: 96 + self.lock().created.len() as u32,
        })
    }

    async fn nodes(&self, _who: &Identity) -> ApiResult<NodesView> {
        let catching = self.catching_up();
        let now = now_ms();
        let ago = |secs: u64| rfc3339(now.saturating_sub(secs * 1000));
        let mut events = vec![
            OperatorEvent {
                at: ago(self.secs() as u64 + 7 * 60 + 5),
                node: "broker-3".into(),
                text: "Pod restarted on 0.4.1 with volume data-broker-3; recovered 6 segments with torn tails cut."
                    .into(),
            },
            OperatorEvent {
                at: ago(self.secs() as u64 + 7 * 60 + 50),
                node: "broker-3".into(),
                text: "405 leaderships moved to broker-1 and broker-2.".into(),
            },
            OperatorEvent {
                at: ago(self.secs() as u64 + 12 * 60 + 27),
                node: "broker-2".into(),
                text: "Soft disk limit: 19 % free. throttle_ms now applied to producers on its leaders.".into(),
            },
            OperatorEvent {
                at: ago(self.secs() as u64 + 16 * 60 + 39),
                node: "broker-1".into(),
                text: "Leaders handed back; node upgraded.".into(),
            },
        ];
        events.insert(
            0,
            OperatorEvent {
                at: ago(0),
                node: "broker-3".into(),
                text: format!(
                    "{} of {} Raft groups caught up.",
                    group(u64::from(RAFT_GROUPS - catching)),
                    group(u64::from(RAFT_GROUPS))
                ),
            },
        );
        Ok(NodesView { nodes: self.node_list(), disk_limits: Some(DISK_LIMITS), upgrade: Some(self.upgrade()), events })
    }

    async fn streams(&self, _who: &Identity) -> ApiResult<Vec<StreamRow>> {
        let s = self.secs();
        let row = |name: &str, tenant: &str, partitions, mib: f64, stored: u64| StreamRow {
            name: name.into(),
            tenant: Some(tenant.into()),
            partitions,
            in_bytes_per_sec: Some(mib * MIB * wave(s, 61.0 + mib, 0.05)),
            stored_bytes: stored,
        };
        Ok(vec![
            row("orders", "commerce", 12, 14.2, 418 * GIB),
            row("clickstream", "analytics", 48, 11.6, (1.62 * TIB as f64) as u64),
            row("inventory-events", "commerce", 6, 4.9, 96 * GIB),
            row("audit.trail", "platform", 3, 2.2, 310 * GIB),
            row("ledger.entries", "finance", 12, 1.7, 188 * GIB),
            row("search.updates", "commerce", 6, 1.1, 41 * GIB),
            row("notifications.sent", "notify", 3, 0.4, 12 * GIB),
        ])
    }

    async fn stream(&self, who: &Identity, name: &str) -> ApiResult<StreamDetail> {
        let mut out: StreamDetail = async {
        let row = self
            .streams(who)
            .await?
            .into_iter()
            .find(|r| r.name == name)
            .ok_or_else(|| ApiError::NotFound(format!("There is no stream \"{name}\" in {}.", self.id)))?;
        let catching = self.catching_up();
        let behind = |p: u32| -> u64 {
            let base = match (name, p) {
                ("orders", 3) => 3_412,
                ("orders", 7) => 1_209,
                _ => 0,
            };
            base * u64::from(catching) / u64::from(CATCH_UP_GROUPS)
        };
        let open_tx = |p: u32| match p {
            2 => 4,
            7 => 11,
            10 => 2,
            _ => 0,
        };
        let partitions = (0..row.partitions)
            .map(|p| {
                let hw = partition_hw(name, p) + (self.secs() * 3.1) as u64;
                let leader = if p % 2 == 0 { "broker-1" } else { "broker-2" };
                let replicas = ["broker-1", "broker-2", "broker-3"]
                    .iter()
                    .map(|n| {
                        let b = if *n == "broker-3" { behind(p) } else { 0 };
                        Replica {
                            node: (*n).into(),
                            state: if *n == leader {
                                ReplicaState::Leader
                            } else if b > 0 {
                                ReplicaState::CatchingUp
                            } else {
                                ReplicaState::InSync
                            },
                            behind: b,
                        }
                    })
                    .collect();
                Partition {
                    id: p,
                    leader: leader.into(),
                    term: 17 + u64::from(p % 3),
                    replicas,
                    log_start: hw - 2_310_000 - u64::from(p) * 4_100,
                    high_watermark: hw,
                    last_stable: hw - open_tx(p) * 37,
                    size_bytes: row.stored_bytes / 3 / u64::from(row.partitions),
                }
            })
            .collect();
        let s = self.secs();
        let (groups, assignment) = if name == "orders" {
            (
                vec![
                    Group {
                        id: "billing-ledger".into(),
                        members: 3,
                        state: chip(Tone::Ok, "Stable"),
                        epoch: 14,
                        lag: billing_lag(s),
                        lag_trend: vec![4_100, 4_900, 5_300, 6_800, 8_200, 9_400, 10_600, 11_700, billing_lag(s)],
                    },
                    Group {
                        id: "fraud-scoring".into(),
                        members: 4,
                        state: chip(Tone::Info, "Reassigning P5"),
                        epoch: 42,
                        lag: 1_102,
                        lag_trend: vec![1_300, 1_250, 1_400, 1_260, 1_320, 1_180, 1_240, 1_090, 1_102],
                    },
                    Group {
                        id: "search-indexer".into(),
                        members: 6,
                        state: chip(Tone::Ok, "Stable"),
                        epoch: 31,
                        lag: 88,
                        lag_trend: vec![90, 85, 110, 92, 80, 96, 88, 84, 88],
                    },
                    Group {
                        id: "analytics-export".into(),
                        members: 2,
                        state: chip(Tone::Ok, "Stable"),
                        epoch: 9,
                        lag: 240,
                        lag_trend: vec![2_400, 1_900, 1_600, 1_200, 900, 600, 420, 300, 240],
                    },
                ],
                Some(Assignment {
                    group: "fraud-scoring".into(),
                    coordinator: "__groups-7 on broker-1".into(),
                    members: vec![
                        AssignedMember { id: "m1 · 10.4.2.17".into(), owned: vec![0, 1, 2], incoming: vec![] },
                        AssignedMember { id: "m2 · 10.4.2.18".into(), owned: vec![3, 4], incoming: vec![] },
                        AssignedMember { id: "m3 · 10.4.3.9".into(), owned: vec![6, 7, 8], incoming: vec![] },
                        AssignedMember { id: "m4 · 10.4.3.11".into(), owned: vec![9, 10, 11], incoming: vec![5] },
                    ],
                    note: Some(
                        "m2 stopped reporting P5 as owned at epoch 41; the coordinator gives it to m4 on m4's next heartbeat. \
                         No other partition paused."
                            .into(),
                    ),
                }),
            )
        } else {
            (vec![], None)
        };
        Ok(StreamDetail {
            name: row.name,
            tenant: row.tenant,
            replication: 3,
            retention: Some(StreamRetention { ms: Some(7 * 86_400_000), bytes: None }),
            compaction: false,
            in_bytes_per_sec: row.in_bytes_per_sec,
            out_bytes_per_sec: row.in_bytes_per_sec.map(|r| r * 2.94),
            records_per_sec: row.in_bytes_per_sec.map(|r| r / 392.0),
            stored_bytes: row.stored_bytes,
            oldest_record_age_ms: Some((6 * 86_400 + 22 * 3_600) * 1000),
            open_transactions: if name == "orders" { Some(17) } else { Some(0) },
            partitions,
            groups,
            assignment,
        })
        }
        .await?;
        self.overlay_stream(&mut out);
        Ok(out)
    }

    async fn queues(&self, _who: &Identity) -> ApiResult<Vec<QueueRow>> {
        let mut out: Vec<QueueRow> = async {
            let row = |name: &str, tenant: &str, ready, dlq, attention: Option<Chip>| QueueRow {
                name: name.into(),
                tenant: Some(tenant.into()),
                ready,
                dead_lettered: dlq,
                attention,
            };
            let mut rows = vec![
                row(
                    "email.send",
                    "notify",
                    18_304 + (self.secs() * 6.0) as u64,
                    Some(0),
                    Some(chip(Tone::Critical, "No consumers")),
                ),
                row("payments.capture", "commerce", 1_942, Some(211), Some(chip(Tone::Warn, "DLQ growing"))),
                row("webhooks.out", "platform", 6_120, Some(43), Some(chip(Tone::Warn, "Oldest 12 min"))),
                row("refunds", "commerce", 12, Some(64), Some(chip(Tone::Info, "DLQ not empty"))),
                row("invoices.render", "finance", 3, Some(0), None),
                row("sms.send", "notify", 0, Some(0), None),
            ];
            rows.extend(self.created_rows());
            Ok(rows)
        }
        .await?;
        self.overlay_queues(&mut out);
        Ok(out)
    }

    async fn queue(&self, who: &Identity, name: &str) -> ApiResult<QueueDetail> {
        let mut out: QueueDetail = async {
            if let Some((q, by)) = self.lock().created.get(name) {
                return Ok(QueueDetail {
                    name: q.name.clone(),
                    tenant: q.tenant.clone(),
                    single_active: q.single_active,
                    ttl_ms: q.ttl_ms,
                    dlq: q.dlq.clone(),
                    max_deliveries: q.max_deliveries,
                    binding: None,
                    ready: 0,
                    in_flight: 0,
                    in_open_txns: 0,
                    publish_per_sec: Some(0.0),
                    ack_per_sec: Some(0.0),
                    dead_lettered: Some(0),
                    levels: (0..q.priority_levels)
                        .rev()
                        .map(|l| PriorityLevel {
                            level: l,
                            leader: ["broker-1", "broker-2"][usize::from(l % 2)].into(),
                            ready: 0,
                            oldest_ms: Some(0),
                        })
                        .collect(),
                    consumers: vec![],
                    dead_letters: vec![],
                    handover: vec![HandoverEvent { at: rfc3339(now_ms()), text: format!("Created by {by}.") }],
                });
            }
            let row = self
                .queues(who)
                .await?
                .into_iter()
                .find(|r| r.name == name)
                .ok_or_else(|| ApiError::NotFound(format!("There is no queue \"{name}\" in {}.", self.id)))?;
            if name != "payments.capture" {
                return Ok(QueueDetail {
                    name: row.name,
                    tenant: row.tenant,
                    single_active: false,
                    ttl_ms: None,
                    dlq: (row.dead_lettered > Some(0)).then(|| format!("{name}.dlq")),
                    max_deliveries: (row.dead_lettered > Some(0)).then_some(5),
                    binding: None,
                    ready: row.ready,
                    in_flight: 0,
                    in_open_txns: 0,
                    publish_per_sec: Some(12.0),
                    ack_per_sec: Some(12.0),
                    dead_lettered: row.dead_lettered,
                    levels: vec![PriorityLevel {
                        level: 0,
                        leader: "broker-1".into(),
                        ready: row.ready,
                        oldest_ms: Some(45_000),
                    }],
                    consumers: vec![],
                    dead_letters: vec![],
                    handover: vec![],
                });
            }
            let now = now_ms();
            let consumer = |tag: &str, conn: &str, active: bool| QueueConsumer {
                tag: tag.into(),
                principal: Some("CN=capture-svc".into()),
                connection: conn.into(),
                in_flight: if active { 118 } else { 0 },
                credit_messages: 128,
                in_flight_bytes: if active { Some((3.1 * MIB) as u64) } else { Some(0) },
                credit_bytes: Some(4 << 20),
                acks_per_sec: if active { Some(805.0 * wave(self.secs(), 19.0, 0.05)) } else { Some(0.0) },
                state: if active { chip(Tone::Ok, "Active") } else { chip(Tone::Info, "Standby") },
            };
            let dead = |offset, key: &str, reason: &str, deliveries, secs_ago: u64| DeadLetter {
                offset,
                key: Some(key.into()),
                reason: reason.into(),
                deliveries,
                at: rfc3339(now - secs_ago * 1000),
            };
            Ok(QueueDetail {
                name: row.name,
                tenant: row.tenant,
                single_active: true,
                ttl_ms: Some(86_400_000),
                dlq: Some("payments.capture.dlq".into()),
                max_deliveries: Some(5),
                binding: Some("payments · payment.*.capture".into()),
                ready: row.ready,
                in_flight: 118,
                in_open_txns: 6,
                publish_per_sec: Some(820.0 * wave(self.secs(), 17.0, 0.05)),
                ack_per_sec: Some(805.0 * wave(self.secs(), 19.0, 0.05)),
                dead_lettered: row.dead_lettered,
                levels: vec![
                    PriorityLevel { level: 2, leader: "broker-1".into(), ready: 12, oldest_ms: Some(400) },
                    PriorityLevel { level: 1, leader: "broker-2".into(), ready: 1_880, oldest_ms: Some(130_000) },
                    PriorityLevel { level: 0, leader: "broker-1".into(), ready: 50, oldest_ms: Some(581_000) },
                ],
                consumers: vec![
                    consumer("capture-worker-1", "10.4.6.21 → broker-2", true),
                    consumer("capture-worker-2", "10.4.6.22 → broker-2", false),
                    consumer("capture-worker-4", "10.4.7.30 → broker-1", false),
                ],
                dead_letters: vec![
                    dead(4_812_330, "pay_8Hq2LmT", "Rejected without requeue (Nack requeue=false)", 1, 108),
                    dead(4_812_118, "pay_3vR9xKa", "Max deliveries reached (5)", 5, 183),
                    dead(4_811_904, "pay_Q0d1sWe", "Max deliveries reached (5)", 5, 267),
                    dead(4_809_215, "pay_kP7yNb2", "TTL expired (24 h)", 0, 832),
                    dead(4_808_977, "pay_Zt4uHc9", "Max deliveries reached (5)", 5, 914),
                ],
                handover: vec![
                    HandoverEvent {
                        at: rfc3339(now - 5_576_000),
                        text: "capture-worker-3 missed its credit deadline (30 s) and was dropped.".into(),
                    },
                    HandoverEvent {
                        at: rfc3339(now - 5_576_000),
                        text: "Its 2 open ack transactions were aborted; 14 messages became deliverable again.".into(),
                    },
                    HandoverEvent {
                        at: rfc3339(now - 5_575_000),
                        text: "capture-worker-1 became the active consumer and received them first.".into(),
                    },
                ],
            })
        }
        .await?;
        self.overlay_queue(&mut out);
        Ok(out)
    }

    async fn create_queue(&self, who: &Identity, req: &CreateQueue) -> ApiResult<()> {
        let Some(wanted) = &req.tenant else {
            return Err(ApiError::BadRequest("Choose the tenant the queue belongs to.".into()));
        };
        let tenants = self.tenants(who).await?;
        let tenant = tenants
            .tenants
            .iter()
            .find(|t| &t.name == wanted)
            .ok_or_else(|| ApiError::NotFound(format!("There is no tenant \"{wanted}\".")))?;
        if tenant.quota_bytes.is_some_and(|q| tenant.used_bytes >= q) {
            return Err(ApiError::Forbidden(format!(
                "Tenant {} has reached its storage quota. Raise the quota before adding queues.",
                tenant.name
            )));
        }
        if self.queues(who).await?.iter().any(|q| q.name == req.name) {
            return Err(ApiError::Conflict(format!("A queue named \"{}\" exists already.", req.name)));
        }
        self.lock().created.insert(req.name.clone(), (req.clone(), who.principal.clone()));
        Ok(())
    }

    async fn read(
        &self,
        who: &Identity,
        stream: &str,
        partition: u32,
        offset: u64,
        count: u32,
        isolation: Isolation,
    ) -> ApiResult<Vec<RecordView>> {
        let detail = self.stream(who, stream).await?;
        let p = detail
            .partitions
            .iter()
            .find(|p| p.id == partition)
            .ok_or_else(|| ApiError::NotFound(format!("{stream} has no partition {partition}.")))?;
        if offset < p.log_start {
            return Err(ApiError::BadRequest(format!(
                "Offset {} is before the log start of {stream} P{partition} ({}). Start at {} or later.",
                group(offset),
                group(p.log_start),
                group(p.log_start)
            )));
        }
        let now = now_ms();
        let mut out = Vec::new();
        let mut o = offset;
        while out.len() < count as usize && o < p.high_watermark {
            let r = synth_record(stream, o, p.high_watermark, now);
            match (isolation, r.txn) {
                (Isolation::Committed, TxnState::Open) => break,
                (Isolation::Committed, TxnState::Aborted | TxnState::AbortMarker | TxnState::CommitMarker) => {}
                _ => out.push(r),
            }
            o += 1;
        }
        Ok(out)
    }

    async fn tenants(&self, _who: &Identity) -> ApiResult<TenantsView> {
        let t = |name: &str, used: u64, quota: u64, streams, queues, principals| {
            let ratio = used as f64 / quota as f64;
            let state = if ratio >= 1.0 {
                chip(Tone::Critical, "Quota reached, writes rejected")
            } else if ratio >= 0.8 {
                chip(Tone::Warn, &format!("{:.0} % used", ratio * 100.0))
            } else {
                chip(Tone::Ok, "Within quota")
            };
            Tenant { name: name.into(), used_bytes: used, quota_bytes: Some(quota), streams, queues, principals, state }
        };
        let created = |tenant: &str| {
            self.lock().created.values().filter(|(q, _)| q.tenant.as_deref() == Some(tenant)).count() as u32
        };
        let b = |principal: &str, auth: &str, role: &str, resources: &[&str]| RoleBinding {
            principal: principal.into(),
            auth: auth.into(),
            role: role.into(),
            resources: resources.iter().map(|r| (*r).to_string()).collect(),
        };
        Ok(TenantsView {
            tenants: vec![
                t("analytics", (2.03 * TIB as f64) as u64, 2 * TIB, 31, 2 + created("analytics"), 9),
                t("finance", 412 * GIB, 512 * GIB, 18, 7 + created("finance"), 6),
                t("commerce", (1.31 * TIB as f64) as u64, 2 * TIB, 64, 41 + created("commerce"), 23),
                t("platform", 380 * GIB, TIB, 22, 19 + created("platform"), 11),
                t("notify", 22 * GIB, 256 * GIB, 4, 27 + created("notify"), 5),
            ],
            bindings: vec![TenantBindings {
                tenant: "commerce".into(),
                bindings: vec![
                    b("CN=checkout-svc", "mTLS", "Producer", &["orders", "payments.*"]),
                    b("CN=capture-svc", "mTLS", "Consumer", &["payments.capture"]),
                    b("CN=billing-ledger", "mTLS", "Consumer", &["orders", "group billing-ledger"]),
                    b("group:commerce-oncall", "OIDC", "Operator", &["*"]),
                    b("pierre", "OIDC", "Tenant admin", &["*"]),
                ],
            }],
        })
    }

    async fn chart_sample(&self, _who: &Identity) -> ApiResult<ChartSample> {
        Ok(sample_at(self.secs()))
    }

    async fn visibility(&self, _who: &Identity) -> ApiResult<Visibility> {
        Ok(Visibility { resources: [DEMO_RESOURCE].into(), cluster_wide: true })
    }

    async fn pause_upgrade(&self, _who: &Identity) -> ApiResult<()> {
        self.lock().paused = true;
        Ok(())
    }

    async fn purge_queue(&self, who: &Identity, name: &str) -> ApiResult<u64> {
        let d = self.queue(who, name).await?;
        self.lock().purged.insert(name.to_string());
        Ok(d.ready)
    }

    async fn requeue(&self, who: &Identity, name: &str) -> ApiResult<u64> {
        let mut moved = 0;
        let mut is_dlq = false;
        for row in self.queues(who).await? {
            let d = self.queue(who, &row.name).await?;
            if d.dlq.as_deref() == Some(name) {
                is_dlq = true;
                moved += d.dead_letters.len() as u64;
            }
        }
        if !is_dlq {
            self.queue(who, name).await?;
            return Ok(0);
        }
        self.lock().requeued.insert(name.to_string());
        Ok(moved)
    }

    async fn move_leader(&self, who: &Identity, req: &MoveLeader) -> ApiResult<()> {
        let replicas: Vec<String> = match self.stream(who, &req.resource).await {
            Ok(s) => {
                let p = s.partitions.iter().find(|p| p.id == req.partition).ok_or_else(|| {
                    ApiError::NotFound(format!("{} has no partition {}.", req.resource, req.partition))
                })?;
                p.replicas.iter().map(|r| r.node.clone()).collect()
            }
            Err(ApiError::NotFound(_)) => {
                let q = self.queue(who, &req.resource).await?;
                if !q.levels.iter().any(|l| u32::from(l.level) == req.partition) {
                    return Err(ApiError::NotFound(format!("{} has no level {}.", req.resource, req.partition)));
                }
                ["broker-1", "broker-2", "broker-3"].map(String::from).to_vec()
            }
            Err(e) => return Err(e),
        };
        let key = (req.resource.clone(), req.partition);
        match &req.node {
            Some(n) if !replicas.contains(n) => Err(ApiError::BadRequest(format!(
                "{n} holds no replica of partition {} of {}; choose one of {}.",
                req.partition,
                req.resource,
                replicas.join(", ")
            ))),
            Some(n) => {
                self.lock().pins.insert(key, n.clone());
                Ok(())
            }
            None => {
                self.lock().pins.remove(&key);
                Ok(())
            }
        }
    }
    async fn users(&self, _who: &Identity) -> ApiResult<UsersView> {
        let st = self.lock();
        let bindings = self.bindings_of(&st);
        let mut names: BTreeMap<String, (u32, bool)> = SAMPLE_USERS
            .iter()
            .filter(|(n, _)| !st.gone.contains(*n))
            .map(|(n, configured)| ((*n).to_string(), (4096, *configured)))
            .collect();
        for (n, it) in &st.users {
            names.entry(n.clone()).and_modify(|e| e.0 = *it).or_insert((*it, false));
        }
        let users = names
            .into_iter()
            .map(|(name, (iterations, configured))| UserRow {
                roles: bindings
                    .iter()
                    .filter(|b| b.subject_kind == "user" && b.subject == name)
                    .map(|b| if b.tenant.is_empty() { b.role.clone() } else { format!("{} ({})", b.role, b.tenant) })
                    .collect(),
                name,
                mechanism: "SCRAM-SHA-512".into(),
                iterations,
                source: if configured { "configuration" } else { "runtime" }.into(),
            })
            .collect();
        Ok(UsersView { users })
    }

    async fn set_user_credential(&self, _who: &Identity, name: &str, credential: &Credential) -> ApiResult<()> {
        if credential.iterations < 4096 {
            return Err(ApiError::BadRequest("The cluster refused the password: too few iterations.".into()));
        }
        if SAMPLE_USERS.iter().any(|(n, configured)| *n == name && *configured) {
            return Err(ApiError::BadRequest(format!("{name} comes from the configuration and cannot be changed.")));
        }
        let mut st = self.lock();
        st.gone.remove(name);
        st.users.insert(name.into(), credential.iterations);
        Ok(())
    }

    async fn delete_user(&self, _who: &Identity, name: &str) -> ApiResult<()> {
        let mut st = self.lock();
        if SAMPLE_USERS.iter().any(|(n, configured)| *n == name && *configured) {
            return Err(ApiError::BadRequest(format!("{name} comes from the configuration and cannot be deleted.")));
        }
        let known = st.users.remove(name).is_some() || SAMPLE_USERS.iter().any(|(n, _)| *n == name);
        if !known || st.gone.contains(name) {
            return Err(ApiError::NotFound(format!("There is no user \"{name}\" on this cluster.")));
        }
        st.gone.insert(name.into());
        Ok(())
    }

    async fn bindings(&self, _who: &Identity) -> ApiResult<BindingsView> {
        let st = self.lock();
        Ok(BindingsView { bindings: self.bindings_of(&st), can_edit: true })
    }

    async fn alter_binding(&self, _who: &Identity, binding: &BindingRow, add: bool) -> ApiResult<()> {
        let b = normalised(binding)?;
        let same = |x: &BindingRow| {
            (&x.subject_kind, &x.subject, &x.role, &x.tenant, &x.prefix)
                == (&b.subject_kind, &b.subject, &b.role, &b.tenant, &b.prefix)
        };
        let mut st = self.lock();
        if add {
            st.removed.retain(|x| !same(x));
            if !sample_bindings().iter().any(&same) && !st.added.iter().any(&same) {
                st.added.push(b);
            }
        } else {
            if sample_bindings().iter().any(|x| same(x) && x.source == "configuration") {
                return Err(ApiError::BadRequest(
                    "That binding comes from the configuration and cannot be removed.".into(),
                ));
            }
            st.added.retain(|x| !same(x));
            if sample_bindings().iter().any(&same) {
                st.removed.push(b);
            }
        }
        Ok(())
    }

    async fn revoke_sessions(&self, _who: &Identity, subject: &str, group: bool) -> ApiResult<()> {
        self.lock().revoked.insert((group, subject.into()));
        Ok(())
    }
}

const DISK_LIMITS: DiskLimits = DiskLimits { soft_free: 0.20, hard_free: 0.10 };

fn chip(tone: Tone, text: &str) -> Chip {
    Chip { tone, text: text.into() }
}

/// `1 + amp·sin(2πt/period)`: slow, smooth drift for demo numbers.
fn wave(t: f64, period: f64, amp: f64) -> f64 {
    1.0 + amp * (TAU * t / period).sin()
}

/// Produce rate `s` seconds after start (negative: before), with a daily
/// swing so a 24 h chart has a shape.
/// The demo's history has one resource that stands for all of its traffic,
/// and everyone may see it.
const DEMO_RESOURCE: u64 = 1;

fn sample_at(s: f64) -> ChartSample {
    let in_rate = in_rate_at(s);
    ChartSample {
        resources: [(DEMO_RESOURCE, [Some(in_rate), Some(in_rate * 2.73), Some(in_rate / 238.0)])].into(),
        commit_p99_ms: Some(7.8 * wave(s, 900.0, 0.08)),
    }
}

fn in_rate_at(s: f64) -> f64 {
    48.2 * MIB * wave(s, 1_800.0, 0.05) * wave(s, 420.0, 0.02) * wave(s, 86_400.0, 0.22)
}

fn billing_lag(secs: f64) -> u64 {
    12_480 + (secs * 3.0) as u64
}

fn partition_hw(stream: &str, p: u32) -> u64 {
    let salt = stream.bytes().fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
    18_400_000 + ((u64::from(p) * 7_919 + salt) % 90_000) * 11
}

/// Thousands separators: 1284 → "1,284".
pub fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// A plausible record for `offset`, following a fixed pattern of twelve:
/// plain records, one aborted transaction with its marker, one committed
/// four-record batch, one transaction left open and a commit marker.
fn synth_record(stream: &str, offset: u64, hw: u64, now: u64) -> RecordView {
    let slot = offset % 12;
    let txn = match slot {
        1 | 5..=8 => TxnState::Committed,
        2 => TxnState::Aborted,
        3 => TxnState::AbortMarker,
        9 => TxnState::Open,
        10 => TxnState::CommitMarker,
        _ => TxnState::None,
    };
    let (batch_base, batch_last) =
        if (5..=8).contains(&slot) { (offset - (slot - 5), offset - (slot - 5) + 3) } else { (offset, offset) };
    let marker = matches!(txn, TxnState::CommitMarker | TxnState::AbortMarker);
    let order = format!("ord_{}", base62(batch_base / 2 + 91_000_000));
    let value = (!marker).then(|| {
        format!(
            "{{\n  \"order_id\": \"{order}\",\n  \"status\": \"{}\",\n  \"currency\": \"EUR\",\n  \"total_minor\": {}\n}}",
            ["paid", "created", "shipped", "paid"][(offset % 4) as usize],
            2_000 + (offset * 37) % 9_000
        )
    });
    let size = value.as_ref().map_or(24, |v| v.len() as u32);
    let headers = if marker {
        vec![]
    } else {
        vec![
            RecordHeader { key: "lotse.schema-id".into(), value: Some("42".into()) },
            RecordHeader { key: "content-type".into(), value: Some("application/json".into()) },
            RecordHeader {
                key: "traceparent".into(),
                value: Some(format!(
                    "00-{:032x}-{:016x}-01",
                    u128::from(offset) * 0x9E37_79B9_7F4A_7C15,
                    offset ^ 0xA5A5
                )),
            },
        ]
    };
    let transactional = txn != TxnState::None;
    RecordView {
        offset,
        timestamp: rfc3339(now.saturating_sub((hw - offset) * 26)),
        key: (!marker).then_some(order),
        value,
        size_bytes: size,
        headers,
        txn,
        producer_id: if transactional { 7_311 } else { 0 },
        producer_epoch: if transactional { 2 } else { 0 },
        sequence: if transactional { (offset % 100_000) as i32 } else { -1 },
        batch_base,
        batch_last,
        term: 18 + u64::from(stream.len() as u32 % 2),
    }
}

fn base62(mut n: u64) -> String {
    const A: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut s = Vec::new();
    while n > 0 {
        s.push(A[(n % 62) as usize]);
        n /= 62;
    }
    s.reverse();
    String::from_utf8(s).expect("ascii")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn me() -> Identity {
        Identity::anonymous("tester")
    }

    #[tokio::test]
    async fn purge_requeue_and_pinned_leaders_show() {
        let c = DemoCluster::new("demo");
        let before = c.queue(&me(), "payments.capture").await.unwrap();
        assert!(before.ready > 0 && !before.dead_letters.is_empty());
        assert_eq!(c.purge_queue(&me(), "payments.capture").await.unwrap(), before.ready);
        assert_eq!(c.queue(&me(), "payments.capture").await.unwrap().ready, 0);
        let row = c.queues(&me()).await.unwrap().into_iter().find(|r| r.name == "payments.capture").unwrap();
        assert_eq!(row.ready, 0, "the list agrees");
        assert!(matches!(c.purge_queue(&me(), "nothing").await, Err(ApiError::NotFound(_))));

        let moved = c.requeue(&me(), "payments.capture.dlq").await.unwrap();
        assert_eq!(moved, before.dead_letters.len() as u64);
        assert!(c.queue(&me(), "payments.capture").await.unwrap().dead_letters.is_empty());

        let s = c.streams(&me()).await.unwrap().remove(0);
        let detail = c.stream(&me(), &s.name).await.unwrap();
        let p = &detail.partitions[0];
        let to = p.replicas.iter().map(|r| r.node.clone()).find(|n| *n != p.leader).unwrap();
        let pin = MoveLeader { resource: s.name.clone(), partition: p.id, node: Some(to.clone()) };
        c.move_leader(&me(), &pin).await.unwrap();
        assert_eq!(c.stream(&me(), &s.name).await.unwrap().partitions[0].leader, to);
        let bad = MoveLeader { node: Some("broker-9".into()), ..pin.clone() };
        assert!(matches!(c.move_leader(&me(), &bad).await, Err(ApiError::BadRequest(_))));
        c.move_leader(&me(), &MoveLeader { node: None, ..pin }).await.unwrap();
        assert_eq!(c.stream(&me(), &s.name).await.unwrap().partitions[0].leader, p.leader, "unpinned");
    }

    #[test]
    fn thousands() {
        assert_eq!(group(0), "0");
        assert_eq!(group(999), "999");
        assert_eq!(group(1_284), "1,284");
        assert_eq!(group(18_442_107), "18,442,107");
    }

    #[tokio::test]
    async fn leaders_add_up_and_nobody_is_led_twice() {
        let c = DemoCluster::new("t");
        let o = c.overview(&me()).await.unwrap();
        assert_eq!(o.nodes.iter().map(|n| n.leads).sum::<u32>(), RAFT_GROUPS);
        let produce: f64 = o.nodes.iter().filter_map(|n| n.produce_bytes_per_sec).sum();
        assert!((produce - o.throughput.in_bytes_per_sec.unwrap()).abs() < 1.0);
    }

    #[tokio::test]
    async fn committed_reads_hide_aborted_data_and_stop_at_open_transactions() {
        let c = DemoCluster::new("t");
        let start = c.stream(&me(), "orders").await.unwrap().partitions[4].log_start;
        let aligned = start - start % 12 + 12;
        let all = c.read(&me(), "orders", 4, aligned, 12, Isolation::Uncommitted).await.unwrap();
        assert_eq!(all.len(), 12);
        let committed = c.read(&me(), "orders", 4, aligned, 12, Isolation::Committed).await.unwrap();
        assert!(committed.iter().all(|r| matches!(r.txn, TxnState::None | TxnState::Committed)));
        // Slots 0, 1, 4, 5..8 are visible; slot 9 is an open transaction, so the read stops there.
        assert_eq!(committed.len(), 7);
    }

    #[tokio::test]
    async fn reads_before_log_start_explain_where_to_start() {
        let c = DemoCluster::new("t");
        let err = c.read(&me(), "orders", 0, 0, 10, Isolation::Committed).await.unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(m) if m.contains("Start at")));
    }

    #[tokio::test]
    async fn created_queues_show_up_and_names_stay_unique() {
        let c = DemoCluster::new("t");
        let req = CreateQueue {
            name: "payments.refund".into(),
            tenant: Some("commerce".into()),
            priority_levels: 3,
            single_active: true,
            ttl_ms: None,
            dlq: None,
            max_deliveries: None,
            create_dlq: false,
        };
        c.create_queue(&me(), &req).await.unwrap();
        assert!(c.queues(&me()).await.unwrap().iter().any(|q| q.name == "payments.refund"));
        let q = c.queue(&me(), "payments.refund").await.unwrap();
        assert_eq!(q.levels.iter().map(|l| l.level).collect::<Vec<_>>(), vec![2, 1, 0]);
        assert!(q.handover[0].text.contains("tester"));
        assert!(matches!(c.create_queue(&me(), &req).await, Err(ApiError::Conflict(_))));
    }

    #[tokio::test]
    async fn a_tenant_over_quota_cannot_add_queues() {
        let c = DemoCluster::new("t");
        let req = CreateQueue {
            name: "x".into(),
            tenant: Some("analytics".into()),
            priority_levels: 1,
            single_active: false,
            ttl_ms: None,
            dlq: None,
            max_deliveries: None,
            create_dlq: false,
        };
        assert!(matches!(c.create_queue(&me(), &req).await, Err(ApiError::Forbidden(_))));
    }
}

//! A real Lotse cluster, over the native protocol through `lotse-client`.
//!
//! What exists comes from `Metadata` (resources, partitions, leaders,
//! voters, bindings); the numbers from `DescribeMetrics`, asked of every
//! node (ADR 0036). Nodes count since they started, so a rate is the
//! difference of two samples: the cluster keeps the samples of the last few
//! minutes and takes each rate over the newest sample at least
//! [`RATE_WINDOW`] old. Until there is one, rates are `null`. Samples are
//! taken at most once a second, whoever asks.
//!
//! The console has no account of its own (ADR 0019). Each principal gets a
//! seat: a client signed in as them (their SCRAM password, or their access
//! token with OAUTHBEARER, renewed as the console renews it) and its own
//! samples, so every screen shows what that principal may see and nothing
//! else. Seats nobody used for [`SEAT_IDLE`] are closed.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lotse_client::{Client, ClientConfig, ClientError, Credentials, DEFAULT_TENANT};
use lotse_proto::{
    DescribeMetricsResponse, ErrorCode, MetadataResponse, ResourceMetadata, RoleBindingData, kind, role, subject_kind,
};
use lotse_record::{Batch, Control, ControlType};
use lotse_rt_tokio::TokioRuntime;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use super::Cluster;
use crate::auth::{Identity, Login};
use crate::error::{ApiError, ApiResult};
use crate::model::*;
use crate::time::rfc3339;

/// Rates are taken over at least this long.
pub const RATE_WINDOW: Duration = Duration::from_secs(5);
/// Samples are kept this long.
const HISTORY: Duration = Duration::from_secs(300);
/// A sample this young is used again instead of asking the nodes.
const FRESH: Duration = Duration::from_secs(1);
/// A seat unused this long is closed.
pub const SEAT_IDLE: Duration = Duration::from_secs(900);

/// What the cluster said at one moment.
#[derive(Debug)]
struct Sample {
    at: Instant,
    meta: MetadataResponse,
    /// Every node's metrics, or why it did not answer.
    nodes: BTreeMap<u32, Result<DescribeMetricsResponse, String>>,
}

impl Sample {
    fn answered(&self) -> impl Iterator<Item = &DescribeMetricsResponse> {
        self.nodes.values().filter_map(|r| r.as_ref().ok())
    }

    fn resource(&self, name: &str, k: u8) -> Option<&ResourceMetadata> {
        self.meta.resources.iter().find(|r| r.name == name && r.kind == k && r.error == ErrorCode::NONE)
    }

    fn resources(&self, k: u8) -> impl Iterator<Item = &ResourceMetadata> {
        self.meta.resources.iter().filter(move |r| r.kind == k && r.error == ErrorCode::NONE)
    }

    /// The leader's report of `(resource, partition)`.
    fn lead(&self, resource_id: u64, partition: u32) -> Option<(u32, &lotse_proto::PartitionMetrics)> {
        self.answered().find_map(|m| {
            m.partitions
                .iter()
                .find(|p| p.leader && p.resource_id == resource_id && p.partition == partition)
                .map(|p| (m.node_id, p))
        })
    }

    /// Every replica's report of a resource's partitions.
    fn replicas(&self, resource_id: u64) -> impl Iterator<Item = &lotse_proto::PartitionMetrics> {
        self.answered().flat_map(|m| &m.partitions).filter(move |p| p.resource_id == resource_id && p.group != 0)
    }
}

/// A rate between two samples. See the module docs.
#[derive(Debug, Clone, Copy)]
struct Window<'a> {
    now: &'a Sample,
    base: Option<&'a Sample>,
}

impl Window<'_> {
    /// Per second, the growth of `counter` summed over nodes that answered
    /// both times and did not restart in between; `None` without a base or
    /// any such node.
    fn rate(&self, counter: impl Fn(&DescribeMetricsResponse) -> u64) -> Option<f64> {
        let base = self.base?;
        let secs = (self.now.at - base.at).as_secs_f64();
        let mut sum = 0u64;
        let mut any = false;
        for (id, now) in &self.now.nodes {
            let (Ok(now), Some(Ok(then))) = (now, base.nodes.get(id)) else { continue };
            if now.uptime_ms < then.uptime_ms {
                continue; // restarted: its counters started over
            }
            sum += counter(now).saturating_sub(counter(then));
            any = true;
        }
        (any && secs > 0.0).then(|| sum as f64 / secs)
    }

    /// p99 of the produces acknowledged in the window, as the upper bound
    /// of its histogram bucket; `None` without produces.
    fn commit_p99_ms(&self) -> Option<f64> {
        let base = self.base?;
        let mut buckets = vec![0u64; lotse_proto::LATENCY_BUCKETS];
        for (id, now) in &self.now.nodes {
            let (Ok(now), Some(Ok(then))) = (now, base.nodes.get(id)) else { continue };
            if now.uptime_ms < then.uptime_ms {
                continue;
            }
            for (i, b) in buckets.iter_mut().enumerate() {
                let (n, t) = (now.commit_latency.get(i).copied(), then.commit_latency.get(i).copied());
                *b += n.unwrap_or(0).saturating_sub(t.unwrap_or(0));
            }
        }
        p99(&buckets)
    }
}

/// The upper bound, in ms, of the bucket that holds the 99th percentile.
fn p99(buckets: &[u64]) -> Option<f64> {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return None;
    }
    let want = total - total / 100;
    let mut seen = 0;
    for (i, b) in buckets.iter().enumerate() {
        seen += b;
        if seen >= want {
            return Some((1u64 << i.min(62)) as f64);
        }
    }
    None
}

/// A real cluster. See the module docs.
#[derive(Debug)]
pub struct NativeCluster {
    id: String,
    bootstrap: Vec<String>,
    rt: TokioRuntime,
    /// By principal and how it signs in (for a password, its hash: another
    /// password is another seat, which the cluster checks anew).
    seats: Mutex<BTreeMap<SeatKey, Arc<Seat>>>,
}

/// A principal and how it signs in.
type SeatKey = (String, Vec<u8>);

/// One principal's connection to the cluster.
struct Seat {
    client: Client<TokioRuntime>,
    /// The access token the client signs in with, for OAUTHBEARER, and
    /// when it expires.
    token: Arc<Mutex<(String, Option<Instant>)>>,
    samples: Mutex<VecDeque<Arc<Sample>>>,
    /// One sample at a time: concurrent callers wait and share it.
    sampling: tokio::sync::Mutex<()>,
    used: Mutex<Instant>,
}

impl std::fmt::Debug for Seat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Seat").finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Seat {
    fn samples(&self) -> std::sync::MutexGuard<'_, VecDeque<Arc<Sample>>> {
        lock(&self.samples)
    }

    /// A fresh sample and the base its rates are taken over.
    async fn window(&self) -> ApiResult<(Arc<Sample>, Option<Arc<Sample>>)> {
        let _turn = self.sampling.lock().await;
        let now = Instant::now();
        let fresh = self.samples().back().filter(|s| now - s.at < FRESH).cloned();
        let sample = match fresh {
            Some(s) => s,
            None => {
                let meta = self.client.metadata(&[]).await.map_err(failed)?;
                let nodes = self.client.node_metrics().await.map_err(failed)?;
                let nodes = nodes.into_iter().map(|(id, r)| (id, r.map_err(|e| e.to_string()))).collect();
                let s = Arc::new(Sample { at: Instant::now(), meta, nodes });
                let mut all = self.samples();
                all.push_back(s.clone());
                while all.front().is_some_and(|f| s.at - f.at > HISTORY) {
                    all.pop_front();
                }
                s
            }
        };
        let base = self.samples().iter().rev().find(|b| sample.at - b.at >= RATE_WINDOW).cloned();
        Ok((sample, base))
    }
}

fn node_name(id: u32) -> String {
    format!("node-{id}")
}

fn chip(tone: Tone, text: &str) -> Chip {
    Chip { tone, text: text.into() }
}

/// What the user is told when a call fails.
fn failed(e: ClientError) -> ApiError {
    match e {
        ClientError::Authentication(why) => {
            ApiError::Forbidden(format!("The cluster did not accept your sign-in: {why}"))
        }
        ClientError::Broker { code: ErrorCode::UNAUTHORIZED, message } => {
            ApiError::Forbidden(message.unwrap_or_else(|| "Your roles on this cluster do not allow this.".into()))
        }
        e => ApiError::Unavailable(format!("The cluster does not answer: {e}")),
    }
}

fn tenant_of(r: &ResourceMetadata) -> Option<String> {
    (!r.tenant.is_empty()).then(|| r.tenant.clone())
}

impl NativeCluster {
    /// A cluster called `id`, reached through `bootstrap` with plain TCP on
    /// loopback: for local clusters and tests.
    pub fn new(id: impl Into<String>, bootstrap: Vec<String>) -> Self {
        // The client reads no files; the runtime's root is never used.
        NativeCluster::with_runtime(id, bootstrap, TokioRuntime::new(std::env::temp_dir()).plaintext_on_loopback())
    }

    /// A cluster called `id`, reached through `bootstrap` on `rt`'s network
    /// (TLS, normally).
    pub fn with_runtime(id: impl Into<String>, bootstrap: Vec<String>, rt: TokioRuntime) -> Self {
        NativeCluster { id: id.into(), bootstrap, rt, seats: Mutex::new(BTreeMap::new()) }
    }

    /// The seat of `who`, opened if need be; closes idle ones.
    fn seat(&self, who: &Identity) -> Arc<Seat> {
        let how = match &who.login {
            Login::None => vec![0],
            Login::Password(p) => [&[1u8][..], &Sha256::digest(p.expose().as_bytes())].concat(),
            Login::Token { .. } => vec![2],
        };
        let now = Instant::now();
        let mut seats = lock(&self.seats);
        seats.retain(|_, s| now - *lock(&s.used) < SEAT_IDLE);
        let seat = seats.entry((who.principal.clone(), how)).or_insert_with(|| {
            let token = Arc::new(Mutex::new((String::new(), None)));
            let credentials = match &who.login {
                Login::None => None,
                Login::Password(p) => {
                    Some(Credentials::Scram { user: who.principal.clone(), password: p.expose().into() })
                }
                Login::Token { .. } => {
                    let t = Arc::clone(&token);
                    Some(Credentials::OAuthBearer(Arc::new(move || lock(&t).0.clone())))
                }
            };
            let cfg = ClientConfig {
                request_timeout: Duration::from_secs(2),
                retry_for: Duration::from_secs(5),
                credentials,
                ..ClientConfig::new(self.bootstrap.clone())
            };
            Arc::new(Seat {
                client: Client::new(self.rt.clone(), cfg),
                token,
                samples: Mutex::new(VecDeque::new()),
                sampling: tokio::sync::Mutex::new(()),
                used: Mutex::new(now),
            })
        });
        *lock(&seat.used) = now;
        if let Login::Token { token, expires } = &who.login {
            // The newest token, for the next time the client signs in; a
            // caller holding an older one (a live view opened earlier)
            // does not set it back.
            let mut held = lock(&seat.token);
            if held.1.is_none_or(|e| *expires >= e) {
                *held = (token.expose().to_owned(), Some(*expires));
            }
        }
        Arc::clone(seat)
    }

    /// How many seats are open (for tests).
    pub fn seats(&self) -> usize {
        lock(&self.seats).len()
    }

    fn nodes_of(&self, w: Window<'_>) -> Vec<Node> {
        let s = w.now;
        s.nodes
            .iter()
            .map(|(&id, r)| match r {
                Ok(m) => {
                    let leads = m.partitions.iter().filter(|p| p.leader).count() as u32;
                    let mine = |f: &dyn Fn(&DescribeMetricsResponse) -> u64| {
                        w.rate(|n| if n.node_id == id { f(n) } else { 0 })
                    };
                    let role = if m.partitions.iter().any(|p| p.leader && p.group == m.metadata_group) {
                        "Metadata leader"
                    } else {
                        "Voter"
                    };
                    let h = m.host;
                    Node {
                        id: node_name(id),
                        role: role.into(),
                        zone: None,
                        uptime_secs: m.uptime_ms / 1000,
                        leads,
                        replicas: m.partitions.len() as u32,
                        disk_free_bytes: h.disk_free_bytes,
                        disk_total_bytes: h.disk_total_bytes,
                        produce_bytes_per_sec: mine(&|n| n.partitions.iter().map(|p| p.produce_bytes).sum()),
                        net_in_bytes_per_sec: mine(&|n| n.client_in_bytes + n.peer_in_bytes),
                        net_out_bytes_per_sec: mine(&|n| n.client_out_bytes + n.peer_out_bytes),
                        connections: m.connections,
                        cpu: h.cpu_permille.map(|c| f64::from(c) / 1000.0),
                        memory_bytes: h.memory_bytes,
                        memory_limit_bytes: h.memory_limit_bytes,
                        version: m.version.clone(),
                        status: chip(Tone::Ok, "Answering"),
                    }
                }
                Err(_) => Node {
                    id: node_name(id),
                    role: "Unknown".into(),
                    zone: None,
                    uptime_secs: 0,
                    leads: 0,
                    replicas: 0,
                    disk_free_bytes: None,
                    disk_total_bytes: None,
                    produce_bytes_per_sec: None,
                    net_in_bytes_per_sec: None,
                    net_out_bytes_per_sec: None,
                    connections: 0,
                    cpu: None,
                    memory_bytes: None,
                    memory_limit_bytes: None,
                    version: String::new(),
                    status: chip(Tone::Critical, "Not answering"),
                },
            })
            .collect()
    }

    fn stream_row(&self, w: Window<'_>, r: &ResourceMetadata) -> StreamRow {
        let id = r.resource_id;
        StreamRow {
            name: r.name.clone(),
            tenant: tenant_of(r),
            partitions: r.partitions.len() as u32,
            in_bytes_per_sec: w
                .rate(|n| n.partitions.iter().filter(|p| p.resource_id == id).map(|p| p.produce_bytes).sum()),
            stored_bytes: w.now.replicas(id).map(|p| p.size_bytes).sum(),
        }
    }

    fn queue_row(&self, s: &Sample, r: &ResourceMetadata) -> QueueRow {
        let q = s.answered().flat_map(|m| &m.queues).find(|q| q.resource_id == r.resource_id);
        let attention = match q {
            None => Some(chip(Tone::Warn, "No leader serving it")),
            Some(q) if q.ready > 0 && q.consumers.is_empty() => Some(chip(Tone::Warn, "No consumers")),
            Some(_) => None,
        };
        QueueRow {
            name: r.name.clone(),
            tenant: tenant_of(r),
            ready: q.map_or(0, |q| q.ready),
            dead_lettered: None,
            attention,
        }
    }
}

#[async_trait]
impl Cluster for NativeCluster {
    fn id(&self) -> &str {
        &self.id
    }

    async fn summary(&self, who: &Identity) -> ApiResult<ClusterRef> {
        let (s, _) = match self.seat(who).window().await {
            Ok(w) => w,
            Err(e) => {
                return Ok(ClusterRef {
                    id: self.id.clone(),
                    nodes: 0,
                    status: e.to_string(),
                    tone: Tone::Critical,
                    demo: false,
                    tenants: true,
                    queue_options: false,
                });
            }
        };
        let (all, up) = (s.nodes.len() as u32, s.answered().count() as u32);
        // Shown next to the node count.
        let (status, tone) = if up == all {
            ("all answering".to_string(), Tone::Ok)
        } else {
            (format!("{} not answering", all - up), Tone::Critical)
        };
        Ok(ClusterRef {
            id: self.id.clone(),
            nodes: all,
            status,
            tone,
            demo: false,
            tenants: true,
            queue_options: false,
        })
    }

    async fn overview(&self, who: &Identity) -> ApiResult<Overview> {
        let (s, base) = self.seat(who).window().await?;
        let w = Window { now: &s, base: base.as_deref() };
        let nodes = self.nodes_of(w);
        let groups: BTreeSet<u64> = s.answered().flat_map(|m| m.partitions.iter().map(|p| p.group)).collect();
        let led: BTreeSet<u64> =
            s.answered().flat_map(|m| m.partitions.iter().filter(|p| p.leader).map(|p| p.group)).collect();
        let meta_group = s.answered().map(|m| m.metadata_group).find(|g| *g != 0).unwrap_or(0);
        let meta_lead = s
            .answered()
            .find_map(|m| m.partitions.iter().find(|p| p.leader && p.group == meta_group).map(|p| (m.node_id, p.term)));
        // A replica behind its leader's high watermark.
        let behind: usize = s
            .answered()
            .flat_map(|m| &m.partitions)
            .filter(|p| p.leader)
            .map(|p| p.replicas.iter().filter(|r| r.log_end < p.high_watermark).count())
            .sum();
        let mut alerts = Vec::new();
        for (id, r) in &s.nodes {
            if let Err(why) = r {
                alerts.push(Alert {
                    id: format!("node-down-{id}"),
                    tone: Tone::Critical,
                    title: format!("{} does not answer", node_name(*id)),
                    detail: format!("Its numbers are missing from every screen. {why}"),
                    node: Some(node_name(*id)),
                });
            }
        }
        let leaderless = groups.difference(&led).count();
        if leaderless > 0 {
            alerts.push(Alert {
                id: "leaderless".into(),
                tone: Tone::Warn,
                title: format!("{leaderless} Raft groups report no leader"),
                detail: "Writes to them wait until a leader is elected, or until their nodes answer again.".into(),
                node: None,
            });
        }
        let consumers: usize = s.answered().flat_map(|m| &m.queues).map(|q| q.consumers.len()).sum::<usize>()
            + s.answered().flat_map(|m| &m.groups).map(|g| g.members.len()).sum::<usize>();
        let versions: BTreeSet<&str> = s.answered().map(|m| m.version.as_str()).collect();
        let health = vec![
            HealthItem {
                label: "Raft groups with a leader".into(),
                value: format!("{} of {}", led.len(), groups.len()),
                detail: "Every group needs a leader to take writes.".into(),
                chip: Some(if leaderless == 0 { chip(Tone::Ok, "All led") } else { chip(Tone::Warn, "Some without") }),
            },
            HealthItem {
                label: "Replicas behind".into(),
                value: behind.to_string(),
                detail: "Replicas that do not hold everything their leader has committed.".into(),
                chip: Some(if behind == 0 { chip(Tone::Ok, "In sync") } else { chip(Tone::Warn, "Catching up") }),
            },
            HealthItem {
                label: "Versions".into(),
                value: versions.into_iter().collect::<Vec<_>>().join(", "),
                detail: "Versions the answering nodes run.".into(),
                chip: None,
            },
        ];
        let mut busiest: Vec<StreamRow> = s.resources(kind::STREAM).map(|r| self.stream_row(w, r)).collect();
        busiest.sort_by(|a, b| b.in_bytes_per_sec.unwrap_or(0.0).total_cmp(&a.in_bytes_per_sec.unwrap_or(0.0)));
        busiest.truncate(5);
        let mut attention: Vec<QueueRow> = s.resources(kind::QUEUE).map(|r| self.queue_row(&s, r)).collect();
        attention.retain(|q| q.attention.is_some());
        Ok(Overview {
            cluster: self.id.clone(),
            raft_groups: groups.len() as u32,
            metadata_leader: meta_lead.map_or_else(|| "none".into(), |(n, _)| node_name(n)),
            metadata_term: meta_lead.map_or(0, |(_, t)| t),
            alerts,
            throughput: Throughput {
                in_bytes_per_sec: w.rate(|n| n.partitions.iter().map(|p| p.produce_bytes).sum()),
                out_bytes_per_sec: w.rate(|n| n.client_out_bytes),
                records_per_sec: w.rate(|n| n.partitions.iter().map(|p| p.produce_records).sum()),
                commit_p99_ms: w.commit_p99_ms(),
                stored_bytes: s.answered().flat_map(|m| &m.partitions).map(|p| p.size_bytes).sum(),
                consumers: consumers as u32,
            },
            health,
            nodes,
            disk_limits: None,
            busiest_streams: busiest,
            attention_queues: attention,
            stream_count: s.resources(kind::STREAM).count() as u32,
            queue_count: s.resources(kind::QUEUE).count() as u32,
        })
    }

    async fn nodes(&self, who: &Identity) -> ApiResult<NodesView> {
        let (s, base) = self.seat(who).window().await?;
        let nodes = self.nodes_of(Window { now: &s, base: base.as_deref() });
        Ok(NodesView { nodes, disk_limits: None, upgrade: None, events: Vec::new() })
    }

    async fn streams(&self, who: &Identity) -> ApiResult<Vec<StreamRow>> {
        let (s, base) = self.seat(who).window().await?;
        let w = Window { now: &s, base: base.as_deref() };
        Ok(s.resources(kind::STREAM).map(|r| self.stream_row(w, r)).collect())
    }

    async fn stream(&self, who: &Identity, name: &str) -> ApiResult<StreamDetail> {
        let (s, base) = self.seat(who).window().await?;
        let w = Window { now: &s, base: base.as_deref() };
        let r = s
            .resource(name, kind::STREAM)
            .ok_or_else(|| ApiError::NotFound(format!("There is no stream \"{name}\" in {}.", self.id)))?;
        let id = r.resource_id;
        let partitions: Vec<Partition> = r
            .partitions
            .iter()
            .map(|pm| match s.lead(id, pm.partition) {
                Some((leader, p)) => Partition {
                    id: pm.partition,
                    leader: node_name(leader),
                    term: p.term,
                    replicas: p
                        .replicas
                        .iter()
                        .map(|rp| {
                            let behind = p.high_watermark.saturating_sub(rp.log_end);
                            let state = if rp.node_id == leader {
                                ReplicaState::Leader
                            } else if behind == 0 {
                                ReplicaState::InSync
                            } else {
                                ReplicaState::CatchingUp
                            };
                            Replica { node: node_name(rp.node_id), state, behind }
                        })
                        .collect(),
                    log_start: p.log_start,
                    high_watermark: p.high_watermark,
                    last_stable: p.last_stable,
                    size_bytes: p.size_bytes,
                },
                None => Partition {
                    id: pm.partition,
                    leader: "none".into(),
                    term: pm.leader_term,
                    replicas: pm
                        .voters
                        .iter()
                        .map(|v| Replica { node: node_name(*v), state: ReplicaState::CatchingUp, behind: 0 })
                        .collect(),
                    log_start: 0,
                    high_watermark: 0,
                    last_stable: 0,
                    size_bytes: 0,
                },
            })
            .collect();
        let hw: BTreeMap<u32, u64> = partitions.iter().map(|p| (p.id, p.high_watermark)).collect();
        let reading: Vec<&lotse_proto::GroupMetrics> = s
            .answered()
            .flat_map(|m| &m.groups)
            .filter(|g| {
                g.offsets.iter().any(|o| o.resource_id == id)
                    || g.members.iter().any(|m| m.owned.iter().any(|o| o.resource_id == id))
            })
            .collect();
        let groups = reading
            .iter()
            .map(|g| {
                let lag = g
                    .offsets
                    .iter()
                    .filter(|o| o.resource_id == id)
                    .map(|o| hw.get(&o.partition).copied().unwrap_or(0).saturating_sub(o.offset))
                    .sum();
                let state =
                    if g.members.is_empty() { chip(Tone::Info, "No members") } else { chip(Tone::Ok, "Stable") };
                Group {
                    id: g.group_id.clone(),
                    members: g.members.len() as u32,
                    state,
                    epoch: g.epoch.max(0) as u32,
                    lag,
                    lag_trend: Vec::new(),
                }
            })
            .collect();
        let coordinator = s
            .answered()
            .find_map(|m| m.partitions.iter().find(|p| p.leader && p.group == m.coordinator_group).map(|_| m.node_id));
        let assignment = reading.iter().find(|g| !g.members.is_empty()).map(|g| Assignment {
            group: g.group_id.clone(),
            coordinator: coordinator.map_or_else(|| "none".into(), node_name),
            members: g
                .members
                .iter()
                .map(|m| AssignedMember {
                    id: m.member_id.clone(),
                    owned: m.owned.iter().filter(|o| o.resource_id == id).flat_map(|o| o.partitions.clone()).collect(),
                    incoming: Vec::new(),
                })
                .collect(),
            note: None,
        });
        let per = |f: fn(&lotse_proto::PartitionMetrics) -> u64| {
            w.rate(move |n| n.partitions.iter().filter(|p| p.resource_id == id).map(f).sum())
        };
        Ok(StreamDetail {
            name: r.name.clone(),
            tenant: tenant_of(r),
            replication: r.partitions.first().map_or(0, |p| p.voters.len() as u32),
            retention_ms: None,
            compaction: false,
            in_bytes_per_sec: per(|p| p.produce_bytes),
            out_bytes_per_sec: per(|p| p.fetch_bytes),
            records_per_sec: per(|p| p.produce_records),
            stored_bytes: s.replicas(id).map(|p| p.size_bytes).sum(),
            oldest_record_age_ms: None,
            open_transactions: None,
            partitions,
            groups,
            assignment,
        })
    }

    async fn queues(&self, who: &Identity) -> ApiResult<Vec<QueueRow>> {
        let (s, _) = self.seat(who).window().await?;
        Ok(s.resources(kind::QUEUE).map(|r| self.queue_row(&s, r)).collect())
    }

    async fn queue(&self, who: &Identity, name: &str) -> ApiResult<QueueDetail> {
        let (s, base) = self.seat(who).window().await?;
        let w = Window { now: &s, base: base.as_deref() };
        let r = s
            .resource(name, kind::QUEUE)
            .ok_or_else(|| ApiError::NotFound(format!("There is no queue \"{name}\" in {}.", self.id)))?;
        let id = r.resource_id;
        let served = s.answered().find_map(|m| m.queues.iter().find(|q| q.resource_id == id).map(|q| (m.node_id, q)));
        let bindings: Vec<String> = s
            .resources(kind::EXCHANGE)
            .flat_map(|x| {
                let bound = x.exchange.iter().flat_map(|e| &e.bindings).filter(|b| b.queue_id == id);
                bound.map(move |b| {
                    if b.binding_key.is_empty() { x.name.clone() } else { format!("{} ({})", x.name, b.binding_key) }
                })
            })
            .collect();
        let consumers = served.map_or_else(Vec::new, |(node, q)| {
            q.consumers
                .iter()
                .map(|c| QueueConsumer {
                    tag: format!("subscription {}", c.subscription_id),
                    principal: None,
                    connection: format!("{} on {}", c.peer, node_name(node)),
                    in_flight: c.in_flight,
                    credit_messages: c.credit,
                    in_flight_bytes: None,
                    credit_bytes: None,
                    acks_per_sec: None,
                    state: chip(Tone::Ok, "Active"),
                })
                .collect()
        });
        let q = served.map(|(_, q)| q);
        Ok(QueueDetail {
            name: r.name.clone(),
            tenant: tenant_of(r),
            single_active: false,
            ttl_ms: None,
            dlq: None,
            max_deliveries: None,
            binding: (!bindings.is_empty()).then(|| bindings.join(", ")),
            ready: q.map_or(0, |q| q.ready),
            in_flight: q.map_or(0, |q| q.in_flight),
            in_open_txns: q.map_or(0, |q| q.in_open_txns),
            publish_per_sec: w
                .rate(|n| n.partitions.iter().filter(|p| p.resource_id == id).map(|p| p.produce_records).sum()),
            ack_per_sec: w.rate(|n| n.queues.iter().filter(|q| q.resource_id == id).map(|q| q.acked).sum()),
            dead_lettered: None,
            levels: vec![PriorityLevel {
                level: 0,
                leader: served.map_or_else(|| "none".into(), |(n, _)| node_name(n)),
                ready: q.map_or(0, |q| q.ready),
                oldest_ms: None,
            }],
            consumers,
            dead_letters: Vec::new(),
            handover: Vec::new(),
        })
    }

    async fn create_queue(&self, who: &Identity, req: &CreateQueue) -> ApiResult<()> {
        let unsupported = [
            (req.priority_levels != 1, "priority levels"),
            (req.single_active, "single active consumer"),
            (req.ttl_ms.is_some(), "message TTL"),
            (req.dlq.is_some() || req.create_dlq, "dead-letter queues"),
            (req.max_deliveries.is_some(), "max deliveries"),
        ];
        let missing: Vec<&str> = unsupported.iter().filter(|(on, _)| *on).map(|(_, what)| *what).collect();
        if !missing.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "This cluster does not support {} yet. Create the queue without them.",
                missing.join(", ")
            )));
        }
        let seat = self.seat(who);
        let meta = seat.client.metadata(&[]).await.map_err(failed)?;
        let replication = meta.brokers.len().clamp(1, 3) as u8;
        let tenant = req.tenant.as_deref().unwrap_or(DEFAULT_TENANT);
        match seat.client.create_queue_in(tenant, &req.name, replication, None).await {
            Ok(_) => Ok(()),
            Err(ClientError::ResourceExists { name, .. }) => Err(ApiError::Conflict(format!("{name} exists already."))),
            Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, message }) => Err(ApiError::BadRequest(
                format!("The cluster refused the queue: {}", message.unwrap_or_else(|| "invalid request".into())),
            )),
            Err(e) => Err(failed(e)),
        }
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
        let mut out: Vec<RecordView> = Vec::new();
        let mut at = offset;
        // Producers whose transaction is aborted from some offset on, until
        // their abort marker.
        let mut aborting: BTreeMap<u64, u64> = BTreeMap::new();
        let seat = self.seat(who);
        for _ in 0..8 {
            if out.len() >= count as usize {
                break;
            }
            let wait = Duration::from_millis(100);
            let f = match isolation {
                Isolation::Committed => seat.client.fetch(stream, partition, at, wait).await,
                Isolation::Uncommitted => seat.client.fetch_uncommitted(stream, partition, at, wait).await,
            };
            let f = match f {
                Ok(f) => f,
                Err(ClientError::UnknownStream(..)) => {
                    return Err(ApiError::NotFound(format!("There is no stream {stream} with partition {partition}.")));
                }
                Err(ClientError::Broker { code: ErrorCode::OFFSET_OUT_OF_RANGE, .. }) => {
                    return Err(ApiError::BadRequest(format!(
                        "Offset {at} is outside {stream} P{partition}. Start at its log start or later."
                    )));
                }
                Err(e) => return Err(failed(e)),
            };
            if f.records.is_empty() {
                break;
            }
            for &(producer, first) in &f.aborted {
                aborting.entry(producer).or_insert(first);
            }
            let mut rest = f.records.clone();
            while !rest.is_empty() {
                let Ok((batch, more)) = Batch::parse(&rest) else { break };
                rest = more;
                at = batch.next_offset();
                let records = views(&batch, f.last_stable_offset, &mut aborting);
                for r in records {
                    let hidden = isolation == Isolation::Committed
                        && matches!(r.txn, TxnState::Aborted | TxnState::CommitMarker | TxnState::AbortMarker);
                    if r.offset >= offset && !hidden && out.len() < count as usize {
                        out.push(r);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn tenants(&self, who: &Identity) -> ApiResult<TenantsView> {
        let seat = self.seat(who);
        let (s, _) = seat.window().await?;
        let tenants = seat.client.describe_tenants().await.map_err(failed)?;
        // Every binding for a cluster admin; one's own for everyone else.
        let bindings = match seat.client.describe_role_bindings().await {
            Ok(all) => all.into_iter().map(|(b, _)| b).collect(),
            Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. }) => {
                seat.client.who_am_i().await.map_err(failed)?.bindings
            }
            Err(e) => return Err(failed(e)),
        };
        let applies = |b: &RoleBindingData, t: &str| b.role == role::CLUSTER_ADMIN || b.tenant == t;
        let rows = tenants
            .iter()
            .map(|t| {
                let mine: Vec<&ResourceMetadata> = s.meta.resources.iter().filter(|r| r.tenant == t.name).collect();
                let subjects: BTreeSet<(u8, &str)> = bindings
                    .iter()
                    .filter(|b| applies(b, &t.name))
                    .map(|b| (b.subject_kind, b.subject.as_str()))
                    .collect();
                Tenant {
                    name: t.name.clone(),
                    used_bytes: mine.iter().flat_map(|r| s.replicas(r.resource_id)).map(|p| p.size_bytes).sum(),
                    quota_bytes: None,
                    streams: mine.iter().filter(|r| r.kind == kind::STREAM).count() as u32,
                    queues: mine.iter().filter(|r| r.kind == kind::QUEUE).count() as u32,
                    principals: subjects.len() as u32,
                    state: if t.source == lotse_proto::config_source::STATIC {
                        chip(Tone::Info, "Configured")
                    } else {
                        chip(Tone::Ok, "Active")
                    },
                }
            })
            .collect();
        let per_tenant = tenants
            .iter()
            .map(|t| TenantBindings {
                tenant: t.name.clone(),
                bindings: bindings.iter().filter(|b| applies(b, &t.name)).map(binding_row).collect(),
            })
            .collect();
        Ok(TenantsView { tenants: rows, bindings: per_tenant })
    }

    async fn pause_upgrade(&self, _who: &Identity) -> ApiResult<()> {
        Err(ApiError::BadRequest(format!("No upgrade is running on {}: it has no operator yet.", self.id)))
    }
}

/// A binding as the tenants screen lists it.
fn binding_row(b: &RoleBindingData) -> RoleBinding {
    let name = |r: u8| match r {
        role::CLUSTER_ADMIN => "cluster-admin",
        role::TENANT_ADMIN => "tenant-admin",
        role::PRODUCER => "producer",
        role::CONSUMER => "consumer",
        role::VIEWER => "viewer",
        _ => "unknown",
    };
    RoleBinding {
        principal: b.subject.clone(),
        auth: if b.subject_kind == subject_kind::GROUP { "group" } else { "user" }.into(),
        role: name(b.role).into(),
        resources: vec![if b.role == role::CLUSTER_ADMIN {
            "every tenant".into()
        } else if b.prefix.is_empty() {
            "*".into()
        } else {
            format!("{}*", b.prefix)
        }],
    }
}

/// The records of one batch as the browser shows them. `last_stable`
/// separates decided transactions from open ones; `aborting` tracks
/// producers inside an aborted transaction.
fn views(batch: &Batch, last_stable: u64, aborting: &mut BTreeMap<u64, u64>) -> Vec<RecordView> {
    let a = batch.attributes();
    let pid = batch.producer_id();
    let txn = if a.control {
        match lotse_record::control(batch) {
            Ok(Some(Control::Marker(ControlType::Commit, _))) => TxnState::CommitMarker,
            Ok(Some(Control::Marker(ControlType::Abort, _))) => {
                aborting.remove(&pid);
                TxnState::AbortMarker
            }
            // Raft's and the broker's own batches are not records.
            _ => return Vec::new(),
        }
    } else if !a.transactional {
        TxnState::None
    } else if aborting.get(&pid).is_some_and(|first| *first <= batch.base_offset()) {
        TxnState::Aborted
    } else if batch.base_offset() < last_stable {
        TxnState::Committed
    } else {
        TxnState::Open
    };
    let Ok(records) = batch.records() else { return Vec::new() };
    let text = |b: &Option<bytes::Bytes>| b.as_ref().and_then(|b| std::str::from_utf8(b).ok().map(str::to_string));
    records
        .into_iter()
        .map(|r| {
            let delta = (r.offset - batch.base_offset()) as i32;
            RecordView {
                offset: r.offset,
                timestamp: rfc3339(r.timestamp.max(0) as u64),
                key: text(&r.key),
                value: text(&r.value),
                size_bytes: r.value.as_ref().map_or(0, |v| v.len() as u32),
                headers: r
                    .headers
                    .iter()
                    .map(|h| RecordHeader { key: String::from_utf8_lossy(&h.key).into_owned(), value: text(&h.value) })
                    .collect(),
                txn,
                producer_id: pid,
                producer_epoch: batch.producer_epoch(),
                sequence: if pid == 0 { -1 } else { batch.base_sequence().wrapping_add(delta) },
                batch_base: batch.base_offset(),
                batch_last: batch.last_offset(),
                term: batch.term(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p99_is_the_bound_of_the_bucket_that_holds_it() {
        assert_eq!(p99(&[0; 17]), None);
        // 98 under 4 ms, 2 under 64 ms: the 99th is in the 64 ms bucket.
        let mut b = [0u64; 17];
        b[2] = 98;
        b[6] = 2;
        assert_eq!(p99(&b), Some(64.0));
        b[2] = 1000;
        assert_eq!(p99(&b), Some(4.0));
    }
}

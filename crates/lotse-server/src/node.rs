//! A broker node: many Raft groups, one transport.
//!
//! [`Node::start`] opens every group's [`Replica`] and runs the node until
//! [`NodeHandle::stop`]. Everything goes through `lotse-rt`, so the same code
//! runs on Tokio and in simulation.
//!
//! - The **main loop** ticks every group once per [`NodeConfig::tick`], runs
//!   the commands from [`NodeHandle`], steps incoming messages into their
//!   groups, processes every group and bundles all messages for one peer
//!   into one frame ([`crate::wire`]).
//! - Per peer, a **sender** task keeps one outgoing connection, reconnects
//!   with a backoff, and sends queued frames. While a peer is unreachable
//!   its queue keeps only the newest frames: Raft resends what matters.
//! - An **accept** task takes incoming connections; each gets a **reader**
//!   task that decodes frames into the node's inbox.
//! - With [`NodeConfig::client_listen`] set, a second accept task serves
//!   clients over the native protocol ([`crate::client`]).
//! - While this node coordinates transactions, a **marker job** per decided
//!   transaction writes its markers into the partitions through an internal
//!   client, then reports back so the main loop can complete it.
//!
//! On stop (or when a replica fails, e.g. its disk lost power) every task
//! ends and the listener is closed before [`Node::start`]'s task finishes, so
//! a restarted node can bind the same address.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_log::LogConfig;
use lotse_raft::{Config, Message, NodeId, Role};
use lotse_rt::{Conn, Disk, Link, Listener, PeerIdentity, Runtime, Spawn};

use crate::access::Access;
use crate::auth::{Auth, AuthConfig};
use crate::client::{
    AdminView, ClientRequest, Clients, ClusterView, MarkerJob, NetStats, NodeView, QueueConfig, StreamConfig,
};
use crate::metadata::MetadataState;
use crate::replica::{Error, ProposeError, Replica, ReplicaOptions, Retention, StateMachine};
use crate::signal::{Signal, before, race};
use crate::wire::{self, GroupId};

/// Frames kept per unreachable peer; older ones are dropped.
const PEER_QUEUE: usize = 256;

/// How often the node reads its host's state, for metrics.
const HOST_EVERY: Duration = Duration::from_secs(5);

/// How one node runs.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node.
    pub id: NodeId,
    /// Where it listens (`host:port`).
    pub listen: String,
    /// Every voter of the groups and its address, this node included.
    pub peers: BTreeMap<NodeId, String>,
    /// Raft groups hosted here; each lives in `groups/<id>` on the disk.
    pub groups: Vec<GroupId>,
    /// Logical Raft tick.
    pub tick: Duration,
    /// A follower that hears no leader for this long, and up to twice as
    /// long (a random time in between), starts an election; a leader that
    /// hears no quorum for this long steps down. Rounded up to whole ticks
    /// ([`crate::DEFAULT_ELECTION_TIMEOUT`] by default, ADR 0144).
    pub election_timeout: Duration,
    /// How often a leader sends heartbeats, rounded up to whole ticks and
    /// shorter than the election timeout ([`crate::DEFAULT_HEARTBEAT_INTERVAL`]
    /// by default, ADR 0144).
    pub heartbeat_interval: Duration,
    /// Pre-vote on or off.
    pub pre_vote: bool,
    /// Log segment settings.
    pub log: LogConfig,
    /// Where clients connect (`host:port`); `None` serves no clients.
    pub client_listen: Option<String>,
    /// Every node's client address, as `Metadata` reports it.
    pub client_addrs: BTreeMap<NodeId, String>,
    /// The zone of each configured node that names one, as `Metadata`
    /// reports it for clients that fetch from a replica in their zone
    /// (ADR 0104).
    pub zones: BTreeMap<NodeId, String>,
    /// Streams and the groups of their partitions.
    pub streams: Vec<StreamConfig>,
    /// Queues and their groups (each one of `groups`).
    pub queues: Vec<QueueConfig>,
    /// Exchanges and their bindings to `queues`; clients route with them.
    pub exchanges: Vec<lotse_route::Exchange>,
    /// Consumer groups: how long a member may go without a heartbeat
    /// ([`crate::DEFAULT_GROUP_SESSION_TIMEOUT`] by default).
    pub group_session_timeout: Duration,
    /// Consumer groups: how often members send heartbeats
    /// ([`crate::DEFAULT_GROUP_HEARTBEAT_INTERVAL`] by default).
    pub group_heartbeat_interval: Duration,
    /// The metadata group (one of `groups`): streams, queues and exchanges
    /// created at runtime with the admin requests. `None`: only the
    /// configured ones.
    pub metadata: Option<GroupId>,
    /// The group that coordinates producers (hands out producer ids); one
    /// of `groups`. `None`: no idempotent produce.
    pub coordinator: Option<GroupId>,
    /// Producers each partition admits for idempotence; while it holds this
    /// many, a new one is refused (ADR 0110). Every node must use the same
    /// value ([`crate::DEFAULT_MAX_PRODUCERS`] by default).
    pub max_producers: usize,
    /// How long a producer may write nothing to a partition before its
    /// leader forgets it (ADR 0110; [`crate::DEFAULT_PRODUCER_EXPIRATION`]
    /// by default).
    pub producer_expiration: Duration,
    /// How long a group's preferred voter must hold everything committed
    /// before its leader hands the group back to it, longer after recent
    /// elections (ADR 0113; [`crate::DEFAULT_LEADER_SETTLE`] by default).
    pub leader_settle: Duration,
    /// How long a follower may go without having applied what its leader
    /// said was committed and still serve `Fetch` and `ListOffsets`
    /// (ADR 0116; [`crate::DEFAULT_FOLLOWER_MAX_LAG`] by default).
    pub follower_max_lag: Duration,
    /// The free space this node keeps on its data volume: below it, the
    /// leaders of the groups it votes in take no new data (ADR 0122).
    pub disk_free_limit: crate::FreeLimit,
    /// How often this node reads its data volume's free space for that
    /// limit (ADR 0124; [`crate::DEFAULT_DISK_CHECK_INTERVAL`] by default).
    pub disk_check_interval: Duration,
    /// How often the metadata group's leader asks every node for its
    /// groups' sizes, which weigh rebalancing (ADR 0052) and count storage
    /// quotas (ADR 0147); [`crate::DEFAULT_QUOTA_CHECK_INTERVAL`] by
    /// default. A tenant may overshoot its limit by what it writes in about
    /// two of these.
    pub quota_check_interval: Duration,
    /// This node's release and the cluster versions it runs with (ADR
    /// 0127); the build's own by default. Simulations give nodes others.
    pub versions: crate::version::Versions,
    /// How clients authenticate.
    pub auth: AuthConfig,
    /// What streams keep unless they say otherwise (ADR 0046).
    pub retention: RetentionConfig,
    /// This node joins a running cluster (ADR 0048): it starts outside the
    /// configured groups and is made a member by their leaders, instead of
    /// being one of their first voters. Only matters on the first start;
    /// after that a group's membership comes from its own log.
    pub join: bool,
}

impl NodeConfig {
    /// The Raft settings of one of this node's groups: its election timeout
    /// and heartbeat in ticks (ADR 0144).
    fn raft(&self, seed: u64, voters: Vec<NodeId>) -> Config {
        Config {
            seed,
            pre_vote: self.pre_vote,
            election_ticks: ticks(self.election_timeout, self.tick),
            heartbeat_ticks: ticks(self.heartbeat_interval, self.tick),
            ..Config::new(self.id, voters)
        }
    }

    /// The election timeout in whole ticks, as the groups count it.
    fn election(&self) -> Duration {
        self.tick * ticks(self.election_timeout, self.tick)
    }
}

/// `d` in ticks, rounded up, at least one.
fn ticks(d: Duration, tick: Duration) -> u32 {
    let n = d.as_nanos().div_ceil(tick.as_nanos().max(1)).max(1);
    u32::try_from(n).unwrap_or(u32::MAX)
}

/// What streams keep by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Records are kept this many ms (by their timestamps); `None`: for
    /// ever.
    pub ms: Option<u64>,
    /// Each partition keeps this many bytes of log; `None`: no limit.
    pub bytes: Option<u64>,
    /// Bytes the map of keys of a compaction sweep may take; the node runs
    /// one sweep at a time (ADR 0098).
    pub compaction_map_bytes: u64,
    /// A runtime stream's or queue's active segment closes once it is this
    /// many ms old, unless the resource sets `segment.ms` (ADR 0146).
    pub segment_ms: u64,
}

/// A segment's age unless the node or the resource says otherwise: an hour,
/// so a drained queue frees its space within about that (ADR 0146).
pub const DEFAULT_SEGMENT_MS: u64 = 3_600_000;

/// How often the metadata leader counts group sizes unless the node says
/// otherwise (ADR 0147): ten seconds, as the size poll always ran.
pub const DEFAULT_QUOTA_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// What a compaction sweep's map may take unless the node says otherwise:
/// 64 MiB, about 1.4 million keys.
pub const DEFAULT_COMPACTION_MAP_BYTES: u64 = 64 << 20;

impl Default for RetentionConfig {
    /// Seven days, no size limit.
    fn default() -> Self {
        RetentionConfig {
            ms: Some(7 * 24 * 3600 * 1000),
            bytes: None,
            compaction_map_bytes: DEFAULT_COMPACTION_MAP_BYTES,
            segment_ms: DEFAULT_SEGMENT_MS,
        }
    }
}

impl RetentionConfig {
    /// Keep everything.
    pub fn keep() -> Self {
        RetentionConfig {
            ms: None,
            bytes: None,
            compaction_map_bytes: DEFAULT_COMPACTION_MAP_BYTES,
            segment_ms: DEFAULT_SEGMENT_MS,
        }
    }
}

/// The file holding the configured groups' first voters.
const FIRST_VOTERS: &str = "first-voters";

/// The first voters of the configured groups (the metadata and coordinator
/// groups), kept on the disk from the first start. Every replica of a group
/// must start from the same voters; once nodes come and go the
/// configuration's node list changes, and a node that restarted with the
/// newer list would otherwise count voters the others do not. A joining
/// node starts outside them: the groups' leaders add it.
async fn first_voters<R: Runtime>(rt: &R, cfg: &NodeConfig) -> Result<Vec<NodeId>, Error> {
    use bytes::{Buf, BufMut};
    if let Some(bytes) = crate::replica::read_file(rt, Path::new(FIRST_VOTERS)).await? {
        let mut b = crate::replica::unseal(bytes, FIRST_VOTERS)?;
        let corrupt = || Error::Corrupt(format!("{FIRST_VOTERS} is truncated"));
        if b.remaining() < 4 {
            return Err(corrupt());
        }
        let n = b.get_u32_le() as usize;
        if b.remaining() != n * 8 {
            return Err(corrupt());
        }
        return Ok((0..n).map(|_| b.get_u64_le()).collect());
    }
    let voters: Vec<NodeId> = cfg.peers.keys().copied().filter(|id| !cfg.join || *id != cfg.id).collect();
    let mut b = bytes::BytesMut::new();
    b.put_u8(crate::replica::FORMAT);
    b.put_u32_le(voters.len() as u32);
    for v in &voters {
        b.put_u64_le(*v);
    }
    crate::replica::write_atomic(rt, Path::new(""), FIRST_VOTERS, crate::replica::seal(b)).await?;
    Ok(voters)
}

/// How often (in ticks) group leaders move their groups' membership on, and
/// the metadata leader plans.
const MEMBERSHIP_EVERY_TICKS: u32 = 20;

/// The groups whose membership follows the metadata group (ADR 0048): the
/// metadata and coordinator groups, and every runtime group.
fn core_groups(cfg: &NodeConfig) -> Vec<GroupId> {
    cfg.metadata.into_iter().chain(cfg.coordinator).collect()
}

/// What the membership passes remember between rounds.
#[derive(Default)]
struct Moves {
    /// Voters reported in place, per group, and when.
    reported: BTreeMap<GroupId, (Vec<NodeId>, lotse_rt::Instant)>,
    /// When this node last handed a group's leadership over.
    last_handover: Option<lotse_rt::Instant>,
    /// When it last tried for each group, so that a preferred leader that
    /// does not take over is not asked again at once.
    tried: BTreeMap<GroupId, lotse_rt::Instant>,
    /// Each group's elections and how long its preferred voter has held
    /// everything (ADR 0113).
    steadiness: BTreeMap<GroupId, crate::membership::Steadiness>,
}

/// A node hands over at most one group's leadership in this time.
const HANDOVER_EVERY_TICKS: u32 = 100;
/// And asks the same group's preferred leader again only after this.
const HANDOVER_RETRY_TICKS: u32 = 1_000;

/// Whether a follower may serve reads (ADR 0116): it hears its leader, and
/// within `max_lag` it had applied everything the leader said was committed.
fn follower_current(
    hears_leader: bool,
    caught_up_at: Option<lotse_rt::Instant>,
    now: lotse_rt::Instant,
    max_lag: Duration,
) -> bool {
    hears_leader && caught_up_at.is_some_and(|t| now - t <= max_lag)
}

/// A leader's commit index over the last `follower_max_lag`, sampled, so
/// it can tell how long ago a voter's match index was the commit index.
/// Comparing with the commit index of the moment would not do: with three
/// voters the faster follower sets it, and under a steady stream of writes
/// the slower one trails it at every round while only a moment behind.
#[derive(Debug, Default)]
pub(crate) struct CommitSamples {
    /// (when, commit index then), oldest first; the oldest may be older than
    /// the lag, as the last one that was in force then.
    samples: VecDeque<(lotse_rt::Instant, u64)>,
}

impl CommitSamples {
    /// Takes a sample at most every hundredth of `max_lag` (100 ms by
    /// default), so a group keeps about a hundred, and drops those no voter
    /// lagging within `max_lag` can need.
    pub(crate) fn record(&mut self, now: lotse_rt::Instant, commit: u64, max_lag: Duration) {
        if self.samples.back().is_none_or(|&(t, _)| now - t >= max_lag / 100) {
            self.samples.push_back((now, commit));
        }
        while self.samples.get(1).is_some_and(|&(t, _)| now - t > max_lag) {
            self.samples.pop_front();
        }
    }

    /// The latest time at which a voter that holds up to `matched` held
    /// everything committed: now if it holds the commit index of the moment,
    /// else the latest sample whose commit index it holds.
    fn caught_up_at(&self, matched: u64, commit: u64, now: lotse_rt::Instant) -> Option<lotse_rt::Instant> {
        if matched >= commit {
            return Some(now);
        }
        self.samples.iter().rev().find(|&&(_, c)| c <= matched).map(|&(t, _)| t)
    }
}

/// The ADR 0116 rule seen from a leader (the zone reads design, section 1):
/// records the commit index in `samples` and returns the voters it heard
/// within an election timeout that held, within `max_lag`, everything that
/// was committed then. A leader names only those to a reader in their zone.
pub(crate) fn current_voters(
    samples: &mut CommitSamples,
    commit: u64,
    progress: impl IntoIterator<Item = (NodeId, lotse_raft::VoterProgress)>,
    now: lotse_rt::Instant,
    max_lag: Duration,
) -> BTreeSet<NodeId> {
    samples.record(now, commit, max_lag);
    progress
        .into_iter()
        .filter(|(_, p)| follower_current(p.heard_recently, samples.caught_up_at(p.matched, commit, now), now, max_lag))
        .map(|(id, _)| id)
        .collect()
}

/// How often a leaving node asks a group's next voter to take over: a
/// transfer whose target did not campaign is given up after an election
/// timeout.
const LEAVE_RETRY_TICKS: u32 = 50;

/// One step of leaving (ADR 0114): asks a voter of every group this node
/// leads to take over, each attempt the next in [`crate::membership::successors`]
/// order, so a voter that is down is passed over. A leader that loses its
/// quorum, the others down, steps down within an election timeout
/// (check-quorum), so such a group does not hold the stop back. Returns
/// whether nothing is left to hand over.
fn leave_pass<R: Runtime, S: StateMachine>(
    cfg: &NodeConfig,
    groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    now: lotse_rt::Instant,
    asked: &mut BTreeMap<GroupId, (lotse_rt::Instant, usize)>,
) -> bool {
    let mut done = true;
    for (&g, replica) in groups.iter_mut() {
        let raft = replica.raft();
        if raft.role() != Role::Leader {
            continue;
        }
        let matched = raft.matched().unwrap_or_default();
        let candidates = crate::membership::successors(raft.membership(), cfg.id, &matched);
        if candidates.is_empty() {
            continue;
        }
        done = false;
        let attempt = match asked.get(&g) {
            Some((t, _)) if now - *t < cfg.tick * LEAVE_RETRY_TICKS => continue,
            Some((_, n)) => n + 1,
            None => 0,
        };
        asked.insert(g, (now, attempt));
        let _ = raft.transfer_leadership(candidates[attempt % candidates.len()]);
    }
    done
}

/// One pass of moving groups between nodes: each group this node leads
/// takes a step towards the voters the metadata group wants for it, or
/// reports them in place; the metadata group's leader plans.
#[allow(clippy::too_many_arguments)]
fn membership_pass<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    known: &Known,
    configured_voters: &[NodeId],
    moves: &mut Moves,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
) {
    let state = &known.state;
    let core = core_groups(cfg);
    let everyone: BTreeSet<NodeId> = known.nodes.keys().copied().collect();
    let preferred = crate::membership::preferred_leaders(
        &crate::client::managed_groups(state, &core, configured_voters),
        &state.pins,
    );
    let now = rt.now();
    moves.steadiness.retain(|g, _| groups.contains_key(g));
    for (&g, replica) in groups.iter_mut() {
        let raft = replica.raft();
        let steadiness = moves.steadiness.entry(g).or_default();
        steadiness.see_term(raft.term(), raft.leader(), now, cfg.leader_settle);
        if raft.role() != Role::Leader {
            steadiness.see_following();
            continue;
        }
        let current = raft.membership().clone();
        let target: BTreeSet<NodeId> = match known.voters.get(&g) {
            Some(v) => v.iter().copied().collect(),
            None if core.contains(&g) => match state.targets.get(&g) {
                Some(v) => v.iter().copied().collect(),
                // Never changed: the voters it has; only learners follow.
                None => current.voters.clone(),
            },
            None => continue,
        };
        let learners = if Some(g) == cfg.metadata { everyone.clone() } else { BTreeSet::new() };
        let tag = state.target_at.get(&g).copied().unwrap_or(0);
        let step = crate::membership::leader_step(&current, &target, &learners, tag, raft.membership_committed());
        match step {
            crate::membership::Step::Propose { voters, learners } => {
                // Refused while a learner catches up or a change commits:
                // the next pass tries again.
                let _ = raft.propose_membership_tagged(voters, learners, tag);
            }
            crate::membership::Step::Wait => {}
            crate::membership::Step::Settled => {
                let voters: Vec<NodeId> = target.iter().copied().collect();
                if !crate::client::moving(state, g, &voters) {
                    // In place: spread the leaders (ADR 0050), once the
                    // preferred voter has held everything committed for a
                    // while, longer after recent elections (ADR 0113). A pin
                    // is an operator's choice and moves without waiting.
                    let last = raft.last_index();
                    let matched = raft.matched().unwrap_or_default();
                    let p = preferred.get(&g).copied();
                    let holds = p.is_some_and(|p| matched.get(&p).copied().unwrap_or(0) >= raft.commit());
                    steadiness.see_leading(p, holds, now);
                    let pinned = state.pins.get(&g).is_some_and(|n| Some(*n) == p);
                    let due = moves.last_handover.is_none_or(|t| now - t >= cfg.tick * HANDOVER_EVERY_TICKS)
                        && moves.tried.get(&g).is_none_or(|t| now - *t >= cfg.tick * HANDOVER_RETRY_TICKS)
                        && (pinned || steadiness.steady(now, cfg.leader_settle));
                    let to = crate::membership::hand_over_to(&current, cfg.id, p, &matched, last);
                    if let (true, Some(to)) = (due, to) {
                        moves.last_handover = Some(now);
                        moves.tried.insert(g, now);
                        let _ = raft.transfer_leadership(to);
                    }
                    continue;
                }
                if moves.reported.get(&g).is_some_and(|(v, at)| *v == voters && now - *at < cfg.tick * 100) {
                    continue;
                }
                moves.reported.insert(g, (voters.clone(), now));
                let client = client.clone();
                let stop = shared.stop.clone();
                jobs.spawn(rt, async move {
                    let _ = race(client.settle_voters(g, voters), &[&stop]).await;
                });
            }
        }
    }
    // The planner runs on the metadata group's leader, one batch of
    // changes at a time: only once everything it proposed has applied.
    let Some(meta) = cfg.metadata.and_then(|g| groups.get_mut(&g)) else { return };
    let raft = meta.raft_ref();
    let caught_up = raft.role() == Role::Leader
        && raft.log().term_at(meta.applied()) == Some(raft.term())
        && meta.applied() == raft.last_index();
    if !caught_up || meta.applied() != known.applied {
        return;
    }
    let sizes = shared.sizes.lock().expect("sizes lock").clone();
    for change in crate::membership::plan(state, &core, configured_voters, &known.active, &sizes) {
        let _ = meta.propose_internal(crate::metadata::record(&change));
    }
    // Tombstones go once every node the cluster lists has removed their
    // groups' files (ADR 0100); one that has not answered holds them.
    let removed = shared.removed_through.lock().expect("removed lock");
    let resource_ids = state.forgettable(known.nodes.keys().map(|id| removed.get(id).copied().flatten()));
    if !resource_ids.is_empty() {
        let change = crate::metadata::Change::ForgetDeleted { resource_ids };
        let _ = meta.propose_internal(crate::metadata::record(&change));
    }
    drop(removed);
    // Storage quotas (ADR 0147): what the last poll's sizes make full or
    // clear, once every runtime group has a size. Sizes of groups no
    // resource holds any more go.
    let sized_at = *shared.sized_at.lock().expect("sized lock");
    let Some(at) = sized_at else { return };
    let usage = {
        let mut sizes = shared.sizes.lock().expect("sizes lock");
        let live: BTreeSet<GroupId> = state
            .streams
            .values()
            .flat_map(|s| s.partitions.iter().map(|(g, _)| *g))
            .chain(state.queues.values().map(|q| q.group))
            .collect();
        sizes.retain(|g, _| live.contains(g) || cfg.metadata == Some(*g) || cfg.coordinator == Some(*g));
        crate::quota::count(state, &sizes)
    };
    let Some(usage) = usage else { return };
    for change in crate::quota::changes(state, &usage) {
        let _ = meta.propose_internal(crate::metadata::record(&change));
    }
    *shared.quota_usage.lock().expect("usage lock") = Some((at, usage));
}

/// Tells the metadata group that this node went low on disk or back, as
/// soon as a read of its volume differs from what its view of the metadata
/// state records (ADR 0124). One report at a time; while the view still
/// differs, the next read reports again, so a lost report, a leader change
/// or a node that restarted all end the same way. A node that is down stays
/// as it was recorded.
#[allow(clippy::too_many_arguments)]
fn report_disk<R: Runtime>(
    rt: &R,
    id: NodeId,
    disk: &crate::disk::DiskWatch,
    known: &Known,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    reporting: &Arc<std::sync::atomic::AtomicBool>,
) {
    // Before a read has told, the node does not know: say nothing.
    let Some(now) = disk.metrics() else { return };
    let Ok(node_id) = u32::try_from(id) else { return };
    if known.state.disk_low.contains(&id) == now.low || reporting.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let (client, stop, reporting) = (client.clone(), shared.stop.clone(), reporting.clone());
    jobs.spawn(rt, async move {
        let _ = race(client.report_disk(node_id, now.low), &[&stop]).await;
        reporting.store(false, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Tells the metadata leader this node's release and the cluster versions
/// it runs with (ADR 0127), while the metadata state says otherwise.
#[allow(clippy::too_many_arguments)]
fn report_version<R: Runtime>(
    rt: &R,
    cfg: &NodeConfig,
    known: &Known,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    reporting: &Arc<std::sync::atomic::AtomicBool>,
    last: &mut Option<lotse_rt::Instant>,
) {
    // Without a metadata group there is no cluster version, and nobody to
    // tell: a report would only fail again in the next round.
    if cfg.metadata.is_none() {
        return;
    }
    let v = &cfg.versions;
    let Ok(node_id) = u32::try_from(cfg.id) else { return };
    let same = known
        .state
        .versions
        .get(&cfg.id)
        .is_some_and(|r| r.release == v.release && r.oldest == v.oldest && r.newest == v.newest);
    // Reported, and the cluster has its version: nothing to say. Until the
    // cluster has one, each report may be the last one missing, so it goes
    // again, but at most every REPORT_AGAIN: while a node stays away or the
    // metadata leader cannot be reached, every round would otherwise add one.
    if same && known.state.cluster_version.is_some() {
        return;
    }
    let now = rt.now();
    if same && last.is_some_and(|t| now.saturating_duration_since(t) < REPORT_AGAIN) {
        return;
    }
    if reporting.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    *last = Some(now);
    let (client, stop, reporting, v) = (client.clone(), shared.stop.clone(), reporting.clone(), v.clone());
    jobs.spawn(rt, async move {
        let _ = race(client.report_version(node_id, &v.release, v.oldest, v.newest), &[&stop]).await;
        reporting.store(false, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Gives back the restart turn this node holds under its previous process
/// (ADR 0127).
fn release_turn<R: Runtime>(
    rt: &R,
    id: NodeId,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    releasing: &Arc<std::sync::atomic::AtomicBool>,
) {
    let Ok(node_id) = u32::try_from(id) else { return };
    if releasing.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let (client, stop, releasing) = (client.clone(), shared.stop.clone(), releasing.clone());
    jobs.spawn(rt, async move {
        let _ = race(client.release_turn(node_id), &[&stop]).await;
        releasing.store(false, std::sync::atomic::Ordering::Relaxed);
    });
}

/// `lotse turn take` asked this node for the restart turn (ADR 0127): it
/// claims it only if no group it votes in loses its quorum without it, as
/// `/safe-to-stop` judges, from every node's answer.
async fn take_turn<R: Runtime>(
    me: NodeId,
    instance: u64,
    mine: BTreeMap<GroupId, lotse_raft::Membership>,
    client: Arc<lotse_client::Client<R>>,
    job: crate::client::TurnJob,
    stop: Signal,
) {
    use lotse_proto::ErrorCode;
    let refuse = |why: String| (ErrorCode::TURN_UNAVAILABLE, Some(why));
    let (code, why) = match race(client.node_metrics(), &[&stop]).await {
        None => return,
        Some(Err(e)) => refuse(format!("node {me} could not ask the nodes whether it may stop: {e}")),
        Some(Ok(nodes)) => {
            let reports: Vec<_> = nodes.into_iter().filter_map(|(_, m)| m.ok()).collect();
            let critical = crate::ops::quorum_critical(me, &mine, &reports);
            if !critical.is_empty() {
                let groups: Vec<String> = critical.iter().map(|c| c.group.to_string()).collect();
                refuse(format!("groups {} would be left without a quorum if node {me} stopped now", groups.join(", ")))
            } else {
                let Ok(node_id) = u32::try_from(me) else { return };
                match race(client.claim_turn(node_id, instance), &[&stop]).await {
                    None => return,
                    Some(Ok(())) => (ErrorCode::NONE, None),
                    Some(Err(lotse_client::ClientError::Broker { code, message })) => (code, message),
                    Some(Err(e)) => refuse(format!("node {me} could not claim the turn: {e}")),
                }
            }
        }
    };
    job.answer(code, why.as_deref());
}

/// Tells the metadata group which client certificates this node accepted
/// since the last report, leaving out what it already holds (ADR 0091).
fn report_certificates<R: Runtime>(
    rt: &R,
    known: &Known,
    auth: &Auth<R>,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
) {
    let fresh: Vec<lotse_proto::SeenCertificate> = auth
        .take_seen()
        .into_iter()
        .filter(|(k, c)| {
            !known.state.certs.get(k).is_some_and(|held| {
                held.last_seen_ms.saturating_add(crate::client::certs::LAST_SEEN_EVERY_MS) > c.last_seen_ms
            })
        })
        .take(crate::client::certs::MAX_REPORTED)
        .map(|(k, c)| lotse_proto::SeenCertificate {
            issuer: Bytes::copy_from_slice(&k.issuer),
            serial: Bytes::from(k.serial),
            principal: c.principal,
            not_before_ms: c.not_before_ms,
            not_after_ms: c.not_after_ms,
            seen_ms: c.last_seen_ms,
        })
        .collect();
    if fresh.is_empty() {
        return;
    }
    let (client, stop) = (client.clone(), shared.stop.clone());
    jobs.spawn(rt, async move {
        let _ = race(client.record_certificates(fresh), &[&stop]).await;
    });
}

/// How often each replica looks whether retention lets something go.
const RETAIN_EVERY: Duration = Duration::from_secs(1);

/// How soon a node reports versions the metadata state already has again,
/// while the cluster has no version yet (ADR 0127).
const REPORT_AGAIN: Duration = Duration::from_secs(1);

/// What group `g` may delete: the metadata and coordinator groups hold
/// everything in their state; a queue what is settled; a stream by age and
/// size.
fn retention_of(cfg: &NodeConfig, g: GroupId, queue: bool) -> Retention {
    if cfg.metadata == Some(g) || cfg.coordinator == Some(g) {
        Retention::State
    } else if queue {
        Retention::Queue
    } else {
        Retention::Stream { ms: cfg.retention.ms, bytes: cfg.retention.bytes, compact: None }
    }
}

/// What a group looks like from outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupStatus {
    /// Role here.
    pub role: Role,
    /// Current term.
    pub term: u64,
    /// The leader this node knows of.
    pub leader: Option<NodeId>,
    /// Commit index here.
    pub commit: u64,
    /// Applied index here.
    pub applied: u64,
    /// A compaction sweep is in progress here (ADR 0098).
    pub sweeping: bool,
}

/// A decoded frame: its sender and the messages per group.
type Incoming = (NodeId, Vec<(GroupId, Message)>);

type Reply = Arc<Mutex<(Option<Result<(u64, u64), String>>, Option<std::task::Waker>)>>;

/// Where the main loop leaves an answer for a handle, and the handle's waker.
type Slot<T> = Arc<Mutex<(Option<T>, Option<std::task::Waker>)>>;

/// Leaves `value` in `slot` and wakes whoever waits for it.
fn fill<T>(slot: &Slot<T>, value: T) {
    let mut s = slot.lock().expect("slot lock");
    s.0 = Some(value);
    if let Some(w) = s.1.take() {
        w.wake();
    }
}

/// Waits for `slot` to be filled, or for `stop`.
async fn taken<T>(slot: Slot<T>, stop: &Signal) -> Option<T> {
    let answer = std::future::poll_fn(|cx| {
        let mut s = slot.lock().expect("slot lock");
        match s.0.take() {
            Some(v) => std::task::Poll::Ready(v),
            None => {
                s.1 = Some(cx.waker().clone());
                std::task::Poll::Pending
            }
        }
    });
    race(answer, &[stop]).await
}

enum Command {
    Propose {
        group: GroupId,
        batch: Bytes,
        reply: Reply,
    },
    Client(ClientRequest),
    /// Every marker of a transaction's decision is written.
    MarkersWritten {
        txn_id: String,
        term: u64,
    },
    /// The transaction coordinator answered a partition leader's question,
    /// or could not be asked (ADR 0057).
    Verified {
        key: crate::client::VerifyKey,
        error: lotse_proto::ErrorCode,
    },
    /// The partitions of a conditional delete were asked (ADR 0063).
    EmptyChecked {
        key: u64,
        answer: crate::client::EmptyAnswer,
    },
    /// A publish's writes are decided (ADR 0079).
    Published {
        key: u64,
        results: Vec<crate::client::PublishWrite>,
    },
    /// A queue leader's dead letters were forwarded, or could not be.
    Forwarded {
        group: GroupId,
        term: u64,
        offsets: Vec<u64>,
        ok: bool,
        /// The dead-letter queue refused them as full.
        full: bool,
    },
    /// The operator's endpoint asks what the node sees of itself (ADR 0117).
    Observe {
        scraper: crate::ops::Scraper,
        reply: Slot<crate::ops::Observation>,
    },
    /// The operator's endpoint asks whether the node may stop (ADR 0117).
    SafeToStop {
        reply: Slot<Result<Vec<crate::ops::Critical>, String>>,
    },
}

#[derive(Default)]
struct Shared {
    inbox: Mutex<VecDeque<Incoming>>,
    commands: Mutex<VecDeque<Command>>,
    status: Mutex<BTreeMap<GroupId, GroupStatus>>,
    /// Resources deleted at runtime, for marker jobs.
    deleted: Mutex<crate::metadata::Deleted>,
    /// Traffic and connections, for metrics.
    net: Arc<NetStats>,
    /// The other nodes of the cluster: only they may open a node link.
    members: Mutex<BTreeSet<NodeId>>,
    /// Nodes this node's metadata state does not list yet but a listed node
    /// does (it lags, after a restart, say): they may open a node link, but
    /// only their metadata group frames are taken, so that this node can
    /// catch up and learn of them (ADR 0050).
    metadata_only: Mutex<BTreeSet<NodeId>>,
    /// Peer addresses of such nodes, as the metadata leader listed them.
    learned: Mutex<BTreeMap<NodeId, String>>,
    /// Set when the node stopped because it was drained (ADR 0051).
    removed: std::sync::atomic::AtomicBool,
    /// Why the node stopped because the cluster runs at a cluster version
    /// it does not run with (ADR 0127).
    refused: Mutex<Option<String>>,
    /// Set when an answer of the metadata leader at least as new as this
    /// node's own metadata state left out this node, which that state lists:
    /// the cluster forgot it (ADR 0105).
    forgotten: std::sync::atomic::AtomicBool,
    /// Bytes of each group's log, the largest any replica reported, as the
    /// metadata leader last asked the nodes: what rebalancing weighs moves
    /// by (ADR 0052).
    sizes: Mutex<BTreeMap<GroupId, u64>>,
    /// On the metadata leader: when the size poll last finished, ms since
    /// the Unix epoch, and what its count found (ADR 0147). Empty on any
    /// other node, and until a poll has given every runtime group a size.
    quota_usage: Mutex<Option<(u64, crate::quota::Usage)>>,
    /// When the last size poll finished, ms since the Unix epoch.
    sized_at: Mutex<Option<u64>>,
    /// On the metadata leader: the metadata index through which each node
    /// that answered has removed the files of deleted resources (ADR 0100).
    removed_through: Mutex<BTreeMap<NodeId, Option<u64>>>,
    /// Set by [`NodeHandle::leave`]: how long the node may take to hand
    /// the groups it leads to other voters before it stops (ADR 0114).
    leaving: Mutex<Option<Duration>>,
    work: Signal,
    stop: Signal,
}

/// Talks to a running node from other tasks.
#[derive(Clone)]
pub struct NodeHandle {
    shared: Arc<Shared>,
}

impl fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeHandle").field("stopped", &self.shared.stop.is_set()).finish()
    }
}

impl NodeHandle {
    /// Proposes a data batch to `group`; returns its indices once the leader
    /// has taken it (not once it is committed), or why not.
    pub async fn propose(&self, group: GroupId, batch: Bytes) -> Result<(u64, u64), String> {
        let reply: Reply = Arc::new(Mutex::new((None, None)));
        self.shared.commands.lock().expect("commands lock").push_back(Command::Propose {
            group,
            batch,
            reply: reply.clone(),
        });
        self.shared.work.set();
        let stop = self.shared.stop.clone();
        let answer = std::future::poll_fn(|cx| {
            let mut r = reply.lock().expect("reply lock");
            match r.0.take() {
                Some(v) => std::task::Poll::Ready(v),
                None => {
                    r.1 = Some(cx.waker().clone());
                    std::task::Poll::Pending
                }
            }
        });
        race(answer, &[&stop]).await.unwrap_or_else(|| Err("node stopped".into()))
    }

    /// What the node sees of itself, for the operator's endpoint (ADR
    /// 0117): its metrics, unfiltered, and where each of its groups stands,
    /// with whether `scraper` may read the metrics. `None` once the node
    /// stopped. An answer at all says the main loop runs (`/healthz`).
    pub async fn observe(&self, scraper: crate::ops::Scraper) -> Option<crate::ops::Observation> {
        let reply: Slot<crate::ops::Observation> = Arc::new(Mutex::new((None, None)));
        self.shared
            .commands
            .lock()
            .expect("commands lock")
            .push_back(Command::Observe { scraper, reply: reply.clone() });
        self.shared.work.set();
        taken(reply, &self.shared.stop.clone()).await
    }

    /// The groups that would be left without a quorum if this node stopped
    /// now, from what every node answers about the groups it leads (ADR
    /// 0117); empty when it may stop. An error when the node stopped.
    pub async fn safe_to_stop(&self) -> Result<Vec<crate::ops::Critical>, String> {
        let reply: Slot<Result<Vec<crate::ops::Critical>, String>> = Arc::new(Mutex::new((None, None)));
        self.shared.commands.lock().expect("commands lock").push_back(Command::SafeToStop { reply: reply.clone() });
        self.shared.work.set();
        taken(reply, &self.shared.stop.clone()).await.unwrap_or_else(|| Err("node stopped".into()))
    }

    /// The last known status of `group`.
    pub fn status(&self, group: GroupId) -> Option<GroupStatus> {
        self.shared.status.lock().expect("status lock").get(&group).copied()
    }

    /// The last known status of every group here.
    pub fn statuses(&self) -> BTreeMap<GroupId, GroupStatus> {
        self.shared.status.lock().expect("status lock").clone()
    }

    /// How many tombstones of deleted resources this node's metadata view
    /// keeps (ADR 0100).
    pub fn tombstones(&self) -> usize {
        self.shared.deleted.lock().expect("deleted lock").tombstones.len()
    }

    /// Asks the node to stop; its task finishes once everything is closed.
    pub fn stop(&self) {
        self.shared.stop.set();
    }

    /// Asks the node to stop after handing every group it leads to another
    /// voter, as Kafka's controlled shutdown and RabbitMQ's maintenance mode
    /// do (ADR 0114): clients then meet a planned move, not a failover. It
    /// stops once it leads no group another voter could take, or after
    /// `within`, whichever comes first.
    pub fn leave(&self, within: Duration) {
        *self.shared.leaving.lock().expect("leaving lock") = Some(within);
        self.shared.work.set();
    }

    /// Whether the node stopped by itself because it was drained: no group
    /// counts it any more, and the cluster forgets it (ADR 0051).
    pub fn removed(&self) -> bool {
        self.shared.removed.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Why the node stopped by itself because the cluster runs at a
    /// cluster version this release does not run with (ADR 0127), if it
    /// did.
    pub fn refused(&self) -> Option<String> {
        self.shared.refused.lock().expect("refused lock").clone()
    }
}

struct Peer {
    queue: Mutex<VecDeque<Bytes>>,
    ready: Signal,
    /// Set when the node leaves the cluster: the link closes for good.
    closed: Signal,
}

/// The links to the other nodes, following the cluster's node list.
struct Links {
    peers: BTreeMap<NodeId, (String, Arc<Peer>)>,
}

impl Links {
    /// Opens links to nodes in `want` that have none (or a new address)
    /// and closes those to nodes no longer in it.
    fn follow<R: Runtime>(
        &mut self,
        rt: &R,
        me: NodeId,
        want: &BTreeMap<NodeId, String>,
        shared: &Arc<Shared>,
        jobs: &Live,
        tick: Duration,
    ) {
        let stale: Vec<NodeId> =
            self.peers.iter().filter(|(id, (addr, _))| want.get(id) != Some(addr)).map(|(id, _)| *id).collect();
        for id in stale {
            if let Some((_, p)) = self.peers.remove(&id) {
                p.closed.set();
                p.ready.set();
            }
        }
        for (&id, addr) in want.iter().filter(|(id, _)| **id != me) {
            if self.peers.contains_key(&id) {
                continue;
            }
            let peer = Arc::new(Peer {
                queue: Mutex::new(VecDeque::new()),
                ready: Signal::default(),
                closed: Signal::default(),
            });
            let task = send_to(rt.clone(), id, addr.clone(), peer.clone(), shared.clone(), tick);
            jobs.spawn(rt, task);
            self.peers.insert(id, (addr.clone(), peer));
        }
        *shared.members.lock().expect("members lock") = want.keys().copied().filter(|id| *id != me).collect();
    }
}

/// A running node.
pub struct Node<R: Runtime> {
    handle: NodeHandle,
    task: <R as Spawn>::JoinHandle<Result<(), Error>>,
}

impl<R: Runtime> fmt::Debug for Node<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node").field("handle", &self.handle).finish()
    }
}

impl<R: Runtime> Node<R> {
    /// Opens the groups (recovering from the disk) and starts the node.
    /// `state_machine` makes a fresh state machine for a group.
    pub async fn start<S: StateMachine>(
        rt: R,
        cfg: NodeConfig,
        state_machine: impl Fn(GroupId) -> S + std::marker::Send + Sync + 'static,
    ) -> Result<Node<R>, Error> {
        let voters = first_voters(&rt, &cfg).await?;
        let mut groups = BTreeMap::new();
        for &g in &cfg.groups {
            let raft = cfg.raft(rt.next_u64(), voters.clone());
            let queue = cfg.queues.iter().any(|q| q.group == g);
            let options =
                ReplicaOptions { max_producers: cfg.max_producers, queue, retention: retention_of(&cfg, g, queue) };
            let mut replica =
                Replica::open_with(rt.clone(), group_dir(g), raft, state_machine(g), cfg.log, options).await?;
            // A group of one voter has nobody to wait for: it elects itself
            // at its first tick, at a first start and after a restart alike,
            // not after an election timeout, which is a second by default
            // (ADR 0144). Electing early is always safe.
            let m = replica.raft_ref().membership();
            if m.outgoing.is_empty() && m.voters.len() == 1 && m.voters.contains(&cfg.id) {
                replica.raft().campaign_soon();
            }
            groups.insert(g, replica);
        }
        // Nodes reach each other's client ports too (transaction markers)
        // and can only prove who they are with their certificates.
        if cfg.auth.required && !rt.verifies_peers() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "authentication needs TLS: nodes authenticate to each other with their certificates",
            )));
        }
        let listener = rt.listen(&cfg.listen, Link::Node).await?;
        let client_listener = match &cfg.client_listen {
            Some(addr) => Some(rt.listen(addr, Link::Client).await?),
            None => None,
        };
        let shared = Arc::new(Shared::default());
        let handle = NodeHandle { shared: shared.clone() };
        let factory: Factory<S> = Arc::new(state_machine);
        let task = rt.clone().spawn(run(rt, cfg, voters, groups, factory, listener, client_listener, shared));
        Ok(Node { handle, task })
    }

    /// A handle to the node.
    pub fn handle(&self) -> NodeHandle {
        self.handle.clone()
    }

    /// Waits until the node has stopped: `Ok` after [`NodeHandle::stop`], the
    /// error if a replica failed.
    pub async fn join(self) -> Result<(), Error> {
        self.task.await
    }
}

/// Makes a fresh state machine for a group.
type Factory<S> = Arc<dyn Fn(GroupId) -> S + std::marker::Send + Sync>;

/// A group's role, term and leader, for the log (ADR 0139).
fn seen<R: Runtime, S: StateMachine>(replica: &Replica<R, S>) -> crate::elections::Seen {
    let raft = replica.raft_ref();
    crate::elections::Seen { role: raft.role(), term: raft.term(), leader: raft.leader() }
}

#[allow(clippy::too_many_arguments)]
async fn run<R: Runtime, S: StateMachine>(
    rt: R,
    cfg: NodeConfig,
    first: Vec<NodeId>,
    mut groups: BTreeMap<GroupId, Replica<R, S>>,
    factory: Factory<S>,
    listener: R::Listener,
    client_listener: Option<R::Listener>,
    shared: Arc<Shared>,
) -> Result<(), Error> {
    let auth = Auth::new(rt.clone(), cfg.auth.clone());
    let mut tasks = Vec::new();
    let jobs = Live::default();
    let mut links = Links { peers: BTreeMap::new() };
    links.follow(&rt, cfg.id, &cfg.peers, &shared, &jobs, cfg.tick);
    tasks.push(rt.spawn(accept(rt.clone(), listener, shared.clone(), cfg.metadata)));
    if let Some(l) = client_listener {
        tasks.push(rt.spawn(accept_clients(rt.clone(), l, shared.clone(), auth.clone())));
    }
    let around = Around { links: &mut links, shared: &shared, jobs: &jobs, auth: &auth };
    let result = main_loop(&rt, &cfg, &first, &mut groups, &factory, around).await;
    // Whatever ended the loop, everything else ends with it.
    shared.stop.set();
    for (_, p) in links.peers.values() {
        p.ready.set();
    }
    for t in tasks {
        t.await;
    }
    jobs.wait().await;
    result
}

/// Tasks that come and go (marker jobs), counted so that the node can wait
/// for them at the end without keeping a handle per task.
#[derive(Clone, Default)]
struct Live(Arc<(Mutex<usize>, Signal)>);

impl Live {
    fn spawn<R: Runtime>(&self, rt: &R, task: impl std::future::Future<Output = ()> + std::marker::Send + 'static) {
        *self.0.0.lock().expect("live lock") += 1;
        let live = self.clone();
        // Dropping the handle detaches the task; the count tracks it.
        drop(rt.spawn(async move {
            task.await;
            let mut n = live.0.0.lock().expect("live lock");
            *n -= 1;
            if *n == 0 {
                live.0.1.set();
            }
        }));
    }

    async fn wait(&self) {
        loop {
            self.0.1.take();
            if *self.0.0.lock().expect("live lock") == 0 {
                return;
            }
            self.0.1.wait().await;
        }
    }
}

/// Writes one decided transaction's markers, one partition after another,
/// until each is written (or found not needed). Stops if the coordinator is
/// fenced, the job is cancelled or the node stops.
async fn write_markers<R: Runtime>(
    rt: R,
    client: Arc<lotse_client::Client<R>>,
    job: MarkerJob,
    shared: Arc<Shared>,
    backoff: Duration,
) {
    let stop = [&job.cancel, &shared.stop];
    for &(resource_id, partition) in &job.partitions {
        loop {
            // A deleted partition took the transaction's data with it: its
            // marker is moot, and its leader no longer answers for it.
            if shared.deleted.lock().expect("deleted lock").contains(&resource_id) {
                break;
            }
            let marker = lotse_proto::TxnMarker {
                producer_id: job.producer_id,
                producer_epoch: job.epoch,
                commit: job.commit,
                coordinator_epoch: job.term as u32,
                resources: Vec::new(),
            };
            match race(client.write_txn_marker(resource_id, partition, marker), &stop).await {
                None => return,
                Some(Ok(lotse_proto::ErrorCode::NONE)) => break,
                // A newer coordinator took over this producer's markers.
                Some(Ok(_)) => return,
                Some(Err(_)) => {
                    if race(rt.sleep(backoff), &stop).await.is_none() {
                        return;
                    }
                }
            }
        }
    }
    shared
        .commands
        .lock()
        .expect("commands lock")
        .push_back(Command::MarkersWritten { txn_id: job.txn_id, term: job.term });
    shared.work.set();
}

/// Sends a queue leader's dead letters to the dead-letter queue and reports
/// whether it has them. One attempt (the client retries what is retriable);
/// the leader tries again later after a failure, which is safe: the queue
/// that receives them knows what it took.
async fn forward_dead_letters<R: Runtime>(
    client: Arc<lotse_client::Client<R>>,
    job: crate::client::ForwardJob,
    shared: Arc<Shared>,
) {
    let crate::client::ForwardJob { group, term, target, offsets, batches } = job;
    let stop = [&shared.stop];
    let Some(result) = race(client.dead_letter(target, batches), &stop).await else { return };
    shared.commands.lock().expect("commands lock").push_back(Command::Forwarded {
        group,
        term,
        offsets,
        ok: result.is_ok(),
        full: matches!(&result, Err(lotse_client::ClientError::Broker { code, .. })
            if *code == lotse_proto::ErrorCode::QUEUE_FULL),
    });
    shared.work.set();
}

/// Asks the transaction coordinator whether a transaction is open and holds
/// a partition, and hands the answer to the main loop (ADR 0057). Any
/// failure to ask counts as a refusal the producer may retry: nothing was
/// written.
async fn verify_txn<R: Runtime>(
    client: Arc<lotse_client::Client<R>>,
    job: crate::client::VerifyJob,
    shared: Arc<Shared>,
) {
    let stop = [&shared.stop];
    let Some(result) = race(client.verify_txn(job.request), &stop).await else { return };
    let error = match result {
        Ok(()) => lotse_proto::ErrorCode::NONE,
        Err(lotse_client::ClientError::Broker { code, .. }) if !code.retriable() => code,
        Err(_) => lotse_proto::ErrorCode::COORDINATOR_NOT_AVAILABLE,
    };
    shared.commands.lock().expect("commands lock").push_back(Command::Verified { key: job.key, error });
    shared.work.set();
}

/// Asks each partition of a conditional delete whether it is empty, and
/// hands the answer to the main loop (ADR 0063). The first partition that
/// is not ends the asking; a failure to ask refuses the delete, which the
/// admin may send again: nothing was deleted.
async fn check_empty<R: Runtime>(
    client: Arc<lotse_client::Client<R>>,
    job: crate::client::EmptyJob,
    shared: Arc<Shared>,
) {
    let stop = [&shared.stop];
    let mut answer = Ok(None);
    for &p in &job.partitions {
        let Some(result) = race(client.check_empty(job.resource_id, p), &stop).await else { return };
        match result {
            Ok(true) => {}
            Ok(false) => {
                answer = Ok(Some(p));
                break;
            }
            Err(lotse_client::ClientError::Broker { code, .. }) if !code.retriable() => {
                answer = Err(code);
                break;
            }
            Err(_) => {
                answer = Err(lotse_proto::ErrorCode::REQUEST_TIMED_OUT);
                break;
            }
        }
    }
    shared.commands.lock().expect("commands lock").push_back(Command::EmptyChecked { key: job.key, answer });
    shared.work.set();
}

/// Writes a published message to each queue it routes to, one after
/// another, and hands the outcomes to the main loop (ADR 0079). What the
/// publisher's time does not allow is unknown, as is a write the client
/// could not settle.
async fn publish<R: Runtime>(
    rt: R,
    client: Arc<lotse_client::Client<R>>,
    job: crate::client::PublishJob,
    shared: Arc<Shared>,
) {
    use lotse_client::ClientError;
    use lotse_proto::ErrorCode;
    let deadline = rt.now() + job.timeout;
    let stop = [&shared.stop];
    let mut results = Vec::new();
    for (entry, queue_id, name, batch) in job.writes {
        let left = deadline.saturating_duration_since(rt.now());
        let write = race(client.produce(&name, 0, batch), &stop);
        let (error, offset) = match crate::signal::before(write, rt.sleep(left)).await {
            None => (ErrorCode::REQUEST_TIMED_OUT, 0),
            Some(None) => return,
            Some(Some(Ok(offset))) => (ErrorCode::NONE, offset),
            Some(Some(Err(e))) => match e {
                ClientError::Broker { code, .. } => (code, 0),
                ClientError::UnknownStream(..) => (ErrorCode::UNKNOWN_RESOURCE, 0),
                // Nothing was written: no leader took it in time.
                ClientError::GaveUp(..) | ClientError::NoAnswer(..) => (ErrorCode::NOT_LEADER, 0),
                _ => (ErrorCode::REQUEST_TIMED_OUT, 0),
            },
        };
        results.push((entry, queue_id, name, error, offset));
    }
    shared.commands.lock().expect("commands lock").push_back(Command::Published { key: job.key, results });
    shared.work.set();
}

/// What the main loop works with besides its groups.
struct Around<'a, R> {
    links: &'a mut Links,
    shared: &'a Arc<Shared>,
    jobs: &'a Live,
    auth: &'a Auth<R>,
}

async fn main_loop<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    first: &[NodeId],
    groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    factory: &Factory<S>,
    around: Around<'_, R>,
) -> Result<(), Error> {
    let Around { links, shared, jobs, auth } = around;
    let mut next_tick = rt.now() + cfg.tick;
    // What the groups do and when this loop falls behind, for the log
    // (ADR 0139); every group has the node's election timeout (ADR 0144).
    let mut elections = crate::elections::Elections::default();
    let election = cfg.election();
    // The node's own clients retry across a failover, which takes an
    // election timeout or two after a leader is lost: at least a second, and
    // five election timeouts when those are longer.
    let retry_for = (cfg.tick * 100).max(election * 5);
    let mut clients = Clients::default();
    clients.random = Some(Box::new({
        let rt = rt.clone();
        move || rt.next_u64()
    }));
    // The coordinator writes markers through the native protocol, like a client.
    let marker_client = Arc::new(lotse_client::Client::new(
        rt.clone(),
        lotse_client::ClientConfig {
            request_timeout: cfg.tick * 100,
            retry_for,
            backoff: cfg.tick * 5,
            idempotent: false,
            ..lotse_client::ClientConfig::new(cfg.client_addrs.values().cloned().collect())
        },
    ));
    // Publishes are written like a client's produces, idempotent so that
    // the client's retries never write a message twice to a queue (ADR
    // 0079).
    let publish_client = Arc::new(lotse_client::Client::new(
        rt.clone(),
        lotse_client::ClientConfig {
            request_timeout: cfg.tick * 100,
            retry_for,
            backoff: cfg.tick * 5,
            ..lotse_client::ClientConfig::new(cfg.client_addrs.values().cloned().collect())
        },
    ));
    let mut known = Known::configured(cfg);
    let configured_voters: Vec<NodeId> = first.to_vec();
    let started = rt.now();
    // The host is read every few seconds, not per request: CPU use is
    // measured between two reads.
    let (mut host, mut next_host) = (lotse_rt::HostStats::default(), started);
    // Whether this node is low on disk (ADR 0122), from each read of its
    // volume, every `disk_check_interval` (ADR 0124).
    let mut disk = crate::disk::DiskWatch::default();
    let (mut next_disk, reporting_disk) = (started, Arc::new(std::sync::atomic::AtomicBool::new(false)));
    // The instance of this process (ADR 0127): a turn it takes is given
    // back by the next process, once that one is ready, never by this one.
    let instance = loop {
        match rt.next_u64() {
            0 => continue,
            n => break n,
        }
    };
    let reporting_version = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut version_reported: Option<lotse_rt::Instant> = None;
    let releasing_turn = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut next_retain = started + RETAIN_EVERY;
    // Compacted partitions take turns at the node's one sweep (ADR 0098):
    // the next one to start is the first after this group.
    let mut sweep_turn: GroupId = 0;
    let mut links_at = 0;
    let mut links_learned: BTreeMap<NodeId, String> = BTreeMap::new();
    let (mut next_ask, asking) = (started, Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let (mut next_sizes, sizing) = (started, Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let mut next_membership = started;
    let mut moves = Moves::default();
    // While leaving (ADR 0114): when the node stops at the latest, and when
    // it last asked each group's voters to take over.
    let mut leave_until: Option<lotse_rt::Instant> = None;
    // When each group this node follows last had applied what its leader
    // said was committed, and the groups it must not serve reads of
    // (ADR 0116).
    let mut caught_up: BTreeMap<GroupId, lotse_rt::Instant> = BTreeMap::new();
    let mut lagging: BTreeSet<GroupId> = BTreeSet::new();
    // For the groups this node leads: in which term, its commit index over
    // the last `follower_max_lag`, and the voters current now, which a
    // leader names to a reader in their zone.
    let mut commit_samples: BTreeMap<GroupId, (u64, CommitSamples)> = BTreeMap::new();
    let mut current: BTreeMap<GroupId, BTreeSet<NodeId>> = BTreeMap::new();
    let mut leave_asked: BTreeMap<GroupId, (lotse_rt::Instant, usize)> = BTreeMap::new();
    while !shared.stop.is_set() {
        if known.refresh(rt, cfg, groups, factory, auth, &configured_voters).await? {
            shared.status.lock().expect("status lock").retain(|g, _| groups.contains_key(g));
        }
        // Drained: nothing here is needed any more, and what it would tell
        // clients only gets staler. It stops (ADR 0051). So does a node the
        // cluster forgot while it was down or cut off, whether or not it
        // saw its drain (ADR 0105): ids are never used twice.
        let forgotten = shared.forgotten.load(std::sync::atomic::Ordering::Relaxed);
        if forgotten || known.state.draining.contains(&cfg.id) {
            let core = core_groups(cfg);
            let managed = crate::client::managed_groups(&known.state, &core, &configured_voters);
            if crate::membership::stops(&known.state, &managed, &core, cfg.id, forgotten) {
                shared.removed.store(true, std::sync::atomic::Ordering::Relaxed);
                shared.stop.set();
                break;
            }
        }
        let learned: BTreeMap<NodeId, String> = shared
            .learned
            .lock()
            .expect("learned lock")
            .iter()
            .filter(|(id, _)| **id != cfg.id && !known.nodes.contains_key(id) && !known.state.gone.contains(id))
            .map(|(id, peer)| (*id, peer.clone()))
            .collect();
        if known.applied != links_at || learned != links_learned {
            links_at = known.applied;
            let mut want: BTreeMap<NodeId, String> =
                known.nodes.iter().map(|(id, (peer, _))| (*id, peer.clone())).collect();
            want.extend(learned.iter().map(|(id, peer)| (*id, peer.clone())));
            links.follow(rt, cfg.id, &want, shared, jobs, cfg.tick);
            *shared.members.lock().expect("members lock") =
                known.nodes.keys().copied().filter(|id| *id != cfg.id).collect();
            *shared.metadata_only.lock().expect("metadata-only lock") = learned.keys().copied().collect();
            links_learned = learned;
        }
        if rt.now() >= next_ask {
            next_ask = rt.now() + cfg.tick * ASK_NODES_EVERY_TICKS;
            ask_for_nodes(rt, cfg, groups, &known, &configured_voters, &marker_client, shared, jobs, &asking);
        }
        if rt.now() >= next_sizes {
            next_sizes = rt.now() + cfg.quota_check_interval;
            ask_for_sizes(rt, cfg, groups, &marker_client, shared, jobs, &sizing);
        }
        if *shared.deleted.lock().expect("deleted lock") != known.deleted {
            // Marker jobs look before each attempt.
            shared.deleted.lock().expect("deleted lock").clone_from(&known.deleted);
        }
        let wait = next_tick - rt.now();
        if !wait.is_zero() && !shared.work.take() {
            race(rt.sleep(wait), &[&shared.work, &shared.stop]).await;
            shared.work.take();
        }
        if rt.now() >= next_tick {
            crate::elections::tick_late(cfg.id, rt.now() - next_tick, cfg.tick, election);
            next_tick = rt.now() + cfg.tick;
            groups.values_mut().for_each(|g| g.tick());
            elections.retain(|g| groups.contains_key(&g));
            for (&g, replica) in groups.iter() {
                elections.observe(cfg.id, g, seen(replica), crate::elections::Cause::Tick);
            }
        }
        if rt.now() >= next_host {
            host = rt.host_stats().await;
            next_host = rt.now() + HOST_EVERY;
        }
        if rt.now() >= next_disk {
            let (free, total) = rt.disk_space().await;
            next_disk = rt.now() + cfg.disk_check_interval;
            disk.observe(cfg.disk_free_limit, free, total);
            report_disk(rt, cfg.id, &disk, &known, &marker_client, shared, jobs, &reporting_disk);
        }
        let now = rt.now();
        caught_up.retain(|g, _| groups.contains_key(g));
        lagging.clear();
        // A group this node no longer leads, or leads in another term (lost
        // and won again between two rounds), starts afresh: its voters are
        // current only once they catch up with this leadership.
        commit_samples.retain(|g, (term, _)| {
            groups.get(g).is_some_and(|r| r.raft_ref().role() == Role::Leader && r.raft_ref().term() == *term)
        });
        current.clear();
        for (&g, replica) in groups.iter() {
            let raft = replica.raft_ref();
            if raft.role() == Role::Leader {
                let progress = raft
                    .membership()
                    .voters
                    .iter()
                    .filter(|&&v| v != cfg.id)
                    .filter_map(|&v| raft.voter_progress(v).map(|p| (v, p)));
                let (_, samples) = commit_samples.entry(g).or_insert_with(|| (raft.term(), CommitSamples::default()));
                let now_current = current_voters(samples, raft.commit(), progress, now, cfg.follower_max_lag);
                if !now_current.is_empty() {
                    current.insert(g, now_current);
                }
                continue;
            }
            if raft.hears_leader() && replica.applied() >= raft.leader_commit() {
                caught_up.insert(g, now);
            }
            if !follower_current(raft.hears_leader(), caught_up.get(&g).copied(), now, cfg.follower_max_lag) {
                lagging.insert(g);
            }
        }
        // Rolling upgrades (ADR 0127): stop if the cluster runs at a version
        // this release does not run with; report this node's versions; give
        // back a turn the previous process took, once every group is ready.
        if let Some(why) = known.state.cluster_version.and_then(|v| cfg.versions.refuses(v)) {
            *shared.refused.lock().expect("refused lock") = Some(why);
            shared.stop.set();
        }
        report_version(rt, cfg, &known, &marker_client, shared, jobs, &reporting_version, &mut version_reported);
        if known.state.turn.is_some_and(|t| t.node == cfg.id && t.instance != instance) {
            let ready = groups.iter().all(|(g, r)| {
                let raft = r.raft_ref();
                crate::ops::GroupState::of(
                    raft.role() == Role::Leader,
                    raft.leads_with_quorum(),
                    raft.hears_leader(),
                    lagging.contains(g),
                )
                .ready()
            });
            if ready {
                release_turn(rt, cfg.id, &marker_client, shared, jobs, &releasing_turn);
            }
        }
        // Nodes low on disk: as the metadata group recorded them, and this
        // one at once, before its record commits (ADR 0122).
        let mut disk_low = known.state.disk_low.clone();
        if disk.low {
            disk_low.insert(cfg.id);
        }
        let view = ClusterView {
            streams: &known.streams,
            queues: &known.queues,
            exchanges: &known.exchanges,
            deleted: &known.deleted,
            group_session: cfg.group_session_timeout,
            group_heartbeat: cfg.group_heartbeat_interval,
            admin: AdminView {
                metadata: cfg.metadata,
                nodes: &known.active,
                configured_groups: cfg.groups.len(),
                voters: &known.voters,
                configured_users: &cfg.auth.users,
                retention: cfg.retention,
                members: &known.nodes,
                configured_voters: &configured_voters,
                client_cas: &cfg.auth.client_cas,
                renewal: &cfg.auth.renewal,
            },
            client_addrs: &known.client_addrs,
            zones: &known.zones,
            coordinator: cfg.coordinator,
            node: NodeView {
                id: cfg.id,
                started,
                net: &shared.net,
                host,
                sessions: &auth.sessions,
                token_sign_ins: &auth.token_sign_ins,
                disk: disk.metrics(),
                disk_low: &disk_low,
            },
            now: rt.now(),
            unix_ms: rt.unix_time().as_millis() as u64,
            access: &known.access,
            lagging: &lagging,
            current_voters: &current,
            metadata_behind: cfg.metadata.is_some_and(|g| match groups.get(&g) {
                Some(r) if r.raft_ref().role() == Role::Leader => {
                    r.raft_ref().log().term_at(r.applied()) != Some(r.raft_ref().term())
                }
                Some(_) => lagging.contains(&g),
                None => false,
            }),
        };
        let commands: Vec<Command> = shared.commands.lock().expect("commands lock").drain(..).collect();
        for command in commands {
            let (group, batch, reply) = match command {
                Command::Propose { group, batch, reply } => (group, batch, reply),
                Command::Client(req) => {
                    clients.handle(req, &view, groups).await?;
                    continue;
                }
                Command::MarkersWritten { txn_id, term } => {
                    clients.markers_written(&txn_id, term, &view, groups);
                    continue;
                }
                Command::Forwarded { group, term, offsets, ok, full } => {
                    clients.forwarded(group, term, offsets, ok, full, &view, groups);
                    continue;
                }
                Command::Verified { key, error } => {
                    clients.verified(key, error, &view, groups);
                    continue;
                }
                Command::EmptyChecked { key, answer } => {
                    clients.empty_checked(key, answer, &view, groups);
                    continue;
                }
                Command::Published { key, results } => {
                    clients.published(key, results);
                    continue;
                }
                Command::Observe { scraper, reply } => {
                    let mut obs = clients.observe(&view, groups);
                    obs.may_scrape = may_scrape(&scraper, auth, &known.access);
                    fill(&reply, obs);
                    continue;
                }
                Command::SafeToStop { reply } => {
                    let mine = groups.iter().map(|(g, r)| (*g, r.raft_ref().membership().clone())).collect();
                    jobs.spawn(rt, judge_stopping(cfg.id, mine, marker_client.clone(), reply, shared.stop.clone()));
                    continue;
                }
            };
            let res = match groups.get_mut(&group) {
                Some(g) => g.propose(batch).map(|p| (p.first, p.last)).map_err(|e: ProposeError| e.to_string()),
                None => Err(format!("group {group} is not hosted here")),
            };
            let mut r = reply.lock().expect("reply lock");
            r.0 = Some(res);
            if let Some(w) = r.1.take() {
                w.wake();
            }
        }
        let inbox: Vec<_> = shared.inbox.lock().expect("inbox lock").drain(..).collect();
        let core = core_groups(cfg);
        let mut stepped = BTreeSet::new();
        for (from, messages) in inbox {
            for (g, msg) in messages {
                // A runtime group's membership waits until this node's
                // replicas were refreshed with the decision behind it
                // (ADR 0108); the metadata and coordinator groups, which
                // carry no streams or queues, never wait.
                if !core.contains(&g) && crate::membership::ahead_of_view(&known.state, g, &msg) {
                    continue;
                }
                // A leaving node takes no group over, also not one pinned
                // to it (ADR 0114).
                if leave_until.is_some() && matches!(msg, Message::TimeoutNow { .. }) {
                    continue;
                }
                if let Some(group) = groups.get_mut(&g) {
                    group.step(from, msg);
                    stepped.insert(g);
                }
            }
            for g in std::mem::take(&mut stepped) {
                if let Some(replica) = groups.get(&g) {
                    elections.observe(cfg.id, g, seen(replica), crate::elections::Cause::Message(from));
                }
            }
        }
        // Appends are filled only after the round's own writes are
        // persisted too: a queue leader checks out messages it has just
        // appended, and the append that carries them carries the checkout
        // as well, so both commit together (ADR 0111).
        //
        // The groups persist at the same time (ADR 0143): each keeps its own
        // order (log, hard state, apply, ADR 0023), and the round waits for
        // the slowest group's syncs, not for their sum. One after the other,
        // a busy disk made a round outlast an election timeout, and the
        // followers of every group this node leads started elections.
        let persisting = rt.now();
        let mut sends: BTreeMap<GroupId, Vec<lotse_raft::Send>> = BTreeMap::new();
        for (g, out) in
            crate::join::all(groups.iter_mut().map(|(&g, group)| async move { (g, group.process_unfilled().await) }))
                .await
        {
            sends.entry(g).or_default().extend(out?.messages);
        }
        let marker_jobs = clients.after_round(&view, groups).await?;
        let again = groups.iter_mut().filter(|(_, group)| group.raft_ref().has_ready());
        for (g, out) in
            crate::join::all(again.map(|(&g, group)| async move { (g, group.process_unfilled().await) })).await
        {
            sends.entry(g).or_default().extend(out?.messages);
        }
        let mut outbound: BTreeMap<NodeId, Vec<(GroupId, Message)>> = BTreeMap::new();
        for (&g, group) in groups.iter_mut() {
            let mut messages = sends.remove(&g).unwrap_or_default();
            group.fill_messages(&mut messages).await?;
            for s in messages {
                outbound.entry(s.to).or_default().push((g, s.msg));
            }
        }
        crate::elections::slow(cfg.id, "persisting a round", rt.now() - persisting, cfg.tick, election);
        for job in marker_jobs {
            let task = write_markers(rt.clone(), marker_client.clone(), job, shared.clone(), cfg.tick * 5);
            jobs.spawn(rt, task);
        }
        for job in clients.take_forward_jobs() {
            jobs.spawn(rt, forward_dead_letters(marker_client.clone(), job, shared.clone()));
        }
        for job in clients.take_verify_jobs() {
            jobs.spawn(rt, verify_txn(marker_client.clone(), job, shared.clone()));
        }
        for job in clients.take_empty_jobs() {
            jobs.spawn(rt, check_empty(marker_client.clone(), job, shared.clone()));
        }
        for job in clients.take_turn_jobs() {
            let mine = groups.iter().map(|(g, r)| (*g, r.raft_ref().membership().clone())).collect();
            jobs.spawn(rt, take_turn(cfg.id, instance, mine, marker_client.clone(), job, shared.stop.clone()));
        }
        for job in clients.take_publish_jobs() {
            jobs.spawn(rt, publish(rt.clone(), publish_client.clone(), job, shared.clone()));
        }
        // After the round's answers: a request waiting on an entry learns
        // its fate before retention may compact it, which would leave the
        // outcome unknown.
        if rt.now() >= next_retain {
            next_retain = rt.now() + RETAIN_EVERY;
            let now_ms = rt.unix_time().as_millis() as u64;
            // A runtime stream's own settings, as the metadata group has
            // applied them here; configured streams keep the node's.
            let own: BTreeMap<GroupId, Retention> = cfg
                .metadata
                .and_then(|m| groups.get(&m))
                .map(|meta| {
                    let state = meta.metadata();
                    state
                        .streams
                        .values()
                        .flat_map(|s| {
                            let r = s.settings.retention(cfg.retention);
                            s.partitions.iter().map(move |(g, _)| (*g, r))
                        })
                        .collect()
                })
                .unwrap_or_default();
            // Runtime resources' own segment ages (ADR 0146), by group.
            let ages: BTreeMap<GroupId, u64> = cfg
                .metadata
                .and_then(|m| groups.get(&m))
                .map(|meta| {
                    let state = meta.metadata();
                    let mut ages = BTreeMap::new();
                    for (id, ms) in &state.segment_ms {
                        if let Some(s) = state.streams.get(id) {
                            ages.extend(s.partitions.iter().map(|(g, _)| (*g, *ms)));
                        } else if let Some(q) = state.queues.get(id) {
                            ages.insert(q.group, *ms);
                        }
                    }
                    ages
                })
                .unwrap_or_default();
            // One sweep at a time: the replica that holds one goes on with
            // it; when none does, the groups after the last to start get the
            // first chance.
            let mut sweeping = groups.iter().find(|(_, r)| r.sweeping()).map(|(g, _)| *g);
            let order: Vec<GroupId> =
                groups.range(sweep_turn + 1..).chain(groups.range(..=sweep_turn)).map(|(g, _)| *g).collect();
            for g in order {
                let group = groups.get_mut(&g).expect("listed above");
                if let Some(r) = own.get(&g) {
                    group.set_retention(*r);
                }
                // Runtime streams and queues close their segments by age
                // (ADR 0146); the node's own groups hold no one's data.
                let runtime = cfg.metadata != Some(g) && cfg.coordinator != Some(g);
                group.set_segment_ms(runtime.then(|| ages.get(&g).copied().unwrap_or(cfg.retention.segment_ms)));
                let sweep = crate::replica::Sweep {
                    may_start: sweeping.is_none_or(|s| s == g),
                    map_bytes: cfg.retention.compaction_map_bytes,
                };
                group.retain_with(now_ms, sweep).await?;
                group.expire_producers(rt.now(), cfg.producer_expiration);
                // The node's own groups hold no records of anyone's.
                if cfg.metadata != Some(g) && cfg.coordinator != Some(g) {
                    group.look_at_oldest().await?;
                }
                if sweeping.is_none() && group.sweeping() {
                    sweeping = Some(g);
                    sweep_turn = g;
                }
            }
        }
        if rt.now() >= next_membership {
            next_membership = rt.now() + cfg.tick * MEMBERSHIP_EVERY_TICKS;
            membership_pass(rt, cfg, groups, &known, &configured_voters, &mut moves, &marker_client, shared, jobs);
            report_certificates(rt, &known, auth, &marker_client, shared, jobs);
        }
        if let Some(within) = *shared.leaving.lock().expect("leaving lock") {
            let now = rt.now();
            let until = *leave_until.get_or_insert(now + within);
            if leave_pass(cfg, groups, now, &mut leave_asked) || now >= until {
                shared.stop.set();
                break;
            }
        }
        for (to, messages) in outbound {
            let Some((_, peer)) = links.peers.get(&to) else { continue };
            let frame = wire::encode(cfg.id, &messages);
            let mut q = peer.queue.lock().expect("peer queue lock");
            if q.len() >= PEER_QUEUE {
                q.pop_front();
            }
            q.push_back(frame);
            drop(q);
            peer.ready.set();
        }
        let mut status = shared.status.lock().expect("status lock");
        for (&g, group) in groups.iter() {
            let r = group.raft_ref();
            status.insert(
                g,
                GroupStatus {
                    role: r.role(),
                    term: r.term(),
                    leader: r.leader(),
                    commit: r.commit(),
                    applied: group.applied(),
                    sweeping: group.sweeping(),
                },
            );
        }
    }
    Ok(())
}

/// How often a node whose metadata group has no leader it lists asks for
/// the node list.
const ASK_NODES_EVERY_TICKS: u32 = 100;

/// A node whose metadata group has no leader, or one its metadata state does
/// not list, may lag behind nodes added since: their links are refused and
/// it has no address to answer them at, so it would never catch up. It asks
/// the metadata leader, through a node it lists, for the node list, and
/// links to those it is missing for the metadata group alone (ADR 0050).
/// A draining node asks as well. A node its own metadata state lists asks
/// for an answer at least as new as that state, and notes when the answer
/// leaves it out: the cluster forgot it (ADR 0105).
#[allow(clippy::too_many_arguments)]
fn ask_for_nodes<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    known: &Known,
    first_voters: &[NodeId],
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    asking: &Arc<std::sync::atomic::AtomicBool>,
) {
    let Some(meta) = cfg.metadata.and_then(|g| groups.get(&g)) else { return };
    let leader = meta.raft_ref().leader();
    // A draining node asks too: it learns so that the cluster forgot it,
    // should it have been cut off when its drain finished (ADR 0051). One
    // that never saw its drain asks because its metadata group has no
    // leader any more: nobody sends it anything once it is forgotten.
    let draining = known.state.draining.contains(&cfg.id);
    if leader.is_some_and(|l| l == cfg.id || known.nodes.contains_key(&l)) && !draining {
        return;
    }
    if asking.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let (client, shared, asking, me) = (client.clone(), shared.clone(), asking.clone(), cfg.id);
    let stop = shared.stop.clone();
    let listed = crate::membership::listed(&known.state, first_voters, me);
    let applied = meta.applied();
    jobs.spawn(rt, async move {
        let ask =
            async { if listed { client.describe_nodes_since(applied).await } else { client.describe_nodes().await } };
        if let Some(Ok(nodes)) = race(ask, &[&stop]).await {
            if crate::membership::forgotten(listed, nodes.iter().any(|n| n.node_id == me)) {
                shared.forgotten.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            let mut learned = shared.learned.lock().expect("learned lock");
            for n in nodes {
                learned.insert(n.node_id, n.peer);
            }
            drop(learned);
            shared.work.set();
        }
        asking.store(false, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Whether `scraper` may read the node's metrics, which name every
/// tenant's resources (ADR 0117): on loopback anyone on the machine; over
/// TLS the principal a client certificate names, as a session would take
/// it, if it sees the whole cluster (cluster-admin or cluster-viewer, ADR
/// 0043) and a cluster admin has not revoked the certificate (ADR 0090).
/// Without authentication required, whoever asks.
fn may_scrape<R: Runtime>(scraper: &crate::ops::Scraper, auth: &Auth<R>, access: &Access) -> bool {
    let crate::ops::Scraper::Tls { identity, cert } = scraper else { return true };
    if cert.as_ref().is_some_and(|c| auth.cert_revoked(c)) {
        return false;
    }
    let principal = match identity {
        Some(PeerIdentity::Node(_)) => return false,
        Some(PeerIdentity::Principal(name)) if auth.cfg.mtls => {
            crate::auth::Principal::User { name: name.clone(), groups: Vec::new() }
        }
        _ if !auth.cfg.required => crate::auth::Principal::Anonymous,
        _ => return false,
    };
    access.sees_cluster(&principal)
}

/// Asks every node for its metrics and judges, from what the leaders of
/// this node's groups heard lately, whether stopping it leaves one without
/// a quorum (ADR 0117). A node that does not answer leaves its groups
/// without a leader's word, which counts as critical: a rolling restart
/// waits rather than guesses.
async fn judge_stopping<R: Runtime>(
    me: NodeId,
    mine: BTreeMap<GroupId, lotse_raft::Membership>,
    client: Arc<lotse_client::Client<R>>,
    reply: Slot<Result<Vec<crate::ops::Critical>, String>>,
    stop: Signal,
) {
    let answer = match race(client.node_metrics(), &[&stop]).await {
        Some(Ok(nodes)) => {
            let reports: Vec<_> = nodes.into_iter().filter_map(|(_, m)| m.ok()).collect();
            Ok(crate::ops::quorum_critical(me, &mine, &reports))
        }
        Some(Err(e)) => Err(e.to_string()),
        None => Err("node stopped".into()),
    };
    fill(&reply, answer);
}

/// On the metadata group's leader: asks every node for its metrics and
/// keeps each group's size, the largest any replica reported, for the
/// planner to weigh moves by (ADR 0052) and to count storage quotas by
/// (ADR 0147). A group no node reported this time keeps the size it had,
/// so a node that is down does not shrink a count; a node that stops
/// leading forgets them all, and a new leader counts only once every group
/// has a size again.
fn ask_for_sizes<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    sizing: &Arc<std::sync::atomic::AtomicBool>,
) {
    let leads = cfg.metadata.and_then(|g| groups.get(&g)).is_some_and(|r| r.raft_ref().role() == Role::Leader);
    if !leads {
        shared.sizes.lock().expect("sizes lock").clear();
        *shared.sized_at.lock().expect("sized lock") = None;
        *shared.quota_usage.lock().expect("usage lock") = None;
        return;
    }
    if sizing.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let rt2 = rt.clone();
    let (client, shared, sizing) = (client.clone(), shared.clone(), sizing.clone());
    let stop = shared.stop.clone();
    jobs.spawn(rt, async move {
        if let Some(Ok(nodes)) = race(client.node_metrics(), &[&stop]).await {
            let mut sizes = BTreeMap::new();
            let mut removed = BTreeMap::new();
            for m in nodes.into_iter().filter_map(|(_, m)| m.ok()) {
                removed.insert(NodeId::from(m.node_id), m.removed_through);
                for p in m.partitions {
                    let s = sizes.entry(p.group).or_insert(0u64);
                    *s = (*s).max(p.size_bytes);
                }
            }
            shared.sizes.lock().expect("sizes lock").extend(sizes);
            *shared.sized_at.lock().expect("sized lock") = Some(rt2.unix_time().as_millis() as u64);
            *shared.removed_through.lock().expect("removed lock") = removed;
        }
        sizing.store(false, std::sync::atomic::Ordering::Relaxed);
    });
}

/// Takes connections from the other nodes. When the network verifies peers,
/// a connection is only taken from a node of this cluster, and carries only
/// that node's frames: a certificate from the cluster's CA for some other
/// node id, or for no node at all, gets nowhere.
async fn accept<R: Runtime>(rt: R, listener: R::Listener, shared: Arc<Shared>, metadata: Option<GroupId>) {
    let mut readers = Vec::new();
    let verify = rt.verifies_peers();
    while let Some(Ok((conn, _peer))) = race(listener.accept(), &[&shared.stop]).await {
        let member = |id| {
            shared.members.lock().expect("members lock").contains(&id)
                || shared.metadata_only.lock().expect("metadata-only lock").contains(&id)
        };
        let sender = match (verify, conn.peer_identity()) {
            (false, _) => None,
            (true, Some(PeerIdentity::Node(id))) if member(id) => Some(id),
            (true, _) => continue,
        };
        readers.push(rt.spawn(read_from(conn, shared.clone(), sender, metadata)));
    }
    drop(listener);
    for r in readers {
        r.await;
    }
}

async fn accept_clients<R: Runtime>(rt: R, listener: R::Listener, shared: Arc<Shared>, auth: Arc<Auth<R>>) {
    let to_main = shared.clone();
    let commands: Arc<dyn Fn(ClientRequest) + std::marker::Send + Sync> = Arc::new(move |req| {
        to_main.commands.lock().expect("commands lock").push_back(Command::Client(req));
        to_main.work.set();
    });
    // Connections come and go for as long as the node runs: count the live
    // ones instead of keeping a handle per connection, and wait for them at
    // the end.
    let live = Arc::new((Mutex::new(0usize), Signal::default()));
    while let Some(Ok((conn, peer))) = race(listener.accept(), &[&shared.stop]).await {
        *live.0.lock().expect("live lock") += 1;
        let (rt2, commands, stop, live) = (rt.clone(), commands.clone(), shared.stop.clone(), live.clone());
        let auth = auth.clone();
        let net = shared.net.clone();
        // Dropping the handle detaches the task; `live` tracks it.
        drop(rt.spawn(async move {
            net.connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::client::serve(rt2, conn, peer, commands, net.clone(), stop, auth).await;
            net.connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            let mut n = live.0.lock().expect("live lock");
            *n -= 1;
            if *n == 0 {
                live.1.set();
            }
        }));
    }
    drop(listener);
    loop {
        live.1.take();
        if *live.0.lock().expect("live lock") == 0 {
            break;
        }
        live.1.wait().await;
    }
}

/// Reads one peer's frames into the inbox. `sender` is the node the
/// connection proved to be, if the network verifies peers.
async fn read_from<C: Conn>(conn: C, shared: Arc<Shared>, sender: Option<NodeId>, metadata: Option<GroupId>) {
    while let Some(Ok(Some(frame))) = race(conn.recv(), &[&shared.stop]).await {
        NetStats::add(&shared.net.peer_in, frame.len());
        // A frame that does not decode means a broken or foreign peer: drop
        // the connection, the peer reconnects.
        let Ok(decoded) = wire::decode(frame) else { return };
        let decoded = match sender {
            None => decoded,
            Some(id) => {
                let listed = shared.members.lock().expect("members lock").contains(&id);
                let metadata_only = shared.metadata_only.lock().expect("metadata-only lock").contains(&id);
                match heard(id, listed, metadata_only, metadata, decoded) {
                    Some(d) => d,
                    None => return,
                }
            }
        };
        shared.inbox.lock().expect("inbox lock").push_back(decoded);
        shared.work.set();
    }
}

/// What of a frame from the node a connection proved to be (`sender`) is
/// taken: nothing if it speaks for another node or is not a member; from a
/// node this node does not list yet but learned of (`metadata_only`), only
/// the metadata group's messages (ADR 0050).
fn heard(
    sender: NodeId,
    listed: bool,
    metadata_only: bool,
    metadata: Option<GroupId>,
    (from, mut messages): Incoming,
) -> Option<Incoming> {
    if from != sender || !(listed || metadata_only) {
        return None;
    }
    if !listed {
        messages.retain(|(g, _)| Some(*g) == metadata);
    }
    Some((from, messages))
}

/// Keeps a connection to node `to` and sends its frames. When the network
/// verifies peers, a connection whose certificate names another node is
/// dropped like a failed one: the address may now belong to someone else.
///
/// The other end never writes on this connection, so a read on it ends
/// only when the other end closes it. Then this end connects anew, even
/// though its sends still succeed: a node that restarts at another address,
/// as a Kubernetes pod does, leaves an address nobody answers at, and TCP
/// would go on taking frames for it for many minutes (ADR 0130).
async fn send_to<R: Runtime>(rt: R, to: NodeId, addr: String, peer: Arc<Peer>, shared: Arc<Shared>, tick: Duration) {
    let (net, stop) = (&shared.net, [&shared.stop, &peer.closed]);
    let mut backoff = tick;
    let verify = rt.verifies_peers();
    while !stop.iter().any(|s| s.is_set()) {
        let connected = race(rt.connect(&addr, Link::Node), &stop).await.map(|r| {
            r.and_then(|c| match c.peer_identity() {
                _ if !verify => Ok(c),
                Some(PeerIdentity::Node(id)) if id == to => Ok(c),
                other => Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("{addr} is {other:?}, not node {to}"),
                )),
            })
        });
        let conn = match connected {
            Some(Ok(c)) => c,
            Some(Err(_)) => {
                race(rt.sleep(backoff), &stop).await;
                backoff = (backoff * 2).min(tick * 50);
                continue;
            }
            None => return,
        };
        backoff = tick;
        let sending = async {
            loop {
                let next = peer.queue.lock().expect("peer queue lock").pop_front();
                let Some(frame) = next else {
                    if race(peer.ready.wait(), &stop).await.is_none() {
                        return;
                    }
                    peer.ready.take();
                    continue;
                };
                NetStats::add(&net.peer_out, frame.len());
                if race(conn.send(frame), &stop).await.is_none_or(|r| r.is_err()) {
                    return; // reconnect; the frame is lost, Raft resends
                }
            }
        };
        let closed = async { while let Ok(Some(_)) = conn.recv().await {} };
        // Either way, connect anew (or stop, which the loop checks).
        let _ = before(sending, closed).await;
    }
}

/// Every configured resource, by id and name.
fn configured_ids(cfg: &NodeConfig) -> Vec<(u64, String)> {
    let streams = cfg.streams.iter().map(|s| (s.resource_id, s.name.clone()));
    let queues = cfg.queues.iter().map(|q| (q.resource_id, q.name.clone()));
    let exchanges = cfg.exchanges.iter().map(|x| (x.resource_id, x.name.clone()));
    streams.chain(queues).chain(exchanges).collect()
}

/// The resources this node knows: configured ones and, once the metadata
/// group has applied them, those created at runtime.
struct Known {
    streams: Vec<StreamConfig>,
    queues: Vec<QueueConfig>,
    exchanges: Vec<lotse_route::Exchange>,
    /// Voters of runtime groups.
    voters: BTreeMap<GroupId, Vec<NodeId>>,
    /// Resources deleted at runtime.
    deleted: crate::metadata::Deleted,
    /// Groups of deleted resources whose files this process has removed.
    removed: BTreeSet<GroupId>,
    /// Groups moved off this node whose logs this process has removed.
    forgotten: BTreeSet<GroupId>,
    /// The metadata group's applied index these were built from.
    applied: u64,
    /// Who may do what.
    access: Access,
    /// The cluster's nodes: those from the configuration and those added
    /// since, without those gone; `(peer, client)` addresses.
    nodes: BTreeMap<NodeId, (String, String)>,
    /// Nodes new groups may be placed on: not draining.
    active: Vec<NodeId>,
    /// Every node's client address, for `Metadata`.
    client_addrs: BTreeMap<NodeId, String>,
    /// The zones of the nodes that have one, for `Metadata`.
    zones: BTreeMap<NodeId, String>,
    /// The metadata state these were built from.
    state: MetadataState,
}

impl Known {
    fn configured(cfg: &NodeConfig) -> Known {
        Known {
            streams: cfg.streams.clone(),
            queues: cfg.queues.clone(),
            exchanges: cfg.exchanges.clone(),
            voters: BTreeMap::new(),
            deleted: crate::metadata::Deleted::default(),
            removed: BTreeSet::new(),
            forgotten: BTreeSet::new(),
            applied: 0,
            access: Access::build(&cfg.auth, &configured_ids(cfg), &MetadataState::default()),
            nodes: cfg
                .peers
                .iter()
                .map(|(id, peer)| (*id, (peer.clone(), cfg.client_addrs.get(id).cloned().unwrap_or_default())))
                .collect(),
            active: cfg.peers.keys().copied().collect(),
            client_addrs: cfg.client_addrs.clone(),
            zones: cfg.zones.clone(),
            state: MetadataState::default(),
        }
    }

    /// Rebuilds the view after the metadata group applied something, opens
    /// the runtime groups this node is a voter of, and closes and removes
    /// deleted ones. Returns whether it closed a group.
    #[allow(clippy::too_many_arguments)]
    async fn refresh<R: Runtime, S: StateMachine>(
        &mut self,
        rt: &R,
        cfg: &NodeConfig,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
        factory: &Factory<S>,
        auth: &Auth<R>,
        configured_voters: &[NodeId],
    ) -> Result<bool, Error> {
        let Some(meta) = cfg.metadata.and_then(|g| groups.get(&g)) else { return Ok(false) };
        if meta.applied() == self.applied {
            return Ok(false);
        }
        self.applied = meta.applied();
        let state = meta.metadata().clone();
        auth.set_users(&state.users);
        auth.set_revocations(&state.revocations);
        auth.set_revoked_certs(&state.revoked_certs);
        auth.set_renewed(&state.renewed);
        auth.set_tokens(&state.tokens);
        let mut known = Known::configured(cfg);
        known.access = Access::build(&cfg.auth, &configured_ids(cfg), &state);
        known.applied = self.applied;
        known.removed = std::mem::take(&mut self.removed);
        known.forgotten = std::mem::take(&mut self.forgotten);
        known.deleted = crate::metadata::Deleted::of(&state, self.applied);
        for (id, n) in &state.nodes {
            known.nodes.insert(*id, (n.peer.clone(), n.client.clone()));
            if !n.zone.is_empty() {
                known.zones.insert(*id, n.zone.clone());
            }
        }
        known.nodes.retain(|id, _| !state.gone.contains(id));
        known.active = known.nodes.keys().copied().filter(|id| !state.draining.contains(id)).collect();
        known.client_addrs = known.nodes.iter().map(|(id, (_, client))| (*id, client.clone())).collect();
        // Deleted groups close and lose their files. The tombstone stays in
        // the metadata state, which is durable before anything is removed,
        // so a node that stops halfway never opens the group again and
        // finishes the removal after a restart.
        let mut closed = false;
        for g in state.deleted_groups() {
            closed |= groups.remove(&g).is_some();
            if known.removed.insert(g) {
                remove_group_files(rt, &group_dir(g)).await?;
            }
        }
        for s in state.streams.values() {
            known.streams.push(StreamConfig {
                name: s.name.clone(),
                resource_id: s.resource_id,
                partitions: s.partitions.iter().map(|(g, _)| *g).collect(),
            });
        }
        for q in state.queues.values() {
            known.queues.push(QueueConfig {
                name: q.name.clone(),
                resource_id: q.resource_id,
                group: q.group,
                ack_timeout: match q.ack_timeout_ms {
                    0 => QueueConfig::DEFAULT_ACK_TIMEOUT,
                    ms => Duration::from_millis(ms),
                },
                limits: lotse_queue::Limits {
                    max_deliveries: q.limits.max_deliveries,
                    message_ttl_ms: q.limits.message_ttl_ms,
                },
                dead_letter: Some(q.limits.dead_letter).filter(|d| *d != 0),
                single_active: q.limits.single_active,
                priority_levels: q.limits.priority_levels,
                length: q.limits.length,
                resume_window: match q.limits.resume_window_ms {
                    0 => QueueConfig::DEFAULT_RESUME_WINDOW,
                    ms => Duration::from_millis(ms),
                },
            });
        }
        for x in state.exchanges.values() {
            known.exchanges.push(lotse_route::Exchange {
                name: x.name.clone(),
                resource_id: x.resource_id,
                kind: x.kind,
                bindings: Vec::new(),
            });
        }
        for x in &mut known.exchanges {
            if let Some(extra) = state.bindings.get(&x.resource_id) {
                for b in extra {
                    if !x.bindings.contains(b) {
                        x.bindings.push(b.clone());
                    }
                }
            }
        }
        // Computed once, and only when a new replica opens.
        let mut preferred: Option<BTreeMap<GroupId, NodeId>> = None;
        for (g, voters, queue) in state.groups() {
            known.voters.insert(g, voters.to_vec());
            // Moved off this node: once the group's leader reported the new
            // voters in place, this replica is no member any more and goes.
            // Nothing places it here again (`left`).
            // So is a replica left behind by a node that was down when the
            // move settled: it never opens the group again, but its files
            // are still there, once per start.
            if voters.contains(&cfg.id) {
                // Back on this node: should it move off again, clean again.
                known.forgotten.remove(&g);
                // Placed here again with a replica from before a move off
                // this node settled, which this node did not see (ADR 0108):
                // that log starts over. A replica opened from an older view
                // is checked here the next round; until then it takes no
                // membership newer than this view (`ahead_of_view`).
                if let Some(r) = groups.get(&g)
                    && let Some(off) =
                        crate::membership::starts_over(&state, g, cfg.id, r.raft_ref().membership().tag, r.moved_at())
                {
                    closed |= groups.remove(&g).is_some();
                    forget_group_log(rt, &group_dir(g), off).await?;
                }
            }
            if !known.forgotten.contains(&g) && !voters.contains(&cfg.id) && state.is_settled(g, voters) {
                let tag = match groups.get(&g) {
                    Some(r) => r.raft_ref().membership().tag,
                    None => tag_on_disk(rt, cfg, g, factory, &state).await?,
                };
                if crate::membership::replica_may_go(&state, g, voters, cfg.id, tag) {
                    known.forgotten.insert(g);
                    closed |= groups.remove(&g).is_some();
                    forget_group_log(rt, &group_dir(g), state.target_at.get(&g).copied().unwrap_or(0)).await?;
                }
            }
            // Only voters replicate a group; the others learn its leader
            // from them. A replica starts from the group's first voters, as
            // every replica of it did: one placed later is outside them and
            // learns who the members are from the group's log.
            if crate::membership::hosts(&state, g, voters, cfg.id)
                && !known.forgotten.contains(&g)
                && !groups.contains_key(&g)
            {
                let first = state.initial_voters(g).unwrap_or(voters).to_vec();
                let raft = cfg.raft(rt.next_u64(), first);
                let options =
                    ReplicaOptions { max_producers: cfg.max_producers, queue, retention: retention_of(cfg, g, queue) };
                let mut replica =
                    Replica::open_with(rt.clone(), group_dir(g), raft, factory(g), cfg.log, options).await?;
                // A new group's first leader is its preferred node (ADR 0095):
                // that replica campaigns at once, the others wait one election
                // timeout more, so no handover follows. A replica with a term
                // (one that was here before, or moved back) waits as always.
                let fresh = replica.raft_ref().term() == 0 && replica.raft_ref().last_index() == 0;
                if fresh {
                    let preferred = preferred.get_or_insert_with(|| {
                        crate::membership::preferred_leaders(
                            &crate::client::managed_groups(&state, &core_groups(cfg), configured_voters),
                            &state.pins,
                        )
                    });
                    match preferred.get(&g) {
                        Some(&p) if p == cfg.id => replica.raft().campaign_soon(),
                        Some(&p) if voters.contains(&p) => replica.raft().delay_election(),
                        _ => {}
                    }
                }
                groups.insert(g, replica);
            }
        }
        known.state = state;
        *self = known;
        Ok(closed)
    }
}

/// Where a group lives on the disk.
fn group_dir(g: GroupId) -> PathBuf {
    PathBuf::from("groups").join(g.to_string())
}

/// The tag of the membership a group's replica on this node's disk holds,
/// for a replica that is not open: 0 when nothing but its term and vote is
/// there (and the record of an earlier move). Opening it reads the snapshot and the log once; a node only does
/// so while deciding whether a moved replica may go.
async fn tag_on_disk<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    g: GroupId,
    factory: &Factory<S>,
    state: &MetadataState,
) -> Result<u64, Error> {
    let dir = group_dir(g);
    let files = match rt.list(&dir).await {
        Ok(files) => files,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let kept = |f: &PathBuf| f.ends_with(crate::replica::HARD_STATE) || f.ends_with(crate::replica::MOVED);
    if files.iter().all(kept) && rt.list(&dir.join("log")).await.is_err() {
        return Ok(0);
    }
    let first = state.initial_voters(g).or_else(|| state.voters_of(g)).unwrap_or(&[]).to_vec();
    let raft = cfg.raft(rt.next_u64(), first);
    let replica = Replica::open_with(rt.clone(), dir, raft, factory(g), cfg.log, ReplicaOptions::default()).await?;
    Ok(replica.raft_ref().membership().tag)
}

/// Removes a group's log and snapshot from this node after its replica
/// moved to another node (ADR 0050), but keeps its term and vote, with the
/// commit index back at 0. Should the group come back to this node, the new
/// replica starts from them and never votes twice in a term: what an empty
/// log rejoining under the same id would otherwise risk. It also records the
/// decision that moved it (`moved_at`): until the replica holds a newer
/// membership it grants no vote (see [`Replica::step`]). Both are written
/// before anything is removed, so a crash halfway leaves a replica that is
/// merely behind.
async fn forget_group_log<D: Disk>(disk: &D, dir: &Path, moved_at: u64) -> Result<(), Error> {
    match disk.list(dir).await {
        Ok(_) => {}
        // Never held here: nothing to forget.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    let path = dir.join(crate::replica::HARD_STATE);
    let kept = match crate::replica::read_file(disk, &path).await? {
        Some(bytes) => Some(crate::replica::decode_hard_state(bytes)?),
        None => None,
    };
    if let Some(mut hs) = kept {
        hs.commit = 0;
        crate::replica::write_atomic(disk, dir, crate::replica::HARD_STATE, crate::replica::encode_hard_state(&hs))
            .await?;
    }
    // Before anything goes: a replica that finds its log gone but no such
    // record would vote as if it had never acknowledged anything.
    crate::replica::write_atomic(disk, dir, crate::replica::MOVED, crate::replica::encode_moved(moved_at)).await?;
    let keep = [path.clone(), dir.join(crate::replica::MOVED)];
    for d in [dir.join("log"), dir.to_path_buf()] {
        let files = match disk.list(&d).await {
            Ok(files) => files,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for f in files.into_iter().filter(|f| !keep.contains(f)) {
            if f == dir.join("log") {
                continue;
            }
            disk.remove(&f).await?;
        }
        if d != *dir {
            disk.remove_dir(&d).await?;
        }
    }
    Ok(())
}

/// Removes a closed group's files and directories, as much as is there:
/// nothing if it never ran here, the rest if a removal was cut short.
async fn remove_group_files<D: Disk>(disk: &D, dir: &Path) -> Result<(), Error> {
    for d in [dir.join("log"), dir.to_path_buf()] {
        let files = match disk.list(&d).await {
            Ok(files) => files,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for f in files {
            disk.remove(&f).await?;
        }
        disk.remove_dir(&d).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

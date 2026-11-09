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
use crate::signal::{Signal, race};
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
    /// Pre-vote on or off.
    pub pre_vote: bool,
    /// Log segment settings.
    pub log: LogConfig,
    /// Where clients connect (`host:port`); `None` serves no clients.
    pub client_listen: Option<String>,
    /// Every node's client address, as `Metadata` reports it.
    pub client_addrs: BTreeMap<NodeId, String>,
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
    /// Producers kept per replica for idempotence; every node must use the
    /// same value ([`crate::DEFAULT_MAX_PRODUCERS`] by default).
    pub max_producers: usize,
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

/// What streams keep by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionConfig {
    /// Records are kept this many ms (by their timestamps); `None`: for
    /// ever.
    pub ms: Option<u64>,
    /// Each partition keeps this many bytes of log; `None`: no limit.
    pub bytes: Option<u64>,
}

impl Default for RetentionConfig {
    /// Seven days, no size limit.
    fn default() -> Self {
        RetentionConfig { ms: Some(7 * 24 * 3600 * 1000), bytes: None }
    }
}

impl RetentionConfig {
    /// Keep everything.
    pub fn keep() -> Self {
        RetentionConfig { ms: None, bytes: None }
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
}

/// A node hands over at most one group's leadership in this time.
const HANDOVER_EVERY_TICKS: u32 = 100;
/// And asks the same group's preferred leader again only after this.
const HANDOVER_RETRY_TICKS: u32 = 1_000;

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
    for (&g, replica) in groups.iter_mut() {
        let raft = replica.raft();
        if raft.role() != Role::Leader {
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
                let now = rt.now();
                if !crate::client::moving(state, g, &voters) {
                    // In place: spread the leaders (ADR 0050).
                    let last = raft.last_index();
                    let matched = raft.matched().unwrap_or_default();
                    let due = moves.last_handover.is_none_or(|t| now - t >= cfg.tick * HANDOVER_EVERY_TICKS)
                        && moves.tried.get(&g).is_none_or(|t| now - *t >= cfg.tick * HANDOVER_RETRY_TICKS);
                    let to =
                        crate::membership::hand_over_to(&current, cfg.id, preferred.get(&g).copied(), &matched, last);
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
}

/// How often each replica looks whether retention lets something go.
const RETAIN_EVERY: Duration = Duration::from_secs(1);

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
}

/// A decoded frame: its sender and the messages per group.
type Incoming = (NodeId, Vec<(GroupId, Message)>);

type Reply = Arc<Mutex<(Option<Result<(u64, u64), String>>, Option<std::task::Waker>)>>;

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
    },
}

#[derive(Default)]
struct Shared {
    inbox: Mutex<VecDeque<Incoming>>,
    commands: Mutex<VecDeque<Command>>,
    status: Mutex<BTreeMap<GroupId, GroupStatus>>,
    /// Resources deleted at runtime, for marker jobs.
    deleted: Mutex<BTreeSet<u64>>,
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
    /// Set when the metadata leader listed the nodes without this one.
    unlisted: std::sync::atomic::AtomicBool,
    /// Bytes of each group's log, the largest any replica reported, as the
    /// metadata leader last asked the nodes: what rebalancing weighs moves
    /// by (ADR 0052).
    sizes: Mutex<BTreeMap<GroupId, u64>>,
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

    /// The last known status of `group`.
    pub fn status(&self, group: GroupId) -> Option<GroupStatus> {
        self.shared.status.lock().expect("status lock").get(&group).copied()
    }

    /// Asks the node to stop; its task finishes once everything is closed.
    pub fn stop(&self) {
        self.shared.stop.set();
    }

    /// Whether the node stopped by itself because it was drained: no group
    /// counts it any more, and the cluster forgets it (ADR 0051).
    pub fn removed(&self) -> bool {
        self.shared.removed.load(std::sync::atomic::Ordering::Relaxed)
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
            let raft = Config { seed: rt.next_u64(), pre_vote: cfg.pre_vote, ..Config::new(cfg.id, voters.clone()) };
            let queue = cfg.queues.iter().any(|q| q.group == g);
            let options =
                ReplicaOptions { max_producers: cfg.max_producers, queue, retention: retention_of(&cfg, g, queue) };
            let replica = Replica::open_with(rt.clone(), group_dir(g), raft, state_machine(g), cfg.log, options);
            groups.insert(g, replica.await?);
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
    let mut clients = Clients::default();
    // The coordinator writes markers through the native protocol, like a client.
    let marker_client = Arc::new(lotse_client::Client::new(
        rt.clone(),
        lotse_client::ClientConfig {
            request_timeout: cfg.tick * 100,
            retry_for: cfg.tick * 100,
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
            retry_for: cfg.tick * 100,
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
    let mut next_retain = started + RETAIN_EVERY;
    let mut links_at = 0;
    let mut links_learned: BTreeMap<NodeId, String> = BTreeMap::new();
    let (mut next_ask, asking) = (started, Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let (mut next_sizes, sizing) = (started, Arc::new(std::sync::atomic::AtomicBool::new(false)));
    let mut next_membership = started;
    let mut moves = Moves::default();
    while !shared.stop.is_set() {
        if known.refresh(rt, cfg, groups, factory, auth).await? {
            shared.status.lock().expect("status lock").retain(|g, _| groups.contains_key(g));
        }
        // Drained: nothing here is needed any more, and what it would tell
        // clients only gets staler. It stops (ADR 0051). So does a draining
        // node the cluster forgot while it was cut off: the metadata leader
        // no longer lists it, and ids are never used twice.
        if known.state.draining.contains(&cfg.id) {
            let core = core_groups(cfg);
            let managed = crate::client::managed_groups(&known.state, &core, &configured_voters);
            let unlisted = shared.unlisted.load(std::sync::atomic::Ordering::Relaxed);
            if crate::membership::stops(&known.state, &managed, &core, cfg.id, unlisted) {
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
            ask_for_nodes(rt, cfg, groups, &known, &marker_client, shared, jobs, &asking);
        }
        if rt.now() >= next_sizes {
            next_sizes = rt.now() + cfg.tick * SIZES_EVERY_TICKS;
            ask_for_sizes(rt, cfg, groups, &marker_client, shared, jobs, &sizing);
        }
        if shared.deleted.lock().expect("deleted lock").len() != known.deleted.len() {
            // Marker jobs look before each attempt.
            shared.deleted.lock().expect("deleted lock").clone_from(&known.deleted);
        }
        let wait = next_tick - rt.now();
        if !wait.is_zero() && !shared.work.take() {
            race(rt.sleep(wait), &[&shared.work, &shared.stop]).await;
            shared.work.take();
        }
        if rt.now() >= next_tick {
            next_tick = rt.now() + cfg.tick;
            groups.values_mut().for_each(|g| g.tick());
        }
        if rt.now() >= next_host {
            host = rt.host_stats().await;
            next_host = rt.now() + HOST_EVERY;
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
            },
            client_addrs: &known.client_addrs,
            coordinator: cfg.coordinator,
            node: NodeView { id: cfg.id, started, net: &shared.net, host },
            now: rt.now(),
            unix_ms: rt.unix_time().as_millis() as u64,
            access: &known.access,
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
                Command::Forwarded { group, term, offsets, ok } => {
                    clients.forwarded(group, term, offsets, ok, &view, groups);
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
        for (from, messages) in inbox {
            for (g, msg) in messages {
                if let Some(group) = groups.get_mut(&g) {
                    group.step(from, msg);
                }
            }
        }
        let mut outbound: BTreeMap<NodeId, Vec<(GroupId, Message)>> = BTreeMap::new();
        for (&g, group) in groups.iter_mut() {
            let out = group.process().await?;
            for s in out.messages {
                outbound.entry(s.to).or_default().push((g, s.msg));
            }
        }
        for job in clients.after_round(&view, groups).await? {
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
            for (g, group) in groups.iter_mut() {
                if let Some(r) = own.get(g) {
                    group.set_retention(*r);
                }
                group.retain(now_ms).await?;
            }
        }
        if rt.now() >= next_membership {
            next_membership = rt.now() + cfg.tick * MEMBERSHIP_EVERY_TICKS;
            membership_pass(rt, cfg, groups, &known, &configured_voters, &mut moves, &marker_client, shared, jobs);
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
/// A draining node asks as well, and notes when the list leaves it out.
#[allow(clippy::too_many_arguments)]
fn ask_for_nodes<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    known: &Known,
    client: &Arc<lotse_client::Client<R>>,
    shared: &Arc<Shared>,
    jobs: &Live,
    asking: &Arc<std::sync::atomic::AtomicBool>,
) {
    let Some(meta) = cfg.metadata.and_then(|g| groups.get(&g)) else { return };
    let leader = meta.raft_ref().leader();
    // A draining node asks too: it learns so that the cluster forgot it,
    // should it have been cut off when its drain finished (ADR 0051).
    let draining = known.state.draining.contains(&cfg.id);
    if leader.is_some_and(|l| l == cfg.id || known.nodes.contains_key(&l)) && !draining {
        return;
    }
    if asking.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let (client, shared, asking, me) = (client.clone(), shared.clone(), asking.clone(), cfg.id);
    let stop = shared.stop.clone();
    jobs.spawn(rt, async move {
        if let Some(Ok(nodes)) = race(client.describe_nodes(), &[&stop]).await {
            if !nodes.iter().any(|n| n.node_id == me) {
                shared.unlisted.store(true, std::sync::atomic::Ordering::Relaxed);
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

/// How often the metadata group's leader asks the nodes for their groups'
/// sizes.
const SIZES_EVERY_TICKS: u32 = 1_000;

/// On the metadata group's leader: asks every node for its metrics and
/// keeps each group's size, the largest any replica reported, for the
/// planner to weigh moves by (ADR 0052). Sizes may be minutes old; they
/// only choose between moves the replica counts already call for.
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
    if !leads || sizing.swap(true, std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let (client, shared, sizing) = (client.clone(), shared.clone(), sizing.clone());
    let stop = shared.stop.clone();
    jobs.spawn(rt, async move {
        if let Some(Ok(nodes)) = race(client.node_metrics(), &[&stop]).await {
            let mut sizes = BTreeMap::new();
            for m in nodes.into_iter().filter_map(|(_, m)| m.ok()) {
                for p in m.partitions {
                    let s = sizes.entry(p.group).or_insert(0u64);
                    *s = (*s).max(p.size_bytes);
                }
            }
            *shared.sizes.lock().expect("sizes lock") = sizes;
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
                break; // reconnect; the frame is lost, Raft resends
            }
        }
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
    deleted: BTreeSet<u64>,
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
            deleted: BTreeSet::new(),
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
            state: MetadataState::default(),
        }
    }

    /// Rebuilds the view after the metadata group applied something, opens
    /// the runtime groups this node is a voter of, and closes and removes
    /// deleted ones. Returns whether it closed a group.
    async fn refresh<R: Runtime, S: StateMachine>(
        &mut self,
        rt: &R,
        cfg: &NodeConfig,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
        factory: &Factory<S>,
        auth: &Auth<R>,
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
        let mut known = Known::configured(cfg);
        known.access = Access::build(&cfg.auth, &configured_ids(cfg), &state);
        known.applied = self.applied;
        known.removed = std::mem::take(&mut self.removed);
        known.forgotten = std::mem::take(&mut self.forgotten);
        known.deleted = state.deleted.keys().copied().collect();
        for (id, n) in &state.nodes {
            known.nodes.insert(*id, (n.peer.clone(), n.client.clone()));
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
                let raft = Config { seed: rt.next_u64(), pre_vote: cfg.pre_vote, ..Config::new(cfg.id, first) };
                let options =
                    ReplicaOptions { max_producers: cfg.max_producers, queue, retention: retention_of(cfg, g, queue) };
                let replica = Replica::open_with(rt.clone(), group_dir(g), raft, factory(g), cfg.log, options).await?;
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
    let raft = Config { seed: rt.next_u64(), pre_vote: cfg.pre_vote, ..Config::new(cfg.id, first) };
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

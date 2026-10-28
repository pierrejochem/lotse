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

use crate::auth::{Auth, AuthConfig};
use crate::client::{
    AdminView, ClientRequest, Clients, ClusterView, MarkerJob, NetStats, NodeView, QueueConfig, StreamConfig,
};
use crate::replica::{Error, ProposeError, Replica, ReplicaOptions, StateMachine};
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
}

struct Peer {
    queue: Mutex<VecDeque<Bytes>>,
    ready: Signal,
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
        let voters: Vec<NodeId> = cfg.peers.keys().copied().collect();
        let mut groups = BTreeMap::new();
        for &g in &cfg.groups {
            let raft = Config { seed: rt.next_u64(), pre_vote: cfg.pre_vote, ..Config::new(cfg.id, voters.clone()) };
            let options =
                ReplicaOptions { max_producers: cfg.max_producers, queue: cfg.queues.iter().any(|q| q.group == g) };
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
        let task = rt.clone().spawn(run(rt, cfg, groups, factory, listener, client_listener, shared));
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

async fn run<R: Runtime, S: StateMachine>(
    rt: R,
    cfg: NodeConfig,
    mut groups: BTreeMap<GroupId, Replica<R, S>>,
    factory: Factory<S>,
    listener: R::Listener,
    client_listener: Option<R::Listener>,
    shared: Arc<Shared>,
) -> Result<(), Error> {
    let auth = Auth::new(rt.clone(), cfg.auth.clone());
    let mut tasks = Vec::new();
    let others: BTreeSet<NodeId> = cfg.peers.keys().copied().filter(|id| *id != cfg.id).collect();
    tasks.push(rt.spawn(accept(rt.clone(), listener, shared.clone(), others)));
    if let Some(l) = client_listener {
        tasks.push(rt.spawn(accept_clients(rt.clone(), l, shared.clone(), auth.clone())));
    }
    let mut peers = BTreeMap::new();
    for (&id, addr) in cfg.peers.iter().filter(|(id, _)| **id != cfg.id) {
        let peer = Arc::new(Peer { queue: Mutex::new(VecDeque::new()), ready: Signal::default() });
        tasks.push(rt.spawn(send_to(
            rt.clone(),
            id,
            addr.clone(),
            peer.clone(),
            shared.net.clone(),
            shared.stop.clone(),
            cfg.tick,
        )));
        peers.insert(id, peer);
    }
    let jobs = Live::default();
    let around = Around { peers: &peers, shared: &shared, jobs: &jobs, auth: &auth };
    let result = main_loop(&rt, &cfg, &mut groups, &factory, around).await;
    // Whatever ended the loop, everything else ends with it.
    shared.stop.set();
    for p in peers.values() {
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

/// What the main loop works with besides its groups.
struct Around<'a, R> {
    peers: &'a BTreeMap<NodeId, Arc<Peer>>,
    shared: &'a Arc<Shared>,
    jobs: &'a Live,
    auth: &'a Auth<R>,
}

async fn main_loop<R: Runtime, S: StateMachine>(
    rt: &R,
    cfg: &NodeConfig,
    groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    factory: &Factory<S>,
    around: Around<'_, R>,
) -> Result<(), Error> {
    let Around { peers, shared, jobs, auth } = around;
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
    let nodes: Vec<NodeId> = cfg.peers.keys().copied().collect();
    let mut known = Known::configured(cfg);
    let started = rt.now();
    // The host is read every few seconds, not per request: CPU use is
    // measured between two reads.
    let (mut host, mut next_host) = (lotse_rt::HostStats::default(), started);
    while !shared.stop.is_set() {
        if known.refresh(rt, cfg, groups, factory, auth).await? {
            shared.status.lock().expect("status lock").retain(|g, _| groups.contains_key(g));
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
            groups.values_mut().for_each(|g| g.raft().tick());
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
                nodes: &nodes,
                configured_groups: cfg.groups.len(),
                voters: &known.voters,
                configured_users: &cfg.auth.users,
            },
            client_addrs: &cfg.client_addrs,
            coordinator: cfg.coordinator,
            node: NodeView { id: cfg.id, started, net: &shared.net, host },
            now: rt.now(),
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
                    group.raft().step(from, msg);
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
        for (to, messages) in outbound {
            let Some(peer) = peers.get(&to) else { continue };
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

/// Takes connections from the other nodes. When the network verifies peers,
/// a connection is only taken from a node of this cluster, and carries only
/// that node's frames: a certificate from the cluster's CA for some other
/// node id, or for no node at all, gets nowhere.
async fn accept<R: Runtime>(rt: R, listener: R::Listener, shared: Arc<Shared>, others: BTreeSet<NodeId>) {
    let mut readers = Vec::new();
    let verify = rt.verifies_peers();
    while let Some(Ok((conn, _peer))) = race(listener.accept(), &[&shared.stop]).await {
        let sender = match (verify, conn.peer_identity()) {
            (false, _) => None,
            (true, Some(PeerIdentity::Node(id))) if others.contains(&id) => Some(id),
            (true, _) => continue,
        };
        readers.push(rt.spawn(read_from(conn, shared.clone(), sender)));
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
async fn read_from<C: Conn>(conn: C, shared: Arc<Shared>, sender: Option<NodeId>) {
    while let Some(Ok(Some(frame))) = race(conn.recv(), &[&shared.stop]).await {
        NetStats::add(&shared.net.peer_in, frame.len());
        // A frame that does not decode means a broken or foreign peer: drop
        // the connection, the peer reconnects.
        let Ok(decoded) = wire::decode(frame) else { return };
        // A node speaks only for itself.
        if sender.is_some_and(|id| decoded.0 != id) {
            return;
        }
        shared.inbox.lock().expect("inbox lock").push_back(decoded);
        shared.work.set();
    }
}

/// Keeps a connection to node `to` and sends its frames. When the network
/// verifies peers, a connection whose certificate names another node is
/// dropped like a failed one: the address may now belong to someone else.
async fn send_to<R: Runtime>(
    rt: R,
    to: NodeId,
    addr: String,
    peer: Arc<Peer>,
    net: Arc<NetStats>,
    stop: Signal,
    tick: Duration,
) {
    let mut backoff = tick;
    let verify = rt.verifies_peers();
    while !stop.is_set() {
        let connected = race(rt.connect(&addr, Link::Node), &[&stop]).await.map(|r| {
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
                race(rt.sleep(backoff), &[&stop]).await;
                backoff = (backoff * 2).min(tick * 50);
                continue;
            }
            None => return,
        };
        backoff = tick;
        loop {
            let next = peer.queue.lock().expect("peer queue lock").pop_front();
            let Some(frame) = next else {
                if race(peer.ready.wait(), &[&stop]).await.is_none() {
                    return;
                }
                peer.ready.take();
                continue;
            };
            NetStats::add(&net.peer_out, frame.len());
            if race(conn.send(frame), &[&stop]).await.is_none_or(|r| r.is_err()) {
                break; // reconnect; the frame is lost, Raft resends
            }
        }
    }
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
    /// The metadata group's applied index these were built from.
    applied: u64,
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
            applied: 0,
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
        let mut known = Known::configured(cfg);
        known.applied = self.applied;
        known.removed = std::mem::take(&mut self.removed);
        known.deleted = state.deleted.keys().copied().collect();
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
            // Only voters replicate a group; the others learn its leader
            // from them.
            if voters.contains(&cfg.id) && !groups.contains_key(&g) {
                let raft =
                    Config { seed: rt.next_u64(), pre_vote: cfg.pre_vote, ..Config::new(cfg.id, voters.to_vec()) };
                let options = ReplicaOptions { max_producers: cfg.max_producers, queue };
                let replica = Replica::open_with(rt.clone(), group_dir(g), raft, factory(g), cfg.log, options).await?;
                groups.insert(g, replica);
            }
        }
        *self = known;
        Ok(closed)
    }
}

/// Where a group lives on the disk.
fn group_dir(g: GroupId) -> PathBuf {
    PathBuf::from("groups").join(g.to_string())
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

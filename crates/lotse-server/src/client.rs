//! Serving clients over the native protocol ([`lotse_proto`]).
//!
//! Connections are read and written by their own tasks; every decoded
//! request goes to the node's main loop, which owns the replicas. There:
//!
//! - `ApiVersions` and `Metadata` are answered at once.
//! - `Produce` proposes each batch to its partition's group and answers once
//!   every batch's outcome is known. A batch is written when its index is
//!   committed **on this node** and holds the term it was proposed in;
//!   committed entries never change, so that answer is final. A batch whose
//!   entry is replaced before it commits here is not reported as failed:
//!   another node may still hold it and commit it later, so the request runs
//!   into its timeout and the outcome is unknown (`REQUEST_TIMED_OUT`).
//! - `Fetch` reads from the leader's log and never returns a batch at or
//!   beyond the commit index (the high watermark), and with `read_committed`
//!   none at or beyond the last stable offset; it lists the aborted
//!   transactions in what it returns. Without enough data it waits until a
//!   commit brings more or `max_wait_ms` runs out.
//! - Transactions ([`coord`]): `InitProducer` with a transactional id,
//!   `AddPartitionsToTxn` and `EndTxn` change the coordinator group's state;
//!   the node then writes the markers with the internal `WriteTxnMarkers`,
//!   which a partition leader answers once the marker is committed (or is
//!   not needed).
//!
//! - `FindCoordinator` names the leader of the coordinator group, and
//!   `InitProducer` hands out producer ids there: it proposes an allocation
//!   entry and, once that is committed under the same rule as a produce,
//!   answers with its Raft index. Indices are never reused, so neither are
//!   ids.
//!
//! - Queues ([`queues`]): `Subscribe`, `Credit`, `Ack`, `Nack` and
//!   `Unsubscribe`; the queue's leader pushes deliveries to subscribers.
//!   Messages reach a queue by `Produce` to its partition 0.
//!
//! Until the metadata group exists, streams and queues and their groups come
//! from the node configuration ([`StreamConfig`], [`QueueConfig`]), and so
//! does the coordinator group.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    AbortedTxn, ApiKey, ApiVersionsResponse, BindingMetadata, Broker, ErrorCode, ExchangeMetadata,
    FetchPartitionResponse, FetchRequest, FetchResourceResponse, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, InitProducerResponse, LeaderHint, MarkerResult, MetadataRequest, MetadataResponse,
    PartitionMetadata, ProducePartitionResponse, ProduceRequest, ProduceResourceResponse, ProduceResponse, Request,
    RequestHeader, ResourceMetadata, Response, TxnPartitionResult, TxnResourceResult, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse, coordinator, isolation, kind,
};
use lotse_raft::{NodeId, RaftGroup, Role};
use lotse_record::Batch;
use lotse_route::Exchange;
use lotse_rt::{Conn, Instant, Runtime};

use crate::access::Access;
use crate::auth::{Auth, Principal};
use crate::replica::{Error, MarkerOutcome, ProposeError, Proposed, Replica, StateMachine};
use crate::signal::{Signal, race};
use crate::wire::GroupId;

mod admin;
mod coord;
mod groups;
mod nodes;
pub(crate) use nodes::{managed_groups, moving};
mod queues;
mod session;
mod stats;

pub(crate) use admin::AdminView;
pub(crate) use coord::MarkerJob;
pub use queues::QueueConfig;
use session::Session;
pub(crate) use stats::{NetStats, NodeView};

/// A stream and the Raft group of each of its partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConfig {
    /// Name clients look it up by.
    pub name: String,
    /// Id used by data-path requests.
    pub resource_id: u64,
    /// Partition `i` is replicated by group `partitions[i]`.
    pub partitions: Vec<GroupId>,
}

/// Responses and pushes waiting to be written to one connection.
#[derive(Default)]
pub(crate) struct Outbox {
    queue: Mutex<VecDeque<Bytes>>,
    ready: Signal,
    /// The connection is gone (its subscriptions end).
    closed: AtomicBool,
    /// The client's address, for metrics.
    peer: String,
}

impl Outbox {
    fn push(&self, correlation_id: u32, response: &Response) {
        self.push_frame(lotse_proto::encode_response(correlation_id, response));
    }

    fn push_frame(&self, frame: Bytes) {
        self.queue.lock().expect("outbox lock").push_back(frame);
        self.ready.set();
    }
}

/// A decoded request on its way to the main loop.
pub(crate) struct ClientRequest {
    outbox: Arc<Outbox>,
    header: RequestHeader,
    request: Request,
    /// Who the request acts as.
    principal: Principal,
}

/// Reads requests from one connection and writes its responses. Requests
/// reach the main loop only once the connection is authenticated (or needs
/// not be); the SASL requests are answered here.
pub(crate) async fn serve<R: Runtime>(
    rt: R,
    conn: R::Conn,
    peer: String,
    commands: Arc<dyn Fn(ClientRequest) + std::marker::Send + Sync>,
    net: Arc<NetStats>,
    stop: Signal,
    auth: Arc<Auth<R>>,
) {
    let mut session = Session::new(auth, conn.peer_identity());
    let conn = Arc::new(conn);
    let outbox = Arc::new(Outbox { peer, ..Outbox::default() });
    let closed = Signal::default();
    let writer = rt.spawn(write_to(conn.clone(), outbox.clone(), net.clone(), stop.clone(), closed.clone()));
    while let Some(Ok(Some(frame))) = race(conn.recv(), &[&stop]).await {
        NetStats::add(&net.client_in, frame.len());
        match lotse_proto::decode_request(frame) {
            Ok((header, Request::SaslHandshake(req))) => {
                outbox.push(header.correlation_id, &Response::SaslHandshake(session.handshake(&req)));
            }
            Ok((header, Request::SaslAuthenticate(req))) => {
                let Some(step) = race(session.authenticate(&rt, &req), &[&stop]).await else { break };
                outbox.push(header.correlation_id, &Response::SaslAuthenticate(step.response));
                if step.close {
                    break;
                }
            }
            Ok((header, request)) => match session.principal(rt.now()) {
                Some(p) => commands(ClientRequest { outbox: outbox.clone(), header, request, principal: p.clone() }),
                // Which versions a node speaks is no secret: a client asks
                // before it knows how to authenticate.
                None if matches!(request, Request::ApiVersions) => {
                    commands(ClientRequest { outbox: outbox.clone(), header, request, principal: Principal::Anonymous })
                }
                None => outbox.push(header.correlation_id, &Response::Error(ErrorCode::UNAUTHORIZED)),
            },
            // The client may retry with a version this node speaks.
            Err(lotse_proto::DecodeError::Unsupported(h)) => {
                outbox.push(h.correlation_id, &Response::Error(ErrorCode::UNSUPPORTED_VERSION))
            }
            // A frame that does not decode leaves nothing to answer: close.
            Err(_) => break,
        }
    }
    closed.set();
    outbox.closed.store(true, Ordering::Release);
    writer.await;
}

async fn write_to<C: Conn>(conn: Arc<C>, outbox: Arc<Outbox>, net: Arc<NetStats>, stop: Signal, closed: Signal) {
    loop {
        let next = outbox.queue.lock().expect("outbox lock").pop_front();
        match next {
            Some(frame) => {
                NetStats::add(&net.client_out, frame.len());
                if race(conn.send(frame), &[&stop]).await.is_none_or(|r| r.is_err()) {
                    return;
                }
            }
            // Once the reader is done, what is queued still goes out (the
            // answer to a failed login, say), then the writer ends.
            None if closed.is_set() || stop.is_set() => return,
            None => {
                race(outbox.ready.wait(), &[&stop, &closed]).await;
                outbox.ready.take();
            }
        }
    }
}

/// One batch of a `Produce` whose outcome is not known yet.
struct Waiting {
    resource: usize,
    partition: usize,
    group: GroupId,
    last: u64,
    term: u64,
    /// The batch's size and records, counted once it is acknowledged.
    bytes: u64,
    records: u64,
}

struct PendingProduce {
    outbox: Arc<Outbox>,
    correlation_id: u32,
    /// When it arrived, for the commit latency.
    arrived: Instant,
    deadline: Instant,
    response: ProduceResponse,
    waiting: Vec<Waiting>,
}

/// An `InitProducer` waiting for its allocation entry to commit.
struct PendingInit {
    outbox: Arc<Outbox>,
    correlation_id: u32,
    deadline: Instant,
    group: GroupId,
    last: u64,
    term: u64,
}

/// How long the coordinator waits for an allocation to commit.
const INIT_TIMEOUT: Duration = Duration::from_secs(5);

struct PendingFetch {
    outbox: Arc<Outbox>,
    correlation_id: u32,
    deadline: Instant,
    request: FetchRequest,
}

/// What the main loop knows about the cluster, for answering clients.
pub(crate) struct ClusterView<'a> {
    pub(crate) streams: &'a [StreamConfig],
    pub(crate) queues: &'a [QueueConfig],
    pub(crate) exchanges: &'a [Exchange],
    /// Resources deleted at runtime: gone for good.
    pub(crate) deleted: &'a BTreeSet<u64>,
    /// How long a group member may go without a heartbeat.
    pub(crate) group_session: Duration,
    /// How often group members send heartbeats.
    pub(crate) group_heartbeat: Duration,
    /// The metadata group and what admin requests need.
    pub(crate) admin: admin::AdminView<'a>,
    pub(crate) client_addrs: &'a BTreeMap<NodeId, String>,
    pub(crate) coordinator: Option<GroupId>,
    /// The node itself, for metrics.
    pub(crate) node: NodeView<'a>,
    pub(crate) now: Instant,
    /// Who may do what.
    pub(crate) access: &'a Access,
}

impl ClusterView<'_> {
    /// The stream or queue partition a group serves.
    fn resource_of(&self, g: GroupId) -> Option<(u64, u32)> {
        if let Some(q) = self.queues.iter().find(|q| q.group == g) {
            return Some((q.resource_id, 0));
        }
        self.streams.iter().find_map(|s| s.partitions.iter().position(|p| *p == g).map(|i| (s.resource_id, i as u32)))
    }

    /// The group of a stream's partition or a queue (partition 0), for
    /// writes.
    fn group_of(&self, resource_id: u64, partition: u32) -> Option<GroupId> {
        if let Some(q) = self.queues.iter().find(|q| q.resource_id == resource_id) {
            return (partition == 0).then_some(q.group);
        }
        self.stream_group_of(resource_id, partition)
    }

    /// The group of a stream's partition. Queues are not fetched: their
    /// logs hold acks too, and are read through subscriptions.
    fn stream_group_of(&self, resource_id: u64, partition: u32) -> Option<GroupId> {
        let s = self.streams.iter().find(|s| s.resource_id == resource_id)?;
        s.partitions.get(partition as usize).copied()
    }

    /// Why a partition cannot be served here: it does not exist (any
    /// more), or the client must look for its leader elsewhere: a group
    /// created at runtime runs on its voters only, and a resource created a
    /// moment ago may not have reached this node's metadata yet (runtime
    /// ids are never configured). A deleted id is gone for good: ids are
    /// never reused.
    fn absent(&self, resource_id: u64, group: Option<GroupId>) -> ErrorCode {
        if self.deleted.contains(&resource_id) {
            ErrorCode::UNKNOWN_RESOURCE
        } else if group.is_some() || resource_id >= crate::metadata::DYNAMIC_BASE {
            ErrorCode::NOT_LEADER
        } else {
            ErrorCode::UNKNOWN_RESOURCE
        }
    }
}

/// What a request waiting on `group` hears when the group was deleted.
const GONE: &str = "the resource was deleted";

/// What it hears when the group moved off this node (ADR 0048).
const MOVED: &str = "outcome unknown: the partition moved to other nodes";

fn hint(raft: &RaftGroup) -> Option<LeaderHint> {
    raft.leader().map(|l| LeaderHint { leader: l as u32, term: raft.term() })
}

/// Requests in flight.
#[derive(Default)]
pub(crate) struct Clients {
    produces: Vec<PendingProduce>,
    inits: Vec<PendingInit>,
    fetches: Vec<PendingFetch>,
    markers: Vec<PendingMarkers>,
    /// Coordinator records in flight, and the transactional ids they change.
    writes: Vec<coord::PendingWrite>,
    busy: std::collections::BTreeSet<coord::Busy>,
    /// Marker jobs running for this coordinator term, per transactional id.
    jobs: BTreeMap<String, (u64, Signal)>,
    /// When this leader first saw each transaction ongoing, for timeouts.
    ongoing_since: BTreeMap<String, Instant>,
    /// Sum of the groups' commit indices when pending fetches were last
    /// tried: they are tried again only after a commit or at their deadline.
    commits_seen: u64,
    /// Queues this node leads and serves, per group.
    queues: BTreeMap<GroupId, queues::Serving>,
    /// The queue group of every subscription here.
    sub_groups: BTreeMap<u32, GroupId>,
    next_subscription: u32,
    /// Acks and delivery counts in flight.
    queue_writes: Vec<queues::QueueWrite>,
    /// The group coordinator's clocks.
    group_timers: groups::GroupTimers,
    /// Admin records in flight.
    admin_writes: Vec<admin::AdminWrite>,
    /// Counted for `DescribeMetrics`.
    stats: stats::Stats,
}

impl Clients {
    /// Serves a request, or keeps it until it can be answered.
    pub(crate) async fn handle<R: Runtime, S: StateMachine>(
        &mut self,
        req: ClientRequest,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) -> Result<(), Error> {
        let ClientRequest { outbox, header, request, principal } = req;
        let id = header.correlation_id;
        if let Err(why) = view.access.check(&principal, &request) {
            outbox.push(id, &Response::Refused { code: ErrorCode::UNAUTHORIZED, message: why });
            return Ok(());
        }
        match request {
            Request::ApiVersions => outbox.push(
                id,
                &Response::ApiVersions(ApiVersionsResponse {
                    versions: ApiKey::SUPPORTED.to_vec(),
                    features: vec!["streams".into(), "queues".into(), "groups".into()],
                }),
            ),
            Request::Metadata(m) => outbox.push(id, &Response::Metadata(metadata(&m, view, groups, &principal))),
            Request::Produce(p) => self.produce(outbox, id, p, view, groups),
            Request::FindCoordinator(f) => {
                outbox.push(id, &Response::FindCoordinator(find_coordinator(&f, view, groups)))
            }
            Request::InitProducer(req) => match req.txn_id.clone() {
                Some(txn_id) => self.init_transactional(outbox, id, req, txn_id, view, groups),
                None => self.init_producer(outbox, id, view, groups),
            },
            Request::AddPartitionsToTxn(req) => self.add_partitions(outbox, id, req, view, groups),
            Request::EndTxn(req) => self.end_txn(outbox, id, req, view, groups),
            Request::WriteTxnMarkers(req) => self.write_markers(outbox, id, req, view, groups),
            Request::Subscribe(req) => self.subscribe(outbox, id, req, view, groups),
            Request::Credit(req) => self.credit(&outbox, req),
            Request::Ack(req) => self.ack(outbox, id, req, view, groups),
            Request::Nack(req) => self.nack(outbox, id, req, view, groups),
            Request::Unsubscribe(req) => self.unsubscribe(outbox, id, req),
            Request::GroupHeartbeat(req) => self.group_heartbeat(outbox, id, req, view, groups),
            Request::CommitOffsets(req) => self.commit_offsets(outbox, id, req, view, groups),
            Request::OffsetFetch(req) => self.offset_fetch(outbox, id, req, view, groups),
            Request::AddOffsetsToTxn(req) => self.add_offsets_to_txn(outbox, id, req, view, groups),
            Request::CreateStream(req) => self.create_stream(outbox, id, req, view, groups),
            Request::CreateQueue(req) => self.create_queue(outbox, id, req, view, groups),
            Request::DeclareExchange(req) => self.declare_exchange(outbox, id, req, view, groups),
            Request::Bind(req) => self.bind(outbox, id, req, false, view, groups),
            Request::Unbind(req) => self.bind(outbox, id, req, true, view, groups),
            Request::DeleteResource(req) => self.delete_resource(outbox, id, req, view, groups),
            Request::AlterConfigs(req) => self.alter_configs(outbox, id, req, view, groups),
            Request::DescribeConfigs(req) => self.describe_configs(outbox, id, req, view, groups),
            Request::DescribeMetrics(_) => self.describe_metrics(&outbox, id, view, groups, &principal),
            Request::AlterUser(req) => self.alter_user(outbox, id, req, view, groups),
            Request::DescribeUsers(_) => self.describe_users(outbox, id, view, groups),
            Request::CreateTenant(req) => self.create_tenant(outbox, id, req, view, groups),
            Request::DeleteTenant(req) => self.delete_tenant(outbox, id, req, view, groups),
            Request::DescribeTenants(_) => self.describe_tenants(&outbox, id, view, &principal),
            Request::AlterRoleBindings(req) => self.alter_role_bindings(outbox, id, req, view, groups),
            Request::DescribeRoleBindings(_) => self.describe_role_bindings(&outbox, id, view),
            Request::WhoAmI(_) => self.who_am_i(&outbox, id, view, &principal),
            Request::AddNode(req) => self.add_node(outbox, id, req, view, groups),
            Request::RemoveNode(req) => self.remove_node(outbox, id, req, view, groups),
            Request::DescribeNodes(_) => self.describe_nodes(&outbox, id, view, groups),
            Request::SettleVoters(req) => self.settle_voters(outbox, id, req, view, groups),
            // Answered by the connection's own task, never forwarded.
            Request::SaslHandshake(_) | Request::SaslAuthenticate(_) => {}
            Request::Fetch(f) => {
                let deadline = view.now + Duration::from_millis(u64::from(f.max_wait_ms));
                let pending = PendingFetch { outbox, correlation_id: id, deadline, request: f };
                if let Some(p) = try_fetch(pending, view, groups, false, &mut self.stats).await? {
                    self.fetches.push(p);
                }
            }
        }
        Ok(())
    }

    fn produce<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: ProduceRequest,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) {
        let mut response = ProduceResponse { throttle_ms: 0, resources: Vec::new() };
        let mut waiting = Vec::new();
        for (ri, res) in req.resources.into_iter().enumerate() {
            let mut partitions = Vec::new();
            for (pi, p) in res.partitions.into_iter().enumerate() {
                let mut out = ProducePartitionResponse {
                    partition: p.partition,
                    error: ErrorCode::NONE,
                    base_offset: 0,
                    append_ts: -1,
                    error_msg: None,
                    leader_hint: None,
                };
                let group = view.group_of(res.resource_id, p.partition);
                match group.and_then(|g| groups.get_mut(&g).map(|r| (g, r))) {
                    None => out.error = view.absent(res.resource_id, group),
                    Some(_) if req.txn_id.is_some() != transactional(&p.batch) => {
                        out.error = ErrorCode::INVALID_TXN_STATE;
                        out.error_msg = Some("transactional batches go with a transactional id, and only they".into());
                    }
                    Some((g, replica)) => match replica.propose(p.batch.clone()) {
                        Ok(Proposed { first, last, term, .. }) => {
                            out.base_offset = first - 1;
                            // A duplicate of an applied batch is written; anything
                            // else (new, or the original still in flight) is
                            // answered once its entry's commit decides.
                            if let Some(term) = term {
                                let bytes = p.batch.len() as u64;
                                let records = Batch::parse(&p.batch).map_or(0, |(b, _)| b.record_count() as u64);
                                waiting.push(Waiting {
                                    resource: ri,
                                    partition: pi,
                                    group: g,
                                    last,
                                    term,
                                    bytes,
                                    records,
                                });
                            }
                        }
                        Err(ProposeError::NotLeader(_)) => {
                            out.error = ErrorCode::NOT_LEADER;
                            out.leader_hint = hint(replica.raft_ref());
                        }
                        Err(e @ ProposeError::OutOfOrderSequence { .. }) => {
                            out.error = ErrorCode::OUT_OF_ORDER_SEQUENCE;
                            out.error_msg = Some(e.to_string());
                        }
                        Err(e @ ProposeError::ProducerFenced { .. }) => {
                            out.error = ErrorCode::PRODUCER_FENCED;
                            out.error_msg = Some(e.to_string());
                        }
                        Err(e @ ProposeError::InvalidTxnState(_)) => {
                            out.error = ErrorCode::INVALID_TXN_STATE;
                            out.error_msg = Some(e.to_string());
                        }
                        Err(e) => {
                            out.error = ErrorCode::CORRUPT_BATCH;
                            out.error_msg = Some(e.to_string());
                        }
                    },
                }
                partitions.push(out);
            }
            response.resources.push(ProduceResourceResponse { resource_id: res.resource_id, partitions });
        }
        let deadline = view.now + Duration::from_millis(u64::from(req.timeout_ms));
        let pending = PendingProduce { outbox, correlation_id, arrived: view.now, deadline, response, waiting };
        if let Some(p) = settle(pending, view, groups, &mut self.stats) {
            self.produces.push(p);
        }
    }

    fn init_producer<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) {
        let fail = |error, leader_hint| {
            Response::InitProducer(InitProducerResponse { error, producer_id: 0, producer_epoch: 0, leader_hint })
        };
        let Some((group, replica)) = view.coordinator.and_then(|g| groups.get_mut(&g).map(|r| (g, r))) else {
            return outbox.push(correlation_id, &fail(ErrorCode::COORDINATOR_NOT_AVAILABLE, None));
        };
        let batch =
            lotse_record::internal_control_batch(lotse_record::InternalControl::ProducerId, &[]).expect("one record");
        if replica.raft_ref().role() != Role::Leader {
            return outbox.push(correlation_id, &fail(ErrorCode::NOT_LEADER, hint(replica.raft_ref())));
        }
        match replica.propose_internal(batch) {
            Ok(Proposed { last, term: Some(term), .. }) => {
                let deadline = view.now + INIT_TIMEOUT;
                self.inits.push(PendingInit { outbox, correlation_id, deadline, group, last, term });
            }
            Ok(_) => unreachable!("an allocation is not idempotent, so never a duplicate"),
            Err(ProposeError::NotLeader(_)) => {
                outbox.push(correlation_id, &fail(ErrorCode::NOT_LEADER, hint(replica.raft_ref())))
            }
            Err(_) => outbox.push(correlation_id, &fail(ErrorCode::COORDINATOR_NOT_AVAILABLE, None)),
        }
    }

    fn write_markers<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: WriteTxnMarkersRequest,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) {
        let mut response = WriteTxnMarkersResponse { markers: Vec::new() };
        let mut waiting = Vec::new();
        for (mi, m) in req.markers.iter().enumerate() {
            let mut resources = Vec::new();
            for (ri, r) in m.resources.iter().enumerate() {
                let mut partitions = Vec::new();
                for (pi, p) in r.partitions.iter().enumerate() {
                    let group = view.group_of(r.resource_id, *p);
                    let error = match group.and_then(|g| groups.get_mut(&g).map(|x| (g, x))) {
                        None => view.absent(r.resource_id, group),
                        Some((g, replica)) => {
                            match replica.propose_marker(m.producer_id, m.producer_epoch, m.commit, m.coordinator_epoch)
                            {
                                Ok(MarkerOutcome::Proposed { last, term }) => {
                                    waiting.push((mi, ri, pi, g, last, term));
                                    ErrorCode::NONE
                                }
                                Ok(MarkerOutcome::Done) => ErrorCode::NONE,
                                Ok(MarkerOutcome::StaleCoordinator) => ErrorCode::COORDINATOR_FENCED,
                                Err(ProposeError::NotLeader(_)) => ErrorCode::NOT_LEADER,
                                Err(_) => ErrorCode::CORRUPT_BATCH,
                            }
                        }
                    };
                    partitions.push(TxnPartitionResult { partition: *p, error });
                }
                resources.push(TxnResourceResult { resource_id: r.resource_id, partitions });
            }
            response.markers.push(MarkerResult { producer_id: m.producer_id, resources });
        }
        let deadline = view.now + MARKER_TIMEOUT;
        let pending = PendingMarkers { outbox, correlation_id, deadline, response, waiting };
        if let Some(p) = settle_markers(pending, view.now, groups) {
            self.markers.push(p);
        }
    }

    /// Answers whatever the last round decided, and requests that ran out
    /// of time; returns marker jobs for the node to start.
    pub(crate) async fn after_round<R: Runtime, S: StateMachine>(
        &mut self,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) -> Result<Vec<MarkerJob>, Error> {
        let produces = std::mem::take(&mut self.produces);
        self.produces = produces.into_iter().filter_map(|p| settle(p, view, groups, &mut self.stats)).collect();
        let inits = std::mem::take(&mut self.inits);
        self.inits = inits.into_iter().filter_map(|p| settle_init(p, view.now, groups)).collect();
        let markers = std::mem::take(&mut self.markers);
        self.markers = markers.into_iter().filter_map(|p| settle_markers(p, view.now, groups)).collect();
        let jobs = self.coordinate(view, groups);
        self.expire_members(view, groups);
        self.settle_admin(view, groups);
        self.settle_queue_writes(view.now, groups);
        self.serve_queues(view, groups).await?;

        // Commits bring new data, applying moves the last stable offset.
        let commits: u64 = groups.values().map(|g| g.raft_ref().commit() + g.applied()).sum();
        let committed = std::mem::replace(&mut self.commits_seen, commits) != commits;
        let fetches = std::mem::take(&mut self.fetches);
        for f in fetches {
            if committed || view.now >= f.deadline {
                let final_try = view.now >= f.deadline;
                if let Some(f) = try_fetch(f, view, groups, final_try, &mut self.stats).await? {
                    self.fetches.push(f);
                }
            } else {
                self.fetches.push(f);
            }
        }
        Ok(jobs)
    }
}

/// How long a partition leader waits for a marker to commit.
const MARKER_TIMEOUT: Duration = Duration::from_secs(5);

/// A `WriteTxnMarkers` waiting for its markers to commit.
struct PendingMarkers {
    outbox: Arc<Outbox>,
    correlation_id: u32,
    deadline: Instant,
    response: WriteTxnMarkersResponse,
    /// `(marker, resource, partition, group, last, term)`.
    waiting: Vec<(usize, usize, usize, GroupId, u64, u64)>,
}

fn settle_markers<R: Runtime, S: StateMachine>(
    mut p: PendingMarkers,
    now: Instant,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
) -> Option<PendingMarkers> {
    let timed_out = now >= p.deadline;
    p.waiting.retain(|&(mi, ri, pi, g, last, term)| {
        let Some(replica) = groups.get(&g) else {
            p.response.markers[mi].resources[ri].partitions[pi].error = ErrorCode::UNKNOWN_RESOURCE;
            return false;
        };
        let error = match outcome(replica.raft_ref(), last, term) {
            Some(code) => code,
            None if timed_out => ErrorCode::REQUEST_TIMED_OUT,
            None => return true,
        };
        p.response.markers[mi].resources[ri].partitions[pi].error = error;
        false
    });
    if p.waiting.is_empty() {
        p.outbox.push(p.correlation_id, &Response::WriteTxnMarkers(p.response));
        return None;
    }
    Some(p)
}

/// The fate of the entry proposed at `last` in `term`, once this node knows
/// it: `NONE` (written), `NOT_LEADER` (replaced, not written), or
/// `REQUEST_TIMED_OUT` (unknown: compacted). `None` while undecided here.
fn outcome(raft: &RaftGroup, last: u64, term: u64) -> Option<ErrorCode> {
    if raft.commit() < last {
        return None;
    }
    Some(match raft.log().term_at(last) {
        Some(t) if t == term => ErrorCode::NONE,
        Some(_) => ErrorCode::NOT_LEADER,
        None => ErrorCode::REQUEST_TIMED_OUT,
    })
}

/// Whether a produced batch is marked transactional (an unparsable batch is
/// refused later anyway).
fn transactional(batch: &Bytes) -> bool {
    Batch::parse(batch).is_ok_and(|(b, _)| b.attributes().transactional)
}

/// Decides what can be decided; answers and returns `None` once nothing is
/// left to wait for.
fn settle<R: Runtime, S: StateMachine>(
    mut p: PendingProduce,
    view: &ClusterView<'_>,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    stats: &mut stats::Stats,
) -> Option<PendingProduce> {
    let now = view.now;
    let timed_out = now >= p.deadline;
    p.waiting.retain(|w| {
        let resource_id = p.response.resources[w.resource].resource_id;
        let out = &mut p.response.resources[w.resource].partitions[w.partition];
        let Some(replica) = groups.get(&w.group) else {
            if view.deleted.contains(&resource_id) {
                out.error = ErrorCode::UNKNOWN_RESOURCE;
                out.error_msg = Some(GONE.into());
            } else {
                // Moved to other nodes while the batch waited (ADR 0048):
                // it may have committed there.
                out.error = ErrorCode::REQUEST_TIMED_OUT;
                out.error_msg = Some(MOVED.into());
            }
            return false;
        };
        let raft = replica.raft_ref();
        if raft.commit() >= w.last {
            // Committed here, so final: the entry at `last` is ours exactly
            // if it carries the term we proposed in.
            match raft.log().term_at(w.last) {
                Some(t) if t == w.term => stats.produced(w.group, w.bytes, w.records, now - p.arrived),
                Some(_) => {
                    out.error = ErrorCode::NOT_LEADER;
                    out.error_msg = Some("leadership changed before the batch was committed".into());
                    out.leader_hint = hint(raft);
                }
                None => {
                    out.error = ErrorCode::REQUEST_TIMED_OUT;
                    out.error_msg = Some("outcome unknown: the log was compacted".into());
                }
            }
            return false;
        }
        if timed_out {
            out.error = ErrorCode::REQUEST_TIMED_OUT;
            out.error_msg = Some("no quorum confirmation in time; the batch may or may not be written".into());
            return false;
        }
        true
    });
    if p.waiting.is_empty() {
        p.outbox.push(p.correlation_id, &Response::Produce(p.response));
        return None;
    }
    Some(p)
}

/// Answers an `InitProducer` once its allocation entry is committed here.
fn settle_init<R: Runtime, S: StateMachine>(
    p: PendingInit,
    now: Instant,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
) -> Option<PendingInit> {
    let raft = groups[&p.group].raft_ref();
    let (error, producer_id, leader_hint) = if raft.commit() >= p.last {
        match raft.log().term_at(p.last) {
            // The id is the index of the committed allocation: unique.
            Some(t) if t == p.term => (ErrorCode::NONE, p.last, None),
            Some(_) => (ErrorCode::NOT_LEADER, 0, hint(raft)),
            None => (ErrorCode::REQUEST_TIMED_OUT, 0, None),
        }
    } else if now >= p.deadline {
        (ErrorCode::REQUEST_TIMED_OUT, 0, None)
    } else {
        return Some(p);
    };
    let resp = InitProducerResponse { error, producer_id, producer_epoch: 0, leader_hint };
    p.outbox.push(p.correlation_id, &Response::InitProducer(resp));
    None
}

/// Reads what the fetch asks for; answers and returns `None` if there is
/// enough data, an error, or `final_try`.
async fn try_fetch<R: Runtime, S: StateMachine>(
    p: PendingFetch,
    view: &ClusterView<'_>,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    final_try: bool,
    stats: &mut stats::Stats,
) -> Result<Option<PendingFetch>, Error> {
    let req = &p.request;
    // Bytes per group, counted only if this try answers.
    let mut served: Vec<(GroupId, usize)> = Vec::new();
    let mut budget = req.max_bytes as usize;
    let mut total = 0usize;
    let mut any_error = false;
    let mut response = FetchResponse { throttle_ms: 0, resources: Vec::new() };
    for res in &req.resources {
        let mut partitions = Vec::new();
        for fp in &res.partitions {
            let mut out = FetchPartitionResponse {
                partition: fp.partition,
                error: ErrorCode::NONE,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_txns: Vec::new(),
                records: Bytes::new(),
                leader_hint: None,
            };
            let group = view.stream_group_of(res.resource_id, fp.partition);
            match group.and_then(|g| groups.get(&g)) {
                None => out.error = view.absent(res.resource_id, group),
                Some(replica) => {
                    let raft = replica.raft_ref();
                    // Offsets below the commit index are committed (index = offset + 1).
                    let hw = raft.commit();
                    let start = replica.log().start_offset();
                    if raft.role() != Role::Leader {
                        out.error = ErrorCode::NOT_LEADER;
                        out.leader_hint = hint(raft);
                    } else if fp.leader_term != 0 && fp.leader_term != raft.term() {
                        out.error = ErrorCode::FENCED_LEADER_TERM;
                        out.leader_hint = hint(raft);
                    } else if fp.fetch_offset < start || fp.fetch_offset > raft.last_index() {
                        // Not `> hw`: a new leader learns the commit index only once
                        // an entry of its own term commits, so for a moment its high
                        // watermark lags offsets a client already read. Those are in
                        // its log (they are committed); the client waits for them.
                        out.error = ErrorCode::OFFSET_OUT_OF_RANGE;
                    } else {
                        // Below the last stable offset every transaction is decided.
                        let lso = replica.txns().last_stable_offset(replica.applied()).min(hw);
                        let limit = if req.isolation == isolation::COMMITTED { lso } else { hw };
                        if fp.fetch_offset < limit && budget > 0 {
                            let max = (fp.max_bytes as usize).min(budget);
                            let read = replica.log().read(fp.fetch_offset, max).await?;
                            out.records = committed_prefix(read, limit)?;
                            served.extend(group.map(|g| (g, out.records.len())));
                            let end = fp.fetch_offset.max(next_offset_of(&out.records));
                            out.aborted_txns = replica
                                .txns()
                                .aborted_between(fp.fetch_offset, end)
                                .into_iter()
                                .map(|(producer_id, first_offset)| AbortedTxn { producer_id, first_offset })
                                .collect();
                        }
                        out.last_stable_offset = lso;
                    }
                    (out.high_watermark, out.log_start_offset) = (hw, start);
                }
            }
            any_error |= out.error != ErrorCode::NONE;
            budget = budget.saturating_sub(out.records.len());
            total += out.records.len();
            partitions.push(out);
        }
        response.resources.push(FetchResourceResponse { resource_id: res.resource_id, partitions });
    }
    if total >= (req.min_bytes as usize).max(1) || any_error || final_try || req.max_wait_ms == 0 {
        for (g, n) in served {
            stats.group(g).fetch_bytes += n as u64;
        }
        p.outbox.push(p.correlation_id, &Response::Fetch(response));
        return Ok(None);
    }
    Ok(Some(p))
}

/// The offset after the last batch in `records`, or 0 if there is none.
fn next_offset_of(records: &Bytes) -> u64 {
    let mut rest = records.clone();
    let mut next = 0;
    while let Ok((b, r)) = Batch::parse(&rest) {
        next = b.next_offset();
        rest = r;
        if rest.is_empty() {
            break;
        }
    }
    next
}

/// The whole batches of `read` that end below `hw`: nothing uncommitted
/// leaves the node.
fn committed_prefix(read: Bytes, hw: u64) -> Result<Bytes, Error> {
    let mut rest = read.clone();
    let mut len = 0;
    while !rest.is_empty() {
        let (batch, next) = Batch::parse(&rest)?;
        if batch.next_offset() > hw {
            break;
        }
        len += batch.len();
        rest = next;
    }
    Ok(read.slice(..len))
}

fn find_coordinator<R: Runtime, S: StateMachine>(
    req: &FindCoordinatorRequest,
    view: &ClusterView<'_>,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
) -> FindCoordinatorResponse {
    let unavailable = |why: &str| FindCoordinatorResponse {
        error: ErrorCode::COORDINATOR_NOT_AVAILABLE,
        node_id: 0,
        host: String::new(),
        port: 0,
        error_msg: Some(why.into()),
    };
    if req.key_type != coordinator::PRODUCER && req.key_type != coordinator::GROUP {
        return unavailable("unknown coordinator type");
    }
    let Some(g) = view.coordinator else { return unavailable("no coordinator group is configured") };
    let leader = groups.get(&g).and_then(|r| r.raft_ref().leader());
    let addr = leader.and_then(|l| view.client_addrs.get(&l).map(|a| (l, a)));
    match addr.and_then(|(l, a)| a.rsplit_once(':').map(|(h, p)| (l, h, p))) {
        Some((leader, host, port)) => FindCoordinatorResponse {
            error: ErrorCode::NONE,
            node_id: leader as u32,
            host: host.into(),
            port: port.parse().unwrap_or(0),
            error_msg: None,
        },
        None => unavailable("the coordinator group has no leader right now"),
    }
}

/// The brokers and the resources asked for, as far as `who` may see them:
/// a resource it may not see is unknown to it, and left out of a listing.
fn metadata<R: Runtime, S: StateMachine>(
    req: &MetadataRequest,
    view: &ClusterView<'_>,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
    who: &Principal,
) -> MetadataResponse {
    let brokers = view
        .client_addrs
        .iter()
        .filter_map(|(id, addr)| {
            let (host, port) = addr.rsplit_once(':')?;
            Some(Broker { node_id: *id as u32, host: host.into(), port: port.parse().ok()?, rack: String::new() })
        })
        .collect();
    let tenant_of = |id: u64| view.access.resources.get(&id).map(|(_, t)| t.clone()).unwrap_or_default();
    let describe = |s: &StreamConfig| ResourceMetadata {
        error: ErrorCode::NONE,
        name: s.name.clone(),
        tenant: tenant_of(s.resource_id),
        resource_id: s.resource_id,
        kind: kind::STREAM,
        partitions: s
            .partitions
            .iter()
            .enumerate()
            .map(|(i, g)| {
                let (leader, leader_term, voters) = match groups.get(g) {
                    Some(r) => {
                        let raft = r.raft_ref();
                        let voters = raft.membership().voters.iter().map(|v| *v as u32).collect();
                        (raft.leader().unwrap_or(0) as u32, raft.term(), voters)
                    }
                    // Not hosted here: its voters know the leader.
                    None => {
                        let voters = view.admin.voters.get(g).map(|v| v.iter().map(|n| *n as u32).collect());
                        (0, 0, voters.unwrap_or_default())
                    }
                };
                PartitionMetadata { partition: i as u32, leader, leader_term, voters }
            })
            .collect(),
        exchange: None,
    };
    let describe_queue = |q: &QueueConfig| {
        let as_stream = StreamConfig { name: q.name.clone(), resource_id: q.resource_id, partitions: vec![q.group] };
        ResourceMetadata { kind: kind::QUEUE, ..describe(&as_stream) }
    };
    let describe_exchange = |x: &Exchange| ResourceMetadata {
        error: ErrorCode::NONE,
        name: x.name.clone(),
        tenant: tenant_of(x.resource_id),
        resource_id: x.resource_id,
        kind: kind::EXCHANGE,
        partitions: Vec::new(),
        exchange: Some(ExchangeMetadata {
            exchange_type: x.kind.code(),
            bindings: x
                .bindings
                .iter()
                .map(|b| BindingMetadata {
                    queue_id: b.queue_id,
                    binding_key: b.key.clone(),
                    header_match: b.headers.clone(),
                    match_all: b.match_all,
                })
                .collect(),
        }),
    };
    let hidden = |r: &ResourceMetadata| {
        r.error == ErrorCode::NONE && !view.access.may(who, lotse_auth::roles::Operation::Describe, &r.tenant, &r.name)
    };
    let resources: Vec<ResourceMetadata> = if req.names.is_empty() {
        view.streams
            .iter()
            .map(describe)
            .chain(view.queues.iter().map(describe_queue))
            .chain(view.exchanges.iter().map(describe_exchange))
            .filter(|r| !hidden(r))
            .collect()
    } else {
        // An exchange comes with the queues bound to it: a client needs
        // their leaders to publish.
        let mut names: Vec<String> = req.names.clone();
        for x in view.exchanges.iter().filter(|x| req.names.contains(&x.name)) {
            for b in &x.bindings {
                if let Some(q) = view.queues.iter().find(|q| q.resource_id == b.queue_id)
                    && !names.contains(&q.name)
                {
                    names.push(q.name.clone());
                }
            }
        }
        names
            .iter()
            .map(|n| match view.streams.iter().find(|s| &s.name == n) {
                Some(s) => describe(s),
                None if let Some(q) = view.queues.iter().find(|q| &q.name == n) => describe_queue(q),
                None if let Some(x) = view.exchanges.iter().find(|x| &x.name == n) => describe_exchange(x),
                None => ResourceMetadata {
                    error: ErrorCode::UNKNOWN_RESOURCE,
                    name: n.clone(),
                    tenant: String::new(),
                    resource_id: 0,
                    kind: kind::STREAM,
                    partitions: Vec::new(),
                    exchange: None,
                },
            })
            .map(|r| {
                if hidden(&r) {
                    let unknown = ErrorCode::UNKNOWN_RESOURCE;
                    ResourceMetadata {
                        error: unknown,
                        tenant: String::new(),
                        resource_id: 0,
                        partitions: Vec::new(),
                        exchange: None,
                        ..r
                    }
                } else {
                    r
                }
            })
            .collect()
    };
    MetadataResponse { brokers, resources }
}

#[cfg(test)]
mod tests;

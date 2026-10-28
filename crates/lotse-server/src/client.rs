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
//!   beyond the commit index (the high watermark). Without enough data it
//!   waits until a commit brings more or `max_wait_ms` runs out.
//!
//! - `FindCoordinator` names the leader of the coordinator group, and
//!   `InitProducer` hands out producer ids there: it proposes an allocation
//!   entry and, once that is committed under the same rule as a produce,
//!   answers with its Raft index. Indices are never reused, so neither are
//!   ids.
//!
//! Until the metadata group exists, streams and their partitions' groups
//! come from the node configuration ([`StreamConfig`]), and so does the
//! coordinator group.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    ApiKey, ApiVersionsResponse, Broker, ErrorCode, FetchPartitionResponse, FetchRequest, FetchResourceResponse,
    FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse, InitProducerRequest, InitProducerResponse,
    LeaderHint, MetadataRequest, MetadataResponse, PartitionMetadata, ProducePartitionResponse, ProduceRequest,
    ProduceResourceResponse, ProduceResponse, Request, RequestHeader, ResourceMetadata, Response, coordinator, kind,
};
use lotse_raft::{NodeId, RaftGroup, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Conn, Instant, Runtime};

use crate::replica::{Error, ProposeError, Proposed, Replica, StateMachine};
use crate::signal::{Signal, race};
use crate::wire::GroupId;

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

/// Responses waiting to be written to one connection.
#[derive(Default)]
pub(crate) struct Outbox {
    queue: Mutex<VecDeque<Bytes>>,
    ready: Signal,
}

impl Outbox {
    fn push(&self, correlation_id: u32, response: &Response) {
        let frame = lotse_proto::encode_response(correlation_id, response);
        self.queue.lock().expect("outbox lock").push_back(frame);
        self.ready.set();
    }
}

/// A decoded request on its way to the main loop.
pub(crate) struct ClientRequest {
    outbox: Arc<Outbox>,
    header: RequestHeader,
    request: Request,
}

/// Reads requests from one connection and writes its responses.
pub(crate) async fn serve<R: Runtime>(
    rt: R,
    conn: R::Conn,
    commands: Arc<dyn Fn(ClientRequest) + std::marker::Send + Sync>,
    stop: Signal,
) {
    let conn = Arc::new(conn);
    let outbox = Arc::new(Outbox::default());
    let closed = Signal::default();
    let writer = rt.spawn(write_to(conn.clone(), outbox.clone(), stop.clone(), closed.clone()));
    while let Some(Ok(Some(frame))) = race(conn.recv(), &[&stop]).await {
        match lotse_proto::decode_request(frame) {
            Ok((header, request)) => commands(ClientRequest { outbox: outbox.clone(), header, request }),
            // The client may retry with a version this node speaks.
            Err(lotse_proto::DecodeError::Unsupported(h)) => {
                outbox.push(h.correlation_id, &Response::Error(ErrorCode::UNSUPPORTED_VERSION))
            }
            // A frame that does not decode leaves nothing to answer: close.
            Err(_) => break,
        }
    }
    closed.set();
    writer.await;
}

async fn write_to<C: Conn>(conn: Arc<C>, outbox: Arc<Outbox>, stop: Signal, closed: Signal) {
    loop {
        let next = outbox.queue.lock().expect("outbox lock").pop_front();
        match next {
            Some(frame) => {
                if race(conn.send(frame), &[&stop]).await.is_none_or(|r| r.is_err()) {
                    return;
                }
            }
            None => {
                if race(outbox.ready.wait(), &[&stop, &closed]).await.is_none() {
                    return;
                }
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
}

struct PendingProduce {
    outbox: Arc<Outbox>,
    correlation_id: u32,
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
    pub(crate) client_addrs: &'a BTreeMap<NodeId, String>,
    pub(crate) coordinator: Option<GroupId>,
    pub(crate) now: Instant,
}

impl ClusterView<'_> {
    fn group_of(&self, resource_id: u64, partition: u32) -> Option<GroupId> {
        let s = self.streams.iter().find(|s| s.resource_id == resource_id)?;
        s.partitions.get(partition as usize).copied()
    }
}

fn hint(raft: &RaftGroup) -> Option<LeaderHint> {
    raft.leader().map(|l| LeaderHint { leader: l as u32, term: raft.term() })
}

/// Requests in flight.
#[derive(Default)]
pub(crate) struct Clients {
    produces: Vec<PendingProduce>,
    inits: Vec<PendingInit>,
    fetches: Vec<PendingFetch>,
    /// Sum of the groups' commit indices when pending fetches were last
    /// tried: they are tried again only after a commit or at their deadline.
    commits_seen: u64,
}

impl Clients {
    /// Serves a request, or keeps it until it can be answered.
    pub(crate) async fn handle<R: Runtime, S: StateMachine>(
        &mut self,
        req: ClientRequest,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) -> Result<(), Error> {
        let ClientRequest { outbox, header, request } = req;
        let id = header.correlation_id;
        match request {
            Request::ApiVersions => outbox.push(
                id,
                &Response::ApiVersions(ApiVersionsResponse {
                    versions: ApiKey::SUPPORTED.to_vec(),
                    features: vec!["streams".into()],
                }),
            ),
            Request::Metadata(m) => outbox.push(id, &Response::Metadata(metadata(&m, view, groups))),
            Request::Produce(p) => self.produce(outbox, id, p, view, groups),
            Request::FindCoordinator(f) => {
                outbox.push(id, &Response::FindCoordinator(find_coordinator(&f, view, groups)))
            }
            Request::InitProducer(req) => self.init_producer(outbox, id, req, view, groups),
            Request::Fetch(f) => {
                let deadline = view.now + Duration::from_millis(u64::from(f.max_wait_ms));
                let pending = PendingFetch { outbox, correlation_id: id, deadline, request: f };
                if let Some(p) = try_fetch(pending, view, groups, false).await? {
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
                    None => out.error = ErrorCode::UNKNOWN_RESOURCE,
                    Some(_) if req.txn_id.is_some() => {
                        out.error = ErrorCode::INVALID_TXN_STATE;
                        out.error_msg = Some("transactions are not supported yet".into());
                    }
                    Some((g, replica)) => match replica.propose(p.batch) {
                        Ok(Proposed { first, last, term, .. }) => {
                            out.base_offset = first - 1;
                            // A duplicate of an applied batch is written; anything
                            // else (new, or the original still in flight) is
                            // answered once its entry's commit decides.
                            if let Some(term) = term {
                                waiting.push(Waiting { resource: ri, partition: pi, group: g, last, term });
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
        let pending = PendingProduce { outbox, correlation_id, deadline, response, waiting };
        if let Some(p) = settle(pending, view.now, groups) {
            self.produces.push(p);
        }
    }

    fn init_producer<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        correlation_id: u32,
        req: InitProducerRequest,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) {
        let fail = |error, leader_hint| {
            Response::InitProducer(InitProducerResponse { error, producer_id: 0, producer_epoch: 0, leader_hint })
        };
        if req.txn_id.is_some() {
            return outbox.push(correlation_id, &fail(ErrorCode::INVALID_TXN_STATE, None));
        }
        let Some((group, replica)) = view.coordinator.and_then(|g| groups.get_mut(&g).map(|r| (g, r))) else {
            return outbox.push(correlation_id, &fail(ErrorCode::COORDINATOR_NOT_AVAILABLE, None));
        };
        let mut b = BatchBuilder::new(BatchOptions::default());
        b.push(0, Some(b"producer-id"), None, &[]);
        let batch = b.finish().expect("one record");
        match replica.propose(batch) {
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

    /// Answers whatever the last round decided, and requests that ran out
    /// of time.
    pub(crate) async fn after_round<R: Runtime, S: StateMachine>(
        &mut self,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) -> Result<(), Error> {
        let produces = std::mem::take(&mut self.produces);
        self.produces = produces.into_iter().filter_map(|p| settle(p, view.now, groups)).collect();
        let inits = std::mem::take(&mut self.inits);
        self.inits = inits.into_iter().filter_map(|p| settle_init(p, view.now, groups)).collect();

        let commits: u64 = groups.values().map(|g| g.raft_ref().commit()).sum();
        let committed = std::mem::replace(&mut self.commits_seen, commits) != commits;
        let fetches = std::mem::take(&mut self.fetches);
        for f in fetches {
            if committed || view.now >= f.deadline {
                let final_try = view.now >= f.deadline;
                if let Some(f) = try_fetch(f, view, groups, final_try).await? {
                    self.fetches.push(f);
                }
            } else {
                self.fetches.push(f);
            }
        }
        Ok(())
    }
}

/// Decides what can be decided; answers and returns `None` once nothing is
/// left to wait for.
fn settle<R: Runtime, S: StateMachine>(
    mut p: PendingProduce,
    now: Instant,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
) -> Option<PendingProduce> {
    let timed_out = now >= p.deadline;
    p.waiting.retain(|w| {
        let out = &mut p.response.resources[w.resource].partitions[w.partition];
        let raft = groups[&w.group].raft_ref();
        if raft.commit() >= w.last {
            // Committed here, so final: the entry at `last` is ours exactly
            // if it carries the term we proposed in.
            match raft.log().term_at(w.last) {
                Some(t) if t == w.term => {}
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
) -> Result<Option<PendingFetch>, Error> {
    let req = &p.request;
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
            match view.group_of(res.resource_id, fp.partition).and_then(|g| groups.get(&g)) {
                None => out.error = ErrorCode::UNKNOWN_RESOURCE,
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
                    } else if fp.fetch_offset < hw && budget > 0 {
                        let max = (fp.max_bytes as usize).min(budget);
                        let read = replica.log().read(fp.fetch_offset, max).await?;
                        out.records = committed_prefix(read, hw)?;
                    }
                    (out.high_watermark, out.last_stable_offset, out.log_start_offset) = (hw, hw, start);
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
        p.outbox.push(p.correlation_id, &Response::Fetch(response));
        return Ok(None);
    }
    Ok(Some(p))
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
    if req.key_type != coordinator::PRODUCER {
        return unavailable("only producer coordination is supported so far");
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

fn metadata<R: Runtime, S: StateMachine>(
    req: &MetadataRequest,
    view: &ClusterView<'_>,
    groups: &BTreeMap<GroupId, Replica<R, S>>,
) -> MetadataResponse {
    let brokers = view
        .client_addrs
        .iter()
        .filter_map(|(id, addr)| {
            let (host, port) = addr.rsplit_once(':')?;
            Some(Broker { node_id: *id as u32, host: host.into(), port: port.parse().ok()?, rack: String::new() })
        })
        .collect();
    let describe = |s: &StreamConfig| ResourceMetadata {
        error: ErrorCode::NONE,
        name: s.name.clone(),
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
                    None => (0, 0, Vec::new()),
                };
                PartitionMetadata { partition: i as u32, leader, leader_term, voters }
            })
            .collect(),
    };
    let resources = if req.names.is_empty() {
        view.streams.iter().map(describe).collect()
    } else {
        req.names
            .iter()
            .map(|n| match view.streams.iter().find(|s| &s.name == n) {
                Some(s) => describe(s),
                None => ResourceMetadata {
                    error: ErrorCode::UNKNOWN_RESOURCE,
                    name: n.clone(),
                    resource_id: 0,
                    kind: kind::STREAM,
                    partitions: Vec::new(),
                },
            })
            .collect()
    };
    MetadataResponse { brokers, resources }
}

#[cfg(test)]
mod tests;

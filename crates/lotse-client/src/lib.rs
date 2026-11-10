//! A Rust client for Lotse over the native protocol ([`lotse_proto`]).
//!
//! The client is written against `lotse-rt`, so the same code runs on Tokio
//! and in the deterministic simulation. It looks up streams with
//! `Metadata`, sends `Produce` and `Fetch` straight to each partition's
//! leader, and follows `NOT_LEADER` hints and metadata changes on its own.
//!
//! Requests on one connection are pipelined: each carries a correlation id
//! and a reader task hands every response to whoever waits for it.
//!
//! Produce is idempotent by default ([`ClientConfig::idempotent`]): the
//! client gets a producer id from the coordinator (`FindCoordinator`,
//! `InitProducer`), stamps every batch with it and a sequence number per
//! partition, and retries a batch whose outcome is unknown (the connection
//! broke after sending, `REQUEST_TIMED_OUT`) with the same sequence; the
//! leader recognises a batch it already has and answers with its offset.
//! Produces to one partition go out one at a time, in call order, so
//! sequences arrive in order.
//!
//! Transactions ([`Client::begin_transaction`], with
//! [`ClientConfig::transactional_id`]) write to several partitions atomically:
//! read_committed consumers see all of a committed transaction and nothing of
//! an aborted one. [`Fetched::committed_batches`] drops what they must not see.
//!
//! Queues are consumed through a [`Consumer`] ([`Client::subscribe`]): the
//! queue's leader pushes messages within the consumer's credit, the consumer
//! acknowledges each one, and after a leader change or a broken connection
//! it subscribes again on its own. Messages not acknowledged by then are
//! delivered again, possibly to another consumer.
//!
//! Only when retrying runs out of time with the outcome still unknown does
//! the caller get [`ClientError::Unknown`]; the client then starts over with
//! a new producer id, so later batches do not wait for a gap that may never
//! fill. Without idempotence an unknown outcome is never retried, since a
//! retry could write the batch twice.

#![warn(missing_docs)]

mod admin;
mod auth;
mod consumer;
mod group;
mod publish;

pub use auth::Credentials;
pub use consumer::{Consumer, Message};
pub use group::{GroupConfig, GroupConsumer, GroupRecord};
pub use publish::{OutMessage, Published};

use std::collections::BTreeMap;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    ApiKey, ErrorCode, FetchPartition, FetchRequest, FetchResource, FindCoordinatorRequest, InitProducerRequest,
    MetadataRequest, MetadataResponse, ProducePartition, ProduceRequest, ProduceResource, Request, Response,
    coordinator, isolation,
};
use lotse_rt::{Clock, Conn, Net, Rng, Spawn};

/// What went wrong.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The broker refused the request.
    #[error("broker error {code}{}", .message.as_deref().map(|m| format!(": {m}")).unwrap_or_default())]
    Broker {
        /// The error code.
        code: ErrorCode,
        /// The broker's explanation, if any.
        message: Option<String>,
    },
    /// The offset is not in the partition: retention deleted it (it is
    /// below `log_start`), or it is past the end.
    #[error("offset {offset} is not in {stream} partition {partition}, which starts at {log_start}")]
    OffsetOutOfRange {
        /// The stream.
        stream: String,
        /// The partition.
        partition: u32,
        /// The offset asked for.
        offset: u64,
        /// The first offset the partition still holds.
        log_start: u64,
    },
    /// A produce may or may not have been written. Read the partition to
    /// find out, or produce again and accept a possible duplicate.
    #[error("outcome unknown: {0}")]
    Unknown(String),
    /// The transaction can no longer be committed (a write's outcome is
    /// unknown, or the producer was fenced): abort it.
    #[error("the transaction must be aborted: {0}")]
    MustAbort(String),
    /// No transactional id is configured.
    #[error("no transactional id is configured")]
    NotTransactional,
    /// No broker could serve the request before the retry time ran out.
    #[error("gave up after {0:?}: {1}")]
    GaveUp(Duration, String),
    /// The stream, queue or exchange does not exist, or has no such
    /// partition.
    #[error("unknown stream {0} or partition {1}")]
    UnknownStream(String, u32),
    /// A mandatory message matched no binding.
    #[error("no queue is bound to {exchange} for routing key {routing_key:?}")]
    Unroutable {
        /// The exchange.
        exchange: String,
        /// The routing key.
        routing_key: String,
    },
    /// A node asked directly did not answer.
    #[error("node {0} did not answer: {1}")]
    NoAnswer(u32, String),
    /// A resource with that name exists already (perhaps created by an
    /// earlier attempt of this same request).
    #[error("{name} exists already (id {resource_id})")]
    ResourceExists {
        /// The name.
        name: String,
        /// The existing resource's id.
        resource_id: u64,
    },
    /// The node did not accept the credentials, or does not offer the
    /// mechanism. Retrying with the same credentials will not help.
    #[error("authentication failed: {0}")]
    Authentication(String),
    /// A published message was written to some of its queues only. Each
    /// failure says whether that write is unknown ([`ClientError::Unknown`])
    /// or did not happen.
    #[error("published to some queues only: {}", publish::describe_partial(written, failed))]
    NotAllWritten {
        /// `(queue, offset)` that hold the message.
        written: Vec<(String, u64)>,
        /// Queues that may not, and why.
        failed: Vec<(String, ClientError)>,
    },
}

/// The tenant every cluster has, which configured resources belong to
/// unless the configuration names another.
pub const DEFAULT_TENANT: &str = "default";

/// How the client behaves.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Brokers to ask for metadata first (`host:port`).
    pub bootstrap: Vec<String>,
    /// How long one request may take before the connection is given up.
    pub request_timeout: Duration,
    /// How long retriable failures (no leader, broken connections) are
    /// retried before [`ClientError::GaveUp`].
    pub retry_for: Duration,
    /// Pause between retries.
    pub backoff: Duration,
    /// How long the leader waits for the quorum on a produce.
    pub produce_timeout: Duration,
    /// Produce idempotently (on by default). Needs a coordinator group in
    /// the cluster.
    pub idempotent: bool,
    /// Transactional id, for [`Client::begin_transaction`]. One client
    /// instance per id: a new instance fences the old one.
    pub transactional_id: Option<String>,
    /// A stable name for this idempotent producer (ADR 0071). With one, the
    /// producer id comes from the coordinator under the name and every
    /// start raises its epoch, so a client that restarts fences its
    /// earlier instance: that instance's retries are refused instead of
    /// written after the new one's batches. Names share one namespace with
    /// transactional ids and need the producer role for them; one client
    /// instance per name. Without one, each client gets a fresh producer id.
    pub producer_name: Option<String>,
    /// How long a transaction may stay open before the coordinator aborts it.
    pub txn_timeout: Duration,
    /// How long exchange bindings learnt from metadata are used before they
    /// are asked for again.
    pub metadata_max_age: Duration,
    /// How to authenticate each connection; `None` sends no credentials
    /// (the runtime's client certificate, if any, still counts).
    pub credentials: Option<Credentials>,
    /// The tenant resources created by this client go to, and the one its
    /// transactional id and consumer group ids live in (ADR 0062): the same
    /// id in another tenant is another id.
    pub tenant: String,
}

impl ClientConfig {
    /// Defaults for `bootstrap`.
    pub fn new(bootstrap: Vec<String>) -> Self {
        ClientConfig {
            bootstrap,
            request_timeout: Duration::from_secs(10),
            retry_for: Duration::from_secs(30),
            backoff: Duration::from_millis(50),
            produce_timeout: Duration::from_secs(5),
            idempotent: true,
            transactional_id: None,
            producer_name: None,
            txn_timeout: Duration::from_secs(60),
            metadata_max_age: Duration::from_secs(30),
            credentials: None,
            tenant: DEFAULT_TENANT.into(),
        }
    }
}

/// A point in a partition to look up with [`Client::list_offset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetAt {
    /// The first offset still stored.
    Earliest,
    /// The offset after the last one a read_committed reader may see.
    Latest,
    /// Milliseconds since the epoch: the first batch with a record at or
    /// after it.
    Time(i64),
}

/// Records read from one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    /// Whole record batches, control batches and aborted ones included; may
    /// be empty. See [`Fetched::committed_batches`].
    pub records: Bytes,
    /// Offsets below this are committed.
    pub high_watermark: u64,
    /// Offsets below this are committed and in no open transaction; reads
    /// stop here.
    pub last_stable_offset: u64,
    /// First offset still in the partition.
    pub log_start_offset: u64,
    /// Aborted transactions with data in `records`: `(producer id, first
    /// offset)`.
    pub aborted: Vec<(u64, u64)>,
}

impl Fetched {
    /// The offset to fetch next: after the last batch, or `from` if there
    /// is none.
    pub fn next_offset(&self, from: u64) -> u64 {
        let mut rest = self.records.clone();
        let mut next = from;
        while let Ok((b, r)) = lotse_record::Batch::parse(&rest) {
            next = b.next_offset();
            rest = r;
            if rest.is_empty() {
                break;
            }
        }
        next
    }

    /// The data batches a read_committed consumer sees: control batches and
    /// batches of aborted transactions left out. An aborted transaction's
    /// batches are those of its producer from its first offset up to the
    /// producer's ABORT marker.
    pub fn committed_batches(&self) -> Result<Vec<lotse_record::Batch>, ClientError> {
        let corrupt = |e: lotse_record::Error| ClientError::Broker {
            code: ErrorCode::CORRUPT_BATCH,
            message: Some(e.to_string()),
        };
        let mut aborted = self.aborted.clone();
        aborted.sort_by_key(|(_, first)| *first);
        let mut pending = aborted.into_iter().peekable();
        let mut skipping = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        let mut rest = self.records.clone();
        while !rest.is_empty() {
            let (b, r) = lotse_record::Batch::parse(&rest).map_err(corrupt)?;
            rest = r;
            while let Some((producer, _)) = pending.next_if(|(_, first)| *first <= b.base_offset()) {
                skipping.insert(producer);
            }
            if b.attributes().control {
                if let Ok(Some(lotse_record::Control::Marker(lotse_record::ControlType::Abort, _))) =
                    lotse_record::control(&b)
                {
                    skipping.remove(&b.producer_id());
                }
                continue;
            }
            if b.attributes().transactional && skipping.contains(&b.producer_id()) {
                continue;
            }
            out.push(b);
        }
        Ok(out)
    }
}

/// One partition's place in the cluster, as metadata last said.
#[derive(Debug, Clone, Copy)]
struct Route {
    resource_id: u64,
    leader: u32,
    term: u64,
}

/// A client. Cheap to share behind an `Arc`; every method takes `&self`.
pub struct Client<R: Net + Clock + Spawn + Rng> {
    rt: R,
    cfg: ClientConfig,
    session: Mutex<Option<Session>>,
    /// Producer id and epoch for the transactional id, once initialised.
    txn_producer: Mutex<Option<(u64, u16)>>,
    /// One transaction at a time.
    txn_lock: AsyncLock,
    coordinator_addr: Mutex<Option<String>>,
    partition_locks: Mutex<BTreeMap<(String, u32), Arc<AsyncLock>>>,
    conns: Mutex<BTreeMap<String, Arc<Connection<R::Conn>>>>,
    brokers: Mutex<BTreeMap<u32, String>>,
    routes: Mutex<BTreeMap<(String, u32), Route>>,
    /// Resource names by id, for requests that address resources by id.
    names: Mutex<BTreeMap<u64, String>>,
    /// Exchanges by name, for routing published messages, and when they
    /// were learnt (bindings may change: they are relearnt after
    /// [`ClientConfig::metadata_max_age`]).
    exchanges: Mutex<BTreeMap<String, (lotse_route::Exchange, lotse_rt::Instant)>>,
    /// The metadata group's leader, for admin requests.
    metadata_leader: Mutex<Option<u32>>,
    /// Spreads admin requests over brokers while the leader is unknown.
    admin_turn: AtomicU32,
    /// A failed login, until a retry loop reports it or a login succeeds.
    fatal: Mutex<Option<ClientError>>,
}

impl<R: Net + Clock + Spawn + Rng> fmt::Debug for Client<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").field("bootstrap", &self.cfg.bootstrap).finish()
    }
}

/// Why one attempt failed.
enum Attempt {
    /// Try again (after refreshing metadata).
    Retry(String),
    /// Try again; the request may have been carried out (only idempotent
    /// produce retries these).
    RetryMaybeDone(String),
    /// Give up with this error.
    Fail(ClientError),
}

/// Ends the session of `producer_id` when dropped while armed: a produce
/// dropped before its outcome was known.
struct Undecided<'a> {
    session: &'a Mutex<Option<Session>>,
    producer_id: u64,
    armed: bool,
}

impl Drop for Undecided<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut session) = self.session.lock()
            && session.as_ref().is_some_and(|s| s.producer_id == self.producer_id)
        {
            *session = None;
        }
    }
}

/// The producer id this client writes with.
#[derive(Debug, Clone)]
struct Session {
    producer_id: u64,
    epoch: u16,
    /// Next sequence per partition.
    next: BTreeMap<(String, u32), i32>,
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// A client; it connects on first use.
    pub fn new(rt: R, cfg: ClientConfig) -> Self {
        Client {
            rt,
            cfg,
            session: Mutex::new(None),
            txn_producer: Mutex::new(None),
            txn_lock: AsyncLock::default(),
            coordinator_addr: Mutex::new(None),
            partition_locks: Mutex::new(BTreeMap::new()),
            conns: Mutex::new(BTreeMap::new()),
            brokers: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(BTreeMap::new()),
            names: Mutex::new(BTreeMap::new()),
            exchanges: Mutex::new(BTreeMap::new()),
            metadata_leader: Mutex::new(None),
            admin_turn: AtomicU32::new(0),
            fatal: Mutex::new(None),
        }
    }

    /// Asks any reachable broker for metadata and remembers the leaders.
    pub async fn metadata(&self, names: &[&str]) -> Result<MetadataResponse, ClientError> {
        let start = self.rt.now();
        let mut why = String::from("no broker known");
        loop {
            let mut addrs: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
            addrs.extend(self.cfg.bootstrap.iter().cloned());
            addrs.dedup();
            for addr in addrs {
                let req = Request::Metadata(MetadataRequest { names: names.iter().map(|n| n.to_string()).collect() });
                match self.call(&addr, &req).await {
                    Ok(Response::Metadata(m)) => {
                        self.learn(&m);
                        return Ok(m);
                    }
                    Ok(other) => why = format!("unexpected response {other:?}"),
                    Err(CallError::Denied(why)) => return Err(ClientError::Authentication(why)),
                    Err(e) => why = format!("{addr}: {e}"),
                }
            }
            if self.rt.now() - start >= self.cfg.retry_for {
                return Err(ClientError::GaveUp(self.cfg.retry_for, why));
            }
            self.rt.sleep(self.cfg.backoff).await;
        }
    }

    /// Asks every known broker and merges what they know: per name, the
    /// newest resource any of them knows.
    async fn metadata_from_all(&self, names: &[&str]) -> MetadataResponse {
        let mut addrs: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
        addrs.extend(self.cfg.bootstrap.iter().cloned());
        addrs.sort();
        addrs.dedup();
        let mut merged = MetadataResponse::default();
        for addr in addrs {
            let req = Request::Metadata(MetadataRequest { names: names.iter().map(|n| n.to_string()).collect() });
            let Ok(Response::Metadata(m)) = self.call(&addr, &req).await else { continue };
            self.learn(&m);
            merge_newest(&mut merged.resources, m.resources);
            merged.brokers = m.brokers;
        }
        merged
    }

    fn learn(&self, m: &MetadataResponse) {
        self.learn_exchanges(m);
        let mut brokers = self.brokers.lock().expect("brokers lock");
        for b in &m.brokers {
            brokers.insert(b.node_id, format!("{}:{}", b.host, b.port));
        }
        let mut names = self.names.lock().expect("names lock");
        for r in m.resources.iter().filter(|r| r.error == ErrorCode::NONE) {
            names.insert(r.resource_id, r.name.clone());
        }
        drop(names);
        let mut routes = self.routes.lock().expect("routes lock");
        for r in m.resources.iter().filter(|r| r.error == ErrorCode::NONE) {
            for p in &r.partitions {
                routes.insert(
                    (r.name.clone(), p.partition),
                    Route { resource_id: r.resource_id, leader: p.leader, term: p.leader_term },
                );
            }
        }
    }

    /// The leader's address, refreshing metadata if none is known.
    async fn route(&self, stream: &str, partition: u32) -> Result<(Route, String), Attempt> {
        let known = self.routes.lock().expect("routes lock").get(&(stream.to_string(), partition)).copied();
        let route = match known {
            Some(r) if r.leader != 0 => r,
            _ => {
                let mut m = self.metadata(&[stream]).await.map_err(Attempt::Fail)?;
                if !m.resources.iter().any(|r| r.name == stream && r.error == ErrorCode::NONE) {
                    // A resource created a moment ago may not have reached
                    // that broker yet: any broker that knows it will do.
                    m = self.metadata_from_all(&[stream]).await;
                }
                let res = m.resources.iter().find(|r| r.name == stream && r.error == ErrorCode::NONE);
                let Some(p) = res.and_then(|r| r.partitions.iter().find(|p| p.partition == partition)) else {
                    return Err(Attempt::Fail(ClientError::UnknownStream(stream.into(), partition)));
                };
                let resource_id = res.expect("found").resource_id;
                let (mut leader, mut term) = (p.leader, p.leader_term);
                if leader == 0 {
                    // Partitions created at runtime live on some nodes only:
                    // their voters know the leader.
                    for v in p.voters.clone() {
                        let addr = self.brokers.lock().expect("brokers lock").get(&v).cloned();
                        let Some(addr) = addr else { continue };
                        let req = Request::Metadata(MetadataRequest { names: vec![stream.to_string()] });
                        let Ok(Response::Metadata(vm)) = self.call(&addr, &req).await else { continue };
                        self.learn(&vm);
                        let found = vm.resources.iter().find(|r| r.name == stream).and_then(|r| {
                            r.partitions.iter().find(|p| p.partition == partition).map(|p| (p.leader, p.leader_term))
                        });
                        if let Some((l, t)) = found.filter(|(l, _)| *l != 0) {
                            (leader, term) = (l, t);
                            break;
                        }
                    }
                }
                if leader == 0 {
                    return Err(Attempt::Retry("the partition has no leader".into()));
                }
                Route { resource_id, leader, term }
            }
        };
        let addr = self.brokers.lock().expect("brokers lock").get(&route.leader).cloned();
        match addr {
            Some(a) => Ok((route, a)),
            None => {
                self.forget(stream, partition);
                Err(Attempt::Retry(format!("no address for node {}", route.leader)))
            }
        }
    }

    fn forget(&self, stream: &str, partition: u32) {
        self.routes.lock().expect("routes lock").remove(&(stream.to_string(), partition));
    }

    /// Follows a leader hint, or forgets the route so metadata is reloaded.
    fn redirect(&self, stream: &str, partition: u32, hint: Option<lotse_proto::LeaderHint>) {
        let mut routes = self.routes.lock().expect("routes lock");
        let key = (stream.to_string(), partition);
        match (hint, routes.get_mut(&key)) {
            (Some(h), Some(r)) if h.term >= r.term && h.leader != r.leader => {
                r.leader = h.leader;
                r.term = h.term;
            }
            _ => {
                routes.remove(&key);
            }
        }
    }

    async fn retrying<T, F, Fut>(&self, mut attempt: F) -> Result<T, ClientError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, Attempt>>,
    {
        let start = self.rt.now();
        let mut maybe_done = false;
        loop {
            let why = match attempt().await {
                Ok(v) => return Ok(v),
                // A failed login or a refusal now says nothing of an earlier
                // attempt whose answer was lost: that one may have been
                // carried out. (An answer about the request itself, such as
                // RESOURCE_EXISTS, is the caller's to read.)
                Err(Attempt::Fail(
                    e @ (ClientError::Authentication(_) | ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. }),
                )) if maybe_done => {
                    return Err(ClientError::Unknown(format!(
                        "an earlier attempt may have been carried out; then {e}"
                    )));
                }
                Err(Attempt::Fail(e)) => return Err(e),
                Err(Attempt::Retry(why)) => why,
                Err(Attempt::RetryMaybeDone(why)) => {
                    maybe_done = true;
                    why
                }
            };
            // A connection that could not authenticate: the same credentials
            // will not do better on the next try.
            if let Some(fatal) = self.fatal.lock().expect("fatal lock").take() {
                return Err(if maybe_done {
                    ClientError::Unknown(format!("an earlier attempt may have been carried out; then {fatal}"))
                } else {
                    fatal
                });
            }
            if self.rt.now() - start >= self.cfg.retry_for {
                return Err(if maybe_done {
                    ClientError::Unknown(format!("still unknown after {:?}: {why}", self.cfg.retry_for))
                } else {
                    ClientError::GaveUp(self.cfg.retry_for, why)
                });
            }
            self.rt.sleep(self.cfg.backoff).await;
        }
    }

    /// Appends one record batch (built with `lotse_record::BatchBuilder`) to
    /// a partition. Returns the offset of its first record once the batch is
    /// committed.
    pub async fn produce(&self, stream: &str, partition: u32, batch: Bytes) -> Result<u64, ClientError> {
        if !self.cfg.idempotent {
            return self.retrying(|| self.produce_once(stream, partition, batch.clone(), false, None)).await;
        }
        let records = lotse_record::Batch::parse(&batch)
            .map_err(|e| ClientError::Broker { code: ErrorCode::CORRUPT_BATCH, message: Some(e.to_string()) })?
            .0
            .record_count();
        let key = (stream.to_string(), partition);
        let lock = self.partition_locks.lock().expect("locks lock").entry(key.clone()).or_default().clone();
        let _turn = lock.lock().await;
        let (producer_id, epoch, seq) = self.next_sequence(&key).await?;
        let mut stamped = bytes::BytesMut::from(&batch[..]);
        lotse_record::set_producer(&mut stamped, producer_id, epoch, seq)
            .map_err(|e| ClientError::Broker { code: ErrorCode::CORRUPT_BATCH, message: Some(e.to_string()) })?;
        let stamped = stamped.freeze();
        // Dropped before its outcome is known (a caller's timeout, a
        // node's deadline), this produce leaves the sequence in doubt like
        // `Unknown` does: the guard ends the session, or the next batch
        // would take the same sequence and be taken for this one.
        let mut undecided = Undecided { session: &self.session, producer_id, armed: true };
        let res = self.retrying(|| self.produce_once(stream, partition, stamped.clone(), true, None)).await;
        undecided.armed = false;
        let mut session = self.session.lock().expect("session lock");
        match &res {
            Ok(_) => {
                if let Some(s) = session.as_mut().filter(|s| s.producer_id == producer_id) {
                    s.next.insert(key, seq.wrapping_add(records as i32));
                }
            }
            // Written or not, nobody can tell: later batches would wait behind a
            // gap that may never fill. Start over with a new producer id.
            // A named producer that is fenced has a newer instance: it keeps
            // its fenced id rather than start over, which would fence that
            // one in turn (ADR 0071).
            Err(ClientError::Broker { code: ErrorCode::PRODUCER_FENCED, .. }) if self.cfg.producer_name.is_some() => {}
            Err(ClientError::Unknown(_))
            | Err(ClientError::Broker {
                code: ErrorCode::OUT_OF_ORDER_SEQUENCE | ErrorCode::PRODUCER_FENCED, ..
            }) => {
                if session.as_ref().is_some_and(|s| s.producer_id == producer_id) {
                    *session = None;
                }
            }
            // Definitely not written: the sequence is free for the next batch.
            Err(_) => {}
        }
        res
    }

    /// The producer id, epoch and next sequence for a partition, getting a
    /// producer id first if there is none.
    async fn next_sequence(&self, key: &(String, u32)) -> Result<(u64, u16, i32), ClientError> {
        if let Some(s) = self.session.lock().expect("session lock").as_ref() {
            return Ok((s.producer_id, s.epoch, s.next.get(key).copied().unwrap_or(0)));
        }
        let name = self.cfg.producer_name.as_deref();
        let (producer_id, epoch) = self.retrying(|| self.init_producer_once(name)).await?;
        let mut session = self.session.lock().expect("session lock");
        // Another partition's produce may have started a session meanwhile.
        let s = session.get_or_insert(Session { producer_id, epoch, next: BTreeMap::new() });
        Ok((s.producer_id, s.epoch, s.next.get(key).copied().unwrap_or(0)))
    }

    /// The coordinator's address, asking any broker for it if needed.
    async fn coordinator(&self, key: &str) -> Result<String, Attempt> {
        if let Some(a) = self.coordinator_addr.lock().expect("coordinator lock").clone() {
            return Ok(a);
        }
        let mut addrs: Vec<String> = self.brokers.lock().expect("brokers lock").values().cloned().collect();
        addrs.extend(self.cfg.bootstrap.iter().cloned());
        let find =
            Request::FindCoordinator(FindCoordinatorRequest { key_type: coordinator::PRODUCER, key: key.into() });
        let mut why = String::from("no broker answered");
        for addr in addrs {
            match self.call(&addr, &find).await {
                Ok(Response::FindCoordinator(f)) if f.error == ErrorCode::NONE => {
                    let a = format!("{}:{}", f.host, f.port);
                    *self.coordinator_addr.lock().expect("coordinator lock") = Some(a.clone());
                    return Ok(a);
                }
                Ok(Response::FindCoordinator(f)) => {
                    why = f.error_msg.unwrap_or_else(|| f.error.to_string());
                    if !f.error.retriable() {
                        return Err(Attempt::Fail(ClientError::Broker { code: f.error, message: Some(why) }));
                    }
                }
                Ok(other) => why = format!("unexpected response {other:?}"),
                Err(e) => why = e.to_string(),
            }
        }
        Err(Attempt::Retry(why))
    }

    /// Asks the coordinator again next time.
    fn forget_coordinator(&self) {
        *self.coordinator_addr.lock().expect("coordinator lock") = None;
    }

    /// One request to the coordinator; retriable failures and lost requests
    /// become `Retry` (every coordinator request may be repeated).
    async fn call_coordinator(&self, key: &str, req: &Request) -> Result<Response, Attempt> {
        let addr = self.coordinator(key).await?;
        match self.call(&addr, req).await {
            Ok(r) => Ok(r),
            Err(e) => {
                self.forget_coordinator();
                Err(e.into())
            }
        }
    }

    async fn init_producer_once(&self, txn: Option<&str>) -> Result<(u64, u16), Attempt> {
        let init = Request::InitProducer(InitProducerRequest {
            txn_id: txn.map(str::to_string),
            tenant: self.cfg.tenant.clone(),
            txn_timeout_ms: u32::try_from(self.cfg.txn_timeout.as_millis()).unwrap_or(u32::MAX),
        });
        match self.call_coordinator(txn.unwrap_or(""), &init).await? {
            Response::InitProducer(r) if r.error == ErrorCode::NONE => Ok((r.producer_id, r.producer_epoch)),
            // An id that was handed out but never received is just unused.
            Response::InitProducer(r) if r.error.retriable() => {
                self.forget_coordinator();
                Err(Attempt::Retry(r.error.to_string()))
            }
            Response::InitProducer(r) => Err(Attempt::Fail(ClientError::Broker { code: r.error, message: None })),
            other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
        }
    }

    /// Starts a transaction for the configured transactional id, getting
    /// its producer id first (which fences any older instance). One at a
    /// time per client; a second call waits for the first to end.
    pub async fn begin_transaction(&self) -> Result<Transaction<'_, R>, ClientError> {
        let txn_id = self.cfg.transactional_id.clone().ok_or(ClientError::NotTransactional)?;
        let turn = self.txn_lock.lock().await;
        let known = *self.txn_producer.lock().expect("txn lock");
        let (producer_id, epoch) = match known {
            Some(p) => p,
            None => {
                let p = self.retrying(|| self.init_producer_once(Some(&txn_id))).await?;
                *self.txn_producer.lock().expect("txn lock") = Some(p);
                p
            }
        };
        Ok(Transaction {
            client: self,
            _turn: turn,
            txn_id,
            producer_id,
            epoch,
            next: BTreeMap::new(),
            added: std::collections::BTreeSet::new(),
            coordinator_may_hold: false,
            poisoned: None,
            ended: false,
        })
    }

    async fn produce_once(
        &self,
        stream: &str,
        partition: u32,
        batch: Bytes,
        idempotent: bool,
        txn_id: Option<String>,
    ) -> Result<u64, Attempt> {
        // An idempotent batch may be sent again whatever happened to it.
        let unknown = |why: String| {
            if idempotent { Attempt::RetryMaybeDone(why) } else { Attempt::Fail(ClientError::Unknown(why)) }
        };
        let (route, addr) = self.route(stream, partition).await?;
        let req = Request::Produce(ProduceRequest {
            txn_id,
            tenant: self.cfg.tenant.clone(),
            timeout_ms: u32::try_from(self.cfg.produce_timeout.as_millis()).unwrap_or(u32::MAX),
            resources: vec![ProduceResource {
                resource_id: route.resource_id,
                partitions: vec![ProducePartition { partition, batch }],
            }],
        });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::Produce(r)) => r,
            Ok(other) => return Err(unknown(format!("unexpected response {other:?}"))),
            // Not sent at all: safe to try again.
            Err(CallError::NotSent(why)) => {
                self.forget(stream, partition);
                return Err(Attempt::Retry(why));
            }
            Err(CallError::Lost(why)) => {
                self.forget(stream, partition);
                return Err(unknown(why));
            }
            Err(CallError::Denied(why)) => return Err(Attempt::Fail(ClientError::Authentication(why))),
            Err(CallError::Refused(code, why)) => {
                return Err(Attempt::Fail(ClientError::Broker { code, message: Some(why) }));
            }
        };
        let p = resp
            .resources
            .first()
            .and_then(|r| r.partitions.first())
            .ok_or_else(|| unknown("empty produce response".into()))?;
        match p.error {
            ErrorCode::NONE => Ok(p.base_offset),
            // Refused before it was proposed, or definitely not written.
            ErrorCode::NOT_LEADER | ErrorCode::FENCED_LEADER_TERM => {
                self.redirect(stream, partition, p.leader_hint);
                Err(Attempt::Retry(format!("{}", p.error)))
            }
            ErrorCode::REQUEST_TIMED_OUT => {
                self.forget(stream, partition);
                Err(unknown(p.error_msg.clone().unwrap_or_else(|| p.error.to_string())))
            }
            code => {
                // Deleted: the name may come back as a new resource.
                if code == ErrorCode::UNKNOWN_RESOURCE {
                    self.forget(stream, partition);
                }
                Err(Attempt::Fail(ClientError::Broker { code, message: p.error_msg.clone() }))
            }
        }
    }

    /// Internal, used by a queue's leader: forwards dead-lettered messages
    /// (one-record batches that carry their origin) to the dead-letter queue
    /// `resource_id`, retrying until its leader answers. Returns once the
    /// queue has them committed. Sending the same message again is harmless.
    pub async fn dead_letter(&self, resource_id: u64, batches: Vec<Bytes>) -> Result<(), ClientError> {
        let name = self.name_of(resource_id).await?;
        self.retrying(|| self.dead_letter_once(&name, resource_id, batches.clone())).await
    }

    async fn name_of(&self, resource_id: u64) -> Result<String, ClientError> {
        let known = self.names.lock().expect("names lock").get(&resource_id).cloned();
        if let Some(n) = known {
            return Ok(n);
        }
        self.metadata(&[]).await?;
        self.names
            .lock()
            .expect("names lock")
            .get(&resource_id)
            .cloned()
            .ok_or_else(|| ClientError::UnknownStream(format!("#{resource_id}"), 0))
    }

    async fn dead_letter_once(&self, name: &str, resource_id: u64, batches: Vec<Bytes>) -> Result<(), Attempt> {
        let (_, addr) = self.route(name, 0).await?;
        let req = Request::DeadLetter(lotse_proto::DeadLetterRequest { queue_id: resource_id, batches });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::Outcome(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            // Messages are recognised when they come again: send again.
            Err(e) => {
                self.forget(name, 0);
                return Err(e.into());
            }
        };
        match resp.error {
            ErrorCode::NONE => Ok(()),
            code if code.retriable() => {
                self.forget(name, 0);
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Internal, used by a partition's leader before it takes a producer's
    /// first transactional batch of an epoch (ADR 0057): asks the
    /// transaction coordinator whether `req` names an open transaction that
    /// holds the partition. `Ok(())` if so; otherwise the coordinator's
    /// answer, such as `INVALID_TXN_STATE` or `PRODUCER_FENCED`.
    pub async fn verify_txn(&self, req: lotse_proto::VerifyTxnRequest) -> Result<(), ClientError> {
        self.retrying(|| self.verify_txn_once(&req)).await
    }

    async fn verify_txn_once(&self, req: &lotse_proto::VerifyTxnRequest) -> Result<(), Attempt> {
        let request = Request::VerifyTxn(req.clone());
        let Response::Outcome(r) = self.call_coordinator(&req.txn_id, &request).await? else {
            return Err(Attempt::Retry("unexpected response".into()));
        };
        match r.error {
            ErrorCode::NONE => Ok(()),
            code if code.retriable() => {
                self.forget_coordinator();
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Internal, used by the metadata leader before a conditional delete
    /// (ADR 0063): whether a partition of a stream or queue holds anything,
    /// asked of its leader.
    pub async fn check_empty(&self, resource_id: u64, partition: u32) -> Result<bool, ClientError> {
        let name = self.name_of(resource_id).await?;
        self.retrying(|| self.check_empty_once(&name, resource_id, partition)).await
    }

    async fn check_empty_once(&self, name: &str, resource_id: u64, partition: u32) -> Result<bool, Attempt> {
        let (_, addr) = self.route(name, partition).await?;
        let req = Request::CheckEmpty(lotse_proto::CheckEmptyRequest { resource_id, partition });
        let r = match self.call(&addr, &req).await {
            Ok(Response::Outcome(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                self.forget(name, partition);
                return Err(e.into());
            }
        };
        match r.error {
            ErrorCode::NONE => Ok(true),
            ErrorCode::RESOURCE_NOT_EMPTY => Ok(false),
            code if code.retriable() => {
                self.redirect(name, partition, r.leader_hint);
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Internal, used by a node's transaction coordinator: writes a marker
    /// into one partition, retrying until its leader answers. Returns
    /// [`ErrorCode::NONE`] once the marker is written (or not needed), or
    /// [`ErrorCode::COORDINATOR_FENCED`] if a newer coordinator took over.
    pub async fn write_txn_marker(
        &self,
        resource_id: u64,
        partition: u32,
        marker: lotse_proto::TxnMarker,
    ) -> Result<ErrorCode, ClientError> {
        let name = self.name_of(resource_id).await?;
        let marker = lotse_proto::TxnMarker {
            resources: vec![lotse_proto::TxnResource { resource_id, partitions: vec![partition] }],
            ..marker
        };
        self.retrying(|| self.write_txn_marker_once(&name, partition, marker.clone())).await
    }

    async fn write_txn_marker_once(
        &self,
        stream: &str,
        partition: u32,
        marker: lotse_proto::TxnMarker,
    ) -> Result<ErrorCode, Attempt> {
        let (_, addr) = self.route(stream, partition).await?;
        let req = Request::WriteTxnMarkers(lotse_proto::WriteTxnMarkersRequest { markers: vec![marker] });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::WriteTxnMarkers(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            // Markers are idempotent: whatever happened, sending again is safe.
            Err(e) => {
                self.forget(stream, partition);
                return Err(e.into());
            }
        };
        let error = resp
            .markers
            .first()
            .and_then(|m| m.resources.first())
            .and_then(|r| r.partitions.first())
            .map(|p| p.error)
            .ok_or_else(|| Attempt::Retry("empty marker response".into()))?;
        match error {
            ErrorCode::NONE | ErrorCode::COORDINATOR_FENCED => Ok(error),
            code if code.retriable() => {
                self.forget(stream, partition);
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Reads committed batches from `offset`, waiting up to `max_wait` for
    /// some to arrive.
    pub async fn fetch(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        max_wait: Duration,
    ) -> Result<Fetched, ClientError> {
        self.retrying(|| self.fetch_once(stream, partition, offset, max_wait, isolation::COMMITTED)).await
    }

    /// Like [`Client::fetch`], but also past the last stable offset:
    /// batches of open and aborted transactions and their markers, up to
    /// the high watermark. For inspecting a log, not for processing it.
    pub async fn fetch_uncommitted(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        max_wait: Duration,
    ) -> Result<Fetched, ClientError> {
        self.retrying(|| self.fetch_once(stream, partition, offset, max_wait, isolation::UNCOMMITTED)).await
    }

    async fn fetch_once(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        max_wait: Duration,
        isolation: u8,
    ) -> Result<Fetched, Attempt> {
        let (route, addr) = self.route(stream, partition).await?;
        let req = Request::Fetch(FetchRequest {
            max_wait_ms: u32::try_from(max_wait.as_millis()).unwrap_or(u32::MAX),
            min_bytes: 1,
            max_bytes: 4 << 20,
            isolation,
            resources: vec![FetchResource {
                resource_id: route.resource_id,
                partitions: vec![FetchPartition {
                    partition,
                    fetch_offset: offset,
                    leader_term: route.term,
                    max_bytes: 1 << 20,
                }],
            }],
        });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::Fetch(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                self.forget(stream, partition);
                return Err(e.into());
            }
        };
        let p = resp
            .resources
            .into_iter()
            .next()
            .and_then(|r| r.partitions.into_iter().next())
            .ok_or_else(|| Attempt::Retry("empty fetch response".into()))?;
        match p.error {
            ErrorCode::NONE => Ok(Fetched {
                records: p.records,
                high_watermark: p.high_watermark,
                last_stable_offset: p.last_stable_offset,
                log_start_offset: p.log_start_offset,
                aborted: p.aborted_txns.iter().map(|a| (a.producer_id, a.first_offset)).collect(),
            }),
            code if code.retriable() => {
                self.redirect(stream, partition, p.leader_hint);
                Err(Attempt::Retry(code.to_string()))
            }
            ErrorCode::OFFSET_OUT_OF_RANGE => Err(Attempt::Fail(ClientError::OffsetOutOfRange {
                stream: stream.into(),
                partition,
                offset,
                log_start: p.log_start_offset,
            })),
            code => {
                if code == ErrorCode::UNKNOWN_RESOURCE {
                    self.forget(stream, partition);
                }
                Err(Attempt::Fail(ClientError::Broker { code, message: None }))
            }
        }
    }

    /// Where `at` falls in a partition, as a read_committed reader sees it
    /// (ADR 0059): [`OffsetAt::Earliest`] is the first offset still stored,
    /// [`OffsetAt::Latest`] the offset after the last one it may read, and
    /// [`OffsetAt::Time`] the first offset of the first batch with a record
    /// at or after that time, or the latest offset if there is none yet.
    pub async fn list_offset(&self, stream: &str, partition: u32, at: OffsetAt) -> Result<u64, ClientError> {
        self.retrying(|| self.list_offset_once(stream, partition, at)).await
    }

    async fn list_offset_once(&self, stream: &str, partition: u32, at: OffsetAt) -> Result<u64, Attempt> {
        let (route, addr) = self.route(stream, partition).await?;
        let timestamp = match at {
            OffsetAt::Earliest => lotse_proto::list_offsets::EARLIEST,
            OffsetAt::Latest => lotse_proto::list_offsets::LATEST,
            OffsetAt::Time(ms) => ms.max(0),
        };
        let req = Request::ListOffsets(lotse_proto::ListOffsetsRequest {
            isolation: isolation::COMMITTED,
            resources: vec![lotse_proto::ListOffsetsResource {
                resource_id: route.resource_id,
                partitions: vec![lotse_proto::ListOffsetsPartition { partition, timestamp }],
            }],
        });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::ListOffsets(r)) => r,
            Ok(other) => return Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            Err(e) => {
                self.forget(stream, partition);
                return Err(e.into());
            }
        };
        let p = resp
            .resources
            .into_iter()
            .next()
            .and_then(|r| r.partitions.into_iter().next())
            .ok_or_else(|| Attempt::Retry("empty list-offsets response".into()))?;
        match p.error {
            ErrorCode::NONE => Ok(p.offset),
            code if code.retriable() => {
                self.redirect(stream, partition, p.leader_hint);
                Err(Attempt::Retry(code.to_string()))
            }
            code => {
                if code == ErrorCode::UNKNOWN_RESOURCE {
                    self.forget(stream, partition);
                }
                Err(Attempt::Fail(ClientError::Broker { code, message: None }))
            }
        }
    }

    /// Sends one request to `addr` and waits for its response.
    async fn call(&self, addr: &str, req: &Request) -> Result<Response, CallError> {
        let conn = match self.connection(addr).await {
            Ok(c) => c,
            Err(e) => {
                self.note_fatal(&e);
                return Err(e);
            }
        };
        let res = conn.call(req, &self.rt, self.cfg.request_timeout).await;
        if let Err(e) = &res {
            self.note_fatal(e);
        }
        if res.is_err() {
            // A connection that failed once is not trusted again.
            let mut conns = self.conns.lock().expect("conns lock");
            if conns.get(addr).is_some_and(|c| Arc::ptr_eq(c, &conn)) {
                conns.remove(addr);
            }
            conn.close();
        }
        res
    }

    async fn connection(&self, addr: &str) -> Result<Arc<Connection<R::Conn>>, CallError> {
        let cached = self.conns.lock().expect("conns lock").get(addr).cloned();
        if let Some(c) = cached {
            let times = *c.session.lock().expect("session lock");
            match times {
                None => return Ok(c),
                Some(t) if self.rt.now() < t.renew_at => return Ok(c),
                // Ended: requests would be refused. Start over.
                Some(t) if self.rt.now() >= t.ends_at => self.drop_connection(addr, &c),
                // One caller renews; the others go on with the session,
                // which still holds.
                Some(_) if c.renewing.swap(true, Ordering::AcqRel) => return Ok(c),
                Some(_) => {
                    let creds = self.cfg.credentials.as_ref().expect("a session comes from credentials");
                    let renewed = c.authenticate(&self.rt, creds, self.cfg.request_timeout).await;
                    c.renewing.store(false, Ordering::Release);
                    match renewed {
                        Ok(times) => {
                            *c.session.lock().expect("session lock") = times;
                            self.logged_in();
                            return Ok(c);
                        }
                        Err(CallError::Denied(why)) => {
                            self.drop_connection(addr, &c);
                            return Err(CallError::Denied(why));
                        }
                        Err(_) => self.drop_connection(addr, &c),
                    }
                }
            }
        }
        let raw =
            match race(self.rt.connect(addr, lotse_rt::Link::Client), self.rt.sleep(self.cfg.request_timeout)).await {
                Some(Ok(c)) => c,
                Some(Err(e)) => return Err(CallError::NotSent(format!("cannot connect to {addr}: {e}"))),
                None => return Err(CallError::NotSent(format!("connecting to {addr} timed out"))),
            };
        let conn = Arc::new(Connection::new(raw));
        // The reader ends when the connection breaks or is closed.
        drop(self.rt.spawn(conn.clone().read_responses()));
        if let Some(creds) = &self.cfg.credentials {
            match conn.authenticate(&self.rt, creds, self.cfg.request_timeout).await {
                Ok(times) => {
                    *conn.session.lock().expect("session lock") = times;
                    self.logged_in();
                }
                Err(e) => {
                    conn.close();
                    // Nothing else went out on this connection.
                    return Err(match e {
                        CallError::Lost(why) => CallError::NotSent(why),
                        e => e,
                    });
                }
            }
        }
        let mut conns = self.conns.lock().expect("conns lock");
        Ok(conns.entry(addr.to_string()).or_insert(conn).clone())
    }
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// Keeps a failed login for the retry loop to report: it is the
    /// client's credentials that fail, so it holds for every request, also
    /// one whose attempt only saw a metadata lookup fail. A refused request
    /// is not kept: its own caller gets the refusal, and keeping it would
    /// report it for another request.
    fn note_fatal(&self, e: &CallError) {
        if let CallError::Denied(why) = e {
            *self.fatal.lock().expect("fatal lock") = Some(ClientError::Authentication(why.clone()));
        }
    }

    /// The credentials work (again): an earlier failed login no longer
    /// holds.
    fn logged_in(&self) {
        self.fatal.lock().expect("fatal lock").take();
    }

    fn drop_connection(&self, addr: &str, conn: &Arc<Connection<R::Conn>>) {
        let mut conns = self.conns.lock().expect("conns lock");
        if conns.get(addr).is_some_and(|c| Arc::ptr_eq(c, conn)) {
            conns.remove(addr);
        }
        conn.close();
    }
}

impl<R: Net + Clock + Spawn + Rng> Drop for Client<R> {
    fn drop(&mut self) {
        for c in self.conns.lock().expect("conns lock").values() {
            c.close();
        }
    }
}

/// Why a request got no response.
#[derive(Debug)]
enum CallError {
    /// It never left: retrying cannot duplicate anything.
    NotSent(String),
    /// It may have reached the broker.
    Lost(String),
    /// The connection could not authenticate: the request never left, and
    /// trying again will not help.
    Denied(String),
    /// The node refused the request (it lacks a role): not carried out, and
    /// trying again will not help.
    Refused(ErrorCode, String),
}

impl From<CallError> for Attempt {
    /// A connection that could not authenticate fails at once; anything
    /// else is tried again.
    fn from(e: CallError) -> Attempt {
        match e {
            CallError::Denied(why) => Attempt::Fail(ClientError::Authentication(why)),
            CallError::Refused(code, why) => Attempt::Fail(ClientError::Broker { code, message: Some(why) }),
            e => Attempt::Retry(e.to_string()),
        }
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::NotSent(s) | CallError::Lost(s) | CallError::Denied(s) => f.write_str(s),
            CallError::Refused(code, s) => write!(f, "{code}: {s}"),
        }
    }
}

/// A response slot and whoever waits on it.
type Slot = Arc<Mutex<(Option<Result<Bytes, String>>, Option<Waker>)>>;

/// Pushes for one subscription (`Err`: the connection broke), and whoever
/// waits on them.
type PushQueue = Arc<Mutex<(std::collections::VecDeque<Result<lotse_proto::DeliverPush, String>>, Option<Waker>)>>;

/// Subscriptions whose pushes arrived before anyone registered for them
/// (the reader can be faster than the subscriber) are kept for this many
/// subscriptions at most.
const ORPHANS: usize = 64;

#[derive(Default)]
struct Waiting {
    slots: BTreeMap<u32, (ApiKey, Slot)>,
    /// Registered subscriptions.
    subs: BTreeMap<u32, PushQueue>,
    /// Pushes for subscriptions not registered yet.
    orphans: BTreeMap<u32, Vec<lotse_proto::DeliverPush>>,
    /// Set once the connection broke or was closed; no new requests.
    broken: Option<String>,
    /// Wakes the reader when the connection is closed.
    reader: Option<Waker>,
}

struct Connection<C: Conn> {
    conn: C,
    waiting: Mutex<Waiting>,
    next_id: AtomicU32,
    /// The authenticated session, if it ends.
    session: Mutex<Option<auth::SessionTimes>>,
    /// Someone is authenticating again.
    renewing: AtomicBool,
}

fn deliver_push(queue: &PushQueue, push: Result<lotse_proto::DeliverPush, String>) {
    let mut q = queue.lock().expect("push lock");
    q.0.push_back(push);
    if let Some(w) = q.1.take() {
        w.wake();
    }
}

fn fill(slot: &Slot, value: Result<Bytes, String>) {
    let mut s = slot.lock().expect("slot lock");
    s.0 = Some(value);
    if let Some(w) = s.1.take() {
        w.wake();
    }
}

impl<C: Conn> Connection<C> {
    fn new(conn: C) -> Self {
        Connection {
            conn,
            waiting: Mutex::new(Waiting::default()),
            next_id: AtomicU32::new(1),
            session: Mutex::new(None),
            renewing: AtomicBool::new(false),
        }
    }

    fn close(&self) {
        self.fail("connection closed".into());
    }

    fn fail(&self, why: String) {
        let mut w = self.waiting.lock().expect("waiting lock");
        if w.broken.is_none() {
            w.broken = Some(why.clone());
        }
        for (_, (_, slot)) in std::mem::take(&mut w.slots) {
            fill(&slot, Err(why.clone()));
        }
        for (_, queue) in std::mem::take(&mut w.subs) {
            deliver_push(&queue, Err(why.clone()));
        }
        if let Some(r) = w.reader.take() {
            r.wake();
        }
    }

    async fn read_responses(self: Arc<Self>) {
        let closed = poll_fn(|cx| {
            let mut w = self.waiting.lock().expect("waiting lock");
            if w.broken.is_some() {
                return Poll::Ready(());
            }
            w.reader = Some(cx.waker().clone());
            Poll::Pending
        });
        let mut closed = pin!(closed);
        loop {
            let frame = match race(self.conn.recv(), closed.as_mut()).await {
                Some(Ok(Some(frame))) => frame,
                Some(Ok(None)) => return self.fail("the broker closed the connection".into()),
                Some(Err(e)) => return self.fail(format!("connection broke: {e}")),
                None => return,
            };
            if lotse_proto::is_push(&frame) {
                let Ok(push) = lotse_proto::decode_push(frame) else {
                    return self.fail("undecodable push".into());
                };
                self.route_push(push);
                continue;
            }
            let Ok(id) = lotse_proto::response_correlation_id(&frame) else {
                return self.fail("undecodable response".into());
            };
            let slot = self.waiting.lock().expect("waiting lock").slots.remove(&id);
            if let Some((_, slot)) = slot {
                fill(&slot, Ok(frame));
            }
        }
    }

    fn route_push(&self, push: lotse_proto::DeliverPush) {
        let mut w = self.waiting.lock().expect("waiting lock");
        match w.subs.get(&push.subscription_id) {
            Some(queue) => deliver_push(queue, Ok(push)),
            None => {
                if !w.orphans.contains_key(&push.subscription_id) && w.orphans.len() >= ORPHANS {
                    w.orphans.pop_first();
                }
                w.orphans.entry(push.subscription_id).or_default().push(push);
            }
        }
    }

    /// Receives the pushes for subscription `id` from now on, and those
    /// that arrived before. `None` if the connection is broken.
    fn register(&self, id: u32) -> Option<PushQueue> {
        let mut w = self.waiting.lock().expect("waiting lock");
        if w.broken.is_some() {
            return None;
        }
        let queue: PushQueue = Arc::default();
        for push in w.orphans.remove(&id).unwrap_or_default() {
            deliver_push(&queue, Ok(push));
        }
        w.subs.insert(id, queue.clone());
        Some(queue)
    }

    fn unregister(&self, id: u32) {
        let mut w = self.waiting.lock().expect("waiting lock");
        w.subs.remove(&id);
        w.orphans.remove(&id);
    }

    /// Sends a request nobody waits for an answer to (`Credit`, or cleanup
    /// on drop); an answer that comes anyway is dropped.
    async fn send_only(&self, req: &Request) -> Result<(), String> {
        if let Some(why) = &self.waiting.lock().expect("waiting lock").broken {
            return Err(why.clone());
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).max(1);
        self.conn.send(lotse_proto::encode_request(id, req)).await.map_err(|e| e.to_string())
    }

    async fn call<T: Clock>(&self, req: &Request, clock: &T, timeout: Duration) -> Result<Response, CallError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).max(1);
        let slot: Slot = Arc::new(Mutex::new((None, None)));
        {
            let mut w = self.waiting.lock().expect("waiting lock");
            if let Some(why) = &w.broken {
                return Err(CallError::NotSent(why.clone()));
            }
            w.slots.insert(id, (req.api_key(), slot.clone()));
        }
        if let Err(e) = self.conn.send(lotse_proto::encode_request(id, req)).await {
            self.waiting.lock().expect("waiting lock").slots.remove(&id);
            // A failed send may still have put bytes on the wire.
            return Err(CallError::Lost(format!("send failed: {e}")));
        }
        let answer = poll_fn(|cx| {
            let mut s = slot.lock().expect("slot lock");
            match s.0.take() {
                Some(v) => Poll::Ready(v),
                None => {
                    s.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        });
        let frame = match race(answer, clock.sleep(timeout)).await {
            Some(Ok(frame)) => frame,
            Some(Err(why)) => return Err(CallError::Lost(why)),
            None => {
                self.waiting.lock().expect("waiting lock").slots.remove(&id);
                return Err(CallError::Lost(format!("no response within {timeout:?}")));
            }
        };
        match lotse_proto::decode_response(frame, req.api_key()) {
            // Not authenticated, or the session ended: sending again with the
            // same credentials gets the same answer.
            Ok((_, Response::Error(ErrorCode::UNAUTHORIZED))) => {
                Err(CallError::Denied("the node requires authentication".into()))
            }
            Ok((_, Response::Error(code))) => Err(CallError::NotSent(format!("refused: {code}"))),
            // Refused as a whole, with a reason: not carried out, and the
            // same request would be refused again.
            Ok((_, Response::Refused { code, message })) => Err(CallError::Refused(code, message)),
            Ok((_, resp)) => Ok(resp),
            Err(e) => Err(CallError::Lost(format!("undecodable response: {e}"))),
        }
    }
}

/// An open transaction, from [`Client::begin_transaction`]. End it with
/// [`Transaction::commit`] or [`Transaction::abort`]; dropped without either,
/// the coordinator aborts it after the transaction timeout.
pub struct Transaction<'a, R: Net + Clock + Spawn + Rng> {
    client: &'a Client<R>,
    _turn: AsyncGuard<'a>,
    txn_id: String,
    producer_id: u64,
    epoch: u16,
    next: BTreeMap<(String, u32), i32>,
    added: std::collections::BTreeSet<(String, u32)>,
    /// Offsets were sent: the coordinator may hold them although no
    /// partition was added. The transaction must then be ended, or the
    /// offsets stay pending until it times out and block the group.
    coordinator_may_hold: bool,
    /// Why only an abort is left.
    poisoned: Option<String>,
    ended: bool,
}

impl<R: Net + Clock + Spawn + Rng> Drop for Transaction<'_, R> {
    fn drop(&mut self) {
        // Left open: the next transaction must not continue this one. A
        // fresh producer id makes the coordinator abort it first.
        if !self.ended {
            *self.client.txn_producer.lock().expect("txn lock") = None;
        }
    }
}

impl<R: Net + Clock + Spawn + Rng> fmt::Debug for Transaction<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("txn_id", &self.txn_id)
            .field("producer_id", &self.producer_id)
            .field("epoch", &self.epoch)
            .finish()
    }
}

impl<R: Net + Clock + Spawn + Rng> Transaction<'_, R> {
    /// Appends a batch to a partition within the transaction; returns the
    /// offset of its first record. Read_committed consumers see it only once
    /// the transaction commits.
    pub async fn produce(&mut self, stream: &str, partition: u32, batch: Bytes) -> Result<u64, ClientError> {
        if let Some(why) = &self.poisoned {
            return Err(ClientError::MustAbort(why.clone()));
        }
        let c = self.client;
        let key = (stream.to_string(), partition);
        self.add(stream, partition).await?;
        let corrupt = |e: lotse_record::Error| ClientError::Broker {
            code: ErrorCode::CORRUPT_BATCH,
            message: Some(e.to_string()),
        };
        let records = lotse_record::Batch::parse(&batch).map_err(corrupt)?.0.record_count();
        let seq = self.next.get(&key).copied().unwrap_or(0);
        let mut stamped = bytes::BytesMut::from(&batch[..]);
        lotse_record::set_producer(&mut stamped, self.producer_id, self.epoch, seq).map_err(corrupt)?;
        lotse_record::set_transactional(&mut stamped, true).map_err(corrupt)?;
        let stamped = stamped.freeze();
        let txn_id = self.txn_id.clone();
        // Until the outcome is known the sequence is in doubt: a produce
        // dropped meanwhile leaves the transaction to be aborted, rather
        // than let the next batch take the same sequence.
        self.poisoned = Some("a produce was dropped before its outcome was known".into());
        let res = c.retrying(|| c.produce_once(stream, partition, stamped.clone(), true, Some(txn_id.clone()))).await;
        self.poisoned = None;
        match &res {
            Ok(_) => {
                self.next.insert(key, seq.wrapping_add(records as i32));
            }
            Err(e) => self.poison(e),
        }
        res
    }

    /// Adds a partition (a queue: partition 0) to the transaction once.
    pub(crate) async fn add(&mut self, name: &str, partition: u32) -> Result<(), ClientError> {
        if let Some(why) = &self.poisoned {
            return Err(ClientError::MustAbort(why.clone()));
        }
        let key = (name.to_string(), partition);
        if !self.added.contains(&key) {
            let res = self.client.retrying(|| self.add_partition_once(name, partition)).await;
            if let Err(e) = res {
                self.poison(&e);
                return Err(e);
            }
            self.added.insert(key);
        }
        Ok(())
    }

    /// After an error, the transaction can only be aborted, except when the
    /// batch was definitely not written and nothing else went wrong.
    pub(crate) fn poison(&mut self, e: &ClientError) {
        if !matches!(e, ClientError::GaveUp(..) | ClientError::UnknownStream(..)) {
            self.poisoned = Some(e.to_string());
        }
    }

    async fn add_partition_once(&self, stream: &str, partition: u32) -> Result<(), Attempt> {
        let c = self.client;
        let (route, _) = c.route(stream, partition).await?;
        let req = Request::AddPartitionsToTxn(lotse_proto::AddPartitionsToTxnRequest {
            txn_id: self.txn_id.clone(),
            tenant: c.cfg.tenant.clone(),
            producer_id: self.producer_id,
            producer_epoch: self.epoch,
            resources: vec![lotse_proto::TxnResource { resource_id: route.resource_id, partitions: vec![partition] }],
        });
        let Response::AddPartitionsToTxn(r) = c.call_coordinator(&self.txn_id, &req).await? else {
            return Err(Attempt::Retry("unexpected response".into()));
        };
        let error = r.resources.first().and_then(|r| r.partitions.first()).map_or(ErrorCode::NONE, |p| p.error);
        match error {
            ErrorCode::NONE => Ok(()),
            code if code.retriable() => {
                c.forget_coordinator();
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Commits: once this returns, the decision is durable and every
    /// partition will show the transaction to read_committed consumers.
    pub async fn commit(self) -> Result<(), ClientError> {
        if let Some(why) = &self.poisoned {
            return Err(ClientError::MustAbort(why.clone()));
        }
        self.end(true).await
    }

    /// Aborts: none of the transaction's batches will be visible to
    /// read_committed consumers.
    pub async fn abort(self) -> Result<(), ClientError> {
        self.end(false).await
    }

    async fn end(mut self, commit: bool) -> Result<(), ClientError> {
        let c = self.client;
        self.ended = true;
        if self.added.is_empty() && !self.coordinator_may_hold {
            return Ok(());
        }
        let req = Request::EndTxn(lotse_proto::EndTxnRequest {
            txn_id: self.txn_id.clone(),
            tenant: c.cfg.tenant.clone(),
            producer_id: self.producer_id,
            producer_epoch: self.epoch,
            commit,
        });
        let res = c
            .retrying(|| async {
                let resp = match c.call_coordinator(&self.txn_id, &req).await {
                    // A lost request may have been decided: retry as maybe done.
                    Err(Attempt::Retry(why)) => return Err(Attempt::RetryMaybeDone(why)),
                    other => other?,
                };
                match resp {
                    Response::EndTxn(r) if r.error == ErrorCode::NONE => Ok((r.producer_id, r.producer_epoch)),
                    Response::EndTxn(r) if r.error.retriable() => {
                        c.forget_coordinator();
                        // The coordinator may have decided before the answer was lost.
                        Err(Attempt::RetryMaybeDone(r.error.to_string()))
                    }
                    Response::EndTxn(r) => Err(Attempt::Fail(ClientError::Broker { code: r.error, message: None })),
                    other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
                }
            })
            .await;
        // The next transaction uses the raised epoch; after a failure, get a
        // fresh one (which also aborts whatever is left open).
        *c.txn_producer.lock().expect("txn lock") = res.as_ref().ok().copied();
        res.map(|_| ())
    }
}

/// A lock held across awaits: one produce per partition at a time, so
/// sequence numbers reach the leader in order.
#[derive(Default)]
struct AsyncLock {
    state: Mutex<(bool, Vec<Waker>)>,
}

struct AsyncGuard<'a>(&'a AsyncLock);

impl AsyncLock {
    async fn lock(&self) -> AsyncGuard<'_> {
        poll_fn(|cx| {
            let mut s = self.state.lock().expect("lock state");
            if s.0 {
                s.1.push(cx.waker().clone());
                Poll::Pending
            } else {
                s.0 = true;
                Poll::Ready(())
            }
        })
        .await;
        AsyncGuard(self)
    }
}

impl Drop for AsyncGuard<'_> {
    fn drop(&mut self) {
        let waiters = {
            let mut s = self.0.state.lock().expect("lock state");
            s.0 = false;
            std::mem::take(&mut s.1)
        };
        waiters.into_iter().for_each(Waker::wake);
    }
}

/// Runs `fut` until it completes or `cancel` does; `None` in the second case.
async fn race<F: Future, G: Future>(fut: F, cancel: G) -> Option<F::Output> {
    let mut fut = pin!(fut);
    let mut cancel = pin!(cancel);
    poll_fn(|cx| {
        if let Poll::Ready(v) = fut.as_mut().poll(cx) {
            return Poll::Ready(Some(v));
        }
        if cancel.as_mut().poll(cx).is_ready() {
            return Poll::Ready(None);
        }
        Poll::Pending
    })
    .await
}

/// Adds what one broker knows to `merged`, keeping per name the newest
/// resource: a name deleted and created again has a new, higher id (ids
/// grow with the metadata log), and a broker that lags may still report the
/// old one.
fn merge_newest(merged: &mut Vec<lotse_proto::ResourceMetadata>, more: Vec<lotse_proto::ResourceMetadata>) {
    for r in more.into_iter().filter(|r| r.error == ErrorCode::NONE) {
        match merged.iter_mut().find(|x| x.name == r.name) {
            Some(x) if x.resource_id < r.resource_id => *x = r,
            Some(_) => {}
            None => merged.push(r),
        }
    }
}

#[cfg(test)]
mod tests;

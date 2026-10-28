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
//! A `Produce` whose outcome is unknown (the connection broke after sending,
//! or `REQUEST_TIMED_OUT`) is **not** retried: without idempotent producers
//! a retry could write the batch twice. The caller gets
//! [`ClientError::Unknown`] and decides.

#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{
    ApiKey, ErrorCode, FetchPartition, FetchRequest, FetchResource, MetadataRequest, MetadataResponse,
    ProducePartition, ProduceRequest, ProduceResource, Request, Response, isolation,
};
use lotse_rt::{Clock, Conn, Net, Spawn};

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
    /// A produce may or may not have been written. Read the partition to
    /// find out, or produce again and accept a possible duplicate.
    #[error("outcome unknown: {0}")]
    Unknown(String),
    /// No broker could serve the request before the retry time ran out.
    #[error("gave up after {0:?}: {1}")]
    GaveUp(Duration, String),
    /// The stream does not exist, or has no such partition.
    #[error("unknown stream {0} or partition {1}")]
    UnknownStream(String, u32),
}

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
        }
    }
}

/// Records read from one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    /// Whole record batches, control batches included; may be empty.
    pub records: Bytes,
    /// Offsets below this are committed.
    pub high_watermark: u64,
    /// First offset still in the partition.
    pub log_start_offset: u64,
}

/// One partition's place in the cluster, as metadata last said.
#[derive(Debug, Clone, Copy)]
struct Route {
    resource_id: u64,
    leader: u32,
    term: u64,
}

/// A client. Cheap to share behind an `Arc`; every method takes `&self`.
pub struct Client<R: Net + Clock + Spawn> {
    rt: R,
    cfg: ClientConfig,
    conns: Mutex<BTreeMap<String, Arc<Connection<R::Conn>>>>,
    brokers: Mutex<BTreeMap<u32, String>>,
    routes: Mutex<BTreeMap<(String, u32), Route>>,
}

impl<R: Net + Clock + Spawn> fmt::Debug for Client<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").field("bootstrap", &self.cfg.bootstrap).finish()
    }
}

/// Why one attempt failed.
enum Attempt {
    /// Try again (after refreshing metadata).
    Retry(String),
    /// Give up with this error.
    Fail(ClientError),
}

impl<R: Net + Clock + Spawn> Client<R> {
    /// A client; it connects on first use.
    pub fn new(rt: R, cfg: ClientConfig) -> Self {
        Client {
            rt,
            cfg,
            conns: Mutex::new(BTreeMap::new()),
            brokers: Mutex::new(BTreeMap::new()),
            routes: Mutex::new(BTreeMap::new()),
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
                    Err(e) => why = format!("{addr}: {e}"),
                }
            }
            if self.rt.now() - start >= self.cfg.retry_for {
                return Err(ClientError::GaveUp(self.cfg.retry_for, why));
            }
            self.rt.sleep(self.cfg.backoff).await;
        }
    }

    fn learn(&self, m: &MetadataResponse) {
        let mut brokers = self.brokers.lock().expect("brokers lock");
        for b in &m.brokers {
            brokers.insert(b.node_id, format!("{}:{}", b.host, b.port));
        }
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
                let m = self.metadata(&[stream]).await.map_err(Attempt::Fail)?;
                let res = m.resources.iter().find(|r| r.name == stream && r.error == ErrorCode::NONE);
                let Some(p) = res.and_then(|r| r.partitions.iter().find(|p| p.partition == partition)) else {
                    return Err(Attempt::Fail(ClientError::UnknownStream(stream.into(), partition)));
                };
                if p.leader == 0 {
                    return Err(Attempt::Retry("the partition has no leader".into()));
                }
                Route { resource_id: res.expect("found").resource_id, leader: p.leader, term: p.leader_term }
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
        loop {
            match attempt().await {
                Ok(v) => return Ok(v),
                Err(Attempt::Fail(e)) => return Err(e),
                Err(Attempt::Retry(why)) => {
                    if self.rt.now() - start >= self.cfg.retry_for {
                        return Err(ClientError::GaveUp(self.cfg.retry_for, why));
                    }
                    self.rt.sleep(self.cfg.backoff).await;
                }
            }
        }
    }

    /// Appends one record batch (built with `lotse_record::BatchBuilder`) to
    /// a partition. Returns the offset of its first record once the batch is
    /// committed.
    pub async fn produce(&self, stream: &str, partition: u32, batch: Bytes) -> Result<u64, ClientError> {
        self.retrying(|| self.produce_once(stream, partition, batch.clone())).await
    }

    async fn produce_once(&self, stream: &str, partition: u32, batch: Bytes) -> Result<u64, Attempt> {
        let (route, addr) = self.route(stream, partition).await?;
        let req = Request::Produce(ProduceRequest {
            txn_id: None,
            timeout_ms: u32::try_from(self.cfg.produce_timeout.as_millis()).unwrap_or(u32::MAX),
            resources: vec![ProduceResource {
                resource_id: route.resource_id,
                partitions: vec![ProducePartition { partition, batch }],
            }],
        });
        let resp = match self.call(&addr, &req).await {
            Ok(Response::Produce(r)) => r,
            Ok(other) => return Err(Attempt::Fail(ClientError::Unknown(format!("unexpected response {other:?}")))),
            // Not sent at all: safe to try again.
            Err(CallError::NotSent(why)) => {
                self.forget(stream, partition);
                return Err(Attempt::Retry(why));
            }
            Err(CallError::Lost(why)) => return Err(Attempt::Fail(ClientError::Unknown(why))),
        };
        let p = resp
            .resources
            .first()
            .and_then(|r| r.partitions.first())
            .ok_or_else(|| Attempt::Fail(ClientError::Unknown("empty produce response".into())))?;
        match p.error {
            ErrorCode::NONE => Ok(p.base_offset),
            // Refused before it was proposed, or definitely not written.
            ErrorCode::NOT_LEADER | ErrorCode::FENCED_LEADER_TERM => {
                self.redirect(stream, partition, p.leader_hint);
                Err(Attempt::Retry(format!("{}", p.error)))
            }
            ErrorCode::REQUEST_TIMED_OUT => {
                Err(Attempt::Fail(ClientError::Unknown(p.error_msg.clone().unwrap_or_else(|| p.error.to_string()))))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: p.error_msg.clone() })),
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
        self.retrying(|| self.fetch_once(stream, partition, offset, max_wait)).await
    }

    async fn fetch_once(
        &self,
        stream: &str,
        partition: u32,
        offset: u64,
        max_wait: Duration,
    ) -> Result<Fetched, Attempt> {
        let (route, addr) = self.route(stream, partition).await?;
        let req = Request::Fetch(FetchRequest {
            max_wait_ms: u32::try_from(max_wait.as_millis()).unwrap_or(u32::MAX),
            min_bytes: 1,
            max_bytes: 4 << 20,
            isolation: isolation::COMMITTED,
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
                return Err(Attempt::Retry(e.to_string()));
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
                log_start_offset: p.log_start_offset,
            }),
            code if code.retriable() => {
                self.redirect(stream, partition, p.leader_hint);
                Err(Attempt::Retry(code.to_string()))
            }
            code => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
        }
    }

    /// Sends one request to `addr` and waits for its response.
    async fn call(&self, addr: &str, req: &Request) -> Result<Response, CallError> {
        let conn = self.connection(addr).await?;
        let res = conn.call(req, &self.rt, self.cfg.request_timeout).await;
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
        if let Some(c) = self.conns.lock().expect("conns lock").get(addr) {
            return Ok(c.clone());
        }
        let raw = match race(self.rt.connect(addr), self.rt.sleep(self.cfg.request_timeout)).await {
            Some(Ok(c)) => c,
            Some(Err(e)) => return Err(CallError::NotSent(format!("cannot connect to {addr}: {e}"))),
            None => return Err(CallError::NotSent(format!("connecting to {addr} timed out"))),
        };
        let conn = Arc::new(Connection::new(raw));
        // The reader ends when the connection breaks or is closed.
        drop(self.rt.spawn(conn.clone().read_responses()));
        let mut conns = self.conns.lock().expect("conns lock");
        Ok(conns.entry(addr.to_string()).or_insert(conn).clone())
    }
}

impl<R: Net + Clock + Spawn> Drop for Client<R> {
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
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::NotSent(s) | CallError::Lost(s) => f.write_str(s),
        }
    }
}

/// A response slot and whoever waits on it.
type Slot = Arc<Mutex<(Option<Result<Bytes, String>>, Option<Waker>)>>;

#[derive(Default)]
struct Waiting {
    slots: BTreeMap<u32, (ApiKey, Slot)>,
    /// Set once the connection broke or was closed; no new requests.
    broken: Option<String>,
    /// Wakes the reader when the connection is closed.
    reader: Option<Waker>,
}

struct Connection<C: Conn> {
    conn: C,
    waiting: Mutex<Waiting>,
    next_id: AtomicU32,
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
        Connection { conn, waiting: Mutex::new(Waiting::default()), next_id: AtomicU32::new(1) }
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
            let Ok(id) = lotse_proto::response_correlation_id(&frame) else {
                return self.fail("undecodable response".into());
            };
            let slot = self.waiting.lock().expect("waiting lock").slots.remove(&id);
            if let Some((_, slot)) = slot {
                fill(&slot, Ok(frame));
            }
        }
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
            Ok((_, Response::Error(code))) => Err(CallError::NotSent(format!("refused: {code}"))),
            Ok((_, resp)) => Ok(resp),
            Err(e) => Err(CallError::Lost(format!("undecodable response: {e}"))),
        }
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

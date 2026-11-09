//! A simulated network with the guarantees of TCP and its failures.
//!
//! Each connection is two pipes, one per direction. A frame arrives whole,
//! in order and after a seeded latency, or not at all: blocking two hosts
//! (a partition) or isolating one (a crash) breaks their connections, drops
//! whatever was still in flight and refuses new connections, exactly what a
//! TCP user observes. Delivery times and latency derive from the seed.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::Duration;

use bytes::Bytes;
use lotse_rt::{CertInfo, Instant, Link, MAX_FRAME, PeerIdentity, Rng};

use crate::SimRng;
use crate::executor::Executor;

struct Pipe {
    from: String,
    to: String,
    /// In send order, each with its delivery time. The reader only takes the
    /// front, so a frame never overtakes an earlier one, whatever its latency.
    frames: VecDeque<(Instant, Bytes)>,
    reader: Option<Waker>,
    /// The sending end is gone; the reader sees the end after draining.
    closed: bool,
    /// The connection broke: both ends see an error, in-flight frames are lost.
    broken: bool,
}

struct ListenerState {
    host: String,
    link: Link,
    /// What the listening end proves (its certificate).
    identity: Option<PeerIdentity>,
    cert: Option<CertInfo>,
    backlog: VecDeque<(SimConn, String)>,
    acceptor: Option<Waker>,
}

struct NetState {
    listeners: BTreeMap<String, ListenerState>,
    pipes: BTreeMap<u64, Pipe>,
    next_pipe: u64,
    next_port: u32,
    /// Host pairs that cannot reach each other, stored both ways.
    blocked: BTreeSet<(String, String)>,
    /// Hosts cut off from everyone.
    isolated: BTreeSet<String>,
    max_latency: Duration,
    /// Whether connections carry proved identities, as with TLS.
    verify: bool,
}

impl NetState {
    fn cut(&self, a: &str, b: &str) -> bool {
        self.isolated.contains(a) || self.isolated.contains(b) || self.blocked.contains(&(a.to_owned(), b.to_owned()))
    }

    fn break_where(&mut self, mut hit: impl FnMut(&Pipe) -> bool) {
        for p in self.pipes.values_mut() {
            if !p.broken && hit(p) {
                p.broken = true;
                p.frames.clear(); // never readable once broken; free them
                if let Some(w) = p.reader.take() {
                    w.wake();
                }
            }
        }
    }
}

/// The network shared by every node of a simulation.
#[derive(Clone)]
pub struct SimNet {
    state: Arc<Mutex<NetState>>,
    exec: Executor,
    rng: SimRng,
}

impl fmt::Debug for SimNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.state.lock().expect("net lock");
        f.debug_struct("SimNet").field("listeners", &s.listeners.len()).field("pipes", &s.pipes.len()).finish()
    }
}

fn host_of(addr: &str) -> &str {
    addr.rsplit_once(':').map_or(addr, |(h, _)| h)
}

impl SimNet {
    pub(crate) fn new(exec: Executor, rng: SimRng) -> Self {
        SimNet {
            state: Arc::new(Mutex::new(NetState {
                listeners: BTreeMap::new(),
                pipes: BTreeMap::new(),
                next_pipe: 0,
                next_port: 40_000,
                blocked: BTreeSet::new(),
                isolated: BTreeSet::new(),
                max_latency: Duration::from_millis(5),
                verify: false,
            })),
            exec,
            rng,
        }
    }

    /// Frames take between zero and `max` to arrive (default 5 ms).
    pub fn set_max_latency(&self, max: Duration) {
        self.state.lock().expect("net lock").max_latency = max;
    }

    /// Connections carry the identities the runtimes were given
    /// ([`crate::SimRuntime::with_identity`]) and are refused when TLS would
    /// refuse them: a node link needs a node identity on both ends, a client
    /// link needs one on the server. Off by default (plain TCP).
    pub fn verify_peers(&self, on: bool) {
        self.state.lock().expect("net lock").verify = on;
    }

    /// Whether connections carry proved identities.
    pub fn verifies_peers(&self) -> bool {
        self.state.lock().expect("net lock").verify
    }

    /// Hosts `a` and `b` can no longer reach each other: their connections
    /// break and new ones are refused.
    pub fn block(&self, a: &str, b: &str) {
        let mut s = self.state.lock().expect("net lock");
        s.blocked.insert((a.to_owned(), b.to_owned()));
        s.blocked.insert((b.to_owned(), a.to_owned()));
        s.break_where(|p| (p.from == a && p.to == b) || (p.from == b && p.to == a));
    }

    /// Host `h` is cut off from everyone (a crash or a pulled cable).
    pub fn isolate(&self, h: &str) {
        let mut s = self.state.lock().expect("net lock");
        s.isolated.insert(h.to_owned());
        s.break_where(|p| p.from == h || p.to == h);
    }

    /// Host `h` is reachable again (connections must be made anew).
    pub fn reconnect(&self, h: &str) {
        self.state.lock().expect("net lock").isolated.remove(h);
    }

    /// Every block and isolation is lifted.
    pub fn heal(&self) {
        let mut s = self.state.lock().expect("net lock");
        s.blocked.clear();
        s.isolated.clear();
    }

    pub(crate) fn listen(
        &self,
        host: &str,
        identity: Option<PeerIdentity>,
        cert: Option<CertInfo>,
        addr: &str,
        link: Link,
    ) -> io::Result<SimListener> {
        let mut s = self.state.lock().expect("net lock");
        if host_of(addr) != host {
            return Err(io::Error::new(io::ErrorKind::AddrNotAvailable, format!("{host} cannot listen on {addr}")));
        }
        let addr = if addr.ends_with(":0") {
            s.next_port += 1;
            format!("{host}:{}", s.next_port)
        } else {
            addr.to_owned()
        };
        if s.listeners.contains_key(&addr) {
            return Err(io::Error::new(io::ErrorKind::AddrInUse, format!("{addr} is in use")));
        }
        s.listeners.insert(
            addr.clone(),
            ListenerState { host: host.to_owned(), link, identity, cert, backlog: VecDeque::new(), acceptor: None },
        );
        Ok(SimListener { net: self.clone(), addr })
    }

    pub(crate) fn connect(
        &self,
        host: &str,
        identity: Option<PeerIdentity>,
        cert: Option<CertInfo>,
        addr: &str,
        link: Link,
    ) -> io::Result<SimConn> {
        let mut s = self.state.lock().expect("net lock");
        let Some((server, server_link, server_identity, server_cert)) =
            s.listeners.get(addr).map(|l| (l.host.clone(), l.link, l.identity.clone(), l.cert.clone()))
        else {
            return Err(io::Error::new(io::ErrorKind::ConnectionRefused, format!("nothing listens on {addr}")));
        };
        if s.cut(host, &server) {
            return Err(io::Error::new(io::ErrorKind::TimedOut, format!("{addr} is unreachable from {host}")));
        }
        // The handshake: what each end demands of the other's certificate.
        let (client_sees, server_sees) = if s.verify {
            let is_node = |i: &Option<PeerIdentity>| matches!(i, Some(PeerIdentity::Node(_)));
            let refused = match (link, server_link) {
                (Link::Node, _) => !is_node(&server_identity),
                (Link::Client, _) => server_identity.is_none(),
            } || (server_link == Link::Node && !is_node(&identity));
            if refused {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("the TLS handshake with {addr} failed: a certificate is missing or of the wrong kind"),
                ));
            }
            ((server_identity, server_cert), (identity, cert))
        } else {
            ((None, None), (None, None))
        };
        let mut pipe = |from: &str, to: &str| {
            let id = s.next_pipe;
            s.next_pipe += 1;
            s.pipes.insert(
                id,
                Pipe {
                    from: from.to_owned(),
                    to: to.to_owned(),
                    frames: VecDeque::new(),
                    reader: None,
                    closed: false,
                    broken: false,
                },
            );
            id
        };
        let up = pipe(host, &server);
        let down = pipe(&server, host);
        s.next_port += 1;
        let peer = format!("{host}:{}", s.next_port);
        let client = SimConn { net: self.clone(), tx: up, rx: down, peer: client_sees.0, cert: client_sees.1 };
        let accepted = SimConn { net: self.clone(), tx: down, rx: up, peer: server_sees.0, cert: server_sees.1 };
        let l = s.listeners.get_mut(addr).expect("checked above");
        l.backlog.push_back((accepted, peer));
        if let Some(w) = l.acceptor.take() {
            w.wake();
        }
        Ok(client)
    }
}

/// A listening socket on the simulated network.
pub struct SimListener {
    net: SimNet,
    addr: String,
}

impl fmt::Debug for SimListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimListener").field("addr", &self.addr).finish()
    }
}

impl Drop for SimListener {
    fn drop(&mut self) {
        // Connections still in the backlog lock the network when they are
        // dropped, so drop them after letting go of the lock.
        let gone = self.net.state.lock().expect("net lock").listeners.remove(&self.addr);
        drop(gone);
    }
}

impl lotse_rt::Listener for SimListener {
    type Conn = SimConn;

    fn local_addr(&self) -> String {
        self.addr.clone()
    }

    fn accept(&self) -> impl Future<Output = io::Result<(SimConn, String)>> + Send {
        poll_fn(|cx| {
            let mut s = self.net.state.lock().expect("net lock");
            let Some(l) = s.listeners.get_mut(&self.addr) else {
                return Poll::Ready(Err(io::Error::other("listener closed")));
            };
            match l.backlog.pop_front() {
                Some(c) => Poll::Ready(Ok(c)),
                None => {
                    l.acceptor = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

/// One end of a simulated connection.
pub struct SimConn {
    net: SimNet,
    tx: u64,
    rx: u64,
    peer: Option<PeerIdentity>,
    cert: Option<CertInfo>,
}

impl fmt::Debug for SimConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimConn").field("tx", &self.tx).field("rx", &self.rx).finish()
    }
}

impl Drop for SimConn {
    fn drop(&mut self) {
        let mut s = self.net.state.lock().expect("net lock");
        if let Some(p) = s.pipes.get_mut(&self.tx) {
            p.closed = true;
            if let Some(w) = p.reader.take() {
                w.wake();
            }
        }
        // Nobody reads our side any more: the peer's sends fail.
        if let Some(p) = s.pipes.get_mut(&self.rx) {
            p.broken = true;
            p.frames.clear();
        }
        let gone = |id: u64, s: &NetState| s.pipes.get(&id).is_none_or(|p| p.closed && p.broken);
        if gone(self.tx, &s) {
            s.pipes.remove(&self.tx);
        }
        if gone(self.rx, &s) {
            s.pipes.remove(&self.rx);
        }
    }
}

fn reset() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionReset, "connection broken")
}

impl lotse_rt::Conn for SimConn {
    fn peer_identity(&self) -> Option<PeerIdentity> {
        self.peer.clone()
    }

    fn peer_cert(&self) -> Option<CertInfo> {
        self.cert.clone()
    }

    fn send(&self, frame: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        let res = (|| {
            if frame.len() > MAX_FRAME {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "frame too large"));
            }
            let now = self.net.exec.now();
            let mut s = self.net.state.lock().expect("net lock");
            let latency = s.max_latency.as_nanos() as u64;
            let Some(p) = s.pipes.get(&self.tx) else { return Err(reset()) };
            if p.broken {
                return Err(reset());
            }
            let (from, to) = (p.from.clone(), p.to.clone());
            if s.cut(&from, &to) {
                s.break_where(|p| (p.from == from && p.to == to) || (p.from == to && p.to == from));
                return Err(reset());
            }
            let delay = Duration::from_nanos(self.net.rng.below(latency + 1));
            let p = s.pipes.get_mut(&self.tx).expect("checked above");
            p.frames.push_back((now + delay, frame));
            if let Some(w) = p.reader.take() {
                w.wake();
            }
            Ok(())
        })();
        std::future::ready(res)
    }

    async fn recv(&self) -> io::Result<Option<Bytes>> {
        loop {
            let wait_until = {
                let now = self.net.exec.now();
                let mut s = self.net.state.lock().expect("net lock");
                let Some(p) = s.pipes.get_mut(&self.rx) else { return Err(reset()) };
                if p.broken {
                    return Err(reset());
                }
                match p.frames.front() {
                    Some((at, _)) if *at <= now => return Ok(p.frames.pop_front().map(|f| f.1)),
                    Some((at, _)) => Some(*at),
                    None if p.closed => return Ok(None),
                    None => None,
                }
            };
            match wait_until {
                Some(at) => self.net.exec.sleep(at - self.net.exec.now()).await,
                None => {
                    poll_fn(|cx| {
                        let mut s = self.net.state.lock().expect("net lock");
                        match s.pipes.get_mut(&self.rx) {
                            Some(p) if p.frames.is_empty() && !p.closed && !p.broken => {
                                p.reader = Some(cx.waker().clone());
                                Poll::Pending
                            }
                            _ => Poll::Ready(()),
                        }
                    })
                    .await
                }
            }
        }
    }
}

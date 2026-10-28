//! The only seam between Lotse and the outside world.
//!
//! Every piece of Lotse that needs time, files, tasks, randomness or the
//! machine's state takes a
//! [`Runtime`] (or one of its parts) as a generic parameter. Production code
//! plugs in a Tokio-backed implementation; tests plug in `lotse-sim`, which
//! runs a whole cluster deterministically from a single seed.
//!
//! Rules for code above this crate:
//! - no `std::time::Instant::now`, no `std::thread`, no OS randomness
//!   (enforced by `clippy.toml`);
//! - no ambient Tokio APIs, so the same code runs under simulation.
//!
//! Implementations: `lotse-rt-tokio` in production, `lotse-sim` in tests.

#![warn(missing_docs)]

use std::fmt;
use std::future::Future;
use std::io;
use std::ops::{Add, Sub};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;

/// A point on the runtime's monotonic clock, in nanoseconds since the
/// runtime started. Under simulation this is virtual time.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Instant(u64);

impl Instant {
    /// The runtime's start.
    pub const ZERO: Instant = Instant(0);

    /// Builds an instant from nanoseconds since the runtime started.
    pub const fn from_nanos(nanos: u64) -> Self {
        Instant(nanos)
    }

    /// Nanoseconds since the runtime started.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Time elapsed from `earlier` to `self`, zero if `earlier` is later.
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        Instant(self.0.saturating_add(nanos))
    }
}

impl Sub for Instant {
    type Output = Duration;
    fn sub(self, rhs: Instant) -> Duration {
        self.saturating_duration_since(rhs)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t+{:?}", Duration::from_nanos(self.0))
    }
}

/// Monotonic time and timers.
pub trait Clock: Clone + Send + Sync + 'static {
    /// The current instant.
    fn now(&self) -> Instant;

    /// Completes once `duration` has passed on this clock.
    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send;
}

/// How a segment file is opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Open an existing file; fail if it does not exist.
    Existing,
    /// Create the file, or open it if it already exists.
    CreateOrOpen,
    /// Create a new file; fail if it already exists.
    CreateNew,
}

/// Access to the log volume.
///
/// Paths are relative to the volume root. Implementations must make
/// `rename` atomic and `remove`/`remove_dir`/`rename` durable once they
/// return.
pub trait Disk: Clone + Send + Sync + 'static {
    /// File handle type.
    type File: SegmentFile;

    /// Opens a file.
    fn open(&self, path: &Path, mode: OpenMode) -> impl Future<Output = io::Result<Self::File>> + Send;

    /// Lists the files directly inside `dir`, sorted by name.
    fn list(&self, dir: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send;

    /// Creates `dir` and its parents if missing.
    fn create_dir_all(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// Atomically renames a file.
    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes a file.
    fn remove(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;

    /// Removes an empty directory; fails if it holds anything.
    fn remove_dir(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send;
}

/// An append-only file as the log uses it.
///
/// Appended bytes are readable immediately but only survive a crash after a
/// successful [`SegmentFile::sync`]. The simulation enforces exactly this:
/// on a crash it keeps synced bytes and may keep any prefix of the rest.
pub trait SegmentFile: Send + Sync + 'static {
    /// Appends `buf` at the end and returns the position it was written at.
    fn append(&self, buf: Bytes) -> impl Future<Output = io::Result<u64>> + Send;

    /// Reads up to `len` bytes starting at `pos`. Returns fewer bytes only at
    /// the end of the file.
    fn read_at(&self, pos: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send;

    /// Current length in bytes, including unsynced appends.
    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send;

    /// Shrinks the file to `len` bytes (used to cut a torn tail on recovery).
    fn truncate(&self, len: u64) -> impl Future<Output = io::Result<()>> + Send;

    /// Makes every append so far durable.
    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send;
}

/// Spawning concurrent tasks.
pub trait Spawn: Clone + Send + Sync + 'static {
    /// Handle to a spawned task.
    type JoinHandle<T: Send + 'static>: Future<Output = T> + Send;

    /// Runs `future` concurrently with the caller. Dropping the handle
    /// detaches the task; it keeps running.
    fn spawn<F>(&self, future: F) -> Self::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

/// Randomness. Under simulation every value derives from the seed.
pub trait Rng: Clone + Send + Sync + 'static {
    /// The next 64 random bits.
    fn next_u64(&self) -> u64;

    /// A value in `0..bound`. `bound` must be greater than zero.
    fn below(&self, bound: u64) -> u64 {
        assert!(bound > 0, "bound must be greater than zero");
        // Lemire's multiply-shift; the bias is negligible for 64-bit input.
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }

    /// `true` with probability `numerator / denominator`.
    fn chance(&self, numerator: u64, denominator: u64) -> bool {
        self.below(denominator) < numerator
    }
}

/// Largest frame a connection carries (the native protocol's limit too).
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Which kind of link a listener or connection is. With TLS the two differ
/// in what the other end must show: a node link needs a node certificate on
/// both ends; a client link needs the server's certificate to match the
/// address dialled, and takes a client certificate if one is offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Link {
    /// Node to node (Raft and replication).
    Node,
    /// Client to node: the native protocol, also when a node is the client.
    Client,
}

/// Who is at the other end of a connection, as its verified certificate
/// names it.
///
/// A node certificate carries the URI `lotse://node/<id>` among its subject
/// alternative names; any other certificate names a principal by its
/// subject's common name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PeerIdentity {
    /// A broker node of the cluster.
    Node(u64),
    /// A client principal.
    Principal(String),
}

impl PeerIdentity {
    /// The URI prefix that marks a node certificate.
    pub const NODE_URI: &'static str = "lotse://node/";

    /// The node id a certificate's URI names, if it is a node URI.
    pub fn node_from_uri(uri: &str) -> Option<u64> {
        let id = uri.strip_prefix(Self::NODE_URI)?;
        // Only the canonical decimal form: no sign, no leading zeros, so one
        // node has exactly one name.
        if id.is_empty() || (id.len() > 1 && id.starts_with('0')) || !id.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        id.parse().ok()
    }
}

impl fmt::Display for PeerIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PeerIdentity::Node(id) => write!(f, "node {id}"),
            PeerIdentity::Principal(p) => write!(f, "{p}"),
        }
    }
}

/// Connections between nodes and to clients.
///
/// A connection carries whole frames: each frame arrives complete and in
/// order, or the connection breaks and both ends see an error. Nothing in
/// between: a frame is never cut, duplicated or reordered. That is what TCP
/// with length prefixes gives, and what the simulation reproduces; lost and
/// reordered *messages* happen across reconnections, which callers handle.
///
/// A network either verifies peers ([`Net::verifies_peers`]: TLS with
/// certificates from the cluster's CA, or a simulation that hands out
/// identities) or it does not (plain TCP, only on loopback). When it does,
/// every connection's [`Conn::peer_identity`] is what the other end proved,
/// and a [`Link::Node`] connection never completes without a node
/// certificate on both ends.
pub trait Net: Clone + Send + Sync + 'static {
    /// A connection.
    type Conn: Conn;
    /// A listening socket.
    type Listener: Listener<Conn = Self::Conn>;

    /// Listens on `addr` (`host:port`) for links of kind `link`.
    fn listen(&self, addr: &str, link: Link) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// Connects to `addr` as a link of kind `link`.
    fn connect(&self, addr: &str, link: Link) -> impl Future<Output = io::Result<Self::Conn>> + Send;

    /// Whether connections carry identities the other end proved. A node
    /// that gets `false` here cannot tell its peers apart and trusts the
    /// network instead, which is only allowed on loopback.
    fn verifies_peers(&self) -> bool;
}

/// Accepts connections.
pub trait Listener: Send + Sync + 'static {
    /// A connection.
    type Conn: Conn;

    /// The address actually listened on (with the port, if 0 was asked for).
    fn local_addr(&self) -> String;

    /// The next incoming connection and the peer's address.
    fn accept(&self) -> impl Future<Output = io::Result<(Self::Conn, String)>> + Send;
}

/// One connection. Sending and receiving may run concurrently from two
/// tasks; two concurrent senders are serialised.
pub trait Conn: Send + Sync + 'static {
    /// Sends one frame of at most [`MAX_FRAME`] bytes.
    fn send(&self, frame: Bytes) -> impl Future<Output = io::Result<()>> + Send;

    /// The next frame; `Ok(None)` once the peer closed the connection cleanly.
    fn recv(&self) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;

    /// Who the other end proved to be; `None` if it showed no certificate or
    /// the network does not verify peers.
    fn peer_identity(&self) -> Option<PeerIdentity>;
}

/// What the machine a node runs on looks like right now, for metrics.
/// A value the platform cannot tell is `None`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct HostStats {
    /// CPU use of the machine since the previous call, 0 to 1.
    pub cpu: Option<f64>,
    /// Memory this process uses, bytes.
    pub memory_bytes: Option<u64>,
    /// Memory available to this process (its cgroup limit, or the
    /// machine's memory), bytes.
    pub memory_limit_bytes: Option<u64>,
    /// Free bytes on the volume that holds the node's data.
    pub disk_free_bytes: Option<u64>,
    /// Size of that volume, bytes.
    pub disk_total_bytes: Option<u64>,
}

/// Reads the state of the machine, for metrics only: nothing may decide
/// anything that safety depends on from it.
pub trait Host: Send + Sync + 'static {
    /// The machine right now.
    fn host_stats(&self) -> impl Future<Output = HostStats> + Send;
}

/// Everything a Lotse node needs from its environment.
pub trait Runtime: Clock + Disk + Spawn + Rng + Net + Host {}

impl<T: Clock + Disk + Spawn + Rng + Net + Host> Runtime for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_arithmetic_saturates() {
        let t = Instant::from_nanos(10);
        assert_eq!(t + Duration::from_nanos(5), Instant::from_nanos(15));
        assert_eq!(Instant::ZERO - t, Duration::ZERO);
        assert_eq!(t - Instant::ZERO, Duration::from_nanos(10));
        assert_eq!(Instant::from_nanos(u64::MAX) + Duration::from_secs(1), Instant::from_nanos(u64::MAX));
    }

    #[derive(Clone)]
    struct Counter(std::sync::Arc<std::sync::atomic::AtomicU64>);
    impl Rng for Counter {
        fn next_u64(&self) -> u64 {
            self.0.fetch_add(0x9E37_79B9_7F4A_7C15, std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[test]
    fn below_stays_in_range() {
        let rng = Counter(Default::default());
        for _ in 0..1000 {
            assert!(rng.below(7) < 7);
        }
    }

    #[test]
    fn a_node_uri_has_one_spelling() {
        assert_eq!(PeerIdentity::node_from_uri("lotse://node/7"), Some(7));
        assert_eq!(PeerIdentity::node_from_uri("lotse://node/0"), Some(0));
        for bad in [
            "lotse://node/",
            "lotse://node/07",
            "lotse://node/+7",
            "lotse://node/7a",
            "lotse://nodes/7",
            "lotse://node/99999999999999999999",
        ] {
            assert_eq!(PeerIdentity::node_from_uri(bad), None, "{bad}");
        }
    }
}

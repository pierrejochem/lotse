//! The only seam between Lotse and the outside world.
//!
//! Every piece of Lotse that needs time, files, tasks or randomness takes a
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
/// `rename` atomic and `remove`/`rename` durable once they return.
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

    /// Runs `future` concurrently with the caller.
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

/// Connections between nodes and to clients.
///
/// A connection carries whole frames: each frame arrives complete and in
/// order, or the connection breaks and both ends see an error. Nothing in
/// between: a frame is never cut, duplicated or reordered. That is what TCP
/// with length prefixes gives, and what the simulation reproduces; lost and
/// reordered *messages* happen across reconnections, which callers handle.
pub trait Net: Clone + Send + Sync + 'static {
    /// A connection.
    type Conn: Conn;
    /// A listening socket.
    type Listener: Listener<Conn = Self::Conn>;

    /// Listens on `addr` (`host:port`).
    fn listen(&self, addr: &str) -> impl Future<Output = io::Result<Self::Listener>> + Send;

    /// Connects to `addr`.
    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<Self::Conn>> + Send;
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
}

/// Everything a Lotse node needs from its environment.
pub trait Runtime: Clock + Disk + Spawn + Rng + Net {}

impl<T: Clock + Disk + Spawn + Rng + Net> Runtime for T {}

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
}

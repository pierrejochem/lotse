//! Deterministic simulation for Lotse.
//!
//! [`Sim::run`] executes an async test on a single thread with a virtual
//! clock, a seeded RNG and an in-memory disk that can crash. Which task runs
//! next, how long nothing happens and what a crash destroys all derive from
//! the seed, so a failing run is reproduced exactly by its seed.
//!
//! ```
//! use lotse_rt::{Clock, Rng};
//! use std::time::Duration;
//!
//! let elapsed = lotse_sim::Sim::new(42).run(|rt| async move {
//!     let start = rt.now();
//!     rt.sleep(Duration::from_secs(3600)).await; // returns at once in real time
//!     rt.now() - start
//! });
//! assert_eq!(elapsed, Duration::from_secs(3600));
//! ```

#![warn(missing_docs)]

mod disk;
mod executor;
mod net;

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_rt::{Clock, Instant, Rng, Spawn};

pub use disk::{CrashReport, SimDisk, SimFile};
pub use executor::JoinHandle;
pub use net::{SimConn, SimListener, SimNet};

/// A simulation, configured by its seed.
#[derive(Debug, Clone)]
pub struct Sim {
    seed: u64,
}

impl Sim {
    /// A simulation driven by `seed`.
    pub fn new(seed: u64) -> Self {
        Sim { seed }
    }

    /// Runs `test` to completion and returns its output. Panics with the
    /// seed in the message if the test deadlocks.
    pub fn run<F, Fut, T>(&self, test: F) -> T
    where
        F: FnOnce(SimRuntime) -> Fut,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let rng = SimRng::new(self.seed);
        let exec = executor::Executor::new(rng.clone());
        let net = SimNet::new(exec.clone(), rng.clone());
        let rt = SimRuntime { exec: exec.clone(), rng: rng.clone(), disk: SimDisk::new(rng), net, host: "sim".into() };
        exec.block_on(test(rt), self.seed)
    }
}

/// The [`lotse_rt::Runtime`] implementation used in tests.
#[derive(Clone, Debug)]
pub struct SimRuntime {
    exec: executor::Executor,
    rng: SimRng,
    disk: SimDisk,
    net: SimNet,
    host: Arc<str>,
}

impl SimRuntime {
    /// The simulated disk, for fault injection.
    pub fn disk(&self) -> &SimDisk {
        &self.disk
    }

    /// The same runtime (clock, tasks, randomness) with a disk of its own:
    /// one per simulated node, so a crash hits only that node.
    pub fn with_own_disk(&self) -> SimRuntime {
        SimRuntime { disk: SimDisk::new(self.rng.clone()), ..self.clone() }
    }

    /// A simulated node: the same clock, tasks and network, its own disk,
    /// and `host` as its name on the network (it listens on `host:port`).
    pub fn node(&self, host: &str) -> SimRuntime {
        SimRuntime { disk: SimDisk::new(self.rng.clone()), host: host.into(), ..self.clone() }
    }

    /// The network, for fault injection.
    pub fn net(&self) -> &SimNet {
        &self.net
    }

    /// This node's host name.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Simulates a power loss: file handles become stale, and unsynced data
    /// is lost or torn as the seed decides. Reopen files afterwards.
    pub fn crash(&self) -> CrashReport {
        self.disk.crash()
    }
}

impl Clock for SimRuntime {
    fn now(&self) -> Instant {
        self.exec.now()
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.exec.sleep(duration)
    }
}

impl Rng for SimRuntime {
    fn next_u64(&self) -> u64 {
        self.rng.next_u64()
    }
}

impl Spawn for SimRuntime {
    type JoinHandle<T: Send + 'static> = JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.exec.spawn(future)
    }
}

impl lotse_rt::Disk for SimRuntime {
    type File = SimFile;

    fn open(
        &self,
        path: &std::path::Path,
        mode: lotse_rt::OpenMode,
    ) -> impl Future<Output = std::io::Result<SimFile>> + Send {
        self.disk.open(path, mode)
    }
    fn list(&self, dir: &std::path::Path) -> impl Future<Output = std::io::Result<Vec<std::path::PathBuf>>> + Send {
        self.disk.list(dir)
    }
    fn create_dir_all(&self, dir: &std::path::Path) -> impl Future<Output = std::io::Result<()>> + Send {
        self.disk.create_dir_all(dir)
    }
    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> impl Future<Output = std::io::Result<()>> + Send {
        self.disk.rename(from, to)
    }
    fn remove(&self, path: &std::path::Path) -> impl Future<Output = std::io::Result<()>> + Send {
        self.disk.remove(path)
    }
}

impl lotse_rt::Net for SimRuntime {
    type Conn = SimConn;
    type Listener = SimListener;

    fn listen(&self, addr: &str) -> impl Future<Output = std::io::Result<SimListener>> + Send {
        std::future::ready(self.net.listen(&self.host, addr))
    }
    fn connect(&self, addr: &str) -> impl Future<Output = std::io::Result<SimConn>> + Send {
        std::future::ready(self.net.connect(&self.host, addr))
    }
}

/// SplitMix64, shared by everything in one simulation.
#[derive(Clone, Debug)]
pub struct SimRng(Arc<Mutex<u64>>);

impl SimRng {
    /// A generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        SimRng(Arc::new(Mutex::new(seed)))
    }
}

impl Rng for SimRng {
    fn next_u64(&self) -> u64 {
        let mut s = self.0.lock().expect("rng lock");
        *s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *s;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Seeds for a test run.
///
/// - `LOTSE_SIM_SEED=<n>` runs exactly that seed (to reproduce a failure);
/// - `LOTSE_SIM_SEEDS=<count>` runs `count` seeds (CI uses thousands);
/// - otherwise `default_count` seeds.
pub fn seeds(default_count: u64) -> Vec<u64> {
    if let Some(seed) = std::env::var("LOTSE_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![seed];
    }
    let count = std::env::var("LOTSE_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(default_count);
    (0..count).collect()
}

/// Runs `check` for every seed from [`seeds`]. On the first failure, panics
/// with the seed and the command that reproduces it.
pub fn check_seeds(default_count: u64, check: impl Fn(u64)) {
    for seed in seeds(default_count) {
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| check(seed))) {
            eprintln!("\nsimulation failed with seed {seed}; reproduce with LOTSE_SIM_SEED={seed}\n");
            resume_unwind(panic);
        }
    }
}

//! `lotse-rt` for production: Tokio, the local file system and TCP.
//!
//! [`TokioRuntime`] implements every `lotse-rt` trait. It is the one place in
//! Lotse that reads the real clock and the OS's randomness; everything above
//! it stays deterministic under simulation.
//!
//! Disk: paths are relative to a root directory. Reads and writes are
//! positional (`pread`/`pwrite`) and run on Tokio's blocking pool.
//! [`SegmentFile::sync`] is `fdatasync`. Creating, renaming and removing a
//! file also syncs its directory, which Linux needs for the change itself to
//! survive a power loss; that is what the [`Disk`] contract promises.
//!
//! Network: TCP with a 4-byte big-endian length before each frame, Nagle off.
//! TLS comes with the native protocol.

#![warn(missing_docs)]

#[cfg(not(unix))]
compile_error!("lotse-rt-tokio needs a Unix file system (positional I/O, directory fsync)");

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{self, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use lotse_rt::{
    Clock, Conn, Disk, Host, HostStats, Instant, Listener, MAX_FRAME, Net, OpenMode, Rng, SegmentFile, Spawn,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

/// The production runtime. Cheap to clone; clones share the clock, the disk
/// root and the random generator. Must be used inside a Tokio runtime.
#[derive(Debug, Clone)]
pub struct TokioRuntime {
    start: std::time::Instant,
    root: Arc<PathBuf>,
    rng: Arc<AtomicU64>,
    /// Kept between calls: CPU use is measured since the previous refresh.
    system: Arc<Mutex<Option<sysinfo::System>>>,
}

impl TokioRuntime {
    /// A runtime whose disk paths are relative to `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        // The real clock and OS randomness are read here and only here.
        #[allow(clippy::disallowed_methods)]
        let start = std::time::Instant::now();
        #[allow(clippy::disallowed_methods)]
        let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        let seed = {
            use std::hash::{BuildHasher, Hasher};
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(wall.as_nanos());
            h.write_u32(std::process::id());
            h.finish()
        };
        TokioRuntime {
            start,
            root: Arc::new(root.into()),
            rng: Arc::new(AtomicU64::new(seed)),
            system: Arc::new(Mutex::new(None)),
        }
    }

    /// The directory disk paths are relative to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, p: &Path) -> PathBuf {
        self.root.join(p)
    }
}

/// Runs blocking file work on Tokio's blocking pool.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> io::Result<T> + Send + 'static) -> io::Result<T> {
    tokio::task::spawn_blocking(f).await.map_err(io::Error::other)?
}

/// Makes a change to `dir`'s entries (a created, renamed or removed file)
/// durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
    File::open(dir)?.sync_all()
}

impl Clock for TokioRuntime {
    fn now(&self) -> Instant {
        Instant::from_nanos(u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        tokio::time::sleep(duration)
    }
}

impl Rng for TokioRuntime {
    fn next_u64(&self) -> u64 {
        // SplitMix64 over a shared counter.
        let mut z = self.rng.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Waits for a task spawned on Tokio; a panic in the task resumes here.
#[derive(Debug)]
pub struct JoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for JoinHandle<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        match Pin::new(&mut self.0).poll(cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(v),
            Poll::Ready(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Poll::Ready(Err(e)) => panic!("task did not finish: {e}"),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Spawn for TokioRuntime {
    type JoinHandle<T: Send + 'static> = JoinHandle<T>;

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle(tokio::spawn(future))
    }
}

/// A file on the local disk.
#[derive(Debug, Clone)]
pub struct TokioFile {
    file: Arc<File>,
    /// Length including unsynced appends; appends are serialised by it.
    len: Arc<Mutex<u64>>,
}

impl Disk for TokioRuntime {
    type File = TokioFile;

    fn open(&self, path: &Path, mode: OpenMode) -> impl Future<Output = io::Result<TokioFile>> + Send {
        let full = self.path(path);
        blocking(move || {
            let existed = full.exists();
            let mut o = OpenOptions::new();
            o.read(true).write(true);
            match mode {
                OpenMode::Existing => {}
                OpenMode::CreateOrOpen => {
                    o.create(true);
                }
                OpenMode::CreateNew => {
                    o.create_new(true);
                }
            }
            let file = o.open(&full)?;
            if !existed {
                sync_dir(full.parent().unwrap_or(Path::new(".")))?;
            }
            let len = file.metadata()?.len();
            Ok(TokioFile { file: Arc::new(file), len: Arc::new(Mutex::new(len)) })
        })
    }

    fn list(&self, dir: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send {
        let full = self.path(dir);
        let rel = dir.to_owned();
        blocking(move || {
            let mut out = Vec::new();
            for entry in std::fs::read_dir(&full)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    out.push(rel.join(entry.file_name()));
                }
            }
            out.sort();
            Ok(out)
        })
    }

    fn create_dir_all(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let full = self.path(dir);
        let root = self.root.as_ref().clone();
        blocking(move || {
            if full.is_dir() {
                return Ok(());
            }
            std::fs::create_dir_all(&full)?;
            // Make every new directory entry durable, up to the root.
            let mut d = full.as_path();
            while let Some(parent) = d.parent() {
                sync_dir(parent)?;
                if parent == root || !parent.starts_with(&root) {
                    break;
                }
                d = parent;
            }
            Ok(())
        })
    }

    fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let (from, to) = (self.path(from), self.path(to));
        blocking(move || {
            std::fs::rename(&from, &to)?;
            let (a, b) = (from.parent().unwrap_or(Path::new(".")), to.parent().unwrap_or(Path::new(".")));
            sync_dir(b)?;
            if a != b {
                sync_dir(a)?;
            }
            Ok(())
        })
    }

    fn remove(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let full = self.path(path);
        blocking(move || {
            std::fs::remove_file(&full)?;
            sync_dir(full.parent().unwrap_or(Path::new(".")))
        })
    }

    fn remove_dir(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send {
        let full = self.path(dir);
        blocking(move || {
            std::fs::remove_dir(&full)?;
            sync_dir(full.parent().unwrap_or(Path::new(".")))
        })
    }
}

impl SegmentFile for TokioFile {
    fn append(&self, buf: Bytes) -> impl Future<Output = io::Result<u64>> + Send {
        let this = self.clone();
        blocking(move || {
            let mut len = this.len.lock().map_err(|_| io::Error::other("file lock poisoned"))?;
            let pos = *len;
            this.file.write_all_at(&buf, pos)?;
            *len += buf.len() as u64;
            Ok(pos)
        })
    }

    fn read_at(&self, pos: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send {
        let this = self.clone();
        blocking(move || {
            let mut out = BytesMut::zeroed(len);
            let mut filled = 0;
            while filled < len {
                match this.file.read_at(&mut out[filled..], pos + filled as u64) {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            out.truncate(filled);
            Ok(out.freeze())
        })
    }

    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send {
        let len = self.len.lock().map(|l| *l).map_err(|_| io::Error::other("file lock poisoned"));
        std::future::ready(len)
    }

    fn truncate(&self, new_len: u64) -> impl Future<Output = io::Result<()>> + Send {
        let this = self.clone();
        blocking(move || {
            let mut len = this.len.lock().map_err(|_| io::Error::other("file lock poisoned"))?;
            if new_len < *len {
                this.file.set_len(new_len)?;
                *len = new_len;
            }
            Ok(())
        })
    }

    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send {
        let file = self.file.clone();
        blocking(move || file.sync_data())
    }
}

/// A TCP listener.
#[derive(Debug)]
pub struct TokioListener {
    inner: tokio::net::TcpListener,
    addr: String,
}

/// A TCP connection carrying length-prefixed frames.
#[derive(Debug)]
pub struct TokioConn {
    rd: tokio::sync::Mutex<OwnedReadHalf>,
    wr: tokio::sync::Mutex<OwnedWriteHalf>,
}

impl TokioConn {
    fn new(stream: tokio::net::TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();
        Ok(TokioConn { rd: tokio::sync::Mutex::new(rd), wr: tokio::sync::Mutex::new(wr) })
    }
}

impl Host for TokioRuntime {
    fn host_stats(&self) -> impl Future<Output = HostStats> + Send {
        let (system, root) = (Arc::clone(&self.system), Arc::clone(&self.root));
        async move {
            let stats = tokio::task::spawn_blocking(move || {
                let mut guard = system.lock().unwrap_or_else(|p| p.into_inner());
                let first = guard.is_none();
                let sys = guard.get_or_insert_with(sysinfo::System::new);
                sys.refresh_cpu_usage();
                sys.refresh_memory();
                let pid = sysinfo::get_current_pid().ok();
                if let Some(pid) = pid {
                    sys.refresh_processes_specifics(
                        sysinfo::ProcessesToUpdate::Some(&[pid]),
                        true,
                        sysinfo::ProcessRefreshKind::nothing().with_memory(),
                    );
                }
                // CPU use is measured between two refreshes: the first has none.
                let cpu = (!first).then(|| f64::from(sys.global_cpu_usage()) / 100.0);
                let limit =
                    sys.cgroup_limits().map(|c| c.total_memory).filter(|m| *m > 0).unwrap_or(sys.total_memory());
                let memory_bytes = pid.and_then(|p| sys.process(p)).map(|p| p.memory());
                let (disk_free_bytes, disk_total_bytes) = volume_of(&root);
                HostStats {
                    cpu,
                    memory_bytes,
                    memory_limit_bytes: (limit > 0).then_some(limit),
                    disk_free_bytes,
                    disk_total_bytes,
                }
            })
            .await;
            stats.unwrap_or_default()
        }
    }
}

/// Free and total bytes of the volume `dir` is on: the mounted disk with the
/// longest mount point that contains it.
fn volume_of(dir: &Path) -> (Option<u64>, Option<u64>) {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let disks = sysinfo::Disks::new_with_refreshed_list();
    let best = disks
        .list()
        .iter()
        .filter(|d| dir.starts_with(d.mount_point()))
        .max_by_key(|d| d.mount_point().as_os_str().len());
    match best {
        Some(d) => (Some(d.available_space()), Some(d.total_space())),
        None => (None, None),
    }
}

impl Net for TokioRuntime {
    type Conn = TokioConn;
    type Listener = TokioListener;

    fn listen(&self, addr: &str) -> impl Future<Output = io::Result<TokioListener>> + Send {
        let addr = addr.to_owned();
        async move {
            let inner = tokio::net::TcpListener::bind(&addr).await?;
            let addr = inner.local_addr()?.to_string();
            Ok(TokioListener { inner, addr })
        }
    }

    fn connect(&self, addr: &str) -> impl Future<Output = io::Result<TokioConn>> + Send {
        let addr = addr.to_owned();
        async move { TokioConn::new(tokio::net::TcpStream::connect(&addr).await?) }
    }
}

impl Listener for TokioListener {
    type Conn = TokioConn;

    fn local_addr(&self) -> String {
        self.addr.clone()
    }

    async fn accept(&self) -> io::Result<(TokioConn, String)> {
        let (stream, peer) = self.inner.accept().await?;
        Ok((TokioConn::new(stream)?, peer.to_string()))
    }
}

impl Conn for TokioConn {
    async fn send(&self, frame: Bytes) -> io::Result<()> {
        if frame.len() > MAX_FRAME {
            return Err(io::Error::new(ErrorKind::InvalidInput, "frame too large"));
        }
        let mut wr = self.wr.lock().await;
        wr.write_all(&(frame.len() as u32).to_be_bytes()).await?;
        wr.write_all(&frame).await?;
        wr.flush().await
    }

    async fn recv(&self) -> io::Result<Option<Bytes>> {
        let mut rd = self.rd.lock().await;
        let mut len = [0u8; 4];
        // A clean close lands between frames; anywhere else it is an error.
        let n = rd.read(&mut len).await?;
        if n == 0 {
            return Ok(None);
        }
        rd.read_exact(&mut len[n..]).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len > MAX_FRAME {
            return Err(io::Error::new(ErrorKind::InvalidData, format!("frame of {len} bytes exceeds the limit")));
        }
        let mut buf = BytesMut::zeroed(len);
        rd.read_exact(&mut buf).await?;
        Ok(Some(buf.freeze()))
    }
}

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
//! Network: TCP with a 4-byte big-endian length before each frame, Nagle off,
//! inside TLS 1.3 ([`TlsConfig`]). Plain TCP only on loopback, and only when
//! asked for ([`TokioRuntime::plaintext_on_loopback`]); a runtime given
//! neither refuses to use the network.

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
use lotse_rt::{Clock, Disk, Host, HostStats, Instant, OpenMode, Rng, SegmentFile, Spawn};

#[cfg(feature = "dev-certs")]
pub mod dev_certs;
mod http;
mod net;
mod tls;

pub use net::{TokioConn, TokioListener};
pub use tls::{TlsConfig, TlsError};

/// The production runtime. Cheap to clone; clones share the clock, the disk
/// root and the random generator. Must be used inside a Tokio runtime.
#[derive(Debug, Clone)]
pub struct TokioRuntime {
    start: std::time::Instant,
    root: Arc<PathBuf>,
    rng: Arc<AtomicU64>,
    /// Kept between calls: CPU use is measured since the previous refresh.
    system: Arc<Mutex<Option<sysinfo::System>>>,
    security: net::Security,
    /// Extra CAs trusted for HTTPS, beyond the web's roots.
    http_cas: Arc<Vec<rustls::pki_types::CertificateDer<'static>>>,
    http_tls: Arc<std::sync::OnceLock<Arc<rustls::ClientConfig>>>,
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
            security: net::Security::Unset,
            http_cas: Arc::new(Vec::new()),
            http_tls: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// The same runtime also trusting the CA certificates in `pem` for
    /// HTTPS (a private identity provider, or one in a test).
    pub fn with_http_ca(self, pem: &[u8]) -> Result<Self, TlsError> {
        use rustls::pki_types::pem::PemObject as _;
        let mut cas = (*self.http_cas).clone();
        for c in rustls::pki_types::CertificateDer::pem_slice_iter(pem) {
            cas.push(c.map_err(|e| TlsError::Pem { what: "an HTTPS CA certificate", detail: e.to_string() })?);
        }
        Ok(TokioRuntime { http_cas: Arc::new(cas), http_tls: Arc::new(std::sync::OnceLock::new()), ..self })
    }

    /// The same runtime speaking TLS 1.3 on every connection.
    pub fn with_tls(self, tls: TlsConfig) -> Self {
        TokioRuntime { security: net::Security::Tls(net::Shared::new(tls)), ..self }
    }

    /// Replaces the TLS material of this runtime and all its clones (ADR
    /// 0060): every handshake from now on uses `tls`, and every open
    /// connection whose other end showed a certificate `tls` revokes is
    /// ended. Returns how many were. Does nothing to a runtime without TLS.
    pub fn reload_tls(&self, tls: TlsConfig) -> usize {
        match &self.security {
            net::Security::Tls(shared) => shared.reload(tls),
            _ => 0,
        }
    }

    /// The same runtime speaking plain TCP, which it then allows only to and
    /// from loopback addresses: for development and tests on one machine.
    /// Nothing proves who is at the other end.
    pub fn plaintext_on_loopback(self) -> Self {
        TokioRuntime { security: net::Security::Plain, ..self }
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

    fn unix_time(&self) -> Duration {
        // The wall clock is read here and only here.
        #[allow(clippy::disallowed_methods)]
        let now = std::time::SystemTime::now();
        now.duration_since(std::time::UNIX_EPOCH).unwrap_or_default()
    }
}

impl TokioRuntime {
    fn http_tls(&self) -> io::Result<Arc<rustls::ClientConfig>> {
        match self.http_tls.get() {
            Some(c) => Ok(c.clone()),
            None => http::client_config(&self.http_cas).map(|c| self.http_tls.get_or_init(|| c).clone()),
        }
    }

    /// `POST url` with form fields, for an OpenID Connect token endpoint;
    /// the same rules as [`lotse_rt::Http::get`] (HTTPS, or HTTP on
    /// loopback; a `200` only).
    pub async fn post_form(&self, url: &str, fields: &[(&str, &str)]) -> io::Result<Bytes> {
        http::post_form(url, fields, self.http_tls()?).await
    }
}

impl lotse_rt::Http for TokioRuntime {
    fn get(&self, url: &str) -> impl Future<Output = io::Result<Bytes>> + Send {
        let url = url.to_owned();
        let tls = self.http_tls();
        async move { http::get(&url, tls?).await }
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
    /// The operating system's randomness, through ring.
    fn fill_secret(&self, buf: &mut [u8]) {
        use ring::rand::SecureRandom as _;
        ring::rand::SystemRandom::new().fill(buf).expect("the operating system's random source");
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

//! An in-memory disk that loses exactly what a real disk may lose.
//!
//! Each file tracks how much of it is synced. On [`SimDisk::crash`], every
//! file keeps its synced bytes plus a random prefix of the unsynced ones, and
//! sometimes garbage in place of the last write (a torn sector). Directory
//! operations (create, rename, remove, truncate) are durable when they
//! return, as the [`lotse_rt::Disk`] contract requires.

use std::collections::BTreeMap;
use std::fmt;
use std::future::{Future, ready};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use lotse_rt::{OpenMode, Rng, SegmentFile};

use crate::SimRng;

#[derive(Default)]
struct FileState {
    data: Vec<u8>,
    synced: usize,
}

struct DiskState {
    files: BTreeMap<PathBuf, FileState>,
    dirs: std::collections::BTreeSet<PathBuf>,
    /// Bumped on every crash; handles from an older incarnation are stale.
    incarnation: u64,
    /// Write operations left before the power goes ([`SimDisk::arm_power_loss`]).
    power_left: Option<u64>,
}

/// What a crash destroyed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CrashReport {
    /// Unsynced bytes dropped, summed over all files.
    pub bytes_lost: usize,
    /// Files whose tail was replaced with garbage.
    pub torn_files: Vec<PathBuf>,
}

/// The simulated disk. Clones share the same contents.
#[derive(Clone)]
pub struct SimDisk {
    state: Arc<Mutex<DiskState>>,
    rng: SimRng,
}

impl fmt::Debug for SimDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.state.lock().expect("disk lock");
        f.debug_struct("SimDisk").field("files", &s.files.len()).field("incarnation", &s.incarnation).finish()
    }
}

/// A handle to a file on the simulated disk.
#[derive(Debug)]
pub struct SimFile {
    disk: SimDisk,
    path: PathBuf,
    incarnation: u64,
}

impl SimDisk {
    /// An empty disk whose crashes draw from `rng`.
    pub fn new(rng: SimRng) -> Self {
        SimDisk {
            state: Arc::new(Mutex::new(DiskState {
                files: BTreeMap::new(),
                dirs: Default::default(),
                incarnation: 0,
                power_left: None,
            })),
            rng,
        }
    }

    /// Power loss. Unsynced data survives only as a random prefix, and the
    /// last unsynced write of a file may be replaced by garbage.
    pub fn crash(&self) -> CrashReport {
        let mut s = self.state.lock().expect("disk lock");
        s.incarnation += 1;
        s.power_left = None;
        let mut report = CrashReport::default();
        for (path, f) in s.files.iter_mut() {
            let unsynced = f.data.len() - f.synced;
            if unsynced == 0 {
                continue;
            }
            let keep = self.rng.below(unsynced as u64 + 1) as usize;
            report.bytes_lost += unsynced - keep;
            f.data.truncate(f.synced + keep);
            if keep > 0 && self.rng.chance(1, 4) {
                // Torn write: the tail of what survived is garbage.
                let garbage = 1 + self.rng.below(keep.min(512) as u64) as usize;
                let len = f.data.len();
                for b in &mut f.data[len - garbage..] {
                    *b = self.rng.next_u64() as u8;
                }
                report.torn_files.push(path.clone());
            }
            f.synced = f.data.len();
        }
        report
    }

    /// The power goes after `ops` more write operations (append, truncate,
    /// sync, create, rename, remove): from then on every operation fails, as
    /// if the process had died mid-way. Call [`SimDisk::crash`] afterwards to
    /// decide what survived.
    pub fn arm_power_loss(&self, ops: u64) {
        self.state.lock().expect("disk lock").power_left = Some(ops);
    }

    /// `true` once an armed power loss has happened.
    pub fn power_lost(&self) -> bool {
        self.state.lock().expect("disk lock").power_left == Some(0)
    }

    /// Flips one bit in a file, synced or not: media corruption that the
    /// checksums must catch.
    pub fn flip_bit(&self, path: &Path, byte: usize, bit: u8) -> io::Result<()> {
        let mut s = self.state.lock().expect("disk lock");
        let f = s.files.get_mut(path).ok_or_else(|| not_found(path))?;
        let b = f.data.get_mut(byte).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "byte out of range"))?;
        *b ^= 1 << (bit % 8);
        Ok(())
    }

    /// Current length of a file, for assertions.
    pub fn file_len(&self, path: &Path) -> Option<usize> {
        self.state.lock().expect("disk lock").files.get(path).map(|f| f.data.len())
    }

    pub(crate) fn open(&self, path: &Path, mode: OpenMode) -> impl Future<Output = io::Result<SimFile>> + Send + use<> {
        let res = (|| {
            let mut s = self.state.lock().expect("disk lock");
            if s.power_left == Some(0) {
                return Err(io::Error::other("simulated power loss"));
            }
            let exists = s.files.contains_key(path);
            if !exists && mode != OpenMode::Existing {
                spend(&mut s)?;
            }
            match (mode, exists) {
                (OpenMode::Existing, false) => return Err(not_found(path)),
                (OpenMode::CreateNew, true) => {
                    return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("{} exists", path.display())));
                }
                _ => {}
            }
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                if !s.dirs.contains(parent) {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("directory {} missing", parent.display()),
                    ));
                }
            }
            s.files.entry(path.to_owned()).or_default();
            Ok(SimFile { disk: self.clone(), path: path.to_owned(), incarnation: s.incarnation })
        })();
        ready(res)
    }

    pub(crate) fn list(&self, dir: &Path) -> impl Future<Output = io::Result<Vec<PathBuf>>> + Send + use<> {
        let s = self.state.lock().expect("disk lock");
        let res = if s.dirs.contains(dir) {
            Ok(s.files.keys().filter(|p| p.parent() == Some(dir)).cloned().collect())
        } else {
            Err(not_found(dir))
        };
        ready(res)
    }

    pub(crate) fn create_dir_all(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let mut s = self.state.lock().expect("disk lock");
        for d in dir.ancestors().filter(|d| !d.as_os_str().is_empty()) {
            s.dirs.insert(d.to_owned());
        }
        ready(Ok(()))
    }

    pub(crate) fn rename(&self, from: &Path, to: &Path) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let mut s = self.state.lock().expect("disk lock");
        if let Err(e) = spend(&mut s) {
            return ready(Err(e));
        }
        let res = match s.files.remove(from) {
            Some(f) => {
                s.files.insert(to.to_owned(), f);
                Ok(())
            }
            None => Err(not_found(from)),
        };
        ready(res)
    }

    pub(crate) fn remove(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let mut s = self.state.lock().expect("disk lock");
        if let Err(e) = spend(&mut s) {
            return ready(Err(e));
        }
        ready(s.files.remove(path).map(|_| ()).ok_or_else(|| not_found(path)))
    }

    pub(crate) fn remove_dir(&self, dir: &Path) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let mut s = self.state.lock().expect("disk lock");
        if !s.dirs.contains(dir) {
            return ready(Err(not_found(dir)));
        }
        let inside = |p: &PathBuf| p.parent() == Some(dir);
        if s.files.keys().any(inside) || s.dirs.iter().any(inside) {
            return ready(Err(io::Error::new(
                io::ErrorKind::DirectoryNotEmpty,
                format!("{} is not empty", dir.display()),
            )));
        }
        if let Err(e) = spend(&mut s) {
            return ready(Err(e));
        }
        s.dirs.remove(dir);
        ready(Ok(()))
    }

    fn with_file<T>(
        &self,
        file: &SimFile,
        write: bool,
        f: impl FnOnce(&mut FileState) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut s = self.state.lock().expect("disk lock");
        if s.incarnation != file.incarnation {
            return Err(io::Error::other(format!("stale handle to {} after a crash", file.path.display())));
        }
        if write {
            spend(&mut s)?;
        } else if s.power_left == Some(0) {
            return Err(io::Error::other("simulated power loss"));
        }
        let state = s.files.get_mut(&file.path).ok_or_else(|| not_found(&file.path))?;
        f(state)
    }
}

impl SegmentFile for SimFile {
    fn append(&self, buf: Bytes) -> impl Future<Output = io::Result<u64>> + Send {
        ready(self.disk.with_file(self, true, |f| {
            let pos = f.data.len() as u64;
            f.data.extend_from_slice(&buf);
            Ok(pos)
        }))
    }

    fn read_at(&self, pos: u64, len: usize) -> impl Future<Output = io::Result<Bytes>> + Send {
        ready(self.disk.with_file(self, false, |f| {
            let start = (pos as usize).min(f.data.len());
            let end = start.saturating_add(len).min(f.data.len());
            Ok(Bytes::copy_from_slice(&f.data[start..end]))
        }))
    }

    fn size(&self) -> impl Future<Output = io::Result<u64>> + Send {
        ready(self.disk.with_file(self, false, |f| Ok(f.data.len() as u64)))
    }

    fn truncate(&self, len: u64) -> impl Future<Output = io::Result<()>> + Send {
        ready(self.disk.with_file(self, true, |f| {
            let len = len as usize;
            if len < f.data.len() {
                f.data.truncate(len);
            }
            f.synced = f.synced.min(f.data.len());
            Ok(())
        }))
    }

    fn sync(&self) -> impl Future<Output = io::Result<()>> + Send {
        ready(self.disk.with_file(self, true, |f| {
            f.synced = f.data.len();
            Ok(())
        }))
    }
}

/// Counts one write operation against an armed power loss.
fn spend(s: &mut DiskState) -> io::Result<()> {
    match &mut s.power_left {
        Some(0) => Err(io::Error::other("simulated power loss")),
        Some(n) => {
            *n -= 1;
            Ok(())
        }
        None => Ok(()),
    }
}

fn not_found(path: &Path) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("{} not found", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sim;
    use lotse_rt::Disk;

    #[test]
    fn an_armed_power_loss_stops_writes_mid_way() {
        Sim::new(7).run(|rt| async move {
            rt.create_dir_all(Path::new("d")).await.unwrap();
            let f = rt.open(Path::new("d/f"), OpenMode::CreateNew).await.unwrap();
            rt.disk().arm_power_loss(2);
            f.append(Bytes::from_static(b"one")).await.unwrap();
            f.sync().await.unwrap();
            assert!(f.append(Bytes::from_static(b"two")).await.is_err(), "the power is gone");
            assert!(f.read_at(0, 3).await.is_err());
            assert!(rt.disk().power_lost());
            rt.crash();
            let f = rt.open(Path::new("d/f"), OpenMode::Existing).await.unwrap();
            assert_eq!(&f.read_at(0, 10).await.unwrap()[..], b"one");
        });
    }

    #[test]
    fn synced_data_survives_and_unsynced_data_is_a_prefix_or_torn() {
        for seed in 0..200 {
            Sim::new(seed).run(|rt| async move {
                rt.create_dir_all(Path::new("d")).await.unwrap();
                let f = rt.open(Path::new("d/f"), OpenMode::CreateNew).await.unwrap();
                f.append(Bytes::from_static(b"durable")).await.unwrap();
                f.sync().await.unwrap();
                f.append(Bytes::from_static(b"volatile")).await.unwrap();
                let report = rt.crash();
                assert!(f.size().await.is_err(), "old handle must be stale");
                let f = rt.open(Path::new("d/f"), OpenMode::Existing).await.unwrap();
                let data = f.read_at(0, 100).await.unwrap();
                assert!(data.starts_with(b"durable"));
                assert!(data.len() <= 15);
                if report.torn_files.is_empty() {
                    assert!(b"durablevolatile".starts_with(&data));
                }
            });
        }
    }
}

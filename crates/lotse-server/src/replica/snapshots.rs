//! Where a replica keeps its snapshots (ADR 0099): each in a file of the
//! replica's directory, read a chunk at a time when it is sent, so that no
//! copy of a snapshot's data stays in memory beside the state it encodes.
//!
//! - `snapshot`: the latest, the one the replica starts from.
//! - `snapshot.<index>.boundary`: one taken where a segment begins, for
//!   retention to cut at later; synced, so that it becomes `snapshot` by a
//!   rename.
//! - `snapshot.<index>.sent`: a replaced snapshot a follower is still being
//!   sent.
//! - `snapshot.incoming`: the chunks of a snapshot being received.
//!
//! Only `snapshot` outlives a restart; the others are removed on open.

use std::io;
use std::path::Path;

use bytes::Bytes;
use lotse_raft::SnapshotMeta;
use lotse_rt::{Disk, OpenMode, SegmentFile};

use super::{Error, SNAPSHOT};

/// The chunks of a snapshot being received.
pub(super) const INCOMING: &str = "snapshot.incoming";

/// What a copy reads at a time.
const COPY_BYTES: usize = 1 << 20;

/// A snapshot in a file, encoded as `encode_snapshot` encodes one: its data
/// is `len` bytes from position `at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SnapshotFile {
    /// The file's name in the replica's directory.
    pub name: String,
    /// Where the data starts.
    pub at: u64,
    /// How long the data is.
    pub len: u64,
}

impl SnapshotFile {
    /// The snapshot `meta` with `len` bytes of data in the file `name`.
    pub fn at(name: String, meta: &SnapshotMeta, len: u64) -> Self {
        // Format, index, term, the membership's length and itself, the
        // data's length (see `encode_snapshot`).
        let at = 1 + 8 + 8 + 4 + meta.membership.encode().len() as u64 + 4;
        SnapshotFile { name, at, len }
    }

    /// The latest snapshot, in `snapshot`.
    pub fn latest(meta: &SnapshotMeta, len: u64) -> Self {
        Self::at(SNAPSHOT.into(), meta, len)
    }

    /// At most `max` bytes (at least one) of the data from `offset`, or from
    /// its end if `offset` lies past it. Returns where the bytes start.
    pub async fn read<D: Disk>(&self, disk: &D, dir: &Path, offset: u64, max: usize) -> Result<(u64, Bytes), Error> {
        let start = offset.min(self.len);
        let n = (self.len - start).min(max.max(1) as u64);
        let file = disk.open(&dir.join(&self.name), OpenMode::Existing).await?;
        let data = file.read_at(self.at + start, n as usize).await?;
        if data.len() as u64 != n {
            return Err(Error::Corrupt(format!("{} ends inside its snapshot's data", self.name)));
        }
        Ok((start, data))
    }
}

/// The file of the snapshot taken where the segment at `offset` begins.
pub(super) fn boundary_name(offset: u64) -> String {
    format!("{SNAPSHOT}.{offset}.boundary")
}

/// The file a replaced snapshot ending at `index` is kept in while it is sent.
pub(super) fn sent_name(index: u64) -> String {
    format!("{SNAPSHOT}.{index}.sent")
}

/// Copies `from` to `to` (both in `dir`) a piece at a time. Not synced:
/// the copy does not outlive a restart.
pub(super) async fn copy<D: Disk>(disk: &D, dir: &Path, from: &str, to: &str) -> Result<(), Error> {
    let src = disk.open(&dir.join(from), OpenMode::Existing).await?;
    let dst = disk.open(&dir.join(to), OpenMode::CreateOrOpen).await?;
    dst.truncate(0).await?;
    let len = src.size().await?;
    let mut pos = 0;
    while pos < len {
        let piece = src.read_at(pos, COPY_BYTES).await?;
        if piece.is_empty() {
            return Err(Error::Corrupt(format!("{from} shrank while it was copied")));
        }
        pos += piece.len() as u64;
        dst.append(piece).await?;
    }
    Ok(())
}

/// Removes `name` from `dir` if it is there.
pub(super) async fn remove<D: Disk>(disk: &D, dir: &Path, name: &str) -> Result<(), Error> {
    match disk.remove(&dir.join(name)).await {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e.into()),
        _ => Ok(()),
    }
}

/// Removes every snapshot file in `dir` but `snapshot` (and the temporary
/// files of writes a crash cut short).
pub(super) async fn remove_others<D: Disk>(disk: &D, dir: &Path) -> Result<(), Error> {
    let prefix = format!("{SNAPSHOT}.");
    for path in disk.list(dir).await? {
        let others = path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix));
        if others {
            disk.remove(&path).await?;
        }
    }
    Ok(())
}

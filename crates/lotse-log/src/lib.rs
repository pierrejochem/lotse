//! The segmented log behind every partition and queue.
//!
//! The Raft log *is* this log: each Raft entry is one record batch, and a
//! batch's `base_offset` is its Raft index. There is no separate WAL.
//!
//! Layout: one directory per partition, segment files named after their
//! first offset (`00000000000000000000.log`). Only the last segment is ever
//! written; once rolled, a segment is synced and immutable.
//!
//! Durability contract:
//! - [`Log::append`] makes a batch readable at once;
//! - it survives a crash only after [`Log::sync`] (or a segment roll);
//! - [`Log::open`] recovers by scanning the active segment, cutting a torn or
//!   corrupt tail, and syncing the result, so recovered data is durable.
//!
//! After a snapshot is installed, [`Log::reset`] restarts the log at the
//! snapshot's end; segments then no longer start at 0.
//!
//! Not yet here: persisted `.index`/`.timeindex`/`.txnindex` files (the
//! sparse index is rebuilt by scanning on open) and retention.

#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};
use lotse_record::{Batch, HEADER_LEN, LEN_PREFIX, MAX_BATCH_LEN};
use lotse_rt::{Disk, OpenMode, SegmentFile};

/// Errors from the log.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The disk failed.
    #[error("disk error: {0}")]
    Io(#[from] io::Error),
    /// An appended batch is not a valid batch.
    #[error("invalid batch: {0}")]
    InvalidBatch(#[from] lotse_record::Error),
    /// An appended batch does not continue the log.
    #[error("batch starts at offset {got}, log expects {expected}")]
    OffsetMismatch {
        /// Next offset of the log.
        expected: u64,
        /// `base_offset` of the batch.
        got: u64,
    },
    /// A read or truncation outside the log.
    #[error("offset {offset} is outside the log [{start}, {end}]")]
    OffsetOutOfRange {
        /// Requested offset.
        offset: u64,
        /// First offset in the log.
        start: u64,
        /// Next offset to be written.
        end: u64,
    },
    /// Truncation must happen at a batch boundary.
    #[error("offset {0} is inside a batch")]
    NotBatchBoundary(u64),
    /// A sealed segment fails validation. Torn tails are repaired on open;
    /// this is damage in data that was already durable.
    #[error("segment {segment} is corrupt at byte {position}: {reason}")]
    Corrupt {
        /// Segment file.
        segment: PathBuf,
        /// Byte position of the bad batch.
        position: u64,
        /// What failed.
        reason: String,
    },
}

/// Tuning knobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogConfig {
    /// Roll to a new segment once the active one would exceed this size.
    pub segment_bytes: u64,
    /// Add a sparse index entry at least every this many bytes.
    pub index_interval_bytes: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig { segment_bytes: 128 * 1024 * 1024, index_interval_bytes: 4096 }
    }
}

/// Where an appended batch landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    /// Offset of the first record.
    pub base_offset: u64,
    /// Offset after the batch.
    pub next_offset: u64,
}

struct Segment<F> {
    path: PathBuf,
    file: F,
    size: u64,
    next_offset: u64,
    /// Sparse `(base_offset of a batch, byte position)` entries, ascending.
    index: Vec<(u64, u64)>,
    bytes_since_index: u64,
}

impl<F: SegmentFile> Segment<F> {
    fn note_batch(&mut self, base_offset: u64, position: u64, len: u64, interval: u64) {
        if self.index.is_empty() || self.bytes_since_index >= interval {
            self.index.push((base_offset, position));
            self.bytes_since_index = 0;
        }
        self.bytes_since_index += len;
    }

    /// Byte position to start scanning from for `offset`.
    fn scan_start(&self, offset: u64) -> u64 {
        match self.index.partition_point(|&(o, _)| o <= offset) {
            0 => 0,
            i => self.index[i - 1].1,
        }
    }
}

/// A partition log on one disk.
pub struct Log<D: Disk> {
    disk: D,
    dir: PathBuf,
    config: LogConfig,
    segments: BTreeMap<u64, Segment<D::File>>,
    start_offset: u64,
    next_offset: u64,
}

impl<D: Disk> fmt::Debug for Log<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Log")
            .field("dir", &self.dir)
            .field("segments", &self.segments.len())
            .field("start_offset", &self.start_offset)
            .field("next_offset", &self.next_offset)
            .finish()
    }
}

impl<D: Disk> Log<D> {
    /// Opens the log in `dir`, creating it if needed, and recovers from any
    /// crash: a torn or corrupt tail in the active segment is cut off and the
    /// result synced.
    pub async fn open(disk: D, dir: impl Into<PathBuf>, config: LogConfig) -> Result<Self, Error> {
        let dir = dir.into();
        disk.create_dir_all(&dir).await?;
        let mut bases: Vec<u64> = disk.list(&dir).await?.iter().filter_map(|p| parse_segment_name(p)).collect();
        bases.sort_unstable();

        let mut log = Log { disk, dir, config, segments: BTreeMap::new(), start_offset: 0, next_offset: 0 };
        let count = bases.len();
        for (i, base) in bases.into_iter().enumerate() {
            let is_last = i + 1 == count;
            if !log.segments.is_empty() && base != log.next_offset {
                return Err(Error::Corrupt {
                    segment: segment_path(&log.dir, base),
                    position: 0,
                    reason: format!("segment starts at {base}, previous segment ends at {}", log.next_offset),
                });
            }
            let seg = log.recover_segment(base, is_last).await?;
            if log.segments.is_empty() {
                log.start_offset = base;
            }
            log.next_offset = seg.next_offset;
            log.segments.insert(base, seg);
        }
        if log.segments.is_empty() {
            log.create_segment(0).await?;
        }
        Ok(log)
    }

    async fn recover_segment(&self, base: u64, is_last: bool) -> Result<Segment<D::File>, Error> {
        let path = segment_path(&self.dir, base);
        let file = self.disk.open(&path, OpenMode::Existing).await?;
        let len = file.size().await?;
        let mut seg = Segment { path, file, size: 0, next_offset: base, index: Vec::new(), bytes_since_index: 0 };
        let mut pos = 0u64;
        let mut problem = None;
        while pos < len {
            match read_batch_at(&seg.file, pos).await {
                Ok(Some(batch)) if batch.base_offset() == seg.next_offset => {
                    let n = batch.len() as u64;
                    seg.note_batch(batch.base_offset(), pos, n, self.config.index_interval_bytes);
                    seg.next_offset = batch.next_offset();
                    pos += n;
                }
                Ok(Some(batch)) => {
                    problem =
                        Some(format!("batch at offset {} where {} was expected", batch.base_offset(), seg.next_offset));
                    break;
                }
                Ok(None) => {
                    problem = Some("incomplete batch".to_owned());
                    break;
                }
                Err(ReadError::Io(e)) => return Err(e.into()),
                Err(ReadError::Record(e)) => {
                    problem = Some(e.to_string());
                    break;
                }
            }
        }
        seg.size = pos;
        if let Some(reason) = problem {
            if !is_last {
                return Err(Error::Corrupt { segment: seg.path, position: pos, reason });
            }
            // A crash tore the tail of the active segment: everything from the
            // first bad batch on was never acknowledged as durable.
            seg.file.truncate(pos).await?;
            seg.file.sync().await?;
        } else if is_last {
            // Make sure what survived is durable before anyone builds on it.
            seg.file.sync().await?;
        }
        Ok(seg)
    }

    async fn create_segment(&mut self, base: u64) -> Result<(), Error> {
        let path = segment_path(&self.dir, base);
        let file = self.disk.open(&path, OpenMode::CreateNew).await?;
        if self.segments.is_empty() {
            self.start_offset = base;
        }
        self.segments
            .insert(base, Segment { path, file, size: 0, next_offset: base, index: Vec::new(), bytes_since_index: 0 });
        Ok(())
    }

    /// First offset still in the log.
    pub fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Offset the next appended batch must start at.
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Number of segment files.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Appends a stamped batch whose `base_offset` equals [`Log::next_offset`].
    /// Readable at once; durable after [`Log::sync`].
    pub async fn append(&mut self, batch: Bytes) -> Result<Appended, Error> {
        let (parsed, rest) = Batch::parse(&batch)?;
        if !rest.is_empty() {
            return Err(Error::InvalidBatch(lotse_record::Error::Malformed("trailing bytes after the batch")));
        }
        if parsed.base_offset() != self.next_offset {
            return Err(Error::OffsetMismatch { expected: self.next_offset, got: parsed.base_offset() });
        }
        let len = batch.len() as u64;
        let roll = {
            let active = self.active();
            active.size > 0 && active.size + len > self.config.segment_bytes
        };
        if roll {
            self.active().file.sync().await?;
            let base = self.next_offset;
            self.create_segment(base).await?;
        }
        let interval = self.config.index_interval_bytes;
        let active = self.active_mut();
        let position = active.file.append(batch).await?;
        debug_assert_eq!(position, active.size);
        active.note_batch(parsed.base_offset(), position, len, interval);
        active.size += len;
        active.next_offset = parsed.next_offset();
        self.next_offset = parsed.next_offset();
        Ok(Appended { base_offset: parsed.base_offset(), next_offset: parsed.next_offset() })
    }

    /// Makes every appended batch durable.
    pub async fn sync(&self) -> Result<(), Error> {
        self.active().file.sync().await?;
        Ok(())
    }

    /// Reads whole batches starting with the one that contains `offset`, up
    /// to `max_bytes`. Always returns at least one batch if `offset` is in
    /// the log, even if it is larger than `max_bytes`. Returns no bytes at
    /// [`Log::next_offset`].
    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Bytes, Error> {
        if offset < self.start_offset || offset > self.next_offset {
            return Err(Error::OffsetOutOfRange { offset, start: self.start_offset, end: self.next_offset });
        }
        if offset == self.next_offset {
            return Ok(Bytes::new());
        }
        let (_, seg) = self.segments.range(..=offset).next_back().expect("offset is inside the log");
        let mut pos = seg.scan_start(offset);
        // Find the batch that contains `offset`.
        let first = loop {
            let batch = self.batch_in(seg, pos).await?;
            if batch.last_offset() >= offset {
                break batch;
            }
            pos += batch.len() as u64;
        };
        let mut out = BytesMut::from(&first.as_bytes()[..]);
        pos += first.len() as u64;
        while pos < seg.size {
            let Some(len) = self.peek_len_in(seg, pos).await? else { break };
            if out.len() + len > max_bytes {
                break;
            }
            let batch = self.batch_in(seg, pos).await?;
            out.extend_from_slice(batch.as_bytes());
            pos += len as u64;
        }
        Ok(out.freeze())
    }

    /// Removes every batch at or after `offset`, which must be a batch
    /// boundary (Raft truncates a conflicting suffix this way). Durable on
    /// return.
    pub async fn truncate_from(&mut self, offset: u64) -> Result<(), Error> {
        if offset < self.start_offset || offset > self.next_offset {
            return Err(Error::OffsetOutOfRange { offset, start: self.start_offset, end: self.next_offset });
        }
        if offset == self.next_offset {
            return Ok(());
        }
        // Drop whole segments that start at or after the cut. The first
        // segment always stays, so a full truncation leaves it empty.
        let doomed: Vec<u64> =
            self.segments.range(offset..).map(|(&b, _)| b).filter(|&b| b != self.start_offset).collect();
        for base in doomed.into_iter().rev() {
            let seg = self.segments.remove(&base).expect("listed above");
            self.disk.remove(&seg.path).await?;
        }
        let interval = self.config.index_interval_bytes;
        let (&base, _) = self.segments.range(..=offset).next_back().expect("start segment is kept");
        // Find the byte position of `offset` and rebuild the index up to it.
        let mut pos = 0u64;
        let mut index = Vec::new();
        let mut since = 0u64;
        {
            let seg = &self.segments[&base];
            while pos < seg.size {
                let batch = self.batch_in(seg, pos).await?;
                if batch.base_offset() >= offset {
                    if batch.base_offset() > offset {
                        return Err(Error::NotBatchBoundary(offset));
                    }
                    break;
                }
                if batch.last_offset() >= offset {
                    return Err(Error::NotBatchBoundary(offset));
                }
                if index.is_empty() || since >= interval {
                    index.push((batch.base_offset(), pos));
                    since = 0;
                }
                since += batch.len() as u64;
                pos += batch.len() as u64;
            }
        }
        let seg = self.segments.get_mut(&base).expect("present");
        seg.file.truncate(pos).await?;
        seg.file.sync().await?;
        seg.size = pos;
        seg.index = index;
        seg.bytes_since_index = since;
        seg.next_offset = offset;
        self.next_offset = offset;
        Ok(())
    }

    /// Drops every segment and starts the log afresh at `offset`, as after
    /// installing a snapshot that ends right before it. Durable on return.
    pub async fn reset(&mut self, offset: u64) -> Result<(), Error> {
        // The new, empty segment first (unless it would clash with one we drop).
        let keep = self.segments.contains_key(&offset);
        for (base, seg) in std::mem::take(&mut self.segments) {
            if keep && base == offset {
                seg.file.truncate(0).await?;
                seg.file.sync().await?;
                self.segments.insert(
                    base,
                    Segment { size: 0, next_offset: offset, index: Vec::new(), bytes_since_index: 0, ..seg },
                );
            } else {
                self.disk.remove(&seg.path).await?;
            }
        }
        if !keep {
            self.create_segment(offset).await?;
        }
        self.start_offset = offset;
        self.next_offset = offset;
        Ok(())
    }

    fn active(&self) -> &Segment<D::File> {
        self.segments.values().next_back().expect("a log always has a segment")
    }

    fn active_mut(&mut self) -> &mut Segment<D::File> {
        self.segments.values_mut().next_back().expect("a log always has a segment")
    }

    async fn peek_len_in(&self, seg: &Segment<D::File>, pos: u64) -> Result<Option<usize>, Error> {
        let prefix = seg.file.read_at(pos, LEN_PREFIX).await?;
        Ok(lotse_record::peek_len(&prefix))
    }

    async fn batch_in(&self, seg: &Segment<D::File>, pos: u64) -> Result<Batch, Error> {
        match read_batch_at(&seg.file, pos).await {
            Ok(Some(batch)) => Ok(batch),
            Ok(None) => {
                Err(Error::Corrupt { segment: seg.path.clone(), position: pos, reason: "incomplete batch".into() })
            }
            Err(e) => Err(Error::Corrupt { segment: seg.path.clone(), position: pos, reason: e.to_string() }),
        }
    }
}

/// Reads and validates the batch at `pos`. `Ok(None)` if the file ends
/// inside it.
async fn read_batch_at<F: SegmentFile>(file: &F, pos: u64) -> Result<Option<Batch>, ReadError> {
    let prefix = file.read_at(pos, LEN_PREFIX).await?;
    let Some(len) = lotse_record::peek_len(&prefix) else { return Ok(None) };
    if !(HEADER_LEN..=MAX_BATCH_LEN).contains(&len) {
        return Err(ReadError::Record(lotse_record::Error::Malformed("implausible batch length")));
    }
    let bytes = file.read_at(pos, len).await?;
    if bytes.len() < len {
        return Ok(None);
    }
    let (batch, _) = Batch::parse(&bytes)?;
    Ok(Some(batch))
}

#[derive(Debug, thiserror::Error)]
enum ReadError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Record(#[from] lotse_record::Error),
}

impl From<ReadError> for Error {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::Io(e) => Error::Io(e),
            ReadError::Record(e) => Error::InvalidBatch(e),
        }
    }
}

/// Path of the segment that starts at `base`.
pub fn segment_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.log"))
}

fn parse_segment_name(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(".log")?;
    (digits.len() == 20).then(|| digits.parse().ok()).flatten()
}

#[cfg(test)]
mod tests;

//! The segmented log behind every partition and queue.
//!
//! The Raft log *is* this log: each Raft entry is one record batch, and a
//! batch's `base_offset` is its Raft index. There is no separate WAL.
//!
//! Layout: one directory per partition, segment files named after their
//! first offset (`00000000000000000000.log`). Only the last segment is ever
//! appended to; once rolled, a segment is synced and changes only as a
//! whole, when compaction rewrites it.
//!
//! Durability contract:
//! - [`Log::append`] makes a batch readable at once;
//! - it survives a crash only after [`Log::sync`] (or a segment roll);
//! - [`Log::open`] recovers by scanning the active segment, cutting a torn or
//!   corrupt tail, and syncing the result, so recovered data is durable.
//!
//! Every sealed segment has an index file beside it (`.index`, ADR 0094):
//! its sparse offset index and the newest timestamp before each entry. It is
//! written when the segment is sealed and read back on open, so opening a
//! log reads the active segment and one batch of each sealed one, not every
//! batch. An index file that does not match its segment is ignored, the
//! segment scanned and the file written again; one is removed before its
//! segment changes (truncation, retention, a rewrite, a reset). Batches of a
//! sealed segment are checked when they are read.
//!
//! After a snapshot is installed, [`Log::reset`] restarts the log at the
//! snapshot's end; segments then no longer start at 0.
//!
//! Retention removes old data with [`Log::trim_before`]: whole segments
//! below an offset are deleted, and the log starts at that offset. Which
//! offset is the replica's to decide (ADR 0046); [`Log::segments`] tells it
//! each segment's size and newest timestamp.
//!
//! Compaction replaces a sealed segment with [`Log::rewrite_segment`]: the
//! same batches with the same offsets and terms, some records left out.

#![warn(missing_docs)]

mod index;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use index::{Entry, SegmentIndex};
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
    /// A segment rewrite would change more than which records it holds.
    #[error("refused segment rewrite: {0}")]
    Rewrite(String),
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
    /// Sparse index entries, ascending.
    index: Vec<Entry>,
    bytes_since_index: u64,
    /// The newest record timestamp in the segment, ms; `i64::MIN` if empty.
    max_timestamp: i64,
    /// Byte position and stored CRC of the last batch, which an index file
    /// records so that a stale file is recognised on open.
    last_position: u64,
    last_crc: u32,
    /// An index file for this segment is on disk.
    indexed: bool,
}

/// What retention needs to know about one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    /// Its first offset.
    pub base_offset: u64,
    /// The offset after its last batch.
    pub next_offset: u64,
    /// Its size.
    pub bytes: u64,
    /// The newest record timestamp in it, ms since the epoch; `i64::MIN`
    /// for an empty segment.
    pub max_timestamp: i64,
}

impl<F> Segment<F> {
    fn empty(path: PathBuf, file: F, base: u64) -> Self {
        Segment {
            path,
            file,
            size: 0,
            next_offset: base,
            index: Vec::new(),
            bytes_since_index: 0,
            max_timestamp: i64::MIN,
            last_position: 0,
            last_crc: 0,
            indexed: false,
        }
    }

    fn note_batch(&mut self, batch: &Batch, position: u64, interval: u64) {
        let len = batch.len() as u64;
        if self.index.is_empty() || self.bytes_since_index >= interval {
            self.index.push(Entry { offset: batch.base_offset(), position, timestamp_before: self.max_timestamp });
            self.bytes_since_index = 0;
        }
        self.bytes_since_index += len;
        self.max_timestamp = self.max_timestamp.max(batch.max_timestamp());
        self.last_position = position;
        self.last_crc = batch.crc();
    }

    /// Byte position to start scanning from for `offset`.
    fn scan_start(&self, offset: u64) -> u64 {
        match self.index.partition_point(|e| e.offset <= offset) {
            0 => 0,
            i => self.index[i - 1].position,
        }
    }

    /// The first offset of the segment from which a search for a batch whose
    /// newest record is at or after `at` may start: every batch before it
    /// is older.
    fn time_start(&self, at: i64) -> u64 {
        match self.index.partition_point(|e| e.timestamp_before < at) {
            0 => self.index.first().map_or(self.next_offset, |e| e.offset),
            i => self.index[i - 1].offset,
        }
    }

    fn index_path(&self) -> PathBuf {
        self.path.with_extension("index")
    }

    fn to_index(&self, base: u64) -> SegmentIndex {
        SegmentIndex {
            base_offset: base,
            size: self.size,
            next_offset: self.next_offset,
            max_timestamp: self.max_timestamp,
            last_position: self.last_position,
            last_crc: self.last_crc,
            entries: self.index.clone(),
        }
    }
}

/// Bytes of one segment read ahead, from `pos`.
struct Window {
    pos: u64,
    bytes: Bytes,
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
    /// result synced. Sealed segments are taken from their index files where
    /// those match, and scanned (and indexed) where not.
    pub async fn open(disk: D, dir: impl Into<PathBuf>, config: LogConfig) -> Result<Self, Error> {
        let dir = dir.into();
        disk.create_dir_all(&dir).await?;
        let files = disk.list(&dir).await?;
        // A rewrite or an index that was not finished: what it replaces is
        // still whole.
        for p in files.iter().filter(|p| p.extension().is_some_and(|e| e == "cleaned" || e == "tmp")) {
            disk.remove(p).await?;
        }
        let mut bases: Vec<u64> = files.iter().filter_map(|p| parse_name(p, ".log")).collect();
        bases.sort_unstable();
        let mut indexed: BTreeSet<u64> = BTreeSet::new();
        for p in &files {
            if let Some(base) = parse_name(p, ".index") {
                // The active segment changes, so its index file, if a crash
                // left one, says nothing; neither does one without a segment.
                if bases.last() != Some(&base) && bases.binary_search(&base).is_ok() {
                    indexed.insert(base);
                } else {
                    disk.remove(p).await?;
                }
            }
        }

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
            let loaded = if indexed.contains(&base) { log.load_segment(base).await? } else { None };
            let seg = match loaded {
                Some(seg) => seg,
                None => {
                    let mut seg = log.recover_segment(base, is_last).await?;
                    seg.indexed = indexed.contains(&base);
                    if !is_last {
                        write_index(&log.disk, base, &mut seg).await?;
                    }
                    seg
                }
            };
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

    /// A sealed segment as its index file describes it, if the file matches
    /// the segment (see the `index` module); `None` to scan it instead.
    async fn load_segment(&self, base: u64) -> Result<Option<Segment<D::File>>, Error> {
        let path = segment_path(&self.dir, base);
        let index_file = self.disk.open(&path.with_extension("index"), OpenMode::Existing).await?;
        let len = index_file.size().await?;
        let bytes = index_file.read_at(0, len as usize).await?;
        drop(index_file);
        let Ok(index) = SegmentIndex::decode(&bytes) else { return Ok(None) };
        if index.base_offset != base {
            return Ok(None);
        }
        let file = self.disk.open(&path, OpenMode::Existing).await?;
        if file.size().await? != index.size {
            return Ok(None);
        }
        let last = match read_batch_at(&file, index.last_position).await {
            Ok(Some(batch)) => batch,
            Ok(None) | Err(ReadError::Record(_)) => return Ok(None),
            Err(ReadError::Io(e)) => return Err(e.into()),
        };
        if last.crc() != index.last_crc
            || last.next_offset() != index.next_offset
            || index.last_position + last.len() as u64 != index.size
        {
            return Ok(None);
        }
        Ok(Some(Segment {
            path,
            file,
            size: index.size,
            next_offset: index.next_offset,
            index: index.entries,
            bytes_since_index: 0,
            max_timestamp: index.max_timestamp,
            last_position: index.last_position,
            last_crc: index.last_crc,
            indexed: true,
        }))
    }

    async fn recover_segment(&self, base: u64, is_last: bool) -> Result<Segment<D::File>, Error> {
        let path = segment_path(&self.dir, base);
        let file = self.disk.open(&path, OpenMode::Existing).await?;
        let len = file.size().await?;
        let mut seg = Segment::empty(path, file, base);
        let mut pos = 0u64;
        let mut problem = None;
        while pos < len {
            match read_batch_at(&seg.file, pos).await {
                Ok(Some(batch)) if batch.base_offset() == seg.next_offset => {
                    seg.note_batch(&batch, pos, self.config.index_interval_bytes);
                    seg.next_offset = batch.next_offset();
                    pos += batch.len() as u64;
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
        self.segments.insert(base, Segment::empty(path, file, base));
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

    /// Bytes in all segments, including unsynced appends.
    pub fn size_bytes(&self) -> u64 {
        self.segments.values().map(|s| s.size).sum()
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
            // Sealed from here on: synced, then indexed, before the next
            // segment exists, so on open a segment that is not the last
            // always is whole.
            let active_base = self.active_base();
            let disk = self.disk.clone();
            let active = self.active_mut();
            active.file.sync().await?;
            write_index(&disk, active_base, active).await?;
            let base = self.next_offset;
            self.create_segment(base).await?;
        }
        let interval = self.config.index_interval_bytes;
        let active = self.active_mut();
        let position = active.file.append(batch).await?;
        debug_assert_eq!(position, active.size);
        active.note_batch(&parsed, position, interval);
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
    ///
    /// The batches come from one segment in one read where the read-ahead
    /// covers them, and are returned as a slice of it, each checked.
    pub async fn read(&self, offset: u64, max_bytes: usize) -> Result<Bytes, Error> {
        if offset < self.start_offset || offset > self.next_offset {
            return Err(Error::OffsetOutOfRange { offset, start: self.start_offset, end: self.next_offset });
        }
        if offset == self.next_offset {
            return Ok(Bytes::new());
        }
        let (_, seg) = self.segments.range(..=offset).next_back().expect("offset is inside the log");
        // From the index entry to the batch, then as much as fits.
        let ahead = (self.config.index_interval_bytes as usize).saturating_add(max_bytes.max(HEADER_LEN));
        let mut window = Window { pos: 0, bytes: Bytes::new() };
        let mut pos = seg.scan_start(offset);
        let first_len = loop {
            let (len, _, last) = self.header_at(seg, &mut window, pos, ahead).await?;
            if last >= offset {
                break len;
            }
            pos += len as u64;
        };
        let start = pos;
        let mut end = start + first_len as u64;
        while end < seg.size {
            let (len, _, _) = self.header_at(seg, &mut window, end, ahead).await?;
            if (end - start) as usize + len > max_bytes {
                break;
            }
            end += len as u64;
        }
        let out = self.bytes_at(seg, &mut window, start, (end - start) as usize, ahead).await?;
        let mut rest = out.clone();
        while !rest.is_empty() {
            let at = start + (out.len() - rest.len()) as u64;
            let (_, tail) = Batch::parse(&rest).map_err(|e| Error::Corrupt {
                segment: seg.path.clone(),
                position: at,
                reason: e.to_string(),
            })?;
            rest = tail;
        }
        Ok(out)
    }

    /// The header of the batch at `pos`: its length, first and last offset.
    async fn header_at(
        &self,
        seg: &Segment<D::File>,
        window: &mut Window,
        pos: u64,
        ahead: usize,
    ) -> Result<(usize, u64, u64), Error> {
        let corrupt = |reason: &str| Error::Corrupt { segment: seg.path.clone(), position: pos, reason: reason.into() };
        if pos + HEADER_LEN as u64 > seg.size {
            return Err(corrupt("incomplete batch"));
        }
        let header = self.bytes_at(seg, window, pos, HEADER_LEN, ahead).await?;
        let (len, base, last) = lotse_record::peek_span(&header).expect("a whole header");
        if !(HEADER_LEN..=MAX_BATCH_LEN).contains(&len) {
            return Err(corrupt("implausible batch length"));
        }
        if pos + len as u64 > seg.size {
            return Err(corrupt("incomplete batch"));
        }
        Ok((len, base, last))
    }

    /// `len` bytes of the segment at `pos`, from the read-ahead if it holds
    /// them, else from a new read of at least `ahead` bytes.
    async fn bytes_at(
        &self,
        seg: &Segment<D::File>,
        window: &mut Window,
        pos: u64,
        len: usize,
        ahead: usize,
    ) -> Result<Bytes, Error> {
        let held = pos >= window.pos && pos + len as u64 <= window.pos + window.bytes.len() as u64;
        if !held {
            let n = len.max(ahead).min((seg.size - pos) as usize);
            window.bytes = seg.file.read_at(pos, n).await?;
            window.pos = pos;
            if window.bytes.len() < len {
                return Err(Error::Corrupt {
                    segment: seg.path.clone(),
                    position: pos,
                    reason: "incomplete batch".into(),
                });
            }
        }
        let from = (pos - window.pos) as usize;
        Ok(window.bytes.slice(from..from + len))
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
            let mut seg = self.segments.remove(&base).expect("listed above");
            remove_index(&self.disk, &mut seg).await?;
            self.disk.remove(&seg.path).await?;
        }
        let interval = self.config.index_interval_bytes;
        let (&base, _) = self.segments.range(..=offset).next_back().expect("start segment is kept");
        // Find the byte position of `offset` and rebuild the index up to it.
        let mut cut = Segment::empty(PathBuf::new(), (), base);
        {
            let seg = &self.segments[&base];
            let mut pos = 0u64;
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
                cut.note_batch(&batch, pos, interval);
                pos += batch.len() as u64;
            }
            cut.size = pos;
        }
        let disk = self.disk.clone();
        let seg = self.segments.get_mut(&base).expect("present");
        // The segment is the active one now and is about to change: its
        // index file, if any, goes first.
        remove_index(&disk, seg).await?;
        seg.file.truncate(cut.size).await?;
        seg.file.sync().await?;
        seg.size = cut.size;
        seg.index = cut.index;
        seg.bytes_since_index = cut.bytes_since_index;
        seg.max_timestamp = cut.max_timestamp;
        seg.last_position = cut.last_position;
        seg.last_crc = cut.last_crc;
        seg.next_offset = offset;
        self.next_offset = offset;
        Ok(())
    }

    /// Every segment, oldest first.
    pub fn segments(&self) -> impl Iterator<Item = SegmentInfo> + '_ {
        self.segments.iter().map(|(&base, s)| SegmentInfo {
            base_offset: base,
            next_offset: s.next_offset,
            bytes: s.size,
            max_timestamp: s.max_timestamp,
        })
    }

    /// Where, in the segment that starts at `base`, a search for the first
    /// batch whose newest record is at or after `at` (ms) may start: every
    /// batch of the segment before the returned offset is older. Found in
    /// the segment's sparse index, so the search reads at most one index
    /// interval of batches it skips. `None` if no segment starts at `base`.
    pub fn time_search_start(&self, base: u64, at: i64) -> Option<u64> {
        self.segments.get(&base).map(|s| s.time_start(at))
    }

    /// Starts the log at `offset`: reads below it are out of range, and
    /// every segment that ends at or below it is deleted, except the
    /// active one. The segment holding `offset` stays whole, so after a
    /// restart the log may start earlier, at that segment's base; whoever
    /// trims calls this again then. Durable on return.
    pub async fn trim_before(&mut self, offset: u64) -> Result<(), Error> {
        if offset < self.start_offset || offset > self.next_offset {
            return Err(Error::OffsetOutOfRange { offset, start: self.start_offset, end: self.next_offset });
        }
        let active = self.active_base();
        let doomed: Vec<u64> = self
            .segments
            .iter()
            .filter(|&(&base, s)| base != active && s.next_offset <= offset)
            .map(|(&base, _)| base)
            .collect();
        self.start_offset = offset;
        // Oldest first: a crash in between leaves a log that still starts
        // at a segment base and has no gap.
        for base in doomed {
            let mut seg = self.segments.remove(&base).expect("listed above");
            remove_index(&self.disk, &mut seg).await?;
            self.disk.remove(&seg.path).await?;
        }
        Ok(())
    }

    /// Replaces the sealed segment that starts at `base` with `batches`,
    /// which must stand for the same batches one for one: the same first
    /// and last offset and the same term each, records left out at most
    /// (compaction, ADR 0067). The active segment is never rewritten. The
    /// new segment is written beside the old one, synced and renamed over
    /// it, so a crash leaves one or the other whole; [`Log::open`] removes
    /// a half-written one. Durable on return.
    pub async fn rewrite_segment(&mut self, base: u64, batches: &[Bytes]) -> Result<(), Error> {
        let Some(old) = self.segments.get(&base) else {
            return Err(Error::OffsetOutOfRange { offset: base, start: self.start_offset, end: self.next_offset });
        };
        if base == self.active_base() {
            return Err(Error::Rewrite(format!("segment {base} is the active segment")));
        }
        // What the old segment holds, batch by batch.
        let mut shape = Vec::new();
        let mut pos = 0;
        while pos < old.size {
            let b = self.batch_in(old, pos).await?;
            shape.push((b.base_offset(), b.last_offset(), b.term()));
            pos += b.len() as u64;
        }
        if shape.len() != batches.len() {
            return Err(Error::Rewrite(format!("{} batches for a segment of {}", batches.len(), shape.len())));
        }
        for (bytes, &(first, last, term)) in batches.iter().zip(&shape) {
            let (b, rest) = Batch::parse(bytes)?;
            if !rest.is_empty() {
                return Err(Error::InvalidBatch(lotse_record::Error::Malformed("trailing bytes after the batch")));
            }
            if (b.base_offset(), b.last_offset(), b.term()) != (first, last, term) {
                return Err(Error::Rewrite(format!(
                    "batch {}..={} of term {} where {first}..={last} of term {term} was",
                    b.base_offset(),
                    b.last_offset(),
                    b.term()
                )));
            }
        }
        let path = segment_path(&self.dir, base);
        let cleaned = cleaned_path(&self.dir, base);
        if self.disk.list(&self.dir).await?.contains(&cleaned) {
            self.disk.remove(&cleaned).await?;
        }
        let file = self.disk.open(&cleaned, OpenMode::CreateNew).await?;
        for bytes in batches {
            file.append(bytes.clone()).await?;
        }
        file.sync().await?;
        drop(file);
        // The old index describes the old bytes: gone before they are.
        let disk = self.disk.clone();
        remove_index(&disk, self.segments.get_mut(&base).expect("checked above")).await?;
        self.disk.rename(&cleaned, &path).await?;
        let mut seg = self.recover_segment(base, false).await?;
        write_index(&self.disk, base, &mut seg).await?;
        self.segments.insert(base, seg);
        Ok(())
    }

    fn active_base(&self) -> u64 {
        *self.segments.keys().next_back().expect("a log always has a segment")
    }

    /// Drops every segment and starts the log afresh at `offset`, as after
    /// installing a snapshot that ends right before it. Durable on return.
    pub async fn reset(&mut self, offset: u64) -> Result<(), Error> {
        // The new, empty segment first (unless it would clash with one we drop).
        let keep = self.segments.contains_key(&offset);
        for (base, mut seg) in std::mem::take(&mut self.segments) {
            remove_index(&self.disk, &mut seg).await?;
            if keep && base == offset {
                seg.file.truncate(0).await?;
                seg.file.sync().await?;
                let Segment { path, file, .. } = seg;
                self.segments.insert(base, Segment::empty(path, file, offset));
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

/// Writes the index file of a sealed segment beside it: to a temporary file,
/// synced, then renamed into place.
async fn write_index<D: Disk>(disk: &D, base: u64, seg: &mut Segment<D::File>) -> Result<(), Error> {
    if seg.size == 0 {
        return Ok(());
    }
    let path = seg.index_path();
    let tmp = path.with_extension("index.tmp");
    let file = disk.open(&tmp, OpenMode::CreateOrOpen).await?;
    file.truncate(0).await?;
    file.append(seg.to_index(base).encode()).await?;
    file.sync().await?;
    drop(file);
    disk.rename(&tmp, &path).await?;
    seg.indexed = true;
    Ok(())
}

/// Removes a segment's index file, if it has one.
async fn remove_index<D: Disk>(disk: &D, seg: &mut Segment<D::File>) -> Result<(), Error> {
    if seg.indexed {
        disk.remove(&seg.index_path()).await?;
        seg.indexed = false;
    }
    Ok(())
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

/// Path of the index file of the segment that starts at `base`.
pub fn index_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.index"))
}

/// Where a rewritten segment is written before it replaces the old one.
fn cleaned_path(dir: &Path, base: u64) -> PathBuf {
    dir.join(format!("{base:020}.log.cleaned"))
}

/// The base offset in a file name `<20 digits><suffix>`.
fn parse_name(path: &Path, suffix: &str) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let digits = name.strip_suffix(suffix)?;
    (digits.len() == 20).then(|| digits.parse().ok()).flatten()
}

#[cfg(test)]
mod tests;

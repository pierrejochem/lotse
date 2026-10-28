//! One Raft group, persisted.
//!
//! A [`Replica`] drives a [`RaftGroup`] the way the [`Ready`](lotse_raft::Ready)
//! contract asks, with real I/O on a [`Disk`]:
//!
//! 1. an installed snapshot goes to `snapshot` (written to a temporary
//!    file, synced, renamed over the old one), the state machine is restored
//!    from it and the log restarts right after it;
//! 2. truncations and new entries go to the log, which is synced;
//! 3. the hard state goes to `raft.state`, the same way as the snapshot;
//!    last, because its commit index refers to the log written before;
//! 4. only then are committed batches applied and messages sent.
//!
//! Every Raft entry is one record batch in the log: data batches as the
//! producer sent them (stamped with `base_offset = first - 1` and the term),
//! no-ops and membership changes as Raft control batches. On [`Replica::open`]
//! everything is rebuilt from what is on disk: the state machine from the
//! snapshot, the term runs and configurations from the log; committed
//! entries after the snapshot are applied again.
//!
//! The replica also keeps the producer state that makes produce idempotent
//! ([`crate::producers`]): checked on [`Replica::propose`], updated as the
//! log grows, is truncated and is applied, and saved in snapshots next to the
//! state machine's data.
//!
//! ```text
//! <dir>/raft.state   term, vote, commit (+ crc)
//! <dir>/snapshot     index, term, membership, state machine data (+ crc)
//! <dir>/log/         the partition log, also the Raft log
//! ```

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_log::{Log, LogConfig};
use lotse_raft::{
    Config, Entry, EntryKind, HardState, Membership, Message, NotLeader, RaftGroup, ReadState, Restored, Send,
    SnapshotMeta, TermRuns,
};
use lotse_record::{Batch, RaftControl};
use lotse_rt::{Disk, OpenMode, SegmentFile};

use crate::producers::{Check, DEFAULT_MAX_PRODUCERS, ProducerState};

const HARD_STATE: &str = "raft.state";
const SNAPSHOT: &str = "snapshot";
const FORMAT: u8 = 2;

/// What a replica applies committed batches to: a partition's producer state,
/// a queue's delivery state, the cluster metadata.
pub trait StateMachine: std::marker::Send + Sync + 'static {
    /// Applies a committed data batch, in log order. Raft's own entries
    /// (no-ops, membership changes) are not passed.
    fn apply(&mut self, batch: &Batch);
    /// The state as of the last applied batch.
    fn snapshot(&self) -> Bytes;
    /// Replaces the state with a snapshot's.
    fn restore(&mut self, data: &[u8]) -> Result<(), String>;
}

/// Errors from a replica. After one, the replica must be reopened.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The disk failed.
    #[error("disk error: {0}")]
    Io(#[from] io::Error),
    /// The log failed.
    #[error("log error: {0}")]
    Log(#[from] lotse_log::Error),
    /// A batch in the log is malformed.
    #[error("record error: {0}")]
    Record(#[from] lotse_record::Error),
    /// A state file is damaged, or the state machine refused a snapshot.
    #[error("corrupt state: {0}")]
    Corrupt(String),
}

/// Why [`Replica::propose`] refused a batch.
#[derive(Debug, thiserror::Error)]
pub enum ProposeError {
    /// This replica does not lead.
    #[error("not the leader (leader: {:?})", .0.leader)]
    NotLeader(NotLeader),
    /// Not a single valid data batch.
    #[error("invalid batch: {0}")]
    Invalid(#[from] lotse_record::Error),
    /// Control batches are written by the broker, not proposed.
    #[error("control batches cannot be proposed")]
    Control,
    /// The batch does not continue its producer's sequence.
    #[error("out of order: the producer's next sequence is {expected}")]
    OutOfOrderSequence {
        /// The sequence the batch should start with.
        expected: i32,
    },
    /// A newer epoch of the producer is active.
    #[error("fenced: the producer's current epoch is {current}")]
    ProducerFenced {
        /// The current epoch.
        current: u16,
    },
}

/// Where a proposed batch is in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposed {
    /// First index.
    pub first: u64,
    /// Last index.
    pub last: u64,
    /// The entry's term while it is not applied; `None` for a duplicate of
    /// a batch that is applied already (and so committed).
    pub term: Option<u64>,
    /// The batch was in the log already (an idempotent retry): nothing new
    /// was proposed, `first..=last` is the original.
    pub duplicate: bool,
}

/// What a [`Replica::process`] round produced for the node.
#[derive(Debug, Default)]
pub struct Output {
    /// Messages to send, filled.
    pub messages: Vec<Send>,
    /// Confirmed reads; the state machine has applied each one's index.
    pub reads: Vec<ReadState>,
    /// Reads that will not be confirmed here.
    pub reads_aborted: Vec<u64>,
}

/// One Raft group persisted in a directory. See the module docs.
pub struct Replica<D: Disk, S> {
    disk: D,
    dir: PathBuf,
    log: Log<D>,
    raft: RaftGroup,
    sm: S,
    applied: u64,
    snapshot: Option<(SnapshotMeta, Bytes)>,
    producers: ProducerState,
    /// Largest number of bytes per append message (at least one entry).
    pub max_fill_bytes: usize,
}

impl<D: Disk, S> fmt::Debug for Replica<D, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replica")
            .field("dir", &self.dir)
            .field("raft", &self.raft)
            .field("log", &self.log)
            .field("applied", &self.applied)
            .finish()
    }
}

impl<D: Disk, S: StateMachine> Replica<D, S> {
    /// Opens the replica in `dir` (creating it if needed) and recovers from
    /// whatever a crash left there. `sm` must be in its initial state.
    pub async fn open(
        disk: D,
        dir: impl Into<PathBuf>,
        cfg: Config,
        sm: S,
        log_config: LogConfig,
    ) -> Result<Self, Error> {
        Self::open_with(disk, dir, cfg, sm, log_config, DEFAULT_MAX_PRODUCERS).await
    }

    /// [`Replica::open`] keeping at most `max_producers` producers. Every
    /// replica of a group must use the same limit: it decides which
    /// producers are evicted.
    pub async fn open_with(
        disk: D,
        dir: impl Into<PathBuf>,
        cfg: Config,
        mut sm: S,
        log_config: LogConfig,
        max_producers: usize,
    ) -> Result<Self, Error> {
        let dir = dir.into();
        let mut producers = ProducerState::new(max_producers);
        disk.create_dir_all(&dir).await?;
        let snapshot = match read_file(&disk, &dir.join(SNAPSHOT)).await? {
            Some(bytes) => Some(decode_snapshot(bytes)?),
            None => None,
        };
        if let Some((_, data)) = &snapshot {
            let (p, d) = split_snapshot_data(data)?;
            producers.restore(&p).map_err(Error::Corrupt)?;
            sm.restore(&d).map_err(Error::Corrupt)?;
        }
        let hard_state = match read_file(&disk, &dir.join(HARD_STATE)).await? {
            Some(bytes) => decode_hard_state(bytes)?,
            None => HardState::default(),
        };
        let mut log = Log::open(disk.clone(), dir.join("log"), log_config).await?;
        let (snap_index, snap_term) = snapshot.as_ref().map_or((0, 0), |(m, _)| (m.index, m.term));
        // A crash between writing an installed snapshot and restarting the log
        // leaves the old log behind; it must continue the snapshot.
        if snapshot.is_some() && !continues(&log, snap_index, snap_term).await? {
            log.reset(snap_index).await?;
        }
        let mut runs =
            if snapshot.is_some() { TermRuns::with_snapshot(snap_index, snap_term) } else { TermRuns::new() };
        let mut configs = Vec::new();
        // Index i lives at offset i - 1, so the first index after the
        // snapshot is at offset `snap_index`.
        let mut offset = log.start_offset().max(snap_index);
        while offset < log.next_offset() {
            for batch in batches(log.read(offset, 1 << 20).await?)? {
                let e = to_entry(&batch)?;
                runs.append(e.first, e.last, e.term);
                // Applied entries are applied again below; until then, and
                // for the uncommitted rest, the producer state sees them as
                // the tail of the log.
                producers.appended(e.first, e.last, e.term, &batch);
                if e.kind == EntryKind::Config {
                    let m = Membership::decode(&e.data).ok_or_else(|| Error::Corrupt("bad membership".into()))?;
                    configs.push((e.last, m));
                }
                offset = batch.next_offset();
            }
        }
        let restored = Restored {
            hard_state,
            log: runs,
            applied: snap_index,
            snapshot: snapshot.as_ref().map(|(m, _)| m.clone()),
            configs,
        };
        Ok(Replica {
            raft: RaftGroup::new(cfg, restored),
            disk,
            dir,
            log,
            sm,
            applied: snap_index,
            snapshot,
            producers,
            max_fill_bytes: 1 << 20,
        })
    }

    /// The Raft group, to tick, step and ask about.
    pub fn raft(&mut self) -> &mut RaftGroup {
        &mut self.raft
    }

    /// The Raft group, read-only.
    pub fn raft_ref(&self) -> &RaftGroup {
        &self.raft
    }

    /// The state machine.
    pub fn state_machine(&self) -> &S {
        &self.sm
    }

    /// Highest index applied to the state machine.
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// The log.
    pub fn log(&self) -> &Log<D> {
        &self.log
    }

    /// Proposes a data batch as the leader. Returns the indices it occupies;
    /// it is stamped when it is persisted. An idempotent batch that is in
    /// the log already is not proposed again: the original is returned.
    pub fn propose(&mut self, batch: Bytes) -> Result<Proposed, ProposeError> {
        let (parsed, rest) = Batch::parse(&batch)?;
        if !rest.is_empty() {
            return Err(ProposeError::Invalid(lotse_record::Error::Malformed("more than one batch")));
        }
        if parsed.attributes().control {
            return Err(ProposeError::Control);
        }
        if parsed.producer_id() != 0 && parsed.base_sequence() < 0 {
            return Err(ProposeError::Invalid(lotse_record::Error::Malformed("idempotent batch without a sequence")));
        }
        // Only a leader answers for the log, duplicates included.
        if self.raft.role() != lotse_raft::Role::Leader {
            return Err(ProposeError::NotLeader(NotLeader { leader: self.raft.leader() }));
        }
        match self.producers.check(&parsed) {
            Check::New => {}
            Check::Duplicate { first, last, term } => return Ok(Proposed { first, last, term, duplicate: true }),
            Check::OutOfOrder { expected } => return Err(ProposeError::OutOfOrderSequence { expected }),
            Check::Fenced { current } => return Err(ProposeError::ProducerFenced { current }),
        }
        let (first, last) =
            self.raft.propose(u64::from(parsed.record_count()), batch).map_err(ProposeError::NotLeader)?;
        let term = self.raft.term();
        self.producers.appended(first, last, term, &parsed);
        Ok(Proposed { first, last, term: Some(term), duplicate: false })
    }

    /// Persists, applies and sends whatever the group has ready.
    pub async fn process(&mut self) -> Result<Output, Error> {
        if !self.raft.has_ready() {
            return Ok(Output::default());
        }
        let ready = self.raft.ready();
        if let Some(snap) = ready.snapshot {
            write_atomic(&self.disk, &self.dir, SNAPSHOT, encode_snapshot(&snap.meta, &snap.data)).await?;
            let (p, d) = split_snapshot_data(&snap.data)?;
            self.producers.restore(&p).map_err(Error::Corrupt)?;
            self.sm.restore(&d).map_err(Error::Corrupt)?;
            self.log.reset(snap.meta.index).await?;
            self.applied = snap.meta.index;
            self.snapshot = Some((snap.meta, snap.data));
        }
        if let Some(from) = ready.truncate_from {
            self.log.truncate_from(from - 1).await?;
            self.producers.truncated(from);
        }
        if !ready.entries.is_empty() {
            for e in &ready.entries {
                let batch = to_batch(e)?;
                if e.kind == EntryKind::Normal {
                    self.producers.appended(e.first, e.last, e.term, &Batch::parse(&batch)?.0);
                }
                self.log.append(batch).await?;
            }
            // Nothing may be acknowledged before it is durable.
            self.log.sync().await?;
        }
        // The hard state goes last: its commit index refers to the log and
        // snapshot written above. Written first, a crash could leave it
        // pointing into a log that still holds a conflicting suffix, and a
        // restart would apply entries that were never committed.
        if let Some(hs) = ready.hard_state {
            write_atomic(&self.disk, &self.dir, HARD_STATE, encode_hard_state(&hs)).await?;
        }
        if let Some((first, last)) = ready.committed {
            self.apply(first, last).await?;
        }
        self.raft.advance();
        let mut out = Output { messages: ready.messages, reads: ready.reads, reads_aborted: ready.reads_aborted };
        for s in &mut out.messages {
            self.fill(s).await?;
        }
        Ok(out)
    }

    async fn apply(&mut self, first: u64, last: u64) -> Result<(), Error> {
        if first != self.applied + 1 {
            return Err(Error::Corrupt(format!("applying from {first}, but {} is applied", self.applied)));
        }
        // Indices first..=last are offsets first-1..=last-1.
        let mut offset = first - 1;
        while offset < last {
            for batch in batches(self.log.read(offset, 1 << 20).await?)? {
                if batch.base_offset() != offset || batch.next_offset() > last {
                    return Err(Error::Corrupt(format!(
                        "committed range {first}..={last} does not end on batch boundaries (batch at {})",
                        batch.base_offset()
                    )));
                }
                self.producers.applied(batch.base_offset() + 1, batch.next_offset(), &batch);
                // Control batches (Raft's own, transaction markers) are not data.
                if !batch.attributes().control {
                    self.sm.apply(&batch);
                }
                offset = batch.next_offset();
                if offset == last {
                    break;
                }
            }
        }
        self.applied = last;
        Ok(())
    }

    async fn fill(&self, s: &mut Send) -> Result<(), Error> {
        if s.attach_snapshot {
            if let (Message::Snapshot { meta, data, .. }, Some((m, d))) = (&mut s.msg, &self.snapshot) {
                *meta = m.clone();
                *data = d.clone();
            }
            s.attach_snapshot = false;
            return Ok(());
        }
        let (Some(from), Message::Append { entries, .. }) = (s.fill_from, &mut s.msg) else { return Ok(()) };
        s.fill_from = None;
        if from <= self.raft.log().snapshot_index() || from > self.log.next_offset() {
            return Ok(()); // compacted since the core asked: send it empty
        }
        for batch in batches(self.log.read(from - 1, self.max_fill_bytes).await?)? {
            entries.push(to_entry(&batch)?);
        }
        if entries.first().is_some_and(|e| e.first != from) {
            return Err(Error::Corrupt(format!("append from {from} does not start at a batch")));
        }
        Ok(())
    }

    /// Snapshots the state machine at the applied index and lets the core
    /// forget the log before it. Returns the snapshot index. (The log's
    /// segments stay until retention removes them.)
    pub async fn compact(&mut self) -> Result<u64, Error> {
        let index = self.applied;
        if index <= self.raft.log().snapshot_index() {
            return Ok(self.raft.log().snapshot_index());
        }
        self.raft.compact(index);
        let meta = self.raft.snapshot_meta();
        let data = snapshot_data(&self.producers.encode(), &self.sm.snapshot());
        write_atomic(&self.disk, &self.dir, SNAPSHOT, encode_snapshot(&meta, &data)).await?;
        self.snapshot = Some((meta, data));
        Ok(index)
    }
}

/// `true` if the log holds the entry ending at the snapshot index with the
/// snapshot's term, or starts right after the snapshot.
async fn continues<D: Disk>(log: &Log<D>, index: u64, term: u64) -> Result<bool, Error> {
    if log.start_offset() == index {
        return Ok(true);
    }
    if index < log.start_offset() || index > log.next_offset() {
        return Ok(false);
    }
    let Some(batch) = batches(log.read(index - 1, 1).await?)?.into_iter().next() else { return Ok(false) };
    Ok(batch.last_offset() == index - 1 && batch.term() == term)
}

pub(crate) fn batches(mut bytes: Bytes) -> Result<Vec<Batch>, Error> {
    let mut out = Vec::new();
    while !bytes.is_empty() {
        let (batch, rest) = Batch::parse(&bytes)?;
        out.push(batch);
        bytes = rest;
    }
    Ok(out)
}

/// The Raft entry a stored batch is.
pub(crate) fn to_entry(batch: &Batch) -> Result<Entry, Error> {
    let (kind, data) = match lotse_record::raft_control(batch)? {
        Some((RaftControl::Noop, _)) => (EntryKind::Noop, Bytes::new()),
        Some((RaftControl::Config, payload)) => (EntryKind::Config, payload),
        None => (EntryKind::Normal, batch.as_bytes().clone()),
    };
    Ok(Entry { first: batch.base_offset() + 1, last: batch.last_offset() + 1, term: batch.term(), kind, data })
}

/// The batch a Raft entry is stored as, stamped. A data batch that already
/// carries the right offset and term (one read from a log) is returned as
/// is, without copying.
pub(crate) fn to_batch(e: &Entry) -> Result<Bytes, Error> {
    if e.kind == EntryKind::Normal
        && let Ok((b, rest)) = Batch::parse(&e.data)
        && rest.is_empty()
        && b.base_offset() + 1 == e.first
        && b.last_offset() + 1 == e.last
        && b.term() == e.term
    {
        return Ok(e.data.clone());
    }
    let raw = match e.kind {
        EntryKind::Normal => e.data.clone(),
        EntryKind::Noop => lotse_record::raft_control_batch(RaftControl::Noop, &[])?,
        EntryKind::Config => lotse_record::raft_control_batch(RaftControl::Config, &e.data)?,
    };
    let mut bytes = BytesMut::from(&raw[..]);
    lotse_record::stamp(&mut bytes, e.first - 1, e.term)?;
    let bytes = bytes.freeze();
    let (batch, _) = Batch::parse(&bytes)?;
    if batch.last_offset() + 1 != e.last {
        return Err(Error::Corrupt(format!("entry {}..={} holds {} records", e.first, e.last, batch.record_count())));
    }
    Ok(bytes)
}

async fn read_file<D: Disk>(disk: &D, path: &Path) -> Result<Option<Bytes>, Error> {
    let file = match disk.open(path, OpenMode::Existing).await {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let len = file.size().await? as usize;
    Ok(Some(file.read_at(0, len).await?))
}

/// Writes `name` so that a crash leaves either the old or the new contents.
async fn write_atomic<D: Disk>(disk: &D, dir: &Path, name: &str, bytes: Bytes) -> Result<(), Error> {
    let tmp = dir.join(format!("{name}.tmp"));
    let file = disk.open(&tmp, OpenMode::CreateOrOpen).await?;
    file.truncate(0).await?;
    file.append(bytes).await?;
    file.sync().await?;
    disk.rename(&tmp, &dir.join(name)).await?;
    Ok(())
}

fn seal(mut b: BytesMut) -> Bytes {
    let crc = crc32c::crc32c(&b);
    b.put_u32_le(crc);
    b.freeze()
}

fn unseal(bytes: Bytes, what: &str) -> Result<Bytes, Error> {
    if bytes.len() < 5 {
        return Err(Error::Corrupt(format!("{what} is truncated")));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().expect("4 bytes")) {
        return Err(Error::Corrupt(format!("{what} fails its checksum")));
    }
    if body[0] != FORMAT {
        return Err(Error::Corrupt(format!("{what} has unknown format {}", body[0])));
    }
    Ok(bytes.slice(1..bytes.len() - 4))
}

fn encode_hard_state(hs: &HardState) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(FORMAT);
    b.put_u64_le(hs.term);
    b.put_u8(u8::from(hs.vote.is_some()));
    b.put_u64_le(hs.vote.unwrap_or(0));
    b.put_u64_le(hs.commit);
    seal(b)
}

fn decode_hard_state(bytes: Bytes) -> Result<HardState, Error> {
    let mut b = unseal(bytes, HARD_STATE)?;
    if b.len() != 25 {
        return Err(Error::Corrupt(format!("{HARD_STATE} has the wrong size")));
    }
    let term = b.get_u64_le();
    let has_vote = b.get_u8() == 1;
    let vote = b.get_u64_le();
    let commit = b.get_u64_le();
    Ok(HardState { term, vote: has_vote.then_some(vote), commit })
}

/// A snapshot's data: the producer state, then the state machine's.
fn snapshot_data(producers: &[u8], sm: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(4 + producers.len() + sm.len());
    b.put_u32_le(producers.len() as u32);
    b.put_slice(producers);
    b.put_slice(sm);
    b.freeze()
}

fn split_snapshot_data(data: &Bytes) -> Result<(Bytes, Bytes), Error> {
    let short = || Error::Corrupt("snapshot data is truncated".into());
    let len = u32::from_le_bytes(data.get(..4).ok_or_else(short)?.try_into().expect("four bytes")) as usize;
    if data.len() < 4 + len {
        return Err(short());
    }
    Ok((data.slice(4..4 + len), data.slice(4 + len..)))
}

fn encode_snapshot(meta: &SnapshotMeta, data: &[u8]) -> Bytes {
    let membership = meta.membership.encode();
    let mut b = BytesMut::with_capacity(32 + membership.len() + data.len());
    b.put_u8(FORMAT);
    b.put_u64_le(meta.index);
    b.put_u64_le(meta.term);
    b.put_u32_le(membership.len() as u32);
    b.put_slice(&membership);
    b.put_u32_le(data.len() as u32);
    b.put_slice(data);
    seal(b)
}

fn decode_snapshot(bytes: Bytes) -> Result<(SnapshotMeta, Bytes), Error> {
    let mut b = unseal(bytes, SNAPSHOT)?;
    let short = || Error::Corrupt(format!("{SNAPSHOT} is truncated"));
    if b.len() < 20 {
        return Err(short());
    }
    let index = b.get_u64_le();
    let term = b.get_u64_le();
    let mlen = b.get_u32_le() as usize;
    if b.len() < mlen + 4 {
        return Err(short());
    }
    let membership = Membership::decode(&b.split_to(mlen)).ok_or_else(|| Error::Corrupt("bad membership".into()))?;
    let dlen = b.get_u32_le() as usize;
    if b.len() != dlen {
        return Err(short());
    }
    Ok((SnapshotMeta { index, term, membership }, b))
}

#[cfg(test)]
mod tests;

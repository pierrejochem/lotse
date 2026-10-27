//! An in-memory driver for tests and simulations.
//!
//! [`MemNode`] does what `lotse-server` does with `lotse-log`: persist a
//! [`Ready`](crate::Ready) in the documented order, apply committed entries,
//! fill outgoing appends from its log and attach its snapshot. Its state
//! machine is simply the list of applied entries, and a snapshot is that
//! list up to the snapshot index. It also checks, as it goes, the rules the
//! core promises: truncations never touch applied entries, committed ranges
//! fall on entry boundaries, and fills start at one.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    Config, Entry, EntryKind, HardState, Membership, Message, RaftGroup, ReadState, Restored, Send, SnapshotMeta,
    TermRuns,
};

/// Size of a batch header in `lotse-record`; counts towards the fill limit.
const BATCH_HEADER: usize = 65;

/// One node with its durable state kept in memory.
#[derive(Debug)]
pub struct MemNode {
    /// The Raft state machine.
    pub raft: RaftGroup,
    /// Persisted entries after the snapshot, in order.
    pub log: Vec<Entry>,
    /// Persisted hard state.
    pub hard_state: HardState,
    /// Applied entries, in order, the snapshot's included.
    pub applied: Vec<Entry>,
    /// The latest snapshot: where it stands and its encoded entries.
    pub snapshot: Option<(SnapshotMeta, Bytes)>,
    /// Largest number of bytes (headers included) per append; at least one
    /// entry is always sent.
    pub max_fill_bytes: usize,
    /// Reads confirmed so far, with the index applied when each was served.
    pub reads: Vec<(ReadState, u64)>,
    /// Reads aborted so far.
    pub reads_aborted: Vec<u64>,
    cfg: Config,
}

impl MemNode {
    /// A fresh node.
    pub fn new(cfg: Config) -> Self {
        MemNode {
            raft: RaftGroup::new(cfg.clone(), Restored::default()),
            log: Vec::new(),
            hard_state: HardState::default(),
            applied: Vec::new(),
            snapshot: None,
            max_fill_bytes: 64 * 1024,
            reads: Vec::new(),
            reads_aborted: Vec::new(),
            cfg,
        }
    }

    /// Simulates a crash and restart: everything not persisted is gone, the
    /// Raft state is rebuilt from the snapshot, the log, the hard state and
    /// what was applied.
    pub fn restart(&mut self) {
        let mut log = match &self.snapshot {
            Some((meta, _)) => TermRuns::with_snapshot(meta.index, meta.term),
            None => TermRuns::new(),
        };
        let mut configs = Vec::new();
        for e in &self.log {
            log.append(e.first, e.last, e.term);
            if e.kind == EntryKind::Config {
                configs.push((e.last, Membership::decode(&e.data).expect("config entry")));
            }
        }
        let applied = self.applied.last().map_or(0, |e| e.last);
        let restored = Restored {
            hard_state: self.hard_state,
            log,
            applied,
            snapshot: self.snapshot.as_ref().map(|s| s.0.clone()),
            configs,
        };
        self.raft = RaftGroup::new(self.cfg.clone(), restored);
    }

    /// Index after which the log starts.
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.as_ref().map_or(0, |s| s.0.index)
    }

    /// Takes a snapshot of everything applied and compacts the log. Returns
    /// the snapshot index.
    pub fn compact(&mut self) -> u64 {
        let Some(last) = self.applied.last().map(|e| e.last) else { return 0 };
        if last <= self.snapshot_index() {
            return last;
        }
        // Only what the log persisted can be compacted, and it is all applied.
        let pos = self.log.partition_point(|e| e.last <= last);
        if pos == 0 || self.log[pos - 1].last != last {
            return self.snapshot_index();
        }
        self.raft.compact(last);
        let meta = self.raft.snapshot_meta();
        assert_eq!(meta.index, last);
        self.snapshot = Some((meta, encode(&self.applied)));
        self.log.drain(..pos);
        last
    }

    /// Persists and applies the group's ready, returns the messages to send,
    /// already filled.
    pub fn process(&mut self) -> Vec<Send> {
        if !self.raft.has_ready() {
            return Vec::new();
        }
        let ready = self.raft.ready();
        if let Some(snap) = ready.snapshot {
            let entries = decode(&snap.data);
            let applied = self.applied.last().map_or(0, |e| e.last);
            assert!(snap.meta.index > applied, "node {} installed a snapshot it had applied", self.cfg.id);
            assert_eq!(entries.last().map_or(0, |e| e.last), snap.meta.index, "snapshot data matches its meta");
            self.applied = entries;
            self.log.clear();
            self.snapshot = Some((snap.meta, snap.data));
        }
        if let Some(from) = ready.truncate_from {
            let applied = self.applied.last().map_or(0, |e| e.last);
            assert!(from > applied, "node {} truncated applied index {from}", self.cfg.id);
            let pos = self.log.partition_point(|e| e.last < from);
            if let Some(e) = self.log.get(pos) {
                assert_eq!(e.first, from, "truncation at {from} is not an entry boundary");
            }
            self.log.truncate(pos);
        }
        for e in ready.entries {
            let next = self.log.last().map_or(self.snapshot_index() + 1, |l| l.last + 1);
            assert_eq!(e.first, next, "node {} appended a gap", self.cfg.id);
            self.log.push(e);
        }
        if let Some(hs) = ready.hard_state {
            assert!(hs.term >= self.hard_state.term, "term went backwards");
            self.hard_state = hs;
        }
        if let Some((first, last)) = ready.committed {
            let start = self.log.partition_point(|e| e.last < first);
            let end = self.log.partition_point(|e| e.last <= last);
            let slice = &self.log[start..end];
            assert_eq!(slice.first().map(|e| e.first), Some(first), "committed range starts inside an entry");
            assert_eq!(slice.last().map(|e| e.last), Some(last), "committed range ends inside an entry");
            self.applied.extend_from_slice(slice);
        }
        // Reads are served after applying; their index is applied by now.
        let applied = self.applied.last().map_or(0, |e| e.last);
        for r in ready.reads {
            assert!(r.index <= applied, "read {} served before its index {} was applied", r.ctx, r.index);
            self.reads.push((r, applied));
        }
        self.reads_aborted.extend(ready.reads_aborted);
        self.raft.advance();
        let mut out = ready.messages;
        for s in &mut out {
            self.fill(s);
        }
        out
    }

    /// Fills an append's entries from the log, starting at `fill_from`, or
    /// attaches the snapshot.
    pub fn fill(&self, s: &mut Send) {
        if s.attach_snapshot {
            if let (Message::Snapshot { meta, data, .. }, Some((m, d))) = (&mut s.msg, &self.snapshot) {
                *meta = m.clone();
                *data = d.clone();
            }
            s.attach_snapshot = false;
            return;
        }
        let (Some(from), Message::Append { entries, .. }) = (s.fill_from, &mut s.msg) else { return };
        s.fill_from = None;
        if from <= self.snapshot_index() {
            return; // compacted since the core asked: send it empty
        }
        let mut pos = self.log.partition_point(|e| e.last < from);
        if let Some(e) = self.log.get(pos) {
            assert_eq!(e.first, from, "fill_from {from} is not an entry boundary");
        }
        let mut bytes = 0;
        while let Some(e) = self.log.get(pos) {
            // Every entry is a batch with a 65-byte header, payload or not.
            let size = BATCH_HEADER + e.data.len();
            if !entries.is_empty() && bytes + size > self.max_fill_bytes {
                break;
            }
            bytes += size;
            entries.push(e.clone());
            pos += 1;
        }
    }

    /// Proposes a batch of `records` records holding `data`.
    pub fn propose(&mut self, records: u64, data: impl Into<Bytes>) -> Result<(u64, u64), crate::NotLeader> {
        self.raft.propose(records, data.into())
    }

    /// Applied normal entries' data, in order (no-ops and configs left out).
    pub fn applied_data(&self) -> Vec<Bytes> {
        self.applied.iter().filter(|e| e.kind == EntryKind::Normal).map(|e| e.data.clone()).collect()
    }

    /// Everything durable, in order: the snapshot's entries, then the log.
    /// The snapshot is always a prefix of `applied`, so it is read from there.
    pub fn durable(&self) -> impl Iterator<Item = &Entry> {
        let snap = self.snapshot_index();
        let k = self.applied.partition_point(|e| e.last <= snap);
        self.applied[..k].iter().chain(self.log.iter())
    }
}

fn encode(entries: &[Entry]) -> Bytes {
    let mut b = BytesMut::new();
    for e in entries {
        b.put_u64_le(e.first);
        b.put_u64_le(e.last);
        b.put_u64_le(e.term);
        b.put_u8(match e.kind {
            EntryKind::Normal => 0,
            EntryKind::Noop => 1,
            EntryKind::Config => 2,
        });
        b.put_u32_le(e.data.len() as u32);
        b.put_slice(&e.data);
    }
    b.freeze()
}

fn decode(mut b: &[u8]) -> Vec<Entry> {
    let mut out = Vec::new();
    while b.has_remaining() {
        let (first, last, term) = (b.get_u64_le(), b.get_u64_le(), b.get_u64_le());
        let kind = match b.get_u8() {
            0 => EntryKind::Normal,
            1 => EntryKind::Noop,
            _ => EntryKind::Config,
        };
        let len = b.get_u32_le() as usize;
        let data = Bytes::copy_from_slice(&b[..len]);
        b.advance(len);
        out.push(Entry { first, last, term, kind, data });
    }
    out
}

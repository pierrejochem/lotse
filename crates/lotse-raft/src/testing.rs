//! An in-memory driver for tests and simulations.
//!
//! [`MemNode`] does what `lotse-server` will do with `lotse-log`: persist a
//! [`Ready`](crate::Ready) in the documented order, apply committed entries,
//! and fill outgoing appends from its log. It also checks, as it goes, the
//! rules the core promises: truncations never touch applied entries,
//! committed ranges fall on entry boundaries, and fills start at one.

use bytes::Bytes;

use crate::{Config, Entry, EntryKind, HardState, Message, RaftGroup, Restored, Send, TermRuns};

/// Size of a batch header in `lotse-record`; counts towards the fill limit.
const BATCH_HEADER: usize = 65;

/// One node with its durable state kept in memory.
#[derive(Debug)]
pub struct MemNode {
    /// The Raft state machine.
    pub raft: RaftGroup,
    /// Persisted entries, in order.
    pub log: Vec<Entry>,
    /// Persisted hard state.
    pub hard_state: HardState,
    /// Applied entries, in order.
    pub applied: Vec<Entry>,
    /// Largest number of bytes (headers included) per append; at least one
    /// entry is always sent.
    pub max_fill_bytes: usize,
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
            max_fill_bytes: 64 * 1024,
            cfg,
        }
    }

    /// Simulates a crash and restart: everything not persisted is gone, the
    /// Raft state is rebuilt from the log, the hard state and what was applied.
    pub fn restart(&mut self) {
        let log = TermRuns::from_entries(self.log.iter().map(|e| (e.first, e.last, e.term)));
        let applied = self.applied.last().map_or(0, |e| e.last);
        self.raft = RaftGroup::new(self.cfg.clone(), Restored { hard_state: self.hard_state, log, applied });
    }

    /// Persists and applies the group's ready, returns the messages to send,
    /// already filled.
    pub fn process(&mut self) -> Vec<Send> {
        if !self.raft.has_ready() {
            return Vec::new();
        }
        let ready = self.raft.ready();
        if let Some(hs) = ready.hard_state {
            assert!(hs.term >= self.hard_state.term, "term went backwards");
            self.hard_state = hs;
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
            let next = self.log.last().map_or(1, |l| l.last + 1);
            assert_eq!(e.first, next, "node {} appended a gap", self.cfg.id);
            self.log.push(e);
        }
        if let Some((first, last)) = ready.committed {
            let start = self.log.partition_point(|e| e.last < first);
            let end = self.log.partition_point(|e| e.last <= last);
            let slice = &self.log[start..end];
            assert_eq!(slice.first().map(|e| e.first), Some(first), "committed range starts inside an entry");
            assert_eq!(slice.last().map(|e| e.last), Some(last), "committed range ends inside an entry");
            self.applied.extend_from_slice(slice);
        }
        self.raft.advance();
        let mut out = ready.messages;
        for s in &mut out {
            self.fill(s);
        }
        out
    }

    /// Fills an append's entries from the log, starting at `fill_from`.
    pub fn fill(&self, s: &mut Send) {
        let (Some(from), Message::Append { entries, .. }) = (s.fill_from, &mut s.msg) else { return };
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
        s.fill_from = None;
    }

    /// Proposes a batch of `records` records holding `data`.
    pub fn propose(&mut self, records: u64, data: impl Into<Bytes>) -> Result<(u64, u64), crate::NotLeader> {
        self.raft.propose(records, data.into())
    }

    /// Applied normal entries' data, in order (no-ops left out).
    pub fn applied_data(&self) -> Vec<Bytes> {
        self.applied.iter().filter(|e| e.kind == EntryKind::Normal).map(|e| e.data.clone()).collect()
    }
}

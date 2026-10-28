//! State of the transaction coordinator, kept in the coordinator group.
//!
//! Every change to a transactional id is one record (an internal control
//! batch) holding its whole new state: producer id, epoch, phase, timeout and
//! the partitions in the transaction. Applying a record replaces the id's
//! state, so the state is a pure function of the log. A record with producer
//! id 0 asks for a new id, which becomes the record's own Raft index (unique,
//! like the ids of idempotent producers).
//!
//! The phases follow the design: `Empty` → `Ongoing` (partitions added) →
//! `PrepareCommit`/`PrepareAbort` (decided; markers are being written) →
//! `CompleteCommit`/`CompleteAbort` (every partition has its marker).
//!
//! The coordinator group also holds consumer groups ([`crate::groups`]).
//! Offsets committed within a transaction wait in its state and become the
//! group's committed offsets when its `PrepareCommit` is applied: the
//! decision is durable then, and offsets need no markers since they live in
//! this same group. A `PrepareAbort` drops them.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::{Batch, Control, InternalControl};

use crate::groups::{Committed, GroupsState, OffsetKey, get_offset, put_offset};

const VERSION: u8 = 2;

/// Where a transaction is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    /// No transaction running.
    Empty,
    /// Partitions added, not ended.
    Ongoing,
    /// Decided to commit; markers being written.
    PrepareCommit,
    /// Decided to abort; markers being written.
    PrepareAbort,
    /// Committed everywhere.
    CompleteCommit,
    /// Aborted everywhere.
    CompleteAbort,
}

impl Phase {
    fn to_u8(self) -> u8 {
        match self {
            Phase::Empty => 0,
            Phase::Ongoing => 1,
            Phase::PrepareCommit => 2,
            Phase::PrepareAbort => 3,
            Phase::CompleteCommit => 4,
            Phase::CompleteAbort => 5,
        }
    }

    fn from_u8(v: u8) -> Option<Phase> {
        Some(match v {
            0 => Phase::Empty,
            1 => Phase::Ongoing,
            2 => Phase::PrepareCommit,
            3 => Phase::PrepareAbort,
            4 => Phase::CompleteCommit,
            5 => Phase::CompleteAbort,
            _ => return None,
        })
    }

    /// Markers still to be written: `Some(commit)`.
    pub(crate) fn preparing(self) -> Option<bool> {
        match self {
            Phase::PrepareCommit => Some(true),
            Phase::PrepareAbort => Some(false),
            _ => None,
        }
    }
}

/// The state of one transactional id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TxnMeta {
    /// 0 in a record that asks for a new id.
    pub(crate) producer_id: u64,
    pub(crate) epoch: u16,
    pub(crate) phase: Phase,
    pub(crate) timeout_ms: u32,
    /// `(resource id, partition)`.
    pub(crate) partitions: BTreeSet<(u64, u32)>,
    /// Consumer group offsets committed within the transaction.
    pub(crate) offsets: BTreeMap<OffsetKey, Committed>,
}

fn put_meta(b: &mut BytesMut, txn_id: &str, m: &TxnMeta) {
    b.put_u16_le(txn_id.len() as u16);
    b.put_slice(txn_id.as_bytes());
    b.put_u64_le(m.producer_id);
    b.put_u16_le(m.epoch);
    b.put_u8(m.phase.to_u8());
    b.put_u32_le(m.timeout_ms);
    b.put_u32_le(m.partitions.len() as u32);
    for (r, p) in &m.partitions {
        b.put_u64_le(*r);
        b.put_u32_le(*p);
    }
    b.put_u32_le(m.offsets.len() as u32);
    for ((g, r, p), c) in &m.offsets {
        b.put_u16_le(g.len() as u16);
        b.put_slice(g.as_bytes());
        put_offset(b, (*r, *p), c);
    }
}

fn get_meta(b: &mut &[u8]) -> Result<(String, TxnMeta), String> {
    let short = || "transaction record is truncated".to_string();
    if b.remaining() < 2 {
        return Err(short());
    }
    let n = usize::from(b.get_u16_le());
    if b.remaining() < n + 19 {
        return Err(short());
    }
    let txn_id = String::from_utf8(b[..n].to_vec()).map_err(|_| "transactional id is not UTF-8".to_string())?;
    b.advance(n);
    let (producer_id, epoch) = (b.get_u64_le(), b.get_u16_le());
    let phase = Phase::from_u8(b.get_u8()).ok_or("unknown transaction phase")?;
    let timeout_ms = b.get_u32_le();
    let count = b.get_u32_le() as usize;
    if b.remaining() < count.saturating_mul(12) {
        return Err(short());
    }
    let partitions = (0..count).map(|_| (b.get_u64_le(), b.get_u32_le())).collect();
    if b.remaining() < 4 {
        return Err(short());
    }
    let mut offsets = BTreeMap::new();
    for _ in 0..b.get_u32_le() {
        if b.remaining() < 2 {
            return Err(short());
        }
        let n = usize::from(b.get_u16_le());
        if b.remaining() < n {
            return Err(short());
        }
        let g = String::from_utf8(b[..n].to_vec()).map_err(|_| "group id is not UTF-8".to_string())?;
        b.advance(n);
        let ((r, p), c) = get_offset(b)?;
        offsets.insert((g, r, p), c);
    }
    Ok((txn_id, TxnMeta { producer_id, epoch, phase, timeout_ms, partitions, offsets }))
}

/// The record that sets `txn_id` to `meta`, as an internal control batch.
pub(crate) fn record(txn_id: &str, meta: &TxnMeta) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(VERSION);
    put_meta(&mut b, txn_id, meta);
    lotse_record::internal_control_batch(InternalControl::TxnState, &b).expect("one record")
}

/// See the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CoordinatorState {
    txns: BTreeMap<String, TxnMeta>,
    groups: GroupsState,
}

impl CoordinatorState {
    /// The state of `txn_id`, as applied.
    pub(crate) fn get(&self, txn_id: &str) -> Option<&TxnMeta> {
        self.txns.get(txn_id)
    }

    /// Every transactional id and its state.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &TxnMeta)> {
        self.txns.iter()
    }

    /// Consumer groups and their committed offsets, as applied.
    pub(crate) fn groups(&self) -> &GroupsState {
        &self.groups
    }

    /// Whether an open transaction holds an offset for `key` that is not
    /// committed yet.
    pub(crate) fn offset_pending(&self, key: &OffsetKey) -> bool {
        self.txns.values().any(|m| m.phase == Phase::Ongoing && m.offsets.contains_key(key))
    }

    /// Applies a committed batch at `first..=last`; anything but a
    /// coordinator record is ignored. A damaged record is an error: the log
    /// cannot be trusted.
    pub(crate) fn applied(&mut self, last: u64, batch: &Batch) -> Result<(), String> {
        let payload = match lotse_record::control(batch) {
            Ok(Some(Control::Internal(InternalControl::TxnState, payload))) => payload,
            Ok(Some(Control::Internal(InternalControl::GroupState, payload))) => {
                return self.groups.apply_group(&payload);
            }
            Ok(Some(Control::Internal(InternalControl::GroupOffsets, payload))) => {
                return self.groups.apply_offsets(&payload);
            }
            _ => return Ok(()),
        };
        let mut b = &payload[..];
        if b.remaining() < 1 || b.get_u8() != VERSION {
            return Err("transaction record has an unknown version".into());
        }
        let (txn_id, mut meta) = get_meta(&mut b)?;
        if meta.producer_id == 0 {
            meta.producer_id = last;
        }
        // The decision is durable: the transaction's offsets take effect.
        if meta.phase == Phase::PrepareCommit {
            for (k, c) in &meta.offsets {
                self.groups.offsets.insert(k.clone(), c.clone());
            }
        }
        self.txns.insert(txn_id, meta);
        Ok(())
    }

    /// The applied state, for a snapshot.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32_le(self.txns.len() as u32);
        for (id, m) in &self.txns {
            put_meta(&mut b, id, m);
        }
        self.groups.encode(&mut b);
        b.freeze()
    }

    /// Replaces everything with a snapshot's state.
    pub(crate) fn restore(&mut self, mut b: &[u8]) -> Result<(), String> {
        if b.remaining() < 4 {
            return Err("coordinator state is truncated".into());
        }
        let mut txns = BTreeMap::new();
        for _ in 0..b.get_u32_le() {
            let (id, m) = get_meta(&mut b)?;
            txns.insert(id, m);
        }
        let groups = GroupsState::decode(&mut b)?;
        if b.has_remaining() {
            return Err("coordinator state has trailing bytes".into());
        }
        self.txns = txns;
        self.groups = groups;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_apply_and_the_state_round_trips() {
        let mut s = CoordinatorState::default();
        let m = TxnMeta {
            producer_id: 0,
            epoch: 0,
            phase: Phase::Empty,
            timeout_ms: 60_000,
            partitions: BTreeSet::new(),
            offsets: BTreeMap::new(),
        };
        let r = record("payments", &m);
        s.applied(17, &Batch::parse(&r).unwrap().0).unwrap();
        assert_eq!(s.get("payments").unwrap().producer_id, 17, "a new id is the record's index");
        let m2 = TxnMeta {
            producer_id: 17,
            epoch: 3,
            phase: Phase::PrepareCommit,
            timeout_ms: 1_000,
            partitions: [(7, 0), (7, 1)].into(),
            offsets: BTreeMap::new(),
        };
        s.applied(20, &Batch::parse(&record("payments", &m2)).unwrap().0).unwrap();
        assert_eq!(s.get("payments"), Some(&m2));
        // Offsets in a transaction wait for its commit decision.
        let key = ("g".to_string(), 7, 0);
        let c = Committed { offset: 5, leader_term: 1, metadata: None };
        let ongoing = TxnMeta { phase: Phase::Ongoing, offsets: [(key.clone(), c.clone())].into(), ..m2.clone() };
        s.applied(21, &Batch::parse(&record("payments", &ongoing)).unwrap().0).unwrap();
        assert!(s.offset_pending(&key) && !s.groups().offsets.contains_key(&key));
        let commit = TxnMeta { phase: Phase::PrepareCommit, ..ongoing.clone() };
        s.applied(22, &Batch::parse(&record("payments", &commit)).unwrap().0).unwrap();
        assert!(!s.offset_pending(&key) && s.groups().offsets.get(&key) == Some(&c), "committed with the decision");
        let other = ("g".to_string(), 7, 1);
        let aborted =
            TxnMeta { phase: Phase::PrepareAbort, offsets: [(other.clone(), c.clone())].into(), ..m2.clone() };
        s.applied(23, &Batch::parse(&record("payments", &aborted)).unwrap().0).unwrap();
        assert!(!s.groups().offsets.contains_key(&other), "an abort drops them");
        let bytes = s.encode();
        let mut back = CoordinatorState::default();
        back.restore(&bytes).unwrap();
        assert_eq!(back, s);
        for cut in 0..bytes.len() {
            assert!(CoordinatorState::default().restore(&bytes[..cut]).is_err(), "cut at {cut}");
        }
    }
}

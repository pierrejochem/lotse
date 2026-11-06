//! Transaction state of one partition replica.
//!
//! A transactional batch opens its producer's transaction in the partition
//! (if none is open); a marker closes it. Ending a transaction bumps the
//! producer epoch: the coordinator writes the markers with the new epoch, and
//! a marker only closes an open transaction whose data has an *older* epoch.
//! A marker that arrives twice (a retry delivered late) therefore cannot
//! close the producer's next transaction, which runs at the new epoch. Each
//! marker also carries the coordinator's epoch (its Raft term); markers from
//! an older coordinator than one already seen for the producer are refused.
//!
//! From the applied state:
//!
//! - the **last stable offset**: below it every transaction is decided. It
//!   is at most the applied index, since the fate of a transaction that starts
//!   in an entry not applied yet is not known here;
//! - the **aborted transactions**, as `(producer, first, last)` index ranges,
//!   which `read_committed` consumers skip.
//!
//! Like the producer state there are two parts: the applied state (kept in
//! snapshots) and the tail of the log not applied yet, which a leader checks
//! markers against.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_record::{Batch, Control, ControlType};

/// An open transaction: its first index and the epoch of its data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Open {
    first: u64,
    epoch: u16,
}

/// An aborted transaction, by Raft index: data from `first`, closed by the
/// marker at `last`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Aborted {
    pub(crate) producer_id: u64,
    pub(crate) first: u64,
    pub(crate) last: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Item {
    /// Transactional data.
    Data { epoch: u16 },
    /// A marker.
    Marker { epoch: u16, coordinator_epoch: u32 },
}

/// What a leader may do with a marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkerCheck {
    /// Append it.
    Append,
    /// A newer coordinator wrote markers for this producer already.
    StaleCoordinator,
    /// Nothing to do: the transaction is closed already, or the producer is
    /// past this marker's epoch (a late retry, which must not close the
    /// current transaction). The coordinator counts it as delivered.
    Done,
}

/// The view of one producer, applied state plus tail.
#[derive(Debug, Clone, Copy, Default)]
struct View {
    open: Option<Open>,
    epoch: Option<u16>,
    coordinator_epoch: Option<u32>,
}

/// See the module docs.
#[derive(Debug, Clone, Default)]
pub(crate) struct TxnState {
    open: BTreeMap<u64, Open>,
    /// Highest epoch seen per producer, from data and markers.
    epochs: BTreeMap<u64, u16>,
    coordinator_epochs: BTreeMap<u64, u32>,
    /// In the order their markers were applied.
    aborted: Vec<Aborted>,
    tail: BTreeMap<u64, (u64, Item)>,
}

/// The transaction-relevant content of a batch: producer and item.
fn item_of(batch: &Batch) -> Option<(u64, Item)> {
    let a = batch.attributes();
    let producer_id = batch.producer_id();
    if producer_id == 0 || !a.transactional {
        return None;
    }
    if !a.control {
        return Some((producer_id, Item::Data { epoch: batch.producer_epoch() }));
    }
    match lotse_record::control(batch) {
        Ok(Some(Control::Marker(_, coordinator_epoch))) => {
            Some((producer_id, Item::Marker { epoch: batch.producer_epoch(), coordinator_epoch }))
        }
        _ => None,
    }
}

impl View {
    /// Applies one item; the same rules as for the applied state.
    fn step(&mut self, item: Item) {
        match item {
            Item::Data { epoch } => {
                if self.open.is_none() {
                    self.open = Some(Open { first: 0, epoch });
                }
                self.epoch = self.epoch.max(Some(epoch));
            }
            Item::Marker { epoch, coordinator_epoch } => {
                if self.coordinator_epoch.is_some_and(|c| coordinator_epoch < c) {
                    return;
                }
                if self.open.is_some_and(|o| o.epoch < epoch) {
                    self.open = None;
                }
                self.epoch = self.epoch.max(Some(epoch));
                self.coordinator_epoch = self.coordinator_epoch.max(Some(coordinator_epoch));
            }
        }
    }
}

impl TxnState {
    fn view(&self, producer_id: u64) -> View {
        let mut v = View {
            open: self.open.get(&producer_id).copied(),
            epoch: self.epochs.get(&producer_id).copied(),
            coordinator_epoch: self.coordinator_epochs.get(&producer_id).copied(),
        };
        for (p, item) in self.tail.values() {
            if *p == producer_id {
                v.step(*item);
            }
        }
        v
    }

    /// Whether a leader may append this marker for `producer_id`.
    pub(crate) fn check_marker(&self, producer_id: u64, epoch: u16, coordinator_epoch: u32) -> MarkerCheck {
        let v = self.view(producer_id);
        if v.coordinator_epoch.is_some_and(|c| coordinator_epoch < c) {
            return MarkerCheck::StaleCoordinator;
        }
        // Needed if it closes an open transaction of an older epoch, or raises
        // the producer's epoch (which also fences a zombie's late data here).
        let closes = v.open.is_some_and(|o| o.epoch < epoch);
        let raises = v.epoch.is_none_or(|e| epoch > e);
        if closes || raises { MarkerCheck::Append } else { MarkerCheck::Done }
    }

    /// Whether `producer_id` has a transaction open in the whole log: then
    /// its non-transactional batches are refused.
    pub(crate) fn has_open(&self, producer_id: u64) -> bool {
        self.view(producer_id).open.is_some()
    }

    /// Notes a batch appended at `first`.
    pub(crate) fn appended(&mut self, first: u64, batch: &Batch) {
        if let Some(entry) = item_of(batch) {
            self.tail.insert(first, entry);
        }
    }

    /// Raft removed every index from `from` on.
    pub(crate) fn truncated(&mut self, from: u64) {
        self.tail.split_off(&from);
    }

    /// Applies a committed batch at `first..=last`.
    pub(crate) fn applied(&mut self, first: u64, last: u64, batch: &Batch) {
        self.tail = self.tail.split_off(&(last + 1));
        let Some((producer_id, item)) = item_of(batch) else { return };
        match item {
            Item::Data { epoch } => {
                self.open.entry(producer_id).or_insert(Open { first, epoch });
                let e = self.epochs.entry(producer_id).or_insert(epoch);
                *e = (*e).max(epoch);
            }
            Item::Marker { epoch, coordinator_epoch } => {
                // A leader never appends a stale marker; every replica skips
                // one the same way, should one be in the log.
                if self.coordinator_epochs.get(&producer_id).is_some_and(|c| coordinator_epoch < *c) {
                    return;
                }
                if let Some(open) = self.open.get(&producer_id).copied().filter(|o| o.epoch < epoch) {
                    self.open.remove(&producer_id);
                    let aborted =
                        matches!(lotse_record::control(batch), Ok(Some(Control::Marker(ControlType::Abort, _))));
                    if aborted {
                        self.aborted.push(Aborted { producer_id, first: open.first, last });
                    }
                }
                let e = self.epochs.entry(producer_id).or_insert(epoch);
                *e = (*e).max(epoch);
                let c = self.coordinator_epochs.entry(producer_id).or_insert(coordinator_epoch);
                *c = (*c).max(coordinator_epoch);
            }
        }
    }

    /// The last stable offset: offsets below it are applied here and not in
    /// an open transaction. `applied` is the applied Raft index, so offsets
    /// below it are applied (index = offset + 1).
    pub(crate) fn last_stable_offset(&self, applied: u64) -> u64 {
        self.open.values().map(|o| o.first - 1).fold(applied, u64::min)
    }

    /// Whether transactional data of `producer_id` at `offset` belongs to
    /// an aborted transaction.
    pub(crate) fn aborted_at(&self, producer_id: u64, offset: u64) -> bool {
        self.aborted.iter().any(|a| a.producer_id == producer_id && a.first - 1 <= offset && offset < a.last - 1)
    }

    /// Aborted transactions with data in the offsets `from..to`, as
    /// `(producer, first offset)`.
    pub(crate) fn aborted_between(&self, from: u64, to: u64) -> Vec<(u64, u64)> {
        let mut out: Vec<(u64, u64)> = self
            .aborted
            .iter()
            .filter(|a| a.last > from && a.first - 1 < to)
            .map(|a| (a.producer_id, a.first - 1))
            .collect();
        out.sort_by_key(|(_, first)| *first);
        out
    }

    /// The applied state, for a snapshot.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32_le(self.open.len() as u32);
        for (p, o) in &self.open {
            b.put_u64_le(*p);
            b.put_u64_le(o.first);
            b.put_u16_le(o.epoch);
        }
        b.put_u32_le(self.epochs.len() as u32);
        for (p, e) in &self.epochs {
            b.put_u64_le(*p);
            b.put_u16_le(*e);
        }
        b.put_u32_le(self.coordinator_epochs.len() as u32);
        for (p, c) in &self.coordinator_epochs {
            b.put_u64_le(*p);
            b.put_u32_le(*c);
        }
        b.put_u32_le(self.aborted.len() as u32);
        for a in &self.aborted {
            b.put_u64_le(a.producer_id);
            b.put_u64_le(a.first);
            b.put_u64_le(a.last);
        }
        b.freeze()
    }

    /// Replaces everything with a snapshot's applied state.
    pub(crate) fn restore(&mut self, mut b: &[u8]) -> Result<(), String> {
        fn count(b: &mut &[u8], each: usize) -> Result<usize, String> {
            if b.remaining() < 4 {
                return Err("transaction state is truncated".into());
            }
            let n = b.get_u32_le() as usize;
            if b.remaining() < n.saturating_mul(each) {
                return Err("transaction state is truncated".into());
            }
            Ok(n)
        }
        let mut s = TxnState::default();
        for _ in 0..count(&mut b, 18)? {
            let p = b.get_u64_le();
            s.open.insert(p, Open { first: b.get_u64_le(), epoch: b.get_u16_le() });
        }
        for _ in 0..count(&mut b, 10)? {
            s.epochs.insert(b.get_u64_le(), b.get_u16_le());
        }
        for _ in 0..count(&mut b, 12)? {
            s.coordinator_epochs.insert(b.get_u64_le(), b.get_u32_le());
        }
        for _ in 0..count(&mut b, 24)? {
            s.aborted.push(Aborted { producer_id: b.get_u64_le(), first: b.get_u64_le(), last: b.get_u64_le() });
        }
        if b.has_remaining() {
            return Err("transaction state has trailing bytes".into());
        }
        *self = s;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

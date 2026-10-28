//! State of consumer groups, kept in the coordinator group next to the
//! transaction coordinator's.
//!
//! Membership is persisted (KIP-848 style): every change to a group is one
//! record holding the whole group (`GroupState`), so a coordinator failover
//! keeps members, their epochs and their partitions. Committed offsets are
//! records of their own (`GroupOffsets`); offsets committed within a
//! transaction wait in the transaction's state and join these when the
//! transaction's `PrepareCommit` is applied (see [`crate::coordinator`]).
//!
//! Partitions move cooperatively. Each member has a **target** (where the
//! assignor wants its partitions), what it was **assigned**, and what it is
//! **revoking** (taken from it, but it may still be reading them). A
//! partition goes to a new member only once no other member has it assigned
//! or revoking: the old owner must report it released first, or be removed.
//! So no partition has two owners at once.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// A partition: `(resource id, partition)`.
pub(crate) type Tp = (u64, u32);

/// One member of a group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MemberMeta {
    /// Raised by one whenever its assignment changes, so the member can
    /// tell it has the latest one.
    pub(crate) epoch: i32,
    pub(crate) subscribed: BTreeSet<u64>,
    pub(crate) rebalance_timeout_ms: u32,
    /// Where the assignor wants its partitions.
    pub(crate) target: BTreeSet<Tp>,
    /// What it may read.
    pub(crate) assigned: BTreeSet<Tp>,
    /// Taken from it; it may still be reading them until it reports them
    /// released.
    pub(crate) revoking: BTreeSet<Tp>,
}

impl MemberMeta {
    /// Whether the member may still hold `tp` (assigned or not yet
    /// released).
    pub(crate) fn holds(&self, tp: &Tp) -> bool {
        self.assigned.contains(tp) || self.revoking.contains(tp)
    }
}

/// One group.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GroupMeta {
    /// Raised whenever membership or subscriptions change.
    pub(crate) epoch: i32,
    pub(crate) members: BTreeMap<String, MemberMeta>,
}

impl GroupMeta {
    /// Recomputes every member's target: per stream, its partitions spread
    /// evenly over the members subscribed to it, each keeping as many of its
    /// current targets as its share allows (sticky).
    pub(crate) fn retarget(&mut self, partitions_of: impl Fn(u64) -> u32) {
        let streams: BTreeSet<u64> = self.members.values().flat_map(|m| m.subscribed.iter().copied()).collect();
        let mut targets: BTreeMap<String, BTreeSet<Tp>> =
            self.members.keys().map(|id| (id.clone(), BTreeSet::new())).collect();
        for r in streams {
            let parts: Vec<Tp> = (0..partitions_of(r)).map(|p| (r, p)).collect();
            let subs: Vec<&String> =
                self.members.iter().filter(|(_, m)| m.subscribed.contains(&r)).map(|(id, _)| id).collect();
            if parts.is_empty() || subs.is_empty() {
                continue;
            }
            let current = |id: &String| -> Vec<Tp> {
                self.members[id].target.iter().copied().filter(|tp| tp.0 == r && tp.1 < parts.len() as u32).collect()
            };
            // Members holding more get the larger shares first.
            let mut order: Vec<&String> = subs.clone();
            order.sort_by_key(|id| (std::cmp::Reverse(current(id).len()), (*id).clone()));
            let (base, extra) = (parts.len() / subs.len(), parts.len() % subs.len());
            let quota: BTreeMap<&String, usize> =
                order.iter().enumerate().map(|(i, id)| (*id, base + usize::from(i < extra))).collect();
            let mut kept: BTreeMap<&String, Vec<Tp>> = BTreeMap::new();
            let mut taken = BTreeSet::new();
            for id in &order {
                let mine: Vec<Tp> = current(id).into_iter().take(quota[id]).collect();
                taken.extend(mine.iter().copied());
                kept.insert(id, mine);
            }
            let mut free = parts.iter().copied().filter(|tp| !taken.contains(tp));
            for id in &order {
                let mine = kept.get_mut(id).expect("every subscriber");
                while mine.len() < quota[id] {
                    mine.push(free.next().expect("shares add up to the partitions"));
                }
            }
            for (id, mine) in kept {
                targets.get_mut(id).expect("a member").extend(mine);
            }
        }
        for (id, t) in targets {
            self.members.get_mut(&id).expect("a member").target = t;
        }
    }

    /// Moves `member` towards its target: partitions it should not have
    /// are revoked, partitions of its target nobody else holds are added.
    /// `owned`, if the member reported it, releases revoked partitions it no
    /// longer reads. Returns whether anything changed.
    pub(crate) fn reconcile(&mut self, member: &str, owned: Option<&BTreeSet<Tp>>) -> bool {
        let others: BTreeSet<Tp> = self
            .members
            .iter()
            .filter(|(id, _)| id.as_str() != member)
            .flat_map(|(_, m)| m.assigned.iter().chain(m.revoking.iter()).copied())
            .collect();
        let Some(m) = self.members.get_mut(member) else { return false };
        let before = m.clone();
        if let Some(owned) = owned {
            m.revoking.retain(|tp| owned.contains(tp));
        }
        // Back in its target: it never stopped reading it.
        let back: Vec<Tp> = m.revoking.iter().copied().filter(|tp| m.target.contains(tp)).collect();
        for tp in back {
            m.revoking.remove(&tp);
            m.assigned.insert(tp);
        }
        let gone: Vec<Tp> = m.assigned.iter().copied().filter(|tp| !m.target.contains(tp)).collect();
        for tp in gone {
            m.assigned.remove(&tp);
            m.revoking.insert(tp);
        }
        let add: Vec<Tp> =
            m.target.iter().copied().filter(|tp| !m.assigned.contains(tp) && !others.contains(tp)).collect();
        m.assigned.extend(add);
        if m.assigned != before.assigned {
            m.epoch = m.epoch.saturating_add(1);
        }
        *m != before
    }
}

/// A committed offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Committed {
    pub(crate) offset: u64,
    pub(crate) leader_term: u64,
    pub(crate) metadata: Option<String>,
}

/// Offsets of one group: `(group, resource, partition)` → committed.
pub(crate) type OffsetKey = (String, u64, u32);

/// Every group and committed offset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GroupsState {
    pub(crate) groups: BTreeMap<String, GroupMeta>,
    pub(crate) offsets: BTreeMap<OffsetKey, Committed>,
}

const VERSION: u8 = 1;

fn put_str(b: &mut BytesMut, s: &str) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s.as_bytes());
}

fn get_str(b: &mut &[u8]) -> Result<String, String> {
    if b.remaining() < 2 {
        return Err("group record is truncated".into());
    }
    let n = usize::from(b.get_u16_le());
    if b.remaining() < n {
        return Err("group record is truncated".into());
    }
    let s = String::from_utf8(b[..n].to_vec()).map_err(|_| "group record holds a string that is not UTF-8")?;
    b.advance(n);
    Ok(s)
}

fn need(b: &&[u8], n: usize) -> Result<(), String> {
    if b.remaining() < n { Err("group record is truncated".into()) } else { Ok(()) }
}

fn put_tps(b: &mut BytesMut, tps: &BTreeSet<Tp>) {
    b.put_u32_le(tps.len() as u32);
    for (r, p) in tps {
        b.put_u64_le(*r);
        b.put_u32_le(*p);
    }
}

fn get_tps(b: &mut &[u8]) -> Result<BTreeSet<Tp>, String> {
    need(b, 4)?;
    let n = b.get_u32_le() as usize;
    need(b, n.saturating_mul(12))?;
    Ok((0..n).map(|_| (b.get_u64_le(), b.get_u32_le())).collect())
}

pub(crate) fn put_group(b: &mut BytesMut, id: &str, g: &GroupMeta) {
    put_str(b, id);
    b.put_i32_le(g.epoch);
    b.put_u32_le(g.members.len() as u32);
    for (mid, m) in &g.members {
        put_str(b, mid);
        b.put_i32_le(m.epoch);
        b.put_u32_le(m.rebalance_timeout_ms);
        b.put_u32_le(m.subscribed.len() as u32);
        for r in &m.subscribed {
            b.put_u64_le(*r);
        }
        put_tps(b, &m.target);
        put_tps(b, &m.assigned);
        put_tps(b, &m.revoking);
    }
}

pub(crate) fn get_group(b: &mut &[u8]) -> Result<(String, GroupMeta), String> {
    let id = get_str(b)?;
    need(b, 8)?;
    let epoch = b.get_i32_le();
    let n = b.get_u32_le();
    let mut members = BTreeMap::new();
    for _ in 0..n {
        let mid = get_str(b)?;
        need(b, 12)?;
        let (epoch, rebalance_timeout_ms, s) = (b.get_i32_le(), b.get_u32_le(), b.get_u32_le() as usize);
        need(b, s.saturating_mul(8))?;
        let subscribed = (0..s).map(|_| b.get_u64_le()).collect();
        let (target, assigned, revoking) = (get_tps(b)?, get_tps(b)?, get_tps(b)?);
        members.insert(mid, MemberMeta { epoch, subscribed, rebalance_timeout_ms, target, assigned, revoking });
    }
    Ok((id, GroupMeta { epoch, members }))
}

pub(crate) fn put_offset(b: &mut BytesMut, (r, p): (u64, u32), c: &Committed) {
    b.put_u64_le(r);
    b.put_u32_le(p);
    b.put_u64_le(c.offset);
    b.put_u64_le(c.leader_term);
    b.put_u8(u8::from(c.metadata.is_some()));
    if let Some(m) = &c.metadata {
        put_str(b, m);
    }
}

pub(crate) fn get_offset(b: &mut &[u8]) -> Result<(Tp, Committed), String> {
    need(b, 29)?;
    let tp = (b.get_u64_le(), b.get_u32_le());
    let (offset, leader_term) = (b.get_u64_le(), b.get_u64_le());
    let metadata = if b.get_u8() != 0 { Some(get_str(b)?) } else { None };
    Ok((tp, Committed { offset, leader_term, metadata }))
}

/// The record that sets group `id` to `g`.
pub(crate) fn group_record(id: &str, g: &GroupMeta) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(VERSION);
    put_group(&mut b, id, g);
    lotse_record::internal_control_batch(lotse_record::InternalControl::GroupState, &b).expect("one record")
}

/// The record that commits `offsets` for group `id`.
pub(crate) fn offsets_record(id: &str, offsets: &[(Tp, Committed)]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(VERSION);
    put_str(&mut b, id);
    b.put_u32_le(offsets.len() as u32);
    for (tp, c) in offsets {
        put_offset(&mut b, *tp, c);
    }
    lotse_record::internal_control_batch(lotse_record::InternalControl::GroupOffsets, &b).expect("one record")
}

impl GroupsState {
    /// Applies a `GroupState` record.
    pub(crate) fn apply_group(&mut self, mut b: &[u8]) -> Result<(), String> {
        if b.remaining() < 1 || b.get_u8() != VERSION {
            return Err("group record has an unknown version".into());
        }
        let (id, g) = get_group(&mut b)?;
        if g.members.is_empty() {
            self.groups.remove(&id);
        } else {
            self.groups.insert(id, g);
        }
        Ok(())
    }

    /// Applies a `GroupOffsets` record.
    pub(crate) fn apply_offsets(&mut self, mut b: &[u8]) -> Result<(), String> {
        if b.remaining() < 1 || b.get_u8() != VERSION {
            return Err("offsets record has an unknown version".into());
        }
        let id = get_str(&mut b)?;
        need(&b, 4)?;
        for _ in 0..b.get_u32_le() {
            let ((r, p), c) = get_offset(&mut b)?;
            self.offsets.insert((id.clone(), r, p), c);
        }
        Ok(())
    }

    pub(crate) fn encode(&self, b: &mut BytesMut) {
        b.put_u32_le(self.groups.len() as u32);
        for (id, g) in &self.groups {
            put_group(b, id, g);
        }
        b.put_u32_le(self.offsets.len() as u32);
        for ((g, r, p), c) in &self.offsets {
            put_str(b, g);
            put_offset(b, (*r, *p), c);
        }
    }

    pub(crate) fn decode(b: &mut &[u8]) -> Result<Self, String> {
        let mut s = GroupsState::default();
        need(b, 4)?;
        for _ in 0..b.get_u32_le() {
            let (id, g) = get_group(b)?;
            s.groups.insert(id, g);
        }
        need(b, 4)?;
        for _ in 0..b.get_u32_le() {
            let g = get_str(b)?;
            let ((r, p), c) = get_offset(b)?;
            s.offsets.insert((g, r, p), c);
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests;

//! A set of `u64` offsets kept as disjoint, merged ranges `[from, to)`.

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// See the module docs. Acknowledged offsets mostly form a few long runs,
/// so ranges stay small where a bitmap per offset would not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    /// `from -> to`, disjoint and not touching.
    ranges: BTreeMap<u64, u64>,
}

impl RangeSet {
    /// An empty set.
    pub fn new() -> Self {
        RangeSet::default()
    }

    /// Adds `[from, to)`.
    pub fn insert(&mut self, mut from: u64, mut to: u64) {
        if from >= to {
            return;
        }
        // Merge with a range that starts before and reaches `from`.
        if let Some((&f, &t)) = self.ranges.range(..=from).next_back()
            && t >= from
        {
            from = f;
            to = to.max(t);
            self.ranges.remove(&f);
        }
        // Swallow ranges that start inside `[from, to]`.
        while let Some((&f, &t)) = self.ranges.range(from..=to).next() {
            to = to.max(t);
            self.ranges.remove(&f);
        }
        self.ranges.insert(from, to);
    }

    /// Whether `offset` is in the set.
    pub fn contains(&self, offset: u64) -> bool {
        self.ranges.range(..=offset).next_back().is_some_and(|(_, &t)| offset < t)
    }

    /// The lowest offset at or after `from` that is not in the set.
    pub fn next_gap(&self, from: u64) -> u64 {
        match self.ranges.range(..=from).next_back() {
            Some((_, &t)) if from < t => t,
            _ => from,
        }
    }

    /// How many offsets in `from..to` are in the set.
    pub fn count_in(&self, from: u64, to: u64) -> u64 {
        let first = self.ranges.range(..=from).next_back().map_or(from, |(f, _)| *f);
        self.ranges.range(first..to).map(|(&f, &t)| t.min(to).saturating_sub(f.max(from))).sum()
    }

    /// The ranges, in order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.ranges.iter().map(|(f, t)| (*f, *t))
    }

    /// Number of ranges.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub(crate) fn encode(&self, b: &mut BytesMut) {
        b.put_u32_le(self.ranges.len() as u32);
        for (f, t) in &self.ranges {
            b.put_u64_le(*f);
            b.put_u64_le(*t);
        }
    }

    pub(crate) fn decode(b: &mut Bytes) -> Result<Self, String> {
        if b.remaining() < 4 {
            return Err("range set is truncated".into());
        }
        let n = b.get_u32_le() as usize;
        if b.remaining() < n.saturating_mul(16) {
            return Err("range set is truncated".into());
        }
        let mut s = RangeSet::new();
        for _ in 0..n {
            let (f, t) = (b.get_u64_le(), b.get_u64_le());
            if f >= t {
                return Err("empty range in range set".into());
            }
            s.insert(f, t);
        }
        Ok(s)
    }
}

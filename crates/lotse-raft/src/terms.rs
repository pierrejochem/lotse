//! Which term each index of the log belongs to.
//!
//! A Raft entry in Lotse is one record batch and covers a range of indices
//! (one per record). Terms never decrease along a log, so the whole log's
//! term information fits in a short list of runs: the first index of each
//! term. That list is all the core keeps in memory; record bytes stay in
//! `lotse-log`.
//!
//! The first index of a run is always an entry boundary: a new term means a
//! new leader, and a new leader starts a new batch.
//!
//! After a snapshot, everything up to the snapshot index is gone; only that
//! index's term is kept, as the `base`.

/// Term runs of a log. Indices are 1-based; 0 means "before the log".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TermRuns {
    /// Snapshot point `(index, term)`: indices up to here are compacted.
    base: (u64, u64),
    /// `(first index, term)` after the base, ascending in both.
    runs: Vec<(u64, u64)>,
    last: u64,
}

impl TermRuns {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// A log that starts right after a snapshot of `index` in `term`.
    pub fn with_snapshot(index: u64, term: u64) -> Self {
        TermRuns { base: (index, term), runs: Vec::new(), last: index }
    }

    /// Rebuilds the runs from stored entries `(first, last, term)` in order,
    /// e.g. from batch headers after a restart.
    pub fn from_entries(entries: impl IntoIterator<Item = (u64, u64, u64)>) -> Self {
        let mut t = TermRuns::new();
        for (first, last, term) in entries {
            t.append(first, last, term);
        }
        t
    }

    /// Last index in the log, 0 if empty.
    pub fn last_index(&self) -> u64 {
        self.last
    }

    /// Term of the last index, 0 if empty.
    pub fn last_term(&self) -> u64 {
        self.runs.last().map_or(self.base.1, |r| r.1)
    }

    /// Index of the snapshot the log starts after, 0 if none.
    pub fn snapshot_index(&self) -> u64 {
        self.base.0
    }

    /// Term of the snapshot index.
    pub fn snapshot_term(&self) -> u64 {
        self.base.1
    }

    /// Term of `index`: the snapshot index has its term, compacted indices
    /// before it and indices past the end have none.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.base.0 {
            return Some(self.base.1);
        }
        if index < self.base.0 || index > self.last {
            return None;
        }
        let i = self.runs.partition_point(|r| r.0 <= index);
        Some(self.runs[i - 1].1)
    }

    /// First index of the run that contains `index` (an entry boundary),
    /// never before the first index after the snapshot.
    pub fn run_start(&self, index: u64) -> u64 {
        let i = self.runs.partition_point(|r| r.0 <= index);
        if i == 0 { self.base.0 + 1 } else { self.runs[i - 1].0 }
    }

    /// Last index holding `term`, if the log has that term.
    pub fn last_of_term(&self, term: u64) -> Option<u64> {
        match self.runs.iter().position(|r| r.1 == term) {
            Some(i) => Some(self.runs.get(i + 1).map_or(self.last, |next| next.0 - 1)),
            None if self.base.1 == term && self.base.0 > 0 => Some(self.runs.first().map_or(self.last, |r| r.0 - 1)),
            None => None,
        }
    }

    /// Drops everything up to `index` (the last index of an entry), keeping
    /// its term as the new base.
    pub fn compact(&mut self, index: u64) {
        if index <= self.base.0 {
            return;
        }
        let term = self.term_at(index).expect("compact within the log");
        let keep = self.runs.partition_point(|r| r.0 <= index);
        // The run holding `index` continues after it if the log does.
        let mut runs: Vec<(u64, u64)> = Vec::new();
        if index < self.last && self.runs[keep - 1].1 == term && self.runs.get(keep).is_none_or(|r| r.0 > index + 1) {
            runs.push((index + 1, term));
        }
        runs.extend_from_slice(&self.runs[keep..]);
        self.runs = runs;
        self.base = (index, term);
    }

    /// Appends an entry covering `first..=last`. Panics if it does not
    /// continue the log or its term goes backwards: both are bugs.
    pub fn append(&mut self, first: u64, last: u64, term: u64) {
        assert_eq!(first, self.last + 1, "entry must continue the log");
        assert!(last >= first, "entry covers at least one index");
        assert!(term >= self.last_term(), "terms never decrease along a log");
        if self.runs.last().is_none_or(|r| r.1 != term) {
            self.runs.push((first, term));
        }
        self.last = last;
    }

    /// Removes every index from `index` on. Compacted indices stay.
    pub fn truncate_from(&mut self, index: u64) {
        if index > self.last {
            return;
        }
        assert!(index > self.base.0, "cannot truncate into a snapshot");
        self.runs.retain(|r| r.0 < index);
        self.last = index.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> TermRuns {
        // entries: [1..=3] t1, [4..=4] t1, [5..=9] t2, [10..=10] t4
        TermRuns::from_entries([(1, 3, 1), (4, 4, 1), (5, 9, 2), (10, 10, 4)])
    }

    #[test]
    fn lookups() {
        let t = sample();
        assert_eq!(t.last_index(), 10);
        assert_eq!(t.last_term(), 4);
        assert_eq!(t.term_at(0), Some(0));
        assert_eq!(t.term_at(4), Some(1));
        assert_eq!(t.term_at(5), Some(2));
        assert_eq!(t.term_at(11), None);
        assert_eq!(t.run_start(7), 5);
        assert_eq!(t.run_start(2), 1);
        assert_eq!(t.last_of_term(1), Some(4));
        assert_eq!(t.last_of_term(2), Some(9));
        assert_eq!(t.last_of_term(3), None);
    }

    #[test]
    fn truncation() {
        let mut t = sample();
        t.truncate_from(5);
        assert_eq!(t.last_index(), 4);
        assert_eq!(t.last_term(), 1);
        t.append(5, 6, 3);
        assert_eq!(t.term_at(6), Some(3));
        t.truncate_from(1);
        assert_eq!(t, TermRuns::new());
    }

    #[test]
    fn compaction() {
        let mut t = sample();
        t.compact(7); // inside the term-2 run 5..=9
        assert_eq!((t.snapshot_index(), t.snapshot_term()), (7, 2));
        assert_eq!(t.term_at(6), None);
        assert_eq!(t.term_at(7), Some(2));
        assert_eq!(t.term_at(8), Some(2));
        assert_eq!(t.run_start(8), 8);
        assert_eq!(t.last_of_term(2), Some(9));
        assert_eq!(t.last_of_term(1), None);
        t.compact(10);
        assert_eq!((t.last_index(), t.last_term()), (10, 4));
        assert_eq!(t.run_start(11), 11);
        t.append(11, 12, 5);
        assert_eq!(t.term_at(11), Some(5));
        t.truncate_from(11);
        assert_eq!(t.last_index(), 10);
        assert_eq!(t.last_of_term(4), Some(10));
        let s = TermRuns::with_snapshot(20, 3);
        assert_eq!((s.last_index(), s.last_term(), s.term_at(20), s.term_at(19)), (20, 3, Some(3), None));
    }

    #[test]
    #[should_panic(expected = "continue the log")]
    fn gaps_are_bugs() {
        let mut t = sample();
        t.append(12, 12, 4);
    }
}

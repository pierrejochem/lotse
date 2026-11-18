//! A node's free disk space limit (ADR 0122).
//!
//! A node reads its data volume every few seconds. Once the free space
//! falls below the limit it is low; it is low again only after it went back
//! a tenth above the limit, so a volume that hovers at the limit does not
//! turn data on and off with every read. The metadata leader records which
//! nodes are low, and the leader of a group with a low voter takes no new
//! data: every voter writes what the leader takes.

use std::fmt;

/// How much free space a node keeps on its data volume: the larger of
/// `bytes` and `permille` of the volume. Both 0: no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreeLimit {
    /// At least this many bytes free.
    pub bytes: u64,
    /// At least this share of the volume free, per mille.
    pub permille: u32,
}

impl Default for FreeLimit {
    /// The larger of 1 GiB and 5 % of the volume.
    fn default() -> Self {
        FreeLimit { bytes: 1 << 30, permille: 50 }
    }
}

impl FreeLimit {
    /// No limit.
    pub const OFF: FreeLimit = FreeLimit { bytes: 0, permille: 0 };

    /// The limit in bytes on a volume of `total` bytes.
    pub fn of(self, total: u64) -> u64 {
        let share = u64::try_from(u128::from(total) * u128::from(self.permille) / 1000).unwrap_or(u64::MAX);
        self.bytes.max(share)
    }
}

impl fmt::Display for FreeLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.bytes, self.permille) {
            (0, 0) => write!(f, "none"),
            (b, 0) => write!(f, "{b} bytes"),
            (0, p) => write!(f, "{}.{} %", p / 10, p % 10),
            (b, p) => write!(f, "{b} bytes or {}.{} %, the larger", p / 10, p % 10),
        }
    }
}

/// Whether a node is low on disk, from what it reads of its volume.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DiskWatch {
    /// The limit in bytes at the last read.
    pub(crate) limit_bytes: u64,
    pub(crate) low: bool,
    /// A read told the volume's size and free space.
    read: bool,
}

impl DiskWatch {
    /// Takes a read of the volume. A read that cannot tell leaves the
    /// node as it was.
    pub(crate) fn observe(&mut self, limit: FreeLimit, free: Option<u64>, total: Option<u64>) {
        let (Some(free), Some(total)) = (free, total) else { return };
        self.read = true;
        self.limit_bytes = limit.of(total);
        if self.limit_bytes == 0 {
            self.low = false;
            return;
        }
        // Back only a tenth above the limit (ADR 0122).
        let clear_at = self.limit_bytes.saturating_add(self.limit_bytes / 10);
        self.low = if self.low { free < clear_at } else { free < self.limit_bytes };
    }

    /// What `DescribeMetrics` says of it; `None` until a read told the size.
    pub(crate) fn metrics(&self) -> Option<lotse_proto::DiskMetrics> {
        self.read.then_some(lotse_proto::DiskMetrics { free_limit_bytes: self.limit_bytes, low: self.low })
    }
}

#[cfg(test)]
mod tests;

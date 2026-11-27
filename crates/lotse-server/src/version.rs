//! Cluster versions (ADR 0127): which formats a node may write while nodes
//! of two releases run side by side.
//!
//! A release's cluster version is its `major.minor`. A release runs with
//! its own cluster version and the previous minor release's: it reads and
//! writes that release's formats, and writes a newer format only once a
//! cluster admin raised the cluster version to the one that introduced it.

use lotse_proto::cluster_version;

/// This build's release, such as `0.5.2`.
pub const RELEASE: &str = env!("CARGO_PKG_VERSION");

const fn parse_u16(s: &str) -> u16 {
    let b = s.as_bytes();
    let mut i = 0;
    let mut n: u32 = 0;
    while i < b.len() {
        assert!(b[i].is_ascii_digit(), "a version part is a number");
        n = n * 10 + (b[i] - b'0') as u32;
        assert!(n <= u16::MAX as u32, "a version part fits 16 bits");
        i += 1;
    }
    n as u16
}

const MAJOR: u16 = parse_u16(env!("CARGO_PKG_VERSION_MAJOR"));
const MINOR: u16 = parse_u16(env!("CARGO_PKG_VERSION_MINOR"));

/// This release's cluster version: the newest it runs with.
pub const CLUSTER_VERSION: u32 = cluster_version::of(MAJOR, MINOR);

/// The oldest cluster version this release runs with: the previous minor
/// release's. A `.0` release of a new major version runs only with itself,
/// unless set here by hand to the last minor release of the major before.
pub const OLDEST_CLUSTER_VERSION: u32 =
    if MINOR == 0 { CLUSTER_VERSION } else { cluster_version::of(MAJOR, MINOR - 1) };

/// The cluster version that introduced access tokens: they are written only
/// while the cluster version in force is at least this (ADR 0127).
pub const TOKENS_SINCE: u32 = cluster_version::of(0, 1);

/// The cluster version that introduced storage quotas: their limits and
/// full records are written only while the cluster version in force is at
/// least this (ADR 0127).
pub const QUOTAS_SINCE: u32 = cluster_version::of(0, 2);

/// The cluster version that introduced a resource's own segment age
/// (`segment.ms`): written only while the cluster version in force is at
/// least this (ADR 0127).
pub const SEGMENT_AGE_SINCE: u32 = cluster_version::of(0, 2);

// The window of ADR 0127: one minor release back, never more. A release
// whose window is wrong does not build.
const _: () = assert!(
    OLDEST_CLUSTER_VERSION == CLUSTER_VERSION
        || (MINOR != 0 && OLDEST_CLUSTER_VERSION == cluster_version::of(MAJOR, MINOR.saturating_sub(1))),
    "a release runs with its own cluster version and the previous minor release's, no older"
);

/// `major.minor`, for messages.
pub fn display(v: u32) -> String {
    format!("{}.{}", cluster_version::major(v), cluster_version::minor(v))
}

/// A cluster version from `major.minor`; a patch part, as in a release
/// name, is ignored.
pub fn parse(s: &str) -> Option<u32> {
    let mut parts = s.trim().split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    if parts.next().is_some_and(|p| p.parse::<u16>().is_err()) {
        return None;
    }
    Some(cluster_version::of(major, minor))
}

/// The versions a node reports (ADR 0127): its release and the range of
/// cluster versions it runs with. Simulations give nodes other ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versions {
    /// Its release, such as `0.5.2`.
    pub release: String,
    /// The oldest cluster version it runs with.
    pub oldest: u32,
    /// The newest, its own.
    pub newest: u32,
}

impl Default for Versions {
    fn default() -> Self {
        Versions { release: RELEASE.to_string(), oldest: OLDEST_CLUSTER_VERSION, newest: CLUSTER_VERSION }
    }
}

impl Versions {
    /// Why a node with these versions may not run in a cluster at
    /// `cluster`, if it may not.
    pub fn refuses(&self, cluster: u32) -> Option<String> {
        if cluster > self.newest {
            Some(format!(
                "the cluster runs at cluster version {}, and release {} runs with {} at the newest: install the release the other nodes run, or a newer one",
                display(cluster),
                self.release,
                display(self.newest)
            ))
        } else if cluster < self.oldest {
            Some(format!(
                "the cluster runs at cluster version {}, and release {} runs with {} at the oldest: a release was skipped; install a {} release first, raise the cluster version, then this one",
                display(cluster),
                self.release,
                display(self.oldest),
                display(self.oldest)
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests;

//! A Lotse broker node.
//!
//! - [`Replica`]: one Raft group (a partition, a queue or a priority level)
//!   persisted on its own [`lotse_log::Log`];
//! - [`Node`]: many replicas on one node behind one transport, with all
//!   messages for a peer bundled into one frame ([`wire`]), serving clients
//!   over the native protocol ([`lotse_proto`]).

#![warn(missing_docs)]

mod access;
mod auth;
mod client;
mod coordinator;
mod disk;
mod groups;
mod membership;
mod metadata;
mod node;
pub mod ops;
mod producers;
mod replica;
mod signal;
mod txns;
pub mod version;
pub mod wire;

pub use access::DEFAULT_TENANT;
pub use auth::{AuthConfig, ClientCas, DEFAULT_MAX_SESSION, JWKS_REFETCH_EVERY, OidcConfig, Principal, RenewalSlot};
pub use client::{QueueConfig, StreamConfig};
pub use disk::FreeLimit;
pub use metadata::{DYNAMIC_BASE, MAX_PARTITIONS};
pub use node::{DEFAULT_COMPACTION_MAP_BYTES, GroupStatus, Node, NodeConfig, NodeHandle, RetentionConfig};
pub use producers::{DEFAULT_MAX_PRODUCERS, DEFAULT_PRODUCER_EXPIRATION};

/// How long a group member may go without a heartbeat (Kafka's default).
pub const DEFAULT_GROUP_SESSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
/// How often group members send heartbeats (Kafka's default).
pub const DEFAULT_GROUP_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
/// How long a group's preferred voter holds everything committed before its
/// leader hands the group back to it, when the group saw no other election
/// recently (ADR 0113).
pub const DEFAULT_LEADER_SETTLE: std::time::Duration = std::time::Duration::from_secs(60);
/// How long a follower may go without having applied what its leader said
/// was committed and still serve reads (ADR 0116), like Kafka's
/// `replica.lag.time.max.ms`.
pub const DEFAULT_FOLLOWER_MAX_LAG: std::time::Duration = std::time::Duration::from_secs(10);
/// How often a node reads its data volume's free space by default (ADR 0124).
pub const DEFAULT_DISK_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
pub use replica::{Error, Output, ProposeError, Proposed, Replica, ReplicaOptions, Retention, StateMachine, Sweep};

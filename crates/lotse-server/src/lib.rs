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
mod groups;
mod membership;
mod metadata;
mod node;
mod producers;
mod replica;
mod signal;
mod txns;
pub mod wire;

pub use access::DEFAULT_TENANT;
pub use auth::{AuthConfig, DEFAULT_MAX_SESSION, JWKS_REFETCH_EVERY, OidcConfig, Principal};
pub use client::{QueueConfig, StreamConfig};
pub use metadata::{DYNAMIC_BASE, MAX_PARTITIONS};
pub use node::{GroupStatus, Node, NodeConfig, NodeHandle, RetentionConfig};
pub use producers::DEFAULT_MAX_PRODUCERS;

/// How long a group member may go without a heartbeat (Kafka's default).
pub const DEFAULT_GROUP_SESSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
/// How often group members send heartbeats (Kafka's default).
pub const DEFAULT_GROUP_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
pub use replica::{Error, Output, ProposeError, Proposed, Replica, ReplicaOptions, Retention, StateMachine};

//! A Lotse broker node.
//!
//! - [`Replica`]: one Raft group (a partition, a queue or a priority level)
//!   persisted on its own [`lotse_log::Log`];
//! - [`Node`]: many replicas on one node behind one transport, with all
//!   messages for a peer bundled into one frame ([`wire`]), serving clients
//!   over the native protocol ([`lotse_proto`]).

#![warn(missing_docs)]

mod client;
mod node;
mod producers;
mod replica;
mod signal;
pub mod wire;

pub use client::StreamConfig;
pub use node::{GroupStatus, Node, NodeConfig, NodeHandle};
pub use producers::DEFAULT_MAX_PRODUCERS;
pub use replica::{Error, Output, ProposeError, Proposed, Replica, StateMachine};

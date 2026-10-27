//! A Lotse broker node.
//!
//! - [`Replica`]: one Raft group (a partition, a queue or a priority level)
//!   persisted on its own [`lotse_log::Log`];
//! - [`Node`]: many replicas on one node behind one transport, with all
//!   messages for a peer bundled into one frame ([`wire`]).
//!
//! Next: the native protocol for clients.

#![warn(missing_docs)]

mod node;
mod replica;
mod signal;
pub mod wire;

pub use node::{GroupStatus, Node, NodeConfig, NodeHandle};
pub use replica::{Error, Output, ProposeError, Replica, StateMachine};

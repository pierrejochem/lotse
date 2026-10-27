//! A Lotse broker node.
//!
//! So far: [`Replica`], one Raft group (a partition, a queue or a priority
//! level) persisted on its own [`lotse_log::Log`]. Next: many replicas per
//! node behind one Multi-Raft transport, the native protocol, and the Tokio
//! implementation of `lotse-rt`.

#![warn(missing_docs)]

mod replica;

pub use replica::{Error, Output, ProposeError, Replica, StateMachine};

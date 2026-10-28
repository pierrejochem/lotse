//! Queue semantics on the Lotse log.
//!
//! A queue is a log plus an ack index. Acks and persisted delivery counts are
//! batches in the same log ([`lotse_record::ack_batch`],
//! [`lotse_record::queue_state_batch`]), so they are replicated and recovered
//! like messages. This crate holds the two state machines a queue needs,
//! both free of I/O:
//!
//! - [`QueueLog`]: the applied state every replica keeps (settled offsets,
//!   delivery counts, transactional messages waiting for their marker);
//! - [`Dispatcher`]: the leader's volatile delivery state (subscriptions,
//!   credits, messages in flight, redelivery, ack timeout).

#![warn(missing_docs)]

mod dispatch;
mod log;
mod ranges;

pub use dispatch::{ConnId, Delivery, Dispatcher, SubscriptionId};
pub use log::QueueLog;
pub use ranges::RangeSet;

#[cfg(test)]
mod tests;

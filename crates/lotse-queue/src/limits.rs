//! The limits a queue puts on its messages (ADR 0055): how often one may be
//! delivered and how long it may wait. Pure functions; the leader applies
//! them and writes its decision to the log, so replicas never decide.

use std::collections::BTreeSet;

use crate::{Dispatcher, QueueLog};

/// A queue's limits; 0 switches each off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Limits {
    /// How often a message is delivered at most; the next time it would go
    /// out it is retired instead.
    pub max_deliveries: u32,
    /// How long a message may wait, in milliseconds, counted from the time
    /// the leader appended it.
    pub message_ttl_ms: u64,
}

impl Limits {
    /// Whether a message delivered `deliveries` times may not be delivered
    /// again.
    pub fn exhausted(&self, deliveries: u32) -> bool {
        self.max_deliveries > 0 && deliveries >= self.max_deliveries
    }

    /// Whether a message appended at `appended_ms` (Unix time) is too old at
    /// `now_ms`. A message from the future (clocks that disagree) is young.
    pub fn expired(&self, appended_ms: i64, now_ms: u64) -> bool {
        self.message_ttl_ms > 0 && (now_ms as i64).saturating_sub(appended_ms) >= self.message_ttl_ms as i64
    }

    /// The Unix time in milliseconds at which a message appended at
    /// `appended_ms` expires.
    pub fn expires_at(&self, appended_ms: i64) -> i64 {
        appended_ms.saturating_add(self.message_ttl_ms as i64)
    }
}

/// The unsettled messages that have been delivered as often as `limits`
/// allow and are not with a consumer: persisted counts (nack, ack timeout)
/// plus, when the leader has a dispatcher, deliveries it only counts in
/// memory (disconnects). Not those in `skip`. At most `max`, lowest first,
/// each with its delivery count.
pub fn exhausted(
    queue: &QueueLog,
    dispatcher: Option<&Dispatcher>,
    limits: Limits,
    skip: &BTreeSet<u64>,
    max: usize,
) -> Vec<(u64, u16)> {
    if limits.max_deliveries == 0 {
        return Vec::new();
    }
    let mut offsets: BTreeSet<u64> = queue.delivered_at_least(1).map(|(o, _)| o).collect();
    if let Some(d) = dispatcher {
        offsets.extend(d.extra_offsets());
    }
    offsets
        .into_iter()
        .filter(|o| !skip.contains(o) && !queue.is_settled(*o) && !dispatcher.is_some_and(|d| d.is_in_flight(*o)))
        .map(|o| {
            let n = u32::from(queue.delivery_count(o)) + dispatcher.map_or(0, |d| u32::from(d.extra_of(o)));
            (o, n.min(u32::from(u16::MAX)) as u16)
        })
        .filter(|(_, n)| limits.exhausted(u32::from(*n)))
        .take(max)
        .collect()
}

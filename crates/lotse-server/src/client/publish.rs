//! Publishing through exchanges, routed by the node (ADR 0079).
//!
//! The node that gets a `Publish` routes each message by the exchange's
//! bindings as its metadata state has them, so bindings are never older
//! than what the node has applied. It checks the publisher's roles: the
//! producer role on the exchange (when the request is admitted) and on
//! every queue the message routes to (here). If one routed queue is not
//! the publisher's to write, nothing is written and the answer says
//! `UNAUTHORIZED`, without naming the queue. The node then writes the
//! message to each queue through the native protocol with an idempotent
//! producer of its own, like any client, and answers once every write is
//! decided. Queues are written one by one, not atomically: a transaction
//! publishes by producing to each queue itself.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_proto::{ErrorCode, PublishRequest, PublishResponse, PublishResult, Response, RoutedMessage};
use lotse_record::{Batch, BatchKind};

use super::{Clients, ClusterView, Outbox};
use crate::auth::Principal;
use lotse_auth::roles::Operation;

/// Writes for the node to make for one `Publish`.
pub(crate) struct PublishJob {
    pub(crate) key: u64,
    /// `(entry, queue id, queue name, batch)`.
    pub(crate) writes: Vec<(usize, u64, String, Bytes)>,
    /// How long the publisher allows.
    pub(crate) timeout: Duration,
}

/// One write's outcome: `(entry, queue id, queue name, error, offset)`,
/// the offset with `NONE`.
pub(crate) type PublishWrite = (usize, u64, String, ErrorCode, u64);

/// A `Publish` waiting for its writes.
struct Waiting {
    outbox: Arc<Outbox>,
    id: u32,
    response: PublishResponse,
}

/// The publishes in flight on this node.
#[derive(Default)]
pub(crate) struct Publishing {
    next: u64,
    waiting: BTreeMap<u64, Waiting>,
    jobs: Vec<PublishJob>,
}

/// The one record a published batch must be: data, not transactional, not
/// a control batch, nothing after it.
fn message(batch: &Bytes) -> Option<lotse_record::Record> {
    let (b, rest) = Batch::parse(batch).ok()?;
    let a = b.attributes();
    if !rest.is_empty() || a.kind != BatchKind::Data || a.control || a.transactional {
        return None;
    }
    let mut records = b.records().ok()?;
    if records.len() != 1 {
        return None;
    }
    records.pop()
}

impl Clients {
    pub(super) fn publish(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: PublishRequest,
        who: &Principal,
        view: &ClusterView<'_>,
    ) {
        let refused = |error| PublishResult { error, routed: Vec::new() };
        let mut response = PublishResponse { throttle_ms: 0, entries: Vec::new() };
        let mut writes = Vec::new();
        for (i, e) in req.entries.iter().enumerate() {
            // A transaction publishes by producing to each queue itself.
            if req.txn_id.is_some() {
                response.entries.push(refused(ErrorCode::INVALID_REQUEST));
                continue;
            }
            let Some(x) = view.exchanges.iter().find(|x| x.resource_id == e.exchange_id) else {
                response.entries.push(refused(ErrorCode::UNKNOWN_RESOURCE));
                continue;
            };
            let Some(record) = message(&e.batch) else {
                response.entries.push(refused(ErrorCode::INVALID_REQUEST));
                continue;
            };
            let headers = record.headers.iter().map(|h| (&h.key[..], h.value.as_deref()));
            let targets = x.route(&e.routing_key, headers);
            if targets.is_empty() {
                let error = if e.mandatory { ErrorCode::UNROUTABLE } else { ErrorCode::NONE };
                response.entries.push(refused(error));
                continue;
            }
            if !targets.iter().all(|q| view.access.may_on(who, Operation::Write, *q)) {
                response.entries.push(refused(ErrorCode::UNAUTHORIZED));
                continue;
            }
            let mut result = PublishResult { error: ErrorCode::NONE, routed: Vec::new() };
            for q in targets {
                match view.queues.iter().find(|c| c.resource_id == q) {
                    Some(c) => writes.push((i, q, c.name.clone(), e.batch.clone())),
                    // Bound, but gone from this node's view (deleted).
                    None => result.routed.push(RoutedMessage {
                        queue_id: q,
                        name: String::new(),
                        error: ErrorCode::UNKNOWN_RESOURCE,
                        base_offset: 0,
                    }),
                }
            }
            response.entries.push(result);
        }
        if writes.is_empty() {
            return outbox.push(id, &Response::Publish(response));
        }
        let p = &mut self.publishing;
        p.next += 1;
        p.waiting.insert(p.next, Waiting { outbox, id, response });
        let timeout = Duration::from_millis(u64::from(req.timeout_ms.max(1)));
        p.jobs.push(PublishJob { key: p.next, writes, timeout });
    }

    /// Writes for the node to make.
    pub(crate) fn take_publish_jobs(&mut self) -> Vec<PublishJob> {
        std::mem::take(&mut self.publishing.jobs)
    }

    /// The writes of a `Publish` are decided: answer it, each message's
    /// queues by id and name.
    pub(crate) fn published(&mut self, key: u64, results: Vec<PublishWrite>) {
        let Some(Waiting { outbox, id, mut response }) = self.publishing.waiting.remove(&key) else { return };
        for (entry, queue_id, name, error, base_offset) in results {
            if let Some(e) = response.entries.get_mut(entry) {
                e.routed.push(RoutedMessage { queue_id, name, error, base_offset });
            }
        }
        for e in &mut response.entries {
            e.routed.sort_by_key(|q| q.queue_id);
        }
        outbox.push(id, &Response::Publish(response));
    }
}

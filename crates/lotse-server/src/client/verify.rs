//! A partition's leader takes a producer's first transactional batch of an
//! epoch only once the transaction coordinator has said that the
//! transaction is open and holds the partition (ADR 0057). Otherwise a
//! client that never called `AddPartitionsToTxn` could open a transaction
//! the coordinator knows nothing of: it would never be ended, and the
//! partition's last stable offset would stay behind it for good.
//!
//! The batch is held while the node asks (the internal `VerifyTxn`, through
//! the same client code as everyone else); one question per partition,
//! producer and epoch, however many batches wait for it. Once the batch is
//! in the log the transaction is open here and later batches go straight
//! through. A transaction that ends meanwhile raises the producer's epoch
//! with its marker, so a batch let through late is refused as fenced.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use lotse_proto::{ErrorCode, VerifyTxnRequest};
use lotse_rt::Runtime;

use super::{Clients, ClusterView, propose_one, settle};
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;

/// What a question is about: one partition's producer at one epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Key {
    pub(crate) group: GroupId,
    pub(crate) producer_id: u64,
    pub(crate) epoch: u16,
}

/// A batch of a produce request waiting for the coordinator's answer.
pub(crate) struct Held {
    /// Where its answer goes in the response.
    pub(crate) resource: usize,
    pub(crate) partition: usize,
    pub(crate) key: Key,
    /// The batch as it will be proposed.
    pub(crate) batch: Bytes,
    /// Its size as produced, for the counters.
    pub(crate) bytes: u64,
}

/// A question for the node to send.
pub(crate) struct VerifyJob {
    pub(crate) key: Key,
    pub(crate) request: VerifyTxnRequest,
}

/// The questions this node has asked and not yet heard back on.
#[derive(Default)]
pub(crate) struct Verifying {
    asked: BTreeSet<Key>,
    jobs: Vec<VerifyJob>,
}

impl Verifying {
    /// Asks about `key`, unless that question is out already.
    pub(crate) fn ask(&mut self, key: Key, request: VerifyTxnRequest) {
        if self.asked.insert(key) {
            self.jobs.push(VerifyJob { key, request });
        }
    }
}

impl Clients {
    /// Questions for the node to send to the coordinator.
    pub(crate) fn take_verify_jobs(&mut self) -> Vec<VerifyJob> {
        std::mem::take(&mut self.verify.jobs)
    }

    /// The coordinator answered about `key` (`NONE`: the transaction is
    /// open and holds the partition), or the node could not reach it. Every
    /// batch waiting on that answer is proposed now, or refused.
    pub(crate) fn verified<R: Runtime, S: StateMachine>(
        &mut self,
        key: Key,
        error: ErrorCode,
        view: &ClusterView<'_>,
        groups: &mut BTreeMap<GroupId, Replica<R, S>>,
    ) {
        self.verify.asked.remove(&key);
        let produces = std::mem::take(&mut self.produces);
        for mut p in produces {
            let (now, rest): (Vec<Held>, Vec<Held>) = p.held.drain(..).partition(|h| h.key == key);
            p.held = rest;
            for h in now {
                let out = &mut p.response.resources[h.resource].partitions[h.partition];
                match groups.get_mut(&key.group) {
                    None => {
                        out.error = ErrorCode::NOT_LEADER;
                        out.error_msg = Some("the partition moved away; nothing was written".into());
                    }
                    Some(_) if error != ErrorCode::NONE => {
                        out.error = error;
                        out.error_msg = Some(refusal(error).into());
                    }
                    Some(replica) => {
                        let w = propose_one(replica, key.group, h.batch, h.bytes, out, (h.resource, h.partition));
                        p.waiting.extend(w);
                    }
                }
            }
            if let Some(p) = settle(p, view, groups, &mut self.stats) {
                self.produces.push(p);
            }
        }
    }
}

/// What the producer reads when the coordinator says no.
fn refusal(error: ErrorCode) -> &'static str {
    match error {
        ErrorCode::INVALID_TXN_STATE => {
            "the transaction coordinator has no open transaction with this partition: add it first (AddPartitionsToTxn)"
        }
        ErrorCode::PRODUCER_FENCED => "a newer producer or epoch holds this transactional id",
        _ => "not written: the transaction coordinator could not be asked",
    }
}

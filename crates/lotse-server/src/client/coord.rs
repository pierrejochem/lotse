//! The transaction coordinator's side of the protocol: `InitProducer` with a
//! transactional id, `AddPartitionsToTxn`, `EndTxn`, and the work that
//! follows a decision (markers, then the completion record) or a timeout.
//!
//! The coordinator acts on the applied state of its group, so it only acts
//! once its leader has applied an entry of its own term (everything older is
//! applied then), and it changes one transactional id at a time: a second
//! request while a record for the same id is in flight gets
//! `CONCURRENT_TRANSACTIONS`. Each change is one record proposed to the
//! group and answered once committed, under the same rule as a produce.
//!
//! Ending a transaction raises the producer epoch: the `PrepareCommit` or
//! `PrepareAbort` record carries the new epoch, the markers are written with
//! it, and `EndTxn` returns it for the producer's next transaction.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use lotse_proto::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, ErrorCode,
    InitProducerRequest, InitProducerResponse, Response, TxnPartitionResult, TxnResourceResult,
};
use lotse_rt::{Instant, Runtime};

use super::{Clients, ClusterView, Outbox, hint, outcome};
use crate::coordinator::{Phase, TxnMeta};
use crate::replica::{ProposeError, Proposed, Replica, StateMachine};
use crate::signal::Signal;
use crate::wire::GroupId;

/// How long a coordinator record may take to commit before the request is
/// answered with `REQUEST_TIMED_OUT`.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

type Groups<R, S> = std::collections::BTreeMap<GroupId, Replica<R, S>>;

/// Why the coordinator cannot act: the code, and the leader if known.
type Refusal = (ErrorCode, Option<lotse_proto::LeaderHint>);

/// What to answer once a coordinator record commits.
pub(super) enum Then {
    /// `InitProducer`: the id (`None`: the record's own index) and epoch.
    Init { producer_id: Option<u64>, epoch: u16 },
    /// `AddPartitionsToTxn`: the answer, all `NONE` once written.
    Add(AddPartitionsToTxnResponse),
    /// `EndTxn`: the producer's id and new epoch.
    End { producer_id: u64, epoch: u16 },
    /// The coordinator's own record (completion, timeout): nobody waits.
    Nothing,
}

/// A coordinator record in flight.
pub(super) struct PendingWrite {
    reply: Option<(Arc<Outbox>, u32)>,
    deadline: Instant,
    txn_id: String,
    last: u64,
    term: u64,
    then: Then,
}

/// Markers for one decided transaction, for the node to write.
#[derive(Debug, Clone)]
pub(crate) struct MarkerJob {
    pub(crate) txn_id: String,
    pub(crate) producer_id: u64,
    pub(crate) epoch: u16,
    pub(crate) commit: bool,
    /// The coordinator group's term, as the markers' coordinator epoch.
    pub(crate) term: u64,
    pub(crate) partitions: BTreeSet<(u64, u32)>,
    /// Set when this node stops coordinating in `term`.
    pub(crate) cancel: Signal,
}

fn error_response(then: &Then, error: ErrorCode, leader_hint: Option<lotse_proto::LeaderHint>) -> Option<Response> {
    Some(match then {
        Then::Init { .. } => {
            Response::InitProducer(InitProducerResponse { error, producer_id: 0, producer_epoch: 0, leader_hint })
        }
        Then::Add(resp) => {
            let mut resp = resp.clone();
            resp.resources.iter_mut().flat_map(|r| r.partitions.iter_mut()).for_each(|p| p.error = error);
            Response::AddPartitionsToTxn(resp)
        }
        Then::End { .. } => Response::EndTxn(EndTxnResponse { error, producer_id: 0, producer_epoch: 0 }),
        Then::Nothing => return None,
    })
}

impl Clients {
    /// The coordinator group's replica, if this node leads it and has applied
    /// an entry of its own term; otherwise the error to answer with.
    fn coordinator<'a, R: Runtime, S: StateMachine>(
        view: &ClusterView<'_>,
        groups: &'a mut Groups<R, S>,
    ) -> Result<(GroupId, &'a mut Replica<R, S>), Refusal> {
        let g = view.coordinator.ok_or((ErrorCode::COORDINATOR_NOT_AVAILABLE, None))?;
        let replica = groups.get_mut(&g).ok_or((ErrorCode::COORDINATOR_NOT_AVAILABLE, None))?;
        let raft = replica.raft_ref();
        if raft.role() != lotse_raft::Role::Leader {
            return Err((ErrorCode::NOT_LEADER, hint(raft)));
        }
        if raft.log().term_at(replica.applied()) != Some(raft.term()) {
            return Err((ErrorCode::COORDINATOR_NOT_AVAILABLE, None));
        }
        Ok((g, replica))
    }

    /// Proposes a record setting `txn_id` to `meta`, answered by `then`.
    fn write<R: Runtime, S: StateMachine>(
        &mut self,
        replica: &mut Replica<R, S>,
        now: Instant,
        txn_id: &str,
        meta: &TxnMeta,
        reply: Option<(Arc<Outbox>, u32)>,
        then: Then,
    ) {
        match replica.propose_internal(crate::coordinator::record(txn_id, meta)) {
            Ok(Proposed { last, term: Some(term), .. }) => {
                self.busy.insert(txn_id.to_string());
                let deadline = now + WRITE_TIMEOUT;
                self.writes.push(PendingWrite { reply, deadline, txn_id: txn_id.into(), last, term, then });
            }
            Ok(_) => unreachable!("a coordinator record is never a duplicate"),
            Err(e) => {
                let code = if matches!(e, ProposeError::NotLeader(_)) {
                    ErrorCode::NOT_LEADER
                } else {
                    ErrorCode::COORDINATOR_NOT_AVAILABLE
                };
                if let (Some((outbox, id)), Some(resp)) = (reply, error_response(&then, code, hint(replica.raft_ref())))
                {
                    outbox.push(id, &resp);
                }
            }
        }
    }

    pub(super) fn init_transactional<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: InitProducerRequest,
        txn_id: String,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let fail = |error, leader_hint| {
            Response::InitProducer(InitProducerResponse { error, producer_id: 0, producer_epoch: 0, leader_hint })
        };
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, h)) => return outbox.push(id, &fail(code, h)),
        };
        if self.busy.contains(&txn_id) {
            return outbox.push(id, &fail(ErrorCode::CONCURRENT_TRANSACTIONS, None));
        }
        let current = replica.coordinator().get(&txn_id).cloned();
        let timeout_ms = req.txn_timeout_ms;
        let fresh = |producer_id, epoch| TxnMeta {
            producer_id,
            epoch,
            phase: Phase::Empty,
            timeout_ms,
            partitions: BTreeSet::new(),
        };
        match current {
            None => {
                let then = Then::Init { producer_id: None, epoch: 0 };
                self.write(replica, view.now, &txn_id, &fresh(0, 0), Some((outbox, id)), then);
            }
            Some(m) if matches!(m.phase, Phase::Empty | Phase::CompleteCommit | Phase::CompleteAbort) => {
                // A new instance fences the old one with a higher epoch; at the
                // end of the epochs it gets a new producer id instead.
                let (producer_id, epoch) = match m.epoch.checked_add(1) {
                    Some(e) => (Some(m.producer_id), e),
                    None => (None, 0),
                };
                let meta = fresh(producer_id.unwrap_or(0), epoch);
                self.write(replica, view.now, &txn_id, &meta, Some((outbox, id)), Then::Init { producer_id, epoch });
            }
            Some(m) if m.phase == Phase::Ongoing => {
                // The old instance left a transaction open: abort it with a
                // higher epoch (fencing the old instance), then the client retries.
                let meta = TxnMeta { epoch: m.epoch.saturating_add(1), phase: Phase::PrepareAbort, ..m };
                self.write(replica, view.now, &txn_id, &meta, None, Then::Nothing);
                outbox.push(id, &fail(ErrorCode::CONCURRENT_TRANSACTIONS, None));
            }
            Some(_) => outbox.push(id, &fail(ErrorCode::CONCURRENT_TRANSACTIONS, None)),
        }
    }

    pub(super) fn add_partitions<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: AddPartitionsToTxnRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let mut resp = AddPartitionsToTxnResponse {
            resources: req
                .resources
                .iter()
                .map(|r| TxnResourceResult {
                    resource_id: r.resource_id,
                    partitions: r
                        .partitions
                        .iter()
                        .map(|p| TxnPartitionResult { partition: *p, error: ErrorCode::NONE })
                        .collect(),
                })
                .collect(),
        };
        let all = |resp: &mut AddPartitionsToTxnResponse, error| {
            resp.resources.iter_mut().flat_map(|r| r.partitions.iter_mut()).for_each(|p| p.error = error);
        };
        // Unknown partitions are refused, and then nothing is added.
        let mut unknown = false;
        for r in &mut resp.resources {
            for p in &mut r.partitions {
                if view.group_of(r.resource_id, p.partition).is_none() {
                    p.error = ErrorCode::UNKNOWN_RESOURCE;
                    unknown = true;
                }
            }
        }
        if unknown {
            return outbox.push(id, &Response::AddPartitionsToTxn(resp));
        }
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, _)) => {
                all(&mut resp, code);
                return outbox.push(id, &Response::AddPartitionsToTxn(resp));
            }
        };
        let current = replica.coordinator().get(&req.txn_id).cloned();
        let error = match &current {
            _ if self.busy.contains(&req.txn_id) => Some(ErrorCode::CONCURRENT_TRANSACTIONS),
            None => Some(ErrorCode::INVALID_TXN_STATE),
            Some(m) if m.producer_id != req.producer_id || m.epoch != req.producer_epoch => {
                Some(ErrorCode::PRODUCER_FENCED)
            }
            Some(m) if m.phase.preparing().is_some() => Some(ErrorCode::CONCURRENT_TRANSACTIONS),
            Some(_) => None,
        };
        if let Some(e) = error {
            all(&mut resp, e);
            return outbox.push(id, &Response::AddPartitionsToTxn(resp));
        }
        let m = current.expect("checked");
        let mut partitions = if m.phase == Phase::Ongoing { m.partitions.clone() } else { BTreeSet::new() };
        let before = partitions.len();
        for r in &req.resources {
            partitions.extend(r.partitions.iter().map(|p| (r.resource_id, *p)));
        }
        if m.phase == Phase::Ongoing && partitions.len() == before {
            return outbox.push(id, &Response::AddPartitionsToTxn(resp));
        }
        let meta = TxnMeta { phase: Phase::Ongoing, partitions, ..m };
        self.write(replica, view.now, &req.txn_id, &meta, Some((outbox, id)), Then::Add(resp));
    }

    pub(super) fn end_txn<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: EndTxnRequest,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let answer = |error, producer_id, producer_epoch| {
            Response::EndTxn(EndTxnResponse { error, producer_id, producer_epoch })
        };
        let (_, replica) = match Self::coordinator(view, groups) {
            Ok(c) => c,
            Err((code, _)) => return outbox.push(id, &answer(code, 0, 0)),
        };
        if self.busy.contains(&req.txn_id) {
            return outbox.push(id, &answer(ErrorCode::CONCURRENT_TRANSACTIONS, 0, 0));
        }
        let Some(m) = replica.coordinator().get(&req.txn_id).cloned() else {
            return outbox.push(id, &answer(ErrorCode::INVALID_TXN_STATE, 0, 0));
        };
        if m.producer_id != req.producer_id {
            return outbox.push(id, &answer(ErrorCode::PRODUCER_FENCED, 0, 0));
        }
        let decided = match m.phase {
            Phase::PrepareCommit | Phase::CompleteCommit => Some(true),
            Phase::PrepareAbort | Phase::CompleteAbort => Some(false),
            _ => None,
        };
        // A retry of the EndTxn that decided: the epoch was raised by one.
        if decided == Some(req.commit) && req.producer_epoch.checked_add(1) == Some(m.epoch) {
            return outbox.push(id, &answer(ErrorCode::NONE, m.producer_id, m.epoch));
        }
        if req.producer_epoch != m.epoch {
            return outbox.push(id, &answer(ErrorCode::PRODUCER_FENCED, 0, 0));
        }
        match m.phase {
            // Nothing was added: nothing to commit or abort.
            Phase::Empty | Phase::CompleteCommit | Phase::CompleteAbort => {
                outbox.push(id, &answer(ErrorCode::NONE, m.producer_id, m.epoch))
            }
            Phase::Ongoing => {
                let Some(epoch) = m.epoch.checked_add(1) else {
                    // Out of epochs: the producer must start over with a new id.
                    return outbox.push(id, &answer(ErrorCode::PRODUCER_FENCED, 0, 0));
                };
                let phase = if req.commit { Phase::PrepareCommit } else { Phase::PrepareAbort };
                let meta = TxnMeta { epoch, phase, ..m };
                let then = Then::End { producer_id: meta.producer_id, epoch };
                self.write(replica, view.now, &req.txn_id, &meta, Some((outbox, id)), then);
            }
            Phase::PrepareCommit | Phase::PrepareAbort => outbox.push(id, &answer(ErrorCode::INVALID_TXN_STATE, 0, 0)),
        }
    }

    /// Answers coordinator records that are decided, starts marker jobs for
    /// decided transactions, aborts transactions past their timeout, and
    /// cancels everything when this node stops coordinating. Returns the
    /// jobs for the node to run.
    pub(super) fn coordinate<R: Runtime, S: StateMachine>(
        &mut self,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) -> Vec<MarkerJob> {
        let writes = std::mem::take(&mut self.writes);
        for w in writes {
            let Some(raft) = view.coordinator.and_then(|g| groups.get(&g)).map(|r| r.raft_ref()) else { continue };
            let code = match outcome(raft, w.last, w.term) {
                Some(code) => code,
                None if view.now >= w.deadline => ErrorCode::REQUEST_TIMED_OUT,
                None => {
                    self.writes.push(w);
                    continue;
                }
            };
            self.busy.remove(&w.txn_id);
            let Some((outbox, id)) = w.reply else { continue };
            let resp = if code == ErrorCode::NONE {
                match w.then {
                    Then::Init { producer_id, epoch } => Response::InitProducer(InitProducerResponse {
                        error: ErrorCode::NONE,
                        producer_id: producer_id.unwrap_or(w.last),
                        producer_epoch: epoch,
                        leader_hint: None,
                    }),
                    Then::Add(resp) => Response::AddPartitionsToTxn(resp),
                    Then::End { producer_id, epoch } => {
                        Response::EndTxn(EndTxnResponse { error: ErrorCode::NONE, producer_id, producer_epoch: epoch })
                    }
                    Then::Nothing => continue,
                }
            } else {
                match error_response(&w.then, code, hint(raft)) {
                    Some(r) => r,
                    None => continue,
                }
            };
            outbox.push(id, &resp);
        }

        let Ok((_, replica)) = Self::coordinator(view, groups) else {
            // Not coordinating (any more): stop the jobs, forget the timers.
            for (_, (_, cancel)) in std::mem::take(&mut self.jobs) {
                cancel.set();
            }
            self.ongoing_since.clear();
            return Vec::new();
        };
        let term = replica.raft_ref().term();
        self.jobs.retain(|_, (t, cancel)| {
            let keep = *t == term;
            if !keep {
                cancel.set();
            }
            keep
        });
        let mut jobs = Vec::new();
        let mut timed_out = Vec::new();
        let state: Vec<(String, TxnMeta)> =
            replica.coordinator().iter().map(|(id, m)| (id.clone(), m.clone())).collect();
        self.ongoing_since.retain(|id, _| state.iter().any(|(i, m)| i == id && m.phase == Phase::Ongoing));
        for (txn_id, m) in state {
            match m.phase {
                Phase::Ongoing => {
                    // Measured from when this leader saw it ongoing.
                    let since = *self.ongoing_since.entry(txn_id.clone()).or_insert(view.now);
                    if view.now - since >= Duration::from_millis(u64::from(m.timeout_ms))
                        && !self.busy.contains(&txn_id)
                    {
                        timed_out.push((txn_id, m));
                    }
                }
                phase => {
                    let Some(commit) = phase.preparing() else { continue };
                    if self.jobs.contains_key(&txn_id) || self.busy.contains(&txn_id) {
                        continue;
                    }
                    let cancel = Signal::default();
                    self.jobs.insert(txn_id.clone(), (term, cancel.clone()));
                    jobs.push(MarkerJob {
                        txn_id,
                        producer_id: m.producer_id,
                        epoch: m.epoch,
                        commit,
                        term,
                        partitions: m.partitions,
                        cancel,
                    });
                }
            }
        }
        for (txn_id, m) in timed_out {
            // Abort with a higher epoch: the producer is fenced everywhere the
            // markers land.
            let meta = TxnMeta { epoch: m.epoch.saturating_add(1), phase: Phase::PrepareAbort, ..m };
            self.ongoing_since.remove(&txn_id);
            self.write(replica, view.now, &txn_id, &meta, None, Then::Nothing);
        }
        jobs
    }

    /// Every marker of `txn_id`'s decision in `term` is written: complete it.
    pub(crate) fn markers_written<R: Runtime, S: StateMachine>(
        &mut self,
        txn_id: &str,
        term: u64,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        if self.jobs.get(txn_id).is_some_and(|(t, _)| *t == term) {
            self.jobs.remove(txn_id);
        }
        let Ok((_, replica)) = Self::coordinator(view, groups) else { return };
        if replica.raft_ref().term() != term || self.busy.contains(txn_id) {
            return;
        }
        let Some(m) = replica.coordinator().get(txn_id).cloned() else { return };
        let phase = match m.phase {
            Phase::PrepareCommit => Phase::CompleteCommit,
            Phase::PrepareAbort => Phase::CompleteAbort,
            _ => return,
        };
        let meta = TxnMeta { phase, partitions: BTreeSet::new(), ..m };
        self.write(replica, view.now, txn_id, &meta, None, Then::Nothing);
    }
}

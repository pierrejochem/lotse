//! Consumer groups for streams: [`Client::join_group`] and
//! [`GroupConsumer`], plus [`Transaction::send_offsets`] for exactly-once
//! consume-transform-produce.
//!
//! The coordinator assigns partitions (KIP-848 style, ADR 0033); the
//! consumer heartbeats from [`GroupConsumer::poll`], reads its partitions
//! with read_committed from the group's committed offsets, and follows
//! assignment changes cooperatively:
//!
//! - a revoked partition is no longer read from the moment the answer that
//!   revokes it arrives; with auto-commit its position is committed, then the
//!   next heartbeat reports it released, and only then may another member
//!   get it;
//! - when no heartbeat has succeeded for the session timeout, counted from
//!   when the last successful one was sent, every partition is dropped at
//!   once (the coordinator may give them to others by then) and the
//!   consumer joins again as a new member.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lotse_proto::{
    AddOffsetsToTxnRequest, CommitOffsetsRequest, ErrorCode, GroupHeartbeatRequest, NO_OFFSET, OffsetCommitPartition,
    OffsetCommitResource, OffsetFetchRequest, Request, Response, TxnResource,
};
use lotse_rt::{Clock, Instant, Net, Rng, Spawn};

use crate::OffsetAt;
use crate::{Attempt, Client, ClientError, Transaction};

/// How a group member behaves.
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// After this long without a successful heartbeat the member drops its
    /// partitions; must not exceed the brokers' group session timeout.
    pub session_timeout: Duration,
    /// How long the member may take to release a revoked partition.
    pub rebalance_timeout: Duration,
    /// Commit a revoked partition's position before releasing it. Off for
    /// exactly-once processing, where offsets go with each transaction.
    pub auto_commit: bool,
    /// Where to start reading a partition the group has committed no
    /// offset for (ADR 0059): the log start by default, the end, or a time.
    pub start_from: OffsetAt,
    /// A stable id for this member across restarts (ADR 0072). A member
    /// that comes back with it within the session timeout takes over its
    /// partitions without a rebalance; a second instance with the same id
    /// takes over too, and the first then gets `FENCED_INSTANCE_ID` and
    /// stops. Leaving keeps the partitions for the session timeout.
    pub instance_id: Option<String>,
}

impl Default for GroupConfig {
    fn default() -> Self {
        GroupConfig {
            session_timeout: Duration::from_secs(45),
            rebalance_timeout: Duration::from_secs(60),
            auto_commit: true,
            start_from: OffsetAt::Earliest,
            instance_id: None,
        }
    }
}

fn fenced_instance() -> ClientError {
    ClientError::Broker {
        code: ErrorCode::FENCED_INSTANCE_ID,
        message: Some("another member joined with this instance id".into()),
    }
}

/// One record read by a group member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRecord {
    /// The stream.
    pub stream: String,
    /// The partition.
    pub partition: u32,
    /// The record (its offset included).
    pub record: lotse_record::Record,
}

/// A partition: `(stream, partition)`.
type Part = (String, u32);

/// A member of a consumer group, from [`Client::join_group`].
pub struct GroupConsumer<'a, R: Net + Clock + Spawn + Rng> {
    client: &'a Client<R>,
    group: String,
    cfg: GroupConfig,
    streams: Vec<String>,
    member_id: String,
    epoch: i32,
    /// Owned partitions and the next offset to read (`None` until the
    /// committed offset is known).
    owned: BTreeMap<Part, Option<u64>>,
    /// Sent time of the last heartbeat that succeeded.
    last_ok: Option<Instant>,
    next_heartbeat: Instant,
    /// Read the partitions round robin.
    turn: usize,
    /// A newer instance with the same instance id took this member's place
    /// (ADR 0072): it stays out.
    fenced: bool,
}

impl<R: Net + Clock + Spawn + Rng> std::fmt::Debug for GroupConsumer<'_, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupConsumer")
            .field("group", &self.group)
            .field("member_id", &self.member_id)
            .field("epoch", &self.epoch)
            .field("owned", &self.owned)
            .finish()
    }
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// Joins `group` to read `streams`; partitions arrive with the first
    /// calls to [`GroupConsumer::poll`].
    pub async fn join_group(
        &self,
        group: &str,
        streams: &[&str],
        cfg: GroupConfig,
    ) -> Result<GroupConsumer<'_, R>, ClientError> {
        // Learn the streams' ids (and names) first.
        self.metadata(streams).await?;
        Ok(GroupConsumer {
            client: self,
            group: group.into(),
            cfg,
            streams: streams.iter().map(|s| s.to_string()).collect(),
            member_id: String::new(),
            epoch: 0,
            owned: BTreeMap::new(),
            last_ok: None,
            next_heartbeat: self.rt.now(),
            turn: 0,
            fenced: false,
        })
    }

    fn stream_id(&self, stream: &str) -> Option<u64> {
        self.names.lock().expect("names lock").iter().find(|(_, n)| n.as_str() == stream).map(|(id, _)| *id)
    }

    fn stream_name(&self, id: u64) -> Option<String> {
        self.names.lock().expect("names lock").get(&id).cloned()
    }
}

fn group_resources(parts: &BTreeMap<Part, u64>, id_of: impl Fn(&str) -> Option<u64>) -> Vec<OffsetCommitResource> {
    let mut by: BTreeMap<u64, Vec<OffsetCommitPartition>> = BTreeMap::new();
    for ((s, p), offset) in parts {
        if let Some(id) = id_of(s) {
            by.entry(id).or_default().push(OffsetCommitPartition {
                partition: *p,
                offset: *offset,
                leader_term: 0,
                metadata: None,
            });
        }
    }
    by.into_iter().map(|(resource_id, partitions)| OffsetCommitResource { resource_id, partitions }).collect()
}

/// The first per-partition error in a commit answer, if any.
fn first_error(resp: &lotse_proto::CommitOffsetsResponse) -> Option<ErrorCode> {
    resp.resources.iter().flat_map(|r| r.partitions.iter()).map(|p| p.error).find(|e| *e != ErrorCode::NONE)
}

impl<R: Net + Clock + Spawn + Rng> GroupConsumer<'_, R> {
    /// The member id, empty until joined.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.epoch
    }

    /// The partitions this member owns now.
    pub fn assignment(&self) -> BTreeSet<(String, u32)> {
        self.owned.keys().cloned().collect()
    }

    /// The next offset to read per owned partition whose position is known.
    pub fn positions(&self) -> BTreeMap<(String, u32), u64> {
        self.owned.iter().filter_map(|(k, v)| v.map(|o| (k.clone(), o))).collect()
    }

    /// Heartbeats when due, then reads what one owned partition has (waiting
    /// up to `wait`). Returns the records, in offset order.
    pub async fn poll(&mut self, wait: Duration) -> Result<Vec<GroupRecord>, ClientError> {
        if self.fenced {
            return Err(fenced_instance());
        }
        let now = self.client.rt.now();
        if self.last_ok.is_some_and(|t| now - t >= self.cfg.session_timeout) {
            // The coordinator may have given everything away by now.
            self.lost();
        }
        if now >= self.next_heartbeat || self.member_id.is_empty() {
            self.heartbeat().await?;
        }
        self.load_positions().await;
        let ready: Vec<(Part, u64)> = self.owned.iter().filter_map(|(k, v)| v.map(|o| (k.clone(), o))).collect();
        if ready.is_empty() {
            let until = self.next_heartbeat.max(now);
            let pause = (until - self.client.rt.now()).min(wait);
            self.client.rt.sleep(pause.max(Duration::from_millis(1))).await;
            return Ok(Vec::new());
        }
        self.turn = self.turn.wrapping_add(1);
        let ((stream, partition), offset) = ready[self.turn % ready.len()].clone();
        let wait = wait.min(self.next_heartbeat.max(now) - now).max(Duration::from_millis(1));
        let f = match self.client.fetch(&stream, partition, offset, wait).await {
            Ok(f) => f,
            Err(ClientError::GaveUp(..)) => return Ok(Vec::new()),
            // Retention deleted what the group had not read: go on from
            // the oldest record left, as Kafka's `earliest` reset does.
            Err(ClientError::OffsetOutOfRange { log_start, .. }) if log_start > offset => {
                let key = (stream, partition);
                if let Some(at) = self.owned.get_mut(&key).filter(|at| **at == Some(offset)) {
                    *at = Some(log_start);
                }
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        if self.last_ok.is_none_or(|t| self.client.rt.now() - t >= self.cfg.session_timeout) {
            self.lost();
            return Ok(Vec::new());
        }
        // Revoked or lost meanwhile: nothing of it is handed out.
        let key = (stream.clone(), partition);
        if self.owned.get(&key) != Some(&Some(offset)) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for b in f.committed_batches()? {
            for record in b
                .records()
                .map_err(|e| ClientError::Broker { code: ErrorCode::CORRUPT_BATCH, message: Some(e.to_string()) })?
            {
                out.push(GroupRecord { stream: stream.clone(), partition, record });
            }
        }
        self.owned.insert(key, Some(f.next_offset(offset)));
        Ok(out)
    }

    /// Everything is gone: no commits, join again as a new member.
    fn lost(&mut self) {
        self.owned.clear();
        self.member_id.clear();
        self.epoch = 0;
        self.last_ok = None;
    }

    /// One heartbeat. Only what retrying cannot fix (a refusal, a login
    /// that fails) is an error; everything else is tried again later.
    async fn heartbeat(&mut self) -> Result<(), ClientError> {
        let c = self.client;
        let sent = c.rt.now();
        let joining = self.member_id.is_empty();
        let ids: Vec<u64> = self.streams.iter().filter_map(|s| c.stream_id(s)).collect();
        let mut owned: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for (s, p) in self.owned.keys() {
            if let Some(id) = c.stream_id(s) {
                owned.entry(id).or_default().push(*p);
            }
        }
        let req = Request::GroupHeartbeat(GroupHeartbeatRequest {
            group_id: self.group.clone(),
            tenant: self.client.cfg.tenant.clone(),
            member_id: self.member_id.clone(),
            member_epoch: if joining { 0 } else { self.epoch },
            rebalance_timeout_ms: u32::try_from(self.cfg.rebalance_timeout.as_millis()).unwrap_or(u32::MAX),
            subscribed: joining.then_some(ids),
            owned: Some(
                owned.into_iter().map(|(resource_id, partitions)| TxnResource { resource_id, partitions }).collect(),
            ),
            instance_id: self.cfg.instance_id.clone(),
        });
        let retry_soon = sent + c.cfg.backoff;
        let resp = match c.call_coordinator(&self.group, &req).await {
            Ok(Response::GroupHeartbeat(r)) => r,
            Err(Attempt::Fail(e)) => return Err(e),
            Ok(_) | Err(_) => {
                c.forget_coordinator();
                self.next_heartbeat = retry_soon;
                return Ok(());
            }
        };
        match resp.error {
            ErrorCode::NONE => {}
            ErrorCode::UNKNOWN_MEMBER_ID | ErrorCode::FENCED_MEMBER_EPOCH => {
                self.lost();
                self.next_heartbeat = retry_soon;
                return Ok(());
            }
            // A newer instance with this instance id took the place: stop,
            // rather than join again and take it back.
            ErrorCode::FENCED_INSTANCE_ID => {
                self.lost();
                self.fenced = true;
                return Err(fenced_instance());
            }
            code => {
                if code.retriable() {
                    c.forget_coordinator();
                }
                self.next_heartbeat = retry_soon;
                return Ok(());
            }
        }
        self.last_ok = Some(sent);
        self.member_id = resp.member_id;
        self.epoch = resp.member_epoch;
        let interval = Duration::from_millis(u64::from(resp.heartbeat_interval_ms));
        self.next_heartbeat = sent + interval.min(self.cfg.session_timeout / 3);
        let Some(assignment) = resp.assignment else { return Ok(()) };
        let target: BTreeSet<Part> = assignment
            .iter()
            .flat_map(|r| {
                let name = c.stream_name(r.resource_id);
                r.partitions.iter().filter_map(move |p| name.clone().map(|n| (n, *p)))
            })
            .collect();
        let revoked: Vec<Part> = self.owned.keys().filter(|k| !target.contains(*k)).cloned().collect();
        if !revoked.is_empty() {
            // Stop reading them now; commit where they got to, then report
            // them released with the next heartbeat, which comes at once.
            let positions: BTreeMap<Part, u64> =
                revoked.iter().filter_map(|k| self.owned.get(k).copied().flatten().map(|o| (k.clone(), o))).collect();
            for k in &revoked {
                self.owned.remove(k);
            }
            if self.cfg.auto_commit && !positions.is_empty() {
                let _ = self.commit_positions(&positions).await;
            }
            self.next_heartbeat = c.rt.now();
        }
        for k in target {
            self.owned.entry(k).or_insert(None);
        }
        Ok(())
    }

    /// Learns the committed offsets of partitions whose position is not
    /// known yet; a partition whose offset is held by an open transaction
    /// waits for it.
    async fn load_positions(&mut self) {
        let c = self.client;
        let unknown: Vec<Part> = self.owned.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| k.clone()).collect();
        if unknown.is_empty() {
            return;
        }
        let mut by: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
        for (s, p) in &unknown {
            if let Some(id) = c.stream_id(s) {
                by.entry(id).or_default().push(*p);
            }
        }
        let req = Request::OffsetFetch(OffsetFetchRequest {
            group_id: self.group.clone(),
            tenant: self.client.cfg.tenant.clone(),
            require_stable: true,
            resources: by
                .into_iter()
                .map(|(resource_id, partitions)| TxnResource { resource_id, partitions })
                .collect(),
        });
        let Ok(Response::OffsetFetch(resp)) = c.call_coordinator(&self.group, &req).await else {
            c.forget_coordinator();
            return;
        };
        for r in resp.resources {
            let Some(name) = c.stream_name(r.resource_id) else { continue };
            for p in r.partitions {
                if p.error != ErrorCode::NONE {
                    continue;
                }
                let start = match (p.offset, self.cfg.start_from) {
                    // From the start: a position below the log start goes
                    // on from there anyway.
                    (NO_OFFSET, OffsetAt::Earliest) => 0,
                    (NO_OFFSET, at) => match c.list_offset(&name, p.partition, at).await {
                        Ok(offset) => offset,
                        // Asked again with the next fetch of positions.
                        Err(_) => continue,
                    },
                    (offset, _) => offset,
                };
                if let Some(slot) = self.owned.get_mut(&(name.clone(), p.partition)) {
                    *slot = Some(start);
                }
            }
        }
    }

    /// Commits the position of every owned partition.
    pub async fn commit(&mut self) -> Result<(), ClientError> {
        let positions: BTreeMap<Part, u64> = self.positions();
        if positions.is_empty() {
            return Ok(());
        }
        self.commit_positions(&positions).await
    }

    async fn commit_positions(&self, positions: &BTreeMap<Part, u64>) -> Result<(), ClientError> {
        let c = self.client;
        let req = Request::CommitOffsets(CommitOffsetsRequest {
            group_id: self.group.clone(),
            tenant: self.client.cfg.tenant.clone(),
            member_id: self.member_id.clone(),
            member_epoch: self.epoch,
            resources: group_resources(positions, |s| c.stream_id(s)),
        });
        c.retrying(|| async {
            match c.call_coordinator(&self.group, &req).await? {
                Response::CommitOffsets(r) => match first_error(&r) {
                    None => Ok(()),
                    Some(code) if code.retriable() => {
                        c.forget_coordinator();
                        Err(Attempt::RetryMaybeDone(code.to_string()))
                    }
                    Some(code) => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
                },
                other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
            }
        })
        .await
    }

    /// Forgets the read positions: every owned partition starts again from
    /// the group's committed offset (after an aborted transaction).
    pub fn rewind(&mut self) {
        for v in self.owned.values_mut() {
            *v = None;
        }
    }

    /// Leaves the group, committing positions first when auto-commit is on.
    pub async fn close(mut self) {
        if self.cfg.auto_commit {
            let _ = self.commit().await;
        }
        if self.member_id.is_empty() {
            return;
        }
        let req = Request::GroupHeartbeat(GroupHeartbeatRequest {
            group_id: self.group.clone(),
            tenant: self.client.cfg.tenant.clone(),
            member_id: self.member_id.clone(),
            member_epoch: -1,
            rebalance_timeout_ms: 0,
            subscribed: None,
            owned: None,
            instance_id: self.cfg.instance_id.clone(),
        });
        let _ = self.client.call_coordinator(&self.group, &req).await;
        self.owned.clear();
    }
}

impl<R: Net + Clock + Spawn + Rng> Transaction<'_, R> {
    /// Commits the consumer's positions with this transaction: they become
    /// the group's committed offsets if it commits, and nothing changes if it
    /// aborts. A consumer that no longer owns a partition is fenced and the
    /// transaction can only be aborted.
    pub async fn send_offsets<C: Net + Clock + Spawn + Rng>(
        &mut self,
        consumer: &GroupConsumer<'_, C>,
    ) -> Result<(), ClientError> {
        if let Some(why) = &self.poisoned {
            return Err(ClientError::MustAbort(why.clone()));
        }
        let c = self.client;
        let positions = consumer.positions();
        let req = Request::AddOffsetsToTxn(AddOffsetsToTxnRequest {
            txn_id: self.txn_id.clone(),
            tenant: c.cfg.tenant.clone(),
            producer_id: self.producer_id,
            producer_epoch: self.epoch,
            group_id: consumer.group.clone(),
            member_id: consumer.member_id.clone(),
            member_epoch: consumer.epoch,
            resources: group_resources(&positions, |s| c.stream_id(s)),
        });
        let res = c
            .retrying(|| async {
                match c.call_coordinator(&self.txn_id, &req).await? {
                    Response::CommitOffsets(r) => match first_error(&r) {
                        None => Ok(()),
                        Some(code) if code.retriable() => {
                            c.forget_coordinator();
                            Err(Attempt::RetryMaybeDone(code.to_string()))
                        }
                        Some(code) => Err(Attempt::Fail(ClientError::Broker { code, message: None })),
                    },
                    other => Err(Attempt::Retry(format!("unexpected response {other:?}"))),
                }
            })
            .await;
        if let Err(e) = &res {
            // Unknown or refused: committing could commit offsets nobody
            // knows, or a zombie's. Only an abort is left.
            self.poisoned = Some(format!("offsets not added: {e}"));
        }
        res
    }
}

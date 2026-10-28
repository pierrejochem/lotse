//! Request and response bodies, version 0.
//!
//! Data-path requests address resources only by the `u64` ids from
//! `Metadata`. Every partition in a response carries its own error code, so
//! a request can partly succeed.

use bytes::Bytes;

use crate::codec::{Reader, Writer};
use crate::{ApiKey, ApiVersionRange, DecodeError, ErrorCode};

/// Tag of [`LeaderHint`] in a partition response.
const TAG_LEADER_HINT: u64 = 0;

/// Where a partition's leader is, sent with [`ErrorCode::NOT_LEADER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaderHint {
    /// The leader's node id.
    pub leader: u32,
    /// Its term.
    pub term: u64,
}

fn put_hint(w: &mut Writer, hint: Option<LeaderHint>) {
    match hint {
        Some(h) => {
            let mut v = Writer::new();
            v.u32(h.leader);
            v.u64(h.term);
            w.tags(&[(TAG_LEADER_HINT, v.finish())]);
        }
        None => w.no_tags(),
    }
}

fn get_hint(r: &mut Reader) -> Result<Option<LeaderHint>, DecodeError> {
    let mut hint = None;
    for (tag, value) in r.tags()? {
        if tag == TAG_LEADER_HINT {
            let mut v = Reader::new(value);
            hint = Some(LeaderHint { leader: v.u32()?, term: v.u64()? });
            v.finish()?;
        }
        // Unknown tags come from newer versions: skipped.
    }
    Ok(hint)
}

/// Kinds of resources in [`MetadataResponse`].
pub mod kind {
    /// A stream with partitions.
    pub const STREAM: u8 = 0;
    /// A queue: one partition, 0.
    pub const QUEUE: u8 = 1;
    /// An exchange: routes into queues, has no partitions.
    pub const EXCHANGE: u8 = 2;
}

/// The node's answer to `ApiVersions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponse {
    /// Version range per API key.
    pub versions: Vec<ApiVersionRange>,
    /// Feature names.
    pub features: Vec<String>,
}

impl ApiVersionsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.versions, |w, v| {
            w.u16(v.api_key.0);
            w.u16(v.min);
            w.u16(v.max);
        });
        w.array(&self.features, |w, f| w.string(f));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let versions =
            r.array(6, |r| Ok(ApiVersionRange { api_key: ApiKey(r.u16()?), min: r.u16()?, max: r.u16()? }))?;
        let features = r.array(2, Reader::string)?;
        r.tags()?;
        Ok(ApiVersionsResponse { versions, features })
    }
}

/// Asks for nodes and resources.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataRequest {
    /// Resource names; empty asks for all.
    pub names: Vec<String>,
}

impl MetadataRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.names, |w, n| w.string(n));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let names = r.array(2, Reader::string)?;
        r.tags()?;
        Ok(MetadataRequest { names })
    }
}

/// A broker node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    /// Node id.
    pub node_id: u32,
    /// Host clients connect to.
    pub host: String,
    /// Port clients connect to.
    pub port: u16,
    /// Rack, or empty.
    pub rack: String,
}

/// One partition of a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Partition number.
    pub partition: u32,
    /// Leader node, `0` if none is known.
    pub leader: u32,
    /// The leader's term.
    pub leader_term: u64,
    /// Voters of the partition's Raft group.
    pub voters: Vec<u32>,
}

/// A resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceMetadata {
    /// [`ErrorCode::UNKNOWN_RESOURCE`] for a name that does not exist.
    pub error: ErrorCode,
    /// Name.
    pub name: String,
    /// Id used by data-path requests.
    pub resource_id: u64,
    /// [`kind`].
    pub kind: u8,
    /// Partitions.
    pub partitions: Vec<PartitionMetadata>,
}

/// Nodes and resources.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataResponse {
    /// Every broker node.
    pub brokers: Vec<Broker>,
    /// The resources asked for.
    pub resources: Vec<ResourceMetadata>,
}

impl MetadataResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.brokers, |w, b| {
            w.u32(b.node_id);
            w.string(&b.host);
            w.u16(b.port);
            w.string(&b.rack);
            w.no_tags();
        });
        w.array(&self.resources, |w, res| {
            w.u16(res.error.0);
            w.string(&res.name);
            w.u64(res.resource_id);
            w.u8(res.kind);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u32(p.leader);
                w.u64(p.leader_term);
                w.array(&p.voters, |w, v| w.u32(*v));
                w.no_tags();
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let brokers = r.array(11, |r| {
            let b = Broker { node_id: r.u32()?, host: r.string()?, port: r.u16()?, rack: r.string()? };
            r.tags()?;
            Ok(b)
        })?;
        let resources = r.array(15, |r| {
            let (error, name, resource_id, kind) = (ErrorCode(r.u16()?), r.string()?, r.u64()?, r.u8()?);
            let partitions = r.array(18, |r| {
                let p = PartitionMetadata {
                    partition: r.u32()?,
                    leader: r.u32()?,
                    leader_term: r.u64()?,
                    voters: r.array(4, Reader::u32)?,
                };
                r.tags()?;
                Ok(p)
            })?;
            r.tags()?;
            Ok(ResourceMetadata { error, name, resource_id, kind, partitions })
        })?;
        r.tags()?;
        Ok(MetadataResponse { brokers, resources })
    }
}

/// A batch for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartition {
    /// Partition number.
    pub partition: u32,
    /// One record batch, as built by the producer.
    pub batch: Bytes,
}

/// Batches for one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResource {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<ProducePartition>,
}

/// Appends batches to stream partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceRequest {
    /// Transaction, if the batches belong to one.
    pub txn_id: Option<String>,
    /// How long to wait for the quorum.
    pub timeout_ms: u32,
    /// The batches.
    pub resources: Vec<ProduceResource>,
}

impl ProduceRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.opt_string(self.txn_id.as_deref());
        w.u32(self.timeout_ms);
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.bytes(&p.batch);
                w.no_tags();
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let (txn_id, timeout_ms) = (r.opt_string()?, r.u32()?);
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(9, |r| {
                let p = ProducePartition { partition: r.u32()?, batch: r.bytes()? };
                r.tags()?;
                Ok(p)
            })?;
            r.tags()?;
            Ok(ProduceResource { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(ProduceRequest { txn_id, timeout_ms, resources })
    }
}

/// The outcome for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducePartitionResponse {
    /// Partition number.
    pub partition: u32,
    /// Outcome.
    pub error: ErrorCode,
    /// Offset of the batch's first record.
    pub base_offset: u64,
    /// Append time, milliseconds since the epoch, or `-1`.
    pub append_ts: i64,
    /// Details for humans.
    pub error_msg: Option<String>,
    /// The leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

/// Outcomes for one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResourceResponse {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<ProducePartitionResponse>,
}

/// The node's answer to `Produce`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceResponse {
    /// How long the client should wait before its next request.
    pub throttle_ms: u32,
    /// Per resource.
    pub resources: Vec<ProduceResourceResponse>,
}

impl ProduceResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.throttle_ms);
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u16(p.error.0);
                w.u64(p.base_offset);
                w.i64(p.append_ts);
                w.opt_string(p.error_msg.as_deref());
                put_hint(w, p.leader_hint);
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let throttle_ms = r.u32()?;
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(25, |r| {
                Ok(ProducePartitionResponse {
                    partition: r.u32()?,
                    error: ErrorCode(r.u16()?),
                    base_offset: r.u64()?,
                    append_ts: r.i64()?,
                    error_msg: r.opt_string()?,
                    leader_hint: get_hint(r)?,
                })
            })?;
            r.tags()?;
            Ok(ProduceResourceResponse { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(ProduceResponse { throttle_ms, resources })
    }
}

/// Isolation levels for [`FetchRequest`].
pub mod isolation {
    /// Everything up to the high watermark.
    pub const UNCOMMITTED: u8 = 0;
    /// Up to the last stable offset; aborted transactions listed.
    pub const COMMITTED: u8 = 1;
}

/// Where to read in one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartition {
    /// Partition number.
    pub partition: u32,
    /// First offset wanted.
    pub fetch_offset: u64,
    /// The leader term the client knows, or `0` not to check.
    pub leader_term: u64,
    /// Byte limit for this partition (at least one batch is returned).
    pub max_bytes: u32,
}

/// Partitions to read of one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResource {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<FetchPartition>,
}

/// Reads batches from stream partitions, waiting up to `max_wait_ms` for at
/// least `min_bytes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchRequest {
    /// Longest wait for `min_bytes`.
    pub max_wait_ms: u32,
    /// Bytes to wait for.
    pub min_bytes: u32,
    /// Byte limit for the whole response.
    pub max_bytes: u32,
    /// [`isolation`].
    pub isolation: u8,
    /// What to read.
    pub resources: Vec<FetchResource>,
}

impl FetchRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.max_wait_ms);
        w.u32(self.min_bytes);
        w.u32(self.max_bytes);
        w.u8(self.isolation);
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u64(p.fetch_offset);
                w.u64(p.leader_term);
                w.u32(p.max_bytes);
                w.no_tags();
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let (max_wait_ms, min_bytes, max_bytes, isolation) = (r.u32()?, r.u32()?, r.u32()?, r.u8()?);
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(25, |r| {
                let p = FetchPartition {
                    partition: r.u32()?,
                    fetch_offset: r.u64()?,
                    leader_term: r.u64()?,
                    max_bytes: r.u32()?,
                };
                r.tags()?;
                Ok(p)
            })?;
            r.tags()?;
            Ok(FetchResource { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(FetchRequest { max_wait_ms, min_bytes, max_bytes, isolation, resources })
    }
}

/// A transaction aborted inside the returned range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    /// Its producer.
    pub producer_id: u64,
    /// Its first offset.
    pub first_offset: u64,
}

/// What was read from one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPartitionResponse {
    /// Partition number.
    pub partition: u32,
    /// Outcome.
    pub error: ErrorCode,
    /// Offsets below this are committed.
    pub high_watermark: u64,
    /// Offsets below this are committed and not in an open transaction.
    pub last_stable_offset: u64,
    /// First offset still in the log.
    pub log_start_offset: u64,
    /// Aborted transactions in `records`, for `read_committed`.
    pub aborted_txns: Vec<AbortedTxn>,
    /// Whole record batches as stored, control batches included.
    pub records: Bytes,
    /// The leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

/// What was read from one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResourceResponse {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<FetchPartitionResponse>,
}

/// The node's answer to `Fetch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResponse {
    /// How long the client should wait before its next request.
    pub throttle_ms: u32,
    /// Per resource.
    pub resources: Vec<FetchResourceResponse>,
}

impl FetchResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.throttle_ms);
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u16(p.error.0);
                w.u64(p.high_watermark);
                w.u64(p.last_stable_offset);
                w.u64(p.log_start_offset);
                w.array(&p.aborted_txns, |w, a| {
                    w.u64(a.producer_id);
                    w.u64(a.first_offset);
                });
                w.bytes(&p.records);
                put_hint(w, p.leader_hint);
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let throttle_ms = r.u32()?;
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(36, |r| {
                Ok(FetchPartitionResponse {
                    partition: r.u32()?,
                    error: ErrorCode(r.u16()?),
                    high_watermark: r.u64()?,
                    last_stable_offset: r.u64()?,
                    log_start_offset: r.u64()?,
                    aborted_txns: r.array(16, |r| Ok(AbortedTxn { producer_id: r.u64()?, first_offset: r.u64()? }))?,
                    records: r.bytes()?,
                    leader_hint: get_hint(r)?,
                })
            })?;
            r.tags()?;
            Ok(FetchResourceResponse { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(FetchResponse { throttle_ms, resources })
    }
}

/// What [`FindCoordinatorRequest::key`] names.
pub mod coordinator {
    /// A consumer group.
    pub const GROUP: u8 = 0;
    /// A producer (idempotent or transactional); the key is the
    /// transactional id, or empty.
    pub const PRODUCER: u8 = 1;
}

/// Asks which node coordinates a group or producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorRequest {
    /// [`coordinator`].
    pub key_type: u8,
    /// Group name or transactional id.
    pub key: String,
}

impl FindCoordinatorRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u8(self.key_type);
        w.string(&self.key);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = FindCoordinatorRequest { key_type: r.u8()?, key: r.string()? };
        r.tags()?;
        Ok(req)
    }
}

/// The coordinating node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindCoordinatorResponse {
    /// [`ErrorCode::COORDINATOR_NOT_AVAILABLE`] while there is none.
    pub error: ErrorCode,
    /// Its node id.
    pub node_id: u32,
    /// Host clients connect to.
    pub host: String,
    /// Port clients connect to.
    pub port: u16,
    /// Details for humans.
    pub error_msg: Option<String>,
}

impl FindCoordinatorResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u32(self.node_id);
        w.string(&self.host);
        w.u16(self.port);
        w.opt_string(self.error_msg.as_deref());
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resp = FindCoordinatorResponse {
            error: ErrorCode(r.u16()?),
            node_id: r.u32()?,
            host: r.string()?,
            port: r.u16()?,
            error_msg: r.opt_string()?,
        };
        r.tags()?;
        Ok(resp)
    }
}

/// Asks the coordinator for a producer id and epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerRequest {
    /// Transactional id; `None` for an idempotent producer.
    pub txn_id: Option<String>,
    /// Transaction timeout.
    pub txn_timeout_ms: u32,
}

impl InitProducerRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.opt_string(self.txn_id.as_deref());
        w.u32(self.txn_timeout_ms);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = InitProducerRequest { txn_id: r.opt_string()?, txn_timeout_ms: r.u32()? };
        r.tags()?;
        Ok(req)
    }
}

/// A producer id and epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The id; never `0`.
    pub producer_id: u64,
    /// The epoch.
    pub producer_epoch: u16,
    /// The coordinator, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl InitProducerResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(InitProducerResponse {
            error: ErrorCode(r.u16()?),
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            leader_hint: get_hint(r)?,
        })
    }
}

/// Partitions of one resource in a transaction request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnResource {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<u32>,
}

fn put_txn_resources(w: &mut Writer, resources: &[TxnResource]) {
    w.array(resources, |w, r| {
        w.u64(r.resource_id);
        w.array(&r.partitions, |w, p| w.u32(*p));
        w.no_tags();
    });
}

fn get_txn_resources(r: &mut Reader) -> Result<Vec<TxnResource>, DecodeError> {
    r.array(10, |r| {
        let res = TxnResource { resource_id: r.u64()?, partitions: r.array(4, Reader::u32)? };
        r.tags()?;
        Ok(res)
    })
}

/// The outcome for one partition of a transaction request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnPartitionResult {
    /// Partition number.
    pub partition: u32,
    /// Outcome.
    pub error: ErrorCode,
}

/// Outcomes for one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnResourceResult {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<TxnPartitionResult>,
}

fn put_txn_results(w: &mut Writer, resources: &[TxnResourceResult]) {
    w.array(resources, |w, r| {
        w.u64(r.resource_id);
        w.array(&r.partitions, |w, p| {
            w.u32(p.partition);
            w.u16(p.error.0);
            w.no_tags();
        });
        w.no_tags();
    });
}

fn get_txn_results(r: &mut Reader) -> Result<Vec<TxnResourceResult>, DecodeError> {
    r.array(10, |r| {
        let resource_id = r.u64()?;
        let partitions = r.array(7, |r| {
            let p = TxnPartitionResult { partition: r.u32()?, error: ErrorCode(r.u16()?) };
            r.tags()?;
            Ok(p)
        })?;
        r.tags()?;
        Ok(TxnResourceResult { resource_id, partitions })
    })
}

/// Adds partitions to the producer's transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnRequest {
    /// Transactional id.
    pub txn_id: String,
    /// Producer id from `InitProducer`.
    pub producer_id: u64,
    /// Its epoch.
    pub producer_epoch: u16,
    /// The partitions.
    pub resources: Vec<TxnResource>,
}

impl AddPartitionsToTxnRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.txn_id);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        put_txn_resources(w, &self.resources);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = AddPartitionsToTxnRequest {
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            resources: get_txn_resources(r)?,
        };
        r.tags()?;
        Ok(req)
    }
}

/// Per partition, whether it was added.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddPartitionsToTxnResponse {
    /// Per resource.
    pub resources: Vec<TxnResourceResult>,
}

impl AddPartitionsToTxnResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        put_txn_results(w, &self.resources);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resp = AddPartitionsToTxnResponse { resources: get_txn_results(r)? };
        r.tags()?;
        Ok(resp)
    }
}

/// Commits or aborts the producer's transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnRequest {
    /// Transactional id.
    pub txn_id: String,
    /// Producer id.
    pub producer_id: u64,
    /// Its epoch.
    pub producer_epoch: u16,
    /// Commit (`true`) or abort.
    pub commit: bool,
}

impl EndTxnRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.txn_id);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        w.u8(u8::from(self.commit));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = EndTxnRequest {
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            commit: r.u8()? != 0,
        };
        r.tags()?;
        Ok(req)
    }
}

/// The decision is durable; markers follow. Ending a transaction raises the
/// producer epoch: the producer's next transaction uses the one returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndTxnResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// Producer id for the next transaction.
    pub producer_id: u64,
    /// Epoch for the next transaction.
    pub producer_epoch: u16,
}

impl EndTxnResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resp = EndTxnResponse { error: ErrorCode(r.u16()?), producer_id: r.u64()?, producer_epoch: r.u16()? };
        r.tags()?;
        Ok(resp)
    }
}

/// One marker to write, in some partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnMarker {
    /// Producer id.
    pub producer_id: u64,
    /// The epoch the transaction ended with.
    pub producer_epoch: u16,
    /// Commit (`true`) or abort.
    pub commit: bool,
    /// The coordinator's epoch (its group's Raft term).
    pub coordinator_epoch: u32,
    /// Where.
    pub resources: Vec<TxnResource>,
}

/// Internal: writes transaction markers into partitions led by this node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkersRequest {
    /// The markers.
    pub markers: Vec<TxnMarker>,
}

impl WriteTxnMarkersRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.markers, |w, m| {
            w.u64(m.producer_id);
            w.u16(m.producer_epoch);
            w.u8(u8::from(m.commit));
            w.u32(m.coordinator_epoch);
            put_txn_resources(w, &m.resources);
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let markers = r.array(17, |r| {
            let m = TxnMarker {
                producer_id: r.u64()?,
                producer_epoch: r.u16()?,
                commit: r.u8()? != 0,
                coordinator_epoch: r.u32()?,
                resources: get_txn_resources(r)?,
            };
            r.tags()?;
            Ok(m)
        })?;
        r.tags()?;
        Ok(WriteTxnMarkersRequest { markers })
    }
}

/// Per marker and partition, whether it is written (or not needed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkerResult {
    /// Producer id of the marker.
    pub producer_id: u64,
    /// Per resource.
    pub resources: Vec<TxnResourceResult>,
}

/// The partition leaders' answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTxnMarkersResponse {
    /// In request order.
    pub markers: Vec<MarkerResult>,
}

impl WriteTxnMarkersResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.markers, |w, m| {
            w.u64(m.producer_id);
            put_txn_results(w, &m.resources);
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let markers = r.array(10, |r| {
            let m = MarkerResult { producer_id: r.u64()?, resources: get_txn_results(r)? };
            r.tags()?;
            Ok(m)
        })?;
        r.tags()?;
        Ok(WriteTxnMarkersResponse { markers })
    }
}

/// Subscribes to a queue; deliveries follow as pushes on this connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeRequest {
    /// The queue.
    pub queue_id: u64,
    /// The consumer's name for itself.
    pub consumer_tag: String,
    /// Initial credit in messages.
    pub credit_msgs: u32,
    /// Initial credit in bytes.
    pub credit_bytes: u32,
    /// Single active consumer (not yet supported: refused).
    pub exclusive: bool,
}

impl SubscribeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.queue_id);
        w.string(&self.consumer_tag);
        w.u32(self.credit_msgs);
        w.u32(self.credit_bytes);
        w.u8(u8::from(self.exclusive));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = SubscribeRequest {
            queue_id: r.u64()?,
            consumer_tag: r.string()?,
            credit_msgs: r.u32()?,
            credit_bytes: r.u32()?,
            exclusive: r.u8()? != 0,
        };
        r.tags()?;
        Ok(req)
    }
}

/// The subscription, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// Names the subscription in pushes, `Credit` and `Unsubscribe`.
    pub subscription_id: u32,
    /// The queue's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl SubscribeResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u32(self.subscription_id);
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(SubscribeResponse { error: ErrorCode(r.u16()?), subscription_id: r.u32()?, leader_hint: get_hint(r)? })
    }
}

/// One message of a delivered batch, for this subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveredOffset {
    /// The message's offset.
    pub offset: u64,
    /// How often it has been delivered, this time included.
    pub delivery_count: u16,
}

/// A push to a subscription: a batch as stored and the messages in it that
/// are this subscription's. With an error, the subscription has ended
/// (e.g. `NOT_LEADER` after a leader change) and carries no batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverPush {
    /// The subscription.
    pub subscription_id: u32,
    /// `NONE` for a delivery; otherwise why the subscription ended.
    pub error: ErrorCode,
    /// One record batch from the log, unchanged.
    pub batch: Bytes,
    /// The offsets in `batch` delivered to this subscription.
    pub deliveries: Vec<DeliveredOffset>,
    /// The queue's leader, after `NOT_LEADER`.
    pub leader_hint: Option<LeaderHint>,
}

impl DeliverPush {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.subscription_id);
        w.u16(self.error.0);
        w.bytes(&self.batch);
        w.array(&self.deliveries, |w, d| {
            w.u64(d.offset);
            w.u16(d.delivery_count);
        });
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DeliverPush {
            subscription_id: r.u32()?,
            error: ErrorCode(r.u16()?),
            batch: r.bytes()?,
            deliveries: r.array(10, |r| Ok(DeliveredOffset { offset: r.u64()?, delivery_count: r.u16()? }))?,
            leader_hint: get_hint(r)?,
        })
    }
}

/// Adds credit to a subscription. Never answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreditRequest {
    /// The subscription.
    pub subscription_id: u32,
    /// Messages to add.
    pub add_msgs: u32,
    /// Bytes to add.
    pub add_bytes: u32,
}

impl CreditRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.subscription_id);
        w.u32(self.add_msgs);
        w.u32(self.add_bytes);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = CreditRequest { subscription_id: r.u32()?, add_msgs: r.u32()?, add_bytes: r.u32()? };
        r.tags()?;
        Ok(req)
    }
}

fn put_ranges(w: &mut Writer, ranges: &[(u64, u64)]) {
    w.array(ranges, |w, (f, t)| {
        w.u64(*f);
        w.u64(*t);
    });
}

fn get_ranges(r: &mut Reader) -> Result<Vec<(u64, u64)>, DecodeError> {
    r.array(16, |r| Ok((r.u64()?, r.u64()?)))
}

/// Acknowledges messages `[from, to)` of a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckRequest {
    /// The queue.
    pub queue_id: u64,
    /// Transaction, for an ack that takes effect with its commit (not yet
    /// supported: refused).
    pub txn_id: Option<String>,
    /// Offset ranges `[from, to)`.
    pub ranges: Vec<(u64, u64)>,
}

impl AckRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.queue_id);
        w.opt_string(self.txn_id.as_deref());
        put_ranges(w, &self.ranges);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = AckRequest { queue_id: r.u64()?, txn_id: r.opt_string()?, ranges: get_ranges(r)? };
        r.tags()?;
        Ok(req)
    }
}

/// Rejects messages `[from, to)`: back to the queue, or discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NackRequest {
    /// The queue.
    pub queue_id: u64,
    /// Offset ranges `[from, to)`.
    pub ranges: Vec<(u64, u64)>,
    /// Deliver again (`true`) or discard.
    pub requeue: bool,
    /// Wait before delivering again (not yet supported: must be 0).
    pub delay_ms: u32,
}

impl NackRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.queue_id);
        put_ranges(w, &self.ranges);
        w.u8(u8::from(self.requeue));
        w.u32(self.delay_ms);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = NackRequest { queue_id: r.u64()?, ranges: get_ranges(r)?, requeue: r.u8()? != 0, delay_ms: r.u32()? };
        r.tags()?;
        Ok(req)
    }
}

/// Ends a subscription; its messages in flight stay with it until acked or
/// the connection closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnsubscribeRequest {
    /// The subscription.
    pub subscription_id: u32,
}

impl UnsubscribeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.subscription_id);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = UnsubscribeRequest { subscription_id: r.u32()? };
        r.tags()?;
        Ok(req)
    }
}

/// Only an outcome (for `Ack`, `Nack`, `Unsubscribe`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutcomeResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The queue's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl OutcomeResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(OutcomeResponse { error: ErrorCode(r.u16()?), leader_hint: get_hint(r)? })
    }
}

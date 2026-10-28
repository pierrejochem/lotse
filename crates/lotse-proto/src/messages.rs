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

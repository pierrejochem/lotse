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

/// Tag of the tenant in a request that names a transactional or group id
/// (ADR 0062). Absent means [`DEFAULT_TENANT`].
const TAG_TENANT: u64 = 1;
/// A consumer group member's stable instance id (ADR 0072).
const TAG_INSTANCE: u64 = 2;
/// A consumer group member's subscription by pattern (ADR 0073).
const TAG_PATTERN: u64 = 3;

/// The tenant that always exists, and the one a request means when it names
/// none.
pub const DEFAULT_TENANT: &str = "default";

/// Writes the tags of a request in `tenant`: none for the default tenant, so
/// such requests are as they were before tenants scoped ids.
fn put_tenant(w: &mut Writer, tenant: &str) {
    if tenant == DEFAULT_TENANT {
        w.no_tags();
    } else {
        let mut v = Writer::new();
        v.string(tenant);
        w.tags(&[(TAG_TENANT, v.finish())]);
    }
}

fn get_tenant(r: &mut Reader) -> Result<String, DecodeError> {
    let mut tenant = DEFAULT_TENANT.to_string();
    for (tag, value) in r.tags()? {
        if tag == TAG_TENANT {
            let mut v = Reader::new(value);
            tenant = v.string()?;
            v.finish()?;
        }
        // Unknown tags come from newer versions: skipped.
    }
    Ok(tenant)
}

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
    /// The tenant it belongs to; empty for a name that does not exist.
    pub tenant: String,
    /// Id used by data-path requests.
    pub resource_id: u64,
    /// [`kind`].
    pub kind: u8,
    /// Partitions.
    pub partitions: Vec<PartitionMetadata>,
    /// For an exchange: its type and bindings.
    pub exchange: Option<ExchangeMetadata>,
}

/// Types of exchanges in [`ExchangeMetadata`].
pub mod exchange_type {
    /// The binding key equals the routing key.
    pub const DIRECT: u8 = 0;
    /// `.`-separated words, `*` one word, `#` zero or more.
    pub const TOPIC: u8 = 1;
    /// Every bound queue.
    pub const FANOUT: u8 = 2;
    /// By message headers.
    pub const HEADERS: u8 = 3;
}

/// An exchange's type and bindings: clients route with them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeMetadata {
    /// [`exchange_type`].
    pub exchange_type: u8,
    /// The bound queues.
    pub bindings: Vec<BindingMetadata>,
}

/// A queue bound to an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingMetadata {
    /// The queue.
    pub queue_id: u64,
    /// Matched against the routing key (direct, topic).
    pub binding_key: String,
    /// Headers to match (headers exchange); a `None` value is null.
    pub header_match: Vec<(Bytes, Option<Bytes>)>,
    /// All headers must match (`true`) or any one.
    pub match_all: bool,
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
            w.string(&res.tenant);
            w.u64(res.resource_id);
            w.u8(res.kind);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u32(p.leader);
                w.u64(p.leader_term);
                w.array(&p.voters, |w, v| w.u32(*v));
                w.no_tags();
            });
            w.u8(u8::from(res.exchange.is_some()));
            if let Some(x) = &res.exchange {
                w.u8(x.exchange_type);
                w.array(&x.bindings, |w, b| {
                    w.u64(b.queue_id);
                    w.string(&b.binding_key);
                    w.array(&b.header_match, |w, (k, v)| {
                        w.bytes(k);
                        w.u8(u8::from(v.is_some()));
                        if let Some(v) = v {
                            w.bytes(v);
                        }
                    });
                    w.u8(u8::from(b.match_all));
                    w.no_tags();
                });
            }
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
        // error, name, tenant, id, kind, no partitions, no exchange, no tags.
        let resources = r.array(18, |r| {
            let (error, name, tenant) = (ErrorCode(r.u16()?), r.string()?, r.string()?);
            let (resource_id, kind) = (r.u64()?, r.u8()?);
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
            let exchange = match r.u8()? {
                0 => None,
                _ => Some(ExchangeMetadata {
                    exchange_type: r.u8()?,
                    bindings: r.array(13, |r| {
                        let b = BindingMetadata {
                            queue_id: r.u64()?,
                            binding_key: r.string()?,
                            header_match: r.array(5, |r| {
                                let k = r.bytes()?;
                                let v = if r.u8()? != 0 { Some(r.bytes()?) } else { None };
                                Ok((k, v))
                            })?,
                            match_all: r.u8()? != 0,
                        };
                        r.tags()?;
                        Ok(b)
                    })?,
                }),
            };
            r.tags()?;
            Ok(ResourceMetadata { error, name, tenant, resource_id, kind, partitions, exchange })
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
    /// The tenant the transactional id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
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
        put_tenant(w, &self.tenant);
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
        let tenant = get_tenant(r)?;
        Ok(ProduceRequest { txn_id, tenant, timeout_ms, resources })
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
    /// The tenant the transactional id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// Transaction timeout.
    pub txn_timeout_ms: u32,
}

impl InitProducerRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.opt_string(self.txn_id.as_deref());
        w.u32(self.txn_timeout_ms);
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let (txn_id, txn_timeout_ms) = (r.opt_string()?, r.u32()?);
        let tenant = get_tenant(r)?;
        Ok(InitProducerRequest { txn_id, tenant, txn_timeout_ms })
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
    /// The tenant the transactional id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
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
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = AddPartitionsToTxnRequest {
            tenant: String::new(),
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            resources: get_txn_resources(r)?,
        };
        req.tenant = get_tenant(r)?;
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
    /// The tenant the transactional id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
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
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = EndTxnRequest {
            tenant: String::new(),
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            commit: r.u8()? != 0,
        };
        req.tenant = get_tenant(r)?;
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

/// Writes optional resources: a presence byte, then the list.
fn put_opt_resources(w: &mut Writer, resources: Option<&[TxnResource]>) {
    w.u8(u8::from(resources.is_some()));
    if let Some(r) = resources {
        put_txn_resources(w, r);
    }
}

fn get_opt_resources(r: &mut Reader) -> Result<Option<Vec<TxnResource>>, DecodeError> {
    Ok(if r.u8()? != 0 { Some(get_txn_resources(r)?) } else { None })
}

/// Joins, stays in or leaves a consumer group, and reconciles the member's
/// partitions (KIP-848 style).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeartbeatRequest {
    /// The group.
    pub group_id: String,
    /// The tenant the group id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// Empty to join; the coordinator names the member.
    pub member_id: String,
    /// 0 to join, -1 to leave, otherwise the member's current epoch.
    pub member_epoch: i32,
    /// How long the member may take to release a revoked partition.
    pub rebalance_timeout_ms: u32,
    /// The streams it reads; only when joining or when they change.
    pub subscribed: Option<Vec<u64>>,
    /// The partitions it still owns; only when that changes.
    pub owned: Option<Vec<TxnResource>>,
    /// A stable id for this member across restarts (ADR 0072), sent when
    /// joining; tag 2. A member that joins with the instance id of one
    /// already in the group takes over its place and partitions.
    pub instance_id: Option<String>,
    /// Subscribe to every stream of the tenant whose whole name matches
    /// this regular expression and that the member may read (ADR 0073),
    /// instead of `subscribed`; tag 3, sent when joining. The coordinator
    /// matches it again with every heartbeat, so streams created later
    /// join the subscription.
    pub subscribed_pattern: Option<String>,
}

impl GroupHeartbeatRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.group_id);
        w.string(&self.member_id);
        w.i32(self.member_epoch);
        w.u32(self.rebalance_timeout_ms);
        w.u8(u8::from(self.subscribed.is_some()));
        if let Some(s) = &self.subscribed {
            w.array(s, |w, id| w.u64(*id));
        }
        put_opt_resources(w, self.owned.as_deref());
        let mut tags = Vec::new();
        if self.tenant != DEFAULT_TENANT {
            let mut v = Writer::new();
            v.string(&self.tenant);
            tags.push((TAG_TENANT, v.finish()));
        }
        if let Some(i) = &self.instance_id {
            let mut v = Writer::new();
            v.string(i);
            tags.push((TAG_INSTANCE, v.finish()));
        }
        if let Some(p) = &self.subscribed_pattern {
            let mut v = Writer::new();
            v.string(p);
            tags.push((TAG_PATTERN, v.finish()));
        }
        if tags.is_empty() {
            w.no_tags();
        } else {
            w.tags(&tags);
        }
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = GroupHeartbeatRequest {
            tenant: String::new(),
            group_id: r.string()?,
            member_id: r.string()?,
            member_epoch: r.i32()?,
            rebalance_timeout_ms: r.u32()?,
            subscribed: if r.u8()? != 0 { Some(r.array(8, Reader::u64)?) } else { None },
            owned: get_opt_resources(r)?,
            instance_id: None,
            subscribed_pattern: None,
        };
        req.tenant = DEFAULT_TENANT.to_string();
        for (tag, value) in r.tags()? {
            let mut v = Reader::new(value);
            match tag {
                TAG_TENANT => req.tenant = v.string()?,
                TAG_INSTANCE => req.instance_id = Some(v.string()?),
                TAG_PATTERN => req.subscribed_pattern = Some(v.string()?),
                // Unknown tags come from newer versions: skipped.
                _ => continue,
            }
            v.finish()?;
        }
        Ok(req)
    }
}

/// The member's state after a heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeartbeatResponse {
    /// Throttle time.
    pub throttle_ms: u32,
    /// Outcome.
    pub error: ErrorCode,
    /// The member's id (new after a join).
    pub member_id: String,
    /// The member's epoch now.
    pub member_epoch: i32,
    /// When to send the next heartbeat.
    pub heartbeat_interval_ms: u32,
    /// The member's full assignment, whenever its epoch changed.
    pub assignment: Option<Vec<TxnResource>>,
}

impl GroupHeartbeatResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u32(self.throttle_ms);
        w.u16(self.error.0);
        w.string(&self.member_id);
        w.i32(self.member_epoch);
        w.u32(self.heartbeat_interval_ms);
        put_opt_resources(w, self.assignment.as_deref());
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resp = GroupHeartbeatResponse {
            throttle_ms: r.u32()?,
            error: ErrorCode(r.u16()?),
            member_id: r.string()?,
            member_epoch: r.i32()?,
            heartbeat_interval_ms: r.u32()?,
            assignment: get_opt_resources(r)?,
        };
        r.tags()?;
        Ok(resp)
    }
}

/// An offset to commit for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    /// Partition number.
    pub partition: u32,
    /// The next offset to read.
    pub offset: u64,
    /// The leader term the offset was read under, or 0.
    pub leader_term: u64,
    /// Free text kept with the offset.
    pub metadata: Option<String>,
}

/// Offsets to commit for one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitResource {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<OffsetCommitPartition>,
}

fn put_commits(w: &mut Writer, resources: &[OffsetCommitResource]) {
    w.array(resources, |w, r| {
        w.u64(r.resource_id);
        w.array(&r.partitions, |w, p| {
            w.u32(p.partition);
            w.u64(p.offset);
            w.u64(p.leader_term);
            w.opt_string(p.metadata.as_deref());
            w.no_tags();
        });
        w.no_tags();
    });
}

fn get_commits(r: &mut Reader) -> Result<Vec<OffsetCommitResource>, DecodeError> {
    r.array(10, |r| {
        let resource_id = r.u64()?;
        let partitions = r.array(23, |r| {
            let p = OffsetCommitPartition {
                partition: r.u32()?,
                offset: r.u64()?,
                leader_term: r.u64()?,
                metadata: r.opt_string()?,
            };
            r.tags()?;
            Ok(p)
        })?;
        r.tags()?;
        Ok(OffsetCommitResource { resource_id, partitions })
    })
}

/// Commits a member's offsets for its group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetsRequest {
    /// The group.
    pub group_id: String,
    /// The tenant the group id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// The member committing.
    pub member_id: String,
    /// Its epoch.
    pub member_epoch: i32,
    /// The offsets.
    pub resources: Vec<OffsetCommitResource>,
}

impl CommitOffsetsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.group_id);
        w.string(&self.member_id);
        w.i32(self.member_epoch);
        put_commits(w, &self.resources);
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = CommitOffsetsRequest {
            tenant: String::new(),
            group_id: r.string()?,
            member_id: r.string()?,
            member_epoch: r.i32()?,
            resources: get_commits(r)?,
        };
        req.tenant = get_tenant(r)?;
        Ok(req)
    }
}

/// The outcome per partition of `CommitOffsets` or `AddOffsetsToTxn`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetsResponse {
    /// Per resource and partition.
    pub resources: Vec<TxnResourceResult>,
}

impl CommitOffsetsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        put_txn_results(w, &self.resources);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resp = CommitOffsetsResponse { resources: get_txn_results(r)? };
        r.tags()?;
        Ok(resp)
    }
}

/// Reads a group's committed offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequest {
    /// The group.
    pub group_id: String,
    /// The tenant the group id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// Refuse with `UNSTABLE_OFFSET_COMMIT` while a transaction holds an
    /// uncommitted offset for a partition.
    pub require_stable: bool,
    /// The partitions.
    pub resources: Vec<TxnResource>,
}

impl OffsetFetchRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.group_id);
        w.u8(u8::from(self.require_stable));
        put_txn_resources(w, &self.resources);
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = OffsetFetchRequest {
            tenant: String::new(),
            group_id: r.string()?,
            require_stable: r.u8()? != 0,
            resources: get_txn_resources(r)?,
        };
        req.tenant = get_tenant(r)?;
        Ok(req)
    }
}

/// No committed offset.
pub const NO_OFFSET: u64 = u64::MAX;

/// One partition's committed offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartition {
    /// Partition number.
    pub partition: u32,
    /// Outcome.
    pub error: ErrorCode,
    /// The next offset to read, or [`NO_OFFSET`].
    pub offset: u64,
    /// The leader term it was committed with.
    pub leader_term: u64,
    /// What was kept with it.
    pub metadata: Option<String>,
}

/// One resource's committed offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResource {
    /// Resource id.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<OffsetFetchPartition>,
}

/// The committed offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchResponse {
    /// Per resource and partition.
    pub resources: Vec<OffsetFetchResource>,
}

impl OffsetFetchResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.resources, |w, r| {
            w.u64(r.resource_id);
            w.array(&r.partitions, |w, p| {
                w.u32(p.partition);
                w.u16(p.error.0);
                w.u64(p.offset);
                w.u64(p.leader_term);
                w.opt_string(p.metadata.as_deref());
                w.no_tags();
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(25, |r| {
                let p = OffsetFetchPartition {
                    partition: r.u32()?,
                    error: ErrorCode(r.u16()?),
                    offset: r.u64()?,
                    leader_term: r.u64()?,
                    metadata: r.opt_string()?,
                };
                r.tags()?;
                Ok(p)
            })?;
            r.tags()?;
            Ok(OffsetFetchResource { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(OffsetFetchResponse { resources })
    }
}

/// Commits a group's offsets within a transaction: they take effect when it
/// commits, and are dropped when it aborts. Answered like `CommitOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddOffsetsToTxnRequest {
    /// Transactional id.
    pub txn_id: String,
    /// The tenant the transactional id and the group id live in (ADR
    /// 0062); sent as a tag, absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// Producer id from `InitProducer`.
    pub producer_id: u64,
    /// Its epoch.
    pub producer_epoch: u16,
    /// The group.
    pub group_id: String,
    /// The member whose partitions these are; a member that no longer owns
    /// them is fenced.
    pub member_id: String,
    /// Its epoch.
    pub member_epoch: i32,
    /// The offsets.
    pub resources: Vec<OffsetCommitResource>,
}

impl AddOffsetsToTxnRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.txn_id);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        w.string(&self.group_id);
        w.string(&self.member_id);
        w.i32(self.member_epoch);
        put_commits(w, &self.resources);
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = AddOffsetsToTxnRequest {
            tenant: String::new(),
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            group_id: r.string()?,
            member_id: r.string()?,
            member_epoch: r.i32()?,
            resources: get_commits(r)?,
        };
        req.tenant = get_tenant(r)?;
        Ok(req)
    }
}

fn put_configs(w: &mut Writer, configs: &[(String, String)]) {
    w.array(configs, |w, (k, v)| {
        w.string(k);
        w.string(v);
    });
}

fn get_configs(r: &mut Reader) -> Result<Vec<(String, String)>, DecodeError> {
    r.array(4, |r| Ok((r.string()?, r.string()?)))
}

/// Creates a stream (admin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamRequest {
    /// Its name; unique among all resources.
    pub name: String,
    /// The tenant it belongs to, which must exist.
    pub tenant: String,
    /// Number of partitions.
    pub partitions: u32,
    /// Replicas per partition.
    pub replication: u8,
    /// Settings, `key = value`.
    pub configs: Vec<(String, String)>,
}

impl CreateStreamRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.name);
        w.string(&self.tenant);
        w.u32(self.partitions);
        w.u8(self.replication);
        put_configs(w, &self.configs);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = CreateStreamRequest {
            name: r.string()?,
            tenant: r.string()?,
            partitions: r.u32()?,
            replication: r.u8()?,
            configs: get_configs(r)?,
        };
        r.tags()?;
        Ok(req)
    }
}

/// Creates a queue (admin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateQueueRequest {
    /// Its name; unique among all resources.
    pub name: String,
    /// The tenant it belongs to, which must exist.
    pub tenant: String,
    /// Priority levels, 1-10 (only 1 is supported so far).
    pub priority_levels: u8,
    /// Single active consumer (not supported so far).
    pub single_active: bool,
    /// Dead-letter queue (not supported so far).
    pub dlq: Option<String>,
    /// Replicas.
    pub replication: u8,
    /// Settings, `key = value` (`ack.timeout.ms`).
    pub configs: Vec<(String, String)>,
}

impl CreateQueueRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.name);
        w.string(&self.tenant);
        w.u8(self.priority_levels);
        w.u8(u8::from(self.single_active));
        w.opt_string(self.dlq.as_deref());
        w.u8(self.replication);
        put_configs(w, &self.configs);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = CreateQueueRequest {
            name: r.string()?,
            tenant: r.string()?,
            priority_levels: r.u8()?,
            single_active: r.u8()? != 0,
            dlq: r.opt_string()?,
            replication: r.u8()?,
            configs: get_configs(r)?,
        };
        r.tags()?;
        Ok(req)
    }
}

/// Declares an exchange (admin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclareExchangeRequest {
    /// Its name; unique among all resources.
    pub name: String,
    /// The tenant it belongs to, which must exist.
    pub tenant: String,
    /// [`exchange_type`].
    pub exchange_type: u8,
    /// Settings, `key = value`.
    pub configs: Vec<(String, String)>,
}

impl DeclareExchangeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.name);
        w.string(&self.tenant);
        w.u8(self.exchange_type);
        put_configs(w, &self.configs);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = DeclareExchangeRequest {
            name: r.string()?,
            tenant: r.string()?,
            exchange_type: r.u8()?,
            configs: get_configs(r)?,
        };
        r.tags()?;
        Ok(req)
    }
}

/// The resource created, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateResponse {
    /// Outcome; [`ErrorCode::RESOURCE_EXISTS`] names the existing one.
    pub error: ErrorCode,
    /// The resource's id.
    pub resource_id: u64,
    /// Why, for a refusal.
    pub error_msg: Option<String>,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl CreateResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u64(self.resource_id);
        w.opt_string(self.error_msg.as_deref());
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(CreateResponse {
            error: ErrorCode(r.u16()?),
            resource_id: r.u64()?,
            error_msg: r.opt_string()?,
            leader_hint: get_hint(r)?,
        })
    }
}

/// Binds a queue to an exchange, or unbinds it (admin). Answered with an
/// outcome only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindRequest {
    /// The exchange.
    pub exchange_id: u64,
    /// The queue.
    pub queue_id: u64,
    /// Matched against routing keys (direct, topic).
    pub binding_key: String,
    /// Headers to match (headers exchange); a `None` value is null.
    pub header_match: Vec<(Bytes, Option<Bytes>)>,
    /// All headers must match (`true`) or any one.
    pub match_all: bool,
}

impl BindRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.exchange_id);
        w.u64(self.queue_id);
        w.string(&self.binding_key);
        w.array(&self.header_match, |w, (k, v)| {
            w.bytes(k);
            w.u8(u8::from(v.is_some()));
            if let Some(v) = v {
                w.bytes(v);
            }
        });
        w.u8(u8::from(self.match_all));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = BindRequest {
            exchange_id: r.u64()?,
            queue_id: r.u64()?,
            binding_key: r.string()?,
            header_match: r.array(5, |r| {
                let k = r.bytes()?;
                let v = if r.u8()? != 0 { Some(r.bytes()?) } else { None };
                Ok((k, v))
            })?,
            match_all: r.u8()? != 0,
        };
        r.tags()?;
        Ok(req)
    }
}

/// Deletes a stream, queue or exchange created at runtime (admin).
/// Answered with [`AdminResponse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteResourceRequest {
    /// The resource.
    pub resource_id: u64,
    /// Only if it holds nothing: no unsettled message in a queue, no record
    /// in any partition of a stream (ADR 0063). A resource that is not
    /// empty is answered [`ErrorCode::RESOURCE_NOT_EMPTY`].
    pub if_empty: bool,
}

impl DeleteResourceRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.resource_id);
        w.u8(u8::from(self.if_empty));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = DeleteResourceRequest { resource_id: r.u64()?, if_empty: r.u8()? != 0 };
        r.tags()?;
        Ok(req)
    }
}

/// Changes a resource's settings (admin). Answered with [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsRequest {
    /// The resource.
    pub resource_id: u64,
    /// Settings to set, `key = value`.
    pub set: Vec<(String, String)>,
    /// Settings to return to their default.
    pub delete: Vec<String>,
}

impl AlterConfigsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.resource_id);
        put_configs(w, &self.set);
        w.array(&self.delete, |w, k| w.string(k));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req =
            AlterConfigsRequest { resource_id: r.u64()?, set: get_configs(r)?, delete: r.array(2, |r| r.string())? };
        r.tags()?;
        Ok(req)
    }
}

/// The outcome of an admin change that creates nothing (`DeleteResource`,
/// `AlterConfigs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// Why, for a refusal.
    pub error_msg: Option<String>,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl AdminResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.opt_string(self.error_msg.as_deref());
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(AdminResponse { error: ErrorCode(r.u16()?), error_msg: r.opt_string()?, leader_hint: get_hint(r)? })
    }
}

/// Reads resources' settings (admin).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsRequest {
    /// The resources.
    pub resource_ids: Vec<u64>,
}

impl DescribeConfigsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.resource_ids, |w, id| w.u64(*id));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = DescribeConfigsRequest { resource_ids: r.array(8, |r| r.u64())? };
        r.tags()?;
        Ok(req)
    }
}

/// Where a setting's value comes from.
pub mod config_source {
    /// Not set: the default.
    pub const DEFAULT: u8 = 0;
    /// Set at runtime (`CreateQueue`, `AlterConfigs`).
    pub const DYNAMIC: u8 = 1;
    /// From the node configuration; cannot be changed at runtime.
    pub const STATIC: u8 = 2;
}

/// One setting of a resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    /// Its key.
    pub key: String,
    /// Its value.
    pub value: String,
    /// [`config_source`].
    pub source: u8,
}

/// One resource's settings, or why not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceConfigs {
    /// The resource.
    pub resource_id: u64,
    /// [`ErrorCode::UNKNOWN_RESOURCE`] if there is no such resource.
    pub error: ErrorCode,
    /// Its settings.
    pub configs: Vec<ConfigEntry>,
}

/// The settings asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResponse {
    /// Whether the request could be served here at all.
    pub error: ErrorCode,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
    /// Per resource, in the order asked.
    pub resources: Vec<ResourceConfigs>,
}

impl DescribeConfigsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
        w.array(&self.resources, |w, r| {
            w.u64(r.resource_id);
            w.u16(r.error.0);
            w.array(&r.configs, |w, c| {
                w.string(&c.key);
                w.string(&c.value);
                w.u8(c.source);
            });
        });
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DescribeConfigsResponse {
            error: ErrorCode(r.u16()?),
            leader_hint: get_hint(r)?,
            // resource id, error, an empty settings array: 8 + 2 + 1 bytes.
            resources: r.array(11, |r| {
                Ok(ResourceConfigs {
                    resource_id: r.u64()?,
                    error: ErrorCode(r.u16()?),
                    configs: r
                        .array(5, |r| Ok(ConfigEntry { key: r.string()?, value: r.string()?, source: r.u8()? }))?,
                })
            })?,
        })
    }
}

/// Asks one node for its metrics (admin). Every node answers for itself:
/// its host and traffic, the groups it hosts, and the queues and consumer
/// groups it leads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeMetricsRequest;

impl DescribeMetricsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(DescribeMetricsRequest)
    }
}

/// Buckets of the commit latency histogram: bucket `i` counts produces
/// acknowledged in under `2^i` ms, the last one all slower ones.
pub const LATENCY_BUCKETS: usize = 17;

/// One node's metrics. Counters run since the node started; rates are the
/// difference of two answers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeMetricsResponse {
    /// Whether the node answered at all.
    pub error: ErrorCode,
    /// The node.
    pub node_id: u32,
    /// Its version.
    pub version: String,
    /// Milliseconds since it started.
    pub uptime_ms: u64,
    /// The metadata group, 0 if there is none.
    pub metadata_group: u64,
    /// The coordinator group, 0 if there is none.
    pub coordinator_group: u64,
    /// Open client connections.
    pub connections: u32,
    /// Bytes received from clients.
    pub client_in_bytes: u64,
    /// Bytes sent to clients.
    pub client_out_bytes: u64,
    /// Bytes received from other nodes.
    pub peer_in_bytes: u64,
    /// Bytes sent to other nodes.
    pub peer_out_bytes: u64,
    /// Acknowledged produces by latency: see [`LATENCY_BUCKETS`].
    pub commit_latency: Vec<u64>,
    /// The machine.
    pub host: HostMetrics,
    /// Every Raft group hosted here.
    pub partitions: Vec<PartitionMetrics>,
    /// Queues this node leads.
    pub queues: Vec<QueueMetrics>,
    /// Consumer groups, if this node leads the coordinator group.
    pub groups: Vec<GroupMetrics>,
}

/// The machine a node runs on; `None` where it cannot tell.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HostMetrics {
    /// CPU use, per mille.
    pub cpu_permille: Option<u16>,
    /// Memory the node uses, bytes.
    pub memory_bytes: Option<u64>,
    /// Memory available to it, bytes.
    pub memory_limit_bytes: Option<u64>,
    /// Free bytes on the data volume.
    pub disk_free_bytes: Option<u64>,
    /// Size of the data volume, bytes.
    pub disk_total_bytes: Option<u64>,
}

/// One Raft group hosted on the node.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionMetrics {
    /// The group.
    pub group: u64,
    /// The stream or queue it belongs to; 0 for the node's own groups
    /// (metadata, coordinator).
    pub resource_id: u64,
    /// Partition (0 for a queue).
    pub partition: u32,
    /// Whether this node leads the group.
    pub leader: bool,
    /// Raft term here.
    pub term: u64,
    /// Committed offsets end here (the high watermark).
    pub high_watermark: u64,
    /// Below the first open transaction.
    pub last_stable: u64,
    /// First offset still stored.
    pub log_start: u64,
    /// Offset after the last one in the log, committed or not.
    pub log_end: u64,
    /// Bytes of the log on this node.
    pub size_bytes: u64,
    /// Acknowledged produce bytes, while this node led the group.
    pub produce_bytes: u64,
    /// Acknowledged produce records, likewise.
    pub produce_records: u64,
    /// Bytes fetched, or for a queue delivered, likewise.
    pub fetch_bytes: u64,
    /// On the leader: the last offset each replica holds (plus one).
    pub replicas: Vec<ReplicaProgress>,
}

/// How far a replica is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaProgress {
    /// The replica's node.
    pub node_id: u32,
    /// Offset after the last one it holds.
    pub log_end: u64,
}

/// A queue the node leads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueMetrics {
    /// The queue.
    pub resource_id: u64,
    /// Messages ready for delivery.
    pub ready: u64,
    /// Delivered and not yet acknowledged.
    pub in_flight: u64,
    /// Waiting for their transaction to end.
    pub in_open_txns: u64,
    /// Messages delivered, while this node led the queue.
    pub delivered: u64,
    /// Messages acknowledged, likewise.
    pub acked: u64,
    /// Subscriptions.
    pub consumers: Vec<ConsumerMetrics>,
}

/// A queue subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerMetrics {
    /// Subscription id.
    pub subscription_id: u32,
    /// The client's address.
    pub peer: String,
    /// Messages delivered to it and not acknowledged.
    pub in_flight: u32,
    /// Messages it may still be sent.
    pub credit: u32,
}

/// A consumer group, from the coordinator's leader.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroupMetrics {
    /// Group id.
    pub group_id: String,
    /// The tenant the group id lives in (ADR 0062).
    pub tenant: String,
    /// Assignment epoch.
    pub epoch: i32,
    /// Members and what they own.
    pub members: Vec<GroupMemberMetrics>,
    /// Committed offsets.
    pub offsets: Vec<CommittedOffset>,
}

/// A member of a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberMetrics {
    /// Member id.
    pub member_id: String,
    /// Partitions it owns.
    pub owned: Vec<TxnResource>,
}

/// A group's committed offset for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedOffset {
    /// The stream.
    pub resource_id: u64,
    /// The partition.
    pub partition: u32,
    /// Next offset to read.
    pub offset: u64,
}

fn put_opt_u64(w: &mut Writer, v: Option<u64>) {
    w.u8(u8::from(v.is_some()));
    if let Some(v) = v {
        w.u64(v);
    }
}

fn get_opt_u64(r: &mut Reader) -> Result<Option<u64>, DecodeError> {
    Ok(if r.u8()? != 0 { Some(r.u64()?) } else { None })
}

impl DescribeMetricsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.u32(self.node_id);
        w.string(&self.version);
        for v in [
            self.uptime_ms,
            self.metadata_group,
            self.coordinator_group,
            u64::from(self.connections),
            self.client_in_bytes,
            self.client_out_bytes,
            self.peer_in_bytes,
            self.peer_out_bytes,
        ] {
            w.u64(v);
        }
        w.array(&self.commit_latency, |w, v| w.u64(*v));
        let h = &self.host;
        w.u8(u8::from(h.cpu_permille.is_some()));
        if let Some(c) = h.cpu_permille {
            w.u16(c);
        }
        for v in [h.memory_bytes, h.memory_limit_bytes, h.disk_free_bytes, h.disk_total_bytes] {
            put_opt_u64(w, v);
        }
        w.array(&self.partitions, |w, p| {
            w.u64(p.group);
            w.u64(p.resource_id);
            w.u32(p.partition);
            w.u8(u8::from(p.leader));
            for v in [
                p.term,
                p.high_watermark,
                p.last_stable,
                p.log_start,
                p.log_end,
                p.size_bytes,
                p.produce_bytes,
                p.produce_records,
                p.fetch_bytes,
            ] {
                w.u64(v);
            }
            w.array(&p.replicas, |w, r| {
                w.u32(r.node_id);
                w.u64(r.log_end);
            });
        });
        w.array(&self.queues, |w, q| {
            for v in [q.resource_id, q.ready, q.in_flight, q.in_open_txns, q.delivered, q.acked] {
                w.u64(v);
            }
            w.array(&q.consumers, |w, c| {
                w.u32(c.subscription_id);
                w.string(&c.peer);
                w.u32(c.in_flight);
                w.u32(c.credit);
            });
        });
        w.array(&self.groups, |w, g| {
            w.string(&g.group_id);
            w.string(&g.tenant);
            w.i32(g.epoch);
            w.array(&g.members, |w, m| {
                w.string(&m.member_id);
                put_txn_resources(w, &m.owned);
            });
            w.array(&g.offsets, |w, o| {
                w.u64(o.resource_id);
                w.u32(o.partition);
                w.u64(o.offset);
            });
        });
        w.no_tags();
    }

    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let (error, node_id, version) = (ErrorCode(r.u16()?), r.u32()?, r.string()?);
        let (uptime_ms, metadata_group, coordinator_group) = (r.u64()?, r.u64()?, r.u64()?);
        let connections = u32::try_from(r.u64()?).map_err(|_| DecodeError::Malformed("connections"))?;
        let (client_in_bytes, client_out_bytes, peer_in_bytes, peer_out_bytes) =
            (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
        let commit_latency = r.array(8, Reader::u64)?;
        let cpu_permille = if r.u8()? != 0 { Some(r.u16()?) } else { None };
        let host = HostMetrics {
            cpu_permille,
            memory_bytes: get_opt_u64(r)?,
            memory_limit_bytes: get_opt_u64(r)?,
            disk_free_bytes: get_opt_u64(r)?,
            disk_total_bytes: get_opt_u64(r)?,
        };
        // group, resource, partition, leader, nine counters, an empty array.
        let partitions = r.array(8 + 8 + 4 + 1 + 9 * 8 + 1, |r| {
            let (group, resource_id, partition, leader) = (r.u64()?, r.u64()?, r.u32()?, r.u8()? != 0);
            let mut v = [0u64; 9];
            for x in &mut v {
                *x = r.u64()?;
            }
            let replicas = r.array(12, |r| Ok(ReplicaProgress { node_id: r.u32()?, log_end: r.u64()? }))?;
            let [
                term,
                high_watermark,
                last_stable,
                log_start,
                log_end,
                size_bytes,
                produce_bytes,
                produce_records,
                fetch_bytes,
            ] = v;
            Ok(PartitionMetrics {
                group,
                resource_id,
                partition,
                leader,
                term,
                high_watermark,
                last_stable,
                log_start,
                log_end,
                size_bytes,
                produce_bytes,
                produce_records,
                fetch_bytes,
                replicas,
            })
        })?;
        // six counters and an empty array.
        let queues = r.array(6 * 8 + 1, |r| {
            let mut v = [0u64; 6];
            for x in &mut v {
                *x = r.u64()?;
            }
            // id, an empty peer, in flight, credit.
            let consumers = r.array(4 + 2 + 4 + 4, |r| {
                Ok(ConsumerMetrics {
                    subscription_id: r.u32()?,
                    peer: r.string()?,
                    in_flight: r.u32()?,
                    credit: r.u32()?,
                })
            })?;
            let [resource_id, ready, in_flight, in_open_txns, delivered, acked] = v;
            Ok(QueueMetrics { resource_id, ready, in_flight, in_open_txns, delivered, acked, consumers })
        })?;
        // an empty id and tenant, the epoch, two empty arrays.
        let groups = r.array(2 + 2 + 4 + 1 + 1, |r| {
            let (group_id, tenant, epoch) = (r.string()?, r.string()?, r.i32()?);
            // an empty id and an empty array.
            let members =
                r.array(2 + 1, |r| Ok(GroupMemberMetrics { member_id: r.string()?, owned: get_txn_resources(r)? }))?;
            let offsets = r.array(8 + 4 + 8, |r| {
                Ok(CommittedOffset { resource_id: r.u64()?, partition: r.u32()?, offset: r.u64()? })
            })?;
            Ok(GroupMetrics { group_id, tenant, epoch, members, offsets })
        })?;
        r.tags()?;
        Ok(DescribeMetricsResponse {
            error,
            node_id,
            version,
            uptime_ms,
            metadata_group,
            coordinator_group,
            connections,
            client_in_bytes,
            client_out_bytes,
            peer_in_bytes,
            peer_out_bytes,
            commit_latency,
            host,
            partitions,
            queues,
            groups,
        })
    }
}

/// Starts authenticating with a SASL mechanism (`SaslHandshake`), or starts
/// over to authenticate again before the session ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeRequest {
    /// `SCRAM-SHA-512` or `OAUTHBEARER`.
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.mechanism);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = SaslHandshakeRequest { mechanism: r.string()? };
        r.tags()?;
        Ok(req)
    }
}

/// The answer to `SaslHandshake`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslHandshakeResponse {
    /// [`ErrorCode::UNSUPPORTED_SASL_MECHANISM`] if the mechanism is not
    /// enabled here.
    pub error: ErrorCode,
    /// The mechanisms this node takes.
    pub mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.array(&self.mechanisms, |w, m| w.string(m));
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(SaslHandshakeResponse { error: ErrorCode(r.u16()?), mechanisms: r.array(2, |r| r.string())? })
    }
}

/// One step of the mechanism chosen with `SaslHandshake`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateRequest {
    /// The mechanism's message.
    pub auth_bytes: Bytes,
}

impl SaslAuthenticateRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.bytes(&self.auth_bytes);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = SaslAuthenticateRequest { auth_bytes: r.bytes()? };
        r.tags()?;
        Ok(req)
    }
}

/// The answer to one `SaslAuthenticate` step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslAuthenticateResponse {
    /// [`ErrorCode::AUTHENTICATION_FAILED`] ends the attempt.
    pub error: ErrorCode,
    /// Why, for a failure.
    pub error_msg: Option<String>,
    /// The mechanism's answer.
    pub auth_bytes: Bytes,
    /// Once authenticated: how long the session lasts, ms. Authenticate
    /// again on the same connection before it ends; after it, requests are
    /// refused with [`ErrorCode::UNAUTHORIZED`].
    pub session_lifetime_ms: u64,
}

impl SaslAuthenticateResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.opt_string(self.error_msg.as_deref());
        w.bytes(&self.auth_bytes);
        w.u64(self.session_lifetime_ms);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(SaslAuthenticateResponse {
            error: ErrorCode(r.u16()?),
            error_msg: r.opt_string()?,
            auth_bytes: r.bytes()?,
            session_lifetime_ms: r.u64()?,
        })
    }
}

/// A user's SCRAM-SHA-512 credential as the broker keeps it: never the
/// password.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialData {
    /// PBKDF2 iterations, at least 4096.
    pub iterations: u32,
    /// The salt.
    pub salt: Bytes,
    /// `H(ClientKey)`.
    pub stored_key: Bytes,
    /// `HMAC(SaltedPassword, "Server Key")`.
    pub server_key: Bytes,
}

/// Sets or removes a user's SCRAM credential (admin). Answered with
/// [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserRequest {
    /// The user name.
    pub user: String,
    /// The new credential; `None` removes the user's.
    pub credential: Option<ScramCredentialData>,
}

impl AlterUserRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.user);
        match &self.credential {
            Some(c) => {
                w.u8(1);
                w.u32(c.iterations);
                w.bytes(&c.salt);
                w.bytes(&c.stored_key);
                w.bytes(&c.server_key);
            }
            None => w.u8(0),
        }
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let user = r.string()?;
        let credential = match r.u8()? {
            0 => None,
            1 => Some(ScramCredentialData {
                iterations: r.u32()?,
                salt: r.bytes()?,
                stored_key: r.bytes()?,
                server_key: r.bytes()?,
            }),
            _ => return Err(DecodeError::Malformed("credential flag is neither 0 nor 1")),
        };
        r.tags()?;
        Ok(AlterUserRequest { user, credential })
    }
}

/// Lists the users with a SCRAM credential (admin).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeUsersRequest;

impl DescribeUsersRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(DescribeUsersRequest)
    }
}

/// One user with a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserEntry {
    /// The user name.
    pub name: String,
    /// `SCRAM-SHA-512`.
    pub mechanism: String,
    /// The credential's iterations.
    pub iterations: u32,
    /// [`config_source`]: set at runtime, or from the node configuration.
    pub source: u8,
}

/// The users, sorted by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUsersResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
    /// The users.
    pub users: Vec<UserEntry>,
}

impl DescribeUsersResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
        w.array(&self.users, |w, u| {
            w.string(&u.name);
            w.string(&u.mechanism);
            w.u32(u.iterations);
            w.u8(u.source);
        });
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DescribeUsersResponse {
            error: ErrorCode(r.u16()?),
            leader_hint: get_hint(r)?,
            // name, mechanism, iterations, source: 2 + 2 + 4 + 1 bytes.
            users: r.array(9, |r| {
                Ok(UserEntry { name: r.string()?, mechanism: r.string()?, iterations: r.u32()?, source: r.u8()? })
            })?,
        })
    }
}

/// Creates a tenant (cluster admin). Answered with [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTenantRequest {
    /// Its name.
    pub name: String,
}

impl CreateTenantRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.name);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = CreateTenantRequest { name: r.string()? };
        r.tags()?;
        Ok(req)
    }
}

/// Deletes a tenant that holds no resources (cluster admin), with its role
/// bindings. Answered with [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTenantRequest {
    /// Its name.
    pub name: String,
}

impl DeleteTenantRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.name);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = DeleteTenantRequest { name: r.string()? };
        r.tags()?;
        Ok(req)
    }
}

/// Lists the tenants the caller has a role in (every tenant for a cluster
/// admin).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeTenantsRequest;

impl DescribeTenantsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(DescribeTenantsRequest)
    }
}

/// One tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantEntry {
    /// The name.
    pub name: String,
    /// [`config_source`]: created at runtime, or from the node configuration.
    pub source: u8,
    /// How many resources it holds.
    pub resources: u32,
}

/// The tenants, sorted by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTenantsResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
    /// The tenants.
    pub tenants: Vec<TenantEntry>,
}

impl DescribeTenantsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
        w.array(&self.tenants, |w, t| {
            w.string(&t.name);
            w.u8(t.source);
            w.u32(t.resources);
        });
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DescribeTenantsResponse {
            error: ErrorCode(r.u16()?),
            leader_hint: get_hint(r)?,
            // name, source, resources: 2 + 1 + 4 bytes.
            tenants: r.array(7, |r| Ok(TenantEntry { name: r.string()?, source: r.u8()?, resources: r.u32()? }))?,
        })
    }
}

/// Kinds of subjects in [`RoleBindingData`].
pub mod subject_kind {
    /// A principal by name.
    pub const USER: u8 = 0;
    /// A group at the identity provider.
    pub const GROUP: u8 = 1;
}

/// Roles in [`RoleBindingData`].
pub mod role {
    /// Everything, everywhere.
    pub const CLUSTER_ADMIN: u8 = 0;
    /// Creating, changing, deleting and binding resources in a tenant, and
    /// what the roles below may.
    pub const TENANT_ADMIN: u8 = 1;
    /// Writing to resources.
    pub const PRODUCER: u8 = 2;
    /// Reading from resources.
    pub const CONSUMER: u8 = 3;
    /// Seeing resources.
    pub const VIEWER: u8 = 4;
    /// Seeing everything, everywhere, and nothing else.
    pub const CLUSTER_VIEWER: u8 = 5;
}

/// A role granted to a user or group, in a tenant, for the resources whose
/// names start with `prefix`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoleBindingData {
    /// [`subject_kind`].
    pub subject_kind: u8,
    /// The user or group.
    pub subject: String,
    /// [`role`].
    pub role: u8,
    /// The tenant; empty for [`role::CLUSTER_ADMIN`] and
    /// [`role::CLUSTER_VIEWER`].
    pub tenant: String,
    /// Only resources whose names start with this; empty for all.
    pub prefix: String,
}

fn put_binding_data(w: &mut Writer, b: &RoleBindingData) {
    w.u8(b.subject_kind);
    w.string(&b.subject);
    w.u8(b.role);
    w.string(&b.tenant);
    w.string(&b.prefix);
}

fn get_binding_data(r: &mut Reader) -> Result<RoleBindingData, DecodeError> {
    Ok(RoleBindingData {
        subject_kind: r.u8()?,
        subject: r.string()?,
        role: r.u8()?,
        tenant: r.string()?,
        prefix: r.string()?,
    })
}

/// The smallest [`RoleBindingData`]: two flags and three empty strings.
const BINDING_DATA_MIN: usize = 1 + 2 + 1 + 2 + 2;

/// Adds and removes role bindings (cluster admin). Answered with
/// [`AdminResponse`]. Removals apply before additions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoleBindingsRequest {
    /// Bindings to add.
    pub add: Vec<RoleBindingData>,
    /// Bindings to remove.
    pub remove: Vec<RoleBindingData>,
}

impl AlterRoleBindingsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.add, put_binding_data);
        w.array(&self.remove, put_binding_data);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = AlterRoleBindingsRequest {
            add: r.array(BINDING_DATA_MIN, get_binding_data)?,
            remove: r.array(BINDING_DATA_MIN, get_binding_data)?,
        };
        r.tags()?;
        Ok(req)
    }
}

/// Lists every role binding (cluster admin).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeRoleBindingsRequest;

impl DescribeRoleBindingsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(DescribeRoleBindingsRequest)
    }
}

/// The role bindings, each with [`config_source`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeRoleBindingsResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
    /// The bindings and where each comes from.
    pub bindings: Vec<(RoleBindingData, u8)>,
}

impl DescribeRoleBindingsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
        w.array(&self.bindings, |w, (b, source)| {
            put_binding_data(w, b);
            w.u8(*source);
        });
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DescribeRoleBindingsResponse {
            error: ErrorCode(r.u16()?),
            leader_hint: get_hint(r)?,
            bindings: r.array(BINDING_DATA_MIN + 1, |r| Ok((get_binding_data(r)?, r.u8()?)))?,
        })
    }
}

/// Asks who the connection acts as, and with which roles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WhoAmIRequest;

impl WhoAmIRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(WhoAmIRequest)
    }
}

/// Kinds of principals in [`WhoAmIResponse`].
pub mod principal_kind {
    /// Not authenticated, on a node that does not require it: may do
    /// everything.
    pub const ANONYMOUS: u8 = 0;
    /// A broker node.
    pub const NODE: u8 = 1;
    /// A user or service.
    pub const USER: u8 = 2;
}

/// Who the connection acts as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhoAmIResponse {
    /// [`principal_kind`].
    pub kind: u8,
    /// The name (the node id for a node).
    pub name: String,
    /// Groups at the identity provider.
    pub groups: Vec<String>,
    /// The bindings that apply, by name or by group.
    pub bindings: Vec<RoleBindingData>,
}

impl WhoAmIResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u8(self.kind);
        w.string(&self.name);
        w.array(&self.groups, |w, g| w.string(g));
        w.array(&self.bindings, put_binding_data);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(WhoAmIResponse {
            kind: r.u8()?,
            name: r.string()?,
            groups: r.array(2, Reader::string)?,
            bindings: r.array(BINDING_DATA_MIN, get_binding_data)?,
        })
    }
}

/// Adds a node to the cluster (cluster admin). Answered with
/// [`AdminResponse`] once the metadata group has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddNodeRequest {
    /// Its id: never used in this cluster before.
    pub node_id: u64,
    /// Where the other nodes reach it (`host:port`).
    pub peer: String,
    /// Where clients reach it (`host:port`).
    pub client: String,
}

impl AddNodeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.node_id);
        w.string(&self.peer);
        w.string(&self.client);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = AddNodeRequest { node_id: r.u64()?, peer: r.string()?, client: r.string()? };
        r.tags()?;
        Ok(req)
    }
}

/// Starts draining a node: its replicas move to other nodes, then it
/// leaves the cluster (cluster admin). Answered with [`AdminResponse`]
/// once the drain has started, not when it is done.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveNodeRequest {
    /// The node.
    pub node_id: u64,
}

impl RemoveNodeRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.node_id);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = RemoveNodeRequest { node_id: r.u64()? };
        r.tags()?;
        Ok(req)
    }
}

/// Lists the cluster's nodes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DescribeNodesRequest;

impl DescribeNodesRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        r.tags()?;
        Ok(DescribeNodesRequest)
    }
}

/// States of a node in [`NodeEntry`].
pub mod node_state {
    /// Holds replicas; new ones may be placed on it.
    pub const ACTIVE: u8 = 0;
    /// Its replicas are moving to other nodes; then it leaves.
    pub const DRAINING: u8 = 1;
}

/// One node of the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntry {
    /// Its id.
    pub node_id: u64,
    /// Where the other nodes reach it.
    pub peer: String,
    /// Where clients reach it.
    pub client: String,
    /// [`node_state`].
    pub state: u8,
    /// Groups it is to be a voter of.
    pub replicas: u32,
    /// Of those, groups whose voters are not yet in place.
    pub moving: u32,
}

/// The cluster's nodes, from the metadata group's leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeNodesResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// The metadata group's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
    /// The nodes, by id.
    pub nodes: Vec<NodeEntry>,
}

impl DescribeNodesResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        put_hint(w, self.leader_hint);
        w.array(&self.nodes, |w, n| {
            w.u64(n.node_id);
            w.string(&n.peer);
            w.string(&n.client);
            w.u8(n.state);
            w.u32(n.replicas);
            w.u32(n.moving);
        });
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(DescribeNodesResponse {
            error: ErrorCode(r.u16()?),
            leader_hint: get_hint(r)?,
            nodes: r.array(30, |r| {
                Ok(NodeEntry {
                    node_id: r.u64()?,
                    peer: r.string()?,
                    client: r.string()?,
                    state: r.u8()?,
                    replicas: r.u32()?,
                    moving: r.u32()?,
                })
            })?,
        })
    }
}

/// A group's leader reports that the group's Raft membership has exactly
/// these voters and nothing in flight (internal, from a node). Answered
/// with [`AdminResponse`] once the metadata group has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleVotersRequest {
    /// The group.
    pub group: u64,
    /// Its voters.
    pub voters: Vec<u64>,
}

impl SettleVotersRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.group);
        w.array(&self.voters, |w, v| w.u64(*v));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = SettleVotersRequest { group: r.u64()?, voters: r.array(8, |r| r.u64())? };
        r.tags()?;
        Ok(req)
    }
}

/// A queue's leader forwards messages to its dead-letter queue (internal,
/// from a node). Answered with [`OutcomeResponse`] once the queue's leader
/// has them committed. Sending a message again is harmless: the receiving
/// queue knows what it took from the source (ADR 0055).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterRequest {
    /// The dead-letter queue.
    pub queue_id: u64,
    /// The messages, each a one-record batch that carries its origin in
    /// its headers.
    pub batches: Vec<Bytes>,
}

impl DeadLetterRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.queue_id);
        w.array(&self.batches, |w, b| w.bytes(b));
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = DeadLetterRequest { queue_id: r.u64()?, batches: r.array(4, |r| r.bytes())? };
        r.tags()?;
        Ok(req)
    }
}

/// A partition's leader asks the transaction coordinator, before it takes a
/// producer's first transactional batch of an epoch, whether that
/// transaction is open and holds the partition (internal, from a node).
/// Pins the leader of a partition of a stream or queue created at runtime to
/// one of its voters, or unpins it (cluster admin, ADR 0065). The pin is
/// stored in the metadata group: the group's leader hands over to the node
/// and the automatic spreading of leaders (ADR 0050) leaves it there while
/// the node is a voter. Answered with [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveLeaderRequest {
    /// The stream or queue.
    pub resource_id: u64,
    /// The partition (0 for a queue).
    pub partition: u32,
    /// The node to lead it; 0 unpins it, so leaders spread by themselves.
    pub node_id: u64,
}

impl MoveLeaderRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.resource_id);
        w.u32(self.partition);
        w.u64(self.node_id);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = MoveLeaderRequest { resource_id: r.u64()?, partition: r.u32()?, node_id: r.u64()? };
        r.tags()?;
        Ok(req)
    }
}

/// Revokes a user's or an IdP group's sessions (cluster admin, ADR 0070):
/// every node ends the sessions that authenticated before `before_ms` and
/// refuses tokens issued before it. Answered with [`AdminResponse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeSessionsRequest {
    /// [`subject_kind`]: a user or a group.
    pub subject_kind: u8,
    /// The user or group.
    pub subject: String,
    /// Milliseconds since the Unix epoch; 0 for now, by the metadata
    /// leader's clock.
    pub before_ms: u64,
}

impl RevokeSessionsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u8(self.subject_kind);
        w.string(&self.subject);
        w.u64(self.before_ms);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = RevokeSessionsRequest { subject_kind: r.u8()?, subject: r.string()?, before_ms: r.u64()? };
        r.tags()?;
        Ok(req)
    }
}

/// `PurgeQueue` and `Requeue` (ADR 0064): an action on a queue's ready
/// messages, served by the queue's leader. Each request acts on at most a
/// few thousand messages below `up_to`; the answer says whether more are
/// left, and the client sends the same request with the answer's `up_to`
/// until none are, so that messages produced meanwhile are left alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueActionRequest {
    /// The queue; for `Requeue`, a dead-letter queue.
    pub queue_id: u64,
    /// Act only below this offset; 0: below the queue's end as the leader
    /// sees it now.
    pub up_to: u64,
}

impl QueueActionRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.queue_id);
        w.u64(self.up_to);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = QueueActionRequest { queue_id: r.u64()?, up_to: r.u64()? };
        r.tags()?;
        Ok(req)
    }
}

/// The answer to a [`QueueActionRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueActionResponse {
    /// Outcome.
    pub error: ErrorCode,
    /// Why, for a refusal.
    pub error_msg: Option<String>,
    /// Messages purged or requeued by this request, once written.
    pub count: u64,
    /// The bound it acted below; send it again to go on.
    pub up_to: u64,
    /// Whether ready messages below `up_to` are left for another request.
    pub more: bool,
    /// The queue's leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

impl QueueActionResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u16(self.error.0);
        w.opt_string(self.error_msg.as_deref());
        w.u64(self.count);
        w.u64(self.up_to);
        w.u8(u8::from(self.more));
        put_hint(w, self.leader_hint);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        Ok(QueueActionResponse {
            error: ErrorCode(r.u16()?),
            error_msg: r.opt_string()?,
            count: r.u64()?,
            up_to: r.u64()?,
            more: r.u8()? != 0,
            leader_hint: get_hint(r)?,
        })
    }
}

/// Whether a partition holds anything (internal, [`ApiKey::CHECK_EMPTY`],
/// ADR 0063): sent by the metadata leader to the partition's leader before a
/// conditional delete. Answered with [`OutcomeResponse`]: `NONE` if it holds
/// nothing, [`ErrorCode::RESOURCE_NOT_EMPTY`] if it does, `NOT_LEADER` with a
/// hint elsewhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckEmptyRequest {
    /// The stream or queue.
    pub resource_id: u64,
    /// The partition (0 for a queue).
    pub partition: u32,
}

impl CheckEmptyRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u64(self.resource_id);
        w.u32(self.partition);
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let req = CheckEmptyRequest { resource_id: r.u64()?, partition: r.u32()? };
        r.tags()?;
        Ok(req)
    }
}

/// Answered with [`OutcomeResponse`]: `NONE` if so, `INVALID_TXN_STATE` if
/// not, `PRODUCER_FENCED` for an older epoch (ADR 0057).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyTxnRequest {
    /// The transactional id the produce came with.
    pub txn_id: String,
    /// The tenant the transactional id lives in (ADR 0062); sent as a tag,
    /// absent for [`DEFAULT_TENANT`].
    pub tenant: String,
    /// The batch's producer id.
    pub producer_id: u64,
    /// The batch's producer epoch.
    pub producer_epoch: u16,
    /// The partition's stream or queue.
    pub resource_id: u64,
    /// The partition.
    pub partition: u32,
}

impl VerifyTxnRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.string(&self.txn_id);
        w.u64(self.producer_id);
        w.u16(self.producer_epoch);
        w.u64(self.resource_id);
        w.u32(self.partition);
        put_tenant(w, &self.tenant);
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let mut req = VerifyTxnRequest {
            tenant: String::new(),
            txn_id: r.string()?,
            producer_id: r.u64()?,
            producer_epoch: r.u16()?,
            resource_id: r.u64()?,
            partition: r.u32()?,
        };
        req.tenant = get_tenant(r)?;
        Ok(req)
    }
}

/// What [`ListOffsetsPartition::timestamp`] may ask for besides a time.
pub mod list_offsets {
    /// The offset after the last one a reader may see: the high watermark,
    /// or the last stable offset for read_committed.
    pub const LATEST: i64 = -1;
    /// The first offset still in the log.
    pub const EARLIEST: i64 = -2;
}

/// Looks up offsets in stream partitions by time, or at either end (ADR 0059).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsRequest {
    /// [`isolation`]: what `LATEST` means and where a time lookup stops.
    pub isolation: u8,
    /// The partitions, per stream.
    pub resources: Vec<ListOffsetsResource>,
}

/// The partitions of one stream to look up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResource {
    /// The stream.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<ListOffsetsPartition>,
}

/// One partition to look up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOffsetsPartition {
    /// Partition number.
    pub partition: u32,
    /// Milliseconds since the epoch, or [`list_offsets::LATEST`] or
    /// [`list_offsets::EARLIEST`].
    pub timestamp: i64,
}

impl ListOffsetsRequest {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.u8(self.isolation);
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.i64(p.timestamp);
                w.no_tags();
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let isolation = r.u8()?;
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(13, |r| {
                let p = ListOffsetsPartition { partition: r.u32()?, timestamp: r.i64()? };
                r.tags()?;
                Ok(p)
            })?;
            r.tags()?;
            Ok(ListOffsetsResource { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(ListOffsetsRequest { isolation, resources })
    }
}

/// The answer for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListOffsetsPartitionResponse {
    /// Partition number.
    pub partition: u32,
    /// Outcome.
    pub error: ErrorCode,
    /// The offset: for a time, the first offset of the first data batch
    /// with a record at or after it, or the latest offset if there is none.
    pub offset: u64,
    /// For a time lookup that found a batch, its newest record's
    /// timestamp; -1 otherwise.
    pub timestamp: i64,
    /// The leader, with [`ErrorCode::NOT_LEADER`].
    pub leader_hint: Option<LeaderHint>,
}

/// The answers for one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResourceResponse {
    /// The stream.
    pub resource_id: u64,
    /// Its partitions.
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

/// Answer to [`ListOffsetsRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponse {
    /// Per stream, in the order asked.
    pub resources: Vec<ListOffsetsResourceResponse>,
}

impl ListOffsetsResponse {
    pub(crate) fn put(&self, w: &mut Writer) {
        w.array(&self.resources, |w, res| {
            w.u64(res.resource_id);
            w.array(&res.partitions, |w, p| {
                w.u32(p.partition);
                w.u16(p.error.0);
                w.u64(p.offset);
                w.i64(p.timestamp);
                put_hint(w, p.leader_hint);
            });
            w.no_tags();
        });
        w.no_tags();
    }
    pub(crate) fn get(r: &mut Reader) -> Result<Self, DecodeError> {
        let resources = r.array(10, |r| {
            let resource_id = r.u64()?;
            let partitions = r.array(23, |r| {
                Ok(ListOffsetsPartitionResponse {
                    partition: r.u32()?,
                    error: ErrorCode(r.u16()?),
                    offset: r.u64()?,
                    timestamp: r.i64()?,
                    leader_hint: get_hint(r)?,
                })
            })?;
            r.tags()?;
            Ok(ListOffsetsResourceResponse { resource_id, partitions })
        })?;
        r.tags()?;
        Ok(ListOffsetsResponse { resources })
    }
}

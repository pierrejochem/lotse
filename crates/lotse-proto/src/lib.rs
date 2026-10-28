//! The Lotse native protocol.
//!
//! A frame is what a `lotse_rt::Conn` carries in one piece; its length comes from the transport (a `u32` prefix on TCP), so
//! the layouts here start after it:
//!
//! ```text
//! Request:  api_key u16 | api_version u16 | correlation_id u32 | flags u16 | tagged_fields | body
//! Response: correlation_id u32 | flags u16 | tagged_fields | body
//! ```
//!
//! Requests on a connection are pipelined; responses carry the request's
//! correlation id and may come back in another order. Every structure ends
//! with tagged fields, so later versions can add fields old readers skip.
//! Field conventions are in [`codec`].
//!
//! Implemented so far: `ApiVersions`, `Metadata`, `FindCoordinator`,
//! `Produce`, `Fetch`, `InitProducer`, `AddPartitionsToTxn`, `EndTxn` and
//! the internal `WriteTxnMarkers`, and for queues `Subscribe`, `Deliver`
//! (a push), `Credit`, `Ack`, `Nack` and `Unsubscribe`, all at version 0.
//!
//! A push frame (a delivery to a queue consumer) uses the response layout
//! with correlation id 0 and flag [`flags::PUSH`], followed by its API key
//! and version: see [`encode_push`].

#![warn(missing_docs)]

pub mod codec;
mod messages;

pub use messages::*;

use bytes::Bytes;
use codec::{Reader, Writer};

/// A frame that cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The frame ends inside a field.
    #[error("frame is truncated")]
    Truncated,
    /// A field holds something no version allows.
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
    /// The request header is fine, but this node does not know the API key
    /// or version: answer with [`ErrorCode::UNSUPPORTED_VERSION`].
    #[error("unsupported API key {} version {}", .0.api_key.0, .0.api_version)]
    Unsupported(RequestHeader),
}

/// An API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApiKey(pub u16);

impl ApiKey {
    /// Versions and features of the node.
    pub const API_VERSIONS: ApiKey = ApiKey(0);
    /// Nodes, resources and partition leaders.
    pub const METADATA: ApiKey = ApiKey(1);
    /// The node coordinating a consumer group or producers.
    pub const FIND_COORDINATOR: ApiKey = ApiKey(2);
    /// Append batches to stream partitions.
    pub const PRODUCE: ApiKey = ApiKey(3);
    /// Read batches from stream partitions.
    pub const FETCH: ApiKey = ApiKey(4);
    /// A producer id and epoch for idempotent (and later transactional)
    /// produce. Keys 5 to 15 are the stream and queue keys of the design.
    pub const INIT_PRODUCER: ApiKey = ApiKey(16);
    /// Joins, stays in or leaves a consumer group.
    pub const GROUP_HEARTBEAT: ApiKey = ApiKey(6);
    /// Commits a group's offsets.
    pub const COMMIT_OFFSETS: ApiKey = ApiKey(7);
    /// Reads a group's committed offsets.
    pub const OFFSET_FETCH: ApiKey = ApiKey(8);
    /// Commits a group's offsets within a transaction.
    pub const ADD_OFFSETS_TO_TXN: ApiKey = ApiKey(19);
    /// Creates a stream (admin).
    pub const CREATE_STREAM: ApiKey = ApiKey(21);
    /// Creates a queue (admin).
    pub const CREATE_QUEUE: ApiKey = ApiKey(22);
    /// Declares an exchange (admin).
    pub const DECLARE_EXCHANGE: ApiKey = ApiKey(23);
    /// Binds a queue to an exchange (admin).
    pub const BIND: ApiKey = ApiKey(24);
    /// Deletes a resource created at runtime (admin).
    pub const DELETE_RESOURCE: ApiKey = ApiKey(25);
    /// Reads resources' settings (admin).
    pub const DESCRIBE_CONFIGS: ApiKey = ApiKey(26);
    /// Changes a resource's settings (admin).
    pub const ALTER_CONFIGS: ApiKey = ApiKey(27);
    /// Removes a binding (admin).
    pub const UNBIND: ApiKey = ApiKey(28);
    /// Subscribes to a queue.
    pub const SUBSCRIBE: ApiKey = ApiKey(10);
    /// Pushes messages to a subscription (never a request).
    pub const DELIVER: ApiKey = ApiKey(11);
    /// Adds credit to a subscription; never answered.
    pub const CREDIT: ApiKey = ApiKey(12);
    /// Acknowledges queue messages.
    pub const ACK: ApiKey = ApiKey(13);
    /// Rejects queue messages: back to the queue, or discarded.
    pub const NACK: ApiKey = ApiKey(14);
    /// Ends a subscription.
    pub const UNSUBSCRIBE: ApiKey = ApiKey(15);
    /// Adds partitions to a transaction.
    pub const ADD_PARTITIONS_TO_TXN: ApiKey = ApiKey(17);
    /// Commits or aborts a transaction. Keys 18 and 19 are `AddAcksToTxn`
    /// and `AddOffsetsToTxn`.
    pub const END_TXN: ApiKey = ApiKey(20);
    /// Internal, between nodes: the coordinator asks a partition leader to
    /// write a transaction marker. Internal keys start at 1000.
    pub const WRITE_TXN_MARKERS: ApiKey = ApiKey(1000);

    /// The keys this crate encodes, with their version range.
    pub const SUPPORTED: [ApiVersionRange; 26] = [
        ApiVersionRange { api_key: ApiKey::API_VERSIONS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::METADATA, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::FIND_COORDINATOR, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::PRODUCE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::FETCH, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::INIT_PRODUCER, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ADD_PARTITIONS_TO_TXN, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::END_TXN, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::WRITE_TXN_MARKERS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::SUBSCRIBE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::CREDIT, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ACK, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::NACK, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::UNSUBSCRIBE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::GROUP_HEARTBEAT, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::COMMIT_OFFSETS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::OFFSET_FETCH, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ADD_OFFSETS_TO_TXN, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::CREATE_STREAM, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::CREATE_QUEUE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DECLARE_EXCHANGE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::BIND, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DELETE_RESOURCE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DESCRIBE_CONFIGS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ALTER_CONFIGS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::UNBIND, min: 0, max: 0 },
    ];
}

/// The versions a node speaks for one API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    /// The key.
    pub api_key: ApiKey,
    /// Oldest version.
    pub min: u16,
    /// Newest version.
    pub max: u16,
}

/// An error code; every partition in a response carries its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(pub u16);

impl ErrorCode {
    /// Success.
    pub const NONE: ErrorCode = ErrorCode(0);
    /// Wrong node; the response carries a [`LeaderHint`] if one is known.
    pub const NOT_LEADER: ErrorCode = ErrorCode(1);
    /// The client's leader term is outdated: reload metadata.
    pub const FENCED_LEADER_TERM: ErrorCode = ErrorCode(2);
    /// The id does not exist (any more).
    pub const UNKNOWN_RESOURCE: ErrorCode = ErrorCode(3);
    /// Offset before the log start or after the leader's log end.
    pub const OFFSET_OUT_OF_RANGE: ErrorCode = ErrorCode(4);
    /// No quorum reachable.
    pub const NOT_ENOUGH_REPLICAS: ErrorCode = ErrorCode(5);
    /// No quorum confirmation within `timeout_ms`; the batch may or may not
    /// have been written.
    pub const REQUEST_TIMED_OUT: ErrorCode = ErrorCode(6);
    /// Gap in the producer sequence.
    pub const OUT_OF_ORDER_SEQUENCE: ErrorCode = ErrorCode(7);
    /// A newer producer epoch is active.
    pub const PRODUCER_FENCED: ErrorCode = ErrorCode(8);
    /// The operation does not fit the transaction's state.
    pub const INVALID_TXN_STATE: ErrorCode = ErrorCode(9);
    /// The tenant's storage quota is used up.
    pub const QUOTA_EXCEEDED: ErrorCode = ErrorCode(10);
    /// The node's hard storage limit is reached.
    pub const STORAGE_FULL: ErrorCode = ErrorCode(11);
    /// CRC or format invalid.
    pub const CORRUPT_BATCH: ErrorCode = ErrorCode(12);
    /// Access denied.
    pub const UNAUTHORIZED: ErrorCode = ErrorCode(13);
    /// API version outside the node's range.
    pub const UNSUPPORTED_VERSION: ErrorCode = ErrorCode(14);
    /// The member's epoch is outdated: join again.
    pub const FENCED_MEMBER_EPOCH: ErrorCode = ErrorCode(15);
    /// The member is not known: join again.
    pub const UNKNOWN_MEMBER_ID: ErrorCode = ErrorCode(16);
    /// The frame is larger than the limit; the connection is closed.
    pub const FRAME_TOO_LARGE: ErrorCode = ErrorCode(17);
    /// No coordinator is configured, or it has no leader right now.
    pub const COORDINATOR_NOT_AVAILABLE: ErrorCode = ErrorCode(18);
    /// Another change to the same transaction is in progress: retry.
    pub const CONCURRENT_TRANSACTIONS: ErrorCode = ErrorCode(19);
    /// A newer coordinator has taken over this producer's markers.
    pub const COORDINATOR_FENCED: ErrorCode = ErrorCode(20);
    /// The request is not valid (e.g. acknowledges offsets that were never
    /// delivered); retrying it unchanged will not help.
    pub const INVALID_REQUEST: ErrorCode = ErrorCode(21);
    /// The subscription does not exist (any more) on this connection.
    pub const UNKNOWN_SUBSCRIPTION: ErrorCode = ErrorCode(22);
    /// The subscription held a message unacknowledged longer than the queue's
    /// ack timeout; it has ended and its messages go to other consumers.
    pub const ACK_TIMED_OUT: ErrorCode = ErrorCode(23);
    /// A transaction holds an uncommitted offset for the partition: retry
    /// once it ends.
    pub const UNSTABLE_OFFSET_COMMIT: ErrorCode = ErrorCode(24);
    /// Another change to the same consumer group is in progress: retry.
    pub const CONCURRENT_GROUP_CHANGE: ErrorCode = ErrorCode(25);
    /// A resource with that name exists already; the answer names its id.
    pub const RESOURCE_EXISTS: ErrorCode = ErrorCode(26);

    /// The name used in documentation and logs.
    pub fn name(self) -> &'static str {
        const NAMES: [&str; 27] = [
            "NONE",
            "NOT_LEADER",
            "FENCED_LEADER_TERM",
            "UNKNOWN_RESOURCE",
            "OFFSET_OUT_OF_RANGE",
            "NOT_ENOUGH_REPLICAS",
            "REQUEST_TIMED_OUT",
            "OUT_OF_ORDER_SEQUENCE",
            "PRODUCER_FENCED",
            "INVALID_TXN_STATE",
            "QUOTA_EXCEEDED",
            "STORAGE_FULL",
            "CORRUPT_BATCH",
            "UNAUTHORIZED",
            "UNSUPPORTED_VERSION",
            "FENCED_MEMBER_EPOCH",
            "UNKNOWN_MEMBER_ID",
            "FRAME_TOO_LARGE",
            "COORDINATOR_NOT_AVAILABLE",
            "CONCURRENT_TRANSACTIONS",
            "COORDINATOR_FENCED",
            "INVALID_REQUEST",
            "UNKNOWN_SUBSCRIPTION",
            "ACK_TIMED_OUT",
            "UNSTABLE_OFFSET_COMMIT",
            "CONCURRENT_GROUP_CHANGE",
            "RESOURCE_EXISTS",
        ];
        NAMES.get(usize::from(self.0)).copied().unwrap_or("UNKNOWN")
    }

    /// Whether the same request may succeed later (after reloading metadata).
    pub fn retriable(self) -> bool {
        matches!(
            self,
            ErrorCode::NOT_LEADER
                | ErrorCode::FENCED_LEADER_TERM
                | ErrorCode::NOT_ENOUGH_REPLICAS
                | ErrorCode::REQUEST_TIMED_OUT
                | ErrorCode::COORDINATOR_NOT_AVAILABLE
                | ErrorCode::CONCURRENT_TRANSACTIONS
                | ErrorCode::UNSTABLE_OFFSET_COMMIT
                | ErrorCode::CONCURRENT_GROUP_CHANGE
        )
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name(), self.0)
    }
}

/// Frame flags.
pub mod flags {
    /// A push frame (delivery to a queue consumer), not a response.
    pub const PUSH: u16 = 1;
    /// A response whose body is only an error code: the request could not be
    /// served at all.
    pub const ERROR: u16 = 2;
}

/// The header of a request frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    /// What is asked.
    pub api_key: ApiKey,
    /// In which version.
    pub api_version: u16,
    /// Echoed in the response; `0` is reserved for push frames.
    pub correlation_id: u32,
    /// [`flags`].
    pub flags: u16,
}

/// A request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// [`ApiKey::API_VERSIONS`].
    ApiVersions,
    /// [`ApiKey::METADATA`].
    Metadata(MetadataRequest),
    /// [`ApiKey::FIND_COORDINATOR`].
    FindCoordinator(FindCoordinatorRequest),
    /// [`ApiKey::PRODUCE`].
    Produce(ProduceRequest),
    /// [`ApiKey::FETCH`].
    Fetch(FetchRequest),
    /// [`ApiKey::INIT_PRODUCER`].
    InitProducer(InitProducerRequest),
    /// [`ApiKey::ADD_PARTITIONS_TO_TXN`].
    AddPartitionsToTxn(AddPartitionsToTxnRequest),
    /// [`ApiKey::END_TXN`].
    EndTxn(EndTxnRequest),
    /// [`ApiKey::WRITE_TXN_MARKERS`].
    WriteTxnMarkers(WriteTxnMarkersRequest),
    /// [`ApiKey::SUBSCRIBE`].
    Subscribe(SubscribeRequest),
    /// [`ApiKey::CREDIT`]; never answered.
    Credit(CreditRequest),
    /// [`ApiKey::ACK`].
    Ack(AckRequest),
    /// [`ApiKey::NACK`].
    Nack(NackRequest),
    /// [`ApiKey::UNSUBSCRIBE`].
    Unsubscribe(UnsubscribeRequest),
    /// [`ApiKey::GROUP_HEARTBEAT`].
    GroupHeartbeat(GroupHeartbeatRequest),
    /// [`ApiKey::COMMIT_OFFSETS`].
    CommitOffsets(CommitOffsetsRequest),
    /// [`ApiKey::OFFSET_FETCH`].
    OffsetFetch(OffsetFetchRequest),
    /// [`ApiKey::ADD_OFFSETS_TO_TXN`].
    AddOffsetsToTxn(AddOffsetsToTxnRequest),
    /// [`ApiKey::CREATE_STREAM`].
    CreateStream(CreateStreamRequest),
    /// [`ApiKey::CREATE_QUEUE`].
    CreateQueue(CreateQueueRequest),
    /// [`ApiKey::DECLARE_EXCHANGE`].
    DeclareExchange(DeclareExchangeRequest),
    /// [`ApiKey::BIND`].
    Bind(BindRequest),
    /// [`ApiKey::UNBIND`].
    Unbind(BindRequest),
    /// [`ApiKey::DELETE_RESOURCE`].
    DeleteResource(DeleteResourceRequest),
    /// [`ApiKey::DESCRIBE_CONFIGS`].
    DescribeConfigs(DescribeConfigsRequest),
    /// [`ApiKey::ALTER_CONFIGS`].
    AlterConfigs(AlterConfigsRequest),
}

impl Request {
    /// The request's key.
    pub fn api_key(&self) -> ApiKey {
        match self {
            Request::ApiVersions => ApiKey::API_VERSIONS,
            Request::Metadata(_) => ApiKey::METADATA,
            Request::FindCoordinator(_) => ApiKey::FIND_COORDINATOR,
            Request::Produce(_) => ApiKey::PRODUCE,
            Request::Fetch(_) => ApiKey::FETCH,
            Request::InitProducer(_) => ApiKey::INIT_PRODUCER,
            Request::AddPartitionsToTxn(_) => ApiKey::ADD_PARTITIONS_TO_TXN,
            Request::EndTxn(_) => ApiKey::END_TXN,
            Request::WriteTxnMarkers(_) => ApiKey::WRITE_TXN_MARKERS,
            Request::Subscribe(_) => ApiKey::SUBSCRIBE,
            Request::Credit(_) => ApiKey::CREDIT,
            Request::Ack(_) => ApiKey::ACK,
            Request::Nack(_) => ApiKey::NACK,
            Request::Unsubscribe(_) => ApiKey::UNSUBSCRIBE,
            Request::GroupHeartbeat(_) => ApiKey::GROUP_HEARTBEAT,
            Request::CommitOffsets(_) => ApiKey::COMMIT_OFFSETS,
            Request::OffsetFetch(_) => ApiKey::OFFSET_FETCH,
            Request::AddOffsetsToTxn(_) => ApiKey::ADD_OFFSETS_TO_TXN,
            Request::CreateStream(_) => ApiKey::CREATE_STREAM,
            Request::CreateQueue(_) => ApiKey::CREATE_QUEUE,
            Request::DeclareExchange(_) => ApiKey::DECLARE_EXCHANGE,
            Request::Bind(_) => ApiKey::BIND,
            Request::Unbind(_) => ApiKey::UNBIND,
            Request::DeleteResource(_) => ApiKey::DELETE_RESOURCE,
            Request::DescribeConfigs(_) => ApiKey::DESCRIBE_CONFIGS,
            Request::AlterConfigs(_) => ApiKey::ALTER_CONFIGS,
        }
    }
}

/// A response; which variant follows from the request it answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    /// To [`Request::ApiVersions`].
    ApiVersions(ApiVersionsResponse),
    /// To [`Request::Metadata`].
    Metadata(MetadataResponse),
    /// To [`Request::FindCoordinator`].
    FindCoordinator(FindCoordinatorResponse),
    /// To [`Request::InitProducer`].
    InitProducer(InitProducerResponse),
    /// To [`Request::AddPartitionsToTxn`].
    AddPartitionsToTxn(AddPartitionsToTxnResponse),
    /// To [`Request::EndTxn`].
    EndTxn(EndTxnResponse),
    /// To [`Request::WriteTxnMarkers`].
    WriteTxnMarkers(WriteTxnMarkersResponse),
    /// To [`Request::Subscribe`].
    Subscribe(SubscribeResponse),
    /// To [`Request::Ack`], [`Request::Nack`] and [`Request::Unsubscribe`]:
    /// only an outcome.
    Outcome(OutcomeResponse),
    /// To [`Request::GroupHeartbeat`].
    GroupHeartbeat(GroupHeartbeatResponse),
    /// To [`Request::CommitOffsets`] and [`Request::AddOffsetsToTxn`].
    CommitOffsets(CommitOffsetsResponse),
    /// To [`Request::OffsetFetch`].
    OffsetFetch(OffsetFetchResponse),
    /// To [`Request::CreateStream`], [`Request::CreateQueue`] and
    /// [`Request::DeclareExchange`].
    Create(CreateResponse),
    /// To [`Request::DeleteResource`] and [`Request::AlterConfigs`].
    Admin(AdminResponse),
    /// To [`Request::DescribeConfigs`].
    DescribeConfigs(DescribeConfigsResponse),
    /// To [`Request::Produce`].
    Produce(ProduceResponse),
    /// To [`Request::Fetch`].
    Fetch(FetchResponse),
    /// The request could not be served at all (unsupported version, or a
    /// body that does not decode); the node closes the connection after it.
    Error(ErrorCode),
}

/// Encodes a version-0 request.
pub fn encode_request(correlation_id: u32, request: &Request) -> Bytes {
    let mut w = Writer::new();
    w.u16(request.api_key().0);
    w.u16(0);
    w.u32(correlation_id);
    w.u16(0);
    w.no_tags();
    match request {
        Request::ApiVersions => w.no_tags(),
        Request::Metadata(r) => r.put(&mut w),
        Request::FindCoordinator(r) => r.put(&mut w),
        Request::Produce(r) => r.put(&mut w),
        Request::Fetch(r) => r.put(&mut w),
        Request::InitProducer(r) => r.put(&mut w),
        Request::AddPartitionsToTxn(r) => r.put(&mut w),
        Request::EndTxn(r) => r.put(&mut w),
        Request::WriteTxnMarkers(r) => r.put(&mut w),
        Request::Subscribe(r) => r.put(&mut w),
        Request::Credit(r) => r.put(&mut w),
        Request::Ack(r) => r.put(&mut w),
        Request::Nack(r) => r.put(&mut w),
        Request::Unsubscribe(r) => r.put(&mut w),
        Request::GroupHeartbeat(r) => r.put(&mut w),
        Request::CommitOffsets(r) => r.put(&mut w),
        Request::OffsetFetch(r) => r.put(&mut w),
        Request::AddOffsetsToTxn(r) => r.put(&mut w),
        Request::CreateStream(r) => r.put(&mut w),
        Request::CreateQueue(r) => r.put(&mut w),
        Request::DeclareExchange(r) => r.put(&mut w),
        Request::Bind(r) | Request::Unbind(r) => r.put(&mut w),
        Request::DeleteResource(r) => r.put(&mut w),
        Request::DescribeConfigs(r) => r.put(&mut w),
        Request::AlterConfigs(r) => r.put(&mut w),
    }
    w.finish()
}

/// Decodes a request. A header this node does not speak gives
/// [`DecodeError::Unsupported`], so it can still answer.
pub fn decode_request(frame: Bytes) -> Result<(RequestHeader, Request), DecodeError> {
    let mut r = Reader::new(frame);
    let header =
        RequestHeader { api_key: ApiKey(r.u16()?), api_version: r.u16()?, correlation_id: r.u32()?, flags: r.u16()? };
    r.tags()?;
    let supported =
        ApiKey::SUPPORTED.iter().any(|v| v.api_key == header.api_key && (v.min..=v.max).contains(&header.api_version));
    if !supported {
        return Err(DecodeError::Unsupported(header));
    }
    let request = match header.api_key {
        ApiKey::API_VERSIONS => {
            r.tags()?;
            Request::ApiVersions
        }
        ApiKey::METADATA => Request::Metadata(MetadataRequest::get(&mut r)?),
        ApiKey::FIND_COORDINATOR => Request::FindCoordinator(FindCoordinatorRequest::get(&mut r)?),
        ApiKey::PRODUCE => Request::Produce(ProduceRequest::get(&mut r)?),
        ApiKey::FETCH => Request::Fetch(FetchRequest::get(&mut r)?),
        ApiKey::INIT_PRODUCER => Request::InitProducer(InitProducerRequest::get(&mut r)?),
        ApiKey::ADD_PARTITIONS_TO_TXN => Request::AddPartitionsToTxn(AddPartitionsToTxnRequest::get(&mut r)?),
        ApiKey::END_TXN => Request::EndTxn(EndTxnRequest::get(&mut r)?),
        ApiKey::WRITE_TXN_MARKERS => Request::WriteTxnMarkers(WriteTxnMarkersRequest::get(&mut r)?),
        ApiKey::SUBSCRIBE => Request::Subscribe(SubscribeRequest::get(&mut r)?),
        ApiKey::CREDIT => Request::Credit(CreditRequest::get(&mut r)?),
        ApiKey::ACK => Request::Ack(AckRequest::get(&mut r)?),
        ApiKey::NACK => Request::Nack(NackRequest::get(&mut r)?),
        ApiKey::UNSUBSCRIBE => Request::Unsubscribe(UnsubscribeRequest::get(&mut r)?),
        ApiKey::GROUP_HEARTBEAT => Request::GroupHeartbeat(GroupHeartbeatRequest::get(&mut r)?),
        ApiKey::COMMIT_OFFSETS => Request::CommitOffsets(CommitOffsetsRequest::get(&mut r)?),
        ApiKey::OFFSET_FETCH => Request::OffsetFetch(OffsetFetchRequest::get(&mut r)?),
        ApiKey::ADD_OFFSETS_TO_TXN => Request::AddOffsetsToTxn(AddOffsetsToTxnRequest::get(&mut r)?),
        ApiKey::CREATE_STREAM => Request::CreateStream(CreateStreamRequest::get(&mut r)?),
        ApiKey::CREATE_QUEUE => Request::CreateQueue(CreateQueueRequest::get(&mut r)?),
        ApiKey::DECLARE_EXCHANGE => Request::DeclareExchange(DeclareExchangeRequest::get(&mut r)?),
        ApiKey::BIND => Request::Bind(BindRequest::get(&mut r)?),
        ApiKey::UNBIND => Request::Unbind(BindRequest::get(&mut r)?),
        ApiKey::DELETE_RESOURCE => Request::DeleteResource(DeleteResourceRequest::get(&mut r)?),
        ApiKey::DESCRIBE_CONFIGS => Request::DescribeConfigs(DescribeConfigsRequest::get(&mut r)?),
        ApiKey::ALTER_CONFIGS => Request::AlterConfigs(AlterConfigsRequest::get(&mut r)?),
        _ => unreachable!("checked against SUPPORTED"),
    };
    r.finish()?;
    Ok((header, request))
}

/// Encodes a response to the request with `correlation_id`.
pub fn encode_response(correlation_id: u32, response: &Response) -> Bytes {
    let mut w = Writer::new();
    w.u32(correlation_id);
    w.u16(if matches!(response, Response::Error(_)) { flags::ERROR } else { 0 });
    w.no_tags();
    match response {
        Response::ApiVersions(r) => r.put(&mut w),
        Response::Metadata(r) => r.put(&mut w),
        Response::FindCoordinator(r) => r.put(&mut w),
        Response::InitProducer(r) => r.put(&mut w),
        Response::AddPartitionsToTxn(r) => r.put(&mut w),
        Response::EndTxn(r) => r.put(&mut w),
        Response::WriteTxnMarkers(r) => r.put(&mut w),
        Response::Subscribe(r) => r.put(&mut w),
        Response::Outcome(r) => r.put(&mut w),
        Response::GroupHeartbeat(r) => r.put(&mut w),
        Response::CommitOffsets(r) => r.put(&mut w),
        Response::OffsetFetch(r) => r.put(&mut w),
        Response::Create(r) => r.put(&mut w),
        Response::Admin(r) => r.put(&mut w),
        Response::DescribeConfigs(r) => r.put(&mut w),
        Response::Produce(r) => r.put(&mut w),
        Response::Fetch(r) => r.put(&mut w),
        Response::Error(code) => {
            w.u16(code.0);
            w.no_tags();
        }
    }
    w.finish()
}

/// Encodes a push frame: correlation id 0, flag [`flags::PUSH`], then the
/// API key and version and the body.
pub fn encode_push(push: &DeliverPush) -> Bytes {
    let mut w = Writer::new();
    w.u32(0);
    w.u16(flags::PUSH);
    w.no_tags();
    w.u16(ApiKey::DELIVER.0);
    w.u16(0);
    push.put(&mut w);
    w.finish()
}

/// Whether a frame from the broker is a push rather than a response.
pub fn is_push(frame: &[u8]) -> bool {
    frame.len() >= 6 && frame[..4] == [0, 0, 0, 0] && u16::from_le_bytes([frame[4], frame[5]]) & flags::PUSH != 0
}

/// Decodes a push frame.
pub fn decode_push(frame: Bytes) -> Result<DeliverPush, DecodeError> {
    let mut r = Reader::new(frame);
    let (correlation_id, flags) = (r.u32()?, r.u16()?);
    if correlation_id != 0 || flags & flags::PUSH == 0 {
        return Err(DecodeError::Malformed("not a push frame"));
    }
    r.tags()?;
    if ApiKey(r.u16()?) != ApiKey::DELIVER || r.u16()? != 0 {
        return Err(DecodeError::Malformed("unknown push"));
    }
    let push = DeliverPush::get(&mut r)?;
    r.finish()?;
    Ok(push)
}

/// The correlation id of a response frame, to find the request it answers.
pub fn response_correlation_id(frame: &[u8]) -> Result<u32, DecodeError> {
    let id = frame.get(..4).ok_or(DecodeError::Truncated)?;
    Ok(u32::from_le_bytes(id.try_into().expect("four bytes")))
}

/// Decodes a response to a request of type `api_key`.
pub fn decode_response(frame: Bytes, api_key: ApiKey) -> Result<(u32, Response), DecodeError> {
    let mut r = Reader::new(frame);
    let correlation_id = r.u32()?;
    let flags = r.u16()?;
    r.tags()?;
    if flags & flags::ERROR != 0 {
        let code = ErrorCode(r.u16()?);
        r.tags()?;
        r.finish()?;
        return Ok((correlation_id, Response::Error(code)));
    }
    let response = match api_key {
        ApiKey::API_VERSIONS => Response::ApiVersions(ApiVersionsResponse::get(&mut r)?),
        ApiKey::METADATA => Response::Metadata(MetadataResponse::get(&mut r)?),
        ApiKey::FIND_COORDINATOR => Response::FindCoordinator(FindCoordinatorResponse::get(&mut r)?),
        ApiKey::INIT_PRODUCER => Response::InitProducer(InitProducerResponse::get(&mut r)?),
        ApiKey::ADD_PARTITIONS_TO_TXN => Response::AddPartitionsToTxn(AddPartitionsToTxnResponse::get(&mut r)?),
        ApiKey::END_TXN => Response::EndTxn(EndTxnResponse::get(&mut r)?),
        ApiKey::WRITE_TXN_MARKERS => Response::WriteTxnMarkers(WriteTxnMarkersResponse::get(&mut r)?),
        ApiKey::SUBSCRIBE => Response::Subscribe(SubscribeResponse::get(&mut r)?),
        ApiKey::ACK | ApiKey::NACK | ApiKey::UNSUBSCRIBE | ApiKey::BIND | ApiKey::UNBIND => {
            Response::Outcome(OutcomeResponse::get(&mut r)?)
        }
        ApiKey::CREATE_STREAM | ApiKey::CREATE_QUEUE | ApiKey::DECLARE_EXCHANGE => {
            Response::Create(CreateResponse::get(&mut r)?)
        }
        ApiKey::DELETE_RESOURCE | ApiKey::ALTER_CONFIGS => Response::Admin(AdminResponse::get(&mut r)?),
        ApiKey::DESCRIBE_CONFIGS => Response::DescribeConfigs(DescribeConfigsResponse::get(&mut r)?),
        ApiKey::GROUP_HEARTBEAT => Response::GroupHeartbeat(GroupHeartbeatResponse::get(&mut r)?),
        ApiKey::COMMIT_OFFSETS | ApiKey::ADD_OFFSETS_TO_TXN => {
            Response::CommitOffsets(CommitOffsetsResponse::get(&mut r)?)
        }
        ApiKey::OFFSET_FETCH => Response::OffsetFetch(OffsetFetchResponse::get(&mut r)?),
        ApiKey::PRODUCE => Response::Produce(ProduceResponse::get(&mut r)?),
        ApiKey::FETCH => Response::Fetch(FetchResponse::get(&mut r)?),
        _ => return Err(DecodeError::Malformed("unknown API key")),
    };
    r.finish()?;
    Ok((correlation_id, response))
}

#[cfg(test)]
mod tests;

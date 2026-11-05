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
#[cfg(any(test, feature = "samples"))]
pub mod samples;

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
    /// One node's metrics (admin).
    pub const DESCRIBE_METRICS: ApiKey = ApiKey(29);
    /// Chooses a SASL mechanism.
    pub const SASL_HANDSHAKE: ApiKey = ApiKey(30);
    /// One step of SASL authentication.
    pub const SASL_AUTHENTICATE: ApiKey = ApiKey(31);
    /// Sets or removes a user's SCRAM credential (admin).
    pub const ALTER_USER: ApiKey = ApiKey(32);
    /// Lists the users with a SCRAM credential (admin).
    pub const DESCRIBE_USERS: ApiKey = ApiKey(33);
    /// Creates a tenant (cluster admin).
    pub const CREATE_TENANT: ApiKey = ApiKey(34);
    /// Deletes an empty tenant (cluster admin).
    pub const DELETE_TENANT: ApiKey = ApiKey(35);
    /// Lists tenants.
    pub const DESCRIBE_TENANTS: ApiKey = ApiKey(36);
    /// Adds and removes role bindings (cluster admin).
    pub const ALTER_ROLE_BINDINGS: ApiKey = ApiKey(37);
    /// Lists role bindings (cluster admin).
    pub const DESCRIBE_ROLE_BINDINGS: ApiKey = ApiKey(38);
    /// Who the connection acts as.
    pub const WHO_AM_I: ApiKey = ApiKey(39);
    /// Adds a node to the cluster (cluster admin).
    pub const ADD_NODE: ApiKey = ApiKey(40);
    /// Drains a node and then removes it (cluster admin).
    pub const REMOVE_NODE: ApiKey = ApiKey(41);
    /// Lists the cluster's nodes (cluster viewers).
    pub const DESCRIBE_NODES: ApiKey = ApiKey(42);
    /// A group's leader reports the voters in place (internal, node to node).
    pub const SETTLE_VOTERS: ApiKey = ApiKey(43);
    /// A queue's leader forwards messages to its dead-letter queue (internal,
    /// node to node).
    pub const DEAD_LETTER: ApiKey = ApiKey(44);
    /// A partition's leader asks the transaction coordinator whether a
    /// transaction it gets a first write for is open and holds the partition
    /// (internal, node to node).
    pub const VERIFY_TXN: ApiKey = ApiKey(45);
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
    pub const SUPPORTED: [ApiVersionRange; 43] = [
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
        ApiVersionRange { api_key: ApiKey::DESCRIBE_METRICS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::SASL_HANDSHAKE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::SASL_AUTHENTICATE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ALTER_USER, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DESCRIBE_USERS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::CREATE_TENANT, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DELETE_TENANT, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DESCRIBE_TENANTS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ALTER_ROLE_BINDINGS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DESCRIBE_ROLE_BINDINGS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::WHO_AM_I, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::ADD_NODE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::REMOVE_NODE, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DESCRIBE_NODES, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::SETTLE_VOTERS, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::DEAD_LETTER, min: 0, max: 0 },
        ApiVersionRange { api_key: ApiKey::VERIFY_TXN, min: 0, max: 0 },
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

/// An error code; every partition in a response carries its own. The
/// default is [`ErrorCode::NONE`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
    /// The credentials or token were not accepted. The node says no more
    /// than that, so names cannot be probed.
    pub const AUTHENTICATION_FAILED: ErrorCode = ErrorCode(27);
    /// The SASL mechanism is not enabled on this node; the answer lists the
    /// ones that are.
    pub const UNSUPPORTED_SASL_MECHANISM: ErrorCode = ErrorCode(28);

    /// The name used in documentation and logs.
    pub fn name(self) -> &'static str {
        const NAMES: [&str; 29] = [
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
            "AUTHENTICATION_FAILED",
            "UNSUPPORTED_SASL_MECHANISM",
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

/// Tag of the reason in a frame-level error ([`Response::Refused`]).
const TAG_REASON: u64 = 0;

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
    /// [`ApiKey::DESCRIBE_METRICS`].
    DescribeMetrics(DescribeMetricsRequest),
    /// [`ApiKey::SASL_HANDSHAKE`].
    SaslHandshake(SaslHandshakeRequest),
    /// [`ApiKey::SASL_AUTHENTICATE`].
    SaslAuthenticate(SaslAuthenticateRequest),
    /// [`ApiKey::ALTER_USER`].
    AlterUser(AlterUserRequest),
    /// [`ApiKey::DESCRIBE_USERS`].
    DescribeUsers(DescribeUsersRequest),
    /// [`ApiKey::CREATE_TENANT`].
    CreateTenant(CreateTenantRequest),
    /// [`ApiKey::DELETE_TENANT`].
    DeleteTenant(DeleteTenantRequest),
    /// [`ApiKey::DESCRIBE_TENANTS`].
    DescribeTenants(DescribeTenantsRequest),
    /// [`ApiKey::ALTER_ROLE_BINDINGS`].
    AlterRoleBindings(AlterRoleBindingsRequest),
    /// [`ApiKey::DESCRIBE_ROLE_BINDINGS`].
    DescribeRoleBindings(DescribeRoleBindingsRequest),
    /// [`ApiKey::WHO_AM_I`].
    WhoAmI(WhoAmIRequest),
    /// [`ApiKey::ADD_NODE`].
    AddNode(AddNodeRequest),
    /// [`ApiKey::REMOVE_NODE`].
    RemoveNode(RemoveNodeRequest),
    /// [`ApiKey::DESCRIBE_NODES`].
    DescribeNodes(DescribeNodesRequest),
    /// [`ApiKey::SETTLE_VOTERS`].
    SettleVoters(SettleVotersRequest),
    /// [`ApiKey::DEAD_LETTER`].
    DeadLetter(DeadLetterRequest),
    /// [`ApiKey::VERIFY_TXN`].
    VerifyTxn(VerifyTxnRequest),
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
            Request::DescribeMetrics(_) => ApiKey::DESCRIBE_METRICS,
            Request::SaslHandshake(_) => ApiKey::SASL_HANDSHAKE,
            Request::SaslAuthenticate(_) => ApiKey::SASL_AUTHENTICATE,
            Request::AlterUser(_) => ApiKey::ALTER_USER,
            Request::DescribeUsers(_) => ApiKey::DESCRIBE_USERS,
            Request::CreateTenant(_) => ApiKey::CREATE_TENANT,
            Request::DeleteTenant(_) => ApiKey::DELETE_TENANT,
            Request::DescribeTenants(_) => ApiKey::DESCRIBE_TENANTS,
            Request::AlterRoleBindings(_) => ApiKey::ALTER_ROLE_BINDINGS,
            Request::DescribeRoleBindings(_) => ApiKey::DESCRIBE_ROLE_BINDINGS,
            Request::WhoAmI(_) => ApiKey::WHO_AM_I,
            Request::AddNode(_) => ApiKey::ADD_NODE,
            Request::RemoveNode(_) => ApiKey::REMOVE_NODE,
            Request::DescribeNodes(_) => ApiKey::DESCRIBE_NODES,
            Request::SettleVoters(_) => ApiKey::SETTLE_VOTERS,
            Request::DeadLetter(_) => ApiKey::DEAD_LETTER,
            Request::VerifyTxn(_) => ApiKey::VERIFY_TXN,
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
    /// To [`Request::DescribeMetrics`].
    DescribeMetrics(Box<DescribeMetricsResponse>),
    /// To [`Request::SaslHandshake`].
    SaslHandshake(SaslHandshakeResponse),
    /// To [`Request::SaslAuthenticate`].
    SaslAuthenticate(SaslAuthenticateResponse),
    /// To [`Request::DescribeUsers`].
    DescribeUsers(DescribeUsersResponse),
    /// To [`Request::DescribeTenants`].
    DescribeTenants(DescribeTenantsResponse),
    /// To [`Request::DescribeRoleBindings`].
    DescribeRoleBindings(DescribeRoleBindingsResponse),
    /// To [`Request::WhoAmI`].
    WhoAmI(WhoAmIResponse),
    /// To [`Request::DescribeNodes`].
    DescribeNodes(DescribeNodesResponse),
    /// To [`Request::Produce`].
    Produce(ProduceResponse),
    /// To [`Request::Fetch`].
    Fetch(FetchResponse),
    /// The request could not be served at all: an unsupported version
    /// ([`ErrorCode::UNSUPPORTED_VERSION`]), or a connection not
    /// authenticated or whose session ended ([`ErrorCode::UNAUTHORIZED`]).
    Error(ErrorCode),
    /// The request was refused as a whole, with a reason: the principal
    /// lacks a role for it ([`ErrorCode::UNAUTHORIZED`]). Encoded as
    /// [`Response::Error`] with the reason in a tagged field.
    Refused {
        /// Why, as a code.
        code: ErrorCode,
        /// Why, for people.
        message: String,
    },
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
        Request::DescribeMetrics(r) => r.put(&mut w),
        Request::SaslHandshake(r) => r.put(&mut w),
        Request::SaslAuthenticate(r) => r.put(&mut w),
        Request::AlterUser(r) => r.put(&mut w),
        Request::DescribeUsers(r) => r.put(&mut w),
        Request::CreateTenant(r) => r.put(&mut w),
        Request::DeleteTenant(r) => r.put(&mut w),
        Request::DescribeTenants(r) => r.put(&mut w),
        Request::AlterRoleBindings(r) => r.put(&mut w),
        Request::DescribeRoleBindings(r) => r.put(&mut w),
        Request::WhoAmI(r) => r.put(&mut w),
        Request::AddNode(r) => r.put(&mut w),
        Request::RemoveNode(r) => r.put(&mut w),
        Request::DescribeNodes(r) => r.put(&mut w),
        Request::SettleVoters(r) => r.put(&mut w),
        Request::DeadLetter(r) => r.put(&mut w),
        Request::VerifyTxn(r) => r.put(&mut w),
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
        ApiKey::DESCRIBE_METRICS => Request::DescribeMetrics(DescribeMetricsRequest::get(&mut r)?),
        ApiKey::SASL_HANDSHAKE => Request::SaslHandshake(SaslHandshakeRequest::get(&mut r)?),
        ApiKey::SASL_AUTHENTICATE => Request::SaslAuthenticate(SaslAuthenticateRequest::get(&mut r)?),
        ApiKey::ALTER_USER => Request::AlterUser(AlterUserRequest::get(&mut r)?),
        ApiKey::DESCRIBE_USERS => Request::DescribeUsers(DescribeUsersRequest::get(&mut r)?),
        ApiKey::CREATE_TENANT => Request::CreateTenant(CreateTenantRequest::get(&mut r)?),
        ApiKey::DELETE_TENANT => Request::DeleteTenant(DeleteTenantRequest::get(&mut r)?),
        ApiKey::DESCRIBE_TENANTS => Request::DescribeTenants(DescribeTenantsRequest::get(&mut r)?),
        ApiKey::ALTER_ROLE_BINDINGS => Request::AlterRoleBindings(AlterRoleBindingsRequest::get(&mut r)?),
        ApiKey::DESCRIBE_ROLE_BINDINGS => Request::DescribeRoleBindings(DescribeRoleBindingsRequest::get(&mut r)?),
        ApiKey::WHO_AM_I => Request::WhoAmI(WhoAmIRequest::get(&mut r)?),
        ApiKey::ADD_NODE => Request::AddNode(AddNodeRequest::get(&mut r)?),
        ApiKey::REMOVE_NODE => Request::RemoveNode(RemoveNodeRequest::get(&mut r)?),
        ApiKey::DESCRIBE_NODES => Request::DescribeNodes(DescribeNodesRequest::get(&mut r)?),
        ApiKey::SETTLE_VOTERS => Request::SettleVoters(SettleVotersRequest::get(&mut r)?),
        ApiKey::DEAD_LETTER => Request::DeadLetter(DeadLetterRequest::get(&mut r)?),
        ApiKey::VERIFY_TXN => Request::VerifyTxn(VerifyTxnRequest::get(&mut r)?),
        _ => unreachable!("checked against SUPPORTED"),
    };
    r.finish()?;
    Ok((header, request))
}

/// Encodes a response to the request with `correlation_id`.
pub fn encode_response(correlation_id: u32, response: &Response) -> Bytes {
    let mut w = Writer::new();
    w.u32(correlation_id);
    w.u16(if matches!(response, Response::Error(_) | Response::Refused { .. }) { flags::ERROR } else { 0 });
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
        Response::DescribeMetrics(r) => r.put(&mut w),
        Response::SaslHandshake(r) => r.put(&mut w),
        Response::SaslAuthenticate(r) => r.put(&mut w),
        Response::DescribeUsers(r) => r.put(&mut w),
        Response::DescribeTenants(r) => r.put(&mut w),
        Response::DescribeRoleBindings(r) => r.put(&mut w),
        Response::WhoAmI(r) => r.put(&mut w),
        Response::DescribeNodes(r) => r.put(&mut w),
        Response::Produce(r) => r.put(&mut w),
        Response::Fetch(r) => r.put(&mut w),
        Response::Error(code) => {
            w.u16(code.0);
            w.no_tags();
        }
        Response::Refused { code, message } => {
            w.u16(code.0);
            w.tags(&[(TAG_REASON, Bytes::copy_from_slice(message.as_bytes()))]);
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
        let reason = r.tags()?.into_iter().find(|(t, _)| *t == TAG_REASON).map(|(_, v)| v);
        r.finish()?;
        let response = match reason {
            Some(m) => Response::Refused {
                code,
                message: String::from_utf8(m.to_vec()).map_err(|_| DecodeError::Malformed("reason is not UTF-8"))?,
            },
            None => Response::Error(code),
        };
        return Ok((correlation_id, response));
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
        ApiKey::ACK
        | ApiKey::NACK
        | ApiKey::UNSUBSCRIBE
        | ApiKey::BIND
        | ApiKey::UNBIND
        | ApiKey::DEAD_LETTER
        | ApiKey::VERIFY_TXN => Response::Outcome(OutcomeResponse::get(&mut r)?),
        ApiKey::CREATE_STREAM | ApiKey::CREATE_QUEUE | ApiKey::DECLARE_EXCHANGE => {
            Response::Create(CreateResponse::get(&mut r)?)
        }
        ApiKey::DELETE_RESOURCE
        | ApiKey::ALTER_CONFIGS
        | ApiKey::ALTER_USER
        | ApiKey::CREATE_TENANT
        | ApiKey::DELETE_TENANT
        | ApiKey::ALTER_ROLE_BINDINGS
        | ApiKey::ADD_NODE
        | ApiKey::REMOVE_NODE
        | ApiKey::SETTLE_VOTERS => Response::Admin(AdminResponse::get(&mut r)?),
        ApiKey::DESCRIBE_NODES => Response::DescribeNodes(DescribeNodesResponse::get(&mut r)?),
        ApiKey::DESCRIBE_CONFIGS => Response::DescribeConfigs(DescribeConfigsResponse::get(&mut r)?),
        ApiKey::DESCRIBE_METRICS => Response::DescribeMetrics(Box::new(DescribeMetricsResponse::get(&mut r)?)),
        ApiKey::SASL_HANDSHAKE => Response::SaslHandshake(SaslHandshakeResponse::get(&mut r)?),
        ApiKey::SASL_AUTHENTICATE => Response::SaslAuthenticate(SaslAuthenticateResponse::get(&mut r)?),
        ApiKey::DESCRIBE_USERS => Response::DescribeUsers(DescribeUsersResponse::get(&mut r)?),
        ApiKey::DESCRIBE_TENANTS => Response::DescribeTenants(DescribeTenantsResponse::get(&mut r)?),
        ApiKey::DESCRIBE_ROLE_BINDINGS => Response::DescribeRoleBindings(DescribeRoleBindingsResponse::get(&mut r)?),
        ApiKey::WHO_AM_I => Response::WhoAmI(WhoAmIResponse::get(&mut r)?),
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

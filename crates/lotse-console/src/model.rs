//! Everything the console API returns or accepts.
//!
//! Numbers are raw (bytes, bytes/s, milliseconds, counts); the frontend
//! formats them. A value the cluster does not report (yet) is `null`, and
//! the UI says so instead of showing a number: a rate needs two samples,
//! and a real cluster has no tenants, zones or disk limits yet. Every type derives `ToSchema`, so the OpenAPI document and
//! the generated TypeScript client always match these definitions.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Health of something, as the UI colours it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Tone {
    /// Healthy, committed, in sync.
    Ok,
    /// Throttled, catching up, near a limit.
    Warn,
    /// Error, hard limit, writes rejected.
    Critical,
    /// Neutral information.
    Info,
}

/// The signed-in user, as the cluster sees them.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Me {
    /// Principal name the cluster authorises.
    pub principal: String,
    /// Name to show.
    pub display_name: String,
    /// `dev` or `oidc`.
    pub auth_mode: String,
}

/// A cluster this console manages.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ClusterRef {
    /// Cluster ID from the console config.
    pub id: String,
    /// Number of broker nodes.
    pub nodes: u32,
    /// One-line status, e.g. "upgrading to 0.4.1".
    pub status: String,
    /// Colour of the status.
    pub tone: Tone,
    /// `true` when served by the built-in demo cluster.
    pub demo: bool,
    /// Whether the cluster has tenants (quotas, role bindings).
    pub tenants: bool,
    /// Whether queues can have priorities, single active consumer, a TTL
    /// and dead-letter queues.
    pub queue_options: bool,
}

/// A banner that needs attention.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Alert {
    /// Stable ID, for dismissing.
    pub id: String,
    /// Severity.
    pub tone: Tone,
    /// Short headline.
    pub title: String,
    /// What happened and what to do.
    pub detail: String,
    /// Node the alert is about, if any.
    pub node: Option<String>,
}

/// Headline numbers for the overview.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Throughput {
    /// Produce and publish, bytes/s.
    pub in_bytes_per_sec: Option<f64>,
    /// Fetch and deliver, bytes/s.
    pub out_bytes_per_sec: Option<f64>,
    /// Records in per second.
    pub records_per_sec: Option<f64>,
    /// p99 time until a produce is acknowledged by a quorum, ms; `null`
    /// without produces to measure.
    pub commit_p99_ms: Option<f64>,
    /// Bytes stored, all replicas.
    pub stored_bytes: u64,
    /// Connected consumers.
    pub consumers: u32,
}

/// One line of the health list.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthItem {
    /// What is measured.
    pub label: String,
    /// The value, already in words ("1,284").
    pub value: String,
    /// Extra context.
    pub detail: String,
    /// Status chip, if the item has one.
    pub chip: Option<Chip>,
}

/// A status chip: colour plus word, never colour alone.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Chip {
    /// Colour.
    pub tone: Tone,
    /// Word shown next to the colour.
    pub text: String,
}

/// A broker node.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Node {
    /// Node ID.
    pub id: String,
    /// "Metadata leader", "Voter", …
    pub role: String,
    /// Availability zone.
    pub zone: Option<String>,
    /// Uptime in seconds.
    pub uptime_secs: u64,
    /// Raft groups this node leads.
    pub leads: u32,
    /// Raft groups with a replica on this node.
    pub replicas: u32,
    /// Free bytes on the log volume.
    pub disk_free_bytes: Option<u64>,
    /// Size of the log volume.
    pub disk_total_bytes: Option<u64>,
    /// Client produce traffic into this node, bytes/s.
    pub produce_bytes_per_sec: Option<f64>,
    /// Network in, bytes/s (clients and replication).
    pub net_in_bytes_per_sec: Option<f64>,
    /// Network out, bytes/s.
    pub net_out_bytes_per_sec: Option<f64>,
    /// Open client connections.
    pub connections: u32,
    /// CPU use, 0–1.
    pub cpu: Option<f64>,
    /// Resident memory, bytes.
    pub memory_bytes: Option<u64>,
    /// Memory limit, bytes.
    pub memory_limit_bytes: Option<u64>,
    /// Running version.
    pub version: String,
    /// Status chip.
    pub status: Chip,
}

/// Disk thresholds, as fractions of free space.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct DiskLimits {
    /// Producers are throttled below this free fraction.
    pub soft_free: f64,
    /// Writes are rejected below this free fraction.
    pub hard_free: f64,
}

/// A row in "busiest streams".
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StreamRow {
    /// Stream name.
    pub name: String,
    /// Tenant.
    pub tenant: Option<String>,
    /// Partition count.
    pub partitions: u32,
    /// Produce rate, bytes/s.
    pub in_bytes_per_sec: Option<f64>,
    /// Stored bytes, all replicas.
    pub stored_bytes: u64,
}

/// A row in queue lists.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueueRow {
    /// Queue name.
    pub name: String,
    /// Tenant.
    pub tenant: Option<String>,
    /// Messages ready for delivery.
    pub ready: u64,
    /// Messages in the dead-letter queue.
    pub dead_lettered: Option<u64>,
    /// Why the queue needs attention, if it does.
    pub attention: Option<Chip>,
}

/// The cluster overview screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Overview {
    /// Cluster ID.
    pub cluster: String,
    /// Raft groups in the cluster.
    pub raft_groups: u32,
    /// Node that leads the metadata group.
    pub metadata_leader: String,
    /// Term of the metadata group.
    pub metadata_term: u64,
    /// Banners.
    pub alerts: Vec<Alert>,
    /// Headline numbers.
    pub throughput: Throughput,
    /// Health list.
    pub health: Vec<HealthItem>,
    /// Nodes.
    pub nodes: Vec<Node>,
    /// Disk thresholds for the bars, if the cluster has them.
    pub disk_limits: Option<DiskLimits>,
    /// Busiest streams by produce rate.
    pub busiest_streams: Vec<StreamRow>,
    /// Queues that need attention.
    pub attention_queues: Vec<QueueRow>,
    /// Total streams.
    pub stream_count: u32,
    /// Total queues.
    pub queue_count: u32,
}

/// One node's progress through a rolling upgrade.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UpgradeStep {
    /// Node ID.
    pub node: String,
    /// Status chip.
    pub status: Chip,
    /// Phases in order, with their state.
    pub phases: Vec<UpgradePhase>,
    /// Explanation.
    pub note: String,
}

/// A phase of upgrading one node.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UpgradePhase {
    /// What happens.
    pub name: String,
    /// `done`, `running` or `pending`.
    pub state: PhaseState,
}

/// State of an upgrade phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PhaseState {
    /// Finished.
    Done,
    /// In progress.
    Running,
    /// Not started.
    Pending,
}

/// A rolling upgrade run by the operator.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Upgrade {
    /// Version before.
    pub from: String,
    /// Version after.
    pub to: String,
    /// Who started it.
    pub started_by: String,
    /// Start, RFC 3339.
    pub started_at: String,
    /// Nodes done.
    pub nodes_done: u32,
    /// Nodes in total.
    pub nodes_total: u32,
    /// Raft groups still catching up on the current node.
    pub groups_catching_up: u32,
    /// Steps, one per node in upgrade order.
    pub steps: Vec<UpgradeStep>,
    /// `true` once the operator stops after the current node.
    pub paused_after_current: bool,
}

/// Something the operator did.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OperatorEvent {
    /// Time, RFC 3339.
    pub at: String,
    /// Node it concerns.
    pub node: String,
    /// What happened.
    pub text: String,
}

/// The nodes screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct NodesView {
    /// Nodes.
    pub nodes: Vec<Node>,
    /// Disk thresholds, if the cluster has them.
    pub disk_limits: Option<DiskLimits>,
    /// Running upgrade, if any.
    pub upgrade: Option<Upgrade>,
    /// Recent operator events, newest first.
    pub events: Vec<OperatorEvent>,
}

/// Role of a replica in a Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    /// Leads the group.
    Leader,
    /// In sync with the leader.
    InSync,
    /// Behind the leader.
    CatchingUp,
}

/// A replica of a partition.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Replica {
    /// Node ID.
    pub node: String,
    /// State.
    pub state: ReplicaState,
    /// Entries behind the leader.
    pub behind: u64,
}

/// A stream partition.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Partition {
    /// Partition number.
    pub id: u32,
    /// Leader node.
    pub leader: String,
    /// Raft term.
    pub term: u64,
    /// Replicas.
    pub replicas: Vec<Replica>,
    /// First offset still stored.
    pub log_start: u64,
    /// Committed by a quorum.
    pub high_watermark: u64,
    /// Below the first open transaction; `read_committed` stops here.
    pub last_stable: u64,
    /// Bytes on one replica.
    pub size_bytes: u64,
}

/// A consumer group reading a stream.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Group {
    /// Group ID.
    pub id: String,
    /// Members.
    pub members: u32,
    /// State chip.
    pub state: Chip,
    /// Assignment epoch.
    pub epoch: u32,
    /// Records behind, summed over partitions.
    pub lag: u64,
    /// Lag samples over the last hour, oldest first.
    pub lag_trend: Vec<u64>,
}

/// Who owns which partition in a group.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Assignment {
    /// Group ID.
    pub group: String,
    /// Coordinator partition and node.
    pub coordinator: String,
    /// One entry per member.
    pub members: Vec<AssignedMember>,
    /// Explanation of a handover in progress.
    pub note: Option<String>,
}

/// A group member's partitions.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AssignedMember {
    /// Member ID and host.
    pub id: String,
    /// Partitions it owns.
    pub owned: Vec<u32>,
    /// Partitions on their way to it.
    pub incoming: Vec<u32>,
}

/// How long and how much a stream keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
pub struct StreamRetention {
    /// Records older than this many milliseconds go; `None`: no limit by age.
    pub ms: Option<u64>,
    /// Each partition keeps about this many bytes; `None`: no limit by size.
    pub bytes: Option<u64>,
}

/// The stream detail screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StreamDetail {
    /// Stream name.
    pub name: String,
    /// Tenant.
    pub tenant: Option<String>,
    /// Replication factor.
    pub replication: u32,
    /// What the stream keeps; `None` when the cluster did not say.
    pub retention: Option<StreamRetention>,
    /// Log compaction on.
    pub compaction: bool,
    /// Produce rate, bytes/s.
    pub in_bytes_per_sec: Option<f64>,
    /// Fetch rate, bytes/s.
    pub out_bytes_per_sec: Option<f64>,
    /// Records in per second.
    pub records_per_sec: Option<f64>,
    /// Bytes stored, all replicas.
    pub stored_bytes: u64,
    /// Age of the oldest record, ms.
    pub oldest_record_age_ms: Option<u64>,
    /// Open transactions touching this stream.
    pub open_transactions: Option<u32>,
    /// Partitions.
    pub partitions: Vec<Partition>,
    /// Consumer groups.
    pub groups: Vec<Group>,
    /// Assignment of the group the UI highlights.
    pub assignment: Option<Assignment>,
}

/// A queue priority level (ADR 0078: levels share the queue's one log).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct PriorityLevel {
    /// Level number; higher is delivered first.
    pub level: u8,
    /// Leader of the queue the level belongs to.
    pub leader: String,
    /// Messages ready.
    pub ready: u64,
    /// Age of the oldest ready message, ms.
    pub oldest_ms: Option<u64>,
}

/// A queue consumer.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueueConsumer {
    /// Consumer tag.
    pub tag: String,
    /// Principal; `null` while the cluster has no authentication.
    pub principal: Option<String>,
    /// Client address and broker.
    pub connection: String,
    /// Messages in flight.
    pub in_flight: u32,
    /// Credit window, messages.
    pub credit_messages: u32,
    /// Bytes in flight.
    pub in_flight_bytes: Option<u64>,
    /// Credit window, bytes.
    pub credit_bytes: Option<u64>,
    /// Acks per second.
    pub acks_per_sec: Option<f64>,
    /// State chip ("Active", "Standby").
    pub state: Chip,
}

/// A dead-lettered message.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DeadLetter {
    /// Offset in the dead-letter queue.
    pub offset: u64,
    /// Message key.
    pub key: Option<String>,
    /// Why it was dead-lettered.
    pub reason: String,
    /// Deliveries before it was dead-lettered.
    pub deliveries: u32,
    /// Time, RFC 3339.
    pub at: String,
}

/// An entry in the single-active-consumer handover log.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HandoverEvent {
    /// Time, RFC 3339.
    pub at: String,
    /// What happened.
    pub text: String,
}

/// The queue detail screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct QueueDetail {
    /// Queue name.
    pub name: String,
    /// Tenant.
    pub tenant: Option<String>,
    /// Single active consumer.
    pub single_active: bool,
    /// Message TTL, ms; `None` = until acked.
    pub ttl_ms: Option<u64>,
    /// Dead-letter queue.
    pub dlq: Option<String>,
    /// Deliveries before dead-lettering.
    pub max_deliveries: Option<u32>,
    /// Exchange and binding key, if bound.
    pub binding: Option<String>,
    /// Ready, all levels.
    pub ready: u64,
    /// Delivered and not acked.
    pub in_flight: u64,
    /// Acks waiting for their transaction to commit.
    pub in_open_txns: u64,
    /// Publish rate, messages/s.
    pub publish_per_sec: Option<f64>,
    /// Ack rate, messages/s.
    pub ack_per_sec: Option<f64>,
    /// Dead-lettered.
    pub dead_lettered: Option<u64>,
    /// Priority levels, highest first.
    pub levels: Vec<PriorityLevel>,
    /// Consumers.
    pub consumers: Vec<QueueConsumer>,
    /// Latest dead letters.
    pub dead_letters: Vec<DeadLetter>,
    /// Last single-active-consumer handover.
    pub handover: Vec<HandoverEvent>,
}

/// Request to create a queue.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateQueue {
    /// Name: lowercase letters, digits, dots and hyphens.
    pub name: String,
    /// Tenant; required where the cluster has tenants, absent elsewhere.
    pub tenant: Option<String>,
    /// 1 to 10.
    pub priority_levels: u8,
    /// Single active consumer.
    pub single_active: bool,
    /// Message TTL, ms; `None` = until acked.
    pub ttl_ms: Option<u64>,
    /// Dead-letter queue name.
    pub dlq: Option<String>,
    /// Deliveries before dead-lettering.
    pub max_deliveries: Option<u32>,
    /// Create the DLQ if missing.
    pub create_dlq: bool,
}

/// Request to purge a queue's ready messages (ADR 0064). Destructive, so
/// the queue's name must be typed again.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PurgeQueue {
    /// The queue's name, typed by the user.
    pub confirm: String,
}

/// Request to pin a partition's leader to one of its replicas, or to unpin
/// it (ADR 0065).
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct MoveLeader {
    /// The stream or queue.
    pub resource: String,
    /// The partition; 0 for a queue.
    pub partition: u32,
    /// The node to lead it, as the console names nodes; `None` unpins it so
    /// that leaders spread by themselves again.
    pub node: Option<String>,
}

/// Result of a write, with the CLI command that does the same.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Done {
    /// What was done.
    pub message: String,
    /// The equivalent `lotse-cli` command.
    pub cli: String,
}

/// Transaction state of a record as a reader sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TxnState {
    /// Not transactional.
    None,
    /// Part of a committed transaction.
    Committed,
    /// Part of an aborted transaction.
    Aborted,
    /// Part of a transaction not yet ended.
    Open,
    /// A COMMIT marker.
    CommitMarker,
    /// An ABORT marker.
    AbortMarker,
}

/// Isolation level for reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Isolation {
    /// Everything, including open and aborted transactions.
    Uncommitted,
    /// Only up to the last stable offset, without aborted data.
    Committed,
}

/// A record header.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecordHeader {
    /// Name.
    pub key: String,
    /// Value as UTF-8, if it is.
    pub value: Option<String>,
}

/// A record read through the browser.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecordView {
    /// Offset.
    pub offset: u64,
    /// Timestamp, RFC 3339 with milliseconds.
    pub timestamp: String,
    /// Key as UTF-8.
    pub key: Option<String>,
    /// Value as UTF-8, if it is.
    pub value: Option<String>,
    /// Value size in bytes.
    pub size_bytes: u32,
    /// Headers.
    pub headers: Vec<RecordHeader>,
    /// Transaction state.
    pub txn: TxnState,
    /// Producer ID, 0 if not idempotent.
    pub producer_id: u64,
    /// Producer epoch.
    pub producer_epoch: u16,
    /// Sequence of this record.
    pub sequence: i32,
    /// First offset of its batch.
    pub batch_base: u64,
    /// Last offset of its batch.
    pub batch_last: u64,
    /// Raft term of the batch.
    pub term: u64,
}

/// A page of records.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecordPage {
    /// Records in offset order.
    pub records: Vec<RecordView>,
    /// Offset to continue from.
    pub next_offset: u64,
    /// The equivalent `lotse-cli` command.
    pub cli: String,
}

/// A tenant.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Tenant {
    /// Tenant name.
    pub name: String,
    /// Bytes stored, all replicas.
    pub used_bytes: u64,
    /// Storage quota, bytes; `None` where the cluster has no quotas.
    pub quota_bytes: Option<u64>,
    /// Streams.
    pub streams: u32,
    /// Queues.
    pub queues: u32,
    /// Principals with bindings.
    pub principals: u32,
    /// State chip.
    pub state: Chip,
}

/// A role binding.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RoleBinding {
    /// Principal.
    pub principal: String,
    /// How the subject is recognised: `user` or `group` (a group at the
    /// identity provider); the demo says `mTLS` or `OIDC`.
    pub auth: String,
    /// Role.
    pub role: String,
    /// Resource patterns.
    pub resources: Vec<String>,
}

/// The tenants screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TenantsView {
    /// Tenants, most critical first.
    pub tenants: Vec<Tenant>,
    /// Role bindings per tenant.
    pub bindings: Vec<TenantBindings>,
}

/// Bindings of one tenant.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct TenantBindings {
    /// Tenant.
    pub tenant: String,
    /// Bindings.
    pub bindings: Vec<RoleBinding>,
}

/// A SCRAM user. Key material never leaves the cluster, so a row says only
/// how the user is stored.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UserRow {
    /// User name, the principal in role bindings.
    pub name: String,
    /// `SCRAM-SHA-512`.
    pub mechanism: String,
    /// PBKDF2 iterations of the stored credential.
    pub iterations: u32,
    /// `configuration` (read only) or `runtime`.
    pub source: String,
    /// Bindings that name this user, as `role (tenant)` texts.
    pub roles: Vec<String>,
}

/// The users screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UsersView {
    /// SCRAM users, by name.
    pub users: Vec<UserRow>,
}

/// A SCRAM credential derived in the browser (ADR 0089). There is no
/// password field: the console never sees one.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct Credential {
    /// Salt, base64.
    pub salt: String,
    /// PBKDF2 iterations; the broker refuses fewer than its minimum.
    pub iterations: u32,
    /// `H(ClientKey)`, base64.
    pub stored_key: String,
    /// `ServerKey`, base64.
    pub server_key: String,
}

/// Request to create a user with a credential.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct CreateUser {
    /// New user name.
    pub name: String,
    /// The credential.
    #[serde(flatten)]
    pub credential: Credential,
}

/// Request to delete a user. Destructive: names the user again.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct DeleteUser {
    /// The user's name, typed again.
    pub confirm: String,
}

/// A role binding with its tenant, prefix and where it comes from.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BindingRow {
    /// `user` or `group` (a group at the identity provider).
    pub subject_kind: String,
    /// User name or group name.
    pub subject: String,
    /// `cluster-admin`, `cluster-viewer`, `tenant-admin`, `producer`,
    /// `consumer` or `viewer`.
    pub role: String,
    /// Tenant; empty for the two cluster-wide roles.
    pub tenant: String,
    /// Name prefix the binding covers; empty means every name.
    pub prefix: String,
    /// `configuration` (read only) or `runtime`. Ignored in requests.
    #[serde(default)]
    pub source: String,
}

/// One client certificate the cluster has seen or had revoked (ADR 0091).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CertificateRow {
    /// SHA-256 of the issuer's name in DER form, 64 hex digits.
    pub issuer: String,
    /// The serial number, hex.
    pub serial: String,
    /// The principal it names; empty for one revoked before any node saw it.
    pub principal: String,
    /// First millisecond it is valid in, since the Unix epoch; 0 if unknown.
    pub not_before_ms: u64,
    /// Millisecond it stops being valid in, since the Unix epoch.
    pub not_after_ms: u64,
    /// When a node first accepted it; 0 if none did.
    pub first_seen_ms: u64,
    /// When a node last accepted it; connections that stay open are noted
    /// again every ten minutes and the cluster records it at most hourly.
    pub last_seen_ms: u64,
    /// `valid`, `expiring` (within 30 days), `expired` or `revoked`.
    pub state: String,
    /// Why it was revoked; empty if it was not.
    pub reason: String,
    /// When it was revoked; 0 if it was not.
    pub revoked_at_ms: u64,
}

/// The certificates screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CertificatesView {
    /// By expiry.
    pub certificates: Vec<CertificateRow>,
    /// The cluster's clock when the list was made, ms since the Unix epoch.
    pub now_ms: u64,
}

/// Request to revoke one client certificate (ADR 0090). Destructive: the
/// serial is typed again.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RevokeCertificate {
    /// SHA-256 of the issuer's name in DER form, 64 hex digits.
    pub issuer: String,
    /// The serial number, hex.
    pub serial: String,
    /// When the certificate stops being valid, ms since the Unix epoch. The
    /// revocation is forgotten after that: an expired certificate is refused
    /// anyway.
    pub not_after_ms: u64,
    /// Why; may be empty.
    #[serde(default)]
    pub reason: String,
    /// The serial, typed again.
    pub confirm: String,
}

/// One request for a client certificate (ADR 0092).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CertificateRequestRow {
    /// SHA-256 of the signing request, 64 hex digits.
    pub id: String,
    /// The principal the certificate will name.
    pub principal: String,
    /// Who asked.
    pub requested_by: String,
    /// When, ms since the Unix epoch.
    pub requested_at_ms: u64,
    /// When an unsigned request lapses, ms since the Unix epoch.
    pub expires_at_ms: u64,
    /// `pending`, `signed` or `expired`.
    pub state: String,
    /// SHA-256 of the requester's public key, 64 hex digits: what
    /// `lotse certs --sign` shows the CA holder, to compare with the key
    /// the browser made. Empty if the request cannot be read.
    pub key_fingerprint: String,
    /// The signed certificate, PEM; empty until signed.
    pub certificate: String,
    /// When it was signed; 0 if not yet.
    pub signed_at_ms: u64,
}

/// The certificate requests screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CertificateRequestsView {
    /// Newest first.
    pub requests: Vec<CertificateRequestRow>,
    /// The cluster's clock when the list was made, ms since the Unix epoch.
    pub now_ms: u64,
}

/// A request for a client certificate (ADR 0092). The browser made the key
/// and the signing request; the console never sees the private key.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RequestCertificate {
    /// The principal the certificate will name: letters, digits, `.`, `_`,
    /// `-` and `@`.
    pub principal: String,
    /// The PKCS#10 signing request, DER, base64.
    pub csr: String,
}

/// The role bindings screen.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct BindingsView {
    /// Every binding the caller may see: all of them for a cluster admin,
    /// the caller's own for everyone else.
    pub bindings: Vec<BindingRow>,
    /// Whether the caller may change bindings.
    pub can_edit: bool,
}

/// Request to revoke the sessions of a user or an IdP group (ADR 0070).
/// Destructive: names the subject again.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct RevokeSessions {
    /// User name or group name.
    pub subject: String,
    /// `true` for a group at the identity provider.
    pub group: bool,
    /// The subject's name, typed again.
    pub confirm: String,
}

/// One point of a time series.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct Point {
    /// Unix time, ms.
    pub t: u64,
    /// Value.
    pub v: f64,
}

/// A time series for charts.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Series {
    /// Series name.
    pub name: String,
    /// Unit of `v`.
    pub unit: String,
    /// Points, oldest first.
    pub points: Vec<Point>,
}

/// Error body for every failed request.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorBody {
    /// Machine-readable code, e.g. `NOT_FOUND`.
    pub code: String,
    /// What went wrong and what to do.
    pub message: String,
}

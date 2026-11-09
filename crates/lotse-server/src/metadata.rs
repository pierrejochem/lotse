//! The cluster's resources created at runtime, kept in the metadata group.
//!
//! Every admin change is one record (`Metadata`, an internal control batch)
//! applied in log order by every replica of the metadata group, so every
//! node learns the same resources. Ids come from the record's Raft index,
//! which is unique: a resource gets `DYNAMIC_BASE + index`, partition `p` of
//! a stream (or a queue, `p = 0`) the Raft group `DYNAMIC_BASE + index *
//! 4096 + p`. Configured resources use small ids, so the two never collide.
//!
//! Which nodes replicate a new group is chosen by the metadata leader when
//! it proposes the record and stored in it: applying never decides
//! anything that could differ between nodes. A record that names a resource
//! that exists already changes nothing; the leader tells the client by
//! looking at the applied state.
//!
//! Users' SCRAM credentials live here too, so every node can check a login
//! without asking anyone.

use std::collections::{BTreeMap, BTreeSet};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lotse_auth::roles::{Role, RoleBinding, Subject};
use lotse_auth::scram::ScramCredential;
use lotse_raft::NodeId;
use lotse_route::{Binding, ExchangeKind};

use crate::wire::GroupId;

/// Ids of resources and groups created at runtime start here.
pub const DYNAMIC_BASE: u64 = 1 << 40;

/// Partitions a created stream may have at most.
pub const MAX_PARTITIONS: u32 = 4096;

/// The resource id of the resource created by the record at `index`.
pub(crate) fn resource_id(index: u64) -> u64 {
    DYNAMIC_BASE + index
}

/// The Raft group of partition `p` of the resource created at `index`.
pub(crate) fn group_id(index: u64, p: u32) -> GroupId {
    DYNAMIC_BASE + index * u64::from(MAX_PARTITIONS) + u64::from(p)
}

/// A change to the cluster's resources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Change {
    /// A stream; one voter set per partition.
    CreateStream { name: String, tenant: String, partitions: Vec<Vec<NodeId>>, settings: StreamSettings },
    /// A queue and its voters; an ack timeout of 0 is the default.
    CreateQueue { name: String, tenant: String, voters: Vec<NodeId>, ack_timeout_ms: u64, limits: QueueLimits },
    /// An exchange.
    DeclareExchange { name: String, tenant: String, kind: ExchangeKind },
    /// A binding added to an exchange.
    Bind { exchange_id: u64, binding: Binding },
    /// A binding removed from an exchange.
    Unbind { exchange_id: u64, binding: Binding },
    /// A resource deleted, with its groups.
    Delete { resource_id: u64 },
    /// Some of a queue's settings set or reset: the ack timeout (0 returns
    /// it to the default) and the limits of ADR 0055 (0 switches them off).
    /// The others stay as they are when this applies, so two changes of
    /// different keys both hold.
    AlterQueue { resource_id: u64, change: QueueChange },
    /// Some of a stream's settings set or reset; the others stay as they
    /// are when this applies, so two changes of different keys both hold.
    AlterStream { resource_id: u64, change: StreamChange },
    /// A user's SCRAM credential set, or removed with `None`.
    SetUser { user: String, credential: Option<ScramCredential> },
    /// A tenant created.
    /// A tenant is created. `incarnated` (every create since ADR 0069)
    /// gives it the record's index as its incarnation; an older record's
    /// tenant keeps incarnation 0.
    CreateTenant { name: String, incarnated: bool },
    /// An empty tenant deleted, with its role bindings.
    DeleteTenant { name: String },
    /// Role bindings removed, then added.
    SetBindings { add: Vec<RoleBinding>, remove: Vec<RoleBinding> },
    /// A node joins the cluster (ADR 0048). Ids are never used twice.
    AddNode { id: NodeId, peer: String, client: String },
    /// A node is to leave: nothing new is placed on it, and its replicas
    /// move to other nodes.
    DrainNode { id: NodeId },
    /// A drained node is gone: nodes drop it, its id stays taken.
    ForgetNode { id: NodeId },
    /// The voters a group is to have; its leader changes the group's Raft
    /// membership until it has them.
    SetVoters { group: GroupId, voters: Vec<NodeId> },
    /// A group's leader reports that the group has these voters (and no
    /// one else) in its Raft membership.
    Settled { group: GroupId, voters: Vec<NodeId> },
    /// An operator pins a group's preferred leader to a node, or unpins it
    /// with `None` (ADR 0065).
    PinLeader { group: GroupId, node: Option<NodeId> },
    /// Sessions of a user or group authenticated before `before_ms`, and
    /// its tokens issued before it, no longer count (ADR 0070).
    RevokeSessions { subject: Subject, before_ms: u64 },
    /// A client certificate, by issuer and serial, no longer counts
    /// (ADR 0090). `at_ms` is the proposing leader's clock; a revocation
    /// whose certificate ended before it is dropped when this one applies.
    RevokeCertificate { cert: CertKey, not_after_ms: u64, at_ms: u64, reason: String },
    /// Nodes report client certificates they accepted (ADR 0091). `at_ms` is
    /// the proposing leader's clock; entries whose certificate ended long
    /// before it are dropped when this applies.
    RecordCertificates { seen: Vec<(CertKey, SeenCert)>, at_ms: u64 },
    /// New partitions for a runtime stream, one voter set each, numbered on
    /// from its last (ADR 0074).
    AddPartitions { resource_id: u64, partitions: Vec<Vec<NodeId>> },
}

/// A node added at runtime: where nodes and clients reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeMeta {
    pub(crate) peer: String,
    pub(crate) client: String,
}

/// A stream's own settings; `None` is the node configuration's default,
/// `Some(-1)` no limit (ADR 0046).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StreamSettings {
    /// `retention.ms`.
    pub(crate) retention_ms: Option<i64>,
    /// `retention.bytes`, per partition.
    pub(crate) retention_bytes: Option<i64>,
    /// `cleanup.policy`; `None` is [`Cleanup::Delete`] (ADR 0067).
    pub(crate) cleanup: Option<Cleanup>,
    /// `delete.retention.ms`: how long compaction keeps tombstones; `None`
    /// is [`DEFAULT_DELETE_RETENTION_MS`], -1 forever.
    pub(crate) delete_retention_ms: Option<i64>,
}

/// What a stream does with old data (ADR 0067).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cleanup {
    /// Deletes by age and size.
    Delete,
    /// Keeps the newest record of each key, and deletes nothing by age or
    /// size.
    Compact,
    /// Both.
    CompactDelete,
}

impl Cleanup {
    /// The setting's value.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Cleanup::Delete => "delete",
            Cleanup::Compact => "compact",
            Cleanup::CompactDelete => "compact,delete",
        }
    }

    /// The policy a setting's value names; the order of a pair does not
    /// matter.
    pub(crate) fn parse(v: &str) -> Option<Cleanup> {
        let mut parts: Vec<&str> = v.split(',').map(str::trim).collect();
        parts.sort_unstable();
        match parts[..] {
            ["delete"] => Some(Cleanup::Delete),
            ["compact"] => Some(Cleanup::Compact),
            ["compact", "delete"] => Some(Cleanup::CompactDelete),
            _ => None,
        }
    }

    fn code(self) -> i64 {
        match self {
            Cleanup::Delete => 0,
            Cleanup::Compact => 1,
            Cleanup::CompactDelete => 2,
        }
    }

    fn from_code(c: i64) -> Result<Cleanup, String> {
        match c {
            0 => Ok(Cleanup::Delete),
            1 => Ok(Cleanup::Compact),
            2 => Ok(Cleanup::CompactDelete),
            _ => Err(format!("unknown cleanup policy {c}")),
        }
    }
}

/// How long compaction keeps a tombstone unless a stream says otherwise:
/// a day, as in Kafka.
pub(crate) const DEFAULT_DELETE_RETENTION_MS: i64 = 86_400_000;

/// What an `AlterStream` does to each setting: `None` leaves it,
/// `Some(None)` returns it to the default, `Some(Some(v))` sets it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StreamChange {
    /// `retention.ms`.
    pub(crate) retention_ms: Option<Option<i64>>,
    /// `retention.bytes`.
    pub(crate) retention_bytes: Option<Option<i64>>,
    /// `cleanup.policy`.
    pub(crate) cleanup: Option<Option<Cleanup>>,
    /// `delete.retention.ms`.
    pub(crate) delete_retention_ms: Option<Option<i64>>,
}

impl StreamChange {
    /// `s` with this change.
    pub(crate) fn applied_to(&self, mut s: StreamSettings) -> StreamSettings {
        self.apply(&mut s);
        s
    }

    fn apply(&self, s: &mut StreamSettings) {
        if let Some(v) = self.retention_ms {
            s.retention_ms = v;
        }
        if let Some(v) = self.retention_bytes {
            s.retention_bytes = v;
        }
        if let Some(v) = self.cleanup {
            s.cleanup = v;
        }
        if let Some(v) = self.delete_retention_ms {
            s.delete_retention_ms = v;
        }
    }

    /// Whether it touches a setting that came with compaction; records
    /// without one stay readable by nodes from before it.
    fn has_compaction(&self) -> bool {
        self.cleanup.is_some() || self.delete_retention_ms.is_some()
    }
}

impl StreamSettings {
    /// What a partition may delete, with `defaults` where unset.
    pub(crate) fn retention(&self, defaults: crate::RetentionConfig) -> crate::Retention {
        let pick = |own: Option<i64>, default: Option<u64>| match own {
            None => default,
            Some(v) if v < 0 => None,
            Some(v) => Some(v as u64),
        };
        let cleanup = self.cleanup.unwrap_or(Cleanup::Delete);
        let deletes = cleanup != Cleanup::Compact;
        let compact = (cleanup != Cleanup::Delete).then(|| {
            let ms = self.delete_retention_ms.unwrap_or(DEFAULT_DELETE_RETENTION_MS);
            u64::try_from(ms).unwrap_or(u64::MAX)
        });
        crate::Retention::Stream {
            ms: pick(self.retention_ms, defaults.ms).filter(|_| deletes),
            bytes: pick(self.retention_bytes, defaults.bytes).filter(|_| deletes),
            compact,
        }
    }

    /// Whether a setting that came with compaction is set.
    fn has_compaction(&self) -> bool {
        self.cleanup.is_some() || self.delete_retention_ms.is_some()
    }
}

/// A stream created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynStream {
    pub(crate) name: String,
    pub(crate) tenant: String,
    pub(crate) resource_id: u64,
    /// Per partition: its group and voters.
    pub(crate) partitions: Vec<(GroupId, Vec<NodeId>)>,
    pub(crate) settings: StreamSettings,
}

/// What a queue does with a message it will not deliver (again) (ADR 0055);
/// 0 switches each off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct QueueLimits {
    /// `max.deliveries`.
    pub(crate) max_deliveries: u32,
    /// `message.ttl.ms`.
    pub(crate) message_ttl_ms: u64,
    /// `dead.letter.queue`: the resource id of a queue of the same tenant.
    pub(crate) dead_letter: u64,
    /// `single.active.consumer` (ADR 0076).
    pub(crate) single_active: bool,
    /// `priority.levels` (ADR 0078): 2 to 10; 0 for one level, none.
    pub(crate) priority_levels: u8,
}

/// What an `AlterQueue` does: each field is left (`None`) or set, 0
/// returning it to its default or off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct QueueChange {
    pub(crate) ack_timeout_ms: Option<u64>,
    pub(crate) max_deliveries: Option<u32>,
    pub(crate) message_ttl_ms: Option<u64>,
    pub(crate) dead_letter: Option<u64>,
    pub(crate) single_active: Option<bool>,
    /// 0 for one level.
    pub(crate) priority_levels: Option<u8>,
}

impl QueueChange {
    /// Whether the queue is as this change would leave it.
    pub(crate) fn holds_in(&self, q: &DynQueue) -> bool {
        self.ack_timeout_ms.is_none_or(|v| q.ack_timeout_ms == v) && self.applied_to(q.limits) == q.limits
    }

    /// Whether the change, decided and applied, was refused rather than
    /// applied and perhaps replaced since. Only a dead-letter queue can make
    /// `apply` refuse; any other change always applies, so a queue that no
    /// longer shows it was overwritten by a later change, which is no error.
    pub(crate) fn refused_in(&self, q: &DynQueue) -> bool {
        self.dead_letter.is_some() && !self.holds_in(q)
    }

    /// `l` with this change.
    pub(crate) fn applied_to(&self, mut l: QueueLimits) -> QueueLimits {
        l.max_deliveries = self.max_deliveries.unwrap_or(l.max_deliveries);
        l.message_ttl_ms = self.message_ttl_ms.unwrap_or(l.message_ttl_ms);
        l.dead_letter = self.dead_letter.unwrap_or(l.dead_letter);
        l.single_active = self.single_active.unwrap_or(l.single_active);
        l.priority_levels = self.priority_levels.unwrap_or(l.priority_levels);
        l
    }
}

/// A queue created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynQueue {
    pub(crate) name: String,
    pub(crate) tenant: String,
    pub(crate) resource_id: u64,
    pub(crate) group: GroupId,
    pub(crate) voters: Vec<NodeId>,
    /// 0: the default.
    pub(crate) ack_timeout_ms: u64,
    pub(crate) limits: QueueLimits,
}

/// An exchange created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynExchange {
    pub(crate) name: String,
    pub(crate) tenant: String,
    pub(crate) resource_id: u64,
    pub(crate) kind: ExchangeKind,
}

/// See the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MetadataState {
    pub(crate) streams: BTreeMap<u64, DynStream>,
    pub(crate) queues: BTreeMap<u64, DynQueue>,
    pub(crate) exchanges: BTreeMap<u64, DynExchange>,
    /// Bindings added at runtime, per exchange (configured ones included).
    pub(crate) bindings: BTreeMap<u64, Vec<Binding>>,
    /// Tombstones: deleted resources and how many groups each had.
    pub(crate) deleted: BTreeMap<u64, u32>,
    /// Users with a SCRAM credential, set at runtime.
    pub(crate) users: BTreeMap<String, ScramCredential>,
    /// Tenants created at runtime.
    pub(crate) tenants: BTreeSet<String>,
    /// Role bindings made at runtime.
    pub(crate) bindings_of_roles: BTreeSet<RoleBinding>,
    /// Tenants deleted at runtime and not created again: nothing may be
    /// created in them, even by a create proposed before the delete.
    pub(crate) deleted_tenants: BTreeSet<String>,
    /// Each runtime tenant's incarnation, live or deleted, where it is not
    /// 0: the index of the record that created it (ADR 0069). Transactional
    /// and group ids are keyed by it, so a tenant created again under a
    /// name starts without the ids of the one deleted before.
    pub(crate) incarnations: BTreeMap<String, u64>,
    /// Revoked sessions (ADR 0070): for a user or group, the time in ms
    /// since the Unix epoch before which its sessions and tokens no longer
    /// count. Only ever raised.
    pub(crate) revocations: BTreeMap<Subject, u64>,
    /// Revoked client certificates (ADR 0090), until they expire.
    pub(crate) revoked_certs: BTreeMap<CertKey, RevokedCert>,
    /// The client certificates nodes accepted (ADR 0091), until they have
    /// been expired for [`CERT_KEEP_MS`].
    pub(crate) certs: BTreeMap<CertKey, SeenCert>,
    /// Nodes added at runtime; those from the configuration are not here.
    pub(crate) nodes: BTreeMap<NodeId, NodeMeta>,
    /// Nodes being drained.
    pub(crate) draining: BTreeSet<NodeId>,
    /// Nodes that left; their ids are never used again.
    pub(crate) gone: BTreeSet<NodeId>,
    /// Voters set at runtime for groups from the configuration (the
    /// metadata and coordinator groups); a runtime group's voters live
    /// with its resource.
    pub(crate) targets: BTreeMap<GroupId, Vec<NodeId>>,
    /// The voters each group's leader last reported in place.
    pub(crate) settled: BTreeMap<GroupId, Vec<NodeId>>,
    /// Nodes that were voters of a group and were moved off it: while such
    /// a move is under way, a drained node is not forgotten.
    pub(crate) left: BTreeMap<GroupId, BTreeSet<NodeId>>,
    /// A group's voters before its first change at runtime: what a new
    /// replica of it starts from, as every replica of the group did. A
    /// replica placed later starts outside the group and learns who the
    /// members are from the group's log.
    pub(crate) initial: BTreeMap<GroupId, Vec<NodeId>>,
    /// The metadata index of each group's last `SetVoters`: the tag its
    /// leader puts on the Raft membership that carries it out, so that a
    /// leader whose view of the metadata group lags never undoes a newer
    /// change (ADR 0048).
    pub(crate) target_at: BTreeMap<GroupId, u64>,
    /// Groups whose preferred leader an operator chose (ADR 0065); it
    /// counts only while the node is one of the group's voters.
    pub(crate) pins: BTreeMap<GroupId, NodeId>,
}

const VERSION: u8 = 4;

impl MetadataState {
    /// The id of the runtime resource called `name`, if any.
    pub(crate) fn id_of(&self, name: &str) -> Option<u64> {
        self.streams
            .values()
            .map(|s| (&s.name, s.resource_id))
            .chain(self.queues.values().map(|q| (&q.name, q.resource_id)))
            .chain(self.exchanges.values().map(|x| (&x.name, x.resource_id)))
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, id)| id)
    }

    /// Whether `target` is a fit dead-letter queue for the queue `from` of
    /// `tenant`: another queue of the same tenant that does not (through its
    /// own dead-letter queues) lead back to `from`. A configured queue (an
    /// id below the runtime ones) is not known here: whoever proposed the
    /// change checked it against the configuration, and it has no
    /// dead-letter queue of its own.
    pub(crate) fn dead_letter_fits(&self, from: u64, tenant: &str, target: u64) -> bool {
        if target == from {
            return false;
        }
        if target < DYNAMIC_BASE {
            return true;
        }
        let Some(q) = self.queues.get(&target) else { return false };
        if q.tenant != tenant {
            return false;
        }
        let mut next = q.limits.dead_letter;
        for _ in 0..=self.queues.len() {
            if next == 0 {
                return true;
            }
            if next == from {
                return false;
            }
            next = self.queues.get(&next).map_or(0, |q| q.limits.dead_letter);
        }
        false
    }

    /// Whether some queue names `id` as its dead-letter queue.
    pub(crate) fn is_dead_letter_of_any(&self, id: u64) -> Option<&str> {
        self.queues.values().find(|q| q.limits.dead_letter == id).map(|q| q.name.as_str())
    }

    /// Whether applying a create in `tenant` may go ahead. Whether the
    /// tenant exists (created here or configured) was checked when the
    /// create was proposed; applying only refuses a tenant deleted since,
    /// so that it never depends on a node's configuration.
    pub(crate) fn tenant_open(&self, tenant: &str) -> bool {
        !self.deleted_tenants.contains(tenant)
    }

    /// How many runtime resources `tenant` holds.
    pub(crate) fn resources_in(&self, tenant: &str) -> usize {
        self.streams.values().filter(|s| s.tenant == tenant).count()
            + self.queues.values().filter(|q| q.tenant == tenant).count()
            + self.exchanges.values().filter(|x| x.tenant == tenant).count()
    }

    /// Whether `id` is a deleted resource.
    pub(crate) fn is_deleted(&self, id: u64) -> bool {
        self.deleted.contains_key(&id)
    }

    /// The groups of deleted resources.
    pub(crate) fn deleted_groups(&self) -> BTreeSet<GroupId> {
        let groups = |(&id, &n): (&u64, &u32)| (0..n).map(move |p| group_id(id - DYNAMIC_BASE, p));
        self.deleted.iter().flat_map(groups).collect()
    }

    /// The voters `g` is to have, if set at runtime (a runtime group's
    /// always are).
    pub(crate) fn voters_of(&self, g: GroupId) -> Option<&[NodeId]> {
        if let Some(v) = self.targets.get(&g) {
            return Some(v);
        }
        self.groups().find(|(group, ..)| *group == g).map(|(_, v, _)| v)
    }

    /// The voters a new replica of runtime group `g` starts from: the
    /// group's voters when it was created.
    pub(crate) fn initial_voters(&self, g: GroupId) -> Option<&[NodeId]> {
        self.initial.get(&g).map(Vec::as_slice).or_else(|| self.voters_of(g))
    }

    /// Whether `g` has the voters it is to have, as its leader reported.
    pub(crate) fn is_settled(&self, g: GroupId, voters: &[NodeId]) -> bool {
        let mut want = voters.to_vec();
        want.sort_unstable();
        self.settled.get(&g).is_some_and(|v| *v == want)
    }

    /// Every runtime group and its voters.
    pub(crate) fn groups(&self) -> impl Iterator<Item = (GroupId, &[NodeId], bool)> + '_ {
        let streams = self.streams.values().flat_map(|s| s.partitions.iter().map(|(g, v)| (*g, v.as_slice(), false)));
        streams.chain(self.queues.values().map(|q| (q.group, q.voters.as_slice(), true)))
    }

    /// Applies a committed `Metadata` record at `index`.
    pub(crate) fn apply(&mut self, index: u64, payload: &[u8]) -> Result<(), String> {
        let change = decode_change(payload)?;
        let id = resource_id(index);
        match change {
            Change::CreateStream { name, tenant, partitions, settings } => {
                if self.id_of(&name).is_none() && self.tenant_open(&tenant) {
                    let partitions = partitions
                        .into_iter()
                        .enumerate()
                        .map(|(p, voters)| (group_id(index, p as u32), voters))
                        .collect();
                    self.streams.insert(id, DynStream { name, tenant, resource_id: id, partitions, settings });
                }
            }
            Change::CreateQueue { name, tenant, voters, ack_timeout_ms, limits } => {
                // A dead-letter queue deleted since the create was checked,
                // or one of another tenant, leaves the queue unchanged:
                // better not created than dead-lettering into the wrong place.
                let fits = limits.dead_letter == 0 || self.dead_letter_fits(id, &tenant, limits.dead_letter);
                if fits && self.id_of(&name).is_none() && self.tenant_open(&tenant) {
                    let group = group_id(index, 0);
                    let q = DynQueue { name, tenant, resource_id: id, group, voters, ack_timeout_ms, limits };
                    self.queues.insert(id, q);
                }
            }
            Change::DeclareExchange { name, tenant, kind } => {
                if self.id_of(&name).is_none() && self.tenant_open(&tenant) {
                    self.exchanges.insert(id, DynExchange { name, tenant, resource_id: id, kind });
                }
            }
            Change::Bind { exchange_id, binding } => {
                // The bind was checked against what existed then; a delete
                // may have come before it in the log.
                if self.is_deleted(exchange_id) || self.is_deleted(binding.queue_id) {
                    return Ok(());
                }
                let list = self.bindings.entry(exchange_id).or_default();
                if !list.contains(&binding) {
                    list.push(binding);
                }
            }
            Change::Unbind { exchange_id, binding } => {
                if let Some(list) = self.bindings.get_mut(&exchange_id) {
                    list.retain(|b| *b != binding);
                }
            }
            Change::Delete { resource_id } => {
                // A queue that another names as its dead-letter queue stays:
                // what it receives would have nowhere to go.
                if self.is_dead_letter_of_any(resource_id).is_some() {
                    return Ok(());
                }
                let groups = if let Some(s) = self.streams.remove(&resource_id) {
                    for (g, _) in &s.partitions {
                        self.pins.remove(g);
                    }
                    s.partitions.len() as u32
                } else if let Some(q) = self.queues.remove(&resource_id) {
                    for list in self.bindings.values_mut() {
                        list.retain(|b| b.queue_id != resource_id);
                    }
                    self.pins.remove(&q.group);
                    1
                } else if self.exchanges.remove(&resource_id).is_some() {
                    0
                } else {
                    // Unknown or deleted already: nothing to do.
                    return Ok(());
                };
                self.bindings.remove(&resource_id);
                self.deleted.insert(resource_id, groups);
            }
            Change::AlterQueue { resource_id, change } => {
                if let Some(q) = self.queues.get(&resource_id) {
                    // All or nothing: a dead-letter queue that does not fit
                    // (deleted, other tenant, a loop) refuses the whole change.
                    let limits = change.applied_to(q.limits);
                    let fits = limits.dead_letter == 0
                        || limits.dead_letter == q.limits.dead_letter
                        || self.dead_letter_fits(resource_id, &q.tenant, limits.dead_letter);
                    if fits {
                        let q = self.queues.get_mut(&resource_id).expect("checked");
                        q.limits = limits;
                        if let Some(ms) = change.ack_timeout_ms {
                            q.ack_timeout_ms = ms;
                        }
                    }
                }
            }
            Change::AlterStream { resource_id, change } => {
                if let Some(s) = self.streams.get_mut(&resource_id) {
                    change.apply(&mut s.settings);
                }
            }
            Change::SetUser { user, credential: Some(c) } => {
                self.users.insert(user, c);
            }
            Change::SetUser { user, credential: None } => {
                self.users.remove(&user);
            }
            Change::CreateTenant { name, incarnated } => {
                self.deleted_tenants.remove(&name);
                if incarnated {
                    self.incarnations.insert(name.clone(), index);
                } else {
                    self.incarnations.remove(&name);
                }
                self.tenants.insert(name);
            }
            Change::DeleteTenant { name } => {
                // Checked when proposed; a create may have come in between.
                if self.resources_in(&name) == 0 {
                    self.tenants.remove(&name);
                    self.deleted_tenants.insert(name.clone());
                    self.bindings_of_roles.retain(|b| b.role.cluster_wide() || b.tenant != name);
                }
            }
            Change::SetBindings { add, remove } => {
                for b in &remove {
                    self.bindings_of_roles.remove(b);
                }
                self.bindings_of_roles.extend(add);
            }
            Change::AddNode { id, peer, client } => {
                // Checked against the configuration when proposed; here
                // only against what applied since.
                if !self.gone.contains(&id) && !self.nodes.contains_key(&id) {
                    self.nodes.insert(id, NodeMeta { peer, client });
                }
            }
            Change::DrainNode { id } => {
                if !self.gone.contains(&id) {
                    self.draining.insert(id);
                }
            }
            Change::ForgetNode { id } => {
                self.nodes.remove(&id);
                self.draining.remove(&id);
                self.gone.insert(id);
                self.pins.retain(|_, n| *n != id);
            }
            Change::SetVoters { group, mut voters } => {
                voters.sort_unstable();
                voters.dedup();
                self.target_at.insert(group, index);
                let Some(old) = self.voters_of(group).map(<[NodeId]>::to_vec) else {
                    // A configured group (not runtime, no target yet).
                    self.targets.insert(group, voters);
                    return Ok(());
                };
                self.initial.entry(group).or_insert_with(|| old.clone());
                let left = self.left.entry(group).or_default();
                left.extend(old.iter().copied().filter(|n| !voters.contains(n)));
                if let Some(slot) = self.runtime_voters_mut(group) {
                    *slot = voters;
                } else {
                    self.targets.insert(group, voters);
                }
            }
            Change::Settled { group, mut voters } => {
                voters.sort_unstable();
                self.settled.insert(group, voters);
            }
            Change::PinLeader { group, node } => match node {
                Some(n) => {
                    self.pins.insert(group, n);
                }
                None => {
                    self.pins.remove(&group);
                }
            },
            Change::AddPartitions { resource_id, partitions } => {
                // A stream deleted since, or one that would pass the limit,
                // stays as it is. The groups keep the stream's own index,
                // so deleting it later finds them all.
                if let Some(st) = self.streams.get_mut(&resource_id)
                    && st.partitions.len() + partitions.len() <= MAX_PARTITIONS as usize
                {
                    let first = st.partitions.len() as u32;
                    for (k, voters) in partitions.into_iter().enumerate() {
                        let p = first + k as u32;
                        st.partitions.push((group_id(resource_id - DYNAMIC_BASE, p), voters));
                    }
                }
            }
            Change::RevokeSessions { subject, before_ms } => {
                // A later revocation with an earlier time does not undo one.
                let at = self.revocations.entry(subject).or_insert(0);
                *at = (*at).max(before_ms);
            }
            Change::RecordCertificates { seen, at_ms } => {
                // An entry stays visible for a while after its certificate
                // ended, then goes, by the leader's time carried here.
                self.certs.retain(|_, c| c.not_after_ms.saturating_add(CERT_KEEP_MS) >= at_ms);
                for (key, c) in seen {
                    if c.not_after_ms.saturating_add(CERT_KEEP_MS) < at_ms {
                        continue;
                    }
                    match self.certs.get_mut(&key) {
                        Some(known) => {
                            known.first_seen_ms = known.first_seen_ms.min(c.first_seen_ms);
                            known.last_seen_ms = known.last_seen_ms.max(c.last_seen_ms);
                        }
                        None => {
                            self.certs.insert(key, c);
                        }
                    }
                }
            }
            Change::RevokeCertificate { cert, not_after_ms, at_ms, reason } => {
                // An expired certificate is refused by its validity anyway:
                // forget those, by the time the leader proposed this at, so
                // that every replica forgets the same ones.
                self.revoked_certs.retain(|_, r| r.not_after_ms >= at_ms);
                // The first revocation stands; a repeat changes nothing.
                if not_after_ms >= at_ms {
                    self.revoked_certs.entry(cert).or_insert(RevokedCert { not_after_ms, at_ms, reason });
                }
            }
        }
        Ok(())
    }

    fn runtime_voters_mut(&mut self, g: GroupId) -> Option<&mut Vec<NodeId>> {
        for s in self.streams.values_mut() {
            if let Some((_, v)) = s.partitions.iter_mut().find(|(group, _)| *group == g) {
                return Some(v);
            }
        }
        self.queues.values_mut().find(|q| q.group == g).map(|q| &mut q.voters)
    }

    /// The applied state, for a snapshot.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32_le(self.streams.len() as u32);
        for s in self.streams.values() {
            put_str(&mut b, &s.name);
            put_str(&mut b, &s.tenant);
            b.put_u64_le(s.resource_id);
            b.put_u32_le(s.partitions.len() as u32);
            for (g, voters) in &s.partitions {
                b.put_u64_le(*g);
                put_nodes(&mut b, voters);
            }
            put_settings(&mut b, &s.settings);
        }
        b.put_u32_le(self.queues.len() as u32);
        for q in self.queues.values() {
            put_str(&mut b, &q.name);
            put_str(&mut b, &q.tenant);
            b.put_u64_le(q.resource_id);
            b.put_u64_le(q.group);
            put_nodes(&mut b, &q.voters);
            b.put_u64_le(q.ack_timeout_ms);
            put_limits(&mut b, &q.limits);
        }
        b.put_u32_le(self.exchanges.len() as u32);
        for x in self.exchanges.values() {
            put_str(&mut b, &x.name);
            put_str(&mut b, &x.tenant);
            b.put_u64_le(x.resource_id);
            b.put_u8(x.kind.code());
        }
        b.put_u32_le(self.bindings.len() as u32);
        for (x, list) in &self.bindings {
            b.put_u64_le(*x);
            b.put_u32_le(list.len() as u32);
            for binding in list {
                put_binding(&mut b, binding);
            }
        }
        b.put_u32_le(self.deleted.len() as u32);
        for (id, groups) in &self.deleted {
            b.put_u64_le(*id);
            b.put_u32_le(*groups);
        }
        b.put_u32_le(self.users.len() as u32);
        for (name, c) in &self.users {
            put_str(&mut b, name);
            put_credential(&mut b, c);
        }
        for set in [&self.tenants, &self.deleted_tenants] {
            b.put_u32_le(set.len() as u32);
            for t in set {
                put_str(&mut b, t);
            }
        }
        put_role_bindings(&mut b, self.bindings_of_roles.iter());
        b.put_u32_le(self.nodes.len() as u32);
        for (id, n) in &self.nodes {
            b.put_u64_le(*id);
            put_str(&mut b, &n.peer);
            put_str(&mut b, &n.client);
        }
        for set in [&self.draining, &self.gone] {
            put_nodes(&mut b, &set.iter().copied().collect::<Vec<_>>());
        }
        for map in [&self.targets, &self.settled] {
            b.put_u32_le(map.len() as u32);
            for (g, v) in map {
                b.put_u64_le(*g);
                put_nodes(&mut b, v);
            }
        }
        b.put_u32_le(self.left.len() as u32);
        for (g, v) in &self.left {
            b.put_u64_le(*g);
            put_nodes(&mut b, &v.iter().copied().collect::<Vec<_>>());
        }
        b.put_u32_le(self.initial.len() as u32);
        for (g, v) in &self.initial {
            b.put_u64_le(*g);
            put_nodes(&mut b, v);
        }
        b.put_u32_le(self.target_at.len() as u32);
        for (g, at) in &self.target_at {
            b.put_u64_le(*g);
            b.put_u64_le(*at);
        }
        // Sections added since are optional and written only up to the
        // last that holds something, so a snapshot without them reads on a
        // node from before them: pins (ADR 0065), compaction settings (ADR
        // 0067), tenants' incarnations (ADR 0069), revoked sessions (ADR
        // 0070), single active consumer queues (ADR 0076), priority levels
        // (ADR 0078), revoked certificates (ADR 0090). A later section needs the ones before it, empty or not.
        let compacting: Vec<&DynStream> = self.streams.values().filter(|s| s.settings.has_compaction()).collect();
        let single: Vec<u64> = self.queues.values().filter(|q| q.limits.single_active).map(|q| q.resource_id).collect();
        let levels: Vec<(u64, u8)> = self
            .queues
            .values()
            .filter(|q| q.limits.priority_levels != 0)
            .map(|q| (q.resource_id, q.limits.priority_levels))
            .collect();
        let sections = if !self.certs.is_empty() {
            8
        } else if !self.revoked_certs.is_empty() {
            7
        } else if !levels.is_empty() {
            6
        } else if !single.is_empty() {
            5
        } else if !self.revocations.is_empty() {
            4
        } else if !self.incarnations.is_empty() {
            3
        } else if !compacting.is_empty() {
            2
        } else {
            usize::from(!self.pins.is_empty())
        };
        if sections >= 1 {
            b.put_u32_le(self.pins.len() as u32);
            for (g, n) in &self.pins {
                b.put_u64_le(*g);
                b.put_u64_le(*n);
            }
        }
        if sections >= 2 {
            b.put_u32_le(compacting.len() as u32);
            for s in compacting {
                b.put_u64_le(s.resource_id);
                put_compaction(&mut b, &s.settings);
            }
        }
        if sections >= 3 {
            b.put_u32_le(self.incarnations.len() as u32);
            for (t, i) in &self.incarnations {
                put_str(&mut b, t);
                b.put_u64_le(*i);
            }
        }
        if sections >= 4 {
            b.put_u32_le(self.revocations.len() as u32);
            for (subject, before) in &self.revocations {
                put_subject(&mut b, subject);
                b.put_u64_le(*before);
            }
        }
        if sections >= 5 {
            b.put_u32_le(single.len() as u32);
            for id in single {
                b.put_u64_le(id);
            }
        }
        if sections >= 6 {
            b.put_u32_le(levels.len() as u32);
            for (id, n) in levels {
                b.put_u64_le(id);
                b.put_u8(n);
            }
        }
        if sections >= 7 {
            b.put_u32_le(self.revoked_certs.len() as u32);
            for (cert, r) in &self.revoked_certs {
                put_cert_key(&mut b, cert);
                b.put_u64_le(r.not_after_ms);
                b.put_u64_le(r.at_ms);
                put_str(&mut b, &r.reason);
            }
        }
        if sections >= 8 {
            b.put_u32_le(self.certs.len() as u32);
            for (cert, c) in &self.certs {
                put_cert_key(&mut b, cert);
                put_seen(&mut b, c);
            }
        }
        b.freeze()
    }

    /// The state a snapshot holds.
    pub(crate) fn decode(mut b: &[u8]) -> Result<Self, String> {
        let b = &mut b;
        let mut s = MetadataState::default();
        for _ in 0..count(b)? {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            need(b, 12)?;
            let resource_id = b.get_u64_le();
            let mut partitions = Vec::new();
            for _ in 0..b.get_u32_le() {
                need(b, 8)?;
                let g = b.get_u64_le();
                partitions.push((g, get_nodes(b)?));
            }
            let settings = get_settings(b)?;
            s.streams.insert(resource_id, DynStream { name, tenant, resource_id, partitions, settings });
        }
        for _ in 0..count(b)? {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            need(b, 16)?;
            let (resource_id, group) = (b.get_u64_le(), b.get_u64_le());
            let voters = get_nodes(b)?;
            need(b, 8)?;
            let ack_timeout_ms = b.get_u64_le();
            let limits = get_limits(b)?;
            s.queues.insert(resource_id, DynQueue { name, tenant, resource_id, group, voters, ack_timeout_ms, limits });
        }
        for _ in 0..count(b)? {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            need(b, 9)?;
            let resource_id = b.get_u64_le();
            let kind = ExchangeKind::from_code(b.get_u8()).ok_or("unknown exchange kind")?;
            s.exchanges.insert(resource_id, DynExchange { name, tenant, resource_id, kind });
        }
        for _ in 0..count(b)? {
            need(b, 12)?;
            let x = b.get_u64_le();
            let mut list = Vec::new();
            for _ in 0..b.get_u32_le() {
                list.push(get_binding(b)?);
            }
            s.bindings.insert(x, list);
        }
        for _ in 0..count(b)? {
            need(b, 12)?;
            let id = b.get_u64_le();
            s.deleted.insert(id, b.get_u32_le());
        }
        for _ in 0..count(b)? {
            let name = get_str(b)?;
            s.users.insert(name, get_credential(b)?);
        }
        for _ in 0..count(b)? {
            s.tenants.insert(get_str(b)?);
        }
        for _ in 0..count(b)? {
            s.deleted_tenants.insert(get_str(b)?);
        }
        s.bindings_of_roles = get_role_bindings(b)?.into_iter().collect();
        for _ in 0..count(b)? {
            need(b, 8)?;
            let id = b.get_u64_le();
            let (peer, client) = (get_str(b)?, get_str(b)?);
            s.nodes.insert(id, NodeMeta { peer, client });
        }
        s.draining = get_nodes(b)?.into_iter().collect();
        s.gone = get_nodes(b)?.into_iter().collect();
        for map in [&mut s.targets, &mut s.settled] {
            for _ in 0..count(b)? {
                need(b, 8)?;
                let g = b.get_u64_le();
                map.insert(g, get_nodes(b)?);
            }
        }
        for _ in 0..count(b)? {
            need(b, 8)?;
            let g = b.get_u64_le();
            s.left.insert(g, get_nodes(b)?.into_iter().collect());
        }
        for _ in 0..count(b)? {
            need(b, 8)?;
            let g = b.get_u64_le();
            s.initial.insert(g, get_nodes(b)?);
        }
        for _ in 0..count(b)? {
            need(b, 16)?;
            let g = b.get_u64_le();
            s.target_at.insert(g, b.get_u64_le());
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                need(b, 16)?;
                let g = b.get_u64_le();
                s.pins.insert(g, b.get_u64_le());
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                need(b, 8)?;
                let id = b.get_u64_le();
                let stream = s.streams.get_mut(&id).ok_or("compaction settings of an unknown stream")?;
                get_compaction(b, &mut stream.settings)?;
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                let t = get_str(b)?;
                need(b, 8)?;
                s.incarnations.insert(t, b.get_u64_le());
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                let subject = get_subject(b)?;
                need(b, 8)?;
                s.revocations.insert(subject, b.get_u64_le());
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                need(b, 8)?;
                let id = b.get_u64_le();
                s.queues.get_mut(&id).ok_or("single active consumer of an unknown queue")?.limits.single_active = true;
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                need(b, 9)?;
                let id = b.get_u64_le();
                let levels = b.get_u8();
                s.queues.get_mut(&id).ok_or("priority levels of an unknown queue")?.limits.priority_levels = levels;
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                let cert = get_cert_key(b)?;
                need(b, 16)?;
                let (not_after_ms, at_ms) = (b.get_u64_le(), b.get_u64_le());
                s.revoked_certs.insert(cert, RevokedCert { not_after_ms, at_ms, reason: get_str(b)? });
            }
        }
        if b.has_remaining() {
            for _ in 0..count(b)? {
                let cert = get_cert_key(b)?;
                s.certs.insert(cert, get_seen(b)?);
            }
        }
        if b.has_remaining() {
            return Err("metadata state has trailing bytes".into());
        }
        Ok(s)
    }
}

/// The record for `change`, as an internal control batch.
pub(crate) fn record(change: &Change) -> Bytes {
    lotse_record::internal_control_batch(lotse_record::InternalControl::Metadata, &change_bytes(change))
        .expect("one record")
}

/// The record's payload, as [`MetadataState::apply`] takes it.
#[cfg(test)]
pub(crate) fn record_payload(change: &Change) -> Bytes {
    change_bytes(change)
}

fn change_bytes(change: &Change) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(VERSION);
    match change {
        Change::CreateStream { name, tenant, partitions, settings } => {
            b.put_u8(0);
            put_str(&mut b, name);
            put_str(&mut b, tenant);
            b.put_u32_le(partitions.len() as u32);
            for voters in partitions {
                put_nodes(&mut b, voters);
            }
            put_settings(&mut b, settings);
            if settings.has_compaction() {
                put_compaction(&mut b, settings);
            }
        }
        Change::CreateQueue { name, tenant, voters, ack_timeout_ms, limits } => {
            b.put_u8(1);
            put_str(&mut b, name);
            put_str(&mut b, tenant);
            put_nodes(&mut b, voters);
            b.put_u64_le(*ack_timeout_ms);
            put_limits(&mut b, limits);
            // A trailing flag byte only when a flag is set, then what the
            // flags say comes: older records read as before.
            put_flags(&mut b, limits.single_active, limits.priority_levels);
        }
        Change::DeclareExchange { name, tenant, kind } => {
            b.put_u8(2);
            put_str(&mut b, name);
            put_str(&mut b, tenant);
            b.put_u8(kind.code());
        }
        Change::Bind { exchange_id, binding } | Change::Unbind { exchange_id, binding } => {
            b.put_u8(if matches!(change, Change::Bind { .. }) { 3 } else { 4 });
            b.put_u64_le(*exchange_id);
            put_binding(&mut b, binding);
        }
        Change::Delete { resource_id } => {
            b.put_u8(5);
            b.put_u64_le(*resource_id);
        }
        Change::AlterQueue { resource_id, change } => {
            b.put_u8(6);
            b.put_u64_le(*resource_id);
            let mask = u8::from(change.ack_timeout_ms.is_some())
                | u8::from(change.max_deliveries.is_some()) << 1
                | u8::from(change.message_ttl_ms.is_some()) << 2
                | u8::from(change.dead_letter.is_some()) << 3
                | u8::from(change.single_active.is_some()) << 4
                | u8::from(change.priority_levels.is_some()) << 5;
            b.put_u8(mask);
            b.put_u64_le(change.ack_timeout_ms.unwrap_or(0));
            b.put_u32_le(change.max_deliveries.unwrap_or(0));
            b.put_u64_le(change.message_ttl_ms.unwrap_or(0));
            b.put_u64_le(change.dead_letter.unwrap_or(0));
            if let Some(on) = change.single_active {
                b.put_u8(u8::from(on));
            }
            if let Some(n) = change.priority_levels {
                b.put_u8(n);
            }
        }
        Change::AlterStream { resource_id, change } => {
            b.put_u8(11);
            b.put_u64_le(*resource_id);
            let mut slots = vec![change.retention_ms, change.retention_bytes];
            if change.has_compaction() {
                slots.push(change.cleanup.map(|c| c.map(Cleanup::code)));
                slots.push(change.delete_retention_ms);
            }
            for v in slots {
                match v {
                    None => b.put_u8(0),
                    Some(None) => b.put_u8(1),
                    Some(Some(v)) => {
                        b.put_u8(2);
                        b.put_i64_le(v);
                    }
                }
            }
        }
        Change::AddNode { id, peer, client } => {
            b.put_u8(12);
            b.put_u64_le(*id);
            put_str(&mut b, peer);
            put_str(&mut b, client);
        }
        Change::DrainNode { id } => {
            b.put_u8(13);
            b.put_u64_le(*id);
        }
        Change::ForgetNode { id } => {
            b.put_u8(14);
            b.put_u64_le(*id);
        }
        Change::SetVoters { group, voters } => {
            b.put_u8(15);
            b.put_u64_le(*group);
            put_nodes(&mut b, voters);
        }
        Change::Settled { group, voters } => {
            b.put_u8(16);
            b.put_u64_le(*group);
            put_nodes(&mut b, voters);
        }
        Change::PinLeader { group, node } => {
            b.put_u8(17);
            b.put_u64_le(*group);
            b.put_u8(u8::from(node.is_some()));
            b.put_u64_le(node.unwrap_or(0));
        }
        Change::AddPartitions { resource_id, partitions } => {
            b.put_u8(19);
            b.put_u64_le(*resource_id);
            b.put_u32_le(partitions.len() as u32);
            for voters in partitions {
                put_nodes(&mut b, voters);
            }
        }
        Change::RevokeSessions { subject, before_ms } => {
            b.put_u8(18);
            put_subject(&mut b, subject);
            b.put_u64_le(*before_ms);
        }
        Change::RecordCertificates { seen, at_ms } => {
            b.put_u8(21);
            b.put_u64_le(*at_ms);
            b.put_u32_le(seen.len() as u32);
            for (cert, c) in seen {
                put_cert_key(&mut b, cert);
                put_seen(&mut b, c);
            }
        }
        Change::RevokeCertificate { cert, not_after_ms, at_ms, reason } => {
            b.put_u8(20);
            put_cert_key(&mut b, cert);
            b.put_u64_le(*not_after_ms);
            b.put_u64_le(*at_ms);
            put_str(&mut b, reason);
        }
        Change::SetUser { user, credential } => {
            b.put_u8(7);
            put_str(&mut b, user);
            b.put_u8(u8::from(credential.is_some()));
            if let Some(c) = credential {
                put_credential(&mut b, c);
            }
        }
        Change::CreateTenant { name, incarnated } => {
            b.put_u8(8);
            put_str(&mut b, name);
            // Absent in records from before ADR 0069.
            if *incarnated {
                b.put_u8(1);
            }
        }
        Change::DeleteTenant { name } => {
            b.put_u8(9);
            put_str(&mut b, name);
        }
        Change::SetBindings { add, remove } => {
            b.put_u8(10);
            put_role_bindings(&mut b, add.iter());
            put_role_bindings(&mut b, remove.iter());
        }
    }
    b.freeze()
}

fn decode_change(mut b: &[u8]) -> Result<Change, String> {
    let b = &mut b;
    need(b, 2)?;
    if b.get_u8() != VERSION {
        return Err("metadata record has an unknown version".into());
    }
    let tag = b.get_u8();
    let change = match tag {
        0 => {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            let partitions = (0..count(b)?).map(|_| get_nodes(b)).collect::<Result<_, _>>()?;
            let mut settings = get_settings(b)?;
            if b.has_remaining() {
                get_compaction(b, &mut settings)?;
            }
            Change::CreateStream { name, tenant, partitions, settings }
        }
        1 => {
            let (name, tenant, voters) = (get_str(b)?, get_str(b)?, get_nodes(b)?);
            need(b, 8)?;
            let ack_timeout_ms = b.get_u64_le();
            let mut limits = get_limits(b)?;
            if b.has_remaining() {
                (limits.single_active, limits.priority_levels) = get_flags(b)?;
            }
            Change::CreateQueue { name, tenant, voters, ack_timeout_ms, limits }
        }
        2 => {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            need(b, 1)?;
            let kind = ExchangeKind::from_code(b.get_u8()).ok_or("unknown exchange kind")?;
            Change::DeclareExchange { name, tenant, kind }
        }
        t @ (3 | 4) => {
            need(b, 8)?;
            let exchange_id = b.get_u64_le();
            let binding = get_binding(b)?;
            if t == 3 { Change::Bind { exchange_id, binding } } else { Change::Unbind { exchange_id, binding } }
        }
        5 => {
            need(b, 8)?;
            Change::Delete { resource_id: b.get_u64_le() }
        }
        6 => {
            need(b, 8 + 1 + 8 + 4 + 8 + 8)?;
            let resource_id = b.get_u64_le();
            let mask = b.get_u8();
            let (ack, max, ttl, dlq) = (b.get_u64_le(), b.get_u32_le(), b.get_u64_le(), b.get_u64_le());
            let change = QueueChange {
                ack_timeout_ms: (mask & 1 != 0).then_some(ack),
                max_deliveries: (mask & 2 != 0).then_some(max),
                message_ttl_ms: (mask & 4 != 0).then_some(ttl),
                dead_letter: (mask & 8 != 0).then_some(dlq),
                single_active: if mask & 16 != 0 { Some(get_flags(b)?.0) } else { None },
                priority_levels: if mask & 32 != 0 {
                    need(b, 1)?;
                    Some(b.get_u8())
                } else {
                    None
                },
            };
            Change::AlterQueue { resource_id, change }
        }
        7 => {
            let user = get_str(b)?;
            need(b, 1)?;
            let credential = match b.get_u8() {
                0 => None,
                1 => Some(get_credential(b)?),
                _ => return Err("bad credential flag".into()),
            };
            Change::SetUser { user, credential }
        }
        8 => {
            let name = get_str(b)?;
            let incarnated = match b.first() {
                None => false,
                Some(1) => {
                    b.advance(1);
                    true
                }
                Some(_) => return Err("unknown tenant record".into()),
            };
            Change::CreateTenant { name, incarnated }
        }
        9 => Change::DeleteTenant { name: get_str(b)? },
        10 => Change::SetBindings { add: get_role_bindings(b)?, remove: get_role_bindings(b)? },
        11 => {
            need(b, 8)?;
            let resource_id = b.get_u64_le();
            fn get(b: &mut &[u8]) -> Result<Option<Option<i64>>, String> {
                need(b, 1)?;
                match b.get_u8() {
                    0 => Ok(None),
                    1 => Ok(Some(None)),
                    2 => {
                        need(b, 8)?;
                        Ok(Some(Some(b.get_i64_le())))
                    }
                    _ => Err("unknown stream setting change".into()),
                }
            }
            let (retention_ms, retention_bytes) = (get(b)?, get(b)?);
            // Written only by nodes that know compaction.
            let (cleanup, delete_retention_ms) = if b.has_remaining() {
                let cleanup = match get(b)? {
                    Some(Some(c)) => Some(Some(Cleanup::from_code(c)?)),
                    Some(None) => Some(None),
                    None => None,
                };
                (cleanup, get(b)?)
            } else {
                (None, None)
            };
            let change = StreamChange { retention_ms, retention_bytes, cleanup, delete_retention_ms };
            Change::AlterStream { resource_id, change }
        }
        12 => {
            need(b, 8)?;
            let id = b.get_u64_le();
            Change::AddNode { id, peer: get_str(b)?, client: get_str(b)? }
        }
        13 | 14 => {
            need(b, 8)?;
            let id = b.get_u64_le();
            if tag == 13 { Change::DrainNode { id } } else { Change::ForgetNode { id } }
        }
        15 | 16 => {
            need(b, 8)?;
            let group = b.get_u64_le();
            let voters = get_nodes(b)?;
            if tag == 15 { Change::SetVoters { group, voters } } else { Change::Settled { group, voters } }
        }
        17 => {
            need(b, 17)?;
            let group = b.get_u64_le();
            let pinned = b.get_u8() != 0;
            let node = b.get_u64_le();
            Change::PinLeader { group, node: pinned.then_some(node) }
        }
        19 => {
            need(b, 8)?;
            let resource_id = b.get_u64_le();
            let partitions = (0..count(b)?).map(|_| get_nodes(b)).collect::<Result<_, _>>()?;
            Change::AddPartitions { resource_id, partitions }
        }
        21 => {
            need(b, 8)?;
            let at_ms = b.get_u64_le();
            let seen =
                (0..count(b)?).map(|_| Ok((get_cert_key(b)?, get_seen(b)?))).collect::<Result<Vec<_>, String>>()?;
            Change::RecordCertificates { seen, at_ms }
        }
        20 => {
            let cert = get_cert_key(b)?;
            need(b, 16)?;
            let (not_after_ms, at_ms) = (b.get_u64_le(), b.get_u64_le());
            Change::RevokeCertificate { cert, not_after_ms, at_ms, reason: get_str(b)? }
        }
        18 => {
            let subject = get_subject(b)?;
            need(b, 8)?;
            Change::RevokeSessions { subject, before_ms: b.get_u64_le() }
        }
        _ => return Err("unknown metadata change".into()),
    };
    if b.has_remaining() {
        return Err("metadata record has trailing bytes".into());
    }
    Ok(change)
}

fn put_limits(b: &mut BytesMut, l: &QueueLimits) {
    b.put_u32_le(l.max_deliveries);
    b.put_u64_le(l.message_ttl_ms);
    b.put_u64_le(l.dead_letter);
}

fn get_limits(b: &mut &[u8]) -> Result<QueueLimits, String> {
    need(b, 20)?;
    Ok(QueueLimits {
        max_deliveries: b.get_u32_le(),
        message_ttl_ms: b.get_u64_le(),
        dead_letter: b.get_u64_le(),
        single_active: false,
        priority_levels: 0,
    })
}

/// A queue's flags, written only when one is set: bit 0 single active
/// consumer; bit 1 priority levels, whose count follows.
fn put_flags(b: &mut BytesMut, single_active: bool, levels: u8) {
    let flags = u8::from(single_active) | u8::from(levels != 0) << 1;
    if flags != 0 {
        b.put_u8(flags);
    }
    if levels != 0 {
        b.put_u8(levels);
    }
}

/// The flags [`put_flags`] writes; in an `AlterQueue` record only bit 0
/// is ever set.
fn get_flags(b: &mut &[u8]) -> Result<(bool, u8), String> {
    need(b, 1)?;
    let flags = b.get_u8();
    if flags > 3 {
        return Err("unknown queue flags".into());
    }
    let levels = if flags & 2 != 0 {
        need(b, 1)?;
        b.get_u8()
    } else {
        0
    };
    Ok((flags & 1 != 0, levels))
}

fn need(b: &&[u8], n: usize) -> Result<(), String> {
    if b.remaining() < n { Err("metadata is truncated".into()) } else { Ok(()) }
}

fn count(b: &mut &[u8]) -> Result<u32, String> {
    need(b, 4)?;
    Ok(b.get_u32_le())
}

fn put_str(b: &mut BytesMut, s: &str) {
    b.put_u16_le(s.len() as u16);
    b.put_slice(s.as_bytes());
}

fn get_str(b: &mut &[u8]) -> Result<String, String> {
    need(b, 2)?;
    let n = usize::from(b.get_u16_le());
    need(b, n)?;
    let s = String::from_utf8(b[..n].to_vec()).map_err(|_| "a name is not UTF-8".to_string())?;
    b.advance(n);
    Ok(s)
}

fn put_bytes(b: &mut BytesMut, v: &[u8]) {
    b.put_u32_le(v.len() as u32);
    b.put_slice(v);
}

fn get_bytes(b: &mut &[u8]) -> Result<Bytes, String> {
    let n = count(b)? as usize;
    need(b, n)?;
    let v = Bytes::copy_from_slice(&b[..n]);
    b.advance(n);
    Ok(v)
}

fn put_role_bindings<'a>(b: &mut BytesMut, list: impl ExactSizeIterator<Item = &'a RoleBinding>) {
    b.put_u32_le(list.len() as u32);
    for r in list {
        let (kind, subject) = match &r.subject {
            Subject::User(u) => (0, u),
            Subject::Group(g) => (1, g),
        };
        b.put_u8(kind);
        put_str(b, subject);
        b.put_u8(r.role.code());
        put_str(b, &r.tenant);
        put_str(b, &r.prefix);
    }
}

fn get_role_bindings(b: &mut &[u8]) -> Result<Vec<RoleBinding>, String> {
    (0..count(b)?)
        .map(|_| {
            need(b, 1)?;
            let kind = b.get_u8();
            let subject = get_str(b)?;
            let subject = match kind {
                0 => Subject::User(subject),
                1 => Subject::Group(subject),
                _ => return Err("unknown subject kind".to_string()),
            };
            need(b, 1)?;
            let role = Role::from_code(b.get_u8()).ok_or("unknown role")?;
            Ok(RoleBinding { subject, role, tenant: get_str(b)?, prefix: get_str(b)? })
        })
        .collect()
}

fn put_credential(b: &mut BytesMut, c: &ScramCredential) {
    b.put_u32_le(c.iterations);
    put_bytes(b, &c.salt);
    put_bytes(b, &c.stored_key);
    put_bytes(b, &c.server_key);
}

fn get_credential(b: &mut &[u8]) -> Result<ScramCredential, String> {
    need(b, 4)?;
    let iterations = b.get_u32_le();
    let salt = get_bytes(b)?.to_vec();
    let stored_key = get_bytes(b)?.to_vec();
    Ok(ScramCredential { iterations, salt, stored_key, server_key: get_bytes(b)?.to_vec() })
}

/// A setting: unset (the default) is `i64::MIN`.
fn put_settings(b: &mut BytesMut, s: &StreamSettings) {
    for v in [s.retention_ms, s.retention_bytes] {
        b.put_i64_le(v.unwrap_or(i64::MIN));
    }
}

fn get_settings(b: &mut &[u8]) -> Result<StreamSettings, String> {
    need(b, 16)?;
    let mut get = || Some(b.get_i64_le()).filter(|v| *v != i64::MIN);
    Ok(StreamSettings { retention_ms: get(), retention_bytes: get(), ..StreamSettings::default() })
}

/// The settings that came with compaction: unset is `i64::MIN`.
fn put_compaction(b: &mut BytesMut, s: &StreamSettings) {
    b.put_i64_le(s.cleanup.map_or(i64::MIN, Cleanup::code));
    b.put_i64_le(s.delete_retention_ms.unwrap_or(i64::MIN));
}

fn get_compaction(b: &mut &[u8], s: &mut StreamSettings) -> Result<(), String> {
    need(b, 16)?;
    let cleanup = b.get_i64_le();
    s.cleanup = if cleanup == i64::MIN { None } else { Some(Cleanup::from_code(cleanup)?) };
    s.delete_retention_ms = Some(b.get_i64_le()).filter(|v| *v != i64::MIN);
    Ok(())
}

/// A client certificate as revocation names it (ADR 0090): the issuer by the
/// SHA-256 of its name, and the serial.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CertKey {
    pub(crate) issuer: [u8; 32],
    pub(crate) serial: Vec<u8>,
}

/// A revoked certificate, until it expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RevokedCert {
    /// Milliseconds since the Unix epoch at which the certificate expires.
    pub(crate) not_after_ms: u64,
    /// The leader's clock when it was revoked.
    pub(crate) at_ms: u64,
    pub(crate) reason: String,
}

/// How long an inventory entry stays after its certificate ended: 30 days.
pub(crate) const CERT_KEEP_MS: u64 = 30 * 24 * 3_600_000;

/// A client certificate a node accepted (ADR 0091).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeenCert {
    /// The principal it names.
    pub(crate) principal: String,
    pub(crate) not_before_ms: u64,
    pub(crate) not_after_ms: u64,
    /// When a node first accepted it, ms since the Unix epoch.
    pub(crate) first_seen_ms: u64,
    /// When a node last accepted it, as nodes report.
    pub(crate) last_seen_ms: u64,
}

fn put_seen(b: &mut BytesMut, c: &SeenCert) {
    put_str(b, &c.principal);
    b.put_u64_le(c.not_before_ms);
    b.put_u64_le(c.not_after_ms);
    b.put_u64_le(c.first_seen_ms);
    b.put_u64_le(c.last_seen_ms);
}

fn get_seen(b: &mut &[u8]) -> Result<SeenCert, String> {
    let principal = get_str(b)?;
    need(b, 32)?;
    Ok(SeenCert {
        principal,
        not_before_ms: b.get_u64_le(),
        not_after_ms: b.get_u64_le(),
        first_seen_ms: b.get_u64_le(),
        last_seen_ms: b.get_u64_le(),
    })
}

fn put_cert_key(b: &mut BytesMut, k: &CertKey) {
    b.put_slice(&k.issuer);
    b.put_u16_le(k.serial.len() as u16);
    b.put_slice(&k.serial);
}

fn get_cert_key(b: &mut &[u8]) -> Result<CertKey, String> {
    need(b, 34)?;
    let mut issuer = [0u8; 32];
    b.copy_to_slice(&mut issuer);
    let n = usize::from(b.get_u16_le());
    need(b, n)?;
    let mut serial = vec![0u8; n];
    b.copy_to_slice(&mut serial);
    Ok(CertKey { issuer, serial })
}

fn put_subject(b: &mut BytesMut, subject: &Subject) {
    let (kind, name) = match subject {
        Subject::User(u) => (0, u),
        Subject::Group(g) => (1, g),
    };
    b.put_u8(kind);
    put_str(b, name);
}

fn get_subject(b: &mut &[u8]) -> Result<Subject, String> {
    need(b, 1)?;
    match b.get_u8() {
        0 => Ok(Subject::User(get_str(b)?)),
        1 => Ok(Subject::Group(get_str(b)?)),
        _ => Err("unknown subject kind".into()),
    }
}

fn put_nodes(b: &mut BytesMut, nodes: &[NodeId]) {
    b.put_u32_le(nodes.len() as u32);
    for n in nodes {
        b.put_u64_le(*n);
    }
}

fn get_nodes(b: &mut &[u8]) -> Result<Vec<NodeId>, String> {
    let n = count(b)? as usize;
    need(b, n.saturating_mul(8))?;
    Ok((0..n).map(|_| b.get_u64_le()).collect())
}

fn put_binding(b: &mut BytesMut, x: &Binding) {
    b.put_u64_le(x.queue_id);
    put_str(b, &x.key);
    b.put_u32_le(x.headers.len() as u32);
    for (k, v) in &x.headers {
        put_bytes(b, k);
        b.put_u8(u8::from(v.is_some()));
        if let Some(v) = v {
            put_bytes(b, v);
        }
    }
    b.put_u8(u8::from(x.match_all));
}

fn get_binding(b: &mut &[u8]) -> Result<Binding, String> {
    need(b, 8)?;
    let queue_id = b.get_u64_le();
    let key = get_str(b)?;
    let mut headers = Vec::new();
    for _ in 0..count(b)? {
        let k = get_bytes(b)?;
        need(b, 1)?;
        let v = if b.get_u8() != 0 { Some(get_bytes(b)?) } else { None };
        headers.push((k, v));
    }
    need(b, 1)?;
    Ok(Binding { queue_id, key, headers, match_all: b.get_u8() != 0 })
}

/// Chooses `replication` voters for each of `groups` new groups: each time
/// the nodes with the fewest groups, ties by node id. `load` counts groups
/// per node so far and is updated.
pub(crate) fn place(
    nodes: &[NodeId],
    load: &mut BTreeMap<NodeId, usize>,
    replication: usize,
    groups: usize,
) -> Vec<Vec<NodeId>> {
    (0..groups)
        .map(|_| {
            let mut order: Vec<NodeId> = nodes.to_vec();
            order.sort_by_key(|n| (load.get(n).copied().unwrap_or(0), *n));
            let chosen: Vec<NodeId> = order.into_iter().take(replication).collect();
            for n in &chosen {
                *load.entry(*n).or_default() += 1;
            }
            let mut sorted = chosen;
            sorted.sort_unstable();
            sorted
        })
        .collect()
}

/// Groups per node: configured ones (on every node) and runtime ones.
pub(crate) fn load(nodes: &[NodeId], configured: usize, state: &MetadataState) -> BTreeMap<NodeId, usize> {
    let mut load: BTreeMap<NodeId, usize> = nodes.iter().map(|n| (*n, configured)).collect();
    for (_, voters, _) in state.groups() {
        for v in voters {
            *load.entry(*v).or_default() += 1;
        }
    }
    load
}

#[cfg(test)]
mod tests;

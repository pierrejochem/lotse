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
    CreateStream { name: String, tenant: String, partitions: Vec<Vec<NodeId>> },
    /// A queue and its voters; an ack timeout of 0 is the default.
    CreateQueue { name: String, tenant: String, voters: Vec<NodeId>, ack_timeout_ms: u64 },
    /// An exchange.
    DeclareExchange { name: String, tenant: String, kind: ExchangeKind },
    /// A binding added to an exchange.
    Bind { exchange_id: u64, binding: Binding },
    /// A binding removed from an exchange.
    Unbind { exchange_id: u64, binding: Binding },
    /// A resource deleted, with its groups.
    Delete { resource_id: u64 },
    /// A queue's ack timeout changed; 0 returns it to the default.
    AlterQueue { resource_id: u64, ack_timeout_ms: u64 },
    /// A user's SCRAM credential set, or removed with `None`.
    SetUser { user: String, credential: Option<ScramCredential> },
    /// A tenant created.
    CreateTenant { name: String },
    /// An empty tenant deleted, with its role bindings.
    DeleteTenant { name: String },
    /// Role bindings removed, then added.
    SetBindings { add: Vec<RoleBinding>, remove: Vec<RoleBinding> },
}

/// A stream created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynStream {
    pub(crate) name: String,
    pub(crate) tenant: String,
    pub(crate) resource_id: u64,
    /// Per partition: its group and voters.
    pub(crate) partitions: Vec<(GroupId, Vec<NodeId>)>,
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
}

const VERSION: u8 = 1;

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
            Change::CreateStream { name, tenant, partitions } => {
                if self.id_of(&name).is_none() && self.tenant_open(&tenant) {
                    let partitions = partitions
                        .into_iter()
                        .enumerate()
                        .map(|(p, voters)| (group_id(index, p as u32), voters))
                        .collect();
                    self.streams.insert(id, DynStream { name, tenant, resource_id: id, partitions });
                }
            }
            Change::CreateQueue { name, tenant, voters, ack_timeout_ms } => {
                if self.id_of(&name).is_none() && self.tenant_open(&tenant) {
                    let group = group_id(index, 0);
                    let q = DynQueue { name, tenant, resource_id: id, group, voters, ack_timeout_ms };
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
                let groups = if let Some(s) = self.streams.remove(&resource_id) {
                    s.partitions.len() as u32
                } else if self.queues.remove(&resource_id).is_some() {
                    for list in self.bindings.values_mut() {
                        list.retain(|b| b.queue_id != resource_id);
                    }
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
            Change::AlterQueue { resource_id, ack_timeout_ms } => {
                if let Some(q) = self.queues.get_mut(&resource_id) {
                    q.ack_timeout_ms = ack_timeout_ms;
                }
            }
            Change::SetUser { user, credential: Some(c) } => {
                self.users.insert(user, c);
            }
            Change::SetUser { user, credential: None } => {
                self.users.remove(&user);
            }
            Change::CreateTenant { name } => {
                self.deleted_tenants.remove(&name);
                self.tenants.insert(name);
            }
            Change::DeleteTenant { name } => {
                // Checked when proposed; a create may have come in between.
                if self.resources_in(&name) == 0 {
                    self.tenants.remove(&name);
                    self.deleted_tenants.insert(name.clone());
                    self.bindings_of_roles.retain(|b| b.role == Role::ClusterAdmin || b.tenant != name);
                }
            }
            Change::SetBindings { add, remove } => {
                for b in &remove {
                    self.bindings_of_roles.remove(b);
                }
                self.bindings_of_roles.extend(add);
            }
        }
        Ok(())
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
        }
        b.put_u32_le(self.queues.len() as u32);
        for q in self.queues.values() {
            put_str(&mut b, &q.name);
            put_str(&mut b, &q.tenant);
            b.put_u64_le(q.resource_id);
            b.put_u64_le(q.group);
            put_nodes(&mut b, &q.voters);
            b.put_u64_le(q.ack_timeout_ms);
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
            s.streams.insert(resource_id, DynStream { name, tenant, resource_id, partitions });
        }
        for _ in 0..count(b)? {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            need(b, 16)?;
            let (resource_id, group) = (b.get_u64_le(), b.get_u64_le());
            let voters = get_nodes(b)?;
            need(b, 8)?;
            let ack_timeout_ms = b.get_u64_le();
            s.queues.insert(resource_id, DynQueue { name, tenant, resource_id, group, voters, ack_timeout_ms });
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
        if b.has_remaining() {
            return Err("metadata state has trailing bytes".into());
        }
        Ok(s)
    }
}

/// The record for `change`, as an internal control batch.
pub(crate) fn record(change: &Change) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u8(VERSION);
    match change {
        Change::CreateStream { name, tenant, partitions } => {
            b.put_u8(0);
            put_str(&mut b, name);
            put_str(&mut b, tenant);
            b.put_u32_le(partitions.len() as u32);
            for voters in partitions {
                put_nodes(&mut b, voters);
            }
        }
        Change::CreateQueue { name, tenant, voters, ack_timeout_ms } => {
            b.put_u8(1);
            put_str(&mut b, name);
            put_str(&mut b, tenant);
            put_nodes(&mut b, voters);
            b.put_u64_le(*ack_timeout_ms);
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
        Change::AlterQueue { resource_id, ack_timeout_ms } => {
            b.put_u8(6);
            b.put_u64_le(*resource_id);
            b.put_u64_le(*ack_timeout_ms);
        }
        Change::SetUser { user, credential } => {
            b.put_u8(7);
            put_str(&mut b, user);
            b.put_u8(u8::from(credential.is_some()));
            if let Some(c) = credential {
                put_credential(&mut b, c);
            }
        }
        Change::CreateTenant { name } | Change::DeleteTenant { name } => {
            b.put_u8(if matches!(change, Change::CreateTenant { .. }) { 8 } else { 9 });
            put_str(&mut b, name);
        }
        Change::SetBindings { add, remove } => {
            b.put_u8(10);
            put_role_bindings(&mut b, add.iter());
            put_role_bindings(&mut b, remove.iter());
        }
    }
    lotse_record::internal_control_batch(lotse_record::InternalControl::Metadata, &b).expect("one record")
}

fn decode_change(mut b: &[u8]) -> Result<Change, String> {
    let b = &mut b;
    need(b, 2)?;
    if b.get_u8() != VERSION {
        return Err("metadata record has an unknown version".into());
    }
    let change = match b.get_u8() {
        0 => {
            let (name, tenant) = (get_str(b)?, get_str(b)?);
            let partitions = (0..count(b)?).map(|_| get_nodes(b)).collect::<Result<_, _>>()?;
            Change::CreateStream { name, tenant, partitions }
        }
        1 => {
            let (name, tenant, voters) = (get_str(b)?, get_str(b)?, get_nodes(b)?);
            need(b, 8)?;
            Change::CreateQueue { name, tenant, voters, ack_timeout_ms: b.get_u64_le() }
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
            need(b, 16)?;
            Change::AlterQueue { resource_id: b.get_u64_le(), ack_timeout_ms: b.get_u64_le() }
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
        8 => Change::CreateTenant { name: get_str(b)? },
        9 => Change::DeleteTenant { name: get_str(b)? },
        10 => Change::SetBindings { add: get_role_bindings(b)?, remove: get_role_bindings(b)? },
        _ => return Err("unknown metadata change".into()),
    };
    if b.has_remaining() {
        return Err("metadata record has trailing bytes".into());
    }
    Ok(change)
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

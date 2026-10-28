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

use std::collections::BTreeMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
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
    CreateStream { name: String, partitions: Vec<Vec<NodeId>> },
    /// A queue and its voters.
    CreateQueue { name: String, voters: Vec<NodeId>, ack_timeout_ms: u64 },
    /// An exchange.
    DeclareExchange { name: String, kind: ExchangeKind },
    /// A binding added to an exchange.
    Bind { exchange_id: u64, binding: Binding },
    /// A binding removed from an exchange.
    Unbind { exchange_id: u64, binding: Binding },
}

/// A stream created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynStream {
    pub(crate) name: String,
    pub(crate) resource_id: u64,
    /// Per partition: its group and voters.
    pub(crate) partitions: Vec<(GroupId, Vec<NodeId>)>,
}

/// A queue created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynQueue {
    pub(crate) name: String,
    pub(crate) resource_id: u64,
    pub(crate) group: GroupId,
    pub(crate) voters: Vec<NodeId>,
    pub(crate) ack_timeout_ms: u64,
}

/// An exchange created at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynExchange {
    pub(crate) name: String,
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
            Change::CreateStream { name, partitions } => {
                if self.id_of(&name).is_none() {
                    let partitions = partitions
                        .into_iter()
                        .enumerate()
                        .map(|(p, voters)| (group_id(index, p as u32), voters))
                        .collect();
                    self.streams.insert(id, DynStream { name, resource_id: id, partitions });
                }
            }
            Change::CreateQueue { name, voters, ack_timeout_ms } => {
                if self.id_of(&name).is_none() {
                    let group = group_id(index, 0);
                    self.queues.insert(id, DynQueue { name, resource_id: id, group, voters, ack_timeout_ms });
                }
            }
            Change::DeclareExchange { name, kind } => {
                if self.id_of(&name).is_none() {
                    self.exchanges.insert(id, DynExchange { name, resource_id: id, kind });
                }
            }
            Change::Bind { exchange_id, binding } => {
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
        }
        Ok(())
    }

    /// The applied state, for a snapshot.
    pub(crate) fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put_u32_le(self.streams.len() as u32);
        for s in self.streams.values() {
            put_str(&mut b, &s.name);
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
            b.put_u64_le(q.resource_id);
            b.put_u64_le(q.group);
            put_nodes(&mut b, &q.voters);
            b.put_u64_le(q.ack_timeout_ms);
        }
        b.put_u32_le(self.exchanges.len() as u32);
        for x in self.exchanges.values() {
            put_str(&mut b, &x.name);
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
        b.freeze()
    }

    /// The state a snapshot holds.
    pub(crate) fn decode(mut b: &[u8]) -> Result<Self, String> {
        let b = &mut b;
        let mut s = MetadataState::default();
        for _ in 0..count(b)? {
            let name = get_str(b)?;
            need(b, 12)?;
            let resource_id = b.get_u64_le();
            let mut partitions = Vec::new();
            for _ in 0..b.get_u32_le() {
                need(b, 8)?;
                let g = b.get_u64_le();
                partitions.push((g, get_nodes(b)?));
            }
            s.streams.insert(resource_id, DynStream { name, resource_id, partitions });
        }
        for _ in 0..count(b)? {
            let name = get_str(b)?;
            need(b, 16)?;
            let (resource_id, group) = (b.get_u64_le(), b.get_u64_le());
            let voters = get_nodes(b)?;
            need(b, 8)?;
            let ack_timeout_ms = b.get_u64_le();
            s.queues.insert(resource_id, DynQueue { name, resource_id, group, voters, ack_timeout_ms });
        }
        for _ in 0..count(b)? {
            let name = get_str(b)?;
            need(b, 9)?;
            let resource_id = b.get_u64_le();
            let kind = ExchangeKind::from_code(b.get_u8()).ok_or("unknown exchange kind")?;
            s.exchanges.insert(resource_id, DynExchange { name, resource_id, kind });
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
        Change::CreateStream { name, partitions } => {
            b.put_u8(0);
            put_str(&mut b, name);
            b.put_u32_le(partitions.len() as u32);
            for voters in partitions {
                put_nodes(&mut b, voters);
            }
        }
        Change::CreateQueue { name, voters, ack_timeout_ms } => {
            b.put_u8(1);
            put_str(&mut b, name);
            put_nodes(&mut b, voters);
            b.put_u64_le(*ack_timeout_ms);
        }
        Change::DeclareExchange { name, kind } => {
            b.put_u8(2);
            put_str(&mut b, name);
            b.put_u8(kind.code());
        }
        Change::Bind { exchange_id, binding } | Change::Unbind { exchange_id, binding } => {
            b.put_u8(if matches!(change, Change::Bind { .. }) { 3 } else { 4 });
            b.put_u64_le(*exchange_id);
            put_binding(&mut b, binding);
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
            let name = get_str(b)?;
            let partitions = (0..count(b)?).map(|_| get_nodes(b)).collect::<Result<_, _>>()?;
            Change::CreateStream { name, partitions }
        }
        1 => {
            let (name, voters) = (get_str(b)?, get_nodes(b)?);
            need(b, 8)?;
            Change::CreateQueue { name, voters, ack_timeout_ms: b.get_u64_le() }
        }
        2 => {
            let name = get_str(b)?;
            need(b, 1)?;
            Change::DeclareExchange { name, kind: ExchangeKind::from_code(b.get_u8()).ok_or("unknown exchange kind")? }
        }
        t @ (3 | 4) => {
            need(b, 8)?;
            let exchange_id = b.get_u64_le();
            let binding = get_binding(b)?;
            if t == 3 { Change::Bind { exchange_id, binding } } else { Change::Unbind { exchange_id, binding } }
        }
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

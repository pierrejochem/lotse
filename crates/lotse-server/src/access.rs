//! Who may do what on this node: the role bindings and tenants the metadata
//! group applied, the configured ones, and which request needs which role.
//!
//! Every check uses the node's applied state: a binding added or removed
//! takes effect on a node once it applies the metadata group.

use std::collections::{BTreeMap, BTreeSet};

use lotse_auth::roles::{Grants, Operation, Role, RoleBinding, Subject, Who};
use lotse_proto::{Request, config_source};

use crate::auth::{AuthConfig, Principal};
use crate::metadata::MetadataState;

/// The tenant configured resources belong to unless the configuration
/// names another; it always exists.
pub use lotse_proto::DEFAULT_TENANT;

/// What the access checks consult.
#[derive(Debug, Clone, Default)]
pub(crate) struct Access {
    /// Bindings from the configuration (bootstrap admins) and the metadata
    /// group.
    pub(crate) grants: Grants,
    /// Those from the configuration.
    pub(crate) configured: BTreeSet<RoleBinding>,
    /// Every tenant and where it comes from ([`config_source`]).
    pub(crate) tenants: BTreeMap<String, u8>,
    /// Every resource: its name and tenant.
    pub(crate) resources: BTreeMap<u64, (String, String)>,
}

impl Access {
    /// The view from the configuration and the applied metadata state.
    /// `configured` lists the configured resources by id and name.
    pub(crate) fn build(cfg: &AuthConfig, configured: &[(u64, String)], state: &MetadataState) -> Access {
        let configured_bindings: BTreeSet<RoleBinding> = cfg
            .admins
            .iter()
            .map(|a| RoleBinding {
                subject: Subject::User(a.clone()),
                role: Role::ClusterAdmin,
                tenant: String::new(),
                prefix: String::new(),
            })
            .collect();
        let mut tenants: BTreeMap<String, u8> = BTreeMap::new();
        for t in state.tenants.iter() {
            tenants.insert(t.clone(), config_source::DYNAMIC);
        }
        let configured_tenants = std::iter::once(DEFAULT_TENANT.to_string())
            .chain(cfg.tenants.iter().cloned())
            .chain(cfg.resource_tenants.values().cloned());
        for t in configured_tenants {
            tenants.insert(t, config_source::STATIC);
        }
        let mut resources = BTreeMap::new();
        for (id, name) in configured {
            let tenant = cfg.resource_tenants.get(name).cloned().unwrap_or_else(|| DEFAULT_TENANT.into());
            resources.insert(*id, (name.clone(), tenant));
        }
        for s in state.streams.values() {
            resources.insert(s.resource_id, (s.name.clone(), s.tenant.clone()));
        }
        for q in state.queues.values() {
            resources.insert(q.resource_id, (q.name.clone(), q.tenant.clone()));
        }
        for x in state.exchanges.values() {
            resources.insert(x.resource_id, (x.name.clone(), x.tenant.clone()));
        }
        let bindings = configured_bindings.iter().chain(state.bindings_of_roles.iter()).cloned().collect();
        Access { grants: Grants { bindings }, configured: configured_bindings, tenants, resources }
    }

    /// Whether `p` may do `op` on the resource called `name` in `tenant`.
    pub(crate) fn may(&self, p: &Principal, op: Operation, tenant: &str, name: &str) -> bool {
        match p {
            Principal::Anonymous => true,
            // Its marker client looks up leaders; nothing else gets past
            // `check`.
            Principal::Node(_) => op == Operation::Describe,
            Principal::User { name: user, groups } => self.grants.allows(&Who { name: user, groups }, op, tenant, name),
        }
    }

    /// Whether `p` may do `op` on resource `id`. A resource this node does
    /// not know is let through: the request then fails as unknown, the
    /// same for everyone.
    pub(crate) fn may_on(&self, p: &Principal, op: Operation, id: u64) -> bool {
        match self.resources.get(&id) {
            Some((name, tenant)) => self.may(p, op, tenant, name),
            None => true,
        }
    }

    /// Whether `p` may administer the cluster (users, tenants, bindings).
    pub(crate) fn cluster_admin(&self, p: &Principal) -> bool {
        self.may(p, Operation::ClusterAdmin, "", "")
    }

    /// Whether `p` may see what belongs to the whole cluster: its own
    /// groups and their metrics.
    pub(crate) fn sees_cluster(&self, p: &Principal) -> bool {
        match p {
            Principal::Anonymous => true,
            Principal::Node(_) => false,
            Principal::User { name, groups } => self.grants.sees_cluster(&Who { name, groups }),
        }
    }

    /// Whether `p` may see that `tenant` exists.
    pub(crate) fn knows_tenant(&self, p: &Principal, tenant: &str) -> bool {
        match p {
            Principal::Anonymous => true,
            Principal::Node(_) => false,
            Principal::User { name, groups } => self.grants.knows_tenant(&Who { name, groups }, tenant),
        }
    }

    /// Whether `p` may send `req` at all; `Err` says why not. Requests that
    /// return many resources (`Metadata`, `DescribeMetrics`,
    /// `DescribeTenants`) are let through and filtered when answered.
    pub(crate) fn check(&self, p: &Principal, req: &Request) -> Result<(), String> {
        use Operation::*;
        // A node acts as a client only to look up leaders and to write
        // transaction markers; and only nodes write markers.
        match (p, req) {
            (_, Request::ApiVersions | Request::WhoAmI(_) | Request::Metadata(_) | Request::FindCoordinator(_)) => {
                return Ok(());
            }
            (
                Principal::Node(_) | Principal::Anonymous,
                Request::WriteTxnMarkers(_)
                | Request::SettleVoters(_)
                | Request::DeadLetter(_)
                | Request::VerifyTxn(_)
                | Request::CheckEmpty(_),
            ) => {
                return Ok(());
            }
            // A lagging node asks for the node list (ADR 0050); the metadata
            // leader asks for the nodes' metrics to weigh moves (ADR 0052).
            (Principal::Node(_), Request::DescribeNodes(_) | Request::DescribeMetrics(_)) => return Ok(()),
            (_, Request::WriteTxnMarkers(_)) => return Err("only broker nodes write transaction markers".into()),
            (_, Request::SettleVoters(_)) => return Err("only broker nodes report a group's voters".into()),
            (_, Request::DeadLetter(_)) => return Err("only broker nodes forward dead letters".into()),
            (_, Request::VerifyTxn(_)) => return Err("only broker nodes verify transactions".into()),
            (_, Request::CheckEmpty(_)) => return Err("only broker nodes ask whether a partition is empty".into()),
            (Principal::Node(id), _) => {
                return Err(format!(
                    "node {id} may only look up metadata, nodes and metrics, write markers, report voters, forward dead letters, verify transactions and check partitions for a delete"
                ));
            }
            (Principal::Anonymous, _) => return Ok(()),
            _ => {}
        }
        let on = |op: Operation, ids: &mut dyn Iterator<Item = u64>| -> Result<(), String> {
            for id in ids {
                if !self.may_on(p, op, id) {
                    let (name, tenant) = self.resources.get(&id).cloned().unwrap_or_default();
                    return Err(format!("{p} has no role that allows {} on {name} in tenant {tenant}", verb(op)));
                }
            }
            Ok(())
        };
        let admin = || if self.cluster_admin(p) { Ok(()) } else { Err(format!("{p} is not a cluster admin")) };
        // A transactional id is the producer's and a group id the
        // consumer's, in the tenant the request names, with the binding's
        // prefix matched against the id (ADR 0062).
        let txn = |tenant: &str, id: &str| self.id(p, Write, tenant, id, "the transactional id");
        let group = |tenant: &str, id: &str| self.id(p, Read, tenant, id, "the group id");
        match req {
            Request::Produce(r) => {
                if let Some(id) = &r.txn_id {
                    txn(&r.tenant, id)?;
                }
                on(Write, &mut r.resources.iter().map(|x| x.resource_id))
            }
            Request::Fetch(r) => on(Read, &mut r.resources.iter().map(|x| x.resource_id)),
            Request::ListOffsets(r) => on(Read, &mut r.resources.iter().map(|x| x.resource_id)),
            Request::AddPartitionsToTxn(r) => {
                txn(&r.tenant, &r.txn_id)?;
                on(Write, &mut r.resources.iter().map(|x| x.resource_id))
            }
            // A plain producer id grants nothing by itself: writing needs
            // the role per resource.
            Request::InitProducer(r) => match &r.txn_id {
                Some(id) => txn(&r.tenant, id),
                None => Ok(()),
            },
            Request::EndTxn(r) => txn(&r.tenant, &r.txn_id),
            Request::Subscribe(r) => on(Read, &mut std::iter::once(r.queue_id)),
            Request::Ack(r) => on(Read, &mut std::iter::once(r.queue_id)),
            Request::Nack(r) => on(Read, &mut std::iter::once(r.queue_id)),
            // Only ever about the connection's own subscriptions.
            Request::Credit(_) | Request::Unsubscribe(_) => Ok(()),
            Request::GroupHeartbeat(r) => {
                group(&r.tenant, &r.group_id)?;
                let subscribed = r.subscribed.iter().flatten().copied();
                let owned = r.owned.iter().flatten().map(|t| t.resource_id);
                on(Read, &mut subscribed.chain(owned))
            }
            Request::CommitOffsets(r) => {
                group(&r.tenant, &r.group_id)?;
                on(Read, &mut r.resources.iter().map(|x| x.resource_id))
            }
            Request::AddOffsetsToTxn(r) => {
                txn(&r.tenant, &r.txn_id)?;
                group(&r.tenant, &r.group_id)?;
                on(Read, &mut r.resources.iter().map(|x| x.resource_id))
            }
            Request::OffsetFetch(r) => {
                group(&r.tenant, &r.group_id)?;
                on(Read, &mut r.resources.iter().map(|x| x.resource_id))
            }
            Request::CreateStream(r) => self.create(p, &r.tenant, &r.name),
            Request::CreateQueue(r) => self.create(p, &r.tenant, &r.name),
            Request::DeclareExchange(r) => self.create(p, &r.tenant, &r.name),
            Request::Bind(r) | Request::Unbind(r) => on(Admin, &mut [r.exchange_id, r.queue_id].into_iter()),
            Request::DeleteResource(r) => on(Admin, &mut std::iter::once(r.resource_id)),
            // Destructive or moving: the tenant's admins (ADR 0064).
            Request::PurgeQueue(r) | Request::Requeue(r) => on(Admin, &mut std::iter::once(r.queue_id)),
            Request::AlterConfigs(r) => on(Admin, &mut std::iter::once(r.resource_id)),
            Request::DescribeConfigs(r) => on(Describe, &mut r.resource_ids.iter().copied()),
            Request::DescribeMetrics(_) | Request::DescribeTenants(_) => Ok(()),
            Request::AlterUser(_)
            | Request::DescribeUsers(_)
            | Request::CreateTenant(_)
            | Request::DeleteTenant(_)
            | Request::AlterRoleBindings(_)
            | Request::DescribeRoleBindings(_)
            | Request::AddNode(_)
            | Request::RemoveNode(_) => admin(),
            Request::DescribeNodes(_) => {
                if self.sees_cluster(p) {
                    Ok(())
                } else {
                    Err(format!("{p} may not see the cluster's nodes"))
                }
            }
            Request::ApiVersions
            | Request::WhoAmI(_)
            | Request::Metadata(_)
            | Request::FindCoordinator(_)
            | Request::WriteTxnMarkers(_)
            | Request::SettleVoters(_)
            | Request::DeadLetter(_)
            | Request::VerifyTxn(_)
            | Request::CheckEmpty(_)
            | Request::SaslHandshake(_)
            | Request::SaslAuthenticate(_) => Ok(()),
        }
    }

    /// Whether `p` may do `op` with the transactional or group `id` in
    /// `tenant`, which must exist.
    fn id(&self, p: &Principal, op: Operation, tenant: &str, id: &str, what: &str) -> Result<(), String> {
        if !self.tenants.contains_key(tenant) || !self.may(p, op, tenant, id) {
            return Err(format!("{p} has no role that allows using {what} {id} in tenant {tenant}"));
        }
        Ok(())
    }

    fn create(&self, p: &Principal, tenant: &str, name: &str) -> Result<(), String> {
        if !self.may(p, Operation::Admin, tenant, name) {
            return Err(format!("{p} has no role that allows creating {name} in tenant {tenant}"));
        }
        Ok(())
    }
}

fn verb(op: Operation) -> &'static str {
    match op {
        Operation::Describe => "seeing",
        Operation::Read => "reading",
        Operation::Write => "writing",
        Operation::Admin => "changing",
        Operation::ClusterAdmin => "administering the cluster",
    }
}

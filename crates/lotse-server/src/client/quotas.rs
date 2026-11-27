//! Storage quotas (ADR 0147): `AlterQuota` and `DescribeQuotas`, served by
//! the metadata group's leader, which holds the applied limits and the
//! usage of its last count.
//!
//! A cluster admin sets a tenant's own limit (the empty prefix) and any
//! prefix limit; a tenant admin sets prefix limits in their tenant under
//! the prefix of their binding, and never changes one a cluster admin set.
//! A tenant a principal holds no role in is unknown to it (ADR 0039).

use std::collections::BTreeMap;
use std::sync::Arc;

use lotse_auth::roles::Operation;
use lotse_proto::{AlterQuotaRequest, DescribeQuotasRequest, DescribeQuotasResponse, ErrorCode, QuotaEntry, Response};
use lotse_rt::Runtime;

use super::admin::{AdminThen, admin_response};
use super::{Clients, ClusterView, Outbox};
use crate::access::Access;
use crate::auth::Principal;
use crate::metadata::{Change, MAX_PREFIX_QUOTAS, MetadataState};
use crate::replica::{Replica, StateMachine};
use crate::wire::GroupId;

type Groups<R, S> = BTreeMap<GroupId, Replica<R, S>>;

/// The record an `AlterQuota` from `who` calls for, or why it is refused,
/// judged against the applied state.
pub(crate) fn alter_quota_change(
    access: &Access,
    state: &MetadataState,
    who: &Principal,
    req: &AlterQuotaRequest,
) -> Result<Change, (ErrorCode, String)> {
    if !state.writes(crate::version::QUOTAS_SINCE) {
        let why = format!(
            "storage quotas need cluster version {}; raise it with `lotse upgrade finalize`",
            crate::version::display(crate::version::QUOTAS_SINCE)
        );
        return Err((ErrorCode::UNSUPPORTED_VERSION, why));
    }
    if !access.tenants.contains_key(&req.tenant) || !access.knows_tenant(who, &req.tenant) {
        return Err((ErrorCode::UNKNOWN_RESOURCE, format!("there is no tenant {:?}", req.tenant)));
    }
    if req.bytes == Some(0) {
        return Err((ErrorCode::INVALID_REQUEST, "a limit is at least 1 byte; leave it out to remove one".into()));
    }
    let admin = access.cluster_admin(who);
    if req.prefix.is_empty() && !admin {
        return Err((ErrorCode::UNAUTHORIZED, format!("{who} may not set a tenant's own limit; a cluster admin does")));
    }
    if !admin && !access.may(who, Operation::Admin, &req.tenant, &req.prefix) {
        let why = format!("{who} may not set a limit on {:?} in tenant {:?}", req.prefix, req.tenant);
        return Err((ErrorCode::UNAUTHORIZED, why));
    }
    let key = (req.tenant.clone(), req.prefix.clone());
    let existing = state.quotas.get(&key);
    if existing.is_some_and(|q| q.by_cluster_admin) && !admin {
        let why = "a cluster admin set this limit; only a cluster admin changes it".to_string();
        return Err((ErrorCode::UNAUTHORIZED, why));
    }
    if req.bytes.is_some() && existing.is_none() && !req.prefix.is_empty() {
        let held = state.quotas.keys().filter(|(t, p)| *t == req.tenant && !p.is_empty()).count();
        if held >= MAX_PREFIX_QUOTAS {
            let why = format!("a tenant holds at most {MAX_PREFIX_QUOTAS} prefix limits");
            return Err((ErrorCode::INVALID_REQUEST, why));
        }
    }
    Ok(Change::SetQuota {
        tenant: req.tenant.clone(),
        prefix: req.prefix.clone(),
        bytes: req.bytes,
        by_cluster_admin: admin,
    })
}

/// The answer to a `DescribeQuotas` from `who`: every limit of the tenants
/// it may see, and each such tenant without a limit of its own, with the
/// usage of the last count, `counted`, when there was one.
pub(crate) fn describe_quotas_answer(
    access: &Access,
    state: &MetadataState,
    who: &Principal,
    counted: Option<(u64, &crate::quota::Usage)>,
    req: &DescribeQuotasRequest,
) -> DescribeQuotasResponse {
    let visible = |t: &str| access.sees_cluster(who) || access.knows_tenant(who, t);
    if let Some(t) = &req.tenant
        && (!access.tenants.contains_key(t) || !visible(t))
    {
        return DescribeQuotasResponse { error: ErrorCode::UNKNOWN_RESOURCE, ..DescribeQuotasResponse::default() };
    }
    let mut quotas = Vec::new();
    for tenant in access.tenants.keys().filter(|t| visible(t)) {
        if req.tenant.as_ref().is_some_and(|t| t != tenant) {
            continue;
        }
        let own = (tenant.clone(), String::new());
        if !state.quotas.contains_key(&own) {
            quotas.push(QuotaEntry {
                tenant: tenant.clone(),
                prefix: String::new(),
                bytes: None,
                set_by_cluster_admin: false,
                full: false,
                used_bytes: counted.map(|(_, u)| u.per_tenant.get(tenant).copied().unwrap_or(0)),
            });
        }
        let limits = state.quotas.range(own.clone()..).take_while(|((t, _), _)| t == tenant);
        for ((t, p), q) in limits {
            quotas.push(QuotaEntry {
                tenant: t.clone(),
                prefix: p.clone(),
                bytes: Some(q.bytes),
                set_by_cluster_admin: q.by_cluster_admin,
                full: q.full,
                used_bytes: counted.map(|(_, u)| u.per_limit.get(&(t.clone(), p.clone())).copied().unwrap_or(0)),
            });
        }
    }
    DescribeQuotasResponse {
        error: ErrorCode::NONE,
        leader_hint: None,
        counted_at_ms: counted.map(|(at, _)| at),
        quotas,
    }
}

impl Clients {
    /// `AlterQuota`: proposed on the metadata leader, answered once applied.
    pub(super) fn alter_quota<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: Arc<Outbox>,
        id: u32,
        req: AlterQuotaRequest,
        who: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((code, h)) => return outbox.push(id, &admin_response(code, None, h)),
        };
        match alter_quota_change(view.access, replica.metadata(), who, &req) {
            Ok(change) => self.propose_admin(replica, view.now, &change, (outbox, id), AdminThen::Changed),
            Err((code, why)) => outbox.push(id, &admin_response(code, Some(&why), None)),
        }
    }

    /// `DescribeQuotas`: only the metadata leader counts usage, so any
    /// other node answers `NOT_LEADER`.
    pub(super) fn describe_quotas<R: Runtime, S: StateMachine>(
        &mut self,
        outbox: &Outbox,
        id: u32,
        req: DescribeQuotasRequest,
        who: &Principal,
        view: &ClusterView<'_>,
        groups: &mut Groups<R, S>,
    ) {
        let replica = match Self::metadata_leader(view, groups) {
            Ok(r) => r,
            Err((error, leader_hint)) => {
                let resp = DescribeQuotasResponse { error, leader_hint, ..DescribeQuotasResponse::default() };
                return outbox.push(id, &Response::DescribeQuotas(resp));
            }
        };
        let counted = view.node.quota_usage.map(|(at, u)| (*at, u));
        let resp = describe_quotas_answer(view.access, replica.metadata(), who, counted, &req);
        outbox.push(id, &Response::DescribeQuotas(resp));
    }
}

#[cfg(test)]
mod tests;

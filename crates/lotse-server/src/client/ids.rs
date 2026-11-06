//! Transactional and consumer group ids live in a tenant (ADR 0062).
//!
//! Requests name an id and, as a tag, its tenant. Once a request has passed
//! the access check, [`scope`] replaces each id with the coordinator's key
//! for it, so the coordinator's state, records and timers never see a bare
//! id and the same id in two tenants is two entries. The key of an id in
//! the default tenant is the id itself, as it was before ids were scoped,
//! so existing state keeps its meaning; any other tenant's is the tenant, a
//! NUL and the id. Ids may not hold NUL, so the last NUL of a key is where
//! the id starts and no two (tenant, id) pairs share a key.

use lotse_proto::{DEFAULT_TENANT, Request};

/// The coordinator's key for `id` in `tenant`.
pub(crate) fn key(tenant: &str, id: &str) -> String {
    if tenant == DEFAULT_TENANT { id.to_string() } else { format!("{tenant}\0{id}") }
}

/// The tenant and id a key stands for.
pub(crate) fn split(key: &str) -> (&str, &str) {
    match key.rfind('\0') {
        Some(i) => (&key[..i], &key[i + 1..]),
        None => (DEFAULT_TENANT, key),
    }
}

/// Puts the coordinator's keys in place of the ids `req` names. `Err` for
/// an id that could not be told apart from another tenant's.
pub(crate) fn scope(req: &mut Request) -> Result<(), String> {
    fn one(tenant: &str, id: &mut String) -> Result<(), String> {
        if id.contains('\0') {
            return Err("a transactional or group id may not contain NUL".into());
        }
        *id = key(tenant, id);
        Ok(())
    }
    match req {
        Request::Produce(r) => match &mut r.txn_id {
            Some(id) => one(&r.tenant, id),
            None => Ok(()),
        },
        Request::InitProducer(r) => match &mut r.txn_id {
            Some(id) => one(&r.tenant, id),
            None => Ok(()),
        },
        Request::AddPartitionsToTxn(r) => one(&r.tenant, &mut r.txn_id),
        Request::EndTxn(r) => one(&r.tenant, &mut r.txn_id),
        Request::VerifyTxn(r) => one(&r.tenant, &mut r.txn_id),
        Request::GroupHeartbeat(r) => one(&r.tenant, &mut r.group_id),
        Request::CommitOffsets(r) => one(&r.tenant, &mut r.group_id),
        Request::OffsetFetch(r) => one(&r.tenant, &mut r.group_id),
        Request::AddOffsetsToTxn(r) => {
            one(&r.tenant, &mut r.txn_id)?;
            one(&r.tenant, &mut r.group_id)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_tell_tenants_apart_and_split_back() {
        assert_eq!(key(DEFAULT_TENANT, "payments"), "payments", "the default tenant's keys are as before");
        let acme = key("acme", "payments");
        assert_ne!(acme, key(DEFAULT_TENANT, "payments"));
        assert_ne!(acme, key("acme2", "payments"));
        assert_eq!(split(&acme), ("acme", "payments"));
        assert_eq!(split("payments"), (DEFAULT_TENANT, "payments"));
        assert_eq!(split(&key("acme", "")), ("acme", ""));
    }

    #[test]
    fn an_id_with_nul_is_refused() {
        let mut r = Request::EndTxn(lotse_proto::EndTxnRequest {
            tenant: DEFAULT_TENANT.into(),
            txn_id: "acme\0payments".into(),
            producer_id: 1,
            producer_epoch: 0,
            commit: true,
        });
        assert!(scope(&mut r).is_err(), "it would be acme's payments");
    }
}

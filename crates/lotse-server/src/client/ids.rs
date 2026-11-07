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
//!
//! A tenant created at runtime since ADR 0069 has an incarnation, the index
//! of the record that created it, and its keys carry it: a NUL, the
//! incarnation in decimal, a NUL, the tenant, a NUL and the id. A tenant
//! deleted and created again is a new incarnation and starts without the
//! old one's ids, which the coordinator leader then forgets
//! ([`doomed`]). Tenants with an incarnation cannot have NUL in their
//! names, so such a key cannot be read as an older one's.

use lotse_proto::{DEFAULT_TENANT, Request};

use crate::access::Access;

/// The coordinator's key for `id` in incarnation `incarnation` of `tenant`.
pub(crate) fn key(tenant: &str, incarnation: u64, id: &str) -> String {
    match (incarnation, tenant == DEFAULT_TENANT) {
        (0, true) => id.to_string(),
        (0, false) => format!("{tenant}\0{id}"),
        (i, _) => format!("\0{i}\0{tenant}\0{id}"),
    }
}

/// The tenant, its incarnation and the id a key stands for.
pub(crate) fn parse(key: &str) -> (&str, u64, &str) {
    if let Some(rest) = key.strip_prefix('\0')
        && let Some((digits, rest)) = rest.split_once('\0')
        && let Ok(i) = digits.parse::<u64>()
        && i != 0
        && let Some((tenant, id)) = rest.rsplit_once('\0')
    {
        return (tenant, i, id);
    }
    match key.rfind('\0') {
        Some(i) => (&key[..i], 0, &key[i + 1..]),
        None => (DEFAULT_TENANT, 0, key),
    }
}

/// The tenant and id a key stands for.
pub(crate) fn split(key: &str) -> (&str, &str) {
    let (tenant, _, id) = parse(key);
    (tenant, id)
}

/// Whether the state under `key` belongs to an incarnation of its tenant
/// that is gone: deleted, or replaced by a newer one. A tenant `access`
/// does not know at all is not taken for gone: this node may not have
/// applied its creation yet.
pub(crate) fn doomed(key: &str, access: &Access) -> bool {
    let (tenant, incarnation, _) = parse(key);
    if access.tenants.contains_key(tenant) {
        return incarnation < access.incarnation(tenant);
    }
    access.gone.get(tenant).is_some_and(|&dead| incarnation <= dead)
}

/// Puts the coordinator's keys in place of the ids `req` names, for the
/// tenants' incarnations `access` knows. `Err` for an id that could not be
/// told apart from another tenant's.
pub(crate) fn scope(req: &mut Request, access: &Access) -> Result<(), String> {
    let one = |tenant: &str, id: &mut String| -> Result<(), String> {
        if id.contains('\0') {
            return Err("a transactional or group id may not contain NUL".into());
        }
        *id = key(tenant, access.incarnation(tenant), id);
        Ok(())
    };
    match req {
        Request::Produce(r) => match &mut r.txn_id {
            Some(id) => one(&r.tenant, id),
            None => Ok(()),
        },
        Request::InitProducer(r) => match &mut r.txn_id {
            Some(id) => one(&r.tenant, id),
            None => Ok(()),
        },
        Request::Ack(r) => match &mut r.txn_id {
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
        assert_eq!(key(DEFAULT_TENANT, 0, "payments"), "payments", "the default tenant's keys are as before");
        let acme = key("acme", 0, "payments");
        assert_ne!(acme, key(DEFAULT_TENANT, 0, "payments"));
        assert_ne!(acme, key("acme2", 0, "payments"));
        assert_eq!(split(&acme), ("acme", "payments"));
        assert_eq!(split("payments"), (DEFAULT_TENANT, "payments"));
        assert_eq!(split(&key("acme", 0, "")), ("acme", ""));
    }

    #[test]
    fn incarnations_are_new_keys() {
        let (old, new) = (key("acme", 0, "payments"), key("acme", 41, "payments"));
        assert_ne!(old, new);
        assert_ne!(new, key("acme", 42, "payments"));
        assert_eq!(parse(&new), ("acme", 41, "payments"));
        assert_eq!(parse(&old), ("acme", 0, "payments"));
        assert_eq!(parse(&key("acme", 7, "")), ("acme", 7, ""));
        assert_eq!(split(&new), ("acme", "payments"), "metrics name the tenant and the id");
    }

    #[test]
    fn only_a_gone_incarnation_is_doomed() {
        let mut access = Access::default();
        access.tenants.insert("acme".into(), lotse_proto::config_source::DYNAMIC);
        access.incarnations.insert("acme".into(), 41);
        access.gone.insert("shop".into(), 9);
        assert!(doomed(&key("acme", 0, "a"), &access), "from before acme was created again");
        assert!(doomed(&key("acme", 40, "a"), &access));
        assert!(!doomed(&key("acme", 41, "a"), &access), "the live incarnation");
        assert!(!doomed(&key("acme", 42, "a"), &access), "newer than this node knows: kept");
        assert!(doomed(&key("shop", 9, "a"), &access) && doomed(&key("shop", 0, "a"), &access), "deleted");
        assert!(!doomed(&key("shop", 10, "a"), &access), "created again elsewhere already");
        assert!(!doomed(&key("mall", 3, "a"), &access), "unknown here: kept");
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
        assert!(scope(&mut r, &Access::default()).is_err(), "it would be acme's payments");
    }
}

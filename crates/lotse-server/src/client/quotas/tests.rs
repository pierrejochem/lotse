use lotse_auth::roles::{Role, RoleBinding, Subject};

use super::*;
use crate::metadata::record_payload;

fn binding(user: &str, role: Role, tenant: &str, prefix: &str) -> RoleBinding {
    RoleBinding { subject: Subject::User(user.into()), role, tenant: tenant.into(), prefix: prefix.into() }
}

/// Tenants `shop` and `lab` at cluster version 0.2; `ta` administers
/// `shop`'s `orders.` names, `v` views `shop`, `cv` views the cluster, and
/// `root` is a bootstrap cluster admin.
fn world(version: u32) -> (MetadataState, Access) {
    let mut s = MetadataState::default();
    let changes = [
        Change::SetClusterVersion { version },
        Change::CreateTenant { name: "shop".into(), incarnated: true },
        Change::CreateTenant { name: "lab".into(), incarnated: true },
        Change::SetBindings {
            add: vec![
                binding("ta", Role::TenantAdmin, "shop", "orders."),
                binding("whole", Role::TenantAdmin, "shop", ""),
                binding("v", Role::Viewer, "shop", ""),
                binding("cv", Role::ClusterViewer, "", ""),
            ],
            remove: vec![],
        },
    ];
    for (i, c) in changes.iter().enumerate() {
        s.apply(i as u64 + 1, &record_payload(c)).unwrap();
    }
    let auth = crate::AuthConfig { required: true, admins: vec!["root".into()], ..crate::AuthConfig::none() };
    let access = Access::build(&auth, &[], &s);
    (s, access)
}

fn user(name: &str) -> Principal {
    Principal::User { name: name.into(), groups: vec![] }
}

fn alter(tenant: &str, prefix: &str, bytes: Option<u64>) -> AlterQuotaRequest {
    AlterQuotaRequest { tenant: tenant.into(), prefix: prefix.into(), bytes }
}

fn apply(s: &mut MetadataState, at: u64, change: &Change) {
    s.apply(at, &record_payload(change)).unwrap();
}

#[test]
fn a_cluster_admin_sets_and_removes_a_tenant_limit() {
    let (mut s, access) = world(crate::version::QUOTAS_SINCE);
    let set = alter_quota_change(&access, &s, &user("root"), &alter("shop", "", Some(1 << 30))).unwrap();
    assert_eq!(
        set,
        Change::SetQuota { tenant: "shop".into(), prefix: String::new(), bytes: Some(1 << 30), by_cluster_admin: true }
    );
    apply(&mut s, 10, &set);
    let remove = alter_quota_change(&access, &s, &user("root"), &alter("shop", "", None)).unwrap();
    apply(&mut s, 11, &remove);
    assert!(s.quotas.is_empty());
}

#[test]
fn a_tenant_admin_cannot_set_the_tenant_limit() {
    let (s, access) = world(crate::version::QUOTAS_SINCE);
    let (code, why) = alter_quota_change(&access, &s, &user("ta"), &alter("shop", "", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNAUTHORIZED, "{why}");
    // Not even one who administers the whole tenant: the limit is the
    // platform's.
    let (code, why) = alter_quota_change(&access, &s, &user("whole"), &alter("shop", "", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNAUTHORIZED, "{why}");
    assert!(alter_quota_change(&access, &s, &user("whole"), &alter("shop", "any.", Some(1))).is_ok());
}

#[test]
fn a_tenant_admin_sets_a_prefix_limit_under_their_binding() {
    let (s, access) = world(crate::version::QUOTAS_SINCE);
    let ok = alter_quota_change(&access, &s, &user("ta"), &alter("shop", "orders.eu.", Some(500))).unwrap();
    assert_eq!(
        ok,
        Change::SetQuota {
            tenant: "shop".into(),
            prefix: "orders.eu.".into(),
            bytes: Some(500),
            by_cluster_admin: false
        }
    );
    for prefix in ["billing.", "ord"] {
        let (code, _) = alter_quota_change(&access, &s, &user("ta"), &alter("shop", prefix, Some(1))).unwrap_err();
        assert_eq!(code, ErrorCode::UNAUTHORIZED, "{prefix} is not under orders.");
    }
    let (code, _) = alter_quota_change(&access, &s, &user("v"), &alter("shop", "orders.", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNAUTHORIZED, "a viewer sets nothing");
}

#[test]
fn a_tenant_admin_cannot_change_a_cluster_admins_prefix_limit() {
    let (mut s, access) = world(crate::version::QUOTAS_SINCE);
    let set = alter_quota_change(&access, &s, &user("root"), &alter("shop", "orders.", Some(500))).unwrap();
    apply(&mut s, 10, &set);
    for bytes in [Some(10_000), None] {
        let (code, why) = alter_quota_change(&access, &s, &user("ta"), &alter("shop", "orders.", bytes)).unwrap_err();
        assert_eq!(code, ErrorCode::UNAUTHORIZED);
        assert!(why.contains("cluster admin"), "{why}");
    }
    // One a tenant admin set, a tenant admin changes.
    let mine = alter_quota_change(&access, &s, &user("ta"), &alter("shop", "orders.eu.", Some(5))).unwrap();
    apply(&mut s, 11, &mine);
    assert!(alter_quota_change(&access, &s, &user("ta"), &alter("shop", "orders.eu.", None)).is_ok());
}

#[test]
fn a_zero_limit_is_refused() {
    let (s, access) = world(crate::version::QUOTAS_SINCE);
    let (code, _) = alter_quota_change(&access, &s, &user("root"), &alter("shop", "", Some(0))).unwrap_err();
    assert_eq!(code, ErrorCode::INVALID_REQUEST);
}

#[test]
fn the_101st_prefix_limit_is_refused() {
    let (mut s, access) = world(crate::version::QUOTAS_SINCE);
    for i in 0..crate::metadata::MAX_PREFIX_QUOTAS {
        let c = alter_quota_change(&access, &s, &user("root"), &alter("shop", &format!("p{i}."), Some(1))).unwrap();
        apply(&mut s, 10 + i as u64, &c);
    }
    let (code, why) = alter_quota_change(&access, &s, &user("root"), &alter("shop", "extra.", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::INVALID_REQUEST);
    assert!(why.contains("100"), "{why}");
    assert!(alter_quota_change(&access, &s, &user("root"), &alter("shop", "", Some(1))).is_ok(), "the tenant's own");
    assert!(alter_quota_change(&access, &s, &user("root"), &alter("shop", "p0.", Some(2))).is_ok(), "an existing one");
}

#[test]
fn quotas_need_0_2() {
    let (s, access) = world(crate::version::TOKENS_SINCE);
    let (code, why) = alter_quota_change(&access, &s, &user("root"), &alter("shop", "", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNSUPPORTED_VERSION);
    assert!(why.contains("0.2"), "{why}");
}

#[test]
fn an_unknown_or_invisible_tenant_is_unknown() {
    let (s, access) = world(crate::version::QUOTAS_SINCE);
    let (code, _) = alter_quota_change(&access, &s, &user("root"), &alter("nowhere", "", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNKNOWN_RESOURCE);
    let (code, _) = alter_quota_change(&access, &s, &user("ta"), &alter("lab", "x.", Some(1))).unwrap_err();
    assert_eq!(code, ErrorCode::UNKNOWN_RESOURCE, "ta holds no role in lab");
}

#[test]
fn describe_shows_only_visible_tenants() {
    let (mut s, access) = world(crate::version::QUOTAS_SINCE);
    for (i, (t, p, b)) in [("shop", "", 1_000u64), ("shop", "orders.", 500), ("lab", "", 50)].iter().enumerate() {
        let c = alter_quota_change(&access, &s, &user("root"), &alter(t, p, Some(*b))).unwrap();
        apply(&mut s, 10 + i as u64, &c);
    }
    apply(&mut s, 20, &Change::QuotaFull { tenant: "shop".into(), prefix: "orders.".into(), full: true });
    let usage = crate::quota::Usage {
        per_limit: BTreeMap::from([
            (("shop".into(), String::new()), 700),
            (("shop".into(), "orders.".into()), 600),
            (("lab".into(), String::new()), 0),
        ]),
        per_tenant: BTreeMap::from([("shop".into(), 700)]),
    };
    let counted = Some((1_234, &usage));
    let all = describe_quotas_answer(&access, &s, &user("cv"), counted, &DescribeQuotasRequest { tenant: None });
    let keys: Vec<(&str, &str)> = all.quotas.iter().map(|q| (q.tenant.as_str(), q.prefix.as_str())).collect();
    assert!(keys.contains(&("lab", "")) && keys.contains(&("shop", "orders.")), "{keys:?}");
    assert_eq!(all.counted_at_ms, Some(1_234));
    let shop = describe_quotas_answer(&access, &s, &user("v"), counted, &DescribeQuotasRequest { tenant: None });
    assert!(shop.quotas.iter().all(|q| q.tenant == "shop"), "{:?}", shop.quotas);
    let orders = shop.quotas.iter().find(|q| q.prefix == "orders.").unwrap();
    assert_eq!(
        (orders.bytes, orders.full, orders.used_bytes, orders.set_by_cluster_admin),
        (Some(500), true, Some(600), true)
    );
    let lab =
        describe_quotas_answer(&access, &s, &user("v"), counted, &DescribeQuotasRequest { tenant: Some("lab".into()) });
    assert_eq!((lab.error, lab.quotas.len()), (ErrorCode::UNKNOWN_RESOURCE, 0), "v holds no role in lab");
}

#[test]
fn describe_before_the_first_count_has_no_usage() {
    let (mut s, access) = world(crate::version::QUOTAS_SINCE);
    let c = alter_quota_change(&access, &s, &user("root"), &alter("shop", "", Some(1_000))).unwrap();
    apply(&mut s, 10, &c);
    let r = describe_quotas_answer(
        &access,
        &s,
        &user("root"),
        None,
        &DescribeQuotasRequest { tenant: Some("shop".into()) },
    );
    assert_eq!(r.counted_at_ms, None);
    assert!(r.quotas.iter().all(|q| q.used_bytes.is_none()), "{:?}", r.quotas);
    // A tenant without a limit is listed too, so an admin can pick one.
    let lab =
        describe_quotas_answer(&access, &s, &user("root"), None, &DescribeQuotasRequest { tenant: Some("lab".into()) });
    assert_eq!(lab.quotas.len(), 1);
    assert_eq!((lab.quotas[0].prefix.as_str(), lab.quotas[0].bytes), ("", None));
}

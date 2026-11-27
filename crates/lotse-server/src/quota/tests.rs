use super::*;
use crate::metadata::{Change, MetadataState, StreamSettings, record_payload};

/// A state at cluster version 0.2 with tenants `a` and `b`.
fn state() -> (MetadataState, u64) {
    let mut s = MetadataState::default();
    s.apply(1, &record_payload(&Change::SetClusterVersion { version: crate::version::QUOTAS_SINCE })).unwrap();
    s.apply(2, &record_payload(&Change::CreateTenant { name: "a".into(), incarnated: true })).unwrap();
    s.apply(3, &record_payload(&Change::CreateTenant { name: "b".into(), incarnated: true })).unwrap();
    (s, 4)
}

/// Creates a stream with one voter set per partition; returns its groups.
fn stream(s: &mut MetadataState, at: &mut u64, tenant: &str, name: &str, voters: &[&[u64]]) -> Vec<GroupId> {
    let create = Change::CreateStream {
        name: name.into(),
        tenant: tenant.into(),
        partitions: voters.iter().map(|v| v.to_vec()).collect(),
        settings: StreamSettings::default(),
    };
    s.apply(*at, &record_payload(&create)).unwrap();
    let id = crate::metadata::resource_id(*at);
    *at += 1;
    s.streams[&id].partitions.iter().map(|(g, _)| *g).collect()
}

fn limit(s: &mut MetadataState, at: &mut u64, tenant: &str, prefix: &str, bytes: u64) {
    let change =
        Change::SetQuota { tenant: tenant.into(), prefix: prefix.into(), bytes: Some(bytes), by_cluster_admin: true };
    s.apply(*at, &record_payload(&change)).unwrap();
    *at += 1;
}

fn key(tenant: &str, prefix: &str) -> (String, String) {
    (tenant.into(), prefix.into())
}

#[test]
fn every_listed_voter_counts() {
    let (mut s, mut at) = state();
    let groups = stream(&mut s, &mut at, "a", "s", &[&[1, 2, 3], &[1, 2, 3]]);
    limit(&mut s, &mut at, "a", "", 10_000);
    let sizes = BTreeMap::from([(groups[0], 100), (groups[1], 50)]);
    let usage = count(&s, &sizes).expect("every group has a size");
    assert_eq!(usage.per_limit[&key("a", "")], 450, "each partition's size times its 3 voters");
    assert_eq!(usage.per_tenant["a"], 450);
}

#[test]
fn a_name_counts_against_every_matching_prefix() {
    let (mut s, mut at) = state();
    let eu = stream(&mut s, &mut at, "a", "orders.eu.x", &[&[1]]);
    let other = stream(&mut s, &mut at, "a", "ordersx", &[&[1, 2]]);
    limit(&mut s, &mut at, "a", "", 10_000);
    limit(&mut s, &mut at, "a", "orders.", 10_000);
    limit(&mut s, &mut at, "a", "orders.eu.", 10_000);
    let sizes = BTreeMap::from([(eu[0], 10), (other[0], 7)]);
    let usage = count(&s, &sizes).unwrap();
    assert_eq!(usage.per_limit[&key("a", "orders.eu.")], 10);
    assert_eq!(usage.per_limit[&key("a", "orders.")], 10, "orders.eu.x is under orders. too; ordersx is not");
    assert_eq!(usage.per_limit[&key("a", "")], 24, "the tenant counts both");
}

#[test]
fn other_tenants_and_the_cluster_groups_do_not_count() {
    let (mut s, mut at) = state();
    let mine = stream(&mut s, &mut at, "a", "s", &[&[1]]);
    let theirs = stream(&mut s, &mut at, "b", "s2", &[&[1, 2, 3]]);
    limit(&mut s, &mut at, "a", "", 10_000);
    // Groups 1 and 2 stand for the metadata and coordinator groups: no
    // resource holds them.
    let sizes = BTreeMap::from([(mine[0], 5), (theirs[0], 1_000), (1, 1 << 30), (2, 1 << 30)]);
    let usage = count(&s, &sizes).unwrap();
    assert_eq!(usage.per_limit[&key("a", "")], 5);
    assert_eq!(usage.per_tenant["b"], 3_000, "a tenant without a limit is counted too");
}

#[test]
fn a_group_without_a_size_means_no_count() {
    let (mut s, mut at) = state();
    let groups = stream(&mut s, &mut at, "a", "s", &[&[1], &[1]]);
    let sizes = BTreeMap::from([(groups[0], 5)]);
    assert!(count(&s, &sizes).is_none(), "partition 1 has no size yet");
}

#[test]
fn full_at_the_limit_and_clear_below_95_percent() {
    let (mut s, mut at) = state();
    let g = stream(&mut s, &mut at, "a", "s", &[&[1]])[0];
    limit(&mut s, &mut at, "a", "", 1_000);
    let step = |s: &mut MetadataState, at: &mut u64, used: u64| -> Vec<Change> {
        let usage = count(s, &BTreeMap::from([(g, used)])).unwrap();
        let out = changes(s, &usage);
        for c in &out {
            s.apply(*at, &record_payload(c)).unwrap();
            *at += 1;
        }
        out
    };
    let full = Change::QuotaFull { tenant: "a".into(), prefix: String::new(), full: true };
    let clear = Change::QuotaFull { tenant: "a".into(), prefix: String::new(), full: false };
    assert_eq!(step(&mut s, &mut at, 999), vec![]);
    assert_eq!(step(&mut s, &mut at, 1_000), vec![full.clone()]);
    assert_eq!(step(&mut s, &mut at, 1_200), vec![], "already full");
    assert_eq!(step(&mut s, &mut at, 951), vec![]);
    assert_eq!(step(&mut s, &mut at, 950), vec![], "950 is 95 %, not below it");
    assert_eq!(step(&mut s, &mut at, 949), vec![clear]);
    assert_eq!(step(&mut s, &mut at, 990), vec![], "below the limit, not full again");
}

#[test]
fn a_limit_below_usage_is_full_at_the_next_count() {
    let (mut s, mut at) = state();
    let g = stream(&mut s, &mut at, "a", "orders.s", &[&[1]])[0];
    limit(&mut s, &mut at, "a", "orders.", 1);
    let usage = count(&s, &BTreeMap::from([(g, 500)])).unwrap();
    assert_eq!(
        changes(&s, &usage),
        vec![Change::QuotaFull { tenant: "a".into(), prefix: "orders.".into(), full: true }]
    );
}

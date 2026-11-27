use super::*;

fn payload(change: &Change) -> Bytes {
    let rec = record(change);
    let batch = lotse_record::Batch::parse(&rec).unwrap().0;
    let Ok(Some(lotse_record::Control::Internal(lotse_record::InternalControl::Metadata, p))) =
        lotse_record::control(&batch)
    else {
        panic!("not a metadata record")
    };
    p
}

#[test]
fn changes_apply_once_per_name_and_round_trip() {
    let mut s = MetadataState::default();
    let stream = Change::CreateStream {
        name: "orders".into(),
        tenant: "t".into(),
        partitions: vec![vec![1, 2, 3], vec![2, 3, 4]],
        settings: StreamSettings::default(),
    };
    s.apply(10, &payload(&stream)).unwrap();
    assert_eq!(s.id_of("orders"), Some(resource_id(10)));
    assert_eq!(s.streams[&resource_id(10)].partitions[1], (group_id(10, 1), vec![2, 3, 4]));
    // The same name again, from a concurrent create: nothing changes.
    s.apply(
        11,
        &payload(&Change::CreateQueue {
            name: "orders".into(),
            tenant: "t".into(),
            voters: vec![1],
            ack_timeout_ms: 5,
            limits: QueueLimits::default(),
        }),
    )
    .unwrap();
    assert!(s.queues.is_empty());
    s.apply(
        12,
        &payload(&Change::CreateQueue {
            name: "jobs".into(),
            tenant: "t".into(),
            voters: vec![1, 4, 5],
            ack_timeout_ms: 5,
            limits: QueueLimits::default(),
        }),
    )
    .unwrap();
    s.apply(13, &payload(&Change::DeclareExchange { name: "x".into(), tenant: "t".into(), kind: ExchangeKind::Topic }))
        .unwrap();
    let b = Binding {
        queue_id: resource_id(12),
        key: "a.#".into(),
        headers: vec![(Bytes::from_static(b"h"), None)],
        match_all: false,
    };
    let bind = Change::Bind { exchange_id: resource_id(13), binding: b.clone() };
    s.apply(14, &payload(&bind)).unwrap();
    s.apply(15, &payload(&bind)).unwrap();
    assert_eq!(s.bindings[&resource_id(13)], vec![b.clone()], "a binding is there once");
    let groups: Vec<(GroupId, Vec<NodeId>, bool)> = s.groups().map(|(g, v, q)| (g, v.to_vec(), q)).collect();
    assert_eq!(groups.len(), 3);
    assert!(groups.contains(&(group_id(12, 0), vec![1, 4, 5], true)));

    let bytes = s.encode();
    assert_eq!(MetadataState::decode(&bytes).unwrap(), s);
    for cut in 0..bytes.len() {
        assert!(MetadataState::decode(&bytes[..cut]).is_err(), "cut at {cut}");
    }
    s.apply(16, &payload(&Change::Unbind { exchange_id: resource_id(13), binding: b })).unwrap();
    assert!(s.bindings[&resource_id(13)].is_empty());
}

#[test]
fn ids_never_collide() {
    assert_ne!(group_id(1, MAX_PARTITIONS - 1), group_id(2, 0));
    assert!(resource_id(1) >= DYNAMIC_BASE && group_id(1, 0) >= DYNAMIC_BASE);
}

#[test]
fn placement_spreads_groups_over_the_least_loaded_nodes() {
    let nodes = [1, 2, 3, 4, 5];
    let mut load = BTreeMap::from([(1, 2), (2, 0), (3, 0), (4, 1), (5, 0)]);
    let placed = place(&nodes, &mut load, 3, 2);
    assert_eq!(placed[0], vec![2, 3, 5], "the three with no groups");
    assert_eq!(placed[1], vec![2, 3, 4], "then the next least loaded");
    assert!(placed.iter().all(|v| v.len() == 3));
    let total: usize = load.values().sum();
    assert_eq!(total, 3 + 6);
}

#[test]
fn a_delete_leaves_a_tombstone_and_takes_its_bindings() {
    let mut s = MetadataState::default();
    s.apply(
        1,
        &payload(&Change::CreateStream {
            name: "s".into(),
            tenant: "t".into(),
            partitions: vec![vec![1], vec![2], vec![3]],
            settings: StreamSettings::default(),
        }),
    )
    .unwrap();
    s.apply(
        2,
        &payload(&Change::CreateQueue {
            name: "q".into(),
            tenant: "t".into(),
            voters: vec![1],
            ack_timeout_ms: 0,
            limits: QueueLimits::default(),
        }),
    )
    .unwrap();
    s.apply(3, &payload(&Change::DeclareExchange { name: "x".into(), tenant: "t".into(), kind: ExchangeKind::Fanout }))
        .unwrap();
    let to_q = Binding { queue_id: resource_id(2), key: String::new(), headers: vec![], match_all: true };
    // A configured exchange (id 9) holds a runtime binding to the queue too.
    for x in [resource_id(3), 9] {
        s.apply(4, &payload(&Change::Bind { exchange_id: x, binding: to_q.clone() })).unwrap();
    }
    s.apply(
        5,
        &payload(&Change::AlterQueue {
            resource_id: resource_id(2),
            change: QueueChange { ack_timeout_ms: Some(750), ..Default::default() },
        }),
    )
    .unwrap();
    assert_eq!(s.queues[&resource_id(2)].ack_timeout_ms, 750);

    s.apply(6, &payload(&Change::Delete { resource_id: resource_id(2) })).unwrap();
    assert!(s.queues.is_empty() && s.is_deleted(resource_id(2)));
    assert!(s.bindings.values().all(Vec::is_empty), "bindings to the queue are gone, on every exchange");
    // A bind and an alter that were admitted before the delete arrive after it.
    s.apply(7, &payload(&Change::Bind { exchange_id: resource_id(3), binding: to_q.clone() })).unwrap();
    s.apply(
        8,
        &payload(&Change::AlterQueue {
            resource_id: resource_id(2),
            change: QueueChange { ack_timeout_ms: Some(1), ..Default::default() },
        }),
    )
    .unwrap();
    assert!(s.bindings.values().all(Vec::is_empty) && s.queues.is_empty());

    s.apply(9, &payload(&Change::Delete { resource_id: resource_id(1) })).unwrap();
    s.apply(10, &payload(&Change::Delete { resource_id: resource_id(3) })).unwrap();
    // Deleting again, or something never created, changes nothing.
    let before = s.clone();
    s.apply(11, &payload(&Change::Delete { resource_id: resource_id(1) })).unwrap();
    s.apply(12, &payload(&Change::Delete { resource_id: 9 })).unwrap();
    assert_eq!(s, before);
    assert!(s.streams.is_empty() && s.exchanges.is_empty() && !s.bindings.contains_key(&resource_id(3)));
    assert_eq!(s.groups().count(), 0);
    let gone: Vec<GroupId> = s.deleted_groups().into_iter().collect();
    assert_eq!(gone, vec![group_id(1, 0), group_id(1, 1), group_id(1, 2), group_id(2, 0)]);
    // The name is free again, under a new id.
    s.apply(
        13,
        &payload(&Change::CreateQueue {
            name: "q".into(),
            tenant: "t".into(),
            voters: vec![1],
            ack_timeout_ms: 0,
            limits: QueueLimits::default(),
        }),
    )
    .unwrap();
    assert_eq!(s.id_of("q"), Some(resource_id(13)));

    let bytes = s.encode();
    assert_eq!(MetadataState::decode(&bytes).unwrap(), s);
    for cut in 0..bytes.len() {
        // A cut where an optional section begins reads as a snapshot from
        // before that section (the snapshot file's checksum catches it):
        // here, tombstones without the index that made them.
        assert!(
            MetadataState::decode(&bytes[..cut]).is_ok_and(|d| d != s) || MetadataState::decode(&bytes[..cut]).is_err(),
            "cut at {cut}"
        );
    }
}

#[test]
fn tenants_bindings_and_a_create_racing_a_delete() {
    use lotse_auth::roles::{Role, RoleBinding, Subject};
    let mut s = MetadataState::default();
    // A record from before incarnations: incarnation 0.
    s.apply(1, &payload(&Change::CreateTenant { name: "shop".into(), incarnated: false })).unwrap();
    assert!(s.incarnations.is_empty());
    let b = |role, tenant: &str| RoleBinding {
        subject: Subject::User("u".into()),
        role,
        tenant: tenant.into(),
        prefix: String::new(),
    };
    let set = Change::SetBindings { add: vec![b(Role::Producer, "shop"), b(Role::ClusterAdmin, "")], remove: vec![] };
    s.apply(2, &payload(&set)).unwrap();
    s.apply(
        3,
        &payload(&Change::CreateStream {
            name: "o".into(),
            tenant: "shop".into(),
            partitions: vec![vec![1]],
            settings: StreamSettings::default(),
        }),
    )
    .unwrap();
    // Not empty: the delete does nothing.
    s.apply(4, &payload(&Change::DeleteTenant { name: "shop".into() })).unwrap();
    assert!(s.tenants.contains("shop") && s.bindings_of_roles.len() == 2);
    s.apply(5, &payload(&Change::Delete { resource_id: resource_id(3) })).unwrap();
    s.apply(6, &payload(&Change::DeleteTenant { name: "shop".into() })).unwrap();
    assert!(!s.tenants.contains("shop"));
    assert_eq!(
        s.bindings_of_roles.iter().map(|x| x.role).collect::<Vec<_>>(),
        vec![Role::ClusterAdmin],
        "the tenant's bindings go"
    );
    // A create proposed before the delete, applied after it: nothing.
    s.apply(
        7,
        &payload(&Change::CreateQueue {
            name: "late".into(),
            tenant: "shop".into(),
            voters: vec![1],
            ack_timeout_ms: 0,
            limits: QueueLimits::default(),
        }),
    )
    .unwrap();
    assert_eq!(s.id_of("late"), None);
    // Created again: open again, as a new incarnation (ADR 0069).
    s.apply(8, &payload(&Change::CreateTenant { name: "shop".into(), incarnated: true })).unwrap();
    assert_eq!(s.incarnations.get("shop"), Some(&8), "the index of its creation");
    s.apply(
        9,
        &payload(&Change::DeclareExchange { name: "x2".into(), tenant: "shop".into(), kind: ExchangeKind::Fanout }),
    )
    .unwrap();
    assert_eq!(s.id_of("x2"), Some(resource_id(9)));
    // Removing a binding; and everything survives a snapshot.
    let unset = Change::SetBindings { add: vec![], remove: vec![b(Role::ClusterAdmin, "")] };
    s.apply(10, &payload(&unset)).unwrap();
    assert!(s.bindings_of_roles.is_empty());
    s.apply(11, &payload(&set)).unwrap();
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // Deleted, it keeps the incarnation it had, in snapshots too.
    s.apply(12, &payload(&Change::DeleteTenant { name: "shop".into() })).unwrap();
    s.apply(13, &payload(&Change::Delete { resource_id: resource_id(9) })).unwrap();
    s.apply(14, &payload(&Change::DeleteTenant { name: "shop".into() })).unwrap();
    assert!(s.deleted_tenants.contains("shop") && s.incarnations.get("shop") == Some(&8));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn stream_settings_change_key_by_key_and_survive_a_snapshot() {
    let mut s = MetadataState::default();
    let settings = StreamSettings { retention_ms: Some(60_000), retention_bytes: None, ..StreamSettings::default() };
    let create = Change::CreateStream { name: "o".into(), tenant: "t".into(), partitions: vec![vec![1]], settings };
    s.apply(4, &payload(&create)).unwrap();
    let id = resource_id(4);
    assert_eq!(s.streams[&id].settings, settings);
    // Two changes of different keys, proposed from the same state: both
    // hold, whichever applies last.
    let bytes = StreamChange { retention_bytes: Some(Some(1 << 20)), ..StreamChange::default() };
    let ms = StreamChange { retention_ms: Some(None), ..StreamChange::default() };
    s.apply(5, &payload(&Change::AlterStream { resource_id: id, change: bytes })).unwrap();
    let want =
        StreamSettings { retention_ms: Some(60_000), retention_bytes: Some(1 << 20), ..StreamSettings::default() };
    assert_eq!(s.streams[&id].settings, want, "a change of the size leaves the age as it was");
    s.apply(6, &payload(&Change::AlterStream { resource_id: id, change: ms })).unwrap();
    let want = StreamSettings { retention_ms: None, retention_bytes: Some(1 << 20), ..StreamSettings::default() };
    assert_eq!(s.streams[&id].settings, want);
    let unlimited = StreamChange { retention_ms: Some(Some(-1)), ..StreamChange::default() };
    s.apply(7, &payload(&Change::AlterStream { resource_id: id, change: unlimited })).unwrap();
    let want = StreamSettings { retention_ms: Some(-1), retention_bytes: Some(1 << 20), ..StreamSettings::default() };
    assert_eq!(s.streams[&id].settings, want);
    let defaults = crate::RetentionConfig { ms: Some(1_000), bytes: None, ..crate::RetentionConfig::keep() };
    assert_eq!(want.retention(defaults), crate::Retention::Stream { ms: None, bytes: Some(1 << 20), compact: None });
    assert_eq!(
        StreamSettings::default().retention(defaults),
        crate::Retention::Stream { ms: Some(1_000), bytes: None, compact: None }
    );
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn compaction_settings_change_and_survive_a_snapshot_and_old_records_still_read() {
    use crate::metadata::Cleanup;
    let mut s = MetadataState::default();
    let settings = StreamSettings { cleanup: Some(Cleanup::Compact), ..StreamSettings::default() };
    let create = Change::CreateStream { name: "c".into(), tenant: "t".into(), partitions: vec![vec![1]], settings };
    s.apply(4, &payload(&create)).unwrap();
    let id = resource_id(4);
    assert_eq!(s.streams[&id].settings, settings);
    let defaults = crate::RetentionConfig { ms: Some(1_000), bytes: Some(10), ..crate::RetentionConfig::keep() };
    assert_eq!(
        settings.retention(defaults),
        crate::Retention::Stream { ms: None, bytes: None, compact: Some(86_400_000) },
        "compact alone deletes nothing by age or size; tombstones stay a day"
    );
    let both = StreamChange {
        cleanup: Some(Some(Cleanup::CompactDelete)),
        delete_retention_ms: Some(Some(-1)),
        ..StreamChange::default()
    };
    s.apply(5, &payload(&Change::AlterStream { resource_id: id, change: both })).unwrap();
    assert_eq!(
        s.streams[&id].settings.retention(defaults),
        crate::Retention::Stream { ms: Some(1_000), bytes: Some(10), compact: Some(u64::MAX) }
    );
    // A snapshot without pins carries the settings too.
    assert!(s.pins.is_empty());
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    let back = StreamChange { cleanup: Some(None), ..StreamChange::default() };
    s.apply(6, &payload(&Change::AlterStream { resource_id: id, change: back })).unwrap();
    assert_eq!(s.streams[&id].settings.cleanup, None);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // Records and snapshots from before compaction read as they did: no
    // compaction settings in them.
    let plain = Change::CreateStream {
        name: "p".into(),
        tenant: "t".into(),
        partitions: vec![vec![1]],
        settings: StreamSettings { retention_ms: Some(5), ..StreamSettings::default() },
    };
    let bytes = payload(&plain);
    let mut t = MetadataState::default();
    t.apply(7, &bytes).unwrap();
    let encoded = t.encode();
    assert_eq!(MetadataState::decode(&encoded).unwrap(), t);
    let mut old = MetadataState::default();
    old.apply(7, &bytes).unwrap();
    assert_eq!(old.encode(), encoded, "nothing new is written when nothing new is set");
}

#[test]
fn nodes_come_and_go_and_groups_get_new_voters() {
    let mut s = MetadataState::default();
    let add = Change::AddNode { id: 4, peer: "n4:7000".into(), client: "n4:9000".into(), zone: "eu-1b".into() };
    s.apply(1, &payload(&add)).unwrap();
    assert_eq!(s.nodes[&4], NodeMeta { peer: "n4:7000".into(), client: "n4:9000".into(), zone: "eu-1b".into() });
    // A zone is written only when there is one, so records without stay
    // as older nodes read them; both decode as they were, and a snapshot
    // keeps the zone.
    assert_eq!(decode_change(&payload(&add)[..]).unwrap(), add);
    let plain = Change::AddNode { id: 5, peer: "n5:7000".into(), client: "n5:9000".into(), zone: String::new() };
    let old = Change::AddNode { id: 5, peer: "n5:7000".into(), client: "n5:9000".into(), zone: "x".into() };
    assert!(payload(&plain).len() < payload(&old).len());
    assert_eq!(decode_change(&payload(&plain)[..]).unwrap(), plain);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // A runtime stream's partition and a configured group get new voters.
    let create = Change::CreateStream {
        name: "o".into(),
        tenant: "t".into(),
        partitions: vec![vec![1, 2, 3]],
        settings: StreamSettings::default(),
    };
    s.apply(2, &payload(&create)).unwrap();
    let g = group_id(2, 0);
    assert_eq!(s.voters_of(g), Some(&[1, 2, 3][..]));
    assert_eq!(s.voters_of(1), None, "a configured group has no voters set at runtime yet");
    s.apply(3, &payload(&Change::DrainNode { id: 2 })).unwrap();
    s.apply(4, &payload(&Change::SetVoters { group: g, voters: vec![4, 3, 1] })).unwrap();
    s.apply(5, &payload(&Change::SetVoters { group: 1, voters: vec![1, 3, 4] })).unwrap();
    assert_eq!(s.voters_of(g), Some(&[1, 3, 4][..]), "kept sorted, and with the resource");
    assert_eq!(s.streams[&resource_id(2)].partitions[0].1, vec![1, 3, 4]);
    assert_eq!(s.voters_of(1), Some(&[1, 3, 4][..]));
    assert_eq!(s.left[&g], BTreeSet::from([2]), "node 2 left this group");
    assert_eq!(s.initial_voters(g), Some(&[1, 2, 3][..]), "new replicas start where the group started");
    assert!(!s.is_settled(g, &[1, 3, 4]));
    s.apply(6, &payload(&Change::Settled { group: g, voters: vec![4, 1, 3] })).unwrap();
    assert!(s.is_settled(g, &[3, 1, 4]));
    s.apply(7, &payload(&Change::ForgetNode { id: 2 })).unwrap();
    assert!(s.gone.contains(&2) && !s.draining.contains(&2));
    // A gone id is never added again, nor drained.
    let again = Change::AddNode { id: 2, peer: "x:1".into(), client: "x:2".into(), zone: String::new() };
    s.apply(8, &payload(&again)).unwrap();
    s.apply(9, &payload(&Change::DrainNode { id: 2 })).unwrap();
    assert!(!s.nodes.contains_key(&2) && !s.draining.contains(&2));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

fn queue(name: &str, tenant: &str, limits: QueueLimits) -> Change {
    Change::CreateQueue { name: name.into(), tenant: tenant.into(), voters: vec![1], ack_timeout_ms: 0, limits }
}

#[test]
fn dead_letter_queues_stay_in_their_tenant_never_loop_and_are_not_deleted_while_used() {
    let mut s = MetadataState::default();
    let limits = |dead_letter| QueueLimits { max_deliveries: 3, message_ttl_ms: 0, dead_letter, ..Default::default() };
    s.apply(1, &payload(&queue("dead", "t", QueueLimits::default()))).unwrap();
    let dead = resource_id(1);
    s.apply(2, &payload(&queue("work", "t", limits(dead)))).unwrap();
    let work = resource_id(2);
    assert_eq!(s.queues[&work].limits.dead_letter, dead);
    // Another tenant's queue, or one that does not exist, is no target.
    s.apply(3, &payload(&queue("other", "u", QueueLimits::default()))).unwrap();
    s.apply(4, &payload(&queue("stray", "t", limits(resource_id(3))))).unwrap();
    s.apply(5, &payload(&queue("ghost", "t", limits(resource_id(99))))).unwrap();
    assert_eq!((s.id_of("stray"), s.id_of("ghost")), (None, None));
    // Itself, or a loop through others.
    let alter = |id, dead_letter| Change::AlterQueue {
        resource_id: id,
        change: QueueChange { dead_letter: Some(dead_letter), ..Default::default() },
    };
    s.apply(6, &payload(&alter(work, work))).unwrap();
    assert_eq!(s.queues[&work].limits.dead_letter, dead, "not its own dead-letter queue");
    s.apply(7, &payload(&alter(dead, work))).unwrap();
    assert_eq!(s.queues[&dead].limits.dead_letter, 0, "dead -> work -> dead is a loop");
    // Settings change alone, and 0 resets.
    let ttl = Change::AlterQueue {
        resource_id: work,
        change: QueueChange { message_ttl_ms: Some(500), ..Default::default() },
    };
    s.apply(8, &payload(&ttl)).unwrap();
    assert_eq!(
        s.queues[&work].limits,
        QueueLimits { max_deliveries: 3, message_ttl_ms: 500, dead_letter: dead, ..Default::default() }
    );
    // A queue that others dead-letter into stays.
    s.apply(9, &payload(&Change::Delete { resource_id: dead })).unwrap();
    assert!(s.queues.contains_key(&dead) && !s.is_deleted(dead));
    s.apply(10, &payload(&alter(work, 0))).unwrap();
    s.apply(11, &payload(&Change::Delete { resource_id: dead })).unwrap();
    assert!(s.is_deleted(dead));
    // The state survives a snapshot.
    s.apply(12, &payload(&alter(work, 0))).unwrap();
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn revoked_sessions_only_move_forward_and_survive_a_snapshot() {
    use lotse_auth::roles::Subject;
    let mut s = MetadataState::default();
    let alice = Subject::User("alice".into());
    let ops = Subject::Group("ops".into());
    s.apply(1, &payload(&Change::RevokeSessions { subject: alice.clone(), before_ms: 5_000 })).unwrap();
    s.apply(2, &payload(&Change::RevokeSessions { subject: alice.clone(), before_ms: 3_000 })).unwrap();
    s.apply(3, &payload(&Change::RevokeSessions { subject: ops.clone(), before_ms: 7_000 })).unwrap();
    assert_eq!(s.revocations.get(&alice), Some(&5_000), "an earlier time does not undo a revocation");
    assert_eq!(s.revocations.get(&ops), Some(&7_000));
    assert!(s.pins.is_empty() && s.incarnations.is_empty(), "the sections before it are written empty");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn revoked_certificates_are_forgotten_once_expired_and_survive_a_snapshot() {
    use crate::metadata::CertKey;
    let key = |issuer: u8, serial: &[u8]| CertKey { issuer: [issuer; 32], serial: serial.to_vec() };
    let revoke = |cert: CertKey, not_after_ms: u64, at_ms: u64| Change::RevokeCertificate {
        cert,
        not_after_ms,
        at_ms,
        reason: "lost".into(),
    };
    let mut s = MetadataState::default();
    s.apply(1, &payload(&revoke(key(1, &[0x0a]), 10_000, 1_000))).unwrap();
    s.apply(2, &payload(&revoke(key(1, &[0x0b]), 2_000, 1_500))).unwrap();
    // A repeat changes nothing, and the issuer is part of the key.
    s.apply(3, &payload(&revoke(key(1, &[0x0a]), 99_000, 1_800))).unwrap();
    s.apply(4, &payload(&revoke(key(2, &[0x0a]), 10_000, 1_900))).unwrap();
    assert_eq!(s.revoked_certs.len(), 3);
    assert_eq!(s.revoked_certs[&key(1, &[0x0a])].not_after_ms, 10_000, "the first revocation stands");
    assert!(s.pins.is_empty() && s.incarnations.is_empty(), "the sections before it are written empty");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // A later revocation drops what ended before it.
    s.apply(5, &payload(&revoke(key(3, &[0x01]), 50_000, 5_000))).unwrap();
    assert!(!s.revoked_certs.contains_key(&key(1, &[0x0b])), "an expired certificate is forgotten");
    assert_eq!(s.revoked_certs.len(), 3);
    // One that has expired already is not recorded.
    s.apply(6, &payload(&revoke(key(4, &[0x02]), 4_000, 6_000))).unwrap();
    assert!(!s.revoked_certs.contains_key(&key(4, &[0x02])));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn the_certificate_inventory_merges_reports_and_forgets_the_long_expired() {
    use crate::metadata::{CERT_KEEP_MS, CertKey, SeenCert};
    let key = |serial: u8| CertKey { issuer: [1; 32], serial: vec![serial] };
    let seen = |principal: &str, not_after_ms: u64, at: u64| SeenCert {
        principal: principal.into(),
        not_before_ms: 0,
        not_after_ms,
        first_seen_ms: at,
        last_seen_ms: at,
    };
    let record = |seen: Vec<(CertKey, SeenCert)>, at_ms: u64| Change::RecordCertificates { seen, at_ms };
    let mut s = MetadataState::default();
    s.apply(1, &payload(&record(vec![(key(1), seen("alice", 10_000_000, 5_000))], 5_000))).unwrap();
    // Two nodes report the same certificate in either order: the earliest
    // first sight and the latest last sight stand.
    s.apply(2, &payload(&record(vec![(key(1), seen("alice", 10_000_000, 9_000))], 9_000))).unwrap();
    s.apply(3, &payload(&record(vec![(key(1), seen("alice", 10_000_000, 2_000))], 9_500))).unwrap();
    let c = &s.certs[&key(1)];
    assert_eq!((c.first_seen_ms, c.last_seen_ms), (2_000, 9_000));
    assert_eq!(s.certs.len(), 1, "one entry per issuer and serial");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // A certificate that ended more than the keep time ago is not recorded,
    // and is forgotten once a report after that time applies.
    let late = 10_000_000 + CERT_KEEP_MS + 1;
    s.apply(4, &payload(&record(vec![(key(2), seen("bob", 10_000_000, late))], late))).unwrap();
    assert!(s.certs.is_empty(), "both are past the keep time: {:?}", s.certs);
    s.apply(5, &payload(&record(vec![(key(3), seen("carol", late + 1_000, late))], late))).unwrap();
    assert_eq!(s.certs.len(), 1);
    // Revoked entries live in their own map, with the inventory's sections
    // before them written.
    s.apply(
        6,
        &payload(&Change::RevokeCertificate {
            cert: key(3),
            not_after_ms: late + 1_000,
            at_ms: late,
            reason: "x".into(),
        }),
    )
    .unwrap();
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn a_renewal_replaces_the_earlier_one_joins_the_inventory_and_survives_a_snapshot() {
    use crate::metadata::{CertKey, MAX_RENEWED};
    let key = |issuer: u8, serial: u16| CertKey { issuer: [issuer; 32], serial: serial.to_be_bytes().to_vec() };
    let renew = |serial: u16, renews: u16, not_after_ms: u64, at_ms: u64| Change::RenewCertificate {
        cert: key(9, serial),
        principal: "alice".into(),
        not_before_ms: at_ms,
        not_after_ms,
        renews: key(1, renews),
        at_ms,
    };
    let mut s = MetadataState::default();
    s.apply(1, &payload(&renew(10, 1, 50_000, 1_000))).unwrap();
    s.apply(2, &payload(&renew(20, 2, 60_000, 1_100))).unwrap();
    assert_eq!(s.renewed.len(), 2);
    assert_eq!(s.renewed[&key(9, 10)].principal, "alice");
    assert_eq!(s.certs[&key(9, 10)].principal, "alice", "the inventory lists it");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // A second renewal of the same certificate replaces the first.
    s.apply(3, &payload(&renew(11, 1, 70_000, 2_000))).unwrap();
    assert!(!s.renewed.contains_key(&key(9, 10)), "the earlier renewal stops counting");
    assert!(s.renewed.contains_key(&key(9, 11)) && s.renewed.contains_key(&key(9, 20)));
    // A later renewal drops those that expired; one expired already is not
    // recorded.
    s.apply(4, &payload(&renew(30, 3, 90_000, 65_000))).unwrap();
    assert!(!s.renewed.contains_key(&key(9, 20)));
    s.apply(5, &payload(&renew(40, 4, 65_500, 66_000))).unwrap();
    assert!(!s.renewed.contains_key(&key(9, 40)));
    assert_eq!(s.renewed.len(), 2);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // The cap counts what is still valid, and a replacement always fits.
    let mut full = MetadataState::default();
    for i in 0..MAX_RENEWED as u16 {
        full.apply(u64::from(i) + 1, &payload(&renew(i, i, 100_000, 1_000))).unwrap();
    }
    assert!(!full.room_for_renewal(&key(1, u16::MAX), 99_999), "full");
    assert!(full.room_for_renewal(&key(1, 7), 99_999), "a replacement fits");
    assert!(full.room_for_renewal(&key(1, u16::MAX), 100_000), "expired ones do not count");
}

#[test]
fn certificate_requests_are_signed_once_lapse_and_survive_a_snapshot() {
    use crate::metadata::{CertKey, REQUEST_TTL_MS, SIGNED_KEEP_MS};
    let day = 24 * 3_600_000;
    let request = |id: u8, at_ms: u64| Change::RequestCertificate {
        id: [id; 32],
        principal: "alice".into(),
        csr: vec![id; 8],
        requested_by: "admin".into(),
        at_ms,
    };
    let cert = CertKey { issuer: [1; 32], serial: vec![0x42] };
    let sign = |id: u8, certificate: u8, at_ms: u64| Change::RecordCertificate {
        id: [id; 32],
        certificate: vec![certificate; 4],
        cert: cert.clone(),
        not_before_ms: 1_000,
        not_after_ms: 400 * day,
        at_ms,
    };
    let mut s = MetadataState::default();
    s.apply(1, &payload(&request(1, day))).unwrap();
    // The same request again changes nothing, not even who asked first.
    let again = Change::RequestCertificate {
        id: [1; 32],
        principal: "mallory".into(),
        csr: vec![9; 8],
        requested_by: "other".into(),
        at_ms: 2 * day,
    };
    s.apply(2, &payload(&again)).unwrap();
    assert_eq!(s.cert_requests[&[1; 32]].requested_by, "admin");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // Signing joins the inventory, with no node having seen it, and takes
    // the certificate once: a second answer does not replace the first.
    s.apply(3, &payload(&sign(1, 7, 3 * day))).unwrap();
    s.apply(4, &payload(&sign(1, 8, 3 * day))).unwrap();
    assert_eq!(s.cert_requests[&[1; 32]].signed.as_ref().unwrap().certificate, vec![7; 4]);
    let seen = &s.certs[&cert];
    assert_eq!((seen.principal.as_str(), seen.first_seen_ms, seen.last_seen_ms), ("alice", 0, 0));
    // A node's first sight then sets the time, though 0 stands for none.
    let sighting = SeenCert {
        principal: "alice".into(),
        not_before_ms: 1_000,
        not_after_ms: 400 * day,
        first_seen_ms: 5 * day,
        last_seen_ms: 5 * day,
    };
    s.apply(5, &payload(&Change::RecordCertificates { seen: vec![(cert.clone(), sighting)], at_ms: 5 * day })).unwrap();
    assert_eq!((s.certs[&cert].first_seen_ms, s.certs[&cert].last_seen_ms), (5 * day, 5 * day));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // An answer to a request nobody made, or one that lapsed, is nothing.
    s.apply(6, &payload(&sign(9, 1, 6 * day))).unwrap();
    assert!(!s.cert_requests.contains_key(&[9; 32]));
    s.apply(7, &payload(&request(2, 10 * day))).unwrap();
    let lapsed = 10 * day + REQUEST_TTL_MS + 1;
    s.apply(8, &payload(&sign(2, 1, lapsed))).unwrap();
    assert!(!s.cert_requests.contains_key(&[2; 32]), "an unsigned request is forgotten after its seven days");
    // A signed one stays for the keep time after signing, then goes.
    assert!(s.cert_requests.contains_key(&[1; 32]));
    s.apply(9, &payload(&request(3, 3 * day + SIGNED_KEEP_MS + 1))).unwrap();
    assert!(!s.cert_requests.contains_key(&[1; 32]));
    assert!(s.cert_requests.contains_key(&[3; 32]));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn a_users_request_in_their_own_name_replaces_their_pending_one() {
    use crate::metadata::REQUEST_TTL_MS;
    let day = 24 * 3_600_000;
    let own = |id: u8, who: &str, at_ms: u64| Change::RequestOwnCertificate {
        id: [id; 32],
        principal: who.into(),
        csr: vec![id; 8],
        at_ms,
    };
    let by_admin = Change::RequestCertificate {
        id: [5; 32],
        principal: "alice".into(),
        csr: vec![5; 8],
        requested_by: "admin".into(),
        at_ms: day,
    };
    let mut s = MetadataState::default();
    s.apply(1, &payload(&by_admin)).unwrap();
    s.apply(2, &payload(&own(1, "alice", day))).unwrap();
    s.apply(3, &payload(&own(2, "bob", day))).unwrap();
    assert_eq!(s.cert_requests[&[1; 32]].requested_by, "alice", "asked by herself");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    let bytes = change_bytes(&own(1, "alice", day));
    assert_eq!(decode_change(&bytes).unwrap(), own(1, "alice", day));

    // Asking again replaces her pending request, and only hers: Bob's and
    // the one an admin asked for in her name stay.
    s.apply(4, &payload(&own(3, "alice", 2 * day))).unwrap();
    let ids: Vec<u8> = s.cert_requests.keys().map(|k| k[0]).collect();
    assert_eq!(ids, vec![2, 3, 5]);
    // The same request again is still one, and replaces nothing.
    s.apply(5, &payload(&own(3, "alice", 2 * day))).unwrap();
    assert_eq!(s.cert_requests.len(), 3);

    // A signed request is not replaced: its certificate stays to fetch.
    let sign = Change::RecordCertificate {
        id: [3; 32],
        certificate: vec![7; 4],
        cert: crate::metadata::CertKey { issuer: [1; 32], serial: vec![1] },
        not_before_ms: 0,
        not_after_ms: 400 * day,
        at_ms: 3 * day,
    };
    s.apply(6, &payload(&sign)).unwrap();
    // Waiting for the CA holder: unsigned and within its seven days.
    assert!(!s.cert_requests[&[3; 32]].pending(3 * day), "signed");
    assert!(s.cert_requests[&[2; 32]].pending(day + REQUEST_TTL_MS));
    assert!(!s.cert_requests[&[2; 32]].pending(day + REQUEST_TTL_MS + 1), "lapsed");
    s.apply(7, &payload(&own(4, "alice", 4 * day))).unwrap();
    assert!(s.cert_requests[&[3; 32]].signed.is_some());
    assert!(s.cert_requests.contains_key(&[4; 32]));
    // Lapsed requests go as before.
    s.apply(8, &payload(&own(6, "carol", 4 * day + REQUEST_TTL_MS + 1))).unwrap();
    assert!(!s.cert_requests.contains_key(&[2; 32]));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn added_partitions_keep_the_streams_index_so_a_delete_finds_them() {
    let mut s = MetadataState::default();
    let create = Change::CreateStream {
        name: "o".into(),
        tenant: "t".into(),
        partitions: vec![vec![1], vec![2]],
        settings: StreamSettings::default(),
    };
    s.apply(3, &payload(&create)).unwrap();
    let id = resource_id(3);
    let add = Change::AddPartitions { resource_id: id, partitions: vec![vec![3], vec![1]] };
    s.apply(7, &payload(&add)).unwrap();
    let groups: Vec<GroupId> = s.streams[&id].partitions.iter().map(|(g, _)| *g).collect();
    assert_eq!(groups, (0..4).map(|p| group_id(3, p)).collect::<Vec<_>>(), "numbered on, under the stream's index");
    assert_eq!(s.streams[&id].partitions[2].1, vec![3]);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // Past the limit: nothing.
    let too_many = Change::AddPartitions { resource_id: id, partitions: vec![vec![1]; MAX_PARTITIONS as usize] };
    s.apply(8, &payload(&too_many)).unwrap();
    assert_eq!(s.streams[&id].partitions.len(), 4);
    s.apply(9, &payload(&Change::Delete { resource_id: id })).unwrap();
    assert_eq!(s.deleted_groups(), groups.into_iter().collect(), "a delete knows every partition's group");
    // A deleted stream gets none.
    s.apply(10, &payload(&add)).unwrap();
    assert!(!s.streams.contains_key(&id));
}

#[test]
fn single_active_consumer_is_set_and_reset_survives_a_snapshot_and_old_forms_still_read() {
    let mut s = MetadataState::default();
    let plain = QueueLimits::default();
    let single = QueueLimits { single_active: true, ..plain };
    // A queue without the flag is written as before: no trailing byte.
    let old = change_bytes(&queue("old", "t", plain));
    assert_eq!(change_bytes(&queue("new", "t", single)).len(), old.len() + 1);
    assert_eq!(decode_change(&old).unwrap(), queue("old", "t", plain));
    s.apply(1, &payload(&queue("orders", "t", single))).unwrap();
    let orders = resource_id(1);
    assert!(s.queues[&orders].limits.single_active);
    let before = s.encode();
    // A state with it reads back; one without the section reads as before.
    assert_eq!(MetadataState::decode(&before).unwrap(), s);
    let alter = |on| Change::AlterQueue {
        resource_id: orders,
        change: QueueChange { single_active: Some(on), ..Default::default() },
    };
    assert_eq!(decode_change(&change_bytes(&alter(false))).unwrap(), alter(false));
    s.apply(2, &payload(&alter(false))).unwrap();
    assert!(!s.queues[&orders].limits.single_active);
    let ttl_only = Change::AlterQueue {
        resource_id: orders,
        change: QueueChange { message_ttl_ms: Some(5), ..Default::default() },
    };
    s.apply(3, &payload(&ttl_only)).unwrap();
    assert!(!s.queues[&orders].limits.single_active, "another setting leaves it");
    s.apply(4, &payload(&alter(true))).unwrap();
    assert!(s.queues[&orders].limits.single_active);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // An unknown flag is refused rather than dropped.
    let mut bad = change_bytes(&queue("x", "t", single)).to_vec();
    *bad.last_mut().unwrap() = 2;
    assert!(decode_change(&bad).is_err());
}

#[test]
fn priority_levels_are_set_and_changed_survive_a_snapshot_and_sit_beside_single_active() {
    let mut s = MetadataState::default();
    let both = QueueLimits { single_active: true, priority_levels: 5, ..Default::default() };
    let levels = QueueLimits { priority_levels: 3, ..Default::default() };
    for (i, l) in [both, levels].into_iter().enumerate() {
        let c = queue(&format!("q{i}"), "t", l);
        assert_eq!(decode_change(&change_bytes(&c)).unwrap(), c);
    }
    s.apply(1, &payload(&queue("q0", "t", both))).unwrap();
    s.apply(2, &payload(&queue("q1", "t", levels))).unwrap();
    let (q0, q1) = (resource_id(1), resource_id(2));
    assert_eq!((s.queues[&q0].limits, s.queues[&q1].limits), (both, levels));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    let alter = |id, n| Change::AlterQueue {
        resource_id: id,
        change: QueueChange { priority_levels: Some(n), ..Default::default() },
    };
    assert_eq!(decode_change(&change_bytes(&alter(q1, 7))).unwrap(), alter(q1, 7));
    s.apply(3, &payload(&alter(q1, 7))).unwrap();
    s.apply(4, &payload(&alter(q0, 0))).unwrap();
    assert_eq!(s.queues[&q1].limits.priority_levels, 7);
    assert_eq!(s.queues[&q0].limits, QueueLimits { single_active: true, ..Default::default() });
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    // Flags with levels promised but missing are refused.
    let mut cut = change_bytes(&queue("x", "t", levels)).to_vec();
    cut.pop();
    assert!(decode_change(&cut).is_err());
}

/// Seed 1580 of the metadata simulation: two admins set a queue's ack timeout
/// at once; the earlier change was applied and then replaced by the later,
/// and its answer must not claim a refusal. Only a dead-letter change can be
/// refused, and one that did not fit shows as not holding.
#[test]
fn only_a_dead_letter_change_can_be_reported_refused() {
    let mut s = MetadataState::default();
    s.apply(1, &payload(&queue("dead", "t", QueueLimits::default()))).unwrap();
    s.apply(2, &payload(&queue("work", "t", QueueLimits::default()))).unwrap();
    let work = resource_id(2);
    let dead = resource_id(1);
    let timeout = |ms| QueueChange { ack_timeout_ms: Some(ms), ..Default::default() };
    let alter = |change| Change::AlterQueue { resource_id: work, change };
    let (first, second) = (timeout(100), timeout(200));
    s.apply(3, &payload(&alter(first))).unwrap();
    s.apply(4, &payload(&alter(second))).unwrap();
    let q = &s.queues[&work];
    assert!(!first.holds_in(q), "the first change was replaced");
    assert!(!first.refused_in(q), "a replaced change is not a refused one");
    // A dead-letter change that fits holds; one that does not fit is refused.
    let fits = QueueChange { dead_letter: Some(dead), ..Default::default() };
    s.apply(5, &payload(&alter(fits))).unwrap();
    assert!(!fits.refused_in(&s.queues[&work]));
    let ghost = QueueChange { dead_letter: Some(resource_id(99)), ..Default::default() };
    s.apply(6, &payload(&alter(ghost))).unwrap();
    assert!(ghost.refused_in(&s.queues[&work]));
}

#[test]
fn a_tombstone_goes_once_every_node_removed_its_files_and_the_id_stays_deleted() {
    let mut s = MetadataState::default();
    let queue = |name: &str| Change::CreateQueue {
        name: name.into(),
        tenant: "t".into(),
        voters: vec![1],
        ack_timeout_ms: 0,
        limits: QueueLimits::default(),
    };
    s.apply(1, &payload(&queue("a"))).unwrap();
    s.apply(2, &payload(&queue("b"))).unwrap();
    s.apply(3, &payload(&Change::DeclareExchange { name: "x".into(), tenant: "t".into(), kind: ExchangeKind::Fanout }))
        .unwrap();
    s.apply(4, &payload(&Change::Delete { resource_id: resource_id(1) })).unwrap();
    s.apply(5, &payload(&Change::Delete { resource_id: resource_id(2) })).unwrap();
    assert_eq!(s.deleted[&resource_id(1)], Tombstone { groups: 1, at: 4 });

    // Every node must have removed the files through the delete's index.
    assert!(s.forgettable([Some(9), None]).is_empty(), "a node that has not answered holds them all");
    assert!(s.forgettable([Some(9), Some(3)]).is_empty(), "nor one that is behind both deletes");
    assert_eq!(s.forgettable([Some(9), Some(4)]), vec![resource_id(1)]);
    assert_eq!(s.forgettable([Some(5), Some(7)]), vec![resource_id(1), resource_id(2)]);
    assert!(s.forgettable(std::iter::empty()).is_empty(), "no node listed is no node that answered");

    let forget = Change::ForgetDeleted { resource_ids: vec![resource_id(1), resource_id(7)] };
    assert_eq!(decode_change(&payload(&forget)[..]).unwrap(), forget);
    s.apply(6, &payload(&forget)).unwrap();
    assert!(!s.deleted.contains_key(&resource_id(1)) && s.deleted.contains_key(&resource_id(2)));
    assert!(s.deleted_groups().iter().all(|g| *g == group_id(2, 0)), "its groups are no longer removed");
    // Forgotten, the id is deleted all the same: a bind admitted before the
    // delete changes nothing, and nodes call it gone.
    assert!(s.is_deleted(resource_id(1)) && !s.is_deleted(resource_id(3)));
    let to_a = Binding { queue_id: resource_id(1), key: String::new(), headers: vec![], match_all: true };
    s.apply(7, &payload(&Change::Bind { exchange_id: resource_id(3), binding: to_a })).unwrap();
    assert!(s.bindings.values().all(Vec::is_empty));
    let seen = Deleted::of(&s, 7);
    assert!(seen.contains(&resource_id(1)) && seen.contains(&resource_id(2)), "with or without a tombstone");
    assert!(!seen.contains(&resource_id(3)), "live");
    assert!(!seen.contains(&resource_id(8)), "newer than what this node applied: maybe not heard of yet");
    assert!(!seen.contains(&9), "configured");
    // The index of a tombstone survives a snapshot.
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn moves_off_round_trip() {
    let mut s = MetadataState::default();
    let create = Change::CreateStream {
        name: "o".into(),
        tenant: "t".into(),
        partitions: vec![vec![1, 2, 3]],
        settings: StreamSettings::default(),
    };
    s.apply(10, &payload(&create)).unwrap();
    let g = group_id(10, 0);
    s.apply(11, &payload(&Change::SetVoters { group: g, voters: vec![1, 3, 4] })).unwrap();
    s.apply(12, &payload(&Change::Settled { group: g, voters: vec![1, 3, 4] })).unwrap();
    assert_eq!(s.moved_off[&g].get(&2), Some(&11));
    let bytes = s.encode();
    assert_eq!(MetadataState::decode(&bytes).unwrap(), s);
    for cut in 0..bytes.len() {
        assert!(
            MetadataState::decode(&bytes[..cut]).is_ok_and(|d| d != s) || MetadataState::decode(&bytes[..cut]).is_err(),
            "cut at {cut}"
        );
    }
}

#[test]
fn resume_windows_are_set_and_changed_and_survive_a_snapshot() {
    let mut s = MetadataState::default();
    let all = QueueLimits { single_active: true, priority_levels: 4, resume_window_ms: 5_000, ..Default::default() };
    let window = QueueLimits { resume_window_ms: 60_000, ..Default::default() };
    for (i, l) in [all, window].into_iter().enumerate() {
        let c = queue(&format!("q{i}"), "t", l);
        assert_eq!(decode_change(&change_bytes(&c)).unwrap(), c);
    }
    let before = s.encode();
    s.apply(1, &payload(&queue("q0", "t", all))).unwrap();
    s.apply(2, &payload(&queue("q1", "t", window))).unwrap();
    let (q0, q1) = (resource_id(1), resource_id(2));
    assert_eq!((s.queues[&q0].limits, s.queues[&q1].limits), (all, window));
    assert_eq!(MetadataState::decode(&before).unwrap(), MetadataState::default(), "without windows as before");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    let alter = |id, ms| Change::AlterQueue {
        resource_id: id,
        change: QueueChange { resume_window_ms: Some(ms), ..Default::default() },
    };
    assert_eq!(decode_change(&change_bytes(&alter(q1, 9))).unwrap(), alter(q1, 9));
    s.apply(3, &payload(&alter(q1, 9))).unwrap();
    s.apply(4, &payload(&alter(q0, 0))).unwrap();
    assert_eq!(s.queues[&q1].limits.resume_window_ms, 9);
    assert_eq!(s.queues[&q0].limits, QueueLimits { resume_window_ms: 0, ..all }, "0: back to the default");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    let mut cut = change_bytes(&queue("x", "t", window)).to_vec();
    cut.pop();
    assert!(decode_change(&cut).is_err(), "a window promised but cut short");
}

#[test]
fn length_limits_are_set_and_changed_and_survive_a_snapshot() {
    use lotse_queue::{LengthLimit, Overflow};
    let mut s = MetadataState::default();
    let length = LengthLimit { max_messages: 1_000, max_bytes: 1 << 20, overflow: Overflow::DropHead };
    let all =
        QueueLimits { single_active: true, priority_levels: 4, resume_window_ms: 5_000, length, ..Default::default() };
    let only = QueueLimits {
        length: LengthLimit { max_messages: 0, max_bytes: 64, overflow: Overflow::RejectPublish },
        ..Default::default()
    };
    for (i, l) in [all, only].into_iter().enumerate() {
        let c = queue(&format!("q{i}"), "t", l);
        assert_eq!(decode_change(&change_bytes(&c)).unwrap(), c);
    }
    // A queue without a limit writes what nodes from before read.
    let plain = queue("p", "t", QueueLimits::default());
    assert!(!change_bytes(&plain).is_empty());
    let before = s.encode();
    s.apply(1, &payload(&queue("q0", "t", all))).unwrap();
    s.apply(2, &payload(&queue("q1", "t", only))).unwrap();
    let (q0, q1) = (resource_id(1), resource_id(2));
    assert_eq!((s.queues[&q0].limits, s.queues[&q1].limits), (all, only));
    assert_eq!(MetadataState::decode(&before).unwrap(), MetadataState::default(), "without limits as before");
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    // Each part changes on its own; 0 lifts a limit.
    let alter = |id, change| Change::AlterQueue { resource_id: id, change };
    let changes = [
        QueueChange { max_length: Some(5), ..Default::default() },
        QueueChange { max_length_bytes: Some(0), ..Default::default() },
        QueueChange { overflow: Some(Overflow::RejectPublish), resume_window_ms: Some(7), ..Default::default() },
    ];
    for (i, c) in changes.into_iter().enumerate() {
        let a = alter(q0, c);
        assert_eq!(decode_change(&change_bytes(&a)).unwrap(), a);
        s.apply(3 + i as u64, &payload(&a)).unwrap();
    }
    assert_eq!(
        s.queues[&q0].limits.length,
        LengthLimit { max_messages: 5, max_bytes: 0, overflow: Overflow::RejectPublish }
    );
    assert_eq!(s.queues[&q0].limits.resume_window_ms, 7);
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);

    let mut cut = change_bytes(&queue("x", "t", only)).to_vec();
    cut.pop();
    assert!(decode_change(&cut).is_err(), "a limit promised but cut short");
    let mut bad = change_bytes(&queue("x", "t", only)).to_vec();
    *bad.last_mut().unwrap() = 9;
    assert!(decode_change(&bad).is_err(), "an unknown overflow");
}

#[test]
fn nodes_low_on_disk_are_recorded_cleared_and_survive_a_snapshot() {
    let mut s = MetadataState::default();
    let before = s.encode();
    let low = |node, low| Change::DiskLow { node, low };
    for c in [low(3, true), low(1 << 40, false)] {
        assert_eq!(decode_change(&change_bytes(&c)).unwrap(), c);
    }
    let mut cut = change_bytes(&low(3, true)).to_vec();
    cut.pop();
    assert!(decode_change(&cut).is_err(), "cut short");
    s.apply(1, &payload(&low(3, true))).unwrap();
    s.apply(2, &payload(&low(4, true))).unwrap();
    s.apply(3, &payload(&low(3, true))).unwrap();
    assert_eq!(s.disk_low, BTreeSet::from([3, 4]));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
    assert_ne!(s.encode(), before, "a section of its own");
    s.apply(4, &payload(&low(3, false))).unwrap();
    assert_eq!(s.disk_low, BTreeSet::from([4]));
    // A node that is forgotten is no longer low, and cannot become so.
    s.apply(5, &payload(&Change::ForgetNode { id: 4 })).unwrap();
    assert!(s.disk_low.is_empty());
    s.apply(6, &payload(&low(4, true))).unwrap();
    assert!(s.disk_low.is_empty(), "a forgotten node");
    // Without anyone low, the state is written as before.
    let mut empty = MetadataState::default();
    empty.apply(1, &payload(&low(2, true))).unwrap();
    empty.apply(2, &payload(&low(2, false))).unwrap();
    assert_eq!(empty.encode(), before);
}

fn token_change(name: &str, owner: &str, created_at_ms: u64, expires_at_ms: u64) -> Change {
    Change::CreateToken {
        id: 0,
        name: name.into(),
        owner: owner.into(),
        hash: [7; 32],
        created_at_ms,
        expires_at_ms,
        created_by: owner.into(),
    }
}

#[test]
fn a_created_token_is_kept_and_revoked() {
    let mut s = MetadataState::default();
    s.apply(10, &payload(&token_change("ci", "alice", 1_000, 5_000))).unwrap();
    // The id is the index the change applied at, never the proposal's 0.
    let t = s.live_token(10, 2_000).expect("kept under its index");
    assert_eq!((t.name.as_str(), t.owner.as_str(), t.hash, t.created_by.as_str()), ("ci", "alice", [7; 32], "alice"));
    assert_eq!((t.created_at_ms, t.expires_at_ms), (1_000, 5_000));
    assert!(s.live_token(10, 5_000).is_none(), "an expired token is not live");
    assert!(s.live_token(0, 2_000).is_none());
    s.apply(11, &payload(&Change::RevokeToken { id: 10 })).unwrap();
    assert!(s.live_token(10, 2_000).is_none());
    assert!(s.tokens.is_empty());
    // Revoking what is not there changes nothing.
    s.apply(12, &payload(&Change::RevokeToken { id: 10 })).unwrap();
}

#[test]
fn expired_tokens_go_when_the_next_token_change_applies() {
    let mut s = MetadataState::default();
    s.apply(1, &payload(&token_change("a", "alice", 1_000, 2_000))).unwrap();
    s.apply(2, &payload(&token_change("b", "alice", 1_000, 9_000))).unwrap();
    assert_eq!(s.tokens.len(), 2, "nothing prunes before a token change");
    s.apply(3, &payload(&token_change("c", "bob", 3_000, 9_000))).unwrap();
    assert!(!s.tokens.contains_key(&1), "the expired one is dropped");
    assert!(s.tokens.contains_key(&2) && s.tokens.contains_key(&3));
}

#[test]
fn room_for_tokens_is_limited_per_owner_and_in_all() {
    use crate::metadata::{MAX_TOKENS, MAX_TOKENS_PER_OWNER};
    let mut s = MetadataState::default();
    for i in 0..MAX_TOKENS_PER_OWNER as u64 {
        assert!(s.token_room("alice", 100).is_ok());
        s.tokens.insert(i, stored("alice", 1_000));
    }
    assert!(s.token_room("alice", 100).is_err(), "a 51st token for one owner");
    assert!(s.token_room("bob", 100).is_ok(), "another owner is not affected");
    assert!(s.token_room("alice", 1_000).is_ok(), "expired tokens do not count");
    // The whole state.
    let mut full = MetadataState::default();
    for i in 0..MAX_TOKENS as u64 {
        full.tokens.insert(i, stored(&format!("user{}", i / 10), 1_000));
    }
    assert!(full.token_room("fresh", 100).is_err(), "the 10,001st token");
    assert!(full.token_room("fresh", 1_000).is_ok());
}

fn stored(owner: &str, expires_at_ms: u64) -> crate::metadata::StoredToken {
    crate::metadata::StoredToken {
        name: "t".into(),
        owner: owner.into(),
        hash: [1; 32],
        created_at_ms: 0,
        expires_at_ms,
        created_by: owner.into(),
    }
}

fn tokens_state() -> MetadataState {
    let mut s = MetadataState::default();
    s.apply(1, &payload(&Change::SetClusterVersion { version: crate::version::TOKENS_SINCE })).unwrap();
    s.apply(2, &payload(&token_change("ci", "alice", 1_000, 5_000))).unwrap();
    s.apply(3, &payload(&token_change("dev", "bob", 1_000, 6_000))).unwrap();
    s
}

#[test]
fn tokens_survive_a_snapshot() {
    let s = tokens_state();
    assert!(s.writes(crate::version::TOKENS_SINCE));
    let decoded = MetadataState::decode(&s.encode()).unwrap();
    assert_eq!(decoded, s);
    assert_eq!(decoded.tokens.len(), 2);
    assert_eq!(decoded.tokens[&3].owner, "bob");
}

#[test]
fn a_snapshot_without_section_19_reads_with_no_tokens() {
    let mut s = tokens_state();
    s.tokens.clear();
    let bytes = s.encode();
    let decoded = MetadataState::decode(&bytes).unwrap();
    assert!(decoded.tokens.is_empty());
    assert_eq!(decoded, s);
    // Before the cluster version allows it, tokens are not written, so a
    // node of the previous release reads the snapshot.
    let mut old = tokens_state();
    old.cluster_version = Some(0);
    let decoded = MetadataState::decode(&old.encode()).unwrap();
    assert!(decoded.tokens.is_empty());
    assert_eq!(decoded.encode(), old.encode(), "the same 18 sections");
}

#[test]
fn deleting_a_user_drops_their_tokens() {
    let mut s = tokens_state();
    s.apply(4, &payload(&Change::SetUser { user: "alice".into(), credential: None })).unwrap();
    assert!(!s.tokens.contains_key(&2), "alice's token is gone");
    assert!(s.tokens.contains_key(&3), "bob's stays");
}

#[test]
fn token_changes_round_trip_through_their_record() {
    for change in [token_change("ci", "alice", 1, 2), Change::RevokeToken { id: 77 }] {
        assert_eq!(decode_change(&payload(&change)).unwrap(), change);
    }
    assert!(decode_change(&payload(&Change::RevokeToken { id: 77 })[..5]).is_err());
}

/// A state at `version` with one runtime stream (id from index 2) whose
/// segment age is two minutes.
fn aged_state(version: u32) -> (MetadataState, u64) {
    let mut s = MetadataState::default();
    s.apply(1, &payload(&Change::SetClusterVersion { version })).unwrap();
    let create = Change::CreateStream {
        name: "s".into(),
        tenant: crate::DEFAULT_TENANT.into(),
        partitions: vec![vec![1]],
        settings: StreamSettings::default(),
    };
    s.apply(2, &payload(&create)).unwrap();
    let id = crate::metadata::resource_id(2);
    s.apply(3, &payload(&Change::SetSegmentMs { resource_id: id, ms: 120_000 })).unwrap();
    (s, id)
}

#[test]
fn segment_ages_survive_a_snapshot_and_go_with_their_resource() {
    let (mut s, id) = aged_state(crate::version::SEGMENT_AGE_SINCE);
    assert_eq!(s.segment_ms, BTreeMap::from([(id, 120_000)]));
    let decoded = MetadataState::decode(&s.encode()).unwrap();
    assert_eq!(decoded, s);
    let change = Change::SetSegmentMs { resource_id: id, ms: 0 };
    assert_eq!(decode_change(&payload(&change)).unwrap(), change);
    s.apply(4, &payload(&Change::SetSegmentMs { resource_id: id, ms: 0 })).unwrap();
    assert!(s.segment_ms.is_empty(), "0 returns it to the default");
    s.apply(5, &payload(&Change::SetSegmentMs { resource_id: id, ms: 90_000 })).unwrap();
    s.apply(6, &payload(&Change::Delete { resource_id: id })).unwrap();
    assert!(s.segment_ms.is_empty(), "it goes with the resource");
    s.apply(7, &payload(&Change::SetSegmentMs { resource_id: id, ms: 90_000 })).unwrap();
    assert!(s.segment_ms.is_empty(), "nor comes back for a deleted one");
}

#[test]
fn segment_ages_are_not_written_before_0_2() {
    let (s, _) = aged_state(crate::version::TOKENS_SINCE);
    let decoded = MetadataState::decode(&s.encode()).unwrap();
    assert!(decoded.segment_ms.is_empty(), "a 0.1 node could not read section 20");
}

fn set_quota(tenant: &str, prefix: &str, bytes: Option<u64>, by_cluster_admin: bool) -> Change {
    Change::SetQuota { tenant: tenant.into(), prefix: prefix.into(), bytes, by_cluster_admin }
}

fn quota_full(tenant: &str, prefix: &str, full: bool) -> Change {
    Change::QuotaFull { tenant: tenant.into(), prefix: prefix.into(), full }
}

/// A state at cluster version 0.2 with tenant `shop` created at index 2.
fn quota_state() -> MetadataState {
    let mut s = MetadataState::default();
    s.apply(1, &payload(&Change::SetClusterVersion { version: crate::version::QUOTAS_SINCE })).unwrap();
    s.apply(2, &payload(&Change::CreateTenant { name: "shop".into(), incarnated: true })).unwrap();
    s
}

#[test]
fn a_quota_change_reads_back() {
    for change in [
        set_quota("shop", "", Some(1 << 30), true),
        set_quota("shop", "orders.", None, false),
        quota_full("shop", "orders.", true),
        quota_full("shop", "", false),
    ] {
        assert_eq!(decode_change(&payload(&change)).unwrap(), change);
    }
}

#[test]
fn quotas_round_trip_in_the_snapshot() {
    let mut s = quota_state();
    s.apply(3, &payload(&set_quota("shop", "", Some(1_000), true))).unwrap();
    s.apply(4, &payload(&set_quota("shop", "orders.", Some(500), false))).unwrap();
    s.apply(5, &payload(&quota_full("shop", "orders.", true))).unwrap();
    let want = Quota { bytes: 500, by_cluster_admin: false, full: true };
    assert_eq!(s.quotas[&("shop".to_string(), "orders.".to_string())], want);
    let decoded = MetadataState::decode(&s.encode()).unwrap();
    assert_eq!(decoded, s);
    assert_eq!(decoded.quotas.len(), 2);
}

#[test]
fn a_changed_limit_keeps_its_full_flag_and_none_removes_it() {
    let mut s = quota_state();
    s.apply(3, &payload(&set_quota("shop", "", Some(1_000), true))).unwrap();
    s.apply(4, &payload(&quota_full("shop", "", true))).unwrap();
    s.apply(5, &payload(&set_quota("shop", "", Some(10_000), true))).unwrap();
    let q = s.quotas[&("shop".to_string(), String::new())];
    assert_eq!((q.bytes, q.full), (10_000, true), "full until the next count says otherwise");
    s.apply(6, &payload(&set_quota("shop", "", None, true))).unwrap();
    assert!(s.quotas.is_empty());
}

#[test]
fn quota_records_wait_for_0_2() {
    let mut s = quota_state();
    s.apply(3, &payload(&set_quota("shop", "", Some(1_000), true))).unwrap();
    s.cluster_version = Some(crate::version::TOKENS_SINCE);
    let decoded = MetadataState::decode(&s.encode()).unwrap();
    assert!(decoded.quotas.is_empty(), "a 0.1 node could not read the quota section");
}

#[test]
fn a_deleted_tenant_drops_its_quotas() {
    let mut s = quota_state();
    s.apply(3, &payload(&set_quota("shop", "", Some(1_000), true))).unwrap();
    s.apply(4, &payload(&set_quota("shop", "orders.", Some(500), false))).unwrap();
    s.apply(5, &payload(&quota_full("shop", "", true))).unwrap();
    s.apply(6, &payload(&Change::DeleteTenant { name: "shop".into() })).unwrap();
    assert!(s.quotas.is_empty(), "they go with the tenant");
    s.apply(7, &payload(&Change::CreateTenant { name: "shop".into(), incarnated: true })).unwrap();
    assert!(s.quotas.is_empty(), "a tenant created again starts without limits");
    s.apply(8, &payload(&quota_full("shop", "", true))).unwrap();
    assert!(s.quotas.is_empty(), "nor does an old count bring one back");
}

#[test]
fn the_101st_prefix_limit_is_ignored() {
    let mut s = quota_state();
    for i in 0..crate::metadata::MAX_PREFIX_QUOTAS {
        s.apply(10 + i as u64, &payload(&set_quota("shop", &format!("p{i}."), Some(1), false))).unwrap();
    }
    s.apply(500, &payload(&set_quota("shop", "", Some(1), true))).unwrap();
    s.apply(501, &payload(&set_quota("shop", "extra.", Some(1), false))).unwrap();
    assert_eq!(s.quotas.len(), crate::metadata::MAX_PREFIX_QUOTAS + 1, "100 prefixes and the tenant's own");
    assert!(!s.quotas.contains_key(&("shop".to_string(), "extra.".to_string())));
    s.apply(502, &payload(&set_quota("shop", "p0.", Some(2), false))).unwrap();
    assert_eq!(s.quotas[&("shop".to_string(), "p0.".to_string())].bytes, 2, "an existing one still changes");
}

#[test]
fn quota_full_for_a_removed_limit_does_nothing() {
    let mut s = quota_state();
    s.apply(3, &payload(&quota_full("shop", "orders.", true))).unwrap();
    assert!(s.quotas.is_empty());
}

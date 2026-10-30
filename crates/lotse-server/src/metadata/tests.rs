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
        &payload(&Change::CreateQueue { name: "q".into(), tenant: "t".into(), voters: vec![1], ack_timeout_ms: 0 }),
    )
    .unwrap();
    s.apply(3, &payload(&Change::DeclareExchange { name: "x".into(), tenant: "t".into(), kind: ExchangeKind::Fanout }))
        .unwrap();
    let to_q = Binding { queue_id: resource_id(2), key: String::new(), headers: vec![], match_all: true };
    // A configured exchange (id 9) holds a runtime binding to the queue too.
    for x in [resource_id(3), 9] {
        s.apply(4, &payload(&Change::Bind { exchange_id: x, binding: to_q.clone() })).unwrap();
    }
    s.apply(5, &payload(&Change::AlterQueue { resource_id: resource_id(2), ack_timeout_ms: 750 })).unwrap();
    assert_eq!(s.queues[&resource_id(2)].ack_timeout_ms, 750);

    s.apply(6, &payload(&Change::Delete { resource_id: resource_id(2) })).unwrap();
    assert!(s.queues.is_empty() && s.is_deleted(resource_id(2)));
    assert!(s.bindings.values().all(Vec::is_empty), "bindings to the queue are gone, on every exchange");
    // A bind and an alter that were admitted before the delete arrive after it.
    s.apply(7, &payload(&Change::Bind { exchange_id: resource_id(3), binding: to_q.clone() })).unwrap();
    s.apply(8, &payload(&Change::AlterQueue { resource_id: resource_id(2), ack_timeout_ms: 1 })).unwrap();
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
        &payload(&Change::CreateQueue { name: "q".into(), tenant: "t".into(), voters: vec![1], ack_timeout_ms: 0 }),
    )
    .unwrap();
    assert_eq!(s.id_of("q"), Some(resource_id(13)));

    let bytes = s.encode();
    assert_eq!(MetadataState::decode(&bytes).unwrap(), s);
    for cut in 0..bytes.len() {
        assert!(MetadataState::decode(&bytes[..cut]).is_err(), "cut at {cut}");
    }
}

#[test]
fn tenants_bindings_and_a_create_racing_a_delete() {
    use lotse_auth::roles::{Role, RoleBinding, Subject};
    let mut s = MetadataState::default();
    s.apply(1, &payload(&Change::CreateTenant { name: "shop".into() })).unwrap();
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
        }),
    )
    .unwrap();
    assert_eq!(s.id_of("late"), None);
    // Created again: open again.
    s.apply(8, &payload(&Change::CreateTenant { name: "shop".into() })).unwrap();
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
}

#[test]
fn stream_settings_change_key_by_key_and_survive_a_snapshot() {
    let mut s = MetadataState::default();
    let settings = StreamSettings { retention_ms: Some(60_000), retention_bytes: None };
    let create = Change::CreateStream { name: "o".into(), tenant: "t".into(), partitions: vec![vec![1]], settings };
    s.apply(4, &payload(&create)).unwrap();
    let id = resource_id(4);
    assert_eq!(s.streams[&id].settings, settings);
    // Two changes of different keys, proposed from the same state: both
    // hold, whichever applies last.
    let bytes = StreamChange { retention_bytes: Some(Some(1 << 20)), ..StreamChange::default() };
    let ms = StreamChange { retention_ms: Some(None), ..StreamChange::default() };
    s.apply(5, &payload(&Change::AlterStream { resource_id: id, change: bytes })).unwrap();
    let want = StreamSettings { retention_ms: Some(60_000), retention_bytes: Some(1 << 20) };
    assert_eq!(s.streams[&id].settings, want, "a change of the size leaves the age as it was");
    s.apply(6, &payload(&Change::AlterStream { resource_id: id, change: ms })).unwrap();
    let want = StreamSettings { retention_ms: None, retention_bytes: Some(1 << 20) };
    assert_eq!(s.streams[&id].settings, want);
    let unlimited = StreamChange { retention_ms: Some(Some(-1)), ..StreamChange::default() };
    s.apply(7, &payload(&Change::AlterStream { resource_id: id, change: unlimited })).unwrap();
    let want = StreamSettings { retention_ms: Some(-1), retention_bytes: Some(1 << 20) };
    assert_eq!(s.streams[&id].settings, want);
    let defaults = crate::RetentionConfig { ms: Some(1_000), bytes: None };
    assert_eq!(want.retention(defaults), crate::Retention::Stream { ms: None, bytes: Some(1 << 20) });
    assert_eq!(
        StreamSettings::default().retention(defaults),
        crate::Retention::Stream { ms: Some(1_000), bytes: None }
    );
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn nodes_come_and_go_and_groups_get_new_voters() {
    let mut s = MetadataState::default();
    let add = Change::AddNode { id: 4, peer: "n4:7000".into(), client: "n4:9000".into() };
    s.apply(1, &payload(&add)).unwrap();
    assert_eq!(s.nodes[&4], NodeMeta { peer: "n4:7000".into(), client: "n4:9000".into() });
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
    assert_eq!(s.left[&g], BTreeSet::from([2]), "node 2 never comes back to this group");
    assert_eq!(s.initial_voters(g), Some(&[1, 2, 3][..]), "new replicas start where the group started");
    assert!(!s.is_settled(g, &[1, 3, 4]));
    s.apply(6, &payload(&Change::Settled { group: g, voters: vec![4, 1, 3] })).unwrap();
    assert!(s.is_settled(g, &[3, 1, 4]));
    s.apply(7, &payload(&Change::ForgetNode { id: 2 })).unwrap();
    assert!(s.gone.contains(&2) && !s.draining.contains(&2));
    // A gone id is never added again, nor drained.
    let again = Change::AddNode { id: 2, peer: "x:1".into(), client: "x:2".into() };
    s.apply(8, &payload(&again)).unwrap();
    s.apply(9, &payload(&Change::DrainNode { id: 2 })).unwrap();
    assert!(!s.nodes.contains_key(&2) && !s.draining.contains(&2));
    assert_eq!(MetadataState::decode(&s.encode()).unwrap(), s);
}

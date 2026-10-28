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
    let stream = Change::CreateStream { name: "orders".into(), partitions: vec![vec![1, 2, 3], vec![2, 3, 4]] };
    s.apply(10, &payload(&stream)).unwrap();
    assert_eq!(s.id_of("orders"), Some(resource_id(10)));
    assert_eq!(s.streams[&resource_id(10)].partitions[1], (group_id(10, 1), vec![2, 3, 4]));
    // The same name again, from a concurrent create: nothing changes.
    s.apply(11, &payload(&Change::CreateQueue { name: "orders".into(), voters: vec![1], ack_timeout_ms: 5 })).unwrap();
    assert!(s.queues.is_empty());
    s.apply(12, &payload(&Change::CreateQueue { name: "jobs".into(), voters: vec![1, 4, 5], ack_timeout_ms: 5 }))
        .unwrap();
    s.apply(13, &payload(&Change::DeclareExchange { name: "x".into(), kind: ExchangeKind::Topic })).unwrap();
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
    s.apply(1, &payload(&Change::CreateStream { name: "s".into(), partitions: vec![vec![1], vec![2], vec![3]] }))
        .unwrap();
    s.apply(2, &payload(&Change::CreateQueue { name: "q".into(), voters: vec![1], ack_timeout_ms: 0 })).unwrap();
    s.apply(3, &payload(&Change::DeclareExchange { name: "x".into(), kind: ExchangeKind::Fanout })).unwrap();
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
    s.apply(13, &payload(&Change::CreateQueue { name: "q".into(), voters: vec![1], ack_timeout_ms: 0 })).unwrap();
    assert_eq!(s.id_of("q"), Some(resource_id(13)));

    let bytes = s.encode();
    assert_eq!(MetadataState::decode(&bytes).unwrap(), s);
    for cut in 0..bytes.len() {
        assert!(MetadataState::decode(&bytes[..cut]).is_err(), "cut at {cut}");
    }
}

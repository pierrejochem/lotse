use super::*;

fn member(subscribed: &[u64]) -> MemberMeta {
    MemberMeta {
        subscribed: subscribed.iter().copied().collect(),
        rebalance_timeout_ms: 1_000,
        ..MemberMeta::default()
    }
}

fn four(_: u64) -> u32 {
    4
}

#[test]
fn targets_are_even_and_sticky() {
    let mut g = GroupMeta::default();
    g.members.insert("a".into(), member(&[7]));
    g.retarget(four);
    assert_eq!(g.members["a"].target.len(), 4);
    g.members.insert("b".into(), member(&[7]));
    g.retarget(four);
    let (a, b) = (g.members["a"].target.clone(), g.members["b"].target.clone());
    assert_eq!((a.len(), b.len()), (2, 2));
    assert!(a.is_disjoint(&b));
    g.members.insert("c".into(), member(&[7]));
    g.retarget(four);
    let a2 = &g.members["a"].target;
    assert!(a2.is_subset(&a) || a.is_subset(a2), "a keeps what it can: {a:?} then {a2:?}");
    let total: usize = g.members.values().map(|m| m.target.len()).sum();
    assert_eq!(total, 4, "every partition has one target");
    // Only subscribers of a stream get its partitions.
    g.members.insert("d".into(), member(&[8]));
    g.retarget(four);
    assert!(g.members["d"].target.iter().all(|tp| tp.0 == 8) && g.members["d"].target.len() == 4);
}

/// A tiny deterministic generator, so the test needs no runtime.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

/// However members join, leave, change subscriptions and report what they
/// own, no partition is ever held by two members, and a member that
/// releases what it was told to gets its whole target eventually.
#[test]
fn no_partition_ever_has_two_holders() {
    for seed in 0..300 {
        let mut rng = Lcg(seed);
        let mut g = GroupMeta::default();
        // What each member really reads: it gives up revoked partitions
        // only when it reports them released.
        let mut reading: BTreeMap<String, BTreeSet<Tp>> = BTreeMap::new();
        let mut next = 0;
        for _ in 0..200 {
            match rng.below(10) {
                0 | 1 => {
                    next += 1;
                    let id = format!("m{next}");
                    let subs: Vec<u64> = [7, 8].into_iter().filter(|_| rng.below(3) > 0).collect();
                    g.members.insert(id.clone(), member(&subs));
                    g.epoch += 1;
                    g.retarget(four);
                    reading.insert(id, BTreeSet::new());
                }
                2 if !g.members.is_empty() => {
                    // Leaves (or is removed after its timeout): it has stopped.
                    let id = g.members.keys().nth(rng.below(g.members.len() as u64) as usize).unwrap().clone();
                    g.members.remove(&id);
                    reading.remove(&id);
                    g.epoch += 1;
                    g.retarget(four);
                }
                _ if !g.members.is_empty() => {
                    let id = g.members.keys().nth(rng.below(g.members.len() as u64) as usize).unwrap().clone();
                    // Sometimes it has released what was revoked by now.
                    let r = reading.get_mut(&id).unwrap();
                    let report = if rng.below(2) == 0 {
                        r.retain(|tp| g.members[&id].assigned.contains(tp));
                        Some(r.clone())
                    } else {
                        None
                    };
                    g.reconcile(&id, report.as_ref());
                    let m = &g.members[&id];
                    r.extend(m.assigned.iter().copied());
                    assert!(r.iter().all(|tp| m.holds(tp)), "seed {seed}: {id} reads what it does not hold");
                }
                _ => {}
            }
            let mut holders: BTreeMap<Tp, &String> = BTreeMap::new();
            for (id, m) in &g.members {
                for tp in m.assigned.iter().chain(m.revoking.iter()) {
                    if let Some(other) = holders.insert(*tp, id) {
                        panic!("seed {seed}: {tp:?} held by {other} and {id}");
                    }
                }
            }
        }
        // Settling: everyone releases what it should and heartbeats.
        for _ in 0..3 {
            let ids: Vec<String> = g.members.keys().cloned().collect();
            for id in ids {
                let owned = g.members[&id].assigned.clone();
                g.reconcile(&id, Some(&owned));
            }
        }
        for (id, m) in &g.members {
            assert_eq!(m.assigned, m.target, "seed {seed}: {id} did not converge");
            assert!(m.revoking.is_empty());
        }
    }
}

#[test]
fn records_and_state_round_trip() {
    let mut g = GroupMeta { epoch: 3, members: BTreeMap::new() };
    let mut m = member(&[7]);
    m.epoch = 2;
    m.target = [(7, 0), (7, 1)].into();
    m.assigned = [(7, 0)].into();
    m.revoking = [(7, 3)].into();
    g.members.insert("m1.1".into(), m);
    let mut s = GroupsState::default();
    let rec = group_record("billing", &g);
    let batch = lotse_record::Batch::parse(&rec).unwrap().0;
    let Ok(Some(lotse_record::Control::Internal(_, payload))) = lotse_record::control(&batch) else { panic!() };
    s.apply_group(&payload).unwrap();
    assert_eq!(s.groups["billing"], g);
    let c = Committed { offset: 42, leader_term: 3, metadata: Some("x".into()) };
    let rec = offsets_record("billing", &[((7, 0), c.clone())]);
    let batch = lotse_record::Batch::parse(&rec).unwrap().0;
    let Ok(Some(lotse_record::Control::Internal(_, payload))) = lotse_record::control(&batch) else { panic!() };
    s.apply_offsets(&payload).unwrap();
    assert_eq!(s.offsets[&("billing".to_string(), 7, 0)], c);
    let mut b = BytesMut::new();
    s.encode(&mut b);
    let bytes = b.freeze();
    assert_eq!(GroupsState::decode(&mut &bytes[..]).unwrap(), s);
    for cut in 0..bytes.len() {
        assert!(GroupsState::decode(&mut &bytes[..cut]).is_err(), "cut at {cut}");
    }
    // A group without members is gone.
    let rec = group_record("billing", &GroupMeta { epoch: 4, members: BTreeMap::new() });
    let batch = lotse_record::Batch::parse(&rec).unwrap().0;
    let Ok(Some(lotse_record::Control::Internal(_, payload))) = lotse_record::control(&batch) else { panic!() };
    s.apply_group(&payload).unwrap();
    assert!(!s.groups.contains_key("billing"));
}

#[test]
fn instance_ids_and_patterns_round_trip_and_old_forms_stay_readable() {
    let mut g = GroupMeta { epoch: 3, ..GroupMeta::default() };
    g.members.insert("a".into(), member(&[7]));
    g.members.insert("b".into(), MemberMeta { instance_id: Some("w1".into()), ..member(&[7]) });
    g.members.insert(
        "c".into(),
        MemberMeta { instance_id: Some("w2".into()), pattern: Some("orders-.*".into()), ..member(&[7, 8]) },
    );
    g.members.insert("d".into(), MemberMeta { pattern: Some("x|y".into()), ..member(&[]) });
    let record = group_record("g", &g);
    let mut s = GroupsState::default();
    let payload = match lotse_record::control(&lotse_record::Batch::parse(&record).unwrap().0) {
        Ok(Some(lotse_record::Control::Internal(_, p))) => p,
        other => panic!("{other:?}"),
    };
    assert_eq!(payload[0], VERSION_EXTRAS);
    s.apply_group(&payload).unwrap();
    assert_eq!(s.groups["g"], g, "a record carries both");
    let mut b = BytesMut::new();
    s.encode(&mut b);
    s.encode_instances(&mut b);
    let mut r = &b[..];
    let mut back = GroupsState::decode(&mut r).unwrap();
    back.decode_instances(&mut r).unwrap();
    assert!(r.is_empty());
    assert_eq!(back, s, "and so does a snapshot");

    // Without either, the record is version 1 and the snapshot has nothing
    // at its end: as before instance ids and patterns.
    let mut plain = GroupMeta::default();
    plain.members.insert("a".into(), member(&[7]));
    let record = group_record("p", &plain);
    let payload = match lotse_record::control(&lotse_record::Batch::parse(&record).unwrap().0) {
        Ok(Some(lotse_record::Control::Internal(_, p))) => p,
        other => panic!("{other:?}"),
    };
    assert_eq!(payload[0], VERSION);
    let mut only = GroupsState::default();
    only.apply_group(&payload).unwrap();
    let mut b = BytesMut::new();
    only.encode_instances(&mut b);
    assert!(b.is_empty());
}

/// The payload of a coordinator record.
fn payload_of(record: &Bytes) -> Bytes {
    match lotse_record::control(&lotse_record::Batch::parse(record).unwrap().0) {
        Ok(Some(lotse_record::Control::Internal(lotse_record::InternalControl::GroupState, p))) => p,
        other => panic!("{other:?}"),
    }
}

/// Takes group `id` in `s` from what it holds to `after` with a record of
/// what changed, as the coordinator does, and returns the record's size.
fn step(s: &mut GroupsState, id: &str, after: &GroupMeta) -> usize {
    let before = s.groups.get(id).cloned();
    let record = group_change_record(id, before.as_ref(), after);
    s.apply_group(&payload_of(&record)).unwrap();
    record.len()
}

#[test]
fn a_change_record_brings_a_replica_to_what_the_coordinator_computed() {
    // The coordinator's own view, changed step by step as a heartbeat or an
    // expiry would, and a replica that only applies the records.
    let mut g = GroupMeta::default();
    let mut replica = GroupsState::default();
    let mut check = |g: &GroupMeta, what: &str| {
        step(&mut replica, "g", g);
        if g.members.is_empty() {
            assert!(!replica.groups.contains_key("g"), "{what}: a group without members is gone");
        } else {
            assert_eq!(replica.groups["g"], *g, "{what}");
        }
    };
    g.members.insert("a".into(), MemberMeta { epoch: 1, ..member(&[7]) });
    g.epoch += 1;
    g.retarget(four);
    check(&g, "a joins a group that did not exist");
    g.reconcile("a", None);
    check(&g, "a takes its partitions");
    g.members.insert("b".into(), MemberMeta { epoch: 1, instance_id: Some("w1".into()), ..member(&[7]) });
    g.members.insert("c".into(), MemberMeta { epoch: 1, pattern: Some("orders-.*".into()), ..member(&[7, 8]) });
    g.epoch += 1;
    g.retarget(four);
    check(&g, "b and c join, every target moves");
    g.reconcile("a", None);
    check(&g, "a revokes what goes to the others");
    let released = g.members["a"].assigned.clone();
    g.reconcile("a", Some(&released));
    check(&g, "a releases");
    for m in ["b", "c"] {
        g.reconcile(m, None);
        check(&g, m);
    }
    g.members.remove("b");
    g.epoch += 1;
    g.retarget(four);
    check(&g, "b leaves");
    g.members.get_mut("c").expect("c").rebalance_timeout_ms = 9_000;
    check(&g, "a member changes one setting");
    check(&g, "nothing changed");
    g.members.clear();
    g.epoch += 1;
    check(&g, "the last members expire");
}

#[test]
fn a_reconcile_writes_only_the_member_it_moves() {
    let mut g = GroupMeta { epoch: 1, ..GroupMeta::default() };
    for i in 0..100 {
        g.members.insert(format!("m{i:03}"), MemberMeta { epoch: 1, ..member(&[7]) });
    }
    let hundred = |_: u64| 100;
    g.retarget(hundred);
    let mut s = GroupsState::default();
    step(&mut s, "g", &g);
    let mut after = g.clone();
    assert!(after.reconcile("m042", None));
    let one = step(&mut s, "g", &after);
    assert_eq!(s.groups["g"], after);
    // Before ADR 0125 this step wrote all hundred members; now it writes
    // one, plus the batch's own header.
    let whole = group_record("g", &after).len();
    assert!(one * 25 < whole, "one member: {one} bytes, the whole group: {whole} bytes");
    let payload = payload_of(&group_change_record("g", Some(&g), &after));
    assert_eq!(payload[0], VERSION_DELTA);
}

#[test]
fn a_change_to_a_forgotten_group_leaves_it_forgotten() {
    let mut g = GroupMeta { epoch: 1, ..GroupMeta::default() };
    g.members.insert("a".into(), MemberMeta { epoch: 1, ..member(&[7]) });
    g.members.insert("b".into(), MemberMeta { epoch: 1, ..member(&[7]) });
    let mut after = g.clone();
    after.members.get_mut("a").expect("a").epoch = 2;
    // Computed while the group existed; applied after a Forget dropped it.
    let record = group_change_record("g", Some(&g), &after);
    let mut s = GroupsState::default();
    s.apply_group(&payload_of(&record)).unwrap();
    assert!(!s.groups.contains_key("g"), "not back with only member a");
    // A group that did not exist is created by its first record.
    let record = group_change_record("g", None, &after);
    s.apply_group(&payload_of(&record)).unwrap();
    assert_eq!(s.groups["g"], after);
}

#[test]
fn a_damaged_change_record_is_refused() {
    let mut g = GroupMeta { epoch: 2, ..GroupMeta::default() };
    g.members.insert("a".into(), MemberMeta { pattern: Some("p".into()), ..member(&[7]) });
    g.members.insert("b".into(), MemberMeta { instance_id: Some("i".into()), ..member(&[7]) });
    let before = GroupMeta { epoch: 1, members: [("z".to_string(), member(&[7]))].into() };
    let payload = payload_of(&group_change_record("g", Some(&before), &g));
    for cut in 1..payload.len() {
        let mut s = GroupsState { groups: [("g".to_string(), before.clone())].into(), ..GroupsState::default() };
        assert!(s.apply_group(&payload[..cut]).is_err(), "cut at {cut}");
    }
    let mut longer = payload.to_vec();
    longer.push(0);
    assert!(GroupsState::default().apply_group(&longer).is_err(), "trailing bytes");
}

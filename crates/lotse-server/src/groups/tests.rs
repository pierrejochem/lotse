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

use super::*;
use lotse_sim::Sim;

fn config(peers: &[NodeId], id: NodeId, join: bool) -> NodeConfig {
    NodeConfig {
        id,
        listen: format!("n{id}:7000"),
        peers: peers.iter().map(|p| (*p, format!("n{p}:7000"))).collect(),
        groups: vec![1, 2],
        tick: Duration::from_millis(10),
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: None,
        client_addrs: BTreeMap::new(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: crate::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: crate::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: crate::DEFAULT_MAX_PRODUCERS,
        auth: AuthConfig::none(),
        retention: RetentionConfig::keep(),
        join,
    }
}

#[test]
fn the_first_voters_are_those_of_the_first_start() {
    Sim::new(1).run(|rt| async move {
        let first = first_voters(&rt, &config(&[1, 2, 3], 1, false)).await.unwrap();
        assert_eq!(first, vec![1, 2, 3]);
        // A power loss, then a configuration that lists a node added since.
        rt.crash();
        let again = first_voters(&rt, &config(&[1, 2, 3, 4], 1, false)).await.unwrap();
        assert_eq!(again, vec![1, 2, 3], "a longer node list never changes who the groups started with");
        let host = rt.node("n4");
        let joining = first_voters(&host, &config(&[1, 2, 3, 4], 4, true)).await.unwrap();
        assert_eq!(joining, vec![1, 2, 3], "a joining node starts outside them");
    });
}

#[test]
fn a_moved_replica_leaves_only_its_term_and_vote() {
    use lotse_raft::HardState;
    use lotse_rt::{Disk, OpenMode, SegmentFile};
    Sim::new(2).run(|rt| async move {
        let dir = group_dir(42);
        rt.create_dir_all(&dir.join("log")).await.unwrap();
        let hs = HardState { term: 7, vote: Some(2), commit: 9 };
        crate::replica::write_atomic(&rt, &dir, crate::replica::HARD_STATE, crate::replica::encode_hard_state(&hs))
            .await
            .unwrap();
        for f in [dir.join("snapshot"), dir.join("log").join("00000000000000000000.log")] {
            let file = rt.open(&f, OpenMode::CreateOrOpen).await.unwrap();
            file.append(Bytes::from_static(b"data")).await.unwrap();
            file.sync().await.unwrap();
        }
        forget_group_log(&rt, &dir, 31).await.unwrap();
        let kept = crate::replica::read_file(&rt, &dir.join(crate::replica::HARD_STATE)).await.unwrap().unwrap();
        let kept = crate::replica::decode_hard_state(kept).unwrap();
        assert_eq!(
            kept,
            HardState { term: 7, vote: Some(2), commit: 0 },
            "the term and vote stay, nothing is committed"
        );
        let mut left = rt.list(&dir).await.unwrap();
        left.sort();
        assert_eq!(left, vec![dir.join(crate::replica::MOVED), dir.join(crate::replica::HARD_STATE)]);
        assert!(rt.list(&dir.join("log")).await.is_err(), "the log is gone");
        // A replica of the group opens from that state again, empty.
        let raft = Config::new(1, vec![1, 2, 3]);
        let r = Replica::open_with(rt.clone(), dir, raft, NoState, LogConfig::default(), ReplicaOptions::default())
            .await
            .unwrap();
        assert_eq!(r.raft_ref().term(), 7);
        assert_eq!(r.raft_ref().last_index(), 0);
        // It grants no vote, not even to a candidate in a newer term, until
        // it holds a membership newer than the move.
        let mut r = r;
        let ask = |term| lotse_raft::Message::Vote { term, last_index: 3, last_term: 7, pre: false, transfer: false };
        r.step(1, ask(8));
        assert_eq!(r.raft_ref().term(), 7, "the request did not even reach Raft");
        // A group never held here leaves nothing behind.
        forget_group_log(&rt, &group_dir(43), 31).await.unwrap();
        assert!(rt.list(&group_dir(43)).await.is_err());
    });
}

#[test]
fn a_moved_replica_votes_again_once_placed_here_again() {
    use crate::replica::may_vote;
    assert!(may_vote(None, 0), "a replica never moved votes");
    assert!(!may_vote(Some(31), 0) && !may_vote(Some(31), 31), "not with the membership it had, or older");
    assert!(may_vote(Some(31), 32), "with the membership that placed it here again");
}

#[derive(Debug, Default)]
struct NoState;

impl StateMachine for NoState {
    fn apply(&mut self, _: &lotse_record::Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn a_node_not_listed_yet_is_heard_only_for_the_metadata_group() {
    let beat = |g| (g, lotse_raft::Message::TimeoutNow { term: 9 });
    let frame = || (5, vec![beat(1), beat(7)]);
    let groups = |i: Option<Incoming>| i.map(|(_, m)| m.into_iter().map(|(g, _)| g).collect::<Vec<_>>());
    assert_eq!(groups(heard(5, true, false, Some(1), frame())), Some(vec![1, 7]), "a listed node, for every group");
    assert_eq!(groups(heard(5, false, true, Some(1), frame())), Some(vec![1]), "a learned one, for the metadata group");
    assert_eq!(groups(heard(5, false, false, Some(1), frame())), None, "anyone else, not at all");
    assert_eq!(groups(heard(6, true, false, Some(1), frame())), None, "nor a node speaking for another");
}

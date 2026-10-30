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

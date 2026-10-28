//! Node links on a network that verifies peers (TLS in production): a node
//! hears only the members of its cluster, each only for itself, and talks
//! only to the node it meant to reach.
//!
//! Node 1 of a three-node cluster runs alone, so its term stays where it is
//! (pre-vote) unless a frame it accepts says otherwise. Outsiders with
//! various certificates send it a heartbeat of a high term from node 2.

use std::collections::BTreeMap;
use std::future::Future;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use lotse_log::LogConfig;
use lotse_raft::{Message, NodeId};
use lotse_record::Batch;
use lotse_rt::{Clock, Conn, Link, Listener, Net, PeerIdentity, Spawn};
use lotse_server::{Node, NodeConfig, StateMachine, wire};
use lotse_sim::{Sim, SimRuntime, check_seeds};

#[derive(Debug, Default)]
struct Nothing;

impl StateMachine for Nothing {
    fn apply(&mut self, _: &Batch) {}
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

const GROUP: u64 = 1;

fn config(id: NodeId) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|i| (i, format!("n{i}:7000"))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        groups: vec![GROUP],
        tick: Duration::from_millis(10),
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: None,
        client_addrs: BTreeMap::new(),
        streams: Vec::new(),
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: None,
        coordinator: None,
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
    }
}

async fn start_node_1(rt: &SimRuntime) -> Node<SimRuntime> {
    rt.net().verify_peers(true);
    let host = rt.node("n1").with_identity(Some(PeerIdentity::Node(1)));
    Node::start(host, config(1), |_| Nothing).await.unwrap()
}

/// Node 1's term after `identity` sent it, on a node link, a heartbeat of
/// term 50 that says it comes from node `from`.
fn term_after_heartbeat(seed: u64, identity: PeerIdentity, from: NodeId) -> u64 {
    Sim::new(seed).run(move |rt| async move {
        let node = start_node_1(&rt).await;
        let outsider = rt.node("x").with_identity(Some(identity));
        rt.sleep(Duration::from_millis(200)).await;
        let heartbeat =
            Message::Append { term: 50, prev_index: 0, prev_term: 0, entries: Vec::new(), commit: 0, seq: 1 };
        // Connections may be dropped: keep trying for a while.
        for _ in 0..20 {
            if let Ok(conn) = outsider.connect("n1:7000", Link::Node).await {
                let _ = conn.send(wire::encode(from, &[(GROUP, heartbeat.clone())])).await;
                rt.sleep(Duration::from_millis(50)).await;
            }
        }
        let term = node.handle().status(GROUP).unwrap().term;
        node.handle().stop();
        node.join().await.unwrap();
        term
    })
}

#[test]
fn a_member_is_heard() {
    check_seeds(10, |seed| {
        assert_eq!(term_after_heartbeat(seed, PeerIdentity::Node(2), 2), 50, "node 2 may speak for node 2");
    });
}

#[test]
fn a_node_outside_the_cluster_is_not_heard() {
    check_seeds(10, |seed| {
        assert!(term_after_heartbeat(seed, PeerIdentity::Node(9), 9) < 50, "node 9 is not a member");
    });
}

#[test]
fn a_member_speaks_only_for_itself() {
    check_seeds(10, |seed| {
        assert!(term_after_heartbeat(seed, PeerIdentity::Node(3), 2) < 50, "node 3 may not speak for node 2");
    });
}

#[test]
fn a_principal_cannot_open_a_node_link() {
    check_seeds(10, |seed| {
        assert!(term_after_heartbeat(seed, PeerIdentity::Principal("node-2".into()), 2) < 50);
    });
}

/// Frames node 1 sends to whoever listens at node 2's address with
/// `identity`.
fn frames_to(seed: u64, identity: PeerIdentity) -> usize {
    Sim::new(seed).run(move |rt| async move {
        let node = start_node_1(&rt).await;
        let listener = rt.node("n2").with_identity(Some(identity)).listen("n2:7000", Link::Node).await.unwrap();
        let counter = rt.clone();
        let got = rt.spawn(async move {
            let mut n = 0;
            let deadline = counter.now() + Duration::from_secs(3);
            while counter.now() < deadline {
                let accepted = within(&counter, listener.accept(), deadline).await;
                let Some(Ok((conn, _))) = accepted else { break };
                while let Some(Ok(Some(_))) = within(&counter, conn.recv(), deadline).await {
                    n += 1;
                }
            }
            n
        });
        let n = got.await;
        node.handle().stop();
        node.join().await.unwrap();
        n
    })
}

/// `f`, unless `deadline` comes first.
async fn within<T>(rt: &SimRuntime, f: impl Future<Output = T>, deadline: lotse_rt::Instant) -> Option<T> {
    let mut f = std::pin::pin!(f);
    let mut sleep = std::pin::pin!(rt.sleep(deadline - rt.now()));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(v) = f.as_mut().poll(cx) {
            return Poll::Ready(Some(v));
        }
        sleep.as_mut().poll(cx).map(|()| None)
    })
    .await
}

#[test]
fn a_node_talks_to_the_node_it_meant() {
    check_seeds(10, |seed| {
        assert!(frames_to(seed, PeerIdentity::Node(2)) > 0, "node 2 hears node 1's pre-votes");
        assert_eq!(frames_to(seed, PeerIdentity::Node(9)), 0, "node 9 at node 2's address hears nothing");
    });
}

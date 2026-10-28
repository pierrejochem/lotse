//! Three real nodes on localhost: Tokio, TCP and the local disk, with a
//! client speaking the native protocol.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig};
use lotse_log::LogConfig;
use lotse_raft::{NodeId, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt_tokio::TokioRuntime;
use lotse_server::wire::GroupId;
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine, StreamConfig};

#[derive(Debug, Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Bytes>>>);

impl StateMachine for Recorder {
    fn apply(&mut self, batch: &Batch) {
        self.0.lock().unwrap().push(batch.records().unwrap()[0].value.clone().unwrap_or_default());
    }
    fn snapshot(&self) -> Bytes {
        Bytes::new()
    }
    fn restore(&mut self, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A started node, its handle and what each of its groups applied.
type Running = (Node<TokioRuntime>, NodeHandle, BTreeMap<GroupId, Recorder>);

struct Cluster {
    root: PathBuf,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    nodes: BTreeMap<NodeId, Running>,
}

impl Cluster {
    async fn start(&mut self, id: NodeId) {
        let rt = TokioRuntime::new(self.root.join(format!("node-{id}")));
        std::fs::create_dir_all(rt.root()).unwrap();
        let recorders: BTreeMap<GroupId, Recorder> = [1, 2, 3].iter().map(|g| (*g, Recorder::default())).collect();
        let made = recorders.clone();
        let node = Node::start(rt, self.cfgs[&id].clone(), move |g| made[&g].clone()).await.unwrap();
        let handle = node.handle();
        self.nodes.insert(id, (node, handle, recorders));
    }

    async fn leader(&self, g: GroupId) -> (NodeId, NodeHandle) {
        for _ in 0..200 {
            for (id, (_, h, _)) in &self.nodes {
                if h.status(g).is_some_and(|s| s.role == Role::Leader) {
                    return (*id, h.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no leader for group {g}");
    }

    async fn everywhere(&self, g: GroupId, value: &str) {
        for _ in 0..300 {
            if self.nodes.values().all(|(_, _, r)| r[&g].0.lock().unwrap().iter().any(|v| v == value.as_bytes())) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("{value} did not reach every node in group {g}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_replicate_over_tcp_and_survive_a_restart() {
    let root = std::env::temp_dir().join(format!("lotse-tokio-cluster-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let clients: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    // Partition 0 is group 1, partition 1 is group 2.
    let streams = vec![StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![1, 2] }];
    let cfgs = peers
        .keys()
        .map(|&id| {
            (
                id,
                NodeConfig {
                    id,
                    listen: peers[&id].clone(),
                    peers: peers.clone(),
                    // Group 3 coordinates producers.
                    groups: vec![1, 2, 3],
                    tick: Duration::from_millis(10),
                    pre_vote: true,
                    log: LogConfig::default(),
                    client_listen: Some(clients[&id].clone()),
                    client_addrs: clients.clone(),
                    streams: streams.clone(),
                    queues: Vec::new(),
                    exchanges: Vec::new(),
                    group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
                    group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
                    coordinator: Some(3),
                    max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                },
            )
        })
        .collect();
    let mut c = Cluster { root: root.clone(), cfgs, nodes: BTreeMap::new() };
    for id in 1..=3 {
        c.start(id).await;
    }
    for g in [1, 2] {
        let (_, leader) = c.leader(g).await;
        leader.propose(g, batch(&format!("first-{g}"))).await.unwrap();
        c.everywhere(g, &format!("first-{g}")).await;
    }
    let client =
        Client::new(TokioRuntime::new(std::env::temp_dir()), ClientConfig::new(clients.values().cloned().collect()));
    let mut acked = Vec::new();
    for p in 0..2 {
        let at = client.produce("orders", p, batch(&format!("client-{p}"))).await.unwrap();
        acked.push((p, at, format!("client-{p}")));
    }

    // Stop group 1's leader; the others elect a new one and keep writing.
    let (old, _) = c.leader(1).await;
    let (node, handle, _) = c.nodes.remove(&old).unwrap();
    handle.stop();
    node.join().await.unwrap();
    let (new, leader) = c.leader(1).await;
    assert_ne!(new, old);
    leader.propose(1, batch("while-down")).await.unwrap();
    // The client finds the new leader by itself.
    let at = client.produce("orders", 0, batch("client-while-down")).await.unwrap();
    acked.push((0, at, "client-while-down".into()));

    // The old leader comes back from its disk and catches up.
    c.start(old).await;
    c.everywhere(1, "while-down").await;
    assert!(
        c.nodes[&old].2[&1].0.lock().unwrap().iter().any(|v| v == &b"first-1"[..]),
        "the restarted node re-applied its log from disk"
    );
    for (p, at, value) in acked {
        let f = client.fetch("orders", p, at, Duration::from_secs(1)).await.unwrap();
        let (b, _) = Batch::parse(&f.records).unwrap();
        assert_eq!(b.base_offset(), at);
        assert_eq!(b.records().unwrap()[0].value.as_deref(), Some(value.as_bytes()), "read back over TCP");
    }
    drop(client);

    for (_, (node, handle, _)) in std::mem::take(&mut c.nodes) {
        handle.stop();
        node.join().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&root);
}

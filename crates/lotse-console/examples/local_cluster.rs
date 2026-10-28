//! Three Lotse nodes on localhost with the console in front, for trying the
//! console against a real cluster:
//!
//! ```sh
//! cd console/web && npm ci && npm run build && cd ../..
//! cargo run -p lotse-console --example local_cluster
//! ```
//!
//! The console listens on `127.0.0.1:8080` (or the first argument). The
//! nodes keep their data in a fresh temporary directory, and a client
//! writes a little to the `orders` stream and the `jobs` queue every
//! second so there is something to see. Stop with Ctrl-C.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig};
use lotse_console::auth::AuthConfig;
use lotse_console::cluster::Cluster;
use lotse_console::cluster::native::NativeCluster;
use lotse_console::metrics::{MetricsStore, run_sampler};
use lotse_console::{AppState, router};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt_tokio::TokioRuntime;
use lotse_server::{Node, NodeConfig, QueueConfig, StateMachine, StreamConfig};

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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn batch(key: &str, value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, Some(key.as_bytes()), Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let listen = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:8080".into());
    let root = std::env::temp_dir().join(format!("lotse-local-cluster-{}", std::process::id()));
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let clients: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let cfg = NodeConfig {
            id,
            listen: peers[&id].clone(),
            peers: peers.clone(),
            // 1 metadata, 2 coordinator, 3-5 the stream's partitions, 6 the queue.
            groups: vec![1, 2, 3, 4, 5, 6],
            tick: Duration::from_millis(10),
            pre_vote: true,
            log: LogConfig::default(),
            client_listen: Some(clients[&id].clone()),
            client_addrs: clients.clone(),
            streams: vec![StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![3, 4, 5] }],
            queues: vec![QueueConfig {
                name: "jobs".into(),
                resource_id: 8,
                group: 6,
                ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
            }],
            exchanges: Vec::new(),
            group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
            group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
            metadata: Some(1),
            coordinator: Some(2),
            max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        };
        let rt = TokioRuntime::new(root.join(format!("node-{id}")));
        std::fs::create_dir_all(rt.root()).expect("a temporary directory");
        nodes.push(Node::start(rt, cfg, |_| Nothing).await.expect("the node starts"));
    }
    let bootstrap: Vec<String> = clients.values().cloned().collect();
    tracing::info!(?bootstrap, data = %root.display(), "three nodes running");

    // A little traffic, so the screens have something to show.
    let writer = Client::new(TokioRuntime::new(&root), ClientConfig::new(bootstrap.clone()));
    tokio::spawn(async move {
        let mut i = 0u64;
        loop {
            let _ = writer
                .produce("orders", (i % 3) as u32, batch(&format!("customer-{}", i % 17), &format!("order {i}")))
                .await;
            if i % 2 == 0 {
                let _ = writer.produce("jobs", 0, batch("job", &format!("job {i}"))).await;
            }
            i += 1;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });

    let store = Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));
    let clusters: Vec<Arc<dyn Cluster>> = vec![Arc::new(NativeCluster::new("local", bootstrap))];
    let who = lotse_console::auth::Identity {
        principal: "console-metrics".into(),
        display_name: "metrics sampler".into(),
        token: None,
    };
    tokio::spawn(run_sampler(clusters.clone(), Arc::clone(&store), who, Duration::from_secs(10)));
    let state = AppState::new(clusters, AuthConfig::Dev { user: "local".into() }, store, Duration::from_secs(2));
    let listener = tokio::net::TcpListener::bind(&listen).await.expect("the console's address is free");
    tracing::info!(%listen, "console ready");
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap();
    for node in nodes {
        node.handle().stop();
        let _ = node.join().await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

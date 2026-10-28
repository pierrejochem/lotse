//! The console in front of a real cluster: three nodes on localhost (Tokio,
//! TCP, the local disk), a client writing to them, and the console's HTTP
//! API reading what they report.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use http_body_util::BodyExt;
use lotse_client::{Client, ClientConfig};
use lotse_console::auth::AuthConfig;
use lotse_console::cluster::Cluster;
use lotse_console::cluster::native::{NativeCluster, RATE_WINDOW};
use lotse_console::metrics::MetricsStore;
use lotse_console::{AppState, router};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt_tokio::TokioRuntime;
use lotse_server::{Node, NodeConfig, QueueConfig, StateMachine, StreamConfig};
use serde_json::{Value, json};
use tower::ServiceExt;

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

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, Some(b"k"), Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    call(app, "GET", uri, None).await
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_shows_a_real_cluster() {
    let root = std::env::temp_dir().join(format!("lotse-console-native-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let clients: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let cfg = NodeConfig {
            id,
            listen: peers[&id].clone(),
            peers: peers.clone(),
            groups: vec![1, 2, 3, 4],
            tick: Duration::from_millis(10),
            pre_vote: true,
            log: LogConfig::default(),
            client_listen: Some(clients[&id].clone()),
            client_addrs: clients.clone(),
            streams: vec![StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![3] }],
            queues: vec![QueueConfig {
                name: "jobs".into(),
                resource_id: 8,
                group: 4,
                ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
            }],
            exchanges: Vec::new(),
            group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
            group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
            metadata: Some(1),
            coordinator: Some(2),
            max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
            auth: lotse_server::AuthConfig::none(),
        };
        let rt = TokioRuntime::new(root.join(format!("node-{id}"))).plaintext_on_loopback();
        std::fs::create_dir_all(rt.root()).unwrap();
        nodes.push(Node::start(rt, cfg, |_| Nothing).await.unwrap());
    }
    let bootstrap: Vec<String> = clients.values().cloned().collect();
    let client = Client::new(TokioRuntime::new(&root).plaintext_on_loopback(), ClientConfig::new(bootstrap.clone()));
    for i in 0..20 {
        client.produce("orders", 0, batch(&format!("order {i}"))).await.expect("produce");
    }
    for i in 0..5 {
        client.produce("jobs", 0, batch(&format!("job {i}"))).await.expect("queue message");
    }

    let store = Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));
    let clusters: Vec<Arc<dyn Cluster>> = vec![Arc::new(NativeCluster::new("local", bootstrap))];
    let app = router(AppState::new(clusters, AuthConfig::Dev { user: "pierre".into() }, store, Duration::from_secs(1)));

    let (status, list) = get(&app, "/api/v1/clusters").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list[0]["nodes"], 3);
    assert_eq!((list[0]["demo"].as_bool(), list[0]["tenants"].as_bool()), (Some(false), Some(false)));
    assert_eq!(list[0]["status"], "all answering");

    // The first look: everything there, but no rates yet.
    let (status, o) = get(&app, "/api/v1/clusters/local/overview").await;
    assert_eq!(status, StatusCode::OK, "{o}");
    assert_eq!((o["stream_count"].as_u64(), o["queue_count"].as_u64()), (Some(1), Some(1)));
    assert_eq!(o["raft_groups"], 4);
    assert!(o["metadata_leader"].as_str().unwrap().starts_with("node-"), "{o}");
    assert!(o["throughput"]["in_bytes_per_sec"].is_null(), "a rate needs two samples: {o}");
    assert!(o["alerts"].as_array().unwrap().is_empty(), "{o}");
    let node = &o["nodes"][0];
    assert!(node["disk_total_bytes"].as_u64().unwrap() > 0 && node["memory_bytes"].as_u64().unwrap() > 0, "{node}");
    assert!(node["zone"].is_null() && o["disk_limits"].is_null());
    assert_eq!(o["attention_queues"][0]["name"], "jobs", "ready messages and nobody consuming: {o}");

    // Traffic, then a second look after the rate window: rates appear.
    for i in 20..40 {
        client.produce("orders", 0, batch(&format!("order {i}"))).await.expect("produce");
    }
    tokio::time::sleep(RATE_WINDOW + Duration::from_millis(500)).await;
    let (_, o) = get(&app, "/api/v1/clusters/local/overview").await;
    let rate = o["throughput"]["in_bytes_per_sec"].as_f64();
    assert!(rate.is_some_and(|r| r > 0.0), "produces in the window show as a rate: {o}");
    assert!(o["throughput"]["commit_p99_ms"].as_f64().is_some_and(|p| p >= 1.0), "{o}");

    // The stream: partition, leader, replicas in sync, the records.
    let (status, s) = get(&app, "/api/v1/clusters/local/streams/orders").await;
    assert_eq!(status, StatusCode::OK, "{s}");
    let p = &s["partitions"][0];
    assert!(p["high_watermark"].as_u64().unwrap() >= 40, "{p}");
    assert_eq!(p["replicas"].as_array().unwrap().len(), 3, "{p}");
    assert_eq!(s["replication"], 3);
    assert!(s["tenant"].is_null() && s["retention_ms"].is_null());
    let (status, page) = get(&app, "/api/v1/clusters/local/streams/orders/records?partition=0&offset=3&count=5").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let records = page["records"].as_array().unwrap();
    // Raft's own entries (a new leader's no-op) take offsets too, but are
    // not records: the page holds orders, in order, from offset 3 on.
    assert_eq!(records.len(), 5, "{page}");
    let number = |r: &Value| r["value"].as_str().unwrap().strip_prefix("order ").unwrap().parse::<u32>().unwrap();
    assert!(records[0]["offset"].as_u64().unwrap() >= 3 && records[0]["key"] == "k", "{page}");
    assert!(records.windows(2).all(|w| number(&w[1]) == number(&w[0]) + 1), "{page}");
    assert_eq!(page["next_offset"].as_u64(), Some(records[4]["offset"].as_u64().unwrap() + 1));

    // The queue: messages ready, nobody consuming.
    let (status, q) = get(&app, "/api/v1/clusters/local/queues/jobs").await;
    assert_eq!(status, StatusCode::OK, "{q}");
    assert_eq!((q["ready"].as_u64(), q["in_flight"].as_u64()), (Some(5), Some(0)), "{q}");
    assert!(q["levels"][0]["leader"].as_str().unwrap().starts_with("node-"));

    // Creating a queue: what the cluster supports goes through, the rest is
    // refused with a reason.
    let plain = json!({"name": "emails", "priority_levels": 1, "single_active": false, "create_dlq": false});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(plain.clone())).await;
    assert_eq!(status, StatusCode::CREATED, "{done}");
    let (status, _) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(plain)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let fancy = json!({"name": "vip", "priority_levels": 3, "single_active": true, "create_dlq": false});
    let (status, err) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(fancy)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(err["message"].as_str().unwrap().contains("priority levels, single active consumer"), "{err}");
    let mut found = false;
    for _ in 0..50 {
        let (_, list) = get(&app, "/api/v1/clusters/local/queues").await;
        if list.as_array().unwrap().iter().any(|q| q["name"] == "emails") {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "the created queue shows up");

    // What a real cluster does not have.
    let (status, t) = get(&app, "/api/v1/clusters/local/tenants").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{t}");
    let (_, n) = get(&app, "/api/v1/clusters/local/nodes").await;
    assert!(n["upgrade"].is_null() && n["events"].as_array().unwrap().is_empty());

    // A node that stops shows as not answering.
    nodes[2].handle().stop();
    let stopped = nodes.pop().unwrap();
    stopped.join().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let (_, o) = get(&app, "/api/v1/clusters/local/overview").await;
    let down: Vec<&Value> =
        o["nodes"].as_array().unwrap().iter().filter(|n| n["status"]["text"] == "Not answering").collect();
    assert_eq!(down.len(), 1, "{o}");
    assert!(o["alerts"].as_array().unwrap().iter().any(|a| a["tone"] == "critical"), "{o}");

    drop(client);
    for node in nodes {
        node.handle().stop();
        node.join().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&root);
}

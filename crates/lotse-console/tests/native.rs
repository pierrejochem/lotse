//! The console in front of a real cluster: three nodes on localhost (Tokio,
//! TCP, the local disk), a client writing to them, and the console's HTTP
//! API reading what they report.

use std::collections::{BTreeMap, BTreeSet};
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

/// A port nothing listens on, below the ephemeral range (32768 and up on
/// Linux, 49152 and up elsewhere): a port the system hands out could be
/// taken by an outgoing connection between this check and the node binding
/// it.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1_200) as u16 * 10;
    loop {
        let port = 20_000 + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, Some(b"k"), Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    call(app, "GET", uri, None).await
}

/// Who leads partition 0 of `name`, and in which term: `(0, 0)` while no
/// leader is known.
async fn leader_term(client: &Client<TokioRuntime>, name: &str) -> (u32, u64) {
    let Ok(m) = client.metadata(&[name]).await else { return (0, 0) };
    m.resources
        .iter()
        .filter(|r| r.name == name)
        .flat_map(|r| &r.partitions)
        .find(|p| p.partition == 0)
        .map_or((0, 0), |p| (p.leader, p.leader_term))
}

/// Which node leads each Raft group with a recent quorum (ADR 0095), as the
/// nodes report it: `(every group led, what each node says)`.
async fn led(client: &Client<TokioRuntime>, groups: &[u64]) -> (bool, String) {
    let Ok(all) = client.node_metrics().await else { return (false, "no node answered".into()) };
    let mut seen = Vec::new();
    let mut leaders = BTreeSet::new();
    for (id, m) in all {
        let Ok(m) = m else {
            seen.push(format!("node {id}: no answer"));
            continue;
        };
        for p in m.partitions.iter().filter(|p| groups.contains(&p.group)) {
            seen.push(format!(
                "node {id} group {}: {} term {}",
                p.group,
                if p.leader { "leads" } else { "follows" },
                p.term
            ));
            if p.leader {
                leaders.insert(p.group);
            }
        }
    }
    (groups.iter().all(|g| leaders.contains(g)), seen.join(", "))
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
            zones: (1..=3).map(|i| (i, format!("zone-{i}"))).collect(),
            streams: vec![StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![3] }],
            queues: vec![QueueConfig {
                name: "jobs".into(),
                resource_id: 8,
                group: 4,
                ack_timeout: QueueConfig::DEFAULT_ACK_TIMEOUT,
                limits: Default::default(),
                dead_letter: None,
                single_active: false,
                priority_levels: 0,
                length: Default::default(),
                resume_window: lotse_server::QueueConfig::DEFAULT_RESUME_WINDOW,
            }],
            exchanges: Vec::new(),
            group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
            group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
            metadata: Some(1),
            coordinator: Some(2),
            max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
            producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
            leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
            follower_max_lag: lotse_server::DEFAULT_FOLLOWER_MAX_LAG,
            // One byte: whatever the machine's disk, never low (ADR 0122).
            disk_free_limit: lotse_server::FreeLimit { bytes: 1, permille: 0 },
            disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
            versions: Default::default(),
            retention: lotse_server::RetentionConfig::keep(),
            join: false,
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

    // The overview below reports the cluster as it is now. A new cluster may
    // still be electing (a stalled runner loses a leader's quorum for an
    // election timeout, 100 to 200 ms here): its alerts are right then, but
    // not what the checks below are about. They start once every group is
    // led.
    let mut settled = (false, String::new());
    for _ in 0..150 {
        settled = led(&client, &[1, 2, 3, 4]).await;
        if settled.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(settled.0, "every group gets a leader within 15 s: {}", settled.1);
    let store = Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));
    let clusters: Vec<Arc<dyn Cluster>> = vec![Arc::new(NativeCluster::new("local", bootstrap))];
    let app = router(AppState::new(
        clusters,
        AuthConfig::Dev { user: "pierre".into(), password: None },
        store,
        Duration::from_secs(1),
    ));

    let (status, list) = get(&app, "/api/v1/clusters").await;
    assert_eq!(status, StatusCode::OK, "{list}");
    assert_eq!(list[0]["nodes"], 3);
    assert_eq!((list[0]["demo"].as_bool(), list[0]["tenants"].as_bool()), (Some(false), Some(true)));
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
    // Zones from `Metadata` (ADR 0104), the free space limit (ADR 0122).
    let zones: BTreeSet<&str> = o["nodes"].as_array().unwrap().iter().map(|n| n["zone"].as_str().unwrap()).collect();
    assert_eq!(zones, BTreeSet::from(["zone-1", "zone-2", "zone-3"]), "{o}");
    assert_eq!((node["disk_free_limit_bytes"].as_u64(), node["disk_low"].as_bool()), (Some(1), Some(false)), "{node}");
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
    assert_eq!(s["tenant"], "default");
    assert_eq!(s["retention"], serde_json::json!({ "ms": null, "bytes": null }), "the nodes keep everything");
    // ADR 0122: replicas look at their oldest record once a second.
    assert_eq!(s["open_transactions"].as_u64(), Some(0), "{s}");
    assert!(s["oldest_record_age_ms"].is_u64(), "{s}");
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
    if status != StatusCode::CREATED {
        // Once in many hundred CI runs the metadata group answered
        // NOT_LEADER for the console's whole 5 s; say who led it then.
        panic!("{status}: {done}; the metadata group now: {}", led(&client, &[1]).await.1);
    }
    let (status, _) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(plain)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Priority levels (ADR 0078) go through as well.
    let fancy = json!({"name": "vip", "priority_levels": 3, "single_active": true, "create_dlq": false});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(fancy)).await;
    assert_eq!(status, StatusCode::CREATED, "{done}");
    // Its leader reports each level, highest first, under one leader (ADR 0120).
    let mut levels = Vec::new();
    for _ in 0..50 {
        let (status, q) = get(&app, "/api/v1/clusters/local/queues/vip").await;
        if status == StatusCode::OK {
            levels = q["levels"].as_array().cloned().unwrap_or_default();
            if levels.len() == 3 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let order: Vec<u64> = levels.iter().map(|l| l["level"].as_u64().unwrap()).collect();
    assert_eq!(order, vec![2, 1, 0], "{levels:?}");
    assert!(levels.iter().all(|l| l["leader"] == levels[0]["leader"] && l["ready"] == 0), "{levels:?}");
    let too_many = json!({"name": "many", "priority_levels": 11, "single_active": false, "create_dlq": false});
    let (status, err) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(too_many)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    // A single active consumer (ADR 0076) goes through and shows.
    let single = json!({"name": "ledger", "priority_levels": 1, "single_active": true, "create_dlq": false});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(single)).await;
    assert_eq!(status, StatusCode::CREATED, "{done}");
    let mut shown = false;
    for _ in 0..50 {
        let (status, q) = get(&app, "/api/v1/clusters/local/queues/ledger").await;
        if status == StatusCode::OK && q["single_active"] == true {
            shown = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(shown, "the queue's single active consumer shows");
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

    // Without authentication there is the default tenant, and no quotas.
    let (status, t) = get(&app, "/api/v1/clusters/local/tenants").await;
    assert_eq!(status, StatusCode::OK, "{t}");
    assert_eq!(t["tenants"][0]["name"], "default", "{t}");
    assert!(t["tenants"][0]["quota_bytes"].is_null() && t["tenants"][0]["queues"] == 4, "{t}");

    // What a real cluster does not have.
    let (_, n) = get(&app, "/api/v1/clusters/local/nodes").await;
    assert!(n["upgrade"].is_null() && n["events"].as_array().unwrap().is_empty());

    // A queue with a dead-letter queue (ADR 0055), made from the console.
    let work = json!({"name": "work", "priority_levels": 1, "single_active": false,
        "dlq": "work.dlq", "max_deliveries": 1, "create_dlq": true});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/queues", Some(work)).await;
    assert_eq!(status, StatusCode::CREATED, "{done}");
    let mut q = Value::Null;
    for _ in 0..50 {
        let (status, got) = get(&app, "/api/v1/clusters/local/queues/work").await;
        if status == StatusCode::OK {
            q = got;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!((q["dlq"].as_str(), q["max_deliveries"].as_u64()), (Some("work.dlq"), Some(1)), "{q}");

    // Purge (ADR 0064): the name typed again, then the five ready jobs go.
    let (status, err) =
        call(&app, "POST", "/api/v1/clusters/local/queues/jobs/purge", Some(json!({"confirm": "job"}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    let (status, done) =
        call(&app, "POST", "/api/v1/clusters/local/queues/jobs/purge", Some(json!({"confirm": "jobs"}))).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(done["message"], "Purged 5 messages from jobs.", "{done}");
    assert_eq!(done["cli"], "lotse-cli queue purge jobs");
    // The console samples the cluster about once a second.
    let mut purged = false;
    for _ in 0..50 {
        let (_, q) = get(&app, "/api/v1/clusters/local/queues/jobs").await;
        if q["ready"] == 0 {
            purged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(purged, "nothing ready in jobs");

    // Requeue: a message that failed once goes to the dead-letter queue, and
    // from there back. Its leader is pinned where it is first: a new group's
    // leader may still move in its first seconds, and a requeue that a move
    // overtakes leaves the message waiting for the closed consumer for the
    // resume window (ADR 0109), longer than the wait below.
    let mut work_leader = (0, 0);
    for _ in 0..100 {
        work_leader = leader_term(&client, "work").await;
        if work_leader.0 != 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    client.move_leader("work", 0, Some(u64::from(work_leader.0))).await.expect("pin work's leader");
    for _ in 0..50 {
        if client.produce("work", 0, batch("task")).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let before = leader_term(&client, "work").await;
    {
        let mut c = client.subscribe("work", 1).await.expect("subscribe");
        let m = c.next(Duration::from_secs(5)).await.expect("next").expect("the task");
        c.nack(&m, true).await.expect("nack");
        c.close().await;
    }
    // 10 s; if work's leader moved meanwhile (an election a starved node
    // lost, despite the pin), the resume window and 10 s more.
    let mut dead = false;
    let mut tries = 100;
    let mut i = 0;
    while i < tries {
        let (_, d) = get(&app, "/api/v1/clusters/local/queues/work.dlq").await;
        if d["ready"] == 1 {
            dead = true;
            break;
        }
        i += 1;
        if i == tries && tries == 100 && leader_term(&client, "work").await != before {
            tries += (QueueConfig::DEFAULT_RESUME_WINDOW.as_millis() / 100) as usize + 100;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after = leader_term(&client, "work").await;
    assert!(dead, "the task reaches the dead-letter queue (work's leader and term {before:?} before, {after:?} now)");
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/queues/work.dlq/requeue", None).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(done["message"], "Requeued 1 message from work.dlq to their queues.", "{done}");
    let mut emptied = false;
    for _ in 0..50 {
        let (_, d) = get(&app, "/api/v1/clusters/local/queues/work.dlq").await;
        if d["ready"] == 0 {
            emptied = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(emptied, "nothing left in the dead-letter queue");

    // Moving a leader (ADR 0065): to another replica, pinned, then unpinned.
    let (_, q) = get(&app, "/api/v1/clusters/local/queues/work").await;
    let leader = q["levels"][0]["leader"].as_str().unwrap().to_string();
    let to = ["node-1", "node-2", "node-3"].into_iter().find(|n| *n != leader).unwrap();
    let bad = json!({"resource": "work", "partition": 0, "node": "node-9"});
    let (status, err) = call(&app, "POST", "/api/v1/clusters/local/leaders", Some(bad)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    let pin = json!({"resource": "work", "partition": 0, "node": to});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/leaders", Some(pin)).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    let mut moved = false;
    for _ in 0..300 {
        let (_, q) = get(&app, "/api/v1/clusters/local/queues/work").await;
        if q["levels"][0]["leader"] == to {
            moved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(moved, "the leader moves to {to}");
    // The queue says where it is pinned (ADR 0121).
    let (_, q) = get(&app, "/api/v1/clusters/local/queues/work").await;
    assert_eq!(q["pinned"], to, "{q}");
    let unpin = json!({"resource": "work", "partition": 0, "node": null});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/leaders", Some(unpin)).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(done["cli"], "lotse-cli leader unpin work --partition 0");

    // Placing the queue on named nodes, and the replication factor that
    // releases it (ADR 0115, ADR 0121).
    let (_, q) = get(&app, "/api/v1/clusters/local/queues/work").await;
    let replicas: Vec<Value> = q["replicas"].as_array().unwrap().clone();
    assert!(!replicas.is_empty(), "{q}");
    let twice = json!({"resource": "work", "partition": 0, "nodes": ["node-1", "node-1"]});
    let (status, err) = call(&app, "POST", "/api/v1/clusters/local/placements", Some(twice)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    let unknown = json!({"resource": "work", "partition": 0, "nodes": ["nine"]});
    let (status, err) = call(&app, "POST", "/api/v1/clusters/local/placements", Some(unknown)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");
    let place = json!({"resource": "work", "partition": 0, "nodes": replicas});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/placements", Some(place)).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert!(
        done["cli"].as_str().unwrap().starts_with("lotse-cli partition place work --partition 0 --nodes "),
        "{done}"
    );
    let placed = |want: bool| {
        let app = app.clone();
        async move {
            for _ in 0..100 {
                let (_, q) = get(&app, "/api/v1/clusters/local/queues/work").await;
                if q["placed"] == want {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            false
        }
    };
    assert!(placed(true).await, "the queue shows as placed");
    let factor = json!({"resource": "work", "factor": replicas.len()});
    let (status, done) = call(&app, "POST", "/api/v1/clusters/local/replication", Some(factor)).await;
    assert_eq!(status, StatusCode::OK, "{done}");
    assert_eq!(done["cli"], format!("lotse-cli replication set work --factor {}", replicas.len()));
    assert!(placed(false).await, "the factor releases the placement");
    let (status, err) =
        call(&app, "POST", "/api/v1/clusters/local/replication", Some(json!({"resource": "work", "factor": 0}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{err}");

    // Open sessions (ADR 0121): the console's own connections, and, on a
    // cluster without authentication, everyone's.
    let (status, v) = get(&app, "/api/v1/clusters/local/sessions").await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["everyone"], true, "{v}");
    let sessions = v["sessions"].as_array().unwrap();
    assert!(!sessions.is_empty(), "{v}");
    assert!(sessions.iter().all(|s| s["kind"] == "anonymous" && s["mechanism"] == "none"), "{v}");
    assert!(sessions.iter().all(|s| s["node"].as_str().unwrap().starts_with("node-")), "{v}");

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

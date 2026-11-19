//! A node low on disk (ADR 0122): three nodes, a stream and a queue whose
//! groups have a voter on each, and one follower whose volume is driven
//! below its free space limit.
//!
//! Checked: the follower says it is low; the leaders on the other nodes,
//! which only learn of it through the metadata group, refuse new data with
//! STORAGE_FULL within a few seconds, since the node reports a change at its
//! next read of its volume (ADR 0124); reads, acks and metadata changes go
//! on; the node stays low until its volume is a tenth above the limit, and
//! then produces pass again just as soon; and a node may report only for
//! itself. A scenario: its value does not grow with seeds
//! (`check_few_seeds`).

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_proto::ErrorCode;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Rng};
use lotse_server::{Node, NodeConfig, QueueConfig, StateMachine, StreamConfig};
use lotse_sim::{SIM_DISK_TOTAL, Sim, SimRuntime, check_few_seeds};

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

const TICK: Duration = Duration::from_millis(10);
const STREAM_GROUP: u64 = 3;
/// The default limit on the simulated volume: 5 % of 4 TiB.
const LIMIT: u64 = SIM_DISK_TOTAL / 20;

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn config(id: NodeId) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|i| (i, format!("n{i}:7000"))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        // 1 metadata, 2 coordinator, 3 the stream, 4 the queue.
        groups: vec![1, 2, 3, 4],
        tick: TICK,
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: (1..=3).map(|i| (i, format!("n{i}:9000"))).collect(),
        zones: Default::default(),
        streams: vec![StreamConfig { name: "s".into(), resource_id: 7, partitions: vec![STREAM_GROUP] }],
        queues: vec![QueueConfig::new("q", 8, 4)],
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
        leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
        follower_max_lag: lotse_server::DEFAULT_FOLLOWER_MAX_LAG,
        disk_free_limit: lotse_server::FreeLimit::default(),
        disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth: lotse_server::AuthConfig::none(),
    }
}

/// Whether a produce to `name` is taken now; panics on anything but a
/// refusal for disk space.
async fn takes(client: &Client<SimRuntime>, name: &str, value: &str) -> bool {
    match client.produce(name, 0, batch(value)).await {
        Ok(_) => true,
        Err(ClientError::Broker { code: ErrorCode::STORAGE_FULL, .. }) => false,
        Err(e) => panic!("{name}: {e}"),
    }
}

/// Waits until a produce to `name` is taken or refused, as `taken` says.
async fn until(rt: &SimRuntime, client: &Client<SimRuntime>, name: &str, taken: bool, within: Duration) {
    let deadline = rt.now() + within;
    let mut i = 0;
    while takes(client, name, &format!("w{i}")).await != taken {
        assert!(rt.now() < deadline, "{name} never {}", if taken { "took data again" } else { "refused" });
        i += 1;
        rt.sleep(Duration::from_secs(1)).await;
    }
}

async fn run(rt: SimRuntime) {
    // Identities on: a node's reports are checked against who sent them.
    rt.net().verify_peers(true);
    let mut runtimes = Vec::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let node_rt = rt.node(&format!("n{id}")).with_identity(Some(lotse_rt::PeerIdentity::Node(id)));
        nodes.push(Node::start(node_rt.clone(), config(id), |_| Nothing).await.unwrap());
        runtimes.push(node_rt);
    }
    let client = Client::new(
        rt.node("c1"),
        ClientConfig {
            // Short, so that a refusal comes back as one.
            retry_for: Duration::from_secs(2),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );
    for i in 0..3 {
        assert!(takes(&client, "s", &format!("a{i}")).await, "a healthy cluster takes produces");
        assert!(takes(&client, "q", &format!("j{i}")).await);
    }

    // A follower of the stream goes low: the leader hears of it only
    // through the metadata group.
    let leader = loop {
        let led: Vec<u32> = client
            .node_metrics()
            .await
            .expect("metadata")
            .into_iter()
            .filter_map(|(n, m)| {
                m.ok().filter(|m| m.partitions.iter().any(|p| p.group == STREAM_GROUP && p.leader)).map(|_| n)
            })
            .collect();
        if let [l] = led[..] {
            break l;
        }
        rt.sleep(TICK * 10).await;
    };
    let followers: Vec<u32> = (1..=3).filter(|n| *n != leader).collect();
    let low = followers[rt.below(2) as usize];
    let disk = runtimes[low as usize - 1].disk();
    disk.set_free_bytes(LIMIT - 1);
    // A read a second, a report, a commit: a few seconds, where the
    // metadata leader's poll of every node's metrics took up to 15.
    until(&rt, &client, "s", false, Duration::from_secs(5)).await;
    assert!(!takes(&client, "q", "refused").await, "the queue has a voter there too");
    let report = client.node_metrics().await.expect("metadata");
    for (n, m) in report {
        let d = m.expect("every node answers").disk.expect("every node read its volume");
        assert_eq!((d.free_limit_bytes, d.low), (LIMIT, n == low), "node {n}");
    }

    // Reads, acks and metadata changes go on.
    let read = client.fetch("s", 0, 0, TICK * 10).await.expect("reads");
    assert!(read.records.len() >= 3);
    let mut consumer = client.subscribe("q", 10).await.expect("subscribes");
    let mut acked = 0;
    let deadline = rt.now() + Duration::from_secs(30);
    while acked < 3 {
        assert!(rt.now() < deadline, "the queue's messages never came");
        if let Some(m) = consumer.next(TICK * 50).await.expect("delivers") {
            consumer.ack(&m).await.expect("an ack is no new data");
            acked += 1;
        }
    }
    client.create_stream("later", 1, 3).await.expect("a metadata change is no new data");

    // Above the limit, but not a tenth above it: still low.
    disk.set_free_bytes(LIMIT + LIMIT / 20);
    rt.sleep(Duration::from_secs(30)).await;
    assert!(!takes(&client, "s", "still").await, "back only a tenth above the limit");
    // Room again while the node is cut off from the others: its report is
    // lost, and it reports again until the metadata state says so.
    for n in (1..=3).filter(|n| *n != low) {
        rt.net().block(&format!("n{low}"), &format!("n{n}"));
    }
    disk.set_free_bytes(LIMIT + LIMIT / 10);
    rt.sleep(Duration::from_secs(3)).await;
    rt.net().heal();
    until(&rt, &client, "s", true, Duration::from_secs(15)).await;
    assert!(takes(&client, "q", "again").await);

    // A node speaks only for itself.
    let other = followers.iter().copied().find(|n| *n != low).unwrap_or(leader);
    let liar = Client::new(
        rt.node("n-liar").with_identity(Some(lotse_rt::PeerIdentity::Node(other.into()))),
        ClientConfig {
            retry_for: Duration::from_secs(2),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );
    let got = liar.report_disk(low, true).await;
    assert!(matches!(&got, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. })), "{got:?}");
    assert!(takes(&client, "s", "still taken").await, "the lie changed nothing");
    drop(liar);

    consumer.close().await;
    drop(client);
    for node in nodes {
        node.handle().stop();
        node.join().await.unwrap();
    }
}

#[test]
fn a_node_low_on_disk_stops_new_data_in_its_groups_until_it_has_room() {
    check_few_seeds(6, |seed| Sim::new(seed).run(run));
}

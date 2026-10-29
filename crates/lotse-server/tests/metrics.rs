//! `DescribeMetrics` against a healthy cluster, where every number is
//! known: three nodes, a stream, a queue and a consumer group, used by one
//! client, then every node asked for its metrics.
//!
//! Checked: produces counted once, on the leader, with their bytes,
//! records and latencies; fetches counted; the leader's view of the
//! partition (high watermark, log end, every replica's progress); the
//! queue's messages all accounted for (ready, in flight, acknowledged);
//! the consumer group's members and committed offsets; traffic,
//! connections and the host.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, GroupConfig};
use lotse_log::LogConfig;
use lotse_proto::DescribeMetricsResponse;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Clock;
use lotse_server::{Node, NodeConfig, QueueConfig, StateMachine, StreamConfig};
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

const TICK: Duration = Duration::from_millis(10);
const STREAM: u64 = 7;
const QUEUE: u64 = 8;
const STREAM_GROUP: u64 = 3;

fn batch(value: &str, records: usize) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    for _ in 0..records {
        b.push(0, None, Some(value.as_bytes()), &[]);
    }
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
        streams: vec![StreamConfig { name: "s".into(), resource_id: STREAM, partitions: vec![STREAM_GROUP] }],
        queues: vec![QueueConfig {
            name: "q".into(),
            resource_id: QUEUE,
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
    }
}

async fn run(rt: SimRuntime) {
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let node = Node::start(rt.node(&format!("n{id}")), config(id), |_| Nothing).await.unwrap();
        nodes.push(node);
    }
    let client = Client::new(
        rt.node("c1"),
        ClientConfig {
            retry_for: Duration::from_secs(10),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );

    // 12 batches of 1 to 3 records to the stream, 10 messages to the queue.
    let (mut bytes, mut records) = (0u64, 0u64);
    for i in 0..12 {
        let n = 1 + i % 3;
        let b = batch(&format!("v{i}"), n);
        bytes += b.len() as u64;
        records += n as u64;
        client.produce("s", 0, b).await.expect("a healthy cluster takes produces");
    }
    let mut smallest_job = u64::MAX;
    for i in 0..10 {
        let b = batch(&format!("job{i}"), 1);
        smallest_job = smallest_job.min(b.len() as u64);
        client.produce("q", 0, b).await.expect("and queue messages");
    }
    let fetched = client.fetch("s", 0, 0, TICK * 10).await.expect("reads").records.len() as u64;

    // A consumer takes some messages and acknowledges some of them.
    let mut consumer = client.subscribe("q", 4).await.expect("subscribes");
    let mut taken = Vec::new();
    while taken.len() < 4 {
        if let Some(m) = consumer.next(TICK * 50).await.expect("delivers") {
            taken.push(m);
        }
    }
    for m in &taken[..2] {
        consumer.ack(m).await.expect("acks");
    }

    // A consumer group commits a position.
    let mut member = client.join_group("g", &["s"], GroupConfig::default()).await.expect("joins");
    let deadline = rt.now() + Duration::from_secs(10);
    while member.poll(TICK * 10).await.expect("polls").is_empty() {
        assert!(rt.now() < deadline, "the member never got the partition or read nothing");
    }
    member.commit().await.expect("commits");
    let committed = member.positions()[&("s".to_string(), 0)];

    rt.sleep(TICK * 20).await;
    let answers = client.node_metrics().await.expect("metadata");
    assert_eq!(answers.len(), 3);
    let all: Vec<DescribeMetricsResponse> =
        answers.into_iter().map(|(n, r)| r.unwrap_or_else(|e| panic!("node {n} did not answer: {e}"))).collect();

    // Produces: counted once, on the leader, with their latencies.
    let sum = |f: &dyn Fn(&lotse_proto::PartitionMetrics) -> u64, resource| -> u64 {
        all.iter().flat_map(|m| &m.partitions).filter(|p| p.resource_id == resource).map(f).sum()
    };
    assert_eq!(sum(&|p| p.produce_bytes, STREAM), bytes, "produce bytes");
    assert_eq!(sum(&|p| p.produce_records, STREAM), records, "produce records");
    assert_eq!(sum(&|p| p.produce_records, QUEUE), 10, "queue messages");
    assert!(sum(&|p| p.fetch_bytes, STREAM) >= fetched, "fetch bytes");
    // Each message taken came in a delivery of its batch.
    assert!(sum(&|p| p.fetch_bytes, QUEUE) >= taken.len() as u64 * smallest_job, "delivered bytes");
    let latencies: u64 = all.iter().flat_map(|m| &m.commit_latency).sum();
    assert_eq!(latencies, 12 + 10, "every acknowledged produce has a latency");

    // The leader's view of the stream partition.
    let leaders: Vec<&lotse_proto::PartitionMetrics> =
        all.iter().flat_map(|m| &m.partitions).filter(|p| p.group == STREAM_GROUP && p.leader).collect();
    let [lead] = &leaders[..] else { panic!("one leader expected: {leaders:?}") };
    assert_eq!((lead.resource_id, lead.partition), (STREAM, 0));
    assert!(lead.high_watermark >= records && lead.high_watermark <= lead.log_end, "{lead:?}");
    assert!(lead.last_stable <= lead.high_watermark && lead.size_bytes >= bytes, "{lead:?}");
    assert_eq!(lead.replicas.len(), 3, "every replica's progress: {lead:?}");
    assert!(lead.replicas.iter().all(|r| r.log_end <= lead.log_end + 1), "{lead:?}");
    let followers = all.iter().flat_map(|m| &m.partitions).filter(|p| p.group == STREAM_GROUP && !p.leader);
    assert!(followers.clone().count() == 2 && followers.clone().all(|p| p.replicas.is_empty()));

    // The queue: every message ready, in flight or acknowledged.
    let queues: Vec<&lotse_proto::QueueMetrics> = all.iter().flat_map(|m| &m.queues).collect();
    let [q] = &queues[..] else { panic!("one leader serves the queue: {queues:?}") };
    assert_eq!(q.resource_id, QUEUE);
    assert_eq!((q.acked, q.ready + q.in_flight + q.acked), (2, 10), "{q:?}");
    assert!(q.delivered >= 4 && q.in_flight >= 2, "{q:?}");
    let [c] = &q.consumers[..] else { panic!("one consumer: {q:?}") };
    assert!(c.in_flight as u64 == q.in_flight && !c.peer.is_empty(), "{c:?}");

    // The consumer group, from the coordinator's leader.
    let groups: Vec<&lotse_proto::GroupMetrics> = all.iter().flat_map(|m| &m.groups).collect();
    let [g] = &groups[..] else { panic!("one coordinator reports the group: {groups:?}") };
    assert_eq!(g.group_id, "g");
    assert_eq!(g.members.len(), 1);
    assert_eq!(g.members[0].owned, vec![lotse_proto::TxnResource { resource_id: STREAM, partitions: vec![0] }]);
    assert!(g.offsets.iter().any(|o| (o.resource_id, o.partition, o.offset) == (STREAM, 0, committed)), "{g:?}");

    // Traffic, connections and the (simulated) host.
    for m in &all {
        assert!(m.uptime_ms > 0 && m.peer_in_bytes > 0 && m.peer_out_bytes > 0, "node {}: {m:?}", m.node_id);
        assert_eq!(m.host.cpu_permille, Some(250));
        assert_eq!((m.host.disk_free_bytes, m.host.disk_total_bytes), (Some(3 << 40), Some(4 << 40)));
        assert_eq!(m.partitions.len(), 4, "node {} hosts every group", m.node_id);
        assert_eq!((m.metadata_group, m.coordinator_group), (1, 2));
    }
    assert!(all.iter().any(|m| m.connections >= 1 && m.client_in_bytes > bytes && m.client_out_bytes > 0));

    member.close().await;
    consumer.close().await;
    drop(client);
    for node in nodes {
        node.handle().stop();
        node.join().await.unwrap();
    }
}

#[test]
fn every_node_reports_what_it_counted() {
    check_seeds(20, |seed| Sim::new(seed).run(run));
}

//! Storage quotas on simulated nodes (ADR 0147): the metadata leader counts
//! usage from every node's group sizes, records full limits, and the groups'
//! leaders refuse new data under them; a node that is down and a new
//! metadata leader change none of that.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError, OutMessage};
use lotse_log::LogConfig;
use lotse_proto::ErrorCode;
use lotse_raft::{NodeId, Role};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Clock;
use lotse_server::{Node, NodeConfig, NodeHandle, RetentionConfig, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_few_seeds};

const TICK: Duration = Duration::from_millis(10);
const METADATA: u64 = 1;
const COORDINATOR: u64 = 2;

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

struct World {
    hosts: BTreeMap<NodeId, SimRuntime>,
    cfgs: BTreeMap<NodeId, NodeConfig>,
    running: BTreeMap<NodeId, Option<(Node<SimRuntime>, NodeHandle)>>,
}

impl World {
    /// Three nodes, every one a voter of the metadata and coordinator
    /// groups, counting quotas every `interval`.
    async fn new(rt: &SimRuntime, interval: Duration) -> World {
        let ids: Vec<NodeId> = vec![1, 2, 3];
        let peers: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:7000"))).collect();
        let client_addrs: BTreeMap<NodeId, String> = ids.iter().map(|id| (*id, format!("n{id}:9000"))).collect();
        let cfgs = ids
            .iter()
            .map(|&id| {
                (
                    id,
                    NodeConfig {
                        id,
                        listen: peers[&id].clone(),
                        peers: peers.clone(),
                        groups: vec![METADATA, COORDINATOR],
                        tick: TICK,
                        election_timeout: TICK * 10,
                        heartbeat_interval: TICK * 2,
                        pre_vote: true,
                        log: LogConfig { segment_bytes: 2_048, index_interval_bytes: 256 },
                        client_listen: Some(client_addrs[&id].clone()),
                        client_addrs: client_addrs.clone(),
                        zones: Default::default(),
                        streams: Vec::new(),
                        queues: Vec::new(),
                        exchanges: Vec::new(),
                        group_session_timeout: Duration::from_secs(5),
                        group_heartbeat_interval: TICK * 5,
                        metadata: Some(METADATA),
                        coordinator: Some(COORDINATOR),
                        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
                        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
                        leader_settle: lotse_server::DEFAULT_LEADER_SETTLE,
                        follower_max_lag: lotse_server::DEFAULT_FOLLOWER_MAX_LAG,
                        disk_free_limit: lotse_server::FreeLimit::default(),
                        disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
                        quota_check_interval: interval,
                        versions: Default::default(),
                        retention: RetentionConfig::keep(),
                        join: false,
                        auth: lotse_server::AuthConfig::none(),
                    },
                )
            })
            .collect();
        let mut w = World {
            hosts: ids.iter().map(|&id| (id, rt.node(&format!("n{id}")))).collect(),
            cfgs,
            running: BTreeMap::new(),
        };
        for id in ids {
            w.start(id).await;
        }
        w
    }

    async fn start(&mut self, id: NodeId) {
        let node = Node::start(self.hosts[&id].clone(), self.cfgs[&id].clone(), |_| Nothing)
            .await
            .unwrap_or_else(|e| panic!("node {id} cannot start: {e}"));
        let handle = node.handle();
        self.running.insert(id, Some((node, handle)));
    }

    async fn crash(&mut self, id: NodeId) {
        let Some(Some((node, handle))) = self.running.insert(id, None) else { return };
        handle.stop();
        let _ = node.join().await;
        self.hosts[&id].crash();
    }

    /// The node that leads the metadata group, if one does.
    fn metadata_leader(&self) -> Option<NodeId> {
        self.running.iter().find_map(|(id, n)| {
            let (_, h) = n.as_ref()?;
            (h.status(METADATA)?.role == Role::Leader).then_some(*id)
        })
    }

    async fn stop(mut self) {
        for (node, handle) in std::mem::take(&mut self.running).into_values().flatten() {
            handle.stop();
            let _ = node.join().await;
        }
    }
}

fn client(rt: &SimRuntime, name: &str) -> Client<SimRuntime> {
    Client::new(
        rt.node(name),
        ClientConfig {
            retry_for: Duration::from_secs(20),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    )
}

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

/// Waits until the cluster takes cluster version 0.2, which quotas need.
async fn quotas_allowed(rt: &SimRuntime, admin: &Client<SimRuntime>) {
    let deadline = rt.now() + Duration::from_secs(60);
    loop {
        let v = admin.describe_upgrade().await.ok().and_then(|d| d.cluster_version);
        if v.is_some_and(|v| v >= lotse_server::version::QUOTAS_SINCE) {
            return;
        }
        assert!(rt.now() < deadline, "the cluster never took cluster version 0.2: {v:?}");
        rt.sleep(TICK * 20).await;
    }
}

fn exceeded(e: &ClientError) -> bool {
    matches!(e, ClientError::Broker { code: ErrorCode::QUOTA_EXCEEDED, .. })
}

/// Produces to `name` until a write is refused with QUOTA_EXCEEDED, within
/// `within`; the number of writes taken.
async fn fill(rt: &SimRuntime, c: &Client<SimRuntime>, name: &str, within: Duration) -> usize {
    let deadline = rt.now() + within;
    let mut taken = 0;
    loop {
        match c.produce(name, 0, batch(&format!("{name}-{taken}-{}", "x".repeat(100)))).await {
            Ok(_) => taken += 1,
            Err(e) if exceeded(&e) => return taken,
            Err(e) => panic!("{name}: {e}"),
        }
        assert!(rt.now() < deadline, "{name}: {taken} writes and none refused");
        rt.sleep(TICK * 5).await;
    }
}

/// A limit trips once usage reaches it, and clears once it is raised; the
/// count goes on while a node is down, since another replica of each group
/// gives its size.
async fn trip_and_clear(rt: SimRuntime) {
    let mut w = World::new(&rt, Duration::from_millis(500)).await;
    let admin = client(&rt, "admin");
    quotas_allowed(&rt, &admin).await;
    admin.create_stream("s", 1, 3).await.expect("a stream");
    admin.alter_quota("default", "", Some(8 * 1024)).await.expect("a limit");
    let taken = fill(&rt, &admin, "s", Duration::from_secs(30)).await;
    assert!(taken > 0, "nothing was taken before the limit tripped");
    let d = admin.describe_quotas(Some("default")).await.expect("quotas");
    let own = d.quotas.iter().find(|q| q.prefix.is_empty()).expect("the tenant's limit");
    assert!(own.full && own.used_bytes.is_some_and(|u| u >= 8 * 1024), "{own:?}");

    // a_full_limit_is_recorded_and_cleared: raised, writes are taken again
    // within a few counts.
    admin.alter_quota("default", "", Some(1 << 30)).await.expect("raised");
    let deadline = rt.now() + Duration::from_secs(10);
    loop {
        match admin.produce("s", 0, batch("after")).await {
            Ok(_) => break,
            Err(e) if exceeded(&e) => assert!(rt.now() < deadline, "the raised limit never cleared"),
            Err(e) => panic!("{e}"),
        }
        rt.sleep(Duration::from_millis(200)).await;
    }

    // a_down_node_does_not_stop_counting: with a node down, a lower limit
    // still trips.
    let down = if w.metadata_leader() == Some(3) { 2 } else { 3 };
    w.crash(down).await;
    admin.alter_quota("default", "", Some(16 * 1024)).await.expect("lowered");
    fill(&rt, &admin, "s", Duration::from_secs(30)).await;
    w.start(down).await;
    w.stop().await;
}

#[test]
fn a_full_limit_is_recorded_cleared_and_counted_with_a_node_down() {
    check_few_seeds(3, |seed| Sim::new(seed).run(trip_and_clear));
}

/// a_new_metadata_leader_keeps_a_full_limit: the full record is in the
/// metadata state, so writes stay refused at once after the metadata
/// leader changes, before the new leader has counted anything.
async fn new_leader(rt: SimRuntime) {
    // Counts far apart: what refuses right after the change is the record.
    let mut w = World::new(&rt, Duration::from_secs(30)).await;
    let admin = client(&rt, "admin");
    quotas_allowed(&rt, &admin).await;
    admin.create_stream("s", 1, 3).await.expect("a stream");
    admin.alter_quota("default", "", Some(1)).await.expect("a limit below any usage");
    fill(&rt, &admin, "s", Duration::from_secs(90)).await;
    let leader = w.metadata_leader().expect("a metadata leader");
    w.crash(leader).await;
    let deadline = rt.now() + Duration::from_secs(5);
    while w.metadata_leader().is_none() {
        assert!(rt.now() < deadline, "no new metadata leader");
        rt.sleep(TICK * 10).await;
    }
    let e = admin.produce("s", 0, batch("right after")).await.expect_err("still full");
    assert!(exceeded(&e), "{e}");
    w.start(leader).await;
    w.stop().await;
}

#[test]
fn a_new_metadata_leader_keeps_a_full_limit() {
    check_few_seeds(3, |seed| Sim::new(seed).run(new_leader));
}

/// Review focus 3 and 4: a transaction in a full tenant still aborts, so
/// it holds nothing back; a publish to two queues writes the one with room
/// and reports the one under a full prefix limit.
async fn transactions_and_publishes(rt: SimRuntime) {
    let w = World::new(&rt, Duration::from_millis(500)).await;
    let admin = client(&rt, "admin");
    quotas_allowed(&rt, &admin).await;
    for q in ["orders.q", "billing.q"] {
        admin.create_queue(q, 3, None).await.expect("a queue");
    }
    admin.declare_exchange("all", lotse_route::ExchangeKind::Fanout).await.expect("an exchange");
    for q in ["orders.q", "billing.q"] {
        admin.bind("all", q, "").await.expect("bound");
    }
    admin.alter_quota("default", "orders.", Some(4 * 1024)).await.expect("a prefix limit");
    fill(&rt, &admin, "orders.q", Duration::from_secs(30)).await;

    // The publish: billing.q takes it, orders.q refuses.
    let e = admin.publish("all", "", OutMessage::value("both"), false).await.expect_err("orders.q is full");
    assert!(e.to_string().contains("billing.q") || e.to_string().contains("QUOTA_EXCEEDED"), "{e}");
    let mut billing = admin.subscribe("billing.q", 10).await.expect("a subscription");
    let deadline = rt.now() + Duration::from_secs(10);
    loop {
        if let Some(m) = billing.next(TICK * 20).await.expect("deliveries")
            && m.record.value.as_deref() == Some(&b"both"[..])
        {
            break;
        }
        assert!(rt.now() < deadline, "billing.q never got the published message");
    }
    drop(billing);

    // The transaction: refused under the full prefix, aborted cleanly.
    let tx = Client::new(
        rt.node("tx"),
        ClientConfig {
            transactional_id: Some("tx".into()),
            retry_for: Duration::from_secs(20),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );
    let mut txn = tx.begin_transaction().await.expect("a transaction");
    txn.produce("billing.q", 0, batch("in the transaction")).await.expect("billing.q has room");
    let e = txn.produce("orders.q", 0, batch("refused")).await.expect_err("orders.q is full");
    assert!(exceeded(&e), "{e}");
    txn.abort().await.expect("the transaction aborts");
    // Nothing holds the queues back: a new transaction begins and commits.
    let mut next = tx.begin_transaction().await.expect("a new transaction");
    next.produce("billing.q", 0, batch("next")).await.expect("written");
    next.commit().await.expect("committed");
    w.stop().await;
}

#[test]
fn a_full_prefix_lets_transactions_abort_and_publishes_write_the_rest() {
    check_few_seeds(3, |seed| Sim::new(seed).run(transactions_and_publishes));
}

/// The quota simulation (ADR 0147): while nodes crash, some mid-write,
/// tenant `default` writes under a limit and tenant `b` without one. The
/// oracle, from what the clients saw:
/// 1. `b` is never refused with QUOTA_EXCEEDED;
/// 2. once the cluster is whole again, writes to the limited tenant are
///    refused within a few counts and stay refused;
/// 3. reads in the full tenant go on;
/// 4. after the limit is raised, its writes are taken again;
/// 5. every acknowledged write is in the log and no refused one is.
async fn under_faults(rt: SimRuntime) {
    use lotse_rt::Rng;
    let interval = Duration::from_millis(200 + rt.below(600));
    let mut w = World::new(&rt, interval).await;
    let admin = client(&rt, "admin");
    quotas_allowed(&rt, &admin).await;
    admin.create_tenant("b").await.expect("tenant b");
    admin.create_stream("a.s", 1, 3).await.expect("a.s");
    let in_b = Client::new(
        rt.node("b-admin"),
        ClientConfig {
            tenant: "b".into(),
            retry_for: Duration::from_secs(20),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );
    in_b.create_stream("b.s", 1, 3).await.expect("b.s");
    let limit = 6 * 1024 + rt.below(6 * 1024);
    admin.alter_quota("default", "", Some(limit)).await.expect("a limit");

    let (mut acked, mut refused) = (BTreeMap::new(), Vec::new());
    let mut b_written = 0;
    let faults_end = rt.now() + Duration::from_secs(20);
    let mut i = 0u64;
    let mut next_fault = rt.now() + Duration::from_millis(500 + rt.below(2_000));
    let mut down: Option<(NodeId, lotse_rt::Instant)> = None;
    while rt.now() < faults_end {
        if down.is_none() && rt.now() >= next_fault {
            let id = 1 + rt.below(3);
            if rt.chance(1, 2) {
                w.hosts[&id].disk().arm_power_loss(rt.below(6));
                rt.sleep(TICK * 3).await;
            }
            w.crash(id).await;
            down = Some((id, rt.now() + Duration::from_millis(200 + rt.below(3_000))));
        }
        if let Some((id, until)) = down
            && rt.now() >= until
        {
            w.start(id).await;
            down = None;
            next_fault = rt.now() + Duration::from_millis(500 + rt.below(3_000));
        }
        let value = format!("a-{i}-{}", "y".repeat(60));
        i += 1;
        match admin.produce("a.s", 0, batch(&value)).await {
            Ok(offset) => {
                acked.insert(offset, value);
            }
            Err(e) if exceeded(&e) => refused.push(value),
            Err(ClientError::Unknown(_)) => {}
            Err(e) => panic!("a.s under faults: {e}"),
        }
        match in_b.produce("b.s", 0, batch(&format!("b-{i}"))).await {
            Ok(_) => b_written += 1,
            Err(e) => assert!(!exceeded(&e), "tenant b has no limit and was refused: {e}"),
        }
        rt.sleep(TICK * 2).await;
    }
    if let Some((id, _)) = down {
        w.start(id).await;
    }

    // 2. Whole again: the limited tenant is refused within a few counts,
    // and stays so.
    let deadline = rt.now() + interval * 6 + Duration::from_secs(5);
    loop {
        let value = format!("a-{i}-late");
        i += 1;
        match admin.produce("a.s", 0, batch(&value)).await {
            Ok(offset) => {
                acked.insert(offset, value);
                assert!(rt.now() < deadline, "the healed cluster never refused tenant default: limit {limit}");
            }
            Err(e) if exceeded(&e) => {
                refused.push(value);
                break;
            }
            Err(ClientError::Unknown(_)) => {}
            Err(e) => panic!("{e}"),
        }
        rt.sleep(interval / 4).await;
    }
    for _ in 0..5 {
        let value = format!("a-{i}-still");
        i += 1;
        let e = admin.produce("a.s", 0, batch(&value)).await.expect_err("a full limit stays full");
        assert!(exceeded(&e), "{e}");
        refused.push(value);
        rt.sleep(interval / 2).await;
    }
    assert!(b_written > 0, "tenant b wrote nothing: the run checked little");

    // 3 and 5: reads go on; what was acknowledged is there, nothing refused is.
    let mut at = 0;
    let mut seen: BTreeMap<u64, String> = BTreeMap::new();
    let end = acked.keys().next_back().map_or(0, |o| o + 1);
    while at < end {
        let f = admin.fetch("a.s", 0, at, TICK * 20).await.expect("reads in a full tenant");
        for b in f.committed_batches().expect("whole valid batches") {
            if let Some(v) = b.records().unwrap()[0].value.as_deref() {
                seen.insert(b.base_offset(), String::from_utf8_lossy(v).into_owned());
            }
        }
        at = f.next_offset(at);
    }
    for (offset, value) in &acked {
        assert_eq!(seen.get(offset), Some(value), "acknowledged write at {offset} is missing");
    }
    let refused_set: std::collections::BTreeSet<&String> = refused.iter().collect();
    for (offset, v) in &seen {
        assert!(!refused_set.contains(v), "refused write {v} is in the log at {offset}");
    }

    // 4. Raised, the tenant writes again.
    admin.alter_quota("default", "", Some(1 << 30)).await.expect("raised");
    let deadline = rt.now() + interval * 6 + Duration::from_secs(5);
    loop {
        match admin.produce("a.s", 0, batch("after the raise")).await {
            Ok(_) => break,
            Err(e) if exceeded(&e) => assert!(rt.now() < deadline, "the raised limit never cleared"),
            Err(ClientError::Unknown(_)) => {}
            Err(e) => panic!("{e}"),
        }
        rt.sleep(interval / 4).await;
    }
    w.stop().await;
}

#[test]
fn storage_limits_hold_under_faults() {
    lotse_sim::check_seeds(20, |seed| Sim::new(seed).run(under_faults));
}

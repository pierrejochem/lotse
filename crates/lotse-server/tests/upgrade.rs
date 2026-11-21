//! Rolling upgrades (ADR 0127): three nodes start on an older release and
//! restart one by one on a newer one, each only with the cluster's restart
//! turn. Every group (metadata, coordinator and one more) has a voter on
//! each node, so two nodes down at once leave every group without a quorum.
//!
//! The simulation (`upgrades_take_turns_under_faults`) starts an upgrader on
//! every node at once, as an unattended update on every machine would,
//! while partitions come and go. Each takes the turn, stops its node as
//! `lotse run` does on SIGTERM, starts it again on the newer release and
//! waits for the turn to come back. Checked: no node gets the turn while
//! another is still down for its upgrade; every upgrade ends, and with it
//! the turn; the cluster version may go up only once every node runs the
//! newer release, and then does.
//!
//! The scenario (`turns_wait_for_quorums_holders_and_pauses`) checks the
//! rules one at a time: a node down for another reason keeps the others
//! from taking the turn, since their groups would lose their quorum; a node
//! holding the turn keeps it from the others; the process that took a turn
//! never gives it back, the restarted one does once it is ready; a pause
//! lets no node take the next turn; and a node on a release older than
//! the cluster version stops by itself.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_log::LogConfig;
use lotse_proto::{AlterUpgradeRequest, ErrorCode, cluster_version::of};
use lotse_raft::NodeId;
use lotse_record::Batch;
use lotse_rt::{Clock, Rng, Spawn};
use lotse_server::version::Versions;
use lotse_server::{Node, NodeConfig, NodeHandle, StateMachine};
use lotse_sim::{Sim, SimRuntime, check_few_seeds, check_seeds};

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
const NODES: u64 = 3;

fn old() -> Versions {
    Versions { release: "0.4.2".into(), oldest: of(0, 3), newest: of(0, 4) }
}

fn new() -> Versions {
    Versions { release: "0.5.0".into(), oldest: of(0, 4), newest: of(0, 5) }
}

fn config(id: NodeId, versions: Versions) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=NODES).map(|i| (i, format!("n{i}:7000"))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        // 1 metadata, 2 coordinator, 3 a group of no resource; every peer
        // votes in each.
        groups: vec![1, 2, 3],
        tick: TICK,
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: (1..=NODES).map(|i| (i, format!("n{i}:9000"))).collect(),
        zones: Default::default(),
        streams: Vec::new(),
        queues: Vec::new(),
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
        versions,
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth: lotse_server::AuthConfig::none(),
    }
}

fn client_config() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_millis(500),
        retry_for: Duration::from_secs(3),
        backoff: Duration::from_millis(20),
        metadata_max_age: Duration::from_millis(300),
        ..ClientConfig::new((1..=NODES).map(|i| format!("n{i}:9000")).collect())
    }
}

/// The nodes, their hosts, and which are down for their upgrade.
struct Cluster {
    hosts: BTreeMap<NodeId, SimRuntime>,
    running: Mutex<BTreeMap<NodeId, (Node<SimRuntime>, NodeHandle)>>,
    /// Nodes an upgrader stopped and has not started again: the oracle's.
    down_for_upgrade: Mutex<BTreeSet<NodeId>>,
}

impl Cluster {
    async fn new(rt: &SimRuntime) -> Arc<Cluster> {
        let hosts: BTreeMap<NodeId, SimRuntime> = (1..=NODES).map(|i| (i, rt.node(&format!("n{i}")))).collect();
        let c = Arc::new(Cluster { hosts, running: Mutex::new(BTreeMap::new()), down_for_upgrade: Mutex::default() });
        for id in 1..=NODES {
            c.start(id, old()).await;
        }
        c
    }

    async fn start(&self, id: NodeId, versions: Versions) {
        let node = Node::start(self.hosts[&id].clone(), config(id, versions), |_| Nothing)
            .await
            .unwrap_or_else(|e| panic!("node {id} cannot start from its disk: {e}"));
        let handle = node.handle();
        self.running.lock().unwrap().insert(id, (node, handle));
    }

    /// Stops node `id` as `lotse run` does on SIGTERM: it hands its groups
    /// over first (ADR 0114).
    async fn stop(&self, id: NodeId) {
        let (node, handle) = self.running.lock().unwrap().remove(&id).expect("running");
        handle.leave(Duration::from_secs(5));
        node.join().await.unwrap_or_else(|e| panic!("node {id} stops cleanly: {e}"));
    }

    /// Stops node `id` at once, as a crash or a pulled cable would.
    async fn kill(&self, id: NodeId) {
        let (node, handle) = self.running.lock().unwrap().remove(&id).expect("running");
        handle.stop();
        let _ = node.join().await;
    }
}

/// Errors a turn's caller meets in a cluster under faults and asks again
/// after.
fn transient(e: &ClientError) -> bool {
    matches!(
        e,
        ClientError::GaveUp(..)
            | ClientError::NoAnswer(..)
            | ClientError::Broker {
                code: ErrorCode::NOT_LEADER
                    | ErrorCode::REQUEST_TIMED_OUT
                    | ErrorCode::COORDINATOR_NOT_AVAILABLE
                    | ErrorCode::TURN_UNAVAILABLE,
                ..
            }
    )
}

/// Waits until `$check` (an expression giving an `Option`) is `Some`,
/// asking every 100 ms, for at most `$within`; gives what it holds.
macro_rules! until {
    ($rt:expr, $what:expr, $within:expr, $check:expr) => {{
        let (rt, within): (&SimRuntime, Duration) = (&$rt, $within);
        let deadline = rt.now() + within;
        loop {
            if let Some(v) = $check {
                break v;
            }
            assert!(rt.now() < deadline, "{} within {within:?}", $what);
            rt.sleep(Duration::from_millis(100)).await;
        }
    }};
}

/// The node holding the turn and the cluster version, as the metadata
/// leader says.
async fn upgrade(client: &Client<SimRuntime>) -> Option<(Option<u64>, u32, bool)> {
    let d = client.describe_upgrade().await.ok()?;
    Some((d.turn.map(|t| t.node_id), d.cluster_version?, d.held))
}

/// One machine's upgrade: take the turn, stop, start the newer release,
/// wait for the turn to come back.
async fn upgrader(rt: SimRuntime, cluster: Arc<Cluster>, id: NodeId) {
    let client = Client::new(rt.clone(), client_config());
    rt.sleep(TICK * rt.below(200) as u32).await;
    let deadline = rt.now() + Duration::from_secs(600);
    loop {
        match client.take_turn(id as u32).await {
            Ok(()) => break,
            Err(e) if transient(&e) => {}
            Err(e) => panic!("node {id} takes the turn: {e}"),
        }
        assert!(rt.now() < deadline, "node {id} never got the restart turn");
        rt.sleep(Duration::from_millis(200)).await;
    }
    {
        let mut down = cluster.down_for_upgrade.lock().unwrap();
        assert!(down.is_empty(), "node {id} got the restart turn while {down:?} was still down for its upgrade");
        down.insert(id);
    }
    cluster.stop(id).await;
    rt.sleep(TICK * (5 + rt.below(100) as u32)).await;
    cluster.start(id, new()).await;
    cluster.down_for_upgrade.lock().unwrap().remove(&id);
    // The restarted node gives the turn back once it is ready.
    until!(rt, &format!("node {id} gives the turn back after its upgrade"), Duration::from_secs(300), {
        upgrade(&client).await.filter(|(turn, ..)| *turn != Some(id)).map(|_| ())
    });
}

async fn partitions(rt: SimRuntime, stop: Arc<std::sync::atomic::AtomicBool>) {
    let hosts: Vec<String> = (1..=NODES).map(|i| format!("n{i}")).collect();
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        rt.sleep(TICK * 2).await;
        if rt.chance(1, 40) {
            let (a, b) = (&hosts[rt.below(NODES) as usize], &hosts[rt.below(NODES) as usize]);
            if a != b {
                rt.net().block(a, b);
                rt.sleep(TICK * (10 + rt.below(150) as u32)).await;
                rt.net().heal();
            }
        }
    }
    rt.net().heal();
}

async fn run(rt: SimRuntime) {
    let cluster = Cluster::new(&rt).await;
    let client = Client::new(rt.node("c1"), client_config());
    // The cluster takes its version once every node has reported.
    until!(rt, "the cluster takes the version its nodes run with", Duration::from_secs(60), {
        upgrade(&client).await.filter(|(_, v, _)| *v == of(0, 4)).map(|_| ())
    });
    match client.alter_upgrade(AlterUpgradeRequest { cluster_version: Some(of(0, 5)), ..Default::default() }).await {
        Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, message: Some(why) }) => {
            assert!(why.contains("0.4"), "{why}");
        }
        other => panic!("the version went up past what the nodes run with: {other:?}"),
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let faults = rt.spawn(partitions(rt.clone(), stop.clone()));
    let upgraders: Vec<_> =
        (1..=NODES).map(|id| rt.spawn(upgrader(rt.node(&format!("u{id}")), cluster.clone(), id))).collect();
    for u in upgraders {
        u.await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    faults.await;

    let d = until!(rt, "every node reports the newer release", Duration::from_secs(60), {
        client.describe_upgrade().await.ok().filter(|d| d.nodes.iter().all(|n| n.release == "0.5.0"))
    });
    assert_eq!(d.turn, None, "every turn came back");
    assert_eq!(d.cluster_version, Some(of(0, 4)), "nothing raised the version on its own");
    until!(rt, "a cluster admin raises the version", Duration::from_secs(30), {
        let raise = AlterUpgradeRequest { cluster_version: Some(of(0, 5)), ..Default::default() };
        client.alter_upgrade(raise).await.ok()
    });
    assert_eq!(upgrade(&client).await.map(|(_, v, _)| v), Some(of(0, 5)));
    for id in 1..=NODES {
        cluster.kill(id).await;
    }
}

#[test]
fn upgrades_take_turns_under_faults() {
    check_seeds(10, |seed| Sim::new(seed).run(run));
}

async fn take(client: &Client<SimRuntime>, id: NodeId) -> Result<(), String> {
    match client.take_turn(id as u32).await {
        Ok(()) => Ok(()),
        Err(ClientError::Broker { code: ErrorCode::TURN_UNAVAILABLE, message }) => Err(message.unwrap_or_default()),
        Err(e) if transient(&e) => Err(format!("transient: {e}")),
        Err(e) => panic!("node {id} takes the turn: {e}"),
    }
}

async fn scenario(rt: SimRuntime) {
    let cluster = Cluster::new(&rt).await;
    let client = Client::new(rt.node("c1"), client_config());
    until!(rt, "the cluster takes its version", Duration::from_secs(60), {
        upgrade(&client).await.filter(|(_, v, _)| *v == of(0, 4)).map(|_| ())
    });

    // Node 2 goes down, not for an upgrade: every group keeps a quorum only
    // while nodes 1 and 3 run, so neither may take the turn.
    cluster.kill(2).await;
    // The leaders judge from whom they heard lately: a node that failed
    // within the last election timeout still counts (ADR 0117).
    rt.sleep(Duration::from_secs(1)).await;
    let why = until!(rt, "node 1 is refused the turn while node 2 is down", Duration::from_secs(30), {
        match take(&client, 1).await {
            Ok(()) => panic!("node 1 got the turn while node 2 was down: its groups had only nodes 1 and 3"),
            Err(why) if why.contains("quorum") => Some(why),
            Err(_) => None,
        }
    });
    assert!(why.contains("node 1"), "{why}");
    rt.sleep(Duration::from_secs(2)).await;
    assert!(take(&client, 1).await.is_err(), "still refused while node 2 is down");
    cluster.start(2, old()).await;
    until!(rt, "node 1 takes the turn once node 2 is back", Duration::from_secs(60), take(&client, 1).await.ok());

    // Node 1 holds the turn: nobody else gets it, and node 1's own process
    // does not give it back, though every group of its is ready.
    let why = take(&client, 3).await.expect_err("node 3 must wait for node 1");
    assert!(why.contains("node 1 holds"), "{why}");
    rt.sleep(Duration::from_secs(5)).await;
    assert_eq!(upgrade(&client).await.and_then(|(t, ..)| t), Some(1), "the process that took the turn kept it");
    take(&client, 1).await.expect("node 1 asks again and still holds it");

    // A pause: node 1 finishes, nobody takes the next turn.
    client.alter_upgrade(AlterUpgradeRequest { hold: Some(true), ..Default::default() }).await.expect("pause");
    cluster.stop(1).await;
    cluster.start(1, new()).await;
    until!(rt, "the restarted node 1 gives the turn back", Duration::from_secs(60), {
        upgrade(&client).await.filter(|(t, ..)| t.is_none()).map(|_| ())
    });
    let why = take(&client, 3).await.expect_err("turns are paused");
    assert!(why.contains("paused"), "{why}");
    client.alter_upgrade(AlterUpgradeRequest { hold: Some(false), ..Default::default() }).await.expect("resume");

    // The others upgrade in turn; then the version goes up.
    for id in 2..=NODES {
        until!(rt, &format!("node {id} takes the turn"), Duration::from_secs(60), { take(&client, id).await.ok() });
        cluster.stop(id).await;
        cluster.start(id, new()).await;
        until!(rt, &format!("node {id} gives the turn back"), Duration::from_secs(60), {
            upgrade(&client).await.filter(|(t, ..)| t.is_none()).map(|_| ())
        });
    }
    let raise = AlterUpgradeRequest { cluster_version: Some(of(0, 5)), ..Default::default() };
    until!(rt, "the version goes up", Duration::from_secs(30), client.alter_upgrade(raise).await.ok());

    // Node 3 comes back on the older release: it stops by itself.
    cluster.stop(3).await;
    cluster.start(3, old()).await;
    let handle = cluster.running.lock().unwrap()[&3].1.clone();
    let why = until!(rt, "node 3 on the older release stops", Duration::from_secs(30), handle.refused());
    assert!(why.contains("0.5") && why.contains("0.4.2"), "{why}");
    let (node, _) = cluster.running.lock().unwrap().remove(&3).expect("node 3");
    node.join().await.expect("a refusing node stops cleanly");
    for id in [1, 2] {
        cluster.kill(id).await;
    }
}

#[test]
fn turns_wait_for_quorums_holders_and_pauses() {
    check_few_seeds(3, |seed| Sim::new(seed).run(scenario));
}

/// Groups 1 and 2 each have a leader that a quorum answered, as some
/// node's metrics say.
async fn every_group_led(client: &Client<SimRuntime>) -> Option<()> {
    let nodes = client.node_metrics().await.ok()?;
    let led: BTreeSet<u64> = nodes
        .into_iter()
        .filter_map(|(_, m)| m.ok())
        .flat_map(|m| m.partitions.into_iter().filter(|p| p.leader).map(|p| p.group))
        .collect();
    (led.contains(&1) && led.contains(&2)).then_some(())
}

/// A rolling restart where each restarted node's old connections vanish
/// instead of breaking, as when a Kubernetes pod is replaced and comes back
/// at another address: the other nodes' sends to the old process succeed
/// and go nowhere, and only the old process's close says it is gone. A node
/// that went on sending into such a connection would never be heard by the
/// restarted node again, and after the second restart no group would have a
/// quorum (seen on kind, ADR 0128).
async fn restarts_at_new_addresses(rt: SimRuntime) {
    rt.net().set_silent_close(true);
    let cluster = Cluster::new(&rt).await;
    let client = Client::new(rt.node("c1"), client_config());
    until!(rt, "the cluster takes the version its nodes run with", Duration::from_secs(60), {
        upgrade(&client).await.map(|_| ())
    });
    for id in (1..=NODES).rev() {
        cluster.stop(id).await;
        cluster.start(id, old()).await;
        until!(rt, &format!("every group has a leader after node {id} restarted"), Duration::from_secs(10), {
            every_group_led(&client).await
        });
    }
    for id in 1..=NODES {
        cluster.kill(id).await;
    }
}

#[test]
fn a_rolling_restart_survives_old_connections_that_vanish() {
    check_few_seeds(3, |seed| Sim::new(seed).run(restarts_at_new_addresses));
}

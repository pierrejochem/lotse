//! The tools against real nodes in the simulation: a cluster that requires
//! authentication, principals with different roles, and each tool run as
//! the principal its client signed in as.
//!
//! Checked: a viewer looks but cannot produce, and the refusal is the
//! broker's, as a tool result; `read_stream` stops at 100 records or 1 MiB
//! of record data; keys and values that are not UTF-8 come back as base64;
//! the destructive tools do nothing unless `confirm` names the resource;
//! invalid arguments are tool errors, never panics.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::scram::ScramCredential;
use lotse_client::{Client, ClientConfig, Credentials};
use lotse_log::LogConfig;
use lotse_mcp::{ToolResult, Tools};
use lotse_proto::{RoleBindingData, role, subject_kind};
use lotse_raft::NodeId;
use lotse_record::Batch;
use lotse_rt::{Clock, PeerIdentity};
use lotse_server::{AuthConfig, Node, NodeConfig, StateMachine};
use lotse_sim::{Sim, SimRuntime};
use serde_json::{Value, json};

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
const USERS: [&str; 3] = ["root", "ta", "v"];

fn config(id: NodeId) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|i| (i, format!("n{i}:7000"))).collect();
    let users =
        USERS.iter().map(|u| (u.to_string(), ScramCredential::new(&format!("{u}-pw"), u.as_bytes(), 4096))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        groups: vec![1, 2],
        tick: TICK,
        election_timeout: TICK * 10,
        heartbeat_interval: TICK * 2,
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: (1..=3).map(|i| (i, format!("n{i}:9000"))).collect(),
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
        quota_check_interval: lotse_server::DEFAULT_QUOTA_CHECK_INTERVAL,
        versions: Default::default(),
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth: AuthConfig {
            required: true,
            mtls: true,
            scram: true,
            users,
            admins: vec!["root".into()],
            ..AuthConfig::none()
        },
    }
}

async fn cluster(rt: &SimRuntime) -> Vec<Node<SimRuntime>> {
    rt.net().verify_peers(true);
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let host = rt.node(&format!("n{id}")).with_identity(Some(PeerIdentity::Node(id)));
        nodes.push(Node::start(host, config(id), |_| Nothing).await.unwrap());
    }
    nodes
}

async fn stop(nodes: Vec<Node<SimRuntime>>) {
    for n in nodes {
        n.handle().stop();
        n.join().await.unwrap();
    }
}

/// The tools as `user`, whose resources go to `tenant`.
fn tools(rt: &SimRuntime, user: &str, tenant: &str) -> Tools<SimRuntime> {
    let client = Client::new(
        rt.node(&format!("c-{user}")),
        ClientConfig {
            retry_for: Duration::from_secs(10),
            credentials: Some(Credentials::Scram { user: user.into(), password: format!("{user}-pw") }),
            tenant: tenant.into(),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    );
    Tools::new(Arc::new(client))
}

/// Lets the metadata group's latest change reach every node.
async fn settle(rt: &SimRuntime) {
    rt.sleep(TICK * 50).await;
}

/// A call that must succeed.
async fn ok(t: &Tools<SimRuntime>, name: &str, args: Value) -> Value {
    let r = t.call(name, args.clone()).await;
    assert!(!r.is_error, "{name} {args} failed: {}", r.text);
    assert!(!r.text.is_empty(), "{name} says what it did");
    r.structured
}

/// A call that must fail, with the error code it must name.
async fn fails(t: &Tools<SimRuntime>, name: &str, args: Value, code: &str) -> ToolResult {
    let r = t.call(name, args.clone()).await;
    assert!(r.is_error, "{name} {args} should fail: {}", r.structured);
    assert!(r.text.starts_with(&format!("{code}: ")), "{name} {args}: {}", r.text);
    assert_eq!(r.structured["error"]["code"], code, "{}", r.structured);
    r
}

fn names(list: &Value) -> Vec<String> {
    list["resources"].as_array().unwrap().iter().map(|r| r["name"].as_str().unwrap().to_string()).collect()
}

#[test]
fn a_viewer_can_look_but_not_produce() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let root = tools(&rt, "root", "shop");
            // Tenants and role bindings are not tools: set them up with the
            // client directly.
            let admin = Client::new(
                rt.node("c-setup"),
                ClientConfig {
                    credentials: Some(Credentials::Scram { user: "root".into(), password: "root-pw".into() }),
                    ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                },
            );
            admin.create_tenant("shop").await.unwrap();
            let bind = |who: &str, role| RoleBindingData {
                subject_kind: subject_kind::USER,
                subject: who.into(),
                role,
                tenant: "shop".into(),
                prefix: String::new(),
            };
            admin
                .alter_role_bindings(vec![bind("ta", role::TENANT_ADMIN), bind("v", role::VIEWER)], vec![])
                .await
                .unwrap();
            settle(&rt).await;

            let ta = tools(&rt, "ta", "shop");
            ok(&ta, "create_stream", json!({"name": "orders", "partitions": 2, "replication": 3})).await;
            ok(&ta, "create_queue", json!({"name": "jobs", "replication": 3})).await;
            ok(&ta, "produce", json!({"name": "orders", "records": [{"value": "o1"}]})).await;
            settle(&rt).await;

            let v = tools(&rt, "v", "shop");
            let overview = ok(&v, "cluster_overview", json!({})).await;
            assert_eq!(overview["nodes"].as_array().unwrap().len(), 3, "{overview}");
            // A viewer of one tenant is refused the upgrade state: that is "not shown", not a failure.
            assert_eq!(overview["cluster_version"], Value::Null, "{overview}");
            assert_eq!(overview["cluster_version_error"], Value::Null, "a refusal is not an error: {overview}");
            let mut seen = names(&ok(&v, "list_resources", json!({})).await);
            seen.sort();
            assert_eq!(seen, vec!["jobs".to_string(), "orders".to_string()], "what the viewer may see");
            let stream = ok(&v, "describe_stream", json!({"name": "orders"})).await;
            assert_eq!(stream["partitions"].as_array().unwrap().len(), 2, "{stream}");
            ok(&v, "describe_queue", json!({"name": "jobs"})).await;
            ok(&v, "node_metrics", json!({})).await;
            ok(&v, "list_sessions", json!({})).await;
            ok(&v, "list_tokens", json!({})).await;

            let refused =
                fails(&v, "produce", json!({"name": "orders", "records": [{"value": "x"}]}), "UNAUTHORIZED").await;
            assert!(refused.text.len() > "UNAUTHORIZED: ".len(), "the broker's reason: {}", refused.text);
            fails(&v, "create_stream", json!({"name": "more", "partitions": 1}), "UNAUTHORIZED").await;

            // Nothing was written by the refused produce. (Offsets count Raft
            // entries too, so count the records.)
            let mut values = Vec::new();
            for partition in 0..2 {
                let read = ok(&root, "read_stream", json!({"stream": "orders", "partition": partition})).await;
                values.extend(read["records"].as_array().unwrap().iter().map(|r| r["value"].clone()));
            }
            assert_eq!(values, vec![json!("o1")], "only the tenant admin's record");
            stop(nodes).await;
        });
    });
}

#[test]
fn read_stream_stops_at_100_records_or_1_mib() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_stream", json!({"name": "many", "partitions": 1})).await;
            for chunk in 0..3 {
                let records: Vec<Value> = (0..50).map(|i| json!({"value": format!("r{}", chunk * 50 + i)})).collect();
                ok(&t, "produce", json!({"name": "many", "records": records})).await;
            }
            // Offsets count Raft entries too: the records' offsets rise, with
            // gaps.
            let first = ok(&t, "read_stream", json!({"stream": "many", "offset": 0})).await;
            let records = first["records"].as_array().unwrap();
            assert_eq!(records.len(), 100, "at most 100 records");
            let values: Vec<String> = records.iter().map(|r| r["value"].as_str().unwrap().to_string()).collect();
            assert_eq!(values, (0..100).map(|i| format!("r{i}")).collect::<Vec<_>>());
            assert!(records.windows(2).all(|w| w[0]["offset"].as_u64() < w[1]["offset"].as_u64()));
            assert_eq!(first["limited_by"], "records");
            let next = first["next_offset"].as_u64().unwrap();
            assert!(next > records[99]["offset"].as_u64().unwrap());
            let rest = ok(&t, "read_stream", json!({"stream": "many", "offset": next})).await;
            let rest_values: Vec<&str> =
                rest["records"].as_array().unwrap().iter().map(|r| r["value"].as_str().unwrap()).collect();
            assert_eq!(rest_values.len(), 50, "the rest");
            assert_eq!(rest_values[0], "r100", "next_offset is the first record not returned");
            assert_eq!(rest["limited_by"], Value::Null);
            let tenth = records[10]["offset"].as_u64().unwrap();
            let fewer = ok(&t, "read_stream", json!({"stream": "many", "offset": tenth, "max_records": 5})).await;
            assert_eq!(fewer["records"].as_array().unwrap().len(), 5);
            assert_eq!(fewer["records"][0]["value"], "r10");
            assert_eq!(fewer["limited_by"], "records");
            let from_start = ok(&t, "read_stream", json!({"stream": "many", "time_ms": 0, "max_records": 1})).await;
            assert_eq!(from_start["records"][0]["value"], "r0", "time 0 is before every record");

            // 300,000 bytes a record: three fit in 1 MiB (1,048,576 bytes),
            // four do not.
            ok(&t, "create_stream", json!({"name": "big", "partitions": 1})).await;
            for _ in 0..5 {
                ok(&t, "produce", json!({"name": "big", "records": [{"value": "x".repeat(300_000)}]})).await;
            }
            let big = ok(&t, "read_stream", json!({"stream": "big"})).await;
            assert_eq!(big["records"].as_array().unwrap().len(), 3, "at most 1 MiB");
            assert_eq!(big["limited_by"], "bytes");
            let more = ok(&t, "read_stream", json!({"stream": "big", "offset": big["next_offset"]})).await;
            assert_eq!(more["records"].as_array().unwrap().len(), 2, "the next read goes on with the fourth");
            assert_eq!(more["limited_by"], Value::Null);
            stop(nodes).await;
        });
    });
}

#[test]
fn a_consumer_group_shows_its_members_and_lag() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_stream", json!({"name": "events", "partitions": 1})).await;
            let ten: Vec<Value> = (0..10).map(|i| json!({"value": format!("e{i}")})).collect();
            ok(&t, "produce", json!({"name": "events", "records": ten})).await;
            let consumer = Client::new(
                rt.node("c-group"),
                ClientConfig {
                    credentials: Some(Credentials::Scram { user: "root".into(), password: "root-pw".into() }),
                    ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                },
            );
            let mut member = consumer.join_group("g", &["events"], lotse_client::GroupConfig::default()).await.unwrap();
            let mut got = 0;
            for _ in 0..100 {
                got += member.poll(TICK * 10).await.unwrap().len();
                if got == 10 {
                    break;
                }
            }
            assert_eq!(got, 10, "the member read everything");
            member.commit().await.unwrap();
            let committed = member.positions()[&("events".to_string(), 0)];
            let five: Vec<Value> = (0..5).map(|i| json!({"value": format!("late {i}")})).collect();
            ok(&t, "produce", json!({"name": "events", "records": five})).await;
            member.poll(TICK).await.unwrap();
            settle(&rt).await;
            member.poll(TICK).await.unwrap();

            let latest =
                ok(&t, "list_offsets", json!({"stream": "events"})).await["partitions"][0]["latest"].as_u64().unwrap();
            let described = ok(&t, "describe_consumer_group", json!({"group_id": "g"})).await;
            let g = &described["groups"][0];
            assert_eq!(g["members"].as_array().unwrap().len(), 1, "{described}");
            assert_eq!(g["members"][0]["partitions"][0]["stream"], "events", "{described}");
            assert_eq!(g["partitions"][0]["committed_offset"], committed, "{described}");
            assert_eq!(g["partitions"][0]["lag"], latest - committed, "lag is the end less the committed offset");
            assert!(latest > committed, "five records came after the commit");
            fails(&t, "describe_consumer_group", json!({"group_id": "nobody"}), "UNKNOWN_RESOURCE").await;
            let stream = ok(&t, "describe_stream", json!({"name": "events"})).await;
            assert_eq!(stream["consumer_groups"][0]["lag"], latest - committed, "{stream}");
            stop(nodes).await;
        });
    });
}

#[test]
fn exchanges_bindings_settings_and_partitions_change_through_the_tools() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_queue", json!({"name": "eu", "configs": {"max.deliveries": "5"}})).await;
            ok(&t, "create_queue", json!({"name": "us"})).await;
            ok(&t, "declare_exchange", json!({"name": "orders", "type": "topic"})).await;
            ok(&t, "bind", json!({"exchange": "orders", "queue": "eu", "routing_key": "eu.#"})).await;
            ok(&t, "bind", json!({"exchange": "orders", "queue": "us", "routing_key": "us.#"})).await;
            settle(&rt).await;
            let sent = ok(&t, "publish", json!({"exchange": "orders", "routing_key": "eu.de", "value": "o1"})).await;
            assert_eq!(sent["routed"].as_array().unwrap().len(), 1, "{sent}");
            assert_eq!(sent["routed"][0]["queue"], "eu");
            let exchanges = ok(&t, "list_resources", json!({"kind": "exchange"})).await;
            assert_eq!(exchanges["resources"][0]["exchange_type"], "topic", "{exchanges}");
            assert_eq!(exchanges["resources"][0]["bindings"].as_array().unwrap().len(), 2, "{exchanges}");

            ok(&t, "unbind", json!({"exchange": "orders", "queue": "eu", "routing_key": "eu.#"})).await;
            settle(&rt).await;
            let dropped = ok(&t, "publish", json!({"exchange": "orders", "routing_key": "eu.de", "value": "o2"})).await;
            assert_eq!(dropped["routed"], json!([]), "no binding left for eu");
            fails(
                &t,
                "publish",
                json!({"exchange": "orders", "routing_key": "eu.de", "value": "o3", "mandatory": true}),
                "UNROUTABLE",
            )
            .await;

            // A setting that deletes nothing changes without confirm.
            ok(&t, "alter_configs", json!({"name": "eu", "set": {"ack.timeout.ms": "45000"}})).await;
            let eu = ok(&t, "describe_queue", json!({"name": "eu"})).await;
            assert_eq!(eu["configs"]["ack.timeout.ms"], "45000", "{eu}");
            // Without a dead-letter queue, a delivery limit or a lifetime drops messages.
            for args in [
                json!({"name": "eu", "set": {"max.deliveries": "7"}}),
                json!({"name": "eu", "set": {"message.ttl.ms": "1000"}}),
                json!({"name": "eu", "reset": ["max.deliveries"]}),
            ] {
                fails(&t, "alter_configs", args, "CONFIRMATION_MISMATCH").await;
            }
            ok(&t, "alter_configs", json!({"name": "eu", "set": {"max.deliveries": "7"}, "confirm": "eu"})).await;
            let eu = ok(&t, "describe_queue", json!({"name": "eu"})).await;
            assert_eq!(eu["configs"]["max.deliveries"], "7", "{eu}");
            ok(&t, "alter_configs", json!({"name": "eu", "reset": ["max.deliveries"], "confirm": "eu"})).await;
            let eu = ok(&t, "describe_queue", json!({"name": "eu"})).await;
            assert_ne!(eu["configs"]["max.deliveries"], "7", "{eu}");
            // A length limit can drop messages: without confirm nothing changes.
            for args in [
                json!({"name": "eu", "set": {"max.length": "1"}}),
                json!({"name": "eu", "set": {"max.deliveries": "3", "overflow": "drop-head"}, "confirm": "EU"}),
                json!({"name": "eu", "reset": ["max.length.bytes"]}),
            ] {
                let r = fails(&t, "alter_configs", args, "CONFIRMATION_MISMATCH").await;
                assert!(r.text.contains("can delete data"), "{}", r.text);
            }
            let eu = ok(&t, "describe_queue", json!({"name": "eu"})).await;
            assert_ne!(eu["configs"]["max.length"], "1", "{eu}");
            assert_ne!(eu["configs"]["max.deliveries"], "3", "nothing of a refused change is applied: {eu}");
            ok(&t, "alter_configs", json!({"name": "eu", "set": {"max.length": "1"}, "confirm": "eu"})).await;
            let eu = ok(&t, "describe_queue", json!({"name": "eu"})).await;
            assert_eq!(eu["configs"]["max.length"], "1", "{eu}");

            ok(&t, "create_stream", json!({"name": "s", "partitions": 1})).await;
            ok(&t, "create_partitions", json!({"name": "s", "count": 3})).await;
            fails(&t, "alter_configs", json!({"name": "s", "set": {"retention.ms": "1000"}}), "CONFIRMATION_MISMATCH")
                .await;
            ok(&t, "alter_configs", json!({"name": "s", "set": {"retention.ms": "1000"}, "confirm": "s"})).await;
            let s = ok(&t, "describe_stream", json!({"name": "s"})).await;
            assert_eq!(s["configs"]["retention.ms"], "1000", "{s}");
            settle(&rt).await;
            let offsets = ok(&t, "list_offsets", json!({"stream": "s", "time_ms": 0})).await;
            assert_eq!(offsets["partitions"].as_array().unwrap().len(), 3, "{offsets}");
            let upgrade = ok(&t, "describe_upgrade", json!({})).await;
            assert_eq!(upgrade["nodes"].as_array().unwrap().len(), 3, "{upgrade}");
            stop(nodes).await;
        });
    });
}

#[test]
fn binary_keys_and_values_come_back_as_base64() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_stream", json!({"name": "bin", "partitions": 1})).await;
            let records = json!([
                // 0xff 0x00 and 0x80 0x81: not UTF-8.
                {"key": "/wA=", "value": "gIE=", "headers": {"h": "/w=="}, "encoding": "base64"},
                {"key": "k", "value": "plain text", "headers": {"h": "v"}},
                // Valid UTF-8 sent as base64 comes back as text.
                {"value": "aGk=", "encoding": "base64"},
            ]);
            ok(&t, "produce", json!({"name": "bin", "records": records})).await;
            let read = ok(&t, "read_stream", json!({"stream": "bin"})).await;
            let r = read["records"].as_array().unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(r[0]["encoding"], "base64", "{}", r[0]);
            assert_eq!(r[0]["key"], "/wA=");
            assert_eq!(r[0]["value"], "gIE=");
            assert_eq!(r[0]["headers"]["h"], "/w==");
            assert_eq!(r[1]["encoding"], "utf8", "{}", r[1]);
            assert_eq!(r[1]["key"], "k");
            assert_eq!(r[1]["value"], "plain text");
            assert_eq!(r[1]["headers"]["h"], "v");
            assert_eq!(r[2]["encoding"], "utf8");
            assert_eq!(r[2]["key"], Value::Null, "no key");
            assert_eq!(r[2]["value"], "hi");
            assert!(r[0]["timestamp_ms"].as_i64().unwrap() > 0, "stamped with the runtime's clock");
            stop(nodes).await;
        });
    });
}

#[test]
fn destructive_tools_need_the_name_to_confirm() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_stream", json!({"name": "doomed", "partitions": 1})).await;
            ok(&t, "create_queue", json!({"name": "jobs"})).await;
            ok(&t, "create_queue", json!({"name": "dead"})).await;
            let three: Vec<Value> = (0..3).map(|i| json!({"value": format!("job {i}")})).collect();
            ok(&t, "produce", json!({"name": "jobs", "records": three})).await;
            settle(&rt).await;

            fails(&t, "delete_resource", json!({"name": "doomed"}), "INVALID_ARGUMENT").await;
            fails(&t, "delete_resource", json!({"name": "doomed", "confirm": "doome"}), "CONFIRMATION_MISMATCH").await;
            fails(&t, "delete_resource", json!({"name": "doomed", "confirm": "DOOMED"}), "CONFIRMATION_MISMATCH").await;
            fails(&t, "purge_queue", json!({"queue": "jobs"}), "INVALID_ARGUMENT").await;
            fails(&t, "purge_queue", json!({"queue": "jobs", "confirm": "job"}), "CONFIRMATION_MISMATCH").await;
            fails(&t, "requeue_dead_letters", json!({"queue": "dead", "confirm": "jobs"}), "CONFIRMATION_MISMATCH")
                .await;
            settle(&rt).await;
            assert!(names(&ok(&t, "list_resources", json!({})).await).contains(&"doomed".to_string()), "still there");

            // With the name, they act: all three jobs were still there.
            let purged = ok(&t, "purge_queue", json!({"queue": "jobs", "confirm": "jobs"})).await;
            assert_eq!(purged["purged"], 3, "{purged}");
            let moved = ok(&t, "requeue_dead_letters", json!({"queue": "dead", "confirm": "dead"})).await;
            assert_eq!(moved["requeued"], 0, "{moved}");
            ok(&t, "delete_resource", json!({"name": "doomed", "confirm": "doomed"})).await;
            settle(&rt).await;
            assert!(!names(&ok(&t, "list_resources", json!({})).await).contains(&"doomed".to_string()), "deleted");
            stop(nodes).await;
        });
    });
}

#[test]
fn invalid_arguments_are_tool_errors_not_panics() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let t = tools(&rt, "root", "default");
            ok(&t, "create_stream", json!({"name": "s", "partitions": 1})).await;
            let bad = [
                ("no_such_tool", json!({})),
                ("list_resources", json!([1, 2])),
                ("list_resources", json!("text")),
                ("list_resources", json!({"kind": "topic"})),
                ("list_resources", json!({"surprise": 1})),
                ("describe_stream", json!({})),
                ("describe_stream", json!({"name": 7})),
                ("create_stream", json!({"name": "x", "partitions": "three"})),
                ("create_stream", json!({"name": "x", "partitions": -1})),
                ("create_stream", json!({"name": "x", "partitions": 1, "replication": 300})),
                ("create_stream", json!({"name": "x", "partitions": 1, "configs": {"retention.ms": 5}})),
                ("read_stream", json!({"stream": "s", "offset": 0, "time_ms": 0})),
                ("read_stream", json!({"stream": "s", "max_records": 101})),
                ("read_stream", json!({"stream": "s", "max_records": 0})),
                ("read_stream", json!({"stream": "s", "partition": 4294967296u64})),
                ("produce", json!({"name": "s", "records": []})),
                ("produce", json!({"name": "s", "records": [{"value": "%%", "encoding": "base64"}]})),
                ("produce", json!({"name": "s", "records": [{"value": "v", "encoding": "latin1"}]})),
                ("produce", json!({"name": "s", "records": [{"value": "v", "colour": "red"}]})),
                ("produce", json!({"name": "s", "records": ["v"]})),
                ("declare_exchange", json!({"name": "x", "type": "bogus"})),
                ("alter_configs", json!({"name": "s"})),
                ("delete_resource", json!({"name": "s", "confirm": 1})),
            ];
            for (name, args) in bad {
                fails(&t, name, args, "INVALID_ARGUMENT").await;
            }
            // Arguments that are well formed but that the cluster refuses.
            let r = t.call("read_stream", json!({"stream": "s", "partition": 9})).await;
            assert!(r.is_error && !r.text.is_empty(), "{}", r.text);
            let r = t.call("describe_stream", json!({"name": "nowhere"})).await;
            assert!(r.is_error, "{}", r.text);
            let r = t.call("create_stream", json!({"name": "s", "partitions": 1})).await;
            assert!(r.is_error && r.text.starts_with("RESOURCE_EXISTS: "), "{}", r.text);
            stop(nodes).await;
        });
    });
}

/// Storage limits (ADR 0147): a cluster admin sets a tenant's limit and
/// reads it back with its usage; a viewer reads but sets nothing.
#[test]
fn storage_limits_are_set_and_read_through_the_tools() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let root = tools(&rt, "root", "default");
            // Quotas need cluster version 0.2, which a new cluster takes
            // once every node has reported its release.
            let deadline = rt.now() + Duration::from_secs(60);
            loop {
                let up = root.call("describe_upgrade", json!({})).await;
                if up.structured["cluster_version"].as_str().is_some_and(|v| v != "0.0" && v != "0.1") {
                    break;
                }
                assert!(rt.now() < deadline, "no cluster version 0.2: {}", up.structured);
                rt.sleep(TICK * 20).await;
            }
            ok(&root, "create_stream", json!({"name": "s", "partitions": 1, "replication": 3})).await;
            ok(&root, "set_quota", json!({"tenant": "default", "bytes": 1_000_000})).await;
            ok(&root, "set_quota", json!({"tenant": "default", "prefix": "s", "bytes": 500_000})).await;
            let q = ok(&root, "describe_quotas", json!({"tenant": "default"})).await;
            let limits = q["quotas"].as_array().unwrap();
            let own = limits.iter().find(|l| l["prefix"] == "").expect("the tenant's own limit");
            assert_eq!((own["bytes"].as_u64(), own["full"].as_bool()), (Some(1_000_000), Some(false)), "{q}");
            assert!(limits.iter().any(|l| l["prefix"] == "s" && l["bytes"] == 500_000), "{q}");
            ok(&root, "set_quota", json!({"tenant": "default", "prefix": "s"})).await;
            let q = ok(&root, "describe_quotas", json!({})).await;
            assert!(!q["quotas"].as_array().unwrap().iter().any(|l| l["prefix"] == "s"), "removed: {q}");

            let admin = Client::new(
                rt.node("c-setup"),
                ClientConfig {
                    credentials: Some(Credentials::Scram { user: "root".into(), password: "root-pw".into() }),
                    ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                },
            );
            let viewer = RoleBindingData {
                subject_kind: subject_kind::USER,
                subject: "v".into(),
                role: role::VIEWER,
                tenant: "default".into(),
                prefix: String::new(),
            };
            admin.alter_role_bindings(vec![viewer], vec![]).await.unwrap();
            settle(&rt).await;
            let v = tools(&rt, "v", "default");
            ok(&v, "describe_quotas", json!({})).await;
            fails(&v, "set_quota", json!({"tenant": "default", "prefix": "x.", "bytes": 1}), "UNAUTHORIZED").await;
            stop(nodes).await;
        });
    });
}

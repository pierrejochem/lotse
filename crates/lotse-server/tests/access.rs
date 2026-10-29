//! Who may do what, on a cluster that requires authentication: a cluster
//! admin from the configuration creates a tenant, grants roles in it
//! (one limited to a name prefix) and takes one back; each principal then
//! tries what its role allows and what it does not.
//!
//! Checked: every request needs the role for it, in the resource's tenant
//! and matching the binding's prefix; what a principal may not see is
//! unknown to it (metadata, tenants, metrics); groups at the identity
//! provider carry roles; a binding taken back stops working; tenants
//! holding resources cannot be deleted; only nodes write transaction
//! markers, and nodes do nothing else; refusals come at once, with a reason.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::scram::ScramCredential;
use lotse_auth::testing::TestIssuer;
use lotse_client::{Client, ClientConfig, ClientError, Credentials};
use lotse_log::LogConfig;
use lotse_proto::{ErrorCode, Request, Response, RoleBindingData, role, subject_kind};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Conn, Link, Net, PeerIdentity};
use lotse_server::{AuthConfig, Node, NodeConfig, OidcConfig, StateMachine, StreamConfig};
use lotse_sim::{Sim, SimRuntime};

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
const ISSUER: &str = "https://idp.test";
const JWKS: &str = "https://idp.test/keys";
const USERS: [&str; 7] = ["root", "ta", "p", "c", "v", "nobody", "mon"];

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn config(id: NodeId) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|i| (i, format!("n{i}:7000"))).collect();
    let users =
        USERS.iter().map(|u| (u.to_string(), ScramCredential::new(&format!("{u}-pw"), u.as_bytes(), 4096))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        groups: vec![1, 2, 3],
        tick: TICK,
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(format!("n{id}:9000")),
        client_addrs: (1..=3).map(|i| (i, format!("n{i}:9000"))).collect(),
        streams: vec![StreamConfig { name: "s".into(), resource_id: 7, partitions: vec![3] }],
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        auth: AuthConfig {
            required: true,
            mtls: true,
            scram: true,
            oidc: Some(OidcConfig::new(ISSUER, "lotse")),
            users,
            admins: vec!["root".into()],
            ..AuthConfig::none()
        },
    }
}

async fn cluster(rt: &SimRuntime) -> Vec<Node<SimRuntime>> {
    rt.net().verify_peers(true);
    let idp = TestIssuer::new(ISSUER, "k1", 5);
    rt.http().serve(&lotse_auth::oauth::discovery_url(ISSUER), idp.discovery(JWKS));
    rt.http().serve(JWKS, idp.jwks());
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let host = rt.node(&format!("n{id}")).with_identity(Some(PeerIdentity::Node(id)));
        nodes.push(Node::start(host, config(id), |_| Nothing).await.unwrap());
    }
    nodes
}

fn as_user(rt: &SimRuntime, user: &str, tenant: &str) -> Client<SimRuntime> {
    Client::new(
        rt.node(&format!("c-{user}")),
        ClientConfig {
            retry_for: Duration::from_secs(10),
            credentials: Some(Credentials::Scram { user: user.into(), password: format!("{user}-pw") }),
            tenant: tenant.into(),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    )
}

fn binding(kind: u8, who: &str, role: u8, tenant: &str, prefix: &str) -> RoleBindingData {
    RoleBindingData { subject_kind: kind, subject: who.into(), role, tenant: tenant.into(), prefix: prefix.into() }
}

fn refused<T: std::fmt::Debug>(r: Result<T, ClientError>) -> bool {
    matches!(r, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, message: Some(_) }))
}

/// Refused, or not even seen: a resource a principal may not see is
/// unknown to it.
fn kept_out<T: std::fmt::Debug>(r: Result<T, ClientError>) -> bool {
    matches!(r, Err(ClientError::UnknownStream(..))) || refused(r)
}

/// How many of the cluster's own groups (no resource) `c` sees in the
/// nodes' metrics.
async fn own_groups(c: &Client<SimRuntime>) -> usize {
    let all = c.node_metrics().await.unwrap();
    all.iter().flat_map(|(_, r)| r.as_ref().unwrap().partitions.iter()).filter(|p| p.resource_id == 0).count()
}

/// Lets the metadata group's latest change reach every node.
async fn settle(rt: &SimRuntime) {
    rt.sleep(TICK * 50).await;
}

#[test]
fn every_request_needs_its_role() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let root = as_user(&rt, "root", "shop");
            root.create_tenant("shop").await.expect("a cluster admin creates tenants");
            let user = |n| binding(subject_kind::USER, n, 0, "shop", "");
            let add = vec![
                RoleBindingData { role: role::TENANT_ADMIN, ..user("ta") },
                RoleBindingData { role: role::PRODUCER, prefix: "orders-".into(), ..user("p") },
                RoleBindingData { role: role::CONSUMER, ..user("c") },
                RoleBindingData { role: role::VIEWER, ..user("v") },
            ];
            root.alter_role_bindings(add, Vec::new()).await.expect("grants");
            settle(&rt).await;

            // The tenant admin creates in its tenant only.
            let ta = as_user(&rt, "ta", "shop");
            ta.create_stream("orders-eu", 1, 3).await.expect("in its tenant");
            ta.create_stream("payments", 1, 3).await.expect("in its tenant");
            ta.create_queue("orders-q", 3, None).await.expect("a queue too");
            let elsewhere = as_user(&rt, "ta", lotse_server::DEFAULT_TENANT);
            assert!(refused(elsewhere.create_stream("x", 1, 3).await), "not in default");
            assert!(refused(ta.create_tenant("other").await), "tenants are a cluster admin's");
            settle(&rt).await;

            // The producer writes to names with its prefix, and reads nothing.
            let start = rt.now();
            let p = as_user(&rt, "p", "shop");
            p.produce("orders-eu", 0, batch("o1")).await.expect("its prefix");
            assert!(kept_out(p.produce("payments", 0, batch("x")).await), "not its prefix");
            assert!(refused(p.fetch("orders-eu", 0, 0, TICK).await), "a producer does not read");
            assert!(kept_out(p.produce("s", 0, batch("x")).await), "not its tenant");
            p.produce("orders-q", 0, batch("job")).await.expect("its prefix covers the queue");
            assert!(refused(p.subscribe("orders-q", 1).await.map(|_| ())), "a producer does not consume");
            let polled = match p.join_group("g", &["orders-eu"], lotse_client::GroupConfig::default()).await {
                Ok(mut member) => member.poll(TICK * 10).await.map(|_| ()),
                Err(e) => Err(e),
            };
            assert!(refused(polled), "nor joins consumer groups");
            assert!(rt.now() - start < Duration::from_secs(2), "refusals come at once");
            let mut seen: Vec<String> = p.metadata(&[]).await.unwrap().resources.into_iter().map(|r| r.name).collect();
            seen.sort();
            assert_eq!(seen, vec!["orders-eu".to_string(), "orders-q".to_string()], "only what it may touch");

            // The consumer reads, the viewer sees.
            let c = as_user(&rt, "c", "shop");
            let f = c.fetch("orders-eu", 0, 0, TICK * 10).await.expect("a consumer reads");
            assert!(f.high_watermark > 0);
            assert!(refused(c.produce("orders-eu", 0, batch("x")).await));
            assert!(refused(c.delete("orders-eu").await), "nor deletes");
            let mut sub = c.subscribe("orders-q", 1).await.expect("a consumer subscribes");
            let m = loop {
                if let Some(m) = sub.next(TICK * 50).await.expect("delivers") {
                    break m;
                }
            };
            sub.ack(&m).await.expect("and acknowledges");
            drop(sub);
            let v = as_user(&rt, "v", "shop");
            let mut seen: Vec<String> = v.metadata(&[]).await.unwrap().resources.into_iter().map(|r| r.name).collect();
            seen.sort();
            assert_eq!(seen, vec!["orders-eu".to_string(), "orders-q".to_string(), "payments".to_string()]);
            assert!(v.metadata(&[]).await.unwrap().resources.iter().all(|r| r.tenant == "shop"));
            assert!(refused(v.fetch("payments", 0, 0, TICK).await));

            // Nobody: authenticated, but no role anywhere.
            let nobody = as_user(&rt, "nobody", "shop");
            assert!(kept_out(nobody.produce("s", 0, batch("x")).await));
            assert!(nobody.metadata(&[]).await.unwrap().resources.is_empty(), "sees nothing");
            assert!(nobody.describe_tenants().await.unwrap().is_empty());
            let unknown = nobody.metadata(&["orders-eu"]).await.unwrap();
            assert_eq!(unknown.resources[0].error, ErrorCode::UNKNOWN_RESOURCE, "as if it did not exist");
            let me = nobody.who_am_i().await.unwrap();
            assert_eq!((me.name.as_str(), me.bindings.len()), ("nobody", 0));
            let metrics = nobody.node_metrics().await.unwrap();
            assert!(metrics.iter().all(|(_, m)| m.as_ref().is_ok_and(|m| m.partitions.is_empty())), "no partitions");
            let metrics = v.node_metrics().await.unwrap();
            let parts: Vec<u64> = metrics
                .iter()
                .flat_map(|(_, m)| m.as_ref().unwrap().partitions.iter().map(|p| p.resource_id))
                .collect();
            assert!(!parts.is_empty() && !parts.contains(&7), "the viewer sees shop's partitions only: {parts:?}");

            // Every request on a resource needs its role, also those the client
            // only sends in the middle of something (a viewer, logged in by its
            // certificate, sends them directly).
            let ids = root.metadata(&[]).await.unwrap();
            let id = |n: &str| ids.resources.iter().find(|r| r.name == n).unwrap().resource_id;
            let (stream, queue) = (id("orders-eu"), id("orders-q"));
            let part = || vec![lotse_proto::TxnResource { resource_id: stream, partitions: vec![0] }];
            let commit = || {
                vec![lotse_proto::OffsetCommitResource {
                    resource_id: stream,
                    partitions: vec![lotse_proto::OffsetCommitPartition {
                        partition: 0,
                        offset: 1,
                        leader_term: 0,
                        metadata: None,
                    }],
                }]
            };
            let viewer = Some(PeerIdentity::Principal("v".into()));
            let requests = [
                Request::AddPartitionsToTxn(lotse_proto::AddPartitionsToTxnRequest {
                    txn_id: "t".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                    resources: part(),
                }),
                Request::Ack(lotse_proto::AckRequest { queue_id: queue, txn_id: None, ranges: vec![(0, 1)] }),
                Request::Nack(lotse_proto::NackRequest {
                    queue_id: queue,
                    ranges: vec![(0, 1)],
                    requeue: true,
                    delay_ms: 0,
                }),
                Request::CommitOffsets(lotse_proto::CommitOffsetsRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    member_epoch: 1,
                    resources: commit(),
                }),
                Request::OffsetFetch(lotse_proto::OffsetFetchRequest {
                    group_id: "g".into(),
                    require_stable: false,
                    resources: part(),
                }),
                Request::AddOffsetsToTxn(lotse_proto::AddOffsetsToTxnRequest {
                    txn_id: "t".into(),
                    producer_id: 1,
                    producer_epoch: 0,
                    group_id: "g".into(),
                    member_id: "m".into(),
                    member_epoch: 1,
                    resources: commit(),
                }),
                Request::Bind(lotse_proto::BindRequest {
                    exchange_id: stream,
                    queue_id: queue,
                    binding_key: "k".into(),
                    header_match: Vec::new(),
                    match_all: true,
                }),
                Request::AlterConfigs(lotse_proto::AlterConfigsRequest {
                    resource_id: queue,
                    set: vec![("ack.timeout.ms".into(), "5000".into())],
                    delete: Vec::new(),
                }),
            ];
            for req in &requests {
                let answer = raw_call(&rt, viewer.clone(), req).await;
                assert!(
                    matches!(answer, Response::Refused { code: ErrorCode::UNAUTHORIZED, .. }),
                    "{req:?}: {answer:?}"
                );
            }
            let describe = Request::DescribeConfigs(lotse_proto::DescribeConfigsRequest { resource_ids: vec![queue] });
            let answer = raw_call(&rt, Some(PeerIdentity::Principal("nobody".into())), &describe).await;
            assert!(matches!(answer, Response::Refused { .. }), "settings are seen by those who may see: {answer:?}");

            // Users and bindings are a cluster admin's.
            assert!(refused(ta.set_user_password("x", "y").await));
            assert!(refused(ta.describe_role_bindings().await));
            let all = root.describe_role_bindings().await.unwrap();
            assert!(all.iter().any(|(b, src)| b.subject == "root" && b.role == role::CLUSTER_ADMIN && *src == 2));
            assert_eq!(all.len(), 5);
            let configured = binding(subject_kind::USER, "root", role::CLUSTER_ADMIN, "", "");
            let e = root.alter_role_bindings(Vec::new(), vec![configured]).await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "configured: {e}");

            // Taken back: the producer is refused from then on.
            root.alter_role_bindings(
                Vec::new(),
                vec![RoleBindingData { role: role::PRODUCER, prefix: "orders-".into(), ..user("p") }],
            )
            .await
            .unwrap();
            settle(&rt).await;
            assert!(kept_out(p.produce("orders-eu", 0, batch("x")).await), "revoked");

            // A tenant with resources stays.
            let e = root.delete_tenant("shop").await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "{e}");
            root.delete("orders-eu").await.unwrap();
            root.delete("payments").await.unwrap();
            root.delete("orders-q").await.unwrap();
            settle(&rt).await;
            root.delete_tenant("shop").await.expect("now empty");
            settle(&rt).await;
            assert_eq!(root.describe_role_bindings().await.unwrap().len(), 1, "its bindings went with it");
            let e = root.create_stream("late", 1, 3).await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "no tenant: {e}");
            let nowhere = as_user(&rt, "root", "nowhere");
            let e = nowhere.create_stream("lost", 1, 3).await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "never was: {e}");
            let e = root.delete_tenant(lotse_server::DEFAULT_TENANT).await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "configured: {e}");
            for n in nodes {
                n.handle().stop();
                n.join().await.unwrap();
            }
        });
    });
}

#[test]
fn a_cluster_viewer_sees_everything_and_changes_nothing() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let root = as_user(&rt, "root", "shop");
            root.create_tenant("shop").await.expect("a cluster admin creates tenants");
            root.create_stream("orders-eu", 1, 3).await.expect("in the shop");
            // Named with a tenant and a prefix, the role holds everywhere
            // all the same.
            let mon = binding(subject_kind::USER, "mon", role::CLUSTER_VIEWER, "shop", "orders-");
            let v = binding(subject_kind::USER, "v", role::VIEWER, "shop", "");
            root.alter_role_bindings(vec![mon, v], Vec::new()).await.expect("grants");
            settle(&rt).await;
            let all = root.describe_role_bindings().await.unwrap();
            assert!(
                all.iter().any(|(b, _)| b.subject == "mon" && b.tenant.is_empty() && b.prefix.is_empty()),
                "stored without tenant and prefix: {all:?}"
            );

            let mon = as_user(&rt, "mon", "shop");
            let mut seen: Vec<String> =
                mon.metadata(&[]).await.unwrap().resources.into_iter().map(|r| r.name).collect();
            seen.sort();
            assert_eq!(seen, ["orders-eu", "s"], "every tenant's resources");
            let tenants: Vec<String> = mon.describe_tenants().await.unwrap().into_iter().map(|t| t.name).collect();
            assert_eq!(tenants, [lotse_server::DEFAULT_TENANT, "shop"]);
            assert!(own_groups(&mon).await > 0, "and the cluster's own groups");
            assert_eq!(own_groups(&as_user(&rt, "v", "shop")).await, 0, "which a tenant's viewer does not see");

            // Seeing is all it does.
            assert!(refused(mon.fetch("s", 0, 0, TICK).await), "no reading");
            assert!(refused(mon.produce("orders-eu", 0, batch("x")).await), "no writing");
            assert!(refused(mon.create_stream("orders-us", 1, 3).await), "no creating");
            assert!(refused(mon.delete("orders-eu").await), "no deleting");
            assert!(refused(mon.create_tenant("other").await), "no tenants");
            assert!(refused(mon.describe_role_bindings().await), "no one else's bindings");
            let grab = binding(subject_kind::USER, "mon", role::CLUSTER_ADMIN, "", "");
            assert!(refused(mon.alter_role_bindings(vec![grab], Vec::new()).await), "no promoting itself");
            assert!(refused(mon.set_user_password("root", "mine").await), "no users");

            drop((root, mon));
            for node in nodes {
                node.handle().stop();
                let _ = node.join().await;
            }
        })
    });
}

#[test]
fn a_refusal_is_reported_for_its_own_request_only() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let mut nodes = cluster(&rt).await;
            let root = as_user(&rt, "root", lotse_server::DEFAULT_TENANT);
            let p = binding(subject_kind::USER, "p", role::PRODUCER, lotse_server::DEFAULT_TENANT, "");
            root.alter_role_bindings(vec![p], Vec::new()).await.expect("grants");
            settle(&rt).await;
            let p = as_user(&rt, "p", lotse_server::DEFAULT_TENANT);
            p.produce("s", 0, batch("first")).await.expect("a producer writes");
            assert!(refused(p.create_stream("mine", 1, 3).await), "but creates nothing");

            // A login that failed no longer holds once the password is set.
            let late = as_user(&rt, "late", lotse_server::DEFAULT_TENANT);
            // Asked of every node at once, outside any retry: each answer fails.
            let early = late.node_metrics().await;
            assert!(
                matches!(&early, Err(ClientError::Authentication(_)))
                    || early.as_ref().is_ok_and(|all| all.iter().all(|(_, r)| r.is_err())),
                "no such user yet: {early:?}"
            );
            root.set_user_password("late", "late-pw").await.expect("creates the user");
            settle(&rt).await;
            late.describe_tenants().await.expect("logs in now");

            // The metadata group's leader goes away: the next admin request
            // has to try again, and must not come back with the create's
            // refusal.
            let all = root.node_metrics().await.unwrap();
            let leader = all
                .iter()
                .filter_map(|(n, r)| r.as_ref().ok().map(|m| (n, m)))
                .find(|(_, m)| m.partitions.iter().any(|g| g.leader && g.group == m.metadata_group))
                .map(|(n, _)| *n as usize)
                .expect("the metadata group has a leader");
            // Nodes 1 to 3, in order.
            let gone = nodes.remove(leader - 1);
            gone.handle().stop();
            let _ = gone.join().await;
            let tenants = p.describe_tenants().await.expect("answered once a new leader is elected");
            assert_eq!(tenants.len(), 1, "{tenants:?}");
            late.describe_tenants().await.expect("and not with the old failed login");

            drop((root, p, late));
            for node in nodes {
                node.handle().stop();
                let _ = node.join().await;
            }
        })
    });
}

#[test]
fn groups_at_the_identity_provider_carry_roles() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let root = as_user(&rt, "root", lotse_server::DEFAULT_TENANT);
            let ops = binding(subject_kind::GROUP, "ops", role::PRODUCER, lotse_server::DEFAULT_TENANT, "");
            root.alter_role_bindings(vec![ops], Vec::new()).await.unwrap();
            settle(&rt).await;
            let idp = TestIssuer::new(ISSUER, "k1", 5);
            let exp = rt.unix_time().as_secs() + 600;
            let token = |groups: &[&str]| {
                let t = idp.token("dana", groups, "lotse", exp);
                Some(Credentials::OAuthBearer(Arc::new(move || t.clone())))
            };
            let with = |host: &str, creds| {
                Client::new(
                    rt.node(host),
                    ClientConfig {
                        retry_for: Duration::from_secs(10),
                        credentials: creds,
                        ..ClientConfig::new(vec!["n1:9000".into()])
                    },
                )
            };
            with("d1", token(&["ops"])).produce("s", 0, batch("dana")).await.expect("through ops");
            assert!(kept_out(with("d2", token(&["dev"])).produce("s", 0, batch("x")).await), "not in ops");
            for n in nodes {
                n.handle().stop();
                n.join().await.unwrap();
            }
        });
    });
}

/// One request on a raw connection to node 1, from a host proving
/// `identity`, logged in as `user` first if given.
async fn raw_call(rt: &SimRuntime, identity: Option<PeerIdentity>, req: &Request) -> Response {
    let conn = rt.node("raw").with_identity(identity).connect("n1:9000", Link::Client).await.unwrap();
    conn.send(lotse_proto::encode_request(1, req)).await.unwrap();
    let frame = conn.recv().await.unwrap().expect("an answer");
    lotse_proto::decode_response(frame, req.api_key()).unwrap().1
}

#[test]
fn only_nodes_write_markers_and_nodes_do_nothing_else() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let marker = Request::WriteTxnMarkers(lotse_proto::WriteTxnMarkersRequest { markers: Vec::new() });
            // A user with a certificate (here a cluster admin's name): refused.
            let admin = Some(PeerIdentity::Principal("root".into()));
            assert!(matches!(
                raw_call(&rt, admin, &marker).await,
                Response::Refused { code: ErrorCode::UNAUTHORIZED, .. }
            ));
            // A node: let through (nothing to write here).
            assert!(matches!(raw_call(&rt, Some(PeerIdentity::Node(2)), &marker).await, Response::WriteTxnMarkers(_)));
            // But a node cannot produce or administer.
            let produce = Request::Produce(lotse_proto::ProduceRequest {
                txn_id: None,
                timeout_ms: 1000,
                resources: vec![lotse_proto::ProduceResource {
                    resource_id: 7,
                    partitions: vec![lotse_proto::ProducePartition { partition: 0, batch: batch("x") }],
                }],
            });
            assert!(matches!(raw_call(&rt, Some(PeerIdentity::Node(2)), &produce).await, Response::Refused { .. }));
            let tenant = Request::CreateTenant(lotse_proto::CreateTenantRequest { name: "n".into() });
            assert!(matches!(raw_call(&rt, Some(PeerIdentity::Node(2)), &tenant).await, Response::Refused { .. }));
            for n in nodes {
                n.handle().stop();
                n.join().await.unwrap();
            }
        });
    });
}

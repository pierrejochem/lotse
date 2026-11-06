//! Authentication on a cluster that requires it: three nodes with node
//! identities, SCRAM users configured and created at runtime, a fake
//! identity provider for OAUTHBEARER, and client certificates.
//!
//! Checked: nothing but `ApiVersions` and SASL works before a connection
//! authenticates; each mechanism lets the right principals in and keeps
//! the wrong ones out, with an answer that says so at once; users and keys
//! change at runtime; sessions end on time and clients renew them before
//! they do; authenticating again cannot change who a connection is; nodes
//! authenticate to each other by their certificates (transaction markers);
//! and authentication is refused without a network that verifies peers.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::scram::{ScramClient, ScramCredential};
use lotse_auth::testing::TestIssuer;
use lotse_client::{Client, ClientConfig, ClientError, Credentials};
use lotse_log::LogConfig;
use lotse_proto::{ErrorCode, Request, Response, SaslAuthenticateRequest, SaslHandshakeRequest};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{Clock, Conn, Link, Net, PeerIdentity};
use lotse_server::{AuthConfig, Node, NodeConfig, OidcConfig, StateMachine, StreamConfig};
use lotse_sim::{SIM_EPOCH, Sim, SimRuntime};

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
const AUDIENCE: &str = "lotse";
const MAX_SESSION: Duration = Duration::from_secs(5);

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn auth() -> AuthConfig {
    let mut users = BTreeMap::new();
    users.insert("admin".to_string(), ScramCredential::new("admin-pw", b"configured-salt", 4096));
    AuthConfig {
        required: true,
        mtls: true,
        scram: true,
        oidc: Some(OidcConfig::new(ISSUER, AUDIENCE)),
        max_session: MAX_SESSION,
        users,
        admins: vec!["admin".into()],
        ..AuthConfig::none()
    }
}

fn config(id: NodeId, auth: AuthConfig) -> NodeConfig {
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|i| (i, format!("n{i}:7000"))).collect();
    NodeConfig {
        id,
        listen: peers[&id].clone(),
        peers,
        // 1 metadata, 2 coordinator, 3 the stream.
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
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth,
    }
}

fn issuer(kid: &str) -> TestIssuer {
    TestIssuer::new(ISSUER, kid, 42)
}

/// Starts the cluster with every node proving its id, and the identity
/// provider serving its documents.
async fn cluster(rt: &SimRuntime) -> Vec<Node<SimRuntime>> {
    cluster_with(rt, auth()).await
}

async fn cluster_with(rt: &SimRuntime, auth: AuthConfig) -> Vec<Node<SimRuntime>> {
    rt.net().verify_peers(true);
    let idp = issuer("k1");
    rt.http().serve(&lotse_auth::oauth::discovery_url(ISSUER), idp.discovery(JWKS));
    rt.http().serve(JWKS, idp.jwks());
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let host = rt.node(&format!("n{id}")).with_identity(Some(PeerIdentity::Node(id)));
        nodes.push(Node::start(host, config(id, auth.clone()), |_| Nothing).await.unwrap());
    }
    // Authenticated is not enough: the users here also get roles.
    let admin = client(rt, "setup", scram("admin", "admin-pw"));
    let grant = |kind, who: &str, role| lotse_proto::RoleBindingData {
        subject_kind: kind,
        subject: who.into(),
        role,
        tenant: lotse_server::DEFAULT_TENANT.into(),
        prefix: String::new(),
    };
    let mut add = Vec::new();
    for role in [lotse_proto::role::PRODUCER, lotse_proto::role::CONSUMER] {
        add.push(grant(lotse_proto::subject_kind::GROUP, "ops", role));
        for user in ["alice", "billing"] {
            add.push(grant(lotse_proto::subject_kind::USER, user, role));
        }
    }
    admin.alter_role_bindings(add, Vec::new()).await.expect("the bootstrap admin grants roles");
    rt.sleep(TICK * 50).await;
    nodes
}

async fn stop(nodes: Vec<Node<SimRuntime>>) {
    for n in nodes {
        n.handle().stop();
        n.join().await.unwrap();
    }
}

fn client(rt: &SimRuntime, host: &str, credentials: Option<Credentials>) -> Client<SimRuntime> {
    Client::new(
        rt.node(host),
        ClientConfig {
            retry_for: Duration::from_secs(10),
            credentials,
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    )
}

fn scram(user: &str, password: &str) -> Option<Credentials> {
    Some(Credentials::Scram { user: user.into(), password: password.into() })
}

/// A token from the fake identity provider, valid for `secs` from now.
fn token(rt: &SimRuntime, idp: &TestIssuer, sub: &str, secs: i64) -> String {
    let exp = (rt.unix_time().as_secs() as i64 + secs) as u64;
    idp.token(sub, &["ops"], AUDIENCE, exp)
}

fn bearer(token: String) -> Option<Credentials> {
    Some(Credentials::OAuthBearer(Arc::new(move || token.clone())))
}

fn denied(r: Result<u64, ClientError>) -> bool {
    matches!(r, Err(ClientError::Authentication(_)))
}

#[test]
fn nothing_works_before_authenticating() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let start = rt.now();
            let anon = client(&rt, "c1", None);
            assert!(denied(anon.produce("s", 0, batch("x")).await), "no credentials");
            assert!(rt.now() - start < Duration::from_secs(1), "the answer comes at once, not after retrying");
            // But a client may ask which versions a node speaks.
            let conn = raw(&rt, "c2").await;
            assert!(matches!(call(&rt, &conn, 1, &Request::ApiVersions).await, Response::ApiVersions(_)));
            assert_eq!(
                call(&rt, &conn, 2, &Request::Metadata(lotse_proto::MetadataRequest { names: vec![] })).await,
                Response::Error(ErrorCode::UNAUTHORIZED)
            );
            stop(nodes).await;
        });
    });
}

#[test]
fn scram_users_from_the_configuration_and_from_the_metadata_group() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.produce("s", 0, batch("from admin")).await.expect("a configured user");
            assert!(denied(client(&rt, "c2", scram("admin", "wrong")).produce("s", 0, batch("x")).await));
            assert!(denied(client(&rt, "c3", scram("alice", "pw")).produce("s", 0, batch("x")).await), "unknown");

            admin.set_user_password("alice", "alice-pw").await.expect("creates alice");
            // Every node learns her credential once the metadata group applied it.
            rt.sleep(TICK * 50).await;
            for i in 0..3 {
                let alice = client(&rt, &format!("a{i}"), scram("alice", "alice-pw"));
                alice.produce("s", 0, batch("from alice")).await.expect("alice, on any node");
            }
            let users = admin.describe_users().await.expect("lists users");
            let names: Vec<(&str, u8)> = users.iter().map(|u| (u.name.as_str(), u.source)).collect();
            assert_eq!(names, vec![("admin", 2), ("alice", 1)], "configured and runtime users");
            let e = admin.set_user_password("admin", "new").await.unwrap_err();
            assert!(matches!(e, ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. }), "{e}");

            // A changed password: the old one stops working.
            admin.set_user_password("alice", "second-pw").await.expect("changes her password");
            rt.sleep(TICK * 50).await;
            assert!(denied(client(&rt, "b1", scram("alice", "alice-pw")).produce("s", 0, batch("x")).await));
            client(&rt, "b2", scram("alice", "second-pw")).produce("s", 0, batch("y")).await.expect("new password");

            // Removed: no new sessions.
            admin.delete_user("alice").await.expect("removes alice");
            rt.sleep(TICK * 50).await;
            assert!(denied(client(&rt, "b3", scram("alice", "second-pw")).produce("s", 0, batch("x")).await));
            stop(nodes).await;
        });
    });
}

#[test]
fn bearer_tokens_from_the_identity_provider() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let idp = issuer("k1");
            let bob = client(&rt, "c1", bearer(token(&rt, &idp, "bob", 600)));
            bob.produce("s", 0, batch("from bob")).await.expect("a good token");
            let expired = client(&rt, "c2", bearer(token(&rt, &idp, "bob", -120)));
            assert!(denied(expired.produce("s", 0, batch("x")).await), "expired");
            let other = idp.token("bob", &[], "someone-else", rt.unix_time().as_secs() + 600);
            assert!(denied(client(&rt, "c3", bearer(other)).produce("s", 0, batch("x")).await), "wrong audience");
            let forged = TestIssuer::new(ISSUER, "k1", 7).token("bob", &[], AUDIENCE, rt.unix_time().as_secs() + 600);
            assert!(denied(client(&rt, "c4", bearer(forged)).produce("s", 0, batch("x")).await), "wrong key");

            // The provider rotates its key. A token with the new key id makes
            // the nodes fetch the key set again, but not more often than
            // every 30 s, so made-up key ids cannot flood the provider.
            let rotated = issuer("k2");
            rt.http().serve(JWKS, rotated.jwks());
            let carol = client(&rt, "c5", bearer(token(&rt, &rotated, "carol", 600)));
            assert!(denied(carol.produce("s", 0, batch("x")).await), "the keys were fetched moments ago");
            rt.sleep(lotse_server::JWKS_REFETCH_EVERY).await;
            let carol = client(&rt, "c6", bearer(token(&rt, &rotated, "carol", 600)));
            carol.produce("s", 0, batch("from carol")).await.expect("after rotation");
            stop(nodes).await;
        });
    });
}

#[test]
fn a_client_certificate_names_its_principal() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let svc = Client::new(
                rt.node("svc").with_identity(Some(PeerIdentity::Principal("billing".into()))),
                ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect()),
            );
            svc.produce("s", 0, batch("from billing")).await.expect("a certificate is enough");
            stop(nodes).await;
        });
    });
}

#[test]
fn a_certificate_counts_only_when_mtls_is_on() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster_with(&rt, AuthConfig { mtls: false, ..auth() }).await;
            let svc = Client::new(
                rt.node("svc").with_identity(Some(PeerIdentity::Principal("billing".into()))),
                ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect()),
            );
            assert!(denied(svc.produce("s", 0, batch("x")).await), "the certificate is ignored");
            stop(nodes).await;
        });
    });
}

#[test]
fn clients_renew_their_sessions() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            // Each authentication gets a token valid for 4 s: shorter than
            // the node's maximum session, so the token's expiry decides.
            let tokens = rt.clone();
            let issued = Arc::new(Mutex::new(0u32));
            let count = issued.clone();
            let fresh = Credentials::OAuthBearer(Arc::new(move || {
                *count.lock().unwrap() += 1;
                token(&tokens, &issuer("k1"), "bob", 4)
            }));
            let bob = client(&rt, "c2", Some(fresh));
            let start = rt.now();
            let mut n = 0;
            while rt.now() - start < MAX_SESSION * 4 {
                admin.produce("s", 0, batch(&format!("a{n}"))).await.expect("admin's session is renewed");
                bob.produce("s", 0, batch(&format!("b{n}"))).await.expect("bob's token is renewed");
                n += 1;
                rt.sleep(Duration::from_millis(300)).await;
            }
            assert!(*issued.lock().unwrap() >= 5, "bob authenticated again and again");
            stop(nodes).await;
        });
    });
}

/// A connection to node 1's client port.
async fn raw(rt: &SimRuntime, host: &str) -> lotse_sim::SimConn {
    rt.node(host).connect("n1:9000", Link::Client).await.unwrap()
}

/// The next frame, or `None` if the connection ended or nothing came for a
/// while: a broken rule must fail the test, not hang it.
async fn next(rt: &SimRuntime, conn: &lotse_sim::SimConn) -> Option<Bytes> {
    let mut recv = std::pin::pin!(conn.recv());
    let mut wait = std::pin::pin!(rt.sleep(Duration::from_secs(5)));
    std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(r) = recv.as_mut().poll(cx) {
            return std::task::Poll::Ready(r.ok().flatten());
        }
        wait.as_mut().poll(cx).map(|()| None)
    })
    .await
}

async fn call(rt: &SimRuntime, conn: &lotse_sim::SimConn, id: u32, req: &Request) -> Response {
    conn.send(lotse_proto::encode_request(id, req)).await.unwrap();
    let frame = next(rt, conn).await.expect("an answer");
    lotse_proto::decode_response(frame, req.api_key()).unwrap().1
}

/// Logs `user` in with SCRAM on `conn`; the session's lifetime in ms.
async fn login(rt: &SimRuntime, conn: &lotse_sim::SimConn, user: &str, password: &str) -> Result<u64, ErrorCode> {
    let hs = Request::SaslHandshake(SaslHandshakeRequest { mechanism: "SCRAM-SHA-512".into() });
    assert!(matches!(call(rt, conn, 10, &hs).await, Response::SaslHandshake(r) if r.error == ErrorCode::NONE));
    let mut c = ScramClient::new(user, password, "rawclientnonce");
    let step = |b: Vec<u8>| Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: Bytes::from(b) });
    let Response::SaslAuthenticate(first) = call(rt, conn, 11, &step(c.first())).await else { panic!("not an answer") };
    if first.error != ErrorCode::NONE {
        return Err(first.error);
    }
    let Response::SaslAuthenticate(last) = call(rt, conn, 12, &step(c.respond(&first.auth_bytes).unwrap())).await
    else {
        panic!("not an answer")
    };
    if last.error != ErrorCode::NONE {
        return Err(last.error);
    }
    c.verify(&last.auth_bytes).expect("the node knows the credential");
    Ok(last.session_lifetime_ms)
}

#[test]
fn sessions_end_and_keep_their_principal() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.set_user_password("eve", "eve-pw").await.unwrap();
            rt.sleep(TICK * 50).await;
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });

            // A session ends after the node's maximum.
            let conn = raw(&rt, "r1").await;
            assert_eq!(login(&rt, &conn, "admin", "admin-pw").await, Ok(MAX_SESSION.as_millis() as u64));
            assert!(matches!(call(&rt, &conn, 1, &meta).await, Response::Metadata(_)));
            rt.sleep(MAX_SESSION + TICK).await;
            assert_eq!(call(&rt, &conn, 2, &meta).await, Response::Error(ErrorCode::UNAUTHORIZED), "ended");
            // Authenticating again opens it anew.
            login(&rt, &conn, "admin", "admin-pw").await.expect("again");
            assert!(matches!(call(&rt, &conn, 3, &meta).await, Response::Metadata(_)));

            // But not as someone else: the attempt fails and the connection closes.
            assert_eq!(login(&rt, &conn, "eve", "eve-pw").await, Err(ErrorCode::AUTHENTICATION_FAILED));
            assert!(conn_closed(&rt, &conn).await, "closed");

            // A failed login closes the connection too.
            let conn = raw(&rt, "r2").await;
            assert_eq!(login(&rt, &conn, "admin", "nope").await, Err(ErrorCode::AUTHENTICATION_FAILED));
            assert!(conn_closed(&rt, &conn).await, "closed");

            // A token's session ends with the token, if that comes first.
            let conn = raw(&rt, "r4").await;
            let hs = Request::SaslHandshake(SaslHandshakeRequest { mechanism: "OAUTHBEARER".into() });
            assert!(matches!(call(&rt, &conn, 1, &hs).await, Response::SaslHandshake(r) if r.error == ErrorCode::NONE));
            let t = token(&rt, &issuer("k1"), "bob", 2);
            let first = Bytes::from(lotse_auth::oauth::initial_response(&t));
            match call(&rt, &conn, 2, &Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: first })).await {
                Response::SaslAuthenticate(r) => {
                    assert_eq!(r.error, ErrorCode::NONE, "{r:?}");
                    assert!(r.session_lifetime_ms > 0 && r.session_lifetime_ms <= 2_000, "{r:?}");
                }
                other => panic!("{other:?}"),
            }
            rt.sleep(Duration::from_millis(2_100)).await;
            assert_eq!(call(&rt, &conn, 3, &meta).await, Response::Error(ErrorCode::UNAUTHORIZED), "the token ran out");

            // A mechanism that is not enabled is named as such.
            let conn = raw(&rt, "r3").await;
            let hs = Request::SaslHandshake(SaslHandshakeRequest { mechanism: "PLAIN".into() });
            match call(&rt, &conn, 1, &hs).await {
                Response::SaslHandshake(r) => {
                    assert_eq!(r.error, ErrorCode::UNSUPPORTED_SASL_MECHANISM);
                    assert_eq!(r.mechanisms, vec!["SCRAM-SHA-512".to_string(), "OAUTHBEARER".to_string()]);
                }
                other => panic!("{other:?}"),
            }
            stop(nodes).await;
        });
    });
}

#[test]
fn nodes_authenticate_to_each_other_by_certificate() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            // Committing writes markers through other nodes' client ports.
            let txn = Client::new(
                rt.node("t1"),
                ClientConfig {
                    retry_for: Duration::from_secs(10),
                    transactional_id: Some("t".into()),
                    credentials: scram("admin", "admin-pw"),
                    ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                },
            );
            let mut t = txn.begin_transaction().await.expect("begins");
            t.produce("s", 0, batch("in a transaction")).await.expect("writes");
            t.commit().await.expect("commits, markers and all");
            let reader = client(&rt, "r1", scram("admin", "admin-pw"));
            let deadline = rt.now() + Duration::from_secs(10);
            loop {
                let f = reader.fetch_uncommitted("s", 0, 0, TICK * 10).await.expect("reads");
                if f.last_stable_offset >= f.high_watermark && f.high_watermark > 0 {
                    break;
                }
                assert!(rt.now() < deadline, "the transaction never became stable");
                rt.sleep(TICK * 10).await;
            }
            stop(nodes).await;
        });
    });
}

#[test]
fn authentication_needs_a_network_that_verifies_peers() {
    Sim::new(1).run(|rt| async move {
        assert!(!rt.verifies_peers());
        let Err(e) = Node::start(rt.node("n1"), config(1, auth()), |_| Nothing).await else { panic!("started") };
        assert!(e.to_string().contains("authentication needs TLS"), "{e}");
    });
}

#[test]
fn the_simulation_starts_on_a_known_day() {
    Sim::new(1).run(|rt| async move {
        assert_eq!(rt.unix_time(), SIM_EPOCH);
    });
}

/// Whether the node closed `conn` (within a few seconds).
async fn conn_closed(rt: &SimRuntime, conn: &lotse_sim::SimConn) -> bool {
    let mut recv = std::pin::pin!(conn.recv());
    let mut wait = std::pin::pin!(rt.sleep(Duration::from_secs(5)));
    std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(r) = recv.as_mut().poll(cx) {
            return std::task::Poll::Ready(!matches!(r, Ok(Some(_))));
        }
        wait.as_mut().poll(cx).map(|()| false)
    })
    .await
}

#[test]
fn revoked_sessions_end_and_old_tokens_are_refused() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.set_user_password("alice", "alice-pw").await.unwrap();
            rt.sleep(TICK * 50).await;
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });

            // A SCRAM session, waiting for nothing, ends when its user is
            // revoked; another user's goes on.
            let alice = raw(&rt, "r1").await;
            login(&rt, &alice, "alice", "alice-pw").await.unwrap();
            assert!(matches!(call(&rt, &alice, 1, &meta).await, Response::Metadata(_)));
            let other = raw(&rt, "r2").await;
            login(&rt, &other, "admin", "admin-pw").await.unwrap();
            rt.sleep(TICK * 10).await;
            admin.revoke_sessions("alice", false).await.unwrap();
            assert!(conn_closed(&rt, &alice).await, "the waiting connection ends");
            assert!(matches!(call(&rt, &other, 2, &meta).await, Response::Metadata(_)), "admin's goes on");
            // With the password she signs in again: a new session.
            rt.sleep(TICK * 10).await;
            let again = raw(&rt, "r3").await;
            login(&rt, &again, "alice", "alice-pw").await.expect("a session after the revocation");
            assert!(matches!(call(&rt, &again, 3, &meta).await, Response::Metadata(_)));

            // A group's revocation covers its members' tokens issued before.
            let idp = issuer("k1");
            let now = rt.unix_time().as_secs();
            let signed = |iat: u64| {
                idp.sign(&serde_json::json!({
                    "iss": ISSUER, "aud": AUDIENCE, "sub": "bob", "groups": ["ops"], "exp": now + 60, "iat": iat,
                }))
            };
            let old = signed(now - 30);
            let bob = client(&rt, "c2", bearer(old.clone()));
            bob.produce("s", 0, batch("before")).await.expect("bob's token works");
            admin.revoke_sessions("ops", true).await.unwrap();
            rt.sleep(TICK * 50).await;
            assert!(denied(bob.produce("s", 0, batch("after")).await), "the old token is refused");
            let mut no_iat = serde_json::json!({"iss": ISSUER, "aud": AUDIENCE, "sub": "bob", "groups": ["ops"]});
            no_iat["exp"] = (now + 60).into();
            let carol = client(&rt, "c3", bearer(idp.sign(&no_iat)));
            assert!(denied(carol.produce("s", 0, batch("x")).await), "a token without iat counts as old");
            rt.sleep(Duration::from_secs(2)).await;
            let fresh = signed(rt.unix_time().as_secs());
            let bob = client(&rt, "c4", bearer(fresh));
            bob.produce("s", 0, batch("fresh")).await.expect("a token issued after the revocation");
            stop(nodes).await;
        });
    });
}

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
use lotse_proto::{ErrorCode, Request, Response, SaslAuthenticateRequest, SaslHandshakeRequest, certificate_state};
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::{CertInfo, Clock, Conn, Link, Net, PeerIdentity};
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

/// An Ed25519 key from a fixed seed: ring signs Ed25519 deterministically,
/// so certificates come out the same in every run of a seed.
fn fixed_key(seed: u8) -> rcgen::KeyPair {
    let mut der = vec![0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20];
    der.extend([seed; 32]);
    rcgen::KeyPair::try_from(&der[..]).unwrap()
}

/// A CA named like the cluster's, with its key from `seed`: the cluster's
/// own is seed 1.
fn test_ca(seed: u8) -> (rcgen::Issuer<'static, rcgen::KeyPair>, Vec<u8>) {
    let mut params = rcgen::CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    params.distinguished_name.push(rcgen::DnType::CommonName, "Lotse cluster CA");
    params.serial_number = Some(rcgen::SerialNumber::from(1u64));
    let key = fixed_key(seed);
    let der = params.self_signed(&key).unwrap().der().to_vec();
    (rcgen::Issuer::new(params, key), der)
}

/// A signing request for `principal` with the key from `seed`.
fn test_csr(seed: u8, principal: &str) -> Vec<u8> {
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, principal);
    params.serialize_request(&fixed_key(seed)).unwrap().der().to_vec()
}

/// A client certificate for `principal` with the key from `key_seed`, as
/// `lotse certs --sign` makes one, signed by the CA from `ca_seed`.
fn test_cert(ca_seed: u8, key_seed: u8, principal: &str, serial: u64) -> Vec<u8> {
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, principal);
    params.serial_number = Some(rcgen::SerialNumber::from(serial));
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    params.use_authority_key_identifier_extension = true;
    params.signed_by(&fixed_key(key_seed), &test_ca(ca_seed).0).unwrap().der().to_vec()
}

/// The issuer as ADR 0096 names it, worked out here: SHA-256 over the
/// issuer's name and the authority key identifier.
fn issuer_of(cert: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    use x509_parser::prelude::{FromDer, ParsedExtension, X509Certificate};
    let (_, c) = X509Certificate::from_der(cert).unwrap();
    let aki = c
        .extensions()
        .iter()
        .find_map(|e| match e.parsed_extension() {
            ParsedExtension::AuthorityKeyIdentifier(a) => a.key_identifier.as_ref().map(|k| k.0.to_vec()),
            _ => None,
        })
        .expect("lotse certs writes the authority key identifier");
    let mut h = sha2::Sha256::new();
    h.update(c.issuer().as_raw());
    h.update(&aki);
    h.finalize().to_vec()
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
        client_cas: lotse_server::ClientCas::new(vec![test_ca(1).1]),
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
        zones: Default::default(),
        streams: vec![StreamConfig { name: "s".into(), resource_id: 7, partitions: vec![3] }],
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
        versions: Default::default(),
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
    cluster_on(rt, auth, lotse_server::version::Versions::default()).await
}

/// The cluster with every node reporting `versions` (ADR 0127).
async fn cluster_on(
    rt: &SimRuntime,
    auth: AuthConfig,
    versions: lotse_server::version::Versions,
) -> Vec<Node<SimRuntime>> {
    rt.net().verify_peers(true);
    let idp = issuer("k1");
    rt.http().serve(&lotse_auth::oauth::discovery_url(ISSUER), idp.discovery(JWKS));
    rt.http().serve(JWKS, idp.jwks());
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let host = rt.node(&format!("n{id}")).with_identity(Some(PeerIdentity::Node(id)));
        let config = NodeConfig { versions: versions.clone(), ..config(id, auth.clone()) };
        nodes.push(Node::start(host, config, |_| Nothing).await.unwrap());
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
                    assert_eq!(
                        r.mechanisms,
                        ["SCRAM-SHA-512", "OAUTHBEARER", "LOTSE-TOKEN"].map(String::from).to_vec()
                    );
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

/// A connection to node 1's client port showing a certificate for `name`.
async fn raw_with_cert(rt: &SimRuntime, host: &str, name: &str, cert: &CertInfo) -> lotse_sim::SimConn {
    let rt = rt.node(host).with_identity(Some(PeerIdentity::Principal(name.into()))).with_cert(cert.clone());
    rt.connect("n1:9000", Link::Client).await.unwrap()
}

#[test]
fn a_revoked_certificate_ends_its_connections_and_is_refused_at_login() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let ends = u64::try_from(rt.unix_time().as_millis()).unwrap() + 86_400_000;
            let cert = |issuer: u8, serial: u8| CertInfo {
                issuer: [issuer; 32],
                serial: vec![serial],
                not_before_ms: 0,
                not_after_ms: ends,
            };
            let (lost, other, elsewhere) = (cert(1, 1), cert(1, 2), cert(2, 1));
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });

            // Two certificates of the same principal, and one of another issuer
            // with the same serial, all work.
            let a = raw_with_cert(&rt, "r1", "billing", &lost).await;
            let b = raw_with_cert(&rt, "r2", "billing", &other).await;
            let c = raw_with_cert(&rt, "r3", "billing", &elsewhere).await;
            for (i, conn) in [&a, &b, &c].into_iter().enumerate() {
                assert!(matches!(call(&rt, conn, i as u32, &meta).await, Response::Metadata(_)), "before");
            }
            rt.sleep(TICK * 10).await;

            // Only a cluster admin revokes; a certificate does not make one.
            let billing = Client::new(
                rt.node("svc").with_identity(Some(PeerIdentity::Principal("billing".into()))).with_cert(other.clone()),
                ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect()),
            );
            let refused = billing.revoke_certificate(lost.issuer, &lost.serial, ends, "no").await;
            assert!(matches!(refused, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. })), "{refused:?}");
            assert!(matches!(call(&rt, &a, 10, &meta).await, Response::Metadata(_)), "nothing was revoked");
            // A certificate that ended already needs no revocation.
            let past = admin.revoke_certificate(lost.issuer, &lost.serial, 1, "late").await;
            assert!(matches!(past, Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. })), "{past:?}");

            admin.revoke_certificate(lost.issuer, &lost.serial, ends, "laptop lost").await.unwrap();
            assert!(conn_closed(&rt, &a).await, "the connection that showed it ends");
            assert!(matches!(call(&rt, &b, 11, &meta).await, Response::Metadata(_)), "another serial goes on");
            assert!(matches!(call(&rt, &c, 12, &meta).await, Response::Metadata(_)), "another issuer goes on");

            // It is refused at login from then on.
            rt.sleep(TICK * 10).await;
            let again = raw_with_cert(&rt, "r4", "billing", &lost).await;
            assert!(conn_closed(&rt, &again).await, "a new connection with it ends");
            let fresh = raw_with_cert(&rt, "r5", "billing", &other).await;
            assert!(matches!(call(&rt, &fresh, 13, &meta).await, Response::Metadata(_)));
            stop(nodes).await;
        });
    });
}

#[test]
fn the_certificate_inventory_lists_what_nodes_accepted() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let day = 86_400_000u64;
            let now = u64::try_from(rt.unix_time().as_millis()).unwrap();
            let cert = |serial: u8, not_after_ms: u64| CertInfo {
                issuer: [1; 32],
                serial: vec![serial],
                not_before_ms: now - 400 * day,
                not_after_ms,
            };
            let (long, soon, over) = (cert(1, now + 100 * day), cert(2, now + 10 * day), cert(3, now - day));
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });

            // Each connection shows its certificate and works.
            let conns = [
                raw_with_cert(&rt, "r1", "billing", &long).await,
                raw_with_cert(&rt, "r2", "reports", &soon).await,
                raw_with_cert(&rt, "r3", "old", &over).await,
            ];
            for (i, conn) in conns.iter().enumerate() {
                assert!(matches!(call(&rt, conn, i as u32, &meta).await, Response::Metadata(_)));
            }
            rt.sleep(TICK * 100).await;
            let listed = admin.describe_certificates().await.unwrap();
            assert_eq!(listed.certificates.len(), 3, "{:?}", listed.certificates);
            let view: Vec<(&str, u8)> = listed.certificates.iter().map(|c| (c.principal.as_str(), c.state)).collect();
            assert_eq!(
                view,
                [
                    ("old", certificate_state::EXPIRED),
                    ("reports", certificate_state::EXPIRING),
                    ("billing", certificate_state::VALID),
                ],
                "by expiry, with how each stands"
            );
            let first = listed.certificates[2].first_seen_ms;
            assert!(first > 0 && listed.certificates[2].not_after_ms == now + 100 * day);

            // The same certificate on new connections is still one entry,
            // first seen when it was.
            rt.sleep(TICK * 100).await;
            for i in 0..5 {
                let again = raw_with_cert(&rt, &format!("q{i}"), "billing", &long).await;
                assert!(matches!(call(&rt, &again, 20, &meta).await, Response::Metadata(_)));
            }
            rt.sleep(TICK * 100).await;
            let listed = admin.describe_certificates().await.unwrap();
            assert_eq!(listed.certificates.len(), 3);
            assert_eq!(listed.certificates[2].first_seen_ms, first, "first seen stays");

            // A revocation shows with its reason, and a certificate revoked
            // before anyone saw it is listed too.
            admin.revoke_certificate(long.issuer, &long.serial, long.not_after_ms, "laptop lost").await.unwrap();
            let unseen = cert(9, now + 50 * day);
            admin.revoke_certificate(unseen.issuer, &unseen.serial, unseen.not_after_ms, "never used").await.unwrap();
            let listed = admin.describe_certificates().await.unwrap();
            let lost = listed.certificates.iter().find(|c| c.serial[..] == [1]).expect("still listed");
            assert_eq!(
                (lost.state, lost.reason.as_str(), lost.principal.as_str()),
                (certificate_state::REVOKED, "laptop lost", "billing")
            );
            let never = listed.certificates.iter().find(|c| c.serial[..] == [9]).expect("listed unseen");
            assert_eq!(
                (never.state, never.principal.as_str(), never.first_seen_ms),
                (certificate_state::REVOKED, "", 0)
            );

            // Only a cluster admin lists them, and only nodes report them.
            let billing = Client::new(
                rt.node("svc").with_identity(Some(PeerIdentity::Principal("reports".into()))).with_cert(soon.clone()),
                ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect()),
            );
            let refused = billing.describe_certificates().await;
            assert!(matches!(refused, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. })), "{refused:?}");
            let report = Request::RecordCertificates(lotse_proto::RecordCertificatesRequest {
                certificates: vec![lotse_proto::SeenCertificate {
                    issuer: Bytes::from_static(&[1; 32]),
                    serial: Bytes::from_static(&[0x42]),
                    principal: "mallory".into(),
                    not_before_ms: 0,
                    not_after_ms: u64::MAX,
                    seen_ms: now,
                }],
            });
            let conn = raw_with_cert(&rt, "r9", "reports", &soon).await;
            assert!(matches!(call(&rt, &conn, 30, &report).await, Response::Refused { .. }), "not from a client");
            stop(nodes).await;
        });
    });
}

#[test]
fn a_certificate_request_waits_for_the_ca_holder_and_is_answered_once() {
    use lotse_proto::certificate_request_state as state;
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let csr = test_csr(2, "alice");
            let invalid = |r: Result<(), ClientError>| {
                assert!(matches!(r, Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, .. })), "{r:?}")
            };

            // Names the CA holder could not sign, and empty requests, are refused.
            for bad in ["", ".hidden", "a b", "../x"] {
                invalid(admin.request_certificate(bad, csr.clone()).await);
            }
            invalid(admin.request_certificate("alice", Vec::new()).await);
            invalid(admin.request_certificate("alice", vec![0; 5000]).await);

            // The same request twice is one request.
            admin.request_certificate("alice", csr.clone()).await.unwrap();
            admin.request_certificate("alice", csr.clone()).await.unwrap();
            let listed = admin.describe_certificate_requests().await.unwrap();
            assert_eq!(listed.requests.len(), 1, "{:?}", listed.requests);
            let one = &listed.requests[0];
            assert_eq!(
                (one.principal.as_str(), one.requested_by.as_str(), one.state),
                ("alice", "admin", state::PENDING)
            );
            assert_eq!(one.csr[..], csr[..]);
            assert!(one.certificate.is_empty() && one.signed_at_ms == 0);
            assert_eq!(one.expires_at_ms - one.requested_at_ms, 7 * 86_400_000, "seven days");

            // The state holds a bounded number of requests.
            for i in 1..256u32 {
                admin.request_certificate("bulk", i.to_le_bytes().to_vec()).await.unwrap();
            }
            invalid(admin.request_certificate("bulk", vec![0xff; 8]).await);
            // A user in their own name finds no place either (ADR 0119).
            let alice = cert_client(&rt, "alice");
            invalid(alice.request_certificate("alice", test_csr(7, "alice")).await);

            // Only a cluster admin asks for others, sees every request or
            // records; a user sees what they asked for (ADR 0119).
            let unauthorized = |r: Result<(), ClientError>| {
                assert!(matches!(r, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. })), "{r:?}")
            };
            unauthorized(alice.request_certificate("mallory", vec![1, 2, 3]).await);
            assert!(alice.describe_certificate_requests().await.unwrap().requests.is_empty());
            let cert = test_cert(1, 2, "alice", 0x42);
            let (_, parsed) =
                <x509_parser::prelude::X509Certificate as x509_parser::prelude::FromDer<_>>::from_der(&cert).unwrap();
            let answer = lotse_proto::RecordCertificateRequest {
                id: one.id.clone(),
                certificate: cert.clone().into(),
                issuer: issuer_of(&cert).into(),
                serial: Bytes::from_static(&[0x42]),
                not_before_ms: parsed.validity().not_before.timestamp() as u64 * 1000,
                not_after_ms: parsed.validity().not_after.timestamp() as u64 * 1000,
            };
            unauthorized(alice.record_certificate(answer.clone()).await);

            // The certificate must answer the request and come from the
            // cluster's CA (ADR 0096), and the record must say what it says.
            let refused = |certificate: Vec<u8>| lotse_proto::RecordCertificateRequest {
                issuer: issuer_of(&certificate).into(),
                certificate: certificate.into(),
                ..answer.clone()
            };
            invalid(admin.record_certificate(refused(test_cert(1, 3, "alice", 0x42))).await); // another key
            invalid(admin.record_certificate(refused(test_cert(1, 2, "mallory", 0x42))).await); // another principal
            invalid(admin.record_certificate(refused(test_cert(4, 2, "alice", 0x42))).await); // another CA, same name
            let mut junk = answer.clone();
            junk.certificate = Bytes::from_static(&[0x30, 0x00]);
            invalid(admin.record_certificate(junk).await);
            let mut other_issuer = answer.clone();
            other_issuer.issuer = Bytes::from_static(&[1; 32]);
            invalid(admin.record_certificate(other_issuer).await);
            let mut other_serial = answer.clone();
            other_serial.serial = Bytes::from_static(&[0x43]);
            invalid(admin.record_certificate(other_serial).await);
            let mut other_end = answer.clone();
            other_end.not_after_ms += 1000;
            invalid(admin.record_certificate(other_end).await);

            // An answer needs an open request and a sound certificate record.
            let mut unknown = answer.clone();
            unknown.id = Bytes::from_static(&[9; 32]);
            invalid(admin.record_certificate(unknown).await);
            let mut short = answer.clone();
            short.issuer = Bytes::from_static(&[1; 31]);
            invalid(admin.record_certificate(short).await);
            let mut backwards = answer.clone();
            backwards.not_after_ms = backwards.not_before_ms;
            invalid(admin.record_certificate(backwards).await);

            admin.record_certificate(answer.clone()).await.unwrap();
            let listed = admin.describe_certificate_requests().await.unwrap();
            let one = listed.requests.iter().find(|r| r.id == answer.id).expect("still listed");
            assert_eq!(one.state, state::SIGNED);
            assert_eq!(one.certificate[..], cert[..]);
            assert!(one.signed_at_ms > 0);
            // A second answer finds the request closed.
            invalid(admin.record_certificate(answer).await);
            // The certificate is in the inventory, which no node has seen yet.
            let inventory = admin.describe_certificates().await.unwrap();
            let entry = inventory.certificates.iter().find(|c| c.serial[..] == [0x42]).expect("in the inventory");
            assert_eq!((entry.principal.as_str(), entry.first_seen_ms), ("alice", 0));
            assert_eq!(entry.issuer[..], issuer_of(&cert)[..], "the issuer by name and key identifier");
            stop(nodes).await;
        });
    });
}

/// A client that signs in with a client certificate naming `name`.
fn cert_client(rt: &SimRuntime, name: &str) -> Client<SimRuntime> {
    Client::new(
        rt.node(&format!("svc-{name}")).with_identity(Some(PeerIdentity::Principal(name.into()))),
        ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect()),
    )
}

/// ADR 0119: any user asks for a certificate in their own name, has one
/// pending request at a time, sees only what they asked for, and users'
/// own requests leave room for cluster admins.
#[test]
fn a_user_asks_for_a_certificate_in_their_own_name() {
    use lotse_proto::certificate_request_state as state;
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let alice = cert_client(&rt, "alice");
            let code = |r: Result<(), ClientError>| match r {
                Ok(()) => ErrorCode::NONE,
                Err(ClientError::Broker { code, .. }) => code,
                Err(e) => panic!("{e:?}"),
            };
            let ids = |listed: lotse_proto::DescribeCertificateRequestsResponse| {
                let mut v: Vec<(String, String, Vec<u8>)> =
                    listed.requests.into_iter().map(|r| (r.principal, r.requested_by, r.csr.to_vec())).collect();
                v.sort();
                v
            };

            // Alice asks for herself; for someone else she may not.
            let first = test_csr(2, "alice");
            alice.request_certificate("alice", first.clone()).await.unwrap();
            assert_eq!(code(alice.request_certificate("bob", test_csr(3, "bob")).await), ErrorCode::UNAUTHORIZED);
            // An admin asks in her name too: she does not see that one.
            let by_admin = test_csr(4, "alice");
            admin.request_certificate("alice", by_admin.clone()).await.unwrap();
            let mine = alice.describe_certificate_requests().await.unwrap();
            assert_eq!(mine.requests.len(), 1);
            assert_eq!(mine.requests[0].state, state::PENDING);
            assert_eq!(ids(mine), vec![("alice".into(), "alice".into(), first.clone())]);

            // Asking again, say after losing the key, replaces her pending
            // request; the admin's stays.
            let second = test_csr(5, "alice");
            alice.request_certificate("alice", second.clone()).await.unwrap();
            assert_eq!(
                ids(alice.describe_certificate_requests().await.unwrap()),
                vec![("alice".into(), "alice".into(), second.clone())]
            );
            assert_eq!(
                ids(admin.describe_certificate_requests().await.unwrap()),
                vec![
                    ("alice".into(), "admin".into(), by_admin.clone()),
                    ("alice".into(), "alice".into(), second.clone()),
                ]
            );

            // A name the CA holder could not sign is refused with a reason.
            let idp = issuer("k1");
            let odd = client(&rt, "c2", bearer(token(&rt, &idp, "oidc|bob", 3600)));
            assert_eq!(code(odd.request_certificate("oidc|bob", vec![1; 8]).await), ErrorCode::INVALID_REQUEST);

            // Users' own requests stop at 192 of the 256 places: Alice holds
            // one, 191 others fill the rest.
            for i in 0..191u32 {
                let user = cert_client(&rt, &format!("u{i}"));
                user.request_certificate(&format!("u{i}"), i.to_le_bytes().to_vec()).await.unwrap();
            }
            let late = cert_client(&rt, "late");
            assert_eq!(code(late.request_certificate("late", vec![7; 8]).await), ErrorCode::INVALID_REQUEST);
            // Alice may still replace hers: it gives its place to the new one.
            let third = test_csr(6, "alice");
            alice.request_certificate("alice", third.clone()).await.unwrap();
            let all = admin.describe_certificate_requests().await.unwrap();
            assert_eq!(all.requests.iter().filter(|r| r.requested_by == r.principal).count(), 192);
            assert!(all.requests.iter().any(|r| r.csr[..] == third[..]));
            assert!(!all.requests.iter().any(|r| r.csr[..] == second[..]));
            // Admins keep room for the rest.
            admin.request_certificate("svc", vec![9; 8]).await.unwrap();

            // Every node tells the operator how many wait for the CA holder:
            // the 192 users' and the admin's two, once each node applied them.
            for node in &nodes {
                let h = node.handle();
                let since = rt.now();
                loop {
                    let obs = h.observe(lotse_server::ops::Scraper::Local).await.unwrap();
                    if obs.certificate_requests_pending == Some(194) {
                        break;
                    }
                    assert!(rt.now() - since < Duration::from_secs(10), "{:?}", obs.certificate_requests_pending);
                    rt.sleep(Duration::from_millis(100)).await;
                }
            }
            stop(nodes).await;
        });
    });
}

/// An idempotent producer with a name (ADR 0071): a new instance fences
/// the old one, and the old one stays fenced instead of fencing back.
#[test]
fn a_named_producer_fences_its_earlier_instance() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.set_user_password("alice", "alice-pw").await.unwrap();
            rt.sleep(TICK * 50).await;
            let named = |host: &str| {
                Client::new(
                    rt.node(host),
                    ClientConfig {
                        retry_for: Duration::from_secs(10),
                        credentials: scram("alice", "alice-pw"),
                        producer_name: Some("orders-writer".into()),
                        ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                    },
                )
            };
            let fenced = |r: Result<u64, ClientError>| {
                matches!(r, Err(ClientError::Broker { code: ErrorCode::PRODUCER_FENCED, .. }))
            };
            let first = named("p1");
            first.produce("s", 0, batch("a")).await.expect("the first instance writes");
            let second = named("p2");
            second.produce("s", 0, batch("b")).await.expect("a restart under the name writes");
            assert!(fenced(first.produce("s", 0, batch("c")).await), "the earlier instance is fenced");
            assert!(fenced(first.produce("s", 0, batch("d")).await), "and stays fenced");
            second.produce("s", 0, batch("e")).await.expect("the newer instance was not fenced back");
            // Without a name, producers do not fence each other.
            let (x, y) = (client(&rt, "p3", scram("alice", "alice-pw")), client(&rt, "p4", scram("alice", "alice-pw")));
            x.produce("s", 0, batch("x")).await.unwrap();
            y.produce("s", 0, batch("y")).await.unwrap();
            x.produce("s", 0, batch("x2")).await.expect("plain idempotent producers each have their own id");
            stop(nodes).await;
        });
    });
}

/// Static membership through the client (ADR 0072): a second instance
/// with the same instance id takes the partitions, and the first stops
/// with `FENCED_INSTANCE_ID` rather than take them back.
#[test]
fn a_member_instance_takes_over_and_the_old_one_stays_out() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.set_user_password("alice", "alice-pw").await.unwrap();
            rt.sleep(TICK * 50).await;
            let cfg = || lotse_client::GroupConfig { instance_id: Some("worker-1".into()), ..Default::default() };
            let (c1, c2) =
                (client(&rt, "m1", scram("alice", "alice-pw")), client(&rt, "m2", scram("alice", "alice-pw")));
            let mut first = c1.join_group("g", &["s"], cfg()).await.unwrap();
            for _ in 0..50 {
                first.poll(TICK * 5).await.unwrap();
                if !first.assignment().is_empty() {
                    break;
                }
            }
            assert_eq!(first.assignment().len(), 1, "the one partition");
            let mut second = c2.join_group("g", &["s"], cfg()).await.unwrap();
            for _ in 0..50 {
                second.poll(TICK * 5).await.unwrap();
                if !second.assignment().is_empty() {
                    break;
                }
            }
            assert_eq!(second.assignment().len(), 1, "taken over at once");
            let fenced = |r: Result<Vec<lotse_client::GroupRecord>, ClientError>| {
                matches!(r, Err(ClientError::Broker { code: ErrorCode::FENCED_INSTANCE_ID, .. }))
            };
            let mut seen = false;
            for _ in 0..100 {
                if fenced(first.poll(TICK * 5).await) {
                    seen = true;
                    break;
                }
            }
            assert!(seen, "the first instance learns it was replaced");
            assert!(fenced(first.poll(TICK * 5).await), "and stays out");
            for _ in 0..50 {
                second.poll(TICK * 5).await.expect("the newer instance goes on");
            }
            assert_eq!(second.assignment().len(), 1, "nobody took it back");
            stop(nodes).await;
        });
    });
}

// ---------------------------------------------------------------------------
// Open sessions (ADR 0121).

/// Every session the nodes report to `c`, with the node that has it.
async fn sessions_seen(c: &Client<SimRuntime>) -> Vec<(u32, lotse_proto::SessionEntry)> {
    let answers = c.node_sessions().await.expect("the nodes");
    answers
        .into_iter()
        .filter_map(|(n, r)| r.ok().map(|r| (n, r)))
        .flat_map(|(n, r)| r.sessions.into_iter().map(move |s| (n, s)))
        .collect()
}

#[test]
fn open_sessions_are_listed_to_admins_and_to_their_own_user() {
    use lotse_proto::{session_mechanism, session_principal};
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = cluster(&rt).await;
            let idp = issuer("k1");
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let bob = client(&rt, "c2", bearer(token(&rt, &idp, "bob", 600)));
            let carol = client(&rt, "c3", bearer(token(&rt, &idp, "carol", 600)));
            bob.produce("s", 0, batch("from bob")).await.expect("bob writes");
            carol.metadata(&[]).await.expect("carol connects");

            // A cluster admin sees everyone: bob with his token's groups and
            // end, carol, and itself with SCRAM.
            let all = sessions_seen(&admin).await;
            let bobs: Vec<_> = all.iter().filter(|(_, s)| s.name == "bob").collect();
            assert!(!bobs.is_empty(), "{all:?}");
            for (_, s) in &bobs {
                assert_eq!((s.principal_kind, s.mechanism), (session_principal::USER, session_mechanism::OAUTHBEARER));
                assert_eq!(s.groups, vec!["ops".to_string()]);
                assert!(s.expires_ms > s.connected_ms && s.peer.starts_with("c2"), "{s:?}");
            }
            assert!(all.iter().any(|(_, s)| s.name == "carol"), "{all:?}");
            assert!(all.iter().any(|(_, s)| s.name == "admin" && s.mechanism == session_mechanism::SCRAM_SHA_512));

            // Anyone else sees only their own, on every node.
            let mine = sessions_seen(&bob).await;
            assert!(!mine.is_empty() && mine.iter().all(|(_, s)| s.name == "bob"), "{mine:?}");
            let mine = sessions_seen(&carol).await;
            assert!(!mine.is_empty() && mine.iter().all(|(_, s)| s.name == "carol"), "{mine:?}");

            // A connection that closes leaves the list.
            let conn = raw(&rt, "r9").await;
            login(&rt, &conn, "admin", "admin-pw").await.expect("logged in");
            let on_r9 =
                |all: &[(u32, lotse_proto::SessionEntry)]| all.iter().any(|(n, s)| *n == 1 && s.peer.starts_with("r9"));
            assert!(on_r9(&sessions_seen(&admin).await), "the raw connection is listed");
            drop(conn);
            let mut gone = false;
            // Well before the session would end by itself (MAX_SESSION).
            for _ in 0..20 {
                rt.sleep(TICK * 10).await;
                if !on_r9(&sessions_seen(&admin).await) {
                    gone = true;
                    break;
                }
            }
            assert!(gone, "a closed connection's session is no longer listed");
            stop(nodes).await;
        });
    });
}

/// The renewal CA (ADR 0123): an intermediate under the cluster's CA (seed
/// 1) with a P-256 key from a fixed seed.
fn renewal_ca() -> lotse_auth::renew::RenewalCa {
    let key = lotse_auth::renew::new_key(&[7; 32], "renewal").unwrap().unwrap();
    let pair = rcgen::KeyPair::from_pem(&key.key_pem).unwrap();
    let mut params = rcgen::CertificateParams::default();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    params.distinguished_name.push(rcgen::DnType::CommonName, "Lotse renewal CA");
    params.serial_number = Some(rcgen::SerialNumber::from(2u64));
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    params.use_authority_key_identifier_extension = true;
    let cert = params.signed_by(&pair, &test_ca(1).0).unwrap();
    lotse_auth::renew::RenewalCa::from_pem(&cert.pem(), &key.key_pem).unwrap()
}

/// A client showing `cert` for `name`.
fn client_with_cert(rt: &SimRuntime, host: &str, name: &str, cert: &CertInfo) -> Client<SimRuntime> {
    Client::new(
        rt.node(host).with_identity(Some(PeerIdentity::Principal(name.into()))).with_cert(cert.clone()),
        ClientConfig {
            retry_for: Duration::from_secs(10),
            ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
        },
    )
}

/// What a connection shows for a renewed certificate in PEM.
fn shown(chain_pem: &str) -> CertInfo {
    let der = x509_parser::pem::parse_x509_pem(chain_pem.as_bytes()).unwrap().1.contents;
    let f = lotse_auth::x509::facts(&der).unwrap();
    CertInfo { issuer: f.issuer, serial: f.serial, not_before_ms: f.not_before_ms, not_after_ms: f.not_after_ms }
}

#[test]
fn a_certificate_is_renewed_over_its_connection_and_works_only_while_recorded() {
    lotse_sim::check_few_seeds(5, |seed| {
        Sim::new(seed).run(|rt| async move {
            let ca = renewal_ca();
            let renewal_issuer = lotse_auth::renew::issued_by(ca.certificate())[0];
            let nodes =
                cluster_with(&rt, AuthConfig { renewal: lotse_server::RenewalSlot::new(Some(ca)), ..auth() }).await;
            let day = 86_400_000u64;
            let now = u64::try_from(rt.unix_time().as_millis()).unwrap();
            let cert = |serial: u8, not_before_ms: u64, not_after_ms: u64| CertInfo {
                issuer: [1; 32],
                serial: vec![serial],
                not_before_ms,
                not_after_ms,
            };
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });
            let invalid = |r: &Result<lotse_client::RenewedCertificate, ClientError>, why: &str| {
                assert!(
                    matches!(r, Err(ClientError::Broker { code: ErrorCode::INVALID_REQUEST, message: Some(m) }) if m.contains(why)),
                    "{r:?}"
                );
            };

            // Not yet in its window: a year long, three months in.
            let early = client_with_cert(&rt, "e", "alice", &cert(1, now - 90 * day, now + 275 * day));
            invalid(&early.renew_certificate().await, "two thirds");
            // No certificate at all: a SCRAM user has nothing to renew.
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            invalid(&admin.renew_certificate().await, "only a connection made with a client certificate");
            // A certificate connection that signed in with SCRAM as someone
            // else acts as them, and renews nothing in the certificate's name.
            let switched = Client::new(
                rt.node("w")
                    .with_identity(Some(PeerIdentity::Principal("alice".into())))
                    .with_cert(cert(3, now - 355 * day, now + 10 * day)),
                ClientConfig {
                    retry_for: Duration::from_secs(10),
                    credentials: scram("admin", "admin-pw"),
                    ..ClientConfig::new((1..=3).map(|i| format!("n{i}:9000")).collect())
                },
            );
            let got = switched.renew_certificate().await;
            assert!(matches!(&got, Err(ClientError::Broker { code: ErrorCode::UNAUTHORIZED, .. })), "{got:?}");

            // In its window: the last 10 days.
            let old = cert(2, now - 355 * day, now + 10 * day);
            let alice = client_with_cert(&rt, "a", "alice", &old);
            let first = alice.renew_certificate().await.expect("renewed");
            let renewed = shown(&first.chain_pem);
            assert_eq!(renewed.issuer, renewal_issuer);
            assert_eq!(first.not_after_ms, renewed.not_after_ms);
            assert_eq!(renewed.not_after_ms / 1000, (now + 90 * day) / 1000);
            assert_eq!(first.chain_pem.matches("BEGIN CERTIFICATE").count(), 2);
            assert!(first.key_pem.contains("BEGIN PRIVATE KEY"));

            // It signs in on every node once the record reached it.
            rt.sleep(TICK * 20).await;
            let mut conns = Vec::new();
            for (i, host) in ["r1", "r2", "r3"].into_iter().enumerate() {
                let rt2 = rt.node(host).with_identity(Some(PeerIdentity::Principal("alice".into()))).with_cert(renewed.clone());
                let conn = rt2.connect(&format!("n{}:9000", i + 1), Link::Client).await.unwrap();
                assert!(matches!(call(&rt, &conn, i as u32, &meta).await, Response::Metadata(_)), "node {}", i + 1);
                conns.push(conn);
            }
            // As anyone else, the same certificate does not.
            let rt2 = rt.node("r4").with_identity(Some(PeerIdentity::Principal("mallory".into()))).with_cert(renewed.clone());
            let other = rt2.connect("n1:9000", Link::Client).await.unwrap();
            assert!(conn_closed(&rt, &other).await, "a recorded certificate holds only for its principal");
            // One the renewal CA issued that the cluster never recorded does not.
            let unrecorded = CertInfo { serial: vec![9, 9, 9], ..renewed.clone() };
            let rt2 = rt.node("r5").with_identity(Some(PeerIdentity::Principal("alice".into()))).with_cert(unrecorded);
            let forged = rt2.connect("n2:9000", Link::Client).await.unwrap();
            assert!(conn_closed(&rt, &forged).await, "an unrecorded certificate from the renewal CA");

            // The inventory lists it.
            let listed = admin.describe_certificates().await.unwrap();
            assert!(listed.certificates.iter().any(|c| c.serial.as_ref() == renewed.serial.as_slice() && c.principal == "alice"));

            // Renewing the same certificate again replaces the first renewal:
            // its connections end; the second works.
            let second = alice.renew_certificate().await.expect("renewed again");
            let again = shown(&second.chain_pem);
            assert_ne!(again.serial, renewed.serial);
            for conn in &conns {
                assert!(conn_closed(&rt, conn).await, "the replaced renewal's connections end");
            }
            rt.sleep(TICK * 20).await;
            let rt2 = rt.node("r6").with_identity(Some(PeerIdentity::Principal("alice".into()))).with_cert(again.clone());
            let conn = rt2.connect("n3:9000", Link::Client).await.unwrap();
            assert!(matches!(call(&rt, &conn, 40, &meta).await, Response::Metadata(_)));
            stop(nodes).await;
        });
    });
}

// Access tokens (ADR 0133). The client's calls for them come later; until
// then the tests speak the protocol on raw connections.

const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// A user's raw connection to the metadata group's leader: it logs in with
/// SCRAM, moves to the next node when one does not lead, and connects anew
/// when its session ended (`MAX_SESSION`).
struct AsUser {
    host: String,
    user: String,
    password: String,
    conn: Option<lotse_sim::SimConn>,
    node: u32,
    next_id: u32,
}

impl AsUser {
    fn new(user: &str, password: &str) -> AsUser {
        AsUser {
            host: format!("raw-{user}"),
            user: user.into(),
            password: password.into(),
            conn: None,
            node: 1,
            next_id: 100,
        }
    }

    async fn call(&mut self, rt: &SimRuntime, req: &Request) -> Response {
        for _ in 0..300 {
            if self.conn.is_none() {
                let conn = rt.node(&self.host).connect(&format!("n{}:9000", self.node), Link::Client).await.unwrap();
                if login(rt, &conn, &self.user, &self.password).await.is_err() {
                    self.node = self.node % 3 + 1;
                    rt.sleep(TICK * 5).await;
                    continue;
                }
                self.conn = Some(conn);
            }
            self.next_id += 1;
            let conn = self.conn.as_ref().unwrap();
            // A connection the node closed (a revocation, say) is made anew.
            if conn.send(lotse_proto::encode_request(self.next_id, req)).await.is_err() {
                self.conn = None;
                continue;
            }
            let answer = next(rt, conn).await.map(|f| lotse_proto::decode_response(f, req.api_key()).unwrap().1);
            match answer {
                // The session ended or the connection with it.
                None | Some(Response::Error(ErrorCode::UNAUTHORIZED)) => self.conn = None,
                Some(r) if matches!(code(&r).0, ErrorCode::NOT_LEADER | ErrorCode::COORDINATOR_NOT_AVAILABLE) => {
                    self.conn = None;
                    self.node = self.node % 3 + 1;
                    rt.sleep(TICK * 5).await;
                }
                Some(r) => return r,
            }
        }
        panic!("no metadata leader answered {req:?}")
    }

    async fn create(
        &mut self,
        rt: &SimRuntime,
        owner: &str,
        name: &str,
        lifetime: u8,
    ) -> lotse_proto::CreateTokenResponse {
        self.create_with(rt, owner, name, lifetime, vec![7; 32]).await
    }

    async fn create_with(
        &mut self,
        rt: &SimRuntime,
        owner: &str,
        name: &str,
        lifetime: u8,
        secret_hash: Vec<u8>,
    ) -> lotse_proto::CreateTokenResponse {
        let req = lotse_proto::CreateTokenRequest {
            name: name.into(),
            owner: owner.into(),
            lifetime,
            secret_hash: Bytes::from(secret_hash),
        };
        match self.call(rt, &Request::CreateToken(req)).await {
            Response::CreateToken(r) => r,
            other => panic!("{other:?}"),
        }
    }

    async fn list(&mut self, rt: &SimRuntime, owner: Option<&str>) -> Vec<lotse_proto::TokenEntry> {
        let req = lotse_proto::DescribeTokensRequest { owner: owner.map(str::to_string) };
        match self.call(rt, &Request::DescribeTokens(req)).await {
            Response::DescribeTokens(r) if r.error == ErrorCode::NONE => r.tokens,
            other => panic!("{other:?}"),
        }
    }

    async fn revoke(&mut self, rt: &SimRuntime, id: u64) -> (ErrorCode, Option<String>) {
        code(&self.call(rt, &Request::RevokeToken(lotse_proto::RevokeTokenRequest { id })).await)
    }
}

/// The outcome of a token request's answer.
fn code(r: &Response) -> (ErrorCode, Option<String>) {
    match r {
        Response::CreateToken(c) => (c.error, c.error_msg.clone()),
        Response::DescribeTokens(d) => (d.error, None),
        Response::Admin(a) => (a.error, a.error_msg.clone()),
        Response::Refused { code, message } => (*code, Some(message.clone())),
        Response::Error(c) => (*c, None),
        other => panic!("not an answer to a token request: {other:?}"),
    }
}

/// Waits until the cluster has taken cluster version `v`.
async fn cluster_version_is(rt: &SimRuntime, admin: &Client<SimRuntime>, v: u32) {
    for _ in 0..600 {
        if admin.describe_upgrade().await.ok().and_then(|d| d.cluster_version) == Some(v) {
            return;
        }
        rt.sleep(TICK * 10).await;
    }
    panic!("the cluster never took version {}", lotse_server::version::display(v));
}

/// The cluster with SCRAM users alice, bob and mon (a cluster viewer), at
/// cluster version 0.1, which tokens need.
async fn token_cluster(rt: &SimRuntime) -> Vec<Node<SimRuntime>> {
    let nodes = cluster(rt).await;
    setup_token_users(rt).await;
    let admin = client(rt, "tv", scram("admin", "admin-pw"));
    cluster_version_is(rt, &admin, lotse_server::version::TOKENS_SINCE).await;
    nodes
}

async fn setup_token_users(rt: &SimRuntime) {
    let admin = client(rt, "tu", scram("admin", "admin-pw"));
    for user in ["alice", "bob", "mon"] {
        admin.set_user_password(user, &format!("{user}-pw")).await.expect("creates the user");
    }
    let viewer = lotse_proto::RoleBindingData {
        subject_kind: lotse_proto::subject_kind::USER,
        subject: "mon".into(),
        role: lotse_proto::role::CLUSTER_VIEWER,
        tenant: lotse_server::DEFAULT_TENANT.into(),
        prefix: String::new(),
    };
    admin.alter_role_bindings(vec![viewer], Vec::new()).await.expect("mon views the cluster");
    rt.sleep(TICK * 50).await;
}

fn names(tokens: &[lotse_proto::TokenEntry]) -> Vec<(String, String, String)> {
    let mut v: Vec<_> = tokens.iter().map(|t| (t.owner.clone(), t.name.clone(), t.created_by.clone())).collect();
    v.sort();
    v
}

fn triple(owner: &str, name: &str, by: &str) -> (String, String, String) {
    (owner.into(), name.into(), by.into())
}

#[test]
fn users_create_list_and_revoke_their_own_tokens() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut bob = AsUser::new("bob", "bob-pw");
            let mut mon = AsUser::new("mon", "mon-pw");
            let mut admin = AsUser::new("admin", "admin-pw");

            let laptop = alice.create(&rt, "alice", "laptop", 2).await;
            assert_eq!((laptop.error, laptop.error_msg.as_deref()), (ErrorCode::NONE, None));
            assert!(laptop.id > 0);
            // No owner named: the token is the caller's.
            let ci = alice.create(&rt, "", "ci", 1).await;
            assert_eq!(ci.error, ErrorCode::NONE, "{:?}", ci.error_msg);
            assert_ne!(ci.id, laptop.id);
            let bobs = bob.create(&rt, "bob", "bob-1", 3).await;
            assert_eq!(bobs.error, ErrorCode::NONE, "{:?}", bobs.error_msg);

            // Out of shape: a name of 1 to 64 bytes, a hash of 32.
            for (name, hash) in [("", 32), (&*"n".repeat(65), 32), ("short", 31), ("long", 33)] {
                let r = alice.create_with(&rt, "alice", name, 2, vec![1; hash]).await;
                assert_eq!(r.error, ErrorCode::INVALID_REQUEST, "{name:?} {hash}");
                assert!(r.error_msg.is_some() && r.id == 0);
            }
            assert_eq!(alice.create(&rt, "alice", &"n".repeat(64), 2).await.error, ErrorCode::NONE);

            // Each user sees their own; a cluster viewer and admin see all.
            let mine = alice.list(&rt, None).await;
            assert_eq!(
                names(&mine),
                vec![
                    triple("alice", "ci", "alice"),
                    triple("alice", "laptop", "alice"),
                    triple("alice", &"n".repeat(64), "alice")
                ]
            );
            let laptop_entry = mine.iter().find(|t| t.id == laptop.id).expect("listed by id");
            assert_eq!(laptop_entry.expires_at_ms, laptop.expires_at_ms);
            assert!(alice.list(&rt, Some("bob")).await.is_empty(), "alice cannot see bob's");
            assert_eq!(names(&bob.list(&rt, None).await), vec![triple("bob", "bob-1", "bob")]);
            assert_eq!(mon.list(&rt, None).await.len(), 4, "a cluster viewer sees every token");
            assert_eq!(admin.list(&rt, None).await.len(), 4, "a cluster admin sees every token");
            assert_eq!(names(&admin.list(&rt, Some("bob")).await), vec![triple("bob", "bob-1", "bob")]);

            // Someone else's token is unknown to alice and to a viewer.
            assert_eq!(alice.revoke(&rt, bobs.id).await.0, ErrorCode::UNKNOWN_RESOURCE);
            assert_eq!(mon.revoke(&rt, bobs.id).await.0, ErrorCode::UNKNOWN_RESOURCE);
            assert_eq!(bob.list(&rt, None).await.len(), 1, "bob's token stays");

            // Her own she revokes, once.
            assert_eq!(alice.revoke(&rt, laptop.id).await, (ErrorCode::NONE, None));
            assert!(!alice.list(&rt, None).await.iter().any(|t| t.id == laptop.id));
            assert_eq!(alice.revoke(&rt, laptop.id).await.0, ErrorCode::UNKNOWN_RESOURCE);

            // Every node tells the operator how many tokens live.
            for node in &nodes {
                let h = node.handle();
                let since = rt.now();
                loop {
                    let obs = h.observe(lotse_server::ops::Scraper::Local).await.unwrap();
                    if obs.tokens == Some(3) {
                        assert!(
                            lotse_server::ops::render(&obs)
                                .contains(&format!("lotse_tokens{{node=\"{}\"}} 3\n", obs.node_id))
                        );
                        break;
                    }
                    assert!(rt.now() - since < Duration::from_secs(10), "{:?}", obs.tokens);
                    rt.sleep(Duration::from_millis(100)).await;
                }
            }
            stop(nodes).await;
        });
    });
}

#[test]
fn a_cluster_admin_creates_a_token_for_another_principal_and_others_cannot() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut bob = AsUser::new("bob", "bob-pw");
            let mut mon = AsUser::new("mon", "mon-pw");
            let mut admin = AsUser::new("admin", "admin-pw");

            let made = admin.create(&rt, "alice", "for-alice", 2).await;
            assert_eq!(made.error, ErrorCode::NONE, "{:?}", made.error_msg);
            assert_eq!(names(&alice.list(&rt, None).await), vec![triple("alice", "for-alice", "admin")]);

            // Anyone else creates only their own: a user, and a viewer too.
            for (who, name) in [(&mut alice, "alice"), (&mut mon, "mon")] {
                let r = who.create(&rt, "bob", "for-bob", 2).await;
                assert_eq!(r.error, ErrorCode::UNAUTHORIZED, "{name} made a token for bob");
                assert!(r.error_msg.is_some_and(|m| m.contains("cluster admin")));
            }
            assert!(bob.list(&rt, None).await.is_empty(), "nothing was written for bob");

            // A cluster admin revokes anyone's.
            let bobs = bob.create(&rt, "bob", "mine", 2).await;
            assert_eq!(bobs.error, ErrorCode::NONE);
            assert_eq!(admin.revoke(&rt, bobs.id).await, (ErrorCode::NONE, None));
            assert_eq!(admin.revoke(&rt, made.id).await, (ErrorCode::NONE, None));
            assert!(admin.list(&rt, None).await.is_empty());
            stop(nodes).await;
        });
    });
}

#[test]
fn token_lifetimes_are_only_the_five_steps() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            for code in [0, 6, 255] {
                let r = alice.create(&rt, "alice", "odd", code).await;
                assert_eq!(r.error, ErrorCode::INVALID_REQUEST, "lifetime code {code}");
            }
            assert!(alice.list(&rt, None).await.is_empty());
            for (code, days) in [(1, 7), (2, 30), (3, 90), (4, 180), (5, 365)] {
                let before = rt.unix_time().as_millis() as u64;
                let r = alice.create(&rt, "alice", &format!("d{days}"), code).await;
                let after = rt.unix_time().as_millis() as u64;
                assert_eq!(r.error, ErrorCode::NONE, "{:?}", r.error_msg);
                let listed = alice.list(&rt, None).await;
                let t = listed.iter().find(|t| t.id == r.id).expect("listed");
                assert!((before..=after).contains(&t.created_at_ms), "created by the leader's clock");
                assert_eq!(t.expires_at_ms - t.created_at_ms, days * DAY_MS, "code {code}");
                assert_eq!(r.expires_at_ms, t.expires_at_ms);
            }
            stop(nodes).await;
        });
    });
}

#[test]
fn token_limits_hold() {
    lotse_sim::check_few_seeds(2, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut bob = AsUser::new("bob", "bob-pw");
            let mut first = 0;
            for i in 0..50 {
                let r = alice.create(&rt, "alice", &format!("t{i}"), 1).await;
                assert_eq!(r.error, ErrorCode::NONE, "token {i}: {:?}", r.error_msg);
                if i == 0 {
                    first = r.id;
                }
            }
            let r = alice.create(&rt, "alice", "t50", 1).await;
            assert_eq!(r.error, ErrorCode::INVALID_REQUEST);
            assert!(r.error_msg.as_deref().is_some_and(|m| m.contains("50")), "{:?}", r.error_msg);
            assert_eq!(alice.list(&rt, None).await.len(), 50, "the 51st was not written");
            // The limit is per owner: bob has room.
            assert_eq!(bob.create(&rt, "bob", "b", 1).await.error, ErrorCode::NONE);
            // A revoked token gives its place back.
            assert_eq!(alice.revoke(&rt, first).await.0, ErrorCode::NONE);
            assert_eq!(alice.create(&rt, "alice", "again", 1).await.error, ErrorCode::NONE);
            stop(nodes).await;
        });
    });
}

#[test]
fn tokens_wait_for_cluster_version_0_1() {
    use lotse_proto::cluster_version::of;
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            // Nodes that run with cluster version 0.0 at most.
            let versions =
                lotse_server::version::Versions { release: "0.0.9".into(), oldest: of(0, 0), newest: of(0, 0) };
            let nodes = cluster_on(&rt, auth(), versions).await;
            setup_token_users(&rt).await;
            cluster_version_is(&rt, &client(&rt, "tv", scram("admin", "admin-pw")), of(0, 0)).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut admin = AsUser::new("admin", "admin-pw");
            let why = "tokens need cluster version 0.1; raise it with `lotse upgrade finalize`";
            let r = alice.create(&rt, "alice", "early", 2).await;
            assert_eq!((r.error, r.error_msg.as_deref(), r.id), (ErrorCode::UNSUPPORTED_VERSION, Some(why), 0));
            let r = admin.create(&rt, "alice", "early", 2).await;
            assert_eq!((r.error, r.error_msg.as_deref()), (ErrorCode::UNSUPPORTED_VERSION, Some(why)));
            assert_eq!(admin.revoke(&rt, 1).await, (ErrorCode::UNSUPPORTED_VERSION, Some(why.to_string())));
            assert!(admin.list(&rt, None).await.is_empty(), "nothing was written");
            for node in &nodes {
                let obs = node.handle().observe(lotse_server::ops::Scraper::Local).await.unwrap();
                assert_eq!(obs.tokens, Some(0));
            }
            stop(nodes).await;
        });
    });
}

// Signing in with a token (ADR 0133).

/// The text of token `id` whose secret is 32 bytes of `seed`.
fn token_text(id: u64, seed: u8) -> String {
    lotse_auth::token::Token::new(id, [seed; 32]).text()
}

/// The hash `CreateToken` takes for the secret of 32 bytes of `seed`.
fn secret_hash(seed: u8) -> Vec<u8> {
    lotse_auth::token::Token::new(0, [seed; 32]).hash().to_vec()
}

/// Signs in with `text` on `conn` (`LOTSE-TOKEN`): the node's answer.
async fn token_sign_in(
    rt: &SimRuntime,
    conn: &lotse_sim::SimConn,
    text: &str,
) -> lotse_proto::SaslAuthenticateResponse {
    let hs = Request::SaslHandshake(SaslHandshakeRequest { mechanism: lotse_auth::token::MECHANISM.into() });
    match call(rt, conn, 1, &hs).await {
        Response::SaslHandshake(r) => assert_eq!(r.error, ErrorCode::NONE, "{r:?}"),
        other => panic!("{other:?}"),
    }
    let step = Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: Bytes::from(text.as_bytes().to_vec()) });
    match call(rt, conn, 2, &step).await {
        Response::SaslAuthenticate(r) => r,
        other => panic!("{other:?}"),
    }
}

/// A connection from `host` to node `node`, signed in with `text` once the
/// node has applied the token; the node's answer with it.
async fn signed_in(
    rt: &SimRuntime,
    host: &str,
    node: u32,
    text: &str,
) -> (lotse_sim::SimConn, lotse_proto::SaslAuthenticateResponse) {
    for _ in 0..100 {
        let conn = rt.node(host).connect(&format!("n{node}:9000"), Link::Client).await.unwrap();
        let r = token_sign_in(rt, &conn, text).await;
        if r.error == ErrorCode::NONE {
            return (conn, r);
        }
        rt.sleep(TICK * 5).await;
    }
    panic!("node {node} never took the token")
}

/// Whether a failed sign-in said only `authentication failed` and closed.
async fn refused(rt: &SimRuntime, host: &str, text: &str) -> bool {
    let conn = raw(rt, host).await;
    let r = token_sign_in(rt, &conn, text).await;
    let same = (r.error, r.error_msg.as_deref(), r.session_lifetime_ms)
        == (ErrorCode::AUTHENTICATION_FAILED, Some("authentication failed"), 0);
    assert!(same, "{r:?}");
    same && conn_closed(rt, &conn).await
}

/// Whether `conn` sees stream `s`.
async fn sees_stream(rt: &SimRuntime, conn: &lotse_sim::SimConn) -> bool {
    let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec!["s".into()] });
    match call(rt, conn, 30, &meta).await {
        Response::Metadata(m) => m.resources.iter().any(|r| r.name == "s" && r.error == ErrorCode::NONE),
        other => panic!("{other:?}"),
    }
}

/// Node 1's token sign-ins so far: succeeded and failed, as `/metrics`
/// renders them.
async fn sign_ins(nodes: &[Node<SimRuntime>]) -> (u64, u64) {
    let obs = nodes[0].handle().observe(lotse_server::ops::Scraper::Local).await.unwrap();
    let text = lotse_server::ops::render(&obs);
    let count = |result: &str| {
        let prefix = format!("lotse_token_sign_ins_total{{node=\"1\",result=\"{result}\"}} ");
        let line = text.lines().find_map(|l| l.strip_prefix(&prefix)).unwrap_or_else(|| panic!("{prefix}\n{text}"));
        line.parse::<u64>().unwrap()
    };
    (count("ok"), count("failed"))
}

/// When `conn` closes, as the unix time in ms, waiting at most `within`;
/// `None` if a frame comes or it stays open.
async fn closed_at(rt: &SimRuntime, conn: &lotse_sim::SimConn, within: Duration) -> Option<u64> {
    let mut recv = std::pin::pin!(conn.recv());
    let mut wait = std::pin::pin!(rt.sleep(within));
    let closed = std::future::poll_fn(|cx| {
        if let std::task::Poll::Ready(r) = recv.as_mut().poll(cx) {
            return std::task::Poll::Ready(!matches!(r, Ok(Some(_))));
        }
        wait.as_mut().poll(cx).map(|()| false)
    })
    .await;
    closed.then(|| rt.unix_time().as_millis() as u64)
}

#[test]
fn a_token_signs_in_as_its_owner_with_only_name_roles() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut admin = AsUser::new("admin", "admin-pw");
            let hers = alice.create_with(&rt, "", "laptop", 1, secret_hash(1)).await;
            assert_eq!(hers.error, ErrorCode::NONE, "{:?}", hers.error_msg);
            let his = admin.create_with(&rt, "bob", "for-bob", 1, secret_hash(2)).await;
            assert_eq!(his.error, ErrorCode::NONE, "{:?}", his.error_msg);
            let (before_ok, _) = sign_ins(&nodes).await;

            // Alice's token acts as alice, with the roles bound to her name.
            let (conn, r) = signed_in(&rt, "t1", 1, &token_text(hers.id, 1)).await;
            assert!(r.session_lifetime_ms > 0 && r.session_lifetime_ms <= MAX_SESSION.as_millis() as u64, "{r:?}");
            assert!(sees_stream(&rt, &conn).await, "alice is a producer by name");
            // A pasted token with a newline works the same.
            let (pasted, _) = signed_in(&rt, "t2", 1, &format!("{}\n", token_text(hers.id, 1))).await;
            assert!(sees_stream(&rt, &pasted).await);

            // Bob is in the group ops at the identity provider, which may
            // produce; with a token, groups do not count.
            let idp = issuer("k1");
            let oauth = raw(&rt, "o1").await;
            let hs = Request::SaslHandshake(SaslHandshakeRequest { mechanism: "OAUTHBEARER".into() });
            assert!(
                matches!(call(&rt, &oauth, 1, &hs).await, Response::SaslHandshake(r) if r.error == ErrorCode::NONE)
            );
            let first = Bytes::from(lotse_auth::oauth::initial_response(&token(&rt, &idp, "bob", 600)));
            let r =
                call(&rt, &oauth, 2, &Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: first })).await;
            assert!(matches!(r, Response::SaslAuthenticate(ref r) if r.error == ErrorCode::NONE), "{r:?}");
            assert!(sees_stream(&rt, &oauth).await, "bob through ops");
            let (bobs, _) = signed_in(&rt, "t3", 1, &token_text(his.id, 2)).await;
            assert!(!sees_stream(&rt, &bobs).await, "bob's token has no group");

            let (after_ok, _) = sign_ins(&nodes).await;
            assert!(after_ok >= before_ok + 3, "{before_ok} {after_ok}");
            stop(nodes).await;
        });
    });
}

#[test]
fn every_failed_token_sign_in_says_the_same() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let kept = alice.create_with(&rt, "alice", "kept", 1, secret_hash(1)).await;
            let gone = alice.create_with(&rt, "alice", "gone", 1, secret_hash(3)).await;
            assert_eq!((kept.error, gone.error), (ErrorCode::NONE, ErrorCode::NONE));
            // Both work, so what follows fails for its own reason.
            signed_in(&rt, "w1", 1, &token_text(kept.id, 1)).await;
            signed_in(&rt, "w2", 1, &token_text(gone.id, 3)).await;

            let (ok, failed) = sign_ins(&nodes).await;
            assert!(refused(&rt, "f1", &token_text(kept.id + 1000, 1)).await, "an unknown id");
            assert!(refused(&rt, "f2", &token_text(kept.id, 9)).await, "a wrong secret");
            assert!(refused(&rt, "f3", "lotse_tok_nonsense").await, "not a token");
            assert_eq!(alice.revoke(&rt, gone.id).await, (ErrorCode::NONE, None));
            rt.sleep(TICK * 50).await;
            assert!(refused(&rt, "f4", &token_text(gone.id, 3)).await, "a revoked token");
            // A week on (lifetime 1), the kept one has expired.
            rt.step_wall_clock(Duration::from_millis(7 * DAY_MS + 1_000));
            assert!(refused(&rt, "f5", &token_text(kept.id, 1)).await, "an expired token");
            assert_eq!(sign_ins(&nodes).await, (ok, failed + 5), "each counted once as failed");
            stop(nodes).await;
        });
    });
}

#[test]
fn revoking_a_token_ends_its_connections_at_once() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let doomed = alice.create_with(&rt, "alice", "doomed", 2, secret_hash(1)).await;
            let other = alice.create_with(&rt, "alice", "other", 2, secret_hash(2)).await;
            assert_eq!((doomed.error, other.error), (ErrorCode::NONE, ErrorCode::NONE));
            let (on_1, _) = signed_in(&rt, "d1", 1, &token_text(doomed.id, 1)).await;
            let (on_2, _) = signed_in(&rt, "d2", 2, &token_text(doomed.id, 1)).await;
            let (kept, _) = signed_in(&rt, "k1", 1, &token_text(other.id, 2)).await;
            let password = raw(&rt, "k2").await;
            login(&rt, &password, "alice", "alice-pw").await.unwrap();

            let start = rt.now();
            assert_eq!(alice.revoke(&rt, doomed.id).await, (ErrorCode::NONE, None));
            // Waiting for nothing, both connections end.
            for conn in [&on_1, &on_2] {
                assert!(conn_closed(&rt, conn).await, "a connection of the revoked token stays open");
            }
            assert!(rt.now() - start < Duration::from_secs(1), "at once: {:?}", rt.now() - start);
            // Alice's other token and her password session go on.
            assert!(sees_stream(&rt, &kept).await);
            assert!(sees_stream(&rt, &password).await);
            // Until alice is deleted, which takes her tokens with her.
            client(&rt, "c1", scram("admin", "admin-pw")).delete_user("alice").await.unwrap();
            assert!(conn_closed(&rt, &kept).await, "a deleted owner's token session stays open");
            stop(nodes).await;
        });
    });
}

#[test]
fn an_idle_session_ends_when_its_token_expires() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let t = alice.create_with(&rt, "alice", "week", 1, secret_hash(1)).await;
            assert_eq!(t.error, ErrorCode::NONE);
            signed_in(&rt, "w", 1, &token_text(t.id, 1)).await;
            // Three seconds before it expires, less than a session's
            // maximum (MAX_SESSION), it signs in once more and then sends
            // nothing.
            let now = rt.unix_time().as_millis() as u64;
            rt.step_wall_clock(Duration::from_millis(t.expires_at_ms - now - 3_000));
            let (conn, r) = signed_in(&rt, "idle", 1, &token_text(t.id, 1)).await;
            assert!(r.session_lifetime_ms > 0 && r.session_lifetime_ms <= 3_000, "{r:?}");
            let closed = closed_at(&rt, &conn, Duration::from_secs(10)).await.expect("the connection closes");
            assert!(closed >= t.expires_at_ms, "not before the token expires: {closed} {}", t.expires_at_ms);
            assert!(closed <= t.expires_at_ms + 1_000, "within a second of expiry: {closed} {}", t.expires_at_ms);
            stop(nodes).await;
        });
    });
}

#[test]
fn a_token_session_cannot_create_tokens() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let t = alice.create_with(&rt, "alice", "mine", 2, secret_hash(1)).await;
            assert_eq!(t.error, ErrorCode::NONE);
            let req = Request::CreateToken(lotse_proto::CreateTokenRequest {
                name: "successor".into(),
                owner: String::new(),
                lifetime: 5,
                secret_hash: Bytes::from(secret_hash(2)),
            });
            // On every node, the metadata leader included.
            let mut answers = Vec::new();
            for node in 1..=3 {
                let (conn, _) = signed_in(&rt, &format!("c{node}"), node, &token_text(t.id, 1)).await;
                answers.push(code(&call(&rt, &conn, 40, &req).await));
            }
            let why = "a session signed in with a token cannot create tokens";
            assert!(answers.iter().all(|a| *a == (ErrorCode::UNAUTHORIZED, Some(why.into()))), "{answers:?}");
            assert_eq!(names(&alice.list(&rt, None).await), vec![triple("alice", "mine", "alice")]);
            stop(nodes).await;
        });
    });
}

#[test]
fn revoking_the_owners_sessions_ends_token_sessions() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let mut bob = AsUser::new("bob", "bob-pw");
            let old = alice.create_with(&rt, "alice", "old", 2, secret_hash(1)).await;
            let bobs = bob.create_with(&rt, "bob", "bobs", 2, secret_hash(2)).await;
            assert_eq!((old.error, bobs.error), (ErrorCode::NONE, ErrorCode::NONE));
            let (hers, _) = signed_in(&rt, "a1", 1, &token_text(old.id, 1)).await;
            let (his, _) = signed_in(&rt, "b1", 1, &token_text(bobs.id, 2)).await;

            rt.sleep(TICK * 10).await;
            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            admin.revoke_sessions("alice", false).await.unwrap();
            assert!(conn_closed(&rt, &hers).await, "alice's token session ends");
            let meta = Request::Metadata(lotse_proto::MetadataRequest { names: vec![] });
            assert!(matches!(call(&rt, &his, 30, &meta).await, Response::Metadata(_)), "bob's goes on");
            // A token made before the revocation stays refused; one made
            // after works.
            rt.sleep(TICK * 50).await;
            assert!(refused(&rt, "a2", &token_text(old.id, 1)).await, "made before the revocation");
            let new = alice.create_with(&rt, "alice", "new", 2, secret_hash(3)).await;
            assert_eq!(new.error, ErrorCode::NONE);
            let (conn, _) = signed_in(&rt, "a3", 1, &token_text(new.id, 3)).await;
            assert!(sees_stream(&rt, &conn).await);
            stop(nodes).await;
        });
    });
}

#[test]
fn describe_sessions_names_the_token_id_and_never_the_secret() {
    use lotse_proto::session_mechanism;
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let mut alice = AsUser::new("alice", "alice-pw");
            let t = alice.create_with(&rt, "alice", "laptop", 2, secret_hash(1)).await;
            assert_eq!(t.error, ErrorCode::NONE);
            let created_at_ms = alice.list(&rt, None).await.iter().find(|e| e.id == t.id).unwrap().created_at_ms;
            let text = token_text(t.id, 1);
            let (conn, _) = signed_in(&rt, "t1", 1, &text).await;

            let admin = client(&rt, "c1", scram("admin", "admin-pw"));
            let all = sessions_seen(&admin).await;
            let hers: Vec<_> = all.iter().filter(|(n, s)| *n == 1 && s.peer.starts_with("t1")).collect();
            assert_eq!(hers.len(), 1, "{all:?}");
            let s = &hers[0].1;
            assert_eq!((s.name.as_str(), s.mechanism, s.token_id), ("alice", session_mechanism::TOKEN, Some(t.id)));
            assert!(s.groups.is_empty(), "{s:?}");
            assert_eq!(s.credential_ms, created_at_ms, "the credential dates from the token's creation");
            assert!(s.expires_ms > 0 && s.expires_ms <= t.expires_at_ms, "{s:?}");
            assert!(
                all.iter().filter(|(_, s)| s.mechanism != session_mechanism::TOKEN).all(|(_, s)| s.token_id.is_none())
            );
            // Alice sees it too, on her token's own connection.
            let req = Request::DescribeSessions(lotse_proto::DescribeSessionsRequest);
            let mine = match call(&rt, &conn, 50, &req).await {
                Response::DescribeSessions(r) => r,
                other => panic!("{other:?}"),
            };
            assert!(mine.sessions.iter().any(|s| s.token_id == Some(t.id)), "{mine:?}");
            // The secret is nowhere in what any of it says.
            // Its last 43 characters (base64url, which may hold `_`).
            let secret = &text[text.len() - 43..];
            let said = format!("{all:?} {mine:?}");
            assert!(!said.contains(secret) && !said.contains(&text), "{said}");
            stop(nodes).await;
        });
    });
}

/// The client's own calls for tokens, end to end: a token made by one
/// client signs another in as the owner, and revoking it ends that.
#[test]
fn a_client_signs_in_with_a_token_it_created() {
    lotse_sim::check_few_seeds(3, |seed| {
        Sim::new(seed).run(|rt| async move {
            let nodes = token_cluster(&rt).await;
            let alice = client(&rt, "a1", scram("alice", "alice-pw"));
            let created = alice.create_token("laptop", None, lotse_client::Lifetime::Week).await.expect("created");
            assert!(created.id > 0 && created.text.starts_with("lotse_tok_"), "{created:?}");
            assert!(created.expires_at_ms > rt.unix_time().as_millis() as u64);
            assert!(!format!("{created:?}").contains(&created.text), "Debug hides the text");

            let with_token = client(&rt, "a2", Some(Credentials::Token(created.text.clone())));
            assert_eq!(with_token.who_am_i().await.expect("signed in").name, "alice");
            with_token.produce("s", 0, batch("from a token")).await.expect("alice's roles apply");
            let listed = with_token.describe_tokens(None).await.expect("lists their own");
            assert_eq!(
                listed.iter().map(|t| (t.id, t.name.as_str())).collect::<Vec<_>>(),
                vec![(created.id, "laptop")]
            );

            // The same id with another secret is not the token.
            let mut forged = created.text.clone();
            forged.pop();
            forged.push(if created.text.ends_with('A') { 'B' } else { 'A' });
            assert!(denied(client(&rt, "a3", Some(Credentials::Token(forged))).produce("s", 0, batch("x")).await));

            // Revoked, it signs no one in and ends the open session.
            alice.revoke_token(created.id).await.expect("revoked");
            assert!(matches!(alice.revoke_token(created.id).await, Err(ClientError::Broker { .. })), "once only");
            let mut ended = false;
            for _ in 0..100 {
                if denied(with_token.produce("s", 0, batch("after")).await) {
                    ended = true;
                    break;
                }
                rt.sleep(TICK * 10).await;
            }
            assert!(ended, "the revoked token still works");
            assert!(denied(
                client(&rt, "a4", Some(Credentials::Token(created.text))).produce("s", 0, batch("x")).await
            ));
            stop(nodes).await;
        });
    });
}

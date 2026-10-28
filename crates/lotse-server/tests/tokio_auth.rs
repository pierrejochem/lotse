//! Logging in on real sockets: three nodes on localhost over TLS that
//! require authentication, a fake identity provider over HTTP on loopback,
//! and clients with a password, a bearer token and a certificate.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::scram::ScramCredential;
use lotse_auth::testing::TestIssuer;
use lotse_client::{Client, ClientConfig, ClientError, Credentials};
use lotse_log::LogConfig;
use lotse_raft::NodeId;
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Clock;
use lotse_rt_tokio::TokioRuntime;
use lotse_rt_tokio::dev_certs::DevCa;
use lotse_server::{AuthConfig, Node, NodeConfig, OidcConfig, StateMachine, StreamConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_way_in_on_real_sockets() {
    let root = std::env::temp_dir().join(format!("lotse-tokio-auth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ca = DevCa::new();

    // The identity provider: its address is its issuer, so it is known
    // before the documents that name it.
    let listener_port = free_port();
    let issuer_url = format!("http://127.0.0.1:{listener_port}");
    let idp = TestIssuer::new(&issuer_url, "k1", 3);
    let mut docs = BTreeMap::new();
    docs.insert("/.well-known/openid-configuration".to_string(), idp.discovery(&format!("{issuer_url}/keys")));
    docs.insert("/keys".to_string(), idp.jwks());
    let served = identity_provider(listener_port, docs).await;
    assert_eq!(served, format!("127.0.0.1:{listener_port}"));

    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let clients: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, format!("127.0.0.1:{}", free_port()))).collect();
    let mut users = BTreeMap::new();
    users.insert("admin".to_string(), ScramCredential::new("admin-pw", b"salt-for-admin", 4096));
    let auth = AuthConfig {
        required: true,
        mtls: true,
        scram: true,
        oidc: Some(OidcConfig::new(&issuer_url, "lotse")),
        max_session: Duration::from_secs(60),
        users,
    };
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let cfg = NodeConfig {
            id,
            listen: peers[&id].clone(),
            peers: peers.clone(),
            groups: vec![1, 2, 3],
            tick: Duration::from_millis(10),
            pre_vote: true,
            log: LogConfig::default(),
            client_listen: Some(clients[&id].clone()),
            client_addrs: clients.clone(),
            streams: vec![StreamConfig { name: "s".into(), resource_id: 7, partitions: vec![3] }],
            queues: Vec::new(),
            exchanges: Vec::new(),
            group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
            group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
            metadata: Some(1),
            coordinator: Some(2),
            max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
            auth: auth.clone(),
        };
        let rt = TokioRuntime::new(root.join(format!("node-{id}"))).with_tls(ca.node_tls(id, &["127.0.0.1"]).unwrap());
        std::fs::create_dir_all(rt.root()).unwrap();
        nodes.push(Node::start(rt, cfg, |_| Nothing).await.unwrap());
    }
    let bootstrap: Vec<String> = clients.values().cloned().collect();
    let client = |tls: lotse_rt_tokio::TlsConfig, credentials: Option<Credentials>| {
        Client::new(
            TokioRuntime::new(&root).with_tls(tls),
            ClientConfig { retry_for: Duration::from_secs(20), credentials, ..ClientConfig::new(bootstrap.clone()) },
        )
    };

    // Nobody.
    let anon = client(ca.client_tls(None).unwrap(), None);
    assert!(matches!(anon.produce("s", 0, batch("x")).await, Err(ClientError::Authentication(_))));

    // A password.
    let admin = client(
        ca.client_tls(None).unwrap(),
        Some(Credentials::Scram { user: "admin".into(), password: "admin-pw".into() }),
    );
    admin.produce("s", 0, batch("admin")).await.expect("SCRAM");
    admin.set_user_password("alice", "alice-pw").await.expect("creates a user");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let alice = client(
        ca.client_tls(None).unwrap(),
        Some(Credentials::Scram { user: "alice".into(), password: "alice-pw".into() }),
    );
    alice.produce("s", 0, batch("alice")).await.expect("a runtime user");

    // A token.
    let now = TokioRuntime::new(&root).unix_time().as_secs();
    let token = idp.token("bob", &["ops"], "lotse", now + 300);
    let bob = client(ca.client_tls(None).unwrap(), Some(Credentials::OAuthBearer(Arc::new(move || token.clone()))));
    bob.produce("s", 0, batch("bob")).await.expect("OAUTHBEARER, keys fetched over HTTP");

    // A certificate.
    let svc = client(ca.client_tls(Some("billing")).unwrap(), None);
    svc.produce("s", 0, batch("billing")).await.expect("a client certificate");

    drop((anon, admin, alice, bob, svc));
    for node in nodes {
        node.handle().stop();
        node.join().await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// Serves `docs` (path to body) over plain HTTP on loopback at `port`.
async fn identity_provider(port: u16, docs: BTreeMap<String, Vec<u8>>) -> String {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let docs = Arc::new(docs);
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let docs = docs.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let path = req.split(' ').nth(1).unwrap_or("/").to_owned();
                let resp = match docs.get(&path) {
                    Some(body) => {
                        let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
                        r.extend_from_slice(body);
                        r
                    }
                    None => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_vec(),
                };
                let _ = s.write_all(&resp).await;
            });
        }
    });
    addr
}

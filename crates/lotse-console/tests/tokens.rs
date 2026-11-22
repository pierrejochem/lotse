//! Access tokens through the console, against a real node that requires
//! authentication (ADR 0133). The console acts as whoever signed in: each
//! user makes, lists and revokes their own tokens, and the token's text
//! exists only in the answer to its creation.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use http_body_util::BodyExt;
use lotse_auth::scram::ScramCredential;
use lotse_client::{Client, ClientConfig, Credentials};
use lotse_console::auth::AuthConfig;
use lotse_console::cluster::Cluster;
use lotse_console::cluster::native::NativeCluster;
use lotse_console::metrics::MetricsStore;
use lotse_console::{AppState, router};
use lotse_log::LogConfig;
use lotse_proto::cluster_version::of;
use lotse_record::Batch;
use lotse_rt::Clock;
use lotse_rt_tokio::TokioRuntime;
use lotse_rt_tokio::dev_certs::DevCa;
use lotse_server::version::Versions;
use lotse_server::{AuthConfig as NodeAuth, Node, NodeConfig, StateMachine};
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

/// A port nothing listens on, below the ephemeral range.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1_200) as u16 * 10 + 5;
    loop {
        let port = 20_000 + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

/// One node with SCRAM users admin, alice and bob, and a console router for
/// each of them.
struct Setup {
    _node: Node<TokioRuntime>,
    root: std::path::PathBuf,
    ca: DevCa,
    bootstrap: Vec<String>,
    native: Arc<NativeCluster>,
}

impl Setup {
    async fn start(name: &str, versions: Versions) -> Setup {
        let root = std::env::temp_dir().join(format!("lotse-console-tokens-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let ca = DevCa::new();
        let peer = format!("127.0.0.1:{}", free_port());
        let client_addr = format!("127.0.0.1:{}", free_port());
        let users: BTreeMap<String, ScramCredential> = ["admin", "alice", "bob"]
            .into_iter()
            .map(|u| {
                (u.to_string(), ScramCredential::new(&format!("{u}-pw"), format!("salt-for-{u}").as_bytes(), 4096))
            })
            .collect();
        let auth = NodeAuth {
            required: true,
            mtls: true,
            scram: true,
            tokens: true,
            max_session: Duration::from_secs(60),
            users,
            admins: vec!["admin".into()],
            tenants: Vec::new(),
            resource_tenants: Default::default(),
            oidc: None,
            client_cas: Default::default(),
            renewal: Default::default(),
        };
        let cfg = NodeConfig {
            id: 1,
            listen: peer.clone(),
            peers: [(1, peer)].into(),
            groups: vec![1, 2],
            tick: Duration::from_millis(10),
            pre_vote: true,
            log: LogConfig::default(),
            client_listen: Some(client_addr.clone()),
            client_addrs: [(1, client_addr.clone())].into(),
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
            disk_free_limit: lotse_server::FreeLimit::OFF,
            disk_check_interval: lotse_server::DEFAULT_DISK_CHECK_INTERVAL,
            versions,
            retention: lotse_server::RetentionConfig::keep(),
            join: false,
            auth,
        };
        let rt = TokioRuntime::new(root.join("node-1")).with_tls(ca.node_tls(1, &["127.0.0.1"]).unwrap());
        std::fs::create_dir_all(rt.root()).unwrap();
        let node = Node::start(rt, cfg, |_| Nothing).await.unwrap();
        let bootstrap = vec![client_addr];
        let rt = TokioRuntime::new(&root).with_tls(ca.client_tls(None).unwrap());
        let native = Arc::new(NativeCluster::with_runtime("local", bootstrap.clone(), rt));
        Setup { _node: node, root, ca, bootstrap, native }
    }

    /// The console for `user`, signed in with their password.
    fn console(&self, user: &str) -> axum::Router {
        let auth = AuthConfig::Dev { user: user.into(), password: Some(format!("{user}-pw")) };
        let store = Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));
        let clusters: Vec<Arc<dyn Cluster>> = vec![self.native.clone()];
        router(AppState::new(clusters, auth, store, Duration::from_secs(1)))
    }

    /// A client of the node with `credentials`.
    fn client(&self, credentials: Credentials) -> Client<TokioRuntime> {
        Client::new(
            TokioRuntime::new(&self.root).with_tls(self.ca.client_tls(None).unwrap()),
            ClientConfig {
                retry_for: Duration::from_secs(5),
                credentials: Some(credentials),
                ..ClientConfig::new(self.bootstrap.clone())
            },
        )
    }

    /// Waits until the cluster has taken cluster version 0.1, which tokens need.
    async fn tokens_allowed(&self) {
        let admin = self.client(Credentials::Scram { user: "admin".into(), password: "admin-pw".into() });
        for _ in 0..300 {
            if admin.describe_upgrade().await.ok().and_then(|d| d.cluster_version) == Some(of(0, 1)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the cluster never took cluster version 0.1");
    }
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

const TOKENS: &str = "/api/v1/clusters/local/tokens";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_creates_lists_and_revokes_tokens_as_the_user() {
    let s = Setup::start("own", Versions::default()).await;
    s.tokens_allowed().await;
    let alice = s.console("alice");
    let before = TokioRuntime::new(&s.root).unix_time().as_millis() as u64;

    let (st, made) = call(&alice, "POST", TOKENS, Some(json!({"name": "ci", "lifetime": "1m"}))).await;
    assert_eq!(st, StatusCode::OK, "{made}");
    let text = made["text"].as_str().expect("the text is in the answer").to_owned();
    assert!(text.starts_with("lotse_tok_"), "{text}");
    let id = made["id"].as_u64().unwrap();
    let month = 30 * 24 * 3600 * 1000;
    let ends = made["expires_at"].as_u64().unwrap();
    assert!(ends >= before + month && ends <= before + month + 60_000, "30 days from now: {ends} {before}");

    let (st, list) = call(&alice, "GET", TOKENS, None).await;
    assert_eq!(st, StatusCode::OK, "{list}");
    let rows = list["tokens"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "{list}");
    assert_eq!((rows[0]["id"].as_u64(), rows[0]["name"].as_str()), (Some(id), Some("ci")));
    assert_eq!((rows[0]["owner"].as_str(), rows[0]["created_by"].as_str()), (Some("alice"), Some("alice")));
    assert_eq!(rows[0]["expires_at"].as_u64(), Some(ends));

    // The token is a credential for alice, and for nobody else.
    let with_token = s.client(Credentials::Token(text.clone()));
    let me = with_token.who_am_i().await.expect("the token signs in");
    assert_eq!(me.name, "alice");

    // Revoking ends the token.
    let (st, done) = call(&alice, "POST", &format!("{TOKENS}/{id}/revoke"), None).await;
    assert_eq!(st, StatusCode::OK, "{done}");
    assert!(done["cli"].as_str().unwrap().contains(&id.to_string()), "{done}");
    let (_, list) = call(&alice, "GET", TOKENS, None).await;
    assert!(list["tokens"].as_array().unwrap().is_empty(), "{list}");
    let again = s.client(Credentials::Token(text));
    assert!(again.who_am_i().await.is_err(), "a revoked token is refused");
    let (st, _) = call(&alice, "POST", &format!("{TOKENS}/{id}/revoke"), None).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "revoked already");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_created_token_text_is_returned_once_and_never_listed() {
    let s = Setup::start("once", Versions::default()).await;
    s.tokens_allowed().await;
    let alice = s.console("alice");

    let (st, made) = call(&alice, "POST", TOKENS, Some(json!({"name": "ci", "lifetime": "1w"}))).await;
    assert_eq!(st, StatusCode::OK, "{made}");
    let text = made["text"].as_str().unwrap().to_owned();
    for (_, view) in [call(&alice, "GET", TOKENS, None).await, call(&alice, "GET", TOKENS, None).await] {
        assert!(!view.to_string().contains(&text), "the list holds no secret: {view}");
        assert!(view["tokens"][0].get("text").is_none(), "{view}");
    }
    // Admins see it no more than its owner.
    let (_, all) = call(&s.console("admin"), "GET", &format!("{TOKENS}?owner=alice"), None).await;
    assert!(!all.to_string().contains(&text), "{all}");

    // The five lifetimes, and nothing else.
    for (lifetime, days) in [("1w", 7u64), ("3m", 90), ("6m", 180), ("1y", 365)] {
        let (st, v) = call(&alice, "POST", TOKENS, Some(json!({"name": lifetime, "lifetime": lifetime}))).await;
        assert_eq!(st, StatusCode::OK, "{lifetime}: {v}");
        let (created, ends) =
            (TokioRuntime::new(&s.root).unix_time().as_millis() as u64, v["expires_at"].as_u64().unwrap());
        assert!(ends.abs_diff(created + days * 86_400_000) < 120_000, "{lifetime}: {ends}");
    }
    for bad in ["2w", "", "30d", "1Y", "1 y"] {
        let (st, v) = call(&alice, "POST", TOKENS, Some(json!({"name": "x", "lifetime": bad}))).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad:?}: {v}");
        assert!(v["message"].as_str().unwrap().contains("1w"), "names the choices: {v}");
    }
    let (st, v) = call(&alice, "POST", TOKENS, Some(json!({"name": "", "lifetime": "1w"}))).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "the broker's refusal reaches the caller: {v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_cannot_list_another_users_tokens() {
    let s = Setup::start("others", Versions::default()).await;
    s.tokens_allowed().await;
    let (alice, bob, admin) = (s.console("alice"), s.console("bob"), s.console("admin"));
    let (st, made) = call(&alice, "POST", TOKENS, Some(json!({"name": "ci", "lifetime": "1w"}))).await;
    assert_eq!(st, StatusCode::OK, "{made}");
    let id = made["id"].as_u64().unwrap();

    // Bob sees none of it: not by asking for alice, not in his own list.
    // What a user may not see is unknown to them: an empty list, not a hint
    // that alice has tokens.
    let (st, v) = call(&bob, "GET", &format!("{TOKENS}?owner=alice"), None).await;
    assert_eq!((st, v["tokens"].as_array().map(Vec::len)), (StatusCode::OK, Some(0)), "{v}");
    let (_, own) = call(&bob, "GET", TOKENS, None).await;
    assert!(own["tokens"].as_array().unwrap().is_empty(), "{own}");
    // Nor can he revoke it, or make one for her.
    let (st, v) = call(&bob, "POST", &format!("{TOKENS}/{id}/revoke"), None).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    let (st, v) = call(&bob, "POST", TOKENS, Some(json!({"name": "x", "lifetime": "1w", "owner": "alice"}))).await;
    assert_eq!(st, StatusCode::FORBIDDEN, "{v}");

    // A cluster admin may, and the list says who made it.
    let (st, v) =
        call(&admin, "POST", TOKENS, Some(json!({"name": "for-bob", "lifetime": "1w", "owner": "bob"}))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let (_, mine) = call(&bob, "GET", TOKENS, None).await;
    assert_eq!(
        (mine["tokens"][0]["owner"].as_str(), mine["tokens"][0]["created_by"].as_str()),
        (Some("bob"), Some("admin"))
    );
    let (st, v) = call(&admin, "GET", &format!("{TOKENS}?owner=alice"), None).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["tokens"].as_array().unwrap().len(), 1, "{v}");
    let (st, _) = call(&admin, "POST", &format!("{TOKENS}/{id}/revoke"), None).await;
    assert_eq!(st, StatusCode::OK, "a cluster admin revokes anyone's");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_below_version_0_1_says_so() {
    let versions = Versions { release: "0.0.9".into(), oldest: of(0, 0), newest: of(0, 0) };
    let s = Setup::start("early", versions).await;
    let (st, v) = call(&s.console("alice"), "POST", TOKENS, Some(json!({"name": "ci", "lifetime": "1w"}))).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert!(v["message"].as_str().unwrap().contains("lotse upgrade finalize"), "the cluster's reason: {v}");
    assert!(v.get("text").is_none(), "{v}");
}

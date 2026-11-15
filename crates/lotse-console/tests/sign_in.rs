//! Signing in to the console, against a real node that requires
//! authentication: an OpenID Connect login (Authorization Code with PKCE)
//! at a fake identity provider, and dev mode with a SCRAM password. The
//! console acts as whoever signed in, so each sees what their roles allow.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use bytes::Bytes;
use http_body_util::BodyExt;
use lotse_auth::scram::ScramCredential;
use lotse_auth::testing::TestIssuer;
use lotse_client::{Client, ClientConfig, Credentials};
use lotse_console::auth::{
    AuthConfig, OidcLogin, SamplerLogin, Secret, ServiceCredentials, ServiceLogin, SessionKey, Sessions,
};
use lotse_console::cluster::Cluster;
use lotse_console::cluster::native::NativeCluster;
use lotse_console::metrics::MetricsStore;
use lotse_console::{AppState, router};
use lotse_log::LogConfig;
use lotse_proto::{RoleBindingData, role, subject_kind};
use lotse_record::{Batch, BatchBuilder, BatchOptions};
use lotse_rt::Clock;
use lotse_rt_tokio::TokioRuntime;
use lotse_rt_tokio::dev_certs::DevCa;
use lotse_server::{AuthConfig as NodeAuth, Node, NodeConfig, OidcConfig, StateMachine, StreamConfig};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// What the fake identity provider's token endpoint answers, in order, and
/// the forms it was sent.
#[derive(Debug, Default)]
struct TokenEndpoint {
    answers: VecDeque<Value>,
    forms: Vec<BTreeMap<String, String>>,
    revoked: Vec<BTreeMap<String, String>>,
}

fn form(body: &str) -> BTreeMap<String, String> {
    let decode = |s: &str| {
        let s = s.replace('+', " ");
        let mut out = Vec::new();
        let b = s.as_bytes();
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' && i + 2 < b.len() {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).unwrap());
                i += 3;
            } else {
                out.push(b[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    };
    body.split('&').filter_map(|kv| kv.split_once('=')).map(|(k, v)| (decode(k), decode(v))).collect()
}

/// Serves `docs` for GET and the token endpoint for `POST /token`, over
/// plain HTTP on loopback.
async fn identity_provider(port: u16, docs: BTreeMap<String, Vec<u8>>, token: Arc<Mutex<TokenEndpoint>>) {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
    let docs = Arc::new(docs);
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let (docs, token) = (docs.clone(), token.clone());
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head, body) = loop {
                    let n = s.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).into_owned();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().to_owned())
                            })
                            .map_or(0, |v| v.parse::<usize>().unwrap());
                        if body.len() >= len {
                            break (head.to_owned(), body.to_owned());
                        }
                    }
                };
                let mut words = head.split(' ');
                let (method, path) = (words.next().unwrap_or(""), words.next().unwrap_or("/"));
                let reply = match (method, path) {
                    ("POST", "/token") => {
                        let mut t = token.lock().unwrap();
                        t.forms.push(form(&body));
                        t.answers.pop_front().map(|v| v.to_string().into_bytes())
                    }
                    ("POST", "/revoke") => {
                        token.lock().unwrap().revoked.push(form(&body));
                        Some(Vec::new())
                    }
                    ("GET", p) => docs.get(p).cloned(),
                    _ => None,
                };
                let resp = match reply {
                    Some(body) => {
                        let mut r = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len()).into_bytes();
                        r.extend_from_slice(&body);
                        r
                    }
                    None => b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_vec(),
                };
                let _ = s.write_all(&resp).await;
            });
        }
    });
}

/// An ID token as the token endpoint hands it over: the console reads it
/// straight from the endpoint over TLS and checks its claims, not its
/// signature (OpenID Connect Core 3.1.3.7).
fn id_token(claims: Value) -> String {
    format!("{}.{}.sig", B64URL.encode(br#"{"alg":"none"}"#), B64URL.encode(claims.to_string()))
}

async fn call(app: &axum::Router, method: &str, uri: &str, cookie: Option<&str>, body: Option<Value>) -> Reply {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(c) = cookie {
        req = req.header(header::COOKIE, c);
    }
    let body = match body {
        Some(v) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let location = res.headers().get(header::LOCATION).map(|v| v.to_str().unwrap().to_owned());
    let cookies: Vec<String> =
        res.headers().get_all(header::SET_COOKIE).iter().map(|v| v.to_str().unwrap().to_owned()).collect();
    let named = |n: &str| cookies.iter().find(|c| c.starts_with(&format!("{n}="))).cloned();
    let (set_cookie, sign_in) = (named("lotse_session"), named("lotse_sign_in"));
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    Reply { status, location, set_cookie, sign_in, json: serde_json::from_slice(&bytes).unwrap_or(Value::Null) }
}

/// The `name=value` part of a Set-Cookie header.
fn pair(set: &str) -> String {
    set.split(';').next().unwrap().to_owned()
}

#[derive(Debug)]
struct Reply {
    status: StatusCode,
    location: Option<String>,
    /// The session cookie set, if any.
    set_cookie: Option<String>,
    /// The sign-in cookie set, if any.
    sign_in: Option<String>,
    json: Value,
}

fn query(url: &str) -> BTreeMap<String, String> {
    form(url.split_once('?').map_or("", |(_, q)| q))
}

fn names(v: &Value) -> Vec<String> {
    let mut n: Vec<String> = v.as_array().unwrap().iter().map(|r| r["name"].as_str().unwrap().to_owned()).collect();
    n.sort();
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_console_acts_as_whoever_signed_in() {
    let root = std::env::temp_dir().join(format!("lotse-console-sign-in-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let ca = DevCa::new();

    // The identity provider, which both the node and the console trust.
    let idp_port = free_port();
    let issuer = format!("http://127.0.0.1:{idp_port}");
    let idp = TestIssuer::new(&issuer, "k1", 7);
    let discovery = json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "end_session_endpoint": format!("{issuer}/logout"),
        "revocation_endpoint": format!("{issuer}/revoke"),
        "jwks_uri": format!("{issuer}/keys"),
    });
    let mut docs = BTreeMap::new();
    docs.insert("/.well-known/openid-configuration".to_string(), discovery.to_string().into_bytes());
    docs.insert("/keys".to_string(), idp.jwks());
    let token_endpoint = Arc::new(Mutex::new(TokenEndpoint::default()));
    identity_provider(idp_port, docs, token_endpoint.clone()).await;

    // One node that requires authentication, with a tenant "shop" holding
    // the stream "orders"; "audit" stays in the default tenant.
    let peer = format!("127.0.0.1:{}", free_port());
    let client_addr = format!("127.0.0.1:{}", free_port());
    let mut users = BTreeMap::new();
    users.insert("admin".to_string(), ScramCredential::new("admin-pw", b"salt-for-admin", 4096));
    let auth = NodeAuth {
        required: true,
        mtls: true,
        scram: true,
        oidc: Some(OidcConfig::new(&issuer, "lotse")),
        max_session: Duration::from_secs(60),
        users,
        admins: vec!["admin".into()],
        tenants: vec!["shop".into()],
        resource_tenants: [("orders".to_string(), "shop".to_string())].into(),
        client_cas: Default::default(),
    };
    let cfg = NodeConfig {
        id: 1,
        listen: peer.clone(),
        peers: [(1, peer)].into(),
        groups: vec![1, 2, 3, 4],
        tick: Duration::from_millis(10),
        pre_vote: true,
        log: LogConfig::default(),
        client_listen: Some(client_addr.clone()),
        client_addrs: [(1, client_addr.clone())].into(),
        zones: Default::default(),
        streams: vec![
            StreamConfig { name: "orders".into(), resource_id: 7, partitions: vec![3] },
            StreamConfig { name: "audit".into(), resource_id: 8, partitions: vec![4] },
        ],
        queues: Vec::new(),
        exchanges: Vec::new(),
        group_session_timeout: lotse_server::DEFAULT_GROUP_SESSION_TIMEOUT,
        group_heartbeat_interval: lotse_server::DEFAULT_GROUP_HEARTBEAT_INTERVAL,
        metadata: Some(1),
        coordinator: Some(2),
        max_producers: lotse_server::DEFAULT_MAX_PRODUCERS,
        producer_expiration: lotse_server::DEFAULT_PRODUCER_EXPIRATION,
        retention: lotse_server::RetentionConfig::keep(),
        join: false,
        auth,
    };
    let rt = TokioRuntime::new(root.join("node-1")).with_tls(ca.node_tls(1, &["127.0.0.1"]).unwrap());
    std::fs::create_dir_all(rt.root()).unwrap();
    let node = Node::start(rt, cfg, |_| Nothing).await.unwrap();
    let bootstrap = vec![client_addr];

    // Bob may view the shop's resources, nothing else.
    let admin = Client::new(
        TokioRuntime::new(&root).with_tls(ca.client_tls(None).unwrap()),
        ClientConfig {
            retry_for: Duration::from_secs(20),
            credentials: Some(Credentials::Scram { user: "admin".into(), password: "admin-pw".into() }),
            ..ClientConfig::new(bootstrap.clone())
        },
    );
    let viewer = RoleBindingData {
        subject_kind: subject_kind::USER,
        subject: "bob".into(),
        role: role::VIEWER,
        tenant: "shop".into(),
        prefix: String::new(),
    };
    admin.alter_role_bindings(vec![viewer], Vec::new()).await.expect("binds a role");

    // One cluster behind every console below: each principal gets a seat of
    // its own.
    let rt = TokioRuntime::new(&root).with_tls(ca.client_tls(None).unwrap());
    let native = Arc::new(NativeCluster::with_runtime("local", bootstrap.clone(), rt));
    let cluster = || -> Vec<Arc<dyn Cluster>> { vec![native.clone()] };
    let store = || Arc::new(MetricsStore::new(Duration::from_secs(3600), Duration::from_secs(10)));

    // --- OpenID Connect ---
    let login = OidcLogin {
        issuer: issuer.clone(),
        client_id: "lotse-console".into(),
        client_secret: None,
        redirect_url: "http://127.0.0.1:8080/auth/callback".into(),
        scopes: vec!["openid".into(), "offline_access".into()],
        principal_claim: "sub".into(),
        ca_file: None,
        session_key_file: None,
        session_hours: 12,
    };
    // A console signing users in, with its session key: a new one is what a
    // restarted console is.
    let console = |key: u8| {
        let sessions = Sessions::new(login.clone(), TokioRuntime::new(&root), SessionKey::from_bytes(&[key; 32]));
        let state = AppState::new(cluster(), AuthConfig::Oidc(login.clone()), store(), Duration::from_secs(1));
        router(state.with_sessions(Arc::new(sessions)))
    };
    let app = console(1);

    let r = call(&app, "GET", "/api/v1/me", None, None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "nobody signed in: {r:?}");

    // The browser goes to the identity provider with a PKCE challenge…
    let r = call(&app, "GET", "/auth/login?return_to=/clusters/local/streams", None, None).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    let set = r.sign_in.expect("the sign-in travels in a cookie");
    assert!(set.contains("Path=/auth") && set.contains("HttpOnly") && set.contains("Max-Age=600"), "{set}");
    let signing_in = pair(&set);
    let to = r.location.unwrap();
    assert!(to.starts_with(&format!("{issuer}/authorize?")), "{to}");
    let q = query(&to);
    assert_eq!((q["response_type"].as_str(), q["client_id"].as_str()), ("code", "lotse-console"));
    assert_eq!((q["code_challenge_method"].as_str(), q["scope"].as_str()), ("S256", "openid offline_access"));
    assert_eq!(q["redirect_uri"], "http://127.0.0.1:8080/auth/callback");

    // …which signs Bob in and sends him back with a code. His first token
    // is about to expire, so the console renews it at once.
    let now = TokioRuntime::new(&root).unix_time().as_secs();
    let claims =
        |nonce: &str| json!({"iss": issuer, "aud": "lotse-console", "sub": "bob", "name": "Bob", "nonce": nonce});
    {
        let mut t = token_endpoint.lock().unwrap();
        t.answers.push_back(json!({
            "access_token": idp.token("bob", &[], "lotse", now + 10),
            "id_token": id_token(claims(&q["nonce"])),
            "refresh_token": "refresh-1",
            "expires_in": 10,
        }));
        t.answers.push_back(json!({"access_token": idp.token("bob", &[], "lotse", now + 300), "expires_in": 300}));
    }
    let r = call(&app, "GET", "/auth/callback?code=c1&state=forged", Some(&signing_in), None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "a state this sign-in did not send");
    let callback = format!("/auth/callback?code=c1&state={}", q["state"]);
    let r = call(&app, "GET", &callback, None, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "a callback without its sign-in cookie");
    let r = call(&console(2), "GET", &callback, Some(&signing_in), None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "a sign-in sealed with another key");
    // The console restarts while Bob is at the provider: the sign-in is in
    // his cookie, so it finishes all the same.
    let app = console(1);
    let r = call(&app, "GET", &callback, Some(&signing_in), None).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER, "{r:?}");
    assert_eq!(r.location.as_deref(), Some("/clusters/local/streams"));
    let set = r.set_cookie.unwrap();
    assert!(set.contains("HttpOnly") && set.contains("SameSite=Lax"), "{set}");
    assert!(set.contains("Max-Age=43200"), "kept for the session's lifetime: {set}");
    assert!(r.sign_in.is_some_and(|c| c.contains("Max-Age=0")), "the sign-in cookie is cleared");
    let mut cookie = set.split(';').next().unwrap().to_owned();
    let exchange = token_endpoint.lock().unwrap().forms[0].clone();
    assert_eq!((exchange["grant_type"].as_str(), exchange["code"].as_str()), ("authorization_code", "c1"));
    assert_eq!(
        B64URL.encode(Sha256::digest(exchange["code_verifier"].as_bytes())),
        q["code_challenge"],
        "the verifier matches the challenge"
    );
    let r = call(&app, "GET", &callback, Some(&signing_in), None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "a sign-in is finished once, also with a copy of its cookie");

    let r = call(&app, "GET", "/api/v1/me", Some(&cookie), None).await;
    assert_eq!((r.json["principal"].as_str(), r.json["display_name"].as_str()), (Some("bob"), Some("Bob")), "{r:?}");
    assert_eq!(r.json["auth_mode"], "oidc");
    let renewal = token_endpoint.lock().unwrap().forms.get(1).cloned().expect("the token was renewed");
    assert_eq!((renewal["grant_type"].as_str(), renewal["refresh_token"].as_str()), ("refresh_token", "refresh-1"));

    // The console restarts: the session is in the cookie, so Bob stays
    // signed in. The provider rotates his refresh token on the way, and the
    // browser gets the new cookie.
    let app = console(1);
    token_endpoint.lock().unwrap().answers.push_back(json!({
        "access_token": idp.token("bob", &[], "lotse", now + 300),
        "refresh_token": "refresh-2",
        "expires_in": 300,
    }));
    // Two requests at once: one renewal, so the rotating token is spent once.
    let (r, twin) = tokio::join!(
        call(&app, "GET", "/api/v1/me", Some(&cookie), None),
        call(&app, "GET", "/api/v1/me", Some(&cookie), None)
    );
    assert_eq!((r.status, r.json["principal"].as_str()), (StatusCode::OK, Some("bob")), "after a restart: {r:?}");
    assert_eq!(twin.status, StatusCode::OK, "{twin:?}");
    let renewal = token_endpoint.lock().unwrap().forms.last().cloned().unwrap();
    assert_eq!(renewal["refresh_token"], "refresh-1", "renewed from the cookie");
    let rotated = r.set_cookie.expect("a rotated refresh token reaches the browser");
    let old_cookie = std::mem::replace(&mut cookie, rotated.split(';').next().unwrap().to_owned());
    assert_ne!(old_cookie, cookie);
    let forms = token_endpoint.lock().unwrap().forms.len();
    for c in [&cookie, &old_cookie] {
        let r = call(&app, "GET", "/api/v1/me", Some(c), None).await;
        assert_eq!(r.status, StatusCode::OK, "{r:?}");
    }
    assert_eq!(token_endpoint.lock().unwrap().forms.len(), forms, "renewed once, the rotated token used once");

    // A cookie sealed with another key, or changed, holds no session.
    let r = call(&console(2), "GET", "/api/v1/me", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "another console's key");
    assert!(r.set_cookie.is_some_and(|c| c.contains("Max-Age=0")), "and the browser forgets it");
    // Change the last character to another one: appending "A" to a cookie
    // that ended in "A" left it as it was, and the test failed one run in 64.
    let last = if cookie.ends_with('A') { 'B' } else { 'A' };
    let tampered = format!("{}{last}", &cookie[..cookie.len() - 1]);
    let r = call(&app, "GET", "/api/v1/me", Some(&tampered), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "a changed cookie");

    // Bob sees the shop's stream and tenant, nothing of the default tenant,
    // and may not create anything.
    let r = call(&app, "GET", "/api/v1/clusters/local/streams", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    assert_eq!(names(&r.json), ["orders"]);
    assert_eq!(r.json[0]["tenant"], "shop");
    let r = call(&app, "GET", "/api/v1/clusters/local/tenants", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    assert_eq!(names(&r.json["tenants"]), ["shop"]);
    assert_eq!(r.json["bindings"][0]["bindings"][0]["role"], "viewer", "{r:?}");
    let queue = json!({"name": "shop-jobs", "tenant": "shop", "priority_levels": 1, "single_active": false, "create_dlq": false});
    let r = call(&app, "POST", "/api/v1/clusters/local/queues", Some(&cookie), Some(queue.clone())).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "a viewer creates nothing: {r:?}");

    // Carol, signed in at the same time, has no role: she sees nothing,
    // and Bob still sees what he may.
    let r = call(&app, "GET", "/auth/login", None, None).await;
    let carol_in = pair(&r.sign_in.unwrap());
    let q = query(&r.location.unwrap());
    token_endpoint.lock().unwrap().answers.push_back(json!({
        "access_token": idp.token("carol", &[], "lotse", now + 600),
        "id_token": id_token(json!({"iss": issuer, "aud": "lotse-console", "sub": "carol", "nonce": q["nonce"]})),
        "expires_in": 600,
    }));
    let r = call(&app, "GET", &format!("/auth/callback?code=c4&state={}", q["state"]), Some(&carol_in), None).await;
    let carol = r.set_cookie.expect("carol signs in").split(';').next().unwrap().to_owned();
    let r = call(&app, "GET", "/api/v1/clusters/local/streams", Some(&carol), None).await;
    assert_eq!((r.status, names(&r.json)), (StatusCode::OK, Vec::<String>::new()), "{r:?}");
    let r = call(&app, "GET", "/api/v1/clusters/local/streams", Some(&cookie), None).await;
    assert_eq!(names(&r.json), ["orders"], "{r:?}");

    // Signing out ends the session here and at the identity provider.
    let r = call(&app, "POST", "/auth/logout", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::SEE_OTHER);
    assert!(r.location.unwrap().starts_with(&format!("{issuer}/logout?client_id=lotse-console")));
    assert!(r.set_cookie.unwrap().contains("Max-Age=0"));
    let r = call(&app, "GET", "/api/v1/me", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "the session is gone");
    // Even where the provider would still renew it.
    token_endpoint.lock().unwrap().answers.push_back(json!({"access_token": "x", "expires_in": 300}));
    let r = call(&app, "GET", "/api/v1/me", Some(&old_cookie), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "with every cookie it had");
    assert!(token_endpoint.lock().unwrap().answers.pop_front().is_some(), "not even asked");
    let revoked = token_endpoint.lock().unwrap().revoked.clone();
    assert_eq!(revoked.len(), 1, "{revoked:?}");
    assert_eq!((revoked[0]["token"].as_str(), revoked[0]["token_type_hint"].as_str()), ("refresh-2", "refresh_token"));
    // A console restarted since cannot renew it either: the provider
    // refuses the revoked token (here: it has no answer).
    let r = call(&console(1), "GET", "/api/v1/me", Some(&cookie), None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "revoked at the provider");

    // ID tokens for another sign-in, another client or from another issuer
    // are refused.
    let mut wrong_nonce = claims("");
    wrong_nonce["nonce"] = json!("someone-else's-nonce");
    let mut wrong_aud = claims("");
    wrong_aud["aud"] = json!("another-app");
    let mut wrong_iss = claims("");
    wrong_iss["iss"] = json!("https://elsewhere.example");
    for (what, bad) in [("nonce", wrong_nonce), ("aud", wrong_aud), ("iss", wrong_iss)] {
        let r = call(&app, "GET", "/auth/login", None, None).await;
        let signing_in = pair(&r.sign_in.unwrap());
        let q = query(&r.location.unwrap());
        let mut bad = bad;
        if what != "nonce" {
            bad["nonce"] = json!(q["nonce"]);
        }
        token_endpoint.lock().unwrap().answers.push_back(json!({
            "access_token": idp.token("bob", &[], "lotse", now + 300),
            "id_token": id_token(bad),
            "expires_in": 300,
        }));
        let r =
            call(&app, "GET", &format!("/auth/callback?code=c2&state={}", q["state"]), Some(&signing_in), None).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "wrong {what}: {r:?}");
        assert!(r.set_cookie.is_none(), "wrong {what}");
    }

    // A sign-in returns only to a path on this console.
    for elsewhere in ["https://evil.example/", "//evil.example/"] {
        let r = call(&app, "GET", &format!("/auth/login?return_to={elsewhere}"), None, None).await;
        let signing_in = pair(&r.sign_in.unwrap());
        let q = query(&r.location.unwrap());
        token_endpoint.lock().unwrap().answers.push_back(json!({
            "access_token": idp.token("bob", &[], "lotse", now + 300),
            "id_token": id_token(claims(&q["nonce"])),
            "expires_in": 300,
        }));
        let r =
            call(&app, "GET", &format!("/auth/callback?code=c3&state={}", q["state"]), Some(&signing_in), None).await;
        assert_eq!(r.location.as_deref(), Some("/"), "not to {elsewhere}");
    }

    // --- Dev mode with a password ---
    let dev_with = |user: &str, password: &str, store: Arc<MetricsStore>| {
        let auth = AuthConfig::Dev { user: user.into(), password: Some(password.into()) };
        router(AppState::new(cluster(), auth, store, Duration::from_secs(1)))
    };
    let dev = |password: &str| dev_with("admin", password, store());
    let app = dev("admin-pw");
    let r = call(&app, "GET", "/api/v1/clusters/local/streams", None, None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    assert_eq!(names(&r.json), ["audit", "orders"], "the admin sees every tenant");
    let r = call(&app, "POST", "/api/v1/clusters/local/queues", None, Some(queue)).await;
    assert_eq!(r.status, StatusCode::CREATED, "{r:?}");
    let mut created = Value::Null;
    for _ in 0..50 {
        created = call(&app, "GET", "/api/v1/clusters/local/queues", None, None).await.json;
        if names(&created) == ["shop-jobs"] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(created[0]["tenant"], "shop", "created in the tenant asked for: {created}");
    // Another password is checked anew, not served from the admin's seat.
    let r = call(&dev("wrong"), "GET", "/api/v1/clusters/local/streams", None, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{r:?}");
    assert!(r.json["message"].as_str().unwrap().contains("did not accept your sign-in"), "{r:?}");

    // --- Users and access, as the signed-in user (ADR 0089) ---
    // The browser derives the credential; the console passes it on.
    let b64 = base64::engine::general_purpose::STANDARD;
    let derived = |password: &str| {
        let c = ScramCredential::new(password, b"sixteen byte salt", 4096);
        json!({
            "salt": b64.encode(&c.salt),
            "iterations": c.iterations,
            "stored_key": b64.encode(&c.stored_key),
            "server_key": b64.encode(&c.server_key),
        })
    };
    let with_name = |name: &str, password: &str| {
        let mut body = derived(password);
        body["name"] = json!(name);
        body
    };
    let admin_app = dev("admin-pw");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/users", None, Some(with_name("carol", "carol-pw"))).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/users", None, Some(with_name("carol", "other"))).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "a second create is refused: {r:?}");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/users", None, Some(with_name("a,b", "pw"))).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "a name SCRAM cannot carry: {r:?}");
    let r = call(&admin_app, "GET", "/api/v1/clusters/local/users", None, None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let carol = r.json["users"].as_array().unwrap().iter().find(|u| u["name"] == "carol").cloned();
    let carol = carol.unwrap_or_else(|| panic!("carol is listed: {}", r.json));
    assert_eq!((carol["source"].as_str(), carol["mechanism"].as_str()), (Some("runtime"), Some("SCRAM-SHA-512")));
    assert!(!r.json.to_string().contains(&b64.encode(b"sixteen byte salt")), "no key material in the list");

    // The password she was given works; she is nobody's admin.
    let carol_app = dev_with("carol", "carol-pw", store());
    let r = call(&carol_app, "GET", "/api/v1/clusters/local/streams", None, None).await;
    assert_eq!(r.status, StatusCode::OK, "she signs in with the derived credential: {r:?}");
    let r = call(&carol_app, "GET", "/api/v1/clusters/local/users", None, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "only cluster admins list users: {r:?}");
    let r = call(&carol_app, "POST", "/api/v1/clusters/local/users", None, Some(with_name("mallory", "pw"))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "the broker decides, not the console: {r:?}");
    let r = call(&carol_app, "GET", "/api/v1/clusters/local/bindings", None, None).await;
    assert_eq!((r.status, r.json["can_edit"].clone()), (StatusCode::OK, json!(false)), "{r:?}");
    let binding = json!({"subject_kind": "user", "subject": "carol", "role": "viewer", "tenant": "shop", "prefix": ""});
    let r = call(&carol_app, "POST", "/api/v1/clusters/local/bindings", None, Some(binding.clone())).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "she cannot bind herself: {r:?}");

    // A new password replaces the old one.
    let r =
        call(&admin_app, "PUT", "/api/v1/clusters/local/users/carol/credential", None, Some(derived("new-pw"))).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    // A console seat keeps its open session, so the broker is asked afresh.
    let signs_in = |user: &str, password: &str| {
        let client = Client::new(
            TokioRuntime::new(&root).with_tls(ca.client_tls(None).unwrap()),
            ClientConfig {
                retry_for: Duration::from_secs(5),
                credentials: Some(Credentials::Scram { user: user.into(), password: password.into() }),
                ..ClientConfig::new(bootstrap.clone())
            },
        );
        async move { client.who_am_i().await.is_ok() }
    };
    assert!(!signs_in("carol", "carol-pw").await, "the old password is refused");
    assert!(signs_in("carol", "new-pw").await, "the new one works");

    // Bindings: add, list with their source, remove.
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/bindings", None, Some(binding.clone())).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    assert_eq!(r.json["message"], "carol is now viewer in shop.");
    let r = call(&admin_app, "GET", "/api/v1/clusters/local/bindings", None, None).await;
    assert_eq!(r.json["can_edit"], true, "{r:?}");
    let listed = |r: &Reply| {
        r.json["bindings"].as_array().unwrap().iter().any(|b| b["subject"] == "carol" && b["source"] == "runtime")
    };
    assert!(listed(&r), "{r:?}");
    let bad = json!({"subject_kind": "user", "subject": "carol", "role": "viewer", "tenant": "", "prefix": ""});
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/bindings", None, Some(bad)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "a tenant role needs a tenant: {r:?}");
    let r = call(&admin_app, "DELETE", "/api/v1/clusters/local/bindings", None, Some(binding)).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let r = call(&admin_app, "GET", "/api/v1/clusters/local/bindings", None, None).await;
    assert!(!listed(&r), "{r:?}");

    // A certificate is revoked by issuer and serial, by a cluster admin only.
    let ends = lotse_console::time::now_ms() + 86_400_000;
    let cert =
        json!({"issuer": "ab".repeat(32), "serial": "0102", "not_after_ms": ends, "reason": "lost", "confirm": "0102"});
    let r = call(&carol_app, "POST", "/api/v1/clusters/local/certificates/revoke", None, Some(cert.clone())).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "only cluster admins revoke certificates: {r:?}");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/certificates/revoke", None, Some(cert.clone())).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let expired =
        json!({"issuer": "ab".repeat(32), "serial": "0102", "not_after_ms": 1, "reason": "", "confirm": "0102"});
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/certificates/revoke", None, Some(expired)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "an expired certificate needs no revocation: {r:?}");

    // The inventory lists them for a cluster admin only.
    let r = call(&admin_app, "GET", "/api/v1/clusters/local/certificates", None, None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let listed =
        r.json["certificates"].as_array().unwrap().iter().any(|c| c["serial"] == "0102" && c["state"] == "revoked");
    assert!(listed, "the certificate revoked above is listed: {r:?}");
    let r = call(&carol_app, "GET", "/api/v1/clusters/local/certificates", None, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{r:?}");

    // A certificate is asked for with a request the browser made, by a
    // cluster admin only; it waits for the CA holder.
    let csr = "MIHKMHICAQAwEDEOMAwGA1UEAwwFYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARnKUSI4DRkymlj6IpyYHjl9g4fkO/KxbIv1mwPnPz6Uzzgu6zhq+BZCN0Ag5wvdlBj3SbgAATmU4mbaITAsGl6oAAwCgYIKoZIzj0EAwIDSAAwRQIgQKvHGNTyKosN+lfdcw26mTmMr9BK5y34EAESHttDA1kCIQCFfhWHtobQFEY8SXm5AquJHSijZHVx/Xps8gbif2hQvQ==";
    let ask = json!({"principal": "alice", "csr": csr});
    let r = call(&carol_app, "POST", "/api/v1/clusters/local/certificate-requests", None, Some(ask.clone())).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "only cluster admins ask: {r:?}");
    let r = call(&carol_app, "GET", "/api/v1/clusters/local/certificate-requests", None, None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{r:?}");
    let bad = json!({"principal": "../alice", "csr": csr});
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/certificate-requests", None, Some(bad)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "a name the CA holder could not sign: {r:?}");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/certificate-requests", None, Some(ask)).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let r = call(&admin_app, "GET", "/api/v1/clusters/local/certificate-requests", None, None).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let waiting = r.json["requests"].as_array().unwrap().iter().any(|q| {
        q["principal"] == "alice"
            && q["state"] == "pending"
            && q["key_fingerprint"] == "23b66907cf9a2d8b55976d1142b8ec5c5a2b85b18685722603440f15c2da42c5"
            && q["certificate"] == ""
    });
    assert!(waiting, "the request waits for the CA holder: {r:?}");

    // Revoking and deleting name the subject again.
    let revoke = |confirm: &str| json!({"subject": "carol", "group": false, "confirm": confirm});
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/sessions/revoke", None, Some(revoke("carl"))).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{r:?}");
    let r = call(&admin_app, "POST", "/api/v1/clusters/local/sessions/revoke", None, Some(revoke("carol"))).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    let r =
        call(&admin_app, "DELETE", "/api/v1/clusters/local/users/carol", None, Some(json!({"confirm": "carl"}))).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{r:?}");
    let r =
        call(&admin_app, "DELETE", "/api/v1/clusters/local/users/carol", None, Some(json!({"confirm": "carol"}))).await;
    assert_eq!(r.status, StatusCode::OK, "{r:?}");
    assert!(!signs_in("carol", "new-pw").await, "a deleted user cannot sign in");

    // --- The metrics sampler, with client credentials ---
    // As a cluster viewer it sees every tenant; Bob, with a password now,
    // only the shop.
    let sampler_sees = RoleBindingData {
        subject_kind: subject_kind::USER,
        subject: "console-metrics".into(),
        role: role::CLUSTER_VIEWER,
        tenant: String::new(),
        prefix: String::new(),
    };
    admin.alter_role_bindings(vec![sampler_sees], Vec::new()).await.unwrap();
    admin.set_user_password("bob", "bob-pw").await.unwrap();
    let creds = ServiceCredentials {
        issuer: issuer.clone(),
        client_id: "lotse-console-metrics".into(),
        client_secret: Secret::new("metrics-secret"),
        scope: None,
    };
    let sampler =
        SamplerLogin::Service(Arc::new(ServiceLogin::new("console-metrics", creds, TokioRuntime::new(&root))));
    token_endpoint.lock().unwrap().answers.push_back(json!({
        "access_token": idp.token("console-metrics", &[], "lotse", now + 600),
        "expires_in": 600,
    }));
    let history = store();
    let sample = || async {
        let who = sampler.identity().await.expect("the sampler signs in");
        history.record("local", lotse_console::time::now_ms(), &native.chart_sample(&who).await.expect("samples"));
    };
    sample().await;
    let grant = token_endpoint.lock().unwrap().forms.last().cloned().unwrap();
    assert_eq!(
        (grant["grant_type"].as_str(), grant["client_id"].as_str(), grant["client_secret"].as_str()),
        ("client_credentials", "lotse-console-metrics", "metrics-secret")
    );
    let forms = token_endpoint.lock().unwrap().forms.len();
    // Traffic: a little to the shop's stream, much more to the default
    // tenant's.
    admin.produce("orders", 0, batch("order")).await.unwrap();
    for i in 0..20 {
        admin.produce("audit", 0, batch(&format!("audit entry {i} {}", "x".repeat(200)))).await.unwrap();
    }
    tokio::time::sleep(lotse_console::cluster::native::RATE_WINDOW + Duration::from_millis(300)).await;
    sample().await;
    assert_eq!(token_endpoint.lock().unwrap().forms.len(), forms, "the token is used until it is about to expire");
    assert!(history.resources("local") >= 2, "the sampler saw both tenants' streams");

    let last_in = |app: axum::Router| async move {
        let r = call(&app, "GET", "/api/v1/clusters/local/metrics?series=in_bytes_per_sec&range=15m", None, None).await;
        assert_eq!(r.status, StatusCode::OK, "{r:?}");
        r.json["points"].as_array().unwrap().last().map_or(0.0, |p| p["v"].as_f64().unwrap())
    };
    let bob = || dev_with("bob", "bob-pw", history.clone());
    let everyone = || dev_with("admin", "admin-pw", history.clone());
    let (bobs, all) = (last_in(bob()).await, last_in(everyone()).await);
    assert!(bobs > 0.0 && all > bobs * 5.0, "Bob's chart holds the shop's traffic only: {bobs} of {all}");
    let p99 = "/api/v1/clusters/local/metrics?series=commit_p99_ms&range=15m";
    assert_eq!(call(&bob(), "GET", p99, None, None).await.status, StatusCode::FORBIDDEN, "cluster-wide");
    assert_eq!(call(&everyone(), "GET", p99, None, None).await.status, StatusCode::OK);
    admin.set_user_password("console-metrics", "m-pw").await.unwrap();
    let viewer = dev_with("console-metrics", "m-pw", history.clone());
    assert_eq!(call(&viewer, "GET", p99, None, None).await.status, StatusCode::OK, "a cluster viewer sees it too");

    drop(admin);
    node.handle().stop();
    node.join().await.unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

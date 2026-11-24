//! `lotse mcp` over stdio (ADR 0134), against a node run from a cluster
//! file on real sockets over TLS: the executable answers `server/discover`
//! and lists the tools with their hints, both without a session (MCP
//! 2026-07-28) and after `initialize` (2025-11-25); a tool runs as the
//! owner of the token in `LOTSE_TOKEN`, so the cluster's checks apply to
//! that owner; without a credential it refuses to start. Its standard
//! output carries nothing but JSON-RPC, even with debug logging on.
//!
//! Over Streamable HTTP (`--listen`): a request without a credential the
//! cluster accepts gets 401 and runs no tool; a token and a password each
//! act as their own principal, never as the other; a revoked token fails on
//! its next request; concurrent requests with one credential sign in once
//! (the node's `lotse_token_sign_ins_total`); plain HTTP off loopback is
//! refused; a browser `Origin` is refused unless allowed; HTTPS with the
//! certificate given.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use lotse_client::{Client, ClientConfig};
use lotse_node::config::ClusterFile;
use lotse_rt_tokio::{TlsConfig, TokioRuntime};
use serde_json::{Value, json};

/// As in the other tests of this crate: a free port below the ephemeral
/// range.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1_200) as u16 * 10 + 3;
    loop {
        let port = 20_000 + (start + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn lotse(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lotse")).args(args).output().expect("the lotse executable runs")
}

fn ok(out: std::process::Output) -> String {
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// A one-node cluster run from a cluster file, with a client certificate
/// for Alice, a cluster admin.
struct Cluster {
    root: PathBuf,
    config: PathBuf,
    client_addr: String,
    /// The node's operator endpoint, plain HTTP on loopback.
    metrics_addr: String,
    stop: tokio::sync::oneshot::Sender<()>,
    node: tokio::task::JoinHandle<()>,
}

impl Cluster {
    async fn start(name: &str) -> Cluster {
        let root = std::env::temp_dir().join(format!("lotse-node-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let r = root.display();
        let client_addr = format!("127.0.0.1:{}", free_port());
        let metrics_addr = format!("127.0.0.1:{}", free_port());
        let text = format!(
            "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
             cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n\
             [disk]\nfree_limit = 0\n\n[auth]\nadmins = [\"alice\"]\n\n\
             [[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"{client_addr}\"\n\
             metrics = \"{metrics_addr}\"\n",
            free_port(),
        );
        let config = root.join("cluster.toml");
        std::fs::write(&config, &text).unwrap();
        let c = config.to_str().unwrap();
        ok(lotse(&["certs", "--config", c]));
        let dir = root.join("clients");
        ok(lotse(&["certs", "--config", c, "--client", "alice", "--out", dir.to_str().unwrap()]));
        let file = ClusterFile::load(&config).unwrap();
        let (stop, rx) = tokio::sync::oneshot::channel::<()>();
        let node = tokio::spawn(async move {
            lotse_node::run(&file, 1, false, async {
                let _ = rx.await;
            })
            .await
            .unwrap();
        });
        let cluster = Cluster { root, config, client_addr, metrics_addr, stop, node };
        // Tokens need cluster version 0.1, which a new cluster takes once
        // its node has reported its release.
        let alice = cluster.alice();
        for _ in 0..300 {
            let version = alice.describe_upgrade().await.ok().and_then(|d| d.cluster_version);
            if version.is_some_and(|v| v >= lotse_server::version::TOKENS_SINCE) {
                return cluster;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the cluster never took cluster version 0.1");
    }

    fn client(&self, file: &str) -> PathBuf {
        self.root.join("clients").join(file)
    }

    fn alice(&self) -> Client<TokioRuntime> {
        let ca = self.root.join("tls/ca.pem");
        let identity = Some((self.client("alice.pem"), self.client("alice.key")));
        let rt = TokioRuntime::new(std::env::temp_dir()).with_tls(TlsConfig::from_files(&ca, identity).unwrap());
        Client::new(
            rt,
            ClientConfig { retry_for: Duration::from_secs(5), ..ClientConfig::new(vec![self.client_addr.clone()]) },
        )
    }

    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.node.await.unwrap();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// `lotse mcp` as an agent runs it: JSON-RPC lines in on stdin, out on
/// stdout, read by a thread so a test waits for a line with a deadline.
/// Killed if the test fails before it ends.
struct Mcp {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    seen: Vec<String>,
}

/// The environment variables `lotse mcp` reads credentials from, removed
/// from what the child inherits so only what a test sets counts.
const CREDENTIAL_VARS: [&str; 3] = ["LOTSE_TOKEN", "LOTSE_USER", "LOTSE_PASSWORD"];

fn mcp_command(config: &Path, args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lotse"));
    cmd.arg("mcp").arg("--config").arg(config).args(args);
    for v in CREDENTIAL_VARS {
        cmd.env_remove(v);
    }
    // Debug logs on: they must go to stderr, never to stdout.
    cmd.env("LOTSE_LOG", "debug");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd
}

impl Mcp {
    fn spawn(config: &Path, args: &[&str], env: &[(&str, &str)]) -> Mcp {
        Mcp::spawn_logging(config, args, env, Stdio::null())
    }

    /// As [`Mcp::spawn`], with its log (debug) going to `stderr`.
    fn spawn_logging(config: &Path, args: &[&str], env: &[(&str, &str)], stderr: Stdio) -> Mcp {
        let mut child = mcp_command(config, args, env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("the lotse executable runs");
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        // A real process's pipe, outside any simulation.
        #[allow(clippy::disallowed_methods)]
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        let stdin = child.stdin.take();
        Mcp { child, stdin, lines, seen: Vec::new() }
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    /// The next line on stdout, which must be a JSON-RPC 2.0 message.
    fn next(&mut self) -> Value {
        let line = self.lines.recv_timeout(Duration::from_secs(30)).expect("lotse mcp answers within 30 s");
        self.seen.push(line.clone());
        let v: Value = serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON on stdout ({e}): {line}"));
        assert_eq!(v["jsonrpc"], "2.0", "not JSON-RPC on stdout: {line}");
        v
    }

    /// Sends a request and returns the answer with its id.
    fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let v = self.next();
            if v["id"] == json!(id) {
                return v;
            }
        }
    }

    /// Closes stdin, waits for the process to end and checks that nothing
    /// but JSON-RPC came out on stdout meanwhile.
    fn finish(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().unwrap();
        while let Ok(line) = self.lines.recv_timeout(Duration::from_secs(5)) {
            let v: Value = serde_json::from_str(&line).unwrap_or_else(|e| panic!("not JSON on stdout ({e}): {line}"));
            assert_eq!(v["jsonrpc"], "2.0", "not JSON-RPC on stdout: {line}");
        }
        assert!(status.success(), "lotse mcp ends cleanly when stdin closes: {status}");
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The `_meta` a 2026-07-28 request carries in place of a session.
fn meta() -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    })
}

fn tool_names(listed: &Value) -> Vec<String> {
    listed["result"]["tools"]
        .as_array()
        .unwrap_or_else(|| panic!("no tools: {listed}"))
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

fn tool<'a>(listed: &'a Value, name: &str) -> &'a Value {
    listed["result"]["tools"].as_array().unwrap().iter().find(|t| t["name"] == name).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_takes_a_zone() {
    let cluster = Cluster::start("mcp-zone").await;
    let (cert, key) = (cluster.client("alice.pem"), cluster.client("alice.key"));
    let (cert, key) = (cert.to_str().unwrap().to_string(), key.to_str().unwrap().to_string());
    let config = cluster.config.clone();
    tokio::task::spawn_blocking(move || {
        // The flag, and LOTSE_ZONE given to the child alone, not the test
        // process, so no other test sees it.
        let flag = ["--zone", "eu-1", "--cert", cert.as_str(), "--key", key.as_str()];
        let env = ["--cert", cert.as_str(), "--key", key.as_str()];
        for (args, vars) in [(&flag[..], &[][..]), (&env[..], &[("LOTSE_ZONE", "eu-1")][..])] {
            let mut m = Mcp::spawn(&config, args, vars);
            let r = m.request(1, "tools/call", json!({"_meta": meta(), "name": "cluster_overview", "arguments": {}}));
            assert_eq!(r["result"]["isError"], false, "{r}");
            m.finish();
        }
    })
    .await
    .unwrap();
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_answers_discover_and_lists_the_tools() {
    let cluster = Cluster::start("mcp-list").await;
    let (cert, key) = (cluster.client("alice.pem"), cluster.client("alice.key"));
    let (cert, key) = (cert.to_str().unwrap().to_string(), key.to_str().unwrap().to_string());
    let config = cluster.config.clone();
    let expected: Vec<String> = lotse_mcp::Tools::<TokioRuntime>::list().iter().map(|s| s.name.to_string()).collect();

    let (list, expected) = tokio::task::spawn_blocking(move || {
        let args = ["--cert", cert.as_str(), "--key", key.as_str()];
        // Without a session: discover, then a request with its own _meta.
        let mut m = Mcp::spawn(&config, &args, &[]);
        let found = m.request(1, "server/discover", json!({"_meta": meta()}));
        let versions = found["result"]["supportedVersions"].as_array().unwrap_or_else(|| panic!("{found}"));
        assert!(versions.contains(&json!("2026-07-28")), "{found}");
        assert!(versions.contains(&json!("2025-11-25")), "{found}");
        assert!(found["result"]["capabilities"]["tools"].is_object(), "{found}");
        let listed = m.request(2, "tools/list", json!({"_meta": meta()}));
        assert_eq!(tool_names(&listed), expected, "every tool, in the order Tools::list gives");
        m.finish();

        // With a session, as a client of 2025-11-25 opens one.
        let mut m = Mcp::spawn(&config, &args, &[]);
        let init = m.request(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            }),
        );
        assert_eq!(init["result"]["protocolVersion"], "2025-11-25", "{init}");
        assert_eq!(init["result"]["serverInfo"]["name"], "lotse", "{init}");
        m.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        let legacy = m.request(2, "tools/list", json!({}));
        assert_eq!(tool_names(&legacy), expected);
        m.finish();
        (listed, expected)
    })
    .await
    .unwrap();

    // The hints follow the tools' own marks.
    for spec in lotse_mcp::Tools::<TokioRuntime>::list() {
        let t = tool(&list, spec.name);
        let hints = &t["annotations"];
        assert_eq!(hints["destructiveHint"], json!(spec.destructive), "{t}");
        assert_eq!(hints["readOnlyHint"], json!(spec.read_only), "{t}");
        assert_eq!(t["inputSchema"], spec.input_schema, "{t}");
        assert_eq!(t["description"], spec.description, "{t}");
    }
    assert_eq!(tool(&list, "delete_resource")["annotations"]["destructiveHint"], true);
    assert_eq!(tool(&list, "cluster_overview")["annotations"]["readOnlyHint"], true);
    assert!(!expected.is_empty());
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_calls_a_tool_as_the_token_owner() {
    let cluster = Cluster::start("mcp-token").await;
    // Dave may administer the default tenant's resources named dave-*.
    let alice = cluster.alice();
    let binding = lotse_proto::RoleBindingData {
        subject_kind: lotse_proto::subject_kind::USER,
        subject: "dave".into(),
        role: lotse_proto::role::TENANT_ADMIN,
        tenant: lotse_server::DEFAULT_TENANT.into(),
        prefix: "dave-".into(),
    };
    alice.alter_role_bindings(vec![binding], Vec::new()).await.expect("binds dave");
    // A cluster admin makes a token in Dave's name.
    let token = alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token");
    let config = cluster.config.clone();

    let text = token.text.clone();
    tokio::task::spawn_blocking(move || {
        let mut m = Mcp::spawn(&config, &[], &[("LOTSE_TOKEN", text.as_str())]);
        let call = |m: &mut Mcp, id: u64, name: &str, args: Value| {
            m.request(id, "tools/call", json!({"_meta": meta(), "name": name, "arguments": args}))
        };

        let made = call(&mut m, 1, "create_stream", json!({"name": "dave-events", "partitions": 1}));
        let r = &made["result"];
        assert_eq!(r["isError"], false, "{made}");
        assert_eq!(r["structuredContent"]["name"], "dave-events", "{made}");
        let content = r["content"].as_array().unwrap_or_else(|| panic!("{made}"));
        assert_eq!(content.len(), 1, "one text item: {made}");
        assert_eq!(content[0]["type"], "text");
        assert!(content[0]["text"].as_str().unwrap().contains("Created stream dave-events"), "{made}");

        // Outside Dave's prefix the cluster refuses Dave, and the refusal
        // is a tool result the agent reads, not a protocol error.
        let refused = call(&mut m, 2, "create_stream", json!({"name": "orders", "partitions": 1}));
        let r = &refused["result"];
        assert_eq!(r["isError"], true, "{refused}");
        assert_eq!(r["structuredContent"]["error"]["code"], "UNAUTHORIZED", "{refused}");
        assert!(r["content"][0]["text"].as_str().unwrap().starts_with("UNAUTHORIZED"), "{refused}");

        // The session the tools use is Dave's, signed in with the token.
        let sessions = call(&mut m, 3, "list_sessions", json!({}));
        let listed = sessions["result"]["structuredContent"]["sessions"]
            .as_array()
            .unwrap_or_else(|| panic!("{sessions}"))
            .clone();
        assert!(!listed.is_empty(), "{sessions}");
        assert!(listed.iter().all(|s| s["name"] == "dave"), "Dave sees only his own: {sessions}");
        assert!(listed.iter().any(|s| s["token_id"] == json!(format!("{:016x}", token.id))), "{sessions}");

        // An unknown tool is a tool error too.
        let unknown = call(&mut m, 4, "no_such_tool", json!({}));
        assert_eq!(unknown["result"]["isError"], true, "{unknown}");
        m.finish();
    })
    .await
    .unwrap();

    let known = alice.metadata(&["dave-events", "orders"]).await.expect("metadata");
    let error = |name: &str| known.resources.iter().find(|r| r.name == name).map(|r| r.error);
    assert_eq!(error("dave-events"), Some(lotse_proto::ErrorCode::NONE), "the stream exists");
    assert_eq!(error("orders"), Some(lotse_proto::ErrorCode::UNKNOWN_RESOURCE));
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_refuses_to_start_without_a_credential() {
    let cluster = Cluster::start("mcp-none").await;
    let config = cluster.config.clone();
    let run = move |args: &[&str], env: &[(&str, &str)]| {
        let mut cmd = mcp_command(&config, args, env);
        let out = cmd.stdin(Stdio::null()).output().expect("the lotse executable runs");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    };
    let (none, user_alone) =
        tokio::task::spawn_blocking(move || (run(&[], &[]), run(&[], &[("LOTSE_USER", "dave")]))).await.unwrap();

    let (code, stdout, stderr) = none;
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stdout.is_empty(), "nothing on stdout: {stdout}");
    for way in ["LOTSE_TOKEN", "LOTSE_USER", "LOTSE_PASSWORD", "--cert", "--key"] {
        assert!(stderr.contains(way), "the message names {way}: {stderr}");
    }
    let (code, stdout, stderr) = user_alone;
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stdout.is_empty(), "{stdout}");
    assert!(stderr.contains("LOTSE_PASSWORD"), "{stderr}");
    cluster.stop().await;
}

/// A token owner Dave and a token of his; `lotse mcp` is given it in the
/// environment.
async fn daves_token(cluster: &Cluster) -> lotse_client::CreatedToken {
    let alice = cluster.alice();
    bind_own_prefix(&alice, "dave").await;
    alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stdio_never_logs_the_token_even_at_debug() {
    let cluster = Cluster::start("mcp-token-log").await;
    let token = daves_token(&cluster).await;
    let config = cluster.config.clone();
    let log = cluster.root.join("stdio.log");
    let (text, file) = (token.text.clone(), log.clone());
    tokio::task::spawn_blocking(move || {
        let stderr = Stdio::from(std::fs::File::create(&file).unwrap());
        let mut m = Mcp::spawn_logging(&config, &[], &[("LOTSE_TOKEN", text.as_str())], stderr);
        m.request(
            1,
            "initialize",
            json!({"protocolVersion": "2025-11-25", "capabilities": {}, "clientInfo": {"name": "test", "version": "1"}}),
        );
        m.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        let r = m.request(2, "tools/call", json!({"name": "list_sessions", "arguments": {}}));
        assert_eq!(r["result"]["isError"], false, "signed in with the token: {r}");
        m.finish();
    })
    .await
    .unwrap();
    let logged = std::fs::read_to_string(&log).unwrap();
    assert!(logged.contains("serving MCP on stdio"), "the log is written, at debug: {logged}");
    let secret = token.text.rsplit('_').next().unwrap();
    for (what, text) in [("token", token.text.as_str()), ("secret", secret)] {
        assert!(!logged.contains(text), "the {what} is in the log:\n{logged}");
    }

    // A token the cluster refuses is not echoed in the failure either.
    let unknown = "lotse_tok_00000000000000ff_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let config = cluster.config.clone();
    let out = tokio::task::spawn_blocking(move || {
        mcp_command(&config, &[], &[("LOTSE_TOKEN", unknown)]).stdin(Stdio::null()).output().unwrap()
    })
    .await
    .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains(unknown) && !stderr.contains(&unknown[unknown.len() - 20..]), "{stderr}");
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_older_client_is_answered_with_the_newest_version_the_server_speaks() {
    let cluster = Cluster::start("mcp-older").await;
    let (cert, key) = (cluster.client("alice.pem"), cluster.client("alice.key"));
    let (cert, key) = (cert.to_str().unwrap().to_string(), key.to_str().unwrap().to_string());
    let config = cluster.config.clone();
    tokio::task::spawn_blocking(move || {
        let args = ["--cert", cert.as_str(), "--key", key.as_str()];
        let mut m = Mcp::spawn(&config, &args, &[]);
        let init = m.request(
            1,
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "old", "version": "1"}}),
        );
        assert_eq!(init["result"]["protocolVersion"], "2025-11-25", "{init}");
        m.finish();
    })
    .await
    .unwrap();
    cluster.stop().await;
}

/// `lotse mcp` with `args`, no cluster behind it: its exit code and stderr.
fn usage(args: &[&str], env: &[(&str, &str)]) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_lotse"));
    cmd.arg("mcp").args(args);
    for v in CREDENTIAL_VARS {
        cmd.env_remove(v);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.stdin(Stdio::null()).output().expect("the lotse executable runs");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn a_token_with_a_password_or_a_user_is_a_usage_error() {
    for env in [
        [("LOTSE_TOKEN", "lotse_tok_x"), ("LOTSE_PASSWORD", "pw")],
        [("LOTSE_TOKEN", "lotse_tok_x"), ("LOTSE_USER", "dave")],
    ] {
        let (code, stdout, stderr) = usage(&["--plaintext-on-loopback", "--bootstrap", "127.0.0.1:1"], &env);
        assert_eq!(code, Some(2), "{stderr}");
        assert!(stdout.is_empty(), "{stdout}");
        assert!(stderr.contains("several"), "{stderr}");
    }
}

#[test]
fn a_contradicting_way_to_reach_the_cluster_is_a_usage_error() {
    // Nothing listens on port 1: were a connection tried, the process would
    // fail with 1 after its retries, not exit 2 at once.
    let creds = [("LOTSE_USER", "dave"), ("LOTSE_PASSWORD", "pw")];
    type Case<'a> = (&'a [&'a str], &'a [(&'a str, &'a str)], &'a str);
    let cases: [Case; 4] = [
        (
            &["--bootstrap", "127.0.0.1:1", "--ca", "ca.pem", "--plaintext-on-loopback"],
            &creds,
            "--plaintext-on-loopback",
        ),
        (
            &["--bootstrap", "127.0.0.1:1", "--plaintext-on-loopback", "--cert", "c.pem", "--key", "c.key"],
            &[],
            "client certificate",
        ),
        (&["--bootstrap", "127.0.0.1:1"], &creds, "no CA"),
        (
            &["--bootstrap", "127.0.0.1:1", "--listen", "127.0.0.1:0", "--ca", "ca.pem", "--plaintext-on-loopback"],
            &[],
            "--plaintext-on-loopback",
        ),
    ];
    for (args, env, says) in cases {
        let (code, stdout, stderr) = usage(args, env);
        assert_eq!(code, Some(2), "{args:?}: {stderr}");
        assert!(stdout.is_empty(), "{stdout}");
        assert!(stderr.contains(says), "{args:?} names {says}: {stderr}");
    }
}

// Streamable HTTP (`lotse mcp --listen`).

/// `lotse mcp --listen` as a shared deployment runs it, on a free loopback
/// port, its log (debug) in a file. Killed when dropped.
struct HttpMcp {
    child: Child,
    addr: String,
    log: PathBuf,
}

impl HttpMcp {
    /// Starts it with `args` after `--listen` and waits until it accepts
    /// connections.
    async fn start(cluster: &Cluster, name: &str, args: &[&str]) -> HttpMcp {
        let addr = format!("127.0.0.1:{}", free_port());
        let log = cluster.root.join(format!("{name}.log"));
        let mut all = vec!["--listen", addr.as_str()];
        all.extend_from_slice(args);
        let child = mcp_command(&cluster.config, &all, &[])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .expect("the lotse executable runs");
        let mut mcp = HttpMcp { child, addr, log };
        for _ in 0..300 {
            if tokio::net::TcpStream::connect(&mcp.addr).await.is_ok() {
                return mcp;
            }
            if let Some(status) = mcp.child.try_wait().unwrap() {
                panic!("lotse mcp --listen ended ({status}): {}", mcp.log_text());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("lotse mcp --listen never accepted a connection: {}", mcp.log_text());
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for HttpMcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// An HTTP answer.
#[derive(Debug)]
struct Answer {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Answer {
    /// Every value of a header, whatever the case of its name.
    fn header(&self, name: &str) -> Vec<&str> {
        self.headers.iter().filter(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str()).collect()
    }

    /// The JSON-RPC message in the body: JSON, or the last `data:` line of
    /// an event stream.
    fn json(&self) -> Value {
        let text = match self.body.trim_start().starts_with('{') {
            true => self.body.clone(),
            false => self
                .body
                .lines()
                .filter_map(|l| l.strip_prefix("data:"))
                .map(str::trim)
                .rfind(|l| l.starts_with('{'))
                .unwrap_or_else(|| panic!("no JSON-RPC message in the answer: {self:?}"))
                .to_string(),
        };
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("not JSON ({e}): {self:?}"))
    }
}

/// Sends one HTTP/1.1 request and reads the answer until the server closes.
async fn exchange<S>(mut s: S, request: String) -> Answer
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    s.write_all(request.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(60), s.read_to_end(&mut out)).await.expect("an answer in 60 s");
    let text = String::from_utf8_lossy(&out).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or_else(|| panic!("no HTTP answer: {text:?}"));
    let mut lines = head.split("\r\n");
    let status = lines.next().unwrap()[9..12].parse().unwrap();
    let headers: Vec<(String, String)> =
        lines.filter_map(|l| l.split_once(':')).map(|(n, v)| (n.trim().to_string(), v.trim().to_string())).collect();
    let chunked =
        headers.iter().any(|(n, v)| n.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked"));
    let body = if chunked { unchunk(body) } else { body.to_string() };
    Answer { status, headers, body }
}

fn unchunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = body.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        body = rest[n..].strip_prefix("\r\n").unwrap_or(&rest[n..]);
    }
    out
}

/// `POST /mcp` with `headers` and a JSON body, plain or over `tls`.
async fn post(addr: &str, tls: Option<Arc<rustls::ClientConfig>>, headers: &[(&str, String)], body: &Value) -> Answer {
    let body = body.to_string();
    let mut request = format!(
        "POST /mcp HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&body);
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    match tls {
        None => exchange(tcp, request).await,
        Some(cfg) => {
            let s = tokio_rustls::TlsConnector::from(cfg)
                .connect(rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap(), tcp)
                .await
                .expect("a TLS handshake with lotse mcp");
            exchange(s, request).await
        }
    }
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

fn basic(user: &str, password: &str) -> String {
    use base64::Engine as _;
    format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}")))
}

/// The headers and body of an MCP 2026-07-28 request.
fn mcp_request(method: &str, name: Option<&str>, params: Value) -> (Vec<(&'static str, String)>, Value) {
    let mut headers = vec![("MCP-Protocol-Version", "2026-07-28".to_string()), ("Mcp-Method", method.to_string())];
    if let Some(n) = name {
        headers.push(("Mcp-Name", n.to_string()));
    }
    (headers, json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
}

/// Calls tool `name` with `args`, with `auth` as the `Authorization` header
/// if given, and `extra` headers.
async fn call_http(
    addr: &str,
    tls: Option<Arc<rustls::ClientConfig>>,
    auth: Option<&str>,
    extra: &[(&str, String)],
    name: &str,
    args: Value,
) -> Answer {
    let (mut headers, body) =
        mcp_request("tools/call", Some(name), json!({"_meta": meta(), "name": name, "arguments": args}));
    if let Some(a) = auth {
        headers.push(("Authorization", a.to_string()));
    }
    headers.extend(extra.iter().cloned());
    post(addr, tls, &headers, &body).await
}

/// A tool call over plain HTTP that the server answered with a tool result:
/// the result.
async fn tool_result(mcp: &HttpMcp, auth: &str, name: &str, args: Value) -> Value {
    let a = call_http(&mcp.addr, None, Some(auth), &[], name, args).await;
    assert_eq!(a.status, 200, "{a:?}");
    let v = a.json();
    assert!(v["result"].is_object(), "{v}");
    v["result"].clone()
}

/// The principals every session the result of `list_sessions` lists.
fn session_names(result: &Value) -> Vec<String> {
    result["structuredContent"]["sessions"]
        .as_array()
        .unwrap_or_else(|| panic!("{result}"))
        .iter()
        .map(|s| s["name"].as_str().unwrap().to_string())
        .collect()
}

/// A 401 that asks for a token or a password, with no MCP answer in it.
fn assert_unauthorized(a: &Answer) {
    assert_eq!(a.status, 401, "{a:?}");
    let challenges = a.header("WWW-Authenticate").join(", ");
    assert!(challenges.contains("Bearer"), "{a:?}");
    assert!(challenges.contains("Basic"), "{a:?}");
    assert!(!a.body.contains("jsonrpc"), "no MCP answer: {a:?}");
}

/// The node's `lotse_token_sign_ins_total` with `result`, from its
/// operator endpoint.
async fn token_sign_ins(cluster: &Cluster, result: &str) -> u64 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(&cluster.metrics_addr).await.unwrap();
    s.write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await.unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    let prefix = format!("lotse_token_sign_ins_total{{node=\"1\",result=\"{result}\"}} ");
    out.lines().find_map(|l| l.strip_prefix(prefix.as_str())).map_or(0, |v| v.trim().parse().unwrap())
}

/// Binds `user` as tenant admin of the default tenant's resources named
/// `<user>-*`.
async fn bind_own_prefix(alice: &Client<TokioRuntime>, user: &str) {
    let binding = lotse_proto::RoleBindingData {
        subject_kind: lotse_proto::subject_kind::USER,
        subject: user.into(),
        role: lotse_proto::role::TENANT_ADMIN,
        tenant: lotse_server::DEFAULT_TENANT.into(),
        prefix: format!("{user}-"),
    };
    alice.alter_role_bindings(vec![binding], Vec::new()).await.expect("binds the user");
}

async fn exists(alice: &Client<TokioRuntime>, name: &str) -> bool {
    let known = alice.metadata(&[name]).await.expect("metadata");
    known.resources.iter().any(|r| r.name == name && r.error == lotse_proto::ErrorCode::NONE)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_needs_a_credential() {
    let cluster = Cluster::start("mcp-http-401").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "erin").await;
    alice.set_user_password("erin", "erin-password").await.expect("erin's password");
    let mcp = HttpMcp::start(&cluster, "mcp", &[]).await;
    let create = json!({"name": "erin-stream", "partitions": 1});

    // No credential, a token the cluster does not know, a wrong password, a
    // header that is no credential at all: 401, and no tool runs.
    let unknown = "lotse_tok_00000000000000ff_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let wrong = basic("erin", "not-her-password");
    for auth in [None, Some(bearer(unknown).as_str()), Some(wrong.as_str()), Some("Basic !!!"), Some("Negotiate x")] {
        let a = call_http(&mcp.addr, None, auth, &[], "create_stream", create.clone()).await;
        assert_unauthorized(&a);
    }
    // tools/list too: nothing is served without a credential.
    let (headers, body) = mcp_request("tools/list", None, json!({"_meta": meta()}));
    assert_unauthorized(&post(&mcp.addr, None, &headers, &body).await);
    assert!(!exists(&alice, "erin-stream").await, "no tool ran");

    // With her password the same call runs.
    let r = tool_result(&mcp, &basic("erin", "erin-password"), "create_stream", create).await;
    assert_eq!(r["isError"], false, "{r}");
    assert!(exists(&alice, "erin-stream").await);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_bearer_token_and_basic_password_both_work() {
    let cluster = Cluster::start("mcp-http-both").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "dave").await;
    bind_own_prefix(&alice, "erin").await;
    let password = "erin-correct-horse";
    alice.set_user_password("erin", password).await.expect("erin's password");
    let token = alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token");
    let mcp = HttpMcp::start(&cluster, "mcp", &[]).await;
    let (dave, erin) = (bearer(&token.text), basic("erin", password));

    // Requests alternate between the two credentials; each runs as its own
    // principal, never as the other.
    for round in 0..2 {
        let r =
            tool_result(&mcp, &dave, "create_stream", json!({"name": format!("dave-{round}"), "partitions": 1})).await;
        assert_eq!(r["isError"], false, "{r}");
        let r =
            tool_result(&mcp, &erin, "create_stream", json!({"name": format!("erin-{round}"), "partitions": 1})).await;
        assert_eq!(r["isError"], false, "{r}");
        let r =
            tool_result(&mcp, &dave, "create_stream", json!({"name": format!("erin-d{round}"), "partitions": 1})).await;
        assert_eq!(r["structuredContent"]["error"]["code"], "UNAUTHORIZED", "dave may not: {r}");
        let r =
            tool_result(&mcp, &erin, "create_stream", json!({"name": format!("dave-e{round}"), "partitions": 1})).await;
        assert_eq!(r["structuredContent"]["error"]["code"], "UNAUTHORIZED", "erin may not: {r}");

        let mine = tool_result(&mcp, &dave, "list_sessions", json!({})).await;
        let names = session_names(&mine);
        assert!(!names.is_empty() && names.iter().all(|n| n == "dave"), "{mine}");
        let listed = mine["structuredContent"]["sessions"].as_array().unwrap();
        assert!(
            listed.iter().all(|s| s["token_id"] == json!(format!("{:016x}", token.id))),
            "signed in with the token: {mine}"
        );
        let hers = tool_result(&mcp, &erin, "list_sessions", json!({})).await;
        let names = session_names(&hers);
        assert!(!names.is_empty() && names.iter().all(|n| n == "erin"), "{hers}");
    }
    for name in ["dave-0", "dave-1", "erin-0", "erin-1"] {
        assert!(exists(&alice, name).await, "{name}");
    }
    for name in ["erin-d0", "dave-e0"] {
        assert!(!exists(&alice, name).await, "{name}");
    }

    // Discovery and the tool list over HTTP, with a credential.
    let (headers, body) = mcp_request("server/discover", None, json!({"_meta": meta()}));
    let mut headers = headers;
    headers.push(("Authorization", erin.clone()));
    let found = post(&mcp.addr, None, &headers, &body).await;
    assert_eq!(found.status, 200, "{found:?}");
    assert!(found.json()["result"]["supportedVersions"].as_array().unwrap().contains(&json!("2026-07-28")));
    let (mut headers, body) = mcp_request("tools/list", None, json!({"_meta": meta()}));
    headers.push(("Authorization", dave.clone()));
    let listed = post(&mcp.addr, None, &headers, &body).await.json();
    assert_eq!(tool_names(&listed).len(), lotse_mcp::Tools::<TokioRuntime>::list().len(), "{listed}");

    // Neither the token, its secret nor the password is logged, though the
    // log is at debug.
    let log = mcp.log_text();
    assert!(!log.is_empty(), "the log is written");
    let secret = token.text.rsplit('_').next().unwrap();
    for (what, text) in [("token", token.text.as_str()), ("secret", secret), ("password", password)] {
        assert!(!log.contains(text), "the {what} is in the log");
    }
    assert!(!log.contains(erin.trim_start_matches("Basic ")), "the Basic credential is in the log");
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_never_logs_a_bearer_token_even_at_debug() {
    let cluster = Cluster::start("mcp-http-token-log").await;
    let token = daves_token(&cluster).await;
    let mcp = HttpMcp::start(&cluster, "mcp", &[]).await;
    let unknown = "lotse_tok_00000000000000ff_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    // One request that is served and one that is refused.
    let r = tool_result(&mcp, &bearer(&token.text), "list_sessions", json!({})).await;
    assert_eq!(r["isError"], false, "{r}");
    assert_unauthorized(&call_http(&mcp.addr, None, Some(&bearer(unknown)), &[], "list_sessions", json!({})).await);

    let log = mcp.log_text();
    assert!(log.contains("listening") || !log.is_empty(), "the log is written at debug");
    let secret = token.text.rsplit('_').next().unwrap();
    for (what, text) in [("token", token.text.as_str()), ("secret", secret), ("refused token", unknown)] {
        assert!(!log.contains(text), "the {what} is in the log:\n{log}");
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revoked_token_fails_on_its_next_request() {
    let cluster = Cluster::start("mcp-http-revoked").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "dave").await;
    let token = alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token");
    let mcp = HttpMcp::start(&cluster, "mcp", &[]).await;
    let dave = bearer(&token.text);

    let r = tool_result(&mcp, &dave, "create_stream", json!({"name": "dave-before", "partitions": 1})).await;
    assert_eq!(r["isError"], false, "{r}");
    alice.revoke_token(token.id).await.expect("revoked");

    // The client signed in with the token is still cached; the next request
    // fails at the cluster with 401, and the one after signs in afresh and
    // fails too. No tool runs.
    for _ in 0..2 {
        let a = call_http(&mcp.addr, None, Some(&dave), &[], "create_stream", json!({"name": "dave-after"})).await;
        assert_unauthorized(&a);
    }
    assert!(exists(&alice, "dave-before").await);
    assert!(!exists(&alice, "dave-after").await, "no tool ran after the revocation");
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_with_one_credential_sign_in_once() {
    let cluster = Cluster::start("mcp-http-once").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "dave").await;
    let token = alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token");
    let mcp = HttpMcp::start(&cluster, "mcp", &[]).await;
    assert_eq!(token_sign_ins(&cluster, "ok").await, 0, "no token sign-in yet");

    // Twenty requests with the same token at once.
    let dave = bearer(&token.text);
    let calls = (0..20).map(|_| {
        let (addr, dave) = (mcp.addr.clone(), dave.clone());
        tokio::spawn(async move { call_http(&addr, None, Some(&dave), &[], "list_sessions", json!({})).await })
    });
    for call in calls.collect::<Vec<_>>() {
        let a = call.await.unwrap();
        assert_eq!(a.status, 200, "{a:?}");
        let v = a.json();
        assert_eq!(session_names(&v["result"]), ["dave"], "one session, dave's: {v}");
    }
    assert_eq!(token_sign_ins(&cluster, "ok").await, 1, "the twenty requests signed in once");
    assert_eq!(token_sign_ins(&cluster, "failed").await, 0);
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_http_is_refused_off_loopback() {
    let root = std::env::temp_dir().join(format!("lotse-node-mcp-http-plain-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let ca = root.join("ca.pem");
    let listen = format!("0.0.0.0:{}", free_port());
    let mut child = Command::new(env!("CARGO_BIN_EXE_lotse"))
        .args(["mcp", "--bootstrap", "127.0.0.1:1", "--ca", ca.to_str().unwrap(), "--listen", listen.as_str()])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the lotse executable runs");
    let mut status = None;
    for _ in 0..300 {
        status = child.try_wait().unwrap();
        if status.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if status.is_none() {
        let _ = child.kill();
        let _ = child.wait();
        panic!("lotse mcp listened with plain HTTP on {listen}");
    }
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(out.stdout.is_empty());
    assert!(stderr.contains("loopback"), "{stderr}");
    assert!(stderr.contains("--tls-cert") && stderr.contains("--tls-key"), "the message names the flags: {stderr}");
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_browser_origin_is_refused_unless_allowed() {
    let cluster = Cluster::start("mcp-http-origin").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "erin").await;
    alice.set_user_password("erin", "erin-password").await.expect("erin's password");
    let erin = basic("erin", "erin-password");
    let origin = |o: &str| vec![("Origin", o.to_string())];

    // None allowed by default: any Origin is refused, before the
    // credential is looked at; a request without one passes.
    let mcp = HttpMcp::start(&cluster, "mcp-default", &[]).await;
    let a =
        call_http(&mcp.addr, None, Some(&erin), &origin("https://console.example"), "list_sessions", json!({})).await;
    assert_eq!(a.status, 403, "{a:?}");
    let a = call_http(&mcp.addr, None, None, &origin("https://console.example"), "list_sessions", json!({})).await;
    assert_eq!(a.status, 403, "the origin before the credential: {a:?}");
    let a = call_http(&mcp.addr, None, Some(&erin), &origin("null"), "list_sessions", json!({})).await;
    assert_eq!(a.status, 403, "{a:?}");
    let a = call_http(&mcp.addr, None, Some(&erin), &[], "list_sessions", json!({})).await;
    assert_eq!(a.status, 200, "no Origin: not a browser: {a:?}");
    drop(mcp);

    // Allowed origins pass; others stay refused.
    let args = ["--allow-origin", "https://console.example", "--allow-origin", "http://localhost:3000"];
    let mcp = HttpMcp::start(&cluster, "mcp-allowed", &args).await;
    for allowed in ["https://console.example", "http://localhost:3000"] {
        let a = call_http(&mcp.addr, None, Some(&erin), &origin(allowed), "list_sessions", json!({})).await;
        assert_eq!(a.status, 200, "{allowed}: {a:?}");
        assert_eq!(session_names(&a.json()["result"]), ["erin"]);
    }
    for refused in ["https://evil.example", "https://console.example:8443", "http://console.example"] {
        let a = call_http(&mcp.addr, None, Some(&erin), &origin(refused), "list_sessions", json!({})).await;
        assert_eq!(a.status, 403, "{refused}: {a:?}");
    }
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_with_the_given_certificate() {
    let cluster = Cluster::start("mcp-http-tls").await;
    let alice = cluster.alice();
    bind_own_prefix(&alice, "dave").await;
    let token = alice.create_token("agent", Some("dave"), lotse_client::Lifetime::Week).await.expect("a token");
    // The listener's certificate, from a CA of its own (not the cluster's).
    let ca = lotse_rt_tokio::dev_certs::DevCa::new();
    let issued = ca.custom("mcp.example", &[], &["127.0.0.1", "localhost"]);
    let (cert, key) = (cluster.root.join("mcp.pem"), cluster.root.join("mcp.key"));
    std::fs::write(&cert, &issued.cert_pem).unwrap();
    std::fs::write(&key, &issued.key_pem).unwrap();
    let mcp =
        HttpMcp::start(&cluster, "mcp", &["--tls-cert", cert.to_str().unwrap(), "--tls-key", key.to_str().unwrap()])
            .await;

    let mut roots = rustls::RootCertStore::empty();
    use rustls::pki_types::pem::PemObject as _;
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(ca.ca_pem().as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let tls = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );
    let a = call_http(&mcp.addr, Some(tls.clone()), Some(&bearer(&token.text)), &[], "list_sessions", json!({})).await;
    assert_eq!(a.status, 200, "{a:?}");
    assert_eq!(session_names(&a.json()["result"]), ["dave"]);
    let a = call_http(&mcp.addr, Some(tls), None, &[], "list_sessions", json!({})).await;
    assert_unauthorized(&a);

    // Plain HTTP to the TLS listener gets no MCP answer.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(&mcp.addr).await.unwrap();
    s.write_all(b"POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n").await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out)).await;
    assert!(!String::from_utf8_lossy(&out).contains("HTTP/1.1 "), "plain HTTP answered: {out:?}");
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_null_origin_is_refused_at_start() {
    let ca = std::env::temp_dir().join(format!("lotse-node-mcp-null-{}-ca.pem", std::process::id()));
    let listen = format!("127.0.0.1:{}", free_port());
    let run = |origin: &str| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_lotse"));
        cmd.args(["mcp", "--bootstrap", "127.0.0.1:1", "--ca", ca.to_str().unwrap(), "--listen", listen.as_str()]);
        cmd.args(["--allow-origin", origin]);
        for v in CREDENTIAL_VARS {
            cmd.env_remove(v);
        }
        cmd.stdin(Stdio::null()).output().expect("the lotse executable runs")
    };
    let out = run("null");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--allow-origin null"), "{stderr}");

    // An origin with a path can never match: a warning, before anything
    // else (here the missing CA ends the process).
    let out = run("https://console.example/");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("warning: --allow-origin https://console.example/ can never match"), "{stderr}");
}

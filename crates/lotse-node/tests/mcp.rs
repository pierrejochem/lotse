//! `lotse mcp` over stdio (ADR 0134), against a node run from a cluster
//! file on real sockets over TLS: the executable answers `server/discover`
//! and lists the tools with their hints, both without a session (MCP
//! 2026-07-28) and after `initialize` (2025-11-25); a tool runs as the
//! owner of the token in `LOTSE_TOKEN`, so the cluster's checks apply to
//! that owner; without a credential it refuses to start. Its standard
//! output carries nothing but JSON-RPC, even with debug logging on.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
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
        let text = format!(
            "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
             cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n\
             [disk]\nfree_limit = 0\n\n[auth]\nadmins = [\"alice\"]\n\n\
             [[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"{client_addr}\"\n",
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
        let cluster = Cluster { root, config, client_addr, stop, node };
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
        let mut child = mcp_command(config, args, env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
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
        assert!(listed.iter().any(|s| s["token_id"] == json!(token.id)), "{sessions}");

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

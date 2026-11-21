//! A rolling restart on real nodes over TLS (ADR 0127), driven by the
//! `lotse` executable as an operator or a package script would: `lotse
//! turn take` on a node's machine, the restart, the turn coming back, and
//! `lotse upgrade` and `lotse probe` around it.

use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

use lotse_node::config::ClusterFile;
use tokio::sync::oneshot;

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

fn lotse(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lotse")).args(args).output().expect("the lotse executable runs")
}

fn text(out: &Output) -> String {
    format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr))
}

type Running = (oneshot::Sender<()>, tokio::task::JoinHandle<()>);

fn start(file: &ClusterFile, id: u64) -> Running {
    let (tx, rx) = oneshot::channel::<()>();
    let f = file.clone();
    let task = tokio::spawn(async move {
        lotse_node::run(&f, id, false, async {
            let _ = rx.await;
        })
        .await
        .unwrap_or_else(|e| panic!("node {id}: {e}"));
    });
    (tx, task)
}

async fn stop((tx, task): Running) {
    let _ = tx.send(());
    task.await.unwrap();
}

/// Runs `lotse` with `args` until `done` holds for its output, for at most
/// `within`; the last output.
async fn until(args: &[&str], within: Duration, done: impl Fn(&Output) -> bool) -> Output {
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let owned: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        let out = tokio::task::spawn_blocking(move || {
            let owned: Vec<&str> = owned.iter().map(String::as_str).collect();
            lotse(&owned)
        })
        .await
        .unwrap();
        if done(&out) {
            return out;
        }
        assert!(tokio::time::Instant::now() < deadline, "lotse {} never did: {}", args.join(" "), text(&out));
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rolling_restart_takes_the_turn_and_gives_it_back() {
    let root = std::env::temp_dir().join(format!("lotse-node-upgrade-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let r = root.display();
    let mut config_text = format!(
        "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
         cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n\
         [disk]\nfree_limit = 0\n\n[auth]\nadmins = [\"alice\"]\n"
    );
    for id in 1..=3 {
        config_text.push_str(&format!(
            "\n[[node]]\nid = {id}\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\nmetrics = \"127.0.0.1:{}\"\n",
            free_port(),
            free_port(),
            free_port()
        ));
    }
    let config = root.join("cluster.toml");
    std::fs::write(&config, &config_text).unwrap();
    let c = config.to_str().unwrap();
    assert!(lotse(&["certs", "--config", c]).status.success());
    let clients = root.join("clients");
    assert!(lotse(&["certs", "--config", c, "--client", "alice", "--out", clients.to_str().unwrap()]).status.success());
    let (cert, key) = (clients.join("alice.pem"), clients.join("alice.key"));
    let admin = ["--cert", cert.to_str().unwrap(), "--key", key.to_str().unwrap()];

    let file = ClusterFile::load(&config).unwrap();
    let mut nodes: Vec<Option<Running>> = (1..=3).map(|id| Some(start(&file, id))).collect();
    for id in ["1", "2", "3"] {
        until(&["probe", "readyz", "--config", c, "--node", id], Duration::from_secs(60), |o| o.status.success()).await;
    }
    let ours = lotse_server::version::display(lotse_server::version::CLUSTER_VERSION);
    let status = until(&["upgrade", "status", "--config", c, "--node", "1"], Duration::from_secs(60), |o| {
        o.status.success() && text(o).contains(&format!("cluster version: {ours}"))
    })
    .await;
    assert!(text(&status).contains("restart turn:    free"), "{}", text(&status));
    assert!(text(&status).contains(lotse_server::version::RELEASE), "{}", text(&status));

    // Node 2's machine takes the turn; node 3's must wait for it.
    let out = lotse(&["turn", "take", "--config", c, "--node", "2", "--wait", "60s"]);
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&out).contains("holds the restart turn"));
    let out = lotse(&["turn", "take", "--config", c, "--node", "3", "--wait", "3s"]);
    assert!(!out.status.success(), "node 3 got the turn node 2 holds: {}", text(&out));
    assert!(text(&out).contains("node 2 holds the restart turn"), "{}", text(&out));
    // Node 3's certificate speaks only for node 3.
    let out = lotse(&["turn", "release", "--config", c, "--node", "3"]);
    assert!(out.status.success(), "{}", text(&out));
    assert!(text(&lotse(&["upgrade", "status", "--config", c, "--node", "1"])).contains("restart turn:    node 2"));

    // The restart; the new process gives the turn back once it is ready.
    stop(nodes[1].take().unwrap()).await;
    nodes[1] = Some(start(&file, 2));
    until(&["upgrade", "status", "--config", c, "--node", "1"], Duration::from_secs(60), |o| {
        text(o).contains("restart turn:    free")
    })
    .await;
    until(&["probe", "readyz", "--config", c, "--node", "2"], Duration::from_secs(60), |o| o.status.success()).await;

    // A pause holds the next turn back; the admin's commands need its
    // certificate.
    let out = lotse(&[&["upgrade", "hold", "--config", c][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", text(&out));
    let out = lotse(&["upgrade", "hold", "--config", c, "--node", "3"]);
    assert!(!out.status.success(), "a node's certificate paused turns: {}", text(&out));
    let out = lotse(&["turn", "take", "--config", c, "--node", "3", "--wait", "3s"]);
    assert!(!out.status.success() && text(&out).contains("paused"), "{}", text(&out));
    let out = lotse(&[&["upgrade", "resume", "--config", c][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", text(&out));
    let out = lotse(&["turn", "take", "--config", c, "--node", "3", "--wait", "60s"]);
    assert!(out.status.success(), "{}", text(&out));
    let out = lotse(&[&["upgrade", "release", "--config", c, "--node", "3"][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", text(&out));

    // Every node runs this release: raising to it changes nothing; a
    // version no node runs with is refused, by name.
    let out = lotse(&[&["upgrade", "finalize", "--config", c][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", text(&out));
    let out = lotse(&[&["upgrade", "finalize", "--config", c, "--version", "999.0"][..], &admin[..]].concat());
    assert!(!out.status.success() && text(&out).contains("node 1"), "{}", text(&out));
    let out = lotse(&["probe", "safe-to-stop", "--config", c, "--node", "1"]);
    assert!(out.status.success(), "{}", text(&out));

    for n in nodes.into_iter().flatten() {
        stop(n).await;
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_probe_reads_only_a_loopback_endpoint() {
    let root = std::env::temp_dir().join(format!("lotse-node-probe-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let config = root.join("cluster.toml");
    std::fs::write(
        &config,
        "[cluster]\ndata_dir = \"/tmp/x\"\n\n[tls]\nca = \"/tmp/x/ca.pem\"\ncert = \"/tmp/x/n.pem\"\nkey = \"/tmp/x/n.key\"\n\n[auth]\nadmins = [\"alice\"]\n\n\
         [[node]]\nid = 1\npeer = \"127.0.0.1:7000\"\nclient = \"127.0.0.1:9000\"\nmetrics = \"10.0.0.1:9464\"\n",
    )
    .unwrap();
    let out = lotse(&["probe", "readyz", "--config", config.to_str().unwrap(), "--node", "1"]);
    assert!(!out.status.success() && text(&out).contains("not loopback"), "{}", text(&out));
    let _ = std::fs::remove_dir_all(Path::new(&root));
}

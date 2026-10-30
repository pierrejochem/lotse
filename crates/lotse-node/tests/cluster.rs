//! A cluster from one cluster file on real sockets: `lotse certs` makes the
//! CA, the node certificates and an admin's client certificate; two nodes
//! run in this process and the third as the `lotse` executable; the admin
//! creates a stream and writes to it, a client without a certificate may
//! not; the executable stops cleanly on SIGTERM; after a restart of all
//! three, what was written is still there.

use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

use bytes::Bytes;
use lotse_client::{Client, ClientConfig, ClientError};
use lotse_node::config::ClusterFile;
use lotse_record::{BatchBuilder, BatchOptions};
use lotse_rt_tokio::{TlsConfig, TokioRuntime};
use tokio::sync::oneshot;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn batch(value: &str) -> Bytes {
    let mut b = BatchBuilder::new(BatchOptions::default());
    b.push(0, None, Some(value.as_bytes()), &[]);
    b.finish().unwrap()
}

fn lotse(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lotse")).args(args).output().expect("the lotse executable runs")
}

/// Nodes 1 and 2 in this process, each stopped by its sender.
fn start_in_process(file: &ClusterFile) -> Vec<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)> {
    (1..=2)
        .map(|id| {
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
        })
        .collect()
}

/// The executable running node 3; killed if the test fails before it is
/// stopped.
struct Third(Option<Child>);

impl Drop for Third {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn start_executable(config: &Path) -> Third {
    let child = Command::new(env!("CARGO_BIN_EXE_lotse"))
        .args(["run", "--config", config.to_str().unwrap(), "--node", "3"])
        .env("LOTSE_LOG", "warn")
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("lotse run starts");
    Third(Some(child))
}

async fn stop_all(nodes: Vec<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>, mut third: Third) {
    let mut third = third.0.take().expect("running");
    let status = Command::new("kill").args(["-TERM", &third.id().to_string()]).status().unwrap();
    assert!(status.success());
    let exit = tokio::task::spawn_blocking(move || third.wait().unwrap()).await.unwrap();
    assert!(exit.success(), "lotse run stops cleanly on SIGTERM: {exit}");
    for (tx, task) in nodes {
        let _ = tx.send(());
        task.await.unwrap();
    }
}

fn client(root: &Path, file: &ClusterFile, tls: TlsConfig) -> Client<TokioRuntime> {
    let bootstrap = file.nodes.iter().map(|n| n.client.clone()).collect();
    let cfg = ClientConfig { retry_for: Duration::from_secs(20), ..ClientConfig::new(bootstrap) };
    Client::new(TokioRuntime::new(root).with_tls(tls), cfg)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_from_one_file() {
    let root = std::env::temp_dir().join(format!("lotse-node-cluster-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let r = root.display();
    let mut text = format!(
        "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
         cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n\
         [retention]\nms = -1\n\n[auth]\nadmins = [\"alice\"]\n"
    );
    for id in 1..=3 {
        text.push_str(&format!(
            "\n[[node]]\nid = {id}\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\n",
            free_port(),
            free_port()
        ));
    }
    let config = root.join("cluster.toml");
    std::fs::write(&config, &text).unwrap();
    let c = config.to_str().unwrap();

    // The executable checks the file and makes the certificates; a second
    // run keeps them, and the admin's certificate comes from the kept CA.
    let out = lotse(&["check", "--config", c]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert!(String::from_utf8_lossy(&out.stdout).contains("3 nodes: 1, 2, 3"));
    let out = lotse(&["certs", "--config", c]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(String::from_utf8_lossy(&out.stdout).matches("wrote").count(), 8, "CA and three nodes, each with a key");
    let out = lotse(&["certs", "--config", c]);
    assert_eq!(String::from_utf8_lossy(&out.stdout).matches("kept").count(), 8, "nothing made twice");
    let clients = root.join("clients");
    let out = lotse(&["certs", "--config", c, "--client", "alice", "--out", clients.to_str().unwrap()]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for key in [root.join("tls/ca.key"), root.join("tls/node-1.key"), clients.join("alice.key")] {
            assert_eq!(std::fs::metadata(&key).unwrap().permissions().mode() & 0o777, 0o600, "{}", key.display());
        }
    }
    let out = lotse(&["run", "--config", c, "--node", "4"]);
    assert!(!out.status.success() && String::from_utf8_lossy(&out.stderr).contains("no node 4"));

    let file = ClusterFile::load(&config).unwrap();
    let ca = root.join("tls/ca.pem");
    let nodes = start_in_process(&file);
    let third = start_executable(&config);

    let alice = client(
        &root,
        &file,
        TlsConfig::from_files(&ca, Some((clients.join("alice.pem"), clients.join("alice.key")))).unwrap(),
    );
    let stranger = client(&root, &file, TlsConfig::from_files(&ca, None::<(&Path, &Path)>).unwrap());
    // Refused; while the nodes are still starting, the client may only
    // learn that its first attempt is unknown, so look afterwards.
    match stranger.create_stream("intruders", 1, 1).await {
        Err(ClientError::Authentication(_) | ClientError::Broker { .. } | ClientError::Unknown(_)) => {}
        other => panic!("a client without a certificate created a stream: {other:?}"),
    }
    alice.create_stream("orders", 2, 3).await.expect("the admin creates a stream");
    match alice.describe_configs("intruders").await {
        Err(ClientError::UnknownStream(..) | ClientError::Broker { .. }) => {}
        other => panic!("a client without a certificate created a stream: {other:?}"),
    }
    let mut written = Vec::new();
    for i in 0..10u32 {
        let value = format!("order-{i}");
        let offset = alice.produce("orders", i % 2, batch(&value)).await.expect("the admin writes");
        written.push((i % 2, offset, value));
    }
    stop_all(nodes, third).await;
    drop(alice);

    // All three again, from their disks.
    let nodes = start_in_process(&file);
    let third = start_executable(&config);
    let alice = client(
        &root,
        &file,
        TlsConfig::from_files(&ca, Some((clients.join("alice.pem"), clients.join("alice.key")))).unwrap(),
    );
    for (p, offset, value) in &written {
        let f = alice.fetch("orders", *p, *offset, Duration::from_millis(100)).await.expect("a read after restart");
        let first = f.committed_batches().unwrap().into_iter().next().expect("the batch is there");
        assert_eq!(first.base_offset(), *offset);
        assert_eq!(first.records().unwrap()[0].value.as_deref(), Some(value.as_bytes()));
    }

    // A fourth node joins and the first leaves (ADR 0048), through the
    // executable, as the admin.
    std::fs::write(
        &config,
        format!(
            "{text}\n[[node]]\nid = 4\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\n",
            free_port(),
            free_port()
        ),
    )
    .unwrap();
    let out = lotse(&["certs", "--config", c]);
    assert!(String::from_utf8_lossy(&out.stdout).contains("wrote") && out.status.success(), "a certificate for node 4");
    let admin = [
        "--cert",
        clients.join("alice.pem").to_str().unwrap().to_string().leak() as &str,
        "--key",
        clients.join("alice.key").to_str().unwrap().to_string().leak(),
    ];
    let out = lotse(&[&["node", "add", "--config", c, "--node", "4"][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let file4 = ClusterFile::load(&config).unwrap();
    let (tx4, rx4) = oneshot::channel::<()>();
    let joining = tokio::spawn(async move {
        lotse_node::run(&file4, 4, true, async {
            let _ = rx4.await;
        })
        .await
        .unwrap_or_else(|e| panic!("node 4: {e}"));
    });
    let out = lotse(&[&["node", "remove", "--config", c, "--node", "1"][..], &admin[..]].concat());
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    // Up to 90 s, in half seconds.
    for attempt in 0.. {
        let out = lotse(&[&["node", "list", "--config", c][..], &admin[..]].concat());
        let listed = String::from_utf8_lossy(&out.stdout).to_string();
        let ids: Vec<&str> = listed.lines().skip(1).filter_map(|l| l.split_whitespace().next()).collect();
        if out.status.success() && ids == ["2", "3", "4"] && !listed.contains("draining") {
            break;
        }
        assert!(attempt < 180, "node 1 never left: {listed}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    for (p, offset, value) in &written {
        let f = alice.fetch("orders", *p, *offset, Duration::from_millis(100)).await.expect("a read after the move");
        let first = f.committed_batches().unwrap().into_iter().next().expect("the batch is there");
        assert_eq!(first.records().unwrap()[0].value.as_deref(), Some(value.as_bytes()));
    }
    alice.produce("orders", 0, batch("after the move")).await.expect("the moved partition takes writes");
    drop(alice);
    drop(stranger);
    let _ = tx4.send(());
    joining.await.unwrap();
    stop_all(nodes, third).await;
    let _ = std::fs::remove_dir_all(&root);
}

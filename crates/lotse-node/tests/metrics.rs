//! The operator's endpoint of a node run from a cluster file (ADR 0117), on
//! real sockets over TLS: Prometheus metrics for principals that see the
//! whole cluster, refused to others, and health answers for anyone.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lotse_client::{Client, ClientConfig};
use lotse_node::config::ClusterFile;
use lotse_proto::{RoleBindingData, role, subject_kind};
use lotse_rt_tokio::{TlsConfig, TokioRuntime};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// As in the other tests of this crate: a free port below the ephemeral
/// range.
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

fn lotse(args: &[&str]) {
    let out = Command::new(env!("CARGO_BIN_EXE_lotse")).args(args).output().expect("the lotse executable runs");
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
}

/// GET `path` over TLS, with the client certificate `who` in `dir` if
/// given; the status and the body.
async fn get(addr: &str, ca: &Path, who: Option<(&Path, &str)>, path: &str) -> (u16, String) {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(ca).unwrap() {
        roots.add(c.unwrap()).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let cfg = match who {
        Some((dir, name)) => {
            let chain = CertificateDer::pem_file_iter(dir.join(format!("{name}.pem")))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let key = PrivateKeyDer::from_pem_file(dir.join(format!("{name}.key"))).unwrap();
            builder.with_client_auth_cert(chain, key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut s = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    (text[9..12].parse().unwrap(), text.split_once("\r\n\r\n").unwrap().1.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_operator_endpoint_serves_metrics_to_cluster_viewers_and_health_to_anyone() {
    let root = std::env::temp_dir().join(format!("lotse-node-metrics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let r = root.display();
    let port = free_port();
    let text = format!(
        "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
         cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n[disk]\nfree_limit = 0\n\n[auth]\nadmins = [\"alice\"]\n\n\
         [[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\nmetrics = \"0.0.0.0:{port}\"\n",
        free_port(),
        free_port()
    );
    let config = root.join("cluster.toml");
    std::fs::write(&config, &text).unwrap();
    let c = config.to_str().unwrap();
    lotse(&["certs", "--config", c]);
    let clients = root.join("clients");
    for who in ["alice", "bob", "prometheus"] {
        lotse(&["certs", "--config", c, "--client", who, "--out", clients.to_str().unwrap()]);
    }
    let file = ClusterFile::load(&config).unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let f = file.clone();
    let node = tokio::spawn(async move {
        lotse_node::run(&f, 1, false, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    let ca = root.join("tls/ca.pem");
    let addr = format!("127.0.0.1:{port}");
    // Waits until `path` answers `status` for `who`, for at most 30 s.
    let until = async |who: Option<&str>, path: &str, status: u16| -> String {
        for _ in 0..300 {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                let (s, body) = get(&addr, &ca, who.map(|w| (clients.as_path(), w)), path).await;
                if s == status {
                    return body;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{path} never answered {status} for {who:?}");
    };

    // Health for anyone, no certificate needed.
    assert_eq!(until(None, "/healthz", 200).await, "ok\n");
    assert_eq!(until(None, "/readyz", 200).await, "ready\n");
    // A group of one voter stops with its node whenever that happens:
    // nothing to wait for.
    assert_eq!(until(None, "/safe-to-stop", 200).await, "safe to stop\n");
    assert_eq!(get(&addr, &ca, None, "/nowhere").await.0, 404);

    // Metrics: a bootstrap admin, yes; no certificate or a principal
    // without a cluster role, no.
    let body = until(Some("alice"), "/metrics", 200).await;
    assert!(body.contains("lotse_node_info{node=\"1\",version=\""), "{body}");
    assert!(body.contains("lotse_partition_leader{node=\"1\",group=\"1\",kind=\"metadata\""), "{body}");
    assert!(body.ends_with("# EOF\n"));
    let (status, body) = get(&addr, &ca, None, "/metrics").await;
    assert_eq!(status, 403, "{body}");
    assert!(body.contains("cluster-viewer"));
    assert_eq!(get(&addr, &ca, Some((&clients, "bob")), "/metrics").await.0, 403);
    assert_eq!(get(&addr, &ca, Some((&clients, "prometheus")), "/metrics").await.0, 403);

    // Bound to cluster-viewer, Prometheus's certificate reads them.
    let admin = Client::new(
        TokioRuntime::new(&root).with_tls(
            TlsConfig::from_files(&ca, Some((clients.join("alice.pem"), clients.join("alice.key")))).unwrap(),
        ),
        ClientConfig { retry_for: Duration::from_secs(20), ..ClientConfig::new(vec![file.nodes[0].client.clone()]) },
    );
    let viewer = RoleBindingData {
        subject_kind: subject_kind::USER,
        subject: "prometheus".into(),
        role: role::CLUSTER_VIEWER,
        tenant: String::new(),
        prefix: String::new(),
    };
    admin.create_stream("orders", 1, 1).await.unwrap();
    admin.alter_role_bindings(vec![viewer], Vec::new()).await.unwrap();
    let body = until(Some("prometheus"), "/metrics", 200).await;
    assert!(body.contains("kind=\"stream\",tenant=\"default\",resource=\"orders\""), "{body}");
    assert_eq!(get(&addr, &ca, Some((&clients, "bob")), "/metrics").await.0, 403, "bob still has no role");
    // A node's certificate is for node links, not for reading metrics.
    let tls = root.join("tls");
    assert_eq!(get(&addr, &ca, Some((&tls, "node-1")), "/metrics").await.0, 403);
    // A certificate a cluster admin revoked reads nothing more (ADR 0090).
    let pem = std::fs::read(clients.join("prometheus.pem")).unwrap();
    let der = CertificateDer::pem_slice_iter(&pem).next().unwrap().unwrap();
    let facts = lotse_auth::x509::facts(der.as_ref()).unwrap();
    admin.revoke_certificate(facts.issuer, &facts.serial, facts.not_after_ms, "test").await.unwrap();
    until(Some("prometheus"), "/metrics", 403).await;

    let _ = tx.send(());
    node.await.unwrap();
    let _ = std::fs::remove_dir_all(&root);
}

//! `lotse certs --sign` against a running node (ADR 0092): a cluster admin
//! asks for a certificate, the CA holder's command lists the request,
//! signs only what is confirmed, and records the certificate, which the
//! request then carries and the inventory lists.

use std::process::{Command, Stdio};
use std::time::Duration;

use base64::Engine as _;
use lotse_client::{Client, ClientConfig};
use lotse_node::config::ClusterFile;
use lotse_proto::certificate_request_state as state;
use lotse_rt_tokio::{TlsConfig, TokioRuntime};
use tokio::sync::oneshot;

/// The request the browser code made for `alice` (see `certificates.rs`).
const BROWSER_CSR: &str = "MIHKMHICAQAwEDEOMAwGA1UEAwwFYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARnKUSI4DRkymlj6IpyYHjl9g4fkO/KxbIv1mwPnPz6Uzzgu6zhq+BZCN0Ag5wvdlBj3SbgAATmU4mbaITAsGl6oAAwCgYIKoZIzj0EAwIDSAAwRQIgQKvHGNTyKosN+lfdcw26mTmMr9BK5y34EAESHttDA1kCIQCFfhWHtobQFEY8SXm5AquJHSijZHVx/Xps8gbif2hQvQ==";

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// Runs `lotse` off the async threads, stdin closed.
async fn lotse(args: Vec<String>) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_lotse"))
            .args(&args)
            .stdin(Stdio::null())
            .output()
            .expect("the lotse executable runs")
    })
    .await
    .unwrap()
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(ToString::to_string).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ca_holder_signs_what_a_cluster_admin_asked_for() {
    let root = std::env::temp_dir().join(format!("lotse-sign-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let r = root.display();
    let text = format!(
        "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
         cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\n\n\
         [auth]\nadmins = [\"admin\"]\n\n[[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\n",
        free_port(),
        free_port()
    );
    let config = root.join("cluster.toml");
    std::fs::write(&config, &text).unwrap();
    let c = config.to_str().unwrap().to_string();
    let clients = root.join("clients");
    assert!(lotse(strings(&["certs", "--config", &c])).await.status.success());
    let out = lotse(strings(&["certs", "--config", &c, "--client", "admin", "--out", clients.to_str().unwrap()])).await;
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));

    let file = ClusterFile::load(&config).unwrap();
    let (stop, rx) = oneshot::channel::<()>();
    let f = file.clone();
    let node = tokio::spawn(async move {
        lotse_node::run(&f, 1, false, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    let ca = root.join("tls/ca.pem");
    let admin = Client::new(
        TokioRuntime::new(&root).with_tls(
            TlsConfig::from_files(&ca, Some((clients.join("admin.pem"), clients.join("admin.key")))).unwrap(),
        ),
        ClientConfig { retry_for: Duration::from_secs(20), ..ClientConfig::new(vec![file.nodes[0].client.clone()]) },
    );

    let csr = base64::engine::general_purpose::STANDARD.decode(BROWSER_CSR).unwrap();
    admin.request_certificate("alice", csr.clone()).await.expect("a cluster admin asks");
    let listed = admin.describe_certificate_requests().await.unwrap();
    let id: String = listed.requests[0].id.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(listed.requests[0].state, state::PENDING);

    let (cert_file, key_file) = (clients.join("admin.pem"), clients.join("admin.key"));
    let identity = ["--cert", cert_file.to_str().unwrap(), "--key", key_file.to_str().unwrap()];
    let sign = |extra: &[&str]| {
        let mut args = strings(&["certs", "--config", &c, "--sign"]);
        args.extend(strings(extra));
        args.extend(strings(&identity));
        args
    };

    // Asked without a terminal to answer on, nothing is signed; and --yes
    // never signs everything, only the request named.
    let out = lotse(sign(&[])).await;
    let said = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success() && said.contains("left for later") && said.contains("0 signed"), "{said}");
    assert!(said.contains("23b66907cf9a2d8b"), "the key fingerprint is shown: {said}");
    let out = lotse(sign(&["--yes"])).await;
    assert!(!out.status.success(), "--yes alone is refused");
    let out = lotse(sign(&["--yes", "--request", "ffff"])).await;
    assert!(String::from_utf8_lossy(&out.stdout).contains("no certificate requests are waiting"));
    assert_eq!(admin.describe_certificate_requests().await.unwrap().requests[0].state, state::PENDING);

    // Named and confirmed, it is signed, recorded and written.
    let signed_dir = root.join("signed");
    let out =
        lotse(sign(&["--yes", "--request", &id[..8], "--out", signed_dir.to_str().unwrap(), "--days", "30"])).await;
    let said = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(out.status.success() && said.contains("1 signed"), "{said} {}", String::from_utf8_lossy(&out.stderr));
    let listed = admin.describe_certificate_requests().await.unwrap();
    let request = &listed.requests[0];
    assert_eq!(request.state, state::SIGNED);
    let written = std::fs::read_to_string(signed_dir.join("alice.pem")).unwrap();
    let (_, pem) = x509_parser::pem::parse_x509_pem(written.as_bytes()).unwrap();
    assert_eq!(pem.contents, request.certificate.to_vec(), "the cluster holds the certificate that was written");
    let cert = pem.parse_x509().unwrap();
    assert_eq!(cert.subject().iter_common_name().next().unwrap().as_str().unwrap(), "alice");
    assert_eq!(cert.public_key().subject_public_key.data.len(), 65, "a P-256 key, the requester's");
    let inventory = admin.describe_certificates().await.unwrap();
    let entry = inventory.certificates.iter().find(|e| e.serial[..] == *cert.raw_serial()).expect("in the inventory");
    assert_eq!((entry.principal.as_str(), entry.first_seen_ms), ("alice", 0));

    // Nothing is left to sign, and a second run says so.
    let out = lotse(sign(&[])).await;
    assert!(String::from_utf8_lossy(&out.stdout).contains("no certificate requests are waiting"));

    let _ = stop.send(());
    node.await.unwrap();
}

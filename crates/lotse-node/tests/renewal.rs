//! Renewing a client certificate over a connection made with it (ADR 0123),
//! on real sockets over TLS with a node run from a cluster file:
//! `lotse certs --renewal-ca` makes the renewal CA, `lotse certs --renew`
//! renews a certificate in its window and writes the new files, which then
//! sign in; a certificate outside its window is refused, and one the
//! renewal CA issued without the cluster recording it does not sign in.

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lotse_client::{Client, ClientConfig, ClientError};
use lotse_node::config::ClusterFile;
use lotse_rt_tokio::{TlsConfig, TokioRuntime};

/// As in the other tests of this crate: a free port below the ephemeral
/// range.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 1_200) as u16 * 10 + 7;
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

/// A client signing in with the certificate and key files given.
fn client_with(ca: &Path, cert: &Path, key: &Path, client_addr: &str) -> Client<TokioRuntime> {
    let rt = TokioRuntime::new(std::env::temp_dir())
        .with_tls(TlsConfig::from_files(ca, Some((cert.to_path_buf(), key.to_path_buf()))).unwrap());
    Client::new(
        rt,
        ClientConfig { retry_for: Duration::from_secs(5), ..ClientConfig::new(vec![client_addr.to_string()]) },
    )
}

fn pem_blocks(path: &Path) -> Vec<Vec<u8>> {
    lotse_node::pem_certificates(&std::fs::read(path).unwrap())
}

/// A one-node cluster run from a cluster file, with a renewal CA, and
/// client certificates for Alice, in her window from the start (20 days,
/// all of them in the last 30), and Bob, a year long and not.
struct Cluster {
    root: std::path::PathBuf,
    config: std::path::PathBuf,
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
             cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\nrenewal_ca = \"{r}/tls/renewal-ca.pem\"\n\n\
             [disk]\nfree_limit = 0\n\n[auth]\nadmins = [\"alice\", \"bob\"]\n\n\
             [[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"{client_addr}\"\n",
            free_port(),
        );
        let config = root.join("cluster.toml");
        std::fs::write(&config, &text).unwrap();
        let c = config.to_str().unwrap();
        ok(lotse(&["certs", "--config", c]));
        let out = ok(lotse(&["certs", "--config", c, "--renewal-ca"]));
        assert!(out.contains("renewal-ca.pem") && out.contains("renewal-ca.key"), "{out}");
        let dir = root.join("clients");
        let dir = dir.to_str().unwrap();
        ok(lotse(&["certs", "--config", c, "--client", "alice", "--out", dir, "--days", "20"]));
        ok(lotse(&["certs", "--config", c, "--client", "bob", "--out", dir]));
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
        let alice = client_with(
            &cluster.ca(),
            &cluster.client("alice.pem"),
            &cluster.client("alice.key"),
            &cluster.client_addr,
        );
        for _ in 0..300 {
            if alice.describe_nodes().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        cluster
    }

    fn ca(&self) -> std::path::PathBuf {
        self.root.join("tls/ca.pem")
    }

    fn client(&self, file: &str) -> std::path::PathBuf {
        self.root.join("clients").join(file)
    }

    async fn stop(self) {
        self.stop.send(()).unwrap();
        self.node.await.unwrap();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_certificate_in_its_window_is_renewed_over_its_own_connection() {
    let cluster = Cluster::start("renewal").await;
    let (root, client_addr) = (cluster.root.clone(), cluster.client_addr.clone());
    let config = cluster.config.clone();
    let c = config.to_str().unwrap();
    let clients = root.join("clients");
    let ca = root.join("tls/ca.pem");
    let (alice_pem, alice_key) = (clients.join("alice.pem"), clients.join("alice.key"));
    let old_serial = lotse_auth::x509::facts(&pem_blocks(&alice_pem)[0]).unwrap().serial;

    // Bob is outside his window.
    let bob = client_with(&ca, &clients.join("bob.pem"), &clients.join("bob.key"), &client_addr);
    let refused = bob.renew_certificate().await;
    assert!(
        matches!(&refused, Err(ClientError::Broker { message: Some(m), .. }) if m.contains("two thirds")),
        "{refused:?}"
    );

    // With --if-due, Bob's certificate is left alone, without asking.
    let bob_pem = clients.join("bob.pem");
    let before = std::fs::read(&bob_pem).unwrap();
    let (b_cert, b_key) =
        (bob_pem.to_str().unwrap().to_string(), clients.join("bob.key").to_str().unwrap().to_string());
    let c2 = c.to_string();
    let out = tokio::task::spawn_blocking(move || {
        lotse(&["certs", "--config", &c2, "--renew", "--if-due", "--cert", &b_cert, "--key", &b_key])
    })
    .await
    .unwrap();
    let out = ok(out);
    assert!(out.contains("not due"), "{out}");
    assert_eq!(std::fs::read(&bob_pem).unwrap(), before);

    // A copy of Alice's first files, to renew the same certificate twice.
    let (orig_pem, orig_key) = (root.join("orig.pem"), root.join("orig.key"));
    std::fs::copy(&alice_pem, &orig_pem).unwrap();
    std::fs::copy(&alice_key, &orig_key).unwrap();

    // Alice renews with the command, which writes the new files: she is due.
    let renew_if_due = |cert: &Path, key: &Path| {
        let (cert, key) = (cert.to_str().unwrap().to_string(), key.to_str().unwrap().to_string());
        let c = c.to_string();
        tokio::task::spawn_blocking(move || {
            lotse(&["certs", "--config", &c, "--renew", "--if-due", "--cert", &cert, "--key", &key])
        })
    };
    let out = ok(renew_if_due(&alice_pem, &alice_key).await.unwrap());
    assert!(out.contains("renewed") && out.contains("valid for 89 more days"), "{out}");
    let chain = pem_blocks(&alice_pem);
    assert_eq!(chain.len(), 2, "the new certificate and the renewal CA's");
    assert_eq!(chain[1], pem_blocks(&root.join("tls/renewal-ca.pem"))[0]);
    let facts = lotse_auth::x509::facts(&chain[0]).unwrap();
    assert_ne!(facts.serial, old_serial);

    // The new files sign in as Alice.
    let renewed = client_with(&ca, &alice_pem, &alice_key, &client_addr);
    let mut signed_in = false;
    for _ in 0..50 {
        if renewed.describe_nodes().await.is_ok() {
            signed_in = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(signed_in, "the renewed certificate signs in");
    let sessions = renewed.node_sessions().await.unwrap();
    let names: Vec<String> =
        sessions.into_iter().flat_map(|(_, s)| s.map(|s| s.sessions).unwrap_or_default()).map(|s| s.name).collect();
    assert!(names.iter().any(|n| n == "alice"), "{names:?}");
    // The inventory lists it.
    let listed = renewed.describe_certificates().await.unwrap();
    assert!(listed.certificates.iter().any(|e| e.serial.as_ref() == facts.serial.as_slice()), "{listed:?}");

    // A certificate the renewal CA signed that the cluster never recorded
    // does not sign in, though its chain is good.
    let forged = root.join("forged");
    std::fs::create_dir_all(&forged).unwrap();
    let ca_pem = std::fs::read_to_string(root.join("tls/renewal-ca.pem")).unwrap();
    let ca_key = std::fs::read_to_string(root.join("tls/renewal-ca.key")).unwrap();
    let issuer = rcgen::Issuer::from_ca_cert_pem(&ca_pem, rcgen::KeyPair::from_pem(&ca_key).unwrap()).unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, "alice");
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
    params.use_authority_key_identifier_extension = true;
    let cert = params.signed_by(&key, &issuer).unwrap();
    std::fs::write(forged.join("alice.pem"), format!("{}{ca_pem}", cert.pem())).unwrap();
    std::fs::write(forged.join("alice.key"), key.serialize_pem()).unwrap();
    let mallory = client_with(&ca, &forged.join("alice.pem"), &forged.join("alice.key"), &client_addr);
    let got = mallory.describe_nodes().await;
    assert!(got.is_err(), "an unrecorded certificate from the renewal CA: {got:?}");

    // Renewing the same certificate again replaces the earlier renewal:
    // its connection ends, and it signs in no more; the new one does.
    let again = client_with(&ca, &orig_pem, &orig_key, &client_addr).renew_certificate().await.unwrap();
    let (third_pem, third_key) = (root.join("third.pem"), root.join("third.key"));
    std::fs::write(&third_pem, &again.chain_pem).unwrap();
    std::fs::write(&third_key, &again.key_pem).unwrap();
    let mut replaced = false;
    for _ in 0..50 {
        if renewed.describe_nodes().await.is_err() {
            replaced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(replaced, "the replaced renewal no longer signs in");
    client_with(&ca, &third_pem, &third_key, &client_addr).describe_nodes().await.unwrap();
    cluster.stop().await;
}

/// A Rust client given its certificate files renews by itself once the
/// window is open, writes the new files, and connections it makes from
/// then on show the renewed certificate (ADR 0123).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_renews_its_certificate_by_itself() {
    let cluster = Cluster::start("auto-renewal").await;
    let (ca, cert, key) = (cluster.ca(), cluster.client("alice.pem"), cluster.client("alice.key"));
    let old = pem_blocks(&cert)[0].clone();
    let rt = TokioRuntime::new(std::env::temp_dir())
        .with_tls(TlsConfig::from_files(&ca, Some((cert.clone(), key.clone()))).unwrap());
    let config =
        ClientConfig { retry_for: Duration::from_secs(5), ..ClientConfig::new(vec![cluster.client_addr.clone()]) };
    let alice = Arc::new(Client::new(rt.clone(), config.clone()));
    let files = lotse_rt_tokio::CertificateFiles::new(rt.clone(), &ca, &cert, &key);
    let renewal = alice.keep_certificate_renewed(files, lotse_client::RENEW_CHECK);
    for _ in 0..100 {
        if renewal.renewals() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!((renewal.renewals(), renewal.failure()), (1, None));
    let chain = pem_blocks(&cert);
    assert_ne!(chain[0], old, "the files hold the renewed certificate");
    assert_eq!(chain[1], pem_blocks(&cluster.root.join("tls/renewal-ca.pem"))[0]);

    // A new connection on the same runtime shows the renewed certificate,
    // which is not in its window: renewing it is refused, which the old
    // one, in its last 20 days, would not have been.
    let fresh = Client::new(rt, config);
    let refused = fresh.renew_certificate().await;
    assert!(
        matches!(&refused, Err(ClientError::Broker { message: Some(m), .. }) if m.contains("two thirds")),
        "{refused:?}"
    );
    drop(renewal);
    cluster.stop().await;
}

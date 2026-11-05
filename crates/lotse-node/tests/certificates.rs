//! Certificates over their life (ADR 0060): how long `lotse certs` makes
//! them valid, `lotse check` warning before they end, `--revoke` writing the
//! revocation list, and a running node taking up changed files: refusing and
//! cutting off a revoked certificate, keeping its material when the files
//! are broken.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use bytes::Bytes;
use lotse_node::certs;
use lotse_node::config::ClusterFile;
use lotse_rt::{Conn, Link, Listener, Net};
use lotse_rt_tokio::{TlsConfig, TokioConn, TokioRuntime};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn lotse(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_lotse")).args(args).output().expect("the lotse executable runs")
}

/// A one-node cluster file with a revocation list, in a fresh directory.
fn setup(name: &str) -> (PathBuf, PathBuf, ClusterFile) {
    let root = std::env::temp_dir().join(format!("lotse-certs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let r = root.display();
    let text = format!(
        "[cluster]\ndata_dir = \"{r}/data/node-{{id}}\"\n\n[tls]\nca = \"{r}/tls/ca.pem\"\n\
         cert = \"{r}/tls/node-{{id}}.pem\"\nkey = \"{r}/tls/node-{{id}}.key\"\ncrl = \"{r}/tls/crl.pem\"\n\n\
         [auth]\nadmins = [\"alice\"]\n\n[[node]]\nid = 1\npeer = \"127.0.0.1:{}\"\nclient = \"127.0.0.1:{}\"\n",
        free_port(),
        free_port()
    );
    let config = root.join("cluster.toml");
    std::fs::write(&config, &text).unwrap();
    let file = ClusterFile::load(&config).unwrap();
    (root, config, file)
}

fn ok(out: &std::process::Output) -> String {
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn days(path: &Path) -> i64 {
    certs::expires_in(path).unwrap() / 86_400
}

#[test]
fn certificates_last_a_year_and_check_warns_before_they_end() {
    let (root, config, _) = setup("validity");
    let c = config.to_str().unwrap();
    let out = ok(&lotse(&["certs", "--config", c]));
    assert!(out.contains(&format!("wrote {}", root.join("tls/crl.pem").display())), "an empty list to start with");
    assert_eq!(days(&root.join("tls/ca.pem")), 3649, "the CA: ten years");
    assert_eq!(days(&root.join("tls/node-1.pem")), 364, "a node: one year");
    assert!(!ok(&lotse(&["check", "--config", c])).contains("warning"));

    let clients = root.join("clients");
    let out = clients.to_str().unwrap();
    ok(&lotse(&["certs", "--config", c, "--client", "bob", "--out", out, "--days", "20"]));
    assert_eq!(days(&clients.join("bob.pem")), 19);
    assert!(!lotse(&["certs", "--config", c, "--days", "0"]).status.success(), "no certificate valid for no time");

    // A node certificate renewed for a short time is what check warns about.
    std::fs::remove_file(root.join("tls/node-1.pem")).unwrap();
    std::fs::remove_file(root.join("tls/node-1.key")).unwrap();
    ok(&lotse(&["certs", "--config", c, "--days", "7"]));
    let out = ok(&lotse(&["check", "--config", c]));
    assert!(out.contains(&format!("warning: {} expires in 6 days", root.join("tls/node-1.pem").display())), "{out}");
    assert_eq!(out.matches("warning").count(), 1, "the CA is fine: {out}");
}

#[test]
fn revoking_adds_to_the_list_and_only_takes_our_own_certificates() {
    let (root, config, _) = setup("revoke");
    let c = config.to_str().unwrap();
    let clients = root.join("clients");
    let out_dir = clients.to_str().unwrap();
    ok(&lotse(&["certs", "--config", c]));
    for p in ["alice", "bob"] {
        ok(&lotse(&["certs", "--config", c, "--client", p, "--out", out_dir]));
    }
    let (alice, bob) = (clients.join("alice.pem"), clients.join("bob.pem"));
    let crl = root.join("tls/crl.pem");
    let ca = root.join("tls/ca.pem");
    let revokes = |cert: &Path| {
        let tls = TlsConfig::from_files_with_crl(&ca, None::<(&Path, &Path)>, Some(&crl)).unwrap();
        let pem = std::fs::read(cert).unwrap();
        let (_, block) = x509_parser::pem::parse_x509_pem(&pem).unwrap();
        tls.revokes(&block.contents)
    };
    assert!(!revokes(&alice) && !revokes(&bob));

    assert!(ok(&lotse(&["certs", "--config", c, "--revoke", alice.to_str().unwrap()])).starts_with("revoked: serial"));
    assert!(revokes(&alice) && !revokes(&bob));
    ok(&lotse(&["certs", "--config", c, "--revoke", bob.to_str().unwrap()]));
    assert!(revokes(&alice) && revokes(&bob), "the list keeps what it named");
    let again = ok(&lotse(&["certs", "--config", c, "--revoke", alice.to_str().unwrap()]));
    assert!(again.starts_with("already revoked"), "{again}");
    assert!(revokes(&alice));

    // Another CA's certificate is not ours to revoke.
    let other = lotse_rt_tokio::dev_certs::DevCa::new().principal("mallory");
    let foreign = root.join("mallory.pem");
    std::fs::write(&foreign, other.cert_pem).unwrap();
    let out = lotse(&["certs", "--config", c, "--revoke", foreign.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("was not issued by the cluster's CA"));
}

/// A client showing `<dir>/<who>.pem`.
fn client(ca: &Path, dir: &Path, who: &str) -> TokioRuntime {
    let id = Some((dir.join(format!("{who}.pem")), dir.join(format!("{who}.key"))));
    TokioRuntime::new(std::env::temp_dir()).with_tls(TlsConfig::from_files(ca, id).unwrap())
}

async fn accepted(l: &lotse_rt_tokio::TokioListener) -> Option<TokioConn> {
    tokio::time::timeout(Duration::from_millis(1500), l.accept()).await.ok().map(|r| r.unwrap().0)
}

async fn open(client: &TokioConn, server: &TokioConn) -> bool {
    client.send(Bytes::from_static(b"ping")).await.is_ok()
        && matches!(tokio::time::timeout(Duration::from_secs(5), server.recv()).await, Ok(Ok(Some(_))))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_running_node_takes_up_a_revocation_and_keeps_working_material() {
    let (root, config, file) = setup("reload");
    let c = config.to_str().unwrap();
    let clients = root.join("clients");
    ok(&lotse(&["certs", "--config", c]));
    for p in ["alice", "bob"] {
        ok(&lotse(&["certs", "--config", c, "--client", p, "--out", clients.to_str().unwrap()]));
    }
    let ca = root.join("tls/ca.pem");

    // The node's runtime and its reload loop, as `run` starts them.
    let rt = lotse_node::runtime(&file, 1).unwrap();
    let l = rt.listen("127.0.0.1:0", Link::Client).await.unwrap();
    let addr = l.local_addr();
    let reload = tokio::spawn(lotse_node::reload_tls_every(rt.clone(), file.clone(), 1, Duration::from_millis(50)));
    let a = client(&ca, &clients, "alice").connect(&addr, Link::Client).await.unwrap();
    let sa = accepted(&l).await.unwrap();
    let b = client(&ca, &clients, "bob").connect(&addr, Link::Client).await.unwrap();
    let sb = accepted(&l).await.unwrap();
    assert!(open(&a, &sa).await && open(&b, &sb).await);

    // A list that does not load changes nothing.
    let crl = root.join("tls/crl.pem");
    let good = std::fs::read(&crl).unwrap();
    std::fs::write(&crl, "-----BEGIN X509 CRL-----\nAAAA\n-----END X509 CRL-----\n").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(open(&a, &sa).await && open(&b, &sb).await, "broken files: the old material stays");
    std::fs::write(&crl, good).unwrap();

    ok(&lotse(&["certs", "--config", c, "--revoke", clients.join("alice.pem").to_str().unwrap()]));
    let ended = tokio::time::timeout(Duration::from_secs(5), sa.recv()).await;
    assert!(matches!(ended, Ok(Err(_))), "alice's connection ends");
    assert!(open(&b, &sb).await, "bob's goes on");
    if let Ok(conn) = client(&ca, &clients, "alice").connect(&addr, Link::Client).await {
        let _ = conn.send(Bytes::from_static(b"hello")).await;
        assert!(matches!(tokio::time::timeout(Duration::from_secs(5), conn.recv()).await, Ok(Err(_) | Ok(None))));
    }
    assert!(accepted(&l).await.is_none(), "alice cannot come back");
    reload.abort();
}

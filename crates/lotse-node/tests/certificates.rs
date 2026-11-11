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
/// A signing request for `cn` made the way a requester does: its own key,
/// signed with it, asking for more than it will get
/// (rcgen cannot write a CA flag into a request; the code ignores all of it).
fn csr_for(cn: &str, key: &rcgen::KeyPair, greedy: bool) -> Vec<u8> {
    let mut params = rcgen::CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, cn);
    if greedy {
        params.subject_alt_names = vec![rcgen::SanType::DnsName("evil.example".try_into().unwrap())];
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    }
    params.serialize_request(key).unwrap().der().to_vec()
}

#[test]
fn a_signing_request_gets_a_client_certificate_for_its_key_and_nothing_it_asks_besides() {
    let (root, config, file) = setup("sign");
    ok(&lotse(&["certs", "--config", config.to_str().unwrap()]));
    let key = rcgen::KeyPair::generate().unwrap();
    let greedy = csr_for("alice", &key, true);

    let info = certs::read_csr(&greedy, "alice").unwrap();
    assert_eq!(info.key_fingerprint.len(), 64);
    let made = certs::sign_csr(&file, &greedy, "alice", 30).unwrap();
    let (_, cert) = x509_parser::parse_x509_certificate(&made.der).unwrap();
    assert_eq!(cert.subject().iter_common_name().next().unwrap().as_str().unwrap(), "alice");
    assert_eq!(cert.public_key().subject_public_key.data.as_ref(), key.public_key_raw(), "the requester's key");
    assert!(!cert.is_ca(), "no CA, whatever was asked");
    assert!(cert.subject_alternative_name().unwrap().is_none(), "no alternative names");
    let eku = cert.extended_key_usage().unwrap().unwrap().value;
    assert!(eku.client_auth && !eku.server_auth, "client authentication only");
    let ca = std::fs::read_to_string(root.join("tls/ca.pem")).unwrap();
    let (_, ca_pem) = x509_parser::pem::parse_x509_pem(ca.as_bytes()).unwrap();
    assert_eq!(cert.issuer().as_raw(), ca_pem.parse_x509().unwrap().subject().as_raw(), "issued by the cluster CA");
    assert_eq!(made.serial, cert.raw_serial());
    assert_eq!(made.issuer, lotse_auth::x509::issuer_fingerprint(&cert), "the issuer as the broker names it");
    // The broker takes what lotse certs signs (ADR 0096).
    let ca_der = ca_pem.contents.clone();
    let facts = lotse_auth::x509::check_signed(&made.der, &greedy, "alice", &[ca_der]).expect("the broker takes it");
    assert_eq!((facts.issuer, facts.serial), (made.issuer, made.serial.clone()));
    assert!(made.not_after_ms > made.not_before_ms);
    let days_left = (made.not_after_ms - made.not_before_ms) / 86_400_000;
    assert!((29..=31).contains(&days_left), "valid for the days asked: {days_left}");
}

/// A request the browser code made (`makeCertificateRequest("alice")` in
/// `console/web/src/certificateRequest.ts`), base64 of its DER, with the
/// SHA-256 of its public key. `openssl req -inform der -verify` accepts it.
/// A change to the browser's encoding that breaks this breaks real requests.
const BROWSER_CSR: &str = "MIHKMHICAQAwEDEOMAwGA1UEAwwFYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARnKUSI4DRkymlj6IpyYHjl9g4fkO/KxbIv1mwPnPz6Uzzgu6zhq+BZCN0Ag5wvdlBj3SbgAATmU4mbaITAsGl6oAAwCgYIKoZIzj0EAwIDSAAwRQIgQKvHGNTyKosN+lfdcw26mTmMr9BK5y34EAESHttDA1kCIQCFfhWHtobQFEY8SXm5AquJHSijZHVx/Xps8gbif2hQvQ==";
const BROWSER_CSR_KEY: &str = "23b66907cf9a2d8b55976d1142b8ec5c5a2b85b18685722603440f15c2da42c5";

#[test]
fn a_request_the_browser_made_is_read_and_signed() {
    use base64::Engine as _;
    let (_, config, file) = setup("sign-browser");
    ok(&lotse(&["certs", "--config", config.to_str().unwrap()]));
    let der = base64::engine::general_purpose::STANDARD.decode(BROWSER_CSR).unwrap();
    assert_eq!(certs::read_csr(&der, "alice").unwrap().key_fingerprint, BROWSER_CSR_KEY);
    let made = certs::sign_csr(&file, &der, "alice", 365).unwrap();
    let (_, cert) = x509_parser::parse_x509_certificate(&made.der).unwrap();
    assert_eq!(cert.subject().iter_common_name().next().unwrap().as_str().unwrap(), "alice");
    assert!(certs::read_csr(&der, "bob").is_err(), "the request names alice");
}

#[test]
fn a_signing_request_that_is_not_what_it_says_is_not_signed() {
    let (_, config, file) = setup("sign-refused");
    ok(&lotse(&["certs", "--config", config.to_str().unwrap()]));
    let key = rcgen::KeyPair::generate().unwrap();
    let csr = csr_for("alice", &key, false);
    let refused = |r: Result<_, certs::CertError>| {
        assert!(matches!(r, Err(certs::CertError::BadRequest(_) | certs::CertError::BadName(_))), "{r:?}");
    };
    // Another principal than the one on the request.
    refused(certs::sign_csr(&file, &csr, "bob", 30).map(|_| ()));
    // A name the CA holder could not put in a file name.
    refused(certs::sign_csr(&file, &csr, "../alice", 30).map(|_| ()));
    // Damaged: the signature no longer matches.
    let mut bad = csr.clone();
    *bad.last_mut().unwrap() ^= 1;
    refused(certs::sign_csr(&file, &bad, "alice", 30).map(|_| ()));
    refused(certs::sign_csr(&file, &[1, 2, 3], "alice", 30).map(|_| ()));
    // Another key type than ECDSA P-256.
    let ed = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    refused(certs::sign_csr(&file, &csr_for("alice", &ed, false), "alice", 30).map(|_| ()));
}

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
    let cas = lotse_server::ClientCas::default();
    let reload =
        tokio::spawn(lotse_node::reload_tls_every(rt.clone(), file.clone(), 1, cas.clone(), Duration::from_millis(50)));
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

    // A second CA in the bundle reaches the check of recorded certificates
    // (ADR 0096), not only the handshakes.
    let mut next = rcgen::CertificateParams::default();
    next.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
    next.distinguished_name.push(rcgen::DnType::CommonName, "Lotse cluster CA");
    let next = next.self_signed(&rcgen::KeyPair::generate().unwrap()).unwrap();
    let bundle = format!("{}{}", std::fs::read_to_string(&ca).unwrap(), next.pem());
    std::fs::write(&ca, &bundle).unwrap();
    let mut waited = 0;
    while cas.get() != lotse_node::pem_certificates(bundle.as_bytes()) {
        waited += 1;
        assert!(waited < 100, "the new bundle is taken up within 5 s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(cas.get().len(), 2);
    assert!(open(&b, &sb).await, "bob's connection goes on under the new bundle");
    reload.abort();
}

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use lotse_rt::{CertificateStore, Conn as _, Link, Listener as _, Net as _, PeerIdentity};
use rustls::pki_types::pem::PemObject as _;

use super::CertificateFiles;
use crate::dev_certs::DevCa;
use crate::{TlsConfig, TokioRuntime};

/// A fresh directory for one test.
fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("lotse-cert-files-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// The principal a node sees on a new connection from `client`.
async fn shown(client: &TokioRuntime, listener: &crate::TokioListener) -> Option<PeerIdentity> {
    let conn = client.connect(&listener.local_addr(), Link::Client).await.unwrap();
    conn.send(Bytes::from_static(b"hi")).await.unwrap();
    let (server, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await.unwrap().unwrap();
    server.peer_identity()
}

/// The store reads the certificate the files hold, writes a renewed pair
/// over them, and the runtime's next connection shows it.
#[tokio::test]
async fn replaced_files_are_shown_from_the_next_connection_on() {
    let d = dir("replace");
    let ca = DevCa::new();
    let (ca_file, cert, key) = (d.join("ca.pem"), d.join("client.pem"), d.join("client.key"));
    let alice = ca.principal("alice");
    std::fs::write(&ca_file, ca.ca_pem()).unwrap();
    std::fs::write(&cert, &alice.cert_pem).unwrap();
    std::fs::write(&key, &alice.key_pem).unwrap();
    let client = TokioRuntime::new(&d).with_tls(TlsConfig::from_files(&ca_file, Some((&cert, &key))).unwrap());
    let node = TokioRuntime::new(&d).with_tls(ca.node_tls(1, &["127.0.0.1"]).unwrap());
    let listener = node.listen("127.0.0.1:0", Link::Client).await.unwrap();
    let files = CertificateFiles::new(client.clone(), &ca_file, &cert, &key);

    let leaf = files.leaf().await.unwrap();
    let first = rustls::pki_types::CertificateDer::from_pem_slice(alice.cert_pem.as_bytes()).unwrap();
    assert_eq!(leaf, first.to_vec());
    assert_eq!(shown(&client, &listener).await, Some(PeerIdentity::Principal("alice".into())));

    // Another certificate of the same CA stands in for a renewed one.
    let renewed = ca.principal("alice-renewed");
    let chain = format!("{}{}", renewed.cert_pem, ca.ca_pem());
    files.replace(&renewed.key_pem, &chain).await.unwrap();
    assert_eq!(std::fs::read_to_string(&cert).unwrap(), chain);
    assert_eq!(std::fs::read_to_string(&key).unwrap(), renewed.key_pem);
    assert!(!d.join("client.pem.new").exists() && !d.join("client.key.new").exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(std::fs::metadata(&key).unwrap().permissions().mode() & 0o777, 0o600, "the key is the owner's");
    }
    let second = rustls::pki_types::CertificateDer::from_pem_slice(renewed.cert_pem.as_bytes()).unwrap();
    assert_eq!(files.leaf().await.unwrap(), second.to_vec());
    assert_eq!(shown(&client, &listener).await, Some(PeerIdentity::Principal("alice-renewed".into())));
    let _ = std::fs::remove_dir_all(&d);
}

/// Files that are missing or hold no certificate are an error, and a
/// renewed pair that does not load is too, the files written all the same.
#[tokio::test]
async fn files_that_do_not_load_are_errors() {
    let d = dir("errors");
    let ca = DevCa::new();
    let (ca_file, cert, key) = (d.join("ca.pem"), d.join("client.pem"), d.join("client.key"));
    std::fs::write(&ca_file, ca.ca_pem()).unwrap();
    let client = TokioRuntime::new(&d).with_tls(ca.client_tls(Some("alice")).unwrap());
    let files = CertificateFiles::new(client, &ca_file, &cert, &key);
    assert_eq!(files.leaf().await.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    std::fs::write(&cert, "not a certificate").unwrap();
    let e = files.leaf().await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidData, "{e}");
    let other = ca.principal("bob");
    assert!(files.replace(&other.key_pem, "no chain").await.is_err());
    let _ = std::fs::remove_dir_all(&d);
}

//! TLS on real sockets: who may connect over which link, and what each end
//! learns about the other.

use std::time::Duration;

use bytes::Bytes;
use lotse_rt::{Conn, Link, Listener, Net, PeerIdentity};
use lotse_rt_tokio::dev_certs::DevCa;
use lotse_rt_tokio::{TlsConfig, TokioRuntime};

fn rt() -> TokioRuntime {
    TokioRuntime::new(std::env::temp_dir())
}

fn node(ca: &DevCa, id: u64) -> TokioRuntime {
    rt().with_tls(ca.node_tls(id, &["127.0.0.1", "localhost"]).unwrap())
}

/// The listener's next connection, or `None` if none completes in time.
async fn accepted(l: &lotse_rt_tokio::TokioListener) -> Option<lotse_rt_tokio::TokioConn> {
    tokio::time::timeout(Duration::from_millis(1500), l.accept()).await.ok().map(|r| r.unwrap().0)
}

/// Whether a connection the other end refused fails on first use.
async fn refused(conn: &lotse_rt_tokio::TokioConn) -> bool {
    let _ = conn.send(Bytes::from_static(b"hello")).await;
    matches!(tokio::time::timeout(Duration::from_secs(5), conn.recv()).await, Ok(Err(_)) | Ok(Ok(None)))
}

#[tokio::test]
async fn nodes_prove_who_they_are_to_each_other() {
    let ca = DevCa::new();
    let (a, b) = (node(&ca, 1), node(&ca, 2));
    assert!(a.verifies_peers());
    let l = a.listen("127.0.0.1:0", Link::Node).await.unwrap();
    let addr = l.local_addr();
    let conn = b.connect(&addr, Link::Node).await.unwrap();
    assert_eq!(conn.peer_identity(), Some(PeerIdentity::Node(1)), "the dialler learns whom it reached");
    let server = accepted(&l).await.expect("a node is let in");
    assert_eq!(server.peer_identity(), Some(PeerIdentity::Node(2)), "the listener learns who dialled");
    for i in 0..20u32 {
        conn.send(Bytes::from(i.to_be_bytes().to_vec())).await.unwrap();
    }
    conn.send(Bytes::from(vec![9u8; 1 << 20])).await.unwrap();
    drop(conn);
    let mut got = Vec::new();
    while let Some(f) = server.recv().await.unwrap() {
        got.push(f);
    }
    assert_eq!(got.len(), 21, "whole frames in order, then a clean end");
    assert_eq!(&got[3][..], &3u32.to_be_bytes());
    assert_eq!(got[20].len(), 1 << 20);
}

#[tokio::test]
async fn a_node_link_needs_node_certificates_on_both_ends() {
    let ca = DevCa::new();
    let a = node(&ca, 1);
    let l = a.listen("127.0.0.1:0", Link::Node).await.unwrap();
    let addr = l.local_addr();

    // A principal's certificate, from the right CA: not a node.
    let alice = rt().with_tls(ca.client_tls(Some("alice")).unwrap());
    if let Ok(conn) = alice.connect(&addr, Link::Node).await {
        assert!(refused(&conn).await, "the listener drops a non-node certificate");
    }
    // No certificate at all.
    let anon = rt().with_tls(ca.client_tls(None).unwrap());
    if let Ok(conn) = anon.connect(&addr, Link::Node).await {
        assert!(refused(&conn).await, "the listener demands a certificate");
    }
    // A certificate claiming two nodes, or a node id spelled oddly, names nobody.
    for uris in [&["lotse://node/3", "lotse://node/4"][..], &["lotse://node/03"][..]] {
        let c = ca.custom("odd", uris, &["127.0.0.1"]);
        let odd = rt().with_tls(
            TlsConfig::from_pem(ca.ca_pem().as_bytes(), Some((c.cert_pem.as_bytes(), c.key_pem.as_bytes()))).unwrap(),
        );
        if let Ok(conn) = odd.connect(&addr, Link::Node).await {
            assert!(refused(&conn).await, "{uris:?}");
        }
    }
    // A node of another cluster.
    let stranger = node(&DevCa::new(), 2);
    if let Ok(conn) = stranger.connect(&addr, Link::Node).await {
        assert!(refused(&conn).await, "another CA's node is a stranger");
    }
    assert!(accepted(&l).await.is_none(), "none of them got in");

    // The other way round: a node dialling a server that is not a node.
    let c = ca.custom("impostor", &[], &["127.0.0.1"]);
    let impostor = rt().with_tls(
        TlsConfig::from_pem(ca.ca_pem().as_bytes(), Some((c.cert_pem.as_bytes(), c.key_pem.as_bytes()))).unwrap(),
    );
    let il = impostor.listen("127.0.0.1:0", Link::Client).await.unwrap();
    let err = node(&ca, 2).connect(&il.local_addr(), Link::Node).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");
    // Or a node of another cluster: the host name is not checked on node
    // links, the chain still is.
    let foreign = node(&DevCa::new(), 1).listen("127.0.0.1:0", Link::Node).await.unwrap();
    assert!(node(&ca, 2).connect(&foreign.local_addr(), Link::Node).await.is_err(), "another CA's node");
}

#[tokio::test]
async fn clients_check_the_name_and_may_show_a_certificate() {
    let ca = DevCa::new();
    let server = node(&ca, 1);
    let l = server.listen("127.0.0.1:0", Link::Client).await.unwrap();
    let addr = l.local_addr();
    let port = addr.rsplit_once(':').unwrap().1;

    let anon = rt().with_tls(ca.client_tls(None).unwrap());
    let conn = anon.connect(&addr, Link::Client).await.unwrap();
    assert_eq!(conn.peer_identity(), Some(PeerIdentity::Node(1)));
    let s = accepted(&l).await.expect("a client without a certificate is let in");
    assert_eq!(s.peer_identity(), None, "and proves nobody");

    let alice = rt().with_tls(ca.client_tls(Some("alice")).unwrap());
    let _conn = alice.connect(&format!("localhost:{port}"), Link::Client).await.unwrap();
    let s = accepted(&l).await.expect("a client with a certificate");
    assert_eq!(s.peer_identity(), Some(PeerIdentity::Principal("alice".into())));

    // A node dialling as a client (transaction markers) proves it is a node.
    let _conn = node(&ca, 2).connect(&addr, Link::Client).await.unwrap();
    assert_eq!(accepted(&l).await.unwrap().peer_identity(), Some(PeerIdentity::Node(2)));

    // The name must match: this server's certificate names only "localhost".
    let c = ca.node(5, &["localhost"]);
    let only_name = rt().with_tls(
        TlsConfig::from_pem(ca.ca_pem().as_bytes(), Some((c.cert_pem.as_bytes(), c.key_pem.as_bytes()))).unwrap(),
    );
    let l2 = only_name.listen("127.0.0.1:0", Link::Client).await.unwrap();
    let port2 = l2.local_addr().rsplit_once(':').unwrap().1.to_owned();
    assert!(anon.connect(&format!("127.0.0.1:{port2}"), Link::Client).await.is_err(), "wrong name");
    assert!(anon.connect(&format!("localhost:{port2}"), Link::Client).await.is_ok());

    // A server from another CA.
    let other = node(&DevCa::new(), 1);
    let l3 = other.listen("127.0.0.1:0", Link::Client).await.unwrap();
    assert!(anon.connect(&l3.local_addr(), Link::Client).await.is_err(), "an unknown CA");
}

#[tokio::test]
async fn plain_tcp_is_opt_in_and_loopback_only() {
    let unset = rt();
    let err = unset.listen("127.0.0.1:0", Link::Client).await.unwrap_err();
    assert!(err.to_string().contains("not configured"), "{err}");
    assert!(unset.connect("127.0.0.1:1", Link::Client).await.is_err());

    let plain = rt().plaintext_on_loopback();
    assert!(!plain.verifies_peers());
    for addr in ["0.0.0.0:0", "10.1.2.3:0"] {
        let err = plain.listen(addr, Link::Node).await.unwrap_err();
        assert!(err.to_string().contains("loopback"), "{addr}: {err}");
    }
    let err = plain.connect("10.1.2.3:9000", Link::Client).await.unwrap_err();
    assert!(err.to_string().contains("loopback"), "{err}");
    let l = plain.listen("127.0.0.1:0", Link::Node).await.unwrap();
    let conn = plain.connect(&l.local_addr(), Link::Node).await.unwrap();
    assert_eq!(conn.peer_identity(), None);
    assert_eq!(accepted(&l).await.unwrap().peer_identity(), None);

    // TLS without a certificate of its own can dial but not listen.
    let client_only = rt().with_tls(DevCa::new().client_tls(None).unwrap());
    assert!(client_only.listen("127.0.0.1:0", Link::Client).await.is_err());
}

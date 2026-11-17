//! The operator's HTTP endpoint on real sockets (ADR 0117): plain on
//! loopback only, TLS elsewhere, with the client certificate passed on to
//! the handler when one is shown and the request let through when not.

use std::sync::Arc;

use lotse_rt::PeerIdentity;
use lotse_rt_tokio::dev_certs::{DevCa, Issued};
use lotse_rt_tokio::{HttpRequest, HttpResponse, TokioRuntime};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Answers with what it was asked and who asked.
async fn echo(req: HttpRequest) -> HttpResponse {
    let who = match &req.identity {
        Some(PeerIdentity::Principal(p)) => p.clone(),
        Some(PeerIdentity::Node(n)) => format!("node {n}"),
        None => "nobody".into(),
    };
    let status = if req.path == "/missing" { 404 } else { 200 };
    HttpResponse::text(status, format!("{} {} tls={} {who} cert={}", req.method, req.path, req.tls, req.cert.is_some()))
}

async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, request: &str) -> (u16, String) {
    s.write_all(request.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    let status = text[9..12].parse().unwrap();
    let body = text.split_once("\r\n\r\n").unwrap().1.to_string();
    (status, body)
}

fn get(path: &str) -> String {
    format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")
}

async fn plain(addr: &str, request: &str) -> (u16, String) {
    exchange(tokio::net::TcpStream::connect(addr).await.unwrap(), request).await
}

async fn tls(addr: &str, ca: &DevCa, client: Option<&Issued>, request: &str) -> (u16, String) {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_slice_iter(ca.ca_pem().as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
    let cfg = match client {
        Some(c) => {
            let chain = CertificateDer::pem_slice_iter(c.cert_pem.as_bytes()).collect::<Result<_, _>>().unwrap();
            builder.with_client_auth_cert(chain, PrivateKeyDer::from_pem_slice(c.key_pem.as_bytes()).unwrap()).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("127.0.0.1").unwrap();
    let s = tokio_rustls::TlsConnector::from(Arc::new(cfg)).connect(name, tcp).await.unwrap();
    exchange(s, request).await
}

#[tokio::test]
async fn plain_http_on_loopback() {
    let rt = TokioRuntime::new(std::env::temp_dir()).plaintext_on_loopback();
    let listener = rt.bind_http("127.0.0.1:0").await.unwrap();
    assert!(!listener.tls());
    let addr = listener.local_addr().to_string();
    tokio::spawn(listener.serve(echo));
    assert_eq!(plain(&addr, &get("/metrics?x=1")).await, (200, "GET /metrics tls=false nobody cert=false".into()));
    assert_eq!(plain(&addr, &get("/missing")).await.0, 404);
    // HEAD answers without a body; other methods are refused.
    assert_eq!(plain(&addr, "HEAD /readyz HTTP/1.1\r\n\r\n").await, (200, String::new()));
    assert_eq!(plain(&addr, "POST /metrics HTTP/1.1\r\n\r\n").await.0, 405);
    assert_eq!(plain(&addr, "nonsense\r\n\r\n").await.0, 400);
    let huge = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "a".repeat(10_000));
    assert_eq!(plain(&addr, &huge).await.0, 431);
}

#[tokio::test]
async fn off_loopback_plain_http_is_refused() {
    for rt in [TokioRuntime::new(std::env::temp_dir()), TokioRuntime::new(std::env::temp_dir()).plaintext_on_loopback()]
    {
        let err = rt.bind_http("0.0.0.0:0").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("configure TLS"), "{err}");
    }
}

#[tokio::test]
async fn tls_off_loopback_with_and_without_a_client_certificate() {
    let ca = DevCa::new();
    let rt = TokioRuntime::new(std::env::temp_dir()).with_tls(ca.node_tls(1, &["127.0.0.1"]).unwrap());
    let listener = rt.bind_http("0.0.0.0:0").await.unwrap();
    assert!(listener.tls());
    let port = listener.local_addr().rsplit_once(':').unwrap().1.to_string();
    let addr = format!("127.0.0.1:{port}");
    tokio::spawn(listener.serve(echo));
    let prometheus = ca.principal("prometheus");
    assert_eq!(
        tls(&addr, &ca, Some(&prometheus), &get("/metrics")).await,
        (200, "GET /metrics tls=true prometheus cert=true".into())
    );
    // A probe without a certificate gets through; the handler decides.
    assert_eq!(tls(&addr, &ca, None, &get("/readyz")).await, (200, "GET /readyz tls=true nobody cert=false".into()));
    // Plain HTTP to a TLS port gets nowhere.
    let mut s = tokio::net::TcpStream::connect(&addr).await.unwrap();
    s.write_all(get("/metrics").as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out).await;
    assert!(!String::from_utf8_lossy(&out).contains("200"), "plain HTTP was answered");
}

//! Fetching documents over HTTP(S) on real sockets: an identity provider's
//! keys are read this way.

use std::sync::Arc;

use lotse_rt::Http;
use lotse_rt_tokio::TokioRuntime;
use lotse_rt_tokio::dev_certs::DevCa;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Serves one `response` to each connection, over TLS if `tls` is given.
async fn serve(response: &'static [u8], tls: Option<Arc<rustls::ServerConfig>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = tls.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                match tls {
                    Some(cfg) => {
                        let Ok(mut s) = tokio_rustls::TlsAcceptor::from(cfg).accept(tcp).await else { return };
                        let _ = s.read(&mut buf).await;
                        let _ = s.write_all(response).await;
                        let _ = s.shutdown().await;
                    }
                    None => {
                        let mut s = tcp;
                        let _ = s.read(&mut buf).await;
                        let _ = s.write_all(response).await;
                    }
                }
            });
        }
    });
    addr
}

fn server_tls(ca: &DevCa) -> Arc<rustls::ServerConfig> {
    let c = ca.custom("idp", &[], &["127.0.0.1", "localhost"]);
    let chain: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(c.cert_pem.as_bytes()).collect::<Result<_, _>>().unwrap();
    let key = PrivateKeyDer::from_pem_slice(c.key_pem.as_bytes()).unwrap();
    let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .unwrap();
    Arc::new(cfg)
}

#[tokio::test]
async fn https_with_a_trusted_ca() {
    let ca = DevCa::new();
    let addr = serve(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"keys\":[]}", Some(server_tls(&ca))).await;
    let port = addr.rsplit_once(':').unwrap().1;
    let rt = TokioRuntime::new(std::env::temp_dir()).with_http_ca(ca.ca_pem().as_bytes()).unwrap();
    let body = rt.get(&format!("https://localhost:{port}/keys")).await.unwrap();
    assert_eq!(&body[..], b"{\"keys\":[]}");
    // Without the CA the server is a stranger.
    let plain = TokioRuntime::new(std::env::temp_dir());
    assert!(plain.get(&format!("https://localhost:{port}/keys")).await.is_err());
}

#[tokio::test]
async fn plain_http_only_on_loopback_and_only_200() {
    let addr = serve(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n", None).await;
    let rt = TokioRuntime::new(std::env::temp_dir());
    assert_eq!(&rt.get(&format!("http://{addr}/")).await.unwrap()[..], b"ok");
    let missing = serve(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n", None).await;
    assert!(rt.get(&format!("http://{missing}/")).await.is_err());
    let err = rt.get("http://10.1.2.3:80/keys").await.unwrap_err();
    assert!(err.to_string().contains("loopback"), "{err}");
}

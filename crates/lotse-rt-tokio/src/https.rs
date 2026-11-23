//! HTTPS for an axum service: TLS 1.3 with a certificate chain and key read
//! from PEM files, and a listener that hands axum only connections whose
//! handshake finished. `lotse mcp --listen` and the management console serve
//! with it (ADR 0134, ADR 0137).

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// How long a TLS handshake may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// TLS handshakes in progress at once; more wait in the kernel's backlog.
const MAX_HANDSHAKES: usize = 64;

/// A TLS 1.3 server configuration with the chain in `cert` and the private
/// key in `key`, both PEM, offering HTTP/1.1. The files are read once; the
/// error names the file that could not be used.
pub fn https_config(cert: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>, String> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .and_then(|i| i.collect())
        .map_err(|e| format!("{}: {e}", cert.display()))?;
    if chain.is_empty() {
        return Err(format!("{}: no certificate in it", cert.display()));
    }
    let private = PrivateKeyDer::from_pem_file(key).map_err(|e| format!("{}: {e}", key.display()))?;
    let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, private)
        .map_err(|e| format!("{} and {}: {e}", cert.display(), key.display()))?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// A listener that hands axum connections whose TLS handshake finished;
/// handshakes run in tasks of their own, so a slow one holds up no other.
#[derive(Debug)]
pub struct HttpsListener {
    accepted: tokio::sync::mpsc::Receiver<(tokio_rustls::server::TlsStream<tokio::net::TcpStream>, SocketAddr)>,
    addr: SocketAddr,
}

impl HttpsListener {
    /// Accepts on `tcp` with `cfg`; `what` names the service in log lines.
    pub fn new(tcp: tokio::net::TcpListener, cfg: Arc<rustls::ServerConfig>, what: &'static str) -> io::Result<Self> {
        let addr = tcp.local_addr()?;
        let (tx, accepted) = tokio::sync::mpsc::channel(MAX_HANDSHAKES);
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        let slots = Arc::new(tokio::sync::Semaphore::new(MAX_HANDSHAKES));
        tokio::spawn(async move {
            loop {
                let Ok(permit) = slots.clone().acquire_owned().await else { return };
                let (stream, peer) = match tcp.accept().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(error = %e, "accepting a {what} connection failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let (acceptor, tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    let _permit = permit;
                    match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls)) => {
                            let _ = tx.send((tls, peer)).await;
                        }
                        Ok(Err(e)) => tracing::debug!(%peer, error = %e, "{what}: TLS handshake failed"),
                        Err(_) => tracing::debug!(%peer, "{what}: TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(HttpsListener { accepted, addr })
    }
}

impl axum::serve::Listener for HttpsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.accepted.recv().await {
            Some(c) => c,
            // The accepting task ended: nothing more comes.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.addr)
    }
}

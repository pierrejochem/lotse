//! Frames over TCP, inside TLS 1.3 or, on loopback only, plain.

use std::future::Future;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use lotse_rt::{Conn, Link, Listener, MAX_FRAME, Net, PeerIdentity};
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::TokioRuntime;
use crate::tls::{TlsConfig, identity_of};

/// How a runtime talks to the network.
#[derive(Debug, Clone)]
pub(crate) enum Security {
    /// Not chosen: the network is refused, so nobody runs plaintext by
    /// accident.
    Unset,
    /// Plain TCP, loopback only.
    Plain,
    /// TLS 1.3.
    Tls(TlsConfig),
}

/// How long a TLS handshake may take before the connection is dropped.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Handshaken connections waiting for `accept`.
const BACKLOG: usize = 128;

fn unset() -> io::Error {
    io::Error::new(
        ErrorKind::PermissionDenied,
        "the network is not configured: give the runtime TLS (with_tls), or allow plain TCP on loopback \
         (plaintext_on_loopback)",
    )
}

/// Plain TCP may only reach this machine: every address `addr` resolves to
/// must be a loopback address.
async fn loopback_only(addr: &str) -> io::Result<()> {
    let resolved: Vec<std::net::SocketAddr> = tokio::net::lookup_host(addr).await?.collect();
    if resolved.is_empty() || resolved.iter().any(|a| !a.ip().is_loopback()) {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            format!("{addr} is not a loopback address; plain TCP is only allowed on loopback, configure TLS"),
        ));
    }
    Ok(())
}

/// A listener. Connections are accepted and handshaken in the background,
/// each on its own, so a slow or hostile peer delays nobody else.
#[derive(Debug)]
pub struct TokioListener {
    addr: String,
    ready: tokio::sync::Mutex<mpsc::Receiver<(TokioConn, String)>>,
    acceptor: tokio::task::AbortHandle,
}

impl Drop for TokioListener {
    fn drop(&mut self) {
        // The accept task owns the socket: stopping it closes the port.
        self.acceptor.abort();
    }
}

type Reader = Box<dyn AsyncRead + Send + Unpin>;
type Writer = Box<dyn AsyncWrite + Send + Unpin>;

/// A connection carrying length-prefixed frames, over TLS or plain TCP.
pub struct TokioConn {
    rd: tokio::sync::Mutex<Reader>,
    wr: tokio::sync::Mutex<Writer>,
    peer: Option<PeerIdentity>,
}

impl std::fmt::Debug for TokioConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokioConn").field("peer", &self.peer).finish()
    }
}

impl TokioConn {
    fn new<S: AsyncRead + AsyncWrite + Send + 'static>(stream: S, peer: Option<PeerIdentity>) -> Self {
        let (rd, wr) = tokio::io::split(stream);
        TokioConn { rd: tokio::sync::Mutex::new(Box::new(rd)), wr: tokio::sync::Mutex::new(Box::new(wr)), peer }
    }
}

/// The identity a handshake proved, checked against what the link demands:
/// a node link without a node certificate on the other end fails, as if the
/// handshake had.
fn checked(
    link: Link,
    certs: Option<&[rustls::pki_types::CertificateDer<'static>]>,
) -> io::Result<Option<PeerIdentity>> {
    let identity = certs.and_then(|c| c.first()).and_then(identity_of);
    if link == Link::Node && !matches!(identity, Some(PeerIdentity::Node(_))) {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "a node link needs a node certificate (lotse://node/<id>) on the other end",
        ));
    }
    Ok(identity)
}

async fn handshake_in(tcp: tokio::net::TcpStream, cfg: Arc<rustls::ServerConfig>, link: Link) -> io::Result<TokioConn> {
    let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, "TLS handshake timed out"))??;
    let peer = checked(link, tls.get_ref().1.peer_certificates())?;
    Ok(TokioConn::new(tls, peer))
}

async fn accept_loop(
    tcp: tokio::net::TcpListener,
    tls: Option<Arc<rustls::ServerConfig>>,
    link: Link,
    out: mpsc::Sender<(TokioConn, String)>,
) {
    loop {
        let (stream, peer) = match tcp.accept().await {
            Ok(c) => c,
            Err(e) => {
                // Out of file descriptors and the like: wait, then go on.
                tracing::warn!(error = %e, "accepting a connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        if stream.set_nodelay(true).is_err() {
            continue;
        }
        let peer = peer.to_string();
        match &tls {
            None => {
                if out.send((TokioConn::new(stream, None), peer)).await.is_err() {
                    return;
                }
            }
            Some(cfg) => {
                let (cfg, out) = (cfg.clone(), out.clone());
                tokio::spawn(async move {
                    match handshake_in(stream, cfg, link).await {
                        Ok(conn) => {
                            let _ = out.send((conn, peer)).await;
                        }
                        Err(e) => tracing::debug!(%peer, error = %e, "TLS handshake refused"),
                    }
                });
            }
        }
    }
}

impl Net for TokioRuntime {
    type Conn = TokioConn;
    type Listener = TokioListener;

    fn listen(&self, addr: &str, link: Link) -> impl Future<Output = io::Result<TokioListener>> + Send {
        let (addr, security) = (addr.to_owned(), self.security.clone());
        async move {
            let tls = match &security {
                Security::Unset => return Err(unset()),
                Security::Plain => {
                    loopback_only(&addr).await?;
                    tracing::warn!(%addr, "listening with plain TCP: nothing is encrypted or authenticated");
                    None
                }
                Security::Tls(t) => Some(t.server(link).ok_or_else(|| {
                    io::Error::new(ErrorKind::InvalidInput, "listening with TLS needs a certificate and key")
                })?),
            };
            let tcp = tokio::net::TcpListener::bind(&addr).await?;
            let addr = tcp.local_addr()?.to_string();
            let (tx, rx) = mpsc::channel(BACKLOG);
            let acceptor = tokio::spawn(accept_loop(tcp, tls, link, tx)).abort_handle();
            Ok(TokioListener { addr, ready: tokio::sync::Mutex::new(rx), acceptor })
        }
    }

    fn connect(&self, addr: &str, link: Link) -> impl Future<Output = io::Result<TokioConn>> + Send {
        let (addr, security) = (addr.to_owned(), self.security.clone());
        async move {
            match security {
                Security::Unset => Err(unset()),
                Security::Plain => {
                    loopback_only(&addr).await?;
                    let tcp = tokio::net::TcpStream::connect(&addr).await?;
                    tcp.set_nodelay(true)?;
                    Ok(TokioConn::new(tcp, None))
                }
                Security::Tls(t) => {
                    let host = addr.rsplit_once(':').map_or(addr.as_str(), |(h, _)| h);
                    let host = host.trim_start_matches('[').trim_end_matches(']');
                    let name = ServerName::try_from(host.to_owned())
                        .map_err(|e| io::Error::new(ErrorKind::InvalidInput, format!("{host}: {e}")))?;
                    let tcp = tokio::net::TcpStream::connect(&addr).await?;
                    tcp.set_nodelay(true)?;
                    let connector = tokio_rustls::TlsConnector::from(t.client(link));
                    let tls = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(name, tcp))
                        .await
                        .map_err(|_| io::Error::new(ErrorKind::TimedOut, "TLS handshake timed out"))??;
                    let peer = checked(link, tls.get_ref().1.peer_certificates())?;
                    Ok(TokioConn::new(tls, peer))
                }
            }
        }
    }

    fn verifies_peers(&self) -> bool {
        matches!(self.security, Security::Tls(_))
    }
}

impl Listener for TokioListener {
    type Conn = TokioConn;

    fn local_addr(&self) -> String {
        self.addr.clone()
    }

    async fn accept(&self) -> io::Result<(TokioConn, String)> {
        self.ready.lock().await.recv().await.ok_or_else(|| io::Error::other("listener closed"))
    }
}

impl Conn for TokioConn {
    async fn send(&self, frame: Bytes) -> io::Result<()> {
        if frame.len() > MAX_FRAME {
            return Err(io::Error::new(ErrorKind::InvalidInput, "frame too large"));
        }
        let mut wr = self.wr.lock().await;
        wr.write_all(&(frame.len() as u32).to_be_bytes()).await?;
        wr.write_all(&frame).await?;
        wr.flush().await
    }

    async fn recv(&self) -> io::Result<Option<Bytes>> {
        let mut rd = self.rd.lock().await;
        let mut len = [0u8; 4];
        // A clean close lands between frames; anywhere else it is an error.
        // A TLS peer that goes away without close_notify ends the stream
        // with UnexpectedEof: between frames that is a close like TCP's FIN,
        // and a frame cut short still fails below.
        let n = match rd.read(&mut len).await {
            Ok(n) => n,
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => 0,
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Ok(None);
        }
        rd.read_exact(&mut len[n..]).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len > MAX_FRAME {
            return Err(io::Error::new(ErrorKind::InvalidData, format!("frame of {len} bytes exceeds the limit")));
        }
        let mut buf = BytesMut::zeroed(len);
        rd.read_exact(&mut buf).await?;
        Ok(Some(buf.freeze()))
    }

    fn peer_identity(&self) -> Option<PeerIdentity> {
        self.peer.clone()
    }
}

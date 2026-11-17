//! A small HTTP/1.1 listener for the operator's endpoint (ADR 0117): GET and
//! HEAD, one request per connection, answers built by the caller. Plain on a
//! loopback address; anywhere else TLS 1.3 with the runtime's certificate,
//! where a client certificate from the cluster's CA is asked for but not
//! required, so a Kubernetes probe without one still gets through, and the
//! caller decides what a request without one may see.

use std::future::Future;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use lotse_rt::{CertInfo, Link, PeerIdentity};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::TokioRuntime;
use crate::net::{Security, cert_info};
use crate::tls::identity_of;

/// One request to the operator's endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// `GET` or `HEAD`; other methods are refused before the handler.
    pub method: String,
    /// The path, without its query.
    pub path: String,
    /// Whether the request came over TLS.
    pub tls: bool,
    /// Who the client certificate names, if one was shown.
    pub identity: Option<PeerIdentity>,
    /// What that certificate says about itself.
    pub cert: Option<CertInfo>,
}

/// An answer: status, content type and body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// 200, 403, 404, 503 and so on.
    pub status: u16,
    /// The `Content-Type`.
    pub content_type: String,
    /// The body; left out for `HEAD`.
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// A plain-text answer.
    pub fn text(status: u16, body: impl Into<String>) -> HttpResponse {
        HttpResponse { status, content_type: "text/plain; charset=utf-8".into(), body: body.into().into_bytes() }
    }
}

/// A bound HTTP listener, not yet serving.
#[derive(Debug)]
pub struct HttpListener {
    tcp: tokio::net::TcpListener,
    security: Option<Arc<crate::net::Shared>>,
    addr: String,
}

/// How long a client has to send its request, or a TLS handshake to finish.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest request head taken.
const MAX_HEAD: usize = 8 * 1024;
/// Connections served at once; more wait in the kernel's backlog.
const MAX_CONNECTIONS: usize = 32;

impl TokioRuntime {
    /// Binds the operator's HTTP endpoint at `addr`: plain if every address
    /// it resolves to is a loopback address, otherwise TLS, which the
    /// runtime must have ([`TokioRuntime::with_tls`]) with a certificate.
    /// Certificates reloaded into the runtime reach the next handshake.
    pub async fn bind_http(&self, addr: &str) -> io::Result<HttpListener> {
        let resolved: Vec<std::net::SocketAddr> = tokio::net::lookup_host(addr).await?.collect();
        let loopback = !resolved.is_empty() && resolved.iter().all(|a| a.ip().is_loopback());
        let security = if loopback {
            None
        } else {
            match &self.security {
                Security::Tls(t) if t.current().server(Link::Client).is_some() => Some(t.clone()),
                _ => {
                    return Err(io::Error::new(
                        ErrorKind::PermissionDenied,
                        format!(
                            "{addr} is not a loopback address; the metrics endpoint serves plain HTTP only on \
                             loopback, configure TLS"
                        ),
                    ));
                }
            }
        };
        let tcp = tokio::net::TcpListener::bind(addr).await?;
        let addr = tcp.local_addr()?.to_string();
        Ok(HttpListener { tcp, security, addr })
    }
}

impl HttpListener {
    /// The address it is bound to (with the port the OS chose for port 0).
    pub fn local_addr(&self) -> &str {
        &self.addr
    }

    /// Whether it speaks TLS.
    pub fn tls(&self) -> bool {
        self.security.is_some()
    }

    /// Serves requests with `handler` until the future is dropped.
    pub async fn serve<F, Fut>(self, handler: F)
    where
        F: Fn(HttpRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HttpResponse> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let slots = Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
        loop {
            let permit = match slots.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let (stream, peer) = match self.tcp.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "accepting a metrics connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let (handler, security) = (handler.clone(), self.security.clone());
            tokio::spawn(async move {
                let _permit = permit;
                let served = tokio::time::timeout(REQUEST_TIMEOUT, async {
                    match security {
                        None => one(stream, false, None, None, &*handler).await,
                        Some(shared) => {
                            let Some(cfg) = shared.current().server(Link::Client) else { return Ok(()) };
                            let tls = tokio_rustls::TlsAcceptor::from(cfg).accept(stream).await?;
                            let first = tls.get_ref().1.peer_certificates().and_then(|c| c.first()).cloned();
                            let identity = first.as_ref().and_then(identity_of);
                            let cert = first.as_ref().and_then(|c| cert_info(c.as_ref()));
                            one(tls, true, identity, cert, &*handler).await
                        }
                    }
                })
                .await;
                match served {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::debug!(%peer, error = %e, "metrics request failed"),
                    Err(_) => tracing::debug!(%peer, "metrics request timed out"),
                }
            });
        }
    }
}

/// Reads one request from `stream`, answers it and closes.
async fn one<S, F, Fut>(
    mut stream: S,
    tls: bool,
    identity: Option<PeerIdentity>,
    cert: Option<CertInfo>,
    handler: &F,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: Fn(HttpRequest) -> Fut,
    Fut: Future<Output = HttpResponse>,
{
    let mut head = Vec::with_capacity(512);
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        if head.len() > MAX_HEAD {
            return write(&mut stream, false, HttpResponse::text(431, "request head too large\n")).await;
        }
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        head.extend_from_slice(&buf[..n]);
    }
    let line = head.split(|b| *b == b'\r').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.split(' ');
    let (method, target, version) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""), parts.next());
    if !version.is_some_and(|v| v.starts_with("HTTP/1.")) || !target.starts_with('/') {
        return write(&mut stream, false, HttpResponse::text(400, "bad request\n")).await;
    }
    let head_only = method == "HEAD";
    if method != "GET" && !head_only {
        return write(&mut stream, false, HttpResponse::text(405, "only GET and HEAD\n")).await;
    }
    let path = target.split('?').next().unwrap_or(target).to_string();
    let response = handler(HttpRequest { method: method.to_string(), path, tls, identity, cert }).await;
    write(&mut stream, head_only, response).await
}

async fn write<S: AsyncWrite + Unpin>(stream: &mut S, head_only: bool, r: HttpResponse) -> io::Result<()> {
    let reason = match r.status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "",
    };
    let mut out = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        r.status,
        r.content_type,
        r.body.len()
    )
    .into_bytes();
    if !head_only {
        out.extend_from_slice(&r.body);
    }
    stream.write_all(&out).await?;
    stream.shutdown().await
}

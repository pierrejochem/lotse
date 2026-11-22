//! `lotse mcp --listen` (ADR 0134): MCP over Streamable HTTP, one endpoint,
//! `POST /mcp`, for a shared deployment that several people and agents
//! reach over the network.
//!
//! The process holds no credential of its own. Every request carries one in
//! its `Authorization` header and is served only by the client that
//! credential signed in ([`ClientCache`]). A layer in front of the MCP
//! service checks the `Origin` (against DNS rebinding: browsers send one,
//! and none is allowed unless named) and then the credential, before the
//! request reaches anything that speaks MCP; it takes the header off the
//! request, so nothing after it holds the credential. Plain HTTP only on
//! loopback (rule 7); elsewhere TLS 1.3 with the certificate given.

use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use lotse_client::Client;
use lotse_rt_tokio::TokioRuntime;
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::cache::{ClientCache, CredentialKey, HttpCredential, SignInError};
use super::{Reach, Server, SignIn, client};

/// What `lotse mcp --listen` says when it is given a credential of its own.
pub const OWN_CREDENTIAL: &str = "lotse mcp --listen holds no credential of its own: every request signs in with \
     its own Authorization header. Unset LOTSE_TOKEN, LOTSE_USER and LOTSE_PASSWORD and leave out --cert and --key";

/// How long a TLS handshake may take.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// TLS handshakes in progress at once; more wait in the kernel's backlog.
const MAX_HANDSHAKES: usize = 64;
/// How often clients unused for the cache's idle time are closed.
const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// Where and how to listen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listen {
    /// `host:port`.
    pub addr: String,
    /// The certificate chain and key files (PEM), for TLS.
    pub tls: Option<(PathBuf, PathBuf)>,
    /// Browser origins allowed; a request with any other `Origin` is
    /// refused.
    pub allow_origins: Vec<String>,
}

impl Listen {
    /// Checks the flags: certificate and key together, and without them a
    /// loopback address only (every address `addr` resolves to).
    /// `own_credential` says whether a credential for the process itself
    /// was given, which HTTP mode refuses: a credential nobody uses is one
    /// more to leak, and taking it silently would let an operator believe
    /// it is used.
    pub fn new(
        addr: String,
        tls_cert: Option<PathBuf>,
        tls_key: Option<PathBuf>,
        allow_origins: Vec<String>,
        own_credential: bool,
    ) -> Result<Listen, String> {
        if own_credential {
            return Err(OWN_CREDENTIAL.into());
        }
        let tls = match (tls_cert, tls_key) {
            (Some(c), Some(k)) => Some((c, k)),
            (None, None) => None,
            _ => return Err("--tls-cert and --tls-key go together".into()),
        };
        if let Some(o) = allow_origins.iter().find(|o| o.trim().is_empty()) {
            return Err(format!("--allow-origin {o:?}: an origin such as https://console.example"));
        }
        if tls.is_none() && !loopback(&addr)? {
            return Err(format!(
                "{addr} is not a loopback address: lotse mcp serves plain HTTP only on loopback; give --tls-cert \
                 <file> --tls-key <file> to serve HTTPS"
            ));
        }
        Ok(Listen { addr, tls, allow_origins })
    }
}

/// Whether every address `addr` resolves to is a loopback address.
fn loopback(addr: &str) -> Result<bool, String> {
    let resolved: Vec<SocketAddr> =
        addr.to_socket_addrs().map_err(|e| format!("--listen {addr}: {e}; give host:port"))?.collect();
    Ok(!resolved.is_empty() && resolved.iter().all(|a| a.ip().is_loopback()))
}

/// The signed-in client a request carries to the MCP handler, put there by
/// the layer once the request's own credential signed in.
#[derive(Clone)]
pub(crate) struct Signed {
    pub(crate) client: Arc<Client<TokioRuntime>>,
    pub(crate) key: CredentialKey,
    pub(crate) cache: Arc<ClientCache>,
}

impl std::fmt::Debug for Signed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Signed")
    }
}

impl Signed {
    /// The cluster refused the credential during a tool call (revoked or
    /// expired in the meantime): the next request signs in afresh.
    pub(crate) fn refused(&self) {
        self.cache.forget(&self.key, &self.client);
    }
}

/// What the layer checks with.
struct Guard {
    cache: Arc<ClientCache>,
    allow_origins: Vec<String>,
}

/// The 401 every request without a credential the cluster accepts gets.
/// The credential is never echoed.
fn unauthorized(why: &str) -> Response {
    let mut r = (
        StatusCode::UNAUTHORIZED,
        format!("{why}: sign in with Authorization: Bearer <access token> or Basic <user:password>\n"),
    )
        .into_response();
    let h = r.headers_mut();
    h.append(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer realm=\"lotse\""));
    h.append(header::WWW_AUTHENTICATE, HeaderValue::from_static("Basic realm=\"lotse\", charset=\"UTF-8\""));
    r
}

/// The layer: `Origin`, then the credential, then the MCP service with the
/// credential's own client.
async fn guard(State(g): State<Arc<Guard>>, mut req: Request, next: Next) -> Response {
    if let Some(origin) = req.headers().get(header::ORIGIN) {
        let allowed = origin.to_str().is_ok_and(|o| g.allow_origins.iter().any(|a| a.eq_ignore_ascii_case(o)));
        if !allowed {
            return (
                StatusCode::FORBIDDEN,
                "this origin is not allowed: lotse mcp --allow-origin names those that are\n",
            )
                .into_response();
        }
    }
    let credential = req
        .headers_mut()
        .remove(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok().and_then(HttpCredential::from_authorization));
    let Some(credential) = credential else {
        return unauthorized("no credential");
    };
    match g.cache.get_or_sign_in(&credential).await {
        Ok(client) => {
            req.extensions_mut().insert(Signed { client, key: credential.key(), cache: g.cache.clone() });
            next.run(req).await
        }
        Err(SignInError::Refused) => unauthorized("the cluster did not accept the credential"),
        Err(e @ SignInError::Unreachable(_)) => {
            tracing::warn!(error = %e, "signing in for an HTTP request failed");
            (StatusCode::SERVICE_UNAVAILABLE, "the cluster could not be reached; try again\n").into_response()
        }
    }
}

/// TLS 1.3 with the chain and key in `cert` and `key`.
fn server_tls(cert: &PathBuf, key: &PathBuf) -> Result<Arc<rustls::ServerConfig>, String> {
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .and_then(|i| i.collect())
        .map_err(|e| format!("--tls-cert {}: {e}", cert.display()))?;
    if chain.is_empty() {
        return Err(format!("--tls-cert {}: no certificate in it", cert.display()));
    }
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| format!("--tls-key {}: {e}", key.display()))?;
    let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| format!("--tls-cert and --tls-key: {e}"))?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// A listener that hands axum connections whose TLS handshake finished;
/// handshakes run in tasks of their own, so a slow one holds up no other.
struct TlsListener {
    accepted: tokio::sync::mpsc::Receiver<(tokio_rustls::server::TlsStream<tokio::net::TcpStream>, SocketAddr)>,
    addr: SocketAddr,
}

impl TlsListener {
    fn new(tcp: tokio::net::TcpListener, cfg: Arc<rustls::ServerConfig>) -> std::io::Result<TlsListener> {
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
                        tracing::warn!(error = %e, "accepting an MCP connection failed");
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
                        Ok(Err(e)) => tracing::debug!(%peer, error = %e, "TLS handshake failed"),
                        Err(_) => tracing::debug!(%peer, "TLS handshake timed out"),
                    }
                });
            }
        });
        Ok(TlsListener { accepted, addr })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.accepted.recv().await {
            Some(c) => c,
            // The accepting task ended: nothing more comes.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.addr)
    }
}

/// Serves MCP over Streamable HTTP at `listen` until `shutdown` completes.
pub async fn serve_http(
    reach: Reach,
    listen: Listen,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    // A client is made per credential later; make one now, without a
    // credential and without connecting, so a missing CA shows at start.
    drop(client(&reach, SignIn::Token(String::new()))?);
    let tls = match &listen.tls {
        Some((cert, key)) => Some(server_tls(cert, key)?),
        None => None,
    };
    let cache = Arc::new(ClientCache::new(reach));
    let sweeper = {
        let cache = cache.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP_EVERY).await;
                cache.sweep();
            }
        })
    };
    let mut config = StreamableHttpServerConfig::default()
        // Stateless: nothing a tool needs is kept between requests, and
        // each request brings its own credential.
        .with_legacy_session_mode(false)
        .with_json_response(true)
        // The layer checks the Origin; this service never sees a refused one.
        .disable_allowed_origins();
    if tls.is_some() {
        // A client checks the certificate's name, which DNS rebinding cannot
        // forge; the hosts a TLS listener is reached by are the operator's.
        config = config.disable_allowed_hosts();
    }
    let service =
        StreamableHttpService::new(|| Ok(Server::per_request()), Arc::new(NeverSessionManager::default()), config);
    let guard_state = Arc::new(Guard { cache, allow_origins: listen.allow_origins.clone() });
    let app = axum::Router::new()
        .route_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state(guard_state, guard));
    let tcp =
        tokio::net::TcpListener::bind(&listen.addr).await.map_err(|e| format!("--listen {}: {e}", listen.addr))?;
    let addr = tcp.local_addr().map_err(|e| e.to_string())?;
    tracing::info!(%addr, tls = tls.is_some(), "serving MCP over Streamable HTTP at /mcp");
    let served = match tls {
        None => axum::serve(tcp, app).with_graceful_shutdown(shutdown).await,
        Some(cfg) => {
            let listener = TlsListener::new(tcp, cfg).map_err(|e| e.to_string())?;
            axum::serve(listener, app).with_graceful_shutdown(shutdown).await
        }
    };
    sweeper.abort();
    served.map_err(|e| format!("MCP over HTTP: {e}"))
}

#[cfg(test)]
mod tests;

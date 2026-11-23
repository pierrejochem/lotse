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

use super::cache::{ClientCache, CredentialKey, HttpCredential, SignInError};
use super::{Reach, Server, SignIn, client};

/// What `lotse mcp --listen` says when it is given a credential of its own.
pub const OWN_CREDENTIAL: &str = "lotse mcp --listen holds no credential of its own: every request signs in with \
     its own Authorization header. Unset LOTSE_TOKEN, LOTSE_USER and LOTSE_PASSWORD and leave out --cert and --key";

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
        if allow_origins.iter().any(|o| o.trim().eq_ignore_ascii_case("null")) {
            return Err("--allow-origin null would admit sandboxed iframes and file: pages from anywhere; name the \
                        origin instead, such as https://console.example"
                .into());
        }
        if tls.is_none() && !loopback(&addr)? {
            return Err(format!(
                "{addr} is not a loopback address: lotse mcp serves plain HTTP only on loopback; give --tls-cert \
                 <file> --tls-key <file> to serve HTTPS"
            ));
        }
        // A browser's Origin has no spaces around it.
        let allow_origins = allow_origins.iter().map(|o| o.trim().to_string()).collect();
        Ok(Listen { addr, tls, allow_origins })
    }
}

impl Listen {
    /// What to warn about at start: an allowed origin that no browser ever
    /// sends, so it can never match. A browser's `Origin` is
    /// `scheme://host[:port]`, without a path or a trailing slash.
    pub fn warnings(&self) -> Vec<String> {
        self.allow_origins
            .iter()
            .map(|o| o.trim())
            .filter(|o| match o.split_once("://") {
                Some((scheme, rest)) => scheme.is_empty() || rest.is_empty() || rest.contains('/'),
                None => true,
            })
            .map(|o| {
                format!(
                    "--allow-origin {o} can never match: a browser sends scheme://host[:port], without a path or a \
                     trailing slash"
                )
            })
            .collect()
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
        // Bounded sign-in work: a flood of made-up credentials gets a
        // quick refusal, not a queue.
        Err(SignInError::Busy) => {
            let mut r = (StatusCode::SERVICE_UNAVAILABLE, "too many sign-ins at once; try again\n").into_response();
            r.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            r
        }
        Err(e @ SignInError::Unreachable(_)) => {
            tracing::warn!(error = %e, "signing in for an HTTP request failed");
            (StatusCode::SERVICE_UNAVAILABLE, "the cluster could not be reached; try again\n").into_response()
        }
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
        Some((cert, key)) => {
            Some(lotse_rt_tokio::https_config(cert, key).map_err(|e| format!("--tls-cert and --tls-key: {e}"))?)
        }
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
            let listener = lotse_rt_tokio::HttpsListener::new(tcp, cfg, "MCP").map_err(|e| e.to_string())?;
            axum::serve(listener, app).with_graceful_shutdown(shutdown).await
        }
    };
    sweeper.abort();
    served.map_err(|e| format!("MCP over HTTP: {e}"))
}

#[cfg(test)]
mod tests;

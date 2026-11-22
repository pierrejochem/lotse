//! `lotse mcp` (ADR 0134): the Model Context Protocol around the tools of
//! `lotse-mcp`, through `rmcp`, the official Rust MCP SDK.
//!
//! Over stdio the process acts for one principal, the one its credential
//! names, and holds no rights of its own: every tool call is a request of
//! that principal's client, which the cluster checks as it checks the same
//! request over the native protocol. Standard output carries only JSON-RPC;
//! logs go to standard error. Over Streamable HTTP ([`http`]) it holds no
//! credential: each request is served by the client its own credential
//! signed in ([`cache`]).

use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use lotse_client::{Client, ClientConfig, Credentials};
use lotse_mcp::{ToolSpec, Tools};
use lotse_rt_tokio::{TlsConfig, TokioRuntime};
use rmcp::ErrorData as McpError;
use rmcp::ServiceExt as _;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation, JsonObject, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerConfig, Tool, ToolAnnotations,
};
use rmcp::service::{RequestContext, RoleServer, ServerInitializeError};
use serde_json::Value;

pub mod cache;
pub mod http;

/// What `lotse mcp` says when it has no credential: the three ways to give
/// one.
pub const NO_CREDENTIAL: &str = "lotse mcp acts as one principal and needs its credential: LOTSE_TOKEN (an access \
     token), or LOTSE_USER and LOTSE_PASSWORD (SCRAM-SHA-512), or --cert <file> --key <file> (a client certificate)";

/// The protocol versions served: 2026-07-28 without a session
/// (`server/discover`, metadata in every request), and 2025-11-25 for
/// clients that still open a session with `initialize`.
const VERSIONS: &[ProtocolVersion] = &[ProtocolVersion::V_2025_11_25, ProtocolVersion::V_2026_07_28];

/// Who `lotse mcp` signs in as.
#[derive(Clone, PartialEq, Eq)]
pub enum SignIn {
    /// An access token (`LOTSE-TOKEN`).
    Token(String),
    /// A SCRAM-SHA-512 user and password.
    Scram {
        /// The user.
        user: String,
        /// The password.
        password: String,
    },
    /// A client certificate and its key.
    Certificate {
        /// The certificate file.
        cert: PathBuf,
        /// The key file.
        key: PathBuf,
    },
}

impl std::fmt::Debug for SignIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the token or the password.
        match self {
            SignIn::Token(_) => f.write_str("Token"),
            SignIn::Scram { user, .. } => write!(f, "Scram({user})"),
            SignIn::Certificate { cert, .. } => write!(f, "Certificate({})", cert.display()),
        }
    }
}

impl SignIn {
    /// The one credential given: `LOTSE_TOKEN`, `LOTSE_USER` with
    /// `LOTSE_PASSWORD`, or `--cert` with `--key`. None, half of one, or
    /// more than one is refused, so the process never acts as a principal
    /// other than the one meant.
    pub fn choose(
        token: Option<String>,
        user: Option<String>,
        password: Option<String>,
        cert: Option<PathBuf>,
        key: Option<PathBuf>,
    ) -> Result<SignIn, String> {
        let mut found = Vec::new();
        if let Some(t) = token.filter(|t| !t.is_empty()) {
            found.push(SignIn::Token(t));
        }
        match (user.filter(|u| !u.is_empty()), password) {
            (Some(user), Some(password)) => found.push(SignIn::Scram { user, password }),
            (Some(_), None) => return Err("LOTSE_USER needs LOTSE_PASSWORD, the user's password".into()),
            (None, Some(_)) if found.is_empty() && cert.is_none() => {
                return Err("LOTSE_PASSWORD needs LOTSE_USER, the user it belongs to".into());
            }
            _ => {}
        }
        match (cert, key) {
            (Some(cert), Some(key)) => found.push(SignIn::Certificate { cert, key }),
            (None, None) => {}
            _ => return Err("--cert and --key go together".into()),
        }
        match found.len() {
            0 => Err(NO_CREDENTIAL.into()),
            1 => Ok(found.remove(0)),
            _ => Err("give one credential, not several: LOTSE_TOKEN, or LOTSE_USER and LOTSE_PASSWORD, or \
                      --cert and --key"
                .into()),
        }
    }
}

/// Where the cluster is and how to reach it.
#[derive(Debug, Clone)]
pub struct Reach {
    /// Client addresses of some of its nodes.
    pub bootstrap: Vec<String>,
    /// The cluster's CA certificate; without one, only plain TCP on
    /// loopback, and only when asked for.
    pub ca: Option<PathBuf>,
    /// Plain TCP, which the runtime allows only to loopback addresses.
    pub plaintext_on_loopback: bool,
}

/// The client the tools act through, signed in as `sign_in`. TLS always,
/// unless plain TCP on loopback was asked for and there is no CA.
pub fn client(reach: &Reach, sign_in: SignIn) -> Result<Client<TokioRuntime>, String> {
    if reach.bootstrap.is_empty() {
        return Err("no node address to reach the cluster at".into());
    }
    let rt = TokioRuntime::new(std::env::temp_dir());
    let (identity, credentials) = match sign_in {
        SignIn::Token(t) => (None, Some(Credentials::Token(t))),
        SignIn::Scram { user, password } => (None, Some(Credentials::Scram { user, password })),
        SignIn::Certificate { cert, key } => (Some((cert, key)), None),
    };
    let rt = match (&reach.ca, reach.plaintext_on_loopback) {
        (Some(_), true) => {
            return Err("--plaintext-on-loopback goes without a CA: the cluster's CA means TLS".into());
        }
        (Some(ca), false) => {
            rt.with_tls(TlsConfig::from_files(ca, identity).map_err(|e| format!("{}: {e}", ca.display()))?)
        }
        (None, true) if identity.is_some() => {
            return Err("a client certificate needs TLS: give the cluster's CA, not --plaintext-on-loopback".into());
        }
        (None, true) => rt.plaintext_on_loopback(),
        (None, false) => {
            return Err("no CA to reach the cluster with TLS: the cluster file's [tls] ca, or --ca <file>; plain \
                        TCP only with --plaintext-on-loopback, to a loopback address"
                .into());
        }
    };
    let cfg = ClientConfig {
        retry_for: std::time::Duration::from_secs(15),
        credentials,
        // `idempotent` stays on: the produce tool promises it.
        ..ClientConfig::new(reach.bootstrap.clone())
    };
    Ok(Client::new(rt, cfg))
}

/// The MCP server: the tools on a signed-in client.
#[derive(Debug)]
pub struct Server {
    acting: Acting,
}

/// Whose client a tool call acts through.
#[derive(Debug)]
enum Acting {
    /// One for the whole process (stdio).
    One(Arc<Client<TokioRuntime>>),
    /// The one each HTTP request's own credential signed in, which the
    /// layer in front put into the request ([`http::Signed`]).
    PerRequest,
}

impl Server {
    /// The tools acting through `client`.
    pub fn new(client: Arc<Client<TokioRuntime>>) -> Server {
        Server { acting: Acting::One(client) }
    }

    /// The tools acting through the client each request brings.
    pub fn per_request() -> Server {
        Server { acting: Acting::PerRequest }
    }
}

/// A tool as `tools/list` shows it, with its hints.
fn tool(spec: ToolSpec) -> Tool {
    let schema = match spec.input_schema {
        Value::Object(m) => m,
        _ => JsonObject::new(),
    };
    let hints = ToolAnnotations::new().read_only(spec.read_only).destructive(spec.destructive);
    Tool::new(spec.name, spec.description, Arc::new(schema)).with_annotations(hints)
}

impl rmcp::ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("lotse", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Tools for a Lotse cluster, each run as the principal this server signed in as and checked by \
                 the cluster like the same request over the native protocol. Destructive tools need confirm set \
                 to the resource's name.",
            )
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(VERSIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(Tools::<TokioRuntime>::list().into_iter().map(tool).collect()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let (client, signed) = match &self.acting {
            Acting::One(c) => (c.clone(), None),
            Acting::PerRequest => {
                let signed = context
                    .extensions
                    .get::<axum::http::request::Parts>()
                    .and_then(|p| p.extensions.get::<http::Signed>())
                    .cloned();
                // Never another credential's client, and none of its own:
                // without the request's client, no tool runs.
                let Some(signed) = signed else {
                    return Err(McpError::internal_error("the request brought no signed-in client", None));
                };
                (signed.client.clone(), Some(signed))
            }
        };
        let args = request.arguments.map_or(Value::Null, Value::Object);
        let r = Tools::new(client).call(&request.name, args).await;
        if r.is_error
            && r.structured["error"]["code"] == "AUTHENTICATION_FAILED"
            && let Some(s) = &signed
        {
            s.refused();
        }
        // A refusal is a result the agent reads, not a protocol error.
        let mut result = if r.is_error {
            CallToolResult::structured_error(r.structured)
        } else {
            CallToolResult::structured(r.structured)
        };
        result.content = vec![ContentBlock::text(r.text)];
        Ok(result.into())
    }
}

/// Serves MCP on standard input and output until the other end closes it.
pub async fn serve_stdio(client: Arc<Client<TokioRuntime>>) -> Result<(), String> {
    let running = match Server::new(client).serve(rmcp::transport::stdio()).await {
        Ok(r) => r,
        // Closed before it asked anything: nothing to serve.
        Err(ServerInitializeError::ConnectionClosed(_)) => return Ok(()),
        Err(e) => return Err(format!("MCP over stdio: {e}")),
    };
    running.waiting().await.map_err(|e| format!("MCP over stdio: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests;

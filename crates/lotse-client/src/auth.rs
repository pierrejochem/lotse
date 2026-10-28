//! Authenticating a connection: SASL SCRAM-SHA-512 or OAUTHBEARER right
//! after connecting, and again on the same connection before the session
//! the node granted ends.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::oauth;
use lotse_auth::scram::{self, ScramClient};
use lotse_proto::{ErrorCode, Request, Response, SaslAuthenticateRequest, SaslHandshakeRequest};
use lotse_rt::{Clock, Conn, Instant, Rng};

use crate::{CallError, Connection};

/// How a client proves who it is. A client certificate, if the runtime has
/// one, needs nothing here.
#[derive(Clone)]
pub enum Credentials {
    /// SASL SCRAM-SHA-512 with a user name and password.
    Scram {
        /// The user.
        user: String,
        /// The password; it never leaves the client.
        password: String,
    },
    /// SASL OAUTHBEARER: called for every authentication (on connecting
    /// and before each session ends), returns the current access token.
    OAuthBearer(Arc<dyn Fn() -> String + Send + Sync>),
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Credentials::Scram { user, .. } => f.debug_struct("Scram").field("user", user).finish_non_exhaustive(),
            Credentials::OAuthBearer(_) => f.write_str("OAuthBearer"),
        }
    }
}

/// When a connection's session must be renewed, and when it ends.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionTimes {
    pub(crate) renew_at: Instant,
    pub(crate) ends_at: Instant,
}

impl SessionTimes {
    /// For a session of `lifetime` that began no later than `start`:
    /// renewed after four fifths of it.
    fn new(start: Instant, lifetime: Duration) -> SessionTimes {
        SessionTimes { renew_at: start + lifetime * 4 / 5, ends_at: start + lifetime }
    }
}

/// A fresh client nonce: 24 secret bytes, hex.
fn nonce<R: Rng>(rt: &R) -> String {
    let mut b = [0u8; 24];
    rt.fill_secret(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

impl<C: Conn> Connection<C> {
    /// Authenticates this connection with `creds`. Returns when the session
    /// must be renewed and when it ends (`None`: it lasts the connection).
    pub(crate) async fn authenticate<R: Clock + Rng>(
        &self,
        rt: &R,
        creds: &Credentials,
        timeout: Duration,
    ) -> Result<Option<SessionTimes>, CallError> {
        let start = rt.now();
        let mechanism = match creds {
            Credentials::Scram { .. } => scram::MECHANISM,
            Credentials::OAuthBearer(_) => oauth::MECHANISM,
        };
        let req = Request::SaslHandshake(SaslHandshakeRequest { mechanism: mechanism.into() });
        match self.call(&req, rt, timeout).await? {
            Response::SaslHandshake(r) if r.error == ErrorCode::NONE => {}
            Response::SaslHandshake(r) => {
                return Err(CallError::Denied(format!(
                    "{mechanism} is not enabled on this node; it offers {}",
                    if r.mechanisms.is_empty() { "none".into() } else { r.mechanisms.join(", ") }
                )));
            }
            other => return Err(CallError::Lost(format!("unexpected response {other:?}"))),
        }
        let step =
            |bytes: Vec<u8>| Request::SaslAuthenticate(SaslAuthenticateRequest { auth_bytes: Bytes::from(bytes) });
        let lifetime = match creds {
            Credentials::Scram { user, password } => {
                let mut client = ScramClient::new(user, password, &nonce(rt));
                let first = self.sasl(step(client.first()), rt, timeout).await?;
                let last = client.respond(&first.0).map_err(|e| CallError::Denied(e.to_string()))?;
                let (server_final, lifetime) = self.sasl(step(last), rt, timeout).await?;
                client.verify(&server_final).map_err(|e| CallError::Denied(e.to_string()))?;
                lifetime
            }
            Credentials::OAuthBearer(token) => self.sasl(step(oauth::initial_response(&token())), rt, timeout).await?.1,
        };
        Ok((lifetime > 0).then(|| SessionTimes::new(start, Duration::from_millis(lifetime))))
    }

    /// One `SaslAuthenticate` step: the node's answer and session lifetime.
    async fn sasl<R: Clock>(&self, req: Request, rt: &R, timeout: Duration) -> Result<(Bytes, u64), CallError> {
        match self.call(&req, rt, timeout).await? {
            Response::SaslAuthenticate(r) if r.error == ErrorCode::NONE => Ok((r.auth_bytes, r.session_lifetime_ms)),
            Response::SaslAuthenticate(r) => Err(CallError::Denied(r.error_msg.unwrap_or_else(|| r.error.to_string()))),
            other => Err(CallError::Lost(format!("unexpected response {other:?}"))),
        }
    }
}

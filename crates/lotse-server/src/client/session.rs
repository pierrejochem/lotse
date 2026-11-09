//! Who one connection acts as, and how it gets there: a certificate, SASL
//! SCRAM-SHA-512 or SASL OAUTHBEARER, and authenticating again before the
//! session ends.
//!
//! All of this runs in the connection's own task. The main loop only ever
//! sees requests together with the principal they act as.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use lotse_auth::oauth;
use lotse_auth::scram::{self, ScramServer};
use lotse_proto::{
    ErrorCode, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use lotse_rt::{CertInfo, Clock, Http, Instant, PeerIdentity, Rng};

use crate::auth::{Auth, Principal};

enum Sasl {
    /// No exchange under way.
    Idle,
    /// `SaslHandshake` chose a mechanism; its first message comes next.
    Chosen(&'static str),
    /// SCRAM's first round is done.
    Scram(Box<ScramServer>),
}

/// One connection's authentication.
pub(super) struct Session<R> {
    auth: Arc<Auth<R>>,
    principal: Option<Principal>,
    /// The certificate the connection showed, if a cluster admin may revoke
    /// it (ADR 0090).
    cert: Option<CertInfo>,
    /// When the session ends; `None` for one that lasts the connection
    /// (a certificate, or no authentication required).
    until: Option<Instant>,
    /// The name a SASL login established; authenticating again must keep it.
    sasl_name: Option<String>,
    /// When the session's credential dates from, ms since the Unix epoch:
    /// a token's `iat` (0 without one), the time of a SCRAM login, or when
    /// the connection was made. A revocation after it ends the session
    /// (ADR 0070).
    since_ms: u64,
    sasl: Sasl,
}

/// What became of one `SaslAuthenticate` step.
pub(super) struct Step {
    pub(super) response: SaslAuthenticateResponse,
    /// The attempt failed: the connection closes after the answer.
    pub(super) close: bool,
}

impl<R: Clock + Http + Rng + Clone> Session<R> {
    /// A session for a connection whose other end proved `identity`.
    pub(super) fn new(
        auth: Arc<Auth<R>>,
        identity: Option<PeerIdentity>,
        cert: Option<CertInfo>,
        now_ms: u64,
    ) -> Session<R> {
        // Node certificates are revoked in the CRL file only (ADR 0090).
        let cert = cert.filter(|_| !matches!(identity, Some(PeerIdentity::Node(_))));
        let principal = match identity {
            Some(PeerIdentity::Node(id)) => Some(Principal::Node(id)),
            Some(PeerIdentity::Principal(name)) if auth.cfg.mtls => Some(Principal::User { name, groups: Vec::new() }),
            _ if !auth.cfg.required => Some(Principal::Anonymous),
            _ => None,
        };
        Session { auth, principal, cert, until: None, sasl_name: None, since_ms: now_ms, sasl: Sasl::Idle }
    }

    /// Who the connection acts as at `now`; `None` before authenticating or
    /// after the session ended.
    pub(super) fn principal(&self, now: Instant) -> Option<&Principal> {
        if self.until.is_some_and(|u| now >= u) {
            return None;
        }
        self.principal.as_ref()
    }

    /// Whether a revocation ended this session (ADR 0070): the connection
    /// closes, and the client authenticates afresh on a new one.
    pub(super) fn revoked(&self) -> bool {
        // A certificate a cluster admin revoked ends the connection whoever
        // it then authenticates as (ADR 0090).
        self.cert.as_ref().is_some_and(|c| self.auth.cert_revoked(c))
            || self.principal.as_ref().is_some_and(|p| self.auth.revoked(p, self.since_ms))
    }

    /// Set when revocations change, so a waiting connection checks again.
    pub(super) fn revocations_changed(&self) -> crate::signal::Signal {
        self.auth.revocations_changed()
    }

    pub(super) fn handshake(&mut self, req: &SaslHandshakeRequest) -> SaslHandshakeResponse {
        let mechanisms = self.auth.cfg.mechanisms();
        let chosen = match req.mechanism.as_str() {
            scram::MECHANISM if self.auth.cfg.scram => Some(scram::MECHANISM),
            oauth::MECHANISM if self.auth.cfg.oidc.is_some() => Some(oauth::MECHANISM),
            _ => None,
        };
        match chosen {
            Some(m) => {
                self.sasl = Sasl::Chosen(m);
                SaslHandshakeResponse { error: ErrorCode::NONE, mechanisms }
            }
            None => {
                self.sasl = Sasl::Idle;
                SaslHandshakeResponse { error: ErrorCode::UNSUPPORTED_SASL_MECHANISM, mechanisms }
            }
        }
    }

    pub(super) async fn authenticate(&mut self, rt: &R, req: &SaslAuthenticateRequest) -> Step {
        match std::mem::replace(&mut self.sasl, Sasl::Idle) {
            Sasl::Idle => failed("SaslHandshake comes first"),
            Sasl::Chosen(scram::MECHANISM) => {
                let mut server = ScramServer::new();
                let auth = self.auth.clone();
                match server.first(&req.auth_bytes, |u| auth.credential(u), &nonce(rt)) {
                    Ok(first) => {
                        self.sasl = Sasl::Scram(Box::new(server));
                        Step { response: answer(ErrorCode::NONE, None, first.into(), 0), close: false }
                    }
                    Err(_) => failed("authentication failed"),
                }
            }
            Sasl::Scram(mut server) => match server.last(&req.auth_bytes) {
                Ok((name, last)) => self.established(rt, name, Vec::new(), None, unix_ms(rt), last.into()),
                Err(_) => failed("authentication failed"),
            },
            Sasl::Chosen(_) => {
                let token = match oauth::token_of(&req.auth_bytes) {
                    Ok(t) => t,
                    Err(e) => return failed(&e.to_string()),
                };
                match self.auth.verify_token(&token).await {
                    Ok(v) => {
                        let left = Duration::from_secs(v.expires_at).saturating_sub(rt.unix_time());
                        // Without `iat` a token counts as issued at time 0:
                        // any revocation of its principal covers it.
                        let issued = v.issued_at.unwrap_or(0).saturating_mul(1000);
                        self.established(rt, v.principal, v.groups, Some(left), issued, Bytes::new())
                    }
                    Err(e) => failed(&e.to_string()),
                }
            }
        }
    }

    /// A SASL login succeeded for `name`: the session runs until
    /// `max_session`, or until `left` if that is sooner.
    fn established(
        &mut self,
        rt: &R,
        name: String,
        groups: Vec<String>,
        left: Option<Duration>,
        since_ms: u64,
        bytes: Bytes,
    ) -> Step {
        // Authenticating again renews the session; it cannot change who
        // the connection is.
        if self.sasl_name.as_ref().is_some_and(|n| *n != name) {
            return failed("authenticating again must keep the same principal");
        }
        let lifetime = left.map_or(self.auth.cfg.max_session, |l| l.min(self.auth.cfg.max_session));
        if lifetime.is_zero() {
            return failed("the token has expired");
        }
        let principal = Principal::User { name: name.clone(), groups };
        if self.auth.revoked(&principal, since_ms) {
            return failed("the principal's sessions were revoked after this credential was issued");
        }
        self.until = Some(rt.now() + lifetime);
        self.sasl_name = Some(name);
        self.since_ms = since_ms;
        self.principal = Some(principal);
        let ms = u64::try_from(lifetime.as_millis()).unwrap_or(u64::MAX);
        Step { response: answer(ErrorCode::NONE, None, bytes, ms), close: false }
    }
}

/// Now, in ms since the Unix epoch.
fn unix_ms<R: Clock>(rt: &R) -> u64 {
    u64::try_from(rt.unix_time().as_millis()).unwrap_or(u64::MAX)
}

fn answer(error: ErrorCode, why: Option<&str>, auth_bytes: Bytes, ms: u64) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse { error, error_msg: why.map(str::to_owned), auth_bytes, session_lifetime_ms: ms }
}

fn failed(why: &str) -> Step {
    Step { response: answer(ErrorCode::AUTHENTICATION_FAILED, Some(why), Bytes::new(), 0), close: true }
}

/// A fresh server nonce: 24 secret bytes, hex.
fn nonce<R: Rng>(rt: &R) -> String {
    let mut b = [0u8; 24];
    rt.fill_secret(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

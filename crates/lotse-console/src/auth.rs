//! Who is calling.
//!
//! The console never acts with its own authority: every call to a cluster
//! carries the signed-in user's [`Identity`], and the brokers' roles decide
//! (ADR 0019).
//!
//! - `oidc`: the user signs in at the organisation's identity provider
//!   (Authorization Code with PKCE). The console keeps the session on the
//!   server, behind an HttpOnly cookie, and passes the user's access token
//!   to the clusters with SASL OAUTHBEARER. When the token expires, the
//!   console renews it with the refresh token or asks the user to sign in
//!   again.
//! - `dev`: everyone is one fixed user, signed in to the clusters with that
//!   user's SCRAM password if one is configured. For a machine only you can
//!   reach.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use axum::extract::{FromRequestParts, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use lotse_rt::{Http as _, Rng as _};
use lotse_rt_tokio::TokioRuntime;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::AppState;
use crate::error::ApiError;

/// The session cookie's name.
pub const COOKIE: &str = "lotse_session";

/// A sign-in that has not come back from the identity provider within this
/// long is dropped.
const LOGIN_TIMEOUT: Duration = Duration::from_secs(600);

/// A token is renewed this long before it expires.
const RENEW_BEFORE: Duration = Duration::from_secs(30);

/// How users sign in.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Local development: everyone is `user`. Never expose this.
    Dev {
        /// Principal used for every request.
        user: String,
        /// Its SCRAM password at the clusters, if they require
        /// authentication.
        #[serde(default)]
        password: Option<String>,
    },
    /// OpenID Connect login.
    Oidc(OidcLogin),
}

/// Where and how users sign in with OpenID Connect.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OidcLogin {
    /// The issuer; its discovery document names the endpoints. The brokers
    /// trust the same issuer.
    pub issuer: String,
    /// The console's client id at the issuer.
    pub client_id: String,
    /// Its secret, for a confidential client; a public client relies on
    /// PKCE alone.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// The console's callback, as registered at the issuer:
    /// `https://<console>/auth/callback`.
    pub redirect_url: String,
    /// Scopes to ask for.
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    /// The claim that names the principal; the brokers must use the same.
    #[serde(default = "default_principal_claim")]
    pub principal_claim: String,
    /// CA certificates to trust for the identity provider, besides the
    /// system's: for a private one.
    #[serde(default)]
    pub ca_file: Option<std::path::PathBuf>,
}

fn default_scopes() -> Vec<String> {
    ["openid", "profile", "email", "offline_access"].map(String::from).to_vec()
}

fn default_principal_claim() -> String {
    "sub".into()
}

impl AuthConfig {
    /// `dev` or `oidc`.
    pub fn mode(&self) -> &'static str {
        match self {
            AuthConfig::Dev { .. } => "dev",
            AuthConfig::Oidc(_) => "oidc",
        }
    }
}

/// A secret passed through to the cluster. Never logged or serialised.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Wraps `s`.
    pub fn new(s: impl Into<String>) -> Secret {
        Secret(s.into())
    }

    /// The raw secret, for the SASL handshake only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(…)")
    }
}

/// How the console signs in to the clusters as the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Login {
    /// Without credentials: for clusters that do not require
    /// authentication.
    None,
    /// SASL SCRAM-SHA-512 with this password.
    Password(Secret),
    /// SASL OAUTHBEARER with an access token.
    Token {
        /// The token.
        token: Secret,
        /// When it expires: of two tokens for one principal, the later
        /// expiring is the newer.
        expires: Instant,
    },
}

/// The user a request acts for.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Principal name the cluster authorises.
    pub principal: String,
    /// Name to show in the UI.
    pub display_name: String,
    /// How to sign in to the clusters as them.
    pub login: Login,
}

impl Identity {
    /// A principal that signs in with nothing (clusters without
    /// authentication, or tests).
    pub fn anonymous(principal: &str) -> Identity {
        Identity { principal: principal.into(), display_name: principal.into(), login: Login::None }
    }
}

/// One signed-in browser.
#[derive(Debug, Clone)]
struct Session {
    principal: String,
    display_name: String,
    access_token: Secret,
    refresh_token: Option<Secret>,
    expires: Instant,
}

/// A sign-in on its way through the identity provider.
#[derive(Debug, Clone)]
struct Pending {
    verifier: String,
    nonce: String,
    return_to: String,
    started: Instant,
}

/// The identity provider's endpoints, from its discovery document.
#[derive(Debug, Clone, Deserialize)]
struct Endpoints {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    #[serde(default)]
    end_session_endpoint: Option<String>,
}

/// What a token endpoint answers.
#[derive(Debug, Deserialize)]
struct Tokens {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Signed-in browsers and sign-ins in progress, for `oidc` mode.
#[derive(Debug)]
pub struct Sessions {
    cfg: OidcLogin,
    rt: TokioRuntime,
    endpoints: tokio::sync::OnceCell<Endpoints>,
    sessions: Mutex<BTreeMap<String, Session>>,
    pending: Mutex<BTreeMap<String, Pending>>,
}

/// Why signing in failed, for the user.
#[derive(Debug, thiserror::Error)]
pub enum SignInError {
    /// The identity provider could not be reached or answered nonsense.
    #[error("the identity provider is not usable: {0}")]
    Provider(String),
    /// The callback does not belong to a sign-in this console started.
    #[error("this sign-in was not started here, or took too long; start again")]
    Unknown,
    /// The identity provider refused, or its answer does not check out.
    #[error("the sign-in was refused: {0}")]
    Refused(String),
}

fn random(rt: &TokioRuntime, n: usize) -> String {
    let mut b = vec![0u8; n];
    rt.fill_secret(&mut b);
    B64URL.encode(b)
}

/// The claims of a JWT, unchecked: only for a token that came straight
/// from the token endpoint over TLS (OpenID Connect Core 3.1.3.7).
fn claims(jwt: &str) -> Option<serde_json::Value> {
    let payload = jwt.split('.').nth(1)?;
    serde_json::from_slice(&B64URL.decode(payload).ok()?).ok()
}

impl Sessions {
    /// Sessions for `cfg`, reaching the identity provider through `rt`.
    pub fn new(cfg: OidcLogin, rt: TokioRuntime) -> Sessions {
        Sessions {
            cfg,
            rt,
            endpoints: tokio::sync::OnceCell::new(),
            sessions: Mutex::new(BTreeMap::new()),
            pending: Mutex::new(BTreeMap::new()),
        }
    }

    async fn endpoints(&self) -> Result<&Endpoints, SignInError> {
        self.endpoints
            .get_or_try_init(|| async {
                let url = format!("{}/.well-known/openid-configuration", self.cfg.issuer.trim_end_matches('/'));
                let body = self.rt.get(&url).await.map_err(|e| SignInError::Provider(format!("{url}: {e}")))?;
                let e: Endpoints =
                    serde_json::from_slice(&body).map_err(|e| SignInError::Provider(format!("{url}: {e}")))?;
                if e.issuer != self.cfg.issuer {
                    return Err(SignInError::Provider(format!("{url} names another issuer, {}", e.issuer)));
                }
                Ok(e)
            })
            .await
    }

    /// Starts a sign-in: where to send the browser.
    pub async fn start(&self, return_to: &str) -> Result<String, SignInError> {
        let e = self.endpoints().await?;
        let (state, verifier, nonce) = (random(&self.rt, 24), random(&self.rt, 48), random(&self.rt, 24));
        let challenge = B64URL.encode(Sha256::digest(verifier.as_bytes()));
        // Only paths on this console: never send a user elsewhere.
        let return_to = if return_to.starts_with('/') && !return_to.starts_with("//") { return_to } else { "/" };
        {
            let mut p = self.pending.lock().expect("pending lock");
            p.retain(|_, x| x.started.elapsed() < LOGIN_TIMEOUT);
            p.insert(
                state.clone(),
                Pending { verifier, nonce: nonce.clone(), return_to: return_to.into(), started: Instant::now() },
            );
        }
        let q = |s: &str| url_encode(s);
        Ok(format!(
            "{}{}response_type=code&client_id={}&redirect_uri={}&scope={}&state={}&nonce={}&code_challenge={}&code_challenge_method=S256",
            e.authorization_endpoint,
            if e.authorization_endpoint.contains('?') { '&' } else { '?' },
            q(&self.cfg.client_id),
            q(&self.cfg.redirect_url),
            q(&self.cfg.scopes.join(" ")),
            q(&state),
            q(&nonce),
            q(&challenge),
        ))
    }

    /// Finishes a sign-in: the new session's id and where to go.
    pub async fn finish(&self, code: &str, state: &str) -> Result<(String, String), SignInError> {
        let pending = self.pending.lock().expect("pending lock").remove(state).ok_or(SignInError::Unknown)?;
        if pending.started.elapsed() >= LOGIN_TIMEOUT {
            return Err(SignInError::Unknown);
        }
        let e = self.endpoints().await?;
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", self.cfg.redirect_url.as_str()),
            ("client_id", self.cfg.client_id.as_str()),
            ("code_verifier", pending.verifier.as_str()),
        ];
        if let Some(s) = &self.cfg.client_secret {
            form.push(("client_secret", s.as_str()));
        }
        let tokens = self.exchange(&e.token_endpoint, &form).await?;
        let id =
            tokens.id_token.as_deref().and_then(claims).ok_or_else(|| SignInError::Refused("no ID token".into()))?;
        let claim = |k: &str| id.get(k).and_then(|v| v.as_str());
        if claim("iss") != Some(e.issuer.as_str()) {
            return Err(SignInError::Refused("the ID token is from another issuer".into()));
        }
        let aud_ok = match id.get("aud") {
            Some(serde_json::Value::String(a)) => *a == self.cfg.client_id,
            Some(serde_json::Value::Array(a)) => a.iter().any(|x| x.as_str() == Some(&self.cfg.client_id)),
            _ => false,
        };
        if !aud_ok || claim("nonce") != Some(pending.nonce.as_str()) {
            return Err(SignInError::Refused("the ID token is not for this sign-in".into()));
        }
        let principal = claim(&self.cfg.principal_claim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| SignInError::Refused(format!("the ID token has no {} claim", self.cfg.principal_claim)))?
            .to_owned();
        let display_name =
            ["name", "preferred_username", "email"].iter().find_map(|k| claim(k)).unwrap_or(&principal).to_owned();
        let sid = random(&self.rt, 32);
        let session = Session {
            principal,
            display_name,
            access_token: Secret(tokens.access_token),
            refresh_token: tokens.refresh_token.map(Secret),
            expires: Instant::now() + Duration::from_secs(tokens.expires_in.unwrap_or(300)),
        };
        self.sessions.lock().expect("sessions lock").insert(sid.clone(), session);
        Ok((sid, pending.return_to))
    }

    async fn exchange(&self, url: &str, form: &[(&str, &str)]) -> Result<Tokens, SignInError> {
        let body = self.rt.post_form(url, form).await.map_err(|e| SignInError::Refused(format!("{url}: {e}")))?;
        serde_json::from_slice(&body).map_err(|e| SignInError::Provider(format!("{url}: {e}")))
    }

    /// The identity behind session `sid`, renewing its token when it is
    /// about to expire; `None` if the user must sign in again.
    pub async fn identity(&self, sid: &str) -> Option<Identity> {
        let s = self.sessions.lock().expect("sessions lock").get(sid).cloned()?;
        let s = if s.expires.saturating_duration_since(Instant::now()) > RENEW_BEFORE {
            s
        } else {
            let renewed = self.renew(&s).await;
            let mut all = self.sessions.lock().expect("sessions lock");
            match renewed {
                Some(r) => {
                    all.insert(sid.to_owned(), r.clone());
                    r
                }
                None => {
                    all.remove(sid);
                    return None;
                }
            }
        };
        Some(Identity {
            principal: s.principal,
            display_name: s.display_name,
            login: Login::Token { token: s.access_token, expires: s.expires },
        })
    }

    async fn renew(&self, s: &Session) -> Option<Session> {
        let refresh = s.refresh_token.as_ref()?;
        let e = self.endpoints().await.ok()?;
        let mut form = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh.expose()),
            ("client_id", self.cfg.client_id.as_str()),
        ];
        if let Some(sec) = &self.cfg.client_secret {
            form.push(("client_secret", sec.as_str()));
        }
        let t = self.exchange(&e.token_endpoint, &form).await.ok()?;
        Some(Session {
            access_token: Secret(t.access_token),
            refresh_token: t.refresh_token.map(Secret).or_else(|| s.refresh_token.clone()),
            expires: Instant::now() + Duration::from_secs(t.expires_in.unwrap_or(300)),
            ..s.clone()
        })
    }

    /// Ends session `sid`; where to send the browser to sign out at the
    /// identity provider too, if it says.
    pub async fn end(&self, sid: &str) -> Option<String> {
        self.sessions.lock().expect("sessions lock").remove(sid);
        let e = self.endpoints().await.ok()?;
        e.end_session_endpoint.clone()
    }
}

/// Percent-encoding for a query parameter.
pub fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The session id in a request's cookies.
pub fn session_id(headers: &axum::http::HeaderMap) -> Option<String> {
    headers.get_all(axum::http::header::COOKIE).iter().filter_map(|v| v.to_str().ok()).find_map(|c| {
        c.split(';')
            .map(str::trim)
            .find_map(|kv| kv.strip_prefix(COOKIE).and_then(|v| v.strip_prefix('=')).map(String::from))
    })
}

impl FromRequestParts<AppState> for Identity {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        match &*state.auth {
            AuthConfig::Dev { user, password } => Ok(Identity {
                principal: user.clone(),
                display_name: user.clone(),
                login: password.clone().map_or(Login::None, |p| Login::Password(Secret(p))),
            }),
            AuthConfig::Oidc(_) => {
                let sessions = state.sessions.as_ref().ok_or(ApiError::Unauthorized)?;
                let sid = session_id(&parts.headers).ok_or(ApiError::Unauthorized)?;
                sessions.identity(&sid).await.ok_or(ApiError::Unauthorized)
            }
        }
    }
}

/// Query of `/auth/login`.
#[derive(Debug, Deserialize)]
pub struct LoginParams {
    /// Path on this console to return to once signed in.
    #[serde(default)]
    pub return_to: Option<String>,
}

/// Query of `/auth/callback`.
#[derive(Debug, Deserialize)]
pub struct CallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

fn see_other(to: &str) -> Response {
    let mut r = StatusCode::SEE_OTHER.into_response();
    if let Ok(v) = HeaderValue::from_str(to) {
        r.headers_mut().insert(header::LOCATION, v);
    }
    r
}

fn cookie(state: &AppState, value: &str, max_age: Option<u64>) -> Option<HeaderValue> {
    // Secure whenever the console is reached over HTTPS, as its callback says.
    let secure = matches!(&*state.auth, AuthConfig::Oidc(o) if o.redirect_url.starts_with("https://"));
    let mut c = format!("{COOKIE}={value}; Path=/; HttpOnly; SameSite=Lax");
    if secure {
        c.push_str("; Secure");
    }
    if let Some(age) = max_age {
        c.push_str(&format!("; Max-Age={age}"));
    }
    HeaderValue::from_str(&c).ok()
}

fn sign_in_failed(e: &SignInError) -> Response {
    tracing::warn!(error = %e, "sign-in failed");
    let status = match e {
        SignInError::Provider(_) => StatusCode::BAD_GATEWAY,
        SignInError::Unknown | SignInError::Refused(_) => StatusCode::FORBIDDEN,
    };
    let page = format!(
        "<!doctype html><meta charset=utf-8><title>Sign-in failed</title>\
         <p>{}</p><p><a href=\"/auth/login\">Sign in again</a></p>",
        html_escape(&e.to_string())
    );
    (status, [(header::CONTENT_TYPE, "text/html; charset=utf-8")], page).into_response()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// `GET /auth/login`: sends the browser to the identity provider (in `dev`
/// mode, straight back).
pub async fn login(State(state): State<AppState>, Query(q): Query<LoginParams>) -> Response {
    let return_to = q.return_to.as_deref().unwrap_or("/");
    match (&*state.auth, &state.sessions) {
        (AuthConfig::Oidc(_), Some(sessions)) => match sessions.start(return_to).await {
            Ok(url) => see_other(&url),
            Err(e) => sign_in_failed(&e),
        },
        _ => see_other("/"),
    }
}

/// `GET /auth/callback`: the identity provider sends the browser back here.
pub async fn callback(State(state): State<AppState>, Query(q): Query<CallbackParams>) -> Response {
    let Some(sessions) = &state.sessions else { return see_other("/") };
    if let Some(e) = q.error {
        let why = q.error_description.map_or(e.clone(), |d| format!("{e}: {d}"));
        return sign_in_failed(&SignInError::Refused(why));
    }
    let (Some(code), Some(st)) = (q.code, q.state) else { return sign_in_failed(&SignInError::Unknown) };
    match sessions.finish(&code, &st).await {
        Ok((sid, to)) => {
            let mut r = see_other(&to);
            if let Some(c) = cookie(&state, &sid, None) {
                r.headers_mut().insert(header::SET_COOKIE, c);
            }
            r
        }
        Err(e) => sign_in_failed(&e),
    }
}

/// `POST /auth/logout`: ends the session, and at the identity provider
/// too if it offers that.
pub async fn logout(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    let mut to = "/".to_owned();
    if let (Some(sessions), Some(sid)) = (&state.sessions, session_id(&headers)) {
        if let (Some(end), AuthConfig::Oidc(o)) = (sessions.end(&sid).await, &*state.auth) {
            let back = o.redirect_url.trim_end_matches("/auth/callback");
            to = format!(
                "{end}{}client_id={}&post_logout_redirect_uri={}",
                if end.contains('?') { '&' } else { '?' },
                url_encode(&o.client_id),
                url_encode(if back.is_empty() { "/" } else { back }),
            );
        }
    }
    let mut r = see_other(&to);
    if let Some(c) = cookie(&state, "", Some(0)) {
        r.headers_mut().insert(header::SET_COOKIE, c);
    }
    r
}

//! Who is calling.
//!
//! The console never acts with its own authority: every call to a cluster
//! carries the signed-in user's [`Identity`], and the brokers' roles decide
//! (ADR 0019).
//!
//! - `oidc`: the user signs in at the organisation's identity provider
//!   (Authorization Code with PKCE). The session lives in an HttpOnly
//!   cookie, sealed with a key only the console has, so it survives a
//!   restart (ADR 0041); the console passes the user's access token to the
//!   clusters with SASL OAUTHBEARER and renews it with the refresh token.
//! - `dev`: everyone is one fixed user, signed in to the clusters with that
//!   user's SCRAM password if one is configured. For a machine only you can
//!   reach.

mod provider;
mod sealed;

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
use lotse_rt::Rng as _;
use lotse_rt_tokio::TokioRuntime;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::AppState;
use crate::error::ApiError;
use crate::time::now_ms;
use provider::Provider;
pub use sealed::SessionKey;
use sealed::{Sealed, SigningIn};

/// The session cookie's name.
pub const COOKIE: &str = "lotse_session";

/// The cookie that carries a sign-in in progress; sent only to `/auth`.
pub const SIGN_IN_COOKIE: &str = "lotse_sign_in";

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
    /// The key sessions are sealed with: 32 random bytes in base64. If the
    /// file does not exist, the console makes it. Default: `session.key`
    /// in the data directory.
    #[serde(default)]
    pub session_key_file: Option<std::path::PathBuf>,
    /// A session ends after this many hours at the latest.
    #[serde(default = "default_session_hours")]
    pub session_hours: u64,
}

fn default_session_hours() -> u64 {
    12
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

/// A session as the console holds it while in use: the access token and
/// the newest cookie.
#[derive(Debug, Clone)]
struct Live {
    sid: String,
    principal: String,
    display_name: String,
    access_token: Secret,
    expires: Instant,
    cookie: String,
}

impl Live {
    fn identity(&self) -> Identity {
        Identity {
            principal: self.principal.clone(),
            display_name: self.display_name.clone(),
            login: Login::Token { token: self.access_token.clone(), expires: self.expires },
        }
    }
}

/// A session found in a request's cookie.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// Who it is.
    pub identity: Identity,
    /// A new cookie value, when the provider rotated the refresh token.
    pub reissue: Option<String>,
}

/// Sign-ins and signed-in browsers, for `oidc` mode. The sessions
/// themselves live in the browsers' cookies, sealed; this only caches
/// access tokens.
#[derive(Debug)]
pub struct Sessions {
    cfg: OidcLogin,
    provider: Provider,
    key: SessionKey,
    /// Sign-ins finished here, by their state, until they would have
    /// expired: a callback is used once.
    finished: Mutex<BTreeMap<String, u64>>,
    /// By the hash of a cookie value; a rotated cookie and its successor
    /// share one entry.
    live: Mutex<BTreeMap<[u8; 32], Live>>,
    /// Sessions signed out here, until they would have ended anyway.
    ended: Mutex<BTreeMap<String, u64>>,
    /// One renewal at a time, so a rotating refresh token is used once.
    renewing: tokio::sync::Mutex<()>,
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

fn hash(cookie: &str) -> [u8; 32] {
    Sha256::digest(cookie.as_bytes()).into()
}

/// `ms` since the epoch, as an instant of this process.
fn instant_at(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms.saturating_sub(now_ms()))
}

impl Sessions {
    /// Sessions for `cfg`, sealed with `key`, reaching the identity
    /// provider through `rt`.
    pub fn new(cfg: OidcLogin, rt: TokioRuntime, key: SessionKey) -> Sessions {
        Sessions {
            provider: Provider::new(&cfg.issuer, rt),
            cfg,
            key,
            finished: Mutex::new(BTreeMap::new()),
            live: Mutex::new(BTreeMap::new()),
            ended: Mutex::new(BTreeMap::new()),
            renewing: tokio::sync::Mutex::new(()),
        }
    }

    fn rt(&self) -> &TokioRuntime {
        &self.provider.rt
    }

    /// How long a session lasts at most.
    pub fn lifetime(&self) -> Duration {
        Duration::from_secs(self.cfg.session_hours.max(1) * 3600)
    }

    fn client(&self) -> Vec<(&str, &str)> {
        let mut c = vec![("client_id", self.cfg.client_id.as_str())];
        if let Some(s) = &self.cfg.client_secret {
            c.push(("client_secret", s.as_str()));
        }
        c
    }

    /// Starts a sign-in: where to send the browser, and the cookie that
    /// carries the sign-in until the browser comes back.
    pub async fn start(&self, return_to: &str) -> Result<(String, String), SignInError> {
        let e = self.provider.endpoints().await?;
        let authorize = e
            .authorization_endpoint
            .as_deref()
            .ok_or_else(|| SignInError::Provider("the discovery document names no authorization endpoint".into()))?;
        let (state, verifier, nonce) = (random(self.rt(), 24), random(self.rt(), 48), random(self.rt(), 24));
        let challenge = B64URL.encode(Sha256::digest(verifier.as_bytes()));
        // Only paths on this console: never send a user elsewhere.
        let return_to = if return_to.starts_with('/') && !return_to.starts_with("//") { return_to } else { "/" };
        let signing_in = SigningIn {
            state: state.clone(),
            verifier,
            nonce: nonce.clone(),
            return_to: return_to.into(),
            started_ms: now_ms(),
        };
        let cookie = self.key.seal_sign_in(self.rt(), &signing_in);
        let q = |s: &str| url_encode(s);
        let url = format!(
            "{}{}response_type=code&client_id={}&redirect_uri={}&scope={}&state={}&nonce={}&code_challenge={}&code_challenge_method=S256",
            authorize,
            if authorize.contains('?') { '&' } else { '?' },
            q(&self.cfg.client_id),
            q(&self.cfg.redirect_url),
            q(&self.cfg.scopes.join(" ")),
            q(&state),
            q(&nonce),
            q(&challenge),
        );
        Ok((url, cookie))
    }

    /// Finishes the sign-in in `signing_in` (its cookie): the new session's
    /// cookie and where to go.
    pub async fn finish(&self, code: &str, state: &str, signing_in: &str) -> Result<(String, String), SignInError> {
        let pending = self.key.open_sign_in(signing_in).ok_or(SignInError::Unknown)?;
        let until = pending.started_ms + LOGIN_TIMEOUT.as_millis() as u64;
        if pending.state != state || until <= now_ms() {
            return Err(SignInError::Unknown);
        }
        {
            // Once per sign-in, also if its cookie was copied.
            let mut finished = self.finished.lock().expect("finished lock");
            let now = now_ms();
            finished.retain(|_, u| *u > now);
            if finished.insert(pending.state.clone(), until).is_some() {
                return Err(SignInError::Unknown);
            }
        }
        let issuer = self.provider.endpoints().await?.issuer.clone();
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", self.cfg.redirect_url.as_str()),
            ("code_verifier", pending.verifier.as_str()),
        ];
        form.extend(self.client());
        let tokens = self.provider.token(&form).await?;
        let id =
            tokens.id_token.as_deref().and_then(claims).ok_or_else(|| SignInError::Refused("no ID token".into()))?;
        let claim = |k: &str| id.get(k).and_then(|v| v.as_str());
        if claim("iss") != Some(issuer.as_str()) {
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
        let lifetime = tokens.lifetime();
        let sealed = Sealed {
            sid: random(self.rt(), 18),
            principal,
            display_name,
            // Without a refresh token the session keeps the access token
            // and ends with it.
            access_token: tokens.refresh_token.is_none().then(|| tokens.access_token.clone()),
            access_expires_ms: tokens.refresh_token.is_none().then(|| now_ms() + lifetime.as_millis() as u64),
            refresh_token: tokens.refresh_token,
            expires_ms: now_ms() + self.lifetime().as_millis() as u64,
        };
        let cookie = self.key.seal(self.rt(), &sealed);
        if cookie.len() > 3800 {
            tracing::warn!(
                bytes = cookie.len(),
                "the session cookie is near the 4 KiB browsers keep; tokens this long may be dropped"
            );
        }
        let live = Live {
            sid: sealed.sid,
            principal: sealed.principal,
            display_name: sealed.display_name,
            access_token: Secret(tokens.access_token),
            expires: Instant::now() + lifetime,
            cookie: cookie.clone(),
        };
        self.remember(&[&cookie], live);
        Ok((cookie, pending.return_to))
    }

    fn remember(&self, cookies: &[&str], live: Live) {
        let mut all = self.live.lock().expect("live lock");
        let now = Instant::now();
        all.retain(|_, l| l.expires > now);
        for c in cookies {
            all.insert(hash(c), live.clone());
        }
    }

    fn cached(&self, cookie: &str) -> Option<Live> {
        self.live.lock().expect("live lock").get(&hash(cookie)).cloned()
    }

    /// The session in `cookie`, renewing its access token when it is about
    /// to expire; `None` if the user must sign in again.
    pub async fn resolve(&self, cookie: &str) -> Option<Resolved> {
        let sealed = self.key.open(cookie)?;
        if sealed.expires_ms <= now_ms() || self.ended.lock().expect("ended lock").contains_key(&sealed.sid) {
            return None;
        }
        let fresh = |l: &Live| l.expires.saturating_duration_since(Instant::now()) > RENEW_BEFORE;
        let answer = |l: Live| {
            let reissue = (l.cookie != cookie).then(|| l.cookie.clone());
            Some(Resolved { identity: l.identity(), reissue })
        };
        if let Some(l) = self.cached(cookie).filter(fresh) {
            return answer(l);
        }
        let _turn = self.renewing.lock().await;
        let known = self.cached(cookie);
        if let Some(l) = known.clone().filter(fresh) {
            return answer(l);
        }
        // The newest state of this session: a rotated cookie supersedes
        // the one the browser sent.
        let newest = known.as_ref().map_or(cookie, |l| l.cookie.as_str());
        let current = self.key.open(newest)?;
        let live = match &current.refresh_token {
            Some(refresh) => {
                let mut form = vec![("grant_type", "refresh_token"), ("refresh_token", refresh.as_str())];
                form.extend(self.client());
                let t = self.provider.token(&form).await.ok()?;
                let lifetime = t.lifetime();
                let rotated = t.refresh_token.filter(|r| r != refresh);
                let next = match rotated {
                    Some(r) => self.key.seal(self.rt(), &Sealed { refresh_token: Some(r), ..current.clone() }),
                    None => newest.to_owned(),
                };
                Live {
                    sid: current.sid.clone(),
                    principal: current.principal.clone(),
                    display_name: current.display_name.clone(),
                    access_token: Secret(t.access_token),
                    expires: Instant::now() + lifetime,
                    cookie: next,
                }
            }
            None => {
                let (token, until) = (current.access_token.clone()?, current.access_expires_ms?);
                if until <= now_ms() {
                    return None;
                }
                Live {
                    sid: current.sid.clone(),
                    principal: current.principal.clone(),
                    display_name: current.display_name.clone(),
                    access_token: Secret(token),
                    expires: instant_at(until),
                    cookie: newest.to_owned(),
                }
            }
        };
        self.remember(&[cookie, newest, &live.cookie.clone()], live.clone());
        answer(live)
    }

    /// Ends the session in `cookie`: here, and its refresh token at the
    /// identity provider. Returns where to send the browser to sign out
    /// there too, if the provider says.
    pub async fn end(&self, cookie: &str) -> Option<String> {
        let sealed = self.key.open(cookie)?;
        let newest = self.cached(cookie).and_then(|l| self.key.open(&l.cookie)).unwrap_or(sealed.clone());
        {
            let mut ended = self.ended.lock().expect("ended lock");
            let now = now_ms();
            ended.retain(|_, until| *until > now);
            ended.insert(sealed.sid.clone(), sealed.expires_ms);
        }
        self.live.lock().expect("live lock").retain(|_, l| l.sid != sealed.sid);
        if let Some(r) = &newest.refresh_token {
            self.provider.revoke(r, &self.client()).await;
        }
        self.provider.endpoints().await.ok()?.end_session_endpoint.clone()
    }
}

/// How a service signs in: the OAuth client credentials grant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceCredentials {
    /// The issuer the brokers trust.
    pub issuer: String,
    /// The service's client id.
    pub client_id: String,
    /// Its secret.
    pub client_secret: Secret,
    /// Scopes to ask for, if the issuer wants any.
    pub scope: Option<String>,
}

/// A service signed in with client credentials, its token renewed before
/// it expires: the metrics sampler.
#[derive(Debug)]
pub struct ServiceLogin {
    principal: String,
    creds: ServiceCredentials,
    provider: Provider,
    token: tokio::sync::Mutex<Option<(Secret, Instant)>>,
}

impl ServiceLogin {
    /// `principal` signing in with `creds` through `rt`.
    pub fn new(principal: &str, creds: ServiceCredentials, rt: TokioRuntime) -> ServiceLogin {
        ServiceLogin {
            principal: principal.into(),
            provider: Provider::new(&creds.issuer, rt),
            creds,
            token: tokio::sync::Mutex::new(None),
        }
    }

    /// The service's identity with a token valid for a while yet.
    pub async fn identity(&self) -> Result<Identity, SignInError> {
        let mut held = self.token.lock().await;
        let fresh = |(_, e): &(Secret, Instant)| e.saturating_duration_since(Instant::now()) > RENEW_BEFORE;
        if !held.as_ref().is_some_and(fresh) {
            let mut form = vec![
                ("grant_type", "client_credentials"),
                ("client_id", self.creds.client_id.as_str()),
                ("client_secret", self.creds.client_secret.expose()),
            ];
            if let Some(s) = &self.creds.scope {
                form.push(("scope", s.as_str()));
            }
            let t = self.provider.token(&form).await?;
            if let Some(sub) =
                claims(&t.access_token).and_then(|c| c.get("sub").and_then(|s| s.as_str()).map(String::from))
            {
                if sub != self.principal {
                    tracing::warn!(%sub, principal = %self.principal, "the sampler's token names another subject than metrics.principal");
                }
            }
            *held = Some((Secret(t.access_token.clone()), Instant::now() + t.lifetime()));
        }
        let (token, expires) = held.clone().expect("a token");
        Ok(Identity {
            principal: self.principal.clone(),
            display_name: "metrics sampler".into(),
            login: Login::Token { token, expires },
        })
    }
}

/// How the metrics sampler signs in.
#[derive(Debug, Clone)]
pub enum SamplerLogin {
    /// With nothing, or a password.
    Fixed(Identity),
    /// With client credentials.
    Service(std::sync::Arc<ServiceLogin>),
}

impl SamplerLogin {
    /// The identity to sample with now.
    pub async fn identity(&self) -> Result<Identity, SignInError> {
        match self {
            SamplerLogin::Fixed(i) => Ok(i.clone()),
            SamplerLogin::Service(s) => s.identity().await,
        }
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

/// The session cookie's value in a request's headers.
pub fn session_cookie(headers: &axum::http::HeaderMap) -> Option<String> {
    cookie_value(headers, COOKIE)
}

fn cookie_value(headers: &axum::http::HeaderMap, name: &str) -> Option<String> {
    headers.get_all(axum::http::header::COOKIE).iter().filter_map(|v| v.to_str().ok()).find_map(|c| {
        c.split(';')
            .map(str::trim)
            .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')).map(String::from))
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
            // Found by [`session_layer`] before the handler runs.
            AuthConfig::Oidc(_) => parts.extensions.get::<Identity>().cloned().ok_or(ApiError::Unauthorized),
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
    set_cookie(state, COOKIE, "/", value, max_age)
}

fn set_cookie(state: &AppState, name: &str, path: &str, value: &str, max_age: Option<u64>) -> Option<HeaderValue> {
    // Secure whenever the console is reached over HTTPS, as its callback says.
    let secure = matches!(&*state.auth, AuthConfig::Oidc(o) if o.redirect_url.starts_with("https://"));
    let mut c = format!("{name}={value}; Path={path}; HttpOnly; SameSite=Lax");
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
            Ok((url, signing_in)) => {
                let mut r = see_other(&url);
                if let Some(c) = set_cookie(&state, SIGN_IN_COOKIE, "/auth", &signing_in, Some(LOGIN_TIMEOUT.as_secs()))
                {
                    r.headers_mut().insert(header::SET_COOKIE, c);
                }
                r
            }
            Err(e) => sign_in_failed(&e),
        },
        _ => see_other("/"),
    }
}

/// `GET /auth/callback`: the identity provider sends the browser back here.
pub async fn callback(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(q): Query<CallbackParams>,
) -> Response {
    let Some(sessions) = &state.sessions else { return see_other("/") };
    let mut r = match (q.error, q.code, q.state, cookie_value(&headers, SIGN_IN_COOKIE)) {
        (Some(e), ..) => {
            let why = q.error_description.map_or(e.clone(), |d| format!("{e}: {d}"));
            sign_in_failed(&SignInError::Refused(why))
        }
        (None, Some(code), Some(st), Some(signing_in)) => match sessions.finish(&code, &st, &signing_in).await {
            Ok((value, to)) => {
                let mut r = see_other(&to);
                if let Some(c) = cookie(&state, &value, Some(sessions.lifetime().as_secs())) {
                    r.headers_mut().append(header::SET_COOKIE, c);
                }
                r
            }
            Err(e) => sign_in_failed(&e),
        },
        _ => sign_in_failed(&SignInError::Unknown),
    };
    // The sign-in is over, one way or the other.
    if let Some(c) = set_cookie(&state, SIGN_IN_COOKIE, "/auth", "", Some(0)) {
        r.headers_mut().append(header::SET_COOKIE, c);
    }
    r
}

/// `POST /auth/logout`: ends the session, and at the identity provider
/// too if it offers that.
pub async fn logout(State(state): State<AppState>, headers: axum::http::HeaderMap) -> Response {
    let mut to = "/".to_owned();
    if let (Some(sessions), Some(value)) = (&state.sessions, session_cookie(&headers)) {
        if let (Some(end), AuthConfig::Oidc(o)) = (sessions.end(&value).await, &*state.auth) {
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

/// Finds the session in the cookie for the handlers behind it, and passes
/// a rotated cookie back to the browser; a cookie that no longer holds a
/// session is cleared.
pub async fn session_layer(
    State(state): State<AppState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (Some(sessions), Some(value)) = (&state.sessions, session_cookie(req.headers())) else {
        return next.run(req).await;
    };
    let resolved = sessions.resolve(&value).await;
    let set = match &resolved {
        Some(r) => r.reissue.as_deref().and_then(|v| cookie(&state, v, Some(sessions.lifetime().as_secs()))),
        None => cookie(&state, "", Some(0)),
    };
    if let Some(r) = resolved {
        req.extensions_mut().insert(r.identity);
    }
    let mut res = next.run(req).await;
    if let Some(c) = set {
        res.headers_mut().append(header::SET_COOKIE, c);
    }
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    fn login() -> OidcLogin {
        OidcLogin {
            issuer: "http://127.0.0.1:9/".into(),
            client_id: "c".into(),
            client_secret: None,
            redirect_url: "http://127.0.0.1:8080/auth/callback".into(),
            scopes: default_scopes(),
            principal_claim: default_principal_claim(),
            ca_file: None,
            session_key_file: None,
            session_hours: 12,
        }
    }

    #[tokio::test]
    async fn a_session_past_its_end_is_over_whatever_it_holds() {
        let rt = TokioRuntime::new(std::env::temp_dir());
        let key = SessionKey::from_bytes(&[3; 32]);
        let sealed = |expires_ms| Sealed {
            sid: "s".into(),
            principal: "bob".into(),
            display_name: "Bob".into(),
            refresh_token: None,
            access_token: Some("token".into()),
            access_expires_ms: Some(now_ms() + 3_600_000),
            expires_ms,
        };
        let sessions = Sessions::new(login(), rt.clone(), SessionKey::from_bytes(&[3; 32]));
        let alive = key.seal(&rt, &sealed(now_ms() + 60_000));
        let r = sessions.resolve(&alive).await.expect("an access token valid for an hour");
        assert_eq!(r.identity.principal, "bob");
        let over = key.seal(&rt, &sealed(now_ms() - 1));
        assert!(sessions.resolve(&over).await.is_none(), "the session's end comes first");
    }

    #[tokio::test]
    async fn a_sign_in_is_finished_within_ten_minutes_or_not_at_all() {
        let rt = TokioRuntime::new(std::env::temp_dir());
        let key = SessionKey::from_bytes(&[4; 32]);
        let sessions = Sessions::new(login(), rt.clone(), SessionKey::from_bytes(&[4; 32]));
        let started = now_ms() - LOGIN_TIMEOUT.as_millis() as u64 - 1;
        let late = SigningIn {
            state: "st".into(),
            verifier: "v".into(),
            nonce: "n".into(),
            return_to: "/".into(),
            started_ms: started,
        };
        let cookie = key.seal_sign_in(&rt, &late);
        // Refused before the provider is asked (which here is not there).
        assert!(matches!(sessions.finish("code", "st", &cookie).await, Err(SignInError::Unknown)));
    }
}

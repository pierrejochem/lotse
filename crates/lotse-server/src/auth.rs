//! Who a client is: the node's authentication settings, the principals a
//! connection can be, and what the connection task needs to check SCRAM
//! credentials and OpenID Connect tokens.
//!
//! Authentication happens in the connection's own task, before anything
//! reaches the main loop: SCRAM looks up credentials the main loop
//! publishes from the applied metadata state, and tokens are checked
//! against the issuer's keys, fetched through the runtime and cached.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_auth::oauth::{self, Jwks, OidcSettings, TokenError, Verified};
use lotse_auth::scram::ScramCredential;
use lotse_raft::NodeId;
use lotse_rt::{Clock, Http, Instant};

/// How long a session lasts at most before the client must authenticate
/// again, by default.
pub const DEFAULT_MAX_SESSION: Duration = Duration::from_secs(3600);

/// The issuer's keys are fetched again for an unknown key id at most this
/// often, so tokens with made-up key ids cannot make the node hammer the
/// identity provider.
pub const JWKS_REFETCH_EVERY: Duration = Duration::from_secs(30);

/// A node's authentication settings.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Clients must authenticate before anything but `ApiVersions` and the
    /// SASL requests. Needs a network that verifies peers (TLS): nodes
    /// authenticate to each other by their certificates.
    pub required: bool,
    /// A client certificate names its principal.
    pub mtls: bool,
    /// SASL SCRAM-SHA-512 with credentials from the metadata group and
    /// [`AuthConfig::users`].
    pub scram: bool,
    /// SASL OAUTHBEARER with tokens from this issuer.
    pub oidc: Option<OidcConfig>,
    /// Sessions end after this at the latest (and a token's at its expiry):
    /// a removed user or revoked token is cut off by then.
    pub max_session: Duration,
    /// SCRAM users from the node configuration; they cannot be changed at
    /// runtime and win over runtime users of the same name.
    pub users: BTreeMap<String, ScramCredential>,
    /// Principals that are cluster admins from the start (by certificate,
    /// SCRAM user or token): who creates the first tenants and bindings.
    pub admins: Vec<String>,
    /// Tenants from the configuration, besides `default`; they cannot be
    /// deleted at runtime.
    pub tenants: Vec<String>,
    /// The tenant of each configured resource, by name; those not named
    /// here belong to `default`.
    pub resource_tenants: BTreeMap<String, String>,
}

impl AuthConfig {
    /// No authentication: every client is anonymous and may do everything.
    /// For tests and single-machine development only.
    pub fn none() -> AuthConfig {
        AuthConfig {
            required: false,
            mtls: false,
            scram: false,
            oidc: None,
            max_session: DEFAULT_MAX_SESSION,
            users: BTreeMap::new(),
            admins: Vec::new(),
            tenants: Vec::new(),
            resource_tenants: BTreeMap::new(),
        }
    }

    /// The SASL mechanisms enabled, in the order offered.
    pub fn mechanisms(&self) -> Vec<String> {
        let mut m = Vec::new();
        if self.scram {
            m.push(lotse_auth::scram::MECHANISM.to_owned());
        }
        if self.oidc.is_some() {
            m.push(oauth::MECHANISM.to_owned());
        }
        m
    }
}

/// Where tokens come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcConfig {
    /// The issuer, as in the tokens' `iss`; its discovery document names
    /// the key set.
    pub issuer: String,
    /// The audience tokens must be for.
    pub audience: String,
    /// The claim naming the principal.
    pub principal_claim: String,
    /// The claim listing the user's groups.
    pub groups_claim: String,
    /// The key set's URL, if not taken from the discovery document.
    pub jwks_url: Option<String>,
}

impl OidcConfig {
    /// Tokens from `issuer` for `audience`, principal in `sub`, groups in
    /// `groups`.
    pub fn new(issuer: &str, audience: &str) -> OidcConfig {
        OidcConfig {
            issuer: issuer.into(),
            audience: audience.into(),
            principal_claim: "sub".into(),
            groups_claim: "groups".into(),
            jwks_url: None,
        }
    }

    fn settings(&self) -> OidcSettings {
        OidcSettings {
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            principal_claim: self.principal_claim.clone(),
            groups_claim: self.groups_claim.clone(),
        }
    }
}

/// Who a connection acts as.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Principal {
    /// Nobody in particular: authentication is not required and the client
    /// did not authenticate.
    Anonymous,
    /// A broker node, by its certificate.
    Node(NodeId),
    /// A user or service, with the groups its identity provider lists.
    User {
        /// The name.
        name: String,
        /// Groups at the identity provider (OAUTHBEARER only).
        groups: Vec<String>,
    },
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Principal::Anonymous => write!(f, "anonymous"),
            Principal::Node(id) => write!(f, "node {id}"),
            Principal::User { name, .. } => write!(f, "{name}"),
        }
    }
}

/// What every connection task of a node shares.
pub(crate) struct Auth<R> {
    pub(crate) cfg: AuthConfig,
    /// Runtime SCRAM users, as the metadata group last applied them.
    users: Mutex<BTreeMap<String, ScramCredential>>,
    oidc: Option<Oidc<R>>,
}

impl<R: Clock + Http + Clone> Auth<R> {
    pub(crate) fn new(rt: R, cfg: AuthConfig) -> Arc<Auth<R>> {
        let oidc = cfg.oidc.clone().map(|c| Oidc { rt, cfg: c, keys: Mutex::new(Keys::default()) });
        Arc::new(Auth { cfg, users: Mutex::new(BTreeMap::new()), oidc })
    }

    /// Publishes the runtime users the metadata group applied.
    pub(crate) fn set_users(&self, users: &BTreeMap<String, ScramCredential>) {
        let mut u = self.users.lock().expect("users lock");
        if *u != *users {
            u.clone_from(users);
        }
    }

    /// The credential of `user`: configured first, then runtime.
    pub(crate) fn credential(&self, user: &str) -> Option<ScramCredential> {
        self.cfg.users.get(user).cloned().or_else(|| self.users.lock().expect("users lock").get(user).cloned())
    }

    /// Checks a bearer token.
    pub(crate) async fn verify_token(&self, token: &str) -> Result<Verified, TokenError> {
        match &self.oidc {
            Some(o) => o.verify(token).await,
            None => Err(TokenError::Invalid("OAUTHBEARER is not enabled".into())),
        }
    }
}

#[derive(Default)]
struct Keys {
    jwks: Jwks,
    fetched: Option<Instant>,
}

/// Tokens of one issuer, with its keys cached.
struct Oidc<R> {
    rt: R,
    cfg: OidcConfig,
    keys: Mutex<Keys>,
}

impl<R: Clock + Http> Oidc<R> {
    async fn verify(&self, token: &str) -> Result<Verified, TokenError> {
        let kid = oauth::key_id(token)?;
        let refetch = {
            let k = self.keys.lock().expect("keys lock");
            !k.jwks.has(&kid) && k.fetched.is_none_or(|t| self.rt.now() - t >= JWKS_REFETCH_EVERY)
        };
        if refetch {
            // Another connection may fetch at the same time; both store the
            // same keys.
            self.keys.lock().expect("keys lock").fetched = Some(self.rt.now());
            let jwks = self.fetch().await?;
            self.keys.lock().expect("keys lock").jwks = jwks;
        }
        let jwks = self.keys.lock().expect("keys lock").jwks.clone();
        oauth::validate(token, &jwks, &self.cfg.settings(), self.rt.unix_time().as_secs())
    }

    async fn fetch(&self) -> Result<Jwks, TokenError> {
        let get = |url: String| async move {
            self.rt.get(&url).await.map_err(|e| TokenError::Metadata(format!("{url}: {e}")))
        };
        let url = match &self.cfg.jwks_url {
            Some(u) => u.clone(),
            None => oauth::jwks_uri(&get(oauth::discovery_url(&self.cfg.issuer)).await?, &self.cfg.issuer)?,
        };
        Jwks::parse(&get(url).await?)
    }
}

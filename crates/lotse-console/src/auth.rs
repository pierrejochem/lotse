//! Who is calling.
//!
//! The console never acts with its own authority: every call to a cluster
//! carries the signed-in user's [`Identity`], and the broker's RBAC decides.
//! In production the identity comes from an OIDC login and its token is
//! passed to the cluster with SASL OAUTHBEARER. That flow is not built yet;
//! `dev` mode signs every request in as a fixed local user.

use std::fmt;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use serde::Deserialize;

use crate::AppState;
use crate::error::ApiError;

/// How users sign in.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Local development: everyone is `user`. Never expose this.
    Dev {
        /// Principal used for every request.
        user: String,
    },
    /// OpenID Connect login (not implemented yet).
    Oidc {
        /// Issuer URL.
        issuer: String,
        /// Client ID registered at the issuer.
        client_id: String,
    },
}

impl AuthConfig {
    /// `dev` or `oidc`.
    pub fn mode(&self) -> &'static str {
        match self {
            AuthConfig::Dev { .. } => "dev",
            AuthConfig::Oidc { .. } => "oidc",
        }
    }
}

/// A token passed through to the cluster. Never logged or serialised.
#[derive(Clone)]
pub struct BearerToken(String);

impl BearerToken {
    /// The raw token, for the SASL OAUTHBEARER handshake only.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken(…)")
    }
}

/// The user a request acts for.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Principal name the cluster authorises.
    pub principal: String,
    /// Name to show in the UI.
    pub display_name: String,
    /// Token for the cluster; `None` in dev mode.
    pub token: Option<BearerToken>,
}

impl FromRequestParts<AppState> for Identity {
    type Rejection = ApiError;

    async fn from_request_parts(_parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        match &*state.auth {
            AuthConfig::Dev { user } => {
                Ok(Identity { principal: user.clone(), display_name: user.clone(), token: None })
            }
            AuthConfig::Oidc { .. } => Err(ApiError::Unavailable("OIDC login is not implemented yet".into())),
        }
    }
}

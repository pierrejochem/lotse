//! The identity provider, as the console talks to it: its discovery
//! document and its token and revocation endpoints.

use std::time::Duration;

use lotse_rt::Http as _;
use lotse_rt_tokio::TokioRuntime;
use serde::Deserialize;

use super::SignInError;

/// The endpoints the discovery document names.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Endpoints {
    pub(crate) issuer: String,
    #[serde(default)]
    pub(crate) authorization_endpoint: Option<String>,
    pub(crate) token_endpoint: String,
    #[serde(default)]
    pub(crate) end_session_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) revocation_endpoint: Option<String>,
}

/// What a token endpoint answers.
#[derive(Debug, Deserialize)]
pub(crate) struct Tokens {
    pub(crate) access_token: String,
    #[serde(default)]
    pub(crate) id_token: Option<String>,
    #[serde(default)]
    pub(crate) refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

impl Tokens {
    /// How long the access token lasts; five minutes if the provider does
    /// not say.
    pub(crate) fn lifetime(&self) -> Duration {
        Duration::from_secs(self.expires_in.unwrap_or(300))
    }
}

/// One issuer.
#[derive(Debug)]
pub(crate) struct Provider {
    issuer: String,
    pub(crate) rt: TokioRuntime,
    endpoints: tokio::sync::OnceCell<Endpoints>,
}

impl Provider {
    pub(crate) fn new(issuer: &str, rt: TokioRuntime) -> Provider {
        Provider { issuer: issuer.into(), rt, endpoints: tokio::sync::OnceCell::new() }
    }

    /// The endpoints, fetched once.
    pub(crate) async fn endpoints(&self) -> Result<&Endpoints, SignInError> {
        self.endpoints
            .get_or_try_init(|| async {
                let url = format!("{}/.well-known/openid-configuration", self.issuer.trim_end_matches('/'));
                let body = self.rt.get(&url).await.map_err(|e| SignInError::Provider(format!("{url}: {e}")))?;
                let e: Endpoints =
                    serde_json::from_slice(&body).map_err(|e| SignInError::Provider(format!("{url}: {e}")))?;
                if e.issuer != self.issuer {
                    return Err(SignInError::Provider(format!("{url} names another issuer, {}", e.issuer)));
                }
                Ok(e)
            })
            .await
    }

    /// Posts `form` to the token endpoint.
    pub(crate) async fn token(&self, form: &[(&str, &str)]) -> Result<Tokens, SignInError> {
        let url = &self.endpoints().await?.token_endpoint;
        let body = self.rt.post_form(url, form).await.map_err(|e| SignInError::Refused(format!("{url}: {e}")))?;
        serde_json::from_slice(&body).map_err(|e| SignInError::Provider(format!("{url}: {e}")))
    }

    /// Revokes a refresh token (RFC 7009), if the provider offers that.
    /// Best effort: a failure is logged, and the token still dies when it
    /// expires.
    pub(crate) async fn revoke(&self, refresh_token: &str, client: &[(&str, &str)]) {
        let Ok(e) = self.endpoints().await else { return };
        let Some(url) = &e.revocation_endpoint else { return };
        let mut form = vec![("token", refresh_token), ("token_type_hint", "refresh_token")];
        form.extend_from_slice(client);
        if let Err(err) = self.rt.post_form(url, &form).await {
            tracing::warn!(%url, error = %err, "revoking a refresh token failed");
        }
    }
}

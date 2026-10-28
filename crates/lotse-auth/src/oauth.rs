//! OAUTHBEARER (RFC 7628) with OpenID Connect access tokens, without I/O.
//!
//! The broker trusts one issuer. It fetches the issuer's discovery document
//! and key set itself (through the runtime), hands them to [`Jwks::parse`],
//! and checks each token with [`validate`] against the time it passes in, so
//! the simulation decides what "now" is.

use std::collections::BTreeMap;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};

/// The mechanism's name in `SaslHandshake`.
pub const MECHANISM: &str = "OAUTHBEARER";

/// How far clocks may disagree when checking `exp` and `nbf`, seconds.
pub const LEEWAY_SECS: u64 = 60;

/// Why a token or message was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TokenError {
    /// The SASL message does not follow RFC 7628.
    #[error("malformed OAUTHBEARER message: {0}")]
    Malformed(&'static str),
    /// No key with the token's `kid`: fetch the key set again.
    #[error("no key {0:?} in the issuer's key set")]
    UnknownKey(String),
    /// The token's algorithm is not allowed (symmetric or none).
    #[error("algorithm {0} is not allowed")]
    Algorithm(String),
    /// Signature, issuer or audience wrong, or the token unreadable.
    #[error("the token is not valid: {0}")]
    Invalid(String),
    /// Past its `exp`.
    #[error("the token has expired")]
    Expired,
    /// Before its `nbf`.
    #[error("the token is not valid yet")]
    NotYetValid,
    /// The claim naming the principal is missing or empty.
    #[error("the token has no {0:?} claim to name the principal")]
    NoPrincipal(String),
    /// The discovery document or key set is unusable.
    #[error("the identity provider's metadata is not usable: {0}")]
    Metadata(String),
}

/// The client's initial response for `token`.
pub fn initial_response(token: &str) -> Vec<u8> {
    format!("n,,\x01auth=Bearer {token}\x01\x01").into_bytes()
}

/// The bearer token in a client's initial response.
pub fn token_of(msg: &[u8]) -> Result<String, TokenError> {
    let msg = std::str::from_utf8(msg).map_err(|_| TokenError::Malformed("not UTF-8"))?;
    let rest = msg.strip_prefix("n,").ok_or(TokenError::Malformed("only \"n,\" is supported"))?;
    // An optional authorization identity, which Lotse does not use.
    let (_authzid, kvs) = rest.split_once(',').ok_or(TokenError::Malformed("no gs2 header"))?;
    let kvs = kvs.strip_prefix('\x01').ok_or(TokenError::Malformed("no key-value pairs"))?;
    let kvs = kvs.strip_suffix("\x01\x01").ok_or(TokenError::Malformed("not terminated"))?;
    for kv in kvs.split('\x01') {
        if let Some(auth) = kv.strip_prefix("auth=") {
            let (scheme, token) = auth.split_once(' ').ok_or(TokenError::Malformed("no scheme"))?;
            if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
                return Err(TokenError::Malformed("not a bearer token"));
            }
            return Ok(token.to_owned());
        }
    }
    Err(TokenError::Malformed("no auth"))
}

/// The `jwks_uri` of an OpenID discovery document, which must name `issuer`.
pub fn jwks_uri(discovery: &[u8], issuer: &str) -> Result<String, TokenError> {
    let doc: serde_json::Value = serde_json::from_slice(discovery).map_err(|e| TokenError::Metadata(e.to_string()))?;
    if doc.get("issuer").and_then(|v| v.as_str()) != Some(issuer) {
        return Err(TokenError::Metadata(format!("the discovery document is not for {issuer}")));
    }
    doc.get("jwks_uri")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| TokenError::Metadata("no jwks_uri".into()))
}

/// The discovery document's URL for `issuer`.
pub fn discovery_url(issuer: &str) -> String {
    format!("{}/.well-known/openid-configuration", issuer.trim_end_matches('/'))
}

/// The issuer's signing keys, by key id.
#[derive(Clone, Default)]
pub struct Jwks {
    keys: BTreeMap<String, (DecodingKey, Option<Algorithm>)>,
}

impl std::fmt::Debug for Jwks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Jwks").field("kids", &self.keys.keys().collect::<Vec<_>>()).finish()
    }
}

impl Jwks {
    /// Reads a JSON Web Key Set. Keys without an id, or of a kind that cannot
    /// verify signatures, are skipped.
    pub fn parse(json: &[u8]) -> Result<Jwks, TokenError> {
        let set: JwkSet = serde_json::from_slice(json).map_err(|e| TokenError::Metadata(e.to_string()))?;
        let mut keys = BTreeMap::new();
        for jwk in &set.keys {
            let Some(kid) = jwk.common.key_id.clone() else { continue };
            let Ok(key) = DecodingKey::from_jwk(jwk) else { continue };
            let alg = jwk.common.key_algorithm.and_then(|a| a.to_string().parse::<Algorithm>().ok());
            keys.insert(kid, (key, alg));
        }
        Ok(Jwks { keys })
    }

    /// Whether the set holds key `kid`.
    pub fn has(&self, kid: &str) -> bool {
        self.keys.contains_key(kid)
    }
}

/// What a broker requires of a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcSettings {
    /// The issuer, exactly as in the token's `iss`.
    pub issuer: String,
    /// The audience the token must name in `aud`.
    pub audience: String,
    /// The claim that names the principal (`sub`, `preferred_username`, …).
    pub principal_claim: String,
    /// The claim listing the user's groups at the identity provider.
    pub groups_claim: String,
}

/// A token that passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The principal.
    pub principal: String,
    /// The groups at the identity provider.
    pub groups: Vec<String>,
    /// `exp`, seconds since the Unix epoch.
    pub expires_at: u64,
}

/// The key id a token names, so a broker can fetch the key set again
/// before validating if it does not have that key.
pub fn key_id(token: &str) -> Result<String, TokenError> {
    let header = jsonwebtoken::decode_header(token).map_err(|e| TokenError::Invalid(e.to_string()))?;
    header.kid.ok_or_else(|| TokenError::Invalid("no kid in the token's header".into()))
}

/// Checks `token`: signed by one of `jwks` with an asymmetric algorithm,
/// issued by `settings.issuer` for `settings.audience`, valid at `now_secs`
/// (seconds since the Unix epoch, with [`LEEWAY_SECS`] of slack), and naming
/// a principal.
pub fn validate(token: &str, jwks: &Jwks, settings: &OidcSettings, now_secs: u64) -> Result<Verified, TokenError> {
    let header = jsonwebtoken::decode_header(token).map_err(|e| TokenError::Invalid(e.to_string()))?;
    // Only asymmetric signatures: a shared secret would let anyone holding
    // it, the broker included, mint tokens.
    if matches!(header.alg, Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512) {
        return Err(TokenError::Algorithm(format!("{:?}", header.alg)));
    }
    let kid = header.kid.ok_or_else(|| TokenError::Invalid("no kid in the token's header".into()))?;
    let (key, key_alg) = jwks.keys.get(&kid).ok_or_else(|| TokenError::UnknownKey(kid.clone()))?;
    if key_alg.is_some_and(|a| a != header.alg) {
        return Err(TokenError::Algorithm(format!("{:?} with a key for {:?}", header.alg, key_alg)));
    }
    let mut v = Validation::new(header.alg);
    v.set_issuer(&[&settings.issuer]);
    v.set_audience(&[&settings.audience]);
    // Time is checked below against the time the caller passes in.
    v.validate_exp = false;
    v.validate_nbf = false;
    v.set_required_spec_claims(&["exp", "iss", "aud"]);
    let data =
        jsonwebtoken::decode::<serde_json::Value>(token, key, &v).map_err(|e| TokenError::Invalid(e.to_string()))?;
    let claims = data.claims;
    let exp =
        claims.get("exp").and_then(|v| v.as_u64()).ok_or_else(|| TokenError::Invalid("exp is not a number".into()))?;
    if exp.saturating_add(LEEWAY_SECS) <= now_secs {
        return Err(TokenError::Expired);
    }
    if let Some(nbf) = claims.get("nbf") {
        let nbf = nbf.as_u64().ok_or_else(|| TokenError::Invalid("nbf is not a number".into()))?;
        if nbf > now_secs.saturating_add(LEEWAY_SECS) {
            return Err(TokenError::NotYetValid);
        }
    }
    let principal = claims
        .get(&settings.principal_claim)
        .and_then(|v| v.as_str())
        .filter(|p| !p.is_empty())
        .ok_or_else(|| TokenError::NoPrincipal(settings.principal_claim.clone()))?
        .to_owned();
    let groups = match claims.get(&settings.groups_claim) {
        Some(serde_json::Value::Array(a)) => a.iter().filter_map(|g| g.as_str().map(str::to_owned)).collect(),
        Some(serde_json::Value::String(g)) => vec![g.clone()],
        _ => Vec::new(),
    };
    Ok(Verified { principal, groups, expires_at: exp })
}

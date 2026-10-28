//! A fake identity provider: an issuer with one Ed25519 key that signs
//! whatever claims a test asks for. Deterministic from its seed.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64URL;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use ring::signature::KeyPair as _;

/// An issuer for tests.
#[derive(Debug, Clone)]
pub struct TestIssuer {
    /// Its `iss`.
    pub issuer: String,
    /// The key id it signs with.
    pub kid: String,
    seed: [u8; 32],
}

impl TestIssuer {
    /// An issuer called `issuer` whose key comes from `seed`.
    pub fn new(issuer: &str, kid: &str, seed: u64) -> TestIssuer {
        let mut s = [0u8; 32];
        for (i, b) in s.iter_mut().enumerate() {
            *b = (seed.rotate_left(i as u32 * 8) as u8) ^ (i as u8).wrapping_mul(151);
        }
        TestIssuer { issuer: issuer.into(), kid: kid.into(), seed: s }
    }

    fn pkcs8(&self) -> Vec<u8> {
        // PKCS#8 v1 for an Ed25519 private key: a fixed prefix, then the seed.
        let mut der =
            vec![0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20];
        der.extend_from_slice(&self.seed);
        der
    }

    /// A token with `claims` (a JSON object), signed with this issuer's key.
    pub fn sign(&self, claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.kid.clone());
        jsonwebtoken::encode(&header, claims, &EncodingKey::from_ed_der(&self.pkcs8())).expect("signing")
    }

    /// A token for `principal` with `groups`, for `audience`, valid until
    /// `exp` (seconds since the Unix epoch).
    pub fn token(&self, principal: &str, groups: &[&str], audience: &str, exp: u64) -> String {
        self.sign(&serde_json::json!({
            "iss": self.issuer, "aud": audience, "sub": principal, "groups": groups, "exp": exp,
        }))
    }

    /// The issuer's key set, JSON.
    pub fn jwks(&self) -> Vec<u8> {
        let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&self.seed).expect("a key from a seed");
        let x = B64URL.encode(pair.public_key().as_ref());
        serde_json::to_vec(&serde_json::json!({
            "keys": [{"kty": "OKP", "crv": "Ed25519", "x": x, "kid": self.kid, "alg": "EdDSA", "use": "sig"}]
        }))
        .expect("json")
    }

    /// The issuer's discovery document, pointing at `jwks_uri`.
    pub fn discovery(&self, jwks_uri: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"issuer": self.issuer, "jwks_uri": jwks_uri})).expect("json")
    }
}

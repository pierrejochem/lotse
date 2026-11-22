//! Access tokens: a credential whose text the user holds and whose SHA-256
//! the cluster keeps, free of I/O.
//!
//! A token reads `lotse_tok_<id>_<secret>`: `<id>` is 16 lowercase hex digits
//! and names the token in the cluster's state, `<secret>` is 32 random bytes
//! in unpadded base64url (43 characters). Only the SHA-256 of the 32 secret
//! bytes is ever stored, so a copy of the cluster's state cannot be used to
//! sign in; the secret is long and random, so a plain hash is enough and
//! needs no salt or stretching. The caller passes in the id and the secret:
//! nothing here draws randomness.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use sha2::{Digest, Sha256};

use crate::scram::ct_eq;

/// The SASL mechanism name a client authenticates with by token.
pub const MECHANISM: &str = "LOTSE-TOKEN";

const PREFIX: &str = "lotse_tok_";
const SECRET_CHARS: usize = 43;
const DAY_MS: u64 = 24 * 60 * 60 * 1000;

/// How long a token lives: the five steps a user may choose from. The code
/// is what travels in requests and the metadata state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifetime {
    /// 1 week (7 days).
    Week = 1,
    /// 1 month (30 days).
    Month = 2,
    /// 3 months (90 days).
    Quarter = 3,
    /// 6 months (180 days).
    Half = 4,
    /// 1 year (365 days).
    Year = 5,
}

impl Lifetime {
    /// The lifetime a code names; `None` for anything but 1 to 5.
    pub fn from_code(code: u8) -> Option<Lifetime> {
        match code {
            1 => Some(Lifetime::Week),
            2 => Some(Lifetime::Month),
            3 => Some(Lifetime::Quarter),
            4 => Some(Lifetime::Half),
            5 => Some(Lifetime::Year),
            _ => None,
        }
    }

    /// The lifetime in milliseconds.
    pub fn millis(self) -> u64 {
        let days = match self {
            Lifetime::Week => 7,
            Lifetime::Month => 30,
            Lifetime::Quarter => 90,
            Lifetime::Half => 180,
            Lifetime::Year => 365,
        };
        days * DAY_MS
    }

    /// The code that names this lifetime in requests and state.
    pub fn code(self) -> u8 {
        self as u8
    }
}

/// An access token: its id and its secret.
#[derive(Clone, PartialEq, Eq)]
pub struct Token {
    /// Names the token in the cluster's state.
    pub id: u64,
    /// The secret bytes; only their hash is stored.
    pub secret: [u8; 32],
}

// By hand: a derived `Debug` would put the secret into logs.
impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token").field("id", &format_args!("{:016x}", self.id)).finish_non_exhaustive()
    }
}

impl Token {
    /// A token from an id and a secret the caller drew.
    pub fn new(id: u64, secret: [u8; 32]) -> Token {
        Token { id, secret }
    }

    /// The text the user holds: `lotse_tok_<id>_<secret>`.
    pub fn text(&self) -> String {
        format!("{PREFIX}{:016x}_{}", self.id, B64.encode(self.secret))
    }

    /// Reads a token's text, ignoring ASCII whitespace around it (a pasted
    /// token often carries a newline). `None` if it is not exactly a token.
    pub fn parse(text: &str) -> Option<Token> {
        let rest = text.trim_ascii().strip_prefix(PREFIX)?;
        let (id, secret) = rest.split_once('_')?;
        // `from_str_radix` accepts a sign and upper case; the text does not.
        if id.len() != 16 || !id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
            return None;
        }
        if secret.len() != SECRET_CHARS {
            return None;
        }
        let id = u64::from_str_radix(id, 16).ok()?;
        // 43 characters leave 2 spare bits: the canonical text has them zero,
        // and only the canonical text is a token, so one secret has one text.
        let bytes = B64.decode(secret).ok()?;
        let secret: [u8; 32] = bytes.try_into().ok()?;
        Some(Token { id, secret })
    }

    /// The SHA-256 of the secret: all the cluster keeps.
    pub fn hash(&self) -> [u8; 32] {
        Sha256::digest(self.secret).into()
    }
}

/// Whether a presented token's secret hashes to the stored hash, in time
/// that does not depend on where they differ.
pub fn matches(stored: &[u8; 32], presented: &Token) -> bool {
    ct_eq(stored, &presented.hash())
}

#[cfg(test)]
mod tests;

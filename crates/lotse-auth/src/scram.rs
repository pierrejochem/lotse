//! SCRAM-SHA-512 (RFC 5802, RFC 7677) for both ends, without I/O.
//!
//! The broker stores a [`ScramCredential`] per user: salt, iteration count,
//! `StoredKey` and `ServerKey`. Neither the password nor anything that lets
//! one log in without it is stored or sent: a stolen credential lets an
//! attacker impersonate the server, not the user.
//!
//! Channel binding is not used (`n,,`): TLS already ties the session to the
//! server's certificate.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

/// The mechanism's name in `SaslHandshake`.
pub const MECHANISM: &str = "SCRAM-SHA-512";

/// Fewest iterations a credential may have (RFC 7677 asks for 4096 at least).
pub const MIN_ITERATIONS: u32 = 4096;

/// Most iterations a credential may have: a bound on the work one login
/// costs the broker.
pub const MAX_ITERATIONS: u32 = 1 << 20;

/// What the broker keeps for one user.
#[derive(Clone, PartialEq, Eq)]
pub struct ScramCredential {
    /// The salt.
    pub salt: Vec<u8>,
    /// PBKDF2 iterations.
    pub iterations: u32,
    /// `H(ClientKey)`, 64 bytes.
    pub stored_key: Vec<u8>,
    /// `HMAC(SaltedPassword, "Server Key")`, 64 bytes.
    pub server_key: Vec<u8>,
}

impl std::fmt::Debug for ScramCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramCredential").field("iterations", &self.iterations).finish_non_exhaustive()
    }
}

impl ScramCredential {
    /// The credential for `password` with `salt` and `iterations`.
    pub fn new(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
        let salted = salted_password(password, salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        ScramCredential {
            salt: salt.to_vec(),
            iterations,
            stored_key: Sha512::digest(client_key).to_vec(),
            server_key: hmac(&salted, b"Server Key"),
        }
    }

    /// Whether the credential is well-formed: keys of the hash's length, a
    /// salt, and an iteration count within bounds.
    pub fn is_valid(&self) -> bool {
        self.stored_key.len() == 64
            && self.server_key.len() == 64
            && !self.salt.is_empty()
            && (MIN_ITERATIONS..=MAX_ITERATIONS).contains(&self.iterations)
    }
}

/// Why a SCRAM exchange failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScramError {
    /// A message does not follow the mechanism's grammar.
    #[error("malformed SCRAM message: {0}")]
    Malformed(&'static str),
    /// Wrong user name or password (the server does not say which).
    #[error("authentication failed")]
    Failed,
    /// The server's signature is wrong: it does not know the credential.
    #[error("the server could not prove it knows the credential")]
    ServerNotVerified,
    /// The step does not fit the exchange's state.
    #[error("unexpected SCRAM step")]
    OutOfOrder,
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha512> as Mac>::new_from_slice(key).expect("HMAC takes any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

fn salted_password(password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut out = vec![0u8; 64];
    pbkdf2::pbkdf2_hmac::<Sha512>(password.as_bytes(), salt, iterations, &mut out);
    out
}

/// Equality that takes as long for every input of one length.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// `saslname`: `,` and `=` escaped as `=2C` and `=3D`.
fn escape(name: &str) -> String {
    name.replace('=', "=3D").replace(',', "=2C")
}

fn unescape(name: &str) -> Result<String, ScramError> {
    let mut out = String::new();
    let mut rest = name;
    while let Some(i) = rest.find('=') {
        out.push_str(&rest[..i]);
        match rest.get(i..i + 3) {
            Some("=2C") => out.push(','),
            Some("=3D") => out.push('='),
            _ => return Err(ScramError::Malformed("bad escape in user name")),
        }
        rest = &rest[i + 3..];
    }
    out.push_str(rest);
    Ok(out)
}

/// The value of attribute `key` (`k=value`) in `part`.
fn attr(part: Option<&str>, key: char) -> Result<&str, ScramError> {
    let part = part.ok_or(ScramError::Malformed("missing attribute"))?;
    let mut chars = part.chars();
    if chars.next() != Some(key) || chars.next() != Some('=') {
        return Err(ScramError::Malformed("unexpected attribute"));
    }
    Ok(&part[2..])
}

fn valid_nonce(n: &str) -> bool {
    !n.is_empty() && n.bytes().all(|b| (0x21..=0x7e).contains(&b) && b != b',')
}

enum ServerState {
    Start,
    SentFirst { user: String, credential: Option<ScramCredential>, nonce: String, auth_prefix: String },
    Done,
}

/// The broker's end of one exchange.
pub struct ScramServer {
    state: ServerState,
}

impl std::fmt::Debug for ScramServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramServer").finish_non_exhaustive()
    }
}

impl Default for ScramServer {
    fn default() -> Self {
        ScramServer::new()
    }
}

impl ScramServer {
    /// A new exchange.
    pub fn new() -> ScramServer {
        ScramServer { state: ServerState::Start }
    }

    /// Takes the client's first message and answers with the server's.
    /// `lookup` finds the user's credential; `server_nonce` must be fresh and
    /// unpredictable (printable ASCII without `,`). An unknown user gets an
    /// answer like any other and fails at the end, so names cannot be probed.
    pub fn first(
        &mut self,
        msg: &[u8],
        lookup: impl FnOnce(&str) -> Option<ScramCredential>,
        server_nonce: &str,
    ) -> Result<Vec<u8>, ScramError> {
        if !matches!(self.state, ServerState::Start) {
            return Err(ScramError::OutOfOrder);
        }
        self.state = ServerState::Done;
        let msg = std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        // gs2 header: no channel binding, no authorization identity.
        let bare = msg.strip_prefix("n,,").ok_or(ScramError::Malformed("only \"n,,\" is supported"))?;
        let mut parts = bare.split(',');
        let user = unescape(attr(parts.next(), 'n')?)?;
        let client_nonce = attr(parts.next(), 'r')?;
        if user.is_empty() || !valid_nonce(client_nonce) || !valid_nonce(server_nonce) {
            return Err(ScramError::Malformed("empty user name or bad nonce"));
        }
        let credential = lookup(&user).filter(ScramCredential::is_valid);
        // An unknown user gets a salt that depends only on the name, so two
        // attempts look alike.
        let (salt, iterations) = match &credential {
            Some(c) => (c.salt.clone(), c.iterations),
            None => (Sha512::digest(format!("lotse unknown user {user}")).to_vec()[..16].to_vec(), MIN_ITERATIONS),
        };
        let nonce = format!("{client_nonce}{server_nonce}");
        let server_first = format!("r={nonce},s={},i={iterations}", B64.encode(&salt));
        let auth_prefix = format!("{bare},{server_first}");
        self.state = ServerState::SentFirst { user, credential, nonce, auth_prefix };
        Ok(server_first.into_bytes())
    }

    /// Takes the client's final message; on success, the authenticated user
    /// and the server's final message, which proves the server knew the
    /// credential.
    pub fn last(&mut self, msg: &[u8]) -> Result<(String, Vec<u8>), ScramError> {
        let ServerState::SentFirst { user, credential, nonce, auth_prefix } =
            std::mem::replace(&mut self.state, ServerState::Done)
        else {
            return Err(ScramError::OutOfOrder);
        };
        let msg = std::str::from_utf8(msg).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let (without_proof, proof) = msg.rsplit_once(",p=").ok_or(ScramError::Malformed("no proof"))?;
        let mut parts = without_proof.split(',');
        // base64("n,,"): the same gs2 header as in the first message.
        if attr(parts.next(), 'c')? != "biws" {
            return Err(ScramError::Malformed("channel binding does not match"));
        }
        if attr(parts.next(), 'r')? != nonce || parts.next().is_some() {
            return Err(ScramError::Failed);
        }
        let proof = B64.decode(proof).map_err(|_| ScramError::Malformed("proof is not base64"))?;
        let Some(credential) = credential else { return Err(ScramError::Failed) };
        let auth_message = format!("{auth_prefix},{without_proof}");
        let client_signature = hmac(&credential.stored_key, auth_message.as_bytes());
        if proof.len() != client_signature.len() {
            return Err(ScramError::Failed);
        }
        let client_key: Vec<u8> = proof.iter().zip(&client_signature).map(|(p, s)| p ^ s).collect();
        if !ct_eq(&Sha512::digest(&client_key), &credential.stored_key) {
            return Err(ScramError::Failed);
        }
        let server_signature = hmac(&credential.server_key, auth_message.as_bytes());
        Ok((user, format!("v={}", B64.encode(server_signature)).into_bytes()))
    }
}

enum ClientState {
    Start,
    SentFirst { bare: String },
    SentFinal { server_signature: Vec<u8> },
    Done,
}

/// A client's end of one exchange.
pub struct ScramClient {
    user: String,
    password: String,
    nonce: String,
    state: ClientState,
}

impl std::fmt::Debug for ScramClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScramClient").field("user", &self.user).finish_non_exhaustive()
    }
}

impl ScramClient {
    /// An exchange for `user` with `password`; `nonce` must be fresh and
    /// unpredictable (printable ASCII without `,`).
    pub fn new(user: &str, password: &str, nonce: &str) -> ScramClient {
        ScramClient { user: user.into(), password: password.into(), nonce: nonce.into(), state: ClientState::Start }
    }

    /// The client's first message.
    pub fn first(&mut self) -> Vec<u8> {
        let bare = format!("n={},r={}", escape(&self.user), self.nonce);
        let msg = format!("n,,{bare}");
        self.state = ClientState::SentFirst { bare };
        msg.into_bytes()
    }

    /// Answers the server's first message with the client's final one.
    pub fn respond(&mut self, server_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let ClientState::SentFirst { bare } = std::mem::replace(&mut self.state, ClientState::Done) else {
            return Err(ScramError::OutOfOrder);
        };
        let server_first = std::str::from_utf8(server_first).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let mut parts = server_first.split(',');
        let nonce = attr(parts.next(), 'r')?;
        let salt = B64.decode(attr(parts.next(), 's')?).map_err(|_| ScramError::Malformed("salt is not base64"))?;
        let iterations: u32 = attr(parts.next(), 'i')?.parse().map_err(|_| ScramError::Malformed("bad iterations"))?;
        // The server's nonce extends ours; a bound on the iterations keeps a
        // hostile server from making the client spin.
        if !nonce.starts_with(&self.nonce)
            || nonce.len() == self.nonce.len()
            || !valid_nonce(nonce)
            || !(MIN_ITERATIONS..=MAX_ITERATIONS).contains(&iterations)
        {
            return Err(ScramError::Malformed("server nonce or iteration count not acceptable"));
        }
        let salted = salted_password(&self.password, &salt, iterations);
        let client_key = hmac(&salted, b"Client Key");
        let stored_key = Sha512::digest(&client_key);
        let without_proof = format!("c=biws,r={nonce}");
        let auth_message = format!("{bare},{server_first},{without_proof}");
        let client_signature = hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key.iter().zip(&client_signature).map(|(k, s)| k ^ s).collect();
        let server_key = hmac(&salted, b"Server Key");
        self.state = ClientState::SentFinal { server_signature: hmac(&server_key, auth_message.as_bytes()) };
        Ok(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
    }

    /// Checks the server's final message: the server proves it knew the
    /// credential.
    pub fn verify(&mut self, server_final: &[u8]) -> Result<(), ScramError> {
        let ClientState::SentFinal { server_signature } = std::mem::replace(&mut self.state, ClientState::Done) else {
            return Err(ScramError::OutOfOrder);
        };
        let msg = std::str::from_utf8(server_final).map_err(|_| ScramError::Malformed("not UTF-8"))?;
        let sig = msg.strip_prefix("v=").ok_or(ScramError::ServerNotVerified)?;
        let sig = B64.decode(sig).map_err(|_| ScramError::ServerNotVerified)?;
        if ct_eq(&sig, &server_signature) { Ok(()) } else { Err(ScramError::ServerNotVerified) }
    }
}

//! Sessions sealed into the browser's cookie.
//!
//! The cookie holds the session itself, encrypted and authenticated with
//! AES-256-GCM under a key only the console has: who signed in, until
//! when, and the refresh token (or, if the provider gives none, the access
//! token). A console that restarts, or another console with the same key,
//! reads it back and renews the access token from it (ADR 0041).

use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD as B64URL};
use lotse_rt::Rng as _;
use lotse_rt_tokio::TokioRuntime;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use serde::{Deserialize, Serialize};

/// Binds a sealed value to its use: a value sealed for something else
/// does not open as a session.
const AAD: &[u8] = b"lotse-console-session-v1";

/// What the cookie holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Sealed {
    /// The session's id: the same through every renewal.
    #[serde(rename = "i")]
    pub(crate) sid: String,
    /// The principal.
    #[serde(rename = "p")]
    pub(crate) principal: String,
    /// The name to show.
    #[serde(rename = "n")]
    pub(crate) display_name: String,
    /// The refresh token.
    #[serde(rename = "r", default, skip_serializing_if = "Option::is_none")]
    pub(crate) refresh_token: Option<String>,
    /// The access token, only when there is no refresh token.
    #[serde(rename = "a", default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    /// When that access token expires, ms since the epoch.
    #[serde(rename = "ae", default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_expires_ms: Option<u64>,
    /// When the session ends whatever happens, ms since the epoch.
    #[serde(rename = "x")]
    pub(crate) expires_ms: u64,
}

/// The key sessions are sealed with.
pub struct SessionKey(LessSafeKey);

impl std::fmt::Debug for SessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionKey(…)")
    }
}

impl SessionKey {
    /// A key from 32 bytes.
    pub fn from_bytes(bytes: &[u8; 32]) -> SessionKey {
        SessionKey(LessSafeKey::new(UnboundKey::new(&AES_256_GCM, bytes).expect("32 bytes is an AES-256 key")))
    }

    /// The key in `path`: 32 random bytes in base64, as
    /// `openssl rand -base64 32` writes them.
    pub fn from_file(path: &Path) -> Result<SessionKey, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let bytes = B64.decode(text.trim()).map_err(|e| format!("{}: not base64: {e}", path.display()))?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|b: Vec<u8>| {
            format!(
                "{}: {} bytes, a session key has 32; make one with `openssl rand -base64 32`",
                path.display(),
                b.len()
            )
        })?;
        Ok(SessionKey::from_bytes(&bytes))
    }

    /// The key in `path`; if there is no such file, a new key, written
    /// there first (readable by this user only). Returns whether it was
    /// made now.
    pub fn load_or_create(path: &Path, rt: &TokioRuntime) -> Result<(SessionKey, bool), String> {
        let mut bytes = [0u8; 32];
        rt.fill_secret(&mut bytes);
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        }
        let mut open = std::fs::OpenOptions::new();
        // Never over an existing key: the one made at the first start, or
        // by another console starting at the same time.
        open.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
        match open.open(path) {
            Ok(mut f) => {
                use std::io::Write as _;
                f.write_all(format!("{}\n", B64.encode(bytes)).as_bytes())
                    .and_then(|()| f.sync_all())
                    .map_err(|e| format!("{}: {e}", path.display()))?;
                Ok((SessionKey::from_bytes(&bytes), true))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => SessionKey::from_file(path).map(|k| (k, false)),
            Err(e) => Err(format!("{}: {e}", path.display())),
        }
    }

    /// `s`, sealed for a cookie.
    pub(crate) fn seal(&self, rt: &TokioRuntime, s: &Sealed) -> String {
        let mut nonce = [0u8; NONCE_LEN];
        rt.fill_secret(&mut nonce);
        let mut buf = serde_json::to_vec(s).expect("a session serialises");
        self.0
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::from(AAD), &mut buf)
            .expect("a session fits AES-GCM");
        let mut out = nonce.to_vec();
        out.extend_from_slice(&buf);
        B64URL.encode(out)
    }

    /// The session in a cookie, if this key sealed it and nobody changed it.
    pub(crate) fn open(&self, cookie: &str) -> Option<Sealed> {
        let raw = B64URL.decode(cookie).ok()?;
        if raw.len() < NONCE_LEN {
            return None;
        }
        let (nonce, sealed) = raw.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce).ok()?;
        let mut buf = sealed.to_vec();
        let plain = self.0.open_in_place(nonce, Aad::from(AAD), &mut buf).ok()?;
        serde_json::from_slice(plain).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> Sealed {
        Sealed {
            sid: "s1".into(),
            principal: "bob".into(),
            display_name: "Bob".into(),
            refresh_token: Some("refresh".into()),
            access_token: None,
            access_expires_ms: None,
            expires_ms: 42,
        }
    }

    #[test]
    fn a_sealed_session_opens_only_with_its_key_and_unchanged() {
        let rt = TokioRuntime::new(std::env::temp_dir());
        let key = SessionKey::from_bytes(&[7; 32]);
        let cookie = key.seal(&rt, &session());
        assert_eq!(key.open(&cookie), Some(session()));
        assert_ne!(cookie, key.seal(&rt, &session()), "a fresh nonce each time");
        assert!(!cookie.contains("refresh") && !B64URL.decode(&cookie).unwrap().windows(7).any(|w| w == b"refresh"));
        assert_eq!(SessionKey::from_bytes(&[8; 32]).open(&cookie), None, "another key");
        let mut raw = B64URL.decode(&cookie).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 1;
        assert_eq!(key.open(&B64URL.encode(raw)), None, "a changed byte");
        assert_eq!(key.open("not a cookie"), None);
    }

    #[test]
    fn a_key_file_holds_32_bytes_in_base64() {
        let dir = std::env::temp_dir().join(format!("lotse-session-key-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let good = dir.join("good");
        std::fs::write(&good, format!("{}\n", B64.encode([9u8; 32]))).unwrap();
        let rt = TokioRuntime::new(&dir);
        let cookie = SessionKey::from_file(&good).unwrap().seal(&rt, &session());
        assert_eq!(SessionKey::from_bytes(&[9; 32]).open(&cookie), Some(session()));
        // Made once, then kept.
        let made = dir.join("data").join("session.key");
        let (first, created) = SessionKey::load_or_create(&made, &rt).unwrap();
        assert!(created);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&made).unwrap().permissions().mode() & 0o777,
                0o600,
                "only its owner reads it"
            );
        }
        let cookie = first.seal(&rt, &session());
        let (again, created) = SessionKey::load_or_create(&made, &rt).unwrap();
        assert!(!created);
        assert_eq!(again.open(&cookie), Some(session()), "the same key after a restart");
        let short = dir.join("short");
        std::fs::write(&short, B64.encode([9u8; 16])).unwrap();
        assert!(SessionKey::from_file(&short).unwrap_err().contains("16 bytes"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

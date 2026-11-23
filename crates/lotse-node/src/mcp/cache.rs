//! The signed-in clients of `lotse mcp --listen` (ADR 0134): one per
//! credential, so a request is served only by the client its own credential
//! signed in, and repeated requests do not sign in again.
//!
//! An entry is keyed by the SHA-256 of the credential, never the credential
//! itself. It goes after [`IDLE`] unused, and when [`MAX`] are held the
//! least recently used goes first; dropping the last handle to a client
//! closes its connections. Sign-ins for one key happen one at a time, so
//! concurrent requests with one credential sign in once.
//!
//! Anyone who reaches the listener can make up credentials, so sign-ins in
//! progress cost bounded work and cannot push out clients that work: they
//! do not count toward [`MAX`] and only clients that signed in are evicted;
//! at most [`MAX_PENDING`] wait at once, and at most [`MAX_SIGN_INS`] reach
//! the cluster at once. A request that cannot start its sign-in within
//! [`SIGN_IN_WAIT`] is refused as busy rather than queued.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_client::{Client, ClientError};
use lotse_rt::{Clock as _, Instant};
use lotse_rt_tokio::TokioRuntime;
use sha2::{Digest as _, Sha256};

use super::{Reach, SignIn, client};

/// How long a client stays unused before it is closed.
pub const IDLE: Duration = Duration::from_secs(15 * 60);

/// The most signed-in clients held at once.
pub const MAX: usize = 1_000;

/// The most credentials whose sign-in is waiting or in progress at once;
/// a request for one more is refused as busy at once.
pub const MAX_PENDING: usize = 256;

/// The most sign-ins that reach the cluster at once: each builds a client,
/// opens a TLS connection and, for a password, computes a SCRAM proof.
pub const MAX_SIGN_INS: usize = 32;

/// How long a request waits for its turn to sign in, in all (behind
/// another request for the same credential and for a permit), before it is
/// refused as busy. Long enough to ride out a burst of real agents (a sign-in takes
/// milliseconds, 32 run at once), short enough that a flood of made-up
/// credentials gets a quick 503 instead of piling up open requests.
pub const SIGN_IN_WAIT: Duration = Duration::from_secs(5);

/// The credential one HTTP request carried in its `Authorization` header.
#[derive(Clone, PartialEq, Eq)]
pub enum HttpCredential {
    /// `Bearer <access token>`.
    Token(String),
    /// `Basic <user:password>`, signed in with SCRAM-SHA-512.
    Password {
        /// The user.
        user: String,
        /// The password.
        password: String,
    },
}

impl std::fmt::Debug for HttpCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the token or the password.
        match self {
            HttpCredential::Token(_) => f.write_str("Token"),
            HttpCredential::Password { user, .. } => write!(f, "Password({user})"),
        }
    }
}

impl HttpCredential {
    /// The credential in an `Authorization` header's value, if it holds
    /// one: `Bearer <token>` or `Basic <base64 of user:password>`, the
    /// scheme in any case.
    pub fn from_authorization(value: &str) -> Option<HttpCredential> {
        use base64::Engine as _;
        let (scheme, rest) = value.trim().split_once(' ')?;
        let rest = rest.trim();
        if scheme.eq_ignore_ascii_case("bearer") {
            return (!rest.is_empty()).then(|| HttpCredential::Token(rest.to_string()));
        }
        if !scheme.eq_ignore_ascii_case("basic") {
            return None;
        }
        let decoded = base64::engine::general_purpose::STANDARD.decode(rest).ok()?;
        let text = String::from_utf8(decoded).ok()?;
        let (user, password) = text.split_once(':')?;
        (!user.is_empty()).then(|| HttpCredential::Password { user: user.to_string(), password: password.to_string() })
    }

    /// The cache key: the SHA-256 of the kind and the whole credential, so
    /// two credentials never share a client and the key does not give the
    /// credential away.
    pub fn key(&self) -> CredentialKey {
        let mut h = Sha256::new();
        match self {
            HttpCredential::Token(t) => {
                h.update(b"token\0");
                h.update(t.as_bytes());
            }
            HttpCredential::Password { user, password } => {
                h.update(b"password\0");
                // The user's length first: "a:bc" and "ab:c" differ.
                h.update((user.len() as u64).to_be_bytes());
                h.update(user.as_bytes());
                h.update(password.as_bytes());
            }
        }
        CredentialKey(h.finalize().into())
    }

    fn sign_in(&self) -> SignIn {
        match self {
            HttpCredential::Token(t) => SignIn::Token(t.clone()),
            HttpCredential::Password { user, password } => {
                SignIn::Scram { user: user.clone(), password: password.clone() }
            }
        }
    }
}

/// The SHA-256 of a credential, which names its client in the cache.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CredentialKey([u8; 32]);

impl std::fmt::Debug for CredentialKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CredentialKey(..)")
    }
}

/// Why a request's credential did not give it a client.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignInError {
    /// The cluster refused the credential: unknown, wrong, expired or
    /// revoked. The same for every reason, as the cluster's answer is.
    #[error("the cluster did not accept the credential")]
    Refused,
    /// The cluster did not answer, or the client could not be made.
    #[error("the cluster could not be reached: {0}")]
    Unreachable(String),
    /// Too many sign-ins are waiting or in progress; try again shortly.
    #[error("too many sign-ins at once")]
    Busy,
}

impl From<Busy> for SignInError {
    fn from(_: Busy) -> SignInError {
        SignInError::Busy
    }
}

impl From<ClientError> for SignInError {
    fn from(e: ClientError) -> SignInError {
        match e {
            ClientError::Authentication(_) => SignInError::Refused,
            e => SignInError::Unreachable(e.to_string()),
        }
    }
}

/// One signed-in client per credential.
pub struct ClientCache {
    reach: Reach,
    cache: Cache<Client<TokioRuntime>>,
}

impl std::fmt::Debug for ClientCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientCache").field("clients", &self.cache.len()).finish_non_exhaustive()
    }
}

impl ClientCache {
    /// No clients yet; each one reaches the cluster as `reach` says.
    pub fn new(reach: Reach) -> ClientCache {
        let rt = TokioRuntime::new(std::env::temp_dir());
        ClientCache { reach, cache: Cache::new(Box::new(move || rt.now()), Limits::default()) }
    }

    /// The client `credential` signed in, signing in first if none is held.
    ///
    /// A held client is asked who it is before it is handed out, so a token
    /// revoked or expired since it signed in fails now, at the cluster: the
    /// client is dropped and the request refused, and the next request
    /// signs in afresh.
    pub async fn get_or_sign_in(&self, credential: &HttpCredential) -> Result<Arc<Client<TokioRuntime>>, SignInError> {
        let key = credential.key();
        let (client, fresh) = self
            .cache
            .get_or_insert(key, || async {
                let c = client(&self.reach, credential.sign_in()).map_err(SignInError::Unreachable)?;
                let me = c.who_am_i().await?;
                tracing::debug!(principal = %me.name, "signed in for an HTTP client");
                Ok::<_, SignInError>(c)
            })
            .await?;
        if !fresh && let Err(e) = still_signed_in(&client).await {
            if e == SignInError::Refused {
                self.cache.forget(&key, &client);
            }
            return Err(e);
        }
        Ok(client)
    }

    /// Drops `client` from the cache if it is still the one held for `key`:
    /// the cluster no longer accepts its credential.
    pub fn forget(&self, key: &CredentialKey, client: &Arc<Client<TokioRuntime>>) {
        self.cache.forget(key, client);
    }

    /// Closes the clients unused for [`IDLE`].
    pub fn sweep(&self) {
        self.cache.sweep();
    }

    /// How many signed-in clients are held.
    pub fn len(&self) -> usize {
        self.cache.held()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Whether the cluster still takes `client`'s credential. `WhoAmI` changes
/// nothing, so an outcome the client calls unknown (its answer lost on a
/// connection the node closed, as it closes those of a revoked token) is
/// asked once more, on a new connection, whose answer says.
async fn still_signed_in(client: &Client<TokioRuntime>) -> Result<(), SignInError> {
    match client.who_am_i().await {
        Ok(_) => Ok(()),
        Err(ClientError::Unknown(_)) => client.who_am_i().await.map(drop).map_err(SignInError::from),
        Err(e) => Err(e.into()),
    }
}

/// The cache refused to start a sign-in: too many wait or run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Busy;

/// Where a request stands once it has waited.
enum Turn<T> {
    /// The key has a value.
    Held(Arc<T>),
    /// Too many keys wait already.
    Busy,
    /// This request makes the value, holding the key's gate and a permit.
    Make {
        gate: Arc<tokio::sync::Mutex<()>>,
        guard: tokio::sync::OwnedMutexGuard<()>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
}

/// The cache's bounds; tests use smaller ones.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    /// Signed-in values held.
    pub(crate) held: usize,
    /// Keys whose sign-in waits or runs.
    pub(crate) pending: usize,
    /// Sign-ins running at once.
    pub(crate) signing: usize,
    /// How long a request waits for its turn in all.
    pub(crate) wait: Duration,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits { held: MAX, pending: MAX_PENDING, signing: MAX_SIGN_INS, wait: SIGN_IN_WAIT }
    }
}

/// The cache itself, over any value, so its rules are tested without a
/// cluster.
pub(crate) struct Cache<T> {
    entries: Mutex<BTreeMap<CredentialKey, Entry<T>>>,
    now: Box<dyn Fn() -> Instant + Send + Sync>,
    limits: Limits,
    /// One permit per sign-in running.
    signing: Arc<tokio::sync::Semaphore>,
}

struct Entry<T> {
    /// Held while one request signs in for this key; the others wait.
    gate: Arc<tokio::sync::Mutex<()>>,
    /// The signed-in value, once there is one.
    value: Option<Arc<T>>,
    /// When it was last asked for.
    used: Instant,
}

impl<T> Cache<T> {
    pub(crate) fn new(now: Box<dyn Fn() -> Instant + Send + Sync>, limits: Limits) -> Cache<T> {
        let signing = Arc::new(tokio::sync::Semaphore::new(limits.signing));
        Cache { entries: Mutex::new(BTreeMap::new()), now, limits, signing }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<CredentialKey, Entry<T>>> {
        self.entries.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The value held for `key`, or the one `make` returns, which is then
    /// held. Only one `make` runs for a key at a time; a request that waited
    /// for it gets its value. A failed `make` holds nothing, and the next
    /// waiter makes its own. The flag says whether the value was just made.
    ///
    /// A key without a value waits for its turn, behind the request
    /// already signing in for it and for one of [`Limits::signing`] permits,
    /// at most [`Limits::wait`] in all for the request, and only
    /// [`Limits::pending`] keys may wait or make at once; otherwise the
    /// answer is [`Busy`]. Only held values are evicted to make room, so
    /// made-up keys never push out a working one.
    pub(crate) async fn get_or_insert<E, F, Fut>(&self, key: CredentialKey, make: F) -> Result<(Arc<T>, bool), E>
    where
        E: From<Busy>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        // One deadline for the whole wait (the key's gate and a permit), not
        // one per attempt: a waiter that starts over after the attempt
        // before it failed still answers within one bound.
        let turn = match tokio::time::timeout(self.limits.wait, self.turn(key)).await {
            Ok(t) => t,
            Err(_) => {
                self.abandon(&key);
                return Err(Busy.into());
            }
        };
        let (gate, _guard, _permit) = match turn {
            Turn::Held(v) => return Ok((v, false)),
            Turn::Busy => return Err(Busy.into()),
            Turn::Make { gate, guard, permit } => (gate, guard, permit),
        };
        let made = make().await;
        let mut entries = self.lock();
        let ours = entries.get_mut(&key).filter(|e| Arc::ptr_eq(&e.gate, &gate));
        match made {
            Ok(v) => {
                let v = Arc::new(v);
                // An entry evicted meanwhile is not put back.
                if let Some(e) = ours {
                    e.value = Some(v.clone());
                    e.used = (self.now)();
                    evict(&mut entries, key, self.limits.held);
                }
                Ok((v, true))
            }
            Err(err) => {
                if ours.is_some() {
                    entries.remove(&key);
                }
                Err(err)
            }
        }
    }

    /// Waits until `key` has a value, or this request holds the key's gate
    /// and a permit to make one.
    async fn turn(&self, key: CredentialKey) -> Turn<T> {
        loop {
            let gate = {
                let mut entries = self.lock();
                let now = (self.now)();
                sweep(&mut entries, now);
                match entries.get_mut(&key) {
                    Some(e) => {
                        e.used = now;
                        if let Some(v) = &e.value {
                            return Turn::Held(v.clone());
                        }
                        e.gate.clone()
                    }
                    None => {
                        if entries.values().filter(|e| e.value.is_none()).count() >= self.limits.pending {
                            return Turn::Busy;
                        }
                        let gate = Arc::new(tokio::sync::Mutex::new(()));
                        entries.insert(key, Entry { gate: gate.clone(), value: None, used: now });
                        gate
                    }
                }
            };
            let guard = gate.clone().lock_owned().await;
            {
                let entries = self.lock();
                match entries.get(&key) {
                    // Made while this request waited.
                    Some(e) if Arc::ptr_eq(&e.gate, &gate) && e.value.is_some() => {
                        return Turn::Held(e.value.clone().expect("checked"));
                    }
                    Some(e) if Arc::ptr_eq(&e.gate, &gate) => {}
                    // The one before failed, or the entry went: start over.
                    _ => continue,
                }
            }
            let permit = self.signing.clone().acquire_owned().await.expect("the semaphore is never closed");
            return Turn::Make { gate, guard, permit };
        }
    }

    /// A request ran out of time: its key's pending entry goes unless
    /// another request holds the gate, which then cleans up after itself.
    fn abandon(&self, key: &CredentialKey) {
        let mut entries = self.lock();
        if entries.get(key).is_some_and(|e| e.value.is_none() && e.gate.try_lock().is_ok()) {
            entries.remove(key);
        }
    }

    /// Drops the entry for `key` if it holds `value`.
    pub(crate) fn forget(&self, key: &CredentialKey, value: &Arc<T>) {
        let mut entries = self.lock();
        if entries.get(key).and_then(|e| e.value.as_ref()).is_some_and(|v| Arc::ptr_eq(v, value)) {
            entries.remove(key);
        }
    }

    /// Drops the entries unused for [`IDLE`].
    pub(crate) fn sweep(&self) {
        let now = (self.now)();
        sweep(&mut self.lock(), now);
    }

    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    /// How many entries hold a value.
    pub(crate) fn held(&self) -> usize {
        self.lock().values().filter(|e| e.value.is_some()).count()
    }
}

/// Drops the least recently used held entries, never `keep` (just made) and
/// never one whose sign-in is in progress, until at most `max` hold a value.
fn evict<T>(entries: &mut BTreeMap<CredentialKey, Entry<T>>, keep: CredentialKey, max: usize) {
    while entries.values().filter(|e| e.value.is_some()).count() > max {
        let oldest = entries
            .iter()
            .filter(|(k, e)| e.value.is_some() && **k != keep)
            .min_by_key(|(_, e)| e.used)
            .map(|(k, _)| *k);
        match oldest {
            Some(k) => entries.remove(&k),
            None => return,
        };
    }
}

fn sweep<T>(entries: &mut BTreeMap<CredentialKey, Entry<T>>, now: Instant) {
    entries.retain(|_, e| now.saturating_duration_since(e.used) < IDLE);
}

#[cfg(test)]
mod tests;

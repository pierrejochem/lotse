//! Renewing the client certificate by itself
//! ([ADR 0123](../../docs/adr/0123-client-certificates-are-renewed-over-a-connection-made-with-them.md)).
//!
//! The client looks at the certificate in a [`CertificateStore`] at once and
//! then every given interval, renews it over a connection once its window
//! is open, and has the store put the new key and chain in place. A failure
//! is tried again a minute later and kept for [`CertificateRenewal::failure`]
//! until a later look succeeds. The loop runs on the client's runtime, so
//! the simulation drives it like everything else.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use lotse_rt::{CertificateStore, Clock, Net, Rng, Spawn};

use crate::Client;

/// How soon a failed look is tried again, unless the interval is shorter.
pub const RENEW_RETRY: Duration = Duration::from_secs(60);

/// How often [`Client::keep_certificate_renewed`] looks by default.
pub const RENEW_CHECK: Duration = Duration::from_secs(3600);

/// A running renewal, from [`Client::keep_certificate_renewed`]. Dropping it
/// stops the renewal at its next look; so does dropping the client.
pub struct CertificateRenewal {
    state: Arc<State>,
}

#[derive(Default)]
struct State {
    stop: AtomicBool,
    renewals: AtomicU64,
    failure: Mutex<Option<String>>,
}

impl CertificateRenewal {
    /// Why the last look failed, or `None` if it succeeded: before the
    /// window, or renewed and written.
    pub fn failure(&self) -> Option<String> {
        self.state.failure.lock().expect("failure lock").clone()
    }

    /// How many times the certificate was renewed and put in place.
    pub fn renewals(&self) -> u64 {
        self.state.renewals.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for CertificateRenewal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateRenewal")
            .field("renewals", &self.renewals())
            .field("failure", &self.failure())
            .finish()
    }
}

impl Drop for CertificateRenewal {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
    }
}

impl<R: Net + Clock + Spawn + Rng> Client<R> {
    /// Renews this client's certificate by itself (ADR 0123): looks at the
    /// certificate in `store` now and every `every` ([`RENEW_CHECK`] is the
    /// usual hour), and once its window is open, its last 30 days or past
    /// two thirds of its validity, renews it over a connection and hands
    /// the new key and chain to `store`. A failure is tried again after
    /// [`RENEW_RETRY`]. The store should make the client's connections use
    /// what it was given from the next connection on, as
    /// `lotse_rt_tokio::CertificateFiles` does.
    pub fn keep_certificate_renewed<S: CertificateStore>(
        self: &Arc<Self>,
        store: S,
        every: Duration,
    ) -> CertificateRenewal {
        let state = Arc::new(State::default());
        let (client, rt, looked) = (Arc::downgrade(self), self.rt.clone(), state.clone());
        drop(self.rt.spawn(async move { keep_renewed(client, rt, store, every, looked).await }));
        CertificateRenewal { state }
    }

    /// One look: renews and stores if the window is open. `Ok(true)` if it
    /// renewed.
    async fn renew_from<S: CertificateStore>(&self, store: &S) -> Result<bool, String> {
        let leaf = store.leaf().await.map_err(|e| format!("cannot read the certificate: {e}"))?;
        let Some(renewed) = self.renew_certificate_if_due(&leaf).await.map_err(|e| e.to_string())? else {
            return Ok(false);
        };
        // The cluster recorded the new certificate; if it cannot be put in
        // place, it is lost, the old one still works, and the next look
        // renews again.
        store
            .replace(&renewed.key_pem, &renewed.chain_pem)
            .await
            .map_err(|e| format!("renewed, but cannot store the new certificate: {e}"))?;
        Ok(true)
    }
}

async fn keep_renewed<R: Net + Clock + Spawn + Rng, S: CertificateStore>(
    client: Weak<Client<R>>,
    rt: R,
    store: S,
    every: Duration,
    state: Arc<State>,
) {
    loop {
        if state.stop.load(Ordering::Acquire) {
            return;
        }
        let Some(c) = client.upgrade() else { return };
        let wait = match c.renew_from(&store).await {
            Ok(renewed) => {
                if renewed {
                    state.renewals.fetch_add(1, Ordering::AcqRel);
                }
                *state.failure.lock().expect("failure lock") = None;
                every
            }
            Err(why) => {
                *state.failure.lock().expect("failure lock") = Some(why);
                every.min(RENEW_RETRY)
            }
        };
        // Not held while waiting: a client dropped meanwhile ends the loop.
        drop(c);
        rt.sleep(wait).await;
    }
}

#[cfg(test)]
mod tests;

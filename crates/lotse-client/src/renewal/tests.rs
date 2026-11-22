use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_proto::{ErrorCode, MetadataResponse, RenewCertificateResponse, Request, Response};
use lotse_rt::{CertificateStore, Clock as _};
use lotse_sim::{Sim, SimRuntime};

use crate::tests::{brokers, serve};
use crate::{Client, ClientConfig};

const DAY_MS: u64 = 86_400_000;

/// A certificate for `alice`, valid between the two times (ms since the
/// Unix epoch), DER; its key comes from `seed`.
fn cert(seed: u8, not_before_ms: u64, not_after_ms: u64) -> Vec<u8> {
    let key = lotse_auth::renew::new_key(&[seed; 32], "alice").unwrap().unwrap();
    let key = rcgen::KeyPair::from_pem(&key.key_pem).unwrap();
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    p.distinguished_name.push(rcgen::DnType::CommonName, "alice");
    let at = |ms: u64| time::OffsetDateTime::from_unix_timestamp(i64::try_from(ms / 1000).unwrap()).unwrap();
    p.not_before = at(not_before_ms);
    p.not_after = at(not_after_ms);
    p.self_signed(&key).unwrap().der().to_vec()
}

fn now_ms(rt: &SimRuntime) -> u64 {
    u64::try_from(rt.unix_time().as_millis()).unwrap()
}

/// What the store was given, and what it gives.
#[derive(Default)]
struct Kept {
    leaf: Option<Vec<u8>>,
    /// What `leaf` becomes once a renewal is stored.
    next: Vec<u8>,
    stored: Vec<(String, String)>,
    reads: u32,
    /// Replacements still to fail.
    failing: u32,
}

#[derive(Clone, Default)]
struct Store(Arc<Mutex<Kept>>);

impl Store {
    fn new(leaf: Vec<u8>, next: Vec<u8>) -> Store {
        Store(Arc::new(Mutex::new(Kept { leaf: Some(leaf), next, ..Kept::default() })))
    }

    fn kept<T>(&self, f: impl FnOnce(&mut Kept) -> T) -> T {
        f(&mut self.0.lock().unwrap())
    }
}

impl CertificateStore for Store {
    fn leaf(&self) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        let leaf = self.kept(|k| {
            k.reads += 1;
            k.leaf.clone()
        });
        async move { leaf.ok_or_else(|| io::Error::other("no certificate file")) }
    }

    fn replace(&self, key_pem: &str, chain_pem: &str) -> impl Future<Output = io::Result<()>> + Send {
        let done = self.kept(|k| {
            if k.failing > 0 {
                k.failing -= 1;
                return Err(io::Error::other("disk full"));
            }
            k.stored.push((key_pem.to_string(), chain_pem.to_string()));
            k.leaf = Some(k.next.clone());
            Ok(())
        });
        async move { done }
    }
}

/// A broker at `a:9000` that renews with `renewed` and counts the renewals
/// it was asked for.
fn renewing_broker(rt: &SimRuntime, renewed: Vec<u8>) -> Arc<AtomicU32> {
    let asked = Arc::new(AtomicU32::new(0));
    let count = asked.clone();
    serve(rt, "a", move |r| match r {
        Request::RenewCertificate(_) => {
            count.fetch_add(1, Ordering::SeqCst);
            Response::RenewCertificate(RenewCertificateResponse {
                error: ErrorCode::NONE,
                error_msg: None,
                leader_hint: None,
                chain: vec![renewed.clone().into()],
                not_after_ms: 0,
            })
        }
        _ => Response::Metadata(MetadataResponse { brokers: brokers(&[(1, "a")]), resources: vec![] }),
    });
    asked
}

fn client(rt: &SimRuntime) -> Arc<Client<SimRuntime>> {
    Arc::new(Client::new(
        rt.node("client"),
        ClientConfig { retry_for: Duration::from_secs(5), ..ClientConfig::new(vec!["a:9000".into()]) },
    ))
}

/// A year-long certificate a hundred days in: the client looks every
/// minute and asks the cluster for nothing.
#[test]
fn a_certificate_before_its_window_is_left_alone() {
    Sim::new(1).run(|rt| async move {
        let now = now_ms(&rt);
        let asked = renewing_broker(&rt, Vec::new());
        let store = Store::new(cert(1, now - 100 * DAY_MS, now + 265 * DAY_MS), Vec::new());
        let c = client(&rt);
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(60));
        rt.sleep(Duration::from_secs(5 * 60 + 1)).await;
        assert_eq!(asked.load(Ordering::SeqCst), 0);
        assert_eq!((renewal.renewals(), renewal.failure()), (0, None));
        assert_eq!(store.kept(|k| k.reads), 6, "at once and every minute");
        assert!(store.kept(|k| k.stored.is_empty()));
    });
}

/// In its last ten days the certificate is renewed and stored; the new one
/// is not due, so later looks leave it alone.
#[test]
fn a_certificate_in_its_window_is_renewed_once_and_stored() {
    Sim::new(2).run(|rt| async move {
        let now = now_ms(&rt);
        let new = cert(3, now, now + 90 * DAY_MS);
        let asked = renewing_broker(&rt, new.clone());
        let store = Store::new(cert(2, now - 355 * DAY_MS, now + 10 * DAY_MS), new.clone());
        let c = client(&rt);
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(60));
        rt.sleep(Duration::from_secs(5 * 60 + 1)).await;
        assert_eq!(asked.load(Ordering::SeqCst), 1);
        assert_eq!((renewal.renewals(), renewal.failure()), (1, None));
        let stored = store.kept(|k| k.stored.clone());
        assert_eq!(stored.len(), 1);
        assert!(stored[0].0.contains("BEGIN PRIVATE KEY"), "{}", stored[0].0);
        assert_eq!(stored[0].1, lotse_auth::renew::pem("CERTIFICATE", &new));
        assert_eq!(store.kept(|k| k.reads), 6);
    });
}

/// A renewal that cannot be stored is reported, and the client renews
/// again a minute later, though it looks only every hour otherwise.
#[test]
fn a_renewal_that_cannot_be_stored_is_tried_again_a_minute_later() {
    Sim::new(3).run(|rt| async move {
        let now = now_ms(&rt);
        let new = cert(5, now, now + 90 * DAY_MS);
        let asked = renewing_broker(&rt, new.clone());
        let store = Store::new(cert(4, now - 355 * DAY_MS, now + 10 * DAY_MS), new);
        store.kept(|k| k.failing = 1);
        let c = client(&rt);
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(3600));
        rt.sleep(Duration::from_secs(30)).await;
        assert_eq!(asked.load(Ordering::SeqCst), 1);
        let why = renewal.failure().expect("the failure is kept");
        assert!(why.contains("cannot store") && why.contains("disk full"), "{why}");
        assert_eq!(renewal.renewals(), 0);
        rt.sleep(Duration::from_secs(20)).await;
        assert_eq!(asked.load(Ordering::SeqCst), 1, "not before a minute");
        rt.sleep(Duration::from_secs(20)).await;
        assert_eq!(asked.load(Ordering::SeqCst), 2, "again after a minute");
        assert_eq!((renewal.renewals(), renewal.failure()), (1, None));
        assert_eq!(store.kept(|k| k.stored.len()), 1);
    });
}

/// A certificate that cannot be read is a failure too, tried again a
/// minute later.
#[test]
fn a_certificate_that_cannot_be_read_is_reported() {
    Sim::new(4).run(|rt| async move {
        let asked = renewing_broker(&rt, Vec::new());
        let store = Store::default();
        let c = client(&rt);
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(3600));
        rt.sleep(Duration::from_secs(61)).await;
        let why = renewal.failure().expect("the failure is kept");
        assert!(why.contains("cannot read the certificate"), "{why}");
        assert_eq!(store.kept(|k| k.reads), 2);
        assert_eq!(asked.load(Ordering::SeqCst), 0);
    });
}

/// Dropping the renewal, or the client, stops the looking.
#[test]
fn dropping_the_renewal_or_the_client_stops_it() {
    Sim::new(5).run(|rt| async move {
        let now = now_ms(&rt);
        renewing_broker(&rt, Vec::new());
        let leaf = cert(6, now - 100 * DAY_MS, now + 265 * DAY_MS);

        let store = Store::new(leaf.clone(), Vec::new());
        let c = client(&rt);
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(60));
        rt.sleep(Duration::from_secs(150)).await;
        assert_eq!(store.kept(|k| k.reads), 3);
        drop(renewal);
        rt.sleep(Duration::from_secs(300)).await;
        assert_eq!(store.kept(|k| k.reads), 3, "no look after the renewal was dropped");

        let store = Store::new(leaf, Vec::new());
        let renewal = c.keep_certificate_renewed(store.clone(), Duration::from_secs(60));
        rt.sleep(Duration::from_secs(150)).await;
        assert_eq!(store.kept(|k| k.reads), 3);
        let gone = Arc::downgrade(&c);
        drop(c);
        rt.sleep(Duration::from_secs(1)).await;
        assert!(gone.upgrade().is_none(), "the renewal does not keep the client while it waits");
        rt.sleep(Duration::from_secs(300)).await;
        assert_eq!(store.kept(|k| k.reads), 3, "no look after the client was dropped");
        drop(renewal);
    });
}

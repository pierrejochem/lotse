use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_rt::Instant;

use super::{Busy, Cache, CredentialKey, HttpCredential, IDLE, Limits, MAX, MAX_SIGN_INS};

/// A clock the test moves by hand.
fn clock() -> (Arc<Mutex<Instant>>, Box<dyn Fn() -> Instant + Send + Sync>) {
    let t = Arc::new(Mutex::new(Instant::ZERO));
    let read = t.clone();
    (t, Box::new(move || *read.lock().unwrap()))
}

/// What a test's sign-in fails with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum E {
    Refused,
    Busy,
}

impl From<Busy> for E {
    fn from(_: Busy) -> E {
        E::Busy
    }
}

fn advance(t: &Mutex<Instant>, by: Duration) {
    let mut t = t.lock().unwrap();
    *t = *t + by;
}

fn key(n: u32) -> CredentialKey {
    HttpCredential::Token(format!("lotse_tok_{n}")).key()
}

#[test]
fn authorization_headers_are_read() {
    use base64::Engine as _;
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s);
    assert_eq!(
        HttpCredential::from_authorization("Bearer lotse_tok_x"),
        Some(HttpCredential::Token("lotse_tok_x".into()))
    );
    assert_eq!(HttpCredential::from_authorization("bearer  t "), Some(HttpCredential::Token("t".into())));
    assert_eq!(
        HttpCredential::from_authorization(&format!("Basic {}", b64("erin:pa:ss"))),
        Some(HttpCredential::Password { user: "erin".into(), password: "pa:ss".into() }),
        "the password may hold colons"
    );
    assert_eq!(HttpCredential::from_authorization("Bearer "), None);
    assert_eq!(HttpCredential::from_authorization(&format!("Basic {}", b64(":pw"))), None, "no user");
    assert_eq!(HttpCredential::from_authorization(&format!("Basic {}", b64("nocolon"))), None);
    assert_eq!(HttpCredential::from_authorization("Basic !!!"), None);
    assert_eq!(HttpCredential::from_authorization("Negotiate abc"), None);
    assert_eq!(HttpCredential::from_authorization("lotse_tok_x"), None);
}

#[test]
fn keys_differ_by_credential_and_do_not_show_it() {
    let pw = |u: &str, p: &str| HttpCredential::Password { user: u.into(), password: p.into() };
    assert_eq!(pw("a", "b").key(), pw("a", "b").key());
    assert_ne!(pw("a", "bc").key(), pw("ab", "c").key());
    assert_ne!(pw("a", "b").key(), pw("a", "c").key());
    assert_ne!(HttpCredential::Token("x".into()).key(), HttpCredential::Token("y".into()).key());
    assert_ne!(HttpCredential::Token("a\0b".into()).key(), pw("a", "b").key());
    let shown = format!("{:?} {:?}", pw("erin", "secret"), HttpCredential::Token("lotse_tok_secret".into()));
    assert!(!shown.contains("secret"), "{shown}");
    assert_eq!(format!("{:?}", key(1)), "CredentialKey(..)");
}

#[tokio::test]
async fn a_value_is_made_once_and_then_held() {
    let (_t, now) = clock();
    let cache: Cache<u32> = Cache::new(now, Limits::default());
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, E>(7) }).await.unwrap();
    assert_eq!((*v, fresh), (7, true));
    let (again, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, E>(8) }).await.unwrap();
    assert!(Arc::ptr_eq(&v, &again) && !fresh);
    // Another key never gets this one's value.
    let (other, _) = cache.get_or_insert(key(2), || async { Ok::<_, E>(9) }).await.unwrap();
    assert_eq!(*other, 9);
}

#[tokio::test]
async fn a_failed_sign_in_holds_nothing() {
    let (_t, now) = clock();
    let cache: Cache<u32> = Cache::new(now, Limits::default());
    assert_eq!(cache.get_or_insert(key(1), || async { Err::<u32, _>(E::Refused) }).await.unwrap_err(), E::Refused);
    assert_eq!(cache.len(), 0);
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, E>(3) }).await.unwrap();
    assert_eq!((*v, fresh), (3, true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_for_one_key_make_once() {
    let (_t, now) = clock();
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now, Limits::default()));
    let made = Arc::new(AtomicUsize::new(0));
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let (cache, made) = (cache.clone(), made.clone());
            tokio::spawn(async move {
                cache
                    .get_or_insert(key(1), || async {
                        made.fetch_add(1, Ordering::SeqCst);
                        // Long enough for every other request to arrive.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, E>(5)
                    })
                    .await
                    .unwrap()
            })
        })
        .collect();
    for t in tasks {
        assert_eq!(*t.await.unwrap().0, 5);
    }
    assert_eq!(made.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn idle_values_go_after_fifteen_minutes() {
    let (t, now) = clock();
    let cache: Cache<u32> = Cache::new(now, Limits::default());
    cache.get_or_insert(key(1), || async { Ok::<_, E>(1) }).await.unwrap();
    cache.get_or_insert(key(2), || async { Ok::<_, E>(2) }).await.unwrap();
    advance(&t, IDLE - Duration::from_secs(1));
    // Used again: its fifteen minutes start over.
    assert!(!cache.get_or_insert(key(2), || async { Ok::<_, E>(0) }).await.unwrap().1);
    advance(&t, Duration::from_secs(1));
    cache.sweep();
    assert_eq!(cache.len(), 1, "key 1 went at fifteen minutes unused");
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, E>(10) }).await.unwrap();
    assert_eq!((*v, fresh), (10, true), "signed in afresh");
}

#[tokio::test]
async fn the_least_recently_used_goes_when_full() {
    let (t, now) = clock();
    let cache: Cache<u32> = Cache::new(now, Limits::default());
    for n in 0..MAX as u32 {
        cache.get_or_insert(key(n), move || async move { Ok::<_, E>(n) }).await.unwrap();
        advance(&t, Duration::from_millis(1));
    }
    // Key 0 is used again, so key 1 is now the least recently used.
    assert!(!cache.get_or_insert(key(0), || async { Ok::<_, E>(0) }).await.unwrap().1);
    advance(&t, Duration::from_millis(1));
    cache.get_or_insert(key(5_000), || async { Ok::<_, E>(5_000) }).await.unwrap();
    assert_eq!(cache.len(), MAX);
    assert!(!cache.get_or_insert(key(0), || async { Ok::<_, E>(0) }).await.unwrap().1, "key 0 stays");
    assert!(cache.get_or_insert(key(1), || async { Ok::<_, E>(1) }).await.unwrap().1, "key 1 went");
}

#[tokio::test]
async fn forgetting_drops_only_the_value_named() {
    let (_t, now) = clock();
    let cache: Cache<u32> = Cache::new(now, Limits::default());
    let (old, _) = cache.get_or_insert(key(1), || async { Ok::<_, E>(1) }).await.unwrap();
    cache.forget(&key(1), &old);
    assert_eq!(cache.len(), 0);
    let (new, _) = cache.get_or_insert(key(1), || async { Ok::<_, E>(2) }).await.unwrap();
    // A late forget of the old value leaves the new one.
    cache.forget(&key(1), &old);
    assert!(Arc::ptr_eq(&new, &cache.get_or_insert(key(1), || async { Ok::<_, E>(3) }).await.unwrap().0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn made_up_credentials_do_not_push_out_held_clients() {
    let (t, now) = clock();
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now, Limits::default()));
    // A full cache of clients that work.
    for n in 0..MAX as u32 {
        cache.get_or_insert(key(n), move || async move { Ok::<_, E>(n) }).await.unwrap();
        advance(&t, Duration::from_millis(1));
    }
    // 1,000 sign-ins with made-up credentials at once, each slow, then
    // refused by the cluster; those over the limits are busy.
    let tasks: Vec<_> = (0..1_000u32)
        .map(|n| {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .get_or_insert(key(100_000 + n), || async {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Err::<u32, _>(E::Refused)
                    })
                    .await
                    .unwrap_err()
            })
        })
        .collect();
    let mut busy = 0;
    for task in tasks {
        if task.await.unwrap() == E::Busy {
            busy += 1;
        }
    }
    assert!(busy > 0, "the pending bound refused some");
    assert_eq!(cache.held(), MAX, "every held client stays");
    assert_eq!(cache.len(), MAX, "nothing pending is left behind");
    for n in 0..MAX as u32 {
        let (v, fresh) = cache.get_or_insert(key(n), || async { Ok::<_, E>(0) }).await.unwrap();
        assert_eq!((*v, fresh), (n, false), "key {n} is still held");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_sign_ins_are_bounded() {
    let (_t, now) = clock();
    let limits = Limits { held: MAX, pending: 8, signing: 2, wait: Duration::from_secs(30) };
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now, limits));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let tasks: Vec<_> = (0..8u32)
        .map(|n| {
            let (cache, release) = (cache.clone(), release.clone());
            tokio::spawn(async move {
                cache
                    .get_or_insert(key(n), || async move {
                        let _ = release.acquire().await;
                        Ok::<_, E>(n)
                    })
                    .await
            })
        })
        .collect();
    while cache.len() < 8 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A ninth credential is refused at once; one already waiting is not.
    let ninth = cache.get_or_insert(key(99), || async { Ok::<_, E>(99) }).await;
    assert_eq!(ninth.unwrap_err(), E::Busy);
    assert_eq!(cache.len(), 8, "the map does not grow past the bound");
    release.add_permits(8);
    for t in tasks {
        assert!(t.await.unwrap().is_ok());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_so_many_sign_ins_run_at_once() {
    let (_t, now) = clock();
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now, Limits::default()));
    let (running, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let tasks: Vec<_> = (0..(MAX_SIGN_INS as u32 * 3))
        .map(|n| {
            let (cache, running, most) = (cache.clone(), running.clone(), most.clone());
            tokio::spawn(async move {
                cache
                    .get_or_insert(key(n), || async move {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        most.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(30)).await;
                        running.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, E>(n)
                    })
                    .await
            })
        })
        .collect();
    for t in tasks {
        assert!(t.await.unwrap().is_ok(), "within the wait, every sign-in gets its turn");
    }
    assert_eq!(most.load(Ordering::SeqCst), MAX_SIGN_INS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sign_in_that_waits_too_long_is_busy() {
    let (_t, now) = clock();
    let limits = Limits { held: MAX, pending: 8, signing: 1, wait: Duration::from_millis(50) };
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now, limits));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let first = {
        let (cache, release) = (cache.clone(), release.clone());
        tokio::spawn(async move {
            cache
                .get_or_insert(key(1), || async move {
                    let _ = release.acquire().await;
                    Ok::<_, E>(1)
                })
                .await
        })
    };
    while cache.len() < 1 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;
    let made = Arc::new(AtomicUsize::new(0));
    let m = made.clone();
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        cache.get_or_insert(key(2), || async move {
            m.fetch_add(1, Ordering::SeqCst);
            Ok::<_, E>(2)
        }),
    )
    .await
    .expect("refused within the wait, not queued");
    assert_eq!(second.unwrap_err(), E::Busy);
    assert_eq!(made.load(Ordering::SeqCst), 0, "it never reached the cluster");
    assert_eq!(cache.len(), 1, "its pending entry went");
    release.add_permits(1);
    assert!(first.await.unwrap().is_ok());
}

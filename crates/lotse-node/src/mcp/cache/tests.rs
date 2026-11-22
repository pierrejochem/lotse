use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lotse_rt::Instant;

use super::{Cache, CredentialKey, HttpCredential, IDLE, MAX};

/// A clock the test moves by hand.
fn clock() -> (Arc<Mutex<Instant>>, Box<dyn Fn() -> Instant + Send + Sync>) {
    let t = Arc::new(Mutex::new(Instant::ZERO));
    let read = t.clone();
    (t, Box::new(move || *read.lock().unwrap()))
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
    let cache: Cache<u32> = Cache::new(now);
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(7) }).await.unwrap();
    assert_eq!((*v, fresh), (7, true));
    let (again, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(8) }).await.unwrap();
    assert!(Arc::ptr_eq(&v, &again) && !fresh);
    // Another key never gets this one's value.
    let (other, _) = cache.get_or_insert(key(2), || async { Ok::<_, ()>(9) }).await.unwrap();
    assert_eq!(*other, 9);
}

#[tokio::test]
async fn a_failed_sign_in_holds_nothing() {
    let (_t, now) = clock();
    let cache: Cache<u32> = Cache::new(now);
    assert_eq!(cache.get_or_insert(key(1), || async { Err::<u32, _>("refused") }).await.unwrap_err(), "refused");
    assert_eq!(cache.len(), 0);
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(3) }).await.unwrap();
    assert_eq!((*v, fresh), (3, true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_for_one_key_make_once() {
    let (_t, now) = clock();
    let cache: Arc<Cache<u32>> = Arc::new(Cache::new(now));
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
                        Ok::<_, ()>(5)
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
    let cache: Cache<u32> = Cache::new(now);
    cache.get_or_insert(key(1), || async { Ok::<_, ()>(1) }).await.unwrap();
    cache.get_or_insert(key(2), || async { Ok::<_, ()>(2) }).await.unwrap();
    advance(&t, IDLE - Duration::from_secs(1));
    // Used again: its fifteen minutes start over.
    assert!(!cache.get_or_insert(key(2), || async { Ok::<_, ()>(0) }).await.unwrap().1);
    advance(&t, Duration::from_secs(1));
    cache.sweep();
    assert_eq!(cache.len(), 1, "key 1 went at fifteen minutes unused");
    let (v, fresh) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(10) }).await.unwrap();
    assert_eq!((*v, fresh), (10, true), "signed in afresh");
}

#[tokio::test]
async fn the_least_recently_used_goes_when_full() {
    let (t, now) = clock();
    let cache: Cache<u32> = Cache::new(now);
    for n in 0..MAX as u32 {
        cache.get_or_insert(key(n), move || async move { Ok::<_, ()>(n) }).await.unwrap();
        advance(&t, Duration::from_millis(1));
    }
    // Key 0 is used again, so key 1 is now the least recently used.
    assert!(!cache.get_or_insert(key(0), || async { Ok::<_, ()>(0) }).await.unwrap().1);
    advance(&t, Duration::from_millis(1));
    cache.get_or_insert(key(5_000), || async { Ok::<_, ()>(5_000) }).await.unwrap();
    assert_eq!(cache.len(), MAX);
    assert!(!cache.get_or_insert(key(0), || async { Ok::<_, ()>(0) }).await.unwrap().1, "key 0 stays");
    assert!(cache.get_or_insert(key(1), || async { Ok::<_, ()>(1) }).await.unwrap().1, "key 1 went");
}

#[tokio::test]
async fn forgetting_drops_only_the_value_named() {
    let (_t, now) = clock();
    let cache: Cache<u32> = Cache::new(now);
    let (old, _) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(1) }).await.unwrap();
    cache.forget(&key(1), &old);
    assert_eq!(cache.len(), 0);
    let (new, _) = cache.get_or_insert(key(1), || async { Ok::<_, ()>(2) }).await.unwrap();
    // A late forget of the old value leaves the new one.
    cache.forget(&key(1), &old);
    assert!(Arc::ptr_eq(&new, &cache.get_or_insert(key(1), || async { Ok::<_, ()>(3) }).await.unwrap().0));
}

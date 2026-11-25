use std::path::PathBuf;

use super::{NO_CREDENTIAL, Reach, SignIn, client, client_config};

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn p(v: &str) -> Option<PathBuf> {
    Some(PathBuf::from(v))
}

#[test]
fn exactly_one_credential_signs_in() {
    assert_eq!(SignIn::choose(s("lotse_tok_x"), None, None, None, None), Ok(SignIn::Token("lotse_tok_x".into())));
    assert_eq!(
        SignIn::choose(None, s("dave"), s("pw"), None, None),
        Ok(SignIn::Scram { user: "dave".into(), password: "pw".into() })
    );
    assert_eq!(
        SignIn::choose(None, None, None, p("c.pem"), p("c.key")),
        Ok(SignIn::Certificate { cert: "c.pem".into(), key: "c.key".into() })
    );
    assert_eq!(SignIn::choose(None, None, None, None, None), Err(NO_CREDENTIAL.to_string()));
    assert_eq!(SignIn::choose(s(""), s(""), None, None, None), Err(NO_CREDENTIAL.to_string()), "empty is none");
    assert!(SignIn::choose(None, s("dave"), None, None, None).unwrap_err().contains("LOTSE_PASSWORD"));
    assert!(SignIn::choose(None, None, s("pw"), None, None).unwrap_err().contains("LOTSE_USER"));
    assert!(SignIn::choose(None, None, None, p("c.pem"), None).unwrap_err().contains("together"));
    assert!(SignIn::choose(s("t"), s("dave"), s("pw"), None, None).unwrap_err().contains("not several"));
    assert!(SignIn::choose(s("t"), None, None, p("c.pem"), p("c.key")).unwrap_err().contains("not several"));
}

#[test]
fn half_of_a_second_credential_still_counts_as_a_second_choice() {
    for (token, user, password, cert, key) in [
        (s("t"), None, s("pw"), None, None),
        (s("t"), s("dave"), None, None, None),
        (s("t"), None, None, p("c.pem"), None),
        (s("t"), None, None, None, p("c.key")),
        (None, s("dave"), None, p("c.pem"), None),
        (None, None, s("pw"), None, p("c.key")),
    ] {
        let err = SignIn::choose(token.clone(), user.clone(), password.clone(), cert.clone(), key.clone())
            .expect_err("two credentials are refused");
        assert!(err.contains("not several"), "{token:?} {user:?} {password:?} {cert:?} {key:?}: {err}");
    }
}

#[test]
fn the_debug_form_hides_secrets() {
    let shown = format!(
        "{:?} {:?}",
        SignIn::Token("lotse_tok_secret".into()),
        SignIn::Scram { user: "dave".into(), password: "hunter2".into() }
    );
    assert!(!shown.contains("secret") && !shown.contains("hunter2"), "{shown}");
}

#[tokio::test]
async fn without_a_ca_only_plain_tcp_on_loopback_when_asked() {
    let reach = |ca: Option<PathBuf>, plain: bool| Reach {
        bootstrap: vec!["127.0.0.1:9092".into()],
        ca,
        plaintext_on_loopback: plain,
        zone: None,
    };
    let token = || SignIn::Token("t".into());
    assert!(client(&reach(None, false), token()).err().unwrap().contains("--plaintext-on-loopback"));
    assert!(client(&reach(None, true), token()).is_ok());
    assert!(client(&reach(p("ca.pem"), true), token()).is_err(), "a CA and plain TCP exclude each other");
    let cert = SignIn::Certificate { cert: "c.pem".into(), key: "c.key".into() };
    assert!(client(&reach(None, true), cert).err().unwrap().contains("needs TLS"));
    let none = Reach { bootstrap: Vec::new(), ca: None, plaintext_on_loopback: true, zone: None };
    assert!(client(&none, token()).is_err());
}

#[test]
fn the_zone_goes_into_the_clients_config() {
    let reach = |zone: Option<&str>| Reach {
        bootstrap: vec!["127.0.0.1:9092".into()],
        ca: None,
        plaintext_on_loopback: true,
        zone: zone.map(String::from),
    };
    assert_eq!(client_config(&reach(Some("eu-1")), None).zone.as_deref(), Some("eu-1"));
    assert_eq!(client_config(&reach(None), None).zone, None);
}

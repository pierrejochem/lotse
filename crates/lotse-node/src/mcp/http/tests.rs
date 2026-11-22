use std::path::PathBuf;

use super::{Listen, OWN_CREDENTIAL};

fn listen(addr: &str, cert: Option<&str>, key: Option<&str>, own: bool) -> Result<Listen, String> {
    Listen::new(addr.into(), cert.map(PathBuf::from), key.map(PathBuf::from), Vec::new(), own)
}

#[test]
fn plain_http_only_on_loopback() {
    assert!(listen("127.0.0.1:8080", None, None, false).is_ok());
    assert!(listen("[::1]:8080", None, None, false).is_ok());
    assert!(listen("localhost:8080", None, None, false).is_ok());
    for addr in ["0.0.0.0:8080", "[::]:8080", "192.0.2.1:8080"] {
        let e = listen(addr, None, None, false).unwrap_err();
        assert!(e.contains("loopback") && e.contains("--tls-cert"), "{addr}: {e}");
    }
    let tls = listen("0.0.0.0:8080", Some("c.pem"), Some("c.key"), false).unwrap();
    assert_eq!(tls.tls, Some((PathBuf::from("c.pem"), PathBuf::from("c.key"))));
    assert!(listen("0.0.0.0:8080", Some("c.pem"), None, false).unwrap_err().contains("together"));
    assert!(listen("no-port", None, None, false).is_err());
}

#[test]
fn a_credential_of_its_own_is_refused() {
    assert_eq!(listen("127.0.0.1:8080", None, None, true), Err(OWN_CREDENTIAL.to_string()));
}

#[test]
fn an_empty_origin_is_refused() {
    let e = Listen::new("127.0.0.1:1".into(), None, None, vec![" ".into()], false).unwrap_err();
    assert!(e.contains("--allow-origin"), "{e}");
}

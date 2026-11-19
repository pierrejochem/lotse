use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair, KeyUsagePurpose, SanType};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

use super::*;
use crate::x509::issuer_fingerprint;

const NOW: u64 = 1_800_000_000_000;

fn ca(name: &str, issuer: Option<(&rcgen::Issuer<'_, KeyPair>, ())>) -> (String, KeyPair, String) {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let mut p = CertificateParams::default();
    p.distinguished_name.push(DnType::CommonName, name);
    p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    p.not_before = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    p.not_after = time::OffsetDateTime::from_unix_timestamp(1_900_000_000).unwrap();
    let cert = match issuer {
        Some((i, ())) => p.signed_by(&key, i).unwrap(),
        None => p.self_signed(&key).unwrap(),
    };
    let pem = cert.pem();
    let key_pem = key.serialize_pem();
    (pem, key, key_pem)
}

fn der(pem: &str) -> Vec<u8> {
    x509_parser::pem::parse_x509_pem(pem.as_bytes()).unwrap().1.contents
}

struct Setup {
    root: Vec<u8>,
    renewal: RenewalCa,
    renewal_der: Vec<u8>,
}

fn setup() -> Setup {
    let (root_pem, root_key, _) = ca("Lotse root", None);
    let root_issuer = rcgen::Issuer::from_ca_cert_der(&der(&root_pem).into(), root_key).unwrap();
    let (renewal_pem, _, renewal_key) = ca("Lotse renewal", Some((&root_issuer, ())));
    let renewal = RenewalCa::from_pem(&renewal_pem, &renewal_key).unwrap();
    Setup { root: der(&root_pem), renewal_der: der(&renewal_pem), renewal }
}

fn csr(seed: u8, cn: &str) -> (NewKey, [u8; 32]) {
    let secret = [seed; 32];
    (new_key(&secret, cn).unwrap().unwrap(), secret)
}

#[test]
fn the_window_opens_in_the_last_30_days_or_after_two_thirds() {
    // A year: two thirds (day 243) come before the last 30 days (day 335).
    let year = 365 * DAY_MS;
    assert_eq!(window_opens_ms(0, year), year / 3 * 2);
    // 90 days: both at day 60.
    assert_eq!(window_opens_ms(0, 90 * DAY_MS), 60 * DAY_MS);
    // 40 days: the last 30 days (day 10) come before two thirds (day 26).
    assert_eq!(window_opens_ms(0, 40 * DAY_MS), 10 * DAY_MS);
    assert_eq!(window_opens_ms(DAY_MS, 31 * DAY_MS), DAY_MS);
    // Shorter than 30 days: renewable all along.
    assert_eq!(window_opens_ms(100, 103), 0);
    assert!(!may_renew(0, year, year / 3 * 2 - 1), "too early");
    assert!(may_renew(0, year, year / 3 * 2));
    assert!(may_renew(0, year, year - 1));
    assert!(!may_renew(0, year, year), "expired");
    assert_eq!(window_opens_ms(u64::MAX - 10, u64::MAX), u64::MAX - WINDOW_MS);
}

#[test]
fn a_renewed_certificate_is_a_client_certificate_for_the_principal_with_the_requests_key() {
    let s = setup();
    s.renewal.check_under(std::slice::from_ref(&s.root)).unwrap();
    let (key, _) = csr(7, "asked-for-someone-else");
    let r = s.renewal.sign(&key.csr, "alice", [0xff; 16], NOW).unwrap();
    let (_, c) = X509Certificate::from_der(&r.der).unwrap();
    let names: Vec<&str> = c.subject().iter_common_name().map(|n| n.as_str().unwrap()).collect();
    assert_eq!(names, ["alice"], "the principal, not what the request asked");
    let request = x509_parser::certification_request::X509CertificationRequest::from_der(&key.csr).unwrap().1;
    assert_eq!(c.public_key().raw, request.certification_request_info.subject_pki.raw, "the request's key");
    assert!(!c.basic_constraints().ok().flatten().is_some_and(|b| b.value.ca));
    let eku = c.extended_key_usage().unwrap().unwrap().value;
    assert!(eku.client_auth && !eku.server_auth);
    assert!(
        c.subject_alternative_name()
            .ok()
            .flatten()
            .is_none_or(|san| { !san.value.general_names.iter().any(|n| matches!(n, GeneralName::URI(_))) })
    );
    // Signed by the renewal CA, and named as issued by it.
    let (_, ca) = X509Certificate::from_der(&s.renewal_der).unwrap();
    c.verify_signature(Some(ca.public_key())).unwrap();
    assert!(issued_by(&s.renewal_der).contains(&issuer_fingerprint(&c)));
    assert!(!issued_by(&s.root).contains(&issuer_fingerprint(&c)), "not as if the root issued it");
    assert_eq!(r.facts.not_before_ms, (NOW - BACKDATE_MS) / 1000 * 1000);
    assert_eq!(r.facts.not_after_ms, (NOW + RENEWED_VALIDITY_MS) / 1000 * 1000);
    assert_eq!(r.facts.serial[0] & 0x80, 0, "positive");
    // Deterministic: the same inputs make the same certificate.
    assert_eq!(s.renewal.sign(&key.csr, "alice", [0xff; 16], NOW).unwrap(), r);
    assert_ne!(s.renewal.sign(&key.csr, "alice", [0xfe; 16], NOW).unwrap().facts.serial, r.facts.serial);
}

#[test]
fn a_renewed_certificate_never_outlives_the_renewal_ca() {
    let s = setup();
    let (key, _) = csr(9, "x");
    let late = 1_900_000_000_000 - DAY_MS;
    assert_eq!(s.renewal.sign(&key.csr, "alice", [1; 16], late).unwrap().facts.not_after_ms, 1_900_000_000_000);
    assert_eq!(s.renewal.sign(&key.csr, "alice", [1; 16], 1_900_000_000_000), Err(RenewError::CaExpired));
}

#[test]
fn bad_requests_keys_and_cas_are_refused() {
    let s = setup();
    let (mut key, _) = csr(3, "x");
    let last = key.csr.len() - 1;
    key.csr[last] ^= 1;
    assert!(
        matches!(s.renewal.sign(&key.csr, "alice", [1; 16], NOW), Err(RenewError::BadRequest(_))),
        "a broken signature"
    );
    assert!(matches!(s.renewal.sign(b"junk", "alice", [1; 16], NOW), Err(RenewError::BadRequest(_))));
    // Not under this root.
    let (other, _, _) = ca("Another root", None);
    assert_eq!(s.renewal.check_under(&[der(&other)]), Err(RenewError::NotUnderRoot));
    // A key that is not the certificate's, and a certificate that is no CA.
    let (pem, _, _) = ca("Lotse renewal", None);
    let (_, _, wrong) = ca("Lotse renewal", None);
    assert!(matches!(RenewalCa::from_pem(&pem, &wrong), Err(RenewError::BadKey(_))));
    let leaf_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let mut p = CertificateParams::new(vec![]).unwrap();
    p.subject_alt_names = vec![SanType::URI("lotse://node/1".try_into().unwrap())];
    let leaf = p.self_signed(&leaf_key).unwrap();
    assert!(matches!(RenewalCa::from_pem(&leaf.pem(), &leaf_key.serialize_pem()), Err(RenewError::BadCa(_))));
    assert!(format!("{:?}", s.renewal).contains("not_after_ms"), "debug output names no key");
}

#[test]
fn a_new_key_is_a_valid_p256_key_and_a_request_signed_with_it() {
    let (key, secret) = csr(5, "alice");
    let parsed = p256::SecretKey::from_pkcs8_pem(&key.key_pem).unwrap();
    assert_eq!(parsed.to_bytes().as_slice(), secret);
    CertificateSigningRequestParams::from_der(&key.csr.clone().into()).unwrap();
    assert!(new_key(&[0; 32], "alice").is_none(), "zero is no key");
    assert!(new_key(&[0xff; 32], "alice").is_none(), "above the group order");
}

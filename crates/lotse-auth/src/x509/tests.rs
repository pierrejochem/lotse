use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    SanType,
};
use sha2::{Digest, Sha256};
use x509_parser::prelude::{FromDer, X509Certificate};

use super::*;

struct Ca {
    der: Vec<u8>,
    issuer: Issuer<'static, KeyPair>,
}

fn make_ca(name: &str) -> Ca {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let key = KeyPair::generate().unwrap();
    let der = params.self_signed(&key).unwrap().der().to_vec();
    Ca { der, issuer: Issuer::new(params, key) }
}

/// A client certificate as `lotse certs --sign` makes one.
fn client_params(principal: &str) -> CertificateParams {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, principal);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.use_authority_key_identifier_extension = true;
    params
}

fn csr(key: &KeyPair, principal: &str) -> Vec<u8> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, principal);
    params.serialize_request(key).unwrap().der().to_vec()
}

fn sign(params: CertificateParams, key: &KeyPair, ca: &Ca) -> Vec<u8> {
    params.signed_by(key, &ca.issuer).unwrap().der().to_vec()
}

#[test]
fn a_certificate_that_answers_its_request_is_taken_with_its_own_facts() {
    let ca = make_ca("Lotse cluster CA");
    let key = KeyPair::generate().unwrap();
    let cert = sign(client_params("alice"), &key, &ca);
    let facts = check_signed(&cert, &csr(&key, "alice"), "alice", std::slice::from_ref(&ca.der)).unwrap();
    let (_, parsed) = X509Certificate::from_der(&cert).unwrap();
    assert_eq!(facts.serial, parsed.raw_serial());
    assert_eq!(facts.not_before_ms, parsed.validity().not_before.timestamp() as u64 * 1000);
    assert_eq!(facts.not_after_ms, parsed.validity().not_after.timestamp() as u64 * 1000);
    assert_eq!(Some(facts.clone()), super::facts(&cert), "the same facts as for a connection");
    // Another CA among the cluster's does not stand in the way.
    let other = make_ca("Another CA");
    check_signed(&cert, &csr(&key, "alice"), "alice", &[other.der, ca.der]).unwrap();
}

#[test]
fn an_issuer_is_its_name_and_key_identifier() {
    let (old, new) = (make_ca("Lotse cluster CA"), make_ca("Lotse cluster CA"));
    let key = KeyPair::generate().unwrap();
    let (a, b) = (sign(client_params("alice"), &key, &old), sign(client_params("alice"), &key, &new));
    let (fa, fb) = (facts(&a).unwrap().issuer, facts(&b).unwrap().issuer);
    assert_ne!(fa, fb, "two CAs with one name are two issuers");
    let (_, parsed) = X509Certificate::from_der(&a).unwrap();
    assert_ne!(fa, <[u8; 32]>::from(Sha256::digest(parsed.issuer().as_raw())), "the key identifier counts");
    let again = sign(client_params("bob"), &KeyPair::generate().unwrap(), &old);
    assert_eq!(facts(&again).unwrap().issuer, fa, "one CA is one issuer");

    // Without the extension, the name alone, as a revocation list has it.
    let mut bare = client_params("carol");
    bare.use_authority_key_identifier_extension = false;
    let c = sign(bare, &key, &old);
    let (_, parsed) = X509Certificate::from_der(&c).unwrap();
    assert_eq!(facts(&c).unwrap().issuer, <[u8; 32]>::from(Sha256::digest(parsed.issuer().as_raw())));
}

#[test]
fn a_certificate_that_does_not_answer_its_request_is_refused() {
    let ca = make_ca("Lotse cluster CA");
    let cas = std::slice::from_ref(&ca.der);
    let key = KeyPair::generate().unwrap();
    let request = csr(&key, "alice");
    let good = sign(client_params("alice"), &key, &ca);

    assert_eq!(check_signed(&good, &request, "alice", &[]), Err(SignedError::NoCa));
    assert_eq!(check_signed(&[0x30, 0x00], &request, "alice", cas), Err(SignedError::NotACertificate));
    let mut trailing = good.clone();
    trailing.push(0);
    assert_eq!(check_signed(&trailing, &request, "alice", cas), Err(SignedError::NotACertificate));
    assert_eq!(check_signed(&good, &[0x30, 0x00], "alice", cas), Err(SignedError::BadRequest));

    let other_key = sign(client_params("alice"), &KeyPair::generate().unwrap(), &ca);
    assert_eq!(check_signed(&other_key, &request, "alice", cas), Err(SignedError::OtherKey));
    let other_name = sign(client_params("mallory"), &key, &ca);
    assert_eq!(check_signed(&other_name, &request, "alice", cas), Err(SignedError::OtherPrincipal("alice".into())));
    let mut two_names = client_params("alice");
    two_names.distinguished_name.push(DnType::OrganizationName, "x");
    check_signed(&sign(two_names, &key, &ca), &request, "alice", cas).expect("other parts of the name are fine");

    let mut as_ca = client_params("alice");
    as_ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    assert_eq!(check_signed(&sign(as_ca, &key, &ca), &request, "alice", cas), Err(SignedError::NotAClientCertificate));
    let mut as_node = client_params("alice");
    as_node.subject_alt_names = vec![SanType::URI("lotse://node/1".try_into().unwrap())];
    assert_eq!(
        check_signed(&sign(as_node, &key, &ca), &request, "alice", cas),
        Err(SignedError::NotAClientCertificate)
    );

    // Signed by another CA, also one with the cluster CA's name.
    let impostor = make_ca("Lotse cluster CA");
    assert_eq!(
        check_signed(&sign(client_params("alice"), &key, &impostor), &request, "alice", cas),
        Err(SignedError::NotSigned)
    );
    // Signed with the cluster CA's key under another name: the handshake
    // finds no CA by that name, and neither does the check.
    let mut renamed = CertificateParams::default();
    renamed.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    renamed.distinguished_name.push(DnType::CommonName, "Another name");
    let renamed = Issuer::new(renamed, KeyPair::try_from(ca.issuer.key().serialize_der()).unwrap());
    let cert = client_params("alice").signed_by(&key, &renamed).unwrap().der().to_vec();
    assert_eq!(check_signed(&cert, &request, "alice", cas), Err(SignedError::NotSigned));
    // A damaged signature: the last byte of the certificate is the signature's.
    let mut damaged = good.clone();
    *damaged.last_mut().unwrap() ^= 1;
    assert_eq!(check_signed(&damaged, &request, "alice", cas), Err(SignedError::NotSigned));
}

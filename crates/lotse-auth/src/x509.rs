//! Client certificates, free of I/O (ADR 0096).
//!
//! - [`issuer_fingerprint`]: how the broker names a certificate's issuer, in
//!   revocations (ADR 0090) and the inventory (ADR 0091): the issuer's name
//!   and, when the certificate has one, its authority key identifier, so two
//!   CAs with one name but different keys are two issuers.
//! - [`facts`]: issuer, serial and validity of a certificate.
//! - [`check_signed`]: whether a certificate the CA holder records answers a
//!   signing request: the request's key, its principal, signed by one of
//!   the cluster's CA certificates (ADR 0092).

use sha2::{Digest, Sha256};
use x509_parser::certification_request::X509CertificationRequest;
use x509_parser::prelude::{FromDer, GeneralName, ParsedExtension, X509Certificate};

/// The URI prefix that makes a certificate a node certificate; a client
/// certificate must not carry it (`lotse_rt::PeerIdentity::NODE_URI`).
const NODE_URI: &str = "lotse://node/";

/// SHA-256 over the issuer's distinguished name (DER) followed by the
/// certificate's authority key identifier, if it has one. A DER name is
/// self-delimiting, so the two parts cannot run into each other. Without
/// the extension the fingerprint is the hash of the name alone, as a
/// revocation list names an issuer.
pub fn issuer_fingerprint(cert: &X509Certificate<'_>) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(cert.issuer().as_raw());
    if let Some(id) = authority_key_id(cert) {
        h.update(id);
    }
    h.finalize().into()
}

/// The certificate's authority key identifier: which key of its issuer
/// signed it, if the certificate says.
pub fn authority_key_id<'a>(cert: &'a X509Certificate<'_>) -> Option<&'a [u8]> {
    cert.extensions().iter().find_map(|e| match e.parsed_extension() {
        ParsedExtension::AuthorityKeyIdentifier(a) => a.key_identifier.as_ref().map(|k| k.0),
        _ => None,
    })
}

/// What a certificate says about itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Facts {
    /// See [`issuer_fingerprint`].
    pub issuer: [u8; 32],
    /// The serial number, big-endian, as the certificate has it.
    pub serial: Vec<u8>,
    /// First millisecond it is valid in, since the Unix epoch.
    pub not_before_ms: u64,
    /// Millisecond it stops being valid in, since the Unix epoch.
    pub not_after_ms: u64,
}

fn facts_of(c: &X509Certificate<'_>) -> Option<Facts> {
    let ms = |t: i64| u64::try_from(t).ok().map(|s| s.saturating_mul(1000));
    Some(Facts {
        issuer: issuer_fingerprint(c),
        serial: c.tbs_certificate.raw_serial().to_vec(),
        not_before_ms: ms(c.validity().not_before.timestamp())?,
        not_after_ms: ms(c.validity().not_after.timestamp())?,
    })
}

/// The facts of the certificate `der`; `None` if it does not parse or its
/// validity lies before 1970.
pub fn facts(der: &[u8]) -> Option<Facts> {
    let (_, c) = X509Certificate::from_der(der).ok()?;
    facts_of(&c)
}

/// Why a recorded certificate is refused. The messages go back to the CA
/// holder as they are.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SignedError {
    /// No CA certificate to check against.
    #[error("this node has no cluster CA to check the certificate against (set [tls] ca)")]
    NoCa,
    /// The bytes are no single DER certificate.
    #[error("the certificate is not one X.509 certificate in DER form")]
    NotACertificate,
    /// The request's bytes do not parse (it was checked when it was made).
    #[error("the signing request does not parse")]
    BadRequest,
    /// Another key than the request's.
    #[error("the certificate does not have the signing request's key")]
    OtherKey,
    /// Another principal than the request's, or none.
    #[error("the certificate's common name is not the requested principal {0}")]
    OtherPrincipal(String),
    /// A CA or node certificate.
    #[error("the certificate is a CA or node certificate, not a client certificate")]
    NotAClientCertificate,
    /// It ends before it starts.
    #[error("the certificate ends before it starts")]
    Backwards,
    /// No CA certificate of the cluster signed it.
    #[error("no CA certificate of this cluster signed the certificate")]
    NotSigned,
}

/// Checks that `cert` (DER) answers the signing request `csr` (DER) for
/// `principal`: the request's public key, the principal as its only common
/// name, no CA flag and no node URI, and a signature by one of `cas` (DER
/// CA certificates) whose name is the certificate's issuer. Returns what
/// the certificate says about itself. Validity against the current time is
/// not checked: a certificate may start later, and an expired one is
/// harmless.
pub fn check_signed(cert: &[u8], csr: &[u8], principal: &str, cas: &[Vec<u8>]) -> Result<Facts, SignedError> {
    if cas.is_empty() {
        return Err(SignedError::NoCa);
    }
    let c = match X509Certificate::from_der(cert) {
        Ok(([], c)) => c,
        _ => return Err(SignedError::NotACertificate),
    };
    let request = match X509CertificationRequest::from_der(csr) {
        Ok((_, r)) => r,
        Err(_) => return Err(SignedError::BadRequest),
    };
    if c.public_key().raw != request.certification_request_info.subject_pki.raw {
        return Err(SignedError::OtherKey);
    }
    let names: Vec<&str> = c.subject().iter_common_name().map(|n| n.as_str().unwrap_or("")).collect();
    if names != [principal] {
        return Err(SignedError::OtherPrincipal(principal.into()));
    }
    let is_ca = c.basic_constraints().ok().flatten().is_some_and(|b| b.value.ca);
    let node_uri = c.subject_alternative_name().ok().flatten().is_some_and(|san| {
        san.value.general_names.iter().any(|n| matches!(n, GeneralName::URI(u) if u.starts_with(NODE_URI)))
    });
    if is_ca || node_uri {
        return Err(SignedError::NotAClientCertificate);
    }
    let facts = facts_of(&c).ok_or(SignedError::Backwards)?;
    if facts.not_after_ms <= facts.not_before_ms {
        return Err(SignedError::Backwards);
    }
    // Of the CA certificates with the issuer's name (an old and a new one
    // during a rotation may share it), the signature decides.
    let signed = cas.iter().any(|der| {
        let Ok((_, ca)) = X509Certificate::from_der(der) else { return false };
        ca.subject().as_raw() == c.issuer().as_raw() && c.verify_signature(Some(ca.public_key())).is_ok()
    });
    if !signed {
        return Err(SignedError::NotSigned);
    }
    Ok(facts)
}

#[cfg(test)]
mod tests;

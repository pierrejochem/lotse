//! Renewing a client certificate over a connection made with it (ADR 0123),
//! free of I/O.
//!
//! - [`RenewalCa`]: the cluster's renewal CA, an intermediate under the
//!   offline root, whose certificate and key every node holds. It signs a
//!   renewed certificate for the principal the connection's certificate
//!   names, with the key of the client's signing request.
//! - [`may_renew`]: when a certificate may be renewed.
//! - [`new_key`]: a new P-256 key and a signing request for it, for clients.
//! - [`issued_by`]: how certificates a CA issued name it, so a node can tell
//!   a certificate its roots issued from one an intermediate issued.
//!
//! Signatures are ECDSA P-256 with SHA-256, made deterministically (RFC
//! 6979): signing needs no randomness, and the only random input, a
//! certificate's serial or a new key, comes from the caller.

use p256::ecdsa::signature::Signer as _;
use p256::pkcs8::{DecodePrivateKey as _, EncodePrivateKey as _, LineEnding};
use rcgen::{
    CertificateParams, CertificateSigningRequestParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyUsagePurpose, PublicKeyData, SerialNumber, SignatureAlgorithm, SigningKey,
};
use sha2::{Digest, Sha256};
use x509_parser::prelude::{FromDer, ParsedExtension, X509Certificate};

use crate::x509::{Facts, facts};

/// How long a renewed certificate is valid: 90 days.
pub const RENEWED_VALIDITY_MS: u64 = 90 * DAY_MS;

/// A certificate may be renewed in its last 30 days.
pub const WINDOW_MS: u64 = 30 * DAY_MS;

const DAY_MS: u64 = 24 * 3_600_000;

/// A renewed certificate starts this long before it was signed, so a
/// client whose clock is a little behind can use it at once.
const BACKDATE_MS: u64 = 60_000;

/// Why a renewal cannot be made.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RenewError {
    /// The renewal CA's certificate does not parse, or is no CA.
    #[error("the renewal CA certificate is not one CA certificate in PEM form: {0}")]
    BadCa(String),
    /// The renewal CA's key does not parse, is no P-256 key, or is not the
    /// certificate's.
    #[error("the renewal CA key: {0}")]
    BadKey(String),
    /// No root of the cluster signed the renewal CA.
    #[error("no CA certificate of this cluster ([tls] ca) signed the renewal CA")]
    NotUnderRoot,
    /// The client's signing request does not parse or its signature is wrong.
    #[error("the signing request is not one PKCS #10 request signed with its own key: {0}")]
    BadRequest(String),
    /// The renewal CA expires before a renewed certificate could start.
    #[error("the renewal CA has expired; the CA holder makes a new one with `lotse certs --renewal-ca`")]
    CaExpired,
    /// The principal cannot be a common name, or signing failed.
    #[error("the certificate could not be made: {0}")]
    Sign(String),
}

/// When a certificate valid from `not_before_ms` until `not_after_ms` may
/// first be renewed: in its last 30 days, or once two thirds of its
/// validity have passed, whichever comes first.
pub fn window_opens_ms(not_before_ms: u64, not_after_ms: u64) -> u64 {
    let span = not_after_ms.saturating_sub(not_before_ms);
    let two_thirds = not_before_ms.saturating_add(span / 3 * 2 + span % 3 * 2 / 3);
    not_after_ms.saturating_sub(WINDOW_MS).min(two_thirds)
}

/// Whether a certificate valid from `not_before_ms` until `not_after_ms`
/// may be renewed at `now_ms`: its window is open and it has not expired.
pub fn may_renew(not_before_ms: u64, not_after_ms: u64, now_ms: u64) -> bool {
    now_ms >= window_opens_ms(not_before_ms, not_after_ms) && now_ms < not_after_ms
}

/// The fingerprints (`x509::issuer_fingerprint`) a certificate that the CA
/// `ca` (DER) issued carries: its name with its key identifier, and its
/// name alone for a certificate without an authority key identifier.
pub fn issued_by(ca: &[u8]) -> Vec<[u8; 32]> {
    let Ok((_, c)) = X509Certificate::from_der(ca) else { return Vec::new() };
    let mut out = Vec::new();
    if let Some(id) = subject_key_id(&c) {
        let mut h = Sha256::new();
        h.update(c.subject().as_raw());
        h.update(id);
        out.push(h.finalize().into());
    }
    out.push(Sha256::digest(c.subject().as_raw()).into());
    out
}

fn subject_key_id<'a>(c: &'a X509Certificate<'_>) -> Option<&'a [u8]> {
    c.extensions().iter().find_map(|e| match e.parsed_extension() {
        ParsedExtension::SubjectKeyIdentifier(k) => Some(k.0),
        _ => None,
    })
}

/// A P-256 key as rcgen signs with it.
struct Signer<'a> {
    key: &'a p256::ecdsa::SigningKey,
    /// The uncompressed public point.
    public: Vec<u8>,
}

impl<'a> Signer<'a> {
    fn new(key: &'a p256::ecdsa::SigningKey) -> Self {
        Signer { key, public: key.verifying_key().to_encoded_point(false).as_bytes().to_vec() }
    }
}

impl PublicKeyData for Signer<'_> {
    fn der_bytes(&self) -> &[u8] {
        &self.public
    }

    fn algorithm(&self) -> &'static SignatureAlgorithm {
        &rcgen::PKCS_ECDSA_P256_SHA256
    }
}

impl SigningKey for Signer<'_> {
    fn sign(&self, msg: &[u8]) -> Result<Vec<u8>, rcgen::Error> {
        let sig: p256::ecdsa::Signature = self.key.sign(msg);
        Ok(sig.to_der().as_bytes().to_vec())
    }
}

/// A renewed certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Renewed {
    /// The certificate, DER.
    pub der: Vec<u8>,
    /// What it says about itself.
    pub facts: Facts,
}

/// The cluster's renewal CA: an intermediate under one of the cluster's
/// roots, with a P-256 key.
pub struct RenewalCa {
    der: Vec<u8>,
    key: p256::ecdsa::SigningKey,
    not_after_ms: u64,
}

impl std::fmt::Debug for RenewalCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the key.
        f.debug_struct("RenewalCa").field("not_after_ms", &self.not_after_ms).finish_non_exhaustive()
    }
}

impl RenewalCa {
    /// The renewal CA from its certificate and its PKCS #8 key, both PEM.
    pub fn from_pem(cert_pem: &str, key_pem: &str) -> Result<RenewalCa, RenewError> {
        let (_, pem) =
            x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).map_err(|e| RenewError::BadCa(e.to_string()))?;
        let der = pem.contents;
        let (_, c) = X509Certificate::from_der(&der).map_err(|e| RenewError::BadCa(e.to_string()))?;
        if !c.basic_constraints().ok().flatten().is_some_and(|b| b.value.ca) {
            return Err(RenewError::BadCa("it is not a CA certificate".into()));
        }
        let secret = p256::SecretKey::from_pkcs8_pem(key_pem).map_err(|e| RenewError::BadKey(e.to_string()))?;
        let key = p256::ecdsa::SigningKey::from(secret);
        if c.public_key().subject_public_key.data.as_ref() != Signer::new(&key).public.as_slice() {
            return Err(RenewError::BadKey("it is not the key of the renewal CA certificate".into()));
        }
        let not_after_ms =
            facts(&der).ok_or_else(|| RenewError::BadCa("its validity is unreadable".into()))?.not_after_ms;
        Ok(RenewalCa { der, key, not_after_ms })
    }

    /// Checks that one of `roots` (DER) signed it.
    pub fn check_under(&self, roots: &[Vec<u8>]) -> Result<(), RenewError> {
        let (_, c) = X509Certificate::from_der(&self.der).map_err(|e| RenewError::BadCa(e.to_string()))?;
        let signed = roots.iter().any(|der| {
            let Ok((_, root)) = X509Certificate::from_der(der) else { return false };
            root.subject().as_raw() == c.issuer().as_raw() && c.verify_signature(Some(root.public_key())).is_ok()
        });
        if signed { Ok(()) } else { Err(RenewError::NotUnderRoot) }
    }

    /// Its certificate, DER: the second link of a renewed certificate's
    /// chain.
    pub fn certificate(&self) -> &[u8] {
        &self.der
    }

    /// When it expires; no renewed certificate outlives it.
    pub fn not_after_ms(&self) -> u64 {
        self.not_after_ms
    }

    /// Signs a renewed certificate for `principal` with the key of the
    /// signing request `csr` (DER), whose own signature must hold. Whatever
    /// else the request asks is ignored: the certificate names `principal`
    /// as its only common name, is a client certificate (no CA, no node
    /// URI, client authentication only), carries `serial`, and is valid
    /// from a minute before `now_ms` for [`RENEWED_VALIDITY_MS`], or until
    /// the renewal CA expires if that is sooner.
    pub fn sign(&self, csr: &[u8], principal: &str, serial: [u8; 16], now_ms: u64) -> Result<Renewed, RenewError> {
        let request = CertificateSigningRequestParams::from_der(&csr.to_vec().into())
            .map_err(|e| RenewError::BadRequest(e.to_string()))?;
        let not_before_ms = now_ms.saturating_sub(BACKDATE_MS);
        let not_after_ms = now_ms.saturating_add(RENEWED_VALIDITY_MS).min(self.not_after_ms);
        if not_after_ms <= now_ms {
            return Err(RenewError::CaExpired);
        }
        let mut params = CertificateParams::default();
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, principal);
        params.distinguished_name = name;
        params.is_ca = IsCa::ExplicitNoCa;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.use_authority_key_identifier_extension = true;
        // A positive, nonzero serial.
        let mut serial = serial;
        serial[0] = (serial[0] & 0x7f) | 0x40;
        params.serial_number = Some(SerialNumber::from_slice(&serial));
        params.not_before = at(not_before_ms)?;
        params.not_after = at(not_after_ms)?;
        let signer = Signer::new(&self.key);
        let issuer =
            Issuer::from_ca_cert_der(&self.der.clone().into(), signer).map_err(|e| RenewError::Sign(e.to_string()))?;
        let cert = CertificateSigningRequestParams { params, public_key: request.public_key }
            .signed_by(&issuer)
            .map_err(|e| RenewError::Sign(e.to_string()))?;
        let der = cert.der().to_vec();
        let facts = facts(&der).ok_or_else(|| RenewError::Sign("the new certificate does not parse".into()))?;
        Ok(Renewed { der, facts })
    }
}

fn at(ms: u64) -> Result<time::OffsetDateTime, RenewError> {
    let secs = i64::try_from(ms / 1000).map_err(|_| RenewError::Sign("a time out of range".into()))?;
    time::OffsetDateTime::from_unix_timestamp(secs).map_err(|e| RenewError::Sign(e.to_string()))
}

/// `der` as a PEM block with `label`, such as `CERTIFICATE`.
pub fn pem(label: &str, der: &[u8]) -> String {
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// A new key and a signing request for it.
#[derive(Debug, Clone)]
pub struct NewKey {
    /// The private key, PKCS #8 PEM: what the client keeps.
    pub key_pem: String,
    /// The signing request, DER: what the client sends.
    pub csr: Vec<u8>,
}

/// A new P-256 key made from `secret`, 32 random bytes, and a signing
/// request for `principal` signed with it. `None` for the rare secret that
/// is no valid key (zero, or not below the group order): draw again.
pub fn new_key(secret: &[u8; 32], principal: &str) -> Option<Result<NewKey, RenewError>> {
    let secret = p256::SecretKey::from_slice(secret).ok()?;
    Some((|| {
        let key_pem = secret.to_pkcs8_pem(LineEnding::LF).map_err(|e| RenewError::Sign(e.to_string()))?.to_string();
        let key = p256::ecdsa::SigningKey::from(&secret);
        let mut params = CertificateParams::default();
        let mut name = DistinguishedName::new();
        name.push(DnType::CommonName, principal);
        params.distinguished_name = name;
        let csr = params.serialize_request(&Signer::new(&key)).map_err(|e| RenewError::Sign(e.to_string()))?;
        Ok(NewKey { key_pem, csr: csr.der().to_vec() })
    })())
}

#[cfg(test)]
mod tests;

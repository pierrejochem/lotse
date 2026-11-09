//! Reading what the console passes on about certificates (ADR 0092): the
//! requester's key fingerprint from a signing request, and a certificate as
//! PEM. The console only reads; it never signs and never sees a private key.

use base64::Engine as _;
use sha2::{Digest, Sha256};
use x509_parser::prelude::FromDer;

/// SHA-256 of the public key in a PKCS#10 signing request, 64 hex digits:
/// the same value `lotse certs --sign` shows. `None` if the request does
/// not parse. The signature is not checked here: the CA holder's tool does.
pub fn key_fingerprint(csr_der: &[u8]) -> Option<String> {
    let (_, csr) = x509_parser::certification_request::X509CertificationRequest::from_der(csr_der).ok()?;
    let key = &csr.certification_request_info.subject_pki.subject_public_key.data;
    Some(Sha256::digest(key).iter().map(|b| format!("{b:02x}")).collect())
}

/// A certificate in DER form as PEM, lines of 64 characters.
pub fn pem(der: &[u8]) -> String {
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_certificate_is_wrapped_at_64_columns_and_unreadable_requests_have_no_fingerprint() {
        let text = pem(&[7u8; 100]);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.first(), Some(&"-----BEGIN CERTIFICATE-----"));
        assert_eq!(lines.last(), Some(&"-----END CERTIFICATE-----"));
        assert!(lines[1..lines.len() - 1].iter().all(|l| l.len() <= 64));
        assert_eq!(lines[1].len(), 64);
        assert_eq!(key_fingerprint(&[1, 2, 3]), None);
    }

    /// The request the browser code made for `alice` (the same one as in
    /// `crates/lotse-node/tests/certificates.rs`): the fingerprint is the
    /// one `lotse certs --sign` shows, so the two sides agree on what the
    /// CA holder compares.
    #[test]
    fn the_fingerprint_of_a_browser_request_is_the_one_the_ca_holder_sees() {
        let der = base64::engine::general_purpose::STANDARD
            .decode("MIHKMHICAQAwEDEOMAwGA1UEAwwFYWxpY2UwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARnKUSI4DRkymlj6IpyYHjl9g4fkO/KxbIv1mwPnPz6Uzzgu6zhq+BZCN0Ag5wvdlBj3SbgAATmU4mbaITAsGl6oAAwCgYIKoZIzj0EAwIDSAAwRQIgQKvHGNTyKosN+lfdcw26mTmMr9BK5y34EAESHttDA1kCIQCFfhWHtobQFEY8SXm5AquJHSijZHVx/Xps8gbif2hQvQ==")
            .unwrap();
        assert_eq!(
            key_fingerprint(&der).as_deref(),
            Some("23b66907cf9a2d8b55976d1142b8ec5c5a2b85b18685722603440f15c2da42c5")
        );
    }
}

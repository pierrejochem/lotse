//! A throwaway certificate authority for tests and local clusters: never
//! for production, where certificates come from the operator's CA.

use rcgen::{
    BasicConstraints, CertificateParams, CertificateRevocationListParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyIdMethod, KeyPair, KeyUsagePurpose, RevokedCertParams, SanType, SerialNumber,
};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;

use crate::tls::{TlsConfig, TlsError};

/// A certificate and its private key, PEM.
#[derive(Debug, Clone)]
pub struct Issued {
    /// The certificate.
    pub cert_pem: String,
    /// Its private key.
    pub key_pem: String,
}

/// A self-signed CA that issues node and principal certificates.
pub struct DevCa {
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl std::fmt::Debug for DevCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevCa").finish_non_exhaustive()
    }
}

impl DevCa {
    /// A new CA with a fresh key.
    pub fn new() -> DevCa {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, "Lotse development CA");
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().expect("a key pair");
        let cert = params.self_signed(&key).expect("a self-signed CA");
        DevCa { cert, key }
    }

    /// The CA certificate, PEM.
    pub fn ca_pem(&self) -> String {
        self.cert.pem()
    }

    /// A node certificate: `lotse://node/<id>`, plus each of `hosts` (DNS
    /// names or IP addresses) so clients can check the name they dial.
    pub fn node(&self, id: u64, hosts: &[&str]) -> Issued {
        let mut sans = vec![SanType::URI(format!("{}{id}", lotse_rt::PeerIdentity::NODE_URI).try_into().expect("ia5"))];
        sans.extend(hosts.iter().map(|h| san(h)));
        self.issue(&format!("node-{id}"), sans, true)
    }

    /// A client certificate for `principal` (its common name).
    pub fn principal(&self, principal: &str) -> Issued {
        self.issue(principal, Vec::new(), false)
    }

    /// A certificate with any subject alternative names, for tests of what
    /// a node must refuse.
    pub fn custom(&self, common_name: &str, uris: &[&str], hosts: &[&str]) -> Issued {
        let mut sans: Vec<SanType> = uris.iter().map(|u| SanType::URI((*u).try_into().expect("ia5"))).collect();
        sans.extend(hosts.iter().map(|h| san(h)));
        self.issue(common_name, sans, true)
    }

    /// TLS for a node with id `id` reachable at `hosts`.
    pub fn node_tls(&self, id: u64, hosts: &[&str]) -> Result<TlsConfig, TlsError> {
        let n = self.node(id, hosts);
        TlsConfig::from_pem(self.ca_pem().as_bytes(), Some((n.cert_pem.as_bytes(), n.key_pem.as_bytes())))
    }

    /// TLS for a client, with a certificate for `principal` or none.
    pub fn client_tls(&self, principal: Option<&str>) -> Result<TlsConfig, TlsError> {
        match principal {
            Some(p) => {
                let c = self.principal(p);
                TlsConfig::from_pem(self.ca_pem().as_bytes(), Some((c.cert_pem.as_bytes(), c.key_pem.as_bytes())))
            }
            None => TlsConfig::from_pem(self.ca_pem().as_bytes(), None),
        }
    }

    /// A certificate revocation list, PEM, signed by this CA and naming the
    /// certificates in `revoked` (each PEM).
    pub fn crl(&self, revoked: &[&str]) -> String {
        let now = time::OffsetDateTime::now_utc();
        let revoked_certs = revoked
            .iter()
            .map(|pem| {
                let der = CertificateDer::from_pem_slice(pem.as_bytes()).expect("a certificate");
                let (_, cert) = x509_parser::parse_x509_certificate(&der).expect("a certificate");
                RevokedCertParams {
                    serial_number: SerialNumber::from_slice(cert.raw_serial()),
                    revocation_time: now,
                    reason_code: None,
                    invalidity_date: None,
                }
            })
            .collect();
        let params = CertificateRevocationListParams {
            this_update: now - time::Duration::minutes(1),
            next_update: now + time::Duration::days(1),
            crl_number: SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs,
            key_identifier_method: KeyIdMethod::Sha256,
        };
        params.signed_by(&self.cert, &self.key).expect("a signed revocation list").pem().expect("PEM")
    }

    fn issue(&self, common_name: &str, sans: Vec<SanType>, server: bool) -> Issued {
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, common_name);
        params.subject_alt_names = sans;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = if server {
            vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth]
        } else {
            vec![ExtendedKeyUsagePurpose::ClientAuth]
        };
        params.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate().expect("a key pair");
        let cert = params.signed_by(&key, &self.cert, &self.key).expect("a signed certificate");
        Issued { cert_pem: cert.pem(), key_pem: key.serialize_pem() }
    }
}

impl Default for DevCa {
    fn default() -> Self {
        DevCa::new()
    }
}

fn san(host: &str) -> SanType {
    match host.parse::<std::net::IpAddr>() {
        Ok(ip) => SanType::IpAddress(ip),
        Err(_) => SanType::DnsName(host.try_into().expect("ia5")),
    }
}
